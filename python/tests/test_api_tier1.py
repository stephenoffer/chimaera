"""API Tier 1 (P2): forwarding, multiple returns, per-call actor options,
concurrency groups, ch.actor, subtree cancel, Run, priority, Queue and Topic."""

from __future__ import annotations

import os
import queue
import tempfile
import threading
import time

import pytest

import chimaera as ch

from conftest import run_script

# ---- forwarding -------------------------------------------------------------


@ch.remote
def add(a, b):
    return a + b


@ch.remote
def fib(n):
    return n if n < 2 else add.remote(fib.remote(n - 1), fib.remote(n - 2))


@ch.remote
def fail(msg):
    raise KeyError(msg)


@ch.remote
def forward_failure():
    return fail.remote("inner")


def test_returning_a_future_forwards_without_holding_workers():
    # fib(14) has ~600 calls each forwarding to an add: if a forwarding call
    # held its worker, the peer would run out of CPUs and deadlock.
    assert fib.remote(14).result(timeout=120) == 377


def test_a_forwarded_failure_arrives_as_the_original_error():
    with pytest.raises(KeyError, match="inner"):
        forward_failure.remote().result(timeout=30)


@ch.remote
def slow_value(x):
    time.sleep(0.3)
    return x


def test_the_same_pending_future_passed_twice_resolves():
    # Regression: a duplicated dependency used to be counted twice and never released.
    x = slow_value.remote(2)
    assert add.remote(x, x).result(timeout=30) == 4


# ---- multiple returns ---------------------------------------------------------------


@ch.remote(returns=2)
def stats(xs):
    return min(xs), max(xs)


@ch.remote
def spread(lo, hi):
    return hi - lo


def test_split_parts_are_separate_dependencies():
    lo, hi = stats.remote([3, 9, 4]).split()
    assert (lo.result(), hi.result()) == (3, 9)
    assert spread.remote(lo, hi).result() == 6
    a, b = stats.options(returns=2).remote([1, 2]).split()
    assert add.remote(a, b).result() == 3


def test_split_count_mismatch_is_reported_where_a_part_is_used():
    first, _, _ = stats.remote([1, 2]).split(3)
    with pytest.raises(ValueError, match="split into 3 values"):
        first.result()
    with pytest.raises(ValueError, match="split into 3 values"):
        spread.remote(first, 1).result()
    with pytest.raises(TypeError, match="returns=n"):
        add.remote(1, 2).split()


# ---- actors: per-call options, method settings, groups, lookup by key ----------------


@ch.remote(groups={"io": 4})
class Server:
    def __init__(self):
        self.log = []

    def slow(self, seconds):
        time.sleep(seconds)
        return "slow"

    @ch.method(group="io")
    def fetch(self, i):
        return i

    @ch.method(concurrency=2)
    def nap(self, seconds):
        time.sleep(seconds)
        return threading.get_ident()

    @ch.method(returns=2)
    def pair(self):
        return "left", "right"

    def record(self, x):
        self.log.append(x)
        return list(self.log)


def test_a_group_runs_beside_a_busy_default_lane():
    s = Server.remote()
    busy = s.slow.remote(1.5)
    t = time.monotonic()
    assert [f.result(timeout=30) for f in [s.fetch.remote(i) for i in range(4)]] == [0, 1, 2, 3]
    assert time.monotonic() - t < 1.2, "io-group calls waited for the default lane"
    assert busy.result(timeout=30) == "slow"


def test_a_method_with_its_own_concurrency_overlaps():
    s = Server.remote()
    t = time.monotonic()
    threads = {f.result(timeout=30) for f in [s.nap.remote(0.6), s.nap.remote(0.6)]}
    assert time.monotonic() - t < 1.1 and len(threads) == 2


def test_per_call_actor_options_and_method_returns():
    s = Server.remote()
    with pytest.raises(ch.DeadlineExceeded):
        s.slow.options(deadline="200ms").remote(5).result(timeout=30)
    left, right = s.pair.remote().split()
    assert (left.result(), right.result()) == ("left", "right")
    with pytest.raises(TypeError, match="unknown option"):
        s.slow.options(bogus=1)


def test_an_undeclared_group_is_rejected_at_decoration():
    with pytest.raises(TypeError, match="group 'db'"):

        @ch.remote(groups={"io": 2})
        class Bad:
            @ch.method(group="db")
            def q(self):
                return 1


def test_actor_lookup_by_key_without_the_class():
    Server.options(name="tier1/server").remote().record.remote("a").result()
    found = ch.actor("tier1/server")
    assert found.record.remote("b").result(timeout=30) == ["a", "b"]
    with pytest.raises(ch.NotFound):
        ch.actor("tier1/nobody")


# ---- cancellation of a call and what it started ------------------------------------------


@ch.remote
def nap_then(seconds, value):
    time.sleep(seconds)
    return value


@ch.remote
def cooperative_nap(seconds):
    # Cancellation is cooperative: a body that checks stops promptly.
    end = time.monotonic() + seconds
    while time.monotonic() < end and not ch.context().cancelled():
        time.sleep(0.05)
    return "child"


@ch.remote
def spawner(marker):
    child = cooperative_nap.remote(30)
    with open(marker, "w") as f:
        f.write(child.id)
    return child.result()


def test_cancelling_a_call_cancels_the_calls_it_started():
    marker = os.path.join(tempfile.mkdtemp(), "child")
    parent = spawner.remote(marker)
    deadline = time.monotonic() + 30
    while not os.path.exists(marker) and time.monotonic() < deadline:
        time.sleep(0.05)
    child_id = open(marker).read()
    assert parent.cancel()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        child = {tid: err for tid, _, err in ch.runs.current_run().outcomes() if child_id.endswith(tid)}
        if child:
            break
        time.sleep(0.1)
    assert isinstance(next(iter(child.values())), ch.Cancelled)


# ---- runs -------------------------------------------------------------------------------


def test_init_returns_the_run_and_str_gives_its_id():
    run = ch.init()
    assert isinstance(run, ch.runs.Run) and run == ch.runs.current_run() and str(run) == run.id
    assert ch.runs.attach(run).status() == ch.runs.attach(str(run)).status()
    assert all(state in ("Pending", "Ready", "Running", "Done") for _, state in run.tasks())


def test_run_cancel_stops_its_unfinished_calls():
    proc = run_script(
        """
        import time, chimaera as ch

        @ch.remote
        def sleepy():
            while not ch.context().cancelled():
                time.sleep(0.05)

        run = ch.init()
        f = sleepy.remote()
        time.sleep(0.5)
        run.cancel()
        try:
            f.result(timeout=30)
        except ch.Cancelled:
            print("cancelled")
        """
    )
    assert proc.stdout.strip().endswith("cancelled"), proc.stderr


# ---- priority -----------------------------------------------------------------------------


@ch.remote
def stamp(tag, seconds):
    t = time.monotonic_ns()
    time.sleep(seconds)
    return tag, t


def test_higher_priority_starts_first_among_queued_calls():
    # Each call takes over half the CPUs, so they run one at a time in queue order.
    cores = ch._runtime.Runtime.get().info["usable_cores"]
    whole = stamp.options(cpus=cores / 2 + 0.5)
    blocker = whole.remote("blocker", 0.8)  # runs while the rest queue behind it
    time.sleep(0.2)
    low = [whole.options(priority=0).remote("low", 0.02) for _ in range(4)]
    high = [whole.options(priority=10).remote("high", 0.02) for _ in range(4)]
    blocker.result(timeout=60)
    lows = [f.result(timeout=60)[1] for f in low]
    highs = [f.result(timeout=60)[1] for f in high]
    assert max(highs) < min(lows), "a later high-priority call started after an earlier low one"


# ---- queues and topics ------------------------------------------------------------------------


@ch.remote
def produce(q, n):
    for i in range(n):
        q.put(i).result()
    return n


@ch.remote
def consume(q):
    return sum(q)


def test_queue_connects_producers_and_consumers_across_calls():
    q = ch.coord.Queue(maxsize=8)
    total = consume.remote(q)
    assert produce.remote(q, 100).result(timeout=60) == 100
    q.close()
    assert total.result(timeout=60) == sum(range(100))


def test_queue_bounds_timeouts_and_close():
    q = ch.coord.Queue(maxsize=1)
    q.put("a").result()
    with pytest.raises(queue.Full):
        q.put("b", timeout=0.2).result(timeout=30)
    assert q.qsize() == 1 and q.get().result() == "a"
    with pytest.raises(queue.Empty):
        q.get(timeout=0.2).result(timeout=30)
    q.put("c").result()
    q.close()
    assert list(q) == ["c"]
    with pytest.raises(ch.errors.Closed):
        q.put("d").result(timeout=30)


def test_keyed_queue_is_shared():
    ch.coord.Queue(name="tier1/jobs").put("job").result()
    assert ch.coord.Queue(name="tier1/jobs").get(timeout=10).result(timeout=30) == "job"


def test_topic_delivers_to_every_subscriber_from_its_start():
    topic = ch.coord.Topic(retain=3)
    early = topic.subscribe("earliest")
    offsets = [topic.publish(i).result() for i in range(5)]
    assert offsets == [0, 1, 2, 3, 4]
    late = topic.subscribe("earliest")
    topic.close()
    assert list(late) == [2, 3, 4] and late.skipped == 2
    assert list(early) == [2, 3, 4] and early.skipped == 2  # read nothing before the log trimmed


def test_a_cursor_resumes_where_it_acked_and_names_the_exact_gap():
    topic = ch.coord.Topic(retain=3)
    sub = topic.subscribe("earliest", cursor="reader")
    topic.publish("first").result()
    assert next(sub) == "first"
    sub.ack().result()
    sub.close()  # the reader goes away
    for i in range(10):  # offsets 1 to 10: the topic keeps 8, 9 and 10
        topic.publish(i).result()
    again = topic.subscribe("latest", cursor="reader")  # start counts only the first time
    assert again.offset == 1
    assert [next(again) for _ in range(3)] == [7, 8, 9]
    assert again.gaps == [ch.coord.Gap(first_missed=1, first_available=8)] and again.skipped == 7
    again.ack(99).result()  # past the end: up to the end
    topic.publish("next").result()
    assert next(topic.subscribe(cursor="reader")) == "next"
    fresh = topic.subscribe("latest", cursor="other")
    topic.publish("last").result()
    assert next(fresh) == "last" and not fresh.gaps
    with pytest.raises(ValueError, match="cursor"):
        topic.subscribe().ack()


def test_latest_starts_at_subscribe_not_at_the_first_read():
    topic = ch.coord.Topic()
    topic.publish("before").result()
    sub = topic.subscribe("latest")
    topic.publish("between").result()  # after subscribing, before any read
    got = []
    reader = threading.Thread(target=lambda: got.append(next(iter(sub))), daemon=True)
    reader.start()
    reader.join(timeout=20)
    assert got == ["between"], "an item published after subscribe() must not be skipped"


def test_topic_latest_sees_only_new_items_and_async_works():
    import asyncio

    topic = ch.coord.Topic()
    topic.publish("old").result()
    sub = topic.subscribe("latest")

    async def read_two():
        got = []
        async for item in sub:
            got.append(item)
            if len(got) == 2:
                return got

    def later():
        time.sleep(0.3)
        topic.publish("new1").result()
        topic.publish("new2").result()

    threading.Thread(target=later).start()
    assert asyncio.run(read_two()) == ["new1", "new2"]
