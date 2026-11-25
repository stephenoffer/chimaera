"""ch.coord.Store: versions and compare-and-set, counters, locks with fencing,
barriers, watches and TTLs; and fencing across a leader failover."""

from __future__ import annotations

import asyncio
import contextlib
import signal
import threading
import time
import uuid

import pytest

import chimaera as ch


def ns() -> ch.coord.Store:
    return ch.coord.Store(f"test-{uuid.uuid4().hex[:8]}")


def test_versions_make_compare_and_set():
    s = ns()
    v1 = s.put("cfg", {"lr": 3e-4}, if_version=0)
    assert s.get("cfg") == {"lr": 3e-4} and s.version("cfg") == v1
    with pytest.raises(ch.errors.VersionConflict) as info:
        s.put("cfg", "other", if_version=0)
    assert info.value.version == v1
    v2 = s.put("cfg", {"lr": 1e-4}, if_version=v1)
    assert v2 > v1
    with pytest.raises(ch.errors.VersionConflict):
        s.delete("cfg", if_version=v1)
    assert s.delete("cfg") and not s.delete("cfg")
    assert s.get("cfg", "gone") == "gone" and s.version("cfg") == 0
    with pytest.raises(ValueError, match="1 MiB"):
        s.put("big", b"x" * (2 << 20))


def test_get_versioned_reads_a_value_and_its_version_together():
    s = ns()
    assert s.get_versioned("n", 0) == (0, 0)
    s.put("n", 1)
    n, version = s.get_versioned("n")
    s.put("n", 10)  # another writer lands between the read and the write
    with pytest.raises(ch.errors.VersionConflict):
        s.put("n", n + 1, if_version=version)
    n, version = s.get_versioned("n")
    s.put("n", n + 1, if_version=version)
    assert s.get("n") == 11


def test_scan_returns_keys_under_a_prefix_in_order():
    s = ns()
    for k in ["b/2", "a/1", "b/1", "c"]:
        s.put(k, k.upper())
    assert list(s.scan("b/")) == [("b/1", "B/1"), ("b/2", "B/2")]
    assert [k for k, _ in s.scan()] == ["a/1", "b/1", "b/2", "c"]


@ch.remote
def bump(store, n):
    c = store.counter("hits")
    for _ in range(n):
        c.add(1)


def test_counters_are_atomic_across_calls():
    s = ns()
    ch.get((bump.remote(s, 25) for _ in range(8)), timeout=120)
    assert s.counter("hits").value() == 200


@ch.remote
def locked_increment(store, k):
    for _ in range(k):
        with store.lock("mutex", ttl="10s"):
            n = store.get("n", 0)
            time.sleep(0.002)  # widen the race a lock must close
            store.put("n", n + 1)


def test_a_lock_excludes_other_holders():
    s = ns()
    ch.get((locked_increment.remote(s, 10) for _ in range(6)), timeout=180)
    assert s.get("n") == 60, "a lost update means two holders ran at once"


def test_a_stale_holder_is_fenced_after_its_lease_expires():
    s = ns()
    a = s.lock("leader", ttl="400ms").acquire()
    s.put("state", "from a", fence=a)
    a._stop.set()  # a stalls: no more renewals
    b = s.lock("leader", ttl="10s", timeout=10).acquire()
    try:
        assert b.token > a.token and s.holder("leader") == b.holder
        with pytest.raises(ch.errors.LeaseLost):
            s.put("state", "late write from a", fence=a)
        s.put("state", "from b", fence=b)
        assert s.get("state") == "from b"
    finally:
        b.release()
    assert s.holder("leader") is None


@ch.remote
def meet(store, parties):
    return store.barrier("round", parties).wait(timeout=60)


def test_a_barrier_releases_everyone_together():
    s = ns()
    assert ch.get((meet.remote(s, 3) for _ in range(3)), timeout=120) == [1, 1, 1]
    assert ch.get((meet.remote(s, 3) for _ in range(3)), timeout=120) == [2, 2, 2]


def test_watch_delivers_changes_in_order():
    s = ns()
    s.put("jobs/old", 0)
    w = s.watch("jobs/")
    s.put("jobs/a", 1)
    s.put("other", "ignored")
    s.counter("jobs/count").add(5)
    s.delete("jobs/a")
    got = [next(w) for _ in range(3)]
    assert [(e.kind, e.key, e.value) for e in got] == [
        ("put", "jobs/a", 1),
        ("put", "jobs/count", 5),
        ("delete", "jobs/a", None),
    ]
    assert got[0].version < got[1].version < got[2].version


def test_a_watch_events_and_subscriptions_all_start_with_start():
    s = ns()
    s.put("jobs/old", 0)
    first = s.put("jobs/a", 1)
    assert next(s.watch("jobs/", start="earliest")).key == "jobs/old", "everything the namespace still holds"
    s.put("jobs/b", 2)
    assert next(s.watch("jobs/", start=first)).key == "jobs/b", "after a revision"
    for bad in ("soon", -1, True):
        with pytest.raises(ValueError, match="start: expected 'latest', 'earliest' or a position"):
            s.watch(start=bad)
        with pytest.raises(ValueError, match="start: expected"):
            ch.runs.cluster().events(start=bad)


def test_async_watch_wakes_on_a_change():
    s = ns()
    w = s.watch("k")

    async def first():
        async for e in w:
            return e

    threading.Timer(0.3, lambda: s.put("k", "v")).start()
    e = asyncio.run(asyncio.wait_for(first(), 30))
    assert (e.kind, e.key, e.value) == ("put", "k", "v")


def test_ttl_entries_expire():
    s = ns()
    s.put("session", "alive", ttl="300ms")
    assert s.get("session") == "alive"
    time.sleep(0.6)
    assert s.get("session") is None


# ---- consistent scans -------------------------------------------------------------------


def load(s, keys, value=0):
    """Put `value` at every key, 60 keys to a command."""
    from chimaera._store import _dumps

    for i in range(0, len(keys), 60):
        assert s._txn([], [("put", k, _dumps(value), 0) for k in keys[i : i + 60]])["ok"]


def follow(s, prefix, start, view):
    """Apply a watch from `start` to `view` up to a marker key put now; how it began."""
    end = f"{prefix}~end"
    w = s.watch(prefix, start=start)
    s.put(end, None)
    kinds = []
    for e in w:
        kinds.append(e.kind)
        if e.kind == "resync":
            view.clear()
        elif e.kind == "put":
            view[e.key] = e.value
        else:
            view.pop(e.key, None)
        if e.key == end:
            view.pop(end)
            return kinds
    raise AssertionError("the watch ended")


def test_a_scan_plus_a_watch_from_its_revision_is_coherent_under_concurrent_writes():
    import random

    s = ns()
    keys = [f"k/{i:05d}" for i in range(2500)]  # three pages
    load(s, keys)
    stop, wrote = threading.Event(), []

    def writer():
        # Paced to span the scan, and fewer writes than the change log keeps
        # (4096), so the watch replays them however slow the scan's pages are;
        # the next test covers a watch that falls behind the log.
        rng = random.Random(7)
        while not stop.is_set() and len(wrote) < 2000:
            time.sleep(0.001)
            k = rng.choice(keys)
            if rng.random() < 0.3:
                s.delete(k)
            else:
                s.put(k, len(wrote))
            wrote.append(k)

    t = threading.Thread(target=writer)
    t.start()
    try:
        scan = s.scan("k/")
        view = {}
        for i, (k, v) in enumerate(scan):
            view[k] = v
            if i % 400 == 0:
                time.sleep(0.2)  # writes land between and inside pages
    finally:
        stop.set()
        t.join()
    assert len(wrote) > 20
    stale = dict(view)
    assert follow(s, "k/", scan.revision, view)[0] != "resync"
    assert view == {k: v for k, v in s.scan("k/") if k != "k/~end"}
    assert view != stale, "the writes during the scan are applied by the watch"


def test_a_watch_that_fell_behind_a_long_scan_resyncs_to_a_coherent_map():
    s = ns()
    keys = [f"k/{i:05d}" for i in range(2500)]
    load(s, keys)
    scan = s.scan("k/")
    view = dict(next(scan) for _ in range(1200))  # into its second page
    # Deletes on both sides of the scan's cursor, then enough changes to trim the log past the scan.
    gone = keys[:100] + keys[2000:2100]
    for i in range(0, len(gone), 60):
        assert s._txn([], [("delete", k, None, 0) for k in gone[i : i + 60]])["ok"]
    for n in range(70):
        load(s, keys[2200:2260], n)
    view.update(scan)
    assert set(gone[:100]) <= view.keys()  # read before they went
    kinds = follow(s, "k/", scan.revision, view)
    assert kinds[0] == "resync", "the watch says it fell behind the change log"
    assert view == {k: v for k, v in s.scan("k/") if k != "k/~end"}
    assert not set(gone) & view.keys()
    assert all(view[k] == 69 for k in keys[2200:2260])


# ---- fencing across a leader failover ---------------------------------------------------

FAILOVER_A = """
import time, chimaera as ch
s = ch.coord.Store("failover")
lease = s.lock("leader", ttl="1500ms").acquire()
s.put("state", "a", fence=lease)
print("TOKEN", lease.token, flush=True)
lease._stop.set()                      # a stalls: its lease will expire
while not __import__("os").path.exists(PROCEED):
    time.sleep(0.1)
try:
    s.put("state", "late a", fence=lease)
    print("FENCED no")
except ch.errors.LeaseLost:
    print("FENCED yes")
"""


def test_fencing_holds_across_a_leader_failover():
    from test_cluster import Cluster

    c = Cluster(3, data_groups=1)
    try:
        import os
        import tempfile

        proceed = os.path.join(tempfile.mkdtemp(), "proceed")
        a = c.driver(0, FAILOVER_A.replace("PROCEED", repr(proceed)))
        token_a = None
        deadline = time.monotonic() + 60
        while token_a is None and time.monotonic() < deadline:
            line = a.stdout.readline()
            if line.startswith("TOKEN"):
                token_a = int(line.split()[1])
        assert token_a is not None, a.stderr.read() if a.poll() is not None else "no token"
        # Kill and restart every peer in turn (peer 0 last, since a's driver uses it):
        # whichever one led the namespace's group has failed over by the end.
        for i in (1, 2, 0):
            c.signal(i, signal.SIGKILL)
            c.start(i)
            c.wait_ready([i])
            time.sleep(1.0)
        out = c.run(
            2,
            """
import chimaera as ch
s = ch.coord.Store("failover")
b = s.lock("leader", ttl="30s", timeout=60).acquire()
s.put("state", "b", fence=b)
print("B", b.token, s.get("state"))
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        token_b = int(out.stdout.split("B ", 1)[1].split()[0])
        assert token_b > token_a
        open(proceed, "w").close()
        stdout, stderr = a.communicate(timeout=120)
        assert "FENCED yes" in stdout, stdout + stderr[-3000:]
    finally:
        c.close()


# ---- durable queues ---------------------------------------------------------------------


def qkey() -> str:
    return f"q-{uuid.uuid4().hex[:8]}"


def test_a_work_queue_is_fifo_bounded_and_synchronous():
    import queue

    q = ch.coord.WorkQueue(qkey(), maxsize=3)
    assert not isinstance(q, ch.coord.Queue)
    for i in range(3):
        assert q.put(i) is None  # stored when it returns
    with pytest.raises(queue.Full):
        q.put(99, timeout=0.2)
    assert [q.get() for _ in range(3)] == [0, 1, 2] and q.qsize() == 0
    with pytest.raises(queue.Empty):
        q.get(timeout=0.2)
    q.put("last")
    q.close()
    assert list(q) == ["last"]
    with pytest.raises(ch.errors.Closed):
        q.put("after close")


def test_a_failed_claim_is_delivered_again():
    q = ch.coord.WorkQueue(qkey())
    q.put("job")
    with pytest.raises(RuntimeError):
        with q.claim() as item:
            assert item == "job"
            raise RuntimeError("consumer failed")
    with q.claim() as item:
        assert item == "job"
    assert q.qsize() == 0


@ch.remote(max_attempts=1)
def die_holding_a_claim(q):
    import os

    with q.claim(lease="500ms"):
        os._exit(1)  # the consumer dies mid-item; its lease lapses


def test_an_item_whose_consumer_died_is_delivered_again():
    q = ch.coord.WorkQueue(qkey())
    q.put("job")
    with pytest.raises(ch.InfrastructureFailure):
        die_holding_a_claim.remote(q).result(timeout=60)
    assert q.get(timeout=10) == "job"


@ch.remote
def consume_all(q):
    return sorted(q)


def test_a_work_queue_is_shared_by_name_across_calls():
    key = qkey()
    q = ch.coord.WorkQueue(key)
    for i in range(20):
        q.put(i)
    q.close()
    parts = ch.get((consume_all.remote(ch.coord.WorkQueue(key)) for _ in range(3)), timeout=120)
    assert sorted(x for p in parts for x in p) == list(range(20))


class AfterCalls:
    """The runtime's client, running `then` right after this thread's `n`-th
    store command: another writer, or the process's death, lands exactly there."""

    def __init__(self, client, n, then):
        self._client, self._left, self._then = client, n, then
        self._thread = threading.get_ident()

    def __getattr__(self, name):
        f = getattr(self._client, name)
        if not name.startswith("store_"):
            return f

        def call(*args, **kwargs):
            r = f(*args, **kwargs)
            if threading.get_ident() == self._thread and self._left > 0:
                self._left -= 1
                if self._left == 0:
                    self._then()
            return r

        return call


@contextlib.contextmanager
def interleave(monkeypatch, n, then):
    """Run `then` after the n-th store command in the block (or after the block, if fewer)."""
    from chimaera._runtime import Runtime

    rt, ran = Runtime.get(), []
    client = rt.client
    monkeypatch.setattr(rt, "client", AfterCalls(client, n, lambda: ran.append(then())))
    yield
    monkeypatch.setattr(rt, "client", client)
    if not ran:
        then()


def depth(q):
    """(qsize, items stored): equal whenever no command is half done."""
    return q.qsize(), len(list(q._store.keys("item/")))


@pytest.mark.parametrize("n", range(1, 8))
def test_puts_interleaved_at_any_step_count_every_item(monkeypatch, n):
    q = ch.coord.WorkQueue(qkey(), maxsize=10)
    with interleave(monkeypatch, n, lambda: q.put("b")):
        q.put("a")
    assert depth(q) == (2, 2)
    assert sorted([q.get(), q.get()]) == ["a", "b"]


@pytest.mark.parametrize("n", range(1, 8))
def test_no_item_lands_after_a_close_interleaved_at_any_step_of_a_put(monkeypatch, n):
    q, at_close = ch.coord.WorkQueue(qkey(), maxsize=10), []

    def close():
        q.close()
        at_close.append(depth(q))

    stored = True
    with interleave(monkeypatch, n, close):
        try:
            q.put("a")
        except ch.errors.Closed:
            stored = False
    assert depth(q) == ((1, 1) if stored else (0, 0))
    assert at_close == [depth(q)], "an item landed after close() returned"
    assert list(q) == (["a"] if stored else [])


@pytest.mark.parametrize("n", range(1, 6))
def test_completions_interleaved_at_any_step_count_once_each(monkeypatch, n):
    q = ch.coord.WorkQueue(qkey(), maxsize=5)
    for i in range(5):
        q.put(i)
    first, second = q.claim(), q.claim()
    assert (first.__enter__(), second.__enter__()) == (0, 1)
    with interleave(monkeypatch, n, lambda: second.__exit__(None, None, None)):
        first.__exit__(None, None, None)
    assert depth(q) == (3, 3)
    q.put(5)
    q.put(6)  # both completions freed their room


def test_a_queue_an_earlier_version_stored_keeps_its_depth():
    import queue

    def stored_by_an_earlier_version(items):
        """A queue in the old layout: its depth as a value at "size", not a counter."""
        name = qkey()
        s = ch.coord.Store(f"queue/{name}")
        s.put("size", len(items))
        for i, item in enumerate(items, 1):
            s.counter("tail").add(1)
            s.put(f"item/{i:020d}", item)
        return ch.coord.WorkQueue(name, maxsize=len(items)), s

    q, s = stored_by_an_earlier_version(["a", "b"])
    assert q.qsize() == 2
    with pytest.raises(queue.Full):
        q.put("c", timeout=0.3)  # its first transaction moves the depth to "count"
    assert s.version("size") == 0 and q.qsize() == 2
    assert q.get() == "a"
    q.put("c")
    with pytest.raises(queue.Full):
        q.put("d", timeout=0.3)
    assert [q.get(), q.get()] == ["b", "c"] and depth(q) == (0, 0)
    q, s = stored_by_an_earlier_version(["a", "b"])
    assert q.get() == "a"  # a completion moves it just the same
    assert s.version("size") == 0 and depth(q) == (1, 1)


KILLED = """
import os, signal, threading
import chimaera as ch
from chimaera._runtime import Runtime

{after_calls}
q = ch.coord.WorkQueue({name!r}, maxsize=8)
q.qsize()  # connected
rt, kill = Runtime.get(), lambda: os.kill(os.getpid(), signal.SIGKILL)
{body}
"""


def run_killed(name, n, body):
    """Run `body` in a process killed right after its n-th store command; True if it finished first."""
    import inspect

    from conftest import run_script

    code = KILLED.format(after_calls=inspect.getsource(AfterCalls), name=name, body=body.format(n=n))
    out = run_script(code)
    assert out.returncode in (0, -signal.SIGKILL), out.stderr[-3000:]
    return out.returncode == 0


def test_a_producer_killed_after_any_step_leaves_whole_items():
    name = qkey()
    q = ch.coord.WorkQueue(name)
    body = """
rt.client = AfterCalls(rt.client, {n}, kill)
q.put("item")
"""
    for n in range(1, 10):
        finished = run_killed(name, n, body)
        assert depth(q)[0] == depth(q)[1], f"killed after store command {n}: {depth(q)}"
        if finished:
            break
    else:
        pytest.fail("the put never finished")


def test_a_consumer_killed_after_any_step_of_completing_counts_its_item_once():
    name = qkey()
    q = ch.coord.WorkQueue(name)
    body = """
with q.claim(lease="1s"):
    if {n} == 0:
        kill()  # dies holding the claim
    rt.client = AfterCalls(rt.client, {n}, kill)
"""
    for n in range(0, 10):
        q.put(f"job {n}")
        finished = run_killed(name, n, body)
        assert depth(q)[0] == depth(q)[1], f"killed after store command {n}: {depth(q)}"
        if depth(q) == (1, 1):
            assert q.get(timeout=30) == f"job {n}"  # delivered again once the lease lapses
        assert depth(q) == (0, 0)
        if finished:
            break
    else:
        pytest.fail("the completion never finished")


def test_a_lapsed_claim_completed_after_its_redelivery_is_refused():
    import queue

    q = ch.coord.WorkQueue(qkey(), maxsize=2)
    q.put("a")
    q.put("b")
    stale = q.claim(lease="500ms")
    assert stale.__enter__() == "a"
    stale.lease._stop.set()  # the consumer stalls: its lease is not renewed
    deadline = time.monotonic() + 30
    while q._store.holder(f"claim/{stale.key}") is not None and time.monotonic() < deadline:
        time.sleep(0.05)
    with q.claim(timeout=30) as again:
        assert again == "a"  # delivered again
    with pytest.raises(ch.errors.LeaseLost):
        stale.__exit__(None, None, None)  # completes the item a second time
    assert depth(q) == (1, 1)
    q.put("c")  # exactly one slot came free
    with pytest.raises(queue.Full):
        q.put("d", timeout=0.2)
    assert [q.get(), q.get()] == ["b", "c"]


@ch.remote(max_attempts=1)
def claim_then_die(q, signals):
    import os

    with q.claim(lease="1s"):
        signals.put("claimed", True)
        while signals.get("die") is None:
            time.sleep(0.05)
        os._exit(1)


def test_a_closed_queue_delivers_a_dead_consumers_item_before_it_reports_closed():
    q, signals = ch.coord.WorkQueue(qkey()), ns()
    q.put("a")
    q.put("b")
    dying = claim_then_die.remote(q, signals)
    deadline = time.monotonic() + 60
    while signals.get("claimed") is None and time.monotonic() < deadline:
        time.sleep(0.05)
    drained: list[str] = []
    with q.claim() as item:  # every item is now claimed
        assert item == "b"
        q.close()
        consumer = threading.Thread(target=lambda: drained.extend(q))
        consumer.start()
        time.sleep(0.5)
        assert consumer.is_alive(), "Closed while claimed items remain"
    signals.put("die", True)
    with pytest.raises(ch.InfrastructureFailure):
        dying.result(timeout=60)
    consumer.join(timeout=60)
    assert drained == ["a"] and depth(q) == (0, 0)


def drain_with(q, consumers):
    """Every item of the closed queue `q`, taken by `consumers` threads; and
    how many keys each claim looked at."""
    got, examined = [], []

    def consume():
        while True:
            c = q.claim()
            try:
                with c as item:
                    got.append(item)
                    examined.append(c._examined)
            except ch.errors.Closed:
                return

    threads = [threading.Thread(target=consume) for _ in range(consumers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return got, examined


def claims_look_at_a_few_keys(consumers, items):
    q = ch.coord.WorkQueue(qkey())
    for i in range(items):
        q.put(i)
    q.close()
    got, examined = drain_with(q, consumers)
    assert sorted(got) == list(range(items)), "each item once"
    assert max(examined) <= 2, f"a claim looked at {max(examined)} keys"
    assert depth(q) == (0, 0)


def test_claims_look_at_two_keys_with_20_consumers_on_10k_items():
    claims_look_at_a_few_keys(20, 10_000)


@pytest.mark.slow
@pytest.mark.timeout(3600)
def test_claims_look_at_two_keys_with_100_consumers_on_100k_items():
    claims_look_at_a_few_keys(100, 100_000)


class Counting:
    """The runtime's client, counting the store commands sent through it."""

    def __init__(self, client):
        self._client, self.sent = client, 0

    def __getattr__(self, name):
        f = getattr(self._client, name)
        if not name.startswith("store_"):
            return f

        def call(*args, **kwargs):
            self.sent += 1
            return f(*args, **kwargs)

        return call


def test_a_waiting_consumer_sleeps_until_the_queue_changes(monkeypatch):
    from chimaera._runtime import Runtime

    q, got = ch.coord.WorkQueue(qkey()), []
    rt = Runtime.get()
    counting = Counting(rt.client)
    monkeypatch.setattr(rt, "client", counting)
    consumer = threading.Thread(target=lambda: got.append(q.get(timeout=60)))
    consumer.start()
    time.sleep(2)
    assert counting.sent <= 4, f"{counting.sent} store commands while waiting on an empty queue"
    put_at = time.monotonic()
    q.put("job")
    consumer.join(timeout=30)
    assert got == ["job"] and time.monotonic() - put_at < 5, "the put wakes it"


def test_an_item_given_back_wakes_a_waiting_consumer():
    q, got = ch.coord.WorkQueue(qkey()), []
    q.put("job")
    first = q.claim(lease="60s")
    assert first.__enter__() == "job"
    waiter = threading.Thread(target=lambda: got.append(q.get(timeout=60)))
    waiter.start()
    time.sleep(0.5)
    back_at = time.monotonic()
    first.__exit__(RuntimeError, None, None)  # its block raised
    waiter.join(timeout=60)
    assert got == ["job"] and time.monotonic() - back_at < 5, "woken by the give-back, not the lease"


def test_a_work_queue_keeps_its_maxsize_for_every_client():
    import queue

    from conftest import run_script

    name = qkey()
    early = ch.coord.WorkQueue(name)  # opened before anyone gave a maxsize
    q = ch.coord.WorkQueue(name, maxsize=2)
    with pytest.raises(ValueError, match="maxsize=2"):
        ch.coord.WorkQueue(name, maxsize=5)
    assert ch.coord.WorkQueue(name, maxsize=2)._maxsize == 2
    early.put(1)
    ch.coord.WorkQueue(name).put(2)  # no maxsize: the stored one
    with pytest.raises(queue.Full):
        early.put(3, timeout=0.2)
    out = run_script(
        f"""
import queue
import chimaera as ch

q = ch.coord.WorkQueue({name!r})
try:
    q.put(3, timeout=0.2)
except queue.Full:
    print("full")
"""
    )
    assert out.stdout.strip() == "full", out.stderr[-3000:]
    assert q.get() == 1
    q.put(3)
    assert q.qsize() == 2


def test_cluster_reports_this_peer():
    nodes = ch.runs.cluster().nodes()
    assert len(nodes) == 1 and nodes[0].alive and nodes[0].this_peer
    assert ch.runs.cluster().resources()["cpus"] == nodes[0].cpus > 0

    @ch.remote
    def where():
        return ch.context().node

    assert where.remote().result() == nodes[0].peer
