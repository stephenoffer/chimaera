"""The direct actor path: a local driver's calls to a one-at-a-time actor go
straight to its executor, and the peer records them afterwards."""

import os
import signal
import threading
import stat
import time

import pytest

import chimaera as ch
from chimaera import _direct
from chimaera._remote import _rebuild_actor


@ch.remote
class Log:
    def __init__(self):
        self.items = []

    def add(self, x):
        self.items.append(x)
        return len(self.items)

    def all(self):
        return list(self.items)

    def pid(self):
        return os.getpid()

    def fail(self):
        raise KeyError("no such entry")

    def big(self, n):
        return b"x" * n

    def emit(self, x):
        return x

    def consume(self, x):
        self.items.append(x)
        return len(x) if isinstance(x, bytes) else x

    def relay(self, x):
        return ch.remote(first_after).remote(x)  # a future: this call resolves to that call's outcome

    def slow_reader(self, seconds):
        _direct.READ_DELAY_S = seconds  # this runs in the actor's executor

    def wait(self, seconds):
        time.sleep(seconds)
        return seconds


def sent() -> int:
    return _direct._counter["direct"]


def ready(a) -> None:
    """Until the actor's executor listens (its constructor ran)."""
    a.all.remote().result(timeout=60)
    path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
    deadline = time.monotonic() + 10
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.01)


def test_calls_go_direct_in_order_and_the_peer_records_them():
    a = Log.remote()
    ready(a)
    before = sent()
    futures = [a.add.remote(i) for i in range(200)]
    assert [f.result(timeout=60) for f in futures] == list(range(1, 201))
    assert sent() - before == 200
    assert a.all.remote().result(timeout=60) == list(range(200))
    # The record lands a moment after the caller had its result.
    deadline = time.monotonic() + 10
    while not (e := futures[-1].explain())["attempts"] and time.monotonic() < deadline:
        time.sleep(0.01)
    assert e["state"] == "Done"
    (attempt,) = e["attempts"]
    assert attempt["placement"].startswith("direct") and attempt["measures"]["wall_seconds"][0] > 0


def test_a_burst_that_starts_before_the_actor_listens_keeps_its_order():
    # The first calls go through the peer (no socket yet), later ones direct:
    # none may overtake an earlier one.
    for _ in range(3):
        a = Log.remote()
        futures = [a.add.remote(i) for i in range(300)]
        assert [f.result(timeout=60) for f in futures] == list(range(1, 301))
        assert a.all.remote().result(timeout=60) == list(range(300))


def first_after(seconds):
    time.sleep(seconds)
    return "first"


def test_a_direct_call_waits_behind_a_call_still_at_the_peer():
    # The first call takes the peer path (a deadline) and waits there for its
    # argument; the second could go direct at once and must not run before it.
    a = Log.remote()
    ready(a)
    waiting = a.add.options(deadline="60s").remote(ch.remote(first_after).remote(0.5))
    after = a.add.remote("second")
    assert (waiting.result(timeout=60), after.result(timeout=60)) == (1, 2)
    # Also when the caller dropped the earlier call's future.
    a.add.options(deadline="60s").remote(ch.remote(first_after).remote(0.5))
    after = a.add.remote("fourth")
    assert after.result(timeout=60) == 4
    assert a.all.remote().result(timeout=60) == ["first", "second", "first", "fourth"]


def test_a_peer_call_waits_behind_direct_calls_still_on_their_way():
    # A slow reader in the executor: without waiting for the direct call, the
    # peer's copy of the next call would reach the actor first.
    a = Log.remote()
    ready(a)
    a.slow_reader.remote(0.3).result(timeout=60)
    direct = a.add.remote("direct")
    peer = a.add.options(deadline="60s").remote("peer")
    assert (direct.result(timeout=60), peer.result(timeout=60)) == (1, 2)
    a.slow_reader.remote(0).result(timeout=60)
    assert a.all.remote().result(timeout=60) == ["direct", "peer"]


def test_an_actor_call_taking_another_actors_result_goes_direct_too():
    prod, cons = Log.remote(), Log.remote()
    ready(prod)
    ready(cons)
    before = sent()
    out = cons.consume.remote(prod.emit.remote(41))
    assert out.result(timeout=60) == 41
    assert sent() - before == 2
    data = cons.consume.remote(prod.big.remote(3 << 20))
    assert data.result(timeout=60) == 3 << 20
    deadline = time.monotonic() + 10
    while not (e := out.explain())["attempts"] and time.monotonic() < deadline:
        time.sleep(0.01)
    assert e["state"] == "Done" and e["attempts"][0]["placement"].startswith("direct")


def test_a_call_waiting_for_its_input_keeps_its_place_on_the_handle():
    prod, cons = Log.remote(), Log.remote()
    ready(prod)
    ready(cons)
    slow = prod.wait.remote(0.5)
    first = cons.add.remote(slow)  # waits here for its input
    second = cons.add.remote("after")  # no input: must not overtake
    assert (first.result(timeout=60), second.result(timeout=60)) == (1, 2)
    assert cons.all.remote().result(timeout=60) == [0.5, "after"]


def test_a_failed_input_fails_the_call_that_takes_it():
    prod, cons = Log.remote(), Log.remote()
    ready(prod)
    ready(cons)
    out = cons.consume.remote(prod.fail.remote())
    with pytest.raises(KeyError, match="no such entry") as caught:
        out.result(timeout=60)
    assert any("prerequisite" in n for n in getattr(caught.value, "__notes__", []))
    assert cons.all.remote().result(timeout=60) == [], "it never ran"


def test_failed_inputs_between_direct_calls_never_stall_the_handle():
    # A call whose input failed goes through the peer; the direct calls around
    # it do not. The shard fails it at submission (its input's record is
    # already there), so it never waits in the actor's mailbox for the
    # handle's direct sequence, which the peer never sees in full.
    prod, cons = Log.remote(), Log.remote()
    ready(prod)
    ready(cons)
    outs = [cons.consume.remote(prod.fail.remote() if i % 2 == 0 else prod.emit.remote(i)) for i in range(6)]
    for i, out in enumerate(outs):
        if i % 2 == 0:
            with pytest.raises(KeyError):
                out.result(timeout=60)
        else:
            assert out.result(timeout=60) == i
    assert cons.all.remote().result(timeout=60) == [1, 3, 5]


def test_a_call_cancelled_while_waiting_for_its_input_never_runs():
    prod, cons = Log.remote(), Log.remote()
    ready(prod)
    ready(cons)
    waiting = cons.consume.remote(prod.wait.remote(0.5))
    assert waiting.cancel()
    with pytest.raises(ch.Cancelled):
        waiting.result(timeout=60)
    assert cons.add.remote("next").result(timeout=60) == 1
    assert cons.all.remote().result(timeout=60) == ["next"]


def test_a_method_returning_a_future_is_recorded_with_that_outcome():
    a = Log.remote()
    ready(a)
    out = a.relay.remote(0.0)
    assert out.result(timeout=60) == "first"
    deadline = time.monotonic() + 10
    while (e := out.explain())["state"] != "Done" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert e["state"] == "Done"


class _Seen:
    """The runtime's client, noting which calls the peer sent an outcome event for."""

    def __init__(self, client):
        self._client, self.outcomes = client, set()

    def next_events(self, *args):
        events = self._client.next_events(*args)
        self.outcomes.update(ev[1] for ev in events or () if ev[0] == "outcome")
        return events

    def __getattr__(self, name):
        return getattr(self._client, name)


def test_the_peer_sends_no_outcome_for_a_direct_call_its_caller_has():
    # The record of a direct call reaches the peer after its caller had the
    # result: sending that outcome back cost the peer a read and an event a call.
    rt = ch.runs.current_run()._rt
    a = Log.remote()
    ready(a)
    client, rt.client = rt.client, _Seen(rt.client)
    try:
        before = sent()
        futures = [a.add.remote(i) for i in range(100)]
        assert [f.result(timeout=60) for f in futures] == list(range(1, 101))
        assert sent() - before == 100
        deadline = time.monotonic() + 10
        while futures[-1].explain()["state"] != "Done" and time.monotonic() < deadline:
            time.sleep(0.01)
        # A later call's outcome event comes after any the records' watches sent.
        marker = ch.remote(first_after).remote(0.2)
        assert marker.result(timeout=60) == "first"
        deadline = time.monotonic() + 10
        while marker.key not in rt.client.outcomes and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.key in rt.client.outcomes
        assert not {f.key for f in futures} & rt.client.outcomes
    finally:
        rt.client = client


def test_a_handle_switching_paths_keeps_its_order():
    a = Log.remote()
    ready(a)
    out = []
    for i in range(60):
        if i % 7 == 3:  # a deadline keeps a call on the peer path
            out.append(a.add.options(deadline="60s").remote(i))
        else:
            out.append(a.add.remote(i))
    assert [f.result(timeout=60) for f in out] == list(range(1, 61))
    assert a.all.remote().result(timeout=60) == list(range(60))


def test_errors_and_large_results_come_back_as_through_the_peer():
    a = Log.remote()
    ready(a)
    before = sent()
    with pytest.raises(KeyError, match="no such entry") as caught:
        a.fail.remote().result(timeout=60)
    assert any("remote traceback" in n for n in getattr(caught.value, "__notes__", []))
    data = a.big.remote(5 << 20).result(timeout=60)
    assert len(data) == 5 << 20 and data[:3] == b"xxx"
    assert sent() - before == 2


def test_a_direct_result_passed_to_another_call_arrives():
    a = Log.remote()
    ready(a)
    n = a.add.remote(5)
    assert ch.remote(lambda x: x * 10).remote(n).result(timeout=60) == 10
    big = a.big.remote(3 << 20)
    assert ch.remote(len).remote(big).result(timeout=60) == 3 << 20


def test_calls_that_must_take_the_peer_never_go_direct():
    plain = Log.remote()
    ready(plain)
    before = sent()
    assert plain.add.options(deadline="60s").remote(1).result(timeout=60) == 1
    assert plain.add.options(effects="external", max_attempts=1).remote(2).result(timeout=60) == 2
    assert plain.add.options(retry_exceptions=(ValueError,)).remote(3).result(timeout=60) == 3
    assert sent() == before


@ch.remote(concurrency=4)
class Wide:
    def all(self):
        return []

    def nap(self, seconds):
        time.sleep(seconds)
        return threading.get_ident()


def test_a_concurrent_actor_goes_direct_and_keeps_its_concurrency():
    a = Wide.remote()
    ready(a)
    # A handle rebuilt from a pickle carries its class but no options: the
    # actor's own concurrency still applies.
    for h in (a, _rebuild_actor(a._actor, a._ctor, a._cls, a._digest)):
        before, start = sent(), time.monotonic()
        threads = ch.get([h.nap.remote(0.5) for _ in range(4)], timeout=60)
        assert time.monotonic() - start < 1.6, "four calls at once, not one after another"
        assert len(set(threads)) == 4
        assert sent() - before == 4


def test_explain_shows_a_direct_call_running_or_queued_at_its_actor():
    a = Log.remote()
    ready(a)
    slow = a.wait.remote(1.0)
    queued = [a.add.remote(i) for i in range(3)]
    time.sleep(0.3)
    e = queued[-1].explain()
    assert e["state"] == "Running"
    assert "queued at its actor behind 3 call(s)" in e["pending_reason"], e["pending_reason"]
    assert "running at its actor" in slow.explain()["pending_reason"]
    ch.get(queued, timeout=60)


def test_a_concurrent_actor_never_holds_a_call_back_for_the_order():
    # Concurrent actors promise no order, so a call through the peer (a
    # deadline) does not wait for the handle's direct calls in flight.
    a = Wide.remote()
    ready(a)
    running = [a.nap.remote(1.0) for _ in range(4)]
    start = time.monotonic()
    other = a.nap.options(deadline="60s").remote(0)
    assert time.monotonic() - start < 0.5, ".remote() waited for the direct calls"
    ch.get([*running, other], timeout=60)


def test_a_queued_direct_call_cancelled_never_runs():
    a = Log.remote()
    ready(a)
    before = sent()
    slow = a.wait.remote(1.0)
    queued = a.add.remote("never")
    assert queued.cancel()
    assert slow.result(timeout=60) == 1.0
    with pytest.raises(ch.Cancelled):
        queued.result(timeout=60)
    assert "never" not in a.all.remote().result(timeout=60)
    assert sent() - before >= 2


def test_calls_waiting_when_the_executor_dies_finish_through_the_peer():
    a = Log.options(restart="fresh", max_restarts=3).remote()
    ready(a)
    pid = a.pid.remote().result(timeout=60)
    slow = a.wait.remote(0.5)
    after = [a.add.remote(i) for i in range(20)]
    time.sleep(0.2)
    os.kill(pid, signal.SIGKILL)
    _, pending = ch.wait([slow, *after], timeout=90)
    assert not pending, f"{len(pending)} calls never finished"
    for f in [slow, *after]:
        try:
            f.result()
        except ch.ActorDied:
            pass  # a call running when the worker died may fail; it must not hang
    # The restarted actor serves calls again.
    assert a.pid.remote().result(timeout=60) != pid


@pytest.mark.parametrize("path", ["direct", "peer"])
def test_a_call_running_when_its_actor_is_closed_raises_actor_died(path):
    a = Log.remote()
    ready(a)
    method = a.wait if path == "direct" else a.wait.options(deadline="60s")
    running = method.remote(5.0)
    time.sleep(0.5)
    a.close()
    with pytest.raises(ch.ActorDied):
        running.result(timeout=60)


def test_a_call_through_a_closed_handle_raises_actor_died_at_once():
    # The peer ends the actor a moment after close(); a call made meanwhile
    # must not run (it did, about half the time, on the direct path).
    for _ in range(10):
        a = Log.remote()
        ready(a)
        a.close()
        with pytest.raises(ch.ActorDied):
            a.add.remote(1).result(timeout=30)


def calls_from_a_task(a, n):
    """Inside a call: n calls on `a`, in order; how many went direct from here."""
    before = _direct._counter["direct"]
    futures = [a.add.remote(i) for i in range(n)]
    return [f.result() for f in futures], _direct._counter["direct"] - before


def test_calls_made_inside_a_task_go_direct_and_keep_their_order():
    a = Log.remote()
    ready(a)
    values, direct = ch.remote(calls_from_a_task).remote(a, 50).result(timeout=60)
    assert values == list(range(1, 51))
    assert direct == 50, "a task on this machine calls the actor directly"
    assert a.all.remote().result(timeout=60) == list(range(50))


@ch.remote
class Caller:
    def __init__(self, other):
        self.other = other

    def relay(self, x):
        return self.other.consume.remote(x).result(), _direct._counter["direct"]


def test_an_actor_calls_another_actor_directly():
    target = Log.remote()
    ready(target)
    caller = Caller.remote(target)
    value, direct = caller.relay.remote(7).result(timeout=60)
    assert value == 7 and direct >= 1


def test_the_socket_is_private_to_the_peers_user():
    a = Log.remote()
    ready(a)
    path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode & 0o077 == 0, oct(mode)
