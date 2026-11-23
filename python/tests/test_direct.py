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
    path = _direct.socket_path(ch.runs.current_run()._rt.home, a._actor)
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
    # The first call waits at the peer for its argument; the second could go
    # direct at once and must not run before it.
    a = Log.remote()
    ready(a)
    waiting = a.add.remote(ch.remote(first_after).remote(0.5))
    after = a.add.remote("second")
    assert (waiting.result(timeout=60), after.result(timeout=60)) == (1, 2)
    # Also when the caller dropped the earlier call's future.
    a.add.remote(ch.remote(first_after).remote(0.5))
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
    # A handle found by key carries no options: the actor's own concurrency
    # still applies.
    for h in (a, _rebuild_actor(a._actor, a._ctor, None, a._digest)):  # as found by key: no options
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


def test_the_socket_is_private_to_the_peers_user():
    a = Log.remote()
    ready(a)
    path = _direct.socket_path(ch.runs.current_run()._rt.home, a._actor)
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode & 0o077 == 0, oct(mode)
