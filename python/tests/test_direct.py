"""The direct actor path: a local driver's calls to a one-at-a-time actor go
straight to its executor, and the peer records them afterwards."""

import asyncio
import glob
import mmap
import os
import pickle
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import stat
import struct
import time

import pytest

import chimaera as ch
from chimaera import _direct
from chimaera._remote import _rebuild_actor
from chimaera._runtime import peer_socket
from conftest import BIN, REPO
from test_cluster import WHERE, Cluster
from test_recovery import _running, _start_peer


@ch.remote
class Log:
    def __init__(self):
        self.items = []

    def add(self, x):
        self.items.append(x)
        return len(self.items)

    @ch.method(deterministic=False)
    def draw(self, x):
        self.items.append(x)
        return len(self.items)

    @ch.method(effects="external")
    def charge(self, x):
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

    def add_then_wait(self, x, marker, seconds):
        """Change the state, say so (a file), then take `seconds` to return."""
        self.items.append(x)
        open(marker, "w").close()
        time.sleep(seconds)
        return len(self.items)

    def count_then_wait(self, tag, seconds):
        return _count_then_wait(tag, seconds)

    def forget_reclaims(self, on):
        _direct.TEST_FORGET_RECLAIMS = on  # this runs in the actor's executor

    def hello_delay(self, seconds):
        _direct.TEST_HELLO_DELAY_S = seconds  # this runs in the actor's executor

    def keep_for(self, seconds):
        _direct.KEEP_S = seconds  # this runs in the actor's executor

    def replies(self):
        """The direct connections the actor's executor knows of."""
        return len(_direct._replies)

    def retire(self):
        """As once its peer closes or replaces this instance: its executor
        begins no direct call after this (it runs in the actor's executor)."""
        for reply in list(_direct._replies.values()):
            reply.executor.generation = _direct.RETIRED

    def supersede(self):
        """As once the actor's generation moved past the one its callers were
        greeted with (it runs in the actor's executor)."""
        for reply in list(_direct._replies.values()):
            reply.executor.generation = reply.generation + 1

    def until_cancelled(self, began, saw):
        open(began, "w").close()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if ch.context().cancelled():
                open(saw, "w").close()
                return "stopped"
            time.sleep(0.01)
        return "never cancelled"


def _count_then_wait(tag, seconds):
    """Count a run outside the actor (its state dies with its process), then take `seconds`."""
    ch.coord.Store("test-direct-runs").counter(tag).add()
    time.sleep(seconds)
    return "done"


def runs(tag) -> int:
    return ch.coord.Store("test-direct-runs").counter(tag).value()


def sent() -> int:
    return _direct._counter["direct"]


def ready(a) -> None:
    """Until the actor's executor listens (its constructor ran)."""
    a.all.remote().result(timeout=60)
    path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
    deadline = time.monotonic() + 10
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.01)


def listening(a) -> None:
    """Until the executor of `a`, one of Chimaera's own actors (a bundle's or
    a gang rank's host) that is built, listens for direct calls."""
    path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
    deadline = time.monotonic() + 30
    while not os.path.exists(path):
        assert time.monotonic() < deadline, "its executor never listened"
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


@ch.remote(deterministic=False)
class Dice:
    def all(self):
        return []

    def roll(self, x):
        return x


def test_a_call_that_is_not_deterministic_never_goes_direct():
    # A direct call runs before its peer has any record of it, so a retry of
    # its caller may run it again: only calls that may run again go direct.
    # deterministic=False says another run may give another answer, wherever
    # it is declared: on the call, the method or the actor.
    a, dice = Log.remote(), Dice.remote()
    ready(a)
    assert dice.all.remote().result(timeout=60) == []  # built (it never listens for direct calls)
    before = sent()
    view = a.add.options(deterministic=False).remote(1)
    assert view.result(timeout=60) == 1
    assert a.draw.remote(2).result(timeout=60) == 2
    assert dice.roll.remote(3).result(timeout=60) == 3
    assert sent() == before, "each went through the peer"
    deadline = time.monotonic() + 10
    while not (e := view.explain())["attempts"] and time.monotonic() < deadline:
        time.sleep(0.01)
    (attempt,) = e["attempts"]
    assert not attempt["placement"].startswith("direct"), attempt["placement"]
    # Deterministic calls still go direct, effects="idempotent" ones too.
    assert a.add.options(effects="idempotent").remote(4).result(timeout=60) == 3
    assert a.add.remote(5).result(timeout=60) == 4
    assert sent() - before == 2
    assert a.all.remote().result(timeout=60) == [1, 2, 4, 5]


def one_call(h, method, x):
    """Inside a call: one call of `method` through `h`. Its value, whether it
    went direct from here, and whether its policy here says deterministic."""
    m = getattr(h, method)
    before = _direct._counter["direct"]
    value = m.remote(x).result()
    return value, _direct._counter["direct"] - before, m.policy().get("deterministic")


def test_a_handle_passed_to_a_call_keeps_whether_its_actors_calls_may_run_again():
    # Inside a call, a direct call is one a retry of its caller could run
    # again. A handle passed there keeps its actor's effects and determinism,
    # declared on its class or set as it was made, so a nondeterministic
    # actor's calls take the peer from there too, from a task or an actor.
    dice, made, plain = Dice.remote(), Log.options(deterministic=False).remote(), Log.remote()
    for a in (dice, made):
        assert a.all.remote().result(timeout=60) == []  # built (neither listens for direct calls)
    ready(plain)
    task = ch.remote(one_call)
    assert task.remote(dice, "roll", 1).result(timeout=60) == (1, 0, False)
    assert task.remote(made, "add", 2).result(timeout=60) == (1, 0, False)
    assert Caller.remote(dice).call.remote("roll", 3).result(timeout=60) == (3, 0, False)
    assert task.remote(plain, "add", 4).result(timeout=60) == (1, 1, True), "a deterministic actor's call goes direct"


def test_a_handle_passed_to_a_call_leaves_its_actors_budget_behind():
    # Only what says whether its calls may run again goes with the handle. A
    # budget would clash with that of the call it is passed to: budgets do
    # not nest, and its calls there come under that call's.
    a = Log.options(budget={"cpu_seconds": 100}).remote()
    task = ch.remote(one_call).options(budget={"cpu_seconds": 100})
    assert task.remote(a, "add", 1).result(timeout=60)[0] == 1


def test_a_passed_handle_loads_where_the_previous_version_runs(monkeypatch):
    # An adjacent version's executor may unpickle it: one whose
    # _rebuild_actor takes five arguments and whose Actor has no
    # __setstate__. It loads there, without the options it knows nothing of.
    import cloudpickle

    from chimaera import _remote

    data = cloudpickle.dumps(Dice.remote())  # as a call's arguments are, when plain pickle can't

    def rebuild(actor, ctor, cls, digest=b"", durable=False):
        eff = _remote.resolve((), _remote.Layer({}, "handle", "decorator"))
        return _remote.Actor(_remote.Runtime.get(), actor, ctor, cls, eff, digest, durable=durable)

    monkeypatch.setattr(_remote, "_rebuild_actor", rebuild)
    monkeypatch.delattr(_remote.Actor, "__setstate__", raising=False)
    assert pickle.loads(data).roll.policy().get("deterministic") is True
    monkeypatch.undo()
    assert pickle.loads(data).roll.policy().get("deterministic") is False


def echo(x):
    return x


def test_calls_within_a_reservation_that_may_not_run_again_never_go_direct():
    # An actor or function placed within= a bundle runs in the bundle's host,
    # an actor of Chimaera's own whose options say any call may run again:
    # each call to it carries the effects and determinism of the code it runs.
    with ch.dist.reserve([{"cpus": 1}]) as res:
        dice, log = Dice.options(within=res[0]).remote(), Log.options(within=res[0]).remote()
        assert log.add.remote(0).result(timeout=60) == 1
        listening(res[0]._host())
        before = sent()
        assert dice.roll.remote(1).result(timeout=60) == 1  # deterministic=False on its class
        assert log.draw.remote(2).result(timeout=60) == 2  # on its method
        assert log.charge.remote(3).result(timeout=60) == 3  # effects="external" on its method
        assert ch.remote(echo).options(within=res[0], deterministic=False).remote(4).result(timeout=60) == 4
        assert sent() == before, "each went through the peer"
        assert log.add.remote(5).result(timeout=60) == 4
        assert ch.remote(echo).options(within=res[0]).remote(6).result(timeout=60) == 6
        assert sent() - before == 2, "the others still go direct"


def test_an_actor_made_within_a_reservation_that_may_not_run_again_is_built_through_the_peer():
    # Its constructor runs in the bundle's host, as a call of the host's own:
    # one whose class says it may not run again must not go straight there.
    with ch.dist.reserve([{"cpus": 1}]) as res:
        log = Log.options(within=res[0]).remote()
        assert log.add.remote(0).result(timeout=60) == 1
        listening(res[0]._host())
        before = sent()
        dice = Dice.options(within=res[0]).remote()  # deterministic=False on its class
        assert dice.roll.remote(1).result(timeout=60) == 1
        assert sent() == before, "its constructor went through the peer"


@ch.remote
def roll_in_a_gang(x):
    """Inside a task: a gang of Dice, one call on every rank, and how many
    calls went direct from here."""
    g = Dice.options(ranks=2).gang()
    try:
        assert g.roll.remote(0).result(timeout=120) == [0, 0]
        for host in g._hosts:
            listening(host)
        before = sent()
        return g.roll.remote(x).result(timeout=60), sent() - before
    finally:
        g.close()


def test_a_gangs_calls_that_may_not_run_again_never_go_direct():
    # A gang's ranks run in hosts of Chimaera's own too.
    g = Log.options(ranks=2).gang()
    try:
        assert g.add.remote(0).result(timeout=120) == [1, 1]
        for host in g._hosts:
            listening(host)
        before = sent()
        assert g.draw.remote(1).result(timeout=60) == [2, 2]  # deterministic=False on its method
        assert g[0].charge.remote(2).result(timeout=60) == 3  # effects="external" on its method
        assert sent() == before, "each went through the peer"
        assert g.add.remote(3).result(timeout=60) == [4, 3]
        assert g[1].add.remote(4).result(timeout=60) == 4
        assert sent() - before == 3, "the others still go direct"
    finally:
        g.close()
    # deterministic=False on its class, in a gang made inside a task: a retry
    # of the task could run its ranks' calls again.
    assert roll_in_a_gang.remote(1).result(timeout=180) == ([1, 1], 0)


@ch.remote
def by_name(name, method, x):
    return one_call(ch.actor(name), method, x)


def test_an_actor_made_nondeterministic_takes_no_direct_call_through_any_handle():
    # A handle found by name knows its actor's class, not the options it was
    # made with: the actor's executor serves no direct calls at all.
    name = "test-direct-nondeterministic"
    dice = Dice.options(name=name).remote()
    assert dice.all.remote().result(timeout=60) == []
    before = sent()
    assert ch.actor(name).roll.remote(1).result(timeout=60) == 1
    # Not even one whose call says it is deterministic: its actor says otherwise.
    assert dice.roll.options(deterministic=True).remote(2).result(timeout=60) == 2
    assert sent() == before, "each went through the peer"
    assert ch.remote(by_name).remote(name, "roll", 3).result(timeout=60)[:2] == (3, 0)
    # A deterministic actor's calls through a handle found by name still go direct.
    plain = Log.options(name=f"{name}-control").remote()
    ready(plain)
    assert ch.actor(f"{name}-control").add.remote(1).result(timeout=60) == 1
    assert sent() - before == 1
    assert ch.remote(by_name).remote(f"{name}-control", "add", 2).result(timeout=60)[:2] == (2, 1)


@ch.remote(concurrency=4)
class Wide:
    def all(self):
        return []

    def nap(self, seconds):
        time.sleep(seconds)
        return threading.get_ident()

    def pid(self):
        return os.getpid()

    def count_then_wait(self, tag, seconds):
        return _count_then_wait(tag, seconds)

    def set_marks(self, n):
        _direct.MARKS = n  # this runs in the actor's executor


def test_a_concurrent_actor_takes_the_peer_path_while_its_marks_are_in_use(monkeypatch):
    # Each direct call in flight to a concurrent actor holds one of its
    # connection's marks (what says it began, should the connection break).
    # With all of them in use, the next calls go through the peer instead.
    a = Wide.remote()
    a.set_marks.options(deadline="60s").remote(4).result(timeout=60)  # through the peer, before any connection
    wait_for(_direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor))
    monkeypatch.setattr(_direct, "MARKS", 4)
    threads = ch.get([a.nap.remote(0.5) for _ in range(8)], timeout=60)
    assert len(threads) == 8
    conn = _direct._conns[a._actor]
    assert conn.sent == 4, "the four calls past the marks took the peer path"
    assert a.nap.remote(0).result(timeout=60)
    assert conn.sent == 5, "with marks free again, calls go direct"


def test_a_long_call_holds_only_its_own_mark(monkeypatch):
    # A mark is free again once its call's outcome is in, whichever mark it
    # is: one long call used to send every call after the next MARKS - 1
    # through the peer until it finished.
    a = Wide.remote()
    a.set_marks.options(deadline="60s").remote(4).result(timeout=60)
    wait_for(_direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor))
    monkeypatch.setattr(_direct, "MARKS", 4)
    long = a.nap.remote(3.0)
    for _ in range(8):
        a.nap.remote(0).result(timeout=60)  # one at a time: two marks in use at most
    conn = _direct._conns[a._actor]
    assert conn.sent == 9, f"{9 - conn.sent} calls took the peer path with marks free"
    long.result(timeout=60)


def test_calls_past_the_marks_leave_the_direct_order_whole_for_a_break(monkeypatch):
    # A call past the marks takes the peer path under its handle's own order.
    # It used to go under the direct one, the peer took that order to be past
    # the direct calls still queued, and when the connection broke those
    # (which had not begun, so went through the peer) never ran.
    a = Wide.remote()
    a.set_marks.options(deadline="60s").remote(8).result(timeout=60)
    wait_for(_direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor))
    monkeypatch.setattr(_direct, "MARKS", 8)
    tags = [os.urandom(8).hex() for _ in range(9)]
    futures = [a.count_then_wait.remote(t, 2.0) for t in tags]  # 4 run, 4 wait for a lane, 1 past the marks
    deadline = time.monotonic() + 30
    while sum(runs(t) for t in tags) < 4:
        assert time.monotonic() < deadline, "the calls never began"
        time.sleep(0.01)
    garble(a)
    assert ch.get(futures, timeout=60) == ["done"] * 9
    assert [runs(t) for t in tags] == [1] * 9, "each ran once"


def test_a_concurrent_actor_goes_direct_and_keeps_its_concurrency():
    a = Wide.remote()
    ready(a)
    # A handle that knows its class but not its actor's options (as one found
    # by name): the actor's own concurrency still applies.
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


def garble(a) -> None:
    """Send the actor's executor a frame that will not decode: its reader
    drops the connection, while the call it is running carries on."""
    c = _direct._conns[a._actor]
    with c.send_lock:
        c.sock.sendall(_direct.HEADER.pack(4) + b"\x00\x00\x00\x00")


def wait_for(path: str) -> None:
    deadline = time.monotonic() + 30
    while not os.path.exists(path):
        assert time.monotonic() < deadline, "the call never began"
        time.sleep(0.01)


def test_a_call_that_ran_runs_once_when_its_connection_breaks_before_the_reply(tmp_path):
    # The connection breaks after the call changed the actor's state and
    # before its outcome came back. Its caller used to send every call still
    # waiting through the peer, and this one then ran a second time.
    a = Log.remote()
    ready(a)
    marker = str(tmp_path / "ran")
    running = a.add_then_wait.remote("once", marker, 1.0)
    queued = a.add.remote("next")  # had not begun at the break: it goes through the peer
    wait_for(marker)
    garble(a)
    assert running.result(timeout=60) == 1, "the outcome of its one run"
    assert queued.result(timeout=60) == 2
    assert a.all.remote().result(timeout=60) == ["once", "next"], "each ran once"


def test_a_call_its_live_executor_cannot_account_for_is_outcome_unknown(tmp_path):
    # The executor no longer knows the broken connection: the call that may
    # have run is never run on a guess; the one behind it had not begun.
    a = Log.remote()
    ready(a)
    a.forget_reclaims.remote(True).result(timeout=60)
    marker = str(tmp_path / "ran")
    running = a.add_then_wait.remote("once", marker, 1.0)
    queued = a.add.remote("next")
    wait_for(marker)
    garble(a)
    with pytest.raises(ch.OutcomeUnknown, match="began there"):
        running.result(timeout=60)
    assert queued.result(timeout=60) == 2
    assert a.all.remote().result(timeout=60) == ["once", "next"], "each ran once"


def test_a_call_whose_live_executor_does_not_answer_is_outcome_unknown(tmp_path, monkeypatch):
    # The executor ended the connection (on a frame it could not read), and
    # does not answer its caller's new one in time. The call that began may
    # have run; the one behind it had not, and never will there.
    a = Log.remote()
    ready(a)
    a.hello_delay.remote(4.0).result(timeout=60)
    monkeypatch.setattr(_direct, "RECLAIM_WAIT_S", 1.0, raising=False)
    marker = str(tmp_path / "ran")
    running = a.add_then_wait.remote("once", marker, 2.0)
    queued = a.add.remote("next")
    wait_for(marker)
    garble(a)
    with pytest.raises(ch.OutcomeUnknown, match="did not answer within 1 s"):
        running.result(timeout=60)
    assert queued.result(timeout=60) == 2
    assert a.all.remote().result(timeout=60) == ["once", "next"], "each ran once"


def test_a_call_running_when_its_executor_dies_is_not_run_again():
    # As through the peer: the call that was running raises ActorDied, and the
    # calls behind it, which had not begun, run on the next instance in order.
    a = Log.options(restart="fresh", max_restarts=3).remote()
    ready(a)
    pid = a.pid.remote().result(timeout=60)
    tag = os.urandom(8).hex()
    running = a.count_then_wait.remote(tag, 3.0)
    after = [a.add.remote(i) for i in range(5)]
    deadline = time.monotonic() + 30
    while runs(tag) == 0:
        assert time.monotonic() < deadline, "the call never began"
        time.sleep(0.01)
    os.kill(pid, signal.SIGKILL)
    with pytest.raises(ch.ActorDied, match="died while this call was running"):
        running.result(timeout=90)
    ran = []
    for i, f in enumerate(after):
        try:
            assert f.result(timeout=90) == len(ran) + 1
            ran.append(i)
        except ch.ActorDied:
            pass  # the peer had sent it to the process before it saw it die: it began there
    assert len(ran) >= len(after) - 1
    assert a.all.remote().result(timeout=60) == ran, "each ran once, in order"
    assert runs(tag) == 1, "the restarted actor never ran it"


def test_a_concurrent_actors_calls_that_began_are_not_run_again_when_it_dies():
    # Four lanes: the executor tells its caller which calls began, and only
    # those may have run.
    a = Wide.options(restart="fresh", max_restarts=3).remote()
    ready(a)
    pid = a.pid.remote().result(timeout=60)
    tags = [os.urandom(8).hex() for _ in range(6)]
    futures = [a.count_then_wait.remote(t, 3.0) for t in tags]
    deadline = time.monotonic() + 30
    while sum(runs(t) for t in tags) < 4:
        assert time.monotonic() < deadline, "the calls never began"
        time.sleep(0.01)
    os.kill(pid, signal.SIGKILL)
    for f in futures[:4]:
        with pytest.raises(ch.ActorDied):
            f.result(timeout=90)
    for f in futures[4:]:
        try:
            assert f.result(timeout=90) == "done"  # had not begun: ran on the next instance
        except ch.ActorDied:
            pass  # the peer had sent it to the process before it saw it die
    assert [runs(t) for t in tags[:4]] == [1] * 4, "a call that began never runs again"
    assert all(runs(t) <= 1 for t in tags[4:])


@pytest.mark.parametrize("cls", [Log, Wide], ids=["one-at-a-time", "concurrent"])
def test_a_call_whose_outcome_would_not_decode_here_never_runs_again(cls, monkeypatch):
    # The call ran and its outcome came back, but would not decode: its
    # caller used to send it through the peer again (and the executor, asked,
    # had forgotten it ran), so it ran twice. The call after it may have begun
    # too: it runs once, whatever its caller learns of it.
    a = cls.remote()
    ready(a)
    read, lost = _direct._recv, {}

    def unreadable(sock):
        m = read(sock)
        if m is not None and m[0] == "done" and m[1] == lost.get("key"):
            raise pickle.UnpicklingError("injected: this outcome would not decode")
        return m

    monkeypatch.setattr(_direct, "_recv", unreadable)
    assert a.pid.remote().result(timeout=60)  # the caller's reader now reads through it
    target, follow = os.urandom(8).hex(), os.urandom(8).hex()
    first = a.count_then_wait.remote(target, 0.5)
    lost["key"] = first.key
    second = a.count_then_wait.remote(follow, 0)
    outcomes = []
    for f in (first, second):
        try:
            outcomes.append(f.result(timeout=60))
        except ch.OutcomeUnknown as e:
            outcomes.append(e)
    monkeypatch.undo()
    assert runs(target) == 1, "the call whose outcome was lost ran again"
    assert isinstance(outcomes[0], ch.OutcomeUnknown) and "lost on the way back" in str(outcomes[0]), outcomes[0]
    assert runs(follow) == 1
    assert outcomes[1] == "done" or isinstance(outcomes[1], ch.OutcomeUnknown), outcomes[1]


def test_a_direct_call_queued_behind_another_callers_call_runs_on_the_next_instance():
    # A call through the peer was running when the actor's process died, and
    # this handle's direct call waited behind it: it had not begun, so it runs
    # on the restarted instance (it used to raise ActorDied, as if it had).
    a = Log.options(restart="fresh", max_restarts=3).remote()
    ready(a)
    pid = a.pid.remote().result(timeout=60)
    other = _rebuild_actor(a._actor, a._ctor, a._cls, a._digest)  # another handle: its own order
    tag = os.urandom(8).hex()
    running = a.count_then_wait.options(deadline="60s").remote(tag, 30.0)  # a deadline: through the peer
    deadline = time.monotonic() + 30
    while runs(tag) == 0:
        assert time.monotonic() < deadline, "the call never began"
        time.sleep(0.01)
    before = sent()
    queued = other.add.remote("x")
    assert sent() == before + 1, "it went direct"
    time.sleep(0.3)  # in the executor's inbox, behind the running call
    os.kill(pid, signal.SIGKILL)
    assert queued.result(timeout=90) == 1, "it ran once, on the next instance"
    with pytest.raises(ch.ActorDied):
        running.result(timeout=90)
    assert other.all.remote().result(timeout=60) == ["x"]
    assert runs(tag) == 1


def test_a_cancel_made_while_its_connection_is_reclaimed_reaches_the_call(tmp_path):
    # The executor answers the caller's new connection a second late; a
    # cancel made meanwhile used to go out on the closed connection, and the
    # running call never heard of it.
    a = Log.remote()
    ready(a)
    a.hello_delay.remote(1.0).result(timeout=60)
    began, saw = str(tmp_path / "began"), str(tmp_path / "saw")
    f = a.until_cancelled.remote(began, saw)
    wait_for(began)
    garble(a)
    time.sleep(0.3)  # its caller is waiting for the executor to answer
    assert f.cancel()
    deadline = time.monotonic() + 20
    while not os.path.exists(saw):
        assert time.monotonic() < deadline, "the cancel never reached the running call"
        time.sleep(0.01)
    try:
        assert f.result(timeout=60) == "stopped"
    except ch.Cancelled:
        pass


@ch.remote(concurrency=1000)
class Hold:
    def __init__(self):
        self.started = 0

    async def all(self):
        return []

    async def count(self):
        return self.started

    async def hold(self, seconds):
        self.started += 1
        await asyncio.sleep(seconds)  # cancellation is cooperative: it carries on
        return "done"


def test_a_break_with_many_cancelled_calls_running_is_settled():
    # Asking what became of its calls, the caller sends its cancels again,
    # and the executor answers with every call still running. Past a socket's
    # buffer each way (8 KiB on macOS), both sides used to block writing.
    a = Hold.remote()
    ready(a)
    futures = [a.hold.remote(5.0) for _ in range(600)]
    deadline = time.monotonic() + 60
    while a.count.remote().result(timeout=60) < len(futures):
        assert time.monotonic() < deadline, "the calls never all began"
        time.sleep(0.05)
    for f in futures[:150]:
        f.cancel()
    garble(a)
    _, waiting = ch.wait(futures, timeout=60)
    assert not waiting, f"{len(waiting)} calls never ended"


def test_an_executor_lets_go_of_outcomes_no_caller_asks_for():
    # A caller that drops its connection without asking after its calls (it
    # closed, or its process died): what the executor kept for it goes after
    # a while, not once 64 more connections ended.
    a = Log.remote()
    ready(a)
    a.keep_for.remote(0.5).result(timeout=60)
    assert a.replies.remote().result(timeout=60) == 1, "this process's connection"
    running = a.wait.remote(1.0)
    time.sleep(0.3)
    _direct.forget(_rebuild_actor(a._actor, a._ctor, a._cls, a._digest))  # asks after nothing
    with pytest.raises(ch.ActorDied):
        running.result(timeout=60)
    deadline = time.monotonic() + 10
    while (n := a.replies.remote().result(timeout=60)) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert n == 0, "it still keeps what it kept for that connection"


def test_a_first_call_does_not_wait_for_the_executors_greeting():
    # The executor answers a new connection's hello 3 s late: the first call
    # still goes at once, direct, and waits there. .remote() used to wait for
    # the answer (and after 5 s send the actor's calls through the peer).
    a = Log.remote()
    a.hello_delay.options(deadline="60s").remote(3.0).result(timeout=60)  # through the peer
    wait_for(_direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor))
    before, start = sent(), time.monotonic()
    first = a.add.remote(1)
    assert time.monotonic() - start < 1.5, ".remote() waited for the executor's greeting"
    assert sent() == before + 1, "it went direct"
    assert first.result(timeout=60) == 1
    assert a.add.remote(2).result(timeout=60) == 2


@ch.remote
def later(x, seconds):
    time.sleep(seconds)
    return x


def test_a_call_made_while_its_connection_is_reclaimed_keeps_its_place(tmp_path):
    # The executor answers its caller's new connection 2 s late. A call whose
    # input lands meanwhile waits, then goes through the peer behind its
    # handle's earlier calls. It used to go at once, the peer skipped past
    # the call before it (not yet recorded), and that one never ran.
    a = Log.remote()
    ready(a)
    a.hello_delay.remote(2.0).result(timeout=60)
    marker = str(tmp_path / "ran")
    first = a.add_then_wait.remote("first", marker, 0.5)
    second = a.add.remote("second")  # queued behind it at the executor
    third = a.add.remote(later.remote("third", 1.0))  # waits here for its input
    wait_for(marker)
    garble(a)
    assert [f.result(timeout=30) for f in (first, second, third)] == [1, 2, 3]
    assert a.all.remote().result(timeout=60) == ["first", "second", "third"]


def test_a_call_cancelled_before_it_began_is_not_run_after_a_break(tmp_path):
    # Its executor would have answered it as cancelled at its turn, but the
    # connection broke first. It used to go through the peer with the calls
    # that had not begun, and run.
    a = Log.remote()
    ready(a)
    marker = str(tmp_path / "ran")
    first = a.add_then_wait.remote("first", marker, 1.0)
    queued = a.add.remote("cancelled")
    wait_for(marker)
    assert queued.cancel()
    time.sleep(0.3)  # the cancel reached the executor's reader
    garble(a)
    assert first.result(timeout=60) == 1
    with pytest.raises(ch.Cancelled):
        queued.result(timeout=60)
    assert a.all.remote().result(timeout=60) == ["first"]


def test_a_call_cancelled_while_waiting_for_its_input_holds_up_no_call_after_a_break(tmp_path):
    # A call cancelled while it waits here for its input is never sent, but it
    # took a place in its handle's order. Once a break sends the handle's
    # later calls through the peer, the peer waits for each place in turn.
    a = Log.remote()
    ready(a)
    marker = str(tmp_path / "ran")
    first = a.add_then_wait.remote(0, marker, 1.0)
    queued = [a.add.remote(n) for n in (1, 2, 3)]
    slow = later.remote(4, 10.0)
    waiting = a.add.remote(slow)
    after = a.add.remote(5)
    wait_for(marker)
    assert waiting.cancel()
    time.sleep(0.3)
    garble(a)
    assert [f.result(timeout=60) for f in [first, *queued]] == [1, 2, 3, 4]
    assert after.result(timeout=60) == 5
    with pytest.raises(ch.Cancelled):
        waiting.result(timeout=60)
    assert a.all.remote().result(timeout=60) == [0, 1, 2, 3, 5]
    slow.cancel(force=True)  # not left running as the session's peer stops
    ch.wait([slow], timeout=30)


def test_calls_a_break_sends_through_the_peer_run_in_their_handles_order(tmp_path):
    # The connection breaks with a call running and calls queued behind it at
    # the executor, which answers its caller's new connection 0.25 s late.
    # Meanwhile another thread makes calls through the handle, and calls
    # wait here for inputs that land before, while and after the caller
    # works out what became of the rest. None that began runs again, and the
    # others go through the peer, which has run none of the handle's direct
    # calls, behind the calls made before them: those queued at the executor,
    # then those that waited here, then those made after. Repeated: the
    # paths race each other.
    paths = [0, 0]  # of the calls made as the connection broke: how many went direct, and through the peer
    for i in range(20):
        a = Log.remote()
        ready(a)
        a.hello_delay.remote(0.25).result(timeout=60)
        marker = str(tmp_path / f"ran-{i}")
        futures = [a.add_then_wait.remote(0, marker, 0.2)]
        futures += [a.add.remote(n) for n in range(1, 6)]  # queued at the executor: not begun at the break
        futures += [a.add.remote(later.remote(n, s)) for n, s in zip(range(6, 10), (0, 0.1, 0.25, 0.5))]
        wait_for(marker)
        more: list = []

        def make():
            for n in range(10, 16):
                more.append(a.add.remote(n))
                time.sleep(0.04)

        sender, before = threading.Thread(target=make), sent()
        sender.start()
        garble(a)
        sender.join(60)
        assert not sender.is_alive(), f"round {i}: a call made during the break never returned"
        paths[0] += sent() - before
        paths[1] += len(more) - (sent() - before)
        futures += more
        _, pending = ch.wait(futures, timeout=30)
        assert not pending, f"round {i}: {len(pending)} of the handle's calls never ran"
        assert [f.result() for f in futures] == list(range(1, 17)), f"round {i}"
        assert a.all.remote().result(timeout=60) == list(range(16)), f"round {i}: not in the handle's order"
        a.close()
    assert all(paths), f"calls made as the connection broke, direct and through the peer: {paths}"


class _HoldRecords:
    """The runtime's client, holding back direct calls' records."""

    def __init__(self, client):
        self._client, self.held = client, []

    def submit(self, specs):
        self.held += [s for s in specs if s.get("ran")]
        rest = [s for s in specs if not s.get("ran")]
        if rest:
            self._client.submit(rest)

    def __getattr__(self, name):
        return getattr(self._client, name)


def test_a_break_after_the_reply_keeps_the_callers_result():
    # The outcome came back, and the connection breaks before the call's
    # record reaches the peer: the result stands and the call never runs again.
    rt = ch.runs.current_run()._rt
    a = Log.remote()
    ready(a)
    client, rt.client = rt.client, _HoldRecords(rt.client)
    try:
        out = a.add.remote("x")
        assert out.result(timeout=60) == 1
        conn = _direct._conns[a._actor]
        garble(a)
        deadline = time.monotonic() + 30
        while not conn.broken:
            assert time.monotonic() < deadline, "the connection never broke"
            time.sleep(0.01)
        held = rt.client.held
    finally:
        rt.client = client
    assert [s["key"] for s in held] == [out.key]
    client.submit(held)
    assert out.result(timeout=60) == 1
    assert a.all.remote().result(timeout=60) == ["x"], "it ran once"
    deadline = time.monotonic() + 10
    while (e := out.explain())["state"] != "Done" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert e["state"] == "Done"


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


@ch.remote
def close_elsewhere(a):
    a.close()  # this process's handle: the test's own connection to the actor is not closed by it


@pytest.mark.parametrize("cls", [Log, Wide], ids=["one-at-a-time", "concurrent"])
def test_calls_queued_when_another_process_closes_the_actor_never_begin_there(cls):
    # Another process closes the actor while this one's direct calls wait at
    # its executor, which then has a few seconds to clean up and exit. A
    # concurrent actor's lanes used to begin the calls they held as its
    # process exited, on an instance that was gone, and they failed with
    # whatever that raised. They never begin there: they end as calls to a
    # closed actor do, through the peer.
    a = cls.remote()
    ready(a)
    width = 4 if cls is Wide else 1
    tags = [os.urandom(8).hex() for _ in range(width + 4)]
    running = [a.count_then_wait.remote(t, 1.5) for t in tags[:width]]
    queued = [a.count_then_wait.remote(t, 0) for t in tags[width:]]
    deadline = time.monotonic() + 30
    while sum(runs(t) for t in tags) < width:
        assert time.monotonic() < deadline, "the calls never began"
        time.sleep(0.01)
    counted = dict(_direct._counter)
    close_elsewhere.remote(a).result(timeout=60)
    for f in queued:
        with pytest.raises(ch.ActorDied):
            f.result(timeout=60)
    for f in running:
        try:
            assert f.result(timeout=60) == "done"  # it began before the close
        except ch.ActorDied:
            pass  # stopped by the close
    assert [runs(t) for t in tags] == [1] * width + [0] * 4
    if cls is Wide:
        # Its lanes took them up as its process exited, and its executor
        # refused each: its instance was no longer the actor's. (One at a
        # time, the process ends before their turn comes.)
        assert _direct._counter["refused"] - counted["refused"] == 4


@pytest.mark.parametrize("how", ["retire", "supersede"])
def test_calls_an_executor_refuses_run_once_through_the_peer_in_order(how):
    # The executor takes no more direct calls for its instance, as once its
    # peer closed it (retired) or its generation moved on (superseded). Each
    # call it refuses never began there: it goes through the peer, which runs
    # it on the actor's current instance (here the same one), in the order
    # made, and so do the calls made after.
    a = Log.remote()
    ready(a)
    before, refused = sent(), _direct._counter["refused"]
    first = [a.wait.remote(1.0), getattr(a, how).remote()]  # meanwhile its executor reads all the calls below
    futures = [a.add.remote(i) for i in range(20)]
    assert sent() == before + 22, "they went direct"
    assert [f.result(timeout=60) for f in futures] == list(range(1, 21))
    assert [f.result(timeout=60) for f in first] == [1.0, None]
    assert _direct._counter["refused"] - refused == 20, "its executor refused each"
    assert a.add.remote(20).result(timeout=60) == 21
    assert sent() == before + 22, "a call made after one was refused takes the peer path"
    assert a.all.options(deadline="60s").remote().result(timeout=60) == list(range(21)), "each ran once, in order"


def lease_file() -> str:
    """The session peer's lease on its executors' direct calls, beside their
    sockets (crates/chimaera-peer/src/direct/lease.rs)."""
    rt = ch.runs.current_run()._rt
    sock = os.environ.get("CHIMAERA_PEER_SOCKET") or peer_socket(rt.home)
    return os.path.join(os.path.dirname(sock), "direct", os.path.basename(sock) + ".lease")


@ch.remote
def calls_counted(a, xs):
    """Inside a call: `a.add(x)` for each of `xs`, in order. Their values, how
    many went direct from here, and how many of those its executor refused."""
    sent0, refused0 = _direct._counter["direct"], _direct._counter.get("refused", 0)
    values = [f.result() for f in [a.add.remote(x) for x in xs]]
    return values, _direct._counter["direct"] - sent0, _direct._counter.get("refused", 0) - refused0


def attempts(f) -> list:
    """The attempts explain() shows for a finished call, once its record landed."""
    deadline = time.monotonic() + 10
    while not (e := f.explain())["attempts"] and time.monotonic() < deadline:
        time.sleep(0.01)
    return e["attempts"]


@pytest.mark.parametrize("lapse", ["ran out", "renewed in a new epoch"])
def test_an_executor_whose_lease_lapsed_begins_no_direct_call_again(lapse):
    # The peer renews its executors' lease only while it hears most of its
    # cluster. One that lapsed means the peer was paused or cut off, maybe
    # long enough to be declared dead and the actor started again on another
    # peer. From then on that executor begins no direct call, even once the
    # peer holds the lease again (in a new epoch): each goes through the
    # peer, in order, and runs once.
    a = Log.remote()
    ready(a)
    assert a.add.remote(0).result(timeout=60) == 1
    assert attempts(a.add.remote(1))[0]["placement"].startswith("direct"), "it went direct while leased"
    path = lease_file()
    made = not os.path.exists(path)  # a peer that keeps no lease
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if made:
            os.ftruncate(fd, 8)
        m = mmap.mmap(fd, 8)
        word = memoryview(m).cast("Q")
        kept = word[0]
        epoch = kept >> 48
        word[0] = (epoch << 48) | 1 if lapse == "ran out" else (((epoch + 1) & 0xFFFF) << 48) | ((1 << 48) - 1)
        try:
            futures = [a.add.remote(i) for i in range(2, 12)]
            assert [f.result(timeout=60) for f in futures] == list(range(3, 13))
        finally:
            word[0] = kept  # the other actors' executors read it too
            word.release()
            m.close()
        for f in futures:
            got = attempts(f)
            assert got and not any(t["placement"].startswith("direct") for t in got), f"it began direct: {got}"
        values, direct, refused = calls_counted.remote(a, list(range(12, 16))).result(timeout=60)
        assert values == list(range(13, 17))
        assert refused == direct, "another process's calls go through the peer too, the lease held again"
    finally:
        os.close(fd)
        if made:
            os.unlink(path)
    assert a.all.options(deadline="60s").remote().result(timeout=60) == list(range(16)), "each ran once, in order"


@ch.remote
class Slow:
    """Its constructor says it began (a file), then waits for another."""

    def __init__(self, began, go):
        open(began, "w").close()
        deadline = time.monotonic() + 60
        while not os.path.exists(go) and time.monotonic() < deadline:
            time.sleep(0.01)
        self.n = 0

    def add(self):
        self.n += 1
        return self.n


def test_an_instance_built_while_its_peers_lease_lapsed_serves_no_direct_call(tmp_path):
    # The lease ran out while the constructor ran and came back in a new
    # epoch: the peer may have been declared dead meanwhile and the actor
    # placed again elsewhere, so this instance takes its calls through the peer.
    began, go = str(tmp_path / "began"), str(tmp_path / "go")
    path = lease_file()
    made = not os.path.exists(path)  # a peer that keeps no lease
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if made:
            os.ftruncate(fd, 8)
        m = mmap.mmap(fd, 8)
        word = memoryview(m).cast("Q")
        kept = word[0]
        epoch = kept >> 48
        try:
            a = Slow.remote(began, go)
            wait_for(began)
            word[0] = (epoch << 48) | 1  # ran out
            time.sleep(0.2)
            word[0] = (((epoch + 1) & 0xFFFF) << 48) | ((1 << 48) - 1)  # held again, in a new epoch
            open(go, "w").close()
            # It runs after the constructor, which listens (or not) before it returns.
            assert a.add.remote().result(timeout=60) == 1
            sock = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
            assert not os.path.exists(sock), "it listens for direct calls"
            futures = [a.add.remote() for _ in range(5)]
            assert [f.result(timeout=60) for f in futures] == [2, 3, 4, 5, 6]
        finally:
            word[0] = kept  # the other actors' executors read it too
            word.release()
            m.close()
        for f in futures:
            got = attempts(f)
            assert got and not any(t["placement"].startswith("direct") for t in got), f"it began direct: {got}"
    finally:
        os.close(fd)
        if made:
            os.unlink(path)


HELD_WHEN_ITS_PEER_DIES = """
import os, sys, time
import chimaera as ch
from chimaera import _direct

MARKS = sys.argv[1]

@ch.remote
class Held:
    def pid(self):
        return os.getpid()

    def hold(self):
        open(os.path.join(MARKS, "began"), "w").close()
        while not os.path.exists(os.path.join(MARKS, "go")):
            time.sleep(0.01)
        return "held"

    def mark(self, i):
        with open(os.path.join(MARKS, f"queued-{i}"), "w") as f:
            f.write(str(os.getpid()))
        return i

a = Held.remote()
print(a.pid.remote().result(timeout=60), flush=True)
path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), a._actor)
deadline = time.monotonic() + 30
while not os.path.exists(path) and time.monotonic() < deadline:
    time.sleep(0.01)
before = _direct._counter["direct"]
held = a.hold.remote()
queued = [a.mark.remote(i) for i in range(5)]
print(_direct._counter["direct"] - before, flush=True)
time.sleep(600)
"""


def test_calls_queued_at_an_actor_whose_peer_died_never_begin_there(tmp_path):
    # The actor's peer dies while a direct call runs at its executor and five
    # wait behind it. The executor lives on a moment (it leaves once its peer
    # has been gone a while), and the calls waiting there used to run then,
    # on an instance that was no longer the actor's: its peer, started again,
    # makes another.
    home = tempfile.mkdtemp(prefix="chk", dir="/tmp")
    peer = _start_peer(home)
    script = tmp_path / "driver.py"
    script.write_text(HELD_WHEN_ITS_PEER_DIES)
    env = dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    cmd = [sys.executable, str(script), str(tmp_path)]
    driver = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    try:
        line = driver.stdout.readline().strip()
        assert line.isdigit(), line or driver.stderr.read()
        pid = int(line)
        assert driver.stdout.readline().strip() == "6", "every call went direct"
        wait_for(str(tmp_path / "began"))
        os.kill(peer.pid, signal.SIGKILL)
        peer.wait(timeout=10)
        time.sleep(0.5)  # its executor saw its peer go
        open(tmp_path / "go", "w").close()  # the running call returns: the others' turns come
        deadline = time.monotonic() + 30
        while _running(pid):
            assert time.monotonic() < deadline, "the executor outlived its peer"
            time.sleep(0.1)
        began = [m for m in glob.glob(str(tmp_path / "queued-*")) if open(m).read() == str(pid)]
        assert not began, f"{len(began)} of the calls waiting there began on the instance whose peer died"
    finally:
        driver.kill()
        driver.wait(timeout=10)
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)  # one its driver started again
        if peer.poll() is None:
            peer.kill()
            peer.wait(timeout=10)
        shutil.rmtree(home, ignore_errors=True)


REPLACED_ELSEWHERE = WHERE + """
import sys
from chimaera import _direct
GO = sys.argv[0] + ".go"
CALLS = sys.argv[0] + ".calls"

def note(what):
    with open(CALLS, "a") as f:
        f.write(f"{os.getpid()} {what} {time.time()}\\n")

@ch.remote(restart="fresh", max_restarts=3)
class Counter:
    def __init__(self):
        self.n = 0
        note("init")

    def bump(self):
        note("call")
        self.n += 1
        return peer_name(), os.getpid(), self.n

def local_counter():
    # One on this driver's own peer, which its calls reach directly.
    for _ in range(8):
        c = Counter.options(placement=ch.dist.Placement(require={"zone": "b"})).remote()
        c.bump.remote().result(timeout=60)
        before = _direct._counter["direct"]
        got = c.bump.remote().result(timeout=60)
        if _direct._counter["direct"] > before:
            return c, got
        c.close()
    raise SystemExit("no instance landed on this driver's peer")

c, (host, pid, n) = local_counter()
print("HOST", host, pid, n, flush=True)
# Calls from this machine as its peer stops and comes back: each goes
# straight to the instance here while it may.
while not os.path.exists(GO):
    try:
        c.bump.remote().result(timeout=2)
    except (TimeoutError, ch.ChimaeraError):
        pass  # through the peer, which is away, or retired: it takes no new calls
    time.sleep(0.02)
try:
    print("AFTER", *c.bump.remote().result(timeout=120), flush=True)
except ch.ChimaeraError as e:  # this peer's incarnation was retired: it takes no new calls
    print("AFTER", type(e).__name__, flush=True)
"""


LEASE_S = 3.0  # how long a renewal of a peer's lease holds (direct/lease.rs, LEASE_MS)


def no_call_began_there_after_its_lease(script: str, pid: int, stopped: float) -> None:
    """The instance `pid`, on the peer stopped at `stopped`, began no call
    once that peer's lease ran out, nor once the next instance started."""
    rows = [ln.split() for ln in open(script + ".calls")]
    old = [float(t) for p, what, t in rows if int(p) == pid and what == "call"]
    late = [t for t in old if t > stopped + LEASE_S + 1.0]  # a second for the call to note itself
    assert not late, f"{len(late)} calls began on the stopped peer's instance after its lease ran out"
    new = [float(t) for p, what, t in rows if int(p) != pid and what == "init" and float(t) > stopped]
    assert not new or max(old) < min(new), "the actor ran calls on two instances at once"


def test_an_instance_its_peer_outlived_ends_once_the_peer_learns_of_the_next():
    # Peer 1 stops answering long enough to be declared dead, while its
    # processes run on, and its actor is placed again on peer 2. A direct
    # caller on its machine kept reaching the old instance, before peer 1
    # came back and after: the actor had two instances, each with its own
    # state. The lease on direct calls ran out first, and back, peer 1 ends
    # the old instance.
    c = Cluster(3, labels={0: "zone=a", 1: "zone=b", 2: "zone=b"})
    d = None
    try:
        d = c.driver(1, REPLACED_ELSEWHERE)
        script = [a for a in d.args if a.endswith(".py")][0]
        line = d.stdout.readline().split()
        assert line[:1] == ["HOST"], (line, d.stderr.read() if d.poll() is not None else "")
        host, pid = line[1], int(line[2])
        stopped = time.time()
        c.signal(1, signal.SIGSTOP)  # past the 6 s suspicion floor: declared dead and retired
        time.sleep(10)
        c.signal(1, signal.SIGCONT)
        assert "retiring it" in c.log(0) + c.log(2), "the pause should have retired peer 1's incarnation"
        time.sleep(1)  # the driver's calls meanwhile
        no_call_began_there_after_its_lease(script, pid, stopped)
        deadline = time.monotonic() + 90
        while _running(pid):
            assert time.monotonic() < deadline, "the instance replaced on another peer still runs"
            time.sleep(0.2)
        no_call_began_there_after_its_lease(script, pid, stopped)
        open(script + ".go", "w").close()
        out, err = d.communicate(timeout=240)
        assert d.returncode == 0, err[-3000:]
        after = next(ln for ln in out.splitlines() if ln.startswith("AFTER")).split()
        # The next instance, fresh and elsewhere, or none, as the peer this
        # caller submits to was retired; never the old one.
        assert after[1:] == ["ChimaeraError"] or (after[1] != host and after[3] == "1"), after
    finally:
        if d is not None and d.poll() is None:
            d.kill()
            d.wait(timeout=10)
        c.close()


TRAFFIC = WHERE + """
@ch.remote
def noop(i):
    return i

deadline = time.monotonic() + 14
while time.monotonic() < deadline:
    ch.get([noop.remote(i) for i in range(20)], timeout=60)
"""


def test_an_instance_its_peer_outlived_ends_once_the_peer_catches_up_from_a_snapshot():
    # As above, but the actor's group moves on so far while peer 1 is away
    # that peer 1 catches up from a snapshot, which raises none of the events
    # that tell it of the next instance: the old one ran on for good.
    env = {i: {"CHIMAERA_RAFT_SNAPSHOT_BYTES": "16384"} for i in range(3)}
    c = Cluster(3, data_groups=1, labels={0: "zone=a", 1: "zone=b", 2: "zone=b"}, env=env)
    d = None
    try:
        d = c.driver(1, REPLACED_ELSEWHERE)
        script = [a for a in d.args if a.endswith(".py")][0]
        line = d.stdout.readline().split()
        assert line[:1] == ["HOST"], (line, d.stderr.read() if d.poll() is not None else "")
        pid = int(line[2])
        stopped = time.time()
        c.signal(1, signal.SIGSTOP)
        traffic = c.run(0, TRAFFIC)  # with peer 1 away: past the snapshot threshold, the log is compacted
        assert traffic.returncode == 0, traffic.stderr[-3000:]
        c.signal(1, signal.SIGCONT)
        deadline = time.monotonic() + 60
        while _running(pid):
            assert time.monotonic() < deadline, "the instance replaced on another peer still runs"
            time.sleep(0.2)
        assert "installed a snapshot streamed from a peer" in c.log(1), "peer 1 should have caught up from a snapshot"
        no_call_began_there_after_its_lease(script, pid, stopped)
    finally:
        if d is not None and d.poll() is None:
            d.kill()
            d.wait(timeout=10)
        c.close()


def test_a_cluster_peers_lease_is_on_the_clock_its_executors_read():
    # The peer writes its lease's deadline and executors compare it on one
    # clock every process on the machine reads (_direct.LEASE_CLOCK): a
    # renewal holds about LEASE_S from now on that clock, as Python reads it.
    c = Cluster(3)
    try:
        path = _direct.lease_path(c.sock(0))
        deadline, left = time.monotonic() + 60, None
        while True:
            try:
                with open(path, "rb") as f:
                    word = struct.unpack("=Q", f.read(8))[0]
                left = (word & _direct.FOREVER) - time.clock_gettime_ns(_direct.LEASE_CLOCK) // 1_000_000
                if 0 < left <= LEASE_S * 1000 + 500:
                    break
            except (FileNotFoundError, struct.error):
                pass
            assert time.monotonic() < deadline, f"its deadline is {left} ms from now on the executors' clock"
            time.sleep(0.05)
    finally:
        c.close()


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

    def call(self, method, x):
        return one_call(self.other, method, x)


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
