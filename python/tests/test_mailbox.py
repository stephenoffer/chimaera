"""Per-method mailbox limits: @ch.method(mailbox=ch.Mailbox(...)). A flood of
one method is turned away or held back at its limit and never takes the room
an unrelated method needs."""

from __future__ import annotations

import time
import uuid

import pytest

import chimaera as ch
from chimaera import _direct


@ch.remote(groups={"admin": 1})
class Service:
    @ch.method(concurrency=2, mailbox=ch.Mailbox(max_calls=4, on_full="reject"))
    def render(self, seconds):
        time.sleep(seconds)
        return "rendered"

    @ch.method(concurrency=1, mailbox=ch.Mailbox(max_calls=2, on_full="wait", max_wait="1s"))
    def export(self, seconds):
        time.sleep(seconds)
        return "exported"

    @ch.method(group="admin")
    def ping(self):
        return "pong"


@ch.remote(groups={"admin": 1})
class Queue:
    """Calls that wait for room, and an admin method in its own group."""

    @ch.method(mailbox=ch.Mailbox(max_calls=2, max_wait="10s"))
    def work(self, seconds):
        time.sleep(seconds)
        return "worked"

    @ch.method(mailbox=ch.Mailbox(max_calls=2, max_wait="2s"))
    def burst(self):
        time.sleep(0.05)
        return "worked"

    @ch.method(group="admin")
    def ping(self):
        return "pong"


@ch.remote
class Serial:
    """Runs one call at a time: no groups, no concurrency."""

    @ch.method(mailbox=ch.Mailbox(max_calls=2, max_wait="3s"))
    def slow(self):
        time.sleep(0.2)
        return "slow"

    def ping(self):
        return "pong"


@pytest.fixture(params=[True, False], ids=["direct", "peer"])
def path(request, monkeypatch):
    """Both ways a driver's actor calls can go: the limits hold on either."""
    monkeypatch.setattr(_direct, "ENABLED", request.param)


def outcomes(futures):
    out = []
    for f in futures:
        try:
            out.append(f.result(timeout=120))
        except ch.errors.Overloaded as e:
            out.append(e)
    return out


def test_calls_past_the_limit_are_turned_away_and_the_admin_method_still_answers(path):
    svc = Service.remote()
    assert svc.ping.remote().result(timeout=60) == "pong"
    flood = [svc.render.remote(0.5) for _ in range(1000)]
    start = time.monotonic()
    assert svc.ping.remote().result(timeout=60) == "pong"
    assert time.monotonic() - start < 1.5, "the admin call waited behind the flood"
    results = outcomes(flood)
    turned_away = [r for r in results if isinstance(r, ch.errors.Overloaded)]
    assert len(turned_away) > 900 and set(results) - set(turned_away) == {"rendered"}
    assert "render" in str(turned_away[0]) and "max_calls" in str(turned_away[0])


def test_calls_past_the_limit_wait_their_max_wait_for_room(path):
    name = f"svc-{uuid.uuid4().hex[:8]}"
    svc = Service.options(name=name).remote()
    assert svc.ping.remote().result(timeout=60) == "pong"
    start = time.monotonic()
    flood = [svc.export.remote(0.4) for _ in range(20)]
    # Another caller's admin call is not held up by the waiting calls.
    assert ch.actor(name).ping.remote().result(timeout=60) == "pong"
    assert time.monotonic() - start < 1.0
    results = outcomes(flood)
    done = [r for r in results if r == "exported"]
    assert 2 <= len(done) < 20, results
    first_refusal = min(i for i, r in enumerate(results) if isinstance(r, ch.errors.Overloaded))
    assert "for the 1.0 s this call may wait for room" in str(results[first_refusal])
    # The ones that got room in time ran; none ran after waiting past its bound.
    assert time.monotonic() - start < 1.0 + 0.4 * len(done) + 5


def test_a_call_waiting_for_room_does_not_hold_back_its_handle(path):
    # An actor with groups promises no order: a later call through the same
    # handle goes ahead of the calls that wait for room.
    q = Queue.remote()
    assert q.ping.remote().result(timeout=60) == "pong"
    flood = [q.work.remote(0.3) for _ in range(10)]
    start = time.monotonic()
    assert q.ping.remote().result(timeout=60) == "pong"
    assert time.monotonic() - start < 1.0, "the admin call waited for the calls ahead of it on its handle"
    assert outcomes(flood) == ["worked"] * 10


def test_an_actor_that_runs_one_call_at_a_time_answers_another_caller_behind_at_most_max_calls(path):
    # Its peer starts the call ready longest, whichever handle's id is lower,
    # so try both orders of the flood's handle and the admin's.
    orders = set()
    while len(orders) < 2:
        name = f"serial-{uuid.uuid4().hex[:8]}"
        flooder = Serial.options(name=name).remote()
        assert flooder.ping.remote().result(timeout=60) == "pong"
        admin = ch.actor(name)
        order = admin._handle < flooder._handle
        if order in orders:
            flooder.close()
            continue
        orders.add(order)
        flood = [flooder.slow.remote() for _ in range(100)]
        start = time.monotonic()
        assert admin.ping.remote().result(timeout=60) == "pong"
        # At most max_calls (2) of the flood's 0.2 s calls go ahead of it.
        took = time.monotonic() - start
        assert took < (2 + 1) * 0.2 + 0.6, f"the admin call waited {took:.2f} s behind the flood"
        results = outcomes(flood)
        assert "slow" in results and any(isinstance(r, ch.errors.Overloaded) for r in results)
        flooder.close()


def test_thousands_of_calls_waiting_for_room_keep_their_peer_quick(monkeypatch):
    monkeypatch.setattr(_direct, "ENABLED", False)
    name = f"queue-{uuid.uuid4().hex[:8]}"
    q = Queue.options(name=name).remote()
    assert q.ping.remote().result(timeout=60) == "pong"
    flood = [q.burst.remote() for _ in range(5000)]
    start = time.monotonic()
    assert ch.actor(name).ping.remote().result(timeout=60) == "pong"
    assert q.ping.remote().result(timeout=60) == "pong"
    assert time.monotonic() - start < 2.0, "the peer stalled over the calls waiting for room"
    results = outcomes(flood)
    waited = [r for r in results if isinstance(r, ch.errors.Overloaded)]
    assert len(waited) > 4000 and set(results) - set(waited) == {"worked"}
    assert "for the 2.0 s this call may wait for room" in str(waited[0])


def test_mailbox_is_checked_where_written():
    with pytest.raises(ValueError, match="max_calls"):
        ch.Mailbox(max_calls=0)
    with pytest.raises(ValueError, match="on_full"):
        ch.Mailbox(max_calls=1, on_full="drop")
    with pytest.raises(ValueError, match="drop max_wait"):
        ch.Mailbox(max_calls=1, on_full="reject", max_wait="1s")
    assert ch.Mailbox(max_calls=1).max_wait == 30.0
    with pytest.raises(TypeError, match="ch.Mailbox"):
        ch.method(mailbox=3)
    with pytest.raises(TypeError, match="drop mailbox="):
        ch.method(batch=ch.Batch(), mailbox=ch.Mailbox(max_calls=1))
