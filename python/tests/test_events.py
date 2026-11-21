"""The peer's event log: what a long poll on Events reports."""

from __future__ import annotations

import time

import pytest

import chimaera as ch
from chimaera._runtime import Runtime


@ch.remote
def fails():
    raise ValueError("boom")


@ch.remote
class Thing:
    def ping(self):
        return 1


def events(kinds, since=0, wait_ms=0):
    out, nxt, peer, inc = Runtime.get().client.events(kinds, since, wait_ms)
    assert peer and inc >= 1
    return out, nxt


def test_a_final_task_failure_is_an_event():
    _, start = events(["task_failed"])
    f = fails.remote()
    with pytest.raises(ValueError):
        f.result(timeout=60)
    got, _ = events(["task_failed"], start, wait_ms=5000)
    assert any("boom" in e["message"] and e["task"] == f.key for e in got), got


def test_actor_transitions_are_events():
    _, start = events(["actor"])
    t = Thing.remote()
    t.ping.remote().result(timeout=60)
    t.close()
    deadline = time.monotonic() + 10
    seen = []
    while time.monotonic() < deadline:
        got, start = events(["actor"], start, wait_ms=1000)
        seen += [e["message"].split()[0] for e in got if e["actor"] == bytes.fromhex(t.id)]
        if "dead:" in seen:
            break
    assert "alive" in seen and "dead:" in seen, seen


def test_a_long_poll_returns_as_soon_as_an_event_arrives():
    _, start = events(["task_failed"])
    t0 = time.monotonic()
    fails.remote()
    got, nxt = events(["task_failed"], start, wait_ms=20_000)
    assert got and nxt > start
    assert time.monotonic() - t0 < 10, "the poll answered when the event came, not at its deadline"
    empty, again = events(["task_failed"], nxt, wait_ms=200)
    assert empty == [] and again == nxt
