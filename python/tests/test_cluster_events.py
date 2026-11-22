"""ch.runs.cluster().events(): the peer's event log as a stream of typed events."""

from __future__ import annotations

import re

import pytest

import chimaera as ch


@ch.remote(max_attempts=1)
def doomed():
    raise ValueError("no luck")


@ch.remote
class Short:
    def ping(self):
        return "pong"


def test_a_stream_from_now_sees_what_happens_next_in_order():
    stream = ch.runs.cluster().events(["task_failed", "actor"])
    f = doomed.remote()
    with pytest.raises(ValueError):
        f.result(timeout=60)
    a = Short.remote()
    assert a.ping.remote().result(timeout=60) == "pong"
    a.close()
    seen = []
    for e in stream:
        seen.append(e)
        if e.kind == "actor" and e.actor == a.id and e.message.startswith("dead"):
            break
    stream.close()
    failed = [e for e in seen if e.kind == "task_failed"]
    assert [e.task for e in failed] == [f.id] and "no luck" in failed[0].message
    mine = [re.match(r"\w+", e.message).group() for e in seen if e.kind == "actor" and e.actor == a.id]
    assert mine == ["created", "alive", "dead"], mine
    assert [e.seq for e in seen] == sorted(e.seq for e in seen)
    assert all(e.at > 0 for e in seen)


def test_history_without_follow_stops_at_the_newest():
    f = doomed.remote()
    with pytest.raises(ValueError):
        f.result(timeout=60)
    history = list(ch.runs.cluster().events(["task_failed"], start="earliest", follow=False))
    assert f.id in [e.task for e in history]
    assert all(e.kind == "task_failed" for e in history)
