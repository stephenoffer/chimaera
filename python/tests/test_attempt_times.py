"""An attempt's timestamps place each stage of a call on the wall clock."""

from __future__ import annotations

import time

import chimaera as ch


@ch.remote
def nap(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


def test_an_attempts_stages_are_in_order_and_its_ready_time_is_not_moved_by_its_run_time():
    nap.remote(0).result(timeout=60)  # an executor is up: the timed call does not wait for one
    before = time.time_ns()
    f = nap.remote(0.5)
    f.result(timeout=60)
    after = time.time_ns()
    a = f.explain()["attempts"][-1]
    ready, dispatched, entry, exit_ = (a[k] for k in ("ready_unix_ns", "dispatched_unix_ns", "py_entry_unix_ns", "py_exit_unix_ns"))
    slack = 20_000_000  # the peer and this process read one clock; allow for rounding between them
    assert before - slack <= ready <= dispatched + slack, (before, ready, dispatched)
    assert dispatched <= entry + slack and entry < exit_ <= after + slack
    assert exit_ - entry >= 0.5e9
    deadline = time.monotonic() + 10  # the shard's answer to the publish can reach the peer after the result reaches us
    while a["published_unix_ns"] is None and time.monotonic() < deadline:
        time.sleep(0.05)
        a = f.explain()["attempts"][-1]
    assert a["published_unix_ns"] is not None and exit_ <= a["published_unix_ns"] <= time.time_ns(), a


def test_a_call_waiting_for_admission_shows_the_wait_after_it_was_queued_not_before():
    (me,) = [n for n in ch.runs.cluster().nodes() if n.this_peer]
    most = int(me.memory_bytes * 0.6)  # a reservation, never allocated: two of them do not fit at once
    hog = nap.options(memory=most).remote(2.0)
    deadline = time.monotonic() + 60
    while hog.explain()["state"] != "Running":  # not before it holds its reservation
        assert time.monotonic() < deadline and not hog.done(), hog.explain()
        time.sleep(0.02)
    f = nap.options(memory=most).remote(0)
    f.result(timeout=60)
    hog.result(timeout=60)
    a = f.explain()["attempts"][-1]
    ready, queued, dispatched, entry = (a[k] for k in ("ready_unix_ns", "queued_unix_ns", "dispatched_unix_ns", "py_entry_unix_ns"))
    assert ready <= queued <= dispatched <= entry, a
    assert queued - ready < 0.3e9, "its inputs were here at once: no wait before it was queued"
    assert dispatched - queued > 0.5e9, f"it waited in the queue for the hog's memory: {a['reservation']}"
