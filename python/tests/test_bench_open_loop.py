"""The open-loop harness (bench/evals/open_loop.py): arrivals follow its clock
whatever the runtime does, its percentiles and stage splits are computed from
the attempt times explain() returns, and past saturation it reports a growing
backlog rather than a plateau."""

from __future__ import annotations

import os
import random
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import open_loop  # noqa: E402


@pytest.mark.parametrize("arrival", ["poisson", "burst"])
def test_arrivals_keep_the_offered_rate_inside_the_window(arrival):
    t = open_loop.schedule(arrival, 200.0, 30.0, random.Random(7))
    assert t == sorted(t) and all(0 <= x < 30.0 for x in t)
    assert abs(len(t) / 30.0 - 200.0) < 200.0 * 0.05


def test_bursts_arrive_together_and_poisson_gaps_vary():
    burst = open_loop.schedule("burst", 100.0, 2.0, random.Random(1), burst=50)
    assert burst == [0.0] * 50 + [0.5] * 50 + [1.0] * 50 + [1.5] * 50
    gaps = [b - a for a, b in zip(*[iter(open_loop.schedule("poisson", 100.0, 5.0, random.Random(1)))] * 2)]
    assert len(set(round(g, 6) for g in gaps)) > len(gaps) // 2
    assert open_loop.schedule("poisson", 0, 5.0, random.Random(1)) == []
    with pytest.raises(ValueError):
        open_loop.schedule("closed", 1.0, 1.0, random.Random(1))


def test_percentiles_by_nearest_rank():
    xs = [float(i) for i in range(1, 101)]
    assert open_loop.percentile(xs, 0.5) == 50.0 and open_loop.percentile(xs, 0.99) == 99.0
    assert open_loop.percentile([3.0], 0.99) == 3.0 and open_loop.percentile([], 0.5) is None


def test_stage_splits_come_from_the_attempts_times_and_say_none_when_one_is_missing():
    s = 1_000_000_000
    a = {
        "ready_unix_ns": 11 * s, "queued_unix_ns": 12 * s, "dispatched_unix_ns": 15 * s,
        "py_entry_unix_ns": 15 * s + s // 10, "py_exit_unix_ns": 17 * s, "published_unix_ns": None,
    }
    got = open_loop.stages(a, offered_ns=10 * s)
    assert got["arrival_to_ready"] == 1.0 and got["ready_to_queued"] == 1.0
    assert got["queued_to_dispatched"] == 3.0 and got["dispatched_to_entry"] == pytest.approx(0.1)
    assert got["run"] == pytest.approx(1.9) and got["exit_to_published"] is None


def test_every_row_names_its_arrival_process_and_offered_rate_and_both_waits():
    r = {
        "arrival": "burst", "offered_rate": 160.0, "offered": 10, "finished_in_window": 4, "backlog_at_window_end": 6,
        "unfinished_after_drain": 0, "arrival_to_start_s_p50": 0.5, "arrival_to_start_s_p99": 2.0,
        "arrival_to_done_s_p99": 2.5, "stages_p99_s": {"queued_to_dispatched": 0.25, "dispatched_to_entry": 1.5},
        "driver_late_p99_s": 0.0,
    }
    header, _, row = open_loop.table([r]).splitlines()
    assert "queued→dispatched" in header and "dispatched→start" in header
    assert row.startswith("| burst | 160 | 10 | 4 | 6 | 0 |") and "| 0.25 | 1.5 |" in row
