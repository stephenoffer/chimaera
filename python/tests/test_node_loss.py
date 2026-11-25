"""The node-loss harness (bench/evals/node_loss.py) on a localhost cluster:
the peer leading the most shard groups is SIGKILLed while a call runs on it,
every result still reaches the driver exactly once, and the experiment
records when the loss was detected, when the lost calls ran again, the first
of their results and the last result."""

from __future__ import annotations

import os
import sys

import pytest

from conftest import REPO

sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import node_loss  # noqa: E402

TOTAL = {
    "tasks": node_loss.TASKS,
    "actors": node_loss.ACTORS * node_loss.ACTOR_CALLS,
    "shuffle": node_loss.PARTS,
}


@pytest.mark.slow
@pytest.mark.parametrize("workload", node_loss.WORKLOADS)
def test_losing_a_leading_peer_mid_call_keeps_every_result_and_times_the_recovery(workload):
    e = node_loss.experiment(seed=0, role="leader", workload=workload, point="running")
    assert e["exactly_once"] and e["results"] == TOTAL[workload], e
    assert e["started_on_victim"] >= 1, e
    assert 0 <= e["detected_s"] <= e["retired_s"] <= e["recovered_s"], e
    # The call running on the victim ran again elsewhere, and its result came back.
    assert e["redispatch_s"] and e["first_ok_s"] is not None, e
    assert min(e["redispatch_s"]) <= e["first_ok_s"] <= e["recovered_s"], e
    assert e["durability"]["effective"] == "machine", e


def test_the_report_pools_each_victim_role_over_workloads_and_kill_points():
    def run(role: str, s: float, workload: str = "tasks") -> dict:
        return {"workload": workload, "role": role, "point": "running", "victim_leads": 1, "exactly_once": True,
                "detected_s": s, "retired_s": s, "redispatch_s": [s], "first_ok_s": s, "recovered_s": s}

    m = {"host": {"platform": "p"}, "versions": {"chimaera": "c"}, "build": {"profile": "dev"},
         "source": {"digest": "d"}, "durability": {"effective": "machine"}, "order": {"seeds": 1}}
    failed = {"workload": "tasks", "role": "follower", "point": "running", "error": "x"}
    doc = {"manifest": m, "experiments": [run("leader", 1.0), run("leader", 2.0, "actors"), run("follower", 3.0), failed]}
    text = node_loss.report(doc)
    leader = " | ".join(["1.50 [1.90, 2.00]"] * 2 + ["1.50 [1.90, 2.00], 2 calls"] + ["1.50 [1.90, 2.00]"] * 2)
    assert f"| `leader` | {leader} |" in text, text  # p50 <= p90 <= max, both experiments in one row
    assert "| `follower` | 3.00 [3.00, 3.00] |" in text, text  # the failed experiment is left out
