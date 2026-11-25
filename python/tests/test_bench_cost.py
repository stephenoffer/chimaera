"""What a cluster run's workloads cost (bench/evals/cost.py): instance-hours,
dollars, core-hours and GPU-hours recomputed from the run's recorded wall
times and machines and a dated price table, which the result carries, and
useful completions counted only for repetitions within the workload's SLO,
the same bound for both runtimes."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from conftest import REPO

sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import cost  # noqa: E402

with open(os.path.join(REPO, "bench", "evals", "prices.json")) as f:
    PRICES = json.load(f)


def rec(runtime: str, workload: str, metric: str, samples: list[float], wall_s: float, **extra) -> dict:
    result = {"metric": metric, "samples": samples, "reps": len(samples), "executions": len(samples) + 1, **extra}
    return {"runtime": runtime, "workload": workload, "wall_s": wall_s, "result": result}


def run() -> dict:
    burst = 200_000  # calls per repetition of `tasks`, warm-up included in ops
    return {
        "manifest": {"topology": {"instances": {"c7i.4xlarge": 4}, "region": "us-east-1"}},
        "results": [
            rec("ray", "tasks", "tasks_per_s", [6_000, 900, 7_000], 30.0, ops=4 * burst),
            rec("chimaera", "tasks", "tasks_per_s", [13_000, 14_000, 12_500], 20.0, ops=4 * burst),
            {**rec("chimaera", "tasks", "tasks_per_s", [17_000] * 3, 15.0, ops=4 * burst),
             "variant": {"CHIMAERA_RAFT_SYNC": "none"}},
            rec("ray", "shuffle", "seconds", [0.15, 5.0, 0.16], 25.0),
            {"runtime": "chimaera", "workload": "shuffle", "error": "timed out", "wall_s": 1200.0},
            rec("ray", "map_small", "naive_items_per_s", [6_000] * 3, 40.0, hand_batched_items_per_s=5e6),
        ],
    }


def test_costs_are_recomputed_from_recorded_instance_hours_and_unit_prices(tmp_path):
    c = cost.costs(run(), PRICES)
    rows = {(r["workload"], r["runtime"], r["variant"]): r for r in c["rows"]}
    usd, vcpus = PRICES["instances"]["c7i.4xlarge"]["per_hour"], PRICES["instances"]["c7i.4xlarge"]["vcpus"]

    ray = rows[("tasks", "ray", "")]
    assert ray["instance_hours"] == pytest.approx(30 / 3600 * 4)
    assert ray["dollars"] == pytest.approx(30 / 3600 * 4 * usd)
    assert ray["core_hours"] == pytest.approx(30 / 3600 * 4 * vcpus) and ray["gpu_hours"] == 0
    # The repetition at 900 calls/s missed the 1,000 calls/s SLO: two bursts count.
    assert ray["useful_completions"] == 2 * 200_000
    assert ray["dollars_per_completion"] == pytest.approx(ray["dollars"] / 400_000)
    assert rows[("tasks", "chimaera", "")]["useful_completions"] == 3 * 200_000
    assert rows[("tasks", "chimaera", "CHIMAERA_RAFT_SYNC=none")]["wall_s"] == 15.0
    # A timed workload completes one job per repetition within its bound (4.2 s).
    assert rows[("shuffle", "ray", "")]["useful_completions"] == 2
    # A failed run costs its hours and completes nothing.
    failed = rows[("shuffle", "chimaera", "")]
    assert failed["failed"] == 1 and failed["useful_completions"] == 0 and failed["dollars_per_completion"] is None
    assert failed["dollars"] == pytest.approx(1200 / 3600 * 4 * usd)
    assert "hand" in rows[("map_small", "ray", "")]["not_equivalent"]

    # The result carries its dated price table: the costs follow from it and the
    # recorded hours alone.
    stored = json.loads(json.dumps(c))
    assert stored["prices"]["date"] == PRICES["date"] and stored["slo"]["shuffle"] == 4.2
    for r in stored["rows"]:
        hours = r["wall_s"] / 3600
        machines = stored["instances"].items()
        assert r["dollars"] == pytest.approx(hours * sum(n * stored["prices"]["instances"][t]["per_hour"]
                                                         for t, n in machines))
        assert r["core_hours"] == pytest.approx(hours * sum(n * stored["prices"]["instances"][t]["vcpus"]
                                                            for t, n in machines))

    gpus = cost.costs(run(), PRICES, {"g5.12xlarge": 1})
    assert gpus["rows"][0]["gpu_hours"] == pytest.approx(gpus["rows"][0]["wall_s"] / 3600 * 4)
    with pytest.raises(ValueError, match=r"types with no price: \['m7i.large'\]"):
        cost.costs(run(), PRICES, {"m7i.large": 2})
    unrecorded = run()
    del unrecorded["manifest"]["topology"]["instances"]
    with pytest.raises(ValueError, match="names no instance types"):
        cost.costs(unrecorded, PRICES)

    # The command line prices a run's raw results the same way.
    path = tmp_path / "multi_results.json"
    path.write_text(json.dumps(run()))
    out = tmp_path / "cost.json"
    p = subprocess.run([sys.executable, os.path.join(REPO, "bench", "evals", "cost.py"), str(path), "--json", str(out)],
                       capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stderr
    assert json.loads(out.read_text())["rows"] == stored["rows"]
    assert f"Machines: 4 x c7i.4xlarge in us-east-1. Prices: {PRICES['source']}, {PRICES['date']}." in p.stdout
    assert "| tasks | ray | 0.033 | 0.02 | 0.53 | 0.00 | ≥ 1,000 tasks_per_s | 400,000 | 5.95e-08 |" in p.stdout
