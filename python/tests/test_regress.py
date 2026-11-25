"""The quick lane's pinned regression run (bench/evals/regress.py): a run
passes against its machine's baseline, and fails when a workload's result is
wrong, an output digest or the pins differ, the build profile differs, or a
median is worse than its tolerance allows. A copy of the package that decodes
an int value one too high fails the lane."""

from __future__ import annotations

import copy
import glob
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

from conftest import BIN, REPO

sys.path.insert(0, os.path.join(REPO, "bench", "evals"))
import regress  # noqa: E402

DURABLE = {"requested": "machine", "effective": "machine", "raft_sync": ["fsync"], "degraded": []}
PINS = dict(regress.PINS, workloads=["tasks", "shuffle"])


def run_doc(tasks: list[float], shuffle: list[float], digest: str = "0f6c03ff760b0170") -> dict:
    def rec(workload: str, metric: str, samples: list[float], **extra) -> dict:
        result = {"metric": metric, "samples": samples, "unit": "u", "reps": len(samples), **extra}
        return {"runtime": "chimaera", "workload": workload, "durability": DURABLE, "result": result}

    return {
        "manifest": {
            "source": {"digest": "0" * 16},
            "build": {"profile": "release", "rustc": "rustc 1.94.1", "rustflags": ""},
            "versions": {"chimaera": "chimaera 0.1.0", "python": {"chimaera": "3.12.13"}},
            "order": {**{k: PINS[k] for k in ("seed", "rounds", "scale")}, "load_avg_after": [1, 1, 1]},
        },
        "results": [
            rec("tasks", "tasks_per_s", tasks),
            rec("shuffle", "seconds", shuffle, digests={digest: len(shuffle)}),
        ],
    }


def test_a_run_is_checked_against_its_machines_baseline():
    clean = run_doc([10_000, 10_200, 9_900], [0.50, 0.52, 0.49])
    base = regress.baseline_of(clean, PINS)
    assert base["workloads"]["tasks"] == {"metric": "tasks_per_s", "median": 10_000, "tolerance": 0.3, "digests": []}
    assert base["workloads"]["shuffle"]["digests"] == ["0f6c03ff760b0170"]
    assert regress.check(clean, PINS, base) == []
    # Within the tolerance: 80% of the throughput, 1.3x the time.
    assert regress.check(run_doc([8_000] * 3, [0.65] * 3), PINS, base) == []

    slower = regress.check(run_doc([6_000] * 3, [0.75] * 3), PINS, base)
    assert slower == [
        "tasks: tasks_per_s 6,000.000, below 70% of the baseline's 10,000.000",
        "shuffle: seconds 0.750, over 1.43x the baseline's 0.500",
    ]
    wrong = regress.check(run_doc([10_000] * 3, [0.5] * 3, digest="ffffffffffffffff"), PINS, base)
    assert wrong == ["shuffle: output digests ['ffffffffffffffff'], the baseline's ['0f6c03ff760b0170']"]

    failed = copy.deepcopy(clean)
    del failed["results"][0]["result"]
    failed["results"][0]["error"] = "AssertionError"
    assert regress.check(failed, PINS, base) == ["tasks: round 0 failed: AssertionError"]

    debug = copy.deepcopy(clean)
    debug["manifest"]["build"]["profile"] = "dev"
    assert regress.check(debug, PINS, base) == [
        "a dev peer's timings do not compare with the baseline's release build"
    ]
    other = dict(PINS, scale=0.1)
    assert regress.check(clean, other, base)[:2] == [
        f"the baseline was recorded with pins {PINS}, this run used {other}",
        f"the run did not keep its pins: {clean['manifest']['order']}",
    ]
    # A baseline of noisy runs carries their p10 to p90 spread as its tolerance, up to 0.9.
    for samples, tolerance in ([8e3, 9e3, 10e3, 14e3, 15e3], 0.6), ([1e3, 2e3, 10e3, 30e3, 40e3], 0.9):
        assert regress.baseline_of(run_doc(samples, [0.5] * 3), PINS)["workloads"]["tasks"]["tolerance"] == tolerance


def tree_copy(dst: str) -> str:
    """The files the lane runs from: the evals, the package (with its native
    module) and the cluster harness."""
    keep = shutil.ignore_patterns("__pycache__", "results")
    shutil.copytree(os.path.join(REPO, "bench", "evals"), os.path.join(dst, "bench", "evals"), ignore=keep)
    shutil.copytree(os.path.join(REPO, "python", "chimaera"), os.path.join(dst, "python", "chimaera"), ignore=keep)
    os.makedirs(os.path.join(dst, "python", "tests"))
    for name in ("conftest.py", "test_cluster.py"):
        shutil.copy(os.path.join(REPO, "python", "tests", name), os.path.join(dst, "python", "tests", name))
    for name in ("Cargo.toml", "rust-toolchain.toml", "pyproject.toml"):
        shutil.copy(os.path.join(REPO, name), os.path.join(dst, name))
    return dst


def lane(tree: str, *args: str) -> subprocess.CompletedProcess:
    script = os.path.join(tree, "bench", "evals", "regress.py")
    return subprocess.run(
        [sys.executable, script, "--only", "tasks,map_small,shuffle", *args],
        capture_output=True, text=True, timeout=280, env=dict(os.environ, CHIMAERA_BIN=BIN), stdin=subprocess.DEVNULL,
    )


@pytest.mark.slow
def test_a_corrupted_result_fails_the_quick_lane(tmp_path):
    base = str(tmp_path / "baseline.json")
    clean = tree_copy(str(tmp_path / "clean"))
    p = lane(clean, "--record", "--baseline", base, "--out", str(tmp_path / "clean-out"))
    assert p.returncode == 0, p.stdout[-3000:] + p.stderr[-3000:]
    with open(glob.glob(str(tmp_path / "clean-out" / "run" / "local*.json"))[0]) as f:
        doc = json.load(f)
    with open(base) as f:
        recorded = json.load(f)
    pins = recorded["pins"]
    assert pins["workloads"] == ["tasks", "map_small", "shuffle"] and regress.check(doc, pins, recorded) == []

    # The mutant: a value that decodes to an int comes out one too high.
    serde = os.path.join(tree_copy(str(tmp_path / "mutant")), "python", "chimaera", "_serde.py")
    with open(serde) as f:
        src = f.read()
    line = "        return pickle.loads(main, buffers=bufs)\n"
    assert src.count(line) == 1
    mutant = "        v = pickle.loads(main, buffers=bufs)\n        return v + 1 if type(v) is int else v\n"
    with open(serde, "w") as f:
        f.write(src.replace(line, mutant))
    p = lane(str(tmp_path / "mutant"), "--baseline", base, "--out", str(tmp_path / "mutant-out"))
    assert p.returncode == 1, p.stdout[-3000:] + p.stderr[-3000:]
    # Every round of the burst of calls reads its last result one too high, and
    # its output check stops the run (the map's and the shuffle's values are
    # containers, which this mutant leaves alone).
    wrong = re.compile(r"regress: FAIL tasks: round \d failed: AssertionError$")
    failed = [ln for ln in p.stdout.splitlines() if wrong.match(ln)]
    assert len(failed) == pins["rounds"], p.stdout[-3000:]
