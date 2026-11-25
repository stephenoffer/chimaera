"""The quick lane's pinned regression run: a fixed set of workloads on three
local peers at a fixed size, seed and number of rounds (PINS), checked against
the baseline this machine recorded (baselines/<machine>.json).

A run fails when any workload's run failed (each one checks its own output),
when an output digest differs from the baseline's, when the run was made with
other pins or another build profile than the baseline, or when a median is
worse than the baseline's by more than that workload's tolerance. Timings
compare only on one machine, so a machine needs a baseline of its own: record
it from a run on a quiet machine, with a release peer, and commit it.

    CHIMAERA_BIN=target/release/chimaera python bench/evals/regress.py [--record]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from typing import Any

import manifest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PINS = {
    "workloads": [
        "tasks", "tasks_serial", "map_small", "actors", "actors_peer", "nested", "shuffle", "shuffle_unfused",
    ],
    "scale": 0.05,
    "rounds": 3,
    "seed": 20261124,
    "min_seconds": 1.0,
}
# How much worse than its baseline a median may be: throughput down to 70%,
# time up to 1/0.7. A baseline whose own runs spread wider records that spread.
TOLERANCE = 0.3
HIGHER_IS_BETTER = {"tasks_per_s", "calls_per_s", "naive_items_per_s", "efficiency"}


def machine() -> str:
    """What a baseline is kept for: timings compare only on one kind of machine."""
    return f"{platform.system().lower()}-{platform.machine()}-{os.cpu_count()}cpu"


def run(pins: dict[str, Any], out: str) -> dict[str, Any]:
    """Run local_cluster.py with `pins`; its document (manifest and raw results)."""
    shutil.rmtree(out, ignore_errors=True)
    os.makedirs(out)
    env = dict(os.environ, EVAL_SCALE=str(pins["scale"]), EVAL_MIN_SECONDS=str(pins["min_seconds"]),
               PYTHONPATH=os.path.join(REPO, "python"))
    cmd = [sys.executable, os.path.join(HERE, "local_cluster.py"), "--rounds", str(pins["rounds"]),
           "--seed", str(pins["seed"]), "--only", ",".join(pins["workloads"]), "--out", out]
    p = subprocess.run(cmd, env=env, stdin=subprocess.DEVNULL, check=False)
    found = glob.glob(os.path.join(out, "local*.json"))
    if p.returncode != 0 or not found:
        raise SystemExit(f"regress: local_cluster.py exited {p.returncode} without its results")
    with open(found[0]) as f:
        return json.load(f)


def failed_runs(doc: dict[str, Any], workloads: list[str]) -> list[str]:
    """Every run that failed, with the last line it printed (the exception a
    child's output check raised), and every pinned workload that never ran."""
    out = []
    for r in doc["results"]:
        if "error" in r:
            last = ([ln.strip() for ln in r["error"].splitlines() if ln.strip()] or ["no output"])[-1]
            out.append(f"{r['workload']}: round {r.get('round', 0)} failed: {last[:300]}")
    ran = {r["workload"] for r in doc["results"]}
    return out + [f"{w}: did not run" for w in workloads if w not in ran]


def summaries(doc: dict[str, Any], workloads: list[str]) -> dict[str, dict[str, Any]]:
    """Each workload's summary over the runs that finished."""
    rows = manifest.table(doc, workloads)
    return {row["workload"]: row["chimaera"] for row in rows if "error" not in row["chimaera"]}


def baseline_of(doc: dict[str, Any], pins: dict[str, Any]) -> dict[str, Any]:
    """A baseline from a run that passed its own checks: each median, its
    output digests, and a tolerance no narrower than the run's own spread."""
    failed = failed_runs(doc, pins["workloads"])
    if failed:
        raise SystemExit("regress: not recording a baseline from a run that failed: " + "; ".join(failed))
    m = doc["manifest"]
    rows = {}
    for w, s in summaries(doc, pins["workloads"]).items():
        spread = (s["p90"] - s["p10"]) / s["median"] if s["median"] else 0.0
        tolerance = round(min(0.9, max(TOLERANCE, spread)), 2)
        rows[w] = {"metric": s["metric"], "median": s["median"], "tolerance": tolerance,
                   "digests": sorted(s.get("digests", {}))}
    return {
        "machine": machine(),
        "recorded": time.strftime("%Y-%m-%d"),
        "pins": pins,
        "build": {k: m["build"][k] for k in ("profile", "rustc", "rustflags")},
        "source": m["source"],
        "versions": m["versions"],
        "load_avg_after": m["order"]["load_avg_after"],
        "workloads": rows,
    }


def check(doc: dict[str, Any], pins: dict[str, Any], base: dict[str, Any]) -> list[str]:
    """Why this run fails against `base`; empty when it passes."""
    order, build = doc["manifest"]["order"], doc["manifest"]["build"]
    failures = []
    if base["pins"] != pins:
        failures.append(f"the baseline was recorded with pins {base['pins']}, this run used {pins}")
    if (order["scale"], order["rounds"], order["seed"]) != (pins["scale"], pins["rounds"], pins["seed"]):
        failures.append(f"the run did not keep its pins: {order}")
    if build["profile"] != base["build"]["profile"]:
        failures.append(f"a {build['profile']} peer's timings do not compare with the baseline's "
                        f"{base['build']['profile']} build")
    failures += failed_runs(doc, pins["workloads"])
    for w, s in summaries(doc, pins["workloads"]).items():
        want = base["workloads"].get(w)
        if not want:
            failures.append(f"{w}: not in the baseline")
            continue
        digests = sorted(s.get("digests", {}))
        if digests != want["digests"]:
            failures.append(f"{w}: output digests {digests}, the baseline's {want['digests']}")
        got, ref, tol = s["median"], want["median"], want["tolerance"]
        if s["metric"] in HIGHER_IS_BETTER and got < ref * (1 - tol):
            failures.append(f"{w}: {s['metric']} {got:,.3f}, below {1 - tol:.0%} of the baseline's {ref:,.3f}")
        elif s["metric"] not in HIGHER_IS_BETTER and got > ref / (1 - tol):
            failures.append(f"{w}: {s['metric']} {got:,.3f}, over {1 / (1 - tol):.2f}x the baseline's {ref:,.3f}")
    return failures


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--record", action="store_true", help="write this machine's baseline from the run")
    ap.add_argument("--baseline", default=os.path.join(HERE, "baselines", f"{machine()}.json"))
    ap.add_argument("--out", default=os.path.join(REPO, "dist", "regress"))
    ap.add_argument("--only", default="", help="a subset of the pinned workloads (a baseline holds one set)")
    a = ap.parse_args()
    pins = dict(PINS, workloads=[w for w in PINS["workloads"] if not a.only or w in a.only.split(",")])
    if not a.record and not os.path.exists(a.baseline):
        raise SystemExit(f"regress: no baseline for {machine()} at {a.baseline}; record one on this machine "
                         "with `make regress-record` and commit it")
    doc = run(pins, os.path.join(a.out, "run"))
    if a.record:
        base = baseline_of(doc, pins)
        os.makedirs(os.path.dirname(a.baseline), exist_ok=True)
        with open(a.baseline, "w") as f:
            json.dump(base, f, indent=2)
            f.write("\n")
        print(f"regress: recorded {a.baseline}")
        return
    with open(a.baseline) as f:
        base = json.load(f)
    failures = check(doc, pins, base)
    with open(os.path.join(a.out, "regress.json"), "w") as f:
        json.dump({"machine": machine(), "pins": pins, "baseline": os.path.relpath(a.baseline, REPO),
                   "failures": failures}, f, indent=2)
    for line in failures:
        print("regress: FAIL", line)
    print(f"regress: {'FAIL' if failures else 'PASS'} against {os.path.relpath(a.baseline, REPO)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
