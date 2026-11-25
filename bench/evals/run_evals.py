"""Run every workload for both runtimes, each in a fresh process, and write
results.json (with the run's manifest, manifest.py) plus a markdown report.
The report is refused when the manifest lacks a runtime's version or the
local peer's durability class.

    python bench/evals/run_evals.py --out results/ [--only latency,tasks]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import manifest  # noqa: E402
from common import CORES  # noqa: E402

# Workload names, in order (both suites implement the same set).
WORKLOADS = [
    "latency", "tasks", "map_small", "dag_chains", "cpu_scaling", "large_object", "actor_calls", "nested",
    "worker_kill", "external_effect", "memory_limit", "accounting", "infeasible",
]

SUITES = {"ray": "ray_suite.py", "chimaera": "chimaera_suite.py"}


def interpreter(runtime: str) -> str:
    """Each runtime may live in its own interpreter (RAY_PYTHON, CHIMAERA_PYTHON)."""
    return os.environ.get(f"{runtime.upper()}_PYTHON") or sys.executable


def run_one(runtime: str, workload: str, timeout: float) -> dict:
    t0 = time.time()
    try:
        p = subprocess.run(
            [interpreter(runtime), os.path.join(HERE, SUITES[runtime]), workload],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=HERE,
        )
    except subprocess.TimeoutExpired:
        return {"runtime": runtime, "workload": workload, "error": f"timed out after {timeout:.0f} s"}
    lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
    if p.returncode != 0 or not lines:
        return {"runtime": runtime, "workload": workload, "error": (p.stderr or p.stdout)[-1500:]}
    rec = json.loads(lines[-1])
    rec["wall_s"] = round(time.time() - t0, 1)
    return rec


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results")
    ap.add_argument("--only", default="")
    ap.add_argument("--timeout", type=float, default=600)
    a = ap.parse_args()
    names = [n for n in WORKLOADS if not a.only or n in a.only.split(",")]
    os.makedirs(a.out, exist_ok=True)
    records = []
    for name in names:
        for runtime in ("ray", "chimaera"):
            rec = run_one(runtime, name, a.timeout)
            records.append(rec)
            print(json.dumps(rec), flush=True)
    # The local peer the Chimaera suite ran on (CHIMAERA_HOME, else the default home).
    home = os.environ.get("CHIMAERA_HOME") or os.path.join(os.path.expanduser("~"), ".chimaera", "local")
    binary = manifest.running_binary(home)
    durability = manifest.durability([manifest.status(binary, home)])
    for rec in records:
        if rec["runtime"] == "chimaera":
            rec["durability"] = durability
    meta = manifest.collect(binary, {rt: interpreter(rt) for rt in SUITES}, durability=durability)
    doc = {"manifest": meta, "results": records}
    with open(os.path.join(a.out, "results.json"), "w") as f:
        json.dump(doc, f, indent=2)
    manifest.validate(doc)
    v = meta["versions"]
    with open(os.path.join(a.out, "report.md"), "w") as f:
        pythons = ", ".join(f"{rt} {x}" for rt, x in v["python"].items())
        f.write(f"# Ray vs Chimaera, single node\n\n{meta['host']['platform']} · {CORES} cores · Python {pythons} · "
                f"Ray {v['ray']} · {v['chimaera']} · durability {durability['effective']}\n\n")
        f.write("| Workload | Ray | Chimaera |\n|---|---|---|\n")
        for name in names:
            cells = []
            for runtime in ("ray", "chimaera"):
                r = next(x for x in records if x["runtime"] == runtime and x["workload"] == name)
                cells.append(json.dumps(r.get("result", {"error": r.get("error", "")[:200]})).replace("|", "/"))
            f.write(f"| {name} | {cells[0]} | {cells[1]} |\n")
    print(f"wrote {a.out}/results.json and report.md")


if __name__ == "__main__":
    main()
