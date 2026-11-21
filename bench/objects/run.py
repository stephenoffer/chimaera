"""Run the object-plane benchmark for each runtime and scenario in fresh
processes, collect the JSON rows, and write a comparison report.

    python bench/objects/run.py --out /mnt/local_storage/objbench/run1 \
        --runtimes ray,chimaera --scenarios put,get,return,pipeline,many
    python bench/objects/run.py --report /mnt/local_storage/objbench/run1

Ray connects to the cluster at RAY_ADDRESS (or starts one); Chimaera to the
peer whose home is CHIMAERA_HOME (started on demand when alone).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import time
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "objbench.py")


def run_one(runtime: str, scenario: str, args: argparse.Namespace, out: str) -> None:
    cmd = [sys.executable, BENCH, "--runtime", runtime, "--scenario", scenario, "--types", args.types,
           "--sizes", args.sizes, "--capacity", args.capacity]
    if runtime == "ray" and os.environ.get("RAY_ADDRESS"):
        cmd += ["--address", os.environ["RAY_ADDRESS"]]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout, check=False)
        stdout, stderr, code = p.stdout, p.stderr, p.returncode
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr, code = "timed out", -1
    rows = [json.loads(ln) for ln in stdout.splitlines() if ln.startswith("{")]
    with open(os.path.join(out, "results.jsonl"), "a") as f:
        for r in rows:
            f.write(json.dumps({"runtime": runtime, **r}) + "\n")
        if code != 0 or not any("done" in r for r in rows):
            f.write(json.dumps({"runtime": runtime, "scenario": scenario, "failed": code,
                                "stderr": stderr[-3000:]}) + "\n")
    print(f"{runtime:9s} {scenario:12s} rows={len(rows):4d} exit={code} {time.time() - t0:6.1f}s", flush=True)


def value(r: dict) -> float | None:
    for k in ("overhead_s", "median_s", "per_object_s", "seconds", "first_get_s"):
        v = r.get(k)
        if isinstance(v, (int, float)) and not math.isnan(v):
            return float(v)
    return None


def fmt(s: float | None) -> str:
    if s is None:
        return "-"
    if s < 1e-3:
        return f"{s * 1e6:.1f} µs"
    if s < 1:
        return f"{s * 1e3:.2f} ms"
    return f"{s:.2f} s"


def report(out: str) -> str:
    rows = [json.loads(ln) for ln in open(os.path.join(out, "results.jsonl"))]
    table: dict[tuple, dict[str, dict]] = defaultdict(dict)
    failures = []
    for r in rows:
        if "failed" in r:
            failures.append(r)
            continue
        if "op" not in r:
            continue
        table[(r["scenario"], r["kind"], r["size"], r["op"])][r["runtime"]] = r
    lines = ["| scenario | type | size | op | Ray | Chimaera | speedup |", "|---|---|---|---|---:|---:|---:|"]
    ratios = defaultdict(list)
    for key in table:
        ray, chm = table[key].get("ray"), table[key].get("chimaera")
        rv, cv = (value(ray) if ray else None), (value(chm) if chm else None)
        note = ""
        for r in (ray, chm):
            if r and r.get("error"):
                note += f" {r['runtime']}: {r['error'][:80]}"
        sp = f"{rv / cv:.1f}x" if rv and cv else "-"
        if rv and cv:
            ratios[key[0]].append(rv / cv)
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {fmt(rv)} | {fmt(cv)} | {sp}{note} |")
    summary = ["| scenario | rows | geomean speedup | min | max |", "|---|---:|---:|---:|---:|"]
    for sc, rs in ratios.items():
        g = math.exp(sum(math.log(x) for x in rs) / len(rs))
        summary.append(f"| {sc} | {len(rs)} | {g:.1f}x | {min(rs):.2f}x | {max(rs):.1f}x |")
    text = "\n".join(["# Object plane: Ray vs Chimaera", "", f"Host: {platform.node()} {platform.machine()}, "
                      f"{os.cpu_count()} CPUs. Speedup = Ray time / Chimaera time (above 1: Chimaera faster). "
                      "Times are medians; `return`/`pipeline` rows subtract the same call carrying None.", "",
                      "## Summary", "", *summary, "", "## All rows", "", *lines])
    if failures:
        text += "\n\n## Failures\n\n" + "\n".join(f"- {f['runtime']} {f['scenario']}: exit {f['failed']}: "
                                                   f"`{f['stderr'][-400:]}`" for f in failures)
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write(text + "\n")
    return text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    ap.add_argument("--report", help="only write the report for this results directory")
    ap.add_argument("--runtimes", default="ray,chimaera")
    ap.add_argument("--scenarios", default="put,get,return,pipeline,many")
    ap.add_argument("--types", default="bytes,numpy,torch,arrow,pandas,pydict,pylist,str,nested")
    ap.add_argument("--sizes", default="1K,64K,1M,16M,256M,1G")
    ap.add_argument("--capacity", default="8G")
    ap.add_argument("--timeout", type=int, default=1800)
    args = ap.parse_args()
    if args.report:
        print(report(args.report))
        return
    os.makedirs(args.out, exist_ok=True)
    for scenario in args.scenarios.split(","):
        for runtime in args.runtimes.split(","):
            run_one(runtime, scenario, args, args.out)
    print(report(args.out))


if __name__ == "__main__":
    main()
