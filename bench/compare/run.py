"""Chimaera against Ray, Dask, multiprocessing and joblib on one machine.

    python bench/compare/run.py --out results/ [--trials 3] [--only tasks,big_arg] [--runtimes chimaera,ray]

Every runtime runs in this interpreter, so all of them use the same Python.
Ray, Dask and Chimaera each get one cluster for the whole run, sized to the
machine (Ray and Dask held by a child of this process, Chimaera a local peer
in its own home), and every workload runs in a fresh driver process attached
to it. multiprocessing and joblib start their pools in that process. Each
trial runs every workload once per runtime, the workloads and the runtimes
in a seeded random order, and the load average is read before and after
every run. `startup` starts a new runtime each time instead.

Writes results.json (machine, versions, order, every sample) and report.md.
`--ray-address auto` uses a Ray cluster already running (an Anyscale node).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
from workloads import CPUS, RUNTIMES, SUPPORT, UNITS  # noqa: E402

ORDER = ["startup", "latency", "tasks", "map_small", "fine_grained", "big_arg", "broadcast", "chain", "nested",
         "actors", "worker_crash", "many_args", "many_returns", "get_many", "get_many_np", "queued", "large_object",
         "many_actors"]
STARTUP_REPS = 3

# A new cluster of its own ("local"), even where another Ray is running here.
RAY_HOLDER = """
import os, sys, ray
ray.init(address="local", num_cpus=os.cpu_count(), include_dashboard=False, log_to_driver=False)
print(ray.get_runtime_context().gcs_address, flush=True)
sys.stdin.read()
ray.shutdown()
"""
DASK_HOLDER = """
import os, sys
from distributed import LocalCluster
c = LocalCluster(n_workers=os.cpu_count(), threads_per_worker=1, dashboard_address=None)
print(c.scheduler_address, flush=True)
sys.stdin.read()
c.close()
"""
CHIMAERA_WARM = "import chimaera as ch; ch.init(); print(ch.remote(abs).remote(-1).result())"


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()[:16]


def machine() -> dict:
    mem = 0
    if sys.platform == "darwin":
        mem = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout)
        cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
    else:
        with open("/proc/meminfo") as f:
            mem = int(f.readline().split()[1]) * 1024
        cpu = next((ln.split(":", 1)[1].strip() for ln in open("/proc/cpuinfo") if ln.startswith("model name")), "?")
    return {"platform": platform.platform(), "cpu": cpu, "cpus": CPUS, "memory_gib": round(mem / 2**30, 1),
            "python": platform.python_version()}


def run_child(runtime: str, workload: str, env: dict) -> dict:
    t0 = time.time()
    load0 = os.getloadavg()[0]
    try:
        p = subprocess.run([sys.executable, os.path.join(HERE, "workloads.py"), runtime, workload],
                           capture_output=True, text=True, timeout=900, env=env, stdin=subprocess.DEVNULL)
        lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
        rec = json.loads(lines[-1]) if p.returncode == 0 and lines else {"error": (p.stderr or p.stdout)[-2000:]}
    except subprocess.TimeoutExpired:
        rec = {"error": "timed out after 900 s"}
    rec.update({"runtime": runtime, "workload": workload, "wall_s": round(time.time() - t0, 1),
                "load_before": round(load0, 1), "load_after": round(os.getloadavg()[0], 1)})
    return rec


def median(xs: list[float]) -> float:
    return statistics.median(xs)


def fmt(v: float, unit: str) -> str:
    if unit in ("calls/s", "items/s", "steps/s"):
        return f"{v:,.0f}/s"
    if unit == "us":
        return f"{v:,.0f} µs"
    if unit == "s":
        return f"{v:.3f} s" if v < 10 else f"{v:.1f} s"
    return f"{v:.2f}"


def summarize(records: list[dict]) -> dict:
    """{workload: {runtime: {"median", "samples", "trials": [per-trial medians], "outcome"}}}."""
    out: dict = {}
    for r in records:
        cell = out.setdefault(r["workload"], {}).setdefault(r["runtime"], {"samples": [], "trials": [], "errors": []})
        if "error" in r:
            cell["errors"].append(r["error"][-300:])
            continue
        xs = [x for x in r["samples"] if x is not None]
        if not xs:  # the runtime's own failure (worker_crash): keep what happened
            cell["outcome"] = f"{r.get('outcome')} after {r.get('after_s')} s"
            continue
        cell["samples"] += xs
        cell["trials"].append(median(xs))
    for row in out.values():
        for cell in row.values():
            if cell["samples"]:
                cell["median"] = median(cell["samples"])
    return out


def report(doc: dict) -> str:
    m, s = doc["machine"], doc["summary"]
    rts = doc["runtimes"]
    lines = [
        f"# Chimaera against {', '.join(r for r in rts if r != 'chimaera')} on one machine ({doc['date']})", "",
        f"{m['cpu']}, {m['cpus']} CPUs, {m['memory_gib']} GiB, {m['platform']}, Python {m['python']}.",
        "Versions: " + "; ".join(f"{k}: {v}" for k, v in doc["versions"].items()) + ".",
        f"Chimaera at commit {doc['commit']}, peer binary sha256 {doc['peer_sha256']} ({doc['profile']} build).",
        f"{doc['trials']} trials in a random order (seed {doc['seed']}), at least {doc['min_seconds']} s of timed "
        f"runs per cell after a warm-up, scale {doc['scale']}. Cells: the median of every timed run; the range "
        "after it is the lowest and highest per-trial median. The load average (1 min) ran "
        f"{doc['load']['min']}–{doc['load']['max']} (median {doc['load']['median']}).", "",
        "| Workload | " + " | ".join(rts) + " | Chimaera vs the next best |",
        "|---|" + "---:|" * len(rts) + "---|",
    ]
    for w in doc["workloads"]:
        unit, better, _ = UNITS[w]
        row = s.get(w, {})
        cells, vals = [], {}
        for rt in rts:
            c = row.get(rt)
            if rt not in SUPPORT[w]:
                cells.append("n/a")
            elif not c:
                cells.append("–")
            elif "median" in c:
                vals[rt] = c["median"]
                lo, hi = min(c["trials"]), max(c["trials"])
                rng = f" ({fmt(lo, unit)}–{fmt(hi, unit)})" if len(c["trials"]) > 1 else ""
                cells.append(f"{fmt(c['median'], unit)}{rng}")
            elif "outcome" in c:
                cells.append(c["outcome"])
            else:
                cells.append("error")
        verdict = ""
        if "chimaera" in vals:
            others = {k: v for k, v in vals.items() if k != "chimaera"}
            if others:
                best = (max if better == "higher" else min)(others, key=others.get)
                ratio = vals["chimaera"] / others[best] if better == "higher" else others[best] / vals["chimaera"]
                verdict = f"{ratio:.2f}x {best}" if ratio >= 1 else f"{1 / ratio:.2f}x slower than {best}"
        lines.append(f"| {w} | " + " | ".join(cells) + f" | {verdict} |")
    lines += ["", "What each row measures (n/a: the runtime has no way to write it):", ""]
    lines += [f"- `{w}` ({UNITS[w][0]}, {UNITS[w][1]} is better): {UNITS[w][2]}" for w in doc["workloads"]]
    errs = [(w, rt, c["errors"][0]) for w, row in s.items() for rt, c in row.items() if c["errors"]]
    if errs:
        lines += ["", "Errors:", ""] + [f"- {w} on {rt}: `{e.strip().splitlines()[-1][:200]}`" for w, rt, e in errs]
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--only", default="")
    ap.add_argument("--runtimes", default=",".join(RUNTIMES))
    ap.add_argument("--ray-address", help="use this Ray cluster instead of starting one (`auto` on Anyscale)")
    ap.add_argument("--commit", default="", help="the commit the Chimaera build came from")
    ap.add_argument("--profile", default="release")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rts = [r for r in a.runtimes.split(",") if r]
    names = [w for w in ORDER if not a.only or w in a.only.split(",")]
    seed = a.seed if a.seed is not None else random.randrange(1 << 31)
    rng = random.Random(seed)
    tmp = tempfile.mkdtemp(prefix="cmp-")
    env = dict(os.environ, COMPARE_TMP=tmp, PYTHONUNBUFFERED="1")
    holders = []

    def hold(code: str) -> str:
        p = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                             env=env)
        holders.append(p)
        return p.stdout.readline().strip()

    binary = ""
    if "chimaera" in rts:
        from chimaera import _runtime

        binary = _runtime._peer_binary()
    home = os.path.join(tmp, "peer")
    try:
        if "ray" in rts and a.ray_address:
            env["RAY_ADDRESS"] = a.ray_address
        elif "ray" in rts:
            # A local Ray 2.60 cluster wants a per-cluster token from every
            # driver; it guards connections only, so the run turns it off.
            env["RAY_AUTH_MODE"] = "disabled"
            env["RAY_ADDRESS"] = hold(RAY_HOLDER)
        if "dask" in rts:
            env["DASK_SCHEDULER"] = hold(DASK_HOLDER)
        if "chimaera" in rts:
            env["CHIMAERA_HOME"] = home
            subprocess.run([sys.executable, "-c", CHIMAERA_WARM], env=env, check=True, timeout=120)
        records = []
        for trial in range(a.trials):
            for w in rng.sample(names, len(names)):
                for rt in rng.sample(rts, len(rts)):
                    if rt not in SUPPORT[w]:
                        continue
                    for _ in range(STARTUP_REPS if w == "startup" else 1):
                        rec = run_child(rt, w, env)
                        rec["trial"] = trial
                        records.append(rec)
                        shown = rec.get("samples") or rec.get("error", "")[-200:]
                        print(f"[{trial}] {w:13s} {rt:16s} load {rec['load_before']:5} {shown}", flush=True)
    finally:
        for p in holders:
            p.stdin.close()
            try:
                p.wait(timeout=120)
            except subprocess.TimeoutExpired:
                p.kill()
        if "chimaera" in rts:
            subprocess.run([binary, "--dir", home, "stop"], capture_output=True, timeout=60, check=False)

    versions = {}
    for r in records:
        if "version" in r:
            versions.setdefault(r["runtime"], r["version"])
    loads = [r["load_before"] for r in records] + [r["load_after"] for r in records]
    doc = {
        "date": time.strftime("%Y-%m-%d"), "machine": machine(), "versions": versions, "commit": a.commit,
        "peer_sha256": sha256(binary) if binary else "", "profile": a.profile, "runtimes": rts, "workloads": names,
        "trials": a.trials, "seed": seed, "scale": float(os.environ.get("COMPARE_SCALE", "1")),
        "min_seconds": float(os.environ.get("COMPARE_MIN_SECONDS", "10")),
        "load": {"min": min(loads), "max": max(loads), "median": median(loads)},
        "records": records,
    }
    doc["summary"] = summarize(records)
    with open(os.path.join(a.out, "results.json"), "w") as f:
        json.dump(doc, f, indent=1)
    text = report(doc)
    with open(os.path.join(a.out, "report.md"), "w") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    main()
