"""Three-node Ray vs three-peer Chimaera on ONE machine, same workloads as
``multi_node.py`` (it runs that file's per-workload children).

Both get the same split of this machine: 3 nodes of ``--cpus-per-node`` CPUs
(Ray through ``ray.cluster_utils.Cluster``, Chimaera through three peers with
``--cpus``). Everything crosses real sockets between node processes, but
nodes share one machine's cores, memory and loopback network: this measures
protocol and scheduling overhead, NOT scaling. ``node_loss`` is left out
(it kills a node by host).

    .venv/bin/python bench/evals/local_cluster.py --ray-python .venv-bench/bin/python --rounds 3
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
WORKLOADS = ["tasks", "cpu_scaling", "map_small", "broadcast", "actors", "nested", "shuffle"]

RAY_HOLDER = """
import sys
from ray.cluster_utils import Cluster
c = Cluster(initialize_head=True, head_node_args={{"num_cpus": {cpus}, "include_dashboard": False}})
for _ in range({nodes} - 1):
    c.add_node(num_cpus={cpus})
c.wait_for_nodes()
print(c.address, flush=True)
sys.stdin.read()  # hold the cluster until the parent closes stdin
c.shutdown()
"""


def start_chimaera(nodes: int, cpus: int):
    if cpus != 4:
        raise SystemExit("the cluster harness gives every peer 4 CPUs; use --cpus-per-node 4")
    sys.path.insert(0, os.path.join(REPO, "python", "tests"))
    import test_cluster  # the tested multi-peer harness

    return test_cluster.Cluster(nodes)


def run_child(python: str, runtime: str, workload: str, env: dict[str, str]) -> dict:
    t0 = time.time()
    try:
        p = subprocess.run(
            [python, os.path.join(HERE, "multi_node.py"), "--workload", workload, "--runtime", runtime],
            capture_output=True,
            text=True,
            timeout=900,
            env=env,
        )
        lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
        if p.returncode == 0 and lines:
            rec = json.loads(lines[-1])
        else:
            rec = {"runtime": runtime, "workload": workload, "error": (p.stderr or p.stdout)[-1500:]}
    except subprocess.TimeoutExpired:
        rec = {"runtime": runtime, "workload": workload, "error": "timed out"}
    rec["wall_s"] = round(time.time() - t0, 1)
    return rec


def main() -> None:
    """Start both clusters, run each workload against each runtime for the
    given rounds (alternating which goes first), then write the medians."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--ray-python", required=True, help="a Python with Ray installed")
    ap.add_argument(
        "--chimaera-python",
        default=sys.executable,
        help="the Python for Chimaera's drivers (and so its executors); give the Ray one to compare on one interpreter",
    )
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--cpus-per-node", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    a = ap.parse_args()
    # Stopped from outside: still run the cleanup below, so only this run's nodes go away.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    names = [w for w in WORKLOADS if not a.only or w in a.only.split(",")]
    os.makedirs(a.out, exist_ok=True)

    holder = subprocess.Popen(
        [a.ray_python, "-c", RAY_HOLDER.format(cpus=a.cpus_per_node, nodes=a.nodes)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    ray_address = holder.stdout.readline().strip()
    cluster = start_chimaera(a.nodes, a.cpus_per_node)
    time.sleep(3)  # founders joined and elected
    envs = {
        "ray": dict(os.environ, RAY_ADDRESS=ray_address),
        "chimaera": dict(os.environ, CHIMAERA_HOME=cluster.dir(0), PYTHONPATH=os.path.join(REPO, "python")),
    }
    pythons = {"ray": a.ray_python, "chimaera": a.chimaera_python}
    records = []
    try:
        for rnd in range(a.rounds):
            order = ("ray", "chimaera") if rnd % 2 == 0 else ("chimaera", "ray")
            for w in names:
                for rt in order:
                    rec = run_child(pythons[rt], rt, w, envs[rt])
                    rec["round"] = rnd
                    print(json.dumps(rec), flush=True)
                    records.append(rec)
    finally:
        cluster.close()
        holder.stdin.close()
        holder.wait(timeout=120)

    def median(rt: str, w: str) -> dict:
        ok = [r["result"] for r in records if r["runtime"] == rt and r["workload"] == w and "result" in r]
        if not ok:
            errs = [r.get("error", "")[:200] for r in records if r["runtime"] == rt and r["workload"] == w]
            return {"error": errs[0] if errs else "no runs"}
        keys = [k for k, v in ok[0].items() if isinstance(v, (int, float))]
        return {k: statistics.median(r[k] for r in ok) for k in keys} | {"runs": len(ok)}

    stamp = time.strftime("%Y-%m-%d")
    summary = {w: {rt: median(rt, w) for rt in ("ray", "chimaera")} for w in names}
    meta = {
        "machine": f"{platform.platform()} {os.cpu_count()} cpus",
        "nodes": a.nodes,
        "cpus_per_node": a.cpus_per_node,
        "rounds": a.rounds,
        "pythons": {rt: subprocess.run([py, "-c", "import platform; print(platform.python_version())"], capture_output=True, text=True).stdout.strip() for rt, py in pythons.items()},
        "load_avg_after": os.getloadavg(),
    }
    with open(os.path.join(a.out, f"local{a.nodes}-{stamp}.json"), "w") as f:
        json.dump({"meta": meta, "summary": summary, "records": records}, f, indent=2)
    rows = [
        f"# Ray vs Chimaera, {a.nodes} nodes on one machine ({stamp})",
        "",
        "Protocol and scheduling overhead only: every node shares one machine's cores and loopback network.",
        f"{meta['machine']}; {a.nodes} x {a.cpus_per_node} CPUs; median of {a.rounds} rounds, alternating which runtime goes first.",
        "",
        "| workload | Ray | Chimaera |",
        "|---|---|---|",
    ]
    rows += [f"| {w} | {json.dumps(summary[w]['ray'])} | {json.dumps(summary[w]['chimaera'])} |" for w in names]
    report = "\n".join(rows) + "\n"
    with open(os.path.join(a.out, f"local{a.nodes}-{stamp}.md"), "w") as f:
        f.write(report)
    print(report)


if __name__ == "__main__":
    main()
