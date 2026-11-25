"""Three-node Ray vs three-peer Chimaera on ONE machine, same workloads as
``multi_node.py`` (it runs that file's per-workload children).

Both get the same split of this machine: 3 nodes of ``--cpus-per-node`` CPUs
(Ray through ``ray.cluster_utils.Cluster``, Chimaera through three peers with
``--cpus``). Everything crosses real sockets between node processes, but
nodes share one machine's cores, memory and loopback network: this measures
protocol and scheduling overhead, NOT scaling. ``node_loss`` is left out
(it kills a node by host; ``node_loss.py`` repeats node loss on localhost).

Each round runs every workload once per runtime, the workloads and which
runtime goes first in a seeded random order (``--seed``). The JSON holds the
run's manifest (``manifest.py``) and every child's raw samples; the report
the medians over all rounds with their 95% intervals, and the raft entries
the peers applied per unit of work. Without ``--ray-python`` only Chimaera
runs: a regression run against its own earlier reports.

    .venv/bin/python bench/evals/local_cluster.py --ray-python .venv-bench/bin/python --rounds 3
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time

import manifest
from multi_node import CHIMAERA_ONLY, SCALE

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
WORKLOADS = [
    "tasks", "tasks_serial", "cpu_scaling", "map_small", "broadcast", "actors", "actors_peer", "nested", "shuffle",
    "shuffle_unfused",
]

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
    given rounds in a seeded random order, then write the manifest, the raw
    results and the report."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--ray-python", help="a Python with Ray installed (without it, only Chimaera runs)")
    ap.add_argument(
        "--chimaera-python",
        default=sys.executable,
        help="the Python for Chimaera's drivers (and so its executors); give the Ray one to compare on one interpreter",
    )
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--cpus-per-node", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--seed", type=int, help="the run order's seed (default: a random one, recorded)")
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    a = ap.parse_args()
    # Stopped from outside: still run the cleanup below, so only this run's nodes go away.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    names = [w for w in WORKLOADS if not a.only or w in a.only.split(",")]
    seed = a.seed if a.seed is not None else random.randrange(1 << 31)
    rng = random.Random(seed)
    os.makedirs(a.out, exist_ok=True)

    holder = None
    pythons = {"chimaera": a.chimaera_python}
    if a.ray_python:
        holder = subprocess.Popen(
            [a.ray_python, "-c", RAY_HOLDER.format(cpus=a.cpus_per_node, nodes=a.nodes)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        ray_address = holder.stdout.readline().strip()
        pythons["ray"] = a.ray_python
    cluster = start_chimaera(a.nodes, a.cpus_per_node)
    time.sleep(3)  # founders joined and elected
    binary = manifest.running_binary(cluster.dir(0))

    def statuses() -> list[dict]:
        return [manifest.status(binary, cluster.dir(i)) for i in range(a.nodes)]

    durability = manifest.settled_durability(statuses)
    envs = {"chimaera": dict(os.environ, CHIMAERA_HOME=cluster.dir(0), PYTHONPATH=os.path.join(REPO, "python"))}
    if holder:
        envs["ray"] = dict(os.environ, RAY_ADDRESS=ray_address)
    records = []
    try:
        for rnd in range(a.rounds):
            for w in rng.sample(names, len(names)):
                for rt in rng.sample(sorted(pythons), len(pythons)):
                    if rt == "ray" and w in CHIMAERA_ONLY:
                        continue
                    entries0 = manifest.raft_entries(statuses()) if rt == "chimaera" else 0
                    rec = run_child(pythons[rt], rt, w, envs[rt])
                    rec["round"] = rnd
                    if rt == "chimaera":
                        rec["durability"] = durability
                        if "result" in rec:  # the commits behind the child's work, warm-up included
                            rec["result"]["raft_entries"] = manifest.raft_entries(statuses()) - entries0
                    print(json.dumps(rec), flush=True)
                    records.append(rec)
    finally:
        cluster.close()
        if holder:
            holder.stdin.close()
            holder.wait(timeout=120)

    stamp = time.strftime("%Y-%m-%d")
    topology = {"nodes": a.nodes, "cpus_per_node": a.cpus_per_node, "data_groups": cluster.data_groups,
                "layout": "one machine, loopback network"}
    order = {"seed": seed, "rounds": a.rounds, "scale": SCALE, "load_avg_after": os.getloadavg()}
    doc = {"manifest": manifest.collect(binary, pythons, topology=topology, durability=durability, order=order),
           "results": records}
    with open(os.path.join(a.out, f"local{a.nodes}-{stamp}.json"), "w") as f:
        json.dump(doc, f, indent=2)
    report = manifest.report(doc, WORKLOADS, f"Ray vs Chimaera, {a.nodes} nodes on one machine ({stamp})", [
        "Protocol and scheduling overhead only: every node shares one machine's cores and loopback network.",
        f"{doc['manifest']['host']['platform']}, {os.cpu_count()} cpus; {a.nodes} x {a.cpus_per_node} CPUs; "
        f"{a.rounds} rounds in a random order (seed {seed}). EVAL_SCALE {SCALE}.",
    ])
    with open(os.path.join(a.out, f"local{a.nodes}-{stamp}.md"), "w") as f:
        f.write(report)
    print(report)


if __name__ == "__main__":
    main()
