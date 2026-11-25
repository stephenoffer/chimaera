"""Ray's distributed release benchmarks, run against Ray and Chimaera on the
same Anyscale cluster (release/benchmarks/distributed and object_store in the
Ray repository), each workload in a fresh driver, Chimaera first:

- broadcast_1g: one actor pinned to every machine reads the same 1 GiB array
  put by the driver (Ray's test_object_store: 50 nodes in 12.9 s, published).
- many_actors: 2 x CPUs actors created, each answering one call (Ray's
  test_many_actors: 10,000 actors at 0.25 CPU, 578 actors/s, published).
- many_tasks: 4 x CPUs calls of 0.25 CPU that each sleep SLEEP_S seconds, so
  all of them can run at once; rate = calls / (wall - SLEEP_S), Ray's formula
  in test_many_tasks (666 tasks/s for 10,000, published).

Ray's actors and calls ask for 0.25 CPU, as its tests do; Chimaera's actors
hold no CPU by default and its calls ask for 0.25 too.

    python bench/evals/release_scale.py --dist /tmp/chimaera-dist --out results/
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

SCALE = float(os.environ.get("EVAL_SCALE", "1"))
SLEEP_S = float(os.environ.get("RELEASE_SLEEP_S", "20"))
WORKLOADS = ["broadcast_1g", "many_actors", "many_tasks"]


def n(x: float) -> int:
    return max(1, int(x * SCALE))


class Reader:
    def ping(self):
        return 1

    def data_len(self, arr):
        return len(arr)


def sleeper(s):
    started = time.time()  # a timestamp, compared across machines
    time.sleep(s)
    return started


def run(rt: str, name: str) -> dict:
    """One workload against one runtime, in this (fresh) process."""
    import numpy as np

    if rt == "ray":
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), log_to_driver=False)
        get, put = ray.get, ray.put
        alive = [x for x in ray.nodes() if x["Alive"]]
        nodes, cpus = [x["NodeID"] for x in alive], int(sum(x["Resources"].get("CPU", 0) for x in alive))

        def pinned_actor(cls, node):
            strategy = NodeAffinitySchedulingStrategy(node, soft=False)
            return ray.remote(num_cpus=0.25)(cls).options(scheduling_strategy=strategy).remote()

        def actor(cls):
            return ray.remote(num_cpus=0.25)(cls).remote()

        task = ray.remote(num_cpus=0.25)(sleeper)
    else:
        import chimaera as ch

        ch.init()
        get, put = ch.get, ch.put
        peers = ch.runs.cluster().nodes()
        nodes, cpus = [x.peer for x in peers], int(sum(x.cpus for x in peers))

        def pinned_actor(cls, node):
            return ch.remote(cls).options(placement=ch.dist.Placement(require={"node": node})).remote()

        def actor(cls):
            return ch.remote(cls).remote()

        task = ch.remote(sleeper).options(cpus=0.25)

    if name == "broadcast_1g":
        readers = [pinned_actor(Reader, node) for node in nodes]
        get([a.ping.remote() for a in readers])  # every actor is up, as Ray's test waits for
        size = n(2**30)
        ref = put(np.ones(size, dtype=np.uint8))
        t0 = time.perf_counter()
        out = get([a.data_len.remote(ref) for a in readers])
        dt = time.perf_counter() - t0
        assert out == [size] * len(readers)
        return {"seconds": round(dt, 3), "nodes": len(nodes), "bytes": size}
    if name == "many_actors":
        k = n(2 * cpus)
        t0 = time.perf_counter()
        actors = [actor(Reader) for _ in range(k)]
        out = get([a.ping.remote() for a in actors])
        dt = time.perf_counter() - t0
        assert sum(out) == k
        return {"actors_per_s": round(k / dt, 1), "actors": k, "seconds": round(dt, 2)}
    if name == "many_tasks":
        k = n(4 * cpus)
        t0, w0 = time.perf_counter(), time.time()
        out = get([task.remote(SLEEP_S) for _ in range(k)])
        dt = time.perf_counter() - t0
        assert len(out) == k
        # When each call started, from the submit of the first: how the burst was taken up.
        starts = sorted(x - w0 for x in out)
        at = {f"start_p{q}_s": round(starts[min(k - 1, k * q // 100)], 2) for q in (10, 50, 90)}
        return {"tasks_per_s": round(k / max(dt - SLEEP_S, 1e-3), 1), "tasks": k, "seconds": round(dt, 2), **at,
                "start_last_s": round(starts[-1], 2)}
    raise ValueError(name)


def child(rt: str, name: str, home: str) -> dict:
    t0 = time.monotonic()
    env = dict(os.environ, CHIMAERA_HOME=home, RAY_ADDRESS=os.environ.get("RAY_ADDRESS", "auto"))
    try:
        p = subprocess.run([sys.executable, __file__, "--workload", name, "--runtime", rt], capture_output=True,
                           text=True, timeout=1800, env=env)
        lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
        rec = json.loads(lines[-1]) if p.returncode == 0 and lines else {
            "runtime": rt, "workload": name, "error": (p.stderr or p.stdout)[-1500:]}
    except subprocess.TimeoutExpired:
        rec = {"runtime": rt, "workload": name, "error": "timed out after 1800 s"}
    rec["wall_s"] = round(time.monotonic() - t0, 1)
    print(json.dumps(rec), flush=True)
    return rec


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", help="directory with the chimaera binary, wheel and token")
    ap.add_argument("--out", default="results")
    ap.add_argument("--workload")
    ap.add_argument("--runtime")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    if a.workload:  # child: one workload, one JSON line
        print(json.dumps({"runtime": a.runtime, "workload": a.workload, "result": run(a.runtime, a.workload)}),
              flush=True)
        return
    import multi_node as mn
    from chimaera._native import socket_path

    os.makedirs(a.out, exist_ok=True)
    names = [w for w in WORKLOADS if not a.only or w in a.only.split(",")]
    records = []
    peers = mn.launch_peers(a.dist)[0]
    deadline = time.monotonic() + 120
    while not os.path.exists(socket_path(mn.PEER_DIR, "peer.sock")):
        assert time.monotonic() < deadline, "the head peer did not start"
        time.sleep(0.5)
    time.sleep(5)  # founders joined and elected
    try:
        records += [child("chimaera", w, mn.PEER_DIR) for w in names]
    finally:
        mn.stop_peers(wipe=True)
    records += [child("ray", w, mn.PEER_DIR) for w in names]
    with open(os.path.join(a.out, "release_results.json"), "w") as f:
        json.dump({"peers": len(peers), "scale": SCALE, "sleep_s": SLEEP_S, "results": records}, f, indent=2)
    rows = ["| Workload | Ray | Chimaera |", "|---|---|---|"]
    for w in names:
        cell = {r["runtime"]: json.dumps(r.get("result", {"error": r.get("error", "")[-160:]})) for r in records
                if r["workload"] == w}
        rows.append(f"| {w} | {cell.get('ray', '')} | {cell.get('chimaera', '')} |")
    report = f"# Ray's release benchmarks, {len(peers)} nodes\n\n" + "\n".join(rows) + "\n"
    with open(os.path.join(a.out, "release_report.md"), "w") as f:
        f.write(report)
    print("===== report =====\n" + report)


if __name__ == "__main__":
    main()
