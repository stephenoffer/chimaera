"""Multi-node head-to-head on an Anyscale (Ray) cluster: Ray vs Chimaera on
the same machines, one after the other.

Ray is the launcher here too: one task pinned to each node installs the
Chimaera wheel and starts a peer, all peers founding one cluster. The driver
then runs each workload against Chimaera (through the head node's peer) and
against Ray (through the node's Ray cluster), each in a fresh process.

    python bench/evals/multi_node.py --dist /tmp/chimaera-dist --out results/
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 7100
PEER_DIR = "/tmp/chimaera-multi"
# EVAL_SCALE < 1 shrinks every workload, for a smoke run on a small machine.
SCALE = float(os.environ.get("EVAL_SCALE", "1"))


def n(x: int) -> int:
    return max(1, int(x * SCALE))

# Each workload is run by `multi_node.py --workload NAME --runtime ray|chimaera`.
WORKLOADS = ["tasks", "cpu_scaling", "map_small", "broadcast", "naive_big_arg", "follow_input", "actors", "nested", "shuffle", "node_loss"]


# ---- launching Chimaera peers on every node (runs under Ray) -------------------


def launch_peers(dist: str) -> tuple[list[str], str]:
    """Start one Chimaera peer per alive Ray node; return (addresses, head address).

    Each peer runs under a detached actor that stays alive with it: Ray reaps
    the child processes of a finished task's worker, so a peer started from a
    plain task would die when the launching driver disconnects."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    nodes = [n for n in ray.nodes() if n["Alive"]]
    ips = sorted({n["NodeManagerAddress"] for n in nodes})
    seeds = ",".join(f"{ip}:{PORT}" for ip in ips)
    files = {name: open(os.path.join(dist, name), "rb").read() for name in os.listdir(dist)}
    head_ip = ray.util.get_node_ip_address()

    @ray.remote(num_cpus=0)
    class PeerHost:
        def __init__(self, files: dict[str, bytes], seeds: str, groups: int) -> None:
            import glob
            import socket

            here = "/tmp/chimaera-dist"
            os.makedirs(here, exist_ok=True)
            for name, data in files.items():
                with open(os.path.join(here, name), "wb") as f:
                    f.write(data)
            os.chmod(os.path.join(here, "chimaera"), 0o755)
            wheel = glob.glob(os.path.join(here, "*.whl"))[0]
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--force-reinstall", wheel], check=True)
            ip = ray.util.get_node_ip_address()
            cpus = os.cpu_count() or 1
            env = dict(os.environ, CHIMAERA_PYTHON=sys.executable)
            log = open("/tmp/chimaera-peer.log", "ab")
            self.proc = subprocess.Popen(
                [os.path.join(here, "chimaera"), "--dir", PEER_DIR, "peer", "start", "--listen", f"{ip}:{PORT}",
                 "--seeds", seeds, "--cluster", "eval", "--data-groups", str(groups), "--cpus", str(cpus),
                 "--token-file", os.path.join(here, "token")],
                stdout=log, stderr=log, start_new_session=True, env=env,
            )
            self.desc = f"{ip} ({socket.gethostname()}, {cpus} cpus)"

        def describe(self) -> str:
            return self.desc

        def alive(self) -> bool:
            return self.proc.poll() is None

        def log_tail(self) -> str:
            return open("/tmp/chimaera-peer.log", errors="replace").read()[-3000:]

        def stop(self) -> None:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.proc.kill()

    groups = max(8, 2 * len(ips))
    hosts = [
        PeerHost.options(
            name=f"chimaera-peer-{n['NodeID']}", lifetime="detached", get_if_exists=True,
            scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False),
        ).remote(files, seeds, groups)
        for n in nodes
    ]
    print("peers:", ray.get([h.describe.remote() for h in hosts]), flush=True)
    ray.shutdown()
    return [f"{ip}:{PORT}" for ip in ips], head_ip


def peer_hosts():
    import ray

    return [ray.get_actor(f"chimaera-peer-{n['NodeID']}") for n in ray.nodes() if n["Alive"]]


def peer_logs() -> str:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    out = []
    for h in peer_hosts():
        try:
            out.append(ray.get(h.log_tail.remote(), timeout=30))
        except Exception as e:  # noqa: BLE001
            out.append(f"(no log: {e})")
    ray.shutdown()
    return "\n----\n".join(out)


def stop_peers() -> None:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    for h in peer_hosts():
        try:
            ray.get(h.stop.remote(), timeout=90)
            ray.kill(h)
        except Exception as e:  # noqa: BLE001
            print("stop:", e)
    ray.shutdown()


# ---- workloads -----------------------------------------------------------------


def total_cpus(rt: str) -> int:
    if rt == "ray":
        import ray

        return int(ray.cluster_resources().get("CPU", 1))
    import chimaera as ch

    return int(sum(n.cpus for n in ch.cluster().nodes()))


def busy(seconds: float) -> int:
    t = time.process_time()
    n = 0
    while time.process_time() - t < seconds:
        n += 1
    return n


def where() -> str:
    import socket

    return socket.gethostname()


def run_workload(rt: str, name: str) -> dict:
    if rt == "ray":
        import ray

        ray.init(address="auto", log_to_driver=False)
        remote = ray.remote
        get = ray.get
    else:
        import chimaera as ch

        ch.init()
        remote = ch.remote

        def get(x, timeout=None):
            return ch.gather(*x, timeout=timeout) if isinstance(x, list) else x.result(timeout)

    cpus = total_cpus(rt)
    t0 = time.perf_counter()

    if name == "tasks":
        noop = remote(lambda x: x)
        k = n(200_000)
        t0 = time.perf_counter()
        out = get([noop.remote(i) for i in range(k)])
        assert out[-1] == k - 1
        dt = time.perf_counter() - t0
        return {"tasks_per_s": round(k / dt), "cpus": cpus}

    if name == "cpu_scaling":
        burn = remote(busy)
        get([remote(where).remote() for _ in range(2 * cpus)])  # warm every worker
        k, each = 4 * cpus, 0.5 * SCALE
        t0 = time.perf_counter()
        get([burn.remote(each) for _ in range(k)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "efficiency": round(k * each / cpus / dt, 3), "cpus": cpus}

    if name == "map_small":
        double = remote(lambda x: 2 * x)
        if rt == "ray":
            k = n(200_000)  # one task per item: the naive Ray program
            t0 = time.perf_counter()
            get([double.remote(i) for i in range(k)])
            naive = k / (time.perf_counter() - t0)
            batch = remote(lambda xs: [2 * x for x in xs])
            n2 = n(4_000_000)
            t0 = time.perf_counter()
            parts = get([batch.remote(list(range(i, min(i + 1000, n2)))) for i in range(0, n2, 1000)])
            hand = n2 / (time.perf_counter() - t0)
            assert sum(len(p) for p in parts) == n2
            return {"naive_items_per_s": round(naive), "hand_batched_items_per_s": round(hand)}
        k = n(4_000_000)
        t0 = time.perf_counter()
        s = sum(double.map(range(k)))
        dt = time.perf_counter() - t0
        assert s == k * (k - 1)
        return {"naive_items_per_s": round(k / dt)}

    if name == "broadcast":
        import numpy as np

        size = n(512 << 20) // 8 * 8
        if rt == "ray":
            import ray

            ref = ray.put(np.ones(size // 8))
        else:
            import chimaera as ch

            ref = ch.put(np.ones(size // 8))
        total = remote(lambda a: (float(a[:10].sum()), where()))
        t0 = time.perf_counter()
        out = get([total.remote(ref) for _ in range(2 * cpus)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "nodes_reached": len({h for _, h in out}), "readers": len(out)}

    if name == "naive_big_arg":
        # The same 32 MiB array passed straight to every call, no put: the
        # naive program (Ray stores a copy per call).
        import numpy as np

        a = np.ones(n(32 << 20) // 8)
        total = remote(lambda a: (float(a[:10].sum()), where()))
        t0 = time.perf_counter()
        out = get([total.remote(a) for _ in range(2 * cpus)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "nodes_reached": len({h for _, h in out}), "calls": len(out)}

    if name == "follow_input":
        # 10 calls on a 256 MiB value the cluster just made, with no placement
        # given: how many ran on one machine (where the value is, when the
        # runtime follows its input), and how long they took.
        import collections

        make = remote(lambda: b"x" * n(256 << 20))
        probe = remote(lambda b: (len(b), where()))
        big = make.remote()
        if rt == "ray":
            import ray

            ray.wait([big], fetch_local=False)
        else:
            import chimaera as ch

            next(ch.as_completed([big]))
        t0 = time.perf_counter()
        out = get([probe.remote(big) for _ in range(10)])
        dt = time.perf_counter() - t0
        most = collections.Counter(h for _, h in out).most_common(1)[0][1]
        return {"seconds": round(dt, 3), "calls_on_one_host": most, "calls": len(out)}

    if name == "actors":
        class Counter:
            def __init__(self):
                self.n = 0

            def inc(self, k):
                self.n += k
                return self.n

            def host(self):
                return where()

        A = remote(Counter)
        k = max(8, cpus // 2)
        actors = [A.remote() for _ in range(k)]
        hosts = get([a.host.remote() for a in actors])
        calls = n(2000)
        t0 = time.perf_counter()
        futs = [a.inc.remote(1) for a in actors for _ in range(calls)]
        out = get(futs)
        dt = time.perf_counter() - t0
        assert max(out) == calls
        return {"calls_per_s": round(k * calls / dt), "actors": k, "hosts": len(set(hosts))}

    if name == "nested":
        child = remote(lambda x: x)

        def parent_fn(n):
            if rt == "ray":
                import ray

                return sum(ray.get([child.remote(i) for i in range(n)]))
            import chimaera as ch

            return sum(ch.gather(*[child.remote(i) for i in range(n)]))

        parent = remote(parent_fn)
        t0 = time.perf_counter()
        out = get([parent.remote(8) for _ in range(4 * cpus)], timeout=600) if rt == "chimaera" else get([parent.remote(8) for _ in range(4 * cpus)])
        dt = time.perf_counter() - t0
        assert out[0] == 28
        return {"seconds": round(dt, 3), "parents": 4 * cpus}

    if name == "shuffle":
        # 64 map outputs of 50k (key, value) pairs each, summed into 64 partitions.
        maps, per, parts = 64, n(50_000), 64

        def produce(i):
            return [((i * 7919 + j) % 100_003, 1) for j in range(per)]

        if rt == "ray":

            def split(i):
                out = [dict() for _ in range(parts)]
                for k, v in produce(i):
                    d = out[hash(k) % parts]
                    d[k] = d.get(k, 0) + v
                return out

            def merge(*ds):
                acc = {}
                for d in ds:
                    for k, v in d.items():
                        acc[k] = acc.get(k, 0) + v
                return acc

            split_r = remote(num_returns=parts)(split)
            merge_r = remote(merge)
            t0 = time.perf_counter()
            mapped = [split_r.remote(i) for i in range(maps)]
            reduced = get([merge_r.remote(*[m[p] for m in mapped]) for p in range(parts)])
            dt = time.perf_counter() - t0
            total = sum(sum(d.values()) for d in reduced)
        else:
            import operator

            gen = remote(produce)
            t0 = time.perf_counter()
            result = gen.map(range(maps)).reduce_by_key(operator.add, partitions=parts)
            total = sum(v for part in result for _, v in part)
            dt = time.perf_counter() - t0
        assert total == maps * per, total
        return {"seconds": round(dt, 3), "pairs": maps * per}

    if name == "node_loss":
        # Kill the runtime on one worker node while 4 × cpus 1-second tasks
        # run; every task must still return exactly once.
        burn = remote(busy)
        k = 4 * cpus
        t0 = time.perf_counter()
        futs = [burn.remote(1.0) for _ in range(k)]
        time.sleep(2.0)
        victim = kill_one_worker_node(rt)
        out = get(futs, timeout=900) if rt == "chimaera" else get(futs)
        dt = time.perf_counter() - t0
        assert len(out) == k
        return {"seconds": round(dt, 3), "tasks": k, "killed": victim}

    raise ValueError(name)


def kill_one_worker_node(rt: str) -> str:
    """SIGKILL the runtime's agent on one non-head node (Ray: its raylet)."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    if not ray.is_initialized():
        ray.init(address="auto", log_to_driver=False)
    head = ray.util.get_node_ip_address()
    victims = [n for n in ray.nodes() if n["Alive"] and n["NodeManagerAddress"] != head]
    if not victims:
        return "none (single node)"
    node = victims[0]
    pattern = "raylet" if rt == "ray" else f"chimaera --dir {PEER_DIR} peer start"

    @ray.remote(num_cpus=0, max_retries=0)
    def kill(p: str) -> None:
        subprocess.run(["pkill", "-9", "-f", p])

    try:
        ray.get(kill.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False)).remote(pattern), timeout=30)
    except Exception:  # killing the raylet also kills this task
        pass
    return node["NodeManagerAddress"]


# ---- driver ----------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", help="directory with the chimaera binary, wheel and token")
    ap.add_argument("--out", default="results")
    ap.add_argument("--workload")
    ap.add_argument("--runtime")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    if a.workload:  # child process: run one workload, print one JSON line
        res = run_workload(a.runtime, a.workload)
        print(json.dumps({"runtime": a.runtime, "workload": a.workload, "result": res}), flush=True)
        return
    os.makedirs(a.out, exist_ok=True)
    peers, head = launch_peers(a.dist)
    deadline = time.time() + 120
    from chimaera._native import socket_path

    while not os.path.exists(socket_path(PEER_DIR, "peer.sock")):
        assert time.time() < deadline, peer_logs()
        time.sleep(0.5)
    time.sleep(5)  # every founder joined and elected
    names = [w for w in WORKLOADS if not a.only or w in a.only.split(",")]
    # node_loss last for each runtime: it takes a node's runtime down.
    results = []
    for rt in ("chimaera", "ray"):
        for w in names:
            env = dict(os.environ, CHIMAERA_HOME=PEER_DIR, RAY_ADDRESS="auto")
            t0 = time.time()
            try:
                p = subprocess.run([sys.executable, __file__, "--workload", w, "--runtime", rt], capture_output=True,
                                   text=True, timeout=1200, env=env)
                lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
                rec = json.loads(lines[-1]) if p.returncode == 0 and lines else {
                    "runtime": rt, "workload": w, "error": (p.stderr or p.stdout)[-1500:]}
            except subprocess.TimeoutExpired:
                rec = {"runtime": rt, "workload": w, "error": "timed out"}
            rec["wall_s"] = round(time.time() - t0, 1)
            if "error" in rec and rt == "chimaera":
                rec["peer_logs"] = peer_logs()[-4000:]
            print(json.dumps(rec), flush=True)
            results.append(rec)
    try:
        stop_peers()
    except Exception as e:  # noqa: BLE001
        print("stopping peers:", e)
    with open(os.path.join(a.out, "multi_results.json"), "w") as f:
        json.dump({"peers": peers, "head": head, "results": results}, f, indent=2)
    rows = ["| Workload | Ray | Chimaera |", "|---|---|---|"]
    for w in names:
        cell = {r["runtime"]: json.dumps(r.get("result", {"error": r.get("error", "")[:120]})) for r in results if r["workload"] == w}
        rows.append(f"| {w} | {cell.get('ray', '')} | {cell.get('chimaera', '')} |")
    report = f"# Ray vs Chimaera, {len(peers)} nodes\n\n" + "\n".join(rows) + "\n"
    with open(os.path.join(a.out, "multi_report.md"), "w") as f:
        f.write(report)
    print("===== report =====\n" + report)


if __name__ == "__main__":
    main()
