"""Multi-node traps on one machine: a 3-node Ray cluster (`ray.cluster_utils`)
against a 3-peer Chimaera cluster, each node 4 CPUs in its own zone, the
driver attached to the first. Every workload is written the obvious way.

    CHIMAERA_BIN=target/release/chimaera python cluster.py --runtime chimaera
    .venv-bench/bin/python cluster.py --runtime ray

Prints one JSON line per workload. After one warmup run, each workload repeats
until it has run for --min-seconds (default 120; EVAL_MIN_SECONDS too) and at
least 3 times: a few seconds is mostly noise. Reported: median, p10, p90 and
the number of runs. Sizes keep Ray inside its in-memory object store (no
spilling) on a laptop.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import tempfile
import time

import numpy as np

ZONES = ("a", "b", "c")
CPUS = 4
BIG = 32 << 20  # the argument passed to many calls
HELD = 64 << 20  # the value calls should follow
TINY = 30_000


def total(a):
    return float(a[:8].sum())


def make():
    return b"x" * HELD


def size(b):
    return len(b)


def noop(i):
    return i


# ---- runtimes ---------------------------------------------------------------


class Ray:
    def __init__(self) -> None:
        import ray
        from ray.cluster_utils import Cluster

        self.ray = ray
        self.cluster = Cluster()
        for z in ZONES:
            self.cluster.add_node(num_cpus=CPUS, resources={f"zone_{z}": 100}, object_store_memory=1536 << 20)
        ray.init(address=self.cluster.address, log_to_driver=False)
        self.cluster.wait_for_nodes()
        here = ray.remote(lambda: ray.get_runtime_context().get_node_id())
        nodes = ray.get([here.options(resources={f"zone_{z}": 1}).remote() for z in ZONES])
        self.zone_of = dict(zip(nodes, ZONES))

    def fn(self, f, zone=None):
        f = self.ray.remote(f)
        return f.options(resources={f"zone_{zone}": 1}) if zone else f

    def where(self, f):
        ray = self.ray

        def wrapped(*a):
            return f(*a), ray.get_runtime_context().get_node_id()

        return wrapped

    def zone(self, node):
        return self.zone_of.get(node, "?")

    def wait(self, futures):
        self.ray.wait(futures, num_returns=len(futures), fetch_local=False)

    def get(self, futures):
        return self.ray.get(futures)

    def close(self) -> None:
        self.ray.shutdown()
        self.cluster.shutdown()


class Chimaera:
    def __init__(self) -> None:
        exe = os.environ.get("CHIMAERA_BIN") or shutil.which("chimaera")
        self.root = tempfile.mkdtemp(prefix="chk", dir="/tmp")
        ports = []
        for _ in ZONES:
            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                ports.append(s.getsockname()[1])
        addrs = [f"127.0.0.1:{p}" for p in ports]
        token = os.path.join(self.root, "token")
        with open(token, "w") as f:
            f.write(subprocess.check_output([exe, "cluster", "token"], text=True))
        self.procs = []
        for i, z in enumerate(ZONES):
            d = os.path.join(self.root, f"p{z}")
            cmd = [exe, "--dir", d, "peer", "start", "--listen", addrs[i], "--seeds", ",".join(addrs)]
            cmd += ["--cluster", "bench", "--cpus", str(CPUS), "--token-file", token]
            log = open(os.path.join(self.root, f"{z}.log"), "ab")
            env = dict(os.environ, CHIMAERA_LABELS=f"zone={z}")
            self.procs.append(subprocess.Popen(cmd, stdout=log, stderr=log, env=env, start_new_session=True))
        from chimaera._native import socket_path

        for z in ZONES:
            sock = socket_path(os.path.join(self.root, f"p{z}"), "peer.sock")
            deadline = time.monotonic() + 60
            while not os.path.exists(sock):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"peer {z} did not start")
                time.sleep(0.05)
        os.environ["CHIMAERA_HOME"] = os.path.join(self.root, "pa")
        import chimaera as ch

        self.ch = ch

    def fn(self, f, zone=None):
        f = self.ch.remote(f)
        return f.options(placement=self.ch.Placement(require={"zone": zone})) if zone else f

    def where(self, f):
        def wrapped(*a):
            from chimaera._runtime import Runtime

            peer = os.path.basename(os.path.dirname(Runtime.get().object_dir))
            return f(*a), peer

        return wrapped

    def zone(self, node):
        return node[1:] if node.startswith("p") else "?"

    def wait(self, futures):
        for _ in self.ch.as_completed(futures):
            pass

    def get(self, futures):
        return self.ch.gather(*futures)

    def close(self) -> None:
        for p in self.procs:  # throwaway peers: no drain (SIGTERM waits up to 30 s)
            p.kill()
            p.wait()
        shutil.rmtree(self.root, ignore_errors=True)


# ---- workloads -----------------------------------------------------------------


def spread_big_arg(rt):
    """A 32 MiB array passed straight to 30 calls spread over the three zones."""
    a = np.ones(BIG // 8)
    fs = [rt.fn(total, ZONES[i % 3]).remote(a) for i in range(30)]
    assert rt.get(fs) == [8.0] * 30
    return {}


def follow_input(rt):
    """10 calls on a 64 MiB value made in zone a, with no placement given."""
    big = rt.fn(make, "a").remote()
    rt.wait([big])  # done, but not read by the driver
    ran = rt.get([rt.fn(rt.where(size)).remote(big) for _ in range(10)])
    return {"ran_in": "".join(rt.zone(node) for _, node in ran)}


def tiny_spread(rt):
    """30,000 one-line calls submitted in a loop to the cluster."""
    f = rt.fn(noop)
    assert rt.get([f.remote(i) for i in range(TINY)])[-1] == TINY - 1
    return {}


WORKLOADS = {"spread_big_arg": spread_big_arg, "follow_input": follow_input, "tiny_spread": tiny_spread}


def _exit_on_signal(signum: int, _frame: object) -> None:
    raise SystemExit(128 + signum)  # a timeout's SIGTERM unwinds through close(), stopping the peers


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runtime", choices=["ray", "chimaera"], required=True)
    ap.add_argument("--min-seconds", type=float, default=float(os.environ.get("EVAL_MIN_SECONDS", 120)))
    ap.add_argument("--min-reps", type=int, default=3)
    a = ap.parse_args()
    signal.signal(signal.SIGTERM, _exit_on_signal)
    rt = Ray() if a.runtime == "ray" else Chimaera()
    try:
        for name, w in WORKLOADS.items():
            extra = w(rt)  # warmup: executors start, functions are learned
            walls: list[float] = []
            while len(walls) < a.min_reps or sum(walls) < a.min_seconds:
                t = time.perf_counter()
                extra = w(rt)
                walls.append(time.perf_counter() - t)
            q = statistics.quantiles(walls, n=10) if len(walls) >= 2 else walls * 9
            row = {"workload": name, "runtime": a.runtime, "runs": len(walls), "wall_s": round(statistics.median(walls), 3),
                   "p10_s": round(q[0], 3), "p90_s": round(q[-1], 3), **extra}
            print(json.dumps(row), flush=True)
    finally:
        rt.close()


if __name__ == "__main__":
    main()
