"""NUMA check for a Linux host with two or more NUMA nodes: what binding
workers to one node promises, on real hardware (python/tests/test_numa.py
emulates the layout and sees only affinity). Prints one JSON line per check
and exits non-zero if any fails.

- each one-CPU worker runs on one node's CPUs, and the workers spread over
  every node;
- the memory a worker first touches lands on its node (/proc/self/numa_maps);
- a memory-bandwidth workload, `--per-node` copy loops per node at once,
  moves at least as many bytes a second bound as unbound;
- a worker granted more CPUs than a node holds runs unbound, and with
  CHIMAERA_NUMA_BIND=0 every worker does.

    python bench/evals/numa_check.py [--per-node 4] [--mib 256] [--seconds 5]

Each mode starts a fresh local peer (its own home, this process's
environment, CHIMAERA_NUMA_BIND set) and stops it; CHIMAERA_BIN chooses the
peer binary.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import chimaera as ch


def cpulist(text: str) -> list[int]:
    out: list[int] = []
    for part in text.strip().split(","):
        if part:
            first, _, last = part.partition("-")
            out += range(int(first), int(last or first) + 1)
    return out


def allowed() -> list[int]:
    with open("/proc/self/status") as f:
        return cpulist(next(line for line in f if line.startswith("Cpus_allowed_list:")).split(":", 1)[1])


def host_nodes() -> dict[int, list[int]]:
    """This host's NUMA nodes and their CPUs."""
    root = "/sys/devices/system/node"
    names = [n for n in os.listdir(root) if n.startswith("node") and n[4:].isdigit()] if os.path.isdir(root) else []
    return {int(n[4:]): cpulist(open(f"{root}/{n}/cpulist").read()) for n in names}


def pages_by_node(address: int) -> dict[int, int]:
    """Pages of the mapping holding `address`, by the node they are on."""
    with open("/proc/self/maps") as f:
        start = next(
            line.split("-", 1)[0]
            for line in f
            if int(line.split("-", 1)[0], 16) <= address < int(line.split()[0].split("-")[1], 16)
        )
    with open("/proc/self/numa_maps") as f:
        line = next(line for line in f if line.split(" ", 1)[0] == start)
    return {int(k[1:]): int(v) for k, v in (w.split("=", 1) for w in line.split() if w[:1] == "N" and "=" in w)}


@ch.remote(cpus=1)
def stream(mib: int, seconds: float, at: float) -> dict:
    """Copy one array into another for `seconds`, starting at `at` (all at once)."""
    import numpy as np

    a = np.ones(mib << 17)  # float64: mib MiB, first touched here
    b = np.zeros_like(a)
    time.sleep(max(0.0, at - time.time()))
    moved, start = 0, time.perf_counter()
    while time.perf_counter() - start < seconds:
        np.copyto(b, a)
        moved += 2 * a.nbytes
    return {"pid": os.getpid(), "cpus": allowed(), "rate": moved / (time.perf_counter() - start), "pages": pages_by_node(b.ctypes.data)}


@ch.remote
def wide_affinity() -> list[int]:
    return allowed()


def run(bind: bool, a: argparse.Namespace, nodes: dict[int, list[int]]) -> tuple[list[dict], list[int]]:
    """One peer with binding on or off: the stream workers and a wide call's CPUs."""
    home = tempfile.mkdtemp(prefix="chq", dir="/tmp")
    os.environ.update(CHIMAERA_HOME=home, CHIMAERA_NUMA_BIND="1" if bind else "0")
    try:
        workers = len(nodes) * a.per_node
        at = time.time() + 5 + a.mib / 256  # after every worker has started and touched its arrays
        runs = ch.get([stream.remote(a.mib, a.seconds, at) for _ in range(workers)], timeout=600)
        largest = max(len(c) for c in nodes.values())
        wide = wide_affinity.options(cpus=min(largest + 1, len(allowed()))).remote().result(timeout=300)
        return runs, wide
    finally:
        ch.shutdown()
        subprocess.run([os.environ.get("CHIMAERA_BIN") or "chimaera", "--dir", home, "stop"], capture_output=True, timeout=60)
        shutil.rmtree(home, ignore_errors=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--per-node", type=int, default=4, help="copy loops per NUMA node")
    p.add_argument("--mib", type=int, default=256, help="size of each loop's two arrays")
    p.add_argument("--seconds", type=float, default=5.0)
    a = p.parse_args()
    failed = 0

    def check(name: str, ok: bool, **detail: object) -> None:
        nonlocal failed
        failed += not ok
        print(json.dumps({"check": name, "ok": ok, **detail}, default=str), flush=True)

    mine = set(allowed())
    nodes = {n: [c for c in cpus if c in mine] for n, cpus in host_nodes().items()}
    nodes = {n: cpus for n, cpus in nodes.items() if cpus}
    if len(nodes) < 2:
        check("two or more NUMA nodes", False, nodes=nodes, error="not a multi-node host (or its cpuset spans one)")
        return 1

    def node_of(cpus: list[int]) -> int | None:
        return next((n for n, c in nodes.items() if set(cpus) <= set(c)), None)

    bound, wide = run(True, a, nodes)
    homes = [node_of(r["cpus"]) for r in bound]
    check("each worker runs on one node's CPUs", None not in homes, workers=[(r["pid"], r["cpus"]) for r in bound])
    check("the workers spread over every node", set(homes) == set(nodes), per_node={n: homes.count(n) for n in nodes})
    local = [r["pages"].get(n, 0) / max(1, sum(r["pages"].values())) for r, n in zip(bound, homes) if n is not None]
    check("a worker's memory is on its node", bool(local) and min(local) >= 0.9, local_fraction=[round(f, 3) for f in local])
    check("a worker wider than a node is unbound", node_of(wide) is None, cpus=wide)

    unbound, wide_off = run(False, a, nodes)
    check(
        "CHIMAERA_NUMA_BIND=0 leaves every worker unbound",
        all(node_of(r["cpus"]) is None for r in unbound) and node_of(wide_off) is None,
    )
    rate_bound, rate_unbound = sum(r["rate"] for r in bound), sum(r["rate"] for r in unbound)
    check(
        "bound copy throughput is at least unbound's",
        rate_bound >= 0.97 * rate_unbound,
        bound_gb_s=round(rate_bound / 1e9, 2),
        unbound_gb_s=round(rate_unbound / 1e9, 2),
        ratio=round(rate_bound / max(rate_unbound, 1.0), 3),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
