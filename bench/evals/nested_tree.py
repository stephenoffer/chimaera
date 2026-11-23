"""Calls that wait on calls they started, as a tree: each node starts
`fanout` children and waits on each in turn. Reports the wall time and the
most worker processes alive at once.

    python bench/evals/nested_tree.py --depth 6        # 3-way: 729 leaves, 364 waiting calls
    CHIMAERA_MEMORY_POOL=1GiB python bench/evals/nested_tree.py --depth 6
    CHIMAERA_HOST_WHILE_WAITING=0 python bench/evals/nested_tree.py --depth 6   # A/B without hosting
    .venv-bench/bin/python bench/evals/nested_tree.py --depth 6 --ray            # the same tree on Ray

Each Chimaera run starts a fresh local peer (its own home, this process's
environment) and stops it afterwards; CHIMAERA_BIN chooses the peer binary.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable


class Peak:
    """The most processes matching `mine` at once, sampled every 100 ms."""

    def __init__(self, mine: Callable[[str], bool]) -> None:
        self.mine, self.most, self.done = mine, 0, threading.Event()
        self.thread = threading.Thread(target=self.watch, daemon=True)

    def watch(self) -> None:
        while not self.done.is_set():
            out = subprocess.run(["ps", "-axo", "command="], capture_output=True, text=True).stdout
            self.most = max(self.most, sum(1 for line in out.splitlines() if self.mine(line)))
            self.done.wait(0.1)

    def __enter__(self) -> Peak:
        self.thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.done.set()
        self.thread.join()


def report(a: argparse.Namespace, system: str, total: int, wall: float, peak: int) -> None:
    assert total == a.fanout**a.depth, total
    print(
        json.dumps(
            {
                "system": system,
                "depth": a.depth,
                "fanout": a.fanout,
                "leaves": total,
                "waiting_calls": (a.fanout**a.depth - 1) // (a.fanout - 1),
                "wall_s": round(wall, 3),
                "peak_workers": peak,
                "cpus": os.cpu_count(),
                "memory_pool": os.environ.get("CHIMAERA_MEMORY_POOL", "") if system == "chimaera" else "",
                "hosting": system == "chimaera" and os.environ.get("CHIMAERA_HOST_WHILE_WAITING", "") != "0",
            }
        )
    )


def run_chimaera(a: argparse.Namespace) -> None:
    # Short: Unix socket paths are limited to about 104 bytes on macOS.
    home = tempfile.mkdtemp(prefix="chn", dir="/tmp")
    os.environ["CHIMAERA_HOME"] = home
    import chimaera as ch

    @ch.remote
    def node(depth: int, fanout: int, leaf_s: float) -> int:
        if depth == 0:
            if leaf_s:
                time.sleep(leaf_s)
            return 1
        children = [node.remote(depth - 1, fanout, leaf_s) for _ in range(fanout)]
        return sum(c.result() for c in children)

    try:
        node.remote(0, 1, 0.0).result()  # the peer and a first executor start untimed
        with Peak(lambda line: "chimaera._executor" in line and f"--socket {home}/" in line) as peak:
            t0 = time.perf_counter()
            total = node.remote(a.depth, a.fanout, a.leaf_ms / 1000).result()
            wall = time.perf_counter() - t0
        report(a, "chimaera", total, wall, peak.most)
    finally:
        subprocess.run([os.environ.get("CHIMAERA_BIN") or "chimaera", "--dir", home, "stop"], capture_output=True, timeout=60)
        shutil.rmtree(home, ignore_errors=True)


def run_ray(a: argparse.Namespace) -> None:
    import ray

    ray.init(num_cpus=os.cpu_count(), include_dashboard=False, logging_level="ERROR")

    @ray.remote
    def node(depth: int, fanout: int, leaf_s: float) -> int:
        if depth == 0:
            if leaf_s:
                time.sleep(leaf_s)
            return 1
        children = [node.remote(depth - 1, fanout, leaf_s) for _ in range(fanout)]
        return sum(ray.get(c) for c in children)

    try:
        ray.get(node.remote(0, 1, 0.0))
        with Peak(lambda line: line.startswith("ray::")) as peak:
            t0 = time.perf_counter()
            total = ray.get(node.remote(a.depth, a.fanout, a.leaf_ms / 1000))
            wall = time.perf_counter() - t0
        report(a, "ray", total, wall, peak.most)
    finally:
        ray.shutdown()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--depth", type=int, default=6)
    p.add_argument("--fanout", type=int, default=3)
    p.add_argument("--leaf-ms", type=float, default=0.0, help="time each leaf sleeps")
    p.add_argument("--ray", action="store_true", help="run the same tree on Ray instead")
    a = p.parse_args()
    (run_ray if a.ray else run_chimaera)(a)


if __name__ == "__main__":
    main()
