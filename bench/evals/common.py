"""Shared parameters and helpers for the Ray vs Chimaera single-node evals."""

from __future__ import annotations

import os
import statistics
import time

CORES = os.cpu_count() or 1


def pct(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def summary_us(samples_s: list[float]) -> dict[str, float]:
    us = [x * 1e6 for x in samples_s]
    return {"p50_us": round(pct(us, 0.5), 1), "p99_us": round(pct(us, 0.99), 1), "mean_us": round(statistics.mean(us), 1)}


class Timer:
    def __enter__(self) -> "Timer":
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.s = time.perf_counter() - self.t0


def busy(seconds: float) -> int:
    t = time.process_time()
    n = 0
    while time.process_time() - t < seconds:
        n += 1
    return n


# Workload sizes (scaled to the node where it matters). EVAL_SCALE shrinks the
# counts for a quick dry run; published runs use 1.
SCALE = float(os.environ.get("EVAL_SCALE", "1"))


def n(x: int) -> int:
    return max(1, int(x * SCALE))



LATENCY_CALLS = n(500)
TASKS = n(50_000)
MAP_ITEMS = n(50_000)
RAY_BATCH = 500  # Ray's documented fix for tiny tasks: batch items per task
CHAINS, DEPTH = 64, 20
SCALING_TASK_S = 0.1
SCALING_TASKS = 4 * CORES
LARGE_BYTES = 256 << 20
LARGE_READERS = 2 * CORES
ACTOR_CALLS = n(20_000)
NESTED_PARENTS, NESTED_CHILDREN = 4 * CORES, 4
