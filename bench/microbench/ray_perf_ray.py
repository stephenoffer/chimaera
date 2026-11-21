"""Run Ray's own microbenchmark suite (`ray microbenchmark`) and print JSON lines.

    .venv-bench/bin/python bench/microbench/ray_perf_ray.py

Calls ray._private.ray_perf.main() unchanged, which ends with the Ray Client
benchmarks. MICROBENCH_SLEEP overrides the 10 s pause timeit takes before each
benchmark; TESTS_TO_RUN / TESTS_TO_SKIP filter by name as in Ray. Prints one
{"name", "value", "sd", "unit"} line per metric after the suite finishes.
"""

from __future__ import annotations

import functools
import json
import os

import ray._private.ray_client_microbenchmark as client_bench
import ray._private.ray_perf as ray_perf
from ray._private.ray_microbenchmark_helpers import timeit

if "MICROBENCH_SLEEP" in os.environ:
    patched = functools.partial(timeit, warmup_time_sec=float(os.environ["MICROBENCH_SLEEP"]))
    ray_perf.timeit = patched
    client_bench.timeit = patched

for row in ray_perf.main():
    if row is None:
        continue
    name, mean, sd = row
    unit = "GB/s" if "gigabytes" in name else "ops/s"
    print(json.dumps({"name": name, "value": float(mean), "sd": float(sd), "unit": unit}), flush=True)
