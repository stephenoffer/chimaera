"""Compiled actor graphs, Chimaera against Ray: a two-actor pipeline
(x -> a.step -> b.step -> driver), one invocation at a time (latency) and
with up to 8 in flight (throughput), and the same calls made one by one.

    python bench/evals/compiled_graph.py --runtime chimaera
    python bench/evals/compiled_graph.py --runtime ray

Run both from one interpreter. Prints one JSON line per measurement.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time


def measure(name: str, invoke, n: int, window: int) -> dict:
    """invoke(i) -> a handle whose .result() / ray.get waits for the value."""
    for i in range(50):  # warm up
        invoke.get(invoke.start(i))
    lat = []
    for i in range(n):
        t = time.perf_counter()
        invoke.get(invoke.start(i))
        lat.append(time.perf_counter() - t)
    t = time.perf_counter()
    pending = []
    for i in range(n * 4):
        pending.append(invoke.start(i))
        if len(pending) >= window:
            invoke.get(pending.pop(0))
    for h in pending:
        invoke.get(h)
    rate = n * 4 / (time.perf_counter() - t)
    lat.sort()
    return {
        "case": name,
        "p50_us": round(statistics.median(lat) * 1e6, 1),
        "p99_us": round(lat[int(len(lat) * 0.99)] * 1e6, 1),
        "per_s": round(rate),
    }


class Invoke:
    def __init__(self, start, get):
        self.start, self.get = start, get


def run_chimaera(n: int, window: int) -> list[dict]:
    import chimaera as ch

    @ch.remote
    class Stage:
        def step(self, x):
            return x + 1

    a, b = Stage.remote(), Stage.remote()

    @ch.dist.graph
    def pipe(x):
        return b.step.remote(a.step.remote(x))

    out = [measure("calls", Invoke(lambda i: pipe.remote(i), lambda f: f.result()), n, window)]
    with pipe.compile(max_in_flight=window) as g:
        out.append(measure("compiled", Invoke(g.remote, lambda f: f.result()), n, window))
    return out


def run_ray(n: int, window: int) -> list[dict]:
    import ray
    from ray.dag import InputNode

    ray.init(address="local", include_dashboard=False, log_to_driver=False)

    @ray.remote
    class Stage:
        def step(self, x):
            return x + 1

    a, b = Stage.remote(), Stage.remote()
    out = [measure("calls", Invoke(lambda i: b.step.remote(a.step.remote(i)), ray.get), n, window)]
    with InputNode() as inp:
        dag = b.step.bind(a.step.bind(inp))
    g = dag.experimental_compile(_max_inflight_executions=window)
    out.append(measure("compiled", Invoke(g.execute, ray.get), n, window))
    g.teardown()
    ray.shutdown()
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runtime", choices=("chimaera", "ray"), required=True)
    p.add_argument("-n", type=int, default=2000)
    p.add_argument("--window", type=int, default=8)
    a = p.parse_args()
    for rec in (run_chimaera if a.runtime == "chimaera" else run_ray)(a.n, a.window):
        print(json.dumps({"runtime": a.runtime, **rec}), flush=True)


if __name__ == "__main__":
    main()
