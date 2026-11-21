"""The same rows as hotpath.py, through Ray, with the same code as ray/_private/ray_perf.py.

    python ray_side.py [--pause S] [--json OUT]
"""

import argparse
import json
import multiprocessing
import os
import sys

import ray

from hotpath import M_CLIENTS, N_CALLS, N_CPU, latency, timeit


@ray.remote
class Actor:
    def small_value(self):
        return b"ok"


@ray.remote
def work(actors):
    ray.get([actors[i % N_CPU].small_value.remote() for i in range(N_CALLS)])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pause", type=float, default=3.0)
    p.add_argument("--json")
    args = p.parse_args()
    ray.init(address="local", include_dashboard=False)
    a = Actor.remote()
    rows = dict(
        [
            timeit("1:1 actor calls sync", lambda: ray.get(a.small_value.remote()), pause=args.pause),
            timeit(
                "1:1 actor calls async",
                lambda: ray.get([a.small_value.remote() for _ in range(1000)]),
                1000,
                pause=args.pause,
            ),
        ]
    )
    actors = [Actor.remote() for _ in range(N_CPU)]
    rows.update(
        [
            timeit(
                "n:n actor calls async",
                lambda: ray.get([work.remote(actors) for _ in range(M_CLIENTS)]),
                M_CLIENTS * N_CALLS,
                pause=args.pause,
            )
        ]
    )
    out = {
        "rates": rows,
        "latency": {"ray": latency(lambda: ray.get(a.small_value.remote()))},
        "load_avg_1m": os.getloadavg()[0],
        "python": sys.version.split()[0],
        "ray": ray.__version__,
        "n_cpu_actors": N_CPU,
        "cpu_count": multiprocessing.cpu_count(),
    }
    print(json.dumps(out, indent=1))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
