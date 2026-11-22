"""Local dispatch latency and throughput (a local measurement, not a cluster claim).

Warm executors at about half load: single-call round trips, the peer's own
grant-to-handoff percentiles, and map throughput of no-op items.

    python bench/local_dispatch.py
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import time

import chimaera as ch


@ch.remote
def noop(x):
    return x


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


def main() -> None:
    rt = ch._runtime.Runtime.get()
    cores = rt.info["usable_cores"]
    wave = max(1, int(cores * 0.5))
    # Warm up: start enough executors for the wave.
    ch.get((noop.remote(i) for i in range(wave * 4)), timeout=120)

    rtt = []
    for _ in range(300):
        t = time.perf_counter()
        noop.remote(1).result(timeout=30)
        rtt.append((time.perf_counter() - t) * 1e6)

    wave_lat = []
    for _ in range(100):
        t = time.perf_counter()
        ch.get((noop.remote(i) for i in range(wave)), timeout=30)
        wave_lat.append((time.perf_counter() - t) * 1e6)

    n = 20_000
    t = time.perf_counter()
    count = sum(1 for _ in noop.map(range(n), ordered=False))
    map_rate = count / (time.perf_counter() - t)

    home = rt.home
    status = json.loads(
        subprocess.run([os.environ.get("CHIMAERA_BIN", "chimaera"), "--dir", home, "status", "--json"], capture_output=True, text=True, check=True).stdout
    )
    print(
        json.dumps(
            {
                "host": f"{platform.system()} {platform.machine()} {cores:.0f} cores",
                "python": platform.python_version(),
                "single_call_round_trip_us": {"p50": round(pct(rtt, 0.5)), "p99": round(pct(rtt, 0.99))},
                f"wave_of_{wave}_round_trip_us": {"p50": round(pct(wave_lat, 0.5)), "p99": round(pct(wave_lat, 0.99))},
                "peer_grant_to_handoff_us": {
                    "p50": round(status["dispatch_p50_us"], 1),
                    "p99": round(status["dispatch_p99_us"], 1),
                    "samples": status["dispatch_samples"],
                },
                "map_noop_items_per_s": round(map_rate),
                "mean_rtt_us": round(statistics.mean(rtt)),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
