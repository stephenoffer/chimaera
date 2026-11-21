"""Ray Core gotchas, written the way a non-expert writes them, for Ray and Chimaera.

    python traps.py ray|chimaera TRAP

Each trap uses only a minimal shim (`remote`, `call`, `get_each`), so the user
code is the same for both runtimes: no `ray.put`, no `num_cpus`, no
`max_calls`, no batching by hand. Prints one JSON line with what happened.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import CORES, n  # noqa: E402

MIB = 1 << 20
# Memory traps size themselves from the container's limit (bytes).
MEM_LIMIT = int(os.environ.get("GOTCHA_MEMORY_LIMIT", str(3 << 30)))


class RayShim:
    def __init__(self) -> None:
        import ray

        ray.init(include_dashboard=False)
        self.ray = ray

    def remote(self, fn):
        return self.ray.remote(fn)

    def get_each(self, futures):
        for f in futures:
            try:
                yield self.ray.get(f), None
            except Exception as e:  # count every failure, by type
                yield None, e


class ChimaeraShim:
    def __init__(self) -> None:
        import chimaera

        self.ch = chimaera

    def remote(self, fn):
        return self.ch.remote(fn)

    def get_each(self, futures):
        for f in futures:
            try:
                yield f.result(), None
            except Exception as e:  # count every failure, by type
                yield None, e


# ---- user functions (module level, as a user would write them) -----------------


def total(a):
    return float(a.sum())


def noop(i):
    return i


def wait_io(i):
    time.sleep(0.1)
    return i


def set_first(a):
    a[0] = 1.0
    return float(a[0])


def allocate(nbytes):
    block = np.ones(nbytes // 8)
    time.sleep(1.0)
    return float(block[-1])


_leaked = []


def leaky(i):
    _leaked.append(np.ones(20 * MIB // 8))
    return i


def grow_forever(_):
    blocks = []
    while True:  # a bug: memory grows until something stops it
        blocks.append(np.ones(64 * MIB // 8))
        time.sleep(0.1)


def scales_with_input(a):
    work = np.repeat(a, 20)  # 20x the input, briefly
    time.sleep(0.2)
    return float(work[-1])


# ---- traps ------------------------------------------------------------------------


def big_arg(rt):
    """The same 200 MB array passed to every call (Ray: re-serialized each time)."""
    a = np.ones(200 * MIB // 8)
    f = rt.remote(total)
    calls = n(500)
    return [f.remote(a) for _ in range(calls)], lambda v: v == float(a.size)


def tiny_tasks(rt):
    """100k calls that do nothing, submitted in a loop."""
    f = rt.remote(noop)
    return [f.remote(i) for i in range(n(100_000))], None


def io_sleep(rt):
    """Calls that wait 100 ms on I/O and hold a whole core while they do (Ray)."""
    f = rt.remote(wait_io)
    return [f.remote(i) for i in range(n(2000))], None


def mutate_arg(rt):
    """Writing into an array argument (Ray: read-only from the object store)."""
    f = rt.remote(set_first)
    a = np.zeros(1 << 20)
    return [f.remote(a) for _ in range(n(20))], lambda v: v == 1.0


def mem_heavy(rt):
    """Each call needs 35% of memory; the runtime decides how many run at once."""
    f = rt.remote(allocate)
    return [f.remote(int(MEM_LIMIT * 0.35)) for _ in range(3 * CORES)], lambda v: v == 1.0


def leak(rt):
    """Every call leaks 20 MiB into its worker (Ray: until max_calls is set)."""
    f = rt.remote(leaky)
    calls = int(1.5 * MEM_LIMIT / (20 * MIB))
    return [f.remote(i) for i in range(calls)], None


def size_scaling(rt):
    """Memory grows 20x with the input; inputs vary from 1 to 24 MiB."""
    f = rt.remote(scales_with_input)
    rng = np.random.default_rng(7)
    sizes = rng.integers(1, 25, size=n(120)) * MIB
    return [f.remote(np.ones(int(s) // 8)) for s in sizes], lambda v: v == 1.0


def runaway(rt):
    """A call whose memory grows without bound: how it is stopped, and what
    the caller learns (Ray retries memory kills without limit by default)."""
    f = rt.remote(grow_forever)
    return [f.remote(0)], None


TRAPS = {
    "big_arg": big_arg,
    "tiny_tasks": tiny_tasks,
    "io_sleep": io_sleep,
    "mutate_arg": mutate_arg,
    "mem_heavy": mem_heavy,
    "leak": leak,
    "size_scaling": size_scaling,
    "runaway": runaway,
}
# Traps that push memory past the limit: run them only inside a memory-limited
# container, never on a workstation (Ray has no memory monitor on macOS).
MEMORY_TRAPS = {"mem_heavy", "leak", "size_scaling", "runaway"}
# Traps that may never finish on a runtime (a retry loop): their own limit, s.
TRAP_TIMEOUT = {"runaway": 180}


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one gotcha trap and print what happened as JSON.")
    ap.add_argument("runtime", choices=["ray", "chimaera"])
    ap.add_argument("trap", choices=sorted(TRAPS))
    a = ap.parse_args()
    runtime, name = a.runtime, a.trap
    rt = RayShim() if runtime == "ray" else ChimaeraShim()
    t0 = time.perf_counter()
    futures, check = TRAPS[name](rt)
    submitted_s = time.perf_counter() - t0
    ok, wrong, errors = 0, 0, {}
    for value, err in rt.get_each(futures):
        if err is not None:
            key = type(err).__name__
            errors[key] = errors.get(key, 0) + 1
        elif check is None or check(value):
            ok += 1
        else:
            wrong += 1
    wall = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "trap": name,
                "runtime": runtime,
                "calls": len(futures),
                "ok": ok,
                "wrong": wrong,
                "errors": errors,
                "submit_s": round(submitted_s, 3),
                "wall_s": round(wall, 3),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
