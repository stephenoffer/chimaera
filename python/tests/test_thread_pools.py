"""A worker's thread pools follow its call's CPU grant. The peer starts each
worker with the BLAS, OpenMP and similar thread variables set to its thread
count, before any library reads them, so concurrent NumPy calls stay within
their grants, in a fresh worker and in one reused. A run that sets one of
those variables itself keeps its value.

Where the kernel reports CPU time per thread (Linux), each call also counts
the threads that worked during its matrix products: NumPy's BLAS pool."""

from __future__ import annotations

import json

from conftest import run_script

SCRIPT = """
import json, os, time
import chimaera as ch

ch.init(env=ENV)

@ch.remote(cpus=2)
def matmul():
    import numpy as np

    def per_thread():
        # CPU seconds of each thread of this process, where the kernel says.
        if not os.path.isdir("/proc/self/task"):
            return None
        cpu = {}
        for tid in os.listdir("/proc/self/task"):
            try:
                with open(f"/proc/self/task/{tid}/stat") as f:
                    fields = f.read().rsplit(")", 1)[1].split()
            except OSError:
                continue
            cpu[tid] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
        return cpu

    a = np.random.rand(800, 800)
    a @ a  # the pool starts
    before, start = per_thread(), time.perf_counter()
    while time.perf_counter() - start < 1.0:
        a @ a
    after, wall = per_thread(), time.perf_counter() - start
    busy = None if after is None else sum(1 for t, c in after.items() if c - before.get(t, 0.0) > 0.1 * wall)
    return os.getpid(), os.environ.get("OPENBLAS_NUM_THREADS"), busy

# Two at once, twice: the second pair runs in the workers the first started.
fresh = ch.get([matmul.remote() for _ in range(2)], timeout=120)
reused = ch.get([matmul.remote() for _ in range(2)], timeout=120)
print(json.dumps({"fresh": fresh, "reused": reused}))
"""


def _run(env: dict[str, str]) -> tuple[list, list]:
    r = run_script(SCRIPT.replace("ENV", repr(env)), timeout=240)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout.splitlines()[-1])
    fresh, reused = out["fresh"], out["reused"]
    assert {pid for pid, _, _ in fresh} & {pid for pid, _, _ in reused}, out
    return fresh, reused


def test_concurrent_numpy_calls_keep_to_their_grant_in_fresh_and_reused_workers():
    fresh, reused = _run({})
    for pid, var, busy in fresh + reused:
        assert var == "2", (pid, var)
        assert busy is None or 1 <= busy <= 2, f"{busy} threads worked in a call granted 2 CPUs"


def test_a_runs_own_thread_setting_wins():
    fresh, reused = _run({"OPENBLAS_NUM_THREADS": "3", "OMP_NUM_THREADS": "3"})
    for pid, var, busy in fresh + reused:
        assert var == "3", (pid, var)
        # Counting threads sees a pool larger than the grant: the first test
        # would see one too, were the grant not applied.
        assert busy is None or busy == 3, f"{busy} threads worked with OPENBLAS_NUM_THREADS=3"
