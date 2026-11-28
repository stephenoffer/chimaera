"""Memory: calls wait for memory instead of overcommitting it, nested calls do
not deadlock on a small pool, and a leaking worker is replaced. Each test runs
its own peer. (Host pressure, which needs a machine driven out of memory, is
the `runaway` trap in bench/evals/gotchas, run only in a memory-limited
container.)"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

import pytest

from conftest import BIN, REPO, run_script


@pytest.fixture
def home():
    d = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    yield d
    subprocess.run([BIN, "--dir", d, "stop"], capture_output=True, timeout=60)


def _most_at_once(spans: list[tuple[float, float]]) -> int:
    edges = sorted([(s, 1) for s, _ in spans] + [(e, -1) for _, e in spans])
    now = most = 0
    for _, step in edges:
        now += step
        most = max(most, now)
    return most


def test_calls_wait_for_memory_instead_of_overcommitting(home):
    # Each call holds 300 MiB for a second. With a 1 GiB pool, at most three run
    # at once (the probe for a function not yet measured), then two once its
    # memory is known. Without admission all ten would run together.
    r = run_script(
        """
        import json, time
        import numpy as np
        import chimaera as ch

        @ch.remote
        def hold(i):
            a = np.ones(300 * 2**20 // 8)
            t0 = time.time()
            time.sleep(1.0)
            return [t0, time.time(), float(a[-1])]

        futures = [hold.remote(i) for i in range(10)]
        print(json.dumps([f.result(timeout=240) for f in futures]))
        print(futures[-1].explain()["attempts"][-1]["reservation"])
        """,
        home=home,
        env={"CHIMAERA_MEMORY_POOL": "1GiB"},
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    spans = [(s, e) for s, e, _ in json.loads(lines[0])]
    assert _most_at_once(spans) <= 3, spans
    assert "learned" in lines[1], lines[1]
    assert _most_at_once(spans[-4:]) <= 2, f"after learning, two 300 MiB calls fill a 1 GiB pool: {spans[-4:]}"


def test_nested_calls_complete_under_a_small_pool(home):
    # Parents wait on their children; a waiting parent is charged what it
    # holds, not its reservation, so the children get the memory.
    r = run_script(
        """
        import time
        import numpy as np
        import chimaera as ch

        @ch.remote
        def child(i):
            a = np.ones(100 * 2**20 // 8)
            time.sleep(0.2)
            return i + int(a[0]) - 1

        @ch.remote
        def parent(i):
            return sum(child.remote(10 * i + j).result() for j in range(3))

        print(sum(f.result(timeout=240) for f in [parent.remote(i) for i in range(6)]))
        """,
        home=home,
        env={"CHIMAERA_MEMORY_POOL": "512MiB"},
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == [str(sum(10 * i + j for i in range(6) for j in range(3)))]


def test_a_leaking_function_gets_a_fresh_worker(home):
    r = run_script(
        """
        import os
        import numpy as np
        import chimaera as ch

        kept = []

        @ch.remote
        def leak(i):
            kept.append(np.ones(40 * 2**20 // 8))  # never released: a leak
            return os.getpid()

        pids = [leak.remote(i).result(timeout=60) for i in range(40)]
        print(len(set(pids)), ch.runs.current_run().id)
        """,
        home=home,
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    workers, run_id = r.stdout.split()
    assert int(workers) >= 2, "the leaking worker was never replaced"
    decisions = subprocess.run(
        [BIN, "--dir", home, "explain", run_id, "--decisions"], capture_output=True, text=True, timeout=60
    ).stdout
    assert "leaks" in decisions and "replaced" in decisions, decisions


def test_fast_calls_that_keep_memory_are_not_batched_past_the_leak_watch(home):
    # The leak watch looks between batches. A 2 ms function that kept memory
    # was batched 100 deep, so its worker grew by 100 calls' worth between two
    # looks; on a small peer one batch outgrew the pool and every call in it
    # failed as out of memory. Each replacement now comes within a few calls
    # of its limit (twice the first reading plus 512 MiB).
    import json
    import re

    r = run_script(
        """
        import numpy as np
        import chimaera as ch

        kept = []

        @ch.remote(cpus=4)
        def keep(i):
            kept.append(np.ones(8 * 2**20 // 8))  # 8 MiB never released
            return i

        assert sum(ch.get([keep.remote(i) for i in range(320)], timeout=240)) == sum(range(320))
        print(ch.runs.current_run().id)
        """,
        home=home,
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    run_id = r.stdout.split()[-1]
    out = subprocess.run(
        [BIN, "--dir", home, "explain", run_id, "--decisions", "--json"], capture_output=True, text=True, timeout=60
    ).stdout
    grew = [
        tuple(map(int, m.groups()))
        for d in json.loads(out)["decisions"]
        if d["controller"] == "leaks" and (m := re.search(r"grew from (\d+) MiB to (\d+) MiB", d["evidence"]))
    ]
    assert len(grew) >= 2, out
    over = [last - (2 * first + 512) for first, last in grew]
    assert max(over) < 128, f"MiB past the limit before each replacement: {over}"


def test_the_kernel_kills_a_worker_before_its_peer_when_memory_runs_out():
    import chimaera as ch

    if sys.platform != "linux":
        pytest.skip("oom_score_adj is Linux's")

    @ch.remote
    def oom_score_adj() -> str:
        with open("/proc/self/oom_score_adj") as f:
            return f.read().strip()

    assert oom_score_adj.remote().result(timeout=60) == "1000"


def test_executors_cap_thread_pools_and_write_no_core_dumps():
    import chimaera as ch

    @ch.remote
    def environment():
        import os
        import resource

        names = (
            "OMP_NUM_THREADS", "POLARS_MAX_THREADS", "NUMBA_NUM_THREADS", "TOKENIZERS_PARALLELISM", "MALLOC_ARENA_MAX",
        )
        return {n: os.environ.get(n) for n in names}, resource.getrlimit(resource.RLIMIT_CORE)

    env, core = environment.remote().result(timeout=60)
    assert env["OMP_NUM_THREADS"] == env["POLARS_MAX_THREADS"] == env["NUMBA_NUM_THREADS"] == "1", env
    assert env["TOKENIZERS_PARALLELISM"] == "false", env
    assert env["MALLOC_ARENA_MAX"] == ("4" if sys.platform == "linux" else None), env
    assert core == (0, 0), core


def test_an_actor_is_charged_its_memory_once_while_a_call_runs(home):
    # An actor's calls run inside its process: the pool charges what that
    # process holds, not that again for each call measured to "use" it.
    r = run_script(
        f"""
        import json, subprocess, time
        import numpy as np
        import chimaera as ch

        @ch.remote
        class Holder:
            def __init__(self):
                self.a = np.ones(300 * 2**20 // 8)

            def nap(self, s):
                time.sleep(s)
                return float(self.a[-1])

        h = Holder.remote()
        h.nap.remote(0).result(timeout=60)  # measured once: its peak includes the 300 MiB
        slow = h.nap.remote(3)
        time.sleep(1.5)
        status = json.loads(subprocess.run(
            [{BIN!r}, "--dir", {home!r}, "status", "--json"], capture_output=True, text=True, timeout=30
        ).stdout)
        slow.result(timeout=60)
        print(status["memory_used"] >> 20)
        """,
        home=home,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    used_mib = int(r.stdout.split()[-1])
    assert 300 <= used_mib < 600, f"{used_mib} MiB charged for one 300 MiB actor"


def test_actors_wait_to_start_while_live_actors_fill_the_pool(home):
    # Tiny-CPU actors each keeping 150 MiB: twelve of them would need about
    # 2 GiB of a 1 GiB pool (Ray starts them all and the node runs out of
    # memory). Here a few start, the rest wait with a reason, and closing
    # finished ones lets the others start in turn.
    r = run_script(
        f"""
        import json, os, subprocess, time
        import numpy as np
        import chimaera as ch

        @ch.remote(cpus=0.01)
        class Holder:
            def __init__(self):
                self.a = np.ones(150 * 2**20 // 8)

            def pid(self):
                return os.getpid()

        actors = [Holder.remote() for _ in range(12)]
        calls = [a.pid.remote() for a in actors]
        ch.wait(calls, num_returns=1, timeout=120)
        time.sleep(5)  # the first ones are measured; no more fit
        started = [i for i, f in enumerate(calls) if f.done()]
        waiting = [i for i, f in enumerate(calls) if not f.done()]
        reason = calls[waiting[0]].explain()["pending_reason"] if waiting else ""
        status = json.loads(subprocess.run(
            [{BIN!r}, "--dir", {home!r}, "status", "--json"], capture_output=True, text=True, timeout=30
        ).stdout)
        left, deadline = set(range(12)), time.time() + 180
        while left and time.time() < deadline:
            for i in sorted(left):
                if calls[i].done():
                    calls[i].result()
                    actors[i].close()
                    left.discard(i)
            time.sleep(0.2)
        print(json.dumps({{"started": len(started), "used": status["memory_used"] >> 20, "reason": reason,
                          "status": status["pending_reasons"], "never_ran": len(left)}}))
        """,
        home=home,
        env={"CHIMAERA_MEMORY_POOL": "1GiB"},
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout.splitlines()[-1])
    assert 1 <= out["started"] <= 6, out
    assert out["used"] <= 1100, out
    assert "its actor has not started" in out["reason"] and "handle.close()" in out["reason"], out
    assert any("actor(s) waiting to start" in s for s in out["status"]), out
    assert out["never_ran"] == 0, out


def test_a_call_killed_for_memory_whose_retry_cannot_fit_is_out_of_memory(home):
    # Killed at its 150 MB limit, its retry needs at least 1.5× that, more
    # than the 200 MiB pool: the caller learns it ran out of memory.
    r = run_script(
        """
        import time
        import chimaera as ch

        @ch.remote(memory_limit="150 MB")
        def hog():
            data = b"\\x01" * (400 << 20)
            time.sleep(2)
            return len(data)

        try:
            hog.remote().result(timeout=120)
        except ch.OutOfMemory as e:
            print("OOM", e)
        """,
        home=home,
        env={"CHIMAERA_MEMORY_POOL": "200MiB"},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.startswith("OOM"), r.stdout + r.stderr


def _fs_type(path: str) -> str:
    """Filesystem type of `path` from /proc/mounts (Linux), else ""."""
    try:
        with open("/proc/mounts") as f:
            mounts = [line.split()[1:3] for line in f]
    except OSError:
        return ""
    best = max((m for m in mounts if path == m[0] or path.startswith(m[0].rstrip("/") + "/")), key=lambda m: len(m[0]), default=None)
    return best[1] if best else ""


def test_objects_past_the_pool_move_to_disk_and_stay_readable(home):
    # Objects in tmpfs count against the memory pool; past 80% of it the
    # oldest move to the spill directory and are still read at their path.
    if not sys.platform.startswith("linux"):
        pytest.skip("objects live in tmpfs only on Linux")
    r = run_script(
        """
        import json, time
        import numpy as np
        import chimaera as ch
        from chimaera._runtime import Runtime

        import os

        held = [ch.put(np.full(100 * 2**20 // 8, float(i))) for i in range(6)]  # 600 MiB, all different
        rt = Runtime.get()
        # Looked at while the run lives: an ended run's stored values go.
        spill = os.path.join(os.environ["CHIMAERA_HOME"], "spill", rt.run.hex())
        deadline = time.monotonic() + 15  # the peer rescans object bytes every second
        while not (os.path.isdir(spill) and os.listdir(spill)) and time.monotonic() < deadline:
            time.sleep(0.2)
        spilled = len(os.listdir(spill)) if os.path.isdir(spill) else 0
        print(json.dumps([float(h.result()[123]) for h in held]))
        print(rt.object_dir)
        print(spilled)
        """,
        home=home,
        env={"CHIMAERA_MEMORY_POOL": "600MiB"},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    values, object_dir, spilled = r.stdout.split("\n")[:3]
    if _fs_type(object_dir) != "tmpfs":
        pytest.skip(f"objects are not in tmpfs here ({object_dir}), so they never count against the pool")
    if os.stat(object_dir).st_dev == os.stat(home).st_dev:
        pytest.skip("objects and the peer's directory share a filesystem: spilling there would free nothing")
    assert int(spilled) > 0, "nothing moved to the spill directory"
    assert json.loads(values) == [float(i) for i in range(6)], "every value reads back, spilled or not"


def test_memory_a_call_freed_goes_back_to_the_system(home):
    # glibc keeps freed blocks in its heap: without malloc_trim the worker
    # would stay ~400 MiB and look like a leak to the peer.
    if not sys.platform.startswith("linux"):
        pytest.skip("malloc_trim is glibc's")
    r = run_script(
        """
        import chimaera as ch

        @ch.remote
        class Churner:
            def churn(self):
                blocks = [bytes(1024) for _ in range(400_000)]  # ~400 MiB of 1 KiB blocks
                kept = blocks[::50]  # holes everywhere: free() cannot shrink the heap
                del blocks
                self.kept = kept
                return len(kept)

            def anon_mib(self):
                with open("/proc/self/status") as f:
                    return next(int(line.split()[1]) // 1024 for line in f if line.startswith("RssAnon:"))

        c = Churner.remote()
        assert c.churn.remote().result(timeout=60) == 8000
        print(c.anon_mib.remote().result(timeout=60))
        """,
        home=home,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert int(r.stdout.split()[-1]) < 200, f"worker still holds {r.stdout.split()[-1]} MiB after freeing ~390 MiB"


def test_a_value_written_past_the_memory_limit_goes_to_a_spill_directory_that_can_take_it(tmp_path):
    # Spilled at birth (memory over its limit), in its own process: the first
    # spill directory in turn cannot hold the run's files, so the value goes
    # to the next, and nothing is left in the first.
    mem, bad, good = (tmp_path / n for n in ("mem", "bad", "good"))
    for d in (mem, bad, good):
        d.mkdir()
    run = "ab" * 16
    (bad / run).write_bytes(b"")  # a file where its run directory would go
    code = f"""
import os
from chimaera import _native
_native.objects_config({str(mem)!r}, None, {f"{bad}:{good}"!r}, 1)  # over a 1-byte limit
for i in range(2):  # round-robin: one starts at each directory
    path = os.path.join({str(mem)!r}, {run!r}, f"{{i:032x}}")
    _native.write_object(path, [b"x" * 5000])
    print(os.readlink(path), open(path, "rb").read() == b"x" * 5000)
"""
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"))
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60, env=env)
    assert r.returncode == 0, r.stderr[-3000:]
    for i, line in enumerate(r.stdout.splitlines()):
        assert line == f"{good / run / f'{i:032x}'} True", r.stdout
    assert os.listdir(bad) == [run] and os.path.isfile(bad / run), "no partial file in the failed directory"
