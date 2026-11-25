"""A call waiting on calls it started lends its CPUs so they can run, but
threads it started may keep running through the wait (a BLAS pool, a thread
of its own). The peer takes back what those threads use, so it does not grant
the same cores twice, and the waited-for calls still run.

The same goes for the tenant's CPU quota: the parent lends its share of it
less what its threads use.

Each test runs its own small peer, with hosting off so that every child runs
in a process of its own and the parent's thread keeps its core, except one
case with hosting on, where a child may run in the waiting parent's process."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, run_script

CGROUPS = "attempts in cgroups of their own need Linux with cgroup v2 delegation"


def _start_peer(cpus: int, quota: int | None, host: bool = False) -> tuple[str, subprocess.Popen]:
    """A peer of `cpus` CPUs, where runs (tenant "default") may hold `quota`
    and, with `host`, waiting calls host others."""
    home = tempfile.mkdtemp(prefix="chn", dir="/tmp")
    env = dict(os.environ, CHIMAERA_HOST_WHILE_WAITING="1" if host else "0")
    if quota is not None:
        env["CHIMAERA_TENANT_QUOTAS"] = f"default={quota}"
    with open(os.path.join(home, "peer.log"), "ab") as log:
        proc = subprocess.Popen(
            [BIN, "--dir", home, "peer", "start", "--cpus", str(cpus), "--idle-timeout", "600"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True, env=env,
        )
    deadline = time.monotonic() + 30
    while not os.path.exists(socket_path(home, "peer.sock")):
        assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
        time.sleep(0.05)
    return home, proc


@pytest.fixture
def peer(request):
    home, proc = _start_peer(*request.param)
    yield home
    subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)
    proc.wait(timeout=60)  # it writes its profiles as it exits
    shutil.rmtree(home, ignore_errors=True)


# The parent waits on its children one by one, with or without a NumPy loop
# on `threads` threads of its own, like a native pool (matrix products release
# the GIL, so they run through the wait). Each child spins for SPIN seconds and
# reports when it ran.
FAN_OUT = """
import threading, time
import numpy as np
import chimaera as ch

SPIN = 0.4

@ch.remote(cpus=1)
def child():
    start = time.time()
    while time.time() - start < SPIN:
        pass
    return start, time.time()

@ch.remote(cpus=1)
def parent(n, threads):
    stop = threading.Event()
    used = []
    def loop():
        a = np.random.rand(300, 300)
        cpu, wall = time.thread_time(), time.time()
        while not stop.is_set():
            a @ a
        used.append((time.thread_time() - cpu) / (time.time() - wall))
    pool = [threading.Thread(target=loop) for _ in range(threads)]
    for t in pool:
        t.start()
    time.sleep(0.5 if pool else 0)
    try:
        kids = [child.remote() for _ in range(n)]
        waited = time.time()
        spans = [k.result() for k in kids]
    finally:
        stop.set()
        for t in pool:
            t.join()
    return waited, spans, sum(used)

waited, spans, used = parent.remote(ARGS).result(timeout=240)
# Among the children that started a second into the wait (the first window's
# measurement has been taken by then): the most running at once just after
# one of them started, and how many ran at once on average.
late = [s for s in spans if s[0] >= waited + 1.0]
most = max(sum(1 for o in spans if o[0] <= s[0] + 0.05 < o[1]) for s in late)
mean = sum(e - s for s, e in late) / (max(e for _, e in late) - min(s for s, _ in late))
print(len(late), most, mean, used)
"""


def _fan_out(home: str, n: int, threads: int) -> tuple[int, int, float, float]:
    """Late children, the most and the mean at once, and the cores the
    parent's threads used."""
    r = run_script(FAN_OUT.replace("ARGS", f"{n}, {threads}"), home=home, timeout=280)
    assert r.returncode == 0, r.stderr
    late, most, mean, used = r.stdout.split()[-4:]
    return int(late), int(most), float(mean), float(used)


# Two CPUs, or four CPUs where the tenant may hold two.
BUSY = pytest.mark.parametrize("peer", [(2, None), (4, 2)], ids=["cpus", "quota"], indirect=True)


@pytest.mark.parametrize("peer", [(2, None), (4, 2), (2, None, True)], ids=["cpus", "quota", "hosting"], indirect=True)
def test_a_waiting_parents_running_threads_keep_its_core(peer):
    # The parent's threads hold one of the two (it lent no more), so its
    # children run one at a time. Granting the lent CPU again would run two
    # beside them (and twelve children, two at a time, still leave some to
    # start late). With hosting on, a child the parent hosts is charged its
    # own grant, beside what the threads keep. On a host so loaded that the
    # threads get no CPU for a whole window, the peer rightly lends it for that
    # window: the mean allows for it.
    late, most, mean, used = _fan_out(peer, 12, threads=4)
    if used < 0.5:
        # The peer lends what the threads do not use; threads the host's own
        # load keeps off the CPUs use next to nothing, so there is no core
        # here to keep for them.
        pytest.skip(f"the parent's threads got {used:.2f} cores from this host")
    assert late >= 3, late
    assert mean < 1.5, f"{mean:.2f} children ran at once on average (most {most}) beside threads using {used:.2f} cores"


@BUSY
def test_an_idle_waiting_parent_lends_both_cpus(peer):
    late, most, _, _ = _fan_out(peer, 8, threads=0)
    assert late >= 2, late
    assert most == 2, f"{most} children at once: the waiting parent should lend its CPU"


@pytest.mark.parametrize("peer", [(1, None), (2, 1)], ids=["cpus", "quota"], indirect=True)
def test_children_and_actors_of_a_busy_parent_still_run_on_one_cpu(peer):
    # Its threads use the only CPU through the wait (four, so that a loaded
    # host still leaves them some), or the tenant's one-CPU quota; the
    # children still start, one at a time, over capacity or quota by one grant
    # at most. So does an actor it starts once the threads' use is known (the
    # children measured it).
    r = run_script(
        """
        import threading, time
        import numpy as np
        import chimaera as ch

        @ch.remote(cpus=1)
        def child(i):
            start = time.time()
            while time.time() - start < 0.5:  # long enough to measure the parent's thread
                pass
            return i * i

        @ch.remote(cpus=1)
        class Square:
            def of(self, i):
                return i * i

        @ch.remote(cpus=1)
        def parent():
            stop = threading.Event()
            def loop():
                a = np.random.rand(300, 300)
                while not stop.is_set():
                    a @ a
            pool = [threading.Thread(target=loop) for _ in range(4)]
            for t in pool:
                t.start()
            try:
                kids = sum(child.remote(i).result() for i in range(5))
                return kids + Square.remote().of.remote(7).result(timeout=60)
            finally:
                stop.set()
                for t in pool:
                    t.join()

        print(parent.remote().result(timeout=120))
        """,
        home=peer,
        timeout=150,
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split()[-1] == "79", r.stdout


@pytest.mark.parametrize("peer", [(2, None)], ids=["cpus"], indirect=True)
@pytest.mark.parametrize(
    "kind",
    ["actors", "direct", pytest.param("isolated", marks=pytest.mark.skipif(sys.platform != "linux", reason=CGROUPS))],
)
def test_calls_let_in_beside_a_busy_parents_threads_run_one_at_a_time(peer, kind):
    # The parent's threads use both CPUs through its waits (its first wait
    # measured them). It then waits on two calls let in over capacity: two new
    # actors' methods, two attempts in cgroups of their own, or a method of an
    # actor let in earlier, sent straight to it from the parent (the direct
    # path), beside a task. Each is let in only once the one before has ended,
    # so they run one at a time. Letting both in at once would grant the cores
    # those threads use twice.
    r = run_script(
        """
        import threading, time
        import numpy as np
        import chimaera as ch
        from chimaera import _direct

        def spin_for(t):
            start = time.time()
            while time.time() - start < t:
                pass
            return start, time.time()

        @ch.remote(cpus=1)
        class Spinner:
            def spin(self, t):
                return spin_for(t)

        @ch.remote(cpus=1, accounting="isolated")
        def spin(t):
            return spin_for(t)

        @ch.remote(cpus=1)
        def task(t):
            return spin_for(t)

        @ch.remote(cpus=1)
        def nap():
            time.sleep(1.0)

        @ch.remote(cpus=2)
        def parent(kind):
            stop = threading.Event()
            def loop():
                a = np.random.rand(300, 300)
                while not stop.is_set():
                    a @ a
            pool = [threading.Thread(target=loop) for _ in range(4)]
            for t in pool:
                t.start()
            try:
                nap.remote().result()
                if kind == "actors":
                    spinners = [Spinner.remote() for _ in range(2)]
                    calls = [s.spin.remote(1.5) for s in spinners]
                elif kind == "direct":
                    s = Spinner.remote()
                    s.spin.remote(0.05).result(timeout=60)
                    time.sleep(0.5)  # it listens for direct calls now
                    sent = _direct._counter["direct"]
                    calls = [s.spin.remote(1.5), task.remote(1.5)]
                    assert _direct._counter["direct"] == sent + 1, "the actor's call went through the peer"
                else:
                    calls = [spin.remote(1.5) for _ in range(2)]
                # The cores its process (the threads and their BLAS pool) used
                # through this wait, as the peer measures them.
                cpu, wall = time.process_time(), time.time()
                spans = ch.get(calls, timeout=60)
                used = (time.process_time() - cpu) / (time.time() - wall)
            finally:
                stop.set()
                for t in pool:
                    t.join()
            return spans, used

        ((s1, e1), (s2, e2)), used = parent.remote(KIND).result(timeout=120)
        print(max(0.0, min(e1, e2) - max(s1, s2)), used)
        """.replace("KIND", repr(kind)),
        home=peer,
        timeout=150,
    )
    if kind == "isolated" and "cgroup v2 delegation" in r.stderr:
        pytest.skip(CGROUPS)
    assert r.returncode == 0, r.stderr
    overlap, used = map(float, r.stdout.split()[-2:])
    if used < 1.0:
        # Threads a loaded host gives less than half the two cores they may
        # hold get none for a whole window now and then, and lending them
        # all is then right.
        pytest.skip(f"the parent's threads got {used:.2f} cores from this host")
    assert overlap < 0.3, f"the two calls overlapped {overlap:.2f}s beside threads using {used:.2f} cores"


@pytest.mark.parametrize("peer", [(2, None), (4, 2), (2, None, True)], ids=["cpus", "quota", "hosting"], indirect=True)
def test_a_busy_parents_child_runs_beside_a_call_that_never_ends_on_its_own(peer):
    # A call that runs until the parent tells it to stop (a server loop)
    # holds one of the two CPUs, or of the tenant's two-CPU quota; the
    # parent's threads hold the other. Its child is let in anyway: only
    # calls let in beside the threads hold back the next one.
    r = run_script(
        """
        import os, tempfile, threading, time
        import numpy as np
        import chimaera as ch

        @ch.remote(cpus=1)
        def serve(flag):
            open(flag + ".started", "w").close()
            while not os.path.exists(flag):
                time.sleep(0.01)
            return "served"

        @ch.remote(cpus=1)
        def nap():
            time.sleep(1.0)

        @ch.remote(cpus=1)
        def child():
            return "child ran"

        @ch.remote(cpus=1)
        def parent(flag):
            stop = threading.Event()
            def loop():
                a = np.random.rand(300, 300)
                while not stop.is_set():
                    a @ a
            pool = [threading.Thread(target=loop) for _ in range(4)]
            for t in pool:
                t.start()
            try:
                nap.remote().result()  # the peer measures the threads
                return child.remote().result(timeout=30)
            finally:
                open(flag, "w").close()
                stop.set()
                for t in pool:
                    t.join()

        flag = os.path.join(tempfile.mkdtemp(), "stop")
        s = serve.remote(flag)
        while not os.path.exists(flag + ".started"):
            time.sleep(0.05)
        print(parent.remote(flag).result(timeout=90), "and", s.result(timeout=60))
        """,
        home=peer,
        timeout=150,
    )
    assert r.returncode == 0, r.stderr[-3000:]
    assert r.stdout.strip().endswith("child ran and served"), r.stdout


@pytest.mark.parametrize("peer", [(2, None)], ids=["cpus"], indirect=True)
def test_a_call_let_in_that_idles_holds_back_no_other(peer):
    # The parent's threads hold both CPUs. The first call let in beside them
    # waits, without using CPU, until the parent has its children: once it has
    # idled for a while, the children are let in, one at a time, as if it had
    # ended.
    r = run_script(
        """
        import os, tempfile, threading, time
        import numpy as np
        import chimaera as ch

        @ch.remote(cpus=1)
        def serve(flag):
            while not os.path.exists(flag):
                time.sleep(0.01)
            return "served"

        @ch.remote(cpus=1)
        def nap():
            time.sleep(1.0)

        @ch.remote(cpus=1)
        def child(i):
            start = time.time()
            while time.time() - start < 0.3:
                pass
            return i

        @ch.remote(cpus=2)
        def parent(flag):
            stop = threading.Event()
            def loop():
                a = np.random.rand(300, 300)
                while not stop.is_set():
                    a @ a
            pool = [threading.Thread(target=loop) for _ in range(4)]
            for t in pool:
                t.start()
            try:
                nap.remote().result()  # the peer measures the threads
                s = serve.remote(flag)  # let in as the parent waits below
                kids = [child.remote(i) for i in range(3)]
                got = sum(k.result(timeout=30) for k in kids)
            finally:
                open(flag, "w").close()
                stop.set()
                for t in pool:
                    t.join()
            return got, s.result(timeout=30)

        print(*parent.remote(os.path.join(tempfile.mkdtemp(), "stop")).result(timeout=90))
        """,
        home=peer,
        timeout=150,
    )
    assert r.returncode == 0, r.stderr[-3000:]
    assert r.stdout.split()[-2:] == ["3", "served"], r.stdout
