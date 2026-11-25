"""A call waiting on calls it started lends its whole grant so they can run,
its CPUs and its share of the tenant's quota, even while threads it started
keep running through the wait (a BLAS pool, a thread of its own). So the
calls it waits for start whatever other tenants run on the peer; under its
own tenant's quota or share they may wait for that tenant's running calls to
end.

Each test runs its own small peer, with hosting off so that every child runs
in a process of its own, except the cases with hosting on, where a child may
run in the waiting parent's process."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import threading
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, run_script


def _start_peer(cpus: int, quota: int | None, host: bool = False, share: bool = False) -> tuple[str, subprocess.Popen]:
    """A peer of `cpus` CPUs, where runs (tenant "default") may hold `quota`
    (as a cluster share with `share`) and, with `host`, waiting calls host
    others."""
    home = tempfile.mkdtemp(prefix="chn", dir="/tmp")
    env = dict(os.environ, CHIMAERA_HOST_WHILE_WAITING="1" if host else "0")
    if quota is not None:
        env["CHIMAERA_TENANT_SHARES" if share else "CHIMAERA_TENANT_QUOTAS"] = f"default={quota}"
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


# The parent waits on its children one by one. Each child spins for SPIN
# seconds and reports when it ran.
FAN_OUT = """
import time
import chimaera as ch

SPIN = 0.4

@ch.remote(cpus=1)
def child():
    start = time.time()
    while time.time() - start < SPIN:
        pass
    return start, time.time()

@ch.remote(cpus=1)
def parent(n):
    kids = [child.remote() for _ in range(n)]
    waited = time.time()
    return waited, [k.result() for k in kids]

waited, spans = parent.remote(ARGS).result(timeout=240)
# Among the children that started a second into the wait (their workers have
# started by then): the most running at once just after one of them started.
late = [s for s in spans if s[0] >= waited + 1.0]
most = max(sum(1 for o in spans if o[0] <= s[0] + 0.05 < o[1]) for s in late)
print(len(late), most)
"""


@pytest.mark.parametrize("peer", [(2, None), (4, 2)], ids=["cpus", "quota"], indirect=True)
def test_an_idle_waiting_parent_lends_both_cpus(peer):
    # Two CPUs, or four CPUs where the tenant may hold two.
    r = run_script(FAN_OUT.replace("ARGS", "8"), home=peer, timeout=280)
    assert r.returncode == 0, r.stderr
    late, most = map(int, r.stdout.split()[-2:])
    assert late >= 2, late
    assert most == 2, f"{most} children at once: the waiting parent should lend its CPU"


@pytest.mark.parametrize("peer", [(1, None), (2, 1)], ids=["cpus", "quota"], indirect=True)
def test_children_and_actors_of_a_busy_parent_still_run_on_one_cpu(peer):
    # Its threads use the only CPU through the wait (four, so that a loaded
    # host still leaves them some), or the tenant's one-CPU quota. Waiting, it
    # lends its whole grant, so its children start, and so does an actor it
    # starts.
    r = run_script(
        """
        import threading, time
        import numpy as np
        import chimaera as ch

        @ch.remote(cpus=1)
        def child(i):
            start = time.time()
            while time.time() - start < 0.5:
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


@pytest.mark.parametrize("peer", [(2, None), (4, 2), (2, None, True)], ids=["cpus", "quota", "hosting"], indirect=True)
def test_a_busy_parents_child_runs_beside_a_call_that_never_ends_on_its_own(peer):
    # A call that runs until the parent tells it to stop (a server loop)
    # holds one of the two CPUs, or of the tenant's two-CPU quota; the
    # parent holds the other, and its threads keep using it. Waiting, the
    # parent lends it, so its child starts.
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
                nap.remote().result()  # a first wait
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


BUSY_CALL = """
import os, tempfile, threading, time
import numpy as np
import chimaera as ch

@ch.remote(cpus=1)
def nap():
    time.sleep(0.5)

@ch.remote(cpus=1)
def busy(flag):
    while not os.path.exists(flag):  # computes, as a loop would
        pass
    return "busy done"

@ch.remote(cpus=1)
def child():
    return time.time()

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
        ch.get([nap.remote(), nap.remote()])  # a first wait, which leaves two workers warm
        b = busy.remote(flag)
        time.sleep(0.5)  # it is queued behind the running parent by now
        sent = time.time()
        try:
            started = "%.2f" % (child.remote().result(timeout=LIMIT) - sent)
        except TimeoutError:
            started = "never"
    finally:
        open(flag, "w").close()
        stop.set()
        for t in pool:
            t.join()
    return started, b.result(timeout=60)

print(*parent.remote(os.path.join(tempfile.mkdtemp(), "stop")).result(timeout=150))
"""


@pytest.mark.parametrize("peer", [(2, None), (4, 2), (2, None, True)], ids=["cpus", "quota", "hosting"], indirect=True)
def test_a_busy_parents_child_starts_beside_a_busy_call_it_started(peer):
    # The parent holds both CPUs of the peer (or the tenant's two-CPU quota),
    # and its NumPy threads keep running through its waits. After a first
    # wait it starts a call that computes until the parent sets a flag, then
    # waits on a child before setting it. Waiting, the parent lends both: the
    # computing call takes one and the child the other, within seconds. A
    # peer that kept back the cores the parent's threads use would let in only
    # the computing call, and the child would wait for it forever.
    limit = 20
    r = run_script(BUSY_CALL.replace("LIMIT", str(limit)), home=peer, timeout=200)
    assert r.returncode == 0, r.stderr[-3000:]
    started, done = r.stdout.split()[-3], r.stdout.split()[-2:]
    assert done == ["busy", "done"], r.stdout
    assert started != "never", f"the child did not start within {limit}s"
    assert float(started) < 5, f"the child started {started}s after it was sent"


def _child_beside_a_stream(peer: str, tenant: str) -> float:
    """How long a busy parent's two-CPU child, of tenant "default", waits to
    start while `tenant` keeps three one-CPU calls going on the same peer."""
    flag = os.path.join(tempfile.mkdtemp(), "stop")
    stream = """
        import os, time
        import chimaera as ch
        ch.init(tenant=TENANT)

        @ch.remote(cpus=1)
        def work():
            t = time.time()
            while time.time() - t < 0.3:
                pass

        end, inflight = time.time() + 60, []
        while not os.path.exists(FLAG) and time.time() < end:
            while len(inflight) < 3:
                inflight.append(work.remote())
            _, inflight = ch.wait(inflight, num_returns=1, timeout=30)
            open(FLAG + ".streaming", "w").close()
        ch.get(inflight, timeout=60)
    """.replace("FLAG", repr(flag)).replace("TENANT", repr(tenant))
    streamed = []
    streamer = threading.Thread(target=lambda: streamed.append(run_script(stream, home=peer, timeout=150)))
    streamer.start()
    try:
        r = run_script(
            """
            import os, threading, time
            import numpy as np
            import chimaera as ch

            @ch.remote(cpus=1)
            def nap():
                time.sleep(1.0)

            @ch.remote(cpus=2)
            def child():
                return time.time()

            @ch.remote(cpus=2)
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
                    nap.remote().result()  # a first wait
                    sent = time.time()
                    return child.remote().result(timeout=120) - sent
                finally:
                    stop.set()
                    for t in pool:
                        t.join()

            while not os.path.exists(FLAG + ".streaming"):
                time.sleep(0.05)
            print(parent.remote().result(timeout=150))
            """.replace("FLAG", repr(flag)),
            home=peer,
            timeout=200,
        )
    finally:
        open(flag, "w").close()
        streamer.join()
    assert r.returncode == 0, r.stderr[-3000:]
    assert streamed and streamed[0].returncode == 0, streamed and streamed[0].stderr[-3000:]
    return float(r.stdout.split()[-1])


@pytest.mark.parametrize("peer", [(8, 2), (8, 2, False, True)], ids=["quota", "share"], indirect=True)
def test_a_busy_parents_child_is_let_in_while_another_tenant_keeps_starting_calls(peer):
    # Tenant "default" may hold two CPUs (a quota here, or a cluster share),
    # and its parent's threads keep running through its wait. Another tenant
    # starts a call every tenth of a second on the same peer the whole time.
    # Its calls hold none of the quota, so they must not hold the child back:
    # it starts within seconds, not once their stream ends a minute later.
    waited = _child_beside_a_stream(peer, "other")
    assert waited < 5, f"the child waited {waited:.1f}s for another tenant's calls to stop starting"


@pytest.mark.parametrize("peer", [(8, 4), (8, 4, False, True)], ids=["quota", "share"], indirect=True)
def test_a_busy_parents_child_is_let_in_while_its_own_tenant_keeps_starting_calls(peer):
    # As above, but the stream is the parent's own tenant's, under a quota or
    # share of four. Waiting, the parent lends its two, and the child, queued
    # ahead of the stream's later calls, takes them as the calls running end:
    # it starts within seconds.
    waited = _child_beside_a_stream(peer, "default")
    assert waited < 5, f"the child waited {waited:.1f}s for its own tenant's calls to stop starting"


@pytest.mark.parametrize("peer", [(2, None)], ids=["cpus"], indirect=True)
def test_a_call_let_in_that_idles_holds_back_no_other(peer):
    # The parent's threads keep running through its waits on both CPUs. A
    # call it starts that idles until the parent has its children holds back
    # none of them: the parent lends both CPUs, one to that call and one to
    # each child in turn.
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
                nap.remote().result()  # a first wait
                s = serve.remote(flag)  # it starts as the parent waits below
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
