"""A call that waits on calls it started lends its process. While every call
in an executor waits on futures, the peer may run another call of the same run
there, on a thread of its own (a guest), instead of starting a process per
waiting call. Each guest is still measured, labelled and cancelled as its own
call.

Each test runs its own one-CPU peer: the waiting parent holds the only
executor, so its child has nowhere to go but beside it."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, run_script


def _start_peer(**env: str) -> str:
    home = tempfile.mkdtemp(prefix="chh", dir="/tmp")
    with open(os.path.join(home, "peer.log"), "ab") as log:
        subprocess.Popen(
            [BIN, "--dir", home, "peer", "start", "--cpus", "1", "--idle-timeout", "600"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True, env=dict(os.environ, **env),
        )
    deadline = time.monotonic() + 30
    while not os.path.exists(socket_path(home, "peer.sock")):
        assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
        time.sleep(0.05)
    return home


@pytest.fixture
def one_cpu():
    home = _start_peer()
    yield home
    subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


TREE = """
import os
import chimaera as ch

@ch.remote
def node(depth):
    if depth == 0:
        return {os.getpid()}
    kids = [node.remote(depth - 1) for _ in range(3)]
    pids = {os.getpid()}
    for k in kids:
        pids |= k.result()
    return pids

print(len(node.remote(3).result(timeout=120)))
"""


def test_a_child_runs_in_its_waiting_parents_process(one_cpu):
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def child():
            return os.getpid()

        @ch.remote
        def parent():
            return os.getpid(), child.remote().result()

        print(*parent.remote().result(timeout=60))
        """,
        home=one_cpu,
    )
    assert r.returncode == 0, r.stderr
    parent, child = r.stdout.split()
    assert parent == child, r.stdout


def test_a_tree_of_waits_runs_in_a_few_processes_and_the_switch_turns_it_off(one_cpu):
    # 13 waiting calls and 27 leaves on one CPU.
    r = run_script(TREE, home=one_cpu, timeout=180)
    assert r.returncode == 0, r.stderr
    assert int(r.stdout.split()[-1]) <= 3, r.stdout
    off = _start_peer(CHIMAERA_HOST_WHILE_WAITING="0")
    try:
        r = run_script(TREE, home=off, timeout=180)
        assert r.returncode == 0, r.stderr
        assert int(r.stdout.split()[-1]) >= 10, r.stdout  # a process per waiting call, as before
    finally:
        subprocess.run([BIN, "--dir", off, "stop"], capture_output=True, timeout=60)


def test_what_a_guest_and_its_host_print_reaches_the_driver_labelled(one_cpu):
    r = run_script(
        """
        import subprocess
        import threading
        import chimaera as ch

        @ch.remote
        def child():
            print("child main")
            t = threading.Thread(target=lambda: print("child thread"))
            t.start()
            t.join()
            subprocess.run(["echo", "child program"], check=True)
            return 1

        @ch.remote
        def parent():
            print("parent before")
            v = child.remote().result()
            print("parent after")
            return v

        print("RESULT", parent.remote().result(timeout=60))
        """,
        home=one_cpu,
    )
    assert r.returncode == 0, r.stderr
    lines = r.stdout.splitlines()
    for what in ("before", "after"):
        assert f"(parent pid={lines[0].split('pid=')[1].split(')')[0]}) parent {what}" in lines, r.stdout
    # The driver names only the calls it made; a call made inside one is
    # labelled by its pid. Each line is the child's own, not its host's.
    for what in ("main", "thread", "program"):
        assert any(ln.startswith("(") and ln.endswith(f"child {what}") and not ln.startswith("(parent") for ln in lines), r.stdout


def test_a_guest_is_measured_apart_from_its_waiting_host(one_cpu):
    r = run_script(
        """
        import time
        import numpy as np
        import chimaera as ch
        from chimaera._runtime import Runtime

        @ch.remote
        def child():
            a = np.ones(20 * 2**20 // 8)
            end = time.process_time() + 2.0
            while time.process_time() < end:
                pass
            time.sleep(0.3)  # several 100 ms memory samples
            return float(a[-1])

        @ch.remote
        def parent():
            a = np.ones(300 * 2**20 // 8)
            c = child.remote()
            c.result()
            return c.key, float(a[-1])

        def measures(key):
            # Read by the driver, so the parent spends no CPU waiting for them.
            client = Runtime.get().client
            deadline = time.monotonic() + 30
            while not (attempts := client.explain(key)["attempts"]) or not attempts[-1].get("measures"):
                assert time.monotonic() < deadline
                time.sleep(0.1)
            return attempts[-1]["measures"]

        f = parent.remote()
        key, _ = f.result(timeout=120)  # a few seconds, unless the host is swamped
        child, mine = measures(key), measures(f.key)
        for name, m in (("child", child), ("parent", mine)):
            print(name, m["memory_private_peak_bytes"][0] / 2**20, m["cpu_seconds"][0])
        """,
        home=one_cpu,
    )
    assert r.returncode == 0, r.stderr
    got = {name: (float(mib), float(cpu)) for name, mib, cpu in (ln.split() for ln in r.stdout.splitlines()[-2:])}
    child_mib, child_cpu = got["child"]
    parent_mib, parent_cpu = got["parent"]
    # Its own 20 MiB, not the 300 its host held when it began.
    assert 15 <= child_mib < 100, got
    assert parent_mib >= 300, got
    # The child's 2 s loop is its own; the waiting parent's CPU leaves it out.
    # The parent's own work is importing numpy and allocating 300 MiB.
    assert 1.8 <= child_cpu <= 3.0, got
    assert parent_cpu < child_cpu - 0.6, got


def test_a_cancelled_guest_stops_while_its_host_goes_on(one_cpu):
    r = run_script(
        """
        import time
        import chimaera as ch

        @ch.remote
        def spin():
            while True:
                sum(range(1000))

        @ch.remote
        def parent():
            s = spin.remote()
            try:
                s.result(timeout=1)  # long enough for it to start beside this call
            except TimeoutError:
                pass
            s.cancel()
            t = time.monotonic()
            try:
                s.result(timeout=20)
            except ch.Cancelled:
                return "cancelled", time.monotonic() - t
            return "finished", 0.0

        print(*parent.remote().result(timeout=60))
        """,
        home=one_cpu,
    )
    assert r.returncode == 0, r.stderr
    what, waited = r.stdout.split()
    assert what == "cancelled" and float(waited) < 10, r.stdout


def test_a_call_with_a_memory_limit_runs_in_a_process_of_its_own(one_cpu):
    # A kill for memory takes the whole process: a call that can be killed for
    # its own memory is never a guest, so its waiting parent is never killed
    # with it (nor again on each of its retries).
    r = run_script(
        """
        import time
        import numpy as np
        import chimaera as ch

        @ch.remote(memory_limit="50MiB", max_attempts=1)
        def greedy():
            a = np.ones(200 * 2**20 // 8)
            time.sleep(5)
            return float(a[-1])

        @ch.remote
        def parent():
            try:
                greedy.remote().result()
            except ch.OutOfMemory:
                return "greedy ran out of memory"
            return "greedy finished"

        f = parent.remote()
        print(f.result(timeout=120))
        print(len(f.explain()["attempts"]))
        """,
        home=one_cpu,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    result, attempts = r.stdout.splitlines()[-2:]
    assert result == "greedy ran out of memory", r.stdout
    assert attempts == "1", r.stdout  # the parent was never killed beside it
