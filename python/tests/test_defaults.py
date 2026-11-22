"""Defaults that keep a naive driver safe and informed: submission waits for
room instead of filling memory, a wait on a call that cannot start says why,
and log records from tasks reach the driver at its level."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pytest
from conftest import BIN, run_script

from chimaera._executor import python_mismatch

PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"


def test_a_function_wrapped_anew_for_every_call_warns_once():
    # ch.remote(lambda ...) inside a loop sends a new function each time, each
    # pickled and hashed again (6x slower than a function defined once).
    r = run_script(
        """
        import warnings
        import chimaera as ch

        warnings.simplefilter("always")
        assert ch.get([ch.remote(lambda x: x).remote(i) for i in range(150)], timeout=120) == list(range(150))
        once = ch.remote(lambda x: 2 * x)
        assert ch.get([once.remote(i) for i in range(150)], timeout=120) == [2 * i for i in range(150)]
        """,
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert r.stderr.count("CH120") == 1 and "Define the remote function once" in r.stderr, r.stderr[-2000:]


def test_a_cuda_error_in_a_call_given_no_gpu_says_how_to_ask_for_one():
    # The peer hides GPUs (CUDA_VISIBLE_DEVICES empty) from calls that did not
    # ask for one, on machines that have them; torch then raises an error that
    # never says why. Simulated here: no GPU on the test machine.
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def cuda(hidden):
            if hidden:
                os.environ["CUDA_VISIBLE_DEVICES"] = ""
            else:
                os.environ.pop("CUDA_VISIBLE_DEVICES", None)
            raise RuntimeError("No CUDA GPUs are available")

        for hidden in (True, False):
            try:
                cuda.remote(hidden).result(timeout=60)
            except Exception as e:
                notes = " ".join(getattr(e, "__notes__", []))
                print("HINT" if "CH119" in notes and "gpus=1" in notes else "PLAIN", hidden)
        """
    )
    assert r.returncode == 0, r.stderr
    assert "HINT True" in r.stdout and "PLAIN False" in r.stdout, r.stdout + r.stderr[-1500:]


def test_a_function_that_will_not_load_on_another_python_says_so(monkeypatch):
    # Workers learn the program's Python version from its run...
    r = run_script(
        """
        import os
        import chimaera as ch

        @ch.remote
        def driver_python():
            return os.environ.get("CHIMAERA_DRIVER_PYTHON")

        print(driver_python.remote().result(timeout=60))
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split()[-1] == PYTHON, r.stdout
    # ...and a function that fails to load names a version difference (a
    # remote cluster's workers run the cluster's Python), and nothing else.
    monkeypatch.setenv("CHIMAERA_DRIVER_PYTHON", "3.99")
    err = python_mismatch(TypeError("code() argument 13 must be bytes, not int"))
    assert err is not None and "CH115" in str(err) and "Python 3.99" in str(err) and f"Python {PYTHON}" in str(err)
    monkeypatch.setenv("CHIMAERA_DRIVER_PYTHON", PYTHON)
    assert python_mismatch(TypeError("x")) is None


def test_a_driver_submitting_faster_than_calls_finish_waits_for_room():
    r = run_script(
        """
        import time, warnings
        import numpy as np
        import chimaera as ch

        @ch.remote
        def slow(a):
            time.sleep(0.3)
            return float(a[0])

        warnings.simplefilter("always")
        started = time.monotonic()
        futures = [slow.remote(np.full(2 << 20, float(i))) for i in range(12)]  # 16 MiB each, all distinct
        submitted = time.monotonic() - started
        assert [f.result(timeout=120) for f in futures] == [float(i) for i in range(12)]
        print(round(submitted, 2))
        """,
        env={"CHIMAERA_MAX_PENDING_BYTES": str(48 << 20)},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert "CH110" in r.stderr, r.stderr
    # At most three 16 MiB calls are outstanding, so submitting twelve takes
    # several of their 0.3 s turns.
    assert float(r.stdout.split()[-1]) >= 0.6, r.stdout


def test_calls_reading_one_stored_value_do_not_wait_for_room():
    # A ch.put value is stored once, before its calls: passing it to many
    # calls adds no pending memory, so it must not count once per call.
    r = run_script(
        """
        import time, warnings
        import numpy as np
        import chimaera as ch

        @ch.remote
        def slow(a):
            time.sleep(0.3)
            return float(a[0])

        warnings.simplefilter("always")
        v = ch.put(np.ones(2 << 20))  # 16 MiB
        started = time.monotonic()
        futures = [slow.remote(v) for _ in range(12)]
        submitted = time.monotonic() - started
        assert [f.result(timeout=120) for f in futures] == [1.0] * 12
        print(round(submitted, 2))
        """,
        env={"CHIMAERA_MAX_PENDING_BYTES": str(48 << 20)},
        timeout=180,
    )
    assert r.returncode == 0, r.stderr
    assert "CH110" not in r.stderr, r.stderr
    assert float(r.stdout.split()[-1]) < 0.3, r.stdout


def test_a_wait_on_a_call_that_cannot_start_says_why():
    r = run_script(
        """
        import os, time
        import chimaera as ch


        @ch.remote
        def hog(seconds):
            time.sleep(seconds)
            return seconds

        cores = ch._runtime.Runtime.get().info["usable_cores"]
        first = hog.options(cpus=cores).remote(3)
        second = hog.options(cpus=cores).remote(0)
        print(second.result(timeout=60))
        """,
        env={"CHIMAERA_STALL_NOTICE_S": "1"},
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "chimaera: waited" in r.stderr and "needs" in r.stderr, r.stderr


def test_log_records_from_tasks_reach_the_driver_at_its_level():
    r = run_script(
        """
        import logging
        import chimaera as ch

        logging.basicConfig(level=logging.INFO)

        @ch.remote
        def work():
            logging.getLogger("task").info("hello from a task")
            logging.getLogger("task").debug("too detailed")
            return 1

        print(work.remote().result(timeout=60))
        """,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "hello from a task" in r.stderr + r.stdout
    assert "too detailed" not in r.stderr + r.stdout


def test_a_program_warns_when_the_local_peer_runs_another_binary(tmp_path):
    import os
    import shutil
    import subprocess
    import tempfile
    import time

    from conftest import BIN

    home = tempfile.mkdtemp(prefix="chb", dir="/tmp")
    exe = tmp_path / "chimaera"
    shutil.copy2(BIN, exe)
    script = "import chimaera as ch\n@ch.remote\ndef f():\n    return 1\nprint(f.remote().result(timeout=60))\n"
    try:
        first = run_script(script, home=home, env={"CHIMAERA_BIN": str(exe)})
        assert first.returncode == 0, first.stderr
        assert "CH111" not in first.stderr, "the peer this program started is its own binary"
        later = time.time_ns() + 10**9
        os.utime(exe, ns=(later, later))  # rebuilt while the peer keeps running
        second = run_script(script, home=home, env={"CHIMAERA_BIN": str(exe)})
        assert second.returncode == 0, second.stderr
        assert "CH111" in second.stderr and "chimaera stop" in second.stderr, second.stderr
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


def test_work_waiting_on_cpu_that_actors_hold_says_so():
    import os
    import subprocess
    import tempfile
    import time

    from chimaera._native import socket_path
    from conftest import BIN

    home = tempfile.mkdtemp(prefix="cha", dir="/tmp")
    subprocess.Popen(
        [BIN, "--dir", home, "peer", "start", "--cpus", "2", "--idle-timeout", "60"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    deadline = time.monotonic() + 30
    while not os.path.exists(socket_path(home, "peer.sock")) and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        r = run_script(
            """
            import time
            import chimaera as ch

            @ch.remote(cpus=2)
            class Holder:
                def ping(self):
                    return 1

            @ch.remote(cpus=1)
            def work():
                return 1

            h = Holder.remote()
            h.ping.remote().result(timeout=60)  # the actor now holds both CPUs
            f = work.remote()
            time.sleep(1)
            print(f.explain()["pending_reason"])
            h.close()
            print(f.result(timeout=60))
            """,
            home=home,
            timeout=120,
        )
        assert r.returncode == 0, r.stderr
        assert "held by 1 actor(s)" in r.stdout and "cpus=" in r.stdout, r.stdout
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)


def _pids_cgroup() -> str | None:
    """A fresh cgroup with the pids controller, or None: not Linux, no
    writable cgroup v2, or pids not delegated outside a container (moving a
    host's processes around is not a test's business)."""
    root = "/sys/fs/cgroup"
    control = os.path.join(root, "cgroup.subtree_control")
    if sys.platform != "linux" or not os.access(control, os.W_OK):
        return None
    try:
        with open(control) as f:
            delegated = "pids" in f.read().split()
        if not delegated:
            if not os.path.exists("/.dockerenv"):
                return None
            # A cgroup holding processes cannot give its children controllers
            # (cgroup v2): move this container's processes into a leaf first,
            # as the peer does for isolated attempts.
            leaf = os.path.join(root, "init")
            os.makedirs(leaf, exist_ok=True)
            with open(os.path.join(root, "cgroup.procs")) as f:
                pids = f.read().split()
            for pid in pids:
                try:
                    with open(os.path.join(leaf, "cgroup.procs"), "w") as f:
                        f.write(pid)
                except OSError:
                    pass  # exited meanwhile
            with open(control, "w") as f:
                f.write("+pids")
        path = os.path.join(root, f"chimaera-pids-{os.getpid()}")
        os.makedirs(path, exist_ok=True)
    except OSError:
        return None
    return path if os.path.exists(os.path.join(path, "pids.max")) else None


def test_under_a_task_limit_calls_wait_for_workers_instead_of_failing(tmp_path):
    # A pod's pids.max counts every thread. Starting more workers than fit
    # failed with EAGAIN ("executor process exited") and lost the calls; now
    # the peer starts only what fits, calls wait, and status names the limit.
    cg = _pids_cgroup()
    if cg is None:
        pytest.skip("needs a writable cgroup v2 with the pids controller (the Linux dev container)")
    home = str(tmp_path / "home")

    def enter() -> None:
        with open(os.path.join(cg, "cgroup.procs"), "w") as f:
            f.write(str(os.getpid()))

    peer = subprocess.Popen(
        [BIN, "--dir", home, "peer", "start", "--cpus", "4"],
        preexec_fn=enter,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(os.path.join(home, "peer.sock")) and time.monotonic() < deadline:
            time.sleep(0.2)
        time.sleep(1)
        with open(os.path.join(cg, "pids.current")) as f:
            idle = int(f.read())
        with open(os.path.join(cg, "pids.max"), "w") as f:
            f.write(str(idle + 10))  # room for two one-thread workers (3 tasks each), not the four it wants
        r = run_script(
            """
            import time
            import chimaera as ch

            @ch.remote
            def work(i):
                time.sleep(0.2)
                return i

            print("DONE", sum(ch.get([work.remote(i) for i in range(16)], timeout=120)))
            """,
            env={"CHIMAERA_HOME": home},
            timeout=180,
        )
        assert r.returncode == 0, r.stderr[-2000:]
        assert r.stdout.split()[-1] == str(sum(range(16))), r.stdout
        status = subprocess.run([BIN, "--dir", home, "status", "--json"], capture_output=True, text=True, timeout=30)
        assert any("task limit" in a for a in json.loads(status.stdout)["automatic"]), status.stdout
    finally:
        peer.terminate()
        try:
            peer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            peer.kill()
            peer.wait(timeout=30)
        try:
            os.rmdir(cg)
        except OSError:
            pass  # a worker still exiting: the cgroup goes with the container
