"""Defaults that keep a naive driver safe and informed: submission waits for
room instead of filling memory, a wait on a call that cannot start says why,
and log records from tasks reach the driver at its level."""

from __future__ import annotations

from conftest import run_script


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
