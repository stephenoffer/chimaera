"""What a script's end does to its work: it waits for calls and maps it never
waited on (as concurrent.futures waits for its pool), and cancels them when
the script failed or was interrupted."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time

from conftest import BIN, HOME, REPO, run_script


def test_a_call_the_script_never_waited_on_finishes_before_it_exits():
    marker = os.path.join(tempfile.mkdtemp(prefix="chx", dir="/tmp"), "done")
    out = run_script(
        f"""
        import chimaera as ch

        @ch.remote
        def slow(path):
            import time
            time.sleep(1)
            open(path, "w").write("done")

        slow.remote({marker!r})   # not kept, not waited on
        """
    )
    assert out.returncode == 0, out.stderr
    assert os.path.exists(marker), "the call ran to the end"
    assert "waiting for 1 unfinished call" in out.stderr


def test_a_map_never_read_still_runs_every_item():
    d = tempfile.mkdtemp(prefix="chx", dir="/tmp")
    out = run_script(
        f"""
        import chimaera as ch

        @ch.remote
        def touch(i):
            open(f"{d}/{{i}}", "w").close()

        touch.map(range(300))   # never iterated
        """
    )
    assert out.returncode == 0, out.stderr
    assert len(os.listdir(d)) == 300
    assert "unread map" in out.stderr


def test_a_script_that_fails_cancels_its_work_instead_of_waiting():
    t0 = time.monotonic()
    out = run_script(
        """
        import chimaera as ch

        @ch.remote
        def sleeper():
            import time
            time.sleep(60)

        f = sleeper.remote()
        raise ValueError("the script broke")
        """
    )
    assert out.returncode != 0
    assert time.monotonic() - t0 < 30, "did not wait for the 60 s call"
    assert "cancelling 1 unfinished call: the script failed" in out.stderr


def test_ctrl_c_cancels_the_scripts_work():
    pidfile = os.path.join(tempfile.mkdtemp(prefix="chx", dir="/tmp"), "pid")
    path = os.path.join(os.path.dirname(pidfile), "script.py")
    with open(path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""
                import signal
                signal.signal(signal.SIGINT, signal.default_int_handler)  # as in a terminal
                import chimaera as ch

                @ch.remote
                def sleeper(p):
                    import os, time
                    open(p, "w").write(str(os.getpid()))
                    time.sleep(60)

                sleeper.remote({pidfile!r}).result()
                """
            )
        )
    env = dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    p = subprocess.Popen([sys.executable, path], stderr=subprocess.PIPE, text=True, env=env)
    deadline = time.monotonic() + 60
    while not os.path.exists(pidfile) and time.monotonic() < deadline:
        time.sleep(0.05)
    p.send_signal(signal.SIGINT)
    try:
        _, err = p.communicate(timeout=20)
    finally:
        p.kill()
    assert "cancelling 1 unfinished call: the script was interrupted" in err, err[-2000:]


def test_a_generator_call_nobody_reads_does_not_hold_up_exit():
    t0 = time.monotonic()
    out = run_script(
        """
        import chimaera as ch

        @ch.remote
        def numbers():
            yield from range(100_000)   # far more than the window ahead of a reader

        stream = numbers.remote()
        """
    )
    assert out.returncode == 0, out.stderr
    assert time.monotonic() - t0 < 30


def test_calls_see_only_their_own_runs_variables_not_those_of_whoever_started_the_peer():
    home = tempfile.mkdtemp(prefix="chv", dir="/tmp")
    script = """
        import os
        import chimaera as ch

        @ch.remote
        def foo():
            return os.environ.get("FOO")

        print("FOO", foo.remote().result(timeout=60))
        """
    try:
        first = run_script(script, home=home, env={"FOO": "first"})  # starts the peer
        second = run_script(script, home=home, env={"FOO": "second"})
        passed = run_script(script, home=home, env={"FOO": "third", "CHIMAERA_ENV_PASSTHROUGH": "FOO"})
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert "FOO None" in first.stdout, first.stdout + first.stderr
    assert "FOO None" in second.stdout, second.stdout + second.stderr
    assert "FOO third" in passed.stdout, passed.stdout + passed.stderr


def test_shutdown_lets_a_script_connect_again_and_init_ends_its_run_as_a_block():
    out = run_script(
        """
        import chimaera as ch

        @ch.remote
        def run_of():
            return str(ch.runs.current_run())   # inside a call: the caller's run

        with ch.init() as first:
            assert run_of.remote().result(timeout=60) == str(first)
            pending = run_of.remote()      # finished by the block's end
        second = ch.init()
        assert second != first and run_of.remote().result(timeout=60) == str(second)
        ch.shutdown()
        ch.shutdown()                      # a second one does nothing
        print("RUNS", first, second, ch.runs.attach(first).status())
        """
    )
    assert out.returncode == 0, out.stderr
    assert "RUNS" in out.stdout and "'Done': 2" in out.stdout, out.stdout


def test_a_large_peer_log_is_rotated_when_the_next_peer_starts():
    home = tempfile.mkdtemp(prefix="chl", dir="/tmp")
    with open(os.path.join(home, "peer.log"), "wb") as f:
        f.truncate(17 << 20)  # past the 16 MiB a peer log may reach
    try:
        out = run_script("import chimaera as ch\nprint('OK', ch.put(1).result())", home=home)
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert "OK 1" in out.stdout, out.stderr
    assert os.path.getsize(os.path.join(home, "peer.log.1")) == 17 << 20
    assert os.path.getsize(os.path.join(home, "peer.log")) < 1 << 20
