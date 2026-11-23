"""Failure handling: retries, effects, memory limits, actor death, peer restart."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time

import pytest

import chimaera as ch
from chimaera._runtime import peer_socket
from conftest import BIN, REPO, run_script


def _tmpfile(name: str) -> str:
    return os.path.join(tempfile.mkdtemp(prefix="chr", dir="/tmp"), name)


class RetryMe(Exception):
    pass


@ch.remote
def die_on_first_attempt(marker: str) -> int:
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getpid(), signal.SIGKILL)
    return ch.context().attempt


@ch.remote
def count_and_fail(path: str):
    with open(path, "a") as f:
        f.write("ran\n")
    raise ValueError("user bug")


@ch.remote(effects="external")
def charge_then_die(path: str):
    with open(path, "a") as f:
        f.write("charged\n")
    os.kill(os.getpid(), signal.SIGKILL)


@ch.remote(retry_exceptions=(RetryMe,), max_attempts=5)
def flaky(path: str) -> int:
    with open(path, "a") as f:
        f.write("x\n")
    n = sum(1 for _ in open(path))
    if n < 3:
        raise RetryMe(f"transient {n}")
    return n


@ch.remote(retry_exceptions=(RetryMe,), max_attempts=5)
def always_same(path: str):
    with open(path, "a") as f:
        f.write("x\n")
    raise RetryMe("the same failure every time")


def test_executor_killed_mid_call_is_retried_on_a_new_attempt():
    assert die_on_first_attempt.remote(_tmpfile("m")).result(timeout=60) == 2


def test_user_exception_runs_exactly_once():
    path = _tmpfile("count")
    with pytest.raises(ValueError, match="user bug"):
        count_and_fail.remote(path).result(timeout=60)
    assert open(path).read().count("ran") == 1


def test_external_effect_lost_after_start_is_outcome_unknown_and_never_replayed():
    path = _tmpfile("ledger")
    with pytest.raises(ch.OutcomeUnknown):
        charge_then_die.remote(path).result(timeout=60)
    assert open(path).read().count("charged") == 1


def test_retryable_exceptions_retry_until_success():
    path = _tmpfile("flaky")
    assert flaky.remote(path).result(timeout=60) == 3


def test_a_second_identical_failure_stops_retrying():
    path = _tmpfile("same")
    with pytest.raises(RetryMe):
        always_same.remote(path).result(timeout=60)
    assert sum(1 for _ in open(path)) == 2


def test_memory_limit_breach_ends_in_out_of_memory_with_the_peak():
    @ch.remote(memory_limit="150 MB")
    def hog():
        data = b"\x01" * (400 << 20)
        time.sleep(2)
        return len(data)

    with pytest.raises(ch.OutOfMemory, match=r"memory limit.*peak"):
        hog.remote().result(timeout=120)


def test_actor_restart_policies():
    @ch.remote(restart="fresh")
    class Phoenix:
        def __init__(self):
            self.calls = 0

        def bump(self):
            self.calls += 1
            return self.calls

        def die(self):
            os.kill(os.getpid(), signal.SIGKILL)

    p = Phoenix.remote()
    assert p.bump.remote().result(timeout=30) == 1
    with pytest.raises(ch.ActorDied):
        p.die.remote().result(timeout=60)
    # A fresh instance serves later calls (state starts over).
    assert p.bump.remote().result(timeout=60) == 1

    @ch.remote(restart="never")
    class Mortal:
        def die(self):
            os.kill(os.getpid(), signal.SIGKILL)

        def ping(self):
            return "pong"

    m = Mortal.remote()
    with pytest.raises(ch.ActorDied):
        m.die.remote().result(timeout=60)
    with pytest.raises(ch.ActorDied):
        m.ping.remote().result(timeout=60)


def test_driver_that_disconnects_without_finishing_has_its_run_cancelled():
    r = run_script(
        """
        import os, chimaera as ch

        @ch.remote
        def slow():
            import time
            time.sleep(60)

        f = slow.remote()
        ch._runtime.Runtime.get().client.flush()
        print(ch.init(), f.key[16:].hex(), flush=True)
        import time
        time.sleep(1)
        os._exit(0)   # no atexit: the session just drops
        """
    )
    assert r.returncode == 0, r.stderr
    run_id, task = r.stdout.split()
    run = ch.runs.attach(run_id)
    counts = run.wait(timeout=60)
    assert counts == {"Done": 1}
    [(tid, _value, error)] = list(run.outcomes())
    assert tid == task and isinstance(error, ch.Cancelled)


def _start_peer(home: str) -> subprocess.Popen:
    log = open(os.path.join(home, "peer.log"), "ab")
    p = subprocess.Popen([BIN, "--dir", home, "peer", "start"], stdout=log, stderr=log, start_new_session=True)
    deadline = time.monotonic() + 30
    while not os.path.exists(peer_socket(home)):
        assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
        time.sleep(0.05)
    return p


def test_peer_killed_mid_run_resumes_and_reattach_sees_every_result_once():
    home = tempfile.mkdtemp(prefix="chk", dir="/tmp")
    peer = _start_peer(home)
    driver = subprocess.Popen(
        [
            os.environ.get("PYTHON", "python3") if False else __import__("sys").executable,
            "-c",
            """
import time, chimaera as ch

@ch.remote
def work(i):
    time.sleep(0.004)
    return i

run_id = ch.init(detach=True)
futs = [work.remote(i) for i in range(10_000)]
ch._runtime.Runtime.get().client.flush()
print(run_id, flush=True)
while sum(f.done() for f in futs) < 3000:
    time.sleep(0.02)
print("progress", flush=True)
time.sleep(600)
""",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN),
    )
    try:
        run_id = driver.stdout.readline().strip()
        assert len(run_id) == 32, driver.stderr.read() if driver.poll() is not None else run_id
        assert driver.stdout.readline().strip() == "progress"
        # Kill the peer mid-run, without warning, then the driver.
        os.kill(peer.pid, signal.SIGKILL)
        peer.wait(timeout=10)
        driver.kill()
        os.unlink(peer_socket(home))
        peer = _start_peer(home)
        r = run_script(
            f"""
            import chimaera as ch
            run = ch.runs.attach("{run_id}")
            counts = run.wait(timeout=180)
            values = [v for _, v, e in run.outcomes() if e is None]
            errors = [e for _, _, e in run.outcomes() if e is not None]
            print(len(values), len(set(values)), counts, errors[:3])
            """,
            timeout=240,
            home=home,
        )
        assert r.returncode == 0, r.stderr
        n, distinct, *rest = r.stdout.split(maxsplit=2)
        assert (n, distinct) == ("10000", "10000"), r.stdout
    finally:
        driver.kill()
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        peer.wait(timeout=30)


def _running(pid: int) -> bool:
    """Whether `pid` is still running. An exited process nobody has reaped yet
    (a zombie: in a container, orphans go to a PID 1 that may never reap them)
    has stopped running."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:  # no /proc (macOS): ask ps
        state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        return bool(state) and not state.startswith("Z")


def test_an_executor_whose_peer_dies_exits_even_inside_a_long_call():
    import shutil
    import sys

    home = tempfile.mkdtemp(prefix="chk", dir="/tmp")
    pid_file = os.path.join(home, "executor.pid")
    peer = _start_peer(home)
    driver = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"""
import os, time, chimaera as ch

@ch.remote
def nap():
    with open({pid_file!r} + ".tmp", "w") as f:
        f.write(str(os.getpid()))
    os.rename({pid_file!r} + ".tmp", {pid_file!r})
    time.sleep(600)

f = nap.remote()
time.sleep(600)
""",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python")),
    )
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(pid_file):
            assert time.monotonic() < deadline and driver.poll() is None, "the call never started"
            time.sleep(0.05)
        with open(pid_file) as f:
            pid = int(f.read())
        os.kill(peer.pid, signal.SIGKILL)  # no shutdown message: the executor sees its socket close
        peer.wait(timeout=10)
        deadline = time.monotonic() + 15
        while _running(pid):
            assert time.monotonic() < deadline, "the executor outlived its peer"
            time.sleep(0.1)
    finally:
        driver.kill()
        driver.wait(timeout=10)
        if peer.poll() is None:
            peer.kill()
            peer.wait(timeout=10)
        shutil.rmtree(home, ignore_errors=True)


def test_a_run_ships_only_allowlisted_environment_variables():
    r = run_script(
        """
        import os
        import chimaera as ch

        ch.init(env={"EXPLICIT": "1"})

        @ch.remote
        def seen():
            return [os.environ.get(k) for k in ("SECRET_TOKEN", "CH_TEST_PASS", "EXPLICIT", "LANG")]

        print(seen.remote().result(timeout=60))
        """,
        env={"SECRET_TOKEN": "s3cret", "CH_TEST_PASS": "yes", "CHIMAERA_ENV_PASSTHROUGH": "CH_TEST_*", "LANG": "C.UTF-8"},
    )
    assert r.returncode == 0, r.stderr
    # The secret stays with the client; passthrough patterns, explicit
    # values and the locale reach the executor.
    assert r.stdout.strip() == "[None, 'yes', '1', 'C.UTF-8']"


def test_a_call_never_sees_the_peers_cluster_token():
    # A call is a tenant's code: with the token it could act as an administrator.
    home = tempfile.mkdtemp(prefix="cht", dir="/tmp")
    log = open(os.path.join(home, "peer.log"), "ab")
    env = dict(os.environ, CHIMAERA_CLUSTER_TOKEN="t" * 32, CHIMAERA_TOKEN="d" * 32)
    peer = subprocess.Popen([BIN, "--dir", home, "peer", "start"], stdout=log, stderr=log, start_new_session=True, env=env)
    try:
        deadline = time.monotonic() + 30
        while not os.path.exists(peer_socket(home)):
            assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
            time.sleep(0.05)
        r = run_script(
            """
            import os
            import chimaera as ch

            @ch.remote
            def seen():
                return [os.environ.get(k) for k in ("CHIMAERA_CLUSTER_TOKEN", "CHIMAERA_TOKEN", "CHIMAERA_EXECUTOR")]

            print(seen.remote().result(timeout=60))
            """,
            home=home,
        )
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "[None, None, '1']"
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        peer.wait(timeout=30)


def test_a_call_never_sees_the_cluster_ca_its_peer_was_given():
    # slurm-launch passes the cluster CA to its peers in the environment; with
    # its key, a call could mint peer certificates.
    home = tempfile.mkdtemp(prefix="cht", dir="/tmp")
    log = open(os.path.join(home, "peer.log"), "ab")
    env = dict(os.environ, CHIMAERA_TLS_CA_PEM="pem", CHIMAERA_TLS_CA_KEY="secret-key")
    peer = subprocess.Popen([BIN, "--dir", home, "peer", "start"], stdout=log, stderr=log, start_new_session=True, env=env)
    try:
        deadline = time.monotonic() + 30
        while not os.path.exists(peer_socket(home)):
            assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
            time.sleep(0.05)
        r = run_script(
            """
            import os
            import chimaera as ch

            @ch.remote
            def seen():
                return [os.environ.get(k) for k in ("CHIMAERA_TLS_CA_PEM", "CHIMAERA_TLS_CA_KEY", "CHIMAERA_EXECUTOR")]

            print(seen.remote().result(timeout=60))
            """,
            home=home,
        )
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "[None, None, '1']"
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        peer.wait(timeout=30)


def test_sigterm_lets_running_work_finish_then_stops_the_peer():
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    peer = _start_peer(home)
    try:
        r = run_script(
            f"""
            import os, signal, time
            import chimaera as ch

            @ch.remote
            def slow():
                time.sleep(2)
                return "finished"

            f = slow.remote()
            time.sleep(0.8)  # running now
            os.kill({peer.pid}, signal.SIGTERM)
            print(f.result(timeout=30))
            """,
            home=home,
            timeout=60,
        )
        assert "finished" in r.stdout, r.stdout + r.stderr[-2000:]
        assert peer.wait(timeout=30) == 0, "the peer exits once drained"
    finally:
        if peer.poll() is None:
            peer.kill()


def test_stop_ends_the_peer_while_a_client_is_still_connected():
    home = tempfile.mkdtemp(prefix="chs", dir="/tmp")
    peer = _start_peer(home)
    client = subprocess.Popen(
        [__import__("sys").executable, "-c", "import time, chimaera as ch\nch.remote(lambda: 1).remote().result(timeout=30)\nprint('connected', flush=True)\ntime.sleep(120)"],
        stdout=subprocess.PIPE,
        text=True,
        env=dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN),
    )
    try:
        assert client.stdout.readline().strip() == "connected"
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        # The idle client's session stream must not keep the stopped peer alive.
        assert peer.wait(timeout=20) == 0, "the peer exits although a client is connected"
    finally:
        client.kill()
        if peer.poll() is None:
            peer.kill()
