"""Where a local peer cannot start, the program says why at once, and
`chimaera doctor` names the step that fails: the Python, Unix sockets, the
cluster."""

from __future__ import annotations

import fcntl
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

import pytest

from chimaera import _runtime
from chimaera._errors import ChimaeraError
from conftest import BIN, REPO
from test_cluster import Cluster, free_ports

# What the peer prints when the sandbox refuses its socket (doctor.rs socket_error).
EPERM_LINE = (
    "chimaera: bind() on the Unix socket {home}/exec.sock: Operation not permitted (os error 1). Unix sockets are "
    "refused here, by a sandbox or seccomp profile that denies them or by the directory's permissions: allow Unix "
    "sockets for chimaera, or set CHIMAERA_HOME (or --dir) to a directory this user can write on a local disk"
)


def _stub(where: str, script: str) -> str:
    path = os.path.join(where, "chimaera")
    with open(path, "w") as f:
        f.write("#!/bin/sh\n" + script)
    os.chmod(path, 0o755)
    return path


def test_a_peer_that_exits_at_start_fails_the_program_at_once_with_its_error(tmp_path, monkeypatch):
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")  # short: a socket path must fit
    try:
        # As the real peer does in a sandbox: log a line, print its error, exit 1.
        line = EPERM_LINE.format(home=home)
        stub = _stub(str(tmp_path), f'echo "INFO starting" >&2\necho "{line}" >&2\nexit 1\n')
        monkeypatch.setenv("CHIMAERA_BIN", stub)
        t0 = time.monotonic()
        with pytest.raises(ChimaeraError) as err:
            _runtime._ensure_local_peer(home)
        assert time.monotonic() - t0 < 5, "not the 20 s launch timeout"
        msg = str(err.value)
        assert line.removeprefix("chimaera: ") in msg
        assert "exited while starting (status 1)" in msg and os.path.join(home, "peer.log") in msg
        assert "INFO starting" not in msg, "the error line, not the whole log"
    finally:
        shutil.rmtree(home, ignore_errors=True)


def test_a_peer_that_loses_the_directory_lock_waits_for_the_one_that_won(monkeypatch):
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    try:
        monkeypatch.setenv("CHIMAERA_BIN", BIN)
        # The winner: holds the lock and opens its socket a moment later. The
        # real peer started here finds the lock taken and says so by its exit
        # status; the program waits without touching the lock itself (a probe
        # that took it could make a peer starting at that moment lose it).
        with open(os.path.join(home, "peer.lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            held = subprocess.run([BIN, "--dir", home, "peer", "start", "--idle-timeout", "600"],
                                  capture_output=True, text=True, timeout=60)
            assert held.returncode == _runtime.DIR_HELD_EXIT, (held.returncode, held.stderr[-2000:])
            assert f"chimaera: another peer is running in {home}" in held.stderr, held.stderr[-2000:]
            sock = _runtime.peer_socket(home)
            threading.Timer(1.0, lambda: open(sock, "w").close()).start()
            _runtime._ensure_local_peer(home)
        assert os.path.exists(sock)
    finally:
        shutil.rmtree(home, ignore_errors=True)


def _init(home: str, *wrapper: str) -> tuple[subprocess.CompletedProcess, float]:
    """Run `ch.init()` in a new program (under `wrapper`) with `home` as its
    local peer's directory: what it printed, and how long it took."""
    env = dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    argv = [*wrapper, sys.executable, "-c", "import chimaera as ch; ch.init()"]
    t0 = time.monotonic()
    done = subprocess.run(argv, capture_output=True, text=True, timeout=120, env=env)
    return done, time.monotonic() - t0


# sandbox-exec is how macOS makes bind() or connect() on a Unix socket return
# a real EPERM; Linux needs a container with a seccomp profile for that, which
# pytest cannot set up.
@pytest.mark.skipif(shutil.which("sandbox-exec") is None, reason="needs macOS sandbox-exec")
@pytest.mark.parametrize(
    ("denied", "named"),
    [
        # The peer, started inside, cannot listen: it exits saying why.
        ("network-bind (local unix-socket)", "the local peer exited while starting: bind() on the Unix socket {home}/"),
        # The peer listens, but the program may not connect to it.
        ("network-outbound (remote unix-socket)", "connect() on the Unix socket {home}/peer.sock: "),
    ],
)
def test_a_sandbox_that_refuses_unix_sockets_is_named_before_the_launch_timeout(denied, named):
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    try:
        done, took = _init(home, "sandbox-exec", "-p", f"(version 1)(allow default)(deny {denied})")
        assert done.returncode != 0
        assert took < 15, f"{took:.1f} s: waited for the launch timeout"
        assert named.format(home=home) in done.stderr.replace(" (status 1)", ""), done.stderr[-2000:]
        assert "Operation not permitted" in done.stderr
        assert "allow Unix sockets for" in done.stderr and "set CHIMAERA_HOME" in done.stderr
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        shutil.rmtree(home, ignore_errors=True)


def test_a_peer_socket_the_program_may_not_use_is_named_and_kept():
    # A live peer's socket that refuses this program (here by its file mode,
    # as another user's would) is not a dead peer's to remove and replace.
    if os.geteuid() == 0:
        pytest.skip("root connects whatever the socket's mode")
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    try:
        sock = _runtime.peer_socket(home)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(sock)
            listener.listen()
            os.chmod(sock, 0)
            done, took = _init(home)
        assert done.returncode != 0 and took < 15, (took, done.stderr[-2000:])
        assert f"connect() on the Unix socket {sock}: Permission denied (os error 13)" in done.stderr, done.stderr
        assert "set CHIMAERA_HOME to a directory of your own" in done.stderr
        assert os.path.exists(sock), "the live peer's socket was removed"
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)  # one started in its place
        shutil.rmtree(home, ignore_errors=True)


def _doctor(*args: str, env: dict[str, str] | None = None, command: list[str] | None = None) -> str:
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    try:
        skip = ("CHIMAERA_CLUSTER", "CHIMAERA_ADDRESS", "CHIMAERA_PYTHON")
        full = {k: v for k, v in os.environ.items() if k not in skip}
        full.update(CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
        full["CHIMAERA_CLUSTERS"] = os.path.join(home, "no-profiles.toml")  # not the user's profile default
        full.update(env or {})
        argv = [*(command or [BIN]), "--dir", home, "doctor", *args]
        done = subprocess.run(argv, capture_output=True, text=True, timeout=120, env=full)
        assert done.returncode == 0, done.stderr
        return done.stdout
    finally:
        shutil.rmtree(home, ignore_errors=True)


def test_doctor_names_the_python_unix_sockets_and_no_cluster():
    out = _doctor(env={"CHIMAERA_PYTHON": sys.executable})
    assert f"python {platform.python_version()} ({sys.executable})" in out, out
    assert "imports chimaera" in out, out
    assert "unix sockets: socket, bind and connect work in" in out, out
    assert "cluster: none selected; drivers here start a local peer" in out, out
    broken = _doctor(env={"CHIMAERA_PYTHON": "/nonexistent/python3"})
    assert "cannot run /nonexistent/python3" in broken and "set CHIMAERA_PYTHON" in broken, broken


def test_doctor_names_the_step_at_which_a_cluster_fails():
    (port,) = free_ports(1)
    out = _doctor("--cluster", f"tcp://127.0.0.1:{port}")
    assert f"tcp://127.0.0.1:{port}: connect(): " in out and "no peer listens there" in out, out
    cluster = Cluster(1, clients=True)
    try:
        address = f"tcp://{cluster.client_addrs[0]}"
        out = _doctor("--cluster", address, env={"CHIMAERA_TOKEN_FILE": cluster.token})
        assert f"{address}: answers: Chimaera " in out, out
        out = _doctor("--cluster", address, env={"CHIMAERA_TOKEN": "x" * 32})
        assert f"{address}: refuses this driver: " in out and "check the cluster token" in out, out
    finally:
        cluster.close()


def test_the_chimaera_command_has_the_doctor_check_its_own_python():
    # The `chimaera` console script runs _cli.main with the wheel's
    # interpreter; the doctor must check that one, not python3 on PATH.
    cli = [sys.executable, "-c", "import sys; from chimaera._cli import main; sys.argv[0] = 'chimaera'; main()"]
    out = _doctor(command=cli)
    assert f"python {platform.python_version()} ({sys.executable})" in out, out
