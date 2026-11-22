"""Cancelling a running call stops it: cooperatively at first, then by
interrupting its main thread, then by killing its executor; never an actor's."""

from __future__ import annotations

import os
import tempfile
import time

import chimaera as ch


def ticks(path: str) -> int:
    try:
        with open(path) as f:
            return len(f.read())
    except FileNotFoundError:
        return 0


@ch.remote
def ticking(path: str, block_interrupts: bool) -> None:
    import signal

    if block_interrupts:  # as a C extension that never returns to Python would
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR2})
    with open(path + ".pid", "w") as f:
        f.write(str(os.getpid()))
    while True:
        with open(path, "a") as f:
            f.write(".")
        time.sleep(0.1)


def start(block_interrupts: bool) -> tuple[ch.Future[None], str]:
    path = os.path.join(tempfile.mkdtemp(prefix="chk", dir="/tmp"), "ticks")
    f = ticking.remote(path, block_interrupts)
    deadline = time.monotonic() + 60
    while ticks(path) < 3 and time.monotonic() < deadline:
        time.sleep(0.05)
    return f, path


def stopped_within(path: str, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        before = ticks(path)
        time.sleep(0.5)
        if ticks(path) == before:
            return True
    return False


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def test_a_cancelled_call_that_never_checks_is_interrupted():
    f, path = start(block_interrupts=False)
    assert f.cancel()
    assert stopped_within(path, 5), "still running 5 s after cancel"
    pid = int(open(path + ".pid").read())
    assert alive(pid), "an interrupt, not a kill: the worker stays for the next call"


def test_a_cancelled_call_that_ignores_interrupts_loses_its_executor():
    f, path = start(block_interrupts=True)
    pid = int(open(path + ".pid").read())
    assert f.cancel()
    assert stopped_within(path, 10), "still running 10 s after cancel"
    assert not alive(pid)


@ch.remote
class Spinner:
    def pid(self) -> int:
        return os.getpid()

    def spin(self, path: str) -> None:
        import signal

        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR2})
        while True:
            with open(path, "a") as f:
                f.write(".")
            time.sleep(0.1)


def test_an_actors_process_is_never_killed_for_a_cancelled_method():
    s = Spinner.options(concurrency=2).remote()
    pid = s.pid.remote().result(timeout=60)
    path = os.path.join(tempfile.mkdtemp(prefix="chk", dir="/tmp"), "ticks")
    f = s.spin.remote(path)
    while ticks(path) < 3:
        time.sleep(0.05)
    f.cancel()
    time.sleep(7)
    assert alive(pid), "the actor's state lives in that process"
    s.close()
