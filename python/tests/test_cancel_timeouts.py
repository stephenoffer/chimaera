"""A peer's cancel escalation is configurable: CHIMAERA_CANCEL_INTERRUPT_SECONDS, then CHIMAERA_CANCEL_KILL_SECONDS."""

import os
import subprocess
import tempfile

import pytest

from conftest import BIN, run_script

SCRIPT = """
import os, tempfile, time
import chimaera as ch

marks = tempfile.mkdtemp()

@ch.remote
def sleeper(path, swallow):
    with open(path, "w") as f:
        f.write(str(os.getpid()))
    try:
        while True:
            try:
                time.sleep(60)
                return
            except ch.Cancelled:  # swallowed, as a C extension or a catch-all handler would
                if not swallow:
                    raise
    finally:  # when the call's own code stopped (a killed worker never gets here)
        with open(path + ".stopped", "w") as f:
            f.write(str(time.time()))

path = os.path.join(marks, "pid")
f = sleeper.remote(path, SWALLOW)
deadline = time.monotonic() + 60
while not os.path.exists(path) and time.monotonic() < deadline:
    time.sleep(0.02)
time.sleep(0.05)  # the pid is written; the call is in its sleep
pid = int(open(path).read())
cancelled_at = time.time()
f.cancel()
stopped, alive = None, True
while time.time() - cancelled_at < 3.5 and stopped is None and alive:  # until it stops, or its worker goes
    if stopped is None and os.path.exists(path + ".stopped"):
        stopped = round(float(open(path + ".stopped").read()) - cancelled_at, 2)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        alive = False
    time.sleep(0.02)
print("STOPPED", stopped, "WORKER", "alive" if alive else "gone")
"""


def _run(env: dict[str, str], *, swallow: bool) -> tuple[float | None, str]:
    home = tempfile.mkdtemp(prefix="chk", dir="/tmp")
    try:
        r = run_script(SCRIPT.replace("SWALLOW", str(swallow)), home=home, env=env)
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert r.returncode == 0, r.stderr[-2000:]
    words = r.stdout.split()
    stopped = words[words.index("STOPPED") + 1]
    return (None if stopped == "None" else float(stopped)), words[words.index("WORKER") + 1]


def test_a_shorter_interrupt_stops_a_cancelled_call_sooner_and_keeps_its_worker():
    stopped, worker = _run({"CHIMAERA_CANCEL_INTERRUPT_SECONDS": "0.2", "CHIMAERA_CANCEL_KILL_SECONDS": "30"}, swallow=False)
    assert stopped is not None and stopped < 1.5, stopped  # unset, the interrupt comes at 2 s
    assert worker == "alive"


def test_a_shorter_kill_ends_a_call_that_ignores_its_interrupt_by_killing_its_worker():
    stopped, worker = _run({"CHIMAERA_CANCEL_INTERRUPT_SECONDS": "0.2", "CHIMAERA_CANCEL_KILL_SECONDS": "1"}, swallow=True)
    assert worker == "gone"  # within 3.5 s of the cancel; unset, the kill comes at 5 s
    assert stopped is None  # killed: its code never finished


@pytest.mark.parametrize(
    "env, message",
    [
        ({"CHIMAERA_CANCEL_INTERRUPT_SECONDS": "3", "CHIMAERA_CANCEL_KILL_SECONDS": "2"}, "must be longer than"),
        ({"CHIMAERA_CANCEL_KILL_SECONDS": "soon"}, "expected a number of seconds above 0"),
        ({"CHIMAERA_CANCEL_INTERRUPT_SECONDS": "0"}, "expected a number of seconds above 0"),
    ],
)
def test_a_bad_cancel_setting_stops_the_peer_from_starting(env, message):
    home = tempfile.mkdtemp(prefix="chk", dir="/tmp")
    try:
        p = subprocess.run(
            [BIN, "--dir", home, "peer", "start"], env={**os.environ, **env}, capture_output=True, text=True, timeout=60
        )
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert p.returncode != 0
    assert message in p.stderr + p.stdout, (p.stdout[-1000:], p.stderr[-1000:])
