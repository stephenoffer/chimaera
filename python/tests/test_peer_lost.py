"""A program waiting on a result does not hang when its peer goes away: once
the session cannot be resumed, the wait raises InfrastructureFailure."""

from __future__ import annotations

import signal
import subprocess

import pytest

from test_cluster import Cluster

DRIVER = """
import time
import chimaera as ch

@ch.remote
def slow():
    time.sleep(60)
    return 1

f = slow.remote()
time.sleep(1)
print("WAITING", flush=True)
try:
    print("RESULT", f.result(), flush=True)
except ch.errors.InfrastructureFailure as e:
    print("RAISED", e, flush=True)
"""


@pytest.mark.parametrize("restart", [False, True], ids=["peer-gone", "peer-restarted"])
def test_a_wait_raises_when_the_peer_is_lost(restart):
    c = Cluster(1)
    try:
        d = c.driver(0, DRIVER)
        assert d.stdout.readline().startswith("WAITING"), d.stderr.read()[-2000:]
        c.signal(0, signal.SIGKILL)
        if restart:
            c.start(0)  # a new process: it does not know the old session
        try:
            out, err = d.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            d.kill()
            pytest.fail("result() neither returned nor raised 90 s after the peer was killed")
        assert "RAISED" in out and "could not be resumed" in out, (out, err[-2000:])
    finally:
        c.close()
