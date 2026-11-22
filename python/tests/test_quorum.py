"""A cluster that lost the majority of its peers says so: ch.errors.QuorumLost, naming the group."""

from __future__ import annotations

import os
import signal

from test_cluster import Cluster

DRIVER = """
import os, time, chimaera as ch

@ch.remote
def one():
    return 1

assert one.remote().result(timeout=60) == 1  # the run exists while the groups are healthy
print("READY", flush=True)
while not os.path.exists(GO):
    time.sleep(0.1)
start = time.time()
try:
    one.remote().result(timeout=300)
    print("RESULT ok", flush=True)
except ch.errors.QuorumLost as e:
    print("RESULT QuorumLost", round(time.time() - start), str(e), flush=True)
except Exception as e:
    print("RESULT", type(e).__name__, str(e), flush=True)
"""

INIT = """
import chimaera as ch

try:
    ch.init()
    print("INIT ok")
except ch.errors.QuorumLost as e:
    print("INIT QuorumLost", e)
except Exception as e:
    print("INIT", type(e).__name__, e)
"""

AGAIN = """
import chimaera as ch

@ch.remote
def one():
    return 1

print("AGAIN", one.remote().result(timeout=120))
"""


def test_a_call_to_a_group_without_a_majority_raises_quorum_lost():
    c = Cluster(3)
    try:
        go = os.path.join(c.root, "go")
        driver = c.driver(0, DRIVER.replace("GO", repr(go)))
        assert driver.stdout.readline().strip() == "READY", driver.stderr.read()[-2000:] if driver.poll() else ""
        for i in (1, 2):
            c.signal(i, signal.SIGKILL)
        open(go, "w").close()
        out, err = driver.communicate(timeout=240)
        line = next((ln for ln in out.splitlines() if ln.startswith("RESULT")), out + err[-2000:])
        kind, seconds, message = (line.split(" ", 3) + ["", ""])[1:4]
        assert kind == "QuorumLost", line
        assert message.startswith("group ") or "quorum lost: group" in message, message
        assert int(seconds) <= 90, line  # within the submit patience (60 s), not a hang
        # A new driver cannot even start its run there, and is told why.
        init = c.run(0, INIT, timeout=180)
        assert "INIT QuorumLost quorum lost: group" in init.stdout, (init.stdout, init.stderr[-2000:])
        # The two peers come back: the groups have a majority again and calls run.
        for i in (1, 2):
            c.start(i)
        c.wait_ready([1, 2])
        again = c.run(0, AGAIN, timeout=180)
        assert again.returncode == 0 and "AGAIN 1" in again.stdout, again.stderr[-2000:]
    finally:
        c.close()


def test_quorum_lost_is_an_infrastructure_failure():
    import chimaera as ch

    assert issubclass(ch.errors.QuorumLost, ch.InfrastructureFailure)
