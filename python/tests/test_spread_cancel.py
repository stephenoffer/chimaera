"""Cancelling a call spread to another vshard reaches the actor calls it made,
which stay in the run's own vshard (shard CancelChildren, through the outbox)."""

from __future__ import annotations

import time

from test_cluster import Cluster, status

DRIVER = """
import os, tempfile, time
import chimaera as ch
from chimaera._native import vshard_of

@ch.remote(concurrency=4)
class Waiter:
    def wait(self, seconds):
        end = time.time() + seconds
        while time.time() < end and not ch.context().cancelled():
            time.sleep(0.05)

@ch.remote(cpus=1)
def boss(marker, burn):
    call = Waiter.remote().wait.remote(120)  # an actor call: it stays in the run's own vshard
    with open(marker, "w") as f:
        f.write(call.id)
    end = time.process_time() + 60
    while time.process_time() < end:
        if not burn:
            time.sleep(0.05)
    return call.result()

run = ch.runs.current_run()
home = vshard_of(bytes.fromhex(run.id))
tmp = tempfile.mkdtemp()

def spread_boss(name, burn, **options):
    # A call outside the run's own vshard: the case the outbox covers.
    for i in range(32):
        marker = os.path.join(tmp, f"{name}{i}")
        f = boss.options(**options).remote(marker, burn)
        if vshard_of(f.key[16:]) != home:
            return f, marker
        f.cancel()
    raise SystemExit("no call was spread: " + str(home))

def child_error(marker):
    deadline = time.monotonic() + 30
    while not os.path.exists(marker) and time.monotonic() < deadline:
        time.sleep(0.05)
    child = open(marker).read()
    while time.monotonic() < deadline:
        found = [err for tid, _, err in run.outcomes() if child.endswith(tid)]
        if found:
            return type(found[0]).__name__
        time.sleep(0.1)
    return "still running"

f, marker = spread_boss("budget", True, budget={"cpu_seconds": 1})
try:
    f.result(timeout=120)
    print("BOSS finished")
except ch.errors.BudgetExceeded:
    print("BOSS BudgetExceeded")
print("CHILD", child_error(marker))

f, marker = spread_boss("cancel", False)
while not os.path.exists(marker):
    time.sleep(0.05)
f.cancel()
print("CANCELLED_CHILD", child_error(marker))
"""


def test_cancelling_a_spread_call_reaches_the_actor_calls_it_made(monkeypatch):
    # Leaders even out after a second, so the driver's peer leads only some
    # groups and the run spreads over the others (as in test_cluster).
    monkeypatch.setenv("CHIMAERA_REBALANCE_SECONDS", "1")
    monkeypatch.setenv("CHIMAERA_SPREAD_RATE", "0")  # every driver call spreads, however slowly it comes
    c = Cluster(3, data_groups=6)
    try:
        deadline = time.monotonic() + 60
        while not any(g["group"] > 0 and g["leader"] and not g["leading"] for g in status(c, 0)["groups"]):
            assert time.monotonic() < deadline, status(c, 0)["groups"]
            time.sleep(0.5)
        out = c.run(0, DRIVER, timeout=300)
        assert out.returncode == 0, out.stderr[-3000:]
        lines = dict(ln.split(" ", 1) for ln in out.stdout.splitlines() if " " in ln)
        assert lines.get("BOSS") == "BudgetExceeded", out.stdout
        assert lines.get("CHILD") == "BudgetExceeded", ("the budget stops the actor call too", out.stdout)
        assert lines.get("CANCELLED_CHILD") == "Cancelled", ("cancelling stops the actor call too", out.stdout)
    finally:
        c.close()
