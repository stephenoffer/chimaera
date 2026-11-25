"""ch.dist.reserve: bundles held on peers; calls and actors placed within them."""

from __future__ import annotations

import os
import re
import tempfile

import pytest

import chimaera as ch
from conftest import run_script
from test_cluster import Cluster


@ch.remote
def where(_=None):
    return ch.context().node


@ch.remote
class Counter:
    def __init__(self, start):
        self.n = start

    def add(self, k):
        self.n += k
        return self.n


def test_calls_and_actors_run_within_their_bundle_until_release():
    with ch.dist.reserve([{"cpus": 1}, {"cpus": 1}], strategy="pack") as res:
        assert len(res) == 2 and res[0].node == res[1].node  # one peer, packed
        assert where.options(within=res[0]).remote().result(timeout=60) == res[0].node
        c = Counter.options(within=res[1]).remote(10)
        assert [c.add.remote(1).result(timeout=60) for _ in range(3)] == [11, 12, 13]
        assert where.options(within=res[1]).remote().result(timeout=60) == res[1].node
    with pytest.raises(RuntimeError, match="released"):
        where.options(within=res[0]).remote()  # refused at once, not lost later


FRESH_PEER = """
import time
import chimaera as ch

cores = int(ch._runtime.Runtime.get().info["usable_cores"])  # a fresh peer: every CPU is free
"""


def on_a_fresh_peer(code: str) -> str:
    """Run `code` against its own new local peer, so no other test's actors hold CPUs."""
    home = tempfile.mkdtemp(prefix="chr", dir="/tmp")
    out = run_script(FRESH_PEER + code, env={"CHIMAERA_HOME": home}, home=home, timeout=400)
    assert out.returncode == 0, out.stderr[-3000:]
    return out.stdout


def test_held_capacity_is_not_given_to_other_work():
    out = on_a_fresh_peer(
        """
@ch.remote(cpus=1)
def busy():
    return time.time()

res = ch.dist.reserve([{"cpus": cores}]).ready(timeout=60)
f = busy.remote()
time.sleep(1.0)
print("WAITED", not f.done())
released_at = time.time()
res.release()
print("RAN_AFTER", f.result(timeout=60) >= released_at)
"""
    )
    assert "WAITED True" in out and "RAN_AFTER True" in out, out


def test_strict_spread_puts_each_bundle_on_its_own_peer():
    cluster = Cluster(3)
    try:
        out = cluster.run(
            0,
            """
import chimaera as ch

@ch.remote
def where():
    return ch.context().node

res = ch.dist.reserve([{"cpus": 1}] * 3, strategy="strict_spread").ready(timeout=60)
nodes = [res[i].node for i in range(3)]
ran = [where.options(within=res[i]).remote().result(timeout=60) for i in range(3)]
print("DISTINCT", len(set(nodes)), ran == nodes)
try:
    ch.dist.reserve([{"cpus": 1}] * 4, strategy="strict_spread")
except ch.Infeasible as e:
    print("INFEASIBLE", "need as many peers" in str(e))
res.release()
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        assert "DISTINCT 3 True" in out.stdout and "INFEASIBLE True" in out.stdout, out.stdout
    finally:
        cluster.close()


def test_within_takes_a_bundle():
    with pytest.raises(ch.OptionsError, match="bundle of a reservation"):
        where.options(within=3)
    with pytest.raises(ValueError, match="strategy"):
        ch.dist.reserve([{"cpus": 1}], strategy="tight")


def test_competing_reservations_take_turns_instead_of_each_holding_part():
    out = on_a_fresh_peer(
        """
ch.dist.Reservation.HOLD_S = 2.0
a = ch.dist.reserve([{"cpus": 1}] * cores)
b = ch.dist.reserve([{"cpus": 1}] * cores)  # together they want twice what the peer has
first = None
deadline = time.time() + 180
while first is None and time.time() < deadline:
    for r in (a, b):
        try:
            first = r.ready(timeout=0.5)
            break
        except TimeoutError:
            pass
other = b if first is a else a
try:
    other.ready(timeout=1)
    print("BOTH_HELD")  # all of it or nothing: this must not happen
except TimeoutError:
    print("OTHER_WAITS")
first.release()
print("TURN", other.ready(timeout=180) is other)
other.release()
"""
    )
    assert "OTHER_WAITS" in out and "TURN True" in out and "BOTH_HELD" not in out, out


def test_a_refused_try_holds_nothing_and_a_later_try_holds_every_bundle(monkeypatch):
    from chimaera import _ledger

    real, refused = _ledger.Holds._ask, []

    def ask(self, peer, slots, renew=False):
        if slots and not renew and not refused:  # the first try's peer refuses (as a full one would)
            refused.append(peer)
            return False, "refused for the test", False
        return real(self, peer, slots, renew)

    monkeypatch.setattr(_ledger.Holds, "_ask", ask)
    res = ch.dist.reserve([{"cpus": 0.25}] * 2).ready(timeout=120)
    assert refused and len(res._nodes) == 2, "held on a later try instead of failing the reservation"
    owner = f"default/reserve/{res.id} "
    [line] = [h for h in _ledger.holds_on() if h.startswith(owner)]
    assert "2 of 2 slot(s) untaken" in line, line
    res.release()
    assert not [h for h in _ledger.holds_on() if h.startswith(owner)]


HOLD_THEN_DIE = """
import chimaera as ch
ch.dist.Reservation.HOLD_S = 10.0  # renewed every 3.3 s; ends 6.7 to 10 s after the driver
res = ch.dist.reserve([{"cpus": cores, "gpus": 8}]).ready(timeout=60)
print("HELD", flush=True)
time.sleep(600)  # killed before anything is launched within it
"""


def test_bundles_held_by_a_driver_that_died_before_launch_are_free_by_expiry():
    import shutil
    import signal
    import subprocess
    import sys

    from conftest import BIN, REPO, SCRIPTS

    home = tempfile.mkdtemp(prefix="chr", dir="/tmp")
    env = dict(os.environ, CHIMAERA_HOME=home, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    env["CHIMAERA_DEVICE_ROOT"] = os.path.join(REPO, "tests", "fixtures", "topology", "dgx-h100")  # 8 GPUs
    script = os.path.join(tempfile.mkdtemp(dir=SCRIPTS), "driver.py")
    with open(script, "w") as f:
        f.write(FRESH_PEER + HOLD_THEN_DIE)
    driver = subprocess.Popen([sys.executable, script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    try:
        assert driver.stdout.readline().decode().strip() == "HELD", driver.stderr.read().decode()[-3000:]
        driver.send_signal(signal.SIGKILL)  # gone between ready() and any launch
        driver.wait(timeout=30)
        out = run_script(
            FRESH_PEER
            + """
from chimaera._ledger import holds_on
res = ch.dist.reserve([{"cpus": cores, "gpus": 8}])
try:
    res.ready(timeout=0.5)
    print("FREED AT ONCE")  # its peer's ledger forgot it without the expiry: must not happen
except TimeoutError:
    pass
start = time.time()
res.ready(timeout=60)
print("HELD AFTER", round(time.time() - start), "s", len(holds_on()), "hold(s)")
res.release()
""",
            env={"CHIMAERA_HOME": home},
            home=home,
        )
        assert out.returncode == 0, out.stderr[-3000:]
        held = re.search(r"HELD AFTER (\d+) s 1 hold\(s\)", out.stdout)
        assert held and int(held.group(1)) <= 20 and "FREED AT ONCE" not in out.stdout, out.stdout
    finally:
        driver.kill()
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
        shutil.rmtree(home, ignore_errors=True)
