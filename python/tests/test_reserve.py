"""ch.reserve: bundles held on peers; calls and actors placed within them."""

from __future__ import annotations

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
    with ch.reserve([{"cpus": 1}, {"cpus": 1}], strategy="pack") as res:
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

res = ch.reserve([{"cpus": cores}]).ready(timeout=60)
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

res = ch.reserve([{"cpus": 1}] * 3, strategy="strict_spread").ready(timeout=60)
nodes = [res[i].node for i in range(3)]
ran = [where.options(within=res[i]).remote().result(timeout=60) for i in range(3)]
print("DISTINCT", len(set(nodes)), ran == nodes)
try:
    ch.reserve([{"cpus": 1}] * 4, strategy="strict_spread")
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
        ch.reserve([{"cpus": 1}], strategy="tight")


def test_competing_reservations_take_turns_instead_of_each_holding_part():
    out = on_a_fresh_peer(
        """
ch.Reservation.HOLD_S = 2.0
a = ch.reserve([{"cpus": 1}] * cores)
b = ch.reserve([{"cpus": 1}] * cores)  # together they want twice what the peer has
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
