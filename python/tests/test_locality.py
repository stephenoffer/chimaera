"""Across peers: a large input reaches each peer once however many calls need
it, a call goes where its large input already is, and what one peer learned
about a function is used by the others."""

from __future__ import annotations

import json
import re
import subprocess
import time

import pytest

from conftest import BIN
from test_cluster import WHERE, Cluster, lines


@pytest.fixture(scope="module")
def zoned():
    c = Cluster(3, labels={0: "zone=a", 1: "zone=b", 2: "zone=c"})
    yield c
    c.close()


def _fetches(c: Cluster, i: int) -> int:
    s = subprocess.run([BIN, "--dir", c.dir(i), "status", "--json"], capture_output=True, text=True, timeout=30)
    links = json.loads(s.stdout)["links"]
    return sum(int(m.group(1)) for m in (re.search(r": (\d+) transfers", ln) for ln in links) if m)


def test_a_large_input_needed_by_many_calls_reaches_each_peer_once(zoned):
    before = {i: _fetches(zoned, i) for i in range(3)}
    out = zoned.run(
        0,
        WHERE
        + """
import numpy as np
zone = {"p0": "a", "p1": "b", "p2": "c"}

@ch.remote
def total(a):
    time.sleep(0.2)
    return peer_name(), float(a.sum())

a = np.ones(4 << 20)  # 32 MiB, written once on peer 0
calls = [
    total.options(placement=ch.Placement(require={"zone": z})).remote(a)
    for _ in range(12)
    for z in ("b", "c")
]
print("RAN", sorted({p for p, _ in ch.gather(*calls, timeout=180)}))
""",
    )
    assert lines(out, "RAN") == ["['p1', 'p2']"]
    for i in (1, 2):
        assert _fetches(zoned, i) - before[i] == 1, f"peer {i} fetched the shared input more than once"


def test_a_call_runs_where_its_large_input_already_is(zoned):
    out = zoned.run(
        1,
        WHERE
        + """
@ch.remote
def make():
    return b"x" * (64 << 20)

@ch.remote
def size(b):
    return peer_name(), len(b)

big = make.options(placement=ch.Placement(require={"zone": "a"})).remote()
# Wait without reading: big.result() would copy the value to this driver's
# peer (p1), and running there would then be just as local.
next(ch.as_completed([big], timeout=60))
ran = [size.remote(big).result(timeout=60)[0] for _ in range(5)]
print("RAN", " ".join(ran))
""",
    )
    ran = lines(out, "RAN")[0].split()
    assert ran.count("p0") >= 4, f"calls on a 64 MiB input should run where it is (p0): {ran}"


def test_what_one_peer_learned_is_used_by_another(zoned):
    learn = """
import numpy as np
import chimaera as ch

@ch.remote
def allocate(i):
    a = np.ones(200 * 2**20 // 8)
    return float(a[-1])
"""
    first = zoned.run(
        0,
        learn
        + """
for i in range(4):
    allocate.options(placement=ch.Placement(require={"zone": "a"})).remote(i).result(timeout=60)
""",
    )
    assert first.returncode == 0, first.stderr
    time.sleep(3)  # a few liveness pings carry the profile to the other peers
    second = zoned.run(
        2,
        learn
        + """
f = allocate.options(placement=ch.Placement(require={"zone": "c"})).remote(0)
f.result(timeout=60)
print("RESERVED", f.explain()["attempts"][-1]["reservation"])
""",
    )
    reservation = lines(second, "RESERVED")[0]
    assert "learned" in reservation, f"peer 2 never ran it, so this came from peer 0: {reservation}"
