"""Across peers: a large input reaches each peer once however many calls need
it, a call goes where its large input already is, and what one peer learned
about a function is used by the others."""

from __future__ import annotations

import json
import os
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
    total.options(placement=ch.dist.Placement(require={"zone": z})).remote(a)
    for _ in range(12)
    for z in ("b", "c")
]
print("RAN", sorted({p for p, _ in ch.get(calls, timeout=180)}))
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

big = make.options(placement=ch.dist.Placement(require={"zone": "a"})).remote()
# Wait without reading: big.result() would copy the value to this driver's
# peer (p1), and running there would then be just as local.
next(ch.as_completed([big], timeout=60))
ran = [size.remote(big).result(timeout=60)[0] for _ in range(5)]
print("RAN", " ".join(ran))
""",
    )
    ran = lines(out, "RAN")[0].split()
    assert ran.count("p0") >= 4, f"calls on a 64 MiB input should run where it is (p0): {ran}"


def test_quick_readers_of_a_put_value_stay_where_it_is(zoned):
    # A put value is stored on the driver's peer, which names itself as its
    # holder when the calls are submitted; without that, the leaders placing
    # the calls saw every peer missing it and spread them by load, each peer
    # fetching the whole value for millisecond reads.
    out = zoned.run(
        0,
        WHERE
        + """
import numpy as np

@ch.remote
def peek(a):
    return peer_name()

learn = ch.put(np.ones(2 * 2**20 // 8))
ch.get([peek.remote(learn) for _ in range(8)], timeout=60)  # its duration is learned
time.sleep(2)  # and reaches every peer
# A fresh value only p0 holds (once other peers have fetched one, running
# there costs nothing and spreading by load is right), large enough that
# moving it outweighs queueing even at localhost speed under load.
v = ch.put(np.ones(256 * 2**20 // 8))
ran = ch.get([peek.remote(v) for _ in range(24)], timeout=120)
print("ON_P0", ran.count("p0"))
""",
    )
    # Placed by load alone, 4 of the 24 ran on p0.
    assert int(lines(out, "ON_P0")[0]) >= 20, out.stdout


def test_bytes_sent_again_after_their_release_reach_a_peer_that_had_them(zoned):
    # A blob stored by content gets the same id when the same bytes are sent
    # again. The peer that fetched it, and was then told it was released, must
    # fetch it again rather than refuse the call as reading a released value.
    out = zoned.run(
        0,
        WHERE
        + """
import gc
import numpy as np

@ch.remote
def first(a):
    return peer_name(), float(a[0])

on_b = first.options(placement=ch.dist.Placement(require={"zone": "b"}))
a = np.full(1 << 19, 7.0)
print("FIRST", on_b.remote(a).result(timeout=60))
del a
gc.collect()
time.sleep(1)  # the release reaches p1, which forgets its copy
a = np.full(1 << 19, 7.0)  # the same bytes, so the same blob id
print("AGAIN", on_b.remote(a).result(timeout=60))
""",
    )
    assert lines(out, "FIRST") == ["('p1', 7.0)"], out.stderr[-2000:]
    assert lines(out, "AGAIN") == ["('p1', 7.0)"], out.stderr[-2000:]


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
    allocate.options(placement=ch.dist.Placement(require={"zone": "a"})).remote(i).result(timeout=60)
""",
    )
    assert first.returncode == 0, first.stderr
    time.sleep(3)  # a few liveness pings carry the profile to the other peers
    second = zoned.run(
        2,
        learn
        + """
f = allocate.options(placement=ch.dist.Placement(require={"zone": "c"})).remote(0)
f.result(timeout=60)
print("RESERVED", f.explain()["attempts"][-1]["reservation"])
""",
    )
    reservation = lines(second, "RESERVED")[0]
    assert "learned" in reservation, f"peer 2 never ran it, so this came from peer 0: {reservation}"


def test_readers_spread_to_other_peers_once_the_holder_is_busy(zoned):
    # Following an input is right until its holder's queue costs more than
    # moving the input once to another peer. The same 36 readers of 0.2 s,
    # kept on the holder, are the yardstick (both runs see the same load).
    out = zoned.run(
        0,
        WHERE
        + """
import numpy as np

@ch.remote
def read(a):
    time.sleep(0.2)
    return peer_name()

v = ch.put(np.ones(64 * 2**20 // 8))
read.remote(v).result(timeout=60)  # its duration is learned
held = read.options(placement=ch.dist.Placement(require={"zone": "a"}))
t = time.perf_counter()
ch.get([held.remote(v) for _ in range(36)], timeout=120)
print("PINNED", time.perf_counter() - t)
t = time.perf_counter()
ran = ch.get([read.remote(v) for _ in range(36)], timeout=120)
print("FREE", time.perf_counter() - t)
print("PEERS", len(set(ran)))
""",
    )
    assert int(lines(out, "PEERS")[0]) >= 2, out.stdout
    pinned, free = float(lines(out, "PINNED")[0]), float(lines(out, "FREE")[0])
    assert free < 0.8 * pinned, f"spreading should beat staying on the holder: {free:.2f}s vs {pinned:.2f}s"


def test_a_released_put_value_is_deleted_on_the_peers_that_fetched_it(zoned):
    p1_objects = os.path.join(zoned.dir(1), "objects")
    out = zoned.run(
        0,
        WHERE
        + f"""
import gc, os
import numpy as np

@ch.remote
def total(a):
    return float(a[:8].sum())

x = ch.put(np.ones(4 << 20))  # 32 MiB, stored on p0
names = [os.path.basename(p) for p in x._owned]
run = ch.runs.current_run().id
held_on_p1 = lambda: [n for n in names if os.path.exists(os.path.join({p1_objects!r}, run, n))]
fs = [total.options(placement=ch.dist.Placement(require={{"zone": "b"}})).remote(x) for _ in range(4)]
ch.get(fs, timeout=60)
print("BEFORE", len(held_on_p1()))
del x, fs
gc.collect()
deadline = time.monotonic() + 10
while held_on_p1() and time.monotonic() < deadline:
    time.sleep(0.1)
print("AFTER", len(held_on_p1()))
""",
    )
    assert int(lines(out, "BEFORE")[0]) >= 1, f"p1 fetched no copy to release: {out.stdout}"
    assert int(lines(out, "AFTER")[0]) == 0, "p1 kept its copy after the driver released the value"
