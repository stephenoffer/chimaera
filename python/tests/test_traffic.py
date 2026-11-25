"""Weighted flow control between peers: while a peer sends a storm of
checkpoint copies at its paced rate, a call's read from it gets its weight's
share of that rate, and Raft round trips stay within their budget.

With CHIMAERA_TC_LINK=1 (as root on Linux), loopback first becomes a link
just above the paced rate with a deep queue (tc tbf), so the budgets hold on
a real bottleneck: unpaced, the storm would fill that queue and hold
heartbeats in it. In the dev container (`make linux-image` first):

    docker run --rm --privileged --cgroupns=private -e CHIMAERA_TC_LINK=1 \
        -v "$PWD":/src -v chimaera-target:/target -v chimaera-cargo:/usr/local/cargo/registry \
        chimaera-dev:latest deploy/dev/linux-test.sh -k test_traffic
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from conftest import BIN
from test_cluster import Cluster, lines

RATE = 20_000_000  # bytes per second each peer paces its sends at
FOREGROUND, CHECKPOINT = 8, 1  # weights (crates/chimaera-peer/src/traffic.rs)
READ = 48 << 20
STORM = 48  # objects of 8 MiB, each copied to the other peer in the background
HEARTBEAT_BUDGET_US = 100_000
HEARTBEAT_WINDOW = 10  # seconds of heartbeats a status reports (traffic.rs RTT_WINDOW)

SENT = """
import json, os, subprocess, time
import chimaera as ch

def status(home):
    out = subprocess.run([{bin!r}, "--dir", home, "status", "--json"], capture_output=True, text=True).stdout
    return json.loads(out)

def sent(name):
    return {{t["name"]: t["bytes"] for t in status({home!r})["traffic"]}}[name]
"""


@pytest.fixture
def link():
    """A bottleneck on loopback when asked for (CHIMAERA_TC_LINK=1)."""
    if os.environ.get("CHIMAERA_TC_LINK") != "1":
        yield
        return
    if not sys.platform.startswith("linux") or os.geteuid() != 0:
        pytest.fail("CHIMAERA_TC_LINK=1 shapes loopback with tc: it needs root on Linux")
    bits = int(RATE / 0.9 * 8)  # the pacing leaves a tenth of the link free
    tbf = ["tc", "qdisc", "replace", "dev", "lo", "root", "tbf", "rate", f"{bits}bit", "burst", "256kb"]
    subprocess.run([*tbf, "latency", "400ms"], check=True)
    try:
        yield
    finally:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], check=False)


def _status(c: Cluster, i: int) -> dict:
    out = subprocess.run([BIN, "--dir", c.dir(i), "status", "--json"], capture_output=True, text=True, timeout=30)
    return json.loads(out.stdout)


def test_a_checkpoint_storm_leaves_foreground_reads_and_raft_heartbeats_their_budgets(link):
    rate = {"CHIMAERA_MESH_RATE": str(RATE)}
    c = Cluster(2, labels={0: "slot=s0", 1: "slot=s1"}, env={0: rate, 1: rate})
    try:
        out = c.run(
            0,
            SENT.format(bin=BIN, home=c.dir(0))
            + f"""
@ch.remote
def ready():
    return True

@ch.remote
def read(data):  # fetched to peer 1 before the call starts
    return len(data)

on1 = ch.dist.Placement(require={{"slot": "s1"}})
ready.options(placement=on1).remote().result(timeout=120)  # a warm executor on peer 1
value = ch.put(os.urandom({READ}), durability="ephemeral")  # on peer 0 alone
start = time.monotonic()
storm = [ch.put(os.urandom(8 << 20)) for _ in range({STORM})]
while sent("checkpoint") < 16 << 20:  # the copies are under way
    time.sleep(0.02)
under_way = time.monotonic()
t0 = time.monotonic()
n = read.options(placement=on1).remote(value).result(timeout=120)
took = time.monotonic() - t0
during = sent("checkpoint")
print("READ", n, took)
print("DURING", during)
# Heartbeat round trips as the leaders report them, once every one they
# report (the last {HEARTBEAT_WINDOW} s) went while the storm did.
while sent("checkpoint") < {STORM} * (8 << 20) and time.monotonic() - start < 180:
    if time.monotonic() - under_way >= {HEARTBEAT_WINDOW}:
        for home in {[c.dir(0), c.dir(1)]!r}:
            s = status(home)
            if any(g["leading"] for g in s["groups"]) and s["heartbeat_p99_us"]:
                print("HEARTBEAT", s["heartbeat_p50_us"], s["heartbeat_p99_us"])
    time.sleep(0.5)
print("STORM", time.monotonic() - start, sent("checkpoint"))
""",
            timeout=300,
        )
        n, took = lines(out, "READ")[0].split()
        assert int(n) == READ
        # Its weight's share of the rate against the storm (8 of 9, about
        # 2.8 s), with a fifth to spare and half a second for the call.
        # Weighed equally it would get half (5 s); in arrival order about a
        # ninth (8 streams against 64).
        share = FOREGROUND / (FOREGROUND + CHECKPOINT)
        assert float(took) <= 1.2 * READ / (RATE * share) + 0.5, out.stdout
        assert int(lines(out, "DURING")[0]) < STORM * (8 << 20), "the read ended before the storm did"
        storm_s, storm_bytes = lines(out, "STORM")[0].split()
        assert int(storm_bytes) >= STORM * (8 << 20), "a checkpoint copy was given up (see the peers' logs)"
        assert float(storm_s) >= 0.9 * STORM * (8 << 20) / RATE, "the storm went at the paced rate"
        # Group leaders send the heartbeats and time them: every report made
        # during the storm is within the budget.
        heartbeats = [tuple(map(float, h.split())) for h in lines(out, "HEARTBEAT")]
        assert len(heartbeats) >= 3, out.stdout
        assert all(p99 <= HEARTBEAT_BUDGET_US for _, p99 in heartbeats), heartbeats
        s0 = _status(c, 0)
        assert s0["mesh_rate"] == RATE
        classes = {t["name"]: t for t in s0["traffic"]}
        assert classes["foreground"]["bytes"] >= READ and classes["checkpoint"]["waited_s"] > 0, classes
        print(out.stdout, max(heartbeats, key=lambda h: h[1]))
    finally:
        c.close()


def test_a_slow_paced_checkpoint_copy_is_not_given_up():
    # At 2 MB/s a 12 MiB copy takes over 6 s: longer than a copy is allowed
    # unpaced (5 s, and 25 MB/s), but no stall when the rate is this low.
    size, rate = 12 << 20, {"CHIMAERA_MESH_RATE": "2MB"}
    c = Cluster(2, env={0: rate, 1: rate})
    try:
        out = c.run(
            0,
            SENT.format(bin=BIN, home=c.dir(0))
            + f"""
value = ch.put(os.urandom({size}))  # copied to the other peer in the background
start = time.monotonic()
while sent("checkpoint") < {size} and time.monotonic() - start < 60:
    time.sleep(0.2)
print("COPIED", sent("checkpoint"), time.monotonic() - start)
time.sleep(1)  # the copy's answer
""",
            timeout=120,
        )
        copied, took = lines(out, "COPIED")[0].split()
        assert int(copied) >= size and float(took) > 5.5, out.stdout
        assert "no replica of" not in c.log(0), c.log(0)[-3000:]
    finally:
        c.close()


def test_many_files_retained_at_once_are_copied_while_a_read_takes_most_of_the_rate():
    # 96 files of 100 KB retained in one request, as a retained put of many
    # arrays makes (each array a file of its own, which on this Mac only the
    # Linux arena gives a single put). They are copied at the checkpoint
    # class's ninth of 4 MB/s while a call reads: about 22 s for all of them.
    # All at once, every copy's first piece (80 KB) would go before any
    # copy's last, so each would take over 17 s and outlast its time limit
    # (11 s); REPLICAS_AT_ONCE at a time, each takes about 2 s.
    files, size, read, rate = 96, 100_000, 96 << 20, {"CHIMAERA_MESH_RATE": "4MB"}
    c = Cluster(2, labels={0: "slot=s0", 1: "slot=s1"}, env={0: rate, 1: rate})
    try:
        out = c.run(
            0,
            SENT.format(bin=BIN, home=c.dir(0))
            + f"""
from chimaera import _runtime

@ch.remote
def read(data):
    return len(data)

on1 = ch.dist.Placement(require={{"slot": "s1"}})
value = ch.put(os.urandom({read}), durability="ephemeral")
reading = read.options(placement=on1).remote(value)
# Files on this peer alone until retained below.
kept = [ch.put(os.urandom({size}), durability="ephemeral") for _ in range({files})]
ids = [i for k in kept for i in k._put[4]]
while sent("foreground") < 4 << 20:  # the read is under way
    time.sleep(0.05)
rt = _runtime.Runtime.get()
t0 = time.monotonic()
rt.client.retain(rt.run, ids)  # what ch.put(durability="retained") asks the peer
print("RETAINED", len(ids), time.monotonic() - t0, sent("foreground") < {read})
print("READ", reading.result(timeout=120))
""",
            timeout=240,
        )
        stored, took, during = lines(out, "RETAINED")[0].split()
        assert int(stored) == files, out.stdout
        assert during == "True", f"the read ended before the copies did ({took} s)"
        assert float(took) >= files * size / 4e6, "the copies were paced"
        assert int(lines(out, "READ")[0]) == read
    finally:
        c.close()
