"""Calls acknowledged from memory (CHIMAERA_RAFT_SYNC=durable) and lost when
the machines holding a majority of their group's voters lose power together:
every future a driver waits on still ends, in about a minute and a half.

CHIMAERA_TEST_LOSE_UNSYNCED=1 (debug builds) keeps what a peer has not synced
in memory, so a killed peer loses it as a machine without power would."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time

import pytest

from conftest import BIN
from test_cluster import Cluster, lines

LOSE = {
    "CHIMAERA_RAFT_SYNC": "durable",
    "CHIMAERA_TEST_LOSE_UNSYNCED": "1",
    "CHIMAERA_LOG": "info,chimaera_peer::core::sweep=debug",
}

DRIVER = """
import os, time
import chimaera as ch

GO, HOME = {go!r}, {home!r}

@ch.remote
def produce():
    time.sleep(4)
    return 41

@ch.remote
def consume(x):
    return x + 1

@ch.remote
def wait_long(i):
    time.sleep(120)
    return i

here = produce.options(placement=ch.dist.Placement(require={{"node": HOME}}))
p = here.remote()
c = consume.remote(p)
time.sleep(1)
print("SYNC", flush=True)  # the test makes a synced commit: p's and c's records reach the disks
while not os.path.exists(GO + ".synced"):
    time.sleep(0.05)
assert ch.get(p, timeout=60) == 41  # its result's publication is acknowledged from memory
lost = [wait_long.remote(i) for i in range(4)]
time.sleep(1)  # acknowledged: committed in memory only
print("CUT", flush=True)
while not os.path.exists(GO):
    time.sleep(0.05)
start = time.monotonic()
kinds = []
for f in lost:
    try:
        ch.get(f, timeout=400)
        kinds.append("value")
    except ch.errors.DataLost:
        kinds.append("DataLost")
    print("AT", round(time.monotonic() - start, 1), kinds[-1], flush=True)
print("LOST", " ".join(kinds), flush=True)
print("CONSUMED", ch.get(c, timeout=400), flush=True)
print("WITHIN", time.monotonic() - start, flush=True)
"""

DETACH = """
import chimaera as ch
print("RUN", ch.init(detach=True))
"""


def groups(c: Cluster, i: int) -> dict[int, str]:
    """The groups peer i hosts, each with its leader's address ("" if none is known)."""
    out = subprocess.run([BIN, "--dir", c.dir(i), "status", "--json"], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return {g.get("group", 0): g.get("leader", "") for g in json.loads(out.stdout).get("groups", [])}


def hosts(c: Cluster, i: int) -> set[int]:
    return set(groups(c, i))


def wait_for(what, check, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, what
        time.sleep(0.5)


def reboot_marker(c: Cluster, i: int) -> None:
    marker = os.path.join(c.dir(i), "raft-boot")
    boot, *rest = open(marker).read().splitlines()
    assert rest == ["unsynced", "running"], rest
    with open(marker, "w") as f:  # as if the machine had rebooted since
        f.write("\n".join(["another-boot", *rest]) + "\n")


@pytest.mark.slow
@pytest.mark.timeout(900)
def test_calls_lost_with_a_majority_of_their_voters_end_instead_of_hanging():
    # One data group, three voters among five peers. Two of its voters and a
    # third peer (a majority of the directory's five) lose power together; the
    # group's third voter is paused meanwhile, so the two elect a leader that
    # lacks what they had not synced.
    c = Cluster(5, data_groups=1, env={i: dict(LOSE) for i in range(5)})
    try:
        wait_for("each peer reports its groups", lambda: all(hosts(c, i) for i in range(5)))
        voters = [i for i in range(5) if 1 in hosts(c, i)]
        assert len(voters) == 3, voters
        others = [i for i in range(5) if i not in voters]
        home, gone_too = others
        survivor, cut = voters[0], voters[1:] + [gone_too]
        peer_id = open(os.path.join(c.dir(home), "peer-id")).read().strip()
        go = os.path.join(c.root, "go")
        d = c.driver(home, DRIVER.format(go=go, home=peer_id))
        assert d.stdout.readline().strip() == "SYNC", d.stderr.read()[-3000:]
        lines(c.run(home, DETACH), "RUN")  # a synced commit in the group
        open(go + ".synced", "w").close()
        assert d.stdout.readline().strip() == "CUT", d.stderr.read()[-3000:]

        c.signal(survivor, signal.SIGSTOP)
        for i in cut:
            c.signal(i, signal.SIGKILL)
            reboot_marker(c, i)
        for i in cut:
            c.start(i)
        c.wait_ready(cut, timeout=120)
        survivor_addr = c.addrs[survivor]
        wait_for(
            "the two voters elect a leader",
            lambda: all(groups(c, i).get(1, "") not in ("", survivor_addr) for i in voters[1:]),
        )
        c.signal(survivor, signal.SIGCONT)
        open(go, "w").close()

        try:
            out, err = d.communicate(timeout=600)
        except subprocess.TimeoutExpired:
            d.kill()
            out, err = d.communicate()
        sweep = [ln for ln in c.log(home).splitlines() if "sweep" in ln or "lost" in ln or "again" in ln]
        assert d.returncode == 0, f"{out}\n{err[-3000:]}\n" + "\n".join(sweep[-40:])
        print(out)  # the driver's timeline, for -rP
        got = {ln.split(" ", 1)[0]: ln.split(" ", 1)[1] for ln in out.splitlines() if " " in ln}
        assert got["CONSUMED"] == "42", "the result whose publication was lost was made again"
        assert got["LOST"] == "DataLost DataLost DataLost DataLost", got
        assert float(got["WITHIN"]) < 150, got

        # The paused voter held entries its group no longer has: it stops,
        # and comes back as a new member; the group has three voters again.
        assert c.procs[survivor].wait(timeout=120) == 3, c.log(survivor)[-3000:]
        c.start(survivor)
        c.wait_ready([survivor], timeout=120)
        wait_for("the survivor rejoins as a new member", lambda: "joining again as a new member" in c.log(survivor))
        wait_for("the group is back to three voters", lambda: sum(1 in hosts(c, i) for i in range(5)) == 3, 180)
    finally:
        c.close()
