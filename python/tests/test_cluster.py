"""Multi-peer clusters on localhost: real peer processes, drivers in their own
processes, peers killed or paused mid-run."""

from __future__ import annotations

import collections
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, REPO, run_script


def free_ports(n: int) -> list[int]:
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


class Cluster:
    """`founders` peers formed from one seed list, plus `spares` that join later."""

    def __init__(
        self,
        founders: int,
        spares: int = 0,
        data_groups: int = 4,
        tls: bool | str = False,  # True: a certificate per peer; "ca": peers issue their own (--tls-ca); "env": from the CA in their environment (slurm-launch)
        clients: bool = False,
        labels: dict[int, str] | None = None,
        one_machine: bool = False,
    ) -> None:
        self.root = tempfile.mkdtemp(prefix="chc", dir="/tmp")
        self.labels = labels or {}  # peer index -> CHIMAERA_LABELS
        # The peers stand for separate machines unless `one_machine`: on one,
        # they take objects straight from each other's stores.
        self.one_machine = one_machine
        self.addrs = [f"127.0.0.1:{p}" for p in free_ports(founders + spares)]
        # --client-listen addresses, for drivers that connect over TCP.
        self.client_addrs = [f"127.0.0.1:{p}" for p in free_ports(len(self.addrs))] if clients else []
        self.seeds = ",".join(self.addrs[:founders])
        self.data_groups = data_groups
        self.tls: dict[int, str] = {}
        self.ca: str | None = None
        self.ca_env: dict[str, str] = {}
        if tls == "ca":
            self.ca = self.make_ca("ca")
            self.token = os.path.join(self.ca, "token")
        elif tls == "env":
            ca = self.make_ca("ca")
            self.token = os.path.join(ca, "token")
            with open(os.path.join(ca, "ca.pem")) as pem, open(os.path.join(ca, "ca.key")) as key:
                self.ca_env = {"CHIMAERA_TLS_CA_PEM": pem.read(), "CHIMAERA_TLS_CA_KEY": key.read()}
        elif tls:
            ca = self.make_ca("ca")
            self.token = os.path.join(ca, "token")
            for i in range(len(self.addrs)):
                self.tls[i] = self.issue(ca, i)
        else:
            self.token = self.write_token("token", subprocess.check_output([BIN, "cluster", "token"], text=True))
        self.procs: dict[int, subprocess.Popen] = {}
        for i in range(len(self.addrs)):
            self.start(i)
        self.wait_ready(range(len(self.addrs)))

    def make_ca(self, name: str) -> str:
        out = os.path.join(self.root, name)
        subprocess.run([BIN, "cluster", "init", "--out", out], check=True, capture_output=True)
        return out

    def issue(self, ca: str, i: int) -> str:
        out = os.path.join(self.root, f"tls-{os.path.basename(ca)}-{i}")
        subprocess.run([BIN, "cluster", "cert", "--ca", ca, "--name", f"p{i}", "--out", out], check=True, capture_output=True)
        return out

    def write_token(self, name: str, value: str) -> str:
        path = os.path.join(self.root, name)
        with open(path, "w") as f:
            f.write(value)
        return path

    def dir(self, i: int) -> str:
        return os.path.join(self.root, f"p{i}")

    def sock(self, i: int) -> str:
        os.makedirs(self.dir(i), exist_ok=True)
        return socket_path(self.dir(i), "peer.sock")

    def start(self, i: int, token: str | None = None) -> None:
        try:
            os.unlink(self.sock(i))  # a killed peer leaves its socket behind
        except FileNotFoundError:
            pass
        log = open(os.path.join(self.root, f"p{i}.log"), "ab")
        cmd = [BIN, "--dir", self.dir(i), "peer", "start", "--listen", self.addrs[i], "--seeds", self.seeds]
        cmd += ["--cluster", "test", "--data-groups", str(self.data_groups), "--cpus", "4"]
        if self.ca is not None:
            cmd += ["--tls-ca", self.ca]  # the token comes from the CA directory too
            if token:
                cmd += ["--token-file", token]
        else:
            cmd += ["--token-file", token or self.token]
        if i in self.tls:
            cmd += ["--tls-dir", self.tls[i]]
        if self.client_addrs:
            cmd += ["--client-listen", self.client_addrs[i]]
        machine = os.path.basename(self.root) + ("" if self.one_machine else f"-{i}")
        env = dict(os.environ, CHIMAERA_LABELS=self.labels.get(i, ""), CHIMAERA_MACHINE_ID=machine, **self.ca_env)
        self.procs[i] = subprocess.Popen(cmd, stdout=log, stderr=log, start_new_session=True, env=env)

    def wait_ready(self, peers, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        for i in peers:
            while not os.path.exists(self.sock(i)):
                if self.procs[i].poll() is not None:
                    raise RuntimeError(f"peer {i} exited: {self.log(i)[-2000:]}")
                if time.monotonic() > deadline:
                    raise TimeoutError(f"peer {i} not ready: {self.log(i)[-2000:]}")
                time.sleep(0.05)

    def log(self, i: int) -> str:
        with open(os.path.join(self.root, f"p{i}.log"), errors="replace") as f:
            return f.read()

    def signal(self, i: int, sig: int) -> None:
        self.procs[i].send_signal(sig)
        if sig == signal.SIGKILL:
            self.procs[i].wait(timeout=10)

    def run(self, i: int, code: str, timeout: float = 180, **kw) -> subprocess.CompletedProcess:
        return run_script(code, timeout=timeout, home=self.dir(i), **kw)

    def driver(self, i: int, code: str) -> subprocess.Popen:
        """Start a driver against peer `i` in the background."""
        path = os.path.join(self.root, f"driver-{time.monotonic_ns()}.py")
        with open(path, "w") as f:
            f.write(code)
        env = dict(os.environ, CHIMAERA_HOME=self.dir(i), CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
        return subprocess.Popen(
            [sys.executable, path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=self.root
        )

    def remote(self, i: int, code: str, *, token: bool = True, ca: str | None = None) -> subprocess.CompletedProcess:
        """Run a driver on "another machine": it reaches peer `i` over TCP only."""
        home = tempfile.mkdtemp(prefix="chr", dir="/tmp")  # its cache of fetched values
        env = {"CHIMAERA_ADDRESS": f"tcp://{self.client_addrs[i]}", "CHIMAERA_HOME": home}
        if token:
            env["CHIMAERA_TOKEN_FILE"] = self.token
        if ca:
            env["CHIMAERA_CA"] = ca
        try:
            return run_script(code, timeout=180, env=env)
        finally:
            shutil.rmtree(home, ignore_errors=True)

    def close(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10)
        # The peers' data (raft logs, snapshots, values) runs to gigabytes over a
        # suite; keep only their logs (p*.log) for looking into a failure.
        for name in os.listdir(self.root):
            if os.path.isdir(os.path.join(self.root, name)):
                shutil.rmtree(os.path.join(self.root, name), ignore_errors=True)


def lines(proc: subprocess.CompletedProcess, prefix: str) -> list[str]:
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    return [ln[len(prefix) :].strip() for ln in proc.stdout.splitlines() if ln.startswith(prefix)]


@pytest.fixture(scope="module")
def three():
    c = Cluster(3)
    yield c
    c.close()


WHERE = """
import collections, os, time
import chimaera as ch
from chimaera._runtime import Runtime

def peer_name():
    return os.path.basename(os.path.dirname(Runtime.get().object_dir))
"""


def test_work_spreads_over_peers_and_values_move_between_them(three):
    out = three.run(
        0,
        WHERE
        + """
@ch.remote
def where(i):
    time.sleep(0.05)
    return peer_name()

@ch.remote
def blob(n):
    return (peer_name(), b"x" * n)

@ch.remote
def total(*parts):
    return sum(len(b) for _, b in parts), sorted({p for p, _ in parts})

placed = collections.Counter(ch.get((where.remote(i) for i in range(90)), timeout=120))
print("PEERS", len(placed))
parts = [blob.remote(2 << 20) for _ in range(9)]
n, producers = total.remote(*parts).result(timeout=120)
print("TOTAL", n)
print("PRODUCERS", len(producers))
print("DRIVER", len(parts[-1].result(timeout=60)[1]))
""",
    )
    assert lines(out, "PEERS") == ["3"], "90 tasks should land on all three peers"
    assert lines(out, "TOTAL") == [str(9 * (2 << 20))]
    assert int(lines(out, "PRODUCERS")[0]) >= 2, "inputs came from more than one peer"
    assert lines(out, "DRIVER") == [str(2 << 20)]


def test_actors_live_on_any_peer_and_keep_call_order(three):
    out = three.run(
        0,
        WHERE
        + """
@ch.remote
class Log:
    def __init__(self):
        self.items = []
        self.home = peer_name()
    def append(self, i):
        self.items.append(i)
    def get(self):
        return self.home, self.items

homes = set()
for _ in range(6):
    a = Log.remote()
    for i in range(200):
        a.append.remote(i)
    home, items = a.get.remote().result(timeout=120)
    assert items == list(range(200)), items[:20]
    homes.add(home)
    a.close()
print("HOMES", len(homes))
"""
    )
    assert int(lines(out, "HOMES")[0]) >= 2, "six actors should not all land on one peer"


def test_a_burst_of_actors_spreads_over_the_peers(three):
    # Actors that ask for no CPUs change no peer's load when placed: a burst
    # of them must still spread, not land on whichever peer looked emptiest.
    out = three.run(
        0,
        WHERE
        + """
@ch.remote
class Where:
    def home(self):
        return peer_name()

actors = [Where.remote() for _ in range(90)]
homes = collections.Counter(ch.get((a.home.remote() for a in actors), timeout=180))
print("HOMES", len(homes), max(homes.values()))
"""
    )
    peers, most = map(int, lines(out, "HOMES")[0].split())
    assert peers == 3 and most <= 45, f"90 actors over 3 peers: {peers} peers used, {most} on the busiest"


def test_a_peer_killed_mid_run_loses_no_results():
    c = Cluster(3)
    try:
        driver = c.driver(
            0,
            WHERE
            + """
@ch.remote
def work(i):
    time.sleep(0.2)
    return i * i

futs = [work.remote(i) for i in range(240)]
values = ch.get(futs, timeout=240)
print("OK", values == [i * i for i in range(240)])
""",
        )
        time.sleep(1.5)
        c.signal(2, signal.SIGKILL)  # peer 2: not the driver's
        out, err = driver.communicate(timeout=240)
        assert driver.returncode == 0, out[-2000:] + err[-3000:]
        assert "OK True" in out
    finally:
        c.close()


def test_detached_run_survives_its_ingress_and_attaches_through_another_peer():
    c = Cluster(3)
    try:
        out = c.run(
            0,
            """
import time
import chimaera as ch

@ch.remote
def work(i):
    time.sleep(0.3)
    return i + 1000

run = ch.init(detach=True)
futs = [work.remote(i) for i in range(300)]
# Report the run only once the cluster holds all 300 calls: a submission
# the ingress never acknowledged dies with it.
while sum(ch.runs.current_run().status().values()) < 300:
    time.sleep(0.05)
print("RUN", run)
""",
        )
        run_id = lines(out, "RUN")[0]
        c.signal(0, signal.SIGKILL)  # the run's ingress, and leader of its group
        t0 = time.monotonic()
        out = c.run(
            1,
            f"""
import chimaera as ch
run = ch.runs.attach({run_id!r})
run.wait(timeout=180)
values = [v for _, v, e in run.outcomes() if e is None]
errors = [e for _, v, e in run.outcomes() if e is not None]
print("COUNT", len(values), len(set(values)), len(errors))
print("EXACT", sorted(values) == [i + 1000 for i in range(300)])
""",
            timeout=240,
        )
        assert lines(out, "COUNT") == ["300 300 0"], out.stdout + out.stderr[-2000:]
        assert lines(out, "EXACT") == ["True"]
        assert time.monotonic() - t0 < 120
    finally:
        c.close()


def test_a_paused_peer_rejoins_without_duplicating_results():
    c = Cluster(3)
    try:
        driver = c.driver(
            0,
            WHERE
            + """
@ch.remote
def work(i):
    time.sleep(0.1)
    return i

futs = [work.remote(i) for i in range(400)]
values = ch.get(futs, timeout=300)
print("EXACT", values == list(range(400)))
""",
        )
        time.sleep(1.0)
        c.signal(1, signal.SIGSTOP)  # past the 6 s suspicion floor: declared dead and retired
        time.sleep(10)
        c.signal(1, signal.SIGCONT)
        out, err = driver.communicate(timeout=300)
        assert driver.returncode == 0, out[-2000:] + err[-3000:]
        assert "EXACT True" in out
        assert "retiring it" in c.log(0) + c.log(2), "the pause should have retired peer 1's incarnation"
    finally:
        c.close()


def test_a_briefly_stalled_peer_is_not_retired():
    # Overloaded is not dead: a peer that stops answering for a few seconds
    # (paging, a saturated disk) keeps its incarnation, and its clients with it.
    c = Cluster(3)
    try:
        driver = c.driver(
            1,
            WHERE
            + """
@ch.remote
def work(i):
    time.sleep(0.1)
    return i

futs = [work.remote(i) for i in range(200)]
values = ch.get(futs, timeout=300)
print("EXACT", values == list(range(200)))
""",
        )
        time.sleep(1.0)
        c.signal(1, signal.SIGSTOP)  # the driver's own peer
        time.sleep(3)
        c.signal(1, signal.SIGCONT)
        out, err = driver.communicate(timeout=300)
        assert driver.returncode == 0, out[-2000:] + err[-3000:]
        assert "EXACT True" in out
        assert "retiring it" not in c.log(0) + c.log(2), "a 3 s stall must not retire the peer"
    finally:
        c.close()


@pytest.mark.slow
def test_dead_voters_are_replaced_so_a_second_loss_is_survivable():
    c = Cluster(3, spares=1)
    try:
        c.signal(1, signal.SIGKILL)
        # Dead after 3 s, replaced after 10 more; then the spare votes.
        deadline = time.monotonic() + 90
        while "replacing a dead voter" not in "".join(c.log(i) for i in (0, 2, 3)):
            assert time.monotonic() < deadline, "no replacement started"
            time.sleep(1)
        time.sleep(8)  # let every group finish its membership change
        c.signal(2, signal.SIGKILL)  # a second founder: quorum needs the spare now
        out = c.run(
            0,
            """
import chimaera as ch

@ch.remote
def add(a, b):
    return a + b

print("SUM", sum(ch.get((add.remote(i, i) for i in range(50)), timeout=120)))
""",
            timeout=180,
        )
        assert lines(out, "SUM") == [str(sum(2 * i for i in range(50)))]
    finally:
        c.close()


def test_a_drained_peer_hands_its_voter_seats_and_values_to_other_peers():
    c = Cluster(3, spares=1)
    try:
        stored = c.run(
            0,
            """
import chimaera as ch
ch.init(detach=True)
ch.coord.Store("drain").put("v", ch.put(bytes(range(256)) * (16 << 10)))  # 4 MiB, held by this peer
print("STORED")
""",
        )
        assert lines(stored, "STORED") == [""], stored.stderr[-2000:]
        c.signal(0, signal.SIGTERM)  # drain a founder: a voter of every group
        c.procs[0].wait(timeout=60)
        assert "replacing a draining voter" in "".join(c.log(i) for i in (1, 2, 3)), c.log(0)[-2000:]
        assert "values handed to other peers" in c.log(0), c.log(0)[-2000:]
        read = c.run(2, 'import chimaera as ch\nv = ch.coord.Store("drain").get("v").result(timeout=60)\nprint("READ", len(v), v[:3])\n')
        assert lines(read, "READ") == [f"{4 << 20} {bytes(range(3))!r}"], read.stderr[-2000:]
        # At once, before any dead voter could be replaced: a second founder.
        # Groups that still counted the drained peer as a voter would have one
        # live voter of three.
        c.signal(1, signal.SIGKILL)
        out = c.run(
            2,
            """
import chimaera as ch

@ch.remote
def add(a, b):
    return a + b

print("SUM", sum(ch.get((add.remote(i, i) for i in range(50)), timeout=60)))
""",
            timeout=120,
        )
        assert lines(out, "SUM") == [str(sum(2 * i for i in range(50)))], out.stderr[-2000:]
    finally:
        c.close()


@pytest.mark.slow
def test_an_unsynced_peer_whose_machine_rebooted_rejoins_as_a_new_member(monkeypatch):
    # Without fsync a voter's log lives in the page cache: after a reboot it
    # may have forgotten its vote, so it must not vote again under its old id.
    monkeypatch.setenv("CHIMAERA_RAFT_SYNC", "none")
    c = Cluster(3, spares=1)
    add = """
import chimaera as ch

@ch.remote
def add(a, b):
    return a + b

print("SUM", sum(ch.get((add.remote(i, i) for i in range(60)), timeout=120)))
"""
    try:
        assert lines(c.run(0, add), "SUM") == [str(sum(2 * i for i in range(60)))]
        c.signal(2, signal.SIGKILL)
        marker = os.path.join(c.dir(2), "raft-boot")
        boot, *rest = open(marker).read().splitlines()
        assert rest == ["unsynced", "running"], rest
        with open(marker, "w") as f:  # as if the machine had rebooted since
            f.write("\n".join(["another-boot", *rest]) + "\n")
        old_id = open(os.path.join(c.dir(2), "peer-id")).read()
        c.start(2)
        c.wait_ready([2])
        assert "joining again as a new member" in c.log(2)
        assert open(os.path.join(c.dir(2), "peer-id")).read() != old_id
        # The cluster keeps serving, through the rejoined peer too.
        assert lines(c.run(2, add, timeout=180), "SUM") == [str(sum(2 * i for i in range(60)))]
    finally:
        c.close()


DETACHED_ADDS = """
import time
import chimaera as ch

@ch.remote
def add(i):
    return i + 1000

run = ch.init(detach=True)
futs = [add.remote(i) for i in range(50)]
print("SUM", sum(ch.get(futs, timeout=120)))
print("RUN", run)
"""


def read_detached(c: Cluster, i: int, run_id: str) -> list[str]:
    out = c.run(
        i,
        f"""
import chimaera as ch
run = ch.runs.attach({run_id!r})
run.wait(timeout=120)
print("VALUES", sorted(v for _, v, e in run.outcomes() if e is None) == [i + 1000 for i in range(50)])
""",
        timeout=240,
    )
    return lines(out, "VALUES")


def test_a_whole_unsynced_cluster_rebooting_at_once_keeps_its_groups(monkeypatch):
    # Every voter of every group comes back from an unclean reboot. Discarding
    # state that no other voter still leads would lose the groups for good.
    monkeypatch.setenv("CHIMAERA_RAFT_SYNC", "none")
    c = Cluster(3)
    try:
        out = c.run(0, DETACHED_ADDS)
        run_id = lines(out, "RUN")[0]
        ids = {}
        for i in range(3):
            c.signal(i, signal.SIGKILL)
            marker = os.path.join(c.dir(i), "raft-boot")
            boot, *rest = open(marker).read().splitlines()
            with open(marker, "w") as f:  # as if every machine had rebooted
                f.write("\n".join(["another-boot", *rest]) + "\n")
            ids[i] = open(os.path.join(c.dir(i), "peer-id")).read()
        for i in range(3):
            c.start(i)
        c.wait_ready(range(3), timeout=120)
        # A peer that finds the other two already leading every group may still
        # rejoin as new (they hold a majority); a majority must have kept state.
        kept = [i for i in range(3) if "keeping its state" in c.log(i)]
        assert len(kept) >= 2, [c.log(i)[-1500:] for i in range(3)]
        for i in kept:
            assert open(os.path.join(c.dir(i), "peer-id")).read() == ids[i]
        live = kept[0]
        assert read_detached(c, live, run_id) == ["True"]
    finally:
        c.close()


def status(c: Cluster, i: int) -> dict:
    out = subprocess.run([BIN, "--dir", c.dir(i), "status", "--json"], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_a_peer_serves_while_some_groups_have_no_quorum():
    c = Cluster(3)
    try:
        run_id = lines(c.run(0, DETACHED_ADDS), "RUN")[0]
        for i in range(3):
            c.signal(i, signal.SIGKILL)
        c.start(0)  # alone: no group has a majority
        c.wait_ready([0], timeout=60)
        time.sleep(5)
        assert c.procs[0].poll() is None, c.log(0)[-2000:]
        alone = status(c, 0)
        assert alone["leaderless_groups"] == [0, 1, 2, 3, 4], alone  # the directory and the data groups
        assert all(g["leader"] == "" for g in alone["groups"]) and len(alone["groups"]) == 5, alone["groups"]
        c.start(1)  # a majority again: the groups come back without a restart
        c.wait_ready([1])
        assert read_detached(c, 0, run_id) == ["True"]
        deadline = time.monotonic() + 30
        while (back := status(c, 0))["leaderless_groups"] and time.monotonic() < deadline:
            time.sleep(0.5)
        assert back["leaderless_groups"] == [] and all(g["leader"] for g in back["groups"]), back
    finally:
        c.close()


def test_cluster_recover_rebuilds_groups_whose_other_voters_are_gone():
    c = Cluster(3, spares=1)
    try:
        run_id = lines(c.run(0, DETACHED_ADDS), "RUN")[0]
        for i in range(3):
            c.signal(i, signal.SIGKILL)
        for i in (1, 2):
            shutil.rmtree(c.dir(i))  # their machines are gone for good
        # The spare gives peer 0 up too, so only peer 0 can find the spare again:
        # its seeds are gone, and it rejoins through the members it recorded.
        deadline = time.monotonic() + 60
        while not any("retiring it" in ln and f"addr={c.addrs[0]}" in ln for ln in c.log(3).splitlines()):
            assert time.monotonic() < deadline, c.log(3)[-2000:]
            time.sleep(0.5)
        recover = [BIN, "--dir", c.dir(0), "cluster", "recover", "--group"]
        refused = subprocess.run([*recover, "1"], capture_output=True, text=True, timeout=60)
        assert refused.returncode != 0 and "--force-new-quorum" in refused.stderr
        for g in range(c.data_groups + 1):
            r = subprocess.run([*recover, str(g), "--force-new-quorum"], capture_output=True, text=True, timeout=60)
            assert r.returncode == 0, r.stderr
        c.start(0)
        c.wait_ready([0], timeout=60)
        assert read_detached(c, 0, run_id) == ["True"]
        deadline = time.monotonic() + 60
        while "adding a voter" not in c.log(0) and time.monotonic() < deadline:
            time.sleep(0.5)
        assert "adding a voter" in c.log(0), "the spare is added back as a voter"
    finally:
        c.close()


def test_a_peer_without_the_cluster_token_is_refused():
    c = Cluster(3, spares=1)
    try:
        # The spare restarts holding a different token.
        c.signal(3, signal.SIGKILL)
        os.rename(c.dir(3), c.dir(3) + "-old")
        c.start(3, token=c.write_token("wrong", "0" * 64))
        deadline = time.monotonic() + 15
        while "missing or wrong cluster token" not in c.log(3):
            assert time.monotonic() < deadline, c.log(3)[-2000:]
            time.sleep(0.25)
        assert not os.path.exists(c.sock(3)), "a peer with the wrong token must never start serving"
        # The members still work.
        out = c.run(0, "import chimaera as ch\n@ch.remote\ndef one():\n    return 1\nprint('ONE', one.remote().result(timeout=60))\n")
        assert lines(out, "ONE") == ["1"]
    finally:
        c.close()


@pytest.mark.parametrize("how", ["ca", "env"], ids=["tls-ca-dir", "ca-in-environment"])
def test_peers_issue_their_own_certificates_from_the_cluster_ca(how):
    # One directory per cluster (`cluster init`), shipped like the token, or
    # the CA in the environment (slurm-launch): each peer issues itself a
    # certificate from it.
    c = Cluster(3, tls=how)
    try:
        out = c.run(0, WHERE + """
@ch.remote
def where(i):
    time.sleep(0.05)
    return peer_name()

print("PEERS", len(set(ch.get((where.remote(i) for i in range(60)), timeout=60))))
""")
        assert int(lines(out, "PEERS")[0]) == 3, "work crosses peers over TLS"
        assert all(os.path.exists(os.path.join(c.dir(i), "tls", "peer.pem")) for i in range(3))
        # A peer issuing itself a certificate from another CA is refused.
        home = os.path.join(c.root, "stranger")
        path = os.path.join(c.root, "stranger.log")
        stranger = subprocess.Popen(
            [BIN, "--dir", home, "peer", "start", "--listen", f"127.0.0.1:{free_ports(1)[0]}", "--seeds", c.seeds,
             "--cluster", "test", "--tls-ca", c.make_ca("other-ca"), "--token-file", c.token],
            stdout=open(path, "ab"), stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 15
            while "refused" not in open(path, errors="replace").read():
                assert time.monotonic() < deadline, open(path, errors="replace").read()[-2000:]
                time.sleep(0.25)
            assert not os.path.exists(socket_path(home, "peer.sock")), "a peer with a foreign CA must never serve"
        finally:
            stranger.kill()
            stranger.wait(timeout=10)
    finally:
        c.close()


def test_mutual_tls_cluster_runs_and_refuses_a_peer_from_another_ca():
    c = Cluster(3, spares=1, tls=True)
    try:
        out = c.run(0, WHERE + """
@ch.remote
def where(i):
    time.sleep(0.05)
    return peer_name()

print("PEERS", len(set(ch.get((where.remote(i) for i in range(60)), timeout=60))))
""")
        assert int(lines(out, "PEERS")[0]) >= 3, "work crosses peers (the spare runs work too) over TLS"
        # The spare restarts with the right token but a certificate from another CA.
        c.signal(3, signal.SIGKILL)
        os.rename(c.dir(3), c.dir(3) + "-old")
        c.tls[3] = c.issue(c.make_ca("other-ca"), 3)
        c.start(3)
        deadline = time.monotonic() + 15
        while "refused" not in c.log(3):
            assert time.monotonic() < deadline, c.log(3)[-2000:]
            time.sleep(0.25)
        assert not os.path.exists(c.sock(3)), "a peer with a foreign certificate must never start serving"
        out = c.run(0, "import chimaera as ch\n@ch.remote\ndef one():\n    return 1\nprint('ONE', one.remote().result(timeout=60))\n")
        assert lines(out, "ONE") == ["1"]
    finally:
        c.close()


REMOTE = """
import hashlib, chimaera as ch

@ch.remote
def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

@ch.remote
def make(n: int) -> bytes:
    return bytes(range(256)) * (n // 256)

big = bytes(range(256)) * (3 << 12)        # 3 MiB: uploaded as a blob
print("ARG", digest.remote(big).result(timeout=120) == hashlib.sha256(big).hexdigest())
out = make.remote(4 << 20).result(timeout=120)   # 4 MiB result: downloaded
print("RESULT", len(out), out == bytes(range(256)) * ((4 << 20) // 256))
stored = ch.put(big)
print("PUT", digest.remote(stored).result(timeout=120) == hashlib.sha256(big).hexdigest())
from chimaera._runtime import Runtime
print("REMOTE", Runtime.get().remote)
"""


@pytest.fixture(scope="module")
def tcp():
    c = Cluster(3, clients=True)
    yield c
    c.close()


def test_a_driver_on_another_machine_runs_over_tcp_and_moves_large_values(tcp):
    out = tcp.remote(1, REMOTE)
    assert lines(out, "ARG ") == ["True"] and lines(out, "PUT ") == ["True"]
    assert lines(out, "RESULT ") == [f"{4 << 20} True"]
    assert lines(out, "REMOTE ") == ["True"], "the driver must not have fallen back to a local peer"


def test_a_driver_on_another_chimaera_version_than_the_cluster_is_told(tcp):
    connect = "import chimaera as ch\nch.init()\nprint('CONNECTED', ch._runtime.Runtime.get().remote)\n"
    same = tcp.remote(1, connect)
    assert lines(same, "CONNECTED ") == ["True"] and "CH116" not in same.stderr, same.stderr[-2000:]
    other = tcp.remote(1, "import chimaera._runtime as r\nr.VERSION = '0.0.1'\n" + connect)
    assert lines(other, "CONNECTED ") == ["True"], other.stderr[-2000:]
    assert "CH116" in other.stderr and "0.0.1" in other.stderr, other.stderr[-2000:]


def test_a_client_without_the_cluster_token_is_rejected(tcp):
    out = tcp.remote(0, "import chimaera as ch\nch.init()\n", token=False)
    assert out.returncode != 0 and "token" in out.stderr, out.stderr[-2000:]


def test_a_run_is_committed_by_several_groups_and_still_lists_and_cancels_as_one(monkeypatch):
    # Leaders move toward an even spread after a second of steady membership,
    # so the driver's peer soon leads only some groups: the run spreads over others.
    monkeypatch.setenv("CHIMAERA_REBALANCE_SECONDS", "1")
    monkeypatch.setenv("CHIMAERA_SPREAD_RATE", "0")  # spread however slowly the driver submits
    c = Cluster(3, data_groups=6)
    try:
        deadline = time.monotonic() + 60
        while not any(g["group"] > 0 and g["leader"] and not g["leading"] for g in status(c, 0)["groups"]):
            assert time.monotonic() < deadline, status(c, 0)["groups"]
            time.sleep(0.5)
        out = c.run(
            0,
            """
import time
import chimaera as ch
from chimaera._native import vshard_of
from chimaera._runtime import Runtime

@ch.remote
def child(x):
    return x

@ch.remote
def parent(n):
    return sum(ch.get([child.remote(i) for i in range(n)]))

@ch.remote
def nap(i):
    time.sleep(300)

futs = [parent.remote(4) for _ in range(24)]
print("SPREAD", len(Runtime.get()._spread))
print("SHARDS", len({vshard_of(f.key[16:]) for f in futs}))
print("SUM", sum(ch.get(futs, timeout=120)))
run = ch.runs.current_run()
print("LISTED", len(run.tasks()))
naps = [nap.remote(i) for i in range(6)]
deadline = time.monotonic() + 60
while len(run.tasks("Running")) < 6 and time.monotonic() < deadline:
    time.sleep(0.05)
run.cancel()
ends = set()
for f in naps:
    try:
        f.result(timeout=60)
        ends.add("finished")
    except ch.Cancelled:
        ends.add("cancelled")
print("NAPS", sorted(ends), len({vshard_of(f.key[16:]) for f in naps}))
""",
        )
        assert int(lines(out, "SPREAD")[0]) >= 2, out.stdout
        assert int(lines(out, "SHARDS")[0]) >= 2, "the parents went to more than one group"
        assert lines(out, "SUM") == [str(24 * 6)]
        assert lines(out, "LISTED") == [str(24 * 5)], "every parent and child is listed, whichever group holds it"
        ends, shards = lines(out, "NAPS")[0].rsplit(" ", 1)
        assert ends == "['cancelled']" and int(shards) >= 2, "cancelling the run reaches every group it spread to"
    finally:
        c.close()


def test_the_cli_lists_inspects_and_cancels_a_run_from_any_peer_over_tcp():
    # Four peers, groups of three voters: some peer holds no replica of the run's group.
    c = Cluster(4, clients=True)
    try:
        out = c.run(
            0,
            """
import time
import chimaera as ch

@ch.remote
def nap(i):
    time.sleep(120)

run = ch.init(detach=True)
futs = [nap.remote(i) for i in range(4)]
while sum(ch.runs.current_run().status().values()) < 4:
    time.sleep(0.05)
print("RUN", run)
""",
        )
        run_id = lines(out, "RUN")[0]
        env = {k: v for k, v in os.environ.items() if not k.startswith("CHIMAERA_")}

        def cli(i: int, *args: str, token: bool = True) -> subprocess.CompletedProcess:
            e = dict(env, CHIMAERA_TOKEN_FILE=c.token) if token else env
            cmd = [BIN, *args, "--cluster", f"tcp://{c.client_addrs[i]}"]
            return subprocess.run(cmd, env=e, capture_output=True, text=True, timeout=60)

        for i in range(4):
            listed = cli(i, "runs", "--json")
            assert listed.returncode == 0, listed.stderr
            runs = {r["run"]: r for r in json.loads(listed.stdout)["runs"]}
            assert runs[run_id]["state"] == "active" and runs[run_id]["detached"], runs
            assert runs[run_id]["command"].endswith(".py"), runs[run_id]
            shown = cli(i, "inspect", run_id, "--json")
            assert json.loads(shown.stdout)["state"] == "Active", (i, shown.stdout, shown.stderr)
        cancelled = cli(3, "cancel", run_id)
        assert cancelled.returncode == 0 and "cancelled" in cancelled.stdout, cancelled.stdout + cancelled.stderr
        for i in range(4):
            shown = json.loads(cli(i, "inspect", run_id, "--json").stdout)
            assert shown["state"] == "Cancelled" and sum(shown["tasks"].values()) == 4, (i, shown)
        refused = cli(0, "runs", token=False)
        assert refused.returncode != 0 and "token" in refused.stderr, refused.stderr
        program = "import chimaera as ch; ch.init(); print('REMOTE', ch._runtime.Runtime.get().remote)"
        ran = subprocess.run(
            [BIN, "run", "--cluster", f"tcp://{c.client_addrs[2]}", "--", sys.executable, "-c", program],
            env=dict(env, CHIMAERA_TOKEN_FILE=c.token, PYTHONPATH=os.path.join(REPO, "python")),
            cwd=c.root,  # a remote run ships its working directory
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert lines(ran, "REMOTE") == ["True"], ran.stderr[-2000:]
    finally:
        c.close()


def test_a_submitted_program_runs_on_the_cluster_and_its_output_is_the_runs_log(tcp):
    project = tempfile.mkdtemp(prefix="chp", dir="/tmp")
    with open(os.path.join(project, "helper.py"), "w") as f:
        f.write("def square(x):\n    return x * x\n")
    with open(os.path.join(project, "app.py"), "w") as f:
        f.write(
            "import os\nimport chimaera as ch\nfrom helper import square\n\n"
            "@ch.remote\ndef sq(x):\n    return square(x)\n\n"
            "print('SQUARES', sum(ch.get([sq.remote(i) for i in range(20)], timeout=60)), flush=True)\n"
        )
    env = {k: v for k, v in os.environ.items() if not k.startswith("CHIMAERA_")}
    env.update(CHIMAERA_TOKEN_FILE=tcp.token, CHIMAERA_PYTHON=sys.executable, PYTHONPATH=os.path.join(REPO, "python"))
    at = ["--cluster", f"tcp://{tcp.client_addrs[1]}"]
    out = subprocess.run([BIN, "submit", *at, "--", "python", "app.py"], cwd=project, env=env, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr[-2000:]
    run_id = out.stdout.strip().splitlines()[-1]
    deadline, text = time.monotonic() + 90, ""
    while "SQUARES" not in text and time.monotonic() < deadline:
        time.sleep(0.5)
        text = subprocess.run([BIN, "logs", run_id, *at], env=env, capture_output=True, text=True, timeout=60).stdout
    assert f"SQUARES {sum(i * i for i in range(20))}" in text, text[-2000:]
    listed = subprocess.run([BIN, "runs", "--json", *at], env=env, capture_output=True, text=True, timeout=60)
    assert any(r["run"] == run_id and r["detached"] for r in json.loads(listed.stdout)["runs"]), listed.stdout


def test_a_driver_credential_reaches_only_its_tenants_work(tcp):
    def credential(tenant: str) -> str:
        cmd = [BIN, "cluster", "credential", "--tenant", tenant, "--token-file", tcp.token]
        return tcp.write_token(f"cred-{tenant}", subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)

    def driver(cred: str, code: str) -> subprocess.CompletedProcess:
        home = tempfile.mkdtemp(prefix="chr", dir="/tmp")
        env = {"CHIMAERA_ADDRESS": f"tcp://{tcp.client_addrs[0]}", "CHIMAERA_HOME": home, "CHIMAERA_TOKEN_FILE": cred}
        return run_script(code, timeout=180, env=env)

    alice, bob = credential("alice"), credential("bob")
    out = driver(
        alice,
        """
import chimaera as ch

@ch.remote
class Counter:
    def __init__(self):
        self.n = 0
    def add(self):
        self.n += 1
        return self.n

run = ch.init(detach=True)
c = Counter.options(name="counter", detach=True).remote()
print("ADD", c.add.remote().result(timeout=60))
ch.coord.Store("shared").put("k", "alice's")
print("TENANT", ch._runtime.Runtime.get().tenant)
print("RUN", run)
""",
    )
    assert lines(out, "ADD") == ["1"] and lines(out, "TENANT") == ["alice"], out.stderr[-2000:]
    run_id = lines(out, "RUN")[0]
    out = driver(
        bob,
        f"""
import chimaera as ch
print("STORE", ch.coord.Store("shared").get("k"))
try:
    ch.actor("counter")
except ch.NotFound:
    print("ACTOR hidden")
try:
    ch.runs.attach({run_id!r}).status()
except Exception as e:
    print("RUN", type(e).__name__, "no run" in str(e))
# A client that claims alice's tenant itself: the peer still knows it is bob.
ch._runtime.Runtime.get().client.set_tenant("alice")
try:
    print("FORGED read", ch.coord.Store("shared").get("k"))
except Exception as e:
    print("FORGED", "credential is for tenant bob" in str(e))
ch._runtime.Runtime.get().tenant = "alice"
try:
    ch.actor("counter")  # alice's named actor's id
    print("FORGED actor found")
except ch.NotFound:
    print("FORGED actor hidden")
""",
    )
    assert lines(out, "STORE") == ["None"], out.stderr[-2000:]
    assert lines(out, "ACTOR") == ["hidden"] and lines(out, "RUN")[0].endswith("True"), out.stdout + out.stderr[-2000:]
    assert lines(out, "FORGED") == ["True", "actor hidden"], out.stdout + out.stderr[-2000:]
    claims = driver(bob, "import chimaera as ch\nch.init(tenant='alice')\n")
    assert claims.returncode != 0 and "credential is for tenant bob" in claims.stderr, claims.stderr[-2000:]
    # An actor whose "constructor" is a call to alice's actor: refused, whoever asks.
    stolen = driver(
        bob,
        """
import os
import chimaera as ch
from chimaera._native import child_id
from chimaera._options import Layer, resolve
from chimaera._remote import _keyed_actor_id, build_call
from chimaera._runtime import CALL_ACTOR_CTOR, CALL_ACTOR_METHOD

@ch.remote
class Counter:
    def add(self):
        return 0

ch.init()
rt = ch._runtime.Runtime.get()
eff = resolve((), Layer({}, "test", "decorator"))
key = rt.run + child_id(rt.run, [b"probe", os.urandom(8)])
spec, _ = build_call(rt, Counter._cls, (), {}, kind=CALL_ACTOR_METHOD, eff=eff, actor=_keyed_actor_id("alice", "counter"),
                     handle=1, seq=0, method="add", key=key)
mine = child_id(rt.run, [b"actor", os.urandom(16)])
ev = rt.wait_actor(rt.client.create_actor(mine, spec, "bob", "", spec["fn_digest"], 0, 0, False, False))
print("CTOR", "refused" if ev[0] == "error" else "accepted", "own CALL_ACTOR_CTOR" in str(ev))
# An unnamed actor at the id alice's named actor has: whether or not it
# exists, refused the same way (no squatting, no telling it exists).
theirs = _keyed_actor_id("alice", "counter")
ctor_key = rt.run + child_id(rt.run, [theirs, b"ctor"])
spec, _ = build_call(rt, Counter._cls, (), {}, kind=CALL_ACTOR_CTOR, eff=eff, actor=theirs, key=ctor_key)
ev = rt.wait_actor(rt.client.create_actor(theirs, spec, "bob", "", spec["fn_digest"], 0, 0, False, False, os.urandom(16)))
print("SQUAT", "refused" if ev[0] == "error" else "accepted", "must be derived" in str(ev))
""",
    )
    assert lines(stolen, "CTOR") == ["refused True"], stolen.stdout + stolen.stderr[-2000:]
    assert lines(stolen, "SQUAT") == ["refused True"], stolen.stdout + stolen.stderr[-2000:]
    again = driver(alice, "import chimaera as ch\nprint('NEXT', ch.actor('counter').add.remote().result(timeout=60))\n")
    assert lines(again, "NEXT") == ["2"], "alice finds her own named actor, which nobody else called: " + again.stderr[-2000:]

    def cli(cred: str, *args: str) -> subprocess.CompletedProcess:
        env = {k: v for k, v in os.environ.items() if not k.startswith("CHIMAERA_")}
        cmd = [BIN, *args, "--cluster", f"tcp://{tcp.client_addrs[1]}"]
        return subprocess.run(cmd, env=dict(env, CHIMAERA_TOKEN_FILE=cred), capture_output=True, text=True, timeout=60)

    listed = lambda cred: {r["run"] for r in json.loads(cli(cred, "runs", "--json").stdout)["runs"]}  # noqa: E731
    assert run_id in listed(alice) and run_id in listed(tcp.token) and run_id not in listed(bob)
    for args in (["inspect", run_id], ["cancel", run_id], ["logs", run_id]):
        refused = cli(bob, *args)
        assert refused.returncode != 0 and "no run" in refused.stderr, (args, refused.stderr)
    assert "cancelled" in cli(alice, "cancel", run_id).stdout


def test_a_client_reaches_a_tls_cluster_only_with_its_ca():
    c = Cluster(3, tls=True, clients=True)
    try:
        ca = os.path.join(os.path.dirname(c.token), "ca.pem")
        out = c.remote(0, REMOTE, ca=ca)
        assert lines(out, "ARG ") == ["True"]
        plain = c.remote(0, "import chimaera as ch\nch.init()\n")
        assert plain.returncode != 0, "a client speaking plain TCP to a TLS listener connected"
    finally:
        c.close()


@pytest.mark.parametrize("sig", [signal.SIGKILL, signal.SIGTERM], ids=["lost", "drained"])
def test_a_durable_actor_survives_the_loss_of_its_peer(sig):
    c = Cluster(3)
    try:
        out = c.run(
            0,
            WHERE
            + """
@ch.remote(state=ch.durable.State())
class Ledger:
    def __init__(self):
        self.total = 0

    def add(self, n):
        self.total += n
        return self.total

    def where(self):
        return peer_name(), self.total

ledger = Ledger.options(name="ledger", detach=True).remote()
for i in range(10):
    ledger.add.remote(i).result(timeout=60)
print("HOST", ledger.where.remote().result(timeout=60)[0])
""",
        )
        host = int(lines(out, "HOST")[0][1:])
        c.signal(host, sig)
        other = next(i for i in range(3) if i != host)
        out = c.run(
            other,
            WHERE
            + """
ledger = ch.actor("ledger")
print("AFTER", *ledger.where.remote().result(timeout=120))
""",
        )
        moved_to, total = lines(out, "AFTER")[0].split()
        assert total == "45", "the committed state survives its peer"
        assert moved_to != f"p{host}"
    finally:
        c.close()


MOVING = WHERE + """
import sys
GO = sys.argv[0] + ".go"

@ch.remote(restart="fresh", max_restarts=1)
class Counter:
    def __init__(self):
        self.n = 0

    def bump(self):
        self.n += 1
        return peer_name(), self.n

    def die(self):
        os.kill(os.getpid(), 9)

c = Counter.options(placement=ch.dist.Placement(require={"zone": "b"})).remote()
for _ in range(3):
    host, n = c.bump.remote().result(timeout=60)
print("HOST", host, n, flush=True)
while not os.path.exists(GO):
    time.sleep(0.1)
print("AFTER", *c.bump.remote().result(timeout=90), flush=True)
try:
    c.die.remote().result(timeout=60)
except ch.ActorDied:
    pass
try:
    print("THEN", *c.bump.remote().result(timeout=60), flush=True)
except ch.ActorDied:
    print("THEN dead", flush=True)
"""


def _lose_the_actors_host(sig: int) -> tuple[str, list[str]]:
    """An actor's host is lost (SIGKILL) or drained (SIGTERM) while a driver
    on another peer holds a handle that has made calls."""
    c = Cluster(3, labels={0: "zone=a", 1: "zone=b", 2: "zone=b"})
    try:
        d = c.driver(0, MOVING)
        line = d.stdout.readline().split()
        assert line[:1] == ["HOST"], (line, d.stderr.read() if d.poll() is not None else "")
        host = int(line[1][1:])
        c.signal(host, sig)
        c.procs[host].wait(timeout=90)  # a drain ends once its work has moved
        open([a for a in d.args if a.endswith(".py")][0] + ".go", "w").close()
        out, err = d.communicate(timeout=240)
        assert d.returncode == 0, err[-3000:]
        return f"p{host}", [ln for ln in out.splitlines() if ln.startswith(("AFTER", "THEN"))]
    finally:
        c.close()


def test_an_actor_handle_keeps_its_order_after_the_actor_restarts_on_another_peer():
    host, got = _lose_the_actors_host(signal.SIGKILL)
    after = got[0].split()
    assert after[1] != host and after[2] == "1", got  # a fresh instance, elsewhere
    assert got[1] == "THEN dead", "losing its peer used its one restart"


def test_draining_a_peer_moves_its_actors_without_using_a_restart():
    host, got = _lose_the_actors_host(signal.SIGTERM)
    after = got[0].split()
    assert after[1] != host and after[2] == "1", got
    then = got[1].split()
    assert then[0] == "THEN" and then[1] != "dead" and then[2] == "1", f"the move is not a restart: {got}"


PLACED = """
import collections, chimaera as ch

@ch.remote
def where(i):
    return ch.context().node

zone = {n.peer: n.labels.get("zone") for n in ch.runs.cluster().nodes()}
print("ZONES", sorted(v for v in zone.values() if v))
a = collections.Counter(zone[p] for p in ch.get((where.options(placement=ch.dist.Placement(require={"zone": "a"})).remote(i) for i in range(30)), timeout=120))
print("REQUIRE_A", dict(a))
target = next(p for p, z in zone.items() if z == "b")
pinned = set(ch.get((where.options(placement=ch.dist.Placement(require={"node": target})).remote(i) for i in range(10)), timeout=120))
print("PINNED", pinned == {target})
try:
    where.options(placement=ch.dist.Placement(require={"zone": "nowhere"})).remote(0).result(timeout=30)
except ch.Infeasible as e:
    print("INFEASIBLE", "zone=nowhere" in str(e))
"""


def test_placement_requirements_choose_peers_by_label():
    c = Cluster(3, labels={0: "zone=a", 1: "zone=b", 2: "zone=b"})
    try:
        out = c.run(1, PLACED)
        assert lines(out, "ZONES ") == ["['a', 'b', 'b']"]
        assert lines(out, "REQUIRE_A ") == ["{'a': 30}"], "a required label must keep every call on matching peers"
        assert lines(out, "PINNED ") == ["True"] and lines(out, "INFEASIBLE ") == ["True"]
    finally:
        c.close()


def test_prints_reach_the_driver_from_peers_outside_the_runs_group():
    # Five peers, groups of three voters: two peers hold no replica of the run's
    # group, so they learn where its driver is from the work they are sent.
    c = Cluster(5, labels={i: f"slot=s{i}" for i in range(5)})
    try:
        out = c.run(
            0,
            """
import time
import chimaera as ch

@ch.remote
def say(slot, i):
    print("PRINTED", slot, i, flush=True)

print("RUN", ch.init())
slots = [f"s{i}" for i in range(5)]
ch.get([say.options(placement=ch.dist.Placement(require={"slot": s})).remote(s, i) for s in slots for i in range(4)], timeout=120)
time.sleep(3)  # output is shipped to the driver's peer on the next housekeeping tick
""",
        )
        assert out.returncode == 0, out.stderr[-2000:]
        printed = collections.Counter(re.findall(r"PRINTED (s\d) ", out.stdout))
        assert printed == {f"s{i}": 4 for i in range(5)}, out.stdout[-3000:]
        # The run's whole output, asked at a peer that is not its home.
        logs = subprocess.run([BIN, "--dir", c.dir(3), "logs", lines(out, "RUN")[0]], capture_output=True, text=True, timeout=60)
        assert collections.Counter(re.findall(r"PRINTED (s\d) ", logs.stdout)) == printed, logs.stdout + logs.stderr
    finally:
        c.close()


def test_actors_on_peers_outside_their_runs_group_start():
    # Five peers, groups of three voters: two peers hold no replica of the run's
    # group. A named actor placed there by a leader that holds none either arrives
    # without its run's environment; its host fetches the run's record from the
    # group and starts it (it used to never start, with no reason given).
    c = Cluster(5, labels={i: f"slot=s{i}" for i in range(5)})
    try:
        out = c.run(
            0,
            """
import chimaera as ch

@ch.remote
class Where:
    def slot(self, s):
        return s

slots = [f"s{i}" for i in range(5)]
# Named actors' ids come from their names, not their run, so their groups
# (whose leaders place them) are unrelated to the run's.
actors = [Where.options(name=f"w-{s}-{i}", placement=ch.dist.Placement(require={"slot": s})).remote() for s in slots for i in range(8)]
got = ch.get([a.slot.remote(s) for a, s in zip(actors, [s for s in slots for _ in range(8)])], timeout=120)
print("SLOTS", sorted(set(got)), len(got))
""",
        )
        assert lines(out, "SLOTS") == ["['s0', 's1', 's2', 's3', 's4'] 40"], out.stdout[-2000:] + out.stderr[-2000:]
    finally:
        c.close()


def test_values_lost_with_their_peer_are_made_again_from_their_calls():
    c = Cluster(3, labels={i: f"slot=s{i}" for i in range(3)})
    marks = tempfile.mkdtemp(prefix="chl", dir="/tmp")
    go = os.path.join(marks, "go")
    try:
        driver = c.driver(
            0,
            f"""
import os, time
import chimaera as ch

def on(slot):
    return ch.dist.Placement(require={{"slot": slot}})

def mark(name):
    with open(os.path.join({marks!r}, name), "a") as f:
        f.write("ran\\n")

@ch.remote
def make(n):
    mark("make")
    return bytes(range(256)) * (n // 256)

@ch.remote
def double(b):
    mark("double")
    return b + b

@ch.remote
def total(b):
    return len(b), b[:3]

a = make.options(placement=on("s1")).remote(1 << 20)
b = double.options(placement=on("s1")).remote(a)
ch.wait([b], timeout=60)
print("READY", flush=True)
while not os.path.exists({go!r}):
    time.sleep(0.05)
print("TOTAL", total.options(placement=on("s2")).remote(b).result(timeout=120), flush=True)
""",
        )
        assert driver.stdout.readline().strip() == "READY", driver.stderr.read()[-2000:]
        c.signal(1, signal.SIGKILL)  # the only peer holding both values
        open(go, "w").close()
        out, err = driver.communicate(timeout=180)
        assert f"TOTAL ({2 << 20}, {bytes(range(3))!r})" in out, out + err[-3000:]
        ran = {n: len(open(os.path.join(marks, n)).read().splitlines()) for n in ("make", "double")}
        if sys.platform == "darwin":  # no replicas here: both calls ran again, inputs first
            assert ran == {"make": 2, "double": 2}, ran
            assert "running the call that made it again" in c.log(2), c.log(2)[-2000:]
        else:  # a replica may have served it instead
            assert set(ran.values()) <= {1, 2}, ran
    finally:
        c.close()


def test_a_generator_call_streams_on_its_consumers_peer(three):
    out = three.run(
        1,
        WHERE
        + """
@ch.remote
def numbers(n):
    yield peer_name()
    for i in range(n):
        yield i

s = numbers.remote(500)
where = next(s)
print("WHERE", where)
print("SUM", sum(s))
""",
    )
    assert lines(out, "WHERE") == ["p1"], "the generator runs where its consumer reads"
    assert lines(out, "SUM") == [str(sum(range(500)))]


def test_queued_work_moves_to_a_peer_that_joins_later():
    c = Cluster(1, spares=1)
    try:
        c.signal(1, signal.SIGKILL)  # the spare is gone before any work exists
        time.sleep(3)
        driver = c.driver(
            0,
            WHERE
            + """
@ch.remote(cpus=1)
def slow(i):
    time.sleep(0.5)
    return peer_name()

t = time.time()
where = collections.Counter(ch.get((slow.remote(i) for i in range(40)), timeout=240))
print("WHERE", dict(where), round(time.time() - t, 1), flush=True)
""",
        )
        time.sleep(1.5)  # all 40 queued on p0 (4 CPUs: 5 s of work there alone)
        c.start(1)  # the spare comes back (a new incarnation) and joins
        out, err = driver.communicate(timeout=300)
        assert driver.returncode == 0, out[-2000:] + err[-3000:]
        line = [ln for ln in out.splitlines() if ln.startswith("WHERE")][0]
        assert "'p1'" in line, f"no queued work moved to the peer that joined: {line}"
    finally:
        c.close()


def test_a_replacement_voter_installs_a_snapshot_streamed_over_the_network(monkeypatch):
    # Snapshots every 64 KiB of log, so the groups purge their early log and a
    # new voter can only catch up from a snapshot sent between peers.
    monkeypatch.setenv("CHIMAERA_RAFT_SNAPSHOT_BYTES", str(64 << 10))
    c = Cluster(3, spares=1)
    # One call at a time: each is its own log entries (submit, grant,
    # publish...), so the run's group passes the 1000 entries a snapshot keeps.
    work = """
import warnings
import chimaera as ch

warnings.simplefilter("ignore")  # CH101: one call at a time, on purpose

@ch.remote
def add(a, b):
    return a + b

print("SUM", sum(add.remote(i, i).result(timeout=60) for i in range(1500)))
"""
    try:
        assert lines(c.run(0, work, timeout=600), "SUM") == [str(sum(2 * i for i in range(1500)))]
        c.signal(1, signal.SIGKILL)
        deadline = time.monotonic() + 120
        while "installed a snapshot streamed from a peer" not in c.log(3) and time.monotonic() < deadline:
            time.sleep(1)
        assert "installed a snapshot streamed from a peer" in c.log(3), c.log(3)[-3000:]
        assert lines(c.run(0, work, timeout=600), "SUM") == [str(sum(2 * i for i in range(1500)))]
    finally:
        c.close()


def test_voters_and_leaders_spread_to_peers_that_joined_later(monkeypatch):
    # Founders hold every seat when the cluster forms. Once membership is
    # steady, seats and leadership move so later peers share the commits.
    import json

    monkeypatch.setenv("CHIMAERA_REBALANCE_SECONDS", "2")
    c = Cluster(3, spares=3)

    def leading() -> dict[int, list[int]]:
        led = {}
        for i in range(6):
            r = subprocess.run([BIN, "--dir", c.dir(i), "status", "--json"], capture_output=True, text=True, timeout=30)
            led[i] = next(n for n in json.loads(r.stdout)["nodes"] if n["this_peer"])["leading"]
        return led

    try:
        deadline = time.monotonic() + 240
        led = leading()
        while sum(1 for g in led.values() if g) < 5 and time.monotonic() < deadline:
            time.sleep(2)
            led = leading()
        assert sum(1 for g in led.values() if g) >= 5, led
        assert any(led[i] for i in (3, 4, 5)), f"a peer that joined later leads a group: {led}"
        add = """
import chimaera as ch

@ch.remote
def add(a, b):
    return a + b

print("SUM", sum(ch.get([add.remote(i, i) for i in range(60)], timeout=120)))
"""
        assert lines(c.run(4, add), "SUM") == [str(sum(2 * i for i in range(60)))]
        # A founder whose seat moved stops serving that group's stale copy, so
        # a program on it still sees every call of every group finish.
        deadline = time.monotonic() + 60
        while not any("dropping its replica" in c.log(i) for i in range(3)) and time.monotonic() < deadline:
            time.sleep(1)
        assert any("dropping its replica" in c.log(i) for i in range(3)), "no founder gave up a seat's replica"
        for i in range(6):
            assert lines(c.run(i, add), "SUM") == [str(sum(2 * i for i in range(60)))], f"a program on peer {i}"
    finally:
        c.close()


def test_a_lost_result_a_driver_reads_is_made_again_and_a_lost_put_says_why(monkeypatch):
    # No replicas: once its only holder dies, running the call again is the
    # only way back for a result. A put has no call, so reading it says why.
    monkeypatch.setenv("CHIMAERA_OBJECT_REPLICAS", "0")
    c = Cluster(3, labels={i: f"slot=s{i}" for i in range(3)})
    marks = tempfile.mkdtemp(prefix="chl", dir="/tmp")
    go = os.path.join(marks, "go")
    try:
        driver = c.driver(
            0,
            f"""
import os, time
import chimaera as ch

def on(slot):
    return ch.dist.Placement(require={{"slot": slot}})

@ch.remote
def make(n):
    with open(os.path.join({marks!r}, "make"), "a") as f:
        f.write("ran\\n")
    return bytes(range(256)) * (n // 256)

@ch.remote
def stash(n):
    return ch.put(bytes(range(256)) * (n // 256))

a = make.options(placement=on("s1")).remote(1 << 20)
p = stash.options(placement=on("s1")).remote(1 << 20)
ch.wait([a, p], timeout=60)  # finished, not read: both values are only on s1
handle = p.result(timeout=60)  # the put's handle (small); its value stays on s1
print("READY", flush=True)
while not os.path.exists({go!r}):
    time.sleep(0.05)
v = a.result(timeout=120)
print("READ", len(v), v[:3], flush=True)
try:
    handle.result(timeout=120)
    print("PUT read")
except ch.errors.DataLost as e:
    print("PUT_LOST", e, flush=True)
""",
        )
        assert driver.stdout.readline().strip() == "READY", driver.stderr.read()[-2000:]
        c.signal(1, signal.SIGKILL)  # the only peer holding either value
        open(go, "w").close()
        out, err = driver.communicate(timeout=240)
        assert f"READ {1 << 20} {bytes(range(3))!r}" in out, out + err[-3000:]
        assert len(open(os.path.join(marks, "make")).read().splitlines()) == 2, "make ran again to remake its result"
        lost = [ln for ln in out.splitlines() if ln.startswith("PUT_LOST")]
        assert lost and "ch.put" in lost[0], out + err[-3000:]
    finally:
        c.close()
