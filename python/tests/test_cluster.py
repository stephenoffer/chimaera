"""Multi-peer clusters on localhost: real peer processes, drivers in their own
processes, peers killed or paused mid-run."""

from __future__ import annotations

import os
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
        tls: bool = False,
        clients: bool = False,
        labels: dict[int, str] | None = None,
    ) -> None:
        self.root = tempfile.mkdtemp(prefix="chc", dir="/tmp")
        self.labels = labels or {}  # peer index -> CHIMAERA_LABELS
        self.addrs = [f"127.0.0.1:{p}" for p in free_ports(founders + spares)]
        # --client-listen addresses, for drivers that connect over TCP.
        self.client_addrs = [f"127.0.0.1:{p}" for p in free_ports(len(self.addrs))] if clients else []
        self.seeds = ",".join(self.addrs[:founders])
        self.data_groups = data_groups
        self.tls: dict[int, str] = {}
        if tls:
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
        cmd += ["--token-file", token or self.token]
        if i in self.tls:
            cmd += ["--tls-dir", self.tls[i]]
        if self.client_addrs:
            cmd += ["--client-listen", self.client_addrs[i]]
        env = dict(os.environ, CHIMAERA_LABELS=self.labels.get(i, ""))
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
        env = {"CHIMAERA_ADDRESS": f"tcp://{self.client_addrs[i]}", "CHIMAERA_HOME": tempfile.mkdtemp(prefix="chr", dir="/tmp")}
        if token:
            env["CHIMAERA_TOKEN_FILE"] = self.token
        if ca:
            env["CHIMAERA_CA"] = ca
        return run_script(code, timeout=180, env=env)

    def close(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10)


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

placed = collections.Counter(ch.gather(*(where.remote(i) for i in range(90)), timeout=120))
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
values = ch.gather(*futs, timeout=240)
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
while sum(ch.current_run().status().values()) < 300:
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
run = ch.attach({run_id!r})
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
values = ch.gather(*futs, timeout=300)
print("EXACT", values == list(range(400)))
""",
        )
        time.sleep(1.0)
        c.signal(1, signal.SIGSTOP)  # long enough to be declared dead and retired
        time.sleep(6)
        c.signal(1, signal.SIGCONT)
        out, err = driver.communicate(timeout=300)
        assert driver.returncode == 0, out[-2000:] + err[-3000:]
        assert "EXACT True" in out
        assert "retiring it" in c.log(0) + c.log(2), "the pause should have retired peer 1's incarnation"
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

print("SUM", sum(ch.gather(*(add.remote(i, i) for i in range(50)), timeout=120)))
""",
            timeout=180,
        )
        assert lines(out, "SUM") == [str(sum(2 * i for i in range(50)))]
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


def test_mutual_tls_cluster_runs_and_refuses_a_peer_from_another_ca():
    c = Cluster(3, spares=1, tls=True)
    try:
        out = c.run(0, WHERE + """
@ch.remote
def where(i):
    time.sleep(0.05)
    return peer_name()

print("PEERS", len(set(ch.gather(*(where.remote(i) for i in range(60)), timeout=60))))
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


def test_a_client_without_the_cluster_token_is_rejected(tcp):
    out = tcp.remote(0, "import chimaera as ch\nch.init()\n", token=False)
    assert out.returncode != 0 and "token" in out.stderr, out.stderr[-2000:]


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


def test_a_durable_actor_survives_the_loss_of_its_peer():
    c = Cluster(3)
    try:
        out = c.run(
            0,
            WHERE
            + """
@ch.remote(state=ch.State())
class Ledger:
    def __init__(self):
        self.total = 0

    def add(self, n):
        self.total += n
        return self.total

    def where(self):
        return peer_name(), self.total

ledger = Ledger.options(key="ledger", lifetime="detached").remote()
for i in range(10):
    ledger.add.remote(i).result(timeout=60)
print("HOST", ledger.where.remote().result(timeout=60)[0])
""",
        )
        host = int(lines(out, "HOST")[0][1:])
        c.signal(host, signal.SIGKILL)
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


PLACED = """
import collections, chimaera as ch

@ch.remote
def where(i):
    return ch.context().node

zone = {n.peer: n.labels.get("zone") for n in ch.cluster().nodes()}
print("ZONES", sorted(v for v in zone.values() if v))
a = collections.Counter(zone[p] for p in ch.gather(*(where.options(placement=ch.Placement(require={"zone": "a"})).remote(i) for i in range(30)), timeout=120))
print("REQUIRE_A", dict(a))
target = next(p for p, z in zone.items() if z == "b")
pinned = set(ch.gather(*(where.options(placement=ch.Placement(require={"node": target})).remote(i) for i in range(10)), timeout=120))
print("PINNED", pinned == {target})
try:
    where.options(placement=ch.Placement(require={"zone": "nowhere"})).remote(0).result(timeout=30)
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
where = collections.Counter(ch.gather(*(slow.remote(i) for i in range(40)), timeout=240))
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
