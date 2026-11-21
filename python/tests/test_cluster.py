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

    def __init__(self, founders: int, spares: int = 0, data_groups: int = 4) -> None:
        self.root = tempfile.mkdtemp(prefix="chc", dir="/tmp")
        self.addrs = [f"127.0.0.1:{p}" for p in free_ports(founders + spares)]
        self.seeds = ",".join(self.addrs[:founders])
        self.data_groups = data_groups
        self.token = self.write_token("token", subprocess.check_output([BIN, "cluster", "token"], text=True))
        self.procs: dict[int, subprocess.Popen] = {}
        for i in range(len(self.addrs)):
            self.start(i)
        self.wait_ready(range(len(self.addrs)))

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
        self.procs[i] = subprocess.Popen(cmd, stdout=log, stderr=log, start_new_session=True)

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
        intruder = c.write_token("wrong", "0" * 64)
        c.signal(3, signal.SIGKILL)
        rogue = Cluster.__new__(Cluster)  # same layout, a fresh spare directory
        rogue.root, rogue.addrs, rogue.seeds, rogue.data_groups, rogue.procs = c.root, c.addrs, c.seeds, 4, {}
        rogue.token = intruder
        os.rename(c.dir(3), c.dir(3) + "-old")
        rogue.start(3)
        deadline = time.monotonic() + 15
        while "refused" not in rogue.log(3) and "unauthenticated" not in rogue.log(3).lower():
            assert time.monotonic() < deadline, rogue.log(3)[-2000:]
            time.sleep(0.25)
        assert not os.path.exists(rogue.sock(3)), "a peer with the wrong token must never start serving"
        assert "missing or wrong cluster token" in rogue.log(3)
        rogue.close()
        # The members still work, and never counted the intruder.
        out = c.run(0, "import chimaera as ch\n@ch.remote\ndef one():\n    return 1\nprint('ONE', one.remote().result(timeout=60))\n")
        assert lines(out, "ONE") == ["1"]
    finally:
        c.close()
