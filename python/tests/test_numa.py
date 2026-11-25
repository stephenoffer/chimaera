"""On a host with several NUMA nodes, each new worker whose thread count fits
one node runs on that node's CPUs and prefers its memory. These tests give the
peer a made-up two-node layout of this machine's own CPUs
(CHIMAERA_TOPOLOGY_ROOT) and read each worker's CPU affinity back. Linux only:
elsewhere workers are never bound.

Whether a bound worker's memory lands on its node, and what that does for
throughput, needs a machine with real NUMA nodes: bench/evals/numa_check.py."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, run_script

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="workers are bound to NUMA nodes on Linux only")


def _cpus(text: str) -> list[int]:
    out: list[int] = []
    for part in text.strip().split(","):
        first, _, last = part.partition("-")
        out += range(int(first), int(last or first) + 1)
    return out


def _allowed() -> list[int]:
    with open("/proc/self/status") as f:
        return _cpus(next(line for line in f if line.startswith("Cpus_allowed_list:")).split(":", 1)[1])


def _halves() -> list[list[int]]:
    """This machine's CPUs as two nodes."""
    cpus = _allowed()
    return [cpus[: len(cpus) // 2], cpus[len(cpus) // 2 :]]


def _start_peer(nodes: list[list[int]], **env: str) -> tuple[str, subprocess.Popen]:
    """A peer that takes `nodes` (CPU ids per node) for its NUMA layout."""
    home = tempfile.mkdtemp(prefix="chu", dir="/tmp")
    root = os.path.join(home, "topology")
    every = ",".join(str(c) for c in sorted(c for n in nodes for c in n))
    files = {"proc/self/status": f"Cpus_allowed_list:\t{every}\n", "sys/devices/system/cpu/online": every}
    for i, cpus in enumerate(nodes):
        files[f"sys/devices/system/node/node{i}/cpulist"] = ",".join(map(str, cpus))
        files[f"sys/devices/system/node/node{i}/meminfo"] = f"Node {i} MemTotal:       1048576 kB\n"
    for path, text in files.items():
        os.makedirs(os.path.dirname(os.path.join(root, path)), exist_ok=True)
        with open(os.path.join(root, path), "w") as f:
            f.write(text)
    with open(os.path.join(home, "peer.log"), "ab") as log:
        proc = subprocess.Popen(
            [BIN, "--dir", home, "peer", "start", "--idle-timeout", "600"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
            env=dict(os.environ, CHIMAERA_TOPOLOGY_ROOT=root, **env),
        )
    deadline = time.monotonic() + 30
    while not os.path.exists(socket_path(home, "peer.sock")):
        assert time.monotonic() < deadline, open(os.path.join(home, "peer.log")).read()
        time.sleep(0.05)
    return home, proc


@pytest.fixture
def peer(request):
    layout, env = request.param
    if len(_allowed()) < 2:
        pytest.skip("two nodes need two CPUs")
    # CPUs and nodes this machine does not have, or its own CPUs in two nodes.
    nodes = [[1000, 1001], [1002, 1003]] if layout == "absent" else _halves()
    home, proc = _start_peer(nodes, **env)
    yield home
    subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=60)
    proc.wait(timeout=60)  # it writes its profiles as it exits
    shutil.rmtree(home, ignore_errors=True)


# Four one-CPU calls at once, then one call granted every CPU; each reports
# the CPUs its worker may run on.
AFFINITY = """
import json, os, time
import chimaera as ch

def allowed():
    with open("/proc/self/status") as f:
        text = next(l for l in f if l.startswith("Cpus_allowed_list:")).split(":", 1)[1]
    out = []
    for part in text.strip().split(","):
        first, _, last = part.partition("-")
        out += range(int(first), int(last or first) + 1)
    return out

@ch.remote(cpus=1)
def small():
    time.sleep(1)  # they overlap, so they run in two workers or more (memory decides how many)
    return os.getpid(), allowed()

@ch.remote(cpus=WIDE)
def wide():
    return os.getpid(), allowed()

small_runs = ch.get([small.remote() for _ in range(4)], timeout=120)
print(json.dumps({"small": small_runs, "wide": wide.remote().result(timeout=120)}))
"""


def _affinity(home: str, wide: int) -> tuple[dict[int, list[int]], list[int]]:
    r = run_script(AFFINITY.replace("WIDE", str(wide)), home=home, timeout=180)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout.splitlines()[-1])
    return {pid: cpus for pid, cpus in out["small"]}, out["wide"][1]


@pytest.mark.parametrize("peer", [("halves", {})], indirect=True)
def test_workers_run_on_one_node_each_and_spread_over_the_nodes(peer):
    nodes = _halves()
    small, wide = _affinity(peer, len(_allowed()))
    assert len(small) >= 2, small
    for pid, cpus in small.items():
        assert cpus in nodes, f"worker {pid} may run on {cpus}, not on one node of {nodes}"
    assert {tuple(c) for c in small.values()} == {tuple(n) for n in nodes}, "both nodes take workers"
    assert wide == _allowed(), "a worker with more threads than a node holds is not bound"


@pytest.mark.parametrize("peer", [("halves", {"CHIMAERA_NUMA_BIND": "0"})], indirect=True)
def test_binding_switched_off_leaves_workers_unbound(peer):
    small, _ = _affinity(peer, 1)
    assert all(cpus == _allowed() for cpus in small.values()), small


@pytest.mark.parametrize("peer", [("absent", {})], indirect=True)
def test_a_worker_the_kernel_will_not_bind_runs_unbound(peer):
    small, _ = _affinity(peer, 1)
    assert all(cpus == _allowed() for cpus in small.values()), small
