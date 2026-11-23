"""Federation (docs/federation.md): a full cell runs calls in another cell, and
losing that cell costs only a retry of what it had."""

from __future__ import annotations

import json
import signal
import time

from test_cluster import Cluster

DRIVER = """
import json, time, chimaera as ch

@ch.remote(cpus=1)
def work(i, secs):
    time.sleep(secs)
    return i, ch.context().node

start = time.time()
out = ch.get([work.remote(i, SECS) for i in range(N)], timeout=300)
print("RESULT", round(time.time() - start, 2), json.dumps(out), flush=True)
"""

PEERS = """
import json, chimaera as ch
print("PEERS", round(sum(n.cpu_used for n in ch.runs.cluster().nodes()), 2), json.dumps([n.peer for n in ch.runs.cluster().nodes()]))
"""


def result(text: str) -> tuple[float, list]:
    line = next(ln for ln in text.splitlines() if ln.startswith("RESULT"))
    _, seconds, body = line.split(" ", 2)
    return float(seconds), json.loads(body)


def peers(cell: Cluster) -> tuple[set[str], float]:
    out = cell.run(0, PEERS)
    assert out.returncode == 0, out.stderr[-2000:]
    _, used, ids = next(ln for ln in out.stdout.splitlines() if ln.startswith("PEERS")).split(" ", 2)
    return set(json.loads(ids)), float(used)


def test_a_full_cell_runs_calls_in_another_and_survives_losing_it(monkeypatch):
    b = Cluster(3, clients=True)  # 3 peers × 4 CPUs
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        # Both cells' workers start before the timed run: this measures
        # delegation. On a 4-CPU Linux container a peer's fresh workers took
        # 2 s to start, and until they had, A had room of its own and sent
        # nothing to B (B's calls began 2.5 s in, and the run took 8.1-8.5 s).
        for cell in (a, b):
            warm = cell.run(0, DRIVER.replace("SECS", "0.5").replace("N", "12"), timeout=120)
            assert warm.returncode == 0, warm.stderr[-3000:]
        # 1. 36 three-second calls: A's 12 CPUs alone can never do them in under
        # 9 s (three rounds). B takes some, and the run beats that by 10%. (Each
        # call waits 300 ms before it may leave, and B starts fresh workers: calls
        # of a few seconds keep that from deciding the result on a small machine.)
        out = a.run(0, DRIVER.replace("SECS", "3.0").replace("N", "36"))
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert [i for i, _ in calls] == list(range(36))
        in_b = sum(1 for _, node in calls if node in b_peers)
        assert in_b > 0, ("no call ran in cell B", seconds)
        assert seconds < 0.9 * 9.0, (seconds, in_b)

        # 2. Kill all of B while it runs A's calls: every call still returns, once.
        driver = a.driver(0, DRIVER.replace("SECS", "2.0").replace("N", "48"))
        deadline = time.monotonic() + 15
        while peers(b)[1] == 0 and time.monotonic() < deadline:
            time.sleep(0.2)
        killed_at = time.monotonic()
        for i in range(3):
            b.signal(i, signal.SIGKILL)
        text, err = driver.communicate(timeout=180)
        assert driver.returncode == 0, err[-3000:]
        seconds, calls = result(text)
        assert sorted(i for i, _ in calls) == list(range(48)), "every call exactly once"
        # A alone needs 8 s for these; losing B adds at most a retry of what B held.
        assert seconds < 8 / 0.95 + 2.0 + 2.0, seconds
        assert time.monotonic() - killed_at < 60

        # 3. B comes back from its directories and serves a new run.
        for i in range(3):
            b.start(i)
        b.wait_ready(range(3))
        again = b.run(0, DRIVER.replace("SECS", "0.1").replace("N", "4"), timeout=120)
        assert again.returncode == 0, again.stderr[-3000:]
        assert [i for i, _ in result(again.stdout)[1]] == list(range(4))
    finally:
        a.close()
        b.close()


def test_cells_find_each_other_through_a_directory(monkeypatch):
    b = Cluster(3, clients=True)
    directory = b.client_addrs[0]  # B hosts the directory, in a store namespace its groups replicate
    monkeypatch.setenv("CHIMAERA_FLEET_DIRECTORY", directory)
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    monkeypatch.setenv("CHIMAERA_CELL", f"b@{directory}")
    for i in range(3):  # B's peers learn their own endpoint, so register B
        b.signal(i, signal.SIGKILL)
    for i in range(3):
        b.start(i)
    b.wait_ready(range(3))
    monkeypatch.setenv("CHIMAERA_CELL", "a@127.0.0.1:9")  # A registers too; no static CHIMAERA_FLEET anywhere
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        out = a.run(0, DRIVER.replace("SECS", "2.0").replace("N", "48"))
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert sorted(i for i, _ in calls) == list(range(48))
        assert any(node in b_peers for _, node in calls), ("A did not find B through the directory", seconds)
    finally:
        a.close()
        b.close()


BIG = """
import json, time
import numpy as np
import chimaera as ch

@ch.remote(cpus=1)
def work(i, secs, value):
    time.sleep(secs)
    return i, ch.context().node, int(value.nbytes)

value = np.ones(512 * 1024)  # 4 MiB passed to every call: stored once, by its content, and shared
start = time.time()
out = ch.get([work.remote(i, 2.0, value) for i in range(48)], timeout=300)
print("RESULT", round(time.time() - start, 2), json.dumps(out), flush=True)
"""


def test_calls_that_read_a_stored_value_can_run_in_another_cell(monkeypatch):
    b = Cluster(3, clients=True)
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        out = a.run(0, BIG)
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert sorted(i for i, _, _ in calls) == list(range(48))
        assert all(size == 4 << 20 for _, _, size in calls), "every call read the whole value, wherever it ran"
        assert any(node in b_peers for _, node, _ in calls), ("no call with a stored value ran in cell B", seconds)
    finally:
        a.close()
        b.close()


PUT = """
import json, time
import numpy as np
import chimaera as ch

@ch.remote(cpus=1)
def work(i, secs, value):
    time.sleep(secs)
    return i, ch.context().node, int(value.nbytes)

value = ch.put(np.ones(512 * 1024))  # a ch.put value: its id is not its content
start = time.time()
out = ch.get([work.remote(i, 2.0, value) for i in range(48)], timeout=300)
print("RESULT", round(time.time() - start, 2), json.dumps(out), flush=True)
"""


def test_calls_that_read_a_put_value_can_run_in_another_cell(monkeypatch):
    b = Cluster(3, clients=True)
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        out = a.run(0, PUT)
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert sorted(i for i, _, _ in calls) == list(range(48))
        assert all(size == 4 << 20 for _, _, size in calls), "every call read the whole value, wherever it ran"
        assert any(node in b_peers for _, node, _ in calls), ("no call reading a ch.put value ran in cell B", seconds)
    finally:
        a.close()
        b.close()


SPLIT = """
import json, time
import chimaera as ch

@ch.remote(cpus=1, returns=3)
def work(i, secs):
    time.sleep(secs)
    return ch.context().node, bytes([i]) * 100_000, bytes([i + 1]) * 200_000

start = time.time()
fs = [work.remote(i, 2.0) for i in range(48)]
whole = ch.get(fs, timeout=300)
second = ch.get([f.split()[2] for f in fs], timeout=60)
out = [(i, node, a == bytes([i]) * 100_000 and b == c == bytes([i + 1]) * 200_000)
       for i, ((node, a, b), c) in enumerate(zip(whole, second))]
print("RESULT", round(time.time() - start, 2), json.dumps(out), flush=True)
"""


def test_a_result_stored_in_parts_in_another_cell_comes_back_whole(monkeypatch):
    b = Cluster(3, clients=True)
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        out = a.run(0, SPLIT)
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert [i for i, _, _ in calls] == list(range(48))
        assert all(ok for _, _, ok in calls), "every value read back, whole and one at a time"
        assert any(node in b_peers for _, node, _ in calls), ("no call ran in cell B", seconds)
    finally:
        a.close()
        b.close()


ARRAY = """
import json, time
import numpy as np
import chimaera as ch

@ch.remote(cpus=1)
def work(i, secs):
    time.sleep(secs)
    # A large array: on Linux it is linked from the arena as a buffer of its own.
    return ch.context().node, np.full(1 << 20, i, dtype=np.float64)

start = time.time()
out = [(node, int(a[0]), int(a[-1]), a.nbytes) for node, a in ch.get([work.remote(i, 2.0) for i in range(48)], timeout=300)]
print("RESULT", round(time.time() - start, 2), json.dumps([(i, node, first == last == i and n == 8 << 20) for i, (node, first, last, n) in enumerate(out)]), flush=True)
"""


def test_a_large_array_result_from_another_cell_comes_back_whole(monkeypatch):
    b = Cluster(3, clients=True)
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    a = Cluster(3)
    try:
        b_peers, _ = peers(b)
        out = a.run(0, ARRAY)
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert [i for i, _, _ in calls] == list(range(48))
        assert all(ok for _, _, ok in calls), "every array arrived whole"
        assert any(node in b_peers for _, node, _ in calls), ("no call ran in cell B", seconds)
    finally:
        a.close()
        b.close()
