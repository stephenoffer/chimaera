"""Federation (docs/federation.md): a full cell runs calls in another cell, and
losing that cell costs only a retry of what it had."""

from __future__ import annotations

import json
import queue
import signal
import socket
import threading
import time
import urllib.request

from test_cluster import Cluster, free_ports

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


def metrics_env(ports: list[int]) -> dict[int, dict[str, str]]:
    return {i: {"CHIMAERA_METRICS_LISTEN": f"127.0.0.1:{p}"} for i, p in enumerate(ports)}


def metric(ports: list[int], name: str) -> list[float]:
    """`chimaera_<name>` on each peer's /metrics (0 for a peer that is down)."""
    out = []
    for p in ports:
        try:
            text = urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics", timeout=5).read().decode()
        except OSError:
            text = ""
        out.append(next((float(ln.split()[1]) for ln in text.splitlines() if ln.startswith(f"chimaera_{name} ")), 0.0))
    return out


def wait_for(ready, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not ready():
        assert time.monotonic() < deadline, what
        time.sleep(0.25)


def per_second(ports: list[int], name: str, seconds: float) -> float:
    """How fast the counter `name`, summed over the peers, grows."""
    start, t0 = sum(metric(ports, name)), time.monotonic()
    time.sleep(seconds)
    return (sum(metric(ports, name)) - start) / (time.monotonic() - t0)


def test_a_full_cell_runs_calls_in_another_and_survives_losing_it(monkeypatch):
    b = Cluster(3, clients=True)  # 3 peers × 4 CPUs
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    monkeypatch.setenv("CHIMAERA_CELL", "a")
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


def test_one_peer_of_a_cell_asks_the_others_however_many_it_has(monkeypatch):
    """A cell's publisher alone asks other cells for their capacity, and the
    cell's peers read what it learned. When it dies another peer takes over,
    with a higher epoch (its lease's fencing token)."""
    b_ports = free_ports(3)
    b = Cluster(3, clients=True, env=metrics_env(b_ports))
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={b.client_addrs[0]}")
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    monkeypatch.setenv("CHIMAERA_CELL", "a")
    rates = {}
    try:
        for n in (2, 8):
            a_ports = free_ports(n)
            a = Cluster(n, env=metrics_env(a_ports))
            try:
                knows_b = lambda: all(c >= 1 for c in metric(a_ports, "fleet_cells"))  # noqa: E731
                wait_for(knows_b, 60, f"the peers of a {n}-peer cell never all learned of B")
                rates[n] = per_second(b_ports, "fleet_summaries_served_total", 6.0)
                if n == 8:
                    publishing = metric(a_ports, "fleet_publishing")
                    assert publishing.count(1.0) == 1, publishing
                    old = publishing.index(1.0)
                    epoch = metric(a_ports, "fleet_epoch")[old]
                    a.signal(old, signal.SIGKILL)

                    def took_over() -> bool:
                        pairs = zip(metric(a_ports, "fleet_publishing"), metric(a_ports, "fleet_epoch"))
                        return any(p == 1.0 and e > epoch for p, e in pairs)

                    wait_for(took_over, 30, "no peer of A took over its view")
                    assert per_second(b_ports, "fleet_summaries_served_total", 4.0) < 1.5 * rates[2], rates
            finally:
                a.close()
        # One ask a round (500 ms) whatever the cell's size. When every peer
        # asked, 8 peers asked 4 times as often as 2.
        assert rates[2] < 3.0, rates
        assert rates[8] < 1.5 * rates[2], rates
    finally:
        b.close()


def test_a_publisher_keeps_its_lease_while_other_cells_do_not_answer(monkeypatch, tmp_path):
    """The directory and the only other cell accept connections and never
    answer. Each round's requests to them time out, and the cell's publisher
    still renews its lease in time: it keeps publishing, under one epoch."""
    silent = socket.create_server(("127.0.0.1", 0))
    held = []

    def accept() -> None:
        while True:
            try:
                held.append(silent.accept()[0])  # kept open, never read or answered
            except OSError:  # closed when the test ends
                return

    threading.Thread(target=accept, daemon=True).start()
    addr = f"127.0.0.1:{silent.getsockname()[1]}"
    token = tmp_path / "fleet-token"
    token.write_text("t" * 64)
    monkeypatch.setenv("CHIMAERA_FLEET", f"b={addr}")
    monkeypatch.setenv("CHIMAERA_FLEET_DIRECTORY", addr)
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", str(token))
    monkeypatch.setenv("CHIMAERA_CELL", "a@127.0.0.1:9")
    ports = free_ports(1)
    a = Cluster(1, env=metrics_env(ports))
    try:
        wait_for(lambda: metric(ports, "fleet_publishing") == [1.0], 30, "the cell's peer never published its view")
        epoch = metric(ports, "fleet_epoch")
        # Every round's request to B times out (1 s), and the directory's
        # requests, sent apart from the rounds, time out twice (5 s each).
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            assert metric(ports, "fleet_publishing") == [1.0], "the publisher lost its lease"
            assert metric(ports, "fleet_epoch") == epoch, "the lease lapsed and was taken again"
            time.sleep(0.5)
    finally:
        a.close()
        silent.close()
        for conn in held:
            conn.close()


def test_cells_find_each_other_through_a_directory(monkeypatch):
    b_ports = free_ports(3)
    b = Cluster(3, clients=True, env=metrics_env(b_ports))
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
    a_ports = free_ports(4)
    a = Cluster(4, clients=True, env=metrics_env(a_ports))
    try:
        b_peers, _ = peers(b)
        out = a.run(0, DRIVER.replace("SECS", "2.0").replace("N", "64"))
        assert out.returncode == 0, out.stderr[-3000:]
        seconds, calls = result(out.stdout)
        assert sorted(i for i, _ in calls) == list(range(64))
        assert any(node in b_peers for _, node in calls), ("A did not find B through the directory", seconds)

        # Restart every peer of A at once, now at an endpoint B can reach and
        # with the fleet's token. Only A's new publisher talks to the directory,
        # from the revision A's stored view holds, and each cell's peers come to
        # know the other.
        monkeypatch.setenv("CHIMAERA_CELL", f"a@{a.client_addrs[0]}")
        listings = sum(metric(b_ports, "fleet_directory_listings_total"))
        assert listings >= 1, "B's first publisher lists the directory whole"
        before, t0 = sum(metric(b_ports, "store_requests_total")), time.monotonic()
        for i in range(4):
            a.signal(i, signal.SIGKILL)
        for i in range(4):
            a.start(i, token=b.token)
        a.wait_ready(range(4))
        knows_b = lambda: all(c >= 1 for c in metric(a_ports, "fleet_cells"))  # noqa: E731
        wait_for(knows_b, 30, "A's peers did not come to know B again")
        wait_for(lambda: max(metric(b_ports, "fleet_cells")) >= 1, 30, "B did not come to know A at its new endpoint")
        time.sleep(max(0.0, 15 - (time.monotonic() - t0)))
        requests = sum(metric(b_ports, "store_requests_total")) - before
        rate = requests / (time.monotonic() - t0)
        # Each cell's publisher renews its entry and reads the changes every
        # 2 s: about one request a second a cell. When every peer of both
        # cells renewed and listed, 7 peers made about 7.
        assert rate < 3.0, (requests, rate)
        # A's new publisher carried on from the revision in A's stored view,
        # and B's from its own: both read only the changes since.
        assert sum(metric(a_ports, "fleet_directory_listings_total")) == 0
        assert sum(metric(b_ports, "fleet_directory_listings_total")) == listings
    finally:
        a.close()
        b.close()


def slow_proxy(target: str, delay: float) -> socket.socket:
    """A listening socket that forwards connections to `target` and delivers
    what `target` sends back `delay` seconds late, in order."""
    host, port = target.rsplit(":", 1)
    server = socket.create_server(("127.0.0.1", 0))

    def forward(src: socket.socket, dst: socket.socket, lag: float) -> None:
        late: queue.Queue = queue.Queue()

        def deliver() -> None:
            while (item := late.get()) is not None:
                time.sleep(max(0.0, item[0] - time.monotonic()))
                try:
                    dst.sendall(item[1])
                except OSError:
                    return
            try:
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass

        threading.Thread(target=deliver, daemon=True).start()
        try:
            while data := src.recv(65536):
                late.put((time.monotonic() + lag, data))
        except OSError:
            pass
        late.put(None)

    def accept() -> None:
        while True:
            try:
                client = server.accept()[0]
            except OSError:  # closed when the test ends
                return
            upstream = socket.create_connection((host, int(port)))
            threading.Thread(target=forward, args=(client, upstream, 0.0), daemon=True).start()
            threading.Thread(target=forward, args=(upstream, client, delay), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    return server


def test_a_cell_learns_the_fleet_through_a_directory_that_answers_slowly(monkeypatch):
    """Each of A's directory requests takes 1.5 s, longer than a round waits
    on another cell. The directory is read apart from the rounds, so A still
    registers and learns of B, and keeps publishing under one epoch."""
    b_ports = free_ports(1)
    b = Cluster(1, clients=True, env=metrics_env(b_ports))
    directory = b.client_addrs[0]
    monkeypatch.setenv("CHIMAERA_FLEET_DIRECTORY", directory)
    monkeypatch.setenv("CHIMAERA_FLEET_TOKEN_FILE", b.token)
    monkeypatch.setenv("CHIMAERA_CELL", f"b@{directory}")
    b.signal(0, signal.SIGKILL)  # B's peer learns its own endpoint, so register B
    b.start(0)
    b.wait_ready([0])
    slow = slow_proxy(directory, 1.5)
    monkeypatch.setenv("CHIMAERA_FLEET_DIRECTORY", f"127.0.0.1:{slow.getsockname()[1]}")
    monkeypatch.setenv("CHIMAERA_CELL", "a")
    a_ports = free_ports(1)
    a = Cluster(1, clients=True, env=metrics_env(a_ports))
    try:
        # A registers at the endpoint B reaches it at, with the fleet's token.
        a.env[0]["CHIMAERA_CELL"] = f"a@{a.client_addrs[0]}"
        a.signal(0, signal.SIGKILL)
        a.start(0, token=b.token)
        a.wait_ready([0])
        wait_for(lambda: metric(a_ports, "fleet_cells") == [1.0], 30, "A never learned of B through the directory")
        wait_for(lambda: metric(b_ports, "fleet_cells") == [1.0], 30, "B never learned of A, which registered through it")
        epoch = metric(a_ports, "fleet_epoch")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            assert metric(a_ports, "fleet_publishing") == [1.0], "A's publisher lost its lease"
            assert metric(a_ports, "fleet_epoch") == epoch, "A's lease lapsed and was taken again"
            time.sleep(0.5)
    finally:
        a.close()
        b.close()
        slow.close()


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
    monkeypatch.setenv("CHIMAERA_CELL", "a")
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
    monkeypatch.setenv("CHIMAERA_CELL", "a")
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
    monkeypatch.setenv("CHIMAERA_CELL", "a")
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
    monkeypatch.setenv("CHIMAERA_CELL", "a")
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
