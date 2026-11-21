"""An elastic gang (ranks=ch.auto) that loses a peer comes back smaller."""

from __future__ import annotations

import signal
import time

from test_cluster import Cluster

DRIVER = """
import time
import chimaera as ch

@ch.remote(cpus=2)
def train():
    g = ch.context().gang
    where = g.all_gather(ch.context().node)
    if g.rank == 0:
        ch.Store("elastic").put(f"where/{g.generation}", where)
    if g.generation == 1:
        time.sleep(600)  # stays busy until a peer is lost
    return g.rank, g.size, g.generation

victim = [n.peer for n in ch.cluster().nodes() if n.address.endswith(":{port}")][0]
gang = train.options(ranks=ch.auto(min=2, max=12)).gang()
deadline = time.monotonic() + 120
while (where := ch.Store("elastic").get("where/1")) is None and time.monotonic() < deadline:
    time.sleep(0.2)
print("READY", len(where or []), victim in (where or []), flush=True)
out = gang.result(timeout=240)
print("RESULT", gang.generation, [r[1] for r in out], flush=True)
for d in gang.decisions:
    print("DECISION", d, flush=True)
"""


def test_an_elastic_gang_that_loses_a_peer_comes_back_smaller():
    cluster = Cluster(3)  # 4 CPUs each: six 2-CPU ranks, two per peer
    try:
        port = cluster.addrs[2].rsplit(":", 1)[1]
        driver = cluster.driver(0, DRIVER.replace("{port}", port))
        ready = driver.stdout.readline().split()
        assert ready == ["READY", "6", "True"], (ready, driver.stderr.read()[-3000:] if driver.poll() else "")
        started = time.monotonic()
        cluster.signal(2, signal.SIGKILL)
        out, err = driver.communicate(timeout=300)
        assert driver.returncode == 0, err[-3000:]
        lines = out.splitlines()
        assert lines[0] == "RESULT 2 [4, 4, 4, 4]", (lines, err[-2000:])
        assert "the live peers hold 6" in lines[1] and lines[2].startswith("DECISION generation 2: 4 ranks"), lines
        assert time.monotonic() - started < 120, "the gang did not wait for the lost peer to come back"
    finally:
        cluster.close()
