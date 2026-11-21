"""Output of calls that run on another peer reaches the driver and the run's log."""

from __future__ import annotations

from test_cluster import Cluster


def test_print_on_another_peer_reaches_the_driver_and_its_peers_run_log():
    cluster = Cluster(3)
    try:
        out = cluster.run(
            0,
            """
import time
import chimaera as ch

@ch.remote
def shout(tag):
    print(f"hello from {tag}", flush=True)
    return ch.context().node

me = [n.peer for n in ch.cluster().nodes() if n.this_peer][0]
others = [n.peer for n in ch.cluster().nodes() if not n.this_peer]
for peer in others:
    pinned = shout.options(placement=ch.Placement(require={"node": peer}))
    assert pinned.remote(peer[:8]).result(timeout=60) == peer
deadline = time.time() + 10
want = {f"hello from {p[:8]}" for p in others}
while time.time() < deadline:
    kept = {line.split("| ", 1)[1] for line in ch.current_run().logs() if "hello from" in line}
    if kept >= want:
        break
    time.sleep(0.2)
print("KEPT", sorted(kept) == sorted(want))
time.sleep(0.5)  # let the live output reach this process before it exits
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        assert "KEPT True" in out.stdout, out.stdout
        for line in [ln for ln in out.stdout.splitlines() if ln.startswith("KEPT")]:
            assert line == "KEPT True"
        live = out.stdout + out.stderr
        assert live.count("hello from ") >= 2, live[-2000:]  # printed live by the driver, from both other peers
    finally:
        cluster.close()
