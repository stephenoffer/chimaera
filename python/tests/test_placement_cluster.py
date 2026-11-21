"""Placement(require=...) on a formed cluster, for actors as for tasks."""

from __future__ import annotations

from test_cluster import Cluster


def test_actors_land_on_the_peer_their_labels_require():
    cluster = Cluster(3)
    try:
        out = cluster.run(
            0,
            """
import chimaera as ch

@ch.remote
class Where:
    def node(self):
        return ch.context().node

peers = [n.peer for n in ch.cluster().nodes()]
placed = []
for _ in range(3):
    for target in peers:
        a = Where.options(placement=ch.Placement(require={"node": target})).remote()
        placed.append(a.node.remote().result(timeout=60) == target)
        a.close()
print(sum(placed), len(placed))
""",
        )
        assert out.returncode == 0, out.stderr[-2000:]
        assert out.stdout.split() == ["9", "9"], out.stdout
    finally:
        cluster.close()
