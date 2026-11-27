"""A peer sending a large value to others is not suspected while it sends.

On a machine whose link is full of a value's bytes, replies to SWIM probes
queue behind those bytes and miss their timeout. The peers receiving the bytes
know it is alive, and say so for themselves and as relays for others. Here
the holder's link is paced (CHIMAERA_MESH_RATE) and, while it sends,
its probe replies are held past every timeout (CHIMAERA_TEST_SLOW_PINGS_MS,
debug builds), as a full link holds them."""

from __future__ import annotations

from datetime import datetime

from test_cluster import WHERE, Cluster, lines


def _at(line: str) -> float:
    return datetime.fromisoformat(line.split()[0]).timestamp()


def test_a_peer_sending_a_value_is_not_suspected_while_its_probe_replies_are_late(monkeypatch):
    # No background copies: only the two readers' fetches leave the holder.
    monkeypatch.setenv("CHIMAERA_OBJECT_REPLICAS", "0")
    slow = {"CHIMAERA_MESH_RATE": "25MB", "CHIMAERA_TEST_SLOW_PINGS_MS": "1500"}
    c = Cluster(3, labels={0: "zone=a", 1: "zone=b", 2: "zone=c"}, env={0: slow})
    try:
        out = c.run(
            0,
            WHERE
            + """
import numpy as np

@ch.remote
def first(a):
    return peer_name(), float(a[0])

x = ch.put(np.full(8 << 20, 5.0))  # 64 MiB, stored on p0
began = time.time()
calls = [first.options(placement=ch.dist.Placement(require={"zone": z})).remote(x) for z in ("b", "c")]
print("READ", sorted(ch.get(calls, timeout=120)))
print("TOOK", time.time() - began)
print("DONE", time.time())
""",
        )
        assert lines(out, "READ") == ["[('p1', 5.0), ('p2', 5.0)]"], out.stderr[-2000:]
        # 128 MiB at 25 MB/s: several probe periods with every reply late.
        took = float(lines(out, "TOOK")[0])
        assert took > 3, f"the transfer must outlast a probe and its relays ({took:.1f} s)"
        assert "holding probe replies" in c.log(0), (
            "the holder never held a probe reply: CHIMAERA_TEST_SLOW_PINGS_MS works only in a debug build of the peer"
        )
        done = float(lines(out, "DONE")[0])
        for i in (1, 2):
            early = [ln for ln in c.log(i).splitlines() if "peer is suspect" in ln and _at(ln) < done]
            assert not early, f"p{i} suspected a peer while p0 was sending to it: {early}"
    finally:
        c.close()
