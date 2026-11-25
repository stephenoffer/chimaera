"""A peer placing a burst asks every live member for its load each period
(loads.rs), instead of placing from SWIM replies up to n/8 periods old, blind
to the other leaders' grants. With more peers than SWIM probes per period, a
burst spread over several group leaders must show some peer asking all the
others. (Its effect, measured on 16 local peers: 6-30 calls per peer became
12-21, and no-op throughput roughly doubled; one run's spread is too noisy a
gate.)"""

import glob
import os
import re

from test_cluster import Cluster, lines


def test_a_peer_placing_a_burst_asks_every_member_for_its_load():
    log = {"CHIMAERA_LOG": "info,chimaera_peer::loads=debug"}
    c = Cluster(16, cpus={i: 2 for i in range(16)}, env={i: log for i in range(16)})
    try:
        out = c.run(
            0,
            """
import os, time
os.environ["CHIMAERA_SPREAD_RATE"] = "1"  # every group leader places part of the burst
import chimaera as ch

@ch.remote(cpus=1)
def nap(s):
    time.sleep(s)

time.sleep(5)  # membership settles
ch.get([nap.remote(0.2) for _ in range(128)], timeout=120)
print("DONE")
""",
        )
        assert lines(out, "DONE") == [""], out.stderr[-2000:]
        logs = [open(f, errors="replace").read() for f in glob.glob(os.path.join(c.root, "p*.log"))]
        asked = [int(n) for text in logs for n in re.findall(r"loads polled asked=(\d+)", text)]
        most = max(asked, default=0)
        assert most == 15, f"rounds of asking: {len(asked)}, the most members asked in one: {most}"
    finally:
        c.close()
