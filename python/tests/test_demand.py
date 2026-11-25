"""What each peer reports as demand (Node.demand), summed over a cluster: only
work more peers would start sooner. A call waiting on another's result is
not queued anywhere yet, and an actor's calls wait in its mailbox; an actor
waiting for CPUs counts once."""

from __future__ import annotations

from test_cluster import Cluster, lines

DEMAND = """
import time
import chimaera as ch

@ch.remote(cpus=1)
def wait(s):
    time.sleep(s)
    return s

@ch.remote
def after(x):
    return x

@ch.remote(cpus=1)
class Serial:
    def step(self, i):
        time.sleep(0.05)
        return i

# Fill every CPU of both peers, then queue behind them.
while sum(n.alive for n in ch.runs.cluster().nodes()) < 2:
    time.sleep(0.1)
cores = int(sum(n.cpus for n in ch.runs.cluster().nodes() if n.alive))
busy = [wait.remote(60) for _ in range(cores)]
while sum(n.cpu_used for n in ch.runs.cluster().nodes()) < cores:
    time.sleep(0.1)
child = after.remote(busy[0])           # waits on its input: no demand
actor = Serial.remote()                 # waits for a CPU: one actor
steps = [actor.step.remote(i) for i in range(100)]  # in its mailbox: no demand
backlog = [wait.remote(1) for _ in range(40)]
deadline = time.monotonic() + 30
best = None
while time.monotonic() < deadline:
    nodes = ch.runs.cluster().nodes()
    demand = [n.demand for n in nodes if n.alive]
    queued = sum(s["queued"] for d in demand for s in d["shapes"])
    actors = sum(s["actors"] for d in demand for s in d["shapes"])
    held = sum(c for d in demand for _, c in d["releases"])
    best = (queued, actors, held)
    if (queued, actors) == (40, 1):
        break
    time.sleep(0.2)
print("DEMAND", *best, cores)
shape = next(s for d in demand for s in d["shapes"] if s["queued"])
print("SHAPE", shape["cpus"], shape["cold"] > 0, shape["wall_s"] >= shape["queued"])
print("FIELDS", all(isinstance(n.load_age_s, float) and n.incarnation > 0 for n in nodes))
"""


def test_only_work_more_peers_would_start_counts_as_demand():
    c = Cluster(2)
    try:
        out = c.run(0, DEMAND, timeout=240)
        queued, actors, held, cores = lines(out, "DEMAND")[0].split()
        assert (int(queued), int(actors)) == (40, 1), out.stdout
        assert float(held) >= int(cores), "the running calls hold every core"
        assert lines(out, "SHAPE")[0] == "1.0 True True", "a cold function's calls count at least a second each"
        assert lines(out, "FIELDS")[0] == "True"
    finally:
        c.close()
