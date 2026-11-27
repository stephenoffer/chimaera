"""A burst of fast calls is shared by the executors that free up, not taken
whole by the first one free.

Fast calls of one function go to an executor in batches, run one after
another. A batch was an even share of the queue for each CPU free at that
moment: when one executor freed up while the others were busy, it took the
whole queue (up to the learned batch), and the CPUs that freed up a moment
later sat idle while it worked through it."""

from __future__ import annotations

from test_cluster import Cluster, lines

BURST = """
import os, time
import chimaera as ch

@ch.remote
def quick(s):
    if s:
        time.sleep(s)
    return os.getpid()

@ch.remote
def hold(path, s):
    open(path, "w").close()
    time.sleep(s)
    return os.getpid()

# Learn that quick is fast, so its calls go to executors in deep batches.
for _ in range(20):
    ch.get([quick.remote(0) for _ in range(200)])
marks = [os.path.join(os.environ["CHIMAERA_HOME"], f"hold{i}") for i in range(3)]
held = [hold.remote(m, 0.5) for m in marks]
deadline = time.time() + 60
while not all(os.path.exists(m) for m in marks):  # three of the four CPUs are taken
    assert time.time() < deadline, "the holding calls never started"
    time.sleep(0.01)
t0 = time.time()
pids = ch.get([quick.remote(0.02) for _ in range(100)], timeout=120)
print("TOOK", time.time() - t0)
print("PIDS", len(set(pids)))
ch.get(held, timeout=60)
"""


def test_a_burst_of_fast_calls_is_shared_by_the_executors_that_free_up():
    c = Cluster(1, cpus={0: 4})
    try:
        out = c.run(0, BURST)
        assert out.returncode == 0, out.stderr[-2000:]
        # 100 calls of 20 ms: one executor alone takes 2 s; three more free up
        # after 0.5 s and share what is left.
        pids, took = int(lines(out, "PIDS")[0]), float(lines(out, "TOOK")[0])
        assert pids >= 3, f"the burst ran on {pids} executor(s) in {took:.2f} s"
        assert took < 1.5, f"the burst took {took:.2f} s"
    finally:
        c.close()
