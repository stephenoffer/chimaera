"""Tenant CPU quotas on a peer (CHIMAERA_TENANT_QUOTAS) and ch.init(tenant=)."""

from __future__ import annotations

import tempfile

from conftest import run_script

SCRIPT = """
import time, chimaera as ch
ch.init(tenant=TENANT)

@ch.remote(cpus=1)
def hold(i):
    t = time.time()
    time.sleep(0.4)
    return (t, time.time())

spans = ch.get([hold.remote(i) for i in range(4)], timeout=60)
overlap = any(a[0] < b[1] - 0.05 and b[0] < a[1] - 0.05 for i, a in enumerate(spans) for b in spans[i + 1:])
print("OVERLAP", overlap)
if TENANT == "capped":
    @ch.remote(cpus=2)
    def big():
        return 1
    try:
        big.remote().result(timeout=30)
        print("BIG ran")
    except ch.Infeasible as e:
        print("INFEASIBLE", "quota of 1" in str(e))

    @ch.remote(cpus=1)
    def parent():
        return hold.remote(0).result(timeout=30)  # waits within the same 1-CPU quota
    print("NESTED", parent.remote().result(timeout=60) is not None)
"""


def test_a_capped_tenant_runs_within_its_quota_and_others_do_not_wait():
    home = tempfile.mkdtemp(prefix="chq", dir="/tmp")
    env = {"CHIMAERA_TENANT_QUOTAS": "capped=1", "CHIMAERA_HOME": home}
    capped = run_script(SCRIPT.replace("TENANT", '"capped"'), env=env, home=home)
    assert capped.returncode == 0, capped.stderr[-3000:]
    assert "OVERLAP False" in capped.stdout, capped.stdout  # one 1-CPU call at a time
    assert "INFEASIBLE True" in capped.stdout and "NESTED True" in capped.stdout, capped.stdout
    free = run_script(SCRIPT.replace("TENANT", '"free"'), env=env, home=home)
    assert free.returncode == 0, free.stderr[-3000:]
    assert "OVERLAP True" in free.stdout, free.stdout  # no quota: the four run together


def test_a_malformed_quota_setting_stops_the_peer():
    home = tempfile.mkdtemp(prefix="chq", dir="/tmp")
    bad = run_script(
        "import chimaera as ch\nch.init()\n",
        env={"CHIMAERA_TENANT_QUOTAS": "capped", "CHIMAERA_HOME": home},
        home=home,
        timeout=60,
    )
    assert bad.returncode != 0 and "did not start" in bad.stderr, bad.stderr[-2000:]
    import os

    with open(os.path.join(home, "peer.log")) as f:
        assert "is not tenant=cpus" in f.read()  # the peer says why it refused to start


SHARE_SCRIPT = """
import time, chimaera as ch
ch.init(tenant=TENANT)

@ch.remote(cpus=1)
def hold(i):
    t = time.time()
    time.sleep(1.0)
    return t, time.time(), ch.context().node

start = time.time()
spans = ch.get([hold.remote(i) for i in range(12)], timeout=120)
edges = sorted([(a, 1) for a, _, _ in spans] + [(b, -1) for _, b, _ in spans], key=lambda e: (e[0], e[1]))
running = peak = 0
for _, step in edges:
    running += step
    peak = max(peak, running)
print("RESULT", round(time.time() - start, 2), peak, len({n for _, _, n in spans}))
"""


def test_a_cluster_share_caps_a_tenant_across_peers(monkeypatch):
    from test_cluster import Cluster

    monkeypatch.setenv("CHIMAERA_TENANT_SHARES", "ml=2")
    cluster = Cluster(2)  # 4 CPUs each: 8 in all
    try:

        def run(tenant):
            out = cluster.run(0, SHARE_SCRIPT.replace("TENANT", repr(tenant)))
            assert out.returncode == 0, out.stderr[-2000:]
            seconds, peak, _ = next(ln for ln in out.stdout.splitlines() if ln.startswith("RESULT")).split()[1:]
            return float(seconds), int(peak)

        seconds, peak = run("ml")
        # 12 one-second calls, 2 at a time across the cluster: about 6 s. A per-peer
        # cap of 2 would allow 4 at once (about 3 s). Peers admitting before they
        # hear each other's report may overshoot briefly, never for the whole run.
        assert seconds >= 4.5, (seconds, peak)
        open_seconds, open_peak = run("web")  # no share: uses the whole cluster
        assert open_seconds < 3.5 and open_peak > 2, (open_seconds, open_peak)
    finally:
        cluster.close()
