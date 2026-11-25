"""The provisioner's scaling decisions (the kind gate runs it against a real StatefulSet)."""

from __future__ import annotations

import getpass
import os
import subprocess

import pytest

import chimaera as ch
from chimaera._cluster import Node
from chimaera.provisioner import Memory, Rules, StatefulSet, Victim, decide
from conftest import BIN


def node(cpus=4.0, used=0.0, queued=0, alive=True, address="a:1", labels=None, peer="p", actors=0):
    return Node(peer, address, cpus, 1 << 30, [], alive, used, queued, False, labels or {}, actors=actors)


def joined(n):
    return Victim("joined", n.address, node=n)


PENDING = Victim("never_joined", "chimaera-2", "it is Pending and none of its containers has started")


RULES = Rules(min=1, max=5, up_after=10, down_after=60)


def test_a_backlog_grows_the_pool_only_after_it_lasts():
    m = Memory()
    busy = [node(used=4, queued=10)]
    assert decide(busy, 1, RULES, m, now=0)[0] == 1
    assert decide(busy, 1, RULES, m, now=5)[0] == 1
    want, why = decide(busy, 1, RULES, m, now=10)
    assert want == 4 and why.startswith("grow")  # 10 queued calls / 4 CPUs per peer -> 3 more
    assert decide([node(used=4, queued=40)] * 4, 4, RULES, Memory(busy_since=0), now=99)[0] == 5  # capped at max


def test_a_short_burst_does_not_grow_it():
    m = Memory()
    assert decide([node(used=4, queued=3)], 1, RULES, m, now=0)[0] == 1
    assert decide([node(used=1, queued=0)], 1, RULES, m, now=5)[0] == 1
    assert decide([node(used=4, queued=3)], 1, RULES, m, now=12)[0] == 1, "the clock restarted"


def test_it_holds_while_new_peers_start():
    want, why = decide([node(queued=50)], 3, RULES, Memory(busy_since=0), now=99)
    assert want == 3 and why.startswith("waiting")


def test_peers_that_cannot_start_stop_blocking_and_are_shrunk_away():
    rules = Rules(min=1, max=5, up_after=10, down_after=60, start_grace=30)
    m = Memory()
    busy = [node(used=4, queued=20)]
    assert decide(busy, 3, rules, m, now=0)[1].startswith("waiting")
    want, why = decide(busy, 3, rules, m, now=31)
    assert want == 3 and "not growing" in why  # more replicas would not start either
    quiet = [node(used=0)]
    assert decide(quiet, 3, rules, m, now=40)[0] == 3
    assert decide(quiet, 3, rules, m, now=101, victim=PENDING)[0] == 2  # the pending pod goes first
    assert decide(quiet, 2, rules, m, now=161)[0] == 2  # the quiet clock restarts after a step
    assert decide(quiet, 2, rules, m, now=221, victim=PENDING._replace(target="chimaera-1"))[0] == 1


def test_a_quiet_pool_shrinks_one_step_per_quiet_period_and_never_below_min():
    m = Memory()
    quiet = [node(used=0.5), node(), node()]
    assert decide(quiet, 3, RULES, m, now=0)[0] == 3
    assert decide(quiet, 3, RULES, m, now=60, victim=joined(quiet[2])) == (2, "shrink: 0.5 of 12 CPUs in use for 60 s")
    assert decide(quiet[:2], 2, RULES, m, now=61)[0] == 2, "the next step waits a full period"
    assert decide(quiet[:2], 2, RULES, m, now=121, victim=joined(quiet[1]))[0] == 1
    assert decide(quiet[:1], 1, RULES, m, now=999)[0] == 1


def test_work_that_needs_every_peer_keeps_them():
    m = Memory()
    assert decide([node(used=3), node(used=3)], 2, RULES, m, now=0) == (2, "steady")
    assert decide([node(used=3), node(used=3)], 2, RULES, m, now=999) == (2, "steady")


def test_it_shrinks_only_once_the_peer_that_would_go_holds_nothing():
    m = Memory()
    keep = node(used=1)
    last = Node("p", "chimaera-2.peers.ml.svc:7700", 4.0, 1 << 30, [], True, 0.0, 0, False, {}, actors=1, object_bytes=5)
    quiet = [keep, node(), last]
    assert decide(quiet, 3, RULES, m, now=0, victim=joined(last))[0] == 3
    want, why = decide(quiet, 3, RULES, m, now=60, victim=joined(last))
    assert want == 3 and why.endswith("holds 1 actors, 5 bytes of stored values"), why
    emptied = last._replace(actors=0, object_bytes=0)
    assert decide([keep, node(), emptied], 3, RULES, m, now=61, victim=joined(emptied))[0] == 2, "quiet long enough already"


def test_a_statefulsets_victim_is_its_highest_pod():
    pods = [node(address=f"chimaera-{i}.peers.ml.svc:7700") for i in range(3)]
    sts = StatefulSet("chimaera", "ml", sa_dir="/nonexistent")
    assert sts.victim(pods, 3).node is pods[2] and sts.victim(pods, 2).node is pods[1]
    sts.pod = lambda name: None
    assert sts.victim(pods[:2], 3) == Victim("never_joined", "chimaera-2", "there is no such pod"), "nothing to lose"


def test_in_cluster_it_calls_the_api_server_with_the_pods_service_account(tmp_path, monkeypatch):
    import http.server
    import json
    import ssl
    import subprocess
    import threading
    import urllib.error

    from chimaera.provisioner import StatefulSet

    cert, key = tmp_path / "ca.crt", tmp_path / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
         "-addext", "subjectAltName=DNS:localhost", "-keyout", str(key), "-out", str(cert)],
        check=True, capture_output=True,
    )
    (tmp_path / "token").write_text("sa-token\n")
    (tmp_path / "namespace").write_text("ml\n")
    seen = []
    state = {"replicas": 2, "version": 1}

    class Api(http.server.BaseHTTPRequestHandler):
        def reply(self):
            body = json.dumps(
                {"metadata": {"resourceVersion": str(state["version"])}, "spec": {"replicas": state["replicas"]}}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            seen.append(("GET", self.path, self.headers["Authorization"], None))
            self.reply()

        def do_PATCH(self):
            patch = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(("PATCH", self.path, self.headers["Authorization"], (self.headers["Content-Type"], patch)))
            test, replace = patch  # JSON Patch: test the size read, then replace it
            if test != {"op": "test", "path": "/spec/replicas", "value": state["replicas"]}:
                self.send_response(422)  # as the API server answers a failed test
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            state["replicas"], state["version"] = replace["value"], state["version"] + 1
            self.reply()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("localhost", 0), Api)
    tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls.load_cert_chain(cert, key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "localhost")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", str(server.server_address[1]))
    try:
        sts = StatefulSet("chimaera", sa_dir=str(tmp_path))
        assert sts.in_cluster
        assert sts.replicas() == 2
        sts.scale(5)
        assert sts.replicas() == 5
        state["version"] += 1  # the controller updates the status: the size is unchanged
        sts.scale(6)
        assert state["replicas"] == 6, "a status update does not block a change"
        state["replicas"] = 7  # another writer changes the size after this look
        with pytest.raises(urllib.error.HTTPError, match="422"):
            sts.scale(8)
        assert state["replicas"] == 7, "a change on top of a stale size never lands"
    finally:
        server.shutdown()
    path = "/apis/apps/v1/namespaces/ml/statefulsets/chimaera/scale"
    assert [s[:3] for s in seen[:3]] == [("GET", path, "Bearer sa-token"), ("PATCH", path, "Bearer sa-token"), ("GET", path, "Bearer sa-token")]
    assert seen[1][3] == (
        "application/json-patch+json",
        [{"op": "test", "path": "/spec/replicas", "value": 2}, {"op": "replace", "path": "/spec/replicas", "value": 5}],
    )


def test_skypilot_spares_launch_join_and_drain_before_teardown(tmp_path, monkeypatch):
    from chimaera.provisioner import SkySpares

    log = tmp_path / "calls"
    for tool in ("sky", "ssh"):  # stand-ins that record their arguments, one line per call
        path = tmp_path / tool
        # One write per line: the two launches run at once, and piecemeal
        # appends would interleave their lines.
        path.write_text(
            f'#!/bin/sh\nline="{tool}"\nfor a in "$@"; do line="$line [$a]"; done\nprintf "%s\\n" "$line" >> {log}\n'
        )
        path.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("CHIMAERA_CLUSTER_TOKEN", "t")
    ch.coord.Store("provisioner/sky").delete("spares")
    peers = [node(2, 0, address="10.0.0.6:7700"), node(2, 0, address="10.0.0.5:7700")]
    monkeypatch.setattr(ch.runs, "cluster", lambda: type("View", (), {"nodes": lambda self: peers})())

    def calls(pool):
        for c in pool._children:
            c.wait(timeout=30)
        return log.read_text().splitlines() if log.exists() else []

    pool = SkySpares(1, ["--infra", "aws", "--use-spot"], drain_s=0)
    pool.scale(3)
    launches = sorted(calls(pool))  # the two launches run at once, in either order
    assert len(launches) == 2 and pool.replicas() == 3
    first = launches[0]
    assert first.startswith(
        "sky [launch] [-c] [chimaera-spare-1] [-y] [-d] [--secret] [CHIMAERA_CLUSTER_TOKEN] [--infra] [aws] [--use-spot] [sudo -n mkdir"
    )
    # A huge-page tmpfs for its objects first (best effort), then its peer.
    assert "huge=within_size" in first and first.index("huge=within_size") < first.index("chimaera peer start")
    assert "--listen 0.0.0.0:7700" in first and "--seeds 10.0.0.5:7700,10.0.0.6:7700]" in first
    assert "chimaera.spare=chimaera-spare-1" in first, "its peer says which spare it is"
    assert "chimaera-spare-2" in launches[1]
    # A restarted provisioner knows the spares; shrinking drains the newest, then tears it down.
    again = SkySpares(1, [], drain_s=0)
    assert again.replicas() == 3
    log.unlink()
    peers.append(node(2, 0, address="10.0.0.9:7700", labels={"chimaera.spare": "chimaera-spare-2"}, peer="s2"))
    assert again.shrink(again.victim(peers, 3)) is None
    assert calls(again) == [
        "ssh [chimaera-spare-2] [pkill] [-USR1] [-x] [chimaera]",
        "sky [down] [-y] [chimaera-spare-2]",
    ]
    assert again.replicas() == 2
    log.unlink()
    # Spare 1 never joined and SkyPilot shows no such cluster: it goes without a drain, and is still torn down.
    gone = again.victim(peers, 2)
    assert gone.state == "never_joined", gone
    assert again.shrink(gone) is None
    assert calls(again) == ["sky [status] [chimaera-spare-1]", "sky [status] [chimaera-spare-1]", "sky [down] [-y] [chimaera-spare-1]"]
    assert again.replicas() == 1
    peers[:] = [node(2, 0, address="")]  # a local peer: nothing for a spare to join
    with pytest.raises(RuntimeError, match="no live peer has a network address"):
        again.scale(2)


def test_sky_spares_get_certificates_of_their_own_and_never_the_ca_key(tmp_path, monkeypatch):
    from chimaera.provisioner import SkySpares

    log = tmp_path / "calls"
    sky = tmp_path / "sky"
    sky.write_text(f'#!/bin/sh\nfor a in "$@"; do printf "[%s]" "$a" >> {log}; done\necho >> {log}\n')
    sky.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("CHIMAERA_CLUSTER_TOKEN", "t")
    ca = tmp_path / "ca"
    subprocess.run([BIN, "cluster", "init", "--out", str(ca)], check=True, capture_output=True)
    monkeypatch.setenv("CHIMAERA_TLS_CA", str(ca))
    monkeypatch.setenv("CHIMAERA_BIN", BIN)
    ch.coord.Store("provisioner/sky").delete("spares")
    peers = [node(2, 0, address="10.0.0.5:7700")]
    monkeypatch.setattr(ch.runs, "cluster", lambda: type("View", (), {"nodes": lambda self: peers})())
    pool = SkySpares(1, ["--infra", "aws"], drain_s=0)
    pool.scale(2)
    for c in pool._children:
        c.wait(timeout=30)
    (launch,) = log.read_text().splitlines()
    assert launch.startswith("[launch][-c][chimaera-spare-1][-y][-d][--secret][CHIMAERA_CLUSTER_TOKEN][--infra][aws][")
    task = launch.rsplit("[", 1)[1].rstrip("]")
    text = open(task).read()
    assert "--tls-dir ~/.chimaera/spare-tls" in text and "chmod 600 ~/.chimaera/spare-tls/peer.key" in text
    uploaded = [line.split(": ", 1)[1] for line in text.splitlines() if line.startswith("  ~/.chimaera/spare-tls/")]
    assert sorted(os.path.basename(f) for f in uploaded) == ["ca.pem", "peer.key", "peer.pem"]
    assert not any("ca.key" in line for line in text.splitlines()), "the CA's key stays here"
    # The spare's certificate is signed by the cluster CA.
    certs = os.path.dirname(uploaded[0])
    assert open(os.path.join(certs, "ca.pem")).read() == (ca / "ca.pem").read_text()
    verify = subprocess.run(["openssl", "verify", "-CAfile", str(ca / "ca.pem"), os.path.join(certs, "peer.pem")],
                            capture_output=True, text=True)
    assert verify.returncode == 0, verify.stdout + verify.stderr


QUIET = [node(used=0.5), node()]  # with a third peer: CPU in use fits on one peer fewer


def quiet_for_long(victim):
    """What `decide` asks for once a three-peer pool has been quiet long enough, given its victim."""
    m = Memory()
    decide([*QUIET, node()], 3, RULES, m, now=0, victim=victim)
    return decide([*QUIET, node()], 3, RULES, m, now=61, victim=victim)


def test_without_a_named_victim_nothing_shrinks():
    want, why = quiet_for_long(Victim("unknown", "chimaera-2", "it is Running but no peer from it has joined"))
    assert want == 3 and "cannot tell what removing chimaera-2 would lose: it is Running" in why
    assert decide([*QUIET, node()], 3, RULES, Memory(idle_since=0), now=61)[0] == 3, "a pool that names none holds"


def pod(phase="Pending", restarts=0, state=None, last=None, uid="u-1"):
    c = {"name": "peer", "restartCount": restarts, "state": state or {"waiting": {"reason": "ContainerCreating"}}}
    if last:
        c["lastState"] = last
    return {"metadata": {"uid": uid}, "status": {"phase": phase, "containerStatuses": [c]}}


def sts_with(pods):
    sts = StatefulSet("chimaera", "ml", sa_dir="/nonexistent")
    sts.pod = lambda name: pods.get(name)
    return sts


def test_a_pods_peer_is_found_by_its_hostname_whatever_address_it_advertises():
    holder = node(address="10.0.0.7:7700", labels={"host": "chimaera-2"}, actors=1)
    v = sts_with({}).victim([*QUIET, holder], 3)
    assert v.state == "joined" and v.node is holder
    want, why = quiet_for_long(v)
    assert want == 3 and why.endswith("10.0.0.7:7700, the peer a shrink removes, holds 1 actors"), why


@pytest.mark.parametrize(
    "members, the_pod, state",
    [
        ([], None, "never_joined"),  # no such pod: nothing on it
        ([], pod(), "never_joined"),  # Pending, its container never started
        ([], pod(state={"running": {}}), "unknown"),  # started, but no peer from it joined (yet)
        ([], pod(phase="Running", state={"running": {}}), "unknown"),
        ([], pod(restarts=1), "unknown"),  # Pending again after a start: its peer may have held work
        ([], pod(last={"terminated": {"exitCode": 1}}), "unknown"),
        ([node(alive=False, labels={"host": "chimaera-2"})], None, "unknown"),  # its peer stopped answering
        ([node(labels={"host": "chimaera-2"}), node(labels={"host": "chimaera-2"}, peer="q")], None, "unknown"),
    ],
)
def test_the_highest_pod_is_removable_only_when_shown_to_hold_nothing(members, the_pod, state):
    v = sts_with({"chimaera-2": the_pod} if the_pod else {}).victim([*QUIET, *members], 3)
    assert v.state == state, v
    assert (quiet_for_long(v)[0] == 2) == (state == "never_joined")


def test_a_shrink_checks_again_and_stops_if_the_pod_joined_meanwhile(monkeypatch):
    members = [*QUIET]
    sts = sts_with({"chimaera-2": pod()})
    scaled = []
    sts.replicas = lambda: 3
    sts.scale = scaled.append
    monkeypatch.setattr(ch.runs, "cluster", lambda: type("View", (), {"nodes": lambda self: members})())
    first = sts.victim(members, 3)
    assert first.state == "never_joined"
    members.append(node(labels={"host": "chimaera-2"}))  # its peer joins between the look and the change
    why = sts.shrink(first)
    assert why and "changed before the shrink" in why and scaled == []
    assert sts.shrink(sts.victim(members, 3)) is None and scaled == [2], "joined and idle: it goes"


class FakeSlurm:
    """squeue/sbatch/scancel over a table of jobs (id, user, name, state)."""

    def __init__(self, jobs):
        self.jobs, self.calls = jobs, []

    def __call__(self, *args):
        self.calls.append(args)
        if args[0] == "squeue":
            user, name = args[args.index("-u") + 1], args[args.index("-n") + 1]
            return "".join(f"{i} {st}\n" for i, u, n, st in self.jobs if u == user and n == name)
        if args[0] == "sbatch":
            self.jobs.append((str(100 + len(self.jobs)), getpass.getuser(), args[args.index("--job-name") + 1], "PENDING"))
            return self.jobs[-1][0]
        if args[0] == "scancel":
            self.jobs = [j for j in self.jobs if j[0] != args[1]]
        return ""


def slurm(monkeypatch, jobs, members):
    from chimaera.provisioner import SlurmSpares

    monkeypatch.setenv("CHIMAERA_CLUSTER_TOKEN", "t")
    monkeypatch.setenv("SLURM_JOB_ID", "7")
    monkeypatch.setattr(ch.runs, "cluster", lambda: type("View", (), {"nodes": lambda self: members})())
    fake = FakeSlurm(jobs)
    pool = SlurmSpares(1)
    monkeypatch.setattr(pool, "_run", fake)
    return pool, fake


def test_slurm_spares_are_this_clusters_own_and_a_shrink_cancels_exactly_its_victim(monkeypatch):
    me = getpass.getuser()
    other = [("40", me, "chimaera-spare-9", "RUNNING"), ("41", "bob", "chimaera-spare-7", "PENDING")]
    members = [node(address="n0:7700")]
    pool, fake = slurm(monkeypatch, [*other, ("50", me, "chimaera-spare-7", "RUNNING")], members)
    assert pool.replicas() == 2, "another cluster's spare (or another user's) is not this pool's"
    assert pool.victim(members, 2).state == "unknown", "running, but its peer has not joined"
    members.append(node(address="10.1.1.1:7700", labels={"slurm.job": "50"}, actors=2))
    v = pool.victim(members, 2)
    assert v.state == "joined" and blocked_reason(v).endswith("holds 2 actors")
    assert pool.shrink(v) and not any(c[0] == "scancel" for c in fake.calls)
    members[-1] = members[-1]._replace(actors=0)
    assert pool.shrink(pool.victim(members, 2)) is None
    assert [c for c in fake.calls if c[0] == "scancel"] == [("scancel", "50")]
    assert [j[0] for j in fake.jobs] == ["40", "41"], "the other clusters' jobs are untouched"
    pool.scale(2)
    (job,) = [j for j in fake.jobs if j[1:3] == (me, "chimaera-spare-7")]
    assert pool.victim(members, 2) == Victim("never_joined", job[0], "the job is pending: it has not started", uid=job[0])
    submit = next(c for c in fake.calls if c[0] == "sbatch")
    assert 'slurm.job=$SLURM_JOB_ID"' in submit[-1], "its peer carries its job id"


def blocked_reason(v):
    from chimaera.provisioner import blocked

    return blocked(v)


def test_a_step_names_its_victim_once_and_says_once_why_it_holds(monkeypatch):
    from chimaera.provisioner import step

    members = [*QUIET, node(labels={"host": "chimaera-2"}, actors=1)]
    monkeypatch.setattr(ch.runs, "cluster", lambda: type("View", (), {"nodes": lambda self: members})())
    asked = []

    class Pool:
        def replicas(self):
            return 3

        def victim(self, nodes, replicas):
            asked.append(replicas)
            return Victim("joined", "chimaera-2", node=nodes[2])

        def shrink(self, v):
            raise AssertionError("a peer holding an actor is never removed")

    m = Memory()
    assert step(Pool(), RULES, m, now=0) is None and asked == [], "no victim is looked up before a shrink is due"
    held = step(Pool(), RULES, m, now=61)
    assert held["holding"] == 3 and held["why"].endswith("holds 1 actors") and held["peers"] == {"asked": 3, "up": 3, "usable": 3}
    assert step(Pool(), RULES, m, now=66) is None, "the same reason is printed once"
    assert asked == [3, 3]
