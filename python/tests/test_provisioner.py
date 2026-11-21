"""The provisioner's scaling decisions (the kind gate runs it against a real StatefulSet)."""

from __future__ import annotations

import os

import pytest

import chimaera as ch
from chimaera._cluster import Node
from chimaera.provisioner import Memory, Rules, decide


def node(cpus=4.0, used=0.0, queued=0, alive=True, address="a:1"):
    return Node("p", address, cpus, 1 << 30, [], alive, used, queued, False, {})


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
    assert decide(quiet, 3, rules, m, now=101)[0] == 2  # the pending pod goes first
    assert decide(quiet, 2, rules, m, now=161)[0] == 2  # the quiet clock restarts after a step
    assert decide(quiet, 2, rules, m, now=221)[0] == 1


def test_a_quiet_pool_shrinks_one_step_per_quiet_period_and_never_below_min():
    m = Memory()
    quiet = [node(used=0.5), node(), node()]
    assert decide(quiet, 3, RULES, m, now=0)[0] == 3
    assert decide(quiet, 3, RULES, m, now=60) == (2, "shrink: 0.5 of 12 CPUs in use for 60 s")
    assert decide(quiet[:2], 2, RULES, m, now=61)[0] == 2, "the next step waits a full period"
    assert decide(quiet[:2], 2, RULES, m, now=121)[0] == 1
    assert decide(quiet[:1], 1, RULES, m, now=999)[0] == 1


def test_work_that_needs_every_peer_keeps_them():
    m = Memory()
    assert decide([node(used=3), node(used=3)], 2, RULES, m, now=0) == (2, "steady")
    assert decide([node(used=3), node(used=3)], 2, RULES, m, now=999) == (2, "steady")


def test_in_cluster_it_calls_the_api_server_with_the_pods_service_account(tmp_path, monkeypatch):
    import http.server
    import json
    import ssl
    import subprocess
    import threading

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
    state = {"replicas": 2}

    class Api(http.server.BaseHTTPRequestHandler):
        def reply(self):
            body = json.dumps({"spec": {"replicas": state["replicas"]}}).encode()
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
            state["replicas"] = patch["spec"]["replicas"]
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
    finally:
        server.shutdown()
    path = "/apis/apps/v1/namespaces/ml/statefulsets/chimaera/scale"
    assert [s[:3] for s in seen] == [("GET", path, "Bearer sa-token"), ("PATCH", path, "Bearer sa-token"), ("GET", path, "Bearer sa-token")]
    assert seen[1][3] == ("application/merge-patch+json", {"spec": {"replicas": 5}})


def test_skypilot_spares_launch_join_and_drain_before_teardown(tmp_path, monkeypatch):
    from chimaera.provisioner import SkySpares

    log = tmp_path / "calls"
    for tool in ("sky", "ssh"):  # stand-ins that record their arguments, one line per call
        path = tmp_path / tool
        path.write_text(
            f'#!/bin/sh\nprintf "{tool}" >> {log}\nfor a in "$@"; do printf " [%s]" "$a" >> {log}; done\necho >> {log}\n'
        )
        path.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("CHIMAERA_CLUSTER_TOKEN", "t")
    ch.Store("provisioner/sky").delete("spares")
    peers = [node(2, 0, address="10.0.0.6:7700"), node(2, 0, address="10.0.0.5:7700")]
    monkeypatch.setattr(ch, "cluster", lambda: type("View", (), {"nodes": lambda self: peers})())

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
        "sky [launch] [-c] [chimaera-spare-1] [-y] [-d] [--env] [CHIMAERA_CLUSTER_TOKEN] [--infra] [aws] [--use-spot] [chimaera peer start"
    )
    assert "--listen 0.0.0.0:7700" in first and "--seeds 10.0.0.5:7700,10.0.0.6:7700]" in first
    assert "chimaera-spare-2" in launches[1]
    # A restarted provisioner knows the spares; shrinking drains the newest, then tears it down.
    again = SkySpares(1, [], drain_s=0)
    assert again.replicas() == 3
    log.unlink()
    again.scale(2)
    assert calls(again) == [
        "ssh [chimaera-spare-2] [pkill] [-USR1] [-x] [chimaera]",
        "sky [down] [-y] [chimaera-spare-2]",
    ]
    assert again.replicas() == 2
    again.scale(1)
    calls(again)
    assert again.replicas() == 1
    peers[:] = [node(2, 0, address="")]  # a local peer: nothing for a spare to join
    with pytest.raises(RuntimeError, match="no live peer has a network address"):
        again.scale(2)
