"""Operations helpers: a function on every peer, and cancelling runs in bulk."""

import json
import os
import subprocess
import sys
import tempfile

import pytest

import chimaera as ch
from conftest import BIN, HOME, REPO
from test_cluster import Cluster, lines


def test_on_every_peer_runs_once_on_each_and_says_where():
    where = ch.runs.on_every_peer(lambda: ch.context().node)
    peers = [n.peer for n in ch.runs.cluster().nodes() if n.alive]
    assert where == {p: p for p in peers}


def test_on_every_peer_pins_each_call_to_its_peer(monkeypatch):
    # On an idle cluster unpinned calls can land one per peer anyway, so the pin
    # itself is checked: every call requires the node it is keyed by.
    from chimaera._remote import RemoteFunction

    seen = []
    original = RemoteFunction.options

    def recording(self, **opts):
        seen.append(dict(opts["placement"].require) if "placement" in opts else None)
        return original(self, **opts)

    monkeypatch.setattr(RemoteFunction, "options", recording)
    where = ch.runs.on_every_peer(lambda: ch.context().node)
    assert seen == [{"node": p} for p in where], seen


def test_on_every_peer_raises_a_peers_error():
    def boom() -> None:
        raise ValueError("not here")

    with pytest.raises(ValueError, match="not here"):
        ch.runs.on_every_peer(boom)


def test_on_every_peer_reaches_every_peer_of_a_cluster():
    c = Cluster(3)
    try:
        out = c.run(
            0,
            """
import chimaera as ch
for _ in range(5):
    print("WHERE", sorted(ch.runs.on_every_peer(lambda: ch.context().node).items()))
print("PEERS", sorted(n.peer for n in ch.runs.cluster().nodes() if n.alive))
""",
        )
        peers = eval(lines(out, "PEERS")[0])
        assert len(peers) == 3, peers
        for row in lines(out, "WHERE"):
            assert dict(eval(row)) == {p: p for p in peers}, (row, peers)
    finally:
        c.close()


def _detached_run(tag: str) -> str:
    """Start a detached run from a script named after `tag`; return its id."""
    path = os.path.join(tempfile.mkdtemp(prefix="chb", dir="/tmp"), f"{tag}.py")
    with open(path, "w") as f:
        f.write(
            "import time, chimaera as ch\n"
            "@ch.remote\n"
            "def nap():\n"
            "    time.sleep(300)\n"
            "run = ch.init(detach=True)\n"
            "futs = [nap.remote() for _ in range(2)]\n"
            "ch._runtime.Runtime.get().client.flush()\n"
            "print('RUN', run, flush=True)\n"
        )
    env = dict(os.environ, CHIMAERA_HOME=HOME, CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
    out = subprocess.run([sys.executable, path], env=env, capture_output=True, text=True, timeout=120)
    return lines(out, "RUN")[0]


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([BIN, "--dir", HOME, *args], capture_output=True, text=True, timeout=120)


def _state(run: str) -> str:
    listed = _cli("runs", "--json", "--limit", "0")
    assert listed.returncode == 0, listed.stderr
    return {r["run"]: r["state"] for r in json.loads(listed.stdout)["runs"]}[run]


def test_cancel_by_match_lists_first_then_cancels_only_matching_runs():
    alpha, beta = _detached_run("batch_alpha_job"), _detached_run("batch_beta_job")
    try:
        listed = _cli("cancel", "--match", "batch_alpha_job")
        assert listed.returncode == 0 and f"would cancel {alpha}" in listed.stdout, listed.stdout + listed.stderr
        assert beta not in listed.stdout
        assert _state(alpha) == "active"
        done = _cli("cancel", "--match", "batch_alpha_job", "--yes")
        assert done.returncode == 0, done.stdout + done.stderr
        assert _state(alpha) == "cancelled" and _state(beta) == "active"
    finally:
        _cli("cancel", alpha, beta)


def test_cancel_takes_several_runs_and_needs_a_selection():
    a, b = _detached_run("batch_many_one"), _detached_run("batch_many_two")
    done = _cli("cancel", a, b)
    assert done.returncode == 0, done.stdout + done.stderr
    assert _state(a) == "cancelled" and _state(b) == "cancelled"
    none = _cli("cancel")
    assert none.returncode != 0 and "--match" in none.stderr
