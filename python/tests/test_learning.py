"""Learned reservations: memory after a few calls, a quarter core for calls
that mostly wait, and both carried over a peer restart. Each test runs its own
peer, so nothing learned elsewhere leaks in."""

from __future__ import annotations

import os
import json
import re
import subprocess
import tempfile
import time

import pytest

from chimaera._native import socket_path
from conftest import BIN, run_script


@pytest.fixture
def home():
    d = tempfile.mkdtemp(prefix="chl", dir="/tmp")
    yield d
    subprocess.run([BIN, "--dir", d, "stop"], capture_output=True, timeout=60)


ALLOCATE = """
import numpy as np
import chimaera as ch

@ch.remote
def allocate(i):
    a = np.ones(200 * 2**20 // 8)
    return float(a[-1])
"""


def _reserved_mib(reservation: str) -> int:
    m = re.search(r"memory (\d+) MiB", reservation)
    assert m, reservation
    return int(m.group(1))


def test_memory_is_learned_after_three_calls(home):
    r = run_script(
        ALLOCATE
        + """
first = allocate.remote(0)
first.result(timeout=60)
for i in range(1, 3):
    allocate.remote(i).result(timeout=60)
fourth = allocate.remote(3)
fourth.result(timeout=60)
print(first.explain()["attempts"][-1]["reservation"])
print(fourth.explain()["attempts"][-1]["reservation"])
""",
        home=home,
    )
    assert r.returncode == 0, r.stderr
    before, after = r.stdout.splitlines()[:2]
    assert "not known yet" in before, before
    assert "learned" in after and _reserved_mib(after) >= 200, after


def test_calls_that_mostly_wait_move_to_a_quarter_core(home):
    r = run_script(
        """
        import time
        import chimaera as ch

        @ch.remote
        def wait(i):
            time.sleep(0.2)
            return i

        assert list(wait.map(range(600))) == list(range(600))
        last = wait.remote(0)
        last.result(timeout=60)
        print(last.explain()["attempts"][-1]["reservation"])
        print(ch.runs.current_run().id)
        """,
        home=home,
        timeout=300,
    )
    assert r.returncode == 0, r.stderr
    reservation, run_id = r.stdout.splitlines()[:2]
    decisions = subprocess.run(
        [BIN, "--dir", home, "explain", run_id, "--decisions"], capture_output=True, text=True, timeout=60
    ).stdout
    assert "cpus 0.25" in reservation, f"{reservation}\n{decisions}"
    assert "cpu-sizing" in decisions and "adopted" in decisions, decisions


def test_what_was_learned_survives_a_peer_restart(home):
    learn = run_script(ALLOCATE + "\nfor i in range(4):\n    allocate.remote(i).result(timeout=60)\n", home=home)
    assert learn.returncode == 0, learn.stderr
    stopped = subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, text=True, timeout=60)
    assert stopped.returncode == 0, stopped.stderr
    again = run_script(
        ALLOCATE
        + """
f = allocate.remote(0)
f.result(timeout=60)
print(f.explain()["attempts"][-1]["reservation"])
""",
        home=home,
    )
    assert again.returncode == 0, again.stderr
    assert "learned" in again.stdout and _reserved_mib(again.stdout) >= 200, again.stdout


HOLD = """
import time
import numpy as np
import chimaera as ch

@ch.remote
def hold(i):
    a = np.ones(200 * 2**20 // 8)
    time.sleep(0.3)  # long enough for the 100 ms memory samples, on any host
    return float(a[-1])
"""


def test_chimaera_profiles_lists_what_was_learned(home):
    learn = run_script(HOLD + "\nfor i in range(4):\n    hold.remote(i).result(timeout=60)\n", home=home)
    assert learn.returncode == 0, learn.stderr
    subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, text=True, timeout=60)
    deadline = time.monotonic() + 15  # a stopping peer saves its profiles first
    while not os.path.exists(os.path.join(home, "profiles.pb")) and time.monotonic() < deadline:
        time.sleep(0.1)
    listed = subprocess.run([BIN, "--dir", home, "profiles", "--fn", "hold", "--json"], capture_output=True, text=True, timeout=60)
    assert listed.returncode == 0, listed.stderr
    [row] = json.loads(listed.stdout)
    assert row["name"].endswith("hold") and row["calls"] == 4, row
    assert (row["memory_bytes"] or 0) >= 200 << 20 and "p95 peak" in (row["memory_evidence"] or ""), row
    text = subprocess.run([BIN, "--dir", home, "profiles"], capture_output=True, text=True, timeout=60).stdout
    assert "hold: 4 calls" in text and "next call gets" in text, text


def test_an_edited_module_is_used_by_the_next_run(home, tmp_path):
    mod = tmp_path / "helper.py"
    script = """
import sys
sys.path.insert(0, {dir!r})
import helper
import chimaera as ch
f = ch.remote(helper.version)
print(sorted({{f.remote().result(timeout=60) for _ in range(8)}}))
""".format(dir=str(tmp_path))
    mod.write_text("def version():\n    return 1\n")
    first = run_script(script, home=home)
    assert first.returncode == 0, first.stderr
    assert first.stdout.strip() == "[1]"
    # A different size: Python's bytecode cache trusts (mtime seconds, size),
    # so an edit within the same second and of the same size is not seen even
    # by a fresh process. Real edits are seconds apart.
    mod.write_text("def version():\n    return 22\n")
    second = run_script(script, home=home)
    assert second.returncode == 0, second.stderr
    assert second.stdout.strip() == "[22]", "a warm worker kept the old module"


def test_auto_bounds_what_is_learned(home):
    r = run_script(
        """
import numpy as np
import chimaera as ch

@ch.remote(memory=ch.auto(max="64 MiB"))
def allocate(i):
    a = np.ones(200 * 2**20 // 8)
    return float(a[-1])

for i in range(3):
    allocate.remote(i).result(timeout=60)
f = allocate.remote(3)
f.result(timeout=60)
print(f.explain()["attempts"][-1]["reservation"])
""",
        home=home,
    )
    assert r.returncode == 0, r.stderr
    assert _reserved_mib(r.stdout) == 64, r.stdout


def test_learned_cpu_never_makes_a_call_infeasible(home):
    # A call of microseconds spends most of its CPU outside the function
    # (decoding, reporting); learning must not turn that into a grant larger
    # than the peer, which would make later calls infeasible.
    subprocess.Popen(
        [BIN, "--dir", home, "peer", "start", "--cpus", "4", "--idle-timeout", "60"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    sock = socket_path(home, "peer.sock")
    deadline = time.monotonic() + 30
    while not os.path.exists(sock) and time.monotonic() < deadline:
        time.sleep(0.05)
    script = """
import chimaera as ch

@ch.remote
def add(a, b):
    return a + b

print(sum(ch.get((add.remote(i, 1) for i in range(50)), timeout=120)))
"""
    for _ in range(2):
        r = run_script(script, home=home)
        assert r.returncode == 0, r.stderr
        assert r.stdout.split() == [str(sum(range(50)) + 50)]


def test_calls_queued_at_an_actor_whose_method_waits_suggest_concurrency(home):
    r = run_script(
        """
        import time
        import chimaera as ch

        @ch.remote
        class Fetcher:
            def fetch(self, i):
                time.sleep(0.1)  # stands in for a network call
                return i

        f = Fetcher.remote()
        ch.get([f.fetch.remote(i) for i in range(12)])  # enough calls to learn they mostly wait
        queued = [f.fetch.remote(i) for i in range(30)]
        time.sleep(0.5)
        print(queued[-1].explain()["pending_reason"])
        ch.get(queued, timeout=60)
        """,
        home=home,
        timeout=120,
    )
    assert r.returncode == 0, r.stderr
    assert "queued at its actor" in r.stdout and "@ch.method(concurrency=N) on fetch" in r.stdout, r.stdout
