"""Actors that ask for CPUs wait for free ones (as Ray's do) instead of over-committing."""

from __future__ import annotations

import tempfile

from conftest import run_script

SCRIPT = """
import time
import chimaera as ch

cores = int(ch._runtime.Runtime.get().info["usable_cores"])  # a fresh peer: every CPU is free

@ch.remote(cpus=1)
class Slot:
    def ping(self):
        return "up"

slots = [Slot.remote() for _ in range(cores)]
print("FULL", all(s.ping.remote().result(timeout=60) == "up" for s in slots))
extra = Slot.remote()
first_call = extra.ping.remote()
time.sleep(2.0)
print("WAITS", not first_call.done())
slots[0].close()  # one CPU frees up
print("STARTS", first_call.result(timeout=60) == "up")

@ch.remote(cpus=cores + 1)
class Huge:
    def ping(self):
        return 1

try:
    Huge.remote().ping.remote().result(timeout=30)
    print("HUGE ran")
except ch.Infeasible:
    print("HUGE infeasible")
"""


def test_an_actor_waits_for_a_free_cpu_and_starts_when_one_frees():
    home = tempfile.mkdtemp(prefix="cha", dir="/tmp")
    out = run_script(SCRIPT, env={"CHIMAERA_HOME": home}, home=home, timeout=300)
    assert out.returncode == 0, out.stderr[-3000:]
    assert "FULL True" in out.stdout, out.stdout
    assert "WAITS True" in out.stdout, out.stdout  # every CPU is held by the other actors
    assert "STARTS True" in out.stdout, out.stdout
    assert "HUGE infeasible" in out.stdout, out.stdout  # larger than the peer: fails, never waits
