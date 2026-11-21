"""Measurement provenance: process-scoped readings everywhere, attempt-scoped
cgroup readings (and enforced limits) where Linux delegation is available."""

from __future__ import annotations

import os
import subprocess
import sys
import time

import pytest

import chimaera as ch
from conftest import BIN, HOME


def _isolated_available() -> bool:
    out = subprocess.run([BIN, "--dir", HOME, "doctor"], capture_output=True, text=True, timeout=30).stdout
    return "isolated accounting: available" in out


@ch.remote
def busy(seconds: float) -> int:
    t = time.process_time()
    x = 0
    while time.process_time() - t < seconds:
        x += 1
    return x


@ch.remote
def nap(seconds: float) -> None:
    time.sleep(seconds)
    bytearray(1 << 20)


def _measure(f, name):
    f.result(timeout=60)
    m = f.explain()["attempts"][0]["measures"]
    return m.get(name)


def test_busy_loop_uses_one_core_and_a_sleep_uses_none():
    f = busy.remote(0.5)
    cpu, scope, method = _measure(f, "cpu_seconds")
    assert scope in ("process", "attempt") and method in ("rusage", "cgroup")
    # The loop burns 0.5 s of CPU whatever else the host runs.
    assert 0.48 <= cpu <= 0.55, cpu
    cores, _, _ = _measure(f, "mean_cores")
    # A whole core per second of wall time only when the host has one free:
    # on an oversubscribed host the same loop truly gets less.
    if os.getloadavg()[0] < (os.cpu_count() or 1) * 0.75:
        assert 0.95 <= cores <= 1.05, cores
    else:
        assert cores <= 1.05, cores
    nap_cpu, _, _ = _measure(nap.remote(0.3), "cpu_seconds")
    assert nap_cpu < 0.006, nap_cpu


isolated = pytest.mark.skipif(
    sys.platform != "linux", reason="per-attempt cgroups need Linux with cgroup v2 delegation"
)


def test_isolated_request_without_delegation_is_infeasible():
    if _isolated_available():
        pytest.skip("this host supports isolated accounting")

    @ch.remote(accounting="isolated")
    def f():
        return 1

    with pytest.raises(ch.Infeasible, match="cgroup v2 delegation"):
        f.remote().result(timeout=30)


@isolated
def test_isolated_attempt_reports_its_own_peak_and_cpu():
    if not _isolated_available():
        pytest.skip("no cgroup v2 delegation on this host")

    @ch.remote(accounting="isolated")
    def alloc():
        b = bytearray(64 << 20)
        for i in range(0, len(b), 4096):
            b[i] = 1
        t = time.process_time()
        while time.process_time() - t < 0.2:
            pass
        return len(b)

    f = alloc.remote()
    assert f.result(timeout=60) == 64 << 20
    m = f.explain()["attempts"][0]["measures"]
    peak, scope, method = m["memory_peak_bytes"]
    assert (scope, method) == ("attempt", "cgroup")
    assert peak >= 64 << 20
    cpu, _, _ = m["cpu_seconds"]
    assert 0.15 <= cpu <= 1.0


@isolated
def test_isolated_limit_breach_is_an_oom_kill_with_the_peak():
    if not _isolated_available():
        pytest.skip("no cgroup v2 delegation on this host")

    @ch.remote(accounting="isolated", memory_limit="100 MiB")
    def hog():
        return len(b"\x01" * (300 << 20))

    with pytest.raises(ch.OutOfMemory, match=r"peak"):
        hog.remote().result(timeout=120)
