"""Budgets: budget={"cpu_seconds": ..., "gpu_seconds": ...} covers a call and every call it makes."""

from __future__ import annotations

import time

import pytest

import chimaera as ch


@ch.remote(cpus=1)
def burn(seconds: float) -> float:
    end = time.process_time() + seconds
    while time.process_time() < end:
        pass
    return seconds


@ch.remote(cpus=0.1)
def sweep(n: int, each: float) -> float:
    return sum(ch.get((burn.remote(each) for _ in range(n))))


def test_a_subtree_over_its_budget_stops_with_an_error_naming_the_budget():
    start = time.monotonic()
    with pytest.raises(ch.errors.BudgetExceeded, match=r"budget exceeded: cpu_seconds=2 allowed, \d+\.\d\d used by this call"):
        sweep.options(budget={"cpu_seconds": 2}).remote(8, 3.0).result(timeout=120)
    assert time.monotonic() - start < 20, "stopped while the children ran, not after all 24 CPU-seconds"


def test_work_within_its_budget_finishes():
    assert sweep.options(budget={"cpu_seconds": 100}).remote(2, 0.2).result(timeout=120) == pytest.approx(0.4)


def test_one_long_call_is_stopped_while_it_runs():
    start = time.monotonic()
    f = burn.options(budget={"cpu_seconds": 1}).remote(60)
    with pytest.raises(ch.errors.BudgetExceeded, match="cpu_seconds=1 allowed"):
        f.result(timeout=120)
    assert time.monotonic() - start < 20, "stopped long before its 60 s of CPU"


@ch.remote(gpus=1)
def hold_gpu(seconds: float) -> str:
    time.sleep(seconds)  # no CPU: only the GPU's time counts
    return ch.dist.device()


def test_gpu_seconds_count_the_time_a_call_holds_its_gpus():
    call = hold_gpu.options(budget={"gpu_seconds": "1s"}).remote(60)
    if ch.runs.cluster().resources()["gpus"] == 0:  # nothing to hold on this host: the call cannot run
        with pytest.raises(ch.Infeasible):
            call.result(timeout=60)
        return
    with pytest.raises(ch.errors.BudgetExceeded, match="gpu_seconds=1 allowed"):
        call.result(timeout=120)


@ch.remote(cpus=0.1)
def nests():
    return burn.options(budget={"cpu_seconds": 5}).remote(0).result()


def test_budgets_do_not_nest_and_are_checked():
    with pytest.raises(ch.OptionsError, match="budgets do not nest"):
        nests.options(budget={"cpu_seconds": 5}).remote().result(timeout=60)
    assert nests.remote().result(timeout=60) == 0, "an inner budget alone is fine"
    with pytest.raises(ValueError, match="unknown 'cpu'; a budget limits cpu_seconds and gpu_seconds"):
        burn.options(budget={"cpu": 1})
    with pytest.raises(ValueError, match="expected a positive number"):
        burn.options(budget={"cpu_seconds": 0})
