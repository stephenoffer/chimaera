"""Durable workflows: a workflow whose worker dies runs again and is answered
from the run's record; finished steps never run twice; a replay that asks for
something else diverges."""

from __future__ import annotations

import asyncio
import gc

import os
import random
import signal
import tempfile
import time

import pytest

import chimaera as ch


@ch.remote
def step(i, path):
    with open(path, "a") as f:
        f.write(f"{i}\n")
    return i * 10


@ch.remote(workflow="durable")
async def pipeline(path, pids):
    total = 0
    for i in range(4):
        total += await step.remote(i, path)
        if i == 1:
            with open(pids, "a") as f:
                f.write(f"{os.getpid()}\n")
            time.sleep(1.0)  # the test kills the worker here, once
    return total


@ch.remote(workflow="durable")
async def draws(pids):
    r = await ch.durable.random()
    t = await ch.durable.now()
    with open(pids, "a") as f:
        f.write(f"{os.getpid()} {r} {t}\n")
    time.sleep(1.0)
    return r, t


@ch.remote(workflow="durable")
async def nondeterministic(path, pids):
    first = await step.remote(random.random(), path)  # differs on every run
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)
    return first


@ch.remote(workflow="durable")
async def dropped_step(path, pids):
    first = await step.remote(1, path)  # its future is dropped at once
    gc.collect()
    time.sleep(1.5)  # long enough for a release of that step to reach the peer
    with open(pids, "a") as f:
        f.write(f"{os.getpid()}\n")
    time.sleep(1.0)  # the test kills the worker here, once
    return first


def kill_first_run(pids):
    deadline = time.monotonic() + 60
    while True:
        try:
            with open(pids) as f:
                line = f.readline()
            if line.endswith("\n"):
                break
        except FileNotFoundError:
            pass
        assert time.monotonic() < deadline, "the workflow never reached its first checkpoint"
        time.sleep(0.02)
    os.kill(int(line.split()[0]), signal.SIGKILL)


def lines(path):
    with open(path) as f:
        return f.read().splitlines()


def test_a_replayed_workflow_does_not_repeat_finished_steps():
    d = tempfile.mkdtemp()
    steps, pids = os.path.join(d, "steps"), os.path.join(d, "pids")
    f = pipeline.remote(steps, pids)
    kill_first_run(pids)
    assert f.result(timeout=120) == 60
    assert sorted(lines(steps)) == ["0", "1", "2", "3"], "every step ran exactly once"
    assert len(lines(pids)) == 2, "the workflow itself ran twice"


def test_recorded_values_are_the_same_on_replay():
    pids = os.path.join(tempfile.mkdtemp(), "pids")
    f = draws.remote(pids)
    kill_first_run(pids)
    r, t = f.result(timeout=120)
    first, second = (ln.split()[1:] for ln in lines(pids))
    assert first == second == [repr(r), repr(t)]


def test_a_replay_that_makes_a_different_call_diverges():
    d = tempfile.mkdtemp()
    f = nondeterministic.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    with pytest.raises(ch.errors.WorkflowDivergence):
        f.result(timeout=120)


def test_a_replay_reads_a_step_whose_future_was_dropped():
    # The replay answers the step from the run's record: dropping the step's
    # future must not have released that record.
    d = tempfile.mkdtemp()
    f = dropped_step.remote(os.path.join(d, "steps"), os.path.join(d, "pids"))
    kill_first_run(os.path.join(d, "pids"))
    assert f.result(timeout=120) == 10
    assert lines(os.path.join(d, "steps")) == ["1"], "the step ran once"


def test_workflow_helpers_need_a_workflow():
    with pytest.raises(RuntimeError, match="workflow='durable'"):
        asyncio.run(ch.durable.now())
