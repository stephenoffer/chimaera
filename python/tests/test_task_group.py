"""ch.TaskGroup: a block owns the calls added to it. Leaving it waits for
them, the first failure cancels the rest, failures that happened by
themselves are raised together, finished calls keep their results, and
groups nested in calls lend their CPU while they wait."""

from __future__ import annotations

import asyncio
import time

import pytest

import chimaera as ch
from chimaera._runtime import Runtime


@ch.remote
def value(x):
    return x


@ch.remote
def fail(x):
    raise ValueError(f"failed {x}")


@ch.remote
def nap(seconds, x):
    time.sleep(seconds)
    return x


@ch.remote
def leaf() -> int:
    time.sleep(0.05)
    return 1


@ch.remote
def branch() -> int:
    with ch.TaskGroup() as g:
        leaves = [g.add(leaf.remote()) for _ in range(2)]
    return sum(f.result() for f in leaves)


@ch.remote
def root() -> int:
    with ch.TaskGroup() as g:
        branches = [g.add(branch.remote()) for _ in range(2)]
    return sum(f.result() for f in branches)


@ch.remote
async def async_branch() -> int:
    async with ch.TaskGroup() as g:
        leaves = [g.add(leaf.remote()) for _ in range(2)]
    return sum(f.result() for f in leaves)


@ch.remote
async def async_root() -> int:
    async with ch.TaskGroup() as g:
        branches = [g.add(async_branch.remote()) for _ in range(2)]
    return sum(f.result() for f in branches)


@ch.remote
async def gathered_root() -> int:
    # Two groups wait at once: the first to finish must not end the call's
    # lending while the other still waits on calls that need a CPU.
    async def group(call, n):
        async with ch.TaskGroup() as g:
            fs = [g.add(call.remote()) for _ in range(n)]
        return sum(f.result() for f in fs)

    a, b = await asyncio.gather(group(leaf, 1), group(async_branch, 2))
    return a + b


def test_leaving_the_block_waits_for_every_call():
    with ch.TaskGroup() as g:
        slow = g.add(nap.remote(0.3, "slow"))
        fast = g.add(value.remote(1))
    assert slow.done() and fast.done()
    assert (slow.result(), fast.result()) == ("slow", 1)


def test_a_failure_cancels_the_calls_still_running_and_keeps_finished_results():
    with pytest.raises(ExceptionGroup) as caught:
        with ch.TaskGroup() as g:
            kept = g.add(value.remote("kept"))
            kept.result()  # published before anything failed
            running = g.add(nap.remote(60, "never"))
            g.add(fail.remote(1))
    assert [str(e) for e in caught.value.exceptions] == ["failed 1"], "a cancelled call was reported as a failure"
    assert kept.result() == "kept"
    assert running.cancelled()


def test_failures_that_happened_by_themselves_are_raised_together():
    with pytest.raises(ExceptionGroup) as caught:
        with ch.TaskGroup() as g:
            a, b = g.add(fail.remote(1)), g.add(fail.remote(2))
            running = g.add(nap.remote(60, "never"))
            ch.wait([a, b])  # both failed before the group could cancel either
    assert sorted(str(e) for e in caught.value.exceptions) == ["failed 1", "failed 2"]
    assert all(isinstance(e, ValueError) for e in caught.value.exceptions)
    assert running.cancelled()


def test_a_failure_is_raised_once_however_many_calls_of_the_group_take_it():
    # Calls that take a failed call's result never run; the failure is
    # raised once, by the call that raised it. A failure from outside the
    # group is the first call of the group's to report.
    outside = fail.remote(2)
    with pytest.raises(ExceptionGroup) as caught:
        with ch.TaskGroup() as g:
            a = g.add(fail.remote(1))
            b = g.add(value.remote(a))
            g.add(value.remote(b))
            c = g.add(value.remote(outside))
            g.add(value.remote(c))
            ch.wait([b, c])
    assert sorted(str(e) for e in caught.value.exceptions) == ["failed 1", "failed 2"]
    assert str(outside.exception()) == "failed 2"


def test_an_error_in_the_block_cancels_its_calls_and_is_raised_with_theirs():
    with pytest.raises(ExceptionGroup) as caught:
        with ch.TaskGroup() as g:
            running = g.add(nap.remote(60, "never"))
            raise KeyError("in the block")
    assert [type(e) for e in caught.value.exceptions] == [KeyError]
    assert running.cancelled()


def test_async_with_waits_without_blocking_the_loop():
    async def main():
        ticks = 0

        async def tick():
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.01)

        ticker = asyncio.ensure_future(tick())
        with pytest.raises(ExceptionGroup) as caught:
            async with ch.TaskGroup() as g:
                slow = g.add(nap.remote(0.5, "slow"))
                await slow
                running = g.add(nap.remote(60, "never"))
                g.add(fail.remote(3))
        ticker.cancel()
        return ticks, slow.result(), running.cancelled(), [str(e) for e in caught.value.exceptions]

    ticks, slow, cancelled, errors = asyncio.run(main())
    assert ticks >= 10, f"the loop ran {ticks} times in half a second: the group's wait blocked it"
    assert (slow, cancelled, errors) == ("slow", True, ["failed 3"])


@pytest.mark.parametrize(
    ("top", "total"), [(root, 4), (async_root, 4), (gathered_root, 5)], ids=["with", "async-with", "gathered"]
)
def test_groups_nested_in_calls_lend_their_cpu_while_they_wait(top, total):
    # Each root waits on two branches, each waiting on two leaves: six
    # waiting calls per core. Waits that held their CPU would leave no core
    # for the leaves.
    n = 2 * int(Runtime.get().info["usable_cores"])
    roots = [top.remote() for _ in range(n)]
    done, pending = ch.wait(roots, timeout=120)
    for f in pending:
        f.cancel()
    assert not pending, f"{len(pending)} of {n} nested groups still waiting after 120 s"
    assert ch.get(roots) == [total] * n


def test_calls_are_added_inside_the_block_only():
    g = ch.TaskGroup()
    with pytest.raises(RuntimeError, match="inside the group's block"):
        g.add(value.remote(1))
    with g:
        f = g.add(value.remote(2))
    with pytest.raises(RuntimeError, match="inside the group's block"):
        g.add(value.remote(3))
    with pytest.raises(ExceptionGroup) as caught:  # an error in the block, raised with the group's
        with ch.TaskGroup() as g2:
            g2.add(3)  # type: ignore[arg-type]
    [error] = caught.value.exceptions
    assert isinstance(error, TypeError) and "takes a future" in str(error)
    assert f.result() == 2
