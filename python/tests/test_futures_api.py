"""Waiting the ways Python code already does: ch.get and ch.wait, a
concurrent.futures executor, non-blocking cancel, and one Results type for
anything that yields many values."""

from __future__ import annotations

import concurrent.futures
import threading
import time

import pytest

import chimaera as ch


@ch.remote
def square(x: int) -> int:
    return x * x


@ch.remote
def nap(seconds: float, value: str) -> str:
    time.sleep(seconds)
    return value


@ch.remote
def numbers(n: int):
    yield from range(n)


def test_get_takes_one_future_or_many():
    assert ch.get(square.remote(3)) == 9
    assert ch.get([square.remote(i) for i in range(4)]) == [0, 1, 4, 9]
    with pytest.raises(TypeError, match="ch.get takes"):
        ch.get(3)


def test_wait_returns_what_finished_and_what_did_not_in_order():
    fast = [square.remote(i) for i in range(3)]
    slow = nap.remote(30, "late")
    done, pending = ch.wait([*fast, slow], num_returns=3, timeout=30)
    assert done == fast and pending == [slow]
    done, pending = ch.wait([slow], timeout=0.2)
    assert done == [] and pending == [slow]
    slow.cancel()


def test_thousands_of_calls_come_back_as_they_finish():
    import asyncio

    n = 3000
    futures = [square.remote(i) for i in range(n)]
    assert sorted(f.result() for f in ch.as_completed(futures, timeout=120)) == [i * i for i in range(n)]
    done, pending = ch.wait(futures, timeout=60)
    assert done == futures and pending == []

    async def gather_as_they_finish():
        more = [square.remote(i) for i in range(n)]
        return sorted([(await f) async for f in ch.as_completed(more, timeout=120)])

    assert asyncio.run(gather_as_they_finish()) == [i * i for i in range(n)]


def test_cancel_returns_at_once():
    f = nap.remote(60, "never")
    t0 = time.monotonic()
    assert f.cancel() is True
    assert time.monotonic() - t0 < 1
    with pytest.raises(ch.Cancelled):
        f.result(timeout=30)
    assert f.cancelled()
    done = square.remote(2)
    done.result(timeout=30)
    assert done.cancel() is False and not done.cancelled()


def test_the_executor_is_a_concurrent_futures_executor():
    with ch.Executor() as ex:
        futures = [ex.submit(lambda x: x + 1, i) for i in range(5)]  # a lambda, unlike ProcessPoolExecutor
        done, _ = concurrent.futures.wait(futures, timeout=60)
        assert len(done) == 5
        assert sorted(f.result() for f in concurrent.futures.as_completed(futures)) == [1, 2, 3, 4, 5]
        assert list(ex.map(square, range(4))) == [0, 1, 4, 9]
        failing = ex.submit(int, "not a number")
        with pytest.raises(ValueError):
            failing.result(timeout=60)
    with pytest.raises(RuntimeError, match="after shutdown"):
        ex.submit(abs, -1)


def test_a_generator_call_is_results_like_a_map():
    items = numbers.remote(5)
    assert isinstance(items, ch.Results)
    assert [o.value for o in items.outcomes()] == [0, 1, 2, 3, 4]
    assert list(square.map(numbers.remote(3))) == [0, 1, 4]


def test_a_slow_done_callback_does_not_hold_up_other_results():
    release = threading.Event()
    first = nap.remote(1, "first")  # still running when its callback is added
    first.add_done_callback(lambda _: release.wait(30))  # blocks its thread, not delivery
    first.result(timeout=30)
    t0 = time.monotonic()
    assert square.remote(5).result(timeout=10) == 25
    assert time.monotonic() - t0 < 5
    release.set()


def test_gather_is_one_future_of_every_value():
    both = ch.gather(square.remote(2), square.remote(3))
    assert isinstance(both, ch.Future)
    assert both.result(timeout=30) == [4, 9]

    @ch.remote
    def total(values: list[int]) -> int:
        return sum(values)

    assert total.remote(ch.gather(square.remote(1), square.remote(2))).result(timeout=30) == 5

    async def main() -> list[int]:
        return await ch.gather(square.remote(4), square.remote(5))

    import asyncio

    assert asyncio.run(main()) == [16, 25]
    assert ch.gather().result(timeout=30) == []
    with pytest.raises(TypeError, match="unpack a list"):
        ch.gather([square.remote(1)])
