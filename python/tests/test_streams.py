"""Generator calls stream their items: in order, bounded ahead of the
consumer, replayed without duplicates when their worker dies."""

from __future__ import annotations

import os
import signal
import tempfile
import time

import pytest

import chimaera as ch


@ch.remote
def squares(n):
    for i in range(n):
        yield i * i


@ch.remote
def fails_after(k):
    for i in range(k):
        yield i
    raise ValueError("generator failed")


@ch.remote
def counted(n, path):
    for i in range(n):
        with open(path, "a") as f:
            f.write(f"{i}\n")
        yield i


@ch.remote
def slow_with_pid(n, path):
    with open(path, "a") as f:
        f.write(f"{os.getpid()}\n")
    for i in range(n):
        time.sleep(0.05)
        yield i


@ch.remote
async def async_squares(n):
    for i in range(n):
        yield i * i


@ch.remote
def evens(n):
    return (i for i in range(n) if i % 2 == 0)  # returns a generator; is not one


def produced(path):
    try:
        with open(path) as f:
            return len(f.read().splitlines())
    except FileNotFoundError:
        return 0


def test_a_function_that_returns_a_generator_says_so_instead_of_hanging():
    # It used to stream to nobody: .result() gave the item count, or hung
    # once more than a window of items were waiting.
    for n in (10, 1000):
        with pytest.raises(TypeError, match="returned a generator"):
            evens.remote(n).result(timeout=30)


def test_items_arrive_in_order_and_the_stream_ends():
    assert list(squares.remote(6)) == [0, 1, 4, 9, 16, 25]
    assert list(squares.remote(0)) == []


def test_an_error_arrives_after_the_items_before_it():
    s = fails_after.remote(2)
    assert next(s) == 0 and next(s) == 1
    with pytest.raises(ValueError, match="generator failed"):
        next(s)


def test_the_generator_stays_a_window_ahead_of_a_slow_consumer():
    path = os.path.join(tempfile.mkdtemp(), "produced")
    s = counted.options(max_in_flight=8).remote(1000, path)
    assert [next(s) for _ in range(2)] == [0, 1]
    time.sleep(1.0)
    ahead = produced(path)
    assert ahead <= 2 + 8 + 1, f"the generator ran {ahead} items ahead of a consumer that took 2"
    assert len(list(s)) == 998, "the rest still arrives once the consumer reads"


def test_close_stops_the_generator():
    path = os.path.join(tempfile.mkdtemp(), "produced")
    s = counted.options(max_in_flight=4).remote(10_000, path)
    next(s)
    s.close()
    time.sleep(1.0)
    stopped_at = produced(path)
    time.sleep(1.0)
    assert produced(path) == stopped_at < 100


def test_async_generators_stream_too():
    async def take():
        return [x async for x in async_squares.remote(4)]

    import asyncio

    assert asyncio.run(take()) == [0, 1, 4, 9]


def test_a_worker_killed_mid_stream_replays_without_duplicates():
    path = os.path.join(tempfile.mkdtemp(), "pids")
    s = slow_with_pid.remote(20, path)
    first = [next(s) for _ in range(3)]
    with open(path) as f:
        pid = int(f.readline())
    os.kill(pid, signal.SIGKILL)
    rest = list(s)
    assert first + rest == list(range(20)), "every item exactly once, in order"
    assert produced(path) == 2, "the generator ran again on a new worker"
