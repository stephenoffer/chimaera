"""Generator calls stream their items: in order, bounded ahead of the
consumer, replayed without duplicates when their worker dies."""

from __future__ import annotations

import os
import signal
import tempfile
import threading
import time

import pytest

import chimaera as ch
from chimaera import _future, _remote


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


@ch.remote
def after_file(path, n):
    while not os.path.exists(path):
        time.sleep(0.02)
    yield from range(n)


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


@ch.remote
def counted_blobs(n, path, size):
    for i in range(n):
        with open(path, "a") as f:
            f.write(f"{i}\n")
        yield bytes(size)


def test_the_generator_stays_a_byte_window_ahead_of_a_slow_consumer():
    path = os.path.join(tempfile.mkdtemp(), "produced")
    s = counted_blobs.options(max_in_flight_bytes="16 MiB").remote(40, path, 4 << 20)
    assert [len(next(s)) for _ in range(2)] == [4 << 20] * 2
    time.sleep(1.0)
    ahead = produced(path)
    # 16 MiB is four items of 4 MiB (and a few bytes): the generator stops
    # at the fifth past the two taken, before sending it.
    assert ahead <= 2 + 4 + 1, f"the generator ran {ahead} items of 4 MiB ahead of a consumer that took 2"
    assert sum(len(x) for x in s) == 38 * (4 << 20), "the rest still arrives once the consumer reads"


def test_a_driver_that_credits_by_items_only_is_not_held_to_a_byte_window(monkeypatch):
    # A driver from before byte credits sends no byte window and credits
    # every max_in_flight // 2 items. A generator that waited on bytes for it
    # stopped 256 MiB ahead, for a credit that driver never sends.
    def deliver(self, outcome):  # that driver's Stream._deliver
        if self._next - self._credited >= max(1, self._window // 2):
            self._credited = self._next
            self._rt.client.stream_credit(self.key, self._next)
        return self._rt.read_value(outcome)

    monkeypatch.setattr(_remote, "STREAM_WINDOW_BYTES", 0)  # its calls carry no byte window
    monkeypatch.setattr(_future.Stream, "_deliver", deliver)
    path = os.path.join(tempfile.mkdtemp(), "produced")
    s = counted_blobs.remote(30, path, 10 << 20)  # 300 MiB, inside the 64-item window
    sizes = []
    reader = threading.Thread(target=lambda: sizes.extend(len(x) for x in s), daemon=True)
    reader.start()
    reader.join(120)
    if reader.is_alive():
        s.close()
    assert sizes == [10 << 20] * 30, f"the generator stopped after {produced(path)} items of 10 MiB"


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


def test_a_cancelled_async_read_leaves_its_item_to_the_next_read():
    import asyncio

    path = os.path.join(tempfile.mkdtemp(), "go")

    async def take():
        s = after_file.remote(path, 3)
        read = asyncio.ensure_future(s.__anext__())
        await asyncio.sleep(0.3)  # waiting: the generator yields nothing until the file exists
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        open(path, "w").close()
        await asyncio.sleep(1.0)  # the items arrive with no read waiting
        return [x async for x in s]

    assert asyncio.run(take()) == [0, 1, 2]


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
