"""Futures at the moment their calls finish, without a peer: a callback added
just then, a wait classifying a future just then, an async read cancelled
before its item arrives, completion order over many futures, and waiters
that stop waiting."""

from __future__ import annotations

import asyncio
import gc
import itertools
import sys
import threading
import time
import types
import weakref

import pytest

import chimaera as ch
from chimaera._future import Future, Results, Stream, _Chunk
from chimaera.compat import ray


class Rt:
    """What a future uses of its runtime, with no peer behind it."""

    def __init__(self) -> None:
        self.client = types.SimpleNamespace(flush=lambda: None, cancel=lambda *a: None, stream_credit=lambda *a: None)
        self.cond = threading.Condition()
        self.in_executor = True  # no exit hooks for maps and streams
        self.forked = False
        self._streams: dict[bytes, Stream] = {}

    def read_value(self, outcome, task=None):
        return outcome[1]

    def release_later(self, key):
        pass


def ok(value):
    return ("ok", value, None, b"", 0, 0)


def key(i: int) -> bytes:
    return i.to_bytes(32, "big")


class Racy(Future):
    """A future that another thread finishes during its `trip`-th done()
    check: the check has answered "not done", and the other thread has 0.2 s
    to finish it before the caller acts on that answer, which it does unless
    the caller holds completion off until it has acted."""

    def __init__(self, rt, trip, value=1):
        super().__init__(rt, key(0))
        self.checks, self.trip, self.value, self.setter = 0, trip, value, None

    def done(self):
        answer = super().done()
        self.checks += 1
        if self.checks == self.trip:
            self.setter = threading.Thread(target=self._set, args=(ok(self.value),))
            self.setter.start()
            self.setter.join(0.2)
        return answer

    def settled(self):
        if self.setter is not None:
            self.setter.join(10)
        return self


# ---- callbacks registered as the call finishes ----------------------------


def test_a_done_callback_added_as_the_call_finishes_runs_once():
    f = Racy(Rt(), trip=1)
    ran, seen = [], threading.Event()
    f.add_done_callback(lambda g: (ran.append(g), seen.set()))
    f.settled()
    assert seen.wait(5), "the callback was lost: added after completion handed the callbacks out"
    time.sleep(0.05)
    assert ran == [f]


@pytest.mark.parametrize("part", [False, True])
def test_an_internal_callback_added_as_the_call_finishes_runs_once(part):
    f = Racy(Rt(), trip=1)
    target = f.split(2)[1] if part else f
    ran = []
    target._on_done(ran.append)
    f.settled()
    assert ran == [target]


def test_an_await_as_the_call_finishes_returns():
    f = Racy(Rt(), trip=2, value="v")  # check 1: __await__'s own; check 2: registering its wake

    async def take():
        return await f

    assert asyncio.run(asyncio.wait_for(take(), 5)) == "v"


def test_of_two_outcomes_the_first_wins_and_callbacks_see_it():
    f = Racy(Rt(), trip=2, value="second")  # check 1: registering; check 2: this _set's
    seen = []
    f._on_done(lambda g: seen.append(g._outcome[1]))
    f._set(ok("first"))
    f.settled()
    assert f._outcome[1] == "first"
    assert seen == ["first"]


def test_callbacks_racing_completion_each_run_once():
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(5):
            rt = Rt()
            futures = [Future(rt, key(i)) for i in range(2000)]
            counts = [0] * len(futures)
            start = threading.Barrier(2)

            def register():
                start.wait()
                for i, f in enumerate(futures):
                    f._on_done(lambda _, i=i: counts.__setitem__(i, counts[i] + 1))

            def finish():
                start.wait()
                for f in futures:
                    f._set(ok(None))

            threads = [threading.Thread(target=register), threading.Thread(target=finish)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert counts == [1] * len(futures)
    finally:
        sys.setswitchinterval(old)


# ---- wait: each future in exactly one list ---------------------------------


def positions(fs, done, pending):
    at = {id(f): i for i, f in enumerate(fs)}
    return [at[id(f)] for f in done], [at[id(f)] for f in pending]


@pytest.mark.parametrize("timeout", [0, 0.05])  # a poll, and a wait that subscribes
@pytest.mark.parametrize("trip", range(1, 7))
def test_wait_puts_a_future_finishing_meanwhile_in_exactly_one_list(trip, timeout):
    rt = Rt()
    racy = Racy(rt, trip)
    done, pending = ch.wait([racy], timeout=timeout)
    racy.settled()
    assert len(done) + len(pending) == 1, (done, pending)

    first, last = Future(rt, key(1)), Future(rt, key(2))
    first._set(ok(0))
    racy = Racy(rt, trip)
    fs = [first, racy, last]
    done, pending = ch.wait(fs, num_returns=2, timeout=timeout)
    racy.settled()
    d, p = positions(fs, done, pending)
    assert sorted(d + p) == [0, 1, 2] and d == sorted(d) and p == sorted(p), (d, p)


@pytest.mark.parametrize("timeout", [0, 0.05])
@pytest.mark.parametrize("trip", range(1, 7))
def test_ray_wait_keeps_its_cap_with_a_future_finishing_meanwhile(trip, timeout):
    rt = Rt()
    first, last = Future(rt, key(1)), Future(rt, key(2))
    first._set(ok(0))
    racy = Racy(rt, trip)
    fs = [first, racy, last]
    ready, pending = ray.wait(fs, num_returns=1, timeout=timeout)
    racy.settled()
    d, p = positions(fs, ready, pending)
    assert len(d) == 1 and sorted(d + p) == [0, 1, 2] and p == sorted(p), (d, p)


def test_wait_drops_its_callbacks_on_futures_it_stops_waiting_for():
    rt = Rt()
    fs = [Future(rt, key(i)) for i in range(3)]
    fs[0]._set(ok(0))
    assert ch.wait(fs, num_returns=1, timeout=5) == ([fs[0]], fs[1:])
    assert ch.wait(fs, timeout=0.01) == ([fs[0]], fs[1:])
    assert [f._callbacks for f in fs[1:]] == [[], []]


# ---- completion order over many futures -------------------------------------

MANY = 100_000


class Counted(Future):
    """Counts done() checks and callback registrations over all instances, and
    fails a test that checks more often than its budget (a rescan)."""

    __slots__ = ()
    checks = registrations = budget = 0

    def done(self):
        Counted.checks += 1
        if Counted.checks > Counted.budget:
            raise AssertionError(f"{Counted.checks} done() checks: the unfinished futures are rescanned")
        return super().done()

    def _on_done(self, fn):
        Counted.registrations += 1
        return super()._on_done(fn)


def many(rt, n=MANY):
    Counted.checks = Counted.registrations = 0
    Counted.budget = 5 * n
    return [Counted(rt, key(i)) for i in range(n)]


def test_as_completed_over_many_futures_finishing_in_reverse_is_linear():
    fs = many(Rt())
    it = ch.as_completed(fs)
    for f in reversed(fs):
        f._set(ok(None))
        assert next(it) is f
    with pytest.raises(StopIteration):
        next(it)
    assert Counted.registrations == MANY


def test_async_as_completed_over_many_futures_finishing_in_reverse_is_linear():
    fs = many(Rt())

    async def main():
        it = ch.as_completed(fs)
        for f in reversed(fs):
            f._set(ok(None))
            assert await it.__anext__() is f
        with pytest.raises(StopAsyncIteration):
            await it.__anext__()

    asyncio.run(main())
    assert Counted.registrations == MANY


def test_wait_over_many_futures_finishing_in_reverse_is_linear():
    rt = Rt()
    fs = many(rt)

    def finish():  # as the dispatcher does: each outcome, then a notify
        for f in reversed(fs):
            f._set(ok(None))
            with rt.cond:
                rt.cond.notify_all()

    t = threading.Thread(target=finish)
    t.start()
    done, pending = ch.wait(fs, timeout=120)
    t.join()
    assert len(done) == MANY and pending == []


def test_async_as_completed_wakes_on_completion_keeps_a_cancelled_read_and_times_out():
    rt = Rt()
    a, b, c = (Future(rt, key(i)) for i in range(3))

    async def main():
        it = ch.as_completed([a, b, c], timeout=3)
        threading.Timer(0.1, a._set, args=(ok(1),)).start()
        first = await it.__anext__()  # woken from another thread
        read = asyncio.ensure_future(it.__anext__())
        await asyncio.sleep(0.05)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        b._set(ok(2))
        second = await it.__anext__()
        with pytest.raises(TimeoutError):
            await it.__anext__()
        return first, second

    assert asyncio.run(main()) == (a, b)


def test_an_as_completed_left_early_drops_its_callbacks():
    rt = Rt()
    fs = [Future(rt, key(i)) for i in range(3)]
    fs[1]._set(ok(1))
    assert next(iter(ch.as_completed(fs))) is fs[1]
    assert [fs[0]._callbacks, fs[2]._callbacks] == [[], []]


def test_as_completed_holds_only_the_futures_it_has_not_handed_out():
    rt = Rt()
    a, b, c = (Future(rt, key(i)) for i in range(3))
    a._set(ok(0))  # finished before the iterator subscribes
    it = ch.as_completed([a, b, c])
    b._set(ok(1))  # finished after
    taken = [weakref.ref(next(it)), weakref.ref(next(it))]
    pending = weakref.ref(c)
    del a, b, c
    assert [r() for r in taken] == [None, None], "a future handed out and let go is still held, with its result"
    f = pending()
    assert f is not None, "an unfinished future was freed: the runtime holds it weakly, so its outcome is lost"
    f._set(ok(2))
    assert next(it) is f


class Collecting(Future):
    """A future whose done() check runs the cycle collector, as the
    interpreter may at any call."""

    __slots__ = ()

    def done(self):
        gc.collect()
        return super().done()


def test_an_abandoned_as_completed_collected_as_a_call_finishes_drops_its_callbacks():
    rt = Rt()
    fs = [Future(rt, key(i)) for i in range(3)]
    gc.disable()
    try:
        it = ch.as_completed(fs)
        held = [it]
        held.append(held)  # only the collector frees it
        fs[0]._set(ok(0))
        assert next(it) is fs[0]
        del it, held
        finishing = Collecting(rt, key(9))
        setter = threading.Thread(target=finishing._set, args=(ok(1),), daemon=True)
        setter.start()  # its done() check, under the lock, collects the as_completed
        setter.join(5)
    finally:
        gc.enable()
    assert not setter.is_alive(), "the collected as_completed waited on the lock its own thread held"
    assert finishing.done() and [fs[1]._callbacks, fs[2]._callbacks] == [[], []]


# ---- async reads cancelled before their item arrives -----------------------


def test_a_cancelled_stream_read_leaves_the_item_for_the_next_read():
    rt = Rt()
    s = Stream(Future(rt, key(0)), 64, 256 << 20)

    def arrive(i):  # as the dispatcher delivers an item
        s._add(i, ok(f"item{i}"))
        with rt.cond:
            rt.cond.notify_all()

    async def main():
        read = asyncio.ensure_future(s.__anext__())
        await asyncio.sleep(0.1)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        arrive(0)
        await asyncio.sleep(0.2)  # time for anything still reading to take it
        arrive(1)
        return await asyncio.wait_for(s.__anext__(), 5)

    assert asyncio.run(main()) == "item0"


@pytest.mark.parametrize("error", [False, True])
def test_a_waiting_stream_read_wakes_for_an_item_and_for_the_end(error):
    rt = Rt()
    s = Stream(Future(rt, key(0)), 64, 256 << 20)

    async def main():
        read = asyncio.ensure_future(s.__anext__())
        await asyncio.sleep(0.05)
        threading.Thread(target=s._add, args=(0, ok("only"))).start()
        got = await asyncio.wait_for(read, 5)
        end = asyncio.ensure_future(s.__anext__())
        await asyncio.sleep(0.05)
        if error:
            threading.Thread(target=s._fut._set_exception, args=(ValueError("generator failed"),)).start()
        else:
            threading.Thread(target=s._fut._set, args=(ok(1),)).start()  # one item in all
        with pytest.raises(ValueError if error else StopAsyncIteration):
            await asyncio.wait_for(end, 5)
        return got

    assert asyncio.run(main()) == "only"


def test_a_cancelled_ordered_map_read_leaves_the_slot_for_the_next_read():
    rt = Rt()
    futures = []

    def submit(i, item):
        futures.append(Future(rt, key(i)))
        return futures[-1]

    r = Results(submit, iter(range(3)), ordered=True, window=8, parallel=3)  # three workers: all three go out at once

    async def main():
        read = asyncio.ensure_future(r.__anext__())
        await asyncio.sleep(0.05)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        for i, f in enumerate(futures):
            f._set(ok(i * 10))
        return [x async for x in r]

    assert asyncio.run(main()) == [0, 10, 20]


def test_a_cancelled_ray_generator_read_leaves_the_item_and_the_end_stops_iteration():
    rt = Rt()
    s = Stream(Future(rt, key(0)), 64, 256 << 20)
    gen = ray.ObjectRefGenerator(s)

    async def main():
        read = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0.1)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        s._add(0, ok("item0"))
        with rt.cond:
            rt.cond.notify_all()
        await asyncio.sleep(0.2)  # time for anything still reading to take it
        s._add(1, ok("item1"))
        s._fut._set(ok(2))
        return [ref.result() async for ref in gen]

    assert asyncio.run(main()) == ["item0", "item1"]


# ---- waiters that stop waiting ----------------------------------------------


async def timing_out(n, awaitable):
    for _ in range(n):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0):  # cancelled once it has started waiting
                await awaitable()


def test_awaits_that_time_out_leave_no_callbacks_or_memory_and_a_closed_loop_hears_nothing(capsys):
    f = Future(Rt(), key(0))
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(timing_out(1000, lambda: f))  # warm up
        gc.collect()
        before = sys.getallocatedblocks()
        loop.run_until_complete(timing_out(50_000, lambda: f))
        gc.collect()
        grown = sys.getallocatedblocks() - before
    finally:
        loop.close()
    assert f._callbacks == [], f"{len(f._callbacks)} callbacks left by awaits that timed out"
    assert grown < 1000, f"50,000 timed-out awaits kept {grown} memory blocks"
    f._set(ok(1))  # its loop has closed
    assert capsys.readouterr().err == ""


def test_an_await_still_pending_when_its_loop_closes_is_not_woken(capsys):
    f = Future(Rt(), key(0))
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda loop, context: None)  # the task left pending is the point

    async def waits():
        return await f

    task = loop.create_task(waits())
    loop.run_until_complete(asyncio.sleep(0.01))
    loop.close()
    f._set(ok(1))
    assert capsys.readouterr().err == ""
    del task


def test_unordered_map_reads_that_time_out_leave_no_callbacks(capsys):
    rt = Rt()
    futures = []

    def submit(i, item):
        futures.append(Future(rt, key(i)))
        return futures[-1]

    r = Results(submit, iter(range(3)), ordered=False, window=8, parallel=3)  # three workers: all three go out at once
    for _ in range(3):  # a new loop each time, closed after
        asyncio.run(timing_out(100, r.__anext__))
    assert [f._callbacks for f in futures] == [[], [], []]
    futures[1]._set(ok(10))
    assert capsys.readouterr().err == ""
    assert asyncio.run(r.__anext__()) == 10


def test_a_map_read_cancelled_while_its_async_source_produces_keeps_the_source_and_its_items():
    rt = Rt()

    def submit(i, item):
        f = Future(rt, key(i))
        f._set(ok(item * 10))
        return f

    async def main():
        gate = asyncio.Event()

        async def source():
            for i in range(3):
                await gate.wait()
                yield i

        r = Results(submit, source(), ordered=True, window=8)
        read = asyncio.ensure_future(r.__anext__())
        await asyncio.sleep(0.05)  # the read waits on the source
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        gate.set()
        return [x async for x in r]

    assert asyncio.run(main()) == [0, 10, 20]


async def until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    return condition()


def test_an_async_source_map_sends_items_as_they_arrive_and_reads_results_while_its_source_waits():
    rt = Rt()
    sent, calls = [], []

    def submit(i, item):
        sent.append(item)
        calls.append(Future(rt, key(i)))
        if i:
            calls[-1]._set(ok(item * 10))
        return calls[-1]

    async def main():
        more = asyncio.Event()

        async def source():
            for i in range(4):
                yield i
                await asyncio.sleep(0.01)  # one at a time
            await more.wait()  # then nothing for a while
            yield 4

        r = Results(submit, source(), ordered=True, window=8, parallel=8)
        first = asyncio.ensure_future(r.__anext__())
        assert await until(lambda: sent == [0, 1, 2, 3]), f"sent {sent} while item 0 ran"
        calls[0]._set(ok(0))
        got = [await asyncio.wait_for(first, 5)]
        got += [await asyncio.wait_for(r.__anext__(), 5) for _ in range(3)]  # the source still waits
        more.set()
        got += [x async for x in r]
        return got

    assert asyncio.run(main()) == [0, 10, 20, 30, 40]


def test_an_async_source_error_is_raised_after_the_items_before_it():
    rt = Rt()

    def submit(i, item):
        f = Future(rt, key(i))
        f._set(ok(item))
        return f

    async def source():
        yield 1
        yield 2
        raise KeyError("the source failed")

    async def main():
        r = Results(submit, source(), ordered=True, window=8, parallel=8)
        got = [await r.__anext__(), await r.__anext__()]
        with pytest.raises(KeyError, match="the source failed"):
            await r.__anext__()
        return got

    assert asyncio.run(main()) == [1, 2]


def test_a_chunked_map_takes_at_most_its_window_from_the_source_and_runs_a_chunk_while_one_is_read():
    rt = Rt()

    def chunk(i, items):  # fast items: chunks grow to the window
        value = _Chunk(x * 10 for x in items)
        value.elapsed = len(items) * 1e-6
        f = Future(rt, key(i))
        f._set(ok(value))
        return f

    pulled = []

    def source():
        for i in range(5000):
            pulled.append(i)
            yield i

    r = Results(lambda i, x: None, source(), ordered=True, window=16, chunk=chunk, parallel=8)
    for n, x in enumerate(r, 1):
        assert x == (n - 1) * 10
        assert len(pulled) - n <= 16, f"{len(pulled) - n} items taken past the {n} read, with max_in_flight=16"
        assert r._slots or not r._buf or len(pulled) == 5000, "nothing ran while a chunk was read"


def test_a_map_whose_items_all_fail_still_fills_its_window():
    rt = Rt()

    def submit(i, item):
        f = Future(rt, key(i))
        f._set_exception(ValueError(item))
        return f

    r = Results(submit, iter(range(1000)), ordered=True, window=64, parallel=2)
    errors = [o.error for o in itertools.islice(r.outcomes(), 10)]
    assert all(isinstance(e, ValueError) for e in errors)
    assert r._items + len(r._buf) > 2, "errors taught the map nothing: one item per worker, forever"


# ---- the bytes a map holds --------------------------------------------------

BIG = 8 << 20


def stored(value, size=BIG):
    """A finished call's outcome whose result is stored as `size` bytes."""
    return ("ok", value, None, b"", size, 0)


def held_by(r):
    """What a map holds: the stored bytes of finished results it has not
    handed out, and of the decoded batch it is handing out."""
    return sum(s.fut._outcome[4] for s in list(r._slots.values()) if s.fut.done()) + r._held


def test_an_ordered_map_behind_slow_items_holds_no_more_than_its_byte_window():
    rt = Rt()
    slow = {0: None, 200: None}

    def submit(i, item):
        f = Future(rt, key(i))
        if i in slow:
            slow[i] = f
        else:
            f._set(stored(i))  # finishes at once, 8 MiB stored
        return f

    def finish(i):  # results about the same size: a running item counts as their mean
        slow[i]._set(stored(i))
        with rt.cond:
            rt.cond.notify_all()

    cap = 64 << 20
    r = Results(submit, iter(range(400)), ordered=True, window=256, window_bytes=cap, parallel=4, grow_window=True)
    got, peaks = [], []

    def read():
        for x in r:
            got.append(x)
            peaks.append(held_by(r))

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    time.sleep(0.1)
    blocked_at_start = held_by(r)  # submitted before any result's size was known
    finish(0)
    deadline = time.monotonic() + 10
    while len(got) < 200 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.1)
    blocked_midway = held_by(r)  # submitted by measured sizes
    finish(200)
    reader.join(10)
    assert got == list(range(400))
    assert blocked_at_start <= cap, f"{blocked_at_start >> 20} MiB held behind the first item"
    assert blocked_midway <= cap + BIG, f"{blocked_midway >> 20} MiB held behind item 200"
    assert max(peaks) <= cap + BIG, f"{max(peaks) >> 20} MiB held"


def test_a_map_counts_the_decoded_batch_it_hands_out_and_reads_on_after_it():
    rt = Rt()

    def chunk(i, items):  # each item measured at 1/80 s: by time, batches would grow to 8 items
        value = _Chunk(items)
        value.elapsed = len(items) / 80
        f = Future(rt, key(i))
        f._set(stored(value, len(items) * BIG))
        return f

    # A batch is at most half the byte window (4 items): one handed out and
    # one running fill it, and nothing more goes out until the first is read.
    r = Results(lambda i, x: None, iter(range(64)), ordered=True, window=64, window_bytes=8 * BIG, chunk=chunk)
    got, seen = [], []
    for x in r:
        got.append(x)
        seen.append((len(r._buf), r._held, r._bytes))
    assert got == list(range(64)), "the map ended while a batch it held kept its window full"
    for buffered, held, charged in seen:
        assert held >= buffered * BIG and (held == 0) == (buffered == 0) and charged >= held, (buffered, held, charged)
        assert charged <= 8 * BIG, f"{charged >> 20} MiB charged to an {8 * BIG >> 20} MiB window"
    assert max(buffered for buffered, _, _ in seen) == 3, "batches did not grow to half the byte window"


def slow_head_map(rt, n=10_000):
    """An ordered map of `n` small items whose first item runs until the
    test sets it; the others finish as they are submitted."""
    head, submitted = [], []

    def submit(i, item):
        f = Future(rt, key(i))
        submitted.append(i)
        if i == 0:
            head.append(f)
        else:
            f._set(ok(item))
        return f

    r = Results(submit, iter(range(n)), ordered=True, window=256, parallel=16, grow_window=True)
    return r, head, submitted


def test_an_ordered_map_behind_a_slow_first_item_fills_its_window_once_a_result_is_measured():
    # Until a result's size is known a map runs one item per worker. The
    # calls behind a slow first item finish first and give the size, so the
    # rest of the window runs meanwhile, not after the first item.
    rt = Rt()
    r, head, submitted = slow_head_map(rt)
    got = []
    reader = threading.Thread(target=lambda: got.extend(r), daemon=True)
    reader.start()
    deadline = time.monotonic() + 5
    while len(submitted) < 256 and time.monotonic() < deadline:
        with rt.cond:
            rt.cond.notify_all()  # as the runtime does when outcomes arrive
        time.sleep(0.01)
    assert len(submitted) == 256, f"{len(submitted)} items ran behind the slow first one, with a window of 256"
    head[0]._set(ok(0))
    with rt.cond:
        rt.cond.notify_all()
    reader.join(10)
    assert got == list(range(10_000))


def test_an_async_ordered_map_behind_a_slow_first_item_fills_its_window_once_a_result_is_measured():
    rt = Rt()

    async def main():
        r, head, submitted = slow_head_map(rt)
        first = asyncio.ensure_future(r.__anext__())
        filled = await until(lambda: len(submitted) == 256)
        n = len(submitted)
        head[0]._set(ok(0))
        return filled, n, [await asyncio.wait_for(first, 5)] + [x async for x in r]

    filled, n, got = asyncio.run(main())
    assert filled, f"{n} items ran behind the slow first one, with a window of 256"
    assert got == list(range(10_000))


def test_fast_items_behind_a_slow_first_item_still_share_calls():
    # A call behind the slow first item gives the time per item as well as
    # the size, so the window fills with batches, not one call per item.
    rt = Rt()
    head, calls = [], []

    def chunk(i, items):
        calls.append(len(items))
        f = Future(rt, key(i))
        value = _Chunk(items)
        value.elapsed = len(items) * 1e-6
        if i == 0:
            head.append((f, value))
        else:
            f._set(ok(value))
        return f

    r = Results(lambda i, x: None, iter(range(100_000)), ordered=True, window=256, chunk=chunk, parallel=16, grow_window=True)
    got = []
    reader = threading.Thread(target=lambda: got.extend(r), daemon=True)
    reader.start()
    deadline = time.monotonic() + 5
    while sum(calls) <= 16 and time.monotonic() < deadline:
        with rt.cond:
            rt.cond.notify_all()
        time.sleep(0.01)
    behind = calls[16:]  # after the first item per worker
    assert behind and sum(behind) / len(behind) >= 100, f"{sum(behind)} items behind the first one took {len(behind)} calls"
    f, value = head[0]
    f._set(ok(value))
    with rt.cond:
        rt.cond.notify_all()
    reader.join(10)
    assert got == list(range(100_000))
