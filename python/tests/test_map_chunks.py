"""Fast map items run several to a call (the hand batching Ray users write),
while each item keeps its own value and error, and slow items run alone. A
map over an async iterable takes items as its window allows and closes the
iterable when its reader leaves early."""

from __future__ import annotations

import asyncio
import time
import types

import pytest
from conftest import run_script

import chimaera as ch
from chimaera import _width
from chimaera._runtime import Runtime


@ch.remote
def fragile(x):
    if x == 4321:
        raise KeyError(x)
    return x * 2


@ch.remote
def slow(x):
    time.sleep(0.15)
    return x


@ch.remote
def inc(x):
    return x + 1


def calls_in_run() -> int:
    return sum(ch.runs.current_run().status().values())


def test_fast_items_share_calls_and_keep_their_own_errors():
    before = calls_in_run()
    outs = list(fragile.map(range(20_000)).outcomes())
    used = calls_in_run() - before
    assert [o.index for o in outs] == list(range(20_000))
    failed = [o for o in outs if o.error is not None]
    assert len(failed) == 1 and failed[0].index == 4321 and isinstance(failed[0].error, KeyError)
    assert outs[4322].value == 8644, "the failing item's neighbours in its chunk still have values"
    assert used < 2_000, f"20,000 fast items took {used} calls"


def test_slow_items_are_never_grouped():
    before = calls_in_run()
    assert sorted(slow.map(range(12), ordered=False)) == list(range(12))
    assert calls_in_run() - before == 12


def test_chained_stages_still_flow_between_workers():
    assert sum(inc.map(range(5000)).map(inc)) == sum(range(5000)) + 2 * 5000


def test_an_explicit_window_still_caps_items_in_flight():
    pulled = []

    def source():
        for i in range(100_000):
            pulled.append(i)
            yield i

    with inc.options(max_in_flight=16).map(source()) as r:
        for _ in range(5000):
            next(r)
        assert len(pulled) <= 5000 + 16 + 1


@ch.remote
def hold(x):
    time.sleep(5)
    return x


@ch.remote(groups={"io": 8})
class Lanes:
    @ch.method(group="io")
    def grouped(self, x):
        time.sleep(2)
        return x

    @ch.method(concurrency=3)
    def own(self, x):
        time.sleep(2)
        return x

    def plain(self, x):
        time.sleep(2)
        return x


def test_until_a_size_is_known_a_map_sends_what_can_run_at_once():
    # Fewer would leave workers idle for a whole item; more would leave
    # results of a size nobody knows yet behind a slow first item.
    cores = int(Runtime.get().info["usable_cores"])
    with hold.options(cpus=0.5).map(range(4 * cores)) as r:
        r._start()
        assert r._items == 2 * cores, f"{r._items} items of cpus=0.5 went out on {cores} cores"
    actor = Lanes.options(concurrency=2).remote()
    pool = Lanes.options(concurrency=2, replicas=2).pool()
    try:
        for name, at_once in (("grouped", 8), ("own", 3), ("plain", 2)):
            with getattr(actor, name).map(range(32)) as r:
                assert r._items == at_once, f"{r._items} items went out to {name}, whose lane runs {at_once}"
            with getattr(pool, name).map(range(32)) as r:
                assert r._items == 2 * at_once, f"{r._items} items went out to two replicas' {name}"
    finally:
        actor.close()
        pool.close()


def test_a_function_map_counts_the_cpus_and_gpus_of_the_live_peers_that_take_calls():
    nodes = [
        {"alive": True, "draining": False, "cpus": 8.0, "gpus": ["0", "1"]},
        {"alive": True, "draining": False, "cpus": 4.0, "gpus": []},
        {"alive": False, "draining": False, "cpus": 64.0, "gpus": ["0"]},
        {"alive": True, "draining": True, "cpus": 64.0, "gpus": ["0"]},
    ]
    rt = types.SimpleNamespace(client=types.SimpleNamespace(nodes=lambda: nodes), info={"usable_cores": 1, "gpus": []})

    def calls(**options):
        return _width.function_calls(rt, types.SimpleNamespace(get=options.get))

    assert (calls(), calls(cpus=0.5), calls(cpus=ch.auto(4)), calls(cpus=3)) == (12, 24, 3, 4)
    assert (calls(gpus=1), calls(cpus=0.25, gpus=0.5)) == (2, 4)


@ch.remote
def picky(x):
    if x == 50:
        raise KeyError(x)
    return x


@ch.remote
def add(x, y):
    return x + y


class Source:
    """An async generator that counts the items taken from it and notes
    whether its finally ran."""

    def __init__(self) -> None:
        self.pulled = 0
        self.closed = False

    async def items(self, n: int = 100_000):
        try:
            for i in range(n):
                self.pulled += 1
                yield i
        finally:
            self.closed = True


async def soon(condition) -> bool:
    for _ in range(500):
        if condition():
            return True
        await asyncio.sleep(0.01)
    return condition()


def test_an_async_source_is_pulled_as_the_window_allows_and_closed_by_a_break():
    src = Source()

    async def main():
        r = inc.options(max_in_flight=16).map(src.items())  # held here: only the loop's end closes it
        got = []
        async for x in r:
            got.append(x)
            assert src.pulled <= len(got) + 16, (src.pulled, len(got))
            if len(got) == 2000:
                break
        return got, await soon(lambda: src.closed)

    got, closed = asyncio.run(main())
    assert got == list(range(1, 2001))
    assert closed, "the source's finally did not run after the loop broke"


def test_an_async_source_closes_when_its_reader_is_cancelled():
    src = Source()

    async def main():
        task = asyncio.ensure_future(read(slow.map(src.items())))
        await asyncio.sleep(0.05)  # waiting on the first item
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return src.closed

    async def read(r):
        async for _ in r:
            pass

    assert asyncio.run(main()), "the source's finally had not run when the cancelled reader ended"


def test_an_async_source_closes_when_an_item_fails():
    src = Source()

    async def main():
        with pytest.raises(KeyError):
            async for _ in picky.map(src.items()):
                pass
        return src.closed

    assert asyncio.run(main()), "the source's finally had not run when the item's error reached the reader"


def test_an_async_with_block_closes_the_source_and_async_sources_zip():
    src = Source()

    async def main():
        async with inc.map(src.items()) as r:
            first = await r.__anext__()
        closed = src.closed
        zipped = [x async for x in add.map(Source().items(3), [10, 20, 30, 40])]
        return first, closed, zipped

    assert asyncio.run(main()) == (1, True, [10, 21, 32])


def test_a_map_over_an_async_source_is_not_read_synchronously():
    src = Source()
    r = inc.map(src.items())
    with pytest.raises(TypeError, match="async for"):
        next(r)
    with pytest.raises(TypeError, match="async for"):
        r.futures()
    assert src.pulled == 0


def test_chaining_a_map_of_an_async_source_raises_where_it_is_chained():
    # Raised later, the error came out of the next unrelated call, which
    # started the chained map, and the chained map then read as empty.
    src = Source()
    with pytest.raises(TypeError, match="async for"):
        inc.map(src.items()).map(inc)
    assert inc.remote(1).result() == 2
    assert src.pulled == 0


def test_an_async_source_that_awaits_between_items_still_shares_calls():
    # As an I/O source does: each item comes a loop step after the last.
    # Items that come while calls run go out together.
    async def items(n):
        for i in range(n):
            await asyncio.sleep(0)
            yield i

    async def main():
        return [x async for x in inc.map(items(5000))]

    before = calls_in_run()
    assert asyncio.run(main()) == list(range(1, 5001))
    used = calls_in_run() - before
    assert used < 1000, f"5,000 items from an async source took {used} calls"


def test_a_map_behind_a_slow_first_item_keeps_this_process_inside_its_byte_window():
    # Results under the inline limit come to this process as their calls
    # finish. Behind a slow first item, 4,096 of them (192 MiB) would wait
    # here; the byte window keeps what waits to 8 MiB. The calls behind the
    # first item give the results' size, so the window fills to that, not
    # just one item per core. Measured while the first item still runs, so
    # nothing has been read yet: reading costs memory of its own.
    out = run_script(
        """
        import os, subprocess, threading, time
        import chimaera as ch

        @ch.remote
        def blob(i):
            if i == 0:
                time.sleep(4)
            return bytes(48 << 10)

        def rss():
            try:
                with open("/proc/self/statm") as f:
                    return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            except FileNotFoundError:
                ps = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True)
                return int(ps.stdout) << 10

        assert len(list(blob.map(range(1, 65)))) == 64  # the peer, its workers and this process warmed up
        before, most, ran, first = rss(), 0, 0, threading.Event()
        r = iter(blob.options(max_in_flight=4096, max_in_flight_bytes="8 MiB").map(range(4096)))

        def sample():
            global most, ran
            while not first.wait(0.05):
                most, ran = max(most, rss() - before), max(ran, r._items)

        sampler = threading.Thread(target=sample)
        sampler.start()
        n = len(next(r))
        first.set()
        sampler.join()
        n += sum(len(x) for x in r)
        print(n, most, ran)
        """
    )
    assert out.returncode == 0, out.stderr
    n, grown, ran = map(int, out.stdout.split())
    assert n == 4096 * (48 << 10)
    assert ran * (48 << 10) >= 4 << 20, f"only {ran} items of 48 KiB ran behind the slow first one: it held the rest back"
    assert grown < 16 << 20, f"this process grew {grown >> 20} MiB behind a slow first item, with an 8 MiB byte window"
