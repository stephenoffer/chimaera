"""Fast map items run several to a call (the hand batching Ray users write),
while each item keeps its own value and error, and slow items run alone."""

from __future__ import annotations

import time

import chimaera as ch


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
