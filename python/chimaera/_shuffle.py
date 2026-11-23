"""Repartitioning (key, value) pairs by key between calls: the engine under
``Results.reduce_by_key``.

``shuffle`` is the whole contract: inputs are futures of iterables of pairs,
the output is one future per partition. This implementation runs in ordinary
calls. One scatter call per input combines its pairs by key (when a combine
is given), splits them into buckets by a stable hash of the key, and returns
the buckets as one tuple, stored value by value. One gather call per
partition reads only its own bucket of every scatter and merges them.
Payloads move between workers, never through the driver; a lost worker
re-runs only its own call.

Buckets and partitions are dicts (key to value, or to the list of values
without a combine), never lists of pairs: a dict pickles and unpickles
without making a tuple per pair, which on a 3.2M-pair shuffle is most of the
cost of moving it (and the cycle collector's, which those tuples wake).

Cost: each gather unpickles one small handle tuple per input, so inputs ×
partitions should stay in the low millions.
"""

from __future__ import annotations

import pickle
from collections.abc import Callable, Iterable
from typing import Any

from ._future import Future
from ._native import blake3_digest
from ._remote import remote


def bucket_of(key: Any, partitions: int) -> int:
    """A stable bucket for `key`, the same in every process. Keys must pickle
    deterministically: str, bytes, numbers and tuples of them do."""
    if type(key) is int:
        # CPython does not randomize int hashing: hash() agrees across
        # processes, at a fraction of the cost of pickling and digesting.
        return hash(key) % partitions
    data = key.encode() if isinstance(key, str) else pickle.dumps(key, protocol=5)
    return int.from_bytes(blake3_digest(data)[:8], "little") % partitions


@remote
def _scatter(pairs: Iterable[tuple[Any, Any]], partitions: int, combine: Callable[[Any, Any], Any] | None) -> tuple:
    return _split(pairs, partitions, combine)


class _MapScatter:
    """A map's function and the scatter of the pairs it returns, in one call:
    the pairs are never stored between them (see ``Results.claim``)."""

    def __init__(self, fn: Callable[..., Any], single: bool) -> None:
        self.fn, self.single = fn, single
        self.__module__ = getattr(fn, "__module__", __name__)
        self.__qualname__ = f"{getattr(fn, '__qualname__', type(fn).__name__)}[scatter]"

    def __call__(self, item: Any, partitions: int, combine: Callable[[Any, Any], Any] | None) -> tuple:
        return _split(self.fn(item) if self.single else self.fn(*item), partitions, combine)


def _split(pairs: Iterable[tuple[Any, Any]], partitions: int, combine: Callable[[Any, Any], Any] | None) -> tuple:
    buckets: list[dict[Any, Any]] = [{} for _ in range(partitions)]
    # The loops run once per pair, so they are written out in full: a helper
    # call per pair doubled their cost.
    if combine is None:
        for key, value in pairs:
            buckets[bucket_of(key, partitions)].setdefault(key, []).append(value)
    else:
        for key, value in pairs:
            b = buckets[hash(key) % partitions if type(key) is int else bucket_of(key, partitions)]
            b[key] = combine(b[key], value) if key in b else value
    # A tuple result is stored value by value: each gather reads only its own.
    return tuple(buckets)


@remote
def _gather(combine: Callable[[Any, Any], Any] | None, *buckets: dict[Any, Any]) -> dict[Any, Any]:
    out: dict[Any, Any] = {}
    if combine is None:
        for bucket in buckets:
            for key, values in bucket.items():
                out.setdefault(key, []).extend(values)
        return out
    # The first bucket is copied whole (the inputs are not ours to change).
    if buckets:
        out.update(buckets[0])
    for bucket in buckets[1:]:
        for key, value in bucket.items():
            out[key] = combine(out[key], value) if key in out else value
    return out


def shuffle(
    inputs: list[Future[Any]], partitions: int, combine: Callable[[Any, Any], Any] | None = None
) -> list[Future[dict[Any, Any]]]:
    """Partition the pairs of `inputs` by key into `partitions` outputs.

    With `combine` (associative), each key's values are folded into one;
    without it, each key maps to the list of its values.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    scattered = [_scatter.options(returns=partitions).remote(f, partitions, combine).split() for f in inputs]
    return _gathers(scattered, partitions, combine)


def shuffle_mapped(
    fn: Callable[..., Any],
    eff: Any,
    single: bool,
    items: list[Any],
    partitions: int,
    combine: Callable[[Any, Any], Any] | None = None,
) -> list[Future[dict[Any, Any]]]:
    """``shuffle`` of `fn` applied to each item (a map nobody read: see
    ``Results.claim``), with `fn` run inside the scatter calls, under the
    map's options."""
    from ._options import Effective
    from ._remote import CALL_FUNCTION, submit_call
    from ._runtime import Runtime

    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    rt = Runtime.get()
    runner = _MapScatter(fn, single)
    split = Effective({**eff.values, "returns": partitions}, {**eff.sources, "returns": "reduce_by_key"})
    scattered = [
        submit_call(rt, runner, (item, partitions, combine), {}, kind=CALL_FUNCTION, eff=split).split(partitions)
        for item in items
    ]
    return _gathers(scattered, partitions, combine)


def _gathers(scattered: list[tuple[Future[Any], ...]], partitions: int, combine: Any) -> list[Future[Any]]:
    return [_gather.remote(combine, *(parts[j] for parts in scattered)) for j in range(partitions)]
