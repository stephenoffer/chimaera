"""Repartitioning (key, value) pairs by key between calls: the engine under
``Results.reduce_by_key``.

``shuffle`` is the whole contract: inputs are futures of iterables of pairs,
the output is one future per partition. This implementation runs in ordinary
calls. One scatter call per input combines its pairs by key (when a combine
is given), splits them into buckets by a stable hash of the key, and stores
each bucket once with ``ch.put``. One gather call per partition reads only its
own bucket of every scatter and merges them. Payloads move between workers,
never through the driver; a lost worker re-runs only its own call.

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
    return tuple(list(b.items()) for b in buckets)


@remote
def _gather(combine: Callable[[Any, Any], Any] | None, *buckets: list[tuple[Any, Any]]) -> list[tuple[Any, Any]]:
    out: dict[Any, Any] = {}
    if combine is None:
        for bucket in buckets:
            for key, values in bucket:
                out.setdefault(key, []).extend(values)
    else:
        for bucket in buckets:
            for key, value in bucket:
                out[key] = combine(out[key], value) if key in out else value
    return list(out.items())


def shuffle(
    inputs: list[Future[Any]], partitions: int, combine: Callable[[Any, Any], Any] | None = None
) -> list[Future[list[tuple[Any, Any]]]]:
    """Partition the pairs of `inputs` by key into `partitions` outputs.

    With `combine` (associative), each key's values are folded into one;
    without it, each key maps to the list of its values.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    scattered = [_scatter.options(returns=partitions).remote(f, partitions, combine).split() for f in inputs]
    return [_gather.remote(combine, *(parts[j] for parts in scattered)) for j in range(partitions)]
