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

from ._future import Future, put
from ._native import blake3_digest
from ._remote import remote


def bucket_of(key: Any, partitions: int) -> int:
    """A stable bucket for `key`, the same in every process. Keys must pickle
    deterministically: str, bytes, numbers and tuples of them do."""
    data = key.encode() if isinstance(key, str) else pickle.dumps(key, protocol=5)
    return int.from_bytes(blake3_digest(data)[:8], "little") % partitions


def _merge(into: dict[Any, Any], key: Any, value: Any, combine: Callable[[Any, Any], Any] | None, grouped: bool) -> None:
    if combine is not None:
        into[key] = combine(into[key], value) if key in into else value
    elif grouped:
        into.setdefault(key, []).extend(value)
    else:
        into.setdefault(key, []).append(value)


@remote
def _scatter(pairs: Iterable[tuple[Any, Any]], partitions: int, combine: Callable[[Any, Any], Any] | None) -> tuple:
    buckets: list[dict[Any, Any]] = [{} for _ in range(partitions)]
    for key, value in pairs:
        _merge(buckets[bucket_of(key, partitions)], key, value, combine, grouped=False)
    return tuple(put(list(b.items())) for b in buckets)


@remote
def _gather(combine: Callable[[Any, Any], Any] | None, *buckets: Future[list[tuple[Any, Any]]]) -> list[tuple[Any, Any]]:
    out: dict[Any, Any] = {}
    for bucket in buckets:
        # Each bucket is the ch.put handle a scatter returned: read just this one.
        for key, value in bucket.result():
            _merge(out, key, value, combine, grouped=combine is None)
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
