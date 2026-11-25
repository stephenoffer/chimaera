"""Repartitioning (key, value) pairs by key between calls: the engine under
``Results.reduce_by_key``.

``shuffle`` is the whole contract: inputs are futures of iterables of pairs,
the output is one future per partition. This implementation runs in ordinary
calls. One scatter call per input combines its pairs by key (when a combine
is given), splits them into buckets by a stable hash of the key, and returns
a bucket for each partition its pairs touch (a ``_serde.Sparse``: none for
the others), stored value by value. One manifest call waits for every
scatter and lists where each stored its result; one gather call per
partition depends on the manifest alone, reads only its own bucket of every
listed result, one at a time, and folds each into the partition before it
reads the next: it holds the partition and one bucket, however many
scatters there are. Payloads move between workers, never through the
driver; a lost worker re-runs only its own call.

Buckets and partitions are dicts (key to value, or to the list of values
without a combine), never lists of pairs: a dict pickles and unpickles
without making a tuple per pair, which on a 3.2M-pair shuffle is most of the
cost of moving it (and the cycle collector's, which those tuples wake).

Cost: the driver and the shard keep inputs + partitions calls and edges,
not inputs × partitions. Each gather reads the whole manifest (an entry per
input, with a scatter result small enough to be inline in it whole) and one
bucket of each input.
"""

from __future__ import annotations

import collections
import concurrent.futures
import operator
import os
import pickle
import sys
import time
import weakref
from collections.abc import Callable, Iterable, Iterator, Sequence
from decimal import Decimal
from fractions import Fraction
from typing import Any

from . import _resident, _serde, _shuffle_phases, _width
from ._future import Future
from ._native import blake3_digest, scatter
from ._remote import remote


def bucket_of(key: Any, partitions: int) -> int:
    """A stable bucket for `key`: the same in every process, and the same for
    equal keys (see key_hash)."""
    if type(key) is int:
        # CPython does not randomize int hashing: hash() agrees across
        # processes, at a fraction of the cost of pickling and digesting.
        return hash(key) % partitions
    return key_hash(key) % partitions


def key_hash(key: Any) -> int:
    """A hash of `key` that every process agrees on, equal for keys that
    compare equal. int, bool, float, complex, Fraction and Decimal values
    (and subclasses that keep their hash) use Python's own hash, which is not
    randomized and is the same for equal values of any of them (1, True and
    1.0; 0.0 and -0.0); a NaN, equal to nothing, gets 0. A NumPy scalar
    counts as the Python value it holds. str and bytes are digested. A tuple
    or frozenset gets Python's hash, not randomized either, of its elements'
    hashes, so (1, "a") and (True, "a") meet. Anything else, a number with a
    hash of its own included (it may mix in a str's), is digested pickled,
    so it must pickle the same in every process."""
    t = type(key)
    if t is str:
        return _digest(key.encode())
    if isinstance(key, tuple):  # a namedtuple equals the plain tuple of its fields
        # int and str elements are hashed in line: a call apiece cost a fifth of the time.
        return hash(
            tuple([k if type(k) is int else _digest(k.encode()) if type(k) is str else key_hash(k) for k in key])
        )
    if t.__hash__ in _NUMBER_HASHES:
        return hash(key) if key == key else 0
    if isinstance(key, frozenset):  # order-free: equal sets may iterate in different orders
        return hash(frozenset([k if type(k) is int else key_hash(k) for k in key]))
    if isinstance(key, str):
        return _digest(key.encode())
    if isinstance(key, bytes):
        return _digest(key)
    numpy = sys.modules.get("numpy")
    if numpy is not None and isinstance(key, numpy.generic):
        item = key.item()  # not hash(key): a datetime64's is randomized
        if isinstance(item, numpy.generic):  # a longdouble or clongdouble: NumPy hashes it as a double, unrandomized
            return hash(key) if key == key else 0
        return key_hash(item)
    return _digest(pickle.dumps(key, protocol=5))


_NUMBER_HASHES = frozenset({int.__hash__, float.__hash__, complex.__hash__, Fraction.__hash__, Decimal.__hash__})


def _digest(data: bytes) -> int:
    return int.from_bytes(blake3_digest(data)[:8], "little")


@remote
def _scatter(
    pairs: Iterable[tuple[Any, Any]],
    partitions: int,
    combine: Callable[[Any, Any], Any] | None,
    phases: bool = False,
    sparse: bool = True,
) -> _serde.Sparse | tuple[dict[Any, Any], ...]:
    out = _timed_split(lambda: pairs, partitions, combine) if phases else _split(pairs, partitions, combine)
    return out if sparse else tuple(out)  # see sparse_readable


class _MapScatter:
    """A map's function and the scatter of the pairs it returns, in one call:
    the pairs are never stored between them (see ``Results.claim``)."""

    def __init__(self, fn: Callable[..., Any], single: bool) -> None:
        self.fn, self.single = fn, single
        self.__module__ = getattr(fn, "__module__", __name__)
        self.__qualname__ = f"{getattr(fn, '__qualname__', type(fn).__name__)}[scatter]"

    def __call__(
        self,
        item: Any,
        partitions: int,
        combine: Callable[[Any, Any], Any] | None,
        phases: bool = False,
        sparse: bool = True,
    ) -> _serde.Sparse | tuple[dict[Any, Any], ...]:
        if phases:
            out = _timed_split(lambda: self.fn(item) if self.single else self.fn(*item), partitions, combine)
        else:
            out = _split(self.fn(item) if self.single else self.fn(*item), partitions, combine)
        return out if sparse else tuple(out)  # see sparse_readable


def _timed_split(make: Callable[[], Any], partitions: int, combine: Callable[[Any, Any], Any] | None) -> _serde.Sparse:
    """``_split`` of the pairs `make()` returns, its phases reported (see _shuffle_phases)."""
    clock = _shuffle_phases.Clock()
    pairs = make()
    clock.lap("map")
    out = _split(pairs, partitions, combine)
    clock.lap("split")
    clock.encode("scatter_encode", out.present.values())
    clock.report("scatter", pairs=len(pairs) if hasattr(pairs, "__len__") else None, buckets=len(out.present))
    return out


# The combines the native scatter folds itself, in the order of its codes
# (see scatter.rs). Any other combine, or all of them with
# CHIMAERA_NATIVE_SCATTER=0, runs _split_py, the reference it matches.
_NATIVE_COMBINES: tuple[Any, ...] = (
    () if os.environ.get("CHIMAERA_NATIVE_SCATTER") == "0" else (None, operator.add, operator.mul, max, min)
)


def _split(
    pairs: Iterable[tuple[Any, Any]], partitions: int, combine: Callable[[Any, Any], Any] | None
) -> _serde.Sparse:
    for code, native in enumerate(_NATIVE_COMBINES):
        if combine is native:
            return _serde.Sparse(partitions, scatter(pairs, partitions, code, bucket_of))
    return _split_py(pairs, partitions, combine)


_EAGER_MAX = 4096  # see _split_py


def _split_py(
    pairs: Iterable[tuple[Any, Any]], partitions: int, combine: Callable[[Any, Any], Any] | None
) -> _serde.Sparse:
    # A bucket for each partition a pair goes to and none for the others: a
    # scatter into a million partitions may touch a few. Up to _EAGER_MAX
    # partitions every bucket is made up front and the empty ones dropped:
    # making each on first use cost the loop 10%. The loops run once per
    # pair, so they are written out in full: a helper call per pair doubled
    # their cost.
    eager = partitions <= _EAGER_MAX
    buckets: Any = [{} for _ in range(partitions)] if eager else collections.defaultdict(dict)
    if combine is None:
        for key, value in pairs:
            buckets[bucket_of(key, partitions)].setdefault(key, []).append(value)
    else:
        for key, value in pairs:
            b = buckets[hash(key) % partitions if type(key) is int else bucket_of(key, partitions)]
            b[key] = combine(b[key], value) if key in b else value
    # Stored value by value: each gather reads only its own.
    return _serde.Sparse(partitions, {i: b for i, b in enumerate(buckets) if b} if eager else dict(buckets))


def sparse_readable(nodes: list[dict[str, Any]]) -> bool:
    """Whether every live peer (``client.nodes()``) reads a stored Sparse
    (see _serde.SPARSE_MAGIC). While one too old to say is live, scatters
    store the tuple a Sparse stands for, in the layouts every peer reads:
    its gathers could read nothing else, nor could its store free the parts."""
    return all(n.get("object_layout", 0) >= _serde.SPARSE_LAYOUT for n in nodes)


@remote
def _manifest(keys: tuple[bytes, ...], phases: bool = False) -> list[tuple[bytes, tuple[Any, ...]]]:
    """Each scatter's key and outcome (where its result is stored), once every
    one has finished: what the gathers read their buckets by. It watches the
    scatters rather than depending on them, so its peer fetches none of their
    results. With `phases`, it reports when it began, saw the last scatter
    finish and returned (see _shuffle_phases)."""
    from ._future import _future_from_key, waiting
    from ._runtime import Runtime

    clock = _shuffle_phases.Clock() if phases else None
    rt = Runtime.get()
    # Its process may be the one that made them (a nested shuffle): their futures there, then.
    futures = [rt._futures.get(k) or _future_from_key(k) for k in keys]
    left = collections.deque(futures)
    with waiting(list(keys)), rt.cond:
        while left:
            if left[0].done():
                left.popleft()
            else:
                rt.cond.wait(1.0)
    if clock is not None:
        clock.lap("wait")
    for f in futures:
        if f._outcome is None or f._outcome[0] != "ok":
            f.result()  # raises the scatter's error, which every gather then fails with
    if clock is not None:
        clock.report("manifest")
    return [(f.key, f._outcome) for f in futures]


@remote
def _gather(
    combine: Callable[[Any, Any], Any] | None,
    manifest: list[tuple[bytes, tuple[Any, ...]]],
    index: int,
    partitions: int,
    phases: bool = False,
    memory: int = 0,
) -> dict[Any, Any]:
    ceiling = _Ceiling(memory, index, partitions)
    if not phases:
        return _merge(combine, _buckets(manifest, index, partitions, None, ceiling))
    clock = _shuffle_phases.Clock()
    out = _merge(combine, _buckets(manifest, index, partitions, clock, ceiling))
    clock.lap("reduce")
    clock.encode("gather_encode", [out])
    clock.report("gather", keys=len(out), rss_growth=ceiling.peak)
    return out


# How often a gather reads its process's private memory (see _resident)
# between buckets: it grows by a few MiB at most in that time. A recorded
# gather reads it after every bucket's read and fold.
CEILING_EVERY_S = 0.01


class _Ceiling:
    """What a gather may grow its process's private memory by while it
    folds: `memory` (reduce_by_key's, which it reserves), else all of its
    peer's. Past that it fails with an error that names partitions=, before
    its peer runs out."""

    def __init__(self, memory: int, index: int, partitions: int) -> None:
        from ._runtime import Runtime

        self.given, self.index, self.partitions = memory, index, partitions
        self.limit = memory or int(Runtime.get().info["memory_bytes"])
        self.base = _resident.resident()
        self.peak, self.at = 0, time.monotonic()

    def check(self, now: bool = False) -> None:
        """Read the memory if CEILING_EVERY_S has passed (or `now`), and fail past the ceiling."""
        t = time.monotonic()
        if self.base is None or not now and t - self.at < CEILING_EVERY_S:
            return
        self.at = t
        grown = (_resident.resident() or 0) - self.base
        self.peak = max(self.peak, grown)
        if grown > self.limit:
            whose = f"memory={self.given}" if self.given else "its peer's memory"
            raise MemoryError(
                f"partition {self.index} of {self.partitions} does not fit its gather: it grew the gather's process "
                f"by {grown >> 20} MiB, past {self.limit >> 20} MiB ({whose}). Give reduce_by_key more partitions= "
                f"than {self.partitions}, so each holds fewer keys."
            )


def _buckets(
    manifest: list[tuple[bytes, tuple[Any, ...]]],
    index: int,
    count: int,
    clock: Any = None,
    ceiling: _Ceiling | None = None,
) -> Iterator[dict[Any, Any]]:
    """Bucket `index` of each result in `manifest`, a stored tuple (or
    Sparse) of `count`, one at a time in the manifest's order, reading only
    that bucket of each. Results stored on other peers are fetched several
    at once first (one after another costs a round trip each). So are the
    buckets kept in files of their own that are not here, once the first of
    them is met: its own and those of every result after it (theirs, and
    those of results an earlier gather on this peer fetched). A result lost
    with every copy is made again by running its scatter. With a `clock`,
    each step is timed (see _shuffle_phases). With a `ceiling`, the memory
    is checked after each bucket is folded and after the last."""
    from ._errors import DataLost
    from ._runtime import Runtime

    rt = Runtime.get()

    def path(run: bytes, obj: bytes) -> str:
        return _serde.object_path(rt.object_dir, run, obj)

    def fetch(objects: Sequence[tuple[bytes, bytes, bytes | None]]) -> None:
        _fetch(rt, objects)
        if clock is not None:
            clock.lap("fetch")

    def apart(entries: list[tuple[bytes, tuple[Any, ...]]]) -> list[tuple[bytes, bytes, None]]:
        """The buckets of these results kept in files of their own that are not here."""
        out = []
        for _, (_, inline, run, obj, _, _) in entries:
            part = None if inline is not None else _serde.part_of(path(run, obj), index, count)
            if part is not None and not os.path.exists(path(run, part)):
                out.append((run, part, None))
        return out

    away = [
        (o[2], o[3], key) for key, o in manifest if o[1] is None and not os.path.exists(path(o[2], o[3]))
    ]
    try:
        fetch(away)
        fetched = False  # the buckets kept apart: once fetched, any still missing is fetched alone
        for i, (_, (_, inline, run, obj, _, _)) in enumerate(manifest):
            rdir = _serde.run_dir(rt.object_dir, run)
            if inline is not None:
                bucket = _serde.loads_part(inline, index, count, rdir)
            else:
                try:
                    bucket = _serde.read_part(path(run, obj), index, count, rdir, fetched)
                except _serde.PartAway:
                    if clock is not None:
                        clock.lap("read")
                    fetch(apart(manifest[i:]))
                    fetched = True
                    bucket = _serde.read_part(path(run, obj), index, count, rdir)
            if clock is not None:
                clock.lap("read")
                clock.count("read", _shuffle_phases.bucket_size(inline, path(run, obj), index, count))
                if ceiling is not None:
                    ceiling.check(now=True)
            yield bucket
            del bucket  # folded: the next one is read without it
            if clock is not None:
                clock.lap("reduce")
            if ceiling is not None:
                ceiling.check(now=clock is not None)
        if ceiling is not None:
            ceiling.check(now=True)
    except FileNotFoundError as e:
        raise DataLost(f"a scatter's result is no longer stored on any live peer: {e}") from e


_fetcher: concurrent.futures.ThreadPoolExecutor | None = None


def _fetch(rt: Any, objects: Sequence[tuple[bytes, bytes, bytes | None]]) -> None:
    """Bring these (run, object, call that made it) into this peer's store,
    several at once, as shuffle traffic (their holders pace it below a call's
    reads)."""
    global _fetcher
    if len(objects) < 2:
        for m in objects:
            rt.localize(*m, traffic="shuffle")
        return
    if _fetcher is None:
        from ._executor import PART_FETCHES

        _fetcher = concurrent.futures.ThreadPoolExecutor(PART_FETCHES, thread_name_prefix="chimaera-buckets")
    list(_fetcher.map(lambda m: rt.localize(*m, traffic="shuffle"), objects))


def _merge(combine: Callable[[Any, Any], Any] | None, buckets: Iterable[dict[Any, Any]]) -> dict[Any, Any]:
    """One partition from its bucket of every scatter, folded in scatter
    order as each comes: none is kept once it is folded."""
    out: dict[Any, Any] = {}
    if combine is None:
        for bucket in buckets:
            for key, values in bucket.items():
                out.setdefault(key, []).extend(values)
            del bucket
        return out
    for bucket in buckets:
        if not out:
            out.update(bucket)  # copied whole: the inputs are not ours to change
        else:
            for key, value in bucket.items():
                out[key] = combine(out[key], value) if key in out else value
        del bucket
    return out


# A partition takes about this many times the bytes its buckets were
# stored in, as the dict a gather makes: 5 to 12 times for int, str and
# tuple keys with int, float and list values (Python objects against the
# plain codec, measured with tracemalloc).
DECODED_PER_STORED = 12
# A partition and its bucket are planned to fill one HEADROOM-th of their
# reducer's memory. The rest is for what that estimate leaves out and the
# gather's ceiling counts (see _Ceiling): decode buffers, a dict's old and
# new tables while it grows, the allocator's slack. A gather planned to fill
# its memory went past it in one run of four.
HEADROOM = 2


def partitions_for(inputs: int, input_bytes: int, nodes: list[dict[str, Any]], memory: int = 0) -> int:
    """How many partitions ``reduce_by_key`` makes when not told: one per
    input, up to the cores of the live peers (the cluster's: a small host's
    script still spreads over it), and more when that many would not each
    fit a reducer with HEADROOM. A reducer has `memory` (reduce_by_key's),
    or else the memory per core of the peer with the least: a gather holds
    its partition and one bucket (about the partition over the inputs),
    beside a call on each other core of its peer. `input_bytes` is what the
    inputs are stored as (0 while unknown). The fit assumes no key repeats,
    the most a partition can hold: a combine that folds many pairs into one
    key needs fewer partitions than it gives, which costs more calls, never
    memory."""
    live = [n for n in nodes if n["alive"] and not n.get("draining")]
    cores = max(1, sum(max(1, int(n["cpus"])) for n in live))
    per_core = memory or min((n["memory_bytes"] / max(1, int(n["cpus"])) for n in live if n["memory_bytes"]), default=0)
    held = input_bytes * DECODED_PER_STORED * (inputs + 1)  # over `inputs`: the partitions and a bucket of each
    fit = -(-held * HEADROOM // (max(1, inputs) * int(per_core))) if per_core >= 1 else 1
    return max(1, min(inputs, cores), fit)


def stored_bytes(inputs: list[Any]) -> int:
    """What `inputs` are stored as, from those already done (scaled to all
    of them); 0 when none is done or they are not futures."""
    sizes = [f._outcome[4] for f in inputs if isinstance(f, Future) and f._outcome and f._outcome[0] == "ok"]
    return sum(sizes) * len(inputs) // len(sizes) if sizes else 0


def shuffle(
    inputs: list[Future[Any]], partitions: int, combine: Callable[[Any, Any], Any] | None = None, memory: int = 0
) -> list[Future[dict[Any, Any]]]:
    """Partition the pairs of `inputs` by key into `partitions` outputs.

    With `combine` (associative), each key's values are folded into one;
    without it, each key maps to the list of its values. A gather reserves
    `memory` and fails once it grows by more (see _Ceiling).
    """
    from ._runtime import Runtime

    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    record, t = _shuffle_phases.current, time.time()
    phases, sparse = record is not None, sparse_readable(_width.live_nodes(Runtime.get()))
    split = _scatter.options(returns=partitions)
    scattered = [split.remote(f, partitions, combine, phases, sparse) for f in inputs]
    manifest, gathers = _gathers(scattered, partitions, combine, inputs, phases, memory)
    if record is not None:
        record.add(t, manifest, scattered, gathers)
    return gathers


def shuffle_mapped(
    fn: Callable[..., Any],
    eff: Any,
    single: bool,
    items: list[Any],
    partitions: int,
    combine: Callable[[Any, Any], Any] | None = None,
    memory: int = 0,
) -> list[Future[dict[Any, Any]]]:
    """``shuffle`` of `fn` applied to each item (a map nobody read: see
    ``Results.claim``), with `fn` run inside the scatter calls, under the
    map's options."""
    from ._options import Effective
    from ._remote import CALL_FUNCTION, submit_call
    from ._runtime import Runtime

    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    record, t = _shuffle_phases.current, time.time()
    rt = Runtime.get()
    phases, sparse = record is not None, sparse_readable(_width.live_nodes(rt))
    runner = _MapScatter(fn, single)
    split = Effective({**eff.values, "returns": partitions}, {**eff.sources, "returns": "reduce_by_key"})
    scattered, reads = [], []
    for item in items:
        f = submit_call(rt, runner, (item, partitions, combine, phases, sparse), {}, kind=CALL_FUNCTION, eff=split)
        scattered.append(f)
        reads.append(rt._reads.get(f.key))  # the futures and values it reads (until its outcome)
    manifest, gathers = _gathers(scattered, partitions, combine, reads, phases, memory)
    if record is not None:
        record.add(t, manifest, scattered, gathers)
    return gathers


def _gathers(
    scattered: list[Future[Any]], partitions: int, combine: Any, inputs: list[Any], phases: bool, memory: int
) -> tuple[Future[Any], list[Future[Any]]]:
    """The manifest and one gather per partition, each depending on the
    manifest alone: the driver and the shard keep inputs + partitions edges,
    where gathers that each depended on every scatter made inputs ×
    partitions. `inputs` is what the scatters read. Each gather reserves
    `memory` when it is given."""
    from ._remote import _release_reads
    from ._runtime import Runtime

    rt = Runtime.get()
    manifest = _manifest.remote(tuple(f.key for f in scattered), phases)
    gather = _gather.options(memory=memory) if memory else _gather
    gathers = [gather.remote(combine, manifest, j, partitions, phases, memory) for j in range(partitions)]
    # A gather reads the scatters' results without depending on them. They
    # stay as its dependencies' would: until its outcome, and while its
    # future lives, with the scatters' inputs (lineage: a lost partition is
    # made again from them). The scatters' futures go now: each outcome
    # holds a table of every bucket, inputs × partitions in all.
    held = _Held(rt, [f.key for f in scattered], inputs)
    for f in scattered:
        f._escaped = True
    for g in gathers:
        reads = rt._reads.get(g.key)  # every gather reads the manifest: there until its outcome
        if reads is not None:
            reads.append(held)
        weakref.finalize(g, _release_reads, [held]).atexit = False
    return manifest, gathers


class _Held:
    """Results some calls read without depending on them, and what made
    them: unpinned, as a dropped future's are, once nothing holds this."""

    def __init__(self, rt: Any, keys: list[bytes], inputs: list[Any]) -> None:
        self.rt, self.keys, self.inputs = rt, keys, inputs

    def __del__(self) -> None:
        if sys.is_finalizing() or self.rt.forked:
            return
        for key in self.keys:
            self.rt.release_later(key)
