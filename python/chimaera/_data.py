"""Partitioned data: ``ch.data.partitions`` inputs, ``reduce_by_key`` and sinks.

    counts = count_words.map(ch.data.partitions("logs/*.jsonl", columns=["text"]))
    totals = counts.reduce_by_key(operator.add)        # Partitioned
    manifest = totals.write(ext.FileSink("out/")).result()

A map over partitions runs one call per partition, on any peer. Results stay
in the object store: ``reduce_by_key`` repartitions them by key between calls
(see ``_shuffle``), and sinks write each partition from the worker that holds
it. Only what you ask for (``to_dict()``, ``items()``, a manifest) comes back.
"""

from __future__ import annotations

import secrets
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

from . import _shuffle, _units, _width
from ._future import Future, Results, put
from ._remote import remote
from .ext import FileSink, FileSource, Sink, Source, _check_filter, _keeper

# ---- sources ------------------------------------------------------------------


class Partition:
    """One partition of a source, as a mapped function receives it."""

    __slots__ = ("source", "part", "columns", "filter")

    def __init__(self, source: Source, part: Any, columns: list[str] | None, filter: Any) -> None:  # noqa: A002
        self.source, self.part, self.columns, self.filter = source, part, columns, filter

    def _plan(self) -> tuple[list[str] | None, Any]:
        """The columns to read, which include the filter's, and the filter
        left to apply after reading: what the source does not apply itself."""
        columns, residual = self.columns, self.filter
        if callable(self.filter):
            columns = None  # a callable may look at any field
        elif self.filter is not None:
            if columns is not None:
                columns = list(dict.fromkeys([*columns, *(col for col, _, _ in self.filter)]))
            residual = self.source.residual(self.part, self.filter)
        return columns, residual

    def rows(self) -> Iterator[Any]:
        columns, residual = self._plan()
        keep = _keeper(residual)
        for row in self.source.read(self.part, columns=columns, filter=self.filter):
            if residual and not keep(row):
                continue
            yield _project(row, self.columns)

    __iter__ = rows

    def batches(self) -> Iterator[Any]:
        """The rows ``rows()`` gives, in the same order, as Arrow record
        batches (``pyarrow.RecordBatch``) that keep the source's column types,
        one batch in memory at a time. For a source that reads columns, such as
        Parquet files; others raise ``TypeError``."""
        columns, residual = self._plan()
        keep = _keeper(residual)
        for batch in self.source.batches(self.part, columns=columns, filter=self.filter):
            if residual:
                import pyarrow as pa

                # Rows only to decide which to keep: the batch stays columnar.
                view = batch if callable(residual) else batch.select(list(dict.fromkeys(c for c, _, _ in residual)))
                batch = batch.filter(pa.array([keep(row) for row in view.to_pylist()], pa.bool_()))
            yield batch if self.columns is None else batch.select(self.columns)

    def __repr__(self) -> str:
        return f"<Partition {self.part!r} of {type(self.source).__name__}>"


def _project(row: Any, columns: list[str] | None) -> Any:
    if columns is None or not isinstance(row, dict):
        return row
    return {c: row.get(c) for c in columns}


class ItemsSource(Source):
    """A Python sequence split into chunks; each chunk is stored once
    (``ch.put``). A partition's descriptor is its chunk's future, so a call
    carries its own chunk and none of the others."""

    def __init__(self, items: Sequence[Any], n: int) -> None:
        size = max(1, -(-len(items) // max(1, n)))
        self.chunks = [put(list(items[i : i + size])) for i in range(0, len(items), size)]

    def __getstate__(self) -> dict[str, Any]:
        return {}  # pickled inside every Partition: the chunks stay with the driver

    def partitions(self) -> list[Any]:
        return list(self.chunks)

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        return part.result()


def partitions(
    source: Source | str | Sequence[Any],
    *,
    n: int | None = None,
    columns: list[str] | None = None,
    filter: Any = None,  # noqa: A002
) -> list[Partition]:
    """Partitions to ``fn.map`` over: an ``ext.Source``, a file glob or
    directory (one partition per file, several for a large one; see
    ``ext.FileSource``), or a sequence split into `n` chunks.

    `columns` keeps only those fields of dict rows; `filter` is a list of
    ``(column, op, value)`` triples (pushed down where the source can) or a
    callable on each row. A triple keeps the same rows in every format: a
    missing field is null, which only ``in`` and ``not in`` test, and a field
    of another type than the value raises ``TypeError`` (see ``ext``).
    """
    _check_filter(filter)
    if isinstance(source, str):
        source = FileSource(source)
    elif not isinstance(source, Source):
        from ._runtime import Runtime

        source = ItemsSource(source, n or max(1, int(_width._live_capacity(Runtime.get())[0])))
    return [Partition(source, part, columns, filter) for part in source.partitions()]


# ---- sinks -------------------------------------------------------------------


@remote
def _write(sink: Sink, part_id: int, items: Iterable[Any]) -> Any:
    # An Arrow table or record batch goes to the sink a batch at a time (of
    # the rows FileSource reads at once), never as Python rows. One that came
    # here imported pyarrow as it was unpickled.
    arrow = sys.modules.get("pyarrow")
    if arrow is not None and isinstance(items, (arrow.Table, arrow.RecordBatch)):
        return sink.write_batches(part_id, arrow.table(items).to_reader(max_chunksize=65536))
    # A reduce_by_key partition is a dict: its rows are its (key, value) pairs.
    return sink.write(part_id, list(items.items()) if isinstance(items, dict) else list(items))


@remote
def _commit(sink: Sink, *receipts: Any) -> Any:
    return sink.commit(list(receipts))


_key_lock = threading.Lock()
_last_key_ns = 0


def _write_key() -> str:
    """A key unique to one write, that sorts by when the write began. A
    process's keys never go back, even when its clock steps back."""
    global _last_key_ns
    with _key_lock:
        ns = _last_key_ns = max(time.time_ns(), _last_key_ns + 1000)
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime(ns // 10**9))
    return f"{stamp}{ns // 1000 % 10**6:06d}Z-{secrets.token_hex(4)}"


def write(futures: list[Future[Any]], sink: Sink) -> Future[Any]:
    """Write each future's items as one partition, then commit.

    The sink begins with a key unique to this write that sorts by when it
    began; it travels inside the pickled sink, so every retry sees the same
    one. The sink's ``effects`` decide a commit whose worker died: run again
    ("idempotent") or ``ch.OutcomeUnknown`` ("external")."""
    sink = sink.begin(_write_key())
    receipts = [_write.remote(sink, i, f) for i, f in enumerate(futures)]
    return _commit.options(effects=sink.effects).remote(sink, *receipts)


# ---- partitioned results -----------------------------------------------------------


# How many partitions an iterator over a Partitioned brings to this machine
# ahead of the one it yields. They mostly live on other peers, and reading
# them one after another costs a round trip each: 64 partitions on four
# machines arrived 40 ms after the last gather finished. Only localized ahead,
# never decoded: one partition is in the driver's memory at a time. Decided
# here alone, so a byte cap can bound the same window.
READ_AHEAD = 8


def _read_ahead(futures: Sequence[Future[Any]]) -> int:
    """The read-ahead window for iterating `futures`."""
    return min(READ_AHEAD, max(0, len(futures) - 1))


def _in_order(futures: Sequence[Future[Any]]) -> Iterator[Any]:
    """Each future's value, in order, with the stored bytes of the next
    ``_read_ahead`` made local in the background. A failed one raises at its
    own position; stopping early stops the background work."""
    window = _read_ahead(futures)
    if window == 0:
        for f in futures:
            yield f.result()
        return
    import concurrent.futures

    from ._future import wait

    pool = concurrent.futures.ThreadPoolExecutor(window, thread_name_prefix="chimaera-read-ahead")

    def localize(f: Future[Any]) -> None:
        try:
            wait([f], fetch_local=True)
        except Exception:  # noqa: BLE001 -- reading it in order raises the real error at its place
            pass

    started = 0
    try:
        for i, f in enumerate(futures):
            while started < len(futures) and started <= i + window:
                pool.submit(localize, futures[started])
                started += 1
            yield f.result()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


class Partitioned:
    """Results split into partitions that stay on the workers: the output of
    ``reduce_by_key``. Each partition is a dict from key to its value (to the
    list of its values without a combine); a key is in one partition only."""

    def __init__(self, futures: list[Future[dict[Any, Any]]]) -> None:
        self.futures = futures

    def __len__(self) -> int:
        return len(self.futures)

    def __iter__(self) -> Iterator[dict[Any, Any]]:
        """Each partition's dict, in partition order (brings them to the
        driver, one partition at a time, fetching the next few meanwhile)."""
        yield from _in_order(self.futures)

    def __repr__(self) -> str:
        return f"<Partitioned {len(self.futures)} partitions>"

    def items(self) -> Iterator[tuple[Any, Any]]:
        """Every (key, value), partition by partition (brings them to the driver)."""
        for part in _in_order(self.futures):
            yield from part.items()

    def to_dict(self) -> dict[Any, Any]:
        out: dict[Any, Any] = {}
        for part in self:
            out.update(part)
        return out

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        """Apply a remote function to each partition's dict."""
        return fn.map(iter(self.futures), ordered=ordered)

    def write(self, sink: Sink) -> Future[Any]:
        return write(self.futures, sink)

    def write_parquet(self, directory: str) -> Future[Any]:
        return write(self.futures, FileSink(directory, format="parquet"))


def reduce_by_key(
    results: Results[Any] | Iterable[Future[Any]],
    combine: Callable[[Any, Any], Any] | None = None,
    *,
    partitions: int | None = None,
    memory: int | str | None = None,
) -> Partitioned:
    """Group the (key, value) pairs that each result yields by key, folding each
    key's values with `combine` (associative; without it, values become lists).
    `memory` is what each gather may take for its partition: it is reserved,
    the default partition count fits it, and a gather that outgrows it fails."""
    from ._runtime import Runtime

    ceiling = 0 if memory is None else _units.parse_bytes(memory, "memory")
    if memory is not None and ceiling < 1:
        raise ValueError(f"memory= is what each gather may take, more than 0 bytes, not {memory!r}")

    def parts(inputs: list[Any]) -> int:
        if partitions is not None:
            return partitions
        nodes = _width.live_nodes(Runtime.get())
        return _shuffle.partitions_for(len(inputs), _shuffle.stored_bytes(inputs), nodes, ceiling)

    # A function's map nobody has read: its function runs inside the scatter
    # calls, and the pairs it returns are never stored in between.
    claimed = results.claim() if isinstance(results, Results) else None
    if claimed is not None:
        (fn, eff, single), source = claimed
        items = list(source)
        return Partitioned(_shuffle.shuffle_mapped(fn, eff, single, items, parts(items), combine, ceiling))
    futures = list(results.futures()) if isinstance(results, Results) else list(results)
    return Partitioned(_shuffle.shuffle(futures, parts(futures), combine, ceiling))
