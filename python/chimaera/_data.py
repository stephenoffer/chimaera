"""Partitioned data: ``ch.partitions`` inputs, ``reduce_by_key`` and sinks.

    counts = count_words.map(ch.partitions("logs/*.jsonl", columns=["text"]))
    totals = counts.reduce_by_key(operator.add)        # Partitioned
    manifest = totals.write(ext.FileSink("out/")).result()

A map over partitions runs one call per partition, on any peer. Results stay
in the object store: ``reduce_by_key`` repartitions them by key between calls
(see ``_shuffle``), and sinks write each partition from the worker that holds
it. Only what you ask for (``to_dict()``, ``items()``, a manifest) comes back.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

from . import _shuffle
from ._future import Future, Results, put
from ._remote import remote
from .ext import FileSink, FileSource, Sink, Source

# ---- sources ------------------------------------------------------------------


class Partition:
    """One partition of a source, as a mapped function receives it."""

    __slots__ = ("source", "part", "columns", "filter")

    def __init__(self, source: Source, part: Any, columns: list[str] | None, filter: Any) -> None:  # noqa: A002
        self.source, self.part, self.columns, self.filter = source, part, columns, filter

    def rows(self) -> Iterator[Any]:
        rows = self.source.read(self.part, columns=self.columns, filter=self.filter)
        for row in rows:
            if self.filter is not None and not _keep(row, self.filter):
                continue
            yield _project(row, self.columns)

    __iter__ = rows

    def __repr__(self) -> str:
        return f"<Partition {self.part!r} of {type(self.source).__name__}>"


_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "in": lambda a, b: a in b,
    "not in": lambda a, b: a not in b,
}


def _keep(row: Any, filter: Any) -> bool:  # noqa: A002
    if callable(filter):
        return bool(filter(row))
    return all(_OPS[op](row.get(col), value) for col, op, value in filter)


def _project(row: Any, columns: list[str] | None) -> Any:
    if columns is None or not isinstance(row, dict):
        return row
    return {c: row.get(c) for c in columns}


class ItemsSource(Source):
    """A Python sequence split into chunks; each chunk is stored once (``ch.put``)."""

    def __init__(self, items: Sequence[Any], n: int) -> None:
        size = max(1, -(-len(items) // max(1, n)))
        self.chunks = [put(list(items[i : i + size])) for i in range(0, len(items), size)]

    def partitions(self) -> list[Any]:
        return list(range(len(self.chunks)))

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        return self.chunks[part].result()


def partitions(
    source: Source | str | Sequence[Any],
    *,
    n: int | None = None,
    columns: list[str] | None = None,
    filter: Any = None,  # noqa: A002
) -> list[Partition]:
    """Partitions to ``fn.map`` over: an ``ext.Source``, a file glob or
    directory (one partition per file), or a sequence split into `n` chunks.

    `columns` keeps only those fields of dict rows; `filter` is a list of
    ``(column, op, value)`` triples (pushed down where the source can) or a
    callable on each row.
    """
    if isinstance(source, str):
        source = FileSource(source)
    elif not isinstance(source, Source):
        from ._runtime import Runtime

        source = ItemsSource(source, n or max(1, int(Runtime.get().info["usable_cores"])))
    return [Partition(source, part, columns, filter) for part in source.partitions()]


# ---- sinks -------------------------------------------------------------------


@remote
def _write(sink: Sink, part_id: int, items: Iterable[Any]) -> Any:
    return sink.write(part_id, list(items))


@remote
def _commit(sink: Sink, *receipts: Any) -> Any:
    return sink.commit(list(receipts))


def write(futures: list[Future[Any]], sink: Sink) -> Future[Any]:
    """Write each future's items as one partition, then commit once."""
    receipts = [_write.remote(sink, i, f) for i, f in enumerate(futures)]
    return _commit.remote(sink, *receipts)


# ---- partitioned results -----------------------------------------------------------


class Partitioned:
    """Results split into partitions that stay on the workers: the output of
    ``reduce_by_key``. Each partition is a list of ``(key, value)`` pairs."""

    def __init__(self, futures: list[Future[list[tuple[Any, Any]]]]) -> None:
        self.futures = futures

    def __len__(self) -> int:
        return len(self.futures)

    def __iter__(self) -> Iterator[list[tuple[Any, Any]]]:
        """Each partition's list of (key, value) pairs, in partition order
        (brings them to the driver, one partition at a time)."""
        for f in self.futures:
            yield f.result()

    def __repr__(self) -> str:
        return f"<Partitioned {len(self.futures)} partitions>"

    def items(self) -> Iterator[tuple[Any, Any]]:
        """Every (key, value), partition by partition (brings them to the driver)."""
        for f in self.futures:
            yield from f.result()

    def to_dict(self) -> dict[Any, Any]:
        return dict(self.items())

    def map(self, fn: Any, *, ordered: bool = True) -> Results[Any]:
        """Apply a remote function to each partition's list of pairs."""
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
) -> Partitioned:
    """Group the (key, value) pairs that each result yields by key, folding each
    key's values with `combine` (associative; without it, values become lists)."""
    futures = list(results.futures()) if isinstance(results, Results) else list(results)
    if partitions is None:
        from ._runtime import Runtime

        partitions = max(1, min(len(futures), int(Runtime.get().info["usable_cores"])))
    return Partitioned(_shuffle.shuffle(futures, partitions, combine))
