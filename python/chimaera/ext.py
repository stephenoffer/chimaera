"""Extension points: plug your own data sources, sinks and serializers into Chimaera.

    from chimaera import ext

    class Shards(ext.Source):
        def partitions(self): ...            # small, picklable descriptors
        def read(self, part, columns=None, filter=None): ...   # rows of one partition

    class Warehouse(ext.Sink):
        def write(self, part_id, items): ...  # idempotent per part; returns a receipt
        def commit(self, receipts): ...       # called once, with every receipt

Sources feed ``ch.partitions(source)``; sinks receive ``Results.write(sink)``
and ``reduce_by_key(...).write(sink)``. Both are pickled to the workers that
read and write, so keep their state small (paths, URIs, credentials names).
"""

from __future__ import annotations

import copyreg
import csv
import glob
import json
import os
import tempfile
from collections.abc import Callable, Iterable
from typing import Any


class Source:
    """Inputs that peers read independently, one partition per call."""

    def partitions(self) -> list[Any]:
        """Descriptors of the partitions: small and picklable (a path, a byte range)."""
        raise NotImplementedError

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        """The rows of one partition. `columns` and `filter` are hints a source
        may push down; Chimaera applies them afterwards either way."""
        raise NotImplementedError


class Sink:
    """Where results go, exactly once: each partition's ``write`` may run more
    than once (a retried call), so it must be idempotent; ``commit`` runs once
    with every partition's receipt, in partition order."""

    def write(self, part_id: int, items: list[Any]) -> Any:
        raise NotImplementedError

    def commit(self, receipts: list[Any]) -> Any:
        return receipts


def register_serializer(cls: type, *, dumps: Callable[[Any], bytes], loads: Callable[[bytes], Any]) -> None:
    """Pickle instances of `cls` with your own `dumps`/`loads`.

    Loading needs nothing registered: the pickle names `loads`, so `loads`
    must be importable (a module-level function, not a lambda). Dumping needs
    the registration in every process that sends such objects, so call this
    at the import of a module the driver and the workers both import.
    """
    copyreg.pickle(cls, lambda obj: (loads, (dumps(obj),)))


# ---- built-in sources and sinks ---------------------------------------------


class FileSource(Source):
    """Files on a filesystem every peer can read, one partition per file.

    By extension: ``.jsonl``/``.ndjson`` yield dicts, ``.csv`` yields dicts of
    strings, ``.parquet`` yields dicts through pyarrow (with column and filter
    pushdown), anything else yields lines.
    """

    def __init__(self, pattern: str) -> None:
        if os.path.isdir(pattern):
            pattern = os.path.join(pattern, "*")
        self.files = sorted(f for f in glob.glob(pattern) if os.path.isfile(f))
        if not self.files:
            raise FileNotFoundError(f"no files match {pattern!r}")

    def partitions(self) -> list[Any]:
        return list(self.files)

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        path: str = part
        if path.endswith((".jsonl", ".ndjson")):
            with open(path) as f:
                return [json.loads(line) for line in f if line.strip()]
        if path.endswith(".csv"):
            with open(path, newline="") as f:
                return list(csv.DictReader(f))
        if path.endswith(".parquet"):
            try:
                import pyarrow.dataset as ds
            except ImportError:
                raise ImportError("reading .parquet files needs pyarrow: pip install pyarrow") from None
            pushed = None if filter is None or callable(filter) else _arrow_filter(filter)
            return ds.dataset(path).to_table(columns=columns, filter=pushed).to_pylist()
        with open(path) as f:
            return [line.rstrip("\n") for line in f]


def _arrow_filter(filter: Any) -> Any:  # noqa: A002
    import pyarrow.dataset as ds

    expr = None
    for col, op, value in filter:
        f = ds.field(col)
        term = {
            "==": f == value,
            "!=": f != value,
            "<": f < value,
            "<=": f <= value,
            ">": f > value,
            ">=": f >= value,
            "in": f.isin(value),
        }.get(op)
        if term is None:
            return None  # not pushable; applied after reading
        expr = term if expr is None else expr & term
    return expr


class FileSink(Sink):
    """One file per partition in `directory`, plus ``_manifest.json`` at commit.

    Each part is written to a temporary name and renamed, so a retried write
    replaces it whole. ``format`` is ``"jsonl"`` (one JSON value per line) or
    ``"parquet"`` (dict rows, through pyarrow).
    """

    def __init__(self, directory: str, format: str = "jsonl") -> None:  # noqa: A002
        if format not in ("jsonl", "parquet"):
            raise ValueError(f"format must be 'jsonl' or 'parquet', got {format!r}")
        self.directory, self.format = directory, format

    def write(self, part_id: int, items: list[Any]) -> Any:
        os.makedirs(self.directory, exist_ok=True)
        path = os.path.join(self.directory, f"part-{part_id:05d}.{self.format}")
        fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=".part-")
        os.close(fd)
        if self.format == "jsonl":
            with open(tmp, "w") as f:
                for item in items:
                    f.write(json.dumps(item, default=_jsonable) + "\n")
        else:
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
            except ImportError:
                os.unlink(tmp)
                raise ImportError("writing parquet needs pyarrow: pip install pyarrow") from None
            pq.write_table(pa.Table.from_pylist([_row(i) for i in items]), tmp)
        os.replace(tmp, path)
        return {"part": part_id, "path": path, "rows": len(items)}

    def commit(self, receipts: list[Any]) -> Any:
        manifest = {"parts": receipts, "rows": sum(r["rows"] for r in receipts)}
        path = os.path.join(self.directory, "_manifest.json")
        with open(path + ".tmp", "w") as f:
            json.dump(manifest, f)
        os.replace(path + ".tmp", path)
        return manifest


def _jsonable(x: Any) -> Any:
    if isinstance(x, (set, frozenset, tuple)):
        return list(x)
    raise TypeError(f"{type(x).__name__} is not JSON serializable")


def _row(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item
    if isinstance(item, tuple) and len(item) == 2:
        return {"key": item[0], "value": item[1]}
    return {"value": item}
