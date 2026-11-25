"""Extension points: plug your own data sources, sinks and serializers into Chimaera.

    from chimaera import ext

    class Shards(ext.Source):
        def partitions(self): ...            # small, picklable descriptors
        def read(self, part, columns=None, filter=None): ...   # rows of one partition

    class Warehouse(ext.Sink):
        def write(self, part_id, items): ...  # idempotent per part; returns a receipt
        def write_batches(self, part_id, batches): ...  # optional: an Arrow partition, batch by batch
        def commit(self, receipts): ...       # every receipt; at most once unless effects says otherwise

Sources feed ``ch.data.partitions(source)``; sinks receive ``Results.write(sink)``
and ``reduce_by_key(...).write(sink)``. Both are pickled to the workers that
read and write, so keep their state small (paths, URIs, credentials names).
"""

from __future__ import annotations

import contextlib
import copy
import copyreg
import csv
import datetime
import decimal
import fcntl
import glob
import hashlib
import hmac
import json
import numbers
import operator
import os
import posixpath
import secrets
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from typing import Any, BinaryIO, Literal

from ._errors import DataLost, VersionConflict


class Source:
    """Inputs that peers read independently, one partition per call."""

    def partitions(self) -> list[Any]:
        """Descriptors of the partitions: small and picklable (a path, a byte range)."""
        raise NotImplementedError

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        """The rows of one partition. `columns` and `filter` are hints a source
        may push down. `columns` includes every column the filter names;
        Chimaera applies what ``residual`` returns, then keeps only the
        columns the caller asked for."""
        raise NotImplementedError

    def residual(self, part: Any, filter: list[Any]) -> list[Any]:  # noqa: A002
        """The ``(column, op, value)`` triples of `filter` that ``read`` does
        not apply exactly for `part`, which Chimaera applies after reading.
        All of them unless you override it for the ones ``read`` pushes down."""
        return filter

    def batches(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        """The rows ``read`` gives, as Arrow record batches, for a source
        that reads columns. It takes the same hints as ``read``."""
        raise TypeError(f"{type(self).__name__} reads rows, not Arrow batches")


class Sink:
    """Where results go. ``begin`` runs first, on the driver, with a key for
    this write; each partition's ``write`` may run more than once (a retried
    call), so it must be idempotent; ``commit`` gets every partition's
    receipt, in partition order.

    ``effects`` says what happens when ``commit``'s worker dies mid-call.
    ``"external"`` (the default): commit starts at most once, and the write's
    future raises ``ch.OutcomeUnknown`` if it may have taken effect.
    ``"idempotent"``: commit runs again, with the same key, and must take
    effect once per key (``FileSink`` does).
    """

    effects: Literal["none", "idempotent", "external"] = "external"

    def begin(self, key: str) -> Sink:
        """The sink the workers get for one write. `key` is unique to the
        write and the same in every retry of its calls; return a copy that
        keeps it, to name the write's output or publish it once."""
        return self

    def write(self, part_id: int, items: list[Any]) -> Any:
        raise NotImplementedError

    def write_batches(self, part_id: int, batches: Any) -> Any:
        """Write a partition that is an Arrow table or record batch, and give
        its receipt as ``write`` does. `batches` is a
        ``pyarrow.RecordBatchReader``: iterate it for the record batches, one
        in memory at a time, each of its ``schema``. The default reads them
        all and hands ``write`` their rows; override it to stream them, as
        ``FileSink`` does."""
        return self.write(part_id, batches.read_all().to_pylist())

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


def register_reducer(cls: type, reduce: Callable[[Any], tuple[Any, ...]]) -> None:
    """Pickle instances of `cls` as ``reduce(obj)`` returns (a callable and
    its arguments, as ``__reduce__`` does) in the values Chimaera stores and
    sends, and only there: a plain ``pickle.dumps`` elsewhere in the program
    is untouched, unlike `register_serializer` (copyreg). The callable may be
    a lambda or a local function: it travels inside each value."""
    from . import _serde

    _serde.reducers[cls] = reduce


def unregister_reducer(cls: type) -> None:
    """Pickle `cls` as before `register_reducer`."""
    from . import _serde

    _serde.reducers.pop(cls, None)


# ---- filters -----------------------------------------------------------------
#
# A filter keeps the same rows whatever the format. A missing field is null,
# and null compares with nothing: ==, !=, <, <=, > and >= skip it, `in` keeps
# it when the list holds None and `not in` when the list doesn't. A field and
# the filter's value compare when both are numbers (a bool is not one), both
# bools, or both of one other type (str, bytes, a date; a datetime or time with
# a time zone and one without are two types); otherwise the filter raises
# TypeError, as Arrow does for a column of another type.

_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "in": lambda a, b: a in b,
    "not in": lambda a, b: a not in b,
}
_NUMBERS = (numbers.Real, decimal.Decimal)
_SCALARS = (*_NUMBERS, str, bytes, datetime.date, datetime.time, datetime.timedelta)
_AWARE = {datetime.datetime: "datetime with a time zone", datetime.time: "time with a time zone"}
_KINDS: dict[type, Any] = {}  # the kind of each type seen, as filters test every row


def _kind(x: Any) -> Any:
    """What a filter compares `x` with: any number for a number, any datetime
    (or time) with a time zone for one with a time zone, else its own type."""
    kind = _KINDS.get(type(x))
    if kind is not None:
        return kind
    if isinstance(x, (datetime.datetime, datetime.time)):  # not kept: its kind depends on its value
        base = datetime.datetime if isinstance(x, datetime.datetime) else datetime.time
        return _AWARE[base] if x.utcoffset() is not None else base
    if isinstance(x, bool):
        kind = bool
    else:
        kind = numbers.Number if isinstance(x, _NUMBERS) else type(x)
    _KINDS[type(x)] = kind
    return kind


def _sample(op: str, value: Any) -> Any:
    """A value of the kind the filter compares fields with (None: only null)."""
    return next((v for v in value if v is not None), None) if op in ("in", "not in") else value


def _check_filter(filter: Any) -> None:  # noqa: A002
    """Raise for a filter that would not keep the same rows in every format."""
    if filter is None or callable(filter):
        return
    for triple in filter:
        col, op, value = triple
        if op not in _OPS:
            raise ValueError(f"filter {triple!r}: the ops are {', '.join(map(repr, _OPS))}")
        if op in ("in", "not in"):
            if not isinstance(value, (list, tuple, set, frozenset)):
                raise TypeError(f"filter {triple!r}: {op!r} takes a list of values")
            values = [v for v in value if v is not None]
        elif value is None:
            use = f"({col!r}, 'in', [None]) or ({col!r}, 'not in', [None])"
            raise ValueError(f"filter {triple!r} keeps no row, as null compares with nothing: use {use}")
        else:
            values = [value]
        if not all(isinstance(v, _SCALARS) for v in values) or len({_kind(v) for v in values}) > 1:
            raise TypeError(f"filter {triple!r}: values are numbers, str, bytes, bools or dates, one kind per filter")


def _keeper(filter: Any) -> Callable[[Any], bool]:  # noqa: A002
    """Whether a row passes `filter`, with what each triple compares worked out once."""
    if callable(filter):
        return lambda row: bool(filter(row))
    tests = []
    for triple in filter or ():
        col, op, value = triple
        sample = _sample(op, value)
        null = op in ("in", "not in") and (None in value) == (op == "in")  # what a null field gives
        tests.append((triple, col, _OPS[op], value, null, sample, None if sample is None else _kind(sample)))

    def keep(row: Any) -> bool:
        for triple, col, test, value, null, sample, kind in tests:
            v = row.get(col)
            if v is None:
                if not null:
                    return False
            elif kind is not None and _kind(v) is not kind:
                found = f"{type(v).__name__} {v!r} in {col!r}"
                raise TypeError(f"filter {triple!r} compares {found} with {type(sample).__name__} {sample!r}")
            elif not test(v, value):
                return False
        return True

    return keep


# ---- built-in sources and sinks ---------------------------------------------


class FileSource(Source):
    """Files on a filesystem every peer can read.

    By extension: ``.jsonl``/``.ndjson`` yield dicts, ``.csv`` yields dicts of
    strings (CSV has no null: a row cut short reads its missing fields as
    None), ``.parquet`` yields dicts through pyarrow (with column and filter
    pushdown; a column the file lacks reads as nulls), anything else yields
    lines (UTF-8, without their ``\\n`` or ``\\r\\n``). Rows stream from the
    open file: a reader holds one line of a text file, and one batch of
    `batch_rows` rows of a Parquet file, which ``batches`` gives as Arrow
    record batches.

    A file up to `split_bytes` is one partition. A larger JSON-lines or text
    file is one partition per `split_bytes` range, and a line belongs to the
    range its first byte is in. A larger Parquet file is one partition per
    run of whole row groups up to `split_bytes`. A CSV file stays whole: a
    quoted field may hold a newline, so no byte tells where a row starts.

    A directory a ``FileSink`` wrote reads as the parts of the generation its
    ``_manifest.json`` names, and raises ``FileNotFoundError`` until a write
    to it has committed.
    """

    def __init__(self, pattern: str, *, split_bytes: int = 128 << 20, batch_rows: int = 65536) -> None:
        if split_bytes < 1 or batch_rows < 1:
            raise ValueError(f"split_bytes and batch_rows must be positive, got {split_bytes} and {batch_rows}")
        self.split_bytes, self.batch_rows = split_bytes, batch_rows
        directory = pattern if os.path.isdir(pattern) else None
        if directory is not None:
            manifest = _manifest(directory)
            if manifest is not None:
                # By name inside this directory, so a copied or remounted output still reads.
                self.files = [os.path.join(directory, os.path.basename(p["path"])) for p in manifest["parts"]]
                return
            pattern = os.path.join(directory, "*")
        self.files = sorted(f for f in glob.glob(pattern) if os.path.isfile(f))
        # A FileSink makes _manifests before it writes a part, so checking after
        # the glob covers every part it found: none of them is committed.
        if directory is not None and os.path.isdir(os.path.join(directory, "_manifests")):
            raise FileNotFoundError(f"nothing committed yet in {directory!r}: it has no _manifest.json")
        if not self.files:
            raise FileNotFoundError(f"no files match {pattern!r}")

    def partitions(self) -> list[Any]:
        """A path for a whole file, or ``(path, start, stop)``: bytes
        ``start:stop`` of a line file, row groups ``start:stop`` of a Parquet file."""
        parts: list[Any] = []
        for path in self.files:
            size, split = os.path.getsize(path), self.split_bytes
            if size <= split or path.endswith(".csv"):
                parts.append(path)
            elif path.endswith(".parquet"):
                parts += [(path, start, stop) for start, stop in _row_group_runs(path, split)]
            else:
                parts += [(path, start, min(start + split, size)) for start in range(0, size, split)]
        return parts

    def residual(self, part: Any, filter: list[Any]) -> list[Any]:  # noqa: A002
        path = part if isinstance(part, str) else part[0]
        if not path.endswith(".parquet"):
            return filter
        return _split(_parquet().read_schema(path), filter)[1]

    def read(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterable[Any]:  # noqa: A002
        path, start, stop = (part, 0, None) if isinstance(part, str) else part
        if path.endswith(".parquet"):
            return (row for batch in self.batches(part, columns, filter) for row in batch.to_pylist())
        if path.endswith(".csv"):
            return _csv_rows(path)
        lines = _lines(path, start, stop)
        if path.endswith((".jsonl", ".ndjson")):
            return (json.loads(line) for line in lines if line.strip())
        return (line.decode() for line in lines)

    def batches(self, part: Any, columns: list[str] | None = None, filter: Any = None) -> Iterator[Any]:  # noqa: A002
        path, start, stop = (part, 0, None) if isinstance(part, str) else part
        if not path.endswith(".parquet"):
            raise TypeError(f"only Parquet files read as Arrow batches, not {path!r}")
        pq = _parquet()
        import pyarrow as pa

        with pq.ParquetFile(path) as f:
            schema = f.schema_arrow
            exact, rest, absent = _split(schema, [] if filter is None or callable(filter) else filter)
            for triple in exact + rest:
                kind, sample = _arrow_kind(schema.field(triple[0]).type), _sample(triple[1], triple[2])
                if kind is not None and sample is not None and kind is not _kind(sample):
                    found = f"the {schema.field(triple[0]).type} column {triple[0]!r}"
                    raise TypeError(f"filter {triple!r} compares {found} with {type(sample).__name__} {sample!r}")
            # A column the file lacks is null in every row, so its filter keeps every row or none.
            if not _keeper(absent)({}):
                return
            pushed = _arrow_filter(schema, exact)
            # The pushed filter's columns are read too, and dropped after it runs.
            wanted = columns
            if columns is not None:
                wanted = [c for c in dict.fromkeys([*columns, *(t[0] for t in exact)]) if c in schema.names]
            missing = [c for c in columns or () if c not in schema.names]
            groups: Iterable[int] = range(start, f.num_row_groups if stop is None else stop)
            if pushed is not None:
                groups = _row_groups_that_may_match(path, groups, pushed)
            # A reader per row group: one reader of several keeps memory for
            # every row group it has read, so its peak grows with the file.
            for group in groups:
                for batch in f.iter_batches(self.batch_rows, row_groups=[group], columns=wanted):
                    if pushed is not None:
                        batch = batch.filter(pushed)
                    for c in missing:  # nulls, as a JSON row's missing field reads
                        batch = batch.append_column(c, pa.nulls(batch.num_rows))
                    yield batch if columns is None else batch.select(columns)


# The ops parquet pushes into the reader; the rest are applied after reading.
_ARROW_OPS: dict[str, Callable[[Any, Any], Any]] = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "in": lambda field, values: field.isin(values),
}


def _arrow_filter(schema: Any, filter: Any) -> Any:  # noqa: A002
    """The Arrow expression of the triples ``_split`` found exact for `schema`."""
    import pyarrow.dataset as ds

    expr = None
    for col, op, value in filter:
        term = _ARROW_OPS[op](ds.field(col), _arrow_value(schema.field(col).type, op, value))
        expr = term if expr is None else expr & term
    return expr


def _arrow_value(t: Any, op: str, value: Any) -> Any:
    """`value` (an `in` list's values) as a value of the column type `t`, or
    None when `t` can't hold it exactly. Arrow casts a column and a value of
    two types to one type, which can round (a big int64 to float32, 0.1 to
    float32) or overflow (a uint64 past int64), and it types a list of nulls
    alone as null, which a string column refuses."""
    import pyarrow as pa

    t = t.value_type if pa.types.is_dictionary(t) else t
    try:
        if op == "in":
            values = list(value)
            typed = pa.array(values, t)
            return typed if typed.to_pylist() == values else None
        typed = pa.scalar(value, t)
        return typed if typed.as_py() == value else None
    except (pa.ArrowException, ArithmeticError, TypeError, ValueError):  # no such value of type `t`
        return None


def _split(schema: Any, filter: list[Any]) -> tuple[list[Any], list[Any], list[Any]]:  # noqa: A002
    """The triples of `filter` that Arrow applies exactly to a file of
    `schema`; those left to apply to rows, where Arrow would not compare as
    Python does: an op Arrow lacks, a decimal, nested (no `in` for a list) or
    float16 (no kernels) column, != on a float column (row-group statistics
    leave NaN out, and NaN != 1.0), or a value the column's type can't hold
    exactly (``_arrow_value``); and those on a column the file lacks."""
    import pyarrow as pa

    def pushable(t: Any, op: str, value: Any) -> bool:
        t = t.value_type if pa.types.is_dictionary(t) else t
        if op not in _ARROW_OPS or pa.types.is_decimal(t) or pa.types.is_nested(t) or pa.types.is_float16(t):
            return False
        return not (op == "!=" and pa.types.is_floating(t)) and _arrow_value(t, op, value) is not None

    exact: list[Any] = []
    rest: list[Any] = []
    absent: list[Any] = []
    for t in filter:
        if t[0] not in schema.names:
            absent.append(t)
        elif pushable(schema.field(t[0]).type, t[1], t[2]):
            exact.append(t)
        else:
            rest.append(t)
    return exact, rest, absent


def _arrow_kind(t: Any) -> Any:
    """``_kind`` of the values of an Arrow column of type `t` (None: nulls only)."""
    import pyarrow.types as is_

    if is_.is_dictionary(t):
        t = t.value_type
    kinds: list[tuple[Any, tuple[Callable[[Any], bool], ...]]] = [
        (None, (is_.is_null,)),
        (bool, (is_.is_boolean,)),
        (numbers.Number, (is_.is_integer, is_.is_floating, is_.is_decimal)),
        (str, (is_.is_string, is_.is_large_string)),
        (bytes, (is_.is_binary, is_.is_large_binary, is_.is_fixed_size_binary)),
        (_AWARE[datetime.datetime], (lambda t: is_.is_timestamp(t) and t.tz is not None,)),
        (datetime.datetime, (is_.is_timestamp,)),
        (datetime.date, (is_.is_date,)),
        (datetime.time, (is_.is_time,)),
        (datetime.timedelta, (is_.is_duration,)),
    ]
    return next((kind for kind, tests in kinds if any(test(t) for test in tests)), t)


def _parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("reading .parquet files needs pyarrow: pip install pyarrow") from None
    return pq


def _row_group_runs(path: str, limit: int) -> list[tuple[int, int]]:
    """Runs ``(start, stop)`` of consecutive row groups of a Parquet file, each
    at most `limit` bytes on disk unless a single row group is larger."""
    meta = _parquet().read_metadata(path)
    runs, start, size = [], 0, 0
    for i in range(meta.num_row_groups):
        group = meta.row_group(i)
        n = sum(group.column(c).total_compressed_size for c in range(group.num_columns))
        if size and size + n > limit:
            runs.append((start, i))
            start, size = i, 0
        size += n
    return [*runs, (start, meta.num_row_groups)]


def _row_groups_that_may_match(path: str, groups: Iterable[int], expr: Any) -> list[int]:
    """The row groups of `groups` whose statistics do not rule `expr` out."""
    import pyarrow.dataset as ds
    from pyarrow import fs

    fragment = ds.ParquetFileFormat().make_fragment(path, filesystem=fs.LocalFileSystem(), row_groups=groups)
    return [group.id for group in fragment.subset(expr).row_groups]


def _lines(path: str, start: int, stop: int | None) -> Iterator[bytes]:
    """The lines of a file that begin in bytes ``start:stop``, without their
    ``\\n`` or ``\\r\\n``. Ranges that tile a file read each line once: a
    ``\\n`` byte never occurs inside a multibyte UTF-8 character."""
    with open(path, "rb") as f:
        if start:
            f.seek(start - 1)
            f.readline()  # through the newline that ends the line before ours: an earlier range's
        pos = f.tell()
        for line in f:
            if stop is not None and pos >= stop:
                return
            pos += len(line)
            yield line.removesuffix(b"\n").removesuffix(b"\r")


def _csv_rows(path: str) -> Iterator[dict[str, str]]:
    with open(path, newline="") as f:
        yield from csv.DictReader(f)


class FileSink(Sink):
    """One file per partition in `directory`, and a manifest per write.

    Every write is its own generation, named by its key: its parts are
    ``part-00000-<key>.jsonl`` and so on, and ``commit`` records them in
    ``_manifests/<key>.json``, then copies that to ``_manifest.json`` unless a
    write that began later has already published. A generation never changes
    once written, so two writes to one directory leave two whole generations,
    and a reader that goes through ``_manifest.json`` sees exactly one. Old
    generations stay, so a glob or a tool that reads the whole directory sees
    them all. Each part is written to a temporary name and renamed, so a
    retried write replaces it whole, and a replayed commit publishes the same
    generation. A part's receipt holds its size and SHA-256, and commit reads
    every part back to check them before it publishes anything: a part that
    changed or went missing raises ``ch.errors.DataLost`` naming it, and the
    directory keeps its last generation. ``format`` is ``"jsonl"`` (one JSON
    value per line) or ``"parquet"`` (dict rows, through pyarrow).

    With `store` (an ``ObjectStore``, such as ``S3Store``), `directory` is a
    key prefix in a store that can't rename. Each part is put whole, and its
    receipt adds the version the store gave it; commit checks each part's
    size and version, records the generation with a put that only creates,
    and moves ``_manifest.json`` forward with a put conditional on the
    version it read. A commit repeated after its reply was lost returns the
    generation it recorded, and never moves ``_manifest.json`` back.
    """

    effects = "idempotent"
    generation = ""  # the write's key, set by begin()

    def __init__(
        self,
        directory: str,
        format: str = "jsonl",  # noqa: A002
        *,
        store: ObjectStore | None = None,
    ) -> None:
        if format not in ("jsonl", "parquet"):
            raise ValueError(f"format must be 'jsonl' or 'parquet', got {format!r}")
        self.directory, self.format, self.store = directory, format, store

    def begin(self, key: str) -> FileSink:
        sink = copy.copy(self)
        sink.generation = key
        return sink

    def _key(self) -> str:
        if not self.generation:
            raise RuntimeError("a FileSink writes under a key: call begin(key) first, as write(sink) does")
        return self.generation

    def write(self, part_id: int, items: list[Any]) -> Any:
        def fill(tmp: str) -> int:
            if self.format == "jsonl":
                with open(tmp, "w") as f:
                    for item in items:
                        f.write(json.dumps(item, default=_jsonable) + "\n")
            else:
                try:
                    import pyarrow as pa
                    import pyarrow.parquet as pq
                except ImportError:
                    raise ImportError("writing parquet needs pyarrow: pip install pyarrow") from None
                pq.write_table(pa.Table.from_pylist([_row(i) for i in items]), tmp)
            return len(items)

        return self._part(part_id, fill)

    def write_batches(self, part_id: int, batches: Any) -> Any:
        """A part from Arrow record batches, one in memory at a time: a
        Parquet part keeps their schema, with a row group per batch."""

        def fill(tmp: str) -> int:
            rows = 0
            if self.format == "jsonl":
                with open(tmp, "w") as f:
                    for batch in batches:
                        f.writelines(json.dumps(row, default=_jsonable) + "\n" for row in batch.to_pylist())
                        rows += batch.num_rows
            else:
                with _parquet().ParquetWriter(tmp, batches.schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)
                        rows += batch.num_rows
            return rows

        return self._part(part_id, fill)

    def _part(self, part_id: int, fill: Callable[[str], int]) -> dict[str, Any]:
        """Write a part, its rows put by `fill` in the file it names, and give
        its receipt: the part's path, rows, and bytes and SHA-256 for commit
        to check."""
        name = f"part-{part_id:05d}-{self._key()}.{self.format}"
        if self.store is None:
            path = os.path.join(self.directory, name)
            # _manifests before any part: a reader that finds it and no
            # _manifest.json knows that the parts it sees are not committed.
            os.makedirs(os.path.join(self.directory, "_manifests"), exist_ok=True)
        else:
            path = posixpath.join(self.directory, name)
        fd, tmp = tempfile.mkstemp(dir=self.directory if self.store is None else None, prefix=".part-")
        os.close(fd)
        try:
            rows = fill(tmp)
            size, sha256 = _digest(tmp)
            receipt = {"part": part_id, "path": path, "rows": rows, "bytes": size, "sha256": sha256}
            if self.store is None:
                os.replace(tmp, path)
            else:
                with open(tmp, "rb") as f:
                    receipt["version"] = self.store.put(path, f, sha256=sha256)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp)
        return receipt

    def _check(self, receipt: dict[str, Any]) -> None:
        """Raise ``DataLost`` unless the part is still what its write made:
        the same bytes in a directory, the same version in a store."""
        path, what = receipt["path"], "SHA-256" if self.store is None else "version"
        got: tuple[int, str] | None
        if self.store is None:
            want = (receipt["bytes"], receipt["sha256"])
            try:
                got = _digest(path)
            except FileNotFoundError:
                got = None
        else:
            want, got = (receipt["bytes"], receipt["version"]), self.store.head(path)
        if got != want:
            now = "it is gone" if got is None else f"it has {got[0]} bytes, {what} {got[1]}"
            raise DataLost(
                f"part {receipt['part']} of write {self.generation} ({path}) is not what its write made, "
                f"{want[0]} bytes, {what} {want[1]}: {now}. The write was not published."
            )

    def commit(self, receipts: list[Any]) -> Any:
        key = self._key()
        for receipt in receipts:  # before anything is published
            self._check(receipt)
        manifest = {"generation": key, "parts": receipts, "rows": sum(r["rows"] for r in receipts)}
        if self.store is not None:
            return self._publish(self.store, manifest)
        versions = os.path.join(self.directory, "_manifests")
        os.makedirs(versions, exist_ok=True)
        _put_json(os.path.join(versions, f"{key}.json"), manifest)
        # Keys sort by when their write began: a replay, or a write that began
        # earlier and finished later, never moves the reader back. The lock
        # keeps two commits from both reading the old manifest before either
        # replaces it. A filesystem without flock (Lustre mounted without
        # -o flock) commits unlocked: every generation is still whole, but two
        # commits at the same moment may leave the older one published.
        with open(os.path.join(versions, ".lock"), "a") as lock:
            with contextlib.suppress(OSError):
                fcntl.flock(lock, fcntl.LOCK_EX)
            current = _manifest(self.directory)
            if current is None or current.get("generation", "") < key:
                _put_json(os.path.join(self.directory, "_manifest.json"), manifest)
        return manifest

    def _publish(self, store: ObjectStore, manifest: dict[str, Any]) -> dict[str, Any]:
        """Commit to a store, where no rename makes a file appear whole: puts
        that can't overwrite what they didn't read."""
        record = posixpath.join(self.directory, "_manifests", f"{manifest['generation']}.json")
        body = json.dumps(manifest).encode()
        try:
            store.put(record, body, if_version="")
        except VersionConflict:  # recorded already, by a replay or a put whose reply was lost
            stored = store.get(record)
            if stored is None:
                raise
            body, manifest = stored[0], json.loads(stored[0])
        pointer = posixpath.join(self.directory, "_manifest.json")
        while True:
            current = store.get(pointer)
            published = None if current is None else _parse_manifest(current[0])
            if published is not None and published.get("generation", "") >= manifest["generation"]:
                return manifest
            with contextlib.suppress(VersionConflict):  # another commit moved it: read it again
                store.put(pointer, body, if_version="" if current is None else current[1])
                return manifest


def _manifest(directory: str) -> dict[str, Any] | None:
    """The manifest a FileSink last published in `directory`, if any. Another
    tool's ``_manifest.json`` is not one."""
    try:
        with open(os.path.join(directory, "_manifest.json"), "rb") as f:
            return _parse_manifest(f.read())
    except FileNotFoundError:
        return None


def _parse_manifest(data: bytes) -> dict[str, Any] | None:
    try:
        manifest = json.loads(data)
    except ValueError:
        return None
    return manifest if isinstance(manifest, dict) and "parts" in manifest else None


def _digest(path: str) -> tuple[int, str]:
    """A file's size and SHA-256, as a part's receipt holds them."""
    with open(path, "rb") as f:
        return os.fstat(f.fileno()).st_size, hashlib.file_digest(f, "sha256").hexdigest()


def _put_json(path: str, value: Any) -> None:
    # A temporary name of its own, then a rename: readers see the old file or
    # the new one, and concurrent writers never share a temporary file.
    tmp = os.path.join(os.path.dirname(path), f".{os.path.basename(path)}.{secrets.token_hex(4)}")
    with open(tmp, "w") as f:
        json.dump(value, f)
    os.replace(tmp, path)


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


# ---- object stores -------------------------------------------------------------


class ObjectStore:
    """Where a ``FileSink`` writes when nothing can rename: an object store,
    whose puts replace an object whole. A put gives the object's new version
    (an ETag, a generation number), and ``if_version`` makes it conditional,
    which is how a commit records a generation once and moves
    ``_manifest.json`` only forward. ``S3Store`` is one; subclass this for
    another store."""

    def get(self, key: str) -> tuple[bytes, str] | None:
        """The object's bytes and version; None if there is none under `key`."""
        raise NotImplementedError

    def head(self, key: str) -> tuple[int, str] | None:
        """The object's size in bytes and version; None if there is none."""
        raise NotImplementedError

    def put(self, key: str, body: bytes | BinaryIO, *, sha256: str | None = None, if_version: str | None = None) -> str:
        """Store `body` (bytes, or a file open for reading) whole under `key`
        and give its version. `sha256` is the body's hex digest, for a store
        that checks what it receives. With `if_version`, put only if the
        object still has that version (``""``: only if there is none), and
        raise ``ch.errors.VersionConflict`` otherwise. A put whose reply was
        lost may have taken effect: repeat it, and let the condition say."""
        raise NotImplementedError


class S3Store(ObjectStore):
    """An S3 bucket, or a store with S3's API (MinIO, Ceph, R2), through the
    standard library. Credentials come from ``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY`` and ``AWS_SESSION_TOKEN`` in each worker's
    peer's environment (a run doesn't ship them). `endpoint` defaults to
    ``AWS_ENDPOINT_URL_S3`` or ``AWS_ENDPOINT_URL``, else AWS's; `region` to
    ``AWS_REGION``, else us-east-1. A FileSink's commit needs conditional
    puts (``If-None-Match`` and ``If-Match``), which AWS and MinIO have. A
    request that gets no reply or a 5xx is sent again, which every request a
    FileSink makes allows."""

    def __init__(self, bucket: str, *, endpoint: str | None = None, region: str | None = None) -> None:
        env = os.environ.get
        self.bucket = bucket
        self.region = region or env("AWS_REGION") or env("AWS_DEFAULT_REGION") or "us-east-1"
        default = f"https://s3.{self.region}.amazonaws.com"
        self.endpoint = (endpoint or env("AWS_ENDPOINT_URL_S3") or env("AWS_ENDPOINT_URL") or default).rstrip("/")

    def get(self, key: str) -> tuple[bytes, str] | None:
        status, headers, body = self._request("GET", key)
        if status == 200:
            return body, headers["ETag"]
        if status == 404 and b"NoSuchKey" in body:  # not NoSuchBucket
            return None
        raise self._failed("GET", key, status, body)

    def head(self, key: str) -> tuple[int, str] | None:
        status, headers, body = self._request("HEAD", key)
        if status in (200, 404):  # a HEAD's 404 has no body to tell a missing bucket by
            return None if status == 404 else (int(headers["Content-Length"]), headers["ETag"])
        raise self._failed("HEAD", key, status, body)

    def put(self, key: str, body: bytes | BinaryIO, *, sha256: str | None = None, if_version: str | None = None) -> str:
        condition = {} if if_version is None else {"If-Match": if_version} if if_version else {"If-None-Match": "*"}
        status, headers, reply = self._request("PUT", key, body, condition, sha256)
        if status == 200:
            return headers["ETag"]
        # 412; 409 for a conditional put that raced another; 404 NoSuchKey for If-Match on nothing.
        if if_version is not None and (status in (409, 412) or b"NoSuchKey" in reply):
            now = "exists already" if if_version == "" else f"is not at version {if_version}"
            raise VersionConflict(f"s3://{self.bucket}/{key} {now} ({status})")
        raise self._failed("PUT", key, status, reply)

    def _request(
        self,
        method: str,
        key: str,
        body: bytes | BinaryIO = b"",
        headers: dict[str, str] | None = None,
        sha256: str | None = None,
    ) -> tuple[int, Any, bytes]:
        """The status, headers and body of the reply. Other failures than
        404, 409 and 412 raise ``OSError``."""
        url = f"{self.endpoint}/{self.bucket}/{urllib.parse.quote(key)}"
        headers = dict(headers or {})
        if isinstance(body, bytes):
            sha256 = sha256 or hashlib.sha256(body).hexdigest()
        if method == "PUT":
            headers["Content-Length"] = str(len(body) if isinstance(body, bytes) else os.fstat(body.fileno()).st_size)
        attempt = 0
        while True:
            if not isinstance(body, bytes):
                body.seek(0)
            signed = self._sign(method, url, sha256 or "UNSIGNED-PAYLOAD", headers)
            request = urllib.request.Request(url, body if method == "PUT" else None, signed, method=method)
            try:
                with urllib.request.urlopen(request, timeout=60) as reply:
                    return reply.status, reply.headers, reply.read()
            except urllib.error.HTTPError as e:
                if e.code in (404, 409, 412):
                    return e.code, e.headers, e.read()
                if e.code < 500 or attempt == 4:
                    raise self._failed(method, key, e.code, e.read()) from None
            except OSError:  # no reply: the request may have taken effect
                if attempt == 4:
                    raise
            attempt += 1
            time.sleep(0.1 * 2**attempt)

    def _failed(self, method: str, key: str, status: int, body: bytes) -> OSError:
        return OSError(f"S3 {method} s3://{self.bucket}/{key}: {status} {body[:500]!r}")

    def _sign(self, method: str, url: str, payload: str, headers: dict[str, str]) -> dict[str, str]:
        """`headers` and those AWS Signature Version 4 adds."""
        try:
            key_id, secret = os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]
        except KeyError:
            raise RuntimeError("S3Store needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment") from None
        now = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        parts = urllib.parse.urlsplit(url)
        signed = {k.lower(): v.strip() for k, v in headers.items()}
        signed |= {"host": parts.netloc, "x-amz-content-sha256": payload, "x-amz-date": now}
        if os.environ.get("AWS_SESSION_TOKEN"):
            signed["x-amz-security-token"] = os.environ["AWS_SESSION_TOKEN"]
        names = ";".join(sorted(signed))
        lines = [f"{k}:{signed[k]}" for k in sorted(signed)]
        canonical = "\n".join([method, parts.path, "", *lines, "", names, payload])
        scope = f"{now[:8]}/{self.region}/s3/aws4_request"
        text = f"AWS4-HMAC-SHA256\n{now}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        key = f"AWS4{secret}".encode()
        for word in (now[:8], self.region, "s3", "aws4_request"):
            key = hmac.new(key, word.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, text.encode(), hashlib.sha256).hexdigest()
        signed["authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={key_id}/{scope}, SignedHeaders={names}, Signature={signature}"
        )
        return signed
