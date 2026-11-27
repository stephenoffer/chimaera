"""Value encoding shared by clients and executors.

Object layout (also used for inline values)::

    b"CHO1" | flags u32 | n_buffers u32 | reserved u32 | main_len u64 | buf_len u64 * n
    [flags & EXTERNAL: blob_id 16 bytes * n, all zero for buffers stored here]
    [flags & OFFSETS: offset u64 * n, where each external buffer starts in its blob]
    pad to 64 | main pickle | pad to 64 | buffer 0 | pad to 64 | buffer 1 ...

The main frame is a protocol-5 pickle; large contiguous buffers (NumPy arrays,
Arrow buffers) travel out of band. A buffer marked external is not stored in
this object at all: it is a content-addressed blob (`<run dir>/<blob id>`)
written once and shared by every call that passes the same bytes. Reading maps
files copy-on-write, so arrays come back writable without copying and a write
never reaches the stored object. Rust never decodes these bytes.
"""

from __future__ import annotations

import copyreg
import errno
import functools
import inspect
import io
import itertools
import mmap
import operator
import os
import pickle
import struct
import sys
import threading
import types
from typing import Any, Callable

import cloudpickle

from . import _native
from ._errors import OutOfDisk

MAGIC = b"CHO1"
_HEAD = struct.Struct("<4sIIIQ")
# A tuple stored element by element: each element a complete object behind an
# offset table, so a reader that needs one element (one value of a call that
# returns several) decodes and touches only that element.
SEQ_MAGIC = b"CHS1"
_SEQ_HEAD = struct.Struct("<4sII")  # magic, reserved, count
_SEQ_ITEM = struct.Struct("<QQ")  # offset, length
# A tuple whose large elements are objects of their own (written beside it,
# named in its table), so a reader that needs one element fetches only that
# element's bytes from another peer. Same header as SEQ_MAGIC.
PARTS_MAGIC = b"CHP1"
_PART_ITEM = struct.Struct("<16sQQ")  # part object id (zeros: in this file), offset, length
# A tuple of values that are empty dicts but for a few (see Sparse), stored as
# those few: the ids of the ones kept in files of their own, then an entry per
# stored value, sorted by position, which a reader of one finds by bisection.
# The parts' count is where PARTS_MAGIC keeps its count, so a reader of either
# header knows how much to read for the ids.
SPARSE_MAGIC = b"CHQ1"
SPARSE_LAYOUT = 1  # the object_layout a peer advertises once it reads SPARSE_MAGIC (Member.object_layout)
_SPARSE_HEAD = struct.Struct("<4sIIIQ")  # magic, reserved, parts kept apart, entries, count
# position, part (its place among the ids, from 1; 0: in this file), offset, length
_SPARSE_ITEM = struct.Struct("<QQQQ")
_ALIGN = 64
_OOB_MIN = 64 * 1024  # smaller buffers stay inside the pickle
EXTERNAL = 1  # header flag: a blob-id table follows the buffer lengths
OFFSETS = 2  # header flag: an offset table follows the blob ids
_NO_BLOB = bytes(16)
# Object files up to this size are read, not mapped. On macOS the first map of
# a file costs about 2.5 ms however small it is (8 ms at 16 MB; reading 400 KB
# takes 0.07 ms, measured on an M3 Pro), and every input a peer fetched is a
# first map: a gather of 64 fetched inputs spent 180 ms mapping them. On Linux
# a map costs microseconds, and from 256 KiB a mapped value is read faster than
# a copy: a 512 KiB array's get and first touch took 0.10 ms against 0.15, and
# at 1 MiB, the arena's size, 0.09 against 0.18. Larger values stay mapped: no
# copy, paged in as they are touched.
_READ_MAX = (16 << 20) if sys.platform == "darwin" else (256 << 10)

# Fetches a blob another peer holds into this peer's store: (run, blob_id).
# Set by the runtime; None means a missing blob is an error.
localize_hook: Callable[[bytes, bytes], None] | None = None
_reading = threading.local()


def _pad(n: int) -> int:
    return (-n) % _ALIGN


class _MainRefused(pickle.PicklingError):
    pass


class _Pickler(pickle.Pickler):
    """The standard pickler, refusing classes and functions of ``__main__``:
    by reference, ``__main__.X`` names the driver's script (or a notebook's
    namespace), which the worker does not have, so cloudpickle ships those by
    value. Checked as objects are pickled, which costs nothing for the plain
    types pickle never asks about (and so never scans a large pickle)."""

    def reducer_override(self, obj: Any) -> Any:
        reduce = reducers.get(type(obj))
        if reduce is not None:
            return reduce(obj)
        if isinstance(obj, (type, types.FunctionType)) and getattr(obj, "__module__", None) == "__main__":
            raise _MainRefused("refers to __main__")
        return NotImplemented


class _CloudPickler(cloudpickle.Pickler):
    """cloudpickle, refusing open text files: it would read the whole file in
    the sender and deliver a copy of its contents in place of the file."""

    def reducer_override(self, obj: Any) -> Any:
        reduce = reducers.get(type(obj))
        if reduce is not None:
            return reduce(obj)
        if isinstance(obj, io.TextIOWrapper) and obj not in (sys.stdin, sys.stdout, sys.stderr):
            name = getattr(obj, "name", "?")
            raise pickle.PicklingError(f"an open file ({name!r}) cannot be sent; pass its path and open it in the call")
        return super().reducer_override(obj)


# Containers at least this long are checked for plain data (see plain_tree).
_FAST_MIN = 1024
_ATOMS = (bytes, bytearray, str, int, float, bool, type(None))
_plain = getattr(_native, "plain_tree", lambda _: False)  # absent from older builds
_plain_encode = getattr(_native, "plain_encode", None)  # absent from older builds
_plain_column = getattr(_native, "plain_encode_column", None)  # absent from older builds
_pread_heap = getattr(_native, "pread_heap", None)  # absent from older builds
_encode_heap = getattr(_native, "encode_utf8_heap", None)  # likewise

# Reducers for values this module pickles, by type (ext.register_reducer),
# and only there: unlike copyreg's, a plain pickle.dumps elsewhere in the
# program does not use them. Both picklers below ask them first.
reducers: dict[type, Callable[[Any], Any]] = {}


def _pickle(value: Any, pickler_cls: type, buffer_callback: Callable[[pickle.PickleBuffer], bool]) -> bytes:
    from ._values import torch_reducers

    f = io.BytesIO()
    p = pickler_cls(f, protocol=5, buffer_callback=buffer_callback)
    extra = {**torch_reducers(), **_numpy_reducers()}
    if extra:
        p.dispatch_table = {**copyreg.dispatch_table, **getattr(p, "dispatch_table", {}), **extra}
    if (
        pickler_cls is _Pickler
        and type(value) in (dict, list, tuple)
        and len(value) >= _FAST_MIN
        and _plain(value)
    ):
        # Plain data with no shared containers: the memo would change nothing
        # but cost most of the time (6x for a dict of a million strings).
        p.fast = True
    p.dump(value)
    return f.getvalue()


class _OutOfBand:
    """A large top-level value pickled out of band: `rebuild(buffer)`. A plain
    instance, freed as soon as pickling ends. (A class made per call sits in a
    reference cycle, so it kept its buffer, hundreds of MiB of heap or arena,
    until the next garbage collection: an idle worker returning large bytes
    kept every one, and filled /dev/shm.)"""

    __slots__ = ("rebuild", "buf")

    def __init__(self, rebuild: Callable[[Any], Any], buf: Any) -> None:
        self.rebuild, self.buf = rebuild, buf

    def __reduce_ex__(self, protocol: int) -> tuple[Any, ...]:
        return (self.rebuild, (pickle.PickleBuffer(self.buf),))


def _plain_from(buf: Any) -> Any:
    """A large plain container, encoded natively (see _native.plain_encode).
    Pickle names this function in every such value it writes, so it stays a
    plain function of this module, whatever the native module calls its own."""
    return _native.plain_decode(buf)


def _object_array_from(buf: Any, shape: tuple[int, ...], order: str) -> Any:
    """An object array of plain values (see _numpy_reducers)."""
    import numpy as np

    items = _native.plain_decode(buf)
    # Filled from the list in C, as unpickling an object array fills one:
    # 43,690 strings in 0.29 ms, against 0.51 ms through np.fromiter.
    out = np.empty(0, dtype=object)
    out.__setstate__((1, (len(items),), out.dtype, False, items if type(items) is list else list(items)))
    return out.reshape(shape, order=order)


def _numpy_reducers() -> dict[type, Callable[[Any], Any]]:
    """Object arrays of plain values, such as a pandas column of strings,
    through the native codec instead of pickle, each repeated str object
    still written once, as pickle's memo would; any other array reduces as
    NumPy's own. Only when NumPy is already imported (never imports it)."""
    np = sys.modules.get("numpy")
    if np is None or _plain_column is None:
        return {}

    def reduce_array(a: Any) -> Any:
        if a.dtype == object and a.size >= _FAST_MIN:
            order = "F" if a.flags.f_contiguous and not a.flags.c_contiguous else "C"
            encoded = _plain_column(a.ravel(order=order).tolist())
            if encoded is not None:  # None: not all plain values
                return (_object_array_from, (pickle.PickleBuffer(encoded), a.shape, order))
        return a.__reduce_ex__(5)

    return {np.ndarray: reduce_array}


def _big_bytes(b: bytes | bytearray) -> Any:
    """A large top-level bytes value pickled out of band (pickle always copies
    bytes into the stream): its buffer goes straight into the object file."""
    return _OutOfBand(type(b), b)


def _big_str(s: str) -> Any:
    """A large top-level str, encoded to UTF-8 once and sent out of band:
    the encoded bytes (in the arena when large, so then stored by link) go
    into the object file as they are, instead of being encoded into pickle's
    stream and copied from there. `surrogatepass` is what pickle uses, so the
    string comes back exactly, lone surrogates included. Below 8 MiB the
    bytes are encoded on the heap and written into the object file (see
    _native.encode_utf8_heap); from 8 MiB the arena is faster, as for reads
    (_pread): its blocks are populated on several threads as they are
    handed out, and the encoded bytes are stored by link."""
    if _encode_heap is not None and len(s) < 8 << 20:
        return _OutOfBand(_str_from_utf8, _encode_heap(s))
    return _OutOfBand(_str_from_utf8, s.encode("utf-8", "surrogatepass"))


def _str_from_utf8(buf: Any) -> str:
    return str(buf, "utf-8", "surrogatepass")


def dumps_parts(
    value: Any, external: Callable[[memoryview], bytes | tuple[bytes, int] | None] | None = None
) -> tuple[list[bytes | memoryview], int, int]:
    """Encode `value`; return (parts, total_size, n_out_of_band_buffers).

    `external(view)` may claim a large buffer by returning its blob id, or
    (blob id, offset of the buffer in that blob); it stores the bytes itself.
    Claimed buffers are referenced, not copied.
    """
    t = type(value)
    if t in _ATOMS or (t in (list, tuple, dict) and len(value) < _FAST_MIN and _plain(value)):
        if t not in (bytes, bytearray, str) or len(value) < _OOB_MIN:
            # Small plain data: no buffers to move and nothing from __main__,
            # so the stock pickler's one call does (about 3x less overhead).
            main = pickle.dumps(value, protocol=5)
            head = _HEAD.pack(MAGIC, 0, 0, 0, len(main))
            return [head, b"\0" * _pad(len(head)), main], len(head) + _pad(len(head)) + len(main), 0
    buffers: list[tuple[memoryview, bytes, int]] = []
    plain: bytes | None = None

    def keep(buf: pickle.PickleBuffer) -> bool:
        raw = buf.raw()
        if raw.nbytes < _OOB_MIN:
            return True
        # The plain codec's bytes stay in this value's own file: they are its
        # size (what promotes an argument, what CH118 measures), and no other
        # value shares them.
        blob = external(raw) if external is not None and raw.obj is not plain else None
        if isinstance(blob, tuple):
            buffers.append((raw, blob[0], blob[1]))
        else:
            buffers.append((raw, blob or _NO_BLOB, 0))
        return False

    if type(value) in (bytes, bytearray) and len(value) >= _OOB_MIN:
        value = _big_bytes(value)
    elif type(value) is str and len(value) >= _OOB_MIN:
        value = _big_str(value)
    elif t in (dict, list, tuple) and len(value) >= _FAST_MIN and _plain_encode is not None:
        # Plain data, encoded natively in one pass and sent out of band like
        # a large bytes value; None (not plain data) is pickled below.
        plain = _plain_encode(value)
        if plain is not None:
            value = _OutOfBand(_plain_from, plain)
    try:
        main = _pickle(value, _Pickler, keep)
    except (pickle.PicklingError, AttributeError, TypeError):
        buffers.clear()  # the failed attempt may have kept some already
        main = _pickle(value, _CloudPickler, keep)
    flags = EXTERNAL if any(b != _NO_BLOB for _, b, _ in buffers) else 0
    if any(o for _, _, o in buffers):
        flags |= OFFSETS
    head = _HEAD.pack(MAGIC, flags, len(buffers), 0, len(main))
    head += b"".join(struct.pack("<Q", r.nbytes) for r, _, _ in buffers)
    if flags & EXTERNAL:
        head += b"".join(b for _, b, _ in buffers)
    if flags & OFFSETS:
        head += b"".join(struct.pack("<Q", o) for _, _, o in buffers)
    parts: list[bytes | memoryview] = [head, b"\0" * _pad(len(head)), main]
    size = len(head) + _pad(len(head)) + len(main)
    for raw, blob, _ in buffers:
        if blob != _NO_BLOB:
            continue
        parts.append(b"\0" * _pad(size))
        size += _pad(size)
        parts.append(raw)
        size += raw.nbytes
    return parts, size, len(buffers)


def dumps(value: Any) -> tuple[bytes, int]:
    parts, _, n = dumps_parts(value)
    return b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts), n


def _map(path: str) -> mmap.mmap | bytearray:
    """Map a file copy-on-write (writable views, writes stay private); fall
    back to read-only where the OS refuses private writable mappings. A file
    of at most _READ_MAX bytes is read into a private buffer instead."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size == 0:
            raise ValueError(f"empty object file {path}")
        if size <= _READ_MAX:
            buf = bytearray(size)
            if f.readinto(buf) != size:
                raise ValueError(f"object file {path} changed while it was read")
            return buf
        try:
            return mmap.mmap(f.fileno(), size, access=mmap.ACCESS_COPY)
        except OSError:  # strict overcommit (vm.overcommit_memory=2)
            return mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)


def _blob_view(blob_dir: str, blob_id: bytes) -> memoryview:
    path = os.path.join(blob_dir, blob_id.hex())
    try:
        return memoryview(_map(path))
    except FileNotFoundError:
        if localize_hook is None:
            raise
    localize_hook(bytes.fromhex(os.path.basename(blob_dir)), blob_id)
    return memoryview(_map(path))


def dumps_seq(
    value: tuple[Any, ...], external: Callable[[memoryview], bytes | tuple[bytes, int] | None] | None = None
) -> tuple[list[bytes | memoryview], int, int]:
    """Encode a tuple element by element (see SEQ_MAGIC); same result shape as
    ``dumps_parts``."""
    return pack_seq([dumps_parts(v, external) for v in value])


class Sparse:
    """`count` values, each an empty dict but those in `present` (position:
    value): a shuffle scatter's buckets, which a scatter into a million
    partitions mostly leaves empty. Stored as the present values alone (see
    SPARSE_MAGIC); it reads as the tuple it stands for."""

    __slots__ = ("count", "present")

    def __init__(self, count: int, present: dict[int, Any]) -> None:
        self.count, self.present = count, present

    def __len__(self) -> int:
        return self.count

    def __getitem__(self, index: int) -> Any:
        if not 0 <= index < self.count:
            raise IndexError(f"position {index} of {self.count}")
        return self.present.get(index, {})

    def __iter__(self) -> Any:
        return (self.present.get(i, {}) for i in range(self.count))


def pack_seq(
    encoded: list[tuple[list[bytes | memoryview], int, int]],
    ids: list[bytes | None] | None = None,
    positions: list[int] | None = None,
    count: int = 0,
) -> tuple[list[bytes | memoryview], int, int]:
    """A tuple from its elements as ``dumps_parts`` encodes them; same result
    shape. An element with an id in `ids` is already stored as that object
    and is only named here (see PARTS_MAGIC). With `positions` (ascending),
    the elements are those a Sparse of `count` holds (see SPARSE_MAGIC).
    Elements start on aligned offsets, so their buffers stay aligned in a
    mapped file."""
    ids = ids or [None] * len(encoded)
    split = any(ids)
    if positions is not None:
        apart = [part for part in ids if part is not None]
        item = _SPARSE_ITEM
        head = _SPARSE_HEAD.pack(SPARSE_MAGIC, 0, len(apart), len(encoded), count) + b"".join(apart)
    else:
        item = _PART_ITEM if split else _SEQ_ITEM
        head = _SEQ_HEAD.pack(PARTS_MAGIC if split else SEQ_MAGIC, 0, len(encoded))
    pos = len(head) + item.size * len(encoded)
    spans = []
    for (_, size, _), part in zip(encoded, ids):
        if part is not None:
            spans.append((0, size))
            continue
        pos += _pad(pos)
        spans.append((pos, size))
        pos += size
    if positions is not None:
        numbers = itertools.accumulate(part is not None for part in ids)  # each part's place among the ids
        table = b"".join(
            _SPARSE_ITEM.pack(position, k if part is not None else 0, o, n)
            for position, k, part, (o, n) in zip(positions, numbers, ids, spans)
        )
    elif split:
        table = b"".join(_PART_ITEM.pack(part or _NO_BLOB, o, n) for part, (o, n) in zip(ids, spans))
    else:
        table = b"".join(_SEQ_ITEM.pack(o, n) for o, n in spans)
    out: list[bytes | memoryview] = [head, table]
    at = len(head) + item.size * len(encoded)
    for (parts, size, _), (off, _), part in zip(encoded, spans, ids):
        if part is None:
            out.append(b"\0" * (off - at))
            out.extend(parts)
            at = off + size
    return out, at, sum(n for _, _, n in encoded)


def _seq_spans(view: memoryview) -> list[tuple[bytes, int, int]] | None:
    """A stored tuple's table: (part id or zeros, offset, length) per element."""
    magic = bytes(view[:4])
    if magic not in (SEQ_MAGIC, PARTS_MAGIC):
        return None
    _, _, n = _SEQ_HEAD.unpack_from(view, 0)
    if magic == SEQ_MAGIC:
        return [(_NO_BLOB, *_SEQ_ITEM.unpack_from(view, _SEQ_HEAD.size + _SEQ_ITEM.size * i)) for i in range(n)]
    return [_PART_ITEM.unpack_from(view, _SEQ_HEAD.size + _PART_ITEM.size * i) for i in range(n)]


def _sparse_ids(view: memoryview) -> list[bytes]:
    """A stored Sparse's ids of the values kept in files of their own."""
    parts = _SPARSE_HEAD.unpack_from(view, 0)[2]
    return [bytes(view[_SPARSE_HEAD.size + 16 * i : _SPARSE_HEAD.size + 16 * (i + 1)]) for i in range(parts)]


def _sparse_spans(view: memoryview) -> list[tuple[int, tuple[bytes, int, int]]]:
    """A stored Sparse's table: (position, (part id or zeros, offset, length)) per value it holds."""
    ids = _sparse_ids(view)
    entries = _SPARSE_HEAD.unpack_from(view, 0)[3]
    table = _SPARSE_HEAD.size + 16 * len(ids)
    out = []
    for i in range(entries):
        position, part, off, n = _SPARSE_ITEM.unpack_from(view, table + _SPARSE_ITEM.size * i)
        out.append((position, (ids[part - 1] if part else _NO_BLOB, off, n)))
    return out


# The entry of a value a Sparse leaves out: an empty dict.
_ABSENT = (_NO_BLOB, 0, 0)


def _sparse_entry(read: Callable[[int, int], Any], index: int, count: int) -> tuple[bytes, int, int] | None:
    """Entry `index` of a stored Sparse of `count` values (_ABSENT if it
    leaves that one out), its header and table read through `read(offset,
    length)`: bisecting reads a few entries of a million. None when the bytes
    are not a Sparse of `count`."""
    head = read(0, _SPARSE_HEAD.size)
    if len(head) < _SPARSE_HEAD.size:
        return None
    magic, _, parts, entries, n = _SPARSE_HEAD.unpack(head)
    if magic != SPARSE_MAGIC or n != count:
        return None
    table = _SPARSE_HEAD.size + 16 * parts
    # Every value present (a scatter whose pairs touched each partition): entry i is position i.
    lo, hi = (index, index + 1) if entries == count else (0, entries)
    while lo < hi:
        mid = (lo + hi) // 2
        position, part, off, length = _SPARSE_ITEM.unpack(read(table + _SPARSE_ITEM.size * mid, _SPARSE_ITEM.size))
        if position < index:
            lo = mid + 1
        elif position > index:
            hi = mid
        else:
            return (bytes(read(_SPARSE_HEAD.size + 16 * (part - 1), 16)) if part else _NO_BLOB, off, length)
    return _ABSENT


def _element(view: memoryview, entry: tuple[bytes, int, int], blob_dir: str | None) -> Any:
    if entry is _ABSENT:
        return {}
    part, off, n = entry
    if part == _NO_BLOB:
        return loads(view[off : off + n], blob_dir)
    if blob_dir is None:
        raise ValueError("this value is stored in parts but no run directory was given")
    return loads(_blob_view(blob_dir, part), blob_dir)  # fetched from the peer that wrote it


def part_id(data: bytes | memoryview | mmap.mmap, index: int, count: int) -> bytes | None:
    """The object element `index` of a stored tuple (or Sparse) of `count`
    values is kept in, when it is an object of its own (see PARTS_MAGIC and
    SPARSE_MAGIC). `data` needs only the header and table; ``part_of``
    reads them from a file."""
    entry = _seq_entry(memoryview(data), index, count)
    # None: one file, or not the count the reader expects (an error when it is read).
    return None if entry is None or entry[0] == _NO_BLOB else entry[0]


def loads_part(data: bytes | memoryview | mmap.mmap, index: int, count: int, blob_dir: str | None = None) -> Any:
    """Element `index` of a stored tuple of `count` values: only that element
    is decoded (and fetched) when it was stored element by element."""
    view = memoryview(data)
    entry = _seq_entry(view, index, count)
    if entry is None:
        from ._future import pick

        return pick(loads(view, blob_dir), index, count)
    return _element(view, entry, blob_dir)


def _seq_entry(view: memoryview, index: int, count: int) -> tuple[bytes, int, int] | None:
    """Entry `index` of a stored tuple's table (see _seq_spans), when `view`
    holds a stored tuple of `count` values: that entry alone is decoded (a
    1024-way scatter's table has 1024). _ABSENT for a value a Sparse leaves out."""
    magic = bytes(view[:4])
    if magic == SPARSE_MAGIC:
        return _sparse_entry(lambda o, n: view[o : o + n], index, count)
    if magic not in (SEQ_MAGIC, PARTS_MAGIC) or _SEQ_HEAD.unpack_from(view, 0)[2] != count:
        return None
    if magic == SEQ_MAGIC:
        return (_NO_BLOB, *_SEQ_ITEM.unpack_from(view, _SEQ_HEAD.size + _SEQ_ITEM.size * index))
    return _PART_ITEM.unpack_from(view, _SEQ_HEAD.size + _PART_ITEM.size * index)


class PartAway(Exception):
    """The element ``read_part`` was asked for without fetching is kept in a
    file of its own, `part`, that is not in this peer's store."""

    def __init__(self, part: bytes) -> None:
        super().__init__(part.hex())
        self.part = part


def read_part(path: str, index: int, count: int, blob_dir: str | None = None, fetch: bool = True) -> Any:
    """``loads_part`` of the object file at `path`, reading only the stored
    tuple's header, that element's entry of its table and that element's
    bytes: a shuffle's gather wants one bucket of each of many 64-bucket
    files, and reading whole files read 64 times the data it decoded. A file
    too large to read whole is mapped instead (pages are read when touched),
    and so is anything that is not a stored tuple of `count` values. With
    `fetch` false, an element in a file of its own that is not here raises
    PartAway, for the caller to fetch with others, not alone."""
    fd = os.open(path, os.O_RDONLY)
    try:
        entry = _file_entry(fd, index, count) if os.fstat(fd).st_size <= _READ_MAX else None
        if entry is not None:
            if entry is _ABSENT:
                return {}
            part, off, n = entry
            if part != _NO_BLOB:
                if not fetch:
                    _here(part, blob_dir)
                return _element(memoryview(b""), entry, blob_dir)
            # Out-of-band buffers (64 KiB and up) keep their alignment
            # in page-aligned memory; anything smaller has none.
            buf: bytearray | mmap.mmap = bytearray(n) if n < _OOB_MIN else mmap.mmap(-1, n)
            if os.preadv(fd, [buf], off) != n:
                raise ValueError(f"object file {path} changed while it was read")
            return loads(buf, blob_dir)
    finally:
        os.close(fd)
    data = _map(path)
    entry = None if fetch else _seq_entry(memoryview(data), index, count)
    if entry is not None and entry[0] != _NO_BLOB:
        _here(entry[0], blob_dir)
    return loads_part(data, index, count, blob_dir)


def _file_entry(fd: int, index: int, count: int) -> tuple[bytes, int, int] | None:
    """``_seq_entry`` of the object file open as `fd`, from its header and
    that entry alone."""
    head = os.pread(fd, _SPARSE_HEAD.size, 0)  # either header
    if head[:4] == SPARSE_MAGIC:
        return _sparse_entry(lambda o, n: head[o : o + n] if o + n <= len(head) else os.pread(fd, n, o), index, count)
    item = {SEQ_MAGIC: _SEQ_ITEM, PARTS_MAGIC: _PART_ITEM}.get(head[:4])
    if item is None or len(head) < _SEQ_HEAD.size or _SEQ_HEAD.unpack_from(head)[2] != count:
        return None
    raw = os.pread(fd, item.size, _SEQ_HEAD.size + item.size * index)
    return (_NO_BLOB, *item.unpack(raw)) if item is _SEQ_ITEM else item.unpack(raw)


def part_of(path: str, index: int, count: int) -> bytes | None:
    """The object element `index` of the stored tuple of `count` values at
    `path` is kept in, when it is an object of its own (see PARTS_MAGIC),
    read from the file's header and that element's entry alone."""
    fd = os.open(path, os.O_RDONLY)
    try:
        entry = _file_entry(fd, index, count)
    finally:
        os.close(fd)
    return None if entry is None or entry[0] == _NO_BLOB else entry[0]


def _here(part: bytes, blob_dir: str | None) -> None:
    if blob_dir is not None and not os.path.exists(os.path.join(blob_dir, part.hex())):
        raise PartAway(part)


def loads(data: bytes | memoryview | mmap.mmap, blob_dir: str | None = None) -> Any:
    """Decode an object. `blob_dir` is the run's object directory, where any
    external buffers, promoted arguments and parts live."""
    view = memoryview(data)
    spans = _seq_spans(view)
    if spans is not None:
        return tuple(_element(view, e, blob_dir) for e in spans)
    if bytes(view[:4]) == SPARSE_MAGIC:
        present = {position: _element(view, e, blob_dir) for position, e in _sparse_spans(view)}
        return Sparse(_SPARSE_HEAD.unpack_from(view, 0)[4], present)
    magic, flags, n, _, main_len = _HEAD.unpack_from(view, 0)
    if magic != MAGIC:
        raise ValueError("not a Chimaera object")
    off = _HEAD.size
    lens = [struct.unpack_from("<Q", view, off + 8 * i)[0] for i in range(n)]
    off += 8 * n
    blobs = [_NO_BLOB] * n
    if flags & EXTERNAL:
        blobs = [bytes(view[off + 16 * i : off + 16 * (i + 1)]) for i in range(n)]
        off += 16 * n
    starts = [0] * n
    if flags & OFFSETS:
        starts = [struct.unpack_from("<Q", view, off + 8 * i)[0] for i in range(n)]
        off += 8 * n
    off += _pad(off)
    main = view[off : off + main_len]
    off += main_len
    bufs = []
    for ln, blob, start in zip(lens, blobs, starts):
        if blob != _NO_BLOB:
            if blob_dir is None:
                raise ValueError("this value refers to stored blobs but no run directory was given")
            bufs.append(_blob_view(blob_dir, blob)[start : start + ln])
            continue
        off += _pad(off)
        bufs.append(view[off : off + ln])
        off += ln
    previous = getattr(_reading, "blob_dir", None)
    _reading.blob_dir = blob_dir
    try:
        return pickle.loads(main, buffers=bufs)
    finally:
        _reading.blob_dir = previous


def external_blobs(head: bytes | memoryview) -> list[bytes]:
    """The blob ids an encoded value links (its external buffers, or the
    parts of a tuple stored in parts), from its header alone; `head` must hold
    the whole header (see header_size)."""
    view = memoryview(head)
    if bytes(view[:4]) == PARTS_MAGIC:
        return [part for part, _, _ in _seq_spans(view) or () if part != _NO_BLOB]
    if bytes(view[:4]) == SPARSE_MAGIC:
        return _sparse_ids(view)
    magic, flags, n, _, _ = _HEAD.unpack_from(view, 0)
    if magic != MAGIC or not flags & EXTERNAL:
        return []
    off = _HEAD.size + 8 * n
    ids = [bytes(view[off + 16 * i : off + 16 * (i + 1)]) for i in range(n)]
    return [b for b in ids if b != _NO_BLOB]


def header_size(n_buffers: int) -> int:
    """Upper bound on the header of a value with `n_buffers` buffers."""
    return _HEAD.size + 32 * n_buffers


def load_blob(blob_id: bytes) -> Any:
    """Unpickle target of a promoted argument: the value stored in a blob of
    the run whose object is being decoded."""
    blob_dir = getattr(_reading, "blob_dir", None)
    if blob_dir is None:
        raise ValueError("a stored argument was decoded outside of a call")
    return loads(_blob_view(blob_dir, blob_id), blob_dir)


def _write_file_python(path: str, parts: list[bytes | memoryview]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp{os.getpid()}-{threading.get_ident()}"
    try:
        with open(tmp, "wb") as f:
            for p in parts:
                f.write(p)
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)  # a failed write leaves nothing behind
        except OSError:
            pass
        raise


def load_inline(data: bytes) -> Any:
    """Unpickle target of an encoded value embedded in a call frame."""
    return loads(data, getattr(_reading, "blob_dir", None))


def write_file(path: str, parts: list[bytes | memoryview]) -> None:
    """Write an object file atomically (temporary name, then rename). A
    failed write leaves nothing behind; a full disk is named in the error."""
    directory = os.path.dirname(path)
    try:
        # Native: no Python-level copies, and large files are filled by several
        # threads at once (see objects.rs). An extension built before it has
        # none: write from Python then.
        native = getattr(_native, "write_object", None)
        if native is not None:
            native(path, parts)
        else:
            _write_file_python(path, parts)
    except OSError as e:
        if e.errno != errno.ENOSPC:
            raise
        size = sum(p.nbytes if isinstance(p, memoryview) else len(p) for p in parts)
        try:
            st = os.statvfs(directory)
            free = f"{st.f_bavail * st.f_frsize >> 20} MiB free"
        except OSError:
            free = "free space unknown"
        raise OutOfDisk(
            errno.ENOSPC,
            f"no space left on the disk holding {directory} ({free}) for a {size >> 20} MiB object. "
            "A run's stored arguments are deleted when it ends; free space there, or set CHIMAERA_HOME "
            "to a directory on a larger disk",
        ) from e


@functools.cache
def _bytes_main() -> bytes:
    """The pickle of a large top-level bytes value (see _big_bytes): the same
    few bytes whatever the value, as its buffer travels out of band."""
    return _pickle(_big_bytes(b"\0" * _OOB_MIN), _Pickler, lambda buf: False)


def _plain_bytes(head: bytes, fd: int, run_dir: str) -> bytes | None:
    """A stored top-level bytes value, read with one ``pread`` from wherever
    its buffer lives (this file, or the blob it links); None for any other
    object. Decoding through a mapping would copy from freshly mapped pages,
    and on a VM their page faults cost about as much as the copy (170 to 390
    ms per GB): a call taking a 256 MiB value would spend half its time there."""
    if len(head) < _HEAD.size:
        return None
    magic, flags, n, _, main_len = _HEAD.unpack_from(head, 0)
    if magic != MAGIC or n != 1 or flags & ~(EXTERNAL | OFFSETS):
        return None
    main = _bytes_main()
    if main_len != len(main):
        return None
    off = _HEAD.size
    (length,) = struct.unpack_from("<Q", head, off)
    off += 8
    blob = None
    if flags & EXTERNAL:
        blob = head[off : off + 16]
        off += 16
    start = 0
    if flags & OFFSETS:
        (start,) = struct.unpack_from("<Q", head, off)
        off += 8
    off += _pad(off)
    if head[off : off + main_len] != main:
        return None
    if blob is None:
        if fd < 0:
            return None  # a frame held in memory: its bytes are right there
        start = off + main_len + _pad(off + main_len)
        data = _pread(fd, length, start)
    else:
        try:
            src = os.open(os.path.join(run_dir, blob.hex()), os.O_RDONLY)
        except FileNotFoundError:
            return None  # held by another peer: the mapped path fetches it
        try:
            data = _pread(src, length, start)
        finally:
            os.close(src)
    return data if len(data) == length else None


def _pread(fd: int, length: int, offset: int) -> bytes:
    """`length` bytes of `fd` at `offset` (fewer if the file is shorter).
    From 1 MiB into a new bytes object on the heap rather than in the arena,
    large ones read on several threads (see _native.pread_heap): ten calls
    reading a 256 MiB value at once took 235 ms into the arena, 141 here."""
    if _pread_heap is not None and length >= 1 << 20:
        try:
            return _pread_heap(fd, length, offset)
        except EOFError:
            return b""
    return os.pread(fd, length, offset)


def read_file(path: str) -> Any:
    """Map an object file and decode it; out-of-band buffers stay mapped
    (except a large bytes value's, read in one copy: see _plain_bytes)."""
    run_dir = os.path.dirname(path)
    fd = os.open(path, os.O_RDONLY)
    try:
        size = os.fstat(fd).st_size
        if 0 < size <= 512:
            # Whole, in one read: most small results, and the head of a large
            # bytes value whose buffer is stored apart.
            head = bytearray(size)  # writable, as _map's read would be
            if os.preadv(fd, [head], 0) == size:
                data = _plain_bytes(head, fd, run_dir)
                return data if data is not None else loads(head, run_dir)
        elif size > _READ_MAX:
            data = _plain_bytes(os.pread(fd, 512, 0), fd, run_dir)
            if data is not None:
                return data
    finally:
        os.close(fd)
    return loads(_map(path), run_dir)


def object_path(object_dir: str, run: bytes, object_id: bytes) -> str:
    return os.path.join(object_dir, run.hex(), object_id.hex())


def run_dir(object_dir: str, run: bytes) -> str:
    return os.path.join(object_dir, run.hex())


def dump_code(obj: Any, names: types.ModuleType | None = None) -> bytes:
    """Callables ship by value from __main__ and notebooks, by reference from
    modules. A durable workflow's function always ships by value, so a retry
    runs the code its history started with, not what is deployed by then.
    cloudpickle erases source filenames (for deterministic output), so the
    origin file travels alongside and is restored for readable tracebacks.

    `names` is a module run apart from sys.modules (a deployment newer than
    the copy this process imported): what of it would go by name if it were
    the imported copy goes by name too."""
    try:
        origin = inspect.getsourcefile(obj)
    except TypeError:
        origin = None
    if isinstance(obj, (types.FunctionType, functools.partial)) and getattr(obj, "__chimaera_workflow__", False):
        obj = _unnamed(obj)
    if names is None:
        return cloudpickle.dumps((obj, origin), protocol=5)
    out = io.BytesIO()
    _ByName(out, names).dump((obj, origin))
    return out.getvalue()


class _ByName(cloudpickle.Pickler):
    """Pickles the functions and classes of `module` (and what pickles as a
    global, such as an lru_cache function) by name, as cloudpickle does those
    of the module sys.modules has: they load as the loading process's own."""

    def __init__(self, file: Any, module: types.ModuleType) -> None:
        super().__init__(file, protocol=5)
        self.module = module

    def reducer_override(self, obj: Any) -> Any:
        name = getattr(obj, "__qualname__", None)
        if isinstance(name, str) and getattr(obj, "__module__", None) == self.module.__name__:
            get = operator.attrgetter(name)
            try:
                found = get(self.module) is obj
            except AttributeError:  # a nested function or class: by value
                found = False
            if found and (isinstance(obj, (types.FunctionType, type)) or isinstance(obj.__reduce_ex__(5), str)):
                return get, (self.module,)  # the module itself pickles by name
        return super().reducer_override(obj)


def _unnamed(fn: Any) -> Any:
    """A copy of `fn` (of a partial's function) that no module names, which
    cloudpickle sends by value."""
    if isinstance(fn, functools.partial):
        func = _unnamed(fn.func) if isinstance(fn.func, (types.FunctionType, functools.partial)) else fn.func
        bound = functools.partial(func, *fn.args, **fn.keywords)
        bound.__dict__.update(fn.__dict__)
        return bound
    copy = types.FunctionType(fn.__code__, fn.__globals__, fn.__name__, fn.__defaults__, fn.__closure__)
    copy.__kwdefaults__, copy.__qualname__, copy.__module__ = fn.__kwdefaults__, fn.__qualname__, fn.__module__
    copy.__doc__ = fn.__doc__
    copy.__dict__.update(fn.__dict__)
    return copy


def load_code(blob: bytes) -> Any:
    obj, origin = pickle.loads(blob)
    if origin:
        _restore_filenames(obj, origin)
    return obj


def _rename(code: types.CodeType, origin: str) -> types.CodeType:
    if code.co_filename != "<dynamic-code>":
        return code
    consts = tuple(_rename(c, origin) if isinstance(c, types.CodeType) else c for c in code.co_consts)
    return code.replace(co_filename=origin, co_consts=consts)


def _restore_filenames(obj: Any, origin: str) -> None:
    targets = [obj]
    if inspect.isclass(obj):
        targets = [v for v in vars(obj).values()]
    for t in targets:
        fn = getattr(t, "__func__", t)
        if isinstance(fn, functools.partial):
            fn = fn.func
        code = getattr(fn, "__code__", None)
        if isinstance(code, types.CodeType):
            try:
                fn.__code__ = _rename(code, origin)
            except (AttributeError, TypeError, ValueError):
                pass  # builtins and C functions keep their own code


class Dep:
    """Placeholder for the i-th dependency inside a pickled call frame."""

    __slots__ = ("index",)

    def __init__(self, index: int) -> None:
        self.index = index

    def __reduce__(self) -> tuple[Any, ...]:
        return (Dep, (self.index,))
