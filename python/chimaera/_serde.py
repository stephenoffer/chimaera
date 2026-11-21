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
import mmap
import os
import pickle
import struct
import threading
import types
from typing import Any, Callable

import cloudpickle

from . import _native

MAGIC = b"CHO1"
_HEAD = struct.Struct("<4sIIIQ")
_ALIGN = 64
_OOB_MIN = 64 * 1024  # smaller buffers stay inside the pickle
EXTERNAL = 1  # header flag: a blob-id table follows the buffer lengths
OFFSETS = 2  # header flag: an offset table follows the blob ids
_NO_BLOB = bytes(16)

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
        if isinstance(obj, (type, types.FunctionType)) and getattr(obj, "__module__", None) == "__main__":
            raise _MainRefused("refers to __main__")
        return NotImplemented


# Containers at least this long are checked for plain data (see plain_tree).
_FAST_MIN = 1024
_ATOMS = (bytes, bytearray, str, int, float, bool, type(None))
_plain = getattr(_native, "plain_tree", lambda _: False)  # absent from older builds


def _pickle(value: Any, pickler_cls: type, buffer_callback: Callable[[pickle.PickleBuffer], bool]) -> bytes:
    from ._values import torch_reducers

    f = io.BytesIO()
    p = pickler_cls(f, protocol=5, buffer_callback=buffer_callback)
    extra = torch_reducers()
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


def _big_bytes(b: bytes | bytearray) -> Any:
    """A large top-level bytes value pickled out of band (pickle always copies
    bytes into the stream): its buffer goes straight into the object file."""

    class Reduced:
        def __reduce_ex__(self, protocol: int) -> tuple[Any, ...]:
            return (type(b), (pickle.PickleBuffer(b),))

    return Reduced()


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

    def keep(buf: pickle.PickleBuffer) -> bool:
        raw = buf.raw()
        if raw.nbytes < _OOB_MIN:
            return True
        blob = external(raw) if external is not None else None
        if isinstance(blob, tuple):
            buffers.append((raw, blob[0], blob[1]))
        else:
            buffers.append((raw, blob or _NO_BLOB, 0))
        return False

    if type(value) in (bytes, bytearray) and len(value) >= _OOB_MIN:
        value = _big_bytes(value)
    try:
        main = _pickle(value, _Pickler, keep)
    except (pickle.PicklingError, AttributeError, TypeError):
        buffers.clear()  # the failed attempt may have kept some already
        main = _pickle(value, cloudpickle.Pickler, keep)
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


def _map(path: str) -> mmap.mmap:
    """Map a file copy-on-write (writable views, writes stay private); fall
    back to read-only where the OS refuses private writable mappings."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size == 0:
            raise ValueError(f"empty object file {path}")
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


def loads(data: bytes | memoryview | mmap.mmap, blob_dir: str | None = None) -> Any:
    """Decode an object. `blob_dir` is the run's object directory, where any
    external buffers and promoted arguments live."""
    view = memoryview(data)
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
        raise OSError(
            errno.ENOSPC,
            f"no space left on the disk holding {directory} ({free}) for a {size >> 20} MiB object. "
            "A run's stored arguments are deleted when it ends; free space there, or set CHIMAERA_HOME "
            "to a directory on a larger disk",
        ) from e


def read_file(path: str) -> Any:
    """Map an object file and decode it; out-of-band buffers stay mapped."""
    return loads(_map(path), os.path.dirname(path))


def object_path(object_dir: str, run: bytes, object_id: bytes) -> str:
    return os.path.join(object_dir, run.hex(), object_id.hex())


def run_dir(object_dir: str, run: bytes) -> str:
    return os.path.join(object_dir, run.hex())


def dump_code(obj: Any) -> bytes:
    """Callables ship by value from __main__ and notebooks, by reference from
    modules. cloudpickle erases source filenames (for deterministic output), so
    the origin file travels alongside and is restored for readable tracebacks."""
    try:
        origin = inspect.getsourcefile(obj)
    except TypeError:
        origin = None
    return cloudpickle.dumps((obj, origin), protocol=5)


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
