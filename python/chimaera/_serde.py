"""Value encoding shared by clients and executors.

Object layout (also used for inline values)::

    b"CHO1" | flags u32 | n_buffers u32 | reserved u32 | main_len u64 | buf_len u64 * n
    pad to 64 | main pickle | pad to 64 | buffer 0 | pad to 64 | buffer 1 ...

The main frame is a protocol-5 pickle; large contiguous buffers (NumPy arrays,
Arrow buffers) travel out of band and are mapped back without copying when a
value is read from a file. Rust never decodes these bytes.
"""

from __future__ import annotations

import functools
import inspect
import mmap
import os
import pickle
import struct
import types
from typing import Any

import cloudpickle

MAGIC = b"CHO1"
_HEAD = struct.Struct("<4sIIIQ")
_ALIGN = 64
_OOB_MIN = 64 * 1024  # smaller buffers stay inside the pickle


def _pad(n: int) -> int:
    return (-n) % _ALIGN


def dumps_parts(value: Any) -> tuple[list[bytes | memoryview], int, int]:
    """Encode `value`; return (parts, total_size, n_out_of_band_buffers)."""
    buffers: list[pickle.PickleBuffer] = []

    def keep(buf: pickle.PickleBuffer) -> bool:
        if buf.raw().nbytes >= _OOB_MIN:
            buffers.append(buf)
            return False
        return True

    try:
        main = pickle.dumps(value, protocol=5, buffer_callback=keep)
    except (pickle.PicklingError, AttributeError, TypeError):
        buffers.clear()
        main = cloudpickle.dumps(value, protocol=5, buffer_callback=keep)
    raws = [b.raw() for b in buffers]
    head = _HEAD.pack(MAGIC, 0, len(raws), 0, len(main)) + b"".join(struct.pack("<Q", r.nbytes) for r in raws)
    parts: list[bytes | memoryview] = [head, b"\0" * _pad(len(head)), main]
    size = len(head) + _pad(len(head)) + len(main)
    for r in raws:
        parts.append(b"\0" * _pad(size))
        size += _pad(size)
        parts.append(r)
        size += r.nbytes
    return parts, size, len(raws)


def dumps(value: Any) -> tuple[bytes, int]:
    parts, _, n = dumps_parts(value)
    return b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts), n


def loads(data: bytes | memoryview | mmap.mmap) -> Any:
    view = memoryview(data)
    magic, _flags, n, _, main_len = _HEAD.unpack_from(view, 0)
    if magic != MAGIC:
        raise ValueError("not a Chimaera object")
    off = _HEAD.size
    lens = [struct.unpack_from("<Q", view, off + 8 * i)[0] for i in range(n)]
    off += 8 * n
    off += _pad(off)
    main = view[off : off + main_len]
    off += main_len
    bufs = []
    for ln in lens:
        off += _pad(off)
        bufs.append(view[off : off + ln])
        off += ln
    return pickle.loads(main, buffers=bufs)


def write_file(path: str, parts: list[bytes | memoryview]) -> None:
    """Write an object file atomically (temporary name, then rename)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "wb") as f:
        for p in parts:
            f.write(p)
    os.replace(tmp, path)


def read_file(path: str) -> Any:
    """Map an object file and decode it; out-of-band buffers stay mapped."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size == 0:
            raise ValueError(f"empty object file {path}")
        m = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
    return loads(m)


def object_path(object_dir: str, run: bytes, object_id: bytes) -> str:
    return os.path.join(object_dir, run.hex(), object_id.hex())


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
