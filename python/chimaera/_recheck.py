"""A writable buffer passed to calls again: compare it with the blob it is
stored as instead of hashing it again.

A writable array may change between calls, so every call must read all of it.
Hashing 200 MB takes 13 ms on twelve idle cores and over 30 ms on a busy
machine; comparing it with the blob, kept mapped, reads it once, which is
memory-bound (4 ms on four threads of an M-series Mac, 10 ms on one when the
machine is busy). The check is exact and made against the file the call will
read: one changed byte and the buffer is hashed and stored again.

On Linux with the arena, NumPy arrays never get here (they are linked, not
hashed). This serves other buffers, and hosts without the arena (macOS).
"""

from __future__ import annotations

import collections
import ctypes
import mmap
import os
import threading
import weakref
from typing import Any, Callable

# Buffers remembered (the most recently passed): a blob freed or spilled while
# still mapped here keeps its pages until its entry goes.
KEEP = 8
# Buffers this large are compared by several threads (memcmp releases the GIL).
SPLIT_BYTES = 32 << 20
THREADS = 4

def _libc_memcmp() -> Callable[..., int] | None:
    try:
        f = ctypes.CDLL(None).memcmp
    except (AttributeError, OSError):
        return None
    f.restype = ctypes.c_int
    f.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    return f


_memcmp = _libc_memcmp()


class _Seen:
    __slots__ = ("ref", "digest", "ino", "map")

    def __init__(self, ref: weakref.ref[Any], digest: bytes, ino: int, m: mmap.mmap) -> None:
        self.ref, self.digest, self.ino, self.map = ref, digest, ino, m


_seen: collections.OrderedDict[int, _Seen] = collections.OrderedDict()
_lock = threading.Lock()


def _entry(view: memoryview) -> _Seen | None:
    with _lock:
        s = _seen.get(id(view.obj))
        if s is None or s.ref() is not view.obj:
            return None
        _seen.move_to_end(id(view.obj))
        return s


def _forget(key: int) -> None:
    with _lock:
        _seen.pop(key, None)


def stored_as(view: memoryview, run_dir: str | None) -> bytes | None:
    """The blob id `view` is stored under in `run_dir` if its bytes are still
    exactly that blob's; None: hash it."""
    if run_dir is None or _memcmp is None or view.readonly:
        return None
    s = _entry(view)
    if s is None:
        return None
    n = view.nbytes
    try:
        st = os.stat(os.path.join(run_dir, s.digest.hex()))
    except OSError:
        st = None
    if st is None or st.st_ino != s.ino or st.st_size != n or len(s.map) != n:
        _forget(id(view.obj))  # freed, spilled, or another run's: hash it
        return None
    try:
        a = (ctypes.c_char * n).from_buffer(view)
    except (TypeError, ValueError, BufferError):
        return None
    b = (ctypes.c_char * n).from_buffer(s.map)
    try:
        same = _equal(ctypes.addressof(a), ctypes.addressof(b), n)
    finally:
        del a, b
    return s.digest if same else None


def _equal(pa: int, pb: int, n: int) -> bool:
    """memcmp in up to THREADS pieces, started here so none outlive the
    call (a pool's threads would be missing in a forked child). A piece
    that does not finish counts as different."""
    assert _memcmp is not None
    k = min(THREADS, max(1, n // SPLIT_BYTES))
    step = -(-n // k)
    diff = [1] * k

    def piece(j: int) -> None:
        off = j * step
        diff[j] = _memcmp(pa + off, pb + off, min(step, n - off))

    others = [threading.Thread(target=piece, args=(j,), daemon=True) for j in range(1, k)]
    for t in others:
        t.start()
    piece(0)
    for t in others:
        t.join()
    return not any(diff)


def remember(view: memoryview, digest: bytes, run_dir: str | None) -> None:
    """`view` is stored as blob `digest` in `run_dir`: map that blob, to
    compare the buffer with when it is passed again."""
    if run_dir is None or _memcmp is None or view.readonly or view.nbytes == 0:
        return
    s = _entry(view)
    if s is not None and s.digest == digest:
        return
    owner, key = view.obj, id(view.obj)
    try:
        ref = weakref.ref(owner, lambda _: _forget(key))
        with open(os.path.join(run_dir, digest.hex()), "rb") as f:
            ino = os.fstat(f.fileno()).st_ino
            m = mmap.mmap(f.fileno(), view.nbytes, access=mmap.ACCESS_COPY)
    except (TypeError, OSError, ValueError):
        return  # not weakly referable, not stored here, or no private mapping
    with _lock:
        _seen[key] = _Seen(ref, digest, ino, m)
        _seen.move_to_end(key)
        while len(_seen) > KEEP:
            _seen.popitem(last=False)
