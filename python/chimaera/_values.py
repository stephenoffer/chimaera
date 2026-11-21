"""Large values: argument promotion, content-addressed blobs and the lints
that go with them.

A call's arguments are encoded by size (`_runtime.pack`):

- under 64 KiB the frame travels inline;
- up to 1 MiB it is one object file for that call, dropped when the call ends;
- every out-of-band buffer (NumPy, Arrow, bytes-like) of 1 MiB or more, and
  every top-level argument that pickles to 1 MiB or more, becomes a blob named
  by its blake3 digest and written once per run.

So the same 200 MB array passed to a thousand calls is hashed a thousand times
(without the GIL, on all cores) but written and shipped once; Ray copies it
into its object store on every call. Values are snapshotted at submission,
exactly as before: nothing is shared with the caller's later mutations.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import warnings
import weakref
from typing import Any, Callable

from . import _objects, _serde
from ._native import blake3_buffer

# Out-of-band buffers and top-level arguments at least this large become blobs.
PROMOTE_BYTES = 1 << 20
# CH107: the same blob from one call site this many times...
REPEAT_WARN_CALLS = 20
# ...after this much time spent hashing and pickling it.
REPEAT_WARN_S = 0.1


def blob_path(object_dir: str, run: bytes, blob_id: bytes) -> str:
    """Blobs are ordinary objects of their run, named by content."""
    return _serde.object_path(object_dir, run, blob_id)


class LocalBlobSink:
    """Writes blobs into the local peer's object directory (the client and its
    peer share a filesystem). Remote clients upload through the peer instead."""

    def __init__(self, object_dir: str) -> None:
        self.object_dir = object_dir
        self._written: set[tuple[bytes, bytes]] = set()
        self._lock = threading.Lock()

    def put(self, run: bytes, blob_id: bytes, data: bytes | memoryview) -> None:
        key = (run, blob_id)
        with self._lock:
            if key in self._written:
                return
        path = blob_path(self.object_dir, run, blob_id)
        if not os.path.exists(path):
            _serde.write_file(path, [data])
        with self._lock:
            self._written.add(key)


# Read-only buffers of objects that can be weakly referenced (NumPy arrays with
# writeable=False, Arrow buffers): hashed once while the object lives.
_memo: dict[int, tuple[weakref.ref[Any], int, bytes]] = {}
_memo_lock = threading.Lock()


def blob_id_of(view: memoryview) -> bytes:
    """blake3 digest (first 16 bytes) of a buffer, memoized for read-only ones."""
    owner = view.obj
    if view.readonly and owner is not None:
        try:
            ref = weakref.ref(owner)
        except TypeError:
            ref = None
        if ref is not None:
            key = id(owner)
            with _memo_lock:
                hit = _memo.get(key)
            if hit is not None and hit[0]() is owner and hit[1] == view.nbytes:
                return hit[2]
            digest = blake3_buffer(view)[:16]

            def forget(_: object, key: int = key) -> None:
                with _memo_lock:
                    _memo.pop(key, None)

            with _memo_lock:
                _memo[key] = (weakref.ref(owner, forget), view.nbytes, digest)
            return digest
    return blake3_buffer(view)[:16]


class BlobRef:
    """A top-level argument stored as a blob; unpickles to the value itself."""

    __slots__ = ("blob_id", "size")

    def __init__(self, blob_id: bytes, size: int) -> None:
        self.blob_id, self.size = blob_id, size

    def __reduce__(self) -> tuple[Any, ...]:
        return (_serde.load_blob, (self.blob_id,))


class InlineRef:
    """A stored value's encoded bytes inside a call frame; unpickles to the
    value itself (its large arrays are mapped from their blobs, not copied)."""

    __slots__ = ("data",)

    def __init__(self, data: bytes) -> None:
        self.data = data

    def __reduce__(self) -> tuple[Any, ...]:
        return (_serde.load_inline, (self.data,))


class Encoded:
    """An encoded call frame and the blobs it reads."""

    __slots__ = ("parts", "size", "oob", "blobs", "seconds")

    def __init__(self, parts: list[bytes | memoryview], size: int, oob: int, blobs: dict[bytes, int], seconds: float):
        self.parts, self.size, self.oob, self.blobs, self.seconds = parts, size, oob, blobs, seconds

    @property
    def total_bytes(self) -> int:
        """Frame plus blobs: what the call reads, for windows and accounting."""
        return self.size + sum(self.blobs.values())


def store(value: Any, run: bytes, sink: LocalBlobSink, inline_limit: int) -> tuple[bytes, bytes | None, int, list[str]]:
    """Store one value (``ch.put``): (object id, inline bytes or None, size,
    files that only this value uses).

    Locally nothing is hashed: arena arrays are linked in place (no copy, see
    _objects), everything else is written once by the native writer, and the
    object gets a fresh id. A remote client stores by content instead, so an
    upload the peer already has is skipped (and nothing is owned)."""
    if isinstance(sink, LocalBlobSink):
        rdir = _serde.run_dir(sink.object_dir, run)
        owned: list[str] = []

        def claim(view: memoryview) -> tuple[bytes, int] | None:
            got = _objects.claim(view, rdir) if view.nbytes >= PROMOTE_BYTES else None
            if got is not None:
                owned.append(os.path.join(rdir, got[0].hex()))
            return got

        parts, size, _ = _serde.dumps_parts(value, external=claim)
        object_id = os.urandom(16)
        if size <= inline_limit:
            return object_id, b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts), size, owned
        path = _serde.object_path(sink.object_dir, run, object_id)
        _serde.write_file(path, parts)
        return object_id, None, size, [*owned, path]

    def external(view: memoryview) -> bytes | None:
        if view.nbytes < PROMOTE_BYTES:
            return None
        bid = blob_id_of(view)
        sink.put(run, bid, view)
        return bid

    parts, size, _ = _serde.dumps_parts(value, external=external)
    data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
    object_id = blake3_buffer(data)[:16]
    if size <= inline_limit:
        return object_id, data, size, []
    sink.put(run, object_id, data)
    return object_id, None, size, []


# (run dir, inode) -> the first blob id an arena array was claimed under.
_first_claims: dict[tuple[str, int], bytes] = {}


def _same_claim(run_dir: str, claimed: tuple[bytes, int]) -> tuple[bytes, int]:
    """An arena array passed again unchanged links the same file (a changed
    one is not claimed at all): reuse the name it got first, so other peers
    see one object and fetch it once instead of once per call."""
    blob, off = claimed
    path = os.path.join(run_dir, blob.hex())
    try:
        inode = os.stat(path).st_ino
    except OSError:
        return claimed
    first = _first_claims.get((run_dir, inode))
    if first is not None and first != blob:
        try:
            if os.stat(os.path.join(run_dir, first.hex())).st_ino == inode:
                os.unlink(path)
                return first, off
        except OSError:
            pass  # the first name is gone: this one becomes the first
    _first_claims[(run_dir, inode)] = blob
    return claimed


def encode_frame(frame: tuple[tuple[Any, ...], dict[str, Any]], run: bytes, sink: LocalBlobSink) -> Encoded:
    """Encode `(args, kwargs)`, moving large buffers and arguments into blobs."""
    t0 = time.perf_counter()
    blobs: dict[bytes, int] = {}

    local_dir = _serde.run_dir(sink.object_dir, run) if isinstance(sink, LocalBlobSink) else None

    def external(view: memoryview) -> bytes | tuple[bytes, int] | None:
        if view.nbytes < PROMOTE_BYTES:
            return None
        if local_dir is not None:
            claimed = _objects.claim(view, local_dir)  # an arena array: linked, not hashed or copied
            if claimed is not None:
                claimed = _same_claim(local_dir, claimed)
                blobs[claimed[0]] = view.nbytes
                return claimed
        bid = blob_id_of(view)
        sink.put(run, bid, view)
        blobs[bid] = view.nbytes
        return bid

    parts, size, oob = _serde.dumps_parts(frame, external=external)
    if size > PROMOTE_BYTES:
        # Large without buffers (big dicts, lists, objects): store each large
        # top-level argument once and pass a reference that loads it.
        args, kwargs = frame
        promote = _promoter(run, sink, external, blobs)
        frame = (tuple(promote(a) for a in args), {k: promote(v) for k, v in kwargs.items()})
        parts, size, oob = _serde.dumps_parts(frame, external=external)
    return Encoded(parts, size, oob, blobs, time.perf_counter() - t0)


def _promoter(
    run: bytes, sink: LocalBlobSink, external: Callable[[memoryview], bytes | None], blobs: dict[bytes, int]
) -> Callable[[Any], Any]:
    def promote(x: Any) -> Any:
        if isinstance(x, (_serde.Dep, BlobRef, InlineRef)) or type(x).__name__ in ("Future", "ObjectRef"):
            return x
        parts, size, _ = _serde.dumps_parts(x, external=external)
        if size < PROMOTE_BYTES:
            return x
        data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
        bid = blake3_buffer(data)[:16]
        sink.put(run, bid, data)
        blobs[bid] = len(data)
        return BlobRef(bid, len(data))

    return promote


class RepeatTracker:
    """CH107: the same large value passed again and again from one call site.
    It is stored once automatically, but still hashed (and, for non-buffer
    objects, pickled) on every call."""

    def __init__(self) -> None:
        self._seen: dict[tuple[str, bytes], list[float]] = {}
        self._warned: set[str] = set()

    def note(self, site: str | None, enc: Encoded) -> None:
        if site is None or site in self._warned or not enc.blobs:
            return
        bid = max(enc.blobs, key=enc.blobs.__getitem__)
        entry = self._seen.setdefault((site, bid), [0, 0.0])
        entry[0] += 1
        entry[1] += enc.seconds
        if entry[0] >= REPEAT_WARN_CALLS and entry[1] >= REPEAT_WARN_S:
            self._warned.add(site)
            self._seen = {k: v for k, v in self._seen.items() if k[0] != site}
            warnings.warn(
                f"CH107: calls from {site} passed the same {enc.blobs[bid] >> 20} MiB value {int(entry[0])} times; "
                f"it is stored once automatically, but encoding it took {entry[1]:.2f} s in total. "
                "Pass ch.put(value) instead, or bind it once with fn.partial(value).",
                stacklevel=4,
            )


class FetchedTracker:
    """CH103: a large value fetched with `.result()` and passed straight back
    into `.remote()` makes a round trip through the caller."""

    def __init__(self) -> None:
        self._fetched: dict[int, weakref.ref[Any]] = {}
        self._warned: set[str] = set()

    def note_fetched(self, value: Any, size: int) -> None:
        # The value's own size: a stored result that links its arrays (the
        # Linux arena) reports only the small frame that refers to them.
        if max(size, getattr(value, "nbytes", 0) or 0) < PROMOTE_BYTES:
            return
        try:
            key = id(value)
            self._fetched[key] = weakref.ref(value, lambda _, key=key: self._fetched.pop(key, None))
        except TypeError:
            pass  # lists and dicts cannot be weakly referenced; not tracked

    def check(self, site: str | None, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        if site is None or site in self._warned or not self._fetched:
            return
        for x in (*args, *kwargs.values()):
            ref = self._fetched.get(id(x))
            if ref is not None and ref() is x:
                self._warned.add(site)
                warnings.warn(
                    f"CH103: {site} passes a large value that was just fetched with .result(); "
                    "pass the future itself so the value goes straight from one call to the next.",
                    stacklevel=4,
                )
                return


repeats = RepeatTracker()
fetched = FetchedTracker()


def torch_reducers() -> dict[type, Callable[[Any], Any]]:
    """Zero-copy reduction for CPU torch tensors, only when torch is already
    imported (never imports it): through NumPy, so the bytes travel out of band
    like any array instead of being copied into the pickle."""
    torch = sys.modules.get("torch")
    if torch is None:
        return {}

    def reduce_tensor(t: Any) -> Any:
        if t.device.type != "cpu" or t.requires_grad or t.is_sparse or not t.is_contiguous():
            return t.__reduce_ex__(5)
        try:
            arr = t.numpy()
        except (TypeError, RuntimeError):  # dtypes NumPy lacks (bfloat16)
            return t.__reduce_ex__(5)
        return (torch.from_numpy, (arr,))  # the array itself travels out of band

    return {torch.Tensor: reduce_tensor}
