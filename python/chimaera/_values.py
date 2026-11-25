"""Large values: argument promotion, content-addressed blobs and the lints
that go with them.

A call's arguments are encoded by size (`_runtime.pack`):

- under 64 KiB the frame travels inline;
- up to 1 MiB it is one object file for that call, dropped when the call ends;
- every out-of-band buffer (NumPy, Arrow, bytes-like) of 1 MiB or more, and
  every top-level argument that pickles to 1 MiB or more, becomes a blob named
  by its blake3 digest, written once while something holds it: the array it
  came from, or a call still to read it (see ClaimHold).

So the same 200 MB array passed to a thousand calls is hashed a thousand times
(without the GIL, on all cores) but written and shipped once; Ray copies it
into its object store on every call. Values are snapshotted at submission,
exactly as before: nothing is shared with the caller's later mutations.
"""

from __future__ import annotations

import collections
import os
import sys
import threading
import time
import warnings
import weakref
from typing import Any, Callable

from . import _objects, _recheck, _serde
from ._native import blake3_buffer

# Out-of-band buffers and top-level arguments at least this large become blobs.
PROMOTE_BYTES = 1 << 20
# CH118: a Python collection that pickles to at least this much, read by
# calls: every reader unpickles a full copy of it.
COLLECTION_WARN_BYTES = 8 << 20
_COLLECTIONS = (list, dict, set, frozenset, tuple)
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

    def forget(self, run: bytes, blob_id: bytes) -> None:
        """The blob was freed: store it again if it is sent again."""
        with self._lock:
            self._written.discard((run, blob_id))


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

    __slots__ = ("parts", "size", "oob", "blobs", "seconds", "holds", "collection")

    def __init__(
        self,
        parts: list[bytes | memoryview],
        size: int,
        oob: int,
        blobs: dict[bytes, int],
        seconds: float,
        holds: list[ClaimHold] | None = None,
    ):
        self.parts, self.size, self.oob, self.blobs, self.seconds = parts, size, oob, blobs, seconds
        self.holds = holds or []
        # The largest Python collection stored for the call: (type name, bytes).
        self.collection: tuple[str, int] | None = None

    @property
    def total_bytes(self) -> int:
        """Frame plus blobs: what the call reads, for windows and accounting."""
        return self.size + sum(self.blobs.values())


def store(
    value: Any,
    run: bytes,
    sink: LocalBlobSink,
    inline_limit: int,
    prefix: bytes = b"",
    device_holds: list[Any] | None = None,
) -> tuple[bytes, bytes | None, int, list[str]]:
    """Store one value (``ch.put``): (object id, inline bytes or None, size,
    files that only this value uses). Local ids start with `prefix`: a put
    made inside a call passes its run's prefix, so its files are named like
    results and outlive the run's values (the call may return the handle).
    With `device_holds`, large CUDA tensors are also shared with readers on
    this machine (see _cuda_share) and added to it, for the caller to keep.

    Locally nothing is hashed: arena arrays are linked in place (no copy, see
    _objects), everything else is written once by the native writer, and the
    object gets a fresh id. A remote client stores by content instead, so an
    upload the peer already has is skipped (and nothing is owned)."""
    if isinstance(sink, LocalBlobSink):
        rdir = _serde.run_dir(sink.object_dir, run)
        owned: list[str] = []

        def claim(view: memoryview) -> tuple[bytes, int] | None:
            got = _objects.claim(view, rdir, prefix) if view.nbytes >= PROMOTE_BYTES else None
            if got is not None:
                owned.append(os.path.join(rdir, got[0].hex()))
            return got

        def write_blob(view: memoryview) -> tuple[bytes, bytes]:
            # A CUDA tensor's host copy, kept apart from the value so a reader
            # that takes it GPU to GPU never fetches it (see tensor_from_gpu).
            blob = prefix + os.urandom(16 - len(prefix))
            path = os.path.join(rdir, blob.hex())
            _serde.write_file(path, [view])
            owned.append(path)
            return run, blob

        if device_holds is not None:
            _put_holds.list = device_holds
            _put_holds.write_blob = write_blob
        try:
            parts, size, _ = _serde.dumps_parts(value, external=claim)
        finally:
            if device_holds is not None:
                del _put_holds.list, _put_holds.write_blob
        object_id = prefix + os.urandom(16 - len(prefix))
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


class ClaimHold:
    """A blob a call reads: an arena array linked into its run (see
    _objects.claim), or a buffer or argument stored by content. A loop that
    passes a fresh array per iteration would fill memory if these lasted for
    the run. Instead the blob is dropped, here and on the peers that fetched
    it, once this hold is unreachable: its owner (the array) has been collected
    and every call that read it has an outcome (the calls' `reads` hold it
    until then); a blob stored by content is forgotten by its `sink` too, so
    the same bytes sent later are stored again. A blob stored by content whose
    owner could not be watched (bytes, lists: the program may send them again)
    waits among the recently freed (_RECENT_BYTES) before it is dropped.
    Without a `release` (inside a task, whose session owns no run) or after
    keep(), it stays for the run."""

    __slots__ = ("blob", "_fin", "_owner", "_watched", "__weakref__")

    def __init__(
        self,
        path: str | None,
        run: bytes,
        blob: bytes,
        release: Callable[[bytes, list[bytes]], None] | None,
        sink: Any = None,
        size: int = 0,
    ):
        self.blob = blob
        self._owner: Any = None
        self._watched = [False]  # shared with the finalizer: an owner was watched
        fin = (
            weakref.finalize(
                self, _drop_claim, path, run, blob, release, os.getpid(), sink, size, self._watched
            )
            if release is not None
            else None
        )
        self._fin = fin
        if self._fin is None:
            _kept.add(self)
        else:
            self._fin.atexit = False  # the run's end removes it

    def keep(self) -> None:
        """Keep for the run: an actor may read it again (a restart re-runs
        the constructor with the same arguments)."""
        if self._fin is not None and self._fin.detach() is not None:
            _kept.add(self)

    def held_by(self, owner: Any) -> None:
        """Stay at least as long as `owner`, the object whose bytes it holds,
        so passing the same live array again reuses this blob. An owner that
        cannot be watched (bytes) leaves it to the calls that read it."""
        if self._fin is None or not self._fin.alive or (self._owner is not None and self._owner() is owner):
            return
        try:
            weakref.finalize(owner, _owner_gone, self).atexit = False
            self._owner = weakref.ref(owner)
            self._watched[0] = True
        except TypeError:
            pass


def _owner_gone(hold: ClaimHold) -> None:
    """The owner was collected: its finalizer, which kept `hold` alive until
    now, lets go of it here, and the hold forgets the owner."""
    hold._owner = None


def _drop_claim(
    path: str | None,
    run: bytes,
    blob: bytes,
    release: Callable[[bytes, list[bytes]], None],
    pid: int,
    sink: Any,
    size: int,
    watched: list[bool],
) -> None:
    if sys.is_finalizing() or os.getpid() != pid:  # a forked child's copy: the blob is its parent's
        return
    with _blob_lock:  # against a call storing the same bytes again meanwhile (see _hold_blob)
        if sink is None or watched[0]:
            _drop_now(path, run, blob, release, sink)
            return
        global _recent_bytes
        _recent[(run, blob)] = (path, release, sink, size)
        _recent_bytes += size
        while _recent_bytes > _RECENT_BYTES and _recent:
            (r, b), (p, rel, snk, n) = _recent.popitem(last=False)
            _recent_bytes -= n
            _drop_now(p, r, b, rel, snk)


def _drop_now(
    path: str | None, run: bytes, blob: bytes, release: Callable[[bytes, list[bytes]], None], sink: Any
) -> None:
    if sink is not None:
        sink.forget(run, blob)
    if path is not None:
        try:
            os.unlink(path)
        except OSError:
            pass
    try:
        release(run, [blob])
    except Exception:  # noqa: S110 -- best effort during garbage collection
        pass


# (run dir, inode) -> the live hold of an arena array's link; holds kept for
# the run stay reachable through _kept.
_claims: weakref.WeakValueDictionary[tuple[str, int], ClaimHold] = weakref.WeakValueDictionary()
_kept: set[ClaimHold] = set()
# (run, blob id) -> the live hold of a blob stored by content.
_blob_holds: weakref.WeakValueDictionary[tuple[bytes, bytes], ClaimHold] = weakref.WeakValueDictionary()
_blob_lock = threading.RLock()
# Blobs stored by content that nothing holds but the program may send again
# (their owner could not be watched), oldest first, up to this many bytes.
_RECENT_BYTES = 256 << 20
_recent: collections.OrderedDict[tuple[bytes, bytes], tuple[str | None, Any, Any, int]] = collections.OrderedDict()
_recent_bytes = 0


def _hold_blob(
    run: bytes, bid: bytes, data: bytes | memoryview, sink: Any, release: Callable[[bytes, list[bytes]], None] | None
) -> ClaimHold | None:
    """Store `data` as blob `bid` of the run (once while it is held) and the
    hold that frees it when nothing needs it; None, kept for the run, without
    a `release`."""
    if release is None:
        sink.put(run, bid, data)
        return None
    global _recent_bytes
    with _blob_lock:
        hold = _blob_holds.get((run, bid))
        cached = _recent.pop((run, bid), None)
        if cached is not None:  # freed recently and still stored: reused as it is
            _recent_bytes -= cached[3]
        sink.put(run, bid, data)
        if hold is None:
            path = blob_path(sink.object_dir, run, bid) if isinstance(sink, LocalBlobSink) else None
            hold = ClaimHold(path, run, bid, release, sink, memoryview(data).nbytes)
            _blob_holds[(run, bid)] = hold
    return hold


def _claim_hold(
    run_dir: str, run: bytes, claimed: tuple[bytes, int], release: Callable[[bytes, list[bytes]], None] | None
) -> ClaimHold:
    """An arena array passed again unchanged links the same file (a changed
    one is not claimed at all): reuse the hold it already has, so other peers
    see one object and fetch it once instead of once per call."""
    path = os.path.join(run_dir, claimed[0].hex())
    try:
        inode = os.stat(path).st_ino
    except OSError:
        return ClaimHold(path, run, claimed[0], release)
    hold = _claims.get((run_dir, inode))
    if hold is not None and hold.blob != claimed[0]:
        try:
            if os.stat(os.path.join(run_dir, hold.blob.hex())).st_ino == inode:
                os.unlink(path)
                return hold
        except OSError:
            pass  # its name is gone (spilled): this link takes over
    hold = ClaimHold(path, run, claimed[0], release)
    _claims[(run_dir, inode)] = hold
    return hold


def encode_frame(
    frame: tuple[tuple[Any, ...], dict[str, Any]],
    run: bytes,
    sink: LocalBlobSink,
    release: Callable[[bytes, list[bytes]], None] | None = None,
) -> Encoded:
    """Encode `(args, kwargs)`, moving large buffers and arguments into blobs.
    Arena arrays are linked instead, each under a ClaimHold (in the result's
    `holds`) that `release`s the link once nothing needs it."""
    t0 = time.perf_counter()
    blobs: dict[bytes, int] = {}
    holds: list[ClaimHold] = []

    local_dir = _serde.run_dir(sink.object_dir, run) if isinstance(sink, LocalBlobSink) else None

    def external(view: memoryview) -> bytes | tuple[bytes, int] | None:
        if view.nbytes < PROMOTE_BYTES:
            return None
        if local_dir is not None:
            claimed = _objects.claim(view, local_dir)  # an arena array: linked, not hashed or copied
            if claimed is not None:
                hold = _claim_hold(local_dir, run, claimed, release)
                hold.held_by(view.obj)
                holds.append(hold)
                blobs[hold.blob] = view.nbytes
                return hold.blob, claimed[1]
        bid = _recheck.stored_as(view, local_dir) or blob_id_of(view)
        hold = _hold_blob(run, bid, view, sink, release)
        _recheck.remember(view, bid, local_dir)
        if hold is not None:
            hold.held_by(view.obj)
            holds.append(hold)
        blobs[bid] = view.nbytes
        return bid

    parts, size, oob = _serde.dumps_parts(frame, external=external)
    if size > PROMOTE_BYTES:
        # Large without buffers (big dicts, lists, objects): store each large
        # top-level argument once and pass a reference that loads it.
        args, kwargs = frame
        collections: list[tuple[str, int]] = []
        promote = _promoter(run, sink, external, blobs, holds, release, collections)
        frame = (tuple(promote(a) for a in args), {k: promote(v) for k, v in kwargs.items()})
        parts, size, oob = _serde.dumps_parts(frame, external=external)
        enc = Encoded(parts, size, oob, blobs, time.perf_counter() - t0, holds)
        enc.collection = max(collections, key=lambda c: c[1], default=None)
        return enc
    return Encoded(parts, size, oob, blobs, time.perf_counter() - t0, holds)


def _promoter(
    run: bytes,
    sink: LocalBlobSink,
    external: Callable[[memoryview], bytes | None],
    blobs: dict[bytes, int],
    holds: list[ClaimHold],
    release: Callable[[bytes, list[bytes]], None] | None,
    collections: list[tuple[str, int]],
) -> Callable[[Any], Any]:
    def promote(x: Any) -> Any:
        if isinstance(x, (_serde.Dep, BlobRef, InlineRef)) or type(x).__name__ in ("Future", "ObjectRef"):
            return x
        parts, size, _ = _serde.dumps_parts(x, external=external)
        if size < PROMOTE_BYTES:
            return x
        data = b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)
        if type(x) in _COLLECTIONS:
            collections.append((type(x).__name__, len(data)))
        bid = blake3_buffer(data)[:16]
        hold = _hold_blob(run, bid, data, sink, release)
        if hold is not None:
            hold.held_by(x)
            holds.append(hold)
        blobs[bid] = len(data)
        return BlobRef(bid, len(data))

    return promote


class CollectionLint:
    """CH118: a large list, dict, set or tuple shared with calls. It is stored
    once, but every call that reads it unpickles a full copy, where a NumPy
    array or an Arrow table is mapped: one copy for every reader on a machine.
    Warns once per call site."""

    def __init__(self) -> None:
        self._warned: set[str | None] = set()

    def note(self, site: str | None, kind: str, nbytes: int, stacklevel: int = 4) -> None:
        if nbytes < COLLECTION_WARN_BYTES or site in self._warned:
            return
        self._warned.add(site)
        warnings.warn(
            f"CH118: {site or 'this call'} shares a {kind} that pickles to {nbytes >> 20} MiB; every call that "
            f"reads it unpickles a full copy. A NumPy array or an Arrow table is mapped instead: one copy, "
            f"shared by every reader on a machine.",
            stacklevel=stacklevel,
        )


collection_lint = CollectionLint()


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
    like any array instead of being copied into the pickle.

    A CUDA tensor goes through host memory too (torch's own pickling copies it
    as well), but is rebuilt on the reader's GPU: torch's pickling names the
    writer's device index, which fails to load where there is no GPU and may
    name a missing or wrong device where other GPUs are visible."""
    torch = sys.modules.get("torch")
    if torch is None:
        return {}

    def reduce_tensor(t: Any) -> Any:
        if t.device.type == "cuda" and not t.requires_grad and not t.is_sparse:
            host = t.detach().cpu()
            try:
                payload: Any = host.contiguous().numpy()
            except (TypeError, RuntimeError):  # dtypes NumPy lacks: torch pickles the host copy
                payload = host
            share = _cuda_share(t)
            write_blob = getattr(_put_holds, "write_blob", None)
            if share and share.get("net") and write_blob is not None and not isinstance(payload, torch.Tensor):
                run, blob = write_blob(memoryview(payload).cast("B"))
                return (tensor_from_gpu, (run, blob, payload.shape, payload.dtype.str, share))
            return (tensor_on_device, (payload, "cuda", share) if share else (payload, "cuda"))
        if t.device.type != "cpu" or t.requires_grad or t.is_sparse or not t.is_contiguous():
            return t.__reduce_ex__(5)
        try:
            arr = t.numpy()
        except (TypeError, RuntimeError):  # dtypes NumPy lacks (bfloat16)
            return t.__reduce_ex__(5)
        return (torch.from_numpy, (arr,))  # the array itself travels out of band

    return {torch.Tensor: reduce_tensor}


_device_warned: set[str] = set()


# While ch.put stores a value: CUDA tensors shared with readers on this
# machine (see _cuda_share), which the put's future then keeps alive.
_put_holds = threading.local()
# Shared tensors whose future was passed on: kept for this process's life.
_kept_device: list[Any] = []
_SHARE_MIN = 1 << 20
# Tensors this process read straight from another process's GPU memory.
cuda_shared_reads = 0


def _boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return ""  # CUDA IPC is Linux's: no sharing elsewhere


def _cuda_share(t: Any) -> dict[str, Any] | None:
    """A CUDA IPC handle to `t` that a process on this machine can open to
    read it straight from this GPU, without the host copy. Only while a
    ch.put stores it: the put's future then holds `t`, so the memory a handle
    names is never freed and reused while a reader may still open it.
    ``CHIMAERA_CUDA_IPC=0`` turns this off."""
    holds = getattr(_put_holds, "list", None)
    host = _boot_id()
    if holds is None or not host or os.environ.get("CHIMAERA_CUDA_IPC", "1") == "0":
        return None
    if t.numel() * t.element_size() < _SHARE_MIN:
        return None  # small: the host copy costs less than the handle
    try:
        import torch
        from torch.multiprocessing.reductions import reduce_tensor as torch_reduce

        t = t.detach().contiguous()
        _, args = torch_reduce(t)
        gpu = str(torch.cuda.get_device_properties(t.device).uuid)
    except Exception:  # noqa: BLE001 -- sharing is an optimization; the host copy is always there
        return None
    holds.append(t)
    from . import _gpunet

    share = {"host": host, "pid": os.getpid(), "gpu": gpu, "args": args}
    net = _gpunet.offer(t)
    if net is not None:
        share["net"] = net  # read GPU to GPU from another machine
    return share


def _from_cuda_share(share: dict[str, Any], target: str | None) -> Any:
    """The tensor read from its owner's GPU memory when this process is on
    the same machine and sees that GPU, copied device to device onto `target`
    (or that GPU), so a write stays in the reader as for every value. None
    when it cannot be: the caller uses the host copy."""
    if share.get("host") != _boot_id() or share.get("pid") == os.getpid():
        return None  # another machine, or the owner itself (it cannot open its own handle)
    try:
        import torch
        from torch.multiprocessing.reductions import rebuild_cuda_tensor

        n = torch.cuda.device_count()
        index = next((i for i in range(n) if str(torch.cuda.get_device_properties(i).uuid) == share["gpu"]), None)
        if index is None:
            return None  # that GPU is not visible to this process
        args = list(share["args"])
        args[6] = index  # the owner's device index, as this process numbers its GPUs
        src = rebuild_cuda_tensor(*args)
        out = src.to(target or src.device, copy=True)
        del src
        global cuda_shared_reads
        cuda_shared_reads += 1
        return out
    except Exception:  # noqa: BLE001 -- the owner exited, or IPC is unavailable: the host copy
        return None


def keep_device_holds(holds: list[Any]) -> None:
    """A put future holding shared CUDA tensors was passed on: copies of it
    may be read for as long as this process lives, so the tensors stay."""
    _kept_device.extend(holds)
    holds.clear()


def tensor_from_gpu(run: bytes, blob: bytes, shape: tuple[int, ...], dtype: str, share: dict[str, Any]) -> Any:
    """A CUDA tensor put with a network offer: on this machine, from its
    owner's GPU through CUDA IPC; on another machine, in a call granted a GPU,
    sent GPU to GPU by its owner (_gpunet); else from its host copy, the blob
    stored with the put, which only then is fetched."""
    import numpy as np
    import torch

    from . import _gpunet
    from ._devices import device, devices

    granted = device()
    target = granted if granted.startswith("cuda") else ("cuda" if not devices() else None)
    if target is not None and torch.cuda.is_available():
        if target == "cuda":
            target = f"cuda:{torch.cuda.current_device()}"
        direct = _from_cuda_share(share, target)
        if direct is None and share.get("host") != _boot_id() and share.get("net"):
            direct = _gpunet.fetch(share["net"], target)
        if direct is not None:
            global cuda_net_reads
            cuda_net_reads += share.get("host") != _boot_id()
            return direct
    blob_dir = getattr(_serde._reading, "blob_dir", None)
    if blob_dir is None:  # an inline value decoded without its run's directory
        from ._runtime import Runtime

        blob_dir = _serde.run_dir(Runtime.get().object_dir, run)
    host = np.frombuffer(_serde._blob_view(blob_dir, blob), dtype=np.dtype(dtype)).reshape(shape)
    return tensor_on_device(host, "cuda")


# CUDA tensors read GPU to GPU from another machine by this process (tests).
cuda_net_reads = 0


def tensor_on_device(payload: Any, kind: str, share: dict[str, Any] | None = None) -> Any:
    """A tensor sent from a `kind` device: on this call's granted device of
    that kind; in the driver, on its current one if it has one; else on the
    CPU, with a warning (CH112) instead of torch's load failure. A CUDA tensor
    put on this machine is read from its owner's GPU memory when this process
    can see that GPU (`share`), else from the host copy."""
    import torch

    from ._devices import device, devices

    granted = device()
    if share is not None and kind == "cuda" and torch.cuda.is_available():
        direct = _from_cuda_share(share, granted if granted.startswith("cuda") else None)
        if direct is not None:
            return direct
    t = payload if isinstance(payload, torch.Tensor) else torch.from_numpy(payload)
    if granted.split(":")[0] == kind:
        return t.to(granted)
    if not devices() and kind == "cuda" and torch.cuda.is_available():
        return t.to(kind)
    if kind not in _device_warned:
        _device_warned.add(kind)
        warnings.warn(
            f"CH112: a tensor sent from a {kind} device arrived where there is none, so it is on the CPU. "
            f"Ask the call for gpus=1 to receive it on a GPU, or send t.cpu() to make the copy explicit.",
            stacklevel=2,
        )
    return t
