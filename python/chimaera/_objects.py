"""The same-host object plane, Python side.

On Linux, with the object directory in memory (tmpfs), NumPy arrays and
Python's own large blocks (``bytes``, ``bytearray``, ``str``, lists) of at
least ``ARENA_MIN`` bytes are allocated as files in that directory (see
``objects.rs``). Storing one, by ``ch.put`` or as a call's return value, then
hard-links its file under the stored object's name instead of copying the
bytes: the cost no longer grows with the value's size. The owner keeps a
copy-on-write view, so its later writes never change what was stored, and
readers in other processes map the same pages. Arrays come back writable,
also copy-on-write.

Everything else (other buffer types, such as torch's or Arrow's own
allocations, or values allocated before the arena was installed) is copied into the object file by the native writer, which uses
several threads for large files. ``CHIMAERA_ARENA=0`` turns the arena off.
"""

from __future__ import annotations

import atexit
import importlib.abc
import os
import sys
import threading
from typing import Any, Callable

from . import _native, _serde

# Arrays at least this large live in the arena (the same threshold at which
# buffers leave the pickle and become blobs; see _values.PROMOTE_BYTES).
ARENA_MIN = 1 << 20

_state: dict[str, Any] = {"dir": None, "installed": False}
_lock = threading.Lock()


def _in_memory(path: str) -> bool:
    """Whether `path` is on tmpfs (statfs magic), where the arena belongs: on
    disk, shared writable mappings would be written back to the device."""
    return _fs_type(path) in ("tmpfs", "ramfs")


def _fs_type(path: str) -> str:
    """The filesystem type of the mount holding `path`, from /proc/self/mounts."""
    real = os.path.realpath(path)
    best, kind = "", ""
    try:
        with open("/proc/self/mounts") as f:
            for line in f:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mnt = fields[1].replace("\\040", " ")
                if (real == mnt or real.startswith(mnt.rstrip("/") + "/")) and len(mnt) >= len(best):
                    best, kind = mnt, fields[2]
    except OSError:
        return ""
    return kind


def _plane(object_dir: str) -> dict[str, str]:
    """The peer's `.plane` file: where the page reserve and the spill
    directory are, and the memory limit for objects (see objplane.rs)."""
    try:
        with open(os.path.join(object_dir, ".plane")) as f:
            return dict(line.strip().split("=", 1) for line in f if "=" in line)
    except OSError:
        return {}


def enable(object_dir: str) -> None:
    """Configure this process's writers from the peer's `.plane` file, and
    route large NumPy allocations to the arena in `object_dir`: now if NumPy
    is loaded, else as soon as it is imported."""
    with _lock:
        if _state["dir"] is not None:
            return
        _state["dir"] = ""
    if not hasattr(_native, "objects_config"):
        return  # an extension built before the object plane: plain writes, no arena
    plane = _plane(object_dir)
    if plane:
        _native.objects_config(object_dir, plane.get("reserve"), plane.get("spill"), int(plane.get("limit", "0")))
    if sys.platform != "linux" or os.environ.get("CHIMAERA_ARENA", "1") == "0" or not _in_memory(object_dir):
        return
    with _lock:
        _state["dir"] = os.path.join(object_dir, ".arena", str(os.getpid()))
    _install()
    if "numpy" not in sys.modules:
        sys.meta_path.insert(0, _NumpyHook())


def _install() -> None:
    """Create the arena once, wrapping Python's raw allocator (large `bytes`,
    `str` and lists), and patch NumPy's allocator if NumPy is loaded; called
    again once `import numpy` finishes."""
    import tracemalloc

    with _lock:
        if not _state["dir"]:
            return
        first = not _state["installed"]
        _state["installed"] = True
    try:
        # tracemalloc.stop() restores the allocator it saved at start, which
        # would leave blocks from our wrapper to a free() that does not know
        # them: while it traces, only NumPy's allocator is routed here.
        ok = _native.arena_install(_state["dir"], ARENA_MIN, raw=not tracemalloc.is_tracing())
    except Exception:  # noqa: BLE001 -- the arena is an optimization; values still copy
        ok = False
    if not ok:
        with _lock:
            _state["installed"] = False
        return
    if first:
        atexit.register(_native.arena_shutdown)
    from . import _arrow_pool

    _arrow_pool.install_soon()  # Arrow's large buffers in the arena too


class _NumpyHook(importlib.abc.MetaPathFinder):
    """Installs the arena right after `import numpy` finishes."""

    def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
        if fullname != "numpy":
            return None
        sys.meta_path.remove(self)
        spec = None
        for finder in sys.meta_path:  # whoever would have found it without us
            find = getattr(finder, "find_spec", None)
            spec = find(fullname, path, target) if find is not None else None
            if spec is not None:
                break
        if spec is None or spec.loader is None:
            return spec
        loader = spec.loader
        original = loader.exec_module

        def exec_module(module: Any) -> None:
            original(module)
            _install()

        loader.exec_module = exec_module  # type: ignore[method-assign]
        return spec


def claim(
    view: memoryview, run_dir: str, prefix: bytes = b"", blob: bytes | None = None, keep_local: bool = False
) -> tuple[bytes, int] | None:
    """Store the arena allocation holding `view` as a blob of the run, by
    hard link: (blob id, offset of `view` in it), or None (not in the arena,
    or changed since stored; the caller copies it). `prefix` starts a random
    blob id (results use their run's prefix so they outlive the run's values);
    `blob` names it instead. `keep_local`: see mark_local."""
    if not _state["installed"]:
        return None
    if blob is None:
        blob = prefix + os.urandom(16 - len(prefix))
    path = os.path.join(run_dir, blob.hex())
    if keep_local:
        mark_local(path)
    off = _native.arena_claim(view, path)
    return None if off is None else (blob, off)


# The peer's replicas::MARKER.
LOCAL_MARKER = ".tmp-local"


def mark_local(path: str) -> None:
    """Have the peer make no background copy of the object about to appear at
    `path`, whose durability class is not the default: an empty marker beside
    it, which the peer's replicator takes away. A peer that copies nothing in
    the background (``replicas`` 0 in its info) is not asked."""
    from ._runtime import Runtime

    if not Runtime.get().info.get("replicas"):
        return
    marker = path + LOCAL_MARKER
    try:
        os.close(os.open(marker, os.O_CREAT | os.O_WRONLY, 0o600))
    except FileNotFoundError:  # the run's directory comes with its first object
        os.makedirs(os.path.dirname(path), exist_ok=True)
        os.close(os.open(marker, os.O_CREAT | os.O_WRONLY, 0o600))


def prefetch(outcome: tuple[Any, ...], object_dir: str, localize: Callable[[bytes, bytes], None]) -> None:
    """Make a finished call's stored value local without decoding it: its
    object file, then every blob its header links (``ch.wait(fetch_local=True)``)."""
    if outcome[0] != "ok":
        return
    _, inline, run, obj, _size, _oob = outcome
    blobs = _serde.external_blobs(inline) if inline is not None else linked_blobs(object_dir, run, obj, localize)
    for blob in blobs:
        if not os.path.exists(os.path.join(_serde.run_dir(object_dir, run), blob.hex())):
            localize(run, blob)


def linked_blobs(object_dir: str, run: bytes, obj: bytes, localize: Callable[[bytes, bytes], None]) -> list[bytes]:
    """The blobs a stored value links, from the header of its object file
    (made local first)."""
    path = _serde.object_path(object_dir, run, obj)
    if not os.path.exists(path):
        localize(run, obj)
    with open(path, "rb") as f:
        first = f.read(_serde.header_size(0))
        n = int.from_bytes(first[8:12], "little")  # n_buffers, after magic and flags
        return _serde.external_blobs(first + f.read(_serde.header_size(n) - len(first)))


def stats() -> dict[str, int]:
    if not hasattr(_native, "arena_stats"):
        return {"live": 0, "live_bytes": 0, "cached_bytes": 0, "claims": 0, "fallbacks": 0}
    live, live_bytes, cached, claims, fallbacks = _native.arena_stats()
    return {"live": live, "live_bytes": live_bytes, "cached_bytes": cached, "claims": claims, "fallbacks": fallbacks}
