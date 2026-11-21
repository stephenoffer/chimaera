"""The same-host object plane, Python side.

On Linux, with the object directory in memory (tmpfs), NumPy arrays of at
least ``ARENA_MIN`` bytes are allocated as files in that directory (see
``objects.rs``). Storing one, by ``ch.put`` or as a call's return value, then
hard-links its file under the stored object's name instead of copying the
bytes: the cost no longer grows with the array's size. The owner keeps a
copy-on-write view, so its later writes never change what was stored, and
readers in other processes map the same pages. Arrays come back writable,
also copy-on-write.

Everything else (other buffer types, or arrays allocated before the arena was
installed) is copied into the object file by the native writer, which uses
several threads for large files. ``CHIMAERA_ARENA=0`` turns the arena off.
"""

from __future__ import annotations

import atexit
import importlib.abc
import os
import sys
import threading
from typing import Any

from . import _native

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


def enable(object_dir: str) -> None:
    """Route large NumPy allocations of this process to the arena in
    `object_dir`, now if NumPy is loaded, else as soon as it is imported."""
    if sys.platform != "linux" or os.environ.get("CHIMAERA_ARENA", "1") == "0":
        return
    with _lock:
        if _state["dir"] is not None:
            return
        if not _in_memory(object_dir):
            return
        _state["dir"] = os.path.join(object_dir, ".arena", str(os.getpid()))
    if "numpy" in sys.modules:
        _install()
    else:
        sys.meta_path.insert(0, _NumpyHook())


def _install() -> None:
    with _lock:
        if _state["installed"] or _state["dir"] is None:
            return
        _state["installed"] = True
    try:
        ok = _native.arena_install(_state["dir"], ARENA_MIN)
    except Exception:  # noqa: BLE001 -- the arena is an optimization; values still copy
        ok = False
    if ok:
        atexit.register(_native.arena_shutdown)


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


def claim(view: memoryview, run_dir: str, prefix: bytes = b"") -> tuple[bytes, int] | None:
    """Store the arena allocation holding `view` as a blob of the run, by
    hard link: (blob id, offset of `view` in it), or None (not in the arena,
    or changed since stored; the caller copies it). `prefix` starts the blob id
    (results use their run's prefix so they outlive the run's values)."""
    if not _state["installed"]:
        return None
    blob = prefix + os.urandom(16 - len(prefix))
    off = _native.arena_claim(view, os.path.join(run_dir, blob.hex()))
    return None if off is None else (blob, off)


def stats() -> dict[str, int]:
    live, live_bytes, cached, claims, fallbacks = _native.arena_stats()
    return {"live": live, "live_bytes": live_bytes, "cached_bytes": cached, "claims": claims, "fallbacks": fallbacks}
