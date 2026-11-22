"""Your project's code, shipped to remote peers.

A driver on a laptop imports modules from its project directory (``import
mylib``). Code shipped by value (functions defined in the script) reaches the
workers on its own, but such modules do not: a remote peer has no copy. So
a remote ``ch.init`` packs the project's source directories (the ``sys.path``
entries that are not the interpreter's, a virtualenv's, or Chimaera's own)
into one archive named by its content, uploads it once, and every worker puts
it on ``sys.path`` before loading the run's code. Peers cache archives by
digest, so an unchanged project uploads and unpacks once.

When the project has a ``uv.lock``, its locked packages come along too: each
peer installs them once (``uv sync --frozen``, with the peer's own Python) into
an environment named by the lock, and workers put that environment's
packages ahead of the peer's. ``uv`` must be on the peers' PATH.

Skipped: ``.git``, ``__pycache__``, virtualenvs, ``node_modules``, build output
and other caches. The archive may hold at most ``CHIMAERA_PROJECT_MAX_BYTES``
(default 100 MiB) of files; past that, ``ch.init`` fails and names the largest.
"""

from __future__ import annotations

import io
import os
import shutil
import site
import sys
import sysconfig
import tarfile
import tempfile
import threading
from typing import Any

from ._native import blake3_digest

ENV = "CHIMAERA_PROJECT"
SKIP_DIRS = {
    ".git", ".hg", ".svn", "__pycache__", ".venv", "venv", "env", ".tox", ".nox", "node_modules",
    ".mypy_cache", ".pytest_cache", ".ruff_cache", "build", "dist", "target", ".ipynb_checkpoints",
}  # fmt: skip


def _max_bytes() -> int:
    return int(os.environ.get("CHIMAERA_PROJECT_MAX_BYTES", 100 << 20))


def roots(paths: list[str]) -> list[str]:
    """The project directories among `paths` (a ``sys.path``)."""
    import chimaera

    system = {os.path.realpath(p) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix)}
    system |= {os.path.realpath(p) for p in sysconfig.get_paths().values()}
    system |= {os.path.realpath(p) for p in [*site.getsitepackages(), site.getusersitepackages()]}
    own = os.path.realpath(os.path.dirname(os.path.dirname(chimaera.__file__)))
    out: list[str] = []
    for p in paths:
        real = os.path.realpath(p or os.getcwd())
        if not os.path.isdir(real) or real in out or real == own:
            continue
        if any(real == s or real.startswith(s + os.sep) for s in system):
            continue
        if "site-packages" in real.split(os.sep) or "dist-packages" in real.split(os.sep):
            continue
        if real == os.path.realpath(os.path.expanduser("~")) or real == os.sep:
            continue  # a home or root directory is not a project
        out.append(real)
    return out


def _files(root: str) -> list[tuple[str, str, int]]:
    """(path, name in the archive, size) for every file under `root`, sorted."""
    out = []
    for d, dirs, names in os.walk(root):
        dirs[:] = sorted(x for x in dirs if x not in SKIP_DIRS and not x.endswith(".egg-info"))
        for n in sorted(names):
            if n.endswith((".pyc", ".pyo")):
                continue
            path = os.path.join(d, n)
            if os.path.isfile(path) and not os.path.islink(path):
                out.append((path, os.path.relpath(path, root), os.path.getsize(path)))
    return out


def pack(paths: list[str]) -> tuple[bytes, bytes] | None:
    """(digest, archive) of the project directories in `paths`, or None if
    there are none. The archive is byte-for-byte the same for the same files."""
    dirs = roots(paths)
    if not dirs:
        return None
    files = [(path, f"r{i}/{name}", size) for i, root in enumerate(dirs) for path, name, size in _files(root)]
    total = sum(size for *_, size in files)
    if total > _max_bytes():
        largest = sorted(files, key=lambda f: -f[2])[:5]
        listed = ", ".join(f"{p} ({s >> 20} MiB)" for p, _, s in largest)
        raise ValueError(
            f"your project's directories hold {total >> 20} MiB of files, over the {_max_bytes() >> 20} MiB a remote "
            f"run ships (CHIMAERA_PROJECT_MAX_BYTES); the largest: {listed}. Move data out of {dirs[0]} "
            "(read it with ch.data.partitions or ch.put instead)"
        )
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for path, name, size in files:
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = size, 0o644, 0
            with open(path, "rb") as f:
                tar.addfile(info, f)
    data = buf.getvalue()
    return blake3_digest(data)[:16], data


_installed: dict[bytes, list[str]] = {}
_lock = threading.Lock()


def ensure(object_dir: str, run: bytes, cache: str) -> None:
    """In a worker: put the run's project (``CHIMAERA_PROJECT``) on sys.path,
    unpacking it into `cache` first if this peer has not yet."""
    value = os.environ.get(ENV)
    if not value:
        return
    digest = bytes.fromhex(value)
    with _lock:
        if digest in _installed:
            return
        target = os.path.join(cache, digest.hex())
        if not os.path.isdir(target):
            from . import _serde

            path = _serde.object_path(object_dir, run, digest)
            if not os.path.exists(path) and _serde.localize_hook is not None:
                _serde.localize_hook(run, digest)  # uploaded to another peer
            os.makedirs(cache, exist_ok=True)
            tmp = tempfile.mkdtemp(prefix=".unpack-", dir=cache)
            with tarfile.open(path) as tar:
                _extract(tar, tmp)
            try:
                os.rename(tmp, target)
            except OSError:  # another worker unpacked it first
                shutil.rmtree(tmp, ignore_errors=True)
        os.utime(target)  # used now: the peer's sweep keeps it (chimaera-peer gc.rs)
        dirs = sorted(os.path.join(target, d) for d in os.listdir(target))
        sys.path[:0] = dirs
        locked = [d for d in dirs if os.path.exists(os.path.join(d, "uv.lock"))]
        if locked:
            _use_locked_env(locked[0], os.path.join(os.path.dirname(cache), "envs"))
        _installed[digest] = dirs


_TAR_FILTERS = hasattr(tarfile, "data_filter")  # Python 3.11.4+, 3.12+


def _extract(tar: tarfile.TarFile, dest: str) -> None:
    """Unpack an archive from `pack`: plain files under relative names only.
    Anything else (links, devices, absolute or `..` names) is refused, so an
    archive can write nowhere but `dest`, on every Python (``filter="data"``
    exists only from 3.11.4)."""
    root = os.path.realpath(dest)
    for m in tar.getmembers():
        target = os.path.realpath(os.path.join(root, m.name))
        if not m.isfile() or os.path.isabs(m.name) or not target.startswith(root + os.sep):
            raise ValueError(f"a project archive may hold only plain files inside it; refusing {m.name!r}")
    if _TAR_FILTERS:
        tar.extractall(dest, filter="data")
    else:
        tar.extractall(dest)  # members checked above


def _use_locked_env(project: str, envs: str) -> None:
    """Install `project`'s uv.lock for this interpreter (once per peer) and put
    its packages first on sys.path."""
    import fcntl
    import subprocess

    with open(os.path.join(project, "uv.lock"), "rb") as f:
        lock = f.read()
    key = blake3_digest(lock + sys.version.encode() + sysconfig.get_platform().encode())[:16].hex()
    env = os.path.join(envs, key)
    os.makedirs(envs, exist_ok=True)
    with open(env + ".lock", "w") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)  # one install per peer; the others wait for it
        if not os.path.exists(os.path.join(env, ".chimaera-ready")):
            uv = shutil.which("uv")
            if uv is None:
                raise RuntimeError("the project has a uv.lock, but uv is not on this peer's PATH: install uv on the peers")
            limit = float(os.environ.get("CHIMAERA_ENV_BUILD_TIMEOUT", "900"))
            try:
                done = subprocess.run(
                    [uv, "sync", "--frozen", "--no-install-project", "--python", sys.executable],
                    cwd=project,
                    env={**os.environ, "UV_PROJECT_ENVIRONMENT": env},
                    capture_output=True,
                    text=True,
                    timeout=limit,
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError(
                    f"installing the project's uv.lock on this peer took over {limit:g} s; "
                    "raise CHIMAERA_ENV_BUILD_TIMEOUT on the peers if it needs longer"
                ) from None
            if done.returncode != 0:
                raise RuntimeError(f"installing the project's uv.lock on this peer failed:\n{done.stderr[-2000:]}")
            open(os.path.join(env, ".chimaera-ready"), "w").close()
    os.utime(env)  # used now: the peer's sweep keeps it (chimaera-peer gc.rs)
    packages = os.path.join(env, "lib", f"python{sys.version_info.major}.{sys.version_info.minor}", "site-packages")
    sys.path.insert(0, packages)
    site.addsitedir(packages)  # .pth files (editable installs, namespace packages)


def ship(sink: Any, run: bytes, packed: tuple[bytes, bytes] | None) -> None:
    """Upload the archive for `run` (a remote blob sink skips what the peer has)."""
    if packed is not None:
        sink.put(run, packed[0], packed[1])
