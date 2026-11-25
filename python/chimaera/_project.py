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

Inside a git work tree, only the files git would track are shipped (tracked
ones, and untracked ones your ``.gitignore`` rules let through), so data,
checkpoints and ``.env`` files you ignore stay home; a directory git ignores
as a whole ships nothing (CH126 says so when the program imported modules from
it). ``CHIMAERA_PROJECT_INCLUDE`` names files or directories to ship even so.
Always skipped: ``.git``, ``__pycache__``, virtualenvs, ``node_modules``,
build output and other caches.
The archive may hold at most ``CHIMAERA_PROJECT_MAX_BYTES``
(default 100 MiB) of files; past that, ``ch.init`` fails and names the largest.
"""

from __future__ import annotations

import io
import logging
import os
import re
import shutil
import site
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import threading
import warnings
from typing import Any

from ._native import blake3_digest

ENV = "CHIMAERA_PROJECT"
_log = logging.getLogger("chimaera.project")
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
        if os.path.isfile(os.path.join(real, "chimaera", "_runtime.py")):
            continue  # another copy of Chimaera (a source checkout): it would shadow the peers' own
        out.append(real)
    return out


def _skipped_dir(name: str) -> bool:
    return name in SKIP_DIRS or name.endswith(".egg-info")


def _walk(root: str, top: str) -> list[str]:
    """Every file under `top`, as paths relative to `root`."""
    out = []
    for d, dirs, names in os.walk(top):
        dirs[:] = [x for x in dirs if not _skipped_dir(x)]
        out += [os.path.relpath(os.path.join(d, n), root) for n in names]
    return out


def _in_work_tree(root: str) -> bool:
    """Whether `root` or a directory above it holds a ``.git`` (a repository,
    or a worktree's or submodule's ``.git`` file), or ``GIT_DIR`` names one."""
    if os.environ.get("GIT_DIR"):
        return True
    d = os.path.realpath(root)
    while True:
        if os.path.lexists(os.path.join(d, ".git")):
            return True
        up = os.path.dirname(d)
        if up == d:
            return False
        d = up


def _under(path: str, root: str) -> bool:
    return path == root or path.startswith(root + os.sep)


def _git_listed(root: str) -> list[str] | None:
    """The files under `root` that git tracks or would track (its ignore rules,
    nested and global, applied by git itself), relative to `root`: empty when
    git ignores all of `root`. None when `root` is not in a git work tree, or
    ``CHIMAERA_PROJECT_GITIGNORE=0`` asks for every file. Submodules come back
    as directories, walked like any other. In a work tree that git cannot
    list (not installed, timed out, refused), it raises: shipping every file
    instead would send what the ignore rules keep home."""
    if os.environ.get("CHIMAERA_PROJECT_GITIGNORE", "1").strip().lower() in ("0", "false", "no", "off"):
        return None
    if not _in_work_tree(root):
        return None
    try:
        p = subprocess.run(
            ["git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            capture_output=True,
            timeout=30,
            env={**os.environ, "LC_ALL": "C"},  # its messages in English, to tell "not a repository" apart
        )
        failed = "" if p.returncode == 0 else os.fsdecode(p.stderr).strip()[-500:] or f"exit status {p.returncode}"
    except (OSError, subprocess.TimeoutExpired) as e:
        failed = str(e)
    if "not a git repository" in failed:
        return None  # a .git that git does not take for one (a copied or broken folder)
    if failed:
        raise ValueError(
            f"{root} is in a git work tree, but git could not list the files its ignore rules let through "
            f"({failed}), so the project was not shipped. Fix git there, or set CHIMAERA_PROJECT_GITIGNORE=0 "
            "to ship every file under it"
        )
    out = []
    for name in os.fsdecode(p.stdout).split("\0"):
        if not name:
            continue
        name = name.rstrip("/")
        out += _walk(root, os.path.join(root, name)) if os.path.isdir(os.path.join(root, name)) else [name]
    return out


def _included(root: str) -> list[str]:
    """The files under `root` that ``CHIMAERA_PROJECT_INCLUDE`` names (files or
    directories, separated by ``os.pathsep``, relative to the working
    directory), relative to `root`. They ship even when git ignores them."""
    real = os.path.realpath(root)
    out = []
    for p in os.environ.get("CHIMAERA_PROJECT_INCLUDE", "").split(os.pathsep):
        path = os.path.realpath(p) if p else ""
        if path and _under(path, real):
            out += _walk(real, path) if os.path.isdir(path) else [os.path.relpath(path, real)]
    return out


_noted: set[str] = set()  # wholly ignored roots already reported, once per process


def _note_ignored(root: str) -> None:
    """Say once that git ignores all of `root`, so none of it ships: as a
    warning (CH126) when this program imported modules from it, which the
    workers then will not find."""
    if root in _noted:
        return
    _noted.add(root)
    real = os.path.realpath(root)
    files = {name: getattr(m, "__file__", None) for name, m in list(sys.modules.items()) if name != "__main__"}
    imported = sorted(n for n, f in files.items() if isinstance(f, str) and _under(os.path.realpath(f), real))
    if not imported:
        _log.info("project: git ignores all of %s, so none of its files ship (CHIMAERA_PROJECT_INCLUDE)", root)
        return
    warnings.warn(
        f"CH126: git ignores all of {root}, so none of its files ship to remote peers, and their workers will not "
        f"find {', '.join(imported[:5])}{' and others' if len(imported) > 5 else ''}, which this program imported "
        f"from it. Name what they need in CHIMAERA_PROJECT_INCLUDE (files or directories, separated by "
        f"{os.pathsep!r}), or stop ignoring them in .gitignore",
        stacklevel=2,
    )


def _files(root: str, shipping: bool = True) -> list[tuple[str, str, int]]:
    """(path, name in the archive, size) for every file under `root` to ship,
    sorted. Not `shipping` (the files a cache key digests), a root git ignores
    as a whole or cannot list counts every file under it, and nothing is said:
    its modules still decide what the program computes."""
    try:
        names = _git_listed(root)
    except ValueError:
        if shipping:
            raise
        names = None
    if names is None or not names and not shipping:
        names = _walk(root, root)
    elif not names:
        _note_ignored(root)
    named = set(_included(root))  # asked for by name: shipped even inside a skipped directory
    out = []
    for name in sorted(set(names) | named):
        parts = name.split(os.sep)
        if name not in named and any(_skipped_dir(x) for x in parts[:-1]) or parts[-1].endswith((".pyc", ".pyo")):
            continue
        path = os.path.join(root, name)
        if os.path.isfile(path) and not os.path.islink(path):
            out.append((path, name, os.path.getsize(path)))
    return out


def _check_included(dirs: list[str]) -> None:
    """Warn about each ``CHIMAERA_PROJECT_INCLUDE`` path that is missing or in
    none of the project's directories `dirs`, so ships nothing."""
    for p in os.environ.get("CHIMAERA_PROJECT_INCLUDE", "").split(os.pathsep):
        if not p:
            continue
        path = os.path.realpath(p)
        if not os.path.exists(path):
            warnings.warn(f"CHIMAERA_PROJECT_INCLUDE names {p}, which does not exist", stacklevel=3)
        elif not any(_under(path, d) for d in dirs):
            warnings.warn(
                f"CHIMAERA_PROJECT_INCLUDE names {p}, which is in none of the project's directories "
                f"({', '.join(dirs)}), so it does not ship",
                stacklevel=3,
            )


def pack(paths: list[str]) -> tuple[bytes, bytes] | None:
    """(digest, archive) of the project directories in `paths`, or None if
    there are none. The archive is byte-for-byte the same for the same files."""
    dirs = roots(paths)
    if not dirs:
        return None
    _check_included(dirs)
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
        dirs = [os.path.join(target, d) for d in sorted(os.listdir(target), key=_root_index)]
        sys.path[:0] = dirs
        locked = [d for d in dirs if os.path.exists(os.path.join(d, "uv.lock"))]
        if locked:
            _use_locked_env(locked[0], os.path.join(os.path.dirname(cache), "envs"))
        _installed[digest] = dirs


def _root_index(name: str) -> tuple[int, str]:
    """Where an unpacked root (``r<i>``, numbered in the driver's sys.path
    order) goes on a worker's sys.path: by its number, so r2 stays ahead of
    r10 and a module in both is imported from the one the driver imports."""
    return (int(name[1:]), "") if re.fullmatch(r"r\d+", name) else (sys.maxsize, name)


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


# This machine's GPU driver versions (NVIDIA, ROCm), for locks that hold GPU packages.
ACCELERATOR_FILES = ("/proc/driver/nvidia/version", "/opt/rocm/.info/version")


def _env_key(project: str, lock: bytes, uv_version: str) -> str:
    """The name of the environment `project`'s uv.lock (`lock`) is installed
    into. Besides the lock it covers everything else that decides what the
    install holds or whether it loads, so an environment built for one
    interpreter or machine never serves another: the project's pyproject.toml
    (its dependency groups and [tool.uv] settings choose what `uv sync
    --frozen` installs), uv's version, this interpreter's version and ABI (a
    debug or free-threaded build), the platform and libc, and, when the lock
    holds GPU packages, this machine's GPU driver versions."""
    try:
        with open(os.path.join(project, "pyproject.toml"), "rb") as f:
            pyproject = f.read()
    except OSError:
        pyproject = b""
    try:
        libc = os.confstr("CS_GNU_LIBC_VERSION") or ""  # glibc's version; SOABI tells glibc from musl
    except (ValueError, OSError):
        libc = ""
    drivers = b""
    if re.search(rb"nvidia|cuda|rocm", lock):
        for path in ACCELERATOR_FILES:
            try:
                with open(path, "rb") as f:
                    drivers += f.readline()
            except OSError:
                pass
    abi = "|".join(
        [sys.version, sys.abiflags, str(sysconfig.get_config_var("SOABI")), sysconfig.get_platform(), libc, uv_version]
    )
    return blake3_digest(b"\0".join([lock, pyproject, drivers, abi.encode()]))[:16].hex()


def _use_locked_env(project: str, envs: str) -> None:
    """Install `project`'s uv.lock for this interpreter (once per peer) and put
    its packages first on sys.path."""
    import fcntl
    import subprocess

    with open(os.path.join(project, "uv.lock"), "rb") as f:
        lock = f.read()
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("the project has a uv.lock, but uv is not on this peer's PATH: install uv on the peers")
    uv_version = subprocess.run([uv, "--version"], capture_output=True, text=True, timeout=60).stdout.strip()
    env = os.path.join(envs, _env_key(project, lock, uv_version))
    os.makedirs(envs, exist_ok=True)
    with open(env + ".lock", "w") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)  # one install per peer; the others wait for it
        if not os.path.exists(os.path.join(env, ".chimaera-ready")):
            limit = float(os.environ.get("CHIMAERA_ENV_BUILD_TIMEOUT", "900"))
            try:
                # Never a chimaera from the lock: the run uses the peer's own
                # package and peer binary, and a second copy first on sys.path
                # would stand in for it.
                done = subprocess.run(
                    [uv, "sync", "--frozen", "--no-install-project", "--no-install-package", "chimaera"]
                    + ["--python", sys.executable],
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
