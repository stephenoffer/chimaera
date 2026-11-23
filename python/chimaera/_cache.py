"""``@ch.remote(cache=...)``: reuse a function's earlier result for the same
code, arguments and environment instead of running it again.

A call's cache key is a digest of:

- the function's code, as it ships (by value from ``__main__`` and notebooks,
  by reference from modules);
- the environment, computed once per process: the interpreter, the installed
  distributions and the project's Python sources, so a changed helper module
  or package version is a miss;
- ``device=`` and ``gpus=``, which may change a result;
- the arguments, bound to the function's parameters and encoded canonically.
  Only plain data has a canonical encoding: None, bool, int, float, str,
  bytes, lists, tuples and dicts with str keys. A future of another cached
  call stands for that call's key, so a pipeline of cached calls hits from end
  to end, and a small ``ch.put`` value stands for its bytes. Pickles of sets
  and of objects in general vary between processes, so any other argument
  runs the call uncached and says why once.

A hit is a finished future and nothing runs. A miss runs the call, and the
worker stores a successful result of up to 1 MiB pickled in a replicated store
(scoped to the tenant) for the cache's time to live. Errors are not stored.
Files and services the function reads are not part of the key: pass what
identifies them (a file's modification time, a dataset version) as arguments.
"""

from __future__ import annotations

import asyncio
import functools
import importlib.machinery
import importlib.metadata
import inspect
import io
import os
import pickle
import struct
import sys
import warnings
from typing import Any

import cloudpickle

from . import _project
from ._errors import OptionsError
from ._future import Future, _Part, put
from ._native import blake3_digest
from ._options import Effective, signature_of
from ._remote import RemoteFunction, _fn_name, submit_call
from ._runtime import CALL_FUNCTION, Runtime
from ._store import MAX_VALUE, Store

NAMESPACES = 16  # entries are spread over this many store namespaces, each its own group
MAX_DEPTH = 64
_MISSING = object()
_warned: set[tuple[str, str]] = set()


class Cached(Future[Any]):
    """The future of a call made with ``cache=``. ``cache`` is ``"hit"``
    (nothing ran), ``"miss"`` (the call ran and stores its result) or
    ``"skipped: <why>"`` (the call ran uncached)."""

    __slots__ = ("cache", "cache_key")

    def __init__(self, rt: Any, key: bytes, site: str | None = None) -> None:
        super().__init__(rt, key, site)
        self.cache = "miss"
        self.cache_key: bytes | None = None

    def explain(self) -> dict[str, Any]:
        if self.cache == "hit":
            out: dict[str, Any] = {"state": "Done", "pending_reason": "", "attempts": []}
        else:
            out = super().explain()
        out["cache"] = self.cache
        if self.cache_key is not None:
            out["cache_key"] = self.cache_key.hex()
        return out


class _Uncacheable(Exception):
    pass


class _Runner:
    """Runs a cached function in its worker, then stores a successful result."""

    def __init__(self, fn: Any) -> None:
        self.fn = fn
        self.__module__ = getattr(fn, "__module__", __name__)
        self.__qualname__ = f"{getattr(fn, '__qualname__', type(fn).__name__)}[cached]"

    def __call__(self, key: str, ttl: float, *args: Any, **kwargs: Any) -> Any:
        result = self.fn(*args, **kwargs)
        if inspect.isawaitable(result) and not isinstance(result, Future):
            return self._later(key, ttl, result)
        _save(self.fn, key, ttl, result)
        return result

    async def _later(self, key: str, ttl: float, awaitable: Any) -> Any:
        result = await awaitable
        await asyncio.to_thread(_save, self.fn, key, ttl, result)
        return result


def _prepare(fn: Any) -> tuple[_Runner, bytes]:
    """The function's runner, and the digest of its code as it ships but
    without its source file's path, which changes with every notebook kernel
    and every copy of a script."""
    return _Runner(fn), blake3_digest(cloudpickle.dumps(fn, protocol=5))


_prepared = functools.lru_cache(maxsize=256)(_prepare)  # once per function: one upload, one pickle


def _for(fn: Any) -> tuple[_Runner, bytes]:
    try:
        hash(fn)
    except TypeError:
        return _prepare(fn)
    return _prepared(fn)


def submit(
    rt: Runtime,
    f: RemoteFunction[Any, Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    eff: Effective,
    *,
    kind: int = CALL_FUNCTION,
    item_index: int = 0,
    site: str | None = None,
) -> Future[Any]:
    """A cached call: a finished future on a hit, else the call itself."""
    fn = f._fn
    where = eff.sources.get("cache", "cache=")
    if f._streams:
        raise OptionsError(f"cache= stores a return value, and a generator's items are not cached ({where})")
    if eff.get("within") is not None:
        raise OptionsError(f"cache= does not combine with within= ({where})")
    if eff.get("effects") != "none":
        raise OptionsError(
            f"cache= skips the call on a hit, so its function must declare effects='none', "
            f"not {eff.get('effects')!r} ({where})"
        )
    if not eff.get("deterministic"):
        raise OptionsError(f"cache= reuses a result, and this function is declared deterministic=False ({where})")
    name = _fn_name(fn, "")
    try:
        key = _key(fn, args, kwargs, eff)
    except _Uncacheable as e:
        _warn_once(
            name,
            "args",
            f"{name}: not cached, because {e}. A cache key is built from plain data (None, bool, int, float, "
            "str, bytes, and lists, tuples or dicts with str keys of them) and futures of cached calls.",
        )
        fut = submit_call(rt, fn, args, kwargs, kind=kind, eff=eff, site=site, item_index=item_index, future_cls=Cached)
        fut.cache = f"skipped: {e}"
        return fut
    try:
        value = _store(key).get(key.hex(), _MISSING)
    except Exception as e:  # the cache is unreachable, or an entry no longer loads: run the call
        _warn_once(name, "lookup", f"{name}: the cache could not be read, so the call runs: {type(e).__name__}: {e}")
        value = _MISSING
    if value is not _MISSING:
        return _hit(rt, value, key, eff, site)
    fut = submit_call(
        rt,
        _for(fn)[0],
        (key.hex(), eff.get("cache"), *args),
        kwargs,
        kind=kind,
        eff=eff,
        site=site,
        item_index=item_index,
        policy_extra={"fn_name": name, "profile_digest": rt.code_digest(fn)},
        future_cls=Cached,
    )
    fut.cache_key = key
    return fut


def _hit(rt: Runtime, value: Any, key: bytes, eff: Effective, site: str | None) -> Cached:
    """A finished future for a stored result: a ch.put value, so it can be
    passed to calls like any other."""
    p = put(value)
    f = Cached(rt, p.key, site)
    for slot in ("_outcome", "_put", "_escaped", "_owned", "_device_holds"):
        setattr(f, slot, getattr(p, slot))
    p._escaped = True  # its files are f's now
    f._returns = eff.get("returns")
    f.cache, f.cache_key = "hit", key
    return f


def _store(key: bytes) -> Store:
    return Store(f"chimaera.cache.{key[0] % NAMESPACES:x}")


def _key(fn: Any, args: tuple[Any, ...], kwargs: dict[str, Any], eff: Effective) -> bytes:
    try:
        items = list(signature_of(fn).bind(*args, **kwargs).arguments.items())
    except (TypeError, ValueError):  # no readable signature: by position, then by name
        items = [(f"#{i}", a) for i, a in enumerate(args)] + sorted(kwargs.items())
    out = [b"chimaera.cache.v1", _for(fn)[1], _env()]
    _encode((eff.get("device") or "", float(eff.get("gpus") or 0)), out, 0)
    for name, value in items:
        out.append(name.encode())
        try:
            _encode(value, out, 0)
        except _Uncacheable as e:
            raise _Uncacheable(f"argument {name!r} holds {e}") from None
    return blake3_digest(b"".join(out))


def _n(n: int) -> bytes:
    return n.to_bytes(8, "big")


def _encode(x: Any, out: list[bytes], depth: int) -> None:
    """Append `x`'s canonical encoding: equal plain data encodes equally in
    every process (a dict's keys are sorted; types are kept apart)."""
    if depth > MAX_DEPTH:
        raise _Uncacheable(f"data nested more than {MAX_DEPTH} deep")
    t = type(x)
    if x is None:
        out.append(b"N")
    elif t is bool:
        out.append(b"T" if x else b"F")
    elif t is int:
        b = x.to_bytes((x.bit_length() + 8) // 8, "big", signed=True)
        out += (b"i", _n(len(b)), b)
    elif t is float:
        out += (b"f", struct.pack(">d", x))
    elif t is str:
        b = x.encode("utf-8", "surrogatepass")
        out += (b"s", _n(len(b)), b)
    elif t is bytes:
        out += (b"b", _n(len(x)), x)
    elif t is tuple or t is list:
        out += (b"(" if t is tuple else b"[", _n(len(x)))
        for e in x:
            _encode(e, out, depth + 1)
    elif t is dict:
        if any(type(k) is not str for k in x):
            raise _Uncacheable("a dict whose keys are not all str")
        out += (b"{", _n(len(x)))
        for k in sorted(x):
            _encode(k, out, depth + 1)
            _encode(x[k], out, depth + 1)
    elif isinstance(x, _Part) and isinstance(x._parent, Cached) and x._parent.cache_key is not None:
        out += (b"P", x._parent.cache_key, _n(x._index), _n(x._count))
    elif isinstance(x, Cached) and x.cache_key is not None:
        out += (b"K", x.cache_key)  # a cached call's value is fixed by its key
    elif isinstance(x, Future) and x._put is not None and x._put[2] is not None:
        out += (b"V", blake3_digest(x._put[2]))  # a small ch.put value, by its bytes
    elif isinstance(x, Future):
        raise _Uncacheable("a future of a call that is not cached, whose value is unknown until it runs")
    else:
        raise _Uncacheable(f"a {t.__qualname__}, which has no canonical encoding")


@functools.cache
def _env() -> bytes:
    """The interpreter, the installed distributions and the project's Python
    sources, read once per process. The ``__main__`` script and notebooks are
    left out: their functions ship by value, so their code is in the call's
    own digest, and editing another cell must not empty the cache."""
    parts = [sys.version.encode(), sys.platform.encode()]
    dists = sorted(f"{d.metadata['Name']}=={d.version}" for d in importlib.metadata.distributions())
    parts.append("\n".join(dists).encode())
    main = getattr(sys.modules.get("__main__"), "__file__", None)
    main = os.path.realpath(main) if main else None
    sources = (".py", *importlib.machinery.EXTENSION_SUFFIXES)
    for i, root in enumerate(_project.roots(sys.path)):
        for path, name, _size in _project._files(root):
            if not name.endswith(sources) or os.path.realpath(path) == main:
                continue
            try:
                with open(path, "rb") as f:
                    parts.append(f"{i}/{name}".encode() + b"\0" + blake3_digest(f.read()))
            except OSError:
                continue
    return blake3_digest(b"\n".join(parts))


class _NoFutures(pickle.Pickler):
    def persistent_id(self, obj: Any) -> None:
        if isinstance(obj, Future):
            raise _Uncacheable("it holds a future, which lasts only as long as its run")
        return None


def _save(fn: Any, key: str, ttl: float, result: Any) -> None:
    """Store a call's result; a result that cannot be stored is reported once
    and the call's own outcome stands."""
    name = _fn_name(fn, "")
    try:
        buf = io.BytesIO()
        _NoFutures(buf, protocol=5).dump(result)
        if buf.tell() > MAX_VALUE:
            raise _Uncacheable(f"it is {buf.tell()} bytes pickled, over the cache's 1 MiB limit")
        _store(bytes.fromhex(key)).put(key, result, ttl=ttl)
    except _Uncacheable as e:
        _warn_once(name, "result", f"{name}: its result was not cached, because {e}")
    except (pickle.PicklingError, TypeError, AttributeError) as e:
        _warn_once(name, "result", f"{name}: its result was not cached, because it cannot be pickled by value: {e}")
    except Exception as e:  # the store refused or is unreachable: the result still stands
        _warn_once(name, "result", f"{name}: its result was not cached: {type(e).__name__}: {e}")


def _warn_once(name: str, kind: str, message: str) -> None:
    """Say why once per function and kind ("args", "result"), not per call."""
    if (name, kind) in _warned:
        return
    _warned.add((name, kind))
    warnings.warn(message, stacklevel=3)
