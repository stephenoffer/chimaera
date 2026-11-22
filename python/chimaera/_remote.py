"""``@ch.remote`` for functions and classes."""

from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as _dt
import functools
import inspect
import itertools
import os
import sys
import threading
import time
import warnings
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, Protocol, TypeVar, Unpack, overload

from . import _context, _serde, _values
from ._errors import ChimaeraError, Infeasible, NotFound, OptionsError, SerializationError
from ._future import Future, Handles, ItemFailed, PartDep, Results, Stream, _Chunk, _Part
from ._native import blake3_digest, child_id, derive_id
from ._options import Effective, Layer, Options, resolve, resolve_cached, signature_of, validate
from ._batch import Batch
from ._pool import Pool

if TYPE_CHECKING:
    from ._gang import Gang, GangActor
from ._runtime import (
    CALL_ACTOR_CTOR,
    CALL_ACTOR_METHOD,
    CALL_FUNCTION,
    CALL_MAP_ITEM,
    EFFECTS_CODE,
    STREAM_WINDOW,
    Runtime,
)
from ._serde import Dep

P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")
T = TypeVar("T")

_RESTART = {"never": 0, "fresh": 1, "checkpoint": 2}


def _site(depth: int = 2) -> str:
    """file:line of the frame `depth` levels up (2 = caller of the API function)."""
    try:
        f = sys._getframe(depth)
    except ValueError:
        return "<unknown>"
    return f"{f.f_code.co_filename}:{f.f_lineno}"


def _deadline_ms(eff: Effective) -> int:
    d = eff.get("deadline")
    own = 0
    if isinstance(d, _dt.datetime):
        own = int(d.timestamp() * 1000)
    elif d is not None:
        own = int((time.time() + d) * 1000)
    ctx = _context.current_task()
    inherited = ctx.deadline_unix_ms if ctx is not None else 0
    candidates = [x for x in (own, inherited) if x]
    return min(candidates) if candidates else 0


def _fn_name(target: Any, method: str) -> str:
    """module.qualname[.method]: names a function across edits of its code."""
    if isinstance(target, bytes):  # an actor found by key, whose class is not here
        return f"actor.{method}"
    if isinstance(target, _MapChunk):  # a map's chunk is profiled as its function
        target = target.fn
    while isinstance(target, functools.partial):
        target = target.func
    name = f"{getattr(target, '__module__', '')}.{getattr(target, '__qualname__', type(target).__name__)}"
    return f"{name}.{method}" if method else name


def _exc_name(t: type[BaseException]) -> str:
    return f"{t.__module__}.{t.__qualname__}"


def build_call(
    rt: Runtime,
    target: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    kind: int,
    eff: Effective,
    item_index: int = 0,
    actor: bytes | None = None,
    handle: int = 0,
    seq: int = 0,
    method: str = "",
    extra_deps: tuple[bytes, ...] = (),
    key: bytes | None = None,
    site: str | None = None,
    durable: bool = False,
    stream: bool = False,
    policy_extra: dict[str, Any] | None = None,
    reads: list[Any] | None = None,
) -> tuple[dict[str, Any], bytes]:
    """Encode a call. Futures passed directly as arguments become dependency
    edges (the callee receives their values; nothing flows through here). A
    value stored with ch.put is read from its stored copy, not re-sent."""
    deps: list[bytes] = []
    dep_index: dict[bytes, int] = {}
    stored: dict[bytes, int] = {}

    def dep(key: bytes) -> int:
        """Index of `key` among the call's dependencies; a future passed twice is one."""
        if key not in dep_index:
            dep_index[key] = len(deps)
            deps.append(key)
        return dep_index[key]

    def sub(x: Any) -> Any:
        """A future passed to the call becomes a dependency, directly or one
        level inside a list, tuple or dict value (not keys, sets, deeper, or
        subclasses), unless the argument is wrapped in ch.handles."""
        if isinstance(x, Handles):
            return x.value
        kind, marked = type(x), (Future, Handles)
        if (kind is list or kind is tuple) and any(isinstance(e, marked) for e in x):
            return kind(sub_one(e) for e in x)
        if kind is dict and any(isinstance(v, marked) for v in x.values()):
            return {k: sub_one(v) for k, v in x.items()}
        return sub_one(x)

    def pin(put: Future[Any]) -> None:
        # A value this call reads: recorded in `reads` so it is released after
        # the call; without `reads`, or for an actor that may keep it, pinned
        # for the run.
        if reads is None or kind in (CALL_ACTOR_CTOR, CALL_ACTOR_METHOD):
            put._escaped = True
        else:
            reads.append(put)

    def sub_one(x: Any) -> Any:
        if isinstance(x, Handles):
            return x.value
        if isinstance(x, _Part):
            # One value of a call returning several: depend on the call, pick in the callee.
            if x._parent._put is not None:
                return x.result()
            return PartDep(dep(x.key), x._index, x._count)
        if isinstance(x, Future):
            if x._put is not None:
                _run, object_id, inline, size = x._put
                for path in x._owned or ():
                    # Files it links (Linux arena arrays) are inputs too, even
                    # when the handle itself is inline: placed by and fetched
                    # before the call, not on first read inside it.
                    try:
                        stored[bytes.fromhex(os.path.basename(path))] = os.stat(path).st_size
                    except (OSError, ValueError):
                        pass
                if inline is not None:
                    if x._owned:
                        pin(x)  # its arrays are blob files the call reads
                    return _values.InlineRef(inline)  # decoded in the callee, never re-encoded here
                pin(x)  # the call reads the stored file
                stored[object_id] = size
                return _values.BlobRef(object_id, size)
            return Dep(dep(x.key))
        return x

    _values.fetched.check(site, args, kwargs)
    frame = (tuple(sub(a) for a in args), {k: sub(v) for k, v in kwargs.items()})
    key = key or rt.new_key()
    holds: list[_values.ClaimHold] = []
    try:
        packed = rt.pack(key, frame, site, holds)
    except Exception as e:
        raise _unsendable(target, method, args, kwargs, site, e) from e
    for hold in holds:  # arena arrays the call reads: kept like a put's file
        if reads is None or kind in (CALL_ACTOR_CTOR, CALL_ACTOR_METHOD):
            hold.keep()
        else:
            reads.append(hold)
    # Stored values are inputs (placement, fetch) but not in `read_bytes`: they
    # exist before the call and are shared by every call that reads them, so
    # pending calls hold no more memory for them (driver room, map windows).
    for object_id, size in stored.items():
        if object_id not in dict(packed["blobs"]):
            packed["blobs"].append((object_id, size))
    ctx = _context.current_task()
    effects = eff.get("effects")
    retry = eff.get("retry_exceptions") or ()
    spec = {
        "key": key,
        "kind": kind,
        "fn_digest": target if isinstance(target, bytes) else rt.code_digest(target),
        **packed,
        "deps": deps + list(extra_deps),
        "effects": EFFECTS_CODE[effects],
        "max_attempts": int(eff.get("max_attempts")),
        "deadline_unix_ms": _deadline_ms(eff),
        "policy": {**eff.native, "fn_name": _fn_name(target, method), **(policy_extra or {})},
        "actor": actor,
        "handle": handle,
        "seq": seq,
        "method": method,
        "item_index": item_index,
        "parent": (ctx.run + ctx.task) if ctx is not None else None,
        "speculation_allowed": effects == "none" and bool(eff.get("deterministic")),
        "retry_exception_types": [_exc_name(t) for t in retry],
        "max_oom_escalations": 2,
    }
    if stream:
        spec["stream"] = True
    if ctx is not None and ctx.workflow and not durable:
        # What this step asks for: a replay asking for something else diverged.
        blobs = b"".join(object_id for object_id, _ in packed["blobs"])
        digest = spec["fn_digest"] if isinstance(spec["fn_digest"], bytes) else b""
        spec["command_fingerprint"] = blake3_digest(
            digest + b"\0" + method.encode() + b"\0" + (packed.get("args_inline") or b"") + blobs
        )
    if durable:
        # A durable actor's command: its id (default: this call) and what it
        # asks for, so a repeat is answered from the stored reply.
        command_id = eff.get("command_id")
        spec["command_id"] = command_id.encode() if command_id else b""
        blobs = b"".join(object_id for object_id, _ in packed["blobs"])
        spec["command_fingerprint"] = blake3_digest(method.encode() + b"\0" + (packed.get("args_inline") or b"") + blobs)
    return spec, key


def _unsendable(
    target: Any, method: str, args: tuple[Any, ...], kwargs: dict[str, Any], site: str | None, err: Exception
) -> SerializationError:
    """The error for a call whose arguments cannot be pickled, naming the
    argument (the pickle error alone names only a type)."""
    fn = getattr(target, method, None) if method and not isinstance(target, bytes) else target
    try:
        named = signature_of(fn).bind(*((None,) if method else ()), *args, **kwargs).arguments
    except (TypeError, ValueError):
        named = {**{f"#{i}": a for i, a in enumerate(args)}, **kwargs}
    culprit = None
    for name, value in named.items():
        try:
            _serde.dumps(value)
        except Exception:
            culprit = name
            break
    where = _fn_name(target, method) + (f" ({site})" if site else "")
    what = f"argument {culprit!r}" if culprit else "its arguments"
    return SerializationError(
        f"{where}: {what} cannot be sent to a worker: {err}. Pass what the call needs to rebuild it "
        "(a path rather than an open file, settings rather than a client) and create it inside the call."
    )


def check_call(fn: Any, args: tuple[Any, ...], kwargs: dict[str, Any], *, bound: bool = False) -> None:
    """Check a call's arguments against `fn`'s signature, naming `fn` when they
    do not fit (`bound`: a method, whose `self` is not passed)."""
    try:
        sig = signature_of(fn)
    except ValueError:  # a builtin with no readable signature: the call itself checks
        return
    try:
        sig.bind(*((None,) if bound else ()), *args, **kwargs)
    except TypeError as e:
        raise TypeError(f"{getattr(fn, '__qualname__', fn)}(): {e}") from None


def submit_call(rt: Runtime, target: Any, args: tuple[Any, ...], kwargs: dict[str, Any], **kw: Any) -> Future[Any]:
    site = kw.pop("site", None)
    future_cls = kw.pop("future_cls", Future)
    reads: list[Any] = []
    spec, key = build_call(rt, target, args, kwargs, site=site, reads=reads, **kw)
    if reads:
        rt._reads[key] = reads
    rt.wait_for_room(spec["read_bytes"], site)
    fut: Future[Any] = future_cls(rt, key, site)
    ctx = _context.current_task()
    if ctx is not None and ctx.workflow:
        fut._escaped = True  # a durable workflow's replay reads this step again: never release it
    fut._arg_bytes = spec["read_bytes"]
    fut._returns = kw["eff"].get("returns")
    rt.submit([spec], [fut])
    return fut


class RemoteFunction(Generic[P, R]):
    """A function usable locally (``fn(x)``) or remotely (``fn.remote(x)``)."""

    def __init__(self, fn: Callable[P, R], decorator: Layer, view: tuple[Layer, ...] = ()) -> None:
        self._fn = fn
        self._decorator = decorator
        self._view = view
        self._resolved: tuple[tuple[Layer, ...], Effective] | None = None
        self._streams = inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)
        functools.update_wrapper(self, fn)  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return f"<remote function {getattr(self._fn, '__qualname__', self._fn)!r}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._fn(*args, **kwargs)

    def gang(self, *args: P.args, **kwargs: P.kwargs) -> Gang[R]:
        """Run as an SPMD gang of ``.options(ranks=n)`` ranks (see ``ch.dist.Gang``)."""
        from ._gang import start

        if not self.policy().get("ranks"):
            raise TypeError("gang() needs the number of ranks: fn.options(ranks=n).gang(...)")
        check_call(self._fn, args, kwargs)
        return start(self, args, kwargs)

    def options(self, **opts: Unpack[Options]) -> RemoteFunction[P, R]:
        """A configured view; the original function is unchanged."""
        layer = validate(opts, kind="view", target="function", source=f".options at {_site(2).rsplit('/', 1)[-1]}")
        return RemoteFunction(self._fn, self._decorator, (layer, *self._view))

    def partial(self, *args: Any, **kwargs: Any) -> RemoteFunction[..., R]:
        """Bind constants once; they ship with the code, not with every call."""
        return RemoteFunction(functools.partial(self._fn, *args, **kwargs), self._decorator, self._view)  # type: ignore[arg-type]

    def policy(self) -> Effective:
        """Effective options and where each value came from."""
        return resolve_cached(self, self._view, self._decorator)

    # What `.remote()` returns, by what the function returns: an async
    # function's awaited value, a generator's items, else the value itself.
    @overload
    def remote(self: RemoteFunction[P, Coroutine[Any, Any, T]], *args: P.args, **kwargs: P.kwargs) -> Future[T]: ...
    @overload
    def remote(self: RemoteFunction[P, Iterator[T]], *args: P.args, **kwargs: P.kwargs) -> Stream[T]: ...  # type: ignore[overload-overlap]
    @overload
    def remote(self: RemoteFunction[P, AsyncIterator[T]], *args: P.args, **kwargs: P.kwargs) -> Stream[T]: ...  # type: ignore[overload-overlap]
    @overload
    def remote(self: RemoteFunction[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]: ...
    def remote(self, *args: Any, **kwargs: Any) -> Any:
        """Run remotely: a Future, or for a generator function a Stream of its items."""
        check_call(self._fn, args, kwargs)
        rt = Runtime.get()
        eff = self.policy()
        if eff.get("ranks") is not None:
            raise OptionsError(f"ranks= makes a gang: run it with .gang(...), not .remote() ({eff.sources['ranks']})")
        if eff.get("within") is not None:
            from ._reserve import call_within

            return call_within(eff.get("within"), self._fn, args, kwargs)
        if self._streams:
            fut = submit_call(rt, self._fn, args, kwargs, kind=CALL_FUNCTION, eff=eff, site=_site(), stream=True)
            return Stream(fut, int(eff.get("max_in_flight") or STREAM_WINDOW))
        return submit_call(rt, self._fn, args, kwargs, kind=CALL_FUNCTION, eff=eff, site=_site())

    def map(self, *sources: Iterable[Any], ordered: bool = True) -> Results[R]:
        """Apply to every item (several sources zip like builtin ``map``)."""
        if not sources:
            raise TypeError("map() needs at least one source")
        rt = Runtime.get()
        eff = self.policy()
        # Deep by default (the peer batches fast items from this queue); the
        # byte bound keeps large items few.
        window = eff.get("max_in_flight") or max(256, int(32 * rt.info["usable_cores"]))
        window_bytes = eff.get("max_in_flight_bytes") or (256 << 20)
        source = iter(sources[0]) if len(sources) == 1 else zip(*sources)
        single = len(sources) == 1

        def submit(i: int, item: Any) -> Future[R]:
            args = (item,) if single else tuple(item)
            return submit_call(rt, self._fn, args, {}, kind=CALL_MAP_ITEM, eff=eff, item_index=i)

        # Fast items go several to a call (what hand batching does), each
        # keeping its own value and error. Calls that may not be replayed as a
        # group, or whose errors drive retries, stay one per item.
        chunkable = (
            eff.get("effects") in ("none", "idempotent")
            and not eff.get("retry_exceptions")
            and eff.get("latency") != "interactive"
            and not inspect.iscoroutinefunction(self._fn)
            and not inspect.isgeneratorfunction(self._fn)
            and not inspect.isasyncgenfunction(self._fn)
        )
        runner = _MapChunk(self._fn, star=not single)

        # Chunks are profiled as the function itself, per item.
        inner = rt.code_digest(self._fn) if chunkable else b""

        def submit_chunk(i: int, items: list[Any]) -> Future[Any]:
            extra = {"profile_digest": inner, "items": len(items)}
            return submit_call(rt, runner, (items,), {}, kind=CALL_MAP_ITEM, eff=eff, item_index=i, policy_extra=extra)

        return Results(
            submit,
            source,
            ordered=ordered,
            window=window,
            window_bytes=window_bytes,
            chunk=submit_chunk if chunkable else None,
            parallel=int(rt.info["usable_cores"]),
            grow_window=eff.get("max_in_flight") is None,
        )


class _MapChunk:
    """Runs a map's function over a chunk of items. An item that raises keeps
    its error to itself; the chunk's other items still return their values."""

    def __init__(self, fn: Callable[..., Any], *, star: bool) -> None:
        self.fn, self.star = fn, star
        self.__module__ = getattr(fn, "__module__", __name__)
        self.__qualname__ = f"{getattr(fn, '__qualname__', type(fn).__name__)}[map]"

    def __call__(self, items: list[Any]) -> _Chunk:
        out = _Chunk()
        start = time.perf_counter()
        for item in items:
            try:
                out.append(self.fn(*item) if self.star else self.fn(item))
            except Exception as exc:  # the item's own error, reported at its position
                out.append(ItemFailed(_item_error(exc)))
        out.elapsed = time.perf_counter() - start
        return out


def _item_error(exc: BaseException) -> tuple[Any, ...]:
    import pickle
    import traceback

    import cloudpickle

    try:
        blob = pickle.dumps(exc)
    except Exception:
        try:
            blob = cloudpickle.dumps(exc)
        except Exception:
            blob = b""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__.tb_next if exc.__traceback__ else None))
    name = f"{type(exc).__module__}.{type(exc).__qualname__}"
    return ("err", "user", name, str(exc), blob, tb, None, 1, "")


class ActorClass(Generic[C]):
    """A class usable locally (``Cls(...)``) or as a remote actor (``Cls.remote(...)``)."""

    def __init__(self, cls: type[C], decorator: Layer, view: tuple[Layer, ...] = ()) -> None:
        self._cls = cls
        self._decorator = decorator
        self._view = view
        self._resolved: tuple[tuple[Layer, ...], Effective] | None = None
        functools.update_wrapper(self, cls, updated=())  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return f"<remote class {self._cls.__qualname__!r}>"

    def __call__(self, *args: Any, **kwargs: Any) -> C:
        return self._cls(*args, **kwargs)

    def options(self, **opts: Unpack[Options]) -> ActorClass[C]:
        layer = validate(opts, kind="view", target="actor", source=f".options at {_site(2).rsplit('/', 1)[-1]}")
        return ActorClass(self._cls, self._decorator, (layer, *self._view))

    def policy(self) -> Effective:
        return resolve_cached(self, self._view, self._decorator)

    def remote(self, *args: Any, **kwargs: Any) -> Actor[C]:
        """Create an actor. With ``name=...`` it is get-or-create by name."""
        check_call(self._cls, args, kwargs)
        eff = self.policy()
        for opt, use in (("replicas", ".pool(...)"), ("ranks", ".gang(...)")):
            if eff.get(opt) is not None:
                raise OptionsError(f"{opt}= makes several actors: create them with {use}, not .remote() ({eff.sources[opt]})")
        if eff.get("within") is not None:
            from ._reserve import HostedActor

            return HostedActor(eff.get("within"), self._cls, args, kwargs)  # type: ignore[return-value]
        return _create_actor(Runtime.get(), self._cls, eff, args, kwargs)

    def pool(self, *args: Any, **kwargs: Any) -> Pool[C]:
        """Independent replicas, each constructed with these arguments. With
        ``name=``, a pool that exists already is joined, and arguments are optional."""
        return Pool(self, args, kwargs, self.policy())

    def gang(self, *args: Any, **kwargs: Any) -> GangActor:
        """``ranks=n`` instances built together as an SPMD gang (see ``ch.dist.GangActor``)."""
        from ._gang import GangActor

        if not self.policy().get("ranks"):
            raise TypeError("gang() needs the number of ranks: Class.options(ranks=n).gang(...)")
        check_call(self._cls, args, kwargs)
        return GangActor(self, args, kwargs)


def _create_actor(rt: Runtime, cls: type[Any], eff: Effective, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Actor[Any]:
    name = eff.get("name")
    tenant = rt.tenant
    ctx = _context.current_task()
    run = ctx.run if ctx is not None else rt.run
    if run is None:
        raise ChimaeraError("no run: create actors from a driver or inside a task")
    if name:
        actor = _keyed_actor_id(tenant, name)
    else:
        actor = child_id(run, [b"actor", os.urandom(16)])
    lanes = sum(_lane_limits(cls).values())
    if lanes:
        # Concurrency groups and per-method lanes run beside the default lane,
        # so the peer may send that many more calls at once.
        own = eff.get("concurrency")
        own = own if isinstance(own, int) else 1
        eff = Effective({**eff.values, "concurrency": own + lanes}, {**eff.sources, "concurrency": "concurrency groups"})
    durable = eff.get("state") is not None
    if durable and eff.get("concurrency") not in (None, 1):
        raise OptionsError("a durable actor (state=ch.durable.State()) runs one call at a time; drop `concurrency`")
    ctor_key = run + child_id(run, [actor, b"ctor"])
    spec, _ = build_call(rt, cls, args, kwargs, kind=CALL_ACTOR_CTOR, eff=eff, actor=actor, key=ctor_key)
    seq = rt.client.create_actor(
        actor,
        spec,
        tenant,
        name or "",
        spec["fn_digest"],
        _RESTART[eff.get("restart")],
        int(eff.get("max_restarts") or 0),
        bool(eff.get("detach")),
        durable,
    )
    ev = rt.wait_actor(seq)
    if ev[0] == "error":
        msg = ev[2]
        raise Infeasible(msg) if msg.startswith("infeasible") else ChimaeraError(msg)
    rt.client.watch([ctor_key])
    return Actor(rt, actor, ctor_key, cls, eff)


# Attributes of an Actor handle itself; an actor method with one of these names is unreachable.
_HANDLE_NAMES = ("close", "id")


def _keyed_actor_id(tenant: str, key: str) -> bytes:
    h = blake3_digest(f"{tenant}\0{key}".encode())
    vshard = int.from_bytes(h[:2], "little") % 1024
    return derive_id(0, vshard, [b"actor", tenant.encode(), key.encode()])


def _lane_limits(cls: type[Any]) -> dict[str, int]:
    """Lanes beside an actor's default one: its concurrency groups, plus a
    private lane for each method declared with ``@ch.method(concurrency=N)``."""
    limits = dict(getattr(cls, "__chimaera_groups__", None) or {})
    for name in dir(cls):
        declared = getattr(inspect.getattr_static(cls, name, None), "__chimaera_method__", None) or {}
        if "group" not in declared and isinstance(declared.get("concurrency"), int):
            limits[f"method:{name}"] = declared["concurrency"]
        elif "batch" in declared:
            limits[f"method:{name}"] = declared["batch"].lane
    return limits


def actor(name: str) -> Actor[Any]:
    """The live actor created with ``name=`` (``Cls.options(name=...).remote()``),
    without needing its class. Raises ``NotFound`` if there is none."""
    rt = Runtime.get()
    actor_id = _keyed_actor_id(rt.tenant, name)
    rec = rt.client.get_actor(actor_id)
    if rec is None or rec[0] == "Dead":
        raise NotFound(f"no live actor named {name!r}")
    _state, ctor, class_digest = rec
    eff = resolve((), Layer({}, f"ch.actor({name!r})", "decorator"))
    return Actor(rt, actor_id, ctor, _load_class(rt, class_digest), eff, class_digest)


def _load_class(rt: Runtime, digest: bytes) -> type[Any] | None:
    """The actor's class from the peer's code store, when this process can read
    it (local mode). Otherwise None: methods are then called by name, unchecked."""
    try:
        with open(os.path.join(rt.info["code_dir"], digest.hex()), "rb") as f:
            cls = _serde.load_code(f.read())
    except (OSError, KeyError):
        return None
    return cls if inspect.isclass(cls) else None


class Actor(Generic[C]):
    """A handle to a remote actor. Calls through one handle run in submission order."""

    def __init__(
        self, rt: Runtime, actor: bytes, ctor_key: bytes, cls: type[C] | None, eff: Effective, digest: bytes = b""
    ) -> None:
        self._rt = rt
        self._actor = actor
        self._ctor = ctor_key
        self._cls = cls  # None for an actor found by key: methods are called by name
        self._digest = digest
        self._eff = eff
        self._handle = int.from_bytes(os.urandom(8), "little") >> 1
        self._seq = itertools.count()
        self._methods: dict[str, RemoteMethod[..., Any]] = {}

    @property
    def id(self) -> str:
        return self._actor.hex()

    def __repr__(self) -> str:
        cls = self._cls.__qualname__ if self._cls is not None else "(by key)"
        return f"<actor {cls} {self._actor.hex()[:12]}>"

    def __getattr__(self, name: str) -> RemoteMethod[..., Any]:
        if name.startswith("__"):
            raise AttributeError(name)
        method = self.__dict__.get("_methods", {}).get(name)
        if method is not None:  # one per name, so its resolved options are reused
            return method
        if self._cls is None:
            method = RemoteMethod(self, name, None)
        else:
            attr = getattr(self._cls, name, None)
            if not callable(attr):
                raise AttributeError(f"{self._cls.__qualname__} has no method {name!r}")
            method = RemoteMethod(self, name, attr)
        self._methods[name] = method
        return method

    def close(self) -> None:
        """End the actor: its process exits, pending calls raise ActorDied, and
        it is not restarted. Non-detached actors also end with their run."""
        self._rt.client.close_actor(self._actor)

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_actor, (self._actor, self._ctor, self._cls, self._digest))


def _rebuild_actor(actor: bytes, ctor: bytes, cls: type[Any] | None, digest: bytes = b"") -> Actor[Any]:
    rt = Runtime.get()
    return Actor(rt, actor, ctor, cls, resolve((), Layer({}, "handle", "decorator")), digest)


class RemoteMethod(Generic[P, R]):
    def __init__(self, actor: Actor[Any], name: str, fn: Callable[..., Any] | None, view: tuple[Layer, ...] = ()) -> None:
        self._actor = actor
        self._name = name
        self._fn = fn
        self._view = view
        self._resolved: tuple[tuple[Layer, ...], Effective] | None = None

    def options(self, **opts: Unpack[Options]) -> RemoteMethod[P, R]:
        """Settings for calls through this view (``deadline``, ``returns``,
        ``max_attempts``, ``priority``, a stricter ``effects``...)."""
        source = f".options at {_site(2).rsplit('/', 1)[-1]}"
        layer = validate(opts, kind="view", target="function", source=source)
        return RemoteMethod(self._actor, self._name, self._fn, (layer, *self._view))

    def _policy(self) -> Effective:
        """The actor's settings, then the method's ``@ch.method``, then scopes and views."""
        a = self._actor
        declared = {k: v for k, v in (getattr(self._fn, "__chimaera_method__", None) or {}).items() if k != "group"}
        layers = self._view + ((Layer(declared, f"@ch.method on {self._name}", "view"),) if declared else ())
        return resolve_cached(self, layers, Layer(a._eff.values, "actor", "decorator"))

    def _check(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        if self._fn is not None:
            check_call(self._fn, args, kwargs, bound=True)

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        self._check(args, kwargs)
        a = self._actor
        return submit_call(
            a._rt,
            a._cls if a._cls is not None else a._digest,
            args,
            kwargs,
            kind=CALL_ACTOR_METHOD,
            eff=self._policy(),
            actor=a._actor,
            handle=a._handle,
            seq=next(a._seq),
            method=self._name,
            extra_deps=(a._ctor,),
            site=_site(),
            durable=a._eff.get("state") is not None,
        )

    def map(self, source: Iterable[Any], *, ordered: bool = True) -> Results[Any]:
        a = self._actor
        eff = self._policy()
        window = eff.get("max_in_flight") or 64

        def submit(i: int, item: Any) -> Future[Any]:
            self._check((item,), {})
            return submit_call(
                a._rt,
                a._cls if a._cls is not None else a._digest,
                (item,),
                {},
                kind=CALL_ACTOR_METHOD,
                eff=eff,
                actor=a._actor,
                handle=a._handle,
                seq=next(a._seq),
                method=self._name,
                extra_deps=(a._ctor,),
                item_index=i,
            )

        return Results(submit, iter(source), ordered=ordered, window=window)


class _Decorator(Protocol):
    """What ``@ch.remote(...)`` with options returns."""

    @overload
    def __call__(self, obj: type[C], /) -> ActorClass[C]: ...
    @overload
    def __call__(self, obj: Callable[P, R], /) -> RemoteFunction[P, R]: ...


@overload
def remote(obj: type[C], /) -> ActorClass[C]: ...
@overload
def remote(obj: Callable[P, R], /) -> RemoteFunction[P, R]: ...
@overload
def remote(obj: None = None, /, **options: Unpack[Options]) -> _Decorator: ...
def remote(obj: Any = None, /, **options: Any) -> Any:
    """Make a function or class usable remotely. ``@ch.remote`` and
    ``@ch.remote(...)`` are equivalent; options set defaults for every call.

    For mypy, add ``plugins = chimaera.mypy`` to your config: it types
    ``Cls.remote(...)`` and actor handles' methods (see chimaera/mypy.py)."""
    if obj is None:
        return lambda o: _wrap(o, options)
    return _wrap(obj, options)


def _decorator_layer(obj: Any, options: dict[str, Any], target: str, name: str) -> Layer:
    """The decorator's options. Chimaera's own decorated helpers (queue hosts,
    gang ranks...) are marked internal: a caller's `with ch.options` block
    must not resize or retry them."""
    layer = validate(options, kind="decorator", target=target, source=f"@ch.remote on {name}")
    internal = str(getattr(obj, "__module__", "") or "").startswith("chimaera.")
    return dataclasses.replace(layer, internal=True) if internal else layer


def _wrap(obj: Any, options: dict[str, Any]) -> Any:
    name = getattr(obj, "__qualname__", repr(obj))
    if inspect.isclass(obj):
        layer = _decorator_layer(obj, options, "actor", name)
        shadowed = sorted(n for n in _HANDLE_NAMES if callable(getattr(obj, n, None)))
        if shadowed:
            warnings.warn(
                f"{name} defines {', '.join(shadowed)}, which its actor handle reserves "
                f"(handle.close() ends the actor; handle.id is its id): rename the method to call it remotely",
                stacklevel=3,
            )
        groups = layer.values.get("groups") or {}
        for attr in dir(obj):
            group = (getattr(inspect.getattr_static(obj, attr, None), "__chimaera_method__", None) or {}).get("group")
            if group is not None and group not in groups:
                known = ", ".join(map(repr, groups)) or "none declared"
                raise TypeError(f"{name}.{attr} uses group {group!r}, but @ch.remote(groups=...) declares {known}")
        if groups:
            obj.__chimaera_groups__ = dict(groups)  # read by the executor to size each lane
        return ActorClass(obj, layer)
    if "groups" in options:
        raise TypeError(f"groups= applies to actor classes, not to {name}")
    if callable(obj):
        if options.get("workflow") == "durable":
            if not inspect.iscoroutinefunction(obj):
                raise TypeError(f"workflow='durable' needs an async def function; {name} is not")
            # Read by the executor; replays need retries beyond the usual three.
            obj.__chimaera_workflow__ = True
            options = {"max_attempts": 100, **options}
        return RemoteFunction(obj, _decorator_layer(obj, options, "function", name))
    raise TypeError(f"@ch.remote applies to functions and classes, not {type(obj).__name__}")


def method(*, group: str | None = None, batch: Batch | None = None, **options: Any) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Per-method settings on an actor class.

    ``group="io"`` runs the method in that concurrency group (declared with
    ``@ch.remote(groups={"io": 8})``), beside the actor's other calls.
    ``concurrency=N`` gives the method its own lane of N overlapping calls.
    ``batch=ch.Batch(...)`` calls it once per list of calls (see ``ch.Batch``).
    Other options (``returns``, ``max_attempts``, ``deadline``...) apply to
    every call of the method. Calls start in the order they were made.
    """
    layer = validate(options, kind="view", target="function", source="@ch.method")
    if group is not None and not isinstance(group, str):
        raise TypeError(f"@ch.method(group=...) takes a group name, got {group!r}")
    if group is not None and "concurrency" in layer.values:
        raise TypeError("@ch.method takes group= or concurrency=, not both: a group sets its own concurrency")
    if batch is not None and not isinstance(batch, Batch):
        raise TypeError(f"@ch.method(batch=...) takes ch.Batch(max_size=, max_wait=), got {batch!r}")
    if batch is not None and (group is not None or "concurrency" in layer.values):
        raise TypeError("@ch.method(batch=...) runs in its own lane, sized by the batch: drop group= and concurrency=")

    def deco(fn: Callable[P, R]) -> Callable[P, R]:
        settings = dict(layer.values)
        if group is not None:
            settings["group"] = group
        if batch is not None:
            settings["batch"] = batch
        fn.__chimaera_method__ = settings  # type: ignore[attr-defined]
        return fn

    return deco


CALL_KINDS = (CALL_FUNCTION, CALL_ACTOR_CTOR, CALL_ACTOR_METHOD, CALL_MAP_ITEM)


@remote(cpus=0.01, latency="interactive")
def _gathered(*values: Any) -> list[Any]:
    return list(values)


def gather(*futures: Future[T]) -> Future[list[T]]:
    """One future of all the futures' values, in order: ``await`` it, call
    ``.result()``, or pass it to another call, which receives the list. It
    fails with the first error. ``ch.get(futures)`` waits for the values here."""
    from ._future import put

    for f in futures:
        if not isinstance(f, Future):
            hint = " (unpack a list: ch.gather(*futures), or wait with ch.get(futures))" if isinstance(f, (list, tuple, set)) else ""
            raise TypeError(f"ch.gather takes futures as separate arguments, not {type(f).__name__}{hint}")
    return _gathered.remote(*futures) if futures else put([])


class _ExecutorFuture(concurrent.futures.Future):  # type: ignore[type-arg]
    def __init__(self, inner: Future[Any]) -> None:
        super().__init__()
        self._inner = inner

    def cancel(self) -> bool:
        return self._inner.cancel() and super().cancel()

    def _settle(self) -> None:
        if self.cancelled():
            return
        try:
            value = self._inner.result()
        except BaseException as e:  # noqa: BLE001 -- the call's error is this future's
            if not self.done():
                self.set_exception(e)
        else:
            if not self.done():
                self.set_result(value)


class Executor(concurrent.futures.Executor):
    """A ``concurrent.futures`` executor whose calls run through Chimaera, so
    code written for ``ProcessPoolExecutor`` runs unchanged, lambdas and
    closures included, and on a cluster. ``options`` apply to every call, as
    on ``@ch.remote(...)``. Futures complete on a thread of their own, so a
    slow done-callback never holds up other results."""

    def __init__(self, **options: Any) -> None:
        validate(options, kind="decorator", target="function", source="ch.Executor(...)")
        self._options = options
        self._pending: set[concurrent.futures.Future[Any]] = set()
        self._settler = concurrent.futures.ThreadPoolExecutor(1, thread_name_prefix="chimaera-executor")
        self._lock = threading.Lock()
        self._closed = False

    def submit(self, fn: Callable[..., R], /, *args: Any, **kwargs: Any) -> concurrent.futures.Future[R]:
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot submit to a ch.Executor after shutdown")
        if isinstance(fn, RemoteFunction):  # already decorated: keep its options
            rf = fn.options(**self._options) if self._options else fn
        else:
            # A class is called like any function here (its instance comes
            # back), as with ProcessPoolExecutor, never made an actor.
            rf = remote(functools.partial(fn) if inspect.isclass(fn) else fn, **self._options)
        inner = rf.remote(*args, **kwargs)
        if not isinstance(inner, Future) or isinstance(inner, Results):
            inner.close()
            raise TypeError(f"{getattr(fn, '__qualname__', fn)} is a generator function; call it with ch.remote(fn).remote(...) to stream its items")
        out = _ExecutorFuture(inner)
        with self._lock:
            self._pending.add(out)
        out.add_done_callback(self._forget)
        inner._on_done(lambda _: self._settler.submit(out._settle))
        return out

    def _forget(self, f: concurrent.futures.Future[Any]) -> None:
        with self._lock:
            self._pending.discard(f)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._lock:
            self._closed = True
            pending = list(self._pending)
        if cancel_futures:
            for f in pending:
                f.cancel()
        if wait:
            concurrent.futures.wait(pending)
        self._settler.shutdown(wait=wait)
