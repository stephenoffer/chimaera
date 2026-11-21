"""``@ch.remote`` for functions and classes."""

from __future__ import annotations

import datetime as _dt
import functools
import inspect
import itertools
import os
import sys
import time
from collections.abc import Callable, Iterable
from typing import Any, Generic, ParamSpec, TypeVar

from . import _context
from ._errors import ChimaeraError, Infeasible
from ._future import Future, Results
from ._native import blake3_digest, child_id, derive_id
from ._options import Effective, Layer, policy_dict, resolve, validate
from ._runtime import (
    CALL_ACTOR_CTOR,
    CALL_ACTOR_METHOD,
    CALL_FUNCTION,
    CALL_MAP_ITEM,
    EFFECTS_CODE,
    Runtime,
)
from ._serde import Dep

P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")

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
) -> tuple[dict[str, Any], bytes]:
    """Encode a call. Futures passed directly as arguments become dependency
    edges (the callee receives their values; nothing flows through here)."""
    deps: list[bytes] = []

    def sub(x: Any) -> Any:
        if isinstance(x, Future):
            deps.append(x.key)
            return Dep(len(deps) - 1)
        return x

    frame = (tuple(sub(a) for a in args), {k: sub(v) for k, v in kwargs.items()})
    key = key or rt.new_key()
    packed = rt.pack(key, frame)
    ctx = _context.current_task()
    effects = eff.get("effects")
    retry = eff.get("retry_exceptions") or ()
    spec = {
        "key": key,
        "kind": kind,
        "fn_digest": rt.code_digest(target),
        **packed,
        "deps": deps + list(extra_deps),
        "effects": EFFECTS_CODE[effects],
        "max_attempts": int(eff.get("max_attempts")),
        "deadline_unix_ms": _deadline_ms(eff),
        "policy": policy_dict(eff),
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
    return spec, key


def submit_call(rt: Runtime, target: Any, args: tuple[Any, ...], kwargs: dict[str, Any], **kw: Any) -> Future[Any]:
    site = kw.pop("site", None)
    spec, key = build_call(rt, target, args, kwargs, **kw)
    fut: Future[Any] = Future(rt, key, site)
    fut._arg_bytes = spec["args_size"]
    rt.submit([spec], [fut])
    return fut


class RemoteFunction(Generic[P, R]):
    """A function usable locally (``fn(x)``) or remotely (``fn.remote(x)``)."""

    def __init__(self, fn: Callable[P, R], decorator: Layer, view: tuple[Layer, ...] = ()) -> None:
        self._fn = fn
        self._decorator = decorator
        self._view = view
        functools.update_wrapper(self, fn)  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return f"<remote function {getattr(self._fn, '__qualname__', self._fn)!r}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._fn(*args, **kwargs)

    def options(self, **opts: Any) -> RemoteFunction[P, R]:
        """A configured view; the original function is unchanged."""
        layer = validate(opts, kind="view", target="function", source=f".options at {_site(2).rsplit('/', 1)[-1]}")
        return RemoteFunction(self._fn, self._decorator, (layer, *self._view))

    def partial(self, *args: Any, **kwargs: Any) -> RemoteFunction[..., R]:
        """Bind constants once; they ship with the code, not with every call."""
        return RemoteFunction(functools.partial(self._fn, *args, **kwargs), self._decorator, self._view)  # type: ignore[arg-type]

    def policy(self) -> Effective:
        """Effective options and where each value came from."""
        return resolve(self._view, self._decorator)

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        if inspect.isgeneratorfunction(self._fn) or inspect.isasyncgenfunction(self._fn):
            raise TypeError(
                f"{self._fn.__qualname__} is a generator; streaming generator calls are not available yet. "
                "Return a list, or use .map over the inputs."
            )
        inspect.signature(self._fn).bind(*args, **kwargs)
        rt = Runtime.get()
        return submit_call(rt, self._fn, args, kwargs, kind=CALL_FUNCTION, eff=self.policy(), site=_site())

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

        return Results(submit, source, ordered=ordered, window=window, window_bytes=window_bytes)


class ActorClass(Generic[C]):
    """A class usable locally (``Cls(...)``) or as a remote actor (``Cls.remote(...)``)."""

    def __init__(self, cls: type[C], decorator: Layer, view: tuple[Layer, ...] = ()) -> None:
        self._cls = cls
        self._decorator = decorator
        self._view = view
        functools.update_wrapper(self, cls, updated=())  # type: ignore[arg-type]

    def __repr__(self) -> str:
        return f"<remote class {self._cls.__qualname__!r}>"

    def __call__(self, *args: Any, **kwargs: Any) -> C:
        return self._cls(*args, **kwargs)

    def options(self, **opts: Any) -> ActorClass[C]:
        layer = validate(opts, kind="view", target="actor", source=f".options at {_site(2).rsplit('/', 1)[-1]}")
        return ActorClass(self._cls, self._decorator, (layer, *self._view))

    def policy(self) -> Effective:
        return resolve(self._view, self._decorator)

    def remote(self, *args: Any, **kwargs: Any) -> Actor[C]:
        """Create an actor. With ``key=...`` it is get-or-create by name."""
        inspect.signature(self._cls).bind(*args, **kwargs)
        return _create_actor(Runtime.get(), self._cls, self.policy(), args, kwargs)

    def pool(self, *args: Any, **kwargs: Any) -> Pool[C]:
        """Independent replicas, each constructed with these arguments."""
        inspect.signature(self._cls).bind(*args, **kwargs)
        eff = self.policy()
        rt = Runtime.get()
        n = eff.get("replicas")
        if n is None or not isinstance(n, int):
            # Without the pool controller (latency/utilization), "auto" starts at
            # one replica per available core, at most 8.
            n = max(1, min(8, int(rt.info["usable_cores"])))
        return Pool([_create_actor(rt, self._cls, eff, args, kwargs) for _ in range(n)])


def _create_actor(rt: Runtime, cls: type[Any], eff: Effective, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Actor[Any]:
    name = eff.get("key")
    tenant = "default"
    ctx = _context.current_task()
    run = ctx.run if ctx is not None else rt.run
    if run is None:
        raise ChimaeraError("no run: create actors from a driver or inside a task")
    if name:
        h = blake3_digest(f"{tenant}\0{name}".encode())
        vshard = int.from_bytes(h[:2], "little") % 1024
        actor = derive_id(0, vshard, [b"actor", tenant.encode(), name.encode()])
    else:
        actor = child_id(run, [b"actor", os.urandom(16)])
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
        eff.get("lifetime") == "detached",
    )
    ev = rt.wait_actor(seq)
    if ev[0] == "error":
        msg = ev[2]
        raise Infeasible(msg) if msg.startswith("infeasible") else ChimaeraError(msg)
    rt.client.watch([ctor_key])
    return Actor(rt, actor, ctor_key, cls, eff)


class Actor(Generic[C]):
    """A handle to a remote actor. Calls through one handle run in submission order."""

    def __init__(self, rt: Runtime, actor: bytes, ctor_key: bytes, cls: type[C], eff: Effective) -> None:
        self._rt = rt
        self._actor = actor
        self._ctor = ctor_key
        self._cls = cls
        self._eff = eff
        self._handle = int.from_bytes(os.urandom(8), "little") >> 1
        self._seq = itertools.count()

    @property
    def id(self) -> str:
        return self._actor.hex()

    def __repr__(self) -> str:
        return f"<actor {self._cls.__qualname__} {self._actor.hex()[:12]}>"

    def __getattr__(self, name: str) -> RemoteMethod[..., Any]:
        if name.startswith("__"):
            raise AttributeError(name)
        attr = getattr(self._cls, name, None)
        if not callable(attr):
            raise AttributeError(f"{self._cls.__qualname__} has no method {name!r}")
        return RemoteMethod(self, name, attr)

    def close(self) -> None:
        """End the actor: its process exits, pending calls raise ActorDied, and
        it is not restarted. Non-detached actors also end with their run."""
        self._rt.client.close_actor(self._actor)

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_actor, (self._actor, self._ctor, self._cls))


def _rebuild_actor(actor: bytes, ctor: bytes, cls: type[Any]) -> Actor[Any]:
    rt = Runtime.get()
    return Actor(rt, actor, ctor, cls, resolve((), Layer({}, "handle", "decorator")))


class RemoteMethod(Generic[P, R]):
    def __init__(self, actor: Actor[Any], name: str, fn: Callable[..., Any]) -> None:
        self._actor = actor
        self._name = name
        self._fn = fn

    def _check(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        inspect.signature(self._fn).bind(None, *args, **kwargs)

    def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
        self._check(args, kwargs)
        a = self._actor
        return submit_call(
            a._rt,
            a._cls,
            args,
            kwargs,
            kind=CALL_ACTOR_METHOD,
            eff=a._eff,
            actor=a._actor,
            handle=a._handle,
            seq=next(a._seq),
            method=self._name,
            extra_deps=(a._ctor,),
            site=_site(),
        )

    def map(self, source: Iterable[Any], *, ordered: bool = True) -> Results[Any]:
        a = self._actor
        window = a._eff.get("max_in_flight") or 64

        def submit(i: int, item: Any) -> Future[Any]:
            self._check((item,), {})
            return submit_call(
                a._rt,
                a._cls,
                (item,),
                {},
                kind=CALL_ACTOR_METHOD,
                eff=a._eff,
                actor=a._actor,
                handle=a._handle,
                seq=next(a._seq),
                method=self._name,
                extra_deps=(a._ctor,),
                item_index=i,
            )

        return Results(submit, iter(source), ordered=ordered, window=window)


class Pool(Generic[C]):
    """Independent replicas of an actor class. Calls go to the replica with the
    fewest calls in flight; each replica keeps its own call order."""

    def __init__(self, replicas: list[Actor[C]]) -> None:
        self._replicas = replicas
        self._inflight = [0] * len(replicas)

    def __len__(self) -> int:
        return len(self._replicas)

    def __getattr__(self, name: str) -> _PoolMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        getattr(self._replicas[0], name)
        return _PoolMethod(self, name)

    def _pick(self) -> int:
        return min(range(len(self._replicas)), key=self._inflight.__getitem__)

    def close(self) -> None:
        for r in self._replicas:
            r.close()
        self._replicas = []

    def __enter__(self) -> Pool[C]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _PoolMethod:
    def __init__(self, pool: Pool[Any], name: str) -> None:
        self._pool = pool
        self._name = name

    def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
        i = self._pool._pick()
        self._pool._inflight[i] += 1
        f = getattr(self._pool._replicas[i], self._name).remote(*args, **kwargs)

        def done(_: Future[Any], i: int = i) -> None:
            self._pool._inflight[i] -= 1

        f.add_done_callback(done)
        return f

    def map(self, source: Iterable[Any], *, ordered: bool = True) -> Results[Any]:
        window = 16 * max(1, len(self._pool))
        return Results(lambda i, item: self.remote(item), iter(source), ordered=ordered, window=window)


def remote(obj: Any = None, /, **options: Any) -> Any:
    """Make a function or class usable remotely. ``@ch.remote`` and
    ``@ch.remote(...)`` are equivalent; options set defaults for every call."""
    if obj is None:
        return lambda o: _wrap(o, options)
    return _wrap(obj, options)


def _wrap(obj: Any, options: dict[str, Any]) -> Any:
    name = getattr(obj, "__qualname__", repr(obj))
    if inspect.isclass(obj):
        return ActorClass(obj, validate(options, kind="decorator", target="actor", source=f"@ch.remote on {name}"))
    if callable(obj):
        if options.get("workflow") == "durable" and not inspect.iscoroutinefunction(obj):
            raise TypeError(f"workflow='durable' needs an async def function; {name} is not")
        return RemoteFunction(obj, validate(options, kind="decorator", target="function", source=f"@ch.remote on {name}"))
    raise TypeError(f"@ch.remote applies to functions and classes, not {type(obj).__name__}")


def method(**options: Any) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Per-method settings on an actor class (``concurrency=N`` lets that
    method's calls overlap; FIFO start order is kept)."""
    layer = validate(options, kind="view", target="function", source="@ch.method")

    def deco(fn: Callable[P, R]) -> Callable[P, R]:
        fn.__chimaera_method__ = dict(layer.values)  # type: ignore[attr-defined]
        return fn

    return deco


CALL_KINDS = (CALL_FUNCTION, CALL_ACTOR_CTOR, CALL_ACTOR_METHOD, CALL_MAP_ITEM)
