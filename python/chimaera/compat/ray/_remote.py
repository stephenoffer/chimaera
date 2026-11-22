"""``@ray.remote`` on functions and classes, as thin views over ``@ch.remote``."""

from __future__ import annotations

import functools
import inspect
import sys
import types
from collections.abc import Callable, Mapping
from typing import Any

import chimaera as ch

from . import _options
from ._unsupported import unsupported, warn_once


class PutRef:
    """What ``ray.put`` returns: a resolved ``ch.Future`` that stays a reference.

    ``ch.put`` returns a resolved future, and Chimaera passes such a future to
    a callee as its value wherever it appears in the arguments. Ray keeps a
    reference nested in a list a reference (the callee calls ``ray.get``), so
    the shim wraps it; at the top level of an argument list it is resolved, as
    in Ray.
    """

    __slots__ = ("_ref",)

    def __init__(self, ref: ch.Future[Any]) -> None:
        self._ref = ref

    def result(self, timeout: float | None = None) -> Any:
        return self._ref.result(timeout)

    def __reduce__(self) -> tuple[Any, ...]:
        return (PutRef, (self._ref,))

    def __repr__(self) -> str:
        return f"<ObjectRef put {self._ref.id[-12:]}>"


def _resolve(x: Any) -> Any:
    # Ray passes top-level ``ray.put`` references to the callee as values.
    return x.result() if isinstance(x, PutRef) else x


def _as_ray(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Ray resolves only top-level refs; refs inside a list, tuple or dict
    argument arrive as refs. Chimaera resolves them one level deep, so the shim
    marks those containers ``ch.handles``."""

    def keep(v: Any) -> Any:
        return ch.handles(v) if type(v) in (list, tuple, dict) else v

    return tuple(keep(a) for a in args), {k: keep(v) for k, v in kwargs.items()}


def _resolving(fn: Callable[..., Any], alias: bool) -> Callable[..., Any]:
    """`fn` with its top-level ObjectRef arguments resolved. With `alias`, the
    callee also sees this shim as ``ray`` (the driver installed it)."""

    def prepare(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        # Absolute import: this closure is pickled by value into executors.
        if alias and sys.modules.get("ray") is not sys.modules.get("chimaera.compat.ray"):
            from chimaera.compat.ray import install

            install()
        return tuple(_resolve(a) for a in args), {k: _resolve(v) for k, v in kwargs.items()}

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def call_async(*args: Any, **kwargs: Any) -> Any:
            a, k = prepare(args, kwargs)
            return await fn(*a, **k)

        return call_async

    @functools.wraps(fn)
    def call(*args: Any, **kwargs: Any) -> Any:
        a, k = prepare(args, kwargs)
        return fn(*a, **k)

    return call


def _resolving_class(cls: type, alias: bool) -> type:
    """A subclass of `cls` whose methods resolve ObjectRef arguments."""
    ns: dict[str, Any] = {}
    for name in dir(cls):
        attr = inspect.getattr_static(cls, name)
        if isinstance(attr, types.FunctionType) and (name == "__init__" or not name.startswith("__")):
            ns[name] = _resolving(attr, alias)
    try:
        sub = type(cls.__name__, (cls,), ns)
    except TypeError:  # a metaclass that refuses subclassing
        warn_once("CHR003", cls.__qualname__, "ray.put references passed to its methods arrive unresolved")
        return cls
    sub.__qualname__, sub.__module__, sub.__doc__ = cls.__qualname__, cls.__module__, cls.__doc__
    return sub


def _installed() -> bool:
    return sys.modules.get("ray") is sys.modules.get("chimaera.compat.ray")


class RemoteFunction:
    """A Ray remote function. ``.remote()`` returns a ``chimaera.Future``, or a
    list of them with ``num_returns=n``."""

    def __init__(self, fn: Callable[..., Any], options: Mapping[str, Any], base: ch.typing.RemoteFunction | None = None) -> None:
        if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
            raise unsupported("a generator remote function", use="generator functions in chimaera directly", lands="M1")
        self._fn = fn
        self._options = dict(options)
        self._base = base or ch.remote(_resolving(fn, _installed()))
        translated = _options.translate(self._options, _options.TASK)
        self._returns = translated.get("returns", 1)
        self._view = self._base.options(**translated)
        functools.update_wrapper(self, fn)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        name = getattr(self._fn, "__name__", "f")
        raise TypeError(f"Remote functions cannot be called directly. Instead of running '{name}()', try '{name}.remote()'.")

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        args, kwargs = _as_ray(args, kwargs)
        f = self._view.remote(*args, **kwargs)
        return list(f.split(self._returns)) if self._returns > 1 else f

    def options(self, **options: Any) -> RemoteFunction:
        return RemoteFunction(self._fn, {**self._options, **options}, self._base)

    def bind(self, *args: Any, **kwargs: Any) -> Any:
        raise unsupported("Ray DAG .bind()", use="ch.graph(...)", lands="P5")


class ActorClass:
    """A Ray actor class. ``.remote()`` returns an ``ActorHandle``."""

    def __init__(self, cls: type, options: Mapping[str, Any], base: ch.typing.ActorClass[Any] | None = None) -> None:
        self._cls = cls
        self._options = dict(options)
        groups = _options.translate(self._options, _options.ACTOR).pop("groups", None)  # fail at decoration, as Ray does
        if base is None or (groups and groups != getattr(base._cls, "__chimaera_groups__", None)):
            # Concurrency groups belong to the class (Chimaera reads them from the decorator).
            base = ch.remote(**({"groups": groups} if groups else {}))(_resolving_class(cls, _installed()))
        self._base = base
        functools.update_wrapper(self, cls, updated=())

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        name = self._cls.__name__
        raise TypeError(f"Actors cannot be instantiated directly. Instead of '{name}()', use '{name}.remote()'.")

    def remote(self, *args: Any, **kwargs: Any) -> ActorHandle:
        opts = _options.translate(self._options, _options.ACTOR)
        opts.pop("groups", None)
        key = _options.actor_key(self._options, _default_namespace)
        if key:
            opts["name"] = key
        if "concurrency" not in opts and _has_async_methods(self._cls):
            opts["concurrency"] = 1000  # Ray's default for async actors
        args, kwargs = _as_ray(args, kwargs)
        return ActorHandle(self._base.options(**opts).remote(*args, **kwargs))

    def options(self, **options: Any) -> ActorClass:
        return ActorClass(self._cls, {**self._options, **options}, self._base)

    def bind(self, *args: Any, **kwargs: Any) -> Any:
        raise unsupported("Ray DAG .bind()", use="ch.graph(...)", lands="P5")


class ActorHandle:
    """A Ray actor handle over a ``chimaera.typing.Actor``: methods by name, with
    Ray's ``num_returns`` (a list of refs) and per-call ``.options()``."""

    __slots__ = ("_actor",)

    def __init__(self, actor: ch.typing.Actor[Any]) -> None:
        self._actor = actor

    def __getattr__(self, name: str) -> ActorMethod:
        if name.startswith("__"):
            raise AttributeError(name)
        return ActorMethod(getattr(self._actor, name))

    def __reduce__(self) -> tuple[Any, ...]:
        return (ActorHandle, (self._actor,))

    def __repr__(self) -> str:
        return f"Actor({self._actor!r})"


class ActorMethod:
    __slots__ = ("_method",)

    def __init__(self, method: Any) -> None:
        self._method = method

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        args, kwargs = _as_ray(args, kwargs)
        f = self._method.remote(*args, **kwargs)
        try:  # a method declared (or called) with num_returns=n gives n refs, as in Ray
            return list(f.split())
        except TypeError:
            return f

    def options(self, **options: Any) -> ActorMethod:
        """Ray's per-call method options: num_returns, max_task_retries, retry_exceptions."""
        out: dict[str, Any] = {}
        for name, value in options.items():
            if name == "num_returns":
                if value in ("streaming", "dynamic"):
                    raise unsupported(f"num_returns={value!r}", use="a generator method", lands="M1")
                if int(value) > 1:
                    out["returns"] = int(value)
            elif name in ("max_task_retries", "max_retries"):
                out["max_attempts"] = _options.translate({"max_retries": value}, _options.TASK)["max_attempts"]
            elif name == "retry_exceptions":
                out.update({k: v for k, v in _options.translate({name: value}, _options.TASK).items() if k == name})
            elif name not in ("name", "enable_task_events"):
                raise unsupported(f"actor method .options({name}=...)", use="method .options() in chimaera")
        return ActorMethod(self._method.options(**out))


def get_actor(name: str, namespace: str | None = None) -> ActorHandle:
    """A named actor, looked up by name."""
    try:
        return ActorHandle(ch.actor(f"{namespace or _default_namespace()}/{name}"))
    except ch.NotFound:
        raise ValueError(f"Failed to look up actor with name '{name}'.") from None


def _has_async_methods(cls: type) -> bool:
    return any(inspect.iscoroutinefunction(inspect.getattr_static(cls, n)) for n in dir(cls) if not n.startswith("__"))


def _default_namespace() -> str:
    from . import _state

    if _state.namespace:
        return _state.namespace
    ctx = ch.context()
    return f"run-{ctx.run_id if ctx is not None else ch.runs.current_run().id}"


def make(obj: Any, options: Mapping[str, Any]) -> RemoteFunction | ActorClass:
    if inspect.isclass(obj):
        return ActorClass(obj, options)
    if callable(obj):
        return RemoteFunction(obj, options)
    raise TypeError("The @ray.remote decorator must be applied to either a function or a class.")
