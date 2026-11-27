"""``@ray.remote`` on functions and classes, as thin views over ``@ch.remote``."""

from __future__ import annotations

import functools
import inspect
import os
import sys
import types
from collections.abc import Callable, Generator, Mapping
from typing import Any

import chimaera as ch

from . import _ids

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

    __slots__ = ("_ref", "_task", "__weakref__")

    def __init__(self, ref: ch.Future[Any], task: bytes | None = None) -> None:
        self._ref = ref
        self._task = task  # the call that put it (ObjectRef.task_id()), None for a driver

    def result(self, timeout: float | None = None) -> Any:
        return self._ref.result(timeout)

    def __await__(self) -> Generator[Any, None, Any]:
        return self._ref.__await__()

    def __reduce__(self) -> tuple[Any, ...]:
        return (PutRef, (self._ref, self._task))

    def __repr__(self) -> str:
        return f"<ObjectRef put {self._ref.id[-12:]}>"


class ItemRef:
    """One item of an ``ObjectRefGenerator``: already resolved (or the error
    the generator raised there, raised by ``ray.get``)."""

    __slots__ = ("_value", "_error", "_id", "__weakref__")

    def __init__(self, value: Any = None, error: BaseException | None = None, id: bytes | None = None) -> None:
        self._value, self._error = value, error
        self._id = id or _ids.item_id()  # its ObjectRef id: an item is not stored under one of its own

    def result(self, timeout: float | None = None) -> Any:
        if self._error is not None:
            raise self._error
        return self._value

    def __await__(self) -> Generator[Any, None, Any]:
        yield from ()  # resolved already: nothing to wait for
        return self.result()

    def __reduce__(self) -> tuple[Any, ...]:
        return (ItemRef, (self._value, self._error, self._id))

    def __repr__(self) -> str:
        return "<ObjectRef generator item>"


class ObjectRefGenerator:
    """What a generator task or method returns in Ray: one ref per yielded
    value, in order, as they arrive (``for ref in gen: ray.get(ref)``)."""

    def __init__(self, results: ch.Results[Any]) -> None:
        self._results, self._it, self._done = results, iter(results), False

    def __iter__(self) -> ObjectRefGenerator:
        return self

    def __next__(self) -> ItemRef:
        if self._done:
            raise StopIteration
        try:
            return ItemRef(next(self._it))
        except StopIteration:
            self._done = True
            raise
        except Exception as e:  # Ray hands the error out as the next ref
            self._done = True
            return ItemRef(error=e)

    def __aiter__(self) -> ObjectRefGenerator:
        return self

    async def __anext__(self) -> ItemRef:
        if self._done:
            raise StopAsyncIteration
        try:  # the stream's own async read: cancelled while it waits, it takes no item
            return ItemRef(await self._results.__anext__())
        except StopAsyncIteration:
            self._done = True
            raise
        except Exception as e:  # Ray hands the error out as the next ref
            self._done = True
            return ItemRef(error=e)

    def completed(self) -> ItemRef:
        """A ref that resolves once the generator has finished."""
        for _ in self:
            pass
        return ItemRef(None)


_PACKAGE = os.path.dirname(os.path.abspath(ch.__file__)) + os.sep


def stamped(f: Any) -> Any:
    """`f` (a call's or a put's future) marked with the line that called into
    the shim, the first frame outside this package, as Chimaera's own API marks
    its calls: its warnings name that line, and an error nothing reads is
    reported (Ray's "Unhandled error"), which a call made from inside the
    package is not. ObjectRef.call_site() gives it too."""
    if isinstance(f, ch.Future):
        frame = sys._getframe(1)
        while frame is not None and frame.f_code.co_filename.startswith(_PACKAGE):
            frame = frame.f_back
        if frame is not None:
            f._site = f"{frame.f_code.co_filename}:{frame.f_lineno}"
    return f


def _streamed(f: Any) -> Any:
    return ObjectRefGenerator(f) if isinstance(f, ch.Results) else f


def _resolve(x: Any) -> Any:
    # Ray passes top-level ``ray.put`` references to the callee as values.
    return x.result() if isinstance(x, (PutRef, ItemRef)) else x


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

    if inspect.isgeneratorfunction(fn):  # still a generator: the call streams

        @functools.wraps(fn)
        def call_gen(*args: Any, **kwargs: Any) -> Any:
            a, k = prepare(args, kwargs)
            yield from fn(*a, **k)

        return call_gen

    if inspect.isasyncgenfunction(fn):

        @functools.wraps(fn)
        async def call_agen(*args: Any, **kwargs: Any) -> Any:
            a, k = prepare(args, kwargs)
            async for item in fn(*a, **k):
                yield item

        return call_agen

    @functools.wraps(fn)
    def call(*args: Any, **kwargs: Any) -> Any:
        a, k = prepare(args, kwargs)
        return fn(*a, **k)

    return call


_GENERATOR_METHOD = "__chimaera_compat_generator__"


def _collected(fn: Callable[..., Any], alias: bool) -> Callable[..., Any]:
    """A generator method as a method returning its items: Chimaera streams
    generator functions, not methods, so the shim hands the items out as an
    ObjectRefGenerator once the method returns (``ActorMethod.remote``)."""
    gen = _resolving(fn, alias)

    @functools.wraps(fn)
    def call(*args: Any, **kwargs: Any) -> list[Any]:
        return list(gen(*args, **kwargs))

    setattr(call, _GENERATOR_METHOD, True)
    return call


def _resolving_class(cls: type, alias: bool) -> type:
    """A subclass of `cls` whose methods resolve ObjectRef arguments."""
    ns: dict[str, Any] = {}
    for name in dir(cls):
        attr = inspect.getattr_static(cls, name)
        if isinstance(attr, types.FunctionType) and (name == "__init__" or not name.startswith("__")):
            ns[name] = _collected(attr, alias) if inspect.isgeneratorfunction(attr) else _resolving(attr, alias)
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
        f = stamped(self._view.remote(*args, **kwargs))  # before split: the parts take its line
        return list(f.split(self._returns)) if self._returns > 1 else _streamed(f)

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
        cls = getattr(self._actor, "_cls", None)
        streams = getattr(inspect.getattr_static(cls, name, None), _GENERATOR_METHOD, False) if cls else False
        return ActorMethod(getattr(self._actor, name), streams)

    def __reduce__(self) -> tuple[Any, ...]:
        return (ActorHandle, (self._actor,))

    def __repr__(self) -> str:
        return f"Actor({self._actor!r})"


class _Items:
    """A finished generator method's items, iterated like ``ch.Results``."""

    def __init__(self, future: ch.Future[list[Any]]) -> None:
        self._future = future

    def __iter__(self) -> Any:
        return iter(self._future.result())


class ActorMethod:
    __slots__ = ("_method", "_streams")

    def __init__(self, method: Any, streams: bool = False) -> None:
        self._method, self._streams = method, streams

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        args, kwargs = _as_ray(args, kwargs)
        f = stamped(self._method.remote(*args, **kwargs))
        if self._streams:
            return ObjectRefGenerator(_Items(f))  # type: ignore[arg-type]
        if isinstance(f, ch.Results):
            return ObjectRefGenerator(f)
        try:  # a method declared (or called) with num_returns=n gives n refs, as in Ray
            return list(f.split())
        except TypeError:
            return f

    def options(self, **options: Any) -> ActorMethod:
        """Ray's per-call method options: num_returns, max_task_retries, retry_exceptions."""
        out: dict[str, Any] = {}
        for name, value in options.items():
            if name == "num_returns":
                if value == "streaming":
                    continue  # a generator method streams anyway
                if value == "dynamic":
                    raise unsupported("num_returns='dynamic'", use="num_returns='streaming' (an ObjectRefGenerator)")
                if int(value) > 1:
                    out["returns"] = int(value)
            elif name in ("max_task_retries", "max_retries"):
                out["max_attempts"] = _options.translate({"max_retries": value}, _options.TASK)["max_attempts"]
            elif name == "retry_exceptions":
                out.update({k: v for k, v in _options.translate({name: value}, _options.TASK).items() if k == name})
            elif name not in ("name", "enable_task_events"):
                raise unsupported(f"actor method .options({name}=...)", use="method .options() in chimaera")
        return ActorMethod(self._method.options(**out), self._streams)


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
