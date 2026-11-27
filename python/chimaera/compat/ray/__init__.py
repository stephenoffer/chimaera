"""Ray Core on Chimaera.

    import chimaera.compat.ray as ray      # in your own code, or
    python -m chimaera.compat.ray app.py   # run an unmodified Ray program

Ray's core API (``remote``, ``get``, ``put``, ``wait``, ``cancel``, ``kill``,
named and detached actors, ``ActorPool``, exceptions) maps onto Chimaera's
public API. Anything that does not map raises ``Unsupported`` naming the
Chimaera alternative; options that mean something different warn once
(codes CHR001-CHR003). The full table is in docs/compat-ray.md.
"""

from __future__ import annotations

import abc
import importlib
import sys
import time
import types
from typing import Any

import chimaera as ch

from . import _ids, _state
from ._nodes import available_resources, cluster_resources, nodes
from ._ids import JobID, TaskID
from ._remote import ActorClass, ActorHandle, ItemRef, ObjectRefGenerator, PutRef, RemoteFunction, get_actor, make
from ._remote import stamped as _stamped
from ._unsupported import ChimaeraCompatWarning, Unsupported, UnsupportedAttribute, unsupported, warn_once
from .exceptions import GetTimeoutError, reraise
from .runtime_context import get_runtime_context

__version__ = "2.60.0+chimaera"


class ObjectRef(abc.ABC):
    """``isinstance(x, ray.ObjectRef)`` holds for Chimaera futures and ``put``
    references. ``ray.ObjectRef(id)`` is the live ref with that id (see _ids)."""

    def __new__(cls, id: bytes, *_: Any, **__: Any) -> Any:
        return _ids.find(id)

    from_binary = staticmethod(_ids.find)
    nil = staticmethod(_ids.nil)
    from_random = staticmethod(_ids.from_random)

    @staticmethod
    def from_hex(hex_id: str) -> Any:
        return _ids.find(bytes.fromhex(hex_id))

    @staticmethod
    def size() -> int:
        return _ids.SIZE


ObjectRef.register(ch.Future)
ObjectRef.register(PutRef)
ObjectRef.register(ItemRef)

_INIT_IGNORED = (
    "num_cpus",
    "num_gpus",
    "resources",
    "labels",
    "object_store_memory",
    "include_dashboard",
    "dashboard_host",
    "dashboard_port",
    "log_to_driver",
    "logging_level",
    "logging_format",
    "logging_config",
    "configure_logging",
    "log_to_stderr",
    "job_config",
    "_temp_dir",
    "_system_config",
    "_metrics_export_port",
    "_node_ip_address",
    "_enable_object_reconstruction",
)


def init(
    address: str | None = None,
    *,
    namespace: str | None = None,
    runtime_env: dict[str, Any] | None = None,
    ignore_reinit_error: bool = False,
    local_mode: bool = False,
    **kwargs: Any,
) -> types.SimpleNamespace:
    """Connect to the local Chimaera peer (started on demand)."""
    if _state.initialized:
        if ignore_reinit_error:
            return types.SimpleNamespace(run_id=ch.runs.current_run().id, dashboard_url=None)
        raise RuntimeError("Maybe you called ray.init twice by accident? Pass ignore_reinit_error=True to allow it.")
    if local_mode:
        raise unsupported("local_mode=True", why="local runs already use the same path as a cluster")
    if address not in (None, "local", "auto"):
        if str(address).startswith("ray://"):
            raise unsupported("ray:// addresses", why="there is no Ray Client protocol", use="ch.init(cluster=...)", lands="P3")
        raise unsupported(f"address={address!r}", use="ch.init(cluster=...)", lands="P3")
    for name, value in kwargs.items():
        if name not in _INIT_IGNORED:
            raise TypeError(f"init() got an unexpected keyword argument '{name}'")
        if value is not None:
            warn_once("CHR001", f"ray.init({name}=...)", "ignored: the local peer sizes itself from the machine")
    env = None
    if runtime_env:
        extra = set(runtime_env) - {"env_vars"}
        if extra:
            raise unsupported(f"runtime_env keys {sorted(extra)}", use="ch.Env", lands="P3")
        env = dict(runtime_env["env_vars"])
    run = ch.init(env=env)
    _state.initialized, _state.namespace = True, namespace
    return types.SimpleNamespace(run_id=run.id, dashboard_url=None)


def is_initialized() -> bool:
    return _state.initialized


def shutdown() -> None:
    """End the run as Ray ends its job: unfinished tasks are cancelled, and a
    later ``ray.init`` starts a new run."""
    _state.initialized, _state.namespace = False, None
    ch.shutdown(cancel=True)


def remote(*args: Any, **kwargs: Any) -> Any:
    """``@ray.remote`` or ``@ray.remote(**options)`` on a function or class."""
    if len(args) == 1 and not kwargs and callable(args[0]):
        return make(args[0], {})
    if args:
        raise TypeError("The @ray.remote decorator takes either no arguments or keyword options only.")
    return lambda obj: make(obj, kwargs)


def method(*args: Any, **kwargs: Any) -> Any:
    """``@ray.method(...)`` on an actor method."""
    if args:
        raise TypeError("@ray.method takes keyword arguments only.")
    settings: dict[str, Any] = {}
    for name, value in kwargs.items():
        if name == "num_returns":
            if value in ("streaming", "dynamic"):
                raise unsupported(f"@ray.method(num_returns={value!r})", use="a generator method", lands="M1")
            if int(value) > 1:
                settings["returns"] = int(value)
        elif name == "concurrency_group":
            settings["group"] = value
        elif name == "max_task_retries":
            settings["max_attempts"] = 100 if value == -1 else int(value) + 1
        elif name == "retry_exceptions":
            settings["retry_exceptions"] = (Exception,) if value is True else tuple(value or ())
        elif name == "tensor_transport":
            raise unsupported("@ray.method(tensor_transport=...)", use='transport="device"', lands="P5")
        elif name != "enable_task_events":
            raise ValueError(f"Invalid option keyword {name} for actor methods.")
    if not settings.get("retry_exceptions"):
        settings.pop("retry_exceptions", None)

    return ch.method(**settings) if settings else (lambda fn: fn)


def _get_one(ref: Any, timeout: float | None) -> Any:
    if not isinstance(ref, ObjectRef):
        raise ValueError(f"'object_refs' must either be an ObjectRef or a list of ObjectRefs, got {type(ref).__name__}.")
    try:
        return ref.result(timeout)
    except Exception as e:
        if isinstance(e, TimeoutError) and isinstance(ref, ch.Future) and not ref.done():
            raise GetTimeoutError(f"Get timed out: some object(s) not ready after {timeout} s.") from None
        reraise(e)


from ._ref_future import attach as _attach_future  # noqa: E402 -- needs _get_one, above

_attach_future(_get_one, (ch.Future, PutRef, ItemRef))  # ref.future(), as in Ray

_ids.attach((ch.Future, PutRef, ItemRef))  # ref.hex(), ref.task_id() and the rest, as in Ray


def get(object_refs: Any, *, timeout: float | None = None) -> Any:
    """Values for one ref or a list of refs. Errors come back Ray-shaped."""
    if not isinstance(object_refs, list):
        return _get_one(object_refs, timeout)
    deadline = None if timeout is None else time.monotonic() + timeout
    out = []
    for ref in object_refs:
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        out.append(_get_one(ref, left))
    return out


def put(value: Any, *, _owner: Any = None) -> PutRef:
    if _owner is not None:
        warn_once("CHR001", "put(_owner=...)", "ignored: objects belong to the run, not to an actor")
    return PutRef(_stamped(ch.put(value)), _ids.this_task())


def wait(
    object_refs: list[Any],
    *,
    num_returns: int = 1,
    timeout: float | None = None,
    fetch_local: bool = True,
) -> tuple[list[Any], list[Any]]:
    """(ready, not ready), each in input order; at most `num_returns` ready."""
    refs = list(object_refs)
    if not refs:
        return [], []
    if len({id(r) for r in refs}) != len(refs):
        raise ValueError("Wait requires a list of unique object refs.")
    if num_returns <= 0:
        raise ValueError(f"Invalid number of objects to return {num_returns}.")
    if num_returns > len(refs):
        raise ValueError("num_returns cannot be greater than the number of objects provided to ray.wait.")
    for r in refs:
        if not isinstance(r, ObjectRef):
            raise TypeError(f"wait() expected a list of ObjectRef, got an element of type {type(r).__name__}")

    def is_ready(r: Any) -> bool:
        return not isinstance(r, ch.Future) or r.done()

    pending = [r for r in refs if not is_ready(r)]
    need = num_returns - (len(refs) - len(pending))
    if need > 0:
        ch.wait(pending, num_returns=need, timeout=timeout)
    ready = [r for r in refs if is_ready(r)][:num_returns]
    chosen = {id(r) for r in ready}
    return ready, [r for r in refs if id(r) not in chosen]


def cancel(ref: Any, *, force: bool = False, recursive: bool = True) -> None:
    """Cancel a call. Its result becomes ``TaskCancelledError`` at once. Without
    `force` a running body is asked to stop (``ch.context().cancelled()``) and
    runs until it checks; with `force` its worker is killed."""
    if not recursive:
        raise unsupported("cancel(recursive=False)", why="cancelling a call cancels the work it started")
    if isinstance(ref, ch.Future):
        ref.cancel(force=force)
    elif not isinstance(ref, ObjectRef):
        raise TypeError(f"ray.cancel() only supported for object refs, got {type(ref).__name__}")


def get_gpu_ids() -> list[int]:
    """The GPUs this call was granted, as it numbers them (torch's ``cuda:N``)."""
    return [d["index"] for d in ch.dist.devices() if d["kind"] in ("cuda", "rocm", "xpu", "mps")]


def kill(actor: Any, *, no_restart: bool = True) -> None:
    if isinstance(actor, ActorHandle):
        actor = actor._actor
    if not isinstance(actor, ch.typing.Actor):
        raise ValueError(f"ray.kill() only supported for actors. For tasks, use ray.cancel(). Got {type(actor).__name__}.")
    if not no_restart:
        raise unsupported("kill(no_restart=False)", why="a killed actor is not restarted")
    actor.close()


_SUBMODULES = (
    "exceptions",
    "experimental",
    "actor",
    "runtime_context",
    "util",
    "util.actor_pool",
    "util.multiprocessing",
    "util.placement_group",
    "util.queue",
    "util.scheduling_strategies",
)


def install() -> None:
    """Make ``import ray`` (and its submodules) resolve to this shim in this process."""
    sys.modules["ray"] = sys.modules[__name__]
    for name in _SUBMODULES:
        sys.modules[f"ray.{name}"] = importlib.import_module(f"{__name__}.{name}")


_KNOWN_UNSUPPORTED = {
    "timeline": ("`chimaera explain RUN`", ""),
    "DynamicObjectRefGenerator": ("num_returns='streaming' (an ObjectRefGenerator)", ""),
    "java_function": ("Python; Chimaera is Python-only", ""),
    "java_actor_class": ("Python; Chimaera is Python-only", ""),
    "cpp_function": ("Python; Chimaera is Python-only", ""),
    "cross_language": ("Python; Chimaera is Python-only", ""),
    "job_config": ("ch.init(env=...)", ""),
}
_LIBRARIES = ("data", "train", "tune", "serve", "rllib", "workflow", "dag", "experimental", "dashboard", "client")


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(name)
    if name in _KNOWN_UNSUPPORTED:
        use, lands = _KNOWN_UNSUPPORTED[name]
        err = unsupported(f"ray.{name}", use=use, lands=lands)
    elif name in _LIBRARIES:
        err = unsupported(f"ray.{name}", why="this shim covers Ray Core only")
    else:
        err = unsupported(f"ray.{name}", why="it is not part of Ray Core as this shim maps it")
    raise UnsupportedAttribute(str(err))


from . import actor, util  # noqa: E402 -- submodules that use the functions above

__all__ = [
    "ActorClass",
    "ChimaeraCompatWarning",
    "JobID",
    "TaskID",
    "UnsupportedAttribute",
    "ObjectRef",
    "ObjectRefGenerator",
    "RemoteFunction",
    "Unsupported",
    "actor",
    "available_resources",
    "cancel",
    "cluster_resources",
    "get",
    "get_actor",
    "get_gpu_ids",
    "get_runtime_context",
    "init",
    "install",
    "is_initialized",
    "kill",
    "method",
    "nodes",
    "put",
    "remote",
    "shutdown",
    "util",
    "wait",
]
