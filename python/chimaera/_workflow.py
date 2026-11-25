"""Helpers for durable workflows: ``@ch.remote(workflow="durable")``, used as
``await ch.durable.now()`` and so on (see ``chimaera.durable``).

A durable workflow is an ``async def`` remote function whose progress survives
its worker dying. When it is retried, it runs again from the top, and every
call it makes is answered from the run's record if that call already ran:
finished steps are not repeated, and calls still running are joined, not
started twice. So a workflow must make the same calls, in the same order, with
the same arguments each time it runs; if a replay asks for something else at
some step, that step raises ``ch.errors.WorkflowDivergence``.

Anything that differs from run to run (the time, random numbers, reading a
file or an API) goes through these helpers, which run it once and record the
answer for replays. A write to another system goes through an ``Activity``,
which runs under a key the run keeps and says what is safe to repeat.

A retried workflow keeps the code it started with, which travels with the
call, unless the definition now deployed under its name declares the
history's ``version`` among those it is ``compatible`` with.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import importlib
import importlib.util
import io
import json
import pickle
import random as _random
import reprlib
import sys
import time as _time
import types
from typing import Any, Callable, Coroutine, Generic, Literal, TypeVar, overload

from . import _context, _serde
from ._errors import OutcomeUnknown
from ._native import blake3_digest, child_id

T = TypeVar("T")
R = TypeVar("R")

# Runs of one activity, the first included, while its steps end in
# ch.OutcomeUnknown: a repeat follows a status lookup that found nothing, or,
# for an idempotent activity, an OutcomeUnknown its run raised (the shard
# reruns an idempotent attempt that was lost without one).
ACTIVITY_RUNS = 3


class Activity(Generic[R]):
    """An operation on another system (a charge, an email, a write to a
    service) run as a step of a durable workflow:
    ``await ch.durable.record(activity, *args, key=...)`` returns the receipt
    of ``run(key, *args, **kwargs)``, recorded for replays.

    The key names the operation to the other system: pass ``key=`` (an order
    id), or the run gives the step one that is the same on every replay.
    Subclass, implement ``run`` and say what is safe to repeat:

    - ``idempotent = True``: ``run`` with a key already done does nothing new
      and returns the first receipt, so a lost attempt simply runs again.
    - ``status(key)``: the receipt of what was done under ``key``, or None if
      nothing was and nothing will be. An attempt lost after it started is
      settled by asking; None runs it again.
    - Neither: an attempt lost after it started raises ``ch.OutcomeUnknown``
      carrying ``key``, since only the other system knows whether it happened.

    ``compensate(key, receipt)`` undoes an operation, through
    ``await ch.durable.compensate(activity, key, receipt)``. The activity is
    pickled into each step, so keep its state small (a URL, a path)."""

    idempotent: bool = False

    def run(self, key: str, *args: Any, **kwargs: Any) -> R:
        raise NotImplementedError

    def status(self, key: str) -> R | None:
        raise NotImplementedError

    def compensate(self, key: str, receipt: R) -> None:
        raise NotImplementedError


def _has(activity: Activity[Any], method: str) -> bool:
    return getattr(type(activity), method) is not getattr(Activity, method)


def _in_workflow(name: str) -> _context.TaskContext:
    ctx = _context.current_task()
    if ctx is None or not ctx.workflow:
        raise RuntimeError(f"ch.durable.{name}() works inside a @ch.remote(workflow='durable') function")
    return ctx


def _site() -> str:
    """file:line of the workflow's line that called into these helpers. Read
    when a helper is called, not when its coroutine runs: under
    asyncio.gather or a task, asyncio's loop runs it."""
    f = sys._getframe(1)
    while f.f_back is not None and f.f_code.co_filename == _site.__code__.co_filename:
        f = f.f_back
    return f"{f.f_code.co_filename}:{f.f_lineno}"


def _call(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    return fn(*args, **kwargs)


async def _step(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    effects: Literal["none", "idempotent", "external"],
    site: str,
) -> Any:
    """Run ``fn(*args, **kwargs)`` as one call of this workflow, made at
    `site` and shown as `fn`'s call if a replay diverges there."""
    from ._remote import CALL_FUNCTION, _fn_name, remote, submit_call
    from ._runtime import Runtime

    view = remote(_call) if effects == "none" else remote(_call).options(effects=effects)
    shown = (_fn_name(fn, ""), args, kwargs)
    eff = view.policy()
    return await submit_call(
        Runtime.get(), _call, (fn, args, kwargs), {}, kind=CALL_FUNCTION, eff=eff, site=site, shown=shown
    )


def _unknown(e: OutcomeUnknown, activity: Activity[Any], key: str) -> OutcomeUnknown:
    e.key = key
    e.add_note(f"{type(activity).__qualname__} key {key!r}: ask that system whether it happened under this key")
    return e


@overload
def record(fn: Activity[R], /, *args: Any, key: str | None = None, **kwargs: Any) -> Coroutine[Any, Any, R]: ...
@overload
def record(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, T]: ...
def record(fn: Any, /, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, Any]:
    """Run ``fn(*args, **kwargs)`` once as a step of this workflow and return
    its result; a replay gets the recorded result without running it again.

    A function is for reads and computations: an attempt lost before its
    result is recorded runs it again. A write to another system is an
    ``Activity``: ``record(activity, *args, key=None, **kwargs)`` returns the
    receipt of ``activity.run(key, *args, **kwargs)``, and an attempt lost
    after it started is run again, looked up or raised as
    ``ch.OutcomeUnknown`` as the activity declares (see ``Activity``).

    The step is identified by ``fn``'s name and its arguments (and a bound
    method's or closure's state), so pass a module-level function: state that
    changes between runs looks like a different call on replay."""
    ctx = _in_workflow("record")
    if not isinstance(fn, Activity):
        return _step(fn, args, kwargs, "none", _site())
    key = kwargs.pop("key", None)
    if key is not None and not isinstance(key, str):
        raise TypeError(f"record(): an activity's key is a str, not {type(key).__name__}")
    return _activity(ctx, fn, args, kwargs, key, _site())


async def _activity(
    ctx: _context.TaskContext,
    fn: Activity[R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    key: str | None,
    site: str,
) -> Any:
    if key is None:
        key = f"{ctx.run_id}-{ctx.task_id}-{ctx._children + 1}"  # the workflow's call count here: replays agree
    effects: Literal["idempotent", "external"] = "idempotent" if fn.idempotent else "external"
    for _ in range(ACTIVITY_RUNS):
        try:
            return await _step(fn.run, (key, *args), kwargs, effects, site)
        except OutcomeUnknown as e:
            unknown = e
        if fn.idempotent:
            continue  # run raised it (the shard itself reruns a lost idempotent attempt): the same key is safe
        if not _has(fn, "status"):
            break
        receipt = await _step(fn.status, (key,), {}, "none", site)
        if receipt is not None:
            return receipt
    raise _unknown(unknown, fn, key)


def compensate(activity: Activity[R], key: str, receipt: R) -> Coroutine[Any, Any, None]:
    """Undo an activity's operation: run ``activity.compensate(key, receipt)``
    once as a step of this workflow, under the activity's contract (an
    idempotent one runs again if lost; any other raises ``ch.OutcomeUnknown``
    with ``key`` if lost after it started)."""
    _in_workflow("compensate")
    if not _has(activity, "compensate"):
        raise TypeError(f"{type(activity).__qualname__} defines no compensate(key, receipt)")
    return _compensate(activity, key, receipt, _site())


async def _compensate(activity: Activity[R], key: str, receipt: R, site: str) -> None:
    effects: Literal["idempotent", "external"] = "idempotent" if activity.idempotent else "external"
    try:
        await _step(activity.compensate, (key, receipt), {}, effects, site)
    except OutcomeUnknown as e:
        raise _unknown(e, activity, key)


def _time_now() -> float:
    return _time.time()


def _random_value() -> float:
    return _random.random()


def now() -> Coroutine[Any, Any, float]:
    """The time (``time.time()``) at this step, the same on every replay."""
    _in_workflow("now")
    return record(_time_now)


def random() -> Coroutine[Any, Any, float]:
    """A random number in [0, 1) for this step, the same on every replay."""
    _in_workflow("random")
    return record(_random_value)


def sleep(seconds: float) -> Coroutine[Any, Any, None]:
    """Wait at least ``seconds`` from the first time this step ran: a replay
    after a crash waits only for what is left."""
    _in_workflow("sleep")
    return _sleep(record(_time_now), seconds)


async def _sleep(started: Coroutine[Any, Any, float], seconds: float) -> None:
    left = await started + seconds - _time.time()
    if left > 0:
        await asyncio.sleep(left)


# ---- replays: fingerprints and versions ------------------------------------


class _Preview(reprlib.Repr):
    """Short reprs for describing a step: bounded, never all of a large bytes
    value, and a placeholder for a __repr__ that fails."""

    def __init__(self) -> None:
        super().__init__()
        self.maxlevel, self.maxstring, self.maxother = 2, 60, 60

    def repr_bytes(self, x: bytes, level: int) -> str:
        return f"<{len(x)} bytes>"

    def repr_bytearray(self, x: bytearray, level: int) -> str:
        return f"<bytearray of {len(x)} bytes>"


_PREVIEW = _Preview()
_PREVIEW_MAX = 200


class _Identity(pickle.Pickler):
    """Pickles a call as what it asks for, the same bytes in any process: a
    function or class by its name (a function's defaults and closure by value,
    and a nested one's code), and no memo, so neither which equal values are
    one object nor how this process came by its classes shows. Never loaded."""

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, type) and obj.__module__ != "builtins":  # a builtin pickles by name already
            return str, (f"{obj.__module__}.{obj.__qualname__}",)
        if isinstance(obj, types.FunctionType):
            name = f"{obj.__module__}.{obj.__qualname__}"
            code = obj.__code__ if "<" in obj.__qualname__ else None  # a lambda or nested def
            cells = tuple(c.cell_contents for c in obj.__closure__ or ())  # an empty or cyclic one: as sent
            return tuple, ((name, code, cells, obj.__defaults__, obj.__kwdefaults__),)
        if isinstance(obj, types.CodeType):
            # What it does, not where it is: bytecode, constants (a set
            # literal's in an order the hash seed does not pick) and names.
            consts = tuple(tuple(sorted(c, key=repr)) if isinstance(c, frozenset) else c for c in obj.co_consts)
            return tuple, ((obj.co_code, consts, obj.co_names),)
        return NotImplemented


def _fingerprint(call: Any, spec: dict[str, Any], packed: dict[str, Any]) -> bytes:
    """A call's fingerprint: its identity pickle, large buffers by digest. A
    call only cloudpickle can send (or a cyclic one) is known by what is sent."""
    out, buffers = io.BytesIO(), []
    p = _Identity(out, protocol=5, buffer_callback=lambda b: buffers.append(hashlib.blake2b(b.raw()).digest()))
    p.fast = True
    try:
        p.dump(call)
    except Exception:
        blobs = b"".join(object_id for object_id, _ in packed["blobs"])
        return blake3_digest(spec["fn_digest"] + b"\0" + (packed.get("args_inline") or b"") + blobs)
    return blake3_digest(out.getvalue() + b"".join(buffers))


def step(
    index: int,
    call: tuple[Any, str, Any],
    spec: dict[str, Any],
    packed: dict[str, Any],
    site: str | None,
    shown: tuple[str, tuple[Any, ...], dict[str, Any]],
) -> tuple[bytes, str]:
    """A workflow call's fingerprint, which a replay's call at the same step
    must match, and the call's description (JSON) for when it does not.

    `call` is (target, method, frame). Functions and classes count by name,
    so a new version of a step's code is the same step."""
    fingerprint = _fingerprint(call, spec, packed)
    name, args, kwargs = shown
    shown_args = [*map(_PREVIEW.repr, args), *(f"{k}={_PREVIEW.repr(v)}" for k, v in kwargs.items())]
    preview = "(" + ", ".join(shown_args) + ")"
    if len(preview) > _PREVIEW_MAX:
        preview = preview[: _PREVIEW_MAX - 4] + "...)"
    described = {"step": index, "function": name, "site": site or "", "args": preview, "fingerprint": fingerprint.hex()}
    return fingerprint, json.dumps(described)


def definition(fn: Any, attempt: int) -> Any:
    """The workflow code an attempt runs: the code of the version its history
    is at. A history starts with ``fn``, the code the call was submitted with,
    which travels with it. A replay moves it to the definition deployed now
    under fn's name when that one is another version and declares the
    history's ``compatible``. Each replay records in the run what it ran, so
    a later one continues from there, even once that code is gone from disk.
    The executor calls it before it marks the attempt a workflow, so these
    records are not steps of the workflow."""
    if attempt < 2:
        return fn
    if isinstance(fn, functools.partial):  # bound arguments: its function's version, with them
        return functools.partial(definition(fn.func, attempt), *fn.args, **fn.keywords)
    if not isinstance(fn, types.FunctionType) or fn.__module__ == "__main__":
        return fn
    ctx = _context.current_task()
    earlier = [f.result() for f in [_ran(ctx, k, None) for k in range(2, attempt)]]
    moves = [blob for blob in earlier if blob is not None]
    code = _serde.load_code(moves[-1]) if moves else fn
    version = getattr(code, "__chimaera_workflow_version__", (1, ()))[0]
    deployed, module = _deployed(fn)
    new_version, compatible = getattr(deployed, "__chimaera_workflow_version__", (None, ()))
    moved = _serde.dump_code(deployed, names=module) if new_version != version and version in compatible else None
    blob = _ran(ctx, attempt, moved).result()
    if blob is None:
        return code
    return deployed if blob == moved else _serde.load_code(blob)


def _keep(value: Any) -> Any:
    return value


def _ran(ctx: Any, attempt: int, moved: bytes | None) -> Any:
    """The record of what attempt `attempt` of this workflow ran: None for
    the code the attempt before it ran, or the code (by value) it moved to.
    Kept under a key of its own, not a step; the first proposal is recorded
    and asking again gets that, whatever it proposes."""
    from ._remote import CALL_FUNCTION, remote, submit_call
    from ._runtime import Runtime

    key = ctx.run + child_id(ctx.task, [ctx.run, b"workflow version", attempt.to_bytes(8, "little")])
    fut = submit_call(Runtime.get(), _keep, (moved,), {}, kind=CALL_FUNCTION, eff=remote(_keep).policy(), key=key)
    fut._escaped = True  # every later attempt reads it again
    return fut


def _deployed(fn: Any) -> tuple[Any, Any]:
    """The definition deployed now under fn's name and the module it is in,
    or (None, None). A process that imported the module may hold an older
    deployment, and the file's modification time cannot tell (rsync -a and
    cp -p keep an older one), so its source is run again apart from
    sys.modules. The loaded definition is kept when it is the same version:
    its classes are the ones this process's values were made with."""

    def find(module: Any) -> Any:
        for part in fn.__qualname__.split("."):
            module = getattr(module, part)
        return getattr(module, "_fn", module)  # the function under @ch.remote

    try:
        loaded = sys.modules.get(fn.__module__)
        if loaded is None:
            module = importlib.import_module(fn.__module__)
            return find(module), module
        spec: Any = loaded.__spec__  # None for a module made in memory: nothing deployed to read
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        now = find(module)
    except Exception:  # not importable here (or broken): the history's own code
        return None, None
    try:
        then = find(loaded)
    except AttributeError:
        return now, module
    version = "__chimaera_workflow_version__"
    return (then if getattr(then, version, None) == getattr(now, version, None) else now), module
