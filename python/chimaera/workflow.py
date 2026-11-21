"""Helpers for durable workflows: ``@ch.remote(workflow="durable")``.

A durable workflow is an ``async def`` remote function whose progress survives
its worker dying. When it is retried, it runs again from the top, and every
call it makes is answered from the run's record if that call already ran:
finished steps are not repeated, and calls still running are joined, not
started twice. So a workflow must make the same calls, in the same order, with
the same arguments each time it runs; if a replay asks for something else at
some step, that step raises ``ch.WorkflowDivergence``.

Anything that differs from run to run (the time, random numbers, reading a
file or an API) goes through these helpers, which run it once and record the
answer for replays.
"""

from __future__ import annotations

import random as _random
import time as _time
from typing import Any, Callable, TypeVar

from . import _context

T = TypeVar("T")


def _in_workflow(name: str) -> None:
    ctx = _context.current_task()
    if ctx is None or not ctx.workflow:
        raise RuntimeError(f"ch.workflow.{name}() works inside a @ch.remote(workflow='durable') function")


def _call(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    return fn(*args, **kwargs)


def record(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run ``fn(*args, **kwargs)`` once as a step of this workflow and return
    its result; a replay gets the recorded result without running it again.

    The step is identified by ``fn`` and its arguments as pickled, so pass a
    module-level function: a bound method or closure carrying state that
    changes between runs looks like a different call on replay."""
    from ._remote import remote

    _in_workflow("record")
    return remote(_call).remote(fn, args, kwargs).result()


def _time_now() -> float:
    return _time.time()


def _random_value() -> float:
    return _random.random()


def now() -> float:
    """The time (``time.time()``) at this step, the same on every replay."""
    _in_workflow("now")
    return record(_time_now)


def random() -> float:
    """A random number in [0, 1) for this step, the same on every replay."""
    _in_workflow("random")
    return record(_random_value)


def sleep(seconds: float) -> None:
    """Wait at least ``seconds`` from the first time this step ran: a replay
    after a crash waits only for what is left."""
    _in_workflow("sleep")
    until = record(_time_now) + seconds
    left = until - _time.time()
    if left > 0:
        _time.sleep(left)
