"""Exceptions raised by Chimaera and reconstruction of remote errors.

A remote exception is re-raised with its original type when it can be
unpickled, with the remote traceback and context attached as a note. Otherwise
it becomes a :class:`RemoteError` that carries the same information.
"""

from __future__ import annotations

import json
import pickle
from typing import Any


class ChimaeraError(Exception):
    """Base class of every error raised by Chimaera itself."""


class RemoteError(ChimaeraError):
    """A remote exception whose original type could not be rebuilt here."""

    def __init__(self, type_name: str, message: str, remote_traceback: str = "") -> None:
        super().__init__(f"{type_name}: {message}" if type_name else message)
        self.type_name = type_name
        self.message = message
        self.remote_traceback = remote_traceback


class NonRetryable(Exception):
    """Raise it, or an exception class that also derives from it, for a failure
    that retrying cannot fix (bad input, a permission refused). Such an error is
    never retried, whatever the call's ``retry_exceptions`` says."""


# Retried in calls that may run again when retry_exceptions is unset: a dropped
# connection or a timeout reaching another service. Named, not imported, so the
# libraries need not be installed; subclasses match.
TRANSIENT_ERRORS = (
    "builtins.ConnectionError",
    "builtins.TimeoutError",
    "requests.exceptions.ConnectionError",
    "requests.exceptions.Timeout",
    "aiohttp.client_exceptions.ClientConnectionError",
    "urllib3.exceptions.ProtocolError",
)


class OutcomeUnknown(ChimaeraError):
    """A call with external effects may or may not have taken effect. From a
    durable workflow's activity (``ch.durable.record(activity, ...)``), ``key``
    is the activity's key: ask the other system what it did under that key."""

    key: str | None = None


class Cancelled(ChimaeraError):
    """The call was cancelled before it produced a result."""


class DeadlineExceeded(Cancelled):
    """The call's deadline passed. Distinct from a wait timeout (TimeoutError)."""


class BudgetExceeded(Cancelled):
    """The call's budget, or that of the call it runs under, was used up: the
    budgeted call and every call it made were cancelled. The message names the
    budget and what was used."""


class Infeasible(ChimaeraError):
    """No peer can ever satisfy the request; the message says which constraint."""


class InfrastructureFailure(ChimaeraError):
    """Attempts were lost to worker or peer failures until the retry budget ran out."""


class QuorumLost(InfrastructureFailure):
    """A group of the cluster lost the majority of its peers, so it accepts no
    writes: calls whose state lives there cannot be submitted until enough of
    those peers come back. The message names the group."""


class WorkerCrashed(InfrastructureFailure):
    """The worker died on this input repeatedly, on healthy peers."""


class OutOfMemory(InfrastructureFailure):
    """The call exceeded its memory limit after the escalation budget."""


class OutOfDisk(ChimaeraError, OSError):
    """A value could not be stored because the disk holding the objects is
    full. It is an ``OSError`` with ``errno.ENOSPC``, so code catching either
    keeps working; the message names the directory, its free space and the
    value's size."""


class ActorDied(InfrastructureFailure):
    """The actor's process died; started calls are not replayed."""


class StreamInterrupted(InfrastructureFailure):
    """A result stream ended early; ``delivered`` items were received."""


class DataLost(InfrastructureFailure):
    """A value this call needs no longer exists and cannot be recomputed."""


class NotFound(ChimaeraError, LookupError):
    """Nothing exists under that name (an actor key, a queue)."""


class Closed(ChimaeraError):
    """The queue or topic was closed."""


class VersionConflict(ChimaeraError):
    """A store write's ``if_version`` did not match; ``version`` is the key's current one (0: absent)."""

    def __init__(self, message: str, version: int = 0) -> None:
        super().__init__(message)
        self.version = version


class LeaseLost(ChimaeraError):
    """A lock's lease expired or passed to another holder; writes fenced by it are refused."""


class OptionsError(ChimaeraError, ValueError, TypeError):
    """Options conflict or are invalid; the message names each field and its source."""


class PreflightError(ChimaeraError, TypeError):
    """A call cannot be sent: it fails a check before submission."""


class SerializationError(PreflightError):
    """A captured value or argument cannot be serialized."""


class WorkflowDivergence(ChimaeraError):
    """A durable workflow made different calls on replay than in its history.

    ``step`` is the step's number (the workflow's nth call). ``recorded`` is
    the call its history holds at that step and ``received`` the one the replay
    made, each a dict of ``function``, ``site`` (the workflow's file:line that
    made it), ``args`` (a shortened repr) and ``fingerprint`` (hex), or None
    for a call recorded without them."""

    step: int | None = None
    recorded: dict[str, Any] | None = None
    received: dict[str, Any] | None = None


def divergence(reason: str) -> WorkflowDivergence:
    """The error for a replayed step the shard rejected: its reason holds the
    recorded and received calls as JSON, ``[recorded, received]``. It runs on
    the thread that settles every future, so it never raises: a reason of
    another shape (text, or a call another version described otherwise) makes
    a WorkflowDivergence that carries it as it came."""
    try:
        recorded, received = json.loads(reason.partition(": ")[2])
        if not all(c is None or isinstance(c, dict) for c in (recorded, received)):
            raise ValueError(reason)
    except Exception:
        return WorkflowDivergence(reason)

    def call(c: dict[str, Any] | None) -> str:
        if not c:
            return "a call described by an older version of Chimaera"
        fingerprint = str(c.get("fingerprint", ""))[:16]
        return f"{c.get('function', '?')}{c.get('args', '')} at {c.get('site') or '?'} (fingerprint {fingerprint})"

    step = (received or recorded or {}).get("step")
    exc = WorkflowDivergence(
        f"{f'step {step}' if step else 'a step'} of this durable workflow made a different call than its history "
        "records:\n"
        f"  history: {call(recorded)}\n"
        f"  replay:  {call(received)}\n"
        "A workflow must make the same calls in the same order every time it runs: get anything that varies "
        "(the time, random numbers, reads) through ch.durable.now(), random() or record()."
    )
    exc.step, exc.recorded, exc.received = step, recorded, received
    return exc


class CommandConflict(ChimaeraError):
    """A command id was reused with a different request."""


class Overloaded(ChimaeraError):
    """The target is shedding load; retry after ``retry_after`` seconds."""

    def __init__(self, message: str, retry_after: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


_BY_KIND: dict[str, type[ChimaeraError]] = {
    "infra": InfrastructureFailure,
    "oom": OutOfMemory,
    "cancelled": Cancelled,
    "deadline": DeadlineExceeded,
    "budget": BudgetExceeded,
    "outcome_unknown": OutcomeUnknown,
    "infeasible": Infeasible,
    "worker_crashed": WorkerCrashed,
    "data_lost": DataLost,
    "actor_died": ActorDied,
    "command_conflict": CommandConflict,
    "overloaded": Overloaded,
}

_NEVER_RERAISE = (SystemExit, KeyboardInterrupt, GeneratorExit)


def from_outcome(err: tuple[Any, ...], task: str = "") -> BaseException:
    """Build the exception for an error outcome tuple from the native layer:
    ("err", kind, type_name, message, exc_pickle, traceback, upstream, attempt, detail_json).
    """
    _, kind, type_name, message, exc_pickle, tb, upstream, attempt, detail = err
    context = f"attempt {attempt}" + (f" of task {task}" if task else "")
    if kind in _BY_KIND:
        exc: BaseException = _BY_KIND[kind](message)
        if detail:
            try:
                exc.details = json.loads(detail)  # type: ignore[attr-defined]
            except ValueError:
                pass
        if tb:
            exc.add_note(f"remote traceback ({context}):\n{tb}")
        return exc
    rebuilt = _rebuild(exc_pickle)
    runtime = _BY_KIND.get(type_name.removeprefix("chimaera:").lower()) if type_name.startswith("chimaera:") else None
    if runtime is not None and rebuilt is None:
        rebuilt = runtime(message)  # a prerequisite failed with a runtime error: the same type here
    elif rebuilt is None or isinstance(rebuilt, _NEVER_RERAISE):
        rebuilt = RemoteError(type_name, message, tb)
    if kind == "causal" and upstream:
        rebuilt.add_note(f"raised by prerequisite task {bytes(upstream[16:]).hex()}; this call never ran")
    if tb:
        rebuilt.add_note(f"remote traceback ({context}):\n{tb.rstrip()}")
    return rebuilt


def _rebuild(blob: bytes) -> BaseException | None:
    if not blob:
        return None
    try:
        exc = pickle.loads(blob)
    except Exception:
        return None
    return exc if isinstance(exc, BaseException) else None
