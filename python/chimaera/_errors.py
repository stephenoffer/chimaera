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


class OutcomeUnknown(ChimaeraError):
    """A call with external effects may or may not have taken effect."""


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
    """A durable workflow made different calls on replay than in its history."""


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
