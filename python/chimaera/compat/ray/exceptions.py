"""Ray's exception classes, and the translation from Chimaera's errors.

An error raised by user code in a task comes back from ``ray.get`` as
``RayTaskError(<original type>)``: one class deriving from both, so
``except ValueError`` and ``except RayTaskError`` both catch it (as in Ray).
"""

from __future__ import annotations

from typing import Any

import chimaera as ch

from ._unsupported import Unsupported


class RayError(Exception):
    """Base class of the errors this shim raises."""


class RayTaskError(RayError):
    """A task raised an exception. Instances also derive from the original type."""

    cause: BaseException | None = None

    def as_instanceof_cause(self) -> RayTaskError:
        return self


class RayActorError(RayError):
    """The actor is not available to run the call."""


class ActorDiedError(RayActorError):
    """The actor's process died or was killed."""


class ActorUnavailableError(RayActorError):
    """The actor is temporarily unreachable."""


class GetTimeoutError(RayError, TimeoutError):
    """``ray.get`` ran out of time; the call keeps running."""


class TaskCancelledError(RayError):
    """The call was cancelled before it produced a result."""


class WorkerCrashedError(RayError):
    """The worker died and the retry budget ran out."""


class OutOfMemoryError(RayError):
    """The call exceeded its memory limit."""


class ObjectLostError(RayError):
    """A value the call needs no longer exists."""


# Ray's kinds of lost value, for code that imports or catches them. A lost
# value comes back as ObjectLostError itself (Chimaera's DataLost): one whose
# owner died is read from its replica instead of raising OwnerDiedError.
class OwnerDiedError(ObjectLostError):
    """The process that owned the value died."""


class ObjectReconstructionFailedError(ObjectLostError):
    """The value could not be made again."""


class ObjectFreedError(ObjectLostError):
    """The value was freed while a reference to it remained."""


class ObjectFetchTimedOutError(ObjectLostError):
    """Fetching the value from another node timed out."""


class ReferenceCountingAssertionError(ObjectLostError, AssertionError):
    """A reference was used after the runtime counted it released."""


class ObjectStoreFullError(RayError):
    """Ray's store-full error. Chimaera's store spills to disk instead of filling."""


class OutOfDiskError(RayError):
    """The disk the values spill to is full."""


class RaySystemError(RayError):
    """An internal failure of the runtime."""


class PendingCallsLimitExceeded(RayError):
    """The target is shedding load."""


# Chimaera error -> Ray error, most specific first.
_INFRA: tuple[tuple[type[BaseException], type[RayError]], ...] = (
    (ch.Cancelled, TaskCancelledError),
    (ch.ActorDied, ActorDiedError),
    (ch.OutOfMemory, OutOfMemoryError),
    (ch.errors.DataLost, ObjectLostError),
    (ch.InfrastructureFailure, WorkerCrashedError),
    (ch.errors.Overloaded, PendingCallsLimitExceeded),
)

_dual: dict[type[BaseException], type[RayTaskError]] = {}


def _task_error(cause: BaseException) -> RayTaskError:
    """``RayTaskError(<type of cause>)``, carrying the cause's args, attributes and notes."""
    base = type(cause)
    cls = _dual.get(base)
    if cls is None:
        try:
            cls = type(f"RayTaskError({base.__name__})", (RayTaskError, base), {"__module__": __name__})
        except TypeError:  # a base whose layout cannot be combined
            cls = RayTaskError
        _dual[base] = cls
    try:
        err = cls.__new__(cls)
        err.args = cause.args
        err.__dict__.update(getattr(cause, "__dict__", {}))
    except Exception:
        err = RayTaskError(str(cause))
    err.cause = cause
    return err


def translate(exc: BaseException) -> BaseException | None:
    """The Ray-shaped error for `exc` raised by a finished call, or None to re-raise it as is."""
    if not isinstance(exc, Exception) or isinstance(exc, (RayError, Unsupported)):
        return None
    for ch_type, ray_type in _INFRA:
        if isinstance(exc, ch_type):
            out: BaseException = ray_type(str(exc))
            break
    else:
        if isinstance(exc, ch.ChimaeraError) and not isinstance(exc, ch.errors.RemoteError):
            return None  # Infeasible, OutcomeUnknown, ...: no Ray equivalent, keep Chimaera's
        out = _task_error(exc)
    have = list(getattr(out, "__notes__", ()))  # a task error already copied the cause's notes
    for note in getattr(exc, "__notes__", ()):
        if note not in have:
            out.add_note(note)
    return out


def reraise(exc: BaseException) -> Any:
    out = translate(exc)
    if out is None:
        raise exc
    raise out from None
