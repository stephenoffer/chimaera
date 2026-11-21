"""Chimaera: one decorator for distributed Python.

    import chimaera as ch

    @ch.remote
    def word_count(text: str) -> int:
        return len(text.split())

    word_count("blue sky")                    # 2, an ordinary local call
    word_count.remote("blue sky").result()    # 2, scheduled by the runtime
"""

from ._context import context
from ._errors import (
    ActorDied,
    Cancelled,
    ChimaeraError,
    CommandConflict,
    DataLost,
    DeadlineExceeded,
    Infeasible,
    InfrastructureFailure,
    OptionsError,
    OutcomeUnknown,
    OutOfMemory,
    Overloaded,
    PreflightError,
    RemoteError,
    SerializationError,
    StreamInterrupted,
    WorkerCrashed,
    WorkflowDivergence,
)
from ._future import Future, ObjectRef, Outcome, Results, as_completed, gather, put
from ._options import Placement, auto, options
from ._remote import Actor, ActorClass, Pool, RemoteFunction, method, remote
from ._run import Run, attach, current_run
from ._runtime import Runtime


def submit(fn, /, *args, **kwargs) -> Future:
    """Run an undecorated callable remotely (same contract as ``remote(fn).remote``)."""
    return remote(fn).remote(*args, **kwargs)


def map(fn, /, *sources, ordered: bool = True) -> Results:  # noqa: A001 -- mirrors builtins.map
    """Apply an undecorated callable to every item remotely."""
    f = fn if isinstance(fn, RemoteFunction) else remote(fn)
    return f.map(*sources, ordered=ordered)


def init(*, detach: bool = False, env: dict[str, str] | None = None) -> str:
    """Connect now (normally the first submission does) and return the run id.

    `env` adds environment variables for this run's executors. By default a run
    ships only locale settings and PYTHONHASHSEED; name more client variables in
    CHIMAERA_ENV_PASSTHROUGH (comma-separated, wildcards allowed). Do not pass
    secrets: the run's variables are stored with the run. Executors get secrets
    from their peer's own environment.
    """
    with Runtime._lock:
        if Runtime._instance is None:
            Runtime._instance = Runtime(detached=detach, env=env)
        elif env is not None:
            raise RuntimeError("ch.init(env=...) must come before the first submission")
    return Runtime._instance.run_id


__all__ = [
    "Actor",
    "ActorClass",
    "ActorDied",
    "Cancelled",
    "ChimaeraError",
    "CommandConflict",
    "DataLost",
    "DeadlineExceeded",
    "Future",
    "Infeasible",
    "InfrastructureFailure",
    "ObjectRef",
    "OptionsError",
    "OutOfMemory",
    "Outcome",
    "OutcomeUnknown",
    "Overloaded",
    "Placement",
    "Pool",
    "PreflightError",
    "RemoteError",
    "RemoteFunction",
    "Run",
    "Results",
    "SerializationError",
    "StreamInterrupted",
    "WorkerCrashed",
    "WorkflowDivergence",
    "as_completed",
    "attach",
    "current_run",
    "auto",
    "context",
    "gather",
    "init",
    "map",
    "method",
    "options",
    "put",
    "remote",
    "submit",
]
