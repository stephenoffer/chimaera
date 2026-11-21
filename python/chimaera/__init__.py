"""Chimaera: one decorator for distributed Python.

    import chimaera as ch

    @ch.remote
    def word_count(text: str) -> int:
        return len(text.split())

    word_count("blue sky")                    # 2, an ordinary local call
    word_count.remote("blue sky").result()    # 2, scheduled by the runtime
"""

from ._context import context
from ._devices import device, devices
from ._gang import Gang, GangActor, GangFailed, Group, GroupHandle, group
from ._reserve import Reservation, reserve
from ._errors import (
    ActorDied,
    Cancelled,
    ChimaeraError,
    Closed,
    CommandConflict,
    DataLost,
    DeadlineExceeded,
    Infeasible,
    InfrastructureFailure,
    LeaseLost,
    NotFound,
    OptionsError,
    OutcomeUnknown,
    OutOfMemory,
    Overloaded,
    PreflightError,
    RemoteError,
    SerializationError,
    StreamInterrupted,
    VersionConflict,
    WorkerCrashed,
    WorkflowDivergence,
)
from ._future import Future, ObjectRef, Outcome, Results, Stream, as_completed, gather, handles, put
from ._batch import Batch
from ._options import Placement, State, auto, options
from ._remote import Actor, ActorClass, Pool, RemoteFunction, actor, method, remote
from ._run import Run, attach, current_run
from ._runtime import Runtime
from . import workflow  # noqa: E402 -- ch.workflow.now/random/sleep/record
from ._coord import Queue, Subscription, Topic  # noqa: E402 -- builds on _remote
from ._store import Store
from ._cluster import cluster
from . import ext  # noqa: E402 -- sources, sinks, serializers
from ._data import Partition, Partitioned, partitions


def submit(fn, /, *args, **kwargs) -> Future:
    """Run an undecorated callable remotely (same contract as ``remote(fn).remote``)."""
    return remote(fn).remote(*args, **kwargs)


def map(fn, /, *sources, ordered: bool = True) -> Results:  # noqa: A001 -- mirrors builtins.map
    """Apply an undecorated callable to every item remotely."""
    f = fn if isinstance(fn, RemoteFunction) else remote(fn)
    return f.map(*sources, ordered=ordered)


def init(
    cluster: str | None = None, *, detach: bool = False, env: dict[str, str] | None = None, tenant: str | None = None
) -> Run:
    """Connect now (normally the first submission does) and return the run.
    A ``Run`` is its id string, so it can be printed, stored and passed to
    ``ch.attach`` from another process.

    `cluster` selects where the work runs: a profile name from
    ``~/.config/chimaera/clusters.toml`` or ``tcp://host:port``. Without it,
    ``$CHIMAERA_CLUSTER``, ``$CHIMAERA_ADDRESS`` or the profile default apply,
    else a local peer starts on demand.

    `env` adds environment variables for this run's executors. By default a run
    ships only locale settings and PYTHONHASHSEED; name more client variables in
    CHIMAERA_ENV_PASSTHROUGH (comma-separated, wildcards allowed). Do not pass
    secrets: the run's variables are stored with the run. Executors get secrets
    from their peer's own environment.

    `tenant` (default ``$CHIMAERA_TENANT``, else "default") is who the run's
    work is charged to: peers with ``CHIMAERA_TENANT_QUOTAS`` hold a tenant's
    calls back while its running calls use its quota of CPUs.
    """
    with Runtime._lock:
        if Runtime._instance is None:
            Runtime._instance = Runtime(detached=detach, env=env, cluster=cluster, tenant=tenant)
        elif env is not None or cluster is not None or tenant is not None:
            raise RuntimeError("ch.init(cluster=..., env=..., tenant=...) must come before the first submission")
    return current_run()


__all__ = [
    "Actor",
    "ActorClass",
    "ActorDied",
    "Batch",
    "Cancelled",
    "ChimaeraError",
    "Closed",
    "CommandConflict",
    "DataLost",
    "DeadlineExceeded",
    "Future",
    "Gang",
    "GangActor",
    "GangFailed",
    "Group",
    "GroupHandle",
    "Infeasible",
    "InfrastructureFailure",
    "LeaseLost",
    "NotFound",
    "ObjectRef",
    "OptionsError",
    "Outcome",
    "OutcomeUnknown",
    "OutOfMemory",
    "Overloaded",
    "Partition",
    "Partitioned",
    "Placement",
    "Reservation",
    "Pool",
    "PreflightError",
    "Queue",
    "RemoteError",
    "RemoteFunction",
    "Results",
    "Run",
    "SerializationError",
    "State",
    "Store",
    "Stream",
    "StreamInterrupted",
    "Subscription",
    "Topic",
    "VersionConflict",
    "WorkerCrashed",
    "WorkflowDivergence",
    "actor",
    "as_completed",
    "handles",
    "attach",
    "auto",
    "cluster",
    "context",
    "current_run",
    "device",
    "devices",
    "ext",
    "gather",
    "group",
    "init",
    "map",
    "method",
    "options",
    "partitions",
    "put",
    "remote",
    "reserve",
    "submit",
    "workflow",
]
