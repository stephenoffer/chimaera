"""Chimaera: one decorator for distributed Python.

    import chimaera as ch

    @ch.remote
    def word_count(text: str) -> int:
        return len(text.split())

    word_count("blue sky")                    # 2, an ordinary local call
    word_count.remote("blue sky").result()    # 2, scheduled by the runtime

The top level is what most programs need. The rest is in submodules:
``chimaera.coord`` (queues, topics, a replicated store), ``chimaera.dist``
(gangs, reservations, placement, devices), ``chimaera.data`` (partitions),
``chimaera.runs`` (runs and the cluster), ``chimaera.durable`` (durable state
and workflows), ``chimaera.errors`` (every error), ``chimaera.typing``
(types for annotations) and ``chimaera.ext`` (sources, sinks, serializers).
"""

from ._batch import Batch
from ._context import context
from ._errors import (
    ActorDied,
    Cancelled,
    ChimaeraError,
    DeadlineExceeded,
    Infeasible,
    InfrastructureFailure,
    NotFound,
    OptionsError,
    OutcomeUnknown,
    OutOfMemory,
    SerializationError,
)
from ._future import Future, Results, as_completed, get, handles, put, wait
from ._options import auto, defaults
from ._remote import Executor, actor, gather, method, remote
from ._run import Run as _Run, current_run as _current_run
from ._runtime import Runtime as _Runtime
from . import coord, data, dist, durable, errors, ext, runs, typing  # noqa: E402 -- build on the above


def init(
    cluster: str | None = None, *, detach: bool = False, env: dict[str, str] | None = None, tenant: str | None = None
) -> _Run:
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
    with _Runtime._lock:
        if _Runtime._instance is None:
            _Runtime._instance = _Runtime(detached=detach, env=env, cluster=cluster, tenant=tenant)
        elif env is not None or cluster is not None or tenant is not None:
            raise RuntimeError(
                "ch.init(cluster=..., env=..., tenant=...) must come before the first call; "
                "call ch.shutdown() first to connect again with other settings"
            )
    return _current_run()


def shutdown(*, cancel: bool = False) -> None:
    """End this script's run now, as exit would: wait for the calls and maps
    it has not waited on (``cancel=True``: cancel them), finish the run and
    disconnect. The next call, or ``ch.init(...)``, connects again, to any
    cluster: a notebook or a test suite can switch clusters this way."""
    with _Runtime._lock:
        rt, _Runtime._instance = _Runtime._instance, None
    if rt is None:
        return
    if rt.in_executor:
        _Runtime._instance = rt
        raise RuntimeError("ch.shutdown() ends a script's run; a call cannot end the run it belongs to")
    rt.shutdown(cancel)


__all__ = [
    "ActorDied",
    "Batch",
    "Cancelled",
    "ChimaeraError",
    "DeadlineExceeded",
    "Executor",
    "Future",
    "Infeasible",
    "InfrastructureFailure",
    "NotFound",
    "OptionsError",
    "OutOfMemory",
    "OutcomeUnknown",
    "Results",
    "SerializationError",
    "actor",
    "as_completed",
    "auto",
    "context",
    "coord",
    "data",
    "dist",
    "durable",
    "errors",
    "ext",
    "gather",
    "get",
    "handles",
    "init",
    "method",
    "defaults",
    "put",
    "remote",
    "runs",
    "shutdown",
    "typing",
    "wait",
]
