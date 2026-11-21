"""Ray task and actor options, translated to Chimaera options.

One table covers every option Ray accepts. Each entry either translates the
value, accepts it with a one-time warning (CHR001 ignored, CHR002 not yet
enforced, CHR003 different semantics), or raises ``Unsupported`` naming the
Chimaera alternative. An option Ray does not have is a ``ValueError``, as in Ray.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ._unsupported import unsupported, warn_once
import chimaera as ch

from .util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

TASK, ACTOR = "task", "actor"
BOTH = (TASK, ACTOR)

# Ray retries a failed task 3 times by default (4 attempts) and never retries
# actor methods; Chimaera's own defaults differ, so the shim sets Ray's.
_RAY_TASK_ATTEMPTS = 4
_INFINITE_ATTEMPTS = 100
_INFINITE_RESTARTS = 1_000_000

Out = dict[str, Any]


def _ignored(why: str) -> Callable[[str, Any, Out], None]:
    def handle(name: str, value: Any, out: Out) -> None:
        if value not in (None, False, {}, (), []):
            warn_once("CHR001", name, f"ignored: {why}")

    return handle


def _not_yet(what: str, lands: str) -> Callable[[str, Any, Out], None]:
    def handle(name: str, value: Any, out: Out) -> None:
        if value not in (None, {}, (), []):
            raise unsupported(f"{name}={value!r}", use=what, lands=lands)

    return handle


def _num_cpus(name: str, value: Any, out: Out) -> None:
    if value is None:
        return
    # Chimaera always charges a share of a core; 0.01 lets ~100 such calls share one.
    out["cpus"] = 0.01 if value == 0 else float(value)


def _num_gpus(name: str, value: Any, out: Out) -> None:
    if value:
        if float(value) != int(value):
            warn_once("CHR003", name, "a fraction of a GPU takes a whole device until sharing is measured")
        out["gpus"] = float(value)


# Ray's accelerator_type values (ray.util.accelerators) as device selectors.
_RAY_ACCELERATOR_PREFIXES = (
    ("TPU-V5LITEPOD", "tpu:v5e"),
    ("TPU-", None),  # TPU-V4 -> tpu:v4
    ("AMD-Instinct-MI250X", "rocm:MI250"),
    ("AMD-Instinct-", None),  # AMD-Instinct-MI300A -> rocm:MI300A
    ("AMD-Radeon", "rocm"),
    ("aws-neuron-core", "neuron"),
    ("Intel-GAUDI", "hpu"),
    ("Intel-GPU-Max", "xpu"),
    ("Apple-Silicon", "mps"),
)
_NO_PLUGIN = ("Ascend", "MXC", "FURIOSA", "MOBILINT")


def ray_accelerator(value: str) -> str:
    """The ``device=`` selector for one of Ray's accelerator types."""
    for prefix, device in _RAY_ACCELERATOR_PREFIXES:
        if value.startswith(prefix):
            if device is not None:
                return device
            rest = value[len(prefix) :]
            return f"tpu:{rest.lower()}" if prefix == "TPU-" else f"rocm:{rest}"
    if value.startswith(_NO_PLUGIN):
        raise unsupported(f"accelerator_type={value!r}", why="Chimaera has no device plugin for this vendor")
    if value[:2] in ("M1", "M2", "M3", "M4"):
        return f"mps:{value.replace('-', ' ')}"  # Apple M3-Pro -> the model "M3 Pro"
    return f"cuda:{value.split('-')[0] if value.startswith('A100-') else value.replace('-', ' ')}"


def _accelerator_type(name: str, value: Any, out: Out) -> None:
    if value:
        out["device"] = ray_accelerator(str(value))


def _memory(name: str, value: Any, out: Out) -> None:
    if value:
        out["memory"] = int(value)


def _resources(name: str, value: Any, out: Out) -> None:
    if not value:
        return
    reserved = {"CPU", "GPU", "memory"} & set(value)
    if reserved:
        raise ValueError(f"Use the num_cpus, num_gpus and memory options instead of resources={sorted(reserved)}")
    warn_once("CHR002", name, "custom resources are recorded but not yet used for placement (P3)")
    out["resources"] = dict(value)


def _attempts(name: str, value: Any, out: Out) -> None:
    if value is None:
        return
    if value == -1:
        warn_once("CHR003", f"{name}=-1", f"infinite retries become {_INFINITE_ATTEMPTS} attempts")
        out["max_attempts"] = _INFINITE_ATTEMPTS
    elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        out["max_attempts"] = value + 1
    else:
        raise ValueError(f"{name} must be an integer >= -1, got {value!r}")


def _retry_exceptions(name: str, value: Any, out: Out) -> None:
    if value is True:
        out["retry_exceptions"] = (Exception,)
    elif value:
        out["retry_exceptions"] = tuple(value)


def _num_returns(name: str, value: Any, out: Out) -> None:
    if value in (None, 1):
        return
    if value in ("streaming", "dynamic"):
        raise unsupported(f"num_returns={value!r}", use="a generator function (it returns ch.Results)", lands="M1")
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"num_returns must be a positive integer, got {value!r}")
    out["returns"] = value  # .remote() then returns a list of `value` refs


def _scheduling_strategy(name: str, value: Any, out: Out) -> None:
    if value in (None, "DEFAULT"):
        return
    if value == "SPREAD":
        warn_once("CHR002", "scheduling_strategy='SPREAD'", "spread placement is not enforced yet (P3)")
    elif isinstance(value, NodeAffinitySchedulingStrategy):
        # A peer's `node` label is its id, which ray.nodes() reports as NodeID.
        if value.soft:
            _place(out, prefer=[f"node={value.node_id}"])
        else:
            _place(out, require={"node": value.node_id})
    elif isinstance(value, NodeLabelSchedulingStrategy):
        _place(out, require=_exact("NodeLabelSchedulingStrategy(hard=...)", value.hard))
        _place(out, prefer=[f"{k}={v}" for k, v in _exact("NodeLabelSchedulingStrategy(soft=...)", value.soft or {}).items()])
    elif isinstance(value, PlacementGroupSchedulingStrategy):
        if value.placement_group_capture_child_tasks:
            warn_once("CHR002", "placement_group_capture_child_tasks", "child tasks are not placed in the group")
        out["within"] = value.placement_group.bundle(value.placement_group_bundle_index)
    else:
        raise ValueError(f"Invalid scheduling_strategy {value!r}")


def _place(out: Out, require: Mapping[str, str] | None = None, prefer: list[str] | None = None) -> None:
    """Merge into the call's ch.Placement (label_selector and scheduling_strategy may both add to it)."""
    have = out.get("placement") or ch.Placement()
    out["placement"] = ch.Placement(require={**have.require, **(require or {})}, prefer=(*have.prefer, *(prefer or ())))


def _exact(what: str, selector: Mapping[str, Any]) -> dict[str, str]:
    """Label pairs that must match exactly: Chimaera placement has no negation or sets."""
    exact = {}
    for key, match in selector.items():
        if isinstance(match, In) and len(match.values) == 1:
            exact[key] = str(match.values[0])
        elif isinstance(match, str) and not match.startswith(("!", "in(")):
            exact[key] = match
        else:
            raise unsupported(f"{what} with {key}: {match!r}", use="exact label values, ch.Placement(require={key: value})")
    return exact


def _label_selector(name: str, value: Any, out: Out) -> None:
    if value:
        _place(out, require=_exact("label_selector", value))


def _max_concurrency(name: str, value: Any, out: Out) -> None:
    if value is not None:
        out["concurrency"] = int(value)


def _max_restarts(name: str, value: Any, out: Out) -> None:
    if not value:
        return
    out["restart"] = "fresh"
    out["max_restarts"] = _INFINITE_RESTARTS if value == -1 else int(value)


def _lifetime(name: str, value: Any, out: Out) -> None:
    if value == "detached":
        out["lifetime"] = "detached"
    elif value not in (None, "non_detached"):
        raise ValueError(f"lifetime must be 'detached', 'non_detached' or None, got {value!r}")


def _max_pending_calls(name: str, value: Any, out: Out) -> None:
    if value not in (None, -1):
        warn_once("CHR002", name, "the pending-call cap is not enforced; submission is bounded by the runtime")


def _out_of_order(name: str, value: Any, out: Out) -> None:
    if value:
        raise unsupported(name, why="calls through one actor handle always run in submission order")


def _concurrency_groups(name: str, value: Any, out: Out) -> None:
    if value:
        out["groups"] = {str(k): int(v) for k, v in dict(value).items()}


def _identity(name: str, value: Any, out: Out) -> None:
    """name / namespace / get_if_exists: combined into ``key`` by ``actor_key``."""


_HANDLERS: dict[str, tuple[tuple[str, ...], Callable[[str, Any, Out], None]]] = {
    "num_cpus": (BOTH, _num_cpus),
    "num_gpus": (BOTH, _num_gpus),
    "memory": (BOTH, _memory),
    "resources": (BOTH, _resources),
    "accelerator_type": (BOTH, _accelerator_type),
    "label_selector": (BOTH, _label_selector),
    "fallback_strategy": (BOTH, _not_yet("ch.Placement(prefer=...)", "P3")),
    "runtime_env": (BOTH, _not_yet("ray.init(runtime_env={'env_vars': ...}), or ch.Env per call", "P3")),
    "scheduling_strategy": (BOTH, _scheduling_strategy),
    "enable_task_events": (BOTH, _ignored("every call is recorded; see `chimaera explain`")),
    "_metadata": (BOTH, _ignored("not used")),
    "_labels": (BOTH, _ignored("not used")),
    "name": (BOTH, _identity),  # tasks: a display name only
    # Tasks.
    "max_retries": ((TASK,), _attempts),
    "retry_exceptions": ((TASK,), _retry_exceptions),
    "num_returns": ((TASK,), _num_returns),
    "max_calls": ((TASK,), _ignored("workers rotate on measured leaks instead")),
    "_generator_backpressure_num_objects": ((TASK,), _not_yet("max_in_flight", "M1")),
    # Actors.
    "max_concurrency": ((ACTOR,), _max_concurrency),
    "max_restarts": ((ACTOR,), _max_restarts),
    "max_task_retries": ((ACTOR,), _attempts),
    "lifetime": ((ACTOR,), _lifetime),
    "namespace": ((ACTOR,), _identity),
    "get_if_exists": ((ACTOR,), _identity),
    "concurrency_groups": ((ACTOR,), _concurrency_groups),
    "max_pending_calls": ((ACTOR,), _max_pending_calls),
    "allow_out_of_order_execution": ((ACTOR,), _out_of_order),
}


def translate(options: Mapping[str, Any], target: str) -> Out:
    """Chimaera options for Ray `options` on a task or actor. Identity options
    (name, namespace, get_if_exists) are left to ``actor_key``."""
    out: Out = {}
    for name, value in options.items():
        entry = _HANDLERS.get(name)
        if entry is None or target not in entry[0]:
            what = "remote functions" if target == TASK else "actors"
            raise ValueError(f"Invalid option keyword {name} for {what}.")
        entry[1](name, value, out)
    if target == TASK:
        out.setdefault("max_attempts", _RAY_TASK_ATTEMPTS)
        if options.get("name"):
            warn_once("CHR001", "name", "ignored: task display names are not used; calls are identified by id")
    else:
        out.setdefault("max_attempts", 1)  # Ray's max_task_retries=0
    return out


def actor_key(options: Mapping[str, Any], default_namespace: Callable[[], str]) -> str | None:
    """``key=`` for a named actor: ``<namespace>/<name>``. Without a namespace the
    name is scoped to this run, as Ray scopes it to the job's anonymous namespace."""
    name = options.get("name")
    if not name:
        return None
    if not options.get("get_if_exists"):
        warn_once(
            "CHR003",
            "get_if_exists=False",
            "a named actor is get-or-create, so an existing actor is returned instead of an error",
        )
    return f"{options.get('namespace') or default_namespace()}/{name}"
