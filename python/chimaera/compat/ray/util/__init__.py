"""``ray.util`` on Chimaera: ActorPool, collective groups, scheduling strategy
values, and the placement group and queue names."""

from __future__ import annotations

from typing import Any

from .._unsupported import UnsupportedAttribute, unsupported
from . import collective, multiprocessing, queue, scheduling_strategies
from .actor_pool import ActorPool
from .placement_group import (
    get_current_placement_group,
    get_placement_group,
    placement_group,
    placement_group_table,
    remove_placement_group,
)

_ALTERNATIVES = {
    "state": ("ch.cluster() and `chimaera inspect`", "P3"),
    "metrics": ("context().report() and the peer /metrics endpoint", "P4"),
    "inspect_serializability": (
        "submission preflight, which names the captured variable",
        "",
    ),
}


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(name)
    use, lands = _ALTERNATIVES.get(name, ("", ""))
    raise UnsupportedAttribute(
        str(unsupported(f"ray.util.{name}", use=use, lands=lands))
    )


__all__ = [
    "ActorPool",
    "collective",
    "get_current_placement_group",
    "get_placement_group",
    "multiprocessing",
    "placement_group",
    "placement_group_table",
    "queue",
    "remove_placement_group",
    "scheduling_strategies",
]
