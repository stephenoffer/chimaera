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
    "state": ("ch.runs.cluster() and `chimaera inspect`", "P3"),
    "metrics": ("context().report() and the peer /metrics endpoint", "P4"),
    "inspect_serializability": (
        "submission preflight, which names the captured variable",
        "",
    ),
}


def list_named_actors(all_namespaces: bool = False) -> list[Any]:
    """Live named actors: names in this job's namespace, or with
    ``all_namespaces=True`` a ``{"name", "namespace"}`` dict each, as in Ray."""
    import chimaera as ch

    from .._remote import _default_namespace

    if not all_namespaces:
        prefix = f"{_default_namespace()}/"
        return [a.name[len(prefix) :] for a in ch.runs.actors(prefix)]
    out = []
    for a in ch.runs.actors():
        namespace, _, name = a.name.partition("/")
        out.append({"name": name, "namespace": namespace})
    return out


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
    "list_named_actors",
    "multiprocessing",
    "placement_group",
    "placement_group_table",
    "queue",
    "remove_placement_group",
    "scheduling_strategies",
]
