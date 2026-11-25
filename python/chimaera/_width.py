"""How many calls of a map can run at once. Until a result's size is known, a
map sends that many items (see Results._room): more would leave results of a
size nobody knows yet piling up behind a slow first item, and fewer would
leave workers idle for a whole item's time."""

from __future__ import annotations

import time
from typing import Any

from ._options import Auto

CAPACITY_EVERY_S = 5.0  # how old the count of the live peers' CPUs and GPUs may be
_capacity: list[Any] = [None, 0.0, 0.0, 0, []]  # the runtime counted for, when, CPUs, GPUs, the live peers


def function_calls(rt: Any, eff: Any) -> int:
    """Calls of a function with options `eff` the live peers can run at once:
    their CPUs over the call's (one when unset, as a peer starts it), and
    their GPUs over the call's if it takes some."""
    cpus, gpus = _live_capacity(rt)
    each = eff.get("cpus")
    if isinstance(each, Auto):
        each = each.initial
    width = cpus / (each if isinstance(each, (int, float)) and each > 0 else 1.0)
    want = eff.get("gpus") or 0
    if want:
        width = min(width, gpus / want)
    return max(1, int(width))


def lane(cls: type[Any] | None, fn: Any, actor_eff: Any) -> int:
    """Calls of method `fn` an actor of `cls` (options `actor_eff`) runs at
    once: its concurrency group's width, its batch lane's, its own
    ``concurrency``, or else the actor's."""
    declared = getattr(fn, "__chimaera_method__", None) or {}
    if "group" in declared:
        return int((getattr(cls, "__chimaera_groups__", None) or {}).get(declared["group"], 1))
    if "batch" in declared:
        return int(declared["batch"].lane)
    own = declared.get("concurrency", actor_eff.get("concurrency"))
    if isinstance(own, Auto):
        own = own.initial
    return own if isinstance(own, int) else 1


def _live_capacity(rt: Any) -> tuple[float, int]:
    """CPUs and GPUs of the live peers that take calls (not draining),
    counted at most CAPACITY_EVERY_S ago."""
    now = time.monotonic()
    if _capacity[0] is not rt or now - _capacity[1] >= CAPACITY_EVERY_S:
        try:
            live = [n for n in rt.client.nodes() if n["alive"] and not n["draining"]]
        except (ConnectionError, RuntimeError):  # the peer could not say: this one alone
            info = rt.info
            live = [
                {
                    "alive": True,
                    "draining": False,
                    "cpus": float(info["usable_cores"]),
                    "gpus": info["gpus"],
                    "memory_bytes": info["memory_bytes"],
                    "object_layout": 0,  # unknown: what every peer reads (see _shuffle.sparse_readable)
                }
            ]
        cpus, gpus = float(sum(n["cpus"] for n in live)), sum(len(n["gpus"]) for n in live)
        _capacity[:] = rt, now, cpus or float(rt.info["usable_cores"]), gpus, live
    return _capacity[2], _capacity[3]


def live_nodes(rt: Any) -> list[dict[str, Any]]:
    """The live peers that take calls, as ``client.nodes()`` lists them,
    from the same count as ``_live_capacity`` (this peer alone, from its own
    info, when it could not say)."""
    _live_capacity(rt)
    return _capacity[4]
