"""``ray.util.placement_group`` over ``ch.reserve``: bundles held on peers,
and ``PlacementGroupSchedulingStrategy(pg, i)`` runs a task or actor within
bundle ``i``. Named placement groups are found by name in this process."""

from __future__ import annotations

import itertools
from typing import Any

import chimaera as ch

from .._unsupported import unsupported

_STRATEGIES = {"PACK": "pack", "SPREAD": "spread", "STRICT_PACK": "strict_pack", "STRICT_SPREAD": "strict_spread"}
_named: dict[str, PlacementGroup] = {}


def _bundle(spec: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"cpus": float(spec.get("CPU", 0)) or 0.01}
    if spec.get("GPU"):
        out["gpus"] = float(spec["GPU"])
    if spec.get("memory"):
        out["memory"] = int(spec["memory"])
    custom = {k: float(v) for k, v in spec.items() if k not in ("CPU", "GPU", "memory", "object_store_memory")}
    if custom:
        out["resources"] = custom
    return out


class PlacementGroup:
    def __init__(self, reservation: ch.Reservation, bundle_specs: list[dict[str, Any]], name: str) -> None:
        self._res, self.bundle_specs, self.name = reservation, bundle_specs, name
        self.id = f"pg-{id(self):x}"
        self._next = itertools.count()

    @property
    def bundle_count(self) -> int:
        return len(self.bundle_specs)

    def ready(self) -> Any:
        """A reference that resolves once every bundle is held: ``ray.get(pg.ready())``."""
        return self._res.ready_future()

    def wait(self, timeout_seconds: float = 30) -> bool:
        try:
            self._res.ready_future().result(timeout=timeout_seconds)
        except TimeoutError:
            return False
        return True

    def bundle(self, index: int) -> Any:
        """``index`` -1 picks the bundles in turn, as Ray's "any bundle" does."""
        if index < 0:
            index = next(self._next) % self.bundle_count
        return self._res[index]

    def __repr__(self) -> str:
        return f"PlacementGroup(id={self.id}, bundles={self.bundle_specs})"


def placement_group(
    bundles: list[dict[str, Any]],
    strategy: str = "PACK",
    name: str = "",
    lifetime: str | None = None,
    _max_cpu_fraction_per_node: float | None = None,
    bundle_label_selector: Any = None,
) -> PlacementGroup:
    if strategy not in _STRATEGIES:
        raise ValueError(f"Invalid placement group strategy {strategy}. Supported strategies are: {list(_STRATEGIES)}.")
    if lifetime == "detached":
        raise unsupported("placement_group(lifetime='detached')", why="reservations end with the run that made them")
    if bundle_label_selector:
        raise unsupported("placement_group(bundle_label_selector=...)", use="ch.reserve bundles with device=")
    if not bundles or any(not b for b in bundles):
        raise ValueError("Bundles must be a non-empty list of non-empty resource dicts.")
    pg = PlacementGroup(ch.reserve([_bundle(b) for b in bundles], strategy=_STRATEGIES[strategy]), list(bundles), name)
    if name:
        _named[name] = pg
    return pg


def remove_placement_group(placement_group: PlacementGroup) -> None:
    placement_group._res.release()
    _named.pop(placement_group.name, None)


def get_placement_group(placement_group_name: str) -> PlacementGroup:
    if placement_group_name not in _named:
        raise ValueError(f"Failed to look up placement group with name: {placement_group_name}")
    return _named[placement_group_name]


def _no(name: str) -> Any:
    def call(*args: Any, **kwargs: Any) -> Any:
        raise unsupported(f"ray.util.{name}", use="keep the PlacementGroup you created")

    call.__name__ = name
    return call


get_current_placement_group = _no("get_current_placement_group")
placement_group_table = _no("placement_group_table")
