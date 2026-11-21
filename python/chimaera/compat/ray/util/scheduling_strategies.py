"""Ray's scheduling strategy objects. They are values only; ``_options`` decides
which ones Chimaera can honor today."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeAffinitySchedulingStrategy:
    node_id: str
    soft: bool = False
    _spill_on_unavailable: bool = False
    _fail_on_unavailable: bool = False


@dataclass
class PlacementGroupSchedulingStrategy:
    placement_group: Any
    placement_group_bundle_index: int = -1
    placement_group_capture_child_tasks: bool | None = None


@dataclass
class NodeLabelSchedulingStrategy:
    hard: dict[str, Any] = field(default_factory=dict)
    soft: dict[str, Any] | None = None


class _LabelMatch:
    """A label expression of NodeLabelSchedulingStrategy (In, NotIn, Exists, DoesNotExist)."""

    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(map(repr, self.values))})"


class In(_LabelMatch):
    pass


class NotIn(_LabelMatch):
    pass


class Exists(_LabelMatch):
    pass


class DoesNotExist(_LabelMatch):
    pass
