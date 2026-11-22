"""Many copies at once: SPMD gangs and their collectives, reservations that
hold resources before work runs, placement, and accelerators."""

from ._devices import device, devices
from ._gang import Gang, GangActor, GangFailed, Group, GroupHandle, group
from ._options import Placement
from ._reserve import Reservation, reserve

__all__ = [
    "Gang", "GangActor", "GangFailed", "Group", "GroupHandle", "Placement", "Reservation", "device", "devices", "group", "reserve",
]
