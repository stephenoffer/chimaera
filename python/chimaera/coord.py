"""Sharing state between calls and processes.

- ``Queue``: a bounded first-in, first-out queue whose ``put`` and ``get`` return futures.
- ``WorkQueue``: a named queue of work items kept in the replicated store; consumers claim items, and an item whose consumer fails is delivered again.
- ``Topic``: a bounded log delivered to every subscriber.
- ``Store``: replicated keys with compare-and-set, counters, leases (locks), barriers and watches.
"""

from ._coord import Claim, Gap, Queue, Subscription, Topic, WorkQueue
from ._errors import Closed, LeaseLost, VersionConflict
from ._store import Lease, Scan, Store, Watch

__all__ = [
    "Claim",
    "Closed",
    "Gap",
    "Lease",
    "LeaseLost",
    "Queue",
    "Scan",
    "Store",
    "Subscription",
    "Topic",
    "VersionConflict",
    "Watch",
    "WorkQueue",
]
