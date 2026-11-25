"""What survives a process dying: actor state (``@ch.remote(state=State())``)
and durable workflows (``@ch.remote(workflow="durable")``), whose
non-repeatable steps go through ``await now()``, ``random()``, ``sleep()``
and ``record(fn, ...)`` so a replay gets the same answers, and whose writes to
other systems are ``Activity`` steps with a key: ``record(activity, ...)``
and ``compensate(activity, key, receipt)``."""

from ._errors import CommandConflict, WorkflowDivergence
from ._options import State
from ._workflow import Activity, compensate, now, random, record, sleep

__all__ = [
    "Activity",
    "CommandConflict",
    "State",
    "WorkflowDivergence",
    "compensate",
    "now",
    "random",
    "record",
    "sleep",
]
