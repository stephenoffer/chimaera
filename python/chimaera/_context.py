"""What a running call knows about itself: ``ch.context()``."""

from __future__ import annotations

import contextvars
import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskContext:
    run: bytes
    task: bytes
    attempt: int
    item_index: int
    policy: dict[str, Any]
    deadline_unix_ms: int
    actor: bytes = b""
    # A durable workflow: its calls are fingerprinted so a replay that makes
    # different calls is caught (ch.workflow helpers need it set).
    workflow: bool = False
    _children: int = 0
    _reports: int = 0
    # This rank's Group inside a gang (fn.options(ranks=n).gang()), else None.
    gang: Any = None
    _cancelled: threading.Event = field(default_factory=threading.Event)

    def next_child(self) -> int:
        self._children += 1
        return self._children

    def report(self, **metrics: Any) -> None:
        """Report progress (``loss=0.3, epoch=2``); the caller reads it with
        ``future.metrics()``. One replicated write each: per epoch, not per sample."""
        from ._metrics import report

        report(self, metrics)

    def cancelled(self) -> bool:
        """True once the caller cancelled this call; check it to stop early."""
        return self._cancelled.is_set()

    @property
    def run_id(self) -> str:
        return self.run.hex()

    @property
    def task_id(self) -> str:
        return self.task.hex()

    @property
    def cpus(self) -> float:
        return float(self.policy.get("cpus") or 1.0)

    @property
    def actor_id(self) -> str | None:
        """The actor this call runs on (hex), or None for a function call."""
        return self.actor.hex() if self.actor else None

    @property
    def node(self) -> str:
        """The peer (hex id) this call runs on, as ``ch.cluster().nodes()`` names it."""
        from ._runtime import Runtime

        return Runtime.get().info["peer"].hex()


_current: contextvars.ContextVar[TaskContext | None] = contextvars.ContextVar("chimaera_task", default=None)


def current_task() -> TaskContext | None:
    return _current.get()


def set_task(ctx: TaskContext | None) -> contextvars.Token[TaskContext | None]:
    return _current.set(ctx)


def reset(token: contextvars.Token[TaskContext | None]) -> None:
    _current.reset(token)


def context() -> TaskContext | None:
    """The current call's context inside a task, or None in the driver."""
    return _current.get()
