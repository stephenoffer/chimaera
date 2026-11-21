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
    _children: int = 0
    _cancelled: threading.Event = field(default_factory=threading.Event)

    def next_child(self) -> int:
        self._children += 1
        return self._children

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
