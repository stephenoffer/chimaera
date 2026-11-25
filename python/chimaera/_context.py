"""What a running call knows about itself: ``ch.context()``."""

from __future__ import annotations

import contextvars
import threading
from dataclasses import dataclass, field
from typing import Any, NoReturn


# The built-in actor method that ``close(drain=True)`` calls last (see
# ``Executor.shut_down``).
SHUTDOWN_METHOD = "__chimaera_shutdown__"
# The built-in actor method a compiled graph runs its actors through (``_graph.serve``).
GRAPH_METHOD = "__chimaera_graph__"


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
    # different calls is caught (ch.durable helpers need it set).
    workflow: bool = False
    _children: int = 0
    _reports: int = 0
    # This rank's Group inside a gang (fn.options(ranks=n).gang()), else None.
    gang: Any = None
    # exit_actor() was called: the worker closes the actor once this call's outcome is sent.
    exiting: bool = False
    _cancelled: threading.Event = field(default_factory=threading.Event)

    def exit_actor(self) -> NoReturn:
        """End the actor this method runs on, as ``close()`` on its handle does:
        it is not restarted, its pending calls raise ``ch.ActorDied``, and so
        does this call."""
        from ._errors import ActorDied

        if not self.actor:
            raise RuntimeError("exit_actor() ends an actor: call it inside one of the actor's methods")
        self.exiting = True
        raise ActorDied("the actor ended itself with ch.context().exit_actor()")

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

    def describe_policy(self) -> str:
        """Each option this call runs with and where it was set, in the words
        ``fn.policy().describe()`` used where the call was made. Only a call
        made with ``explain=True`` carries them."""
        from ._errors import OptionsError
        from ._options import describe

        said = self.policy.get("explained")
        if not said:
            raise OptionsError(
                "describe_policy(): this call was made without explain=True, so it carries no record of where its "
                "options were set; make it with .options(explain=True), on its decorator or in a "
                "`with ch.defaults(explain=True):` block"
            )
        return describe(said)

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
        """The peer (hex id) this call runs on, as ``ch.runs.cluster().nodes()`` names it."""
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
