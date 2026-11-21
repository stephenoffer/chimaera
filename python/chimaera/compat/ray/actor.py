"""``ray.actor``: the handle and class types, for annotations and ``isinstance``."""

from __future__ import annotations

from ._remote import ActorClass, ActorHandle
from ._unsupported import unsupported


def exit_actor() -> None:
    raise unsupported("ray.actor.exit_actor()", why="an actor is ended by its owner", use="ray.kill(handle)")


__all__ = ["ActorClass", "ActorHandle", "exit_actor"]
