"""``ray.actor``: the handle and class types, for annotations and ``isinstance``."""

from __future__ import annotations

from ._remote import ActorClass, ActorHandle


def exit_actor() -> None:
    """End the actor this method runs on (``ch.context().exit_actor()``)."""
    import chimaera as ch

    ctx = ch.context()  # None outside a call
    if ctx is None or ctx.actor_id is None:
        raise TypeError("exit_actor API is called on a non-actor worker")
    ctx.exit_actor()


__all__ = ["ActorClass", "ActorHandle", "exit_actor"]
