"""``ray.util.register_serializer``: a class serialized with your own two
callables, lambdas and local functions included, as in Ray.

The pair is used where Chimaera serializes values (``ch.ext.register_reducer``),
not by a plain ``pickle.dumps`` elsewhere in the program, as Ray scopes it to
its own serialization. The deserializer travels inside each value, by value
when it has to, so a process that only reads such values needs nothing
registered. As in Ray, the registration is this process's: one that sends
instances of the class needs it too.
"""

from __future__ import annotations

from typing import Any, Callable

import chimaera as ch

_ours: set[type] = set()  # the types registered here, which deregister may remove


def register_serializer(cls: type, *, serializer: Callable[[Any], Any], deserializer: Callable[[Any], Any]) -> None:
    """Send instances of `cls` as ``serializer(obj)``; rebuild them with ``deserializer(that)``."""
    if not isinstance(cls, type):
        raise TypeError(f"register_serializer takes a class, not {type(cls).__name__}")
    ch.ext.register_reducer(cls, lambda obj: (deserializer, (serializer(obj),)))
    _ours.add(cls)


def deregister_serializer(cls: type) -> None:
    """Serialize `cls` the usual way again (only a registration made here is removed)."""
    if cls in _ours:
        _ours.discard(cls)
        ch.ext.unregister_reducer(cls)
