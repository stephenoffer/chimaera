"""A durable actor's state as committed: a header naming this codec and the
version of the state's shape, then the state pickled.

    b"CHS1" | version (4 bytes, big endian) | cloudpickle(state)

The version is the class's ``ch.durable.State(version=)``. A class restoring
state an older version wrote passes it through its ``State(migrate=)``
first; state a newer version wrote is refused. State committed before the
header existed is a bare pickle (its first byte is the pickle protocol
opcode, never b"C"): it reads as version 1.
"""

from __future__ import annotations

import pickle
import struct
from typing import Any

import cloudpickle

from ._options import State

MAGIC = b"CHS1"
HEADER = struct.Struct(">4sI")


def spec_of(cls: type[Any]) -> State:
    """The class's ``State(...)``, set by ``@ch.remote(state=...)``."""
    return getattr(cls, "__chimaera_state__", None) or State()


def dumps(instance: Any) -> bytes:
    """What to commit for `instance`: what ``__getstate__`` returns (its
    attributes by default), under its class's state version."""
    version = spec_of(type(instance)).version
    return HEADER.pack(MAGIC, version) + cloudpickle.dumps(instance.__getstate__(), protocol=5)


def loads(blob: bytes) -> tuple[int, Any]:
    """(version, state) of a committed blob."""
    if blob[:4] == MAGIC:
        _, version = HEADER.unpack_from(blob)
        return version, pickle.loads(memoryview(blob)[HEADER.size :])
    return 1, pickle.loads(blob)


def restore(cls: type[Any], blob: bytes) -> Any:
    """The state an instance of `cls` takes from a committed blob, migrated
    from the version that wrote it to the class's own."""
    version, state = loads(blob)
    spec = spec_of(cls)
    if version > spec.version:
        raise RuntimeError(
            f"{cls.__qualname__}: the committed state is version {version}, newer than this class's "
            f"State(version={spec.version}); restore it with the class that wrote it, or a later one"
        )
    if version < spec.version and spec.migrate is not None:
        state = spec.migrate(state, version)
    return state
