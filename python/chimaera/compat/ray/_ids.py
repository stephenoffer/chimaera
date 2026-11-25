"""Ray's ids for refs: ``ray.TaskID`` and ``ray.JobID``, and what every ref
gives: ``binary()``, ``hex()``, ``task_id()``, ``job_id()``, ``is_nil()`` and
``call_site()``, with ``ObjectRef(id)``, ``ObjectRef.from_binary()``,
``from_hex()``, ``nil()``, ``from_random()`` and ``size()``.

A ref's id is SIZE bytes: its run (Ray's job), the call or ``put`` it comes
from, and which of a call's ``num_returns`` values it is (from 1; 0 for a
whole result). A generator item's middle part is its own, random. An id
finds its ref while the ref is alive in this process and its id was read
here, as in Ray an id resolves through its owner's table of live refs. Any
other id makes a ref whose ``ray.get`` raises ObjectLostError.
"""

from __future__ import annotations

import os
import weakref
from typing import Any

import chimaera as ch

from .exceptions import ObjectLostError

SIZE = 36
_NIL = b"\xff" * SIZE
_live: weakref.WeakValueDictionary[bytes, Any] = weakref.WeakValueDictionary()  # refs whose id was read


class BaseID:
    """A 16-byte id with Ray's methods."""

    __slots__ = ("_id",)
    _SIZE = 16

    def __init__(self, id: bytes) -> None:
        if len(id) != self._SIZE:
            raise ValueError(f"a {type(self).__name__} is {self._SIZE} bytes, not {len(id)}")
        self._id = bytes(id)

    @classmethod
    def from_binary(cls, id_bytes: bytes) -> Any:
        return cls(id_bytes)

    @classmethod
    def from_hex(cls, hex_id: str) -> Any:
        return cls(bytes.fromhex(hex_id))

    @classmethod
    def nil(cls) -> Any:
        return cls(b"\xff" * cls._SIZE)

    @classmethod
    def size(cls) -> int:
        return cls._SIZE

    def binary(self) -> bytes:
        return self._id

    def hex(self) -> str:
        return self._id.hex()

    def is_nil(self) -> bool:
        return self._id == b"\xff" * self._SIZE

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other._id == self._id  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self._id))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.hex()})"


class TaskID(BaseID):
    """A call's id in its run: what ``get_runtime_context().get_task_id()`` gives inside it."""

    __slots__ = ()


class JobID(BaseID):
    """A run's id (Ray's job): what ``get_runtime_context().get_job_id()`` gives."""

    __slots__ = ()


def this_task() -> bytes | None:
    """The call this code runs in, or None in a driver."""
    ctx = ch.context()
    return ctx.task if ctx is not None else None


def item_id() -> bytes:
    """A new generator item's id: this run's, then random."""
    return bytes.fromhex(ch.runs.current_run().id) + os.urandom(SIZE - 20) + bytes(4)


def binary(ref: Any) -> bytes:
    """The ref's id (Ray's ``ObjectRef.binary()``)."""
    from ._remote import ItemRef, PutRef

    if isinstance(ref, ItemRef):
        out = ref._id
    else:  # a call's or put's run and object ids, and the index of one of a call's values
        whole, _, index = (ref._ref if isinstance(ref, PutRef) else ref).id.partition("#")
        out = bytes.fromhex(whole) + (int(index) + 1 if index else 0).to_bytes(4, "little")
    _live[out] = ref
    return out


def task_id(ref: Any) -> TaskID:
    """The call that made the ref's value (Ray's ``ObjectRef.task_id()``): nil
    for a driver's ``put`` and for a generator item."""
    from ._remote import ItemRef, PutRef

    if isinstance(ref, PutRef):
        return TaskID(ref._task) if ref._task else TaskID.nil()
    if isinstance(ref, ItemRef) or ref._put is not None:
        return TaskID.nil()
    return TaskID(ref.key[16:32])


def find(id_bytes: bytes) -> Any:
    """``ObjectRef(id)``: the live ref with that id, or one whose get says there is none."""
    from ._remote import ItemRef

    id_bytes = bytes(id_bytes)
    if len(id_bytes) != SIZE:
        raise ValueError(f"an ObjectRef id is {SIZE} bytes, not {len(id_bytes)}")
    ref = _live.get(id_bytes)
    if ref is None:
        lost = ObjectLostError(f"no ObjectRef {id_bytes.hex()} is alive in this process")
        ref = ItemRef(error=lost, id=id_bytes)
    return ref


def nil() -> Any:
    """``ObjectRef.nil()``: the ref that names no object."""
    from ._remote import ItemRef

    return ItemRef(error=ObjectLostError("the nil ObjectRef names no object"), id=_NIL)


def from_random() -> Any:
    """``ObjectRef.from_random()``: a ref with a new id that names no object."""
    from ._remote import ItemRef

    return ItemRef(error=ObjectLostError("an ObjectRef from from_random names no object"), id=os.urandom(SIZE))


def attach(types: tuple[type, ...]) -> None:
    """Give each of `types` Ray's ObjectRef id methods."""
    methods = {
        "binary": binary,
        "hex": lambda self: binary(self).hex(),
        "task_id": task_id,
        "job_id": lambda self: JobID(binary(self)[:16]),
        "is_nil": lambda self: binary(self) == _NIL,
        # Ray records a ref's call site only with RAY_record_ref_creation_sites set; Chimaera never does.
        "call_site": lambda self: "",
    }
    for t in types:
        for name, fn in methods.items():
            setattr(t, name, fn)
