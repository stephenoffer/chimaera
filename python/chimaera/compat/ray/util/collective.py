"""``ray.util.collective`` on Chimaera: collective groups of actors, over ``ch.group``.

A group is declared from the driver (``create_collective_group``) or by each
actor (``init_collective_group``); the collectives run inside the actors and
work in place, as Ray's do. Torch tensors go through ``torch.distributed``
(``gloo`` on CPU, ``nccl`` on CUDA), so an actor process uses one
torch-backed group at a time. NumPy arrays go through the replicated store, so
keep them small (a value is at most 1 MiB). Not mapped: ``send``/``recv``,
``reducescatter`` and the ``*_multigpu`` calls.
"""

from __future__ import annotations

import enum
import threading
from typing import Any

import chimaera as ch

from .._unsupported import UnsupportedAttribute, unsupported


class ReduceOp(enum.Enum):
    SUM = 0
    PRODUCT = 1
    MIN = 2
    MAX = 3


class Backend:
    NCCL = "nccl"
    GLOO = "gloo"


TTL = "24h"
_groups: dict[str, Any] = {}  # group name -> this actor's ch.Group
_lock = threading.Lock()


def _declared() -> ch.Store:
    return ch.Store("ray-collective")


def create_collective_group(
    actors: list[Any],
    world_size: int,
    ranks: list[int],
    backend: str = Backend.NCCL,
    group_name: str = "default",
    gloo_timeout: int = 30000,
) -> None:
    """Declare `actors` a group from the driver; each joins on its first collective."""
    if len(ranks) != len(actors):
        raise RuntimeError(
            f"Each actor should correspond to one rank. Got '{len(ranks)}' ranks but '{len(actors)}' actors"
        )
    if sorted(ranks) != list(range(len(ranks))) or world_size != len(actors):
        raise RuntimeError(f"Ranks must be a permutation from 0 to '{len(ranks)}', one per actor of '{world_size}'.")
    if _declared().get(group_name) is not None:
        raise RuntimeError("Trying to initialize a group twice.")
    from .._remote import ActorHandle  # imports ray.util, so not at module level

    native = [a._actor if isinstance(a, ActorHandle) else a for a in actors]
    ordered = [a for _, a in sorted(zip(ranks, native, strict=True), key=lambda ra: ra[0])]
    _declared().put(group_name, ch.group(*ordered, key=f"ray-{group_name}"), ttl=TTL)


def init_collective_group(
    world_size: int, rank: int, backend: str = Backend.NCCL, group_name: str = "default", gloo_timeout: int = 30000
) -> None:
    """Join a group from inside an actor, as rank `rank` of `world_size`."""
    ctx = ch.context()
    if ctx is None or ctx.actor_id is None:
        raise RuntimeError("init_collective_group runs inside an actor")
    if not 0 <= rank < world_size:
        raise ValueError(f"rank {rank} is not in [0, {world_size})")
    if group_name in _groups:
        raise RuntimeError("Trying to initialize a group a second time.")
    store = ch.Store(f"ray-collective/{group_name}")
    store.put(f"member/{rank}", ctx.actor_id, ttl=TTL)
    round_ = store.barrier("init", world_size).wait()  # the same number on every rank
    members = tuple(store.get(f"member/{r}") for r in range(world_size))
    joined = ch.GroupHandle(f"ray-init-{group_name}", round_, members).join()
    with _lock:
        _groups[group_name] = joined


def _group(name: str) -> Any:
    """This actor's group `name`, joining a driver-declared one on first use
    (the join waits for the other members; it holds no lock meanwhile)."""
    g = _groups.get(name)
    if g is None:
        handle = _declared().get(name)
        if handle is None:
            raise RuntimeError(f"The collective group '{name}' is not initialized in the process.")
        joined = handle.join()  # one rendezvous per member, however many calls race here
        with _lock:
            g = _groups.setdefault(name, joined)
    return g


def is_group_initialized(group_name: str) -> bool:
    return group_name in _groups


def destroy_collective_group(group_name: str = "default") -> None:
    with _lock:
        _groups.pop(group_name, None)


def get_rank(group_name: str = "default") -> int:
    g = _groups.get(group_name)
    return g.rank if g is not None else -1


def get_collective_group_size(group_name: str = "default") -> int:
    g = _groups.get(group_name)
    return g.size if g is not None else -1


def _torch(tensor: Any) -> bool:
    return type(tensor).__module__.startswith("torch")


def _combine(op: ReduceOp) -> Any:
    import numpy as np

    return {ReduceOp.SUM: np.add, ReduceOp.PRODUCT: np.multiply, ReduceOp.MIN: np.minimum, ReduceOp.MAX: np.maximum}[op]


def _into(tensor: Any, value: Any) -> None:
    import numpy as np

    if not isinstance(tensor, np.ndarray):
        raise TypeError(f"collectives work in place on numpy arrays or torch tensors, got {type(tensor).__name__}")
    np.copyto(tensor, value)


def allreduce(tensor: Any, group_name: str = "default", op: ReduceOp = ReduceOp.SUM) -> None:
    g = _group(group_name)
    if _torch(tensor):
        dist = g.torch()
        dist.all_reduce(tensor, op=getattr(dist.ReduceOp, op.name))
    else:
        _into(tensor, g.all_reduce(tensor, op=_combine(op)))


def reduce(tensor: Any, dst_rank: int = 0, group_name: str = "default", op: ReduceOp = ReduceOp.SUM) -> None:
    g = _group(group_name)
    if _torch(tensor):
        dist = g.torch()
        dist.reduce(tensor, dst_rank, op=getattr(dist.ReduceOp, op.name))
        return
    total = g.all_reduce(tensor, op=_combine(op))
    if g.rank == dst_rank:
        _into(tensor, total)


def broadcast(tensor: Any, src_rank: int = 0, group_name: str = "default") -> None:
    g = _group(group_name)
    if _torch(tensor):
        g.torch().broadcast(tensor, src_rank)
    else:
        _into(tensor, g.broadcast(tensor if g.rank == src_rank else None, src=src_rank))


def allgather(tensor_list: list[Any], tensor: Any, group_name: str = "default") -> None:
    g = _group(group_name)
    if len(tensor_list) != g.size:
        raise RuntimeError(f"tensor_list has {len(tensor_list)} entries; the group has {g.size} ranks")
    if _torch(tensor):
        g.torch().all_gather(tensor_list, tensor)
        return
    for out, value in zip(tensor_list, g.all_gather(tensor), strict=True):
        _into(out, value)


def barrier(group_name: str = "default") -> None:
    _group(group_name).barrier()


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(name)
    use = "torch.distributed on group.torch() inside the actors" if not name.endswith("_multigpu") else ""
    raise UnsupportedAttribute(str(unsupported(f"ray.util.collective.{name}", use=use)))
