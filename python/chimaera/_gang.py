"""Gangs: SPMD jobs whose ranks start together, share a rendezvous, and fail
and retry as one. ``fn.options(ranks=4).gang(*args)``.

    @ch.remote(cpus=2)
    def train(steps):
        g = ch.context().gang                 # rank, size, local_rank, generation
        dist = g.torch()                      # torch.distributed, initialized
        ...
        return loss

    losses = train.options(ranks=4).gang(1000).result()   # one result per rank

Every rank gets the environment torch, JAX and DeepSpeed read: RANK,
WORLD_SIZE, LOCAL_RANK, LOCAL_WORLD_SIZE, MASTER_ADDR and MASTER_PORT (rank
0's host and a port it holds there until its code starts), plus CHIMAERA_GANG
and CHIMAERA_GANG_GENERATION. If any rank is lost (its worker or peer dies),
the whole generation is cancelled and the gang starts again as a new
generation, up to ``max_attempts`` times; resume from your own checkpoint. A
rank that raises fails the gang with that error.

A generation is admitted all or none: the peers' ledgers hold a slot (its
CPUs and devices) for every rank before any rank starts, and each rank starts
on its own slot with a launch token (see ``_ledger``). A gang that does not
fit waits holding nothing, so two gangs that each need the whole cluster run
one after the other instead of each holding half. One that could not fit even
on idle peers fails at once as ``ch.Infeasible``, naming the first rank that
does not fit. Admitted, the generation commits one rendezvous record (the
ranks' peers, the deadline, rank 0's endpoint); every rank must reach the
rendezvous within ``rendezvous_timeout`` (default 300 s), or the generation
is lost with each missing rank's stage (queued on its peer and why, or
started). A rank of a generation that was replaced cannot start, join a
collective or torch's store of the next one: it raises ``GangFailed``.

``ranks=ch.auto(initial, min=, max=)`` makes the gang elastic: each
generation has as many ranks as the live peers can hold (by CPUs, and GPUs
when the ranks need them), between ``min`` and ``max``; the first starts at
``initial`` when given. So a gang that loses a peer comes back smaller instead
of waiting for a replacement. ``gang.decisions`` says what each generation
chose. shortcut: a running generation never grows; new capacity is used by
the next one.

``ch.dist.group(*actors)`` makes existing actors one group: pass the handle to them,
and inside a member ``handle.join()`` is its ``Group``.

``explain()`` on a gang, actor gang or group handle says which collective each
rank last entered and names the ranks holding the others up. A collective's
wait includes its name, so ranks on different code paths hang where
``explain()`` names them instead of mixing their data.

A rank waits in a store collective, or in ``torch()`` for the other ranks,
at most ``collective_timeout`` seconds (default 600; torch's process group
gets the same timeout). Past it the generation is lost: the rank that gives
up closes the collective, so either every rank passes it or none does, and
records why. Every rank raises that ``GangFailed``: at once in that
collective, within a second in another, or at its next one, so none waits
out the deadline alone. A function gang then starts its next generation, as
for a lost rank; an actor gang's calls raise it until ``restart()``. A
``torch.distributed`` ``DistError`` out of a rank or out of ``torch()``
(torch's watchdog timed a collective out, or it lost a peer) loses the
generation the same way. A collective's own ``timeout=`` closes it with a
``TimeoutError`` instead, raised in every rank that waits in it or enters
it: the rank's error, which fails the gang.

A collective value that pickles to more than the peer's inline limit (64
KiB) goes through the object plane: the sender stores it with ``ch.put``,
the store carries only its handle, and the sender frees it once every rank
has read it. A copy that a rank on another peer fetched stays on that peer
until the run ends.

"""

from __future__ import annotations

import functools
import math
import operator
import os
import pickle
import socket
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, NamedTuple, NoReturn, TypeVar

import cloudpickle

from . import _ledger
from ._errors import InfrastructureFailure, VersionConflict
from ._future import Future
from ._native import blake3_digest, child_id
from ._options import Auto, Effective, Placement, _pos_float
from ._remote import Actor, method_options, remote, with_replay

R = TypeVar("R")

RENDEZVOUS_S = 300.0  # rendezvous_timeout's default
COLLECTIVE_S = 600.0  # collective_timeout's default: torch's own for NCCL
TTL = "24h"  # rendezvous and collective keys expire
# The rank options a gang passes on: what a rank needs, not how the gang retries.
_RANK_OPTIONS = ("cpus", "memory", "memory_limit", "gpus", "device", "resources", "placement", "priority", "deadline")


class GangFailed(InfrastructureFailure):
    """Every attempt of a gang lost a rank (``max_attempts`` generations). In
    a rank: its generation was given up (a collective outlasted
    ``collective_timeout``) or replaced, so the rank must stop."""


# The gang whose torch.distributed group this worker process initialized,
# and torch's DistError (its backend, network and store errors) then.
_torch_group: tuple[str, int] | None = None
_torch_error: type[BaseException] | None = None


def _end_torch_group() -> None:
    global _torch_group
    dist = sys.modules.get("torch.distributed")
    if _torch_group is not None and dist is not None and dist.is_initialized():
        dist.destroy_process_group()
    _torch_group = None


def _torch_failure(e: BaseException, group: Group) -> GangFailed | None:
    """`e`, out of a rank, as its generation's loss if it is torch's
    ``DistError`` from this gang's process group: torch's watchdog timed a
    collective out, or torch lost a peer. The gang reforms, as it does when a
    store collective stalls."""
    if _torch_group != (group.id, group.generation) or _torch_error is None or not isinstance(e, _torch_error):
        return None
    from ._store import Store

    why = f"rank {group.rank}: torch.distributed raised {type(e).__name__}: {e}"
    return _abandon(Store(f"gang/{group._root}"), group._root, group.generation, why)


class Group:
    """This rank's view of its gang (``ch.context().gang`` inside a rank)."""

    def __init__(self, env: dict[str, str]) -> None:
        self.rank = int(env["RANK"])
        self.size = int(env["WORLD_SIZE"])
        self.local_rank = int(env["LOCAL_RANK"])
        self.local_size = int(env["LOCAL_WORLD_SIZE"])
        self.generation = int(env["CHIMAERA_GANG_GENERATION"])
        self.id = env["CHIMAERA_GANG"]
        self._root = self.id  # the gang's own id (a mesh axis's id names its axis)
        self.master = (env["MASTER_ADDR"], int(env["MASTER_PORT"]))
        self.ranks = list(range(self.size))  # members, as ranks of the gang (a mesh axis lists a subset)
        self._axis = ""  # set on a mesh axis
        self._seq = 0  # this group's last collective
        # Each group's last collective by id, shared with its mesh axes: an
        # axis built again by a later mesh() goes on from its last number.
        self._seqs: dict[str, int] = {}
        self._stall: float | None = None  # collective_timeout, read from the record when first needed

    def __repr__(self) -> str:
        axis = f" axis {self._axis}" if self._axis else ""
        return f"<Group{axis} rank {self.rank}/{self.size} generation {self.generation}>"

    def mesh(self, **axes: int) -> dict[str, Group]:
        """This rank's groups along each axis of a grid of the ranks:
        ``mesh(dp=2, tp=4)`` on 8 ranks (the sizes multiply to the group's; the
        last axis varies fastest, as in torch's DeviceMesh). ``m["tp"]`` holds
        the ranks that differ from this one only along ``tp``, with the same
        store collectives; its ``ranks`` are their ranks in this group, for
        ``dist.new_group`` or torch's ``init_device_mesh``."""
        if not axes or any(isinstance(n, bool) or not isinstance(n, int) or n < 1 for n in axes.values()):
            raise ValueError(f"mesh() takes axis sizes as positive integers, got {axes}")
        if math.prod(axes.values()) != self.size:
            raise ValueError(
                f"mesh{tuple(axes.items())}: sizes multiply to {math.prod(axes.values())}, not {self.size}"
            )
        names, shape = list(axes), list(axes.values())
        coords, rest = [], self.rank
        for n in reversed(shape):
            coords.insert(0, rest % n)
            rest //= n
        out = {}
        for i, name in enumerate(names):
            stride = math.prod(shape[i + 1 :])
            first = self.rank - coords[i] * stride
            fixed = ",".join(f"{a}{c}" for j, (a, c) in enumerate(zip(names, coords, strict=True)) if j != i)
            axis = Group.__new__(Group)
            axis.__dict__.update(self.__dict__)
            axis.id = f"{self.id}/mesh-{'.'.join(f'{a}{n}' for a, n in axes.items())}/{name}/{fixed}"
            axis.ranks = [self.ranks[first + k * stride] for k in range(shape[i])]
            axis.rank, axis.size, axis._axis = coords[i], shape[i], name
            axis._seq = self._seqs.get(axis.id, 0)
            out[name] = axis
        return out

    def table(self) -> list[dict[str, Any]]:
        """This group's ranks as this generation started them, in order: each
        rank's ``rank`` in the gang, ``node`` (its peer), ``local_rank``,
        ``devices`` (the ids of the devices it was granted) and ``visible``
        (their indexes as its process numbers them, so ``0`` for a rank with
        one GPU). One read of the gang's store."""
        from ._store import Store

        rows = {v["rank"]: v for _, v in Store(f"gang/{self._root}").scan(f"{self.generation}/host/")}
        seen: dict[str, int] = {}
        for r in sorted(rows):  # as LOCAL_RANK: its place among the ranks on its peer
            rows[r]["local_rank"] = seen.get(rows[r]["node"], 0)
            seen[rows[r]["node"]] = rows[r]["local_rank"] + 1
        return [rows[r] for r in self.ranks]

    @property
    def backend(self) -> str:
        """The ``torch.distributed`` backend of this rank's devices (``gloo`` on CPU)."""
        from ._devices import devices

        granted = devices()
        return granted[0]["backend"] if granted else "gloo"

    def torch(self, backend: str | None = None) -> Any:
        """``torch.distributed``, its default process group initialized for this
        gang (the rank's call tears it down when it returns)."""
        if self._axis:
            raise RuntimeError(
                f"mesh axis {self._axis!r} has no process group of its own: after the gang's group.torch(), "
                "use torch's init_device_mesh, or dist.new_group(axis.ranks) on every rank"
            )
        from ._store import Store

        _fence(Store(f"gang/{self._root}"), self._root, self.generation)
        from datetime import timedelta

        import torch.distributed as dist

        global _torch_group, _torch_error
        if dist.is_initialized() and _torch_group != (self.id, self.generation):
            dist.destroy_process_group()  # left by another gang in this reused worker
        if not dist.is_initialized():
            error = getattr(dist, "DistError", None)
            try:
                store = self._torch_store(dist)
                timeout = timedelta(seconds=self._deadline())  # torch's collectives get the store's deadline
                dist.init_process_group(
                    backend or self.backend, store=store, rank=self.rank, world_size=self.size, timeout=timeout
                )
            except Exception as e:
                if error is None or not isinstance(e, error):
                    raise
                # torch's store or backend gave up meeting the other ranks: as a stalled collective
                why = f"rank {self.rank}: torch.distributed raised {type(e).__name__} setting up: {e}"
                raise _abandon(Store(f"gang/{self._root}"), self._root, self.generation, why) from e
            _torch_group, _torch_error = (self.id, self.generation), error
        return dist

    def _torch_store(self, dist: Any) -> Any:
        """torch's store for this generation. Rank 0 binds its committed
        endpoint again or, if something took that port since its code
        started, another port, and hands the bound socket to torch (so
        nothing can take it in between), committing where it serves as the
        record's next endpoint; the others connect there. Its keys are this
        generation's, so a stale rank could not mix in."""
        from datetime import timedelta

        from ._store import Store

        store = Store(f"gang/{self._root}")
        timeout = self._deadline()  # meeting in torch's store is a collective too
        addr, port = self.master
        wait = timedelta(seconds=timeout)
        if self.rank == 0:
            sock = socket.socket()
            try:
                sock.bind(("", port))
            except OSError:  # taken since rank 0's code started
                sock.close()
                sock = socket.socket()
                sock.bind(("", 0))
            sock.listen()
            port = sock.getsockname()[1]
            fd = sock.detach()  # torch's server listens on it from here
            server = dist.TCPStore(addr, port, self.size, True, wait, wait_for_workers=False, master_listen_fd=fd)
            _publish(store, self.generation, addr, port, serving=True)
        else:
            addr, port = _serving(store, self._root, self.generation, timeout)
            server = dist.TCPStore(addr, port, self.size, False, wait)
        self.master = (addr, port)
        os.environ["MASTER_PORT"] = str(port)
        return dist.PrefixStore(f"{self._root}/{self.generation}", server)

    def _deadline(self) -> float:
        """How long this rank waits in a collective before its generation is
        lost: the gang's collective_timeout, from its record."""
        if self._stall is None:
            from ._store import Store

            record = Store(f"gang/{self._root}").get(f"{self.generation}/record") or {}
            self._stall = float(record.get("collective_timeout", COLLECTIVE_S))
        return self._stall

    # Collectives through the store: no torch needed. Each one records where
    # this rank is, for explain().

    def _collective(
        self, op: str, timeout: float | None, value: Any = None, send: bool = False, read: Sequence[int] = ()
    ) -> list[Any]:
        """Enter collective `op`: send `value` if `send`, wait for every rank,
        and return the values ranks `read` sent, in order. A value larger than
        the peer's inline limit goes through the object plane (``ch.put``):
        the store holds its handle, and the sender keeps it until every rank
        has read it."""
        from ._future import put
        from ._runtime import Runtime
        from ._store import Store

        _fence(Store(f"gang/{self._root}"), self._root, self.generation)
        self._seq = self._seqs[self.id] = self._seqs.get(self.id, 0) + 1
        store, key = Store(f"gang/{self.id}"), f"{self.generation}/c{self._seq}/{op}"
        store.put(f"{self.generation}/at/{self.rank}", (self._seq, op, time.time()), ttl=TTL)
        held = None  # this rank's stored value, freed when the collective returns
        if send:
            if _pickled_bytes(value) > Runtime.get().inline_limit:
                held = put(value, durability="ephemeral")  # a lost peer loses the generation anyway: no copy
                assert held._put is not None  # a ch.put future carries its handle
                value = _Stored(held._put)
            store.put(f"{key}/{self.rank}", value, ttl=TTL)
        self._wait(store, key, op, timeout)
        values = [store.get(f"{key}/{r}") for r in read]
        if any(type(v) is _Stored for v in values):
            from ._leases import unpickle

            try:
                values = [unpickle(*v.put).result() if type(v) is _Stored else v for v in values]
            except Exception:
                # A sender that gave up freed its value: raise why it gave up, not the read's error.
                _fence(Store(f"gang/{self._root}"), self._root, self.generation)
                if store.version(f"{key}/read/closed"):
                    self._refused(store, f"{key}/read/closed")
                raise
            self._wait(store, f"{key}/read", op, timeout)  # every rank has read: the senders free theirs
        return values

    def _wait(self, store: Any, key: str, op: str, timeout: float | None) -> None:
        """Arrive at `key` and wait for every rank: up to `timeout`, then a
        TimeoutError (the caller's own deadline), else up to the gang's
        collective_timeout, then the generation is lost. A rank that gives up
        closes the collective first, unless the last rank came in meanwhile;
        the ranks waiting in it and any that arrive later raise its error, so
        either every rank passes or none does. A waiting rank also sees,
        within a second, its generation given up anywhere else."""
        from ._future import waiting
        from ._store import Store, _raise

        root, closed = Store(f"gang/{self._root}"), f"{key}/closed"
        r = store._txn([(closed, 0)], [("add", key, None, 1)])  # arrive, unless closed
        if not r["ok"]:
            if not r["error"].startswith("version mismatch"):
                _raise(r)
            self._refused(store, closed)
        wait = self._deadline() if timeout is None else timeout
        deadline, check, pause = time.monotonic() + wait, time.monotonic() + 1.0, 0.005
        while True:
            arrived, version = store.get_versioned(key, 0)
            if arrived >= self.size:
                return
            if arrived < 0:  # closed: the count drops below zero
                self._refused(store, closed)
            now = time.monotonic()
            if now >= check:
                _fence(root, self._root, self.generation)
                check = now + 1.0
            if now >= deadline:
                break
            with waiting():
                time.sleep(pause)
            pause = min(pause * 2, 0.2)
        held = _summary(_positions(store, self.generation, self.size))
        if timeout is None:
            why = f"{op} #{self._seq} of {self!r} waited {wait:g} s (collective_timeout): {held}"
        else:
            why = f"{op} #{self._seq} of {self!r} timed out: {held}"
        # Close it at the count just read: refused if a rank arrived (or closed it) since.
        close = [("put", closed, pickle.dumps((timeout is not None, why)), 0), ("add", key, None, -self.size)]
        while not (r := store._txn([(key, version), (closed, 0)], close))["ok"]:
            if not r["error"].startswith("version mismatch"):
                _raise(r)
            arrived, version = store.get_versioned(key, 0)
            if arrived >= self.size:
                return  # the last rank came in meanwhile: the collective completed
            if arrived < 0:
                self._refused(store, closed)
        if timeout is not None:
            raise TimeoutError(why)
        raise _abandon(root, self._root, self.generation, why)

    def _refused(self, store: Any, closed: str) -> NoReturn:
        """Raise what the rank that closed a collective raised: a TimeoutError
        (its own ``timeout=``) or its generation's loss."""
        from ._store import Store

        explicit, why = store.get(closed)
        if explicit:
            raise TimeoutError(why)
        raise _abandon(Store(f"gang/{self._root}"), self._root, self.generation, why)

    def barrier(self, timeout: float | None = None) -> None:
        self._collective("barrier", timeout)

    def all_gather(self, obj: Any, timeout: float | None = None) -> list[Any]:
        """Every rank's `obj`, in rank order (any picklable value; large ones
        go through the object plane)."""
        return self._collective("all_gather", timeout, obj, send=True, read=range(self.size))

    def all_reduce(self, obj: Any, op: Callable[[Any, Any], Any] = operator.add, timeout: float | None = None) -> Any:
        """Every rank's `obj` combined with `op` (sum by default), on every rank."""
        return functools.reduce(op, self._collective("all_reduce", timeout, obj, send=True, read=range(self.size)))

    def broadcast(self, obj: Any = None, src: int = 0, timeout: float | None = None) -> Any:
        """Rank `src`'s `obj`, on every rank."""
        return self._collective("broadcast", timeout, obj, send=self.rank == src, read=(src,))[0]


class _Stored(NamedTuple):
    """A collective value stored with ``ch.put``: its handle, as the store holds it."""

    put: tuple[Any, ...]


def _pickled_bytes(value: Any) -> int:
    """`value`'s size pickled, without copying its buffers (NumPy arrays)."""
    buffers: list[pickle.PickleBuffer] = []
    try:
        data = pickle.dumps(value, protocol=5, buffer_callback=buffers.append)
    except (pickle.PicklingError, TypeError, AttributeError):
        data = cloudpickle.dumps(value, protocol=5, buffer_callback=buffers.append)
    return len(data) + sum(b.raw().nbytes for b in buffers)


def _positions(store: Any, generation: int, size: int) -> list[Any]:
    """Each rank's last collective as (number, name, unix time entered); None before the rendezvous."""
    return [store.get(f"{generation}/at/{r}") for r in range(size)]


def _summary(positions: list[Any], pending: list[str] | None = None) -> str:
    """One sentence naming the ranks that hold the others up."""
    missing = [r for r, p in enumerate(positions) if p is None]
    if missing:
        why = {pending[r] for r in missing if pending and pending[r]}
        return f"rank(s) {_names(missing)} have not reached the rendezvous" + (
            f" ({'; '.join(sorted(why))})" if why else ""
        )
    newest = max(p[0] for p in positions)
    ahead = [r for r, p in enumerate(positions) if p[0] == newest]
    names = {positions[r][1] for r in ahead}
    if len(names) > 1:
        calls = ", ".join(f"rank {r} {positions[r][1]}" for r in ahead)
        return f"the ranks call different collectives as #{newest}: {calls}"
    behind = [r for r in range(len(positions)) if r not in ahead]
    if not behind:
        return f"every rank is in collective #{newest} ({names.pop()}); none is behind"
    now = time.time()
    last = "; ".join(
        f"rank {r} last entered #{positions[r][0]} ({positions[r][1]}, {now - positions[r][2]:.0f} s ago)"
        for r in behind
    )
    return f"{last}; ranks {_names(ahead)} wait in #{newest} ({names.pop()})"


def _names(ranks: list[int]) -> str:
    return ", ".join(map(str, ranks))


def _explain(gang: str, generation: int, size: int, pending: list[str] | None = None) -> dict[str, Any]:
    from ._store import Store

    positions = _positions(Store(f"gang/{gang}"), generation, size)
    ranks = [
        {"rank": r, "collective": p[0], "op": p[1], "entered": p[2]}
        if p is not None
        else {"rank": r, "collective": None, "pending": pending[r] if pending else ""}
        for r, p in enumerate(positions)
    ]
    return {"generation": generation, "ranks": ranks, "summary": _summary(positions, pending)}


def _wanted(v: Any) -> int | Auto:
    """``ranks`` as an int, or a checked ch.auto with min and max filled in."""
    if not isinstance(v, Auto):
        return int(v)
    lo, hi = (1 if v.min is None else v.min), (v.initial if v.max is None else v.max)
    for name, x in (("initial", v.initial), ("min", lo), ("max", hi)):
        if x is not None and (isinstance(x, bool) or not isinstance(x, int) or x < 1):
            raise ValueError(f"ranks=ch.auto: {name} must be an integer >= 1, got {x!r}")
    if hi is None:
        raise ValueError("ranks=ch.auto(...) needs max= (or an initial size)")
    if lo > hi or (v.initial is not None and not lo <= v.initial <= hi):
        raise ValueError(f"ranks=ch.auto: need min <= initial <= max; got initial={v.initial}, min={lo}, max={hi}")
    return Auto(v.initial, lo, hi)


def _fits(opts: dict[str, Any]) -> int:
    """How many ranks of this shape the live peers can hold (CPUs, and GPUs if the ranks need them)."""
    from ._cluster import cluster

    cpus, gpus = opts.get("cpus") or 1.0, _ledger.whole_devices(float(opts.get("gpus") or 0.0))
    cpus = (cpus.initial or 1.0) if isinstance(cpus, Auto) else float(cpus)
    total = 0
    for n in cluster().nodes():
        if n.alive and not n.draining:  # as _ledger.plan: a draining peer takes no rank
            fit = math.floor(n.cpus / cpus + 1e-9)
            total += min(fit, len(n.gpus) // gpus) if gpus else fit
    return total


# How long a supervisor waits for its peer to see the peer of a lost rank go.
PEER_LOSS_WAIT = 15.0


def _await_peer_loss(node: str | None) -> None:
    """A rank was lost on `node`. If its peer died, another peer may have
    noticed first: wait (up to PEER_LOSS_WAIT) until this one counts it gone
    too, so the next generation is not sized to fit on it. A worker that
    died on a healthy peer costs the wait, and the peer still counts."""
    from ._cluster import cluster

    deadline = time.monotonic() + PEER_LOSS_WAIT
    while node and time.monotonic() < deadline and any(n.peer == node and n.alive for n in cluster().nodes()):
        time.sleep(0.25)


def _size(want: int | Auto, opts: dict[str, Any], first: bool) -> tuple[int, str]:
    """This generation's rank count, and why (empty for a fixed count)."""
    if not isinstance(want, Auto):
        return want, ""
    fits = _fits(opts)
    size = max(want.min, min(want.initial if first and want.initial else want.max, fits))
    return (size, f"{size} ranks: the live peers hold {fits} of this shape (min {want.min}, max {want.max})")


def _ended(store: Any, gang: str, generation: int) -> GangFailed | None:
    """Why a rank of `generation` must stop, if it must: a rank gave the
    generation up (``_abandon``), or its successor replaced it."""
    record = store.get(f"{generation + 1}/record")
    if record is None:
        return None
    if "lost" in record:
        return GangFailed(f"gang {gang}: generation {generation} is lost: {record['lost']}")
    return GangFailed(
        f"gang {gang}: generation {generation} was replaced by generation {generation + 1}; this rank is stale"
    )


def _fence(store: Any, gang: str, generation: int) -> None:
    """Raise in a rank of a generation that was given up or replaced: it must
    not go on, or join (or wait for) the next one."""
    ended = _ended(store, gang, generation)
    if ended is not None:
        raise ended


def _abandon(store: Any, gang: str, generation: int, why: str) -> GangFailed:
    """Give `generation` up for `why`, unless a rank did first or a successor
    replaced it: the successor's record slot holds the reason until that
    generation's own record replaces it, so every rank raises the same error
    at its next collective or ``torch()``. Returns that error."""
    try:
        store.put(f"{generation + 1}/record", {"lost": why}, if_version=0, ttl=TTL)
    except VersionConflict:
        pass  # given up already, or replaced
    return _ended(store, gang, generation) or GangFailed(f"gang {gang}: generation {generation} is lost: {why}")


def _publish(store: Any, generation: int, addr: str, port: int, serving: bool) -> None:
    """Commit rank 0's endpoint to the generation's record as its next
    version: bound (held for its code) or `serving` torch's store."""
    key = f"{generation}/record"
    while True:
        record, version = store.get_versioned(key)
        n = record["endpoint"][2] + 1 if record["endpoint"] else 1
        try:
            store.put(key, {**record, "endpoint": (addr, port, n, serving)}, if_version=version, ttl=TTL)
            return
        except VersionConflict:
            continue


def _serving(store: Any, gang: str, generation: int, timeout: float) -> tuple[str, int]:
    """The endpoint rank 0 committed as serving torch's store, once it has;
    past `timeout` (the collective_timeout) the generation is lost."""
    from ._future import waiting

    deadline, pause = time.monotonic() + timeout, 0.01
    while True:
        endpoint = store.get(f"{generation}/record")["endpoint"]
        if endpoint[3]:
            return endpoint[0], endpoint[1]
        _fence(store, gang, generation)
        if time.monotonic() > deadline:
            why = f"rank 0 started no torch store in {timeout:g} s (collective_timeout)"
            raise _abandon(store, gang, generation, why)
        with waiting():
            time.sleep(pause)
        pause = min(pause * 2, 0.2)


# Rank 0's endpoints, bound from when they are committed until the rank's
# code starts (`_hand_off`), so nothing else takes the port meanwhile.
_listeners: dict[tuple[str, int], socket.socket] = {}


def _hand_off(gang: str, generation: int) -> None:
    """Free rank 0's held port for its code (torch binds it)."""
    sock = _listeners.pop((gang, generation), None)
    if sock is not None:
        sock.close()


def _host_addr() -> str:
    """An address other hosts reach this one at."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))  # no packet is sent; picks the outbound interface
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def _record(
    store: Any,
    generation: int,
    peers: Sequence[str | None],
    timeout: float,
    collective: float = COLLECTIVE_S,
    keys: Sequence[bytes] = (),
) -> dict[str, Any] | None:
    """Decide a generation, once: its ranks' peers, their calls' keys (so a
    supervisor's retry can finish or undo the launch), its deadlines (to
    meet, and in each collective), and rank 0's endpoint once it adds it.
    Its successor's record replacing it makes every rank of it stale.
    Returns None, or the record another writer decided this generation with
    first (it replaces only the slot's marker of a lost predecessor)."""
    record = {"ranks": list(peers), "timeout": timeout, "collective_timeout": collective, "endpoint": None}
    record["keys"] = [k.hex() for k in keys]
    while True:
        current, version = store.get_versioned(f"{generation}/record")
        if current is not None and "lost" not in current:
            return current
        try:
            store.put(f"{generation}/record", record, if_version=version, ttl=TTL)
            return None
        except VersionConflict:
            continue


def _rank_key(run: bytes, supervisor: bytes, generation: int, rank: int) -> bytes:
    """A rank's call key: the same in every attempt of its supervisor (whose
    call counter starts over), and different for each generation."""
    return run + child_id(supervisor, [run, b"gang-rank", generation.to_bytes(8, "little"), rank.to_bytes(4, "little")])


def _rendezvous(gang: str, generation: int, rank: int, world: int, collective: float = COLLECTIVE_S) -> dict[str, str]:
    """Meet the other ranks of this generation, within its record's deadline;
    return this rank's environment. Rank 0 binds the endpoint it commits and
    holds it until its code starts (`_hand_off`). A record the members
    create waits `collective` seconds in each collective."""
    from . import _context
    from ._devices import devices
    from ._store import Store

    store = Store(f"gang/{gang}")
    while True:
        record, version = store.get_versioned(f"{generation}/record")
        if record is not None and "lost" not in record:
            break
        # A group's generation named by its members (ch.dist.GroupHandle):
        # the first creates it, over why its predecessor was given up.
        try:
            record = {
                "ranks": [None] * world, "timeout": RENDEZVOUS_S, "collective_timeout": collective, "endpoint": None
            }
            store.put(f"{generation}/record", record, if_version=version, ttl=TTL)
            break
        except VersionConflict:
            continue
    _fence(store, gang, generation)
    node = _context.context().node
    store.put(f"{generation}/at/{rank}", (0, "rendezvous", time.time()), ttl=TTL)
    if rank == 0:
        sock = socket.socket()
        sock.bind(("", 0))
        _listeners[(gang, generation)] = sock
        _publish(store, generation, _host_addr(), sock.getsockname()[1], serving=False)
    store.put(f"{generation}/node/{rank}", node, ttl=TTL)  # the supervisor's: where a lost rank ran
    # This rank's row of the rank table (`Group.table`), under its peer, so a
    # rank reads only its own peer's rows: one read however large the gang.
    granted = devices()
    row = {"rank": rank, "node": node, "devices": [d["id"] for d in granted], "visible": [d["index"] for d in granted]}
    store.put(f"{generation}/host/{node}/{rank:06d}", row, ttl=TTL)
    timeout = record["timeout"]
    fenced = [time.monotonic()]

    def fence() -> None:  # at most once a second: a gang's ranks all poll at once
        if time.monotonic() - fenced[0] >= 1.0:
            fenced[0] = time.monotonic()
            _fence(store, gang, generation)

    try:
        store.barrier(f"{generation}/start", world).wait(timeout, check=fence)
    except TimeoutError:
        missing = [r for r, p in enumerate(_positions(store, generation, world)) if p is None]
        raise InfrastructureFailure(
            f"gang rendezvous: rank(s) {_names(missing)} did not arrive within {timeout:g} s"
        ) from None
    here = [v["rank"] for _, v in store.scan(f"{generation}/host/{node}/")]  # in rank order
    addr, port = store.get(f"{generation}/record")["endpoint"][:2]
    env = {
        "RANK": str(rank),
        "WORLD_SIZE": str(world),
        "LOCAL_RANK": str(here.index(rank)),
        "LOCAL_WORLD_SIZE": str(len(here)),
        "MASTER_ADDR": addr,
        "MASTER_PORT": str(port),
        "CHIMAERA_GANG": gang,
        "CHIMAERA_GANG_GENERATION": str(generation),
    }
    return env


@remote(max_attempts=1)
def _rank(fn: Any, gang: str, generation: int, rank: int, world: int, args: tuple, kwargs: dict) -> Any:
    from . import _context

    try:
        env = _rendezvous(gang, generation, rank, world)
    finally:
        _hand_off(gang, generation)
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)  # read by torch, JAX and DeepSpeed at init
    ctx = _context.context()
    ctx.gang = group = Group(env)
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        lost = _torch_failure(e, group)
        if lost is None:
            raise
        raise lost from e
    finally:
        _end_torch_group()
        ctx.gang = None
        for k, v in saved.items():  # the worker is reused by other calls
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _require(opts: dict[str, Any]) -> dict[str, str]:
    placement = opts.get("placement")
    return dict(placement.require) if placement else {}


def _pinned(opts: dict[str, Any], peer: str) -> dict[str, Any]:
    """Rank options that place it on `peer` (where its slot is held)."""
    placement = opts.get("placement") or Placement()
    return {**opts, "placement": Placement(require={**placement.require, "node": peer}, prefer=placement.prefer)}


def _stages(store: Any, generation: int, futures: list[Future[Any]], peers: list[str]) -> str:
    """Where each rank that has not reached the rendezvous is: queued on its
    peer (and why it waits) or started."""
    from ._runtime import Runtime

    client, out = Runtime.get().client, []
    for r, at in enumerate(_positions(store, generation, len(futures))):
        if at is None:
            why = "" if futures[r].done() else str(client.explain(futures[r].key).get("pending_reason") or "")
            stage = why if why not in ("", "running") else "started, not at the rendezvous yet"
            stage = "ended" if futures[r].done() else stage
            out.append(f"rank {r} (peer {peers[r][:12]}): {stage}")
    return "; ".join(out)


@remote(cpus=0.01, latency="interactive")
def _supervise(
    fn: Any,
    args: tuple,
    kwargs: dict,
    want: int | Auto,
    attempts: int,
    rank_opts: dict,
    timeout: float,
    collective: float = COLLECTIVE_S,
) -> list[Any]:
    """Run a function gang to its end: admit each generation's ranks all or
    none, start them on their slots and wait for them; when one is lost (or a
    rank misses the rendezvous deadline, or the ranks give the generation up
    after a collective outlasts `collective` seconds), stop the others and
    start the next generation (resized if elastic), up to `attempts`. A
    rank's own error fails the gang. Each generation is reported, for
    ``Gang.generation``, ``decisions`` and ``explain``.

    If the supervisor itself is lost with its peer, its retry carries on: the
    gang is named by this call, its generations are counted in the
    replicated store, and each generation's record holds its ranks' call
    keys. A retry supervises the generation the lost supervisor decided, its
    ranks started already running on (each is started once, under its key);
    a generation given up, or replaced, is followed by the next."""
    from ._context import context
    from ._future import wait
    from ._runtime import Runtime
    from ._store import Store

    ctx, rt = context(), Runtime.get()
    gang = blake3_digest(ctx.run + ctx.task)[:8].hex()
    store = Store(f"gang/{gang}")
    generations = store.counter("generation")
    owner = f"{rt.tenant}/gang/{gang}"
    # A retry after this supervisor was lost: the generation it decided runs
    # on, so carry on supervising it.
    generation = generations.value() if ctx.attempt > 1 else 0
    decided = store.get(f"{generation}/record") if generation else None
    if decided is not None and ("keys" not in decided or _ended(store, gang, generation) is not None):
        decided = None
    while True:
        if decided is not None:
            size, why = len(decided["keys"]), "resumed after its supervisor was lost"
            holds = _ledger.Holds(owner, generation, decided["ranks"], [_ledger.shape(rank_opts)] * size, _ledger.TTL)
            holds.resume()  # slots its ranks took stay theirs; one not taken yet is refused if its hold expired
            keys, decided = [bytes.fromhex(k) for k in decided["keys"]], None
        else:
            generation = generations.add(1)
            # Ranks of the last generation still waiting in a collective, or
            # never started, hold CPUs: end them before the next.
            for k in (store.get(f"{generation - 1}/record") or {}).get("keys", []) if generation > 1 else []:
                rt.client.cancel(bytes.fromhex(k), True)
            size, why = _size(want, rank_opts, first=generation == 1)
            holds = _admit(ctx, gang, generation, size, rank_opts)
            keys = [_rank_key(ctx.run, ctx.task, generation, r) for r in range(size)]
            decided = _record(store, generation, holds.peers, timeout, collective, keys)
            if decided is not None:  # another attempt decided this generation first: supervise its ranks
                holds.release()
                decided = decided if "keys" in decided else None  # a record without them: go on to the next
                continue
        try:
            call = (fn, gang, generation)
            futures = [_launch(*call, r, size, args, kwargs, rank_opts, holds, keys[r]) for r in range(size)]
            report = {"gang": gang, "generation": generation, "size": size, "ranks": [f.key.hex() for f in futures]}
            ctx.report(**report, **({"decision": f"generation {generation}: {why}"} if why else {}))
            lost: BaseException | None = None
            lost_rank, arrived = -1, False
            try:
                deadline, pending = time.monotonic() + timeout, futures
                while pending:
                    done, pending = wait(pending, num_returns=1, timeout=None if arrived else 0.25)
                    for f in done:
                        lost_rank = futures.index(f)
                        f.result()  # raises the rank's error
                    arrived = arrived or sum(1 for _ in store.keys(f"{generation}/at/")) >= size
                    if not arrived and time.monotonic() > deadline:
                        raise InfrastructureFailure(f"not every rank reached the rendezvous within {timeout:g} s")
            except InfrastructureFailure as e:
                where = "" if arrived else f"; {_stages(store, generation, futures, holds.peers)}"
                lost = InfrastructureFailure(f"{e}{where}") if where else e
                if not isinstance(e, GangFailed):  # a rank that gave its generation up lives on: no peer to wait for
                    _await_peer_loss(store.get(f"{generation}/node/{lost_rank}"))
            except BaseException:
                for f in futures:
                    f.cancel(force=True)  # the others may be blocked in a collective with it
                raise
            if lost is None:
                return [f.result() for f in futures]
            for f in futures:
                f.cancel(force=True)
            if generation >= attempts:
                if isinstance(lost, GangFailed):
                    raise lost  # it names the gang, the generation and why
                raise GangFailed(f"gang {gang}: generation {generation} lost a rank: {lost}")
        finally:
            holds.release()


def _admit(ctx: Any, gang: str, generation: int, size: int, rank_opts: dict) -> Any:
    """Hold a slot for every rank of the generation (all or none), reporting
    each new reason the gang waits."""
    from ._runtime import Runtime

    waits: list[str] = []

    def note(reason: str) -> None:
        if reason not in waits[-1:]:  # one report per new reason
            waits.append(reason)
            ctx.report(gang=gang, generation=generation, ranks=[], admission=reason)

    holds = _ledger.admit(
        f"{Runtime.get().tenant}/gang/{gang}",
        generation,
        [_ledger.shape(rank_opts)] * size,
        name=lambda r: f"rank {r} of {size}",
        require=_require(rank_opts),
        note=note,
    )
    assert holds is not None  # admit returns None only when told to stop
    return holds


def _launch(
    fn: Any,
    gang: str,
    generation: int,
    rank: int,
    world: int,
    args: tuple,
    kwargs: dict,
    rank_opts: dict,
    holds: Any,
    key: bytes,
) -> Future[Any]:
    """Start a rank on its peer, with its slot's launch token, under its
    recorded key: a rank started already is not started again."""
    from ._remote import CALL_FUNCTION, Runtime, submit_call

    view = _rank.options(**_pinned(rank_opts, holds.peers[rank]))
    call = (fn, gang, generation, rank, world, args, kwargs)
    extra = {"launch": holds.tokens[rank]}
    eff = view.policy()
    return submit_call(Runtime.get(), _rank._fn, call, {}, kind=CALL_FUNCTION, eff=eff, policy_extra=extra, key=key)


class Gang(Future[list[R]]):
    """A function gang's future: every rank's value, in rank order. Wait for it,
    ``await`` it, or pass it to another call, as any future. ``generation``,
    ``size`` and ``decisions`` say how it ran: an elastic gang's size per
    generation, and why. Cancelling it stops every rank."""

    __slots__ = ()

    def _latest(self, field: str) -> Any:
        return next((m[field] for m in reversed(self.metrics()) if field in m), None)

    @property
    def generation(self) -> int:
        return self._latest("generation") or 0

    @property
    def size(self) -> int:
        return self._latest("size") or 0

    @property
    def decisions(self) -> list[str]:
        return [m["decision"] for m in self.metrics() if "decision" in m]

    def explain(self) -> dict[str, Any]:
        """The call's state, and where each rank of the current generation is:
        its last collective, or why it has not started; ``summary`` names the
        ranks holding the others up."""
        info = super().explain()
        gang, ranks = self._latest("gang"), self._latest("ranks")
        if gang and ranks:
            pending = [str(self._rt.client.explain(bytes.fromhex(k)).get("pending_reason") or "") for k in ranks]
            info.update(_explain(gang, self.generation, len(ranks), pending))
        else:
            waits = self._latest("admission")
            summary = f"waiting for every rank's slot to be free: {waits}" if waits else "starting: no rank placed yet"
            info.update(generation=self.generation, ranks=[], summary=summary)
        return info

    def result(self, timeout: float | None = None) -> list[R]:
        """Every rank's value. A timeout names the ranks holding the others up."""
        try:
            return super().result(timeout)
        except TimeoutError:
            if self.done():
                raise  # a rank's own TimeoutError, not this wait's
            try:
                held = f": {self.explain()['summary']}"
            except Exception:  # noqa: BLE001 -- the timeout is the error; the explanation is extra
                held = ""
            raise TimeoutError(f"the gang did not finish within {timeout} s{held}") from None

    def __repr__(self) -> str:
        state = "done" if self.done() else f"generation {self.generation}"
        return f"<Gang {self.key[16:].hex()[:12]} {self.size} ranks, {state}>"


def start(fn: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Gang[Any]:
    """``fn.options(ranks=...).gang(*args)``: the gang's supervisor, as a call."""
    from ._remote import CALL_FUNCTION, Runtime, _site, submit_call

    from ._future import handles, put

    def opaque(a: Any) -> Any:
        # The supervisor never loads the arguments (only each rank does, as
        # its own call's): it holds a handle to each, stored once.
        return handles(a if isinstance(a, Future) else put(a))

    eff = fn.policy()
    rank_opts = {k: v for k, v in eff.values.items() if k in _RANK_OPTIONS and eff.sources.get(k) != "default"}
    held = tuple(opaque(a) for a in args), {k: opaque(v) for k, v in kwargs.items()}
    timeout = float(eff.get("rendezvous_timeout") or RENDEZVOUS_S)
    collective = float(eff.get("collective_timeout") or COLLECTIVE_S)
    want, attempts = _wanted(eff.get("ranks")), int(eff.get("max_attempts") or 3)
    supervisor = (fn, *held, want, attempts, rank_opts, timeout, collective)
    rt = Runtime.get()
    # The supervisor runs on this program's own peer. Elsewhere, losing its
    # peer would leave its retry queued behind the ranks it has to cancel,
    # which hold every CPU of a full cluster; this peer lives as long as the
    # program that waits for the gang.
    here = _supervise.options(placement=Placement(require={"node": bytes(rt.info["peer"]).hex()}))
    return submit_call(rt, _supervise._fn, supervisor, {}, kind=CALL_FUNCTION, eff=here.policy(), site=_site(3), future_cls=Gang)


@remote
class _RankHost:
    """One rank of an actor gang: meets the others, sets the rank's environment
    for good (this process is the rank's own), then builds the user's object."""

    def __init__(self, cls: type, gang: str, generation: int, rank: int, world: int, args: tuple, kwargs: dict) -> None:
        from . import _context

        try:
            env = _rendezvous(gang, generation, rank, world)
        finally:
            _hand_off(gang, generation)
        os.environ.update(env)
        self.group = Group(env)
        _context.context().gang = self.group
        self.obj = cls(*args, **kwargs)

    def call(self, method: str, args: tuple, kwargs: dict) -> Any:
        from . import _context

        _context.context().gang = self.group
        try:
            return getattr(self.obj, method)(*args, **kwargs)
        except Exception as e:
            lost = _torch_failure(e, self.group)
            if lost is None:
                raise
            raise lost from e


@remote(cpus=0.01)
def _collect(*values: Any) -> list[Any]:
    return list(values)


class _RankMethod:
    def __init__(self, hosts: list[Any], name: str, eff: Effective) -> None:
        self._hosts, self._name, self._eff = hosts, name, eff

    def remote(self, *args: Any, **kwargs: Any) -> Future[list[Any]]:
        """Call the method on every rank; one future of the results, in rank order."""
        calls = (with_replay(h.call, self._eff).remote(self._name, args, kwargs) for h in self._hosts)
        return _collect.remote(*calls)


class _Rank:
    """One rank of an actor gang: ``g[i].method.remote(...)`` calls only it."""

    def __init__(self, host: Any, options: Any) -> None:
        self._host, self._options = host, options

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        host, eff = self._host, self._options(name)

        class _One:
            def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
                return with_replay(host.call, eff).remote(name, args, kwargs)

        return _One()


class GangActor:
    """``Class.options(ranks=n).gang(*args)``: n instances of a class, one per
    rank, built together with the rendezvous environment (so ``__init__`` can
    set up ``torch.distributed``). ``g.method.remote(...)`` runs on every rank
    and gives one future of the list of results; ``g[i]`` is one rank.

    A lost rank breaks the gang: its calls raise ``ch.ActorDied``. So does a
    collective that outlasts ``collective_timeout``: every rank's collectives
    raise the same ``GangFailed``. Then ``restart()`` builds every rank again
    as the next generation (restore from your checkpoint in ``__init__``), and
    ``resize(n)`` does so with n ranks.

    Each generation is admitted all or none, as a function gang's: building
    one waits until the peers hold a slot for every rank.
    """

    def __init__(self, cls_view: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        eff = cls_view.policy()
        self._cls = cls_view._cls
        self._methods = {n for n in dir(self._cls) if not n.startswith("_") and callable(getattr(self._cls, n))}
        rank_opts = {k: v for k, v in eff.values.items() if k in _RANK_OPTIONS and eff.sources.get(k) != "default"}
        self._rank_opts = rank_opts
        self._args, self._kwargs = args, kwargs
        self.id = blake3_digest(os.urandom(16))[:8].hex()
        self.generation = 0
        self._timeout = float(eff.get("rendezvous_timeout") or RENDEZVOUS_S)
        self._collective_timeout = float(eff.get("collective_timeout") or COLLECTIVE_S)
        self._hosts: list[Any] = []
        self._holds: _ledger.Holds | None = None
        # A rank's call of a method runs as one that may not run again when
        # the method, with the class's options, may not (`with_replay`).
        self._eff = eff
        self._method_options: dict[str, Effective] = {}
        self._want = _wanted(eff.get("ranks"))
        self.decisions: list[str] = []
        self._build(self._sized(first=True))

    def _sized(self, first: bool) -> int:
        size, why = _size(self._want, self._rank_opts, first)
        if why:
            self.decisions.append(f"generation {self.generation + 1}: {why}")
        return size

    def _build(self, size: int) -> None:
        from ._remote import Runtime, _create_actor
        from ._store import Store

        self.generation += 1
        rt = Runtime.get()
        # An actor holds only the CPUs it asks for: so does its slot.
        holds = _ledger.admit(
            f"{rt.tenant}/gang/{self.id}",
            self.generation,
            [_ledger.shape(self._rank_opts, cpus=0.0)] * size,
            name=lambda r: f"rank {r} of {size}",
            require=_require(self._rank_opts),
        )
        assert holds is not None  # admit returns None only when told to stop
        self._holds = holds
        try:
            _record(Store(f"gang/{self.id}"), self.generation, holds.peers, self._timeout, self._collective_timeout)
            for r in range(size):
                view = _RankHost.options(**_pinned(self._rank_opts, holds.peers[r]))
                ctor = (self._cls, self.id, self.generation, r, size, self._args, self._kwargs)
                self._hosts.append(_create_actor(rt, view._cls, view.policy(), ctor, {}, {"launch": holds.tokens[r]}))
        except BaseException:
            self.close()  # the ranks built so far end, and the slots not taken are free again
            raise

    def __len__(self) -> int:
        return len(self._hosts)

    def __getitem__(self, rank: int) -> _Rank:
        return _Rank(self._hosts[rank], self._options)

    def __getattr__(self, name: str) -> _RankMethod:
        if name.startswith("_") or name not in self._methods:
            raise AttributeError(f"{self._cls.__name__} has no method {name!r}")
        return _RankMethod(self._hosts, name, self._options(name))

    def _options(self, name: str) -> Effective:
        """Method `name`'s options, as a handle's call resolves them, once."""
        eff = self._method_options.get(name)
        if eff is None:
            eff = self._method_options[name] = method_options(self._cls, name, self._eff)
        return eff

    def restart(self) -> None:
        """Build every rank again as the next generation (an elastic gang sizes it
        to what the live peers hold now)."""
        self.resize(self._sized(first=False) if isinstance(self._want, Auto) else len(self._hosts))

    def resize(self, size: int) -> None:
        """Rebuild the gang with `size` ranks (a new generation)."""
        if size < 1:
            raise ValueError("a gang needs at least one rank")
        self.close()
        self._build(size)

    def close(self) -> None:
        for h in self._hosts:
            h.close()
        self._hosts = []
        if self._holds is not None:
            self._holds.release()

    def explain(self) -> dict[str, Any]:
        """Where each rank is (its last collective); ``summary`` names the ranks holding the others up."""
        return _explain(self.id, self.generation, len(self._hosts))

    def __enter__(self) -> GangActor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<GangActor {self._cls.__name__} {len(self._hosts)} ranks, generation {self.generation}>"


# The groups this process's actors have joined: (key, generation, actor) -> Group,
# with one lock per entry so a slow rendezvous never holds up another group's.
_joined: dict[tuple[str, int, str], Group] = {}
_joining: dict[tuple[str, int, str], threading.Lock] = {}
_joining_lock = threading.Lock()


@dataclass(frozen=True)
class GroupHandle:
    """Actors made one collective group by ``ch.dist.group``. Pass it to them; inside
    a member's method, ``join()`` is that member's ``Group`` (rank in the order
    given to ``ch.dist.group``). The first ``join()`` in each member waits for all
    of them, as a gang's rendezvous does."""

    key: str
    generation: int
    members: tuple[str, ...]
    collective_timeout: float = COLLECTIVE_S  # for a generation its members create

    def join(self) -> Group:
        from . import _context

        ctx = _context.context()
        me = ctx.actor_id if ctx is not None else None
        if me not in self.members:
            raise ValueError("join() runs inside a method of one of the group's actors")
        entry = (self.key, self.generation, me)
        with _joining_lock:
            lock = _joining.setdefault(entry, threading.Lock())
        with lock:  # one rendezvous per member, even with concurrent calls
            joined = _joined.get(entry)
            if joined is None:
                gang = f"group-{self.key}"
                try:
                    rank, world = self.members.index(me), len(self.members)
                    env = _rendezvous(gang, self.generation, rank, world, self.collective_timeout)
                finally:
                    _hand_off(gang, self.generation)
                joined = _joined[entry] = Group(env)
            return joined

    def explain(self) -> dict[str, Any]:
        """Where each member is (its last collective); ``summary`` names the ones holding the others up."""
        return _explain(f"group-{self.key}", self.generation, len(self.members))


def group(*actors: Any, name: str | None = None, collective_timeout: float = COLLECTIVE_S) -> GroupHandle:
    """Make `actors` one collective group (ranks in the order given). Calling it
    again with the same ``name`` starts a new generation, for a changed set.
    A member waits in one collective at most ``collective_timeout`` seconds;
    past it the generation is lost, and every member's collectives raise
    ``GangFailed`` until a new generation."""
    from ._store import Store

    collective = _pos_float("collective_timeout", collective_timeout)

    if not actors:
        raise ValueError("ch.dist.group needs at least one actor")
    bad = [a for a in actors if not isinstance(a, Actor)]
    if bad:
        raise TypeError(f"ch.dist.group takes actors (from Class.remote()), got {bad[0]!r}")
    ids = tuple(a.id for a in actors)
    if len(set(ids)) != len(ids):
        raise ValueError("ch.dist.group: an actor appears more than once")
    key = name or os.urandom(8).hex()
    store = Store(f"gang/group-{key}")
    generation = store.counter("declared").add(1)
    _record(store, generation, [None] * len(ids), RENDEZVOUS_S, collective)
    return GroupHandle(key, generation, ids, collective)
