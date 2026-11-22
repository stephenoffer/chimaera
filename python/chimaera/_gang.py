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
0's host and a free port there), plus CHIMAERA_GANG and
CHIMAERA_GANG_GENERATION. If any rank is lost (its worker or peer dies), the
whole generation is cancelled and the gang starts again as a new generation,
up to ``max_attempts`` times; resume from your own checkpoint. A rank that
raises fails the gang with that error.

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

shortcut: ranks meet through a ch.coord.Store rendezvous and are placed one by one,
so a gang larger than the free capacity waits at the rendezvous (and retries
after ``RENDEZVOUS_S``). All-or-nothing placement arrives with the shard's
gang commit (plan P5).
"""

from __future__ import annotations

import functools
import math
import operator
import os
import socket
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ._errors import InfrastructureFailure
from ._future import Future
from ._native import blake3_digest
from ._options import Auto
from ._remote import Actor, remote

R = TypeVar("R")

RENDEZVOUS_S = 300.0
TTL = "24h"  # rendezvous and collective keys expire
# The rank options a gang passes on: what a rank needs, not how the gang retries.
_RANK_OPTIONS = ("cpus", "memory", "memory_limit", "gpus", "device", "resources", "placement", "priority", "deadline")


class GangFailed(InfrastructureFailure):
    """Every attempt of a gang lost a rank (``max_attempts`` generations)."""


# The gang whose torch.distributed group this worker process initialized.
_torch_group: tuple[str, int] | None = None


def _end_torch_group() -> None:
    global _torch_group
    dist = sys.modules.get("torch.distributed")
    if _torch_group is not None and dist is not None and dist.is_initialized():
        dist.destroy_process_group()
    _torch_group = None


class Group:
    """This rank's view of its gang (``ch.context().gang`` inside a rank)."""

    def __init__(self, env: dict[str, str]) -> None:
        self.rank = int(env["RANK"])
        self.size = int(env["WORLD_SIZE"])
        self.local_rank = int(env["LOCAL_RANK"])
        self.local_size = int(env["LOCAL_WORLD_SIZE"])
        self.generation = int(env["CHIMAERA_GANG_GENERATION"])
        self.id = env["CHIMAERA_GANG"]
        self.master = (env["MASTER_ADDR"], int(env["MASTER_PORT"]))
        self.ranks = list(range(self.size))  # members, as ranks of the gang (a mesh axis lists a subset)
        self._axis = ""  # set on a mesh axis
        self._seq = 0

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
            axis.rank, axis.size, axis._axis, axis._seq = coords[i], shape[i], name, 0
            out[name] = axis
        return out

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
        import torch.distributed as dist

        global _torch_group
        if dist.is_initialized() and _torch_group != (self.id, self.generation):
            dist.destroy_process_group()  # left by another gang in this reused worker
        if not dist.is_initialized():
            addr, port = self.master
            dist.init_process_group(
                backend or self.backend, init_method=f"tcp://{addr}:{port}", rank=self.rank, world_size=self.size
            )
            _torch_group = (self.id, self.generation)
        return dist

    # Small-object collectives through the store: no torch needed. Each one
    # records where this rank is, for explain().

    def _collective(self, op: str, timeout: float | None, value: Any = None, send: bool = False) -> tuple[Any, str]:
        from ._store import Store

        self._seq += 1
        store, key = Store(f"gang/{self.id}"), f"{self.generation}/c{self._seq}/{op}"
        store.put(f"{self.generation}/at/{self.rank}", (self._seq, op, time.time()), ttl=TTL)
        if send:
            store.put(f"{key}/{self.rank}", value, ttl=TTL)
        try:
            store.barrier(key, self.size).wait(timeout)
        except TimeoutError:
            held = _summary(_positions(store, self.generation, self.size))
            raise TimeoutError(f"{op} #{self._seq} of {self!r} timed out: {held}") from None
        return store, key

    def barrier(self, timeout: float | None = None) -> None:
        self._collective("barrier", timeout)

    def all_gather(self, obj: Any, timeout: float | None = None) -> list[Any]:
        """Every rank's `obj`, in rank order (picklable values up to 1 MiB)."""
        store, key = self._collective("all_gather", timeout, obj, send=True)
        return [store.get(f"{key}/{r}") for r in range(self.size)]

    def all_reduce(self, obj: Any, op: Callable[[Any, Any], Any] = operator.add, timeout: float | None = None) -> Any:
        """Every rank's `obj` combined with `op` (sum by default), on every rank."""
        store, key = self._collective("all_reduce", timeout, obj, send=True)
        return functools.reduce(op, [store.get(f"{key}/{r}") for r in range(self.size)])

    def broadcast(self, obj: Any = None, src: int = 0, timeout: float | None = None) -> Any:
        """Rank `src`'s `obj`, on every rank."""
        store, key = self._collective("broadcast", timeout, obj, send=self.rank == src)
        return store.get(f"{key}/{src}")


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

    cpus, gpus = opts.get("cpus") or 1.0, opts.get("gpus") or 0.0
    cpus = (cpus.initial or 1.0) if isinstance(cpus, Auto) else float(cpus)
    total = 0
    for n in cluster().nodes():
        if n.alive:
            fit = math.floor(n.cpus / cpus + 1e-9)
            total += min(fit, math.floor(len(n.gpus) / float(gpus) + 1e-9)) if gpus else fit
    return total


def _size(want: int | Auto, opts: dict[str, Any], first: bool) -> tuple[int, str]:
    """This generation's rank count, and why (empty for a fixed count)."""
    if not isinstance(want, Auto):
        return want, ""
    fits = _fits(opts)
    size = max(want.min, min(want.initial if first and want.initial else want.max, fits))
    return (size, f"{size} ranks: the live peers hold {fits} of this shape (min {want.min}, max {want.max})")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _host_addr() -> str:
    """An address other hosts reach this one at."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))  # no packet is sent; picks the outbound interface
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def _rendezvous(gang: str, generation: int, rank: int, world: int) -> dict[str, str]:
    """Meet the other ranks of this generation; return this rank's environment."""
    from . import _context
    from ._store import Store

    store = Store(f"gang/{gang}")
    node = _context.context().node
    store.put(f"{generation}/at/{rank}", (0, "rendezvous", time.time()), ttl=TTL)
    if rank == 0:
        store.put(f"{generation}/master", (_host_addr(), _free_port()), ttl=TTL)
    store.put(f"{generation}/node/{rank}", node, ttl=TTL)
    try:
        store.barrier(f"{generation}/start", world).wait(RENDEZVOUS_S)
    except TimeoutError:
        raise InfrastructureFailure(f"gang rendezvous: not every rank started within {RENDEZVOUS_S:.0f} s") from None
    nodes = [store.get(f"{generation}/node/{r}") for r in range(world)]
    addr, port = store.get(f"{generation}/master")
    env = {
        "RANK": str(rank),
        "WORLD_SIZE": str(world),
        "LOCAL_RANK": str(sum(1 for r in range(rank) if nodes[r] == node)),
        "LOCAL_WORLD_SIZE": str(nodes.count(node)),
        "MASTER_ADDR": addr,
        "MASTER_PORT": str(port),
        "CHIMAERA_GANG": gang,
        "CHIMAERA_GANG_GENERATION": str(generation),
    }
    return env


@remote(max_attempts=1)
def _rank(fn: Any, gang: str, generation: int, rank: int, world: int, args: tuple, kwargs: dict) -> Any:
    from . import _context

    env = _rendezvous(gang, generation, rank, world)
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)  # read by torch, JAX and DeepSpeed at init
    ctx = _context.context()
    ctx.gang = Group(env)
    try:
        return fn(*args, **kwargs)
    finally:
        _end_torch_group()
        ctx.gang = None
        for k, v in saved.items():  # the worker is reused by other calls
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@remote(cpus=0.01, latency="interactive")
def _supervise(fn: Any, args: tuple, kwargs: dict, want: int | Auto, attempts: int, rank_opts: dict) -> list[Any]:
    """Run a function gang to its end: start its ranks together; when one is
    lost, stop the others and start the next generation (resized if
    elastic), up to `attempts`. A rank's own error fails the gang. Each
    generation is reported, for ``Gang.generation``, ``decisions`` and ``explain``.

    If the supervisor itself is lost with its peer, its retry carries on: the
    gang is named by this call and its generations are counted in the
    replicated store, so the next one never reuses a rendezvous."""
    from ._context import context
    from ._future import as_completed
    from ._store import Store

    from ._runtime import Runtime

    ctx = context()
    gang = blake3_digest(ctx.run + ctx.task)[:8].hex()
    store = Store(f"gang/{gang}")
    generations = store.counter("generation")
    submit = _rank.options(**rank_opts) if rank_opts else _rank
    while True:
        generation = generations.add(1)
        # A supervisor lost with its peer left its generation's ranks waiting
        # in a collective, holding their CPUs: end them before the next.
        for k in store.get(f"ranks/{generation - 1}", []) if generation > 1 else []:
            Runtime.get().client.cancel(bytes.fromhex(k), True)
        size, why = _size(want, rank_opts, first=generation == 1)
        futures = [submit.remote(fn, gang, generation, r, size, args, kwargs) for r in range(size)]
        store.put(f"ranks/{generation}", [f.key.hex() for f in futures], ttl="24h")
        report = {"gang": gang, "generation": generation, "size": size, "ranks": [f.key.hex() for f in futures]}
        ctx.report(**report, **({"decision": f"generation {generation}: {why}"} if why else {}))
        lost: BaseException | None = None
        try:
            for f in as_completed(futures):
                f.result()  # raises the rank's error
        except InfrastructureFailure as e:
            lost = e
        except BaseException:
            for f in futures:
                f.cancel(force=True)  # the others may be blocked in a collective with it
            raise
        if lost is None:
            return [f.result() for f in futures]
        for f in futures:
            f.cancel(force=True)
        if generation >= attempts:
            raise GangFailed(f"gang {gang}: generation {generation} lost a rank: {lost}")


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
            info.update(generation=0, ranks=[], summary="starting: no rank placed yet")
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
    supervisor = (fn, *held, _wanted(eff.get("ranks")), int(eff.get("max_attempts") or 3), rank_opts)
    return submit_call(
        Runtime.get(), _supervise._fn, supervisor, {}, kind=CALL_FUNCTION, eff=_supervise.policy(), site=_site(3), future_cls=Gang
    )


@remote
class _RankHost:
    """One rank of an actor gang: meets the others, sets the rank's environment
    for good (this process is the rank's own), then builds the user's object."""

    def __init__(self, cls: type, gang: str, generation: int, rank: int, world: int, args: tuple, kwargs: dict) -> None:
        from . import _context

        env = _rendezvous(gang, generation, rank, world)
        os.environ.update(env)
        self.group = Group(env)
        _context.context().gang = self.group
        self.obj = cls(*args, **kwargs)

    def call(self, method: str, args: tuple, kwargs: dict) -> Any:
        from . import _context

        _context.context().gang = self.group
        return getattr(self.obj, method)(*args, **kwargs)


@remote(cpus=0.01)
def _collect(*values: Any) -> list[Any]:
    return list(values)


class _RankMethod:
    def __init__(self, hosts: list[Any], name: str) -> None:
        self._hosts, self._name = hosts, name

    def remote(self, *args: Any, **kwargs: Any) -> Future[list[Any]]:
        """Call the method on every rank; one future of the results, in rank order."""
        return _collect.remote(*(h.call.remote(self._name, args, kwargs) for h in self._hosts))


class _Rank:
    """One rank of an actor gang: ``g[i].method.remote(...)`` calls only it."""

    def __init__(self, host: Any) -> None:
        self._host = host

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        host = self._host

        class _One:
            def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
                return host.call.remote(name, args, kwargs)

        return _One()


class GangActor:
    """``Class.options(ranks=n).gang(*args)``: n instances of a class, one per
    rank, built together with the rendezvous environment (so ``__init__`` can
    set up ``torch.distributed``). ``g.method.remote(...)`` runs on every rank
    and gives one future of the list of results; ``g[i]`` is one rank.

    A lost rank breaks the gang: its calls raise ``ch.ActorDied``. Then
    ``restart()`` builds every rank again as the next generation (restore from
    your checkpoint in ``__init__``), and ``resize(n)`` does so with n ranks.
    """

    def __init__(self, cls_view: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        eff = cls_view.policy()
        self._cls = cls_view._cls
        self._methods = {n for n in dir(self._cls) if not n.startswith("_") and callable(getattr(self._cls, n))}
        rank_opts = {k: v for k, v in eff.values.items() if k in _RANK_OPTIONS and eff.sources.get(k) != "default"}
        self._rank_opts = rank_opts
        self._host = _RankHost.options(**rank_opts) if rank_opts else _RankHost
        self._args, self._kwargs = args, kwargs
        self.id = blake3_digest(os.urandom(16))[:8].hex()
        self.generation = 0
        self._hosts: list[Any] = []
        self._want = _wanted(eff.get("ranks"))
        self.decisions: list[str] = []
        self._build(self._sized(first=True))

    def _sized(self, first: bool) -> int:
        size, why = _size(self._want, self._rank_opts, first)
        if why:
            self.decisions.append(f"generation {self.generation + 1}: {why}")
        return size

    def _build(self, size: int) -> None:
        self.generation += 1
        self._hosts = [
            self._host.remote(self._cls, self.id, self.generation, r, size, self._args, self._kwargs)
            for r in range(size)
        ]

    def __len__(self) -> int:
        return len(self._hosts)

    def __getitem__(self, rank: int) -> _Rank:
        return _Rank(self._hosts[rank])

    def __getattr__(self, name: str) -> _RankMethod:
        if name.startswith("_") or name not in self._methods:
            raise AttributeError(f"{self._cls.__name__} has no method {name!r}")
        return _RankMethod(self._hosts, name)

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
                env = _rendezvous(f"group-{self.key}", self.generation, self.members.index(me), len(self.members))
                joined = _joined[entry] = Group(env)
            return joined

    def explain(self) -> dict[str, Any]:
        """Where each member is (its last collective); ``summary`` names the ones holding the others up."""
        return _explain(f"group-{self.key}", self.generation, len(self.members))


def group(*actors: Any, name: str | None = None) -> GroupHandle:
    """Make `actors` one collective group (ranks in the order given). Calling it
    again with the same ``name`` starts a new generation, for a changed set."""
    from ._store import Store

    if not actors:
        raise ValueError("ch.dist.group needs at least one actor")
    bad = [a for a in actors if not isinstance(a, Actor)]
    if bad:
        raise TypeError(f"ch.dist.group takes actors (from Class.remote()), got {bad[0]!r}")
    ids = tuple(a.id for a in actors)
    if len(set(ids)) != len(ids):
        raise ValueError("ch.dist.group: an actor appears more than once")
    key = name or os.urandom(8).hex()
    generation = Store(f"gang/group-{key}").counter("declared").add(1)
    return GroupHandle(key, generation, ids)
