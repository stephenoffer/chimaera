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

shortcut: ranks meet through a ch.Store rendezvous and are placed one by one,
so a gang larger than the free capacity waits at the rendezvous (and retries
after ``RENDEZVOUS_S``). All-or-nothing placement arrives with the shard's
gang commit (plan P5).
"""

from __future__ import annotations

import os
import socket
import sys
import threading
from collections.abc import Iterator
from typing import Any, Generic, TypeVar

from ._errors import InfrastructureFailure
from ._future import Future
from ._native import blake3_digest
from ._remote import remote

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
        self._seq = 0

    def __repr__(self) -> str:
        return f"<Group rank {self.rank}/{self.size} generation {self.generation}>"

    @property
    def backend(self) -> str:
        """The ``torch.distributed`` backend of this rank's devices (``gloo`` on CPU)."""
        from ._devices import devices

        granted = devices()
        return granted[0]["backend"] if granted else "gloo"

    def torch(self, backend: str | None = None) -> Any:
        """``torch.distributed``, its default process group initialized for this
        gang (the rank's call tears it down when it returns)."""
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

    # Small-object collectives through the store: no torch needed.

    def _round(self) -> tuple[Any, str]:
        from ._store import Store

        self._seq += 1
        return Store(f"gang/{self.id}"), f"{self.generation}/c{self._seq}"

    def barrier(self, timeout: float | None = None) -> None:
        store, key = self._round()
        store.barrier(key, self.size).wait(timeout)

    def all_gather(self, obj: Any, timeout: float | None = None) -> list[Any]:
        """Every rank's `obj`, in rank order (picklable values up to 1 MiB)."""
        store, key = self._round()
        store.put(f"{key}/{self.rank}", obj, ttl=TTL)
        store.barrier(key, self.size).wait(timeout)
        return [store.get(f"{key}/{r}") for r in range(self.size)]

    def broadcast(self, obj: Any = None, src: int = 0, timeout: float | None = None) -> Any:
        """Rank `src`'s `obj`, on every rank."""
        store, key = self._round()
        if self.rank == src:
            store.put(f"{key}/{src}", obj, ttl=TTL)
        store.barrier(key, self.size).wait(timeout)
        return store.get(f"{key}/{src}")


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


class Gang(Generic[R]):
    """A running gang: ``result()`` is every rank's return value, in rank order."""

    def __init__(self, fn: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        eff = fn.policy()
        self.size = int(eff.get("ranks"))
        self._attempts = int(eff.get("max_attempts") or 3)
        rank_opts = {k: v for k, v in eff.values.items() if k in _RANK_OPTIONS and eff.sources.get(k) != "default"}
        self._submit = _rank.options(**rank_opts) if rank_opts else _rank
        self._fn, self._args, self._kwargs = fn, args, kwargs
        self.id = blake3_digest(os.urandom(16))[:8].hex()
        self.generation = 0
        self.futures: list[Future[R]] = []
        self._done = threading.Event()
        self._result: list[R] | None = None
        self._error: BaseException | None = None
        self._cancelled = False
        self._start()
        threading.Thread(target=self._supervise, name=f"gang-{self.id}", daemon=True).start()

    def _start(self) -> None:
        self.generation += 1
        self.futures = [
            self._submit.remote(self._fn, self.id, self.generation, r, self.size, self._args, self._kwargs)
            for r in range(self.size)
        ]

    def _supervise(self) -> None:
        from ._future import as_completed

        while True:
            lost: BaseException | None = None
            try:
                for f in as_completed(self.futures):
                    f.result()  # raises the rank's error
            except InfrastructureFailure as e:
                lost = e
            except BaseException as e:  # a rank's own error, or the gang was cancelled
                self._finish(error=e)
                return
            if lost is None:
                self._finish(result=[f.result() for f in self.futures])
                return
            # Force: the other ranks may be blocked in a collective with the lost one.
            for f in self.futures:
                f.cancel(force=True)
            if self._cancelled or self.generation >= self._attempts:
                self._finish(error=GangFailed(f"gang {self.id}: generation {self.generation} lost a rank: {lost}"))
                return
            self._start()

    def _finish(self, result: list[R] | None = None, error: BaseException | None = None) -> None:
        if error is not None:
            for f in self.futures:
                f.cancel(force=True)
        self._result, self._error = result, error
        self._done.set()

    def result(self, timeout: float | None = None) -> list[R]:
        if not self._done.wait(timeout):
            raise TimeoutError(f"gang {self.id} did not finish within {timeout} s")
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result

    def cancel(self) -> None:
        self._cancelled = True
        for f in self.futures:
            f.cancel(force=True)

    def __iter__(self) -> Iterator[R]:
        return iter(self.result())

    def __repr__(self) -> str:
        state = "done" if self._done.is_set() else f"generation {self.generation}"
        return f"<Gang {self.id} {self.size} ranks, {state}>"


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
        self._host = _RankHost.options(**rank_opts) if rank_opts else _RankHost
        self._args, self._kwargs = args, kwargs
        self.id = blake3_digest(os.urandom(16))[:8].hex()
        self.generation = 0
        self._hosts: list[Any] = []
        self._build(int(eff.get("ranks")))

    def _build(self, size: int) -> None:
        self.generation += 1
        self._hosts = [
            self._host.remote(self._cls, self.id, self.generation, r, size, self._args, self._kwargs) for r in range(size)
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
        """Build every rank again as the next generation."""
        self.resize(len(self._hosts))

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

    def __enter__(self) -> GangActor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<GangActor {self._cls.__name__} {len(self._hosts)} ranks, generation {self.generation}>"
