"""Reservations (placement groups): ``ch.dist.reserve(bundles, strategy=...)``.

    res = ch.dist.reserve([{"cpus": 2, "gpus": 1}] * 4, strategy="spread").ready()
    workers = [Trainer.options(within=res[i]).remote(i) for i in range(4)]
    step.options(within=res[0]).remote(batch)

Each bundle's resources are held by a host process on one peer from
``ready()`` until ``release()``, whether or not anything runs in it. Calls
and actors created ``within=res[i]`` run inside that host, on its peer and
with its resources, so nothing else can take them. ``strategy``:
``"pack"`` (one peer when it fits), ``"spread"`` (different peers when
there are enough), and ``"strict_pack"``/``"strict_spread"`` (required, or
the reservation is infeasible).

Calls in one bundle share its host process (threads, up to the bundle's
CPUs at once); give each parallel worker its own bundle.

Every bundle is held, or none: a try that cannot hold them all within
``Reservation.HOLD_S`` (or whose holder dies) lets go and tries again after a
random pause, so
reservations competing for the same capacity take turns (a try starts only
when the peers report the CPUs free, and gives everything back if a peer ends
up over capacity). shortcut: the holds are taken by the client from the peers'
load reports, not in one shard commit, so a burst of reservations against
stale reports can briefly over-commit before backing off; atomic holds come
with the shard's gang commit (plan P5).
"""

from __future__ import annotations

import itertools
import os
import random
import threading
import time
from typing import Any

from ._errors import ChimaeraError, Infeasible
from ._future import Future, get
from ._options import Placement
from ._remote import remote

STRATEGIES = ("pack", "spread", "strict_pack", "strict_spread")


@remote
class _BundleHost:
    """Holds one bundle's resources and runs what is placed within it."""

    def __init__(self) -> None:
        self.objects: dict[int, Any] = {}
        self.ids = itertools.count()

    def node(self) -> str:
        from . import _context

        return _context.context().node

    def call(self, fn: Any, args: tuple, kwargs: dict) -> Any:
        return fn(*args, **kwargs)

    def new(self, cls: type, args: tuple, kwargs: dict) -> int:
        oid = next(self.ids)
        self.objects[oid] = cls(*args, **kwargs)
        return oid

    def invoke(self, oid: int, method: str, args: tuple, kwargs: dict) -> Any:
        return getattr(self.objects[oid], method)(*args, **kwargs)

    def drop(self, oid: int) -> None:
        self.objects.pop(oid, None)


class Bundle:
    """One bundle of a reservation: pass it as ``within=``."""

    def __init__(self, reservation: Reservation, index: int) -> None:
        self.reservation, self.index = reservation, index

    @property
    def node(self) -> str:
        """The peer (hex id) holding this bundle."""
        return self.reservation._nodes[self.index]

    def __repr__(self) -> str:
        return f"<Bundle {self.index} of {self.reservation!r}>"

    def _host(self) -> Any:
        return self.reservation._ready_hosts()[self.index]


class Reservation:
    """Resources held on peers for later calls (``ch.dist.reserve``)."""

    # One try holds some bundles for at most this long while waiting for the rest.
    HOLD_S = 15.0

    def __init__(self, bundles: list[dict[str, Any]], strategy: str, timeout: float | None) -> None:
        if strategy not in STRATEGIES:
            raise ValueError(f"strategy must be one of {', '.join(STRATEGIES)}, got {strategy!r}")
        if not bundles:
            raise ValueError("a reservation needs at least one bundle")
        self.bundles = [dict(b) for b in bundles]
        self.strategy = strategy
        self._timeout = timeout
        self.id = os.urandom(8).hex()
        self._hosts: list[Any] = []
        self._nodes: list[str] = []
        self._released = False
        self._error: BaseException | None = None
        self._done = threading.Event()
        first = self._targets()  # strict placement that can never fit fails here, at once
        threading.Thread(target=self._acquire, args=(first,), name=f"reserve-{self.id}", daemon=True).start()

    def _acquire(self, targets: list[str | None]) -> None:
        """Hold every bundle, or none: a try that cannot hold them all within
        HOLD_S lets go of what it held and tries again after a random, growing
        pause, so reservations competing for the same capacity take turns
        instead of each holding part of it forever."""
        from ._store import Store

        store = Store(f"reserve/{self.id}")
        attempt = 0
        def back_off() -> list[str | None]:
            nonlocal attempt
            attempt += 1
            time.sleep(random.uniform(0.0, min(8.0, 0.25 * 2**attempt)))
            return self._targets()

        try:
            while not self._released:
                if not self._fits(targets):  # someone else holds it: wait for a turn
                    targets = back_off()
                    continue
                hosts = [self._place(i, t) for i, t in enumerate(targets)]
                probes = [h.node.remote() for h in hosts]
                try:
                    nodes = get(probes, timeout=self.HOLD_S)
                except (TimeoutError, ChimaeraError):  # slow, or a holder died: this try failed, not the reservation
                    nodes = None
                # Another reservation may have taken the same CPUs in the meantime:
                # then neither keeps them, and the random pause picks a winner.
                if nodes is None or self._overcommitted(nodes):
                    _let_go(hosts, probes)
                    targets = back_off()
                    continue
                if self._released:  # released while this try was waiting
                    _let_go(hosts, probes)
                    return
                self._hosts, self._nodes = hosts, nodes
                store.put("nodes", nodes, ttl="24h")
                return
        except BaseException as e:  # noqa: BLE001 - e.g. infeasible bundles: ready() raises it
            self._error = e
            store.put("error", f"{type(e).__name__}: {e}", ttl="24h")
        finally:
            self._done.set()

    def __repr__(self) -> str:
        state = "released" if self._released else ("ready" if self._nodes else "pending")
        return f"<Reservation {len(self.bundles)} bundles, {self.strategy}, {state}>"

    def _targets(self) -> list[str | None]:
        """A peer for each bundle (None: wherever it fits), from the live peers' free CPUs."""
        from ._cluster import cluster

        live = [n for n in cluster().nodes() if n.alive]
        free = {n.peer: n.cpus - n.cpu_used for n in live}
        need = [float(b.get("cpus", 1)) for b in self.bundles]
        if self.strategy in ("pack", "strict_pack"):
            fits = [p for p, f in sorted(free.items(), key=lambda kv: -kv[1]) if f + 1e-9 >= sum(need)]
            if fits:
                return [fits[0]] * len(self.bundles)
            if self.strategy == "strict_pack":
                raise Infeasible(f"strict_pack: no live peer has {sum(need):g} CPUs free for all {len(need)} bundles")
            return [None] * len(self.bundles)
        order = [p for p, _ in sorted(free.items(), key=lambda kv: -kv[1])]
        if len(order) >= len(self.bundles):
            return order[: len(self.bundles)]
        if self.strategy == "strict_spread":
            raise Infeasible(f"strict_spread: {len(self.bundles)} bundles need as many peers; {len(order)} are live")
        return [order[i % len(order)] if order else None for i in range(len(self.bundles))]

    def _free(self) -> dict[str, float]:
        from ._cluster import cluster

        return {n.peer: n.cpus - n.cpu_used for n in cluster().nodes() if n.alive}

    def _fits(self, targets: list[str | None]) -> bool:
        """Whether the free CPUs (as the peers last reported) take every bundle now."""
        free, want = self._free(), {}
        for b, t in zip(self.bundles, targets, strict=True):
            want[t] = want.get(t, 0.0) + float(b.get("cpus", 1))
        anywhere = want.pop(None, 0.0)
        if any(free.get(t, 0.0) + 1e-9 < need for t, need in want.items()):
            return False
        return sum(free.values()) - sum(want.values()) + 1e-9 >= anywhere

    def _overcommitted(self, nodes: list[str]) -> bool:
        time.sleep(0.3)  # let the peers count every holder placed alongside these
        free = self._free()
        return any(free.get(n, 0.0) < -1e-9 for n in set(nodes))

    def _place(self, i: int, target: str | None) -> Any:
        b = self.bundles[i]
        opts: dict[str, Any] = {"cpus": float(b.get("cpus", 1)), "concurrency": max(1, int(b.get("cpus", 1)))}
        for k in ("gpus", "memory", "device", "resources"):
            if k in b:
                opts[k] = b[k]
        if target is not None:
            strict = self.strategy.startswith("strict")
            opts["placement"] = (
                Placement(require={"node": target}) if strict else Placement(prefer=(f"node={target}",))
            )
        return _BundleHost.options(**opts).remote()

    def ready(self, timeout: float | None = None) -> Reservation:
        """Wait until every bundle is held. On a timeout this raises and the
        reservation keeps trying (``release()`` to give up)."""
        if self._released:
            raise RuntimeError("this reservation was released")
        wait = timeout if timeout is not None else self._timeout
        if not self._done.wait(wait):
            raise TimeoutError(f"{self!r}: not every bundle is held yet; still trying (release() to give up)")
        if self._error is not None:
            raise self._error
        if self._released:
            raise RuntimeError("this reservation was released")
        return self

    def ready_future(self) -> Future[list[str]]:
        """A future of the bundles' peers, done once every bundle is held."""
        return _ready_when.remote(f"reserve/{self.id}")

    def _ready_hosts(self) -> list[Any]:
        self.ready()
        return self._hosts

    def __await__(self) -> Any:
        async def wait() -> Reservation:
            import asyncio

            return await asyncio.to_thread(self.ready)

        return wait().__await__()

    def __len__(self) -> int:
        return len(self.bundles)

    def __getitem__(self, i: int) -> Bundle:
        if not -len(self.bundles) <= i < len(self.bundles):
            raise IndexError(f"bundle {i} of a {len(self.bundles)}-bundle reservation")
        return Bundle(self, i % len(self.bundles))

    def release(self) -> None:
        """Give the resources back; what runs within it ends."""
        if not self._released:
            self._released = True
            for h in self._hosts:
                h.close()
            self._done.set()

    def __enter__(self) -> Reservation:
        return self.ready()

    def __exit__(self, *exc: object) -> None:
        self.release()


def _let_go(hosts: list[Any], probes: list[Future[Any]]) -> None:
    """Give up a try: cancel its probes (a holder that never started would
    leave its probe unfinished, and the driver waits for those at exit) and
    close its holders."""
    for p in probes:
        p.cancel()
    for h in hosts:
        h.close()


@remote(cpus=0.01)
def _ready_when(namespace: str) -> list[str]:
    from ._future import waiting
    from ._store import Store

    store = Store(namespace)
    while True:
        nodes = store.get("nodes")
        if nodes is not None:
            return nodes
        error = store.get("error")
        if error is not None:
            raise RuntimeError(f"the reservation failed: {error}")
        with waiting():
            time.sleep(0.2)


def reserve(bundles: list[dict[str, Any]], *, strategy: str = "pack", timeout: float | None = None) -> Reservation:
    """Hold resources for later calls: one bundle per dict (``cpus``, ``gpus``,
    ``memory``, ``device``, ``resources``). Returns at once; ``ready()`` (or
    ``await``) waits until every bundle is held."""
    return Reservation(bundles, strategy, timeout)


# ---- placing calls and actors within a bundle ------------------------------------


def call_within(bundle: Bundle, fn: Any, args: tuple, kwargs: dict) -> Future[Any]:
    return bundle._host().call.remote(fn, args, kwargs)


class _HostedMethod:
    def __init__(self, actor: HostedActor, name: str) -> None:
        self._actor, self._name = actor, name

    def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
        return self._actor._host.invoke.remote(self._actor._oid_future, self._name, args, kwargs)


class HostedActor:
    """An actor created ``within=`` a bundle: it lives in the bundle's host."""

    def __init__(self, bundle: Bundle, cls: type, args: tuple, kwargs: dict) -> None:
        self._bundle, self._cls = bundle, cls
        self._host = bundle._host()
        self._oid_future = self._host.new.remote(cls, args, kwargs)
        self.id = f"{os.urandom(4).hex()}@bundle{bundle.index}"

    def __getattr__(self, name: str) -> _HostedMethod:
        if name.startswith("_") or not callable(getattr(self._cls, name, None)):
            raise AttributeError(f"{self._cls.__name__} has no method {name!r}")
        return _HostedMethod(self, name)

    def close(self) -> None:
        self._host.drop.remote(self._oid_future)

    def __repr__(self) -> str:
        return f"<{self._cls.__name__} within {self._bundle!r}>"
