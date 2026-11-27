"""Reservations (placement groups): ``ch.dist.reserve(bundles, strategy=...)``.

    res = ch.dist.reserve([{"cpus": 2, "gpus": 1}] * 4, strategy="spread").ready()
    workers = [Trainer.options(within=res[i]).remote(i) for i in range(4)]
    step.options(within=res[0]).remote(batch)

Each bundle's CPUs and devices are held on one peer's resource ledger from
``ready()`` until ``release()``, whether or not anything runs in it: every
bundle, or none (see ``_ledger``). The first call or actor created
``within=res[i]`` starts the bundle's host process on that peer, on the
resources held for it; it and everything later within the bundle run inside
that host, so nothing else can take them. ``strategy``: ``"pack"`` (one peer
when it fits), ``"spread"`` (different peers when there are enough), and
``"strict_pack"``/``"strict_spread"`` (required, or the reservation is
infeasible).

Calls in one bundle share its host process (threads, up to the bundle's
CPUs at once); give each parallel worker its own bundle.

Reservations competing for the same capacity take turns: one holds every
bundle and the others hold nothing, trying again after random pauses. The
driver renews its holds while the reservation lives; if the driver
disappears, bundles no host has started on yet are free again within
``Reservation.HOLD_S``.
"""

from __future__ import annotations

import itertools
import os
import threading
import time
from typing import Any

from . import _ledger
from ._errors import Infeasible
from ._future import Future
from ._options import Effective, Placement
from ._remote import method_options, remote, with_replay

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
        return self.reservation._host(self.index)


class Reservation:
    """Resources held on peers for later calls (``ch.dist.reserve``)."""

    # A hold its driver stops renewing (the driver is gone) ends after this long.
    HOLD_S = _ledger.TTL

    def __init__(self, bundles: list[dict[str, Any]], strategy: str, timeout: float | None) -> None:
        if strategy not in STRATEGIES:
            raise ValueError(f"strategy must be one of {', '.join(STRATEGIES)}, got {strategy!r}")
        if not bundles:
            raise ValueError("a reservation needs at least one bundle")
        self.bundles = [dict(b) for b in bundles]
        self.strategy = strategy
        self._timeout = timeout
        self.id = os.urandom(8).hex()
        self._shapes = [(float(b.get("cpus", 1)), float(b.get("gpus", 0)), b.get("device", "")) for b in self.bundles]
        self._hosts: list[Any] = [None] * len(self.bundles)
        self._nodes: list[str] = []
        self._holds: _ledger.Holds | None = None
        self._released = False
        self._error: BaseException | None = None
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._check()  # strict placement that can never fit fails here, at once
        threading.Thread(target=self._acquire, name=f"reserve-{self.id}", daemon=True).start()

    def _check(self) -> None:
        from ._cluster import cluster

        live = sum(1 for n in cluster().nodes() if n.alive)
        if self.strategy == "strict_spread" and live < len(self.bundles):
            raise Infeasible(f"strict_spread: {len(self.bundles)} bundles need as many peers; {live} are live")
        if self.strategy == "strict_pack" and _ledger.plan(self._shapes, {}, self.strategy, free=False)[0] is None:
            need = sum(s[0] for s in self._shapes)
            raise Infeasible(f"strict_pack: no live peer has room for all {len(self.bundles)} bundles ({need:g} CPUs)")

    def _acquire(self) -> None:
        """Hold every bundle, or none, waiting while others hold the capacity."""
        from ._runtime import Runtime
        from ._store import Store

        store = Store(f"reserve/{self.id}")
        try:
            holds = _ledger.admit(
                f"{Runtime.get().tenant}/reserve/{self.id}",
                1,
                self._shapes,
                name=lambda i: f"bundle {i}",
                strategy=self.strategy,
                ttl=self.HOLD_S,
                stopped=lambda: self._released,
            )
            with self._lock:
                if holds is not None and self._released:
                    holds.release()  # released while this try held them
                elif holds is not None:
                    self._holds, self._nodes = holds, holds.peers
            if self._nodes:
                store.put("nodes", self._nodes, ttl="24h")
        except BaseException as e:  # noqa: BLE001 - e.g. infeasible bundles: ready() raises it
            self._error = e
            store.put("error", f"{type(e).__name__}: {e}", ttl="24h")
        finally:
            self._done.set()

    def __repr__(self) -> str:
        state = "released" if self._released else ("ready" if self._nodes else "pending")
        return f"<Reservation {len(self.bundles)} bundles, {self.strategy}, {state}>"

    def _host(self, i: int) -> Any:
        """Bundle `i`'s host process: started on the slot held for it by the
        first call or actor placed within the bundle."""
        from ._remote import Runtime, _create_actor

        self.ready()
        with self._lock:
            if self._released:
                raise RuntimeError("this reservation was released")
            if self._hosts[i] is None:
                b, holds = self.bundles[i], self._holds
                assert holds is not None  # ready() returned
                opts: dict[str, Any] = {"cpus": self._shapes[i][0], "concurrency": max(1, int(self._shapes[i][0]))}
                opts.update((k, b[k]) for k in ("gpus", "memory", "device", "resources") if k in b)
                view = _BundleHost.options(**opts, placement=Placement(require={"node": self._nodes[i]}))
                launch = {"launch": holds.tokens[i]}
                self._hosts[i] = _create_actor(Runtime.get(), view._cls, view.policy(), (), {}, launch)
            return self._hosts[i]

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
        with self._lock:
            if self._released:
                return
            self._released = True
            hosts, holds = self._hosts, self._holds
        for h in hosts:
            if h is not None:
                h.close()
        if holds is not None:
            holds.release()
        self._done.set()

    def __enter__(self) -> Reservation:
        return self.ready()

    def __exit__(self, *exc: object) -> None:
        self.release()


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


def call_within(bundle: Bundle, fn: Any, args: tuple, kwargs: dict, eff: Effective) -> Future[Any]:
    """`fn`, whose options are `eff`, run in the bundle's host: as a call that
    may not run again if `fn` may not (``with_replay``)."""
    return with_replay(bundle._host().call, eff).remote(fn, args, kwargs)


class _HostedMethod:
    def __init__(self, actor: HostedActor, name: str) -> None:
        self._actor, self._name = actor, name
        # The host's invoke, as a call that may not run again if this method,
        # with its actor's options, may not (`with_replay`).
        self._invoke = with_replay(actor._host.invoke, method_options(actor._cls, name, actor._eff))

    def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
        return self._invoke.remote(self._actor._oid_future, self._name, args, kwargs)


class HostedActor:
    """An actor created ``within=`` a bundle: it lives in the bundle's host."""

    def __init__(self, bundle: Bundle, cls: type, args: tuple, kwargs: dict, eff: Effective) -> None:
        self._bundle, self._cls, self._eff = bundle, cls, eff
        self._host = bundle._host()
        # Its constructor, as one that may not run again if the class's options say so.
        self._oid_future = with_replay(self._host.new, eff).remote(cls, args, kwargs)
        self._methods: dict[str, _HostedMethod] = {}  # one per name, so its options resolve once
        self.id = f"{os.urandom(4).hex()}@bundle{bundle.index}"

    def __getattr__(self, name: str) -> _HostedMethod:
        if name.startswith("_") or not callable(getattr(self._cls, name, None)):
            raise AttributeError(f"{self._cls.__name__} has no method {name!r}")
        method = self._methods.get(name)
        if method is None:
            method = self._methods[name] = _HostedMethod(self, name)
        return method

    def close(self) -> None:
        self._host.drop.remote(self._oid_future)

    def __repr__(self) -> str:
        return f"<{self._cls.__name__} within {self._bundle!r}>"
