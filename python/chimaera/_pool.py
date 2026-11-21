"""Pools of interchangeable actor replicas: ``Class.options(replicas=...).pool(...)``.

Calls go to the replica with the fewest calls in flight; ``route(key)`` sends
everything for one key to one replica (rendezvous hashing, so resizing moves
few keys). ``resize`` grows or shrinks the pool and ``upgrade`` replaces every
replica, both without dropping calls: a replica leaving the pool takes no new
calls and closes once its calls finish. ``key=`` shares one pool between
processes (its membership lives in a ``ch.Store``), and ``replicas=ch.auto(...)``
sizes it from load.
"""

from __future__ import annotations

import itertools
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ._future import Future, Results
from ._native import blake3_digest
from ._options import Auto, Effective, signature_of

C = TypeVar("C")


@dataclass(eq=False)  # replicas compare by identity
class _Replica:
    actor: Any
    name: str  # stable id for routing: the actor key, or its id
    inflight: int = 0
    draining: bool = False


class Pool(Generic[C]):
    """Interchangeable replicas of an actor class."""

    _REFRESH_S = 1.0

    def __init__(self, cls_view: Any, args: tuple[Any, ...], kwargs: dict[str, Any], eff: Effective) -> None:
        from ._runtime import Runtime

        self._cls_view, self._args, self._kwargs, self._eff = cls_view, args, kwargs, eff
        self._lock = threading.RLock()
        self._replicas: list[_Replica] = []
        self._closed = False
        self._turn = itertools.count()
        self.decisions: list[str] = []  # what the auto-sizer did, and why
        self._key: str | None = eff.get("key")
        self._store: Any = None
        self._seen_version = -1
        self._last_refresh = 0.0
        want = eff.get("replicas")
        auto = want if isinstance(want, Auto) else None
        cores = int(Runtime.get().info["usable_cores"])
        if auto is not None:
            initial = auto.initial or 1
        elif isinstance(want, int):
            initial = want
        else:
            initial = max(1, min(8, cores))
        self._min = max(1, auto.min or 1) if auto else initial
        self._max = (auto.max or max(initial, cores)) if auto else initial
        if self._key is not None:
            from ._store import Store

            self._store = Store(f"pool/{self._key}")
            self._join(initial)
        else:
            self._check_args()
            for _ in range(initial):
                self._add()
        if auto is not None:
            threading.Thread(target=self._autosize, name="pool-autosize", daemon=True).start()

    # ---- membership -------------------------------------------------------------

    def _spawn(self, name: str | None) -> Any:
        from ._remote import _create_actor
        from ._runtime import Runtime

        values = {**self._eff.values, "key": name}
        eff = Effective(values, {**self._eff.sources, "key": "pool replica"})
        return _create_actor(Runtime.get(), self._cls_view._cls, eff, self._args, self._kwargs)

    def _check_args(self) -> None:
        """Fail here, not in each replica, when the constructor arguments don't fit."""
        signature_of(self._cls_view._cls).bind(*self._args, **self._kwargs)

    def _add(self) -> _Replica:
        if self._store is None:
            actor = self._spawn(None)
            r = _Replica(actor, actor.id)
        else:
            name = f"pool/{self._key}/{self._store.counter('next').add(1)}"
            r = _Replica(self._spawn(name), name)
            self._publish(lambda names: [*names, name])
        with self._lock:
            self._replicas.append(r)
        return r

    def _join(self, initial: int) -> None:
        """Adopt a shared pool's replicas, creating them if this is the first user."""
        from ._errors import VersionConflict

        if self._store.version("members") == 0:
            self._check_args()
            names = [f"pool/{self._key}/{self._store.counter('next').add(1)}" for _ in range(initial)]
            try:
                self._store.put("members", names, if_version=0)
            except VersionConflict:
                pass  # another process created it first
        self._refresh(force=True)

    def _publish(self, change: Any) -> None:
        from ._errors import VersionConflict

        while True:
            version = self._store.version("members")
            try:
                self._store.put("members", change(self._store.get("members", [])), if_version=version)
                self._seen_version = self._store.version("members")
                return
            except VersionConflict:
                continue

    def _refresh(self, force: bool = False) -> None:
        if self._store is None or (not force and time.monotonic() - self._last_refresh < self._REFRESH_S):
            return
        self._last_refresh = time.monotonic()
        version = self._store.version("members")
        if version == self._seen_version:
            return
        names = self._store.get("members", [])
        with self._lock:
            have = {r.name: r for r in self._replicas}
            self._replicas = [have.get(n) or _Replica(self._spawn(n), n) for n in names]
            for gone in set(have) - set(names):
                self._retire(have[gone])
            self._seen_version = version

    def __len__(self) -> int:
        self._refresh()
        return len([r for r in self._replicas if not r.draining])

    def _live(self) -> list[_Replica]:
        self._refresh()
        live = [r for r in self._replicas if not r.draining]
        if not live:
            raise RuntimeError("the pool has no replicas (it was closed or resized to zero)")
        return live

    def _pick(self) -> _Replica:
        """The least busy replica; ties rotate, so sequential calls spread too."""
        with self._lock:
            live = self._live()
            start = next(self._turn) % len(live)
            return min(live[start:] + live[:start], key=lambda r: r.inflight)

    def route(self, key: Any) -> Any:
        """The replica for `key`: the same one while the pool's membership does
        not change, and after a resize, the same one for most keys."""
        k = str(key).encode()
        with self._lock:
            return max(self._live(), key=lambda r: blake3_digest(r.name.encode() + b"\0" + k)).actor

    # ---- changes ------------------------------------------------------------------

    def _retire(self, r: _Replica) -> None:
        """No new calls; close once the calls in flight finish."""
        r.draining = True

        def close_when_idle() -> None:
            while r.inflight > 0:
                time.sleep(0.01)
            r.actor.close()

        threading.Thread(target=close_when_idle, daemon=True).start()

    def resize(self, n: int) -> None:
        """Grow or shrink to `n` replicas; shrinking drains the newest first."""
        if n < 1:
            raise ValueError("a pool keeps at least one replica; use close() to end it")
        while len(self) < n:
            self._add()
        with self._lock:
            extra = [r for r in self._replicas if not r.draining][n:]
            for r in extra:
                self._retire(r)
            self._replicas = [r for r in self._replicas if r not in extra]
        if self._store is not None and extra:
            gone = {r.name for r in extra}
            self._publish(lambda names: [x for x in names if x not in gone])

    def upgrade(self, *args: Any, cls: Any = None, surge: int = 1, **kwargs: Any) -> None:
        """Replace every replica with one built from `cls` (default: the same
        class) and these constructor arguments, `surge` at a time. Calls in
        flight finish on the old replicas; new calls go to the new ones."""
        from ._remote import ActorClass

        if args and isinstance(args[0], ActorClass) and cls is None:
            cls, args = args[0], args[1:]
        old = [r for r in self._replicas if not r.draining]
        if cls is not None:
            # The new class's own options, keeping this pool's identity and size.
            keep = {k: v for k, v in self._eff.values.items() if k in ("key", "replicas")}
            new = cls.policy()
            self._cls_view = cls
            self._eff = Effective({**new.values, **keep}, {**new.sources, **{k: "pool" for k in keep}})
        self._args, self._kwargs = args, kwargs
        self._check_args()
        for i in range(0, len(old), max(1, surge)):
            batch = old[i : i + max(1, surge)]
            for _ in batch:
                self._add()
            for r in batch:
                self._retire(r)
            if self._store is not None:
                gone = {r.name for r in batch}
                self._publish(lambda names, gone=gone: [x for x in names if x not in gone])
            while any(r.inflight for r in batch):
                time.sleep(0.01)
            with self._lock:
                self._replicas = [r for r in self._replicas if r not in batch]

    def _autosize(self) -> None:
        """Multiplicative growth while busy (calls in flight over 80% of what
        the replicas can run at once), one step down after a quiet minute."""
        concurrency = self._eff.get("concurrency")
        per = concurrency if isinstance(concurrency, int) else 1
        busy, quiet_since = 0, time.monotonic()
        while not self._closed:
            time.sleep(0.25)
            with self._lock:
                live = [r for r in self._replicas if not r.draining]
                util = sum(r.inflight for r in live) / max(1, len(live) * per)
            if util > 0.8:
                busy, quiet_since = busy + 1, time.monotonic()
            else:
                busy = 0
                if util > 0.25:
                    quiet_since = time.monotonic()
            if busy >= 3 and len(live) < self._max:
                n = min(self._max, 2 * len(live))
                self.decisions.append(f"grow {len(live)} -> {n}: {util:.0%} busy for 0.75 s")
                self.resize(n)
                busy = 0
            elif time.monotonic() - quiet_since > 60 and len(live) > self._min:
                self.decisions.append(f"shrink {len(live)} -> {len(live) - 1}: under 25% busy for 60 s")
                self.resize(len(live) - 1)
                quiet_since = time.monotonic()

    def close(self) -> None:
        self._closed = True
        with self._lock:
            replicas, self._replicas = self._replicas, []
        for r in replicas:
            r.actor.close()
        if self._store is not None:
            self._store.delete("members")

    def __enter__(self) -> Pool[C]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __getattr__(self, name: str) -> _PoolMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        getattr(self._cls_view._cls, name)  # an AttributeError names the class
        return _PoolMethod(self, name)

    def __reduce__(self) -> tuple[Any, ...]:
        if self._key is None:
            raise TypeError("only a pool created with key= can be passed to calls; give it a key")
        return (_shared, (self._cls_view, self._key))


def _shared(cls_view: Any, key: str) -> Pool[Any]:
    return cls_view.options(key=key).pool()


class _PoolMethod:
    def __init__(self, pool: Pool[Any], name: str) -> None:
        self._pool, self._name = pool, name

    def remote(self, *args: Any, **kwargs: Any) -> Future[Any]:
        lock = self._pool._lock
        with lock:
            r = self._pool._pick()
            r.inflight += 1
        try:
            f = getattr(r.actor, self._name).remote(*args, **kwargs)
        except BaseException:
            with lock:
                r.inflight -= 1
            raise

        def done(_: Future[Any]) -> None:
            with lock:
                r.inflight -= 1

        f.add_done_callback(done)
        return f

    def map(self, source: Iterable[Any], *, ordered: bool = True) -> Results[Any]:
        window = 16 * max(1, len(self._pool))
        return Results(lambda i, item: self.remote(item), iter(source), ordered=ordered, window=window)
