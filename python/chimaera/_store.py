"""Replicated keys, counters, locks and barriers: ``ch.coord.Store``.

A namespace lives in one Raft group, so its operations are linearizable and
survive the loss of a minority of peers. Every change bumps the namespace's
revision: a key's ``version`` is the revision of its last change, which makes
``if_version`` a compare-and-set (0 means "must not exist") and positions
watches. For more throughput, spread keys over several namespaces.
"""

from __future__ import annotations

import asyncio
import os
import pickle
import socket
import threading
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from typing import Any, NamedTuple

import cloudpickle

from . import _units
from ._errors import ChimaeraError, LeaseLost, VersionConflict
from ._future import waiting
from ._runtime import Runtime

MAX_VALUE = 1 << 20


class Event(NamedTuple):
    """One change seen by a watch: ``kind`` is ``"put"`` or ``"delete"``, or
    ``"resync"`` when the watch fell behind (see ``Watch``)."""

    kind: str
    key: str
    value: Any
    version: int


def _dumps(value: Any) -> bytes:
    try:
        data = pickle.dumps(value, protocol=5)
    except (pickle.PicklingError, TypeError, AttributeError):
        data = cloudpickle.dumps(value, protocol=5)
    if len(data) > MAX_VALUE:
        raise ValueError(f"a store value is at most 1 MiB pickled, this one is {len(data)} bytes; store ch.put(value) instead")
    return data


def _value(entry: dict[str, Any]) -> Any:
    return entry["counter"] if entry["is_counter"] else pickle.loads(entry["value"])


def _ms(duration: float | str | timedelta) -> int:
    return int(_units.parse_duration(duration, "duration") * 1000)


def _raise(reply: dict[str, Any]) -> None:
    err = reply["error"]
    if err.startswith("version mismatch"):
        entry = reply["entry"]
        raise VersionConflict(err, entry["version"] if entry else 0)
    if err.startswith("fenced"):
        raise LeaseLost(err)
    raise ChimaeraError(err)


class Store:
    """A namespace of replicated keys. Values are pickled, at most 1 MiB each
    (store a ``ch.put`` future for larger data)."""

    def __init__(self, namespace: str) -> None:
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("a store namespace is a non-empty string")
        self.namespace = namespace

    def __repr__(self) -> str:
        return f"<Store {self.namespace!r}>"

    def __reduce__(self) -> tuple[Any, ...]:
        return (Store, (self.namespace,))

    def _write(self, op: str, key: str, **kw: Any) -> dict[str, Any]:
        return Runtime.get().client.store_write(self.namespace, op, key, **kw)

    def _read(self, kind: str, key: str = "", **kw: Any) -> dict[str, Any]:
        return Runtime.get().client.store_read(self.namespace, kind, key, **kw)

    def _txn(
        self, checks: list[tuple[str, int]], ops: list[tuple[str, str, bytes | None, int]], fence: Lease | None = None
    ) -> dict[str, Any]:
        """Apply `ops` ((op, key, pickled value, delta): put, delete or add)
        together, once each (key, version) of `checks` holds and `fence` is
        still held, or none of them; return the reply."""
        f = fence._fence() if fence is not None else None
        return Runtime.get().client.store_txn(self.namespace, checks, [(*op, f) for op in ops])

    def _claim(
        self, prefix: str, lock_prefix: str, ttl: float | str | timedelta
    ) -> tuple[dict[str, Any], Lease | None]:
        """Claim the first key under `prefix` that nobody holds: the reply,
        and the held lease on `lock_prefix` + key if one was granted."""
        lease = Lease(self, lock_prefix, _ms(ttl), None)
        r = Runtime.get().client.store_claim(self.namespace, prefix, lock_prefix, lease.holder, lease._ttl_ms)
        if not r["ok"]:
            return r, None
        lease.key += r["entry"]["key"]
        return r, lease._held(r["lease"][1])

    # ---- keys ----------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        r = self._read("get", key)
        return _value(r["entry"]) if r["ok"] else default

    def version(self, key: str) -> int:
        """The key's version (0 if it does not exist)."""
        r = self._read("get", key)
        return r["entry"]["version"] if r["ok"] else 0

    def get_versioned(self, key: str, default: Any = None) -> tuple[Any, int]:
        """The key's value and version from one read, for a compare-and-set
        (``put(..., if_version=version)``); ``(default, 0)`` if it does not exist."""
        r = self._read("get", key)
        return (_value(r["entry"]), r["entry"]["version"]) if r["ok"] else (default, 0)

    def put(
        self,
        key: str,
        value: Any,
        *,
        if_version: int | None = None,
        ttl: float | str | timedelta | None = None,
        fence: Lease | None = None,
    ) -> int:
        """Store `value`; return its version. ``if_version`` makes it a
        compare-and-set (``VersionConflict`` otherwise); ``ttl`` expires it;
        ``fence`` refuses the write (``LeaseLost``) unless that lock is still held."""
        r = self._write(
            "put",
            key,
            value=_dumps(value),
            if_version=if_version,
            ttl_ms=_ms(ttl) if ttl is not None else 0,
            fence=fence._fence() if fence is not None else None,
        )
        if not r["ok"]:
            _raise(r)
        return r["revision"]

    def delete(self, key: str, *, if_version: int | None = None, fence: Lease | None = None) -> bool:
        """Remove the key; False if it did not exist."""
        r = self._write("delete", key, if_version=if_version, fence=fence._fence() if fence is not None else None)
        if r["ok"]:
            return True
        if r["error"].endswith("does not exist"):
            return False
        _raise(r)
        return False

    def scan(self, prefix: str = "") -> Scan:
        """(key, value) for every key starting with `prefix`, in key order;
        ``.revision`` is where to start a watch that keeps it current."""
        return Scan(self, prefix)

    def keys(self, prefix: str = "") -> Iterator[str]:
        """Keys starting with `prefix`, in order, without fetching their values."""
        after = ""
        while True:
            r = self._read("scan", prefix, start_after=after, keys_only=True)
            for e in r["entries"]:
                yield e["key"]
                after = e["key"]
            if not r["more"]:
                return

    def watch(self, prefix: str = "", *, start: str | int = "latest") -> Watch:
        """Changes to keys starting with `prefix`, in order, from `start`:
        ``"latest"`` (from now on), ``"earliest"`` (every change the namespace
        still holds) or after a revision."""
        start = _units.start_position(start)
        after = self._read("get", "")["revision"] if start == "latest" else 0 if start == "earliest" else start
        return Watch(self, prefix, after)

    # ---- coordination ------------------------------------------------------------

    def counter(self, key: str) -> Counter:
        return Counter(self, key)

    def lock(self, key: str, *, ttl: float | str | timedelta = "30s", timeout: float | None = None) -> Lease:
        """A lock: ``with store.lock("leader") as lease: ...``. While held, the
        lease is renewed in the background; ``lease.token`` grows with every
        grant, and ``store.put(..., fence=lease)`` refuses a holder that lost it."""
        return Lease(self, key, _ms(ttl), timeout)

    def holder(self, key: str) -> str | None:
        """Who holds the lock `key` now (for leader election), or None."""
        r = self._read("lease", key)
        return r["lease"][0] if r["ok"] else None

    def barrier(self, key: str, parties: int) -> Barrier:
        return Barrier(self, key, parties)


class Counter:
    """An integer in a store; ``add`` is atomic."""

    def __init__(self, store: Store, key: str) -> None:
        self._store, self._key = store, key

    def add(self, n: int = 1) -> int:
        """Add `n` (may be negative); return the new value."""
        r = self._store._write("add", self._key, delta=int(n))
        if not r["ok"]:
            _raise(r)
        return r["entry"]["counter"]

    def value(self) -> int:
        r = self._store._read("get", self._key)
        return r["entry"]["counter"] if r["ok"] else 0


class Lease:
    """A held lock. Use it as a context manager (``with`` or ``async with``),
    or call ``acquire()`` and ``release()``."""

    def __init__(self, store: Store, key: str, ttl_ms: int, timeout: float | None) -> None:
        self._store, self.key, self._ttl_ms, self._timeout = store, key, max(1, ttl_ms), timeout
        self.holder = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.token = 0
        self._lost = False
        self._stop = threading.Event()

    @property
    def valid(self) -> bool:
        """True while held: acquired, renewed, not released or lost."""
        return self.token != 0 and not self._lost and not self._stop.is_set()

    def _fence(self) -> tuple[str, int]:
        return (self.key, self.token)

    def acquire(self) -> Lease:
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        pause = 0.01
        while True:
            r = self._store._write("lock", self.key, ttl_ms=self._ttl_ms, holder=self.holder)
            if r["ok"]:
                return self._held(r["lease"][1])
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"lock {self.key!r} is held by {r['lease'][0] if r['lease'] else 'another holder'}")
            with waiting():
                time.sleep(pause if deadline is None else min(pause, max(0.0, deadline - time.monotonic())))
            pause = min(pause * 2, 0.5)

    def _held(self, token: int) -> Lease:
        """Granted with `token`: renewed in the background until released."""
        self.token = token
        self._lost = False
        self._stop.clear()
        threading.Thread(target=self._renew, name=f"lease-{self.key}", daemon=True).start()
        return self

    def _renew(self) -> None:
        while not self._stop.wait(self._ttl_ms / 3000):
            r = self._store._write("renew", self.key, ttl_ms=self._ttl_ms, holder=self.holder, token=self.token)
            if not r["ok"]:
                self._lost = True
                return

    def release(self) -> None:
        self._stop.set()
        if self.token and not self._lost:
            self._store._write("unlock", self.key, holder=self.holder, token=self.token)

    def __enter__(self) -> Lease:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()

    async def __aenter__(self) -> Lease:
        return await asyncio.to_thread(self.acquire)

    async def __aexit__(self, *exc: object) -> None:
        await asyncio.to_thread(self.release)


class Barrier:
    """Wait until `parties` participants have arrived, round after round."""

    def __init__(self, store: Store, key: str, parties: int) -> None:
        if not isinstance(parties, int) or parties < 1:
            raise ValueError(f"parties: expected an integer >= 1, got {parties!r}")
        self._store, self._key, self._parties = store, key, parties

    def wait(self, timeout: float | None = None) -> int:
        """Arrive and wait for the rest; return the round number that completed.
        On timeout the arrival still counts toward that round."""
        r = self._store._write("arrive", self._key, parties=self._parties)
        if not r["ok"]:
            _raise(r)
        _, arrived, generation = r["barrier"]
        if arrived == 0:
            return generation  # this arrival completed the round
        deadline = None if timeout is None else time.monotonic() + timeout
        pause = 0.005
        while True:
            state = self._store._read("barrier", self._key)["barrier"]
            if state and state[2] > generation:
                return generation + 1
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"barrier {self._key!r}: {state[1] if state else 0} of {self._parties} arrived")
            with waiting():
                time.sleep(pause)
            pause = min(pause * 2, 0.2)


class Scan:
    """(key, value) pairs under a prefix in key order (``store.scan``), read a
    page at a time from a cursor, the last key read.

    ``revision`` is the revision its first page was read at; later pages are
    no older, so writes during the scan may or may not show. A watch from it,
    ``store.watch(prefix, start=scan.revision)``, delivers every change the
    pages may have missed: applying its events to the pairs gives the
    namespace as it is now, with some changes seen twice and none missed.
    """

    def __init__(self, store: Store, prefix: str) -> None:
        self._store, self._prefix = store, prefix
        self._page: deque[tuple[str, Any]] = deque()
        self._after, self._more = "", True
        self.revision: int = self._fetch()

    def _fetch(self) -> int:
        r = self._store._read("scan", self._prefix, start_after=self._after)
        for e in r["entries"]:
            self._page.append((e["key"], _value(e)))
            self._after = e["key"]
        self._more = r["more"]
        return int(r["revision"])

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return self

    def __next__(self) -> tuple[str, Any]:
        while not self._page:
            if not self._more:
                raise StopIteration
            self._fetch()
        return self._page.popleft()


class Watch:
    """Changes to a prefix in revision order; iterate it (sync or async).

    If it falls behind the namespace's bounded change log, it delivers a
    ``resync`` event, then the current value of every key under the prefix
    as ``put`` events, and continues from there (``resyncs`` counts this): a
    key those puts leave out was deleted meanwhile. A change may be seen
    twice, never missed.
    """

    _WAIT_MS = 30_000

    def __init__(self, store: Store, prefix: str, after: int) -> None:
        self._store, self._prefix, self._after = store, prefix, after
        self._buffer: deque[Event] = deque()
        self._closed = False
        self.resyncs = 0

    @property
    def version(self) -> int:
        """The revision this watch has delivered up to."""
        return self._after

    def _fetch(self) -> None:
        with waiting():
            r = self._store._read("changes", self._prefix, after_revision=self._after, wait_ms=self._WAIT_MS)
        if r["compacted"]:
            self.resyncs += 1
            scan = self._store.scan(self._prefix)
            self._buffer.append(Event("resync", self._prefix, None, scan.revision))
            self._buffer.extend(Event("put", key, value, scan.revision) for key, value in scan)
            self._after = scan.revision
            return
        for c in r["changes"]:
            value = c["counter"] if c["is_counter"] else (None if c["deleted"] else pickle.loads(c["value"]))
            self._buffer.append(Event("delete" if c["deleted"] else "put", c["key"], value, c["revision"]))
            self._after = c["revision"]
        if not r["changes"] and not r["more"]:
            self._after = max(self._after, r["revision"])

    def __iter__(self) -> Iterator[Event]:
        return self

    def __next__(self) -> Event:
        while not self._buffer:
            if self._closed:
                raise StopIteration
            self._fetch()
        return self._buffer.popleft()

    def __aiter__(self) -> AsyncIterator[Event]:
        return self

    async def __anext__(self) -> Event:
        while not self._buffer:
            if self._closed:
                raise StopAsyncIteration
            await asyncio.to_thread(self._fetch)
        return self._buffer.popleft()

    def close(self) -> None:
        self._closed = True
