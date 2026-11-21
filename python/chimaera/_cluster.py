"""Which peers a driver talks to, and how values reach them from another machine.

A driver uses, in order: ``ch.init(cluster=...)``, then ``$CHIMAERA_CLUSTER``,
then ``$CHIMAERA_ADDRESS`` (with ``$CHIMAERA_TOKEN`` or ``$CHIMAERA_TOKEN_FILE``
and ``$CHIMAERA_CA``), then the ``default`` in the profile file, then a local
peer. A cluster is a profile name or an address ``tcp://host:port``.

Profiles live in ``~/.config/chimaera/clusters.toml`` (``$CHIMAERA_CLUSTERS``
overrides the path)::

    default = "lab"

    [clusters.lab]
    peers = ["10.0.0.1:7710", "10.0.0.2:7710"]   # the peers' --client-listen addresses

A profile's credentials sit in ``~/.chimaera/clusters/<name>/``: ``token``
(from ``chimaera cluster init``) and, for TLS, ``ca.pem``. Keys ``token_file``
and ``ca`` in the profile name other files.
"""

from __future__ import annotations

import os
import threading
import tomllib
from dataclasses import dataclass
from typing import Any, NamedTuple

from ._errors import ChimaeraError


@dataclass(frozen=True)
class Target:
    name: str
    addresses: tuple[str, ...]  # tcp://host:port, tried in order
    token: str | None
    ca: str | None


def _profile_path() -> str:
    return os.environ.get("CHIMAERA_CLUSTERS") or os.path.join(os.path.expanduser("~"), ".config", "chimaera", "clusters.toml")


def _read(path: str) -> str:
    with open(path) as f:
        return f.read().strip()


def _tcp(address: str) -> str:
    for prefix in ("chimaera://", "tcp://"):
        if address.startswith(prefix):
            return "tcp://" + address[len(prefix) :]
    return "tcp://" + address


def _token_from_env() -> str | None:
    if os.environ.get("CHIMAERA_TOKEN"):
        return os.environ["CHIMAERA_TOKEN"]
    path = os.environ.get("CHIMAERA_TOKEN_FILE")
    return _read(path) if path else None


def load_profiles() -> dict[str, Any]:
    path = _profile_path()
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}
    except tomllib.TOMLDecodeError as e:
        raise ChimaeraError(f"{path}: {e}") from None


def resolve(cluster: str | None) -> Target | None:
    """The remote cluster to use, or None for a local peer."""
    cluster = cluster or os.environ.get("CHIMAERA_CLUSTER") or None
    if cluster is None and os.environ.get("CHIMAERA_ADDRESS"):
        addrs = tuple(_tcp(a.strip()) for a in os.environ["CHIMAERA_ADDRESS"].split(",") if a.strip())
        return Target("$CHIMAERA_ADDRESS", addrs, _token_from_env(), os.environ.get("CHIMAERA_CA"))
    profiles = load_profiles()
    cluster = cluster or profiles.get("default")
    if cluster in (None, "local"):
        return None
    if cluster.startswith(("tcp://", "chimaera://")):
        return Target(cluster, (_tcp(cluster),), _token_from_env(), os.environ.get("CHIMAERA_CA"))
    known = profiles.get("clusters", {})
    spec = known.get(cluster)
    if spec is None:
        names = ", ".join(sorted(known)) or "none"
        raise ChimaeraError(f"no cluster {cluster!r} in {_profile_path()} (profiles: {names})")
    peers = spec.get("peers") or []
    if not peers:
        raise ChimaeraError(f"cluster {cluster!r} lists no peers: add peers = [\"host:port\", ...] (their --client-listen)")
    creds = os.path.join(os.path.expanduser("~"), ".chimaera", "clusters", cluster)
    token_file = spec.get("token_file") or os.path.join(creds, "token")
    token = _token_from_env() or (_read(token_file) if os.path.exists(token_file) else None)
    ca = spec.get("ca") or os.path.join(creds, "ca.pem")
    return Target(cluster, tuple(_tcp(p) for p in peers), token, ca if os.path.exists(ca) else None)


class RemoteBlobSink:
    """Uploads blobs to the connected peer (the client cannot write its store)."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self._sent: set[tuple[bytes, bytes]] = set()
        self._lock = threading.Lock()

    def put(self, run: bytes, blob_id: bytes, data: bytes | memoryview) -> None:
        key = (run, blob_id)
        with self._lock:
            if key in self._sent:
                return
        if self.client.has_blobs(run, [blob_id]):
            self.client.put_blob(run, blob_id, bytes(data))
        with self._lock:
            self._sent.add(key)


class Node(NamedTuple):
    peer: str
    address: str
    cpus: float
    memory_bytes: int
    gpus: list[str]
    alive: bool
    cpu_used: float
    queued: int
    this_peer: bool
    labels: dict[str, str]


class Event(NamedTuple):
    """Something the cluster did, as a peer saw it."""

    seq: int
    at: float  # unix seconds
    kind: str  # "member", "actor", "task_failed", "decision" or "drain"
    node: str  # the peer it is about (hex), or ""
    run: str
    task: str  # run id + task id (hex), as Future.id, or ""
    actor: str
    message: str
    detail: dict[str, Any]


class EventStream:
    """Events in order, as ``ch.cluster().events()`` returns them. Iterating
    waits for new ones (``follow=True``) or stops at the newest."""

    _WAIT_MS = 10_000

    def __init__(self, kinds: list[str], since: int | None, follow: bool) -> None:
        self._kinds, self._follow = list(kinds), follow
        self._buffer: list[Event] = []
        self._closed = False
        self._peer: tuple[bytes, int] | None = None
        if since is None:  # only what happens from now on
            _, since, peer, inc = self._client().events(self._kinds, 0, 0)
            self._peer = (bytes(peer), inc)
        self._since = since

    @staticmethod
    def _client() -> Any:
        from ._runtime import Runtime

        return Runtime.get().client

    def __iter__(self) -> EventStream:
        return self

    def __next__(self) -> Event:
        import json

        while not self._buffer:
            if self._closed:
                raise StopIteration
            events, nxt, peer, inc = self._client().events(self._kinds, self._since, self._WAIT_MS if self._follow else 0)
            if self._peer is not None and self._peer != (bytes(peer), inc):
                # The peer restarted and its log began again: read the new one from the start.
                self._peer, self._since = (bytes(peer), inc), 0
                continue
            self._peer, self._since = (bytes(peer), inc), nxt
            self._buffer = [
                Event(
                    e["seq"],
                    e["at_unix_ms"] / 1000,
                    e["kind"],
                    e["node"].hex(),
                    e["run"].hex(),
                    e["task"].hex() if e["task"] else "",
                    e["actor"].hex(),
                    e["message"],
                    json.loads(e["detail_json"]) if e["detail_json"] else {},
                )
                for e in events
            ]
            if not self._buffer and not self._follow:
                raise StopIteration
        return self._buffer.pop(0)

    def close(self) -> None:
        """Stop following: iteration ends after what has been received."""
        self._closed = True


class ClusterView:
    """What the connected peer knows about the cluster (``ch.cluster()``)."""

    def nodes(self) -> list[Node]:
        from ._runtime import Runtime

        return [Node(**n) for n in Runtime.get().client.nodes()]

    def events(self, kinds: list[str] | None = None, since: int | None = None, *, follow: bool = True) -> EventStream:
        """Cluster events, oldest first: members joining and leaving, actors
        starting, restarting and dying, calls failing for good, controller
        decisions and drains. `kinds` narrows them; `since=0` starts at the
        oldest the peer still holds (it keeps the latest 10,000), the default
        at what happens next. With ``follow=False`` iteration stops at the newest.

        Events come from the peer this process is connected to: member events
        from the whole cluster, call and actor events for the shards it leads.
        They are notifications, not records: ``Run.outcomes()`` and
        ``ch.actor(key)`` are the durable answers."""
        return EventStream(kinds or [], since, follow)

    def resources(self) -> dict[str, float]:
        """Capacity of the live nodes, and how much CPU they report in use."""
        live = [n for n in self.nodes() if n.alive]
        return {
            "nodes": len(live),
            "cpus": sum(n.cpus for n in live),
            "cpus_in_use": sum(n.cpu_used for n in live),
            "memory_bytes": sum(n.memory_bytes for n in live),
            "gpus": sum(len(n.gpus) for n in live),
        }


def cluster() -> ClusterView:
    return ClusterView()
