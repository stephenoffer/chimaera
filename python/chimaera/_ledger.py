"""Holds on the peers' resource ledgers: the slots (CPUs and whole devices)
a gang generation or a reservation keeps on each peer, taken all or none.

A peer grants a hold only from what is free, so two gangs that each need the
whole cluster never split it: one holds every slot, the other holds none and
tries again after a random pause, first on the peers that refused it least.
Slots are taken in peer order, so two holders never wait on each other. The holder renews its holds; one that
disappears lets them expire within ``ttl``. A call or actor started with a
slot's launch token runs on exactly that slot, and a token whose hold
ended (expired, released, or replaced by a newer generation) is refused.
"""

from __future__ import annotations

import math
import random
import threading
import time
from collections.abc import Callable, Collection
from typing import Any

from ._errors import Infeasible
from ._options import Auto

TTL = 15.0  # seconds a hold outlives its holder; renewed every third of it

Shape = tuple[float, float, str]  # one slot: (cpus, gpus, device)


def shape(opts: dict[str, Any], cpus: float = 1.0) -> Shape:
    """One slot for a call or actor with these options (`cpus`: the default)."""
    c = opts.get("cpus")
    c = (c.initial if isinstance(c, Auto) else c) or cpus
    return float(c), float(opts.get("gpus") or 0), opts.get("device") or ""


def whole_devices(gpus: float) -> int:
    """The whole devices a slot asking for `gpus` takes, as its peer grants
    them (devices.rs ``Devices::want``): a fraction takes a whole device,
    since devices are not shared."""
    return math.ceil(gpus) if gpus > 0 else 0


def plan(
    shapes: list[Shape],
    require: dict[str, str],
    strategy: str,
    free: bool,
    refused: dict[str, int] | None = None,
    never: Collection[tuple[str, Shape]] = (),
) -> tuple[list[str] | None, int]:
    """A live peer (hex id) for each slot, as the peers last reported their
    free CPUs (`free`) or their whole capacity, and their devices; else None
    and how many slots fit. ``pack`` fills the roomiest peers first, on one
    when it can; ``spread`` puts each slot on a peer without one, while there
    are such peers; ``strict_*`` allow nothing else. Peers that `refused`
    fewer of this holder's tries come first (a report does not show which
    devices are free), and no slot goes to a peer that said it `never` could
    hold that slot's shape."""
    from ._cluster import cluster

    refused = refused or {}
    room = {
        n.peer: [n.cpus - (n.cpu_used if free else 0.0), float(len(n.gpus))]
        for n in cluster().nodes()
        if n.alive and not n.draining and all(n.labels.get(k) == v for k, v in require.items())
    }
    order = sorted(room, key=lambda p: (refused.get(p, 0), -room[p][0], p))
    need = [sum(s[0] for s in shapes) + 1e-9, sum(whole_devices(s[1]) for s in shapes) + 1e-9]
    whole = [p for p in order if all((p, s) not in never for s in shapes)]
    one = next((p for p in whole if room[p][0] >= need[0] and room[p][1] >= need[1]), None)
    if strategy.endswith("pack") and one is not None:
        return [one] * len(shapes), len(shapes)
    if strategy == "strict_pack":
        return None, 0
    peers: list[str] = []
    for shape in shapes:
        cpus, gpus = shape[0], whole_devices(shape[1])
        fits = [p for p in order if room[p][0] + 1e-9 >= cpus and room[p][1] + 1e-9 >= gpus and (p, shape) not in never]
        if strategy.endswith("spread"):
            fresh = [p for p in fits if p not in peers]
            fits = fresh if fresh or strategy == "strict_spread" else fits
            fits.sort(key=lambda p: (refused.get(p, 0), -room[p][0], p))
        if not fits:
            return None, len(peers)
        peers.append(fits[0])
        room[fits[0]][0] -= cpus
        room[fits[0]][1] -= gpus
    return peers, len(peers)


class Holds:
    """One owner's generation held on its peers, renewed until released."""

    def __init__(self, owner: str, generation: int, peers: list[str], shapes: list[Shape], ttl: float) -> None:
        self.owner, self.generation, self.peers, self.ttl = owner, generation, peers, ttl
        self._slots: dict[str, list[Shape]] = {}
        # Each slot's launch token: its index among the slots on its peer.
        self.tokens: list[tuple[str, int, int]] = []
        for peer, s in zip(peers, shapes, strict=True):
            mine = self._slots.setdefault(peer, [])
            self.tokens.append((owner, generation, len(mine)))
            mine.append(s)
        self._stop = threading.Event()

    def _ask(self, peer: str, slots: list[Shape], renew: bool = False) -> tuple[bool, str, bool]:
        from ._runtime import Runtime

        try:
            held, why, _, never = Runtime.get().client.hold(
                bytes.fromhex(peer), self.owner, self.generation, slots, int(self.ttl * 1000), renew
            )
        except Exception as e:  # noqa: BLE001 - a peer that cannot answer holds nothing
            return False, str(e), False
        return held, why, never

    def take(self) -> tuple[str, str, bool] | None:
        """Hold every slot, peer by peer in peer order: None once all are held,
        else the peer that refused, why, and whether it never could, even
        idle (and nothing stays held)."""
        held: list[str] = []
        for peer in sorted(self._slots):
            ok, why, never = self._ask(peer, self._slots[peer])
            if not ok:
                self._release(held)
                return peer, why, never
            held.append(peer)
        threading.Thread(target=self._renew, name=f"hold-{self.owner}", daemon=True).start()
        return None

    def _renew(self) -> None:
        while not self._stop.wait(self.ttl / 3):
            for peer, slots in self._slots.items():
                self._ask(peer, slots, renew=True)  # a hold lost anyway shows when its token is refused

    def _release(self, peers: list[str]) -> None:
        for peer in peers:
            self._ask(peer, [])

    def release(self) -> None:
        """End the holds: slots no call or actor took are free again."""
        if not self._stop.is_set():
            self._stop.set()
            self._release(list(self._slots))


def admit(
    owner: str,
    generation: int,
    shapes: list[Shape],
    *,
    name: Callable[[int], str],
    require: dict[str, str] | None = None,
    strategy: str = "pack",
    ttl: float = TTL,
    note: Callable[[str], None] | None = None,
    stopped: Callable[[], bool] | None = None,
) -> Holds | None:
    """Hold every slot on live peers, all or none, waiting while they are
    taken (random, growing pauses between tries; `note` hears why each try
    failed). Raises ``Infeasible`` (naming the first slot, as `name` words
    it) when the live peers could not hold them all even idle; None once
    `stopped`."""
    from ._future import waiting

    require = dict(require or {})
    attempt, gone = 0, ""  # gone: the last peer that never could hold its slots, and why
    refused: dict[str, int] = {}
    never: set[tuple[str, Shape]] = set()
    with waiting():  # a call admitting lends its own CPUs meanwhile: they may be part of the room it needs
        while not (stopped and stopped()):
            peers, _ = plan(shapes, require, strategy, True, refused, never)
            if peers is None:
                peers, fit = plan(shapes, require, strategy, False, refused, never)
                if peers is None:
                    cpus, gpus, device = shapes[fit]
                    what = f"cpus={cpus:g}" + (f", gpus={gpus:g}" if gpus else "")
                    what += f", device={device!r}" if device else ""
                    where = f" with labels {require}" if require else ""
                    where += f" beside the {fit} before it" if fit else ""
                    why = f"{name(fit)} fits on no live peer{where} ({what} each; strategy {strategy!r})"
                    raise Infeasible(why + gone)
            holds = Holds(owner, generation, peers, shapes, ttl)
            refusal = holds.take()
            if refusal is None:
                return holds
            peer, why, permanent = refusal
            if permanent:  # plan again at once, without that peer for those slots
                never.update((peer, s) for s in holds._slots[peer])
                gone = f"; peer {peer[:12]}: {why}"
                continue
            refused[peer] = refused.get(peer, 0) + 1
            if note is not None:
                note(f"peer {peer[:12]}: {why}")
            attempt += 1
            time.sleep(random.uniform(0.05, min(4.0, 0.1 * 2**attempt)))
    return None


def holds_on(peer: str = "") -> list[str]:
    """What a peer's ledger holds now (this peer's by default), one line per owner."""
    from ._runtime import Runtime

    return Runtime.get().client.hold(bytes.fromhex(peer), "", 0, [], 0)[2]
