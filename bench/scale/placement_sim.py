"""Placement with no global scheduler: how close does it get to an ideal one?

An event-driven simulation of the placement path in docs/architecture.md §6:

1. Run on the submitting peer if it has a free slot (no messages).
2. Otherwise probe `d` random peers in the same cell and start on the one
   with the most free slots.
3. If every probed peer is full, read the (stale) cell summaries, pick the
   freer of two random cells, and probe `d` peers there.
4. If still nothing is free, leave a reservation on every probed peer (late
   binding, as in Sparrow): the first of them to free a slot runs the task.
5. A peer that finishes a task with nothing queued probes `s` peers in its
   cell and steals the oldest waiting task.

It is compared with the ideal (one global FIFO over every slot, which no real
system can run at fleet scale) and with two naive policies.

    python bench/scale/placement_sim.py            # the table in docs/architecture.md
    python bench/scale/placement_sim.py --quick    # smaller, for a smoke run

The model is deliberately simple: probes are answered with live state and take
no time, and service times are lognormal (sigma 1, a heavy tail). It checks the
policy, not the network.
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import random
from collections import deque
from dataclasses import dataclass, field

ARRIVE, DONE, SUMMARY = 0, 1, 2


@dataclass
class Config:
    name: str
    cells: int
    peers_per_cell: int
    slots: int
    load: float  # offered load as a fraction of all slots
    policy: str  # ideal | local | random | chimaera
    mean_service: float = 1.0  # seconds
    sigma: float = 1.0  # lognormal shape
    hot_cells: float = 0.0  # fraction of cells that are hot
    hot_share: float = 0.0  # fraction of arrivals landing on hot cells
    d: int = 2  # probes per placement step
    steal_probes: int = 2
    summary_every: float = 2.0  # seconds between cell summaries
    horizon: float = 20.0
    warmup: float = 5.0
    seed: int = 1


@dataclass
class Task:
    arrive: float
    service: float
    ingress: int
    started: bool = False


@dataclass
class Result:
    config: Config
    waits: list[float] = field(default_factory=list)
    local: int = 0
    in_cell: int = 0
    cross_cell: int = 0
    stolen: int = 0
    messages: int = 0
    measured: int = 0
    busy_slot_seconds: float = 0.0

    def row(self) -> dict:
        w = sorted(self.waits)
        n = len(w)

        def q(p: float) -> float:
            return w[min(n - 1, int(p * n))] * 1000 if n else float("nan")

        placed = max(1, self.local + self.in_cell + self.cross_cell + self.stolen)
        c = self.config
        return {
            "name": c.name,
            "policy": c.policy,
            "slots": c.cells * c.peers_per_cell * c.slots,
            "cells": c.cells,
            "load": c.load,
            "tasks": n,
            "wait_mean_ms": round(sum(w) / n * 1000, 1) if n else None,
            "wait_p50_ms": round(q(0.50), 1),
            "wait_p99_ms": round(q(0.99), 1),
            "wait_p999_ms": round(q(0.999), 1),
            "local_pct": round(100 * self.local / placed, 1),
            "in_cell_pct": round(100 * self.in_cell / placed, 1),
            "cross_cell_pct": round(100 * self.cross_cell / placed, 1),
            "stolen_pct": round(100 * self.stolen / placed, 1),
            "messages_per_task": round(self.messages / max(1, self.measured), 2),
        }


def service_time(rng: random.Random, mean: float, sigma: float) -> float:
    mu = math.log(mean) - sigma * sigma / 2
    return rng.lognormvariate(mu, sigma)


def simulate(c: Config) -> Result:
    rng = random.Random(c.seed)
    n = c.cells * c.peers_per_cell
    total_slots = n * c.slots
    rate = c.load * total_slots / c.mean_service
    res = Result(c)

    free = [c.slots] * n
    queues: list[deque[Task]] = [deque() for _ in range(n)]
    cell_free = [c.peers_per_cell * c.slots] * c.cells  # live
    summary = list(cell_free)  # what other cells last heard
    global_free = total_slots
    global_queue: deque[Task] = deque()

    hot = set(range(int(round(c.hot_cells * c.cells))))
    cold = [x for x in range(c.cells) if x not in hot]
    hot_list = sorted(hot)

    events: list[tuple[float, int, int, int, Task | None]] = []
    seq = 0

    def push(t: float, kind: int, peer: int, task: Task | None) -> None:
        nonlocal seq
        seq += 1
        heapq.heappush(events, (t, seq, kind, peer, task))

    def ingress_peer() -> int:
        if hot_list and rng.random() < c.hot_share:
            cell = rng.choice(hot_list)
        else:
            cell = rng.choice(cold) if cold else rng.randrange(c.cells)
        return cell * c.peers_per_cell + rng.randrange(c.peers_per_cell)

    def start(peer: int, task: Task, now: float, stolen: bool = False) -> None:
        nonlocal global_free
        task.started = True
        if task.arrive >= c.warmup:
            res.waits.append(now - task.arrive)
            if stolen:
                how = "stolen"
            elif c.policy == "ideal" or peer == task.ingress:
                how = "local"
            elif peer // c.peers_per_cell == task.ingress // c.peers_per_cell:
                how = "in_cell"
            else:
                how = "cross_cell"
            setattr(res, how, getattr(res, how) + 1)
        if c.policy == "ideal":
            global_free -= 1
        else:
            free[peer] -= 1
            cell_free[peer // c.peers_per_cell] -= 1
        if now >= c.warmup:
            res.busy_slot_seconds += task.service
        push(now + task.service, DONE, peer, None)

    def probe(cell: int, exclude: int, k: int) -> list[int]:
        base = cell * c.peers_per_cell
        picks: list[int] = []
        while len(picks) < min(k, c.peers_per_cell - (1 if exclude >= 0 else 0)):
            p = base + rng.randrange(c.peers_per_cell)
            if p != exclude and p not in picks:
                picks.append(p)
        return picks

    def count(task: Task, m: int) -> None:
        if task.arrive >= c.warmup:
            res.messages += m

    def place(task: Task, p: int, now: float) -> None:
        if c.policy == "ideal":
            if global_free > 0:
                start(0, task, now)
            else:
                global_queue.append(task)
            return
        if c.policy == "local":
            if free[p] > 0:
                start(p, task, now)
            else:
                queues[p].append(task)
            return
        cell = p // c.peers_per_cell
        if c.policy == "random":
            q = cell * c.peers_per_cell + rng.randrange(c.peers_per_cell)
            count(task, 1)
            if free[q] > 0:
                start(q, task, now)
            else:
                queues[q].append(task)
            return
        # chimaera
        if free[p] > 0:
            start(p, task, now)
            return
        probed = probe(cell, p, c.d)
        count(task, 2 * len(probed))
        best = max(probed, key=lambda x: free[x], default=None)
        if best is not None and free[best] > 0:
            start(best, task, now)
            return
        if c.cells > 1:
            if c.cells == 2:
                other = 1 - cell
            else:
                a, b = _two_others(rng, c.cells, cell)
                other = a if summary[a] >= summary[b] else b
            remote = probe(other, -1, c.d)
            count(task, 2 * len(remote))
            rbest = max(remote, key=lambda x: free[x], default=None)
            if rbest is not None and free[rbest] > 0:
                start(rbest, task, now)
                return
            probed += remote
        # Late binding: a reservation on every probed peer and on the ingress.
        for x in [p] + probed:
            queues[x].append(task)

    def finish(peer: int, now: float) -> None:
        nonlocal global_free
        if c.policy == "ideal":
            global_free += 1
            while global_queue:
                t = global_queue.popleft()
                start(0, t, now)
                break
            return
        free[peer] += 1
        cell_free[peer // c.peers_per_cell] += 1
        q = queues[peer]
        while q:
            t = q.popleft()
            if not t.started:
                start(peer, t, now)
                return
        if c.policy != "chimaera":
            return
        cell = peer // c.peers_per_cell
        victims = probe(cell, peer, c.steal_probes)
        res.messages += 2 * len(victims) if now >= c.warmup else 0
        for v in sorted(victims, key=lambda x: len(queues[x]), reverse=True):
            vq = queues[v]
            while vq:
                t = vq.popleft()
                if not t.started:
                    start(peer, t, now, stolen=True)
                    return

    # Arrivals as one Poisson stream.
    t = rng.expovariate(rate)
    push(t, ARRIVE, -1, None)
    if c.policy == "chimaera":
        push(c.summary_every, SUMMARY, -1, None)
    end = c.warmup + c.horizon
    while events:
        now, _, kind, peer, _task = heapq.heappop(events)
        if now > end:
            break
        if kind == ARRIVE:
            p = ingress_peer()
            task = Task(now, service_time(rng, c.mean_service, c.sigma), p)
            if now >= c.warmup:
                res.measured += 1
            place(task, p, now)
            push(now + rng.expovariate(rate), ARRIVE, -1, None)
        elif kind == DONE:
            finish(peer, now)
        else:
            summary[:] = cell_free
            if now >= c.warmup:
                res.messages += c.cells  # one summary per cell per period (fleet-wide, not per task)
            push(now + c.summary_every, SUMMARY, -1, None)
    return res


def _two_others(rng: random.Random, cells: int, me: int) -> list[int]:
    picks: list[int] = []
    while len(picks) < 2:
        x = rng.randrange(cells)
        if x != me and x not in picks:
            picks.append(x)
    return picks


def suite(quick: bool) -> list[Config]:
    if quick:
        base = dict(cells=8, peers_per_cell=32, slots=8, horizon=8.0, warmup=2.0)
    else:
        base = dict(cells=16, peers_per_cell=256, slots=8)
    out: list[Config] = []
    for load in (0.7, 0.9):
        for policy in ("ideal", "local", "random", "chimaera"):
            out.append(Config(f"uniform {load:.0%}", policy=policy, load=load, **base))
    for policy in ("ideal", "chimaera"):
        out.append(Config("uniform 95%", policy=policy, load=0.95, **base))
    for policy in ("ideal", "random", "chimaera"):
        out.append(Config("hot cells 70%", policy=policy, load=0.7, hot_cells=0.125, hot_share=0.4, **base))
    # Fleet-size invariance: same slots, more and smaller cells.
    if not quick:
        for cells, ppc in ((4, 1024), (64, 64), (256, 16)):
            out.append(Config(f"{cells} cells 90%", policy="chimaera", load=0.9, cells=cells, peers_per_cell=ppc, slots=8))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    rows = [simulate(c).row() for c in suite(a.quick)]
    cols = ["name", "policy", "slots", "cells", "wait_p50_ms", "wait_p99_ms", "wait_p999_ms",
            "local_pct", "in_cell_pct", "cross_cell_pct", "stolen_pct", "messages_per_task"]
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    for r in rows:
        print("| " + " | ".join(str(r[k]) for k in cols) + " |")
    if a.json:
        with open(a.json, "w") as f:
            json.dump(rows, f, indent=1)


if __name__ == "__main__":
    main()
