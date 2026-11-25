"""Replay one CPU pool's workloads against today's provisioner rule and the
forecast planner, on the same arrivals and the same provider startup times.

    python bench/autoscale/replay.py            # every trace, full length
    python bench/autoscale/replay.py --quick    # shorter traces, a few seconds
    python bench/autoscale/replay.py --json     # results and gates as JSON

A deterministic event simulation, in the spirit of bench/scale/placement_sim.py:
peers of one shape start calls from one first-in, first-out queue, and every
`--interval` seconds a provisioner looks at the cluster as
``ch.runs.cluster().nodes()`` would show it, each node with the demand report
its peer sends (queued calls by shape with their estimated run time, running
calls' cores by time left, cumulative starts). Each policy then asks for a
pool size: `decide` is today's ``chimaera.provisioner.decide``; `forecast` is
``chimaera._planner.plan`` for growth, with today's shrink rule when it holds
and nothing it asked for is still starting. Both shrink by today's rule, so
only growth differs.

Estimates are what a peer has: a function's run time is the median of its
finished calls, and a function with none finished yet is "cold", its estimate
the longest a running call of it has run (at least 1 s). A peer asked for is
usable after a startup drawn per pod ordinal from the trace's seeded model,
the same for both policies; it is paid for from the request on. Calls not yet
started at the end count their wait up to the end. Gates are in `gates()`.
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import random
import sys
from collections import deque
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "python"))

from chimaera._planner import Pool, Startup, Target, arrival_rate, plan  # noqa: E402
from chimaera.provisioner import Memory, Rules, Victim, decide  # noqa: E402

GiB = 1 << 30
BUCKETS = (1.0, 4.0, 16.0, 64.0, 256.0)
STALE_AGE_S = 30.0


@dataclass
class Job:
    at: float  # submitted
    wall: float
    fn: str
    cpus: float = 1.0
    memory: int = 0
    after: int | None = None  # the call it waits for (a DAG's child): runnable when that ends
    actor: bool = False  # an actor's method call: runs on the actor, never pool demand


@dataclass
class Trace:
    name: str
    jobs: list[Job]
    end_s: float
    base: int = 1  # peers there from the start (and the pool's minimum)
    base_cpus: float = 8.0
    base_memory: int = 32 * GiB
    peer_cpus: float = 8.0
    peer_memory: int = 32 * GiB
    max: int = 16
    startup: tuple[Any, ...] = ("fixed", 60.0)
    target_start: float = 60.0
    stale: tuple[int, ...] = ()  # pod ordinals whose load reports are 30 s old
    lag_s: float = 0.0  # a new peer runs calls this long before nodes() shows it
    rejected: int = 0  # calls refused at submit: no peer could ever fit them


@dataclass
class Peer:
    ordinal: int
    cpus: float
    memory: int
    requested: float
    ready: float
    visible: float
    removed: float | None = None
    used: float = 0.0
    running: dict[int, float] = field(default_factory=dict)  # job -> start
    started: int = 0
    started_core_s: float = 0.0
    hosts_actor: bool = False
    base: bool = False  # there from the start, not asked for


def _pct(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    return s[max(0, math.ceil(q * len(s)) - 1)]


class Sim:
    def __init__(self, trace: Trace, policy: str, seed: int, interval: float) -> None:
        self.t, self.policy, self.seed, self.interval = trace, policy, seed, interval
        self.now = 0.0
        self.peers: list[Peer] = []
        self.requests: dict[int, int] = {}  # pod ordinal -> times asked for (for its startup draw)
        for _ in range(trace.base):
            self._add(0.0, base=True)
        self.queue: deque[int] = deque()
        self.runnable_at: dict[int, float] = {}
        self.start: dict[int, float] = {}
        self.done: dict[int, float] = {}
        self.walls: dict[str, list[float]] = {}
        self.children: dict[int, list[int]] = {}
        self.events: list[tuple[float, int, int, int]] = []  # (time, kind, seq, payload)
        self.seq = 0
        self.actor_free = 0.0
        self.grows = 0
        self.max_replicas = trace.base
        self.rules = Rules(min=trace.base, max=trace.max, up_after=30.0, down_after=300.0, start_grace=300.0)
        self.memory = Memory()
        self.history: list[tuple[float, list[Any]]] = []
        self.reports: dict[int, list[tuple[float, Any]]] = {}
        for i, j in enumerate(trace.jobs):
            if j.after is not None:
                self.children.setdefault(j.after, []).append(i)
            elif j.actor:
                self._push(j.at, 2, i)
            else:
                self._push(j.at, 1, i)
        if any(j.actor for j in trace.jobs):
            self.peers[0].used += 1.0  # the actor holds a core for as long as it lives
            self.peers[0].hosts_actor = True
        t = 0.0
        while t <= trace.end_s:
            self._push(t, 3, -1)
            t += interval

    def _push(self, at: float, kind: int, payload: int) -> None:
        # kind: 0 finish, 1 runnable, 2 actor call, 3 provisioner look
        self.seq += 1
        heapq.heappush(self.events, (at, kind, self.seq, payload))

    def _startup(self, ordinal: int) -> float:
        nth = self.requests.get(ordinal, 0)
        self.requests[ordinal] = nth + 1
        model = self.t.startup
        if model[0] == "fixed":
            return float(model[1])
        rng = random.Random(f"{self.seed}:{self.t.name}:{ordinal}:{nth}")
        return float(model[1]) * math.exp(float(model[2]) * rng.gauss(0.0, 1.0))

    def _add(self, now: float, base: bool = False) -> None:
        ordinal = len(self.active())
        if base:
            cpus, mem, ready = self.t.base_cpus, self.t.base_memory, 0.0
        else:
            cpus, mem, ready = self.t.peer_cpus, self.t.peer_memory, now + self._startup(ordinal)
        visible = ready if base else ready + self.t.lag_s
        self.peers.append(Peer(ordinal, cpus, mem, now, ready, visible, base=base))

    def active(self) -> list[Peer]:
        return [p for p in self.peers if p.removed is None]

    def usable(self) -> list[Peer]:
        return [p for p in self.active() if p.ready <= self.now]

    # --- estimates, as a peer has them
    def estimate(self, fn: str) -> tuple[float, bool]:
        """A call of `fn`'s run time, and whether that is only a lower bound (cold)."""
        w = self.walls.get(fn)
        if w:
            return _pct(w, 0.5), False
        running = [self.now - s for p in self.peers for j, s in p.running.items() if self.t.jobs[j].fn == fn]
        return max([1.0, *running]), True

    # --- the run
    def run(self) -> dict[str, Any]:
        while self.events:
            at, kind, _, x = heapq.heappop(self.events)
            if at > self.t.end_s:
                break
            self.now = at
            if kind == 0:
                self._finish(x)
            elif kind == 1:
                self.runnable_at[x] = at
                self.queue.append(x)
            elif kind == 2:
                self._actor_call(x)
            else:
                self._look()
            if not self.events or self.events[0][0] > at:
                self._dispatch()
        return self._result()

    def _actor_call(self, x: int) -> None:
        j = self.t.jobs[x]
        self.runnable_at[x] = j.at
        s = max(self.actor_free, j.at)
        self.start[x] = s
        self.actor_free = s + j.wall
        if self.actor_free <= self.t.end_s:
            self.done[x] = self.actor_free

    def _finish(self, x: int) -> None:
        j = self.t.jobs[x]
        for p in self.peers:
            if x in p.running:
                del p.running[x]
                p.used -= j.cpus
        self.done[x] = self.now
        self.walls.setdefault(j.fn, []).append(j.wall)
        for c in self.children.get(x, ()):
            self._push(self.now, 1, c)

    def _dispatch(self) -> None:
        peers = [p for p in self.usable() if p.cpus - p.used > 1e-9]
        skipped: list[int] = []
        while self.queue and peers and len(skipped) < 64:
            x = self.queue.popleft()
            j = self.t.jobs[x]
            p = next((p for p in peers if p.cpus - p.used >= j.cpus - 1e-9 and p.memory >= j.memory), None)
            if p is None:
                skipped.append(x)
                continue
            p.used += j.cpus
            p.running[x] = self.now
            est, _ = self.estimate(j.fn)
            p.started += 1
            p.started_core_s += j.cpus * est
            self.start[x] = self.now
            self._push(self.now + j.wall, 0, x)
            peers = [q for q in peers if q.cpus - q.used > 1e-9]
        self.queue.extendleft(reversed(skipped))

    # --- what the provisioner sees
    def _report(self, p: Peer, queued: list[int]) -> SimpleNamespace:
        shapes: dict[tuple[float, int, int], dict[str, Any]] = {}
        for x in queued:
            j = self.t.jobs[x]
            est, cold = self.estimate(j.fn)
            b = next((i for i, u in enumerate(BUCKETS) if est <= u), len(BUCKETS))
            s = shapes.setdefault(
                (j.cpus, j.memory, b),
                {"cpus": j.cpus, "memory": j.memory, "queued": 0, "wall_s": 0.0, "cold": 0, "actors": 0},
            )
            s["queued"] += 1
            s["wall_s"] += est
            s["cold"] += cold
        releases: dict[float, float] = {}
        for x, start in p.running.items():
            j = self.t.jobs[x]
            est, cold = self.estimate(j.fn)
            left = math.inf if cold else max(0.0, est - (self.now - start))
            upper = next((u for u in BUCKETS if left <= u), math.inf)
            releases[upper] = releases.get(upper, 0.0) + j.cpus
        if p.hosts_actor:
            releases[math.inf] = releases.get(math.inf, 0.0) + 1.0
        demand = {
            "shapes": sorted(shapes.values(), key=lambda s: -s["wall_s"])[:16],
            "releases": sorted(releases.items()),
            "started_total": p.started,
            "started_core_s_total": p.started_core_s,
            "oldest_ready_s": max((self.now - self.runnable_at[x] for x in queued), default=0.0),
            "excluded": {"labels": 0, "devices": 0},
            "gangs": [],
        }
        return SimpleNamespace(
            peer=f"p{p.ordinal}",
            address=f"chimaera-{p.ordinal}.peers:7700",
            cpus=p.cpus,
            memory_bytes=p.memory,
            gpus=[],
            alive=True,
            cpu_used=p.used,
            queued=len(queued),
            this_peer=p.ordinal == 0,
            labels={},
            actors=int(p.hosts_actor),
            object_bytes=0,
            draining=False,
            object_layout=0,
            starting=False,
            load_age_s=0.0,
            incarnation=0,
            demand=demand,
        )

    def nodes(self) -> list[Any]:
        shown = [p for p in self.active() if p.visible <= self.now]
        # Queued calls sit on the peers that were granted them: spread evenly.
        per: dict[int, list[int]] = {p.ordinal: [] for p in shown}
        for i, x in enumerate(self.queue):
            if shown:
                per[shown[i % len(shown)].ordinal].append(x)
        out = []
        for p in shown:
            n = self._report(p, per[p.ordinal])
            self.reports.setdefault(p.ordinal, []).append((self.now, n))
            if p.ordinal in self.t.stale:
                old = [(t, r) for t, r in self.reports[p.ordinal] if t <= self.now - STALE_AGE_S]
                t, n = old[-1] if old else self.reports[p.ordinal][0]
                n = SimpleNamespace(**{**vars(n), "load_age_s": self.now - t})
            out.append(n)
        return out

    def _look(self) -> None:
        nodes = self.nodes()
        active = self.active()
        replicas = len(active)
        # The unit a shrink removes is the highest ordinal: its peer if it
        # shows, else one whose pod never started (the replay knows).
        last = f"p{replicas - 1}"
        node = next((n for n in nodes if n.peer == last), None)
        victim = Victim("joined", last, node=node) if node is not None else Victim("never_joined", last, "not up yet")
        want, _ = decide(nodes, replicas, self.rules, self.memory, self.now, victim)
        if self.policy == "forecast":
            self.history = [h for h in self.history if h[0] >= self.now - 300.0] + [(self.now, nodes)]
            samples = tuple(p.visible - p.requested for p in self.peers if not p.base and p.visible <= self.now)
            startup = Startup(samples, 60.0).seconds()
            shown = {n.peer for n in nodes}
            pending = tuple(
                Startup(samples, 60.0).remaining(self.now - p.requested)
                for p in active
                if f"p{p.ordinal}" not in shown or p.ready > self.now
            )
            pool = Pool(self.t.peer_cpus, self.t.peer_memory, replicas, self.t.base, self.t.max, pending)
            p = plan(nodes, pool, Target(start_s=self.t.target_start), startup, arrival_rate(self.history))
            if p.want > replicas:
                want = p.want
            elif want > replicas or pending:
                want = replicas  # today's rule shrinks; the planner alone grows
        self._scale(want)

    def _scale(self, want: int) -> None:
        active = self.active()
        if want > len(active):
            self.grows += 1
            for _ in range(want - len(active)):
                self._add(self.now)
        elif want < len(active):
            top = max(active, key=lambda p: p.ordinal)
            if not top.running and not top.hosts_actor and not top.base:
                top.removed = self.now
        self.max_replicas = max(self.max_replicas, len(self.active()))

    def _result(self) -> dict[str, Any]:
        end = self.t.end_s
        jobs = range(len(self.t.jobs))
        waits = [self.start.get(x, end) - self.runnable_at[x] for x in jobs if x in self.runnable_at]
        # A DAG child whose parent never ended was never runnable: it has no wait.
        latency = [self.done.get(x, end) - self.runnable_at[x] for x in jobs if x in self.runnable_at]
        peer_s = sum(min(p.removed if p.removed is not None else end, end) - p.requested for p in self.peers)
        return {
            "completions": len(self.done),
            "unfinished": len(self.t.jobs) - len(self.done),
            "rejected": self.t.rejected,
            "peer_s": round(peer_s, 1),
            "start_p95_s": round(_pct(waits, 0.95), 1),
            "start_p99_s": round(_pct(waits, 0.99), 1),
            "completion_p99_s": round(_pct(latency, 0.99), 1),
            "max_replicas": self.max_replicas,
            "grows": self.grows,
        }


def _poisson(rng: random.Random, rate: float, start: float, stop: float) -> list[float]:
    out: list[float] = []
    t = start
    while True:
        t += rng.expovariate(rate)
        if t >= stop:
            return out
        out.append(t)


def traces(quick: bool, seed: int) -> list[Trace]:
    q = 0.6 if quick else 1.0
    rng = random.Random(seed)
    burst = lambda at, n, wall, fn: [Job(at, wall, fn) for _ in range(n)]  # noqa: E731
    out = [
        Trace("short_burst", burst(10, 200, 2.0, "a"), end_s=900 * q),
        Trace("recurring_bursts", [j for at in (10, 310, 610)[: 2 if quick else 3] for j in burst(at, 200, 2.0, "a")],
              end_s=900 * q),
        Trace("persistent", [Job(t, 4.0, "p") for t in _poisson(rng, 8.0, 0.0, 500 * q)], end_s=750 * q,
              startup=("lognormal", 60.0, 0.3)),
        Trace("long_calls", burst(0, 12, 300.0, "l"), end_s=900 * q),
        Trace("alternating_sizes", [Job(t, 0.5 if i % 2 else 8.0, "s" if i % 2 else "l")
                                    for i, t in enumerate(_poisson(rng, 3.0, 0.0, 400 * q))], end_s=600 * q),
        Trace("blocked_dag", burst(0, 20, 30.0, "parent") + [Job(0, 2.0, "child", after=i) for i in range(20)
                                                             for _ in range(20)], end_s=600 * q),
        Trace("serial_actor", [Job(0, 1.0, "m", actor=True) for _ in range(300)] + burst(0, 50, 1.0, "f"),
              end_s=600 * q),
        Trace("stale_load", burst(10, 600, 2.0, "a"), end_s=600 * q, base=2, stale=(1,)),
        Trace("lagging_inventory", [Job(t, 4.0, "p") for t in _poisson(rng, 5.0, 0.0, 400 * q)], end_s=600 * q,
              lag_s=30.0),
        # Calls only the big existing peer fits (12 CPUs; pool peers have 8), and
        # calls no peer fits, which submit refuses, so they never show as demand.
        Trace("infeasible", [Job(0, 20.0, "big", cpus=12.0) for _ in range(30)], end_s=900 * q, base_cpus=16.0,
              base_memory=64 * GiB, rejected=50),
        Trace("already_clearing", burst(0, 640, 1.0, "a"), end_s=400 * q, base=2),
        Trace("recurring_heavy", [j for at in (10, 310) for j in burst(at, 1200, 2.0, "a")], end_s=700 * q),
    ]
    return out


BURSTS = ("short_burst", "recurring_bursts")


def gates(results: dict[str, dict[str, dict[str, Any]]], targets: dict[str, float]) -> list[dict[str, Any]]:
    """Pass or fail, with the numbers behind each."""
    out = []
    grew = {n: results[n]["forecast"]["grows"] for n in ("infeasible", "already_clearing")}
    out.append({"gate": "no growth for work new peers cannot help", "pass": all(g == 0 for g in grew.values()),
                "detail": f"forecast grows: {grew}"})
    f = sum(results[n]["forecast"]["peer_s"] for n in BURSTS)
    d = sum(results[n]["decide"]["peer_s"] for n in BURSTS)
    matched = all(results[n]["forecast"]["completions"] >= results[n]["decide"]["completions"] for n in BURSTS)
    in_target = all(results[n]["forecast"]["start_p95_s"] <= targets[n] for n in BURSTS)
    out.append({"gate": "bursts: >= 10% fewer peer-seconds at matched completions, p95 start within target",
                "pass": f <= 0.9 * d and matched and in_target,
                "detail": f"peer-s forecast {f:.0f} vs decide {d:.0f} ({(1 - f / d) * 100:+.0f}% saved); "
                          f"completions matched: {matched}; p95 start within target: {in_target}"})
    fp, dp = results["persistent"]["forecast"]["completion_p99_s"], results["persistent"]["decide"]["completion_p99_s"]
    out.append({"gate": "persistent load: completion p99 <= 1.05x today's", "pass": fp <= 1.05 * dp,
                "detail": f"forecast {fp:.0f} s vs decide {dp:.0f} s"})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--quick", action="store_true", help="shorter traces")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between provisioner looks")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    results: dict[str, dict[str, dict[str, Any]]] = {}
    ts = traces(a.quick, a.seed)
    for t in ts:
        results[t.name] = {pol: Sim(t, pol, a.seed, a.interval).run() for pol in ("decide", "forecast")}
    g = gates(results, {t.name: t.target_start for t in ts})
    if a.json:
        print(json.dumps({"seed": a.seed, "quick": a.quick, "interval": a.interval, "results": results, "gates": g}))
    else:
        cols = ("completions", "unfinished", "peer_s", "start_p95_s", "start_p99_s", "completion_p99_s",
                "max_replicas", "grows")
        print(f"seed {a.seed}{' (quick)' if a.quick else ''}, look every {a.interval:g} s")
        print(f"{'trace':<20}{'policy':<10}" + "".join(f"{c:>17}" for c in cols))
        for name, by in results.items():
            for pol, r in by.items():
                print(f"{name:<20}{pol:<10}" + "".join(f"{r[c]:>17}" for c in cols))
        for x in g:
            print(f"{'PASS' if x['pass'] else 'FAIL'}  {x['gate']}: {x['detail']}")
    return 0 if all(x["pass"] for x in g) else 1


if __name__ == "__main__":
    sys.exit(main())
