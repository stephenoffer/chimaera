"""How many peers a pool of identical peers should have, from the work that is
runnable now and how long it takes.

``plan()`` is a pure function of a snapshot: the nodes of
``ch.runs.cluster().nodes()``, each with its peer's demand report, and the
pool's size and limits. It reads no clock and draws no randomness, so the
provisioner, a preview and a replay get the same answer from the same inputs.

It simulates the queued calls on the peers there are, on the peers already
asked for (usable when they are expected to be) and on each candidate number of
new ones (usable after the provider's startup time). It asks for the fewest new
peers that meet the target: every queued call starts within ``start_s`` and,
when set, finishes within ``finish_s``. When no number of peers meets it, the
plan says so and names what binds. It then asks for the fewest peers that come
as close as any, leaving out peers that would run less work than the time they
take to start.

Only confirmed demand drives growth: a peer that is starting, whose load report
is stale, or that sends no demand report (an older peer) has its queue left out,
and the plan's ``inputs`` say whose and why.
"""

from __future__ import annotations

import bisect
import heapq
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, NamedTuple, Sequence

# A peer's load older than this does not count as demand.
STALE_S = 10.0
# Calls simulated at most; the rest are left out and the plan says how many.
MAX_CALLS = 20_000
# Calls predicted to arrive, at most: more are merged, keeping their work.
MAX_ARRIVALS = 2_000
# Up to this many candidates are each simulated; beyond, a binary search.
LINEAR_UP_TO = 16
EPS = 1e-6
# Startup times a Forecast keeps.
KEEP_SAMPLES = 32


@dataclass(frozen=True)
class Pool:
    """The managed pool. ``replicas`` counts every peer asked for, usable or
    not. ``pending`` holds, for each asked-for peer that is not usable yet
    (not joined, or joined and still starting), the seconds until it is
    expected to be."""

    peer_cpus: float
    peer_memory: int
    replicas: int
    min: int
    max: int
    pending: tuple[float, ...] = ()


@dataclass(frozen=True)
class Target:
    """Every queued call starts within ``start_s`` and, when set, finishes
    within ``finish_s`` (seconds from now)."""

    start_s: float | None = 60.0
    finish_s: float | None = None


@dataclass(frozen=True)
class Startup:
    """Seconds from asking for a peer to its first usable moment: the 90th
    percentile of what was seen, once there are three samples; until then
    `default_s`, which should be a 90th percentile too (an estimate of a
    typical startup leaves the plan short when startups run long)."""

    samples: tuple[float, ...] = ()
    default_s: float = 60.0

    def seconds(self) -> float:
        if len(self.samples) < 3:
            return self.default_s
        s = sorted(self.samples)
        return s[math.ceil(0.9 * len(s)) - 1]

    def remaining(self, elapsed: float) -> float:
        """Seconds until a peer asked for `elapsed` seconds ago is expected
        usable. Startup times have a long tail: the later a peer already is,
        the longer it is likely to take still, so one past the estimate is
        expected to take half as long again as it has so far."""
        return max(self.seconds() - elapsed, 0.5 * elapsed)


@dataclass(frozen=True)
class Plan:
    want: int  # peers to ask for in all
    action: str  # "hold" or "grow"
    reason: str
    latest_start_s: float  # when the last queued call starts, at `want`
    finish_s: float  # when the last queued call with a known end finishes
    missed: str | None  # the target that cannot be met, and by how much
    binding: str | None  # what stops a better plan
    runner_up: tuple[int, str] | None  # the next-best size, and why it lost
    reconsider_s: float
    inputs: dict[str, Any] = field(default_factory=dict)


# One call to place: (runnable at, cpus, memory, wall seconds, queued now,
# how many calls it stands for).
Call = tuple[float, float, int, float, bool, int]
# One peer: (cpus, memory bytes, usable at, [(frees at, cores)]).
Peer = tuple[float, int, float, list[tuple[float, float]]]


def _usable(n: Any) -> bool:
    return bool(getattr(n, "alive", False)) and not getattr(n, "draining", False)


def _size(b: int) -> str:
    return f"{b / 2**30:.0f} GiB" if b >= 2**30 else f"{b / 2**20:.0f} MiB"


def read(nodes: Sequence[Any], pool: Pool, stale_s: float = STALE_S) -> tuple[list[Call], list[Peer], dict[str, Any]]:
    """The queued calls (confirmed demand only), the usable peers with their
    running calls' release times, and what was read, left out and why."""
    supply: list[Peer] = []
    shapes: list[tuple[float, int, int, float, int]] = []  # cpus, memory, queued, wall_s, actors
    gangs: dict[str, dict[str, Any]] = {}
    unconfirmed: list[dict[str, Any]] = []
    excluded: dict[str, Any] = {"labels": 0, "devices": 0, "too_big": [], "gangs": []}
    cold, oldest = 0, 0.0
    for n in nodes:
        if not _usable(n):
            continue
        d = getattr(n, "demand", None)
        starting = bool(getattr(n, "starting", False))
        age = float(getattr(n, "load_age_s", 0.0) or 0.0)
        if not starting:
            if d is not None:
                rel = [(float(t), float(c)) for t, c in d.get("releases", ())]
            else:  # an older peer: its running calls' ends are unknown
                rel = [(math.inf, float(n.cpu_used))] if n.cpu_used else []
            supply.append((float(n.cpus), int(n.memory_bytes), 0.0, rel))
        if starting:
            why = "starting"
        elif d is None:
            why = "no demand report (an older peer)"
        elif age > stale_s:
            why = f"load report {age:.0f} s old"
        else:
            why = ""
        if why:
            unconfirmed.append({"peer": getattr(n, "peer", ""), "why": why, "queued": int(n.queued)})
            continue
        assert d is not None
        for s in d.get("shapes", ()):
            shapes.append(
                (
                    float(s.get("cpus", 1.0)),
                    int(s.get("memory", 0)),
                    int(s.get("queued", 0)),
                    float(s.get("wall_s", 0.0)),
                    int(s.get("actors", 0)),
                )
            )
            cold += int(s.get("cold", 0))
        for g in d.get("gangs", ()):  # the same gang may wait on several peers: count it once
            seen = gangs.get(g["owner"])
            if seen is None or g.get("generation", 0) > seen.get("generation", 0):
                gangs[g["owner"]] = g
        ex = d.get("excluded", {})
        excluded["labels"] += int(ex.get("labels", 0))
        excluded["devices"] += int(ex.get("devices", 0))
        oldest = max(oldest, float(d.get("oldest_ready_s", 0.0)))

    def fits(cpus: float, memory: int) -> bool:
        if cpus <= pool.peer_cpus + EPS and memory <= pool.peer_memory:
            return True
        return any(cpus <= c + EPS and memory <= m for c, m, _, _ in supply)

    calls: list[Call] = []
    for cpus, memory, queued, wall_s, actors in shapes:
        if not fits(cpus, memory):
            excluded["too_big"].append(f"{queued + actors} calls of {cpus:g} CPUs and {_size(memory)}: no peer fits them")
            continue
        per = wall_s / queued if queued else 0.0
        calls += [(0.0, cpus, memory, per, True, 1)] * queued
        # A waiting actor holds its CPUs for as long as it lives.
        calls += [(0.0, cpus, memory, math.inf, True, 1)] * actors
    for owner, g in gangs.items():
        bundles = [(float(b.get("cpus", 0.0)), int(b.get("memory", 0))) for b in g.get("bundles", ())]
        too_big = [b for b in bundles if not fits(*b)]
        if too_big:
            excluded["gangs"].append(f"gang {owner}: a bundle of {too_big[0][0]:g} CPUs fits no peer")
            continue
        # shortcut: a gang's bundles are placed one by one and held from then
        # on, where the real ledger holds them all at once; revisit with C8.
        calls += [(0.0, c, m, math.inf, True, 1) for c, m in bundles]
    queued_calls = len(calls)
    truncated = max(0, queued_calls - MAX_CALLS)
    del calls[MAX_CALLS:]
    inputs = {
        "queued": queued_calls,
        "core_s": sum(c[1] * c[3] for c in calls if math.isfinite(c[3])),
        "cold": cold,
        "oldest_ready_s": oldest,
        "peers": len(supply),
        "gangs": len(gangs) - len(excluded["gangs"]),
        "unconfirmed": unconfirmed,
        "excluded": excluded,
        "truncated": truncated,
    }
    return calls, supply, inputs


def queued_core_s(node: Any) -> float:
    d = getattr(node, "demand", None) or {}
    return sum(float(s.get("cpus", 1.0)) * float(s.get("wall_s", 0.0)) for s in d.get("shapes", ()))


def arrival_rate(history: Sequence[tuple[float, Sequence[Any]]], windows: Sequence[float] = (30.0, 300.0)) -> float:
    """Core-seconds of work per second that became runnable, sustained: the
    lowest of the rates over each window, each divided by the window's full
    length, so one burst does not read as load that keeps coming. `history`
    is (seconds, nodes) snapshots, oldest first."""
    if len(history) < 2:
        return 0.0
    now, cur = history[-1]
    rates = []
    for w in windows:
        old = next((h for h in history if h[0] >= now - w), None)
        rates.append(_work(cur, old[1]) / w if old is not None and old[0] < now else 0.0)
    return max(0.0, min(rates))


def _work(cur: Sequence[Any], old: Sequence[Any]) -> float:
    """Work that became runnable between two snapshots: what all the queues
    grew by (calls move between peers, so in total), plus what each peer
    started. A peer new since, or restarted, counts all it has started."""

    def key(n: Any) -> tuple[Any, Any]:
        return getattr(n, "peer", ""), getattr(n, "incarnation", 0)

    prev = {key(n): n for n in old if getattr(n, "demand", None) is not None}
    now = [n for n in cur if getattr(n, "demand", None) is not None]
    work = sum(queued_core_s(n) for n in now) - sum(queued_core_s(n) for n in prev.values())
    for n in now:
        b = prev.get(key(n))
        work += float(n.demand.get("started_core_s_total", 0.0))
        if b is not None:
            work -= float(b.demand.get("started_core_s_total", 0.0))
    return work


def _arrivals(calls: list[Call], rate: float, horizon: float) -> list[Call]:
    """Calls arriving at `rate` core-seconds per second over `horizon`, shaped
    like the queued ones (or 1 CPU for 1 s)."""
    finite = [c for c in calls if math.isfinite(c[3]) and c[3] > 0]
    if finite:
        cpus = round(sum(c[1] for c in finite) / len(finite) * 4) / 4 or 0.25
        wall = sum(c[3] for c in finite) / len(finite)
        memory = max(c[2] for c in finite)
    else:
        cpus, wall, memory = 1.0, 1.0, 0
    n = rate * horizon / (cpus * wall)
    merge = max(1, math.ceil(n / MAX_ARRIVALS))
    step = cpus * wall * merge / rate
    return [(i * step, cpus, memory, wall * merge, False, merge) for i in range(int(horizon / step))]


class Run(NamedTuple):
    wait: float  # the longest any call waits to start
    finish: float  # when the last queued call with a known end finishes
    late: float  # call-seconds past the target, summed over calls
    useful: list[float]  # core-seconds each peer runs


def _simulate(peers: list[Peer], calls: list[Call], unit: float, target: Target) -> Run:
    """Place `calls` in order, each on the peer where it starts first (that
    fits its CPUs and memory)."""
    units: list[list[float]] = []
    for cpus, _, ready, releases in peers:
        free = [ready] * int(cpus / unit + EPS)
        held = sorted((max(ready, t), round(c / unit)) for t, c in releases)
        i = 0
        for t, k in held:
            for _ in range(min(k, len(free) - i)):
                free[i] = t
                i += 1
        units.append(sorted(free))
    wait = finish = late = 0.0
    useful = [0.0] * len(peers)
    start_s = math.inf if target.start_s is None else target.start_s
    finish_s = math.inf if target.finish_s is None else target.finish_s

    def placed(i: int, s: float, call: Call) -> None:
        nonlocal wait, finish, late
        at, cpus, _, wall, queued, n = call
        wait = max(wait, s - at)
        late += n * max(0.0, s - at - start_s)
        if queued and math.isfinite(wall):
            finish = max(finish, s + wall)
            late += max(0.0, s + wall - finish_s)
        useful[i] += cpus * wall

    never = Run(math.inf, math.inf, math.inf, useful)
    # Every call takes one unit and fits every peer's memory: one heap of
    # free units serves them all, in order.
    memory = max((c[2] for c in calls), default=0)
    if all(c[1] <= unit + EPS for c in calls) and all(memory <= p[1] for p in peers):
        heap = [(t, i) for i, free in enumerate(units) for t in free]
        heapq.heapify(heap)
        for call in calls:
            if not heap or heap[0][0] == math.inf:
                return never
            t, i = heapq.heappop(heap)
            s = max(t, call[0])
            heapq.heappush(heap, (s + call[3], i))
            placed(i, s, call)
        return Run(wait, finish, late, useful)
    for call in calls:
        at, cpus, memory, wall = call[:4]
        u = max(1, math.ceil(cpus / unit - EPS))
        best: tuple[float, int] | None = None
        for i, (pcpus, pmem, _, _) in enumerate(peers):
            if cpus > pcpus + EPS or memory > pmem or u > len(units[i]):
                continue
            s = max(units[i][u - 1], at)
            if best is None or s < best[0]:
                best = (s, i)
        if best is None or best[0] == math.inf:
            return never
        s, i = best
        del units[i][:u]
        for _ in range(u):
            bisect.insort(units[i], s + wall)
        placed(i, s, call)
    return Run(wait, finish, late, useful)


def plan(
    nodes: Sequence[Any],
    pool: Pool,
    target: Target,
    startup_s: float,
    arrivals_core_per_s: float = 0.0,
    *,
    stale_s: float = STALE_S,
) -> Plan:
    """The pool size to ask for now, and why. See the module docstring."""
    calls, supply, inputs = read(nodes, pool, stale_s)
    inputs.update(
        pending=list(pool.pending),
        startup_s=startup_s,
        arrivals_core_per_s=arrivals_core_per_s,
        target={"start_s": target.start_s, "finish_s": target.finish_s},
    )
    n_queued = len(calls)
    kmax = max(0, pool.max - pool.replicas)
    if not calls and arrivals_core_per_s <= 0:
        if pool.replicas < pool.min:
            why = f"grow to the pool minimum of {pool.min} peers; nothing runnable is queued"
            return Plan(pool.min, "grow", why, 0.0, 0.0, None, None, None, 30.0, inputs)
        return Plan(pool.replicas, "hold", "hold: nothing runnable is queued", 0.0, 0.0, None, None, None, 30.0, inputs)
    horizon = 2 * (startup_s + (target.start_s or target.finish_s or 60.0))
    if arrivals_core_per_s > 0:
        calls = calls + _arrivals(calls, arrivals_core_per_s, horizon)
    whole = all(float(c[1]).is_integer() for c in calls) and all(float(p[0]).is_integer() for p in supply)
    unit = 1.0 if whole and float(pool.peer_cpus).is_integer() else 0.25
    base: list[Peer] = supply + [(pool.peer_cpus, pool.peer_memory, max(0.0, t), []) for t in pool.pending]
    cache: dict[int, Run] = {}

    def sim(k: int) -> Run:
        if k not in cache:
            new: list[Peer] = [(pool.peer_cpus, pool.peer_memory, startup_s, []) for _ in range(k)]
            cache[k] = _simulate(base + new, calls, unit, target)
        return cache[k]

    def late(k: int) -> float:
        return sim(k).late

    def least_useful(k: int) -> float:
        """Seconds of work the least busy new peer runs, at k new peers."""
        return min(sim(k).useful[len(base) :]) / pool.peer_cpus if k else math.inf

    k = _fewest(late, kmax)
    missed = binding = None
    worth = ""
    if late(k) > EPS:
        # The target cannot be met. Ask for no peer that would mostly start.
        while k > 0 and least_useful(k) < startup_s:
            worth = f"a peer more would run only {least_useful(k):.0f} s of work, less than its {startup_s:.0f} s startup"
            k -= 1
        missed = _missed(target, sim(k))
        max_wall = max((c[3] for c in calls[:n_queued] if math.isfinite(c[3])), default=0.0)
        if target.finish_s is not None and max_wall > target.finish_s:
            binding = f"calls that run {max_wall:.0f} s"
        elif kmax and late(kmax) >= late(0) - EPS:
            binding = "no number of new peers starts the queued calls sooner"
        elif worth:
            binding = worth
        elif k == kmax and (kmax == 0 or late(kmax) < late(kmax - 1) - EPS):
            binding = f"pool max {pool.max} peers"
        else:
            binding = f"provider startup {startup_s:.0f} s"
    wait, finish = sim(k).wait, sim(k).finish
    want = pool.replicas + k
    runner_up: tuple[int, str] | None = None
    if k:
        action = "grow"
        reason = f"grow from {pool.replicas} to {want} peers: with {pool.replicas} {_metric(target, sim(0))}"
        reason += f"; with {want} {_metric(target, sim(k))}"
        runner_up = (want - 1, f"with {want - 1} peers {_metric(target, sim(k - 1))}")
    else:
        action = "hold"
        reason = f"hold at {want} peers: {n_queued} queued calls; {_metric(target, sim(0))}"
        if kmax:
            runner_up = (want + 1, worth or f"with {want + 1} peers {_metric(target, sim(1))}")
    reason += f" ({_goal(target)})"
    if missed:
        reason += f"; {missed}, bound by {binding}"
    if want < pool.min:  # below its floor (scaled down by hand): back to it first
        want, action = pool.min, "grow"
        reason = f"grow to the pool minimum of {pool.min} peers; {reason}"
    slack = target.start_s - wait if target.start_s is not None and math.isfinite(wait) else 0.0
    return Plan(want, action, reason, wait, finish, missed, binding, runner_up, min(60.0, max(5.0, slack)), inputs)


def _fewest(late: Callable[[int], float], kmax: int) -> int:
    """The fewest new peers (up to `kmax`) that meet the target, else the
    fewest that come as close as any. More peers never make calls later, so
    beyond a few candidates a binary search finds them."""
    if kmax <= LINEAR_UP_TO:
        for k in range(kmax + 1):
            if late(k) <= EPS:
                return k
        best = min(late(k) for k in range(kmax + 1))
        return next(k for k in range(kmax + 1) if late(k) <= best + EPS)
    goal = max(0.0, late(kmax))
    lo, hi = 0, kmax
    while lo < hi:
        mid = (lo + hi) // 2
        if late(mid) <= goal + EPS:
            hi = mid
        else:
            lo = mid + 1
    return lo


def _goal(target: Target) -> str:
    parts = []
    if target.start_s is not None:
        parts.append(f"start within {target.start_s:g} s")
    if target.finish_s is not None:
        parts.append(f"finish within {target.finish_s:g} s")
    return "target: " + (", ".join(parts) or "none")


def _metric(target: Target, r: Run) -> str:
    wait, finish = r.wait, r.finish
    if not math.isfinite(wait):
        return "some calls never start"
    out = f"the last call starts at {wait:.0f} s"
    if target.finish_s is not None:
        out += f" and the last finishes at {finish:.0f} s"
    return out


def _missed(target: Target, r: Run) -> str:
    wait, finish = r.wait, r.finish
    parts = []
    if target.start_s is not None and wait > target.start_s + EPS:
        parts.append(
            f"start target {target.start_s:g} s cannot be met: "
            + (f"calls wait up to {wait:.0f} s" if math.isfinite(wait) else "some calls never start")
        )
    if target.finish_s is not None and finish > target.finish_s + EPS:
        parts.append(f"finish target {target.finish_s:g} s cannot be met: earliest finish {finish:.0f} s")
    return "; ".join(parts)


class Forecast:
    """What the planner carries from one look to the next, for a provisioner
    (provisioner.py): recent snapshots, for the arrival rate; when each peer
    asked for and not yet usable was asked for; and how long peers took to
    become usable, for the startup estimate. It does no IO: `samples` is what
    a provisioner keeps in its store to start from after a restart."""

    def __init__(
        self,
        target: Target,
        startup_s: float = 60.0,
        samples: Sequence[float] = (),
        window_s: float = 300.0,
        shape: tuple[float, int] | None = None,
    ):
        """`shape`: (cpus, memory bytes) of a peer the pool adds, when known;
        else learned from the first peer it adds that joins."""
        self.target = target
        self.given = shape
        self.startup = Startup(tuple(samples)[-KEEP_SAMPLES:], startup_s)
        self.window_s = window_s
        self.history: deque[tuple[float, Sequence[Any]]] = deque()
        self.asked: list[float] = []  # when each peer not usable yet was asked for, oldest first
        self.usable: int | None = None
        self.seen: set[str] | None = None  # usable peers at the last look
        self.shape: tuple[float, int] | None = None  # (cpus, memory) of a peer this pool added

    @property
    def samples(self) -> tuple[float, ...]:
        return self.startup.samples

    def grew(self, peers: int, now: float) -> None:
        """The provisioner asked for `peers` more at `now`."""
        self.asked += [now] * peers

    def look(self, nodes: Sequence[Any], replicas: int, pool_min: int, pool_max: int, now: float) -> Plan:
        """The plan for this look. `replicas` counts every peer asked for."""
        usable = [n for n in nodes if _usable(n) and not getattr(n, "starting", False)]
        if self.usable is not None and len(usable) > self.usable:
            # Peers that became usable end the oldest waits.
            came = min(len(usable) - self.usable, len(self.asked))
            for _ in range(came):
                took = now - self.asked.pop(0)
                self.startup = Startup((self.startup.samples + (took,))[-KEEP_SAMPLES:], self.startup.default_s)
            new = [n for n in usable if self.seen is not None and n.peer not in self.seen]
            if came and new:
                self.shape = (float(new[-1].cpus), int(new[-1].memory_bytes))
        self.usable, self.seen = len(usable), {n.peer for n in usable}
        # As many waits as peers missing: a shrink drops the newest; peers
        # asked for before this provisioner looked count from now.
        missing = max(0, replicas - len(usable))
        del self.asked[missing:]
        self.asked += [now] * (missing - len(self.asked))
        # A new peer is as large as one this pool added; until one has joined,
        # as the smallest peer there is: assuming more would buy peers for
        # calls that only a larger peer (one it did not add) can run.
        shape = self.given or self.shape
        if shape is None and usable:
            small = min(usable, key=lambda n: (float(n.cpus), int(n.memory_bytes)))
            shape = (float(small.cpus), int(small.memory_bytes))
        self.history.append((now, nodes))
        while self.history and self.history[0][0] < now - self.window_s:
            self.history.popleft()
        if shape is None:
            return Plan(replicas, "hold", "hold: no usable peer yet to size new ones by", 0.0, 0.0, None, None, None,
                        30.0, {})
        pending = tuple(self.startup.remaining(now - t) for t in self.asked)
        pool = Pool(shape[0], shape[1], replicas, pool_min, pool_max, pending)
        return plan(nodes, pool, self.target, self.startup.seconds(), arrival_rate(list(self.history)))
