"""Node loss, repeated, on localhost: a cluster of four real peer processes
loses one peer at a chosen point of a workload's life, over seeds x victim
roles x workloads x kill points. Each experiment checks that every result
reached the driver exactly once, and times the recovery.

Victim roles: ``leader`` is the peer (other than the driver's) that leads the
most shard groups, ``follower`` the one that leads the fewest; ties go by the
seed, and each record keeps how many groups the victim led. Kill points:
``submitted`` (the driver has made every call), ``running`` (a call has
started on the victim) and ``midway`` (half the calls have started). Workloads:
``tasks`` (half-second calls), ``actors`` (durable actors, whose calls cut off
by a crash run again without being applied twice) and ``shuffle``
(``reduce_by_key`` over a map).

Every call attempt leaves an empty file naming the call (or actor), the peer
it ran on and when it started. Times, in seconds after the SIGKILL:

    detected     the driver's peer first saw the victim as suspect or dead
    retired      ... as dead
    redispatch   for each call (or actor) that had started on the victim and
                 ran again: when it started again (one value per call; calls
                 granted to the victim that had not started leave no trace)
    first_ok     the first result of such a call reaching the driver
    recovered    the last result reaching the driver

Every peer shares one machine and its loopback network, so detection is the
protocol's own timing (probe intervals, suspicion), not a real network's: the
report labels these numbers local.

    .venv/bin/python bench/evals/node_loss.py --seeds 3 --out bench/evals/results
"""

from __future__ import annotations

import argparse
import collections
import json
import operator
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import manifest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
ROLES = ("leader", "follower")
WORKLOADS = ("tasks", "actors", "shuffle")
POINTS = ("submitted", "running", "midway")
PEERS = 4
TASKS, TASK_S = 48, 0.5
ACTORS, ACTOR_CALLS, ACTOR_S = 8, 10, 0.1
MAPS, MAP_S, PARTS = 16, 0.3, 8


# ---- the driver (its own process, connected to peer 0) ---------------------------


def attempt(marks: str, owner: str) -> None:
    """Note that an attempt of `owner` starts: on which peer, and when."""
    import chimaera as ch

    open(os.path.join(marks, f"{owner}.{ch.context().node}.{time.time_ns()}"), "x").close()


def produce_pairs(i: int) -> list[tuple[int, int]]:
    return [((i * 7919 + j) % 1009, 1) for j in range(2000)]


class Ledger:
    """A durable actor's state: what it has been given."""

    def __init__(self, marks: str, name: str) -> None:
        self.marks, self.name, self.total = marks, name, 0

    def add(self, n: int) -> int:
        attempt(self.marks, self.name)
        time.sleep(ACTOR_S)
        self.total += n
        return self.total


def drive(workload: str, marks: str) -> None:
    """Run `workload`, printing a JSON line when every call is made, one per
    result as it arrives, then whether each arrived exactly once and the
    member events the driver's peer saw."""
    import chimaera as ch

    def say(**kw: object) -> None:
        print(json.dumps(kw), flush=True)

    if workload == "tasks":

        def work(i: int) -> int:
            attempt(marks, f"t{i}")
            time.sleep(TASK_S)
            return i

        futs = {f: f"t{i}" for i, f in enumerate(ch.remote(work).remote(i) for i in range(TASKS))}
    elif workload == "actors":
        durable = ch.remote(state=ch.durable.State())(Ledger)
        ledgers = [durable.remote(marks, f"a{k}") for k in range(ACTORS)]
        futs = {ledger.add.remote(1): f"a{k}" for _ in range(ACTOR_CALLS) for k, ledger in enumerate(ledgers)}
    else:

        def produce(i: int) -> list[tuple[int, int]]:
            attempt(marks, f"m{i}")
            time.sleep(MAP_S)
            return produce_pairs(i)

        parts = ch.remote(produce).map(range(MAPS)).reduce_by_key(operator.add, partitions=PARTS)
        futs = {f: "*" for f in parts.futures}  # each partition holds every map's pairs
    calls = {"tasks": TASKS, "actors": ACTORS * ACTOR_CALLS, "shuffle": MAPS}[workload]  # calls that leave marks
    say(submitted=time.time(), total=len(futs), calls=calls)
    values: dict[str, list] = collections.defaultdict(list)
    for f in ch.as_completed(list(futs), timeout=240):
        say(result=futs[f], at=time.time())
        values[futs[f]].append(f.result())
    if workload == "tasks":
        ok = sorted(v for vs in values.values() for v in vs) == list(range(TASKS))
    elif workload == "actors":  # each add applied once: totals 1..calls, on every actor
        ok = all(sorted(values[f"a{k}"]) == list(range(1, ACTOR_CALLS + 1)) for k in range(ACTORS))
    else:
        serial: dict[int, int] = collections.Counter()
        for i in range(MAPS):
            for k, v in produce_pairs(i):
                serial[k] += v
        merged = [kv for d in values["*"] for kv in d.items()]
        ok = len(merged) == len(serial) and dict(merged) == dict(serial)
    deadline = time.monotonic() + 15  # the victim's retirement may come after the last result
    while True:
        events = list(ch.runs.cluster().events(["member"], start="earliest", follow=False))
        if any(e.message.startswith("dead") for e in events) or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    say(exactly_once=ok, events=[{"node": e.node, "at": e.at, "message": e.message} for e in events])


# ---- one experiment ----------------------------------------------------------------


def experiment(seed: int, role: str, workload: str, point: str) -> dict:
    """Start a cluster, run `workload` from peer 0's driver, SIGKILL the peer
    in `role` at `point`, and time what follows."""
    sys.path.insert(0, os.path.join(REPO, "python", "tests"))
    from test_cluster import BIN, Cluster  # the tested multi-peer harness

    rng = random.Random(f"{seed}/{role}/{workload}/{point}")
    c = Cluster(PEERS)
    marks = tempfile.mkdtemp(prefix="chx", dir="/tmp")
    d = None
    try:
        statuses, leads = leadership(c, BIN, others_lead=role == "leader")
        others = [i for i in leads if i != 0]
        most = (max if role == "leader" else min)(leads[i] for i in others)
        victim = rng.choice([i for i in others if leads[i] == most])
        node = statuses[victim]["peer"]
        durability = manifest.settled_durability(lambda: [manifest.status(BIN, c.dir(i)) for i in range(PEERS)])
        env = dict(os.environ, CHIMAERA_HOME=c.dir(0), CHIMAERA_BIN=BIN, PYTHONPATH=os.path.join(REPO, "python"))
        with tempfile.TemporaryFile("w+") as err:
            d = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "--drive", workload, "--marks", marks],
                stdout=subprocess.PIPE, stderr=err, stdin=subprocess.DEVNULL, text=True, env=env, cwd=c.root,
            )
            lines: list[dict] = []

            def read() -> None:
                for x in d.stdout:
                    if x.startswith("{"):
                        lines.append(json.loads(x))

            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            killed, state = kill_at(c, victim, point, d, lines, marks, lambda: started_on(marks, node))
            d.wait(timeout=280)
            reader.join()
            err.seek(0)
            assert d.returncode == 0, err.read()[-3000:]
        attempts = collections.defaultdict(list)
        for name in os.listdir(marks):
            owner, peer, ns = name.split(".")
            attempts[owner].append((peer, int(ns) / 1e9))
    finally:
        if d and d.poll() is None:  # a failed or stopped experiment: its driver would wait out a dead cluster
            d.kill()
            d.wait()
        c.close()
        shutil.rmtree(marks, ignore_errors=True)
    shutil.rmtree(c.root, ignore_errors=True)  # the peers' logs: kept only when the experiment fails
    return {
        "seed": seed,
        "role": role,
        "workload": workload,
        "point": point,
        "victim": f"p{victim}",
        "victim_leads": leads[victim],
        **state,
        **timings(lines, attempts, node, killed),
        "durability": durability,
    }


def started_on(marks: str, node: str) -> int:
    """Attempts that have started on peer `node` so far."""
    return sum(1 for name in os.listdir(marks) if name.split(".")[1] == node)


def kill_at(
    c, victim: int, point: str, d: subprocess.Popen, lines: list[dict], marks: str, on_victim
) -> tuple[float, dict]:
    """Wait for `point` in the driver's output (a minute at most), then
    SIGKILL the victim; when, and how far the work had got."""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and d.poll() is None:
        made = next((x for x in lines if "submitted" in x), None)
        if made and (
            point == "submitted"
            or point == "running" and on_victim()
            or point == "midway" and len(os.listdir(marks)) * 2 >= made["calls"]
        ):
            break
        time.sleep(0.01)
    state = {"results_before_kill": sum(1 for x in list(lines) if "result" in x), "started_on_victim": on_victim()}
    killed = time.time()
    c.signal(victim, signal.SIGKILL)
    return killed, state


def timings(lines: list[dict], attempts: dict[str, list], node: str, killed: float) -> dict:
    """Seconds from the kill to detection, each lost call's restart, the first
    of their results and the last result; whether each arrived exactly once."""
    # Calls (or actors) that started on the victim before the kill and again elsewhere after it.
    again = {
        o: min(t for p, t in xs if p != node and t >= killed) - killed
        for o, xs in attempts.items()
        if any(p == node and t < killed for p, t in xs) and any(p != node and t >= killed for p, t in xs)
    }
    restarted = min(again.values(), default=float("inf"))  # a partition ("*") holds every call's pairs
    results = [x for x in lines if "result" in x]
    last = results[-1]["at"]
    end = next(x for x in lines if "exactly_once" in x)
    seen = [e for e in end["events"] if e["node"] == node and e["at"] >= killed]

    def first(xs: list[float]) -> float | None:
        return round(min(xs), 3) if xs else None

    return {
        "results": len(results),
        "exactly_once": end["exactly_once"],
        "detected_s": first([e["at"] - killed for e in seen if e["message"].startswith(("suspect", "dead"))]),
        "retired_s": first([e["at"] - killed for e in seen if e["message"].startswith("dead")]),
        "redispatch_s": sorted(round(t, 3) for t in again.values()),
        # A result of a call (or actor) that ran again, arriving after it started again.
        "first_ok_s": first(
            [
                x["at"] - killed
                for x in results
                if x["at"] - killed >= (restarted if x["result"] == "*" else again.get(x["result"], float("inf")))
            ]
        ),
        "recovered_s": first([last - killed] if last >= killed else []),  # none when all were in before the kill
    }


def leadership(c, binary: str, others_lead: bool, timeout: float = 60) -> tuple[list[dict], dict[int, int]]:
    """Every peer's status, and how many groups each leads (as it says
    itself), once every group has a leader all peers know of; with
    `others_lead`, also once a peer other than the driver's leads one (for 30
    s at most: the record keeps what the victim led)."""
    start = time.monotonic()
    while True:
        statuses = [manifest.status(binary, c.dir(i)) for i in range(PEERS)]
        if all(s and not s["leaderless_groups"] and len(s["nodes"]) == PEERS for s in statuses):
            leads = {i: len(next(n["leading"] for n in s["nodes"] if n["this_peer"])) for i, s in enumerate(statuses)}
            if not others_lead or any(leads[i] for i in leads if i != 0) or time.monotonic() - start > 30:
                return statuses, leads
        assert time.monotonic() - start < timeout, f"groups without a leader: {statuses[0].get('leaderless_groups')}"
        time.sleep(0.2)


# ---- the matrix and its report -------------------------------------------------------


def spread(xs: list[float]) -> str:
    """p50 [min, max] of what was measured, and how many."""
    xs = [x for x in xs if x is not None]
    if not xs:
        return "none"
    return f"{statistics.median(xs):.2f} [{min(xs):.2f}, {max(xs):.2f}] n={len(xs)}"


def tail(xs: list[float]) -> str:
    """p50 [p90, max] of what was measured, each interpolated between ranks as the median is."""
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return "none"
    p90 = statistics.quantiles(xs, n=10, method="inclusive")[-1] if len(xs) > 1 else xs[0]
    return f"{statistics.median(xs):.2f} [{p90:.2f}, {xs[-1]:.2f}]"


def report(doc: dict) -> str:
    m = doc["manifest"]
    lines = [
        "# Node loss, repeated (local)",
        "",
        f"{PEERS} peers on one machine and its loopback network ({m['host']['platform']}): detection is the "
        "protocol's own timing, not a real network's. Seconds after the SIGKILL: p50 [min, max] n=values; "
        "redispatch has one value per call that ran again.",
        f"Peer {m['versions']['chimaera']}, {m['build']['profile']} build; source digest {m['source']['digest']}"
        + (f", commit {m['source']['commit'][:12]}" if "commit" in m["source"] else "")
        + f"; durability {m['durability'].get('effective', 'unknown')}; seeds {m['order']['seeds']}.",
        "",
        "| Workload | Victim | Groups it led | Kill point | Runs | Detected | Retired | Redispatch | First success "
        "| Recovered | Exactly once |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    groups = collections.defaultdict(list)
    for e in doc["experiments"]:
        groups[(e["workload"], e["role"], e["point"])].append(e)
    for (w, role, point), all_runs in groups.items():
        es = [e for e in all_runs if "error" not in e]
        led = sorted({e["victim_leads"] for e in es})
        failed = len(all_runs) - len(es)
        lines.append(
            f"| {w} | {role} | {f'{led[0]}-{led[-1]}' if led else ''} | {point} | {len(all_runs)} "
            f"| {spread([e['detected_s'] for e in es])} | {spread([e['retired_s'] for e in es])} "
            f"| {spread([t for e in es for t in e['redispatch_s']])} | {spread([e['first_ok_s'] for e in es])} "
            f"| {spread([e['recovered_s'] for e in es])} | {sum(e['exactly_once'] for e in es)}/{len(all_runs)}"
            + (f", {failed} failed" if failed else "")
            + " |"
        )
    lines += [
        "",
        "Pooled over workloads and kill points, p50 [p90, max]:",
        "",
        "| Victim | Detected | Retired | Lost calls started again | First of their results | Last result |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for role in ROLES:
        es = [e for e in doc["experiments"] if e["role"] == role and "error" not in e]
        if es:
            again = [t for e in es for t in e["redispatch_s"]]
            lines.append(
                f"| `{role}` | {tail([e['detected_s'] for e in es])} | {tail([e['retired_s'] for e in es])} "
                f"| {tail(again)}, {len(again)} calls | {tail([e['first_ok_s'] for e in es])} "
                f"| {tail([e['recovered_s'] for e in es])} |"
            )
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--roles", default=",".join(ROLES))
    ap.add_argument("--workloads", default=",".join(WORKLOADS))
    ap.add_argument("--points", default=",".join(POINTS))
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    ap.add_argument("--drive", help=argparse.SUPPRESS)  # the driver process: run this workload
    ap.add_argument("--marks", help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.drive:
        drive(a.drive, a.marks)
        return
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))  # stopped from outside: the cluster still closes
    os.makedirs(a.out, exist_ok=True)
    experiments = []
    for seed in range(a.seeds):
        for role in a.roles.split(","):
            for workload in a.workloads.split(","):
                for point in a.points.split(","):
                    try:
                        e = experiment(seed, role, workload, point)
                    except Exception as exc:  # noqa: BLE001 -- a failed experiment is a finding: keep it, go on
                        e = dict(seed=seed, role=role, workload=workload, point=point, error=str(exc)[-3000:])
                    print(json.dumps(e), flush=True)
                    experiments.append(e)
    from test_cluster import BIN

    durability = next((e["durability"] for e in experiments if "durability" in e), {})
    meta = manifest.collect(BIN, {"chimaera": sys.executable}, durability=durability,
                            topology={"nodes": PEERS, "layout": "one machine, loopback network"},
                            order={"seeds": a.seeds})
    doc = {"manifest": meta, "experiments": experiments}
    stamp = time.strftime("%Y-%m-%d")
    with open(os.path.join(a.out, f"node-loss-{stamp}.json"), "w") as f:
        json.dump(doc, f, indent=2)
    text = report(doc)
    with open(os.path.join(a.out, f"node-loss-{stamp}.md"), "w") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    main()
