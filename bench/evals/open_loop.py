"""Open-loop arrivals: Chimaera at fixed offered rates, and what queueing does past saturation.

A closed loop (submit, wait, submit again) shows only the rate the runtime
sustains. Past saturation its throughput plateaus and its latency looks
bounded, because the driver slows down with the runtime. Here a clock sets the
arrivals (Poisson, or bursts) whatever the runtime does, as on a shared cluster.
So past saturation the backlog and the wait from arrival to start keep growing,
and that is what this measures.

For each offered rate it reports:
- calls offered, accepted (``.remote()`` returned), finished by the end of the
  window, and finished after a drain;
- the backlog at the window's end, and how late the driver fell behind its own
  schedule (a submit that blocks shows up there);
- percentiles of arrival-to-start and arrival-to-done;
- where a sample of calls spent the wait, from each attempt's stage times in
  ``explain()``: ready to queued (fetching inputs), queued to dispatched
  (waiting for a slot and admission), dispatched to entry (handing it to an
  executor).

Every number is labelled with the arrival process and offered rate it was
measured at, so a plateau cannot be read as a throughput figure.

    .venv/bin/python bench/evals/open_loop.py --rates 20,80,320 --seconds 10 --workload mixed
    .venv/bin/python bench/evals/open_loop.py --arrival burst --rates 200 --workload join64 --cluster chimaera://h:7710

The driver and the peers it reaches must share a clock (one machine, or hosts
synced well below the delays measured): arrival times are this process's, and
start and done times are the executor's.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
from typing import Any

import chimaera as ch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

WORKLOADS = {
    "tiny": "a no-op call",
    "mixed": "calls of 1 ms (80%), 250 ms (18%) and 5 s (2%), each holding one CPU",
    "join64": "per arrival, 64 no-op calls and one call that joins all 64",
    "memtail": "no-op calls, and 5% that allocate 256 MiB for 0.5 s",
}


def schedule(arrival: str, rate: float, seconds: float, rng: random.Random, burst: int = 50) -> list[float]:
    """Arrival offsets (s from the window's start) for `rate` calls per second
    over `seconds`: ``poisson`` (exponential gaps), or ``burst`` (`burst` calls
    at once, the bursts evenly spaced so the mean rate is `rate`)."""
    if rate <= 0 or seconds <= 0:
        return []
    if arrival == "poisson":
        out, t = [], rng.expovariate(rate)
        while t < seconds:
            out.append(t)
            t += rng.expovariate(rate)
        return out
    if arrival == "burst":
        gap = burst / rate
        return [k * gap for k in range(math.ceil(seconds / gap)) for _ in range(burst)]
    raise ValueError(f"unknown arrival process {arrival!r}")


def percentile(xs: list[float], q: float) -> float | None:
    """The `q`-quantile (0..1) of `xs` by nearest rank; None for no samples."""
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))]


def stages(attempt: dict[str, Any], offered_ns: int) -> dict[str, float | None]:
    """Seconds each stage of one attempt took, from its explain() times; None where one is missing."""

    def gap(a: str | int, b: str) -> float | None:
        t0 = offered_ns if isinstance(a, int) else attempt.get(a)
        t1 = attempt.get(b)
        return (t1 - t0) / 1e9 if t0 and t1 else None

    return {
        "arrival_to_ready": gap(offered_ns, "ready_unix_ns"),
        "ready_to_queued": gap("ready_unix_ns", "queued_unix_ns"),
        "queued_to_dispatched": gap("queued_unix_ns", "dispatched_unix_ns"),
        "dispatched_to_entry": gap("dispatched_unix_ns", "py_entry_unix_ns"),
        "run": gap("py_entry_unix_ns", "py_exit_unix_ns"),
        "exit_to_published": gap("py_exit_unix_ns", "published_unix_ns"),
    }


def summary(name: str, xs: list[float]) -> dict[str, Any]:
    return {f"{name}_p50": percentile(xs, 0.5), f"{name}_p99": percentile(xs, 0.99), f"{name}_n": len(xs)}


@ch.remote
def noop(i: int = 0) -> int:
    return i


@ch.remote(cpus=1)
def nap(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


@ch.remote
def join(*xs: int) -> int:
    return len(xs)


@ch.remote(memory="320 MiB")
def big() -> int:
    b = bytearray(256 << 20)
    time.sleep(0.5)
    return len(b)


def calls(workload: str):
    """How one arrival of `workload` submits: a function of (rng) returning
    the futures to wait on and the one whose attempt times are read."""

    def one(rng: random.Random):
        if workload == "tiny":
            f = noop.remote()
            return [f], f
        if workload == "mixed":
            x = rng.random()
            f = nap.remote(0.001 if x < 0.80 else 0.25 if x < 0.98 else 5.0)
            return [f], f
        if workload == "join64":
            parts = [noop.remote(i) for i in range(64)]
            f = join.remote(*parts)
            return [*parts, f], f
        if workload == "memtail":
            f = big.remote() if rng.random() < 0.05 else noop.remote()
            return [f], f
        raise ValueError(f"unknown workload {workload!r}")

    return one


def run_rate(one, arrival: str, rate: float, seconds: float, drain: float, sample: int, seed: int) -> dict[str, Any]:
    """Offer `rate` arrivals per second for `seconds`, then wait up to `drain` s for the rest."""
    rng = random.Random(seed)
    offsets = schedule(arrival, rate, seconds, rng)
    submitted: list[tuple[int, int, list[Any], Any]] = []  # (offered ns, accepted ns, futures, timed future)
    late: list[float] = []
    start = time.monotonic()
    wall0 = time.time_ns()
    for off in offsets:
        wait = start + off - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        else:
            late.append(-wait)
        offered = wall0 + int(off * 1e9)  # on schedule, whether or not the driver kept up
        futures, timed = one(rng)
        submitted.append((offered, time.time_ns(), futures, timed))
    window_end = start + seconds
    if time.monotonic() < window_end:
        time.sleep(window_end - time.monotonic())
    done_in_window = sum(all(f.done() for f in fs) for _, _, fs, _ in submitted)
    deadline = time.monotonic() + drain
    pending = [fs for _, _, fs, _ in submitted]
    while pending and time.monotonic() < deadline:
        pending = [fs for fs in pending if not all(f.done() for f in fs)]
        time.sleep(0.05)
    failed = sum(1 for _, _, fs, _ in submitted for f in fs if f.done() and f.exception() is not None)
    to_start: list[float] = []
    to_done: list[float] = []
    split: dict[str, list[float]] = {}
    picks = random.Random(seed + 1).sample(range(len(submitted)), min(sample, len(submitted)))
    for i in picks:
        offered, _, fs, timed = submitted[i]
        if not timed.done():
            continue
        attempts = timed.explain().get("attempts") or []
        if not attempts:
            continue
        a = attempts[-1]
        if a.get("py_entry_unix_ns"):
            to_start.append((a["py_entry_unix_ns"] - offered) / 1e9)
        end = a.get("published_unix_ns") or a.get("py_exit_unix_ns")
        if end:
            to_done.append((end - offered) / 1e9)
        for k, v in stages(a, offered).items():
            if v is not None:
                split.setdefault(k, []).append(v)
    accept = [(acc - off) / 1e9 for off, acc, _, _ in submitted]
    return {
        "arrival": arrival,
        "offered_rate": rate,
        "seconds": seconds,
        "offered": len(offsets),
        "accepted": len(submitted),
        "finished_in_window": done_in_window,
        "backlog_at_window_end": len(submitted) - done_in_window,
        "finished_after_drain": len(submitted) - len(pending),
        "unfinished_after_drain": len(pending),
        "failed": failed,
        "driver_late_p99_s": percentile(late, 0.99) or 0.0,
        **summary("accept_s", accept),
        **summary("arrival_to_start_s", to_start),
        **summary("arrival_to_done_s", to_done),
        "stages_p50_s": {k: percentile(v, 0.5) for k, v in split.items()},
        "stages_p99_s": {k: percentile(v, 0.99) for k, v in split.items()},
        "raw": {"arrival_to_start_s": to_start, "arrival_to_done_s": to_done},
    }


def table(results: list[dict[str, Any]]) -> str:
    def f(x: Any) -> str:
        return "–" if x is None else f"{x:.3g}" if isinstance(x, float) else str(x)

    rows = [
        "| arrivals | offered/s | offered | finished in window | backlog at end | unfinished after drain "
        "| arrival→start p50 / p99 (s) | arrival→done p99 (s) | queued→dispatched p99 (s) "
        "| dispatched→start p99 (s) | driver late p99 (s) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        rows.append(
            f"| {r['arrival']} | {f(r['offered_rate'])} | {r['offered']} | {r['finished_in_window']} "
            f"| {r['backlog_at_window_end']} | {r['unfinished_after_drain']} "
            f"| {f(r['arrival_to_start_s_p50'])} / {f(r['arrival_to_start_s_p99'])} | {f(r['arrival_to_done_s_p99'])} "
            f"| {f(r['stages_p99_s'].get('queued_to_dispatched'))} | {f(r['stages_p99_s'].get('dispatched_to_entry'))} "
            f"| {f(r['driver_late_p99_s'])} |"
        )
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--rates", default="20,80,320", help="offered arrivals per second, comma-separated")
    p.add_argument("--seconds", type=float, default=10.0, help="window per rate")
    p.add_argument("--drain", type=float, default=60.0, help="how long to wait for the rest after each window")
    p.add_argument("--arrival", choices=["poisson", "burst"], default="poisson")
    p.add_argument("--workload", choices=sorted(WORKLOADS), default="mixed")
    p.add_argument("--sample", type=int, default=300, help="calls per rate whose stage times are read")
    p.add_argument("--seed", type=int, help="default: a random one, recorded")
    p.add_argument("--cluster", help="a profile or chimaera://host:port (default: a local peer)")
    p.add_argument("--out", help="write the JSON here")
    a = p.parse_args(argv)
    import manifest

    seed = a.seed if a.seed is not None else random.randrange(1 << 31)
    ch.init(cluster=a.cluster)
    nodes = ch.runs.cluster().nodes()
    one = calls(a.workload)
    one(random.Random(0))[1].result(timeout=120)  # executors up before the first window
    results = []
    for k, rate in enumerate(float(r) for r in a.rates.split(",")):
        results.append(run_rate(one, a.arrival, rate, a.seconds, a.drain, a.sample, seed + k))
        print(json.dumps({key: v for key, v in results[-1].items() if key != "raw"}), flush=True)
    doc = {
        "manifest": manifest.collect(
            os.environ.get("CHIMAERA_BIN"),
            {"chimaera": sys.executable},
            seed=seed,
            arrival=a.arrival,
            rates=a.rates,
            workload=f"{a.workload}: {WORKLOADS[a.workload]}",
            peers=[{"address": n.address, "cpus": n.cpus, "memory_bytes": n.memory_bytes} for n in nodes],
        ),
        "results": results,
    }
    print(f"\n{a.workload} ({WORKLOADS[a.workload]}), {a.arrival} arrivals, {len(nodes)} peer(s), "
          f"{sum(n.cpus for n in nodes):g} CPUs, seed {seed}\n")
    print(table(results))
    if a.out:
        with open(a.out, "w") as f:
            json.dump(doc, f, indent=1)


if __name__ == "__main__":
    main()
