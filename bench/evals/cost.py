"""What each workload of a cluster run cost, recomputed from its raw results:
instance-hours, dollars, allocated core-hours and GPU-hours, and the useful
completions it delivered at one service level for both runtimes.

A workload's instance-hours are the wall time of its runs (each child process,
its start and warm-up included) times the machines the cluster held, by
instance type, as multi_node.py records them in the manifest's topology.
Dollars, core-hours and GPU-hours are those hours times each type's price,
vCPUs and GPUs in a dated price table (prices.json), which the report carries
with it. A timed repetition is a useful completion when it met the workload's
SLO, one bound for both runtimes in the row's own metric, and it completes the
calls or items of one repetition (one job for a timed workload). A run that
failed counts its hours and no completions. The time between runs (starting
and stopping peers) belongs to the round, not to a workload.

    python bench/evals/cost.py multi_results.json [--prices prices.json] [--instances c7i.4xlarge=64]
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
# Each repetition's bound, in its row's metric: at most this many seconds, at
# least this rate or efficiency. About 5x worse than the slower runtime's
# median at 64 nodes (2026-11-22), so a repetition misses it only by stalling.
# Controls take their row's bound; tasks_serial is 5x the single-node round
# trip (3.47 ms).
SLO = {
    "tasks": 1_000, "tasks_serial": 58, "map_small": 1_000, "actors": 1_900, "actors_peer": 1_900,
    "cpu_scaling": 0.12, "broadcast": 2.0, "naive_big_arg": 18.0, "follow_input": 1.9, "nested": 6.5,
    "shuffle": 4.2, "shuffle_unfused": 4.2, "node_loss": 93.0,
}
# Runs that do other work than the other runtime's on the same row.
NOT_EQUIVALENT = {("ray", "map_small"): "each Ray run also times 4M items batched by hand"}


def met(metric: str, sample: float, bound: float) -> bool:
    return sample <= bound if metric == "seconds" else sample >= bound


def costs(doc: dict[str, Any], prices: dict[str, Any], instances: dict[str, int] | None = None) -> dict[str, Any]:
    """Every (workload, runtime, variant)'s costs and useful completions."""
    topology = doc["manifest"].get("topology", {})
    instances = instances or topology.get("instances") or {}
    unpriced = sorted(set(instances) - set(prices["instances"]))
    if not instances or unpriced:
        raise ValueError(f"the run names no instance types to price ({instances}), or types with no price: {unpriced}")
    spec = prices["instances"]
    per_hour = sum(n * spec[t]["per_hour"] for t, n in instances.items())
    vcpus = sum(n * spec[t]["vcpus"] for t, n in instances.items())
    gpus = sum(n * spec[t]["gpus"] for t, n in instances.items())
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in doc["results"]:
        variant = ",".join(f"{k}={v}" for k, v in sorted((r.get("variant") or {}).items()))
        g = groups.setdefault((r["workload"], r["runtime"], variant),
                              {"wall_s": 0.0, "runs": 0, "failed": 0, "done": 0.0, "metric": ""})
        g["wall_s"] += r["wall_s"]
        g["runs"] += 1
        res = r.get("result")
        if not res:
            g["failed"] += 1
            continue
        g["metric"] = res["metric"]
        units = res["ops"] / res["executions"] if "ops" in res else 1
        g["done"] += units * sum(met(res["metric"], x, SLO[r["workload"]]) for x in res["samples"])
    rows = []
    for (w, rt, variant), g in sorted(groups.items()):
        hours = g["wall_s"] / 3600
        row = {
            "workload": w, "runtime": rt, "variant": variant, "runs": g["runs"], "failed": g["failed"],
            "wall_s": round(g["wall_s"], 1), "instance_hours": hours * sum(instances.values()),
            "dollars": hours * per_hour, "core_hours": hours * vcpus, "gpu_hours": hours * gpus,
            "metric": g["metric"], "slo": SLO[w], "useful_completions": g["done"],
        }
        row["dollars_per_completion"] = row["dollars"] / g["done"] if g["done"] else None
        if (rt, w) in NOT_EQUIVALENT:
            row["not_equivalent"] = NOT_EQUIVALENT[(rt, w)]
        rows.append(row)
    return {"prices": prices, "instances": instances, "region": topology.get("region", ""), "slo": SLO, "rows": rows}


def report(c: dict[str, Any]) -> str:
    """The costs as markdown."""
    p = c["prices"]
    machines = ", ".join(f"{n} x {t}" for t, n in sorted(c["instances"].items())) + (
        f" in {c['region']}" if c["region"] else "")
    lines = [
        f"Machines: {machines}. Prices: {p['source']}, {p['date']}.",
        "",
        "| Workload | Runtime | Instance-hours | Dollars | Core-hours | GPU-hours | SLO per repetition "
        "| Useful completions | Dollars per completion |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in c["rows"]:
        name = r["workload"] + (f" ({r['variant']})" if r["variant"] else "")
        per = f"{r['dollars_per_completion']:.3g}" if r["dollars_per_completion"] else "none"
        if "not_equivalent" in r:
            per = f"not compared: {r['not_equivalent']}"
        bound = ("≤ " if r["metric"] == "seconds" else "≥ ") + f"{r['slo']:,} {r['metric']}"
        lines.append(
            f"| {name} | {r['runtime']} | {r['instance_hours']:.3f} | {r['dollars']:.2f} | {r['core_hours']:.2f} "
            f"| {r['gpu_hours']:.2f} | {bound} | {r['useful_completions']:,.0f} | {per} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("results", help="a cluster run's multi_results.json")
    ap.add_argument("--prices", default=os.path.join(HERE, "prices.json"))
    ap.add_argument("--instances", default="", help="type=count,... for a run that did not record its machines")
    ap.add_argument("--json", help="also write the costs, with the price table, here")
    a = ap.parse_args()
    with open(a.results) as f, open(a.prices) as g:
        doc, prices = json.load(f), json.load(g)
    given = {t: int(n) for t, n in (kv.split("=") for kv in a.instances.split(",") if kv)}
    c = costs(doc, prices, given or None)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(c, f, indent=2)
    print(report(c), end="")


if __name__ == "__main__":
    main()
