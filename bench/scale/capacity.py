"""What each part of Chimaera costs, from one laptop to 262,144 peers.

The rule this checks (docs/architecture.md §2): nothing above a cell sees
per-call traffic. Per-peer cost then stays flat as the fleet grows, and
fleet-wide parts grow only with the number of cells. Every input is a named
assumption; override one with --set and rerun.

    python bench/scale/capacity.py
    python bench/scale/capacity.py --set cell_size=1024 --set service_fine_s=0.002

This is arithmetic over stated assumptions, not a measurement. Sources for the
inputs are in docs/architecture.md §14; M6's fleet emulator checks the
membership and directory rows against the real code.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import asdict, dataclass, fields

SIZES = [1, 64, 1_024, 10_240, 102_400, 262_144]
MiB = 1 << 20


@dataclass
class A:
    cell_size: int = 512  # peers per cell
    slots_per_peer: int = 32  # one-core execution slots after the agent's reserve
    occupancy: float = 0.7
    service_coarse_s: float = 0.25  # mean useful time per call, batch work
    service_fine_s: float = 0.005  # mean useful time per call, fine-grained work
    service_tiny_s: float = 0.001  # unbatched 1 ms calls: where the per-peer budgets bind

    # Hot path (§3): each call is journaled by its owner peer to two backups, in batches.
    agent_us_per_call: float = 30.0  # owner + executing agent CPU per call (target, to measure)
    journal_bytes_per_call: int = 256  # submit + outcome records, before compression
    journal_backups: int = 2
    journal_retain_s: float = 60.0  # finished records kept before compaction to the frontier

    # Membership inside a cell (§7): SWIM with Lifeguard, memberlist's LAN defaults.
    swim_period_s: float = 1.0
    swim_msgs_per_period: float = 2.2  # ping + ack, plus occasional indirect probes
    swim_msg_bytes: int = 512  # with piggybacked updates
    swim_suspicion_mult: float = 4.0  # suspicion timeout = mult * log10(n) * period
    member_record_bytes: int = 96
    # Owner loss is detected on the journal stream, not by gossip (§3, D3).
    stream_silence_s: float = 1.0  # an idle stream heartbeats every 200 ms
    confirm_probe_s: float = 0.5  # the cell directory leader's direct probe

    # Cell summaries (§6, §7): one compact summary per cell per period, to every cell's ambassadors.
    summary_bytes: int = 4_096
    summary_period_s: float = 2.0

    # Raft groups (§7) hold metadata only: runs, actors, stores, gangs, the directory.
    metadata_ops_per_peer_s: float = 5.0
    group_ops_budget_s: float = 2_000.0  # sustained per group, 60% of an assumed ceiling
    min_groups_per_cell: int = 8
    voters: int = 3
    raft_tick_s: float = 0.1  # one coalesced heartbeat per peer pair per tick
    active_group_fraction: float = 0.25  # the rest are quiescent

    # Global directory (§7): cell registry, global names, tenant envelopes.
    global_lookups_per_peer_s: float = 0.05  # cache misses only
    global_group_ops_budget_s: float = 2_000.0

    # Telemetry (§9): records for interesting attempts, sketches for the rest.
    attempt_record_bytes: int = 160
    record_sample_fraction: float = 0.02  # errors, retries, slow tails, 1% random
    sketch_bytes_per_fn_minute: int = 2_048
    functions_per_peer: int = 200
    telemetry_window_s: float = 900.0

    # Budgets that define "fits".
    agent_core_budget: float = 0.5  # of one core
    peer_net_budget_Bps: float = 0.01 * 25e9 / 8  # 1% of a 25 Gbit/s NIC
    peer_mem_budget_B: float = 512 * MiB  # agent control state, not the object store
    ambassador_net_budget_Bps: float = 10 * MiB


def per_peer(a: A, n: int, service_s: float) -> dict:
    cell = min(n, a.cell_size)
    calls = a.slots_per_peer * a.occupancy / service_s
    backups = min(a.journal_backups, cell - 1)
    journal_out = calls * a.journal_bytes_per_call * backups
    journal_mem = calls * a.journal_bytes_per_call * (1 + backups) * a.journal_retain_s
    swim = (a.swim_msgs_per_period / a.swim_period_s) * a.swim_msg_bytes if cell > 1 else 0.0
    suspicion = a.swim_suspicion_mult * math.log10(max(cell, 10)) * a.swim_period_s if cell > 1 else 0.0
    owner_loss = a.stream_silence_s + a.confirm_probe_s if cell > 1 else 0.0
    groups = max(min(a.min_groups_per_cell, cell), math.ceil(cell * a.metadata_ops_per_peer_s / a.group_ops_budget_s))
    replicas = groups * min(a.voters, cell) / cell
    partners = min(cell - 1, replicas * a.active_group_fraction * (a.voters - 1))
    raft_msgs = partners / a.raft_tick_s if cell > 1 else 0.0
    records = calls * a.record_sample_fraction * a.attempt_record_bytes * a.telemetry_window_s
    sketches = a.functions_per_peer * a.sketch_bytes_per_fn_minute * a.telemetry_window_s / 60
    return {
        "calls": calls,
        "agent_cores": calls * a.agent_us_per_call / 1e6,
        # Each peer backs up as many records as it sends, so traffic in equals traffic out.
        "net_Bps": 2 * journal_out + swim + raft_msgs * 200,
        "mem_B": journal_mem + cell * a.member_record_bytes + records + sketches,
        "suspicion_s": suspicion,
        "owner_loss_s": owner_loss,
        "groups_per_cell": groups,
    }


def fleet(a: A, n: int, service_s: float) -> dict:
    cells = math.ceil(n / a.cell_size)
    multi = cells > 1
    lookups = n * a.global_lookups_per_peer_s if multi else 0.0
    return {
        "cells": cells,
        "calls": n * a.slots_per_peer * a.occupancy / service_s,
        "ambassador_Bps": cells * a.summary_bytes / a.summary_period_s if multi else 0.0,
        "global_ops": lookups,
        "global_groups": max(1, math.ceil(lookups / a.global_group_ops_budget_s)) if multi else 0,
        "lost_with_a_cell": min(n, a.cell_size) * a.slots_per_peer * a.occupancy,
    }


def over_budget(a: A, p: dict, f: dict) -> list[str]:
    over = []
    if p["agent_cores"] > a.agent_core_budget:
        over.append(f"agent CPU {p['agent_cores']:.2f} cores")
    if p["net_Bps"] > a.peer_net_budget_Bps:
        over.append(f"peer control traffic {p['net_Bps'] / MiB:.1f} MiB/s")
    if p["mem_B"] > a.peer_mem_budget_B:
        over.append(f"peer control memory {p['mem_B'] / MiB:.0f} MiB")
    if f["ambassador_Bps"] > a.ambassador_net_budget_Bps:
        over.append(f"summaries {f['ambassador_Bps'] / MiB:.1f} MiB/s per ambassador")
    return over


def human(x: float) -> str:
    for unit, div in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if abs(x) >= div:
            return f"{x / div:.2f}{unit}"
    return f"{x:.3g}"


def report(a: A) -> str:
    out = []
    for label, svc in (("coarse", a.service_coarse_s), ("fine", a.service_fine_s), ("tiny", a.service_tiny_s)):
        out.append(f"\n### {label}: {svc * 1000:g} ms calls, {a.slots_per_peer} slots per peer, {a.occupancy:.0%} busy\n")
        out.append("| peers | cells | fleet calls/s | calls/s per peer | agent cores | control MiB/s per peer "
                   "| control MiB per peer | owner loss detected s | SWIM suspicion s | metadata groups per cell "
                   "| summaries MiB/s per ambassador | global directory ops/s | over budget |")
        out.append("|" + "---:|" * 12 + "---|")
        for n in SIZES:
            p, f = per_peer(a, n, svc), fleet(a, n, svc)
            out.append(
                f"| {n:,} | {f['cells']:,} | {human(f['calls'])} | {human(p['calls'])} | {p['agent_cores']:.3f} "
                f"| {p['net_Bps'] / MiB:.2f} | {p['mem_B'] / MiB:.1f} | {p['owner_loss_s']:.1f} | {p['suspicion_s']:.1f} | {p['groups_per_cell']} "
                f"| {f['ambassador_Bps'] / MiB:.2f} | {human(f['global_ops'])} | {'; '.join(over_budget(a, p, f)) or 'none'} |"
            )
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", action="append", default=[], metavar="NAME=VALUE")
    args = ap.parse_args()
    a = A()
    kinds = {f.name: f.type for f in fields(A)}
    for kv in args.set:
        name, _, value = kv.partition("=")
        if name not in kinds:
            raise SystemExit(f"unknown assumption {name!r}; known: {', '.join(sorted(kinds))}")
        setattr(a, name, int(value) if kinds[name] == "int" else float(value))
    print("Assumptions: " + ", ".join(f"{k}={v}" for k, v in asdict(a).items()))
    print(report(a))


if __name__ == "__main__":
    main()
