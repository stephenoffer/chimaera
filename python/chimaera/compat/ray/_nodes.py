"""``ray.nodes()``, ``ray.cluster_resources()`` and ``ray.available_resources()``
from ``ch.cluster()``: what the connected peer knows about the cluster."""

from __future__ import annotations

from typing import Any

import chimaera as ch


def nodes() -> list[dict[str, Any]]:
    """One Ray-shaped record per peer. ``NodeID`` is the peer id, the value of
    its ``node`` label, so ``NodeAffinitySchedulingStrategy(node_id)`` works."""
    out = []
    for n in ch.cluster().nodes():
        host, _, port = n.address.rpartition(":")
        resources: dict[str, float] = {"CPU": n.cpus, "memory": float(n.memory_bytes), f"node:{host}": 1.0}
        if n.gpus:
            resources["GPU"] = float(len(n.gpus))
        out.append(
            {
                "NodeID": n.peer,
                "Alive": n.alive,
                "NodeManagerAddress": host,
                "NodeManagerHostname": n.labels.get("host", host),
                "NodeManagerPort": int(port) if port.isdigit() else 0,
                "Resources": resources,
                "Labels": {"ray.io/node_id": n.peer, **n.labels},
            }
        )
    return out


def cluster_resources() -> dict[str, float]:
    """Total capacity of the live peers."""
    totals: dict[str, float] = {}
    for record in nodes():
        if record["Alive"]:
            for name, amount in record["Resources"].items():
                totals[name] = totals.get(name, 0.0) + amount
    return totals


def available_resources() -> dict[str, float]:
    """CPUs not in use on the live peers, by their last load reports; memory and
    GPUs as capacity (Chimaera does not reserve them ahead of a call yet)."""
    free = cluster_resources()
    free["CPU"] = max(0.0, free.get("CPU", 0.0) - sum(n.cpu_used for n in ch.cluster().nodes() if n.alive))
    return free


def this_node_id() -> str:
    """The peer this process works with: the call's peer, or the driver's."""
    ctx = ch.context()
    if ctx is not None:
        return ctx.node
    return next(n.peer for n in ch.cluster().nodes() if n.this_peer)
