"""Where a reduce_by_key's time and bytes go, phase by phase, on local peers.

Starts ``--nodes`` peers on this machine (the tested ``test_cluster``
harness), then runs the 3.2M-pair shuffle of ``multi_node.py`` (64 maps of
50k ``(key, 1)`` pairs into 64 partitions, the map run inside the scatters)
for uniform keys (the benchmark's) and Zipf-skewed ones, ``--rounds`` times
each after a warm-up. Each run is recorded by ``chimaera._shuffle_phases``:
planning, the map, the split, encoding the buckets, fetches from other
peers, reading the buckets, the fold, encoding the partitions, and bringing
the partitions to the driver, each with its bytes, and the timeline of the
stage boundaries (submit, scatter start and end, the manifest, gather start,
fetch and fold, the driver's collect). Each peer's ``/metrics`` is read
before and after every run for what the peers did themselves: the bytes they
sent each other for the shuffle (``network``) and what their stores moved
out of memory, with the time it took (``spill``). Every run's output is
checked, a partition at a time, against the digest of the same pairs counted
serially. The JSON keeps every run; the report has the median of each phase.

    .venv/bin/python bench/evals/shuffle_phases.py --rounds 3

Larger than memory: run it in a memory-limited Linux container (the dev
image) with ``--maps`` and ``--per`` raised until the buckets the scatters
store are several times the limit, as in docs/qualification.md. A gather
holds its partition and one bucket, and the driver one partition, so it fits
while each partition does. A container's 64 MiB /dev/shm keeps the stores'
objects on disk; with ``--shm-size`` of 2.5 GiB or more they are kept in
memory, and with ``CHIMAERA_MEMORY_POOL`` set below the container's share of
each peer the stores spill them. On Linux the manifest records the cgroup's
memory limit, its peak and its OOM kills, the size of /dev/shm and each
peer's pool.

The partition count on a cluster: start the peers on their hosts, then run
this on the smallest one, with ``CHIMAERA_HOME`` naming its peer, as
``--join --nodes N --partitions 0 --maps M --memory 2GiB``. ``--partitions 0``
leaves the count to reduce_by_key, and the run fails unless it is the live
peers' cores (with at least as many maps as cores). A gather that grows past
``--memory`` fails the run, and the report has the most any gather grew.
``--metrics`` lists the peers' /metrics addresses
(``CHIMAERA_METRICS_LISTEN``) to read their counters from; without it a
joined run has none. On one machine, ``--driver-cpus 1`` gives the script's
peer one core and the others ``--cpus`` each.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PHASES = ["plan", "map", "split", "scatter_encode", "fetch", "read", "reduce", "gather_encode", "final"]
# What each peer counts itself (its /metrics), per run.
PEER_COUNTERS = {
    "network_bytes": 'chimaera_mesh_sent_bytes_total{class="shuffle"}',
    "spilled_bytes": "chimaera_spilled_bytes_total",
    "spill_us": "chimaera_spill_microseconds_total",
}

CHILD = r"""
import json, operator, sys, time
import numpy
import chimaera as ch
from chimaera import _shuffle_phases

maps, per, parts, keys, rounds, skew = {maps}, {per}, {partitions} or None, {keys}, {rounds}, {skew!r}
memory = {memory!r} or None

def drawn(i):
    if skew == "zipf":
        return numpy.random.default_rng(i).zipf(1.2, per) % keys
    return (i * 7919 + numpy.arange(per)) % keys

def produce(i):
    return [(int(k), 1) for k in drawn(i)]

while sum(n.alive for n in ch.runs.cluster().nodes()) < {nodes}:
    time.sleep(0.1)
cores = sum(max(1, int(n.cpus)) for n in ch.runs.cluster().nodes() if n.alive and not n.draining)
gen = ch.remote(produce)
# The reference: the same pairs counted serially, digested a slice at a time.
counts = numpy.zeros(keys, dtype=numpy.int64)
for i in range(maps):
    counts += numpy.bincount(drawn(i), minlength=keys)
ref, present = _shuffle_phases.Digest(), numpy.flatnonzero(counts)
for at in range(0, len(present), 1 << 16):  # small slices: the driver holds little at once
    some = present[at : at + (1 << 16)]
    ref.add(dict(zip(some.tolist(), counts[some].tolist())))
reference = ref.value()
del counts, present
list(gen.map(range(min(maps, 64))).reduce_by_key(operator.add, partitions=min(parts or 64, 64)))  # warm-up
for r in range(rounds):
    print("COUNT", flush=True)
    sys.stdin.readline()  # the parent has read the peers' counters
    got, largest = _shuffle_phases.Digest(), [0]

    def each(part):
        got.add(part)
        largest[0] = max(largest[0], len(part))

    t0 = time.perf_counter()
    with _shuffle_phases.Record() as rec:
        out = gen.map(range(maps)).reduce_by_key(operator.add, partitions=parts, memory=memory)
    rec.final(out, each)
    seconds = time.perf_counter() - t0
    run = {{"round": r, "seconds": round(seconds, 4), "phases": rec.phases(), "timeline": rec.timeline(),
           "digest_ok": got.value() == reference, "largest_partition": largest[0],
           "partitions": len(out.futures), "cluster_cores": cores, "memory": memory}}
    print("RUN " + json.dumps(run), flush=True)
"""


def counters(addresses: list[str]) -> dict[str, float]:
    """PEER_COUNTERS summed over the peers' /metrics (host:port each)."""
    total = dict.fromkeys(PEER_COUNTERS, 0.0)
    for address in addresses:
        with urllib.request.urlopen(f"http://{address}/metrics", timeout=10) as r:
            text = r.read().decode()
        for line in text.splitlines():
            name, _, value = line.rpartition(" ")
            for key, metric in PEER_COUNTERS.items():
                if name == metric:
                    total[key] += float(value)
    return total


def median_table(runs: list[dict]) -> list[str]:
    rows = ["| Phase | Seconds (sum over calls) | CPU seconds (sum) | Per call, median (s) | Slowest call (s) | Bytes |",
            "|---|---:|---:|---:|---:|---:|"]
    for name in PHASES:
        med = {k: statistics.median(r["phases"][name].get(k, 0.0) for r in runs)
               for k in ("seconds", "cpu_seconds", "median_seconds", "max_seconds", "bytes")}
        size = "" if name == "fetch" else f"{med['bytes']:,.0f}"
        cpu = "" if name in ("plan", "final") else f"{med['cpu_seconds']:.3f}"  # the driver's: not timed
        rows.append(f"| {name} | {med['seconds']:.3f} | {cpu} | {med['median_seconds']:.4f} | "
                    f"{med['max_seconds']:.3f} | {size} |")
    if all(r["peers"] for r in runs):  # a joined run without --metrics has none
        net = statistics.median(r["peers"]["network_bytes"] for r in runs)
        rows.append(f"| network (the peers' shuffle sends) | | | | | {net:,.0f} |")
        spill = statistics.median(r["peers"]["spill_us"] for r in runs) / 1e6
        rows.append(f"| spill (the peers' stores) | {spill:.3f} | | | | "
                    f"{statistics.median(r['peers']['spilled_bytes'] for r in runs):,.0f} |")
    growth = statistics.median(r["phases"]["reduce"]["rss_growth_max"] for r in runs)
    rows.append(f"| most a gather grew its process | | | | | {growth:,.0f} |")
    return rows


def timeline_table(runs: list[dict]) -> list[str]:
    rows = ["| Boundary (ms after the driver's start) | First | Median | Last |", "|---|---:|---:|---:|"]
    for name in runs[0]["timeline"]:
        med = {k: statistics.median(r["timeline"][name][k] for r in runs) for k in ("first", "median", "last")}
        rows.append(f"| {name} | {med['first']:.1f} | {med['median']:.1f} | {med['last']:.1f} |")
    return rows


def limits() -> dict:
    """What a Linux run was held to: its cgroup's memory limit (None:
    unlimited), the most it used (page cache included) and its OOM kills,
    and the size of /dev/shm, where a store keeps objects in memory when it
    has room. {} elsewhere."""
    if not sys.platform.startswith("linux"):
        return {}

    def read(name: str) -> str:
        try:
            with open(os.path.join("/sys/fs/cgroup", name)) as f:
                return f.read().strip()
        except OSError:
            return ""

    events = dict(line.split() for line in read("memory.events").splitlines())
    shm = os.statvfs("/dev/shm")
    return {"memory_max": int(read("memory.max")) if read("memory.max").isdigit() else None,
            "memory_peak": int(read("memory.peak") or 0), "oom_kill": int(events.get("oom_kill", 0)),
            "shm_bytes": shm.f_blocks * shm.f_frsize}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=3, help="peers to start, or with --join to wait for")
    ap.add_argument("--cpus", type=int, default=4, help="each peer's --cpus")
    ap.add_argument("--driver-cpus", type=int, default=0, help="the script's peer's --cpus (default --cpus)")
    ap.add_argument("--join", action="store_true", help="run on the cluster of the peer in CHIMAERA_HOME")
    ap.add_argument("--metrics", default="", help="with --join: the peers' /metrics addresses, host:port,...")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--maps", type=int, default=64)
    ap.add_argument("--per", type=int, default=50_000)
    ap.add_argument("--partitions", type=int, default=64, help="0: reduce_by_key chooses")
    ap.add_argument("--memory", default="", help="reduce_by_key's memory= (e.g. 2GiB)")
    ap.add_argument("--keys", type=int, default=100_003)
    ap.add_argument("--skews", default="uniform,zipf")
    ap.add_argument("--name", default="", help="added to the result files' names (e.g. larger-than-memory)")
    ap.add_argument("--date", default=time.strftime("%Y-%m-%d"),
                    help="the files' date (default today here; give the host's to a container on UTC)")
    ap.add_argument("--out", default=os.path.join(HERE, "results"))
    a = ap.parse_args()
    joined = os.environ.get("CHIMAERA_HOME") or os.path.expanduser("~/.chimaera/local")  # before conftest sets its own
    sys.path.insert(0, os.path.join(REPO, "python", "tests"))
    sys.path.insert(0, HERE)
    import manifest
    import test_cluster  # the tested multi-peer harness

    path = os.environ.get("PYTHONPATH") or os.path.join(REPO, "python")
    if a.join:
        cluster, addresses, home, layout = None, [m for m in a.metrics.split(",") if m], joined, "joined cluster"
    else:
        ports = test_cluster.free_ports(a.nodes)
        addresses = [f"127.0.0.1:{p}" for p in ports]
        cpus = {i: a.cpus for i in range(a.nodes)} | ({0: a.driver_cpus} if a.driver_cpus else {})
        cluster = test_cluster.Cluster(a.nodes, env={i: {"CHIMAERA_METRICS_LISTEN": m} for i, m in enumerate(addresses)},
                                       cpus=cpus)
        home, layout = cluster.dir(0), "one machine"
    binary = manifest.running_binary(home) or test_cluster.BIN
    env = dict(os.environ, CHIMAERA_HOME=home, PYTHONPATH=path)
    results: dict[str, list[dict]] = {}
    try:
        for skew in a.skews.split(","):
            code = CHILD.format(maps=a.maps, per=a.per, partitions=a.partitions, keys=a.keys, rounds=a.rounds,
                                skew=skew, nodes=a.nodes, memory=a.memory)
            child = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     text=True, env=env, cwd=cluster.root if cluster else None)
            runs, before = [], {}
            for line in child.stdout:
                if line.startswith("COUNT"):
                    before = counters(addresses)
                    child.stdin.write("\n")
                    child.stdin.flush()
                elif line.startswith("RUN "):
                    run = json.loads(line[4:])
                    after = counters(addresses)
                    run["peers"] = {k: after[k] - before[k] for k in PEER_COUNTERS} if addresses else {}
                    runs.append(run)
                    print(skew, json.dumps(run), flush=True)
            if child.wait() != 0 or len(runs) != a.rounds:
                raise SystemExit(f"the {skew} driver failed (exit {child.returncode})")
            results[skew] = runs
    finally:
        if cluster:
            cluster.close()

    stamp = a.date
    doc = {
        "manifest": manifest.collect(binary, {"chimaera": sys.executable},
                                     topology={"nodes": a.nodes, "cpus_per_node": a.cpus,
                                               "driver_cpus": a.driver_cpus or a.cpus, "layout": layout,
                                               "machine_cpus": os.cpu_count() or 0,
                                               "memory_pool": os.environ.get("CHIMAERA_MEMORY_POOL") or "auto"},
                                     shuffle={"maps": a.maps, "per": a.per, "partitions": a.partitions,
                                              "memory": a.memory, "keys": a.keys},
                                     limits=limits()),
        "results": results,
    }
    os.makedirs(a.out, exist_ok=True)
    name = f"shuffle-phases-{a.nodes}-{a.name + '-' if a.name else ''}{stamp}"
    with open(os.path.join(a.out, f"{name}.json"), "w") as f:
        json.dump(doc, f, indent=2)
    lines = [f"# Shuffle phases, {a.nodes} peers ({layout}, {stamp})", "",
             f"{a.maps} maps of {a.per:,} pairs into {a.partitions or 'the chosen number of'} partitions, "
             f"{a.keys:,} keys; median of {a.rounds} runs.", ""]
    held = doc["manifest"]["limits"]
    if held:
        lines += [f"Held to {held['memory_max'] or 'no limit'} bytes (cgroup memory.max); memory.peak "
                  f"{held['memory_peak']:,} (page cache included); OOM kills {held['oom_kill']}; /dev/shm "
                  f"{held['shm_bytes']:,} bytes; each peer's memory pool "
                  f"{doc['manifest']['topology']['memory_pool']}.", ""]
    for skew, runs in results.items():
        ok = all(r["digest_ok"] for r in runs)
        lines += [f"## {skew}: {statistics.median(r['seconds'] for r in runs):.3f} s, digest "
                  f"{'matches' if ok else 'DIFFERS from'} the serial reference, largest partition "
                  f"{max(r['largest_partition'] for r in runs):,} keys, {runs[0]['partitions']} partitions over "
                  f"{runs[0]['cluster_cores']} cores", "", *median_table(runs), "", *timeline_table(runs), ""]
    with open(os.path.join(a.out, f"{name}.md"), "w") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    runs = [r for rs in results.values() for r in rs]
    if not all(r["digest_ok"] for r in runs):
        raise SystemExit("an output differs from the serial reference")
    if not a.partitions and a.maps >= runs[0]["cluster_cores"]:
        wrong = {r["partitions"] for r in runs} - {runs[0]["cluster_cores"]}
        if wrong:
            raise SystemExit(f"reduce_by_key chose {wrong} partitions, not the cluster's {runs[0]['cluster_cores']} cores")


if __name__ == "__main__":
    main()
