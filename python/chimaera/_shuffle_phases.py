"""Where a shuffle's time and bytes go, phase by phase: a record a benchmark
or test opens around the shuffles it measures (bench/evals/shuffle_phases.py).
Shuffles started outside one run as they always do.

    with _shuffle_phases.Record() as rec:
        out = produce.map(items).reduce_by_key(operator.add, partitions=64)
    parts = rec.final(out)  # every partition in the driver
    rec.phases()            # {"plan": {"seconds": ..., "median_seconds": ..., "max_seconds": ..., "bytes": ...}, ...}
    rec.timeline()          # {"submitted": {"first": ms, "median": ms, "last": ms}, "scatter_start": ...}

While a record is open, the scatters, the manifest call and the gathers of
each shuffle it starts time their own steps and report them once
(``ch.context().report``: one replicated write per call), wherever they ran,
and the record reads the reports back. The phases:

- ``plan``: the driver submitting the scatters, the manifest and the gathers.
- ``map``: the mapped function, inside the scatters of a map nothing read
  (a generator it returns runs during ``split``).
- ``split``: hashing pairs into buckets and combining them; ``pairs`` (when
  the input has a length) and ``buckets`` touched.
- ``scatter_encode``: encoding the buckets as the store does. The executor
  encodes a call's result after the call returns, so the scatter encodes
  them once more itself to time it: a recorded scatter encodes twice. Bytes:
  the encoded buckets.
- ``fetch``: gathers bringing scatter results and buckets from other peers.
  Its bytes are the peers' to count, not a call's: gathers on one peer may
  wait on the same fetch. The benchmark reads what crossed the network from
  the peers' shuffle traffic (``chimaera_mesh_sent_bytes_total``).
- ``read``: gathers reading and decoding their buckets. Bytes: the buckets'
  encoded size.
- ``reduce``: gathers folding the buckets into partitions; ``keys`` out, and
  ``rss_growth_max``: the most any gather grew its process's private memory
  by (``_resident``, sampled after each bucket's read and fold).
- ``gather_encode``: encoding each partition as the store does, timed the
  same way. Bytes: the encoded partitions.
- ``final``: the driver bringing every partition to it, once all are done.

A phase's ``seconds`` (wall clock) add up over its calls, and so do its
``cpu_seconds``, the CPU the calls' threads spent in it (a fetch waits on
other threads and on the peer, so it takes little); ``median_seconds`` and
``max_seconds`` are per call. What the peers' stores spill is the stores'
work, not a call's: the benchmark reads it from each peer's
``chimaera_spilled_bytes_total`` and ``chimaera_spill_microseconds_total``.

The timeline gives each stage boundary as milliseconds after the record
opened, for the first, median and last call to pass it: the driver's submit
(``submit_start``, ``submitted``); each scatter's start, the end of its map
and split (``scatter_mapped``, ``scatter_split``) and its end; the manifest
call's start, the moment it had seen every scatter finish
(``manifest_waited``) and its return; each gather's start, the end of its
last fetch (``gather_fetched``), of its fold (``gather_merged``) and its end;
the driver seeing every partition done (``driver_done``) and holding them all
(``driver_collected``). Calls stamp them with their own machine's clock.
"""

from __future__ import annotations

import collections
import hashlib
import os
import statistics
import time
from typing import Any

current: Record | None = None

SCATTER = ("map", "split", "scatter_encode")
GATHER = ("fetch", "read", "reduce", "gather_encode")


class Record:
    """The shuffles started while it is open (see the module's docstring)."""

    def __init__(self) -> None:
        self.t0 = 0.0
        self.shuffles: list[tuple[float, float, Any, list[Any], list[Any]]] = []
        self.final_s = 0.0
        self.done_at: list[float] = []
        self.collected_at: list[float] = []

    def __enter__(self) -> Record:
        global current
        current, self.t0 = self, time.time()
        return self

    def __exit__(self, *exc: Any) -> None:
        global current
        current = None

    def add(self, start: float, manifest: Any, scatters: list[Any], gathers: list[Any]) -> None:
        """A shuffle whose driver began submitting at `start` and has just finished."""
        self.shuffles.append((start, time.time(), manifest, list(scatters), list(gathers)))

    def final(self, partitioned: Any, each: Any = None) -> list[dict[Any, Any]]:
        """Every partition of `partitioned`, timed from the moment all are
        done. With `each`, each partition is handed to it in turn and none is
        kept (a run larger than the driver's memory)."""
        from ._future import wait

        wait(list(partitioned.futures))
        t = time.time()
        self.done_at.append(t)
        parts = []
        for part in partitioned:
            if each is None:
                parts.append(part)
            else:
                each(part)
        now = time.time()
        self.final_s += now - t
        self.collected_at.append(now)
        return parts

    def phases(self) -> dict[str, dict[str, float]]:
        per_call: dict[str, list[float]] = {p: [] for p in ("plan", *SCATTER, *GATHER, "final")}
        out = {
            p: {"seconds": 0.0, "median_seconds": 0.0, "max_seconds": 0.0, "cpu_seconds": 0.0, "bytes": 0}
            for p in per_call
        }

        def add(phase: str, seconds: float, size: int = 0, cpu: float = 0.0) -> None:
            per_call[phase].append(seconds)
            out[phase]["bytes"] += size
            out[phase]["cpu_seconds"] += cpu

        counts: collections.Counter[str] = collections.Counter()
        growth = 0
        for start, end, _, scatters, gathers in self.shuffles:
            add("plan", end - start)
            for calls, kind, names in ((scatters, "scatter", SCATTER), (gathers, "gather", GATHER)):
                for f in calls:
                    report = _report(f, kind)
                    for name in names:
                        add(
                            name,
                            report.get(f"{name}_s", 0.0),
                            report.get(f"{name}_bytes", 0),
                            report.get(f"{name}_cpu_s", 0.0),
                        )
                    for k in ("pairs", "buckets", "keys"):
                        counts[k] += report.get(k) or 0
                    growth = max(growth, report.get("rss_growth") or 0)
        add("final", self.final_s)
        for phase, seconds in per_call.items():
            if seconds:
                out[phase].update(
                    seconds=sum(seconds), median_seconds=statistics.median(seconds), max_seconds=max(seconds)
                )
        out["split"]["pairs"] = counts["pairs"]
        out["split"]["buckets"] = counts["buckets"]
        out["reduce"]["keys"] = counts["keys"]
        out["reduce"]["rss_growth_max"] = growth
        return out

    def timeline(self) -> dict[str, dict[str, float]]:
        """Each stage boundary in ms after the record opened (see the module's docstring)."""
        marks: dict[str, list[float]] = collections.defaultdict(list)

        def at(name: str, t: float | None) -> None:
            if t:
                marks[name].append((t - self.t0) * 1000)

        for start, end, manifest, scatters, gathers in self.shuffles:
            at("submit_start", start)
            at("submitted", end)
            for f in scatters:
                r = _report(f, "scatter")
                for name, t in (("start", r["start"]), ("mapped", r.get("map_at")), ("split", r.get("split_at"))):
                    at(f"scatter_{name}", t)
                at("scatter_end", r["end"])
            r = _report(manifest, "manifest")
            for name, t in (("start", r["start"]), ("waited", r.get("wait_at")), ("end", r["end"])):
                at(f"manifest_{name}", t)
            for f in gathers:
                r = _report(f, "gather")
                for name, t in (("start", r["start"]), ("fetched", r.get("fetch_at")), ("merged", r.get("reduce_at"))):
                    at(f"gather_{name}", t)
                at("gather_end", r["end"])
        for t in self.done_at:
            at("driver_done", t)
        for t in self.collected_at:
            at("driver_collected", t)
        return {
            name: {"first": round(min(v), 3), "median": round(statistics.median(v), 3), "last": round(max(v), 3)}
            for name, v in marks.items()
        }


def _report(f: Any, kind: str) -> dict[str, Any]:
    """The phases call `f` reported, once it has finished."""
    from ._future import wait

    wait([f])  # its outcome, not its value
    return next(m for m in reversed(f.metrics()) if m.get("phase") == kind)


class Clock:
    """A call's own phases: ``lap(name)`` adds the time since the last lap
    to `name`, and this thread's CPU time, and marks when it ended (wall
    clock, for the timeline)."""

    def __init__(self) -> None:
        self.start = self.at = time.time()
        self.cpu_at = time.thread_time()
        self.seconds: collections.defaultdict[str, float] = collections.defaultdict(float)
        self.cpu: collections.defaultdict[str, float] = collections.defaultdict(float)
        self.bytes: collections.defaultdict[str, int] = collections.defaultdict(int)
        self.ends: dict[str, float] = {}

    def lap(self, phase: str) -> None:
        now, cpu = time.time(), time.thread_time()
        self.seconds[phase] += now - self.at
        self.cpu[phase] += cpu - self.cpu_at
        self.at = self.ends[phase] = now
        self.cpu_at = cpu

    def count(self, phase: str, size: int) -> None:
        """Bytes for `phase`, counted outside any lap."""
        self.bytes[phase] += size
        self.at, self.cpu_at = time.time(), time.thread_time()

    def encode(self, phase: str, values: Any) -> None:
        """Time encoding `values` as the store does, under `phase`, with their bytes."""
        from . import _serde

        size = sum(_serde.dumps_parts(v)[1] for v in values)
        self.lap(phase)
        self.count(phase, size)

    def report(self, kind: str, **counts: Any) -> None:
        from ._context import current_task

        ctx = current_task()
        if ctx is None:
            raise RuntimeError("a shuffle's call reports its phases from inside the call")
        fields: dict[str, Any] = {"start": self.start, "end": time.time()}
        fields.update({f"{p}_s": s for p, s in self.seconds.items()})
        fields.update({f"{p}_cpu_s": s for p, s in self.cpu.items()})
        fields.update({f"{p}_at": t for p, t in self.ends.items()})
        fields.update({f"{p}_bytes": n for p, n in self.bytes.items()})
        ctx.report(phase=kind, **fields, **counts)


def bucket_size(inline: bytes | None, path: str, index: int, count: int) -> int:
    """The encoded size of bucket `index` of a scatter's stored result: its
    `inline` bytes, or else the file at `path`."""
    from . import _serde

    if inline is not None:
        entry = _serde._seq_entry(memoryview(inline), index, count)
    else:
        fd = os.open(path, os.O_RDONLY)
        try:
            entry = _serde._file_entry(fd, index, count)
        finally:
            os.close(fd)
    return 0 if entry is None else entry[2]


class Digest:
    """``digest`` taken a partition at a time, keeping none of them."""

    def __init__(self) -> None:
        self.total = self.count = 0

    def add(self, part: dict[Any, Any]) -> None:
        for key, value in part.items():
            h = hashlib.blake2b(repr((key, value)).encode(), digest_size=16).digest()
            self.total = (self.total + int.from_bytes(h, "little")) % (1 << 128)
            self.count += 1

    def value(self) -> tuple[str, int]:
        return f"{self.total:032x}", self.count


def digest(partitions: Any) -> tuple[str, int]:
    """A digest of a shuffle's output, the same for any partitioning and
    order of the same (key, value) pairs, and their count: the reference a
    run is checked against is ``digest([serial_dict])``. Keys and values are
    digested by repr, so a key's type counts (1 and 1.0 differ), as it does
    in the dict a serial fold makes. A key in two partitions counts twice."""
    d = Digest()
    for part in partitions:
        d.add(part)
    return d.value()
