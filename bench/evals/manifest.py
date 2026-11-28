"""What a benchmark run ran on, recorded beside its numbers, and the
statistics and table its reports print.

A comparison says what it compared: the code (its commit and uncommitted diff
where git can tell, and always a digest of the source files themselves, since
a cluster job uploads the tree without .git), how the peer binary was built,
each runtime's version, the machines, the durability class the peers report
and the raw samples behind every median. ``validate`` refuses a comparison
that cannot name a measured runtime's version or the durability class of a
Chimaera measurement, and ``report`` validates before it prints a row.

    python bench/evals/manifest.py   # this checkout's source identity, to match a run's
"""

from __future__ import annotations

import collections
import hashlib
import json
import os
import platform
import random
import re
import statistics
import subprocess
import sys
import time
import tomllib
from typing import Any, Callable

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
# What the source digest covers: what builds the peer and the package, and the evals.
SOURCE = (
    "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "pyproject.toml",
    "crates", "proto", "python/chimaera", "bench/evals",
)
SKIP_DIRS = {"__pycache__", "target", "results"}
SKIP_SUFFIXES = (".pyc", ".so")
# Guarantee names, weakest first (chimaera_core::durability::Guarantee).
GUARANTEES = ("process", "replicated-memory", "reboot", "machine")
SYNC = re.compile(r"raft sync (\S+) \(")


class ManifestError(ValueError):
    """A comparison whose manifest cannot say what was compared."""


def _out(cmd: list[str]) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=REPO)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return p.stdout.strip() if p.returncode == 0 else ""


def _files(path: str):
    if os.path.isfile(path):
        yield path
        return
    for d, dirs, names in os.walk(path):
        dirs[:] = sorted(x for x in dirs if x not in SKIP_DIRS)
        yield from (os.path.join(d, x) for x in sorted(names) if not x.endswith(SKIP_SUFFIXES))


def source(repo: str = REPO) -> dict[str, str]:
    """The code: a digest of its files, plus its commit and a digest of the
    uncommitted diff ("none" when clean) where git can tell."""
    h = hashlib.sha256()
    for top in SOURCE:
        for path in _files(os.path.join(repo, top)):
            with open(path, "rb") as f:
                data = f.read()
            h.update(f"{os.path.relpath(path, repo)}\0{len(data)}\0".encode())
            h.update(data)
    out = {"digest": h.hexdigest()[:16]}
    commit = _out(["git", "-C", repo, "rev-parse", "HEAD"])
    if commit:
        diff = subprocess.run(["git", "-C", repo, "diff", "HEAD", "--binary"], capture_output=True, check=False).stdout
        out.update(commit=commit, diff=hashlib.sha256(diff).hexdigest()[:16] if diff else "none")
    return out


def build(binary: str | None) -> dict[str, Any]:
    """How the peer binary was built: its cargo profile (from its path under
    target/, else EVAL_CARGO_PROFILE from the script that built it), that
    profile's settings in Cargo.toml, rustc and RUSTFLAGS."""
    path = os.path.realpath(binary) if binary else ""
    parts = path.split(os.sep)
    # target/debug holds the `dev` profile's output.
    profile = next((p for d, p in (("release", "release"), ("debug", "dev")) if d in parts), "")
    profile = profile or os.environ.get("EVAL_CARGO_PROFILE", "")
    with open(os.path.join(REPO, "Cargo.toml"), "rb") as f:
        settings = tomllib.load(f).get("profile", {}).get(profile, {})
    return {
        # Relative to the checkout: reports carry no home directory.
        "binary": os.path.relpath(path, REPO) if path.startswith(REPO + os.sep) else path,
        "profile": profile or "unknown",
        "settings": settings,
        "rustc": _out(["rustc", "-V"]),
        "rustflags": os.environ.get("RUSTFLAGS", ""),
    }


def host() -> dict[str, Any]:
    """The machine and image the driver runs on (EVAL_IMAGE names a container image)."""
    try:
        distro = platform.freedesktop_os_release().get("PRETTY_NAME", "")
    except OSError:  # not Linux
        distro = ""
    return {
        "platform": platform.platform(),
        "distro": distro,
        "cpus": os.cpu_count(),
        "image": os.environ.get("EVAL_IMAGE", ""),
    }


def collect(binary: str | None, pythons: dict[str, str], **extra: Any) -> dict[str, Any]:
    """The manifest of a run whose peers run `binary` and whose drivers run
    under `pythons` (runtime -> interpreter). `extra` adds topology and order."""
    py = "import platform; print(platform.python_version())"
    versions: dict[str, Any] = {"python": {rt: _out([exe, "-c", py]) for rt, exe in pythons.items()}}
    if binary:
        versions["chimaera"] = _out([binary, "--version"])
    if "ray" in pythons:
        versions["ray"] = _out([pythons["ray"], "-c", "import ray; print(ray.__version__)"])
    if "dask" in pythons:
        versions["dask"] = _out([pythons["dask"], "-c", "import distributed; print(distributed.__version__)"])
    return {"source": source(), "build": build(binary), "versions": versions, "host": host(), **extra}


def running_binary(home: str) -> str | None:
    """The binary the peer in `home` runs: it writes the path to peer.binary once ready."""
    try:
        with open(os.path.join(home, "peer.binary")) as f:
            return f.readline().strip() or None
    except OSError:
        return None


def status(binary: str | None, home: str) -> dict[str, Any]:
    """The peer's `chimaera status --json`, or {} when it does not answer."""
    out = _out([binary, "--dir", home, "status", "--json"]) if binary else ""
    return json.loads(out) if out else {}


def durability(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    """The durability class the peers report: the guarantee asked for, the
    weakest any of them sees a group give (what an acknowledged commit
    survives), the weakest an attached run's per-call commits get (`calls`;
    empty from a peer too old to say), how they write their raft logs (a
    local peer keeps none) and why service is below what was asked. {} when
    no peer answered."""
    seen = [s["durability"] for s in statuses if s.get("durability")]
    if not seen:
        return {}

    def rank(g: str) -> int:
        return GUARANTEES.index(g) if g in GUARANTEES else -1

    return {
        "requested": min((d["requested"] for d in seen), key=rank),
        "effective": min((d["effective"] for d in seen), key=rank),
        "calls": min((d.get("calls", "") for d in seen), key=rank),
        "raft_sync": sorted({m[1] for s in statuses for line in s.get("automatic", []) if (m := SYNC.match(line))}),
        "degraded": sorted({x for d in seen for x in d.get("degraded", [])}),
    }


def settled_durability(read: Callable[[], list[dict[str, Any]]], timeout: float = 30) -> dict[str, Any]:
    """The durability class of the peers `read` returns the statuses of, once
    every one gives what it asks for. A peer reports a group as process until
    it learns the group's voters, so a cluster still forming reads below its
    class; after `timeout` s the reading stands as what the peers give."""
    deadline = time.monotonic() + timeout
    while True:
        statuses = read()
        seen = [s.get("durability") or {} for s in statuses]
        if seen and all(d and d["effective"] == d["requested"] for d in seen) or time.monotonic() > deadline:
            return durability(statuses)
        time.sleep(0.2)


def raft_entries(statuses: list[dict[str, Any]]) -> int:
    """Raft entries applied by the groups these peers host, summed over groups:
    the commits behind the work done between two readings."""
    applied: dict[int, int] = {}
    for s in statuses:
        for g in s.get("groups", []):
            applied[g["group"]] = max(applied.get(g["group"], 0), g["applied"])
    return sum(applied.values())


# ---- statistics -------------------------------------------------------------------


def ci(trials: list[list[float]], level: float = 0.95, rounds: int = 2000, seed: int = 0) -> list[float]:
    """Bootstrap interval of the median: resample the runs, then the samples
    within each, so the spread between runs counts as well as within them."""
    rng = random.Random(seed)
    medians = []
    for _ in range(rounds):
        pool = [x for t in rng.choices(trials, k=len(trials)) for x in rng.choices(t, k=len(t))]
        medians.append(statistics.median(pool))
    medians.sort()
    return [medians[int((1 - level) / 2 * rounds)], medians[int((1 + level) / 2 * rounds) - 1]]


# Per-run fields that are bookkeeping, not results.
_BOOKKEEPING = {
    "metric", "reps", "p10", "p90", "samples", "unit", "digests", "ops", "executions", "raft_entries", "direct_calls",
}


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    """One row's numbers from the runs of one workload on one runtime: the
    median over every timed sample with its 95% interval, the spread, the
    output digests the runs saw, commits per unit of work and what share of
    actor calls went direct."""
    trials = [r["samples"] for r in results]
    xs = sorted(x for t in trials for x in t)
    out: dict[str, Any] = {
        "metric": results[0]["metric"],
        "unit": results[0]["unit"],
        "median": statistics.median(xs),
        "ci95": ci(trials),
        "p10": xs[int(0.1 * (len(xs) - 1))],
        "p90": xs[int(0.9 * (len(xs) - 1))],
        "samples": len(xs),
        "runs": len(results),
    }
    also = sorted(
        k for k, v in results[0].items() if isinstance(v, (int, float)) and k not in _BOOKKEEPING and k != out["metric"]
    )
    if also:  # the rest of what the runs reported (Ray's hand-batched rate, nodes reached, ...)
        out["also"] = {k: statistics.median(r[k] for r in results if k in r) for k in also}
    digests: collections.Counter[str] = collections.Counter()
    for r in results:
        digests.update(r.get("digests", {}))
    if digests:
        out["digests"] = dict(digests)
    ops = sum(r.get("ops", 0) for r in results)
    if all("raft_entries" in r for r in results):
        entries = sum(r["raft_entries"] for r in results)
        if ops:
            out["raft_entries_per_op"] = entries / ops
        else:  # a timed job, not a rate: commits per execution of it, warm-ups included
            out["raft_entries_per_run"] = entries / sum(r["executions"] for r in results)
    if ops and all("direct_calls" in r for r in results):
        out["direct_share"] = sum(r["direct_calls"] for r in results) / ops
    return out


def _variant(record: dict[str, Any]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted((record.get("variant") or {}).items()))


def validate(doc: dict[str, Any]) -> None:
    """Refuse a comparison that cannot say what it compared: each runtime it
    measured needs its version, and each Chimaera measurement the durability
    class its peers reported."""
    versions = doc.get("manifest", {}).get("versions", {})
    records = doc.get("results", [])
    missing = sorted({f"{r['runtime']} version" for r in records if not versions.get(r["runtime"])})
    missing += sorted(
        {
            f"durability class for chimaera {r['workload']}" + (f" ({_variant(r)})" if r.get("variant") else "")
            for r in records
            if r["runtime"] == "chimaera" and not (r.get("durability") or {}).get("effective")
        }
    )
    if missing:
        raise ManifestError("not a comparable run, its manifest lacks: " + "; ".join(missing))


def table(doc: dict[str, Any], workloads: list[str]) -> list[dict[str, Any]]:
    """Validated rows, one per workload (and Chimaera variant) in the order of
    `workloads`, each runtime's summary or first error."""
    validate(doc)
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for r in doc["results"]:
        variant = _variant(r)
        row = rows.setdefault((r["workload"], variant), {"workload": r["workload"], "variant": variant})
        row.setdefault(r["runtime"], []).append(r)
    for row in rows.values():
        for rt in ("ray", "dask", "chimaera"):
            runs = row.pop(rt, [])
            ok = [r["result"] for r in runs if "result" in r]
            if ok:
                row[rt] = summarize(ok)
            elif runs:
                row[rt] = {"error": runs[0].get("error", "")[:200]}
    return sorted(rows.values(), key=lambda r: (workloads.index(r["workload"]), r["variant"]))


def _num(x: float) -> str:
    return f"{x:,.0f}" if abs(x) >= 100 or x == int(x) else f"{x:.3f}"


def _cell(s: dict[str, Any] | None) -> str:
    if not s:
        return ""
    if "error" in s:
        return "error: " + s["error"].replace("|", "/").replace("\n", " ")[:120]
    lo, hi = s["ci95"]
    text = f"{_num(s['median'])} [{_num(lo)}, {_num(hi)}], {s['samples']} samples in {s['runs']} runs"
    if "digests" in s:
        text += ", output " + " ".join(sorted(s["digests"]))
    if "also" in s:
        text += "; " + ", ".join(f"{k} {_num(x)}" for k, x in s["also"].items())
    return text


def _path(s: dict[str, Any] | None) -> str:
    if not s or "error" in s:
        return ""
    parts = []
    if "raft_entries_per_op" in s:
        parts.append(f"{s['raft_entries_per_op']:.3f} raft entries per unit")
    if "raft_entries_per_run" in s:
        parts.append(f"{s['raft_entries_per_run']:,.0f} raft entries per run")
    if "direct_share" in s:
        parts.append(f"{s['direct_share']:.0%} direct")
    return ", ".join(parts) or "not counted"


def report(doc: dict[str, Any], workloads: list[str], title: str, notes: list[str]) -> str:
    """The comparison as markdown: what ran where, then one row per workload
    with its unit, each runtime's median [95% interval], and how Chimaera
    committed the work. Raises ManifestError for an incomplete manifest."""
    rows = table(doc, workloads)
    dask = any("dask" in row for row in rows)  # a Dask column only when Dask ran
    m = doc["manifest"]
    src, b, v, d = m["source"], m["build"], m["versions"], m.get("durability", {})
    durable = ", ".join(f"{k} {' / '.join(x) if isinstance(x, list) else x}" for k, x in d.items() if x)
    lines = [
        f"# {title}",
        "",
        *notes,
        f"Source digest {src['digest']}"
        + (f", commit {src['commit'][:12]}, uncommitted diff {src['diff']}" if "commit" in src else "")
        + f". Peer {v.get('chimaera', '')}, {b['profile']} build ({b['rustc'] or 'rustc unknown'}"
        + (f", RUSTFLAGS {b['rustflags']}" if b["rustflags"] else "")
        + ")"
        + (f", Ray {v['ray']}" if v.get("ray") else "")
        + (f", Dask {v['dask']}" if v.get("dask") else "")
        + ", Python "
        + ", ".join(f"{rt} {x}" for rt, x in v.get("python", {}).items())
        + ".",
        f"Durability: {durable or 'per row in the JSON'}.",
        "Cells: median [bootstrap 95% interval of the median], over every timed sample of every run.",
        "",
        "| Workload | Unit | Ray | " + ("Dask | " if dask else "") + "Chimaera | Chimaera commits |",
        "|---|---|---|" + ("---|" if dask else "") + "---|---|",
    ]
    for row in rows:
        name = row["workload"] + (f" ({row['variant']})" if row["variant"] else "")
        s = row.get("chimaera") or row.get("ray") or row.get("dask") or {}
        lines.append(
            f"| {name} | {s.get('unit', '')} | {_cell(row.get('ray'))} | "
            + (f"{_cell(row.get('dask'))} | " if dask else "")
            + f"{_cell(row.get('chimaera'))} | {_path(row.get('chimaera'))} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    json.dump(source(), sys.stdout, indent=2)
    print()
