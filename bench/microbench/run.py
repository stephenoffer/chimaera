"""Run Ray's microbenchmark suite and its Chimaera port, alternating, and tabulate medians.

    python bench/microbench/run.py [--rounds 3] [--sleep 10] [--chimaera-bin PATH] [--chimaera-path DIR]

Each suite runs in a fresh process, `--rounds` times, alternating which runtime
goes first. Writes results-<date>.json (every run's values and load averages)
and RESULTS.md (medians) next to this file. --chimaera-bin / --chimaera-path
pin the peer binary and Python package (CHIMAERA_BIN / PYTHONPATH) so a tree
being rebuilt mid-run does not change what is measured.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PYTHON = {"ray": os.path.join(REPO, ".venv-bench", "bin", "python"), "chimaera": os.path.join(REPO, ".venv", "bin", "python")}
SCRIPT = {"ray": "ray_perf_ray.py", "chimaera": "ray_perf_chimaera.py"}


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def load_average() -> str:
    m = re.search(r"load averages?: (.*)$", sh("uptime"))
    return m.group(1).replace(",", "") if m else "?"


def stop_peer(peer_dir: str) -> None:
    """Stop a Chimaera peer this harness started (its pid file), then its executors."""
    try:
        with open(os.path.join(peer_dir, "peer.pid")) as f:
            pid = int(f.read().split()[0])
    except (OSError, ValueError, IndexError):
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        for _ in range(50):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)


def last_line(path: str) -> str:
    with open(path, errors="replace") as f:
        lines = [line.strip() for line in f if line.strip()]
    return lines[-1][:200] if lines else ""


def prune_snapshots(raft_dir: str) -> int:
    """Delete superseded raft snapshot files (group-G-N.snap below the newest N
    per group); the local peer never removes them, so a suite run would leave
    several GB. Returns the bytes freed."""
    snaps = os.path.join(raft_dir, "snapshots")
    try:
        names = os.listdir(snaps)
    except OSError:
        return 0
    newest: dict[str, int] = {}
    parsed = []
    for name in names:
        m = re.fullmatch(r"group-(\d+)-(\d+)\.snap", name)
        if m:
            parsed.append((name, m.group(1), int(m.group(2))))
            newest[m.group(1)] = max(newest.get(m.group(1), -1), int(m.group(2)))
    freed = 0
    for name, group, index in parsed:
        if index < newest[group]:
            path = os.path.join(snaps, name)
            try:
                freed += os.path.getsize(path)
                os.unlink(path)
            except OSError:
                pass
    return freed


def guard(proc: subprocess.Popen, home: str | None, args: argparse.Namespace, record: dict, stop: threading.Event) -> None:
    """While a suite runs: prune stale snapshots and kill the run if the shared disk gets low."""
    while not stop.wait(2.0):
        if home is not None:
            for raft in (os.path.join(home, "raft"), os.path.join(home + "-client", "peer", "raft")):
                record["snapshot_bytes_pruned"] = record.get("snapshot_bytes_pruned", 0) + prune_snapshots(raft)
        if shutil.disk_usage(args.workdir).free < args.min_free_gb * 2**30:
            record["aborted"] = f"free disk below {args.min_free_gb} GiB"
            os.killpg(proc.pid, signal.SIGKILL)
            return


def run_suite(runtime: str, args: argparse.Namespace, round_no: int) -> dict:
    env = {**os.environ, "MICROBENCH_SLEEP": str(args.sleep)}
    if runtime == "ray":
        # Ray refuses to spill (OutOfDiskError in "put gigabytes") once the disk is
        # 95% full; the shared laptop disk is past that. A safety check, not a speed knob.
        env.setdefault("RAY_local_fs_capacity_threshold", "0.99")
        # Always start a fresh local instance: without this, ray.init() joins any
        # Ray cluster another process on this machine started.
        env["RAY_ADDRESS"] = "local"
    home = None
    if runtime == "chimaera":
        home = tempfile.mkdtemp(prefix="chmb-", dir=args.workdir)
        env["CHIMAERA_HOME"] = home
        if args.chimaera_bin:
            env["CHIMAERA_BIN"] = args.chimaera_bin
        if args.chimaera_path:
            env["PYTHONPATH"] = os.pathsep.join(p for p in (args.chimaera_path, env.get("PYTHONPATH")) if p)
    log_path = os.path.join(args.workdir, f"{runtime}-round{round_no}.log")
    record = {"runtime": runtime, "round": round_no, "load_before": load_average(), "log": log_path}
    t0 = time.monotonic()
    with open(log_path, "wb") as log:
        proc = subprocess.Popen(
            [PYTHON[runtime], os.path.join(HERE, SCRIPT[runtime])],
            stdout=subprocess.PIPE, stderr=log, env=env, cwd=args.workdir, start_new_session=True,
        )
        stop = threading.Event()
        watcher = threading.Thread(target=guard, args=(proc, home, args, record, stop), daemon=True)
        watcher.start()
        try:
            out, _ = proc.communicate(timeout=args.timeout)
            record["returncode"] = proc.returncode
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            out, _ = proc.communicate()
            record["returncode"] = "timeout"
        stop.set()
        watcher.join()
    if record["returncode"] != 0:
        record["error"] = last_line(log_path)
    record["seconds"] = round(time.monotonic() - t0, 1)
    record["load_after"] = load_average()
    if home is not None:
        stop_peer(os.path.join(home + "-client", "peer"))
        stop_peer(home)
        shutil.rmtree(home, ignore_errors=True)
        shutil.rmtree(home + "-client", ignore_errors=True)
    rows = []
    for line in out.decode(errors="replace").splitlines():
        if line.startswith("{"):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "name" in row:
                rows.append(row)
    record["rows"] = rows
    print(f"{runtime} round {round_no}: {len(rows)} metrics in {record['seconds']} s, exit {record['returncode']}, "
          f"load {record['load_before']} -> {record['load_after']}", flush=True)
    return record


def machine_info(args: argparse.Namespace) -> dict:
    def version(runtime: str, code: str) -> str:
        return sh(PYTHON[runtime], "-c", code)

    info = {
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string"),
        "hw.ncpu": sh("sysctl", "-n", "hw.ncpu"),
        "memory_gib": round(int(sh("sysctl", "-n", "hw.memsize") or 0) / 2**30, 1),
        "os": f"macOS {sh('sw_vers', '-productVersion')} ({platform.machine()})",
        "python_ray": version("ray", "import sys; print(sys.version.split()[0])"),
        "python_chimaera": version("chimaera", "import sys; print(sys.version.split()[0])"),
        "ray": version("ray", "import ray; print(ray.__version__)"),
        "chimaera_git_head": sh("git", "-C", REPO, "rev-parse", "--short", "HEAD"),
        "chimaera_tree_dirty_files": len(sh("git", "-C", REPO, "status", "--porcelain").splitlines()),
    }
    if args.chimaera_bin:
        with open(args.chimaera_bin, "rb") as f:
            info["chimaera_bin_sha256"] = hashlib.sha256(f.read()).hexdigest()[:16]
    return info


def fmt(v: float, unit: str) -> str:
    if unit == "GB/s":
        return f"{v:.2f} GB/s"
    return f"{v:,.0f}" if v >= 100 else f"{v:.1f}"


def summarize(runs: list[dict]) -> list[dict]:
    values: dict[str, dict[str, list[float]]] = {"ray": {}, "chimaera": {}}
    info: dict[str, dict] = {}
    order: dict[str, list[str]] = {"ray": [], "chimaera": []}
    for run in runs:
        rt = run["runtime"]
        for row in run["rows"]:
            name = row["name"]
            if name not in order[rt]:
                order[rt].append(name)
            if "value" in row:
                values[rt].setdefault(name, []).append(row["value"])
            info.setdefault(name, {}).update({k: row[k] for k in ("unit", "note", "unsupported") if k in row})
    names = list(order["ray"])
    prev = None
    for name in order["chimaera"]:
        if name not in names:
            names.insert(names.index(prev) + 1 if prev in names else len(names), name)
        prev = name
    table = []
    for name in names:
        r, c = values["ray"].get(name), values["chimaera"].get(name)
        row = {"name": name, "unit": info[name].get("unit", "ops/s"), "ray": r, "chimaera": c}
        row["ray_median"] = statistics.median(r) if r else None
        row["chimaera_median"] = statistics.median(c) if c else None
        if row["ray_median"] and row["chimaera_median"]:
            row["ratio"] = row["chimaera_median"] / row["ray_median"]
        row["note"] = info[name].get("unsupported") and f"unsupported: {info[name]['unsupported']}" or info[name].get("note", "")
        table.append(row)
    return table


def write_markdown(path: str, date: str, machine: dict, args: argparse.Namespace, runs: list[dict], table: list[dict]) -> None:
    ok = {rt: sum(1 for r in runs if r["runtime"] == rt and r["rows"]) for rt in ("ray", "chimaera")}
    lines = [
        f"# Ray microbenchmarks vs Chimaera ({date})",
        "",
        "Local measurements on a shared laptop: other sessions were building and testing Chimaera on the same "
        "machine during the runs (see the load averages). Not a cluster claim.",
        "",
        f"- Machine: {machine['cpu']}, hw.ncpu={machine['hw.ncpu']}, {machine['memory_gib']} GiB, {machine['os']}",
        f"- Ray {machine['ray']} on Python {machine['python_ray']}; Chimaera {machine['chimaera_git_head']} "
        f"(working tree with {machine['chimaera_tree_dirty_files']} changed files) on Python {machine['python_chimaera']}"
        + (f"; peer binary sha256 {machine['chimaera_bin_sha256']}" if "chimaera_bin_sha256" in machine else ""),
        f"- Method: Ray's `ray_microbenchmark_helpers.timeit` (1 s warmup, mean of 4 windows of 2 s) for both; "
        f"pause before each benchmark {args.sleep:g} s (Ray's default is 10 s); each run is a fresh process, "
        f"alternating which runtime goes first; completed runs: Ray {ok['ray']}, Chimaera {ok['chimaera']} (failed "
        "runs are listed below and excluded); cells show the median of the per-run means, with the "
        "min–max across rounds in parentheses.",
        "- Ratio = Chimaera / Ray; every metric is a rate, so above 1 means Chimaera is faster.",
        "- Disk: the local peer never deletes superseded raft snapshots (one full state-machine copy every 50k "
        "log entries), so the harness deletes all but the newest during Chimaera runs to protect the shared disk. "
        "Building the snapshots still happens and is measured. Ray runs with RAY_local_fs_capacity_threshold=0.99 "
        "because the disk was over Ray's default 95% spill cutoff.",
        "",
        "| metric | Ray 2.60 | Chimaera | ratio | note |",
        "|---|---:|---:|---:|---|",
    ]
    def cell(values: list[float] | None, median: float | None, unit: str) -> str:
        if median is None:
            return "-"
        if len(values) < 2:
            return fmt(median, unit)
        return f"{fmt(median, unit)} ({fmt(min(values), unit)}–{fmt(max(values), unit)})"

    for row in table:
        r = cell(row["ray"], row["ray_median"], row["unit"])
        c = cell(row["chimaera"], row["chimaera_median"], row["unit"])
        ratio = f"{row['ratio']:.2f}x" if "ratio" in row else "-"
        lines.append(f"| {row['name']} | {r} | {c} | {ratio} | {row['note']} |")
    lines += [
        "", "## Runs", "",
        "| runtime | round | load avg before (1/5/15 min) | after | seconds | exit | snapshots pruned |",
        "|---|---:|---|---|---:|---|---:|",
    ]
    for run in runs:
        reason = run.get("aborted") or run.get("error")
        exit_note = f"{run['returncode']} ({reason})" if reason else run["returncode"]
        pruned = f"{run.get('snapshot_bytes_pruned', 0) / 2**30:.1f} GiB" if run["runtime"] == "chimaera" else "-"
        lines.append(f"| {run['runtime']} | {run['round']} | {run['load_before']} | {run['load_after']} | "
                     f"{run['seconds']} | {exit_note} | {pruned} |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--sleep", type=float, default=10.0, help="pause before each benchmark (Ray's timeit uses 10 s)")
    p.add_argument("--timeout", type=float, default=3600.0, help="seconds before one suite run is killed")
    p.add_argument("--chimaera-bin", help="peer binary to use (CHIMAERA_BIN)")
    p.add_argument("--chimaera-path", help="directory holding the chimaera package to import (prepended to PYTHONPATH)")
    p.add_argument("--workdir", help="where logs and peer homes go (default: a new temporary directory)")
    p.add_argument("--only", choices=("ray", "chimaera"), help="run one runtime only")
    p.add_argument("--min-free-gb", type=float, default=3.0, help="kill a run when the disk has less free space")
    p.add_argument("--append", action="store_true", help="add these runs to today's results file instead of replacing it")
    args = p.parse_args()
    args.workdir = args.workdir or tempfile.mkdtemp(prefix="microbench-")
    os.makedirs(args.workdir, exist_ok=True)
    date = datetime.date.today().isoformat()
    machine = machine_info(args)
    print(f"logs in {args.workdir}; ray processes before start: {sh('pgrep', '-fl', 'raylet|gcs_server') or 'none'}", flush=True)
    runs = []
    results_path = os.path.join(HERE, f"results-{date}.json")
    if args.append and os.path.exists(results_path):
        with open(results_path) as f:
            runs = json.load(f)["runs"]
    first = 1 + max((r["round"] for r in runs), default=-1)
    for i in range(first, first + args.rounds):
        order = ["ray", "chimaera"] if i % 2 == 0 else ["chimaera", "ray"]
        for runtime in order:
            if args.only in (None, runtime):
                runs.append(run_suite(runtime, args, i))
                # Written after every run, so a later failure keeps what finished.
                table = summarize(runs)
                with open(results_path, "w") as f:
                    json.dump({"date": date, "machine": machine, "args": vars(args), "runs": runs, "table": table}, f, indent=1)
                write_markdown(os.path.join(HERE, "RESULTS.md"), date, machine, args, runs, table)
    leftovers = sh("pgrep", "-fl", "raylet|gcs_server")
    print(f"wrote results-{date}.json and RESULTS.md; ray processes left: {leftovers or 'none'}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
