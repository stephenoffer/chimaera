"""Run the gotcha traps for Ray and Chimaera and write results.json and report.md.

    python run.py --out results/ [--only big_arg,io_sleep] [--reps 3] [--allow-host-memory]

Every run is a fresh process (and, for Chimaera, a fresh peer directory unless
the trap says otherwise), preceded by one warmup. While a run goes, host (or
container) memory is sampled every 100 ms; inside a container, kernel OOM kills
are counted from the cgroup. Interpreters: RAY_PYTHON and CHIMAERA_PYTHON
(default: this one). Results are local measurements of this machine only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import textwrap
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from traps import MEMORY_TRAPS, TRAP_TIMEOUT, TRAPS  # noqa: E402

CGROUP = "/sys/fs/cgroup"
# Two runs on one peer directory: the second starts from what the first learned.
SPECIAL = ["mem_heavy_again", "edited_module"]


def in_container() -> bool:
    return os.path.exists("/.dockerenv") or os.environ.get("GOTCHA_IN_CONTAINER") == "1"


def used_bytes() -> int:
    """Memory in use by everything here: the container's cgroup, else the host."""
    try:
        with open(f"{CGROUP}/memory.current") as f:
            return int(f.read())
    except (OSError, ValueError):
        pass
    if sys.platform == "linux":
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                info[k] = int(v.split()[0]) * 1024
        return info["MemTotal"] - info["MemAvailable"]
    out = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=10, check=True).stdout
    page = int(out.split("page size of ")[1].split()[0])
    pages = {}
    for line in out.splitlines()[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            pages[k.strip()] = int(v.strip().rstrip("."))
    occupied = pages.get("Pages active", 0) + pages.get("Pages wired down", 0)
    occupied += pages.get("Pages occupied by compressor", 0)
    return occupied * page


def oom_kills() -> int | None:
    """Kernel OOM kills in this cgroup so far; None where they cannot be read
    (macOS, or no cgroup v2), so a report never shows 0 for "not measured"."""
    try:
        with open(f"{CGROUP}/memory.events") as f:
            return next(int(line.split()[1]) for line in f if line.startswith("oom_kill "))
    except (OSError, StopIteration, ValueError):
        return None


class Sampler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.base = used_bytes()
        self.peak = self.base
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.wait(0.1):
            self.peak = max(self.peak, used_bytes())


def interpreter(runtime: str) -> str:
    return os.environ.get(f"{runtime.upper()}_PYTHON") or sys.executable


def stop_peer(home: str) -> None:
    """Stop the trap's peer before its directory is deleted: a peer still
    running keeps the deleted object files open, so their space stays used."""
    exe = os.environ.get("CHIMAERA_BIN") or shutil.which("chimaera")
    if exe and os.path.isdir(home):
        stopped = subprocess.run([exe, "--dir", home, "stop"], capture_output=True, timeout=60)
        if stopped.returncode == 0:
            return
    try:  # no binary, or it failed: signal the peer that wrote this directory's pid file
        with open(os.path.join(home, "peer.pid")) as f:
            pid = int(f.read().strip())
        cmd = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True).stdout
        if home in cmd:  # still that peer, not a process that reused its pid
            os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError):
        pass


def run_script(runtime: str, argv: list[str], home: str, timeout: float) -> dict:
    env = dict(os.environ, CHIMAERA_HOME=home)
    sampler = Sampler()
    kills = oom_kills()
    sampler.start()
    t0 = time.time()
    try:
        p = subprocess.run([interpreter(runtime), *argv], capture_output=True, text=True, timeout=timeout, env=env)
        out, err, code = p.stdout, p.stderr, p.returncode
    except subprocess.TimeoutExpired:
        out, err, code = "", f"timed out after {timeout:.0f} s", -1
    sampler.stop.set()
    sampler.join()
    lines = [ln for ln in out.splitlines() if ln.startswith("{")]
    rec = json.loads(lines[-1]) if lines else {"error": (err or out)[-1500:], "exit": code}
    rec.update(
        process_s=round(time.time() - t0, 2),
        peak_extra_mib=round((sampler.peak - sampler.base) / 2**20),
        oom_kills=None if kills is None or (now := oom_kills()) is None else now - kills,
    )
    return rec


def run_trap(runtime: str, trap: str, timeout: float) -> dict:
    home = tempfile.mkdtemp(prefix="chg", dir="/tmp")
    try:
        if trap == "mem_heavy_again":
            first = run_script(runtime, [os.path.join(HERE, "traps.py"), runtime, "mem_heavy"], home, timeout)
            rec = run_script(runtime, [os.path.join(HERE, "traps.py"), runtime, "mem_heavy"], home, timeout)
            rec["trap"], rec["first_run"] = trap, first
            return rec
        if trap == "edited_module":
            return edited_module(runtime, home, timeout)
        limit = min(timeout, TRAP_TIMEOUT.get(trap, timeout))
        return run_script(runtime, [os.path.join(HERE, "traps.py"), runtime, trap], home, limit)
    finally:
        stop_peer(home)
        shutil.rmtree(home, ignore_errors=True)


def edited_module(runtime: str, home: str, timeout: float) -> dict:
    """Run, edit an imported module, run again within seconds: the second run
    must use the edited code (a warm worker must not keep the old import)."""
    work = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    script = os.path.join(work, "main.py")
    with open(script, "w") as f:
        f.write(
            textwrap.dedent(
                f"""
                import json, sys
                sys.path.insert(0, {HERE!r})
                import mod
                from traps import ChimaeraShim, RayShim
                rt = RayShim() if sys.argv[1] == "ray" else ChimaeraShim()
                f = rt.remote(mod.version)
                got = [v for v, e in rt.get_each([f.remote() for _ in range(8)])]
                print(json.dumps({{"versions": got}}))
                """
            )
        )

    def write_mod(v: int) -> None:
        with open(os.path.join(work, "mod.py"), "w") as f:
            f.write(f"def version():\n    return {v}\n")

    try:
        write_mod(1)
        first = run_script(runtime, [script, runtime], home, timeout)
        # A different size: Python's bytecode cache trusts (mtime seconds,
        # size), so a same-size edit within the same second is not seen even by
        # a fresh process. Real edits are seconds apart.
        write_mod(22)
        second = run_script(runtime, [script, runtime], home, timeout)
        stale = [v for v in second.get("versions", []) if v != 22]
        return {
            "trap": "edited_module",
            "runtime": runtime,
            "calls": len(second.get("versions", [])),
            "ok": len(second.get("versions", [])) - len(stale),
            "wrong": len(stale),
            "errors": {} if "versions" in second else {"run failed": 1},
            "wall_s": second.get("process_s"),
            "first_run": first,
        }
    finally:
        shutil.rmtree(work, ignore_errors=True)


def median(records: list[dict], key: str):
    xs = [r[key] for r in records if isinstance(r.get(key), (int, float))]
    return round(statistics.median(xs), 3) if xs else None


def _exit_on_signal(signum: int, _frame: object) -> None:
    raise SystemExit(128 + signum)  # unwinds through run_trap's cleanup


def main() -> None:
    signal.signal(signal.SIGTERM, _exit_on_signal)
    signal.signal(signal.SIGHUP, _exit_on_signal)
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="gotcha-results")
    ap.add_argument("--only", default="")
    ap.add_argument("--runtimes", default="ray,chimaera")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--allow-host-memory", action="store_true", help="run memory traps outside a container")
    a = ap.parse_args()
    names = [t for t in [*TRAPS, *SPECIAL] if not a.only or t in a.only.split(",")]
    memory = MEMORY_TRAPS | {"mem_heavy_again"}
    if not in_container() and not a.allow_host_memory:
        skipped = [t for t in names if t in memory]
        names = [t for t in names if t not in memory]
        if skipped:
            print(f"skipping {', '.join(skipped)}: memory traps run only in a memory-limited container", flush=True)
    os.makedirs(a.out, exist_ok=True)
    results = []
    for trap in names:
        for runtime in a.runtimes.split(","):
            run_trap(runtime, trap, a.timeout)  # warmup: imports, caches, first peer start
            reps = [run_trap(runtime, trap, a.timeout) for _ in range(a.reps)]
            summary = {
                "trap": trap,
                "runtime": runtime,
                "wall_s": median(reps, "wall_s"),
                "peak_extra_mib": median(reps, "peak_extra_mib"),
                "oom_kills": None if any(r.get("oom_kills") is None for r in reps) else sum(r["oom_kills"] for r in reps),
                "ok": f"{median(reps, 'ok')}/{median(reps, 'calls')}",
                "errors": [r.get("errors") or r.get("error") for r in reps if r.get("errors") or r.get("error")],
                "reps": reps,
            }
            results.append(summary)
            print(json.dumps({k: v for k, v in summary.items() if k != "reps"}), flush=True)
    meta = {
        "host": f"{platform.system()} {platform.machine()}",
        "cores": os.cpu_count(),
        "container": in_container(),
        "memory_limit": os.environ.get("GOTCHA_MEMORY_LIMIT"),
        "reps": a.reps,
    }
    with open(os.path.join(a.out, "results.json"), "w") as f:
        json.dump({"meta": meta, "results": results}, f, indent=2)
    with open(os.path.join(a.out, "report.md"), "w") as f:
        f.write(f"# Gotchas: naive code on Ray and Chimaera\n\n{meta}\n\n")
        f.write("| Trap | Runtime | Wall s (median) | Peak extra MiB | OOM kills | OK | Errors |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in results:
            errs = json.dumps(r["errors"])[:160].replace("|", "/")
            f.write(
                f"| {r['trap']} | {r['runtime']} | {r['wall_s']} | {r['peak_extra_mib']} | {r['oom_kills']} | "
                f"{r['ok']} | {errs} |\n"
            )
    print(f"wrote {a.out}/results.json and report.md")


if __name__ == "__main__":
    main()
