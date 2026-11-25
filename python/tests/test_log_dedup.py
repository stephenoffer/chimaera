"""A line several workers print reaches the driver once, then as a count; `chimaera logs` keeps every copy."""

import subprocess
import tempfile

from conftest import BIN, run_script

SCRIPT = """
import os, time
import chimaera as ch

store = ch.coord.Store("dedup-" + os.urandom(4).hex())

@ch.remote
def work(i):
    store.barrier("all", 4).wait(timeout=60)  # four calls at once: four worker processes
    print("warning: the cache is cold", flush=True)
    print(f"worker {i} pid {os.getpid()}", flush=True)
    return os.getpid()

pids = ch.get([work.remote(i) for i in range(4)], timeout=120)
print("PIDS", len(set(pids)), flush=True)
time.sleep(6.5)  # the 5 s window closes, and its count is printed
print("RUN", ch.runs.current_run().id, flush=True)
"""


def _run(env: dict[str, str]) -> tuple[list[str], str, str]:
    home = tempfile.mkdtemp(prefix="chd", dir="/tmp")
    try:
        r = run_script(SCRIPT, home=home, env=env)
        assert r.returncode == 0, r.stderr[-2000:]
        run = r.stdout.split("RUN ", 1)[1].split()[0]
        logs = subprocess.run([BIN, "--dir", home, "logs", run], capture_output=True, text=True, timeout=60).stdout
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    return r.stdout.splitlines(), r.stdout.split("PIDS ", 1)[1].split()[0], logs


def test_a_line_from_several_workers_is_printed_once_then_counted():
    lines, pids, logs = _run({})
    assert pids == "4"
    plain = [line for line in lines if line.endswith("warning: the cache is cold")]
    counted = [line for line in lines if line.endswith("warning: the cache is cold [repeated 3x across workers]")]
    assert len(plain) == 1 and len(counted) == 1, lines
    assert sum(" pid " in line and "worker " in line for line in lines) == 4, "distinct lines all print"
    assert logs.count("warning: the cache is cold") == 4, logs  # the run's log keeps every copy


def test_dedup_off_prints_every_copy():
    lines, pids, _ = _run({"CHIMAERA_DEDUP_LOGS": "0"})
    assert pids == "4"
    assert sum(line.endswith("warning: the cache is cold") for line in lines) == 4, lines
    assert not any("repeated" in line for line in lines)
