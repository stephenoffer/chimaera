"""A lost cell restored from a backup of its Raft snapshots: `chimaera cluster
backup` on a stopped peer, then every peer lost, then `chimaera cluster
restore` into a new directory at a new address. The run, its results and the
store come back; a call the lost peers were running is retried."""

import os
import shutil
import signal
import subprocess
import tempfile
import time

from conftest import BIN, run_script
from chimaera._native import socket_path
from test_cluster import Cluster, free_ports, lines


def cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([BIN, *args], capture_output=True, text=True, timeout=120)


def test_a_cell_lost_whole_is_restored_from_a_backup_of_one_peer():
    c = Cluster(3, labels={i: f"slot=s{i}" for i in range(3)})
    scratch = tempfile.mkdtemp(prefix="chb", dir="/tmp")
    out, new, go = os.path.join(scratch, "backup"), os.path.join(scratch, "new"), os.path.join(scratch, "go")
    restored = None
    try:
        made = c.run(
            0,
            f"""
import os, time
import chimaera as ch

@ch.remote
def square(i):
    return i * i

@ch.remote(placement=ch.dist.Placement(require={{"slot": "s1"}}))
def waits():
    while not os.path.exists({go!r}):
        time.sleep(0.05)
    return "went"

run = ch.init(detach=True)
ch.coord.Store("restore-test").put("k", "kept")
print("SUM", sum(ch.get([square.remote(i) for i in range(20)], timeout=120)))
w = waits.remote()
while ch.runs.current_run().status().get("Running", 0) < 1:
    time.sleep(0.05)
print("RUN", run)
print("WAITS", w.key.hex())
""",
        )
        assert lines(made, "SUM") == [str(sum(i * i for i in range(20)))], made.stderr[-2000:]
        run_id, waits = lines(made, "RUN")[0], lines(made, "WAITS")[0]

        # Back up peer 0 (it votes in every group of three peers), stopped.
        stopped = cli("--dir", c.dir(0), "stop")
        assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        c.procs[0].wait(timeout=60)  # the stop is asked for; the peer exits after its drain
        backup = cli("--dir", c.dir(0), "cluster", "backup", "--out", out)
        assert backup.returncode == 0, backup.stdout + backup.stderr
        assert os.path.exists(os.path.join(out, "backup.json"))
        # Then the whole cell is lost.
        for i in (1, 2):
            c.signal(i, signal.SIGKILL)

        port = free_ports(1)[0]
        addr = f"127.0.0.1:{port}"
        refused = cli("--dir", new, "cluster", "restore", "--from", out, "--advertise", addr)
        assert refused.returncode != 0 and "--force-new-cell" in refused.stdout + refused.stderr
        r = cli("--dir", new, "cluster", "restore", "--from", out, "--advertise", addr, "--force-new-cell")
        assert r.returncode == 0, r.stdout + r.stderr
        assert "--data-groups 4" in r.stdout, r.stdout
        log = open(os.path.join(scratch, "new.log"), "ab")
        env = dict(os.environ, CHIMAERA_LABELS="slot=s1")
        start = ["--dir", new, "peer", "start", "--listen", addr, "--seeds", addr, "--cluster", "test"]
        start += ["--data-groups", "4", "--token-file", c.token, "--cpus", "4"]
        restored = subprocess.Popen([BIN, *start], stdout=log, stderr=log, start_new_session=True, env=env)
        deadline = time.monotonic() + 60
        while not os.path.exists(socket_path(new, "peer.sock")):
            assert restored.poll() is None and time.monotonic() < deadline, open(log.name).read()[-3000:]
            time.sleep(0.05)

        open(go, "w").close()
        back = run_script(
            f"""
import chimaera as ch
run = ch.runs.attach({run_id!r})
print("STORE", ch.coord.Store("restore-test").get("k"))
run.wait(timeout=180)
done = {{k: v for k, v, e in run.outcomes() if e is None}}
print("SQUARES", sorted(v for v in done.values() if isinstance(v, int)) == [i * i for i in range(20)])
print("WAITED", [v for k, v in done.items() if {waits!r}.endswith(k)])
print("WAITS_OUTCOME", [(v, repr(e)[:300]) for k, v, e in run.outcomes() if {waits!r}.endswith(k)])

@ch.remote
def fresh():
    return "new work"
print("FRESH", fresh.remote().result(timeout=120))
""",
            timeout=240,
            home=new,
        )
        assert lines(back, "STORE") == ["kept"], back.stdout + back.stderr[-3000:]
        assert lines(back, "SQUARES") == ["True"]
        assert lines(back, "WAITED") == ["['went']"], back.stdout[-3000:]
        assert lines(back, "FRESH") == ["new work"]
    finally:
        if restored is not None:
            cli("--dir", new, "stop")
            restored.wait(timeout=30)
        c.close()
        shutil.rmtree(scratch, ignore_errors=True)
