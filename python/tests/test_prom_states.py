"""The Prometheus endpoint counts calls and actors by state, and the backlog's age."""

import socket
import subprocess
import tempfile

from conftest import BIN, run_script

SCRIPT = """
import time, urllib.request
import chimaera as ch

def scrape():
    text = urllib.request.urlopen("http://ADDR/metrics", timeout=10).read().decode()
    return {
        line.rsplit(" ", 1)[0]: float(line.rsplit(" ", 1)[1])
        for line in text.splitlines()
        if line.startswith(("chimaera_tasks{", "chimaera_actors{"))
    }

@ch.remote
class Holder:
    def ping(self):
        return 1

@ch.remote
def hold(seconds):
    time.sleep(seconds)

actors = [Holder.remote() for _ in range(2)]
assert [a.ping.remote().result(timeout=60) for a in actors] == [1, 1]
held = hold.remote(30)
deadline = time.monotonic() + 30
m = scrape()
while m.get('chimaera_tasks{state="running"}', 0) < 1 and time.monotonic() < deadline:
    time.sleep(0.1)
    m = scrape()
for name in sorted(m):
    print(name, int(m[name]))
held.cancel()
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_the_metrics_endpoint_counts_calls_and_actors_by_state():
    home = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    addr = f"127.0.0.1:{_free_port()}"
    try:
        r = run_script(SCRIPT.replace("ADDR", addr), home=home, env={"CHIMAERA_METRICS_LISTEN": addr})
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert r.returncode == 0, r.stderr[-2000:]
    lines = set(r.stdout.splitlines())
    assert 'chimaera_tasks{state="running"} 1' in lines, r.stdout
    assert 'chimaera_actors{state="alive"} 2' in lines, r.stdout
    states = {line.split("{", 1)[1].split("}", 1)[0] for line in lines if line.startswith("chimaera_actors{")}
    assert states == {f'state="{s}"' for s in ("pending", "starting", "alive", "moving", "restarting", "dead")}
    assert {line.split("}", 1)[0] + "}" for line in lines if line.startswith("chimaera_tasks{")} == {
        'chimaera_tasks{state="queued"}',
        'chimaera_tasks{state="fetching_inputs"}',
        'chimaera_tasks{state="running"}',
    }


BACKLOG = """
import time, urllib.request
import chimaera as ch

def oldest():
    text = urllib.request.urlopen("http://ADDR/metrics", timeout=10).read().decode()
    return float(next(l.split()[1] for l in text.splitlines() if l.startswith("chimaera_queue_oldest_seconds ")))

@ch.remote
def hold(seconds):
    time.sleep(seconds)

cap = ch.runs.cluster().nodes()[0].cpus  # starts the peer
print("IDLE", oldest())
everything = hold.options(cpus=cap).remote(60)  # every CPU: the next call waits
time.sleep(0.5)
waiting = hold.options(cpus=1).remote(0)
time.sleep(1.5)
print("WAITING", oldest())
everything.cancel(force=True)
waiting.result(timeout=60)
deadline = time.monotonic() + 30
while oldest() > 0 and time.monotonic() < deadline:
    time.sleep(0.1)
print("DRAINED", oldest())
"""


def test_the_backlog_age_is_the_longest_wait_in_the_queue():
    home = tempfile.mkdtemp(prefix="chm", dir="/tmp")
    addr = f"127.0.0.1:{_free_port()}"
    try:
        r = run_script(BACKLOG.replace("ADDR", addr), home=home, env={"CHIMAERA_METRICS_LISTEN": addr})
    finally:
        subprocess.run([BIN, "--dir", home, "stop"], capture_output=True, timeout=30)
    assert r.returncode == 0, r.stderr[-2000:]
    got = dict(line.split() for line in r.stdout.splitlines() if line.split()[:1] in (["IDLE"], ["WAITING"], ["DRAINED"]))
    assert float(got["IDLE"]) == 0 and float(got["DRAINED"]) == 0, got
    assert 1.0 <= float(got["WAITING"]) < 30, got
