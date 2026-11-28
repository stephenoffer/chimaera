"""Multi-node head-to-head on an Anyscale (Ray) cluster: Ray vs Chimaera, and
Dask when asked, on the same machines, one after the other.

Ray is the launcher here too: one task pinned to each node installs the
Chimaera wheel and starts a peer, all peers founding one cluster. The driver
then runs each workload against Chimaera (through the head node's peer) and
against Ray (through the node's Ray cluster), each in a fresh process.
EVAL_RUNTIMES picks the runtimes (default "ray,chimaera"); with "dask" in it,
Ray also starts a Dask scheduler on the head node and a Dask worker on every
node (launch_dask), and the workloads reach it through DASK_SCHEDULER.

Runs are paired: every runtime stays up, and each trial runs every workload
once per runtime, the workloads and the runtimes' order in a seeded
random order (EVAL_SEED, else a random seed; the manifest records it).
EVAL_TRIALS repeats the trial (the promotion gate asks for 7, with a smaller
EVAL_MIN_SECONDS to fit the job). node_loss runs last, once per runtime.
multi_results.json holds the run's manifest (bench/evals/manifest.py) and
every run's raw samples; multi_report.md the medians and their intervals.

    python bench/evals/multi_node.py --dist /tmp/chimaera-dist --out results/
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import time

import cost
import manifest

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = 7100
DASK_PORT = 8786
PEER_DIR = "/tmp/chimaera-multi"
# EVAL_SCALE < 1 shrinks every workload, for a smoke run on a small machine.
SCALE = float(os.environ.get("EVAL_SCALE", "1"))


def n(x: int) -> int:
    return max(1, int(x * SCALE))

# Each workload is run by `multi_node.py --workload NAME --runtime ray|chimaera|dask`.
WORKLOADS = [
    "tasks", "tasks_serial", "cpu_scaling", "map_small", "broadcast", "naive_big_arg", "follow_input", "actors",
    "actors_peer", "nested", "shuffle", "shuffle_unfused", "node_loss",
]
# The exact unit of work behind each row's number, its sizes at EVAL_SCALE 1
# (the reports state the scale they ran at).
UNITS = {
    "tasks": "calls/s: one call of a no-op function, made in one burst with the others, its result read",
    "tasks_serial": "calls/s with one call in flight: one call of a no-op function made alone, its result read "
    "before the next call",
    "cpu_scaling": "busy CPU-seconds / (CPUs x wall seconds) for 4 x CPUs calls of 0.5 s of CPU",
    "map_small": "items/s: one int doubled (Chimaera: a 4M-item map; Ray naive: 200k items, one task each; "
    "Ray hand_batched: 4M items, 1,000 per task; Dask: client.map over 200k items, one task each)",
    "broadcast": "seconds: 2 x CPUs calls each reading one 512 MiB put value",
    "naive_big_arg": "seconds: 2 x CPUs calls each passed the same 32 MiB array",
    "follow_input": "seconds: 10 calls on a 256 MiB value just made",
    "actors": "calls/s: one method call (an increment) on max(8, CPUs / 2) actors, results read",
    "actors_peer": "calls/s: as actors, with direct actor calls off (CHIMAERA_DIRECT=0)",
    "nested": "seconds: 4 x CPUs calls each making and reading 8 child calls",
    "shuffle": "seconds: 3.2M (key, 1) pairs summed into 64 partitions, all in the driver; "
    "the map runs inside the split (Ray: by hand, Dask: by hand with dask.delayed(nout=64), "
    "Chimaera: Results.reduce_by_key)",
    "shuffle_unfused": "seconds: as shuffle, with the map's outputs stored first and read by separate split calls",
    "node_loss": "seconds: 4 x CPUs one-second calls all returned while one worker node's runtime is SIGKILLed",
}
# Regression controls for Chimaera alone, and the environment their child runs in.
CHIMAERA_ONLY = {"actors_peer"}
CONTROL_ENV = {"actors_peer": {"CHIMAERA_DIRECT": "0"}}


# ---- launching Chimaera peers on every node (runs under Ray) -------------------


def launch_peers(dist: str, peer_env: dict[str, str] | None = None) -> tuple[list[str], str, list[str], int, dict]:
    """Start one Chimaera peer per alive Ray node; return (addresses, head
    address, each machine's description, data groups, the machines' instance
    types and region, which bench/evals/cost.py prices).

    Each peer runs under a detached actor that stays alive with it: Ray reaps
    the child processes of a finished task's worker, so a peer started from a
    plain task would die when the launching driver disconnects."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    nodes = [n for n in ray.nodes() if n["Alive"]]
    ips = sorted({n["NodeManagerAddress"] for n in nodes})
    seeds = ",".join(f"{ip}:{PORT}" for ip in ips)
    files = {name: open(os.path.join(dist, name), "rb").read() for name in os.listdir(dist)}
    head_ip = ray.util.get_node_ip_address()

    @ray.remote(num_cpus=0)
    class PeerHost:
        def __init__(self, files: dict[str, bytes], seeds: str, groups: int, peer_env: dict[str, str]) -> None:
            import glob
            import platform
            import socket

            here = "/tmp/chimaera-dist"
            os.makedirs(here, exist_ok=True)
            for name, data in files.items():
                with open(os.path.join(here, name), "wb") as f:
                    f.write(data)
            os.chmod(os.path.join(here, "chimaera"), 0o755)
            wheel = glob.glob(os.path.join(here, "*.whl"))[0]
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--force-reinstall", wheel], check=True)
            ip = ray.util.get_node_ip_address()
            cpus = os.cpu_count() or 1
            env = dict(os.environ, CHIMAERA_PYTHON=sys.executable, **peer_env)
            log = open("/tmp/chimaera-peer.log", "ab")
            self.proc = subprocess.Popen(
                [os.path.join(here, "chimaera"), "--dir", PEER_DIR, "peer", "start", "--listen", f"{ip}:{PORT}",
                 "--seeds", seeds, "--cluster", "eval", "--data-groups", str(groups), "--cpus", str(cpus),
                 "--token-file", os.path.join(here, "token")],
                stdout=log, stderr=log, start_new_session=True, env=env,
            )
            self.bin = os.path.join(here, "chimaera")
            self.desc = f"{ip} ({socket.gethostname()}, {cpus} cpus, {platform.platform()})"

        def describe(self) -> str:
            return self.desc

        def instance(self) -> tuple[str, str]:
            """This machine's EC2 instance type and region (IMDSv2), or ("", "") off EC2."""
            import urllib.request

            url = "http://169.254.169.254/latest/"
            try:
                ttl = {"X-aws-ec2-metadata-token-ttl-seconds": "60"}
                token = urllib.request.urlopen(urllib.request.Request(url + "api/token", method="PUT", headers=ttl),
                                               timeout=2).read().decode()
                ask = [urllib.request.Request(url + f"meta-data/{k}", headers={"X-aws-ec2-metadata-token": token})
                       for k in ("instance-type", "placement/region")]
                kind, region = (urllib.request.urlopen(r, timeout=2).read().decode() for r in ask)
                return kind, region
            except OSError:
                return "", ""

        def alive(self) -> bool:
            return self.proc.poll() is None

        def status(self) -> dict:
            """The peer's `chimaera status --json`, or {} when it does not answer."""
            p = subprocess.run([self.bin, "--dir", PEER_DIR, "status", "--json"], capture_output=True, text=True,
                               timeout=30, check=False)
            return json.loads(p.stdout) if p.returncode == 0 else {}

        def log_tail(self) -> str:
            return open("/tmp/chimaera-peer.log", errors="replace").read()[-3000:]

        def settings(self) -> str:
            """What the peer chose for itself (raft sync and why)."""
            with open("/tmp/chimaera-peer.log", errors="replace") as f:
                found = [ln for ln in f if "raft sync" in ln]
            return found[-1].strip()[-120:] if found else "(not logged yet)"

        def cpu_seconds(self) -> float:
            """CPU time the peer process has used (user + system)."""
            try:
                with open(f"/proc/{self.proc.pid}/stat") as f:
                    fields = f.read().rsplit(")", 1)[1].split()
                return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
            except OSError:  # no /proc (macOS smoke runs)
                t = subprocess.run(["ps", "-o", "time=", "-p", str(self.proc.pid)], capture_output=True, text=True, check=False).stdout
                parts = [float(x) for x in t.strip().replace("-", ":").split(":")] or [0.0]
                return sum(v * 60 ** i for i, v in enumerate(reversed(parts)))

        def stop(self, wipe: bool = False) -> None:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            if wipe:  # the next launch founds a new cluster
                import glob
                import shutil

                shutil.rmtree(PEER_DIR, ignore_errors=True)
                # On Linux the objects live in /dev/shm under the peer's id,
                # outside PEER_DIR; left there, the next pass's peers would
                # find less shared memory and fall back to disk.
                for d in glob.glob("/dev/shm/chimaera-*"):
                    shutil.rmtree(d, ignore_errors=True)

    groups = max(8, 2 * len(ips))
    hosts = [
        PeerHost.options(
            name=f"chimaera-peer-{n['NodeID']}", lifetime="detached", get_if_exists=True,
            scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False),
        ).remote(files, seeds, groups, peer_env or {})
        for n in nodes
    ]
    machines = ray.get([h.describe.remote() for h in hosts])
    print("peers:", machines, flush=True)
    found = ray.get([h.instance.remote() for h in hosts])
    priced = {"instances": dict(collections.Counter(t for t, _ in found if t)),
              "region": ",".join(sorted({r for _, r in found if r}))}
    ray.shutdown()
    return [f"{ip}:{PORT}" for ip in ips], head_ip, machines, groups, priced


def peer_hosts():
    import ray

    return [ray.get_actor(f"chimaera-peer-{n['NodeID']}") for n in ray.nodes() if n["Alive"]]


def peer_logs() -> str:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    out = []
    for ref in [h.log_tail.remote() for h in peer_hosts()]:  # asked all at once
        try:
            out.append(ray.get(ref, timeout=30))
        except Exception as e:  # noqa: BLE001
            out.append(f"(no log: {e})")
    ray.shutdown()
    return "\n----\n".join(out)


def peer_settings() -> list[str]:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    try:
        return ray.get([h.settings.remote() for h in peer_hosts()], timeout=30)
    except Exception as e:  # noqa: BLE001
        return [f"(unavailable: {e})"]
    finally:
        ray.shutdown()


def peer_status() -> list[dict]:
    """Every peer's status: the durability class it reports and the raft
    entries its groups applied."""
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    try:
        return ray.get([h.status.remote() for h in peer_hosts()], timeout=60)
    except Exception:  # noqa: BLE001
        return []
    finally:
        ray.shutdown()


def peer_cpu() -> list[float]:
    """CPU seconds used so far by each peer process."""
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    try:
        return ray.get([h.cpu_seconds.remote() for h in peer_hosts()], timeout=30)
    except Exception:  # noqa: BLE001
        return []
    finally:
        ray.shutdown()


def stop_peers(wipe: bool = False) -> None:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    names = [f"chimaera-peer-{n['NodeID']}" for n in ray.nodes() if n["Alive"]]
    hosts = peer_hosts()
    stops = [h.stop.remote(wipe) for h in hosts]  # all peers drain at once
    for h, ref in zip(hosts, stops):
        try:
            ray.get(ref, timeout=90)
            ray.kill(h)
        except Exception as e:  # noqa: BLE001
            print("stop:", e)
    # The next launch reuses these names: wait until they are free.
    deadline = time.time() + 60
    for name in names:
        while time.time() < deadline:
            try:
                ray.get_actor(name)
            except ValueError:
                break
            time.sleep(0.5)
    ray.shutdown()


# ---- launching a Dask cluster on every node (runs under Ray) ---------------------


def launch_dask() -> tuple[str, int]:
    """Start (or restart) a Dask scheduler on the head node and one `dask
    worker` per alive Ray node, with one single-threaded worker process per
    CPU: Ray and Chimaera also run one worker process per CPU, and the
    workloads' Python holds the GIL. Return the scheduler's address and the
    worker count, once every worker has joined.

    As with the peers, each node's processes run under a detached actor,
    which first installs the driver's Dask version on its node."""
    import distributed
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    nodes = [n for n in ray.nodes() if n["Alive"]]
    head_ip = ray.util.get_node_ip_address()
    address = f"tcp://{head_ip}:{DASK_PORT}"

    @ray.remote(num_cpus=0)
    class DaskHost:
        def __init__(self, address: str, version: str, scheduler: bool) -> None:
            spec = f"dask[distributed]=={version}"
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", spec], check=True)
            ip = ray.util.get_node_ip_address()
            self.cpus = os.cpu_count() or 1
            dask = [sys.executable, "-m", "dask"]
            self.cmds = []
            if scheduler:
                self.cmds.append(dask + ["scheduler", "--host", ip, "--port", str(DASK_PORT), "--no-dashboard"])
            self.cmds.append(dask + ["worker", address, "--host", ip, "--nworkers", str(self.cpus), "--nthreads", "1",
                                     "--no-dashboard"])
            self.procs: list[subprocess.Popen] = []

        def start(self) -> int:
            log = open("/tmp/dask.log", "ab")
            self.procs = [subprocess.Popen(c, stdout=log, stderr=log, start_new_session=True) for c in self.cmds]
            return self.cpus

        def stop(self) -> None:
            # Each command leads its own process group: the CLI and the worker processes it spawned.
            for p in self.procs:
                for sig, wait in ((signal.SIGTERM, 60), (signal.SIGKILL, 10)):
                    p.poll()  # reap a CLI already killed (node_loss): macOS refuses to signal a zombie's group
                    try:
                        os.killpg(p.pid, sig)
                        p.wait(timeout=wait)
                    except (ProcessLookupError, subprocess.TimeoutExpired):
                        pass
            self.procs = []

    hosts = [
        DaskHost.options(
            name=f"dask-host-{n['NodeID']}", lifetime="detached", get_if_exists=True,
            scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False),
        ).remote(address, distributed.__version__, n["NodeManagerAddress"] == head_ip)
        for n in nodes
    ]
    ray.get([h.stop.remote() for h in hosts])  # a restart: no new worker joins the old scheduler
    workers = sum(ray.get([h.start.remote() for h in hosts]))
    ray.shutdown()
    with distributed.Client(address, timeout=120) as c:
        c.wait_for_workers(workers, timeout=600)
    print("dask:", address, workers, "workers", flush=True)
    return address, workers


def stop_dask() -> None:
    import ray

    ray.init(address="auto", namespace="chimaera-eval", log_to_driver=False)
    hosts = [ray.get_actor(f"dask-host-{n['NodeID']}") for n in ray.nodes() if n["Alive"]]
    stops = [h.stop.remote() for h in hosts]  # all nodes at once
    for h, ref in zip(hosts, stops):
        try:
            ray.get(ref, timeout=90)
            ray.kill(h)
        except Exception as e:  # noqa: BLE001
            print("stop dask:", e)
    ray.shutdown()


# ---- workloads -----------------------------------------------------------------


def total_cpus(rt: str) -> int:
    if rt == "ray":
        import ray

        return int(ray.cluster_resources().get("CPU", 1))
    if rt == "dask":  # one thread per worker process
        from distributed import default_client

        return sum(default_client().nthreads().values())
    import chimaera as ch

    return int(sum(n.cpus for n in ch.runs.cluster().nodes()))


def busy(seconds: float) -> int:
    t = time.process_time()
    n = 0
    while time.process_time() - t < seconds:
        n += 1
    return n


def where() -> str:
    import socket

    return socket.gethostname()


# Each workload repeats its timed section until MIN_SECONDS have passed (at
# least MIN_REPS times) after one uncounted warm-up, and reports the spread.
MIN_SECONDS = float(os.environ.get("EVAL_MIN_SECONDS", "120"))
MIN_REPS = 3
# The headline number of a rep: the first of these keys it has.
METRICS = ("tasks_per_s", "calls_per_s", "naive_items_per_s", "efficiency", "seconds")


def repeat_workload(rt: str, name: str) -> dict:
    once = name == "node_loss"  # takes a node down: once only
    runs = [] if once else [run_workload(rt, name)]  # warm-up: executors start, code and values ship
    reps: list[dict] = []
    t0 = time.perf_counter()
    while not reps or not once and (len(reps) < MIN_REPS or time.perf_counter() - t0 < MIN_SECONDS):
        reps.append(run_workload(rt, name))
    runs += reps
    key = next(k for k in METRICS if k in reps[0])
    xs = sorted(r[key] for r in reps)

    def q(f: float) -> float:
        return xs[min(len(xs) - 1, int(f * len(xs)))]

    out = dict(reps[-1])
    out[key] = xs[len(xs) // 2]
    out.update({"metric": key, "reps": len(xs), "p10": q(0.1), "p90": q(0.9), "samples": [r[key] for r in reps],
                "unit": UNITS[name], "executions": len(runs)})
    # Totals over every run, the warm-up's too: the peers' commit count over
    # the whole child is divided by them.
    for k in ("ops", "direct_calls"):
        if k in out:
            out[k] = sum(r[k] for r in runs)
    digests = collections.Counter(r["digest"] for r in reps if "digest" in r)
    if digests:  # each timed run's output digest, and how many runs saw it
        del out["digest"]
        out["digests"] = dict(digests)
    return out


_INIT = False


def run_workload(rt: str, name: str) -> dict:
    global _INIT
    if rt == "ray":
        import ray

        if not _INIT:
            ray.init(address="auto", log_to_driver=False)
        remote = ray.remote
        get = ray.get
    elif rt == "dask":
        import types

        from distributed import Client, default_client

        client = default_client() if _INIT else Client(os.environ["DASK_SCHEDULER"])

        def remote(f):  # f.remote(*args) is one client.submit; pure=False, so equal calls each run
            return types.SimpleNamespace(remote=lambda *a: client.submit(f, *a, pure=False))

        def get(x):
            return client.gather(x) if isinstance(x, list) else x.result()
    else:
        import chimaera as ch

        if not _INIT:
            ch.init()
        remote = ch.remote

        def get(x, timeout=None):
            return ch.get(x, timeout=timeout) if isinstance(x, list) else x.result(timeout)

    _INIT = True
    cpus = total_cpus(rt)
    t0 = time.perf_counter()

    if name in ("tasks", "tasks_serial"):
        noop = remote(lambda x: x)
        if name == "tasks":  # one burst: Chimaera's client coalesces the calls into few submissions
            k = n(200_000)
            t0 = time.perf_counter()
            out = get([noop.remote(i) for i in range(k)])
        else:  # one call at a time: each is a submission of its own (the per-call path)
            k = n(2_000)
            t0 = time.perf_counter()
            out = [get(noop.remote(i)) for i in range(k)]
        assert out[-1] == k - 1
        dt = time.perf_counter() - t0
        return {"tasks_per_s": round(k / dt), "cpus": cpus, "ops": k}

    if name == "cpu_scaling":
        burn = remote(busy)
        get([remote(where).remote() for _ in range(2 * cpus)])  # warm every worker
        k, each = 4 * cpus, 0.5 * SCALE
        t0 = time.perf_counter()
        get([burn.remote(each) for _ in range(k)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "efficiency": round(k * each / cpus / dt, 3), "cpus": cpus}

    if name == "map_small":
        double = remote(lambda x: 2 * x)
        if rt == "ray":
            k = n(200_000)  # one task per item: the naive Ray program
            t0 = time.perf_counter()
            get([double.remote(i) for i in range(k)])
            naive = k / (time.perf_counter() - t0)
            batch = remote(lambda xs: [2 * x for x in xs])
            n2 = n(4_000_000)
            t0 = time.perf_counter()
            parts = get([batch.remote(list(range(i, min(i + 1000, n2)))) for i in range(0, n2, 1000)])
            hand = n2 / (time.perf_counter() - t0)
            assert sum(len(p) for p in parts) == n2
            return {"naive_items_per_s": round(naive), "hand_batched_items_per_s": round(hand)}
        if rt == "dask":  # Dask's map makes one task per item, as Ray's naive program does
            k = n(200_000)
            t0 = time.perf_counter()
            s = sum(client.gather(client.map(lambda x: 2 * x, range(k), pure=False)))
            dt = time.perf_counter() - t0
            assert s == k * (k - 1)
            return {"naive_items_per_s": round(k / dt), "ops": k}
        k = n(4_000_000)
        t0 = time.perf_counter()
        s = sum(double.map(range(k)))
        dt = time.perf_counter() - t0
        assert s == k * (k - 1)
        return {"naive_items_per_s": round(k / dt), "ops": k}

    if name == "broadcast":
        import numpy as np

        size = n(512 << 20) // 8 * 8
        if rt == "ray":
            import ray

            ref = ray.put(np.ones(size // 8))
        elif rt == "dask":
            # One stored copy, as the put: the calls fetch it where they run. Not
            # broadcast=True: it copies to every worker before the clock starts,
            # and fights the Active Memory Manager's default ReduceReplicas policy.
            # hash=False: a new key, as a put is a new object (the content's key
            # would race the release of the previous run's).
            ref = client.scatter(np.ones(size // 8), hash=False)
        else:
            import chimaera as ch

            ref = ch.put(np.ones(size // 8))
        total = remote(lambda a: (float(a[:10].sum()), where()))
        t0 = time.perf_counter()
        out = get([total.remote(ref) for _ in range(2 * cpus)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "nodes_reached": len({h for _, h in out}), "readers": len(out)}

    if name == "naive_big_arg":
        # The same 32 MiB array passed straight to every call, no put: the
        # naive program (Ray stores a copy per call).
        import numpy as np

        a = np.ones(n(32 << 20) // 8)
        total = remote(lambda a: (float(a[:10].sum()), where()))
        t0 = time.perf_counter()
        out = get([total.remote(a) for _ in range(2 * cpus)])
        dt = time.perf_counter() - t0
        return {"seconds": round(dt, 3), "nodes_reached": len({h for _, h in out}), "calls": len(out)}

    if name == "follow_input":
        # 10 calls on a 256 MiB value the cluster just made, with no placement
        # given: how many ran on one machine (where the value is, when the
        # runtime follows its input), and how long they took.
        import collections

        make = remote(lambda: b"x" * n(256 << 20))
        probe = remote(lambda b: (len(b), where()))
        big = make.remote()
        if rt == "ray":
            import ray

            ray.wait([big], fetch_local=False)
        elif rt == "dask":
            import distributed

            distributed.wait([big])  # done, not fetched
        else:
            import chimaera as ch

            next(ch.as_completed([big]))
        t0 = time.perf_counter()
        out = get([probe.remote(big) for _ in range(10)])
        dt = time.perf_counter() - t0
        most = collections.Counter(h for _, h in out).most_common(1)[0][1]
        return {"seconds": round(dt, 3), "calls_on_one_host": most, "calls": len(out)}

    if name in ("actors", "actors_peer"):
        class Counter:
            def __init__(self):
                self.n = 0

            def inc(self, k):
                self.n += k
                return self.n

            def host(self):
                return where()

        k = max(8, cpus // 2)
        if rt == "dask":  # an actor is a submit's result; a method call returns an ActorFuture
            actors = [client.submit(Counter, actor=True).result() for _ in range(k)]
            hosts = [a.host().result() for a in actors]
        else:
            A = remote(Counter)
            actors = [A.remote() for _ in range(k)]
            hosts = get([a.host.remote() for a in actors])
        calls = n(2000)
        if rt == "chimaera":
            from chimaera import _direct  # counts the calls this process sent straight to an actor

            direct = _direct._counter["direct"]
        t0 = time.perf_counter()
        if rt == "dask":
            futs = [a.inc(1) for a in actors for _ in range(calls)]
            out = [f.result() for f in futs]
        else:
            futs = [a.inc.remote(1) for a in actors for _ in range(calls)]
            out = get(futs)
        dt = time.perf_counter() - t0
        assert max(out) == calls
        res = {"calls_per_s": round(k * calls / dt), "actors": k, "hosts": len(set(hosts)), "ops": k * calls}
        if rt == "chimaera":  # its actors live until closed or the run ends; reps repeat
            res["direct_calls"] = _direct._counter["direct"] - direct
            for a in actors:
                a.close()
        return res

    if name == "nested":
        child = remote(lambda x: x)

        def parent_fn(n):
            if rt == "ray":
                import ray

                return sum(ray.get([child.remote(i) for i in range(n)]))
            import chimaera as ch

            return sum(ch.get([child.remote(i) for i in range(n)]))

        def dask_parent(n):
            from distributed import worker_client

            # The parent secedes from its worker's one thread while its children run.
            with worker_client() as c:
                return sum(c.gather(c.map(lambda x: x, range(n), pure=False)))

        parent = remote(dask_parent if rt == "dask" else parent_fn)
        t0 = time.perf_counter()
        out = get([parent.remote(8) for _ in range(4 * cpus)], timeout=600) if rt == "chimaera" else get([parent.remote(8) for _ in range(4 * cpus)])
        dt = time.perf_counter() - t0
        assert out[0] == 28
        return {"seconds": round(dt, 3), "parents": 4 * cpus}

    if name in ("shuffle", "shuffle_unfused"):
        # 64 map outputs of 50k (key, value) pairs each, summed into 64 partitions.
        # "shuffle" is each runtime's idiomatic program, with the map run inside
        # the split; "shuffle_unfused" the stage-for-stage control, with the
        # map's outputs stored first and read by separate split calls.
        maps, per, parts = 64, n(50_000), 64
        fused = name == "shuffle"

        def produce(i):
            return [((i * 7919 + j) % 100_003, 1) for j in range(per)]

        if rt in ("ray", "dask"):

            def split(src):
                out = [dict() for _ in range(parts)]
                for k, v in produce(src) if fused else src:
                    d = out[hash(k) % parts]
                    d[k] = d.get(k, 0) + v
                return out

            def merge(*ds):
                acc = {}
                for d in ds:
                    for k, v in d.items():
                        acc[k] = acc.get(k, 0) + v
                return acc

            if rt == "ray":
                split_r = remote(num_returns=parts)(split)
                merge_r = remote(merge)
                gen_r = remote(produce)
                t0 = time.perf_counter()
                mapped = [split_r.remote(i if fused else gen_r.remote(i)) for i in range(maps)]
                reduced = get([merge_r.remote(*[m[p] for m in mapped]) for p in range(parts)])
            else:  # Dask's num_returns: a delayed call with nout outputs, each a getitem task beside it
                from dask import delayed

                t0 = time.perf_counter()
                mapped = [delayed(split, nout=parts, pure=False)(i if fused else delayed(produce, pure=False)(i))
                          for i in range(maps)]
                merged = [delayed(merge, pure=False)(*[m[p] for m in mapped]) for p in range(parts)]
                reduced = client.gather(client.compute(merged))
            dt = time.perf_counter() - t0
        else:
            import operator

            from chimaera._data import reduce_by_key  # over stored results, as Results.reduce_by_key on a read map

            gen = remote(produce)
            t0 = time.perf_counter()
            if fused:
                # Stop the clock where Ray's does: every partition in the driver.
                reduced = list(gen.map(range(maps)).reduce_by_key(operator.add, partitions=parts))
            else:
                reduced = list(reduce_by_key([gen.remote(i) for i in range(maps)], operator.add, partitions=parts))
            dt = time.perf_counter() - t0
        # Every key's total, against one dict filled serially after the clock
        # stopped, and each key in one partition only.
        serial = {}
        for i in range(maps):
            for k, v in produce(i):
                serial[k] = serial.get(k, 0) + v
        merged = {k: v for d in reduced for k, v in d.items()}
        assert sum(len(d) for d in reduced) == len(serial), "a key is in two partitions"
        assert merged == serial, "the partitions differ from the serial result"
        # The output's digest: one value for every runtime and variant that got it right.
        digest = hashlib.sha256(repr(sorted(merged.items())).encode()).hexdigest()[:16]
        return {"seconds": round(dt, 3), "pairs": maps * per, "digest": digest}

    if name == "node_loss":
        # Kill the runtime on one worker node while 4 × cpus 1-second tasks
        # run; every task must still return exactly once.
        burn = remote(busy)
        k = 4 * cpus
        if rt == "chimaera":
            import chimaera as ch

            peer_at = {n.address.rsplit(":", 1)[0]: n.peer for n in ch.runs.cluster().nodes()}
        t0 = time.perf_counter()
        futs = [burn.remote(1.0) for _ in range(k)]
        time.sleep(2.0)
        killed = time.time()
        victim = kill_one_worker_node(rt)
        out = get(futs, timeout=900) if rt == "chimaera" else get(futs)
        dt = time.perf_counter() - t0
        assert len(out) == k
        # recovered_s: from the kill to the last result. detected_s: to the
        # head peer's first suspicion of the victim (Chimaera's member events).
        res = {"seconds": round(dt, 3), "tasks": k, "killed": victim, "recovered_s": round(time.time() - killed, 3)}
        if rt == "chimaera" and victim in peer_at:
            seen = [e.at for e in ch.runs.cluster().events(["member"], start="earliest", follow=False)
                    if e.node == peer_at[victim] and e.at >= killed and e.message.startswith(("suspect", "dead"))]
            if seen:
                res["detected_s"] = round(min(seen) - killed, 3)
        return res

    raise ValueError(name)


def kill_one_worker_node(rt: str) -> str:
    """SIGKILL the runtime's agent on one non-head node (Ray: its raylet; Dask:
    its `dask worker`, whose worker processes exit when it does)."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    if not ray.is_initialized():
        ray.init(address="auto", log_to_driver=False)
    head = ray.util.get_node_ip_address()
    victims = [n for n in ray.nodes() if n["Alive"] and n["NodeManagerAddress"] != head]
    if not victims:
        return "none (single node)"
    node = victims[0]
    pattern = {"ray": "raylet", "dask": "dask worker tcp://"}.get(rt, f"chimaera --dir {PEER_DIR} peer start")

    @ray.remote(num_cpus=0, max_retries=0)
    def kill(p: str) -> None:
        subprocess.run(["pkill", "-9", "-f", p])

    try:
        ray.get(kill.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node["NodeID"], soft=False)).remote(pattern), timeout=30)
    except Exception:  # killing the raylet also kills this task
        pass
    return node["NodeManagerAddress"]


# ---- driver ----------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist", help="directory with the chimaera binary, wheel and token")
    ap.add_argument("--out", default="results")
    ap.add_argument("--workload")
    ap.add_argument("--runtime")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    if a.workload:  # child process: run one workload, print one JSON line
        os.environ.update(CONTROL_ENV.get(a.workload, {}))  # before chimaera is imported
        res = repeat_workload(a.runtime, a.workload)
        print(json.dumps({"runtime": a.runtime, "workload": a.workload, "result": res}), flush=True)
        return
    os.makedirs(a.out, exist_ok=True)
    from chimaera._native import socket_path

    names = [w for w in WORKLOADS if not a.only or w in a.only.split(",")]
    seed = int(os.environ.get("EVAL_SEED") or random.randrange(1 << 31))
    trials = int(os.environ.get("EVAL_TRIALS", "1"))
    rng = random.Random(seed)
    # Extra Chimaera passes with other peer settings, before the main one:
    # EVAL_CHIMAERA_VARIANTS="CHIMAERA_RAFT_SYNC=none" (";" between variants,
    # "," between variables), each running EVAL_VARIANT_WORKLOADS.
    variants = [dict(kv.split("=", 1) for kv in v.split(",")) for v in os.environ.get("EVAL_CHIMAERA_VARIANTS", "").split(";") if v]
    subset = [w for w in os.environ.get("EVAL_VARIANT_WORKLOADS", "tasks,actors").split(",") if w in names]
    # The runtimes measured, in the paired trials and in node_loss.
    runtimes = os.environ.get("EVAL_RUNTIMES", "ray,chimaera").split(",")
    assert set(runtimes) <= {"ray", "chimaera", "dask"}, runtimes
    results = []
    cluster: dict = {}

    def launch(peer_env: dict[str, str]) -> dict:
        """Start the peers; the durability class they report."""
        peers, head, machines, groups, priced = launch_peers(a.dist, peer_env)
        cluster.update(nodes=len(peers), peers=peers, head=head, machines=machines, data_groups=groups, **priced)
        deadline = time.time() + 120
        while not os.path.exists(socket_path(PEER_DIR, "peer.sock")):
            assert time.time() < deadline, peer_logs()
            time.sleep(0.5)
        time.sleep(5)  # every founder joined and elected
        print("peer settings:", peer_settings(), flush=True)
        durability = manifest.settled_durability(peer_status)
        print("durability:", durability, flush=True)
        return durability

    def stop() -> None:
        try:
            stop_peers(wipe=True)
        except Exception as e:  # noqa: BLE001
            print("stopping peers:", e)

    def child(rt: str, w: str, durability: dict, **extra) -> None:
        env = dict(os.environ, CHIMAERA_HOME=PEER_DIR, RAY_ADDRESS="auto")
        counted = rt == "chimaera" and w != "node_loss"
        cpu0 = peer_cpu() if counted else []
        entries0 = manifest.raft_entries(peer_status()) if counted else 0
        t0 = time.time()
        try:
            p = subprocess.run([sys.executable, __file__, "--workload", w, "--runtime", rt], capture_output=True,
                               text=True, timeout=1200, env=env)
            lines = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
            rec = json.loads(lines[-1]) if p.returncode == 0 and lines else {
                "runtime": rt, "workload": w, "error": (p.stderr or p.stdout)[-1500:]}
        except subprocess.TimeoutExpired:
            rec = {"runtime": rt, "workload": w, "error": "timed out"}
        rec["wall_s"] = round(time.time() - t0, 1)
        rec.update(extra)
        if rt == "chimaera":
            rec["durability"] = durability
        cpu1 = peer_cpu() if cpu0 else []
        if cpu0 and len(cpu1) == len(cpu0):
            # Cores each peer process kept busy over the workload: one
            # near 1.0 or more while others idle points at a serial bottleneck.
            rec["peer_cores"] = sorted((round((b - a) / max(rec["wall_s"], 0.1), 2) for a, b in zip(cpu0, cpu1)), reverse=True)
        if counted and "result" in rec:  # the commits behind the child's work, warm-up included
            rec["result"]["raft_entries"] = manifest.raft_entries(peer_status()) - entries0
        if "error" in rec and rt == "chimaera":
            rec["peer_logs"] = peer_logs()[-4000:]
        if "error" in rec and rt == "dask" and w != "node_loss":
            # A failed run can leave Dask's cluster broken (its scheduler out of
            # memory, say): the next run gets a fresh one, as bench/compare does.
            try:
                launch_dask()
                rec["dask_restarted"] = True
            except Exception as e:  # noqa: BLE001
                print("restarting dask:", e, flush=True)
        print(json.dumps(rec), flush=True)
        results.append(rec)

    for v in variants:
        durability = launch(v)
        for w in subset:
            child("chimaera", w, durability, variant=v)
        stop()
    durability = launch({})
    if "dask" in runtimes:
        os.environ["DASK_SCHEDULER"], cluster["dask_workers"] = launch_dask()
    paired = [w for w in names if w != "node_loss"]
    for trial in range(trials):
        for w in rng.sample(paired, len(paired)):
            for rt in rng.sample(runtimes, len(runtimes)):
                if rt == "chimaera" or w not in CHIMAERA_ONLY:
                    child(rt, w, durability, trial=trial)
    # node_loss last: it takes a node's runtime down. Chimaera's first, then
    # Dask's: Ray, their launcher, stops each afterwards, and goes last.
    if "node_loss" in names and "chimaera" in runtimes:
        child("chimaera", "node_loss", durability)
    stop()
    if "dask" in runtimes:
        if "node_loss" in names:
            child("dask", "node_loss", durability)
        stop_dask()
    if "node_loss" in names and "ray" in runtimes:
        child("ray", "node_loss", durability)
    order = {"seed": seed, "trials": trials, "min_seconds": MIN_SECONDS, "scale": SCALE, "variants": variants,
             "runtimes": runtimes}
    meta = manifest.collect(os.path.join(a.dist, "chimaera"), dict.fromkeys(runtimes, sys.executable),
                            topology=cluster, durability=durability, order=order)
    doc = {"manifest": meta, "results": results}
    with open(os.path.join(a.out, "multi_results.json"), "w") as f:
        json.dump(doc, f, indent=2)
    notes = [
        f"Machines: {'; '.join(cluster['machines'])}. {cluster['data_groups']} data groups.",
        f"{trials} paired trial(s) in a random order (seed {seed}), each run timed for at least {MIN_SECONDS:.0f} s "
        f"after a warm-up; node_loss once per runtime, last. EVAL_SCALE {SCALE}.",
    ]
    if "dask" in runtimes:
        notes.append(f"Dask: a scheduler on the head node and {cluster['dask_workers']} worker processes of one "
                     "thread each, one per CPU.")
    title = " vs ".join(rt.capitalize() for rt in runtimes) + f", {cluster['nodes']} nodes"
    report = manifest.report(doc, WORKLOADS, title, notes)
    try:  # what each workload cost, from the machines' instance types (bench/evals/cost.py)
        with open(os.path.join(HERE, "prices.json")) as f:
            spend = cost.costs(doc, json.load(f))
    except ValueError as e:
        print("costs:", e, flush=True)
    else:
        with open(os.path.join(a.out, "multi_cost.json"), "w") as f:
            json.dump(spend, f, indent=2)
        report += "\n## Cost per workload\n\n" + cost.report(spend)
    with open(os.path.join(a.out, "multi_report.md"), "w") as f:
        f.write(report)
    print("===== report =====\n" + report)


if __name__ == "__main__":
    main()
