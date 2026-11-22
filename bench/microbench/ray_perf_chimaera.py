"""Ray's microbenchmark suite (ray/_private/ray_perf.py, Ray 2.60) ported to Chimaera.

Same benchmarks, names, sizes, loop structure and timing method
(ray_microbenchmark_helpers.timeit). Prints one JSON line per metric:
{"name", "value", "sd", "unit"[, "note"]} or {"name", "unsupported": reason}.

    python bench/microbench/ray_perf_chimaera.py

The "client:" benchmarks (Ray Client) run in a child process that drives a
single-peer Chimaera cluster over TCP (`peer start --client-listen`), the
closest equivalent. MICROBENCH_SLEEP overrides the 10 s pause before each
benchmark; TESTS_TO_RUN / TESTS_TO_SKIP filter by name as in Ray.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import numpy as np

import chimaera as ch

filter_pattern = os.environ.get("TESTS_TO_RUN", "")
skip_pattern = os.environ.get("TESTS_TO_SKIP", "")
SLEEP = float(os.environ.get("MICROBENCH_SLEEP", "10"))

NOTES = {
    "single client get calls (Plasma Store)": "ch.put(0) is inline in the handle; .result() returns the value cached on "
    "the future after the first read (no IPC per call)",
    "single client put calls (Plasma Store)": "ch.put(0) pickles and hashes in the driver; small values stay inline, no IPC",
    "multi client put calls (Plasma Store)": "same as single put: inline, no IPC, inside 10 tasks",
    "single client put gigabytes": "content-addressed: the same array is blake3-hashed every put but written once per run",
    "multi client put gigabytes": "identical zero arrays: hashed every put, written once per run",
    "single client get object containing 10k refs": ".result() decodes once and caches on the future; see the "
    "uncached row",
    "single client get object containing 10k refs (uncached handle)": "supplemental: a fresh handle per get "
    "(private ch._future._future_from_key) so each get asks the peer and decodes 10k handles",
    "single client wait 1k refs": "no ch.wait: 1000 next() calls on one ch.as_completed over the shrinking set",
    "1:1 async-actor calls sync": "concurrency=1000, Ray's async-actor default",
    "1:1 async-actor calls async": "concurrency=1000, Ray's async-actor default",
    "1:1 async-actor calls with args async": "concurrency=1000, Ray's async-actor default",
    "1:n async-actor calls async": "concurrency=1000, Ray's async-actor default",
    "n:n async-actor calls async": "concurrency=1000, Ray's async-actor default",
    "client: get calls": "remote driver over TCP (--client-listen); value cached on the future",
    "client: put calls": "remote driver: small values inline, no IPC",
    "client: tasks and put batch": "remote driver: small values inline, no IPC",
    "client: put gigabytes": "remote driver: content-addressed, uploaded once, hashed every put",
}


def emit(**row) -> None:
    print(json.dumps(row), flush=True)


CLIENT_NAMES = [
    "client: get calls", "client: tasks and get batch", "client: put calls", "client: tasks and put batch",
    "client: put gigabytes", "client: 1:1 actor calls sync", "client: 1:1 actor calls async",
    "client: 1:1 actor calls concurrent",
]


def selected(name: str) -> bool:
    """TESTS_TO_RUN / TESTS_TO_SKIP, matched as substrings exactly as Ray does."""
    return not (filter_pattern and filter_pattern not in name) and not (skip_pattern and skip_pattern in name)


def timeit(name, fn, multiplier=1, warmup_time_sec=None):
    """ray_microbenchmark_helpers.timeit, emitting JSON instead of returning tuples."""
    if not selected(name):
        return
    time.sleep(SLEEP if warmup_time_sec is None else warmup_time_sec)
    start = time.perf_counter()
    count = 0
    while time.perf_counter() - start < 1:
        fn()
        count += 1
    step = count // 10 + 1
    stats = []
    for _ in range(4):
        start = time.perf_counter()
        count = 0
        while time.perf_counter() - start < 2:
            for _ in range(step):
                fn()
            count += step
        end = time.perf_counter()
        stats.append(multiplier * count / (end - start))
    row = {"name": name, "value": float(np.mean(stats)), "sd": float(np.std(stats))}
    row["unit"] = "GB/s" if "gigabytes" in name else "ops/s"
    if name in NOTES:
        row["note"] = NOTES[name]
    print(name, "per second", round(row["value"], 2), "+-", round(row["sd"], 2), file=sys.stderr)
    emit(**row)


@ch.remote
class Actor:
    def small_value(self):
        return b"ok"

    def small_value_arg(self, x):
        return b"ok"

    def small_value_batch(self, n):
        ch.get([small_value.remote() for _ in range(n)])


@ch.remote(concurrency=1000)
class AsyncActor:
    async def small_value(self):
        return b"ok"

    async def small_value_with_arg(self, x):
        return b"ok"


@ch.remote
class Client:
    def __init__(self, servers):
        if not isinstance(servers, list):
            servers = [servers]
        self.servers = servers

    def small_value_batch(self, n):
        results = []
        for s in self.servers:
            results.extend([s.small_value.remote() for _ in range(n)])
        ch.get(results)

    def small_value_batch_arg(self, n):
        x = ch.put(0)
        results = []
        for s in self.servers:
            results.extend([s.small_value_arg.remote(x) for _ in range(n)])
        ch.get(results)


@ch.remote
def small_value():
    return b"ok"


@ch.remote
def create_object_containing_ref():
    obj_refs = []
    for _ in range(10000):
        obj_refs.append(ch.put(1))
    return obj_refs


@ch.remote
def do_put_small():
    for _ in range(100):
        ch.put(0)


@ch.remote
def do_put():
    for _ in range(10):
        ch.put(np.zeros(10 * 1024 * 1024, dtype=np.int64))


def main():
    ch.init()

    value = ch.put(0)

    def get_small():
        value.result()

    def put_small():
        ch.put(0)

    def put_multi_small():
        ch.get([do_put_small.remote() for _ in range(10)])

    arr = np.zeros(100 * 1024 * 1024, dtype=np.int64)

    timeit("single client get calls (Plasma Store)", get_small)
    timeit("single client put calls (Plasma Store)", put_small)
    timeit("multi client put calls (Plasma Store)", put_multi_small, 1000)

    def put_large():
        ch.put(arr)

    timeit("single client put gigabytes", put_large, 8 * 0.1)

    def small_value_batch():
        submitted = [small_value.remote() for _ in range(1000)]
        ch.get(submitted)
        return 0

    timeit("single client tasks and get batch", small_value_batch)

    def put_multi():
        ch.get([do_put.remote() for _ in range(10)])

    timeit("multi client put gigabytes", put_multi, 10 * 8 * 0.1)

    obj_containing_ref = create_object_containing_ref.remote()

    def get_containing_object_ref():
        obj_containing_ref.result()

    timeit("single client get object containing 10k refs", get_containing_object_ref)

    def get_containing_object_ref_uncached():
        ch._future._future_from_key(obj_containing_ref.key).result()

    timeit("single client get object containing 10k refs (uncached handle)", get_containing_object_ref_uncached)

    def wait_multiple_refs():
        num_objs = 1000
        not_ready = [small_value.remote() for _ in range(num_objs)]
        pending = ch.as_completed(not_ready)
        for _ in range(num_objs):
            next(pending)

    timeit("single client wait 1k refs", wait_multiple_refs)

    def small_task():
        small_value.remote().result()

    timeit("single client tasks sync", small_task)

    def small_task_async():
        ch.get([small_value.remote() for _ in range(1000)])

    timeit("single client tasks async", small_task_async, 1000)

    n = 10000
    m = 4
    actors = [Actor.remote() for _ in range(m)]

    def multi_task():
        submitted = [a.small_value_batch.remote(n) for a in actors]
        ch.get(submitted)

    timeit("multi client tasks async", multi_task, n * m)

    a = Actor.remote()

    def actor_sync():
        a.small_value.remote().result()

    timeit("1:1 actor calls sync", actor_sync)

    a = Actor.remote()

    def actor_async():
        ch.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 actor calls async", actor_async, 1000)

    a = Actor.options(concurrency=16).remote()

    def actor_concurrent():
        ch.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 actor calls concurrent", actor_concurrent, 1000)

    n = 5000
    n_cpu = multiprocessing.cpu_count() // 2
    actors = [Actor.remote() for _ in range(n_cpu)]
    client = Client.remote(actors)

    def actor_async_direct():
        client.small_value_batch.remote(n).result()

    timeit("1:n actor calls async", actor_async_direct, n * len(actors))

    n_cpu = multiprocessing.cpu_count() // 2
    a = [Actor.remote() for _ in range(n_cpu)]

    @ch.remote
    def work(actors):
        ch.get([actors[i % n_cpu].small_value.remote() for i in range(n)])

    def actor_multi2():
        ch.get([work.remote(a) for _ in range(m)])

    timeit("n:n actor calls async", actor_multi2, m * n)

    n = 1000
    actors = [Actor.remote() for _ in range(n_cpu)]
    clients = [Client.remote(a) for a in actors]

    def actor_multi2_direct_arg():
        ch.get([c.small_value_batch_arg.remote(n) for c in clients])

    timeit("n:n actor calls with arg async", actor_multi2_direct_arg, n * len(clients))

    a = AsyncActor.remote()

    def actor_sync():
        a.small_value.remote().result()

    timeit("1:1 async-actor calls sync", actor_sync)

    a = AsyncActor.remote()

    def async_actor():
        ch.get([a.small_value.remote() for _ in range(1000)])

    timeit("1:1 async-actor calls async", async_actor, 1000)

    a = AsyncActor.remote()

    def async_actor():
        ch.get([a.small_value_with_arg.remote(i) for i in range(1000)])

    timeit("1:1 async-actor calls with args async", async_actor, 1000)

    n = 5000
    n_cpu = multiprocessing.cpu_count() // 2
    actors = [AsyncActor.remote() for _ in range(n_cpu)]
    client = Client.remote(actors)

    def async_actor_async():
        client.small_value_batch.remote(n).result()

    timeit("1:n async-actor calls async", async_actor_async, n * len(actors))

    n = 5000
    m = 4
    n_cpu = multiprocessing.cpu_count() // 2
    a = [AsyncActor.remote() for _ in range(n_cpu)]

    @ch.remote
    def async_actor_work(actors):
        ch.get([actors[i % n_cpu].small_value.remote() for i in range(n)])

    def async_actor_multi():
        ch.get([async_actor_work.remote(a) for _ in range(m)])

    timeit("n:n async-actor calls async", async_actor_multi, m * n)

    if selected("placement group create/removal"):
        emit(name="placement group create/removal", unsupported="Chimaera has no placement groups or reservations "
             "(placement= is P3, reservations P5)")

    run_client_benchmarks()


# ---- Ray Client equivalents: a remote driver over TCP -----------------------


def client_main():
    """Runs in a child process whose driver connects over TCP ($CHIMAERA_ADDRESS)."""
    ch.init()
    assert ch._runtime.Runtime.get().remote

    value = ch.put(0)
    timeit("client: get calls", lambda: value.result())

    def small_value_batch():
        ch.get([small_value.remote() for _ in range(1000)])
        return 0

    timeit("client: tasks and get batch", small_value_batch)
    timeit("client: put calls", lambda: ch.put(0))
    timeit("client: tasks and put batch", lambda: ch.get([do_put_small.remote() for _ in range(10)]), 1000)

    arr = np.zeros(100 * 1024 * 1024, dtype=np.int64)
    timeit("client: put gigabytes", lambda: ch.put(arr), 8 * 0.1)

    a = Actor.remote()
    timeit("client: 1:1 actor calls sync", lambda: a.small_value.remote().result())
    timeit("client: 1:1 actor calls async", lambda: ch.get([a.small_value.remote() for _ in range(1000)]), 1000)
    a = Actor.options(concurrency=16).remote()
    timeit("client: 1:1 actor calls concurrent", lambda: ch.get([a.small_value.remote() for _ in range(1000)]), 1000)


def _peer_bin() -> str:
    return ch._runtime._peer_binary()


def run_client_benchmarks():
    if not any(selected(name) for name in CLIENT_NAMES):
        return
    home = ch._runtime.default_home() + "-client"
    shutil.rmtree(home, ignore_errors=True)
    os.makedirs(home)
    token = os.path.join(home, "token")
    with open(token, "w") as f:
        f.write(subprocess.run([_peer_bin(), "cluster", "token"], capture_output=True, text=True, check=True).stdout)
    ports = []
    for _ in range(2):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        ports.append(s)
    mesh, client = (s.getsockname()[1] for s in ports)
    for s in ports:
        s.close()
    peer_dir = os.path.join(home, "peer")
    with open(os.path.join(home, "peer.log"), "ab") as log:
        peer = subprocess.Popen(
            [_peer_bin(), "--dir", peer_dir, "peer", "start", "--listen", f"127.0.0.1:{mesh}", "--seeds",
             f"127.0.0.1:{mesh}", "--client-listen", f"127.0.0.1:{client}", "--token-file", token],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        )
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                socket.create_connection(("127.0.0.1", client), timeout=1).close()
                break
            except OSError:
                if time.monotonic() > deadline or peer.poll() is not None:
                    raise RuntimeError(f"client-listen peer did not start; see {home}/peer.log") from None
                time.sleep(0.1)
        env = {**os.environ, "CHIMAERA_ADDRESS": f"tcp://127.0.0.1:{client}", "CHIMAERA_TOKEN_FILE": token,
               "CHIMAERA_HOME": os.path.join(home, "driver")}
        subprocess.run([sys.executable, os.path.abspath(__file__), "--client"], env=env, check=False,
                       timeout=float(os.environ.get("MICROBENCH_CLIENT_TIMEOUT", "1800")))
    finally:
        peer.send_signal(signal.SIGTERM)
        try:
            peer.wait(10)
        except subprocess.TimeoutExpired:
            peer.kill()
            peer.wait()


if __name__ == "__main__":
    if sys.argv[1:] == ["--client"]:
        client_main()
    else:
        main()
