"""One workload on one runtime, in a fresh process:

    python workloads.py RUNTIME WORKLOAD

Runtimes: chimaera, ray, dask, multiprocessing (multiprocessing.Pool) and
joblib (its default loky backend). Each workload is written once against a
thin adapter, so every runtime runs the same program; where runtimes differ
in idiom (a map, a call that waits on calls it made, actors) each adapter
uses its own runtime's documented way. Nothing is batched or tuned by hand.

The child connects to a runtime already running when the orchestrator holds
one (RAY_ADDRESS, DASK_SCHEDULER, CHIMAERA_HOME); multiprocessing and joblib
start their pools here. One uncounted warm-up run comes first, then timed
runs until COMPARE_MIN_SECONDS have passed (at least three). The last line
of stdout is a JSON record with every timed sample.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import time

SCALE = float(os.environ.get("COMPARE_SCALE", "1"))
MIN_SECONDS = float(os.environ.get("COMPARE_MIN_SECONDS", "10"))
CRASH_TIMEOUT = 30.0
CPUS = os.cpu_count() or 1


def n(x: float) -> int:
    return max(1, int(x * SCALE))


# ---- the functions every runtime runs -------------------------------------------


def ident(x):
    return x


def double(x):
    return 2 * x


def add1(x):
    return x + 1


def busy(seconds):
    t = time.process_time()
    k = 0
    while time.process_time() - t < seconds:
        k += 1
    return k


def head_sum(a):
    return float(a[:10].sum())


def die_once(marker):
    """SIGKILLs its own worker the first time it runs, then returns."""
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getpid(), signal.SIGKILL)
    return "recovered"


def sum_lists(*lists):
    return sum(sum(x) for x in lists)


def lists_out(k):
    return tuple(list(range(10_000)) for _ in range(k))


def last_byte(a):
    return int(a[-1])


class Counter:
    def __init__(self):
        self.n = 0

    def inc(self, k):
        self.n += k
        return self.n

    def ping(self):
        return 1


# ---- adapters --------------------------------------------------------------------


class Chimaera:
    name = "chimaera"

    def __init__(self, fresh: bool = False):
        import chimaera as ch

        self.ch = ch
        ch.init()

    def version(self):
        return "chimaera " + getattr(self.ch, "__version__", "?")

    def fn(self, f):
        return self.ch.remote(f)

    def call(self, h, *args):
        return h.remote(*args)

    def get(self, futs):
        return self.ch.get(futs)

    def get1(self, fut, timeout=None):
        return fut.result(timeout=timeout)

    def put(self, v):
        return self.ch.put(v)

    def put_many(self, vs):
        return [self.ch.put(v) for v in vs]

    def many_out(self, f, k, *args):
        return self.ch.remote(f).options(returns=k).remote(*args).split()

    def map(self, f, items):
        return list(self.ch.remote(f).map(items))

    def actor(self, cls):
        return self.ch.remote(cls).remote()

    def method(self, a, name, *args):
        return getattr(a, name).remote(*args)

    def drop_actors(self, actors):
        for a in actors:
            a.close()

    def parent(self, child):
        ch = self.ch

        def parent(k):
            return sum(ch.get([child.remote(i) for i in range(k)]))

        return ch.remote(parent)

    def close(self):
        pass


class Ray:
    name = "ray"

    def __init__(self, fresh: bool = False):
        import ray

        self.ray = ray
        if fresh:  # a new local cluster, even where one is already up (Anyscale sets both of these)
            os.environ.pop("RAY_ADDRESS", None)
            os.environ.pop("RAY_OVERRIDE_RESOURCES", None)
            ray.init(address="local", num_cpus=CPUS, include_dashboard=False, log_to_driver=False)
        else:
            ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), log_to_driver=False)

    def version(self):
        return "ray " + self.ray.__version__

    def fn(self, f):
        return self.ray.remote(f)

    def call(self, h, *args):
        return h.remote(*args)

    def get(self, futs):
        return self.ray.get(futs)

    def get1(self, fut, timeout=None):
        return self.ray.get(fut, timeout=timeout)

    def put(self, v):
        return self.ray.put(v)

    def put_many(self, vs):
        return [self.ray.put(v) for v in vs]

    def many_out(self, f, k, *args):
        return self.ray.remote(num_returns=k)(f).remote(*args)

    def map(self, f, items):
        # Ray Core has no map: the program a Ray user writes is one task per item.
        h = self.ray.remote(f)
        return self.ray.get([h.remote(x) for x in items])

    def actor(self, cls):
        return self.ray.remote(cls).remote()

    def method(self, a, name, *args):
        return getattr(a, name).remote(*args)

    def drop_actors(self, actors):
        for a in actors:
            self.ray.kill(a)

    def parent(self, child):
        ray = self.ray

        def parent(k):
            return sum(ray.get([child.remote(i) for i in range(k)]))

        return ray.remote(parent)

    def close(self):
        self.ray.shutdown()


class Dask:
    name = "dask"

    def __init__(self, fresh: bool = False):
        from distributed import Client, LocalCluster

        if fresh:
            self.cluster = LocalCluster(n_workers=CPUS, threads_per_worker=1, dashboard_address=None)
            self.client = Client(self.cluster)
        else:
            self.cluster = None
            self.client = Client(os.environ["DASK_SCHEDULER"])

    def version(self):
        import distributed

        return "dask distributed " + distributed.__version__

    def fn(self, f):
        return f

    def call(self, f, *args):
        return self.client.submit(f, *args, pure=False)

    def get(self, futs):
        return self.client.gather(futs)

    def get1(self, fut, timeout=None):
        return fut.result(timeout=timeout)

    def put(self, v):
        return self.client.scatter(v)

    def put_many(self, vs):
        return self.client.scatter(list(vs))  # Dask's way: one scatter of the list

    def map(self, f, items):
        return self.client.gather(self.client.map(f, items, pure=False))

    def actor(self, cls):
        return self.client.submit(cls, actor=True).result()

    def method(self, a, name, *args):
        return getattr(a, name)(*args)

    def get_actor_results(self, futs):
        return [f.result() for f in futs]

    def drop_actors(self, actors):
        pass

    def parent(self, child):
        def parent(k):
            from distributed import worker_client

            with worker_client() as c:
                return sum(c.gather(c.map(child, range(k), pure=False)))

        return parent

    def close(self):
        self.client.close()
        if self.cluster:
            self.cluster.close()


class Multiprocessing:
    name = "multiprocessing"

    def __init__(self, fresh: bool = False):
        import multiprocessing

        self.pool = multiprocessing.Pool(CPUS)

    def version(self):
        return "multiprocessing (Python %d.%d)" % sys.version_info[:2]

    def fn(self, f):
        return f

    def call(self, f, *args):
        return self.pool.apply_async(f, args)

    def get(self, futs):
        return [f.get() for f in futs]

    def get1(self, fut, timeout=None):
        return fut.get(timeout)

    def map(self, f, items):
        return self.pool.map(f, items)

    def close(self):
        self.pool.terminate()


class Joblib:
    name = "joblib"

    def __init__(self, fresh: bool = False):
        from joblib import Parallel

        self.par = Parallel(n_jobs=-1)

    def version(self):
        import joblib

        return "joblib " + joblib.__version__

    def run(self, f, arg_lists):
        from joblib import delayed

        return self.par(delayed(f)(*args) for args in arg_lists)

    def map(self, f, items):
        return self.run(f, ([x] for x in items))

    def close(self):
        pass


RUNTIMES = {c.name: c for c in (Chimaera, Ray, Dask, Multiprocessing, Joblib)}

# ---- workloads -------------------------------------------------------------------
# Each returns (metric value, extra fields). `UNITS` names the unit and
# whether higher is better.

UNITS = {
    "startup": ("s", "lower", "a fresh process: start the runtime (a new cluster, not one already up) and read one call's result"),
    "latency": ("us", "lower", "p50 round trip of one no-op call, made alone and read before the next"),
    "tasks": ("calls/s", "higher", "20,000 no-op calls made in one burst, every result read"),
    "map_small": ("items/s", "higher", "the runtime's map over tiny items (doubling an int), all results read"),
    "fine_grained": ("efficiency", "higher", "500 x CPUs calls of 2 ms of CPU through the runtime's map: ideal time / wall time"),
    "big_arg": ("s", "lower", "the same 32 MiB NumPy array passed straight to 2 x CPUs calls, no put"),
    "broadcast": ("s", "lower", "one 512 MiB NumPy array put once and read by 2 x CPUs calls, put included"),
    "chain": ("steps/s", "higher", "64 chains of 20 calls, each taking the previous call's future"),
    "nested": ("s", "lower", "4 x CPUs calls that each make 8 child calls and wait for them"),
    "actors": ("calls/s", "higher", "1,000 method calls on each of max(8, CPUs / 2) actors, results read"),
    "worker_crash": ("s", "lower", "a call whose worker is SIGKILLed mid-call: seconds until its result arrives"),
    # Modeled on Ray's release suite (release/benchmarks: single_node and
    # distributed), each run once per trial without a warm-up, as Ray runs them.
    "many_args": ("s", "lower", "one call given 10,000 arguments, each a list of 10,000 ints (Ray: 10,000 object args)"),
    "many_returns": ("s", "lower", "one call returning 3,000 values, each a list of 10,000 ints, every one read"),
    "get_many": ("s", "lower", "10,000 lists of 10,000 ints put, then all read in one get"),
    "get_many_np": ("s", "lower", "10,000 NumPy arrays of 10,000 int64 put, then all read in one get (zero-copy where offered)"),
    "queued": ("s", "lower", "1,000,000 no-op calls made in one burst, every result read (Ray: 1M queued tasks)"),
    "large_object": ("s", "lower", "a 16 GiB NumPy array put, then read by a call (Ray: a 100 GiB object)"),
    "many_actors": ("actors/s", "higher", "1,000 actors created, each answering one call"),
}
# Who can run what: a runtime missing from a row has no way to write it
# (no futures as arguments, no nested calls, no actors, no store to put into).
SUPPORT = {
    "startup": set(RUNTIMES),
    "latency": {"chimaera", "ray", "dask", "multiprocessing"},
    "tasks": {"chimaera", "ray", "dask", "multiprocessing"},
    "map_small": set(RUNTIMES),
    "fine_grained": set(RUNTIMES),
    "big_arg": set(RUNTIMES),
    "broadcast": {"chimaera", "ray", "dask"},
    "chain": {"chimaera", "ray", "dask"},
    "nested": {"chimaera", "ray", "dask"},
    "actors": {"chimaera", "ray", "dask"},
    "worker_crash": set(RUNTIMES),
    "many_args": set(RUNTIMES),
    "many_returns": {"chimaera", "ray"},  # one call, separate result objects
    "get_many": {"chimaera", "ray", "dask"},
    "get_many_np": {"chimaera", "ray", "dask"},
    "queued": {"chimaera", "ray", "dask", "multiprocessing"},
    "large_object": {"chimaera", "ray", "dask"},
    "many_actors": {"chimaera", "ray", "dask"},
}
# Long workloads run once per trial, with no warm-up.
ONCE = {"many_args", "many_returns", "get_many", "get_many_np", "queued", "large_object", "many_actors"}
# Runtimes that run one task per item in their map: fewer items keep a run short.
PER_ITEM_MAP = {"ray", "dask"}


def w_latency(rt):
    h = rt.fn(ident)
    lat = []
    for i in range(n(1000)):
        t = time.perf_counter()
        assert rt.get1(rt.call(h, i)) == i
        lat.append(time.perf_counter() - t)
    lat.sort()
    return round(lat[len(lat) // 2] * 1e6, 1), {"p99_us": round(lat[int(0.99 * (len(lat) - 1))] * 1e6, 1)}


def w_tasks(rt):
    h = rt.fn(ident)
    k = n(20_000)
    t0 = time.perf_counter()
    out = rt.get([rt.call(h, i) for i in range(k)])
    dt = time.perf_counter() - t0
    assert out[-1] == k - 1
    return round(k / dt), {"calls": k}


def w_map_small(rt):
    k = n(50_000 if rt.name in PER_ITEM_MAP else 1_000_000)
    t0 = time.perf_counter()
    out = rt.map(double, range(k))
    dt = time.perf_counter() - t0
    assert len(out) == k and out[-1] == 2 * (k - 1)
    return round(k / dt), {"items": k}


def w_fine_grained(rt):
    k, each = n(500 * CPUS), 0.002
    t0 = time.perf_counter()
    out = rt.map(busy, [each] * k)
    dt = time.perf_counter() - t0
    assert len(out) == k
    return round(k * each / CPUS / dt, 3), {"calls": k, "seconds": round(dt, 3)}


def w_big_arg(rt):
    import numpy as np

    a = np.ones(n(32 << 20) // 8)
    k = 2 * CPUS
    t0 = time.perf_counter()
    if isinstance(rt, Joblib):
        out = rt.run(head_sum, [[a]] * k)
    else:
        h = rt.fn(head_sum)
        out = rt.get([rt.call(h, a) for _ in range(k)])
    dt = time.perf_counter() - t0
    assert out == [10.0] * k
    return round(dt, 4), {"calls": k}


def w_broadcast(rt):
    import numpy as np

    a = np.ones(n(512 << 20) // 8)
    k = 2 * CPUS
    h = rt.fn(head_sum)
    t0 = time.perf_counter()
    ref = rt.put(a)
    out = rt.get([rt.call(h, ref) for _ in range(k)])
    dt = time.perf_counter() - t0
    assert out == [10.0] * k
    return round(dt, 4), {"readers": k}


def w_chain(rt):
    h = rt.fn(add1)
    chains, depth = 64, 20
    t0 = time.perf_counter()
    heads = []
    for c in range(chains):
        f = rt.call(h, c)
        for _ in range(depth - 1):
            f = rt.call(h, f)
        heads.append(f)
    out = rt.get(heads)
    dt = time.perf_counter() - t0
    assert out[0] == depth
    return round(chains * depth / dt), {"steps": chains * depth}


def w_nested(rt):
    child = rt.fn(ident)
    parent = rt.parent(child)
    k = 4 * CPUS
    t0 = time.perf_counter()
    out = rt.get([rt.call(parent, 8) for _ in range(k)])
    dt = time.perf_counter() - t0
    assert out == [28] * k
    return round(dt, 4), {"parents": k}


def w_actors(rt):
    k, calls = max(8, CPUS // 2), n(1000)
    actors = [rt.actor(Counter) for _ in range(k)]
    gather = getattr(rt, "get_actor_results", rt.get)
    gather([rt.method(a, "inc", 0) for a in actors])  # every actor is up
    t0 = time.perf_counter()
    out = gather([rt.method(a, "inc", 1) for a in actors for _ in range(calls)])
    dt = time.perf_counter() - t0
    assert max(out) == calls
    rt.drop_actors(actors)
    return round(k * calls / dt), {"actors": k}


def w_worker_crash(rt):
    marker = os.path.join(tempfile.mkdtemp(), "m")
    t0 = time.perf_counter()
    try:
        if isinstance(rt, Joblib):
            out = rt.run(die_once, [[marker]])[0]
        else:
            out = rt.get1(rt.call(rt.fn(die_once), marker), timeout=CRASH_TIMEOUT)
    except Exception as e:  # the runtime's own failure: recorded, not hidden
        if isinstance(rt, Multiprocessing):
            # Pool replaces a dead worker but loses its call: the result never comes.
            rt.close()
            rt.__init__()
        return None, {"outcome": type(e).__name__, "after_s": round(time.perf_counter() - t0, 2)}
    assert out == "recovered"
    return round(time.perf_counter() - t0, 4), {"outcome": "recovered"}


def w_many_args(rt):
    k = n(10_000)
    args = [[1] * 10_000 for _ in range(k)]
    t0 = time.perf_counter()
    if isinstance(rt, Joblib):
        out = rt.run(sum_lists, [args])[0]
    else:
        out = rt.get1(rt.call(rt.fn(sum_lists), *args))
    dt = time.perf_counter() - t0
    assert out == k * 10_000
    return round(dt, 3), {"args": k}


def w_many_returns(rt):
    k = max(2, n(3000))  # one return is a value, not a list of them
    t0 = time.perf_counter()
    futs = rt.many_out(lists_out, k, k)
    out = rt.get(list(futs))
    dt = time.perf_counter() - t0
    assert len(out) == k and out[-1][-1] == 9_999
    return round(dt, 3), {"returns": k}


def w_get_many(rt):
    k = n(10_000)
    t0 = time.perf_counter()
    refs = rt.put_many(list(range(10_000)) for _ in range(k))
    out = rt.get(refs)
    dt = time.perf_counter() - t0
    assert len(out) == k and out[-1][-1] == 9_999
    return round(dt, 3), {"objects": k}


def w_get_many_np(rt):
    import numpy as np

    k = n(10_000)
    t0 = time.perf_counter()
    refs = rt.put_many(np.arange(10_000) for _ in range(k))
    out = rt.get(refs)
    dt = time.perf_counter() - t0
    assert len(out) == k and int(out[-1][-1]) == 9_999
    return round(dt, 3), {"objects": k}


def w_queued(rt):
    k = n(1_000_000)
    h = rt.fn(ident)
    t0 = time.perf_counter()
    out = rt.get([rt.call(h, i) for i in range(k)])
    dt = time.perf_counter() - t0
    assert out[-1] == k - 1
    return round(dt, 3), {"calls": k}


def w_large_object(rt):
    import numpy as np

    size = n(16 << 30)
    a = np.zeros(size, dtype=np.int8)
    a[-1] = 7
    t0 = time.perf_counter()
    ref = rt.put(a)
    out = rt.get1(rt.call(rt.fn(last_byte), ref))
    dt = time.perf_counter() - t0
    assert out == 7
    return round(dt, 3), {"bytes": size}


def w_many_actors(rt):
    k = n(1000)
    t0 = time.perf_counter()
    actors = [rt.actor(Counter) for _ in range(k)]
    gather = getattr(rt, "get_actor_results", rt.get)
    out = gather([rt.method(a, "ping") for a in actors])
    dt = time.perf_counter() - t0
    assert sum(out) == k
    rt.drop_actors(actors)
    return round(k / dt, 1), {"actors": k}


WORKLOADS = {k[2:]: v for k, v in dict(globals()).items() if k.startswith("w_")}


def startup(runtime: str) -> dict:
    """One measurement per process: the clock starts before the runtime is
    imported, and the runtime is brand new (its own cluster, its own peer)."""
    t0 = time.perf_counter()
    if runtime == "chimaera":
        os.environ["CHIMAERA_HOME"] = tempfile.mkdtemp(prefix="cmp-", dir=os.environ.get("COMPARE_TMP"))
    rt = RUNTIMES[runtime](fresh=True)
    if isinstance(rt, Joblib):
        assert rt.run(ident, [[1]]) == [1]
    else:
        assert rt.get1(rt.call(rt.fn(ident), 1)) == 1
    dt = time.perf_counter() - t0
    version = rt.version()
    rt.close()
    if runtime == "chimaera":
        import subprocess

        from chimaera import _runtime

        subprocess.run([_runtime._peer_binary(), "--dir", os.environ["CHIMAERA_HOME"], "stop"],
                       capture_output=True, timeout=60, check=False)
    return {"samples": [round(dt, 4)], "version": version}


def main() -> None:
    runtime, workload = sys.argv[1], sys.argv[2]
    if workload == "startup":
        rec = startup(runtime)
    else:
        rt = RUNTIMES[runtime]()
        fn = WORKLOADS[workload]
        pool_only = runtime in ("multiprocessing", "joblib")
        if workload == "worker_crash" and pool_only:  # warm the pool with a plain call: a crash costs it the pool
            rt.map(ident, [1])
        elif workload in ONCE:  # Ray's release tests run these cold: warm only the workers
            rt.map(ident, list(range(CPUS)))
        else:
            fn(rt)  # warm-up: workers start, code ships
        samples, extra, t0 = [], {}, time.perf_counter()
        reps = 1 if (workload == "worker_crash" and pool_only) or workload in ONCE else 3
        while len(samples) < reps or (time.perf_counter() - t0 < MIN_SECONDS and workload not in ONCE | {"worker_crash"}):
            value, extra = fn(rt)
            samples.append(value)
            if value is None:
                break
        rec = {"samples": samples, "version": rt.version(), **extra}
        rt.close()
    unit, better, what = UNITS[workload]
    rec.update({"runtime": runtime, "workload": workload, "unit": unit, "better": better, "what": what,
                "cpus": CPUS, "scale": SCALE})
    print(json.dumps(rec), flush=True)


if __name__ == "__main__":
    main()
