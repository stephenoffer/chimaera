"""Object-plane benchmark: Ray vs Chimaera over many data types and sizes.

One process runs one runtime and one scenario and prints a JSON line per
measurement; ``run.py`` runs every runtime/scenario pair in fresh processes
and writes the report.

    python bench/objects/objbench.py --runtime ray --scenario put --sizes 1K,1M,256M

Scenarios (all times are seconds, medians over repetitions):

- ``put``: the driver stores a value it already holds (``ray.put`` / ``ch.put``).
- ``get``: a consumer process on the same node receives a reference (inside a
  list, so the runtime does not resolve it) and times ``get`` alone, then reads
  every page of the value (``touch``) separately.
- ``return``: a producer process returns a value it holds; the driver times the
  call minus the same call returning ``None``.
- ``pipeline``: producer -> consumer on the same node, the reference passed as
  an argument; driver-timed, minus the same pipeline carrying ``None``.
- ``many``: thousands of small objects put by the driver and fetched by a
  consumer process in one batched get.
- ``cross_node``: ``get`` and ``pipeline`` with the consumer on another node.
- ``spill``: stores three times the configured object memory, then reads all of
  it back from another process (restore).
- ``recovery``: the node holding an object dies; time until a get succeeds,
  or the error the runtime raises.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from typing import Any

import numpy as np

KB, MB, GB = 1 << 10, 1 << 20, 1 << 30
SIZES = {"1K": KB, "64K": 64 * KB, "1M": MB, "16M": 16 * MB, "256M": 256 * MB, "1G": GB, "4G": 4 * GB}
TYPES = ["bytes", "numpy", "torch", "arrow", "pandas", "pydict", "pylist", "str", "nested"]
# Generating (and pickling) millions of small Python objects is slow in both
# runtimes alike; these types stop at 256 MiB.
PY_HEAVY = {"pydict", "pylist", "str", "pandas"}
PY_HEAVY_MAX = 256 * MB


# ---- data ------------------------------------------------------------------------


def parse_size(text: str) -> int:
    """'64K', '8G', '1.5G' -> bytes."""
    units = {"K": KB, "M": MB, "G": GB}
    return int(float(text[:-1]) * units[text[-1].upper()]) if text[-1].upper() in units else int(text)


def available(kind: str) -> bool:
    try:
        if kind == "torch":
            import torch  # noqa: F401
        elif kind == "arrow":
            import pyarrow  # noqa: F401
        elif kind == "pandas":
            import pandas  # noqa: F401
    except ImportError:
        return False
    return True


def make(kind: str, size: int, seed: int = 0) -> Any:
    """A value of roughly `size` bytes of payload, deterministic in `seed`."""
    rng = np.random.default_rng(seed)
    if kind == "bytes":
        return rng.bytes(size)
    if kind == "numpy":
        return rng.random(max(1, size // 4), dtype=np.float32)
    if kind == "torch":
        import torch

        return torch.from_numpy(rng.random(max(1, size // 4), dtype=np.float32)).clone()
    if kind == "arrow":
        import pyarrow as pa

        n = max(1, size // 24)  # int64 + float64 + ~8-byte strings
        ints = rng.integers(0, 1 << 40, n)
        return pa.table({
            "id": pa.array(ints),
            "x": pa.array(rng.random(n)),
            "s": pa.array((ints % 100_000_000).astype("U8")),
        })
    if kind == "pandas":
        import pandas as pd

        n = max(1, size // 24)
        ints = rng.integers(0, 1 << 40, n)
        return pd.DataFrame({"id": ints, "x": rng.random(n), "s": (ints % 100_000_000).astype("U8").astype(object)})
    if kind == "pydict":
        n = max(1, size // 64)  # ~64 bytes pickled per entry
        vals = rng.random(n).tolist()
        return {f"key-{i:012d}": v for i, v in enumerate(vals)}
    if kind == "pylist":
        n = max(1, size // 24)
        return rng.integers(0, 1 << 30, n).tolist()
    if kind == "str":
        return rng.bytes(max(1, size // 2)).hex()
    if kind == "nested":
        k = 8
        return {
            "meta": {"name": "batch", "seed": seed, "tags": ["a", "b", "c"]},
            "arrays": [rng.random(max(1, size // (4 * k)), dtype=np.float32) for _ in range(k)],
            "labels": rng.integers(0, 10, 128).tolist(),
        }
    raise ValueError(kind)


def _pages(buf: Any) -> int:
    a = np.frombuffer(buf, dtype=np.uint8)
    return int(a[::4096].sum()) + len(a)


def touch(v: Any) -> int:
    """Read every page of a value's buffers (forces any lazy mapping or fetch)."""
    if v is None:
        return 0
    if isinstance(v, np.ndarray):
        return _pages(np.ascontiguousarray(v).reshape(-1).view(np.uint8))
    if isinstance(v, (bytes, bytearray, memoryview)):
        return _pages(v)
    if isinstance(v, str):
        return len(v)
    if isinstance(v, dict):
        return sum(touch(x) for x in v.values()) + len(v)
    if isinstance(v, (list, tuple)):
        if v and isinstance(v[0], (int, float)):
            return len(v)
        return sum(touch(x) for x in v) + len(v)
    mod = type(v).__module__
    if mod.startswith("torch"):
        return touch(v.numpy())
    if mod.startswith("pyarrow"):
        total = 0
        for col in v.columns:
            for chunk in col.chunks:
                for b in chunk.buffers():
                    if b is not None and b.size:
                        total += _pages(b)
        return total
    if mod.startswith("pandas"):
        total = 0
        for name in v.columns:
            col = v[name].to_numpy()
            total += touch(col) if col.dtype != object else len(col)
        return total
    if isinstance(v, (int, float)):
        return 1
    raise TypeError(type(v))


def payload_bytes(v: Any) -> int:
    if isinstance(v, np.ndarray):
        return v.nbytes
    if isinstance(v, (bytes, str)):
        return len(v)
    mod = type(v).__module__
    if mod.startswith("torch"):
        return v.numel() * v.element_size()
    if mod.startswith("pyarrow"):
        return v.nbytes
    if mod.startswith("pandas"):
        return int(v.memory_usage(deep=False).sum())
    if isinstance(v, dict) and "arrays" in v:
        return sum(a.nbytes for a in v["arrays"])
    return 0


# ---- runtimes --------------------------------------------------------------------


class Runtime:
    """The few operations the scenarios need, for one runtime."""

    name: str

    def put(self, v: Any) -> Any: ...
    def get(self, ref: Any, timeout: float | None = None) -> Any: ...
    def get_many(self, refs: list[Any]) -> list[Any]: ...
    def actor(self, cls: type, node: str | None = None, *args: Any) -> Any: ...
    def call(self, actor: Any, method: str, *args: Any) -> Any: ...
    def nodes(self) -> list[str]: ...
    def here(self) -> str: ...
    def release(self, ref: Any) -> None:
        del ref


class RayRuntime(Runtime):
    name = "ray"

    def __init__(self, address: str | None) -> None:
        import ray

        self.ray = ray
        if address is None:
            kw: dict[str, Any] = {}
            if os.environ.get("RAY_PLASMA_DIR"):
                kw["_plasma_directory"] = os.environ["RAY_PLASMA_DIR"]
            if os.environ.get("RAY_OBJECT_STORE_BYTES"):
                kw["object_store_memory"] = int(os.environ["RAY_OBJECT_STORE_BYTES"])
            ray.init(log_to_driver=False, include_dashboard=False, **kw)
        else:
            ray.init(address=address, log_to_driver=False)

    def put(self, v: Any) -> Any:
        return self.ray.put(v)

    def get(self, ref: Any, timeout: float | None = None) -> Any:
        return self.ray.get(ref, timeout=timeout)

    def get_many(self, refs: list[Any]) -> list[Any]:
        return self.ray.get(refs)

    def actor(self, cls: type, node: str | None = None, *args: Any) -> Any:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        opts: dict[str, Any] = {"num_cpus": 1}
        if node is not None:
            opts["scheduling_strategy"] = NodeAffinitySchedulingStrategy(node, soft=False)
        a = self.ray.remote(cls).options(**opts).remote(*args)
        self.ray.get(a.ready.remote())
        return a

    def call(self, actor: Any, method: str, *args: Any) -> Any:
        return getattr(actor, method).remote(*args)

    def nodes(self) -> list[str]:
        return sorted(n["NodeID"] for n in self.ray.nodes() if n["Alive"])

    def here(self) -> str:
        return self.ray.get_runtime_context().get_node_id()


class ChimaeraRuntime(Runtime):
    name = "chimaera"

    def __init__(self, address: str | None) -> None:
        import chimaera as ch

        self.ch = ch
        if address:
            ch.init(cluster=address)
        # Connect now, as ray.init does, so the driver's arrays made after this
        # are allocated in the shared-memory arena like a worker's.
        from chimaera._runtime import Runtime

        Runtime.get()

    def put(self, v: Any) -> Any:
        return self.ch.put(v)

    def get(self, ref: Any, timeout: float | None = None) -> Any:
        return ref.result(timeout)

    def get_many(self, refs: list[Any]) -> list[Any]:
        return self.ch.gather(*refs)

    def actor(self, cls: type, node: str | None = None, *args: Any) -> Any:
        opts: dict[str, Any] = {"cpus": 1}
        if node is not None:
            opts["placement"] = self.ch.Placement(require={"node": node})
        a = self.ch.remote(cls).options(**opts).remote(*args)
        a.ready.remote().result()
        return a

    def call(self, actor: Any, method: str, *args: Any) -> Any:
        return getattr(actor, method).remote(*args)

    def nodes(self) -> list[str]:
        return sorted(n.peer for n in self.ch.cluster().nodes())

    def here(self) -> str:
        return next(n.peer for n in self.ch.cluster().nodes() if n.this_peer)


def connect(runtime: str, address: str | None) -> Runtime:
    return RayRuntime(address) if runtime == "ray" else ChimaeraRuntime(address)


# The actor classes are defined here (run as __main__) so both runtimes ship
# them by value to processes on any node.


def _rt_get(runtime: str) -> Any:
    if runtime == "ray":
        import ray

        return lambda r: ray.get(r)
    return lambda r: r.result()


def _rt_get_many(runtime: str) -> Any:
    if runtime == "ray":
        import ray

        return lambda rs: ray.get(rs)
    import chimaera as ch

    return lambda rs: ch.gather(*rs)


class Consumer:
    def __init__(self, runtime: str) -> None:
        self.runtime = runtime
        self._get = _rt_get(runtime)
        self._get_many = _rt_get_many(runtime)

    def ready(self) -> bool:
        return True

    def get_touch(self, refs: list[Any]) -> tuple[float, float, int]:
        t0 = time.perf_counter()
        v = self._get(refs[0])
        t1 = time.perf_counter()
        c = touch(v)
        t2 = time.perf_counter()
        del v
        return t1 - t0, t2 - t1, c

    def get_all(self, refs: list[Any], do_touch: bool) -> tuple[float, int]:
        t0 = time.perf_counter()
        vs = self._get_many(refs)
        c = sum(touch(v) for v in vs) if do_touch else len(vs)
        return time.perf_counter() - t0, c

    def consume(self, v: Any) -> int:
        return touch(v)


class Producer:
    def __init__(self, runtime: str) -> None:
        self.runtime = runtime
        self.value: Any = None

    def ready(self) -> bool:
        return True

    def make(self, kind: str, size: int, seed: int) -> int:
        self.value = make(kind, size, seed)
        return payload_bytes(self.value)

    def emit(self) -> Any:
        return self.value

    def emit_none(self) -> None:
        return None

    def put_nested(self, kind: str, size: int) -> list[Any]:
        """Store a value from this process and return a reference to it."""
        v = make(kind, size, 7)
        if self.runtime == "ray":
            import ray

            return [ray.put(v)]
        import chimaera as ch

        return [ch.put(v)]


# ---- measurement -----------------------------------------------------------------


def emit(**row: Any) -> None:
    print(json.dumps(row), flush=True)


def reps_for(size: int) -> int:
    if size <= MB:
        return 200
    if size <= 16 * MB:
        return 40
    if size <= 256 * MB:
        return 8
    return 4


def timed(fn: Any, reps: int, budget_s: float = 8.0) -> list[float]:
    out = []
    t_end = time.perf_counter() + budget_s
    for i in range(reps):
        t0 = time.perf_counter()
        fn()
        out.append(time.perf_counter() - t0)
        if i >= 2 and time.perf_counter() > t_end:
            break
    return out


def med(xs: list[float]) -> float:
    return statistics.median(xs) if xs else float("nan")


def combos(types: list[str], sizes: list[str]) -> list[tuple[str, str, int]]:
    out = []
    for kind in types:
        if not available(kind):
            emit(kind=kind, skipped="not installed")
            continue
        for s in sizes:
            size = parse_size(s)
            if kind in PY_HEAVY and size > PY_HEAVY_MAX:
                continue
            out.append((kind, s, size))
    return out


def scenario_put(rt: Runtime, types: list[str], sizes: list[str], **_: Any) -> None:
    for kind, s, size in combos(types, sizes):
        v = make(kind, size)
        for _ in range(2):
            rt.release(rt.put(v))

        def once() -> None:
            ref = rt.put(v)
            rt.release(ref)

        ts = timed(once, reps_for(size))
        emit(scenario="put", kind=kind, size=s, op="put", median_s=med(ts), min_s=min(ts), n=len(ts),
             payload=payload_bytes(v))
        del v


def scenario_get(rt: Runtime, types: list[str], sizes: list[str], node: str | None = None, label: str = "get",
                 **_: Any) -> None:
    cons = rt.actor(Consumer, node or rt.here(), rt.name)
    for kind, s, size in combos(types, sizes):
        v = make(kind, size)
        gets, touches, puts = [], [], []
        reps = reps_for(size)
        t_end = time.perf_counter() + 10
        for i in range(reps + 1):
            t0 = time.perf_counter()
            ref = rt.put(v)
            tp = time.perf_counter() - t0
            g, t, _ = rt.get(rt.call(cons, "get_touch", [ref]))
            rt.release(ref)
            if i == 0:
                continue  # warm-up
            puts.append(tp)
            gets.append(g)
            touches.append(t)
            if i >= 3 and time.perf_counter() > t_end:
                break
        emit(scenario=label, kind=kind, size=s, op="get", median_s=med(gets), min_s=min(gets), n=len(gets))
        emit(scenario=label, kind=kind, size=s, op="put+get", median_s=med([a + b for a, b in zip(puts, gets)]),
             n=len(gets))
        emit(scenario=label, kind=kind, size=s, op="touch", median_s=med(touches), n=len(touches))
        del v


def scenario_return(rt: Runtime, types: list[str], sizes: list[str], **_: Any) -> None:
    prod = rt.actor(Producer, rt.here(), rt.name)
    base = med(timed(lambda: rt.get(rt.call(prod, "emit_none")), 200, 3))
    emit(scenario="return", kind="none", size="0", op="call", median_s=base)
    for kind, s, size in combos(types, sizes):
        rt.get(rt.call(prod, "make", kind, size, 1))
        holder: list[Any] = []

        def once() -> None:
            holder.append(rt.get(rt.call(prod, "emit")))
            holder.clear()

        once()
        ts = timed(once, reps_for(size))
        emit(scenario="return", kind=kind, size=s, op="return", median_s=med(ts), min_s=min(ts),
             overhead_s=max(0.0, med(ts) - base), n=len(ts))
    rt.get(rt.call(prod, "make", "bytes", 1, 0))


def scenario_pipeline(rt: Runtime, types: list[str], sizes: list[str], node: str | None = None,
                      label: str = "pipeline", **_: Any) -> None:
    prod = rt.actor(Producer, rt.here(), rt.name)
    cons = rt.actor(Consumer, node or rt.here(), rt.name)
    base = med(timed(lambda: rt.get(rt.call(cons, "consume", rt.call(prod, "emit_none"))), 200, 3))
    emit(scenario=label, kind="none", size="0", op="call", median_s=base)
    for kind, s, size in combos(types, sizes):
        rt.get(rt.call(prod, "make", kind, size, 1))
        once = lambda: rt.get(rt.call(cons, "consume", rt.call(prod, "emit")))  # noqa: E731
        once()
        ts = timed(once, reps_for(size))
        emit(scenario=label, kind=kind, size=s, op="pipeline", median_s=med(ts), min_s=min(ts),
             overhead_s=max(0.0, med(ts) - base), n=len(ts))
    rt.get(rt.call(prod, "make", "bytes", 1, 0))


def scenario_many(rt: Runtime, types: list[str], sizes: list[str], **_: Any) -> None:
    cons = rt.actor(Consumer, rt.here(), rt.name)
    for s in ("1K", "64K", "1M"):
        size = SIZES[s]
        n = min(10_000, (2 * GB) // size)
        vals = [make("bytes", size, i) for i in range(min(n, 64))]
        t0 = time.perf_counter()
        refs = [rt.put(vals[i % len(vals)]) for i in range(n)]
        t_put = time.perf_counter() - t0
        t0 = time.perf_counter()
        t_get, _ = rt.get(rt.call(cons, "get_all", refs, True))
        t_e2e = time.perf_counter() - t0
        emit(scenario="many", kind="bytes", size=s, op="put", per_object_s=t_put / n, n=n)
        emit(scenario="many", kind="bytes", size=s, op="get", per_object_s=t_get / n, n=n, call_s=t_e2e)
        for r in refs:
            rt.release(r)
        del refs


def scenario_cross_node(rt: Runtime, types: list[str], sizes: list[str], **_: Any) -> None:
    here = rt.here()
    others = [n for n in rt.nodes() if n != here]
    if not others:
        emit(scenario="cross_node", skipped="one node")
        return
    scenario_get(rt, types, sizes, node=others[0], label="cross_get")
    scenario_pipeline(rt, types, sizes, node=others[0], label="cross_pipeline")


def scenario_spill(rt: Runtime, types: list[str], sizes: list[str], capacity: int = 8 * GB, **_: Any) -> None:
    """Store 3x the object memory in objects of each size, then read it back
    in another process; every value is checked."""
    cons = rt.actor(Consumer, rt.here(), rt.name)
    for s in sizes:
        size = parse_size(s)
        n = max(4, (3 * capacity) // size)
        base = make("numpy", size, 0)
        refs, sums = [], []
        t0 = time.perf_counter()
        err = None
        try:
            for i in range(n):
                v = base.copy()
                v[:1024] = i  # distinct objects, so nothing is deduplicated
                sums.append(touch(v))
                refs.append(rt.put(v))
                del v
        except Exception as e:  # noqa: BLE001 -- report the runtime's own failure
            err = f"{type(e).__name__}: {str(e)[:300]}"
        t_put = time.perf_counter() - t0
        emit(scenario="spill", kind="numpy", size=s, op="put", seconds=t_put, n=len(refs),
             gb_per_s=len(refs) * size / t_put / 1e9, error=err)
        # Restore: oldest first (those were spilled first).
        t0 = time.perf_counter()
        first = None
        ok = 0
        try:
            for i, r in enumerate(refs):
                g, _, c = rt.get(rt.call(cons, "get_touch", [r]))
                if first is None:
                    first = g
                ok += int(c == sums[i])
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {str(e)[:300]}"
        t_get = time.perf_counter() - t0
        emit(scenario="spill", kind="numpy", size=s, op="restore", seconds=t_get, n=len(refs), verified=ok,
             first_get_s=first, gb_per_s=len(refs) * size / max(t_get, 1e-9) / 1e9, error=err)
        for r in refs:
            rt.release(r)
        del refs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runtime", choices=["ray", "chimaera"], required=True)
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--types", default=",".join(TYPES))
    ap.add_argument("--sizes", default="1K,64K,1M,16M,256M,1G")
    ap.add_argument("--address", default=None)
    ap.add_argument("--capacity", default="8G", help="object memory for the spill scenario")
    a = ap.parse_args()
    rt = connect(a.runtime, a.address)
    fn = {
        "put": scenario_put,
        "get": scenario_get,
        "return": scenario_return,
        "pipeline": scenario_pipeline,
        "many": scenario_many,
        "cross_node": scenario_cross_node,
        "spill": scenario_spill,
    }[a.scenario]
    t0 = time.time()
    fn(rt, a.types.split(","), a.sizes.split(","), capacity=parse_size(a.capacity))
    emit(done=a.scenario, runtime=a.runtime, wall_s=round(time.time() - t0, 1))
    sys.stdout.flush()
    os._exit(0)  # skip slow runtime teardown; the harness owns cleanup


if __name__ == "__main__":
    main()
