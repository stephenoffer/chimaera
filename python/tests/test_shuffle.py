"""The shuffle under reduce_by_key: which bucket a key goes to, and how
scatters and gathers meet."""

from __future__ import annotations

import collections
import functools
import itertools
import operator
import os
import random
import resource
import signal
import sys
import threading
import time
import tracemalloc
import types
from decimal import Decimal
from fractions import Fraction

import numpy
import pytest

import chimaera as ch
from chimaera import _data, _serde, _shuffle, _shuffle_phases, _width

Pair = collections.namedtuple("Pair", "n s")

# Keys that compare equal, of different types: each group must end in one partition.
EQUAL_KEYS = [
    [1, True, 1.0, Fraction(1), Decimal(1), numpy.int64(1), numpy.bool_(True), numpy.float32(1.0), complex(1, 0)],
    [0, 0.0, -0.0, False, Decimal("-0"), numpy.float64(-0.0)],
    [2.5, Fraction(5, 2), Decimal("2.5"), numpy.float16(2.5)],
    # A longdouble's or clongdouble's item() is a NumPy scalar again. Neither
    # equals a Fraction or Decimal, so they get a group of their own.
    [1.5, numpy.longdouble(1.5), numpy.clongdouble(1.5), numpy.float32(1.5), complex(1.5, 0)],
    [(1, "a"), (1.0, "a"), (True, "a"), (numpy.int64(1), numpy.str_("a")), Pair(1, "a"), (numpy.longdouble(1), "a")],
    [((1, "a"), 2.0), ((True, "a"), 2), Pair(Pair(1.0, numpy.str_("a")), Fraction(2))],
    [frozenset({1, "b"}), frozenset({True, "b"}), frozenset({1.0, numpy.str_("b")})],
    ["x", numpy.str_("x")],
    [b"y", numpy.bytes_(b"y")],
]
MAPPERS = 3


def mapper_pairs(i: int) -> list[tuple[object, int]]:
    """Mapper i's pairs: every MAPPERS-th key of each group, value 1."""
    keys = [k for group in EQUAL_KEYS for k in group]
    return [(k, 1) for k in keys[i::MAPPERS]] + [(group[i % len(group)], 1) for group in EQUAL_KEYS]


def serial(combine):
    """What one Python dict makes of every mapper's pairs, in mapper order."""
    out: dict = {}
    for i in range(MAPPERS):
        for k, v in mapper_pairs(i):
            if combine is None:
                out.setdefault(k, []).append(v)
            else:
                out[k] = combine(out[k], v) if k in out else v
    return out


@pytest.mark.parametrize("combine", [operator.add, None])
@pytest.mark.parametrize("partitions", [17, 31])
def test_equal_keys_of_different_types_meet_in_one_partition(partitions, combine):
    scattered = [_shuffle._split(mapper_pairs(i), partitions, combine) for i in range(MAPPERS)]
    parts = [_shuffle._merge(combine, [s[j] for s in scattered]) for j in range(partitions)]
    expected = serial(combine)
    assert len(expected) == len(EQUAL_KEYS)
    assert sum(len(p) for p in parts) == len(expected), "a group was split across partitions"
    assert {k: v for p in parts for k, v in p.items()} == expected


def test_a_nan_key_has_one_bucket_in_every_process():
    # NaN's own hash is its address; it equals nothing, so any fixed bucket does.
    nans = [float("nan"), Decimal("NaN"), numpy.longdouble("nan"), numpy.clongdouble(complex(0, float("nan")))]
    assert {_shuffle.bucket_of(k, 64) for k in nans} == {0}


@ch.remote
def mixed_pairs(i):
    return mapper_pairs(i)


@pytest.mark.parametrize("partitions", [17, 31])
def test_reduce_by_key_folds_equal_keys_as_a_dict_does(partitions):
    got = mixed_pairs.map(range(MAPPERS)).reduce_by_key(operator.add, partitions=partitions)
    parts = list(got)
    assert sum(len(p) for p in parts) == len(EQUAL_KEYS)
    assert got.to_dict() == serial(operator.add) == {g[0]: len(g) + MAPPERS for g in EQUAL_KEYS}


# ---- the native scatter --------------------------------------------------------


def produce(i: int, per: int = 5000) -> list[tuple[int, int]]:
    """The integer benchmark's pairs (bench/evals/multi_node.py)."""
    return [((i * 7919 + j) % 100_003, 1) for j in range(per)]


def rows(buckets):
    """Bucket by bucket, each key (with its type, the first one seen) and value."""
    return [[(type(k), k, type(v), v) for k, v in b.items()] for b in buckets]


MIXED = [(k, n) for n, group in enumerate(EQUAL_KEYS) for k in group] + [
    (-1, 2), (2**70, 3), (-(2**65), 4), ("ü", 5), (float("nan"), 6), (float("inf"), 7), (None, 8), (b"z", 9),
]
INPUTS = {
    "ints": lambda: [(k, 1 + k % 3) for i in range(4) for k, _ in produce(i)],
    "mixed": lambda: MIXED,
    "a generator": lambda: iter(MIXED),
    "lists": lambda: [list(p) for p in MIXED],
    "dict items": lambda: collections.Counter(k for i in range(3) for k, _ in produce(i, 1000)).items(),
}


@pytest.mark.parametrize("combine", [None, operator.add, operator.mul, max, min])
@pytest.mark.parametrize("name", list(INPUTS))
def test_the_native_scatter_makes_the_python_loops_buckets(name, combine):
    assert combine in _shuffle._NATIVE_COMBINES
    for partitions in (1, 17, 64):
        native = _shuffle._split(INPUTS[name](), partitions, combine)
        assert rows(native) == rows(_shuffle._split_py(INPUTS[name](), partitions, combine))


@pytest.mark.parametrize(
    "pairs, error",
    [([(1, 2, 3)], ValueError), ([(1,)], ValueError), ([1], TypeError), ([([], 1)], TypeError), ([("\ud800", 1)], UnicodeEncodeError)],
)
def test_the_native_scatter_raises_what_the_python_loop_raises(pairs, error):
    for split in (_shuffle._split, _shuffle._split_py):
        with pytest.raises(error):
            split(pairs, 8, operator.add)


def test_the_native_scatter_runs_signal_handlers_as_the_python_loop_does():
    # A cancelled call's SIGUSR2 raises ch.Cancelled in a Python loop; the
    # native loop must let the handler run too, not only once it is done.
    class Stop(Exception):
        pass

    def stop(*_):
        raise Stop

    old = signal.signal(signal.SIGUSR2, stop)
    try:
        for split in (_shuffle._split, _shuffle._split_py):
            pairs = itertools.repeat((1, 1), 10**9)  # C code only: minutes, were it run to the end
            threading.Timer(0.2, signal.pthread_kill, (threading.main_thread().ident, signal.SIGUSR2)).start()
            t0 = time.monotonic()
            with pytest.raises(Stop):
                split(pairs, 8, operator.add)
            assert time.monotonic() - t0 < 10
    finally:
        signal.signal(signal.SIGUSR2, old)


def test_the_native_scatter_is_chosen_by_combine(monkeypatch):
    def native(*args):
        raise AssertionError("the native scatter ran")

    monkeypatch.setattr(_shuffle, "scatter", native)
    with pytest.raises(AssertionError, match="native scatter ran"):
        _shuffle._split([(1, 2)], 4, operator.add)
    # Any other combine takes the Python reference loop.
    assert _shuffle._split([(1, 2), (1, 3)], 4, lambda a, b: a - b)[1] == {1: -1}


def test_the_integer_workload_matches_the_serial_result_on_both_paths():
    maps, partitions = 8, 64
    expected = collections.Counter(k for i in range(maps) for k, _ in produce(i))
    for split in (_shuffle._split, _shuffle._split_py):
        scattered = [split(produce(i), partitions, operator.add) for i in range(maps)]
        parts = [_shuffle._merge(operator.add, [s[j] for s in scattered]) for j in range(partitions)]
        assert sum(len(p) for p in parts) == len(expected)
        assert {k: v for p in parts for k, v in p.items()} == expected


# ---- the manifest and the gathers ---------------------------------------------------


@ch.remote
def failing_pairs(i):
    if i == 2:
        raise KeyError("no pairs for 2")
    return [(i, 1)]


def test_a_failed_scatter_fails_every_partition_with_its_error():
    got = failing_pairs.map(range(4)).reduce_by_key(operator.add, partitions=3)
    for f in got.futures:
        with pytest.raises(KeyError, match="no pairs for 2"):
            f.result(timeout=120)


@ch.remote
def unique_pairs(i):
    return [(i * 100_000 + j, 1) for j in range(50_000)] + [(-1, 1)]


def gated_add(gate, a, b):
    """operator.add, once `gate` exists."""
    while not os.path.exists(gate):
        time.sleep(0.01)
    return a + b


def test_scatter_results_stay_while_a_gather_may_read_them(monkeypatch, tmp_path):
    from chimaera._runtime import Runtime

    scattered: list[ch.Future] = []
    gathers = _shuffle._gathers
    monkeypatch.setattr(_shuffle, "_gathers", lambda fs, *rest: scattered.extend(fs) or gathers(fs, *rest))
    gate = str(tmp_path / "go")
    # Each mapper's keys are its own but -1: only the gather of -1 combines, and waits.
    got = unique_pairs.map(range(4)).reduce_by_key(functools.partial(gated_add, gate), partitions=8)
    rt = Runtime.get()
    paths = []
    for f in scattered:
        assert f._wait(120) and f._outcome[0] == "ok" and f._outcome[1] is None, "a scatter result in a file"
        paths.append(_serde.object_path(rt.object_dir, f._outcome[2], f._outcome[3]))
    waiting = got.futures[_shuffle.bucket_of(-1, 8)]
    ch.wait([f for f in got.futures if f is not waiting], timeout=120)
    time.sleep(2)  # releases go out in batches
    assert all(os.path.exists(p) for p in paths), "a gather has yet to read them"
    open(gate, "w").close()
    assert got.to_dict()[-1] == 4
    time.sleep(2)
    assert all(os.path.exists(p) for p in paths), "a lost partition is made again from them while its future lives"
    del got, waiting
    deadline = time.monotonic() + 60
    while any(os.path.exists(p) for p in paths) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not any(os.path.exists(p) for p in paths), "the partitions' futures are gone: so are they"


@pytest.mark.parametrize("mapped", [False, True], ids=["read", "mapped"])
def test_a_gathers_read_names_a_bucket_kept_apart_that_is_not_here(tmp_path, monkeypatch, mapped):
    # A gather reads each scatter's result without fetching: a bucket in a
    # file of its own that this peer lacks is named, to be fetched with the
    # others, whether the result's file is read in pieces or mapped whole.
    if mapped:
        monkeypatch.setattr(_serde, "_READ_MAX", 0)
    run_dir = str(tmp_path)
    buckets = ({i: i for i in range(20_000)}, {"small": 1})
    encoded = [_serde.dumps_parts(b) for b in buckets]
    part = b"\x07" * 16
    result = os.path.join(run_dir, "result")
    _serde.write_file(result, _serde.pack_seq(encoded, [part, None])[0])
    with pytest.raises(_serde.PartAway) as away:
        _serde.read_part(result, 0, 2, run_dir, fetch=False)
    assert away.value.part == part
    assert _serde.read_part(result, 1, 2, run_dir, fetch=False) == buckets[1], "a bucket in the result's file is read"
    _serde.write_file(os.path.join(run_dir, part.hex()), encoded[0][0])
    assert _serde.read_part(result, 0, 2, run_dir, fetch=False) == buckets[0], "a bucket kept apart, here, is read"


@ch.remote
def tiny_pairs(i):
    return [(i % 1000, 1), (-(i % 7), i)]


@pytest.mark.slow
def test_driver_metadata_grows_with_maps_plus_reducers(monkeypatch):
    """4096 maps into 1024 reducers: the driver sends one call per map, one
    manifest and one gather per reducer, with one dependency each, instead
    of a gather depending on every map (4.2M edges)."""
    from chimaera._runtime import Runtime

    maps, reducers = 4096, 1024
    rt = Runtime.get()
    sent = {"calls": 0, "deps": 0, "frame_bytes": 0}
    submit = rt.submit

    def counting(specs, futures, send=True):
        for spec in specs:
            sent["calls"] += 1
            sent["deps"] += len(spec["deps"])
            sent["frame_bytes"] += spec["args_size"]
        return submit(specs, futures, send)

    monkeypatch.setattr(rt, "submit", counting)
    tracemalloc.start()
    got = tiny_pairs.map(range(maps)).reduce_by_key(operator.add, partitions=reducers)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    monkeypatch.undo()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    print(f"driver: {sent}, submission peak {peak / 2**20:.1f} MiB (tracemalloc), max RSS {rss / 2**20:.0f} MiB")
    assert sent["calls"] == maps + 1 + reducers
    assert sent["deps"] <= maps + reducers
    assert sent["frame_bytes"] < 256 * (maps + reducers)
    assert peak < 32 * 2**20
    expected: dict[int, int] = collections.Counter()
    for i in range(maps):
        for k, v in tiny_pairs(i):
            expected[k] += v
    parts = list(got)
    assert sum(len(p) for p in parts) == len(expected)
    assert {k: v for p in parts for k, v in p.items()} == expected


# ---- a gather's memory ----------------------------------------------------------------


@ch.remote
def same_keys(i: int, keys: int = 20_000) -> list[tuple[str, int]]:
    """Every map's pairs have the same keys: a gather reads maps x keys pairs and makes keys."""
    return [(f"k{j}", i * keys + j) for j in range(keys)]


def test_a_gather_folds_eight_times_its_memory_ceiling_holding_one_bucket_at_a_time():
    # A ceiling of three buckets' memory; the one partition's input is 24
    # scatters' buckets, eight times that. The gather holds the partition it
    # makes and the bucket it folds, never every scatter's.
    maps = 24
    scattered = [_shuffle._scatter.remote(same_keys.remote(i), 1, operator.add) for i in range(maps)]
    manifest = _shuffle._manifest.remote(tuple(f.key for f in scattered)).result(timeout=120)
    assert all(o[1] is None for _, o in manifest), "each scatter's result in a file, read as a gather reads it"
    tracemalloc.start()
    first = next(_shuffle._buckets(manifest, 0, 1))
    bucket = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    del first
    ceiling = 3 * bucket
    assert maps * bucket >= 8 * ceiling
    tracemalloc.start()
    out = _shuffle._merge(operator.add, _shuffle._buckets(manifest, 0, 1))
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    print(f"bucket {bucket / 2**20:.1f} MiB, input {maps * bucket / 2**20:.0f} MiB, gather peak {peak / 2**20:.1f} MiB")
    assert peak <= ceiling, f"peak {peak / 2**20:.1f} MiB over a ceiling of {ceiling / 2**20:.1f} MiB"
    expected: dict[str, int] = {}
    for i in range(maps):
        for k, v in same_keys(i):
            expected[k] = expected.get(k, 0) + v
    assert _shuffle_phases.digest([out]) == _shuffle_phases.digest([expected])


def test_a_gathers_resident_memory_stays_under_its_ceiling_with_eight_times_that_in_its_buckets():
    # A real gather, in an executor, given memory= four of its buckets: it
    # reads 32, eight times that. Its process's private memory (what its
    # peer charges it) grows by its partition and the bucket it folds, and
    # past the ceiling it would fail.
    maps, keys = 32, 40_000
    tracemalloc.start()
    one = dict(same_keys(0, keys))
    bucket = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    del one
    ceiling = 4 * bucket
    assert maps * bucket >= 8 * ceiling
    with _shuffle_phases.Record() as rec:
        out = same_keys.map(range(maps), [keys] * maps).reduce_by_key(operator.add, partitions=1, memory=ceiling)
    parts = rec.final(out)
    growth = rec.phases()["reduce"]["rss_growth_max"]
    print(f"bucket {bucket / 2**20:.1f} MiB, input {maps * bucket / 2**20:.0f} MiB, ceiling {ceiling / 2**20:.1f} MiB, "
          f"gather grew {growth / 2**20:.1f} MiB")
    assert growth <= ceiling
    expected: dict[str, int] = {}
    for i in range(maps):
        for k, v in same_keys(i, keys):
            expected[k] = expected.get(k, 0) + v
    assert _shuffle_phases.digest(parts) == _shuffle_phases.digest([expected])


@ch.remote
def distinct_keys(i: int, keys: int = 200_000) -> list[tuple[str, int]]:
    return [(f"{i}-{j}", j) for j in range(keys)]


def test_a_gather_whose_partition_outgrows_its_memory_fails_and_names_partitions():
    # The fused map with no partition count: nothing is stored yet to size
    # the partitions by (one per input here), so the gathers' ceiling is what
    # holds them: 200,000 keys each, past 8 MiB.
    got = distinct_keys.map(range(2)).reduce_by_key(operator.add, memory="8MiB")
    assert len(got) == 2
    with pytest.raises(MemoryError, match=r"memory=8388608\). Give reduce_by_key more partitions= than 2"):
        got.futures[0].result(timeout=120)
    with pytest.raises(ValueError, match="more than 0 bytes"):
        _data.reduce_by_key([], operator.add, memory=0)


@pytest.mark.parametrize("sparse", [False, True], ids=["tuple", "sparse"])
def test_a_gather_folds_in_scatter_order_and_fetches_the_buckets_kept_apart_together(monkeypatch, tmp_path, sparse):
    # Results 1 and 3 keep bucket 0 in a file of its own that is not here.
    # Both are fetched in one go once the first is met, and the buckets are
    # folded in scatter order (a str + is associative, not commutative).
    from chimaera._runtime import Runtime

    rt = Runtime.get()
    monkeypatch.setattr(rt, "object_dir", str(tmp_path))
    run = os.urandom(16)
    count = 1_000_000 if sparse else 2
    manifest, apart = [], {}
    for i in range(4):
        encoded = [_serde.dumps_parts({"k": f"s{i}"}), _serde.dumps_parts({"other": i})]
        part = os.urandom(16) if i in (1, 3) else None
        if part is not None:
            apart[part] = encoded[0][0]
        layout = _serde.pack_seq(encoded, [part, None], [0, count - 1], count) if sparse else _serde.pack_seq(encoded, [part, None])
        head = b"".join(bytes(x) for x in layout[0])
        assert [_serde.part_id(head, j, count) for j in (0, count - 1)] == [part, None], "what a PartDep fetches"
        obj = os.urandom(16)
        _serde.write_file(_serde.object_path(rt.object_dir, run, obj), layout[0])
        manifest.append((bytes(32), ("ok", None, run, obj, 0, 0)))
    fetches = []

    def fetch(rt, objects):
        fetches.append({obj for _, obj, _ in objects})
        for r, obj, _ in objects:
            _serde.write_file(_serde.object_path(rt.object_dir, r, obj), apart[obj])

    monkeypatch.setattr(_shuffle, "_fetch", fetch)
    assert _shuffle._merge(operator.add, _shuffle._buckets(manifest, 0, count)) == {"k": "s0s1s2s3"}
    assert fetches == [set(), set(apart)], "the results are here; both buckets kept apart in one fetch"


# ---- sparse destinations ----------------------------------------------------------------

MILLION = 1_000_000


@pytest.mark.parametrize("split", [_shuffle._split, _shuffle._split_py], ids=["native", "python"])
def test_a_scatter_into_a_million_partitions_makes_and_stores_only_the_buckets_it_touches(split, tmp_path):
    pairs = [(k * 7919, k) for k in range(100)]
    tracemalloc.start()
    out = split(pairs, MILLION, operator.add)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    touched = {_shuffle.bucket_of(k, MILLION) for k, _ in pairs}
    assert len(out) == MILLION and set(out.present) == touched
    assert peak < 2048 * len(touched), f"{peak} bytes for {len(touched)} buckets: a dict for each of the million"
    # Stored as the executor stores it: an entry for each bucket touched, none for the others.
    positions = sorted(out.present)
    encoded = [_serde.dumps_parts(out.present[p]) for p in positions]
    parts, size, _ = _serde.pack_seq(encoded, None, positions, MILLION)
    table = (size - sum(n for _, n, _ in encoded)) / len(touched)
    print(f"{len(touched)} of {MILLION} buckets: {size / len(touched):.0f} bytes each stored, {table:.0f} of them its entry")
    assert table <= 128
    path = str(tmp_path / "result")
    _serde.write_file(path, parts)
    absent = next(i for i in range(MILLION) if i not in out.present)
    for p in (positions[0], positions[-1], absent, MILLION - 1):
        assert _serde.read_part(path, p, MILLION) == out[p]
        assert _serde.loads_part(open(path, "rb").read(), p, MILLION) == out[p]
    assert _serde.part_of(path, positions[0], MILLION) is None, "kept in the result's file"
    whole = _serde.loads(open(path, "rb").read())
    assert type(whole) is _serde.Sparse and len(whole) == MILLION and whole.present == out.present


@ch.remote
def split_watching_fds(pairs: list[tuple[int, bytes]], partitions: int, report: str) -> _serde.Sparse:
    """``_split`` in a call, with a thread counting this process's open
    descriptors until the call's result is stored (after it returns); it
    writes how many were open before and the most it saw to `report`."""
    from chimaera._runtime import Runtime

    rdir = _serde.run_dir(Runtime.get().object_dir, ch.context().run)
    before = set(os.listdir(rdir))
    base = len(os.listdir("/dev/fd"))

    def watch() -> None:
        most, deadline = base, time.monotonic() + 60
        while time.monotonic() < deadline:
            most = max(most, len(os.listdir("/dev/fd")))
            if any(".tmp" not in n and ".part" not in n for n in set(os.listdir(rdir)) - before):
                break
        with open(report, "w") as f:
            f.write(f"{base} {most}")

    threading.Thread(target=watch, daemon=True).start()
    return _shuffle._split(pairs, partitions, None)


def test_a_scatter_into_a_million_partitions_writes_one_file_and_opens_one_descriptor_at_a_time(tmp_path):
    from chimaera._runtime import Runtime

    rt = Runtime.get()
    pairs = ch.put([(k * 7919, b"v" * 1000) for k in range(300)])  # 300 KB of buckets: a file, not inline
    rdir = _serde.run_dir(rt.object_dir, pairs._put[0])
    before, report = set(os.listdir(rdir)), str(tmp_path / "fds")
    f = split_watching_fds.options(returns=MILLION).remote(pairs, MILLION, report)
    assert f._wait(120) and f._outcome[0] == "ok" and f._outcome[1] is None
    _, _, run, obj, size, _ = f._outcome
    assert run == pairs._put[0] and set(os.listdir(rdir)) - before == {obj.hex()}, "one file for a million buckets"
    with open(_serde.object_path(rt.object_dir, run, obj), "rb") as fh:
        head = fh.read(_serde.header_size(0))
    assert head[:4] == _serde.SPARSE_MAGIC and _serde.external_blobs(head) == [], "no bucket in a file of its own"
    touched = len({_shuffle.bucket_of(k, MILLION) for k, _ in pairs.result()})
    deadline = time.monotonic() + 60
    while not os.path.exists(report) and time.monotonic() < deadline:
        time.sleep(0.01)
    base, most = map(int, open(report).read().split())
    print(f"{touched} buckets of {MILLION}: one file of {size} bytes, {size / touched:.0f} per bucket; "
          f"{base} descriptors open before, at most {most} while it split and stored them")
    assert size / touched < 1000 + 256
    assert most - base <= 2, "the result's file, and the listing that counts them: none per destination"


def stored_magic(f: ch.Future) -> bytes:
    """The layout a finished call's result is stored in: its first bytes."""
    from chimaera._runtime import Runtime

    o = f._outcome
    if o[1] is not None:
        return bytes(o[1][:4])
    with open(_serde.object_path(Runtime.get().object_dir, o[2], o[3]), "rb") as fh:
        return fh.read(4)


def test_scatters_store_the_tuple_a_sparse_stands_for_while_a_peer_cannot_read_one(monkeypatch):
    from chimaera._runtime import Runtime

    assert [n["object_layout"] for n in Runtime.get().client.nodes()] == [_serde.SPARSE_LAYOUT], "this peer reads them"
    old = dict(NODE, cpus=4.0, memory_bytes=8 << 30)  # a peer too old to say
    assert _shuffle.sparse_readable([dict(old, object_layout=_serde.SPARSE_LAYOUT)])
    assert not _shuffle.sparse_readable([dict(old, object_layout=_serde.SPARSE_LAYOUT), old])
    expected: dict[int, int] = collections.Counter()
    for i in range(6):
        for k, v in two_keys(i):
            expected[k] += v
    for nodes, magic in (([old], _serde.SEQ_MAGIC), (Runtime.get().client.nodes(), _serde.SPARSE_MAGIC)):
        monkeypatch.setattr(_width, "live_nodes", lambda rt, nodes=nodes: nodes)
        with _shuffle_phases.Record() as rec:
            got = two_keys.map(range(6)).reduce_by_key(operator.add, partitions=8)
        assert {k: v for p in rec.final(got) for k, v in p.items()} == expected
        assert {stored_magic(f) for f in rec.shuffles[0][3]} == {magic}


@ch.remote
def two_keys(i: int) -> list[tuple[int, int]]:
    return [(i % 3, 1), (10_000 + i, i)]


def test_reduce_by_key_into_more_partitions_than_a_tuple_is_stored_in():
    # Past SEQ_MAX a tuple is stored whole: every gather would decode every bucket of it.
    partitions = 4500
    got = two_keys.map(range(6)).reduce_by_key(operator.add, partitions=partitions)
    expected: dict[int, int] = collections.Counter()
    for i in range(6):
        for k, v in two_keys(i):
            expected[k] += v
    parts = list(got)
    assert len(parts) == partitions and sum(len(p) for p in parts) == len(expected)
    assert {k: v for p in parts for k, v in p.items()} == expected


# ---- phases ----------------------------------------------------------------------------


@ch.remote
def keyed(i: int, skew: str, per: int = 2000) -> list[tuple[int, int]]:
    rng = random.Random(i)
    keys = [int(rng.paretovariate(1.2)) % 5000 if skew == "zipf" else rng.randrange(5000) for _ in range(per)]
    return [(k, 1) for k in keys]


@pytest.mark.parametrize("fused", [True, False], ids=["fused", "stored"])
@pytest.mark.parametrize("skew", ["uniform", "zipf"])
def test_a_recorded_shuffle_times_each_phase_and_matches_the_reference_digest(skew, fused):
    maps, per = 8, 2000
    with _shuffle_phases.Record() as rec:
        if fused:
            out = keyed.map(range(maps), [skew] * maps).reduce_by_key(operator.add, partitions=4)
        else:
            out = _data.reduce_by_key([keyed.remote(i, skew) for i in range(maps)], operator.add, partitions=4)
    parts = rec.final(out)
    phases = rec.phases()
    assert list(phases) == ["plan", "map", "split", "scatter_encode", "fetch", "read", "reduce", "gather_encode", "final"]
    for name, p in phases.items():
        assert 0 <= p["max_seconds"] <= p["seconds"] + 1e-9, name
    for name in ("plan", "split", "scatter_encode", "read", "reduce", "gather_encode", "final"):
        assert phases[name]["seconds"] > 0, name
    assert phases["split"]["pairs"] == maps * per and phases["split"]["buckets"] == maps * 4
    for name in ("scatter_encode", "read", "gather_encode"):
        assert phases[name]["bytes"] > 0, name
    assert phases["read"]["bytes"] == phases["scatter_encode"]["bytes"], "each bucket read once, as it was encoded"
    assert phases["fetch"]["bytes"] == 0, "the peers count what crosses the network, not the gathers"
    for name in ("split", "read", "reduce"):
        assert 0 < phases[name]["cpu_seconds"] <= phases[name]["seconds"] + 1e-3, name
    assert phases["reduce"]["rss_growth_max"] >= 0
    expected = collections.Counter(k for i in range(maps) for k, _ in keyed(i, skew))
    assert phases["reduce"]["keys"] == len(expected)
    assert _shuffle_phases.digest(parts) == _shuffle_phases.digest([dict(expected)])
    # Every boundary, in the order a shuffle passes them (one machine: one clock).
    t = rec.timeline()
    order = ["submit_start", "submitted", "scatter_start", "scatter_split", "scatter_end", "manifest_start",
             "manifest_waited", "manifest_end", "gather_start", "gather_merged", "gather_end", "driver_done",
             "driver_collected"]
    assert [k for k in t if k in order] == order
    assert t["submit_start"]["first"] >= 0 and t["scatter_start"]["first"] >= t["submit_start"]["first"]
    for one in t.values():
        assert one["first"] <= one["median"] <= one["last"]
    assert t["scatter_split"]["last"] <= t["scatter_end"]["last"] <= t["manifest_waited"]["last"]
    assert t["manifest_end"]["last"] <= t["gather_start"]["first"], "each gather starts once the manifest returned"
    assert t["gather_merged"]["last"] <= t["gather_end"]["last"] <= t["driver_done"]["first"]
    assert t["driver_done"]["last"] <= t["driver_collected"]["first"]


def test_the_reference_digest_ignores_partitioning_and_order_but_not_types_or_repeats():
    digest = _shuffle_phases.digest
    assert digest([{1: 2, "a": 3}]) == digest([{"a": 3}, {1: 2}])
    assert digest([{1: 2}]) != digest([{1.0: 2}]) != digest([{1: 3}])
    assert digest([{1: 2}, {1: 2}])[1] == 2, "a key in two partitions counts twice"


def test_a_shuffle_outside_a_record_reports_nothing():
    got = keyed.map(range(2), ["uniform"] * 2).reduce_by_key(operator.add, partitions=2)
    list(got)
    assert all(f.metrics() == [] for f in got.futures)


# ---- the partition count ------------------------------------------------------------------

NODE = {"alive": True, "draining": False}


def test_the_default_partition_count_spreads_over_the_cluster_and_fits_each_reducer():
    small = dict(NODE, cpus=2.0, memory_bytes=8 << 30)  # the script's host
    nodes = [small] + [dict(NODE, cpus=16.0, memory_bytes=64 << 30) for _ in range(63)]
    assert _shuffle.partitions_for(4096, 0, nodes) == 2 + 63 * 16, "the cluster's cores, not the script's peer's 2"
    assert _shuffle.partitions_for(8, 0, nodes) == 8
    # 1 TiB stored takes about 12 TiB as partitions, and a gather holds one
    # with a bucket (an eighth of one, of 8 inputs) in half its memory; the
    # least memory per core is 4 GiB.
    parts = _shuffle.partitions_for(8, 1 << 40, nodes)
    assert _shuffle.HEADROOM == 2
    assert parts == 6912 and (1 << 40) * _shuffle.DECODED_PER_STORED / parts * (1 + 1 / 8) <= (8 << 30) / 2 / 2
    assert _shuffle.partitions_for(8, 1 << 40, nodes, memory=1 << 30) == 27648, "a reducer of reduce_by_key's memory="
    assert _shuffle.partitions_for(64, 0, [dict(small, alive=False), dict(small, draining=True), nodes[1]]) == 16
    assert _shuffle.partitions_for(0, 0, nodes) == 1 and _shuffle.partitions_for(5, 0, []) == 1


def test_the_partition_count_falls_back_to_this_peers_cores_when_its_peer_cannot_list_the_others(monkeypatch):
    class Unreachable:
        def nodes(self):
            raise ConnectionError("the peer is restarting")

    rt = types.SimpleNamespace(client=Unreachable(), info={"usable_cores": 6, "gpus": [], "memory_bytes": 12 << 30})
    monkeypatch.setattr(_width, "_capacity", [None, 0.0, 0.0, 0, []])
    nodes = _width.live_nodes(rt)
    assert _shuffle.partitions_for(64, 0, nodes) == 6 and _shuffle.partitions_for(3, 0, nodes) == 3
    assert _shuffle.partitions_for(4, 1 << 30, nodes) == 15, "this peer's memory per core: 2 GiB"
    assert _width._live_capacity(rt) == (6.0, 0)
    assert not _shuffle.sparse_readable(nodes), "the peers it cannot see may be too old to read a Sparse"


@ch.remote
def sized_pairs(i: int, per: int = 10_000) -> list[tuple[int, int]]:
    return [(i * per + j, j) for j in range(per)]


def test_reduce_by_key_chooses_from_its_inputs_their_stored_bytes_and_the_live_peers(monkeypatch):
    from chimaera._runtime import Runtime

    seen = []
    choose = _shuffle.partitions_for
    monkeypatch.setattr(_shuffle, "partitions_for", lambda *a: seen.append(a) or choose(*a))
    stored = [sized_pairs.remote(i) for i in range(3)]
    ch.wait(stored)
    got = _data.reduce_by_key(stored, operator.add)
    (inputs, nbytes, nodes, memory), = seen
    assert inputs == 3 and nbytes == sum(f._outcome[4] for f in stored) > 0 and memory == 0
    assert [n["peer"] for n in nodes] == [n["peer"] for n in Runtime.get().client.nodes()]
    assert len(got) == choose(inputs, nbytes, nodes)
    assert _shuffle.stored_bytes([]) == 0 and _shuffle.stored_bytes([3, "not a future"]) == 0


def test_reduce_by_key_given_memory_makes_as_many_partitions_as_fit_it():
    from chimaera._runtime import Runtime

    # Each gather is planned at half its 32 MiB, about 14 MiB: a few MiB of
    # its process's own growth leave it well inside.
    stored = [sized_pairs.remote(i, 150_000) for i in range(4)]
    ch.wait(stored)
    nbytes, ceiling = _shuffle.stored_bytes(stored), 32 << 20
    cores = sum(int(n["cpus"]) for n in _width.live_nodes(Runtime.get()))
    with _shuffle_phases.Record() as rec:
        got = _data.reduce_by_key(stored, operator.add, memory="32MiB")
    parts = rec.final(got)
    print(f"{nbytes / 2**20:.1f} MiB stored into {len(got)} partitions of 32 MiB, "
          f"the largest gather grew {rec.phases()['reduce']['rss_growth_max'] / 2**20:.1f} MiB")
    assert len(got) == _shuffle.partitions_for(4, nbytes, _width.live_nodes(Runtime.get()), ceiling)
    assert len(got) > min(4, cores), "more partitions than inputs: four would not each fit 32 MiB"
    assert rec.phases()["reduce"]["rss_growth_max"] <= ceiling
    assert _shuffle_phases.digest(parts) == _shuffle_phases.digest([dict(sized_pairs(i, 150_000)) for i in range(4)])
