"""Large values: stored once by content, read zero-copy and writable, released
when the call that needed them ends."""

from __future__ import annotations

import gc
import json
import os
import select
import sys
import time
import types
import warnings

import numpy as np
import pytest

import chimaera as ch
from chimaera import _serde, _values
from chimaera._native import blake3_buffer
from chimaera._runtime import Runtime


@ch.remote
def total(a) -> float:
    return float(np.asarray(a).sum())


@ch.remote
def set_first(a, value: float) -> float:
    a[0] = value
    return float(a[0])


@ch.remote
def first(a) -> float:
    return float(a[0])


@ch.remote
def length(xs) -> int:
    return len(xs)


@ch.remote
def size_of(b: bytes) -> int:
    return len(b)


@ch.remote
def ones(n: int):
    return np.ones(n)


@ch.remote
def echo(x):
    return x


def _run_dir() -> str:
    rt = Runtime.get()
    return os.path.join(rt.object_dir, rt.run.hex())


def _files() -> set[str]:
    d = _run_dir()
    return {n for n in os.listdir(d) if ".tmp" not in n} if os.path.isdir(d) else set()


def _inodes(names) -> set[int]:
    """Distinct stored files behind `names`: on Linux an arena array is linked
    under a name per use, all one file (one copy of the bytes)."""
    return {os.stat(os.path.join(_run_dir(), n)).st_ino for n in names}


def _blob_name(buffer) -> str:
    """Blobs are named by the first 16 bytes of their blake3 digest."""
    return blake3_buffer(buffer)[:16].hex()


def test_the_same_array_passed_many_times_is_stored_once():
    a = np.arange(2 << 20, dtype=np.float64)  # 16 MiB
    before = _files()
    futures = [total.remote(a) for _ in range(50)]
    assert {f.result(timeout=120) for f in futures} == {float(a.sum())}
    new = _files() - before
    assert len(_inodes(new)) == 1, f"one stored copy of the bytes, not one per call: {len(_inodes(new))} files"


def _stored_since(before: set[str], min_bytes: int = 2 << 20) -> int:
    """Distinct files of at least `min_bytes` the run stored since `before`."""
    inodes = set()
    for n in _files() - before:
        try:
            st = os.stat(os.path.join(_run_dir(), n))
        except OSError:
            continue  # removed meanwhile
        if st.st_size >= min_bytes:
            inodes.add(st.st_ino)
    return len(inodes)


def test_pickling_large_bytes_or_str_leaves_nothing_holding_them():
    # A worker returning large bytes must not keep them: a class made per call
    # sat in a reference cycle until the next garbage collection, and every
    # returned value stayed in memory (on Linux, an arena file in /dev/shm).
    import tracemalloc

    b = b"x" * (4 << 20)
    s = "y" * (4 << 20)
    base = sys.getrefcount(b)
    gc.disable()
    tracemalloc.start()
    try:
        _serde.dumps_parts(b)
        assert sys.getrefcount(b) == base, "something still holds the pickled bytes"
        before = tracemalloc.get_traced_memory()[0]
        _serde.dumps_parts(s)
        kept = tracemalloc.get_traced_memory()[0] - before
        assert kept < 1 << 20, f"{kept >> 20} MiB of the encoded str is still held"
    finally:
        tracemalloc.stop()
        gc.enable()


def test_a_worker_returning_large_bytes_keeps_none_of_them():
    # On Linux each large bytes value is an arena file of its worker; kept
    # alive after the call, one more stayed in /dev/shm per call. Freed
    # allocations wait in a reuse cache of at most 256 MiB per worker, so 24
    # calls of 64 MiB (more than six per worker) tell the two apart.
    @ch.remote
    def blob(i):
        return bytes([i]) * (64 << 20)

    for i in range(24):
        assert len(blob.remote(i).result(timeout=60)) == 64 << 20
    arena = os.path.join(Runtime.get().object_dir, ".arena")
    held = {
        d: sum(os.path.getsize(os.path.join(arena, d, f)) for f in os.listdir(os.path.join(arena, d)))
        for d in (os.listdir(arena) if os.path.isdir(arena) else [])
    }
    assert all(n <= 320 << 20 for n in held.values()), {d: n >> 20 for d, n in held.items()}


def test_large_results_released_mid_run_free_their_files():
    """A result's large buffers may be stored apart from its frame (on Linux
    an arena array is linked, not copied, and the frame may be a few inline
    bytes). Releasing the result frees them too, so a loop over fresh large
    results stays flat; kept until the run ended, they filled the memory pool
    and killed the peer in a 1-node cloud run."""
    before = _files()
    for _ in range(8):
        f = ones.remote(1 << 22)  # 32 MiB
        assert f.result(timeout=60)[0] == 1.0
        del f
        gc.collect()
    deadline = time.monotonic() + 15
    while _stored_since(before, 16 << 20) > 1 and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _stored_since(before, 16 << 20) <= 1, f"{_stored_since(before, 16 << 20)} results still stored"


def test_a_calls_inputs_stay_while_its_result_is_held():
    """A lost result is made again by running its call, which reads the same
    inputs (lineage): an array passed to a call stays stored while the call's
    future lives, even once the array is gone, and goes with the future."""
    before = _files()
    a = np.full(1 << 19, 9.0)  # 4 MiB
    f = total.remote(a)
    assert f.result(timeout=60) == 9.0 * a.size
    del a
    gc.collect()
    time.sleep(0.5)
    assert _stored_since(before) == 1, "the input went while its call's result was still held"
    del f
    gc.collect()
    deadline = time.monotonic() + 10
    while _stored_since(before) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert _stored_since(before) == 0, "the input stayed after its call's future was dropped"


def test_fresh_values_per_iteration_do_not_pile_up(monkeypatch):
    """A long-lived loop that passes new bytes each time keeps a bounded number
    of copies stored, not one per iteration: an array's blob (stored by
    content, or on Linux linked from the arena) goes once the array is gone and
    its calls are done; a bytes value, which cannot be watched, waits among the
    recently freed up to a byte budget. Kept for the run, the arena links
    filled /dev/shm in a 60 s eval loop."""
    monkeypatch.setattr(_values, "_RECENT_BYTES", 4 << 20)  # two of the 2 MiB payloads below
    before = _files()

    def stored() -> int:
        return _stored_since(before)

    def settled() -> int:
        deadline = time.monotonic() + 10
        while stored() > 2 and time.monotonic() < deadline:
            time.sleep(0.1)
        return stored()

    for i in range(12):
        a = np.full(1 << 19, float(i))  # 4 MiB, different bytes each time
        assert {f.result(timeout=60) for f in [total.remote(a) for _ in range(4)]} == {float(i) * a.size}
        del a
    assert settled() <= 2, f"{stored()} stored arrays after 12 iterations"
    for _ in range(12):
        payload = os.urandom(2 << 20)
        assert [f.result(timeout=60) for f in [size_of.remote(payload) for _ in range(2)]] == [len(payload)] * 2
    assert settled() <= 3, f"{stored()} stored bytes values after 12 iterations"


def test_arrays_arrive_writable_and_a_write_stays_in_its_call():
    a = np.zeros(1 << 20)  # 8 MiB: travels as a blob
    assert set_first.remote(a, 7.0).result(timeout=60) == 7.0
    assert first.remote(a).result(timeout=60) == 0.0, "a later call saw an earlier call's write"
    assert a[0] == 0.0, "the caller's array changed"
    small = np.zeros(1000)  # inline: copied, also writable
    assert set_first.remote(small, 3.0).result(timeout=60) == 3.0


def test_a_large_python_object_is_stored_once():
    xs = list(range(400_000))  # about 2 MiB pickled, no out-of-band buffers
    before = _files()
    assert [length.remote(xs).result(timeout=60) for _ in range(5)] == [len(xs)] * 5
    assert len(_files() - before) == 1


def test_per_call_argument_files_go_away_when_the_call_ends():
    payload = os.urandom(200 << 10)  # 200 KiB: one file per call
    before = _files()
    assert [size_of.remote(payload).result(timeout=60) for _ in range(5)] == [len(payload)] * 5
    deadline = time.monotonic() + 10
    while _files() - before and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _files() - before, f"left behind: {sorted(_files() - before)}"


def test_put_returns_a_resolved_future_that_calls_read_by_reference():
    a = np.ones(1 << 20)
    x = ch.put(a)
    assert isinstance(x, ch.Future) and x.done()
    assert x.result()[0] == 1.0
    before = _files()
    stored = _inodes(before)
    assert [total.remote(x).result(timeout=60) for _ in range(3)] == [float(a.size)] * 3
    assert _inodes(_files() - before) <= stored, "put already stored it: calls add no bytes"


def test_passing_the_same_large_object_again_and_again_warns_once():
    xs = list(range(400_000))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        futures = [length.remote(xs) for _ in range(30)]
        [f.result(timeout=120) for f in futures]
    ch107 = [w for w in caught if "CH107" in str(w.message)]
    assert len(ch107) == 1, [str(w.message) for w in caught]
    assert "ch.put" in str(ch107[0].message)


def test_a_fetched_value_passed_straight_back_warns():
    value = ones.remote(1 << 20).result(timeout=60)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        total.remote(value).result(timeout=60)
    assert any("CH103" in str(w.message) for w in caught)


def test_cpu_tensors_travel_out_of_band():
    torch = pytest.importorskip("torch")  # only where torch is installed
    t = torch.arange(1 << 20, dtype=torch.float64)
    # Its 8 MiB are a blob named by their content, not bytes in the call frame.
    # (Checked on the encoding: a call's argument blobs are freed when it ends.)
    rt = Runtime.get()
    enc = _values.encode_frame(((t,), {}), rt.run, rt.blob_sink, None)
    assert enc.blobs == {bytes.fromhex(_blob_name(t.numpy())): t.numel() * 8}
    assert enc.size < 4096, enc.size
    assert total.remote(t).result(timeout=60) == float(t.sum())


def test_a_large_python_collection_shared_with_calls_warns_once_per_site(monkeypatch):
    # Every reader unpickles its own copy of a list; an array is mapped.
    monkeypatch.setattr(_values, "collection_lint", _values.CollectionLint())
    xs = [float(i) for i in range(1_200_000)]  # pickles to about 11 MiB
    arr = np.arange(1_200_000, dtype=np.float64)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert [length.remote(xs).result(timeout=60) for _ in range(2)] == [len(xs)] * 2
        assert length.remote(arr).result(timeout=60) == len(arr)
        assert len(ch.put(xs).result()) == len(xs)
    warned = [str(w.message) for w in caught if "CH118" in str(w.message)]
    assert len(warned) == 2, warned  # the call site once, the put once; the array never
    assert all("NumPy array or an Arrow table" in m and "list" in m for m in warned), warned


def test_a_gather_that_outgrows_the_machine_warns_once(monkeypatch):
    from chimaera import _future

    monkeypatch.setattr(_future, "GATHER_WARN_BYTES", 1 << 20)  # stands in for a quarter of the machine
    monkeypatch.setattr(_future, "_gather_warned", False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert len(ch.get([ones.remote(1 << 10) for _ in range(4)], timeout=60)) == 4  # 8 KiB each
        assert not [w for w in caught if "CH117" in str(w.message)], "small results must not warn"
        ch.get([ones.remote(1 << 18) for _ in range(4)], timeout=60)  # 2 MiB each
        ch.get([ones.remote(1 << 18) for _ in range(4)], timeout=60)
    warned = [str(w.message) for w in caught if "CH117" in str(w.message)]
    assert len(warned) == 1 and "as_completed" in warned[0], warned


def _in_forked_child(work) -> str:
    """What `work()` returned in a forked child of this program, how it
    failed, or "hung" after 30 s."""
    r, w = os.pipe()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)  # forking a process with threads
        pid = os.fork()
    if pid == 0:
        try:
            out = str(work())
        except BaseException as e:  # noqa: BLE001 -- reported to the parent
            out = f"{type(e).__name__}: {e}"
        os.write(w, out.encode()[:4000])
        os._exit(0)
    os.close(w)
    ready, _, _ = select.select([r], [], [], 30)
    if not ready:
        os.kill(pid, 9)
    out = os.read(r, 4000).decode() if ready else "hung"
    os.close(r)
    os.waitpid(pid, 0)
    return out


def test_a_forked_child_cannot_use_its_parents_connection():
    # Without the guard the child shares the parent's session stream: either
    # process may read the other's events, so a call hangs or the parent loses
    # an outcome, depending on timing.
    out = _in_forked_child(lambda: total.remote(np.ones(10)).result(timeout=20))
    assert "CH113" in out, out


def test_a_forked_child_dropping_a_put_value_leaves_it_to_the_parent():
    held = [ch.put(np.ones(1 << 20))]
    files = list(held[0]._owned or ())
    assert files

    def drop() -> str:
        held.clear()
        gc.collect()
        return "dropped"

    assert _in_forked_child(drop) == "dropped"
    assert all(os.path.exists(f) for f in files), "the child's copy deleted the parent's stored value"
    assert total.remote(held[0]).result(timeout=60) == float(1 << 20)


def test_a_cuda_tensor_is_rebuilt_where_its_reader_is(monkeypatch):
    """torch's own pickling names the writer's CUDA device, which fails to load
    where there is no GPU and may be missing or wrong where others are visible.
    With a stand-in torch module (no GPU here): the tensor travels as a host
    array and is rebuilt on the reader's device."""

    class Tensor:
        requires_grad = False
        is_sparse = False

        def __init__(self, arr, where="cpu"):
            self.arr, self.where = arr, where
            self.device = types.SimpleNamespace(type=where.split(":")[0])

        def detach(self):
            return self

        def cpu(self):
            return Tensor(self.arr)

        def contiguous(self):
            return self

        def is_contiguous(self):
            return True

        def numpy(self):
            assert self.where == "cpu", "a CUDA tensor has no array view"
            return self.arr

        def to(self, where):
            return Tensor(self.arr, where)

        def __reduce_ex__(self, protocol):
            raise AssertionError("pickled by torch itself, naming the writer's device")

    gpu = {"available": False}
    fake = types.ModuleType("torch")
    fake.Tensor, fake.from_numpy = Tensor, Tensor
    fake.cuda = types.SimpleNamespace(is_available=lambda: gpu["available"])
    monkeypatch.setitem(sys.modules, "torch", fake)
    monkeypatch.delenv("CHIMAERA_DEVICES", raising=False)
    monkeypatch.setattr(_values, "_device_warned", set())
    sent = Tensor(np.arange(1 << 18, dtype=np.float64), "cuda:3")

    def received():
        data, _ = _serde.dumps(sent)
        return _serde.loads(data)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        got = received()  # a reader without a GPU
    assert got.where == "cpu" and np.array_equal(got.arr, sent.arr)
    assert any("CH112" in str(w.message) for w in caught)

    monkeypatch.setenv("CHIMAERA_DEVICES", json.dumps([{"kind": "gpu", "torch": "cuda:0"}]))
    assert received().where == "cuda:0", "a call granted a GPU gets it there, not on the writer's cuda:3"

    monkeypatch.delenv("CHIMAERA_DEVICES")
    gpu["available"] = True
    assert received().where == "cuda", "a driver with a GPU gets it on its current one"


def test_an_instance_of_a_class_defined_in_the_script_reaches_the_task():
    from conftest import run_script

    r = run_script(
        """
        import dataclasses
        import chimaera as ch

        @dataclasses.dataclass
        class Point:
            x: int
            y: int

        @ch.remote
        def norm(p):
            return p.x * p.x + p.y * p.y

        print(norm.remote(Point(3, 4)).result(timeout=60))
        print(norm.remote(ch.put(Point(6, 8))).result(timeout=60))
        """
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.split() == ["25", "100"]


def test_a_finished_runs_stored_arguments_go_and_its_results_stay():
    from conftest import run_script

    r = run_script(
        """
        import os
        import numpy as np
        import chimaera as ch
        from chimaera._runtime import Runtime

        @ch.remote
        def scaled(a):
            return np.asarray(a)[: 1 << 18] * 2  # 2 MiB: stored as a file

        a = np.ones(2 << 20)  # 16 MiB argument: stored once, by content
        assert scaled.remote(a).result(timeout=60)[0] == 2.0
        rt = Runtime.get()
        print(os.path.join(rt.object_dir, rt.run.hex()))
        print(rt.run.hex())
        """
    )
    assert r.returncode == 0, r.stderr
    run_dir, run_id = r.stdout.split()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and any(not n.startswith(run_id[:8]) for n in os.listdir(run_dir)):
        time.sleep(0.1)
    left = os.listdir(run_dir)
    assert left and all(n.startswith(run_id[:8]) for n in left), f"stored argument outlived its run: {left}"
    outcomes = list(ch.runs.attach(run_id).outcomes())
    assert [float(v[0]) for _, v, e in outcomes if e is None] == [2.0], "the result is still readable"


def test_a_put_value_a_call_returned_outlives_its_run_like_the_result():
    # The result is a handle to a value the call stored with ch.put: when the
    # run ends, the peer deletes the run's values but keeps its results, so
    # the value must be kept with them.
    from conftest import run_script

    r = run_script(
        """
        import os
        import numpy as np
        import chimaera as ch
        from chimaera._runtime import Runtime

        @ch.remote
        def make():
            return ch.put(np.full(1 << 18, 3.0))  # 2 MiB: a file, stored inside the call

        v = make.remote().result(timeout=60)
        v = v.result(timeout=60) if hasattr(v, "result") else v
        assert v[0] == 3.0
        rt = Runtime.get()
        print(os.path.join(rt.object_dir, rt.run.hex()))
        print(rt.run.hex())
        """
    )
    assert r.returncode == 0, r.stderr
    run_dir, run_id = r.stdout.split()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and any(not n.startswith(run_id[:8]) for n in os.listdir(run_dir)):
        time.sleep(0.1)  # the run's end reaches the peer, which deletes its values
    handles = [v for _, v, e in ch.runs.attach(run_id).outcomes() if e is None]
    assert len(handles) == 1, handles
    value = handles[0].result(timeout=60) if hasattr(handles[0], "result") else handles[0]
    assert float(value[0]) == 3.0, "the value the result names was deleted with the run's values"


def test_a_full_disk_is_named_in_the_error(tmp_path, monkeypatch):
    # Objects are written natively (objects.rs), which also removes a partial
    # file on failure; what reaches the caller is checked here.
    import errno

    from chimaera import _serde

    def full_disk(path, parts):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(_serde._native, "write_object", full_disk, raising=False)
    (tmp_path / "run").mkdir()
    target = tmp_path / "run" / "object"
    with pytest.raises(OSError, match=r"no space left on the disk holding .*run \(\d+ MiB free\) for a 3 MiB object"):
        _serde.write_file(str(target), [b"x" * (1 << 20), b"y" * (2 << 20)])


def test_a_put_value_is_freed_once_its_readers_finish_and_its_future_is_dropped():
    import gc

    before = _files()
    x = ch.put(np.ones(1 << 20))
    stored = _files() - before
    assert stored, "the value is stored as a file"
    assert [total.remote(x).result(timeout=60) for _ in range(3)] == [float(1 << 20)] * 3
    assert stored <= _files(), "kept while the future lives"
    del x
    gc.collect()
    deadline = time.monotonic() + 10
    while stored & _files() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not stored & _files(), "freed once nothing refers to it and its readers finished"


def test_a_put_value_embedded_deeper_than_one_level_survives_its_future():
    import gc

    @ch.remote
    def first_total(d):
        return float(d["xs"][0].result().sum())

    before = _files()
    x = ch.put(np.ones(1 << 20))
    stored = _files() - before
    # Two levels deep it travels pickled, as a handle the call may keep: it
    # stays for the run.
    assert first_total.remote({"xs": [x]}).result(timeout=60) == float(1 << 20)
    del x
    gc.collect()
    time.sleep(1.0)
    assert stored <= _files(), "a value that escaped as a handle is kept for the run"


def test_another_program_cannot_release_this_runs_values():
    from conftest import run_script

    x = ch.put(np.ones(1 << 20))
    run = Runtime.get().run.hex()
    names = [os.path.basename(p) for p in x._owned]
    r = run_script(
        f"""
        import time
        import chimaera as ch
        from chimaera._runtime import Runtime

        ch.init()
        rt = Runtime.get()
        rt.client.release_objects(bytes.fromhex({run!r}), [bytes.fromhex(n) for n in {names!r}])
        rt.client.flush()
        time.sleep(0.5)
        """
    )
    assert r.returncode == 0, r.stderr
    time.sleep(0.5)
    assert all(os.path.exists(os.path.join(_run_dir(), n)) for n in names), "another run's client deleted this run's value"
    assert total.remote(x).result(timeout=60) == float(1 << 20)


def test_an_argument_that_cannot_be_sent_is_named_and_open_files_are_refused(tmp_path):
    import threading

    @ch.remote
    def size(data, *, label="x"):
        return len(data)

    with pytest.raises(ch.SerializationError, match=r"size .*argument 'label'.*cannot be sent"):
        size.remote([1, 2], label=threading.Lock())
    path = tmp_path / "notes.txt"
    path.write_text("x" * 1000)
    with open(path) as f, pytest.raises(ch.SerializationError, match=r"argument 'data'.*open file.*pass its path"):
        size.remote(f)  # used to read the file here and send a copy of its text


def test_a_call_with_the_wrong_arguments_names_the_function():
    @ch.remote
    def pair(a, b):
        return a + b

    with pytest.raises(TypeError, match=r"pair\(\): .*missing a required argument"):
        pair.remote(1)
    with pytest.raises(TypeError, match=r"unpack a list: ch.gather\(\*futures\)"):
        ch.gather([pair.remote(1, 2)])


def test_a_shared_cuda_tensor_is_read_directly_only_on_its_own_machine():
    # A put CUDA tensor carries an IPC handle for readers on its machine. One
    # on another machine (another boot id), or its owner itself (which cannot
    # open its own handle), reads the host copy instead; neither touches CUDA.
    share = {"host": "another-machine", "pid": os.getpid() + 1, "gpu": "GPU-x", "args": ()}
    assert _values._from_cuda_share(share, None) is None
    here = _values._boot_id()
    assert _values._from_cuda_share({**share, "host": here, "pid": os.getpid()}, None) is None
    if here:  # Linux: a share from a process that is gone falls back too
        assert _values._from_cuda_share({**share, "host": here}, None) is None


def test_a_large_plain_dict_round_trips_through_put_an_argument_and_a_result():
    # Encoded by the native plain-data codec (see test_plain) on every hop.
    d = {f"k{i}": [i, float(i) / 3, str(i), None, i % 2 == 0, (i, b"x")] for i in range(20_000)}
    assert ch.put(d).result() == d
    assert length.remote(d).result(timeout=60) == len(d)
    back = echo.remote(d).result(timeout=60)
    assert back == d and type(back["k1"][4]) is bool and type(back["k1"][5]) is tuple


class _DriverA:
    """Another program (driver A, a run of its own): it stores `held` (64 MiB)
    and `loose` (both escaped, so only their run's end frees them), runs
    `handover` to pass `held` on, and exits when told to."""

    CODE = """
import os, pickle, sys
import numpy as np
import chimaera as ch

held = ch.put(np.full(1 << 23, 7.0))
loose = ch.put(np.full(1 << 20, 1.0))
pickle.dumps(loose)
{handover}
names = lambda f: [os.path.basename(p) for p in f._owned]
print(held._put[0].hex(), ",".join(names(held)), ",".join(names(loose)), flush=True)
sys.stdin.readline()
"""

    def __init__(self, handover: str) -> None:
        import subprocess

        from conftest import REPO

        env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"))
        code = self.CODE.format(handover=handover)
        pipe = subprocess.PIPE
        self.proc = subprocess.Popen([sys.executable, "-c", code], stdin=pipe, stdout=pipe, text=True, env=env)
        self.run, self.held, self.loose = self.proc.stdout.readline().split()
        assert all(self.exists(self.held)) and all(self.exists(self.loose)), (self.held, self.loose)

    def exists(self, names: str) -> list[bool]:
        return [os.path.exists(os.path.join(Runtime.get().object_dir, self.run, n)) for n in names.split(",")]

    def exit(self) -> None:
        """A exits: its run ends, and its values with it, but not `held`."""
        self.proc.communicate("go\n", timeout=60)
        deadline = time.monotonic() + 60
        while any(self.exists(self.loose)) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not any(self.exists(self.loose)), "A's run ended, and its values with it"
        assert all(self.exists(self.held)), "a value another run holds stays"

    def stored(self) -> int:
        """Bytes the peer stores for A's run (``chimaera objects --json``)."""
        import subprocess

        from conftest import BIN

        out = subprocess.run([BIN, "--dir", Runtime.get().home, "objects", "--json"], capture_output=True, text=True)
        return sum(r["memory_bytes"] + r["disk_bytes"] for r in json.loads(out.stdout)["runs"] if r["run"] == self.run)

    def wait_reclaimed(self) -> None:
        deadline = time.monotonic() + 60
        while self.stored() and time.monotonic() < deadline:
            time.sleep(0.2)
        assert self.stored() == 0, "the last holder let go: the peer reclaims the value"
        assert not any(self.exists(self.held))


def test_a_value_another_driver_holds_outlives_its_driver_until_released(tmp_path):
    # This driver (B, another run) unpickles A's handle, and so holds the
    # value: it reads it after A exits, and once it lets go the peer reclaims it.
    import pickle

    handle = tmp_path / "handle"
    a = _DriverA(f"open({str(handle)!r}, 'wb').write(pickle.dumps(held))")
    h = pickle.loads(handle.read_bytes())
    a.exit()
    v = h.result(timeout=60)
    assert v.shape == (1 << 23,) and float(v.sum()) == 7.0 * (1 << 23)
    del v, h
    gc.collect()
    a.wait_reclaimed()


def test_a_value_held_again_as_its_last_handle_goes_stays_held(tmp_path):
    # A dropped handle's release is sent without waiting, and unpickling the
    # value again sends a hold straight after it: the peer commits the two in
    # that order, so each of these values outlives A.
    import pickle

    from chimaera import _leases

    handle = tmp_path / "handles"
    a = _DriverA(
        "many = [ch.put(np.full(1 << 13, float(i))) for i in range(64)]\n"
        f"open({str(handle)!r}, 'wb').write(pickle.dumps((held, [pickle.dumps(v) for v in many])))"
    )
    h, many = pickle.loads(handle.read_bytes())
    kept = []
    for data in many:
        pickle.loads(data)  # held, and let go at once
        kept.append(pickle.loads(data))
    assert len(_leases._releases) == 1 + len(kept), "each dropped handle let go as it went"
    a.exit()
    assert [float(k.result(timeout=60)[0]) for k in kept] == [float(i) for i in range(64)]
    del h, kept
    gc.collect()
    a.wait_reclaimed()


def test_an_actor_holds_a_value_another_run_passed_it_until_it_lets_go():
    # The actor is this run's; A's call hands it a handle to A's value, which
    # it keeps past A's end.
    @ch.remote
    class Keeper:
        def keep(self, box):
            self.box = box

        def total(self) -> float:
            return float(self.box[0].result().sum())

        def drop(self) -> None:
            self.box = None

    k = Keeper.options(name="test_values/keeper").remote()
    a = _DriverA('ch.actor("test_values/keeper").keep.remote(ch.handles([held])).result(timeout=60)')
    a.exit()
    assert k.total.remote().result(timeout=60) == 7.0 * (1 << 23)
    k.drop.remote().result(timeout=60)
    a.wait_reclaimed()


@pytest.mark.parametrize("error", [ConnectionError, ValueError, RuntimeError, FileNotFoundError])
def test_a_handle_whose_hold_is_refused_still_unpickles(monkeypatch, error):
    # The peer refused the hold: the value is gone (DataLost when read), or
    # the hold failed for now (a leader change, a session not open yet) and
    # the handle reads the value, unheld, while its run lasts.
    from chimaera import _leases

    class Peer:
        run = os.urandom(16)  # the current run: not the handle's

        def __init__(self) -> None:
            self.client = self

        def borrow(self, run: bytes, files: list[bytes], release: bool = False) -> None:
            raise error("refused")

    resolved = _leases._resolved_future

    def unheld(*put):
        f = resolved(*put)
        f._rt = Peer()
        return f

    monkeypatch.setattr(_leases, "_resolved_future", unheld)
    put = (os.urandom(16), os.urandom(16), None, 1 << 20, (os.urandom(16),), "")
    if error is FileNotFoundError:
        f = _leases.unpickle(*put)
        assert isinstance(f._exc, ch.errors.DataLost)
    else:
        with pytest.warns(UserWarning, match="not held past its run's end"):
            f = _leases.unpickle(*put)
        assert f._exc is None
    assert f not in _leases._releases, "nothing to let go of"


@pytest.mark.parametrize("passed", ["argument", "handles"])
def test_a_held_value_passed_to_a_call_stays_until_the_call_has_read_it(tmp_path, passed):
    # Driver B holds A's value and, once A has exited, passes it to a call of
    # its own that starts after B dropped its handle: as an argument (the call
    # reads A's files) or inside ch.handles (pickled again). The call reads
    # it, and the peer reclaims the value once B ends.
    import pickle
    import subprocess

    from conftest import REPO

    handle = tmp_path / "handle"
    a = _DriverA(f"open({str(handle)!r}, 'wb').write(pickle.dumps(held))")
    arg = "h" if passed == "argument" else "ch.handles([h])"
    code = f"""
import gc, pickle, sys, time
import chimaera as ch

@ch.remote
def pause():
    time.sleep(2)

@ch.remote
def argument(_, x):
    return float(x.sum())

@ch.remote
def handles(_, box):
    return float(box[0].result().sum())

h = pickle.loads(open({str(handle)!r}, "rb").read())
print("HELD", flush=True)
sys.stdin.readline()
out = {passed}.remote(pause.remote(), {arg})
del h
gc.collect()
print("READ", out.result(timeout=60), flush=True)
"""
    env = dict(os.environ, PYTHONPATH=os.path.join(REPO, "python"))
    pipe = subprocess.PIPE
    b = subprocess.Popen([sys.executable, "-c", code], stdin=pipe, stdout=pipe, stderr=pipe, text=True, env=env)
    assert b.stdout.readline().strip() == "HELD", b.stderr.read()[-3000:]
    a.exit()
    out, err = b.communicate("go\n", timeout=120)
    assert f"READ {7.0 * (1 << 23)}" in out, out + err[-3000:]
    a.wait_reclaimed()


def test_object_locations_name_the_peer_storing_each_value():
    import numpy as np

    @ch.remote
    def big():
        return np.ones(1 << 20)  # 8 MiB

    f = big.remote()
    f.result(timeout=60)
    p = ch.put(np.ones(1 << 18))  # 2 MiB
    small = ch.put(1)  # held inline in its handle
    me = next(n.peer for n in ch.runs.cluster().nodes() if n.this_peer)
    loc = ch.runs.object_locations([f, p, small])
    assert loc[f] == {"peers": [me], "size": loc[f]["size"], "inline": False} and loc[f]["size"] >= 8 << 20
    assert loc[p] == {"peers": [me], "size": loc[p]["size"], "inline": False} and loc[p]["size"] >= 2 << 20
    assert loc[small]["peers"] == [] and loc[small]["inline"], loc[small]


def test_a_put_above_4_kib_is_a_file_its_handle_names():
    small, mid = ch.put(b"x" * 1024), ch.put(b"x" * (64 << 10))
    assert small._put[2] is not None, "a small put travels in its handle"
    assert mid._put[2] is None, "a 64 KiB put is a file: passing its handle to calls copies no bytes"
    assert mid.result() == b"x" * (64 << 10)
