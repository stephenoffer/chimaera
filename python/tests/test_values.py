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
        sizes = set()
        for n in _files() - before:
            try:
                st = os.stat(os.path.join(_run_dir(), n))
            except OSError:
                continue  # removed meanwhile
            if st.st_size >= 2 << 20:
                sizes.add(st.st_ino)
        return len(sizes)

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
    before = _files()
    assert total.remote(t).result(timeout=60) == float(t.sum())
    assert _files() - before == {_blob_name(t.numpy())}


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
