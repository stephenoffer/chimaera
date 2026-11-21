"""Large values: stored once by content, read zero-copy and writable, released
when the call that needed them ends."""

from __future__ import annotations

import os
import time
import warnings

import numpy as np
import pytest

import chimaera as ch
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
    outcomes = list(ch.attach(run_id).outcomes())
    assert [float(v[0]) for _, v, e in outcomes if e is None] == [2.0], "the result is still readable"


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
