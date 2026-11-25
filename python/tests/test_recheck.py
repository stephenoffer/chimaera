"""A writable array passed to calls again is compared with its stored blob
instead of hashed again (_recheck), and a change is never missed."""

from __future__ import annotations

import mmap
import os

import numpy as np

import chimaera as ch
from chimaera import _recheck, _values


@ch.remote
def at(a, i: int) -> float:
    return float(a[i])


def _outside_the_arena(n: int) -> np.ndarray:
    # An anonymous mapping is never an arena allocation, so on Linux too the
    # array is stored by content rather than linked.
    return np.frombuffer(mmap.mmap(-1, n * 8), dtype=np.float64)


def test_an_array_changed_in_place_is_seen_by_the_next_call():
    a = _outside_the_arena(1 << 20)  # 8 MiB, writable
    assert at.remote(a, 0).result(timeout=60) == 0.0
    assert at.remote(a, 0).result(timeout=60) == 0.0
    a[0] = 1.0
    assert at.remote(a, 0).result(timeout=60) == 1.0
    a[-1] = 5.0  # only the last bytes differ
    assert at.remote(a, -1).result(timeout=60) == 5.0
    a[0], a[-1] = 0.0, 0.0  # back to bytes stored before
    assert [at.remote(a, i).result(timeout=60) for i in (0, -1)] == [0.0, 0.0]


def test_passing_the_same_array_again_hashes_it_once(tmp_path, monkeypatch):
    hashed: list[int] = []
    real = _values.blake3_buffer
    monkeypatch.setattr(_values, "blake3_buffer", lambda v: hashed.append(v.nbytes) or real(v))
    monkeypatch.setattr(_recheck, "SPLIT_BYTES", 512 << 10)  # four threads compare a piece each
    sink = _values.LocalBlobSink(str(tmp_path))
    run = os.urandom(16)
    a = _outside_the_arena(1 << 18)  # 2 MiB

    def blob() -> bytes:
        (bid,) = _values.encode_frame(((a,), {}), run, sink).blobs
        return bid

    first = blob()
    assert [blob() for _ in range(4)] == [first] * 4
    assert hashed == [a.nbytes], "compared with the stored blob, not hashed again"
    a[len(a) // 3] = 3.0  # inside the second piece
    middle = blob()
    assert middle != first and len(hashed) == 2
    a[-1] = 7.0  # inside the last
    changed = blob()
    assert changed not in (first, middle) and len(hashed) == 3
    with open(_values.blob_path(str(tmp_path), run, changed), "rb") as f:
        assert f.read() == a.tobytes()
    # A freed blob is gone from the store: hash and store the bytes again.
    sink.forget(run, changed)
    os.remove(_values.blob_path(str(tmp_path), run, changed))
    assert blob() == changed and len(hashed) == 4
    assert os.path.exists(_values.blob_path(str(tmp_path), run, changed))
