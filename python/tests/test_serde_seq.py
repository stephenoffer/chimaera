"""Tuples stored element by element: one value of a call that returns several
is decoded without the others."""

from __future__ import annotations

import numpy as np
import pytest

import chimaera as ch
from chimaera import _serde


def _bytes(parts):
    return b"".join(bytes(p) if isinstance(p, memoryview) else p for p in parts)


def test_a_stored_tuple_decodes_whole_or_one_value_at_a_time(tmp_path):
    value = ("small", list(range(1000)), np.arange(200_000, dtype=np.float64), {"k": (1, 2)}, b"x" * 70_000)
    parts, size, oob = _serde.dumps_seq(value)
    assert len(_bytes(parts)) == size and oob == 2  # the array and the large bytes travel out of band
    path = tmp_path / "obj"
    path.write_bytes(_bytes(parts))
    data = _serde._map(str(path))  # as executors read stored objects: page-aligned
    whole = _serde.loads(data)
    assert isinstance(whole, tuple) and len(whole) == len(value)
    for i, expected in enumerate(value):
        got = _serde.loads_part(data, i, len(value))
        if isinstance(expected, np.ndarray):
            assert np.array_equal(got, expected) and got.ctypes.data % 64 == 0, "buffers stay aligned"
        else:
            assert got == expected == whole[i]


def test_one_value_of_a_stored_tuple_is_read_without_the_rest(tmp_path):
    value = ("small", list(range(1000)), np.arange(200_000, dtype=np.float64), {"k": (1, 2)}, b"x" * 70_000)
    parts, _, _ = _serde.dumps_seq(value)
    path = tmp_path / "obj"
    path.write_bytes(_bytes(parts))
    for i, expected in enumerate(value):
        got = _serde.read_part(str(path), i, len(value))
        if isinstance(expected, np.ndarray):
            assert np.array_equal(got, expected) and got.ctypes.data % 64 == 0, "buffers stay aligned"
            got[0] = 1.0  # writable, as a mapped copy-on-write value is
        else:
            assert got == expected
    single = tmp_path / "single"
    single.write_bytes(_bytes(_serde.dumps_parts((1, 2, 3))[0]))
    assert _serde.read_part(str(single), 2, 3) == 3  # not stored element by element: decoded whole


def test_stored_bytes_read_in_one_copy_and_other_files_as_before(tmp_path):
    values = [b"z" * ((_serde._READ_MAX) + 4096), b"tiny", bytearray(b"q" * 100_000), {"a": [1, 2]}, "s" * 70_000]
    for i, v in enumerate(values):
        path = tmp_path / f"o{i}"
        path.write_bytes(_bytes(_serde.dumps_parts(v)[0]))
        got = _serde.read_file(str(path))
        assert type(got) is type(v) and got == v


def test_a_plain_object_still_gives_its_parts():
    parts, _, _ = _serde.dumps_parts((1, 2, 3))
    assert _serde.loads_part(_bytes(parts), 1, 3) == 2
    with pytest.raises(ValueError, match="returns=4"):
        _serde.loads_part(_bytes(parts), 1, 4)


def test_split_values_reach_their_calls():
    @ch.remote(returns=3)
    def three(n):
        return np.ones(n), list(range(n)), "tag"

    @ch.remote
    def size_of(x):
        return len(x)

    a, b, c = three.remote(100_000).split()
    assert ch.get([size_of.remote(a), size_of.remote(b), size_of.remote(c)], timeout=60) == [100_000, 100_000, 3]
    assert c.result(timeout=60) == "tag"
