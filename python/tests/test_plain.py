"""The native codec for large plain containers (_native.plain_encode): what it
takes, what it declines (pickled instead), and that both round-trip exactly."""

from __future__ import annotations

import collections
import datetime
import math
import struct

import pytest

from chimaera import _native, _serde

BIG = 1100  # past _serde._FAST_MIN, where the codec is tried


def _same(a, b) -> bool:
    """Equal, with the same types throughout (True is not 1, a tuple is not a
    list), and NaN and -0.0 compared by their bits."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float):
        return struct.pack("<d", a) == struct.pack("<d", b)
    if isinstance(a, dict):
        return list(a) == list(b) and all(_same(a[k], b[k]) for k in a) and all(
            type(x) is type(y) for x, y in zip(a, b)
        )
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    return a == b


PLAIN = [
    {f"key-{i:06d}": i * 0.5 for i in range(BIG)},
    [i - BIG // 2 for i in range(BIG)] + [2**63 - 1, -(2**63)],
    [True, False, None, 0, 1, b"", b"\x00\xff" * 40, "", "é", "中文", "🚀"] * 100,
    [0.0, -0.0, math.inf, -math.inf, math.nan, 1e-310, 5e-324] * 160,
    tuple(range(BIG)),
    {(1, "a"): [1, {"x": None}], 3: (), None: {}, True: "t", 2.5: b"b", "nested": [[[]]]} | {i: str(i) for i in range(BIG)},
    [{"id": i, "tags": ["a", "b"], "w": (i, i + 0.5)} for i in range(BIG)],
    # The edges of each int and str size (1 and 4 byte ints, 1 byte lengths).
    [-129, -128, 127, 128, 2**31 - 1, 2**31, -(2**31), -(2**31) - 1] * 140,
    ["a" * 255, "a" * 256, "é" * 127, "é" * 128, "x" * 70_000] * 230,
]


@pytest.mark.parametrize("value", PLAIN, ids=lambda v: type(v).__name__)
def test_plain_values_round_trip_exactly_through_the_codec_and_the_object_format(value):
    encoded = _native.plain_encode(value)
    assert encoded is not None
    assert _same(_native.plain_decode(encoded), value)
    parts, size, oob = _serde.dumps_parts(value)
    frame = b"".join(bytes(p) for p in parts)
    assert len(frame) == size
    assert _same(_serde.loads(frame), value)


def test_a_large_plain_value_travels_out_of_band_not_in_the_pickle_stream():
    value = {f"key-{i:06d}": float(i) for i in range(50_000)}
    parts, _, oob = _serde.dumps_parts(value)
    assert oob == 1
    head = bytes(parts[0])
    main_len = _serde._HEAD.unpack_from(head, 0)[4]
    assert main_len < 200, main_len  # only the call that rebuilds it from its buffer


shared = [1, 2]
cyclic: list = [0] * BIG
cyclic.append(cyclic)
deep: list = []
node = deep
for _ in range(300):
    node.append([])
    node = node[0]


class Text(str):
    pass


DECLINED = {
    "an int past 64 bits": [2**64] + list(range(BIG)),
    "a lone surrogate": ["\ud800"] + [str(i) for i in range(BIG)],
    "a container reachable twice": [shared, shared] * (BIG // 2),
    "a cycle": cyclic,
    "nesting past the limit": [deep] + list(range(BIG)),
    "a dict subclass": collections.OrderedDict((i, i) for i in range(BIG)),
    "a defaultdict": collections.defaultdict(int, {i: i for i in range(BIG)}),
    "a str subclass": [Text("x")] + list(range(BIG)),
    "a bytearray": [bytearray(b"x")] + list(range(BIG)),
    "a set": [{1}] + list(range(BIG)),
    "a datetime": [datetime.date(2026, 11, 22)] + list(range(BIG)),
}


@pytest.mark.parametrize("why", list(DECLINED))
def test_anything_else_is_declined_and_pickled_as_before(why):
    value = DECLINED[why]
    assert _native.plain_encode(value) is None, why
    if why == "a cycle":
        got = _serde.loads(b"".join(bytes(p) for p in _serde.dumps_parts(value)[0]))
        assert got[-1] is got and len(got) == len(value)
        return
    got = _serde.loads(b"".join(bytes(p) for p in _serde.dumps_parts(value)[0]))
    assert got == value and type(got) is type(value)


def test_a_shared_container_keeps_its_identity_through_the_pickle_fallback():
    value = DECLINED["a container reachable twice"]
    got = _serde.loads(b"".join(bytes(p) for p in _serde.dumps_parts(value)[0]))
    assert got[0] is got[1]


@pytest.mark.parametrize(
    "data, why",
    [
        (b"", "it ends early"),
        (b"\x03\x00", "an unknown version"),
        (b"\x01\x00\x00", "bytes follow the value"),
        (b"\x01\x05\x10\x00\x00\x00ab", "it ends early"),
        (b"\x01\x07\xff\xff\xff\x7f", "a count is larger than the data"),
        (b"\x01\x63", "an unknown tag"),
        (b"\x01\x09\x01\x00\x00\x00\x07\x00\x00\x00\x00\x00", "unhashable"),
    ],
)
def test_corrupt_input_is_refused_with_an_error_not_a_crash(data, why):
    with pytest.raises((ValueError, TypeError), match=why):
        _native.plain_decode(data)


def test_decoding_reads_from_any_contiguous_buffer():
    value = list(range(BIG))
    encoded = _native.plain_encode(value)
    assert _native.plain_decode(memoryview(encoded)) == value
    assert _native.plain_decode(bytearray(encoded)) == value


def test_with_an_external_store_the_codec_bytes_stay_in_the_values_own_file():
    # They are the value's size: what promotes a call argument to a blob of
    # its own, and what the CH118 lint measures.
    value, offered = list(range(200_000)), []
    parts, size, oob = _serde.dumps_parts(value, external=lambda v: offered.append(v.nbytes))
    assert offered == [] and oob == 1 and size > len(_native.plain_encode(value))


def _roundtrip(value):
    return _serde.loads(b"".join(bytes(p) for p in _serde.dumps_parts(value)[0]))


def test_a_column_shares_each_repeated_string_as_picklememo_would():
    s, t = "x" * 40, "y" * 300
    items = [s, t, None, 1.5, s, t, b"b", 7] * 200
    got = _native.plain_decode(_native.plain_encode_column(items))
    assert got == items
    assert got[0] is got[4] and got[1] is got[5]  # one object each, as written


def test_a_column_holds_atoms_only():
    assert _native.plain_encode_column([[1]] + list(range(BIG))) is None
    assert _native.plain_encode_column(tuple(range(BIG))) is None
    assert _native.plain_encode_column({"a": 1}) is None


@pytest.mark.parametrize("order", ["C", "F"])
def test_object_arrays_of_plain_values_round_trip_with_shape_order_and_sharing(order):
    np = pytest.importorskip("numpy")
    words = ["NY", "CA", "TX", None, 2.5]
    a = np.array([words[i % 5] for i in range(4000)], dtype=object).reshape((40, 100), order=order)
    got = _roundtrip(a)
    assert got.dtype == object and got.shape == a.shape and (got == a).all()
    assert got.flags.f_contiguous == a.flags.f_contiguous
    flat = got.ravel(order="K")
    assert flat[0] is flat[5] and flat[1] is flat[6], "repeated strings stay one object"


def test_other_arrays_reduce_as_numpys_own():
    np = pytest.importorskip("numpy")
    import decimal

    nums = np.arange(5000, dtype=np.float64)
    assert (_roundtrip(nums) == nums).all()
    mixed = np.array([decimal.Decimal(1)] + list(range(BIG)), dtype=object)
    got = _roundtrip(mixed)
    assert got.dtype == object and list(got) == list(mixed)


def test_a_pandas_frame_with_an_object_column_round_trips():
    pd = pytest.importorskip("pandas")
    # An object column, as pandas before 3 makes for strings (pandas 3 makes a
    # string dtype column by default, which is not an object array).
    s = pd.Series([f"s{i % 7}" for i in range(5000)], dtype=object)
    df = pd.DataFrame({"id": range(5000), "x": [i / 3 for i in range(5000)], "s": s})
    assert df["s"].dtype == object
    got = _roundtrip(df)
    assert got.equals(df) and got["s"].dtype == object


@pytest.mark.parametrize(
    "data, why",
    [
        (b"\x01\x0d\x00\x00\x00\x00", "a string reference in version 1"),
        (b"\x02\x07\x01\x00\x00\x00\x0d\x00\x00\x00\x00", "a reference to a string not yet read"),
        (b"\x02\x09\x00\x00\x00\x00", "a column that is not a list"),
        (b"\x02\x07\x01\x00\x00\x00\x07\x00\x00\x00\x00", "a container inside a column"),
    ],
)
def test_corrupt_string_references_are_refused(data, why):
    with pytest.raises(ValueError, match=why):
        _native.plain_decode(data)
