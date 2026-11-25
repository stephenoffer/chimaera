"""Partitioned data: ch.data.partitions, reduce_by_key (the shuffle contract) and sinks."""

from __future__ import annotations

import collections
import copy
import datetime
import decimal
import errno
import fcntl
import hashlib
import json
import operator
import os
import random
import signal
import struct
import tempfile
import time
import tracemalloc
import types

import pytest

import chimaera as ch
from chimaera import ext

LINES = ["the quick brown fox", "jumps over the lazy dog", "the dog barks", "a fox again"] * 25


@ch.remote
def count_words(partition):
    return collections.Counter(w for line in partition for w in line.split()).items()


def test_word_count_matches_a_serial_count():
    totals = count_words.map(ch.data.partitions(LINES, n=7)).reduce_by_key(operator.add, partitions=3)
    assert isinstance(totals, ch.data.Partitioned) and len(totals) == 3
    parts = list(totals)  # one dict per partition
    assert len(parts) == 3 and all(type(p) is dict for p in parts)
    assert sum(len(p) for p in parts) == len(totals.to_dict())
    assert totals.to_dict() == collections.Counter(w for line in LINES for w in line.split())


@ch.remote
def pairs(seed):
    rng = random.Random(seed)
    return [(f"k{rng.randrange(5000)}", rng.randrange(100)) for _ in range(1000)]


def test_a_larger_shuffle_matches_the_serial_reference():
    seeds = range(60)
    expected: dict[str, int] = collections.defaultdict(int)
    for s in seeds:
        for k, v in pairs(s):
            expected[k] += v
    got = pairs.map(seeds).reduce_by_key(operator.add, partitions=8)
    assert got.to_dict() == dict(expected)
    # Every key lands in exactly one partition.
    seen = [k for f in got.futures for k in f.result()]
    assert len(seen) == len(set(seen)) == len(expected)


def test_without_combine_values_are_grouped_into_lists():
    got = pairs.map(range(5)).reduce_by_key(partitions=4).to_dict()
    expected: dict[str, list[int]] = collections.defaultdict(list)
    for s in range(5):
        for k, v in pairs(s):
            expected[k].append(v)
    assert {k: sorted(v) for k, v in got.items()} == {k: sorted(v) for k, v in expected.items()}


@ch.remote
def crashy_pairs(seed, marker_dir):
    marker = os.path.join(marker_dir, f"crashed-{seed}")
    if seed == 3 and not os.path.exists(marker):
        open(marker, "w").close()
        os._exit(1)  # this worker dies once, mid-run
    return pairs(seed)


def test_a_mapper_killed_mid_run_still_gives_the_same_result():
    marker_dir = tempfile.mkdtemp()
    got = crashy_pairs.map(range(10), [marker_dir] * 10).reduce_by_key(operator.add, partitions=5).to_dict()
    expected = pairs.map(range(10)).reduce_by_key(operator.add, partitions=5).to_dict()
    assert got == expected
    assert os.path.exists(os.path.join(marker_dir, "crashed-3")), "the crash must have happened"


@ch.remote
def names(partition):
    return [row["name"] for row in partition]


def test_file_sources_project_and_filter():
    d = tempfile.mkdtemp()
    for i in range(3):
        with open(os.path.join(d, f"people-{i}.jsonl"), "w") as f:
            for j in range(4):
                f.write(json.dumps({"name": f"p{i}{j}", "age": 20 + 10 * j, "city": "x"}) + "\n")
    with open(os.path.join(d, "more.csv"), "w") as f:
        f.write("name,age\nc1,50\nc2,10\n")
    parts = ch.data.partitions(os.path.join(d, "*.jsonl"), columns=["name"], filter=[("age", ">=", 40)])
    assert len(parts) == 3
    assert sorted(n for ns in names.map(parts) for n in ns) == ["p02", "p03", "p12", "p13", "p22", "p23"]
    rows = list(ch.data.partitions(os.path.join(d, "*.jsonl"), columns=["name", "age"])[0].rows())
    assert rows[0] == {"name": "p00", "age": 20}
    young = ch.data.partitions(os.path.join(d, "*.csv"), filter=lambda r: int(r["age"]) < 30)
    assert list(names.map(young)) == [["c2"]]
    with pytest.raises(FileNotFoundError):
        ch.data.partitions(os.path.join(d, "*.nothing"))


def _rows_of(manifest):
    return [json.loads(line) for part in manifest["parts"] for line in open(part["path"])]


def _generations(out):
    """The generations a FileSink committed in `out`."""
    names = os.listdir(os.path.join(out, "_manifests"))
    return sorted(name.removesuffix(".json") for name in names if name.endswith(".json"))


def test_file_sink_writes_parts_and_a_manifest_and_a_rewrite_is_a_new_generation():
    out = tempfile.mkdtemp()
    totals = count_words.map(ch.data.partitions(LINES, n=4)).reduce_by_key(operator.add, partitions=2)
    manifest = totals.write(ext.FileSink(out)).result()
    assert manifest["rows"] == len(totals.to_dict()) and len(manifest["parts"]) == 2
    assert dict(_rows_of(manifest)) == totals.to_dict()
    again = totals.write(ext.FileSink(out)).result()  # the same rows, in a generation of its own
    assert again["generation"] > manifest["generation"] and dict(_rows_of(again)) == totals.to_dict()
    gens = [manifest["generation"], again["generation"]]
    parts = [f"part-0000{i}-{g}.jsonl" for g in gens for i in (0, 1)]
    assert sorted(os.listdir(out)) == sorted(["_manifest.json", "_manifests", *parts])
    assert _generations(out) == gens
    with open(os.path.join(out, "_manifest.json")) as f:
        assert json.load(f) == again
    # Reading the directory goes through the manifest: one generation's rows, and no manifest lines.
    read = sorted(tuple(row) for rows in keep.map(ch.data.partitions(out)) for row in rows)
    assert read == sorted(totals.to_dict().items())


def test_parquet_filters_on_numbers_push_down():
    path = _sizes_parquet()
    source = ext.FileSource(path)
    assert list(source.read(path, filter=[("size", "==", 11)])) == [{"id": 2, "size": 11}]
    assert list(source.read(path, filter=[("size", ">", 5)])) == [{"id": 2, "size": 11}]
    assert list(source.read(path, filter=[("size", "in", [3])])) == [{"id": 1, "size": 3}]


def _sizes_parquet():
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = os.path.join(tempfile.mkdtemp(), "sizes.parquet")
    pq.write_table(pa.table({"id": [1, 2], "size": [3, 11]}), path)
    return path


class Projecting(ext.Source):
    """Honors the columns hint and leaves the filter to Chimaera."""

    ROWS = [{"id": 1, "size": 3}, {"id": 2, "size": 11}]

    def partitions(self):
        return [0]

    def read(self, part, columns=None, filter=None):
        return [{c: row[c] for c in columns} if columns else row for row in self.ROWS]


def test_a_filter_on_a_column_left_out_of_columns_matches_the_unprojected_scan():
    path = _sizes_parquet()
    filters = [
        [("size", "==", 11)],
        [("size", "in", [11])],
        [("size", ">", 5), ("id", ">=", 2)],  # both pushed down
        [("size", ">", 1), ("id", "not in", [1])],  # one pushed down, one applied after reading
        lambda row: row["size"] > 5,
    ]
    for source in (path, Projecting()):
        for flt in filters:
            (whole,) = ch.data.partitions(source, filter=flt)
            (projected,) = ch.data.partitions(source, columns=["id"], filter=flt)
            assert list(projected.rows()) == [{"id": row["id"]} for row in whole.rows()] == [{"id": 2}], (source, flt)
    # Parquet applies what it pushed down exactly; only the rest is applied again.
    mixed = [("size", ">", 1), ("id", "not in", [1])]
    assert ext.FileSource(path).residual(path, mixed) == [("id", "not in", [1])]
    assert Projecting().residual(0, mixed) == mixed


# One table in each format. Row 2's fields are null and row 3 has none:
# Parquet stores both as nulls, and CSV, which has no null, as a row cut short
# after its id. CSV values are all strings, and no file has a "height". In
# Parquet, "size" is int64, "hash" uint64 and "weight" float32.
TENTH = struct.unpack("f", struct.pack("f", 0.1))[0]  # the float32 nearest 0.1, as JSON and Parquet both hold it
PEOPLE = [
    {"id": 1, "name": "ann", "age": 30, "size": 500_000, "hash": 1, "weight": TENTH},
    {"id": 2, "name": None, "age": None, "size": None, "hash": None, "weight": None},
    {"id": 3},
    {"id": 4, "name": "bob", "age": 50, "size": 40_000_000, "hash": 2**64 - 1, "weight": 2.5},
]
PEOPLE_CSV = (
    f"id,name,age,size,hash,weight\n1,ann,30,500000,1,{TENTH!r}\n2\n3\n4,bob,50,40000000,{2**64 - 1},2.5\n"
)
FILTERS = [
    # filter                       the ids kept from JSON lines and Parquet, or the error   from CSV
    (("name", "==", "ann"), [1], [1]),
    (("name", "!=", "ann"), [4], [4]),  # null compares with nothing
    (("name", "<", "b"), [1], [1]),
    (("name", "in", ["bob", None]), [2, 3, 4], [2, 3, 4]),  # in [None] keeps null
    (("name", "in", [None]), [2, 3], [2, 3]),  # a list of nulls alone, which Arrow types as null
    (("name", "in", []), [], []),
    (("name", "not in", ["bob"]), [1, 2, 3], [1, 2, 3]),
    (("name", "not in", [None]), [1, 4], [1, 4]),
    (("age", "==", 50), [4], TypeError),  # a number filter on strings raises
    (("age", ">=", 40), [4], TypeError),
    (("age", "!=", 30), [4], TypeError),
    (("age", ">", 29.5), [1, 4], TypeError),  # an int compares with a float
    (("age", "in", [30, None]), [1, 2, 3], TypeError),
    (("age", "not in", [30]), [2, 3, 4], TypeError),
    (("age", "<", 2**70), [1, 4], TypeError),  # past int64, so Parquet compares it after reading
    (("age", "<", decimal.Decimal("1E+400")), [1, 4], TypeError),  # and this, past Arrow's decimals
    (("size", ">", 1e6), [4], TypeError),  # Arrow would compare as float32, and refuse 40,000,000
    (("size", ">", 0.5), [1, 4], TypeError),
    (("hash", ">", 5), [4], TypeError),  # Arrow would compare as int64, and refuse 2**64 - 1
    (("hash", "!=", 1), [4], TypeError),
    (("hash", "in", [-1]), [], TypeError),
    (("weight", "in", [0.1]), [], TypeError),  # 0.1 is no float32, but Arrow would round it to one
    (("weight", "==", 0.1), [], TypeError),
    (("weight", ">", 0.1), [1, 4], TypeError),
    (("weight", "in", [TENTH, None]), [1, 2, 3], TypeError),
    (("age", ">=", "40"), TypeError, [4]),
    (("age", "==", True), TypeError, TypeError),  # a bool is not a number
    (("name", "in", [1]), TypeError, TypeError),
    (("height", "==", 1), [], []),  # a field no row has is null in each
    (("height", "in", [None]), [1, 2, 3, 4], [1, 2, 3, 4]),
    (("height", "not in", [1]), [1, 2, 3, 4], [1, 2, 3, 4]),
]


def _ids(read, triple):
    try:
        return [int(row["id"]) for row in read()]
    except TypeError as e:
        assert str(e).startswith(f"filter {triple!r} compares"), e  # the filter's own error, not a bug's
        return TypeError


def test_a_filter_keeps_the_same_rows_of_nulls_missing_fields_and_other_types_in_every_format():
    import pyarrow as pa
    import pyarrow.parquet as pq

    d = tempfile.mkdtemp()
    paths = {kind: os.path.join(d, f"people.{kind}") for kind in ("jsonl", "parquet", "csv")}
    with open(paths["jsonl"], "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in PEOPLE)
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("age", pa.int64()),
            ("size", pa.int64()),
            ("hash", pa.uint64()),
            ("weight", pa.float32()),
        ]
    )
    pq.write_table(pa.Table.from_pylist(PEOPLE, schema=schema), paths["parquet"])
    with open(paths["csv"], "w") as f:
        f.write(PEOPLE_CSV)
    for triple, typed, strings in FILTERS:
        for kind, path in paths.items():
            want = strings if kind == "csv" else typed
            for columns in (["id"], None):
                (part,) = ch.data.partitions(path, columns=columns, filter=[triple])
                assert _ids(part.rows, triple) == want, (kind, triple, columns)
                if kind == "parquet":
                    assert _ids(lambda: (r for b in part.batches() for r in b.to_pylist()), triple) == want, triple
    (part,) = ch.data.partitions(paths["parquet"], columns=["id", "height"])
    assert list(part.rows()) == [{"id": i, "height": None} for i in (1, 2, 3, 4)]  # as a JSON row's missing field
    # A JSON field of another type than the filter's raises when a row reaches it.
    mixed = os.path.join(d, "mixed.jsonl")
    with open(mixed, "w") as f:
        f.write('{"id": 1, "age": 30}\n{"id": 2, "age": "40"}\n')
    with pytest.raises(TypeError, match="compares str '40' in 'age' with int 20"):
        list(ch.data.partitions(mixed, filter=[("age", ">=", 20)])[0])
    with pytest.raises(TypeError, match="the int64 column 'age' with str '20'"):
        list(ch.data.partitions(paths["parquet"], filter=[("age", ">=", "20")])[0])
    # A filter no format could apply alike fails where it is made, before any read.
    for bad, error in [
        (("age", "==", None), ValueError),  # keeps nothing: null compares with nothing
        (("age", "~", 1), ValueError),
        (("age", "in", 30), TypeError),
        (("age", "in", [30, "40"]), TypeError),
        (("age", "==", [30]), TypeError),
    ]:
        with pytest.raises(error):
            ch.data.partitions(paths["jsonl"], filter=[bad])


class Listed(ext.Source):
    def __init__(self, rows):
        self.rows = rows

    def partitions(self):
        return [0]

    def read(self, part, columns=None, filter=None):
        return self.rows


def test_a_filter_on_a_dictionary_list_zoned_half_float_or_nan_column_keeps_the_rows_of_its_python_values():
    import pyarrow as pa
    import pyarrow.parquet as pq

    utc = datetime.timezone.utc
    when = datetime.datetime(2026, 1, 1, tzinfo=utc)
    naive = when.replace(tzinfo=None)

    def check(path, rows, cases):
        for triple, want in cases:
            (part,) = ch.data.partitions(path, filter=[triple])
            assert _ids(part.rows, triple) == want, triple
            assert _ids(lambda: (r for b in part.batches() for r in b.to_pylist()), triple) == want, triple
            (part,) = ch.data.partitions(Listed(rows), filter=[triple])
            assert _ids(part.rows, triple) == want, triple

    rows = [
        {"id": 1, "kind": "a", "tags": ["a"], "at": when, "half": 0.5, "tm": datetime.time(1)},
        {"id": 2, "kind": None, "tags": None, "at": None, "half": None, "tm": None},
    ]
    path = os.path.join(tempfile.mkdtemp(), "typed.parquet")
    table = pa.Table.from_pylist(rows)
    table = table.set_column(1, "kind", table["kind"].dictionary_encode())
    pq.write_table(table.set_column(4, "half", table["half"].cast(pa.float16())), path)
    stored = pq.read_schema(path)
    assert pa.types.is_dictionary(stored.field("kind").type) and pa.types.is_float16(stored.field("half").type)
    cases = [
        (("kind", "in", [None]), [2]),
        (("kind", "in", []), []),
        (("kind", "==", "a"), [1]),
        (("tags", "in", [None]), [2]),  # Arrow has no `in` for a list column: it runs on rows
        (("tags", "not in", [None]), [1]),
        (("at", ">=", when), [1]),
        (("at", "in", [None]), [2]),
        (("at", "<", naive), TypeError),  # a datetime without a time zone is another type
        (("at", "==", naive), TypeError),
        (("half", ">", 0.25), [1]),  # Arrow has no kernels for float16: it runs on rows
        (("half", "in", [None]), [2]),
        (("tm", ">=", datetime.time(1)), [1]),
        (("tm", ">", datetime.time(0, tzinfo=utc)), TypeError),  # and a time with a time zone is another
    ]
    check(path, rows, cases)
    for col, mixed in [("at", [when, naive]), ("tm", [datetime.time(1), datetime.time(1, tzinfo=utc)])]:
        with pytest.raises(TypeError, match="one kind per filter"):
            ch.data.partitions(path, filter=[(col, "in", mixed)])
    # NaN equals nothing in Python. A row group's statistics leave NaN out, so
    # they would rule the first group out of != 1.0, and Arrow's `in` matches
    # NaN in the second, whose statistics hold nothing.
    path = os.path.join(tempfile.mkdtemp(), "nan.parquet")
    rows = [{"id": 1, "x": 1.0}, {"id": 2, "x": float("nan")}, {"id": 3, "x": float("nan")}]
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=2)
    check(path, rows, [(("x", "!=", 1.0), [2, 3]), (("x", "in", [float("nan")]), []), (("x", "==", 1.0), [1])])


BATCH = 2000


def _write_rows(path, n):
    """`n` rows in the format the file's extension names."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [{"id": i, "text": f"row {i} " + "é" * 40} for i in range(n)]
    if path.endswith(".parquet"):
        pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=BATCH)
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        if path.endswith(".csv"):
            f.write("id,text\n" + "".join(f"{r['id']},{r['text']}\n" for r in rows))
        elif path.endswith(".jsonl"):
            f.write("".join(json.dumps(r) + "\n" for r in rows))
        else:
            f.write("".join(f"{r['id']} {r['text']}\n" for r in rows))


def _peak(read):
    """How many rows `read()` gives, and the most Python memory held while they are consumed one by one."""
    tracemalloc.start()
    try:
        return sum(1 for _ in read()), tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def test_a_reader_holds_one_batch_of_a_file_ten_batches_long():
    d = tempfile.mkdtemp()
    for kind in ("jsonl", "csv", "txt", "parquet"):
        one, ten = os.path.join(d, f"one.{kind}"), os.path.join(d, f"ten.{kind}")
        _write_rows(one, BATCH)
        _write_rows(ten, 10 * BATCH)
        source = ext.FileSource(os.path.join(d, f"*.{kind}"), batch_rows=BATCH)
        rows, batch = _peak(lambda: list(source.read(one)))  # one batch of rows, all held at once
        assert rows == BATCH
        rows, peak = _peak(lambda: source.read(ten))
        assert rows == 10 * BATCH
        assert peak < 1.5 * batch, f"streaming a {kind} file ten batches long held {peak} bytes; one batch is {batch}"
        # Closing rows read halfway closes the file.
        fds = len(os.listdir("/dev/fd"))
        rows = iter(source.read(ten))
        next(rows)
        rows.close()
        assert len(os.listdir("/dev/fd")) == fds, kind


def test_a_file_split_at_any_byte_reads_every_line_once():
    d = tempfile.mkdtemp()
    texts = ["plain", "é and ü", "日本語のテキスト", "emoji 🎉 here", "", "x" * 37, 'tab\tand "quotes"'] * 3
    rows = [{"i": i, "t": t} for i, t in enumerate(texts)]
    # \n and \r\n line ends, blank lines, multibyte characters for the ranges
    # to cut through, and a last line with no line end.
    lines_path, rows_path = os.path.join(d, "lines.txt"), os.path.join(d, "rows.jsonl")
    with open(lines_path, "wb") as f:
        f.write(b"".join(t.encode() + (b"\r\n" if i % 2 else b"\n") for i, t in enumerate(texts)) + "last ✓".encode())
    with open(rows_path, "wb") as f:
        ends = (b"\n\n", b"\r\n", b"\n")
        f.write(b"".join(json.dumps(r, ensure_ascii=False).encode() + ends[r["i"] % 3] for r in rows))
        f.write(b'{"i": "last"}')
    for path, want in ((lines_path, [*texts, "last ✓"]), (rows_path, [*rows, {"i": "last"}])):
        size = os.path.getsize(path)
        assert ext.FileSource(path).partitions() == [path] and list(ext.FileSource(path).read(path)) == want
        for split in range(1, size + 2):
            source = ext.FileSource(path, split_bytes=split)
            parts = source.partitions()
            assert len(parts) == -(-size // split)
            assert [row for part in parts for row in source.read(part)] == want, (path, split)


def test_a_parquet_file_splits_into_runs_of_whole_row_groups():
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = os.path.join(tempfile.mkdtemp(), "rows.parquet")
    table = pa.table({"id": list(range(50)), "name": [f"n{i}" if i % 4 else None for i in range(50)]})
    pq.write_table(table, path, row_group_size=7)  # eight row groups, the last of one row
    runs = set()
    for split in (1, 600, 1500, os.path.getsize(path) - 1):
        source = ext.FileSource(path, split_bytes=split)
        parts = source.partitions()
        assert {p[0] for p in parts} == {path} and parts[0][1] == 0 and parts[-1][2] == 8
        assert all(a[2] == b[1] for a, b in zip(parts, parts[1:])), parts  # the runs tile the row groups
        assert [row for part in parts for row in source.read(part)] == table.to_pylist(), split
        runs.add(len(parts))
    assert max(runs) == 8 and min(runs) == 1 and len(runs) > 2, runs  # one row group each, all of them, and between
    # Columns and filters, pushed down or not, work on a run as on the whole file.
    parts = ch.data.partitions(
        ext.FileSource(path, split_bytes=1), columns=["id"], filter=[("id", ">=", 45), ("name", "not in", [None])]
    )
    assert [row for part in parts for row in part] == [{"id": 45}, {"id": 46}, {"id": 47}, {"id": 49}]


def test_peers_read_the_parts_of_one_large_file():
    import pyarrow as pa
    import pyarrow.parquet as pq
    from test_cluster import WHERE, Cluster, lines

    d = tempfile.mkdtemp()
    rows = [{"id": i, "text": "é日🎉 " * (i % 5)} for i in range(3000)]
    with open(os.path.join(d, "big.jsonl"), "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r, ensure_ascii=False) for r in rows))  # no line end after the last
    pq.write_table(pa.Table.from_pylist(rows), os.path.join(d, "big.parquet"), row_group_size=50)
    cluster = Cluster(3)
    try:
        out = cluster.run(
            0,
            WHERE
            + f"""
import json
from chimaera import ext

@ch.remote
def read(partition):
    time.sleep(0.5)  # long enough for the calls to spread over the peers
    return peer_name(), list(partition)

for name in ("big.jsonl", "big.parquet"):
    parts = ch.data.partitions(ext.FileSource(os.path.join({d!r}, name), split_bytes=4096))
    got = list(read.map(parts))
    peers = len({{peer for peer, _ in got}})
    print("FILE", json.dumps([name, len(parts), peers, [row for _, part in got for row in part]]))
""",
        )
    finally:
        cluster.close()
    for line in lines(out, "FILE"):
        name, parts, peers, got = json.loads(line)
        assert parts >= 8 and peers >= 2, f"{name}: {parts} parts read on {peers} peers"
        assert got == rows, f"{name}: every row, in order, once"


def test_parquet_batches_keep_their_arrow_types_and_hold_the_rows_rows_gives():
    import datetime
    import decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    n = 30
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int32()),
            "score": pa.array([i / 2 if i % 3 else None for i in range(n)], pa.float32()),
            "name": pa.array([f"n{i}" if i % 5 else None for i in range(n)]),
            "ok": pa.array([i % 2 == 0 if i % 7 else None for i in range(n)]),
            "at": pa.array(
                [datetime.datetime(2026, 1, 1) + datetime.timedelta(hours=i) if i % 4 else None for i in range(n)],
                pa.timestamp("ms"),
            ),
            "tags": pa.array([[i, i + 1] if i % 6 else None for i in range(n)], pa.list_(pa.int16())),
            "price": pa.array([decimal.Decimal(i) / 100 for i in range(n)], pa.decimal128(9, 2)),
        }
    )
    path = os.path.join(tempfile.mkdtemp(), "typed.parquet")
    pq.write_table(table, path, row_group_size=8)
    stored = pq.read_schema(path)
    source = ext.FileSource(path, batch_rows=5)
    cases = [
        (None, None, lambda r: True),
        (["name", "id"], None, lambda r: True),
        (["id"], [("score", ">", 3)], lambda r: r["score"] is not None and r["score"] > 3),  # pushed down
        (None, [("name", "not in", [None, "n7"])], lambda r: r["name"] not in (None, "n7")),  # applied after reading
        (["tags", "at"], lambda r: r["ok"] is None, lambda r: r["ok"] is None),
    ]
    for columns, flt, keep in cases:
        (part,) = ch.data.partitions(source, columns=columns, filter=flt)
        batches = list(part.batches())
        rows = list(part.rows())
        want = [{c: r[c] for c in columns or r} for r in table.to_pylist() if keep(r)]
        assert rows == [row for b in batches for row in b.to_pylist()] == want, (columns, flt)
        assert [[type(v) for v in r.values()] for r in rows] == [[type(v) for v in r.values()] for r in want]
        schema = pa.schema([stored.field(c) for c in columns or stored.names])
        assert all(b.schema == schema and b.num_rows <= 5 for b in batches), (columns, flt)  # int32 stays int32
    lines_path = os.path.join(os.path.dirname(path), "lines.txt")
    with open(lines_path, "w") as f:
        f.write("a\n")
    with pytest.raises(TypeError, match="Parquet"):
        list(ch.data.partitions(lines_path)[0].batches())


def test_a_columnar_transform_over_parquet_batches_holds_a_batch_not_the_file():
    import subprocess
    import sys

    import pyarrow as pa
    import pyarrow.parquet as pq

    d, batch = tempfile.mkdtemp(), 20_000
    for name, n in (("one", batch), ("ten", 10 * batch)):
        table = pa.table({"id": pa.array(range(n), pa.int64()), "text": [f"row {i:08d} " + "x" * 60 for i in range(n)]})
        pq.write_table(table, os.path.join(d, f"{name}.parquet"), row_group_size=batch)
    # A process of its own for each file: Arrow's peak is since the process began.
    code = (
        "import sys\n"
        "import pyarrow as pa\n"
        "import pyarrow.compute as pc\n"
        "import chimaera as ch\n"
        "from chimaera import ext\n"
        "source = ext.FileSource(sys.argv[1], batch_rows=int(sys.argv[2]))\n"
        "(part,) = ch.data.partitions(source, filter=[('id', '>=', 0)])\n"
        "total = sum(pc.sum(pc.multiply(b['id'], 2)).as_py() for b in part.batches())\n"
        "print(total, pa.default_memory_pool().max_memory())"
    )

    def run(name):
        out = subprocess.run(
            [sys.executable, "-c", code, os.path.join(d, f"{name}.parquet"), str(batch)], capture_output=True, text=True
        )
        assert out.returncode == 0, out.stderr[-3000:]
        return [int(x) for x in out.stdout.split()]

    (_, one), (total, ten) = run("one"), run("ten")
    assert total == 10 * batch * (10 * batch - 1)
    assert ten < 2 * one, f"Arrow peaked at {ten} bytes for ten batches and {one} for one"


def test_a_file_sink_writes_an_arrow_partition_a_batch_at_a_time():
    import subprocess
    import sys

    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq

    d, batch = tempfile.mkdtemp(), 20_000
    schema = pa.schema([("id", pa.int32()), ("text", pa.string())])
    for name, batches in (("one", 1), ("ten", 10)):
        with ipc.new_file(os.path.join(d, f"{name}.arrow"), schema) as f:
            for b in range(batches):
                ids = range(b * batch, (b + 1) * batch)
                texts = [f"row {i:08d} " + "x" * 60 for i in ids]
                f.write_batch(pa.record_batch([pa.array(ids, pa.int32()), texts], schema))
    # A process of its own for each table, mapped from its file and not read
    # into memory: Arrow's peak is since the process began.
    code = (
        "import sys, tracemalloc\n"
        "import pyarrow as pa\n"
        "import pyarrow.ipc as ipc\n"
        "from chimaera import ext\n"
        "table = ipc.open_file(pa.memory_map(sys.argv[1])).read_all()\n"
        "tracemalloc.start()\n"
        "sink = ext.FileSink(sys.argv[2], sys.argv[3]).begin('20260101T000000000001Z-a')\n"
        "receipt = sink.write_batches(0, table.to_reader(max_chunksize=int(sys.argv[4])))\n"
        "peaks = pa.default_memory_pool().max_memory(), tracemalloc.get_traced_memory()[1]\n"
        "print(receipt['rows'], *peaks, receipt['path'])"
    )

    def run(name, fmt):
        args = [os.path.join(d, f"{name}.arrow"), os.path.join(d, f"{name}-{fmt}"), fmt, str(batch)]
        out = subprocess.run([sys.executable, "-c", code, *args], capture_output=True, text=True)
        assert out.returncode == 0, out.stderr[-3000:]
        rows, arrow, python, path = out.stdout.split()
        return int(rows), int(arrow), int(python), path

    for fmt in ("parquet", "jsonl"):
        (_, arrow_one, python_one, _), (rows, arrow_ten, python_ten, path) = run("one", fmt), run("ten", fmt)
        assert rows == 10 * batch
        slack = 1 << 20
        assert arrow_ten < 2 * arrow_one + slack, f"{fmt}: Arrow peaked at {arrow_ten} bytes for ten batches, {arrow_one} for one"
        assert python_ten < 2 * python_one + slack, f"{fmt}: Python peaked at {python_ten} bytes for ten, {python_one} for one"
        if fmt == "parquet":
            written = pq.ParquetFile(path)
            assert written.schema_arrow.equals(schema) and written.metadata.num_rows == 10 * batch
            assert written.metadata.num_row_groups == 10  # one per batch
        else:
            with open(path) as f:
                assert sum(1 for _ in f) == 10 * batch


@ch.remote
def squares_table(partition):
    import pyarrow as pa

    xs = list(partition)
    return pa.table({"x": pa.array(xs, pa.int16()), "square": pa.array([x * x for x in xs], pa.int64())})


class Batches(ext.Sink):
    """Takes each Arrow partition as batches."""

    def write_batches(self, part_id, batches):
        return str(batches.schema), sum(batch.num_rows for batch in batches)

    def commit(self, receipts):
        return receipts


class Rows(ext.Sink):
    """Has only write: an Arrow partition reaches it as rows."""

    def write(self, part_id, items):
        return items

    def commit(self, receipts):
        return [row for rows in receipts for row in rows]


def test_an_arrow_partition_reaches_the_sink_as_batches_and_keeps_its_types():
    import pyarrow as pa
    import pyarrow.parquet as pq

    def tables():
        return squares_table.map(ch.data.partitions(list(range(100)), n=4))

    out = tempfile.mkdtemp()
    manifest = tables().write_parquet(out).result(timeout=120)
    assert manifest["rows"] == 100 and len(manifest["parts"]) == 4
    assert all(pq.read_schema(p["path"]).field("x").type == pa.int16() for p in manifest["parts"])
    assert sorted(row["square"] for part in ch.data.partitions(out) for row in part) == [x * x for x in range(100)]
    receipts = tables().write(Batches()).result(timeout=120)
    assert [rows for _, rows in receipts] == [25] * 4 and all("x: int16" in schema for schema, _ in receipts)
    assert sorted(row["x"] for row in tables().write(Rows()).result(timeout=120)) == list(range(100))


def test_each_partition_of_a_list_carries_its_own_chunk_and_no_other():
    import pickle

    def descriptor_bytes(chunks):
        parts = ch.data.partitions(list(range(chunks)), n=chunks)
        assert len(parts) == chunks
        return sum(len(pickle.dumps(p)) for p in parts)

    small = descriptor_bytes(100)
    assert descriptor_bytes(1_000) < 15 * small  # a partition that carried every chunk made this 100 times
    assert descriptor_bytes(10_000) < 150 * small
    (part,) = ch.data.partitions(list(range(5)), n=1)
    assert list(pickle.loads(pickle.dumps(part))) == [0, 1, 2, 3, 4]


def test_two_writers_to_one_directory_leave_two_whole_generations():
    out = tempfile.mkdtemp()

    def read():
        source = ext.FileSource(out)
        return sorted(row for part in source.partitions() for row in source.read(part))

    a = ext.FileSink(out).begin("20260101T000000000001Z-a")
    b = ext.FileSink(out).begin("20260101T000000000002Z-b")
    staged_a = [a.write(i, [f"a{i}"] * 3) for i in range(2)]
    staged_b = [b.write(i, [f"b{i}"] * 2) for i in range(3)]  # the same part numbers
    with pytest.raises(FileNotFoundError, match="nothing committed"):
        read()  # staged parts of either write are not readable before a commit
    with pytest.raises(FileNotFoundError, match="nothing committed"):
        ch.data.partitions(out)
    mb = b.commit(staged_b)
    assert read() == sorted(_rows_of(mb)) == ["b0", "b0", "b1", "b1", "b2", "b2"]
    ma = a.commit(staged_a)  # began first, commits last
    assert _rows_of(ma) == ["a0"] * 3 + ["a1"] * 3 and _rows_of(mb) == ["b0", "b0", "b1", "b1", "b2", "b2"]
    assert read() == sorted(_rows_of(mb)), "a write that began earlier never replaces a later one"
    assert a.commit(staged_a) == ma and b.commit(staged_b) == mb  # a replayed commit publishes the same generation
    assert read() == sorted(_rows_of(mb))
    c = ext.FileSink(out).begin("20260101T000000000003Z-c")
    c.commit([c.write(0, ["c0"])])
    assert read() == ["c0"]
    assert _generations(out) == [s.generation for s in (a, b, c)]


def test_a_commit_refuses_a_part_that_changed_since_its_write_and_the_last_generation_stays():
    out = tempfile.mkdtemp()

    def read():
        source = ext.FileSource(out)
        return sorted(row for part in source.partitions() for row in source.read(part))

    old = ext.FileSink(out).begin("20260101T000000000001Z-a")
    old.commit([old.write(0, ["a0", "a1"])])
    new = ext.FileSink(out).begin("20260101T000000000002Z-b")
    receipts = [new.write(i, [f"b{i}"] * 50) for i in range(3)]
    for r in receipts:
        with open(r["path"], "rb") as f:
            assert (r["bytes"], r["sha256"]) == (os.path.getsize(r["path"]), hashlib.sha256(f.read()).hexdigest())

    def damage(part, how):
        with open(receipts[part]["path"], "r+b") as f:
            how(f)

    def refused(part, why):
        with pytest.raises(ch.errors.DataLost, match=rf"part {part} of write {new.generation} .*{why}"):
            new.commit(receipts)
        assert read() == ["a0", "a1"], "the last committed generation still reads"
        assert _generations(out) == [old.generation]

    damage(1, lambda f: f.truncate(receipts[1]["bytes"] // 2))  # a staged part, truncated
    refused(1, f"it has {receipts[1]['bytes'] // 2} bytes")
    receipts[1] = new.write(1, ["b1"] * 50)  # the part's retried write
    damage(2, lambda f: f.write(b'"x'))  # the same size, other bytes
    refused(2, "SHA-256")
    receipts[2] = new.write(2, ["b2"] * 50)
    os.unlink(receipts[0]["path"])
    refused(0, "it is gone")
    receipts[0] = new.write(0, ["b0"] * 50)
    assert new.commit(receipts)["rows"] == 150 and read() == ["b0"] * 50 + ["b1"] * 50 + ["b2"] * 50


class Bucket(ext.ObjectStore):
    """An object store in memory: whole-object puts, conditional ones, and no
    rename. A put to a key in `lose` takes effect, then loses its reply, once."""

    def __init__(self):
        self.objects, self.puts, self.lose = {}, collections.Counter(), set()

    def get(self, key):
        return self.objects.get(key)

    def head(self, key):
        found = self.objects.get(key)
        return None if found is None else (len(found[0]), found[1])

    def put(self, key, body, *, sha256=None, if_version=None):
        data = body if isinstance(body, bytes) else body.read()
        assert sha256 in (None, hashlib.sha256(data).hexdigest())
        if if_version is not None and self.objects.get(key, (b"", ""))[1] != if_version:
            raise ch.errors.VersionConflict(f"{key} is not at version {if_version!r}")
        self.puts[key] += 1
        self.objects[key] = (data, f"v{self.puts.total()}")
        if key in self.lose:
            self.lose.discard(key)
            raise ConnectionResetError(f"the reply to the put of {key} was lost")
        return self.objects[key][1]


def test_a_commit_to_an_object_store_repeated_after_a_lost_reply_returns_the_generation_it_recorded():
    bucket, pointer = Bucket(), "out/_manifest.json"

    def staged(key, rows):
        sink = ext.FileSink("out", store=bucket).begin(key)
        return sink, [sink.write(i, [f"{key[-1]}{i}"] * rows) for i in range(2)]

    def published():
        return json.loads(bucket.objects[pointer][0])

    a, receipts_a = staged("20260101T000000000001Z-a", 3)
    assert [r["path"] for r in receipts_a] == [f"out/part-0000{i}-{a.generation}.jsonl" for i in range(2)]
    record = f"out/_manifests/{a.generation}.json"
    bucket.lose.add(record)  # the record is stored; the commit never hears so
    with pytest.raises(ConnectionResetError):
        a.commit(receipts_a)
    version = bucket.objects[record][1]
    ma = a.commit(receipts_a)  # the retry
    assert ma["generation"] == a.generation and ma["rows"] == 6 and published() == ma
    assert bucket.objects[record][1] == version and bucket.puts[record] == 1, "the record was put once"
    # A lost reply to the put that publishes, then a later write commits, then the retry.
    b, receipts_b = staged("20260101T000000000002Z-b", 2)
    c, receipts_c = staged("20260101T000000000003Z-c", 1)
    bucket.lose.add(pointer)
    with pytest.raises(ConnectionResetError):
        b.commit(receipts_b)
    mb = json.loads(bucket.objects[f"out/_manifests/{b.generation}.json"][0])
    mc = c.commit(receipts_c)
    assert b.commit(receipts_b) == mb and published() == mc, "the retry returns b's generation and leaves c's published"
    assert a.commit(receipts_a) == ma and published() == mc  # a replay of an older commit moves nothing
    assert bucket.puts[pointer] == 3  # once each for a, b and c
    # A part changed since its write: nothing is published, and the commit names it.
    d, receipts_d = staged("20260101T000000000004Z-d", 4)
    path = receipts_d[1]["path"]
    bucket.objects[path] = (bucket.objects[path][0][:5], "v-replaced")
    changed = rf"part 1 of write {d.generation} .*it has 5 bytes, version v-replaced"
    with pytest.raises(ch.errors.DataLost, match=changed):
        d.commit(receipts_d)
    assert published() == mc and f"out/_manifests/{d.generation}.json" not in bucket.objects


def test_a_directory_with_another_tools_manifest_reads_as_files():
    for manifest in ('{"version": 3}', "not json"):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "data.jsonl"), "w") as f:
            f.write('{"x": 1}\n')
        with open(os.path.join(d, "_manifest.json"), "w") as f:
            f.write(manifest)
        assert ext.FileSource(d).files == [os.path.join(d, "_manifest.json"), os.path.join(d, "data.jsonl")]


def test_a_file_sink_commits_where_the_filesystem_has_no_flock(monkeypatch):
    def no_flock(fd, op):
        raise OSError(errno.ENOSYS, "Function not implemented")

    monkeypatch.setattr(ext, "fcntl", types.SimpleNamespace(flock=no_flock, LOCK_EX=fcntl.LOCK_EX))
    out = tempfile.mkdtemp()
    sink = ext.FileSink(out).begin("20260101T000000000001Z-a")
    manifest = sink.commit([sink.write(0, ["a0"])])
    with open(os.path.join(out, "_manifest.json")) as f:
        assert json.load(f) == manifest


def test_a_file_sink_needs_a_key_from_begin():
    out = tempfile.mkdtemp()
    with pytest.raises(RuntimeError, match="begin"):
        ext.FileSink(out).write(0, ["a0"])
    with pytest.raises(RuntimeError, match="begin"):
        ext.FileSink(out).commit([])
    assert os.listdir(out) == []


def test_write_keys_never_go_back_when_the_clock_does(monkeypatch):
    from chimaera import _data

    now = time.time_ns()
    clock = iter([now, now - 3600 * 10**9, now - 3600 * 10**9])  # an NTP step back of an hour
    fake = types.SimpleNamespace(time_ns=lambda: next(clock), strftime=time.strftime, gmtime=time.gmtime)
    monkeypatch.setattr(_data, "_last_key_ns", _data._last_key_ns)  # restored after the test
    monkeypatch.setattr(_data, "time", fake)
    keys = [_data._write_key() for _ in range(3)]
    assert keys == sorted(keys) and len(set(keys)) == 3


@ch.remote
def keep(partition):
    return list(partition)


def test_concurrent_writes_through_results_each_publish_a_whole_generation():
    out = tempfile.mkdtemp()
    first = keep.map(ch.data.partitions(list(range(40)), n=4)).write(ext.FileSink(out))
    second = keep.map(ch.data.partitions(list(range(100, 130)), n=3)).write(ext.FileSink(out))
    m1, m2 = first.result(timeout=120), second.result(timeout=120)
    assert sorted(_rows_of(m1)) == list(range(40)) and sorted(_rows_of(m2)) == list(range(100, 130))
    assert _generations(out) == sorted(m["generation"] for m in (m1, m2))
    newest = max(m1, m2, key=lambda m: m["generation"])
    assert sorted(x for rows in keep.map(ch.data.partitions(out)) for x in rows) == sorted(_rows_of(newest))


def _die_once(marker):
    if not os.path.exists(marker):
        open(marker, "w").close()
        os.kill(os.getpid(), signal.SIGKILL)


class Ledger(ext.Sink):
    """Publishes a total once per write key; its commit's worker dies once, right after publishing."""

    effects = "idempotent"

    def __init__(self, path, marker):
        self.path, self.marker, self.key = path, marker, None

    def begin(self, key):
        sink = copy.copy(self)
        sink.key = key
        return sink

    def write(self, part_id, items):
        return sum(items)

    def commit(self, receipts):
        with open(self.path, "a+") as f:
            f.seek(0)
            if self.key not in f.read().split():
                f.write(f"{self.key} {sum(receipts)}\n")
        _die_once(self.marker)
        return sum(receipts)


class Publish(ext.Sink):
    """Publishes a total with no way to tell a replay apart; its commit's worker dies once, after publishing."""

    def __init__(self, path, marker):
        self.path, self.marker = path, marker

    def write(self, part_id, items):
        return sum(items)

    def commit(self, receipts):
        with open(self.path, "a") as f:
            f.write(f"{sum(receipts)}\n")
        _die_once(self.marker)
        return sum(receipts)


class DyingFileSink(ext.FileSink):
    def __init__(self, directory, marker):
        super().__init__(directory)
        self.marker = marker

    def commit(self, receipts):
        manifest = super().commit(receipts)
        _die_once(self.marker)
        return manifest


def test_a_commit_killed_after_it_published_takes_effect_once_or_reports_an_unknown_outcome():
    d = tempfile.mkdtemp()
    parts = ch.data.partitions(Squares(35))
    expected = sum(x * x for x in range(40))
    # An idempotent sink's commit runs again with the same key, and publishes once.
    assert total.map(parts).write(Ledger(f"{d}/ledger", f"{d}/m1")).result(timeout=120) == expected
    assert os.path.exists(f"{d}/m1"), "the commit's worker must have died"
    (line,) = open(f"{d}/ledger").read().splitlines()
    assert line.endswith(f" {expected}")
    # A sink that declares nothing is never committed twice: the write reports the unknown outcome.
    with pytest.raises(ch.OutcomeUnknown):
        total.map(parts).write(Publish(f"{d}/published", f"{d}/m2")).result(timeout=120)
    assert open(f"{d}/published").read().splitlines() == [str(expected)]
    # FileSink is idempotent: its replayed commit publishes the same, single generation.
    out = os.path.join(d, "out")
    manifest = total.map(parts).write(DyingFileSink(out, f"{d}/m3")).result(timeout=120)
    assert os.path.exists(f"{d}/m3")
    assert _generations(out) == [manifest["generation"]]
    with open(os.path.join(out, "_manifest.json")) as f:
        assert json.load(f) == manifest and sum(_rows_of(manifest)) == expected


class Squares(ext.Source):
    def __init__(self, n):
        self.n = n

    def partitions(self):
        return [(i, i + 10) for i in range(0, self.n, 10)]

    def read(self, part, columns=None, filter=None):
        return [x * x for x in range(*part)]


class Collect(ext.Sink):
    def write(self, part_id, items):
        return (part_id, sum(items))

    def commit(self, receipts):
        return dict(receipts)


@ch.remote
def total(partition):
    return [sum(partition)]


def test_custom_sources_and_sinks_plug_in():
    parts = ch.data.partitions(Squares(35))
    assert len(parts) == 4
    committed = total.map(parts).write(Collect()).result()
    assert sum(committed.values()) == sum(x * x for x in range(40)) and sorted(committed) == [0, 1, 2, 3]


class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y


def point_dumps(p):
    return f"{p.x},{p.y}".encode()


def point_loads(b):
    return Point(*map(int, b.split(b",")))


def test_register_serializer_round_trips():
    import pickle

    ext.register_serializer(Point, dumps=point_dumps, loads=point_loads)
    data = pickle.dumps(Point(3, 4))
    assert b"point_loads" in data  # the custom form, not Point's __dict__
    p = pickle.loads(data)
    assert (p.x, p.y) == (3, 4)


def test_shuffle_buckets_agree_across_processes_whatever_their_hash_seed():
    # Scatters run in different executors: a key must land in the same bucket
    # in all of them, including the fast path for ints. A frozenset of str
    # iterates in an order the seed picks; a NaN hashes by its address; a
    # number may hash with a str (SymPy's do), so hash() of it moves.
    import subprocess
    import sys

    code = (
        "import numbers\n"
        "import numpy\n"
        "from decimal import Decimal\n"
        "from fractions import Fraction\n"
        "from chimaera._shuffle import bucket_of\n"
        "class Money(numbers.Number):\n"
        "    def __init__(self, cents): self.cents = cents\n"
        "    def __eq__(self, other): return isinstance(other, Money) and other.cents == self.cents\n"
        "    def __hash__(self): return hash(('Money', self.cents))\n"
        "print([bucket_of(k, 64) for k in (0, 1, -1, -2, 2**61, 2**70, 123456789012, 'a', b'b', (1, 'x'), 2.5, "
        "True, 1.0, -0.0, Fraction(1, 3), Decimal('0.5'), (True, 'x'), ((1, ('a', 2.0)), 'b'), (float('nan'), 'z'), "
        "frozenset('abcdefgh'), frozenset({1, 'a', (2, 'b')}), float('nan'), None, Money(5), "
        "numpy.longdouble(2.5), numpy.clongdouble(1 + 2j), (numpy.longdouble(0.1), 'x'))])\n"
    )
    env = {k: v for k, v in __import__("os").environ.items() if k != "PYTHONHASHSEED"}
    outs = {
        subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env={**env, "PYTHONHASHSEED": s}, check=True).stdout
        for s in ("0", "1", "random", "4242")
    }
    assert len(outs) == 1, outs


@ch.remote
def marked(i, marks):
    with open(os.path.join(marks, f"ran-{i}"), "w"):
        pass
    return [(i % 3, 1)]


@ch.remote
def plus_one(x):
    return x + 1


def test_reduce_by_key_on_an_unread_map_runs_the_function_inside_its_scatters():
    run = ch.runs.current_run()
    before = {key for key, _ in run.tasks()}
    marks = tempfile.mkdtemp()
    totals = marked.map(range(8), [marks] * 8).reduce_by_key(operator.add, partitions=2).to_dict()
    assert totals == {0: 3, 1: 3, 2: 2}
    assert sorted(os.listdir(marks)) == sorted(f"ran-{i}" for i in range(8)), "each item ran once"
    new = {key for key, _ in run.tasks()} - before
    assert len(new) == 8 + 1 + 2, (
        "8 calls that map and scatter, 1 that lists their results and 2 gathers: the pairs were never stored between"
    )


def test_a_map_kept_unread_starts_once_the_driver_moves_on():
    marks = tempfile.mkdtemp()
    kept = marked.map(range(4), [marks] * 4)
    assert plus_one.remote(1).result(timeout=60) == 2  # the driver submits and waits on something else
    deadline = time.monotonic() + 30
    while len(os.listdir(marks)) < 4 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(os.listdir(marks)) == 4, "the map ran before anything read it"
    assert list(kept) == [[(i % 3, 1)] for i in range(4)]


def test_a_script_that_makes_a_map_and_never_reads_it_still_runs_it():
    from conftest import run_script

    marks = tempfile.mkdtemp()
    out = run_script(
        f"""
        import os
        import chimaera as ch

        @ch.remote
        def mark(i):
            open(os.path.join({marks!r}, f"ran-{{i}}"), "w").close()

        mark.map(range(3))
        """
    )
    assert out.returncode == 0, out.stderr
    assert sorted(os.listdir(marks)) == ["ran-0", "ran-1", "ran-2"], "the script's map ran at exit"
