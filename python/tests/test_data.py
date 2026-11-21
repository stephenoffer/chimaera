"""Partitioned data: ch.partitions, reduce_by_key (the shuffle contract) and sinks."""

from __future__ import annotations

import collections
import json
import operator
import os
import random
import tempfile

import pytest

import chimaera as ch
from chimaera import ext

LINES = ["the quick brown fox", "jumps over the lazy dog", "the dog barks", "a fox again"] * 25


@ch.remote
def count_words(partition):
    return collections.Counter(w for line in partition for w in line.split()).items()


def test_word_count_matches_a_serial_count():
    totals = count_words.map(ch.partitions(LINES, n=7)).reduce_by_key(operator.add, partitions=3)
    assert isinstance(totals, ch.Partitioned) and len(totals) == 3
    parts = list(totals)  # one list of (key, value) pairs per partition
    assert len(parts) == 3 and sum(len(p) for p in parts) == len(totals.to_dict())
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
    seen = [k for f in got.futures for k, _ in f.result()]
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
    parts = ch.partitions(os.path.join(d, "*.jsonl"), columns=["name"], filter=[("age", ">=", 40)])
    assert len(parts) == 3
    assert sorted(n for ns in names.map(parts) for n in ns) == ["p02", "p03", "p12", "p13", "p22", "p23"]
    rows = list(ch.partitions(os.path.join(d, "*.jsonl"), columns=["name", "age"])[0].rows())
    assert rows[0] == {"name": "p00", "age": 20}
    young = ch.partitions(os.path.join(d, "*.csv"), filter=lambda r: int(r["age"]) < 30)
    assert list(names.map(young)) == [["c2"]]
    with pytest.raises(FileNotFoundError):
        ch.partitions(os.path.join(d, "*.nothing"))


def test_file_sink_writes_parts_and_a_manifest_and_rewrites_cleanly():
    out = tempfile.mkdtemp()
    totals = count_words.map(ch.partitions(LINES, n=4)).reduce_by_key(operator.add, partitions=2)
    manifest = totals.write(ext.FileSink(out)).result()
    assert manifest["rows"] == len(totals.to_dict()) and len(manifest["parts"]) == 2
    on_disk = {}
    for part in manifest["parts"]:
        with open(part["path"]) as f:
            on_disk.update(dict(json.loads(line) for line in f))
    assert on_disk == totals.to_dict()
    again = totals.write(ext.FileSink(out)).result()  # idempotent: same files, same manifest
    assert again == manifest and sorted(os.listdir(out)) == ["_manifest.json", "part-00000.jsonl", "part-00001.jsonl"]


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
    parts = ch.partitions(Squares(35))
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
