"""Results of several values stored as parts (_serde.PARTS_MAGIC): at scale a
consumer of one value fetches only that value, and the parts go when the
result goes."""

from __future__ import annotations

import glob
import json
import os
import time

from test_cluster import Cluster

from chimaera import _serde
from chimaera._executor import worth_splitting


def test_a_result_in_parts_reads_back_whole_or_one_value_at_a_time(tmp_path):
    run_dir = str(tmp_path)
    values = (b"a" * 100_000, [1, 2, 3], b"c" * 200_000)
    encoded = [_serde.dumps_parts(v) for v in values]
    ids = [b"\x01" * 16, None, b"\x03" * 16]
    for part, (p, _, _) in zip(ids, encoded):
        if part is not None:
            _serde.write_file(os.path.join(run_dir, part.hex()), p)
    parts, size, _ = _serde.pack_seq(encoded, ids)
    head = b"".join(bytes(p) for p in parts)
    assert len(head) == size < 1000, "the parent holds the table and the small value only"
    assert _serde.loads(head, run_dir) == values
    assert [_serde.loads_part(head, i, 3, run_dir) for i in range(3)] == list(values)
    assert [_serde.part_id(head, i, 3) for i in range(3)] == ids
    assert _serde.external_blobs(head) == [b"\x01" * 16, b"\x03" * 16], "fetch_local brings the parts"
    os.unlink(os.path.join(run_dir, (b"\x03" * 16).hex()))
    assert _serde.loads_part(head, 0, 3, run_dir) == values[0], "reading one value touches only its part"
    seq = b"".join(bytes(p) for p in _serde.dumps_seq(values)[0])  # results stored before parts existed
    assert _serde.loads(seq) == values and _serde.loads_part(seq, 2, 3) == values[2]
    assert _serde.part_id(seq, 0, 3) is None and _serde.external_blobs(seq) == []


def test_parts_are_chosen_only_when_consumers_spread_over_the_peers():
    mb = 1 << 20
    assert not worth_splitting(64, 500 * mb, 1), "one peer fetches nothing"
    assert not worth_splitting(64, 5 * mb, 3), "21 consumers a peer: one fetch of the whole beats 21 of parts"
    assert worth_splitting(3, 5 * mb, 3), "a consumer a peer: the same fetches, a third of the bytes"
    assert worth_splitting(64, mb // 2, 64), "at scale even a small result"
    assert worth_splitting(64, 400 * mb, 3), "large enough that bytes outweigh the extra fetches"


DRIVER = """
import gc, json, os, time
import chimaera as ch

def wait_for(path):
    while not os.path.exists(path):
        time.sleep(0.05)

@ch.remote(returns=3)
def three():
    return tuple(bytes([i]) * (2 << 20) for i in range(3))  # 2 MiB each, in the pickle

@ch.remote
def read(x):
    return ch.context().node, x[0], len(x)

nodes = sorted(n.peer for n in ch.runs.cluster().nodes())
on = lambda i: ch.dist.Placement(require={"node": nodes[i]})
f = three.options(placement=on(0)).remote()
parts = f.split()
got = ch.get([read.options(placement=on(i)).remote(parts[i]) for i in range(3)], timeout=120)
print("READ", json.dumps(got), flush=True)
print("NODES", json.dumps(nodes), flush=True)
wait_for(LOOKED)  # the test looks at the stores
whole = f.result(timeout=60)  # read here, through this driver's peer
print("WHOLE", json.dumps([(w[0], len(w)) for w in whole]), flush=True)
wait_for(BROKEN)  # the test removed one part from every peer
from chimaera._runtime import Runtime, read_object
run, name = (bytes.fromhex(h) for h in open(BROKEN).read().split())
try:
    read_object(Runtime.get().object_dir, run, name)
    print("LOST", json.dumps("read without its part"), flush=True)
except ch.errors.DataLost as e:
    print("LOST", json.dumps(str(e)), flush=True)
del f, parts, whole, got
gc.collect()
print("RELEASED", flush=True)
wait_for(DONE)
"""


def stored(c: Cluster, i: int) -> dict[str, bytes]:
    """Object files on peer `i`: name -> first bytes."""
    out = {}
    for path in glob.glob(os.path.join(c.dir(i), "objects", "*", "*")):
        name = os.path.basename(path)
        if len(name) == 32 and os.path.isfile(path):
            with open(path, "rb") as f:
                out[name] = f.read(4096)
    return out


def test_a_consumer_at_scale_fetches_only_its_value_and_parts_go_with_their_result(monkeypatch):
    # What each peer fetched, so no replicas: those copy every part to the
    # next peer as soon as it is written (see test_cluster for replicas).
    monkeypatch.setenv("CHIMAERA_OBJECT_REPLICAS", "0")
    c = Cluster(3)  # three peers, a consumer on each: parts pay (see worth_splitting)
    looked, broken, done = (os.path.join(c.root, n) for n in ("looked", "broken", "done"))
    driver = DRIVER.replace("LOOKED", repr(looked)).replace("BROKEN", repr(broken)).replace("DONE", repr(done))
    drv = c.driver(0, driver)
    try:
        def line(key: str):
            text = drv.stdout.readline()
            assert text.startswith(key + " "), (text, drv.stderr.read()[-3000:] if not text else "")
            return json.loads(text.partition(" ")[2])

        got, nodes = line("READ"), line("NODES")
        assert [(v, n) for _, v, n in got] == [(0, 2 << 20), (1, 2 << 20), (2, 2 << 20)]
        assert [node for node, _, _ in got] == nodes, "each value was read on its own peer"

        stores = [stored(c, i) for i in range(3)]
        parents = [(i, name, head) for i, s in enumerate(stores) for name, head in s.items() if head[:4] == b"CHP1"]
        assert parents, "the result was stored as one file, not in parts"
        producer = next(i for i, name, _ in parents if all(p.hex() in stores[i] for p in _serde.external_blobs(stores[i][name])))
        name = next(n for i, n, _ in parents if i == producer)
        part_names = [p.hex() for p in _serde.external_blobs(stores[producer][name])]
        assert len(part_names) == 3 and all(p in stores[producer] for p in part_names)
        held = sorted(tuple(p for p in part_names if p in stores[i]) for i in range(3) if i != producer)
        assert [len(h) for h in held] == [1, 1] and held[0] != held[1], (
            "each other peer fetched only the part its consumer read", held
        )

        open(looked, "w").close()
        assert line("WHOLE") == [[0, 2 << 20], [1, 2 << 20], [2, 2 << 20]], "the whole result rebuilds from its parts"
        gone = glob.glob(os.path.join(c.root, "p*", "objects", "*", part_names[0]))
        assert gone, "the part is stored somewhere"
        for path in gone:
            os.unlink(path)
        with open(broken, "w") as f:
            f.write(f"{os.path.basename(os.path.dirname(gone[0]))} {name}")
        assert "part of value" in line("LOST"), "a lost part reads as DataLost, naming the value"
        assert drv.stdout.readline().strip() == "RELEASED"
        deadline = time.monotonic() + 30
        while name in stored(c, producer) and time.monotonic() < deadline:
            time.sleep(0.2)
        left = stored(c, producer)
        assert name not in left, "the released result is still stored"
        assert not [p for p in part_names if p in left], "its parts outlived it"
        open(done, "w").close()
        drv.communicate(timeout=60)
    finally:
        if drv.poll() is None:
            drv.kill()
        c.close()


def test_a_small_object_file_is_read_and_a_large_one_mapped_both_private(tmp_path):
    # A first map costs ~2.5 ms on macOS however small the file; every input a
    # peer fetched is a first map, so small files are read instead.
    import mmap

    for n, kind in ((1000, bytearray), ((_serde._READ_MAX >> 3) + 1, mmap.mmap)):
        value = bytes(range(256)) * (n * 8 // 256 + 1)
        parts, _, _ = _serde.dumps_parts(bytearray(value))
        path = str(tmp_path / f"o{n}")
        _serde.write_file(path, parts)
        assert isinstance(_serde._map(path), kind)
        got = _serde.read_file(path)
        got[0] = (got[0] + 1) % 256  # writable, and private to this reader
        assert _serde.read_file(path) == bytearray(value)
