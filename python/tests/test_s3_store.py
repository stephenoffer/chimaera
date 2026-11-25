"""FileSink on an S3 API (MinIO, AWS): conditional puts, and commits whose replies are lost.

deploy/dev/s3-test.sh starts MinIO in Docker and runs this file. Without
CHIMAERA_TEST_S3_ENDPOINT it is skipped. Credentials come from AWS_*.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid

import pytest

import chimaera as ch
from chimaera import ext

ENDPOINT = os.environ.get("CHIMAERA_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("CHIMAERA_TEST_S3_BUCKET", "chimaera-test")
pytestmark = pytest.mark.skipif(not ENDPOINT, reason="needs an S3 endpoint: deploy/dev/s3-test.sh starts MinIO")


@pytest.fixture
def prefix():
    return f"chimaera-test/{uuid.uuid4().hex}"


def store():
    return ext.S3Store(BUCKET, endpoint=ENDPOINT)


def published(prefix):
    return json.loads(store().get(f"{prefix}/_manifest.json")[0])


def lose_first_reply(monkeypatch, key):
    """The store takes the first PUT of `key`, and its reply never arrives.
    Gives the status the store answered each PUT of `key` with."""
    real, statuses = urllib.request.urlopen, []

    def urlopen(request, *args, **kwargs):
        mine = request.get_method() == "PUT" and request.full_url.endswith("/" + key)
        try:
            reply = real(request, *args, **kwargs)
        except urllib.error.HTTPError as e:
            if mine:
                statuses.append(e.code)
            raise
        if mine:
            statuses.append(reply.status)
            if len(statuses) == 1:
                reply.close()
                raise ConnectionResetError(f"the reply to the PUT of {key} was lost")
        return reply

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return statuses


def test_conditional_puts_refuse_what_they_did_not_read(prefix):
    s, key = store(), f"{prefix}/object"
    assert s.get(key) is None and s.head(key) is None
    first = s.put(key, b"one", if_version="")
    with pytest.raises(ch.errors.VersionConflict):
        s.put(key, b"two", if_version="")
    second = s.put(key, b"two", if_version=first)
    with pytest.raises(ch.errors.VersionConflict):
        s.put(key, b"three", if_version=first)
    assert s.get(key) == (b"two", second) and s.head(key) == (3, second)
    with pytest.raises(OSError, match="XAmzContentSHA256Mismatch"):
        s.put(key, b"four", sha256="0" * 64)  # the store checks the body against its digest
    # A missing bucket is an error, not a missing object or a conflict.
    missing = ext.S3Store(f"no-bucket-{uuid.uuid4().hex[:12]}", endpoint=ENDPOINT)
    calls = [lambda: missing.get(key), lambda: missing.put(key, b"one"), lambda: missing.put(key, b"one", if_version="")]
    for call in calls:
        with pytest.raises(OSError, match="NoSuchBucket"):
            call()


@pytest.mark.parametrize("lost", ["record", "pointer"])
def test_a_commit_whose_reply_is_lost_is_sent_again_and_returns_the_generation_it_recorded(monkeypatch, prefix, lost):
    sink = ext.FileSink(prefix, store=store()).begin("20260101T000000000002Z-b")
    receipts = [sink.write(i, [f"b{i}"] * 3) for i in range(2)]
    record = f"{prefix}/_manifests/{sink.generation}.json"
    statuses = lose_first_reply(monkeypatch, record if lost == "record" else f"{prefix}/_manifest.json")
    manifest = sink.commit(receipts)
    assert statuses == [200, 412], "the store took the first put and refused the one sent again"
    assert manifest["generation"] == sink.generation and manifest["rows"] == 6
    assert json.loads(store().get(record)[0]) == manifest == published(prefix)
    assert sink.commit(receipts) == manifest  # a replayed commit returns the same generation
    older = ext.FileSink(prefix, store=store()).begin("20260101T000000000001Z-a")
    older.commit([older.write(0, ["a0"])])
    assert published(prefix) == manifest, "a write that began earlier never replaces it"


def test_a_commit_refuses_a_part_replaced_since_its_write(prefix):
    s = store()
    sink = ext.FileSink(prefix, store=s).begin("20260101T000000000001Z-a")
    receipts = [sink.write(i, [f"a{i}"] * 3) for i in range(2)]
    s.put(receipts[1]["path"], b'"a1"\n')
    with pytest.raises(ch.errors.DataLost, match=f"part 1 of write {sink.generation}"):
        sink.commit(receipts)
    assert s.get(f"{prefix}/_manifest.json") is None


def test_workers_write_with_the_credentials_in_their_peers_environment(prefix):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from test_cluster import Cluster, lines

    # Only the peer has them: a run ships no AWS_* variable to its workers.
    cluster = Cluster(1, env={0: {k: os.environ[k] for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")}})
    try:
        out = cluster.run(
            0,
            f"""
import json
import pyarrow as pa
import chimaera as ch
from chimaera import ext

@ch.remote
def table(rows):
    return pa.table({{"id": pa.array(list(rows), pa.int32())}})

sink = ext.FileSink({prefix!r}, "parquet", store=ext.S3Store({BUCKET!r}, endpoint={ENDPOINT!r}))
manifest = table.map(ch.data.partitions(list(range(30)), n=3)).write(sink).result(timeout=120)
print("MANIFEST", json.dumps(manifest))
""",
        )
    finally:
        cluster.close()
    (line,) = lines(out, "MANIFEST")
    manifest = json.loads(line)
    assert manifest["rows"] == 30 and published(prefix) == manifest
    read = [pq.read_table(pa.BufferReader(store().get(p["path"])[0])) for p in manifest["parts"]]
    assert sorted(i for t in read for i in t["id"].to_pylist()) == list(range(30))
    assert all(t.schema.field("id").type == pa.int32() for t in read)  # Arrow partitions keep their types
