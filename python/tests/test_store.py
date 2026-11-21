"""ch.Store: versions and compare-and-set, counters, locks with fencing,
barriers, watches and TTLs; and fencing across a leader failover."""

from __future__ import annotations

import asyncio
import signal
import threading
import time
import uuid

import pytest

import chimaera as ch


def ns() -> ch.Store:
    return ch.Store(f"test-{uuid.uuid4().hex[:8]}")


def test_versions_make_compare_and_set():
    s = ns()
    v1 = s.put("cfg", {"lr": 3e-4}, if_version=0)
    assert s.get("cfg") == {"lr": 3e-4} and s.version("cfg") == v1
    with pytest.raises(ch.VersionConflict) as info:
        s.put("cfg", "other", if_version=0)
    assert info.value.version == v1
    v2 = s.put("cfg", {"lr": 1e-4}, if_version=v1)
    assert v2 > v1
    with pytest.raises(ch.VersionConflict):
        s.delete("cfg", if_version=v1)
    assert s.delete("cfg") and not s.delete("cfg")
    assert s.get("cfg", "gone") == "gone" and s.version("cfg") == 0
    with pytest.raises(ValueError, match="1 MiB"):
        s.put("big", b"x" * (2 << 20))


def test_scan_returns_keys_under_a_prefix_in_order():
    s = ns()
    for k in ["b/2", "a/1", "b/1", "c"]:
        s.put(k, k.upper())
    assert list(s.scan("b/")) == [("b/1", "B/1"), ("b/2", "B/2")]
    assert [k for k, _ in s.scan()] == ["a/1", "b/1", "b/2", "c"]


@ch.remote
def bump(store, n):
    c = store.counter("hits")
    for _ in range(n):
        c.add(1)


def test_counters_are_atomic_across_calls():
    s = ns()
    ch.gather(*(bump.remote(s, 25) for _ in range(8)), timeout=120)
    assert s.counter("hits").value() == 200


@ch.remote
def locked_increment(store, k):
    for _ in range(k):
        with store.lock("mutex", ttl="10s"):
            n = store.get("n", 0)
            time.sleep(0.002)  # widen the race a lock must close
            store.put("n", n + 1)


def test_a_lock_excludes_other_holders():
    s = ns()
    ch.gather(*(locked_increment.remote(s, 10) for _ in range(6)), timeout=180)
    assert s.get("n") == 60, "a lost update means two holders ran at once"


def test_a_stale_holder_is_fenced_after_its_lease_expires():
    s = ns()
    a = s.lock("leader", ttl="400ms").acquire()
    s.put("state", "from a", fence=a)
    a._stop.set()  # a stalls: no more renewals
    b = s.lock("leader", ttl="10s", timeout=10).acquire()
    try:
        assert b.token > a.token and s.holder("leader") == b.holder
        with pytest.raises(ch.LeaseLost):
            s.put("state", "late write from a", fence=a)
        s.put("state", "from b", fence=b)
        assert s.get("state") == "from b"
    finally:
        b.release()
    assert s.holder("leader") is None


@ch.remote
def meet(store, parties):
    return store.barrier("round", parties).wait(timeout=60)


def test_a_barrier_releases_everyone_together():
    s = ns()
    assert ch.gather(*(meet.remote(s, 3) for _ in range(3)), timeout=120) == [1, 1, 1]
    assert ch.gather(*(meet.remote(s, 3) for _ in range(3)), timeout=120) == [2, 2, 2]


def test_watch_delivers_changes_in_order():
    s = ns()
    s.put("jobs/old", 0)
    w = s.watch("jobs/")
    s.put("jobs/a", 1)
    s.put("other", "ignored")
    s.counter("jobs/count").add(5)
    s.delete("jobs/a")
    got = [next(w) for _ in range(3)]
    assert [(e.kind, e.key, e.value) for e in got] == [
        ("put", "jobs/a", 1),
        ("put", "jobs/count", 5),
        ("delete", "jobs/a", None),
    ]
    assert got[0].version < got[1].version < got[2].version


def test_async_watch_wakes_on_a_change():
    s = ns()
    w = s.watch("k")

    async def first():
        async for e in w:
            return e

    threading.Timer(0.3, lambda: s.put("k", "v")).start()
    e = asyncio.run(asyncio.wait_for(first(), 30))
    assert (e.kind, e.key, e.value) == ("put", "k", "v")


def test_ttl_entries_expire():
    s = ns()
    s.put("session", "alive", ttl="300ms")
    assert s.get("session") == "alive"
    time.sleep(0.6)
    assert s.get("session") is None


# ---- fencing across a leader failover ---------------------------------------------------

FAILOVER_A = """
import time, chimaera as ch
s = ch.Store("failover")
lease = s.lock("leader", ttl="1500ms").acquire()
s.put("state", "a", fence=lease)
print("TOKEN", lease.token, flush=True)
lease._stop.set()                      # a stalls: its lease will expire
while not __import__("os").path.exists(PROCEED):
    time.sleep(0.1)
try:
    s.put("state", "late a", fence=lease)
    print("FENCED no")
except ch.LeaseLost:
    print("FENCED yes")
"""


def test_fencing_holds_across_a_leader_failover():
    from test_cluster import Cluster

    c = Cluster(3, data_groups=1)
    try:
        import os
        import tempfile

        proceed = os.path.join(tempfile.mkdtemp(), "proceed")
        a = c.driver(0, FAILOVER_A.replace("PROCEED", repr(proceed)))
        token_a = None
        deadline = time.monotonic() + 60
        while token_a is None and time.monotonic() < deadline:
            line = a.stdout.readline()
            if line.startswith("TOKEN"):
                token_a = int(line.split()[1])
        assert token_a is not None, a.stderr.read() if a.poll() is not None else "no token"
        # Kill and restart every peer in turn (peer 0 last, since a's driver uses it):
        # whichever one led the namespace's group has failed over by the end.
        for i in (1, 2, 0):
            c.signal(i, signal.SIGKILL)
            c.start(i)
            c.wait_ready([i])
            time.sleep(1.0)
        out = c.run(
            2,
            """
import chimaera as ch
s = ch.Store("failover")
b = s.lock("leader", ttl="30s", timeout=60).acquire()
s.put("state", "b", fence=b)
print("B", b.token, s.get("state"))
""",
        )
        assert out.returncode == 0, out.stderr[-3000:]
        token_b = int(out.stdout.split("B ", 1)[1].split()[0])
        assert token_b > token_a
        open(proceed, "w").close()
        stdout, stderr = a.communicate(timeout=120)
        assert "FENCED yes" in stdout, stdout + stderr[-3000:]
    finally:
        c.close()


# ---- durable queues ---------------------------------------------------------------------


def qkey() -> str:
    return f"q-{uuid.uuid4().hex[:8]}"


def test_durable_queue_is_fifo_and_bounded():
    import queue

    q = ch.Queue(maxsize=3, key=qkey(), durable=True)
    assert isinstance(q, ch.Queue)
    for i in range(3):
        q.put(i)
    with pytest.raises(queue.Full):
        q.put(99, timeout=0.2)
    assert [q.get().result() for _ in range(3)] == [0, 1, 2] and q.qsize() == 0
    with pytest.raises(queue.Empty):
        q.get(timeout=0.2)
    q.put("last")
    q.close()
    assert list(q) == ["last"]
    with pytest.raises(ch.Closed):
        q.put("after close")


def test_a_failed_claim_is_delivered_again():
    q = ch.Queue(key=qkey(), durable=True)
    q.put("job")
    with pytest.raises(RuntimeError):
        with q.claim() as item:
            assert item == "job"
            raise RuntimeError("consumer failed")
    with q.claim() as item:
        assert item == "job"
    assert q.qsize() == 0


@ch.remote(max_attempts=1)
def die_holding_a_claim(q):
    import os

    with q.claim(lease="500ms"):
        os._exit(1)  # the consumer dies mid-item; its lease lapses


def test_an_item_whose_consumer_died_is_delivered_again():
    q = ch.Queue(key=qkey(), durable=True)
    q.put("job")
    with pytest.raises(ch.InfrastructureFailure):
        die_holding_a_claim.remote(q).result(timeout=60)
    assert q.get(timeout=10).result() == "job"


@ch.remote
def consume_all(q):
    return sorted(q)


def test_durable_queue_is_shared_by_key_across_calls():
    key = qkey()
    q = ch.Queue(key=key, durable=True)
    for i in range(20):
        q.put(i)
    q.close()
    parts = ch.gather(*(consume_all.remote(ch.Queue(key=key, durable=True)) for _ in range(3)), timeout=120)
    assert sorted(x for p in parts for x in p) == list(range(20))


def test_cluster_reports_this_peer():
    nodes = ch.cluster().nodes()
    assert len(nodes) == 1 and nodes[0].alive and nodes[0].this_peer
    assert ch.cluster().resources()["cpus"] == nodes[0].cpus > 0

    @ch.remote
    def where():
        return ch.context().node

    assert where.remote().result() == nodes[0].peer
