"""Batched actor methods: @ch.method(batch=ch.Batch(...))."""

from __future__ import annotations

import re
import time

import pytest

import chimaera as ch
from chimaera import _batch


@ch.remote
class Embedder:
    def __init__(self):
        self.sizes: list[int] = []

    @ch.method(batch=ch.Batch(max_size=8, max_wait="50ms"))
    def embed(self, texts):
        self.sizes.append(len(texts))
        time.sleep(0.05)
        return [len(t) for t in texts]

    @ch.method(batch=ch.Batch(max_size=4, max_wait="10ms"))
    async def shout(self, words):
        return [w.upper() for w in words]

    @ch.method(batch=ch.Batch(max_size=4, max_wait="10ms"))
    def broken(self, xs):
        if "boom" in xs:
            raise ValueError("bad batch")
        return xs[:-1]  # one result short

    def batch_sizes(self):
        return self.sizes


def test_calls_are_batched_and_each_gets_its_own_result():
    e = Embedder.remote()
    texts = ["x" * i for i in range(40)]
    futures = [e.embed.remote(t) for t in texts]
    assert [f.result(timeout=60) for f in futures] == list(range(40))
    sizes = e.batch_sizes.remote().result(timeout=60)
    assert sum(sizes) == 40 and max(sizes) <= 8
    assert max(sizes) > 1, sizes  # calls submitted together ran together


def test_a_lone_call_waits_at_most_max_wait():
    e = Embedder.remote()
    e.embed.remote("warm").result(timeout=60)
    start = time.monotonic()
    assert e.embed.remote("abc").result(timeout=60) == 3
    assert time.monotonic() - start < 2


def test_async_batched_methods():
    e = Embedder.remote()
    assert ch.get((e.shout.remote(w) for w in ["a", "b", "c"]), timeout=60) == ["A", "B", "C"]


def test_errors_reach_every_call_in_the_batch():
    e = Embedder.remote()
    with pytest.raises(ValueError, match="bad batch"):
        e.broken.remote("boom").result(timeout=60)
    with pytest.raises(ValueError, match="1 results for a batch of 2|0 results for a batch of 1"):
        ch.get((e.broken.remote(x) for x in ["a", "b"]), timeout=60)
    with pytest.raises(TypeError):
        e.embed.remote("a", "b")  # the method takes one list: the caller's binding rejects two
    with pytest.raises(TypeError, match="call it with one argument"):
        e.embed.remote(texts="a").result(timeout=60)
    assert e.embed.remote("ok").result(timeout=60) == 2  # the actor keeps serving


def test_batch_is_validated_where_written():
    with pytest.raises(ch.OptionsError, match="put it on the method"):

        @ch.remote(batch=ch.Batch())
        def f(x):
            return x

    with pytest.raises(TypeError, match="drop group= and concurrency="):
        ch.method(batch=ch.Batch(), concurrency=2)
    with pytest.raises(ValueError, match="max_size"):
        ch.Batch(max_size=0)
    with pytest.raises(ValueError, match="max_bytes"):
        ch.Batch(max_bytes=0)
    assert ch.Batch(max_bytes="32MiB").max_bytes == 32 << 20


@ch.remote
class Sizes:
    def __init__(self):
        self.batches: list[int] = []

    @ch.method(batch=ch.Batch(max_size=16, max_wait="20ms", max_bytes="32MiB"))
    def take(self, items):
        self.batches.append(sum(len(x) for x in items))
        time.sleep(0.2)
        return [len(x) for x in items]

    def seen(self):
        return self.batches


def test_a_byte_budget_bounds_each_batch_and_what_its_actor_holds():
    budget = 32 << 20
    small, big = b"s" * 1024, b"b" * (8 << 20)
    items = [big if i % 3 == 0 else small for i in range(48)]  # 16 of 8 MiB: 128 MiB in all
    s = Sizes.remote()
    futures = [s.take.remote(x) for x in items]
    # While they wait, the peer says what the actor's process holds of them.
    held = []
    while not all(f.done() for f in futures):
        for f in futures:
            m = re.search(r"holds \d+ take calls with ([\d.]+) MiB of arguments", f.explain()["pending_reason"] or "")
            if m:
                held.append(float(m.group(1)))
    assert [f.result(timeout=120) for f in futures] == [len(x) for x in items]
    assert held and max(held) <= 32, held
    batches = s.seen.remote().result(timeout=60)
    assert sum(batches) == sum(len(x) for x in items) and max(batches) <= budget, batches


class _Executor:
    """What a Batcher uses of its executor; reporting call `fail`'s result fails."""

    def __init__(self, fail: bytes):
        self.instance, self.running, self.fail, self.sent = self, {}, fail, []
        self.conn = self

    def double(self, xs):
        return [2 * x for x in xs]

    def call_args(self, args_ref, deps):
        return (args_ref,), {}

    def ok(self, key, attempt, result, keep_local=False):
        return ("ok", result)

    def err(self, exc):
        return ("err", type(exc).__name__)

    def send_done(self, key, attempt, outcome, *rest):
        if key == self.fail and outcome[0] == "ok":
            raise ConnectionError("the reply did not go")
        self.sent.append((key, outcome))


def test_every_call_in_a_batch_reports_when_one_report_fails():
    ex = _Executor(fail=b"k1")
    msg = {"policy": {}, "deadline_unix_ms": 0, "actor": b"a", "durable": False, "retry_exception_types": []}
    batcher = _batch.Batcher(ex, "double", ch.Batch(max_size=4))
    batcher._run([(msg, (key, 1, x, [], 0)) for key, x in [(b"k0", 1), (b"k1", 2), (b"k2", 3)]])
    # Each report is what lets the peer send the method's next call.
    assert ex.sent == [(b"k0", ("ok", 2)), (b"k1", ("err", "ConnectionError")), (b"k2", ("ok", 6))]
