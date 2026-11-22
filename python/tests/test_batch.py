"""Batched actor methods: @ch.method(batch=ch.Batch(...))."""

from __future__ import annotations

import time

import pytest

import chimaera as ch


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
