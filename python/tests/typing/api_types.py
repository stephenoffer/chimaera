"""What a type checker sees in ordinary Chimaera code. test_typing.py runs mypy
(with the chimaera.mypy plugin) over this file: every assert_type must hold,
and each line marked `# type: ignore[...]` must still be an error there."""

from collections.abc import AsyncIterator, Iterator
from typing import Any, assert_type

import chimaera as ch


@ch.remote
def add(a: int, b: int) -> int:
    return a + b


@ch.remote
async def fetch(url: str) -> bytes:
    return url.encode()


@ch.remote
def count(n: int) -> Iterator[int]:
    yield from range(n)


@ch.remote(cpus=2)
def heavy(x: float) -> float:
    return x


@ch.remote
class Counter:
    def __init__(self, start: int) -> None:
        self.value = start

    def increment(self, by: int) -> int:
        self.value += by
        return self.value


assert_type(add(1, 2), int)  # a local call is the function itself
assert_type(add.remote(1, 2), ch.Future[int])
assert_type(add.remote(1, 2).result(), int)
assert_type(fetch.remote("u"), ch.Future[bytes])
assert_type(heavy.remote(1.0), ch.Future[float])
assert_type(ch.put([1, 2]), ch.Future[list[int]])
assert_type(ch.put([1, 2], durability="retained"), ch.Future[list[int]])
ch.put([1, 2], durability="replayable")  # type: ignore[arg-type]
assert_type(ch.gather(add.remote(1, 2), add.remote(3, 4)), ch.Future[list[int]])
assert_type(ch.get(add.remote(1, 2)), int)
assert_type(ch.get([add.remote(1, 2), add.remote(3, 4)]), list[int])
done, pending = ch.wait([add.remote(1, 2)], num_returns=1, timeout=5)
assert_type(done, list[ch.Future[int]])
assert_type(count.remote(3), ch.typing.Stream[int])
for item in count.remote(3):
    assert_type(item, int)

add.remote("one", 2)  # type: ignore[arg-type]
add.remote(1)  # type: ignore[call-arg]

counter = Counter.remote(0)
assert_type(counter.increment.remote(2), ch.Future[int])
Counter.remote("zero")  # type: ignore[arg-type]
counter.increment.remote("two")  # type: ignore[arg-type]
counter.decrement.remote(1)  # type: ignore[misc]
assert_type(counter.increment.policy().describe(), str)  # read-only, with each value's source
assert_type(heavy.policy().sources["cpus"], str)
context = ch.context()
if context is not None:
    assert_type(context.describe_policy(), str)  # the same words, inside a call made with explain=True
add.options(explain=True).remote(1, 2)


async def main() -> None:
    assert_type(await add.remote(1, 2), int)


async def readings() -> AsyncIterator[float]:
    yield 1.0


async def grouped() -> None:
    with ch.TaskGroup() as group:
        assert_type(group.add(add.remote(1, 2)), ch.Future[int])
        group.add(3)  # type: ignore[arg-type]
    async with ch.TaskGroup() as group:
        group.add(fetch.remote("u"))
    async for value in heavy.map(readings()):  # an async source: read with async for
        assert_type(value, float)

add.options(cpus=2, effects="idempotent").remote(1, 2)
assert_type(ch.coord.Store("typing").get_versioned("k"), tuple[Any, int])  # value and version from one read
assert_type(ch.coord.Store("typing").scan("k/").revision, int)  # a watch from it keeps the scan current
assert_type(ch.coord.Topic[int]().subscribe(cursor="reader").gaps, list[ch.coord.Gap])  # exactly what it missed
assert_type(ch.coord.Topic[int]().subscribe(cursor="reader").ack(), ch.Future[None])
add.options(cpu=2)  # type: ignore[call-arg]
add.options(effects="sometimes")  # type: ignore[arg-type]
assert_type(add.options(ranks=2, rendezvous_timeout=30.0).gang(1, 2), ch.dist.Gang[int])  # admitted all or none
add.options(ranks=2, rendezvous_timeout="soon")  # type: ignore[arg-type]
assert_type(add.options(ranks=2, collective_timeout=60.0).gang(1, 2), ch.dist.Gang[int])  # a stalled collective reforms
add.options(ranks=2, collective_timeout="soon")  # type: ignore[arg-type]
assert_type(ch.dist.group(counter, collective_timeout=60.0), ch.dist.GroupHandle)  # a group's collectives, bounded
ch.dist.group(counter, collective_timeout="soon")  # type: ignore[arg-type]


class Ledger(ch.durable.Activity[str]):
    """An activity's run takes its own arguments after the key."""

    idempotent = True

    def run(self, key: str, cents: int) -> str:
        return f"{key}:{cents}"


def parse(text: str) -> int:
    return int(text)


@ch.remote(workflow="durable", version=2, compatible=(1,))
async def checkout(order: str) -> str:
    assert_type(await ch.durable.record(parse, "1"), int)
    receipt = await ch.durable.record(Ledger(), 500, key=order)
    assert_type(receipt, str)
    await ch.durable.compensate(Ledger(), order, receipt)
    await ch.durable.compensate(Ledger(), order, 5)  # type: ignore[misc]
    return receipt


try:
    checkout.remote("order-1").result()
except ch.OutcomeUnknown as unknown:
    assert_type(unknown.key, str | None)
except ch.errors.WorkflowDivergence as diverged:
    assert_type(diverged.step, int | None)


class Upsert(ch.ext.Sink):
    """A sink declares what a commit whose worker died does."""

    effects = "idempotent"

    def write(self, part_id: int, items: list[Any]) -> int:
        return len(items)


class Typo(ch.ext.Sink):
    effects = "sometimes"  # type: ignore[assignment]


assert_type(Upsert().begin("write key"), ch.ext.Sink)
assert_type(ch.ext.FileSource("out").residual("out/a.parquet", [("size", "==", 11)]), list[Any])
big = ch.ext.FileSource("logs/*.parquet", split_bytes=64 << 20, batch_rows=8192)
assert_type(big.batches(("logs/a.parquet", 0, 4), columns=["id"]), Iterator[Any])
assert_type(ch.data.partitions(big)[0].batches(), Iterator[Any])
assert_type(heavy.map([1.0, 2.0]).reduce_by_key(max, partitions=4, memory="2GiB"), Any)


class Columns(ch.ext.Sink):
    """A sink that takes an Arrow partition batch by batch."""

    def write_batches(self, part_id: int, batches: Any) -> int:
        return sum(batch.num_rows for batch in batches)


class Memory(ch.ext.ObjectStore):
    """An object store a FileSink publishes to with conditional puts."""

    def put(self, key: str, body: Any, *, sha256: str | None = None, if_version: str | None = None) -> str:
        return "1"


assert_type(Memory().put("out/_manifest.json", b"{}", if_version=""), str)
assert_type(Memory().head("out/part-00000.jsonl"), tuple[int, str] | None)
assert_type(ch.ext.S3Store("bucket", endpoint="http://127.0.0.1:9000").get("out/_manifest.json"), tuple[bytes, str] | None)
ch.ext.FileSink("out", "parquet", store=ch.ext.S3Store("bucket"))
ch.ext.FileSink("out", store="s3://bucket")  # type: ignore[arg-type]


@ch.remote
class Renderer:
    """Per-method bounds: a method's mailbox, a batch's bytes."""

    @ch.method(mailbox=ch.Mailbox(max_calls=8, on_full="reject"))
    def render(self, page: int) -> str:
        return str(page)

    @ch.method(batch=ch.Batch(max_size=8, max_bytes="32MiB"))
    def embed(self, texts: list[str]) -> list[int]:
        return [len(t) for t in texts]


ch.Mailbox(max_calls=8, on_full="wait", max_wait="2s")
ch.Mailbox(max_calls=8, on_full="drop")  # type: ignore[arg-type]
ch.durable.State(version=2, migrate=lambda state, version: state)


# Training: checkpoints a restarted gang resumes from, and a rank's share of the input.
ckpts = ch.dist.Checkpoints("bert-7", "/shared/ckpt", keep=3)
assert_type(ckpts.latest(), ch.dist.Checkpoint | None)
assert_type(ckpts.save(10, cursor={"epoch": 1}, meta={"loss": 0.5}).checkpoint, ch.dist.Checkpoint | None)
ch.dist.Checkpoints("bert-7", ch.ext.S3Store("bucket"))
feed = ch.data.loader("data/*.parquet", batch_size=32, columns=["x", "y"], num_workers=2)
assert_type(feed, ch.data.Loader)
assert_type(feed.state(), dict[str, Any])
ch.data.loader("data/*.parquet")  # type: ignore[call-arg]
