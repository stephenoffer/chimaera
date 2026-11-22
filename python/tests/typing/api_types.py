"""What a type checker sees in ordinary Chimaera code. test_typing.py runs mypy
(with the chimaera.mypy plugin) over this file: every assert_type must hold,
and each line marked `# type: ignore[...]` must still be an error there."""

from collections.abc import Iterator
from typing import assert_type

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


async def main() -> None:
    assert_type(await add.remote(1, 2), int)

add.options(cpus=2, effects="idempotent").remote(1, 2)
add.options(cpu=2)  # type: ignore[call-arg]
add.options(effects="sometimes")  # type: ignore[arg-type]
