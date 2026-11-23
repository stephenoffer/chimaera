# Use Chimaera from the tools you already have

Already using `concurrent.futures`, joblib or asyncio? Your code runs on
Chimaera with a line or two changed, and mypy checks your remote calls the way
it checks local ones.

## Swap in for a process pool

`ch.Executor` is a `concurrent.futures.Executor`. Change just the
constructor in code written against `ProcessPoolExecutor`, and it runs on one
machine or a whole cluster.

```python run
import concurrent.futures

import chimaera as ch

def square(x: int) -> int:
    return x * x

with ch.Executor() as ex:
    futures = [ex.submit(square, i) for i in range(5)]
    done, _ = concurrent.futures.wait(futures)
    assert sorted(f.result() for f in done) == [0, 1, 4, 9, 16]
    assert list(ex.map(square, range(4))) == [0, 1, 4, 9]

    offset = 10
    assert ex.submit(lambda x: x + offset, 1).result() == 11   # lambdas and closures work
```

`submit` returns a standard `concurrent.futures.Future`. The constructor takes
the same call options as `@ch.remote(...)` and applies them to every call. It
won't take `max_workers`, though: the cluster's capacity, not a pool size,
decides how many calls run at once.

```python run
ex = ch.Executor(cpus=0.5, max_attempts=1)
assert ex.submit(lambda: ch.context().cpus).result() == 0.5
ex.shutdown()

try:
    ch.Executor(max_workers=4)
except ch.OptionsError as e:
    assert "unknown option 'max_workers'" in str(e)
```

<details>
<summary>Callbacks and two rough edges</summary>

`concurrent.futures.wait`, `as_completed`, `add_done_callback` and
`asyncio.wrap_future` all accept these futures. Done-callbacks run on a
thread of their own, so a slow one never holds up other results.

- A function already decorated with `@ch.remote` keeps its options. Where
  both set one, the executor's wins.
- Pass a class to `submit` and it's called like a function: you get its
  instance back. It never becomes an actor ([actors.md](actors.md)).
- A generator function raises `TypeError`. Stream it with
  `ch.remote(fn).remote(...)` instead ([tasks.md](tasks.md)).
- `future.cancel()` cancels the Chimaera call and returns `True` if it hadn't
  finished yet. `shutdown(cancel_futures=True)` cancels everything still pending.

Two rough edges remain:

- `shutdown(wait=False)` also stops the thread that completes futures, so any
  still pending never complete. Stick with the `with` block or a plain
  `shutdown()`.
- A future you cancel just before the executor shuts down can print
  `chimaera: done-callback raised RuntimeError('cannot schedule new futures after shutdown')`
  on stderr. Ignore it. The future's already canceled.

</details>

## Run joblib's Parallel on Chimaera

Import `chimaera.joblib` and it registers a backend named `"chimaera"`. It
needs joblib (`pip install joblib`) and raises `ImportError` without it.

```python
import chimaera.joblib  # registers the "chimaera" backend
from joblib import Parallel, delayed, parallel_config

def square(x):
    return x * x

with parallel_config(backend="chimaera", n_jobs=4):
    squares = Parallel()(delayed(square)(i) for i in range(100))
assert squares == [i * i for i in range(100)]
```

<details>
<summary>How joblib batches run</summary>

Each batch joblib dispatches becomes one Chimaera call, placed on whichever
peers have room. joblib sizes its batches by how long they take, so lots of
small tasks end up sharing a call. `n_jobs` is the number of batches in flight.
By default, and with `-1`, that's the CPU count of the machine your script
runs on.

If a task raises, `Parallel` raises that error, and the batches still running
get canceled. `return_as="generator"` works too.

</details>

## Use it from asyncio

Every Chimaera future is awaitable from any event loop without blocking it,
so `asyncio.gather` and `asyncio.wait_for` take futures directly.

```python run
import asyncio

@ch.remote
def add(a: int, b: int) -> int:
    return a + b

async def main() -> list[int]:
    one = await add.remote(1, 2)
    pair = await ch.gather(add.remote(1, 1), add.remote(2, 2))
    many = await asyncio.gather(*(add.remote(i, i) for i in range(3)))
    return [one, *pair, *many]

assert asyncio.run(main()) == [3, 2, 4, 0, 2, 4]
```

Inside a coroutine, reach for `await`: `.result()` blocks the loop until the
call finishes. A timeout from `asyncio.wait_for`, or canceling the task that
awaits, ends your wait but leaves the call running. `.cancel()` on the future
stops it.

A map, a generator's stream and `ch.as_completed` all work with `async for`,
and `with ch.defaults(...)` has an `async with` form.

```python run
async def collect() -> tuple[list[int], list[int]]:
    mapped = [x async for x in add.map(range(3), range(3))]
    finished = [await f async for f in ch.as_completed([add.remote(5, 5)])]
    return mapped, finished

assert asyncio.run(collect()) == ([0, 2, 4], [10])
```

### Async remote functions

```python run
@ch.remote
async def add_then_double(x: int) -> int:
    first = await add.remote(x, 1)          # await other calls inside
    await asyncio.sleep(0.01)
    return first * 2

assert add_then_double.remote(3).result() == 8
```

Each call to an `async def` remote function runs its coroutine to the end on a
fresh event loop in the worker. The future holds whatever the coroutine
returns. An `async def` generator streams its items just like a plain one.

### Async actor methods

```python run
@ch.remote
class Fetcher:
    async def get(self, i: int) -> int:
        await asyncio.sleep(0.2)
        return i

fetcher = Fetcher.options(concurrency=4).remote()
assert ch.get([fetcher.get.remote(i) for i in range(4)]) == [0, 1, 2, 3]
```

`async` alone doesn't change how many calls run at once. It's still one,
unless you set `concurrency`. With `concurrency=4`, up to four calls overlap at
their `await` points.

<details>
<summary>Where async methods run</summary>

An actor's async methods run on one event loop in its process.
[actors.md](actors.md) covers the rest.

</details>

## Check types with mypy

Good news for functions: they need nothing extra. `@ch.remote` keeps the
signature, so mypy checks `.remote(...)` arguments and gives `.result()` the
function's return type. Classes need a plugin, since mypy ignores what a class
decorator returns.
Add it to `mypy.ini`:

```ini
[mypy]
plugins = chimaera.mypy
```

or to `pyproject.toml`:

```toml
[tool.mypy]
plugins = ["chimaera.mypy"]
```

With the plugin on, mypy checks `Counter.remote(...)` against `__init__` and
`handle.method.remote(...)` against the method. Try it on this file:

```python
import chimaera as ch


@ch.remote
def add(a: int, b: int) -> int:
    return a + b


@ch.remote
class Counter:
    def __init__(self, start: int) -> None:
        self.value = start

    def increment(self, by: int) -> int:
        self.value += by
        return self.value


counter = Counter.remote(0)
reveal_type(counter.increment.remote(1))
add.remote("one", 2)
counter.increment.remote("two")
counter.decrement.remote(1)
add.options(cpu=2)
```

```bash
$ mypy app.py
app.py:20: note: Revealed type is "chimaera._future.Future[int]"
app.py:21: error: Argument 1 to "remote" of "RemoteFunction" has incompatible type "str"; expected "int"  [arg-type]
app.py:22: error: Argument 1 to "remote" of "RemoteMethod" has incompatible type "str"; expected "int"  [arg-type]
app.py:23: error: "Counter" has no method "decrement"  [misc]
app.py:24: error: Unexpected keyword argument "cpu" for "options" of "RemoteFunction"; did you mean "cpus"?  [call-arg]
app.py:24: note: "options" defined in "chimaera._remote"
Found 4 errors in 1 file (checked 1 source file)
```

<details>
<summary>Inferred types and known gaps</summary>

Those calls get real types: `Counter.remote(...)` is an `Actor[Counter]`, and
a method call is a `Future` of the method's return type.

Skip the plugin and mypy reports `"type[Counter]" has no attribute "remote"`
instead, and the handle's calls go unchecked (their type is `Any`). You get the
errors on `add` either way, since functions and `.options(...)` are typed
without it. Using Pyright? The plugin's own notes say you don't need it there:
Pyright types handles as `Actor[Counter]`, and their methods accept any
arguments.

Known gaps:

- `.remote(...)` takes the function's own parameter types. Passing a future
  where the function expects its value works at run time, but mypy calls it
  an `arg-type` error (`incompatible type "Future[int]"; expected "int"`).
- The plugin adds only `remote` to a class, so mypy still reports
  `Counter.options(...)`, and with it `.pool(...)`, as a missing attribute.
- A call to an `async def` actor method is typed as a `Future` of a
  coroutine, not of the value the method returns.
- A generator function needs a return annotation (`-> Iterator[int]`).
  Without one, mypy types the call as a `Future`, not a stream.

</details>

For your own annotations, `ch.Future[T]` and `ch.Results[T]` are at the top
level, and `chimaera.typing` has the rest: `RemoteFunction`, `RemoteMethod`,
`ActorClass`, `Actor`, `Pool`, `Stream`, `Outcome`, `Options` and
`TaskContext`.

```python run
from chimaera.typing import Actor, RemoteFunction

@ch.remote
class Tally:
    def __init__(self) -> None:
        self.total = 0

    def add(self, n: int) -> int:
        self.total += n
        return self.total

def run_twice(fn: RemoteFunction[[int, int], int]) -> list[int]:
    return ch.get([fn.remote(1, 2), fn.remote(3, 4)])

def bump(tally: Actor[Tally]) -> ch.Future[int]:
    return tally.add.remote(1)

assert run_twice(add) == [3, 7]
assert bump(Tally.remote()).result() == 1
```

## Run a Ray program

Coming from Ray? `python -m chimaera.compat.ray app.py` runs an unmodified Ray
script.
`import chimaera.compat.ray as ray` maps Ray Core's API onto Chimaera, and
anything that doesn't map raises `Unsupported` naming the Chimaera
alternative. [from-ray.md](from-ray.md) walks you through a port.

<details>
<summary>Which Ray APIs map</summary>

The mapping covers `remote`, `get`, `put`, `wait`, `cancel`, `kill`, named
and detached actors, and `ActorPool`. [../compat-ray.md](../compat-ray.md)
has the full table.

</details>

Next: run all of this on several machines with [clusters.md](clusters.md).
