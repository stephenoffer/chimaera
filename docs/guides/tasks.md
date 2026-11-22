# Tasks and futures

Put `@ch.remote` on a function and it runs on Chimaera's workers. Then chain
calls without routing values through your script, or fan out over thousands
of items while Chimaera keeps the work in flight bounded.

## Run a function remotely

```python run
import chimaera as ch

@ch.remote
def word_count(text: str) -> int:
    return len(text.split())

@ch.remote
def add(a: int, b: int) -> int:
    return a + b

assert word_count("blue sky") == 2          # a plain local call, as before
future = word_count.remote("blue sky")      # runs on a worker
assert future.result() == 2
```

The decorator leaves `word_count(...)` a plain local call.
`word_count.remote(...)` checks the arguments against the signature before
anything leaves your process, so `add.remote(1)` raises `TypeError` on that
line. Then it returns a `ch.Future` at once. Your script's first call
connects to a peer, starting a local one if none is running
([clusters.md](clusters.md) covers clusters).

Arguments and results travel by cloudpickle, which handles lambdas and
closures. An argument that can't be pickled, like a lock or an open file,
raises `ch.SerializationError` naming it: pass what the function needs to
rebuild it (a path, settings) and create it inside. CPUs, memory and GPUs are
in [resources.md](resources.md).

## Wait for a result

```python run
import time

@ch.remote
def nap(seconds: float) -> float:
    time.sleep(seconds)
    return seconds

slow = nap.remote(1.0)
try:
    slow.result(timeout=0.1)
except TimeoutError:
    pass                                    # your wait ended; the call did not
assert slow.result() == 1.0 and slow.done()
```

| On a future | What it does |
|---|---|
| `f.result(timeout=None)` | Waits, then returns the value or raises the call's error. A timeout raises `TimeoutError` and leaves the call running. |
| `await f` | The same, inside a coroutine ([integrations.md](integrations.md)). |
| `f.done()` | True once the call has finished, however it ended. |
| `f.exception(timeout=None)` | Waits, then returns the call's error, or `None`. |
| `f.add_done_callback(fn)` | Calls `fn(f)` on a callback thread when the call finishes, in the order added. |
| `f.running()` | True while a worker runs it (asks the peer). |
| `f.cancel(force=False)` | Stops it; see the next section. |
| `f.cancelled()` | True if it ended canceled, past its deadline or over its budget. |
| `f.explain()` | A dict with `state`, `pending_reason`, `attempts` and `failures`. |
| `f.metrics()` | What it reported with `ch.context().report(...)`. |
| `f.id` | Its id, as hex. |

Your exception comes back with its own type and the remote traceback
([failures.md](failures.md)). One that nothing reads is printed on stderr.

Waiting on each call from one line before making the next draws warning CH101
after 200 in a row. Submit everything first, or use `fn.map`.

## Cancel a call

```python run
f = nap.remote(30)
assert f.cancel() is True                   # it had not finished
try:
    f.result()
except ch.Cancelled:
    assert f.cancelled()
finished = add.remote(1, 2)
finished.result()
assert finished.cancel() is False           # too late: the result stays
assert finished.result() == 3
```

`cancel()` returns at once. `True` means the call had not finished, and its
result now raises `ch.Cancelled` (unless it finishes first). `False` means it
had, and it keeps its outcome. Canceling a call also cancels every call it
started, including ones it starts afterward.

A running call gets a grace period to stop by itself, so check
`ch.context().cancelled()` in long loops. After 2 s Chimaera interrupts it (a
`time.sleep` or a pure-Python loop on its main thread raises `ch.Cancelled`);
after 5 s it kills the worker process, though never an actor's, since the
actor's state lives there.

`cancel(force=True)` kills the process running the call immediately, an
actor's included. Later calls on that actor raise `ch.ActorDied`.

## Pass futures into other calls

A future passed as an argument arrives as its value, sent straight from the
worker that made it once it is ready. Your script never downloads it.

```python run
total = add.remote(word_count.remote("blue sky"), word_count.remote("rain today"))
assert total.result() == 4

@ch.remote
def total_of(counts: list[int]) -> int:
    return sum(counts)

counts = [word_count.remote(t) for t in ("a b", "c d e")]
assert total_of.remote(counts).result() == 5      # futures inside a list

@ch.remote
def first_done(futures: list[ch.Future[int]]) -> int:
    return next(iter(ch.as_completed(futures))).result()

assert first_done.remote(ch.handles(counts)).result() in (2, 3)
```

If the input failed, the dependent call fails with the same error.

Futures one level inside a list, tuple or dict value arrive as values too.
Deeper ones, and those in dict keys, sets, or subclasses of list, tuple and
dict, arrive as a `ch.Future` the function can wait on. To pass futures as
futures on purpose, wrap the argument in `ch.handles(...)` like `first_done`.

For a large value that many calls read, `ch.put(value)` stores it once and
returns a finished future to pass like any other
([../objects.md](../objects.md)).

## Map a function over many items

`fn.map` fans a function out over thousands of items, a window at a time.

```python run
assert list(word_count.map(["a", "a b", "a b c"])) == [1, 2, 3]
assert list(add.map([1, 2], [10, 20])) == [11, 22]     # several sources zip, as map() does

with word_count.map(["x y"] * 1000, ordered=False) as results:
    assert sum(results) == 2000

@ch.remote
def inverse(x: float) -> float:
    return 1 / x

assert [o.index for o in inverse.map([1, 0, 2]).outcomes() if o.error] == [1]
assert list(word_count.options(latency="interactive").map(["a", "b c"])) == [1, 2]
```

`fn.map(items)` returns a `ch.Results`, a single-pass iterator in input
order; `ordered=False` yields results as they finish. A failed item raises its
error at its position. Catch it and keep iterating, or read `.outcomes()`,
whose `(index, value, error)` tuples never raise.

More items go out as you read, so a slow consumer slows the producer instead
of filling memory. The default window is the larger of 256 items and 32 per
usable core (widened so every core can hold two batches), and 256 MiB of
arguments plus the measured size of results. A `max_in_flight` or
`max_in_flight_bytes` you set is a hard cap. Leaving a `with` block or calling
`.close()` cancels the items still running and closes the source iterator.

Fast items are batched. Each batch is one call that grows from one item, by
the measured time per item, to about 0.1 s of worker time (4096 items at
most).
Every item still gets its own value or error, and slow items are never
grouped.

Good to know:

- A map your script creates submits nothing until you read it, make or wait
  on any other call, or exit, so `reduce_by_key` can take it over and run the
  function inside its own calls ([data.md](data.md)). A map made inside a call
  starts right away.
- Batching is off for `effects="external"`, `retry_exceptions`, `async def`
  and generator functions, and `latency="interactive"` (last line above),
  which sends each item as its own call right away and stops the peer from
  grouping quick plain calls of one function into a single worker round trip.
  `latency` accepts a duration too, but today only `"interactive"` changes
  anything.

## Chain stages and bind constants

Bind constants once, then pipe one map into the next without values passing
through your script.

```python run
@ch.remote
def scale(x: float, factor: float) -> float:
    return x * factor

double = scale.partial(factor=2)
assert double(4) == 8                       # still callable locally
assert list(word_count.map(["a", "b c"]).map(double)) == [2, 4]
```

`fn.partial(*args, **kwargs)` binds arguments as `functools.partial` does and
returns a remote function whose bound values ship once with the code, not with
every call. That matters for a big map over a large lookup table. Options from
`.options(...)` carry over.

`.map(other_fn)` on a map's results chains a second stage, and each result
reaches it as a future. `.futures()` gives the futures without reading values.

## Wait on many futures

```python run
futures = [word_count.remote("a " * n) for n in range(5)]
assert ch.get(futures) == [0, 1, 2, 3, 4]

done, pending = ch.wait(futures, num_returns=2, timeout=10)
assert len(done) >= 2 and len(done) + len(pending) == 5

assert sorted(f.result() for f in ch.as_completed(futures)) == [0, 1, 2, 3, 4]
assert ch.gather(add.remote(1, 2), add.remote(3, 4)).result() == [3, 7]
```

| Helper | Returns | Notes |
|---|---|---|
| `ch.get(f, timeout=None)` | The value; for an iterable of futures, a list of values in order | Raises the first error. One timeout covers them all; `TimeoutError` leaves the calls running. |
| `ch.wait(futures, num_returns=None, timeout=None, fetch_local=False)` | `(done, pending)`, each in the order given | Waits for all by default. Values are not decoded; `fetch_local=True` copies finished results to this machine first. |
| `ch.as_completed(futures, timeout=None)` | Futures, in completion order | Works with `for` and `async for`. The timeout raises `TimeoutError`. |
| `ch.gather(*futures)` | One `Future` of a list | Fails with the first error. Await it, call `.result()`, or pass it to another call, which receives the list. Unpack a list: `ch.gather(*futures)`. |

`ch.get` on a list holds every value in your script at once. Past a quarter of
the machine's memory it warns (CH117): consume results with `ch.as_completed`
or `fn.map`, or reduce them inside calls.

## Build the graph as you go

A call can return a future instead of a value, and whoever waits gets that
future's final value. The worker is free as soon as the function returns, so
recursion like `fib` never ties up workers, however deep it goes.

```python run
@ch.remote
def fib(n: int) -> int:
    if n < 2:
        return n
    return add.remote(fib.remote(n - 1), fib.remote(n - 2))   # a future, not an int

assert fib.remote(10).result() == 55

@ch.remote(returns=2)
def bounds(xs: list[int]) -> tuple[int, int]:
    return min(xs), max(xs)

lo, hi = bounds.remote([4, 1, 9]).split()
assert add.remote(lo, hi).result() == 10 and lo.result() == 1
```

With `returns=n`, `.split()` gives one future per value. Each part is a
separate dependency, so a call that takes `lo` receives only that value. The
function must return a tuple or list of exactly `n` values, or reading a part
raises `ValueError`. Without `returns=`, call `.split(n)`. Canceling a part
cancels the whole call.

## Stream results from a generator

On a generator function, `.remote()` returns a stream (`ch.typing.Stream`)
that hands you items in order as the generator yields them.

```python run
from collections.abc import Iterator

@ch.remote
def countdown(n: int) -> Iterator[int]:
    for i in range(n, 0, -1):
        yield i

assert list(countdown.remote(3)) == [3, 2, 1]

with countdown.remote(1_000_000) as stream:
    assert next(stream) == 1_000_000        # leaving the block stops the generator
```

The generator pauses once it is `max_in_flight` items ahead of you (default
64). An exception in it is raised after the items before it.
`stream.close()`, like leaving a `with` block, stops the generator and drops
what you haven't taken.

If the worker dies mid-stream, the generator runs again from the start on
another worker and skips the items you already have, so make it yield the same
items each time.

Streams work with `async for` and as the input of `fn.map(...)`, and
`async def` generators stream too. A function that returns a generator without
being one (no `yield` in its body) fails with
`TypeError: ... returned a generator`.

## Set options, deadlines and priorities

```python run
@ch.remote(max_attempts=2)
def fetch_page(url: str) -> int:
    return len(url)

assert fetch_page.options(priority=5).remote("a.example").result() == 9

with ch.defaults(deadline="30s", max_attempts=5):
    policy = fetch_page.policy()
    assert policy.get("max_attempts") == 2          # the decorator wins over the block
    assert policy.get("deadline") == 30.0           # the block fills in what is unset
    assert fetch_page.remote("b.example").result() == 9

try:
    fetch_page.options(max_attempt=4)
except ch.OptionsError as e:
    assert "did you mean 'max_attempts'?" in str(e)

try:
    nap.options(deadline="300ms").remote(5).result()
except ch.DeadlineExceeded:
    pass
```

The first of these that sets an option wins:

1. `fn.options(...)`, a configured copy; the original is unchanged.
2. The decorator, `@ch.remote(...)`.
3. `with ch.defaults(...)` blocks, innermost first (`async with` works too).
4. What the runtime has learned about the function ([resources.md](resources.md)).
5. Built-in defaults.

`fn.policy().describe()` lists each effective value and where it came from.
A typo or a bad value raises `ch.OptionsError` on the line where you wrote it,
with a suggestion.

A deadline is `"250ms"`, `"30s"`, `"5m"`, `"2h"`, a number of seconds, or a
`datetime`. It counts from `.remote()`, so time queued for a worker counts
too. When it passes, the call is canceled and its result raises
`ch.DeadlineExceeded`, a subclass of `ch.Cancelled`. Calls made inside it
inherit the deadline, and the earlier one wins. `result(timeout=...)` only
limits how long you wait.

`priority` (an integer, default 0) puts higher values first among calls
waiting for a peer. It never stops a call that is already running.

| Option | Default | Meaning |
|---|---|---|
| `deadline` | none | Cancel the call this long after `.remote()`. |
| `priority` | `0` | Order among calls waiting for room on a peer. |
| `max_in_flight` | map: max(256, 32 per core); stream: 64 | Map items in flight, or stream items produced ahead of you. |
| `max_in_flight_bytes` | `256 MiB` | Argument bytes in flight for a map. |
| `latency` | unset | `"interactive"` turns batching off. |
| `max_attempts`, `retry_exceptions`, `effects` | `3`, none, `"none"` | Retries; see [failures.md](failures.md). |
| `cpus`, `memory`, `gpus`, `memory_limit` | see [resources.md](resources.md) | What a call reserves. |

[../api.md](../api.md) lists every option.

## Read the call's own context

```python run
@ch.remote
def where_am_i() -> tuple[int, str]:
    return ch.context().attempt, ch.context().task_id

attempt, task_id = where_am_i.remote().result()
assert attempt == 1 and len(task_id) == 32
assert ch.context() is None                 # None in your script
```

Inside a call, `ch.context()` carries `run_id`, `task_id`, `attempt` (1 on
the first try), `item_index` (the position in a map), `cpus`, `node` (the
peer's id) and `actor_id` (`None` outside an actor). `cancelled()` says
whether the caller canceled, and `report(**metrics)` sends progress the
caller reads with `future.metrics()`.

In a batched map, every item of a batch sees the batch's first index; use
`latency="interactive"` if each item needs its own.

## Know what happens when your script exits

At exit, Chimaera waits for every call and map you never read to finish, as
`concurrent.futures` waits for its pool, and says so on stderr:

```text
chimaera: waiting for 4 unfinished calls and 1 unread map before exiting (Ctrl-C cancels them; ch.init(detach=True) lets work outlive the script)
```

Streams nobody reads are stopped, not waited on. If the script raised
(`chimaera: cancelling 1 unfinished call: the script failed`), or you press
Ctrl-C before or during that wait, the unfinished work is canceled instead.
`ch.shutdown()` does the same at any point (`cancel=True` cancels instead of
waiting); the next call connects again.

To let the work outlive the script, call `ch.init(detach=True)` before its
first call (after that it is silently ignored). Reattach from any other
process by the run's id:

```python
import time
import chimaera as ch

run = ch.init(detach=True)

@ch.remote
def slow(i: int) -> int:
    time.sleep(2)
    return i * 10

for i in range(3):
    slow.remote(i)
print(run)                  # the run id; the script exits at once
```

```python
import chimaera as ch       # the second script, any time later

run = ch.runs.attach("0000024d0e181fff138223c6e3fcab81")   # the id printed above
run.wait()
print(sorted(value for _, value, _ in run.outcomes()))     # [0, 10, 20]
run.finish()                # a detached run lasts until you finish it
```

Next: keep state between calls with [actors.md](actors.md).
