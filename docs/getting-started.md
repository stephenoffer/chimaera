# Getting started

Ten minutes from here, you'll have run functions in parallel, chained them
without waiting, kept state in an actor and looked at what the runtime did.
Every example runs on a laptop as written, and the same code runs on a
cluster without changes.

You need Chimaera installed: build it from source as the
[README](../README.md#install) shows, or `pip install` a wheel from
`make wheel`.

## Your first remote function

```python run
import chimaera as ch

@ch.remote
def square(x: int) -> int:
    return x * x

assert square(4) == 16                     # a plain call runs right here
assert square.remote(4).result() == 16     # .remote() runs it in a worker
```

Save that as `first.py` and run `python first.py`. The first `.remote()` call
starts a local peer in the background (its files live in `~/.chimaera/local`),
and the peer stops by itself after ten idle minutes. Your script runs once:
workers receive the function, so your top-level code doesn't run again in
them.

`.remote()` returns a `Future` straight away. `.result()` waits for the value,
and `.result(timeout=5)` gives up after five seconds with `TimeoutError` while
the call keeps running. You can also `await` a future.

## Chain calls without waiting

Pass a future into another call. The second call starts when its inputs are
ready, and the value moves between workers without coming back to you.

```python run
@ch.remote
def add(a: int, b: int) -> int:
    return a + b

total = add.remote(square.remote(3), square.remote(4))
assert total.result() == 25
```

That works for a future inside a list, tuple or dict argument too, one level
deep. If a call fails, everything that depends on it fails with the same
error and never runs.

## Run many items

`fn.map(items)` streams results back in order. Only a bounded number of items
are in flight at once, so a slow loop slows the producer down instead of
filling memory, and fast items are batched for you.

```python run
assert list(square.map(range(5))) == [0, 1, 4, 9, 16]

with square.map(range(1000), ordered=False) as stream:   # completion order
    for y in stream:
        if y > 100:
            break                                         # leaving the block cancels the rest
```

For a handful of futures, `ch.get(futures)` returns their values in order and
`ch.as_completed(futures)` yields them as they finish.

```python run
futures = [square.remote(n) for n in range(4)]
assert ch.get(futures) == [0, 1, 4, 9]
assert sorted(f.result() for f in ch.as_completed(futures)) == [0, 1, 4, 9]
```

A generator function sends its items back as it yields them:

```python run
import os
import tempfile

log = os.path.join(tempfile.mkdtemp(), "app.log")
with open(log, "w") as f:
    f.write("ok\nerror: disk\nok\nerror: net\n")

@ch.remote
def errors(path: str):
    with open(path) as f:
        for line in f:
            if line.startswith("error"):
                yield line.strip()

assert list(errors.remote(log)) == ["error: disk", "error: net"]
```

## Keep state in an actor

Put the decorator on a class and each instance lives in its own worker.
Calls through one handle run in the order you made them.

```python run
@ch.remote
class Tally:
    def __init__(self):
        self.seen = []

    def add(self, item: str) -> int:
        self.seen.append(item)
        return len(self.seen)

    def items(self) -> list[str]:
        return self.seen

t = Tally.remote()
for word in ["a", "b", "c"]:
    t.add.remote(word)
assert t.items.remote().result() == ["a", "b", "c"]
t.close()
```

A method that raises leaves the actor usable. `Tally.options(name="tally")`
gets or creates a named actor any process can find, and
`Tally.options(replicas=4).pool()` gives you interchangeable replicas.
[Actors](guides/actors.md) covers all of it, including actors whose state
survives a crash.

## Let the runtime size your calls

You usually don't need to say how much CPU or memory a call takes. The peer
measures each function, and after a few calls it reserves what they were seen
to use, so calls wait for memory rather than run out of it. When you do know,
say so:

```python run
@ch.remote(cpus=2, memory="512 MiB")
def heavy(x: int) -> int:
    return x + 1

assert heavy.options(cpus=1).remote(5).result() == 6      # override for one call
with ch.defaults(deadline="30s"):                          # or for a block
    assert heavy.remote(6).result() == 7
```

Sizes need a unit: `"512 MiB"` or `"2 GB"`, and each means what it says. A
misspelled option fails on the line where you wrote it. GPUs, memory limits
and the tuning you can watch are in
[Resources and GPUs](guides/resources.md).

## When something fails

```python run
@ch.remote
def parse(text: str) -> int:
    return int(text)

try:
    parse.remote("not a number").result()
except ValueError as e:                  # your exception, with the remote traceback attached
    assert "not a number" in str(e)
```

| You see | It means |
|---|---|
| Your own exception | Your code raised it. It ran once, unless you listed it in `retry_exceptions`. |
| `ch.InfrastructureFailure` | Workers kept dying; all three attempts were used. |
| `ch.OutcomeUnknown` | An `effects="external"` call may have run before its worker died. |
| `ch.Infeasible` | No machine could ever run this request; the message says why. |
| `ch.Cancelled`, `ch.DeadlineExceeded` | The call was stopped. |
| `TimeoutError` | Your wait ended; the call is still running. |

When your script exits, it waits for the calls it started and never waited
on. Ctrl-C cancels them, and `ch.init(detach=True)` leaves them running so
you can come back later with `ch.runs.attach(run_id)`.
[Failures](guides/failures.md) explains retries, effects and recovery.

## See what happened

Every call can explain itself:

```python run
f = square.remote(7)
f.result()
info = f.explain()
assert info["state"] == "Done" and info["attempts"]
```

And the `chimaera` command shows the rest:

```bash
chimaera status                  # CPU in use, the queue, why work is waiting
chimaera runs                    # recent runs: ids, state, the command that started them
chimaera logs RUN                # what a run's calls printed
chimaera explain RUN --decisions # what the runtime tuned, and why
chimaera profiles                # per function: calls, time, cores, what the next call reserves
```

## Take it to a cluster

Start a `chimaera` peer on each machine, then point your script at them:

```python
ch.init(cluster="lab")           # a profile in ~/.config/chimaera/clusters.toml
```

or run it unchanged with `chimaera run --cluster lab -- python app.py`.
[Run on a cluster](guides/clusters.md) walks through it.

## A few words

| Word | Meaning |
|---|---|
| script | your program, the process that calls `.remote()` (also called the driver) |
| peer | the `chimaera` process on each machine; it schedules work and stores values |
| worker | a Python process a peer starts to run calls |
| call | one `.remote(...)`; it runs in a worker |
| future | what `.remote()` returns; `.result()` waits for the value |
| actor | an instance of a `@ch.remote` class, living in one worker |
| run | everything one script submitted; its id lets you inspect it or reattach |

Next: [Tasks and futures](guides/tasks.md), then the rest of the
[guides](README.md).
