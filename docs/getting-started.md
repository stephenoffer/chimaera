# Getting started

This walks through a first program, then the parts you will use most. Every
example runs on one machine today. The code is meant to run unchanged on a
cluster once multi-peer support lands.

## 1. Your first remote function

```python
import chimaera as ch

@ch.remote
def square(x: int) -> int:
    return x * x

print(square(4))                   # 16, runs here like any function
print(square.remote(4).result())   # 16, runs in a worker process
```

Save it as `first.py` and run `python first.py`. The first `.remote()` call
starts a local peer if none is running. Your script runs once: workers receive
the function itself, so top-level code is not executed again.

## 2. Connect calls without waiting

Pass a future into another call. The second call starts when the first one
finishes, and the value moves between workers without coming back to your
script.

```python
@ch.remote
def add(a: int, b: int) -> int:
    return a + b

total = add.remote(square.remote(3), square.remote(4))
print(total.result())              # 25
```

If a call fails, everything that depends on it fails with that error and never
runs.

## 3. Many items

```python
for y in square.map(range(10)):
    print(y)

results = square.map(range(1000), ordered=False)   # completion order
with square.map(huge_iterable) as stream:           # stop early safely
    for y in stream:
        if y > 100:
            break
```

The runtime keeps a bounded number of items in flight and batches fast items
for you. Leaving the `with` block cancels whatever is still running.

To wait on a handful of futures, use `ch.gather(*futures)` (values in order) or
`ch.as_completed(futures)` (futures as they finish).

## 4. State in an actor

```python
@ch.remote
class Tally:
    def __init__(self):
        self.seen = []

    def add(self, item):
        self.seen.append(item)
        return len(self.seen)

    def items(self):
        return self.seen

t = Tally.remote()
for word in ["a", "b", "c"]:
    t.add.remote(word)
print(t.items.remote().result())   # ['a', 'b', 'c'], in the order you called
t.close()
```

`async def` methods are ordered too. If you want calls to overlap,
say so: `@ch.remote(concurrency=4)`.

## 5. Ask for resources

```python
@ch.remote(cpus=2, memory="2 GiB", memory_limit="3 GiB")
def train_step(batch): ...
```

`cpus` and `memory` reserve capacity. `memory_limit` is enforced: a call that
goes over it is stopped and retried twice more (for now under that limit, not a
larger one), then fails with `ch.OutOfMemory` and the peak it reached.

Sizes need a unit: `"512 MiB"`, `"2 GB"`. Library thread pools (OpenMP, BLAS)
are sized to the CPUs you asked for.

## 6. When something goes wrong

| You see | It means |
|---|---|
| Your own exception, with a remote traceback note | Your code raised it; it ran once |
| `ch.InfrastructureFailure` | Workers kept dying; three attempts were used |
| `ch.OutcomeUnknown` | An `effects="external"` call may have run before its worker died |
| `ch.Infeasible` | Nothing can ever run this request; the message says why |
| `ch.Cancelled` / `ch.DeadlineExceeded` | The call was stopped |
| `TimeoutError` | Your wait ended; the call is still running |

`future.explain()` returns the state, the reason it is waiting, and each
attempt's measurements.

## 7. Come back to a run

Every script gets a run id (`ch.init()` returns it). Another process can
reattach and read the results, even after the local peer restarted:

```python
run = ch.attach("0000...")         # the run id
run.wait()
for task_id, value, error in run.outcomes():
    ...
```

Start a run with `ch.init(detach=True)` (or `chimaera run --detach -- python app.py`)
to keep its work going after your script exits.

## 8. Environment variables

Workers do not see your shell's environment. A run carries only locale
settings and `PYTHONHASHSEED`, plus what you name:

```python
ch.init(env={"MODEL_DIR": "/data/models"})
```

or, from the shell, `CHIMAERA_ENV_PASSTHROUGH=MODEL_*,MY_FLAG`. Never pass
secrets this way: the run's variables are stored with the run. Put secrets
(cloud credentials, tokens) in the environment of the peer itself.

## 9. See what happened

```bash
chimaera status
chimaera inspect RUN
chimaera explain RUN --task TASK
chimaera explain RUN --decisions
```
