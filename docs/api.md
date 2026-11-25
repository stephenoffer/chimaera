# The Chimaera API

This is the contract for Chimaera's Python API: what each name does, what it
promises, and when it is available. A first program needs five things. The
rest is there when a program grows into a service, a training job or a
pipeline across many machines, and you reach it from objects you already hold
or keyword options you already know.

Examples in blocks marked **runs today** are executed by
`python/tests/test_docs.py` against a local peer, in order, in one process.
The other examples show planned API and say which phase adds it.

| Phase | Adds |
|---|---|
| M1 (today) | The local runtime: functions, futures, maps, actors, pools, options, recovery, accounting, reattach. Streaming generators are finishing. |
| P2 (today) | The rest of Tier 1: forwarding, multiple returns, per-call actor options, concurrency groups, `ch.actor`, subtree cancel, `Run.cancel`, priority, `ch.coord.Queue`, `ch.coord.Topic`. |
| P3 | Several machines: clusters on SSH, Kubernetes, Slurm and clouds, `ch.coord.Store`, placement, `ch.runs.cluster()`, shipped environments. |
| P4 | CPU at scale: sources, sinks and shuffle, autoscaled and shared pools, metrics. |
| P5 | Accelerators and gangs: device plugins, rank groups and collectives, reservations, compiled graphs. |
| P6 | Elastic infrastructure: autoscaling providers, tenants, budgets. |

## Rules the API follows

1. **Five things to start:** `@ch.remote`, `.remote()`, `.map()`, `.result()` or `await`, and `Class.remote()`.
2. **One handle type.** A call returns a `Future[T]`. Anything that yields many values returns `Results[T]`: maps, generators, subscriptions, watches.
3. **Settings are keyword options**, checked where you write them. A typo fails on that line with a suggestion. Your own keyword arguments never collide with options, because `.remote(...)` takes only your arguments.
4. **The same words mean the same thing everywhere.** `name=` is a stable identity (get or create) for actors, pools, queues, topics and groups; `detach=True` says something outlives the script.
5. **Three ways to run many copies of a class:** an actor is one identity, a pool is interchangeable replicas, a gang is SPMD ranks.
6. **Options never change a return type.** Generators are recognized from the function, multiple returns use `.split()`, gangs use `.gang()`.
7. **`async def` never changes semantics.** An async method runs one call at a time per actor, like a plain one, unless you ask for concurrency.
8. **A laptop and a cluster behave the same.** Local runs use the same runtime path as clusters.

## Tier 0: the first hour

Decorate a function. Calling it directly runs it here; `.remote()` runs it
through the runtime and returns a future. Pass futures into other calls: the
second call starts when its inputs are ready, and the values travel between
workers without coming back to your script. **Runs today:**

```python run
import chimaera as ch

@ch.remote
def word_count(text: str) -> int:
    return len(text.split())

@ch.remote
def add(a: int, b: int) -> int:
    return a + b

assert word_count("blue sky") == 2                     # an ordinary local call
total = add.remote(word_count.remote("blue sky"), word_count.remote("rain today"))
assert total.result() == 4
```

A future passed to a call arrives as its value, and the call starts once the
value is ready. That holds for a future passed directly and for one inside a
list, tuple or dict argument (dict values, one level deep), so a call that
takes a list of results never starts early and sits waiting on them. To pass
futures as futures, so the call can wait on them itself (with
`ch.as_completed`, say), wrap the argument in `ch.handles(...)`. **Runs today:**

```python run
@ch.remote
def total_of(counts: list[int]) -> int:
    return sum(counts)

@ch.remote
def first_done(futures: list[ch.Future[int]]) -> int:
    return next(iter(ch.as_completed(futures))).result()

parts = [word_count.remote(t) for t in ("a b", "c d e")]
assert total_of.remote(parts).result() == 5                  # values inside the list
assert first_done.remote(ch.handles(parts)).result() in (2, 3)
```

`fn.map(items)` applies a function to every item and streams results back in
order (`ordered=False` gives completion order). Only a bounded number of items
are in flight, so a slow consumer slows the producer instead of filling
memory. Leaving a `with` block cancels whatever is still running. A map starts
when it is first read, or as soon as the script submits or waits on anything
else, or at exit, whichever comes first. **Runs today:**

```python run
assert list(word_count.map(["a", "a b", "a b c"])) == [1, 2, 3]
with word_count.map(["x"] * 1000, ordered=False) as stream:
    assert sum(stream) == 1000
```

The decorator works on a class. Calls through one handle run in the order you
made them, including after the actor restarts on another peer. **Runs today:**

```python run
@ch.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self, by: int) -> int:
        self.value += by
        return self.value

counter = Counter.remote()
assert counter.increment.remote(add.remote(1, 2)).result() == 3
```

Futures are awaitable. **Runs today:**

```python run
import asyncio

async def main() -> int:
    return await add.remote(20, 22)

assert asyncio.run(main()) == 42
```

A generator function streams its values as they are produced (main session,
finishing M1):

```python
@ch.remote
def lines(path: str):
    with open(path) as f:
        yield from f

for line in lines.remote("/data/big.txt"):     # Results[str], with backpressure
    ...
```

`ch.init(cluster=...)` (P3) picks where the work runs: a profile name from
`~/.config/chimaera/clusters.toml`, `chimaera://host:port`, or `"auto"`. With
no argument a local peer starts on demand. See [deploy.md](deploy.md).

## Tier 1: everyday tools

### Options, scopes and partial application

Set options on the decorator, on one call with `.options(...)`, or for a block
with `with ch.defaults(...)`. The closest setting wins. Sizes need a byte unit.
**Runs today:**

```python run
heavy = word_count.options(cpus=2, memory="512 MiB")
assert heavy.remote("a b c").result() == 3

with ch.defaults(deadline="30s"):
    assert word_count.remote("x").result() == 1

@ch.remote
def scale(x: float, factor: float) -> float:
    return x * factor

double = scale.partial(factor=2)            # constants ship once, not per item
assert list(double.map([1, 2, 3])) == [2, 4, 6]
```

| Option | Meaning |
|---|---|
| `cpus`, `memory` | Reserve capacity. `ch.auto(...)` leaves the value to the runtime. An actor that asks for `cpus` holds them while it lives, and waits until some peer has them free; an actor no peer could ever hold fails as `ch.Infeasible`. |
| `memory_limit` | Enforced; going over raises `ch.OutOfMemory` with the measured peak. |
| `gpus`, `device` | Accelerators (P5 assigns and isolates them; see Tier 3). |
| `effects` | `"none"` (default), `"idempotent"` or `"external"`. External calls start at most once. |
| `max_attempts`, `retry_exceptions` | Infrastructure failures are retried; your exceptions only when listed. |
| `deadline` | Relative (`"30s"`) or a `datetime`; children inherit the earliest one. |
| `concurrency` | Calls an actor may run at once (default 1). |
| `max_in_flight`, `max_in_flight_bytes` | Bounds for a map's window. |
| `name`, `detach`, `restart`, `max_restarts` | Actor identity and recovery. |
| `replicas` | Pool size, or `ch.auto(...)` (P4 controller). |
| `latency="interactive"` | Never batch these calls. |
| `adaptive` | `"on"` (default: learned reservations apply), `"observe"` (record proposals only) or `"frozen"` (no learning). See [resources](guides/resources.md). |
| `cache` | `True` (keep results 7 days) or a time to live (`"30d"`): a repeat of a call returns its stored result. See [Caching results](#caching-results). |

### Caching results

`cache=True` reuses a function's earlier result when its code, its arguments and
the environment (the installed packages and the project's Python files) are the
same. A hit is a finished future, and nothing runs. **Runs today:**

```python run
@ch.remote(cache=True)                     # cache="30d" keeps results longer than the default 7 days
def features(path: str, version: int) -> int:
    return len(path) * version             # stands in for an expensive step

assert features.remote("data/a.csv", 1).result() == 10
again = features.remote("data/a.csv", 1)
assert again.result() == 10 and again.explain()["cache"] == "hit"   # nothing ran
```

- Only functions with `effects="none"` that are deterministic can be cached;
  anything else is an `OptionsError`.
- Arguments are keyed by value when they are plain data (None, bool, int, float,
  str, bytes, and lists, tuples or dicts with str keys of these), futures of other
  cached calls (so a pipeline of cached calls hits from end to end), or small
  `ch.put` values. Any other argument runs the call uncached, with one warning
  per function.
- Results up to 1 MiB pickled are stored in the cluster's replicated store,
  scoped to your tenant. Errors are not stored.
- Files and services the function reads are not part of the key. Pass what
  identifies them, such as a modification time or a dataset version, as an
  argument.
- A function from a script or notebook is keyed by its own code, so editing
  other cells keeps its results. Adding lines above it in the same file moves
  its line numbers, which is a miss.

### Waiting on many futures

**Runs today:**

```python run
futures = [word_count.remote("a " * n) for n in range(5)]
assert ch.get(futures) == [0, 1, 2, 3, 4]
assert sorted(f.result() for f in ch.as_completed(futures)) == [0, 1, 2, 3, 4]
```

### Large values

`ch.put` stores a value once; pass the reference to as many calls as you like.
**Runs today:**

```python run
words = ch.put(["blue", "sky"] * 50_000)

@ch.remote
def count(items: list[str], word: str) -> int:
    return items.count(word)

assert ch.get([count.remote(words, "blue"), count.remote(words, "sky")]) == [50_000, 50_000]
```

`ch.put` returns an already-finished `Future[T]`, so there is one handle
type everywhere: pass it like any other future.

### Errors

Your exception comes back with its own type and the remote traceback attached.
A request no machine can satisfy fails at once. **Runs today:**

```python run
@ch.remote
def parse(text: str) -> int:
    return int(text)

try:
    parse.remote("not a number").result()
except ValueError as e:
    assert "not a number" in str(e)

try:
    word_count.options(cpus=10_000).remote("x").result(timeout=10)
except ch.Infeasible as e:
    assert "cpus" in str(e)
```

| You see | It means |
|---|---|
| Your own exception | Your code raised it; it ran once (unless listed in `retry_exceptions`). |
| `ch.InfrastructureFailure`, `ch.errors.WorkerCrashed` | Workers kept dying until the attempts ran out. |
| `ch.OutOfMemory` | Over `memory_limit`; carries the peak and the limit. |
| `ch.OutcomeUnknown` | An `effects="external"` call may have run before its worker died. |
| `ch.Infeasible` | Nothing can ever run this request; the message names the constraint. |
| `ch.Cancelled`, `ch.DeadlineExceeded` | The call was stopped. |
| `ch.ActorDied` | The actor's process ended. |
| `ch.OptionsError` | Options conflict; the message names each field and where it was set. |
| `TimeoutError` | Your wait ended; the call is still running. |

`ch.NotFound` means nothing exists under a name (`ch.actor(name)`), `ch.errors.Closed`
that a queue or topic was closed, `ch.errors.VersionConflict` that a store's
compare-and-set lost, and `ch.errors.LeaseLost` that a fenced write's lock passed to
someone else. `ch.errors.GangFailed` means every generation of a gang lost a rank.
`ch.errors.QuorumLost` (an `InfrastructureFailure`) means a group of the cluster lost
the majority of its peers and took no writes for a minute; the message names
the group, and calls go through again once enough of its peers are back.
`ch.EnvironmentMismatch` is planned, for a driver whose environment no peer
can run.

### Actors: names, pools, context

`name=` gets or creates one named actor. A pool is a set of interchangeable
replicas; `pool.method.map(items)` spreads calls across them. Inside a call,
`ch.context()` says who you are. **Runs today:**

```python run
a = Counter.options(name="docs/counter").remote()
b = Counter.options(name="docs/counter").remote()
a.increment.remote(5).result()
assert b.increment.remote(1).result() == 6

@ch.remote
class Tokenizer:
    def __init__(self, lower: bool):
        self.lower = lower

    def split(self, text: str) -> list[str]:
        return (text.lower() if self.lower else text).split()

with Tokenizer.options(replicas=2).pool(True) as pool:
    assert list(pool.split.map(["A B", "C"])) == [["a", "b"], ["c"]]

@ch.remote
def whoami() -> str:
    return ch.context().task_id

assert whoami.remote().result() and ch.context() is None   # None in the driver
```

### Runs, reattaching, explaining

Every script gets a run. `ch.init()` returns it; a `Run` is its id string, so
another process can reattach with `ch.runs.attach(run_id)`, even after the local
peer restarted. `run.tasks()` lists its calls, `run.cancel()` stops them all,
and any call can explain what happened to it. **Runs today:**

```python run
run = ch.init()
assert ch.runs.attach(run).status()             # this run's calls, counted by state

f = word_count.remote("explain me")
f.result()
info = f.explain()
assert info["state"] == "Done" and info["attempts"]
```

What a run's calls print is kept on the peer that ran them, after the driver
has gone: `run.logs()` yields it line by line (`<call> <stream>| text`),
`run.logs(follow=True)` waits for more, and `chimaera logs RUN [--follow]`
prints it from a shell. On a cluster, output from calls on other peers also
reaches the driver as it is printed, and the peer the run was created on keeps
the whole run's output. **Runs today:**

```python run
@ch.remote
def shout(word: str) -> None:
    print(word.upper())

shout.remote("kept").result()
assert any(line.endswith("stdout| KEPT") for line in ch.runs.current_run().logs())
```

### Dynamic graphs and multiple returns

A call can return another call's future: the caller receives the final value,
and the worker is free while that future is computed, so recursion as deep as
you like never ties up workers. `returns=n` with `.split()` gives one future
per value, each its own dependency. **Runs today:**

```python run
@ch.remote
def fib(n: int) -> int:
    return n if n < 2 else add.remote(fib.remote(n - 1), fib.remote(n - 2))

assert fib.remote(10).result() == 55

@ch.remote(returns=2)
def bounds(xs: list[int]) -> tuple[int, int]:
    return min(xs), max(xs)

lo, hi = bounds.remote([4, 1, 9]).split()
assert add.remote(lo, hi).result() == 10
```

A consumer of one value decodes only that value. Where the values are stored
depends on the cluster. On a few peers, a result is one file: each peer
fetches it once for all of its consumers. When the consumers spread out, about
one per peer as in a large shuffle, each value of 64 KiB or more is a stored
object of its own, and a consumer fetches only its value. The executor picks
the layout from the live peer count (`worth_splitting` in `_executor.py`: one
more fetch is weighed as 1 MiB of transfer). Parts are freed with their result.

`future.cancel()` cancels the call and every call it started that has not
finished, including calls a cancelled body starts afterwards. Cancellation is
cooperative: a running body stops when it checks `ch.context().cancelled()`,
and `cancel(force=True)` kills its worker. `priority=` orders calls waiting
for capacity on a peer (higher first); it never stops a running call.

### More from actors

`.options()` on a method sets things for one call. `@ch.method` sets them for
every call of a method. A concurrency group runs its methods beside the actor's
other calls, so a slow call does not block them. `ch.actor(name)` finds a named
actor from anywhere. An actor handle reserves the names `close` and `id`
(`handle.close()` ends the actor), so give methods other names. **Runs today:**

```python run
import time

@ch.remote(groups={"io": 4})
class Service:
    def slow(self, seconds: float) -> str:
        time.sleep(seconds)
        return "done"

    @ch.method(group="io")
    def ping(self) -> str:
        return "pong"

    @ch.method(returns=2)
    def pair(self) -> tuple[str, str]:
        return "a", "b"

svc = Service.options(name="docs/service").remote()
busy = svc.slow.remote(2.0)
assert svc.ping.remote().result(timeout=1.5) == "pong"     # not stuck behind slow()
try:
    svc.slow.options(deadline="100ms").remote(0.3).result()
except ch.DeadlineExceeded:
    pass
a, b = svc.pair.remote().split()
assert (a.result(), b.result()) == ("a", "b")
assert ch.actor("docs/service").ping.remote().result() == "pong"
```

### Queues and topics

A `ch.coord.Queue` is a bounded first-in, first-out queue any process can use: pass
it to calls, or share it by `name=`. A full queue makes `put` wait. Iterating
takes items until the queue is closed and empty. A `ch.coord.Topic` keeps a bounded
log and delivers every item to every subscriber. Both are held by an actor,
so they last as long as their run and are not replicated. **Runs today:**

```python run
q = ch.coord.Queue(maxsize=100)

@ch.remote
def total(q) -> int:
    return sum(q)

t = total.remote(q)
for i in range(10):
    q.put(i).result()
q.close()
assert t.result() == 45

topic = ch.coord.Topic(retain=100)
sub = topic.subscribe("earliest")
for i in range(3):
    topic.publish(i).result()
topic.close()
assert list(sub) == [0, 1, 2]
```

## Tier 2: building applications

### Replicated state and coordination

A `ch.coord.Store` namespace holds keys replicated with Raft: it survives the loss
of any process and a minority of peers. Operations on one namespace are
linearizable. Every change bumps the namespace's revision. A key's version is
the revision of its last change, so `if_version` is a compare-and-set, and 0
means the key must not exist. Values are pickled and limited to 1 MiB; store a
`ch.put` future for larger data. A namespace lives in one Raft group, so
spread heavy traffic across several namespaces. **Runs today:**

```python run
store = ch.coord.Store("docs")
v1 = store.put("config", {"lr": 3e-4}, if_version=0)     # create only if absent
v2 = store.put("config", {"lr": 1e-4}, if_version=v1)    # compare-and-set
try:
    store.put("config", {"lr": 9.9}, if_version=v1)      # stale version
except ch.errors.VersionConflict as e:
    assert e.version == v2
assert store.get("config") == {"lr": 1e-4}
assert store.counter("seen").add(2) == 2

with store.lock("leader", ttl="10s") as lease:            # renewed while held
    assert store.holder("leader") == lease.holder
    store.put("state", "mine", fence=lease)               # refused if the lease was lost

changes = store.watch("jobs/")
store.put("jobs/1", "queued")
event = next(changes)
assert (event.kind, event.key, event.value) == ("put", "jobs/1", "queued")
```

`lock()` returns a lease: its `token` grows with every grant, and a write with
`fence=lease` fails with `ch.errors.LeaseLost` once another holder has taken over.
This is the safe way to elect a leader. `barrier(key, parties).wait()` blocks
until that many participants arrive, round after round. `ttl=` makes a key
expire. A watch that falls behind the namespace's bounded change log delivers
the current value of every key and continues; it may repeat a change, but it
never misses one. Anyone who holds the cluster token can write values that
readers unpickle, the same trust that running calls requires.

`ch.coord.WorkQueue(name)` keeps a queue of work items in a store, so it outlives any
process. A consumer `claim`s an item: the item is removed when the block
finishes, and delivered again if the block raises or the consumer dies. Its
methods are synchronous (`put` returns once the item is stored, `get` returns
the item), unlike `Queue`'s futures.
**Runs today:**

```python run
jobs = ch.coord.WorkQueue("docs/jobs")
jobs.put("resize image 7")
with jobs.claim(lease="30s") as job:
    assert job == "resize image 7"
assert jobs.qsize() == 0
```

### The cluster

`ch.runs.cluster()` reports what the connected peer knows about the cluster, and
`ch.context().node` names the peer a call runs on. **Runs today:**

```python run
nodes = ch.runs.cluster().nodes()
assert nodes and all(n.cpus > 0 for n in nodes)
assert ch.runs.cluster().resources()["cpus"] >= 1
```

`ch.runs.object_locations(futures)` names the peers each finished value is
stored at, with its size: `{"peers": [...], "size": bytes, "inline": bool}`
per future, the peer ids being those of `nodes()`. A call's result names the
peer it was stored at (with a retained value's copies), and a `ch.put` value
made here this peer. A small value held inline in its handle has no peers
(`inline` is true). A `ch.put` made inside a call on another peer is not
located: its size, no peers. Background copies are not listed either.
**Runs today:**

```python run
value = ch.put(bytes(1 << 20))
where = ch.runs.object_locations([value])[value]
assert where["peers"] and where["size"] >= 1 << 20
```

`ch.runs.cluster().events(kinds, start=)` streams what the cluster does: members
joining and leaving (`member`), actors starting, restarting and dying
(`actor`), calls failing for good (`task_failed`), controller `decision`s and
`drain`s. By default it starts at what happens next and waits for more;
`start="earliest"` begins at the oldest the peer still holds (or pass a
sequence number), and `follow=False` stops
at the newest. Events are notifications; `Run.outcomes()` and `ch.actor(name)`
are the durable record. **Runs today:**

```python run
stream = ch.runs.cluster().events(["actor"])
temp = Counter.remote()
temp.close()
for event in stream:                      # Event(seq, at, kind, node, run, task, actor, message, detail)
    if event.actor == temp.id and event.message.startswith("dead"):
        break
stream.close()
```

### Pools

A pool spreads calls over interchangeable replicas, sending each call to the
least busy one. `route(key)` pins a key to one replica, and resizing moves
only a few keys. `resize(n)` and `upgrade(...)` change the replicas without
dropping calls: a leaving replica takes no new calls and closes once its calls
finish. `name=` names a pool that other processes and calls share; it can be
passed to a call like any value. `replicas=ch.auto(initial, min=, max=)` grows
the pool when its calls back up and shrinks it after a quiet minute, and
`pool.decisions` says why. **Runs today:**

```python run
@ch.remote
class Model:
    def __init__(self, weights: str):
        self.weights = weights

    def generate(self, prompt: str) -> str:
        return f"{self.weights}: {prompt}"

pool = Model.options(replicas=2).pool("weights-v1")
pool.route("user-7").generate.remote("hi").result()   # user-7 always reaches the same replica
pool.resize(3)
pool.upgrade(Model, "weights-v2", surge=1)            # one replica at a time
assert pool.generate.remote("hi").result() == "weights-v2: hi"
pool.close()

shared = Model.options(name="docs/model", replicas=ch.auto(1, max=4)).pool("weights-v1")
assert len(Model.options(name="docs/model").pool()) == 1   # a second handle on the same replicas
shared.close()
```

### Batched methods

`@ch.method(batch=ch.Batch(max_size=, max_wait=))` writes a method for a list
and calls it for one item. The actor collects calls until it has `max_size` or
the first has waited `max_wait`, runs the method once, and gives each caller
its element of the returned list. If the method raises, every call in the
batch gets the error. **Runs today:**

```python run
@ch.remote
class Embedder:
    @ch.method(batch=ch.Batch(max_size=32, max_wait="5ms"))
    def embed(self, texts: list[str]) -> list[int]:
        return [len(t) for t in texts]        # one call for up to 32 texts

embedder = Embedder.remote()
assert ch.get((embedder.embed.remote(t) for t in ["a", "bb", "ccc"])) == [1, 2, 3]
```

### Partitioned data

`ch.data.partitions(...)` cuts input into partitions: a list, a file glob (one
partition per file; `.jsonl`, `.csv`, `.parquet` or lines), or any
`ext.Source`. `columns=` and `filter=` are pushed down where the format
allows. `reduce_by_key` regroups `(key, value)` results by key between calls,
so the data moves between workers, not through the driver. Its result is a
`ch.data.Partitioned`: iterate it for each partition's dict (key to value; to
the list of values without a combine), or call `to_dict()`.
`write` stores each partition from the worker that holds it, then commits once. **Runs today:**

```python run
import operator
import tempfile

from chimaera import ext

@ch.remote
def count_words(lines) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for line in lines:
        for word in line.split():
            counts[word] = counts.get(word, 0) + 1
    return list(counts.items())

lines = ["the cat", "the dog", "a cat"] * 10
# A map nothing has read yet runs its function inside reduce_by_key's scatter
# calls: the pairs it returns are never stored in between.
totals = count_words.map(ch.data.partitions(lines, n=3)).reduce_by_key(operator.add, partitions=2)
assert totals.to_dict()["cat"] == 20
manifest = totals.write(ext.FileSink(tempfile.mkdtemp())).result()   # one file per partition
assert manifest["rows"] == 4 and len(manifest["parts"]) == 2
```

### Durable state, workflows and placement

Durable actors, durable workflows and placement on peer labels work today;
[actors](guides/actors.md), [failures](guides/failures.md) and
[resources](guides/resources.md) run them. A per-call environment is still
planned.

```python
# Durable actors: state survives the process; a repeated command id returns
# the stored reply.
@ch.remote(state=ch.durable.State(), restart="checkpoint")
class Cart: ...
cart = Cart.options(name="cart/42").remote()

# Durable workflows: replayed from the run's record after any process dies.
@ch.remote(workflow="durable")
async def checkout(order):
    payment = await charge.remote(order)
    if payment.declined:
        await notify.remote(order, "declined")
    return await ship.remote(order)

# Placement: hard requirements and preferences on peer labels.
@ch.remote(placement=ch.dist.Placement(require={"zone": "us-east1-b"}, prefer=("rack=r7",)))
def shard(i): ...

# Per-call environment (planned).
@ch.remote(env=ch.Env(pip=["pandas==2.2.3"]))
def report(df): ...
```

## Tier 3: accelerators, gangs and fast paths

### Devices

`gpus=N` counts accelerator units of any vendor. `device="KIND[:MODEL]"`
narrows it: `KIND` is `cuda`, `rocm`, `tpu`, `neuron`, `hpu`, `xpu` or `mps`,
and `MODEL` is a case-insensitive prefix such as `H100`, `MI300X`, `v5e` or
`trn2`. `device` alone means one unit, and a fraction takes a whole unit until
sharing is measured. Each call sees only the devices it was given: the peer
sets `CUDA_VISIBLE_DEVICES`, `HIP_VISIBLE_DEVICES`, `TPU_VISIBLE_CHIPS` and
the rest, and a call without `gpus=` sees none. A request no peer can meet is
`ch.Infeasible`, with the reason. **Runs today** (verified on Apple GPUs; the
other vendors are checked against captured host trees until they run on that
hardware):

```python run
@ch.remote
def where() -> str:
    return ch.dist.device()          # "cuda:0", "xla:0", "hpu", "xpu:0", "mps" or "cpu", no torch import

assert where.remote().result() == "cpu" and ch.dist.devices() == []
try:
    where.options(gpus=64).remote().result()
except ch.Infeasible as e:
    assert "accelerator" in str(e)
```

```python
@ch.remote(gpus=8, device="cuda:H100")
def train_step(shard): ...       # ch.dist.devices(): kind, id, index, torch, backend, model, memory_bytes
```

### Gangs and collectives

A gang runs the same function on N ranks that start together, meet at a
rendezvous, and fail together. Ranks get `RANK`, `WORLD_SIZE`, `LOCAL_RANK`,
`LOCAL_WORLD_SIZE`, `MASTER_ADDR` and `MASTER_PORT`, so PyTorch, JAX and
DeepSpeed code runs unchanged; `group.torch()` initializes
`torch.distributed` with the devices' backend (`gloo` on CPU). If a rank is
lost, every rank stops and the gang starts again as the next `generation`
(resume from your checkpoint), up to `max_attempts`; a rank that raises fails
the gang with its error. **Runs today:**

```python run
@ch.remote(cpus=0.5)
def shard_sum(rows: int) -> int:
    group = ch.context().gang                     # rank, size, local_rank, generation
    mine = sum(range(group.rank, rows, group.size))
    return sum(group.all_gather(mine))            # also broadcast(obj, src=) and barrier()

assert shard_sum.options(ranks=4).gang(100).result() == [4950] * 4
```

```python
@ch.remote(gpus=1)
def train(steps: int) -> float:
    dist = ch.context().gang.torch()              # torch.distributed, initialized
    ...

losses = train.options(ranks=16).gang(1000).result()    # one value per rank
```

A class makes a long-lived gang: its ranks are built together, so `__init__`
can set up `torch.distributed`, and a method call runs on every rank and gives
one future of the results. A lost rank breaks the gang (its calls raise
`ch.ActorDied`); `restart()` and `resize(n)` build the ranks again as the next
generation. **Runs today:**

```python run
@ch.remote(cpus=0.5)
class Worker:
    def __init__(self, base: int):
        self.rank = ch.context().gang.rank
        self.base = base

    def step(self) -> int:
        return self.base + self.rank

with Worker.options(ranks=2).gang(100) as workers:
    assert workers.step.remote().result() == [100, 101]
    assert workers[1].step.remote().result() == 101
```

**Elastic gangs.** `ranks=ch.auto(initial, min=, max=)` sizes each generation
to what the live peers can hold (by CPUs, and GPUs when the ranks need them).
A gang that loses a peer comes back smaller instead of waiting for a
replacement, and `gang.decisions` says what each generation chose. A running
generation never grows; new capacity is used from the next one. **Runs today:**

```python run
@ch.remote(cpus=0.5)
def world() -> int:
    return ch.context().gang.size

assert world.options(ranks=ch.auto(2, min=1, max=4)).gang().result() == [2, 2]  # starts at 2
```

**Groups of actors you already have.** `ch.dist.group(*actors)` makes them one
group, ranked in the order given. Pass the handle to them; inside a member,
`handle.join()` is its `Group`, with the same collectives and `torch()` as a
gang's rank. The first `join()` in each member waits for all of them.
`ch.dist.group(..., name=k)` again with the same key starts a new generation, for a
changed set of actors. **Runs today:**

```python run
@ch.remote(cpus=0.5)
class Learner:
    def __init__(self, weight: int):
        self.weight = weight

    def average(self, handle: ch.dist.GroupHandle) -> float:
        group = handle.join()
        return group.all_reduce(self.weight) / group.size   # sum by default; op= any function

learners = [Learner.remote(w) for w in (1, 2, 6)]
handle = ch.dist.group(*learners)
assert [f.result() for f in [l.average.remote(handle) for l in learners]] == [3.0] * 3
```

**When a gang hangs,** `explain()` (on a gang, an actor gang or a group handle)
says which collective each rank last entered and names the ranks holding the
others up. A collective's timeout says the same. Each collective waits under
its own name, so ranks that call different collectives at the same point wait
for each other, and `explain()` says so, rather than exchanging the wrong
data. It sees the rendezvous and the store collectives (`barrier`,
`all_gather`, `all_reduce`, `broadcast`), not calls inside `torch.distributed`.

```python
print(gang.explain()["summary"])
# rank 5 last entered #41 (all_gather, 312 s ago); ranks 0, 1, 2, 3, 4, 6, 7 wait in #42 (barrier)
```

Ranks are placed one by one today and meet through the replicated store, so a
gang larger than the free capacity waits at the rendezvous. Coming with the
shard's gang commit (P5): all-or-nothing placement.

`group.mesh(dp=2, tp=4)` splits a group's ranks into a grid (the last axis
varies fastest, as in torch's DeviceMesh) and gives this rank's group along
each axis, with the store collectives and `ranks` (their ranks in the whole
group). An axis has no torch process group of its own: after `group.torch()`,
use torch's `init_device_mesh` or `dist.new_group(axis.ranks)`. **Runs today:**

```python run
@ch.remote(cpus=0.25)
def row_sum() -> int:
    group = ch.context().gang
    tp = group.mesh(dp=2, tp=2)["tp"]                 # ranks 0,1 and 2,3
    return tp.all_reduce(group.rank)

assert row_sum.options(ranks=4).gang().result() == [1, 1, 5, 5]
```

### Reservations

`ch.dist.reserve(bundles, strategy=)` holds resources on peers before anything
runs there: one bundle per dict (`cpus`, `gpus`, `memory`, `device`,
`resources`), placed `"pack"` (one peer when it fits), `"spread"` (different
peers), or the `"strict_"` versions of either, which are infeasible rather than
approximate. `ready()` (or `await`) waits until every bundle is held. A call
or actor created `within=res[i]` runs inside bundle `i`, on its peer and its
resources; `release()` gives them back. Calls in one bundle share its process,
so give each parallel worker its own bundle. **Runs today:**

```python run
with ch.dist.reserve([{"cpus": 1}, {"cpus": 1}], strategy="pack") as res:
    @ch.remote
    def node_of() -> str:
        return ch.context().node

    assert node_of.options(within=res[0]).remote().result() == res[0].node
    tally = Counter.options(within=res[1]).remote()
    assert tally.increment.remote(2).result() == 2
```

A reservation holds every bundle or none. If it can't hold them all in time,
it gives back what it took and tries again after a random pause, so
reservations competing for the same CPUs take turns instead of each holding
part. `ready(timeout)` raises `TimeoutError` while it keeps trying;
`release()` gives up. The holds are taken from the peers' load reports, not
in one atomic step: a burst against stale reports can over-commit briefly
before one reservation backs off. Atomic holds come with the shard's gang
commit.

### Budgets

`budget={"cpu_seconds": ..., "gpu_seconds": ...}` caps what a call and every
call made inside it may use, in seconds or durations (`"8h"`). Once either is
used up, the call and everything it submitted are cancelled with
`ch.errors.BudgetExceeded` (a `ch.Cancelled`), whose message names the budget and
what was used. Usage is charged while calls run, about once a second, so one
long call is stopped too:
- **CPU seconds:** a call's measured CPU when it has its worker to itself, else its CPU grant times the time it ran.
- **GPU seconds:** its GPUs times that time.

A budget covers the call until it finishes. A call inside a budgeted call
can't set its own budget (`ch.OptionsError`): budgets don't nest. **Runs
today:**

```python run
import time

@ch.remote
def trial(seconds: float) -> float:
    end = time.process_time() + seconds
    while time.process_time() < end:
        pass
    return seconds

@ch.remote(cpus=0.1)
def search(n: int) -> float:
    return sum(ch.get((trial.remote(1.0) for _ in range(n))))

try:
    search.options(budget={"cpu_seconds": 2}).remote(16).result()
except ch.errors.BudgetExceeded as e:
    print(e)  # budget exceeded: cpu_seconds=2 allowed, 2.31 used by this call and the calls it made
else:
    raise AssertionError("16 CPU-seconds of trials should not fit a 2-second budget")
```

### Device transport, compiled graphs (P5)

```python
@ch.remote(transport="device")     # tensors move GPU to GPU, not through host memory
def stage(t): ...

@ch.graph
def serve(request):
    tokens = tokenizer.encode.remote(request)
    return model.generate.remote(tokens)
fast = serve.compile(max_in_flight=2)        # reusable channels, no per-call bookkeeping
reply = fast.remote("hello").result()
```

## Tier 4: extending Chimaera (`chimaera.ext`)

Sources, sinks and serializers plug in today. Sources and sinks are pickled to
the workers that use them, so keep their state small. A sink's `write` may run
more than once for a part (a retried call) and must be idempotent; `commit`
runs once with every part's receipt. **Runs today:**

```python run
class Squares(ext.Source):
    def __init__(self, n: int):
        self.n = n

    def partitions(self):                     # small, picklable descriptors
        return [range(i, min(i + 10, self.n)) for i in range(0, self.n, 10)]

    def read(self, part, columns=None, filter=None):
        return [x * x for x in part]

class Total(ext.Sink):
    def write(self, part_id, items):          # idempotent per part
        return sum(items)

    def commit(self, receipts):               # once, with every receipt
        return sum(receipts)

@ch.remote
def keep(rows) -> list[int]:
    return list(rows)

assert keep.map(ch.data.partitions(Squares(25))).write(Total()).result() == sum(x * x for x in range(25))
```

`ext.register_serializer(cls, dumps=, loads=)` pickles a type your way;
`loads` must be importable (a module-level function), and every process that
sends the type registers it. `ext.register_reducer(cls, reduce)` changes only
the values Chimaera stores and sends, not a plain `pickle.dumps` elsewhere in
the program, and `reduce` may return a lambda or a local function, which
travels inside each value; `ext.unregister_reducer(cls)` undoes it. Planned:

```python
class RackAware(ext.PlacementPolicy):         # ranks reservations and gangs (P5)
    def rank(self, request, candidates): ...

class QueueDepth(ext.Controller):             # proposals go through canary and rollback (P4)
    def observe(self, window): ...
```

## What you can build from these

| Pattern | Built from |
|---|---|
| MapReduce, streaming data pipelines | `partitions` → generator `map` → `Pool.map` → `reduce_by_key` → `write(sink)` |
| Parameter server | keyed actors or `Pool.route` shards, concurrency groups, `put` |
| Data, tensor, pipeline and expert parallel training | `.gang()`, `Group.mesh(...)`, collectives, `pack="nvlink"` |
| Elastic training | `ranks=ch.auto(...)`, `generation`, `GangFailed`, checkpoints |
| Model serving with batching | keyed `Pool`, `batch=`, `replicas=ch.auto`, `deadline`, `Overloaded`, generator methods, `upgrade`, `ch.graph` |
| Work queues | `Queue` with `claim` |
| RL rollout loops | generator-method rollout pool, learner gang, `ch.dist.group().broadcast` |
| Hyperparameter search with early stopping | trials as calls or gangs, `context().report()`, `Topic`, `as_completed`, subtree `cancel`, `budget` |
| Sharded state, locks, leader election | `Store` |
| Sagas | durable workflows with `try`/`except` and `command_id` |
| Dynamic task graphs | futures as arguments, forwarding, `as_completed` |

## Coming from Ray

`python -m chimaera.compat.ray app.py` runs a Ray Core program unchanged, and
`import chimaera.compat.ray as ray` works in your own code.
[compat-ray.md](compat-ray.md) maps every Ray Core API and option.
