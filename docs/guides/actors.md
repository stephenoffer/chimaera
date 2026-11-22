# Actors: state that lives in a worker

An actor is an instance of your class that lives in its own worker process and
keeps its state between calls. Use one when calls share something expensive or
mutable: a loaded model, a connection, a counter. You can create actors, call
them in order, find them by name, restart them after a crash, spread calls over
a pool, batch calls and keep state through crashes.

## Create an actor and call it

Put `@ch.remote` on a class. `Class.remote(...)` starts an instance in a worker
and returns a handle, and `handle.method.remote(...)` runs a method there and
returns a future.

```python run
import chimaera as ch

@ch.remote
class Counter:
    def __init__(self, start: int = 0):
        self.value = start

    def add(self, by: int = 1) -> int:
        self.value += by
        return self.value

counter = Counter.remote(10)
futures = [counter.add.remote() for _ in range(100)]
assert [f.result() for f in futures] == list(range(11, 111))   # in the order you made them
assert Counter(0).add(5) == 5                                   # still an ordinary class here
assert Counter.remote(1).add.remote().result() == 2             # no handle kept: still works
```

- `Class.remote(...)` checks the arguments against `__init__` on your line and
  returns once the peer has accepted the actor. `__init__` runs in the worker.
  If it raises, every call to the actor raises that same error.
- Calling a method without `.remote` raises a `TypeError` that names the fix:
  `Counter.add runs in the actor, not here: call it with .add.remote(...)`.
- An actor lives until you close it or its run ends, not as long as a handle
  does. `Counter.remote().add.remote(1)` works with no handle kept.
- A method can't stream with `yield`: calling a generator method fails with
  `TypeError: cannot pickle 'generator' object`. Return a list, or stream from a
  generator function (see [tasks.md](tasks.md)).

## Calls through one handle run in order

Calls made through one handle start in the order you made them, one at a time,
so methods never race on `self`. A method that raises fails only that call. The
actor keeps running, and whatever the method changed before raising stays
changed.

```python run
@ch.remote
class Ledger:
    def __init__(self):
        self.entries: list[str] = []

    def record(self, entry: str) -> int:
        if not entry:
            raise ValueError("empty entry")
        self.entries.append(entry)
        return len(self.entries)

    def all(self) -> list[str]:
        return list(self.entries)

ledger = Ledger.remote()
ledger.record.remote("open")
assert isinstance(ledger.record.remote("").exception(), ValueError)   # your exception, its own type
ledger.record.remote("close")
assert ledger.all.remote().result() == ["open", "close"]
```

Order holds per handle. You can pass a handle to another call like any
argument, but there it is a separate handle (as is one from `ch.actor(name)`):
its calls run in order among themselves, interleaved with yours.

```python run
@ch.remote
def deposit_twice(counter, amount: int) -> int:
    counter.add.remote(amount).result()
    return counter.add.remote(amount).result()

assert deposit_twice.remote(Counter.remote(0), 5).result() == 10
```

One exception to "one at a time": while a method waits on another call's result
(`.result()`, `ch.get`, a queue), the actor may start its next call, so calls
that wait on each other can't deadlock. Don't hold an invariant on `self` across
such a wait.

## Write async methods

`async def` doesn't change how an actor runs. It still takes one call at a time,
awaiting or not, until you give it `concurrency`. With `concurrency=N`, up to N
calls run as coroutines on the actor's one event loop, and an awaiting call
holds no thread.

```python run
import asyncio

@ch.remote
class Fetcher:
    def __init__(self):
        self.active = 0

    async def fetch(self) -> int:
        self.active += 1
        await asyncio.sleep(0.05)
        running = self.active                 # calls in progress, this one included
        self.active -= 1
        return running

serial = Fetcher.remote()
assert max(ch.get([serial.fetch.remote() for _ in range(10)])) == 1
overlapping = Fetcher.options(concurrency=10).remote()
assert max(ch.get([overlapping.fetch.remote() for _ in range(10)])) > 1
```

## Run calls side by side

`concurrency=N` on the class lets N calls run at once. Plain methods then run on
threads: they start in order but finish in any order, and you protect shared
state yourself (a `threading.Lock`).

To keep a slow method from blocking quick ones, put the quick ones in a
concurrency group. Declare groups on the class with `groups={name: concurrency}`
and assign methods with `@ch.method(group=...)`. `@ch.method(concurrency=N)`
gives one method a lane of its own.

```python run
import threading
import time

@ch.remote(groups={"io": 4})
class Service:
    def slow(self, seconds: float) -> str:
        time.sleep(seconds)
        return "done"

    @ch.method(group="io")
    def ping(self) -> str:
        return "pong"

    @ch.method(concurrency=2)
    def nap(self, seconds: float) -> int:
        time.sleep(seconds)
        return threading.get_ident()

svc = Service.remote()
svc.ping.remote().result()                          # the actor is up
busy = svc.slow.remote(1.0)
assert svc.ping.remote().result() == "pong" and not busy.done()   # ping didn't wait for slow()
assert busy.result() == "done"
naps = [svc.nap.remote(0.2), svc.nap.remote(0.2)]
assert len({f.result() for f in naps}) == 2         # two threads at once
```

- Each lane (the default one, each group, each method with its own
  `concurrency`) starts its calls in arrival order. Lanes don't wait on each
  other. The default lane runs `concurrency` calls at once (default 1).
- A method naming a group the class didn't declare fails at decoration with a
  `TypeError` that lists the declared groups.
- A method takes `group=` or `concurrency=`, not both.

## Set options for a method or one call

`@ch.method(...)` sets options for every call of a method. `.options(...)` on a
method sets them for the calls made through that view.

```python run
@ch.remote
class Stats:
    @ch.method(returns=2)
    def bounds(self, xs: list[int]) -> tuple[int, int]:
        return min(xs), max(xs)

    def slow_sum(self, xs: list[int], seconds: float) -> int:
        time.sleep(seconds)
        return sum(xs)

stats = Stats.remote()
lo, hi = stats.bounds.remote([3, 9, 4]).split()
assert (lo.result(), hi.result()) == (3, 9)

late = stats.slow_sum.options(deadline="100ms").remote([1, 2], 1.0)
assert isinstance(late.exception(), ch.DeadlineExceeded) and late.cancelled()
```

| Where | Applies to | Options you'd set there |
|---|---|---|
| `@ch.remote(...)` on the class, or `Class.options(...)` | the actor and all its calls | `cpus`, `memory`, `gpus`, `concurrency`, `name`, `detach`, `restart`, `max_restarts`; `groups` and `state` on the decorator only |
| `@ch.method(...)` | every call of that method | `group`, `concurrency`, `batch`, `returns`, `deadline`, `max_attempts`, `retry_exceptions` |
| `handle.method.options(...)` | calls through that view | `deadline`, `returns`, `max_attempts`, `priority`, a stricter `effects`, `command_id` |

Options are checked on the line that sets them: `stats.bounds.options(bogus=1)`
raises `ch.OptionsError` naming the option. A deadline or `cancel()` ends your
wait at once, but a running method stops only when it checks
`ch.context().cancelled()`. Until then the actor is busy with it.

## Find an actor by name

`Class.options(name=...).remote(...)` gets the actor with that name, or creates
it. `ch.actor(name)` finds it from anywhere without the class.

```python run
name = f"registry-{ch.init()}"           # the run id keeps the name fresh (see below)
registry = Counter.options(name=name).remote(0)
registry.add.remote(5).result()
again = Counter.options(name=name).remote(1000)               # the same actor; 1000 is ignored
assert again.id == registry.id and again.add.remote(1).result() == 6
assert ch.actor(name).add.remote(1).result() == 7
assert [a.name for a in ch.runs.actors(name)] == [name]
try:
    ch.actor("nobody")
except ch.NotFound:                       # also a LookupError
    pass
```

- A name is one actor across the cluster for your tenant. Any script or call
  that uses the name reaches the same instance, and the constructor arguments
  of every call after the first are ignored.
- A named actor still belongs to the run that created it and ends with that run,
  unless it was created with `detach=True`.
- `ch.runs.actors(prefix)` lists live named actors, and `chimaera actors
  --prefix P` does the same from a shell.
- A name stays bound to its actor after the actor ends. Creating it again
  doesn't start a new actor: calls through the handle raise `ch.ActorDied` in
  the same run, and in a later run they wait forever, while `ch.actor(name)`
  raises `ch.NotFound`. Until that is fixed, give each new actor a new name,
  as the example does with the run id.

## Keep an actor after your script exits

`detach=True` keeps an actor alive after its run ends. Give it a name so a later
script can find it, and close it when you're done: nothing else ends it. Once
closed, its name stays taken (see above).

```python
# first.py, with Counter defined as above
cache = Counter.options(name="cache", detach=True).remote(0)
cache.add.remote(1).result()
```

```python
# later.py, a separate process
import chimaera as ch

cache = ch.actor("cache")
print(cache.add.remote(1).result())   # 2
cache.close()
```

## End an actor

`handle.close()` ends the actor: its process exits, its unfinished calls raise
`ch.ActorDied`, it isn't restarted and it gives back any CPUs it held.

```python run
temp = Counter.remote(0)
temp.add.remote(1).result()
temp.close()
assert isinstance(temp.add.remote(1).exception(), ch.ActorDied)
```

- `close()` returns at once and doesn't wait for calls in flight. Wait on their
  futures first if you need their results.
- Inside a method, `ch.context().exit_actor()` ends the actor from within. That
  call raises `ch.ActorDied` too.
- A handle reserves the names `close` and `id` (`handle.id` is the actor's id).
  A class that defines a method with one of these names gets a warning at
  decoration, and the method can't be reached through the handle. Rename it.

## Restart an actor after a crash

By default an actor whose process dies stays dead. `restart=` changes that.

| `restart=` | When the process dies |
|---|---|
| `"never"` (default) | The actor is dead. The call that was running and every later call raise `ch.ActorDied`. |
| `"fresh"` | A new process runs `__init__` again with the original arguments, so state starts over. Up to `max_restarts` times (default 3), then it stays dead. |
| `"checkpoint"` | Accepted, and today restarts like `"fresh"`. For state that survives, use a [durable actor](#keep-state-through-crashes-with-a-durable-actor). |

```python run
import os
import signal

@ch.remote(restart="fresh", max_restarts=2)
class Worker:
    def __init__(self):
        self.calls = 0

    def work(self) -> int:
        self.calls += 1
        return self.calls

    def crash(self) -> None:
        os.kill(os.getpid(), signal.SIGKILL)       # stands in for a segfault or an OOM kill

w = Worker.remote()
assert w.work.remote().result() == 1
died = w.crash.remote().exception()
assert isinstance(died, ch.ActorDied) and "died while this call was running" in str(died)
assert w.work.remote().result() == 1              # a fresh instance: state started over
```

A call that was running when the process died is never replayed on the new
instance. It raises `ch.ActorDied`. Calls that hadn't started yet run on the new
instance, in order. A move off a drained peer restarts the actor without using
up a restart.

`ch.ActorDied` (an `InfrastructureFailure`) means the call won't run: the actor
was closed, ended itself, ended with its run, died with `restart="never"` or
ran out of restarts, or died while this call was running. See
[failures.md](failures.md) for the other errors.

## Give an actor CPUs and memory

An actor asks for no CPU unless you set `cpus`, so idle actors never block other
calls. With `@ch.remote(cpus=2)` it holds two CPUs from the moment it starts
until it ends, and it waits to start until some peer has them free. A request
no peer could ever meet makes `Class.remote(...)` raise `ch.Infeasible`, naming
the constraint. Memory, GPUs and placement work as they do for functions; see
[resources.md](resources.md).

## Spread calls over a pool

A pool is a set of interchangeable replicas of one class, each built with the
same arguments. `Class.options(replicas=n).pool(...)` creates it, and its
methods look like an actor's.

```python run
@ch.remote
class Model:
    def __init__(self, weights: str):
        self.weights = weights

    def predict(self, x: int) -> str:
        return f"{self.weights}:{x}"

    def replica(self) -> str:
        return ch.context().actor_id

with Model.options(replicas=3).pool("v1") as pool:
    assert pool.predict.remote(1).result() == "v1:1"
    assert list(pool.predict.map(range(3))) == ["v1:0", "v1:1", "v1:2"]
    user = pool.route("user-7")                            # one replica for this key
    assert user.replica.remote().result() == user.replica.remote().result()
    pool.resize(4)
    pool.upgrade("v2", surge=1)                            # replace replicas one at a time
    assert pool.predict.remote(1).result() == "v2:1" and len(pool) == 4
```

- Without `replicas=`, a pool has one replica per core, at most 8.
- A call goes to the replica with the fewest calls in flight, picked when you
  make the call. Calls through a pool have no order across replicas.
- `pool.method.map(items)` streams results in input order (`ordered=False` for
  completion order) and keeps 16 calls per replica in flight.
- `route(key)` sends every call for a key to one replica, by rendezvous hashing
  of `str(key)`, so a resize moves only a few keys.
- `resize(n)` grows or shrinks the pool (`n` at least 1); shrinking drops the
  newest first. `upgrade(*args, cls=None, surge=1, **kwargs)` rebuilds every
  replica from new arguments or a new class, `surge` at a time. Either way a
  leaving replica takes no new calls and closes once its calls finish.
- `close()`, or leaving the `with` block, ends every replica.
- Set call options on the class (`Model.options(cpus=2, replicas=3)`): pool
  methods take only `.remote` and `.map`. A pool handle reserves `close`,
  `decisions`, `resize`, `route` and `upgrade`. Ray's `ActorPool` maps here
  ([from-ray.md](from-ray.md)).

## Share a pool and size it from load

`name=` makes a pool that other scripts and calls share. Its membership lives in
the cluster's replicated store, and other handles re-read it at most once a
second. Only a named pool can be passed to a call; passing an unnamed one raises
`ch.SerializationError`.

```python run
shared = Model.options(name="docs/model", replicas=2).pool("v1")
assert len(Model.options(name="docs/model").pool()) == 2   # joins; no arguments needed

@ch.remote
def ask(pool, x: int) -> str:
    return pool.predict.remote(x).result()

assert ask.remote(shared, 5).result() == "v1:5"
shared.close()
```

`replicas=ch.auto(initial, min=, max=)` sizes a pool from load. It starts at
`initial` (default 1). It doubles while calls in flight stay above 80% of what
the replicas can run at once for 0.75 s, up to `max` (default the larger of
`initial` and the core count). After a minute under 25% busy it drops one
replica, down to `min` (default 1). `pool.decisions` says what it did and why.

```python run
@ch.remote
class SlowModel:
    def predict(self, x: int) -> int:
        time.sleep(0.1)
        return x

elastic = SlowModel.options(replicas=ch.auto(1, max=4)).pool()
assert len(elastic) == 1
assert list(elastic.predict.map(range(40))) == list(range(40))
assert elastic.decisions[0].startswith("grow 1 -> 2")
elastic.close()
```

A replica is picked when the call is made, so calls already queued on a busy
replica stay there. Growth helps the calls made after it.

## Batch calls into one method call

`@ch.method(batch=ch.Batch(max_size=, max_wait=))` lets you write a method for a
list while callers pass one item. The actor collects calls until it has
`max_size` of them or the first has waited `max_wait`, runs the method once,
and gives each caller its element of the returned list.

```python run
@ch.remote
class Embedder:
    def __init__(self):
        self.batch_sizes: list[int] = []

    @ch.method(batch=ch.Batch(max_size=8, max_wait="20ms"))
    def embed(self, texts: list[str]) -> list[int]:
        self.batch_sizes.append(len(texts))
        return [len(t) for t in texts]

    def sizes(self) -> list[int]:
        return self.batch_sizes

embedder = Embedder.remote()
futures = [embedder.embed.remote("x" * n) for n in range(20)]
assert [f.result() for f in futures] == list(range(20))
sizes = embedder.sizes.remote().result()
assert sum(sizes) == 20 and max(sizes) <= 8
```

- `ch.Batch()` defaults to `max_size=32` and `max_wait=0.005` (seconds, or a
  string such as `"5ms"`).
- Call it with exactly one positional argument. The method gets the arguments
  in arrival order and must return a list of the same length; otherwise every
  call in the batch fails with a `ValueError` giving both counts.
- If the method raises, every call in that batch gets the error.
- A batched method runs in its own lane, beside the actor's other methods, so
  it can run while another method does. The peer keeps up to two batches of
  calls in flight, so the next fills while one runs. `async def` works too.
- `batch=` belongs on the method. On `@ch.remote` it raises `ch.OptionsError`,
  and it can't be combined with `group=` or `concurrency=`.

## Keep state through crashes with a durable actor

`@ch.remote(state=ch.durable.State())` commits the actor's state with every
call. A restarted instance resumes from the last commit instead of running
`__init__` again, and a repeated command gets its first reply.

```python run
@ch.remote(state=ch.durable.State())
class Account:
    def __init__(self):
        self.balance = 0

    def deposit(self, amount: int) -> int:
        self.balance += amount
        return self.balance

    def withdraw(self, amount: int) -> int:
        self.balance -= amount
        if self.balance < 0:
            raise ValueError("insufficient funds")
        return self.balance

    def pid(self) -> int:
        return os.getpid()

acct = Account.remote()
assert acct.deposit.options(command_id="dep-1").remote(5).result() == 5
assert acct.deposit.options(command_id="dep-1").remote(5).result() == 5   # a repeat: no second deposit
reused = acct.deposit.options(command_id="dep-1").remote(9)          # same id, other arguments
assert isinstance(reused.exception(), ch.errors.CommandConflict)
assert isinstance(acct.withdraw.remote(50).exception(), ValueError)
assert acct.deposit.remote(2).result() == 7       # the failed withdraw left no trace

os.kill(acct.pid.remote().result(), signal.SIGKILL)
assert acct.deposit.remote(0).result() == 7       # resumed from the last commit
```

- After each method returns, the instance's attributes (or what `__getstate__`
  returns) are pickled and committed before the reply goes out. A call that
  raises commits nothing, and the instance goes back to the last commit.
- `.options(command_id=...)` names a command, per call only. A repeat returns
  the stored reply, and the same id with other arguments raises
  `ch.errors.CommandConflict`. Without one, the call's own id is used, so a
  call cut off by a crash is retried on the restarted instance without being
  applied twice. Command ids are kept for 7 days.
- The pickled state may be at most 1 MiB. A call that grows it past that fails
  with `ch.errors.RemoteError` ("durable state exceeds the 1 MiB inline
  limit"), and the state stays at the last commit. Keep large data elsewhere
  and store its key.
- A durable actor runs one call at a time: setting `concurrency` raises
  `ch.OptionsError`. `state=` goes on the decorator only.
- It restarts after a crash whatever `restart=` says, up to `max_restarts`
  (default 3).

Next: give actors CPUs, memory and GPUs in [resources.md](resources.md).
