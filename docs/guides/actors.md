# Actors: state that lives in a worker

An actor is an instance of your class living in its own worker process,
keeping its state between calls. Use one when calls share something expensive
or mutable: a loaded model, a connection, a counter. This page starts with a
counter and ends with state that survives a crash.

## Create an actor and call it

Put `@ch.remote` on a class. `Class.remote(...)` starts an instance in a worker
and returns a handle; `handle.method.remote(...)` runs the method there and
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

Each actor lives until you close it or its run ends, handle or no handle.

<details>
<summary>More: constructor checks and two common mistakes</summary>

`Class.remote(...)` checks your arguments against `__init__` on your line and
returns once the peer accepts the actor. `__init__` runs in the worker; if it
raises, every call to that actor raises that error.

- Calling a handle's method without `.remote` raises a `TypeError` that names
  the fix: `Counter.add runs in the actor, not here: call it with .add.remote(...)`.
- Methods can't stream with `yield`. Calling a generator method fails with
  `TypeError: cannot pickle 'generator' object`, so return a list, or stream
  from a generator function ([tasks.md](tasks.md)).

</details>

## Calls through one handle run in order

Calls through one handle start one at a time, in the order you made them, so
methods never race on `self`. A method that raises fails only its own call. The
actor keeps running, and whatever that call changed before raising stays
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

<details>
<summary>More: order across handles, and methods that block on other calls</summary>

Order is per handle. A handle you pass to another call is a separate handle
there, as is one from `ch.actor(name)`: its calls keep their own order,
interleaved with yours.

```python run
@ch.remote
def deposit_twice(counter, amount: int) -> int:
    counter.add.remote(amount).result()
    return counter.add.remote(amount).result()

assert deposit_twice.remote(Counter.remote(0), 5).result() == 10
```

One exception to "one at a time": while a running call waits on another call's
result (`.result()`, `ch.get`, a queue), the next queued call may start, so
calls that wait on each other can't deadlock. Don't hold an invariant on `self`
across such a wait.

</details>

## Write async methods

Give an async actor `concurrency=N` and up to N calls run as coroutines on its
one event loop, where an awaiting call holds no thread. Without it, `async def`
changes nothing: calls still run one by one, awaiting or not.

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

With `concurrency=N` on the class (default 1), plain methods run N at once on
threads. They start in order and finish in any order, so guard shared state
yourself (a `threading.Lock`).

To keep a slow method from blocking quick ones, declare
`groups={name: concurrency}` on the class and put the quick ones in a
concurrency group with `@ch.method(group=...)`. `@ch.method(concurrency=N)`
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

<details>
<summary>More: how lanes start calls, and decoration errors</summary>

Within each lane (the default one, each group, each method given
`concurrency`), calls start in arrival order, and no lane waits for another.
Each method takes `group=` or `concurrency=`, not both, and naming an
undeclared group fails at decoration with a `TypeError` listing the declared
groups.

</details>

## Set options for a method or one call

Options go on the class, on a method (`@ch.method(...)`), or on one view of
that method (`.options(...)`).

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
raises `ch.OptionsError` naming the option.

A deadline or `cancel()` ends your wait at once. The running method stops only
when it checks `ch.context().cancelled()`, and until then it keeps the actor
busy.

## Find an actor by name

A name is one actor across the cluster for your tenant, reached by every script
or call using it. `Class.options(name=...).remote(...)` gets or creates it
(later constructor arguments are ignored), and anywhere else `ch.actor(name)`
finds it with just the name.

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

A name stays bound to its actor after that actor ends. Until that is fixed,
give each new actor a new name, as the example does with the run id.

<details>
<summary>More: when named actors end, and listing or reusing names</summary>

- A named actor still ends with the run that created it, unless it was created
  with `detach=True`.
- `ch.runs.actors(prefix)` lists live named actors, as does
  `chimaera actors --prefix P` from a shell.
- Creating a name again after its actor ends doesn't start a new one. Calls
  through the handle raise `ch.ActorDied` in the same run and wait forever in a
  later one, while `ch.actor(name)` raises `ch.NotFound`.

</details>

## Keep an actor after your script exits

`detach=True` keeps an actor alive after its run ends. Name it so a later script
can find it, and close it when you're done, since nothing else ends it. Once
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
`ch.ActorDied`, it isn't restarted, and any CPUs it held go back.

```python run
temp = Counter.remote(0)
temp.add.remote(1).result()
temp.close()
assert isinstance(temp.add.remote(1).exception(), ch.ActorDied)
```

`close()` returns without waiting for calls in flight, so wait on their
futures first if you need the results.

<details>
<summary>More: exit_actor() and the reserved names close and id</summary>

Inside a method, `ch.context().exit_actor()` ends its actor, and that call
raises `ch.ActorDied` too.

A handle reserves the names `close` and `id` (`handle.id` is the actor's id).
Methods with either name get a warning at decoration and can't be reached
through the handle, so rename them.

</details>

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

A call that was running when the process died is never replayed; it raises
`ch.ActorDied`. Calls that hadn't started run on the new instance, in order.

<details>
<summary>More: drained peers, and every reason for ActorDied</summary>

Moving off a drained peer restarts the actor without using up a restart.

`ch.ActorDied` (an `InfrastructureFailure`) means the call won't run: the actor
was closed, ended itself or with its run, died with `restart="never"` or ran
out of restarts, or died while this call was running. The other errors are in
[failures.md](failures.md).

</details>

## Give an actor CPUs and memory

Actors ask for no CPU unless you set `cpus`, so idle ones never block other
calls. With `@ch.remote(cpus=2)` each instance holds two CPUs for its whole
life and won't start until some peer has them free. A request no peer could ever meet
makes `Class.remote(...)` raise `ch.Infeasible`, naming the constraint. Memory,
GPUs and placement work as they do for functions; see
[resources.md](resources.md).

## Spread calls over a pool

A pool spreads calls over interchangeable replicas of one class, all built with
the same arguments. `Class.options(replicas=n).pool(...)` creates one, and its
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

Each call goes to the replica with the fewest calls in flight when you make
it. There's no order across replicas. Ray's `ActorPool` maps here
([from-ray.md](from-ray.md)).

<details>
<summary>More: default size, map, route, resize, upgrade and close</summary>

- Without `replicas=`, you get one replica per core, at most 8.
- `pool.method.map(items)` streams results in input order (`ordered=False` for
  completion order), keeping 16 calls per replica in flight.
- `route(key)` sends all calls for a key to one replica by rendezvous hashing
  of `str(key)`, so a resize moves only a few keys.
- `resize(n)` grows or shrinks the pool (`n` at least 1), dropping the newest
  replicas first. `upgrade(*args, cls=None, surge=1, **kwargs)` rebuilds every
  replica from new arguments or a new class, `surge` at a time. Either way a
  leaving replica takes no new calls and closes once its calls finish.
- `close()`, or leaving the `with` block, ends every replica.
- Pool methods take only `.remote` and `.map`, so set call options when you
  build the pool (`Model.options(cpus=2, replicas=3)`). A pool handle reserves
  `close`, `decisions`, `resize`, `route` and `upgrade`.

</details>

## Share a pool and size it from load

Pass `name=` and other scripts and calls can share the pool. Only a named pool
can be passed to calls; an unnamed one raises `ch.SerializationError`.

```python run
shared = Model.options(name="docs/model", replicas=2).pool("v1")
assert len(Model.options(name="docs/model").pool()) == 2   # joins; no arguments needed

@ch.remote
def ask(pool, x: int) -> str:
    return pool.predict.remote(x).result()

assert ask.remote(shared, 5).result() == "v1:5"
shared.close()
```

`replicas=ch.auto(initial, min=, max=)` sizes a pool from load: it grows while
replicas stay busy and shrinks after a quiet minute. `pool.decisions` says what
it did and why.

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

<details>
<summary>More: shared membership and the auto-sizing thresholds</summary>

A named pool's membership lives in the cluster's replicated store, and other
handles re-read it at most once a second.

Auto-sizing starts at `initial` (default 1). It doubles while calls in flight
stay above 80% of what the replicas can run at once for 0.75 s, up to `max`
(default the larger of `initial` and the core count). After a minute under 25% busy it
drops one replica, down to `min` (default 1).

Growth helps only calls made after it. Calls already queued on a busy replica
stay there.

</details>

## Batch calls into one method call

Write the method for a list; callers still pass one item each. With
`@ch.method(batch=ch.Batch(max_size=, max_wait=))`, calls queue up until there
are `max_size` of them or the first has waited `max_wait`. Then the method runs
once, and each caller gets its element of the returned list.

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

<details>
<summary>More: batch defaults, length rules, errors and lanes</summary>

- `ch.Batch()` defaults to `max_size=32` and `max_wait=0.005` (seconds, or a
  string such as `"5ms"`).
- Call it with exactly one positional argument. The method gets the arguments
  in arrival order and must return a list of the same length, or every call in
  the batch fails with a `ValueError` giving both counts. If the method raises,
  every caller in the batch gets the error.
- A batched method gets a separate lane, so it can run while other methods do.
  The peer keeps at most two batches going, so the next fills while one runs.
  `async def` works too.
- `batch=` goes in `@ch.method(...)` (on `@ch.remote` it raises
  `ch.OptionsError`) and can't be combined with `group=` or `concurrency=`.

</details>

## Keep state through crashes with a durable actor

`@ch.remote(state=ch.durable.State())` commits the actor's state after each
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

<details>
<summary>More: what gets committed, command ids, the 1 MiB cap and restarts</summary>

- After each method returns, the instance's attributes (or what `__getstate__`
  returns) are pickled and committed before the reply goes out. A call that
  raises commits nothing and puts the instance back at the last commit.
- `.options(command_id=...)` names a command, per call only. A repeat returns
  the stored reply, and reusing the id with other arguments raises
  `ch.errors.CommandConflict`. Without one, the call's own id is used, so a
  call cut off by a crash is retried on the restarted instance without being
  applied twice. Command ids are kept for 7 days.
- Pickled state is capped at 1 MiB. Growing it past that fails the call with
  `ch.errors.RemoteError` ("durable state exceeds the 1 MiB inline limit") and
  leaves the state at the last commit. Keep large data elsewhere and store its
  key.
- A durable actor runs calls one by one, so setting `concurrency` raises
  `ch.OptionsError`.
- It restarts after a crash whatever `restart=` says, up to `max_restarts`
  (default 3).

</details>

Next: give actors CPUs, memory and GPUs in [resources.md](resources.md).
