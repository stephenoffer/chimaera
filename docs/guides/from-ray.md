# Coming from Ray

Most Ray Core programs port to Chimaera line by line. Use the table to find each
Ray name's counterpart, read the differences that change behavior, or run your
Ray program unchanged through the compatibility shim.

## Map Ray names to Chimaera

The left column is Ray Core; the right is the native Chimaera API. The shim's
own translation of every option is in [../compat-ray.md](../compat-ray.md).

| Ray | Chimaera |
|---|---|
| `ray.init()` | Nothing, or `ch.init()`. The first call starts or connects to the local peer. |
| `ray.init(address=...)`, `ray job submit` | `ch.init(cluster="lab")` (a profile) or `"tcp://host:port"`; `chimaera run --cluster lab -- python app.py`, `chimaera submit`. See [clusters.md](clusters.md). |
| `runtime_env={"env_vars": ...}` | `ch.init(env={...})` |
| `runtime_env` `working_dir`, `pip` | A script on a remote cluster ships its project directories, and a `uv.lock` is installed on each peer. There is no per-call environment. See [clusters.md](clusters.md). |
| `@ray.remote`, `f.remote(...)` | `@ch.remote`, `f.remote(...)`, which returns a `ch.Future` |
| `ray.get(ref)`, `ray.get(refs)` | `future.result()`, `ch.get(futures)` |
| `ray.put(value)` | `ch.put(value)`, a finished future |
| `ray.wait(refs, num_returns=1)` | `ch.wait(futures, num_returns=1)`, or iterate `ch.as_completed(futures)` |
| `ray.cancel(ref, force=)` | `future.cancel(force=)` |
| `num_cpus`, `num_gpus`, `memory` | `cpus`, `gpus`, `memory` (usually left to the peer, see below) |
| `accelerator_type` | `device="cuda:H100"` and other `KIND[:MODEL]` strings |
| `num_returns=n` | `returns=n`, then `future.split()` |
| `max_retries=n` | `max_attempts=n + 1` (default 3 attempts) |
| `retry_exceptions=[...]` | `retry_exceptions=(...)`, exception types only |
| (no equivalent) | `effects="idempotent"` or `"external"` |
| `scheduling_strategy=NodeAffinitySchedulingStrategy(...)`, `label_selector` | `placement=ch.dist.Placement(require={...}, prefer=[...])` |
| `placement_group(...)` with `PlacementGroupSchedulingStrategy` | `res = ch.dist.reserve(bundles, strategy="pack")`, then `.options(within=res[i])`. See [gangs.md](gangs.md). |
| `max_concurrency` | `concurrency` |
| `concurrency_groups`, `@ray.method(concurrency_group=)` | `@ch.remote(groups={...})`, `@ch.method(group=)` |
| `@ray.method(num_returns=n)` | `@ch.method(returns=n)` |
| `.options(name=, get_if_exists=True)`, `namespace=` | `.options(name="ns/name")`, always get-or-create |
| `lifetime="detached"` | `detach=True` |
| `ray.get_actor(name)` | `ch.actor(name)`, which raises `ch.NotFound` |
| `max_restarts=n` | `restart="fresh", max_restarts=n` |
| `max_task_retries` | No equivalent: a call running when its actor dies raises `ch.ActorDied` |
| `ray.kill(actor)`, `ray.actor.exit_actor()` | `actor.close()`, `ch.context().exit_actor()` |
| `ray.util.ActorPool` | `Cls.options(replicas=n).pool(...)` ([actors.md](actors.md)) |
| `ray.util.queue.Queue` | `ch.coord.Queue`; its `put` and `get` return futures |
| `ray.util.multiprocessing.Pool` | `ch.Executor()`, a `concurrent.futures` executor ([integrations.md](integrations.md)) |
| `ray.util.collective` | `ch.dist.group()` ([gangs.md](gangs.md)) |
| `ray.get_runtime_context()`, `ray.get_gpu_ids()` | `ch.context()` (None in your script), `ch.dist.devices()` |
| `ray.nodes()`, `ray.cluster_resources()` | `ch.runs.cluster().nodes()`, `ch.runs.cluster().resources()` |
| `ray.timeline()`, the dashboard | `chimaera explain RUN`, `chimaera status` ([observability.md](observability.md)) |
| `RayTaskError` | Your own exception, with its own type |
| `RayActorError`, `ActorDiedError` | `ch.ActorDied` |
| `GetTimeoutError` | `TimeoutError` (the call keeps running) |
| `TaskCancelledError` | `ch.Cancelled` |
| `OutOfMemoryError`, `ObjectLostError`, `WorkerCrashedError` | `ch.OutOfMemory`, `ch.errors.DataLost`, `ch.errors.WorkerCrashed` |
| Ray Data, Train, Tune, Serve, RLlib | Not provided. `ch.data` covers partitioned data ([data.md](data.md)). |

## Port a program

Here is a small Ray program: tokenize documents in parallel, count words in an
actor.

```python
import ray

ray.init()


@ray.remote(max_retries=2)
def tokenize(text, stopwords):
    return [w for w in text.lower().split() if w not in stopwords]


@ray.remote
class Vocabulary:
    def __init__(self):
        self.counts = {}

    def add(self, words):
        for w in words:
            self.counts[w] = self.counts.get(w, 0) + 1
        return len(self.counts)

    def top(self, n):
        return sorted(self.counts.items(), key=lambda kv: -kv[1])[:n]


stopwords = ray.put({"the", "a"})
docs = ["The cat sat", "A cat ran", "the dog sat"]
refs = [tokenize.remote(d, stopwords) for d in docs]
ready, _ = ray.wait(refs, num_returns=len(refs), timeout=30)
vocab = Vocabulary.remote()
ray.get([vocab.add.remote(r) for r in refs])
print(ray.get(vocab.top.remote(2)))
```

The same program on Chimaera:

```python run
import chimaera as ch

@ch.remote(max_attempts=3)               # max_retries=2; 3 is also the default
def tokenize(text: str, stopwords: set[str]) -> list[str]:
    return [w for w in text.lower().split() if w not in stopwords]

@ch.remote
class Vocabulary:
    def __init__(self):
        self.counts: dict[str, int] = {}

    def add(self, words: list[str]) -> int:
        for w in words:
            self.counts[w] = self.counts.get(w, 0) + 1
        return len(self.counts)

    def top(self, n: int) -> list[tuple[str, int]]:
        return sorted(self.counts.items(), key=lambda kv: -kv[1])[:n]

stopwords = ch.put({"the", "a"})
docs = ["The cat sat", "A cat ran", "the dog sat"]
futures = [tokenize.remote(d, stopwords) for d in docs]
done, pending = ch.wait(futures, timeout=30)            # waits for all of them by default
vocab = Vocabulary.remote()
ch.get([vocab.add.remote(f) for f in futures])
assert vocab.top.remote(2).result() == [("cat", 2), ("sat", 2)]
```

There's no `init` to call, `ray.get(x)` becomes `x.result()` or `ch.get(xs)`,
and the decorator options take Chimaera's names. The result is the same because
the three `add` calls go through one handle, so they run in the order they were
made.

## Know the differences that change behavior

### There is no head node

Every machine runs the same `chimaera` peer, and the state that matters is
replicated with Raft across peers. There is no head node to size or to lose. On
a laptop your script starts a local peer the first time it needs one. To use a
cluster, point the script at it ([clusters.md](clusters.md)).

### Options are checked where you write them

An unknown or invalid option raises `ch.OptionsError` on the line that sets it,
with a suggestion.

```python run
try:
    tokenize.options(num_cpus=2)
except ch.OptionsError as e:
    assert "did you mean 'cpus'?" in str(e)
```

`max_retries` gets the suggestion `max_restarts`, which is the actor option. For
a function you want `max_attempts`. Options that conflict with each other, such
as `effects="external"` with `max_attempts=3`, raise when you make the call.
`cpus` must be above zero, so Ray's `num_cpus=0` has no direct equivalent; an
actor holds no CPU unless you ask ([actors.md](actors.md)).

### Your exceptions are not retried

As in Ray, an exception your code raises fails the call at once. Only lost
workers and peers are retried, up to `max_attempts` (default 3 attempts in all,
where Ray's default `max_retries=3` gives 4). List the exceptions worth retrying
as types: `retry_exceptions=True` is rejected.

```python run
import os
import tempfile

@ch.remote(retry_exceptions=ConnectionError)
def flaky(marker: str) -> str:
    if not os.path.exists(marker):
        open(marker, "w").close()
        raise ConnectionError("first attempt fails")
    return "ok"

assert flaky.remote(tempfile.mktemp()).result() == "ok"     # retried once
```

### Declare calls with side effects

Ray retries a task whose worker died, whatever the task did. Chimaera asks you to
say what a call does to the outside world. `effects="external"` (charging a
card, sending an email) means the call starts at most once. If its worker dies
mid-call, you get `ch.OutcomeUnknown` instead of a silent second attempt.

```python run
import signal

@ch.remote(effects="external")
def charge(amount: int) -> None:
    os.kill(os.getpid(), signal.SIGKILL)       # the worker dies mid-call

try:
    charge.remote(5).result()
except ch.OutcomeUnknown as e:
    assert "may have started" in str(e)
```

`effects="idempotent"` says a repeat is harmless. See
[failures.md](failures.md) for what each level allows.

### Arrays arrive writable

Ray hands a task read-only NumPy arrays, so `arr[0] = 1` raises there. Chimaera
maps the stored value copy-on-write: the array is writable, and a write stays
private to the call that made it.

```python run
import numpy as np

@ch.remote
def bump(a: np.ndarray) -> float:
    a[0] += 1
    return float(a[0])

zeros = ch.put(np.zeros(1_000_000))
assert bump.remote(zeros).result() == 1.0
assert bump.remote(zeros).result() == 1.0     # the stored array is unchanged
```

On a host that refuses private writable mappings (strict memory overcommit) the
array falls back to read-only. [data.md](data.md) has the details.

### Memory is measured, not declared

You rarely need `memory=`. The peer measures each function's memory peak and
reserves that for its later calls, so calls wait for memory instead of
overcommitting a machine. Values you set still win, and `memory_limit=`
enforces a cap. `chimaera profiles` shows what was learned. See
[resources.md](resources.md).

### Futures inside lists arrive as values

Ray resolves only refs passed directly as arguments; refs inside a list stay
refs for the task to `ray.get`. Chimaera resolves futures directly in the
arguments and one level inside a list, tuple or dict value, and the call starts
once they're ready. Wrap the argument in `ch.handles(...)` to pass the futures
themselves.

```python run
@ch.remote
def total_words(token_lists: list[list[str]]) -> int:
    return sum(len(t) for t in token_lists)      # values, not futures

assert total_words.remote(futures).result() == 6

@ch.remote
def first_done(fs: list[ch.Future[list[str]]]) -> list[str]:
    return next(iter(ch.as_completed(fs))).result()

assert first_done.remote(ch.handles(futures)).result() in (["cat", "sat"], ["cat", "ran"], ["dog", "sat"])
```

### Actors live as long as their run

A Ray actor dies when its last handle goes away. A Chimaera actor lives until
you call `close()` or its run ends, whether or not any handle remains, so
`Vocabulary.remote().add.remote(["x"])` works. Close actors you're done with to
give back the CPUs they hold.

### Calls through one actor handle run in order, async ones too

Calls through one handle start in the order you made them, one at a time. Ray
runs an async actor's calls concurrently (up to 1,000 by default). Here an
`async def` method still runs one call at a time until you set `concurrency=`
on the class. Ray's `allow_out_of_order_execution` has no equivalent.

### Named actors are always get-or-create

`.options(name=...)` returns the existing actor if there is one, where Ray's
default raises. A name covers your whole tenant on the cluster, not one job's
namespace, so put the namespace in the name (`"ns/name"`). `ch.actor(name)`
raises `ch.NotFound`, a `LookupError`, where `ray.get_actor` raises
`ValueError`. A name also stays taken after its actor ends
([actors.md](actors.md#find-an-actor-by-name)).

### `ch.wait` waits for everything by default

`ray.wait` returns after one ref and caps the ready list at `num_returns`.
`ch.wait` waits for all futures unless you pass `num_returns`, and its `done`
list holds every finished future, which can be more than `num_returns`. It
doesn't fetch values unless you pass `fetch_local=True`.

```python run
done, pending = ch.wait(futures, num_returns=1, timeout=10)
assert len(done) >= 1 and len(done) + len(pending) == 3
```

### A decorated function still runs locally

Calling a decorated function without `.remote` runs it in your process, where
Ray raises `TypeError`. That's handy in unit tests.

```python run
assert tokenize("The cat", {"the"}) == ["cat"]
```

## Run a Ray program unchanged

The shim in `chimaera.compat.ray` maps Ray Core's API onto Chimaera, and needs no
Ray installation. Run a whole program with `-m`, which installs the shim as
`ray` (with `ray.exceptions`, `ray.util` and their submodules) before your
script starts. Here it is on the Ray program above, saved as `app.py`:

```bash
python -m chimaera.compat.ray app.py
```

```text
[('cat', 2), ('sat', 2)]
```

`chimaera run -- python -m chimaera.compat.ray app.py` works too, and with
`--cluster lab` the program's `ray.init()` connects to that cluster profile.
`ray.init(address=...)` itself is refused by the shim.

In your own code, import the shim under the name `ray`:

```python run
import chimaera.compat.ray as ray

@ray.remote
def square(x):
    return x * x

@ray.remote
class Total:
    def __init__(self):
        self.n = 0

    def add(self, x):
        self.n += x
        return self.n

refs = [square.remote(i) for i in range(4)]
assert ray.get(refs) == [0, 1, 4, 9]
total = Total.remote()
assert ray.get([total.add.remote(r) for r in refs]) == [0, 1, 5, 14]
assert isinstance(refs[0], ch.Future)               # Chimaera futures underneath

@ray.remote
def fails():
    raise KeyError("missing")

try:
    ray.get(fails.remote())
except KeyError as e:                               # also a ray.exceptions.RayTaskError
    assert isinstance(e, ray.exceptions.RayTaskError)
```

What the shim does and doesn't do:

- It covers Ray Core only. `ray.data`, `ray.train`, `ray.tune`, `ray.serve` and
  the other libraries raise `Unsupported`.
- A feature that doesn't map raises `Unsupported` (a `NotImplementedError`)
  naming the Chimaera alternative. An option that behaves differently warns
  once with a `ChimaeraCompatWarning` coded CHR001 (ignored), CHR002 (accepted,
  not enforced yet) or CHR003 (different semantics).
- It keeps Ray's defaults where they differ: four attempts for a task, one for
  an actor call, and 1,000 concurrent calls for an async actor. A named actor
  without a namespace is scoped to the run.
- A call's ref is a Chimaera future, so `await ref`, `ch.as_completed` and the
  rest of the native API work on it. (`ray.put` returns a thin wrapper around
  one.)

The full option-by-option table is in [../compat-ray.md](../compat-ray.md).

Next: put state in actors the Chimaera way in [actors.md](actors.md).
