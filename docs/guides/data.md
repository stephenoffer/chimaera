# Data: large values, pipelines and shared state

You'll pass large values to calls without copying them and run partitioned pipelines that shuffle by key and write to a sink. You'll also stream items from generators and share state between calls through queues, topics and a replicated store.

## Pass a large value to many calls

```python run
import chimaera as ch
import numpy as np

@ch.remote
def column_mean(table, col: int) -> float:
    return float(table[:, col].mean())

table = ch.put(np.arange(400_000, dtype=np.float64).reshape(100_000, 4))   # 3 MiB, stored once
assert table.done()                                    # ch.put returns a finished future
means = ch.get([column_mean.remote(table, c) for c in range(4)])
assert means == [199998.0, 199999.0, 200000.0, 200001.0]
```

`ch.put(value)` stores the value and returns a finished `ch.Future`. Pass that future to as many calls as you like. Each call receives the value, read from the stored copy, and your script gets it back with `table.result()`.

You don't always need `ch.put`. Every call stores its arguments by size:

| What you pass | How it travels |
|---|---|
| Arguments that encode to 64 KiB or less | inside the call message |
| Arguments of 64 KiB up to 1 MiB | one file for that call, deleted when the call ends |
| A NumPy array, `bytes` or other buffer of 1 MiB or more, or a top-level argument that pickles to 1 MiB or more | stored once, named by a hash of its contents; every call that passes the same bytes reads that one copy |
| `ch.put(value)` | inside the future when it is 256 KiB or less, else one stored file |

The rules people trip on:

- Arguments are captured when you call `.remote()`. Changing the array afterward doesn't change what the call sees.
- Passing the same large value straight to many calls stores it once, but hashes it on every call. Once one line has done that 20 times and spent 0.1 s or more encoding, you get warning `CH107`, which suggests `ch.put(value)` or binding it once with `fn.partial(value)`.
- A large `list` or `dict` is stored once, but every call that reads it unpickles a full copy of its own. A NumPy array or an Arrow table is mapped instead, one copy per machine. Warning `CH118` fires for collections of 8 MiB or more.
- Don't call `.result()` on a large value only to pass it into the next call. Pass the future, and the value goes from worker to worker (warning `CH103` points this out).
- A future passed as an argument, or one level inside a list, tuple or dict argument, arrives as its value. Nested deeper it arrives as a future. [tasks.md](tasks.md) covers dependencies and `ch.handles`.
- An open file can't be sent. Pass its path and open it in the call.

## Write to an array inside a call

```python run
@ch.remote
def set_first(a, value: float) -> float:
    a[0] = value                    # allowed: the call writes to its own copy-on-write view
    return float(a[0])

@ch.remote
def first(a) -> float:
    return float(a[0])

zeros = np.zeros(1 << 20)           # 8 MiB
assert set_first.remote(zeros, 7.0).result() == 7.0
assert first.remote(zeros).result() == 0.0     # the stored value is unchanged
assert zeros[0] == 0.0                         # and so is your array
```

Reading a stored value maps its file copy-on-write (small files are read instead). Arrays arrive writable, and a write stays in the call that made it. Ray hands out read-only arrays; here `a[0] = value` works.

## Know what is kept after a run

| What | Deleted |
|---|---|
| A call's argument file (64 KiB to 1 MiB) | when the call ends |
| A `ch.put` value whose future you dropped | once the calls reading it finish (exceptions below) |
| Large arguments, and `ch.put` values your script made | when the run ends |
| Results, stream items, and `ch.put` values made inside a call | 24 hours after the run ends |

Results outlive the run so that another process can read them by run id:

```python
import chimaera as ch

run = ch.runs.attach("000003df45a0b6526faca7be64431c6e")   # str(ch.init()) in the original script
for task_id, value, error in run.outcomes():
    print(task_id, value if error is None else error)
```

A dropped `ch.put` value waits for the end of the run instead when its future was sent to an actor, nested deeper than one level in an argument, or made by a script connected to a remote cluster. While a detached actor of the run is alive, the run's stored arguments and `ch.put` values stay too, because a restart builds the actor again from them.

One consequence: a `ch.put` future that you keep somewhere outside the run, such as a `ch.coord.Store` key, stops reading when the run ends, unless the value was small enough to travel inside the future.

## See what the object store holds

`chimaera objects` lists what this machine's peer has stored, per run and largest first. This is the output while a script holds one 32 MiB `ch.put` value and a 16 MiB argument (paths shortened):

```bash
$ chimaera objects
objects in .../objects (huge pages: false)
  in memory: 2 objects, 48.0 MiB
  on disk:   0 objects, 0.0 MiB (spilled, or replicas of other peers' objects)
  ...
  run 000003df45a0b6526faca7be64431c6e: 2 objects, 48.0 MiB in memory, 0.0 MiB on disk
        32.0 MiB  000003df45a0b6526faca7be64431c6e/a3b39be9ccb87d5f6b795d40a6550f1e  (memory)
        16.0 MiB  000003df45a0b6526faca7be64431c6e/2073c64c0226e4386789053c9da3b8b1  (memory)
```

`--json` prints the same as JSON, and `--cluster` asks every live peer for its store.

## Let objects spill to disk

On Linux the peer keeps objects in memory (a huge-page tmpfs if one is mounted, else `/dev/shm` when it has at least 2 GiB). Past 80% of the memory limit it moves the oldest objects to disk in the background. Past the limit plus a quarter, new objects are written straight to disk. A spilled object is read by mapping its disk file, so there is no restore step. There is no store-full error either: a full disk raises `OSError` naming the directory. On macOS, or when the object directory is already on disk, nothing spills.

These are environment variables of the peer, so set them where the peer starts:

| Variable | Default | Effect |
|---|---|---|
| `CHIMAERA_MEMORY_POOL` | 90% of the object filesystem | memory objects may use before new ones go to disk, e.g. `8GiB` |
| `CHIMAERA_SPILL_DIR` | `spill/` in the peer's directory | where objects spill; several directories separated by `:` are used in turn |
| `CHIMAERA_SPILL_THRESHOLD` | `0.8` | share of the limit past which the oldest objects move to disk |
| `CHIMAERA_OBJECT_DIR` | found automatically | where objects live |
| `CHIMAERA_OBJECT_REPLICAS` | `1` | copies of each object kept on other peers (Linux only); `0` turns them off |
| `CHIMAERA_SPILL_URL` | unset | spill to `s3://`, `gs://` or `az://`; only in a peer built with the `object-store-spill` cargo feature, which is off by default |

[../objects.md](../objects.md) explains the internals: the arena, replicas, transfers between machines and every setting. [../configuration.md](../configuration.md) lists all variables.

## Process data in partitions

A word count over a directory of JSON-lines files, end to end:

```python run
import collections
import json
import operator
import os
import tempfile

from chimaera import ext

logs = tempfile.mkdtemp()
for day, lines in enumerate([["the cat sat", "the dog ran"], ["a cat ran", "the end"]]):
    with open(os.path.join(logs, f"day-{day}.jsonl"), "w") as f:
        for i, text in enumerate(lines):
            f.write(json.dumps({"text": text, "level": "info" if i == 0 else "debug"}) + "\n")

@ch.remote
def count_words(partition) -> list[tuple[str, int]]:
    counts = collections.Counter()
    for row in partition:                   # rows are read here, in the worker
        counts.update(row["text"].split())
    return list(counts.items())             # (key, value) pairs for the shuffle

parts = ch.data.partitions(os.path.join(logs, "*.jsonl"), columns=["text"])
totals = count_words.map(parts).reduce_by_key(operator.add, partitions=2)
counts = totals.to_dict()
assert counts["the"] == 3 and counts["cat"] == 2 and len(counts) == 7

out = tempfile.mkdtemp()
manifest = totals.write(ext.FileSink(out)).result()
assert manifest["rows"] == 7 and len(manifest["parts"]) == 2
assert sorted(os.listdir(out)) == ["_manifest.json", "part-00000.jsonl", "part-00001.jsonl"]
```

`fn.map(partitions)` runs your function on each partition, spread over the peers. The function receives a `ch.data.Partition` and iterates it for rows. Only what you ask for comes back to the script: here the `to_dict()` and the manifest.

### Cut input into partitions

`ch.data.partitions(source, *, n=None, columns=None, filter=None)` takes three kinds of source:

| Source | Partitions |
|---|---|
| A list or other sequence | `n` chunks (default: the peer's usable cores), each stored once with `ch.put` |
| A glob or a directory | one per file. `.jsonl`/`.ndjson` rows are dicts, `.csv` rows are dicts of strings, `.parquet` rows are dicts (needs `pyarrow`), any other file gives lines |
| An `ext.Source` | whatever its `partitions()` returns |

Every peer that may run a call needs to read the files at the same path. A pattern that matches nothing raises `FileNotFoundError`.

`columns=` keeps only those keys of dict rows. `filter=` is either a callable on each row or a list of `(column, op, value)` triples, all of which must hold; the ops are `==`, `!=`, `<`, `<=`, `>`, `>=`, `in` and `not in`:

```python run
info_only = ch.data.partitions(os.path.join(logs, "*.jsonl"), columns=["text"], filter=[("level", "==", "info")])
assert count_words.map(info_only).reduce_by_key(operator.add).to_dict()["cat"] == 2
```

Only `.parquet` pushes the filter and columns down into the reader. Other formats read the whole file in the worker and filter there. In this build, parquet filters have two bugs: a triple whose value is a number, such as `("age", ">", 20)`, raises `TypeError`, and filtering on a column you left out of `columns=` drops every row. Until they are fixed, use a callable filter on parquet and keep the filtered column in `columns=`.

### Group by key with `reduce_by_key`

`results.reduce_by_key(combine=None, *, partitions=None)` takes results that are iterables of `(key, value)` pairs and regroups them by key. Scatter calls split each result into buckets by key, and one gather call per output partition merges its buckets, so the pairs move between workers and never through your script. A worker that dies re-runs only its own call.

- `combine` folds two values for the same key and must be associative (`operator.add`). Without it, each key maps to the list of its values.
- `partitions` sets the number of output partitions. The default is the number of inputs, capped at the peer's usable cores.
- Keys go to buckets by a stable hash, so they must pickle the same way in every process: `str`, `bytes`, numbers and tuples of them do.
- When nothing has read the map yet, as in the example, its function runs inside the scatter calls and its pairs are never stored in between.
- Each gather reads one small handle per input, so keep inputs × partitions in the low millions.

### Read a `Partitioned` result

`reduce_by_key` returns a `ch.data.Partitioned`. The partitions stay on the workers until you read them:

```python run
assert len(totals) == 2
for part in totals:                       # one dict per partition: word -> count
    assert all(isinstance(word, str) for word in part)
assert dict(totals.items()) == counts
```

Each partition is a dict from key to its value (to the list of its values without a `combine`), and a key is in one partition only. `to_dict()` and `items()` bring every pair to the script. `totals.map(fn)` runs a remote function on each partition's dict instead, and `totals.futures` holds one future per partition.

### Write to a sink

`write(sink)` writes each partition in a call of its own, so the data never passes through your script, then calls the sink's `commit` once. It works on a `Partitioned` and on a map's results, where each result becomes one partition. `ext.FileSink(directory, format="jsonl")` writes `part-00000.jsonl` and so on, one JSON value per line (a pair becomes `["the", 3]`), then `_manifest.json`:

```python run
with open(os.path.join(out, "_manifest.json")) as f:
    assert json.load(f) == manifest
assert manifest["parts"][0]["part"] == 0 and sum(p["rows"] for p in manifest["parts"]) == 7
```

Each part is written under a temporary name and renamed, so a retried write replaces the file whole, and writing the result again leaves identical files. `format="parquet"` (or `write_parquet(directory)`) needs `pyarrow`. On a cluster, put the directory on storage every peer can write.

### Plug in your own source or sink

A source lists small, picklable partition descriptors and reads one of them. A sink's `write` returns a receipt per partition, and `commit` gets every receipt once, in partition order:

```python run
class Ranges(ext.Source):
    def __init__(self, n: int):
        self.n = n

    def partitions(self):                      # cheap descriptors, not data
        return [(i, min(i + 10, self.n)) for i in range(0, self.n, 10)]

    def read(self, part, columns=None, filter=None):
        return range(*part)                    # columns and filter are hints you may ignore

class SumSink(ext.Sink):
    def write(self, part_id, items):           # may run again if the call is retried
        return sum(items)

    def commit(self, receipts):                # runs once
        return sum(receipts)

@ch.remote
def squared(partition) -> list[int]:
    return [x * x for x in partition]

assert squared.map(ch.data.partitions(Ranges(25))).write(SumSink()).result() == sum(x * x for x in range(25))
```

Sources and sinks are pickled to the workers, so keep their state small: paths, URIs, names of credentials. A retried call runs `write` again for its partition, so `write` must be idempotent: overwrite a part, never append to it. Do the one-time step, such as publishing a table, in `commit`.

### Send a type pickle can't handle

`ext.register_serializer(cls, dumps=..., loads=...)` makes values of `cls` travel as the bytes your `dumps` returns:

```python run
import threading

class Tally:
    def __init__(self, n: int = 0):
        self.n, self.lock = n, threading.Lock()       # pickle refuses locks

def tally_dumps(t: Tally) -> bytes:
    return str(t.n).encode()

def tally_loads(data: bytes) -> Tally:
    return Tally(int(data))

ext.register_serializer(Tally, dumps=tally_dumps, loads=tally_loads)

@ch.remote
def bump(t: Tally) -> int:
    with t.lock:
        t.n += 1
    return t.n

assert bump.remote(Tally(41)).result() == 42
```

`loads` must be a module-level function, not a lambda. Register in every process that sends the type: a worker that returns a `Tally` needs the registration too, so put the call in a module that both your script and your functions import.

## Stream items from a generator

A generator function called with `.remote()` streams its items as it yields them:

```python run
@ch.remote
def events(n: int):
    for i in range(n):
        yield {"id": i}

stream = events.options(max_in_flight=8).remote(100)
assert next(stream) == {"id": 0}
assert sum(1 for _ in stream) == 99
```

The generator runs at most `max_in_flight` items ahead of you (default 64), so a slow consumer holds back the producer instead of filling memory. `stream.close()` stops it, and `async for` works too.

Generator methods on actors don't stream yet. The call fails with `cannot pickle 'generator' object`, so return a list from the method or put items on a `ch.coord.Queue`. [tasks.md](tasks.md) covers generators in full: errors, retries and replay.

## Share state between calls

Four tools in `ch.coord` hold state that calls and scripts share. They differ in what they survive:

| Tool | Held by | Lasts | If its holder dies |
|---|---|---|---|
| `Queue`, `Topic` | one actor | until the run ends, or past it with `detach=True` | contents are lost; later calls raise `ch.ActorDied` |
| `Store`, `WorkQueue` | a Raft group, replicated across peers | until you delete the keys, or their `ttl=` passes | nothing is lost while a majority of the group's peers are alive |

### Pass work through a queue

```python run
@ch.remote
def drain(q) -> int:
    return sum(q)                         # takes items until the queue is closed and empty

q = ch.coord.Queue(maxsize=100)
total = drain.remote(q)
for i in range(10):
    q.put(i).result()
q.close()
assert total.result() == 45
```

`put` and `get` return futures. With `maxsize=` set, a `put` on a full queue waits inside the queue until there is room; the default `maxsize=0` means unbounded. With `timeout=`, the future of a `put` fails with the standard library's `queue.Full`, and that of a `get` with `queue.Empty`. After `close()`, a `put` fails with `ch.errors.Closed`, and `get` drains what is left, then fails with `Closed`. `name="jobs"` shares one queue between everyone who names it, and the first creator's `maxsize` applies.

### Broadcast through a topic

```python run
topic = ch.coord.Topic(retain=100)
sub = topic.subscribe("earliest")
for i in range(3):
    topic.publish(i).result()             # each publish gives the item's offset
topic.close()
assert list(sub) == [0, 1, 2]
```

Every subscriber gets every item, at its own pace. The topic keeps a bounded log of the newest items: `retain=1024` (the default) counts items, `retain="1 GiB"` counts bytes. `subscribe("latest")` starts at items published from now on, `"earliest"` at everything retained, and an integer at that offset. A subscriber that falls behind the log skips ahead, and `sub.skipped` says how many items it missed.

### Keep replicated state in a store

A `ch.coord.Store` namespace is a set of keys kept in one Raft group: every operation on it is linearizable, and it survives the loss of any process and a minority of peers. Keys outlive the run, so this example takes a fresh namespace:

```python run
import uuid

store = ch.coord.Store(f"guide-{uuid.uuid4().hex[:8]}")
v1 = store.put("config", {"lr": 3e-4}, if_version=0)     # create only if absent
v2 = store.put("config", {"lr": 1e-4}, if_version=v1)    # compare-and-set
try:
    store.put("config", {"lr": 9.9}, if_version=v1)      # stale version
except ch.errors.VersionConflict as e:
    assert e.version == v2
assert store.get("config") == {"lr": 1e-4}
assert store.counter("seen").add(2) == 2                 # atomic

with store.lock("leader", ttl="10s") as lease:           # renewed while held
    store.put("state", "mine", fence=lease)              # refused once the lease is lost

changes = store.watch("jobs/")
store.put("jobs/1", "queued")
event = next(changes)
assert (event.kind, event.key, event.value) == ("put", "jobs/1", "queued")

@ch.remote
def meet(store, parties: int) -> int:
    return store.barrier("round", parties).wait(timeout=60)

assert ch.get([meet.remote(store, 2) for _ in range(2)]) == [1, 1]
```

What each part does:

- `put` returns the key's new version. `if_version=v` makes it a compare-and-set that raises `ch.errors.VersionConflict` (its `.version` is the current one), and `if_version=0` means "only if the key doesn't exist". `delete` takes `if_version=` too.
- Values are pickled and limited to 1 MiB; a bigger one raises `ValueError`. Store a `ch.put` future instead, remembering that it reads only while its run lasts.
- `ttl="30s"` on `put` makes the key expire.
- `lock(key, ttl="30s", timeout=None)` returns a lease that is renewed in the background while you hold it. Its `token` grows with every grant. A write with `fence=lease` raises `ch.errors.LeaseLost` once the lease expired or passed to another holder, which is what makes leader election safe. `store.holder(key)` says who holds it.
- `watch(prefix, start="latest")` yields `put` and `delete` events in order. A watch that falls behind replays the current value of every key under the prefix, so it may repeat a change but never misses one.
- `barrier(key, parties).wait()` returns when that many callers have arrived, round after round.
- `scan(prefix)` and `keys(prefix)` list keys in order.

All keys of a namespace go through one Raft group, so spread heavy traffic over several namespaces.

### Hand out work that must not be lost

`ch.coord.WorkQueue(name)` is a first-in, first-out queue kept in a store. A consumer claims an item under a lease: the item is removed when the `with` block finishes, and delivered again if the block raises or the consumer's process dies and the lease lapses.

```python run
jobs = ch.coord.WorkQueue(f"guide-jobs-{uuid.uuid4().hex[:8]}")
jobs.put("resize image 7")
try:
    with jobs.claim(lease="30s") as job:
        raise RuntimeError("worker failed")
except RuntimeError:
    pass
with jobs.claim() as job:                 # delivered again
    assert job == "resize image 7"
assert jobs.qsize() == 0
```

Unlike `Queue`, its methods are synchronous: `put` returns once the item is stored, and `get()` claims and removes in one step. `maxsize=`, `timeout=`, `close()` and iteration work as they do on `Queue`. Pass the queue to calls, or open it by name from any process.

Next: [failures.md](failures.md) covers what happens to calls, actors and stored values when processes and machines fail.
