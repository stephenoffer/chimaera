# See what your program is doing

Every call leaves a record on the peer: what it printed, each attempt, what
it measured and why it waited. Here is how to read it from Python or the
`chimaera` command, and what the `CHnnn` warnings mean.

## Read what your calls print

Whatever a call prints or logs with `logging` shows up in your script's
terminal, labeled with the function and the worker's process id:

```python run
import logging
import chimaera as ch

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("etl")


@ch.remote
def tokenize(text: str) -> list[str]:
    print(f"tokenizing {len(text)} chars")
    log.info("got %d chars", len(text))
    return text.split()


run = ch.init()
words = tokenize.remote("the quick brown fox")
assert words.result() == ["the", "quick", "brown", "fox"]
```

What the script printed:

```text
(tokenize pid=61338) tokenizing 19 chars
(tokenize pid=61338) INFO etl: got 19 chars
```

Workers log at your script's level, so
`logging.basicConfig(level=logging.INFO)` in the script shows `log.info`
from inside calls. At the default `WARNING` level those records are
dropped.

The output outlives your script: its peer keeps it, and `run.logs()` reads
it back:

```python run
lines = list(run.logs())
assert any(line.endswith("stdout| tokenizing 19 chars") for line in lines)
assert any(line.endswith("stderr| INFO etl: got 19 chars") for line in lines)
```

From a shell, `chimaera logs RUN` prints the same lines:

```bash
chimaera logs 0000027da0522d95e30e656fd7268a93
```

```text
0000027d12b9 stdout| tokenizing 19 chars
0000027d12b9 stderr| INFO etl: got 19 chars
```

<details>
<summary>More: how workers get your log level, the line format, following, where output is kept</summary>

The run carries your root logger's level, and a worker with no logging
handlers of its own calls `logging.basicConfig` at that level.

`run.logs()` yields lines of `<task> <stream>| <text>`, where `<task>` is
the first 12 hex digits of the task id (the second half of `future.id`).

`run.logs(follow=True)` keeps waiting until you break out of the loop, and
`chimaera logs RUN --follow` waits for more (Ctrl-C to stop).

Each peer stores its calls' output in `logs/runs/<run>.log` under its data
directory, up to 64 MiB per run. On a cluster, output from calls on other
peers reaches your terminal as it is printed.

</details>

## Ask a call what happened

`future.explain()` returns a dict with the call's state, why it is
waiting, and one entry per attempt:

```python run
info = words.explain()
assert info["state"] == "Done" and info["pending_reason"] == ""
attempt = info["attempts"][0]
assert attempt["end_reason"] == "ok"
assert attempt["placement"]                      # why it ran where it ran
assert "wall_seconds" in attempt["measures"]
```

<details>
<summary>More: every key in the dict</summary>

| Key | What it holds |
|---|---|
| `state` | `"Pending"`, `"Ready"`, `"Running"` or `"Done"` |
| `pending_reason` | Why a call that has not started is waiting, e.g. `waiting on 1 prerequisite(s)`; empty once it runs |
| `attempts` | One dict per attempt: `attempt`, `end_reason` (`ok`, `user_error`, ...), `detail`, `placement` (why this peer), `reservation` (the CPUs and memory it was given, and where those numbers came from), `granted` and `measures` |
| `measures` | Name to `(value, scope, method)`: `wall_seconds`, `cpu_seconds`, `mean_cores`, `rss_peak_bytes`, `rss_end_bytes`, `memory_private_peak_bytes` |
| `failures` | Messages of attempts lost to infrastructure (a worker or peer that died) |

</details>

`explain()` is most useful on a stuck call. Here `plus_one` waits for
`slow`:

```python run
import time


@ch.remote
def slow(x: int) -> int:
    time.sleep(1)
    return x


@ch.remote
def plus_one(x: int) -> int:
    return x + 1


later = plus_one.remote(slow.remote(1))
time.sleep(0.2)
assert later.explain()["pending_reason"] == "waiting on 1 prerequisite(s)"
assert later.result() == 2
```

You don't always have to ask. After `result()` waits 60 seconds on a call
that hasn't started, your script prints the reason to stderr.

<details>
<summary>More: the stall notice, and what a failed call looks like</summary>

The notice is printed once and reads
`chimaera: waited 60 s on call <id>: <reason>`. `CHIMAERA_STALL_NOTICE_S`
changes the 60.

A call that raised ends with `user_error`:

```python run
@ch.remote(max_attempts=1)
def parse_row() -> None:
    raise ValueError("bad row 17")


bad = parse_row.remote()
try:
    bad.result()
except ValueError:
    pass
assert bad.explain()["attempts"][0]["end_reason"] == "user_error"
```

</details>

From a shell, `chimaera inspect RUN` counts a run's calls by state, and
`chimaera explain RUN --task TASK` shows one of them:

```text
$ chimaera inspect 0000027da0522d95e30e656fd7268a93
run 0000027da0522d95e30e656fd7268a93 · Finished
  failed:user              1
  succeeded                204

$ chimaera explain 0000027da0522d95e30e656fd7268a93 --task 0000027d1e747e2e9da0a324364a670e
task Done
  error (User): builtins.ValueError bad row 17
  attempt 1 · user_error · wall 0.001 s [attempt] · cpu 0.001 s [process] · mean cores 0.782 [process] · rss peak 23.7 MiB [process_lifetime]
```

<details>
<summary>More: where TASK comes from, JSON output, tuning decisions</summary>

`TASK` is the second half of `future.id`, which is the run id and then the
task id, 32 hex digits each. Add `--json` to `inspect` or `explain` for the
full record.

`chimaera explain RUN --decisions` lists the tuning decisions made while
the run was active: which controller changed which setting, from what
to what, and why. It prints nothing if there were none.

</details>

## Report progress from inside a call

A long call publishes progress with `ch.context().report(...)`. Whoever
holds its future reads it with `future.metrics()`:

```python run
@ch.remote
def train(epochs: int) -> str:
    for epoch in range(epochs):
        ch.context().report(epoch=epoch, loss=1.0 / (epoch + 1))
    return "done"


job = train.remote(3)
assert job.result() == "done"
assert [m["epoch"] for m in job.metrics()] == [0, 1, 2]
```

Each report is one write to the replicated store, so report per epoch or
per thousand steps, not per sample. Reports expire after a day. The rest of
the call's context is in [tasks.md](tasks.md).

## Look at the whole cluster

`ch.runs.cluster()` shows the cluster as your script's peer knows it:

```python run
nodes = ch.runs.cluster().nodes()
assert nodes and all(n.cpus > 0 for n in nodes)
total = ch.runs.cluster().resources()
assert total["nodes"] >= 1 and total["cpus"] >= 1
```

<details>
<summary>More: the fields of a node and of resources()</summary>

Each node is a named tuple: `peer`, `address`, `cpus`, `memory_bytes`, `gpus`,
`alive`, `cpu_used`, `queued`, `this_peer`, `labels`, `actors`,
`object_bytes` and `draining`. `resources()` sums the live ones into `nodes`,
`cpus`, `cpus_in_use`, `memory_bytes` and `gpus`.

</details>

`events()` streams what happens: peers joining and leaving, actors
restarting, calls that failed for good. Here it reads back the failed-call
events still on record, then stops:

```python run
failed = [e for e in ch.runs.cluster().events(["task_failed"], start="earliest", follow=False)]
assert any(e.task == bad.id and "bad row 17" in e.message for e in failed)
```

<details>
<summary>More: event kinds, start and follow, where events come from</summary>

The kinds are `member` (peers joining and leaving), `actor` (actors
starting, restarting and dying), `task_failed` (calls that failed for
good), `decision` and `drain`. By default `events()` starts with the next
event and keeps waiting. `start="earliest"` begins at the oldest event the
peer still holds (it keeps the latest 10,000), and `follow=False` stops at
the newest.

Events come from the connected peer: member events for the whole cluster,
call and actor events for the shards it leads. They are notifications. The
durable record is `run.outcomes()` or the actor itself.

</details>

## Check a peer from the shell

When work is slow or stuck, run `chimaera status` first. It shows the
peer's CPUs, workers, queue, memory pool and the settings it chose by
itself:

```bash
chimaera status
```

```text
peer 0000000024e6 (incarnation 1) · 0.00/12.00 CPU in use · 16 executors (0 busy) · 0 queued · 0 running · 3 sessions · 0 object bytes
  memory: 392 of 33177 MiB pool in use, 9447 MiB available on the host
  automatic: memory pool 33177 MiB = usable 36864 MiB - agent reserve 3686 MiB; objects on disk, outside the pool
  automatic: cpu capacity 12.00
  automatic: no task limit (cgroup pids.max)
  automatic: 23 learned function profile(s), saved in .../profiles.pb
```

The other commands:

| Command | Shows |
|---|---|
| `chimaera runs` | Runs, newest first: state, age, the command that started each (`--limit`, default 20; `0` for all) |
| `chimaera inspect RUN` | A run's calls counted by state |
| `chimaera explain RUN --task TASK` | One call's attempts, measurements, error and waiting reason |
| `chimaera explain RUN --decisions` | The tuning decisions the peer made |
| `chimaera logs RUN [--follow]` | What the run's calls printed |
| `chimaera actors` | Live named actors (`--prefix` to filter) |
| `chimaera profiles` | What the peer learned about each function, and the reservation its next call gets |
| `chimaera objects` | What the object store holds; see [data.md](data.md) |
| `chimaera doctor` | What this host supports: cgroups, NUMA, GPUs, fsync speed |

`status`, `runs`, `inspect`, `explain`, `logs`, `actors` and `cancel` take
`--cluster NAME` to ask a cluster instead of the local peer
([clusters.md](clusters.md)). If a peer itself misbehaves, read
`peer.log` in its data directory.

<details>
<summary>More: cluster status, sample runs, profiles and doctor output, peer and worker logs</summary>

On a cluster, `status` also lists every peer, marking the ones it can't
reach `unreachable`, and each Raft group's leader and voters.

`profiles` and `objects` read this machine's data directory. Most commands take
`--json`. Every flag is in [../cli.md](../cli.md).

`chimaera runs` on the local peer after the examples above:

```text
RUN                               STATE       AGE  COMMAND
0000027da0522d95e30e656fd7268a93  finished     6s  etl.py
```

`chimaera profiles` reads what the peer saves every 30 seconds while
profiles change, and when it stops. Right after a first run, it may say
none are saved yet. `calls` counts the samples kept, at most 64 per
function. A name can repeat, because profiles are kept per code digest:

```text
$ chimaera profiles --fn __main__
23 function profile(s), saved 18 s ago in .../profiles.pb
  __main__.double: 64 calls, 0.000 s p50, 0.66 cores on average; next call gets cpus 1 (default), memory 30 MiB (p95 peak 24 MiB over 64 calls of this input size × 1.2)
  __main__.train: 1 calls, 0.004 s p50, 0.51 cores on average; next call gets cpus 1 (default), memory not known yet
  __main__.tokenize: 1 calls, 0.001 s p50, 0.78 cores on average; next call gets cpus 1 (default), memory not known yet
```

`chimaera doctor` on a laptop:

```text
os macos aarch64 · kernel 23.5.0
cpus 12 (usable 12.00) · memory 36.0 GiB
numa nodes 1 · gpus 1
isolated accounting: unavailable: accounting="isolated" fails preflight here; measurements are process-scoped
fsync p50 39 µs in .../homes/observability (cluster peers pay this per commit)
```

By default `peer.log` is `~/.chimaera/local/peer.log`. Each worker
writes `logs/executor-<n>.log` in the same directory. `CHIMAERA_LOG` sets
the log filter in the `tracing` crate's syntax; the default is
`chimaera=info,chimaera_peer=info,warn`.

</details>

## Scrape metrics with Prometheus

Start a peer with `CHIMAERA_METRICS_LISTEN` set to an address and it serves
Prometheus metrics at `GET /metrics`:

```bash
CHIMAERA_METRICS_LISTEN=127.0.0.1:9700 chimaera peer start --idle-timeout 120
curl -s http://127.0.0.1:9700/metrics
```

```text
# HELP chimaera_tasks_queued Calls waiting for a slot on this peer.
# TYPE chimaera_tasks_queued gauge
chimaera_tasks_queued 0
# HELP chimaera_tasks_running Calls running on this peer.
# TYPE chimaera_tasks_running gauge
chimaera_tasks_running 0
# HELP chimaera_cpu_in_use CPUs granted to running calls.
# TYPE chimaera_cpu_in_use gauge
chimaera_cpu_in_use 0
...
# HELP chimaera_worker_deaths_total Worker processes that exited while running a call or hosting an actor.
# TYPE chimaera_worker_deaths_total counter
chimaera_worker_deaths_total 0
...
chimaera_cluster_nodes{state="alive"} 1
chimaera_cluster_nodes{state="dead"} 0
```

The endpoint has no token. It serves counts only, never run names or
values, but bind it where only your scraper can reach it.

<details>
<summary>More: every metric, other paths, a script's own peer, Kubernetes</summary>

- Gauges: calls queued and running, CPUs in use, workers, object and memory bytes, connected drivers, proposals in flight, and dispatch latency (p50 and p99).
- Counters since the peer started: proposals and proposals no leader accepted, peers declared dead, worker deaths, OOM kills, workers killed for a cancelled call, and failed object replicas.
- On cluster peers, `chimaera_groups_without_leader`, and for each group they hold, `chimaera_group_leading{group="N"}` plus, on the leader, `chimaera_group_max_lag_entries{group="N"}`.

Every other path answers 404.

A local peer that your script starts inherits the script's `CHIMAERA_*`
variables, so `CHIMAERA_METRICS_LISTEN` works for it too.
`chimaera k8s render` turns the endpoint on and adds the
`prometheus.io/scrape` annotations ([../deploy.md](../deploy.md)).

</details>

## Understand the CHnnn warnings

When Chimaera sees a pattern that is slow or will fail, it warns with a
code. Most are Python warnings (`UserWarning`, shown once per line by
default); a few are errors. Here is CH101, from waiting on each call before
submitting the next:

```python run
import warnings


@ch.remote
def double(x: int) -> int:
    return 2 * x


with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    total = 0
    for i in range(200):
        total += double.remote(i).result()    # one call at a time
assert total == 2 * sum(range(200))
assert any(str(w.message).startswith("CH101") for w in caught)
```

| Code | Raised as | What it means | What to do |
|---|---|---|---|
| CH101 | warning | Calls from one line ran 200 times one at a time: each result was awaited before the next call was submitted | Submit them all first, or use `fn.map(items)` |
| CH103 | warning | A large value fetched with `.result()` was passed straight back into `.remote()`, a round trip through your script | Pass the future itself |
| CH107 | warning | The same large value (1 MiB or more) was passed from one line at least 20 times, and encoding it took 0.1 s or more in total; it is stored once but encoded every time | `ch.put(value)` once, or `fn.partial(value)` |
| CH108 | warning | A function captures a variable over 1 MiB; it is snapshotted on first submission and shipped once per peer | Pass it with `ch.put(...)` if it changes (over 256 MiB is an error) |
| CH110 | warning | Calls are submitted faster than they finish, and `.remote()` now waits for room | `fn.map(items)`, or consume results with `ch.as_completed` |
| CH111 | warning | The local peer runs a different `chimaera` binary than this program would start (rebuilt or upgraded) | `chimaera stop`; the next call starts the new one |
| CH112 | warning | A tensor sent from a GPU arrived where there is no GPU, so it is on the CPU | Ask the call for `gpus=1`, or send `t.cpu()` |
| CH113 | error (`ChimaeraError`) | A process forked from one using Chimaera tried to submit, wait or fetch | Use `multiprocessing.get_context("spawn")` or `"forkserver"`, or make the work a call |
| CH115 | error, in the call | The call's function would not load, and the worker runs a different Python version than your script | Run the script with the workers' Python version, or put the function in an importable module |
| CH116 | warning | The cluster runs a different Chimaera version than your script | Install the cluster's version locally, or upgrade the cluster |
| CH117 | warning | One `ch.get` has read results totalling more than a quarter of this machine's memory | Consume results as they finish, or reduce them inside calls |
| CH118 | warning | A list, dict, set or tuple shared with calls pickles to 8 MiB or more; every reader unpickles its own copy | Use a NumPy array or an Arrow table, which readers map instead |
| CH119 | note on the error | A call that asked for no GPU hit a CUDA error on a machine that has GPUs | `@ch.remote(gpus=1)` or `fn.options(gpus=1)` |
| CH120 | warning | The same code was wrapped as a new remote function 100 times (for example `ch.remote(lambda ...)` in a loop) | Define the remote function once, outside the loop |

To silence one code, filter on the start of its message:

```python run
warnings.filterwarnings("ignore", message="CH101")
```

Next: when calls fail or peers die, [failures.md](failures.md) explains what is retried and what you see.
