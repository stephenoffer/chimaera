# See what your program is doing

Wondering what your calls are up to? Every call leaves a record on the peer:
what it printed, each attempt, what it measured and why it waited. Read it
from Python or the `chimaera` command, and look up any `CHnnn` warning in
the table at the end.

## Read what your calls print

Just print. Whatever a call prints or logs with `logging` shows up in your
script's terminal, labeled with the function and the worker's process id:

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

Here's what the script printed:

```text
(tokenize pid=61338) tokenizing 19 chars
(tokenize pid=61338) INFO etl: got 19 chars
```

The output outlives your script: its peer keeps it, and `run.logs()` reads
it back:

```python run
lines = list(run.logs())
assert any(line.endswith("stdout| tokenizing 19 chars") for line in lines)
assert any(line.endswith("stderr| INFO etl: got 19 chars") for line in lines)
```

Prefer the shell? `chimaera logs RUN` prints the same lines:

```bash
chimaera logs 0000027da0522d95e30e656fd7268a93
```

```text
0000027d12b9 stdout| tokenizing 19 chars
0000027d12b9 stderr| INFO etl: got 19 chars
```

<details>
<summary>Log levels and where output goes</summary>

Workers log at your script's level. The run carries your root logger's
level, and a worker with no logging handlers of its own calls
`logging.basicConfig` at that level. So
`logging.basicConfig(level=logging.INFO)` in the script shows `log.info`
from inside calls. Leave the default `WARNING` level and those records are
dropped.

`run.logs()` yields lines of `<task> <stream>| <text>`, where `<task>` is
the first 12 hex digits of the task id (the second half of `future.id`).

To follow new output, use `run.logs(follow=True)` (break out of the loop to
stop) or `chimaera logs RUN --follow` (Ctrl-C to stop).

Each peer stores its calls' output in `logs/runs/<run>.log` under its data
directory, up to 64 MiB per run. On a cluster, output from calls on other
peers reaches your terminal as it's printed. A script that stops reading
(paused, or busy for long) doesn't grow the peer's memory: the peer holds
16 MiB for it, its results wait as ids, and output past that is skipped,
with a line saying how many lines `chimaera logs` still has.

</details>

## Ask a call what happened

Want one call's story? `future.explain()` returns a dict with its state, why
it's waiting, and one entry per attempt:

```python run
info = words.explain()
assert info["state"] == "Done" and info["pending_reason"] == ""
attempt = info["attempts"][0]
assert attempt["end_reason"] == "ok"
assert attempt["placement"]                      # why it ran where it ran
assert "wall_seconds" in attempt["measures"]
```

<details>
<summary>Every key explain returns</summary>

| Key | What it holds |
|---|---|
| `state` | `"Pending"`, `"Ready"`, `"Running"` or `"Done"` |
| `pending_reason` | Why a call that has not started is waiting, e.g. `waiting on 1 prerequisite(s)`; empty once it runs |
| `attempts` | One dict per attempt: `attempt`, `end_reason` (`ok`, `user_error`, ...), `detail`, `placement` (why this peer), `reservation` (the CPUs and memory it was given, and where those numbers came from), `settings` (the same per setting: `cpus`, `memory` and `batch`, each with `value`, `requested`, `min`, `max`, `source` and `reason`; `source` is `"clamped"` when a `ch.auto` bound or the peer's size changed what was chosen, and `reason` says which), `granted` (the policy the call asked for), `measures`, and the wall-clock time in ns of each stage it reached, `None` where it was not recorded: `submitted_unix_ns`, `ready_unix_ns` (the attempt reached the peer that ran it), `queued_unix_ns` (its inputs were there and it joined the queue), `dispatched_unix_ns`, `py_entry_unix_ns`, `py_exit_unix_ns`, `published_unix_ns` |
| `measures` | Name to `(value, scope, method)`: `wall_seconds`, `cpu_seconds`, `mean_cores`, `rss_peak_bytes`, `rss_end_bytes`, `memory_private_peak_bytes` |
| `failures` | Messages of attempts lost to infrastructure (a worker or peer that died) |

</details>

Where `explain()` really helps is a stuck call. Here `plus_one` is waiting
on `slow`:

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

You don't always have to ask. Once `result()` waits 60 seconds on a call
that hasn't started, your script prints the reason to stderr.

<details>
<summary>Stall notices and failed calls</summary>

You'll see the notice once, and it reads
`chimaera: waited 60 s on call <id>: <reason>`. Set
`CHIMAERA_STALL_NOTICE_S` to change the 60.

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
`chimaera explain RUN --task TASK` zooms in on one:

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
<summary>Task ids and tuning decisions</summary>

`TASK` is the second half of `future.id`, which is the run id and then the
task id, 32 hex digits each. Add `--json` to `inspect` or `explain` for the
full record.

`chimaera explain RUN --decisions` lists the tuning decisions made while the
run was active: which controller changed which setting, from what to what,
and why. If there were none, it prints nothing.

</details>

## Report progress from inside a call

Want to see how far along a long call is? Have it publish progress with
`ch.context().report(...)`, and whoever holds its future reads it with
`future.metrics()`:

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

Each report is one write to the replicated store, so report per epoch or per
thousand steps, not per sample. Reports expire after a day. The rest of the
call's context is in [tasks.md](tasks.md).

## Look at the whole cluster

Curious what's out there? `ch.runs.cluster()` shows the cluster as your
script's peer knows it:

```python run
nodes = ch.runs.cluster().nodes()
assert nodes and all(n.cpus > 0 for n in nodes)
total = ch.runs.cluster().resources()
assert total["nodes"] >= 1 and total["cpus"] >= 1
```

<details>
<summary>Node and resources() fields</summary>

Each node is a named tuple: `peer`, `address`, `cpus`, `memory_bytes`,
`gpus`, `alive`, `cpu_used`, `queued`, `this_peer`, `labels`, `actors`,
`object_bytes`, `draining` and `object_layout` (the newest layout of stored
results it reads; a shuffle writes the newest only while every live peer
reads it). `resources()` adds up the live ones into
`nodes`, `cpus`, `cpus_in_use`, `memory_bytes` and `gpus`.

</details>

`events()` streams what happens: peers joining and leaving, actors
restarting, calls that failed for good. Here it reads back the failed-call
events still on record, then stops:

```python run
failed = [e for e in ch.runs.cluster().events(["task_failed"], start="earliest", follow=False)]
assert any(e.task == bad.id and "bad row 17" in e.message for e in failed)
```

<details>
<summary>Event kinds and sources</summary>

The kinds are `member` (peers joining and leaving), `actor` (actors
starting, restarting and dying), `task_failed` (calls that failed for
good), `decision` and `drain`. By default `events()` starts with the next
event and keeps waiting. `start="earliest"` begins at the oldest event the
peer still holds (it keeps the latest 10,000), and `follow=False` stops at
the newest.

Events come from the connected peer: member events for the whole cluster,
call and actor events for the shards it leads. They're notifications. For
the durable record, read `run.outcomes()` or ask the actor itself.

</details>

## Check a peer from the shell

Something slow or stuck? Run `chimaera status` first. It shows the peer's
CPUs, workers, queue, memory pool and the settings it picked on its own:

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

<details>
<summary>The other CLI commands</summary>

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
([clusters.md](clusters.md)). If a peer itself misbehaves, check `peer.log`
in its data directory.

</details>

<details>
<summary>Sample output and log files</summary>

On a cluster, `status` also lists every peer, marking the ones it can't
reach `unreachable`, and each Raft group's leader and voters.

`profiles` and `objects` read this machine's data directory. Most commands
take `--json`. Every flag is in [../cli.md](../cli.md).

Here's `chimaera runs` on the local peer after the examples above:

```text
RUN                               STATE       AGE  COMMAND
0000027da0522d95e30e656fd7268a93  finished     6s  etl.py
```

`chimaera profiles` reads what the peer has saved: every 30 seconds while
profiles change, and again when it stops. So right after a first run it may
say none are saved yet. `calls` counts the samples kept, at most 64 per
function. A name can repeat, because profiles are kept per code digest:

```text
$ chimaera profiles --fn __main__
23 function profile(s), saved 18 s ago in .../profiles.pb
  __main__.double: 64 calls, 0.000 s p50, 0.66 cores on average; next call gets cpus 1 (default), memory 30 MiB (p95 peak 24 MiB over 64 calls of this input size × 1.2)
  __main__.train: 1 calls, 0.004 s p50, 0.51 cores on average; next call gets cpus 1 (default), memory not known yet
  __main__.tokenize: 1 calls, 0.001 s p50, 0.78 cores on average; next call gets cpus 1 (default), memory not known yet
```

And `chimaera doctor` on a laptop:

```text
os macos aarch64 · kernel 23.5.0
cpus 12 (usable 12.00) · memory 36.0 GiB
cores 12 · sockets 1 · last-level caches 1 · numa nodes 1 · gpus 1
isolated accounting: unavailable: accounting="isolated" fails preflight here; measurements are process-scoped
fsync p50 39 µs in .../homes/observability (cluster peers pay this per commit)
python 3.14.4 (.../.venv/bin/python) imports chimaera (.../python/chimaera/_native.abi3.so, this binary's build) and starts the peer binary .../target/release/chimaera
unix sockets: socket, bind and connect work in .../homes/observability
cluster: none selected; drivers here start a local peer
```

When one of the last three checks fails, its line names the step that
failed and the fix: an interpreter that cannot import chimaera, or whose
`chimaera._native` is another build than the binary; a sandbox that refuses
Unix sockets; a cluster that TCP, TLS or the token cannot reach
([cli.md](../cli.md#chimaera-doctor)).

By default `peer.log` is `~/.chimaera/local/peer.log`. Each worker
writes `logs/executor-<n>.log` in the same directory. `CHIMAERA_LOG` sets
the log filter in the `tracing` crate's syntax; the default is
`chimaera=info,chimaera_peer=info,warn`.

</details>

## Scrape metrics with Prometheus

Already running Prometheus? Start a peer with `CHIMAERA_METRICS_LISTEN` set
to an address and it serves metrics at `GET /metrics`:

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

The endpoint has no token. It serves counts only, never run names or values,
but still bind it where only your scraper can reach it.

<details>
<summary>Exported metrics and Kubernetes</summary>

- Gauges: calls queued and running, CPUs in use, workers, object and memory bytes, connected drivers, proposals in flight, and dispatch latency (p50 and p99).
- Counters since startup: proposals and proposals no leader accepted, peers declared dead, worker deaths, OOM kills, workers killed for a canceled call, and failed object replicas. Also the capacity summaries the peer answered for other cells (`chimaera_fleet_summaries_served_total`), the store requests it served (`chimaera_store_requests_total`), and the times it listed the fleet directory whole as its cell's publisher (`chimaera_fleet_directory_listings_total`).
- On peers of a federated cell ([../federation.md](../federation.md)), what the cell's view of the fleet holds: `chimaera_fleet_cells`, `chimaera_fleet_publishing` (1 on the cell's summary publisher) and `chimaera_fleet_epoch`.
- On cluster peers, `chimaera_groups_without_leader`, and for each group they hold, `chimaera_group_leading{group="N"}` plus, on the leader, `chimaera_group_max_lag_entries{group="N"}`.
- On cluster peers, Raft heartbeat round trips (`chimaera_raft_heartbeat_p50_microseconds`, `..._p99_...`), the paced rate (`chimaera_mesh_rate_bytes`, see `CHIMAERA_MESH_RATE` in [../configuration.md](../configuration.md)), and per traffic class the bytes sent to other peers and the time they waited for their turn (`chimaera_mesh_sent_bytes_total{class="checkpoint"}`, `chimaera_mesh_wait_seconds_total{...}`).

Any other path gets a 404.

A local peer your script starts inherits the script's `CHIMAERA_*`
variables, so `CHIMAERA_METRICS_LISTEN` works there too. On Kubernetes,
`chimaera k8s render` turns the endpoint on and adds the
`prometheus.io/scrape` annotations ([../deploy.md](../deploy.md)).

</details>

## Understand the CHnnn warnings

When Chimaera spots a pattern that's slow or will fail, it warns you with a
code. Most are Python warnings (`UserWarning`, shown once per line by
default); a few are errors. Here's CH101, from waiting on each call before
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
| CH107 | warning | The same large value (1 MiB or more) was passed from one line at least 20 times, and encoding it took 0.1 s or more in total; it is stored once but encoded every time | `ch.put(value)` once, or `fn.partial(value)`; for an array made before the first call, `ch.init()` before making it |
| CH108 | warning | A function captures a variable over 1 MiB; it is snapshotted on first submission and shipped once per peer | Pass it with `ch.put(...)` if it changes (over 256 MiB is an error) |
| CH110 | warning | Calls are submitted faster than they finish, and `.remote()` now waits for room | `fn.map(items)`, or consume results with `ch.as_completed` |
| CH111 | warning | The local peer runs a different `chimaera` binary than this program would start (rebuilt or upgraded) | `chimaera stop`; the next call starts the new one |
| CH112 | warning | A tensor sent from a GPU arrived where there is no GPU, so it is on the CPU | Ask the call for `gpus=1`, or send `t.cpu()` |
| CH113 | error (`ChimaeraError`) | A process forked from one using Chimaera tried to submit, wait or fetch | Use `multiprocessing.get_context("spawn")` or `"forkserver"`, or make the work a call |
| CH115 | error, in the call | The call's function would not load, and the worker runs a different Python version than your script | Run the script with the workers' Python version, or put the function in an importable module |
| CH116 | warning | The cluster runs a different Chimaera version than your script | Install the cluster's version locally, or upgrade the cluster |
| CH117 | warning | One `ch.get` holds results totalling more than a quarter of this machine's memory, or more than half of what this process's cgroup memory limit has left | Consume results as they finish, or reduce them inside calls |
| CH118 | warning | A list, dict, set or tuple shared with calls pickles to 8 MiB or more; every reader unpickles its own copy | Use a NumPy array or an Arrow table, which readers map instead |
| CH119 | note on the error | A call that asked for no GPU hit a CUDA error on a machine that has GPUs | `@ch.remote(gpus=1)` or `fn.options(gpus=1)` |
| CH120 | warning | The same code was wrapped as a new remote function 100 times (for example `ch.remote(lambda ...)` in a loop) | Define the remote function once, outside the loop |
| CH124 | warning | A CUDA tensor of 64 MiB or more was passed straight to calls from one line; each call copies it to host memory | `ch.put(t)` once and pass the future: copied once, and readers on its machine read it from GPU memory |
| CH126 | warning | Git ignores a whole directory of the project, so none of it ships to remote peers, and the script imported modules from it | Name what the workers need in `CHIMAERA_PROJECT_INCLUDE`, or stop ignoring it in `.gitignore` |

Seen enough of one code? Silence it by filtering on the start of its
message:

```python run
warnings.filterwarnings("ignore", message="CH101")
```

Next: when calls fail or peers die, [failures.md](failures.md) explains what
gets retried and what you'll see.
