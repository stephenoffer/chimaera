# Ray Core gotchas and what Chimaera does about them

These are problems Ray Core users commonly hit, and what Chimaera does about
each by default, with no option to set. "Handled" means a test or a measurement
in [qualification.md](qualification.md) shows it; "partly" says what is missing;
"open" means Chimaera does not avoid it yet.

## Memory and out-of-memory kills

| Ray problem | Chimaera | Status |
|---|---|---|
| Workers OOM-killed by the memory monitor, or by the kernel before the monitor acts; nodes over the threshold | Each function's private memory peak is learned; calls reserve it and wait for memory instead of overcommitting. Under host pressure the call furthest over its reservation is stopped (never the most recent), and its retry needs 1.5× the peak it reached | Handled: 12 of 12 memory-heavy calls with no kernel OOM kill where Ray finished 3 with 43 (3 GiB container) |
| The memory monitor reads host memory when a cgroup limit is unset, above host memory, or set on a parent; containers sized from host memory | The pool and CPU capacity take the tightest limit from the process's cgroup and every cgroup above it | Handled (unit test of the cgroup chain; container runs) |
| Leaky workers, recycled by hand with `max_calls` | A worker whose memory between calls keeps rising is replaced, with a recorded decision | Handled |
| High RSS after objects are freed (allocator retention) | On Linux, a worker grown by 256 MiB hands freed memory back (`malloc_trim`) before the leak check reads it | Handled (fails with the trim disabled) |
| `memory=` and `num_cpus=` guessed by hand | Learned per function after a few calls; explicit values still win; `chimaera profiles` lists what was learned | Handled |
| Daemons on long-running clusters slowly grow | Not measured: no multi-day run | Open (unverified) |
| Head node OOM when the driver builds and fetches a whole per-element DAG | No head node; `.remote()` waits for room once unfinished calls hold half the pool in arguments (CH110), and maps stream with a bounded window | Partly: bounds submission, not what the driver itself holds |

## Objects and large values

| Ray problem | Chimaera | Status |
|---|---|---|
| The same large argument re-serialized on every call; large objects captured in closures | Arguments of 1 MiB or more are stored once by content; large captured values are warned about at submission | Handled: a 32 MiB array passed to 128 calls on 4 machines took 2.23 s against Ray's 3.07 s |
| Object store full or sized by hand; tasks waiting for plasma memory; the 200 GB cap; a small `/dev/shm` | One memory pool per peer instead of a fixed store; objects past 80% of it move to disk and stay readable; `/dev/shm` is used only when it has room | Handled (spill tested end to end on Linux) |
| Spill fills the disk; head-node disk saturation | Below 512 MiB free on the filesystem holding objects, new calls wait with a reason naming it (a fixed amount, so a large disk that is merely mostly full keeps working); a run's stored arguments are deleted when it ends; a full disk is named in the error with its free space | Handled (unit test; checked on a 700 MiB tmpfs) |
| Lost objects that cannot be rebuilt; `ray.get` hanging when the owner dies | Objects are replicated to a second peer; a value no peer holds raises `DataLost` instead of hanging | Partly: on Linux the replica copy currently races the first reader's fetch, so a peer can fetch an object twice (being fixed) |
| Accumulating every result before consuming any | Maps keep a bounded number of items in flight; awaiting results one at a time in a loop warns (CH101) | Handled |
| Medium results (100–500 KB) of short tasks | A result is stored on the peer that produced it (in memory on Linux) and read there by the next call, without a copy through the driver | Handled (no setting to tune) |
| Passing CUDA tensors between tasks | CPU tensors travel zero-copy; CUDA tensors fall back to torch's own pickling, as in Ray, with no warning | Open |
| A task receiving a list of unready references blocks and grows | Futures passed directly, or one level inside a list, tuple or dict argument, are dependencies: the call starts when they are ready and receives their values. `ch.handles(x)` passes them as futures instead | Handled (`test_api.py`) |

## Scheduling and resources

| Ray problem | Chimaera | Status |
|---|---|---|
| Work pending forever because actors or placement groups hold the resources | Actors hold no CPU by default; a waiting call says what it waits for, including CPU held by actors and their `cpus=` setting; a stalled wait prints the reason after a minute | Handled |
| Infeasible requests only warned about | A request no peer can satisfy fails at once with `ch.Infeasible` | Handled |
| Library thread pools oversubscribe cores; thread counts explode on large machines | OpenMP, BLAS, Polars, Numba and tokenizer pools are sized to the call's CPU grant | Handled |
| Tiny tasks cost more than they compute; lease churn saturates the raylet | Fast calls are batched automatically, and maps chunk their items, each item learned on its own | Handled: on 4 machines, maps of tiny items ran 194k items/s against 7k for Ray without hand batching |
| Slow task start while a fresh worker spawns | Executors start in parallel ahead of queued work | Handled |
| Nested `ray.get` starves workers | A call waiting on another gives back its CPU while it waits | Handled |
| Stragglers from cost skew in sweeps | No speculative re-execution of slow calls | Open |
| Spread work for resilience; target node types by labels; scheduler tuning for large clusters | Placement weighs load, input locality and memory; labels with `Placement(require=, prefer=)`; there are no scheduler knobs to tune | Handled for labels and locality |
| Gang-scheduling collective workers | `ch.reserve` bundles and gangs | Covered by the gang and reservation work (not part of this plan) |

## Workers, actors and processes

| Ray problem | Chimaera | Status |
|---|---|---|
| Too many open files | Each peer raises its open-file limit to the hard limit at start | Handled |
| Children of a killed worker survive | Each executor is its own process group, and stopping one stops the group | Handled (a child that calls `setsid` escapes) |
| Workers cache every dynamically defined function | The executor's code cache is bounded (256 entries) | Handled |
| Module-level CUDA allocation in every worker | Executors without a GPU grant see no GPU | Handled |
| Fork-based multiprocessing inside a task hangs | Executors keep Python's default start method: forkserver from Python 3.14, fork before it on Linux | Open: forkserver by default would also break pools of functions defined in the driver's script |
| Exceptions that cannot be unpickled; objects that cannot be pickled | An exception that cannot cross keeps its type name, message and traceback; captured values are checked before submission | Handled |
| Actors dying, failing in `__init__`, or dying with their owner; restart failing when a creation argument was evicted; detached actors leaking | Creation arguments are kept until the run ends; actors end with their run unless detached | Handled for argument eviction; actor lifecycle is the actor API's |
| Workers crashing | Calls lost to a dead worker are retried (three attempts); an external effect reports `OutcomeUnknown` | Handled |
| Retrying transient application errors | `retry_exceptions` lists the exceptions to retry; your own exceptions are never retried otherwise | Handled (same choice as Ray) |

## Setup, versions and configuration

| Ray problem | Chimaera | Status |
|---|---|---|
| Driver and cluster on different versions | A program that would start a different peer binary than the one serving it warns once (CH111); the wire version is checked on connect | Partly: local peers only |
| Port conflicts | A local program reaches its peer through a Unix socket, so no port is involved | Doesn't apply locally |
| Runtime environments: package size, setup timeouts, evicted packages, oversized inline settings, credentials | Project shipping and environments are the layered API's (`_project.py`) | Not part of this plan |
| Worker logging goes nowhere | Task log records reach the driver at the driver's level | Handled |
| System settings set in the wrong place | Automatic settings are derived at start and printed by `chimaera status` | Handled |
| `ray.get` timeouts | A wait that times out leaves the call running; a stalled wait says why after a minute | Handled |
| Ray-specific infrastructure: protobuf conflicts, dashboard agents, Ray Client, worker registration, native ABI crashes, State API imports, compiled graphs, constraints files, connectivity preflight | These concern Ray's own components, or GPU and pool features other parts of the plan own | Doesn't apply, or not part of this plan |

## What is still open

- Speculative re-execution of stragglers.
- A warning when CUDA tensors are passed between calls.
- Fork-based multiprocessing inside tasks keeps Python's default start method.
- On Linux, a replica copy that races the first reader's fetch of the same object.
