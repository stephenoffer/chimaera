# Running Ray programs on Chimaera

`chimaera.compat.ray` runs Ray Core programs on Chimaera. Use it two ways:

```bash
python -m chimaera.compat.ray app.py --your-args     # an unmodified Ray program
```

or, in your own code, **runs today:**

```python run
import chimaera.compat.ray as ray

@ray.remote
def square(x):
    return x * x

assert ray.get([square.remote(i) for i in range(4)]) == [0, 1, 4, 9]
```

The `-m` form installs the shim as `ray` (including `ray.exceptions`,
`ray.util` and their submodules) before your script starts. Code imported by
the script sees the shim too.

The shim covers Ray Core only. Ray Data, Train, Tune, Serve and RLlib are not
shimmed. It uses only Chimaera's public API, which a test enforces. Futures
are Chimaera futures, so `await ref`, `ch.as_completed` and the rest of the
Chimaera API work on them too.

## How it says no

- **`Unsupported`** (a `NotImplementedError`): a feature that does not map.
  The message names the Chimaera alternative and the phase that adds it (see
  the phase table in [api.md](api.md)). Accessing an unknown `ray.<name>`
  raises `UnsupportedAttribute`, so `hasattr` still returns False.
- **`ChimaeraCompatWarning`**, once per option:
  - CHR001: ignored, because Chimaera handles it.
  - CHR002: accepted, but not enforced yet.
  - CHR003: works, with different semantics.

## The mapping

### Starting and stopping

| Ray | On Chimaera |
|---|---|
| `ray.init()`, `address="auto"` or `"local"` | Connects to the local peer, starting it if needed. Returns an object with `run_id`. |
| `ray.init(address=...)` | `Unsupported` until clusters: `ch.init(cluster=...)` (P3) |
| `ray.init("ray://...")` | `Unsupported`: there is no Ray Client protocol |
| `ray.init(namespace=...)` | The namespace for named actors |
| `ray.init(runtime_env={"env_vars": ...})` | `ch.init(env=...)`. Other `runtime_env` keys: `Unsupported` until `ch.Env` (P3) |
| `ray.init(num_cpus=..., object_store_memory=..., include_dashboard=..., log_to_driver=..., ...)` | CHR001: the local peer sizes itself from the machine |
| `ray.init(local_mode=True)` | `Unsupported`: local runs already take the cluster path |
| `ray.init` twice | `RuntimeError`, unless `ignore_reinit_error=True` (as in Ray) |
| `ray.shutdown()`, `ray.is_initialized()` | Forget or report `init`; the run itself finishes when the process exits |

### Tasks and objects

| Ray | On Chimaera |
|---|---|
| `@ray.remote`, `@ray.remote(**options)` | `@ch.remote`, with options translated below. Calling the function directly raises `TypeError`, as in Ray |
| `f.remote(...)` | Returns a `chimaera.Future`. Futures passed as arguments arrive as values, as in Ray |
| `f.options(**options)` | A configured copy |
| `ray.get(ref or list, timeout=)` | Waits for the values. Errors are translated (see Exceptions) |
| `ray.put(value)` | `ch.put`. The reference arrives as a value at the top level of an argument list and stays a reference when nested, as in Ray |
| `ray.wait(refs, num_returns=, timeout=)` | Same contract: at most `num_returns` ready, both lists in input order, unique refs required |
| `ray.cancel(ref, force=)` | The result becomes `TaskCancelledError` at once. Without `force`, a running body is asked to stop through `ch.context().cancelled()` and keeps running until it checks; Ray interrupts it. With `force`, the worker is killed. `recursive=False` is `Unsupported` |
| `isinstance(x, ray.ObjectRef)` | True for futures and `put` references |
| `ref.hex()`, `ref.binary()`, `ref.future()`, `ref.as_future()`, `await ref` | The same, for every kind of ref. An id is 36 bytes (`ObjectRef.size()`): the run, the call or `put`, and which of a call's `num_returns` values; a generator item's middle is random |
| `ref.task_id()`, `ref.job_id()`, `ray.TaskID`, `ray.JobID` | The call that made the value and its run, the ids `get_runtime_context()` gives inside that call. A driver's `put` and a generator item have the nil `TaskID` |
| `ray.ObjectRef(id)`, `ObjectRef.from_binary`, `from_hex`, `nil()`, `from_random()`, `ref.is_nil()` | The ref with that id while it is alive in this process and its id was read here, as Ray finds an id through its owner. Any other id, `nil()` and `from_random()` make a ref whose `ray.get` raises `ObjectLostError` (Ray's `from_hex` raises `NotImplementedError`) |
| `ref.call_site()` | `""`, Ray's answer unless `RAY_record_ref_creation_sites` is set |
| `ref.owner_address()` | Missing: a value has no owner process; it belongs to its run, and its copies are on other peers |
| `f.bind()`, `ray.dag` | `Unsupported` until `ch.graph` (P5) |

### Task options

| Ray option | On Chimaera |
|---|---|
| `num_cpus` | `cpus`. `0` becomes `0.01`: Chimaera always charges a share of a core, so about 100 such calls share one |
| `num_gpus` | `gpus`: whole devices, assigned and isolated (the call sees only its own). A fraction takes a whole device (CHR003) |
| `memory` | `memory` (bytes) |
| `resources` | `resources`. CHR002: recorded, not yet used for placement (P3) |
| `accelerator_type` | `device=`: NVIDIA names become `cuda:<model>`, `TPU-V4` becomes `tpu:v4` (`TPU-V5LITEPOD` is `tpu:v5e`), `AMD-Instinct-MI300A` becomes `rocm:MI300A`, `aws-neuron-core` becomes `neuron`, `Intel-GAUDI` becomes `hpu`, `Intel-GPU-Max-*` becomes `xpu`, Apple `M3-Pro` becomes `mps:M3 Pro`. Vendors without a Chimaera plugin (Ascend, MetaX, Furiosa, Mobilint) are `Unsupported`. As with `device=` alone, it asks for one device when `num_gpus` is unset |
| `max_retries` | `max_attempts = max_retries + 1`. Unset means 4 attempts, Ray's default. `-1` becomes 100 attempts (CHR003) |
| `retry_exceptions` | `retry_exceptions`. `True` means `(Exception,)`; `False` means `()`. Unset, a task retries a dropped connection or a timeout, where Ray retries no exception |
| `num_returns=n` | `returns=n`; `.remote()` returns a list of `n` refs, one per value, as in Ray |
| Generator tasks and methods, `num_returns="streaming"` | Supported: `.remote()` gives an `ObjectRefGenerator` with one resolved ref per item; an error raised mid-stream comes out as the next ref. Tasks stream as they yield; a generator method's items arrive once it returns |
| `num_returns="dynamic"` | `Unsupported`: use `num_returns="streaming"` |
| `scheduling_strategy="SPREAD"` | CHR002: not enforced until P3 |
| `NodeAffinitySchedulingStrategy(node_id, soft=False)` | `ch.dist.Placement(require={"node": node_id})`. A node id is a peer id, as `ray.nodes()` reports it. If no live peer has it, `ray.get` raises `ch.Infeasible` naming the labels on offer; Ray would leave the task pending |
| `NodeAffinitySchedulingStrategy(node_id, soft=True)` | `ch.dist.Placement(prefer=["node=<node_id>"])` |
| `label_selector={"k": "v"}`, `NodeLabelSchedulingStrategy(hard={"k": In("v")}, soft=...)` | `require` for exact values (hard), `prefer` for soft ones. Negations, sets and `Exists`/`DoesNotExist` (`"!v"`, `"in(a,b)"`, `NotIn`, `In` with several values) are `Unsupported`: Chimaera labels match exactly |
| `PlacementGroupSchedulingStrategy(pg, i)` | `within=` bundle `i` of the group's `ch.dist.reserve`; `-1` takes the bundles in turn. `placement_group_capture_child_tasks` is accepted with CHR002 (child tasks are not placed in the group) |
| `runtime_env` | `Unsupported` per call until `ch.Env` (P3) |
| `name`, `max_calls`, `enable_task_events`, `_metadata`, `_labels` | CHR001 |
| anything else | `ValueError: Invalid option keyword ...`, as in Ray |

### Actors

| Ray | On Chimaera |
|---|---|
| `Cls.remote(...)`, `handle.method.remote(...)` | An `ActorHandle` over a `chimaera.typing.Actor`. Calls through one handle run in submission order |
| `max_concurrency=n` | `concurrency=n` |
| An async actor with no `max_concurrency` | `concurrency=1000`, Ray's default, so an awaiting call doesn't block others. Native Chimaera async actors default to 1 |
| `name=`, `namespace=` | `key="<namespace>/<name>"`. Without a namespace, the name is scoped to this run, as Ray scopes it to the job |
| `get_if_exists=True` | Get or create |
| `get_if_exists=False` (Ray's default) | CHR003: still get-or-create, so an existing actor is returned instead of an error |
| `detach=True` | `detach=True` |
| `max_restarts=n` / `-1` | `restart="fresh"`, `max_restarts=n` (a very large number for `-1`) |
| `max_task_retries=n` | `max_attempts=n+1` on the actor's calls. Unset means 1, Ray's default |
| `ray.kill(actor)` | `actor.close()`. A call made after `kill` returns raises `ActorDiedError`; in Ray the kill is asynchronous and such a call can still succeed |
| `ray.kill(actor, no_restart=False)` | `Unsupported` |
| `concurrency_groups={...}` | `groups={...}`: each group runs beside the default group, with its own concurrency |
| `@ray.method(concurrency_group=, num_returns=, max_task_retries=, retry_exceptions=)` | `@ch.method(group=, returns=, max_attempts=, retry_exceptions=)` |
| `handle.method.options(num_returns=, max_task_retries=, retry_exceptions=)` | Method `.options()`; other per-call options are `Unsupported` |
| `max_pending_calls` | CHR002: not enforced; the runtime bounds submission itself |
| `allow_out_of_order_execution=True` | `Unsupported`: calls through one handle always run in order |
| `ray.get_actor(name, namespace=)` | `ch.actor(name)`. A missing actor raises `ValueError`, as in Ray |
| `ray.actor.exit_actor()` | `Unsupported`: an actor is ended by its owner with `ray.kill(handle)` |

### Utilities

| Ray | On Chimaera |
|---|---|
| `ray.util.ActorPool` | Supported: `map`, `map_unordered`, `submit`, `get_next`, `get_next_unordered`, `has_next`, `has_free`, `pop_idle`, `push` |
| `ray.util.queue.Queue` | Ray's blocking API (`put`, `get`, `*_nowait`, `*_nowait_batch`, `qsize`, `empty`, `full`, `shutdown`) over `ch.coord.Queue`. `actor_options=` is `Unsupported` |
| `ray.util.placement_group(...)` | `ch.dist.reserve`: `PACK`/`SPREAD`/`STRICT_PACK`/`STRICT_SPREAD`, bundles of `CPU`/`GPU`/`memory`/custom resources; `pg.ready()` (a ref for `ray.get`), `pg.wait(t)`, `remove_placement_group`, `get_placement_group(name)` (names are known in the process that made them). Detached lifetime, `bundle_label_selector`, `get_current_placement_group` and `placement_group_table` are `Unsupported` |
| `ray.util.scheduling_strategies` | The strategy classes import; see Task options for which are honored |
| `ray.util.multiprocessing.Pool` | The standard library's API over a pool of actors (one per process): `initializer`, `map`/`map_async`, `starmap`, `imap`/`imap_unordered`, `apply`/`apply_async` with callbacks, `close`/`terminate`/`join`. `maxtasksperchild` and `ray_address` are `Unsupported` |
| `ray.util.collective` | `ch.dist.group`. `create_collective_group` (driver) and `init_collective_group` (in the actors); `allreduce`, `reduce`, `broadcast`, `allgather` and `barrier` work in place on torch tensors (through `torch.distributed`, one torch-backed group per actor process) or NumPy arrays (through the store collectives, the object plane past 64 KiB); a gloo group's `gloo_timeout` bounds each collective's wait (`collective_timeout`); `send`, `recv`, `reducescatter` and the `*_multigpu` calls raise `Unsupported` |
| `ray.util.state`, `ray.util.metrics` | `Unsupported`, naming `ch.runs.cluster()` and `context().report()` |
| `ray.get_runtime_context()` | `get_job_id()` (the run id), `get_task_id()`, `get_actor_id()`, `get_node_id()` (the peer id), `get_accelerator_ids()` (`{"GPU": [...], "TPU": [...], ...}` from `ch.dist.devices()`), `namespace`. Others are `Unsupported`, naming `ch.context()` |
| `ray.get_gpu_ids()` | The call's GPUs (CUDA, ROCm, Intel, Apple) from `ch.dist.devices()`, numbered as the call sees them, so `cuda:N` works directly |
| `ray.nodes()` | One record per peer from `ch.runs.cluster()`: `NodeID`, `Alive`, `NodeManagerAddress`, `NodeManagerHostname`, `NodeManagerPort`, `Resources` (`CPU`, `memory`, `GPU`, `node:<ip>`), `Labels` (the peer's labels plus `ray.io/node_id`) |
| `ray.cluster_resources()`, `ray.available_resources()` | Sums over live peers. Available CPU is capacity minus each peer's reported use; memory and GPUs are reported as capacity, since nothing reserves them ahead of a call yet |
| `ray.timeline()` | `Unsupported`: use `chimaera explain RUN` |

### Exceptions

`ray.get` translates errors as follows. Your own exception arrives as
`RayTaskError(<type>)`, a class deriving from both, so `except ValueError` and
`except ray.exceptions.RayTaskError` both catch it, as in Ray. The remote
traceback is attached as a note.

| Chimaera | Ray |
|---|---|
| your exception | `RayTaskError(<type>)` |
| `ch.Cancelled`, `ch.DeadlineExceeded` | `TaskCancelledError` |
| `ch.ActorDied` | `ActorDiedError` (a `RayActorError`) |
| `ch.OutOfMemory` | `OutOfMemoryError` |
| `ch.errors.DataLost` | `ObjectLostError`. Ray's kinds of it (`OwnerDiedError`, `ObjectReconstructionFailedError`, `ObjectFreedError`, `ObjectFetchTimedOutError`, `ReferenceCountingAssertionError`) are there to import and catch, and so are `ObjectStoreFullError` and `OutOfDiskError`, but the shim raises none of them: a lost value is `ObjectLostError`, and a value whose owner died is read from its replica |
| `ch.InfrastructureFailure`, `ch.errors.WorkerCrashed` | `WorkerCrashedError` |
| `ch.errors.Overloaded` | `PendingCallsLimitExceeded` |
| a `ray.get` timeout | `GetTimeoutError`, which is also a `TimeoutError` |
| `ch.Infeasible`, `ch.OutcomeUnknown` | Kept as they are. Ray has no equivalent: it waits forever for an infeasible request |

`await ref` raises Chimaera's own errors, untranslated.

## The corpus

`python/tests/compat_corpus/ex_*.py` are complete Ray programs: tasks,
`put`, `wait`, actors, named actors, kill, task errors, cancel and timeout,
async and threaded actors, `ActorPool`, nested tasks, worker-crash retries,
MapReduce, options, and `num_returns`, concurrency groups, `get_actor` and
`ray.util.queue`. Each program's `.expected` file is what real Ray 2.60
printed, recorded with:

```bash
python python/tests/compat_corpus/record.py      # uses .venv-bench, which has Ray 2.60
```

`test_compat_ray.py` runs every program under `python -m chimaera.compat.ray`
and requires identical output. Ray's forwarded `(raylet)` log lines are
dropped when recording. They are runtime chatter, not program output.
