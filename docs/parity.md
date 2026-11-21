# Feature, performance and scale parity

**21 November 2026.** Everything Ray Core and the other distributed-Python runtimes offer, and how Chimaera provides
each item: today, in a planned phase, through a decision in [architecture.md](architecture.md), or deliberately not
at all. The competitor review behind the non-Ray rows is in [landscape.md](landscape.md).

Ray rows come from the installed Ray 2.60.0 source (`ray/_common/ray_option_utils.py`, `ray/_private/worker.py`,
`ray/util/`, `ray/runtime_env/`, `ray/scripts/scripts.py`), not from memory. Chimaera rows come from
[api.md](api.md), [compat-ray.md](compat-ray.md) and [qualification.md](qualification.md).

**Status words**

- **Today**: works now; api.md shows it in a block that the test suite runs, or qualification.md records it.
- **P3…P6, M2…M6**: planned in that phase of the roadmap (phases in api.md, milestones in the plan).
- **New (Dn, phase)**: not in any plan before this review; added by decision Dn of architecture.md, or by this
  document, in the phase named.
- **Won't**: left out on purpose, with the reason.

Each "New" item is an option keyword, a method on an object the user already holds, or a name in a submodule. The
API budget in architecture.md §13 is what keeps this list from growing the first-hour API.

## 1. Ray Core, item by item

### Starting, connecting, running

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `ray.init()` starts or finds a local cluster | `ch.init()`, or nothing: the first `.remote()` starts a local peer (130–180 ms measured, 37 MB idle) | Today |
| `ray.init(address=...)`, `"auto"` | `ch.init(cluster="profile" \| "chimaera://host:port" \| "auto")` | P3 |
| Ray Client (`ray://`), deprecated in 2.60 | Remote drivers connect over TCP to a peer started with `--client-listen` (today); connecting through any peer with a versioned protocol and no version lock-step is P3 | Today / P3 |
| `namespace=` | Key prefix `"ns/name"` | Today |
| `num_cpus`, `num_gpus`, `resources`, `labels` on `init` (caps for tests and CI) | `ch.init(cpus=, memory=, gpus=)` caps the local peer; cluster peers advertise what they were granted | New (P3) |
| `logging_config=LoggingConfig(encoding="JSON", log_level=...)` | `ch.init(log_format="json", log_level=...)`, applied to every executor | New (P4) |
| `log_to_driver`, worker stdout forwarded with a prefix and de-duplicated | Task output reaches the driver at its log level (Today); de-duplication of repeated lines | Today / New (P4) |
| `JobConfig(metadata=...)` | `ch.init(metadata={...})` on the run record | New (P3) |
| `enable_resource_isolation`, `system_reserved_*` | The agent always runs in its own reserved cgroup when delegation exists (requirement P6); attempts get their own when `accounting="isolated"` | Today (attempts) / M2 (agent) |
| `ray.shutdown()`, `is_initialized()` | Shim; a run ends when its process exits unless detached | Today |
| `local_mode` (removed in 2.60) | Local runs use the cluster path already | Today |

### Tasks and futures

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `@ray.remote`, `.remote()`, `.options()` | `@ch.remote`, `.remote()`, `.options()`; a plain call runs locally | Today |
| `ray.get(refs, timeout=)` | `future.result(timeout=)`, `ch.gather(*fs)`, `await future` | Today |
| `ray.wait(refs, num_returns, timeout, fetch_local)` | `ch.as_completed(fs)`; readiness never moves data, so `fetch_local=False` is the only behaviour | Today |
| `ObjectRef.future()`, `_on_completed(cb)` | `Future` implements the `concurrent.futures.Future` protocol: `add_done_callback`, `exception`, `done`, `running` | New (P3) |
| `num_returns=n` | `returns=n`, `.split()` | Today |
| Generator tasks (`num_returns="streaming"`), `_generator_backpressure_num_objects` | Generator functions return `Results`, bounded ahead of the consumer (`max_in_flight=`), replayed after a worker dies | Today |
| `num_returns="dynamic"` (deprecated) | Generators | Today |
| Nested tasks | Calls inside calls; a returned future is forwarded without holding a worker | Today |
| `ray.cancel(ref, force=, recursive=True)` | `future.cancel(force=)` cancels the subtree; `cancel(children=False)` keeps children | Today / New (P3) |
| A running task gets `KeyboardInterrupt` on cancel | A running call gets `ch.Cancelled` raised at its next bytecode boundary after a 2 s cooperative grace; `force=True` kills the process | New (P3) |
| `max_retries` (default 3), `retry_exceptions` | `max_attempts`, `retry_exceptions`, within the effects contract (`external` calls never repeat) | Today |
| `max_calls` (recycle the worker after N calls; 1 for GPU tasks) | `max_calls=` option; leaking workers are also rotated by measurement (Today) | New (P3) |
| `name=` and `_labels` on tasks | `name=` and `tags=` options, shown in `explain`, `top`, the dashboard and metrics | New (P3) |
| No per-call timeout (requested since 2021) | `deadline=`, inherited by children | Today |
| No priorities | `priority=` | Today |
| Infeasible requests wait forever | `ch.Infeasible` at submission, naming the constraint | Today |

### Actors

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `Cls.remote()`, `handle.method.remote()`, per-handle order | Same | Today |
| `max_concurrency`, async actors, threaded actors | `concurrency=`; `async def` never changes semantics on its own | Today |
| `concurrency_groups`, `@ray.method(concurrency_group=)` | `groups=`, `@ch.method(group=)` | Today |
| `allow_out_of_order_execution` (default on for threaded and async actors) | With `concurrency > 1`, calls start as their inputs become ready; with `concurrency=1`, in submission order | New (P3) |
| `name`, `namespace`, `get_if_exists=True`, `ray.get_actor` | `key=` (get or create), `ch.actor(key)` | Today |
| `get_if_exists=False` (create only; `ActorAlreadyExistsError`) | `if_exists="error"` with `key=`; raises `ch.AlreadyExists` | New (P3) |
| `ray.util.list_named_actors(all_namespaces)` | `ch.cluster().actors(prefix=...)` | New (P3) |
| `lifetime="detached"` | `lifetime="run"` (default for keyed actors) or `"detached"` | Today |
| Non-detached actors die with their owner or when no handle is left | Unkeyed actors end when their last handle is dropped or their creating call's scope ends (structured lifetimes, D4) | New (D4, P3) |
| `max_restarts`, `max_task_retries` | `restart="fresh"`, `max_restarts`, `max_attempts`; `restart="checkpoint"` for durable state | Today |
| `ActorUnavailableError`; `was_current_actor_reconstructed` | `ch.ActorRestarting` (transient, retryable) vs `ch.ActorDied`; `ch.context().restarted` | New (P3) |
| `max_pending_calls` → `PendingCallsLimitExceeded` | `max_pending_calls=` enforced per handle, raising `ch.Overloaded(retry_after)` | New (P3) |
| `ray.kill(actor)`, `no_restart=False` | `handle.close()`; `handle.restart()` forces a restart under the actor's policy | Today / New (P3) |
| `__ray_terminate__` (kill after pending calls) | `handle.close(drain=True)` | New (P3) |
| `ray.actor.exit_actor()` | `ch.context().exit()` from inside a method: queued calls fail with `ch.ActorDied`, `__exit__` runs | New (P3) |
| `__ray_call__(fn)` (run a function on the instance) | `chimaera debug` attaches to the actor's process instead; no API name spent on it | Won't |
| Actor handles passed to other calls | Handles are values; pass them anywhere | Today |
| Generator methods | Same as generator tasks | Today |
| One process per actor only | Process, shared (`process="shared"`) and virtual (`idle=`) actors | New (D4, P2–M3) |
| No lifecycle hooks (`__del__` is unreliable) | `__enter__` after construction, `__exit__` on close, drain, preemption or passivation | New (D4, P2) |
| `ActorPool` | `Class.options(replicas=...).pool(...)` with routing, resize, rolling upgrade, autoscaling; `ActorPool` in the shim | Today |

### Objects

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `ray.put` | `ch.put` returns a finished `Future`; content-addressed, stored once | Today |
| Owner death → `OwnerDiedError`; `put` objects cannot be rebuilt | Owners are agents with successors (D3); `put` values get one replica under budget (D5) | New (D2–D5, M2–M3) |
| Lineage reconstruction (`max_lineage_bytes`) | Rebuild lost values by replaying replayable producers, cut points chosen by cost (D5, §10) | M2 (plan item 6) / New (D5) |
| Reference counting and borrowing | A value lives while a future, a pickled future or a pending dependent refers to it, and at most until its run ends (design.md amendment 14) | Today (needs api.md text) |
| Nested refs stay refs | Same | Today |
| Zero-copy numpy (read-only arrays) | Copy-on-write mappings: arrays arrive writable, writes stay in the call | Today |
| Spilling to disk / S3 | Disk: Today (`stores.rs`); object-store URI target: M3 | Today / M3 |
| `register_serializer` / `deregister_serializer` | `ext.register_serializer` / `ext.unregister_serializer` | P4 |
| `inspect_serializability` | Submission preflight names the unpicklable variable and its path | Today (CH108 for oversized closures) / P3 (full path) |
| `ray memory`, `get_object_locations` | `chimaera memory RUN` and `ch.cluster().objects(run=)`: owner, holders, size, who pins it | New (P4) |
| `ObjectStoreFullError`, `OutOfDiskError` | `ch.DiskFull` with the budget and the largest holders; objects spill before that | M3 |
| `put(_tensor_transport="nixl")` | `ch.put(tensor, transport="device")` | P5 |
| Ray Direct Transport (alpha), `tensor_transport=` on methods | `transport="device"` on any call or method; transport negotiated per pair (D5) | P5 |

### Scheduling and placement

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `num_cpus`, `memory`, `num_gpus`, `resources` | `cpus`, `memory`, `gpus`, `resources`; omitted values are learned from earlier calls | Today (cpus, memory) / P3 (resources) / P5 (gpus assigned) |
| Fractional `num_gpus` | `gpus=0.5` after a measured peak, capped at 0.8 of the fraction; MIG and MPS partitions as devices | P5 |
| `accelerator_type`, 15 accelerator families | `device="kind:model"` for cuda, rocm, tpu, neuron, hpu, xpu, mps; `ext.DevicePlugin` for the rest | P5 |
| `"DEFAULT"` (pack, prefer nodes holding large arguments) | Placement ladder with input locality (D6) | Today (locality on localhost clusters) / M2 (ladder) |
| `"SPREAD"`, `NodeAffinitySchedulingStrategy`, `NodeLabelSchedulingStrategy`, `label_selector`, `fallback_strategy` | `placement=ch.Placement(require=, prefer=, spread=, near=)` | P3 |
| Placement groups: `PACK`, `SPREAD`, `STRICT_*`, `bundle_label_selector`, `topology_strategy` | `ch.reserve(shapes, placement=Placement(pack="nvlink" \| "host" \| "rack", spread=...), key=, lifetime=)`; strict = `require`, soft = `prefer` | P5 |
| `get_placement_group(name)`, `placement_group_table`, `get_current_placement_group`, `capture_child_tasks` | `ch.reservation(key)`, `ch.cluster().reservations()`, `ch.context().reservation`; calls made inside a call placed in a reservation stay in it unless they say otherwise | New (P5) |
| `ray.util.tpu` slice helpers | `device="tpu:v5p"` gangs with slice-shaped reservations; JAX coordinator variables set | P5 |
| `autoscaler.sdk.request_resources` | `ch.cluster().request(shapes, until=)` | New (P6) |
| `experimental.set_resource` | `chimaera peer label/resource set` on a live peer | New (P3) |
| No fairness between jobs; no quotas | Weighted DRF across tenants and runs; delegated quota envelopes (§6) | P6 / New (D6) |

### Environments and deployment

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `runtime_env={"env_vars"}` | `ch.init(env=)` | Today |
| `working_dir`, `py_modules`, `excludes` | `ch.Env(files=[...], exclude=[...])`, content-addressed, size-capped by budget not by a fixed 500 MiB | P3 |
| `pip`, `uv`, `conda`, `image_uri`, `py_executable` | `ch.Env(pip=, uv_lock=, conda=, image=, python=)`; a `uv run` driver's lock is used automatically | P3 |
| `worker_process_setup_hook` | `ch.Env(setup=callable)` | New (P3) |
| `RuntimeEnvConfig(setup_timeout_seconds, eager_install)` | Environments are built before admission (never on a call's critical path); `ch.Env(build_timeout=)` | P3 |
| Runtime-env plugins, nsight/rocprof wrappers | `ext.EnvPlugin`; `chimaera profile --nsight` | New (P4) |
| `ray start`, `ray up`/`down`, KubeRay, Slurm community scripts | `chimaera peer start --seeds`, providers for SSH, Kubernetes (StatefulSet, no operator), Slurm, SkyPilot (deploy.md) | Today (SSH, k8s, Slurm launchers in tree) / P3 |
| `ray attach/exec/rsync`, `get-head-ip` | `chimaera exec/attach/sync --cluster`; there is no head | New (P3) |
| `JobSubmissionClient` and REST API, entrypoint resources, list and delete | `chimaera submit/logs/inspect/cancel`, a Python `ch.jobs` client and the same over HTTP on any peer | P3 / New (P3) |
| `experimental.sandbox` (gVisor actors for agentic code) | `@ch.remote(sandbox=ch.Sandbox(image=, network=False, ttl=))` on tenant sandboxes | New (P6) |
| Java and C++ calls | Python only; the wire protocol is protobuf, so other clients are possible later | Won't |

### Coordination

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `ray.util.queue.Queue` | `ch.Queue`; durable with `key=, durable=True` and `claim()` | Today |
| Internal KV (private) | `ch.Store`: linearizable, compare-and-set, watches, TTLs, counters | Today |
| No locks, barriers or leader election | `store.lock(ttl)` with fencing tokens, `store.barrier`, `store.counter` | Today |
| No pub/sub | `ch.Topic` | Today |

### GPUs, collectives, compiled graphs

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `ray.get_gpu_ids()`, `get_accelerator_ids()` | `ch.devices()`, `ch.device()` | P5 |
| `ray.util.collective` (NCCL, GLOO; allreduce, allgather, broadcast, reduce, reducescatter, send/recv, barrier, multi-GPU variants, custom backends) | `ch.context().gang` and `ch.group(*actors)` with the same operations; `reduce`, multi-GPU variants and `ext.CollectiveBackend` | P5 / New (P5) |
| Placement-group SPMD by hand | `.gang()` with rank variables for PyTorch, JAX and DeepSpeed; elastic `ranks=ch.auto(...)` | P5 |
| `ray.dag`, Compiled Graphs (`experimental_compile`, NCCL channels) | `@ch.graph`, `.compile(max_in_flight=)` | P5 |
| `visualize()` | `graph.explain()` renders the compiled plan | New (P5) |
| `register_tensor_transport`, NIXL memory registration | `ext.Transport`; NIXL and UCX registration are admission resources | New (P5) |

### Observability and debugging

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| Task events and the State API (tasks) | `future.explain()`, `run.tasks()`, `chimaera inspect/explain`, with attempts, physical measurements and the waiting reason | Today |
| State API for actors, objects, workers, summaries | `ch.cluster().actors()/objects()/workers()`, `chimaera list actors\|objects\|workers` | New (P3–P4) |
| `ray.nodes()`, `cluster_resources()`, `available_resources()` | `ch.cluster().nodes()`, `.resources()`; available vs total | Today / New (P3, available) |
| Dashboard (jobs, nodes, actors, tasks, logs, Grafana) | Served by any peer from fan-out queries; no head to host it (D8) | New (P4) |
| Prometheus metrics, Grafana dashboards | Prometheus and OTLP export with bounded cardinality | P4 |
| `ray.util.metrics` (Counter, Gauge, Histogram) | `ch.metrics.counter/gauge/histogram` | New (P4) |
| `ray.timeline()` (Chrome trace) | `chimaera timeline RUN` writes a Perfetto/Chrome trace | New (P4) |
| `ray stack`, py-spy flame graphs, memray, GPU profilers | `chimaera stack CALL` (remote unwinding, PEP 768 on 3.14+), `chimaera profile CALL --cpu/--memory/--gpu` | New (P4) |
| `ray debug`, `breakpoint()` in tasks, post-mortem | `breakpoint()` in a remote call pauses it for `chimaera debug`; attach to a running call; `debug="post-mortem"` option | New (P4) |
| OpenTelemetry tracing hook | Trace context propagated through every call | New (P4) |
| `ray memory` | `chimaera memory` | New (P4) |
| No per-task physical CPU or memory | Per-attempt CPU seconds, mean cores, memory peak, OOM cause, GPU memory and utilization where the hardware allows (D8) | Today (CPU, memory) / P5 (GPU) |
| `tqdm_ray` | Progress of `map` and `Results` in the driver (terminal and notebook); `tqdm` inside calls forwarded | New (P4) |
| `ray.widgets` | `_repr_html_` for `Future`, `Run`, `Results` in notebooks | New (P4) |

### Interoperability

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `ray.util.multiprocessing.Pool` | `chimaera.multiprocessing.Pool` with the same methods | New (P3) |
| `ray.util.joblib` backend | `chimaera.joblib` backend (`parallel_backend("chimaera")`) | New (P3) |
| Dask on Ray | `dask.compute(..., scheduler=chimaera.dask.get)` | New (P4) |
| Spark on Ray | Not a core concern | Won't |
| `concurrent.futures` (not offered by Ray) | `ch.executor()` is a `concurrent.futures.Executor` | New (P3) |
| Ray programs unchanged | `python -m chimaera.compat.ray app.py` | Today |

### CLI

| Ray 2.60 | Chimaera | Status |
|---|---|---|
| `start`, `stop`, `status` | `chimaera peer start/stop`, `chimaera status` (says why work waits) | Today |
| `list`, `get`, `summary` (tasks) | `chimaera inspect`, `explain` | Today |
| `memory`, `timeline`, `stack`, `debug`, `kill-actor`, `drain-node`, `health-check`, `check-open-ports`, `cluster-dump`, `microbenchmark`, `dashboard` | `chimaera memory/timeline/stack/debug/kill/drain/doctor/dump/bench/dashboard` | `doctor` Today; rest New (P3–P4) |
| `job submit/stop/status/logs/list/delete` | `chimaera submit/cancel/inspect/logs/runs` | P3 |

## 2. Beyond Ray: what other runtimes do better

Features Ray Core lacks or does worse, from the review in [landscape.md](landscape.md). Only items that belong in a
core runtime are listed; library features (Modal Volumes, Temporal's search attributes, Monarch's tensor engine)
are out of scope.

### Compute and futures

| Feature | From | Chimaera | Status |
|---|---|---|---|
| `as_completed(...).add(f)` for dynamic frontiers (crawlers, search, rollouts) | Dask | `ch.as_completed(fs)` returns an iterator with `.add(future)` | New (P3) |
| A waiting task gives back its slot (`secede`/`rejoin`) | Dask | Automatic: a call blocked on other calls releases its CPU and keeps its memory charge | Today |
| Fire and forget | Dask | Dropping a future releases its result; the call still runs | Today |
| Retrieve a call by id from another process, later | Modal (`FunctionCall.from_id`, 7 days), Temporal handles | `run.future(call_id)`; retention follows the run's lifetime | New (P3) |
| `concurrent.futures`-compatible executor with callbacks | Flux, mpi4py | `ch.executor()`; `Future.add_done_callback` | New (P3) |
| Sync and async from one API | Modal `.aio`, Monarch | Every `Future` is awaitable; `.remote()` never blocks | Today |
| Retries with backoff and jitter | Celery, Modal | Retries of listed exceptions and actor restarts back off exponentially with jitter by default | New (P3) |
| Ordered `map` with `return_exceptions` | Modal | `fn.map(items, errors="return")` yields exceptions in place | New (P3) |
| Per-task timeouts | Modal, Temporal | `deadline=` | Today |

### Actors

| Feature | From | Chimaera | Status |
|---|---|---|---|
| Virtual actors: get-or-activate by key, idle passivation, reactivation anywhere | Orleans, Akka, Dapr, Restate | `key=` with `idle=`; single activation guaranteed by a fenced generation | New (D4, M3) |
| Many cheap actors per process | Orleans, Akka, Erlang | `process="shared"` | New (D4, P2–M3) |
| Actor meshes: slicing, call all and gather, call one, stream replies | Monarch | `.gang()` returns a mesh; `mesh.method.remote()` returns `Results`; `mesh[i]`, `mesh.slice(dp=0)`, `.reduce(op)` | New (D4, P5) |
| Load-balanced routing and sticky sessions over replicas | torchforge, Modal `@sessioned` | `pool.method.remote()` goes to the least busy replica; `pool.route(key)` | Today |
| Lifecycle hooks (`@enter`, `@exit`, `__cleanup__`) | Modal, Monarch | `__enter__` / `__exit__` on the class | New (D4, P2) |
| Supervision trees, fail-fast by default | Monarch, Erlang | Child lifetimes follow the creating scope; `actor.watch()`; errors propagate | New (D4, P3) |
| Restart intensity and backoff | Erlang/OTP, Akka | `max_restarts=(count, window)`; exponential backoff with jitter | New (P3) |
| Links and monitors | Erlang/OTP | `actor.watch()` yields `ActorDied` and restart events | New (P3) |
| Reentrancy control | Orleans | Call cycles are detected and reported as `ch.Deadlock`; `concurrency>1` or groups allow overlap | New (P3) |
| Durable reminders | Orleans, Dapr | `start_after=` on an actor method call; durable when longer than the journal window | New (P3) |
| Live migration and rebalancing | Orleans (experimental) | Shared actors move between executors (actor-packing controller); durable actors restart elsewhere from state | New (M3) |
| Per-replica concurrency and dynamic batching | Modal `@concurrent`, `@batched` | `concurrency=`, `@ch.method(batch=ch.Batch(...))` | Today |

### Data and objects

| Feature | From | Chimaera | Status |
|---|---|---|---|
| `scatter(broadcast=True)` | Dask | `ch.put` plus pipelined broadcast trees for popular values | Today (put) / New (D5, M3) |
| Spill, pause and terminate thresholds per worker | Dask | One memory pool per peer: spill at 80%, pause admission under pressure, stop the call furthest over its reservation | Today |
| Graceful retirement that migrates data | Dask Active Memory Manager | `chimaera drain PEER` moves objects, journals and durable actors before the peer leaves | New (M3) |
| One-sided RDMA buffers | Monarch, NIXL, `torch.distributed._transport` | `transport="device"`: handle on the control path, bytes moved one-sided | P5 |
| Peer-to-peer shuffle | Dask | `reduce_by_key` (Today on one peer); disk shuffle across peers | Today / M3 |
| External payload storage | Temporal | Spill target URI for values and checkpoints | M3 |
| Code and environment sync to every host | Monarch (RDMA file sync), Modal images | Content-addressed `ch.Env(files=...)`, one copy per peer | P3 |

### Coordination

| Feature | From | Chimaera | Status |
|---|---|---|---|
| Named persistent Dict and Queue | Modal, Dask | `ch.Store` namespaces; `ch.Queue(key=, durable=True)` | Today |
| Lock, barrier, counter | Dask | `store.lock`, `store.barrier`, `store.counter` | Today |
| Semaphore (lease-based) and Event | Dask | `store.semaphore(key, n, ttl=)`; an event is a key plus `store.watch` | New (P3) |
| Global rate limits and per-key concurrency | Dramatiq, Hatchet, DBOS, Inngest | `limit=ch.Limit(key, rate=, concurrency=)`, enforced by delegated token shares | New (D6, P4) |
| Pub/sub | Orleans streams (Dask removed its own) | `ch.Topic` | Today |
| CRDT replicated maps | Akka Distributed Data | Not offered: `ch.Store` is linearizable and sharded by namespace | Won't |

### Durability

| Feature | From | Chimaera | Status |
|---|---|---|---|
| Workflows replayed from history after any crash | Temporal, DBOS, Restate | `workflow="durable"` | Today |
| Workflow id as idempotency key | DBOS, Temporal | `key=` on a call: a second submission with the same key returns the first call's future | New (P3) |
| Durable sleep for days | Temporal, DBOS | `await ch.workflow.sleep("2d")` inside a durable workflow | New (P3) |
| Signals, queries, updates | Temporal | Signals through a durable `ch.Queue`, state published to `ch.Store`; no separate signal API | Today (building blocks) |
| Patch markers for in-flight code | Temporal | `ch.workflow.patched("change-id")` | New (P3) |
| History compaction | Temporal (`continue_as_new`, 51,200-event cap) | Workflow history snapshots automatically; no cap to manage | New (M3) |
| Fork, rewind and resume an execution | DBOS | `chimaera replay RUN --from CALL` | New (P6) |
| Exactly-once state changes co-committed with the checkpoint | DBOS transactions, Restate virtual objects | Durable actors: state, reply and outbox commit together | Today |
| HA cron schedules | Celery Beat (no HA), Modal, Temporal | `fn.options(key=...).schedule(cron=...)` held by a metadata group | New (P4) |
| Delayed start, ETA, countdown | Celery, DBOS, Temporal | `start_after=` | New (P3) |

### Scheduling and resources

| Feature | From | Chimaera | Status |
|---|---|---|---|
| Fractional, device-specific and variant resources | HyperQueue | `gpus=0.5`; `device="cuda:GPU-<uuid>"`; `device=[...]` as a fallback list | P5 / New (P5) |
| GPU fallback lists | Modal | `device=["cuda:H100", "cuda:A100-80GB"]` | New (P5) |
| Fairness keys across tenants | Temporal, Hatchet | Weighted DRF across tenants and runs | P6 |
| Root-task admission that prevents memory blow-up | Dask | Credit-based submission and memory-aware admission | Today |
| Resource-signal placement | Orleans | Cost model on measured load, input locality and learned compute (D6) | Today (locality) / M2 |
| Hierarchical schedulers | Flux | Peer, cell and fleet tiers (D1) | M2–M5 |
| Backlog-driven allocation on Slurm | HyperQueue, Parsl | Slurm provider asks for allocations from per-shape demand | P6 |
| Topology levels with required and preferred per level, segment sizes | Kueue TAS, Slurm topology/block | `Placement(pack=..., segment=...)` over the topology graph (D9) | P5 |

### GPU and training

| Feature | From | Chimaera | Status |
|---|---|---|---|
| Gang-scheduled multi-node clusters with RDMA | Modal `@clustered` | `.gang()` | P5 |
| Replica groups with per-step commit and recovery from a peer | torchft | Gangs of replica groups; partial restart (L13) | P5 |
| Elastic resharding after losing a slice | Cloud Pathways | `ranks=ch.auto(min, max)`, new generation per membership change | P5 |
| KV-aware routing hooks | Dynamo | `pool.route(key)` and an `ext.Router` for custom policies | Today (route) / New (P5) |
| GPU memory snapshots | Modal (alpha) | Not offered: cuda-checkpoint is single-node and skips UVM; revisit when it matures | Won't |
| Interactive SPMD from a notebook | Monarch | A notebook is a driver; gangs and meshes work from it, and the run survives the kernel | P5 |

### Deployment, observability, developer experience

| Feature | From | Chimaera | Status |
|---|---|---|---|
| An allocation that outlives the client and is reattached | Monarch jobs | Runs detach and reattach by id; the cluster has no per-job allocation | Today |
| Single binary, no external database | Restate, HyperQueue | One `chimaera` binary with embedded Raft and redb | Today |
| Discovery without etcd | Dynamo | Seeds or a Kubernetes headless service | Today |
| Rolling upgrades across versions | (Orleans and Flux need lockstep) | `feature_level` gating, N/N−1 protocol (design.md amendment 12) | M5 |
| Python-defined images and secrets | Modal | `ch.Env(image=, pip=, secrets=[...])` | P3 / New (P3, secrets) |
| Sandboxes for untrusted code | Modal, Ray (experimental) | `sandbox=ch.Sandbox(...)` | New (P6) |
| SQL over live telemetry | Monarch | `ch.cluster().query(sql)` executed on the peers (D8) | New (P4) |
| One-file performance report | Dask `performance_report` | `chimaera report RUN --html` | New (P4) |
| Spans | Dask | OpenTelemetry spans, propagated through calls | New (P4) |
| Per-call logs | Modal | `future.logs()`, `chimaera logs CALL` | New (P3) |
| Mesh-wide debugger, py-spy stacks, Perfetto profiles | Monarch | `chimaera debug`, `chimaera stack`, `chimaera timeline` | New (P4) |
| Local equals remote | Modal `.local()` | A plain call runs locally | Today |
| Edited code used on the next run | Modal `serve` | Today: an edited module is used by the next run | Today |

## 3. Performance parity

Ray's own microbenchmark suite (`ray._private.ray_perf`) was ported call for call to Chimaera in
[bench/microbench/](../bench/microbench/). Both ran on one machine on 2026-11-21: three runs each, alternating
runtimes, median shown. [RESULTS.md](../bench/microbench/RESULTS.md) has the min–max of every cell, the excluded
runs and the method.

**Treat these as rough.**
- **Machine:** a shared M3 Pro laptop (12 cores, macOS 14.5). Other sessions were building and testing on it at the
  same time.
- **Load:** the 1-minute load average before counted runs ranged from 4 to 20, and a cell often varies 2× between runs.
- **Pause:** 3 s before each benchmark instead of Ray's 10 s.
- **Builds:** Chimaera is a frozen build of the working tree (peer binary sha256 `99f83ae2…`) on Python 3.14. Ray
  is 2.60.0 on Python 3.12.
- **Not measured:** Linux, or more than one machine.

Ray's published numbers (one m5.16xlarge, five runs, `release/perf_metrics/microbenchmark.json` for 2.60.0) are lower
than Ray on this laptop for most rows. Compare Chimaera with the "Ray here" column.

### Measured

Ratio is Chimaera ÷ Ray on the same machine. Every metric is a rate, so above 1× means Chimaera is faster. Targets
come from architecture.md §3. "≥ 1×" is plain parity.

| Benchmark | Ray published | Ray here | Chimaera here | Ratio | Target |
|---|---:|---:|---:|---:|---:|
| Tasks, sync | 840 | 2,238 | 1,419 | 0.63× | ≥ 1× |
| Tasks, async | 7,181 | 10,416 | 15,362 | **1.47×** | ≥ 3× |
| Tasks, async, 4 clients | 22,932 | 13,552 | 13,364 | 0.99× | ≥ 1× |
| Tasks and get, batch | 7.38 | 10.5 | 12.2 | 1.16× | ≥ 1× |
| Wait on 1k refs (`ch.as_completed`) | 5.08 | 5.8 | 13.0 | **2.24×** | ≥ 1× |
| Get an object holding 10k refs (fresh handle each get) | 12.3 | 24.5 | 110 | **4.5×** | ≥ 1× |
| 1:1 actor calls, sync | 1,888 | 4,968 | 1,154 | 0.23× | ≥ 1.5× |
| 1:1 actor calls, async | 8,664 | 16,573 | 5,471 | 0.33× | ≥ 1.5× |
| 1:1 actor calls, concurrent | 5,575 | 16,513 | 3,891 | 0.24× | ≥ 1.5× |
| 1:n actor calls, async | 7,420 | 15,087 | 7,507 | 0.50× | ≥ 1× |
| n:n actor calls, async | 23,828 | 24,681 | 10,489 | 0.43× | ≥ 1× |
| n:n actor calls with an argument | 12,894 | 9,167 | 7,608 | 0.83× | ≥ 1× |
| 1:1 async-actor calls, sync | 1,369 | 2,997 | 1,012 | 0.34× | ≥ 1× |
| 1:1 async-actor calls, async | 4,678 | 9,865 | 4,496 | 0.46× | ≥ 1× |
| 1:1 async-actor calls with arguments | 2,882 | 6,429 | 4,041 | 0.63× | ≥ 1× |
| 1:n async-actor calls, async | 6,851 | 13,305 | 3,247 | 0.24× | ≥ 1× |
| n:n async-actor calls, async | 22,044 | 17,576 | 5,273 | 0.30× | ≥ 1× |
| Put, GB/s, one client | 20.6 | 54.0 | 13.1 | 0.24× | ≥ 90% of memory bandwidth |
| Put, GB/s, 10 clients | 40.3 | 13.1 | 8.5 | 0.65× | ≥ 90% of memory bandwidth |
| Placement groups created and removed per second | 678 | 516 | not built | — | ≥ 10× (§4) |
| Remote driver: tasks and get, batch | 1.0 | 2.1 | 11.9 | **5.8×** | ≥ 1× |
| Remote driver: tasks and put, batch | 13,243 | 23,214 | 87,195 | **3.8×** | ≥ 1× |
| Remote driver: 1:1 actor calls, sync | 547 | 1,439 | 869 | 0.60× | ≥ 1× |
| Remote driver: 1:1 actor calls, async | 1,162 | 2,014 | 9,150 | **4.5×** | ≥ 1× |
| Remote driver: 1:1 actor calls, concurrent | 1,124 | 2,753 | 7,131 | **2.6×** | ≥ 1× |

Notes on three rows:
- **Remote driver.** Ray's rows use Ray Client, a gRPC proxy. Chimaera's rows use a driver connected over TCP to a
  single-peer cluster started with `--client-listen`. That peer runs in cluster mode: three commits per call, each
  with fsync.
- **Fresh handle.** That row uses the private `ch._future._future_from_key`, so each get asks the peer and decodes 10k
  handles, as Ray's does.
- **Wait.** There is no `ch.wait`. The port makes 1,000 `next()` calls on one `ch.as_completed`.

**Not comparable**, so left out:
- **Small `put` and `get` calls**, direct and through the remote driver:
  - Chimaera: 7.7M gets/s and 275k puts/s.
  - Ray: 16k gets/s and 7.9k puts/s.
  - Chimaera keeps small values inline in the handle and caches `.result()` on the future. No IPC happens, while
    Ray's numbers measure a round trip to its object store.
- **Remote driver put GB/s:**
  - Chimaera: 13.3 GB/s. Ray: 0.35 GB/s.
  - Chimaera content-addresses values, so the identical array is uploaded once.

### Where the time goes

A local Chimaera call today makes:
- two Raft commits: the submission with the grant folded in, then the publication. Each commit is two redb write
  transactions.
- four process crossings: driver, peer, executor, peer, driver.
- about three Python thread handoffs.

A cluster call makes three commits, each with fsync.

Ray's steady-state call makes two crossings and persists nothing. The owner pushes the call to a leased worker or
straight to the actor process, and a small result comes back inline.

The table below is from timing loops at load 6–9.

| Path | Chimaera p50 | Ray p50 |
|---|---:|---:|
| Task round trip | 530–628 µs | 386–391 µs |
| Actor round trip | 607–826 µs | 171–177 µs |
| One commit (a `ch.Store.put`) | 165–200 µs | — |
| One RPC to the peer, no commit | 33 µs | — |

- **Submission is not the gap.** `.remote()` costs 21–25 µs in the Chimaera driver against 40–50 µs in Ray's, which
  is why async task submission already wins.
- **Commits.** The two commits are about 330 µs of the 530–628 µs task round trip. The function body takes about
  55 µs, and the peer's grant-to-executor handoff about 10 µs.
- **Peer CPU.** The peer spends about 140 µs of CPU per call, nearly all in redb. A profile at 15k actor calls/s puts
  the log writer at 31% of a core, state-machine apply at 28% and the scheduler under 3%.
- **Machine load.** Many serial stages and about two cores of storage work per peer explain why Chimaera loses more
  than Ray when the machine is busy. At load 6, a 1:1 async-actor loop reached 15.3k calls/s, close to Ray's 16.6k.
  During the busier suite runs it reached 5.5k.
- **Concurrent and async actors.** An actor with `concurrency` above 1 gets one call per Execute frame. Batching
  applies only to one-at-a-time actors. Each call goes through a thread-pool lane, async methods add a hop to the
  event-loop thread, and starting a call scans the mailbox linearly.

### A prototype of the new call path

[bench/proto/hotpath](../bench/proto/hotpath/RESULTS.md) builds the D2 call path:
- A Rust agent forwards each call at once and journals it to two backup processes every 1 ms, off the call's path.
- Executors and drivers are Python, and calls are pickled.

It ran Ray's actor rows against Ray 2.60 on the same laptop: three alternating runs each, Python 3.12 for both, load
average 8–20, median shown. It is not Chimaera: it has no `@ch.remote` layer, futures, placement, accounting or
failover. Its numbers are the floor under the real path, and the headroom is what those layers may spend.

| Benchmark | Ray here | Prototype | Prototype ÷ Ray |
|---|---:|---:|---:|
| 1:1 actor calls, sync | 5,839 | 50,390 | 8.6× |
| 1:1 actor calls, async | 19,143 | 592,910 | 31× |
| n:n actor calls, async | 26,436 | 1,629,758 | 62× |
| Round trip, p50 / p99 | 167 / 558 µs | 17.8 / 27.9 µs | — |
| Round trip with `effects="external"`, p50 | — | 32.6 µs | — |

- **Journal size:** 98 B per call per backup.
- **Unsafe window:** p99 1.5 ms.
- **Worst case:** about 250 ms during the n:n burst, because nothing slowed submissions when the backups fell
  behind. architecture.md D2 now bounds unsafe journal bytes per owner (8 MiB). With the bound in the prototype, the
  worst case fell to 88–112 ms at 1.86M calls/s, with no loss of throughput.

### First step on the real runtime

[bench/proto/local-group](../bench/proto/local-group/RESULTS.md) is a patch to the runtime, not yet applied, that
makes a local peer's group apply commands without a Raft log.

| Measurement | Without the patch | With the patch |
|---|---:|---:|
| Sync task round trip, p50 | 483–499 µs | 295–301 µs |
| Sync actor call round trip, p50 | 514–531 µs | 330–337 µs |
| One commit, p50 | 346 µs | 132 µs |

The round trips are three alternating runs at the same load.

- **Tests:** the Raft tests pass, including a new crash test that fails if commits are not durable. The Python suite
  has the same 258 passes with and without the patch.
- **Ray's actor round trip** here is 167–177 µs, so the gap shrinks from about 3× to about 2×.
- **What closes the rest:** the next D2 step removes the commits from ordinary calls.

### What closes each gap

| Gap | Change | Design | Expected effect |
|---|---|---|---|
| Sync tasks and actor calls (0.23–0.63×) | No commit on the call path. A call is a journal entry replicated to two backups off the critical path; only `effects="external"` waits for one in-cell round trip. | D2, D3 | Removes the ~330 µs of commits and the ~140 µs of peer redb CPU per call. What is left of the task round trip is about 200–300 µs. |
| Actor round trip slower than Ray's (607–826 vs 171–177 µs) | Direct actor path: caller's agent to host's agent in one message, ordered by `(handle, sequence)`, with replies deduplicated by deterministic call id. The executor answers from its reader thread's loop with no handoff to a main thread. | D2, "Actor calls take the direct path" | The prototype keeps all four crossings and measures 17.8 µs p50; the agent hop costs about 9 µs. To reach ≤ 150 µs, the commits and per-call Python stages must go; the agent hop can stay. |
| Concurrent and async actors (0.24–0.46×) | Batch Execute frames for every actor, not only one-at-a-time actors. Run coroutines on the executor's own loop with no thread-pool hop. Index the mailbox by readiness instead of scanning it. | D4 | Same per-call cost as one-at-a-time actors. Async actors then scale with frame coalescing, as Ray's n:n benchmarks do. |
| n:n and 1:n actor calls (0.24–0.50×) | Per-pair channels with 50 µs coalescing only under load. No shared commit log between pairs. | D2, D4 | Throughput scales with cores. Today every call on the peer serializes through one Raft log writer. |
| Put bandwidth (0.24×, 0.65×) | Hash large puts on several threads while copying, or name puts above a size threshold by a random id and hash only when the value crosses machines. | D5 | Single-threaded blake3 on every put caps throughput at the hash rate. Ray's 54 GB/s here is a memcpy. |
| Placement groups (not built) | `ch.reserve`: a two-phase commit in one metadata group per gang. | D6, D9 | Phase P5. |
| Peer CPU under load | All of the above. Each call then costs ≤ 30 µs of agent CPU (§14) instead of ~140 µs. | D2 | Less sensitivity to a busy machine. |

Async task submission, `wait`, gets of objects holding many refs, and the remote-driver rows already beat Ray. They
need to keep winning when the commit path changes; M2's gate reruns this suite.

## 4. Scale parity

Ray's published envelope (`release/benchmarks/README.md`, "being refreshed") and its latest scale report, against
Chimaera's design targets and the evidence for each. Chimaera numbers are targets until a qualification run says
otherwise.

| Dimension | Ray | Chimaera target | Mechanism and evidence |
|---|---|---|---|
| Machines in one cluster | 2,000+ (envelope); 10,000 (Anyscale, August 2026, nightly build, GCS main thread 38% busy idle) | 262,144 peers in 512 cells | No component above a cell sees per-call traffic; per-peer cost is flat (capacity.py, §14 of architecture.md). The fleet emulator (`crates/chimaera-fleet`) runs the real SWIM, shard and cell-summary code for a 512-peer cell and scales by cells. At 262,144 peers: cell summaries ≤ 210 B, 2.8 µs of shard apply per record, and every live peer sees a death within 14–16 s with no false dead views. A real multi-machine run is M6 |
| Actors | 40,000+ (10,000 nodes) | 10⁵ shared actors per peer; 10⁷+ virtual actors per fleet | Shared and virtual actors (D4); directory sharded per cell |
| Tasks running at once | 10,000+ | Every slot in the fleet (8.4M at 262,144 × 32) | Placement is per peer; simulation holds p99 queueing at 10 ms at 90% load from 4 to 256 cells |
| Tasks queued on one node | 1,000,000+ (101.9 s to queue) | Bounded by admission credits, never by an unbounded queue; 1M submitted in a single `map` with bounded memory | Credit-based submission (Today) |
| Placement groups | 1,000+ at once; 678 create/remove per second; 84/s at 64 nodes | Reservations are local two-phase commits in one metadata group per gang; target ≥ 10× Ray's rate per cell | Gang groups spread across cells' metadata groups |
| Broadcast of 1 GiB | 50 nodes in 12.9 s | Pipelined tree: log-depth, about one transfer time plus per-hop latency | D5 broadcast trees |
| Objects in one get / wait | 10,000+ | `ch.gather` and `as_completed` over 10⁶ futures with bounded driver memory | Readiness notifications are batched per owner |
| Largest object | 100 GiB+ | Bounded by peer memory and spill budget | Chunked fetch with credits (Today) |
| Head failure | Cluster stops without external Redis; actor and PG creation stop during recovery | No head; any single loss has a successor (architecture.md §8) | Localhost cluster tests (Today); turmoil simulation (M2) |
| Owner failure | Objects lost, cannot be rebuilt | Successor owner from journal backups (D3) | M2 gate |

## 5. The gap list in delivery order

Everything marked New above, grouped by phase. "Effort" is a rough size: S under a day, M a few days, L a week or
more.

**M2 (runtime core, before the 10k-seed simulation gate)**
- Call journal with primary-backup replication and successor owners; calls stop committing to Raft (D2, D3). L
- Direct actor-call path with per-handle sequence numbers and a reply table. M
- SWIM with Lifeguard inside a cell; retirement confirmed by the cell directory. M
- Placement ladder in the owner: local, in-cell sampling, late binding, stealing (D6). M
- `bench/microbench/` as a CI gate against the Ray numbers in §3. S

**P2 / P3 (API, same machine first)**
- `process="shared"`, `__enter__`/`__exit__`, structured actor lifetimes, `actor.watch()`, `ch.Deadlock`. M
- `if_exists="error"`, `ch.cluster().actors()`, `ch.ActorRestarting`, `ch.context().restarted/exit()`, `handle.restart()`, `close(drain=True)`, `max_pending_calls`. M
- Interrupting cancel, `cancel(children=False)`, `max_calls`, `name=`/`tags=`, ready-order start for `concurrency>1`. M
- `Future.add_done_callback`, `ch.executor()`, `as_completed(...).add()`, `run.future(id)`, `map(errors="return")`. S
- Call `key=` as idempotency key, `start_after=`, retry backoff, `max_restarts=(n, window)`. M
- `ch.workflow.sleep`, `ch.workflow.patched`. M
- `store.semaphore`. S
- `chimaera.multiprocessing.Pool`, joblib backend. S
- `ch.Env` fields: `files`, `exclude`, `pip`, `uv_lock`, `conda`, `image`, `python`, `setup`, `secrets`, `build_timeout`; one `uv sync` per peer per lock digest. L
- `ch.init(cpus=, memory=, gpus=, log_format=, metadata=)`; `future.logs()`; `ch.jobs` client and HTTP API; `chimaera exec/attach/sync`. M

**M3 (CPU at scale)**
- Virtual actors (`idle=`) and the activation directory. L
- `put` replicas, lineage cut points, broadcast trees, `chimaera drain`. L
- Free-threaded executors with GIL-fallback detection; template-process preloading. M
- Actor-packing controller; workflow history snapshots; spill to an object-store URI. M

**P4 (observability)**
- Dashboard on any peer; `ch.cluster().query(sql)` with DataFusion; `chimaera top/stack/profile/debug/timeline/memory/report`. L
- `ch.metrics`; OpenTelemetry propagation; JSON logs and de-duplicated task output; notebook reprs and progress. M
- `ch.Limit` flow control; HA schedules; `ext.EnvPlugin`; Dask scheduler adapter. M

**P5 / M4 (accelerators)**
- Device plugins with fallback lists and device-specific requests; fractional GPUs after measurement. M
- Gangs as meshes (`mesh.method.remote`, slicing, reduce); replica groups; hierarchical communicator setup with shrink and grow. L
- `ch.reserve` lifecycle (`key=`, `lifetime=`, `ch.reservation`, inherited by child calls); topology levels with segments; isolation tokens (IMEX channels, ComputeDomains). L
- `transport="device"` over NIXL/UCX, `ch.put(..., transport="device")`, `ext.Transport`, `ext.CollectiveBackend`, `graph.explain()`. L
- XID-tiered health with repair throttling. M

**M5 / P6 (fleet)**
- Cells, ambassadors and summaries, global directory, scoped names, delegated quotas, `durability="region"`. L
- `ch.cluster().request(...)`, Slurm backlog allocation, `ch.Sandbox`, `chimaera replay --from`. M
