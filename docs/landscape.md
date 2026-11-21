# The landscape: who Chimaera competes with and what to take from each

**21 November 2026.** A review of every runtime that distributes Python tasks or actors, or that users reach for
instead. Each entry says what to adopt, what to avoid and why. The item-level matrix is in [parity.md](parity.md);
the design that follows from it is [architecture.md](architecture.md). Ray's own failure record is in
[ray-lessons.md](ray-lessons.md) and is not repeated here.

Versions are as of this date. Numbers come from the linked primary sources; "vendor claim" marks numbers only the
vendor has published.

## Who matters

| System | Version | What it is | Why it matters to Chimaera |
|---|---|---|---|
| Ray Core | 2.60.0 (4 Nov 2026) | Tasks, actors, object store; head node with the GCS | The baseline users compare against. 3.0 is unreleased; Anyscale's acquisition by Nscale was announced 30 July 2026. |
| PyTorch Monarch | 0.8.0 (13 Nov 2026), pre-1.0 | Single-controller actor meshes on a Rust actor system (hyperactor) | The new choice for RL and training control planes; best ideas on meshes, supervision and telemetry. |
| Modal | client 1.6.4 (11 Nov 2026), hosted only | Serverless functions and classes with infrastructure in the decorator | The ergonomics bar. |
| Dask distributed | 2026.8.0, maintenance mode | Futures, actors, coordination primitives; one Python scheduler | The futures API Python users already know. |
| Temporal, DBOS, Restate, Inngest | Nov 2026 | Durable execution | What "survives any crash" means to application developers. |
| Orleans, Akka/Pekko, Erlang/OTP, Dapr | Oct–Nov 2026 | Actor systems | Virtual actors, supervision, passivation, membership at thousands of nodes. |
| Celery, Dramatiq, RQ, Hatchet | Sep–Nov 2026 | Task queues | Flow control: rate limits, concurrency keys, delayed starts, schedules. |
| Parsl, HyperQueue, Flux | Oct–Nov 2026 | HPC task runtimes | Rust single-binary deployment, rich resource models, hierarchical scheduling at 11,520 nodes. |
| Pathways, torchft, Dynamo, NIXL, SkyPilot | Oct–Nov 2026 | Accelerator controllers and data movement | What large GPU fleets need: elastic groups, peer recovery, one-sided transfers. |

The serious 2026 alternatives to Ray Core are Monarch for training and RL control planes, Modal for ergonomics,
durable engines for reliability, and hand-rolled ZMQ-plus-NCCL orchestrators (Prime Intellect's prime-rl, among
others). Daft's Flotilla still runs on Ray. Bytewax has stalled since November 2024.

## Ray Core

**Strengths to keep.** One decorator for functions and classes; futures as dependencies; a shared-memory object
store with zero-copy reads; actors with ordered calls; placement groups; a large ecosystem. Published single-node
microbenchmarks (m5.16xlarge, Ray 2.60): 7,181 async tasks/s from one client, 22,932 from four, 8,664 async actor
calls/s on one handle, 23,828 n:n, 20.6 GB/s `put`. Scale: 2,000+ nodes in the published envelope, 10,000 nodes
and 40,000 actors in Anyscale's August 2026 report on a nightly build.
([microbenchmark.json][raymicro] · [envelope][rayenv] · [Anyscale][anyscale])

**What still hurts**, beyond ray-lessons.md, from 2026 evidence:

- The GCS remains a single process. Active-passive failover shipped in 2.60 but is off by default. At 10,000 idle
  nodes its main thread was 38% busy after a year of fixes, and the resource syncer took about 200 s per update at
  peak. ([Anyscale][anyscale])
- Actor creation needs four or more owner-GCS messages; Microsoft measured 89.4 s to create actors across 32,000
  workers, with 55.3 s p99 RPC, and built a proxy to cut it to about 33 s. ([Ray Summit 2026][summit])
- The driver is a hot spot: a publish lock took 17.4% of the scheduling loop, and over a million callbacks queued
  on one thread in a shuffle. ([Anyscale][anyscale])
- Owner death still loses objects; `put` objects cannot be rebuilt; resources are logical; there is no tenancy.

**Where each Ray pain point ends up in Chimaera:**

| Ray pain | Chimaera's answer |
|---|---|
| Head node and GCS are a single point of failure; HA needs Redis | No head: every head is a movable Raft leader or lease (architecture.md D1, §8) |
| Owner death loses objects; `put` objects cannot be rebuilt | Owners are agents with journal backups and successors (D2, D3); `put` values get a replica (D5) |
| Per-task metadata through the owner and per-actor-start messages through the GCS | Calls never touch a quorum; actor creation touches only the actor's metadata group (D2, D4) |
| One process per actor | Shared and virtual actors (D4) |
| Resources are logical; the OOM killer races the kernel | Learned, enforced reservations and one memory pool per peer (built); per-call physical accounting (D8) |
| Infeasible work waits forever; pending work unexplained | `Infeasible` at submission; every waiting call has a reason (built) |
| Fine-grained tasks are slow; anti-patterns left to users | Automatic batching and bounded submission (built); journal off the critical path (D2) |
| No per-call timeout, no priorities | `deadline=`, `priority=` (built) |
| Placement groups leak; detached actors leak | Structured lifetimes, owner and expiry on every reservation (D4, R10) |
| Resource view goes stale at scale (200 s at 10,000 nodes) | No global view: live probes in the cell, summaries only steer (D6) |
| Health checks into one process (about 33,000 RPC/s at 100,000 nodes) | SWIM inside cells; retirement is one commit in the cell's directory (D7) |
| `runtime_env` installs on the critical path; `uv run` per worker breaks at scale | Content-addressed environments built before admission, one `uv sync` per peer (§12) |
| No multi-tenancy | Tenants, DRF and delegated quotas (§6) |
| Async actors silently change semantics | `async def` never changes semantics (built) |

**Take:** the decorator, futures, ordered actors, zero-copy, lease reuse on the fast path, and Ray's
own microbenchmark suite as the yardstick (ported in `bench/microbench/`). **Avoid:** a metadata service on any
per-call or per-actor-start path, owners inside user processes, logical-only resources.

## PyTorch Monarch

**What it is.** One Python script drives meshes of actors: `HostMesh → ProcMesh → ActorMesh`, with named
dimensions. Endpoints have adverbs: `call_one`, `call` (gather into a value mesh), `choose`, `broadcast`, `stream`.
Meshes slice and reshape. Supervision is fail-fast by default with an opt-in `__supervise__` hook. RDMA buffers move
bytes one-sided over InfiniBand, EFA or ROCm. Telemetry is queried with SQL, executed in place by DataFusion on each
host. Jobs cache their allocation so a new client can reattach. ([actors guide][monarchactors] ·
[observability][monarchobs] · [v0.8.0][monarch08])

**Numbers.** Bootstrap of 2,048 hosts and 16,384 processes in 4.64 s p50; a no-op RPC to 16,384 actors in 9.03 ms
p50, 12.8 ms p99 (v0.8.0, CPU workers). TorchFT on 240 H100s recovered from process failures in 90 s on average.

**Weaknesses.** The single controller is the failure domain: an unhandled error crashes the client. No global actor
registry, no object store, no lineage, no task farm, `choose()` ignores load, pre-1.0 churn.

**Take:** meshes with slicing and gather in one call (architecture.md §4, actor meshes); tree fan-out for any
broadcast (D5); fail-fast errors with supervision as an option; SQL over live telemetry (§9); one-sided device
transfers (D5). **Avoid:** making the driver the place where the system's state lives.

## Modal

**What it is.** Functions and classes with the image, GPU, secrets, volumes, retries, timeout and schedule declared
in the decorator; `.remote()`, `.local()`, `.map()`, `.spawn()` with retrieval by id for seven days;
`@modal.enter`/`@modal.exit` lifecycle hooks; `@modal.concurrent` and `@modal.batched`; named `Dict` and `Queue`;
memory snapshots for 3–10× faster cold starts (Stable Diffusion 13 s → 3.5 s); gang-scheduled `@modal.clustered`
with RDMA (GA 1 October 2026); sandboxes. ([docs][modal] · [snapshots][modalsnap] · [clusters][modalclusters])

**Why its ergonomics win.** Infrastructure sits next to the code, in Python; the same function runs locally or
remotely; edit-run loops take seconds because Modal owns the whole stack (its own scheduler, a lazy
content-addressed image filesystem, snapshots).

**Weaknesses.** Hosted only. 4,000 concurrent containers per function. Classes are anonymous pools, not addressable
actors. No shared object store.

**Take:** options on the decorator for everything (Chimaera's `env=`, `gpus=`, `device=`, `deadline=`), lifecycle
hooks (as plain `__enter__`/`__exit__`), retrieval of a call by id from any process (`run.future(id)`), device
fallback lists (`device=["cuda:H100", "cuda:A100"]`), and cold-start engineering: warm pools, preloaded template
processes, content-addressed environments. **Avoid:** a hosted-only design; anonymous replicas as the only stateful
abstraction.

## Dask distributed

**What it is.** `submit`/`map`/`gather`/`scatter`; `as_completed` with `.add()` for dynamic frontiers;
`secede`/`rejoin` so a waiting task gives back its thread; `Lock`, `Semaphore`, `Event`, `Queue`, `Variable`;
priorities and annotations; work stealing; root-task queuing that prevents memory blow-up; spill, pause and
terminate thresholds; graceful worker retirement that moves data; `performance_report`. ([futures][daskfutures])

**Numbers.** One Python asyncio scheduler holds all state. Overhead is 200 µs to 1 ms per task; fast tasks
plateau at 2,000–3,000 per second regardless of worker count. A Rust scheduler (RSDS) beat it, and a random
policy in Rust matched Dask's work stealing: the bottleneck was runtime overhead, not policy. ([best
practices][daskbp] · [RSDS][rsds])

**Weaknesses.** The scheduler is a single point of failure with no HA; actors are not fault tolerant; client,
scheduler and workers must share an environment.

**Take:** `as_completed(...).add()`, automatic secede on a blocking wait (built: a waiting call releases its CPU),
semaphores, work stealing (D6), graceful drain that migrates data, a one-file performance report. **Lesson:** a
native control plane with simple placement beats a clever policy on a slow one.

## Durable execution: Temporal, DBOS, Restate, Inngest

**What they offer.** Workflows replayed from an event history; activities with timeouts and retry policies;
signals, queries and validated updates; durable timers lasting days; child workflows; patch-based versioning of
in-flight code (`workflow.patched("id")`); workflow ids as idempotency keys; queues with per-key concurrency, global
rate limits, priorities, fairness and delayed starts (DBOS, Inngest); virtual objects with keyed single-writer state
(Restate). ([Temporal limits][temporallimits] · [DBOS queues][dbosqueues] · [Restate 1.2][restate12])

**Numbers.** Restate, a single Rust binary with a replicated log and partition processors: 23,131 one-step
workflows/s at p50 16 ms on three nodes, 84,000 actions/s. DBOS on one Postgres: 43,000 workflows/s, 12,100
queued/s (vendor claims). Temporal history is capped at 51,200 events or 50 MB per execution, with history shards
fixed at cluster creation.

**Take:** durable workflows (built), durable sleep, signals through a durable queue, patch markers
(`ch.workflow.patched`), call keys as idempotency keys, delayed calls (`start_after=`), HA schedules, and flow
control by key (`ch.Limit`). **Avoid:** fixed shard counts (Chimaera's vshards move between groups), a database on
the call path, and history without compaction.

## Actor systems: Orleans, Akka/Pekko, Erlang/OTP, Dapr

**What they offer.** Orleans: virtual actors activated on first call, collected when idle, reactivated elsewhere
after failure; resource-aware placement; experimental live migration; reminders. Akka: cluster sharding,
passivation with a W-TinyLFU cap, split-brain resolver, restart with backoff. OTP: supervision strategies, restart
intensity, links and monitors. Dapr: sidecar virtual actors, reminders in a leaderless scheduler.
([Orleans placement][orleansplacement] · [Orleans directory][orleans])

**Numbers.** Orleans served Halo 4 presence with 1M actors on 25 servers and stayed flat from 2,000 to 2 million
actors. Akka gossip reached 2,400 nodes. Erlang's full mesh stops near 1,500 nodes; WhatsApp needed a custom
transport for 10,000.

**Take:** virtual and shared actors (D4); idle passivation; restart backoff and intensity; watches; placement that
can change after creation (actor packing, §10). **Avoid:** full-mesh membership (Chimaera runs SWIM inside cells),
fixed shard counts, eventually consistent single-activation.

## Task queues: Celery, Dramatiq, RQ, Hatchet

**What they offer.** ETA and countdown, retries with backoff and jitter, chains and chords, rate limits,
concurrency keys with fair round-robin (Hatchet), schedules. **Known bugs to avoid:** Celery's `rate_limit` is per
worker, not global; Redis visibility timeouts redeliver long ETA tasks in a loop; Beat has no HA. Postgres-backed
queues top out around 10⁴ operations per second (Hatchet: about 2,000 runs/s on 8 vCPUs).

**Take:** global limits enforced through delegated token envelopes, so a limit is fleet-wide without a counter on
the call path (`ch.Limit`); delayed calls; HA schedules held by a metadata group.

## HPC: Parsl, HyperQueue, Flux

HyperQueue is a Rust single binary with fractional, device-specific and variant resources, NUMA awareness and
backlog-driven Slurm allocation, at about 0.1 ms overhead per task. Flux schedules hierarchically: any job can host
its own scheduler; it runs El Capitan's 11,520 nodes and gave 48× throughput for a million short jobs. Parsl peaks
near 1,200 tasks/s. ([HyperQueue][hq] · [Flux][fluxslides])

**Take:** the single-binary deployment (Chimaera already ships one), device-specific and fractional resources,
backlog-driven allocation in the Slurm provider, and Flux's lesson that hierarchy is how schedulers scale (D1).

## Accelerator controllers: Pathways, torchft, Dynamo, NIXL

Pathways drives thousands of TPUs from one client at near-full utilization, and Cloud Pathways (GA 9 November 2026)
reshards elastically after losing a slice without crashing the client. torchft treats each training step as a
transaction across replica groups and recovers a failed replica by copying weights from a healthy peer instead of a
checkpoint: 82% step efficiency with a failure every 60 s on 300 GPUs, and effective training time from 44% to 80%
at O(100K) GPUs. torchrun, by contrast, kills every survivor on any membership change. Dynamo routes inference by KV
cache and runs on Kubernetes without etcd; NIXL moves bytes between GPU, CPU, file and object storage through
plugins. ([Cloud Pathways][pathways] · [torchft][torchft] · [Dynamo][dynamo])

**Take:** replica groups as the unit of failure inside gangs, with recovery from peers (L13); elastic resharding as
a gang generation change; NIXL and UCX as device transports (D5); a driver-style programming model over a headless
core, so the driver is a client and never the cluster.

## Ten architectural lessons

1. **A central Python scheduler caps throughput at a few thousand tasks per second**, and the limit is overhead, not
   policy (Dask, Parsl, RSDS). Chimaera: native agent, simple sampling, batched submission.
2. **Fan-out must be a tree.** Monarch's multicast trees bootstrap 16,384 processes in under 5 s; Ray's star-shaped
   actor creation takes 89 s at 32,000 workers. Chimaera: broadcast trees, and actor creation that touches only the
   actor's metadata group.
3. **Keep control and data apart.** Handles on the control path, bytes on RDMA or direct streams (Monarch, Dynamo,
   Ray's late RDT).
4. **A partitioned replicated log scales without external databases** (Restate), but shard counts must be
   changeable (Temporal, Akka and Orleans regret fixing them). Chimaera: vshards that move between Raft groups.
5. **A database as coordinator stops near 10⁴ operations per second** (DBOS, Hatchet). Embedded replicated state
   goes further.
6. **Decentralized membership beats a head node, but not as a full mesh** (Erlang at 1,500 nodes). Chimaera: SWIM
   inside cells, summaries between them.
7. **Choose the unit of fate-sharing deliberately.** torchft's replica groups doubled effective training time at
   scale; Ray's owner fate-sharing loses objects. Chimaera: agents with successors, gangs with replica groups.
8. **Driver-style programming wins, if the driver is only a client** (Pathways, Monarch's reattachable jobs).
   Chimaera: runs that outlive their client, reattach by id.
9. **Cold start is a systems problem** that Kubernetes does not solve (Modal). Chimaera: warm pools, preloaded
   template processes, content-addressed environments and weights.
10. **Placement must stay changeable after creation** (Orleans rebalancing, Akka passivation, Dask root-task
    queuing). Chimaera: actor packing, passivation, admission by measured memory.

[raymicro]: https://raw.githubusercontent.com/ray-project/ray/master/release/perf_metrics/microbenchmark.json
[rayenv]: https://raw.githubusercontent.com/ray-project/ray/master/release/benchmarks/README.md
[anyscale]: https://www.anyscale.com/blog/how-we-scaled-ray-from-batch-inference-to-10000-node-training-clusters
[summit]: https://www.anyscale.com/blog/ray-summit-2026-recap
[monarchactors]: https://meta-pytorch.org/monarch/stable/actors.html
[monarchobs]: https://meta-pytorch.org/monarch/stable/observability.html
[monarch08]: https://github.com/meta-pytorch/monarch/releases/tag/v0.8.0
[modal]: https://modal.com/docs/guide/concurrent-inputs
[modalsnap]: https://modal.com/blog/mem-snapshots
[modalclusters]: https://modal.com/blog/modal-clusters-generally-available
[daskfutures]: https://docs.dask.org/en/stable/futures.html
[daskbp]: https://docs.dask.org/en/stable/best-practices.html
[rsds]: https://arxiv.org/abs/2010.11105
[temporallimits]: https://docs.temporal.io/workflow-execution/limits
[dbosqueues]: https://docs.dbos.dev/python/tutorials/queue-tutorial
[restate12]: https://restate.dev/blog/announcing-restate-1.2
[orleansplacement]: https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-placement
[orleans]: https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory
[hq]: https://it4innovations.github.io/hyperqueue/stable/
[fluxslides]: https://www.openfabrics.org/wp-content/uploads/2025_webinars/webinar_5/Flux_OFA_Final.pdf
[pathways]: https://cloud.google.com/blog/topics/ai-infrastructure/cloud-pathways-a-distributed-ml-runtime-is-ga
[torchft]: https://pytorch.org/blog/fault-tolerant-llama-training-with-2000-synthetic-failures-every-15-seconds-and-no-checkpoints-on-crusoe-l40s/
[dynamo]: https://docs.nvidia.com/dynamo/latest/reference/api/python/runtime
