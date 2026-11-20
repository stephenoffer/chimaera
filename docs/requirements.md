# Requirements catalog

Where Chimaera's automatic behavior comes from, and which milestone must prove it. The requirements come from
the problems Ray users report most often (grouped into the pain map below), from published Ray tuning advice,
and from [ray-lessons.md](ray-lessons.md) (`L1`–`L16`). Tuning thresholds taken from that advice are treated as
priors to measure against, not as facts.

Every requirement must end up with a test that can fail, or an explicit note that it cannot be verified on the
available hardware (no GPU, no real fabric). The milestone column says where that happens.

## Pain map

| Category | Ray root cause | Chimaera answer |
|---|---|---|
| Data IO, format and API | Arrow types inferred per block; API churn | Schema unification, block splitting (A8) |
| LLM engine crash/hang/perf | Multi-process engine inside actors | Engine-load protocol, process-tree kill (R2) |
| Host OOM | Memory logical and unenforced; 250 ms poller races the kernel | Per-attempt cgroups, OOM ladder (R1, R2) |
| Serve deploy and health | Ping-only health on the user's event loop | Progress-based health, liveness off the user loop (O3, R6) |
| NCCL / collectives / fabric | No fabric validation, no straggler attribution | Fabric probe, rank heartbeats (R8) |
| GPU memory and KV cache | GPU memory unscheduled, engines sized statically | GPU batch probe, measured fractional packing (A11) |
| Train checkpoint/storage/failures | User-driven, non-atomic checkpoints | Manifest-committed checkpoints, cadence (A14, R8) |
| Data throughput/backpressure | Heuristic budgets over Core | Binding-stage controller (C1, A9) |
| Credentials and security | Ambient per-process credentials | Credential preflight (P5), auth by default (L11) |
| Autoscaler and capacity | Logical demand only; uncoordinated autoscalers | Queue + measured use (L8), reservation expiry (R10) |
| Serialization | Implicit cloudpickle of closures | Preflight naming the variable (P2) |
| Object store and spilling | Fixed per-node store, global budgets | Per-node byte budgets (A7) |
| Observability | Log files per worker, scrape-reset gauges | Typed events, bounded cardinality (O5) |
| Dependencies and version skew | Environments built at run time | Content-addressed environments (L12, P4) |
| Scheduling and placement | Reservations never expire; nested requests deadlock | Owner+expiry (R10), blocked tasks release CPU (L8) |
| GCS and control plane | Slow treated as dead; suicide on control-plane loss | Static stability (R6, L3) |
| Process and OS limits | Process per worker, many sockets and files, fork | Process hygiene (P6) |
| KubeRay ops | Two control loops; blue-green needs 2× | In-place upgrades (L15) |

Other categories (LLM output correctness, RL post-training, LLM startup, training frameworks, GPU drivers)
inform P4, P5 and the correctness gate on canaries (C2).

## Controller discipline

| Id | Requirement | Proven in |
|---|---|---|
| C1 | Find the binding stage first (busy = udf time / (span × parallelism), consumer-blocked fraction); only its knobs move. | M3 |
| C2 | Canary inside the run against a concurrent control; ≥10 observations in steady state; adopt only if the gain beats measured noise and guards (memory, errors, p99, correctness) hold. | M1 (replay), M3 (live) |
| C3 | Vetoes: no CPU cut or concurrency increase while memory kills are in the window; no CPU rules on GPU tasks; never shrink keys with CV > 0.5 or tasks < 50 ms. | M3 |
| C4 | Learned state keyed by (code digest, input-size bucket, hardware class); thresholds are priors updated per key. | M3 |
| C5 | Physical counters only: cgroup CPU, CPU-seconds per byte, physical GPU activity; never logical slots. | M1 |
| C6 | Every action logged with signal, arms, effect, noise and rollback reason; `chimaera explain --decisions` shows it. | M1 |

## Autoconfiguration (signal → threshold → action → guard → rollback)

| Id | Requirement | Proven in |
|---|---|---|
| A1 | Granularity: dispatch overhead > 10% or p50 < 50 ms → coalesce k = ⌈200 ms / item⌉ into ranges; never for `latency="interactive"` work. | M1 |
| A2 | Arguments > 1 MB content-hashed and stored once; closures > 1 MB hoisted with a warning naming the variable. | M1 |
| A3 | In-flight window by bytes per node; halve on spill or pressure; grow while the consumer is blocked > 5% and bytes fit. | M1 (guard), M3 |
| A4 | CPU reservation = quantum(avg × 1.5) over {0.25, 0.5, 1, 2, 4}; shrink only if avg < 30%, p95 low, CV < 0.3; I/O reads 0.25. | M3 |
| A5 | Thread pools set at executor spawn so threads × concurrency ≈ granted cores; executors keyed by thread class. | M1 |
| A6 | Memory reservation = p95 peak × {1.2, 1.5, 2.0} by CV and OOM history; trend projection raises it early; enforced as a cgroup limit; OOM-killed attempts are censored lower bounds. | M3 |
| A7 | Byte budgets per peer (objects, spill, in-flight) and disk budgets per peer (spill, shuffle, checkpoints, logs, caches). | M1, M3 |
| A8 | Blocks/batches sized by bytes: 0.1–5 s per task, 1–128 MiB per block, ceiling from the attempt's memory budget, split before Arrow's 2 GiB offset limit. | M3 |
| A9 | Stage balancer: GPU idle > 20% with an upstream queue → move cores to the binding CPU stage; the canary checks the sign. | M4 |
| A10 | `replicas="auto"`: GPU stages start at all free GPUs; ×-up when util > 0.8, −1 down after cooldown; prefer concurrency per actor; ≤ ~1000 actors per pool. | M4 |
| A11 | GPU batch: probe toward 70–80% VRAM during warmup; ×0.5 and pin on OOM; fractional GPUs only after a measured peak, capped at fraction × 0.8. | M4 (unverified on GPU) |
| A12 | Shuffle: partitions ≈ bytes / 256 MiB; hash, not sort; hot-key split only for associative combines; broadcast when one side < 1 GiB. | M3 |
| A13 | Serving: AIMD per-replica limiter finds the latency knee, target 0.6 × knee; batch wait = min(SLO slack, fill time); downscale delay ≥ max(600 s, 2 × measured cold start); admission cap derived from the engine cap. | M4 |
| A14 | Checkpoint interval = max(5 min, 3 × write time, √(2 × write × MTBF)) within a 10% overhead budget; async upload. | M4 |
| A15 | dtype defaults from GPU capability; dtype/quantization changes canaried with a correctness gate. | M4 (unverified on GPU) |

## Mid-run correction and recovery

| Id | Requirement | Proven in |
|---|---|---|
| R1 | Memory pressure, in order: throttle via PSI/`memory.high`; kill the attempt furthest over its reservation; retry OOMs on a separate budget at ×1.5 on a peer with room; split replayable input; an indivisible input fails with its size and smallest feasible shape. | M0 (state machine budget), M1, M3 |
| R2 | Kill the whole cgroup process tree on worker death; verify GPU memory is freed before reuse. | M1 |
| R3 | Classify failures: transient vs repeated identical fingerprint (stop on the second); deterministic re-execution; idempotent sink commits. | M0 (state machine), M3 (sinks) |
| R4 | Rotate workers on an RSS/VRAM leak slope; jemalloc where available. | M3 |
| R5 | Speculation for replayable, deterministic work only; first publication wins. | M0 (state machine), M2 |
| R6 | Static stability: granted work keeps running through leader loss; overloaded is not dead. | M2 |
| R7 | Preemption notices trigger checkpoint and drain; node replacement with backoff, bounded attempts and an alert. | M5 |
| R8 | Gangs: fabric probe (refuse TCP fallback unless `allow_tcp=True`); per-rank heartbeats and collective sequence numbers; restart only the affected gang from a manifest-committed checkpoint; invalidate checkpoints written after XID/ECC faults. | M4 (fabric unverified) |
| R9 | GPU health: XID/ECC/thermal/PCIe faults quarantine the device, rate-limited. | M4 (unverified on GPU) |
| R10 | Every reservation has an owner and an expiry; leftovers never block scale-down. | M2 |
| R11 | Correlated failures form one incident; report the first error, not the loudest. | M2 |

## Observability and explanation

| Id | Requirement | Proven in |
|---|---|---|
| O1 | Hang explainer: after T s without progress, stack-sample and classify (divergent rank, nested wait, unschedulable, backpressure, kernel OOM). | M1, M4 |
| O2 | Every kill reports mechanism, offender, and reservation vs peak. | M1 |
| O3 | Per-stage busy fraction, consumer-blocked time, bytes in flight per peer, spill, CPU-s/byte, event-loop lag. | M1, M3 |
| O4 | Unschedulable work explained immediately with a feasibility diff (requested vs largest shape, blocking holders with owner and age). | M1 |
| O5 | Typed runtime events, not log regexes; bounded label cardinality; measured diagnostics overhead. | M1 |
| O6 | Error messages cover the errors users hit most (OOM names attempt, peak vs declared, growth, retry plan; NCCL timeout names the missing rank; …). | M1–M4 |

## API, configuration and environment

| Id | Requirement | Proven in |
|---|---|---|
| P1 | Strict config: unknown, conflicting or shadowed options rejected at submit with field names and sources; effective config printable. | M1 |
| P2 | Lint at submit: get-in-loop, nested blocking wait, closure > 1 MB, blocking call in async method, unpicklable capture with its path. | M1 |
| P3 | `deterministic=` and `latency=` options; declarative projection/predicate pushdown for partitions, warning on lambda filters. | M1, M3 |
| P4 | Content-addressed peer-to-peer artifact cache: single-flight download, atomic writes, prefetch at placement. | M5 |
| P5 | Preflight credentials and storage with the job's scoped identity; driver/CUDA/compute capability as schedulable labels. | M4, M5 |
| P6 | Process hygiene: spawn not fork for executors, fork-after-CUDA detection, ulimits, one multiplexed port per peer, agent in a reserved cgroup. | M1, M2 |
| P7 | Bounded queues with `Overloaded(retry_after)`; cancellation when the caller disconnects. A native HTTP proxy is out of scope. | M4 |

## Benchmarks and baselines

- **Acceptance workloads:** image classification, image embedding and training ingest (flagship 1); fuzzy
  dedup and a multimodal join (flagship 2); video frame explosion (memory and stragglers). Rule: naive user code
  on Chimaera matches or beats a tuned Ray version, and controllers do not regress the trap cases (a read cap
  whose effect changes sign, threads past the crossover, vision `max_num_seqs`).
- **Baselines:** Chimaera measures task submit, actor creation and recovery time for itself and for Ray on
  identical hardware, and labels them local measurements ([qualification.md](qualification.md)).
