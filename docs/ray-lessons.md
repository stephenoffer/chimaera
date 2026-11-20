# Lessons from Ray's architecture and operations

Researched 20 November 2026, against Ray 2.60.0 (released 4 November 2026). Each item names the pain, the
architectural cause in Ray, the evidence, and the requirement Chimaera takes from it. Items marked
**[unverified]** come only from secondary or anecdotal sources. Requirement ids (`L1`…`L16`) are referenced
from [requirements.md](requirements.md).

## Head node and GCS

- **The GCS is a single point of failure.** All cluster metadata lives in one in-memory process by default; "if
  it fails, the entire Ray cluster fails." HA needs external Redis (officially supported for Serve only on
  KubeRay); the RocksDB backend is alpha and single-writer. During recovery, actor and placement-group creation
  and worker registration stop, and raylets that cannot reconnect within 60 s kill themselves.
  [GCS fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/gcs.html)
- **HA without fencing.** Users ask how two heads can coexist during RayService upgrades with a shared Redis
  namespace ([discuss](https://discuss.ray.io/t/how-can-rayservice-incremental-upgrades-two-heads-briefly-safely-coexist-with-redis-backed-gcs-ft-without-split-brain-or-stale-state-issues/23592)).
  A user on 2.55 saw `gcs_server` SIGSEGV during a Valkey primary failover [unverified by maintainers]
  ([discuss](https://discuss.ray.io/t/ray-head-gcs-server-crashes-with-sigsegv-exit-code-11-during-elasticache-valkey-primary-failover-gcs-ft-does-not-survive-redis-primary-switchover/23594)).
- **Head memory grows.** The docs say head memory "can steadily increase over time" and recommend a 16-core,
  64 GB head with `num-cpus: 0` ([doc](https://docs.ray.io/en/latest/ray-core/head-node-memory-management.html));
  open leaks [#45338](https://github.com/ray-project/ray/issues/45338),
  [#63118](https://github.com/ray-project/ray/issues/63118),
  [#58815](https://github.com/ray-project/ray/issues/58815). Task events lived in the GCS until 2.58.
- **The head runs everything:** GCS, autoscaler, dashboard and every job's driver. Its most stressed resource is
  outbound bandwidth ([large clusters](https://docs.ray.io/en/latest/cluster/vms/user-guides/large-cluster-best-practices.html)).
  A user OOM on the head takes the control plane with it. Ant Group's fork adds head HA and virtual clusters
  ([fork](https://antgroup.github.io/ant-ray/ray-core/fault_tolerance/head-ha.html)).

## Ownership

- **An owner's death loses its objects.** "Ray does not support recovery from owner failure" (`OwnerDiedError`);
  `ray.put` objects cannot be rebuilt; lineage is evicted past 1 GB
  ([object FT](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html)). The request for GCS-owned
  objects ([#12635](https://github.com/ray-project/ray/issues/12635), 53 comments) was closed as stale in 2025.
- **Actors share fate with their creator.** Ray "will not automatically recover an actor whose owner is dead,
  even if it has a nonzero max_restarts"; defaults are `max_restarts=0`, at-most-once delivery, and a restart
  re-runs the constructor ([actor FT](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)).
  Placement groups are scoped to the driver by default.

## Resources and memory

- **Resources are logical and unenforced** ([resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)).
  The OOM killer acts at 95% and "makes no guarantees"; killed tasks retry ignoring `max_retries`
  ([OOM](https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html)). cgroup v2 isolation
  (2.51+) reserves memory for Ray's own processes but does not isolate tasks from each other
  ([doc](https://docs.ray.io/en/latest/ray-core/resource-isolation-with-cgroupv2.html),
  [forum](https://discuss.ray.io/t/resource-isolation-on-ray-actor-level/23478)).
- **Hidden pinning:** five reference kinds pin objects; finding a leak needs `ray memory --group-by STACK_TRACE`
  ([memory](https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html)).
- **Thirteen documented anti-patterns** (`ray.get` in a loop, too-fine tasks, large args passed repeatedly,
  closures capturing large objects, …) are footguns left to users
  ([patterns](https://docs.ray.io/en/latest/ray-core/patterns/index.html)).
- **Uncapped idle workers:** 90+ `ray::IDLE` processes on a 4-CPU node
  ([#27499](https://github.com/ray-project/ray/issues/27499)).

## Scheduling

- **Fine-grained tasks are slow:** the docs' example takes 1.6 s for 10k tiny tasks vs 0.165 s serially and
  0.03 s batched ([doc](https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html)).
- **The autoscaler ignores physical usage** and only reacts to resource requests. A task blocked in `ray.get`
  releases its CPU, the node looks idle, gets removed, and its children die
  ([#46492](https://github.com/ray-project/ray/issues/46492); see also
  [#52573](https://github.com/ray-project/ray/issues/52573)).
- **Placement groups:** atomic only at creation; partial groups after node loss; no warning for infeasible
  groups; PACK/SPREAD are soft; topology strategy is alpha with one label
  ([PG doc](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html)).
- **Pending work is unexplained:** tasks stuck in `PENDING_NODE_ASSIGNMENT`, sometimes fixed by restarting the
  head ([forum](https://discuss.ray.io/t/subset-of-tasks-stuck-in-pending-node-assignment-forever/9821)).

## Deployment and operations

- **Security came late.** No authentication for years; CVE-2023-48022 was disputed and never patched; the
  ShadowRay 2.0 campaign ran in November 2025
  ([Oligo](https://oligo.security/blog/shadowray-2-0-attackers-turn-ai-against-itself-in-global-campaign-that-hijacks-ai-into-self-propagating-botnet)).
  Token auth arrived in 2.52, on by default only for local clusters since 2.59
  ([token auth](https://docs.ray.io/en/latest/ray-security/token-auth.html)).
- **No multi-tenancy:** sharing a cluster is "not recommended in production"
  ([FAQ](https://docs.ray.io/en/latest/cluster/faq.html)).
- **Version skew:** Ray Client requires the same Ray and Python minor version and kills the workload after a 30 s
  disconnect ([Ray Client](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)).
- **`runtime_env`:** `working_dir` capped at 500 MiB; pip/conda installed at run time
  ([dependencies](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html)); runtime-env agent
  failures mark nodes dead ([#54321](https://github.com/ray-project/ray/issues/54321)).
- **Logs and history:** no native log storage; 200K+ log files on one node
  ([#32112](https://github.com/ray-project/ray/issues/32112)); dashboard locks up as history grows
  ([#35202](https://github.com/ray-project/ray/issues/35202)); the state API truncates at 10k entries
  ([forum](https://discuss.ray.io/t/state-api-list-tasks-stops-working-for-long-running-jobs/22457)).

## API

- **Any `async def` silently makes an actor async**, with default `max_concurrency` 1000 and no ordering
  guarantee ([async actors](https://docs.ray.io/en/latest/ray-core/actors/async_api.html)).
- **No per-call timeout**, requested since 2021 ([#18916](https://github.com/ray-project/ray/issues/18916)).
- **Arrays passed as arguments become read-only** through zero-copy deserialization
  ([serialization](https://docs.ray.io/en/latest/ray-core/objects/serialization.html)).
- **Error taxonomy:** `ObjectLostError` with `OwnerDiedError`, `ObjectReconstructionFailedError` and
  `ObjectFetchTimedOutError`; callers must tell `ActorUnavailableError` from `ActorDiedError`.
- **`local_mode` diverged** from cluster semantics and was deprecated.

## Libraries

- Ray Data assumes UDFs use 0 memory ([OOM guide](https://docs.ray.io/en/latest/data/how-to-avoid-ooms.html));
  `random_shuffle` materializes the dataset ([shuffling](https://docs.ray.io/en/latest/data/shuffling-data.html));
  no job-level checkpointing in open source; one dead vLLM engine kills a 10M-prompt job
  ([#59522](https://github.com/ray-project/ray/issues/59522)).
- Ray Train restarts the whole worker group on any failure, with `max_failures=0` by default
  ([Train FT](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html)).
- Serve without GCS FT has downtime on head failure; with it, apps cannot be updated while the head recovers
  ([Serve FT](https://docs.ray.io/en/latest/serve/production-guide/fault-tolerance.html)).

## What Ray has fixed (do not overclaim)

Token auth (2.52) on by default locally (2.59); Kubernetes RBAC auth (2.55); cgroup v2 isolation for Ray's own
processes (2.51) and stronger OOM guarantees (2.56); RocksDB GCS (alpha); active-passive GCS leader election
(2.58, release notes only); task events off the GCS (2.58, opt-in); autoscaler v2 default on KubeRay (≥2.47);
label selectors (2.49+); Ray Direct Transport for GPU tensors; Ray Data shuffle v2 and a rate-based autoscaler
(2.60); incremental RayService upgrades (KubeRay 1.5 alpha, 1.7 beta). Still true: owner failure is
unrecoverable, resources are logical, the GCS is not replicated consensus, and there is no multi-tenancy.

## How Ray is deployed, and what hurts

- **VMs via `ray up`:** SSH-driven, flaky head bootstrap ([#15893](https://github.com/ray-project/ray/issues/15893)),
  no HA unless built by hand.
- **KubeRay RayCluster:** HA needs external Redis; logs need sidecars; Uber wrote its own head discovery and an
  idle-cluster sidecar ([Uber](https://www.uber.com/blog/ubers-journey-to-ray-on-kubernetes-ray-setup/));
  Discord's per-team YAML copies drifted ([InfoQ](https://www.infoq.com/news/2025/12/discord-ray/)).
- **RayJob:** a cluster per job, whose TTL deletes the debug data (hence GKE's history server).
- **RayService:** the default upgrade brings up a second full cluster.
- **Slurm:** community-documented; manual port ranges; the repository's most-commented open issue is an `srun`
  registration failure ([#30012](https://github.com/ray-project/ray/issues/30012)).
- **Anyscale** adds proprietary Data checkpointing, faster startup and head-node FT for services.

## Requirements Chimaera takes from this

| Id | Requirement |
|---|---|
| L1 | No process holds unreplicated metadata for objects, actors or gangs it created; creator death never loses them. |
| L2 | Every lifetime (object, actor, gang, run) is a declared policy with a stable id independent of process incarnation. |
| L3 | Losing a minority of a Raft group never stops admission; no worker self-terminates because one endpoint went quiet. |
| L4 | Every ownership change carries a fencing epoch checked on every write; HA never depends on an external database's failover. |
| L5 | No special node; user code never shares a process or memory budget with control-plane state. |
| L6 | Every metadata class has bounded retention, visible as metrics. |
| L7 | Resource requests are enforced (cgroups) and measured per attempt; logical-only is opt-in. |
| L8 | Autoscaling uses queue depth and measured use; a blocked task never makes its node look idle. |
| L9 | Every pending item has a machine-readable reason, queryable without truncation. |
| L10 | Submission is bounded and batched automatically; the documented anti-patterns are corrected or flagged at submit. |
| L11 | Authentication and tenant quotas are on by default in every mode; secrets are never displayed. |
| L12 | Environments are content-addressed and built before admission; client and peers negotiate a versioned protocol. |
| L13 | Gangs and replica groups restart partially and resize elastically; one replica's death degrades capacity. |
| L14 | Pipelines and workflows resume after any process dies, the submitter included. |
| L15 | Upgrades roll in place under version fences, without a second cluster. |
| L16 | Local mode runs the same runtime path as cluster mode. |
