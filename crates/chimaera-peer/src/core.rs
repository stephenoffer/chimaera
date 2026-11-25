//! The peer's scheduling core: one task that owns all dispatch state.
//!
//! Inputs arrive as [`CoreMsg`]: applied shard events (ready, granted, done,
//! actor changes), executor events, session requests, and work other peers
//! placed here. For the groups this peer leads, the core places ready work
//! (here or on another peer) and hands granted attempts to their peer. It runs
//! the attempts placed on this peer within its CPU capacity, fetching the
//! objects they read first, records each attempt's measurements, and publishes
//! outcomes in batches (one proposal per vshard per flush).

use crate::accounting::{self, IsoUsage, Isolation, Reading};
use crate::budget::{self, Budget};
use crate::devices::Want;
use crate::executor::{ExecEnv, ExecEvent, Supervisor};
use crate::mesh::MeshClient;
use crate::profiles::{self, FnKey, Profiles, Request, Source};
use crate::router::{Router, now_ns};
use crate::sharing;
use crate::stores::{CodeStore, ObjectStore, hex};
use chimaera_core::hlc;
use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::profile::{LeakWatch, Sample};
use chimaera_core::shard::{Event, Tk, read};
use chimaera_raft::GroupEvents;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

mod actor_limits;
mod actor_state;
mod demand;
mod holds;
mod shortlist;

type Kind = pb::error_record::Kind;

/// Work behind the head of the queue that may start while the head waits for
/// memory (backfill), and how long the head waits before it is owed the memory.
const BACKFILL: usize = 64;
const HEAD_WAIT: Duration = Duration::from_secs(30);
/// A function never measured runs a few calls at a time until one finishes or
/// has run this long (its memory is then known).
const PROBE_FOR: Duration = Duration::from_secs(2);
/// Free pool memory per call of a not-yet-measured function admitted beyond
/// the base probe width: a first wave of calls up to this size cannot
/// overcommit the pool.
const PROBE_SLOT: u64 = 4 << 30;
/// Memory taken as the least a call (or actor) not yet measured may need, to
/// bound how many probe at once in a small pool: a quarter of 32 CPUs is 8,
/// which a 1 GiB pool cannot hold at 300 MiB each.
const PROBE_MEM: u64 = 512 << 20;
/// Memory pressure (R1): below PAUSE_BELOW of usable memory available (or PSI
/// `some avg10` above PSI_PAUSE) stop admitting until RESUME_ABOVE; below
/// KILL_BELOW stop the attempt furthest over its reservation.
const PAUSE_BELOW: f64 = 0.10;
const RESUME_ABOVE: f64 = 0.15;
const KILL_BELOW: f64 = 0.05;
const PSI_PAUSE: f64 = 20.0;
/// Memory kills this recent veto smaller CPU grants (C3).
const KILLS_RECENT: Duration = Duration::from_secs(60);
const PROFILES_SAVE_EVERY: Duration = Duration::from_secs(30);
/// Objects in a memory-backed store move to disk past this share of the pool
/// (`CHIMAERA_SPILL_THRESHOLD`, default 0.8; Ray's `object_spilling_threshold`).
fn spill_above() -> f64 {
    static AT: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *AT.get_or_init(|| {
        let set = std::env::var("CHIMAERA_SPILL_THRESHOLD").ok().and_then(|v| v.parse::<f64>().ok());
        set.filter(|f| (0.05..=1.0).contains(f)).unwrap_or(0.8)
    })
}
/// Calls queued at one actor before a waiting call's reason suggests
/// declaring concurrency on a method whose calls mostly wait.
const ACTOR_HINT_QUEUE: usize = 16;
const OBJECTS_SCAN_EVERY: Duration = Duration::from_secs(1);
const MIB: u64 = 1 << 20;
/// Profiles sent with one ping reply.
const GOSSIP_MAX: usize = 64;
/// Inputs smaller than this never sway placement.
const LOCALITY_MIN_BYTES: u64 = 1 << 20;
/// Assumed transfer rate from a peer not yet measured (bytes/s).
const DEFAULT_BANDWIDTH: f64 = 200e6;
const FLUSH_AT: usize = 256;
const ATTEMPT_LOG_CAP: usize = 200_000;
/// Tasks (threads) a new executor takes beyond its granted threads: the
/// interpreter, the client's two I/O threads and one spare (measured: 3).
const EXEC_TASKS: u64 = 4;
/// ...and at most this many encoded bytes of them, about 25k recent attempts.
/// A record takes about nine times its encoded size in memory (3 KB: policy,
/// measures, reasons), so the count alone let explain()'s log hold 700 MB,
/// most of a busy peer's heap.
const ATTEMPT_LOG_BYTES: usize = 8 << 20;
/// Events kept per peer for `Events` long polls.
const EVENT_LOG_CAP: usize = 10_000;
/// Queued work waits this long here before it may move to a peer with room.
const SPILL_AFTER: Duration = Duration::from_secs(1);
/// Executors started for a run's environment when it registers.
const PRESTART: usize = 4;
/// Calls waiting on calls they started before the run's driver is told (and at
/// least 4 per CPU): each keeps a worker process.
const NESTED_NOTE_AT: usize = 32;
/// How long a closed actor's process has for its own cleanup before it is
/// stopped outright.
const ACTOR_CLOSE_GRACE: Duration = Duration::from_secs(5);
/// Why an actor placed here has not started while its run's record is read.
const ENV_WAIT: &str = "waiting for its run's environment to reach this peer";
/// How often a run's record is read again for actors waiting on it.
const RUN_FETCH_EVERY: Duration = Duration::from_secs(1);
/// Calls of a one-at-a-time actor sent to its executor in one Execute.
const ACTOR_BATCH: usize = 64;
/// Inputs of one call fetched from other peers at once.
const INPUT_FETCHES: usize = 16;
/// Times one attempt's lost inputs are rebuilt (each rebuild may itself
/// rebuild its own lost inputs) before the attempt fails.
const MAX_REBUILDS: u32 = 3;
/// Publish proposals this peer keeps in flight per vshard. Results finishing
/// meanwhile wait and go in the next one, so under load each commit carries
/// many results instead of one or two (a lone result still goes at once).
const PUBLISH_IN_FLIGHT: u32 = 2;
const IDLE_EXECUTOR_TTL: Duration = Duration::from_secs(60);
/// A running call told to stop that has not stopped by itself (the executor
/// interrupts its main thread after 2 s) loses its executor after this long.
static CANCEL_KILL_AFTER: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();

/// Read the cancel timeouts, once at the peer's start: a running call told to
/// stop is interrupted by its executor after `CHIMAERA_CANCEL_INTERRUPT_SECONDS`
/// (2 s; the executor reads it), and loses its executor after
/// `CHIMAERA_CANCEL_KILL_SECONDS` (5 s). A value that is not a positive number
/// of seconds, or a kill before the interrupt, stops the peer instead.
pub fn configure_cancel() -> Result<(), String> {
    let seconds = |name: &str, unset: f64| match std::env::var(name) {
        Err(_) => Ok(unset),
        Ok(v) => v
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|s| s.is_finite() && *s > 0.0)
            .ok_or_else(|| format!("{name}={v:?}: expected a number of seconds above 0, such as 2.5")),
    };
    let interrupt = seconds("CHIMAERA_CANCEL_INTERRUPT_SECONDS", 2.0)?;
    let kill = seconds("CHIMAERA_CANCEL_KILL_SECONDS", 5.0)?;
    if kill <= interrupt {
        return Err(format!(
            "CHIMAERA_CANCEL_KILL_SECONDS ({kill}) must be longer than CHIMAERA_CANCEL_INTERRUPT_SECONDS \
             ({interrupt}): a cancelled call is interrupted before its worker is killed"
        ));
    }
    let _ = CANCEL_KILL_AFTER.set(Duration::from_secs_f64(kill));
    Ok(())
}

fn cancel_kill_after() -> Duration {
    CANCEL_KILL_AFTER.get().copied().unwrap_or(Duration::from_secs(5))
}

pub enum CoreMsg {
    Raft(GroupEvents),
    Exec(Box<ExecEvent>),
    OpenSession {
        id: u64,
        mailbox: Arc<crate::mailbox::Mailbox>,
        /// The caller's tenant (None: any), which a resume must match.
        tenant: Option<String>,
    },
    BindRun {
        session: u64,
        run: Id,
    },
    CloseSession {
        id: u64,
    },
    /// The session's reader made room for what waited (mailbox.rs).
    SessionRoom {
        id: u64,
    },
    /// Session `id` continues session `old`, whose stream broke.
    ResumeSession {
        id: u64,
        seq: u64,
        old: u64,
        cursor: u64,
    },
    RegisterRun {
        run: Id,
        env: ExecEnv,
        detached: bool,
    },
    Watch {
        session: u64,
        tasks: Vec<Tk>,
    },
    /// Calls a driver sent straight to their actors' executors.
    DirectInflight(Vec<(Tk, crate::direct::Call)>),
    /// Calls the direct path ran: their attempts, and samples to learn from.
    DirectAttempts(Vec<crate::direct::Ran>),
    /// A run's record read from its group for actors here waiting on it
    /// (None: it could not be read; tried again later).
    RunFetched {
        run: Id,
        rec: Option<Box<pb::RunRecord>>,
    },
    Cancel {
        task: Tk,
        force: bool,
    },
    /// The consumer of a generator call has taken items below `up_to`.
    StreamCredit {
        task: Tk,
        up_to: u64,
    },
    /// The session's driver is exiting normally: finish its run.
    FinishSessionRun {
        session: u64,
    },
    /// Finish or cancel a run by id (`Run.finish()` / `Run.cancel()`).
    EndRun {
        run: Id,
        cancel: bool,
    },
    /// End an actor on purpose (never restarted).
    CloseActor {
        actor: Id,
        reason: String,
    },
    Explain {
        req: pb::ExplainRequest,
        reply: oneshot::Sender<pb::ExplainReply>,
    },
    /// Why an actor placed here has not started ("" if it is not waiting).
    ActorWait {
        actor: Id,
        reply: oneshot::Sender<String>,
    },
    /// Result of committing an external-effect attempt's start.
    StartResult {
        work: Box<Work>,
        accepted: bool,
    },
    /// A publish proposal for this vshard was answered (or failed): the
    /// attempts whose outcome the shard took, and when.
    Published {
        v: u16,
        published: Vec<(Tk, u32)>,
        at: u64,
    },
    /// Result of giving back an unstarted attempt (spillback). Refused or
    /// unknown: the work runs here after all.
    GaveBack {
        work: Box<Work>,
        accepted: bool,
    },
    Status {
        reply: oneshot::Sender<pb::StatusReply>,
    },
    /// This peer started or stopped leading a group.
    Leading {
        group: u32,
        leading: bool,
    },
    /// Attempts a group leader placed on this peer, and actor records it hosts.
    Remote {
        work: Vec<pb::RemoteWork>,
        actors: Vec<pb::ActorRecord>,
        actor_env: Option<pb::ExecEnvSpec>,
    },
    /// ch.put values released by their driver: delete them here and on every
    /// peer. Only the session's own run's values: ids show up in explain
    /// output and logs, so another client must not be able to delete them.
    ReleaseObjects {
        session: u64,
        run: Vec<u8>,
        objects: Vec<Vec<u8>>,
    },
    /// A session's Borrow request, counted (leases.rs): the leases to propose.
    Lease {
        session: u64,
        tenant: Option<String>,
        run: Id,
        files: Vec<Vec<u8>>,
        release: bool,
        reply: oneshot::Sender<Result<crate::leases::Leases, String>>,
    },
    /// The objects an attempt reads are now local (or could not be fetched).
    Localized {
        work: Box<Work>,
        error: Option<String>,
    },
    /// An input of `consumer` was lost with every peer that held it: run the
    /// call that made it again here (lineage), then retry `consumer`.
    Rebuild {
        consumer: Box<Work>,
        producer: Tk,
        attempt: u32,
        spec: Box<pb::TaskSpec>,
        deps: Vec<pb::Outcome>,
    },
    /// This peer stopped serving `group` (no longer a member): what it
    /// watched there is watched on the group's members instead.
    GroupDropped {
        group: u32,
    },
    /// A program reads a call's result that was lost with every peer that
    /// held it: run the call again here, then answer `reply` (Ok once the
    /// value is in this peer's store, else why not).
    RebuildRead {
        producer: Tk,
        attempt: u32,
        spec: Box<pb::TaskSpec>,
        deps: Vec<pb::Outcome>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Where a handle's order stands, for an actor host that had not seen it
    /// (see `handle_floor`): `None` while an earlier call is unfinished.
    HandleFloor {
        actor: Id,
        handle: u64,
        floor: Option<(u64, Vec<u64>)>,
    },
    /// An actor's move off this draining peer committed (or failed).
    ActorMoved {
        actor: Id,
        ok: bool,
    },
    /// A call run again by `Rebuild` finished here.
    Rebuilt {
        work: Box<Work>,
        outcome: Option<pb::Outcome>,
    },
    /// A retained result's copies are made (`error` None: publish `entry`,
    /// which names them), or could not be (see replicas).
    Retained {
        entry: Box<pb::PublishEntry>,
        error: Option<String>,
    },
    /// An outcome reported by another peer's replica (remote watch).
    Outcome {
        task: Tk,
        outcome: pb::Outcome,
    },
    /// A remote watch ended without reporting these tasks.
    WatchLost {
        tasks: Vec<Tk>,
    },
    /// Place a ready task again (its actor's host is now known).
    Place {
        task: Tk,
    },
    /// Tasks that ended elsewhere, objects to drop, runs that ended.
    Forget(pb::ForgetRequest),
    /// A call this peer ran in another cell ended (docs/federation.md).
    Delegated {
        work: Box<Work>,
        cell: String,
        result: crate::fleet::Delegated,
    },
    /// Output of calls another peer ran, for runs created here.
    RemoteLogs {
        run: Vec<u8>,
        lines: Vec<pb::LogEvent>,
    },
    /// Load summary for a liveness ping from peer `from` (node id), with the
    /// profiles learned here since that peer last asked.
    Load {
        from: u64,
        reply: oneshot::Sender<pb::PingReply>,
    },
    /// Stop taking new work (the peer is shutting down).
    Drain,
    /// Record an event seen outside the core (membership changes).
    Event(pb::ClusterEvent),
    /// Events with seq >= `since` of `kinds`; with `wait`, answer only once
    /// there is one (the caller bounds the wait).
    Events {
        kinds: Vec<String>,
        since: u64,
        wait: bool,
        reply: oneshot::Sender<Vec<pb::ClusterEvent>>,
    },
    /// `Peer.Hold` for this peer's resource ledger (core/holds.rs).
    Hold {
        req: pb::HoldRequest,
        reply: oneshot::Sender<pb::HoldReply>,
    },
    /// Write learned profiles to disk now (clean shutdown).
    Persist {
        reply: oneshot::Sender<()>,
    },
    /// A durable actor call's commit was answered.
    Committed {
        actor: Id,
        work: Box<Work>,
        ack: Commit,
        reply: Box<pb::Outcome>,
        state: Vec<u8>,
    },
}

/// How a durable actor call's commit ended.
pub enum Commit {
    Answered(Box<pb::ActorAck>),
    /// The call raised: nothing was proposed.
    Raised,
    /// No answer (leader lost, timeout): it may or may not have committed.
    Unknown(String),
}

/// One granted attempt waiting for, or running on, an executor.
#[derive(Clone)]
pub struct Work {
    tk: Tk,
    attempt: u32,
    spec: pb::TaskSpec,
    policy: pb::Policy,
    deps: Vec<pb::Outcome>,
    cpus: f64,
    ready_at: Instant,
    /// False when re-running an actor constructor after a restart: its outcome
    /// is already committed.
    publish: bool,
    /// For external-effect calls: the at-most-once start is committed.
    start_consumed: bool,
    /// Memory reserved, bytes (0 = not known yet: the probe admits it).
    mem: u64,
    /// Memory an earlier attempt was killed at: it needs at least this.
    mem_floor: u64,
    /// Where `cpus` and `mem` came from, for the attempt record.
    cpus_source: String,
    mem_source: String,
    /// In a CPU trial: Some(true) for the canary, Some(false) for the control.
    arm: Option<bool>,
    /// What each source chose and what was granted, the placing peer's
    /// reservation, and calls in its executor round trip (settings.rs).
    cpus_chosen: crate::settings::Chosen,
    mem_chosen: crate::settings::Chosen,
    placer: (f64, u64),
    batched: u32,
    /// The function's median call time when it was reserved; 0 = never measured.
    est_s: f64,
    /// Bytes the call reads: arguments, blobs and dependency values.
    read_bytes: u64,
    /// Spillback was refused for this attempt: it runs here.
    stays: bool,
    /// Lost inputs rebuilt for this attempt so far (see `MAX_REBUILDS`).
    rebuilds: u32,
    /// When it reached this peer (unix ns), read once like every other
    /// stamp: rebuilding it from `ready_at` mixes the monotonic clock with
    /// the wall clock, and the two drift apart by microseconds.
    ready_ns: u64,
    /// When its inputs were all here and it joined the queue (unix ns; 0: not yet).
    queued_ns: u64,
}

/// The call that made `object`, a lost input of `w`, as running it again
/// needs it: its task, the attempt that made the value (the same attempt
/// writes the same object id), its spec and its own inputs' outcomes. Only a
/// plain function or map item without external effects runs again; the
/// error says why another cannot.
async fn rebuild_plan(
    router: &Router,
    mesh: &crate::mesh::MeshClient,
    w: &Work,
    object: &[u8],
) -> Result<(Tk, u32, pb::TaskSpec, Vec<pb::Outcome>), String> {
    let Some(key) = w.spec.deps.iter().zip(&w.deps).find(|(_, o)| made(o, object)).map(|(k, _)| k) else {
        return Err("it is a value the program stored (ch.put, or an argument), not one a call returned".into());
    };
    let producer = Tk::parse(key).ok_or("malformed task key")?;
    let (attempt, spec, deps) = producer_plan(router, mesh, producer, object).await?;
    Ok((producer, attempt, spec, deps))
}

/// Why the call `spec` describes may not run again to remake a value it
/// returned, or None when it may: only a plain function or map item without
/// external effects, declared deterministic (the default), runs again. A
/// function declared `deterministic=False` could return other bytes, which
/// would then be read under the old value's id.
fn replay_refusal(spec: &pb::TaskSpec) -> Option<&'static str> {
    if !matches!(spec.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem) {
        return Some("an actor method made it, and such a call does not run again apart from its actor");
    }
    if spec.effects() == pb::Effects::External {
        return Some("its call has external effects, which run at most once");
    }
    if !pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default().deterministic {
        return Some("its function is declared deterministic=False, so running it again could return another value");
    }
    None
}

/// How to make `object` again by running `producer`, the call that returned
/// it: the attempt that made it (the same attempt writes the same object id),
/// its spec and its own inputs' outcomes. The error says why the call cannot
/// run again (see `replay_refusal`).
pub(crate) async fn producer_plan(
    router: &Router,
    mesh: &crate::mesh::MeshClient,
    producer: Tk,
    object: &[u8],
) -> Result<(u32, pb::TaskSpec, Vec<pb::Outcome>), String> {
    let record = task_record(router, mesh, producer).await?.ok_or("its call's record is gone (its run ended)")?;
    let spec = record.spec.ok_or("its call's record holds no spec")?;
    if let Some(why) = replay_refusal(&spec) {
        return Err(why.into());
    }
    let Some(outcome) = record.outcome.filter(|o| made(o, object)) else {
        return Err("its call's recorded outcome is another value".into());
    };
    if matches!(&outcome.value, Some(pb::outcome::Value::Ok(o)) if o.durability() == pb::Durability::Ephemeral) {
        return Err("it was stored with durability='ephemeral', which is lost with its peer, never made again".into());
    }
    let mut deps = Vec::with_capacity(spec.deps.len());
    for d in &spec.deps {
        let dep = Tk::parse(d).ok_or("malformed task key")?;
        let rec = task_record(router, mesh, dep).await?.ok_or("an input of its call has no record")?;
        deps.push(rec.outcome.ok_or("an input of its call has no outcome")?);
    }
    Ok((outcome.attempt, spec, deps))
}

/// How far back `handle_floor` walks a handle's calls before assuming the
/// rest finished.
const HANDLE_WALK: usize = 64;
/// How often a handle waiting on an earlier, unfinished call is looked up again.
const HANDLE_RECHECK: Duration = Duration::from_millis(500);

/// Where a handle's order stands for an actor host that has not run any of
/// its calls (the actor restarted or moved here): walk back from `prev`, the
/// call before the lowest one waiting. One that finished by running ran in
/// order wherever it was, so every call before it had finished: the handle
/// resumes after it. One cancelled (or past its deadline) may have ended out
/// of order: keep walking. Returns the next seq and the finished seqs above
/// it, or None while a call is unfinished (it reaches this host, or ends).
async fn handle_floor(
    router: &Router,
    mesh: Option<&crate::mesh::MeshClient>,
    mut prev: Tk,
    seq: u64,
) -> Result<Option<(u64, Vec<u64>)>, String> {
    let mut done = Vec::new();
    let mut at = seq.saturating_sub(1); // `prev`'s seq
    for _ in 0..HANDLE_WALK {
        let rec = match mesh {
            Some(mesh) => task_record(router, mesh, prev).await?,
            None => router.read(prev.v(), |kv| read::task(kv, prev)).flatten(),
        };
        // Never committed: nothing to wait for (and nothing earlier to read).
        let Some(rec) = rec else { return Ok(Some((at + 1, done))) };
        let Some(outcome) = rec.outcome else { return Ok(None) };
        let out_of_order = matches!(&outcome.value, Some(pb::outcome::Value::Err(e))
            if matches!(e.kind(), Kind::Cancelled | Kind::Deadline));
        match rec.spec.and_then(|s| s.prev).as_ref().and_then(Tk::parse) {
            Some(earlier) if out_of_order && at > 0 => {
                done.push(at);
                at -= 1;
                prev = earlier;
            }
            _ => return Ok(Some((at + 1, done))),
        }
    }
    Ok(Some((at + 1, done))) // assume the calls before these finished
}

/// Whether `o` is a value stored as `object`: its own file, or one of the
/// blobs its frame links (a result's large buffers, stored apart from it).
fn made(o: &pb::Outcome, object: &[u8]) -> bool {
    matches!(&o.value, Some(pb::outcome::Value::Ok(obj))
        if obj.object_id == object || obj.blobs.iter().any(|b| b.object_id == object))
}

/// `tk`'s record, from this peer's replica of its group or another's.
async fn task_record(
    router: &Router,
    mesh: &crate::mesh::MeshClient,
    tk: Tk,
) -> Result<Option<pb::TaskRecord>, String> {
    if let Some(rec) = router.read(tk.v(), |kv| read::task(kv, tk)) {
        return Ok(rec);
    }
    let req = pb::ReadRequest { tasks: vec![tk.pb()], ..Default::default() };
    let reply = mesh.read_group(router.group_of(tk.v()), req).await.map_err(|e| format!("reading its record: {e}"))?;
    Ok(reply.tasks.into_iter().next().filter(|r| r.spec.is_some()))
}

/// Whether the shard took a give-back (spillback) of an unstarted attempt, so
/// this peer must not run it. Existing: the task already has its outcome.
/// Anything else (refused, fenced, unknown, or no answer) leaves it here.
fn give_back_taken<E>(reply: &Result<pb::ShardReply, E>) -> bool {
    use pb::task_ack::Status::{Accepted, Existing};
    match reply {
        Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(t)) }) => {
            t.acks.first().is_some_and(|a| matches!(a.status(), Accepted | Existing))
        }
        _ => false,
    }
}

impl Work {
    fn new(tk: Tk, attempt: u32, spec: pb::TaskSpec, policy: pb::Policy, deps: Vec<pb::Outcome>) -> Work {
        let cpus = if policy.cpus > 0.0 { policy.cpus } else { 1.0 };
        Work {
            tk,
            attempt,
            spec,
            policy,
            deps,
            cpus,
            ready_at: Instant::now(),
            publish: true,
            start_consumed: false,
            mem: 0,
            mem_floor: 0,
            cpus_source: "default".into(),
            mem_source: "default".into(),
            arm: None,
            cpus_chosen: Default::default(),
            mem_chosen: Default::default(),
            placer: (0.0, 0),
            batched: 1,
            est_s: 0.0,
            read_bytes: 0,
            stays: false,
            rebuilds: 0,
            ready_ns: now_ns(),
            queued_ns: 0,
        }
    }

    fn kind(&self) -> pb::CallKind {
        self.spec.kind()
    }

    fn key(&self) -> FnKey {
        profile_key(&self.spec, &self.policy)
    }

    /// Runs inside an actor's process and grant.
    fn in_actor(&self) -> bool {
        matches!(self.kind(), pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod)
    }

    /// The settings it runs with (AttemptRecord.settings).
    fn settings(&self, capacity: f64) -> Vec<pb::Setting> {
        crate::settings::of(
            &self.policy,
            self.cpus_chosen,
            self.mem_chosen,
            self.mem_floor,
            self.placer,
            capacity,
            self.batched,
        )
    }

    /// CPU and memory reserved, and where each came from.
    fn reservation(&self) -> String {
        let mem = if self.mem > 0 { format!("{} MiB", self.mem.div_ceil(MIB)) } else { "not known yet".into() };
        format!("cpus {} ({}); memory {mem} ({})", self.cpus, self.cpus_source, self.mem_source)
    }

    /// Memory to learn from the function's profile: no value was set, none
    /// is known yet, and learning is on.
    fn learns_memory(&self) -> bool {
        // An initial value (ch.auto(initial=...)) holds only until there is an estimate.
        let unlearned =
            self.mem == 0 || (self.policy.memory_initial && self.mem == self.policy.memory && self.mem_floor == 0);
        unlearned
            && (self.policy.memory == 0 || self.policy.memory_initial)
            && self.policy.adaptive() == pb::policy::Adaptive::On
    }

    /// Fast calls of one function that can share an executor round trip
    /// (granularity, A1).
    fn batchable(&self) -> bool {
        match self.kind() {
            pb::CallKind::CallMapItem => !self.policy.interactive,
            pb::CallKind::CallFunction => {
                !self.policy.interactive
                    && self.spec.deps.is_empty()
                    && self.spec.effects() != pb::Effects::External
                    && self.arm.is_none()
            }
            _ => false,
        }
    }

    /// May run as a guest beside waiting calls (sharing): a plain call with no
    /// external effects, no memory limit (a kill for memory takes the whole
    /// process, its host included, and the call's retries would do it again),
    /// and not a gang's (a rank sets process-wide variables and torch's
    /// per-process group). shortcut: gang calls are known by their module's
    /// name; give Policy a flag if `chimaera._gang` moves.
    fn guest_ok(&self) -> bool {
        matches!(self.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem)
            && self.spec.effects() != pb::Effects::External
            && self.policy.memory_limit == 0
            && !self.policy.fn_name.starts_with("chimaera._gang.")
    }

    fn batches_with(&self, other: &Work) -> bool {
        other.batchable()
            && other.kind() == self.kind()
            && other.spec.fn_digest == self.spec.fn_digest
            && other.tk.run == self.tk.run
            && other.policy == self.policy
            && other.spec.retry_exception_types == self.spec.retry_exception_types
            // A batch runs at its head's grant and each call is recorded
            // under its own trial arm: never mix arms or grants.
            && other.arm == self.arm
            && other.cpus == self.cpus
    }
}

/// The profile a call's measurements belong to: for a map chunk, the function
/// it runs over its items; otherwise the code it was submitted with.
/// `obj` and the files its frame refers to (`blobs`, a result's large buffers
/// stored apart), each naming where it is: a blob without locations is where
/// its object is.
/// Fetch a call's inputs into the store, INPUT_FETCHES at once: a call reading
/// many small values (a shuffle's gather reads one per scatter) would otherwise
/// pay one round trip per input, one after another. On failure, the first input
/// that could not be fetched, and why.
async fn fetch_inputs(
    mesh: &Arc<MeshClient>,
    store: &ObjectStore,
    objs: Vec<pb::ObjectRef>,
) -> Result<(), (Vec<u8>, String)> {
    use futures_util::StreamExt;
    let fetches = objs.into_iter().map(|obj| async move {
        let holders = mesh.holders(&obj);
        mesh.localize(store, &obj.run, &obj.object_id, obj.size, &holders, pb::TrafficClass::TrafficForeground)
            .await
            .map_err(|e| (obj.object_id.clone(), format!("cannot fetch input object {}: {e}", hex(&obj.object_id))))
    });
    let mut fetches = futures_util::stream::iter(fetches).buffer_unordered(INPUT_FETCHES);
    while let Some(r) = fetches.next().await {
        r?;
    }
    Ok(())
}

fn with_blobs(obj: &pb::ObjectRef) -> Vec<pb::ObjectRef> {
    let mut all = Vec::with_capacity(1 + obj.blobs.len());
    for b in &obj.blobs {
        let mut b = b.clone();
        if b.locations.is_empty() {
            b.locations = obj.locations.clone();
        }
        all.push(b);
    }
    let mut head = obj.clone();
    head.blobs.clear();
    all.insert(0, head);
    all
}

/// Whether to read a run's record again: none in flight, and the last read
/// asked at least RUN_FETCH_EVERY ago.
fn run_fetch_due(last: Option<&(bool, Instant)>, now: Instant) -> bool {
    last.is_none_or(|(in_flight, at)| !*in_flight && now.duration_since(*at) >= RUN_FETCH_EVERY)
}

/// Why no more workers start: the task limit (a pod's pids.max) is reached.
fn pids_full(current: u64, max: u64) -> String {
    let at = if max > 0 { format!(" (cgroup pids.max {max}, {current} in use)") } else { String::new() };
    format!(
        "the task limit{at} has no room for another worker, so calls wait for one to finish; raise it \
         (docker --pids-limit, Kubernetes podPidsLimit) or give calls fewer CPUs (each thread counts)"
    )
}

pub(crate) fn profile_key(spec: &pb::TaskSpec, policy: &pb::Policy) -> FnKey {
    let digest = if policy.profile_digest.is_empty() { &spec.fn_digest } else { &policy.profile_digest };
    FnKey::new(digest, &spec.method)
}

/// What a call's placement weighs besides load (see `Core::extra_cost`).
struct PlaceCost {
    inputs: Vec<pb::ObjectRef>,
    /// The live peers holding each input (as `inputs`), looked up once per
    /// placement rather than once per candidate peer; empty below
    /// LOCALITY_MIN_BYTES, where locality is not weighed.
    held: Vec<Vec<u64>>,
    total: u64,
    est_s: f64,
    need: u64,
    /// Peers holding the largest input.
    holders: Vec<u64>,
}

/// An attempt's private memory peak and how it was measured: its cgroup's
/// peak when isolated, else the largest of the 100 ms samples and the reading
/// at the end (on macOS also the process's lifetime peak, when this attempt
/// raised it).
#[allow(clippy::too_many_arguments)]
fn attempt_peak(
    sampled: u64,
    end_private: Option<u64>,
    start: Option<Reading>,
    end: Option<Reading>,
    iso: Option<&IsoUsage>,
    hwm_reset: bool,
    mapped_inputs: u64,
) -> Option<(u64, &'static str, &'static str)> {
    if let Some(u) = iso {
        return u.memory_peak.map(|p| (p, "attempt", "cgroup"));
    }
    let mut peak = sampled.max(end_private.unwrap_or(0));
    // A call shorter than the 100 ms sampling is caught by the process's
    // peak: on Linux the high-water mark reset when the attempt started (less
    // its mapped inputs, which are shared, not private); on macOS the
    // lifetime peak of the physical footprint when this attempt raised it.
    if let Some(b) = end {
        if hwm_reset {
            let hwm = b.peak_rss_bytes.saturating_sub(mapped_inputs);
            if hwm > peak {
                return Some((hwm, "attempt", "vmhwm"));
            }
        } else if cfg!(target_os = "macos")
            && let Some(a) = start
            && b.peak_rss_bytes > a.peak_rss_bytes
        {
            peak = peak.max(b.peak_rss_bytes);
        }
    }
    (peak > 0).then_some((peak, "sampled", "sampled"))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ExecState {
    Starting,
    Idle,
    Busy,
}

struct Exec {
    pid: u32,
    env_key: String,
    /// CPU an actor executor holds for its lifetime (0 for pooled executors,
    /// whose calls are charged one by one).
    held_cpus: f64,
    /// Calls that may run at once on this executor (actors with concurrency).
    concurrency: usize,
    /// The attempt's own cgroup, for an executor dedicated to one isolated attempt.
    cgroup: Option<std::path::PathBuf>,
    state: ExecState,
    actor: Option<Id>,
    idle_since: Instant,
    /// Set when the peer killed it for exceeding its memory limit.
    killed_for_memory: Option<(u64, u64)>,
    /// Why the peer killed it for memory, when not for its own limit.
    kill_note: Option<String>,
    /// Private memory at the last sample, bytes.
    private: u64,
    /// Its NumPy arena's files (Linux: arrays allocated as tmpfs files under
    /// `<objects>/.arena/<pid>`, which are not private memory), bytes.
    arena: u64,
    /// Memory between calls, to replace a leaking executor (R4).
    leak: LeakWatch,
    /// The function and run of its last call: an idle executor that already
    /// loaded a function's code (and its imports) is preferred for it.
    last_fn: Vec<u8>,
    last_run: Option<Id>,
    /// Process CPU at the last change while it held several calls (sharing).
    cpu_mark: u64,
    /// A memory kill of a process holding several calls, for this one's
    /// memory: the others are retried as infra failures, with no memory hint.
    killed_for: Option<Tk>,
}

struct Running {
    exec: u32,
    items: Vec<Work>,
    /// Which of `items` the executor reported finished (the batch stays
    /// until the last one does).
    finished: Vec<bool>,
    remaining: usize,
    cpus: f64,
    blocked: bool,
    start: Option<Reading>,
    dispatched_ns: u64,
    /// Memory reserved for these items (the largest item's).
    mem: u64,
    /// Largest private memory sampled while they ran.
    peak: u64,
    started: Instant,
    /// The executor's peak-RSS mark was reset when these started (Linux).
    hwm_reset: bool,
    /// Calls of this batch that finished before its CPU could be measured
    /// (that takes the whole batch): learned from when it ends.
    unlearned: Vec<(Work, pb::ExecDone, u64)>,
    /// Its latest wait offered to host a guest, and what it waits on (sharing).
    host: bool,
    waiting_on: Vec<Tk>,
    /// Once its process held another call beside it, its CPU is measured in
    /// slices (`cpu_own`: None when two ran at once), and its memory from
    /// `baseline`, what the process held when it started (0 if alone).
    sliced: bool,
    cpu_own: Option<u64>,
    baseline: u64,
}

#[derive(Default)]
struct ActorHost {
    generation: u64,
    exec: Option<u32>,
    /// Its process has not started: live actors and running calls fill the
    /// memory pool (see start_actor).
    waiting_memory: Option<String>,
    /// When its current process was started.
    started: Option<Instant>,
    run: Option<Id>,
    /// Constructor finished on the current executor.
    constructed: bool,
    ctor: Option<Work>,
    /// Calls waiting to start, by (handle, seq): in through actor_limits::enqueue.
    mailbox: BTreeMap<(u64, u64), Work>,
    next_seq: HashMap<u64, u64>,
    done_seqs: HashMap<u64, BTreeSet<u64>>,
    inflight: BTreeSet<(u64, u64)>,
    dead: bool,
    /// Generation an ActorPlace was proposed for (by this peer as leader).
    placing: Option<u64>,
    restart: pb::actor_spec::Restart,
    detached: bool,
    /// Durable actor: its committed state and version, and the class digest
    /// to rebuild an instance from them.
    durable: bool,
    state_version: u64,
    durable_state: Vec<u8>,
    class_digest: Vec<u8>,
    /// A call's commit is in flight: the next call waits for it.
    committing: bool,
    /// Handles whose place in their order this host knows: it ran one of
    /// their calls, or learned it (`handle_floor`). Another handle's calls
    /// may follow calls that ran on another host.
    known_handles: HashSet<u64>,
    /// Handles being looked up (`handle_floor`), and when last asked.
    resolving: HashMap<u64, Instant>,
    /// This peer drains: the actor starts no new call here and moves once
    /// its running ones finish (`try_move_actor`).
    moving: bool,
    /// It moved away: calls that reach this peer are given back to the shard,
    /// which grants them again at its new host.
    moved: bool,
    /// Per-method limits on its calls (ActorSpec.methods; actor_limits), and
    /// its mailbox methods' calls queued here in the order they take room.
    limits: Vec<pb::MethodLimit>,
    waiting: actor_limits::Waiting,
    /// Its record names a newer class than its instance was built from: the
    /// instance is rebuilt from the committed state between calls (actor_state).
    upgrade: bool,
    /// The state version its record's class writes (ActorSpec.state_schema).
    state_schema: u32,
    /// The digests of its committed state's chunks, when it is that large.
    state_chunks: Vec<Vec<u8>>,
}

struct RunInfo {
    env: ExecEnv,
    detached: bool,
    sessions: BTreeSet<u64>,
}

use crate::mailbox::Session;

pub struct Core {
    me_tx: mpsc::Sender<CoreMsg>,
    router: Arc<Router>,
    sup: Arc<Supervisor>,
    objects: ObjectStore,
    code: CodeStore,
    capacity: f64,
    cpu_used: f64,
    queue: crate::keyed_queue::KeyedQueue<Work, Tk>,
    /// Items still running per task, so a finished call heard of (most ran
    /// elsewhere, or already finished here) costs a lookup, not a scan.
    unfinished: HashMap<Tk, u32>,
    /// Items per task in `running`, finished or not (a batch keeps its items
    /// until its last one finishes): the checks made for every finished call
    /// look into the batches only for a task with another item there.
    batched: HashMap<Tk, u32>,
    execs: HashMap<u32, Exec>,
    running: Vec<Running>,
    /// Calls told to stop while running: (executor, call, when).
    cancelling: Vec<(u32, Tk, Instant)>,
    actors: HashMap<Id, ActorHost>,
    sessions: HashMap<u64, Session>,
    /// Resumes waiting for the old session's stream to end here: old -> (new, seq, cursor).
    resuming: HashMap<u64, (u64, u64, u64)>,
    /// Sessions resumed into another, by the session now holding them, for a
    /// client that did not hear the resume's answer and names the old one again.
    resumed_as: HashMap<u64, u64>,
    /// Files of other runs sessions hold (leases.rs).
    borrows: crate::leases::Borrows,
    watchers: HashMap<Tk, Vec<u64>>,
    runs: HashMap<Id, RunInfo>,
    pending_grants: BTreeMap<u16, Vec<pb::Grant>>,
    pending_publish: BTreeMap<u16, Vec<pb::PublishEntry>>,
    /// Publish proposals in flight, per vshard (see PUBLISH_IN_FLIGHT).
    publishing: BTreeMap<u16, u32>,
    attempts: HashMap<Tk, Vec<pb::AttemptRecord>>,
    /// Encoded size of everything in `attempts`.
    attempt_bytes: usize,
    /// Calls drivers sent straight to actors here, until their records land.
    direct: crate::direct::Inflight,
    /// Actors placed here whose process waits for memory (see start_actor).
    actors_waiting_memory: VecDeque<(Id, Option<Id>, pb::ActorSpec)>,
    /// Actors placed here whose run's record (its environment) this peer
    /// does not hold yet, oldest first (see start_actor).
    actors_waiting_env: VecDeque<(Id, Option<Id>, pb::ActorSpec)>,
    /// Runs whose record is being read from their group: whether a read is in
    /// flight, and when the last one was asked (see fetch_run).
    run_fetches: HashMap<Id, (bool, Instant)>,
    /// Runs told that many of their calls wait on calls they started (see note_nested_waits).
    nested_noted: VecDeque<Id>,
    /// What waiting calls of one-at-a-time actors wait for, by (executor, call) (see note_actor_wait).
    actor_waits: HashMap<(u32, Tk), Vec<Tk>>,
    /// Why no more executors start: the cgroup task limit is reached (see
    /// topology::pids_room). Shown as the wait reason; cleared by a start.
    pids_note: Option<String>,
    /// Workers that exit before they connect (see start_failures).
    start_failures: crate::start_failures::StartFailures,
    attempt_order: VecDeque<Tk>,
    profiles: Profiles,
    profiles_path: PathBuf,
    profiles_saved: Instant,
    budget: Budget,
    /// Memory the OS can hand out now (last sample).
    available: Option<u64>,
    /// Bytes of objects in the pool (tmpfs only), rescanned every second.
    object_bytes: u64,
    objects_scanned: Instant,
    /// (free, total) bytes of the objects' filesystem, at the last scan.
    disk_space: Option<(u64, u64)>,
    /// Admission paused by memory pressure.
    mem_paused: bool,
    /// When this peer last killed attempts for memory.
    mem_kills: VecDeque<Instant>,
    /// Functions without an estimate yet: the largest peak of a finished
    /// call, or the most private memory a call running past the probe held.
    live_floor: HashMap<FnKey, u64>,
    /// Ids of ch.put values their drivers released (bounded).
    released: HashSet<Vec<u8>>,
    epoch: Instant,
    /// Per peer (node id): the profile version last sent to it.
    gossip_cursor: HashMap<u64, u64>,
    /// A spill to disk is running.
    spilling: Arc<std::sync::atomic::AtomicBool>,
    decisions: VecDeque<pb::Decision>,
    peer_socket: String,
    isolation: Option<Isolation>,
    iso_seq: u64,
    /// Isolated attempts waiting for their dedicated executor to connect.
    iso_waiting: HashMap<u32, Work>,
    /// Recent grant-to-handoff latencies (µs) of attempts that found a warm
    /// executor waiting, for the dispatch-latency gate.
    dispatch_us: VecDeque<f64>,
    /// Recent events (bounded) and long-poll waiters for them.
    events: VecDeque<pb::ClusterEvent>,
    event_seq: u64,
    event_waiters: Vec<(Vec<String>, u64, oneshot::Sender<Vec<pb::ClusterEvent>>)>,
    /// Last (generation, state) reported per actor, so updates that change
    /// neither are not events.
    actor_seen: HashMap<Id, (u64, i32)>,
    /// Attempts granted to other peers, sent with the next flush (by node id).
    pending_dispatch: BTreeMap<u64, Vec<pb::RemoteWork>>,
    /// Attempts waiting for their objects to be fetched.
    localizing: HashSet<(Tk, u32)>,
    /// Calls being run again here because a value they made was lost, and
    /// the attempts waiting for that value.
    rebuilding: HashMap<Tk, Vec<Work>>,
    /// Programs waiting for a call to be run again (`RebuildRead`), by call.
    rebuild_readers: HashMap<Tk, Vec<oneshot::Sender<Result<(), String>>>>,
    /// Tasks watched through another peer's replica.
    remote_watched: HashSet<Tk>,
    /// Hosts of actors whose records live in groups not hosted here.
    actor_hosts: HashMap<Id, pb::PeerInc>,
    /// Ready actor calls waiting for their actor to be placed.
    waiting_actor: HashMap<Id, Vec<Tk>>,
    /// Shutting down: place nothing new here.
    draining: bool,
    /// Where this peer, as a leader, placed each unfinished task (node id).
    /// Load reports lag by a ping interval and a burst of ready tasks is
    /// placed before any reaches a queue, so placement counts these too.
    placed_on: HashMap<Tk, u64>,
    outstanding: HashMap<u64, u32>,
    /// Per node: when this leader's tasks there finished (last few seconds).
    /// A load report overstates the load by those that finished after it.
    finished_at: HashMap<u64, VecDeque<Instant>>,
    /// Proposals sent and not yet answered (a draining peer waits for them).
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    /// Accelerators, and which executors hold them (`gpus=`, `device=`).
    devices: crate::devices::Devices,
    /// Resources held for gang generations and reservations (core/holds.rs).
    ledger: holds::Ledger,
    /// Each run's output on this peer (`chimaera logs`).
    run_logs: crate::runlogs::RunLogs,
    /// Output bound for runs whose driver is on another peer.
    log_ship: crate::logship::LogShip,
    /// Lines several workers of a run print at once, collapsed for its drivers.
    log_dedup: crate::logdedup::LogDedup,
    /// Per-tenant CPU quotas on this peer (CHIMAERA_TENANT_QUOTAS).
    quotas: crate::quotas::Quotas,
    budgets: crate::budgets::Budgets,
    /// Other cells this peer may run calls in when its cell is full; None without a fleet.
    fleet: Option<crate::fleet::Fleet>,
    /// Actors waiting for a peer with their CPUs free (placed on a later tick).
    actors_waiting: BTreeSet<Id>,
    /// Those actors' CPUs and when each was last seen waiting (core/demand.rs).
    actor_demand: BTreeMap<Id, (f64, Instant)>,
    /// Attempts started here, and their cores × estimated seconds (core/demand.rs).
    started: (u64, f64),
    /// CPUs of actors just placed (actor, peer, cpus, when), counted as used
    /// until the peers' own numbers include them.
    actor_holds: Vec<(Id, Vec<u8>, f64, Instant)>,
    /// Actors this leader placed lately, by host peer (whatever CPUs they
    /// asked for): a burst of actors that ask for none must still spread.
    actor_recent: VecDeque<(Vec<u8>, Instant)>,
}

/// Where an actor goes: a peer with its CPUs free now, wait until one has
/// them, or nowhere (no live peer could ever hold it).
enum ActorPlacement {
    Host(pb::PeerInc),
    Wait,
    Infeasible,
}

/// How long a placed actor's CPUs count against its peer before that peer's
/// own load numbers include them.
const ACTOR_HOLD_TTL: Duration = Duration::from_secs(3);
/// How long a placement counts toward a host's load (until reports show it).
/// How long a remote watch may stay silent before it is placed again: a
/// replica that lost its seat keeps the stream open but applies nothing.
const WATCH_QUIET: Duration = Duration::from_secs(5);
const ACTOR_RECENT_TTL: Duration = Duration::from_secs(10);

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        me_tx: mpsc::Sender<CoreMsg>,
        router: Arc<Router>,
        sup: Arc<Supervisor>,
        objects: ObjectStore,
        code: CodeStore,
        capacity: f64,
        peer_socket: String,
        isolation: Option<Isolation>,
        budget: Budget,
        profiles: Profiles,
        profiles_path: PathBuf,
    ) -> Core {
        Core {
            me_tx,
            devices: crate::devices::Devices::discover(),
            ledger: Default::default(),
            run_logs: Default::default(),
            log_ship: Default::default(),
            log_dedup: Default::default(),
            // Validated before the core starts (runtime::serve).
            quotas: crate::quotas::Quotas::from_env().unwrap_or_default(),
            budgets: Default::default(),
            fleet: crate::fleet::Fleet::from_env().unwrap_or_else(|e| {
                tracing::warn!("no fleet: {e}");
                None
            }),
            actors_waiting: BTreeSet::new(),
            actor_demand: BTreeMap::new(),
            started: (0, 0.0),
            actor_holds: Vec::new(),
            actor_recent: VecDeque::new(),
            router,
            sup,
            objects,
            code,
            capacity,
            cpu_used: 0.0,
            queue: crate::keyed_queue::KeyedQueue::new(|w: &Work| w.tk),
            unfinished: HashMap::new(),
            batched: HashMap::new(),
            execs: HashMap::new(),
            running: Vec::new(),
            cancelling: Vec::new(),
            actors: HashMap::new(),
            sessions: HashMap::new(),
            resuming: HashMap::new(),
            resumed_as: HashMap::new(),
            borrows: Default::default(),
            watchers: HashMap::new(),
            runs: HashMap::new(),
            pending_grants: BTreeMap::new(),
            pending_publish: BTreeMap::new(),
            publishing: BTreeMap::new(),
            attempts: HashMap::new(),
            attempt_bytes: 0,
            direct: Default::default(),
            actors_waiting_memory: VecDeque::new(),
            actors_waiting_env: VecDeque::new(),
            run_fetches: HashMap::new(),
            nested_noted: VecDeque::new(),
            actor_waits: HashMap::new(),
            pids_note: None,
            start_failures: Default::default(),
            attempt_order: VecDeque::new(),
            profiles,
            profiles_path,
            profiles_saved: Instant::now(),
            budget,
            available: None,
            object_bytes: 0,
            objects_scanned: Instant::now() - OBJECTS_SCAN_EVERY,
            disk_space: None,
            mem_paused: false,
            mem_kills: VecDeque::new(),
            live_floor: HashMap::new(),
            released: Default::default(),
            epoch: Instant::now(),
            gossip_cursor: HashMap::new(),
            spilling: Arc::default(),
            decisions: VecDeque::new(),
            peer_socket,
            isolation,
            iso_seq: 0,
            iso_waiting: HashMap::new(),
            dispatch_us: VecDeque::new(),
            events: VecDeque::new(),
            event_seq: 0,
            event_waiters: Vec::new(),
            actor_seen: HashMap::new(),
            pending_dispatch: BTreeMap::new(),
            localizing: HashSet::new(),
            rebuilding: HashMap::new(),
            rebuild_readers: HashMap::new(),
            remote_watched: HashSet::new(),
            actor_hosts: HashMap::new(),
            waiting_actor: HashMap::new(),
            draining: false,
            placed_on: HashMap::new(),
            outstanding: HashMap::new(),
            finished_at: HashMap::new(),
            in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<CoreMsg>) {
        // A new leader resumes work that was ready but never granted.
        for g in self.router.leading() {
            self.resume(g);
        }
        if let Some(f) = &self.fleet {
            f.spawn_summaries(self.router.clone());
        }
        let mut house = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(m) => {
                        self.handle(m);
                        // Handle everything already waiting, then propose the
                        // grants and results it produced together: a burst
                        // becomes one proposal per vshard, and a lone call is
                        // not held back by a timer.
                        let mut drained = 0;
                        while drained < FLUSH_AT {
                            match rx.try_recv() {
                                Ok(m) => self.handle(m),
                                Err(_) => break,
                            }
                            drained += 1;
                        }
                        self.flush();
                    }
                    None => {
                        self.save_profiles();
                        return;
                    }
                },
                _ = house.tick() => {
                    self.housekeeping();
                    self.flush();
                }
            }
        }
    }

    fn me(&self) -> (Id, u64) {
        (self.router.identity.peer, self.router.identity.incarnation)
    }

    fn propose(&self, vshard: u16, body: pb::shard_cmd::Body) {
        use std::sync::atomic::Ordering::Relaxed;
        let p = self.router.clone();
        let in_flight = self.in_flight.clone();
        in_flight.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            if let Err(e) = p.propose(vshard, body).await {
                tracing::warn!("proposal failed: {e}");
            }
            in_flight.fetch_sub(1, Relaxed);
        });
    }

    /// Propose results; the answer reopens the vshard's publish window.
    fn propose_publish(&self, v: u16, entries: Vec<pb::PublishEntry>) {
        use std::sync::atomic::Ordering::Relaxed;
        let (router, tx, in_flight) = (self.router.clone(), self.me_tx.clone(), self.in_flight.clone());
        in_flight.fetch_add(1, Relaxed);
        let keys: Vec<(Option<Tk>, u32)> =
            entries.iter().map(|e| (e.task.as_ref().and_then(Tk::parse), e.attempt)).collect();
        tokio::spawn(async move {
            let reply = router.propose(v, pb::shard_cmd::Body::Publish(pb::PublishResults { entries })).await;
            let at = now_ns();
            let published = match reply {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(t)) }) => keys
                    .into_iter()
                    .zip(t.acks)
                    .filter(|(_, a)| a.status() == pb::task_ack::Status::Published)
                    .filter_map(|((tk, attempt), _)| Some((tk?, attempt)))
                    .collect(),
                Ok(_) => Vec::new(),
                Err(e) => {
                    tracing::warn!("publish proposal failed: {e}");
                    Vec::new()
                }
            };
            in_flight.fetch_sub(1, Relaxed);
            let _ = tx.send(CoreMsg::Published { v, published, at }).await;
        });
    }

    /// Propose grants; a task whose grant was refused while it is still ready
    /// (its peer was retired meanwhile, or the proposal failed) is placed
    /// again, since no new event would ever mention it.
    fn propose_grants(&self, v: u16, grants: Vec<pb::Grant>) {
        use std::sync::atomic::Ordering::Relaxed;
        let (router, tx, in_flight) = (self.router.clone(), self.me_tx.clone(), self.in_flight.clone());
        let local = !router.is_cluster();
        in_flight.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let tasks: Vec<Tk> = grants.iter().filter_map(|g| g.task.as_ref().and_then(Tk::parse)).collect();
            let again: Vec<Tk> = match router.propose(v, pb::shard_cmd::Body::Grant(pb::GrantAttempts { grants })).await
            {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(t)) }) => t
                    .acks
                    .iter()
                    .filter(|a| {
                        a.status() == pb::task_ack::Status::Denied && a.state() == pb::task_record::State::Ready
                    })
                    .filter_map(|a| a.task.as_ref().and_then(Tk::parse))
                    .collect(),
                Ok(_) => Vec::new(),
                Err(e) => {
                    tracing::warn!("grant proposal failed: {e}");
                    tasks
                }
            };
            in_flight.fetch_sub(1, Relaxed);
            if again.is_empty() || local {
                return;
            }
            // Give liveness a moment to catch up with whatever refused them.
            tokio::time::sleep(Duration::from_millis(50)).await;
            for task in again {
                let _ = tx.send(CoreMsg::Place { task }).await;
            }
        });
    }

    fn flush(&mut self) {
        // Room for local-first grants at ingress: idle CPUs not already spoken
        // for by work queued here.
        if !self.draining {
            self.router.set_local_room((self.capacity - self.cpu_used).floor() as i64 - self.queue.len() as i64);
        } else {
            self.router.set_local_room(0);
        }
        for (v, grants) in std::mem::take(&mut self.pending_grants) {
            self.propose_grants(v, grants);
        }
        let open: Vec<u16> = self
            .pending_publish
            .keys()
            .copied()
            .filter(|v| self.publishing.get(v).copied().unwrap_or(0) < PUBLISH_IN_FLIGHT)
            .collect();
        for v in open {
            let entries = self.pending_publish.remove(&v).expect("listed above");
            *self.publishing.entry(v).or_default() += 1;
            self.propose_publish(v, entries);
        }
        let Some(mesh) = self.router.mesh.clone() else { return };
        for (node, work) in std::mem::take(&mut self.pending_dispatch) {
            let mesh = mesh.clone();
            tokio::spawn(async move {
                let req = pb::DispatchRequest { cluster: mesh.cluster.clone(), work, ..Default::default() };
                // A lost dispatch is recovered when the peer is retired (its
                // grants are requeued) or when a new leader re-sends grants.
                if let Err(e) = mesh.dispatch(node, req).await {
                    tracing::warn!("dispatch to node {node} failed: {e}");
                }
            });
        }
    }

    fn handle(&mut self, msg: CoreMsg) {
        match msg {
            CoreMsg::Raft(ev) => {
                for e in ev.events {
                    self.on_event(ev.group, e);
                }
                if ev.reloaded {
                    self.recheck_watches(ev.group);
                }
                self.dispatch();
            }
            CoreMsg::Leading { group, leading } => {
                self.router.set_leading(group, leading);
                if leading {
                    self.resume(group);
                } else {
                    // Another peer places this group's work now.
                    let gone: Vec<Tk> =
                        self.placed_on.keys().filter(|t| self.router.group_of(t.v()) == group).copied().collect();
                    for tk in gone {
                        self.unplace(tk);
                    }
                    if self.router.hosted(group).is_none() {
                        self.watch_elsewhere(group);
                    }
                }
            }
            CoreMsg::Remote { work, actors, actor_env } => {
                for rec in actors {
                    self.on_actor_record(rec, actor_env.clone());
                }
                for rw in work {
                    self.accept_remote(rw);
                }
                self.dispatch();
            }
            CoreMsg::Lease { session, tenant, run, files, release, reply } => {
                let owner = self.sessions.get(&session).map(|s| &s.tenant);
                let _ = reply.send(self.borrows.request(owner, &tenant, session, run, &files, release));
            }
            CoreMsg::ReleaseObjects { session, run, objects } => {
                let own = self.sessions.get(&session).and_then(|s| s.run).is_some_and(|r| r.0[..] == run[..]);
                if own {
                    let refs: Vec<pb::ObjectRef> = objects
                        .into_iter()
                        .map(|object_id| pb::ObjectRef { run: run.clone(), object_id, ..Default::default() })
                        .collect();
                    self.forget_objects(&refs);
                    self.broadcast_forget(pb::ForgetRequest { objects: refs, ..Default::default() });
                } else {
                    tracing::warn!(session, "ignored a release of objects of a run this session does not own");
                }
            }
            CoreMsg::Localized { work, error } => {
                self.localizing.remove(&(work.tk, work.attempt));
                match error {
                    None => self.enqueue(*work),
                    Some(why) => {
                        let why = self.released_input(&work).unwrap_or(why);
                        self.give_up(&work, &why)
                    }
                }
                self.dispatch();
            }
            CoreMsg::HandleFloor { actor, handle, floor } => self.on_handle_floor(actor, handle, floor),
            CoreMsg::ActorMoved { actor, ok } => self.on_actor_moved(actor, ok),
            CoreMsg::Retained { entry, error } => self.on_retained(*entry, error),
            CoreMsg::Rebuilt { work, outcome } => {
                self.rebuilt(&work, outcome.as_ref());
                self.dispatch();
            }
            CoreMsg::Rebuild { consumer, producer, attempt, spec, deps } => {
                self.localizing.remove(&(consumer.tk, consumer.attempt));
                let mut consumer = *consumer;
                consumer.rebuilds += 1;
                let depth = consumer.rebuilds;
                let running = self.rebuild_readers.contains_key(&producer);
                let waiting = self.rebuilding.entry(producer).or_default();
                waiting.push(consumer);
                if waiting.len() == 1 && !running {
                    tracing::info!(
                        task = %producer.task.hex(),
                        "an input was lost with every peer that held it: running the call that made it again"
                    );
                    self.start_rebuild(producer, attempt, *spec, deps, depth);
                }
                self.dispatch();
            }
            CoreMsg::RebuildRead { producer, attempt, spec, deps, reply } => {
                let running = self.rebuilding.contains_key(&producer) || self.rebuild_readers.contains_key(&producer);
                self.rebuild_readers.entry(producer).or_default().push(reply);
                if !running {
                    tracing::info!(
                        task = %producer.task.hex(),
                        "a result a program reads was lost with every peer that held it: running its call again"
                    );
                    self.start_rebuild(producer, attempt, *spec, deps, 1);
                }
                self.dispatch();
            }
            CoreMsg::Outcome { task, outcome } => {
                self.remote_watched.remove(&task);
                self.notify_watchers(task, &outcome);
            }
            CoreMsg::GroupDropped { group } => {
                let router = &self.router;
                let tasks: Vec<Tk> = self
                    .watchers
                    .keys()
                    .filter(|tk| router.group_of(tk.v()) == group && !self.remote_watched.contains(tk))
                    .copied()
                    .collect();
                for tk in tasks {
                    for s in self.watchers.remove(&tk).unwrap_or_default() {
                        self.watch(s, vec![tk]);
                    }
                }
            }
            CoreMsg::WatchLost { tasks } => {
                for tk in tasks {
                    self.remote_watched.remove(&tk);
                    let sessions = self.watchers.remove(&tk).unwrap_or_default();
                    for s in sessions {
                        self.watch(s, vec![tk]);
                    }
                }
            }
            CoreMsg::Place { task } => self.place(task),
            CoreMsg::Drain => {
                self.draining = true;
                self.move_actors();
                let node = self.router.identity.peer.to_vec();
                self.record(pb::ClusterEvent {
                    kind: "drain".into(),
                    node,
                    message: "draining: no new work; stopping once running calls finish".into(),
                    ..Default::default()
                });
            }
            CoreMsg::Event(e) => self.record(e),
            CoreMsg::Events { kinds, since, wait, reply } => {
                let found = self.events_since(&kinds, since);
                if found.is_empty() && wait {
                    self.event_waiters.push((kinds, since, reply));
                } else {
                    let _ = reply.send(found);
                }
            }
            CoreMsg::Committed { actor, work, ack, reply, state } => {
                self.on_committed(actor, *work, ack, *reply, state);
                self.dispatch();
            }
            CoreMsg::Hold { req, reply } => {
                let _ = reply.send(self.on_hold(req));
            }
            CoreMsg::Persist { reply } => {
                self.save_profiles();
                let _ = reply.send(());
            }
            CoreMsg::Forget(f) => self.forget(f),
            CoreMsg::RemoteLogs { lines, .. } => self.on_remote_logs(lines),
            CoreMsg::Delegated { work, cell, result } => self.on_delegated(*work, &cell, result),
            CoreMsg::Load { from, reply } => {
                let since = self.gossip_cursor.get(&from).copied().unwrap_or(0);
                let (profiles, next) = self.profiles.changed_since(since, GOSSIP_MAX);
                self.gossip_cursor.insert(from, next);
                let recent_blobs = self.router.mesh.as_ref().map(|m| m.recent_objects()).unwrap_or_default();
                let _ = reply.send(pb::PingReply {
                    profiles,
                    recent_blobs,
                    cpus: self.capacity,
                    cpu_used: self.cpu_used,
                    queued: self.queue.len() as u32,
                    running: self.running.len() as u32,
                    actors: self.hosted_actors() as u32,
                    draining: self.draining,
                    stopping: self.draining,
                    memory_pool: self.budget.pool,
                    memory_used: self.memory_used(),
                    memory_available: self.available.unwrap_or(0),
                    object_bytes: self.object_bytes,
                    tenant_cpu: self.quotas.usage(),
                    demand: Some(self.demand()),
                    ..Default::default()
                });
            }
            CoreMsg::Exec(e) => {
                self.on_exec(*e);
                self.dispatch();
            }
            CoreMsg::OpenSession { id, mailbox, tenant } => {
                self.sessions.insert(id, Session::new(mailbox, tenant));
            }
            CoreMsg::BindRun { session, run } => {
                if let Some(s) = self.sessions.get_mut(&session) {
                    s.run = Some(run);
                }
                if let Some(r) = self.runs.get_mut(&run) {
                    r.sessions.insert(session);
                }
            }
            CoreMsg::CloseSession { id } => self.close_session(id),
            CoreMsg::SessionRoom { id } => self.session_room(id),
            CoreMsg::ResumeSession { id, seq, old, cursor } => self.resume_session(id, seq, old, cursor),
            CoreMsg::RegisterRun { run, env, detached } => {
                self.prestart(&env);
                self.runs.entry(run).or_insert(RunInfo { env, detached, sessions: BTreeSet::new() });
            }
            CoreMsg::Watch { session, tasks } => self.watch(session, tasks),
            CoreMsg::DirectInflight(calls) => self.direct.note(calls),
            CoreMsg::RunFetched { run, rec } => self.on_run_fetched(run, rec),
            CoreMsg::DirectAttempts(records) => {
                for r in records {
                    self.learn_direct(&r);
                    self.push_attempt(r.tk, r.attempt);
                }
            }
            CoreMsg::Cancel { task, force } => self.cancel(task, force),
            CoreMsg::StreamCredit { task, up_to } => self.on_stream_credit(task, up_to),
            CoreMsg::EndRun { run, cancel } => {
                self.direct.end_run(run);
                let body = if cancel {
                    pb::shard_cmd::Body::CancelRun(pb::CancelRun {
                        run: run.to_vec(),
                        message: "cancelled by its owner".into(),
                        ..Default::default()
                    })
                } else {
                    pb::shard_cmd::Body::RunFinish(pb::RunFinish { run: run.to_vec(), state: 0 })
                };
                self.propose(run.vshard(), body);
            }
            CoreMsg::FinishSessionRun { session } => {
                let run = self.sessions.get_mut(&session).and_then(|s| s.run.take());
                if let Some(run) = run {
                    if let Some(r) = self.runs.get_mut(&run) {
                        r.sessions.remove(&session);
                    }
                    let detached = self.runs.get(&run).is_some_and(|r| r.detached);
                    if !detached {
                        self.propose(
                            run.vshard(),
                            pb::shard_cmd::Body::RunFinish(pb::RunFinish { run: run.to_vec(), state: 0 }),
                        );
                    }
                }
            }
            CoreMsg::StartResult { work, accepted } => {
                if accepted {
                    let mut w = *work;
                    w.start_consumed = true;
                    self.queue.push_front(w);
                    self.dispatch();
                }
                // Denied or stale: another attempt owns the start, or the task
                // finished; its outcome arrives through the shard.
            }
            CoreMsg::Published { v, published, at } => {
                for (tk, attempt) in published {
                    let records = self.attempts.get_mut(&tk).into_iter().flatten();
                    if let Some(r) = records.filter(|r| r.attempt == attempt).last() {
                        r.published_unix_ns = at;
                    }
                }
                if let Some(n) = self.publishing.get_mut(&v) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        self.publishing.remove(&v);
                    }
                }
            }
            CoreMsg::GaveBack { work, accepted } => {
                if !accepted {
                    let mut w = *work;
                    w.stays = true;
                    self.enqueue(w);
                    self.dispatch();
                }
            }
            CoreMsg::CloseActor { actor, reason } => self.close_actor(actor, &reason),
            CoreMsg::Explain { req, reply } => {
                let _ = reply.send(self.explain(req));
            }
            CoreMsg::ActorWait { actor, reply } => {
                let _ = reply.send(self.actors.get(&actor).and_then(|h| h.waiting_memory.clone()).unwrap_or_default());
            }
            CoreMsg::Status { reply } => {
                let _ = reply.send(self.status());
            }
        }
    }

    // ---- shard events -----------------------------------------------------

    fn on_event(&mut self, group: u32, e: Event) {
        // Every replica applies every command; only the leader acts on work.
        let leader = self.router.leads(group);
        match e {
            Event::Ready(tk) if leader => self.place(tk),
            Event::Granted { task, attempt, peer, .. } if leader => self.on_granted(task, attempt, peer),
            Event::Ready(_) | Event::Granted { .. } | Event::DepsSatisfied(_) => {}
            Event::Done { task, outcome } => self.on_done(task, outcome, leader),
            // Idempotent at the producer, so every replica may send it.
            Event::Subscribe { producer, subscriber_vshard, hold } => self.propose(
                producer.v(),
                pb::shard_cmd::Body::Subscribe(pb::Subscribe {
                    producer: Some(producer.pb()),
                    subscriber_vshard: subscriber_vshard as u32,
                    hold,
                }),
            ),
            Event::Outbox { seq, .. } if leader => self.deliver_outbox(group, seq),
            Event::Outbox { .. } => {}
            Event::Releasable { object: Some(obj), .. } if leader => self.drop_object(obj),
            Event::Releasable { .. } => {}
            Event::ActorChanged(actor) => {
                if leader {
                    self.on_actor_changed(actor);
                }
                // Calls parked until the actor had a host can be placed now.
                if let Some(parked) = self.waiting_actor.remove(&actor) {
                    for tk in parked {
                        self.place(tk);
                    }
                }
            }
            Event::CancelRunContinues { run, v, message, after } if leader => {
                self.propose(v, pb::shard_cmd::Body::CancelRun(pb::CancelRun { run: run.to_vec(), message, after }))
            }
            Event::CancelRunContinues { .. } => {}
            // The copies a run keeps in its spread end and expire with it; the
            // run's own group does the run-wide work, once.
            Event::RunExpired(_) | Event::RunEnded(_) if !self.is_run_home(group, &e) => {}
            Event::RunExpired(run) => {
                self.objects.delete_run(&run);
                self.run_logs.remove(&self.code, &run.0);
                self.runs.remove(&run);
                if leader {
                    self.broadcast_forget(pb::ForgetRequest {
                        run: run.to_vec(),
                        run_expired: true,
                        ..Default::default()
                    });
                }
            }
            Event::RunEnded(run) => {
                if leader {
                    self.end_spread(run);
                    self.end_run_actors(run);
                    // Values that sessions hold stay (leases.rs).
                    let kept = self
                        .router
                        .read(run.vshard(), |kv| chimaera_core::shard::leases::held(kv, run))
                        .unwrap_or_default();
                    self.drop_run_values(run, kept.clone());
                    self.broadcast_forget(pb::ForgetRequest {
                        run: run.to_vec(),
                        run_ended: true,
                        kept,
                        ..Default::default()
                    });
                }
            }
            Event::Unleased { run, files } if leader => {
                let objects: Vec<pb::ObjectRef> = files
                    .into_iter()
                    .map(|object_id| pb::ObjectRef { run: run.to_vec(), object_id, ..Default::default() })
                    .collect();
                self.forget_objects(&objects);
                self.broadcast_forget(pb::ForgetRequest { objects, ..Default::default() });
            }
            Event::Unleased { .. } => {}
        }
    }

    fn is_run_home(&self, group: u32, e: &Event) -> bool {
        match e {
            Event::RunExpired(run) | Event::RunEnded(run) => self.router.group_of(run.vshard()) == group,
            _ => true,
        }
    }

    /// End the copies of an ended run in the other vshards of its spread, in
    /// the same state, so their calls are cancelled and they expire with it.
    fn end_spread(&mut self, run: Id) {
        let Some(Some(rec)) = self.router.read(run.vshard(), |kv| read::run(kv, run)) else { return };
        let cancelled = rec.state() == pb::run_record::State::Cancelled;
        for v in rec.spread.iter().map(|v| *v as u16).filter(|v| *v != run.vshard()) {
            let body = if cancelled {
                pb::shard_cmd::Body::CancelRun(pb::CancelRun { run: run.to_vec(), ..Default::default() })
            } else {
                pb::shard_cmd::Body::RunFinish(pb::RunFinish { run: run.to_vec(), state: 0 })
            };
            self.propose(v, body);
        }
    }

    /// Actors belong to their run unless created detached.
    fn end_run_actors(&mut self, run: Id) {
        let ending: Vec<Id> = self
            .actors
            .iter()
            .filter(|(_, h)| h.run == Some(run) && !h.detached && !h.dead)
            .map(|(id, _)| *id)
            .collect();
        for actor in ending {
            self.close_actor(actor, "its run ended");
        }
    }

    /// A run ended: its stored arguments and `ch.put` values go now, not when
    /// the run expires a day later. A detached actor of the run hosted here
    /// may be constructed again from them, so they stay while it lives, as
    /// do the files in `kept`, which sessions hold (leases.rs).
    fn drop_run_values(&self, run: Id, kept: Vec<Vec<u8>>) {
        if self.actors.values().any(|h| h.run == Some(run) && h.detached && !h.dead) {
            return;
        }
        let objects = self.objects.clone();
        tokio::task::spawn_blocking(move || {
            let freed = objects.delete_values(&run, &kept);
            if freed > 0 {
                tracing::debug!(run = %run.hex(), freed, "deleted an ended run's stored arguments");
            }
        });
    }

    /// Delete objects here, remembering their ids so a late reader is told
    /// the value was released rather than that it is missing.
    fn forget_objects(&mut self, objects: &[pb::ObjectRef]) {
        if self.released.len() > 100_000 {
            self.released.clear();
        }
        for obj in objects {
            self.objects.delete(obj);
            self.released.insert(obj.object_id.clone());
        }
    }

    fn broadcast_forget(&self, req: pb::ForgetRequest) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        for p in mesh.live_others() {
            let (mesh, req) = (mesh.clone(), req.clone());
            tokio::spawn(async move {
                let _ = mesh.forget(p.member.node_id, req).await;
            });
        }
    }

    /// Delete an object's bytes here, or ask the peers holding it to.
    fn drop_object(&self, obj: pb::ObjectRef) {
        // The files the frame refers to go with it: an inline frame of a few
        // bytes may name hundreds of MiB.
        for obj in with_blobs(&obj) {
            let Some(mesh) = self.router.mesh.clone() else {
                self.objects.delete(&obj);
                continue;
            };
            let holders = mesh.holders(&obj);
            if holders.is_empty() || holders.contains(&mesh.me.node_id) {
                self.objects.delete(&obj);
            }
            for node in holders.into_iter().filter(|n| *n != mesh.me.node_id) {
                let (mesh, obj) = (mesh.clone(), obj.clone());
                tokio::spawn(async move {
                    let _ = mesh.forget(node, pb::ForgetRequest { objects: vec![obj], ..Default::default() }).await;
                });
            }
        }
    }

    fn forget(&mut self, f: pb::ForgetRequest) {
        for key in &f.tasks {
            let Some(tk) = Tk::parse(key) else { continue };
            self.stop_local(tk);
        }
        for rec in f.ended {
            if let Some(spec) = rec.spec {
                self.actor_call_ended(&spec);
            }
        }
        self.forget_objects(&f.objects);
        if let Ok(run) = Id::from_slice(&f.run) {
            if f.run_ended {
                self.end_run_actors(run);
                self.drop_run_values(run, f.kept);
            }
            if f.run_expired {
                self.objects.delete_run(&run);
                self.run_logs.remove(&self.code, &run.0);
                self.runs.remove(&run);
            }
        }
    }

    /// A new leader of `group` places work that was ready but never granted,
    /// re-sends attempts granted to other peers, and resumes cross-vshard
    /// deliveries the previous leader may not have finished.
    fn resume(&mut self, group: u32) {
        let Some(g) = self.router.hosted(group) else { return };
        let owned = g.read(read::owned_vshards).unwrap_or_default();
        let me = self.me();
        for (v, _) in owned {
            for tk in g.read(|kv| read::ready(kv, v, usize::MAX)).unwrap_or_default() {
                self.place(tk);
            }
            for (tk, attempt, holder) in g.read(|kv| read::granted(kv, v, usize::MAX)).unwrap_or_default() {
                // This peer's own grants from before it led are still queued
                // or running here; only other peers' grants need re-sending.
                if holder != me {
                    self.on_granted(tk, attempt, holder);
                }
            }
        }
        for e in g.read(|kv| read::outbox(kv, usize::MAX)).unwrap_or_default() {
            self.deliver_outbox_entry(group, e);
        }
        // Peers this one already considers dead may have died while the
        // group had no leader to retire them: fence them here now.
        if let (Some(mesh), Some(first)) = (self.router.mesh.clone(), self.router.vshards_of(group).first().copied()) {
            for p in mesh.members().into_iter().filter(|p| p.dead) {
                let peer = pb::PeerInc { peer: p.member.peer.clone(), inc: p.member.incarnation };
                self.propose(first as u16, pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(peer) }));
            }
        }
        // Actors the previous leader had not finished placing.
        for v in self.router.vshards_of(group) {
            for actor in g.read(|kv| read::actors(kv, v as u16, usize::MAX)).unwrap_or_default() {
                self.on_actor_changed(actor);
            }
        }
    }

    fn deliver_outbox(&self, group: u32, seq: u64) {
        let Some(g) = self.router.hosted(group) else { return };
        if let Some(entry) = g.read(|kv| read::outbox_entry(kv, seq)).ok().flatten() {
            self.deliver_outbox_entry(group, entry);
        }
    }

    fn deliver_outbox_entry(&self, group: u32, entry: pb::OutboxEntry) {
        let (seq, target) = (entry.seq, entry.target_vshard as u16);
        let body = match (entry.notify, entry.unsubscribe, entry.cancel_children) {
            (Some(n), _, _) => pb::shard_cmd::Body::DepNotify(n),
            (None, Some(u), _) => pb::shard_cmd::Body::Unsubscribe(u),
            (None, None, Some(c)) => pb::shard_cmd::Body::CancelChildren(c),
            (None, None, None) => return,
        };
        let p = self.router.clone();
        // The ack goes to the group holding the outbox (any vshard it owns).
        let home = p.vshards_of(group).first().copied().unwrap_or(0) as u16;
        tokio::spawn(async move {
            if p.propose(target, body).await.is_ok() {
                let _ = p.propose(home, pb::shard_cmd::Body::OutboxAck(pb::OutboxAck { seqs: vec![seq] })).await;
            }
        });
    }

    /// Choose where a ready task runs and propose the grant. Actor calls go
    /// to their actor's host; other work goes to the least loaded of this
    /// peer and two sampled live peers that can fit it.
    fn place(&mut self, tk: Tk) {
        let target =
            if self.router.is_cluster() { self.choose_peer(tk) } else { Some(Some(self.router.identity.pb())) };
        match target {
            Some(Some(peer)) => {
                let node = if peer.peer == self.router.identity.peer.to_vec() {
                    Some(self.router.node_id())
                } else {
                    self.router.mesh.as_ref().and_then(|m| m.member_by_peer(&peer.peer)).map(|p| p.member.node_id)
                };
                if let Some(node) = node
                    && self.router.is_cluster()
                {
                    self.unplace(tk);
                    self.placed_on.insert(tk, node);
                    *self.outstanding.entry(node).or_default() += 1;
                }
                self.pending_grants.entry(tk.v()).or_default().push(pb::Grant {
                    task: Some(tk.pb()),
                    peer: Some(peer),
                    speculative: false,
                });
            }
            Some(None) => {} // parked or resolving; placed again later
            None => {
                let largest = self
                    .router
                    .mesh
                    .as_ref()
                    .map(|m| m.members().iter().filter(|p| !p.dead).map(|p| p.member.cpus).fold(0.0, f64::max))
                    .unwrap_or(self.capacity);
                let cpus = self
                    .router
                    .read(tk.v(), |kv| read::task(kv, tk))
                    .flatten()
                    .and_then(|r| r.spec)
                    .map(|s| pb::Policy::decode(s.policy.as_slice()).unwrap_or_default().cpus)
                    .unwrap_or(1.0);
                self.propose(
                    tk.v(),
                    pb::shard_cmd::Body::Cancel(pb::Cancel {
                        task: Some(tk.pb()),
                        reason: Kind::Infeasible as i32,
                        message: format!("infeasible: needs {cpus} CPUs; the largest live peer offers {largest}"),
                    }),
                );
            }
        }
    }

    /// The task left `placed_on`; `finished` when it ended (not re-placed).
    fn unplace_as(&mut self, tk: Tk, finished: bool) {
        if let Some(node) = self.placed_on.remove(&tk) {
            if let Some(n) = self.outstanding.get_mut(&node) {
                *n = n.saturating_sub(1);
            }
            if finished {
                let times = self.finished_at.entry(node).or_default();
                times.push_back(Instant::now());
                while times.front().is_some_and(|t| t.elapsed() > Duration::from_secs(5)) {
                    times.pop_front();
                }
            }
        }
    }

    fn unplace(&mut self, tk: Tk) {
        self.unplace_as(tk, false);
    }

    /// Load of a peer for placement, per CPU: its last report less what this
    /// leader saw finish there since, or what this leader alone has
    /// outstanding there, whichever is larger.
    fn pressure(&self, node: u64, reported: f64, report_at: Option<Instant>, cpus: f64) -> f64 {
        let since = match (report_at, self.finished_at.get(&node)) {
            (Some(at), Some(times)) => times.iter().rev().take_while(|t| **t > at).count(),
            _ => 0,
        };
        let current = (reported - since as f64).max(0.0);
        let mine = self.outstanding.get(&node).copied().unwrap_or(0) as f64;
        current.max(mine) / cpus.max(1e-9)
    }

    /// `None`: nothing can run it. `Some(None)`: not yet decidable.
    fn choose_peer(&mut self, tk: Tk) -> Option<Option<pb::PeerInc>> {
        let Some(Some(rec)) = self.router.read(tk.v(), |kv| read::task(kv, tk)) else { return Some(None) };
        let spec = rec.spec.unwrap_or_default();
        if matches!(spec.kind(), pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod) {
            let Ok(actor) = Id::from_slice(&spec.actor) else { return None };
            return Some(self.actor_host(actor, tk));
        }
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        let cpus = if policy.cpus > 0.0 { policy.cpus } else { 1.0 };
        let mesh = self.router.mesh.clone()?;
        let cost = self.place_cost(tk, &spec, &policy, rec.memory_hint, &mesh);
        let here = self.cpu_used + self.queue.len() as f64;
        let mine = self.pressure(self.router.node_id(), here, None, self.capacity)
            + self.extra_cost(&mesh, &cost, mesh.me.node_id, self.memory_used(), self.budget.pool);
        // Placement(require=...) keeps only peers with every label; each
        // prefer entry a peer matches lowers its cost by half a core's load.
        let allowed = |m: &pb::Member| crate::labels::satisfies(&m.labels, &policy.require);
        let bonus = |m: &pb::Member| 0.5 * crate::labels::preferred(&m.labels, &policy.prefer) as f64;
        let fits_here = !self.draining && self.capacity + 1e-9 >= cpus && allowed(&mesh.me);
        let mut best = fits_here.then(|| (mine - bonus(&mesh.me), self.router.identity.pb()));
        // Node ids only: copying every member's view for each call placed
        // cost a 64-peer leader 63 copies per call.
        let others = mesh.live_other_ids(|m| m.cpus + 1e-9 >= cpus && allowed(m));
        if !others.is_empty() {
            crate::loads::placing(&mesh); // weigh them from fresh loads (loads.rs)
            // Two random choices plus the peers holding the most input bytes
            // (core/shortlist.rs); below LOCALITY_MIN_BYTES, the largest
            // input's holders.
            let seed = shortlist::seed(&tk.task.0, now_ns());
            let ranked = if cost.held.is_empty() {
                cost.holders.clone()
            } else {
                shortlist::by_bytes(&cost.inputs, &cost.held, seed as u64)
            };
            for i in shortlist::shortlist(&others, seed, &ranked) {
                let Some(p) = mesh.member(others[i]) else { continue };
                let reported = p.load.as_ref().map_or(0.0, |l| l.cpu_used + l.queued as f64);
                let (used, pool) = p.load.as_ref().map_or((0, 0), |l| (l.memory_used, l.memory_pool));
                let score = self.pressure(p.member.node_id, reported, p.last_ok, p.member.cpus)
                    + self.extra_cost(&mesh, &cost, p.member.node_id, used, pool)
                    - bonus(&p.member);
                if best.as_ref().is_none_or(|(b, _)| score < *b) {
                    best = Some((score, pb::PeerInc { peer: p.member.peer.clone(), inc: p.member.incarnation }));
                }
            }
        }
        tracing::trace!(
            task = %tk.task.hex(),
            mine,
            others = ?others.iter().map(|n| (n % 1000, self.outstanding.get(n).copied())).collect::<Vec<_>>(),

            chosen = ?best.as_ref().map(|(s, p)| (*s, hex(&p.peer[12..]))),
            "placement"
        );
        best.map(|(_, p)| Some(p))
    }

    /// What placing a call weighs besides load: its inputs (to fetch where
    /// they are missing), how long it runs, and its memory.
    fn place_cost(&self, tk: Tk, spec: &pb::TaskSpec, policy: &pb::Policy, hint: u64, mesh: &MeshClient) -> PlaceCost {
        let deps = self.dep_outcomes(tk, spec);
        let dep_objects = deps.iter().filter_map(|o| match &o.value {
            Some(pb::outcome::Value::Ok(obj)) => Some(with_blobs(obj)),
            _ => None,
        });
        let dep_objects = dep_objects.flatten();
        let inputs: Vec<pb::ObjectRef> = spec
            .args
            .iter()
            .cloned()
            .chain(spec.blobs.iter().cloned())
            .chain(dep_objects)
            .filter(|o| o.inline_data.is_empty() && !o.run.is_empty())
            .collect();
        let inputs = shortlist::distinct(inputs);
        let total = inputs.iter().map(|o| o.size).sum();
        let key = profile_key(spec, policy);
        let est_s = self.profiles.summary(&key).map(|s| s.wall_p50_s).filter(|s| *s > 0.0).unwrap_or(1.0);
        let need = self.profiles.memory_estimate(&key, total).map_or(0, |m| m.bytes).max(hint);
        let held: Vec<Vec<u64>> =
            if total >= LOCALITY_MIN_BYTES { inputs.iter().map(|o| mesh.holders(o)).collect() } else { Vec::new() };
        let holders = if held.is_empty() {
            inputs.iter().max_by_key(|o| o.size).map(|o| mesh.holders(o)).unwrap_or_default()
        } else {
            inputs.iter().zip(&held).max_by_key(|(o, _)| o.size).map(|(_, h)| h.clone()).unwrap_or_default()
        };
        PlaceCost { inputs, held, total, est_s, need, holders }
    }

    /// Cost beyond load of running the call on `node`: inputs it lacks (only
    /// when over 30% of them would move, so locality is a preference, never a
    /// pin) and a pool it would overflow.
    fn extra_cost(&self, mesh: &MeshClient, c: &PlaceCost, node: u64, mem_used: u64, mem_pool: u64) -> f64 {
        let mut extra = 0.0;
        if c.total >= LOCALITY_MIN_BYTES {
            let missing: u64 =
                c.inputs.iter().zip(&c.held).filter(|(o, h)| !self.holds(mesh, node, o, h)).map(|(o, _)| o.size).sum();
            if missing as f64 > 0.3 * c.total as f64 {
                // A peer holding the input wins unless it is saturated (the
                // missing share costs up to one full core's queue), plus the
                // time to move the bytes, in units of the call's duration.
                let bw = c.holders.iter().filter_map(|h| mesh.bandwidth(*h)).fold(0.0, f64::max);
                let bw = if bw > 0.0 { bw } else { DEFAULT_BANDWIDTH };
                extra += missing as f64 / c.total as f64 + missing as f64 / bw / c.est_s;
            }
        }
        if c.need > 0 && mem_pool > 0 && mem_used + c.need > mem_pool {
            extra += 1.0 + (mem_used + c.need - mem_pool) as f64 / mem_pool as f64;
        }
        extra
    }

    /// Whether `node` has input `o`, held by the peers `held` (mesh.holders).
    fn holds(&self, mesh: &MeshClient, node: u64, o: &pb::ObjectRef, held: &[u64]) -> bool {
        if node == mesh.me.node_id {
            return self.objects.path(&o.run, &o.object_id).exists();
        }
        held.contains(&node) || mesh.recently_has(node, &o.object_id)
    }

    /// Where `actor` lives, if known. Unknown hosts are resolved in the
    /// background (or wait for the actor's placement) and `tk` placed again.
    fn actor_host(&mut self, actor: Id, tk: Tk) -> Option<pb::PeerInc> {
        if let Some(rec) = self.router.read(actor.vshard(), |kv| read::actor(kv, actor)).flatten() {
            return match rec.state() {
                pb::actor_record::State::Alive => rec.host,
                // Dead: the host that held it cancels the call on arrival. One
                // closed before it ever had a host has nobody to do that.
                pb::actor_record::State::Dead => match rec.host {
                    Some(h) => Some(h),
                    None => {
                        let why = rec.death.as_ref().map_or("the actor is dead".to_string(), |d| d.message.clone());
                        self.cancel_actor_call(tk, &why);
                        None
                    }
                },
                _ => {
                    self.waiting_actor.entry(actor).or_default().push(tk);
                    None
                }
            };
        }
        if let Some(h) = self.actor_hosts.get(&actor) {
            return Some(h.clone());
        }
        let (router, tx) = (self.router.clone(), self.me_tx.clone());
        tokio::spawn(async move {
            let Some(mesh) = router.mesh.clone() else { return };
            let group = router.group_of(actor.vshard());
            for _ in 0..600 {
                let req = pb::ReadRequest { actor: actor.to_vec(), ..Default::default() };
                if let Ok(r) = mesh.read_group(group, req).await
                    && let Some(rec) = r.actor
                    && matches!(rec.state(), pb::actor_record::State::Alive | pb::actor_record::State::Dead)
                {
                    let _ = tx.send(CoreMsg::Remote { work: vec![], actors: vec![rec], actor_env: None }).await;
                    let _ = tx.send(CoreMsg::Place { task: tk }).await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        None
    }

    /// The leader saw an attempt granted: run it here, or send it to its peer.
    fn on_granted(&mut self, tk: Tk, attempt: u32, peer: (Id, u64)) {
        let Some(Some(rec)) = self.router.read(tk.v(), |kv| read::task(kv, tk)) else { return };
        let Some(spec) = rec.spec else { return };
        let deps = self.dep_outcomes(tk, &spec);
        if peer != self.me() {
            let Some(mesh) = self.router.mesh.clone() else { return };
            let Some(target) = mesh.member_by_peer(&peer.0.0) else {
                tracing::warn!("granted to unknown peer {}", peer.0.hex());
                return;
            };
            let run = self.router.read(tk.v(), |kv| read::run_in(kv, tk.v(), tk.run)).flatten();
            let key = profile_key(&spec, &pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default());
            let reads = spec.args.as_ref().map_or(0, |a| a.size) + spec.blobs.iter().map(|b| b.size).sum::<u64>();
            let memory = self.profiles.memory_estimate(&key, reads).map_or(0, |m| m.bytes).max(rec.memory_hint);
            let rw = pb::RemoteWork {
                task: Some(tk.pb()),
                attempt,
                cpus: self.profiles.learned_cpus(&key),
                memory,
                spec: Some(spec),
                deps,
                env: run.as_ref().and_then(|r| r.exec_env.clone()),
                detached: run.as_ref().is_some_and(|r| r.detached),
                replay: false,
                home: run.and_then(|r| r.home).map(|h| h.peer).unwrap_or_default(),
            };
            self.pending_dispatch.entry(target.member.node_id).or_default().push(rw);
            return;
        }
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        let mut work = Work::new(tk, attempt, spec, policy, deps);
        self.reserve(&mut work, rec.memory_hint, 0.0, 0);
        self.learn_run(tk);
        if self.env_of(&tk.run).is_none() {
            self.fail_infeasible(&work, "the run has no executor environment on record");
            return;
        }
        self.admit(work);
    }

    /// An attempt another peer's leader placed here.
    fn accept_remote(&mut self, rw: pb::RemoteWork) {
        let Some(tk) = rw.task.as_ref().and_then(Tk::parse) else { return };
        let attempt = rw.attempt;
        let known = self.localizing.contains(&(tk, attempt))
            || self.queue.any_for(&tk, |w| w.tk == tk && w.attempt == attempt)
            || (self.batched.contains_key(&tk)
                && self.running.iter().any(|r| r.items.iter().any(|w| w.tk == tk && w.attempt == attempt)))
            || self
                .actors
                .values()
                .any(|h| h.mailbox.values().chain(h.ctor.iter()).any(|w| w.tk == tk && w.attempt == attempt));
        if known {
            return; // a new leader re-sent it
        }
        if !rw.home.is_empty() {
            let node = match &self.router.mesh {
                _ if rw.home == self.router.identity.peer.to_vec() => None,
                Some(mesh) => mesh.member_by_peer(&rw.home).map(|p| p.member.node_id),
                None => None,
            };
            self.log_ship.set_home(tk.run, node);
        }
        if let Some(env) = rw.env {
            self.runs.entry(tk.run).or_insert(RunInfo {
                env: ExecEnv::from_spec(env),
                detached: rw.detached,
                sessions: BTreeSet::new(),
            });
        }
        let spec = rw.spec.unwrap_or_default();
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        let mut work = Work::new(tk, attempt, spec, policy, rw.deps);
        if rw.replay {
            // A replayed constructor rebuilds a restarted actor's state; its
            // outcome is already published and it runs inside the actor's CPU.
            work.cpus = 0.0;
            work.publish = false;
            work.start_consumed = true;
        } else {
            self.reserve(&mut work, 0, rw.cpus, rw.memory);
        }
        self.admit(work);
    }

    /// Why a fetch failed, when an input the attempt lacks was released by
    /// its driver. Checked after trying the fetch, not before: a value stored
    /// by content gets the same id when its bytes are sent again, and its
    /// holder then has it once more.
    fn released_input(&self, w: &Work) -> Option<String> {
        let obj = w
            .spec
            .args
            .iter()
            .chain(w.spec.blobs.iter())
            .find(|o| self.released.contains(&o.object_id) && !self.objects.path(&o.run, &o.object_id).exists())?;
        Some(format!(
            "input {} was released by its driver: nothing held it once the calls known to read it had finished \
             (a dropped ch.put future, or an array passed to calls and since collected); keep it while calls may \
             still read it",
            hex(&obj.object_id)
        ))
    }

    /// Queue an attempt once its code and every object it reads are in the
    /// local stores (a cluster peer fetches what it is missing).
    fn admit(&mut self, w: Work) {
        let Some(mesh) = self.router.mesh.clone() else {
            self.enqueue(w);
            return;
        };
        let mut missing: Vec<pb::ObjectRef> = Vec::new();
        let args = w.spec.args.iter().chain(w.spec.blobs.iter());
        // A dependency stored in a file of its own whose buffers are not all
        // here: its buffers wait for that file. A result stored in parts
        // (returns=N) leaves its parts' buffers to the callee, which reads
        // only the value it was given and fetches what it touches (see parts).
        let mut later: Vec<(pb::ObjectRef, Vec<pb::ObjectRef>)> = Vec::new();
        let mut deps: Vec<pb::ObjectRef> = Vec::new();
        for o in &w.deps {
            let Some(pb::outcome::Value::Ok(obj)) = &o.value else { continue };
            let mut all = with_blobs(obj);
            let head = all.remove(0);
            if head.inline_data.is_empty() && !head.run.is_empty() {
                all.retain(|b| !self.objects.path(&b.run, &b.object_id).exists());
                if !all.is_empty() {
                    later.push((head.clone(), std::mem::take(&mut all)));
                }
            }
            deps.push(head);
            deps.extend(all);
        }
        for obj in args.chain(deps.iter()) {
            if obj.inline_data.is_empty()
                && !obj.run.is_empty()
                && !self.objects.path(&obj.run, &obj.object_id).exists()
            {
                missing.push(obj.clone());
            }
        }
        let digest = w.spec.fn_digest.clone();
        let need_code = !digest.is_empty() && !self.code.has(&digest);
        if missing.is_empty() && later.is_empty() && !need_code {
            self.enqueue(w);
            return;
        }
        self.localizing.insert((w.tk, w.attempt));
        let (tx, store, code) = (self.me_tx.clone(), self.objects.clone(), self.code.clone());
        let router = self.router.clone();
        tokio::spawn(async move {
            let mut error = None;
            let mut lost = None;
            if need_code && let Err(e) = mesh.localize_code(&code, &digest).await {
                error = Some(format!("cannot fetch the function's code {}: {e}", hex(&digest)));
            }
            if error.is_none() {
                let mut fetched = fetch_inputs(&mesh, &store, missing).await;
                if fetched.is_ok() && !later.is_empty() {
                    let s = store.clone();
                    // Off the async workers: reads each file's header.
                    let blobs = tokio::task::spawn_blocking(move || {
                        later
                            .into_iter()
                            .filter(|(head, _)| crate::parts::file_ids(&s.path(&head.run, &head.object_id)).is_empty())
                            .flat_map(|(_, blobs)| blobs)
                            .collect::<Vec<_>>()
                    })
                    .await
                    .unwrap_or_default();
                    fetched = fetch_inputs(&mesh, &store, blobs).await;
                }
                if let Err((object, e)) = fetched {
                    error = Some(e);
                    lost = Some(object);
                }
            }
            if let (Some(object), Some(why)) = (lost, error.as_mut())
                && w.rebuilds < MAX_REBUILDS
            {
                match rebuild_plan(&router, &mesh, &w, &object).await {
                    Ok((producer, attempt, spec, deps)) => {
                        let consumer = Box::new(w);
                        let msg = CoreMsg::Rebuild { consumer, producer, attempt, spec: Box::new(spec), deps };
                        let _ = tx.send(msg).await;
                        return;
                    }
                    Err(cannot) => why.push_str(&format!("; it cannot be made again: {cannot}")),
                }
            }
            let _ = tx.send(CoreMsg::Localized { work: Box::new(w), error }).await;
        });
    }

    fn enqueue(&mut self, mut w: Work) {
        if w.queued_ns == 0 {
            w.queued_ns = now_ns();
        }
        if w.spec.parent.is_some() && matches!(w.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem) {
            // Possibly a local-first grant arriving (see ingress).
            self.router.local_arrived();
        }
        match w.kind() {
            pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod => self.route_to_actor(w),
            // A gang rank's slot is held for it (core/holds.rs): it takes
            // nothing the work ahead waits for, so a call too big to start
            // yet never holds it up.
            _ if w.policy.launch.is_some() => self.queue.push_front(w),
            // Higher `priority` first; equal priorities keep arrival order.
            // Ordering only: nothing running is preempted.
            _ if self.queue.back().is_none_or(|b| b.policy.priority >= w.policy.priority) => self.queue.push_back(w),
            _ => {
                let at =
                    self.queue.iter().position(|q| q.policy.priority < w.policy.priority).unwrap_or(self.queue.len());
                self.queue.insert(at, w);
            }
        }
    }

    /// Spillback: work that has waited here while other peers have room goes
    /// back to its group leader to be placed again. Placement happens once,
    /// with what the leader knew then; peers that joined later or freed up
    /// since would otherwise never see work already queued here.
    fn spill(&mut self) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        let waited = |w: &Work| w.ready_at.elapsed() >= SPILL_AFTER;
        if self.draining || !self.queue.iter().any(waited) {
            return;
        }
        // Room elsewhere, as peers last reported it (minus what this peer
        // already sent them since).
        let room: f64 = mesh
            .live_others()
            .iter()
            .filter_map(|p| p.load.as_ref().map(|l| (p, l)))
            .map(|(p, l)| (p.member.cpus - l.cpu_used - l.queued as f64 - p.placed_since_report as f64).max(0.0))
            .sum();
        if room < 1.0 {
            return;
        }
        // Newest first: the oldest stay, to run here next.
        let mut budget = room.floor() as usize;
        let picks: Vec<(Tk, u32)> = self
            .queue
            .iter()
            .rev()
            .filter(|w| {
                waited(w)
                    && !w.stays
                    && w.policy.launch.is_none() // its slot is held here
                    && matches!(w.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem)
            })
            .take_while(|_| {
                let go = budget > 0;
                budget = budget.saturating_sub(1);
                go
            })
            .map(|w| (w.tk, w.attempt))
            .collect();
        for (tk, attempt) in picks {
            self.requeue(tk, attempt);
        }
    }

    /// Give back a queued attempt that has not started here, so the task's
    /// group leader places it again (on a peer with room). Nothing is counted
    /// against the task's retries. The work is kept until the shard accepts:
    /// if it refuses, or the answer is lost, the attempt runs here (a give-back
    /// that did commit revokes this attempt, so its publish is fenced). False
    /// if it is not queued here (it started, finished, or was never here).
    pub(crate) fn requeue(&mut self, tk: Tk, attempt: u32) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(i) = self.queue.position_for(&tk, |w| w.tk == tk && w.attempt == attempt) else { return false };
        if !self.queue[i].publish || self.queue[i].start_consumed {
            // A replayed constructor, or an external effect whose start is
            // committed, must stay where it is.
            return false;
        }
        let w = self.queue.remove(i).expect("index in range");
        self.unplace(tk);
        let body = pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
            task: Some(tk.pb()),
            attempt,
            peer: Some(self.router.identity.pb()),
            kind: pb::record_failure::Kind::Requeued as i32,
            error: Some(pb::ErrorRecord {
                message: "given back unstarted to run elsewhere".into(),
                attempt,
                ..Default::default()
            }),
            fingerprint: Vec::new(),
            memory_peak: 0,
        });
        let (router, tx, in_flight) = (self.router.clone(), self.me_tx.clone(), self.in_flight.clone());
        in_flight.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let reply = router.propose(tk.v(), body).await;
            if let Err(e) = &reply {
                tracing::warn!("give-back of {tk:?} failed, running it here: {e}");
            }
            let accepted = give_back_taken(&reply);
            in_flight.fetch_sub(1, Relaxed);
            let _ = tx.send(CoreMsg::GaveBack { work: Box::new(w), accepted }).await;
        });
        true
    }

    /// An attempt cannot run here: report it (the shard retries it), or, for
    /// a call being run again to remake a lost value, fail what waits for it.
    fn give_up(&mut self, w: &Work, why: &str) {
        if w.publish {
            self.fail_attempt(w, pb::record_failure::Kind::Infra, why);
            return;
        }
        for reader in self.rebuild_readers.remove(&w.tk).unwrap_or_default() {
            let _ = reader.send(Err(why.to_string()));
        }
        if let Some(waiting) = self.rebuilding.remove(&w.tk) {
            let why = format!("an input was lost with its peer, and running the call that made it again failed: {why}");
            for c in waiting {
                self.give_up(&c, &why);
            }
        }
    }

    /// Run `producer` again here to remake a lost value: its outcome is
    /// committed, so this run only rewrites the value under the same id (the
    /// same attempt) and has nothing to start or publish.
    fn start_rebuild(&mut self, producer: Tk, attempt: u32, spec: pb::TaskSpec, deps: Vec<pb::Outcome>, depth: u32) {
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        let mut work = Work::new(producer, attempt, spec, policy, deps);
        work.publish = false;
        work.start_consumed = true;
        work.rebuilds = depth;
        self.reserve(&mut work, 0, 0.0, 0);
        self.admit(work);
    }

    /// A call run again to remake a lost value finished: the attempts waiting
    /// for that value go on (it is in this peer's store now), or fail with it.
    fn rebuilt(&mut self, w: &Work, outcome: Option<&pb::Outcome>) {
        match outcome.and_then(|o| o.value.as_ref()) {
            Some(pb::outcome::Value::Ok(_)) => {
                for reader in self.rebuild_readers.remove(&w.tk).unwrap_or_default() {
                    let _ = reader.send(Ok(()));
                }
                for c in self.rebuilding.remove(&w.tk).unwrap_or_default() {
                    self.admit(c);
                }
            }
            Some(pb::outcome::Value::Err(e)) => self.give_up(w, &format!("it raised {}: {}", e.type_name, e.message)),
            None => self.give_up(w, "it produced no value"),
        }
    }

    /// Report an attempt that could not run here; the shard retries it.
    fn fail_attempt(&mut self, w: &Work, kind: pb::record_failure::Kind, why: &str) {
        if !w.publish {
            return;
        }
        self.propose(
            w.tk.v(),
            pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
                task: Some(w.tk.pb()),
                attempt: w.attempt,
                peer: Some(self.router.identity.pb()),
                kind: kind as i32,
                error: Some(pb::ErrorRecord {
                    kind: Kind::Infra as i32,
                    message: why.to_string(),
                    attempt: w.attempt,
                    ..Default::default()
                }),
                fingerprint: Vec::new(),
                memory_peak: 0,
            }),
        );
    }

    fn dep_outcomes(&self, tk: Tk, spec: &pb::TaskSpec) -> Vec<pb::Outcome> {
        // One read for all of them: a gather may have hundreds.
        let read_all = |kv: &dyn chimaera_core::kv::KvRead| {
            spec.deps
                .iter()
                .map(|d| Tk::parse(d).and_then(|dep| read::dep_outcome(kv, tk.v(), dep)).unwrap_or_default())
                .collect()
        };
        self.router.read(tk.v(), read_all).unwrap_or_else(|| vec![pb::Outcome::default(); spec.deps.len()])
    }

    /// A retained result's copies are made: publish it. Else, in a cluster,
    /// the attempt failed for want of them (it is retried within its
    /// attempts); a peer that runs alone never has a copy, so the call fails.
    fn on_retained(&mut self, mut entry: pb::PublishEntry, error: Option<String>) {
        let Some(tk) = entry.task.as_ref().and_then(Tk::parse) else { return };
        let Some(why) = error else {
            self.pending_publish.entry(tk.v()).or_default().push(entry);
            return;
        };
        tracing::warn!(task = %tk.task.hex(), "{why}");
        let err = pb::ErrorRecord { message: why, attempt: entry.attempt, ..Default::default() };
        if !self.router.is_cluster() {
            let err = pb::ErrorRecord { kind: pb::error_record::Kind::Infeasible as i32, ..err };
            entry.outcome = Some(pb::Outcome { value: Some(pb::outcome::Value::Err(err)), ..Default::default() });
            self.pending_publish.entry(tk.v()).or_default().push(entry);
            return;
        }
        let failure = pb::RecordFailure {
            task: Some(tk.pb()),
            attempt: entry.attempt,
            peer: entry.peer,
            kind: pb::record_failure::Kind::Infra as i32,
            error: Some(pb::ErrorRecord { kind: pb::error_record::Kind::Infra as i32, ..err }),
            fingerprint: Vec::new(),
            memory_peak: 0,
        };
        self.propose(tk.v(), pb::shard_cmd::Body::RecordFailure(failure));
    }

    /// Whether an attempt's outcome may reach its callers before it is
    /// published: on a local peer, for an attached run (a crash ends its driver
    /// too), when no other attempt of the task could publish a different one.
    fn notify_early(&self, tk: Tk, attempt: u32) -> bool {
        let other = |w: &Work| w.tk == tk && w.attempt != attempt;
        // This attempt's own item is still in its batch: look through the
        // batches only when the task has another item there.
        let other_running =
            || self.batched.get(&tk).is_some_and(|&n| n > 1) && self.running.iter().any(|r| r.items.iter().any(other));
        !self.router.is_cluster()
            && self.runs.get(&tk.run).is_some_and(|r| !r.detached)
            && !self.queue.any_for(&tk, other)
            && !other_running()
    }

    /// Count `items` out of `batched`: their batch left `running`.
    fn unbatch(&mut self, items: &[Work]) {
        for w in items {
            if let Some(n) = self.batched.get_mut(&w.tk) {
                *n -= 1;
                if *n == 0 {
                    self.batched.remove(&w.tk);
                }
            }
        }
    }

    /// A replica here installed a snapshot: outcomes it skipped over raised no
    /// events, so tell the watchers of its tasks that are done now.
    fn recheck_watches(&mut self, group: u32) {
        let done: Vec<(Tk, pb::Outcome)> = self
            .watchers
            .keys()
            .filter(|tk| self.router.group_of(tk.v()) == group && !self.remote_watched.contains(tk))
            .filter_map(|tk| Some((*tk, self.router.read(tk.v(), |kv| read::task(kv, *tk)).flatten()?.outcome?)))
            .collect();
        for (tk, outcome) in done {
            self.notify_watchers(tk, &outcome);
        }
    }

    fn notify_watchers(&mut self, tk: Tk, outcome: &pb::Outcome) {
        let Some(ids) = self.watchers.remove(&tk) else { return };
        for id in ids {
            if let Some(s) = self.sessions.get_mut(&id) {
                s.outcome(tk, outcome.clone());
            }
        }
    }

    /// Stop queued or running attempts of a task that ended.
    fn stop_local(&mut self, tk: Tk) {
        // Called for every finished call this peer hears of; few were queued here.
        if self.queue.contains_key(&tk) {
            self.queue.retain(|w| w.tk != tk);
        }
        if !self.unfinished.contains_key(&tk) {
            return;
        }
        let running_on: Vec<u32> = self
            .running
            .iter()
            .filter(|r| r.items.iter().zip(&r.finished).any(|(w, f)| w.tk == tk && !f))
            .map(|r| r.exec)
            .collect();
        for exec in running_on {
            self.sup.send(
                exec,
                pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Cancel(pb::ExecCancel { task: Some(tk.pb()) })) },
            );
            self.cancelling.push((exec, tk, Instant::now()));
        }
    }

    /// One running item of `tk` finished or was lost with its executor.
    fn item_ended(&mut self, tk: Tk) {
        if let Some(n) = self.unfinished.get_mut(&tk) {
            *n -= 1;
            if *n == 0 {
                self.unfinished.remove(&tk);
            }
        }
    }

    /// Kill the executor of a cancelled call that kept running past
    /// [`cancel_kill_after`] (a C extension or a thread that never checks),
    /// when every call on it was cancelled. An actor's executor is kept: its
    /// state lives there, so cancelling a method stays cooperative.
    fn kill_stuck_cancelled(&mut self) {
        let running = &self.running;
        let still = |exec: u32, tk: Tk| running.iter().any(|r| r.exec == exec && r.items.iter().any(|w| w.tk == tk));
        self.cancelling.retain(|&(exec, tk, _)| still(exec, tk));
        let due: BTreeSet<u32> = self
            .cancelling
            .iter()
            .filter(|(_, _, at)| at.elapsed() >= cancel_kill_after())
            .map(|&(exec, _, _)| exec)
            .collect();
        for exec in due {
            let actor = self.execs.get(&exec).is_some_and(|e| e.actor.is_some());
            let all_cancelled = self
                .running
                .iter()
                .filter(|r| r.exec == exec)
                .flat_map(|r| r.items.iter())
                .all(|w| self.cancelling.iter().any(|&(e, tk, _)| e == exec && tk == w.tk));
            if !actor && all_cancelled {
                tracing::info!(exec, "killing an executor whose cancelled call did not stop");
                crate::counters::CANCEL_KILLS.add(1);
                self.sup.kill(exec);
            }
            self.cancelling.retain(|&(e, _, _)| e != exec);
        }
    }

    fn on_done(&mut self, tk: Tk, outcome: pb::Outcome, leader: bool) {
        self.direct.done(tk);
        self.unplace_as(tk, true);
        if leader && let Some(pb::outcome::Value::Err(e)) = &outcome.value {
            self.record(pb::ClusterEvent {
                kind: "task_failed".into(),
                run: tk.run.to_vec(),
                task: Some(tk.pb()),
                message: format!("{:?}: {}", e.kind(), e.message.lines().next().unwrap_or("")),
                ..Default::default()
            });
        }
        self.notify_watchers(tk, &outcome);
        // A queued attempt whose task already finished (cancelled, or another
        // attempt won) is dropped; a running one is told to stop.
        self.stop_local(tk);
        let rec = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten();
        if let Some(spec) = rec.as_ref().and_then(|r| r.spec.as_ref()) {
            self.actor_call_ended(spec);
            // The call's own argument file is not needed once it has an
            // outcome. Constructor arguments stay until the run ends: a
            // restarted actor runs its constructor again.
            if leader
                && spec.kind() != pb::CallKind::CallActorCtor
                && let Some(args) = spec.args.clone().filter(|a| a.inline_data.is_empty() && !a.object_id.is_empty())
            {
                self.drop_object(args);
            }
        }
        // The leader tells other peers about endings they cannot see: an
        // attempt to stop, or an actor call that never reached its host.
        let failed = matches!(outcome.value, Some(pb::outcome::Value::Err(_)));
        if leader
            && failed
            && let (Some(mesh), Some(rec)) = (self.router.mesh.clone(), rec)
        {
            let me = mesh.me.node_id;
            let mut nodes: BTreeSet<u64> = rec
                .attempts
                .iter()
                .filter_map(|a| a.peer.as_ref())
                .filter_map(|p| mesh.member_by_peer(&p.peer))
                .map(|p| p.member.node_id)
                .collect();
            let actor = rec.spec.as_ref().and_then(|s| Id::from_slice(&s.actor).ok());
            if let Some(h) = actor.and_then(|a| {
                self.actor_hosts
                    .get(&a)
                    .cloned()
                    .or_else(|| self.router.read(a.vshard(), |kv| read::actor(kv, a)).flatten().and_then(|r| r.host))
            }) && let Some(p) = mesh.member_by_peer(&h.peer)
            {
                nodes.insert(p.member.node_id);
            }
            nodes.remove(&me);
            for node in nodes {
                let req = pb::ForgetRequest { tasks: vec![tk.pb()], ended: vec![rec.clone()], ..Default::default() };
                let mesh = mesh.clone();
                tokio::spawn(async move {
                    let _ = mesh.forget(node, req).await;
                });
            }
        }
    }

    fn watch(&mut self, session: u64, tasks: Vec<Tk>) {
        let mut remote: BTreeMap<u32, Vec<Tk>> = BTreeMap::new();
        for tk in tasks {
            // Watched where the outcome surely arrives: here only if this
            // peer's replica keeps up with the group, else on its members.
            if !self.router.current(tk.v()) {
                self.watchers.entry(tk).or_default().push(session);
                if self.remote_watched.insert(tk) {
                    remote.entry(self.router.group_of(tk.v())).or_default().push(tk);
                }
                continue;
            }
            let done = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten().and_then(|r| r.outcome);
            match done {
                Some(outcome) => {
                    if let Some(s) = self.sessions.get_mut(&session) {
                        s.outcome(tk, outcome);
                    }
                }
                None => self.watchers.entry(tk).or_default().push(session),
            }
        }
        for (group, tks) in remote {
            self.remote_watch(group, tks);
        }
    }

    /// This peer dropped its replica of `group`: what its sessions watched
    /// there is followed through a peer that still holds one.
    fn watch_elsewhere(&mut self, group: u32) {
        let moved: Vec<Tk> = self
            .watchers
            .keys()
            .filter(|t| self.router.group_of(t.v()) == group && !self.remote_watched.contains(t))
            .copied()
            .collect();
        if moved.is_empty() {
            return;
        }
        self.remote_watched.extend(moved.iter().copied());
        self.remote_watch(group, moved);
    }

    /// Follow outcomes of tasks whose group this peer does not host, through
    /// a peer that does; another replica takes over if that one goes away or
    /// goes quiet.
    fn remote_watch(&self, group: u32, tasks: Vec<Tk>) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        let (tx, router) = (self.me_tx.clone(), self.router.clone());
        tokio::spawn(async move {
            let mut left: BTreeSet<Tk> = tasks.into_iter().collect();
            // Rounds in which no replica took the watch; a watch that was
            // taken and went quiet is not a failure, it is placed again.
            let mut failed = 0u64;
            while !left.is_empty() && failed < 20 {
                // The known leader first, then live voters; voters liveness
                // has given up on last (a peer that hosts no replica of the
                // group keeps the plan's voter list, which can name the dead).
                let mut nodes = mesh.voters_of(group);
                let hint = router.leader_hint(group);
                nodes.sort_by_key(|n| {
                    let gone = mesh.member(*n).is_none_or(|p| p.dead || p.suspect);
                    (gone, Some(*n) != hint)
                });
                let mut taken = false;
                for node in nodes.into_iter().filter(|n| *n != mesh.me.node_id) {
                    let keys = left.iter().map(|t| t.pb()).collect();
                    let Ok(mut stream) = mesh.watch(node, keys).await else { continue };
                    taken = true;
                    // A peer whose seat in the group moved away keeps the
                    // stream open and stops applying the group: a stream that
                    // stays quiet is placed again, on the leader as known then.
                    while let Ok(Ok(Some(ev))) = tokio::time::timeout(WATCH_QUIET, stream.message()).await {
                        let Some(tk) = ev.task.as_ref().and_then(Tk::parse) else { continue };
                        if left.remove(&tk) {
                            let outcome = ev.outcome.unwrap_or_default();
                            if tx.send(CoreMsg::Outcome { task: tk, outcome }).await.is_err() {
                                return;
                            }
                        }
                    }
                    break;
                }
                if !taken {
                    failed += 1;
                    tokio::time::sleep(Duration::from_millis(100 * failed)).await;
                }
            }
            if !left.is_empty() {
                let _ = tx.send(CoreMsg::WatchLost { tasks: left.into_iter().collect() }).await;
            }
        });
    }

    fn cancel(&mut self, tk: Tk, force: bool) {
        self.propose(
            tk.v(),
            pb::shard_cmd::Body::Cancel(pb::Cancel {
                task: Some(tk.pb()),
                reason: Kind::Cancelled as i32,
                message: "cancelled by the caller".into(),
            }),
        );
        if force {
            let execs: Vec<u32> =
                self.running.iter().filter(|r| r.items.iter().any(|w| w.tk == tk)).map(|r| r.exec).collect();
            for e in execs {
                self.sup.kill(e);
            }
        }
    }

    /// A session's stream ended. A resumable one waits for its client to
    /// resume it (mailbox::RESUME_WINDOW), or is taken over at once by a
    /// resume that waited for this; any other ends now.
    fn close_session(&mut self, id: u64) {
        // A resume this session waited to make ends with it.
        self.resuming.retain(|_, (new, ..)| *new != id);
        let waiting = self.resuming.remove(&id);
        let Some(s) = self.sessions.get_mut(&id) else { return };
        if s.park(Instant::now()) {
            if let Some((new, seq, cursor)) = waiting {
                self.resume_session(new, seq, id, cursor);
            }
            return;
        }
        self.end_session(id);
        if let Some((new, seq, _)) = waiting {
            self.session_error(new, seq, format!("cannot resume session {id}: its client acknowledged nothing"));
        }
    }

    /// Session `id` continues session `old`, whose client received up to
    /// `cursor`: its run, its watches, and what it sent that the client missed.
    fn resume_session(&mut self, id: u64, seq: u64, old: u64, cursor: u64) {
        let Some(tenant) = self.sessions.get(&id).map(|s| s.tenant.clone()) else { return };
        let old = self.resumed_as.get(&old).copied().unwrap_or(old);
        let why = match self.sessions.get(&old) {
            None => Some("it has ended (or this peer restarted)".to_string()),
            Some(s) if s.tenant != tenant => Some("it belongs to another credential".to_string()),
            Some(s) => s.cannot_resume(cursor),
        };
        if let Some(why) = why {
            return self.session_error(id, seq, format!("cannot resume session {old}: {why}"));
        }
        if let Some(s) = self.sessions.get(&old)
            && !s.parked()
        {
            // Its stream has not ended here yet: it ends after the request it
            // is handling, and the resume waits for that (close_session).
            s.mailbox.supersede();
            self.resuming.insert(old, (id, seq, cursor));
            return;
        }
        let Some(prev) = self.sessions.remove(&old) else { return };
        let Some(s) = self.sessions.get_mut(&id) else { return };
        s.adopt(prev, cursor);
        self.borrows.adopt(old, id);
        self.resumed_as.values_mut().filter(|v| **v == old).for_each(|v| *v = id);
        self.resumed_as.insert(old, id);
        let run = s.run;
        for w in self.watchers.values_mut() {
            w.iter_mut().filter(|x| **x == old).for_each(|x| *x = id);
        }
        if let Some(r) = run.and_then(|run| self.runs.get_mut(&run)) {
            r.sessions.remove(&old);
            r.sessions.insert(id);
        }
        self.session_room(id);
    }

    fn session_error(&self, id: u64, seq: u64, message: String) {
        if let Some(s) = self.sessions.get(&id) {
            let ev =
                pb::SessionEvent { seq, event: Some(pb::session_event::Event::Error(message)), ..Default::default() };
            let _ = s.mailbox.offer(ev, crate::mailbox::Kind::Notice);
        }
    }

    /// The session's reader made room: what waited for it goes now.
    fn session_room(&mut self, id: u64) {
        let Some(s) = self.sessions.get_mut(&id) else { return };
        let again = s.refill();
        if !again.is_empty() {
            self.watch(id, again);
        }
    }

    /// A session ends: its watches go, and its run, if no other session holds
    /// it and it is not detached, is cancelled.
    fn end_session(&mut self, id: u64) {
        let Some(s) = self.sessions.remove(&id) else { return };
        self.resumed_as.retain(|_, now| *now != id);
        crate::leases::release_ended(self.router.clone(), self.in_flight.clone(), self.borrows.end(id));
        for w in self.watchers.values_mut() {
            w.retain(|x| *x != id);
        }
        if let Some(run) = s.run
            && let Some(r) = self.runs.get_mut(&run)
        {
            r.sessions.remove(&id);
            if r.sessions.is_empty() && !r.detached {
                // The driver disconnected without finishing: cancel its run.
                // (A clean exit sends FinishRun first, which wins the race.)
                self.propose(
                    run.vshard(),
                    pb::shard_cmd::Body::CancelRun(pb::CancelRun {
                        run: run.to_vec(),
                        message: "the driver disconnected".into(),
                        ..Default::default()
                    }),
                );
            }
        }
    }

    // ---- dispatch ---------------------------------------------------------

    /// The run's executor environment: registered by its client, or (after a
    /// peer restart) loaded from the replicated run record.
    /// The run's tenant (for quotas), read once from its record.
    /// Run queued call `i` in another cell (docs/federation.md): only when it
    /// has waited `DELEGATE_AFTER`, may leave the cell, no peer of this cell
    /// has `cpus` free, and another cell reports room for it.
    fn delegate(&mut self, i: usize, cpus: f64) -> bool {
        let w = &self.queue[i];
        if self.fleet.is_none()
            || w.ready_at.elapsed() < crate::fleet::DELEGATE_AFTER
            || !w.publish
            || !crate::fleet::eligible(&w.spec, &w.policy)
            || w.policy.durability() != pb::Durability::Default // published here, with its class (see replicas)
            || self.cell_has_room(cpus)
        {
            return false;
        }
        let (run, digest) = (w.tk.run, w.spec.fn_digest.clone());
        let Ok(code) = std::fs::read(self.code.path(&digest)) else { return false };
        // Stored values travel with the call: all must be on this peer, and not too large.
        if crate::fleet::stored(&w.spec).map(|o| o.size).sum::<u64>() > crate::fleet::MAX_STORED_BYTES {
            return false;
        }
        let mut values = Vec::new();
        for o in crate::fleet::stored(&w.spec) {
            let Ok(bytes) = std::fs::read(self.objects.path(&o.run, &o.object_id)) else { return false };
            values.push((o.object_id.clone(), bytes));
        }
        let Some(env) = self.env_of(&run) else { return false };
        // Every live peer of this cell may delegate: each counts on its share of another cell.
        let peers = self.router.mesh.as_ref().map_or(1, |m| m.members().iter().filter(|v| !v.dead).count().max(1));
        let fleet = self.fleet.as_mut().expect("checked above");
        let Some(cell) = fleet.pick(cpus, 1.0 / peers as f64, &self.queue[i].tk.task.0) else { return false };
        let Some(link) = fleet.link(&cell) else { return false };
        let w = self.queue.remove(i).expect("index in range");
        let (spec, tx) = (w.spec.clone(), self.me_tx.clone());
        tracing::debug!(task = %w.tk.task.hex(), %cell, "running a call in another cell");
        tokio::spawn(async move {
            let result = link.delegate(run, &env, spec, code, values).await;
            let _ = tx.send(CoreMsg::Delegated { work: Box::new(w), cell, result }).await;
        });
        true
    }

    /// Whether another live peer of this cell has `cpus` free (spillback's job, not a cell's).
    fn cell_has_room(&self, cpus: f64) -> bool {
        let me = self.router.identity.peer.0.to_vec();
        self.router.mesh.as_ref().is_some_and(|m| {
            m.members().into_iter().any(|v| {
                !v.dead
                    && !v.suspect
                    && v.member.peer != me
                    && v.load.is_some_and(|l| !l.draining && l.cpus - l.cpu_used + 1e-9 >= cpus)
            })
        })
    }

    /// A delegated call ended. Its outcome is published as this attempt's, through
    /// the same fenced path as an executor's (a stale attempt is refused there). A
    /// call the other cell never ran, or lost, comes back to this peer's queue: it
    /// was granted here, and running it twice is harmless (`effects="none"`).
    fn on_delegated(&mut self, work: Work, cell: &str, result: crate::fleet::Delegated) {
        let (tk, attempt) = (work.tk, work.attempt);
        if let Some(f) = self.fleet.as_mut() {
            f.done(cell, work.cpus);
        }
        let stored = match result {
            Ok((mut outcome, files)) => match outcome.value.as_mut() {
                Some(pb::outcome::Value::Ok(obj)) if !files.is_empty() => {
                    let written = files.iter().try_for_each(|(id, bytes)| {
                        let path = self.objects.path(&tk.run.0, id);
                        path.parent().map_or(Ok(()), std::fs::create_dir_all).and_then(|()| {
                            let tmp = path.with_extension("fleet");
                            std::fs::write(&tmp, bytes).and_then(|()| std::fs::rename(&tmp, &path))
                        })
                    });
                    obj.run = tk.run.0.to_vec();
                    obj.locations = vec![self.router.identity.pb()];
                    for b in &mut obj.blobs {
                        b.run = tk.run.0.to_vec();
                        b.locations.clear(); // here, with the value (see with_blobs)
                    }
                    written.map(|()| outcome).map_err(|e| format!("could not store its result here: {e}"))
                }
                _ => Ok(outcome),
            },
            Err(crate::fleet::Lost::NotSent(why) | crate::fleet::Lost::Lost(why)) => Err(why),
        };
        match stored {
            Ok(mut outcome) => {
                outcome.attempt = attempt;
                outcome.peer = Some(self.router.identity.pb());
                self.pending_publish.entry(tk.v()).or_default().push(pb::PublishEntry {
                    task: Some(tk.pb()),
                    attempt,
                    peer: Some(self.router.identity.pb()),
                    outcome: Some(outcome),
                    forward_to: None,
                });
            }
            Err(why) => {
                tracing::info!(task = %tk.task.hex(), %cell, "a call sent to another cell comes back: {why}");
                if let Some(f) = self.fleet.as_mut() {
                    f.failed(cell);
                }
                self.queue.push_front(work);
            }
        }
    }

    /// Per tenant, the CPUs the other live peers last reported holding.
    fn tenants_elsewhere(&self) -> HashMap<String, f64> {
        let mut out = HashMap::new();
        let me = self.router.identity.peer.0.to_vec();
        for v in self.router.mesh.as_ref().map(|m| m.members()).unwrap_or_default() {
            if v.dead || v.member.peer == me {
                continue;
            }
            for (tenant, cpus) in v.load.map(|l| l.tenant_cpu).unwrap_or_default() {
                *out.entry(tenant).or_insert(0.0) += cpus;
            }
        }
        out
    }

    fn tenant_of(&mut self, run: Id) -> String {
        let router = self.router.clone();
        self.quotas.tenant(run, || router.read(run.vshard(), |kv| read::run(kv, run)).flatten().map(|r| r.tenant))
    }

    /// Remember the run of a call granted here from the copy of its record in
    /// the call's own vshard: this peer may lead that group without hosting
    /// the run's own.
    fn learn_run(&mut self, tk: Tk) {
        if self.runs.contains_key(&tk.run) {
            return;
        }
        let Some(Some(rec)) = self.router.read(tk.v(), |kv| read::run_in(kv, tk.v(), tk.run)) else { return };
        self.quotas.tenant(tk.run, || Some(rec.tenant.clone()));
        if let Some(spec) = rec.exec_env {
            let env = ExecEnv::from_spec(spec);
            self.runs.insert(tk.run, RunInfo { env, detached: rec.detached, sessions: BTreeSet::new() });
        }
    }

    fn env_of(&mut self, run: &Id) -> Option<ExecEnv> {
        if let Some(r) = self.runs.get(run) {
            return Some(r.env.clone());
        }
        let rec = self.router.read(run.vshard(), |kv| read::run(kv, *run)).flatten()?;
        let env = ExecEnv::from_spec(rec.exec_env?);
        self.runs.insert(*run, RunInfo { env: env.clone(), detached: rec.detached, sessions: BTreeSet::new() });
        Some(env)
    }

    /// Start queued work that fits: CPU in queue order, memory with bounded
    /// backfill (work behind a head that waits for memory may start if it
    /// fits, until the head has waited HEAD_WAIT).
    fn dispatch(&mut self) {
        let mut i = 0;
        let mut elsewhere: Option<HashMap<String, f64>> = None; // tenants' CPUs on other peers, read once
        while i < self.queue.len() && i <= BACKFILL {
            self.refresh_cpus(i);
            // A gang rank or a bundle's host runs on the slot held for it
            // (core/holds.rs), never on free capacity, and not at all once
            // that slot is gone: then it is lost, as to a failure.
            let (run, launch) = (self.queue[i].tk.run, self.queue[i].policy.launch.clone());
            let slot = match launch.map(|t| self.slot_of(run, &t)) {
                None => None,
                Some(Ok((cpus, held))) => {
                    self.queue.get_mut(i).expect("index in range").cpus = cpus;
                    Some(held)
                }
                Some(Err(why)) => {
                    let w = self.queue.remove(i).expect("index in range");
                    self.fail_attempt(&w, pb::record_failure::Kind::Infra, &why);
                    continue;
                }
            };
            let w = &self.queue[i];
            let (cpus, floor, waited) = (w.cpus, w.mem_floor, w.ready_at.elapsed());
            if cpus > self.capacity + 1e-9 {
                let w = self.queue.remove(i).expect("index in range");
                let why = format!("needs {cpus} CPUs; this peer offers {} (largest feasible shape)", self.capacity);
                self.fail_infeasible(&w, &why);
                continue;
            }
            if floor > self.budget.pool {
                // It ran out of memory: its retry needs more than the pool.
                let w = self.queue.remove(i).expect("index in range");
                self.propose(
                    w.tk.v(),
                    pb::shard_cmd::Body::Cancel(pb::Cancel {
                        task: Some(w.tk.pb()),
                        reason: Kind::Oom as i32,
                        message: format!(
                            "out of memory: an attempt was killed for memory and its retry needs {} MiB, \
                             more than this peer's memory pool of {} MiB",
                            floor / MIB,
                            self.budget.pool / MIB
                        ),
                    }),
                );
                continue;
            }
            if slot.is_none() && self.cpu_used + cpus > self.capacity + 1e-9 {
                if self.delegate(i, cpus) {
                    continue; // it runs in another cell; look at the next
                }
                return;
            }
            if self.quotas.enabled() {
                let tenant = self.tenant_of(self.queue[i].tk.run);
                let others = if self.quotas.has_share(&tenant) {
                    elsewhere.get_or_insert_with(|| self.tenants_elsewhere()).get(&tenant).copied().unwrap_or(0.0)
                } else {
                    0.0
                };
                match self.quotas.admit(&tenant, cpus, others) {
                    crate::quotas::Admit::Yes => {}
                    crate::quotas::Admit::Wait(_) => {
                        i += 1; // other tenants' work may go ahead
                        continue;
                    }
                    crate::quotas::Admit::Never(why) => {
                        let w = self.queue.remove(i).expect("index in range");
                        self.fail_infeasible(&w, &why);
                        continue;
                    }
                }
            }
            self.refresh_memory(i);
            if self.memory_wait(&self.queue[i]).is_some() {
                if i == 0 && waited > HEAD_WAIT {
                    return;
                }
                i += 1;
                continue;
            }
            let w = &self.queue[i];
            if w.spec.effects() == pb::Effects::External && !w.start_consumed {
                // User code with external effects starts only after its start
                // is committed: a later loss then reports OutcomeUnknown.
                let w = self.queue.remove(i).expect("index in range");
                self.consume_start(w);
                continue;
            }
            if w.policy.accounting() == pb::policy::Accounting::Isolated {
                if self.devices.wait_reason(w.policy.gpus, &w.policy.device).is_some() {
                    i += 1; // its devices are held: it waits, as any call
                    continue;
                }
                let w = self.queue.remove(i).expect("index in range");
                self.start_isolated(w);
                continue;
            }
            let (run, gpus, device) = (w.tk.run, w.policy.gpus, w.policy.device.clone());
            let Some(env) = self.env_of(&run) else {
                let w = self.queue.remove(i).expect("index in range");
                self.fail_infeasible(&w, "no executor environment is registered for this run");
                continue;
            };
            let threads = cpus.ceil().max(1.0) as u32;
            let want = match &slot {
                Some(held) => Want::Key(self.devices.key_of(&env.key(threads), held)),
                None => self.devices.key_for(gpus, &device, &env.key(threads)),
            };
            let key = match want {
                Want::None => env.key(threads),
                Want::Key(key) => key,
                Want::Wait => {
                    i += 1;
                    continue;
                }
                Want::Infeasible(why) => {
                    let w = self.queue.remove(i).expect("index in range");
                    self.fail_infeasible(&w, &why);
                    continue;
                }
            };
            let (digest, run) = (self.queue[i].spec.fn_digest.clone(), self.queue[i].tk.run);
            let Some(exec) = self.idle_exec(&key, &digest, run).or_else(|| self.host_exec(&key, &self.queue[i])) else {
                if let Some(why) = self.start_failures.gave_up(&key) {
                    let w = self.queue.remove(i).expect("index in range");
                    self.fail_infeasible(&w, &why);
                    continue;
                }
                // Other pools need not wait for this one's executor to start.
                self.ensure_executor(&env, threads, &key, cpus);
                i += 1;
                continue;
            };
            let batch = self.take_batch(i);
            if let Some(t) = &batch[0].policy.launch {
                let _ = self.take_slot(run, t); // found above in this pass: its CPUs and devices are the call's now
            }
            self.devices.hold(exec, &key);
            self.start(exec, batch);
        }
    }

    /// CPU and memory for an attempt: the caller's values, else what this
    /// function's calls have used (profiles), within `ch.auto` bounds and
    /// raised to an OOM retry's hint. `placer_*`: what the placing peer
    /// reserved, used when this peer has not seen the function.
    fn reserve(&mut self, w: &mut Work, hint: u64, placer_cpus: f64, placer_mem: u64) {
        let dep_bytes: u64 = w
            .deps
            .iter()
            .filter_map(|o| match &o.value {
                Some(pb::outcome::Value::Ok(obj)) => Some(obj.size),
                _ => None,
            })
            .sum();
        let blob_bytes: u64 = w.spec.blobs.iter().map(|b| b.size).sum();
        w.read_bytes = w.spec.args.as_ref().map_or(0, |a| a.size) + blob_bytes + dep_bytes;
        if matches!(w.kind(), pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod) {
            return; // runs inside the actor's own grant
        }
        let p = &w.policy;
        let req = Request::of(p);
        let r = self.profiles.reserve(&w.key(), &p.fn_name, w.read_bytes, req);
        let clip = |x: f64, lo: f64, hi: f64| {
            let x = if lo > 0.0 { x.max(lo) } else { x };
            if hi > 0.0 { x.min(hi) } else { x }
        };
        let mut cpus = r.cpus;
        if r.cpus_source != Source::User {
            if r.cpus_source == Source::Default && placer_cpus > 0.0 {
                cpus = placer_cpus;
            }
            // A learned value never makes a call infeasible here.
            cpus = clip(cpus, p.cpus_min, p.cpus_max).min(self.capacity.max(0.25));
        }
        let mut mem = r.memory;
        if r.memory_source != Source::User {
            mem = clip(mem as f64, p.memory_min as f64, p.memory_max as f64) as u64;
        }
        let mem = mem.max(hint).max(placer_mem);
        w.mem_source = if hint > 0 && hint >= mem {
            "raised after an attempt was killed for memory".to_string()
        } else {
            r.memory_source.describe()
        };
        w.cpus_source = r.cpus_source.describe();
        w.cpus_chosen = crate::settings::Chosen { asked: r.cpus, granted: cpus, source: r.cpus_source.kind() };
        let raised = hint > 0 && hint >= mem;
        let mem_kind = if raised { "raised" } else { r.memory_source.kind() };
        w.mem_chosen = crate::settings::Chosen { asked: r.memory as f64, granted: mem as f64, source: mem_kind };
        w.placer = (placer_cpus, placer_mem);
        (w.cpus, w.mem, w.mem_floor, w.arm, w.est_s) = (cpus, mem, hint, r.arm, r.est_s);
    }

    /// Queued work takes the CPU grant learning has now (a trial's next arm,
    /// or a learned grant): a burst submitted before the function was
    /// measured would otherwise all run at the default.
    fn refresh_cpus(&mut self, i: usize) {
        let w = &self.queue[i];
        if !Request::of(&w.policy).learns_cpus()
            || !w.publish
            || matches!(w.kind(), pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod)
        {
            return;
        }
        let req = Request::of(&w.policy);
        let (lo, hi) = (w.policy.cpus_min, w.policy.cpus_max);
        let Some((asked, source, arm)) = self.profiles.cpus_now(&w.key(), req, w.arm) else { return };
        let cpus = if hi > 0.0 { asked.min(hi) } else { asked }.max(lo).min(self.capacity.max(0.25));
        let w = self.queue.get_mut(i).expect("index in range");
        w.cpus_chosen = crate::settings::Chosen { asked, granted: cpus, source: source.kind() };
        (w.cpus, w.arm, w.cpus_source) = (cpus, arm, source.describe());
    }

    /// Queued work admitted before its function had a memory estimate takes
    /// one as soon as the profile has it.
    fn refresh_memory(&mut self, i: usize) {
        let w = &self.queue[i];
        if !w.learns_memory() {
            return;
        }
        let Some(m) = self.profiles.memory_estimate(&w.key(), w.read_bytes) else { return };
        let (lo, hi) = (w.policy.memory_min, w.policy.memory_max);
        let w = self.queue.get_mut(i).expect("index in range");
        w.mem = if hi > 0 { m.bytes.min(hi) } else { m.bytes }.max(lo);
        w.mem_chosen = crate::settings::Chosen { asked: m.bytes as f64, granted: w.mem as f64, source: "learned" };
        w.mem_source = Source::Learned(m.evidence).describe();
    }

    /// Memory charged against the pool now: each executor its reservation
    /// while it runs (or what it holds, if more), what it holds while idle or
    /// while its call waits on others, plus objects in tmpfs.
    fn memory_used(&self) -> u64 {
        let mut used = if self.budget.objects_in_pool { self.object_bytes } else { 0 };
        for (id, e) in &self.execs {
            let reserved = self
                .running
                .iter()
                .filter(|r| r.exec == *id && !r.blocked)
                .map(|r| {
                    // A call admitted before its function had an estimate is
                    // charged the estimate once there is one: a just-started
                    // executor reads 0 until its first sample.
                    let w = &r.items[0];
                    let mem = if r.mem > 0 {
                        r.mem
                    } else {
                        self.profiles.memory_estimate(&w.key(), w.read_bytes).map_or(0, |m| m.bytes)
                    };
                    // A guest's reservation comes on top of what its process
                    // held when it started (its waiting host's memory).
                    r.baseline + mem
                })
                .max()
                .unwrap_or(0);
            used += reserved.max(e.private + e.arena);
        }
        used
    }

    /// Memory to reserve for `w` now: its estimate or, for a function without
    /// one yet, 2× its live floor (one finished call's peak, or what a call
    /// running past the probe holds; the margin for an unknown spread).
    fn need(&self, w: &Work) -> u64 {
        if w.in_actor() {
            return 0; // charged as the actor's process, whatever its calls hold
        }
        let m = if w.mem > 0 { w.mem } else { self.live_floor.get(&w.key()).map_or(0, |f| f.saturating_mul(2)) };
        m.min(self.budget.pool)
    }

    /// How many calls of a function (or actors of a class) not yet measured
    /// start at once: a quarter of the CPUs, but no more than the free pool
    /// holds at PROBE_MEM each.
    fn probe_width(&self) -> usize {
        let free = self.budget.pool.saturating_sub(self.memory_used());
        ((self.capacity / 4.0) as usize).min((free / PROBE_MEM) as usize).max(1)
    }

    /// Why `w` cannot start yet for lack of memory, or None.
    fn memory_wait(&self, w: &Work) -> Option<String> {
        // With nothing of ours running, waiting frees nothing: run it alone.
        if !self.running.iter().any(|r| !r.blocked) {
            return None;
        }
        if let Some(why) = budget::disk_wait(self.disk_space, self.objects.dir()) {
            return Some(why);
        }
        if self.mem_paused {
            return Some(format!(
                "memory pressure on this host ({} MiB available); new work waits",
                self.available.unwrap_or(0) / MIB
            ));
        }
        let need = self.need(w);
        if need == 0 {
            // Nothing measured yet (no call has finished or run past
            // PROBE_FOR): a few calls at a time.
            // Beyond that, any function's probes may use the free pool at
            // PROBE_SLOT each.
            let key = w.key();
            let limit = self.probe_width();
            let probes = self.running.iter().filter(|r| r.mem == 0);
            let (mine, all) = probes.fold((0, 0), |(m, a), r| (m + usize::from(r.items[0].key() == key), a + 1));
            let slots = (self.budget.pool.saturating_sub(self.memory_used()) / PROBE_SLOT) as usize;
            return (mine >= limit && all >= slots).then(|| {
                format!("a function not yet measured runs {limit} call(s) at a time until its memory use is known")
            });
        }
        let used = self.memory_used();
        if used + need > self.budget.pool {
            // Live actors hold their memory between calls: say so, since the
            // usual fix is closing the ones no longer needed.
            let actors: Vec<u64> =
                self.execs.values().filter(|e| e.actor.is_some()).map(|e| e.private + e.arena).collect();
            let held: u64 = actors.iter().sum();
            let by_actors = if held > 0 {
                format!(
                    ", {} MiB of it held by {} live actor(s); closing ones you are done with (handle.close()) frees it",
                    held / MIB,
                    actors.len()
                )
            } else {
                String::new()
            };
            return Some(format!(
                "needs {} MiB of memory ({}); {} of {} MiB in use{by_actors}",
                need / MIB,
                w.reservation(),
                used / MIB,
                self.budget.pool / MIB
            ));
        }
        if let Some(avail) = self.available
            && avail < need + self.budget.pool / 20
        {
            return Some(format!("needs {} MiB of memory; {} MiB available on this host", need / MIB, avail / MIB));
        }
        None
    }

    /// Why queued work `w` has not started.
    fn wait_reason(&self, w: &Work) -> String {
        let leased = w.policy.launch.is_some(); // its CPUs and devices are held for it
        if !leased && self.cpu_used + w.cpus > self.capacity + 1e-9 {
            // Actors given cpus= keep them while idle: name that, since it is
            // the usual reason work waits forever.
            let holders: Vec<f64> =
                self.execs.values().filter(|e| e.actor.is_some() && e.held_cpus > 0.0).map(|e| e.held_cpus).collect();
            let by_actors = if holders.is_empty() {
                String::new()
            } else {
                format!(
                    ", {:.2} of it held by {} actor(s) for as long as they live (their cpus= setting)",
                    holders.iter().sum::<f64>(),
                    holders.len()
                )
            };
            let held = self.ledger.cpus();
            let by_holds =
                if held > 0.0 { format!(", {held:.2} of it held for gangs and reservations") } else { String::new() };
            return format!(
                "needs {} CPU ({}); {:.2} of {:.2} in use{by_actors}{by_holds}",
                w.cpus,
                w.reservation(),
                self.cpu_used,
                self.capacity
            );
        }
        if let Some(why) = self.devices.wait_reason(w.policy.gpus, &w.policy.device).filter(|_| !leased) {
            return why;
        }
        self.quota_note(w)
            .or_else(|| self.memory_wait(w))
            .or_else(|| self.pids_note.clone())
            .unwrap_or_else(|| "waiting for an executor to start".into())
    }

    /// The tenant quota or share that keeps `w` waiting here, if one does.
    fn quota_note(&self, w: &Work) -> Option<String> {
        let tenant = self.quotas.tenant_of(w.tk.run)?;
        let others = if self.quotas.has_share(tenant) {
            self.tenants_elsewhere().get(tenant).copied().unwrap_or(0.0)
        } else {
            0.0
        };
        match self.quotas.admit(tenant, w.cpus, others) {
            crate::quotas::Admit::Wait(why) => Some(why),
            _ => None,
        }
    }

    fn consume_start(&self, w: Work) {
        let p = self.router.clone();
        let tx = self.me_tx.clone();
        tokio::spawn(async move {
            let body = pb::shard_cmd::Body::StartExternal(pb::StartExternal {
                task: Some(w.tk.pb()),
                attempt: w.attempt,
                peer: Some(p.identity.pb()),
            });
            let accepted = matches!(
                p.propose(w.tk.v(), body).await,
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(ref t)) })
                    if t.acks.first().is_some_and(|a| a.status() == pb::task_ack::Status::Accepted)
            );
            let _ = tx.send(CoreMsg::StartResult { work: Box::new(w), accepted }).await;
        });
    }

    /// An isolated attempt gets a fresh executor inside its own cgroup, so its
    /// CPU and memory readings (and its hard memory limit) are its own.
    fn start_isolated(&mut self, w: Work) {
        let Some(env) = self.env_of(&w.tk.run) else {
            self.fail_infeasible(&w, "the run has no executor environment on record");
            return;
        };
        let Some(iso) = &self.isolation else {
            self.fail_infeasible(&w, "accounting='isolated' needs cgroup v2 delegation; this peer has none");
            return;
        };
        self.iso_seq += 1;
        let group = match iso.group(self.iso_seq, w.policy.memory_limit, w.cpus) {
            Ok(g) => g,
            Err(e) => {
                self.fail_infeasible(&w, &format!("cannot create the attempt's cgroup: {e}"));
                return;
            }
        };
        let threads = w.cpus.ceil() as u32;
        let base = format!("isolated-{}", self.iso_seq);
        let key = match self.devices.key_for(w.policy.gpus, &w.policy.device, &base) {
            Want::Infeasible(why) => {
                self.fail_infeasible(&w, &why);
                return;
            }
            Want::Key(key) => key,
            Want::None | Want::Wait => base, // Wait: dispatch checked its devices are free
        };
        let mut extra = vec![("CHIMAERA_PEER_SOCKET", Some(self.peer_socket.clone()))];
        extra.extend(self.devices.env_for_key(&key));
        match self.sup.spawn(&env, threads, &extra, Some(&group), &self.devices.numa_for_key(&key)) {
            Ok((id, pid)) => {
                self.devices.hold(id, &key);
                self.cpu_used += w.cpus;
                self.execs.insert(
                    id,
                    Exec {
                        pid,
                        env_key: key,
                        held_cpus: w.cpus,
                        concurrency: 1,
                        cgroup: Some(group),
                        state: ExecState::Starting,
                        actor: None,
                        idle_since: Instant::now(),
                        killed_for_memory: None,
                        kill_note: None,
                        last_fn: Vec::new(),
                        last_run: None,
                        private: 0,
                        arena: 0,
                        leak: LeakWatch::default(),
                        cpu_mark: 0,
                        killed_for: None,
                    },
                );
                self.iso_waiting.insert(id, w);
            }
            Err(e) => {
                Isolation::remove(&group);
                self.fail_infeasible(&w, &format!("cannot start an isolated executor: {e}"));
            }
        }
    }

    /// An idle pooled executor for `key`: one that last ran this function,
    /// else one that last ran a call of this run (its modules are loaded),
    /// else the oldest.
    fn idle_exec(&self, key: &str, fn_digest: &[u8], run: Id) -> Option<u32> {
        self.execs
            .iter()
            .filter(|(_, e)| e.actor.is_none() && e.state == ExecState::Idle && e.env_key == key)
            .min_by_key(|(id, e)| (e.last_fn.as_slice() != fn_digest, e.last_run != Some(run), **id))
            .map(|(id, _)| *id)
    }

    /// A pooled executor of `key` whose calls, all of `w`'s run, wait on
    /// futures and offered to host: `w` runs there as a guest, on a thread
    /// beside them, instead of in a process of its own (sharing). Prefers one
    /// waiting on `w` itself: the parent runs its own child. (A device-holding
    /// executor never matches: a key names free devices, and its are held.)
    fn host_exec(&self, key: &str, w: &Work) -> Option<u32> {
        if !sharing::enabled() || !w.guest_ok() || !self.running.iter().any(|r| r.blocked && r.host) {
            return None;
        }
        let mut found: Option<(bool, u32)> = None;
        for (id, e) in &self.execs {
            if e.actor.is_some() || e.cgroup.is_some() || e.state != ExecState::Busy || e.env_key != key {
                continue;
            }
            let on: Vec<&Running> = self.running.iter().filter(|r| r.exec == *id).collect();
            let entries: Vec<sharing::Entry> =
                on.iter().map(|r| sharing::Entry { blocked: r.blocked, host: r.host, items: r.items.len() }).collect();
            if !sharing::can_host(&entries) || on.iter().any(|r| r.items[0].tk.run != w.tk.run) {
                continue;
            }
            let rank = (!on.iter().any(|r| r.waiting_on.contains(&w.tk)), *id);
            found = Some(found.map_or(rank, |f| f.min(rank)));
        }
        found.map(|(_, id)| id)
    }

    /// What `pid`'s NumPy arena holds now (Linux: arrays allocated as tmpfs
    /// files), read fresh: the 100 ms sample can miss one just allocated.
    fn arena_bytes(&self, pid: u32) -> u64 {
        crate::stores::dir_bytes(&self.objects.dir().join(".arena").join(pid.to_string()))
    }

    /// A guest starts on `exec`: from now its process is measured in slices.
    /// The calls there ran alone until now, so each gets what the process
    /// used since its start reading, and what it holds now. Returns the
    /// guest's memory baseline: that, read fresh (a host may have allocated
    /// just before it waited, after the last sample).
    fn begin_sharing(&mut self, exec: u32) -> u64 {
        self.slice(exec);
        let Some((pid, last)) = self.execs.get(&exec).map(|e| (e.pid, e.private)) else { return 0 };
        let held = budget::private_bytes(pid).unwrap_or(last) + self.arena_bytes(pid);
        let now = accounting::read(pid).map(|r| r.cpu_ns);
        for r in self.running.iter_mut().filter(|r| r.exec == exec && !r.sliced) {
            r.sliced = true;
            r.cpu_own = r.start.zip(now).map(|(s, n)| n.saturating_sub(s.cpu_ns));
            r.peak = r.peak.max(held);
        }
        if let (Some(n), Some(e)) = (now, self.execs.get_mut(&exec)) {
            e.cpu_mark = n;
        }
        held
    }

    /// Before the calls on a shared executor change (one starts, ends, waits
    /// or resumes): the CPU the process used since the last change, and the
    /// memory it holds now, go to the one call that ran meanwhile
    /// (`sharing::attribute_cpu`, `attribute_memory`).
    fn slice(&mut self, exec: u32) {
        if !self.running.iter().any(|r| r.exec == exec && r.sliced) {
            return;
        }
        let Some((pid, mark)) = self.execs.get(&exec).map(|e| (e.pid, e.cpu_mark)) else { return };
        let now = accounting::read(pid).map(|r| r.cpu_ns);
        let held = budget::private_bytes(pid).map(|p| p + self.arena_bytes(pid));
        let mut on: Vec<&mut Running> = self.running.iter_mut().filter(|r| r.exec == exec && r.sliced).collect();
        let active: Vec<bool> = on.iter().map(|r| !r.blocked).collect();
        let mut own: Vec<Option<u64>> = on.iter().map(|r| r.cpu_own).collect();
        match now {
            Some(n) => sharing::attribute_cpu(n.saturating_sub(mark), &active, &mut own),
            None => own.iter_mut().for_each(|o| *o = None),
        }
        for (r, o) in on.iter_mut().zip(own) {
            r.cpu_own = o;
        }
        let baselines: Vec<u64> = on.iter().map(|r| r.baseline).collect();
        if let Some((i, mem)) = held.and_then(|h| sharing::attribute_memory(h, &active, &baselines)) {
            on[i].peak = on[i].peak.max(mem);
        }
        if let (Some(n), Some(e)) = (now, self.execs.get_mut(&exec)) {
            e.cpu_mark = n;
        }
    }

    /// A new run's first calls should find warm executors: start a few
    /// single-thread ones for its environment now (Ray starts one per CPU at
    /// boot; a few keeps idle processes cheap, and demand grows the pool).
    fn prestart(&mut self, env: &ExecEnv) {
        let key = env.key(1);
        let have = self.execs.values().filter(|e| e.actor.is_none() && e.env_key == key).count();
        let want = (self.capacity.floor() as usize).clamp(1, PRESTART);
        for _ in have..want {
            self.spawn_exec(env, 1, &key, None);
        }
    }

    /// Start executors for a pool with queued work, several at once: as many
    /// as the queued work that free CPU could run now (an interpreter takes
    /// 100 ms to seconds to start, so one at a time serializes a cold burst).
    fn ensure_executor(&mut self, env: &ExecEnv, threads: u32, key: &str, cpus: f64) {
        let starting = self.execs.values().filter(|e| e.state == ExecState::Starting && e.env_key == key).count();
        // Held slots count as free: the ranks and bundles queued for them need executors too.
        let free = ((self.capacity - self.cpu_used + self.ledger.cpus()) / cpus.max(0.01)).floor().max(1.0) as usize;
        // Only up to `free` matter below: not a count of the whole queue each time.
        let queued = self.queue.iter().filter(|w| w.cpus.ceil().max(1.0) as u32 == threads).take(free).count();
        if starting >= queued.min(free).max(1) {
            return;
        }
        // Make room: retire an idle executor of another environment.
        let pool = self.execs.values().filter(|e| e.actor.is_none()).count();
        let blocked = self.running.iter().filter(|r| r.blocked).count();
        let limit = self.capacity.ceil() as usize + blocked + 1;
        if pool >= limit
            && let Some(victim) = self
                .execs
                .iter()
                .filter(|(_, e)| e.actor.is_none() && e.state == ExecState::Idle && e.env_key != key)
                .map(|(id, _)| *id)
                .next()
        {
            self.retire_exec(victim);
        }
        self.spawn_exec(env, threads, key, None);
    }

    fn spawn_exec(&mut self, env: &ExecEnv, threads: u32, key: &str, actor: Option<Id>) -> Option<u32> {
        // Under a task limit (a pod's pids.max), starting a worker that cannot
        // fit fails with EAGAIN and loses the attempt: wait for one to exit
        // instead. The first worker is always tried, or nothing could run.
        if !self.execs.is_empty()
            && let Some((current, max)) = crate::topology::pids_room()
            && current + EXEC_TASKS + u64::from(threads) > max
        {
            self.pids_note = Some(pids_full(current, max));
            return None;
        }
        let mut extra = vec![("CHIMAERA_PEER_SOCKET", Some(self.peer_socket.clone()))];
        extra.extend(self.devices.env_for_key(key));
        match self.sup.spawn(env, threads, &extra, None, &self.devices.numa_for_key(key)) {
            Ok((id, pid)) => {
                self.execs.insert(
                    id,
                    Exec {
                        pid,
                        env_key: key.to_string(),
                        held_cpus: 0.0,
                        concurrency: 1,
                        cgroup: None,
                        state: ExecState::Starting,
                        actor,
                        idle_since: Instant::now(),
                        killed_for_memory: None,
                        kill_note: None,
                        last_fn: Vec::new(),
                        last_run: None,
                        private: 0,
                        arena: 0,
                        leak: LeakWatch::default(),
                        cpu_mark: 0,
                        killed_for: None,
                    },
                );
                self.pids_note = None;
                self.start_failures.spawned(id, key);
                Some(id)
            }
            Err(e) => {
                if e.raw_os_error() == Some(libc::EAGAIN) {
                    let (current, max) = crate::topology::pids_room().unwrap_or((0, 0));
                    self.pids_note = Some(pids_full(current, max));
                }
                tracing::error!(
                    "cannot start executor with {}: {e}{}",
                    env.python,
                    self.pids_note.as_deref().map(|n| format!(" ({n})")).unwrap_or_default()
                );
                None
            }
        }
    }

    fn retire_exec(&mut self, id: u32) {
        self.sup.send(id, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Shutdown(pb::ExecShutdown {})) });
        if let Some(e) = self.execs.get_mut(&id) {
            e.state = ExecState::Starting; // not dispatchable; removed on exit
        }
    }

    /// Head of the queue plus, for fast map items of the same function, more
    /// of the same (granularity controller A1).
    /// The work at `at` plus, for fast calls of the same function, more of the
    /// same right behind it (granularity, A1).
    fn take_batch(&mut self, at: usize) -> Vec<Work> {
        let first = self.queue.remove(at).expect("caller checked");
        // The profile's batch counts calls of the function; a map chunk is
        // already `items` of them.
        // Learned batching is learning: an observed or frozen call runs alone.
        let k = if first.batchable() && first.policy.adaptive() == pb::policy::Adaptive::On {
            let profile =
                (self.profiles.batch(&first.key()) / first.policy.items.max(1) as usize).clamp(1, profiles::MAX_BATCH);
            // Never more than an even share of the queue for each free slot:
            // a batch runs its calls one after another, so while CPUs sit
            // idle a deep batch only delays the calls at its back. Sixteen
            // 5 ms calls on a 16-CPU peer went to one executor as a 200 ms
            // batch and took 80 ms instead of 5. A long queue (a large map)
            // still gets the profile's batches.
            let free = ((self.capacity - self.cpu_used) / first.cpus.max(0.01)).floor().max(1.0) as usize;
            profile.min((self.queue.len() + 1).div_ceil(free))
        } else {
            1
        };
        let mut batch = vec![first];
        while batch.len() < k {
            match self.queue.get(at) {
                Some(w) if batch[0].batches_with(w) => batch.push(self.queue.remove(at).expect("present")),
                _ => break,
            }
        }
        let n = batch.len() as u32;
        for w in &mut batch {
            w.batched = n;
        }
        batch
    }

    fn start(&mut self, exec: u32, items: Vec<Work>) {
        self.note_started(&items);
        let head = items[0].clone();
        if let Some(e) = self.execs.get_mut(&exec) {
            e.last_fn.clone_from(&head.spec.fn_digest);
            e.last_run = Some(head.tk.run);
        }
        // Calls on an actor run inside the CPU the actor already holds.
        let cpus = if self.execs.get(&exec).is_some_and(|e| e.actor.is_some() || e.cgroup.is_some()) {
            0.0
        } else {
            head.cpus
        };
        let msg = pb::Execute {
            kind: head.spec.kind,
            fn_digest: head.spec.fn_digest.clone(),
            method: head.spec.method.clone(),
            actor: head.spec.actor.clone(),
            items: items
                .iter()
                .map(|w| pb::ExecItem {
                    task: Some(w.tk.pb()),
                    attempt: w.attempt,
                    args: w.spec.args.clone(),
                    deps: w.deps.clone(),
                    item_index: w.spec.item_index,
                })
                .collect(),
            policy: Some(head.policy.clone()),
            deadline_unix_ms: if head.spec.deadline_hlc == 0 { 0 } else { hlc::wall_ms(head.spec.deadline_hlc) },
            run: head.tk.run.to_vec(),
            retry_exception_types: head.spec.retry_exception_types.clone(),
            // A durable actor's constructor and methods return its state too.
            durable: matches!(head.kind(), pb::CallKind::CallActorMethod | pb::CallKind::CallActorCtor)
                && Id::from_slice(&head.spec.actor).ok().and_then(|a| self.actors.get(&a)).is_some_and(|h| h.durable),
        };
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        let alone = !self.running.iter().any(|r| r.exec == exec);
        // Linux: reset the executor's peak-RSS mark, so this attempt's own peak
        // can be read when it ends (clear_refs, kernel 4.0+).
        let hwm_reset = cfg!(target_os = "linux")
            && alone
            && pid > 0
            && std::fs::write(format!("/proc/{pid}/clear_refs"), "5").is_ok();
        if !self.sup.send(exec, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Execute(msg)) }) {
            // Executor vanished between ready and dispatch; requeue.
            for w in items.into_iter().rev() {
                self.queue.push_front(w);
            }
            self.execs.remove(&exec);
            return;
        }
        self.cpu_used += cpus;
        if self.quotas.enabled() {
            let tenant = self.tenant_of(head.tk.run);
            self.quotas.take((head.tk.run, head.tk.task), &tenant, cpus);
        }
        let waited = head.ready_at.elapsed().as_secs_f64() * 1e6;
        self.dispatch_us.push_back(waited);
        if self.dispatch_us.len() > 10_000 {
            self.dispatch_us.pop_front();
        }
        let remaining = items.len();
        // Overlapping calls on one process share its CPU counters, so a delta
        // is only exact when the call runs alone; a guest beside waiting calls
        // is measured in slices instead (sharing).
        let start = if alone { accounting::read(pid) } else { None };
        let guest = !alone && self.execs.get(&exec).is_some_and(|e| e.actor.is_none() && e.cgroup.is_none());
        let baseline = if guest { self.begin_sharing(exec) } else { 0 };
        // Charged from the start: calls admitted on a live floor read 0
        // until their first sample, and one pass may admit many.
        let mem = items.iter().map(|w| self.need(w)).max().unwrap_or(0);
        for w in &items {
            *self.unfinished.entry(w.tk).or_default() += 1;
            *self.batched.entry(w.tk).or_default() += 1;
        }
        self.running.push(Running {
            exec,
            finished: vec![false; items.len()],
            items,
            remaining,
            cpus,
            blocked: false,
            start,
            dispatched_ns: now_ns(),
            mem,
            peak: 0,
            started: Instant::now(),
            hwm_reset,
            unlearned: Vec::new(),
            host: false,
            waiting_on: Vec::new(),
            sliced: guest,
            cpu_own: guest.then_some(0),
            baseline,
        });
        let inflight = self.running.iter().filter(|r| r.exec == exec).count();
        if let Some(e) = self.execs.get_mut(&exec)
            && inflight >= e.concurrency
        {
            e.state = ExecState::Busy;
        }
    }

    fn fail_infeasible(&mut self, w: &Work, why: &str) {
        self.propose(
            w.tk.v(),
            pb::shard_cmd::Body::Cancel(pb::Cancel {
                task: Some(w.tk.pb()),
                reason: Kind::Infeasible as i32,
                message: format!("infeasible: {why}"),
            }),
        );
    }

    // ---- executor events --------------------------------------------------

    fn on_exec(&mut self, e: ExecEvent) {
        match e {
            ExecEvent::Ready { exec } => {
                self.start_failures.ready(exec);
                if let Some(x) = self.execs.get_mut(&exec) {
                    x.state = ExecState::Idle;
                    x.idle_since = Instant::now();
                    if let Some(actor) = x.actor {
                        self.actor_exec_ready(actor, exec);
                    }
                }
                if let Some(w) = self.iso_waiting.remove(&exec) {
                    self.start(exec, vec![w]);
                }
            }
            ExecEvent::Done { exec, done } => self.on_exec_done(exec, *done),
            ExecEvent::Blocked { exec, msg } => {
                let task = msg.task.as_ref().and_then(Tk::parse);
                if let Some(tk) = task {
                    self.note_actor_wait(exec, tk, &msg.waiting_on, msg.blocked);
                }
                if let Some(i) =
                    self.running.iter().position(|r| r.exec == exec && r.items.iter().any(|w| Some(w.tk) == task))
                {
                    // Waiting or resuming changes which call the process's CPU is
                    // charged to; a wait that offers to host lets a guest in (sharing).
                    if self.running[i].blocked != msg.blocked {
                        self.slice(exec);
                    }
                    let r = &mut self.running[i];
                    r.host = msg.blocked && msg.host;
                    r.waiting_on =
                        if msg.blocked { msg.waiting_on.iter().filter_map(Tk::parse).collect() } else { Vec::new() };
                }
                if let Some(r) =
                    self.running.iter_mut().find(|r| r.exec == exec && r.items.iter().any(|w| Some(w.tk) == task))
                    && r.blocked != msg.blocked
                {
                    r.blocked = msg.blocked;
                    let head = r.items.first().map(|w| (w.tk.run, w.tk.task));
                    // A task waiting on other futures lends out its CPU so the
                    // work it waits for can run (it keeps its memory charge).
                    if msg.blocked {
                        self.cpu_used -= r.cpus;
                    } else {
                        self.cpu_used += r.cpus;
                    }
                    if let Some(call) = head {
                        if msg.blocked {
                            self.quotas.pause(call);
                        } else {
                            self.quotas.resume(call);
                        }
                    }
                    if msg.blocked
                        && let Some(tk) = task
                    {
                        self.note_nested_waits(exec, tk);
                    }
                }
            }
            ExecEvent::Log { exec, log } => self.forward_log(exec, log),
            ExecEvent::Yield { item, .. } => self.on_yield(item),
            ExecEvent::Exited { exec, detail, signal } => self.on_exec_exit(exec, detail, signal),
        }
    }

    /// Hand a generator's item to the sessions watching the call. Generator
    /// calls run on their consumer's peer, so the watchers are local.
    fn on_yield(&mut self, item: pb::ExecYield) {
        let Some(tk) = item.task.as_ref().and_then(Tk::parse) else { return };
        let Some(ids) = self.watchers.get(&tk) else { return };
        let ev = pb::SessionEvent {
            event: Some(pb::session_event::Event::StreamItem(pb::StreamItem {
                task: item.task.clone(),
                attempt: item.attempt,
                index: item.index,
                value: item.value,
            })),
            ..Default::default()
        };
        for id in ids {
            if let Some(s) = self.sessions.get_mut(id) {
                s.item(ev.clone());
            }
        }
    }

    /// Pass the consumer's progress to the executor running the generator.
    fn on_stream_credit(&mut self, tk: Tk, up_to: u64) {
        let Some(exec) = self.running.iter().find(|r| r.items.iter().any(|w| w.tk == tk)).map(|r| r.exec) else {
            return;
        };
        let credit = pb::ExecCredit { task: Some(tk.pb()), up_to };
        self.sup.send(exec, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Credit(credit)) });
    }

    fn forward_log(&mut self, exec: u32, log: pb::ExecLog) {
        let Some(tk) = log.task.as_ref().and_then(Tk::parse) else { return };
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        let ev = pb::LogEvent { task: log.task, stream: log.stream, text: log.text, pid };
        self.deliver_log(tk, &ev);
        // The run's driver may be on another peer: send the output there too.
        let (router, mesh) = (self.router.clone(), self.router.mesh.clone());
        let home = self.log_ship.home(tk.run, || {
            let home = router.read(tk.v(), |kv| read::run_in(kv, tk.v(), tk.run)).flatten()?.home?;
            if home.peer == router.identity.peer.to_vec() {
                return None;
            }
            mesh?.member_by_peer(&home.peer).map(|p| p.member.node_id)
        });
        if let Some(node) = home
            && self.log_ship.add(node, &tk.run.0, ev)
            && let Some(mesh) = &self.router.mesh
        {
            self.log_ship.flush(mesh);
        }
    }

    /// Track what a call of a one-at-a-time actor waits for. Waiting for a
    /// call to the same actor that has not started is waiting forever: that
    /// call can start only after this one ends (Ray hangs). Such a call is
    /// failed at once, saying why, so the waiting `.result()` raises instead;
    /// the awaited calls are kept while it waits, as they may reach the
    /// actor's queue only after it said it waits.
    fn note_actor_wait(&mut self, exec: u32, task: Tk, waiting: &[pb::TaskKey], blocked: bool) {
        if !blocked {
            self.actor_waits.remove(&(exec, task));
            return;
        }
        if self.execs.get(&exec).is_some_and(|e| e.actor.is_some() && e.concurrency <= 1) {
            self.actor_waits.insert((exec, task), waiting.iter().filter_map(Tk::parse).collect());
            self.fail_self_waits(exec);
        }
    }

    /// Fail the queued calls of `exec`'s actor that its own waiting calls
    /// wait for (see note_actor_wait).
    fn fail_self_waits(&mut self, exec: u32) {
        let Some(host) = self.execs.get(&exec).and_then(|e| e.actor).and_then(|a| self.actors.get(&a)) else { return };
        let short = |w: &Work| {
            let parts: Vec<&str> = w.policy.fn_name.rsplit('.').take(2).collect();
            parts.into_iter().rev().collect::<Vec<_>>().join(".")
        };
        let mut fail = Vec::new();
        for ((e, task), waits) in &self.actor_waits {
            if *e != exec {
                continue;
            }
            // Queued here, or already sent to its busy executor in another
            // batch (a call sent straight to the actor is not counted as
            // running here, so the next one may be sent behind it). A call in
            // the waiting call's own batch starts when that one waits.
            let sent = self
                .running
                .iter()
                .filter(|r| r.exec == exec && !r.items.iter().any(|w| w.tk == *task))
                .flat_map(|r| r.items.iter());
            for w in host.mailbox.values().chain(sent).filter(|w| waits.contains(&w.tk)) {
                let waiter = self
                    .running
                    .iter()
                    .filter(|r| r.exec == exec)
                    .flat_map(|r| r.items.iter())
                    .find(|x| x.tk == *task)
                    .map(short)
                    .or_else(|| self.direct.get(task).map(|c| c.name.clone())) // sent to it directly
                    .unwrap_or_else(|| "a call".to_string());
                fail.push((*task, w.tk, waiter, short(w)));
            }
        }
        for (task, tk, waiter, name) in fail {
            if let Some(waits) = self.actor_waits.get_mut(&(exec, task)) {
                waits.retain(|t| *t != tk); // proposed once
            }
            self.propose(
                tk.v(),
                pb::shard_cmd::Body::Cancel(pb::Cancel {
                    task: Some(tk.pb()),
                    reason: Kind::Infeasible as i32,
                    message: format!(
                        "deadlock: {waiter} waits for {name}, a call to the same actor, which runs one call at a \
                         time, so {name} could start only after {waiter} ends. Call the method directly \
                         (self.method(...)), or let the actor run calls at once (concurrency=)"
                    ),
                }),
            );
        }
    }

    /// How many calls here wait on calls they started, and what their worker
    /// processes hold, once there are many: each keeps its process.
    fn nested_waits(&self) -> Option<(usize, u64)> {
        // Processes, not calls: a waiting call's process may run others
        // meanwhile (a guest), so several waits can share one.
        let mut procs: Vec<u32> = self.running.iter().filter(|r| r.blocked).map(|r| r.exec).collect();
        procs.sort_unstable();
        procs.dedup();
        if procs.len() < NESTED_NOTE_AT.max(4 * self.capacity.ceil() as usize) {
            return None;
        }
        let held = procs.iter().filter_map(|x| self.execs.get(x)).map(|e| e.private + e.arena).sum();
        Some((procs.len(), held))
    }

    /// Say once per run, to its driver, when many of its calls wait inside a
    /// call for calls they started: nothing deadlocks (a waiting call lends out
    /// its CPU), but each keeps a worker process, which costs memory and start
    /// time. Ray prints only a count of worker processes.
    fn note_nested_waits(&mut self, exec: u32, tk: Tk) {
        if self.nested_noted.contains(&tk.run) {
            return;
        }
        let Some((n, held)) = self.nested_waits() else { return };
        if self.nested_noted.len() >= 256 {
            self.nested_noted.pop_front();
        }
        self.nested_noted.push_back(tk.run);
        let text = format!(
            "chimaera: {n} worker processes here are held by calls waiting inside a call for calls they started \
             ({} MiB in all; the memory pool is {} MiB). Nothing deadlocks, but each costs memory and start time: \
             return the futures, or pass them to the next call, instead of calling .result() inside a call\n",
            held / MIB,
            self.budget.pool / MIB
        );
        self.forward_log(exec, pb::ExecLog { task: Some(tk.pb()), stream: "stderr".into(), text });
    }

    /// Keep a run's output for `chimaera logs` and pass it to its drivers here.
    fn deliver_log(&mut self, tk: Tk, ev: &pb::LogEvent) {
        if let Err(e) = self.run_logs.append(&self.code, &tk.run.0, &tk.task.0, &ev.stream, &ev.text) {
            tracing::warn!("cannot keep run output: {e}");
        }
        if self.runs.get(&tk.run).is_none_or(|r| r.sessions.is_empty()) {
            return; // no driver here to print it
        }
        if let Some(ev) = self.log_dedup.filter(tk.run, ev, Instant::now()) {
            self.send_log(tk.run, &ev);
        }
    }

    fn send_log(&mut self, run: Id, ev: &pb::LogEvent) {
        if let Some(r) = self.runs.get(&run) {
            for s in &r.sessions {
                if let Some(sess) = self.sessions.get_mut(s) {
                    sess.log(ev);
                }
            }
        }
    }

    /// Output another peer pushed here: this is its run's home.
    fn on_remote_logs(&mut self, lines: Vec<pb::LogEvent>) {
        for ev in lines {
            if let Some(tk) = ev.task.as_ref().and_then(Tk::parse) {
                self.deliver_log(tk, &ev);
            }
        }
    }

    fn on_exec_done(&mut self, exec: u32, done: pb::ExecDone) {
        let Some(tk) = done.task.as_ref().and_then(Tk::parse) else { return };
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        let attempt = done.attempt;
        let this = move |w: &Work| w.tk == tk && w.attempt == attempt;
        let at = |r: &Running| {
            let next = r.items.len().saturating_sub(r.remaining);
            r.items.get(next).filter(|w| this(w)).map(|_| next).or_else(|| r.items.iter().position(this))
        };
        let Some((idx, i)) =
            self.running.iter().enumerate().filter(|(_, r)| r.exec == exec).find_map(|(idx, r)| Some((idx, at(r)?)))
        else {
            return;
        };
        let run = &mut self.running[idx];
        let work = run.items[i].clone();
        let first_report = !std::mem::replace(&mut run.finished[i], true);
        run.remaining = run.remaining.saturating_sub(1);
        if first_report {
            self.item_ended(tk);
        }
        let run = &mut self.running[idx];
        if !work.publish && (self.rebuilding.contains_key(&tk) || self.rebuild_readers.contains_key(&tk)) {
            // After this call's bookkeeping: the waiting attempts change the queue,
            // and the programs reading its value go on.
            let (tx, work, outcome) = (self.me_tx.clone(), Box::new(work.clone()), done.outcome.clone());
            tokio::spawn(async move { tx.send(CoreMsg::Rebuilt { work, outcome }).await });
        }
        let finished = run.remaining == 0;
        let batch_len = run.items.len();
        // A durable actor's constructor commits the first state (version 1),
        // so a later call that raises always has a committed state to go back to.
        // Its reply waits for that commit (below).
        let durable_actor = self.execs.get(&exec).and_then(|e| e.actor).filter(|a| {
            matches!(work.kind(), pb::CallKind::CallActorMethod | pb::CallKind::CallActorCtor)
                && self.actors.get(a).is_some_and(|h| h.durable)
        });
        let mut done = done;
        if let Some(pb::Outcome { value: Some(pb::outcome::Value::Ok(obj)), .. }) = done.outcome.as_mut() {
            // Other peers fetch the bytes from here: the result's file, and the
            // files its frame refers to (an inline frame has only those).
            let me = self.router.identity.pb();
            if obj.inline_data.is_empty() && obj.locations.is_empty() {
                obj.locations.push(me.clone());
            }
            for b in obj.blobs.iter_mut().filter(|b| b.locations.is_empty()) {
                b.locations.push(me.clone());
            }
        }
        if durable_actor.is_none()
            && work.publish
            && !done.retryable
            && done.forward_to.is_none()
            && work.policy.durability() != pb::Durability::Retained // not before its copies exist
            && self.notify_early(tk, done.attempt)
            && let Some(outcome) = done.outcome.as_ref()
        {
            // D2 on a local peer: the caller of an attached run gets the outcome
            // before the accounting below; the publication follows through the
            // group and is the record (on_done finds no watchers left).
            self.notify_watchers(tk, outcome);
        }
        let sliced = self.running[idx].sliced;
        if finished && sliced {
            self.slice(exec); // its last slice, before it leaves
        }
        let alone = self.running.iter().filter(|r| r.exec == exec).count() == 1;
        let end = if finished && (alone || sliced) { accounting::read(pid) } else { None };
        let end_memory = if finished && alone { budget::private_and_held(pid) } else { None };
        let end_private = end_memory.map(|(p, _)| p.saturating_sub(self.running[idx].baseline));
        let (start, end) = if sliced {
            // Its process ran other calls beside it: its own CPU is the sum of
            // its slices (none when two ran at once), read as a delta from 0.
            match (self.running[idx].cpu_own, end) {
                (Some(c), Some(b)) => (Some(Reading { cpu_ns: 0, ..b }), Some(Reading { cpu_ns: c, ..b })),
                _ => (None, None),
            }
        } else {
            (self.running[idx].start, end)
        };
        let dispatched_ns = self.running[idx].dispatched_ns;
        let started = (start.map(|r| r.cpu_ns), self.running[idx].started);
        let share = self.running[idx].cpus / batch_len as f64;
        let measured = end.filter(|_| batch_len == 1).map(|r| r.cpu_ns);
        if self.budgets.charge(exec, tk, done.attempt, &work.policy, started, measured, share, true) {
            self.send_charges(); // before its result: else its caller may finish, and drop them, first
        }
        let sampled_peak = self.running[idx].peak;
        let hwm_reset = self.running[idx].hwm_reset && finished && alone && !sliced;
        let mut unlearned = Vec::new();
        if finished {
            let mut r = self.running.remove(idx);
            self.unbatch(&r.items);
            unlearned = std::mem::take(&mut r.unlearned);
            if !r.blocked {
                self.cpu_used -= r.cpus;
            }
            if let Some(w) = r.items.first() {
                self.quotas.give((w.tk.run, w.tk.task));
            }
            // A pooled executor stays busy while a guest, or its host, still runs there.
            let empty = !self.running.iter().any(|r| r.exec == exec);
            if let Some(x) = self.execs.get_mut(&exec)
                && (x.actor.is_some() || empty)
            {
                x.state = ExecState::Idle;
                x.idle_since = Instant::now();
                if x.actor.is_none() {
                    // A pooled executor's devices go back; an actor keeps its own.
                    self.devices.release(exec);
                }
            }
        }
        let cgroup = self.execs.get(&exec).and_then(|e| e.cgroup.clone());
        let iso = cgroup.as_deref().and_then(Isolation::read);
        let peak = attempt_peak(sampled_peak, end_private, start, end, iso.as_ref(), hwm_reset, work.read_bytes);
        let cores = self.record_attempt(&work, &done, exec, start, end, dispatched_ns, batch_len, iso, peak);
        if cgroup.is_some() {
            // One attempt per isolated executor: retire it (its cgroup goes with it).
            self.retire_exec(exec);
        } else if let Some(p) = end_private {
            // Leaks are judged on what the worker holds, arena arrays included
            // (they are shared tmpfs pages, not private memory).
            let held = end_memory.and_then(|(_, held)| held).unwrap_or(p);
            if let Some(before) = self.execs.get(&exec).and_then(|e| e.leak.last()) {
                // What each call left behind: keeps batches of such calls small.
                self.profiles.note_kept(&work.key(), held.saturating_sub(before) / batch_len.max(1) as u64);
            }
            self.check_leak(exec, p, held);
        }
        let peak_bytes = peak.map_or(0, |p| p.0);
        if cores.is_none() && !finished && batch_len > 1 {
            // The batch found above, unless it moved; then a search.
            let here = self.running.get(idx).is_some_and(|r| r.exec == exec && r.items.get(i).is_some_and(this));
            let mine = |r: &&mut Running| r.exec == exec && r.items.iter().any(this);
            let r = if here { self.running.get_mut(idx) } else { self.running.iter_mut().find(mine) };
            if let Some(r) = r {
                r.unlearned.push((work.clone(), done.clone(), peak_bytes));
            }
        } else {
            // The calls of a finished batch, at the cores the batch used: the
            // last that fit a profile's sample ring (older ones would only be
            // pushed out of it, at a summary's cost each).
            let skip = unlearned.len().saturating_sub(chimaera_core::profile::RING - 1);
            for (w, d, p) in unlearned.into_iter().skip(skip) {
                self.learn(&w, &d, cores, p);
            }
            self.learn(&work, &done, cores, peak_bytes);
        }
        if let Some(actor) = durable_actor
            && work.publish
            && !done.retryable
            && done.forward_to.is_none()
        {
            // The reply goes out only after the state it leaves is committed.
            self.commit_command(actor, work, done);
            return;
        }
        if work.publish {
            if done.retryable {
                self.propose(
                    tk.v(),
                    pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
                        task: Some(tk.pb()),
                        attempt: done.attempt,
                        peer: Some(self.router.identity.pb()),
                        kind: pb::record_failure::Kind::RetryableUser as i32,
                        error: done.outcome.and_then(|o| match o.value {
                            Some(pb::outcome::Value::Err(e)) => Some(e),
                            _ => None,
                        }),
                        fingerprint: done.fingerprint,
                        memory_peak: 0,
                    }),
                );
            } else if done.outcome.is_some() || done.forward_to.is_some() {
                let mut entry = pb::PublishEntry {
                    task: Some(tk.pb()),
                    attempt: done.attempt,
                    peer: Some(self.router.identity.pb()),
                    outcome: done.outcome,
                    forward_to: done.forward_to,
                };
                // The class its result is published with (see replicas).
                if let Some(pb::outcome::Value::Ok(obj)) = entry.outcome.as_mut().and_then(|o| o.value.as_mut()) {
                    obj.durability = work.policy.durability;
                }
                if work.policy.durability() == pb::Durability::Retained {
                    let (mesh, objects, tx) = (self.router.mesh.clone(), self.objects.clone(), self.me_tx.clone());
                    crate::replicas::publish_when_retained(mesh, objects, entry, tx);
                } else {
                    self.pending_publish.entry(tk.v()).or_default().push(entry);
                }
            }
        }
        if let Some(actor) = self.execs.get(&exec).and_then(|e| e.actor) {
            self.actor_after_call(actor, &work, finished);
        }
    }

    /// Commit a durable actor call: its command id, argument fingerprint and
    /// the state it left. A call that raised commits nothing; its error goes
    /// out and the committed state is put back.
    fn commit_command(&mut self, actor: Id, w: Work, done: pb::ExecDone) {
        let reply = done.outcome.clone().unwrap_or_default();
        let Some(host) = self.actors.get_mut(&actor) else { return };
        host.committing = true;
        let Some(pb::outcome::Value::Ok(obj)) = reply.value.clone() else {
            self.on_committed(actor, w, Commit::Raised, reply, Vec::new());
            return;
        };
        // Default command id: the call itself (its retries share it).
        let call_id = [w.tk.run.0, w.tk.task.0].concat();
        let cmd_id = if w.spec.command_id.is_empty() { call_id.clone() } else { w.spec.command_id.clone() };
        let fingerprint = if w.spec.command_fingerprint.is_empty() {
            blake3::hash(&call_id).as_bytes().to_vec()
        } else {
            w.spec.command_fingerprint.clone()
        };
        let commit = pb::ActorCommandCommit {
            actor: actor.to_vec(),
            generation: host.generation,
            cmd_id,
            fingerprint,
            prev_version: host.state_version,
            new_state: Vec::new(),
            reply: Some(obj),
        };
        let stored = host.state_chunks.clone();
        let (router, tx, state) = (self.router.clone(), self.me_tx.clone(), done.durable_state);
        tokio::spawn(async move {
            // Inline, or as chunks when it is too large (actor_state).
            let ack = actor_state::commit(&router, commit, &state, &stored).await;
            let _ = tx.send(CoreMsg::Committed { actor, work: Box::new(w), ack, reply: Box::new(reply), state }).await;
        });
    }

    /// Publish a durable call's outcome according to its commit, and keep
    /// the instance's state equal to the committed one.
    fn on_committed(&mut self, actor: Id, w: Work, commit: Commit, reply: pb::Outcome, state: Vec<u8>) {
        use pb::actor_ack::Status as A;
        let err = |kind: Kind, message: String| pb::Outcome {
            value: Some(pb::outcome::Value::Err(pb::ErrorRecord { kind: kind as i32, message, ..Default::default() })),
            ..Default::default()
        };
        // (outcome to publish, whether to put the committed state back)
        let (outcome, restore) = match &commit {
            Commit::Answered(a) if a.status() == A::Ok => {
                if let Some(h) = self.actors.get_mut(&actor) {
                    h.state_version = a.version;
                    h.durable_state = state;
                    h.state_chunks = a.record.as_ref().map(|r| r.state_chunks.clone()).unwrap_or_default();
                }
                (Some(reply), false)
            }
            Commit::Raised => (Some(reply), true),
            Commit::Answered(a) if a.status() == A::Duplicate => {
                let stored = a.reply.clone().unwrap_or_default();
                (Some(pb::Outcome { value: Some(pb::outcome::Value::Ok(stored)), ..Default::default() }), true)
            }
            Commit::Answered(a) if a.status() == A::Conflict => (
                Some(err(
                    Kind::CommandConflict,
                    "this command id was already used with different arguments; its first result stands".into(),
                )),
                true,
            ),
            Commit::Answered(a) if a.status() == A::Rejected => {
                (Some(err(Kind::User, format!("durable state refused: {}", a.reason))), true)
            }
            Commit::Answered(a) => {
                // Stale generation or version: another instance moved on. Run
                // the call again there; this instance restarts from the record.
                let why = format!("commit refused: {:?} {}", a.status(), a.reason);
                self.fail_attempt(&w, pb::record_failure::Kind::Infra, &why);
                (None, true)
            }
            Commit::Unknown(e) => {
                // It may have committed: the retry finds out (a repeat returns
                // the stored reply). This instance's state is suspect.
                self.fail_attempt(&w, pb::record_failure::Kind::Infra, &format!("commit not confirmed: {e}"));
                if let Some(exec) = self.actors.get(&actor).and_then(|h| h.exec) {
                    self.sup.kill(exec);
                }
                (None, false)
            }
        };
        if outcome.is_none() {
            self.retry_in_order(actor, &w);
        }
        if let Some(outcome) = outcome {
            self.pending_publish.entry(w.tk.v()).or_default().push(pb::PublishEntry {
                task: Some(w.tk.pb()),
                attempt: w.attempt,
                peer: Some(self.router.identity.pb()),
                outcome: Some(outcome),
                forward_to: None,
            });
        }
        if let Some(h) = self.actors.get_mut(&actor) {
            h.committing = false;
            if restore && let Some(exec) = h.exec {
                let msg = pb::ExecRestore {
                    actor: actor.to_vec(),
                    fn_digest: h.class_digest.clone(),
                    state: h.durable_state.clone(),
                    version: h.state_version,
                };
                self.sup.send(exec, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Restore(msg)) });
            }
        }
        self.actor_after_call(actor, &w, true);
    }

    /// Budgets: charge running calls under a budget (at most once a second
    /// each, measured when a call has its worker to itself) and send what is
    /// due to the budgeted calls' shards.
    fn charge_budgets(&mut self) {
        let mut live = std::collections::HashSet::new();
        for r in &self.running {
            let budgeted: Vec<&Work> =
                r.items.iter().filter(|w| crate::budgets::root_of(w.tk, &w.policy).is_some()).collect();
            if budgeted.is_empty() {
                continue;
            }
            let alone = r.items.len() == 1 && self.running.iter().filter(|x| x.exec == r.exec).count() == 1;
            let pid = self.execs.get(&r.exec).map_or(0, |e| e.pid);
            let cpu_ns = if alone { accounting::read(pid).map(|x| x.cpu_ns) } else { None };
            let started = (r.start.map(|x| x.cpu_ns), r.started);
            for w in budgeted {
                live.insert((r.exec, w.tk, w.attempt));
                let share = r.cpus / r.items.len() as f64;
                self.budgets.charge(r.exec, w.tk, w.attempt, &w.policy, started, cpu_ns, share, false);
            }
        }
        self.budgets.keep(&live);
        self.send_charges();
    }

    fn send_charges(&mut self) {
        for (root, charge) in self.budgets.take() {
            self.propose(root.v(), pb::shard_cmd::Body::Charge(charge));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_attempt(
        &mut self,
        w: &Work,
        done: &pb::ExecDone,
        exec: u32,
        start: Option<Reading>,
        end: Option<Reading>,
        dispatched_ns: u64,
        batch_len: usize,
        iso: Option<IsoUsage>,
        peak: Option<(u64, &'static str, &'static str)>,
    ) -> Option<f64> {
        let mut measures = HashMap::new();
        let mut cpu_measured = None;
        let wall_s = done.exit_unix_ns.saturating_sub(done.entry_unix_ns) as f64 / 1e9;
        if let Some((bytes, scope, method)) = peak {
            measures.insert(
                "memory_private_peak_bytes".into(),
                pb::Measure { value: bytes as f64, scope: scope.into(), method: method.into(), coverage: 1.0 },
            );
        }
        if let Some(u) = iso {
            // Charged to the attempt's own cgroup: scope "attempt".
            let m = |value: f64| pb::Measure { value, scope: "attempt".into(), method: "cgroup".into(), coverage: 1.0 };
            // Includes the dedicated interpreter's start: not learned from.
            let cpu_s = u.cpu_ns as f64 / 1e9;
            measures.insert("cpu_seconds".into(), m(cpu_s));
            if wall_s > 0.0 {
                measures.insert("mean_cores".into(), m(cpu_s / wall_s));
            }
            measures.insert("cpu_throttled_seconds".into(), m(u.throttled_ns as f64 / 1e9));
            if let Some(peak) = u.memory_peak {
                measures.insert("memory_peak_bytes".into(), m(peak as f64));
            }
            measures.insert("memory_current_bytes".into(), m(u.memory_current as f64));
        } else if let (Some(a), Some(b)) = (start, end) {
            let scope = if batch_len > 1 { "batch" } else { "process" };
            let cpu_s = b.cpu_ns.saturating_sub(a.cpu_ns) as f64 / 1e9;
            let span_s = now_ns().saturating_sub(dispatched_ns) as f64 / 1e9;
            // Cores over the whole attempt, decoding and reporting included:
            // a call of microseconds otherwise looks like many cores. The
            // runtime's helper threads are not the function's: a 256 MiB
            // argument read on four threads made a one-core function learn
            // four CPUs, and four such calls filled a 16-CPU machine.
            let helper_s = done.helper_cpu_ns as f64 / 1e9;
            cpu_measured = (span_s > 0.0).then(|| (cpu_s - helper_s).max(0.0) / span_s);
            measures.insert(
                "cpu_seconds".into(),
                pb::Measure { value: cpu_s, scope: scope.into(), method: "rusage".into(), coverage: 1.0 },
            );
            if span_s > 0.0 {
                measures.insert(
                    "mean_cores".into(),
                    pb::Measure { value: cpu_s / span_s, scope: scope.into(), method: "rusage".into(), coverage: 1.0 },
                );
            }
            measures.insert(
                "rss_peak_bytes".into(),
                pb::Measure {
                    value: b.peak_rss_bytes as f64,
                    scope: "process_lifetime".into(),
                    method: "rusage".into(),
                    coverage: 1.0,
                },
            );
            measures.insert(
                "rss_end_bytes".into(),
                pb::Measure {
                    value: b.rss_bytes as f64,
                    scope: "process".into(),
                    method: "rusage".into(),
                    coverage: 1.0,
                },
            );
        }
        measures.insert(
            "wall_seconds".into(),
            pb::Measure { value: wall_s, scope: "attempt".into(), method: "clock".into(), coverage: 1.0 },
        );
        let end_reason = match done.outcome.as_ref().and_then(|o| o.value.as_ref()) {
            Some(pb::outcome::Value::Ok(_)) => "ok",
            Some(pb::outcome::Value::Err(_)) if done.retryable => "user_error_retryable",
            Some(pb::outcome::Value::Err(_)) => "user_error",
            None if done.forward_to.is_some() => "forwarded",
            None => "unknown",
        };
        let rec = pb::AttemptRecord {
            task: Some(w.tk.pb()),
            attempt: w.attempt,
            peer: Some(self.router.identity.pb()),
            executor: exec,
            fn_name: w.spec.method.clone(),
            submitted_unix_ns: 0,
            ready_unix_ns: w.ready_ns,
            queued_unix_ns: w.queued_ns,
            dispatched_unix_ns: dispatched_ns,
            py_entry_unix_ns: done.entry_unix_ns,
            py_exit_unix_ns: done.exit_unix_ns,
            published_unix_ns: 0,
            granted: Some(w.policy.clone()),
            measures,
            end_reason: end_reason.into(),
            detail: String::new(),
            placement_reason: "local peer had a warm executor and free CPU".into(),
            reservation: w.reservation(),
            settings: w.settings(self.capacity),
        };
        self.push_attempt(w.tk, rec);
        cpu_measured
    }

    fn push_attempt(&mut self, tk: Tk, rec: pb::AttemptRecord) {
        self.attempt_bytes += rec.encoded_len();
        self.attempts.entry(tk).or_default().push(rec);
        self.attempt_order.push_back(tk);
        while self.attempt_order.len() > ATTEMPT_LOG_CAP || self.attempt_bytes > ATTEMPT_LOG_BYTES {
            let Some(old) = self.attempt_order.pop_front() else { break };
            if let Some(gone) = self.attempts.remove(&old) {
                self.attempt_bytes -= gone.iter().map(|r| r.encoded_len()).sum::<usize>();
            }
        }
    }

    /// Learn from a finished attempt: memory, CPU and batching per function
    /// (profiles). Attempts whose CPU could not be measured alone are skipped,
    /// as are failures (they stop early).
    fn learn(&mut self, w: &Work, done: &pb::ExecDone, cores: Option<f64>, peak: u64) {
        let out_bytes = match done.outcome.as_ref().and_then(|o| o.value.as_ref()) {
            // The files the frame refers to are output too (the frame alone may
            // be a few bytes for hundreds of MiB of arrays).
            Some(pb::outcome::Value::Ok(obj)) => obj.size + obj.blobs.iter().map(|b| b.size).sum::<u64>(),
            _ => return,
        };
        if peak > 0 && !w.in_actor() {
            // One finished call ends the probe: later calls are admitted by
            // memory (need()), as wide as the pool allows.
            let floor = self.live_floor.entry(w.key()).or_default();
            *floor = (*floor).max(peak);
        }
        let Some(cores) = cores else { return };
        if !w.publish {
            return;
        }
        // A map chunk ran `items` calls one after another: time, CPU and
        // output are per item; its peak and inputs are the chunk's.
        let items = w.policy.items.max(1) as f64;
        let wall_s = done.exit_unix_ns.saturating_sub(done.entry_unix_ns) as f64 / 1e9 / items;
        let sample = Sample {
            wall_s,
            cpu_s: cores * wall_s,
            peak,
            arg_bytes: w.read_bytes,
            out_bytes: (out_bytes as f64 / items) as u64,
            cpus: w.cpus,
            oom: false,
        };
        let req = Request::of(&w.policy);
        let kills = self.mem_kills.back().is_some_and(|t| t.elapsed() < KILLS_RECENT);
        let now = self.epoch.elapsed().as_secs_f64();
        for d in self.profiles.record(&w.key(), &w.policy.fn_name, sample, w.arm, req, kills, now, now_ns()) {
            self.decide(d);
        }
    }

    /// Replace a pooled executor whose memory between calls keeps rising (R4).
    fn check_leak(&mut self, exec: u32, private: u64, held: u64) {
        let Some(e) = self.execs.get_mut(&exec) else { return };
        e.private = private;
        if e.actor.is_some() {
            return;
        }
        let Some(why) = e.leak.after_call(held) else { return };
        let pid = e.pid;
        self.retire_exec(exec);
        self.decide(pb::Decision {
            at_unix_ns: now_ns(),
            controller: "leaks".into(),
            knob: "executor".into(),
            scope: format!("executor {exec} (pid {pid})"),
            old_value: "running".into(),
            new_value: "replaced".into(),
            outcome: "adopted".into(),
            evidence: why,
            reason: "its memory between calls keeps growing; a fresh process starts clean (more memory would only \
                     hide the leak)"
                .into(),
        });
    }

    fn save_profiles(&mut self) {
        self.profiles_saved = Instant::now();
        if self.profiles.take_dirty().is_empty() {
            return;
        }
        if let Err(e) = self.profiles.save(&self.profiles_path) {
            tracing::warn!("cannot save profiles to {}: {e}", self.profiles_path.display());
        }
    }

    /// Append an event (bounded) and answer long polls waiting for it.
    fn record(&mut self, mut e: pb::ClusterEvent) {
        self.event_seq += 1;
        e.seq = self.event_seq;
        e.at_unix_ms = now_ns() / 1_000_000;
        self.events.push_back(e);
        while self.events.len() > EVENT_LOG_CAP {
            self.events.pop_front();
        }
        let waiters = std::mem::take(&mut self.event_waiters);
        for (kinds, since, reply) in waiters {
            if reply.is_closed() {
                continue;
            }
            let found = self.events_since(&kinds, since);
            if found.is_empty() {
                self.event_waiters.push((kinds, since, reply));
            } else {
                let _ = reply.send(found);
            }
        }
    }

    fn events_since(&self, kinds: &[String], since: u64) -> Vec<pb::ClusterEvent> {
        self.events
            .iter()
            .filter(|e| e.seq >= since && (kinds.is_empty() || kinds.contains(&e.kind)))
            .take(1000)
            .cloned()
            .collect()
    }

    fn decide(&mut self, d: pb::Decision) {
        self.record(pb::ClusterEvent {
            kind: "decision".into(),
            message: format!("{} {}: {} -> {} ({})", d.controller, d.knob, d.old_value, d.new_value, d.outcome),
            detail_json: format!(
                "{{\"scope\": {:?}, \"reason\": {:?}, \"evidence\": {:?}}}",
                d.scope, d.reason, d.evidence
            ),
            ..Default::default()
        });
        self.decisions.push_back(d);
        while self.decisions.len() > 10_000 {
            self.decisions.pop_front();
        }
    }

    fn on_exec_exit(&mut self, exec: u32, detail: String, signal: Option<i32>) {
        self.start_failures.exited(exec, signal.is_some() || detail == "exit status: 0", &detail);
        let info = self.execs.remove(&exec);
        self.actor_waits.retain(|(e, _), _| *e != exec);
        self.sup.forget(exec);
        self.devices.release(exec);
        if let Some(e) = &info {
            self.cpu_used -= e.held_cpus;
        }
        self.iso_waiting.remove(&exec);
        let mut oom = info.as_ref().and_then(|e| e.killed_for_memory);
        let note = info.as_ref().and_then(|e| e.kill_note.clone());
        let culprit = info.as_ref().and_then(|e| e.killed_for);
        if let Some(group) = info.as_ref().and_then(|e| e.cgroup.clone()) {
            // The kernel enforces memory.max: an OOM kill shows in memory.events.
            if let Some(u) = Isolation::read(&group)
                && u.oom_kills > 0
            {
                let limit = std::fs::read_to_string(group.join("memory.max"))
                    .ok()
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                oom = Some((u.memory_peak.unwrap_or(u.memory_current), limit));
            }
            Isolation::remove(&group);
        }
        let lost: Vec<Running> = {
            let (lost, keep): (Vec<_>, Vec<_>) =
                std::mem::take(&mut self.running).into_iter().partition(|r| r.exec == exec);
            self.running = keep;
            lost
        };
        for r in &lost {
            self.unbatch(&r.items);
        }
        if !lost.is_empty() || info.as_ref().is_some_and(|e| e.actor.is_some()) {
            crate::counters::WORKER_DEATHS.add(1);
        }
        if oom.is_some() {
            crate::counters::OOM_KILLS.add(1);
        }
        for r in lost {
            // A process killed for the memory of one of the calls it held: the
            // others lost their worker (infra, retried with no memory hint).
            let bystander = oom.is_some() && culprit.is_some_and(|c| !r.items.iter().any(|w| w.tk == c));
            let oom = if bystander { None } else { oom };
            for (w, _) in r.items.iter().zip(&r.finished).filter(|(_, f)| !**f) {
                self.item_ended(w.tk);
            }
            if !r.blocked {
                self.cpu_used -= r.cpus;
            }
            if let Some(w) = r.items.first() {
                self.quotas.give((w.tk.run, w.tk.task));
            }
            let actor_exec = info.as_ref().and_then(|e| e.actor).is_some();
            for w in r.items.iter().filter(|w| !w.publish) {
                self.give_up(w, "its worker died");
            }
            for w in r.items.iter().filter(|w| w.publish) {
                let reason = match (oom, signal) {
                    _ if bystander => "its worker was killed for the memory of another call running beside it".into(),
                    (Some(_), _) if note.is_some() => note.clone().unwrap_or_default(),
                    (Some((peak, limit)), _) => {
                        format!("killed for exceeding its memory limit: peak {peak} > limit {limit} bytes")
                    }
                    (None, Some(sig)) => format!("executor process died with signal {sig} ({detail})"),
                    (None, None) => format!("executor process exited ({detail})"),
                };
                // A durable actor's call left nothing committed, and its command
                // id makes a replay safe: it is retried on the restarted instance.
                let durable = w.kind() == pb::CallKind::CallActorMethod
                    && Id::from_slice(&w.spec.actor).ok().and_then(|a| self.actors.get(&a)).is_some_and(|h| h.durable);
                if durable && let Some(h) = Id::from_slice(&w.spec.actor).ok().and_then(|a| self.actors.get_mut(&a)) {
                    // Its retry keeps its sequence number: let the handle's
                    // order wait for it again rather than skip past it.
                    h.inflight.remove(&(w.spec.handle, w.spec.seq));
                    let next = h.next_seq.entry(w.spec.handle).or_insert(0);
                    *next = (*next).min(w.spec.seq);
                }
                if actor_exec && w.kind() == pb::CallKind::CallActorMethod && !durable {
                    let actor = Id::from_slice(&w.spec.actor).ok();
                    let pending = actor
                        .and_then(|a| self.actors.get(&a))
                        .is_some_and(|h| h.inflight.contains(&(w.spec.handle, w.spec.seq)));
                    if !pending {
                        continue; // it finished before the process died
                    }
                    // In a batch only the earliest unfinished call had started;
                    // the ones after it go to the next instance, in order.
                    let first_unfinished = r
                        .items
                        .iter()
                        .find(|x| {
                            actor
                                .and_then(|a| self.actors.get(&a))
                                .is_some_and(|h| h.inflight.contains(&(x.spec.handle, x.spec.seq)))
                        })
                        .map(|x| (x.spec.handle, x.spec.seq));
                    if first_unfinished != Some((w.spec.handle, w.spec.seq))
                        && let Some(h) = actor.and_then(|a| self.actors.get_mut(&a))
                    {
                        h.inflight.remove(&(w.spec.handle, w.spec.seq));
                        let next = h.next_seq.entry(w.spec.handle).or_insert(0);
                        *next = (*next).min(w.spec.seq);
                        actor_limits::enqueue(h, w.clone());
                        continue;
                    }
                    // A started actor call is not replayed on a fresh instance.
                    self.propose(
                        w.tk.v(),
                        pb::shard_cmd::Body::Cancel(pb::Cancel {
                            task: Some(w.tk.pb()),
                            reason: Kind::ActorDied as i32,
                            message: format!("the actor process died while this call was running: {reason}"),
                        }),
                    );
                    continue;
                }
                let (kind, peak) = match oom {
                    Some((peak, _)) => (pb::record_failure::Kind::Oom, peak),
                    None => (pb::record_failure::Kind::Infra, 0),
                };
                if oom.is_some() {
                    // Censored: it needed at least `peak`; later calls of this
                    // size or larger reserve 1.5× that.
                    let sample =
                        Sample { peak, arg_bytes: w.read_bytes, cpus: w.cpus, oom: true, ..Default::default() };
                    let req = Request::of(&w.policy);
                    let now = self.epoch.elapsed().as_secs_f64();
                    for d in self.profiles.record(&w.key(), &w.policy.fn_name, sample, w.arm, req, true, now, now_ns())
                    {
                        self.decide(d);
                    }
                }
                self.push_attempt(
                    w.tk,
                    pb::AttemptRecord {
                        task: Some(w.tk.pb()),
                        attempt: w.attempt,
                        peer: Some(self.router.identity.pb()),
                        executor: exec,
                        dispatched_unix_ns: r.dispatched_ns,
                        end_reason: if oom.is_some() { "oom".into() } else { "killed".into() },
                        detail: reason.clone(),
                        granted: Some(w.policy.clone()),
                        settings: w.settings(self.capacity),
                        ..Default::default()
                    },
                );
                self.propose(
                    w.tk.v(),
                    pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
                        task: Some(w.tk.pb()),
                        attempt: w.attempt,
                        peer: Some(self.router.identity.pb()),
                        kind: kind as i32,
                        error: Some(pb::ErrorRecord {
                            kind: if oom.is_some() { Kind::Oom as i32 } else { Kind::Infra as i32 },
                            message: reason,
                            attempt: w.attempt,
                            ..Default::default()
                        }),
                        fingerprint: Vec::new(),
                        memory_peak: peak,
                    }),
                );
            }
        }
        if let Some(actor) = info.and_then(|e| e.actor) {
            self.actor_exec_lost(actor, exec, &detail);
        }
    }

    // ---- actors -----------------------------------------------------------

    /// As leader of the actor's group: place a pending actor, and keep its
    /// host (this peer or another) informed of its record.
    fn on_actor_changed(&mut self, actor: Id) {
        let Some(Some(rec)) = self.router.read(actor.vshard(), |kv| actor_state::whole_record(kv, actor)) else {
            return;
        };
        // A placed actor's CPUs stop being held for it once it is gone, or once
        // it runs here (this peer's own count then includes them).
        let here = rec.host.as_ref().is_some_and(|h| h.peer == self.router.identity.peer.to_vec());
        if rec.state() == pb::actor_record::State::Dead || (rec.state() == pb::actor_record::State::Alive && here) {
            self.actor_holds.retain(|(a, ..)| *a != actor);
        }
        if self.actor_seen.get(&actor) != Some(&(rec.generation, rec.state)) {
            self.actor_seen.insert(actor, (rec.generation, rec.state));
            let message = match rec.state() {
                pb::actor_record::State::Pending => "created; waiting for a host".to_string(),
                pb::actor_record::State::Alive => format!("alive (generation {})", rec.generation),
                pb::actor_record::State::Restarting => format!("restarting (restart {})", rec.restarts),
                pb::actor_record::State::Dead => {
                    format!("dead: {}", rec.death.as_ref().map_or("", |d| d.message.as_str()))
                }
            };
            let run = rec.spec.as_ref().and_then(|s| s.ctor.as_ref()).map(|c| c.run.clone()).unwrap_or_default();
            self.record(pb::ClusterEvent {
                kind: "actor".into(),
                actor: actor.to_vec(),
                run,
                node: rec.host.as_ref().map(|h| h.peer.clone()).unwrap_or_default(),
                message,
                ..Default::default()
            });
        }
        let spec = rec.spec.clone().unwrap_or_default();
        if matches!(rec.state(), pb::actor_record::State::Pending | pb::actor_record::State::Restarting) {
            let host = self.actors.entry(actor).or_default();
            if host.placing != Some(rec.generation) {
                host.placing = Some(rec.generation);
                let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
                let placement = self.choose_actor_host(actor, &policy);
                self.note_actor_waiting(actor, matches!(placement, ActorPlacement::Wait).then_some(policy.cpus));
                match placement {
                    ActorPlacement::Host(target) => self.propose(
                        actor.vshard(),
                        pb::shard_cmd::Body::ActorPlace(pb::ActorPlace {
                            actor: actor.to_vec(),
                            expected_generation: rec.generation,
                            host: Some(target),
                        }),
                    ),
                    // Every peer that could hold it has its CPUs in use: it stays
                    // pending and is placed once some free up (as Ray's actors wait).
                    ActorPlacement::Wait => {
                        if let Some(h) = self.actors.get_mut(&actor) {
                            h.placing = None;
                        }
                        self.actors_waiting.insert(actor);
                    }
                    ActorPlacement::Infeasible => self.propose(
                        actor.vshard(),
                        pb::shard_cmd::Body::ActorDied(pb::ActorDied {
                            actor: actor.to_vec(),
                            generation: rec.generation,
                            reason: Some(pb::ErrorRecord {
                                kind: Kind::Infeasible as i32,
                                message: format!("infeasible: no live peer offers {} CPUs for this actor", policy.cpus),
                                ..Default::default()
                            }),
                            terminal: true,
                            moved: false,
                        }),
                    ),
                }
            }
            return;
        }
        let me = self.router.identity.pb();
        let host = rec.host.clone().or_else(|| self.actors_last_host(actor));
        let dead = rec.state() == pb::actor_record::State::Dead;
        // Ended before its constructor ran (closed while it waited for CPUs, or
        // infeasible): end the constructor too, or the calls waiting on it would
        // wait forever. Its group's leader asks; a finished constructor keeps
        // its outcome.
        if dead
            && self.router.leads(self.router.group_of(actor.vshard()))
            && let Some(ctor) = spec.ctor.as_ref().and_then(Tk::parse)
            && self.router.read(ctor.v(), |kv| read::task(kv, ctor)).flatten().is_none_or(|t| t.outcome.is_none())
        {
            let why = rec.death.as_ref().map_or("", |d| d.message.as_str());
            self.propose(
                ctor.v(),
                pb::shard_cmd::Body::Cancel(pb::Cancel {
                    task: Some(ctor.pb()),
                    reason: Kind::ActorDied as i32,
                    message: format!("the actor ended before it started: {why}"),
                }),
            );
        }
        let remote = host.filter(|h| *h != me && self.router.is_cluster());
        // A death also applies here: calls routed to this peer must fail.
        if remote.is_none() || dead {
            self.on_actor_record(rec.clone(), None);
        }
        let Some(host) = remote else { return };
        // Hosted elsewhere: push the record, and after a restart the committed
        // constructor to replay (its outcome is already published).
        let Some(mesh) = self.router.mesh.clone() else { return };
        let Some(target) = mesh.member_by_peer(&host.peer) else { return };
        let run = spec.ctor.as_ref().and_then(Tk::parse).map(|t| t.run);
        let env = run.and_then(|r| self.env_of(&r)).map(|e| e.to_spec());
        let mut work = Vec::new();
        if rec.state() == pb::actor_record::State::Alive
            && rec.generation > 1
            && let Some(ctor) = spec.ctor.as_ref().and_then(Tk::parse)
            && let Some(Some(crec)) = self.router.read(ctor.v(), |kv| read::task(kv, ctor))
            && crec.outcome.as_ref().is_some_and(|o| matches!(o.value, Some(pb::outcome::Value::Ok(_))))
        {
            let cspec = crec.spec.unwrap_or_default();
            work.push(pb::RemoteWork {
                task: Some(ctor.pb()),
                attempt: 0,
                deps: self.dep_outcomes(ctor, &cspec),
                spec: Some(cspec),
                env: env.clone(),
                detached: spec.detached,
                replay: true,
                ..Default::default()
            });
        }
        let req = pb::DispatchRequest { cluster: mesh.cluster.clone(), work, actors: vec![rec], actor_env: env };
        let node = target.member.node_id;
        tokio::spawn(async move {
            if let Err(e) = mesh.dispatch(node, req).await {
                tracing::warn!("cannot reach actor host {node}: {e}");
            }
        });
    }

    fn actors_last_host(&self, actor: Id) -> Option<pb::PeerInc> {
        self.actor_hosts.get(&actor).cloned()
    }

    /// Where a new actor instance goes: this peer or the least loaded of two
    /// sampled live peers with room for its CPU.
    fn choose_actor_host(&mut self, actor: Id, policy: &pb::Policy) -> ActorPlacement {
        // A bundle's or rank's host takes CPUs its peer holds for it (Placement names that peer).
        let cpus = if policy.launch.is_some() { 0.0 } else { policy.cpus.max(0.0) };
        let me = self.router.identity.pb();
        self.actor_holds.retain(|(.., at)| at.elapsed() < ACTOR_HOLD_TTL);
        let holds = &self.actor_holds;
        let held =
            |peer: &[u8]| holds.iter().filter(|(_, p, ..)| p.as_slice() == peer).map(|(_, _, c, _)| c).sum::<f64>();
        let free_here = self.capacity - self.cpu_used - held(&me.peer);
        let Some(mesh) = self.router.mesh.clone() else {
            return if self.capacity + 1e-9 < cpus {
                ActorPlacement::Infeasible
            } else if free_here + 1e-9 >= cpus {
                self.hold_actor_cpus(actor, &me, cpus);
                ActorPlacement::Host(me)
            } else {
                ActorPlacement::Wait
            };
        };
        // Placement(require=, prefer=), as for tasks (choose_peer).
        let allowed = |m: &pb::Member| crate::labels::satisfies(&m.labels, &policy.require);
        let bonus = |m: &pb::Member| 0.5 * crate::labels::preferred(&m.labels, &policy.prefer) as f64;
        while self.actor_recent.front().is_some_and(|(_, at)| at.elapsed() > ACTOR_RECENT_TTL) {
            self.actor_recent.pop_front();
        }
        let recent = |peer: &[u8]| self.actor_recent.iter().filter(|(p, _)| p.as_slice() == peer).count() as f64;
        // Actors placed here lately count too: their executors may not exist yet.
        let hosted = self.hosted_actors() as f64 + recent(&me.peer);
        let mine = (self.cpu_used + self.queue.len() as f64 + 0.25 * hosted) / self.capacity.max(1e-9);
        let fits_here = !self.draining && self.capacity + 1e-9 >= cpus && allowed(&mesh.me);
        let mut best = (fits_here && free_here + 1e-9 >= cpus).then(|| (mine - bonus(&mesh.me), me.clone()));
        let able: Vec<_> =
            mesh.live_others().into_iter().filter(|p| p.member.cpus + 1e-9 >= cpus && allowed(&p.member)).collect();
        // A draining peer could hold it but takes no new actor: it waits.
        let ever =
            fits_here || !able.is_empty() || (self.draining && self.capacity + 1e-9 >= cpus && allowed(&mesh.me));
        // `stopping`, not `draining`: a busy peer whose core answered a ping
        // late also reports `draining` (mesh.rs ping handler).
        let others: Vec<_> = able.into_iter().filter(|p| !p.load.as_ref().is_some_and(|l| l.stopping)).collect();
        // Free CPUs as each peer last reported them, less actors placed there since.
        let free: Vec<_> = others
            .iter()
            .filter(|p| {
                let used = p.load.as_ref().map_or(0.0, |l| l.cpu_used);
                p.member.cpus - used - held(&p.member.peer) + 1e-9 >= cpus
            })
            .collect();
        let seed = now_ns() as usize;
        for i in 0..2.min(free.len()) {
            let p = free[(seed + i * 7919) % free.len()];
            let actors = p.load.as_ref().map_or(0, |l| l.actors) as f64 + recent(&p.member.peer);
            let score = p.pressure() + 0.25 * actors / p.member.cpus.max(1e-9) - bonus(&p.member);
            if best.as_ref().is_none_or(|(b, _)| score < *b) {
                best = Some((score, pb::PeerInc { peer: p.member.peer.clone(), inc: p.member.incarnation }));
            }
        }
        match best {
            Some((_, p)) => {
                // CPU pressure until the peer's next report. An actor with no
                // CPUs adds none: it counts through `recent` alone, or a peer
                // whose report just reset this would soak up a burst.
                if cpus > 0.0
                    && let Some(v) = mesh.member_by_peer(&p.peer)
                {
                    mesh.note_placed(v.member.node_id);
                }
                self.hold_actor_cpus(actor, &p, cpus);
                self.actor_recent.push_back((p.peer.clone(), Instant::now()));
                ActorPlacement::Host(p)
            }
            None if ever => ActorPlacement::Wait,
            None => ActorPlacement::Infeasible,
        }
    }

    fn hold_actor_cpus(&mut self, actor: Id, peer: &pb::PeerInc, cpus: f64) {
        if cpus > 0.0 {
            self.actor_holds.push((actor, peer.peer.clone(), cpus, Instant::now()));
        }
    }

    /// Start the process of an actor placed here, unless live actors and
    /// running calls already fill the memory pool: an actor keeps what its
    /// constructor allocates, so hundreds of tiny-CPU actors would otherwise
    /// exhaust the machine. It then waits (retried from housekeeping) with a
    /// reason naming what holds the pool.
    fn start_actor(&mut self, actor: Id, run: Option<Id>, spec: &pb::ActorSpec) {
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        // Devices other executors hold are never shared: the actor waits for
        // them as for memory (a bundle's or rank's are held for it).
        let devices = policy.launch.is_none().then(|| self.devices.wait_reason(policy.gpus, &policy.device)).flatten();
        if let Some(why) = devices.or_else(|| self.actor_memory_wait(actor)) {
            if let Some(h) = self.actors.get_mut(&actor) {
                h.waiting_memory = Some(why);
            }
            if !self.actors_waiting_memory.iter().any(|(a, ..)| *a == actor) {
                self.actors_waiting_memory.push_back((actor, run, spec.clone()));
            }
            return;
        }
        let env = run.and_then(|r| self.env_of(&r));
        if let (Some(r), None) = (run, &env) {
            // This peer holds no record of the actor's run (it is not in the
            // run's group, and the leader that placed it held none either):
            // wait, saying why, and read it from the run's group.
            if let Some(h) = self.actors.get_mut(&actor) {
                h.waiting_memory = Some(ENV_WAIT.into());
            }
            if !self.actors_waiting_env.iter().any(|(a, ..)| *a == actor) {
                self.actors_waiting_env.push_back((actor, run, spec.clone()));
            }
            self.fetch_run(r);
            return;
        }
        if let Some(h) = self.actors.get_mut(&actor) {
            h.waiting_memory = None;
        }
        let threads = if policy.cpus > 0.0 { policy.cpus.ceil() as u32 } else { 1 };
        if let Some(env) = env {
            // An actor holds its devices for its life (as the CPU it asked for).
            let key = match (&policy.launch, run) {
                (Some(t), Some(run)) => match self.take_slot(run, t) {
                    Ok(held) => self.devices.key_of(&env.key(threads), &held),
                    Err(why) => {
                        self.close_actor(actor, &why);
                        return;
                    }
                },
                _ => match self.devices.key_for(policy.gpus, &policy.device, &env.key(threads)) {
                    Want::Key(key) => key,
                    Want::None | Want::Wait | Want::Infeasible(_) => env.key(threads),
                },
            };
            let exec = self.spawn_exec(&env, threads, &key, Some(actor));
            if let Some(id) = exec {
                self.devices.hold(id, &key);
            }
            // An actor holds only the CPU it asked for: idle actors
            // must not starve tasks (Ray's default is the same).
            let cpus = policy.cpus.max(0.0);
            if let Some(e) = exec.and_then(|id| self.execs.get_mut(&id)) {
                e.held_cpus = cpus;
                e.concurrency = (policy.concurrency as usize).max(1);
                self.cpu_used += cpus;
            }
            if let Some(h) = self.actors.get_mut(&actor) {
                h.exec = exec;
                h.started = Some(Instant::now());
            }
        }
    }

    /// Why an actor's process cannot start yet for lack of memory, or None.
    /// One actor of a class is taken to hold what the most a started one of
    /// that class here holds (once its constructor finished or it has run
    /// PROBE_FOR), or its constructor's learned memory; until either is known,
    /// a few start at a time. Siblings started but not yet measured are
    /// charged that much, so a burst of creations cannot overshoot the pool
    /// between memory samples.
    fn actor_memory_wait(&self, actor: Id) -> Option<String> {
        let host = self.actors.get(&actor)?;
        if host.class_digest.is_empty() {
            return None;
        }
        let mut one = host.ctor.as_ref().map_or(0, |w| w.mem);
        let mut probing: Vec<u64> = Vec::new(); // what each unmeasured sibling holds so far
        for (a, h) in &self.actors {
            if *a == actor || h.dead || h.class_digest != host.class_digest {
                continue;
            }
            let Some(e) = h.exec.and_then(|x| self.execs.get(&x)) else { continue };
            if h.constructed || h.started.is_some_and(|t| t.elapsed() >= PROBE_FOR) {
                one = one.max(e.private + e.arena);
            } else {
                probing.push(e.private + e.arena);
            }
        }
        if one == 0 {
            let limit = self.probe_width();
            return (probing.len() >= limit).then(|| {
                format!(
                    "an actor class not yet measured starts {limit} actor(s) at a time until the memory one holds is known"
                )
            });
        }
        let held: Vec<u64> = self.execs.values().filter(|e| e.actor.is_some()).map(|e| e.private + e.arena).collect();
        if held.is_empty() && !self.running.iter().any(|r| !r.blocked) {
            return None; // nothing here would free memory by waiting: start it alone
        }
        let used = self.memory_used() + probing.iter().map(|p| one.saturating_sub(*p)).sum::<u64>();
        (used + one > self.budget.pool).then(|| {
            format!(
                "the actor needs {} MiB of memory to start (what one of its class holds here); {} of {} MiB in use, \
                 {} MiB of it held by {} live actor(s); closing ones you are done with (handle.close()) frees it",
                one / MIB,
                used / MIB,
                self.budget.pool / MIB,
                held.iter().sum::<u64>() / MIB,
                held.len()
            )
        })
    }

    /// Actors this peer hosts: running, and placed here but waiting (for
    /// memory, or their run's record) to start. Placement must count those
    /// too, or a peer whose actors wait looks empty and gets more.
    fn hosted_actors(&self) -> usize {
        self.execs.values().filter(|e| e.actor.is_some()).count()
            + self.actors_waiting_memory.len()
            + self.actors_waiting_env.len()
    }

    /// Actors waiting for memory to start, oldest first, as room frees.
    fn start_waiting_actors(&mut self) {
        while let Some((actor, run, spec)) = self.actors_waiting_memory.front().cloned() {
            if self.actors.get(&actor).is_some_and(|h| !h.dead && h.exec.is_none() && h.waiting_memory.is_some()) {
                self.start_actor(actor, run, &spec);
                let for_env = self.actors_waiting_env.iter().any(|(a, ..)| *a == actor);
                if !for_env && self.actors.get(&actor).is_some_and(|h| h.waiting_memory.is_some()) {
                    break; // still no room: later ones wait their turn
                }
            }
            self.actors_waiting_memory.pop_front();
        }
    }

    /// Actors waiting for their run's record: each starts once the record is
    /// here (or moves on to wait for memory); the others stay, in order, and
    /// their runs are read again.
    fn start_env_waiters(&mut self) {
        for (actor, run, spec) in std::mem::take(&mut self.actors_waiting_env) {
            if self.actors.get(&actor).is_some_and(|h| !h.dead && h.exec.is_none()) {
                self.start_actor(actor, run, &spec);
            }
        }
    }

    /// Read `run`'s record from its group for the actors here waiting on it:
    /// one read at a time per run, and no more often than RUN_FETCH_EVERY.
    fn fetch_run(&mut self, run: Id) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        let now = Instant::now();
        if !run_fetch_due(self.run_fetches.get(&run), now) {
            return;
        }
        self.run_fetches.insert(run, (true, now));
        let (group, tx) = (self.router.group_of(run.vshard()), self.me_tx.clone());
        tokio::spawn(async move {
            let req = pb::ReadRequest { run: run.to_vec(), ..Default::default() };
            let rec = mesh.read_group(group, req).await.ok().and_then(|r| r.run).map(Box::new);
            let _ = tx.send(CoreMsg::RunFetched { run, rec }).await;
        });
    }

    fn on_run_fetched(&mut self, run: Id, rec: Option<Box<pb::RunRecord>>) {
        if let Some(f) = self.run_fetches.get_mut(&run) {
            f.0 = false;
        }
        let Some(rec) = rec else { return }; // housekeeping asks again
        let Some(spec) = rec.exec_env.clone() else { return };
        self.quotas.tenant(run, || Some(rec.tenant.clone()));
        self.runs.entry(run).or_insert(RunInfo {
            env: ExecEnv::from_spec(spec),
            detached: rec.detached,
            sessions: BTreeSet::new(),
        });
        self.run_fetches.remove(&run);
        self.start_env_waiters();
    }

    /// The current record of an actor this peer hosts (or is about to).
    fn on_actor_record(&mut self, rec: pb::ActorRecord, env: Option<pb::ExecEnvSpec>) {
        let spec = rec.spec.clone().unwrap_or_default();
        let Ok(actor) = Id::from_slice(&spec.actor) else { return };
        let run = spec.ctor.as_ref().and_then(Tk::parse).map(|t| t.run);
        if let (Some(run), Some(env)) = (run, env) {
            self.runs.entry(run).or_insert(RunInfo {
                env: ExecEnv::from_spec(env),
                detached: spec.detached,
                sessions: BTreeSet::new(),
            });
        }
        if let Some(h) = rec.host.as_ref() {
            self.actor_hosts.insert(actor, h.clone());
        }
        let host = self.actors.entry(actor).or_default();
        host.restart = spec.restart();
        host.detached = spec.detached;
        host.durable = spec.durable;
        // A record read before an upgrade (from a lagging replica) never undoes it.
        if spec.state_schema >= host.state_schema {
            host.upgrade |= host.durable && !host.class_digest.is_empty() && host.class_digest != spec.class_digest;
            host.class_digest = spec.class_digest.clone();
            host.state_schema = spec.state_schema;
            host.limits = spec.methods.clone();
        }
        // A record read without its chunks put back together (whole_record)
        // carries none of the state they hold, and starts no instance: the
        // leader's copy, which has them, does.
        let whole = rec.state_chunks.is_empty() || !rec.durable_state.is_empty();
        if rec.state_version >= host.state_version && whole {
            host.state_version = rec.state_version;
            host.durable_state = rec.durable_state.clone();
            host.state_chunks = rec.state_chunks.clone();
        }
        host.run = run;
        match rec.state() {
            pb::actor_record::State::Pending | pb::actor_record::State::Restarting => {}
            pb::actor_record::State::Alive => {
                let mine = rec.host.as_ref().is_some_and(|h| {
                    h.peer == self.router.identity.peer.to_vec() && h.inc == self.router.identity.incarnation
                });
                if mine && rec.generation > host.generation && whole {
                    if rec.generation != host.generation + 1 {
                        // It ran elsewhere since it was last here: where its
                        // handles stand is learned again (resolve_handles).
                        host.next_seq.clear();
                        host.done_seqs.clear();
                        host.known_handles.clear();
                        host.resolving.clear();
                    }
                    host.generation = rec.generation;
                    host.constructed = false;
                    host.dead = false;
                    host.moving = false;
                    host.moved = false;
                    if self.draining && (host.restart != pb::actor_spec::Restart::Never || host.durable) {
                        // Placed here by a leader that had not yet heard this
                        // peer drains: send it on rather than start it.
                        host.moving = true;
                        self.try_move_actor(actor);
                    } else {
                        self.start_actor(actor, run, &spec);
                    }
                }
            }
            pb::actor_record::State::Dead => {
                host.dead = true;
                host.waiting_memory = None;
                // Its process ends now; the exit releases the CPU it held. An
                // idle one runs its own cleanup first (Ray's kill skips
                // `__del__` and `atexit`). A busy one stops outright, as a call
                // still running would otherwise finish in the grace period and
                // succeed although the actor was closed.
                if let Some(exec) = host.exec.take() {
                    if self.running.iter().any(|r| r.exec == exec) {
                        self.sup.kill(exec);
                    } else {
                        self.sup.close(exec, ACTOR_CLOSE_GRACE);
                    }
                }
                let host = self.actors.get_mut(&actor).expect("present");
                let calls: Vec<Tk> = host.mailbox.values().map(|w| w.tk).collect();
                host.mailbox.clear();
                let why = rec.death.as_ref().map_or("the actor is dead".to_string(), |d| d.message.clone());
                for tk in calls {
                    self.cancel_actor_call(tk, &why);
                }
            }
        }
    }

    /// End an actor wherever it runs. The death is committed against the
    /// record's current generation, read from this peer's replica of its
    /// group or from another peer's; the leader then tells the host, which
    /// stops the process and fails its queued calls.
    fn close_actor(&mut self, actor: Id, reason: &str) {
        if self.actors.get(&actor).is_some_and(|h| h.dead) {
            return;
        }
        let router = self.router.clone();
        let message = format!("actor closed: {reason}");
        tokio::spawn(async move {
            for _ in 0..5 {
                let local = router.read(actor.vshard(), |kv| read::actor(kv, actor)).flatten();
                let rec = match (local, &router.mesh) {
                    (Some(r), _) => Some(r),
                    (None, Some(mesh)) => {
                        let req = pb::ReadRequest { actor: actor.to_vec(), linearizable: true, ..Default::default() };
                        mesh.read_group(router.group_of(actor.vshard()), req).await.ok().and_then(|r| r.actor)
                    }
                    (None, None) => None,
                };
                let Some(rec) = rec else { return };
                if rec.state() == pb::actor_record::State::Dead {
                    return;
                }
                let body = pb::shard_cmd::Body::ActorDied(pb::ActorDied {
                    actor: actor.to_vec(),
                    generation: rec.generation,
                    reason: Some(pb::ErrorRecord {
                        kind: Kind::ActorDied as i32,
                        message: message.clone(),
                        ..Default::default()
                    }),
                    terminal: true,
                    moved: false,
                });
                match router.propose(actor.vshard(), body).await {
                    // A follower's copy can lag a restart: read again.
                    Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Actor(ack)) })
                        if ack.status() == pb::actor_ack::Status::StaleGeneration => {}
                    Ok(_) => return,
                    Err(e) => {
                        tracing::warn!("closing actor {}: {e}", actor.hex());
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
    }

    fn cancel_actor_call(&self, tk: Tk, why: &str) {
        self.propose(
            tk.v(),
            pb::shard_cmd::Body::Cancel(pb::Cancel {
                task: Some(tk.pb()),
                reason: Kind::ActorDied as i32,
                message: format!("actor died: {why}"),
            }),
        );
    }

    fn route_to_actor(&mut self, w: Work) {
        let Ok(actor) = Id::from_slice(&w.spec.actor) else {
            self.fail_infeasible(&w, "actor call without an actor id");
            return;
        };
        let host = self.actors.entry(actor).or_default();
        if host.dead {
            self.cancel_actor_call(w.tk, "the actor is dead");
            return;
        }
        if host.moved {
            self.give_back_actor_call(w);
            return;
        }
        let handle = w.spec.handle;
        if w.kind() == pb::CallKind::CallActorCtor {
            if host.constructed {
                // Its instance is built, or restored from its durable state:
                // this is its constructor sent again (a new leader pushes an
                // actor's record and constructor once more), and running it
                // would start the instance over.
                return;
            }
            host.ctor = Some(w);
        } else {
            actor_limits::enqueue(host, w);
            if !host.known_handles.contains(&handle) {
                self.resolve_handles(actor, Some(&[handle]));
            }
        }
        if let Some(exec) = self.actors.get(&actor).and_then(|h| h.exec)
            && self.actor_waits.keys().any(|(e, _)| *e == exec)
        {
            self.fail_self_waits(exec); // a call its running call already waits for
        }
        self.actor_pump(actor);
    }

    /// `handles` (or, from housekeeping, those already being looked up) that
    /// this host has not run a call of and whose lowest waiting call is not
    /// their first: look up where their order stands (`handle_floor`). Asked
    /// again at most every HANDLE_RECHECK while an earlier call is unfinished.
    /// Costs a lookup per handle, not a pass over the mailbox: it runs for
    /// every call routed here.
    fn resolve_handles(&mut self, actor: Id, handles: Option<&[u64]>) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
        let handles: Vec<u64> = match handles {
            Some(h) => h.to_vec(),
            None => host.resolving.keys().copied().collect(),
        };
        let mut ask = Vec::new();
        for handle in handles {
            if host.known_handles.contains(&handle) {
                host.resolving.remove(&handle);
                continue;
            }
            // The mailbox is ordered by (handle, seq): its first entry for
            // the handle is the lowest call waiting.
            let Some(w) = host.mailbox.range((handle, 0)..=(handle, u64::MAX)).next().map(|(_, w)| w) else {
                host.resolving.remove(&handle);
                continue;
            };
            let Some(prev) = w.spec.prev.as_ref().and_then(Tk::parse) else { continue };
            if w.spec.seq == 0 || host.resolving.get(&handle).is_some_and(|at| at.elapsed() < HANDLE_RECHECK) {
                continue;
            }
            host.resolving.insert(handle, Instant::now());
            ask.push((handle, prev, w.spec.seq));
        }
        for (handle, prev, seq) in ask {
            let (router, tx) = (self.router.clone(), self.me_tx.clone());
            tokio::spawn(async move {
                let floor = match handle_floor(&router, router.mesh.as_deref(), prev, seq).await {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::debug!("where actor handle {handle} stands: {e}");
                        None
                    }
                };
                let _ = tx.send(CoreMsg::HandleFloor { actor, handle, floor }).await;
            });
        }
    }

    /// Draining: move the actors that may restart elsewhere, so they do
    /// without using up a restart (their peer did not fail). Ones that never
    /// restart stay until the peer stops.
    fn move_actors(&mut self) {
        let me = self.router.identity.pb();
        let movable: Vec<Id> = self
            .actors
            .iter()
            .filter(|(a, h)| {
                h.generation > 0
                    && !h.dead
                    && !h.moved
                    && (h.restart != pb::actor_spec::Restart::Never || h.durable)
                    && self.actor_hosts.get(*a) == Some(&me)
            })
            .map(|(a, _)| *a)
            .collect();
        for actor in movable {
            if let Some(h) = self.actors.get_mut(&actor) {
                h.moving = true;
            }
            self.try_move_actor(actor);
        }
    }

    /// Move a draining peer's actor once its running calls have finished:
    /// stop its process and commit the move; the shard restarts it elsewhere
    /// without counting a restart. Its waiting calls go back once that commits.
    fn try_move_actor(&mut self, actor: Id) {
        let Some(host) = self.actors.get(&actor) else { return };
        let running = host.exec.is_some_and(|exec| self.running.iter().any(|r| r.exec == exec));
        if !host.moving || host.committing || !host.inflight.is_empty() || running {
            return;
        }
        let host = self.actors.get_mut(&actor).expect("present");
        host.moving = false;
        host.moved = true;
        host.constructed = false;
        let generation = host.generation;
        // Taken first, so its exit is not reported as a crash (actor_exec_lost).
        if let Some(exec) = host.exec.take() {
            self.sup.kill(exec);
        }
        let body = pb::shard_cmd::Body::ActorDied(pb::ActorDied {
            actor: actor.to_vec(),
            generation,
            reason: Some(pb::ErrorRecord {
                kind: Kind::ActorDied as i32,
                message: "moved: its peer is draining".into(),
                ..Default::default()
            }),
            terminal: false,
            moved: true,
        });
        use std::sync::atomic::Ordering::Relaxed;
        let (router, tx, in_flight) = (self.router.clone(), self.me_tx.clone(), self.in_flight.clone());
        in_flight.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let ok = match router.propose(actor.vshard(), body).await {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Actor(ack)) }) => {
                    ack.status() == pb::actor_ack::Status::Ok
                }
                Ok(_) => false,
                Err(e) => {
                    tracing::warn!("moving actor {}: {e}", actor.hex());
                    false
                }
            };
            in_flight.fetch_sub(1, Relaxed);
            let _ = tx.send(CoreMsg::ActorMoved { actor, ok }).await;
        });
    }

    /// The move committed: the record no longer names this peer, so calls
    /// given back now are granted again to the actor's next host. If it did
    /// not commit, the calls stay; they are retried elsewhere once this
    /// peer's incarnation is retired.
    fn on_actor_moved(&mut self, actor: Id, ok: bool) {
        self.record(pb::ClusterEvent {
            kind: "actor".into(),
            actor: actor.to_vec(),
            node: self.router.identity.peer.to_vec(),
            message: if ok {
                "moved off this draining peer".into()
            } else {
                "could not move off this draining peer".into()
            },
            ..Default::default()
        });
        if !ok {
            return;
        }
        let Some(host) = self.actors.get_mut(&actor) else { return };
        let mut calls: Vec<Work> = std::mem::take(&mut host.mailbox).into_values().collect();
        calls.extend(host.ctor.take().filter(|w| w.publish)); // a replayed constructor has its outcome
        host.next_seq.clear();
        host.done_seqs.clear();
        host.known_handles.clear();
        host.resolving.clear();
        for w in calls {
            self.give_back_actor_call(w);
        }
    }

    /// Return an unstarted actor call's attempt to the shard, which grants it
    /// again wherever its actor now lives. Nothing is counted against it.
    fn give_back_actor_call(&mut self, w: Work) {
        use std::sync::atomic::Ordering::Relaxed;
        let body = pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
            task: Some(w.tk.pb()),
            attempt: w.attempt,
            peer: Some(self.router.identity.pb()),
            kind: pb::record_failure::Kind::Requeued as i32,
            error: Some(pb::ErrorRecord {
                message: "given back unstarted: its actor moved to another peer".into(),
                attempt: w.attempt,
                ..Default::default()
            }),
            fingerprint: Vec::new(),
            memory_peak: 0,
        });
        let (router, in_flight, tk) = (self.router.clone(), self.in_flight.clone(), w.tk);
        in_flight.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let reply = router.propose(tk.v(), body).await;
            if !give_back_taken(&reply) {
                tracing::warn!("give-back of actor call {tk:?} not taken; it is retried when this peer stops");
            }
            in_flight.fetch_sub(1, Relaxed);
        });
    }

    fn on_handle_floor(&mut self, actor: Id, handle: u64, floor: Option<(u64, Vec<u64>)>) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
        let Some((next, done)) = floor else { return }; // asked again later (housekeeping)
        host.resolving.remove(&handle);
        if !host.known_handles.insert(handle) {
            return; // one of its calls started here meanwhile
        }
        host.next_seq.insert(handle, next);
        host.done_seqs.entry(handle).or_default().extend(done.into_iter().filter(|s| *s >= next));
        advance(host, handle);
        self.actor_pump(actor);
    }

    fn actor_exec_ready(&mut self, actor: Id, exec: u32) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
        if host.durable && host.state_version > 0 && !host.constructed {
            // A durable actor resumes from its last committed state; its
            // constructor ran once, before the first command. The restore
            // uses the class its record names now.
            host.ctor = None;
            host.constructed = true;
            host.upgrade = false;
            self.send_restore(actor, exec);
            self.actor_pump(actor);
            return;
        }
        if host.constructed || host.ctor.is_some() {
            self.actor_pump(actor);
            return;
        }
        // Restarted instance: re-run the committed constructor without
        // publishing (a remote host gets it pushed with the actor's record).
        let ctor_tk = self
            .router
            .read(actor.vshard(), |kv| read::actor(kv, actor))
            .flatten()
            .and_then(|r| r.spec)
            .and_then(|s| s.ctor)
            .and_then(|c| Tk::parse(&c));
        if let Some(tk) = ctor_tk
            && let Some(Some(rec)) = self.router.read(tk.v(), |kv| read::task(kv, tk))
            && rec.outcome.as_ref().is_some_and(|o| matches!(o.value, Some(pb::outcome::Value::Ok(_))))
            && let Some(spec) = rec.spec
        {
            let deps = self.dep_outcomes(tk, &spec);
            let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
            let mut w = Work::new(tk, 0, spec, policy, deps);
            w.cpus = 0.0;
            w.publish = false;
            w.start_consumed = true;
            if let Some(h) = self.actors.get_mut(&actor) {
                h.ctor = Some(w);
            }
        }
        self.actor_pump(actor);
    }

    /// Start the actor's next call if its executor is idle: the constructor
    /// first, then the lowest pending sequence number of any handle whose
    /// earlier calls have all started or finished.
    fn actor_pump(&mut self, actor: Id) {
        if self.actors.get(&actor).is_some_and(|h| h.moving) {
            self.try_move_actor(actor);
            return;
        }
        loop {
            if self.actors.get(&actor).is_some_and(|h| h.committing) {
                return;
            }
            let Some(exec) = self.actors.get(&actor).and_then(|h| h.exec) else { return };
            let Some(e) = self.execs.get(&exec) else { return };
            let inflight = self.running.iter().filter(|r| r.exec == exec).count();
            // Calls of limited methods count against their own limits (actor_limits).
            let limited = self.actors.get(&actor).is_some_and(|h| h.constructed && !h.limits.is_empty());
            if e.state == ExecState::Starting || (inflight >= e.concurrency && !limited) {
                return;
            }
            if inflight == 0 {
                self.actor_upgrade_now(actor, exec);
            }
            // The constructor runs alone, before any method.
            let constructing = self.actors.get(&actor).is_some_and(|h| !h.constructed);
            if constructing && inflight > 0 {
                return;
            }
            let started =
                if limited { self.actor_start_limited(actor, exec) } else { self.actor_start_next(actor, exec) };
            if !started {
                return;
            }
        }
    }

    fn actor_start_next(&mut self, actor: Id, exec: u32) -> bool {
        // An actor that runs one call at a time gets the calls ready after
        // the first in the same Execute (same method and options), which the
        // executor runs back to back in order: no peer round trip between
        // them. Durable actors commit between calls, so they go one by one.
        let one_at_a_time = self.execs.get(&exec).is_some_and(|e| e.concurrency <= 1);
        let Some(host) = self.actors.get_mut(&actor) else { return false };
        let limit = if one_at_a_time && !host.durable { ACTOR_BATCH } else { 1 };
        let mut batch: Vec<Work> = Vec::new();
        if let Some(ctor) = host.ctor.take() {
            batch.push(ctor);
        } else if host.constructed {
            while batch.len() < limit {
                let key = host.mailbox.keys().find(|(h, s)| host.next_seq.get(h).copied().unwrap_or(0) == *s).copied();
                let Some(key) = key else { break };
                let fits = batch.first().is_none_or(|head| {
                    let w = &host.mailbox[&key];
                    w.spec.method == head.spec.method
                        && w.spec.policy == head.spec.policy
                        && w.spec.deadline_hlc == head.spec.deadline_hlc
                });
                if !fits {
                    break;
                }
                let w = host.mailbox.remove(&key).expect("key found above");
                host.next_seq.insert(w.spec.handle, w.spec.seq + 1);
                host.known_handles.insert(w.spec.handle);
                host.inflight.insert((w.spec.handle, w.spec.seq));
                advance(host, w.spec.handle);
                batch.push(w);
            }
        }
        if batch.is_empty() {
            return false;
        }
        self.start(exec, batch);
        true
    }

    fn actor_after_call(&mut self, actor: Id, w: &Work, finished: bool) {
        if let Some(host) = self.actors.get_mut(&actor) {
            if w.kind() == pb::CallKind::CallActorCtor {
                host.constructed = true;
                // Built with the class it was created with: an upgraded one is rebuilt.
                host.upgrade |= host.durable && w.spec.fn_digest != host.class_digest;
            } else {
                host.inflight.remove(&(w.spec.handle, w.spec.seq));
            }
        }
        if finished {
            self.actor_pump(actor); // a moving actor moves once idle
        }
    }

    /// A call that finished without running here (failed prerequisite,
    /// cancelled) still counts toward its handle's order.
    fn actor_call_ended(&mut self, spec: &pb::TaskSpec) {
        if spec.kind() != pb::CallKind::CallActorMethod {
            return;
        }
        let Ok(actor) = Id::from_slice(&spec.actor) else { return };
        if let Some(host) = self.actors.get_mut(&actor) {
            host.mailbox.remove(&(spec.handle, spec.seq));
            if host.next_seq.get(&spec.handle).copied().unwrap_or(0) <= spec.seq {
                host.done_seqs.entry(spec.handle).or_default().insert(spec.seq);
            }
            advance(host, spec.handle);
        }
        self.actor_pump(actor);
    }

    fn actor_exec_lost(&mut self, actor: Id, exec: u32, detail: &str) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
        if host.exec != Some(exec) {
            return;
        }
        host.exec = None;
        host.constructed = false;
        // Calls that started on the dead instance were cancelled above; calls
        // that never started are redelivered in order to the next instance.
        host.inflight.clear();
        let generation = host.generation;
        self.propose(
            actor.vshard(),
            pb::shard_cmd::Body::ActorDied(pb::ActorDied {
                actor: actor.to_vec(),
                generation,
                reason: Some(pb::ErrorRecord {
                    kind: Kind::ActorDied as i32,
                    message: format!("actor process exited: {detail}"),
                    ..Default::default()
                }),
                terminal: false,
                moved: false,
            }),
        );
    }

    // ---- housekeeping -----------------------------------------------------

    fn housekeeping(&mut self) {
        self.expire_holds();
        self.spill();
        self.charge_budgets();
        // Actors that waited for CPUs: try again (each re-reads its record).
        for actor in std::mem::take(&mut self.actors_waiting) {
            self.on_actor_changed(actor);
        }
        self.pump_limited_actors();
        // Handles still waiting on an earlier call that ran elsewhere.
        let waiting: Vec<Id> = self.actors.iter().filter(|(_, h)| !h.resolving.is_empty()).map(|(a, _)| *a).collect();
        for actor in waiting {
            self.resolve_handles(actor, None);
        }
        if let Some(mesh) = &self.router.mesh {
            self.log_ship.flush(mesh);
        }
        for (run, summary) in self.log_dedup.expire(Instant::now()) {
            self.send_log(run, &summary);
        }
        // Sessions whose client did not come back to resume them.
        let now = Instant::now();
        let gone: Vec<u64> = self.sessions.iter().filter(|(_, s)| s.expired(now)).map(|(id, _)| *id).collect();
        for id in gone {
            self.end_session(id);
        }
        let now_hlc = self.router.hlc();
        // Deadlines: cancel queued or running work past its deadline.
        let mut expired = Vec::new();
        for w in self.queue.iter().chain(self.running.iter().flat_map(|r| r.items.iter())) {
            if w.spec.deadline_hlc != 0 && w.spec.deadline_hlc < now_hlc {
                expired.push(w.tk);
            }
        }
        for tk in expired {
            self.propose(
                tk.v(),
                pb::shard_cmd::Body::Cancel(pb::Cancel {
                    task: Some(tk.pb()),
                    reason: Kind::Deadline as i32,
                    message: "deadline exceeded".into(),
                }),
            );
        }
        self.kill_stuck_cancelled();
        // Memory limits: kill an attempt's executor when it exceeds its cap.
        let mut kill: Vec<(u32, u64, u64, Option<Tk>)> = Vec::new();
        for r in &self.running {
            let limit = r.items[0].policy.memory_limit;
            // Beside a guest, a waiting call holds what it held when it began
            // waiting; the running call is judged on its own memory (sharing).
            if limit == 0 || (r.sliced && r.blocked) || kill.iter().any(|(e, ..)| *e == r.exec) {
                continue;
            }
            if let Some(e) = self.execs.get(&r.exec)
                && let Some(reading) = accounting::read(e.pid)
                && reading.rss_bytes.saturating_sub(r.baseline) > limit
            {
                let peak = reading.peak_rss_bytes.max(reading.rss_bytes).saturating_sub(r.baseline);
                kill.push((r.exec, peak, limit, r.sliced.then(|| r.items[0].tk)));
            }
        }
        for (exec, peak, limit, culprit) in kill {
            if let Some(e) = self.execs.get_mut(&exec) {
                e.killed_for_memory = Some((peak, limit));
                e.killed_for = culprit;
            }
            self.mem_kills.push_back(Instant::now());
            self.sup.kill(exec);
        }
        self.sample_memory();
        if !self.actors_waiting_memory.is_empty() {
            self.start_waiting_actors();
        }
        if !self.actors_waiting_env.is_empty() {
            self.start_env_waiters();
        }
        if let Some(mesh) = self.router.mesh.clone() {
            for (node, p) in mesh.take_profiles() {
                self.profiles.merge_remote(node, &p);
            }
        }
        if self.profiles_saved.elapsed() > PROFILES_SAVE_EVERY {
            self.save_profiles();
        }
        // Reap executors idle for too long.
        let idle: Vec<u32> = self
            .execs
            .iter()
            .filter(|(_, e)| {
                e.actor.is_none() && e.state == ExecState::Idle && e.idle_since.elapsed() > IDLE_EXECUTOR_TTL
            })
            .map(|(id, _)| *id)
            .collect();
        for id in idle {
            self.retire_exec(id);
        }
        // Probes end, pressure lifts and heads age without any event.
        self.dispatch();
    }

    /// Memory readings each housekeeping tick: every executor's private
    /// memory, the host's availability, and the pressure response (R1).
    fn sample_memory(&mut self) {
        let arenas = self.objects.dir().join(".arena");
        for (id, e) in &mut self.execs {
            if let Some(p) = budget::private_bytes(e.pid) {
                e.private = p;
            }
            e.arena = crate::stores::dir_bytes(&arenas.join(e.pid.to_string()));
            let sample = e.private + e.arena;
            let mut on: Vec<&mut Running> = self.running.iter_mut().filter(|r| r.exec == *id).collect();
            if on.iter().any(|r| r.sliced) {
                // A guest beside waiting calls: the sample is the running call's,
                // beyond what the process held when it started (sharing).
                let active: Vec<bool> = on.iter().map(|r| !r.blocked).collect();
                let baselines: Vec<u64> = on.iter().map(|r| r.baseline).collect();
                if let Some((i, own)) = sharing::attribute_memory(sample, &active, &baselines) {
                    on[i].peak = on[i].peak.max(own);
                }
            } else {
                for r in on {
                    r.peak = r.peak.max(sample);
                }
            }
        }
        for r in &self.running {
            if r.mem == 0
                && r.started.elapsed() >= PROBE_FOR
                && let Some(e) = self.execs.get(&r.exec)
            {
                let floor = self.live_floor.entry(r.items[0].key()).or_default();
                *floor = (*floor).max(if r.sliced { r.peak } else { e.private });
            }
        }
        if self.objects_scanned.elapsed() >= OBJECTS_SCAN_EVERY {
            self.objects_scanned = Instant::now();
            let space = budget::disk_space(self.objects.dir());
            self.disk_space = space;
            // Objects, plus the object plane's pre-zeroed reserve: both memory.
            self.object_bytes = if self.budget.objects_in_pool { self.objects.memory_bytes() } else { 0 };
            crate::objplane::publish_usage(self.objects.dir(), self.object_bytes);
            // Objects in memory move to disk (oldest first, off the core's
            // thread) past spill_above() of the pool, or when their filesystem
            // runs short, which a tmpfs smaller than the pool does first.
            let over = self.memory_used().saturating_sub((spill_above() * self.budget.pool as f64) as u64).max(
                space
                    .filter(|_| self.budget.objects_in_pool)
                    .map_or(0, |(free, total)| budget::spill_for_space(free, total)),
            );
            if over > 0 && self.object_bytes > 0 && !self.spilling.swap(true, std::sync::atomic::Ordering::AcqRel) {
                let (objects, flag) = (self.objects.clone(), self.spilling.clone());
                // Until the objects are under the line again, not one round a
                // second (see spill_down_to).
                let keep = self.object_bytes.saturating_sub(over);
                tokio::task::spawn_blocking(move || {
                    let to = objects.spill_target();
                    let publish = |used| crate::objplane::publish_usage(objects.dir(), used);
                    match objects.spill_down_to(keep, over, publish) {
                        Ok(n) if n > 0 => {
                            tracing::info!(
                                "objects past the memory pool's or their filesystem's room: moved {n} bytes to {to}"
                            )
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!("cannot spill objects to {to}: {e}"),
                    }
                    flag.store(false, std::sync::atomic::Ordering::Release);
                });
            }
        }
        self.available = budget::available_bytes();
        let usable = self.budget.usable as f64;
        let avail = self.available.map(|a| a as f64);
        let psi = budget::memory_pressure();
        let low = avail.is_some_and(|a| a < PAUSE_BELOW * usable) || psi.is_some_and(|p| p > PSI_PAUSE);
        let recovered = avail.is_none_or(|a| a > RESUME_ABOVE * usable) && psi.is_none_or(|p| p <= PSI_PAUSE);
        if low && !self.mem_paused {
            self.mem_paused = true;
            tracing::warn!(available = ?self.available, ?psi, "memory pressure: pausing admission");
        } else if recovered && self.mem_paused {
            self.mem_paused = false;
        }
        if avail.is_some_and(|a| a < KILL_BELOW * usable)
            && self.mem_kills.back().is_none_or(|t| t.elapsed() > Duration::from_secs(1))
        {
            self.kill_for_pressure();
        }
        while self.mem_kills.front().is_some_and(|t| t.elapsed() > KILLS_RECENT) {
            self.mem_kills.pop_front();
        }
    }

    /// Host memory is nearly gone: stop the running attempt furthest over its
    /// reservation, never chosen by recency (Ray's monitor killed a 0.22 GB
    /// producer and spared a 147 GB writer). Actors and started external
    /// effects go last. Its retry waits until 1.5× its peak fits.
    fn kill_for_pressure(&mut self) {
        let victim = self
            .running
            .iter()
            .filter(|r| !r.blocked)
            .filter_map(|r| {
                let e = self.execs.get(&r.exec)?;
                // A guest is judged on its own memory, beyond its baseline (sharing).
                let held = (e.private + e.arena).saturating_sub(r.baseline);
                let over = held.saturating_sub(r.mem);
                let last = e.actor.is_some() || r.items.iter().any(|w| w.spec.effects() == pb::Effects::External);
                let culprit = r.sliced.then(|| r.items[0].tk);
                (over > 0).then_some((last, std::cmp::Reverse(over), r.exec, held, r.mem, culprit))
            })
            .min();
        let Some((_, _, exec, private, reserved, culprit)) = victim else { return };
        let available = self.available.unwrap_or(0) / MIB;
        if let Some(e) = self.execs.get_mut(&exec) {
            e.killed_for_memory = Some((private, reserved));
            e.killed_for = culprit;
            e.kill_note = Some(format!(
                "killed under host memory pressure ({available} MiB available): it held {} MiB with {} MiB reserved, \
                 the most over its reservation; the retry waits until 1.5x that fits",
                private / MIB,
                reserved / MIB
            ));
        }
        self.mem_kills.push_back(Instant::now());
        self.sup.kill(exec);
    }

    // ---- queries ----------------------------------------------------------

    fn explain(&self, req: pb::ExplainRequest) -> pb::ExplainReply {
        let mut reply = pb::ExplainReply::default();
        if let Ok(run) = Id::from_slice(&req.run) {
            // Task counts come from every vshard of the run's spread, which
            // ingress reads (most are in groups this peer does not host).
            reply.run = self.router.read(run.vshard(), |kv| read::run(kv, run)).flatten();
        }
        if let Some(tk) = req.task.as_ref().and_then(Tk::parse) {
            let rec = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten();
            reply.attempts = self.attempts.get(&tk).cloned().unwrap_or_default();
            reply.pending_reason = self.pending_reason(tk, rec.as_ref());
            reply.task = rec;
            if reply.task.is_none()
                && let Some(call) = self.direct.get(&tk)
            {
                // Sent straight to its actor: running or queued there.
                let spec = pb::TaskSpec {
                    key: Some(tk.pb()),
                    actor: call.actor.to_vec(),
                    method: call.method.clone(),
                    ..Default::default()
                };
                let mut task = pb::TaskRecord { spec: Some(spec), ..Default::default() };
                task.set_state(pb::task_record::State::Running);
                reply.task = Some(task);
                reply.pending_reason = self.direct_reason(tk);
            }
        }
        if req.decisions {
            reply.decisions = self.decisions.iter().cloned().collect();
        }
        reply
    }

    fn pending_reason(&self, tk: Tk, rec: Option<&pb::TaskRecord>) -> String {
        let Some(rec) = rec else { return "unknown task".into() };
        match rec.state() {
            pb::task_record::State::Done => String::new(),
            pb::task_record::State::Pending => {
                let mut why = format!(
                    "waiting on {} prerequisite(s)",
                    self.router.read(tk.v(), |kv| read::deps_left(kv, tk)).flatten().unwrap_or(rec.deps_remaining)
                );
                // A call to an actor whose process waits for memory waits on
                // its constructor, which waits on that.
                let actor = rec.spec.as_ref().and_then(|s| Id::from_slice(&s.actor).ok());
                if let Some(wait) = actor.and_then(|a| self.actors.get(&a)).and_then(|h| h.waiting_memory.as_ref()) {
                    why += &format!("; its actor has not started: {wait}");
                }
                why
            }
            pb::task_record::State::Ready => "ready; waiting to be granted".into(),
            pb::task_record::State::Running => {
                if let Some(pos) = self.queue.iter().position(|w| w.tk == tk) {
                    format!("queued at position {pos}: {}", self.wait_reason(&self.queue[pos]))
                } else if let Some(r) = self.running.iter().find(|r| r.items.iter().any(|w| w.tk == tk)) {
                    // An actor runs a batch's calls in order: one not yet
                    // reached is waiting behind the ones before it.
                    let pos = r.items.iter().position(|w| w.tk == tk).expect("found above");
                    let done = r.items.len() - r.remaining;
                    let at_actor = self.execs.get(&r.exec).is_some_and(|e| e.actor.is_some());
                    if at_actor && pos > done {
                        self.actor_queue_reason(&r.items[pos], pos - done)
                    } else {
                        "running".into()
                    }
                } else if let Some(why) = self
                    .actors
                    .values()
                    .find(|h| h.ctor.as_ref().is_some_and(|w| w.tk == tk) || h.mailbox.values().any(|w| w.tk == tk))
                    .and_then(|h| h.waiting_memory.clone())
                {
                    why
                } else if let Some(w) = self.actors.values().flat_map(|h| h.mailbox.values()).find(|w| w.tk == tk) {
                    let host = self.actors.values().find(|h| h.mailbox.values().any(|x| x.tk == tk)).expect("found");
                    let running =
                        self.running.iter().filter(|r| host.exec == Some(r.exec)).map(|r| r.remaining).sum::<usize>();
                    self.limit_reason(host, w)
                        .unwrap_or_else(|| self.actor_queue_reason(w, host.mailbox.len() - 1 + running))
                } else {
                    "granted; waiting for an actor or executor".into()
                }
            }
        }
    }

    /// A call queued at its actor: how many are queued there, and when that
    /// method's calls mostly wait on I/O, how to let them overlap (an actor
    /// runs one call at a time unless its method declares concurrency).
    fn actor_queue_reason(&self, w: &Work, ahead: usize) -> String {
        self.queue_reason(&w.key(), &w.spec.method, ahead)
    }

    fn queue_reason(&self, key: &FnKey, method: &str, ahead: usize) -> String {
        let mut why = format!("queued at its actor behind {ahead} call(s); an actor runs its calls in order");
        if ahead >= ACTOR_HINT_QUEUE
            && let Some(s) = self.profiles.summary(key).filter(|s| s.io_bound())
        {
            why += &format!(
                ". Its {} calls mostly wait ({:.2} cores over {:.2} s on average): \
                 @ch.method(concurrency=N) on {} lets N of them overlap",
                method, s.cores_mean, s.wall_p50_s, method
            );
        }
        why
    }

    /// A direct call in flight: running if its actor has a free lane for it,
    /// else queued behind the direct calls sent before it.
    fn direct_reason(&self, tk: Tk) -> String {
        let (Some(call), Some(ahead)) = (self.direct.get(&tk), self.direct.ahead(tk)) else {
            return "unknown task".into();
        };
        let exec = self.actors.get(&call.actor).and_then(|h| h.exec).and_then(|e| self.execs.get(&e));
        if ahead < exec.map_or(1, |e| e.concurrency.max(1)) {
            return "running at its actor (its caller sent it there directly)".into();
        }
        self.queue_reason(&call.key, &call.method, ahead)
    }

    /// Learn from a call the direct path ran, as `learn` does for one run here.
    fn learn_direct(&mut self, r: &crate::direct::Ran) {
        if r.attempt.end_reason != "ok" {
            return;
        }
        let policy = r.attempt.granted.clone().unwrap_or_default();
        let m = |k: &str| r.attempt.measures.get(k).map_or(0.0, |m| m.value);
        let sample = Sample {
            wall_s: m("wall_seconds"),
            cpu_s: m("cpu_seconds"),
            peak: 0,
            arg_bytes: r.arg_bytes,
            out_bytes: r.out_bytes,
            cpus: policy.cpus,
            oom: false,
        };
        let req = Request::of(&policy);
        let kills = self.mem_kills.back().is_some_and(|t| t.elapsed() < KILLS_RECENT);
        let now = self.epoch.elapsed().as_secs_f64();
        for d in self.profiles.record(&r.key, &policy.fn_name, sample, None, req, kills, now, now_ns()) {
            self.decide(d);
        }
    }

    /// What this peer knows of the cluster's members, itself included.
    fn nodes(&self) -> Vec<pb::ClusterNode> {
        let me = self.router.identity.peer.to_vec();
        let own = |cpus: f64, memory_bytes: u64, addr: String| pb::ClusterNode {
            peer: me.clone(),
            addr,
            cpus,
            memory_bytes,
            alive: true,
            cpu_used: self.cpu_used,
            queued: self.queue.len() as u64,
            this_peer: true,
            leading: self.router.leading(),
            actors: self.hosted_actors() as u32,
            object_bytes: self.objects.bytes_used(),
            draining: self.draining,
            object_layout: crate::parts::OBJECT_LAYOUT,
            demand: Some(self.demand()),
            incarnation: self.router.identity.incarnation,
            ..Default::default()
        };
        let Some(mesh) = self.router.mesh.as_ref() else {
            let labels = crate::labels::own(&me).into_iter().collect();
            let gpus = self.devices.inventory().devices.iter().map(|d| d.id.clone()).collect();
            return vec![pb::ClusterNode { labels, gpus, ..own(self.capacity, self.budget.pool, String::new()) }];
        };
        mesh.members()
            .into_iter()
            .map(|v| {
                if v.member.peer == me {
                    return pb::ClusterNode {
                        gpus: v.member.gpus.clone(),
                        labels: v.member.labels.clone(),
                        ..own(v.member.cpus, v.member.memory_bytes, v.member.addr.clone())
                    };
                }
                pb::ClusterNode {
                    peer: v.member.peer.clone(),
                    addr: v.member.addr.clone(),
                    cpus: v.member.cpus,
                    memory_bytes: v.member.memory_bytes,
                    gpus: v.member.gpus.clone(),
                    alive: !v.dead,
                    cpu_used: v.load.as_ref().map_or(0.0, |l| l.cpu_used),
                    queued: v.load.as_ref().map_or(0, |l| u64::from(l.queued)),
                    this_peer: false,
                    labels: v.member.labels.clone(),
                    leading: v.load.as_ref().map(|l| l.leading.clone()).unwrap_or_default(),
                    actors: v.load.as_ref().map_or(0, |l| l.actors),
                    object_bytes: v.load.as_ref().map_or(0, |l| l.object_bytes),
                    draining: v.load.as_ref().is_some_and(|l| l.stopping),
                    object_layout: v.member.object_layout,
                    demand: v.load.as_ref().and_then(|l| l.demand.clone()),
                    load_age_ms: v.last_ok.map_or(0, |t| t.elapsed().as_millis() as u64),
                    // A load report its core did not fill (mesh.rs's ping handler) says nothing.
                    starting: v.load.as_ref().is_none_or(|l| l.draining && !l.stopping),
                    incarnation: v.member.incarnation,
                }
            })
            .collect()
    }

    fn group_status(&self) -> Vec<pb::GroupStatus> {
        let addr = |node: u64| match &self.router.mesh {
            Some(mesh) => mesh.member(node).map(|p| p.member.addr).unwrap_or_else(|| format!("node {node}")),
            None => "this peer".to_string(),
        };
        self.router
            .hosted_groups()
            .iter()
            .map(|g| {
                let s = g.stats();
                pb::GroupStatus {
                    group: g.id,
                    leader: s.leader.map(addr).unwrap_or_default(),
                    voters: s.voters.len() as u32,
                    last_log: s.last_log,
                    applied: s.applied,
                    leading: s.leader == Some(self.router.node_id()),
                    max_lag: s.max_lag.unwrap_or(0),
                    durability: crate::durability::group(&self.router, &s.voters).0.name().into(),
                }
            })
            .collect()
    }

    /// The actors this peer hosts, by state, every state listed. `pending`:
    /// never started (placing, or waiting for memory); `starting`: its
    /// constructor runs; `restarting`: its process ended and another is due;
    /// `moving`: its peer drains. An actor that moved away is not counted.
    fn actor_states(&self) -> HashMap<String, u32> {
        const STATES: [&str; 6] = ["pending", "starting", "alive", "moving", "restarting", "dead"];
        let mut by: HashMap<String, u32> = STATES.iter().map(|s| (s.to_string(), 0)).collect();
        for h in self.actors.values().filter(|h| !h.moved) {
            let state = if h.dead {
                "dead"
            } else if h.moving {
                "moving"
            } else if h.exec.is_none() || h.waiting_memory.is_some() {
                if h.started.is_some() { "restarting" } else { "pending" }
            } else if !h.constructed {
                "starting"
            } else {
                "alive"
            };
            *by.get_mut(state).expect("every state is listed") += 1;
        }
        by
    }

    fn status(&self) -> pb::StatusReply {
        let durability = crate::durability::report(&self.router);
        let mesh = self.router.mesh.as_ref();
        let heartbeats = mesh.map(|m| m.heartbeats.samples()).unwrap_or_default();
        let busy = self.execs.values().filter(|e| e.state == ExecState::Busy).count() as u32;
        let mut reasons = Vec::new();
        if let Some(w) = self.queue.front() {
            reasons.push(format!("{} attempt(s) queued; the first {}", self.queue.len(), self.wait_reason(w)));
        }
        if let Some(why) = self
            .actors_waiting_memory
            .front()
            .and_then(|(a, ..)| self.actors.get(a))
            .and_then(|h| h.waiting_memory.as_ref())
        {
            reasons.push(format!("{} actor(s) waiting to start; the first: {why}", self.actors_waiting_memory.len()));
        }
        if !self.actors_waiting_env.is_empty() {
            let n = self.actors_waiting_env.len();
            reasons.push(format!("{n} actor(s) waiting for their run's environment to reach this peer"));
        }
        if let Some((n, held)) = self.nested_waits() {
            reasons.push(format!(
                "{n} worker processes held by calls waiting on calls they started ({} MiB in all)",
                held / MIB
            ));
        }
        pb::StatusReply {
            peer: None,
            runs: Vec::new(),
            executors: self.execs.len() as u32,
            busy_executors: busy,
            queued: (self.queue.len() + self.localizing.len()) as u64,
            running: self.running.iter().map(|r| r.remaining as u64).sum(),
            cpu_in_use: self.cpu_used,
            object_bytes: self.objects.bytes_used(),
            pending_reasons: reasons,
            sessions: self.sessions.len() as u32,
            session_bytes: self.sessions.values().map(|s| s.mailbox.queued_bytes() as u64).sum(),
            session_backlog: self.sessions.values().map(|s| s.backlog() as u64).sum(),
            dispatch_p50_us: percentile(&self.dispatch_us, 0.50),
            dispatch_p99_us: percentile(&self.dispatch_us, 0.99),
            dispatch_samples: self.dispatch_us.len() as u64,
            // Batched publishes and grants not yet flushed count too.
            proposals_in_flight: self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
                + self.pending_publish.values().map(|v| v.len() as u64).sum::<u64>()
                + self.pending_grants.values().map(|v| v.len() as u64).sum::<u64>(),
            memory_pool: self.budget.pool,
            memory_used: self.memory_used(),
            memory_available: self.available.unwrap_or(0),
            links: self.router.mesh.as_ref().map(|m| m.links()).unwrap_or_default(),
            traffic: mesh.map(|m| m.traffic.status()).unwrap_or_default(),
            mesh_rate: mesh.map_or(0, |m| m.traffic.rate() as u64),
            heartbeat_p50_us: percentile(&heartbeats, 0.50),
            heartbeat_p99_us: percentile(&heartbeats, 0.99),
            fleet: self.fleet.as_ref().map(|f| f.view()),
            tasks_by_state: [
                ("queued", self.queue.len() as u64),
                ("fetching_inputs", self.localizing.len() as u64),
                ("running", self.running.iter().map(|r| r.remaining as u64).sum()),
            ]
            .into_iter()
            .map(|(state, n)| (state.to_string(), n))
            .collect(),
            actors_by_state: self.actor_states(),
            queue_oldest_seconds: self.queue.iter().map(|w| w.ready_at.elapsed().as_secs_f64()).fold(0.0, f64::max),
            nodes: self.nodes(),
            groups: self.group_status(),
            data_groups: if self.router.is_cluster() { self.router.data_groups().count() as u32 } else { 0 },
            durability_requested: durability.0.name().into(),
            durability_effective: durability.1.name().into(),
            durability_degraded: durability.2,
            objects: Vec::new(), // filled by the ingress when asked (StatusRequest.objects)
            automatic: vec![
                self.budget.describe(),
                format!("cpu capacity {:.2}", self.capacity),
                crate::topology::pids_room().map_or_else(
                    || "no task limit (cgroup pids.max)".into(),
                    |(current, max)| format!("task limit {max} (cgroup pids.max), {current} in use"),
                ),
                format!(
                    "{} learned function profile(s), saved in {}",
                    self.profiles.len(),
                    self.profiles_path.display()
                ),
            ]
            .into_iter()
            .chain(crate::durability::describe())
            .collect(),
        }
    }
}

fn percentile(xs: &VecDeque<f64>, q: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v: Vec<f64> = xs.iter().copied().collect();
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * q).round() as usize]
}

/// Skip over sequence numbers of calls that already finished elsewhere.
fn advance(host: &mut ActorHost, handle: u64) {
    let next = host.next_seq.entry(handle).or_insert(0);
    if let Some(done) = host.done_seqs.get_mut(&handle) {
        while done.remove(next) {
            *next += 1;
        }
    }
}

/// The `chimaera inspect` row a task counts under: its state, or for a
/// finished task whether it succeeded or how it failed.
pub(crate) fn count_key(t: &pb::TaskRecord) -> String {
    match (t.state(), t.outcome.as_ref().and_then(|o| o.value.as_ref())) {
        (pb::task_record::State::Done, Some(pb::outcome::Value::Ok(_))) => "succeeded".to_string(),
        (pb::task_record::State::Done, Some(pb::outcome::Value::Err(e))) => {
            format!("failed:{:?}", e.kind()).to_lowercase()
        }
        (s, _) => format!("{s:?}").to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acked(status: pb::task_ack::Status) -> Result<pb::ShardReply, ()> {
        let ack = pb::TaskAck { status: status as i32, ..Default::default() };
        Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![ack] })) })
    }

    #[test]
    fn a_runs_record_is_read_again_only_once_none_is_in_flight_and_a_second_passed() {
        let now = Instant::now();
        assert!(run_fetch_due(None, now), "the first read goes out at once");
        assert!(!run_fetch_due(Some(&(true, now - Duration::from_secs(5))), now), "one read at a time");
        assert!(!run_fetch_due(Some(&(false, now)), now), "a failed read is not repeated at once");
        assert!(run_fetch_due(Some(&(false, now - RUN_FETCH_EVERY)), now), "it is asked again after a second");
    }

    #[test]
    fn only_an_accepted_give_back_moves_the_work_away() {
        use pb::task_ack::Status;
        assert!(give_back_taken(&acked(Status::Accepted)));
        assert!(give_back_taken(&acked(Status::Existing)));
        for refused in [Status::Stale, Status::Rejected, Status::NotFound, Status::Denied] {
            assert!(!give_back_taken(&acked(refused)), "{refused:?} must keep the work here");
        }
        assert!(!give_back_taken::<()>(&Err(())), "a lost answer must keep the work here");
        assert!(!give_back_taken::<()>(&Ok(pb::ShardReply { body: None })));
    }

    #[test]
    fn only_a_deterministic_function_without_external_effects_runs_again_to_remake_its_value() {
        let call = |kind: pb::CallKind, effects: pb::Effects, deterministic: bool| pb::TaskSpec {
            kind: kind as i32,
            effects: effects as i32,
            policy: pb::Policy { deterministic, ..Default::default() }.encode_to_vec(),
            ..Default::default()
        };
        use pb::CallKind::{CallActorMethod, CallFunction, CallMapItem};
        let pure = pb::Effects::None;
        assert_eq!(replay_refusal(&call(CallFunction, pure, true)), None);
        assert_eq!(replay_refusal(&call(CallMapItem, pb::Effects::Idempotent, true)), None);
        let why = replay_refusal(&call(CallFunction, pure, false)).expect("a nondeterministic function is refused");
        assert!(why.contains("deterministic=False"), "{why}");
        assert!(replay_refusal(&call(CallMapItem, pure, false)).is_some(), "so is a nondeterministic map item");
        assert!(replay_refusal(&call(CallFunction, pb::Effects::External, true)).is_some());
        assert!(replay_refusal(&call(CallActorMethod, pure, true)).is_some());
        let no_policy = pb::TaskSpec { policy: vec![], ..call(CallFunction, pure, true) };
        assert!(replay_refusal(&no_policy).is_some(), "a call recorded without a policy is not assumed deterministic");
    }
}
