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
use crate::executor::{ExecEnv, ExecEvent, Supervisor};
use crate::router::{Router, now_ns};
use crate::stores::{CodeStore, ObjectStore, hex};
use chimaera_core::hlc;
use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::shard::{Event, Tk, read};
use chimaera_raft::GroupEvents;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

type Kind = pb::error_record::Kind;

/// Largest auto-batched range (A1).
const MAX_BATCH: usize = 256;
/// Target wall time of an auto-batched range (A1).
const BATCH_TARGET_S: f64 = 0.2;
/// Items faster than this are worth batching (A1).
const BATCH_BELOW_S: f64 = 0.05;
const FLUSH_AT: usize = 256;
const ATTEMPT_LOG_CAP: usize = 200_000;
const IDLE_EXECUTOR_TTL: Duration = Duration::from_secs(60);

pub enum CoreMsg {
    Raft(GroupEvents),
    Exec(Box<ExecEvent>),
    OpenSession {
        id: u64,
        tx: mpsc::UnboundedSender<pb::SessionEvent>,
    },
    BindRun {
        session: u64,
        run: Id,
    },
    CloseSession {
        id: u64,
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
    Cancel {
        task: Tk,
        force: bool,
    },
    /// The session's driver is exiting normally: finish its run.
    FinishSessionRun {
        session: u64,
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
    /// Result of committing an external-effect attempt's start.
    StartResult {
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
    /// The objects an attempt reads are now local (or could not be fetched).
    Localized {
        work: Box<Work>,
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
    /// Load summary for a liveness ping.
    Load {
        reply: oneshot::Sender<pb::PingReply>,
    },
    /// Stop taking new work (the peer is shutting down).
    Drain,
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
}

impl Work {
    fn kind(&self) -> pb::CallKind {
        self.spec.kind()
    }
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
}

struct Running {
    exec: u32,
    items: Vec<Work>,
    remaining: usize,
    cpus: f64,
    blocked: bool,
    start: Option<Reading>,
    dispatched_ns: u64,
}

#[derive(Default)]
struct ActorHost {
    generation: u64,
    exec: Option<u32>,
    run: Option<Id>,
    /// Constructor finished on the current executor.
    constructed: bool,
    ctor: Option<Work>,
    mailbox: BTreeMap<(u64, u64), Work>,
    next_seq: HashMap<u64, u64>,
    done_seqs: HashMap<u64, BTreeSet<u64>>,
    inflight: BTreeSet<(u64, u64)>,
    dead: bool,
    /// Generation an ActorPlace was proposed for (by this peer as leader).
    placing: Option<u64>,
    restart: pb::actor_spec::Restart,
    detached: bool,
}

#[derive(Default)]
struct Profile {
    samples: u64,
    ewma_s: f64,
    batch: usize,
}

struct RunInfo {
    env: ExecEnv,
    detached: bool,
    sessions: BTreeSet<u64>,
}

struct Session {
    tx: mpsc::UnboundedSender<pb::SessionEvent>,
    run: Option<Id>,
}

pub struct Core {
    me_tx: mpsc::Sender<CoreMsg>,
    router: Arc<Router>,
    sup: Arc<Supervisor>,
    objects: ObjectStore,
    code: CodeStore,
    capacity: f64,
    cpu_used: f64,
    queue: VecDeque<Work>,
    execs: HashMap<u32, Exec>,
    running: Vec<Running>,
    actors: HashMap<Id, ActorHost>,
    sessions: HashMap<u64, Session>,
    watchers: HashMap<Tk, Vec<u64>>,
    runs: HashMap<Id, RunInfo>,
    pending_grants: BTreeMap<u16, Vec<pb::Grant>>,
    pending_publish: BTreeMap<u16, Vec<pb::PublishEntry>>,
    attempts: HashMap<Tk, Vec<pb::AttemptRecord>>,
    attempt_order: VecDeque<Tk>,
    profiles: HashMap<Vec<u8>, Profile>,
    decisions: VecDeque<pb::Decision>,
    peer_socket: String,
    isolation: Option<Isolation>,
    iso_seq: u64,
    /// Isolated attempts waiting for their dedicated executor to connect.
    iso_waiting: HashMap<u32, Work>,
    /// Recent grant-to-handoff latencies (µs) of attempts that found a warm
    /// executor waiting, for the dispatch-latency gate.
    dispatch_us: VecDeque<f64>,
    /// Attempts granted to other peers, sent with the next flush (by node id).
    pending_dispatch: BTreeMap<u64, Vec<pb::RemoteWork>>,
    /// Attempts waiting for their objects to be fetched.
    localizing: HashSet<(Tk, u32)>,
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
}

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
    ) -> Core {
        Core {
            me_tx,
            router,
            sup,
            objects,
            code,
            capacity,
            cpu_used: 0.0,
            queue: VecDeque::new(),
            execs: HashMap::new(),
            running: Vec::new(),
            actors: HashMap::new(),
            sessions: HashMap::new(),
            watchers: HashMap::new(),
            runs: HashMap::new(),
            pending_grants: BTreeMap::new(),
            pending_publish: BTreeMap::new(),
            attempts: HashMap::new(),
            attempt_order: VecDeque::new(),
            profiles: HashMap::new(),
            decisions: VecDeque::new(),
            peer_socket,
            isolation,
            iso_seq: 0,
            iso_waiting: HashMap::new(),
            dispatch_us: VecDeque::new(),
            pending_dispatch: BTreeMap::new(),
            localizing: HashSet::new(),
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
                    None => return,
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
        for (v, grants) in std::mem::take(&mut self.pending_grants) {
            self.propose_grants(v, grants);
        }
        for (v, entries) in std::mem::take(&mut self.pending_publish) {
            self.propose(v, pb::shard_cmd::Body::Publish(pb::PublishResults { entries }));
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
            CoreMsg::Localized { work, error } => {
                self.localizing.remove(&(work.tk, work.attempt));
                match error {
                    None => self.enqueue(*work),
                    Some(why) => self.fail_attempt(&work, pb::record_failure::Kind::Infra, &why),
                }
                self.dispatch();
            }
            CoreMsg::Outcome { task, outcome } => {
                self.remote_watched.remove(&task);
                self.notify_watchers(task, &outcome);
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
            CoreMsg::Drain => self.draining = true,
            CoreMsg::Forget(f) => self.forget(f),
            CoreMsg::Load { reply } => {
                let _ = reply.send(pb::PingReply {
                    cpus: self.capacity,
                    cpu_used: self.cpu_used,
                    queued: self.queue.len() as u32,
                    running: self.running.len() as u32,
                    actors: self.execs.values().filter(|e| e.actor.is_some()).count() as u32,
                    draining: self.draining,
                    ..Default::default()
                });
            }
            CoreMsg::Exec(e) => {
                self.on_exec(*e);
                self.dispatch();
            }
            CoreMsg::OpenSession { id, tx } => {
                self.sessions.insert(id, Session { tx, run: None });
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
            CoreMsg::RegisterRun { run, env, detached } => {
                self.runs.entry(run).or_insert(RunInfo { env, detached, sessions: BTreeSet::new() });
            }
            CoreMsg::Watch { session, tasks } => self.watch(session, tasks),
            CoreMsg::Cancel { task, force } => self.cancel(task, force),
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
            CoreMsg::CloseActor { actor, reason } => self.close_actor(actor, &reason),
            CoreMsg::Explain { req, reply } => {
                let _ = reply.send(self.explain(req));
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
            Event::Subscribe { producer, subscriber_vshard } => self.propose(
                producer.v(),
                pb::shard_cmd::Body::Subscribe(pb::Subscribe {
                    producer: Some(producer.pb()),
                    subscriber_vshard: subscriber_vshard as u32,
                }),
            ),
            Event::Outbox { seq, target_vshard } if leader => self.deliver_outbox(group, seq, target_vshard),
            Event::Outbox { .. } => {}
            Event::Releasable(tk) if leader => {
                if let Some(Some(rec)) = self.router.read(tk.v(), |kv| read::task(kv, tk))
                    && let Some(pb::outcome::Value::Ok(obj)) = rec.outcome.and_then(|o| o.value)
                {
                    self.drop_object(obj);
                }
            }
            Event::Releasable(_) => {}
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
            Event::CancelRunContinues(run) if leader => self.propose(
                run.vshard(),
                pb::shard_cmd::Body::CancelRun(pb::CancelRun { run: run.to_vec(), message: String::new() }),
            ),
            Event::CancelRunContinues(_) => {}
            Event::RunExpired(run) => {
                self.objects.delete_run(&run);
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
                    self.end_run_actors(run);
                    self.broadcast_forget(pb::ForgetRequest {
                        run: run.to_vec(),
                        run_ended: true,
                        ..Default::default()
                    });
                }
            }
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
        let Some(mesh) = self.router.mesh.clone() else {
            self.objects.delete(&obj);
            return;
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
        for obj in &f.objects {
            self.objects.delete(obj);
        }
        if let Ok(run) = Id::from_slice(&f.run) {
            if f.run_ended {
                self.end_run_actors(run);
            }
            if f.run_expired {
                self.objects.delete_run(&run);
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
            if let Some(n) = e.notify {
                self.deliver_outbox_entry(group, e.seq, e.target_vshard as u16, n);
            }
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

    fn deliver_outbox(&self, group: u32, seq: u64, target: u16) {
        let Some(g) = self.router.hosted(group) else { return };
        let entries = g.read(|kv| read::outbox(kv, usize::MAX)).unwrap_or_default();
        let Some(entry) = entries.into_iter().find(|e| e.seq == seq) else { return };
        let Some(notify) = entry.notify else { return };
        self.deliver_outbox_entry(group, seq, target, notify);
    }

    fn deliver_outbox_entry(&self, group: u32, seq: u64, target: u16, notify: pb::DepNotify) {
        let p = self.router.clone();
        // The ack goes to the group holding the outbox (any vshard it owns).
        let home = p.vshards_of(group).first().copied().unwrap_or(0) as u16;
        tokio::spawn(async move {
            if p.propose(target, pb::shard_cmd::Body::DepNotify(notify)).await.is_ok() {
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
        let here = self.cpu_used + self.queue.len() as f64;
        let mine = self.pressure(self.router.node_id(), here, None, self.capacity);
        let fits_here = !self.draining && self.capacity + 1e-9 >= cpus;
        let mut best = fits_here.then(|| (mine, self.router.identity.pb()));
        let others: Vec<_> = mesh.live_others().into_iter().filter(|p| p.member.cpus + 1e-9 >= cpus).collect();
        if !others.is_empty() {
            // Two random choices: near-least-loaded without a global view.
            let seed = (tk.task.0[0] as usize) ^ (now_ns() as usize);
            for i in 0..2.min(others.len()) {
                let p = &others[(seed + i * 7919) % others.len()];
                let reported = p.load.as_ref().map_or(0.0, |l| l.cpu_used + l.queued as f64);
                let score = self.pressure(p.member.node_id, reported, p.last_ok, p.member.cpus);
                if best.as_ref().is_none_or(|(b, _)| score < *b) {
                    best = Some((score, pb::PeerInc { peer: p.member.peer.clone(), inc: p.member.incarnation }));
                }
            }
        }
        tracing::trace!(
            task = %tk.task.hex(),
            mine,
            others = ?others
                .iter()
                .map(|p| (
                    p.member.node_id % 1000,
                    p.load.as_ref().map(|l| (l.cpu_used, l.queued)),
                    p.last_ok.map(|t| t.elapsed().as_millis()),
                    self.outstanding.get(&p.member.node_id).copied(),
                ))
                .collect::<Vec<_>>(),

            chosen = ?best.as_ref().map(|(s, p)| (*s, hex(&p.peer[12..]))),
            "placement"
        );
        best.map(|(_, p)| Some(p))
    }

    /// Where `actor` lives, if known. Unknown hosts are resolved in the
    /// background (or wait for the actor's placement) and `tk` placed again.
    fn actor_host(&mut self, actor: Id, tk: Tk) -> Option<pb::PeerInc> {
        if let Some(rec) = self.router.read(actor.vshard(), |kv| read::actor(kv, actor)).flatten() {
            return match rec.state() {
                pb::actor_record::State::Alive => rec.host,
                // Dead: the host that held it cancels the call on arrival.
                pb::actor_record::State::Dead => rec.host.or(Some(self.router.identity.pb())),
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
            let run = self.router.read(tk.v(), |kv| read::run(kv, tk.run)).flatten();
            let rw = pb::RemoteWork {
                task: Some(tk.pb()),
                attempt,
                spec: Some(spec),
                deps,
                env: run.as_ref().and_then(|r| r.exec_env.clone()),
                detached: run.is_some_and(|r| r.detached),
                replay: false,
            };
            self.pending_dispatch.entry(target.member.node_id).or_default().push(rw);
            return;
        }
        let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
        let cpus = if policy.cpus > 0.0 { policy.cpus } else { 1.0 };
        let work = Work {
            tk,
            attempt,
            spec,
            policy,
            deps,
            cpus,
            ready_at: Instant::now(),
            publish: true,
            start_consumed: false,
        };
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
            || self.queue.iter().any(|w| w.tk == tk && w.attempt == attempt)
            || self.running.iter().any(|r| r.items.iter().any(|w| w.tk == tk && w.attempt == attempt))
            || self
                .actors
                .values()
                .any(|h| h.mailbox.values().chain(h.ctor.iter()).any(|w| w.tk == tk && w.attempt == attempt));
        if known {
            return; // a new leader re-sent it
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
        let cpus = if policy.cpus > 0.0 { policy.cpus } else { 1.0 };
        // A replayed constructor rebuilds a restarted actor's state; its
        // outcome is already published and it runs inside the actor's CPU.
        self.admit(Work {
            tk,
            attempt,
            spec,
            policy,
            deps: rw.deps,
            cpus: if rw.replay { 0.0 } else { cpus },
            ready_at: Instant::now(),
            publish: !rw.replay,
            start_consumed: rw.replay,
        });
    }

    /// Queue an attempt once its code and every object it reads are in the
    /// local stores (a cluster peer fetches what it is missing).
    fn admit(&mut self, w: Work) {
        let Some(mesh) = self.router.mesh.clone() else {
            self.enqueue(w);
            return;
        };
        let mut missing: Vec<pb::ObjectRef> = Vec::new();
        let args = w.spec.args.iter();
        let deps = w.deps.iter().filter_map(|o| match &o.value {
            Some(pb::outcome::Value::Ok(obj)) => Some(obj),
            _ => None,
        });
        for obj in args.chain(deps) {
            if obj.inline_data.is_empty()
                && !obj.run.is_empty()
                && !self.objects.path(&obj.run, &obj.object_id).exists()
            {
                missing.push(obj.clone());
            }
        }
        let digest = w.spec.fn_digest.clone();
        let need_code = !digest.is_empty() && !self.code.has(&digest);
        if missing.is_empty() && !need_code {
            self.enqueue(w);
            return;
        }
        self.localizing.insert((w.tk, w.attempt));
        let (tx, store, code) = (self.me_tx.clone(), self.objects.clone(), self.code.clone());
        tokio::spawn(async move {
            let mut error = None;
            if need_code && let Err(e) = mesh.localize_code(&code, &digest).await {
                error = Some(format!("cannot fetch the function's code {}: {e}", hex(&digest)));
            }
            for obj in missing.iter().filter(|_| error.is_none()) {
                let holders = mesh.holders(obj);
                if let Err(e) = mesh.localize(&store, &obj.run, &obj.object_id, &holders).await {
                    error = Some(format!("cannot fetch input object {}: {e}", hex(&obj.object_id)));
                    break;
                }
            }
            let _ = tx.send(CoreMsg::Localized { work: Box::new(w), error }).await;
        });
    }

    fn enqueue(&mut self, w: Work) {
        match w.kind() {
            pb::CallKind::CallActorCtor | pb::CallKind::CallActorMethod => self.route_to_actor(w),
            _ => self.queue.push_back(w),
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
        spec.deps
            .iter()
            .map(|d| {
                Tk::parse(d)
                    .and_then(|dep| self.router.read(tk.v(), |kv| read::dep_outcome(kv, tk.v(), dep)).flatten())
                    .unwrap_or_default()
            })
            .collect()
    }

    fn notify_watchers(&mut self, tk: Tk, outcome: &pb::Outcome) {
        let Some(ids) = self.watchers.remove(&tk) else { return };
        let ev = pb::SessionEvent {
            seq: 0,
            event: Some(pb::session_event::Event::Outcome(pb::OutcomeEvent {
                task: Some(tk.pb()),
                outcome: Some(outcome.clone()),
            })),
        };
        for id in ids {
            if let Some(s) = self.sessions.get(&id) {
                let _ = s.tx.send(ev.clone());
            }
        }
    }

    /// Stop queued or running attempts of a task that ended.
    fn stop_local(&mut self, tk: Tk) {
        self.queue.retain(|w| w.tk != tk);
        let running_on: Vec<u32> =
            self.running.iter().filter(|r| r.items.iter().any(|w| w.tk == tk)).map(|r| r.exec).collect();
        for exec in running_on {
            self.sup.send(
                exec,
                pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Cancel(pb::ExecCancel { task: Some(tk.pb()) })) },
            );
        }
    }

    fn on_done(&mut self, tk: Tk, outcome: pb::Outcome, leader: bool) {
        self.unplace_as(tk, true);
        self.notify_watchers(tk, &outcome);
        // A queued attempt whose task already finished (cancelled, or another
        // attempt won) is dropped; a running one is told to stop.
        self.stop_local(tk);
        let rec = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten();
        if let Some(spec) = rec.as_ref().and_then(|r| r.spec.as_ref()) {
            self.actor_call_ended(spec);
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
            if !self.router.hosts(tk.v()) {
                self.watchers.entry(tk).or_default().push(session);
                if self.remote_watched.insert(tk) {
                    remote.entry(self.router.group_of(tk.v())).or_default().push(tk);
                }
                continue;
            }
            let done = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten().and_then(|r| r.outcome);
            match done {
                Some(outcome) => {
                    if let Some(s) = self.sessions.get(&session) {
                        let _ = s.tx.send(pb::SessionEvent {
                            seq: 0,
                            event: Some(pb::session_event::Event::Outcome(pb::OutcomeEvent {
                                task: Some(tk.pb()),
                                outcome: Some(outcome),
                            })),
                        });
                    }
                }
                None => self.watchers.entry(tk).or_default().push(session),
            }
        }
        for (group, tks) in remote {
            self.remote_watch(group, tks);
        }
    }

    /// Follow outcomes of tasks whose group this peer does not host, through
    /// a peer that does; another replica takes over if that one goes away.
    fn remote_watch(&self, group: u32, tasks: Vec<Tk>) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        let tx = self.me_tx.clone();
        tokio::spawn(async move {
            let mut left: BTreeSet<Tk> = tasks.into_iter().collect();
            for round in 0..20 {
                for node in mesh.voters_of(group) {
                    if left.is_empty() {
                        return;
                    }
                    if node == mesh.me.node_id {
                        continue;
                    }
                    let keys = left.iter().map(|t| t.pb()).collect();
                    let Ok(mut stream) = mesh.watch(node, keys).await else { continue };
                    while let Ok(Some(ev)) = stream.message().await {
                        let Some(tk) = ev.task.as_ref().and_then(Tk::parse) else { continue };
                        if left.remove(&tk) {
                            let outcome = ev.outcome.unwrap_or_default();
                            if tx.send(CoreMsg::Outcome { task: tk, outcome }).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                if left.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100 * (round + 1))).await;
            }
            let _ = tx.send(CoreMsg::WatchLost { tasks: left.into_iter().collect() }).await;
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

    fn close_session(&mut self, id: u64) {
        let Some(s) = self.sessions.remove(&id) else { return };
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
                    }),
                );
            }
        }
    }

    // ---- dispatch ---------------------------------------------------------

    /// The run's executor environment: registered by its client, or (after a
    /// peer restart) loaded from the replicated run record.
    fn env_of(&mut self, run: &Id) -> Option<ExecEnv> {
        if let Some(r) = self.runs.get(run) {
            return Some(r.env.clone());
        }
        let rec = self.router.read(run.vshard(), |kv| read::run(kv, *run)).flatten()?;
        let env = ExecEnv::from_spec(rec.exec_env?);
        self.runs.insert(*run, RunInfo { env: env.clone(), detached: rec.detached, sessions: BTreeSet::new() });
        Some(env)
    }

    fn dispatch(&mut self) {
        while let Some(head) = self.queue.front() {
            let cpus = head.cpus;
            if cpus > self.capacity + 1e-9 {
                let w = self.queue.pop_front().expect("head exists");
                let why = format!("needs {cpus} CPUs; this peer offers {} (largest feasible shape)", self.capacity);
                self.fail_infeasible(&w, &why);
                continue;
            }
            if self.cpu_used + cpus > self.capacity + 1e-9 {
                return;
            }
            if head.spec.effects() == pb::Effects::External && !head.start_consumed {
                // User code with external effects starts only after its start
                // is committed: a later loss then reports OutcomeUnknown.
                let w = self.queue.pop_front().expect("head exists");
                self.consume_start(w);
                continue;
            }
            if head.policy.accounting() == pb::policy::Accounting::Isolated {
                let w = self.queue.pop_front().expect("head exists");
                self.start_isolated(w);
                continue;
            }
            let run = head.tk.run;
            let Some(env) = self.env_of(&run) else {
                let w = self.queue.pop_front().expect("head exists");
                self.fail_infeasible(&w, "no executor environment is registered for this run");
                continue;
            };
            let threads = cpus.ceil() as u32;
            let key = env.key(threads);
            let Some(exec) = self.idle_exec(&key) else {
                self.ensure_executor(&env, threads, &key);
                return;
            };
            let batch = self.take_batch();
            self.start(exec, batch);
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
        let key = format!("isolated-{}", self.iso_seq);
        let extra = [("CHIMAERA_PEER_SOCKET", self.peer_socket.clone())];
        match self.sup.spawn(&env, threads, &extra, Some(&group)) {
            Ok((id, pid)) => {
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

    fn idle_exec(&self, key: &str) -> Option<u32> {
        self.execs
            .iter()
            .filter(|(_, e)| e.actor.is_none() && e.state == ExecState::Idle && e.env_key == key)
            .map(|(id, _)| *id)
            .min()
    }

    fn ensure_executor(&mut self, env: &ExecEnv, threads: u32, key: &str) {
        let starting = self.execs.values().filter(|e| e.state == ExecState::Starting && e.env_key == key).count();
        if starting > 0 {
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
        let extra = [("CHIMAERA_PEER_SOCKET", self.peer_socket.clone())];
        match self.sup.spawn(env, threads, &extra, None) {
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
                    },
                );
                Some(id)
            }
            Err(e) => {
                tracing::error!("cannot start executor with {}: {e}", env.python);
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
    fn take_batch(&mut self) -> Vec<Work> {
        let first = self.queue.pop_front().expect("caller checked");
        let mut batch = vec![first];
        let head = &batch[0];
        if head.kind() != pb::CallKind::CallMapItem || head.policy.interactive {
            return batch;
        }
        let k = self.profiles.get(&head.spec.fn_digest).map_or(1, |p| p.batch.max(1));
        let (digest, run) = (head.spec.fn_digest.clone(), head.tk.run);
        while batch.len() < k {
            match self.queue.front() {
                Some(w) if w.kind() == pb::CallKind::CallMapItem && w.spec.fn_digest == digest && w.tk.run == run => {
                    batch.push(self.queue.pop_front().expect("front exists"));
                }
                _ => break,
            }
        }
        batch
    }

    fn start(&mut self, exec: u32, items: Vec<Work>) {
        let head = items[0].clone();
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
        };
        if !self.sup.send(exec, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Execute(msg)) }) {
            // Executor vanished between ready and dispatch; requeue.
            for w in items.into_iter().rev() {
                self.queue.push_front(w);
            }
            self.execs.remove(&exec);
            return;
        }
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        self.cpu_used += cpus;
        let waited = head.ready_at.elapsed().as_secs_f64() * 1e6;
        self.dispatch_us.push_back(waited);
        if self.dispatch_us.len() > 10_000 {
            self.dispatch_us.pop_front();
        }
        let remaining = items.len();
        // Overlapping calls on one process share its CPU counters, so a delta
        // is only exact when the call runs alone.
        let start = if self.running.iter().any(|r| r.exec == exec) { None } else { accounting::read(pid) };
        self.running.push(Running { exec, items, remaining, cpus, blocked: false, start, dispatched_ns: now_ns() });
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
                if let Some(r) =
                    self.running.iter_mut().find(|r| r.exec == exec && r.items.iter().any(|w| Some(w.tk) == task))
                    && r.blocked != msg.blocked
                {
                    r.blocked = msg.blocked;
                    // A task waiting on other futures lends out its CPU so the
                    // work it waits for can run (it keeps its memory charge).
                    if msg.blocked {
                        self.cpu_used -= r.cpus;
                    } else {
                        self.cpu_used += r.cpus;
                    }
                }
            }
            ExecEvent::Log { exec, log } => self.forward_log(exec, log),
            ExecEvent::Exited { exec, detail, signal } => self.on_exec_exit(exec, detail, signal),
        }
    }

    fn forward_log(&self, exec: u32, log: pb::ExecLog) {
        let run = log.task.as_ref().and_then(Tk::parse).map(|t| t.run);
        let Some(run) = run else { return };
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        let ev = pb::SessionEvent {
            seq: 0,
            event: Some(pb::session_event::Event::Log(pb::LogEvent {
                task: log.task.clone(),
                stream: log.stream.clone(),
                text: log.text.clone(),
                pid,
            })),
        };
        if let Some(r) = self.runs.get(&run) {
            for s in &r.sessions {
                if let Some(sess) = self.sessions.get(s) {
                    let _ = sess.tx.send(ev.clone());
                }
            }
        }
    }

    fn on_exec_done(&mut self, exec: u32, done: pb::ExecDone) {
        let Some(tk) = done.task.as_ref().and_then(Tk::parse) else { return };
        let pid = self.execs.get(&exec).map_or(0, |e| e.pid);
        let Some(idx) = self
            .running
            .iter()
            .position(|r| r.exec == exec && r.items.iter().any(|w| w.tk == tk && w.attempt == done.attempt))
        else {
            return;
        };
        let run = &mut self.running[idx];
        let work = run.items.iter().find(|w| w.tk == tk && w.attempt == done.attempt).cloned().expect("found above");
        run.remaining = run.remaining.saturating_sub(1);
        let finished = run.remaining == 0;
        let batch_len = run.items.len();
        let alone = self.running.iter().filter(|r| r.exec == exec).count() == 1;
        let end = if finished && alone { accounting::read(pid) } else { None };
        let start = self.running[idx].start;
        let dispatched_ns = self.running[idx].dispatched_ns;
        if finished {
            let r = self.running.remove(idx);
            if !r.blocked {
                self.cpu_used -= r.cpus;
            }
            if let Some(x) = self.execs.get_mut(&exec) {
                x.state = ExecState::Idle;
                x.idle_since = Instant::now();
            }
        }
        let cgroup = self.execs.get(&exec).and_then(|e| e.cgroup.clone());
        let iso = cgroup.as_deref().and_then(Isolation::read);
        self.record_attempt(&work, &done, exec, start, end, dispatched_ns, batch_len, iso);
        if cgroup.is_some() {
            // One attempt per isolated executor: retire it (its cgroup goes with it).
            self.retire_exec(exec);
        }
        self.learn(&work, &done);
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
            } else if let Some(outcome) = done.outcome {
                self.pending_publish.entry(tk.v()).or_default().push(pb::PublishEntry {
                    task: Some(tk.pb()),
                    attempt: done.attempt,
                    peer: Some(self.router.identity.pb()),
                    outcome: Some(outcome),
                });
            }
        }
        if let Some(actor) = self.execs.get(&exec).and_then(|e| e.actor) {
            self.actor_after_call(actor, &work, finished);
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
    ) {
        let mut measures = HashMap::new();
        let wall_s = done.exit_unix_ns.saturating_sub(done.entry_unix_ns) as f64 / 1e9;
        if let Some(u) = iso {
            // Charged to the attempt's own cgroup: scope "attempt".
            let m = |value: f64| pb::Measure { value, scope: "attempt".into(), method: "cgroup".into(), coverage: 1.0 };
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
            None => "unknown",
        };
        let rec = pb::AttemptRecord {
            task: Some(w.tk.pb()),
            attempt: w.attempt,
            peer: Some(self.router.identity.pb()),
            executor: exec,
            fn_name: w.spec.method.clone(),
            submitted_unix_ns: 0,
            ready_unix_ns: dispatched_ns.saturating_sub(w.ready_at.elapsed().as_nanos() as u64),
            dispatched_unix_ns: dispatched_ns,
            py_entry_unix_ns: done.entry_unix_ns,
            py_exit_unix_ns: done.exit_unix_ns,
            published_unix_ns: 0,
            granted: Some(w.policy.clone()),
            measures,
            end_reason: end_reason.into(),
            detail: String::new(),
            placement_reason: "local peer had a warm executor and free CPU".into(),
        };
        self.push_attempt(w.tk, rec);
    }

    fn push_attempt(&mut self, tk: Tk, rec: pb::AttemptRecord) {
        self.attempts.entry(tk).or_default().push(rec);
        self.attempt_order.push_back(tk);
        while self.attempt_order.len() > ATTEMPT_LOG_CAP {
            if let Some(old) = self.attempt_order.pop_front() {
                self.attempts.remove(&old);
            }
        }
    }

    /// Granularity controller (A1): learn per-function item time and size
    /// batches of fast map items toward `BATCH_TARGET_S`.
    fn learn(&mut self, w: &Work, done: &pb::ExecDone) {
        if w.kind() != pb::CallKind::CallMapItem {
            return;
        }
        let item_s = done.exit_unix_ns.saturating_sub(done.entry_unix_ns) as f64 / 1e9;
        let p = self.profiles.entry(w.spec.fn_digest.clone()).or_default();
        p.samples += 1;
        p.ewma_s = if p.samples == 1 { item_s } else { 0.8 * p.ewma_s + 0.2 * item_s };
        if p.samples < 3 {
            return;
        }
        let want = if p.ewma_s < BATCH_BELOW_S {
            ((BATCH_TARGET_S / p.ewma_s.max(1e-6)).ceil() as usize).clamp(1, MAX_BATCH)
        } else {
            1
        };
        // Move toward the target by doubling or halving, one step per sample.
        let next = if want > p.batch.max(1) { (p.batch.max(1) * 2).min(want) } else { want };
        if next != p.batch {
            let old = p.batch;
            p.batch = next;
            let ewma = p.ewma_s;
            self.decide(pb::Decision {
                at_unix_ns: now_ns(),
                controller: "granularity".into(),
                knob: "range_size".into(),
                scope: format!("fn {}", &hex(&w.spec.fn_digest)[..12]),
                old_value: old.max(1).to_string(),
                new_value: next.to_string(),
                outcome: "adopted".into(),
                evidence: format!(
                    "{} samples, ewma item time {:.3} ms",
                    self.profiles[&w.spec.fn_digest].samples,
                    ewma * 1e3
                ),
                reason: format!(
                    "items under {} ms are batched toward {} ms per range",
                    BATCH_BELOW_S * 1e3,
                    BATCH_TARGET_S * 1e3
                ),
            });
        }
    }

    fn decide(&mut self, d: pb::Decision) {
        self.decisions.push_back(d);
        while self.decisions.len() > 10_000 {
            self.decisions.pop_front();
        }
    }

    fn on_exec_exit(&mut self, exec: u32, detail: String, signal: Option<i32>) {
        let info = self.execs.remove(&exec);
        self.sup.forget(exec);
        if let Some(e) = &info {
            self.cpu_used -= e.held_cpus;
        }
        self.iso_waiting.remove(&exec);
        let mut oom = info.as_ref().and_then(|e| e.killed_for_memory);
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
        for r in lost {
            if !r.blocked {
                self.cpu_used -= r.cpus;
            }
            let actor_exec = info.as_ref().and_then(|e| e.actor).is_some();
            for w in r.items.iter().filter(|w| w.publish) {
                let reason = match (oom, signal) {
                    (Some((peak, limit)), _) => {
                        format!("killed for exceeding its memory limit: peak {peak} > limit {limit} bytes")
                    }
                    (None, Some(sig)) => format!("executor process died with signal {sig} ({detail})"),
                    (None, None) => format!("executor process exited ({detail})"),
                };
                if actor_exec && w.kind() == pb::CallKind::CallActorMethod {
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
        let Some(Some(rec)) = self.router.read(actor.vshard(), |kv| read::actor(kv, actor)) else { return };
        let spec = rec.spec.clone().unwrap_or_default();
        if matches!(rec.state(), pb::actor_record::State::Pending | pb::actor_record::State::Restarting) {
            let host = self.actors.entry(actor).or_default();
            if host.placing != Some(rec.generation) {
                host.placing = Some(rec.generation);
                let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
                match self.choose_actor_host(policy.cpus.max(0.0)) {
                    Some(target) => self.propose(
                        actor.vshard(),
                        pb::shard_cmd::Body::ActorPlace(pb::ActorPlace {
                            actor: actor.to_vec(),
                            expected_generation: rec.generation,
                            host: Some(target),
                        }),
                    ),
                    None => self.propose(
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
                        }),
                    ),
                }
            }
            return;
        }
        let me = self.router.identity.pb();
        let host = rec.host.clone().or_else(|| self.actors_last_host(actor));
        let dead = rec.state() == pb::actor_record::State::Dead;
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
    fn choose_actor_host(&self, cpus: f64) -> Option<pb::PeerInc> {
        let me = self.router.identity.pb();
        let Some(mesh) = &self.router.mesh else { return Some(me) };
        let hosted = self.execs.values().filter(|e| e.actor.is_some()).count() as f64;
        let mine = (self.cpu_used + self.queue.len() as f64 + 0.25 * hosted) / self.capacity.max(1e-9);
        let mut best = (!self.draining && self.capacity + 1e-9 >= cpus).then_some((mine, me));
        let others: Vec<_> = mesh.live_others().into_iter().filter(|p| p.member.cpus + 1e-9 >= cpus).collect();
        let seed = now_ns() as usize;
        for i in 0..2.min(others.len()) {
            let p = &others[(seed + i * 7919) % others.len()];
            let actors = p.load.as_ref().map_or(0, |l| l.actors) as f64;
            let score = p.pressure() + 0.25 * actors / p.member.cpus.max(1e-9);
            if best.as_ref().is_none_or(|(b, _)| score < *b) {
                best = Some((score, pb::PeerInc { peer: p.member.peer.clone(), inc: p.member.incarnation }));
            }
        }
        if let Some((_, p)) = &best
            && let Some(v) = mesh.member_by_peer(&p.peer)
        {
            mesh.note_placed(v.member.node_id);
        }
        best.map(|(_, p)| p)
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
        host.run = run;
        match rec.state() {
            pb::actor_record::State::Pending | pb::actor_record::State::Restarting => {}
            pb::actor_record::State::Alive => {
                let mine = rec.host.as_ref().is_some_and(|h| {
                    h.peer == self.router.identity.peer.to_vec() && h.inc == self.router.identity.incarnation
                });
                if mine && rec.generation > host.generation {
                    host.generation = rec.generation;
                    host.constructed = false;
                    host.dead = false;
                    let env = run.and_then(|r| self.env_of(&r));
                    let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
                    let threads = if policy.cpus > 0.0 { policy.cpus.ceil() as u32 } else { 1 };
                    if let Some(env) = env {
                        let key = env.key(threads);
                        let exec = self.spawn_exec(&env, threads, &key, Some(actor));
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
                        }
                    }
                }
            }
            pb::actor_record::State::Dead => {
                host.dead = true;
                // Its process ends now; the exit releases the CPU it held.
                if let Some(exec) = host.exec.take() {
                    self.sup.kill(exec);
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

    fn close_actor(&mut self, actor: Id, reason: &str) {
        let Some(host) = self.actors.get(&actor) else { return };
        if host.dead {
            return;
        }
        let generation = host.generation;
        self.propose(
            actor.vshard(),
            pb::shard_cmd::Body::ActorDied(pb::ActorDied {
                actor: actor.to_vec(),
                generation,
                reason: Some(pb::ErrorRecord {
                    kind: Kind::ActorDied as i32,
                    message: format!("actor closed: {reason}"),
                    ..Default::default()
                }),
                terminal: true,
            }),
        );
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
        if w.kind() == pb::CallKind::CallActorCtor {
            host.ctor = Some(w);
        } else {
            host.mailbox.insert((w.spec.handle, w.spec.seq), w);
        }
        self.actor_pump(actor);
    }

    fn actor_exec_ready(&mut self, actor: Id, _exec: u32) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
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
            let w = Work {
                tk,
                attempt: 0,
                spec,
                policy,
                deps,
                cpus: 0.0,
                ready_at: Instant::now(),
                publish: false,
                start_consumed: true,
            };
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
        loop {
            let Some(exec) = self.actors.get(&actor).and_then(|h| h.exec) else { return };
            let Some(e) = self.execs.get(&exec) else { return };
            let inflight = self.running.iter().filter(|r| r.exec == exec).count();
            if e.state == ExecState::Starting || inflight >= e.concurrency {
                return;
            }
            // The constructor runs alone, before any method.
            let constructing = self.actors.get(&actor).is_some_and(|h| !h.constructed);
            if constructing && inflight > 0 {
                return;
            }
            if !self.actor_start_next(actor, exec) {
                return;
            }
        }
    }

    fn actor_start_next(&mut self, actor: Id, exec: u32) -> bool {
        let Some(host) = self.actors.get_mut(&actor) else { return false };
        let next = if let Some(ctor) = host.ctor.take() {
            Some(ctor)
        } else if host.constructed {
            let key = host.mailbox.keys().find(|(h, s)| host.next_seq.get(h).copied().unwrap_or(0) == *s).copied();
            key.and_then(|k| host.mailbox.remove(&k)).inspect(|w| {
                host.next_seq.insert(w.spec.handle, w.spec.seq + 1);
                host.inflight.insert((w.spec.handle, w.spec.seq));
                advance(host, w.spec.handle);
            })
        } else {
            None
        };
        match next {
            Some(w) => {
                self.start(exec, vec![w]);
                true
            }
            None => false,
        }
    }

    fn actor_after_call(&mut self, actor: Id, w: &Work, finished: bool) {
        if let Some(host) = self.actors.get_mut(&actor) {
            if w.kind() == pb::CallKind::CallActorCtor {
                host.constructed = true;
            } else {
                host.inflight.remove(&(w.spec.handle, w.spec.seq));
            }
        }
        if finished {
            self.actor_pump(actor);
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
            }),
        );
    }

    // ---- housekeeping -----------------------------------------------------

    fn housekeeping(&mut self) {
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
        // Memory limits: kill an attempt's executor when it exceeds its cap.
        let mut kill = Vec::new();
        for r in &self.running {
            let limit = r.items[0].policy.memory_limit;
            if limit == 0 || kill.iter().any(|(e, _, _)| *e == r.exec) {
                continue;
            }
            if let Some(e) = self.execs.get(&r.exec)
                && let Some(reading) = accounting::read(e.pid)
                && reading.rss_bytes > limit
            {
                kill.push((r.exec, reading.peak_rss_bytes.max(reading.rss_bytes), limit));
            }
        }
        for (exec, peak, limit) in kill {
            if let Some(e) = self.execs.get_mut(&exec) {
                e.killed_for_memory = Some((peak, limit));
            }
            self.sup.kill(exec);
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
    }

    // ---- queries ----------------------------------------------------------

    fn explain(&self, req: pb::ExplainRequest) -> pb::ExplainReply {
        let mut reply = pb::ExplainReply::default();
        if let Ok(run) = Id::from_slice(&req.run) {
            reply.run = self.router.read(run.vshard(), |kv| read::run(kv, run)).flatten();
            if req.task.is_none() {
                let tasks =
                    self.router.read(run.vshard(), |kv| read::tasks_of_run(kv, run, 1_000_000)).unwrap_or_default();
                for t in tasks {
                    let key = match (t.state(), t.outcome.as_ref().and_then(|o| o.value.as_ref())) {
                        (pb::task_record::State::Done, Some(pb::outcome::Value::Ok(_))) => "succeeded".to_string(),
                        (pb::task_record::State::Done, Some(pb::outcome::Value::Err(e))) => {
                            format!("failed:{:?}", e.kind()).to_lowercase()
                        }
                        (s, _) => format!("{s:?}").to_lowercase(),
                    };
                    *reply.counts.entry(key).or_default() += 1;
                }
            }
        }
        if let Some(tk) = req.task.as_ref().and_then(Tk::parse) {
            let rec = self.router.read(tk.v(), |kv| read::task(kv, tk)).flatten();
            reply.attempts = self.attempts.get(&tk).cloned().unwrap_or_default();
            reply.pending_reason = self.pending_reason(tk, rec.as_ref());
            reply.task = rec;
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
            pb::task_record::State::Pending => format!("waiting on {} prerequisite(s)", rec.deps_remaining),
            pb::task_record::State::Ready => "ready; waiting to be granted".into(),
            pb::task_record::State::Running => {
                if let Some(pos) = self.queue.iter().position(|w| w.tk == tk) {
                    let w = &self.queue[pos];
                    format!(
                        "queued at position {pos}: needs {} CPU, {:.2} of {:.2} in use",
                        w.cpus, self.cpu_used, self.capacity
                    )
                } else if self.running.iter().any(|r| r.items.iter().any(|w| w.tk == tk)) {
                    "running".into()
                } else {
                    "granted; waiting for an actor or executor".into()
                }
            }
        }
    }

    fn status(&self) -> pb::StatusReply {
        let busy = self.execs.values().filter(|e| e.state == ExecState::Busy).count() as u32;
        let mut reasons = Vec::new();
        if let Some(w) = self.queue.front() {
            reasons.push(format!(
                "{} attempt(s) queued; head needs {} CPU with {:.2} of {:.2} in use",
                self.queue.len(),
                w.cpus,
                self.cpu_used,
                self.capacity
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
            dispatch_p50_us: percentile(&self.dispatch_us, 0.50),
            dispatch_p99_us: percentile(&self.dispatch_us, 0.99),
            dispatch_samples: self.dispatch_us.len() as u64,
            // Batched publishes and grants not yet flushed count too.
            proposals_in_flight: self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
                + self.pending_publish.values().map(|v| v.len() as u64).sum::<u64>()
                + self.pending_grants.values().map(|v| v.len() as u64).sum::<u64>(),
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
