//! The client-facing `Peer` service.
//!
//! Submissions are validated against what this peer can ever offer (an
//! impossible request is rejected with the reason instead of pending forever),
//! grouped by vshard and proposed in order. Every submitted task is watched for
//! its session, so outcomes stream back without a separate request.

use crate::auth::Caller;
use crate::core::CoreMsg;
use crate::executor::ExecEnv;
use crate::router::Router;
use crate::stores::{CodeStore, ObjectStore};
use chimaera_core::ids::{Id, VSHARDS};
use chimaera_core::pb;
use chimaera_core::shard::{Tk, read};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

/// How long explain() waits for an actor's host to say why it waits.
const ACTOR_WAIT_ASK: std::time::Duration = std::time::Duration::from_millis(500);

/// Make an object's file readable here, downloading it from object storage
/// if it was spilled there: whether it is here now. A failed download is
/// logged and reads as absent, so the caller looks elsewhere.
pub async fn restore(objects: &ObjectStore, run: &[u8], object: &[u8]) -> bool {
    let (objects, run, object) = (objects.clone(), run.to_vec(), object.to_vec());
    match tokio::task::spawn_blocking(move || objects.ensure_local(&run, &object)).await {
        Ok(Ok(here)) => here,
        Ok(Err(e)) => {
            tracing::warn!("cannot restore an object from the spill store: {e}");
            false
        }
        Err(_) => false,
    }
}

pub struct Ingress {
    pub core: mpsc::Sender<CoreMsg>,
    pub router: Arc<Router>,
    pub code: CodeStore,
    pub objects: ObjectStore,
    pub info: pb::PeerInfo,
    pub shutdown: Arc<Notify>,
    next_session: AtomicU64,
    isolated_available: bool,
    /// Each run's and actor's tenant, as read from its group: neither changes.
    /// Keyed by (id, is an actor): a run and an actor may share an id.
    owners: std::sync::Mutex<HashMap<(Id, bool), String>>,
}

/// Owners kept before the cache starts over.
const OWNERS_CACHED: usize = 100_000;
/// Vshards a run's calls are spread over, its own included: each in another
/// data group, led by another peer where leaders are known. One group leader
/// commits roughly 10,000 calls a second; a large run's nested calls and
/// stages are shared by this many. `CHIMAERA_RUN_SPREAD` sets it (1: every
/// run stays in one vshard).
fn spread_limit() -> usize {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("CHIMAERA_RUN_SPREAD").ok().and_then(|v| v.parse().ok()).unwrap_or(16).clamp(1, 64)
    })
}
/// How long creating a run waits for each copy; a group that does not answer
/// is left out of the spread.
const COPY_PATIENCE: std::time::Duration = std::time::Duration::from_secs(5);

impl Ingress {
    pub fn new(
        core: mpsc::Sender<CoreMsg>,
        router: Arc<Router>,
        code: CodeStore,
        objects: ObjectStore,
        info: pb::PeerInfo,
        shutdown: Arc<Notify>,
        isolated_available: bool,
    ) -> Ingress {
        Ingress {
            core,
            router,
            code,
            objects,
            info,
            shutdown,
            next_session: AtomicU64::new(1),
            isolated_available,
            owners: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// What every live peer's object store holds, this one's first: each
    /// objplane::inventory as JSON, with the peer's address (or the error
    /// that peer gave).
    async fn inventories(&self, largest: u32) -> Vec<String> {
        let dir = self.objects.dir().to_path_buf();
        let keep = crate::objplane::listed(largest);
        let mut own =
            tokio::task::spawn_blocking(move || crate::objplane::inventory(&dir, keep)).await.unwrap_or_default();
        own["addr"] = serde_json::json!("this peer");
        let mut out = vec![own.to_string()];
        let Some(mesh) = &self.router.mesh else { return out };
        let mut asks = tokio::task::JoinSet::new();
        for p in mesh.members().into_iter().filter(|p| !p.dead && p.member.node_id != mesh.me.node_id) {
            let mesh = mesh.clone();
            asks.spawn(async move {
                let got = mesh.inventory(p.member.node_id, largest).await;
                let mut v = match got {
                    Ok(json) => serde_json::from_str(&json).unwrap_or_default(),
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };
                v["addr"] = serde_json::json!(p.member.addr);
                v.to_string()
            });
        }
        while let Some(r) = asks.join_next().await {
            out.extend(r.ok());
        }
        out
    }

    /// The largest shape any live peer offers: (cpus, memory, gpus, peers).
    fn largest(&self) -> (f64, u64, usize, usize) {
        let Some(mesh) = &self.router.mesh else {
            return (self.info.usable_cores, self.info.memory_bytes, self.info.gpus.len(), 1);
        };
        let live: Vec<_> = mesh.members().into_iter().filter(|p| !p.dead).collect();
        let cpus = live.iter().map(|p| p.member.cpus).fold(0.0, f64::max);
        let memory = live.iter().map(|p| p.member.memory_bytes).max().unwrap_or(0);
        let gpus = live.iter().map(|p| p.member.gpus.len()).max().unwrap_or(0);
        (cpus, memory, gpus, live.len())
    }

    /// Why a policy can never run on any live peer, or `None` if it can.
    fn infeasible(&self, p: &pb::Policy) -> Option<String> {
        if !p.require.is_empty() {
            let labelled: Vec<std::collections::HashMap<String, String>> = match &self.router.mesh {
                Some(mesh) => mesh.members().into_iter().filter(|m| !m.dead).map(|m| m.member.labels).collect(),
                None => vec![crate::labels::own(&self.info.peer).into_iter().collect()],
            };
            if !labelled.iter().any(|l| crate::labels::satisfies(l, &p.require)) {
                let mut want: Vec<String> = p.require.iter().map(|(k, v)| format!("{k}={v}")).collect();
                want.sort();
                let offered: Vec<String> = labelled
                    .iter()
                    .map(|l| {
                        let mut kv: Vec<String> =
                            p.require.keys().map(|k| format!("{k}={}", l.get(k).map_or("-", |v| v))).collect();
                        kv.sort();
                        kv.join(",")
                    })
                    .collect();
                return Some(format!(
                    "infeasible: no live peer has labels {} (the {} live peer(s) offer: {})",
                    want.join(","),
                    labelled.len(),
                    offered.join("; ")
                ));
            }
        }
        let (cpus, memory, gpus, peers) = self.largest();
        if p.cpus > cpus + 1e-9 {
            return Some(format!(
                "infeasible: requested cpus={} but the largest peer offers {} (requested shape cpus={}, memory={}; {} candidate peer(s))",
                p.cpus, cpus, p.cpus, p.memory, peers
            ));
        }
        if p.memory > memory {
            return Some(format!(
                "infeasible: requested memory={} bytes but the largest peer offers {} bytes",
                p.memory, memory
            ));
        }
        if p.accounting() == pb::policy::Accounting::Isolated && !self.isolated_available {
            return Some(
                "infeasible: accounting='isolated' needs cgroup v2 delegation and no peer has it (macOS and \
                 undelegated containers report process-scoped measurements instead)"
                    .to_string(),
            );
        }
        let wants_gpu = p.gpus > 0.0 || !p.device.is_empty();
        if wants_gpu && (gpus as f64) < p.gpus.max(1.0) {
            return Some(format!(
                "infeasible: requested {} accelerator(s) (device={:?}) but the largest peer has {} ({} candidate peer(s))",
                p.gpus.max(1.0),
                p.device,
                gpus,
                peers
            ));
        }
        None
    }

    /// Propose a submission, waiting out a group that is busy or electing:
    /// task ids are deterministic, so a repeat after an ambiguous failure
    /// lands on the same tasks. The session submits one batch at a time, so
    /// this wait is the client's backpressure; only a group that stays
    /// unavailable fails the calls.
    async fn propose_patiently(
        &self,
        v: u16,
        body: pb::shard_cmd::Body,
    ) -> Result<pb::ShardReply, chimaera_raft::ProposeError> {
        let started = std::time::Instant::now();
        loop {
            // One attempt may itself wait (connects to a dead leader, retries
            // inside the router): bound it by what is left of the patience.
            let left = SUBMIT_PATIENCE.saturating_sub(started.elapsed()).max(std::time::Duration::from_secs(1));
            let attempt = match tokio::time::timeout(left, self.router.propose(v, body.clone())).await {
                Ok(r) => r,
                Err(_) => Err(chimaera_raft::ProposeError::Unavailable {
                    group: self.router.group_of(v),
                    reason: format!("no answer within {}s", SUBMIT_PATIENCE.as_secs()),
                }),
            };
            // No leader known: an election, or no majority to hold one. Wait as
            // for an unavailable group, and report a lost majority the same way.
            let attempt = attempt.map_err(|e| match e {
                chimaera_raft::ProposeError::NotLeader { group, leader: None } => {
                    chimaera_raft::ProposeError::Unavailable { group, reason: "no leader".into() }
                }
                e => e,
            });
            match attempt {
                Err(chimaera_raft::ProposeError::Unavailable { .. }) if started.elapsed() < SUBMIT_PATIENCE => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                other => return other,
            }
        }
    }

    /// The call's inputs its client stored here (argument file, blobs, ch.put
    /// values) name this peer as their holder. Without it the group leader
    /// placing the call sees every peer, this one included, missing them and
    /// places by load alone, and other peers fetching them try every member.
    fn stamp_holder(&self, spec: &mut pb::TaskSpec) {
        let me = self.router.identity.pb();
        for o in spec.args.iter_mut().chain(spec.blobs.iter_mut()) {
            if o.inline_data.is_empty()
                && !o.object_id.is_empty()
                && o.locations.is_empty()
                && self.objects.path(&o.run, &o.object_id).exists()
            {
                o.locations.push(me.clone());
            }
        }
    }

    async fn submit(&self, s: pb::SubmitTasks) -> pb::TaskAcks {
        let mut acks = Vec::with_capacity(s.tasks.len());
        // Keyed by (vshard, stream): generator calls run on this peer, where
        // their consumer reads the items.
        let mut by_vshard: BTreeMap<(u16, bool), Vec<pb::TaskSpec>> = BTreeMap::new();
        let mut order = Vec::new();
        let cluster = self.router.is_cluster();
        for mut spec in s.tasks {
            if cluster {
                self.stamp_holder(&mut spec);
            }
            let Some(tk) = spec.key.as_ref().and_then(Tk::parse) else {
                acks.push(pb::TaskAck {
                    status: pb::task_ack::Status::Rejected as i32,
                    reason: "malformed task key".into(),
                    ..Default::default()
                });
                continue;
            };
            let checked = crate::policy_check::decode(&spec.policy);
            if let Some(why) = checked.as_ref().map_or_else(|e| Some(e.clone()), |p| self.infeasible(p)) {
                acks.push(pb::TaskAck {
                    task: Some(tk.pb()),
                    status: pb::task_ack::Status::Rejected as i32,
                    reason: why,
                    ..Default::default()
                });
                continue;
            }
            order.push(tk);
            by_vshard.entry((tk.v(), spec.stream)).or_default().push(spec);
        }
        let mut results: BTreeMap<Tk, pb::TaskAck> = BTreeMap::new();
        // A cluster of one (or one whose other members are all gone) has
        // nowhere else to place work: grant it here in the submission's own
        // commit, as a local peer does, instead of a placement round and a
        // second commit per batch. Only calls that may run on any peer: the
        // others are only thought gone (a peer whose core answers a ping late
        // reports itself draining), and an actor's calls must reach its host.
        let alone = self.router.mesh.as_ref().is_some_and(|m| !m.has_live_others());
        // Local first: plain calls go to this peer's idle CPUs in the
        // submission's own commit (nested calls whose parent waits here, a
        // driver's burst on its own machine). The rest are placed by the
        // group leader, which weighs inputs, labels and devices.
        let mut split: BTreeMap<(u16, bool), Vec<pb::TaskSpec>> = BTreeMap::new();
        if self.router.is_cluster() {
            for ((v, stream), tasks) in by_vshard {
                if stream {
                    split.entry((v, true)).or_default().extend(tasks);
                    continue;
                }
                let (here, rest): (Vec<_>, Vec<_>) = if alone {
                    tasks.into_iter().partition(runs_anywhere)
                } else {
                    let (mut plain, other): (Vec<_>, Vec<_>) = tasks.into_iter().partition(local_first_candidate);
                    let mut rest = plain.split_off(self.router.take_local_room(plain.len()));
                    rest.extend(other);
                    (plain, rest)
                };
                if !here.is_empty() {
                    split.entry((v, true)).or_default().extend(here);
                }
                if !rest.is_empty() {
                    split.entry((v, false)).or_default().extend(rest);
                }
            }
        } else {
            split = by_vshard;
        }
        for ((v, here), tasks) in split {
            // A local peer grants to itself in the same commit; in a cluster
            // the group's leader places the rest.
            let grant_to = (!self.router.is_cluster() || here).then(|| self.router.identity.pb());
            let batch: Vec<Tk> = tasks.iter().filter_map(|t| t.key.as_ref().and_then(Tk::parse)).collect();
            let body = pb::shard_cmd::Body::SubmitTasks(pb::SubmitTasks { tasks, grant_to });
            match self.propose_patiently(v, body).await {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Tasks(t)) }) => {
                    for a in t.acks {
                        if let Some(tk) = a.task.as_ref().and_then(Tk::parse) {
                            results.insert(tk, a);
                        }
                    }
                }
                other => {
                    let why = match other {
                        Ok(r) => format!("submission rejected: {r:?}"),
                        // Still no writable leader after SUBMIT_PATIENCE: the group has
                        // lost its majority. The client raises ch.QuorumLost on this prefix.
                        Err(chimaera_raft::ProposeError::Unavailable { group, reason }) => format!(
                            "quorum lost: group {group} accepted no writes for {} s ({reason}); \
                             its calls wait until a majority of its peers is back",
                            SUBMIT_PATIENCE.as_secs()
                        ),
                        Err(e) => format!("submission not accepted: {e}"),
                    };
                    for tk in &batch {
                        results.insert(
                            *tk,
                            pb::TaskAck {
                                task: Some(tk.pb()),
                                status: pb::task_ack::Status::Rejected as i32,
                                reason: why.clone(),
                                ..Default::default()
                            },
                        );
                    }
                }
            }
        }
        acks.extend(order.iter().filter_map(|tk| results.remove(tk)));
        pb::TaskAcks { acks }
    }
}

/// Whether a call may be granted to the submitting peer at submission: a
/// call made inside a task (its parent waits on this peer) with no
/// dependencies, that may run on any peer. Everything else, a driver's calls
/// included (gang ranks that wait for each other must spread), is placed by
/// the group leader.
fn local_first_candidate(spec: &pb::TaskSpec) -> bool {
    spec.parent.is_some() && spec.deps.is_empty() && runs_anywhere(spec)
}

/// Whether a call may run on whichever peer takes it: a plain function call
/// or map item with no placement constraint, no devices and at most one CPU.
/// An actor's calls must reach its host, which its group's leader looks up.
fn runs_anywhere(spec: &pb::TaskSpec) -> bool {
    if !matches!(spec.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem) {
        return false;
    }
    // One that does not decode was refused at submit; never place it here.
    let Ok(policy) = pb::Policy::decode(spec.policy.as_slice()) else { return false };
    policy.gpus == 0.0 && policy.device.is_empty() && policy.require.is_empty() && policy.cpus <= 1.0
}

/// How long a submission waits for its group before the calls fail.
const SUBMIT_PATIENCE: std::time::Duration = std::time::Duration::from_secs(60);

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::SessionEvent, Status>> + Send>>;

fn ev(seq: u64, event: pb::session_event::Event) -> pb::SessionEvent {
    pb::SessionEvent { seq, event: Some(event), ..Default::default() }
}

/// The gRPC service: a handle to the shared ingress state.
#[derive(Clone)]
pub struct Service(pub Arc<Ingress>);

/// The vshards holding a run's calls, ascending (task ids sort the same way).
fn spread_list(rec: &pb::RunRecord, run: Id) -> Vec<u16> {
    let mut v: Vec<u16> = rec.spread.iter().map(|v| *v as u16).collect();
    if v.is_empty() {
        v.push(run.vshard());
    }
    v.sort_unstable();
    v.dedup();
    v
}

#[tonic::async_trait]
impl pb::peer_server::Peer for Service {
    async fn hello(&self, req: Request<pb::ClientHello>) -> Result<Response<pb::PeerInfo>, Status> {
        let h = req.into_inner();
        if h.wire_version == 0 || h.wire_version > chimaera_proto::WIRE_VERSION {
            return Err(Status::failed_precondition(format!(
                "client speaks wire version {}; this peer speaks {}",
                h.wire_version,
                chimaera_proto::WIRE_VERSION
            )));
        }
        Ok(Response::new(self.0.info.clone()))
    }

    async fn create_run(&self, req: Request<pb::CreateRunRequest>) -> Result<Response<pb::RunRecord>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        let tenant = match (caller.tenant(), r.tenant.as_str()) {
            (Some(bound), "") => bound.to_string(),
            (Some(bound), asked) if asked != bound => {
                return Err(Status::permission_denied(format!(
                    "this credential is for tenant {bound}; the run asks for tenant {asked}"
                )));
            }
            (_, "") => "default".to_string(),
            (_, asked) => asked.to_string(),
        };
        if !crate::auth::valid_tenant(&tenant) {
            return Err(Status::invalid_argument(format!(
                "tenant {tenant:?} must be 1-64 letters, digits, '-' or '_'"
            )));
        }
        let key = if r.submission_key.is_empty() {
            format!("{}-{}", crate::router::now_ns(), self.0.next_session.fetch_add(1, Ordering::Relaxed))
        } else {
            r.submission_key.clone()
        };
        let h = blake3::hash(format!("{tenant}\0{key}").as_bytes());
        let mut vshard = u16::from_le_bytes([h.as_bytes()[0], h.as_bytes()[1]]) % VSHARDS;
        // A run without a caller-chosen key goes to a group this peer leads,
        // so its watches and placement stay local. (A keyed run must map to
        // the same vshard from any peer, for idempotent resubmission.)
        let leading: Vec<u32> = self.0.router.leading().into_iter().filter(|g| *g != 0).collect();
        if r.submission_key.is_empty() && self.0.router.is_cluster() && !leading.is_empty() {
            let group = leading[h.as_bytes()[2] as usize % leading.len()];
            let owned = self.0.router.vshards_of(group);
            vshard = owned[(vshard as usize) % owned.len()] as u16;
        }
        let run = Id::derive(0, vshard, &[tenant.as_bytes(), key.as_bytes()]);
        // A keyed run must be the same record from any peer: it stays in one vshard.
        let spread = if r.submission_key.is_empty() { self.0.spread_for(vshard) } else { Vec::new() };
        let mut record = pb::RunRecord {
            run: run.to_vec(),
            tenant,
            submission_key: key,
            detached: r.detached,
            client_grace_ms: r.client_grace_ms,
            code_digest: r.code_digest.clone(),
            env_digest: r.env_digest.clone(),
            description: r.description.clone(),
            exec_env: Some(pb::ExecEnvSpec {
                python: r.python.clone(),
                cwd: r.cwd.clone(),
                sys_path: r.sys_path.clone(),
                env: r.env.clone(),
            }),
            home: Some(self.0.router.identity.pb()),
            spread: spread.iter().map(|v| u32::from(*v)).collect(),
            ..Default::default()
        };
        // The copies first, so no call is routed to a vshard that would refuse it.
        if !spread.is_empty() {
            record.spread = self.0.create_copies(&record, vshard, &spread).await;
        }
        // Patient like a submission: a leader election is not a lost majority.
        let reply = self
            .0
            .propose_patiently(vshard, pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(record) }))
            .await
            .map_err(|e| match e {
                // The client raises ch.QuorumLost on this prefix.
                chimaera_raft::ProposeError::Unavailable { group, reason } => Status::unavailable(format!(
                    "quorum lost: group {group} accepted no writes for {} s ({reason}); \
                     runs can start again once a majority of its peers is back",
                    SUBMIT_PATIENCE.as_secs()
                )),
                e => Status::unavailable(e.to_string()),
            })?;
        let Some(pb::shard_reply::Body::Run(rec)) = reply.body else {
            return Err(Status::internal(format!("unexpected reply: {reply:?}")));
        };
        let env = ExecEnv { python: r.python, cwd: r.cwd, sys_path: r.sys_path, env: r.env.into_iter().collect() };
        let run_id = Id::from_slice(&rec.run).map_err(|e| Status::internal(e.to_string()))?;
        self.0.owners.lock().expect("lock").insert((run_id, false), rec.tenant.clone());
        let _ = self.0.core.send(CoreMsg::RegisterRun { run: run_id, env, detached: rec.detached }).await;
        Ok(Response::new(rec))
    }

    async fn put_code(&self, req: Request<pb::PutCodeRequest>) -> Result<Response<pb::PutCodeReply>, Status> {
        let r = req.into_inner();
        self.0.code.put(&r.digest, &r.code).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if let Some(mesh) = &self.0.router.mesh {
            mesh.spread_code(r.digest, r.code);
        }
        Ok(Response::new(pb::PutCodeReply { stored: true }))
    }

    type SessionStream = EventStream;

    async fn session(&self, req: Request<Streaming<pb::ClientMsg>>) -> Result<Response<EventStream>, Status> {
        let caller = Caller::of(&req);
        let mut inbound = req.into_inner();
        let id = self.0.next_session.fetch_add(1, Ordering::Relaxed);
        // Bounded (mailbox.rs): replies below wait for room, so a client that
        // stops reading stops being read.
        let tx = crate::mailbox::Mailbox::new();
        let _ = tx.offer(ev(0, pb::session_event::Event::Opened(id)), crate::mailbox::Kind::Notice);
        let tenant = caller.tenant().map(str::to_string);
        self.0
            .core
            .send(CoreMsg::OpenSession { id, mailbox: tx.clone(), tenant })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        let this = self.0.clone();
        let out = crate::mailbox::stream(tx.clone(), {
            let core = self.0.core.clone();
            move || {
                let core = core.clone();
                async move {
                    let _ = core.send(CoreMsg::SessionRoom { id }).await;
                }
            }
        });
        tokio::spawn(async move {
            // A resume of this session on another stream ends this one, though it is still open here.
            let superseded = tx.superseded();
            tokio::pin!(superseded);
            while let Some(Ok(msg)) = tokio::select! { m = inbound.next() => m, () = &mut superseded => None } {
                let seq = msg.seq;
                if let Err(e) = this.may_send(&caller, msg.msg.as_ref()).await {
                    tx.send(ev(seq, pb::session_event::Event::Error(e.message().to_string()))).await;
                    tx.took(seq);
                    continue;
                }
                match msg.msg {
                    Some(pb::client_msg::Msg::Submit(mut s)) => {
                        let inflight = crate::direct::inflight(&mut s);
                        if !inflight.is_empty() {
                            let _ = this.core.send(CoreMsg::DirectInflight(inflight)).await;
                        }
                        let mut ran = crate::direct::attempts(&s, &this.router.identity.pb());
                        // A direct call's record: its caller has the outcome, so the
                        // session is not sent it again (one event and one read a call).
                        let recorded: BTreeSet<Tk> = ran.iter().map(|r| r.tk).collect();
                        let acks = this.submit(s).await;
                        let accepted: Vec<Tk> = acks
                            .acks
                            .iter()
                            .filter(|a| a.status() != pb::task_ack::Status::Rejected)
                            .filter_map(|a| a.task.as_ref().and_then(Tk::parse))
                            .filter(|t| !recorded.contains(t))
                            .collect();
                        if !ran.is_empty() {
                            // A resubmission of one already recorded is a Duplicate: no second attempt.
                            let fresh: BTreeSet<Tk> = acks
                                .acks
                                .iter()
                                .filter(|a| a.status() == pb::task_ack::Status::Accepted)
                                .filter_map(|a| a.task.as_ref().and_then(Tk::parse))
                                .collect();
                            ran.retain(|r| fresh.contains(&r.tk));
                            let _ = this.core.send(CoreMsg::DirectAttempts(ran)).await;
                        }
                        tx.send(ev(seq, pb::session_event::Event::Acks(acks))).await;
                        let _ = this.core.send(CoreMsg::Watch { session: id, tasks: accepted }).await;
                    }
                    Some(pb::client_msg::Msg::Watch(w)) => {
                        let tasks = w.tasks.iter().filter_map(Tk::parse).collect();
                        let _ = this.core.send(CoreMsg::Watch { session: id, tasks }).await;
                    }
                    Some(pb::client_msg::Msg::ReleaseObjects(r)) => {
                        let msg = CoreMsg::ReleaseObjects { session: id, run: r.run, objects: r.object_ids };
                        let _ = this.core.send(msg).await;
                    }
                    Some(pb::client_msg::Msg::DiscardFailed(r)) => {
                        let mut by_v: BTreeMap<u16, Vec<pb::TaskKey>> = BTreeMap::new();
                        for t in r.tasks {
                            if let Some(tk) = Tk::parse(&t) {
                                by_v.entry(tk.v()).or_default().push(t);
                            }
                        }
                        for (v, tasks) in by_v {
                            let p = this.router.clone();
                            tokio::spawn(async move {
                                p.propose(v, pb::shard_cmd::Body::DiscardDead(pb::Release { tasks })).await
                            });
                        }
                    }
                    Some(pb::client_msg::Msg::Release(r)) => {
                        let mut by_v: BTreeMap<u16, Vec<pb::TaskKey>> = BTreeMap::new();
                        for t in r.tasks {
                            if let Some(tk) = Tk::parse(&t) {
                                by_v.entry(tk.v()).or_default().push(t);
                            }
                        }
                        for (v, tasks) in by_v {
                            let p = this.router.clone();
                            tokio::spawn(async move {
                                p.propose(v, pb::shard_cmd::Body::Release(pb::Release { tasks })).await
                            });
                        }
                    }
                    Some(pb::client_msg::Msg::StreamCredit(c)) => {
                        if let Some(task) = c.task.as_ref().and_then(Tk::parse) {
                            let _ = this.core.send(CoreMsg::StreamCredit { task, up_to: c.up_to }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::Cancel(c)) => {
                        if let Some(task) = c.task.as_ref().and_then(Tk::parse) {
                            let _ = this.core.send(CoreMsg::Cancel { task, force: c.force }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::ActorCreate(a)) => {
                        let reply = this.create_actor(a).await;
                        tx.send(ev(seq, reply)).await;
                    }
                    Some(pb::client_msg::Msg::AttachRun(run)) => {
                        if let Ok(run) = Id::from_slice(&run) {
                            let _ = this.core.send(CoreMsg::BindRun { session: id, run }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::CloseActor(actor)) => {
                        if let Ok(actor) = Id::from_slice(&actor) {
                            let reason = "closed by its owner".to_string();
                            let _ = this.core.send(CoreMsg::CloseActor { actor, reason }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::FinishRun(true)) => {
                        let _ = this.core.send(CoreMsg::FinishSessionRun { session: id }).await;
                    }
                    Some(pb::client_msg::Msg::EndRun(e)) => {
                        if let Ok(run) = Id::from_slice(&e.run) {
                            let _ = this.core.send(CoreMsg::EndRun { run, cancel: e.cancel }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::AckCursor(cursor)) => tx.ack(cursor),
                    Some(pb::client_msg::Msg::Resume(r)) => {
                        let msg = CoreMsg::ResumeSession { id, seq, old: r.session, cursor: r.cursor };
                        let _ = this.core.send(msg).await;
                        // Not counted as taken: the requests the client sends
                        // again after it have lower seqs (mailbox.rs).
                        continue;
                    }
                    _ => {}
                }
                tx.took(seq);
            }
            let _ = this.core.send(CoreMsg::CloseSession { id }).await;
        });
        Ok(Response::new(Box::pin(out)))
    }

    async fn get_task(&self, req: Request<pb::TaskKey>) -> Result<Response<pb::TaskRecord>, Status> {
        let caller = Caller::of(&req);
        let tk = Tk::parse(&req.into_inner()).ok_or_else(|| Status::invalid_argument("malformed task key"))?;
        self.0.may_run(&caller, &tk.run.0).await?;
        // A replica here may still be catching up (a voter just added): only
        // the group's leader may say there is no such task.
        let rec = match self.0.router.read(tk.v(), |kv| read::task(kv, tk)).flatten() {
            Some(rec) => Some(rec),
            None => {
                let req = pb::ReadRequest { tasks: vec![tk.pb()], linearizable: true, ..Default::default() };
                let local = self.0.router.hosted(self.0.router.group_of(tk.v())).is_some();
                match self.0.remote_read(tk.v(), req).await {
                    Ok(mut reply) => reply.tasks.pop().filter(|r| r.spec.is_some()),
                    Err(_) if local => None, // no other replica answered: what this one has
                    Err(e) => return Err(e),
                }
            }
        };
        rec.map(Response::new).ok_or_else(|| Status::not_found("unknown task"))
    }

    async fn list_tasks(&self, req: Request<pb::ListTasksRequest>) -> Result<Response<pb::ListTasksReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        let run = Id::from_slice(&r.run).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let after = if r.after_task.is_empty() {
            None
        } else {
            Some(Id::from_slice(&r.after_task).map_err(|e| Status::invalid_argument(e.to_string()))?)
        };
        if r.failed {
            let mut reply = pb::ListTasksReply::default();
            for v in self.0.spread_of(run).await? {
                for (tk, d) in self.0.dead_letters(run, v).await? {
                    reply.failed_keys.push(tk.pb());
                    reply.failed.push(d);
                }
            }
            return Ok(Response::new(reply));
        }
        let limit = (r.limit as usize).clamp(1, 10_000);
        // Task ids lead with their vshard, so the run's vshards in order, each
        // in id order, list its tasks in id order.
        let spread = self.0.spread_of(run).await?;
        let mut cursor = after.unwrap_or_else(|| read::page_start(run, spread[0]));
        let (mut page, first) = (Vec::new(), cursor.vshard());
        for &v in spread.iter().filter(|v| **v >= first) {
            if v != cursor.vshard() {
                cursor = read::page_start(run, v);
            }
            page.extend(self.0.tasks_page(run, cursor, limit - page.len()).await?);
            if page.len() == limit {
                break;
            }
        }
        let next_after = if page.len() == limit {
            page.last().map(|(tk, _)| tk.task.to_vec()).unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut reply = pb::ListTasksReply { next_after, ..Default::default() };
        for (tk, rec) in page {
            reply.states.push(rec.state);
            reply.tasks.push(pb::OutcomeEvent { task: Some(tk.pb()), outcome: rec.outcome });
        }
        Ok(Response::new(reply))
    }

    async fn list_runs(&self, req: Request<pb::ListRunsRequest>) -> Result<Response<pb::ListRunsReply>, Status> {
        let caller = Caller::of(&req);
        let limit = req.into_inner().limit;
        let router = &self.0.router;
        let mut reply = pb::ListRunsReply::default();
        for group in router.data_groups() {
            // From the leader, which has applied every commit; a replica here
            // that may lag is the fallback when no leader answers.
            let runs = match &router.mesh {
                Some(mesh) if !router.leads(group) => {
                    let req = pb::ReadRequest { list_runs_group: group, linearizable: true, ..Default::default() };
                    mesh.read_group(group, req).await.ok().map(|r| r.runs).or_else(|| router.runs_of(group))
                }
                _ => router.runs_of(group),
            };
            match runs {
                Some(runs) => reply.runs.extend(runs.into_iter().filter(|r| caller.may(&r.tenant))),
                None => reply.unavailable.push(group),
            }
        }
        reply.runs.sort_by(|a, b| b.created_hlc.cmp(&a.created_hlc));
        if limit > 0 {
            reply.runs.truncate(limit as usize);
        }
        Ok(Response::new(reply))
    }

    async fn list_actors(&self, req: Request<pb::ListActorsRequest>) -> Result<Response<pb::ListActorsReply>, Status> {
        let caller = Caller::of(&req);
        let prefix = req.into_inner().prefix;
        let router = &self.0.router;
        let mut reply = pb::ListActorsReply::default();
        for group in router.data_groups() {
            let actors = match &router.mesh {
                Some(mesh) if !router.leads(group) => {
                    let req = pb::ReadRequest { list_actors_group: group, linearizable: true, ..Default::default() };
                    mesh.read_group(group, req).await.ok().map(|r| r.actors).or_else(|| router.actors_of(group))
                }
                _ => router.actors_of(group),
            };
            let Some(actors) = actors else {
                reply.unavailable.push(group);
                continue;
            };
            reply.actors.extend(actors.into_iter().filter(|a| {
                a.state() != pb::actor_record::State::Dead
                    && a.spec
                        .as_ref()
                        .is_some_and(|s| !s.key.is_empty() && s.key.starts_with(&prefix) && caller.may(&s.tenant))
            }));
        }
        let name = |a: &pb::ActorRecord| a.spec.as_ref().map(|s| (s.tenant.clone(), s.key.clone())).unwrap_or_default();
        reply.actors.sort_by_key(name);
        Ok(Response::new(reply))
    }

    async fn get_actor(&self, req: Request<pb::ActorQuery>) -> Result<Response<pb::ActorRecord>, Status> {
        let caller = Caller::of(&req);
        let id = Id::from_slice(&req.into_inner().actor).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if caller.tenant().is_some() && self.0.may_actor(&caller, &id.0).await.is_err() {
            return Err(Status::not_found("unknown actor"));
        }
        let rec = match self.0.router.read(id.vshard(), |kv| read::actor(kv, id)) {
            Some(rec) => rec,
            None => {
                self.0
                    .remote_read(id.vshard(), pb::ReadRequest { actor: id.to_vec(), ..Default::default() })
                    .await?
                    .actor
            }
        };
        rec.map(Response::new).ok_or_else(|| Status::not_found("unknown actor"))
    }

    async fn explain(&self, req: Request<pb::ExplainRequest>) -> Result<Response<pb::ExplainReply>, Status> {
        let caller = Caller::of(&req);
        let req = req.into_inner();
        self.0.may_run(&caller, &req.run).await?;
        if let Some(task) = &req.task {
            self.0.may_run(&caller, &task.run).await?;
        }
        // Decisions are the peer's, about every tenant's functions.
        if req.decisions && caller != Caller::Admin {
            return Err(Status::permission_denied(
                "the peer's decisions cover every tenant: they take the cluster token",
            ));
        }
        let (reply, rx) = oneshot::channel();
        self.0
            .core
            .send(CoreMsg::Explain { req: req.clone(), reply })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        let mut out = rx.await.map_err(|_| Status::unavailable("peer is stopping"))?;
        // A call to an actor placed on another peer waits on what that peer
        // knows (the actor may not have started there): ask it.
        if let Some(note) = self.0.actor_wait_elsewhere(&out).await {
            out.pending_reason += &note;
        }
        // The core reads only the groups this peer hosts: read another group's run from a replica of it.
        if let Ok(run) = Id::from_slice(&req.run) {
            if out.run.is_none() && !self.0.router.hosts(run.vshard()) {
                let read = pb::ReadRequest { run: run.to_vec(), linearizable: true, ..Default::default() };
                out.run = self.0.remote_read(run.vshard(), read).await?.run;
            }
            if let Some(rec) = out.run.as_ref().filter(|_| req.task.is_none()) {
                for v in spread_list(rec, run) {
                    let mut cursor = read::page_start(run, v);
                    loop {
                        let page = self.0.tasks_page(run, cursor, 10_000).await?;
                        for (_, t) in &page {
                            *out.counts.entry(crate::core::count_key(t)).or_default() += 1;
                        }
                        match page.last() {
                            Some((tk, _)) if page.len() == 10_000 => cursor = tk.task,
                            _ => break,
                        }
                    }
                }
            }
        }
        // The run's environment stays on the peers that run its calls.
        if let Some(run) = &mut out.run {
            run.exec_env = None;
        }
        Ok(Response::new(out))
    }

    async fn status(&self, req: Request<pb::StatusRequest>) -> Result<Response<pb::StatusReply>, Status> {
        let largest = req.get_ref().largest; // with `objects`: how many of each store's objects to list
        let caller = Caller::of(&req);
        let pb::StatusRequest { objects: want_objects, fleet, .. } = req.into_inner();
        if want_objects && caller != Caller::Admin {
            return Err(Status::permission_denied(
                "every peer's object store lists every tenant's runs: it takes the cluster token",
            ));
        }
        let (reply, rx) = oneshot::channel();
        self.0.core.send(CoreMsg::Status { reply }).await.map_err(|_| Status::unavailable("peer is stopping"))?;
        let mut s = rx.await.map_err(|_| Status::unavailable("peer is stopping"))?;
        s.peer = Some(self.0.info.clone());
        if fleet {
            crate::counters::FLEET_SUMMARIES_SERVED.add(1);
            return Ok(Response::new(crate::fleet::answer(s)));
        }
        if want_objects {
            s.objects = self.0.inventories(largest).await;
        }
        Ok(Response::new(s))
    }

    async fn localize(&self, req: Request<pb::LocalizeRequest>) -> Result<Response<pb::LocalizeReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        if restore(&self.0.objects, &r.run, &r.object_id).await {
            return Ok(Response::new(pb::LocalizeReply {}));
        }
        let Some(mesh) = &self.0.router.mesh else {
            return Err(Status::not_found("object not in this peer's store"));
        };
        let Err(e) = mesh.localize(&self.0.objects, &r.run, &r.object_id, 0, &[], r.traffic()).await else {
            return Ok(Response::new(pb::LocalizeReply {}));
        };
        // Every copy is lost. A call's result can be made again by running
        // the call (lineage); anything else fails here with the reason.
        let Some(producer) = r.task.as_ref().and_then(Tk::parse) else {
            return Err(Status::not_found(format!("object not found on any live peer: {e}")));
        };
        let (attempt, spec, deps) = crate::core::producer_plan(&self.0.router, mesh, producer, &r.object_id)
            .await
            .map_err(|why| Status::not_found(format!("every copy is lost, and it cannot be made again: {why}")))?;
        let (reply, rx) = oneshot::channel();
        let msg = CoreMsg::RebuildRead { producer, attempt, spec: Box::new(spec), deps, reply };
        self.0.core.send(msg).await.map_err(|_| Status::unavailable("peer is stopping"))?;
        match rx.await {
            // Remade here; a large value may have gone straight to a spill store.
            Ok(Ok(())) if restore(&self.0.objects, &r.run, &r.object_id).await => {
                Ok(Response::new(pb::LocalizeReply {}))
            }
            Ok(Ok(())) => Err(Status::not_found("every copy is lost, and running its call again did not remake it")),
            Ok(Err(why)) => {
                Err(Status::not_found(format!("every copy is lost, and running its call again failed: {why}")))
            }
            Err(_) => Err(Status::unavailable("peer is stopping")),
        }
    }

    /// A retained ch.put: copy its files to other peers (replicas::retain)
    /// and answer once they are made.
    async fn retain(&self, req: Request<pb::RetainRequest>) -> Result<Response<pb::RetainReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        let Some(mesh) = &self.0.router.mesh else {
            return Err(Status::failed_precondition(
                "durability='retained' keeps a copy on another peer, and this peer runs alone",
            ));
        };
        let copies = crate::objplane::replica_count().max(1);
        crate::replicas::retain(mesh, &self.0.objects, &r.run, &r.object_ids, copies)
            .await
            .map_err(|why| Status::aborted(format!("durability='retained': no copy was made: {why}")))?;
        Ok(Response::new(pb::RetainReply {}))
    }

    /// A session holds (or lets go of) stored files of another run (leases).
    async fn borrow(&self, req: Request<pb::BorrowRequest>) -> Result<Response<pb::BorrowReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        let tenant = caller.tenant().map(str::to_string);
        crate::leases::borrow(&self.0.core, &self.0.router, tenant, r).await?;
        Ok(Response::new(pb::BorrowReply {}))
    }

    /// Hold resources on this peer's ledger (core/holds.rs), or on another's
    /// through the mesh. Owners are named under their tenant.
    async fn hold(&self, req: Request<pb::HoldRequest>) -> Result<Response<pb::HoldReply>, Status> {
        let caller = Caller::of(&req);
        let mut r = req.into_inner();
        let tenant_of = |owner: &str| owner.split('/').next().unwrap_or_default().to_string();
        if !r.owner.is_empty() && !caller.may(&tenant_of(&r.owner)) {
            return Err(Status::permission_denied(format!("this credential may not hold for {}", r.owner)));
        }
        let gone = || Status::unavailable("peer is stopping");
        let mut reply = match &self.0.router.mesh {
            Some(mesh) if !r.peer.is_empty() && r.peer != self.0.router.identity.peer.to_vec() => {
                let node = mesh.member_by_peer(&r.peer).filter(|p| !p.dead).map(|p| p.member.node_id);
                let node = node.ok_or_else(|| Status::unavailable("that peer is not live"))?;
                r.peer.clear();
                mesh.hold(node, r).await.map_err(|e| Status::unavailable(format!("holding on another peer: {e}")))?
            }
            _ => {
                let (reply, rx) = oneshot::channel();
                self.0.core.send(CoreMsg::Hold { req: r, reply }).await.map_err(|_| gone())?;
                rx.await.map_err(|_| gone())?
            }
        };
        reply.holds.retain(|line| caller.may(&tenant_of(line)));
        Ok(Response::new(reply))
    }

    async fn has_blobs(&self, req: Request<pb::HasBlobsRequest>) -> Result<Response<pb::HasBlobsReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        let missing = r.blob_ids.into_iter().filter(|b| !self.0.objects.path(&r.run, b).exists()).collect();
        Ok(Response::new(pb::HasBlobsReply { missing }))
    }

    async fn put_blob(&self, req: Request<Streaming<pb::PutBlobChunk>>) -> Result<Response<pb::PutBlobReply>, Status> {
        let caller = Caller::of(&req);
        let mut chunks = req.into_inner();
        let first = chunks.next().await.ok_or_else(|| Status::invalid_argument("empty upload"))??;
        let (run, blob_id, size, keep_local) = (first.run, first.blob_id, first.size, first.keep_local);
        self.0.may_run(&caller, &run).await?;
        if blob_id.len() != 16 || run.is_empty() {
            return Err(Status::invalid_argument("the first chunk names the run and the 16-byte blob id"));
        }
        if size > self.0.info.memory_bytes.max(1 << 30) {
            return Err(Status::resource_exhausted(format!("blob of {size} bytes is larger than this peer's memory")));
        }
        let mut data = first.data;
        data.reserve(size.saturating_sub(data.len() as u64) as usize);
        while let Some(c) = chunks.next().await {
            data.extend_from_slice(&c?.data);
            if data.len() as u64 > size {
                return Err(Status::invalid_argument(format!("upload is longer than the declared {size} bytes")));
            }
        }
        if data.len() as u64 != size {
            return Err(Status::invalid_argument(format!("upload ended at {} of {size} bytes", data.len())));
        }
        if keep_local {
            crate::replicas::keep_local(&blob_id); // no background copy (see replicas)
        }
        let objects = self.0.objects.clone();
        tokio::task::spawn_blocking(move || objects.put_blob(&run, &blob_id, &data))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(pb::PutBlobReply {}))
    }

    /// A value under the id it already has (another cell's ch.put value, sent with a
    /// delegated call): checked against its checksum, bounded like a blob, and
    /// refused when the id already holds different bytes.
    async fn put_object(
        &self,
        req: Request<Streaming<pb::PutObjectChunk>>,
    ) -> Result<Response<pb::PutObjectReply>, Status> {
        let caller = Caller::of(&req);
        let mut chunks = req.into_inner();
        let first = chunks.next().await.ok_or_else(|| Status::invalid_argument("empty upload"))??;
        let (run, id, size, checksum) = (first.run, first.object_id, first.size, first.checksum);
        self.0.may_run(&caller, &run).await?;
        if id.len() != 16 || run.is_empty() || checksum.len() != 16 {
            return Err(Status::invalid_argument("the first chunk names the run, the 16-byte id and its checksum"));
        }
        if size > self.0.info.memory_bytes.max(1 << 30) {
            return Err(Status::resource_exhausted(format!("value of {size} bytes is larger than this peer's memory")));
        }
        let mut data = first.data;
        data.reserve(size.saturating_sub(data.len() as u64) as usize);
        while let Some(c) = chunks.next().await {
            data.extend_from_slice(&c?.data);
            if data.len() as u64 > size {
                return Err(Status::invalid_argument(format!("upload is longer than the declared {size} bytes")));
            }
        }
        if data.len() as u64 != size {
            return Err(Status::invalid_argument(format!("upload ended at {} of {size} bytes", data.len())));
        }
        let path = self.0.objects.path(&run, &id);
        tokio::task::spawn_blocking(move || {
            let existing = std::fs::read(&path).ok();
            if object_upload(&data, &checksum, existing.as_deref())? {
                std::fs::create_dir_all(path.parent().expect("object paths have a run directory"))
                    .and_then(|()| crate::stores::write_atomic(&path, &data))
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            Ok::<_, Status>(())
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))??;
        Ok(Response::new(pb::PutObjectReply {}))
    }

    type GetObjectStream = Pin<Box<dyn Stream<Item = Result<pb::GetObjectChunk, Status>> + Send>>;

    async fn get_object(&self, req: Request<pb::GetObjectRequest>) -> Result<Response<Self::GetObjectStream>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        restore(&self.0.objects, &r.run, &r.object_id).await; // spilled to object storage
        let path = self.0.objects.path(&r.run, &r.object_id);
        let chunks = crate::stores::read_chunks(&path, r.offset, r.len, 1 << 20).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Status::not_found(format!("object not in this peer's store: {e}")),
            _ => Status::invalid_argument(e.to_string()),
        })?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(chunks).map(|c| {
            c.map(|c| pb::GetObjectChunk { data: c.data, total: c.total, checksum: c.checksum })
                .map_err(|e| Status::internal(e.to_string()))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    /// ch.Store: a write or a linearizable read, applied through the
    /// namespace's group. A CHANGES read with `wait_ms` waits for a change
    /// (a watch's long poll): it watches this peer's replica of the namespace
    /// cheaply and re-reads through the log once the revision moves.
    async fn events(&self, req: Request<pb::EventsRequest>) -> Result<Response<pb::EventsReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        let wait = r.wait_ms > 0;
        let (reply, rx) = oneshot::channel();
        self.0
            .core
            .send(CoreMsg::Events { kinds: r.kinds, since: r.since, wait, reply })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        let events = if wait {
            let limit = std::time::Duration::from_millis(u64::from(r.wait_ms.min(60_000)));
            tokio::time::timeout(limit, rx).await.ok().and_then(|x| x.ok()).unwrap_or_default()
        } else {
            rx.await.unwrap_or_default()
        };
        let next = events.last().map_or(r.since, |e| e.seq + 1);
        // A tenant sees membership and drains, and its own runs' and actors'
        // events. Those come from the groups this peer leads, so the check
        // reads them here; decisions (about every tenant's functions) are left out.
        let events = match caller.tenant() {
            None => events,
            Some(tenant) => {
                let owner = |id: &[u8], actor: bool| {
                    let id = Id::from_slice(id).ok()?;
                    self.0.router.read(id.vshard(), |kv| match actor {
                        true => read::actor(kv, id).and_then(|r| r.spec).map(|s| s.tenant),
                        false => read::run(kv, id).map(|r| r.tenant),
                    })?
                };
                events
                    .into_iter()
                    .filter(|e| match (e.kind.as_str(), e.run.is_empty(), e.actor.is_empty()) {
                        ("decision", ..) => false,
                        (_, true, true) => true,
                        (_, false, _) => owner(&e.run, false).as_deref() == Some(tenant),
                        (_, true, false) => owner(&e.actor, true).as_deref() == Some(tenant),
                    })
                    .collect()
            }
        };
        Ok(Response::new(pb::EventsReply {
            events,
            next,
            peer: self.0.info.peer.clone(),
            incarnation: self.0.info.incarnation,
        }))
    }

    /// A run's output from the calls this peer ran (crate::runlogs), from
    /// `offset`, at most 1 MiB; waits up to `wait_ms` for more.
    async fn logs(&self, req: Request<pb::LogsRequest>) -> Result<Response<pb::LogsReply>, Status> {
        let caller = Caller::of(&req);
        let r = req.into_inner();
        self.0.may_run(&caller, &r.run).await?;
        // Every peer sends a run's output to the run's home peer (logship):
        // read it there. A home peer that is gone leaves this peer's part.
        if let Some(mesh) = &self.0.router.mesh
            && let Ok(run) = Id::from_slice(&r.run)
        {
            // A replica here may lag (or be one this peer is leaving): without
            // the record here, ask the group.
            let rec = match self.0.router.read(run.vshard(), |kv| read::run(kv, run)).flatten() {
                Some(rec) => Some(rec),
                None => {
                    let req = pb::ReadRequest { run: run.to_vec(), linearizable: true, ..Default::default() };
                    self.0.remote_read(run.vshard(), req).await.ok().and_then(|r| r.run)
                }
            };
            let home = rec.and_then(|r| r.home).filter(|h| h.peer != self.0.router.identity.peer.to_vec());
            if let Some(node) =
                home.and_then(|h| mesh.member_by_peer(&h.peer)).filter(|p| !p.dead).map(|p| p.member.node_id)
                && let Ok(reply) = mesh.logs(node, r.clone()).await
            {
                return Ok(Response::new(reply));
            }
        }
        crate::runlogs::serve(&self.0.code, &r)
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(format!("reading run output: {e}")))
    }

    async fn store(&self, req: Request<pb::StoreRequest>) -> Result<Response<pb::StoreReply>, Status> {
        use chimaera_core::shard::store;
        crate::counters::STORE_REQUESTS.add(1);
        let caller = Caller::of(&req);
        let r = req.into_inner();
        let (tenant, namespace) = match (&r.write, &r.txn, &r.read, &r.claim) {
            (Some(w), _, _, _) => (w.tenant.clone(), w.namespace.clone()),
            (None, Some(t), _, _) => (t.tenant.clone(), t.namespace.clone()),
            (None, None, Some(rd), _) => (rd.tenant.clone(), rd.namespace.clone()),
            (None, None, None, Some(c)) => (c.tenant.clone(), c.namespace.clone()),
            (None, None, None, None) => {
                return Err(Status::invalid_argument(
                    "a store request needs a write, a transaction, a read or a claim",
                ));
            }
        };
        if namespace.is_empty() {
            return Err(Status::invalid_argument("a store namespace must not be empty"));
        }
        if !caller.may(&tenant) {
            return Err(Status::permission_denied(format!(
                "this credential is for tenant {}; the store request names tenant {tenant}",
                caller.tenant().unwrap_or_default()
            )));
        }
        let v = store::vshard_of(&tenant, &namespace);
        let propose = |body| async move {
            match self.0.router.propose(v, body).await {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Store(reply)) }) => Ok(reply),
                Ok(other) => Err(Status::unavailable(format!("store command rejected: {other:?}"))),
                Err(e) => Err(Status::unavailable(format!("store command not accepted: {e}"))),
            }
        };
        if let Some(w) = r.write {
            return propose(pb::shard_cmd::Body::StoreWrite(w)).await.map(Response::new);
        }
        if let Some(t) = r.txn {
            return propose(pb::shard_cmd::Body::StoreTxn(t)).await.map(Response::new);
        }
        let Some(read) = r.read else {
            return propose(pb::shard_cmd::Body::StoreClaim(r.claim.expect("checked above"))).await.map(Response::new);
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(r.wait_ms as u64);
        let watching = read.kind() == pb::store_read::Kind::Changes && r.wait_ms > 0;
        loop {
            let reply = propose(pb::shard_cmd::Body::StoreRead(read.clone())).await?;
            if !watching || !reply.changes.is_empty() || reply.compacted || tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(reply));
            }
            // Wait for the namespace's revision to pass what we just read.
            let probe = pb::StoreRead { kind: pb::store_read::Kind::Changes as i32, limit: 1, ..read.clone() };
            while tokio::time::Instant::now() < deadline {
                let hosted = self.0.router.read(v, |kv| store::read(kv, v, &probe, 0).revision);
                tokio::time::sleep(std::time::Duration::from_millis(if hosted.is_some() { 20 } else { 200 })).await;
                if hosted.is_none_or(|rev| rev > reply.revision) {
                    break;
                }
            }
        }
    }

    async fn shutdown(&self, req: Request<pb::ShutdownRequest>) -> Result<Response<pb::ShutdownReply>, Status> {
        if Caller::of(&req) != Caller::Admin {
            return Err(Status::permission_denied("stopping a peer takes the cluster token, not a driver credential"));
        }
        self.0.shutdown.notify_waiters();
        Ok(Response::new(pb::ShutdownReply {}))
    }
}

impl Ingress {
    /// The tenant owning run or actor `id` (`actor`: which), or None when there is none.
    async fn owner(&self, id: Id, actor: bool) -> Result<Option<String>, Status> {
        if let Some(t) = self.owners.lock().expect("lock").get(&(id, actor)) {
            return Ok(Some(t.clone()));
        }
        let local = |kv: &dyn chimaera_core::kv::KvRead| match actor {
            true => read::actor(kv, id).and_then(|r| r.spec).map(|s| s.tenant),
            false => read::run(kv, id).map(|r| r.tenant),
        };
        // A replica here may not have applied a record just created: only a
        // linearizable read may say that there is none.
        let group_id = self.router.group_of(id.vshard());
        let here = match self.router.hosted(group_id) {
            Some(group) => match group.read(local).map_err(|e| Status::internal(e.to_string()))? {
                Some(t) => Some(Some(t)),
                None if self.router.leads(group_id) => {
                    group.linearizable().await.map_err(|e| Status::unavailable(e.to_string()))?;
                    Some(group.read(local).map_err(|e| Status::internal(e.to_string()))?)
                }
                None => None, // a follower: ask the leader
            },
            None => None,
        };
        let tenant = match here {
            Some(t) => t,
            None => {
                let mut req = pb::ReadRequest { linearizable: true, ..Default::default() };
                if actor {
                    req.actor = id.to_vec();
                } else {
                    req.run = id.to_vec();
                }
                let reply = self.remote_read(id.vshard(), req).await?;
                if actor { reply.actor.and_then(|r| r.spec).map(|s| s.tenant) } else { reply.run.map(|r| r.tenant) }
            }
        };
        if let Some(t) = &tenant {
            let mut owners = self.owners.lock().expect("lock");
            if owners.len() >= OWNERS_CACHED {
                owners.clear();
            }
            owners.insert((id, actor), t.clone());
        }
        Ok(tenant)
    }

    /// Refuse a tenant-bound caller anything of a run that is not its
    /// tenant's, saying the same as for a run that does not exist.
    async fn may_run(&self, caller: &Caller, run: &[u8]) -> Result<(), Status> {
        let Some(tenant) = caller.tenant() else { return Ok(()) };
        let id = Id::from_slice(run).map_err(|_| Status::not_found("no such run"))?;
        match self.owner(id, false).await? {
            Some(owner) if owner == tenant => Ok(()),
            _ => Err(Status::not_found(format!("tenant {tenant} has no run {}", id.hex()))),
        }
    }

    /// As [`Ingress::may_run`], for an actor.
    async fn may_actor(&self, caller: &Caller, actor: &[u8]) -> Result<(), Status> {
        let Some(tenant) = caller.tenant() else { return Ok(()) };
        let id = Id::from_slice(actor).map_err(|_| Status::not_found("no such actor"))?;
        match self.owner(id, true).await? {
            Some(owner) if owner == tenant => Ok(()),
            _ => Err(Status::not_found(format!("tenant {tenant} has no actor {}", id.hex()))),
        }
    }

    /// Every run a submitted call names: its own, its parent's and its inputs'.
    async fn may_runs_of(&self, caller: &Caller, spec: &pb::TaskSpec) -> Result<(), Status> {
        if caller.tenant().is_none() {
            return Ok(());
        }
        let mut runs: BTreeSet<&[u8]> = BTreeSet::new();
        runs.extend(spec.key.iter().chain(&spec.parent).chain(&spec.deps).map(|k| k.run.as_slice()));
        runs.extend(spec.args.iter().chain(&spec.blobs).map(|o| o.run.as_slice()).filter(|r| !r.is_empty()));
        for run in runs {
            self.may_run(caller, run).await?;
        }
        Ok(())
    }

    /// Whether a tenant-bound caller may send session message `msg`: it may
    /// touch only its tenant's runs and actors.
    async fn may_send(&self, caller: &Caller, msg: Option<&pb::client_msg::Msg>) -> Result<(), Status> {
        use pb::client_msg::Msg;
        if caller.tenant().is_none() {
            return Ok(());
        }
        let tasks: Vec<&pb::TaskKey> = match msg {
            Some(Msg::Submit(s)) => {
                for spec in &s.tasks {
                    self.may_runs_of(caller, spec).await?;
                    if !spec.actor.is_empty() {
                        self.may_actor(caller, &spec.actor).await?;
                    }
                }
                return Ok(());
            }
            Some(Msg::Watch(w)) => w.tasks.iter().collect(),
            Some(Msg::Release(r) | Msg::DiscardFailed(r)) => r.tasks.iter().collect(),
            Some(Msg::Cancel(c)) => c.task.iter().collect(),
            Some(Msg::StreamCredit(c)) => c.task.iter().collect(),
            Some(Msg::ReleaseObjects(r)) => return self.may_run(caller, &r.run).await,
            Some(Msg::AttachRun(run)) => return self.may_run(caller, run).await,
            Some(Msg::EndRun(e)) => return self.may_run(caller, &e.run).await,
            Some(Msg::CloseActor(actor)) => return self.may_actor(caller, actor).await,
            Some(Msg::ActorCreate(a)) => {
                let spec = a.spec.as_ref().ok_or_else(|| Status::invalid_argument("actor create needs a spec"))?;
                if !caller.may(&spec.tenant) {
                    return Err(Status::permission_denied(format!(
                        "this credential is for tenant {}; the actor asks for tenant {}",
                        caller.tenant().unwrap_or_default(),
                        spec.tenant
                    )));
                }
                return match &a.ctor {
                    Some(ctor) => self.may_runs_of(caller, ctor).await,
                    None => Ok(()),
                };
            }
            // A resume checks the credential against the session's own (core.rs).
            Some(Msg::Heartbeat(_) | Msg::FinishRun(_) | Msg::AckCursor(_) | Msg::Resume(_)) | None => return Ok(()),
        };
        let runs: BTreeSet<&[u8]> = tasks.iter().map(|t| t.run.as_slice()).collect();
        for run in runs {
            self.may_run(caller, run).await?;
        }
        Ok(())
    }

    /// Vshards for a new run whose own is `home`: `home`, then one in each of
    /// up to spread_limit() - 1 other data groups, preferring groups led by distinct
    /// other peers. Empty (only `home`) on a single peer or when this peer
    /// leads every group.
    fn spread_for(&self, home: u16) -> Vec<u16> {
        let router = &self.router;
        if !router.is_cluster() {
            return Vec::new();
        }
        let home_group = router.group_of(home);
        let mut leaders = BTreeSet::new();
        let mut picked = vec![home];
        for g in router.data_groups() {
            if picked.len() >= spread_limit() {
                break;
            }
            if g == home_group || router.leads(g) {
                continue;
            }
            // A leader already chosen adds no committing capacity.
            if let Some(leader) = router.leader_hint(g)
                && (leader == router.node_id() || !leaders.insert(leader))
            {
                continue;
            }
            let owned = router.vshards_of(g);
            if let Some(v) = owned.get(home as usize % owned.len().max(1)) {
                picked.push(*v as u16);
            }
        }
        if picked.len() == 1 { Vec::new() } else { picked }
    }

    /// Create the run's copy in every vshard of `spread` but its own; the
    /// spread to record: those that took it.
    async fn create_copies(&self, record: &pb::RunRecord, home: u16, spread: &[u16]) -> Vec<u32> {
        let copies = spread.iter().filter(|v| **v != home).map(|&v| {
            let body = pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(record.clone()) });
            async move {
                let took = tokio::time::timeout(COPY_PATIENCE, self.router.propose(v, body)).await;
                let ok = matches!(took, Ok(Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Run(_)), .. })));
                ok.then_some(u32::from(v))
            }
        });
        let took = futures_util::future::join_all(copies).await;
        std::iter::once(u32::from(home)).chain(took.into_iter().flatten()).collect()
    }

    /// The vshards holding `run`'s calls, in id order.
    async fn spread_of(&self, run: Id) -> Result<Vec<u16>, Status> {
        let rec = match self.router.read(run.vshard(), |kv| read::run(kv, run)) {
            Some(rec) => rec,
            None => {
                let req = pb::ReadRequest { run: run.to_vec(), linearizable: true, ..Default::default() };
                self.remote_read(run.vshard(), req).await?.run
            }
        };
        Ok(rec.map_or_else(|| vec![run.vshard()], |r| spread_list(&r, run)))
    }

    /// One page of `run`'s tasks after `after` in `after`'s vshard (see
    /// `read::tasks_page`), from a replica here or the group's.
    async fn tasks_page(&self, run: Id, after: Id, limit: usize) -> Result<Vec<(Tk, pb::TaskRecord)>, Status> {
        if let Some(page) = self.router.read(after.vshard(), |kv| read::tasks_page(kv, run, Some(after), limit)) {
            return Ok(page);
        }
        let req = pb::ReadRequest {
            run: run.to_vec(),
            after_task: after.to_vec(),
            limit: limit as u32,
            linearizable: true,
            ..Default::default()
        };
        let reply = self.remote_read(after.vshard(), req).await?;
        Ok(reply.page_keys.iter().filter_map(Tk::parse).zip(reply.tasks).collect())
    }

    /// A run's dead letters in vshard `v`.
    async fn dead_letters(&self, run: Id, v: u16) -> Result<Vec<(Tk, pb::DeadLetter)>, Status> {
        if let Some(dead) = self.router.read(v, |kv| read::dead_letters(kv, v, run)) {
            return Ok(dead);
        }
        let req = pb::ReadRequest {
            run: run.to_vec(),
            failed: true,
            failed_vshard: v as u32,
            linearizable: true,
            ..Default::default()
        };
        let reply = self.remote_read(v, req).await?;
        Ok(reply.failed_keys.iter().filter_map(Tk::parse).zip(reply.failed).collect())
    }

    /// Why the actor a pending call goes to has not started on its host, when
    /// the host is another peer: its own reason, or that it did not answer in
    /// ACTOR_WAIT_ASK. None when the call is not to an actor, its actor is here
    /// (the core explained it), the core already named the actor's wait, or
    /// the actor is not waiting.
    async fn actor_wait_elsewhere(&self, out: &pb::ExplainReply) -> Option<String> {
        if out.pending_reason.is_empty() || out.pending_reason.contains("its actor") {
            return None;
        }
        let actor = Id::from_slice(&out.task.as_ref()?.spec.as_ref()?.actor).ok()?;
        let rec = match self.router.read(actor.vshard(), |kv| read::actor(kv, actor)) {
            Some(local) => local,
            None => {
                let req = pb::ReadRequest { actor: actor.to_vec(), ..Default::default() };
                self.remote_read(actor.vshard(), req).await.ok()?.actor
            }
        };
        let host = rec?.host?;
        if host.peer == self.router.identity.peer.to_vec() {
            return None;
        }
        let mesh = self.router.mesh.clone()?;
        let member = mesh.member_by_peer(&host.peer)?.member;
        match tokio::time::timeout(ACTOR_WAIT_ASK, mesh.actor_wait(member.node_id, &actor.to_vec())).await {
            Ok(Ok(reason)) if reason.is_empty() => None,
            Ok(Ok(reason)) => Some(format!("; its actor, on {}, has not started: {reason}", member.addr)),
            _ => Some(format!("; its actor's host ({}) did not answer", member.addr)),
        }
    }

    /// Read from a replica on another peer (this one does not host the group).
    async fn remote_read(&self, vshard: u16, req: pb::ReadRequest) -> Result<pb::ReadReply, Status> {
        let mesh = self.router.mesh.as_ref().ok_or_else(|| Status::internal("group not hosted on a local peer"))?;
        mesh.read_group(self.router.group_of(vshard), req).await.map_err(|e| Status::unavailable(e.to_string()))
    }

    async fn create_actor(&self, a: pb::ActorCreateMsg) -> pb::session_event::Event {
        let (Some(spec), Some(ctor)) = (a.spec, a.ctor) else {
            return pb::session_event::Event::Error("actor create needs a spec and a constructor call".into());
        };
        let policy = match crate::policy_check::decode(&ctor.policy) {
            Ok(p) => p,
            Err(why) => return pb::session_event::Event::Error(why),
        };
        if let Some(why) = self.infeasible(&policy) {
            return pb::session_event::Event::Error(why);
        }
        let Ok(actor) = Id::from_slice(&spec.actor) else {
            return pb::session_event::Event::Error("malformed actor id".into());
        };
        // The constructor builds this actor and is the one its record names
        // (a restart replays it); a named actor's id is its tenant's.
        if ctor.kind() != pb::CallKind::CallActorCtor
            || ctor.actor != spec.actor
            || spec.ctor.as_ref() != ctor.key.as_ref()
        {
            return pb::session_event::Event::Error(
                "an actor's constructor must be its own CALL_ACTOR_CTOR call".into(),
            );
        }
        // An actor's id is derived, never chosen: from its tenant and name, or
        // from its creating run and a nonce. Nobody can create one at an id
        // that is (or will be) another tenant's.
        let derived = if spec.key.is_empty() {
            let run = ctor.key.as_ref().and_then(|k| Id::from_slice(&k.run).ok());
            run.filter(|_| spec.nonce.len() == 16).map(|r| r.child(&[b"actor", &spec.nonce]))
        } else {
            Some(keyed_actor_id(&spec.tenant, &spec.key))
        };
        if derived != Some(actor) {
            return pb::session_event::Event::Error(
                "an actor's id must be derived from its tenant and name, or from its run and nonce".into(),
            );
        }
        let tenant = spec.tenant.clone();
        let upgrade = pb::ActorUpgrade {
            actor: actor.to_vec(),
            class_digest: spec.class_digest.clone(),
            state_schema: spec.state_schema,
            methods: spec.methods.clone(),
        };
        // Create (or find) the actor first: for keyed actors a second creator
        // gets the existing one and must not submit a second constructor.
        let created = self
            .router
            .propose(actor.vshard(), pb::shard_cmd::Body::ActorCreate(pb::ActorCreate { spec: Some(spec) }))
            .await;
        let ack = match created {
            Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Actor(ack)) }) => ack,
            Ok(other) => return pb::session_event::Event::Error(format!("actor create rejected: {other:?}")),
            Err(e) => return pb::session_event::Event::Error(e.to_string()),
        };
        if ack.record.as_ref().and_then(|r| r.spec.as_ref()).is_some_and(|s| s.tenant != tenant) {
            return pb::session_event::Event::Error(format!("tenant {tenant} has no actor {}", actor.hex()));
        }
        if ack.status() == pb::actor_ack::Status::Ok {
            let acks = self.submit(pb::SubmitTasks { tasks: vec![ctor], grant_to: None }).await;
            if let Some(a) = acks.acks.first()
                && a.status() == pb::task_ack::Status::Rejected
            {
                return pb::session_event::Event::Error(format!("constructor rejected: {}", a.reason));
            }
        }
        // A named durable actor found with an older state version is bound
        // to this creator's class (the shard ignores an older or equal one).
        let older = ack.record.as_ref().and_then(|r| r.spec.as_ref()).is_some_and(|s| {
            s.durable && s.state_schema.max(1) < upgrade.state_schema // 0: from before versions, version 1's
        });
        if ack.status() == pb::actor_ack::Status::Existing && older {
            match self.router.propose(actor.vshard(), pb::shard_cmd::Body::ActorUpgrade(upgrade)).await {
                Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Actor(a)) }) => {
                    if a.status() == pb::actor_ack::Status::Ok {
                        self.tell_host(&a).await;
                    }
                    return pb::session_event::Event::Actor(a);
                }
                Ok(other) => return pb::session_event::Event::Error(format!("actor upgrade rejected: {other:?}")),
                Err(e) => return pb::session_event::Event::Error(e.to_string()),
            }
        }
        pb::session_event::Event::Actor(ack)
    }

    /// Give an upgraded record to the actor's host before its creator hears
    /// of it, so the creator's next call cannot reach the instance of the
    /// older class first: that call comes to the host later, on this or
    /// another link. (Its group's leader also sends the record.)
    async fn tell_host(&self, ack: &pb::ActorAck) {
        let Some(rec) = ack.record.clone() else { return };
        let Some(host) = rec.host.clone() else { return }; // not running: it starts with the new class
        let mesh = self.router.mesh.as_ref().filter(|_| host.peer != self.router.identity.peer.to_vec());
        let Some(mesh) = mesh else {
            let _ = self.core.send(CoreMsg::Remote { work: Vec::new(), actors: vec![rec], actor_env: None }).await;
            return;
        };
        let Some(target) = mesh.member_by_peer(&host.peer) else { return };
        let req = pb::DispatchRequest { cluster: mesh.cluster.clone(), actors: vec![rec], ..Default::default() };
        if let Err(e) = mesh.dispatch(target.member.node_id, req).await {
            tracing::warn!("cannot give actor host {} its upgraded record: {e}", target.member.node_id);
        }
    }
}

/// The id of `tenant`'s actor named `key` (python/chimaera/_remote.py
/// `_keyed_actor_id`): named actors of two tenants never share an id.
fn keyed_actor_id(tenant: &str, key: &str) -> Id {
    let h = blake3::hash(format!("{tenant}\0{key}").as_bytes());
    let vshard = u16::from_le_bytes([h.as_bytes()[0], h.as_bytes()[1]]) % VSHARDS;
    Id::derive(0, vshard, &[b"actor", tenant.as_bytes(), key.as_bytes()])
}

/// Whether an uploaded value (PutObject) should be written: it must match its
/// checksum; an id that already holds the same bytes is a no-op (false), and one
/// that holds different bytes is refused.
fn object_upload(data: &[u8], checksum: &[u8], existing: Option<&[u8]>) -> Result<bool, Status> {
    let digest = |b: &[u8]| blake3::hash(b).as_bytes()[..16].to_vec();
    if digest(data) != checksum {
        return Err(Status::invalid_argument("value does not match its checksum"));
    }
    match existing {
        Some(old) if digest(old) == checksum => Ok(false),
        Some(_) => Err(Status::already_exists("that id already holds a different value")),
        None => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::{keyed_actor_id, local_first_candidate, object_upload, runs_anywhere};
    use chimaera_core::pb;
    use prost::Message;

    #[test]
    fn only_calls_that_may_run_on_any_peer_are_granted_where_they_were_submitted() {
        let call = |kind: pb::CallKind, policy: pb::Policy| pb::TaskSpec {
            kind: kind as i32,
            policy: policy.encode_to_vec(),
            ..Default::default()
        };
        let plain = pb::Policy::default;
        assert!(runs_anywhere(&call(pb::CallKind::CallFunction, plain())));
        assert!(runs_anywhere(&call(pb::CallKind::CallMapItem, plain())));
        // An actor's calls go to its host, whichever peer they were submitted to.
        assert!(!runs_anywhere(&call(pb::CallKind::CallActorCtor, plain())));
        assert!(!runs_anywhere(&call(pb::CallKind::CallActorMethod, plain())));
        let pinned = pb::Policy { require: [("slot".to_string(), "s1".to_string())].into(), ..plain() };
        assert!(!runs_anywhere(&call(pb::CallKind::CallFunction, pinned)));
        assert!(!runs_anywhere(&call(pb::CallKind::CallFunction, pb::Policy { gpus: 1.0, ..plain() })));
        assert!(!runs_anywhere(&call(pb::CallKind::CallFunction, pb::Policy { cpus: 2.0, ..plain() })));
        // Local first also needs a parent waiting here and no dependencies.
        let mut nested = call(pb::CallKind::CallFunction, plain());
        assert!(!local_first_candidate(&nested), "a driver's call is placed by the leader");
        nested.parent = Some(pb::TaskKey::default());
        assert!(local_first_candidate(&nested));
        nested.deps.push(pb::TaskKey::default());
        assert!(!local_first_candidate(&nested), "a call with prerequisites is not ready at submission");
    }

    #[test]
    fn a_named_actors_id_is_the_one_the_client_derives() {
        // python -c "from chimaera._remote import _keyed_actor_id; print(_keyed_actor_id('alice','counter').hex())"
        assert_eq!(keyed_actor_id("alice", "counter").hex(), "000000ed8f8e6f7dfdd825d528086d96");
        assert_ne!(keyed_actor_id("bob", "counter"), keyed_actor_id("alice", "counter"));
    }

    #[test]
    fn an_uploaded_value_must_match_its_checksum_and_never_replaces_another() {
        let sum = |b: &[u8]| blake3::hash(b).as_bytes()[..16].to_vec();
        let (v, w) = (b"value".as_slice(), b"other".as_slice());
        assert!(object_upload(v, &sum(v), None).unwrap(), "new: written");
        assert!(!object_upload(v, &sum(v), Some(v)).unwrap(), "the same bytes again: nothing to do");
        let e = object_upload(v, &sum(w), None).unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument, "bytes that do not match the checksum");
        let e = object_upload(v, &sum(v), Some(w)).unwrap_err();
        assert_eq!(e.code(), tonic::Code::AlreadyExists, "an id never changes value");
    }
}
