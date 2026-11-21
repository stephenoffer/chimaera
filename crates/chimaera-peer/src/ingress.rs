//! The client-facing `Peer` service.
//!
//! Submissions are validated against what this peer can ever offer (an
//! impossible request is rejected with the reason instead of pending forever),
//! grouped by vshard and proposed in order. Every submitted task is watched for
//! its session, so outcomes stream back without a separate request.

use crate::core::CoreMsg;
use crate::executor::ExecEnv;
use crate::router::Router;
use crate::stores::{CodeStore, ObjectStore};
use chimaera_core::ids::{Id, VSHARDS};
use chimaera_core::pb;
use chimaera_core::shard::{Tk, read};
use prost::Message;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub struct Ingress {
    pub core: mpsc::Sender<CoreMsg>,
    pub router: Arc<Router>,
    pub code: CodeStore,
    pub objects: ObjectStore,
    pub info: pb::PeerInfo,
    pub shutdown: Arc<Notify>,
    next_session: AtomicU64,
    isolated_available: bool,
}

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
        Ingress { core, router, code, objects, info, shutdown, next_session: AtomicU64::new(1), isolated_available }
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
        let wants_gpu = p.gpus > 0.0 || p.device.starts_with("cuda");
        if wants_gpu && (gpus as f64) < p.gpus.max(1.0) {
            return Some(format!(
                "infeasible: requested {} GPU(s) (device={:?}) but the largest peer has {} ({} candidate peer(s))",
                p.gpus.max(1.0),
                p.device,
                gpus,
                peers
            ));
        }
        None
    }

    async fn submit(&self, s: pb::SubmitTasks) -> pb::TaskAcks {
        let mut acks = Vec::with_capacity(s.tasks.len());
        let mut by_vshard: BTreeMap<u16, Vec<pb::TaskSpec>> = BTreeMap::new();
        let mut order = Vec::new();
        for spec in s.tasks {
            let Some(tk) = spec.key.as_ref().and_then(Tk::parse) else {
                acks.push(pb::TaskAck {
                    status: pb::task_ack::Status::Rejected as i32,
                    reason: "malformed task key".into(),
                    ..Default::default()
                });
                continue;
            };
            let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
            if let Some(why) = self.infeasible(&policy) {
                acks.push(pb::TaskAck {
                    task: Some(tk.pb()),
                    status: pb::task_ack::Status::Rejected as i32,
                    reason: why,
                    ..Default::default()
                });
                continue;
            }
            order.push(tk);
            by_vshard.entry(tk.v()).or_default().push(spec);
        }
        let mut results: BTreeMap<Tk, pb::TaskAck> = BTreeMap::new();
        for (v, tasks) in by_vshard {
            // A local peer grants to itself in the same commit; in a cluster
            // the group's leader places the work.
            let grant_to = (!self.router.is_cluster()).then(|| self.router.identity.pb());
            let body = pb::shard_cmd::Body::SubmitTasks(pb::SubmitTasks { tasks, grant_to });
            match self.router.propose(v, body).await {
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
                        Err(e) => format!("submission not accepted: {e}"),
                    };
                    for tk in order.iter().filter(|t| t.v() == v) {
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

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::SessionEvent, Status>> + Send>>;

fn ev(seq: u64, event: pb::session_event::Event) -> pb::SessionEvent {
    pb::SessionEvent { seq, event: Some(event) }
}

/// The gRPC service: a handle to the shared ingress state.
#[derive(Clone)]
pub struct Service(pub Arc<Ingress>);

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
        let r = req.into_inner();
        let tenant = if r.tenant.is_empty() { "default".to_string() } else { r.tenant.clone() };
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
        let record = pb::RunRecord {
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
            ..Default::default()
        };
        let reply = self
            .0
            .router
            .propose(vshard, pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(record) }))
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let Some(pb::shard_reply::Body::Run(rec)) = reply.body else {
            return Err(Status::internal(format!("unexpected reply: {reply:?}")));
        };
        let env = ExecEnv { python: r.python, cwd: r.cwd, sys_path: r.sys_path, env: r.env.into_iter().collect() };
        let run_id = Id::from_slice(&rec.run).map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.0.core.send(CoreMsg::RegisterRun { run: run_id, env, detached: rec.detached }).await;
        Ok(Response::new(rec))
    }

    async fn put_code(&self, req: Request<pb::PutCodeRequest>) -> Result<Response<pb::PutCodeReply>, Status> {
        let r = req.into_inner();
        self.0.code.put(&r.digest, &r.code).map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(pb::PutCodeReply { stored: true }))
    }

    type SessionStream = EventStream;

    async fn session(&self, req: Request<Streaming<pb::ClientMsg>>) -> Result<Response<EventStream>, Status> {
        let mut inbound = req.into_inner();
        let id = self.0.next_session.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel::<pb::SessionEvent>();
        self.0
            .core
            .send(CoreMsg::OpenSession { id, tx: tx.clone() })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        let this = self.0.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = inbound.next().await {
                let seq = msg.seq;
                match msg.msg {
                    Some(pb::client_msg::Msg::Submit(s)) => {
                        let acks = this.submit(s).await;
                        let accepted: Vec<Tk> = acks
                            .acks
                            .iter()
                            .filter(|a| a.status() != pb::task_ack::Status::Rejected)
                            .filter_map(|a| a.task.as_ref().and_then(Tk::parse))
                            .collect();
                        let _ = tx.send(ev(seq, pb::session_event::Event::Acks(acks)));
                        let _ = this.core.send(CoreMsg::Watch { session: id, tasks: accepted }).await;
                    }
                    Some(pb::client_msg::Msg::Watch(w)) => {
                        let tasks = w.tasks.iter().filter_map(Tk::parse).collect();
                        let _ = this.core.send(CoreMsg::Watch { session: id, tasks }).await;
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
                    Some(pb::client_msg::Msg::Cancel(c)) => {
                        if let Some(task) = c.task.as_ref().and_then(Tk::parse) {
                            let _ = this.core.send(CoreMsg::Cancel { task, force: c.force }).await;
                        }
                    }
                    Some(pb::client_msg::Msg::ActorCreate(a)) => {
                        let reply = this.create_actor(a).await;
                        let _ = tx.send(ev(seq, reply));
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
                    _ => {}
                }
            }
            let _ = this.core.send(CoreMsg::CloseSession { id }).await;
        });
        let out = tokio_stream::wrappers::UnboundedReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(out)))
    }

    async fn get_task(&self, req: Request<pb::TaskKey>) -> Result<Response<pb::TaskRecord>, Status> {
        let tk = Tk::parse(&req.into_inner()).ok_or_else(|| Status::invalid_argument("malformed task key"))?;
        let rec = match self.0.router.read(tk.v(), |kv| read::task(kv, tk)) {
            Some(rec) => rec,
            None => {
                let req = pb::ReadRequest { tasks: vec![tk.pb()], ..Default::default() };
                self.0.remote_read(tk.v(), req).await?.tasks.pop().filter(|r| r.spec.is_some())
            }
        };
        rec.map(Response::new).ok_or_else(|| Status::not_found("unknown task"))
    }

    async fn list_tasks(&self, req: Request<pb::ListTasksRequest>) -> Result<Response<pb::ListTasksReply>, Status> {
        let r = req.into_inner();
        let run = Id::from_slice(&r.run).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let after = if r.after_task.is_empty() {
            None
        } else {
            Some(Id::from_slice(&r.after_task).map_err(|e| Status::invalid_argument(e.to_string()))?)
        };
        let limit = (r.limit as usize).clamp(1, 10_000);
        let page = match self.0.router.read(run.vshard(), |kv| read::tasks_page(kv, run, after, limit)) {
            Some(page) => page,
            None => {
                let req = pb::ReadRequest {
                    run: run.to_vec(),
                    after_task: r.after_task.clone(),
                    limit: limit as u32,
                    linearizable: true,
                    ..Default::default()
                };
                let reply = self.0.remote_read(run.vshard(), req).await?;
                reply.page_keys.iter().filter_map(Tk::parse).zip(reply.tasks).collect()
            }
        };
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

    async fn get_actor(&self, req: Request<pb::ActorQuery>) -> Result<Response<pb::ActorRecord>, Status> {
        let id = Id::from_slice(&req.into_inner().actor).map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let (reply, rx) = oneshot::channel();
        self.0
            .core
            .send(CoreMsg::Explain { req: req.into_inner(), reply })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        Ok(Response::new(rx.await.map_err(|_| Status::unavailable("peer is stopping"))?))
    }

    async fn status(&self, _req: Request<pb::StatusRequest>) -> Result<Response<pb::StatusReply>, Status> {
        let (reply, rx) = oneshot::channel();
        self.0.core.send(CoreMsg::Status { reply }).await.map_err(|_| Status::unavailable("peer is stopping"))?;
        let mut s = rx.await.map_err(|_| Status::unavailable("peer is stopping"))?;
        s.peer = Some(self.0.info.clone());
        Ok(Response::new(s))
    }

    async fn localize(&self, req: Request<pb::LocalizeRequest>) -> Result<Response<pb::LocalizeReply>, Status> {
        let r = req.into_inner();
        if self.0.objects.path(&r.run, &r.object_id).exists() {
            return Ok(Response::new(pb::LocalizeReply {}));
        }
        let Some(mesh) = &self.0.router.mesh else {
            return Err(Status::not_found("object not in this peer's store"));
        };
        mesh.localize(&self.0.objects, &r.run, &r.object_id, &[])
            .await
            .map_err(|e| Status::not_found(format!("object not found on any live peer: {e}")))?;
        Ok(Response::new(pb::LocalizeReply {}))
    }

    async fn shutdown(&self, _req: Request<pb::ShutdownRequest>) -> Result<Response<pb::ShutdownReply>, Status> {
        self.0.shutdown.notify_waiters();
        Ok(Response::new(pb::ShutdownReply {}))
    }
}

impl Ingress {
    /// Read from a replica on another peer (this one does not host the group).
    async fn remote_read(&self, vshard: u16, req: pb::ReadRequest) -> Result<pb::ReadReply, Status> {
        let mesh = self.router.mesh.as_ref().ok_or_else(|| Status::internal("group not hosted on a local peer"))?;
        mesh.read_group(self.router.group_of(vshard), req).await.map_err(|e| Status::unavailable(e.to_string()))
    }

    async fn create_actor(&self, a: pb::ActorCreateMsg) -> pb::session_event::Event {
        let (Some(spec), Some(ctor)) = (a.spec, a.ctor) else {
            return pb::session_event::Event::Error("actor create needs a spec and a constructor call".into());
        };
        let policy = pb::Policy::decode(ctor.policy.as_slice()).unwrap_or_default();
        if let Some(why) = self.infeasible(&policy) {
            return pb::session_event::Event::Error(why);
        }
        let Ok(actor) = Id::from_slice(&spec.actor) else {
            return pb::session_event::Event::Error("malformed actor id".into());
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
        if ack.status() == pb::actor_ack::Status::Ok {
            let acks = self.submit(pb::SubmitTasks { tasks: vec![ctor], grant_to: None }).await;
            if let Some(a) = acks.acks.first()
                && a.status() == pb::task_ack::Status::Rejected
            {
                return pb::session_event::Event::Error(format!("constructor rejected: {}", a.reason));
            }
        }
        pb::session_event::Event::Actor(ack)
    }
}
