//! Peer-to-peer RPC: the client side (one HTTP/2 channel per peer, the raft
//! transport over it) and the `Mesh` service every cluster peer serves.

use crate::core::CoreMsg;
use crate::router::Router;
use crate::stores::{CodeStore, ObjectStore};
use chimaera_core::pb;
use chimaera_core::shard::{Tk, read};
use chimaera_raft::{ProposeError, RpcFuture, RpcKind, Transport};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

type Rpc = pb::mesh_client::MeshClient<tonic::service::interceptor::InterceptedService<Channel, AddToken>>;

/// Adds the cluster token to every outgoing mesh call.
#[derive(Clone)]
pub struct AddToken(Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for AddToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

/// Refuses mesh calls that do not carry this cluster's token. Compares
/// blake3 digests, whose equality check runs in constant time.
#[derive(Clone)]
pub struct CheckToken(Option<blake3::Hash>);

impl CheckToken {
    pub fn new(token: Option<&str>) -> CheckToken {
        CheckToken(token.map(|t| blake3::hash(t.as_bytes())))
    }
}

impl tonic::service::Interceptor for CheckToken {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let Some(want) = &self.0 else { return Ok(req) };
        let got = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|s| s.strip_prefix("Bearer "));
        match got {
            Some(t) if blake3::hash(t.as_bytes()) == *want => Ok(req),
            _ => Err(Status::unauthenticated("missing or wrong cluster token")),
        }
    }
}

const MAX_MESSAGE: usize = 256 << 20;
const FETCH_CHUNK: usize = 1 << 20;
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// What this peer knows about another member.
#[derive(Clone, Debug)]
pub struct PeerView {
    pub member: pb::Member,
    pub load: Option<pb::PingReply>,
    pub last_ok: Option<Instant>,
    pub dead: bool,
    /// Attempts this peer placed there since its last load report.
    pub placed_since_report: u32,
}

impl PeerView {
    /// Fraction of the peer's CPU in use or promised, for placement.
    pub fn pressure(&self) -> f64 {
        let cpus = self.member.cpus.max(1e-9);
        let (used, queued) = self.load.as_ref().map_or((0.0, 0), |l| (l.cpu_used, l.queued));
        (used + queued as f64 + self.placed_since_report as f64) / cpus
    }
}

pub struct MeshClient {
    pub cluster: String,
    pub me: pb::Member,
    auth: AddToken,
    members: RwLock<BTreeMap<u64, PeerView>>,
    voters: RwLock<HashMap<u32, Vec<u64>>>,
    channels: Mutex<HashMap<String, Channel>>,
}

fn status_err(s: Status) -> std::io::Error {
    std::io::Error::other(format!("{}: {}", s.code(), s.message()))
}

impl MeshClient {
    /// `token`: the cluster's shared secret, sent on every call (None: no auth).
    pub fn new(cluster: String, me: pb::Member, token: Option<&str>) -> std::io::Result<MeshClient> {
        let auth = match token {
            Some(t) => AddToken(Some(
                format!("Bearer {t}").parse().map_err(|_| std::io::Error::other("the cluster token must be printable ASCII"))?,
            )),
            None => AddToken(None),
        };
        let mut members = BTreeMap::new();
        members.insert(
            me.node_id,
            PeerView {
                member: me.clone(),
                load: None,
                last_ok: Some(Instant::now()),
                dead: false,
                placed_since_report: 0,
            },
        );
        Ok(MeshClient {
            cluster,
            me,
            auth,
            members: RwLock::new(members),
            voters: RwLock::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
        })
    }

    fn channel(&self, addr: &str) -> Result<Rpc, std::io::Error> {
        let mut chans = self.channels.lock().expect("lock");
        let ch = match chans.get(addr) {
            Some(c) => c.clone(),
            None => {
                let c = Endpoint::from_shared(format!("http://{addr}"))
                    .map_err(std::io::Error::other)?
                    .connect_timeout(Duration::from_secs(1))
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(5))
                    .keep_alive_timeout(Duration::from_secs(5))
                    .connect_lazy();
                chans.insert(addr.to_string(), c.clone());
                c
            }
        };
        Ok(pb::mesh_client::MeshClient::with_interceptor(ch, self.auth.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE))
    }

    fn rpc(&self, node: u64) -> Result<Rpc, std::io::Error> {
        let addr = self
            .members
            .read()
            .expect("lock")
            .get(&node)
            .map(|p| p.member.addr.clone())
            .ok_or_else(|| std::io::Error::other(format!("unknown node {node}")))?;
        self.channel(&addr)
    }

    // ---- membership view ---------------------------------------------------

    pub fn members(&self) -> Vec<PeerView> {
        self.members.read().expect("lock").values().cloned().collect()
    }

    pub fn member(&self, node: u64) -> Option<PeerView> {
        self.members.read().expect("lock").get(&node).cloned()
    }

    pub fn member_by_peer(&self, peer: &[u8]) -> Option<PeerView> {
        self.members.read().expect("lock").values().find(|p| p.member.peer == peer).cloned()
    }

    /// Record a member (or a newer incarnation of one).
    pub fn upsert(&self, m: pb::Member) {
        let mut ms = self.members.write().expect("lock");
        match ms.get_mut(&m.node_id) {
            Some(p) if p.member.incarnation > m.incarnation => {}
            Some(p) => {
                if p.member.incarnation < m.incarnation {
                    p.dead = false;
                    p.load = None;
                }
                p.member = m;
            }
            None => {
                ms.insert(
                    m.node_id,
                    PeerView { member: m, load: None, last_ok: None, dead: false, placed_since_report: 0 },
                );
            }
        }
    }

    pub fn set_voters(&self, group: u32, voters: Vec<u64>) {
        self.voters.write().expect("lock").insert(group, voters);
    }

    pub fn voters_of(&self, group: u32) -> Vec<u64> {
        let v = self.voters.read().expect("lock").get(&group).cloned().unwrap_or_default();
        if v.is_empty() { self.members.read().expect("lock").keys().copied().collect() } else { v }
    }

    pub fn note_ping(&self, node: u64, reply: Option<pb::PingReply>) {
        let mut ms = self.members.write().expect("lock");
        let Some(p) = ms.get_mut(&node) else { return };
        if let Some(r) = reply {
            if r.incarnation > p.member.incarnation {
                p.member.incarnation = r.incarnation;
            }
            p.last_ok = Some(Instant::now());
            p.dead = false;
            p.placed_since_report = 0;
            p.load = Some(r);
        }
    }

    pub fn mark_dead(&self, node: u64) -> Option<pb::Member> {
        let mut ms = self.members.write().expect("lock");
        let p = ms.get_mut(&node)?;
        if p.dead {
            return None;
        }
        p.dead = true;
        Some(p.member.clone())
    }

    pub fn note_placed(&self, node: u64) {
        if let Some(p) = self.members.write().expect("lock").get_mut(&node) {
            p.placed_since_report += 1;
        }
    }

    /// Live members other than this peer that take new work.
    pub fn live_others(&self) -> Vec<PeerView> {
        self.members
            .read()
            .expect("lock")
            .values()
            .filter(|p| !p.dead && p.member.node_id != self.me.node_id)
            .filter(|p| !p.load.as_ref().is_some_and(|l| l.draining))
            .cloned()
            .collect()
    }

    // ---- calls ------------------------------------------------------------

    pub async fn join(&self, addr: &str, expect: u32) -> std::io::Result<pb::JoinReply> {
        let mut c = self.channel(addr)?;
        let req =
            pb::JoinRequest { cluster: self.cluster.clone(), member: Some(self.me.clone()), bootstrap_expect: expect };
        let r = tokio::time::timeout(CALL_TIMEOUT, c.join(req)).await.map_err(std::io::Error::other)?;
        Ok(r.map_err(status_err)?.into_inner())
    }

    pub async fn propose(&self, node: u64, group: u32, cmd: &pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
        let unavailable = |reason: String| ProposeError::Unavailable { group, reason };
        let mut c = self.rpc(node).map_err(|e| unavailable(e.to_string()))?;
        let req = pb::ProposeRequest { cluster: self.cluster.clone(), group, cmd: Some(cmd.clone()) };
        let r = tokio::time::timeout(CALL_TIMEOUT, c.propose(req))
            .await
            .map_err(|_| unavailable(format!("node {node} did not answer")))?
            .map_err(|s| unavailable(s.message().to_string()))?
            .into_inner();
        if r.not_leader {
            return Err(ProposeError::NotLeader { group, leader: (r.leader_hint != 0).then_some(r.leader_hint) });
        }
        if !r.error.is_empty() {
            return Err(unavailable(r.error));
        }
        Ok(r.reply.unwrap_or_default())
    }

    pub async fn read(&self, node: u64, req: pb::ReadRequest) -> std::io::Result<pb::ReadReply> {
        let mut c = self.rpc(node)?;
        let r = tokio::time::timeout(CALL_TIMEOUT, c.read(req)).await.map_err(std::io::Error::other)?;
        let reply = r.map_err(status_err)?.into_inner();
        if !reply.error.is_empty() {
            return Err(std::io::Error::other(reply.error));
        }
        Ok(reply)
    }

    /// Read records from any replica of `group`, trying each in turn.
    pub async fn read_group(&self, group: u32, mut req: pb::ReadRequest) -> std::io::Result<pb::ReadReply> {
        req.cluster = self.cluster.clone();
        let mut last = std::io::Error::other(format!("no replica of group {group} answered"));
        for node in self.voters_of(group) {
            if node == self.me.node_id {
                continue;
            }
            match self.read(node, req.clone()).await {
                Ok(r) => return Ok(r),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    pub async fn dispatch(&self, node: u64, req: pb::DispatchRequest) -> std::io::Result<()> {
        let mut c = self.rpc(node)?;
        tokio::time::timeout(CALL_TIMEOUT, c.dispatch(req))
            .await
            .map_err(std::io::Error::other)?
            .map_err(status_err)?;
        Ok(())
    }

    pub async fn forget(&self, node: u64, mut req: pb::ForgetRequest) -> std::io::Result<()> {
        req.cluster = self.cluster.clone();
        let mut c = self.rpc(node)?;
        tokio::time::timeout(CALL_TIMEOUT, c.forget(req)).await.map_err(std::io::Error::other)?.map_err(status_err)?;
        Ok(())
    }

    pub async fn ping(&self, node: u64, timeout: Duration) -> std::io::Result<pb::PingReply> {
        let mut c = self.rpc(node)?;
        let req = pb::PingRequest { cluster: self.cluster.clone(), from_node: self.me.node_id };
        let r = tokio::time::timeout(timeout, c.ping(req)).await.map_err(std::io::Error::other)?;
        Ok(r.map_err(status_err)?.into_inner())
    }

    pub async fn watch(&self, node: u64, tasks: Vec<pb::TaskKey>) -> std::io::Result<Streaming<pb::OutcomeEvent>> {
        let mut c = self.rpc(node)?;
        let req = pb::WatchRequest { cluster: self.cluster.clone(), tasks };
        Ok(c.watch(req).await.map_err(status_err)?.into_inner())
    }

    /// Copy an object from `node` into `dest` (atomically: temp file + rename).
    pub async fn fetch(&self, node: u64, run: &[u8], object: &[u8], dest: &Path) -> std::io::Result<u64> {
        use tokio::io::AsyncWriteExt;
        let mut c = self.rpc(node)?;
        let req = pb::FetchRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            code: false,
        };
        let mut stream = c.fetch(req).await.map_err(status_err)?.into_inner();
        if let Some(dir) = dest.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let tmp = dest.with_extension(format!("part-{}", std::process::id()));
        let mut f = tokio::fs::File::create(&tmp).await?;
        let (mut total, mut got) = (None, 0u64);
        loop {
            match stream.message().await {
                Ok(Some(chunk)) => {
                    if total.is_none() {
                        total = Some(chunk.total);
                    }
                    got += chunk.data.len() as u64;
                    f.write_all(&chunk.data).await?;
                }
                Ok(None) => break,
                Err(s) => {
                    let _ = tokio::fs::remove_file(&tmp).await;
                    return Err(status_err(s));
                }
            }
        }
        f.flush().await?;
        drop(f);
        if total.is_some_and(|t| t != got) {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(std::io::Error::other(format!("short fetch: {got} of {total:?} bytes")));
        }
        tokio::fs::rename(&tmp, dest).await?;
        Ok(got)
    }

    /// Make an object present in the local store, fetching it from one of
    /// `holders` (node ids), or from any live member when none is known.
    pub async fn localize(
        &self,
        store: &ObjectStore,
        run: &[u8],
        object: &[u8],
        holders: &[u64],
    ) -> std::io::Result<()> {
        let dest = store.path(run, object);
        if dest.exists() {
            return Ok(());
        }
        let mut candidates: Vec<u64> = holders.iter().copied().filter(|n| *n != self.me.node_id).collect();
        if candidates.is_empty() {
            candidates = self.live_others().iter().map(|p| p.member.node_id).collect();
        }
        let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no peer holds the object");
        for node in candidates {
            match self.fetch(node, run, object, &dest).await {
                Ok(_) => return Ok(()),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Make a code blob present in the local store, from any live member.
    /// The store checks the bytes against the digest.
    pub async fn localize_code(&self, store: &CodeStore, digest: &[u8]) -> std::io::Result<()> {
        if store.has(digest) {
            return Ok(());
        }
        let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no peer holds the code");
        for p in self.live_others() {
            let Ok(mut c) = self.rpc(p.member.node_id) else { continue };
            let req = pb::FetchRequest {
                cluster: self.cluster.clone(),
                object_id: digest.to_vec(),
                code: true,
                ..Default::default()
            };
            let mut stream = match c.fetch(req).await {
                Ok(s) => s.into_inner(),
                Err(s) => {
                    last = status_err(s);
                    continue;
                }
            };
            let mut bytes = Vec::new();
            let mut ok = true;
            loop {
                match stream.message().await {
                    Ok(Some(chunk)) => bytes.extend_from_slice(&chunk.data),
                    Ok(None) => break,
                    Err(s) => {
                        last = status_err(s);
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                return store.put(digest, &bytes);
            }
        }
        Err(last)
    }

    /// Node ids of the peers listed as an object's locations.
    pub fn holders(&self, obj: &pb::ObjectRef) -> Vec<u64> {
        obj.locations.iter().filter_map(|l| self.member_by_peer(&l.peer).map(|p| p.member.node_id)).collect()
    }
}

/// Raft RPCs over the mesh.
pub struct MeshTransport(pub Arc<MeshClient>);

impl Transport for MeshTransport {
    fn call(&self, target: u64, addr: &str, group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture {
        let client = self.0.channel(addr);
        let cluster = self.0.cluster.clone();
        Box::pin(async move {
            let mut c = client?;
            let req = pb::RaftRequest { cluster, group, kind: kind.code() as u32, body };
            let r = c.raft(req).await.map_err(|s| std::io::Error::other(format!("node {target}: {}", s.message())))?;
            Ok(r.into_inner().body)
        })
    }
}

// ---- server -----------------------------------------------------------------

/// Starts a group this peer was asked to replicate (a new voter or learner).
#[tonic::async_trait]
pub trait GroupStarter: Send + Sync {
    async fn ensure(&self, group: u32) -> std::io::Result<chimaera_raft::Group>;
}

pub struct MeshServer {
    pub mesh: Arc<MeshClient>,
    /// Set once the cluster plan is known (Join and Ping work before).
    pub router: std::sync::OnceLock<Arc<Router>>,
    pub core: mpsc::Sender<CoreMsg>,
    pub objects: ObjectStore,
    pub code: CodeStore,
    pub starter: Arc<dyn GroupStarter>,
    pub plan: Arc<RwLock<Option<pb::ClusterPlan>>>,
    pub scratch: PathBuf,
    pub next_session: AtomicU64,
}

impl MeshServer {
    fn router(&self) -> Result<&Arc<Router>, Status> {
        self.router.get().ok_or_else(|| Status::unavailable("peer is still forming the cluster"))
    }

    fn check(&self, cluster: &str) -> Result<(), Status> {
        if cluster != self.mesh.cluster {
            return Err(Status::permission_denied(format!(
                "this peer belongs to cluster {:?}, not {cluster:?}",
                self.mesh.cluster
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct MeshService(pub Arc<MeshServer>);

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl pb::mesh_server::Mesh for MeshService {
    async fn join(&self, req: Request<pb::JoinRequest>) -> Result<Response<pb::JoinReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        if let Some(m) = r.member {
            self.0.mesh.upsert(m);
        }
        let members = self.0.mesh.members().into_iter().map(|p| p.member).collect();
        let plan = self.0.plan.read().expect("lock").clone();
        Ok(Response::new(pb::JoinReply { members, plan }))
    }

    async fn raft(&self, req: Request<pb::RaftRequest>) -> Result<Response<pb::RaftReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let kind = RpcKind::from_code(r.kind as u8).ok_or_else(|| Status::invalid_argument("unknown raft rpc"))?;
        let group = match self.0.router()?.hosted(r.group) {
            Some(g) => g,
            None => self.0.starter.ensure(r.group).await.map_err(|e| Status::unavailable(e.to_string()))?,
        };
        let body = chimaera_raft::handle_rpc(&group, kind, &r.body, &self.0.scratch)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::RaftReply { body }))
    }

    async fn propose(&self, req: Request<pb::ProposeRequest>) -> Result<Response<pb::ProposeReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let Some(group) = self.0.router()?.hosted(r.group) else {
            return Ok(Response::new(pb::ProposeReply { not_leader: true, ..Default::default() }));
        };
        let cmd = r.cmd.unwrap_or_default();
        let reply = match group.propose(&cmd).await {
            Ok(reply) => pb::ProposeReply { reply: Some(reply), ..Default::default() },
            Err(ProposeError::NotLeader { leader, .. }) => {
                pb::ProposeReply { not_leader: true, leader_hint: leader.unwrap_or(0), ..Default::default() }
            }
            Err(e) => pb::ProposeReply { error: e.to_string(), ..Default::default() },
        };
        Ok(Response::new(reply))
    }

    async fn read(&self, req: Request<pb::ReadRequest>) -> Result<Response<pb::ReadReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let router = self.0.router()?;
        let mut reply = pb::ReadReply::default();
        let vshard = r
            .tasks
            .first()
            .and_then(Tk::parse)
            .map(|t| t.v())
            .or_else(|| chimaera_core::ids::Id::from_slice(&r.run).ok().map(|i| i.vshard()))
            .or_else(|| chimaera_core::ids::Id::from_slice(&r.actor).ok().map(|i| i.vshard()));
        let Some(v) = vshard else { return Ok(Response::new(reply)) };
        let Some(group) = router.hosted(router.group_of(v)) else {
            reply.error = "group not hosted here".into();
            return Ok(Response::new(reply));
        };
        if r.linearizable
            && let Err(e) = group.linearizable().await
        {
            reply.error = e.to_string();
            return Ok(Response::new(reply));
        }
        let page_of = chimaera_core::ids::Id::from_slice(&r.run).ok().filter(|_| r.limit > 0);
        let after = chimaera_core::ids::Id::from_slice(&r.after_task).ok();
        let view = group.read(|kv| {
            let mut tasks: Vec<pb::TaskRecord> =
                r.tasks.iter().map(|k| Tk::parse(k).and_then(|t| read::task(kv, t)).unwrap_or_default()).collect();
            let mut keys = Vec::new();
            if let Some(run) = page_of {
                for (tk, rec) in read::tasks_page(kv, run, after, r.limit as usize) {
                    keys.push(tk.pb());
                    tasks.push(rec);
                }
            }
            let run = chimaera_core::ids::Id::from_slice(&r.run).ok().and_then(|id| read::run(kv, id));
            let actor = chimaera_core::ids::Id::from_slice(&r.actor).ok().and_then(|id| read::actor(kv, id));
            (tasks, keys, run, actor)
        });
        match view {
            Ok((tasks, keys, run, actor)) => {
                reply.tasks = tasks;
                reply.page_keys = keys;
                reply.run = run;
                reply.actor = actor;
            }
            Err(e) => reply.error = e.to_string(),
        }
        Ok(Response::new(reply))
    }

    async fn dispatch(&self, req: Request<pb::DispatchRequest>) -> Result<Response<pb::DispatchReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let accepted = r.work.len() as u32;
        self.0
            .core
            .send(CoreMsg::Remote { work: r.work, actors: r.actors, actor_env: r.actor_env })
            .await
            .map_err(|_| Status::unavailable("peer is stopping"))?;
        Ok(Response::new(pb::DispatchReply { accepted }))
    }

    type FetchStream = BoxStream<pb::FetchChunk>;

    async fn fetch(&self, req: Request<pb::FetchRequest>) -> Result<Response<Self::FetchStream>, Status> {
        use tokio::io::AsyncReadExt;
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let path = if r.code { self.0.code.path(&r.object_id) } else { self.0.objects.path(&r.run, &r.object_id) };
        let mut f = tokio::fs::File::open(&path).await.map_err(|e| Status::not_found(e.to_string()))?;
        let total = f.metadata().await.map_err(|e| Status::internal(e.to_string()))?.len();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let mut buf = vec![0u8; FETCH_CHUNK];
                let n = match f.read(&mut buf).await {
                    Ok(0) if !first => return,
                    Ok(n) => n,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                };
                buf.truncate(n);
                let chunk = pb::FetchChunk { data: buf, total: if first { total } else { 0 } };
                first = false;
                if tx.send(Ok(chunk)).await.is_err() || n == 0 {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))))
    }

    type WatchStream = BoxStream<pb::OutcomeEvent>;

    async fn watch(&self, req: Request<pb::WatchRequest>) -> Result<Response<Self::WatchStream>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let tasks: Vec<Tk> = r.tasks.iter().filter_map(Tk::parse).collect();
        let session = (1u64 << 62) | self.0.next_session.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let core = self.0.core.clone();
        core.send(CoreMsg::OpenSession { id: session, tx }).await.map_err(|_| Status::unavailable("stopping"))?;
        core.send(CoreMsg::Watch { session, tasks: tasks.clone() })
            .await
            .map_err(|_| Status::unavailable("stopping"))?;
        let (out_tx, out_rx) = mpsc::channel(256);
        tokio::spawn(async move {
            let mut left = tasks.len();
            while left > 0 {
                let Some(ev) = rx.recv().await else { break };
                if let Some(pb::session_event::Event::Outcome(o)) = ev.event {
                    left -= 1;
                    if out_tx.send(Ok(o)).await.is_err() {
                        break;
                    }
                }
            }
            let _ = core.send(CoreMsg::CloseSession { id: session }).await;
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(out_rx))))
    }

    async fn ping(&self, req: Request<pb::PingRequest>) -> Result<Response<pb::PingReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.0.core.send(CoreMsg::Load { reply }).await.map_err(|_| Status::unavailable("stopping"))?;
        let mut p = rx.await.map_err(|_| Status::unavailable("stopping"))?;
        p.node_id = self.0.mesh.me.node_id;
        p.incarnation = self.0.mesh.me.incarnation;
        p.leading = self.0.router.get().map(|r| r.leading()).unwrap_or_default();
        Ok(Response::new(p))
    }

    async fn forget(&self, req: Request<pb::ForgetRequest>) -> Result<Response<pb::ForgetReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let _ = self.0.core.send(CoreMsg::Forget(r)).await;
        Ok(Response::new(pb::ForgetReply {}))
    }
}
