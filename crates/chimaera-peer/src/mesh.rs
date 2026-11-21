//! Peer-to-peer RPC: the client side (one HTTP/2 channel per peer, the raft
//! transport over it) and the `Mesh` service every cluster peer serves.

use crate::core::CoreMsg;
use crate::router::Router;
use crate::stores::{CodeStore, ObjectStore, hex};
use chimaera_core::pb;
use chimaera_core::shard::{Tk, read};
use chimaera_core::swim::{Config as SwimConfig, State as SwimState, Swim, Update as SwimUpdate};
use chimaera_raft::{ProposeError, RpcFuture, RpcKind, Transport};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
        let got =
            req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|s| s.strip_prefix("Bearer "));
        match got {
            Some(t) if blake3::hash(t.as_bytes()) == *want => Ok(req),
            _ => Err(Status::unauthenticated("missing or wrong cluster token")),
        }
    }
}

const MAX_MESSAGE: usize = 256 << 20;
const FETCH_CHUNK: usize = 4 << 20;
/// Objects this large come over several streams at once (the guides measured
/// 8-10x from multiple connections on large transfers).
const PARALLEL_FETCH: u64 = 64 << 20;
/// Peers asked for an object (holders included) before it counts as lost.
const REPLICA_SEARCH: usize = 8;
const FETCH_RANGES: u64 = 8;
/// Transfers this large teach the bandwidth estimate.
const RATE_SAMPLE_MIN: u64 = 1 << 20;
/// Objects fetched this recently are offered as locality hints.
const RECENT_FOR: Duration = Duration::from_secs(60);
const RECENT_MAX: usize = 256;
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a relay waits for the target of an indirect probe.
pub const RELAY_TIMEOUT: Duration = Duration::from_millis(500);
/// How long a ping waits for the core's load report before answering without it.
const LOAD_WAIT: Duration = Duration::from_millis(200);

/// SWIM timing for cluster peers: probe every 300 ms; a missed direct and
/// indirect probe makes a member suspect; it is declared dead after
/// 4 × log10(max(n, 10)) periods without refuting (1.2 s up to 10 peers).
pub fn swim_config() -> SwimConfig {
    SwimConfig { period_ms: 300, indirect: 3, suspicion: 4, retransmit: 4, piggyback: 8, suspicion_floor_ms: 6_000 }
}

/// What this peer knows about another member.
#[derive(Clone, Debug)]
pub struct PeerView {
    pub member: pb::Member,
    pub load: Option<pb::PingReply>,
    pub last_ok: Option<Instant>,
    pub dead: bool,
    /// Missed a direct and an indirect probe: no new work goes there until
    /// it answers again or is declared dead.
    pub suspect: bool,
    /// Attempts this peer placed there since its last load report.
    pub placed_since_report: u32,
}

impl PeerView {
    /// Fraction of the peer's CPU in use or promised, for placement.
    pub fn pressure(&self) -> f64 {
        let cpus = self.member.cpus.max(1e-9);
        let (used, queued) = self.load.as_ref().map_or((0.0, 0), |l| (l.cpu_used, l.queued));
        ((used + queued as f64 + self.placed_since_report as f64) / cpus).max(self.memory_pressure())
    }

    /// Fraction of the peer's memory pool in use (0 when it has not said).
    pub fn memory_pressure(&self) -> f64 {
        self.load.as_ref().filter(|l| l.memory_pool > 0).map_or(0.0, |l| l.memory_used as f64 / l.memory_pool as f64)
    }

    /// Whether the peer reported holding `object` recently.
    pub fn recently_has(&self, object: &[u8]) -> bool {
        self.load.as_ref().is_some_and(|l| l.recent_blobs.iter().any(|b| b == object))
    }
}

pub struct MeshClient {
    pub cluster: String,
    pub me: pb::Member,
    auth: AddToken,
    /// Mutual TLS for every peer connection (None: plaintext).
    tls: Option<tonic::transport::ClientTlsConfig>,
    members: RwLock<BTreeMap<u64, PeerView>>,
    voters: RwLock<HashMap<u32, Vec<u64>>>,
    channels: Mutex<HashMap<String, Channel>>,
    /// One transfer per object at a time, however many attempts need it.
    inflight: Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
    /// Learned transfer rate from each peer, bytes per second (EWMA).
    bandwidth: Mutex<HashMap<u64, f64>>,
    /// Objects fetched from each peer: (transfers, bytes).
    fetched: Mutex<HashMap<u64, (u64, u64)>>,
    /// Objects fetched recently (locality hints for other peers).
    recent: Mutex<VecDeque<(Instant, Vec<u8>)>>,
    /// Objects other peers asked this one for, and when: their fetched copy
    /// already makes a replica unnecessary (see objplane).
    served: Mutex<HashMap<Vec<u8>, Instant>>,
    /// Profiles other peers sent with their ping replies, for the core.
    received: Mutex<Vec<(u64, pb::ProfileSummary)>>,
    /// SWIM membership (who is alive), driven by the liveness loop.
    pub swim: Mutex<Swim>,
    epoch: Instant,
}

/// SWIM incarnation of a member: process incarnation high, refutations low.
pub fn swim_inc(process_incarnation: u64) -> u64 {
    process_incarnation << 32
}

fn swim_state(s: SwimState) -> u32 {
    match s {
        SwimState::Alive => 0,
        SwimState::Suspect => 1,
        SwimState::Dead => 2,
    }
}

static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// `size` bytes in `n` contiguous ranges (offset, len).
fn ranges(size: u64, n: u64) -> Vec<(u64, u64)> {
    let step = size.div_ceil(n.max(1)).max(1);
    (0..size).step_by(step as usize).map(|off| (off, step.min(size - off))).collect()
}

/// Whether a call failed because nothing listens at the peer's address: its
/// process is gone (a stalled or unreachable peer times out instead).
fn refused(s: &Status) -> bool {
    let mut e: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(s);
    while let Some(err) = e {
        if err.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused) {
            return true;
        }
        e = err.source();
    }
    format!("{s:?}").contains("onnection refused")
}

fn status_err(s: Status) -> std::io::Error {
    std::io::Error::other(format!("{}: {}", s.code(), s.message()))
}

impl MeshClient {
    /// `token`: the cluster's shared secret, sent on every call (None: no
    /// auth). `tls`: dial peers over mutual TLS.
    pub fn new(
        cluster: String,
        me: pb::Member,
        token: Option<&str>,
        tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> std::io::Result<MeshClient> {
        let (me_node, me_inc) = (me.node_id, me.incarnation);
        let auth = match token {
            Some(t) => AddToken(Some(
                format!("Bearer {t}")
                    .parse()
                    .map_err(|_| std::io::Error::other("the cluster token must be printable ASCII"))?,
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
                suspect: false,
                placed_since_report: 0,
            },
        );
        Ok(MeshClient {
            cluster,
            me,
            auth,
            tls,
            members: RwLock::new(members),
            voters: RwLock::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            bandwidth: Mutex::new(HashMap::new()),
            fetched: Mutex::new(HashMap::new()),
            recent: Mutex::new(VecDeque::new()),
            served: Mutex::new(HashMap::new()),
            received: Mutex::new(Vec::new()),
            swim: Mutex::new(Swim::new(me_node, swim_inc(me_inc), me_node, swim_config())),
            epoch: Instant::now(),
        })
    }

    fn channel(&self, addr: &str) -> Result<Rpc, std::io::Error> {
        let mut chans = self.channels.lock().expect("lock");
        let ch = match chans.get(addr) {
            Some(c) => c.clone(),
            None => {
                let scheme = if self.tls.is_some() { "https" } else { "http" };
                let mut e = Endpoint::from_shared(format!("{scheme}://{addr}")).map_err(std::io::Error::other)?;
                if let Some(tls) = &self.tls {
                    e = e.tls_config(tls.clone()).map_err(std::io::Error::other)?;
                }
                let c = e
                    .connect_timeout(Duration::from_secs(1))
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(5))
                    .keep_alive_timeout(Duration::from_secs(5))
                    // Objects stream over these channels: windows sized for
                    // bulk transfer (and grown with the measured bandwidth-delay
                    // product), not HTTP/2's 64 KiB default.
                    .initial_stream_window_size(8 << 20)
                    .initial_connection_window_size(32 << 20)
                    .http2_adaptive_window(true)
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

    /// Record a member (or a newer incarnation of one), and tell SWIM.
    pub fn upsert(&self, m: pb::Member) {
        if m.node_id != self.me.node_id {
            let now = self.now_ms();
            let mut swim = self.swim.lock().expect("lock");
            swim.add(now, m.node_id, swim_inc(m.incarnation));
            let up = SwimUpdate { node: m.node_id, inc: swim_inc(m.incarnation), state: SwimState::Alive };
            swim.merge(now, &[up]);
        }
        let mut ms = self.members.write().expect("lock");
        match ms.get_mut(&m.node_id) {
            Some(p) if p.member.incarnation > m.incarnation => {}
            Some(p) => {
                if p.member.incarnation < m.incarnation {
                    p.dead = false;
                    p.suspect = false;
                    p.load = None;
                }
                p.member = m;
            }
            None => {
                ms.insert(
                    m.node_id,
                    PeerView {
                        member: m,
                        load: None,
                        last_ok: None,
                        dead: false,
                        suspect: false,
                        placed_since_report: 0,
                    },
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
        if let Some(mut r) = reply {
            let profiles = std::mem::take(&mut r.profiles);
            if !profiles.is_empty() {
                self.received.lock().expect("lock").extend(profiles.into_iter().map(|s| (node, s)));
            }
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
            .filter(|p| !p.dead && !p.suspect && p.member.node_id != self.me.node_id)
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

    pub async fn push_logs(&self, node: u64, mut req: pb::PushLogsRequest) -> std::io::Result<()> {
        req.cluster = self.cluster.clone();
        let mut c = self.rpc(node)?;
        tokio::time::timeout(CALL_TIMEOUT, c.push_logs(req))
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

    /// Ask `node` to keep a copy of an object this peer stores (it pulls the
    /// bytes from here, to its disk).
    pub async fn replicate(&self, node: u64, run: &[u8], object: &[u8], size: u64) -> std::io::Result<()> {
        let req = pb::ReplicateRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            size,
            from_node: self.me.node_id,
        };
        let mut c = self.rpc(node)?;
        tokio::time::timeout(CALL_TIMEOUT, c.replicate(req))
            .await
            .map_err(std::io::Error::other)?
            .map_err(status_err)?;
        Ok(())
    }

    /// Another peer asked for `object` (it will hold a copy).
    pub fn note_served(&self, object: &[u8]) {
        let mut s = self.served.lock().expect("lock");
        if s.len() > 65536 {
            s.retain(|_, t| t.elapsed() < Duration::from_secs(60));
        }
        s.insert(object.to_vec(), Instant::now());
    }

    /// Whether another peer asked for `object` in the last minute.
    pub fn was_served(&self, object: &[u8]) -> bool {
        self.served.lock().expect("lock").get(object).is_some_and(|t| t.elapsed() < Duration::from_secs(60))
    }

    /// Where this peer's objects are replicated: the next live member by
    /// node id, wrapping around (None when alone).
    pub fn successor(&self) -> Option<u64> {
        let mut ids: Vec<u64> = self.live_others().iter().map(|p| p.member.node_id).collect();
        ids.sort_unstable();
        ids.iter().copied().find(|&n| n > self.me.node_id).or_else(|| ids.first().copied())
    }

    /// Give every live peer a copy of a code blob (best effort): the peer a
    /// program connected to may die while its calls still need the code.
    pub fn spread_code(self: &Arc<Self>, digest: Vec<u8>, code: Vec<u8>) {
        for p in self.live_others() {
            let (mesh, node) = (self.clone(), p.member.node_id);
            let req = pb::MeshPutCode { cluster: self.cluster.clone(), digest: digest.clone(), code: code.clone() };
            tokio::spawn(async move {
                let Ok(mut c) = mesh.rpc(node) else { return };
                if let Err(e) = tokio::time::timeout(CALL_TIMEOUT, c.put_code(req)).await {
                    tracing::debug!("code copy to node {node} timed out: {e}");
                }
            });
        }
    }

    /// Probe `node` directly, gossiping membership both ways.
    pub async fn ping(&self, node: u64, timeout: Duration) -> std::io::Result<pb::PingReply> {
        self.ping_with(node, 0, timeout).await
    }

    /// Ask `relay` to probe `target` (SWIM's indirect probe).
    pub async fn ping_via(&self, relay: u64, target: u64, timeout: Duration) -> bool {
        self.ping_with(relay, target, timeout).await.is_ok_and(|r| r.relay_ok)
    }

    async fn ping_with(&self, node: u64, relay_to: u64, timeout: Duration) -> std::io::Result<pb::PingReply> {
        let mut c = self.rpc(node)?;
        let req = pb::PingRequest {
            cluster: self.cluster.clone(),
            from_node: self.me.node_id,
            updates: self.swim_out(),
            relay_to,
        };
        let r = tokio::time::timeout(timeout, c.ping(req)).await.map_err(std::io::Error::other)?;
        let mut reply = r
            .map_err(|s| {
                if refused(&s) {
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, s.to_string())
                } else {
                    status_err(s)
                }
            })?
            .into_inner();
        self.swim_in(std::mem::take(&mut reply.updates));
        Ok(reply)
    }

    pub fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Membership claims to piggyback on an outgoing message.
    pub fn swim_out(&self) -> Vec<pb::SwimUpdate> {
        let ups = self.swim.lock().expect("lock").outgoing();
        let members = self.members.read().expect("lock");
        ups.into_iter()
            .map(|u| pb::SwimUpdate {
                node: u.node,
                inc: u.inc,
                state: swim_state(u.state),
                member: if u.state == SwimState::Alive {
                    if u.node == self.me.node_id {
                        Some(self.me.clone())
                    } else {
                        members.get(&u.node).map(|p| p.member.clone())
                    }
                } else {
                    None
                },
            })
            .collect()
    }

    /// Fold in membership claims carried by an incoming message.
    pub fn swim_in(&self, updates: Vec<pb::SwimUpdate>) {
        if updates.is_empty() {
            return;
        }
        let mut claims = Vec::with_capacity(updates.len());
        for u in updates {
            let state = match u.state {
                0 => SwimState::Alive,
                1 => SwimState::Suspect,
                _ => SwimState::Dead,
            };
            if let Some(m) = u.member.filter(|m| state == SwimState::Alive && m.node_id != self.me.node_id) {
                let known = self.members.read().expect("lock").get(&m.node_id).map(|p| p.member.incarnation);
                match known {
                    None => {
                        self.members.write().expect("lock").entry(m.node_id).or_insert(PeerView {
                            member: m,
                            load: None,
                            last_ok: None,
                            dead: false,
                            suspect: false,
                            placed_since_report: 0,
                        });
                    }
                    // A restarted peer: its new record replaces the old one
                    // and clears what was decided about the old incarnation.
                    Some(inc) if inc < m.incarnation => self.upsert(m),
                    Some(_) => {}
                }
            }
            claims.push(SwimUpdate { node: u.node, inc: u.inc, state });
        }
        let now = self.now_ms();
        self.swim.lock().expect("lock").merge(now, &claims);
    }

    pub fn set_suspect(&self, node: u64, suspect: bool) {
        if let Some(p) = self.members.write().expect("lock").get_mut(&node) {
            p.suspect = suspect;
        }
    }

    pub async fn watch(&self, node: u64, tasks: Vec<pb::TaskKey>) -> std::io::Result<Streaming<pb::OutcomeEvent>> {
        let mut c = self.rpc(node)?;
        let req = pb::WatchRequest { cluster: self.cluster.clone(), tasks };
        Ok(c.watch(req).await.map_err(status_err)?.into_inner())
    }

    /// Copy an object from `node` into `dest` (atomically: temp file +
    /// rename). With its `size` known and at least PARALLEL_FETCH, it comes as
    /// FETCH_RANGES ranges over parallel streams. Every chunk is checked
    /// against its checksum, and the rate teaches the bandwidth estimate.
    pub async fn fetch(&self, node: u64, run: &[u8], object: &[u8], size: u64, dest: &Path) -> std::io::Result<u64> {
        if let Some(dir) = dest.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let tmp =
            dest.with_extension(format!("part-{}-{}", std::process::id(), FETCH_SEQ.fetch_add(1, Ordering::Relaxed)));
        let started = Instant::now();
        // Size unknown (a reader asked by name): learn it from a one-byte
        // range, so a large object still comes over parallel streams.
        let size = if size == 0 { self.object_size(node, run, object).await.unwrap_or(0) } else { size };
        let mut got = if size >= PARALLEL_FETCH {
            self.fetch_ranges(node, run, object, size, &tmp).await
        } else {
            Err(std::io::Error::other("small"))
        };
        if got.is_err() {
            // Small, or a peer that does not serve ranges: one stream.
            tokio::fs::File::create(&tmp).await?;
            got = self.fetch_range(node, run, object, 0, 0, &tmp).await;
        }
        match got {
            Ok(n) => {
                tokio::fs::rename(&tmp, dest).await?;
                self.note_transfer(node, n, started.elapsed());
                self.note_recent(object);
                Ok(n)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(e)
            }
        }
    }

    /// The size of an object held by `node` (from the first chunk of a
    /// one-byte range); None from a peer that does not report it.
    async fn object_size(&self, node: u64, run: &[u8], object: &[u8]) -> Option<u64> {
        let mut c = self.rpc(node).ok()?;
        let req = pb::FetchRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            code: false,
            offset: 0,
            len: 1,
        };
        let mut stream = c.fetch(req).await.ok()?.into_inner();
        let first = stream.message().await.ok()??;
        (first.object_size > 0).then_some(first.object_size)
    }

    async fn fetch_ranges(&self, node: u64, run: &[u8], object: &[u8], size: u64, tmp: &Path) -> std::io::Result<u64> {
        tokio::fs::File::create(tmp).await?.set_len(size).await?;
        let parts = ranges(size, FETCH_RANGES);
        let got: u64 = futures_util::future::try_join_all(
            parts.iter().map(|&(off, len)| self.fetch_range(node, run, object, off, len, tmp)),
        )
        .await?
        .into_iter()
        .sum();
        if got != size {
            return Err(std::io::Error::other(format!("fetched {got} of {size} bytes")));
        }
        Ok(got)
    }

    /// Write bytes [offset, offset+len) of the object (len 0: to the end) into
    /// `path` at the same offset, checking each chunk.
    async fn fetch_range(
        &self,
        node: u64,
        run: &[u8],
        object: &[u8],
        offset: u64,
        len: u64,
        path: &Path,
    ) -> std::io::Result<u64> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        let mut c = self.rpc(node)?;
        let req = pb::FetchRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            code: false,
            offset,
            len,
        };
        let mut stream = c.fetch(req).await.map_err(status_err)?.into_inner();
        let mut f = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        f.seek(std::io::SeekFrom::Start(offset)).await?;
        let (mut total, mut got) = (None, 0u64);
        while let Some(chunk) = stream.message().await.map_err(status_err)? {
            total.get_or_insert(chunk.total);
            if !chunk.checksum.is_empty() && blake3::hash(&chunk.data).as_bytes()[..16] != chunk.checksum[..] {
                return Err(std::io::Error::other(format!("corrupt chunk at byte {} from node {node}", offset + got)));
            }
            got += chunk.data.len() as u64;
            f.write_all(&chunk.data).await?;
        }
        f.flush().await?;
        if total.is_some_and(|t| t != got) || (len > 0 && got != len) {
            return Err(std::io::Error::other(format!("short fetch: {got} of {total:?} bytes")));
        }
        Ok(got)
    }

    fn note_transfer(&self, node: u64, bytes: u64, took: Duration) {
        let mut fetched = self.fetched.lock().expect("lock");
        let f = fetched.entry(node).or_default();
        (f.0, f.1) = (f.0 + 1, f.1 + bytes);
        drop(fetched);
        if bytes < RATE_SAMPLE_MIN || took.is_zero() {
            return;
        }
        let rate = bytes as f64 / took.as_secs_f64();
        let mut bw = self.bandwidth.lock().expect("lock");
        let e = bw.entry(node).or_insert(rate);
        *e = 0.7 * *e + 0.3 * rate;
    }

    fn note_recent(&self, object: &[u8]) {
        let mut r = self.recent.lock().expect("lock");
        r.push_back((Instant::now(), object.to_vec()));
        while r.len() > RECENT_MAX || r.front().is_some_and(|(t, _)| t.elapsed() > RECENT_FOR) {
            r.pop_front();
        }
    }

    /// Learned transfer rate from `node`, bytes per second.
    pub fn bandwidth(&self, node: u64) -> Option<f64> {
        self.bandwidth.lock().expect("lock").get(&node).copied()
    }

    /// What was fetched from each peer and at what learned rate, for status:
    /// "from node N: T transfers, B bytes, R MB/s".
    pub fn links(&self) -> Vec<String> {
        let bw = self.bandwidth.lock().expect("lock");
        let fetched = self.fetched.lock().expect("lock");
        let mut out: Vec<String> = fetched
            .iter()
            .map(|(n, (t, b))| {
                let rate = bw.get(n).map_or("rate not measured yet".into(), |r| format!("{:.0} MB/s", r / 1e6));
                format!("from node {n}: {t} transfers, {b} bytes, {rate}")
            })
            .collect();
        out.sort();
        out
    }

    /// Objects fetched or written here recently (for ping replies).
    pub fn recent_objects(&self) -> Vec<Vec<u8>> {
        self.recent
            .lock()
            .expect("lock")
            .iter()
            .filter(|(t, _)| t.elapsed() <= RECENT_FOR)
            .map(|(_, o)| o.clone())
            .collect()
    }

    /// Profiles received from other peers since the last call.
    pub fn take_profiles(&self) -> Vec<(u64, pb::ProfileSummary)> {
        std::mem::take(&mut *self.received.lock().expect("lock"))
    }

    /// Make an object present in the local store, fetching it from one of
    /// `holders` (node ids), or from any live member when none is known.
    /// `size`: the object's bytes if known (0: unknown; it then comes over one
    /// stream). Attempts that need the same object at once share one transfer.
    pub async fn localize(
        &self,
        store: &ObjectStore,
        run: &[u8],
        object: &[u8],
        size: u64,
        holders: &[u64],
    ) -> std::io::Result<()> {
        let dest = store.path(run, object);
        if dest.exists() {
            return Ok(());
        }
        let gate = self.inflight.lock().expect("lock").entry(dest.clone()).or_default().clone();
        let held = gate.lock().await;
        let result = if dest.exists() { Ok(()) } else { self.fetch_from(run, object, size, holders, &dest).await };
        drop(held);
        let mut inflight = self.inflight.lock().expect("lock");
        if Arc::strong_count(&gate) <= 2 {
            inflight.remove(&dest); // nobody else is waiting on it
        }
        result
    }

    async fn fetch_from(
        &self,
        run: &[u8],
        object: &[u8],
        size: u64,
        holders: &[u64],
        dest: &Path,
    ) -> std::io::Result<()> {
        let mut candidates: Vec<u64> = holders.iter().copied().filter(|n| *n != self.me.node_id).collect();
        if candidates.is_empty() {
            candidates = self.live_others().iter().map(|p| p.member.node_id).collect();
        }
        let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no peer holds the object");
        // The fastest known holder first.
        candidates.sort_by(|a, b| self.bandwidth(*b).unwrap_or(0.0).total_cmp(&self.bandwidth(*a).unwrap_or(0.0)));
        for &node in &candidates {
            match self.fetch(node, run, object, size, dest).await {
                Ok(_) => return Ok(()),
                Err(e) => last = e,
            }
        }
        // Every known holder failed (gone, or freed it). Replicas live on the
        // writer's successor (see Replicate), so try each holder's successor
        // first, then a few other live members (membership may have moved).
        let mut live: Vec<u64> = self.live_others().iter().map(|p| p.member.node_id).collect();
        live.sort_unstable();
        let succ = |h: u64| live.iter().copied().find(|&n| n > h).or_else(|| live.first().copied());
        let mut order: Vec<u64> = candidates.iter().filter_map(|&h| succ(h)).collect();
        order.extend(live.iter().copied());
        let mut tried = candidates;
        for node in order {
            if tried.contains(&node) || node == self.me.node_id {
                continue;
            }
            if tried.len() >= REPLICA_SEARCH {
                break;
            }
            tried.push(node);
            if self.fetch(node, run, object, size, dest).await.is_ok() {
                return Ok(());
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
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        if !r.code && r.len != 1 {
            self.0.mesh.note_served(&r.object_id); // not a size probe: a copy is on its way
        }
        let path = if r.code { self.0.code.path(&r.object_id) } else { self.0.objects.path(&r.run, &r.object_id) };
        let chunks =
            crate::stores::read_chunks(&path, r.offset, r.len, FETCH_CHUNK).await.map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found(e.to_string()),
                _ => Status::invalid_argument(e.to_string()),
            })?;
        let stream = tokio_stream::StreamExt::map(tokio_stream::wrappers::ReceiverStream::new(chunks), |c| {
            c.map(|c| pb::FetchChunk { data: c.data, total: c.total, checksum: c.checksum, object_size: c.object_size })
                .map_err(|e| Status::internal(e.to_string()))
        });
        Ok(Response::new(Box::pin(stream)))
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
        let from = r.from_node;
        self.0.mesh.swim_in(r.updates);
        self.0.core.send(CoreMsg::Load { from, reply }).await.map_err(|_| Status::unavailable("stopping"))?;
        // Liveness is the process answering. A peer whose core is not running
        // yet (still starting: joining, opening its groups) answers anyway,
        // marked draining so nothing is placed on it until it reports load.
        let mut p = match tokio::time::timeout(LOAD_WAIT, rx).await {
            Ok(r) => r.map_err(|_| Status::unavailable("stopping"))?,
            Err(_) => pb::PingReply { draining: true, ..Default::default() },
        };
        if r.relay_to != 0 {
            p.relay_ok = self.0.mesh.ping(r.relay_to, RELAY_TIMEOUT).await.is_ok();
        }
        p.node_id = self.0.mesh.me.node_id;
        p.incarnation = self.0.mesh.me.incarnation;
        p.leading = self.0.router.get().map(|r| r.leading()).unwrap_or_default();
        p.updates = self.0.mesh.swim_out();
        Ok(Response::new(p))
    }

    async fn put_code(&self, req: Request<pb::MeshPutCode>) -> Result<Response<pb::PutCodeReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        self.0.code.put(&r.digest, &r.code).map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(pb::PutCodeReply { stored: true }))
    }

    async fn forget(&self, req: Request<pb::ForgetRequest>) -> Result<Response<pb::ForgetReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let _ = self.0.core.send(CoreMsg::Forget(r)).await;
        Ok(Response::new(pb::ForgetReply {}))
    }

    async fn push_logs(&self, req: Request<pb::PushLogsRequest>) -> Result<Response<pb::PushLogsReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let _ = self.0.core.send(CoreMsg::RemoteLogs { run: r.run, lines: r.lines }).await;
        Ok(Response::new(pb::PushLogsReply {}))
    }

    /// Keep a copy of another peer's object. With an in-memory store the copy
    /// goes to the spill directory on disk behind a symlink (the layout of a
    /// spilled object): it costs no memory, readers and Fetch follow the link,
    /// and deleting the object or its run removes it as for any spilled one.
    async fn replicate(&self, req: Request<pb::ReplicateRequest>) -> Result<Response<pb::ReplicateReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        if r.run.is_empty() || r.object_id.len() != 16 {
            return Err(Status::invalid_argument("replicate names a run and a 16-byte object id"));
        }
        let (mesh, objects) = (self.0.mesh.clone(), self.0.objects.clone());
        tokio::spawn(async move {
            let dest = objects.path(&r.run, &r.object_id);
            if tokio::fs::symlink_metadata(&dest).await.is_ok() {
                return;
            }
            let disk = objects.spill_dir().map(|d| d.join(hex(&r.run)).join(hex(&r.object_id)));
            let got = mesh.fetch(r.from_node, &r.run, &r.object_id, r.size, disk.as_deref().unwrap_or(&dest)).await;
            if let Err(e) = got {
                tracing::debug!("replica of {} from node {}: {e}", hex(&r.object_id), r.from_node);
                return;
            }
            if let Some(disk) = disk {
                let link = dest.with_extension("part-replica");
                let _ = tokio::fs::remove_file(&link).await;
                let made = async {
                    tokio::fs::create_dir_all(dest.parent().expect("object paths have a run directory")).await?;
                    tokio::fs::symlink(&disk, &link).await?;
                    tokio::fs::rename(&link, &dest).await
                };
                if let Err(e) = made.await {
                    tracing::debug!("replica of {}: cannot link it: {e}", hex(&r.object_id));
                    let _ = tokio::fs::remove_file(&disk).await;
                }
            }
        });
        Ok(Response::new(pb::ReplicateReply {}))
    }
}
