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
        req.metadata_mut().insert(LEVEL_HEADER, chimaera_proto::PEER_LEVEL.into());
        Ok(req)
    }
}

/// The peer protocol level a mesh call carries (see `chimaera_proto::PEER_LEVEL`).
const LEVEL_HEADER: &str = "chimaera-level";

/// Refuses calls that carry neither this cluster's token nor, from clients,
/// a driver credential issued from it (see `crate::auth`).
#[derive(Clone)]
pub struct CheckToken {
    verifier: Option<crate::auth::Verifier>,
    /// Callers are peers, which must present the token itself and run at
    /// this peer level.
    peers: bool,
}

impl CheckToken {
    /// For the mesh, whose callers are peers.
    pub fn peers(token: Option<&str>) -> CheckToken {
        CheckToken { verifier: token.map(crate::auth::Verifier::new), peers: true }
    }

    /// For drivers and the CLI on other machines; `Hello` checks their wire
    /// version. Each request gets its `auth::Caller`.
    pub fn clients(token: Option<&str>) -> CheckToken {
        CheckToken { peers: false, ..CheckToken::peers(token) }
    }
}

impl tonic::service::Interceptor for CheckToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let level = req.metadata().get(LEVEL_HEADER).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u32>().ok());
        if self.peers && level != Some(chimaera_proto::PEER_LEVEL) {
            let theirs = level.map_or_else(|| "a release without levels".to_string(), |l| format!("level {l}"));
            return Err(Status::failed_precondition(format!(
                "a peer at {theirs} called this peer at level {}: every peer of a cluster must run the same \
                 release (upgrade by restarting the peers onto the new one)",
                chimaera_proto::PEER_LEVEL
            )));
        }
        let Some(verifier) = &self.verifier else {
            req.extensions_mut().insert(crate::auth::Caller::Admin); // --insecure: anyone is trusted
            return Ok(req);
        };
        let bearer =
            req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|s| s.strip_prefix("Bearer "));
        let caller = match bearer {
            Some(b) if self.peers => verifier.is_token(b).then_some(crate::auth::Caller::Admin),
            Some(b) => verifier.caller(b),
            None => None,
        };
        let Some(caller) = caller else {
            return Err(Status::unauthenticated("missing or wrong cluster token or driver credential"));
        };
        req.extensions_mut().insert(caller);
        Ok(req)
    }
}

const MAX_MESSAGE: usize = 256 << 20;
const FETCH_CHUNK: usize = 4 << 20;
/// Objects this large come over several streams at once (the guides measured
/// 8-10x from multiple connections on large transfers).
const PARALLEL_FETCH: u64 = 1 << 20;
/// Peers asked for an object (holders included) before it counts as lost.
const REPLICA_SEARCH: usize = 8;
const FETCH_RANGES: u64 = 8;
/// TCP connections per peer that bulk fetches spread over (see channel_lane).
const FETCH_LANES: usize = 4;
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
    /// Which peer holds the parts of results fetched here (see parts).
    parts: crate::parts::Hints,
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
            parts: crate::parts::Hints::default(),
            served: Mutex::new(HashMap::new()),
            received: Mutex::new(Vec::new()),
            swim: Mutex::new(Swim::new(me_node, swim_inc(me_inc), me_node, swim_config())),
            epoch: Instant::now(),
        })
    }

    fn channel(&self, addr: &str) -> Result<Rpc, std::io::Error> {
        self.channel_lane(addr, 0)
    }

    /// A connection to `addr`: lane 0 is the shared one; bulk transfers
    /// spread over lanes 1..=FETCH_LANES, each its own TCP connection, since
    /// one HTTP/2 connection carries all its streams on one socket.
    fn channel_lane(&self, addr: &str, lane: usize) -> Result<Rpc, std::io::Error> {
        let key = if lane == 0 { addr.to_string() } else { format!("{addr}#{lane}") };
        let mut chans = self.channels.lock().expect("lock");
        let ch = match chans.get(&key) {
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
                    // Objects stream over these channels: fixed windows sized
                    // for bulk transfer. (hyper's adaptive window would override
                    // them and start from HTTP/2's 64 KiB, which holds one stream
                    // to about 250 MB/s at this RTT.)
                    .initial_stream_window_size(8 << 20)
                    .initial_connection_window_size(32 << 20)
                    .connect_lazy();
                chans.insert(key, c.clone());
                c
            }
        };
        Ok(pb::mesh_client::MeshClient::with_interceptor(ch, self.auth.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE))
    }

    fn rpc_lane(&self, node: u64, lane: usize) -> Result<Rpc, std::io::Error> {
        let addr = self
            .members
            .read()
            .expect("lock")
            .get(&node)
            .map(|p| p.member.addr.clone())
            .ok_or_else(|| std::io::Error::other(format!("unknown node {node}")))?;
        self.channel_lane(&addr, lane)
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
        crate::counters::PEERS_DECLARED_DEAD.add(1);
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

    /// `node`'s copy of a run's output (see `runlogs::serve`).
    pub async fn logs(&self, node: u64, logs: pb::LogsRequest) -> std::io::Result<pb::LogsReply> {
        let wait = Duration::from_millis(u64::from(logs.wait_ms.min(30_000)));
        let req = pb::MeshLogsRequest { cluster: self.cluster.clone(), logs: Some(logs) };
        let mut c = self.rpc(node)?;
        let r = tokio::time::timeout(CALL_TIMEOUT + wait, c.logs(req)).await.map_err(std::io::Error::other)?;
        Ok(r.map_err(status_err)?.into_inner())
    }

    pub async fn forget(&self, node: u64, mut req: pb::ForgetRequest) -> std::io::Result<()> {
        req.cluster = self.cluster.clone();
        let mut c = self.rpc(node)?;
        tokio::time::timeout(CALL_TIMEOUT, c.forget(req)).await.map_err(std::io::Error::other)?.map_err(status_err)?;
        Ok(())
    }

    /// Ask `node` to keep a copy of an object this peer stores (it pulls the
    /// bytes from here, to its disk).
    /// Ask `node` to keep a copy of `object`. With `wait`, the answer comes
    /// once the copy is made, so an error means there is no copy.
    pub async fn replicate(&self, node: u64, run: &[u8], object: &[u8], size: u64, wait: bool) -> std::io::Result<()> {
        let req = pb::ReplicateRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            size,
            from_node: self.me.node_id,
            wait,
        };
        let mut c = self.rpc(node)?;
        // A waited copy is one transfer: allow it 25 MB/s at worst.
        let limit = if wait { CALL_TIMEOUT + Duration::from_secs_f64(size as f64 / 25e6) } else { CALL_TIMEOUT };
        tokio::time::timeout(limit, c.replicate(req))
            .await
            .map_err(std::io::Error::other)?
            .map_err(status_err)?;
        Ok(())
    }

    /// What `node`'s object store holds (objplane::inventory, as JSON).
    pub async fn inventory(&self, node: u64) -> std::io::Result<String> {
        let mut c = self.rpc(node)?;
        let req = pb::InventoryRequest { cluster: self.cluster.clone() };
        let r = tokio::time::timeout(CALL_TIMEOUT, c.inventory(req))
            .await
            .map_err(std::io::Error::other)?
            .map_err(status_err)?;
        Ok(r.into_inner().json)
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

    /// The groups the peer at `addr` leads, asked without gossip: for a peer
    /// still starting, before it is a member.
    pub async fn leads(&self, addr: &str, timeout: Duration) -> std::io::Result<Vec<u32>> {
        let mut c = self.channel(addr)?;
        let req = pb::PingRequest { cluster: self.cluster.clone(), ..Default::default() };
        let r = tokio::time::timeout(timeout, c.ping(req)).await.map_err(std::io::Error::other)?;
        Ok(r.map_err(status_err)?.into_inner().leading)
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
            got = self.fetch_range(node, run, object, 0, 0, &tmp, 0).await;
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
            parts
                .iter()
                .enumerate()
                .map(|(i, &(off, len))| self.fetch_range(node, run, object, off, len, tmp, 1 + i % FETCH_LANES)),
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
    #[allow(clippy::too_many_arguments)]
    async fn fetch_range(
        &self,
        node: u64,
        run: &[u8],
        object: &[u8],
        offset: u64,
        len: u64,
        path: &Path,
        lane: usize,
    ) -> std::io::Result<u64> {
        let mut c = self.rpc_lane(node, lane)?;
        let req = pb::FetchRequest {
            cluster: self.cluster.clone(),
            run: run.to_vec(),
            object_id: object.to_vec(),
            code: false,
            offset,
            len,
        };
        let mut stream = c.fetch(req).await.map_err(status_err)?.into_inner();
        // A blocking writer checks and writes each chunk (pwrite at its offset)
        // while the next one arrives: hashing and disk work stay off the async
        // workers and overlap the network.
        let (wtx, mut wrx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>(4);
        let target = path.to_path_buf();
        let writer = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&target)?;
            let mut got = 0u64;
            while let Some((data, checksum)) = wrx.blocking_recv() {
                if !checksum.is_empty() && blake3::hash(&data).as_bytes()[..16] != checksum[..] {
                    return Err(std::io::Error::other(format!(
                        "corrupt chunk at byte {} from node {node}",
                        offset + got
                    )));
                }
                f.write_all_at(&data, offset + got)?;
                got += data.len() as u64;
            }
            Ok(got)
        });
        let mut total = None;
        let mut received = Ok(());
        loop {
            match stream.message().await {
                Ok(Some(chunk)) => {
                    total.get_or_insert(chunk.total);
                    if wtx.send((chunk.data, chunk.checksum)).await.is_err() {
                        break; // the writer stopped: its error says why
                    }
                }
                Ok(None) => break,
                Err(s) => {
                    received = Err(status_err(s));
                    break;
                }
            }
        }
        drop(wtx);
        let got = writer.await.map_err(std::io::Error::other)??;
        received?;
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
        // A part of a result of several values is asked for by id alone: it is
        // where that result came from (see parts).
        let hinted = if holders.is_empty() { self.parts.holder(run, object) } else { Vec::new() };
        let holders = if holders.is_empty() { &hinted[..] } else { holders };
        let gate = self.inflight.lock().expect("lock").entry(dest.clone()).or_default().clone();
        let held = gate.lock().await;
        let result = if dest.exists() {
            Ok(())
        } else {
            self.fetch_from(run, object, size, holders, &dest).await.map(|node| self.parts.learn(run, &dest, node))
        };
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
    ) -> std::io::Result<u64> {
        let mut candidates: Vec<u64> = holders.iter().copied().filter(|n| *n != self.me.node_id).collect();
        if candidates.is_empty() {
            candidates = self.live_others().iter().map(|p| p.member.node_id).collect();
        }
        let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no peer holds the object");
        // The fastest known holder first.
        candidates.sort_by(|a, b| self.bandwidth(*b).unwrap_or(0.0).total_cmp(&self.bandwidth(*a).unwrap_or(0.0)));
        for &node in &candidates {
            match self.fetch(node, run, object, size, dest).await {
                Ok(_) => return Ok(node),
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
                return Ok(node);
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

    fn send_snapshot(&self, target: u64, addr: &str, group: u32, header: Vec<u8>, file: std::fs::File) -> RpcFuture {
        let client = self.0.channel(addr);
        let cluster = self.0.cluster.clone();
        Box::pin(async move {
            use futures_util::StreamExt;
            let mut c = client?;
            let size = file.metadata()?.len();
            let first = pb::SnapshotChunk { cluster, group, header, size, data: Vec::new() };
            let rest = snapshot_chunks(tokio::fs::File::from_std(file), FETCH_CHUNK)
                .map(|data| pb::SnapshotChunk { data, ..Default::default() });
            let r = c
                .install_snapshot(futures_util::stream::once(async move { first }).chain(rest))
                .await
                .map_err(|s| std::io::Error::other(format!("node {target}: {}", s.message())))?;
            Ok(r.into_inner().body)
        })
    }
}

/// `file`'s bytes in pieces of at most `chunk`. A read error ends the stream
/// early; the receiver then sees fewer bytes than the header announced.
fn snapshot_chunks(file: tokio::fs::File, chunk: usize) -> impl tokio_stream::Stream<Item = Vec<u8>> + Send + 'static {
    futures_util::stream::unfold(file, move |mut f| async move {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; chunk];
        match f.read(&mut buf).await {
            Ok(0) | Err(_) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((buf, f))
            }
        }
    })
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
        let body =
            chimaera_raft::handle_rpc(&group, kind, &r.body).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::RaftReply { body }))
    }

    async fn install_snapshot(
        &self,
        req: Request<Streaming<pb::SnapshotChunk>>,
    ) -> Result<Response<pb::RaftReply>, Status> {
        use tokio::io::AsyncWriteExt;
        let mut chunks = req.into_inner();
        let first = chunks.message().await?.ok_or_else(|| Status::invalid_argument("an empty snapshot stream"))?;
        self.0.check(&first.cluster)?;
        // Unique per transfer: two installs of one group may overlap.
        let path = self.0.scratch.join(format!("incoming-{}-{}.snap", first.group, crate::router::now_ns()));
        let written = async {
            let mut f = tokio::fs::File::create(&path).await?;
            let mut n = 0u64;
            while let Some(chunk) = chunks.message().await.map_err(std::io::Error::other)? {
                f.write_all(&chunk.data).await?;
                n += chunk.data.len() as u64;
            }
            f.flush().await?;
            Ok::<u64, std::io::Error>(n)
        }
        .await;
        let reply = match written {
            Ok(n) if n == first.size => {
                let group = match self.0.router()?.hosted(first.group) {
                    Some(g) => g,
                    None => self.0.starter.ensure(first.group).await.map_err(|e| Status::unavailable(e.to_string()))?,
                };
                let r = chimaera_raft::handle_snapshot(&group, &first.header, &path).await;
                tracing::info!(group = first.group, bytes = n, "installed a snapshot streamed from a peer");
                r.map_err(|e| Status::internal(e.to_string()))
            }
            Ok(n) => Err(Status::data_loss(format!("the snapshot stream ended after {n} of {} bytes", first.size))),
            Err(e) => Err(Status::internal(e.to_string())),
        };
        let _ = tokio::fs::remove_file(&path).await;
        Ok(Response::new(pb::RaftReply { body: reply? }))
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
        if r.list_runs_group > 0 {
            match router.runs_of(r.list_runs_group) {
                Some(runs) => reply.runs = runs,
                None => reply.error = "group not hosted here".into(),
            }
            return Ok(Response::new(reply));
        }
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

    async fn logs(&self, req: Request<pb::MeshLogsRequest>) -> Result<Response<pb::LogsReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let logs = r.logs.unwrap_or_default();
        crate::runlogs::serve(&self.0.code, &logs)
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(format!("reading run output: {e}")))
    }

    async fn push_logs(&self, req: Request<pb::PushLogsRequest>) -> Result<Response<pb::PushLogsReply>, Status> {
        let r = req.into_inner();
        self.0.check(&r.cluster)?;
        let _ = self.0.core.send(CoreMsg::RemoteLogs { run: r.run, lines: r.lines }).await;
        Ok(Response::new(pb::PushLogsReply {}))
    }

    async fn inventory(&self, req: Request<pb::InventoryRequest>) -> Result<Response<pb::InventoryReply>, Status> {
        self.0.check(&req.into_inner().cluster)?;
        let dir = self.0.objects.dir().to_path_buf();
        let json = tokio::task::spawn_blocking(move || crate::objplane::inventory(&dir).to_string())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::InventoryReply { json }))
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
        let (mesh, objects, wait) = (self.0.mesh.clone(), self.0.objects.clone(), r.wait);
        let copy = async move {
            let dest = objects.path(&r.run, &r.object_id);
            if tokio::fs::symlink_metadata(&dest).await.is_ok() {
                return Ok(());
            }
            let disk = objects.spill_path(&r.run, &r.object_id);
            mesh.fetch(r.from_node, &r.run, &r.object_id, r.size, disk.as_deref().unwrap_or(&dest))
                .await
                .map_err(|e| format!("replica of {} from node {}: {e}", hex(&r.object_id), r.from_node))?;
            if let Some(disk) = disk {
                let link = dest.with_extension("part-replica");
                let _ = tokio::fs::remove_file(&link).await;
                let made = async {
                    tokio::fs::create_dir_all(dest.parent().expect("object paths have a run directory")).await?;
                    tokio::fs::symlink(&disk, &link).await?;
                    tokio::fs::rename(&link, &dest).await
                };
                if let Err(e) = made.await {
                    let _ = tokio::fs::remove_file(&disk).await;
                    return Err(format!("replica of {}: cannot link it: {e}", hex(&r.object_id)));
                }
            }
            Ok(())
        };
        if wait {
            copy.await.map_err(Status::unavailable)?;
        } else {
            tokio::spawn(async move {
                if let Err(e) = copy.await {
                    tracing::warn!("{e}");
                }
            });
        }
        Ok(Response::new(pb::ReplicateReply {}))
    }
}

#[cfg(test)]
mod tests {
    use tonic::service::Interceptor;

    #[test]
    fn a_peer_from_another_release_is_refused() {
        let mut check = super::CheckToken::peers(Some("secret"));
        let mut add = super::AddToken(Some("Bearer secret".parse().unwrap()));
        let ok = add.call(tonic::Request::new(())).unwrap();
        assert!(check.call(ok).is_ok());
        let mut old = tonic::Request::new(());
        old.metadata_mut().insert("authorization", "Bearer secret".parse().unwrap());
        let e = check.call(old).unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition);
        assert!(e.message().contains("same release"), "{}", e.message());
        let mut other = add.call(tonic::Request::new(())).unwrap();
        other.metadata_mut().insert(super::LEVEL_HEADER, (chimaera_proto::PEER_LEVEL + 1).into());
        assert_eq!(check.call(other).unwrap_err().code(), tonic::Code::FailedPrecondition);
        // Drivers carry no level: only their token is checked here.
        let mut clients = super::CheckToken::clients(Some("secret"));
        let mut driver = tonic::Request::new(());
        driver.metadata_mut().insert("authorization", "Bearer secret".parse().unwrap());
        assert!(clients.call(driver).is_ok());
        assert_eq!(clients.call(tonic::Request::new(())).unwrap_err().code(), tonic::Code::Unauthenticated);
        // A driver credential is its tenant on the client listener, and no peer on the mesh.
        let bearer = format!("Bearer {}", crate::auth::credential("secret", "alice"));
        let mut with = |b: &str| {
            let mut r = add.call(tonic::Request::new(())).unwrap();
            r.metadata_mut().insert("authorization", b.parse().unwrap());
            r
        };
        let seen = clients.call(with(&bearer)).unwrap();
        assert_eq!(seen.extensions().get::<crate::auth::Caller>(), Some(&crate::auth::Caller::Tenant("alice".into())));
        assert_eq!(check.call(with(&bearer)).unwrap_err().code(), tonic::Code::Unauthenticated);
        let admin = clients.call(with("Bearer secret")).unwrap();
        assert_eq!(admin.extensions().get::<crate::auth::Caller>(), Some(&crate::auth::Caller::Admin));
    }

    #[tokio::test]
    async fn a_snapshot_goes_out_in_pieces_no_larger_than_a_chunk() {
        use futures_util::StreamExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.snap");
        let bytes: Vec<u8> = (0..(10 << 20) + 17).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let file = tokio::fs::File::open(&path).await.unwrap();
        let chunks: Vec<Vec<u8>> = super::snapshot_chunks(file, super::FETCH_CHUNK).collect().await;
        assert!(chunks.iter().all(|c| !c.is_empty() && c.len() <= super::FETCH_CHUNK));
        assert_eq!(chunks.concat(), bytes);
    }
}
