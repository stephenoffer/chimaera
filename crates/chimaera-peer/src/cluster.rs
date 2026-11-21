//! Running a peer as a member of a cluster.
//!
//! Formation: the founders are the peers listed in `--seeds`. Each founder
//! exchanges member records with the others until it knows all of them, then
//! computes the same plan from the same set: group 0 (the directory) on up to
//! five founders, and `data_groups` shard groups on three founders each, chosen
//! by rendezvous hashing. The lowest node id among a group's voters initializes
//! it; the others join through raft. A peer that is not a founder asks a seed
//! for the plan and joins as a spare that runs work and can replace voters.
//!
//! Liveness: every peer pings every other peer. A peer silent for
//! `DEAD_AFTER` is marked dead locally (no new placements), and the leader of
//! each data group retires its incarnation there, which fences its grants and
//! requeues them. A voter dead for `REPLACE_AFTER` is replaced by a live
//! non-voter, so the group can lose another member later.

use crate::core::CoreMsg;
use crate::mesh::{CheckToken, GroupStarter, MeshClient, MeshServer, MeshService, MeshTransport};
use crate::router::Router;
use crate::runtime::{self, Node};
use crate::stores::write_atomic;
use chimaera_core::pb;
use chimaera_raft::{BasicNode, DiskDurability, Group, RaftHost};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

const PLAN_FILE: &str = "cluster.plan";
const HOSTED_FILE: &str = "hosted-groups";
const PING_EVERY: Duration = Duration::from_millis(300);
const PING_TIMEOUT: Duration = Duration::from_millis(800);
pub const DEAD_AFTER: Duration = Duration::from_secs(3);
const REPLACE_AFTER: Duration = Duration::from_secs(10);
const DIRECTORY_VOTERS: usize = 5;
const GROUP_VOTERS: usize = 3;

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub dir: PathBuf,
    pub cluster: String,
    pub listen: SocketAddr,
    /// Address other peers dial (defaults to `listen`).
    pub advertise: String,
    /// Founders' addresses; this peer is a founder if its address is listed.
    pub seeds: Vec<String>,
    pub data_groups: u32,
    pub cpus: Option<f64>,
    /// Shared secret every mesh call must carry (None: peers do not authenticate).
    pub token: Option<String>,
}

/// Deterministic plan from the founders' member records.
pub fn compute_plan(cluster: &str, data_groups: u32, mut founders: Vec<pb::Member>) -> pb::ClusterPlan {
    founders.sort_by_key(|m| m.node_id);
    let directory_voters = founders.iter().take(DIRECTORY_VOTERS).map(|m| m.node_id).collect();
    let groups = (1..=data_groups)
        .map(|g| {
            let mut ranked: Vec<(u64, u64)> = founders
                .iter()
                .map(|m| {
                    let h = blake3::hash(&[g.to_le_bytes().as_slice(), &m.node_id.to_le_bytes()].concat());
                    (u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes")), m.node_id)
                })
                .collect();
            ranked.sort();
            let mut voters: Vec<u64> = ranked.into_iter().take(GROUP_VOTERS).map(|(_, n)| n).collect();
            voters.sort();
            pb::GroupVoters { group: g, voters }
        })
        .collect();
    pb::ClusterPlan { cluster: cluster.to_string(), data_groups, directory_voters, groups, founders }
}

fn voters_in(plan: &pb::ClusterPlan, group: u32) -> Vec<u64> {
    if group == 0 {
        return plan.directory_voters.clone();
    }
    plan.groups.iter().find(|g| g.group == group).map(|g| g.voters.clone()).unwrap_or_default()
}

/// Starts groups on this peer: planned ones at boot, others when raft
/// traffic for them arrives (this peer was added as a learner).
struct Groups {
    host: RaftHost,
    router: Arc<Router>,
    mesh: Arc<MeshClient>,
    core: mpsc::Sender<CoreMsg>,
    plan: pb::ClusterPlan,
    dir: PathBuf,
    lock: Mutex<()>,
}

impl Groups {
    async fn start(&self, id: u32, bootstrap: bool) -> io::Result<Group> {
        let _guard = self.lock.lock().await;
        if let Some(g) = self.router.hosted(id) {
            return Ok(g);
        }
        let voters = voters_in(&self.plan, id);
        let members: BTreeMap<u64, BasicNode> =
            voters.iter().filter_map(|n| self.mesh.member(*n).map(|p| (*n, BasicNode::new(p.member.addr)))).collect();
        let group = self.host.start_group(id, members, bootstrap).await?;
        self.router.add_group(group.clone());
        self.persist_hosted()?;
        watch_leadership(group.clone(), self.router.clone(), self.mesh.clone(), self.core.clone());
        Ok(group)
    }

    fn persist_hosted(&self) -> io::Result<()> {
        let ids: Vec<String> = self.router.hosted_groups().iter().map(|g| g.id.to_string()).collect();
        write_atomic(&self.dir.join(HOSTED_FILE), ids.join("\n").as_bytes())
    }
}

#[tonic::async_trait]
impl GroupStarter for Groups {
    async fn ensure(&self, group: u32) -> io::Result<Group> {
        if group > self.plan.data_groups {
            return Err(io::Error::other(format!("group {group} is not in this cluster's plan")));
        }
        self.start(group, false).await
    }
}

/// Late-bound group starter: the mesh server answers Join and Ping while the
/// cluster forms, before groups exist.
#[derive(Default)]
struct StarterSlot(RwLock<Option<Arc<Groups>>>);

#[tonic::async_trait]
impl GroupStarter for StarterSlot {
    async fn ensure(&self, group: u32) -> io::Result<Group> {
        let groups = self.0.read().expect("lock").clone();
        match groups {
            Some(g) => g.ensure(group).await,
            None => Err(io::Error::other("peer is still forming the cluster")),
        }
    }
}

/// Report leadership changes to the core and keep hints and voter lists current.
fn watch_leadership(group: Group, router: Arc<Router>, mesh: Arc<MeshClient>, core: mpsc::Sender<CoreMsg>) {
    let me = router.node_id();
    tokio::spawn(async move {
        let mut watch = group.watch();
        let mut was = false;
        loop {
            let (leader, voters) = watch.view();
            if let Some(l) = leader {
                router.note_leader(group.id, l);
            }
            if !voters.is_empty() {
                mesh.set_voters(group.id, voters);
            }
            let now = leader == Some(me);
            if now != was {
                was = now;
                router.set_leading(group.id, now);
                if core.send(CoreMsg::Leading { group: group.id, leading: now }).await.is_err() {
                    return;
                }
            }
            if !watch.changed().await {
                return;
            }
        }
    });
}

/// Learn every founder's record (or the plan, if the cluster already formed).
async fn form(mesh: &MeshClient, cfg: &ClusterConfig) -> io::Result<pb::ClusterPlan> {
    let founder = cfg.seeds.contains(&cfg.advertise);
    let started = Instant::now();
    let mut last_report = Instant::now();
    loop {
        let mut refusals = Vec::new();
        for seed in cfg.seeds.iter().filter(|s| **s != cfg.advertise) {
            match mesh.join(seed, cfg.seeds.len() as u32).await {
                Ok(reply) => {
                    for m in reply.members {
                        mesh.upsert(m);
                    }
                    if let Some(plan) = reply.plan {
                        return Ok(plan);
                    }
                }
                Err(e) => refusals.push(format!("{seed}: {e}")),
            }
        }
        if last_report.elapsed() > Duration::from_secs(5) && !refusals.is_empty() {
            last_report = Instant::now();
            tracing::warn!(?refusals, "seeds did not answer or refused this peer");
        }
        if founder {
            let members = mesh.members();
            let founders: Vec<pb::Member> =
                members.iter().filter(|p| cfg.seeds.contains(&p.member.addr)).map(|p| p.member.clone()).collect();
            if founders.len() == cfg.seeds.len() {
                return Ok(compute_plan(&cfg.cluster, cfg.data_groups, founders));
            }
        }
        if founder && started.elapsed() > Duration::from_secs(5) && started.elapsed().as_millis() % 5000 < 250 {
            let known: Vec<String> = mesh.members().iter().map(|p| p.member.addr.clone()).collect();
            tracing::info!(?known, seeds = ?cfg.seeds, "waiting for the other founders");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Run a cluster peer until shutdown.
pub async fn run_cluster(cfg: ClusterConfig) -> io::Result<()> {
    let (node, _lock) = Node::open(&cfg.dir, cfg.cpus)?;
    let me = pb::Member {
        peer: node.identity.peer.to_vec(),
        incarnation: node.identity.incarnation,
        node_id: node.identity.node_id(),
        addr: cfg.advertise.clone(),
        cpus: node.capacity,
        memory_bytes: node.shape.memory_bytes,
        gpus: node.shape.gpus.iter().map(|g| g.uuid.clone()).collect(),
    };
    let mesh = Arc::new(MeshClient::new(cfg.cluster.clone(), me.clone(), cfg.token.as_deref())?);
    if cfg.token.is_none() {
        tracing::warn!("no cluster token: anyone who can reach {} can join this cluster", cfg.listen);
    }
    let plan_slot = Arc::new(RwLock::new(None));
    let starter = Arc::new(StarterSlot::default());
    let server = Arc::new(MeshServer {
        mesh: mesh.clone(),
        router: std::sync::OnceLock::new(),
        core: node.core_tx.clone(),
        objects: node.objects.clone(),
        code: node.code.clone(),
        starter: starter.clone(),
        plan: plan_slot.clone(),
        scratch: cfg.dir.join("raft"),
        next_session: AtomicU64::new(1),
    });
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let svc = tonic::service::interceptor::InterceptedService::new(
        pb::mesh_server::MeshServer::new(MeshService(server.clone()))
            .max_decoding_message_size(256 << 20)
            .max_encoding_message_size(256 << 20),
        CheckToken::new(cfg.token.as_deref()),
    );
    tokio::spawn(async move {
        let r = tonic::transport::Server::builder()
            .tcp_nodelay(true)
            .http2_adaptive_window(Some(true))
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
        if let Err(e) = r {
            tracing::error!("mesh server stopped: {e}");
        }
    });

    // The plan: from disk after a restart, else formed with the founders.
    let plan_path = cfg.dir.join(PLAN_FILE);
    let plan = match std::fs::read(&plan_path).ok().and_then(|b| pb::ClusterPlan::decode(b.as_slice()).ok()) {
        Some(p) => p,
        None => {
            let p = form(&mesh, &cfg).await?;
            write_atomic(&plan_path, &p.encode_to_vec())?;
            p
        }
    };
    if plan.cluster != cfg.cluster {
        return Err(io::Error::other(format!(
            "{} belongs to cluster {:?}, not {:?}",
            cfg.dir.display(),
            plan.cluster,
            cfg.cluster
        )));
    }
    for m in &plan.founders {
        if m.node_id != me.node_id {
            mesh.upsert(m.clone());
        }
    }
    mesh.set_voters(0, plan.directory_voters.clone());
    for g in &plan.groups {
        mesh.set_voters(g.group, g.voters.clone());
    }
    *plan_slot.write().expect("lock") = Some(plan.clone());

    let (host, raft_events) =
        RaftHost::open(&cfg.dir.join("raft"), me.node_id, Arc::new(MeshTransport(mesh.clone())), DiskDurability::Os)?;
    let router = Arc::new(Router::cluster(node.identity, plan.data_groups, mesh.clone()));
    let _ = server.router.set(router.clone());
    let groups = Arc::new(Groups {
        host,
        router: router.clone(),
        mesh: mesh.clone(),
        core: node.core_tx.clone(),
        plan: plan.clone(),
        dir: cfg.dir.clone(),
        lock: Mutex::new(()),
    });
    *starter.0.write().expect("lock") = Some(groups.clone());

    // Groups this peer votes in by plan, plus any it joined later.
    let mut hosted: BTreeSet<u32> = std::fs::read_to_string(cfg.dir.join(HOSTED_FILE))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect();
    for g in 0..=plan.data_groups {
        if voters_in(&plan, g).contains(&me.node_id) {
            hosted.insert(g);
        }
    }
    for g in hosted {
        let voters = voters_in(&plan, g);
        let bootstrap = voters.iter().min() == Some(&me.node_id);
        groups.start(g, bootstrap).await?;
    }

    // Every vshard owned by its group (idempotent at epoch 1), and anything
    // the previous incarnation of this peer held fenced and requeued.
    for g in router.data_groups() {
        let vshards = router.vshards_of(g);
        let first = vshards[0] as u16;
        router
            .propose(first, pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards, epoch: 1 }))
            .await
            .map_err(|e| io::Error::other(format!("group {g} did not accept its vshards: {e}")))?;
        if node.identity.incarnation > 1 {
            let prev = pb::PeerInc { peer: node.identity.peer.to_vec(), inc: node.identity.incarnation - 1 };
            router
                .propose(first, pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(prev) }))
                .await
                .map_err(io::Error::other)?;
        }
    }
    tracing::info!(
        node = me.node_id,
        addr = %cfg.advertise,
        hosted = ?router.hosted_groups().iter().map(|g| g.id).collect::<Vec<_>>(),
        "cluster peer ready"
    );

    tokio::spawn(liveness(router.clone(), mesh.clone()));
    runtime::serve(node, router, raft_events, None).await
}

/// Ping every member; mark silent ones dead; as a group leader, retire dead
/// incarnations and replace dead voters.
async fn liveness(router: Arc<Router>, mesh: Arc<MeshClient>) {
    let me = mesh.me.node_id;
    let started = Instant::now();
    let mut dead_since: BTreeMap<u64, Instant> = BTreeMap::new();
    let mut tick = tokio::time::interval(PING_EVERY);
    loop {
        tick.tick().await;
        let peers: Vec<_> = mesh.members().into_iter().filter(|p| p.member.node_id != me).collect();
        let pings = peers.iter().map(|p| {
            let mesh = mesh.clone();
            let node = p.member.node_id;
            async move { (node, mesh.ping(node, PING_TIMEOUT).await.ok()) }
        });
        for (node, reply) in futures_util::future::join_all(pings).await {
            if reply.is_some() {
                mesh.note_ping(node, reply);
                dead_since.remove(&node);
            }
        }
        for p in mesh.members() {
            if p.member.node_id == me || p.dead {
                continue;
            }
            let silent = match p.last_ok {
                Some(t) => t.elapsed() > DEAD_AFTER,
                None => started.elapsed() > DEAD_AFTER * 3,
            };
            if !silent {
                continue;
            }
            let Some(m) = mesh.mark_dead(p.member.node_id) else { continue };
            tracing::warn!(node = m.node_id, addr = %m.addr, incarnation = m.incarnation, "peer is unreachable; retiring it");
            dead_since.insert(m.node_id, Instant::now());
            retire_everywhere(&router, &m);
        }
        replace_voters(&router, &mesh, &dead_since).await;
    }
}

/// Fence the incarnation in every data group this peer leads (each leader
/// does the same for its groups; the command is idempotent).
fn retire_everywhere(router: &Arc<Router>, m: &pb::Member) {
    for g in router.data_groups() {
        if !router.leads(g) {
            continue;
        }
        let first = router.vshards_of(g)[0] as u16;
        let peer = pb::PeerInc { peer: m.peer.clone(), inc: m.incarnation };
        let r = router.clone();
        tokio::spawn(async move {
            if let Err(e) =
                r.propose(first, pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(peer) })).await
            {
                tracing::warn!("cannot retire in group {g}: {e}");
            }
        });
    }
}

/// For each group this peer leads, swap voters dead longer than
/// `REPLACE_AFTER` for live members that do not vote in it yet.
async fn replace_voters(router: &Arc<Router>, mesh: &Arc<MeshClient>, dead_since: &BTreeMap<u64, Instant>) {
    for g in router.hosted_groups() {
        if !router.leads(g.id) {
            continue;
        }
        let voters: BTreeSet<u64> = g.view().1.into_iter().collect();
        let dead: Vec<u64> =
            voters.iter().copied().filter(|n| dead_since.get(n).is_some_and(|t| t.elapsed() > REPLACE_AFTER)).collect();
        if dead.is_empty() {
            continue;
        }
        let mut spares: Vec<_> =
            mesh.live_others().into_iter().filter(|p| !voters.contains(&p.member.node_id)).collect();
        spares.sort_by(|a, b| a.pressure().total_cmp(&b.pressure()));
        let mut next: BTreeSet<u64> = voters.clone();
        for (gone, spare) in dead.iter().zip(spares.iter()) {
            let id = spare.member.node_id;
            tracing::info!(group = g.id, gone, replacement = id, "replacing a dead voter");
            if let Err(e) = g.add_learner(id, &spare.member.addr).await {
                tracing::warn!(group = g.id, "cannot add learner {id}: {e}");
                continue;
            }
            next.remove(gone);
            next.insert(id);
        }
        if next != voters
            && let Err(e) = g.change_voters(next.clone()).await
        {
            tracing::warn!(group = g.id, "membership change failed: {e}");
        } else if next != voters {
            mesh.set_voters(g.id, next.into_iter().collect());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(n: u64) -> pb::Member {
        pb::Member { node_id: n, addr: format!("10.0.0.{n}:7000"), cpus: 4.0, ..Default::default() }
    }

    #[test]
    fn every_founder_computes_the_same_plan() {
        let a = compute_plan("c", 8, (1..=5).map(member).collect());
        let b = compute_plan("c", 8, (1..=5).rev().map(member).collect());
        assert_eq!(a, b);
        assert_eq!(a.directory_voters, vec![1, 2, 3, 4, 5]);
        for g in &a.groups {
            assert_eq!(g.voters.len(), 3);
            assert!(g.voters.windows(2).all(|w| w[0] < w[1]));
        }
        // Rendezvous hashing spreads groups over more than three peers.
        let used: BTreeSet<u64> = a.groups.iter().flat_map(|g| g.voters.iter().copied()).collect();
        assert!(used.len() > 3, "{used:?}");
    }

    #[test]
    fn small_clusters_use_every_founder() {
        let p = compute_plan("c", 4, (1..=2).map(member).collect());
        assert!(p.groups.iter().all(|g| g.voters == vec![1, 2]));
        assert_eq!(p.directory_voters, vec![1, 2]);
    }
}
