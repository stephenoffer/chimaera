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
//! Liveness: SWIM (chimaera-core::swim). Each period a peer probes a few
//! members (all of them in a small cluster, at most `PROBE_FANOUT` in a large
//! one) and gossips membership changes on every probe and reply. A member
//! that misses a direct probe is probed through relays; if none reaches it,
//! it is suspect (no new placements there) and, unless it refutes, dead after
//! the suspicion timeout. The leader of each data group then retires the dead
//! incarnation there, which fences its grants and requeues them. A voter dead
//! for `REPLACE_AFTER` is replaced by a live non-voter, so the group can lose
//! another member later.

use crate::core::CoreMsg;
use crate::mesh::{CheckToken, GroupStarter, MeshClient, MeshServer, MeshService, MeshTransport};
use crate::router::Router;
use crate::runtime::{self, Node};
use crate::stores::write_atomic;
use chimaera_core::pb;
use chimaera_core::swim::State as SwimState;
use chimaera_raft::{BasicNode, Group, RaftHost};
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
/// Addresses of the members this peer last knew, one per line: a restart
/// whose seeds are all gone rejoins through them.
const MEMBERS_FILE: &str = "members";

fn known_members(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(dir.join(MEMBERS_FILE)).unwrap_or_default().lines().map(str::to_string).collect()
}
const PING_EVERY: Duration = Duration::from_millis(100);
const PING_TIMEOUT: Duration = Duration::from_millis(800);
/// A member whose port refuses connections this long is dead (its process is gone).
const REFUSED_FOR: Duration = Duration::from_millis(1000);
/// Members probed per SWIM period: per-peer traffic stays bounded however
/// large the cluster grows.
const PROBE_FANOUT: usize = 8;
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
    /// Mutual TLS between peers (`--tls-dir`).
    pub tls: Option<crate::tls::Tls>,
    /// Where drivers on other machines connect (`--client-listen`); they must
    /// present the cluster token, over TLS when `tls` is set.
    pub client_listen: Option<SocketAddr>,
}

/// Deterministic plan from the founders' member records.
pub fn compute_plan(cluster: &str, data_groups: u32, mut founders: Vec<pb::Member>) -> pb::ClusterPlan {
    founders.sort_by_key(|m| m.node_id);
    let directory_voters = founders.iter().take(DIRECTORY_VOTERS).map(|m| m.node_id).collect();
    let groups = (1..=data_groups)
        .map(|g| pb::GroupVoters { group: g, voters: target_voters(g, &founders, GROUP_VOTERS) })
        .collect();
    pb::ClusterPlan { cluster: cluster.to_string(), data_groups, directory_voters, groups, founders }
}

/// The `n` members that should vote in group `group`, sorted: each member's
/// rank is a hash of (group, node), so groups spread over every member and a
/// member joining or leaving moves few seats. Members labelled with distinct
/// `zone`s are taken first, so a zone's loss leaves a majority where it can.
pub fn target_voters(group: u32, members: &[pb::Member], n: usize) -> Vec<u64> {
    let mut ranked: Vec<(u64, &pb::Member)> = members
        .iter()
        .map(|m| {
            let h = blake3::hash(&[group.to_le_bytes().as_slice(), &m.node_id.to_le_bytes()].concat());
            (u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes")), m)
        })
        .collect();
    ranked.sort_by_key(|(h, m)| (*h, m.node_id));
    let ranked: Vec<&pb::Member> = ranked.into_iter().map(|(_, m)| m).collect();
    let mut voters = crate::labels::spread(&ranked, n, &["zone"], None);
    voters.sort();
    voters
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

/// How long a restarting peer looks for leaders of its groups before keeping
/// state it may have lost (see `durability::guard`).
const OTHERS_LEAD_WAIT: Duration = Duration::from_secs(10);

/// Whether, for every group whose state is under `raft`, another voter is up
/// and leads it: then this peer's possibly stale copy is not needed. The
/// groups and whom to ask come from the stored memberships, the cluster plan,
/// the hosted-groups file and the seeds: memberships alone can be missing from
/// unsynced state, and an unknown group must never count as led.
fn others_lead(raft: &std::path::Path, cfg: &ClusterConfig) -> bool {
    let stored = chimaera_raft::stored_voters(raft).unwrap_or_else(|e| {
        tracing::warn!("could not read the stored raft voters ({e}); asking the cluster plan's peers");
        BTreeMap::new()
    });
    let plan = std::fs::read(cfg.dir.join(PLAN_FILE)).ok().and_then(|b| pb::ClusterPlan::decode(b.as_slice()).ok());
    let me = std::fs::read_to_string(cfg.dir.join("peer-id"))
        .ok()
        .and_then(|s| chimaera_core::ids::Id::parse_hex(s.trim()))
        .map(|peer| crate::stores::Identity { peer, incarnation: 0 }.node_id());
    let mut groups: BTreeSet<u32> = stored.keys().copied().collect();
    groups.extend(
        std::fs::read_to_string(cfg.dir.join(HOSTED_FILE))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok()),
    );
    let mut others: BTreeSet<String> = stored.values().flatten().map(|(_, addr)| addr.clone()).collect();
    others.extend(cfg.seeds.iter().cloned());
    if let Some(plan) = &plan {
        groups.extend((0..=plan.data_groups).filter(|g| me.is_some_and(|me| voters_in(plan, *g).contains(&me))));
        others.extend(plan.founders.iter().map(|m| m.addr.clone()));
    }
    others.extend(known_members(&cfg.dir));
    others.remove(&cfg.advertise);
    if groups.is_empty() {
        return true;
    }
    let me = pb::Member { addr: cfg.advertise.clone(), ..Default::default() };
    let Ok(mesh) =
        MeshClient::new(cfg.cluster.clone(), me, cfg.token.as_deref(), cfg.tls.as_ref().map(|t| t.client.clone()))
    else {
        return false;
    };
    // Called from the peer's (multi-threaded) runtime, before it serves.
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let deadline = tokio::time::Instant::now() + OTHERS_LEAD_WAIT;
            loop {
                let mut led = std::collections::BTreeSet::new();
                for addr in &others {
                    if let Ok(g) = mesh.leads(addr, Duration::from_secs(1)).await {
                        led.extend(g);
                    }
                }
                if groups.iter().all(|g| led.contains(g)) {
                    return true;
                }
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    })
}

/// Run a cluster peer until shutdown.
pub async fn run_cluster(cfg: ClusterConfig) -> io::Result<()> {
    let durability = crate::durability::choose()?;
    let (node, _lock) = Node::open_with(&cfg.dir, cfg.cpus, || {
        crate::durability::guard(&cfg.dir, durability, |raft| others_lead(raft, &cfg))
    })?;
    chimaera_raft::set_divergence_hook(crate::durability::on_divergence(cfg.dir.clone()));
    let me = pb::Member {
        peer: node.identity.peer.to_vec(),
        incarnation: node.identity.incarnation,
        node_id: node.identity.node_id(),
        addr: cfg.advertise.clone(),
        cpus: node.capacity,
        memory_bytes: node.shape.memory_bytes,
        gpus: node.shape.gpus.iter().map(|g| g.uuid.clone()).collect(),
        labels: crate::labels::own(&node.identity.peer.0).into_iter().collect(),
        host: std::env::var("CHIMAERA_MACHINE_ID")
            .ok()
            .filter(|m| !m.is_empty())
            .or_else(crate::durability::boot_id)
            .unwrap_or_default(),
        object_dir: node.objects.dir().to_string_lossy().into_owned(),
        raft_sync: crate::durability::advertised(durability) as i32,
        object_layout: crate::parts::OBJECT_LAYOUT,
    };
    let mesh = Arc::new(MeshClient::new(
        cfg.cluster.clone(),
        me.clone(),
        cfg.token.as_deref(),
        cfg.tls.as_ref().map(|t| t.client.clone()),
    )?);
    if cfg.token.is_none() {
        tracing::warn!("no cluster token: anyone who can reach {} can join this cluster", cfg.listen);
    }
    crate::objplane::replicate_new_objects(
        node.objects.dir().to_path_buf(),
        mesh.clone(),
        tokio::runtime::Handle::current(),
    );
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
    // The builder's tcp_nodelay applies only to listeners tonic binds itself:
    // accepted sockets need it set here, or replies (raft appends, forwarded
    // proposals) wait out Nagle against the caller's delayed ACK.
    let incoming = tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true));
    let svc = tonic::service::interceptor::InterceptedService::new(
        pb::mesh_server::MeshServer::new(MeshService(server.clone()))
            .max_decoding_message_size(256 << 20)
            .max_encoding_message_size(256 << 20),
        CheckToken::peers(cfg.token.as_deref()),
    );
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = &cfg.tls {
        builder = builder.tls_config(tls.server.clone()).map_err(|e| io::Error::other(format!("mesh TLS: {e}")))?;
    }
    tokio::spawn(async move {
        let r = builder
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
        Some(p) => {
            // A restart: tell the seeds this incarnation exists (best effort;
            // SWIM gossip spreads it too) and learn who joined meanwhile. If no
            // seed answers (they may all be gone), try the members it knew.
            let mut joined = false;
            let known = known_members(&cfg.dir);
            let seeds = cfg.seeds.iter().filter(|s| **s != cfg.advertise);
            for addr in seeds.chain(known.iter().filter(|a| !cfg.seeds.contains(a) && **a != cfg.advertise)) {
                if joined && !cfg.seeds.contains(addr) {
                    break;
                }
                if let Ok(reply) = mesh.join(addr, cfg.seeds.len() as u32).await {
                    joined = true;
                    for m in reply.members {
                        if m.node_id != me.node_id {
                            mesh.upsert(m);
                        }
                    }
                }
            }
            p
        }
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
        RaftHost::open(&cfg.dir.join("raft"), me.node_id, Arc::new(MeshTransport(mesh.clone())), durability)?;
    crate::durability::watch(host.disk_stats());
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
    // the previous incarnation of this peer held fenced and requeued. Healthy
    // groups take a moment; one that lost its quorum keeps trying in the
    // background while this peer serves the others.
    let prev = (node.identity.incarnation > 1)
        .then(|| pb::PeerInc { peer: node.identity.peer.to_vec(), inc: node.identity.incarnation - 1 });
    let mut adopting = tokio::task::JoinSet::new();
    for g in router.data_groups() {
        adopting.spawn(adopt_vshards(router.clone(), g, prev.clone()));
    }
    let all = async { while adopting.join_next().await.is_some() {} };
    if tokio::time::timeout(ADOPT_WAIT, all).await.is_err() {
        tracing::warn!("serving while some groups have no leader yet; they are retried in the background");
    }
    adopting.detach_all();
    tracing::info!(
        node = me.node_id,
        addr = %cfg.advertise,
        hosted = ?router.hosted_groups().iter().map(|g| g.id).collect::<Vec<_>>(),
        "cluster peer ready"
    );

    let planned: BTreeMap<u32, usize> = (0..=plan.data_groups).map(|g| (g, voters_in(&plan, g).len())).collect();
    tokio::spawn(liveness(router.clone(), mesh.clone(), node.core_tx.clone(), planned, cfg.dir.join(MEMBERS_FILE)));
    let clients = cfg.client_listen.map(|addr| runtime::ClientListen {
        addr,
        token: cfg.token.clone(),
        tls: cfg.tls.as_ref().map(|t| t.clients.clone()),
    });
    let served = runtime::serve(node, router, raft_events, None, clients).await;
    if served.is_ok()
        && let Err(e) = crate::durability::mark_clean(&cfg.dir)
    {
        tracing::warn!("could not record a clean shutdown: {e}");
    }
    served
}

/// How long startup waits for its groups to accept their vshards before
/// serving without the ones that have not.
const ADOPT_WAIT: Duration = Duration::from_secs(15);

/// Have group `g` own its vshards and retire this peer's previous
/// incarnation there, retrying until the group has a leader again.
async fn adopt_vshards(router: Arc<Router>, g: u32, prev: Option<pb::PeerInc>) {
    let vshards = router.vshards_of(g);
    let first = vshards[0] as u16;
    let mut warned = Instant::now();
    loop {
        let adopt = pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vshards.clone(), epoch: 1 });
        let mut r = router.propose(first, adopt).await.map(|_| ());
        if let (Ok(()), Some(p)) = (&r, &prev) {
            let retire = pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(p.clone()) });
            r = router.propose(first, retire).await.map(|_| ());
        }
        match r {
            Ok(()) => return,
            Err(e) if warned.elapsed() >= Duration::from_secs(30) => {
                tracing::warn!("group {g} did not accept its vshards yet ({e}); still trying");
                warned = Instant::now();
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Ping every member; mark silent ones dead; as a group leader, retire dead
/// incarnations and replace dead voters.
async fn liveness(
    router: Arc<Router>,
    mesh: Arc<MeshClient>,
    core: mpsc::Sender<CoreMsg>,
    planned: BTreeMap<u32, usize>,
    members_file: PathBuf,
) {
    let mut dead_since: BTreeMap<u64, Instant> = BTreeMap::new();
    // Members whose port has refused every probe since then: their process is
    // gone, so they are declared dead after REFUSED_FOR, not after suspicion.
    let mut refused_since: BTreeMap<u64, Instant> = BTreeMap::new();
    let mut recorded: Vec<String> = Vec::new();
    // Rebalancing waits for membership to settle, and for a group's last change.
    let mut stable_since = Instant::now();
    let mut changed_at: BTreeMap<u32, Instant> = BTreeMap::new();
    let mut tick = tokio::time::interval(PING_EVERY);
    loop {
        tick.tick().await;
        let now = mesh.now_ms();
        let targets = mesh.swim.lock().expect("lock").next_probes(now, PROBE_FANOUT);
        let probes = targets.into_iter().map(|target| {
            let mesh = mesh.clone();
            async move {
                // Only the target itself counts: a new identity answering at
                // a dead one's address must not keep the dead one alive.
                let refused = match mesh.ping(target, PING_TIMEOUT).await {
                    Ok(reply) if reply.node_id == target => {
                        mesh.note_ping(target, Some(reply));
                        return (target, true, false);
                    }
                    Ok(_) => true, // another process holds the address now
                    Err(e) => e.kind() == std::io::ErrorKind::ConnectionRefused,
                };
                let relays = mesh.swim.lock().expect("lock").relays(target);
                let tries = relays.into_iter().map(|r| {
                    let mesh = mesh.clone();
                    async move { mesh.ping_via(r, target, PING_TIMEOUT).await }
                });
                let reached = futures_util::future::join_all(tries).await.into_iter().any(|ok| ok);
                (target, reached, refused && !reached)
            }
        });
        let results = futures_util::future::join_all(probes).await;
        let changes = {
            let now = mesh.now_ms();
            let mut swim = mesh.swim.lock().expect("lock");
            for (target, reached, refused) in results {
                swim.probed(now, target, reached);
                // A refusal starts the clock and only an answer stops it: once
                // the process is gone, later probes may fail other ways (a
                // reset connection, a reconnect backing off) without it
                // coming back.
                if reached {
                    refused_since.remove(&target);
                    continue;
                }
                let since = if refused {
                    Some(*refused_since.entry(target).or_insert_with(Instant::now))
                } else {
                    refused_since.get(&target).copied()
                };
                if since.is_some_and(|t| t.elapsed() >= REFUSED_FOR) {
                    refused_since.remove(&target);
                    swim.fail(now, target);
                }
            }
            swim.expire(now);
            swim.take_changes()
        };
        // Who else is here, for a restart whose seeds are gone. Never
        // emptied: a peer that finds itself alone keeps the list it had.
        let mut live: Vec<String> = mesh.live_others().into_iter().map(|p| p.member.addr).collect();
        live.sort();
        if live != recorded {
            stable_since = Instant::now();
        }
        if !live.is_empty() && live != recorded {
            match write_atomic(&members_file, live.join("\n").as_bytes()) {
                Ok(()) => recorded = live,
                Err(e) => tracing::warn!("could not record the members: {e}"),
            }
        }
        for c in changes {
            let peer = mesh.member(c.node).map(|p| p.member.peer).unwrap_or_default();
            let what = match c.state {
                SwimState::Alive => "alive",
                SwimState::Suspect => "suspect: missed direct and indirect probes",
                SwimState::Dead => "dead: retired",
            };
            let event =
                pb::ClusterEvent { kind: "member".into(), node: peer, message: what.into(), ..Default::default() };
            let _ = core.try_send(CoreMsg::Event(event));
            match c.state {
                SwimState::Alive => {
                    mesh.set_suspect(c.node, false);
                    dead_since.remove(&c.node);
                }
                SwimState::Suspect => {
                    tracing::info!(node = c.node, "peer is suspect: missed direct and indirect probes");
                    mesh.set_suspect(c.node, true);
                }
                SwimState::Dead => {
                    let Some(m) = mesh.mark_dead(c.node) else { continue };
                    tracing::warn!(node = m.node_id, addr = %m.addr, incarnation = m.incarnation, "peer is unreachable; retiring it");
                    dead_since.insert(m.node_id, Instant::now());
                    retire_everywhere(&router, &m);
                }
            }
        }
        replace_voters(&router, &mesh, &dead_since, &planned).await;
        leave_stale_groups(&router, &mesh, &core, &members_file.with_file_name(HOSTED_FILE)).await;
        if stable_since.elapsed() >= rebalance_after() {
            rebalance(&router, &mesh, planned.len(), &mut changed_at).await;
        }
    }
}

/// How long a replica may go without hearing from its group's leader before
/// it asks whether it is still a member.
const STALE_AFTER: Duration = Duration::from_secs(5);

/// A replica swapped out of its group (`rebalance`, `replace_voters`) stops
/// receiving its log, but nothing tells this peer to stop serving it, and its
/// copy grows stale while reads and watches go on using it. So a replica that
/// has not heard from a leader for `STALE_AFTER` (or at all) asks the group's
/// leader for the membership, and stops serving a group that no longer lists
/// it; reads then go to the group's members. No answer (no leader reachable)
/// changes nothing.
async fn leave_stale_groups(
    router: &Arc<Router>,
    mesh: &Arc<MeshClient>,
    core: &mpsc::Sender<CoreMsg>,
    hosted_file: &std::path::Path,
) {
    let me = router.node_id();
    for g in router.hosted_groups() {
        // A replica that applied its own removal reports no leader contact
        // at all; one that missed it, contact that grows old.
        if router.leads(g.id) || g.since_leader_contact().is_some_and(|d| d < STALE_AFTER) {
            continue;
        }
        // Only the leader answers, and it may be a peer this replica never
        // heard of (leadership moves after seats do): ask the last leader it
        // knew, then every live member.
        let req = pb::ReadRequest {
            cluster: mesh.cluster.clone(),
            membership: true,
            membership_group: g.id,
            ..Default::default()
        };
        let hint = router.leader_hint(g.id);
        let others = mesh.live_others().into_iter().map(|p| p.member.node_id).filter(|n| Some(*n) != hint);
        let mut members = None;
        for node in hint.into_iter().chain(others) {
            if let Ok(reply) = mesh.read(node, req.clone()).await
                && !reply.members.is_empty()
            {
                members = Some(reply.members);
                break;
            }
        }
        tracing::debug!(group = g.id, ?members, "no word from the group's leader lately: asked it for the members");
        let Some(members) = members.filter(|m| !m.contains(&me)) else { continue };
        tracing::info!(group = g.id, "this peer is no longer a member of the group: dropping its replica");
        // Shut down before forgetting it: until then a re-add finds this
        // instance, never a second one on the same storage.
        g.shutdown().await;
        router.remove_group(g.id);
        // Reads and watches of the group now go to the members the leader named.
        mesh.set_voters(g.id, members);
        let _ = core.send(CoreMsg::Leading { group: g.id, leading: false }).await;
        let _ = core.send(CoreMsg::GroupDropped { group: g.id }).await;
        let ids: Vec<String> = router.hosted_groups().iter().map(|g| g.id.to_string()).collect();
        if let Err(e) = write_atomic(hosted_file, ids.join("\n").as_bytes()) {
            tracing::warn!("cannot record the groups this peer hosts: {e}");
        }
    }
}

/// How long membership must be steady before voters and leaders move
/// (`CHIMAERA_REBALANCE_SECONDS`, default 60).
fn rebalance_after() -> Duration {
    static AFTER: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *AFTER.get_or_init(|| {
        let s = std::env::var("CHIMAERA_REBALANCE_SECONDS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
        Duration::from_secs(s)
    })
}

/// Move one voter seat, or one leadership, toward an even spread, when
/// membership has been steady. Founders hold every seat when a cluster forms;
/// without this, peers that join later run calls but never share the
/// commits, and every call in the cluster commits on the founders.
///
/// For each group this peer leads whose voters differ from
/// [`target_voters`] over the live members: add one missing target as a
/// learner, then swap it in for one voter that is not a target (one change
/// per group per `rebalance_after`). Then, if this peer leads more groups
/// than its share, it hands one to a voter that leads fewer.
async fn rebalance(
    router: &Arc<Router>,
    mesh: &Arc<MeshClient>,
    groups: usize,
    changed_at: &mut BTreeMap<u32, Instant>,
) {
    let others = mesh.live_others();
    let mut members: Vec<pb::Member> = others.iter().map(|p| p.member.clone()).collect();
    members.push(mesh.me.clone());
    // Seats go only to peers that sync their log as this peer asks.
    let seats: Vec<pb::Member> = members.iter().filter(|m| crate::durability::meets_floor(m)).cloned().collect();
    let me = mesh.me.node_id;
    for g in router.hosted_groups() {
        if !router.leads(g.id) || changed_at.get(&g.id).is_some_and(|t| t.elapsed() < rebalance_after()) {
            continue;
        }
        let voters: BTreeSet<u64> = g.view().1.into_iter().collect();
        let target: BTreeSet<u64> = target_voters(g.id, &seats, voters.len()).into_iter().collect();
        let (Some(add), Some(remove)) =
            (target.difference(&voters).next().copied(), voters.difference(&target).find(|v| **v != me).copied())
        else {
            continue;
        };
        let Some(addr) = others.iter().find(|p| p.member.node_id == add).map(|p| p.member.addr.clone()) else {
            continue;
        };
        changed_at.insert(g.id, Instant::now());
        tracing::info!(group = g.id, add, remove, "moving a voter seat to spread the group");
        if let Err(e) = g.add_learner(add, &addr).await {
            tracing::warn!(group = g.id, "cannot add learner {add}: {e}");
            continue;
        }
        let next: BTreeSet<u64> = voters.iter().copied().filter(|v| *v != remove).chain([add]).collect();
        match g.change_voters(next.clone()).await {
            Ok(()) => mesh.set_voters(g.id, next.into_iter().collect()),
            Err(e) => tracing::warn!(group = g.id, "membership change failed: {e}"),
        }
        return; // one change at a time across the cluster's groups led here
    }
    // Leadership: at most an even share of the groups led here.
    let led = router.leading();
    let share = groups.div_ceil(members.len().max(1));
    if led.len() <= share {
        return;
    }
    let leads = |node: u64| {
        others.iter().find(|p| p.member.node_id == node).and_then(|p| p.load.as_ref()).map(|l| l.leading.len())
    };
    for g in router.hosted_groups().into_iter().filter(|g| led.contains(&g.id)) {
        if changed_at.get(&g.id).is_some_and(|t| t.elapsed() < rebalance_after()) {
            continue;
        }
        let to = g.view().1.into_iter().filter(|v| *v != me).filter_map(|v| leads(v).map(|n| (n, v))).min();
        if let Some((n, to)) = to.filter(|(n, _)| *n < share) {
            changed_at.insert(g.id, Instant::now());
            tracing::info!(group = g.id, to, leads = n, "handing leadership to spread the groups' leaders");
            if let Err(e) = g.transfer_leader(to).await {
                tracing::warn!(group = g.id, "leadership transfer failed: {e}");
            }
            return;
        }
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
/// `REPLACE_AFTER` for live members that do not vote in it yet, and add
/// voters to a group left with fewer than `planned` (after `cluster recover`).
async fn replace_voters(
    router: &Arc<Router>,
    mesh: &Arc<MeshClient>,
    dead_since: &BTreeMap<u64, Instant>,
    planned: &BTreeMap<u32, usize>,
) {
    for g in router.hosted_groups() {
        if !router.leads(g.id) {
            continue;
        }
        let voters: BTreeSet<u64> = g.view().1.into_iter().collect();
        // A draining voter is replaced at once: it is about to stop.
        // Only a real drain: `draining` alone may mean a slow core (the ping handler).
        let draining = |n: &u64| mesh.member(*n).and_then(|p| p.load).is_some_and(|l| l.stopping);
        let dead: Vec<u64> = voters
            .iter()
            .copied()
            .filter(|n| dead_since.get(n).is_some_and(|t| t.elapsed() > REPLACE_AFTER) || draining(n))
            .collect();
        let missing = planned.get(&g.id).copied().unwrap_or(GROUP_VOTERS).saturating_sub(voters.len());
        if dead.is_empty() && missing == 0 {
            continue;
        }
        let mut spares: Vec<_> =
            mesh.live_others().into_iter().filter(|p| !voters.contains(&p.member.node_id)).collect();
        // Only peers that sync their log as this peer asks: a dead voter stays
        // unreplaced rather than its seat going to one that does not sync.
        spares.retain(|p| crate::durability::meets_floor(&p.member));
        spares.sort_by(|a, b| a.pressure().total_cmp(&b.pressure()));
        let mut next: BTreeSet<u64> = voters.clone();
        let wanted = dead.iter().map(Some).chain(std::iter::repeat_n(None, missing));
        for (gone, spare) in wanted.zip(spares.iter()) {
            let id = spare.member.node_id;
            match gone {
                Some(gone) if draining(gone) => {
                    tracing::info!(group = g.id, gone, added = id, "adding a voter: replacing a draining voter")
                }
                Some(gone) => tracing::info!(group = g.id, gone, added = id, "adding a voter: replacing a dead voter"),
                None => tracing::info!(group = g.id, added = id, "adding a voter"),
            }
            if let Err(e) = g.add_learner(id, &spare.member.addr).await {
                tracing::warn!(group = g.id, "cannot add learner {id}: {e}");
                continue;
            }
            if let Some(gone) = gone {
                next.remove(gone);
            }
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

/// `chimaera cluster recover`: with this peer stopped, make it the only voter
/// of `group` from the state it holds (see `chimaera_raft::force_single_voter`).
pub fn recover_group(dir: &std::path::Path, group: u32) -> io::Result<u64> {
    let _lock = runtime::lock_dir(dir)?;
    let peer = std::fs::read_to_string(dir.join("peer-id"))
        .ok()
        .and_then(|s| chimaera_core::ids::Id::parse_hex(s.trim()))
        .ok_or_else(|| io::Error::other(format!("{} holds no peer identity", dir.display())))?;
    let node = crate::stores::Identity { peer, incarnation: 0 }.node_id();
    chimaera_raft::force_single_voter(&dir.join("raft"), group, node)
}

#[cfg(test)]
mod tests {

    use super::*;

    fn member(n: u64) -> pb::Member {
        pb::Member { node_id: n, addr: format!("10.0.0.{n}:7000"), cpus: 4.0, ..Default::default() }
    }

    #[test]
    fn voters_spread_over_zones_and_a_new_member_moves_few_seats() {
        let zoned = |n: u64, zone: &str| pb::Member {
            node_id: n,
            labels: [("zone".to_string(), zone.to_string())].into(),
            ..member(n)
        };
        let members: Vec<pb::Member> =
            vec![zoned(1, "a"), zoned(2, "a"), zoned(3, "a"), zoned(4, "b"), zoned(5, "b"), zoned(6, "c")];
        for g in 0..32 {
            let v = target_voters(g, &members, 3);
            let zones: BTreeSet<_> =
                v.iter().map(|n| members.iter().find(|m| m.node_id == *n).unwrap().labels["zone"].clone()).collect();
            assert_eq!(zones.len(), 3, "group {g}: {v:?} share a zone");
        }
        let before: Vec<pb::Member> = (1..=6).map(member).collect();
        let after: Vec<pb::Member> = (1..=7).map(member).collect();
        let moved: usize = (0..64)
            .map(|g| {
                let (a, b) = (target_voters(g, &before, 3), target_voters(g, &after, 3));
                a.iter().filter(|n| !b.contains(n)).count()
            })
            .sum();
        assert!(moved <= 64 * 3 / 3, "a seventh member took {moved} of 192 seats");
        assert!(moved > 0, "a new member takes some seats");
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
