//! Builds shard commands (vshard, epoch, issuer, HLC) and delivers them to the
//! group that owns the vshard: through the local replica when this peer leads
//! the group, otherwise to the leader over the mesh.
//!
//! A local peer has one group (0) that owns every vshard. In a cluster, group
//! 0 is the directory and groups `1..=data_groups` own the vshards, vshard `v`
//! belonging to group `1 + v % data_groups`.

use crate::mesh::MeshClient;
use crate::stores::Identity;
use chimaera_core::hlc;
use chimaera_core::kv::KvRead;
use chimaera_core::pb;
use chimaera_raft::{Group, ProposeError};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

pub fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64)
}

/// How long a proposal keeps looking for a leader before giving up.
const PROPOSE_PATIENCE: Duration = Duration::from_secs(15);

pub struct Router {
    pub identity: Identity,
    /// 0 for a local peer: group 0 owns every vshard.
    data_groups: u32,
    epoch: u64,
    clock: AtomicU64,
    groups: RwLock<BTreeMap<u32, Group>>,
    leading: RwLock<BTreeSet<u32>>,
    leader_hint: Mutex<HashMap<u32, u64>>,
    pub mesh: Option<Arc<MeshClient>>,
    /// Calls this peer could start now (free CPUs less what is queued),
    /// published by the core, taken by ingress for local-first grants.
    local_room: std::sync::atomic::AtomicI64,
    /// Local-first grants handed out whose work has not reached the queue
    /// yet: they count against the room the core publishes.
    local_pending: std::sync::atomic::AtomicI64,
    /// What this peer was told about runs and calls, for `sync_class`.
    pub known: crate::sync_class::Known,
}

impl Router {
    pub fn local(identity: Identity, group: Group) -> Router {
        let r = Router::new(identity, 0, None);
        r.add_group(group);
        r.leading.write().expect("lock").insert(0);
        r
    }

    pub fn cluster(identity: Identity, data_groups: u32, mesh: Arc<MeshClient>) -> Router {
        Router::new(identity, data_groups.max(1), Some(mesh))
    }

    fn new(identity: Identity, data_groups: u32, mesh: Option<Arc<MeshClient>>) -> Router {
        Router {
            identity,
            data_groups,
            epoch: 1,
            clock: AtomicU64::new(0),
            groups: RwLock::new(BTreeMap::new()),
            leading: RwLock::new(BTreeSet::new()),
            leader_hint: Mutex::new(HashMap::new()),
            local_room: std::sync::atomic::AtomicI64::new(0),
            local_pending: std::sync::atomic::AtomicI64::new(0),
            mesh,
            known: Default::default(),
        }
    }

    pub fn is_cluster(&self) -> bool {
        self.mesh.is_some()
    }

    pub fn node_id(&self) -> u64 {
        self.identity.node_id()
    }

    pub fn group_of(&self, vshard: u16) -> u32 {
        if self.data_groups == 0 { 0 } else { 1 + vshard as u32 % self.data_groups }
    }

    /// The vshards group `group` owns.
    pub fn vshards_of(&self, group: u32) -> Vec<u32> {
        (0..chimaera_core::ids::VSHARDS as u32).filter(|v| self.group_of(*v as u16) == group).collect()
    }

    pub fn data_groups(&self) -> std::ops::RangeInclusive<u32> {
        if self.data_groups == 0 { 0..=0 } else { 1..=self.data_groups }
    }

    pub fn add_group(&self, g: Group) {
        self.groups.write().expect("lock").insert(g.id, g);
    }

    /// Stop serving `group` from this peer: it is no longer a member.
    pub fn remove_group(&self, group: u32) -> Option<Group> {
        self.leading.write().expect("lock").remove(&group);
        self.groups.write().expect("lock").remove(&group)
    }

    pub fn hosted(&self, group: u32) -> Option<Group> {
        self.groups.read().expect("lock").get(&group).cloned()
    }

    pub fn hosted_groups(&self) -> Vec<Group> {
        self.groups.read().expect("lock").values().cloned().collect()
    }

    /// Whether this peer's replica of the group owning `vshard` is current:
    /// it leads the group, or heard from its leader within the last 2 s. A
    /// replica that is catching up, or that the leader is backing off from,
    /// may not apply a command for a long time.
    pub fn current(&self, vshard: u16) -> bool {
        let g = self.group_of(vshard);
        self.leads(g)
            || self.hosted(g).and_then(|grp| grp.since_leader_contact()).is_some_and(|d| d < Duration::from_secs(2))
    }

    /// Whether this peer hosts a replica of the group owning `vshard`.
    pub fn hosts(&self, vshard: u16) -> bool {
        self.groups.read().expect("lock").contains_key(&self.group_of(vshard))
    }

    pub fn set_leading(&self, group: u32, leading: bool) {
        let mut l = self.leading.write().expect("lock");
        if leading {
            l.insert(group);
        } else {
            l.remove(&group);
        }
    }

    /// Whether this peer currently leads `group` (as last reported by raft).
    pub fn leads(&self, group: u32) -> bool {
        self.leading.read().expect("lock").contains(&group)
    }

    pub fn leading(&self) -> Vec<u32> {
        self.leading.read().expect("lock").iter().copied().collect()
    }

    pub fn note_leader(&self, group: u32, node: u64) {
        self.leader_hint.lock().expect("lock").insert(group, node);
    }

    /// Publish how many more calls this peer could start now, less the
    /// local-first grants still on their way to the queue.
    pub fn set_local_room(&self, room: i64) {
        use std::sync::atomic::Ordering::Relaxed;
        let pending = self.local_pending.load(Relaxed);
        self.local_room.store((room - pending).max(0), Relaxed);
    }

    /// Work reached this peer's queue: one fewer grant on its way.
    pub fn local_arrived(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let _ = self.local_pending.fetch_update(Relaxed, Relaxed, |p| Some((p - 1).max(0)));
    }

    /// Claim up to `want` of the local room; returns how many were claimed.
    pub fn take_local_room(&self, want: usize) -> usize {
        use std::sync::atomic::Ordering::Relaxed;
        let mut have = self.local_room.load(Relaxed);
        loop {
            let take = have.min(want as i64).max(0);
            if take == 0 {
                return 0;
            }
            match self.local_room.compare_exchange_weak(have, have - take, Relaxed, Relaxed) {
                Ok(_) => {
                    self.local_pending.fetch_add(take, Relaxed);
                    return take as usize;
                }
                Err(now) => have = now,
            }
        }
    }

    /// The last leader this peer heard of for `group`.
    pub fn leader_hint(&self, group: u32) -> Option<u64> {
        self.leader_hint.lock().expect("lock").get(&group).copied()
    }

    /// Run `f` over the local replica of the group owning `vshard`, if hosted.
    /// A follower's view may lag the leader's.
    pub fn read<T>(&self, vshard: u16, f: impl FnOnce(&dyn KvRead) -> T) -> Option<T> {
        self.hosted(self.group_of(vshard))?.read(f).ok()
    }

    /// The actor records this peer's replica of `group` holds; None when this
    /// peer does not host the group.
    pub fn actors_of(&self, group: u32) -> Option<Vec<pb::ActorRecord>> {
        let vshards = self.vshards_of(group);
        self.hosted(group)?
            .read(|kv| {
                vshards
                    .iter()
                    .flat_map(|v| chimaera_core::shard::read::actor_records(kv, *v as u16, usize::MAX))
                    .collect::<Vec<_>>()
            })
            .ok()
    }

    /// The runs whose records this peer's replica of `group` holds, without
    /// their environment; None when this peer does not host the group.
    pub fn runs_of(&self, group: u32) -> Option<Vec<pb::RunRecord>> {
        let vshards = self.vshards_of(group);
        let mut runs = self
            .hosted(group)?
            .read(|kv| {
                vshards
                    .iter()
                    .flat_map(|v| chimaera_core::shard::read::runs(kv, *v as u16, usize::MAX))
                    .collect::<Vec<_>>()
            })
            .ok()?;
        for r in &mut runs {
            r.exec_env = None;
        }
        Some(runs)
    }

    /// Next local HLC timestamp (monotonic even if the wall clock steps back).
    pub fn hlc(&self) -> u64 {
        let mut prev = self.clock.load(Ordering::Acquire);
        loop {
            let next = hlc::next(prev, now_ms());
            match self.clock.compare_exchange_weak(prev, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return next,
                Err(p) => prev = p,
            }
        }
    }

    pub fn cmd(&self, vshard: u16, body: pb::shard_cmd::Body) -> pb::ShardCmd {
        pb::ShardCmd {
            vshard: vshard as u32,
            vshard_epoch: self.epoch,
            issuer: Some(self.identity.pb()),
            hlc: self.hlc(),
            body: Some(body),
            sync_optional: false,
        }
    }

    pub async fn propose(&self, vshard: u16, body: pb::shard_cmd::Body) -> Result<pb::ShardReply, ProposeError> {
        self.propose_cmd(self.cmd(vshard, body)).await
    }

    /// As `propose`; with `sync_optional`, the voters may acknowledge the
    /// command before syncing it (only what `sync_class` allows).
    pub async fn propose_opt(
        &self,
        vshard: u16,
        body: pb::shard_cmd::Body,
        sync_optional: bool,
    ) -> Result<pb::ShardReply, ProposeError> {
        self.propose_cmd(pb::ShardCmd { sync_optional, ..self.cmd(vshard, body) }).await
    }

    /// Deliver `cmd` to its group's leader, following leader hints and
    /// retrying through elections. Shard commands are idempotent, so a retry
    /// after an ambiguous failure is safe.
    pub async fn propose_cmd(&self, cmd: pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
        crate::counters::PROPOSALS.add(1);
        let reply = self.propose_until_accepted(cmd).await;
        if reply.is_err() {
            crate::counters::PROPOSAL_FAILURES.add(1);
        }
        reply
    }

    async fn propose_until_accepted(&self, cmd: pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
        let group = self.group_of(cmd.vshard as u16);
        let Some(mesh) = &self.mesh else {
            let g = self.hosted(group).ok_or(ProposeError::Unavailable { group, reason: "not hosted".into() })?;
            return g.propose(&cmd).await;
        };
        let me = self.node_id();
        let started = std::time::Instant::now();
        let mut delay = Duration::from_millis(5);
        let mut tried: BTreeSet<u64> = BTreeSet::new();
        let mut last = ProposeError::Unavailable { group, reason: "no leader found".into() };
        while started.elapsed() < PROPOSE_PATIENCE {
            let hint = self.leader_hint.lock().expect("lock").get(&group).copied();
            let target = match hint {
                Some(n) => n,
                None if self.hosted(group).is_some() && !tried.contains(&me) => me,
                None => match mesh.voters_of(group).into_iter().find(|n| !tried.contains(n)) {
                    Some(n) => n,
                    None => {
                        tried.clear();
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_millis(500));
                        continue;
                    }
                },
            };
            tried.insert(target);
            let result = if target == me {
                match self.hosted(group) {
                    Some(g) => g.propose(&cmd).await,
                    None => Err(ProposeError::NotLeader { group, leader: None }),
                }
            } else {
                mesh.propose(target, group, &cmd).await
            };
            match result {
                Ok(reply) => {
                    self.note_leader(group, target);
                    return Ok(reply);
                }
                Err(ProposeError::NotLeader { leader: Some(l), .. }) if l != target => {
                    self.note_leader(group, l);
                }
                Err(e) => {
                    self.leader_hint.lock().expect("lock").remove(&group);
                    last = e;
                    if !matches!(last, ProposeError::NotLeader { .. }) {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_millis(500));
                    }
                }
            }
        }
        Err(last)
    }
}
