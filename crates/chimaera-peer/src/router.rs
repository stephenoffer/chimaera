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
            mesh,
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

    pub fn hosted(&self, group: u32) -> Option<Group> {
        self.groups.read().expect("lock").get(&group).cloned()
    }

    pub fn hosted_groups(&self) -> Vec<Group> {
        self.groups.read().expect("lock").values().cloned().collect()
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

    /// The last leader this peer heard of for `group`.
    pub fn leader_hint(&self, group: u32) -> Option<u64> {
        self.leader_hint.lock().expect("lock").get(&group).copied()
    }

    /// Run `f` over the local replica of the group owning `vshard`, if hosted.
    /// A follower's view may lag the leader's.
    pub fn read<T>(&self, vshard: u16, f: impl FnOnce(&dyn KvRead) -> T) -> Option<T> {
        self.hosted(self.group_of(vshard))?.read(f).ok()
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
        }
    }

    pub async fn propose(&self, vshard: u16, body: pb::shard_cmd::Body) -> Result<pb::ShardReply, ProposeError> {
        self.propose_cmd(self.cmd(vshard, body)).await
    }

    /// Deliver `cmd` to its group's leader, following leader hints and
    /// retrying through elections. Shard commands are idempotent, so a retry
    /// after an ambiguous failure is safe.
    pub async fn propose_cmd(&self, cmd: pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
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
