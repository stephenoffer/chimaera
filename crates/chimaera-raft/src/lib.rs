//! Multi-raft host: many shard groups per peer on one log database and one
//! state-machine database, with group commit across groups.
//!
//! The rest of Chimaera talks to a group through [`Group::propose`] and
//! [`Group::read`]; openraft stays an implementation detail of this crate.

mod backend;
mod diverge;
mod local;
mod log_store;
mod network;
mod read_pass;
mod recover;
mod restore;
mod settle;
mod snapshot_file;
mod state_machine;
mod wal;

pub use log_store::{DiskReport, DiskStats, DiskWriter, LogStore, MAX_COMMIT_DELAY, SLOW_COMMIT};
pub use network::{NetworkFactory, NoTransport, RpcFuture, RpcKind, Transport, handle_rpc, handle_snapshot};
pub use read_pass::ReadPass;
pub use recover::force_single_voter;
pub use restore::{Backed, Restored, backup, restore};
pub use settle::settle_files;
pub use state_machine::{GroupEvents, GroupRead, StateMachine, mark_state_machine_clean};

use chimaera_core::kv::KvRead;
use chimaera_core::pb;
use openraft::Config;
use prost::Message;
use redb::Database;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// An encoded protobuf message carried through the raft log (commands) or back
/// to the proposer (replies). Rust never interprets user payloads here.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Bytes(pub Vec<u8>);

impl std::fmt::Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

openraft::declare_raft_types!(
    /// Raft types for shard groups: commands and replies are encoded protobuf.
    pub TypeConfig:
        D = Bytes,
        R = Bytes,
);

pub type Raft = openraft::Raft<TypeConfig, StateMachine>;
pub type NodeId = u64;
pub use diverge::set_divergence_hook;
pub use openraft::BasicNode;

#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    #[error("not the leader of group {group}; leader is {leader:?}")]
    NotLeader { group: u32, leader: Option<NodeId> },
    #[error("group {group} cannot accept writes: {reason}")]
    Unavailable { group: u32, reason: String },
}

/// Raft timing shared by every group on a peer.
pub fn raft_config() -> Arc<Config> {
    let cfg = Config {
        cluster_name: "chimaera".into(),
        heartbeat_interval: 300,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        // Snapshots are triggered by size instead (`snapshot_when_due`).
        snapshot_policy: openraft::SnapshotPolicy::Never,
        max_in_snapshot_log_to_keep: 1000,
        ..Default::default()
    };
    Arc::new(cfg.validate().expect("static raft config is valid"))
}

/// This boot of the machine (Linux boot id; macOS boot time), to tell a
/// process restart, which keeps the page cache, from a machine restart.
pub(crate) fn boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok().map(|s| s.trim().to_string())
    }
    #[cfg(target_os = "macos")]
    {
        let mut tv = libc::timeval { tv_sec: 0, tv_usec: 0 };
        let mut len = std::mem::size_of::<libc::timeval>();
        // SAFETY: kern.boottime fills a timeval of the given length.
        let rc = unsafe {
            libc::sysctlbyname(c"kern.boottime".as_ptr(), (&raw mut tv).cast(), &mut len, std::ptr::null_mut(), 0)
        };
        (rc == 0).then(|| format!("{}.{}", tv.tv_sec, tv.tv_usec))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// How hard a commit pushes data to stable storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskDurability {
    /// redb's default sync (`F_FULLFSYNC` on macOS): survives power loss.
    Full,
    /// Plain `fsync`: survives an OS crash on most disks, not drive-cache loss.
    Os,
    /// No sync: survives the peer process dying, not the machine. Local peers.
    Process,
}

fn open_db(path: &Path, durability: DiskDurability) -> io::Result<Database> {
    let db = match durability {
        DiskDurability::Full => Database::create(path),
        DiskDurability::Os => Database::builder().create_with_backend(backend::OsSyncBackend::open(path, true)?),
        DiskDurability::Process => Database::builder().create_with_backend(backend::OsSyncBackend::open(path, false)?),
    };
    db.map_err(log_store::io_err)
}

/// The least log a group writes between snapshots.
const SNAPSHOT_MIN_LOG_BYTES: u64 = 64 << 20;

/// Build a snapshot whenever group `raft`'s log since the last one is due
/// (see [`state_machine::SnapshotDue`]); ends when the group shuts down.
async fn snapshot_when_due(raft: Raft, due: Arc<state_machine::SnapshotDue>, min: u64) {
    use openraft::async_runtime::WatchReceiver;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        if raft.metrics().borrow_watched().running_state.is_err() {
            return;
        }
        if due.due(min) && raft.trigger().snapshot().await.is_err() {
            return;
        }
    }
}

fn compact(db: &mut Database, path: &Path) {
    match db.compact(redb::CompactionMode::Balanced) {
        Ok(_) => {}
        Err(e) => tracing::warn!("could not compact {}: {e}", path.display()),
    }
}

/// Opens the peer's raft databases and starts groups on them.
pub struct RaftHost {
    node_id: NodeId,
    log_db: Arc<Database>,
    sm_db: Arc<Database>,
    writer: DiskWriter,
    snapshot_dir: PathBuf,
    sm_path: PathBuf,
    events: tokio::sync::mpsc::Sender<GroupEvents>,
    config: Arc<Config>,
    transport: Arc<dyn Transport>,
    snapshot_min_log_bytes: u64,
}

impl RaftHost {
    /// Open (or create) the databases under `dir`. Events from every group's
    /// applies arrive on the returned receiver.
    pub fn open(
        dir: &Path,
        node_id: NodeId,
        transport: Arc<dyn Transport>,
        durability: DiskDurability,
    ) -> io::Result<(RaftHost, tokio::sync::mpsc::Receiver<GroupEvents>)> {
        let (log_path, sm_path) = (dir.join("raft-log.redb"), dir.join("raft-sm.redb"));
        let (mut log_db, mut sm_db, wal) = settle::recover_files(dir, durability)?;
        // Purged logs and expired state leave free pages that redb reuses but
        // never gives back to the disk; return them at each start.
        compact(&mut log_db, &log_path);
        compact(&mut sm_db, &sm_path);
        state_machine::prune_snapshots(&sm_db, &dir.join("snapshots"), dir)?;
        let (log_db, sm_db) = (Arc::new(log_db), Arc::new(sm_db));
        // CHIMAERA_RAFT_MAX_COMMIT_DELAY_US: the most a batch is held open for company.
        let max_hold = std::env::var("CHIMAERA_RAFT_MAX_COMMIT_DELAY_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .map_or(MAX_COMMIT_DELAY, std::time::Duration::from_micros);
        let writer = DiskWriter::start(log_db.clone(), max_hold, wal)?;
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let host = RaftHost {
            node_id,
            log_db,
            sm_db,
            writer,
            snapshot_dir: dir.join("snapshots"),
            sm_path,
            events: tx,
            config: raft_config(),
            transport,
            // CHIMAERA_RAFT_SNAPSHOT_BYTES: the least log between snapshots.
            snapshot_min_log_bytes: std::env::var("CHIMAERA_RAFT_SNAPSHOT_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(SNAPSHOT_MIN_LOG_BYTES),
        };
        Ok((host, rx))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// How the log's disk writer has been doing.
    pub fn disk_stats(&self) -> Arc<DiskStats> {
        self.writer.stats()
    }

    /// Start group `id`. With `bootstrap` set and no prior state, initialize it
    /// with `members` (all voters). Restarted groups recover from disk.
    pub async fn start_group(
        &self,
        id: u32,
        members: BTreeMap<NodeId, BasicNode>,
        bootstrap: bool,
    ) -> io::Result<Group> {
        let log = LogStore::new(id, self.log_db.clone(), self.writer.clone());
        let due = log.due.clone();
        let sm = StateMachine::new(id, self.sm_db.clone(), self.events.clone(), self.snapshot_dir.clone())
            .with_due(due.clone());
        if let Some(Ok(size)) = current_snapshot(&self.sm_db, id).map(|p| std::fs::metadata(p).map(|m| m.len())) {
            due.last_size.store(size, std::sync::atomic::Ordering::Relaxed);
        }
        let net = NetworkFactory::new(id, self.transport.clone());
        let raft =
            Raft::new(self.node_id, self.config.clone(), net, log.clone(), sm).await.map_err(log_store::io_err)?;
        if bootstrap && !raft.is_initialized().await.map_err(log_store::io_err)? {
            raft.initialize(members).await.map_err(log_store::io_err)?;
        }
        tokio::spawn(snapshot_when_due(raft.clone(), due, self.snapshot_min_log_bytes));
        Ok(Group { id, node_id: self.node_id, raft, db: self.sm_db.clone(), log, local: None })
    }

    /// Turn a local peer's single-member group into one that applies commands
    /// without a Raft log. Call it once the group leads: it waits until every
    /// committed entry is applied (a restart replays the log tail first), then
    /// shuts Raft down. Cluster groups must not use this.
    pub async fn without_log(&self, group: Group) -> io::Result<Group> {
        group.linearizable().await.map_err(io::Error::other)?;
        let _ = group.raft.shutdown().await;
        let applier = local::Applier::start(group.id, self.sm_db.clone(), self.sm_path.clone(), self.events.clone())?;
        Ok(Group { local: Some(Arc::new(applier)), ..group })
    }
}

/// Each group's voters (node id and address) as the state machine under
/// `dir` last stored them; empty when there is no state. Read before the
/// host opens the databases, to find who else holds a group's state.
pub fn stored_voters(dir: &Path) -> io::Result<BTreeMap<u32, Vec<(NodeId, String)>>> {
    use redb::{ReadableDatabase, ReadableTable};
    let path = dir.join("raft-sm.redb");
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let db = Database::open(&path).map_err(log_store::io_err)?;
    let txn = db.begin_read().map_err(log_store::io_err)?;
    let t = txn.open_table(state_machine::SM_META).map_err(log_store::io_err)?;
    let mut out = BTreeMap::new();
    for r in t.iter().map_err(log_store::io_err)? {
        let (k, v) = r.map_err(log_store::io_err)?;
        let (group, key) = k.value();
        if key != "membership" {
            continue;
        }
        let m: openraft::type_config::alias::StoredMembershipOf<TypeConfig> = log_store::dec(v.value())?;
        let voters = m
            .membership()
            .voter_ids()
            .map(|id| (id, m.membership().get_node(&id).map(|n| n.addr.clone()).unwrap_or_default()))
            .collect();
        out.insert(group, voters);
    }
    Ok(out)
}

/// The snapshot file group `group` currently points at, if any.
fn current_snapshot(sm_db: &Database, group: u32) -> Option<PathBuf> {
    use redb::{ReadableDatabase, ReadableTable};
    let txn = sm_db.begin_read().ok()?;
    let t = txn.open_table(state_machine::SM_META).ok()?;
    let v = t.get((group, "snapshot")).ok()??;
    log_store::dec::<String>(v.value()).ok().map(PathBuf::from)
}

fn create_tables(log_db: &Database, sm_db: &Database) -> io::Result<()> {
    let txn = log_db.begin_write().map_err(log_store::io_err)?;
    txn.open_table(log_store::LOG).map_err(log_store::io_err)?;
    txn.open_table(log_store::LOG_META).map_err(log_store::io_err)?;
    txn.commit().map_err(log_store::io_err)?;
    let txn = sm_db.begin_write().map_err(log_store::io_err)?;
    txn.open_table(state_machine::SM).map_err(log_store::io_err)?;
    txn.open_table(state_machine::SM_META).map_err(log_store::io_err)?;
    txn.commit().map_err(log_store::io_err)
}

/// Follows a group's leadership and membership.
pub struct GroupWatch {
    rx: openraft::type_config::alias::WatchReceiverOf<TypeConfig, openraft::RaftMetrics<TypeConfig>>,
}

impl GroupWatch {
    /// Wait for the next change; false once the group has shut down.
    pub async fn changed(&mut self) -> bool {
        use openraft::async_runtime::WatchReceiver;
        self.rx.changed().await.is_ok()
    }

    pub fn view(&mut self) -> (Option<NodeId>, Vec<NodeId>) {
        use openraft::async_runtime::WatchReceiver;
        let m = self.rx.borrow_and_update();
        (m.current_leader, m.membership_config.membership().voter_ids().collect())
    }
}

/// One replica's view of its group (see [`Group::stats`]).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupStats {
    pub leader: Option<NodeId>,
    pub voters: Vec<NodeId>,
    pub last_log: u64,
    pub applied: u64,
    /// On the leader: entries the furthest-behind voter lacks. None elsewhere.
    pub max_lag: Option<u64>,
}

/// One shard group hosted on this peer.
#[derive(Clone)]
pub struct Group {
    pub id: u32,
    node_id: NodeId,
    pub raft: Raft,
    db: Arc<Database>,
    /// This replica's log, to check a leader's appends against what it committed.
    log: LogStore,
    /// Set for a local peer's group after recovery: commands apply without a
    /// log (see [`RaftHost::without_log`]) and `raft` is shut down.
    local: Option<Arc<local::Applier>>,
}

impl Group {
    /// Replicate `cmd` and return the state machine's reply. Only the leader
    /// accepts proposals; others return `NotLeader` with a hint.
    pub async fn propose(&self, cmd: &pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
        use openraft::error::{ClientWriteError, RaftError};
        if let Some(local) = &self.local {
            return local
                .propose(cmd.clone())
                .await
                .map_err(|reason| ProposeError::Unavailable { group: self.id, reason });
        }
        match self.raft.client_write(Bytes(cmd.encode_to_vec())).await {
            Ok(resp) => pb::ShardReply::decode(resp.data.0.as_slice())
                .map_err(|e| ProposeError::Unavailable { group: self.id, reason: format!("undecodable reply: {e}") }),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Err(ProposeError::NotLeader { group: self.id, leader: f.leader_id })
            }
            Err(e) => Err(ProposeError::Unavailable { group: self.id, reason: e.to_string() }),
        }
    }

    /// Run `f` over this group's locally applied state. On a follower or a
    /// deposed leader the view may be stale; use [`Group::linearizable`] first
    /// when the caller needs the latest committed state.
    pub fn read<T>(&self, f: impl FnOnce(&dyn KvRead) -> T) -> io::Result<T> {
        if let Some(local) = &self.local {
            return local.read(f); // includes writes not yet in the database
        }
        let mut f = Some(f);
        if let Some(r) = read_pass::with(&self.db, self.id, |view| (f.take().expect("once"))(view)) {
            return r; // a pass is active on this thread (ReadPass)
        }
        let view = GroupRead::open(&self.db, self.id)?;
        Ok((f.take().expect("once"))(&view))
    }

    /// Wait until local state reflects everything committed before this call.
    pub async fn linearizable(&self) -> Result<(), ProposeError> {
        if self.local.is_some() {
            return Ok(()); // one applier: local state is always the latest
        }
        self.raft
            .ensure_linearizable(openraft::raft::ReadPolicy::ReadIndex)
            .await
            .map(|_| ())
            .map_err(|e| ProposeError::Unavailable { group: self.id, reason: e.to_string() })
    }

    pub async fn is_leader(&self) -> bool {
        self.local.is_some() || self.raft.current_leader().await == Some(self.node_id)
    }

    /// Leader and voters as this replica last saw them.
    pub fn view(&self) -> (Option<NodeId>, Vec<NodeId>) {
        use openraft::async_runtime::WatchReceiver;
        if self.local.is_some() {
            return (Some(self.node_id), vec![self.node_id]);
        }
        let rx = self.raft.metrics();
        let m = rx.borrow_watched();
        (m.current_leader, m.membership_config.membership().voter_ids().collect())
    }

    /// This replica's view of the group's health.
    pub fn stats(&self) -> GroupStats {
        use openraft::async_runtime::WatchReceiver;
        if self.local.is_some() {
            return GroupStats {
                leader: Some(self.node_id),
                voters: vec![self.node_id],
                last_log: 0,
                applied: 0,
                max_lag: Some(0),
            };
        }
        let rx = self.raft.metrics();
        let m = rx.borrow_watched();
        let last_log = m.last_log_index.unwrap_or(0);
        let voters: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
        let matched = |v: &NodeId| m.replication.as_ref()?.get(v)?.as_ref().map(|l| l.index());
        let max_lag = m.replication.as_ref().map(|_| {
            voters
                .iter()
                .filter(|v| **v != self.node_id)
                .map(|v| last_log.saturating_sub(matched(v).unwrap_or(0)))
                .max()
                .unwrap_or(0)
        });
        GroupStats {
            leader: m.current_leader,
            voters,
            last_log,
            applied: m.last_applied.as_ref().map_or(0, |l| l.index()),
            max_lag,
        }
    }

    /// How long since this replica last heard from a leader; None if it never
    /// has, or leads. A voter swapped out of the group stops hearing from it
    /// (openraft does not send it the configuration that removed it).
    pub fn since_leader_contact(&self) -> Option<std::time::Duration> {
        use openraft::Instant as _;
        use openraft::async_runtime::WatchReceiver;
        let contact = self.raft.metrics().borrow_watched().last_leader_contact?;
        Some(contact.elapsed())
    }

    /// The node ids in this replica's latest membership, voters and learners.
    pub fn members(&self) -> Vec<NodeId> {
        use openraft::async_runtime::WatchReceiver;
        let rx = self.raft.metrics();
        let m = rx.borrow_watched();
        m.membership_config.membership().nodes().map(|(id, _)| *id).collect()
    }

    /// Changes to leadership or membership: `changed` waits for the next one.
    pub fn watch(&self) -> GroupWatch {
        GroupWatch { rx: self.raft.metrics() }
    }

    /// Add `id` (at `addr`) as a learner and wait until it has caught up.
    pub async fn add_learner(&self, id: NodeId, addr: &str) -> Result<(), ProposeError> {
        self.raft
            .add_learner(id, BasicNode::new(addr), true)
            .await
            .map(|_| ())
            .map_err(|e| ProposeError::Unavailable { group: self.id, reason: e.to_string() })
    }

    /// Replace the voter set (joint consensus). Removed voters leave the
    /// group entirely: they are replaced because they are gone.
    pub async fn change_voters(&self, voters: std::collections::BTreeSet<NodeId>) -> Result<(), ProposeError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map(|_| ())
            .map_err(|e| ProposeError::Unavailable { group: self.id, reason: e.to_string() })
    }

    /// Hand leadership to voter `to` (it takes over once caught up).
    pub async fn transfer_leader(&self, to: NodeId) -> Result<(), ProposeError> {
        self.raft
            .trigger()
            .transfer_leader(to)
            .await
            .map_err(|e| ProposeError::Unavailable { group: self.id, reason: e.to_string() })
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

#[cfg(test)]
mod tests;
