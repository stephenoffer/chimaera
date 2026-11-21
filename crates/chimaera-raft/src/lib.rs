//! Multi-raft host: many shard groups per peer on one log database and one
//! state-machine database, with group commit across groups.
//!
//! The rest of Chimaera talks to a group through [`Group::propose`] and
//! [`Group::read`]; openraft stays an implementation detail of this crate.

mod backend;
mod log_store;
mod network;
mod state_machine;

pub use log_store::{DiskWriter, LogStore};
pub use network::{NetworkFactory, NoTransport, RpcFuture, RpcKind, Transport, handle_rpc};
pub use state_machine::{GroupEvents, GroupRead, StateMachine};

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
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(50_000),
        max_in_snapshot_log_to_keep: 1000,
        ..Default::default()
    };
    Arc::new(cfg.validate().expect("static raft config is valid"))
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

/// Opens the peer's raft databases and starts groups on them.
pub struct RaftHost {
    node_id: NodeId,
    log_db: Arc<Database>,
    sm_db: Arc<Database>,
    writer: DiskWriter,
    snapshot_dir: PathBuf,
    events: tokio::sync::mpsc::Sender<GroupEvents>,
    config: Arc<Config>,
    transport: Arc<dyn Transport>,
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
        std::fs::create_dir_all(dir.join("snapshots"))?;
        let log_db = Arc::new(open_db(&dir.join("raft-log.redb"), durability)?);
        let sm_db = Arc::new(open_db(&dir.join("raft-sm.redb"), durability)?);
        create_tables(&log_db, &sm_db)?;
        let writer = DiskWriter::start(log_db.clone())?;
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let host = RaftHost {
            node_id,
            log_db,
            sm_db,
            writer,
            snapshot_dir: dir.join("snapshots"),
            events: tx,
            config: raft_config(),
            transport,
        };
        Ok((host, rx))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
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
        let sm = StateMachine::new(id, self.sm_db.clone(), self.events.clone(), self.snapshot_dir.clone());
        let net = NetworkFactory::new(id, self.transport.clone());
        let raft = Raft::new(self.node_id, self.config.clone(), net, log, sm).await.map_err(log_store::io_err)?;
        if bootstrap && !raft.is_initialized().await.map_err(log_store::io_err)? {
            raft.initialize(members).await.map_err(log_store::io_err)?;
        }
        Ok(Group { id, node_id: self.node_id, raft, db: self.sm_db.clone() })
    }
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

/// One shard group hosted on this peer.
#[derive(Clone)]
pub struct Group {
    pub id: u32,
    node_id: NodeId,
    pub raft: Raft,
    db: Arc<Database>,
}

impl Group {
    /// Replicate `cmd` and return the state machine's reply. Only the leader
    /// accepts proposals; others return `NotLeader` with a hint.
    pub async fn propose(&self, cmd: &pb::ShardCmd) -> Result<pb::ShardReply, ProposeError> {
        use openraft::error::{ClientWriteError, RaftError};
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
        let view = GroupRead::open(&self.db, self.id)?;
        Ok(f(&view))
    }

    /// Wait until local state reflects everything committed before this call.
    pub async fn linearizable(&self) -> Result<(), ProposeError> {
        self.raft
            .ensure_linearizable(openraft::raft::ReadPolicy::ReadIndex)
            .await
            .map(|_| ())
            .map_err(|e| ProposeError::Unavailable { group: self.id, reason: e.to_string() })
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// Leader and voters as this replica last saw them.
    pub fn view(&self) -> (Option<NodeId>, Vec<NodeId>) {
        use openraft::async_runtime::WatchReceiver;
        let rx = self.raft.metrics();
        let m = rx.borrow_watched();
        (m.current_leader, m.membership_config.membership().voter_ids().collect())
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

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

#[cfg(test)]
mod tests;
