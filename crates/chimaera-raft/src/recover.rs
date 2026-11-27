//! Rebuild a group around one surviving replica (`chimaera cluster recover`).
//!
//! For a group whose other voters are gone for good: Raft cannot elect a
//! leader without a majority, and openraft has no way to force one. With the
//! peer stopped, this rewrites its storage so the group's committed state, as
//! this replica holds it, belongs to a group whose only voter is this node.
//! Started again, the node elects itself and the cluster adds voters back.
//! Commits this replica never received are lost.

use crate::log_store::{LOG, LOG_META, dec, enc, io_err};
use crate::state_machine::{SM_META, apply_batch, write_group_snapshot};
use crate::{BasicNode, DEFAULT_CACHE_BYTES, DiskDurability, NodeId, TypeConfig, open_db};
use chimaera_core::shard::ShardState;
use openraft::type_config::alias::{EntryOf, LogIdOf, StoredMembershipOf};
use redb::{ReadableDatabase, ReadableTable};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

/// Make node `me` the only voter of group `group` in the raft state under
/// `dir` (the peer must be stopped). Returns the log index the group resumes
/// from.
pub fn force_single_voter(dir: &Path, group: u32, me: NodeId) -> io::Result<u64> {
    // What the peer's next start would read: the write-ahead log replayed,
    // and a state machine its unclean stop may have left behind rebuilt.
    crate::settle::settle_files(dir)?;
    let log_db = open_db(&dir.join("raft-log.redb"), DiskDurability::Os, DEFAULT_CACHE_BYTES)?;
    let sm_db = open_db(&dir.join("raft-sm.redb"), DiskDurability::Os, DEFAULT_CACHE_BYTES)?;

    // Every committed entry this replica holds, applied.
    let (reached, entries) = {
        let txn = log_db.begin_read().map_err(io_err)?;
        let meta = txn.open_table(LOG_META).map_err(io_err)?;
        let committed: Option<LogIdOf<TypeConfig>> =
            meta.get((group, "committed")).map_err(io_err)?.map(|v| dec(v.value())).transpose()?;
        let sm = sm_db.begin_read().map_err(io_err)?;
        let applied: Option<LogIdOf<TypeConfig>> = sm
            .open_table(SM_META)
            .map_err(io_err)?
            .get((group, "applied"))
            .map_err(io_err)?
            .map(|v| dec::<Option<LogIdOf<TypeConfig>>>(v.value()))
            .transpose()?
            .flatten();
        let from = applied.as_ref().map_or(0, |l| l.index + 1);
        let to = committed.as_ref().map_or(0, |l| l.index);
        let log = txn.open_table(LOG).map_err(io_err)?;
        let mut entries: Vec<EntryOf<TypeConfig>> = Vec::new();
        if committed.is_some() && from <= to {
            for r in log.range((group, from)..=(group, to)).map_err(io_err)? {
                entries.push(dec(r.map_err(io_err)?.1.value())?);
            }
        }
        let reached = entries.last().map(|e| e.log_id).or(applied);
        (reached, entries)
    };
    if !entries.is_empty() {
        apply_batch(&sm_db, group, &ShardState::new(), entries.into_iter().map(|e| (e, None)).collect(), true)?;
    }

    // This node alone, at the same address.
    {
        let txn = sm_db.begin_write().map_err(io_err)?;
        {
            let mut meta = txn.open_table(SM_META).map_err(io_err)?;
            let stored: StoredMembershipOf<TypeConfig> = meta
                .get((group, "membership"))
                .map_err(io_err)?
                .map(|v| dec(v.value()))
                .transpose()?
                .ok_or_else(|| io_err(format!("this peer holds no state for group {group}")))?;
            let addr = stored
                .membership()
                .get_node(&me)
                .ok_or_else(|| io_err(format!("node {me} is not a member of group {group}")))?
                .addr
                .clone();
            let alone =
                openraft::Membership::new(vec![BTreeSet::from([me])], BTreeMap::from([(me, BasicNode::new(addr))]))
                    .map_err(io_err)?;
            let recovered = openraft::StoredMembership::new(reached, alone);
            meta.insert((group, "membership"), enc(&recovered)?.as_slice()).map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;
    }
    let snapshots = dir.join("snapshots");
    std::fs::create_dir_all(&snapshots)?;
    let (meta, _, _) = write_group_snapshot(&sm_db, group, &snapshots)?;

    // The log starts after the snapshot: old memberships and entries this
    // replica never saw committed must not come back.
    let txn = log_db.begin_write().map_err(io_err)?;
    {
        let mut log = txn.open_table(LOG).map_err(io_err)?;
        log.retain_in((group, 0)..=(group, u64::MAX), |_, _| false).map_err(io_err)?;
        let mut m = txn.open_table(LOG_META).map_err(io_err)?;
        match &meta.last_log_id {
            Some(last) => {
                m.insert((group, "purged"), enc(last)?.as_slice()).map_err(io_err)?;
                m.insert((group, "committed"), enc(last)?.as_slice()).map_err(io_err)?;
            }
            None => {
                m.remove((group, "purged")).map_err(io_err)?;
                m.remove((group, "committed")).map_err(io_err)?;
            }
        }
    }
    txn.commit().map_err(io_err)?;
    Ok(meta.last_log_id.map_or(0, |l| l.index))
}
