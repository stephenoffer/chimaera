//! A cell rebuilt from backed-up group snapshots (`chimaera cluster backup`
//! and `chimaera cluster restore`).
//!
//! Backup works on a stopped peer: its files settled as its next start would
//! settle them, every committed entry applied, then each group's state taken
//! as a snapshot, the peer's own, which is copied out. Restore works on an
//! empty directory. Every file is checked before anything is written; then
//! each group's snapshot becomes a group whose only voter is the restoring
//! node, with its log starting after the snapshot, as `cluster recover` leaves
//! a group. The peer incarnations the state names (grants, hosted actors,
//! leases) belonged to the lost cell, and are returned for retiring.

use crate::log_store::{LOG_META, enc, io_err};
use crate::settle::{apply_committed, recover_files, settle_files};
use crate::state_machine::{
    SM, SM_META, mark_state_machine_clean, prefix, rebuild_from_snapshots, snapshot_path, write_group_snapshot,
};
use crate::{BasicNode, DiskDurability, NodeId, TypeConfig, snapshot_file};
use chimaera_core::shard::keys;
use openraft::type_config::alias::{StoredMembershipOf, VoteOf};
use redb::{ReadableDatabase, ReadableTable};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};

/// One group's snapshot in a backup.
#[derive(Clone, Debug, PartialEq)]
pub struct Backed {
    pub group: u32,
    /// The log index the snapshot holds the state at.
    pub index: u64,
    pub file: PathBuf,
}

/// What a restore installed.
#[derive(Debug, Default)]
pub struct Restored {
    /// Each group and the log index it resumes after.
    pub groups: Vec<(u32, u64)>,
    /// (group, peer id, incarnation) the restored state names: the lost
    /// cell's peers, to retire once the group runs.
    pub retire: Vec<(u32, Vec<u8>, u64)>,
}

/// Snapshot every group of the stopped peer whose raft directory is `dir`,
/// at everything it holds as committed, and copy each file into `out`.
pub fn backup(dir: &Path, out: &Path) -> io::Result<Vec<Backed>> {
    settle_files(dir)?;
    let (log_db, sm_db, _) = recover_files(dir, DiskDurability::Process)?;
    let snapshots = dir.join("snapshots");
    std::fs::create_dir_all(out)?;
    let mut backed = Vec::new();
    for group in groups(&sm_db)? {
        apply_committed(&log_db, &sm_db, group)?;
        let (meta, path, _) = write_group_snapshot(&sm_db, group, &snapshots)?;
        let file = out.join(path.file_name().expect("a snapshot file has a name"));
        std::fs::copy(&path, &file)?;
        std::fs::File::open(&file)?.sync_all()?;
        snapshot_file::sync_dir(&file)?;
        snapshot_file::check(&file, group)?; // the copy is whole
        backed.push(Backed { group, index: meta.last_log_id.map_or(0, |l| l.index), file });
    }
    // The applies and snapshot pointers just written, synced before the
    // state machine is said to be clean again.
    drop((log_db, sm_db));
    mark_state_machine_clean(dir)?;
    Ok(backed)
}

/// The groups the state machine holds state for.
fn groups(sm_db: &redb::Database) -> io::Result<Vec<u32>> {
    let txn = sm_db.begin_read().map_err(io_err)?;
    let meta = txn.open_table(SM_META).map_err(io_err)?;
    let mut out = BTreeSet::new();
    for r in meta.iter().map_err(io_err)? {
        let (k, _) = r.map_err(io_err)?;
        let (group, key) = k.value();
        if key == "membership" {
            out.insert(group);
        }
    }
    Ok(out.into_iter().collect())
}

/// Install `backed` into the empty raft directory `dir`, each group with
/// node `me` (reached at `addr`) as its only voter. Every file must be whole,
/// of its group, at its index, and one per group; nothing is written
/// otherwise.
pub fn restore(dir: &Path, backed: &[Backed], me: NodeId, addr: &str) -> io::Result<Restored> {
    for f in ["raft-log.redb", "raft-sm.redb"] {
        if dir.join(f).exists() {
            return Err(io_err(format!("{} already holds raft state: restore into an empty directory", dir.display())));
        }
    }
    let mut checked = BTreeMap::new();
    for b in backed {
        let meta = snapshot_file::check(&b.file, b.group)
            .map_err(|e| io_err(format!("{}: not a whole snapshot of group {}: {e}", b.file.display(), b.group)))?;
        let index = meta.last_log_id.map_or(0, |l| l.index);
        if index != b.index {
            return Err(io_err(format!(
                "{}: holds group {} at log index {index}, but its backup says {}: a file from another backup",
                b.file.display(),
                b.group,
                b.index
            )));
        }
        if checked.insert(b.group, (meta, &b.file)).is_some() {
            return Err(io_err(format!("two snapshots of group {}", b.group)));
        }
    }

    let snapshots = dir.join("snapshots");
    std::fs::create_dir_all(&snapshots)?;
    for (group, (meta, file)) in &checked {
        std::fs::copy(file, snapshot_path(&snapshots, *group, meta))?;
    }
    let (log_db, sm_db, _) = recover_files(dir, DiskDurability::Full)?;
    let restored = rebuild_from_snapshots(&sm_db, &snapshots)?;
    if restored != checked.len() {
        return Err(io_err(format!("{restored} of {} snapshots loaded", checked.len())));
    }
    let mut out = Restored::default();
    for (group, (meta, _)) in &checked {
        let last = meta.last_log_id;
        // This node alone, from the snapshot's position on.
        let alone = openraft::Membership::new(
            vec![BTreeSet::from([me])],
            BTreeMap::from([(me, BasicNode::new(addr.to_string()))]),
        )
        .map_err(io_err)?;
        let stored: StoredMembershipOf<TypeConfig> = openraft::StoredMembership::new(last, alone);
        let txn = sm_db.begin_write().map_err(io_err)?;
        {
            let mut m = txn.open_table(SM_META).map_err(io_err)?;
            m.insert((*group, "membership"), enc(&stored)?.as_slice()).map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;
        // Its snapshot, with that membership (the copy it replaces goes).
        write_group_snapshot(&sm_db, *group, &snapshots)?;
        // The log starts after the snapshot, under the snapshot's term: a
        // vote below it would let this node lead a term older than its log.
        let term = last.as_ref().map_or(0, |l| l.committed_leader_id().term);
        let vote: VoteOf<TypeConfig> = openraft::Vote::new(term, me);
        let txn = log_db.begin_write().map_err(io_err)?;
        {
            let mut m = txn.open_table(LOG_META).map_err(io_err)?;
            m.insert((*group, "vote"), enc(&vote)?.as_slice()).map_err(io_err)?;
            if let Some(last) = &last {
                m.insert((*group, "purged"), enc(last)?.as_slice()).map_err(io_err)?;
                m.insert((*group, "committed"), enc(last)?.as_slice()).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)?;
        out.groups.push((*group, last.map_or(0, |l| l.index)));
        out.retire.extend(incarnations(&sm_db, *group)?.into_iter().map(|(p, i)| (*group, p, i)));
    }
    drop((log_db, sm_db));
    mark_state_machine_clean(dir)?;
    Ok(out)
}

/// The peer incarnations group `group`'s state names as holding attempts,
/// actors or leases.
fn incarnations(sm_db: &redb::Database, group: u32) -> io::Result<BTreeSet<(Vec<u8>, u64)>> {
    let txn = sm_db.begin_read().map_err(io_err)?;
    let t = txn.open_table(SM).map_err(io_err)?;
    let (lo, hi) = (prefix(group), (group + 1).to_be_bytes());
    let mut out = BTreeSet::new();
    for r in t.range(lo.as_slice()..hi.as_slice()).map_err(io_err)? {
        let (k, _) = r.map_err(io_err)?;
        // Group prefix (4 bytes), then vshard (2) and tag, then peer (16) and
        // incarnation (8) for each of these families.
        let k = &k.value()[4..];
        if k.len() >= 27 && [keys::GRANT, keys::ACTOR_HOST, keys::LEASE_HOLDER].contains(&k[2]) {
            out.insert((k[3..19].to_vec(), u64::from_be_bytes(k[19..27].try_into().expect("8 bytes"))));
        }
    }
    Ok(out)
}
