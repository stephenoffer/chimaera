//! The raft files' recovery a start makes before it serves, for the tools
//! that work on a stopped peer's directory (backup, restore, recover) as well:
//! they read what the peer's next start would read.

use crate::log_store::{LOG, LOG_META, dec, io_err};
use crate::state_machine::{self, SM_META, apply_batch};
use crate::{DEFAULT_CACHE_BYTES, DiskDurability, TypeConfig, create_tables, open_db, wal};
use chimaera_core::shard::ShardState;
use openraft::type_config::alias::{EntryOf, LogIdOf};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::io;
use std::path::Path;

/// Open the databases under `dir` as a start does: a state machine a machine's
/// restart may have torn (its marker says another boot ran on it) is rebuilt
/// from the synced snapshot files, and the synced log replays the rest; what
/// the write-ahead log holds goes into the log database first.
pub(crate) fn recover_files(
    dir: &Path,
    durability: DiskDurability,
    cache_bytes: usize,
) -> io::Result<(Database, Database, Option<wal::Wal>)> {
    std::fs::create_dir_all(dir.join("snapshots"))?;
    let (log_path, sm_path) = (dir.join("raft-log.redb"), dir.join("raft-sm.redb"));
    // A peer that syncs its log keeps its state machine's file unsynced:
    // syncing it cost ~600 write IOs a checkpoint (redb's copy-on-write
    // pages), most of a network volume's IOPS. After a machine restart
    // without a clean stop it may be torn: it is rebuilt from the synced
    // snapshot files, and the synced log replays the rest.
    let unsynced_sm = durability != DiskDurability::Process;
    let rebuild = unsynced_sm && !state_machine::sm_trusted(dir);
    if rebuild {
        tracing::warn!(
            "{}: the machine restarted without a clean stop: rebuilding the raft state from snapshots and the log",
            dir.display()
        );
        let _ = std::fs::remove_file(&sm_path);
    }
    let log_db = open_db(&log_path, durability, cache_bytes)?;
    let sm_db = open_db(&sm_path, DiskDurability::Process, cache_bytes)?;
    create_tables(&log_db, &sm_db)?;
    if rebuild {
        let groups = state_machine::rebuild_from_snapshots(&sm_db, &dir.join("snapshots"))?;
        tracing::info!("raft state rebuilt: {groups} group(s) from snapshots");
    }
    if unsynced_sm {
        state_machine::mark_sm(dir, false)?;
    }
    // A syncing peer writes its log through a wal (crate::wal): first what
    // the last run left there goes into redb.
    let wal = match durability {
        DiskDurability::Process => None,
        d => {
            let (wal, replayed) =
                wal::Wal::recover(&log_db, &dir.join("raft-log.wal"), matches!(d, DiskDurability::Full))?;
            if replayed > 0 {
                tracing::info!("raft log: {replayed} operations replayed from its write-ahead log");
            }
            Some(wal)
        }
    };
    Ok((log_db, sm_db, wal))
}

/// Bring a stopped peer's raft directory to what its next start would make of
/// it: the write-ahead log replayed into the log database (synced), and a state
/// machine the machine's restart may have torn rebuilt from the snapshot
/// files, synced and marked clean. A directory a running peer holds is refused
/// (redb lets one process open a file). Idempotent; a directory without a
/// write-ahead log, or with a clean state machine, is left as it is.
pub fn settle_files(dir: &Path) -> io::Result<()> {
    // The write-ahead log and the state machine's marker exist only for a
    // peer that syncs its log (DiskDurability Full or Os).
    let synced = dir.join("raft-log.wal").exists() || state_machine::has_marker(dir);
    let (_log_db, _sm_db, wal) =
        recover_files(dir, if synced { DiskDurability::Full } else { DiskDurability::Process }, DEFAULT_CACHE_BYTES)?;
    drop(wal);
    drop((_log_db, _sm_db));
    if synced { state_machine::mark_state_machine_clean(dir) } else { Ok(()) }
}

/// Apply to group `group`'s state machine every entry its log holds as
/// committed beyond what was applied. Returns the position it reached.
pub(crate) fn apply_committed(
    log_db: &Database,
    sm_db: &Database,
    group: u32,
) -> io::Result<Option<LogIdOf<TypeConfig>>> {
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
        (entries.last().map(|e| e.log_id).or(applied), entries)
    };
    if !entries.is_empty() {
        apply_batch(sm_db, group, &ShardState::new(), entries.into_iter().map(|e| (e, None)).collect(), true)?;
    }
    Ok(reached)
}
