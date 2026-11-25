//! A voter told to replace or drop entries it committed. Only a group whose
//! majority lost entries it had acknowledged before they reached the disk,
//! together, sends that: a correlated power loss under `CHIMAERA_RAFT_SYNC`
//! `none`. The voter refuses, so it never builds on or serves a state the
//! group no longer has, and the peer's hook makes it rejoin as a new member.
//!
//! openraft 0.10.1 does not stop this itself. It takes every index up to its
//! commit point as matching (`has_log_id`): a debug build panics there, and a
//! release build appends the leader's entries after the ones the group lost,
//! without asking to truncate, and applies them. A snapshot at or below the
//! commit point that contradicts it fails an `assert!`, in release builds
//! too. So the receiving side checks what a leader sends before openraft sees
//! it ([`refuse_rewrite`], [`refuse_snapshot`]), and the log store refuses to
//! truncate committed entries (`LogStore::truncate_after`).

use crate::TypeConfig;
use crate::log_store::{LogStore, io_err};
use openraft::raft::AppendEntriesRequest;
use openraft::storage::RaftLogReader;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf};
use std::collections::BTreeMap;
use std::io;
use std::sync::OnceLock;

type Hook = Box<dyn Fn(u32) + Send + Sync>;
static DIVERGED: OnceLock<Hook> = OnceLock::new();

/// Called with the group id each time a voter refuses a leader's entries or
/// snapshot that contradict what it committed, or a truncation of committed
/// entries (see the module). The peer installs a hook that marks its raft
/// state untrusted and exits, so it rejoins as a new member.
pub fn set_divergence_hook(f: impl Fn(u32) + Send + Sync + 'static) {
    let _ = DIVERGED.set(Box::new(f));
}

pub(crate) fn diverged(group: u32) {
    if let Some(f) = DIVERGED.get() {
        f(group);
    }
}

/// Refuse an append whose entries (or the entry it follows) contradict this
/// voter's log at an index it committed.
pub(crate) async fn refuse_rewrite(log: &LogStore, req: &AppendEntriesRequest<TypeConfig>) -> io::Result<()> {
    let ids = req.prev_log_id.iter().chain(req.entries.iter().map(|e| &e.log_id)).copied().collect();
    refuse(log, ids).await
}

/// Refuse a snapshot that ends at an index this voter committed with an
/// entry other than the one it has.
pub(crate) async fn refuse_snapshot(log: &LogStore, meta: &SnapshotMetaOf<TypeConfig>) -> io::Result<()> {
    refuse(log, meta.last_log_id.into_iter().collect()).await
}

async fn refuse(log: &LogStore, theirs: Vec<LogIdOf<TypeConfig>>) -> io::Result<()> {
    let Some(committed) = *log.committed.lock().expect("committed lock") else { return Ok(()) };
    let theirs: Vec<LogIdOf<TypeConfig>> = theirs.into_iter().filter(|l| l.index <= committed.index).collect();
    // Log ids only grow along a log: one at or below the commit point's
    // index that is greater than the commit point is not in this log.
    let mut conflict = theirs.iter().copied().find(|l| *l > committed);
    if conflict.is_none() && !theirs.is_empty() && theirs[..] != [committed] {
        let (first, last) = (theirs[0].index, theirs[theirs.len() - 1].index);
        let mine: BTreeMap<u64, LogIdOf<TypeConfig>> = log
            .clone()
            .try_get_log_entries(first..=last)
            .await?
            .into_iter()
            .map(|e| (e.log_id.index, e.log_id))
            .collect();
        // An index this voter no longer has (purged into its snapshot) cannot be compared.
        conflict = theirs.into_iter().find(|l| mine.get(&l.index).is_some_and(|m| m != l));
    }
    let Some(conflict) = conflict else { return Ok(()) };
    diverged(log.group);
    Err(io_err(format!(
        "group {}: the leader holds {conflict}, which contradicts this voter's log, committed up to {committed}; \
         only a majority of the group losing entries it had acknowledged unsynced can cause that, so this voter \
         leaves the group",
        log.group
    )))
}
