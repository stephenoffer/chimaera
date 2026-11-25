//! Raft log storage for many groups in one redb database.
//!
//! Every write goes through one [`DiskWriter`] thread, which drains whatever is
//! queued (from all groups), commits it in one transaction with a single fsync,
//! and then completes each operation. Vote and append share that FIFO, so a
//! vote is never reordered behind a later append.
//!
//! On a disk whose commits take milliseconds (a network volume), operations
//! that arrive a moment apart, such as one group's append and the next
//! group's, would each wait a whole commit behind the one before. So while
//! each of the last `PACE` commits took a millisecond or more and operations
//! keep company, the writer holds each batch open for a quarter of the
//! fastest of those commits, never more than the host's maximum
//! (`CHIMAERA_RAFT_MAX_COMMIT_DELAY_US`), counted from when it could first
//! have taken the batch's first operation: the operation's arrival, or the
//! end of the commit it queued behind, whose completions bring the next
//! operations. So no operation waits for company longer than the maximum.
//! One slow commit on a fast disk (another process's burst of writes) holds
//! nothing back. Every commit still syncs: holding changes how many
//! operations share an fsync, never whether they get one.
//!
//! openraft requires appended entries to be readable as soon as `append`
//! returns, before they are flushed, so each group keeps its unflushed tail in
//! memory. Each buffered entry carries the sequence number of the append that
//! wrote it; a completion removes only entries it wrote, so a truncation
//! followed by a new append at the same index is never clobbered.

use crate::TypeConfig;
use openraft::OptionalSend;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::wal::{Wal, WalOp};

pub(crate) const LOG: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("log");
pub(crate) const LOG_META: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("log_meta");

const MAX_BATCH_OPS: usize = 4096;
const MAX_BATCH_BYTES: usize = 4 << 20;
/// A commit slower than this is worth holding a batch open for; a faster
/// disk commits each operation before many more would arrive.
pub const SLOW_COMMIT: Duration = Duration::from_millis(1);
/// The default most a batch is held open (`CHIMAERA_RAFT_MAX_COMMIT_DELAY_US`).
pub const MAX_COMMIT_DELAY: Duration = Duration::from_millis(2);
/// Commits the writer's report covers.
const RECENT: usize = 512;
/// Commits that must all be slow before the writer holds batches open.
const PACE: usize = 8;

pub(crate) fn io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

pub(crate) fn enc<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
    postcard::to_stdvec(v).map_err(io_err)
}

pub(crate) fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> io::Result<T> {
    postcard::from_bytes(b).map_err(io_err)
}

type Done = tokio::sync::oneshot::Sender<io::Result<()>>;

enum Op {
    Append { group: u32, seq: u64, entries: Vec<(u64, Vec<u8>)>, cb: IOFlushed<TypeConfig> },
    Meta { group: u32, key: &'static str, value: Option<Vec<u8>>, done: Done },
    Truncate { group: u32, from: u64, done: Done },
    Purge { group: u32, upto: u64, record: Vec<u8>, done: Done },
}

impl Op {
    /// What the batch writes, for the wal and redb.
    fn wal(&self) -> WalOp {
        match self {
            Op::Append { group, entries, .. } => WalOp::Append { group: *group, entries: entries.clone() },
            Op::Meta { group, key, value, .. } => {
                WalOp::Meta { group: *group, key: (*key).to_string(), value: value.clone() }
            }
            Op::Truncate { group, from, .. } => WalOp::Truncate { group: *group, from: *from },
            Op::Purge { group, upto, record, .. } => {
                WalOp::Purge { group: *group, upto: *upto, record: record.clone() }
            }
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Op::Append { entries, .. } => entries.iter().map(|(_, e)| e.len() + 8).sum(),
            Op::Meta { value, .. } => value.as_ref().map_or(0, Vec::len),
            _ => 16,
        }
    }
}

/// Unflushed tail of one group: index -> (append seq, encoded entry).
type Tail = Arc<Mutex<BTreeMap<u64, (u64, Vec<u8>)>>>;

/// How the disk writer has been doing, for status.
#[derive(Debug, Default)]
pub struct DiskStats {
    commits: AtomicU64,
    operations: AtomicU64,
    /// The last `RECENT` commits: how long each took, how long its oldest
    /// operation waited from queueing to being on disk, and how long the
    /// writer meant to hold it open (µs; the scheduler may wake it later).
    recent: Mutex<std::collections::VecDeque<[u32; 3]>>,
}

/// A summary of [`DiskStats`] over the recent commits.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DiskReport {
    pub commits: u64,
    pub operations: u64,
    pub commit_p50_us: u32,
    pub commit_p99_us: u32,
    pub wait_p99_us: u32,
    pub hold_max_us: u32,
}

impl DiskStats {
    fn record(&self, ops: usize, took: Duration, waited: Duration, held: Duration) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.operations.fetch_add(ops as u64, Ordering::Relaxed);
        let us = |d: Duration| d.as_micros().min(u128::from(u32::MAX)) as u32;
        let mut r = self.recent.lock().expect("stats lock");
        if r.len() == RECENT {
            r.pop_front();
        }
        r.push_back([us(took), us(waited), us(held)]);
    }

    pub fn report(&self) -> DiskReport {
        let r = self.recent.lock().expect("stats lock");
        let column = |i: usize| {
            let mut v: Vec<u32> = r.iter().map(|x| x[i]).collect();
            v.sort_unstable();
            v
        };
        let at = |v: &[u32], q: f64| v.get(((v.len().max(1) - 1) as f64 * q).round() as usize).copied().unwrap_or(0);
        let (took, waited, held) = (column(0), column(1), column(2));
        DiskReport {
            commits: self.commits.load(Ordering::Relaxed),
            operations: self.operations.load(Ordering::Relaxed),
            commit_p50_us: at(&took, 0.5),
            commit_p99_us: at(&took, 0.99),
            wait_p99_us: at(&waited, 0.99),
            hold_max_us: held.last().copied().unwrap_or(0),
        }
    }
}

/// How long to hold a batch open after its first operation queued, given the
/// last `PACE` commits' durations and whether operations keep company.
fn hold(recent: &[Duration; PACE], company: bool, max: Duration) -> Duration {
    let fastest = recent.iter().min().copied().unwrap_or_default();
    if !company || fastest < SLOW_COMMIT { Duration::ZERO } else { (fastest / 4).min(max) }
}

#[derive(Clone)]
pub struct DiskWriter {
    tx: mpsc::Sender<(Instant, Op)>,
    tails: Arc<Mutex<BTreeMap<u32, Tail>>>,
    /// Append sequence numbers, unique across every store on this writer:
    /// two store instances of one group (sharing its tail) can never trim
    /// each other's entries.
    seq: Arc<std::sync::atomic::AtomicU64>,
    stats: Arc<DiskStats>,
}

impl DiskWriter {
    /// Start the writer; a batch is held open at most `max_hold` for company.
    /// With a `wal`, each batch is synced there and committed to redb unsynced
    /// (see `crate::wal`); without, each commit syncs redb (or nothing does).
    pub(crate) fn start(db: Arc<Database>, max_hold: Duration, wal: Option<Wal>) -> io::Result<DiskWriter> {
        let (tx, rx) = mpsc::channel::<(Instant, Op)>();
        let tails: Arc<Mutex<BTreeMap<u32, Tail>>> = Arc::default();
        let stats: Arc<DiskStats> = Arc::default();
        let (t2, s2) = (tails.clone(), stats.clone());
        std::thread::Builder::new()
            .name("chimaera-raft-disk".into())
            .spawn(move || run_writer(&db, &rx, &t2, &s2, max_hold, wal))?;
        Ok(DiskWriter { tx, tails, seq: Arc::default(), stats })
    }

    pub fn stats(&self) -> Arc<DiskStats> {
        self.stats.clone()
    }

    fn tail(&self, group: u32) -> Tail {
        self.tails.lock().expect("tail lock").entry(group).or_default().clone()
    }

    async fn call(&self, make: impl FnOnce(Done) -> Op) -> io::Result<()> {
        let (done, rx) = tokio::sync::oneshot::channel();
        self.tx.send((Instant::now(), make(done))).map_err(|_| io_err("raft disk writer stopped"))?;
        rx.await.map_err(|_| io_err("raft disk writer dropped a request"))?
    }
}

fn run_writer(
    db: &Database,
    rx: &mpsc::Receiver<(Instant, Op)>,
    tails: &Mutex<BTreeMap<u32, Tail>>,
    stats: &DiskStats,
    max_hold: Duration,
    mut wal: Option<Wal>,
) {
    let mut recent = [Duration::ZERO; PACE]; // the last commits' durations
    let (mut last_end, mut last_len, mut commits) = (Instant::now(), 0, 0);
    loop {
        // Idle with batches only in the wal: checkpoint once it is due.
        let waiting = wal.as_ref().is_some_and(|w| w.written() > 0);
        let (queued, first) =
            match rx.recv_timeout(if waiting { crate::wal::CHECKPOINT } else { Duration::from_secs(3600) }) {
                Ok(item) => item,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    checkpoint(db, wal.as_mut());
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
        let got = Instant::now();
        // Company is likely when the last batch had some, or this operation
        // queued while the last commit ran (the disk set the pace).
        let company = last_len > 1 || queued < last_end;
        let until = queued.max(last_end) + hold(&recent, company, max_hold);
        let mut bytes = first.bytes();
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH_OPS && bytes < MAX_BATCH_BYTES {
            let next = match rx.try_recv() {
                Ok((_, op)) => Some(op),
                Err(mpsc::TryRecvError::Empty) => match until.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => rx.recv_timeout(left).ok().map(|(_, op)| op),
                    _ => None,
                },
                Err(mpsc::TryRecvError::Disconnected) => None,
            };
            let Some(op) = next else { break };
            bytes += op.bytes();
            batch.push(op);
        }
        let start = Instant::now();
        let ops: Vec<WalOp> = batch.iter().map(Op::wal).collect();
        let result = match wal.as_mut() {
            // Synced in the wal first: once that returns, the batch is safe;
            // redb's copy reaches the disk at the next checkpoint.
            Some(w) => w
                .append(&ops)
                .map_err(|e| e.to_string())
                .and_then(|()| crate::wal::apply(db, &ops, redb::Durability::None).map_err(|e| e.to_string())),
            None => crate::wal::apply(db, &ops, redb::Durability::Immediate).map_err(|e| e.to_string()),
        };
        let end = Instant::now();
        recent[commits % PACE] = end - start;
        commits += 1;
        stats.record(batch.len(), end - start, end - queued, until.saturating_duration_since(got));
        last_len = batch.len();
        for op in batch {
            let r = result.clone().map_err(io_err);
            match op {
                Op::Append { group, seq, entries, cb } => {
                    if r.is_ok()
                        && let Some(tail) = tails.lock().expect("tail lock").get(&group).cloned()
                    {
                        let mut t = tail.lock().expect("tail lock");
                        for (idx, _) in &entries {
                            if t.get(idx).is_some_and(|(s, _)| *s == seq) {
                                t.remove(idx);
                            }
                        }
                    }
                    cb.io_completed(r);
                }
                Op::Meta { done, .. } | Op::Truncate { done, .. } | Op::Purge { done, .. } => {
                    let _ = done.send(r);
                }
            }
        }
        last_end = Instant::now(); // its completions are out
        if wal.as_ref().is_some_and(Wal::due) {
            checkpoint(db, wal.as_mut());
        }
    }
}

/// Persist redb and empty the wal; on failure the wal keeps its records
/// (replayed at start), and the next checkpoint tries again.
fn checkpoint(db: &Database, wal: Option<&mut Wal>) {
    if let Some(w) = wal
        && let Err(e) = w.checkpoint(db)
    {
        tracing::warn!("raft log checkpoint failed (the wal keeps its records): {e}");
    }
}

/// Log storage handle for one group.
#[derive(Clone)]
pub struct LogStore {
    group: u32,
    db: Arc<Database>,
    writer: DiskWriter,
    tail: Tail,
    /// Bytes appended since the group's last snapshot (shared with its state machine).
    pub(crate) due: Arc<crate::state_machine::SnapshotDue>,
}

impl LogStore {
    pub fn new(group: u32, db: Arc<Database>, writer: DiskWriter) -> LogStore {
        let tail = writer.tail(group);
        LogStore { group, db, writer, tail, due: Arc::default() }
    }

    fn read_meta(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read().map_err(io_err)?;
        let t = txn.open_table(LOG_META).map_err(io_err)?;
        Ok(t.get((self.group, key)).map_err(io_err)?.map(|v| v.value().to_vec()))
    }

    fn last_on_disk(&self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
        let txn = self.db.begin_read().map_err(io_err)?;
        let t = txn.open_table(LOG).map_err(io_err)?;
        let mut range = t.range((self.group, 0)..=(self.group, u64::MAX)).map_err(io_err)?;
        match range.next_back() {
            Some(r) => {
                let (_, v) = r.map_err(io_err)?;
                let e: EntryOf<TypeConfig> = dec(v.value())?;
                Ok(Some(e.log_id))
            }
            None => Ok(None),
        }
    }
}

fn bounds<RB: RangeBounds<u64>>(r: &RB) -> (u64, u64) {
    let start = match r.start_bound() {
        Bound::Included(s) => *s,
        Bound::Excluded(s) => s + 1,
        Bound::Unbounded => 0,
    };
    let end = match r.end_bound() {
        Bound::Included(e) => e.saturating_add(1),
        Bound::Excluded(e) => *e,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let (start, end) = bounds(&range);
        if start >= end {
            return Ok(Vec::new());
        }
        // The tail first, then the disk: the writer trims an entry from the
        // tail only after committing it, so an entry missing from this tail
        // copy was committed before the read transaction below began. (Disk
        // first lets an entry flush and leave the tail between the two reads,
        // and the read returns a gap that openraft's replication asserts on.)
        for _ in 0..3 {
            let mut out: BTreeMap<u64, Vec<u8>> =
                self.tail.lock().expect("tail lock").range(start..end).map(|(i, (_, e))| (*i, e.clone())).collect();
            {
                let txn = self.db.begin_read().map_err(io_err)?;
                let t = txn.open_table(LOG).map_err(io_err)?;
                for r in t.range((self.group, start)..(self.group, end)).map_err(io_err)? {
                    let (k, v) = r.map_err(io_err)?;
                    // Unflushed entries are newer than anything on disk at the same index.
                    out.entry(k.value().1).or_insert_with(|| v.value().to_vec());
                }
            }
            // No hole between the first entry found and the last. (A range
            // may legitimately begin before the log does: index 0, or a
            // purged prefix.)
            let first = out.keys().next().copied().unwrap_or(start);
            let contiguous = out.keys().copied().eq(first..first + out.len() as u64);
            // Starting late is only legal where purge removed the prefix.
            let late = first > start && !out.is_empty() && {
                let purged: Option<LogIdOf<TypeConfig>> = self.read_meta("purged")?.map(|b| dec(&b)).transpose()?;
                purged.is_none_or(|p| first > p.index + 1) && start > 0
            };
            if late {
                tracing::warn!(
                    group = self.group,
                    start,
                    first,
                    "log read began after its start above the purge point; reading again"
                );
                continue;
            }
            if contiguous {
                return out.values().map(|b| dec(b)).collect();
            }
        }
        Err(io_err(format!(
            "group {}: log entries {start}..{end} are not contiguous on disk and in memory",
            self.group
        )))
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        self.read_meta("vote")?.map(|b| dec(&b)).transpose()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged: Option<LogIdOf<TypeConfig>> = self.read_meta("purged")?.map(|b| dec(&b)).transpose()?;
        let tail_last = {
            let t = self.tail.lock().expect("tail lock");
            t.values().next_back().map(|(_, e)| dec::<EntryOf<TypeConfig>>(e).map(|e| e.log_id)).transpose()?
        };
        let last = tail_last.or(self.last_on_disk()?).or(last_purged);
        Ok(LogState { last_purged_log_id: last_purged, last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        let value = enc(vote)?;
        let group = self.group;
        self.writer.call(|done| Op::Meta { group, key: "vote", value: Some(value), done }).await
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<TypeConfig>>) -> Result<(), io::Error> {
        let value = committed.as_ref().map(enc).transpose()?;
        let group = self.group;
        self.writer.call(|done| Op::Meta { group, key: "committed", value, done }).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        self.read_meta("committed")?.map(|b| dec(&b)).transpose()
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let seq = self.writer.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let encoded: Vec<(u64, Vec<u8>)> =
            entries.into_iter().map(|e| Ok((e.log_id.index, enc(&e)?))).collect::<io::Result<_>>()?;
        let bytes: usize = encoded.iter().map(|(_, e)| e.len()).sum();
        self.due.log_bytes.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
        {
            let mut t = self.tail.lock().expect("tail lock");
            for (idx, e) in &encoded {
                t.insert(*idx, (seq, e.clone()));
            }
        }
        self.writer
            .tx
            .send((Instant::now(), Op::Append { group: self.group, seq, entries: encoded, cb: callback }))
            .map_err(|_| io_err("raft disk writer stopped"))
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<TypeConfig>>) -> Result<(), io::Error> {
        let from = last_log_id.map_or(0, |l| l.index + 1);
        self.tail.lock().expect("tail lock").retain(|idx, _| *idx < from);
        let group = self.group;
        self.writer.call(|done| Op::Truncate { group, from, done }).await
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        let upto = log_id.index;
        self.tail.lock().expect("tail lock").retain(|idx, _| *idx > upto);
        let record = enc(&log_id)?;
        let group = self.group;
        self.writer.call(|done| Op::Purge { group, upto, record, done }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_disk_whose_recent_commits_were_all_slow_holds_batches_open() {
        let (ms, max) = (Duration::from_millis, MAX_COMMIT_DELAY);
        // A fast disk with one slow commit among the last eight (another
        // process wrote a burst): nothing is held.
        let mut spiked = [Duration::from_micros(80); PACE];
        spiked[3] = ms(40);
        assert_eq!(hold(&spiked, true, max), Duration::ZERO);
        // Fewer than eight commits so far: nothing is held.
        let mut fresh = [Duration::ZERO; PACE];
        fresh[0] = ms(5);
        assert_eq!(hold(&fresh, true, max), Duration::ZERO);
        // A network volume: a quarter of its fastest recent commit, at most `max`.
        let volume = [ms(5), ms(4), ms(6), ms(5), ms(5), ms(5), ms(7), ms(5)];
        assert_eq!(hold(&volume, true, max), ms(1));
        assert_eq!(hold(&[ms(20); PACE], true, max), max);
        assert_eq!(hold(&volume, true, Duration::ZERO), Duration::ZERO);
        // An operation with no company is never held.
        assert_eq!(hold(&volume, false, max), Duration::ZERO);
    }
}
