//! Raft log storage for many groups in one redb database.
//!
//! Every write goes through one [`DiskWriter`] thread, which drains whatever is
//! queued (from all groups), commits it in one transaction with a single fsync,
//! and then completes each operation. Vote and append share that FIFO, so a
//! vote is never reordered behind a later append.
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
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

pub(crate) const LOG: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("log");
pub(crate) const LOG_META: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("log_meta");

const MAX_BATCH_OPS: usize = 4096;
const MAX_BATCH_BYTES: usize = 4 << 20;

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

#[derive(Clone)]
pub struct DiskWriter {
    tx: mpsc::Sender<Op>,
    tails: Arc<Mutex<BTreeMap<u32, Tail>>>,
    /// Append sequence numbers, unique across every store on this writer:
    /// two store instances of one group (sharing its tail) can never trim
    /// each other's entries.
    seq: Arc<std::sync::atomic::AtomicU64>,
}

impl DiskWriter {
    pub fn start(db: Arc<Database>) -> io::Result<DiskWriter> {
        let (tx, rx) = mpsc::channel::<Op>();
        let tails: Arc<Mutex<BTreeMap<u32, Tail>>> = Arc::default();
        let t2 = tails.clone();
        std::thread::Builder::new().name("chimaera-raft-disk".into()).spawn(move || run_writer(&db, &rx, &t2))?;
        Ok(DiskWriter { tx, tails, seq: Arc::default() })
    }

    fn tail(&self, group: u32) -> Tail {
        self.tails.lock().expect("tail lock").entry(group).or_default().clone()
    }

    async fn call(&self, make: impl FnOnce(Done) -> Op) -> io::Result<()> {
        let (done, rx) = tokio::sync::oneshot::channel();
        self.tx.send(make(done)).map_err(|_| io_err("raft disk writer stopped"))?;
        rx.await.map_err(|_| io_err("raft disk writer dropped a request"))?
    }
}

fn run_writer(db: &Database, rx: &mpsc::Receiver<Op>, tails: &Mutex<BTreeMap<u32, Tail>>) {
    while let Ok(first) = rx.recv() {
        let mut bytes = first.bytes();
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH_OPS && bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(op) => {
                    bytes += op.bytes();
                    batch.push(op);
                }
                Err(_) => break,
            }
        }
        let result = write_batch(db, &batch).map_err(|e| e.to_string());
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
    }
}

fn write_batch(db: &Database, batch: &[Op]) -> Result<(), redb::Error> {
    let txn = db.begin_write()?;
    {
        let mut log = txn.open_table(LOG)?;
        let mut meta = txn.open_table(LOG_META)?;
        for op in batch {
            match op {
                Op::Append { group, entries, .. } => {
                    for (idx, e) in entries {
                        log.insert((*group, *idx), e.as_slice())?;
                    }
                }
                Op::Meta { group, key, value, .. } => match value {
                    Some(v) => {
                        meta.insert((*group, *key), v.as_slice())?;
                    }
                    None => {
                        meta.remove((*group, *key))?;
                    }
                },
                Op::Truncate { group, from, .. } => {
                    log.retain_in((*group, *from)..=(*group, u64::MAX), |_, _| false)?;
                }
                Op::Purge { group, upto, record, .. } => {
                    log.retain_in((*group, 0)..=(*group, *upto), |_, _| false)?;
                    meta.insert((*group, "purged"), record.as_slice())?;
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}

/// Log storage handle for one group.
#[derive(Clone)]
pub struct LogStore {
    group: u32,
    db: Arc<Database>,
    writer: DiskWriter,
    tail: Tail,
}

impl LogStore {
    pub fn new(group: u32, db: Arc<Database>, writer: DiskWriter) -> LogStore {
        let tail = writer.tail(group);
        LogStore { group, db, writer, tail }
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
        {
            let mut t = self.tail.lock().expect("tail lock");
            for (idx, e) in &encoded {
                t.insert(*idx, (seq, e.clone()));
            }
        }
        self.writer
            .tx
            .send(Op::Append { group: self.group, seq, entries: encoded, cb: callback })
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
