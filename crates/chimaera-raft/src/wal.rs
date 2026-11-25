//! A write-ahead log under the raft log store.
//!
//! On a network volume a sync is cheap but an IO is not. A redb commit
//! rewrites its B-tree and allocator pages, about twenty write IOs, and a gp3
//! volume's 3,000 IOPS then cap a peer at about 150 synced commits a second
//! (measured: 1.0 operations per commit, p50 7 ms, the volume pinned at its
//! IOPS limit). So each batch is first appended to this file and synced, one
//! sequential write, then committed to redb without a sync. A checkpoint
//! (`CHECKPOINT`, or `MAX_BYTES` written) commits redb durably, which persists
//! every commit before it, and the file starts over.
//!
//! At start, the records the file holds are applied to redb again, in order,
//! in one durable commit. Every operation is absolute (an entry at an index, a
//! key's value, a range of indexes removed), so applying the whole file over
//! whatever redb kept, from before the file's first record up to its last,
//! leaves the state its last record left. A record cut short by a crash was
//! never acknowledged (its sync had not returned): replay stops there.

use crate::log_store::{LOG, LOG_META, io_err};
use redb::{Database, Durability};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

/// The most time between checkpoints while batches are written.
pub(crate) const CHECKPOINT: Duration = Duration::from_secs(1);
/// The most the file grows between checkpoints.
pub(crate) const MAX_BYTES: u64 = 64 << 20;
/// A record's header: payload length (u32 LE) and the payload's blake3 (first 8 bytes).
const HEADER: usize = 12;

/// One operation of a batch, as written to the file and applied to redb.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum WalOp {
    Append { group: u32, entries: Vec<(u64, Vec<u8>)> },
    Meta { group: u32, key: String, value: Option<Vec<u8>> },
    Truncate { group: u32, from: u64 },
    Purge { group: u32, upto: u64, record: Vec<u8> },
}

/// Apply `ops` to the log tables in one transaction.
pub(crate) fn apply(db: &Database, ops: &[WalOp], durability: Durability) -> Result<(), redb::Error> {
    let mut txn = db.begin_write()?;
    txn.set_durability(durability)?;
    {
        let mut log = txn.open_table(LOG)?;
        let mut meta = txn.open_table(LOG_META)?;
        for op in ops {
            match op {
                WalOp::Append { group, entries } => {
                    for (idx, e) in entries {
                        log.insert((*group, *idx), e.as_slice())?;
                    }
                }
                WalOp::Meta { group, key, value } => match value {
                    Some(v) => {
                        meta.insert((*group, key.as_str()), v.as_slice())?;
                    }
                    None => {
                        meta.remove((*group, key.as_str()))?;
                    }
                },
                WalOp::Truncate { group, from } => {
                    log.retain_in((*group, *from)..=(*group, u64::MAX), |_, _| false)?;
                }
                WalOp::Purge { group, upto, record } => {
                    log.retain_in((*group, 0)..=(*group, *upto), |_, _| false)?;
                    meta.insert((*group, "purged"), record.as_slice())?;
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}

/// The file, open for appending batches.
pub(crate) struct Wal {
    file: File,
    /// `F_FULLFSYNC` on macOS (`DiskDurability::Full`), else a plain fsync.
    full: bool,
    written: u64,
    since: Instant,
}

impl Wal {
    /// Apply what the file at `path` holds to `db` (durably), then open it
    /// empty for new batches. Returns the wal and the operations replayed.
    pub(crate) fn recover(db: &Database, path: &Path, full: bool) -> io::Result<(Wal, usize)> {
        let ops = read_records(path)?;
        if !ops.is_empty() {
            apply(db, &ops, Durability::Immediate).map_err(io_err)?;
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let mut wal = Wal { file, full, written: 0, since: Instant::now() };
        // Only after redb holds them durably may the records go.
        wal.reset()?;
        Ok((wal, ops.len()))
    }

    /// Append one batch and sync it: once this returns, the batch survives a crash.
    pub(crate) fn append(&mut self, ops: &[WalOp]) -> io::Result<()> {
        let payload = postcard::to_stdvec(ops).map_err(io_err)?;
        let len = u32::try_from(payload.len()).map_err(|_| io_err("wal record over 4 GiB"))?;
        let mut rec = Vec::with_capacity(HEADER + payload.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(&blake3::hash(&payload).as_bytes()[..8]);
        rec.extend_from_slice(&payload);
        self.file.write_all(&rec)?;
        self.sync()?;
        self.written += rec.len() as u64;
        Ok(())
    }

    /// Bytes written since the last checkpoint.
    pub(crate) fn written(&self) -> u64 {
        self.written
    }

    /// Whether a checkpoint is due.
    pub(crate) fn due(&self) -> bool {
        self.written > 0 && (self.written >= MAX_BYTES || self.since.elapsed() >= CHECKPOINT)
    }

    /// Commit redb durably (persisting every commit made since the last
    /// checkpoint), then start the file over.
    pub(crate) fn checkpoint(&mut self, db: &Database) -> io::Result<()> {
        let mut txn = db.begin_write().map_err(io_err)?;
        txn.set_durability(Durability::Immediate).map_err(io_err)?;
        txn.commit().map_err(io_err)?;
        self.reset()
    }

    fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.sync()?;
        self.written = 0;
        self.since = Instant::now();
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        if self.full {
            return self.file.sync_data(); // F_FULLFSYNC on macOS
        }
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `self.file` and open for the call.
        if unsafe { libc::fsync(self.file.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Every whole, intact record in the file at `path`, in order.
fn read_records(path: &Path) -> io::Result<Vec<WalOp>> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    }
    let mut ops = Vec::new();
    let mut at = 0;
    while at + HEADER <= data.len() {
        let len = u32::from_le_bytes(data[at..at + 4].try_into().expect("4 bytes")) as usize;
        let Some(payload) = data.get(at + HEADER..at + HEADER + len) else { break }; // cut short
        if blake3::hash(payload).as_bytes()[..8] != data[at + 4..at + HEADER] {
            break; // torn: written but never synced
        }
        let batch: Vec<WalOp> = postcard::from_bytes(payload).map_err(io_err)?;
        ops.extend(batch);
        at += HEADER + len;
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{ReadableDatabase, ReadableTable};

    fn open(dir: &Path) -> Database {
        let db = Database::create(dir.join("log.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(LOG).unwrap();
        txn.open_table(LOG_META).unwrap();
        txn.commit().unwrap();
        db
    }

    fn entries(db: &Database, group: u32) -> Vec<(u64, Vec<u8>)> {
        let txn = db.begin_read().unwrap();
        let log = txn.open_table(LOG).unwrap();
        log.range((group, 0)..=(group, u64::MAX))
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value().1, v.value().to_vec())
            })
            .collect()
    }

    fn batch(group: u32, from: u64, n: u64) -> WalOp {
        WalOp::Append { group, entries: (from..from + n).map(|i| (i, format!("e{i}").into_bytes())).collect() }
    }

    #[test]
    fn batches_a_crash_kept_only_in_the_wal_are_replayed_at_start() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 0);
        // As the writer does: each batch synced in the wal, then committed to
        // redb without a sync. A copy of redb's file taken before stands for a
        // machine that lost those unsynced commits.
        drop(db);
        std::fs::copy(dir.path().join("log.redb"), dir.path().join("lost.redb")).unwrap();
        let db = Database::open(dir.path().join("log.redb")).unwrap();
        let ops = vec![
            batch(1, 1, 10),
            WalOp::Truncate { group: 1, from: 6 },
            batch(1, 6, 3),
            WalOp::Meta { group: 1, key: "vote".into(), value: Some(b"v2".to_vec()) },
            WalOp::Purge { group: 1, upto: 2, record: b"p2".to_vec() },
            batch(2, 1, 4),
        ];
        for op in &ops {
            wal.append(std::slice::from_ref(op)).unwrap();
            apply(&db, std::slice::from_ref(op), Durability::None).unwrap();
        }
        let want1 = entries(&db, 1);
        let want2 = entries(&db, 2);
        assert_eq!(want1.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![3, 4, 5, 6, 7, 8]);
        drop(db);
        drop(wal);
        // The machine comes back with redb as it was before those batches.
        std::fs::rename(dir.path().join("lost.redb"), dir.path().join("log.redb")).unwrap();
        let db = Database::open(dir.path().join("log.redb")).unwrap();
        assert!(entries(&db, 1).is_empty(), "the stand-in really lost them");
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, ops.len());
        assert_eq!(entries(&db, 1), want1);
        assert_eq!(entries(&db, 2), want2);
        let txn = db.begin_read().unwrap();
        let meta = txn.open_table(LOG_META).unwrap();
        assert_eq!(meta.get((1, "vote")).unwrap().unwrap().value(), b"v2");
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0, "replayed records go once redb holds them");
    }

    #[test]
    fn replaying_over_a_redb_that_kept_the_batches_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        let ops = vec![batch(1, 1, 5), WalOp::Truncate { group: 1, from: 3 }, batch(1, 3, 4)];
        for op in &ops {
            wal.append(std::slice::from_ref(op)).unwrap();
            apply(&db, std::slice::from_ref(op), Durability::Immediate).unwrap();
        }
        let want = entries(&db, 1);
        // A crash after redb's checkpoint but before the file was emptied.
        drop(wal);
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, ops.len());
        assert_eq!(entries(&db, 1), want);
    }

    #[test]
    fn a_record_cut_short_or_torn_ends_the_replay() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        wal.append(&[batch(1, 1, 3)]).unwrap();
        let whole = std::fs::metadata(&wal_path).unwrap().len();
        wal.append(&[batch(1, 4, 3)]).unwrap();
        drop(wal);
        // The second record half written, as a crash mid-write leaves it.
        let full = std::fs::metadata(&wal_path).unwrap().len();
        OpenOptions::new().write(true).open(&wal_path).unwrap().set_len((whole + full) / 2).unwrap();
        assert_eq!(read_records(&wal_path).unwrap(), vec![batch(1, 1, 3)]);
        // A bit flipped in a whole record: also not replayed.
        let mut data = std::fs::read(&wal_path).unwrap();
        data[HEADER + 2] ^= 1;
        std::fs::write(&wal_path, &data).unwrap();
        assert!(read_records(&wal_path).unwrap().is_empty());
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 0);
    }

    #[test]
    fn a_checkpoint_persists_redb_and_empties_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        wal.append(&[batch(1, 1, 3)]).unwrap();
        apply(&db, &[batch(1, 1, 3)], Durability::None).unwrap();
        assert!(std::fs::metadata(&wal_path).unwrap().len() > 0);
        wal.checkpoint(&db).unwrap();
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
        assert!(!wal.due());
        assert_eq!(entries(&db, 1).len(), 3);
    }
}
