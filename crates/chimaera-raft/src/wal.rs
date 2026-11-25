//! A write-ahead log under the raft log store.
//!
//! On a network volume a sync is cheap but an IO is not. A redb commit
//! rewrites its B-tree and allocator pages, about twenty write IOs, and a gp3
//! volume's 3,000 IOPS then cap a peer at about 150 synced commits a second
//! (measured: 1.0 operations per commit, p50 7 ms, the volume pinned at its
//! IOPS limit). So each batch is first written to this file and synced, one
//! write, then committed to redb without a sync. A checkpoint (`CHECKPOINT`,
//! or `MAX_BYTES` written) commits redb durably, which persists every commit
//! before it, and the file starts over.
//!
//! The file is zeroed to its full size once and then only overwritten in
//! place, with `fdatasync`: a sync then carries the batch's own pages and no
//! file metadata. An appended file's size, and an fsync's times, went through
//! the filesystem's journal on every sync, several IOs each (about 5 ms a sync
//! on gp3). Starting over writes a new epoch into the file's header; each
//! record's checksum covers the epoch, so what an earlier epoch left past the
//! newest record never replays. A file from before the header (records from
//! the start, checksummed over the payload alone) is replayed once, at start.
//!
//! `write` leaves a batch unsynced, for callers that need only some batches
//! to survive: the next sync makes it durable along with everything before it.
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
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// The most time between checkpoints while batches are written.
pub(crate) const CHECKPOINT: Duration = Duration::from_secs(1);
/// The most written between checkpoints, and the size the file is zeroed to
/// (a larger batch grows it, which is correct but syncs its size).
pub(crate) const MAX_BYTES: u64 = 16 << 20;
/// The file's header: `MAGIC`, then the epoch (u64 LE).
const MAGIC: &[u8; 8] = b"chwal\0\0\x02";
const FILE_HEADER: u64 = 16;
/// A record's header: payload length (u32 LE) and the first 8 bytes of the
/// blake3 of the epoch (u64 LE) and the payload.
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

/// The file, open for writing batches.
pub(crate) struct Wal {
    file: File,
    /// `F_FULLFSYNC` on macOS (`DiskDurability::Full`), else fdatasync.
    full: bool,
    epoch: u64,
    /// Where the next record goes.
    at: u64,
    written: u64,
    since: Instant,
}

impl Wal {
    /// Apply what the file at `path` holds to `db` (durably), then open it
    /// for new batches in a new epoch. Returns the wal and the operations
    /// replayed.
    pub(crate) fn recover(db: &Database, path: &Path, full: bool) -> io::Result<(Wal, usize)> {
        let (ops, epoch) = read_records(path)?;
        if !ops.is_empty() {
            apply(db, &ops, Durability::Immediate).map_err(io_err)?;
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let mut wal = Wal { file, full, epoch, at: FILE_HEADER, written: 0, since: Instant::now() };
        wal.zero_fill(path)?;
        // Only after redb holds them durably may the records go.
        wal.reset()?;
        Ok((wal, ops.len()))
    }

    /// Zero the file to its full size, once: writes inside it then change no
    /// metadata (a hole or an unwritten extent would, on its first write).
    fn zero_fill(&mut self, path: &Path) -> io::Result<()> {
        let want = FILE_HEADER + MAX_BYTES;
        let mut len = self.file.metadata()?.len();
        if len >= want {
            return Ok(());
        }
        let zeros = vec![0u8; 1 << 20];
        while len < want {
            let n = (want - len).min(zeros.len() as u64);
            self.file.write_all_at(&zeros[..n as usize], len)?;
            len += n;
        }
        self.file.sync_all()?;
        crate::snapshot_file::sync_dir(path) // the file's name, made once
    }

    /// Write one batch and sync it: once this returns, the batch survives a crash.
    pub(crate) fn append(&mut self, ops: &[WalOp]) -> io::Result<()> {
        self.write(ops)?;
        self.sync()
    }

    /// Write one batch without syncing it. A crash may keep it or not (a torn
    /// record ends the replay); the next `sync`, or checkpoint, makes it and
    /// every batch before it durable.
    pub(crate) fn write(&mut self, ops: &[WalOp]) -> io::Result<()> {
        let payload = postcard::to_stdvec(ops).map_err(io_err)?;
        let len = u32::try_from(payload.len()).map_err(|_| io_err("wal record over 4 GiB"))?;
        let mut rec = Vec::with_capacity(HEADER + payload.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(&digest(self.epoch, &payload));
        rec.extend_from_slice(&payload);
        self.file.write_all_at(&rec, self.at)?;
        self.at += rec.len() as u64;
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

    /// A new epoch: the records written so far stop counting once the header
    /// naming it is synced. Until then a crash replays them, over a redb that
    /// already holds them durably, which changes nothing.
    fn reset(&mut self) -> io::Result<()> {
        self.epoch += 1;
        let mut header = [0u8; FILE_HEADER as usize];
        header[..8].copy_from_slice(MAGIC);
        header[8..].copy_from_slice(&self.epoch.to_le_bytes());
        self.file.write_all_at(&header, 0)?;
        self.sync()?;
        self.at = FILE_HEADER;
        self.written = 0;
        self.since = Instant::now();
        Ok(())
    }

    /// Make every batch written so far durable.
    pub(crate) fn sync(&self) -> io::Result<()> {
        if self.full {
            return self.file.sync_data(); // F_FULLFSYNC on macOS
        }
        use std::os::fd::AsRawFd;
        #[cfg(target_os = "linux")]
        // SAFETY: the descriptor is owned by `self.file` and open for the call.
        let r = unsafe { libc::fdatasync(self.file.as_raw_fd()) };
        #[cfg(not(target_os = "linux"))]
        // SAFETY: as above.
        let r = unsafe { libc::fsync(self.file.as_raw_fd()) };
        if r != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

fn digest(epoch: u64, payload: &[u8]) -> [u8; 8] {
    let mut h = blake3::Hasher::new();
    h.update(&epoch.to_le_bytes());
    h.update(payload);
    h.finalize().as_bytes()[..8].try_into().expect("8 bytes")
}

/// Every whole, intact record of the file's epoch at `path`, in order, and
/// that epoch (0 for no file, or one that never got a header).
fn read_records(path: &Path) -> io::Result<(Vec<WalOp>, u64)> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(e),
    };
    let start = FILE_HEADER as usize;
    if data.len() < start || &data[..8] != MAGIC {
        // The earlier format: no header, records from the start, each
        // checksummed over its payload alone. A peer that stopped on it still
        // replays them; the first reset then writes a header over the file.
        // A file zeroed but never started reads as no records here too.
        let ops = records(&data, 0, |p| blake3::hash(p).as_bytes()[..8].try_into().expect("8 bytes"))?;
        return Ok((ops, 0));
    }
    let epoch = u64::from_le_bytes(data[8..start].try_into().expect("8 bytes"));
    Ok((records(&data, start, |p| digest(epoch, p))?, epoch))
}

/// The records from `at` on, up to the first one cut short or failing `check`.
fn records(data: &[u8], mut at: usize, check: impl Fn(&[u8]) -> [u8; 8]) -> io::Result<Vec<WalOp>> {
    let mut ops = Vec::new();
    while at + HEADER <= data.len() {
        let len = u32::from_le_bytes(data[at..at + 4].try_into().expect("4 bytes")) as usize;
        let Some(payload) = data.get(at + HEADER..at + HEADER + len) else { break }; // cut short
        if check(payload) != data[at + 4..at + HEADER] {
            break; // torn (written but never synced), or an earlier epoch's, or zeros
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
        assert!(read_records(&wal_path).unwrap().0.is_empty(), "replayed records go once redb holds them");
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
        let whole = wal.at;
        wal.append(&[batch(1, 4, 3)]).unwrap();
        let full = wal.at;
        drop(wal);
        // The second record half written, as a crash mid-write leaves it.
        OpenOptions::new().write(true).open(&wal_path).unwrap().set_len((whole + full) / 2).unwrap();
        assert_eq!(read_records(&wal_path).unwrap().0, vec![batch(1, 1, 3)]);
        // A bit flipped in a whole record: also not replayed.
        let mut data = std::fs::read(&wal_path).unwrap();
        data[FILE_HEADER as usize + HEADER + 2] ^= 1;
        std::fs::write(&wal_path, &data).unwrap();
        assert!(read_records(&wal_path).unwrap().0.is_empty());
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
        assert_eq!(read_records(&wal_path).unwrap().0, vec![batch(1, 1, 3)]);
        wal.checkpoint(&db).unwrap();
        assert!(read_records(&wal_path).unwrap().0.is_empty());
        assert!(!wal.due());
        assert_eq!(entries(&db, 1).len(), 3);
    }

    #[test]
    fn an_earlier_epochs_records_past_the_newest_never_replay() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        for from in [1, 4, 7] {
            wal.append(&[batch(1, from, 3)]).unwrap();
            apply(&db, &[batch(1, from, 3)], Durability::None).unwrap();
        }
        let old_epoch = wal.epoch;
        wal.checkpoint(&db).unwrap();
        // One record of the same length as the first: the old second record
        // now starts exactly where the newest ends, whole and intact.
        wal.append(&[batch(2, 1, 3)]).unwrap();
        let end = wal.at as usize;
        drop(wal);
        let data = std::fs::read(&wal_path).unwrap();
        let len = u32::from_le_bytes(data[end..end + 4].try_into().unwrap()) as usize;
        let stale = &data[end + HEADER..end + HEADER + len];
        assert_eq!(digest(old_epoch, stale), data[end + 4..end + HEADER], "the old record is still there, intact");
        assert_eq!(read_records(&wal_path).unwrap().0, vec![batch(2, 1, 3)]);
        // Recovered into an empty log: only the newest epoch's batch arrives.
        drop(db);
        let fresh = tempfile::tempdir().unwrap();
        let db = open(fresh.path());
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 1);
        assert!(entries(&db, 1).is_empty());
        assert_eq!(entries(&db, 2).len(), 3);
    }

    #[test]
    fn batches_are_written_in_place_in_a_file_zeroed_once() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        let size = FILE_HEADER + MAX_BYTES;
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), size);
        for round in 0..3 {
            for from in [1, 4] {
                wal.append(&[batch(round + 1, from, 3)]).unwrap();
            }
            wal.checkpoint(&db).unwrap();
            assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), size, "never grown or cut");
        }
        // Reopened: the file is kept as it is, in a later epoch.
        let epoch = wal.epoch;
        drop(wal);
        let (wal, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 0);
        assert!(wal.epoch > epoch);
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), size);
    }

    #[test]
    fn a_file_in_the_earlier_format_is_replayed_once_then_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        // What the earlier format left after a crash: records from offset 0,
        // each checksummed over its payload alone, the last one torn.
        let ops = [batch(1, 1, 3), WalOp::Meta { group: 1, key: "vote".into(), value: Some(b"v1".to_vec()) }];
        let mut data = Vec::new();
        for op in ops.iter().chain([&batch(1, 4, 3)]) {
            let payload = postcard::to_stdvec(std::slice::from_ref(op)).unwrap();
            data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            data.extend_from_slice(&blake3::hash(&payload).as_bytes()[..8]);
            data.extend_from_slice(&payload);
        }
        data.truncate(data.len() - 2);
        std::fs::write(&wal_path, &data).unwrap();
        let db = open(dir.path());
        let (wal, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, ops.len());
        assert_eq!(entries(&db, 1).len(), 3);
        assert_eq!(wal.epoch, 1);
        drop(wal);
        // Now in the new format, and the old records never come back.
        let data = std::fs::read(&wal_path).unwrap();
        assert_eq!(&data[..8], MAGIC);
        assert_eq!(data.len() as u64, FILE_HEADER + MAX_BYTES);
        let fresh = tempfile::tempdir().unwrap();
        let db = open(fresh.path());
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 0);
        assert!(entries(&db, 1).is_empty());
    }

    #[test]
    fn a_batch_written_unsynced_is_one_record_like_any_other() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("log.wal");
        let db = open(dir.path());
        let (mut wal, _) = Wal::recover(&db, &wal_path, false).unwrap();
        wal.write(&[batch(1, 1, 3)]).unwrap();
        wal.write(&[batch(1, 4, 3)]).unwrap();
        wal.sync().unwrap();
        wal.append(&[batch(1, 7, 3)]).unwrap();
        wal.write(&[batch(1, 10, 3)]).unwrap();
        let want: Vec<_> = [1, 4, 7, 10].map(|from| batch(1, from, 3)).into();
        assert_eq!(read_records(&wal_path).unwrap().0, want);
        assert!(wal.written() > 0);
        drop(wal);
        let (_, replayed) = Wal::recover(&db, &wal_path, false).unwrap();
        assert_eq!(replayed, 4);
        assert_eq!(entries(&db, 1).len(), 12);
    }
}
