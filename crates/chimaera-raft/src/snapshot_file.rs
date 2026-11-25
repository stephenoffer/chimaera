//! The snapshot file: one group's whole state at one log position.
//!
//! `[u32 len][header]`, then `[u32 klen][key][u32 vlen][value]`*. The header
//! is the postcard meta (the position and membership the snapshot was taken
//! at), then `MAGIC`, the format version (u32), the group (u32) and the
//! blake3 hash of every byte of the file but the hash itself. A file cut
//! anywhere, changed, or taken for another group or position is refused
//! before any of it is installed, so a group keeps the state and snapshot it
//! had.
//!
//! Peers from before the format had a version wrote the meta alone as the
//! header, and read a new file as one of their own: postcard decodes the
//! meta and ignores the rest of the header. So a release can still replace
//! peers one at a time (docs/deploy.md, "Upgrading"). Their files are read
//! here with the same record checks but no checksum, until the group's next
//! snapshot replaces them.

use crate::TypeConfig;
use crate::log_store::{enc, io_err};
use openraft::type_config::alias::SnapshotMetaOf;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: [u8; 8] = *b"chm-snap";
const FORMAT: u32 = 1;
/// What follows the meta in a header: `MAGIC`, format, group and hash.
const EXTRA: usize = MAGIC.len() + 4 + 4 + 32;

type Meta = SnapshotMetaOf<TypeConfig>;

fn corrupt(path: &Path, why: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("snapshot {}: {why}", path.display()))
}

/// Writes through to `w` and hashes what it wrote.
struct Hashing<W> {
    w: W,
    hash: blake3::Hasher,
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.w.write(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

fn put(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

/// Write group `group`'s snapshot at `meta` to `path` (through a synced
/// temporary file). Written from an iterator, so a group's state never has
/// to fit in memory. Returns the file's size.
pub(crate) fn write(
    path: &Path,
    group: u32,
    meta: &Meta,
    pairs: impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>>,
) -> io::Result<u64> {
    // A temporary file of its own: a snapshot taken in place of a damaged one
    // can overlap a scheduled one at the same position.
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let tmp = path.with_extension(format!("{}.tmp", WRITES.fetch_add(1, Ordering::Relaxed)));
    let meta = enc(meta)?;
    let mut head = Vec::with_capacity(4 + meta.len() + EXTRA);
    head.extend_from_slice(&((meta.len() + EXTRA) as u32).to_le_bytes());
    head.extend_from_slice(&meta);
    head.extend_from_slice(&MAGIC);
    head.extend_from_slice(&FORMAT.to_le_bytes());
    head.extend_from_slice(&group.to_le_bytes());
    let size = {
        let mut w = Hashing { w: BufWriter::new(File::create(&tmp)?), hash: blake3::Hasher::new() };
        w.write_all(&head)?;
        w.w.write_all(&[0; 32])?; // the hash's place, filled in after the records
        for pair in pairs {
            let (k, v) = pair?;
            put(&mut w, &k)?;
            put(&mut w, &v)?;
        }
        let Hashing { w, hash } = w;
        let mut f = w.into_inner().map_err(io_err)?;
        f.seek(SeekFrom::Start(head.len() as u64))?;
        f.write_all(hash.finalize().as_bytes())?;
        f.sync_all()?;
        f.metadata()?.len()
    };
    std::fs::rename(&tmp, path)?;
    // The name too: a peer whose state machine is not synced rebuilds from
    // this file after a machine restart, and the log before it is purged.
    sync_dir(path)?;
    Ok(size)
}

/// Sync the directory holding `path`, so a rename into it survives a crash.
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) => File::open(dir)?.sync_all(),
        None => Ok(()),
    }
}

/// Reads a snapshot file's records after its header.
pub(crate) struct Reader {
    r: Take<BufReader<File>>,
    pub(crate) meta: Meta,
    path: PathBuf,
    /// Where the hash sits in the file, and the hash (none before versions).
    checksum: Option<(u64, [u8; 32])>,
}

impl Reader {
    /// The header of group `group`'s snapshot at `path`, without checking
    /// the rest.
    fn header(path: &Path, group: u32) -> io::Result<Reader> {
        let f = File::open(path)?;
        let len = f.metadata()?.len();
        let mut r =
            Reader { r: BufReader::new(f).take(len), meta: Meta::default(), path: path.to_path_buf(), checksum: None };
        let head = r.chunk()?.ok_or_else(|| corrupt(path, "empty"))?;
        let (meta, extra) =
            postcard::take_from_bytes::<Meta>(&head).map_err(|e| corrupt(path, format!("unreadable header: {e}")))?;
        r.meta = meta;
        if extra.is_empty() {
            return Ok(r); // written before the format had a version
        }
        if extra.len() < 12 || extra[..MAGIC.len()] != MAGIC {
            return Err(corrupt(path, "unreadable header"));
        }
        let word = |at: usize| u32::from_le_bytes(extra[at..at + 4].try_into().expect("4 bytes"));
        let format = word(8);
        if format != FORMAT {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("snapshot {}: format {format}, this peer reads format {FORMAT}", path.display()),
            ));
        }
        if extra.len() != EXTRA {
            return Err(corrupt(path, "unreadable header"));
        }
        if word(12) != group {
            return Err(corrupt(path, format!("taken for group {}, not {group}", word(12))));
        }
        r.checksum = Some((4 + (head.len() - 32) as u64, extra[16..].try_into().expect("32 bytes")));
        Ok(r)
    }

    /// Open group `group`'s snapshot at `path` for reading, having checked
    /// its format, group and checksum.
    pub(crate) fn open(path: &Path, group: u32) -> io::Result<Reader> {
        let r = Reader::header(path, group)?;
        if let Some((at, want)) = r.checksum {
            let mut f = File::open(path)?;
            let mut hash = blake3::Hasher::new();
            io::copy(&mut (&mut f).take(at), &mut hash)?;
            f.seek(SeekFrom::Current(32))?;
            io::copy(&mut f, &mut hash)?;
            if hash.finalize().as_bytes() != &want {
                return Err(corrupt(path, "checksum mismatch: the file was changed or cut short"));
            }
        }
        Ok(r)
    }

    /// Refuse a snapshot taken at another position or membership than `meta`.
    pub(crate) fn expect(self, meta: &Meta) -> io::Result<Reader> {
        if self.meta != *meta {
            return Err(corrupt(&self.path, format!("taken at {}, not at {meta}", self.meta)));
        }
        Ok(self)
    }

    /// The next key and value; None at the end of the records.
    pub(crate) fn next_pair(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some(k) = self.chunk()? else { return Ok(None) };
        let v = self.chunk()?.ok_or_else(|| corrupt(&self.path, "ends inside a record"))?;
        Ok(Some((k, v)))
    }

    /// Read every record to the end; returns the meta.
    fn rest(mut self) -> io::Result<Meta> {
        while self.next_pair()?.is_some() {}
        Ok(self.meta)
    }

    fn u32(&mut self) -> io::Result<u32> {
        let mut b = [0u8; 4];
        self.r.read_exact(&mut b).map_err(|e| self.cut(e))?;
        Ok(u32::from_le_bytes(b))
    }

    /// The next length-prefixed chunk; None only at the end of the records,
    /// never part way into one.
    fn chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.r.limit() == 0 {
            return Ok(None);
        }
        let len = u64::from(self.u32()?);
        if len > self.r.limit() {
            return Err(corrupt(&self.path, "a record runs past the end"));
        }
        let mut buf = vec![0u8; len as usize];
        self.r.read_exact(&mut buf).map_err(|e| self.cut(e))?;
        Ok(Some(buf))
    }

    fn cut(&self, e: io::Error) -> io::Error {
        if e.kind() == io::ErrorKind::UnexpectedEof { corrupt(&self.path, "ends inside a record") } else { e }
    }
}

/// Check all of group `group`'s snapshot at `path`: format, group, checksum
/// and every record. Returns the position and membership it was taken at.
pub(crate) fn check(path: &Path, group: u32) -> io::Result<Meta> {
    Reader::open(path, group)?.rest()
}

/// `check`, and refuse a snapshot taken at another position or membership
/// than `meta`.
pub(crate) fn verify(path: &Path, group: u32, meta: &Meta) -> io::Result<()> {
    Reader::open(path, group)?.expect(meta)?.rest().map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_store::dec;
    use openraft::testing::log_id;

    fn meta_at(index: u64) -> Meta {
        Meta { last_log_id: Some(log_id::<TypeConfig>(2, 1, index)), last_membership: Default::default() }
    }

    fn pairs(n: u8) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n).map(|i| (vec![b'k', i], vec![i; 3 + i as usize])).collect()
    }

    fn written(dir: &Path) -> (PathBuf, Vec<u8>) {
        let path = dir.join("group-3-40.snap");
        write(&path, 3, &meta_at(40), pairs(4).into_iter().map(Ok)).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        (path, bytes)
    }

    fn read_all(path: &Path, group: u32) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut r = Reader::open(path, group)?;
        let mut out = Vec::new();
        while let Some(p) = r.next_pair()? {
            out.push(p);
        }
        Ok(out)
    }

    #[test]
    fn a_snapshot_reads_back_whole_and_checks_its_position() {
        let d = tempfile::tempdir().unwrap();
        let (path, _) = written(d.path());
        assert_eq!(read_all(&path, 3).unwrap(), pairs(4));
        assert_eq!(check(&path, 3).unwrap(), meta_at(40));
        verify(&path, 3, &meta_at(40)).unwrap();
        // The header the leader sent names another position: refused.
        let e = verify(&path, 3, &meta_at(41)).unwrap_err();
        assert!(e.to_string().contains("not at"), "{e}");
        // Another group's snapshot: refused.
        assert!(Reader::open(&path, 4).err().unwrap().to_string().contains("taken for group 3"));
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 1, "the temporary file is gone");
    }

    /// Cut at every byte (every record boundary, inside each length prefix,
    /// the header and the hash): no prefix of a snapshot passes for a whole
    /// one. Flipping any one byte is caught the same way.
    #[test]
    fn a_cut_or_changed_snapshot_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let (path, bytes) = written(d.path());
        let bad = d.path().join("bad.snap");
        for cut in 0..bytes.len() {
            std::fs::write(&bad, &bytes[..cut]).unwrap();
            assert!(verify(&bad, 3, &meta_at(40)).is_err(), "a file cut at byte {cut} of {} passed", bytes.len());
        }
        for at in 0..bytes.len() {
            let mut b = bytes.clone();
            b[at] ^= 0x10;
            std::fs::write(&bad, &b).unwrap();
            assert!(verify(&bad, 3, &meta_at(40)).is_err(), "a change at byte {at} passed");
        }
        verify(&path, 3, &meta_at(40)).unwrap();
    }

    #[test]
    fn an_unknown_format_is_refused_and_an_unversioned_one_still_reads() {
        let d = tempfile::tempdir().unwrap();
        let (_, mut bytes) = written(d.path());
        let at = bytes.windows(MAGIC.len()).position(|w| w == MAGIC).unwrap() + MAGIC.len();
        bytes[at..at + 4].copy_from_slice(&2u32.to_le_bytes());
        let newer = d.path().join("newer.snap");
        std::fs::write(&newer, &bytes).unwrap();
        assert_eq!(Reader::open(&newer, 3).err().map(|e| e.kind()), Some(io::ErrorKind::Unsupported));
        // The layout before versions: the meta alone as the header, then the records.
        let mut old = Vec::new();
        put(&mut old, &enc(&meta_at(40)).unwrap()).unwrap();
        for (k, v) in pairs(4) {
            put(&mut old, &k).unwrap();
            put(&mut old, &v).unwrap();
        }
        let legacy = d.path().join("legacy.snap");
        std::fs::write(&legacy, &old).unwrap();
        assert_eq!(read_all(&legacy, 3).unwrap(), pairs(4));
        verify(&legacy, 3, &meta_at(40)).unwrap();
        // Without a checksum, a cut inside a record or a length is still caught.
        for cut in [old.len() - 1, old.len() - 5, old.len() - 9] {
            std::fs::write(&legacy, &old[..cut]).unwrap();
            assert!(verify(&legacy, 3, &meta_at(40)).is_err(), "an unversioned file cut at {cut} passed");
        }
    }

    /// A peer from before the format had a version (its reader, as it was)
    /// reads a new file as its own: the same position and the same records.
    #[test]
    fn an_older_peer_reads_a_new_snapshot_as_its_own() {
        fn chunk(r: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
            let mut len = [0u8; 4];
            match r.read_exact(&mut len) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            }
            let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
            r.read_exact(&mut buf)?;
            Ok(Some(buf))
        }
        let d = tempfile::tempdir().unwrap();
        let (_, bytes) = written(d.path());
        let mut r = &bytes[..];
        let meta: Meta = dec(&chunk(&mut r).unwrap().expect("a header")).unwrap();
        assert_eq!(meta, meta_at(40));
        let mut records = Vec::new();
        while let Some(k) = chunk(&mut r).unwrap() {
            records.push((k, chunk(&mut r).unwrap().expect("a value after each key")));
        }
        assert_eq!(records, pairs(4));
    }
}
