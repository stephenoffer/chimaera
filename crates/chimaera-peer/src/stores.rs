//! Peer identity, the code store and the object store.
//!
//! Objects are files written by Python (executors and local clients) in a
//! shared layout; Rust only sizes, checks and deletes them. Each run's objects
//! live in their own directory so a finished run is reclaimed in one step.

use chimaera_core::ids::Id;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A peer's stable id plus the incarnation of this process lifetime.
#[derive(Clone, Copy, Debug)]
pub struct Identity {
    pub peer: Id,
    pub incarnation: u64,
}

/// The Raft node id of the peer with id `peer`.
pub fn node_id_of(peer: &[u8]) -> u64 {
    let h = blake3::hash(peer);
    u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes")) & (u64::MAX >> 1)
}

impl Identity {
    /// Load or create the peer id under `dir`, and bump the incarnation. A
    /// restarted peer never reuses an incarnation, so grants and votes from the
    /// previous process lifetime are fenced.
    pub fn load(dir: &Path) -> io::Result<Identity> {
        std::fs::create_dir_all(dir)?;
        let id_path = dir.join("peer-id");
        let peer = match std::fs::read_to_string(&id_path).ok().and_then(|s| Id::parse_hex(s.trim())) {
            Some(id) => id,
            None => {
                let mut seed = [0u8; 32];
                getrandom(&mut seed)?;
                let id = Id::derive(0, 0, &[b"peer", &seed]);
                write_atomic(&id_path, id.hex().as_bytes())?;
                id
            }
        };
        let inc_path = dir.join("incarnation");
        let prev: u64 = std::fs::read_to_string(&inc_path).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
        let incarnation = prev + 1;
        write_atomic(&inc_path, incarnation.to_string().as_bytes())?;
        Ok(Identity { peer, incarnation })
    }

    /// Raft node id. It derives from the peer id, which is stored on the same
    /// disk as the raft log and vote: a restart with that disk keeps its vote
    /// and identity, and a peer that lost its disk gets a new peer id, so it
    /// can never vote again under the old one. Fencing of a restarted
    /// process's grants uses the incarnation, not the node id.
    pub fn node_id(&self) -> u64 {
        node_id_of(&self.peer.0)
    }

    pub fn pb(&self) -> chimaera_core::pb::PeerInc {
        chimaera_core::pb::PeerInc { peer: self.peer.to_vec(), inc: self.incarnation }
    }
}

fn getrandom(buf: &mut [u8]) -> io::Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(buf)
}

/// Write `data` to `path` via a uniquely named temporary file and a rename, so
/// concurrent writers of the same path never interfere and readers never see
/// a partial file.
pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = path.with_extension(format!("tmp{}-{n}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Code blobs (cloudpickled callables) by blake3 digest.
#[derive(Clone)]
pub struct CodeStore {
    dir: PathBuf,
}

impl CodeStore {
    pub fn open(dir: PathBuf) -> io::Result<CodeStore> {
        std::fs::create_dir_all(&dir)?;
        Ok(CodeStore { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn has(&self, digest: &[u8]) -> bool {
        self.path(digest).exists()
    }

    pub fn path(&self, digest: &[u8]) -> PathBuf {
        self.dir.join(hex(digest))
    }

    /// Store `code` if its blake3 digest matches the claimed one.
    pub fn put(&self, digest: &[u8], code: &[u8]) -> io::Result<()> {
        if blake3::hash(code).as_bytes()[..] != *digest {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "code digest does not match its bytes"));
        }
        if self.has(digest) {
            return Ok(());
        }
        write_atomic(&self.path(digest), code)
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Objects copied to disk at once when memory is over its limit
/// (`CHIMAERA_SPILL_THREADS`, default 4; Ray's `max_io_workers`).
fn spill_threads() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("CHIMAERA_SPILL_THREADS").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(4)
    })
}

/// Large values, one file per object under `objects/<run>/<object>`.
#[derive(Clone)]
pub struct ObjectStore {
    dir: PathBuf,
    /// On disk, for objects moved out of a memory-backed store under
    /// pressure; each spilled object goes to the next one in turn.
    spill: Vec<PathBuf>,
    next_spill: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ObjectStore {
    pub fn open(dir: PathBuf) -> io::Result<ObjectStore> {
        std::fs::create_dir_all(&dir)?;
        Ok(ObjectStore { dir, spill: Vec::new(), next_spill: Default::default() })
    }

    /// Spill to `dirs` (on disk) when the store itself is in memory (tmpfs).
    pub fn with_spill(self, dirs: Vec<PathBuf>) -> ObjectStore {
        ObjectStore { spill: dirs, ..self }
    }

    /// The next spill directory, in turn (None: this store does not spill).
    fn next_spill(&self) -> Option<&Path> {
        let n = self.spill.len();
        (n > 0).then(|| self.spill[self.next_spill.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % n].as_path())
    }

    /// Where a copy of `object` on disk goes (a spill or a replica), when
    /// this store spills.
    pub fn spill_path(&self, run: &[u8], object: &[u8]) -> Option<PathBuf> {
        self.next_spill().map(|d| d.join(hex(run)).join(hex(object)))
    }

    /// Move the least recently written objects to disk until `want` bytes are
    /// out of memory; return the bytes moved. Each moved object's path becomes
    /// a symlink to its copy, so every reader keeps working unchanged; a
    /// reader that already mapped it keeps its pages until it is done.
    pub fn spill_oldest(&self, want: u64) -> io::Result<u64> {
        if self.spill.is_empty() {
            return Ok(0);
        }
        let mut files = Vec::new();
        for run in std::fs::read_dir(&self.dir)?.flatten().filter(|e| !is_plane_dir(e)) {
            let Ok(entries) = std::fs::read_dir(run.path()) else { continue };
            for e in entries.flatten() {
                let Ok(m) = e.metadata() else { continue }; // does not follow symlinks
                let name = e.file_name();
                if m.is_file() && !name.to_string_lossy().contains(".tmp") && !name.to_string_lossy().contains(".part")
                {
                    files.push((m.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH), m.len(), e.path()));
                }
            }
        }
        files.sort();
        // The oldest files that cover `want`, moved spill_threads() at a time
        // (one copy stream is well below what the disk takes).
        let mut picked = Vec::new();
        let mut planned = 0;
        for (_, len, path) in files {
            if planned >= want {
                break;
            }
            planned += len;
            picked.push((len, path));
        }
        let next = std::sync::atomic::AtomicUsize::new(0);
        let moved = std::sync::atomic::AtomicU64::new(0);
        let failed: std::sync::Mutex<Option<io::Error>> = std::sync::Mutex::new(None);
        std::thread::scope(|s| {
            for _ in 0..spill_threads().min(picked.len()) {
                s.spawn(|| {
                    while let Some((len, path)) = picked.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)) {
                        let spill = self.next_spill().expect("checked above");
                        match self.spill_one(spill, path) {
                            Ok(()) => {
                                moved.fetch_add(*len, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(e) => {
                                *failed.lock().expect("lock") = Some(e);
                                return;
                            }
                        }
                    }
                });
            }
        });
        match failed.into_inner().expect("lock") {
            Some(e) if moved.load(std::sync::atomic::Ordering::Relaxed) == 0 => Err(e),
            _ => Ok(moved.into_inner()),
        }
    }

    /// Copy one in-memory object to the spill directory and make its path a
    /// symlink to the copy.
    fn spill_one(&self, spill: &Path, path: &Path) -> io::Result<()> {
        let rel = path.strip_prefix(&self.dir).expect("listed under the store");
        let target = spill.join(rel);
        std::fs::create_dir_all(target.parent().expect("run directory"))?;
        std::fs::copy(path, &target)?;
        let link = path.with_extension("spilling");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&target, &link)?;
        std::fs::rename(&link, path) // atomic: readers see the file or the link
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path(&self, run: &[u8], object: &[u8]) -> PathBuf {
        self.dir.join(hex(run)).join(hex(object))
    }

    /// Delete one object's bytes (inline objects have no file), with the
    /// parts of a result of several values (see parts).
    pub fn delete(&self, obj: &chimaera_core::pb::ObjectRef) {
        if obj.inline_data.is_empty() && !obj.run.is_empty() {
            for part in crate::parts::file_ids(&self.path(&obj.run, &obj.object_id)) {
                self.remove(&obj.run, &part);
            }
            self.remove(&obj.run, &obj.object_id);
        }
    }

    fn remove(&self, run: &[u8], object: &[u8]) {
        let _ = std::fs::remove_file(self.path(run, object));
        for spill in &self.spill {
            let _ = std::fs::remove_file(spill.join(hex(run)).join(hex(object)));
        }
    }

    /// Delete an ended run's content-addressed values: large arguments, uploads
    /// and `ch.put` values, which no call of the run can read any more.
    /// Results and stream items stay until the run expires, for clients that
    /// reattach; they are named by ids that start with the run's own cell and
    /// vshard bytes, which a content digest does only by chance (it then stays
    /// until expiry too). A `ch.put` made inside a call is named like a
    /// result (chimaera-py's `_future.put`), so a call that returns its handle
    /// keeps it readable; one made by the driver goes. Returns the bytes freed.
    pub fn delete_values(&self, run: &Id) -> u64 {
        let (dir, run_hex) = (self.dir.join(run.hex()), run.hex());
        let Ok(entries) = std::fs::read_dir(&dir) else { return 0 };
        let mut freed = 0;
        for e in entries.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            let digest = name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit()) && name[..8] != run_hex[..8];
            if !digest {
                continue;
            }
            freed += std::fs::metadata(e.path()).map_or(0, |m| m.len());
            let _ = std::fs::remove_file(e.path());
            for spill in &self.spill {
                let _ = std::fs::remove_file(spill.join(&run_hex).join(name));
            }
        }
        freed
    }

    pub fn delete_run(&self, run: &Id) {
        let _ = std::fs::remove_dir_all(self.dir.join(run.hex()));
        for spill in &self.spill {
            let _ = std::fs::remove_dir_all(spill.join(run.hex()));
        }
    }

    /// Bytes currently held, for admission and status.
    pub fn bytes_used(&self) -> u64 {
        fn walk(p: &Path) -> u64 {
            std::fs::read_dir(p)
                .map(|it| {
                    it.flatten()
                        .filter(|e| !is_plane_dir(e))
                        .map(|e| match e.metadata() {
                            Ok(m) if m.is_dir() => walk(&e.path()),
                            Ok(m) => m.len(),
                            Err(_) => 0,
                        })
                        .sum()
                })
                .unwrap_or(0)
        }
        walk(&self.dir)
    }

    /// Store a blob uploaded by a client, checking its size and content
    /// digest (blake3, first 16 bytes) before it becomes visible.
    pub fn put_blob(&self, run: &[u8], blob_id: &[u8], data: &[u8]) -> io::Result<()> {
        if blake3::hash(data).as_bytes()[..16] != *blob_id {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "blob content does not match its id"));
        }
        let path = self.path(run, blob_id);
        if path.exists() {
            return Ok(());
        }
        std::fs::create_dir_all(path.parent().expect("object paths have a run directory"))?;
        write_atomic(&path, data)
    }
}

/// Bytes of the files directly in `dir` (0 when it does not exist).
pub fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|it| it.flatten().filter_map(|e| e.metadata().ok()).filter(|m| m.is_file()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

/// The object plane's own directories (`.arena`: live NumPy arrays of running
/// processes; `.reserve`: pre-zeroed pages): not objects, never counted,
/// spilled or deleted as such.
fn is_plane_dir(e: &std::fs::DirEntry) -> bool {
    e.file_name().as_encoded_bytes().first() == Some(&b'.')
}

/// One piece of an object being streamed to another process.
pub struct Chunk {
    pub data: Vec<u8>,
    /// Set on the first chunk: bytes in the requested range.
    pub total: u64,
    /// blake3 of `data`, first 16 bytes.
    pub checksum: Vec<u8>,
    /// Set on the first chunk: the whole object's size.
    pub object_size: u64,
}

/// Stream `len` bytes of `path` from `offset` (`len` 0 = to the end) in
/// chunks of at most `chunk_bytes`, each with its checksum.
pub async fn read_chunks(
    path: &Path,
    offset: u64,
    len: u64,
    chunk_bytes: usize,
) -> io::Result<tokio::sync::mpsc::Receiver<io::Result<Chunk>>> {
    let p = path.to_path_buf();
    let (f, size) = tokio::task::spawn_blocking(move || -> io::Result<(std::fs::File, u64)> {
        let f = std::fs::File::open(&p)?;
        let size = f.metadata()?.len();
        Ok((f, size))
    })
    .await
    .map_err(io::Error::other)??;
    if offset > size {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("offset {offset} past the end ({size})")));
    }
    let total = if len == 0 { size - offset } else { len.min(size - offset) };
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    // One blocking thread reads (pread: no async file wrapper and its bounce
    // buffers) and checksums each chunk; the async side only moves it on.
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let (mut left, mut pos, mut first) = (total, offset, true);
        loop {
            let want = left.min(chunk_bytes as u64) as usize;
            let mut data = vec![0u8; want];
            if let Err(e) = f.read_exact_at(&mut data, pos) {
                let _ = tx.blocking_send(Err(e));
                return;
            }
            left -= want as u64;
            pos += want as u64;
            let checksum = blake3::hash(&data).as_bytes()[..16].to_vec();
            let chunk = Chunk {
                data,
                total: if first { total } else { 0 },
                checksum,
                object_size: if first { size } else { 0 },
            };
            first = false;
            if tx.blocking_send(Ok(chunk)).is_err() || left == 0 {
                return;
            }
        }
    });
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ranges_stream_in_checked_chunks() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("obj");
        let data: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let collect = |offset, len| {
            let path = path.clone();
            async move {
                let mut rx = read_chunks(&path, offset, len, 1024).await.unwrap();
                let (mut out, mut total) = (Vec::new(), None);
                while let Some(c) = rx.recv().await {
                    let c = c.unwrap();
                    assert_eq!(c.checksum, blake3::hash(&c.data).as_bytes()[..16].to_vec());
                    total.get_or_insert(c.total);
                    out.extend(c.data);
                }
                (out, total.unwrap())
            }
        };
        assert_eq!(collect(0, 0).await, (data.clone(), 10_000));
        assert_eq!(collect(5000, 3000).await, (data[5000..8000].to_vec(), 3000));
        assert_eq!(collect(9000, 0).await, (data[9000..].to_vec(), 1000));
        assert_eq!(collect(10_000, 0).await, (Vec::new(), 0), "an empty range still answers once");
        assert!(read_chunks(&path, 10_001, 0, 1024).await.is_err());
        let mut rx = read_chunks(&path, 0, 1, 1024).await.unwrap();
        let first = rx.recv().await.unwrap().unwrap();
        assert_eq!((first.data.len(), first.object_size), (1, 10_000), "a one-byte range reports the whole size");
    }

    #[test]
    fn spilled_objects_stay_readable_at_their_path_and_go_with_their_run() {
        let mem = tempfile::tempdir().unwrap();
        let (disk, other) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let dirs = vec![disk.path().to_path_buf(), other.path().to_path_buf()];
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(dirs);
        let run = Id::derive(0, 0, &[b"run"]);
        for (i, size) in [(1u8, 3000usize), (2, 2000), (3, 1000)] {
            let path = store.path(&run.0, &[i]);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![i; size]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20)); // distinct mtimes
        }
        let before = store.bytes_used();
        assert_eq!(store.spill_oldest(4000).unwrap(), 5000, "the two oldest move");
        // What remains is the newest object and two links (a path each).
        assert!(store.bytes_used() < before - 4000, "spilled bytes no longer count as stored");
        assert_eq!(std::fs::read(store.path(&run.0, &[1])).unwrap(), vec![1u8; 3000], "read through the link");
        assert!(std::fs::symlink_metadata(store.path(&run.0, &[3])).unwrap().is_file(), "the newest stays");
        let spilled =
            |i: u8| [&disk, &other].iter().filter(|d| d.path().join(run.hex()).join(hex(&[i])).exists()).count();
        assert_eq!((spilled(1), spilled(2)), (1, 1), "each spilled object is in one spill directory");
        assert!(
            [&disk, &other].iter().all(|d| d.path().join(run.hex()).exists()),
            "the two spilled objects went to different directories"
        );
        let obj =
            |i: u8| chimaera_core::pb::ObjectRef { run: run.0.to_vec(), object_id: vec![i], ..Default::default() };
        store.delete(&obj(1));
        assert_eq!(spilled(1), 0, "deleting removes the spilled copy");
        store.delete_run(&run);
        assert!(!disk.path().join(run.hex()).exists() && !other.path().join(run.hex()).exists());
    }

    #[test]
    fn an_ended_runs_stored_values_go_and_its_results_stay() {
        let d = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(d.path().to_path_buf()).unwrap();
        let run = Id::derive(3, 7, &[b"run"]);
        let result = run.child(&[b"task", b"attempt"]); // named like every result
        let value = blake3::hash(b"a large argument").as_bytes()[..16].to_vec(); // named by content
        for (id, len) in [(result.0.to_vec(), 100usize), (value.clone(), 5000)] {
            let path = store.path(&run.0, &id);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![1u8; len]).unwrap();
        }
        std::fs::write(store.path(&run.0, b"x").with_extension("tmp1"), b"partial").unwrap();
        assert_eq!(store.delete_values(&run), 5000);
        assert!(!store.path(&run.0, &value).exists(), "the stored argument is gone");
        assert!(store.path(&run.0, &result.0).exists(), "the result stays for reattaching clients");
        assert_eq!(store.delete_values(&run), 0);
    }

    #[test]
    fn plane_directories_are_neither_counted_nor_spilled() {
        let (mem, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(vec![disk.path().to_path_buf()]);
        for d in [".arena/123", ".reserve"] {
            std::fs::create_dir_all(mem.path().join(d)).unwrap();
            std::fs::write(mem.path().join(d).join("21-0"), vec![0u8; 3000]).unwrap();
        }
        store.put_blob(b"run", &blake3::hash(b"x").as_bytes()[..16], b"x").unwrap();
        assert_eq!(store.bytes_used(), 1, "only the object counts");
        store.spill_oldest(1 << 20).unwrap();
        assert!(mem.path().join(".reserve/21-0").symlink_metadata().unwrap().is_file(), "reserve files stay in memory");
        assert!(mem.path().join(".arena/123/21-0").symlink_metadata().unwrap().is_file(), "arena files stay in memory");
    }

    #[test]
    fn uploaded_blobs_are_checked_against_their_id() {
        let d = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(d.path().to_path_buf()).unwrap();
        let data = b"some large value".repeat(100);
        let id = blake3::hash(&data).as_bytes()[..16].to_vec();
        assert!(store.put_blob(b"run", &id, b"something else").is_err());
        store.put_blob(b"run", &id, &data).unwrap();
        assert_eq!(std::fs::read(store.path(b"run", &id)).unwrap(), data);
    }

    #[test]
    fn identity_is_stable_and_incarnation_increases() {
        let d = tempfile::tempdir().unwrap();
        let a = Identity::load(d.path()).unwrap();
        let b = Identity::load(d.path()).unwrap();
        assert_eq!(a.peer, b.peer);
        assert_eq!(b.incarnation, a.incarnation + 1);
        assert_eq!(a.node_id(), b.node_id(), "a restart with the same disk keeps its raft identity");
    }

    #[test]
    fn concurrent_writers_of_one_blob_do_not_collide() {
        let d = tempfile::tempdir().unwrap();
        let s = CodeStore::open(d.path().join("code")).unwrap();
        let code = vec![7u8; 1 << 16];
        let digest = blake3::hash(&code);
        std::thread::scope(|sc| {
            for _ in 0..16 {
                sc.spawn(|| s.put(digest.as_bytes(), &code).unwrap());
            }
        });
        assert!(s.has(digest.as_bytes()));
    }

    #[test]
    fn code_store_rejects_a_wrong_digest() {
        let d = tempfile::tempdir().unwrap();
        let s = CodeStore::open(d.path().join("code")).unwrap();
        let code = b"def f(): pass";
        let digest = blake3::hash(code);
        s.put(digest.as_bytes(), code).unwrap();
        assert!(s.has(digest.as_bytes()));
        assert!(s.put(&[0u8; 32], code).is_err());
    }
}
