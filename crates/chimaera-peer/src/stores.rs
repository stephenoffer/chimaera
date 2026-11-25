//! Peer identity, the code store and the object store.
//!
//! Objects are files written by Python (executors and local clients) in a
//! shared layout; Rust only sizes, checks and deletes them. Each run's objects
//! live in their own directory so a finished run is reclaimed in one step.

use chimaera_core::ids::Id;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A new random peer id (a peer's first start, or a restored peer's).
pub fn new_peer_id() -> io::Result<Id> {
    let mut seed = [0u8; 32];
    getrandom(&mut seed)?;
    Ok(Id::derive(0, 0, &[b"peer", &seed]))
}

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
                let id = new_peer_id()?;
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
    /// pressure; each spilled object goes to the next one in turn (see
    /// spill_order).
    spill: Vec<PathBuf>,
    next_spill: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// When a copy to each spill directory last failed (none: never).
    spill_failed: std::sync::Arc<std::sync::Mutex<Vec<Option<std::time::Instant>>>>,
    /// Object storage that spilled objects go to instead (CHIMAERA_SPILL_URL).
    remote: Option<std::sync::Arc<crate::remote_spill::RemoteSpill>>,
    /// Objects still stored in each pack uploaded by this process, by (run,
    /// pack): the pack is deleted when its last one is freed (after a
    /// restart, with its run).
    packs: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<(String, String), usize>>>,
}

impl ObjectStore {
    pub fn open(dir: PathBuf) -> io::Result<ObjectStore> {
        std::fs::create_dir_all(&dir)?;
        Ok(ObjectStore {
            dir,
            spill: Vec::new(),
            next_spill: Default::default(),
            spill_failed: Default::default(),
            remote: None,
            packs: Default::default(),
        })
    }

    /// Spill to `dirs` (on disk) when the store itself is in memory (tmpfs).
    pub fn with_spill(self, dirs: Vec<PathBuf>) -> ObjectStore {
        let failed = std::sync::Arc::new(std::sync::Mutex::new(vec![None; dirs.len()]));
        ObjectStore { spill: dirs, spill_failed: failed, ..self }
    }

    /// Spill to object storage instead of the spill directories.
    /// Where spilled objects go, for messages: the spill store's URL, or disk.
    pub fn spill_target(&self) -> String {
        self.remote.as_ref().map_or_else(|| "disk".to_string(), |r| r.url.clone())
    }

    pub fn with_remote(self, remote: std::sync::Arc<crate::remote_spill::RemoteSpill>) -> ObjectStore {
        ObjectStore { remote: Some(remote), ..self }
    }

    /// Make the object's file readable here: true when it is, after
    /// downloading it from object storage if it was spilled there (its path
    /// is then a link to a cache file not yet restored). False: not here.
    pub fn ensure_local(&self, run: &[u8], object: &[u8]) -> io::Result<bool> {
        let path = self.path(run, object);
        if path.exists() {
            return Ok(true);
        }
        let (Some(remote), Ok(target)) = (&self.remote, std::fs::read_link(&path)) else { return Ok(false) };
        // Packed with other small objects: its range of the pack (spill_pack).
        if let Ok(entry) = std::fs::read_to_string(target.with_extension("pack")) {
            let f: Vec<&str> = entry.split_whitespace().collect();
            let (Some(pack), Some(offset), Some(len)) =
                (f.first(), f.get(1).and_then(|o| o.parse().ok()), f.get(2).and_then(|l| l.parse().ok()))
            else {
                return Err(io::Error::other(format!("a malformed pack index for {}", hex(object))));
            };
            remote.download_range(&hex(run), pack, offset, len, &target)?;
            return Ok(true);
        }
        remote.download(&hex(run), &hex(object), &target)?;
        Ok(true)
    }

    /// The spill directories to try for `len` bytes, starting from the next
    /// one in turn: first those with that much free and no failed copy in the
    /// last SPILL_RETRY_AFTER, then the others (a reading may be stale, and
    /// a directory recovers). Empty when this store does not spill.
    fn spill_order(&self, len: u64) -> Vec<usize> {
        let n = self.spill.len();
        let start = self.next_spill.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let failed = self.spill_failed.lock().expect("lock").clone();
        let (mut good, mut rest): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
        for i in (0..n).map(|k| (start + k) % n) {
            let healthy = failed[i].is_none_or(|at| at.elapsed() >= SPILL_RETRY_AFTER);
            let room = crate::objplane::free_bytes(&self.spill[i]).is_none_or(|free| free >= len);
            if healthy && room { good.push(i) } else { rest.push(i) }
        }
        good.extend(rest);
        good
    }

    /// Where a copy of `object` on disk goes (a spill or a replica), when
    /// this store spills: the first spill directory of spill_order.
    pub fn spill_path(&self, run: &[u8], object: &[u8], len: u64) -> Option<PathBuf> {
        self.spill_order(len).first().map(|&i| self.spill[i].join(hex(run)).join(hex(object)))
    }

    /// Move the least recently written objects to disk until `want` bytes are
    /// out of memory; return the bytes moved. Each moved object's path becomes
    /// a symlink to its copy, so every reader keeps working unchanged; a
    /// reader that already mapped it keeps its pages until it is done.
    pub fn spill_oldest(&self, want: u64) -> io::Result<u64> {
        if self.spill.is_empty() && self.remote.is_none() {
            return Ok(0);
        }
        let started = std::time::Instant::now();
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
        let moved = std::sync::atomic::AtomicU64::new(0);
        let failed: std::sync::Mutex<Option<io::Error>> = std::sync::Mutex::new(None);
        // To object storage, small objects go several to one upload (Ray's
        // fused spilling): one request each would cost more than their bytes.
        let picked = match &self.remote {
            Some(remote) => {
                let (small, large): (Vec<_>, Vec<_>) = picked.into_iter().partition(|(len, _)| *len < PACK_MAX_OBJECT);
                let mut single = large;
                for batch in pack_batches(small) {
                    if batch.len() < 2 {
                        single.extend(batch);
                        continue;
                    }
                    match self.spill_pack(remote, &batch) {
                        Ok(n) => {
                            moved.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => *failed.lock().expect("lock") = Some(e),
                    }
                }
                single
            }
            None => picked,
        };
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..spill_threads().min(picked.len()) {
                s.spawn(|| {
                    while let Some((len, path)) = picked.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)) {
                        let spilled = match &self.remote {
                            Some(remote) => self.spill_remote(remote, path),
                            None => self.spill_one(path, *len),
                        };
                        match spilled {
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
        crate::counters::SPILLED_BYTES.add(moved.load(std::sync::atomic::Ordering::Relaxed));
        crate::counters::SPILL_MICROSECONDS.add(started.elapsed().as_micros() as u64);
        match failed.into_inner().expect("lock") {
            Some(e) if moved.load(std::sync::atomic::Ordering::Relaxed) == 0 => Err(e),
            _ => Ok(moved.into_inner()),
        }
    }

    /// Upload small in-memory objects of one run as one object storage object
    /// (a pack), then make each one's path a link to its cache file, with an
    /// index beside that file naming its range of the pack: the first reader
    /// restores it with one ranged read (ensure_local). The pack goes with
    /// its run (delete_run), however many of its objects are freed sooner.
    fn spill_pack(&self, remote: &crate::remote_spill::RemoteSpill, members: &[(u64, PathBuf)]) -> io::Result<u64> {
        use std::io::Write;
        let rel = |p: &Path| p.strip_prefix(&self.dir).expect("listed under the store").to_path_buf();
        let run_rel = rel(&members[0].1).parent().map(Path::to_path_buf).unwrap_or_default();
        let Some(run) = run_rel.to_str() else { return Err(io::Error::other("not an object path")) };
        let mut id = [0u8; 8];
        getrandom(&mut id)?;
        let name = format!("pack-{}", hex(&id));
        let tmp = remote.cache.join(run).join(format!(".{name}.tmp"));
        std::fs::create_dir_all(tmp.parent().expect("run directory"))?;
        let mut ranges = Vec::with_capacity(members.len());
        let written: io::Result<()> = (|| {
            let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
            let mut at = 0u64;
            for (_, path) in members {
                let data = std::fs::read(path)?;
                f.write_all(&data)?;
                ranges.push((at, data.len() as u64));
                at += data.len() as u64;
            }
            f.flush()
        })();
        let uploaded = written.and_then(|()| remote.upload(&tmp, run, &name));
        let _ = std::fs::remove_file(&tmp);
        uploaded?;
        let mut total = 0;
        for ((_, path), (offset, len)) in members.iter().zip(ranges) {
            let target = remote.cache.join(rel(path));
            std::fs::write(target.with_extension("pack"), format!("{name} {offset} {len}\n"))?;
            let link = path.with_extension("spilling");
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&target, &link)?;
            std::fs::rename(&link, path)?; // readers see the file or the link
            total += len;
        }
        self.packs.lock().expect("lock").insert((run.to_string(), name), members.len());
        Ok(total)
    }

    /// Upload one in-memory object to object storage and make its path a
    /// link to its cache file, which the first reader restores.
    fn spill_remote(&self, remote: &crate::remote_spill::RemoteSpill, path: &Path) -> io::Result<()> {
        let rel = path.strip_prefix(&self.dir).expect("listed under the store");
        let (Some(run), Some(object)) =
            (rel.parent().and_then(|r| r.to_str()), rel.file_name().and_then(|o| o.to_str()))
        else {
            return Err(io::Error::other("not an object path"));
        };
        remote.upload(path, run, object)?;
        let target = remote.cache.join(rel);
        std::fs::create_dir_all(target.parent().expect("run directory"))?;
        std::fs::write(target.with_extension("remote"), b"")?; // remove() deletes the upload
        let link = path.with_extension("spilling");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&target, &link)?;
        std::fs::rename(&link, path) // readers see the file or the link
    }

    /// Copy one in-memory object of `len` bytes to a spill directory and
    /// make its path a symlink to the copy. The copy's bytes are reserved on
    /// that disk before any is written; a copy that fails (a full or broken
    /// disk) leaves nothing there, marks the directory and goes to the next
    /// one (spill_order). The object stays in memory until a copy is whole.
    fn spill_one(&self, path: &Path, len: u64) -> io::Result<()> {
        let rel = path.strip_prefix(&self.dir).expect("listed under the store");
        let mut last = io::Error::new(io::ErrorKind::StorageFull, "this store has no spill directory");
        for i in self.spill_order(len) {
            let target = self.spill[i].join(rel);
            match copy_reserved(path, &target, len) {
                Ok(()) => {
                    let link = path.with_extension("spilling");
                    let _ = std::fs::remove_file(&link);
                    // The rename is atomic: readers see the file or the link.
                    let linked = std::os::unix::fs::symlink(&target, &link).and_then(|()| std::fs::rename(&link, path));
                    if linked.is_err() {
                        let _ = std::fs::remove_file(&link);
                        let _ = std::fs::remove_file(&target);
                    }
                    return linked;
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&target);
                    self.spill_failed.lock().expect("lock")[i] = Some(std::time::Instant::now());
                    tracing::warn!("cannot spill to {}: {e}; trying another spill directory", self.spill[i].display());
                    last = e;
                }
            }
        }
        Err(last)
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
        let path = self.path(run, object);
        let _ = std::fs::remove_file(&path);
        for spill in &self.spill {
            let _ = std::fs::remove_file(spill.join(hex(run)).join(hex(object)));
        }
        if let Some(remote) = &self.remote {
            let run = hex(run);
            if let Some(gone) = self.forget_remote(remote, &run, &hex(object)) {
                remote.delete(&run, Some(vec![gone]));
            }
        }
    }

    /// Drop this peer's record of an object it spilled to object storage:
    /// its cache file and the sidecar its spill wrote there (`.remote` for
    /// an upload of its own, `.pack` for a range of a pack). The record, not
    /// the object's path, decides: that path may be gone already (a driver
    /// unlinks the put values it owns before the peer hears of their
    /// release). Returns the name to delete from object storage: the
    /// object's upload, or a pack whose last live member this was.
    fn forget_remote(&self, remote: &crate::remote_spill::RemoteSpill, run: &str, object: &str) -> Option<String> {
        let cached = remote.cache.join(run).join(object);
        let _ = std::fs::remove_file(&cached);
        if std::fs::remove_file(cached.with_extension("remote")).is_ok() {
            return Some(object.to_string());
        }
        let index = cached.with_extension("pack");
        let pack = std::fs::read_to_string(&index).ok()?.split_whitespace().next()?.to_string();
        let _ = std::fs::remove_file(&index);
        let key = (run.to_string(), pack);
        let mut packs = self.packs.lock().expect("lock");
        let left = packs.get_mut(&key)?; // unknown after a restart: it goes with its run
        if *left > 1 {
            *left -= 1;
            return None;
        }
        packs.remove(&key);
        Some(key.1)
    }

    /// Delete an ended run's content-addressed values: large arguments, uploads
    /// and `ch.put` values, which no call of the run can read any more.
    /// Results and stream items stay until the run expires, for clients that
    /// reattach; they are named by ids that start with the run's own cell and
    /// vshard bytes, which a content digest does only by chance (it then stays
    /// until expiry too). A `ch.put` made inside a call is named like a
    /// result (chimaera-py's `_future.put`), so a call that returns its handle
    /// keeps it readable; one made by the driver goes. So do the files in
    /// `kept`, which sessions hold (leases). Returns the bytes freed.
    pub fn delete_values(&self, run: &Id, kept: &[Vec<u8>]) -> u64 {
        let (dir, run_hex) = (self.dir.join(run.hex()), run.hex());
        let Ok(entries) = std::fs::read_dir(&dir) else { return 0 };
        let kept: std::collections::HashSet<String> = kept.iter().map(|k| hex(k)).collect();
        let mut freed = 0;
        let mut gone = Vec::new();
        for e in entries.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            let digest = name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit()) && name[..8] != run_hex[..8];
            if !digest || kept.contains(name) {
                continue;
            }
            freed += std::fs::metadata(e.path()).map_or(0, |m| m.len());
            if let Some(remote) = &self.remote {
                gone.extend(self.forget_remote(remote, &run_hex, name));
            }
            let _ = std::fs::remove_file(e.path());
            for spill in &self.spill {
                let _ = std::fs::remove_file(spill.join(&run_hex).join(name));
            }
        }
        if let Some(remote) = self.remote.as_ref().filter(|_| !gone.is_empty()) {
            remote.delete(&run_hex, Some(gone));
        }
        freed
    }

    pub fn delete_run(&self, run: &Id) {
        let _ = std::fs::remove_dir_all(self.dir.join(run.hex()));
        for spill in &self.spill {
            let _ = std::fs::remove_dir_all(spill.join(run.hex()));
        }
        if let Some(remote) = &self.remote {
            let _ = std::fs::remove_dir_all(remote.cache.join(run.hex()));
            remote.delete(&run.hex(), None);
        }
    }

    /// Move the oldest objects to disk until those in memory (with the
    /// object plane's reserve) are at most `keep` bytes; `want` is what a
    /// measurement found past that. Writers go on writing while a round
    /// runs, so each round measures again, tells writers (`publish`), and the
    /// next one moves what is past `keep` by then: waiting for the next scan
    /// instead let writers reach the hard cap, where each new object is
    /// written to disk before its put returns. Returns the bytes moved.
    pub fn spill_down_to(&self, keep: u64, mut want: u64, publish: impl Fn(u64)) -> io::Result<u64> {
        let mut moved = 0;
        while want > 0 {
            let n = match self.spill_oldest(want) {
                Ok(n) => n,
                Err(_) if moved > 0 => break,
                Err(e) => return Err(e),
            };
            moved += n;
            let used = self.memory_bytes();
            publish(used);
            if n == 0 {
                break;
            }
            want = used.saturating_sub(keep);
        }
        Ok(moved)
    }

    /// Bytes in memory: the objects and the object plane's pre-zeroed reserve.
    pub fn memory_bytes(&self) -> u64 {
        self.bytes_used() + dir_bytes(&self.dir.join(".reserve"))
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

/// A spill directory whose copy failed is tried after the others for this long.
const SPILL_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// Copy `src` (`len` bytes) to a new file `dst`, reserving its bytes on
/// dst's filesystem first: a disk without room fails before anything is
/// written, and no other writer can take the room while it is copied.
fn copy_reserved(src: &Path, dst: &Path, len: u64) -> io::Result<()> {
    std::fs::create_dir_all(dst.parent().expect("object paths have a run directory"))?;
    let mut out = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(dst)?;
    preallocate(&out, len)?;
    let copied = std::io::copy(&mut std::fs::File::open(src)?, &mut out)?;
    if copied != len {
        return Err(io::Error::other(format!("copied {copied} of {len} bytes (the object changed)")));
    }
    Ok(())
}

/// Reserve `len` bytes of disk for `f` (posix_fallocate; F_PREALLOCATE on
/// macOS): ENOSPC now rather than partway through writing it.
pub fn preallocate(f: &std::fs::File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: posix_fallocate on a descriptor we own; it returns the error.
        match unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, len as libc::off_t) } {
            0 => Ok(()),
            libc::EOPNOTSUPP => Ok(()), // a filesystem that cannot: the copy reports a full disk itself
            e => Err(io::Error::from_raw_os_error(e)),
        }
    }
    #[cfg(target_os = "macos")]
    {
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        // SAFETY: F_PREALLOCATE reads and fills `store` for a descriptor we own.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (f.as_raw_fd(), len);
        Ok(())
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

/// Objects smaller than this go to object storage packed with others.
const PACK_MAX_OBJECT: u64 = 4 << 20;
/// A pack holds objects up to about this many bytes.
const PACK_TARGET: u64 = 64 << 20;

/// Small objects to spill, in packs: one run's objects to a pack (it goes
/// with its run), up to PACK_TARGET bytes each.
fn pack_batches(small: Vec<(u64, PathBuf)>) -> Vec<Vec<(u64, PathBuf)>> {
    let mut by_run: std::collections::BTreeMap<PathBuf, Vec<(u64, PathBuf)>> = std::collections::BTreeMap::new();
    for (len, path) in small {
        by_run.entry(path.parent().map(Path::to_path_buf).unwrap_or_default()).or_default().push((len, path));
    }
    let mut out = Vec::new();
    for (_, objects) in by_run {
        let (mut batch, mut bytes) = (Vec::new(), 0);
        for (len, path) in objects {
            if bytes + len > PACK_TARGET && !batch.is_empty() {
                out.push(std::mem::take(&mut batch));
                bytes = 0;
            }
            bytes += len;
            batch.push((len, path));
        }
        if !batch.is_empty() {
            out.push(batch);
        }
    }
    out
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
    fn spilling_goes_on_until_memory_is_down_to_what_is_kept() {
        let (mem, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(vec![disk.path().to_path_buf()]);
        let run = Id::derive(0, 0, &[b"run"]);
        for i in 1..=5u8 {
            let path = store.path(&run.0, &[i]);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![i; 1000]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20)); // distinct mtimes
        }
        let seen = std::sync::Mutex::new(Vec::new());
        // Keep 2500 (2000 of objects, and the links left in place of the
        // spilled ones). The measurement that started it is stale: 1000
        // past, where about 2500 are.
        let moved = store.spill_down_to(2500, 1000, |used| seen.lock().unwrap().push(used)).unwrap();
        assert_eq!(moved, 3000, "it measured again and moved what was still past the line");
        assert!(store.memory_bytes() <= 2500, "{}", store.memory_bytes());
        let seen = seen.into_inner().unwrap();
        assert_eq!(seen.len(), 2, "each round's measurement is published: {seen:?}");
        assert_eq!(*seen.last().unwrap(), store.memory_bytes());
        for i in 1..=3u8 {
            assert!(std::fs::symlink_metadata(store.path(&run.0, &[i])).unwrap().is_symlink(), "the oldest moved");
            assert_eq!(std::fs::read(store.path(&run.0, &[i])).unwrap(), vec![i; 1000]);
        }
        assert_eq!(store.spill_down_to(u64::MAX, 0, |_| panic!("nothing past the line")).unwrap(), 0);
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
        let counted = crate::counters::SPILLED_BYTES.get();
        assert_eq!(store.spill_oldest(4000).unwrap(), 5000, "the two oldest move");
        // Process wide: other tests may spill meanwhile.
        assert!(crate::counters::SPILLED_BYTES.get() >= counted + 5000, "/metrics counts what moved");
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
    fn a_spill_a_directory_cannot_take_goes_to_the_next_and_leaves_nothing_there() {
        let mem = tempfile::tempdir().unwrap();
        let (broken, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let dirs = vec![broken.path().to_path_buf(), disk.path().to_path_buf()];
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(dirs);
        let run = Id::derive(0, 0, &[b"run"]);
        // The first directory in turn cannot hold the run's files: a file has
        // its run directory's name (root cannot write there either).
        std::fs::write(broken.path().join(run.hex()), b"").unwrap();
        let path = store.path(&run.0, &[1]);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![7u8; 5000]).unwrap();
        assert_eq!(store.spill_oldest(1).unwrap(), 5000);
        assert_eq!(std::fs::read_link(&path).unwrap(), disk.path().join(run.hex()).join(hex(&[1])));
        assert_eq!(std::fs::read(&path).unwrap(), vec![7u8; 5000], "read through the link");
        assert!(store.spill_failed.lock().unwrap()[0].is_some(), "the failure is remembered");
        assert_eq!(store.spill_order(1), [1, 0], "the failed directory is tried last for a while");
    }

    /// A spill directory that fills up: `CHIMAERA_TEST_SMALL_SPILL_DIR` names
    /// a directory on a filesystem with less than 8 MiB free (Linux: `docker
    /// run --tmpfs /small:size=4m`; macOS: a small disk image, see
    /// docs/objects.md). Skipped without it.
    #[test]
    fn a_spill_to_a_full_disk_fails_before_writing_and_goes_to_the_next() {
        let Some(small) = std::env::var_os("CHIMAERA_TEST_SMALL_SPILL_DIR").map(PathBuf::from) else {
            eprintln!("skipped: set CHIMAERA_TEST_SMALL_SPILL_DIR to a directory on a small filesystem");
            return;
        };
        let (mem, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let small = tempfile::tempdir_in(small).unwrap();
        let size = 8 << 20;
        assert!(crate::objplane::free_bytes(small.path()).unwrap() < size as u64, "the small filesystem has room");
        let run = Id::derive(0, 0, &[b"run"]);
        let only_small = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(vec![small.path().into()]);
        let path = only_small.path(&run.0, &[1]);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![9u8; size]).unwrap();
        let e = only_small.spill_oldest(1).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::StorageFull, "{e}");
        assert!(std::fs::symlink_metadata(&path).unwrap().is_file(), "the object stays where it was");
        assert!(!small.path().join(run.hex()).join(hex(&[1])).exists(), "no partial copy is left");
        let dirs = vec![small.path().to_path_buf(), disk.path().to_path_buf()];
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_spill(dirs);
        assert_eq!(store.spill_oldest(1).unwrap(), size as u64);
        assert_eq!(std::fs::read_link(&path).unwrap(), disk.path().join(run.hex()).join(hex(&[1])));
        assert_eq!(std::fs::read(&path).unwrap(), vec![9u8; size]);
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
        let held = blake3::hash(b"a value another session holds").as_bytes()[..16].to_vec();
        std::fs::write(store.path(&run.0, &held), vec![1u8; 300]).unwrap();
        assert_eq!(store.delete_values(&run, std::slice::from_ref(&held)), 5000);
        assert!(!store.path(&run.0, &value).exists(), "the stored argument is gone");
        assert!(store.path(&run.0, &result.0).exists(), "the result stays for reattaching clients");
        assert!(store.path(&run.0, &held).exists(), "a held value stays");
        assert_eq!(store.delete_values(&run, &[]), 300);
    }

    #[cfg(feature = "object-store-spill")]
    #[test]
    fn objects_spilled_to_object_storage_come_back_on_first_read_and_go_with_their_run() {
        let (mem, bucket, cache) =
            (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let url = format!("file://{}", bucket.path().display());
        let peer = "ab".repeat(16);
        let remote = crate::remote_spill::RemoteSpill::open(&url, &peer, cache.path().to_path_buf()).unwrap();
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_remote(std::sync::Arc::new(remote));
        let run = Id::derive(0, 0, &[b"run"]);
        for (i, size) in [(1u8, 3000usize), (2, 1000)] {
            let path = store.path(&run.0, &[i]);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![i; size]).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20)); // distinct mtimes
        }
        assert_eq!(store.spill_oldest(2000).unwrap(), 3000, "the oldest goes to object storage");
        let remote_dir = bucket.path().join(format!("chimaera-{}", &peer[..16])).join(run.hex());
        assert_eq!(std::fs::read(remote_dir.join("01")).unwrap(), vec![1u8; 3000], "uploaded");
        let path = store.path(&run.0, &[1]);
        assert!(!path.exists(), "its path waits for the first reader to restore it");
        assert!(store.ensure_local(&run.0, &[1]).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), vec![1u8; 3000], "restored through the same path");
        assert!(store.ensure_local(&run.0, &[2]).unwrap(), "an object never spilled is simply here");
        store.delete_run(&run);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while remote_dir.join("01").exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50)); // deleted in the background
        }
        assert!(!remote_dir.join("01").exists(), "the remote copy goes with its run");
    }

    #[cfg(feature = "object-store-spill")]
    #[test]
    fn small_objects_go_to_object_storage_packed_and_come_back_one_by_one() {
        let (mem, bucket, cache) =
            (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let url = format!("file://{}", bucket.path().display());
        let peer = "cd".repeat(16);
        let remote = crate::remote_spill::RemoteSpill::open(&url, &peer, cache.path().to_path_buf()).unwrap();
        let store = ObjectStore::open(mem.path().to_path_buf()).unwrap().with_remote(std::sync::Arc::new(remote));
        let (run_a, run_b) = (Id::derive(0, 0, &[b"a"]), Id::derive(0, 0, &[b"b"]));
        let body = |i: u8, n: usize| (0..n).map(|k| i.wrapping_add(k as u8)).collect::<Vec<u8>>();
        let mut wrote = Vec::new();
        for (run, i, n) in
            [(&run_a, 1u8, 3000), (&run_a, 2, 70_000), (&run_a, 3, 1), (&run_b, 4, 5000), (&run_b, 5, 9000)]
        {
            let path = store.path(&run.0, &[i]);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body(i, n)).unwrap();
            wrote.push((run, i, n));
        }
        let big = store.path(&run_a.0, &[9]);
        std::fs::write(&big, body(9, (PACK_MAX_OBJECT + 10) as usize)).unwrap();
        let all: u64 = wrote.iter().map(|(_, _, n)| *n as u64).sum::<u64>() + PACK_MAX_OBJECT + 10;
        assert_eq!(store.spill_oldest(u64::MAX).unwrap(), all, "everything went to object storage");
        let prefix = bucket.path().join(format!("chimaera-{}", &peer[..16]));
        let listed = |run: &Id| {
            let mut names: Vec<String> = std::fs::read_dir(prefix.join(run.hex()))
                .map(|d| d.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect())
                .unwrap_or_default();
            names.sort();
            names
        };
        let (a, b) = (listed(&run_a), listed(&run_b));
        assert!(a.len() == 2 && a.iter().any(|n| n.starts_with("pack-")) && a.contains(&"09".to_string()), "{a:?}");
        assert!(b.len() == 1 && b[0].starts_with("pack-"), "one pack per run: {b:?}");
        for (run, i, n) in &wrote {
            let path = store.path(&run.0, &[*i]);
            assert!(!path.exists(), "its path waits for the first reader");
            assert!(store.ensure_local(&run.0, &[*i]).unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), body(*i, *n), "object {i} restored from its range of the pack");
        }
        assert!(store.ensure_local(&run_a.0, &[9]).unwrap(), "a large object comes back on its own");
        // The last object freed from a pack takes the pack with it, and a large
        // object its own upload, though their paths were unlinked first (as a
        // driver does with the put values it owns before releasing them).
        for i in [1u8, 2, 3] {
            if i != 3 {
                std::fs::remove_file(store.path(&run_a.0, &[i])).unwrap();
            }
            store.remove(&run_a.0, &[i]);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while listed(&run_a).iter().any(|n| n.starts_with("pack-")) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(listed(&run_a), vec!["09".to_string()], "run a's pack went with its last object");
        std::fs::remove_file(&big).unwrap();
        store.remove(&run_a.0, &[9]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !listed(&run_a).is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(listed(&run_a).is_empty(), "the large object's upload went with it");
        // Freeing one packed object leaves the others readable (the pack stays).
        let first = store.path(&run_b.0, &[4]);
        store.remove(&run_b.0, &[4]);
        assert!(!first.exists());
        let other = store.path(&run_b.0, &[5]);
        std::fs::remove_file(std::fs::read_link(&other).unwrap()).unwrap(); // its cached copy, as if evicted
        assert!(store.ensure_local(&run_b.0, &[5]).unwrap());
        assert_eq!(std::fs::read(&other).unwrap(), body(5, 9000));
        store.delete_run(&run_b);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !listed(&run_b).is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(listed(&run_b).is_empty(), "the pack goes with its run");
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
