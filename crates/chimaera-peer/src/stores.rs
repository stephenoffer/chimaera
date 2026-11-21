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
        let h = blake3::hash(&self.peer.0);
        u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes")) & (u64::MAX >> 1)
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

/// Large values, one file per object under `objects/<run>/<object>`.
#[derive(Clone)]
pub struct ObjectStore {
    dir: PathBuf,
}

impl ObjectStore {
    pub fn open(dir: PathBuf) -> io::Result<ObjectStore> {
        std::fs::create_dir_all(&dir)?;
        Ok(ObjectStore { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path(&self, run: &[u8], object: &[u8]) -> PathBuf {
        self.dir.join(hex(run)).join(hex(object))
    }

    /// Delete one object's bytes (inline objects have no file).
    pub fn delete(&self, obj: &chimaera_core::pb::ObjectRef) {
        if obj.inline_data.is_empty() && !obj.run.is_empty() {
            let _ = std::fs::remove_file(self.path(&obj.run, &obj.object_id));
        }
    }

    pub fn delete_run(&self, run: &Id) {
        let _ = std::fs::remove_dir_all(self.dir.join(run.hex()));
    }

    /// Bytes currently held, for admission and status.
    pub fn bytes_used(&self) -> u64 {
        fn walk(p: &Path) -> u64 {
            std::fs::read_dir(p)
                .map(|it| {
                    it.flatten()
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
