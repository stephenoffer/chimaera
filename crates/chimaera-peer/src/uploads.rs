//! A value uploaded under the id it already has (PutObject: another cell's
//! ch.put value, sent with a delegated call), stored as it arrives: a few
//! chunks in memory however large it is, checked against its checksum on the
//! way, and published only where its id is free or holds the same bytes.

use crate::stores::ObjectStore;
use std::io::{self, Write};
use std::path::Path;
use tokio::sync::mpsc::Receiver;

/// Store value `id` of `run`, `size` bytes in `chunks` whose blake3 digest
/// starts with `checksum` (16 bytes). An id that already holds these bytes
/// keeps them (the upload is only drained); one that holds other bytes, or
/// that cannot be read here, refuses them at once (`AlreadyExists`,
/// `ResourceBusy`). A short, long or corrupt upload leaves no file behind.
pub fn put_object_from(
    objects: &ObjectStore,
    run: &[u8],
    id: &[u8],
    size: u64,
    checksum: &[u8],
    mut chunks: Receiver<Vec<u8>>,
) -> io::Result<()> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = objects.path(run, id);
    match holds(&path, size, checksum)? {
        Some(true) => {
            while chunks.blocking_recv().is_some() {} // stored already: the upload ends as it would
            return Ok(());
        }
        Some(false) => return Err(taken()),
        None => {}
    }
    std::fs::create_dir_all(path.parent().expect("object paths have a run directory"))?;
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = path.with_extension(format!("tmp{}-{n}", std::process::id()));
    let written = (|| -> io::Result<()> {
        let (mut f, mut hash, mut got) = (std::fs::File::create(&tmp)?, blake3::Hasher::new(), 0u64);
        while let Some(chunk) = chunks.blocking_recv() {
            hash.update(&chunk);
            f.write_all(&chunk)?;
            got += chunk.len() as u64;
        }
        if got != size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("upload ended at {got} of {size} bytes")));
        }
        if hash.finalize().as_bytes()[..16] != *checksum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "value does not match its checksum"));
        }
        f.sync_all()?;
        publish(&tmp, &path, size, checksum)
    })();
    let _ = std::fs::remove_file(&tmp);
    written
}

/// Give `tmp` the name `path` unless `path` exists (a hard link, unlike a
/// rename, never replaces it): two uploads of one id at once publish one
/// value, and the other succeeds only if it carried the same bytes.
fn publish(tmp: &Path, path: &Path, size: u64, checksum: &[u8]) -> io::Result<()> {
    match std::fs::hard_link(tmp, path) {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => match holds(path, size, checksum)? {
            Some(true) => Ok(()),
            _ => Err(taken()),
        },
        done => done,
    }
}

/// Whether `path` holds `size` bytes with this checksum: None if nothing is
/// stored there. Hashed as it is read, not read whole. A name that cannot be
/// read (a spilled value not restored, a broken link) is an error, not a
/// free id: the upload must not replace what it cannot compare.
fn holds(path: &Path, size: u64, checksum: &[u8]) -> io::Result<Option<bool>> {
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(None);
    }
    let unreadable = |e: io::Error| {
        io::Error::new(io::ErrorKind::ResourceBusy, format!("that id is stored but cannot be read here: {e}"))
    };
    let mut f = std::fs::File::open(path).map_err(unreadable)?;
    if f.metadata().map_err(unreadable)?.len() != size {
        return Ok(Some(false));
    }
    let mut hash = blake3::Hasher::new();
    io::copy(&mut f, &mut hash).map_err(unreadable)?;
    Ok(Some(hash.finalize().as_bytes()[..16] == *checksum))
}

/// The reply to an upload `put_object_from` refused.
pub fn refused(e: io::Error) -> tonic::Status {
    match e.kind() {
        io::ErrorKind::InvalidData => tonic::Status::invalid_argument(e.to_string()),
        io::ErrorKind::AlreadyExists => tonic::Status::already_exists(e.to_string()),
        io::ErrorKind::ResourceBusy => tonic::Status::unavailable(e.to_string()),
        _ => tonic::Status::internal(e.to_string()),
    }
}

fn taken() -> io::Error {
    io::Error::new(io::ErrorKind::AlreadyExists, "that id already holds a different value")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(b: &[u8]) -> Vec<u8> {
        blake3::hash(b).as_bytes()[..16].to_vec()
    }

    /// `parts` sent as an upload of `size` bytes with `checksum`.
    fn upload(objects: &ObjectStore, id: &[u8], size: u64, checksum: &[u8], parts: &[&[u8]]) -> io::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(parts.len().max(1));
        for p in parts {
            tx.try_send(p.to_vec()).expect("room for every part");
        }
        drop(tx);
        put_object_from(objects, b"run", id, size, checksum, rx)
    }

    fn name(objects: &ObjectStore, id: &[u8]) -> String {
        objects.path(b"run", id).file_name().unwrap().to_string_lossy().into()
    }

    fn files(dir: &Path) -> Vec<String> {
        let mut out: Vec<String> = walk(dir).iter().map(|p| p.file_name().unwrap().to_string_lossy().into()).collect();
        out.sort();
        out
    }

    fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.flatten().flat_map(|e| if e.path().is_dir() { walk(&e.path()) } else { vec![e.path()] }).collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn a_value_appears_under_its_id_only_once_all_its_bytes_match_its_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let objects = ObjectStore::open(dir.path().to_path_buf()).unwrap();
        let (id, value) = (b"0123456789abcdef", vec![7u8; 3 << 20]);
        let (a, b) = value.split_at(1 << 20);
        upload(&objects, id, value.len() as u64, &digest(&value), &[a, b]).unwrap();
        assert_eq!(std::fs::read(objects.path(b"run", id)).unwrap(), value);
        assert_eq!(files(dir.path()), vec![name(&objects, id)], "no temporary file is left");
    }

    #[test]
    fn a_short_long_or_corrupt_upload_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let objects = ObjectStore::open(dir.path().to_path_buf()).unwrap();
        let value = b"the bytes of a value".to_vec();
        let (n, sum) = (value.len() as u64, digest(&value));
        let short = upload(&objects, b"0000000000000001", n, &sum, &[&value[..5]]).unwrap_err();
        let long = upload(&objects, b"0000000000000002", n - 1, &sum, &[&value]).unwrap_err();
        let corrupt = upload(&objects, b"0000000000000003", n, &digest(b"other"), &[&value]).unwrap_err();
        for e in [short, long, corrupt] {
            assert_eq!(refused(e).code(), tonic::Code::InvalidArgument);
        }
        assert!(files(dir.path()).is_empty(), "{:?}", files(dir.path()));
    }

    #[test]
    fn an_id_keeps_its_bytes_against_the_same_bytes_again_and_refuses_others() {
        let dir = tempfile::tempdir().unwrap();
        let objects = ObjectStore::open(dir.path().to_path_buf()).unwrap();
        let (id, value, other) = (b"0123456789abcdef", b"first value".to_vec(), b"other value".to_vec());
        upload(&objects, id, value.len() as u64, &digest(&value), &[&value]).unwrap();
        let path = objects.path(b"run", id);
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        upload(&objects, id, value.len() as u64, &digest(&value), &[&value]).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), before, "not written again");
        let e = upload(&objects, id, other.len() as u64, &digest(&other), &[&other]).unwrap_err();
        assert_eq!(refused(e).code(), tonic::Code::AlreadyExists, "an id never changes value");
        let longer = b"a longer other value".to_vec();
        let e = upload(&objects, id, longer.len() as u64, &digest(&longer), &[&longer]).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), value);
        assert_eq!(files(dir.path()), vec![name(&objects, id)]);
    }

    #[test]
    fn of_two_uploads_racing_for_one_id_the_first_to_publish_keeps_it() {
        // The second has written and checked its file when the first takes
        // the id: it must not replace the first's bytes, as a rename would.
        let dir = tempfile::tempdir().unwrap();
        let (path, tmp) = (dir.path().join("value"), dir.path().join("value.tmp"));
        std::fs::write(&path, b"first").unwrap();
        std::fs::write(&tmp, b"other").unwrap();
        let e = publish(&tmp, &path, 5, &digest(b"other")).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"first");
        std::fs::write(&tmp, b"first").unwrap();
        publish(&tmp, &path, 5, &digest(b"first")).unwrap();
    }

    #[test]
    fn an_id_that_cannot_be_read_is_not_written_over() {
        // A spilled value whose link points at nothing readable here.
        let dir = tempfile::tempdir().unwrap();
        let objects = ObjectStore::open(dir.path().to_path_buf()).unwrap();
        let (id, value) = (b"0123456789abcdef", b"a value".to_vec());
        let path = objects.path(b"run", id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone"), &path).unwrap();
        let e = upload(&objects, id, value.len() as u64, &digest(&value), &[&value]).unwrap_err();
        assert_eq!(refused(e).code(), tonic::Code::Unavailable);
        assert!(std::fs::symlink_metadata(&path).unwrap().file_type().is_symlink());
    }
}
