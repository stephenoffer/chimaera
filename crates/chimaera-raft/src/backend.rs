//! A redb file backend with a weaker sync than redb's default.
//!
//! On macOS, `File::sync_data` issues `F_FULLFSYNC`, which flushes the drive
//! cache and costs milliseconds per commit (measured p50 6.7 ms per sequential
//! proposal). With `fsync: true` this backend issues a plain `fsync` instead
//! (on Linux the two are the same). With `fsync: false` it does not sync at
//! all: every write has reached the kernel's page cache when a commit returns,
//! which survives the peer process dying but not the machine crashing. That
//! is the local peer's guarantee; a cloud volume's fsync (milliseconds per
//! commit, three commits per call) would otherwise set the call latency.

use redb::{BackendError, StorageBackend};
use std::fs::File;
use std::ops::Bound;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::Path;

#[derive(Debug)]
pub struct OsSyncBackend {
    file: File,
    fsync: bool,
}

impl OsSyncBackend {
    pub fn open(path: &Path, fsync: bool) -> std::io::Result<OsSyncBackend> {
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        Ok(OsSyncBackend { file, fsync })
    }
}

/// A plain `fsync` of `file` (not `F_FULLFSYNC` on macOS).
pub(crate) fn fsync(file: &File) -> std::io::Result<()> {
    // SAFETY: the descriptor is owned by `file` and open for the call.
    if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

impl StorageBackend for OsSyncBackend {
    fn len(&self) -> Result<u64, BackendError> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        self.file.read_exact_at(out, offset)?;
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), BackendError> {
        self.file.set_len(len)?;
        Ok(())
    }

    fn sync_data(&self) -> Result<(), BackendError> {
        if !self.fsync {
            return Ok(());
        }
        Ok(fsync(&self.file)?)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
        self.file.write_all_at(data, offset)?;
        Ok(())
    }

    fn close(&self) -> Result<(), BackendError> {
        self.file.unlock()?;
        Ok(())
    }

    /// Whole-file locks only (redb's "level 2"): enough to stop a second peer
    /// process from opening the same database.
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        if !matches!(start, Bound::Unbounded | Bound::Included(0)) || end != Bound::Unbounded {
            return Err(BackendError::Unsupported);
        }
        match self.file.try_lock() {
            Ok(()) => Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => Ok(false),
            Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
        }
    }
}
