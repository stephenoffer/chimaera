//! How a cluster peer's raft logs reach the disk, and what keeps it safe when
//! they do not.
//!
//! A commit is acknowledged once a majority of a group's voters hold it. With
//! `fsync` each voter has it on disk; without, in its page cache. The page
//! cache survives the peer process dying but not the machine, and a voter
//! that comes back from a crash having forgotten its vote could vote twice in
//! one term. So a peer whose logs were not synced, and whose machine rebooted
//! without a clean shutdown, discards its raft identity and joins again as a
//! new member (the cluster replaces the old one like any dead voter), but only
//! while the rest of the cluster still leads every group it held. When a
//! majority of a group's machines went down together nobody leads it, and
//! discarding would lose the group for good: the peer keeps its state then,
//! and the group comes back with whatever its voters' disks kept (possibly
//! missing its last moments of commits).
//!
//! The choice: `CHIMAERA_RAFT_SYNC` (`fsync`, `full`, `none`), else measured:
//! keep `fsync` where it is cheap (local SSDs), skip it where a sync costs a
//! millisecond or more (network volumes), since every call waits for commits.

use chimaera_raft::DiskDurability;
use std::io;
use std::path::Path;
use std::sync::OnceLock;

/// An fsync faster than this is kept on every commit.
const FAST_FSYNC_US: f64 = 1000.0;
const MARKER: &str = "raft-boot";

static CHOSEN: OnceLock<String> = OnceLock::new();

/// The durability for this peer's raft logs under `raft_dir`, and why.
pub fn choose(raft_dir: &Path) -> io::Result<DiskDurability> {
    let (d, why) = match std::env::var("CHIMAERA_RAFT_SYNC").ok().as_deref() {
        None | Some("") => match fsync_p50_us(raft_dir, 20) {
            Ok(us) if us < FAST_FSYNC_US => (DiskDurability::Os, format!("fsync p50 {us:.0} µs: fast")),
            Ok(us) => (DiskDurability::Process, format!("fsync p50 {us:.0} µs: slow, replicas only")),
            Err(e) => (DiskDurability::Os, format!("fsync probe failed ({e})")),
        },
        Some(v) => (parse(v)?, "CHIMAERA_RAFT_SYNC".to_string()),
    };
    let name = match d {
        DiskDurability::Full => "full",
        DiskDurability::Os => "fsync",
        DiskDurability::Process => "none",
    };
    let _ = CHOSEN.set(format!("raft sync {name} ({why})"));
    tracing::info!("raft sync {name} ({why})");
    Ok(d)
}

/// What `choose` decided, for `chimaera status`.
pub fn describe() -> Option<String> {
    CHOSEN.get().cloned()
}

fn parse(v: &str) -> io::Result<DiskDurability> {
    match v {
        "fsync" => Ok(DiskDurability::Os),
        "full" => Ok(DiskDurability::Full),
        "none" => Ok(DiskDurability::Process),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("CHIMAERA_RAFT_SYNC={other:?}: expected fsync, full or none"),
        )),
    }
}

/// Median latency of a 4 KiB write followed by a plain fsync in `dir`.
pub fn fsync_p50_us(dir: &Path, samples: usize) -> io::Result<f64> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let path = dir.join(".fsync-probe");
    let mut f = std::fs::File::create(&path)?;
    let mut took = Vec::with_capacity(samples);
    for i in 0..samples {
        let t = std::time::Instant::now();
        f.write_all(&[i as u8; 4096])?;
        // Plain fsync, as the cluster backend issues (not macOS F_FULLFSYNC).
        // SAFETY: the descriptor is owned by `f` and open for the call.
        if unsafe { libc::fsync(std::os::fd::AsRawFd::as_raw_fd(&f)) } != 0 {
            return Err(io::Error::last_os_error());
        }
        took.push(t.elapsed().as_secs_f64() * 1e6);
    }
    let _ = std::fs::remove_file(&path);
    took.sort_by(f64::total_cmp);
    Ok(took[took.len() / 2])
}

/// What the previous peer process under this directory left behind.
#[derive(Debug, PartialEq, Eq)]
struct Marker {
    boot: String,
    unsynced: bool,
    clean: bool,
}

impl Marker {
    fn parse(s: &str) -> Option<Marker> {
        let mut it = s.lines();
        let boot = it.next()?.to_string();
        let unsynced = it.next()? == "unsynced";
        let clean = it.next()? == "clean";
        Some(Marker { boot, unsynced, clean })
    }

    fn encode(&self) -> String {
        let synced = if self.unsynced { "unsynced" } else { "synced" };
        let clean = if self.clean { "clean" } else { "running" };
        format!("{}\n{synced}\n{clean}\n", self.boot)
    }
}

/// Whether raft state written by the previous process can be trusted.
fn trusted(prev: Option<&Marker>, boot: Option<&str>) -> bool {
    match prev {
        // A new directory, or state written by a peer that synced.
        None => true,
        Some(m) if !m.unsynced || m.clean => true,
        // Unsynced and not shut down cleanly: only a process restart within
        // the same boot kept the page cache.
        Some(m) => boot.is_some_and(|b| b == m.boot),
    }
}

/// Run before the peer loads its identity (holding the directory lock):
/// discard raft state the machine may have lost, if `others_lead` (given the
/// raft directory) says the rest of the cluster leads every group it held,
/// then record this process.
pub fn guard(dir: &Path, durability: DiskDurability, others_lead: impl FnOnce(&Path) -> bool) -> io::Result<()> {
    let boot = boot_id();
    let prev = std::fs::read_to_string(dir.join(MARKER)).ok().and_then(|s| Marker::parse(&s));
    let raft = dir.join("raft");
    if !trusted(prev.as_ref(), boot.as_deref()) && raft.exists() {
        if others_lead(&raft) {
            tracing::warn!(
                "{}: raft logs were not synced and the machine restarted without a clean shutdown; \
                 joining again as a new member",
                dir.display()
            );
            std::fs::remove_dir_all(&raft)?;
            let _ = std::fs::remove_file(dir.join("peer-id"));
            let _ = std::fs::remove_file(dir.join("hosted-groups"));
        } else {
            tracing::error!(
                "{}: raft logs were not synced and the machine restarted without a clean shutdown, but no \
                 other voter leads this peer's groups: keeping its state so they can recover (their last \
                 commits may be missing)",
                dir.display()
            );
        }
    }
    let now = Marker {
        boot: boot.unwrap_or_else(|| "unknown".into()),
        unsynced: durability == DiskDurability::Process,
        clean: false,
    };
    write_synced(&dir.join(MARKER), now.encode().as_bytes())
}

/// After a clean shutdown: sync the raft files, then record that they are.
pub fn mark_clean(dir: &Path) -> io::Result<()> {
    let Some(mut m) = std::fs::read_to_string(dir.join(MARKER)).ok().and_then(|s| Marker::parse(&s)) else {
        return Ok(());
    };
    if m.unsynced {
        sync_tree(&dir.join("raft"))?;
    }
    m.clean = true;
    write_synced(&dir.join(MARKER), m.encode().as_bytes())
}

fn sync_tree(dir: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            sync_tree(&path)?;
        } else {
            std::fs::File::open(&path)?.sync_all()?;
        }
    }
    std::fs::File::open(dir)?.sync_all()
}

fn write_synced(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        io::Write::write_all(&mut f, data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Identifies this boot of the machine: it changes on every reboot.
fn boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok().map(|s| s.trim().to_string())
    }
    #[cfg(target_os = "macos")]
    {
        let mut tv = libc::timeval { tv_sec: 0, tv_usec: 0 };
        let mut len = std::mem::size_of::<libc::timeval>();
        // SAFETY: kern.boottime fills a timeval of the given length.
        let rc = unsafe {
            libc::sysctlbyname(c"kern.boottime".as_ptr(), (&raw mut tv).cast(), &mut len, std::ptr::null_mut(), 0)
        };
        (rc == 0).then(|| format!("{}.{}", tv.tv_sec, tv.tv_usec))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(boot: &str, unsynced: bool, clean: bool) -> Marker {
        Marker { boot: boot.into(), unsynced, clean }
    }

    #[test]
    fn unsynced_state_is_dropped_only_after_an_unclean_reboot() {
        assert!(trusted(None, Some("b1")), "a new directory");
        assert!(trusted(Some(&marker("b1", false, false)), Some("b2")), "synced logs survive a crash");
        assert!(trusted(Some(&marker("b1", true, true)), Some("b2")), "synced at a clean shutdown");
        assert!(trusted(Some(&marker("b1", true, false)), Some("b1")), "a process restart keeps the page cache");
        assert!(!trusted(Some(&marker("b1", true, false)), Some("b2")), "the machine rebooted mid-run");
        assert!(!trusted(Some(&marker("b1", true, false)), None), "no boot id: assume the worst");
    }

    #[test]
    fn marker_round_trips_and_settings_parse() {
        let m = marker("abc-123", true, false);
        assert_eq!(Marker::parse(&m.encode()), Some(m));
        assert_eq!(parse("none").unwrap(), DiskDurability::Process);
        assert_eq!(parse("fsync").unwrap(), DiskDurability::Os);
        assert!(parse("off").is_err());
    }

    #[test]
    fn guard_discards_raft_state_and_identity_after_an_unclean_reboot() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("raft")).unwrap();
        std::fs::write(d.path().join("raft/log.redb"), b"x").unwrap();
        std::fs::write(d.path().join("peer-id"), b"old").unwrap();
        std::fs::write(d.path().join("hosted-groups"), b"3\n").unwrap();
        // The previous process ran unsynced in another boot and never stopped cleanly.
        std::fs::write(d.path().join(MARKER), marker("another-boot", true, false).encode()).unwrap();
        guard(d.path(), DiskDurability::Process, |_| true).unwrap();
        assert!(!d.path().join("raft").exists() && !d.path().join("peer-id").exists());
        assert!(!d.path().join("hosted-groups").exists(), "the old identity's groups are not rejoined");
        // A clean stop syncs and marks it: the next start keeps its state.
        std::fs::create_dir_all(d.path().join("raft")).unwrap();
        mark_clean(d.path()).unwrap();
        std::fs::write(d.path().join("peer-id"), b"new").unwrap();
        guard(d.path(), DiskDurability::Process, |_| panic!("a clean stop needs no probe")).unwrap();
        assert!(d.path().join("raft").exists() && d.path().join("peer-id").exists());
    }

    #[test]
    fn guard_keeps_raft_state_when_no_other_voter_leads_its_groups() {
        // A whole group rebooted at once: discarding every copy would lose it.
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("raft")).unwrap();
        std::fs::write(d.path().join("peer-id"), b"old").unwrap();
        std::fs::write(d.path().join(MARKER), marker("another-boot", true, false).encode()).unwrap();
        let mut asked = None;
        guard(d.path(), DiskDurability::Process, |raft| {
            asked = Some(raft.to_path_buf());
            false
        })
        .unwrap();
        assert_eq!(asked.as_deref(), Some(d.path().join("raft").as_path()));
        assert!(d.path().join("raft").exists(), "state kept");
        assert_eq!(std::fs::read(d.path().join("peer-id")).unwrap(), b"old", "identity kept");
    }
}
