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
//! The choice: `CHIMAERA_RAFT_SYNC` (`fsync`, `full`, `none`), `fsync` unless
//! told otherwise. A slow disk never lowers it: commits there wait longer and
//! batch more (`chimaera_raft::DiskWriter`), and `chimaera status` says the
//! service is degraded. Keeping commits in the page cache only (`none`) is
//! always an explicit choice.
//!
//! What a commit survives is a property of its group, not of one peer: each
//! peer advertises how it writes its log, and status reports the guarantee
//! asked for next to the one the groups' voters give
//! (`chimaera_core::durability`). Voter replacements and moves take only
//! peers that keep the guarantee asked for.

use crate::router::Router;
use chimaera_core::durability::{self as guarantee, Guarantee};
use chimaera_core::pb;
use chimaera_raft::{DiskDurability, DiskReport, DiskStats};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

const MARKER: &str = "raft-boot";

/// What `choose` decided, and why.
static CHOSEN: OnceLock<(DiskDurability, &str)> = OnceLock::new();
/// The raft log's disk writer, for status.
static DISK: OnceLock<Arc<DiskStats>> = OnceLock::new();

/// The durability for this peer's raft logs: `CHIMAERA_RAFT_SYNC`, else `fsync`.
pub fn choose() -> io::Result<DiskDurability> {
    let (d, why) = decide(std::env::var("CHIMAERA_RAFT_SYNC").ok().as_deref())?;
    let _ = CHOSEN.set((d, why));
    tracing::info!("raft sync {} ({why})", name(d));
    Ok(d)
}

fn decide(setting: Option<&str>) -> io::Result<(DiskDurability, &'static str)> {
    match setting {
        None | Some("") => Ok((DiskDurability::Os, "the default")),
        Some(v) => Ok((parse(v)?, "CHIMAERA_RAFT_SYNC")),
    }
}

fn name(d: DiskDurability) -> &'static str {
    match d {
        DiskDurability::Full => "full",
        DiskDurability::Os => "fsync",
        DiskDurability::Process => "none",
    }
}

/// How a peer writing its raft log this way tells the others.
pub fn advertised(d: DiskDurability) -> pb::RaftSync {
    match d {
        DiskDurability::Full => pb::RaftSync::Full,
        DiskDurability::Os => pb::RaftSync::Fsync,
        DiskDurability::Process => pb::RaftSync::Off,
    }
}

/// Report on the raft log's disk writer in status.
pub fn watch(disk: Arc<DiskStats>) {
    let _ = DISK.set(disk);
}

/// The guarantee this peer asks of the groups it commits to. A local peer
/// never syncs its log, so it asks for (and gives) `process`.
pub fn requested() -> Guarantee {
    CHOSEN.get().map_or(Guarantee::Process, |(d, _)| Guarantee::requested(advertised(*d)))
}

/// Whether `m` may take a voter seat: it syncs its log when this peer asks
/// for more than `process`. Only the sync half of the floor: a peer on a
/// machine that already holds a voter passes, and a group whose voters end up
/// with a majority on one machine reports `reboot` (see `report`).
pub fn meets_floor(m: &pb::Member) -> bool {
    guarantee::meets(requested(), m)
}

/// What `choose` decided and how the disk has kept up, for `chimaera status`.
pub fn describe() -> Option<String> {
    let (d, why) = CHOSEN.get()?;
    let mut line = format!("raft sync {} ({why})", name(*d));
    if let Some(r) = DISK.get().map(|s| s.report()).filter(|r| r.commits > 0) {
        line += &format!(
            ": commits take p50 {} µs, p99 {} µs, {:.1} entries each; the oldest entry waits p99 {} µs",
            r.commit_p50_us,
            r.commit_p99_us,
            r.operations as f64 / r.commits as f64,
            r.wait_p99_us,
        );
        if r.hold_max_us > 0 {
            line += &format!("; batches held open up to {} µs", r.hold_max_us);
        }
    }
    Some(line)
}

/// A slow disk degrades service (commits wait on it) without lowering the
/// guarantee: say so.
fn slow_disk(r: &DiskReport) -> Option<String> {
    let slow = u128::from(r.commit_p50_us) >= chimaera_raft::SLOW_COMMIT.as_micros();
    slow.then(|| {
        let mut line = format!(
            "slow disk: raft commits take p50 {:.1} ms; every one still syncs",
            f64::from(r.commit_p50_us) / 1000.0
        );
        if r.hold_max_us > 0 {
            line += &format!(", and batches are held open up to {} µs so more entries share each", r.hold_max_us);
        }
        line
    })
}

/// The guarantee a group with these voters gives, as this peer knows them,
/// and why it is no stronger.
pub fn group(router: &Router, voters: &[u64]) -> (Guarantee, Option<String>) {
    let Some(mesh) = &router.mesh else { return local() };
    let me = mesh.me.node_id;
    let known = voters.iter().filter_map(|&n| {
        let m = if n == me { Some(mesh.me.clone()) } else { mesh.member(n).map(|p| p.member) };
        m.map(|m| (n, m))
    });
    qualify(&known.collect(), voters)
}

fn local() -> (Guarantee, Option<String>) {
    (Guarantee::Process, Some("a local peer keeps its commits in the page cache".into()))
}

fn qualify(known: &BTreeMap<u64, pb::Member>, voters: &[u64]) -> (Guarantee, Option<String>) {
    let voters: Vec<(u64, Option<&pb::Member>)> = voters.iter().map(|n| (*n, known.get(n))).collect();
    guarantee::qualify(&voters)
}

/// For status: the guarantee asked for, the weakest the cluster's groups
/// give, and why service is below what was asked.
pub fn report(router: &Router) -> (Guarantee, Guarantee, Vec<String>) {
    let requested = requested();
    let (mut effective, mut degraded) = (Guarantee::Machine, Vec::new());
    let mut each = |g: u32, (got, why): (Guarantee, Option<String>)| {
        effective = effective.min(got);
        if got < requested {
            degraded.push(format!("group {g} gives {}: {}", got.name(), why.unwrap_or_default()));
        }
    };
    match &router.mesh {
        None => each(0, local()),
        Some(mesh) => {
            let mut known: BTreeMap<u64, pb::Member> =
                mesh.members().into_iter().map(|p| (p.member.node_id, p.member)).collect();
            known.insert(mesh.me.node_id, mesh.me.clone());
            for g in 0..=*router.data_groups().end() {
                let voters = router.hosted(g).map_or_else(|| mesh.voters_of(g), |h| h.view().1);
                each(g, qualify(&known, &voters));
            }
        }
    }
    if requested > Guarantee::Process {
        degraded.extend(DISK.get().and_then(|s| slow_disk(&s.report())));
    }
    (requested, effective, degraded)
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
    // A peer that syncs its log leaves its state machine's file unsynced as it
    // runs (chimaera_raft): sync it now, so it is trusted at the next start.
    chimaera_raft::mark_state_machine_clean(&dir.join("raft"))?;
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
pub(crate) fn boot_id() -> Option<String> {
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
    fn peers_sync_unless_told_not_to_and_a_slow_disk_degrades_without_lowering_it() {
        // Nothing about the disk goes into the choice: a slow one cannot lower it.
        assert_eq!(decide(None).unwrap().0, DiskDurability::Os);
        assert_eq!(decide(Some("")).unwrap().0, DiskDurability::Os);
        assert_eq!(decide(Some("none")).unwrap().0, DiskDurability::Process);
        assert_eq!(Guarantee::requested(advertised(DiskDurability::Os)), Guarantee::Machine);
        assert_eq!(Guarantee::requested(advertised(DiskDurability::Process)), Guarantee::Process);
        // A 5 ms fsync volume is reported as degraded service; a fast disk is not.
        let slow = DiskReport {
            commits: 100,
            operations: 800,
            commit_p50_us: 5_200,
            hold_max_us: 1_300,
            ..Default::default()
        };
        let line = slow_disk(&slow).expect("a 5 ms commit is slow");
        assert!(line.contains("5.2 ms") && line.contains("still syncs"), "{line}");
        assert_eq!(slow_disk(&DiskReport { commit_p50_us: 150, ..slow }), None);
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
