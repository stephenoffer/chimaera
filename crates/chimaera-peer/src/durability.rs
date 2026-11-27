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
//! The choice: `CHIMAERA_RAFT_SYNC` (`fsync`, `durable`, `full`, `none`),
//! `fsync` unless told otherwise. `durable` syncs every entry before
//! acknowledging it except an attached run's per-call ones
//! (`ShardCmd.sync_optional`, which `sync_class` decides), which it
//! acknowledges from the page cache: those are lost only when a majority of a
//! group's voters restart together in their last moments, and the peer is
//! treated as unsynced by the rule above. A slow disk never lowers the
//! choice: commits there wait longer and batch more
//! (`chimaera_raft::DiskWriter`), and `chimaera status` says the service is
//! degraded. Keeping every commit in the page cache only (`none`) is always
//! an explicit choice.
//!
//! A voter told to drop entries it committed (a majority of its group lost
//! entries they had acknowledged unsynced, together) marks its raft state
//! untrusted and exits ([`on_divergence`]), so it never serves a state the
//! group no longer has and rejoins as a new member.
//!
//! What a commit survives is a property of its group, not of one peer: each
//! peer advertises how it writes its log, and status reports the guarantee
//! asked for next to the one the groups' voters give, for the entries that
//! must survive and for an attached run's per-call ones
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
        DiskDurability::Durable => "durable",
    }
}

/// How a peer writing its raft log this way tells the others.
pub fn advertised(d: DiskDurability) -> pb::RaftSync {
    match d {
        DiskDurability::Full => pb::RaftSync::Full,
        DiskDurability::Os => pb::RaftSync::Fsync,
        DiskDurability::Process => pb::RaftSync::Off,
        // FSYNC holds for every entry that must survive; Member.raft_sync_selective says the rest.
        DiskDurability::Durable => pb::RaftSync::Fsync,
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

/// The guarantee this peer asks for an attached run's per-call entries.
fn requested_calls() -> Guarantee {
    CHOSEN
        .get()
        .map_or(Guarantee::Process, |(d, _)| Guarantee::requested_calls(advertised(*d), *d == DiskDurability::Durable))
}

/// Whether `m` may take a voter seat: it syncs what must survive (`fsync`,
/// `full` or `durable`) when this peer asks for more than `process`. A
/// `durable` peer may join voters that sync every entry; status then says
/// the group gives an attached run's calls less (see `report`). Only the
/// sync half of the floor: a peer on a machine that already holds a voter
/// passes, and a group whose voters end up with a majority on one machine
/// reports `reboot` (see `report`).
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
        if r.unsynced_commits > 0 {
            line += &format!("; {} batches of attached runs' calls written without a sync", r.unsynced_commits);
        }
    }
    Some(line)
}

/// A slow disk degrades service (commits wait on it) without lowering the
/// guarantee: say so.
fn slow_disk(r: &DiskReport, d: DiskDurability) -> Option<String> {
    let slow = u128::from(r.commit_p50_us) >= chimaera_raft::SLOW_COMMIT.as_micros();
    let synced = if d == DiskDurability::Durable { "every one that must survive" } else { "every one" };
    slow.then(|| {
        let mut line = format!(
            "slow disk: raft commits take p50 {:.1} ms; {synced} still syncs",
            f64::from(r.commit_p50_us) / 1000.0
        );
        if r.hold_max_us > 0 {
            line += &format!(", and batches are held open up to {} µs so more entries share each", r.hold_max_us);
        }
        line
    })
}

/// The guarantees a group with these voters gives, as this peer knows them:
/// to the entries that must survive, and to an attached run's per-call ones.
pub fn group(router: &Router, voters: &[u64]) -> (Guarantee, Guarantee) {
    let Some(mesh) = &router.mesh else { return (Guarantee::Process, Guarantee::Process) };
    let me = mesh.me.node_id;
    let known = voters.iter().filter_map(|&n| {
        let m = if n == me { Some(mesh.me.clone()) } else { mesh.member(n).map(|p| p.member) };
        m.map(|m| (n, m))
    });
    let [(must, _), (calls, _)] = qualify(&known.collect(), voters);
    (must, calls)
}

fn local() -> (Guarantee, Option<String>) {
    (Guarantee::Process, Some("a local peer keeps its commits in the page cache".into()))
}

/// The guarantee of a group with these voters for the entries that must
/// survive, then for an attached run's per-call entries.
fn qualify(known: &BTreeMap<u64, pb::Member>, voters: &[u64]) -> [(Guarantee, Option<String>); 2] {
    let voters: Vec<(u64, Option<&pb::Member>)> = voters.iter().map(|n| (*n, known.get(n))).collect();
    [guarantee::qualify(&voters), guarantee::qualify_calls(&voters)]
}

/// What status reports of the cluster's durability.
pub struct Report {
    /// The guarantee asked for the entries that must survive.
    pub requested: Guarantee,
    /// The weakest any group gives them.
    pub effective: Guarantee,
    /// The weakest any group gives an attached run's per-call entries.
    pub calls: Guarantee,
    /// Why service is below what was asked.
    pub degraded: Vec<String>,
}

/// For status: the guarantees asked for and the weakest the cluster's groups
/// give, per class of entry, and why service is below what was asked.
pub fn report(router: &Router) -> Report {
    let (requested, requested_calls) = (requested(), requested_calls());
    let (mut effective, mut calls, mut degraded) = (Guarantee::Machine, Guarantee::Machine, Vec::new());
    let mut each = |g: u32, [(got, why), (got_calls, why_calls)]: [(Guarantee, Option<String>); 2]| {
        effective = effective.min(got);
        calls = calls.min(got_calls);
        if got < requested {
            degraded.push(format!("group {g} gives {}: {}", got.name(), why.unwrap_or_default()));
        } else if got_calls < requested_calls {
            let why = why_calls.unwrap_or_default();
            degraded.push(format!("group {g} gives an attached run's calls {}: {why}", got_calls.name()));
        }
    };
    match &router.mesh {
        None => each(0, [local(), local()]),
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
    if let Some((d, _)) = CHOSEN.get().filter(|_| requested > Guarantee::Process) {
        degraded.extend(DISK.get().and_then(|s| slow_disk(&s.report(), *d)));
    }
    Report { requested, effective, calls, degraded }
}

fn parse(v: &str) -> io::Result<DiskDurability> {
    match v {
        "fsync" => Ok(DiskDurability::Os),
        "full" => Ok(DiskDurability::Full),
        "none" => Ok(DiskDurability::Process),
        "durable" => Ok(DiskDurability::Durable),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("CHIMAERA_RAFT_SYNC={other:?}: expected durable, fsync, full or none"),
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
        // `durable` acknowledges some entries unsynced: the same rule applies.
        unsynced: matches!(durability, DiskDurability::Process | DiskDurability::Durable),
        clean: false,
    };
    write_synced(&dir.join(MARKER), now.encode().as_bytes())
}

/// A boot id no machine has: a marker carrying it is never trusted.
const DIVERGED_BOOT: &str = "diverged";
/// The exit code of a peer that left its group's history (see `on_divergence`).
pub const DIVERGED_EXIT: i32 = 3;

/// Record that the raft state under `dir` holds entries its group no longer
/// has: the next start treats it as lost after an unclean reboot (`guard`).
fn mark_diverged(dir: &Path) -> io::Result<()> {
    let m = Marker { boot: DIVERGED_BOOT.into(), unsynced: true, clean: false };
    write_synced(&dir.join(MARKER), m.encode().as_bytes())
}

/// The hook for `chimaera_raft::set_divergence_hook`, for the peer under
/// `dir`: this voter was told to drop entries it committed, which happens
/// only after a majority of the group lost entries they had acknowledged
/// unsynced, together (a correlated power loss under `durable` or `none`).
/// It marks its raft state untrusted, so the next start discards it and
/// rejoins as a new member while the others lead, and exits.
pub fn on_divergence(dir: std::path::PathBuf) -> impl Fn(u32) + Send + Sync + 'static {
    move |group| {
        tracing::error!(
            "group {group}: this voter was told to drop raft entries it had committed, so a majority of the \
             group's voters lost entries they had acknowledged before syncing them (their machines restarted \
             together); exiting, to rejoin as a new member"
        );
        if let Err(e) = mark_diverged(&dir) {
            tracing::error!("{}: could not mark its raft state untrusted: {e}", dir.display());
        }
        std::process::exit(DIVERGED_EXIT);
    }
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
    fn peers_sync_every_commit_unless_told_not_to_and_a_slow_disk_degrades_without_lowering_it() {
        // Nothing about the disk goes into the choice: a slow one cannot lower it.
        assert_eq!(decide(None).unwrap(), (DiskDurability::Os, "the default"));
        assert_eq!(decide(Some("")).unwrap().0, DiskDurability::Os);
        assert_eq!(decide(Some("durable")).unwrap(), (DiskDurability::Durable, "CHIMAERA_RAFT_SYNC"));
        assert_eq!(decide(Some("fsync")).unwrap(), (DiskDurability::Os, "CHIMAERA_RAFT_SYNC"));
        assert_eq!(decide(Some("full")).unwrap().0, DiskDurability::Full);
        assert_eq!(decide(Some("none")).unwrap().0, DiskDurability::Process);
        assert!(decide(Some("fast")).is_err());
        assert_eq!(Guarantee::requested(advertised(DiskDurability::Os)), Guarantee::Machine);
        assert_eq!(Guarantee::requested(advertised(DiskDurability::Process)), Guarantee::Process);
        // `durable` syncs what must survive; an attached run's calls get less.
        assert_eq!(Guarantee::requested(advertised(DiskDurability::Durable)), Guarantee::Machine);
        assert_eq!(Guarantee::requested_calls(advertised(DiskDurability::Durable), true), Guarantee::ReplicatedMemory);
        // A 5 ms fsync volume is reported as degraded service; a fast disk is not.
        let slow = DiskReport {
            commits: 100,
            operations: 800,
            commit_p50_us: 5_200,
            hold_max_us: 1_300,
            ..Default::default()
        };
        let line = slow_disk(&slow, DiskDurability::Os).expect("a 5 ms commit is slow");
        assert!(line.contains("5.2 ms") && line.contains("every one still syncs"), "{line}");
        let line = slow_disk(&slow, DiskDurability::Durable).expect("a 5 ms commit is slow");
        assert!(line.contains("every one that must survive still syncs"), "{line}");
        assert_eq!(slow_disk(&DiskReport { commit_p50_us: 150, ..slow }, DiskDurability::Durable), None);
    }

    #[test]
    fn marker_round_trips_and_settings_parse() {
        let m = marker("abc-123", true, false);
        assert_eq!(Marker::parse(&m.encode()), Some(m));
        assert_eq!(parse("none").unwrap(), DiskDurability::Process);
        assert_eq!(parse("fsync").unwrap(), DiskDurability::Os);
        assert_eq!(parse("full").unwrap(), DiskDurability::Full);
        assert_eq!(parse("durable").unwrap(), DiskDurability::Durable);
        let e = parse("off").unwrap_err().to_string();
        assert!(e.contains("expected durable, fsync, full or none"), "{e}");
    }

    #[test]
    fn a_durable_peer_takes_a_voter_seat_as_an_fsync_peer_does() {
        // meets_floor is `meets(requested(), m)`, `requested()` from this
        // peer's own choice: whatever that is, a durable member (FSYNC plus
        // raft_sync_selective) is judged as an fsync member is, so a leader
        // too old to read the new field seats it too.
        let member = |d: DiskDurability| pb::Member {
            node_id: 2,
            raft_sync: advertised(d) as i32,
            raft_sync_selective: d == DiskDurability::Durable,
            ..Default::default()
        };
        let all = [DiskDurability::Durable, DiskDurability::Os, DiskDurability::Full, DiskDurability::Process];
        for asks in all {
            let floor = Guarantee::requested(advertised(asks));
            assert_eq!(
                guarantee::meets(floor, &member(DiskDurability::Durable)),
                guarantee::meets(floor, &member(DiskDurability::Os)),
                "{asks:?}"
            );
            assert!(guarantee::meets(floor, &member(DiskDurability::Durable)), "{asks:?}");
        }
        assert!(!guarantee::meets(Guarantee::Machine, &member(DiskDurability::Process)));
        assert_eq!(advertised(DiskDurability::Durable), pb::RaftSync::Fsync, "the field an older peer reads");
    }

    #[test]
    fn status_reports_each_class_and_never_more_than_the_voters_give() {
        let m = |n: u64, d: DiskDurability| pb::Member {
            node_id: n,
            addr: format!("10.0.0.{n}:7700"),
            host: format!("m{n}"),
            raft_sync: advertised(d) as i32,
            raft_sync_selective: d == DiskDurability::Durable,
            ..Default::default()
        };
        let known = |ds: [DiskDurability; 3]| -> BTreeMap<u64, pb::Member> {
            ds.iter().zip(1..).map(|(d, n)| (n, m(n, *d))).collect()
        };
        use DiskDurability::{Durable, Os, Process};
        let classes = |ds| {
            let [(must, _), (calls, _)] = qualify(&known(ds), &[1, 2, 3]);
            (must, calls)
        };
        assert_eq!(classes([Durable; 3]), (Guarantee::Machine, Guarantee::ReplicatedMemory));
        assert_eq!(classes([Os, Os, Durable]), (Guarantee::Machine, Guarantee::ReplicatedMemory));
        assert_eq!(classes([Os; 3]), (Guarantee::Machine, Guarantee::Machine));
        assert_eq!(classes([Durable, Durable, Process]), (Guarantee::Process, Guarantee::Process));
        // A voter this peer does not know counts as one that syncs nothing.
        let [(must, _), (calls, _)] = qualify(&known([Durable; 3]), &[1, 2, 9]);
        assert_eq!((must, calls), (Guarantee::Process, Guarantee::Process));
    }

    #[test]
    fn guard_applies_the_amnesia_rule_to_a_durable_peer() {
        // `durable` acknowledges an attached run's calls before syncing them:
        // after an unclean reboot its state is as suspect as `none`'s.
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("raft")).unwrap();
        std::fs::write(d.path().join("peer-id"), b"old").unwrap();
        guard(d.path(), DiskDurability::Durable, |_| panic!("a new directory needs no probe")).unwrap();
        let m = Marker::parse(&std::fs::read_to_string(d.path().join(MARKER)).unwrap()).unwrap();
        assert!(m.unsynced && !m.clean, "{m:?}");
        if boot_id().is_some() {
            // A process restart within the same boot kept the page cache.
            guard(d.path(), DiskDurability::Durable, |_| panic!("the same boot needs no probe")).unwrap();
            assert!(d.path().join("raft").exists() && d.path().join("peer-id").exists());
        }
        let rebooted = Marker { boot: "another-boot".into(), ..m };
        std::fs::write(d.path().join(MARKER), rebooted.encode()).unwrap();
        guard(d.path(), DiskDurability::Durable, |_| true).unwrap();
        assert!(!d.path().join("raft").exists() && !d.path().join("peer-id").exists(), "rejoins as a new member");
        // An fsync peer's marker is trusted after a crash in another boot.
        guard(d.path(), DiskDurability::Os, |_| panic!("no state to probe")).unwrap();
        let m = Marker::parse(&std::fs::read_to_string(d.path().join(MARKER)).unwrap()).unwrap();
        assert!(trusted(Some(&Marker { boot: "another-boot".into(), ..m }), Some("b2")));
    }

    #[test]
    fn a_voter_that_diverged_rejoins_as_a_new_member_at_its_next_start() {
        // Even a peer that syncs every entry: its log holds entries its group lost.
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("raft")).unwrap();
        std::fs::write(d.path().join("peer-id"), b"old").unwrap();
        guard(d.path(), DiskDurability::Os, |_| panic!("a new directory needs no probe")).unwrap();
        mark_diverged(d.path()).unwrap();
        let m = Marker::parse(&std::fs::read_to_string(d.path().join(MARKER)).unwrap()).unwrap();
        assert!(!trusted(Some(&m), boot_id().as_deref()), "{m:?}");
        guard(d.path(), DiskDurability::Os, |_| true).unwrap();
        assert!(!d.path().join("raft").exists() && !d.path().join("peer-id").exists(), "identity discarded");
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
