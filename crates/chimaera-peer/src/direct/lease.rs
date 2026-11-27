//! The lease a peer's executors hold to begin direct calls
//! (python/chimaera/_direct.py, `Reply.begin`).
//!
//! A direct call reaches an actor's executor without passing its peer, so
//! nothing that fences the peer (its Raft groups, its retired incarnation)
//! stops it. An actor is placed again on another peer only once its peer
//! was declared dead, after missing every probe for the suspicion floor
//! (mesh.rs, `swim_config`: 6 s). So the peer leases its executors the right
//! to begin direct calls, `LEASE_MS` at a time, renewed only while it hears
//! most of the cluster (`Hearing`). A peer that stops (paused, stalled) or is
//! cut off lets the lease run out within `LEASE_WINDOW_MS + LEASE_MS` of its
//! last contact, before anyone can declare it dead.
//!
//! A lease that ran out never comes back for the executors that held it: the
//! actors they serve may have been placed again meanwhile. The next renewal
//! starts a new epoch, and an executor serves direct calls only in the epoch
//! it began in (its callers then go through the peer). A peer that is not in
//! a cluster leases for ever: no other peer can place its actors.
//!
//! The lease is one word in a file next to the peer's direct sockets, which
//! each executor maps: the epoch in its top 16 bits and the deadline below,
//! in milliseconds of `CLOCK` (`clock_ms`). One aligned word, so it is read
//! whole.
//!
//! `CLOCK` is a kernel clock read through clock_gettime, the same for every
//! process on the machine (Python reads it as `time.clock_gettime_ns`,
//! python/chimaera/_direct.py `LEASE_CLOCK`), never `Instant`, whose base is
//! private to its process and which on macOS stops while the machine
//! sleeps. Both clocks used never go back and keep counting while the
//! machine is suspended, as the rest of the cluster's clocks do: Linux's
//! CLOCK_BOOTTIME (its CLOCK_MONOTONIC stops then, so a peer suspended past
//! the suspicion floor would wake to a lease that still looked live), and
//! macOS's CLOCK_MONOTONIC.

use std::collections::VecDeque;
use std::fs::{DirBuilder, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

/// How long a renewal holds, from the start of the probes that earned it.
pub const LEASE_MS: u64 = 3_000;
/// The probes a renewal weighs: the batches begun this long before the latest.
pub const LEASE_WINDOW_MS: u64 = 1_000;
const UNTIL_BITS: u32 = 48;
const EPOCH_MASK: u64 = (1 << (64 - UNTIL_BITS)) - 1;
/// A deadline that never comes.
pub const FOREVER: u64 = (1 << UNTIL_BITS) - 1;

/// The clock a lease's deadline is in (see the module's notes).
#[cfg(target_os = "linux")]
const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
#[cfg(not(target_os = "linux"))]
const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

/// `CLOCK` in milliseconds, the clock executors compare the lease with.
pub fn clock_ms() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: clock_gettime only writes the timespec it is given.
    unsafe { libc::clock_gettime(CLOCK, &mut ts) };
    ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000
}

/// Where the peer whose client socket is `socket` keeps its lease: beside
/// its direct sockets, named after that socket (peers whose data directories
/// are too deep share one socket directory).
pub fn path(socket: &Path) -> PathBuf {
    let name = socket.file_name().unwrap_or_default().to_string_lossy();
    socket.with_file_name("direct").join(format!("{name}.lease"))
}

/// The lease of the peer whose data directory is `dir`, made before its
/// executors start and held until `until_ms`. None if its file cannot be
/// made: its executors then serve no direct calls (python/chimaera/_direct.py,
/// `listen`), and their callers go through the peer.
pub fn start(dir: &Path, until_ms: u64) -> Option<Lease> {
    let socket = chimaera_proto::uds::socket_path(dir, crate::runtime::CLIENT_SOCKET);
    Lease::create(&socket, until_ms)
        .inspect_err(|e| tracing::warn!("actor calls all go through this peer: cannot make the direct-call lease: {e}"))
        .ok()
}

/// A lease's deadline and epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Term {
    until_ms: u64,
    epoch: u64,
}

impl Term {
    /// The word executors read.
    fn word(self) -> u64 {
        (self.epoch << UNTIL_BITS) | self.until_ms
    }

    /// Renewed at `now_ms` until `until_ms`. One that had run out lapsed: it
    /// comes back in a new epoch, which no executor that held it accepts.
    fn renewed(self, now_ms: u64, until_ms: u64) -> Term {
        let until_ms = until_ms.min(FOREVER);
        if now_ms >= self.until_ms {
            Term { until_ms, epoch: (self.epoch + 1) & EPOCH_MASK }
        } else {
            Term { until_ms: until_ms.max(self.until_ms), epoch: self.epoch }
        }
    }
}

/// This peer's lease, mapped from its file.
pub struct Lease {
    word: NonNull<AtomicU64>,
    term: Term,
}

// SAFETY: the mapping is the Lease's own until it drops, and an AtomicU64 may
// be stored from any thread.
unsafe impl Send for Lease {}

impl Lease {
    /// A new lease for the peer whose client socket is `socket`, held until
    /// `until_ms`. One an earlier process of this peer left is replaced, not
    /// written over: its executors, still mapping it, see it run out.
    pub fn create(socket: &Path, until_ms: u64) -> io::Result<Lease> {
        let path = path(socket);
        if let Some(dir) = path.parent() {
            DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
        }
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let file = OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(&path)?;
        file.set_len(8)?;
        // SAFETY: a new shared mapping of the file's first 8 bytes (page
        // aligned, so the word is too), unmapped when the Lease drops.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                8,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let word = NonNull::new(p.cast::<AtomicU64>()).ok_or_else(|| io::Error::other("mmap returned null"))?;
        let lease = Lease { word, term: Term { until_ms: until_ms.min(FOREVER), epoch: 0 } };
        lease.store();
        Ok(lease)
    }

    /// Hold it until `until_ms` (`clock_ms`), in a new epoch if it ran out.
    pub fn renew(&mut self, until_ms: u64) {
        self.term = self.term.renewed(clock_ms(), until_ms);
        self.store();
    }

    fn store(&self) {
        // SAFETY: the mapping lives as long as self.
        unsafe { self.word.as_ref() }.store(self.term.word(), Ordering::Release);
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // SAFETY: the mapping create() made; nothing uses it after this.
        unsafe { libc::munmap(self.word.as_ptr().cast(), 8) };
    }
}

/// Whether this peer hears most of the cluster, from its SWIM probes
/// (cluster.rs, `liveness`).
#[derive(Default)]
pub struct Hearing {
    batches: VecDeque<(u64, usize, usize)>,
}

impl Hearing {
    /// A batch of probes begun at `at_ms` reached `reached` of the `probed`
    /// members. Whether the lease may be renewed: over the batches begun in
    /// the window up to this one, this peer and the members it reached are
    /// more than half of itself and those it probed. A batch that probed
    /// nobody tells nothing, unless this peer knows of no other member
    /// (`alone`): then no other peer can place its actors.
    pub fn batch(&mut self, at_ms: u64, reached: usize, probed: usize, alone: bool) -> bool {
        if probed == 0 {
            return alone;
        }
        self.batches.push_back((at_ms, reached, probed));
        while self.batches.front().is_some_and(|b| b.0 + LEASE_WINDOW_MS < at_ms) {
            self.batches.pop_front();
        }
        let (r, p) = self.batches.iter().fold((0, 0), |(r, p), b| (r + b.1, p + b.2));
        2 * r >= p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_that_ran_out_comes_back_in_a_new_epoch() {
        let t = Term { until_ms: 1_000, epoch: 0 };
        assert_eq!(t.renewed(900, 3_900), Term { until_ms: 3_900, epoch: 0 }, "renewed in time: the same lease");
        assert_eq!(t.renewed(900, 800), t, "a renewal never shortens it");
        assert_eq!(t.renewed(1_000, 4_000), Term { until_ms: 4_000, epoch: 1 }, "it ran out: a new epoch");
        let last = Term { until_ms: 5, epoch: EPOCH_MASK };
        assert_eq!(last.renewed(9, 10).epoch, 0, "the epoch wraps within its 16 bits");
        assert_eq!(Term { until_ms: FOREVER, epoch: 0 }.renewed(u64::MAX >> 16, u64::MAX).until_ms, FOREVER);
        assert_eq!(Term { until_ms: 7, epoch: 3 }.word(), (3 << 48) | 7);
    }

    #[test]
    fn a_lease_runs_out_before_its_peer_can_be_declared_dead() {
        // The last renewal is begun at most a window after the last contact,
        // and holds LEASE_MS from then; a peer is declared dead only after
        // missing every probe for the suspicion floor.
        let floor = crate::mesh::swim_config().suspicion_floor_ms;
        assert!(LEASE_WINDOW_MS + LEASE_MS + 1_000 < floor, "at least a second to spare");
    }

    #[test]
    fn a_peer_renews_only_while_it_hears_most_of_the_cluster() {
        let mut h = Hearing::default();
        assert!(h.batch(0, 2, 2, false), "everyone answers");
        assert!(h.batch(300, 1, 2, false), "one of three peers is gone: two of three remain");
        let mut cut = Hearing::default();
        assert!(!cut.batch(0, 0, 2, false), "nobody answers");
        assert!(!cut.batch(100, 0, 0, false), "no probes this tick tell nothing");
        assert!(!Hearing::default().batch(0, 1, 4, false), "with one of five peers: a minority");
        assert!(Hearing::default().batch(0, 2, 4, false), "with two of five: a majority");
        assert!(Hearing::default().batch(0, 0, 0, true), "a peer on its own");
        // Cut off after 600 ms: earlier answers weigh only through the window.
        let mut h = Hearing::default();
        for at in [0, 300, 600] {
            assert!(h.batch(at, 2, 2, false));
        }
        assert!(h.batch(900, 0, 2, false), "most of the window's probes were answered");
        assert!(h.batch(1_200, 0, 2, false), "half of them");
        assert!(!h.batch(1_500, 0, 2, false), "the answers left the window");
    }

    #[test]
    fn executors_read_the_lease_from_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("peer.sock");
        let read = || u64::from_ne_bytes(std::fs::read(path(&socket)).unwrap().try_into().unwrap());
        let mut lease = Lease::create(&socket, 1_000).unwrap();
        assert_eq!(path(&socket), dir.path().join("direct").join("peer.sock.lease"));
        assert_eq!(read(), 1_000);
        lease.renew(clock_ms() + 60_000);
        assert_eq!(read() >> 48, 1, "the first term had run out");
        let again = Lease::create(&socket, FOREVER).unwrap();
        assert_eq!(read(), FOREVER, "a new process of the peer starts a new lease file");
        drop((lease, again));
        assert_eq!(read(), FOREVER, "the word outlives the mapping");
    }
}
