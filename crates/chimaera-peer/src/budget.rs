//! Memory and file budgets a peer sets for itself, and the live readings that
//! admission and the pressure response use.
//!
//! One memory pool per peer: executor charges (reservations, idle baselines)
//! and, when objects live in tmpfs, object bytes share it. Ray instead fixes
//! an object-store fraction users must tune by workload (0.3, 0.5 or 0.6 in
//! the guides); here neither side sits idle while the other is short.
//!
//! Memory means private (anonymous) memory: pages a process mapped from a
//! shared object file are charged once, as object bytes, not again per
//! reader (the guides' USS rule).

use std::path::Path;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// How a peer divides its memory, decided at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Memory this peer may use (cgroup-aware).
    pub usable: u64,
    /// Kept back for the agent itself.
    pub reserve: u64,
    /// Pages each of the agent's two raft databases may cache, within the
    /// reserve. redb's default, 1 GiB each, would let the two hold four times
    /// a small machine's whole reserve.
    pub raft_cache: u64,
    /// Executor charges plus (if `objects_in_pool`) object bytes fit here.
    pub pool: u64,
    /// Objects live in tmpfs, so their bytes are memory.
    pub objects_in_pool: bool,
}

impl Budget {
    /// Reserve 10% for the agent, at least 512 MiB and at most 10 GiB, but
    /// never more than half of what there is; a quarter of it, at most 1 GiB,
    /// for each raft database's cache.
    pub fn plan(usable: u64, objects_in_pool: bool) -> Budget {
        let reserve = (usable / 10).clamp(512 * MIB, 10 * GIB).min(usable / 2);
        Budget { usable, reserve, raft_cache: (reserve / 4).min(GIB), pool: usable - reserve, objects_in_pool }
    }

    /// A test or operator override of the pool size.
    pub fn with_pool(self, pool: u64) -> Budget {
        Budget { pool: pool.min(self.usable), ..self }
    }

    pub fn describe(&self) -> String {
        format!(
            "memory pool {} MiB = usable {} MiB - agent reserve {} MiB (raft cache {} MiB per database); objects {}",
            self.pool / MIB,
            self.usable / MIB,
            self.reserve / MIB,
            self.raft_cache / MIB,
            if self.objects_in_pool { "share the pool (tmpfs)" } else { "on disk, outside the pool" }
        )
    }
}

/// A byte size such as `1073741824`, `512 MiB`, `1GiB` or `2.5 GB`.
pub fn parse_size(text: &str) -> Option<u64> {
    let t = text.trim();
    let split = t.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let value: f64 = num.parse().ok()?;
    let scale: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kib" => 1024.0,
        "mib" => MIB as f64,
        "gib" => GIB as f64,
        "tib" => (GIB as f64) * 1024.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        _ => return None,
    };
    Some((value * scale) as u64) // digits and a dot only: never negative
}

/// `RssAnon + VmSwap` from `/proc/<pid>/status`, in bytes.
pub fn parse_status_private(status: &str) -> Option<u64> {
    let mut anon = None;
    let mut swap = 0;
    for line in status.lines() {
        let kb = |l: &str| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok());
        if line.starts_with("RssAnon:") {
            anon = kb(line);
        } else if line.starts_with("VmSwap:") {
            swap = kb(line).unwrap_or(0);
        }
    }
    anon.map(|a| (a + swap) * 1024)
}

/// Freed allocations a worker's NumPy arena keeps for reuse at most (Linux;
/// the default `cache_bytes` of `arena_install` in chimaera-py objects.rs).
const ARENA_CACHE_BYTES: u64 = 256 << 20;

/// Memory a process holds between calls, for the leak check: its private
/// memory plus resident shared memory past what the arena may cache (arena
/// arrays are tmpfs pages, not anonymous ones).
pub fn parse_status_held(status: &str) -> Option<u64> {
    let shmem_kb = status
        .lines()
        .find(|l| l.starts_with("RssShmem:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0);
    parse_status_private(status).map(|p| p + (shmem_kb * 1024).saturating_sub(ARENA_CACHE_BYTES))
}

/// `MemAvailable` from `/proc/meminfo`, in bytes.
pub fn parse_mem_available(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find(|l| l.starts_with("MemAvailable:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

/// One counter from a cgroup `memory.stat`.
pub fn parse_memory_stat(stat: &str, key: &str) -> Option<u64> {
    stat.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == key).then(|| v.trim().parse().ok()).flatten()
    })
}

/// `some avg10` from a PSI file (`/proc/pressure/memory`, `memory.pressure`).
pub fn parse_psi_some_avg10(psi: &str) -> Option<f64> {
    let line = psi.lines().find(|l| l.starts_with("some "))?;
    line.split_whitespace().find_map(|f| f.strip_prefix("avg10=")).and_then(|v| v.parse().ok())
}

/// Available memory under a cgroup limit: the limit minus what is in use,
/// not counting reclaimable page cache. `None` when unlimited.
pub fn cgroup_available(max: &str, current: &str, stat: &str) -> Option<u64> {
    let limit: u64 = max.trim().parse().ok()?;
    let current: u64 = current.trim().parse().ok()?;
    let inactive = parse_memory_stat(stat, "inactive_file").unwrap_or(0);
    Some(limit.saturating_sub(current.saturating_sub(inactive)))
}

fn read(path: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Memory that can be allocated now without pushing out other work.
#[cfg(target_os = "linux")]
pub fn available_bytes() -> Option<u64> {
    let host = read("/proc/meminfo").and_then(|m| parse_mem_available(&m));
    // The tightest of this cgroup and every limited one above it.
    let limited = crate::topology::own_cgroup_chain().into_iter().filter_map(|dir| {
        let (max, cur, stat) =
            (read(dir.join("memory.max"))?, read(dir.join("memory.current"))?, read(dir.join("memory.stat"))?);
        cgroup_available(&max, &cur, &stat)
    });
    limited.chain(host).min()
}

/// Free, inactive, speculative and purgeable pages: what macOS can hand out
/// without compressing or swapping active memory.
#[cfg(target_os = "macos")]
pub fn available_bytes() -> Option<u64> {
    unsafe extern "C" {
        // <mach/mach_init.h>; declared here because libc marks it deprecated.
        fn mach_host_self() -> libc::mach_port_t;
    }
    // SAFETY: vm_statistics64 is plain data; host_statistics64 writes at most
    // `count` integers into it and updates `count`.
    let vm = unsafe {
        let mut vm: libc::vm_statistics64 = std::mem::zeroed();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        let rc = libc::host_statistics64(
            mach_host_self(),
            libc::HOST_VM_INFO64,
            (&mut vm as *mut libc::vm_statistics64).cast(),
            &mut count,
        );
        if rc != libc::KERN_SUCCESS {
            return None;
        }
        vm
    };
    // SAFETY: sysconf has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(4096) as u64;
    let pages =
        vm.free_count as u64 + vm.inactive_count as u64 + vm.speculative_count as u64 + vm.purgeable_count as u64;
    Some(pages * page)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn available_bytes() -> Option<u64> {
    None
}

/// Linux memory pressure (`some avg10`, percent of time stalled), from this
/// peer's cgroup if it has one, else the host.
pub fn memory_pressure() -> Option<f64> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    crate::topology::own_cgroup_dir()
        .and_then(|d| read(d.join("memory.pressure")))
        .or_else(|| read("/proc/pressure/memory"))
        .and_then(|p| parse_psi_some_avg10(&p))
}

/// A process's private memory: anonymous resident plus swapped on Linux, the
/// physical footprint on macOS (which leaves out clean file mappings).
pub fn private_bytes(pid: u32) -> Option<u64> {
    if cfg!(target_os = "linux") {
        read(format!("/proc/{pid}/status")).and_then(|s| parse_status_private(&s))
    } else {
        crate::accounting::read(pid).map(|r| r.rss_bytes)
    }
}

/// A process's private memory (see [`private_bytes`]) and what it holds
/// between calls (see [`parse_status_held`]; None: the private memory, where
/// there is no arena), from one read of its status: both are taken after
/// every call.
pub fn private_and_held(pid: u32) -> Option<(u64, Option<u64>)> {
    if cfg!(target_os = "linux") {
        let s = read(format!("/proc/{pid}/status"))?;
        Some((parse_status_private(&s)?, parse_status_held(&s)))
    } else {
        private_bytes(pid).map(|p| (p, None))
    }
}

/// Free space on the filesystem holding objects below which new calls wait
/// (writing an object there would fail part-way): 512 MiB, or a tenth of a
/// smaller filesystem, so it stays under what spilling keeps free (a fifth).
pub const DISK_RESERVE: u64 = 512 << 20;

fn disk_reserve(total: u64) -> u64 {
    DISK_RESERVE.min(total / 10)
}

/// Bytes free to unprivileged writers on the filesystem holding `path`.
pub fn disk_free(path: &Path) -> Option<u64> {
    disk_space(path).map(|(free, _)| free)
}

/// (free, total) bytes of the filesystem holding `path`.
pub fn disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs writes one struct into `st`; `c` is NUL-terminated.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut st) != 0 {
            return None;
        }
        // Field widths differ by platform (u32 vs u64), hence the casts.
        #[allow(clippy::unnecessary_cast)]
        Some((st.f_bavail as u64 * st.f_frsize as u64, st.f_blocks as u64 * st.f_frsize as u64))
    }
}

/// Bytes of in-memory objects to move to disk so their filesystem keeps a
/// fifth free: a tmpfs (`/dev/shm` defaults to half of RAM) often fills
/// before the memory pool does.
pub fn spill_for_space(free: u64, total: u64) -> u64 {
    (total / 5).saturating_sub(free)
}

/// Why new calls wait for disk, or None: the objects' filesystem is under
/// DISK_RESERVE free. A fixed amount, not a fraction: a large disk that is
/// mostly full but has room keeps working.
pub fn disk_wait(space: Option<(u64, u64)>, dir: &Path) -> Option<String> {
    let (free, total) = space?;
    (free < disk_reserve(total)).then(|| {
        format!(
            "the filesystem holding objects ({}) has {} MiB free; new calls wait for room \
             (finished calls' arguments and ended runs' stored values are deleted)",
            dir.display(),
            free >> 20
        )
    })
}

/// Whether `path` is on tmpfs (its bytes are memory).
#[cfg(target_os = "linux")]
pub fn is_tmpfs(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_bytes()) else { return false };
    // SAFETY: statfs writes one struct into `st`; `c` is NUL-terminated.
    unsafe {
        let mut st: libc::statfs = std::mem::zeroed();
        libc::statfs(c.as_ptr(), &mut st) == 0 && st.f_type == libc::TMPFS_MAGIC
    }
}

#[cfg(not(target_os = "linux"))]
pub fn is_tmpfs(_path: &Path) -> bool {
    false
}

/// Raise the open-file limit to the hard limit, at most 65,536. Executors
/// inherit it (Ray's "too many open files" comes from a 1,024 default).
pub fn raise_nofile() -> Option<u64> {
    // SAFETY: getrlimit/setrlimit read and write one struct we own.
    unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return None;
        }
        let want = lim.rlim_max.min(65_536);
        if lim.rlim_cur < want {
            lim.rlim_cur = want;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
                return None;
            }
        }
        Some(lim.rlim_cur)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_small_container_keeps_half_a_gib_for_the_agent() {
        let b = Budget::plan(6 * GIB, true);
        assert_eq!((b.reserve, b.pool), (6 * GIB / 10, 6 * GIB - 6 * GIB / 10));
        let tiny = Budget::plan(GIB, false);
        assert_eq!((tiny.reserve, tiny.pool), (512 * MIB, 512 * MIB));
        let micro = Budget::plan(512 * MIB, false);
        assert_eq!(micro.reserve, 256 * MIB, "never more than half");
        assert_eq!((tiny.raft_cache, micro.raft_cache), (128 * MIB, 64 * MIB), "the raft caches fit the reserve");
    }

    #[test]
    fn a_large_host_caps_the_reserve() {
        let mac = Budget::plan(36 * GIB, false);
        assert_eq!(mac.reserve, 36 * GIB / 10);
        let big = Budget::plan(1024 * GIB, true);
        assert_eq!((big.reserve, big.pool), (10 * GIB, 1014 * GIB));
        assert_eq!((mac.raft_cache, big.raft_cache), (36 * GIB / 40, GIB), "redb's own 1 GiB at most");
        assert_eq!(big.with_pool(GIB).pool, GIB);
        assert!(mac.describe().contains("on disk"));
    }

    #[test]
    fn sizes_need_a_byte_unit_or_none() {
        assert_eq!(parse_size("1073741824"), Some(GIB));
        assert_eq!(parse_size("512 MiB"), Some(512 * MIB));
        assert_eq!(parse_size("1GiB"), Some(GIB));
        assert_eq!(parse_size("2.5 GB"), Some(2_500_000_000));
        assert_eq!(parse_size("1g"), None, "ambiguous: Docker means GiB, Kubernetes means GB");
        assert_eq!(parse_size("lots"), None);
    }

    #[test]
    fn private_memory_is_anonymous_plus_swapped() {
        let status = "Name:\tpython\nVmHWM:\t 900000 kB\nVmRSS:\t 800000 kB\nRssAnon:\t  300000 kB\n\
                      RssFile:\t  450000 kB\nRssShmem:\t   50000 kB\nVmSwap:\t    1000 kB\n";
        assert_eq!(parse_status_private(status), Some(301_000 * 1024));
        assert_eq!(parse_status_private("Name: x\n"), None);
        // New calls wait only when the objects' filesystem is nearly full.
        let dir = Path::new("/tmp/objects");
        let big = 400u64 << 30;
        assert!(
            disk_wait(Some((100 << 20, big)), dir)
                .is_some_and(|w| w.contains("100 MiB free") && w.contains("/tmp/objects"))
        );
        assert_eq!(disk_wait(Some((20 << 30, big)), dir), None);
        assert_eq!(disk_wait(None, dir), None, "unknown free space never blocks");
        // A small filesystem waits only below a tenth of it, under the fifth
        // that spilling keeps free, so spilling always gets there first.
        assert_eq!(disk_wait(Some((100 << 20, 700 << 20)), dir), None);
        assert!(disk_wait(Some((60 << 20, 700 << 20)), dir).is_some());
        assert_eq!(spill_for_space(100 << 20, 1000 << 20), 100 << 20, "a 1000 MiB tmpfs keeps 200 MiB free");
        assert_eq!(spill_for_space(500 << 20, 1000 << 20), 0);
        assert!(disk_free(Path::new("/")).is_some_and(|b| b > 0));
        // Arena arrays are shared tmpfs pages: what is held past the arena's
        // reuse cache counts toward the leak check, the cache itself does not.
        let held = |shmem_mib: u64| {
            parse_status_held(&format!("RssAnon:\t  102400 kB\nRssShmem:\t{} kB\n", shmem_mib * 1024)).unwrap() >> 20
        };
        assert_eq!((held(0), held(256), held(1256)), (100, 100, 1100));
    }

    #[test]
    fn cgroup_availability_ignores_reclaimable_cache() {
        let stat = "anon 1000\nfile 5000\ninactive_file 4000\nactive_file 1000\n";
        assert_eq!(cgroup_available("10000\n", "7000\n", stat), Some(7000));
        assert_eq!(cgroup_available("max\n", "7000\n", stat), None);
        assert_eq!(parse_memory_stat(stat, "anon"), Some(1000));
        assert_eq!(parse_mem_available("MemTotal: 100 kB\nMemAvailable:    42 kB\n"), Some(42 * 1024));
    }

    #[test]
    fn psi_some_avg10_is_read() {
        let psi = "some avg10=12.50 avg60=3.00 avg300=0.50 total=123\nfull avg10=1.00 avg60=0 avg300=0 total=5\n";
        assert_eq!(parse_psi_some_avg10(psi), Some(12.5));
        assert_eq!(parse_psi_some_avg10("full avg10=1.00\n"), None);
    }

    #[test]
    fn live_readings_work_on_this_host() {
        let avail = available_bytes().expect("available memory");
        assert!(avail > 0);
        let mine = private_bytes(std::process::id()).expect("own private memory");
        assert!(mine > 0, "{mine}");
        assert!(raise_nofile().expect("nofile") >= 1024);
    }
}
