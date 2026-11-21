//! Per-process resource readings for attempt records.
//!
//! An executor runs one attempt at a time, so the difference between two
//! readings of its process is that attempt's CPU time (scope `process`, exact
//! for the interval). The peak resident size the OS reports is a high-water
//! mark for the whole process lifetime, so it is labelled `process_lifetime`:
//! reporting it as the attempt's own peak would misattribute an earlier
//! attempt's allocation (the design doc's 8/64/8 MiB example).

/// One reading of a process.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Reading {
    pub cpu_ns: u64,
    pub rss_bytes: u64,
    pub peak_rss_bytes: u64,
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTimebaseInfo {
    numer: u32,
    denom: u32,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    // <mach/mach_time.h>; stable since 10.0.
    fn mach_timebase_info(info: *mut MachTimebaseInfo) -> i32;
}

#[cfg(target_os = "macos")]
pub fn read(pid: u32) -> Option<Reading> {
    use std::sync::OnceLock;
    static TIMEBASE: OnceLock<(u32, u32)> = OnceLock::new();
    let (numer, denom) = *TIMEBASE.get_or_init(|| {
        let mut tb = MachTimebaseInfo { numer: 0, denom: 0 };
        // SAFETY: tb is a valid out-parameter for the duration of the call.
        unsafe { mach_timebase_info(&mut tb) };
        if tb.denom == 0 { (1, 1) } else { (tb.numer, tb.denom) }
    });
    // SAFETY: rusage_info_v4 is plain data; proc_pid_rusage writes at most one
    // struct of the flavor requested into it.
    let ri = unsafe {
        let mut ri: libc::rusage_info_v4 = std::mem::zeroed();
        let rc = libc::proc_pid_rusage(pid as i32, libc::RUSAGE_INFO_V4, (&mut ri as *mut libc::rusage_info_v4).cast());
        if rc != 0 {
            return None;
        }
        ri
    };
    // ri_user_time / ri_system_time are mach absolute-time ticks on Apple
    // Silicon (nanoseconds only on Intel); convert with the timebase.
    let ticks = ri.ri_user_time + ri.ri_system_time;
    let cpu_ns = (ticks as u128 * numer as u128 / denom as u128) as u64;
    Some(Reading { cpu_ns, rss_bytes: ri.ri_phys_footprint, peak_rss_bytes: ri.ri_lifetime_max_phys_footprint })
}

#[cfg(target_os = "linux")]
pub fn read(pid: u32) -> Option<Reading> {
    // utime and stime are fields 14 and 15 of /proc/<pid>/stat, after the
    // parenthesised command name (which may itself contain spaces).
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f.get(11)?.parse().ok()?;
    let stime: u64 = f.get(12)?.parse().ok()?;
    // SAFETY: sysconf has no preconditions.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kb = |key: &str| -> u64 {
        status
            .lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
            * 1024
    };
    Some(Reading {
        cpu_ns: (utime + stime) * 1_000_000_000 / hz,
        rss_bytes: kb("VmRSS:"),
        peak_rss_bytes: kb("VmHWM:"),
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn read(_pid: u32) -> Option<Reading> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_process_accumulates_cpu() {
        let me = std::process::id();
        let a = read(me).expect("can read own process");
        let t = std::time::Instant::now();
        let mut x = 0u64;
        while t.elapsed().as_millis() < 200 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(x);
        let b = read(me).unwrap();
        let cpu = (b.cpu_ns - a.cpu_ns) as f64 / 1e9;
        assert!((0.15..1.0).contains(&cpu), "cpu {cpu}");
        assert!(b.peak_rss_bytes >= b.rss_bytes && b.rss_bytes > 0);
    }
}

/// Per-attempt cgroups (Linux, cgroup v2 with delegation).
///
/// Layout under the peer's own cgroup: `agent/` holds the peer itself (cgroup
/// v2 forbids processes in a group that delegates controllers), `iso/a<n>/`
/// holds one isolated attempt's executor. The executor joins its group before
/// it starts, so every byte it allocates and every CPU-second it uses is
/// charged there: `memory.peak` and `cpu.stat` are the attempt's own.
#[cfg(target_os = "linux")]
pub mod cgroup {
    use std::io;
    use std::path::{Path, PathBuf};

    pub struct Cgroups {
        iso: PathBuf,
    }

    fn write(path: &Path, value: &str) -> io::Result<()> {
        std::fs::write(path, value)
    }

    impl Cgroups {
        /// Prepare delegation under `own` (this process's cgroup directory).
        pub fn setup(own: &Path) -> io::Result<Cgroups> {
            let agent = own.join("agent");
            std::fs::create_dir_all(&agent)?;
            // Move every process of our group (this peer and anything it
            // already started) into agent/.
            let procs = std::fs::read_to_string(own.join("cgroup.procs"))?;
            for pid in procs.split_whitespace() {
                let _ = write(&agent.join("cgroup.procs"), pid);
            }
            write(&own.join("cgroup.subtree_control"), "+cpu +memory +pids")?;
            let iso = own.join("iso");
            std::fs::create_dir_all(&iso)?;
            write(&iso.join("cgroup.subtree_control"), "+cpu +memory +pids")?;
            Ok(Cgroups { iso })
        }

        /// Create the group for one isolated attempt with its limits.
        pub fn attempt(&self, n: u64, memory_limit: u64, cpus: f64) -> io::Result<PathBuf> {
            let g = self.iso.join(format!("a{n}"));
            std::fs::create_dir_all(&g)?;
            if memory_limit > 0 {
                write(&g.join("memory.max"), &memory_limit.to_string())?;
                // Swapping would hide the overrun instead of failing it.
                let _ = write(&g.join("memory.swap.max"), "0");
            }
            if cpus > 0.0 {
                let period = 100_000u64;
                write(&g.join("cpu.max"), &format!("{} {period}", (cpus * period as f64).ceil() as u64))?;
            }
            Ok(g)
        }

        pub fn read(group: &Path) -> Option<Usage> {
            let stat = std::fs::read_to_string(group.join("cpu.stat")).ok()?;
            let field = |text: &str, key: &str| -> u64 {
                text.lines().find_map(|l| l.strip_prefix(key).and_then(|v| v.trim().parse().ok())).unwrap_or(0)
            };
            let events = std::fs::read_to_string(group.join("memory.events")).unwrap_or_default();
            let num =
                |name: &str| -> Option<u64> { std::fs::read_to_string(group.join(name)).ok()?.trim().parse().ok() };
            Some(Usage {
                cpu_usec: field(&stat, "usage_usec "),
                throttled_usec: field(&stat, "throttled_usec "),
                nr_throttled: field(&stat, "nr_throttled "),
                memory_peak: num("memory.peak"),
                memory_current: num("memory.current").unwrap_or(0),
                oom_kills: field(&events, "oom_kill "),
            })
        }

        /// Kill everything in the group and remove it.
        pub fn remove(group: &Path) {
            let _ = write(&group.join("cgroup.kill"), "1");
            for _ in 0..100 {
                if std::fs::remove_dir(group).is_ok() {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct Usage {
        pub cpu_usec: u64,
        pub throttled_usec: u64,
        pub nr_throttled: u64,
        /// `None` on kernels without memory.peak.
        pub memory_peak: Option<u64>,
        pub memory_current: u64,
        pub oom_kills: u64,
    }
}

/// Per-attempt isolation, when this host supports it (Linux with cgroup v2
/// delegation). Elsewhere it cannot be constructed and isolated requests are
/// rejected as infeasible.
pub struct Isolation {
    #[cfg(target_os = "linux")]
    cg: cgroup::Cgroups,
}

/// What an isolated attempt used, read from its own cgroup.
#[derive(Clone, Copy, Debug, Default)]
pub struct IsoUsage {
    pub cpu_ns: u64,
    pub throttled_ns: u64,
    pub memory_peak: Option<u64>,
    pub memory_current: u64,
    pub oom_kills: u64,
}

impl Isolation {
    #[cfg(target_os = "linux")]
    pub fn setup(own: &std::path::Path) -> std::io::Result<Isolation> {
        Ok(Isolation { cg: cgroup::Cgroups::setup(own)? })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn setup(_own: &std::path::Path) -> std::io::Result<Isolation> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "per-attempt cgroups need Linux cgroup v2"))
    }

    #[cfg(target_os = "linux")]
    pub fn group(&self, n: u64, memory_limit: u64, cpus: f64) -> std::io::Result<std::path::PathBuf> {
        self.cg.attempt(n, memory_limit, cpus)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn group(&self, _n: u64, _memory_limit: u64, _cpus: f64) -> std::io::Result<std::path::PathBuf> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "per-attempt cgroups need Linux cgroup v2"))
    }

    pub fn read(group: &std::path::Path) -> Option<IsoUsage> {
        #[cfg(target_os = "linux")]
        {
            cgroup::Cgroups::read(group).map(|u| IsoUsage {
                cpu_ns: u.cpu_usec * 1000,
                throttled_ns: u.throttled_usec * 1000,
                memory_peak: u.memory_peak,
                memory_current: u.memory_current,
                oom_kills: u.oom_kills,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = group;
            None
        }
    }

    pub fn remove(group: &std::path::Path) {
        #[cfg(target_os = "linux")]
        cgroup::Cgroups::remove(group);
        #[cfg(not(target_os = "linux"))]
        let _ = group;
    }
}
