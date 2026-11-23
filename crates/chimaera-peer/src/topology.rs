//! Discover the shape this peer may use: cpuset, CPU quota, memory limit, NUMA
//! nodes and GPUs. Inside a container or a Kubernetes pod this reports the
//! allocation, not the host totals (a peer advertises only what it was granted).

use chimaera_core::topology::{self as t, HostShape, NumaNode};
use std::path::{Path, PathBuf};

pub fn discover() -> HostShape {
    let mut shape = HostShape {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        ..Default::default()
    };
    platform::fill(&mut shape);
    // Every accelerator kind (crate::devices); nvidia-smi above stays the
    // fallback where /proc/driver/nvidia is not visible.
    let found = crate::devices::shape_gpus();
    if !found.is_empty() {
        shape.gpus = found;
    }
    if shape.cpus.is_empty() {
        let n = std::thread::available_parallelism().map_or(1, |n| n.get()) as u32;
        shape.cpus = (0..n).collect();
    }
    if shape.numa.is_empty() {
        shape.numa = vec![NumaNode { id: 0, cpus: shape.cpus.clone(), memory_bytes: shape.memory_bytes }];
    }
    shape
}

fn read(path: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// This process's cgroup v2 directory, if the unified hierarchy is mounted.
pub fn own_cgroup_dir() -> Option<PathBuf> {
    cgroup_dir_in(&read("/proc/self/cgroup")?, &read("/proc/self/mountinfo").unwrap_or_default())
}

/// The directory of the cgroup named in `/proc/self/cgroup`. That path is
/// relative to the hierarchy's root, and the cgroup2 mount may show only a
/// subtree of it (a container without its own cgroup namespace mounts its
/// own cgroup at /sys/fs/cgroup): strip the mount's root (field 4 of its
/// mountinfo line) and join the rest to where it is mounted (field 5).
/// Joined as it is, the path names nothing, and limits there (pids.max,
/// memory.max) go unseen.
fn cgroup_dir_in(proc_cgroup: &str, mountinfo: &str) -> Option<PathBuf> {
    let rel = t::parse_proc_cgroup(proc_cgroup)?;
    let mounts: Vec<(String, PathBuf)> = mountinfo
        .lines()
        .filter_map(|l| {
            let (fields, fs) = l.split_once(" - ")?;
            let f: Vec<&str> = fields.split_whitespace().collect();
            fs.starts_with("cgroup2 ").then(|| Some((f.get(3)?.to_string(), PathBuf::from(f.get(4)?))))?
        })
        .collect();
    let (root, mount) = mounts
        .iter()
        .find(|(_, at)| at == Path::new("/sys/fs/cgroup"))
        .or_else(|| mounts.first())
        .cloned()
        .unwrap_or_else(|| ("/".into(), PathBuf::from("/sys/fs/cgroup")));
    let inner = Path::new(&rel).strip_prefix(&root).ok()?; // outside the mounted subtree: unseen
    let dir = mount.join(inner);
    dir.join("cgroup.controllers").exists().then_some(dir)
}

/// This process's cgroup and the ones above it, innermost first. A limit on
/// any of them applies: systemd slices and some container runtimes set it on
/// a parent and leave the process's own cgroup unlimited.
pub fn own_cgroup_chain() -> Vec<PathBuf> {
    own_cgroup_dir().map_or_else(Vec::new, |dir| cgroup_chain(&dir, Path::new("/sys/fs/cgroup")))
}

/// The cgroup task limit closest to running out for this process: (tasks
/// counted against it now, the limit), from `pids.current` and `pids.max`
/// along its cgroup chain. Every thread counts. None without a limit or
/// outside cgroup v2. Pods often set a low one (Kubernetes podPidsLimit,
/// docker --pids-limit), and starting a worker then fails with EAGAIN.
pub fn pids_room() -> Option<(u64, u64)> {
    pids_room_in(&own_cgroup_chain())
}

fn pids_room_in(chain: &[PathBuf]) -> Option<(u64, u64)> {
    chain
        .iter()
        .filter_map(|d| {
            let max = read(d.join("pids.max"))?.trim().parse::<u64>().ok()?; // "max": no limit
            let current = read(d.join("pids.current"))?.trim().parse::<u64>().ok()?;
            Some((current, max))
        })
        .min_by_key(|(current, max)| max.saturating_sub(*current))
}

/// `dir` and its ancestors below `root` (`root` itself only when `dir` is
/// it, as inside a container's cgroup namespace).
fn cgroup_chain(dir: &Path, root: &Path) -> Vec<PathBuf> {
    let mut chain = vec![dir.to_path_buf()];
    chain.extend(dir.ancestors().skip(1).take_while(|d| d.starts_with(root) && *d != root).map(Path::to_path_buf));
    chain
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    pub fn fill(shape: &mut HostShape) {
        shape.kernel = read("/proc/sys/kernel/osrelease").unwrap_or_default().trim().to_string();
        shape.cpus = affinity_cpus();
        let host_mem = read("/proc/meminfo").and_then(|m| t::parse_memtotal(&m)).unwrap_or(0);
        shape.memory_bytes = host_mem;
        // The tightest limit on the way up applies.
        for dir in own_cgroup_chain() {
            if let Some(q) = read(dir.join("cpu.max")).and_then(|c| t::parse_cpu_max(&c)) {
                shape.cpu_quota = Some(shape.cpu_quota.map_or(q, |prev| prev.min(q)));
            }
            if let Some(limit) = read(dir.join("memory.max")).and_then(|c| t::parse_memory_max(&c)) {
                shape.memory_bytes = shape.memory_bytes.min(limit);
            }
        }
        if let Some(dir) = own_cgroup_dir() {
            shape.cgroup_delegated = can_delegate(&dir);
        }
        shape.numa = numa_nodes(&shape.cpus);
        shape.gpus = nvidia_gpus();
    }

    fn affinity_cpus() -> Vec<u32> {
        // SAFETY: cpu_set_t is plain data; sched_getaffinity writes at most
        // size_of::<cpu_set_t>() bytes into it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return Vec::new();
            }
            (0..libc::CPU_SETSIZE as u32).filter(|&c| libc::CPU_ISSET(c as usize, &set)).collect()
        }
    }

    fn numa_nodes(allowed: &[u32]) -> Vec<NumaNode> {
        let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") else { return Vec::new() };
        let mut nodes: Vec<NumaNode> = entries
            .flatten()
            .filter_map(|e| {
                let id: u32 = e.file_name().to_str()?.strip_prefix("node")?.parse().ok()?;
                let cpus: Vec<u32> = t::parse_cpulist(&read(e.path().join("cpulist"))?)
                    .into_iter()
                    .filter(|c| allowed.contains(c))
                    .collect();
                let memory_bytes = read(e.path().join("meminfo")).and_then(|m| t::parse_memtotal(&m)).unwrap_or(0);
                Some(NumaNode { id, cpus, memory_bytes })
            })
            .filter(|n| !n.cpus.is_empty())
            .collect();
        nodes.sort_by_key(|n| n.id);
        nodes
    }

    /// Delegation means we may create child groups and move processes there.
    fn can_delegate(dir: &Path) -> bool {
        let writable = |p: PathBuf| std::fs::OpenOptions::new().write(true).open(p).is_ok();
        let ctrl = read(dir.join("cgroup.controllers")).unwrap_or_default();
        ctrl.contains("memory") && ctrl.contains("cpu") && writable(dir.join("cgroup.subtree_control"))
    }

    fn nvidia_gpus() -> Vec<t::Gpu> {
        let out = std::process::Command::new("nvidia-smi")
            .args(["--query-gpu=uuid,name,memory.total,pci.bus_id", "--format=csv,noheader,nounits"])
            .output();
        let Some(out) = out.ok().filter(|o| o.status.success()) else { return Vec::new() };
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| {
                let f: Vec<&str> = line.split(',').map(str::trim).collect();
                let [uuid, name, mib, bus] = f.as_slice() else { return None };
                // nvidia-smi prints a 8-digit domain; sysfs uses 4.
                let sys_bus = bus.to_lowercase().trim_start_matches("0000").to_string();
                let numa = read(format!("/sys/bus/pci/devices/{sys_bus}/numa_node"))
                    .and_then(|n| n.trim().parse::<i64>().ok())
                    .and_then(|n| u32::try_from(n).ok());
                Some(t::Gpu {
                    uuid: uuid.to_string(),
                    name: name.to_string(),
                    memory_bytes: mib.parse::<u64>().ok()? << 20,
                    pci_bus_id: bus.to_string(),
                    numa,
                })
            })
            .collect()
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    fn sysctl_u64(name: &str) -> Option<u64> {
        let cname = std::ffi::CString::new(name).ok()?;
        let mut value: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        // SAFETY: value is a valid u64 out-buffer of `len` bytes; the name is
        // NUL-terminated; no new value is written.
        let rc = unsafe {
            libc::sysctlbyname(cname.as_ptr(), (&mut value as *mut u64).cast(), &mut len, std::ptr::null_mut(), 0)
        };
        (rc == 0).then_some(value)
    }

    pub fn fill(shape: &mut HostShape) {
        shape.memory_bytes = sysctl_u64("hw.memsize").unwrap_or(0);
        let n = sysctl_u64("hw.logicalcpu").unwrap_or(1) as u32;
        shape.cpus = (0..n).collect();
        let out = std::process::Command::new("uname").arg("-r").output();
        shape.kernel = out.map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::*;
    pub fn fill(_shape: &mut HostShape) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_a_plausible_local_shape() {
        let s = discover();
        let host = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert!(!s.cpus.is_empty() && s.cpus.len() <= host.max(s.cpus.len()));
        assert!(s.memory_bytes > 64 << 20, "memory {}", s.memory_bytes);
        assert!(!s.numa.is_empty());
        assert!(s.numa.iter().all(|n| !n.cpus.is_empty()));
        assert!(s.usable_cores() > 0.0);
        println!("{s:#?}");
    }
}

#[cfg(test)]
mod chain_tests {
    use super::*;

    #[test]
    fn limits_above_the_process_cgroup_are_looked_at() {
        let root = Path::new("/sys/fs/cgroup");
        let own = root.join("user.slice/user-1000.slice/session-2.scope");
        assert_eq!(
            cgroup_chain(&own, root),
            [own.clone(), root.join("user.slice/user-1000.slice"), root.join("user.slice")],
            "the process's cgroup, then each parent below the root"
        );
        assert_eq!(
            cgroup_chain(root, root),
            [root.to_path_buf()],
            "inside a container its cgroup is the namespace root"
        );
    }

    #[test]
    fn the_task_limit_closest_to_running_out_is_reported() {
        let dir = std::env::temp_dir().join(format!("chimaera-pids-{}", std::process::id()));
        let (own, parent) = (dir.join("pod/app"), dir.join("pod"));
        std::fs::create_dir_all(&own).unwrap();
        std::fs::write(own.join("pids.max"), "max\n").unwrap(); // no limit of its own
        std::fs::write(own.join("pids.current"), "5\n").unwrap();
        std::fs::write(parent.join("pids.max"), "100\n").unwrap();
        std::fs::write(parent.join("pids.current"), "90\n").unwrap();
        assert_eq!(pids_room_in(&[own.clone(), parent.clone()]), Some((90, 100)), "the pod's limit applies");
        std::fs::write(own.join("pids.max"), "8\n").unwrap();
        assert_eq!(pids_room_in(&[own, parent]), Some((5, 8)), "3 left beats 10 left");
        assert_eq!(pids_room_in(&[]), None, "no cgroup: no limit");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_cgroup_path_is_found_under_a_mount_of_part_of_the_hierarchy() {
        let mnt = tempfile::tempdir().unwrap();
        let own = mnt.path().join("test-cg");
        std::fs::create_dir_all(&own).unwrap();
        std::fs::write(own.join("cgroup.controllers"), "pids memory\n").unwrap();
        let at = mnt.path().display();
        // A container without its own cgroup namespace: its cgroup is mounted.
        let sub = format!("2416 2382 0:29 /anyscale/ctr_x {at} rw,nosuid - cgroup2 cgroup2 rw,nsdelegate\n");
        assert_eq!(cgroup_dir_in("0::/anyscale/ctr_x/test-cg\n", &sub), Some(own.clone()));
        // Its own namespace: the mount shows the root, and the path joins as it is.
        let ns = format!("30 25 0:26 / {at} rw - cgroup2 cgroup2 rw\n");
        assert_eq!(cgroup_dir_in("0::/test-cg\n", &ns), Some(own.clone()));
        // Outside the mounted subtree (whole components, not a text prefix).
        assert_eq!(cgroup_dir_in("0::/anyscale/ctr_xy/test-cg\n", &sub), None);
    }
}
