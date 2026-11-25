//! Host shape description and the pure parsers for Linux sysfs/cgroup files.
//!
//! IO lives in `chimaera-peer`; these functions only interpret file contents,
//! so they are unit-tested here against captured samples.

use crate::devices::SysFs;
use std::collections::BTreeSet;

/// What one peer may actually use: its allocation, not the host's totals.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostShape {
    /// Logical CPUs this process may run on (cpuset / affinity).
    pub cpus: Vec<u32>,
    /// Where each of `cpus` sits, in the same order; empty when unknown.
    pub cpu_topology: Vec<LogicalCpu>,
    /// CPU bandwidth limit in cores from `cpu.max`, if any.
    pub cpu_quota: Option<f64>,
    pub numa: Vec<NumaNode>,
    /// min(host memory, cgroup `memory.max`).
    pub memory_bytes: u64,
    pub gpus: Vec<Gpu>,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    /// True when this process can create child cgroups (isolated accounting).
    pub cgroup_delegated: bool,
}

impl HostShape {
    /// Usable cores: the cpuset size, capped by the bandwidth quota.
    pub fn usable_cores(&self) -> f64 {
        let n = self.cpus.len().max(1) as f64;
        self.cpu_quota.map_or(n, |q| q.min(n))
    }

    /// Physical cores among `cpus` (SMT siblings count once), the sockets
    /// they span, and their last-level caches when every one is known.
    /// Without a layout each CPU is a core on one socket.
    pub fn cores_sockets_llcs(&self) -> (usize, usize, Option<usize>) {
        if self.cpu_topology.is_empty() {
            return (self.cpus.len(), 1, None);
        }
        let count = |key: fn(&LogicalCpu) -> u32| self.cpu_topology.iter().map(key).collect::<BTreeSet<_>>().len();
        let llcs: Option<BTreeSet<u32>> = self.cpu_topology.iter().map(|c| c.llc).collect();
        (count(|c| c.core), count(|c| c.socket), llcs.map(|l| l.len()))
    }
}

/// One usable logical CPU and the hardware it shares. Cores and caches are
/// named by the lowest CPU id on the host that shares them, so the names hold
/// whichever of their CPUs a cpuset leaves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogicalCpu {
    pub id: u32,
    /// Physical package.
    pub socket: u32,
    /// Physical core: its SMT siblings have the same one.
    pub core: u32,
    /// Last-level cache; None when the host does not describe its caches.
    pub llc: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NumaNode {
    pub id: u32,
    pub cpus: Vec<u32>,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Gpu {
    pub uuid: String,
    pub name: String,
    pub memory_bytes: u64,
    pub pci_bus_id: String,
    pub numa: Option<u32>,
}

/// Parse a cpulist such as `0-3,8-11` into sorted, unique CPU ids.
pub fn parse_cpulist(input: &str) -> Vec<u32> {
    let mut cpus = Vec::new();
    for part in input.trim().split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse::<u32>(), b.parse::<u32>()) {
                    cpus.extend(a.min(b)..=a.max(b));
                }
            }
            None => {
                if let Ok(c) = part.parse() {
                    cpus.push(c);
                }
            }
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

/// `MemTotal` in bytes from a per-node meminfo (`Node 0 MemTotal: 32768000 kB`)
/// or from `/proc/meminfo` (`MemTotal: 32768000 kB`).
pub fn parse_memtotal(content: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let mut words = line.split_whitespace().skip_while(|w| *w != "MemTotal:");
        words.next()?;
        let kb: u64 = words.next()?.parse().ok()?;
        Some(kb * 1024)
    })
}

/// cgroup v2 `cpu.max` (`"max 100000"` or `"200000 100000"`) as cores.
pub fn parse_cpu_max(content: &str) -> Option<f64> {
    let mut it = content.split_whitespace();
    let quota = it.next()?;
    let period: f64 = it.next()?.parse().ok()?;
    if quota == "max" || period <= 0.0 {
        return None;
    }
    Some(quota.parse::<f64>().ok()? / period)
}

/// cgroup v2 `memory.max`: `"max"` (no limit) or bytes.
pub fn parse_memory_max(content: &str) -> Option<u64> {
    let t = content.trim();
    if t == "max" { None } else { t.parse().ok() }
}

/// The cgroup v2 path of this process from `/proc/self/cgroup` (`0::/a/b`).
pub fn parse_proc_cgroup(content: &str) -> Option<String> {
    content.lines().find_map(|l| l.strip_prefix("0::").map(|p| p.trim().to_string()))
}

/// The CPUs this process may run on: `Cpus_allowed_list` in
/// `/proc/self/status` (its affinity, which its cpuset bounds) less those
/// not online (an unrestricted mask also names absent, possible CPUs).
/// Unlike `sched_getaffinity` with a fixed `cpu_set_t`, no 1024-CPU limit.
pub fn allowed_cpus(fs: &dyn SysFs) -> Vec<u32> {
    let status = fs.read("/proc/self/status").unwrap_or_default();
    let Some(mask) = status.lines().find_map(|l| l.strip_prefix("Cpus_allowed_list:")) else { return Vec::new() };
    let mut cpus = parse_cpulist(mask);
    if let Some(online) = fs.read("/sys/devices/system/cpu/online").map(|o| parse_cpulist(&o)) {
        cpus.retain(|c| online.binary_search(c).is_ok());
    }
    cpus
}

/// Where each of `cpus` sits, from `/sys/devices/system/cpu/cpu<N>/topology`
/// and the CPU's highest-level data or unified cache. A CPU without those
/// files is a core of its own on socket 0, with no known cache.
pub fn cpu_topology(fs: &dyn SysFs, cpus: &[u32]) -> Vec<LogicalCpu> {
    cpus.iter()
        .map(|&id| {
            let dir = format!("/sys/devices/system/cpu/cpu{id}");
            let lowest =
                |file: &str| fs.read(&format!("{dir}/{file}")).and_then(|l| parse_cpulist(&l).first().copied());
            // core_cpus_list replaced thread_siblings_list in Linux 5.7.
            let core = lowest("topology/core_cpus_list").or_else(|| lowest("topology/thread_siblings_list"));
            // -1 (unknown) on some virtual machines.
            let socket = fs.read(&format!("{dir}/topology/physical_package_id")).and_then(|s| s.trim().parse().ok());
            let llc = fs
                .list(&format!("{dir}/cache"))
                .iter()
                .filter_map(|index| {
                    let file = |name: &str| fs.read(&format!("{dir}/cache/{index}/{name}"));
                    if file("type")?.trim() == "Instruction" {
                        return None;
                    }
                    let level: u32 = file("level")?.trim().parse().ok()?;
                    Some((level, parse_cpulist(&file("shared_cpu_list")?).first().copied()?))
                })
                .max_by_key(|(level, _)| *level)
                .map(|(_, first)| first);
            LogicalCpu { id, socket: socket.unwrap_or(0), core: core.unwrap_or(id), llc }
        })
        .collect()
}

/// The NUMA nodes holding any of `cpus`, each with those of `cpus` it holds,
/// from `/sys/devices/system/node`.
pub fn numa_nodes(fs: &dyn SysFs, cpus: &[u32]) -> Vec<NumaNode> {
    let root = "/sys/devices/system/node";
    let mut nodes: Vec<NumaNode> = fs
        .list(root)
        .iter()
        .filter_map(|name| {
            let id: u32 = name.strip_prefix("node")?.parse().ok()?;
            let mut mine = parse_cpulist(&fs.read(&format!("{root}/{name}/cpulist"))?);
            mine.retain(|c| cpus.contains(c));
            let memory_bytes = fs.read(&format!("{root}/{name}/meminfo")).and_then(|m| parse_memtotal(&m)).unwrap_or(0);
            Some(NumaNode { id, cpus: mine, memory_bytes })
        })
        .filter(|n| !n.cpus.is_empty())
        .collect();
    nodes.sort_by_key(|n| n.id);
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpulist_ranges_singles_and_junk() {
        assert_eq!(parse_cpulist("0-3,8-9\n"), vec![0, 1, 2, 3, 8, 9]);
        assert_eq!(parse_cpulist("5,1,3-2,x,"), vec![1, 2, 3, 5]);
        assert!(parse_cpulist("").is_empty());
    }

    #[test]
    fn memtotal_from_node_and_proc_formats() {
        assert_eq!(parse_memtotal("Node 0 MemTotal:       32768000 kB\nNode 0 MemFree: 1 kB"), Some(32768000 * 1024));
        assert_eq!(parse_memtotal("MemTotal:        8000 kB\nMemFree: 3 kB"), Some(8000 * 1024));
        assert_eq!(parse_memtotal("nothing here"), None);
    }

    #[test]
    fn cgroup_limits() {
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max("250000 100000\n"), Some(2.5));
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("1073741824\n"), Some(1 << 30));
        assert_eq!(parse_proc_cgroup("0::/user.slice/x.scope\n"), Some("/user.slice/x.scope".into()));
        let shape = HostShape { cpus: vec![0, 1, 2, 3], cpu_quota: Some(2.5), ..Default::default() };
        assert_eq!(shape.usable_cores(), 2.5);
    }
}
