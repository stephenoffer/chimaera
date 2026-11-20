//! Host shape description and the pure parsers for Linux sysfs/cgroup files.
//!
//! IO lives in `chimaera-peer`; these functions only interpret file contents,
//! so they are unit-tested here against captured samples.

/// What one peer may actually use: its allocation, not the host's totals.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostShape {
    /// Logical CPUs this process may run on (cpuset / affinity).
    pub cpus: Vec<u32>,
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
