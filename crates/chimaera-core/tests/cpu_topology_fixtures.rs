//! CPU layout discovery against host captures in `tests/fixtures/topology/cpu/`.
//!
//! A capture is what this prints on a host, one `path:line` per line:
//!
//! ```sh
//! grep -rH . /proc/self/status /sys/devices/system/cpu/online \
//!     /sys/devices/system/cpu/cpu[0-9]*/topology /sys/devices/system/cpu/cpu[0-9]*/cache/index[0-9]* \
//!     /sys/devices/system/node/node[0-9]*/cpulist /sys/devices/system/node/node[0-9]*/meminfo
//! ```
//!
//! (`/proc/self/status` is then grep's own, with the shell's affinity and cpuset.)
//! The three here are written by hand in that form, after the layouts they are
//! named for; a capture from such a host replaces one as it is.

use chimaera_core::devices::SysFs;
use chimaera_core::topology::*;
use std::collections::BTreeMap;

/// A recorded host's files, by absolute path.
struct Capture(BTreeMap<String, String>);

impl Capture {
    fn new(host: &str) -> Capture {
        let path = format!("{}/../../tests/fixtures/topology/cpu/{host}.txt", env!("CARGO_MANIFEST_DIR"));
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let mut files: BTreeMap<String, String> = BTreeMap::new();
        for (file, line) in text.lines().filter_map(|l| l.split_once(':')) {
            let f = files.entry(file.to_string()).or_default();
            f.push_str(line);
            f.push('\n');
        }
        Capture(files)
    }

    fn under(&self, dir: &str) -> impl Iterator<Item = &str> {
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        self.0.keys().filter_map(move |p| p.strip_prefix(&prefix))
    }
}

impl SysFs for Capture {
    fn read(&self, path: &str) -> Option<String> {
        self.0.get(path).cloned()
    }

    fn list(&self, dir: &str) -> Vec<String> {
        let mut names: Vec<String> =
            self.under(dir).map(|rest| rest.split('/').next().unwrap_or(rest).to_string()).collect();
        names.sort();
        names.dedup();
        names
    }

    fn is_dir(&self, path: &str) -> bool {
        self.under(path).next().is_some()
    }

    fn exists(&self, path: &str) -> bool {
        self.0.contains_key(path) || self.is_dir(path)
    }
}

fn shape(host: &str) -> HostShape {
    let fs = Capture::new(host);
    let cpus = allowed_cpus(&fs);
    HostShape { cpu_topology: cpu_topology(&fs, &cpus), numa: numa_nodes(&fs, &cpus), cpus, ..Default::default() }
}

/// (cpu, socket, core, llc) for each usable CPU.
fn places(s: &HostShape) -> Vec<(u32, u32, u32, Option<u32>)> {
    s.cpu_topology.iter().map(|c| (c.id, c.socket, c.core, c.llc)).collect()
}

#[test]
fn sparse_ids_keep_their_numbers_and_pair_siblings_64_apart() {
    let s = shape("sparse-ids");
    // The mask names 0-127; only the online ones are usable.
    assert_eq!(s.cpus, [0, 1, 2, 3, 64, 65, 66, 67]);
    assert_eq!(
        places(&s),
        [
            (0, 0, 0, None),
            (1, 0, 1, None),
            (2, 0, 2, None),
            (3, 0, 3, None),
            (64, 0, 0, None),
            (65, 0, 1, None),
            (66, 0, 2, None),
            (67, 0, 3, None)
        ],
        "this machine lists no caches"
    );
    assert_eq!(s.cores_sockets_llcs(), (4, 1, None));
    assert_eq!(s.numa.len(), 1);
    assert_eq!(s.numa[0].cpus, s.cpus);
    assert_eq!(s.numa[0].memory_bytes, 16 << 30);
}

#[test]
fn smt_off_gives_one_cpu_per_core_from_an_older_kernels_sibling_list() {
    let s = shape("smt-off");
    // Siblings 4-7 are offline.
    assert_eq!(s.cpus, [0, 1, 2, 3]);
    assert_eq!(places(&s), [(0, 0, 0, Some(0)), (1, 0, 1, Some(0)), (2, 0, 2, Some(2)), (3, 0, 3, Some(2))]);
    assert_eq!(s.cores_sockets_llcs(), (4, 1, Some(2)), "two L3 groups, not the L1 or L2 of each core");
}

#[test]
fn a_container_cpuset_across_two_sockets() {
    let s = shape("two-socket-container");
    assert_eq!(s.cpus, [2, 3, 4, 5, 10, 11, 12, 13]);
    // Cores are named by their lowest CPU on the host, caches likewise (CPU 0
    // and 4, outside the cpuset, name the sockets' L3s).
    assert_eq!(
        places(&s),
        [
            (2, 0, 2, Some(0)),
            (3, 0, 3, Some(0)),
            (4, 1, 4, Some(4)),
            (5, 1, 5, Some(4)),
            (10, 0, 2, Some(0)),
            (11, 0, 3, Some(0)),
            (12, 1, 4, Some(4)),
            (13, 1, 5, Some(4))
        ]
    );
    assert_eq!(s.cores_sockets_llcs(), (4, 2, Some(2)));
    let nodes: Vec<(u32, Vec<u32>, u64)> = s.numa.iter().map(|n| (n.id, n.cpus.clone(), n.memory_bytes)).collect();
    assert_eq!(nodes, [(0, vec![2, 3, 10, 11], 64 << 30), (1, vec![4, 5, 12, 13], 64 << 30)]);
}

#[test]
fn a_cpuset_inside_one_socket_sees_one_node() {
    let mut fs = Capture::new("two-socket-container");
    fs.0.insert("/proc/self/status".into(), "Cpus_allowed_list:\t5-6,13\n".into());
    let cpus = allowed_cpus(&fs);
    assert_eq!(cpus, [5, 6, 13]);
    let nodes = numa_nodes(&fs, &cpus);
    assert_eq!(nodes.iter().map(|n| n.id).collect::<Vec<_>>(), [1]);
    let s = HostShape { cpu_topology: cpu_topology(&fs, &cpus), cpus, ..Default::default() };
    assert_eq!(s.cores_sockets_llcs(), (2, 1, Some(1)), "5 and 13 are one core");
}

#[test]
fn missing_files_mean_no_layout_not_a_wrong_one() {
    let fs = Capture(BTreeMap::new());
    assert!(allowed_cpus(&fs).is_empty(), "no /proc: the peer falls back to its own count");
    assert_eq!(
        cpu_topology(&fs, &[0, 1]),
        [LogicalCpu { id: 0, socket: 0, core: 0, llc: None }, LogicalCpu { id: 1, socket: 0, core: 1, llc: None }]
    );
    assert!(numa_nodes(&fs, &[0, 1]).is_empty());
}
