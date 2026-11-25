//! Executors bound to one NUMA domain.
//!
//! On a host with several NUMA domains (sockets, or sub-NUMA clusters), a
//! process whose threads wander between them reads much of its memory across
//! the interconnect. A new executor whose thread cap fits in one domain is
//! bound to it before it runs Python: its CPU affinity becomes the domain's
//! usable CPUs and its memory policy prefers the domain's node, so the pages
//! it first touches land there (and on other nodes once that one is full).
//! An executor holding devices goes to their node; any other to the domain
//! with the most CPUs not already taken by the executors bound there.
//!
//! Binding is best effort: when the kernel refuses it (a CPU gone offline, a
//! node it does not have) the executor runs unbound. A host with one domain
//! never binds, and `CHIMAERA_NUMA_BIND=0` turns binding off.

use chimaera_core::topology::NumaNode;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Mutex;

pub const ENV: &str = "CHIMAERA_NUMA_BIND";

/// The domains executors may be bound to, and the executors bound to each.
pub struct Domains {
    nodes: Vec<NumaNode>,
    /// Each live bound executor's node and thread cap.
    bound: Mutex<HashMap<u32, (u32, u32)>>,
}

impl Domains {
    /// The peer's NUMA nodes (their CPUs within its cpuset); none to bind to
    /// when there is only one or `CHIMAERA_NUMA_BIND=0`.
    pub fn new(nodes: &[NumaNode]) -> Domains {
        let on = std::env::var(ENV).map_or(true, |v| v.trim() != "0");
        let nodes = if on && nodes.len() > 1 { nodes.to_vec() } else { Vec::new() };
        if !nodes.is_empty() {
            tracing::info!(domains = nodes.len(), "executors are bound to one NUMA domain each");
        }
        Domains { nodes, bound: Mutex::default() }
    }

    /// The domain for a new executor of `threads` threads whose devices are
    /// on `near` (one entry per device), if it should be bound.
    pub fn pick(&self, threads: u32, near: &[Option<u32>]) -> Option<NumaNode> {
        if self.nodes.is_empty() {
            return None;
        }
        let bound = self.bound.lock().expect("numa");
        choose(&self.nodes, threads, near, bound.values().copied()).cloned()
    }

    /// `exec`, with `threads` threads, runs bound to `node`.
    pub fn bound(&self, exec: u32, node: u32, threads: u32) {
        self.bound.lock().expect("numa").insert(exec, (node, threads));
    }

    /// `exec` exited.
    pub fn forget(&self, exec: u32) {
        self.bound.lock().expect("numa").remove(&exec);
    }
}

/// Of the domains with at least `threads` CPUs: the devices' node when they
/// share one (and none when they do not), else the one with the most CPUs
/// left after the thread caps already bound there (`bound`: node, threads),
/// the lowest id on a tie.
pub fn choose<'a>(
    nodes: &'a [NumaNode],
    threads: u32,
    near: &[Option<u32>],
    bound: impl Iterator<Item = (u32, u32)>,
) -> Option<&'a NumaNode> {
    let fits = |n: &&NumaNode| n.cpus.len() >= threads.max(1) as usize;
    if let Some(&first) = near.first() {
        let node = first.filter(|f| near.iter().all(|n| *n == Some(*f)))?;
        return nodes.iter().filter(fits).find(|n| n.id == node);
    }
    let mut taken: HashMap<u32, i64> = HashMap::new();
    for (node, threads) in bound {
        *taken.entry(node).or_default() += i64::from(threads);
    }
    let left = |n: &NumaNode| n.cpus.len() as i64 - taken.get(&n.id).copied().unwrap_or(0);
    nodes.iter().filter(fits).max_by_key(|n| (left(n), Reverse(n.id)))
}

/// Bind the child to `node` before it executes: its CPUs, then a memory
/// policy preferring the node. A child the kernel will not give those CPUs
/// runs unbound, its memory policy untouched.
#[cfg(target_os = "linux")]
pub fn bind_before_exec(cmd: &mut tokio::process::Command, node: &NumaNode) {
    const MPOL_PREFERRED: libc::c_long = 1;
    let mut cpus = vec![0u64; node.cpus.iter().max().map_or(1, |m| *m as usize / 64 + 1)];
    for &c in &node.cpus {
        cpus[c as usize / 64] |= 1 << (c % 64);
    }
    let mut nodes = vec![0u64; node.id as usize / 64 + 1];
    nodes[node.id as usize / 64] |= 1 << (node.id % 64);
    // SAFETY: runs in the forked child before exec and makes two system calls
    // on buffers built before the fork; neither allocates. The kernel reads
    // `maxnode - 1` bits of the node mask, hence the + 1.
    unsafe {
        cmd.pre_exec(move || {
            if libc::sched_setaffinity(0, cpus.len() * 8, cpus.as_ptr().cast()) == 0 {
                libc::syscall(
                    libc::SYS_set_mempolicy,
                    MPOL_PREFERRED,
                    nodes.as_ptr(),
                    (nodes.len() * 64 + 1) as libc::c_ulong,
                );
            }
            Ok(())
        });
    }
}

/// No NUMA binding here: the executor runs unbound.
#[cfg(not(target_os = "linux"))]
pub fn bind_before_exec(_cmd: &mut tokio::process::Command, _node: &NumaNode) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u32, cpus: std::ops::Range<u32>) -> NumaNode {
        NumaNode { id, cpus: cpus.collect(), memory_bytes: 0 }
    }

    fn ids(n: Option<&NumaNode>) -> Option<u32> {
        n.map(|n| n.id)
    }

    #[test]
    fn new_executors_alternate_between_domains_by_threads_bound() {
        let nodes = [node(0, 0..4), node(1, 4..8)];
        assert_eq!(ids(choose(&nodes, 1, &[], [].into_iter())), Some(0), "a tie goes to the lowest id");
        assert_eq!(ids(choose(&nodes, 1, &[], [(0, 1)].into_iter())), Some(1));
        assert_eq!(ids(choose(&nodes, 2, &[], [(0, 1), (1, 1)].into_iter())), Some(0));
        assert_eq!(ids(choose(&nodes, 1, &[], [(0, 3), (1, 2), (1, 2)].into_iter())), Some(0), "1 left beats 0");
    }

    #[test]
    fn a_grant_larger_than_every_domain_is_not_bound() {
        let nodes = [node(0, 0..4), node(1, 4..6)];
        assert_eq!(ids(choose(&nodes, 4, &[], [(0, 4)].into_iter())), Some(0), "only node 0 holds 4");
        assert_eq!(ids(choose(&nodes, 5, &[], [].into_iter())), None);
    }

    #[test]
    fn devices_pull_their_executor_to_their_node() {
        let nodes = [node(0, 0..4), node(1, 4..8)];
        assert_eq!(ids(choose(&nodes, 1, &[Some(1), Some(1)], [(1, 4)].into_iter())), Some(1), "even when busy");
        assert_eq!(ids(choose(&nodes, 1, &[Some(0), Some(1)], [].into_iter())), None, "split across nodes");
        assert_eq!(ids(choose(&nodes, 1, &[None], [].into_iter())), None, "node unknown");
        assert_eq!(ids(choose(&nodes, 8, &[Some(1)], [].into_iter())), None, "too many threads for it");
    }

    #[test]
    fn one_domain_or_the_switch_off_binds_nothing() {
        let one = Domains::new(&[node(0, 0..8)]);
        assert!(one.pick(1, &[]).is_none());
        let two = Domains::new(&[node(0, 0..4), node(1, 4..8)]);
        let first = two.pick(1, &[]).expect("two domains");
        two.bound(7, first.id, 1);
        assert_eq!(two.pick(1, &[]).map(|n| n.id), Some(1 - first.id));
        two.forget(7);
        assert_eq!(two.pick(1, &[]).map(|n| n.id), Some(0));
    }

    /// The pre-exec hook binds a real child: its affinity is the node's CPUs.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_bound_child_runs_on_its_domains_cpus_only() {
        let allowed = chimaera_core::topology::allowed_cpus(&crate::devices::HostFs::new(std::path::Path::new("/")));
        let status = |n: Option<&NumaNode>| {
            let mut cmd = tokio::process::Command::new("cat");
            cmd.arg("/proc/self/status");
            if let Some(n) = n {
                bind_before_exec(&mut cmd, n);
            }
            async move {
                let out = cmd.output().await.expect("cat runs");
                let text = String::from_utf8_lossy(&out.stdout).into_owned();
                let line = text.lines().find_map(|l| l.strip_prefix("Cpus_allowed_list:")).unwrap_or_default();
                chimaera_core::topology::parse_cpulist(line)
            }
        };
        let first = NumaNode { id: 0, cpus: vec![allowed[0]], memory_bytes: 0 };
        assert_eq!(status(Some(&first)).await, [allowed[0]]);
        // A CPU the host does not have: the child runs, unbound.
        let gone = NumaNode { id: 0, cpus: vec![100_000], memory_bytes: 0 };
        assert_eq!(status(Some(&gone)).await, status(None).await);
    }
}
