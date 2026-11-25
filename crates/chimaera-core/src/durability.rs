//! What an acknowledged commit of a shard group survives, from what its
//! voters advertise.
//!
//! A group acknowledges an entry once a majority of its voters hold it, and
//! the leader counts whichever majority answers first. So a group promises
//! only what its weakest majority gives, and the weakest majority always
//! takes in the weakest voter: one voter that keeps its log in the page cache
//! is enough for a commit to be acknowledged by a majority of which one
//! member forgets it when its machine restarts. And where one machine holds a
//! majority of the voters, losing that machine for good can take every copy
//! of a commit.

use crate::pb;
use std::collections::BTreeMap;

/// What an acknowledged commit survives, weakest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Guarantee {
    /// The acknowledging peers' processes dying: some voter keeps commits
    /// only in its machine's page cache.
    Process,
    /// Also every machine crashing or restarting at once (every voter syncs
    /// its log), but not the loss of the machine that holds a majority of
    /// the voters.
    Reboot,
    /// Also the permanent loss of any one machine: every voter syncs, and no
    /// machine holds a majority of them.
    Machine,
}

impl Guarantee {
    pub fn name(self) -> &'static str {
        match self {
            Guarantee::Process => "process",
            Guarantee::Reboot => "reboot",
            Guarantee::Machine => "machine",
        }
    }

    /// What a peer writing its raft log this way asks of its groups: a synced
    /// voting majority on separate machines, unless it was told not to sync.
    pub fn requested(sync: pb::RaftSync) -> Guarantee {
        if syncs(sync) { Guarantee::Machine } else { Guarantee::Process }
    }
}

fn syncs(sync: pb::RaftSync) -> bool {
    matches!(sync, pb::RaftSync::Fsync | pb::RaftSync::Full)
}

/// Whether `m` may take a voter seat in a group that must give `floor`.
pub fn meets(floor: Guarantee, m: &pb::Member) -> bool {
    floor == Guarantee::Process || syncs(m.raft_sync())
}

/// The guarantee of a group with these voters (node id, and its member
/// record if this peer knows it), and why it is no stronger.
pub fn qualify(voters: &[(u64, Option<&pb::Member>)]) -> (Guarantee, Option<String>) {
    let name = |id: u64, m: Option<&pb::Member>| m.map_or_else(|| format!("node {id}"), |m| m.addr.clone());
    if voters.is_empty() {
        return (Guarantee::Process, Some("no voters known".into()));
    }
    for &(id, m) in voters {
        let why = match m.map(pb::Member::raft_sync) {
            Some(s) if syncs(s) => continue,
            Some(pb::RaftSync::Off) => "keeps its raft log in the page cache (CHIMAERA_RAFT_SYNC=none)",
            Some(_) => "does not say whether it syncs its raft log",
            None => "is not known to this peer",
        };
        return (Guarantee::Process, Some(format!("voter {} {why}", name(id, m))));
    }
    let mut per_machine: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, m) in voters {
        *per_machine.entry(m.map_or("", |m| m.host.as_str())).or_default() += 1;
    }
    let majority = voters.len() / 2 + 1;
    match per_machine.into_iter().find(|(_, n)| *n >= majority) {
        Some((host, n)) => {
            let on = if host.is_empty() { "machines they do not name".to_string() } else { format!("machine {host}") };
            (Guarantee::Reboot, Some(format!("{n} of its {} voters are on {on}", voters.len())))
        }
        None => (Guarantee::Machine, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voter(n: u64, sync: pb::RaftSync, host: &str) -> pb::Member {
        pb::Member {
            node_id: n,
            addr: format!("10.0.0.{n}:7700"),
            raft_sync: sync as i32,
            host: host.into(),
            ..Default::default()
        }
    }

    fn of(ms: &[pb::Member]) -> Guarantee {
        let v: Vec<(u64, Option<&pb::Member>)> = ms.iter().map(|m| (m.node_id, Some(m))).collect();
        qualify(&v).0
    }

    use pb::RaftSync::{Fsync, Full, Off, Unknown};

    #[test]
    fn a_majority_that_does_not_sync_gives_process_durability_only() {
        // Two of three voters keep their log in the page cache: they alone
        // make a majority, so a commit may be acknowledged by them alone.
        let (g, why) = qualify(&[
            (1, Some(&voter(1, Fsync, "a"))),
            (2, Some(&voter(2, Off, "b"))),
            (3, Some(&voter(3, Off, "c"))),
        ]);
        assert_eq!(g, Guarantee::Process);
        assert!(why.unwrap().contains("10.0.0.2:7700 keeps its raft log in the page cache"));
        // One unsynced voter is enough: it is in some majority.
        assert_eq!(of(&[voter(1, Fsync, "a"), voter(2, Full, "b"), voter(3, Off, "c")]), Guarantee::Process);
        // A peer too old to say, or one this peer does not know, counts as unsynced.
        assert_eq!(of(&[voter(1, Fsync, "a"), voter(2, Fsync, "b"), voter(3, Unknown, "c")]), Guarantee::Process);
        assert_eq!(qualify(&[(1, Some(&voter(1, Fsync, "a"))), (2, None), (3, None)]).0, Guarantee::Process);
        assert_eq!(qualify(&[]).0, Guarantee::Process);
    }

    #[test]
    fn synced_voters_give_machine_durability_only_spread_over_machines() {
        assert_eq!(of(&[voter(1, Fsync, "a"), voter(2, Full, "b"), voter(3, Fsync, "c")]), Guarantee::Machine);
        // Two of three on one machine: its loss can take a whole majority.
        let shared = [voter(1, Fsync, "a"), voter(2, Fsync, "a"), voter(3, Fsync, "c")];
        assert_eq!(of(&shared), Guarantee::Reboot);
        // Five voters: a machine may hold two (fewer than a majority), not three.
        let five = |hosts: [&str; 5]| (1..=5).map(|n| voter(n, Fsync, hosts[n as usize - 1])).collect::<Vec<_>>();
        assert_eq!(of(&five(["a", "a", "b", "c", "d"])), Guarantee::Machine);
        assert_eq!(of(&five(["a", "a", "a", "c", "d"])), Guarantee::Reboot);
        // One voter: a reboot keeps its synced log, the machine's loss does not.
        assert_eq!(of(&[voter(1, Fsync, "a")]), Guarantee::Reboot);
        // Voters that do not name their machine may share one.
        assert_eq!(of(&[voter(1, Fsync, ""), voter(2, Fsync, ""), voter(3, Fsync, "c")]), Guarantee::Reboot);
    }

    #[test]
    fn the_floor_admits_only_voters_that_keep_it() {
        assert_eq!(Guarantee::requested(Fsync), Guarantee::Machine);
        assert_eq!(Guarantee::requested(Full), Guarantee::Machine);
        assert_eq!(Guarantee::requested(Off), Guarantee::Process);
        assert!(meets(Guarantee::Machine, &voter(1, Fsync, "a")));
        assert!(!meets(Guarantee::Machine, &voter(1, Off, "a")));
        assert!(!meets(Guarantee::Machine, &voter(1, Unknown, "a")));
        assert!(meets(Guarantee::Process, &voter(1, Off, "a")));
    }
}
