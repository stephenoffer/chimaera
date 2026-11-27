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
//!
//! Entries come in two classes. Most must survive whatever their group
//! promises, and every voter that syncs (`fsync`, `full`, `durable`) syncs
//! them before acknowledging. A `durable` voter (advertised as `FSYNC` with
//! `raft_sync_selective`) acknowledges an attached run's per-call entries
//! (`ShardCmd.sync_optional`) from its page cache, so those survive less
//! where one votes: [`qualify_calls`].

use crate::pb;
use std::collections::BTreeMap;

/// What an acknowledged commit survives, weakest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Guarantee {
    /// The acknowledging peers' processes dying: some voter keeps commits
    /// only in its machine's page cache.
    Process,
    /// Also the restart or loss of machines holding fewer than half of a
    /// group's voters, but not a majority of its voters restarting together
    /// (a correlated power loss): some voter acknowledges the entry from its
    /// page cache, and no machine holds a majority of the voters. Only an
    /// attached run's per-call entries get it ([`qualify_calls`]).
    ReplicatedMemory,
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
            Guarantee::ReplicatedMemory => "replicated-memory",
            Guarantee::Reboot => "reboot",
            Guarantee::Machine => "machine",
        }
    }

    /// What a peer writing its raft log this way asks of its groups for the
    /// entries that must survive: a synced voting majority on separate
    /// machines, unless it was told not to sync.
    pub fn requested(sync: pb::RaftSync) -> Guarantee {
        if syncs(sync) { Guarantee::Machine } else { Guarantee::Process }
    }

    /// What it asks for an attached run's per-call entries: a `selective`
    /// (`durable`) peer acknowledges them unsynced itself, so it asks no more
    /// than `ReplicatedMemory` of them.
    pub fn requested_calls(sync: pb::RaftSync, selective: bool) -> Guarantee {
        Guarantee::requested(sync).min(if selective { Guarantee::ReplicatedMemory } else { Guarantee::Machine })
    }
}

/// Whether a voter writing its log this way syncs the entries that must
/// survive. A `durable` peer advertises `FSYNC`, which holds for those.
fn syncs(sync: pb::RaftSync) -> bool {
    matches!(sync, pb::RaftSync::Fsync | pb::RaftSync::Full)
}

/// Whether a voter syncs every entry, an attached run's calls included.
fn syncs_calls(m: &pb::Member) -> bool {
    syncs(m.raft_sync()) && !m.raft_sync_selective
}

fn voter_name(id: u64, m: Option<&pb::Member>) -> String {
    m.map_or_else(|| format!("node {id}"), |m| m.addr.clone())
}

/// Whether `m` may take a voter seat in a group that must give `floor`. A
/// `durable` peer may: it syncs every entry that must survive, and this reads
/// only `raft_sync`, as a peer that predates `raft_sync_selective` does.
pub fn meets(floor: Guarantee, m: &pb::Member) -> bool {
    floor == Guarantee::Process || syncs(m.raft_sync())
}

/// The guarantee of a group with these voters (node id, and its member
/// record if this peer knows it) for the entries that must survive, and why
/// it is no stronger.
pub fn qualify(voters: &[(u64, Option<&pb::Member>)]) -> (Guarantee, Option<String>) {
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
        return (Guarantee::Process, Some(format!("voter {} {why}", voter_name(id, m))));
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

/// The guarantee of the same group for an attached run's per-call entries,
/// and why it is no stronger: what [`qualify`] gives where every voter syncs
/// every entry. A `durable` voter acknowledges these from its page cache, so
/// they survive the restart or loss of machines holding fewer than half of
/// the voters (`ReplicatedMemory`), or only the processes dying where one
/// machine holds a majority of the voters (its restart alone is a majority
/// restarting). Never more than `qualify` gives: a voter that syncs nothing
/// makes both `Process`.
pub fn qualify_calls(voters: &[(u64, Option<&pb::Member>)]) -> (Guarantee, Option<String>) {
    let (must, why) = qualify(voters);
    let Some(&(id, m)) = voters.iter().find(|(_, m)| !m.is_some_and(syncs_calls)).filter(|_| must > Guarantee::Process)
    else {
        return (must, why);
    };
    let acks = format!(
        "voter {} acknowledges an attached run's calls before syncing them (CHIMAERA_RAFT_SYNC=durable)",
        voter_name(id, m)
    );
    match (must, why) {
        (Guarantee::Machine, _) => (
            Guarantee::ReplicatedMemory,
            Some(format!("{acks}: a majority of the voters restarting together can lose the last of them")),
        ),
        (_, why) => (
            Guarantee::Process,
            Some(format!("{acks}, and {}: that machine restarting can lose the last of them", why.unwrap_or_default())),
        ),
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

    fn calls_of(ms: &[pb::Member]) -> Guarantee {
        let v: Vec<(u64, Option<&pb::Member>)> = ms.iter().map(|m| (m.node_id, Some(m))).collect();
        qualify_calls(&v).0
    }

    /// A `durable` peer, as it advertises itself: FSYNC, selective.
    fn durable(n: u64, host: &str) -> pb::Member {
        pb::Member { raft_sync_selective: true, ..voter(n, Fsync, host) }
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
        // A durable peer syncs what must survive: it takes a seat as an fsync one does.
        for floor in [Guarantee::Process, Guarantee::Machine] {
            assert_eq!(meets(floor, &durable(1, "a")), meets(floor, &voter(1, Fsync, "a")), "{floor:?}");
        }
        // Of an attached run's calls, it asks only what it gives them itself.
        assert_eq!(Guarantee::requested_calls(Fsync, true), Guarantee::ReplicatedMemory);
        assert_eq!(Guarantee::requested_calls(Fsync, false), Guarantee::Machine);
        assert_eq!(Guarantee::requested_calls(Full, false), Guarantee::Machine);
        assert_eq!(Guarantee::requested_calls(Off, false), Guarantee::Process);
        assert_eq!(Guarantee::requested_calls(Off, true), Guarantee::Process, "never more than the rest");
    }

    #[test]
    fn an_older_peer_reads_a_durable_member_as_one_that_syncs() {
        // A peer that predates raft_sync_selective decodes only the fields it
        // knows: it reads FSYNC, and seats the durable peer as a voter.
        #[derive(Clone, PartialEq, prost::Message)]
        struct OlderMember {
            #[prost(uint64, tag = "3")]
            node_id: u64,
            #[prost(enumeration = "pb::RaftSync", tag = "11")]
            raft_sync: i32,
        }
        use prost::Message;
        let old = OlderMember::decode(durable(4, "a").encode_to_vec().as_slice()).unwrap();
        assert_eq!((old.node_id, old.raft_sync()), (4, Fsync));
        let seen = pb::Member { node_id: old.node_id, raft_sync: old.raft_sync, ..Default::default() };
        assert!(meets(Guarantee::Machine, &seen));
        assert_eq!(Guarantee::requested(seen.raft_sync()), Guarantee::Machine);
    }

    #[test]
    fn durable_voters_sync_what_must_survive_and_give_calls_less() {
        assert_eq!(Guarantee::ReplicatedMemory.name(), "replicated-memory");
        assert!(Guarantee::Process < Guarantee::ReplicatedMemory && Guarantee::ReplicatedMemory < Guarantee::Reboot);
        // Every voter durable, on separate machines: must-survive entries keep
        // machine; an attached run's calls survive a minority restarting.
        let spread = [durable(1, "a"), durable(2, "b"), durable(3, "c")];
        assert_eq!(of(&spread), Guarantee::Machine);
        let v: Vec<(u64, Option<&pb::Member>)> = spread.iter().map(|m| (m.node_id, Some(m))).collect();
        let (calls, why) = qualify_calls(&v);
        assert_eq!(calls, Guarantee::ReplicatedMemory);
        let why = why.unwrap();
        assert!(why.contains("10.0.0.1:7700 acknowledges an attached run's calls before syncing them"), "{why}");
        assert!(why.contains("a majority of the voters restarting together"), "{why}");
        // One durable voter among synced ones is enough: it is in some majority.
        let mixed = [voter(1, Fsync, "a"), durable(2, "b"), voter(3, Full, "c")];
        assert_eq!((of(&mixed), calls_of(&mixed)), (Guarantee::Machine, Guarantee::ReplicatedMemory));
        // Every voter syncing every entry: calls get what everything else gets.
        assert_eq!(calls_of(&[voter(1, Fsync, "a"), voter(2, Full, "b"), voter(3, Fsync, "c")]), Guarantee::Machine);
        let shared = [voter(1, Fsync, "a"), voter(2, Fsync, "a"), voter(3, Fsync, "c")];
        assert_eq!(calls_of(&shared), Guarantee::Reboot);
        // A machine holding a majority of the voters, one durable: its restart
        // alone is a majority restarting, so calls survive only processes dying.
        let shared = [durable(1, "a"), voter(2, Fsync, "a"), voter(3, Fsync, "c")];
        assert_eq!(of(&shared), Guarantee::Reboot);
        let v: Vec<(u64, Option<&pb::Member>)> = shared.iter().map(|m| (m.node_id, Some(m))).collect();
        let (calls, why) = qualify_calls(&v);
        assert_eq!(calls, Guarantee::Process);
        assert!(why.unwrap().contains("2 of its 3 voters are on machine a"));
        assert_eq!(calls_of(&[durable(1, "a")]), Guarantee::Process, "one voter is a majority");
        assert_eq!(calls_of(&[durable(1, ""), durable(2, ""), durable(3, "c")]), Guarantee::Process);
        // Five on four machines: no machine holds three.
        let five = [durable(1, "a"), voter(2, Fsync, "a"), durable(3, "b"), durable(4, "c"), voter(5, Full, "d")];
        assert_eq!((of(&five), calls_of(&five)), (Guarantee::Machine, Guarantee::ReplicatedMemory));
        // A voter that syncs nothing, or does not say: calls never claim more than the rest.
        let off = [durable(1, "a"), durable(2, "b"), voter(3, Off, "c")];
        assert_eq!((of(&off), calls_of(&off)), (Guarantee::Process, Guarantee::Process));
        let unknown = [durable(1, "a"), durable(2, "b"), voter(3, Unknown, "c")];
        assert_eq!((of(&unknown), calls_of(&unknown)), (Guarantee::Process, Guarantee::Process));
        assert_eq!(qualify_calls(&[(1, Some(&durable(1, "a"))), (2, None), (3, None)]).0, Guarantee::Process);
        assert_eq!(qualify_calls(&[]).0, Guarantee::Process);
        // A selective flag without a sync says nothing more: still process.
        let odd = [pb::Member { raft_sync_selective: true, ..voter(1, Off, "a") }, durable(2, "b"), durable(3, "c")];
        assert_eq!((of(&odd), calls_of(&odd)), (Guarantee::Process, Guarantee::Process));
    }
}
