//! Events counted since the peer started, for `/metrics` (prom.rs). Process
//! wide: a test that runs several peers in one process sees their sum.

use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counter {
    pub name: &'static str,
    pub help: &'static str,
    n: AtomicU64,
}

impl Counter {
    const fn new(name: &'static str, help: &'static str) -> Counter {
        Counter { name, help, n: AtomicU64::new(0) }
    }

    pub fn add(&self, k: u64) {
        self.n.fetch_add(k, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.n.load(Ordering::Relaxed)
    }
}

pub static PROPOSALS: Counter = Counter::new("proposals_total", "Shard commands this peer proposed.");
pub static PROPOSAL_FAILURES: Counter =
    Counter::new("proposal_failures_total", "Proposals that no leader accepted in time (the group may lack a quorum).");
pub static PEERS_DECLARED_DEAD: Counter =
    Counter::new("peers_declared_dead_total", "Other peers this one declared dead and retired.");
pub static WORKER_DEATHS: Counter =
    Counter::new("worker_deaths_total", "Worker processes that exited while running a call or hosting an actor.");
pub static OOM_KILLS: Counter = Counter::new("oom_kills_total", "Workers killed for exceeding their memory.");
pub static CANCEL_KILLS: Counter =
    Counter::new("cancel_kills_total", "Workers killed because a cancelled call kept running.");

pub static REPLICATION_FAILURES: Counter = Counter::new(
    "replication_failures_total",
    "Objects this peer wrote whose replica on the next peer could not be made.",
);

pub static ALL: &[&Counter] = &[
    &PROPOSALS,
    &PROPOSAL_FAILURES,
    &PEERS_DECLARED_DEAD,
    &WORKER_DEATHS,
    &OOM_KILLS,
    &CANCEL_KILLS,
    &REPLICATION_FAILURES,
];
