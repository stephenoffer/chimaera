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

pub static FLEET_SUMMARIES_SERVED: Counter = Counter::new(
    "fleet_summaries_served_total",
    "Capacity summaries this peer answered for other cells' publishers (Status asked with fleet).",
);
pub static STORE_REQUESTS: Counter = Counter::new(
    "store_requests_total",
    "Store requests this peer's client endpoint served (ch.Store, and other cells' directory reads and renewals).",
);
pub static FLEET_DIRECTORY_LISTINGS: Counter = Counter::new(
    "fleet_directory_listings_total",
    "Whole listings of the fleet directory this peer read as its cell's publisher (else it reads only changes).",
);
pub static SPILLED_BYTES: Counter = Counter::new(
    "spilled_bytes_total",
    "Bytes of objects this peer's store moved out of memory, to disk or object storage.",
);
pub static SPILL_MICROSECONDS: Counter =
    Counter::new("spill_microseconds_total", "Time this peer's store spent moving objects out of memory.");

pub static OBJECT_BYTES_FETCHED: Counter =
    Counter::new("object_bytes_fetched_total", "Bytes of objects this peer fetched from other peers over the network.");
pub static OBJECT_BYTES_LINKED: Counter = Counter::new(
    "object_bytes_linked_total",
    "Bytes of objects this peer took from another peer's store on the same machine (a hard link, or a local copy).",
);

pub static ALL: &[&Counter] = &[
    &PROPOSALS,
    &PROPOSAL_FAILURES,
    &PEERS_DECLARED_DEAD,
    &WORKER_DEATHS,
    &OOM_KILLS,
    &CANCEL_KILLS,
    &REPLICATION_FAILURES,
    &FLEET_SUMMARIES_SERVED,
    &STORE_REQUESTS,
    &FLEET_DIRECTORY_LISTINGS,
    &SPILLED_BYTES,
    &SPILL_MICROSECONDS,
    &OBJECT_BYTES_FETCHED,
    &OBJECT_BYTES_LINKED,
];
