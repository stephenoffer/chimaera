//! Fresh loads for a peer that is placing calls.
//!
//! A peer learns the others' loads from SWIM ping replies, PROBE_FANOUT
//! members per 100 ms period, so a given peer's load is n/8 periods old: 800
//! ms at 64 peers. Group leaders placing one burst from views that old pile
//! its calls onto the peers whose last reports looked emptiest, each leader
//! blind to the others' grants (16 local peers, 3 leaders: 2 to 26 calls per
//! peer). While this peer places calls, it asks every live member for its
//! load each period instead; a peer that places nothing asks nobody.

use crate::mesh::MeshClient;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const EVERY: Duration = Duration::from_millis(100);
/// How long after its last placement this peer keeps asking.
const AFTER_MS: u64 = 1000;
const TIMEOUT: Duration = Duration::from_millis(300);

/// When this peer last placed a call (mesh time, plus one: 0 is never).
static PLACED_AT: AtomicU64 = AtomicU64::new(0);

/// This peer is placing calls (core::choose_peer with other peers to weigh).
pub fn placing(mesh: &MeshClient) {
    PLACED_AT.store(mesh.now_ms() + 1, Ordering::Relaxed);
}

fn recently(placed_at: u64, now_ms: u64) -> bool {
    placed_at != 0 && now_ms + 1 - placed_at <= AFTER_MS
}

/// Every period while placing: each live member's load, as `note_ping`
/// records a ping reply (the SWIM probes go on as before).
pub async fn poll(mesh: Arc<MeshClient>) {
    let mut tick = tokio::time::interval(EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if !recently(PLACED_AT.load(Ordering::Relaxed), mesh.now_ms()) {
            continue;
        }
        let members = mesh.live_other_ids(|_| true);
        let asked = members.len();
        let asks = members.into_iter().map(|node| {
            let mesh = mesh.clone();
            async move {
                if let Ok(reply) = mesh.ping(node, TIMEOUT).await
                    && reply.node_id == node
                {
                    mesh.note_ping(node, Some(reply));
                }
            }
        });
        futures_util::future::join_all(asks).await;
        tracing::debug!(asked, "loads polled");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_asks_while_it_places_and_for_a_second_after() {
        assert!(!recently(0, 5), "never placed");
        assert!(recently(10 + 1, 10));
        assert!(recently(10 + 1, 10 + AFTER_MS));
        assert!(!recently(10 + 1, 10 + AFTER_MS + 1));
    }
}
