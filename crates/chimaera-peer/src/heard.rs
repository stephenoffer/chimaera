//! Bytes arriving from a peer prove it is alive.
//!
//! A peer sending a large value to many others at once fills its link: its
//! replies to SWIM probes queue behind the value's bytes and miss their
//! timeout, and without more evidence the members it is sending to would
//! suspect it, then retire it mid-transfer, and every call reading the value
//! would wait on a fetch from a peer the cluster had just declared dead. So
//! each chunk of an object that arrives from a peer is noted here, and a probe
//! of that peer (direct, or relayed for another member) that times out still
//! counts as an answer when the peer's bytes arrived while it waited. A peer
//! that has stopped sends nothing, so it is suspected as before.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// When bytes last arrived from each peer.
#[derive(Default)]
pub struct Heard(Mutex<HashMap<u64, Instant>>);

impl Heard {
    /// Bytes from `node` arrived just now.
    pub fn note(&self, node: u64) {
        self.0.lock().expect("lock").insert(node, Instant::now());
    }

    /// Whether bytes from `node` arrived at or after `since`.
    pub fn since(&self, node: u64, since: Instant) -> bool {
        self.0.lock().expect("lock").get(&node).is_some_and(|t| *t >= since)
    }
}

/// Debug builds only: `CHIMAERA_TEST_SLOW_PINGS_MS` holds each of this peer's
/// probe replies for that long while it is sending objects that calls read
/// (foreground bytes went out in the last 300 ms), as a link full of them
/// does, so a test can show that a peer whose bytes keep arriving is not
/// suspected. Release builds ignore it.
pub async fn slow_ping(traffic: &crate::traffic::Shaper) {
    #[cfg(debug_assertions)]
    {
        use std::sync::OnceLock;
        use std::time::Duration;
        static MS: OnceLock<u64> = OnceLock::new();
        // Foreground bytes sent as of the last reply, and when they last grew.
        static SENT: Mutex<(u64, Option<Instant>)> = Mutex::new((0, None));
        let ms = *MS.get_or_init(|| {
            std::env::var("CHIMAERA_TEST_SLOW_PINGS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0)
        });
        if ms == 0 {
            return;
        }
        let sent = traffic.status().first().map_or(0, |c| c.bytes);
        let sending = {
            let mut last = SENT.lock().expect("lock");
            if sent > last.0 {
                *last = (sent, Some(Instant::now()));
            }
            last.1.is_some_and(|t| t.elapsed() < Duration::from_millis(300))
        };
        if sending {
            static TOLD: std::sync::Once = std::sync::Once::new();
            TOLD.call_once(|| tracing::info!("holding probe replies {ms} ms while sending objects (test setting)"));
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
    #[cfg(not(debug_assertions))]
    let _ = traffic;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bytes_that_arrived_since_the_probe_began_count() {
        let heard = Heard::default();
        let before = Instant::now();
        assert!(!heard.since(7, before), "nothing heard from a peer yet");
        heard.note(7);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let asked = Instant::now();
        assert!(heard.since(7, before));
        assert!(!heard.since(7, asked), "bytes from before the probe began prove nothing about it");
        assert!(!heard.since(8, before), "another peer's bytes are not this one's");
        heard.note(7);
        assert!(heard.since(7, asked));
    }
}
