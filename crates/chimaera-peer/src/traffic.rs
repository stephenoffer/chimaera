//! Weighted flow control for the bytes this peer sends other peers in bulk:
//! the objects it serves (the Fetch RPC), Raft snapshots, and code copies.
//!
//! Every such send carries its class (TrafficClass): a call or a driver
//! reading a value now (foreground), a shuffle's gather reading its parts, a
//! copy restoring what a loss took or is about to take (recovery: a drain's
//! hand-off, a snapshot for a Raft replica that fell behind), or a copy made
//! ahead of a loss (checkpoint: background replicas, retained puts, code).
//!
//! With `CHIMAERA_MESH_RATE` set (bytes per second, a little under what the
//! link carries), the peer sends no faster than that, and while several
//! classes want more than it, they share it by weight, in start-time fair
//! queueing: foreground 8, shuffle 4, recovery 2, checkpoint 1. Each class is
//! promised its weight's share of the rate while all four want it; a class
//! alone may use all of it, and none waits for ever. Paced sends go in pieces
//! of about SLICE at the rate, so a foreground read waits at most about one
//! piece for a turn.
//!
//! Raft messages, and the calls, results and output this peer sends other
//! peers, are never held: their bytes count against the rate (paced sends
//! make room for them). Raft's travel on connections of their own (mesh.rs,
//! channel_lane). With bulk bytes paced under the link's capacity its queues
//! stay short, and Raft heartbeats go round within HEARTBEAT_BUDGET.
//!
//! Without `CHIMAERA_MESH_RATE` nothing is paced; sends are only counted.

use chimaera_core::pb;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

pub const ENV: &str = "CHIMAERA_MESH_RATE";

/// The classes in TrafficClass order: (name, weight).
const CLASSES: [(&str, u64); 4] = [("foreground", 8), ("shuffle", 4), ("recovery", 2), ("checkpoint", 1)];
const MAX_WEIGHT: u64 = 8;
/// A send may go this long before the rate has drained the one before it,
/// so the link does not idle between turns.
const SLACK: Duration = Duration::from_millis(2);
/// A paced stream's pieces carry about this long of the rate.
const SLICE: Duration = Duration::from_millis(20);
const MIN_PIECE: usize = 64 << 10;
/// What a Raft heartbeat's round trip should take while bulk bytes are
/// paced: a third of the heartbeat interval (chimaera-raft), far under an
/// election timeout.
pub const HEARTBEAT_BUDGET: Duration = Duration::from_millis(100);
/// Heartbeat round trips are reported over this long, and at most this many.
const RTT_WINDOW: Duration = Duration::from_secs(10);
const RTT_SAMPLES: usize = 4096;

pub struct Shaper {
    /// Bytes per second; 0: unpaced.
    rate: f64,
    s: Mutex<State>,
    turn: Notify,
}

#[derive(Default)]
struct State {
    /// The start tag of the send that went last, each class's last finish
    /// tag, and the sends waiting for their turn by (start tag, arrival).
    vtime: u64,
    finish: [u64; 4],
    waiting: BTreeSet<(u64, u64)>,
    arrivals: u64,
    /// When the rate will have drained everything sent so far.
    free_at: Option<Instant>,
    sent: [u64; 4],
    waited: [Duration; 4],
}

/// `CHIMAERA_MESH_RATE`: bytes per second (`125000000`, `120MB`, `1GiB`) or
/// bits (`1gbit`, `800mbit`).
pub fn parse_rate(text: &str) -> Option<f64> {
    let t = text.trim().to_ascii_lowercase();
    match t.strip_suffix("bit") {
        Some(bits) => crate::budget::parse_size(&format!("{bits}b")).map(|b| b as f64 / 8.0),
        None => crate::budget::parse_size(&t).map(|b| b as f64),
    }
}

impl Shaper {
    pub fn new(rate: f64) -> Self {
        Shaper { rate: rate.max(0.0), s: Mutex::default(), turn: Notify::new() }
    }

    pub fn from_env() -> Self {
        let rate = std::env::var(ENV).ok().map(|v| {
            parse_rate(&v).unwrap_or_else(|| {
                tracing::warn!("{ENV}={v} is not a rate (bytes per second, or e.g. 800mbit): sends are not paced");
                0.0
            })
        });
        Shaper::new(rate.unwrap_or(0.0))
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// The bytes per second `class` is promised while every class wants
    /// more (its weight's share of the rate); None when unpaced.
    pub fn promised(&self, class: pb::TrafficClass) -> Option<f64> {
        let all: u64 = CLASSES.iter().map(|c| c.1).sum();
        (self.rate > 0.0).then(|| self.rate * CLASSES[class as usize].1 as f64 / all as f64)
    }

    /// Bytes per piece of a paced stream (about SLICE at the rate), else `most`.
    pub fn piece(&self, most: usize) -> usize {
        if self.rate <= 0.0 { most } else { ((self.rate * SLICE.as_secs_f64()) as usize).clamp(MIN_PIECE, most) }
    }

    /// Wait for the turn to send `bytes` of `class`.
    pub async fn send(&self, class: pb::TrafficClass, bytes: usize) {
        let c = class as usize;
        let asked = Instant::now();
        let mut turn = {
            let mut s = self.s.lock().expect("traffic");
            if self.rate <= 0.0 {
                s.sent[c] += bytes as u64;
                return;
            }
            let start = s.vtime.max(s.finish[c]);
            s.finish[c] = start + bytes as u64 * (MAX_WEIGHT / CLASSES[c].1);
            s.arrivals += 1;
            let key = (start, s.arrivals);
            s.waiting.insert(key);
            Turn { shaper: self, key: Some(key) }
        };
        loop {
            let notified = self.turn.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let due = {
                let mut s = self.s.lock().expect("traffic");
                let key = turn.key.expect("still waiting");
                if s.waiting.first() != Some(&key) {
                    None // another send's turn first
                } else {
                    let now = Instant::now();
                    let free = s.free_at.unwrap_or(now).max(now);
                    if free <= now + SLACK {
                        s.waiting.pop_first();
                        s.vtime = key.0;
                        s.free_at = Some(free + Duration::from_secs_f64(bytes as f64 / self.rate));
                        s.sent[c] += bytes as u64;
                        s.waited[c] += now - asked;
                        turn.key = None;
                        drop(s);
                        self.turn.notify_waiters();
                        return;
                    }
                    Some(free - SLACK)
                }
            };
            match due {
                Some(at) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(at) => {}
                        _ = &mut notified => {}
                    }
                }
                None => notified.await,
            }
        }
    }

    /// Bytes sent without waiting (Raft): paced sends leave room for them.
    pub fn charge(&self, bytes: usize) {
        if self.rate > 0.0 {
            let now = Instant::now();
            let mut s = self.s.lock().expect("traffic");
            let free = s.free_at.unwrap_or(now).max(now);
            s.free_at = Some(free + Duration::from_secs_f64(bytes as f64 / self.rate));
        }
    }

    pub fn status(&self) -> Vec<pb::TrafficStatus> {
        let s = self.s.lock().expect("traffic");
        CLASSES
            .iter()
            .enumerate()
            .map(|(c, (name, weight))| pb::TrafficStatus {
                name: (*name).into(),
                weight: *weight as u32,
                bytes: s.sent[c],
                waited_s: s.waited[c].as_secs_f64(),
            })
            .collect()
    }
}

/// A send waiting for its turn; dropped before it (its stream went away),
/// it leaves the queue so the sends behind it go on.
struct Turn<'a> {
    shaper: &'a Shaper,
    key: Option<(u64, u64)>,
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            self.shaper.s.lock().expect("traffic").waiting.remove(&key);
            self.shaper.turn.notify_waiters();
        }
    }
}

/// Round trips of the Raft heartbeats this peer sent in the last RTT_WINDOW,
/// in microseconds.
#[derive(Default)]
pub struct Rtts(Mutex<VecDeque<(Instant, f64)>>);

impl Rtts {
    pub fn note(&self, took: Duration) {
        let mut v = self.0.lock().expect("rtts");
        v.push_back((Instant::now(), took.as_secs_f64() * 1e6));
        if v.len() > RTT_SAMPLES {
            v.pop_front();
        }
    }

    pub fn samples(&self) -> VecDeque<f64> {
        let since = Instant::now().checked_sub(RTT_WINDOW);
        let v = self.0.lock().expect("rtts");
        v.iter().filter(|(at, _)| since.is_none_or(|s| *at >= s)).map(|(_, us)| *us).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::TrafficClass as T;
    use std::sync::Arc;

    /// Senders of each class (count each) sending `piece`-byte pieces for
    /// `secs`; the bytes each class sent.
    async fn contend(shaper: Arc<Shaper>, senders: &[(T, usize)], piece: usize, secs: u64) -> [u64; 4] {
        let mut tasks = Vec::new();
        for &(class, n) in senders {
            for _ in 0..n {
                let shaper = shaper.clone();
                tasks.push(tokio::spawn(async move {
                    loop {
                        shaper.send(class, piece).await;
                    }
                }));
            }
        }
        tokio::time::sleep(Duration::from_secs(secs)).await;
        tasks.iter().for_each(|t| t.abort());
        let s = shaper.s.lock().unwrap();
        s.sent
    }

    #[tokio::test(start_paused = true)]
    async fn contending_classes_share_the_rate_by_weight() {
        let shaper = Arc::new(Shaper::new(10e6));
        // A checkpoint storm (eight streams) against one foreground reader
        // and one shuffle gather: 8 : 4 : 1 of the rate, whatever the streams.
        let sent = contend(
            shaper.clone(),
            &[(T::TrafficCheckpoint, 8), (T::TrafficForeground, 1), (T::TrafficShuffle, 1)],
            64 << 10,
            20,
        )
        .await;
        let total: u64 = sent.iter().sum();
        assert!((total as f64 - 200e6).abs() < 2e6, "paced at the rate: {total}");
        let share = |c: T| sent[c as usize] as f64 / total as f64;
        assert!((share(T::TrafficForeground) - 8.0 / 13.0).abs() < 0.01, "{sent:?}");
        assert!((share(T::TrafficShuffle) - 4.0 / 13.0).abs() < 0.01, "{sent:?}");
        assert!((share(T::TrafficCheckpoint) - 1.0 / 13.0).abs() < 0.01, "{sent:?}");
        assert_eq!(sent[T::TrafficRecovery as usize], 0);
        // Each class got at least what it is promised with all four contending.
        for c in [T::TrafficForeground, T::TrafficShuffle, T::TrafficCheckpoint] {
            assert!(sent[c as usize] as f64 / 20.0 >= shaper.promised(c).unwrap(), "{c:?}: {sent:?}");
        }
        assert_eq!(shaper.promised(T::TrafficCheckpoint), Some(10e6 / 15.0));
        assert_eq!(Shaper::new(0.0).promised(T::TrafficForeground), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_class_alone_takes_the_whole_rate() {
        let shaper = Arc::new(Shaper::new(10e6));
        let sent = contend(shaper, &[(T::TrafficCheckpoint, 3)], 64 << 10, 10).await;
        assert!((sent[T::TrafficCheckpoint as usize] as f64 - 100e6).abs() < 1e6, "{sent:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_foreground_read_waits_about_one_piece_behind_a_storm() {
        let shaper = Arc::new(Shaper::new(10e6));
        let piece = shaper.piece(4 << 20);
        assert_eq!(piece, 200_000, "{SLICE:?} at 10 MB/s");
        for _ in 0..16 {
            let shaper = shaper.clone();
            tokio::spawn(async move {
                loop {
                    shaper.send(T::TrafficCheckpoint, piece).await;
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let t0 = Instant::now();
        shaper.send(T::TrafficForeground, piece).await;
        assert!(t0.elapsed() <= SLICE + SLACK, "waited {:?}", t0.elapsed());
        // 4 MB read in pieces while the storm goes on: 8/9 of the rate.
        let t0 = Instant::now();
        for _ in 0..20 {
            shaper.send(T::TrafficForeground, piece).await;
        }
        let took = t0.elapsed().as_secs_f64();
        assert!(took < 4e6 / (10e6 * 8.0 / 9.0) * 1.1, "took {took} s");
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_given_up_while_it_waits_does_not_hold_up_the_rest() {
        let shaper = Shaper::new(1e6);
        shaper.send(T::TrafficCheckpoint, 1_000_000).await; // the link is busy for 1 s
        // A foreground piece first in line, then given up (its reader went away).
        let gone = tokio::time::timeout(Duration::from_millis(100), shaper.send(T::TrafficForeground, 1_000)).await;
        assert!(gone.is_err());
        assert!(shaper.s.lock().unwrap().waiting.is_empty());
        let next = tokio::time::timeout(Duration::from_secs(2), shaper.send(T::TrafficCheckpoint, 1_000)).await;
        assert!(next.is_ok(), "the checkpoint piece behind it went once the link was free");
    }

    #[tokio::test(start_paused = true)]
    async fn raft_bytes_are_never_held_and_paced_sends_make_room_for_them() {
        let shaper = Shaper::new(1e6);
        shaper.charge(500_000);
        let t0 = Instant::now();
        shaper.send(T::TrafficForeground, 1_000).await;
        assert!(t0.elapsed() >= Duration::from_millis(490), "{:?}", t0.elapsed());
        // Unpaced: counted, never waited for.
        let free = Shaper::new(0.0);
        free.charge(1 << 30);
        let t0 = Instant::now();
        free.send(T::TrafficCheckpoint, 1 << 30).await;
        assert_eq!(t0.elapsed(), Duration::ZERO);
        assert_eq!(free.status()[3].bytes, 1 << 30);
        assert_eq!(free.piece(4 << 20), 4 << 20);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_round_trips_are_reported_over_the_last_window() {
        let rtts = Rtts::default();
        rtts.note(Duration::from_millis(400));
        tokio::time::advance(RTT_WINDOW + Duration::from_secs(1)).await;
        rtts.note(Duration::from_millis(3));
        assert_eq!(rtts.samples(), [3000.0], "the slow one went before the window");
    }

    #[test]
    fn rates_read_as_bytes_or_bits_per_second() {
        assert_eq!(parse_rate("125000000"), Some(125e6));
        assert_eq!(parse_rate("120MB"), Some(120e6));
        assert_eq!(parse_rate("1gbit"), Some(125e6));
        assert_eq!(parse_rate(" 800Mbit "), Some(100e6));
        assert_eq!(parse_rate("fast"), None);
    }
}
