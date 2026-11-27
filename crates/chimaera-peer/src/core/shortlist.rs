//! Which other peers a call's placement weighs. Weighing every live peer for
//! every call costs too much, so the leader takes two at random (near least
//! loaded without a global view) and the peers holding the most of the call's
//! input bytes. Ranking holders by the bytes they hold across all inputs,
//! rather than taking the holders of the largest input alone, keeps a peer
//! that holds several medium inputs (more bytes in all) in the running.

use chimaera_core::pb;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Holders weighed besides the two random choices, most input bytes first.
const HOLDER_PICKS: usize = 4;

/// A call's inputs, each object once: a gather naming one large buffer at
/// several positions fetches it once, so placement weighs it once.
pub(super) fn distinct(mut inputs: Vec<pb::ObjectRef>) -> Vec<pb::ObjectRef> {
    let mut seen = HashSet::new();
    inputs.retain(|o| seen.insert((o.run.clone(), o.object_id.clone())));
    inputs
}

/// The peers holding any of `inputs` (`held[i]`: the holders of `inputs[i]`),
/// most bytes held first. Equal holders come in an order that changes with
/// `seed`, so peers holding the same bytes share the calls.
pub(super) fn by_bytes(inputs: &[pb::ObjectRef], held: &[Vec<u64>], seed: u64) -> Vec<u64> {
    let mut bytes: Vec<(u64, u64)> = Vec::new();
    for (o, holders) in inputs.iter().zip(held) {
        for &h in holders {
            match bytes.iter_mut().find(|(n, _)| *n == h) {
                Some((_, b)) => *b += o.size,
                None => bytes.push((h, o.size)),
            }
        }
    }
    bytes.sort_by_key(|&(n, b)| (std::cmp::Reverse(b), n ^ seed));
    bytes.into_iter().map(|(n, _)| n).collect()
}

/// The seed of a call's random choices: its id's hash bytes, different for
/// each call (its first bytes, cell and vshard, are the same for a run's
/// calls), mixed with the clock so a call placed again draws again. The
/// clock is no seed alone: on macOS it moves in whole microseconds.
pub(super) fn seed(task: &[u8; 16], now_ns: u64) -> usize {
    (u64::from_le_bytes(task[8..16].try_into().expect("8 bytes")) ^ now_ns) as usize
}

/// What moving `missing` of a call's `total` input bytes to a peer costs, in
/// the units of load (a core's queue): the missing share, up to one full
/// core's queue (a peer holding the input wins unless it is saturated), plus
/// the time to move the bytes at `bw` (bytes per second), in units of the
/// call's duration `est_s`. It grows with every byte that moves, so a fifth
/// of a large input on a slow link counts for what it costs; locality stays a
/// preference, never a pin, since load can always outweigh it.
pub(super) fn moving(missing: u64, total: u64, bw: f64, est_s: f64) -> f64 {
    if missing == 0 || total == 0 {
        return 0.0;
    }
    missing as f64 / total as f64 + missing as f64 / bw / est_s
}

/// Inputs at least this large are fetched by at most FANOUT_MAX peers at once.
pub(super) const FANOUT_MIN_BYTES: u64 = 64 << 20;
/// Peers placement may send calls to that lack one large input, before they
/// hold it: a burst of readers then pulls it from its holder a few peers at a
/// time, not with every peer at once (64 peers pulling 512 MiB through one
/// NIC took minutes and made the holder miss its liveness probes).
pub(super) const FANOUT_MAX: usize = 4;
/// How long a peer chosen for a call reading an input it lacks counts as
/// having it on the way (its fetch is done well before, unless it failed).
const FANOUT_FOR: Duration = Duration::from_secs(30);
/// What a call costs on a peer the input could not go to yet: it goes there
/// only if no peer where the input is, or is going, can run it.
pub(super) const FANOUT_WAIT: f64 = 1e3;

/// The peers large inputs are on their way to: chosen for calls reading
/// them, and not yet holding them.
#[derive(Default)]
pub(super) struct Fanout(HashMap<Vec<u8>, Vec<(u64, Instant)>>);

impl Fanout {
    /// Whether `object` is on its way to `node`.
    pub(super) fn going(&self, object: &[u8], node: u64, now: Instant) -> bool {
        self.0.get(object).is_some_and(|to| to.iter().any(|&(n, at)| n == node && now - at < FANOUT_FOR))
    }

    /// Whether `object` is on its way to as many peers as may fetch it at once.
    pub(super) fn full(&self, object: &[u8], now: Instant) -> bool {
        self.0.get(object).is_some_and(|to| to.iter().filter(|&&(_, at)| now - at < FANOUT_FOR).count() >= FANOUT_MAX)
    }

    /// A call reading `object` was placed on `node`, which lacks it.
    pub(super) fn sent(&mut self, object: &[u8], node: u64, now: Instant) {
        let to = self.0.entry(object.to_vec()).or_default();
        to.retain(|&(n, at)| n != node && now - at < FANOUT_FOR);
        to.push((node, now));
        if self.0.len() > 4096 {
            self.0.retain(|_, to| to.iter().any(|&(_, at)| now - at < FANOUT_FOR));
        }
    }

    /// The peers among those `object` is going to that hold it now.
    pub(super) fn arrived(&mut self, object: &[u8], holds: impl Fn(u64) -> bool) {
        if let Some(to) = self.0.get_mut(object) {
            to.retain(|&(n, _)| !holds(n));
            if to.is_empty() {
                self.0.remove(object);
            }
        }
    }
}

/// Indexes into `others` to weigh: two random choices from `seed`, then up to
/// HOLDER_PICKS of `ranked` (peers by input bytes held, see `by_bytes`).
pub(super) fn shortlist(others: &[u64], seed: usize, ranked: &[u64]) -> Vec<usize> {
    if others.is_empty() {
        return Vec::new();
    }
    let mut picks: Vec<usize> = (0..2.min(others.len())).map(|i| (seed + i * 7919) % others.len()).collect();
    picks.dedup();
    for i in ranked.iter().filter_map(|h| others.iter().position(|n| n == h)).take(HOLDER_PICKS) {
        if !picks.contains(&i) {
            picks.push(i);
        }
    }
    picks
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    #[test]
    fn the_random_choices_reach_every_peer_on_a_clock_of_whole_microseconds() {
        // One vshard's calls, placed a microsecond apart: seeded from the
        // id's first byte and the clock, the two choices among 15 peers fell
        // on 6 of them only, and a 16-peer cluster ran on 7.
        let others: Vec<u64> = (0..15).collect();
        let mut weighed = HashSet::new();
        for i in 0..300u64 {
            let task = chimaera_core::ids::Id::derive(0, 7, &[&i.to_le_bytes()]);
            weighed.extend(shortlist(&others, seed(&task.0, 1_764_000_000_000_000_000 + i * 1000), &[]));
        }
        assert_eq!(weighed.len(), others.len());
    }

    fn obj(id: u8, size: u64) -> pb::ObjectRef {
        pb::ObjectRef { run: vec![1], object_id: vec![id; 16], size, ..Default::default() }
    }

    #[test]
    fn a_peer_holding_several_medium_inputs_is_weighed_whatever_the_random_choices() {
        // This peer (A, node 1) alone holds the largest input, 9 GiB; B (node
        // 10, slot zero of the seven others) holds three 4 GiB inputs, 12 GiB
        // in all. Taking only the largest input's holders weighed B for two
        // of the seven seed residues; ranked by bytes, B is weighed for all.
        let (a, b) = (1, 10);
        let inputs = [obj(0, 9 * GIB), obj(1, 4 * GIB), obj(2, 4 * GIB), obj(3, 4 * GIB)];
        let held = [vec![a], vec![b], vec![b], vec![b]];
        let ranked = by_bytes(&inputs, &held, 0);
        assert_eq!(ranked, vec![b, a], "B holds 12 GiB, A 9 GiB");
        let others = [b, 11, 12, 13, 14, 15, 16];
        let largest_only: Vec<usize> =
            (0..7).filter(|&seed| shortlist(&others, seed, &[a]).iter().any(|&i| others[i] == b)).collect();
        assert_eq!(largest_only, vec![0, 5], "the old rule, for comparison");
        for seed in 0..7 {
            let picks = shortlist(&others, seed, &ranked);
            assert!(picks.iter().any(|&i| others[i] == b), "seed {seed}: {picks:?}");
        }
    }

    #[test]
    fn moving_a_fifth_of_a_large_input_counts_and_nothing_jumps_at_a_threshold() {
        let (total, slow) = (100 * GIB, 125e6); // 1 Gbit/s
        let fifth = moving(total / 5, total, slow, 60.0);
        assert!(fifth > 2.0, "20 GiB over a 1 Gbit/s link is minutes, against a 60 s call: {fifth}");
        assert_eq!(moving(0, total, slow, 60.0), 0.0);
        // No step where a threshold was: 29% and 31% cost about the same.
        let (below, above) = (moving(29 * GIB, total, slow, 60.0), moving(31 * GIB, total, slow, 60.0));
        assert!(above > below && above - below < 0.1 * above, "{below} {above}");
        // A small input on a fast link costs little more than its share.
        let small = moving(2 << 20, 4 << 20, 1e9, 1.0);
        assert!((0.5..0.51).contains(&small), "{small}");
    }

    #[test]
    fn a_large_input_goes_to_a_few_peers_at_once_and_to_more_as_they_get_it() {
        let (mut f, t0) = (Fanout::default(), Instant::now());
        let big = [9u8; 16];
        for node in 1..=FANOUT_MAX as u64 {
            assert!(!f.full(&big, t0));
            f.sent(&big, node, t0);
        }
        assert!(f.full(&big, t0), "{FANOUT_MAX} peers are fetching it");
        assert!(f.going(&big, 2, t0) && !f.going(&big, 99, t0));
        f.sent(&big, 2, t0); // another call there: still one fetch
        assert!(f.full(&big, t0));
        f.arrived(&big, |n| n == 1); // peer 1 holds it now: room for another
        assert!(!f.full(&big, t0) && !f.going(&big, 1, t0));
        assert!(!f.full(&big, t0 + FANOUT_FOR), "a fetch that never finished stops counting");
        assert!(!f.full(&[1u8; 16], t0), "each input apart");
    }

    #[test]
    fn holders_are_capped_and_each_peer_weighed_once() {
        let others: Vec<u64> = (100..120).collect();
        let ranked: Vec<u64> = (100..120).rev().collect();
        let picks = shortlist(&others, 3, &ranked);
        assert!(picks.len() <= 2 + HOLDER_PICKS, "{picks:?}");
        let mut unique = picks.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), picks.len(), "{picks:?}");
        assert_eq!(shortlist(&[7], 5, &[7]), vec![0], "one other peer: weighed once");
        assert!(shortlist(&[], 5, &[7]).is_empty());
    }

    #[test]
    fn one_buffer_named_six_times_is_one_input() {
        let one = obj(4, GIB);
        let inputs = distinct(vec![one.clone(), one.clone(), obj(5, 1), one.clone(), one.clone(), one.clone(), one]);
        assert_eq!(inputs.iter().map(|o| o.size).sum::<u64>(), GIB + 1);
        let other_run = pb::ObjectRef { run: vec![2], ..obj(4, GIB) };
        assert_eq!(distinct(vec![obj(4, GIB), other_run]).len(), 2, "same id in another run is another object");
    }

    #[test]
    fn peers_holding_equal_bytes_take_turns_at_the_top() {
        let inputs = [obj(0, GIB)];
        let held = [vec![20, 21, 22, 23, 24, 25]];
        let firsts: HashSet<u64> = (0..64).map(|seed| by_bytes(&inputs, &held, seed)[0]).collect();
        assert!(firsts.len() > 1, "{firsts:?}");
    }
}
