//! Which other peers a call's placement weighs. Weighing every live peer for
//! every call costs too much, so the leader takes two at random (near least
//! loaded without a global view) and the peers holding the most of the call's
//! input bytes. Ranking holders by the bytes they hold across all inputs,
//! rather than taking the holders of the largest input alone, keeps a peer
//! that holds several medium inputs (more bytes in all) in the running.

use chimaera_core::pb;
use std::collections::HashSet;

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
