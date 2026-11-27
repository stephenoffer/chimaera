//! Bounds on running calls again to remake values lost with every peer that
//! held them (lineage). A lost value's call may itself read a lost value, so
//! a rebuild can climb a chain of calls; these keep that finite and say why
//! when it stops.

use std::collections::HashMap;

use chimaera_core::ids::Id;
use chimaera_core::pb;

/// Levels of calls run again above one reader: a chain of pure calls this
/// long recovers from its first surviving value.
pub(super) const MAX_DEPTH: u32 = 64;
/// Lost inputs one attempt may have made again beyond one per input it
/// reads: an input remade and then lost again.
const SLACK: u32 = 2;
/// Calls of one run made again on one peer before the next lost value
/// fails instead: a lost graph larger than this stops, naming that value.
pub(super) const RUN_BUDGET: u32 = 10_000;

/// Why an attempt `depth` levels above its reader, which has had `rebuilt`
/// lost inputs made again and reads `inputs` values that calls returned, may
/// not have another made again; None when it may.
pub(super) fn refusal(depth: u32, rebuilt: u32, inputs: usize) -> Option<String> {
    if depth >= MAX_DEPTH {
        return Some(format!("{MAX_DEPTH} levels of lost values above it are being made again already, the most"));
    }
    let most = inputs as u32 + SLACK;
    if rebuilt > most {
        return Some(format!("{rebuilt} of its lost inputs were made again already, more than the {inputs} it reads"));
    }
    None
}

/// Why `remade`, the value a call run again made, is not the value it
/// `published` (None: the same, as far as its shape tells). The shape is its
/// size and its buffers' ids and sizes, not its bytes: an equal value may
/// pickle in another order in another process (a set of strings, under hash
/// randomization), but a value of another size is another value, and its
/// bytes must never be read under the old ids.
pub(super) fn divergence(published: Option<&pb::ObjectRef>, remade: &pb::ObjectRef) -> Option<String> {
    /// Its id, size and out-of-band buffer count, and its buffers' ids and sizes.
    type Shape<'a> = (&'a [u8], u64, u32, Vec<(&'a [u8], u64)>);
    fn shape(o: &pb::ObjectRef) -> Shape<'_> {
        let buffers = o.blobs.iter().map(|b| (b.object_id.as_slice(), b.size)).collect();
        (o.object_id.as_slice(), o.size, o.oob_buffers, buffers)
    }
    let published = published?;
    if shape(published) == shape(remade) {
        return None;
    }
    let said = |o: &pb::ObjectRef| match o.blobs.len() {
        0 => format!("{} bytes", o.size),
        n => format!("{} bytes and {n} buffers of {}", o.size, o.blobs.iter().map(|b| b.size).sum::<u64>()),
    };
    Some(format!(
        "running its call again made another value ({}) than the one it published ({}): its function returns other \
         bytes for the same inputs; declare it deterministic=False to have a lost result reported instead",
        said(remade),
        said(published)
    ))
}

/// Calls run again per run on this peer (RUN_BUDGET).
#[derive(Default)]
pub(super) struct Budget(HashMap<Id, u32>);

impl Budget {
    /// Count one more call of `run` run again; the reason when its budget is spent.
    pub(super) fn spend(&mut self, run: Id) -> Option<String> {
        let spent = self.0.entry(run).or_default();
        if *spent >= RUN_BUDGET {
            return Some(format!(
                "its run has had {RUN_BUDGET} calls run again on this peer to remake lost values, the most"
            ));
        }
        *spent += 1;
        None
    }

    pub(super) fn end_run(&mut self, run: Id) {
        self.0.remove(&run);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chain_recovers_to_its_bound_and_a_gather_remakes_each_input() {
        assert_eq!(refusal(40, 1, 1), None, "40 levels deep");
        assert!(refusal(MAX_DEPTH, 0, 1).unwrap().contains("levels"));
        assert_eq!(refusal(0, 6, 6), None, "a gather of six lost inputs remakes all six");
        assert_eq!(refusal(0, 8, 6), None, "and two that were lost again");
        assert!(refusal(0, 9, 6).unwrap().contains("more than the 6 it reads"));
    }

    #[test]
    fn a_remade_value_of_another_shape_is_refused_and_one_of_the_same_shape_is_not() {
        let blob = |id: u8, size| pb::ObjectRef { object_id: vec![id; 16], size, ..Default::default() };
        let published = pb::ObjectRef {
            object_id: vec![1; 16],
            size: 120,
            oob_buffers: 1,
            blobs: vec![blob(2, 1 << 20)],
            inline_data: b"one order".to_vec(),
            ..Default::default()
        };
        let reordered = pb::ObjectRef { inline_data: b"other order".to_vec(), ..published.clone() };
        assert_eq!(divergence(Some(&published), &reordered), None, "same shape, bytes may differ");
        assert_eq!(divergence(None, &reordered), None, "nothing published to compare with");
        let longer = pb::ObjectRef { size: 121, ..published.clone() };
        let why = divergence(Some(&published), &longer).expect("another size");
        assert!(why.contains("121 bytes and 1 buffers") && why.contains("(120 bytes"), "{why}");
        let bigger = pb::ObjectRef { blobs: vec![blob(2, 2 << 20)], ..published.clone() };
        assert!(divergence(Some(&published), &bigger).is_some(), "a buffer of another size");
    }

    #[test]
    fn a_runs_budget_is_its_own_and_ends_with_it() {
        let (a, b) = (Id::derive(0, 1, &[b"a"]), Id::derive(0, 1, &[b"b"]));
        let mut budget = Budget::default();
        for _ in 0..RUN_BUDGET {
            assert_eq!(budget.spend(a), None);
        }
        assert!(budget.spend(a).unwrap().contains("10000 calls"));
        assert_eq!(budget.spend(b), None, "another run's budget is its own");
        budget.end_run(a);
        assert_eq!(budget.spend(a), None, "a new start once the run ended");
    }
}
