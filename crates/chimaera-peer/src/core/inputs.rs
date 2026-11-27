//! How an attempt's stored inputs reached its peer (AttemptRecord.inputs), for
//! explain(): already here, linked from a peer's store on this machine,
//! fetched over the network, or made again because every copy was lost.

use crate::mesh::{Arrival, MeshClient};
use chimaera_core::pb;
use chimaera_core::shard::Tk;
use std::collections::HashMap;
use std::time::Duration;

/// Input reads an attempt record keeps, the largest first; its
/// input_bytes_* measures count them all.
const INPUT_READS: usize = 16;

/// An input that was in this peer's store when the attempt arrived.
pub(super) fn here(o: &pb::ObjectRef) -> pb::InputRead {
    pb::InputRead { object_id: o.object_id.clone(), bytes: o.size, how: "here".into(), ..Default::default() }
}

/// An input `localize` made present (`got`), in `took`.
pub(super) fn arrived(mesh: &MeshClient, o: &pb::ObjectRef, got: Arrival, took: Duration) -> pb::InputRead {
    let (how, bytes) = match got {
        Arrival::Here => return here(o), // another fetch brought it meanwhile
        Arrival::Linked { bytes, .. } => ("linked", bytes),
        Arrival::Fetched { bytes, .. } => ("fetched", bytes),
    };
    let source = got.node().and_then(|n| mesh.member(n)).map(|p| p.member.addr).unwrap_or_default();
    pb::InputRead {
        object_id: o.object_id.clone(),
        bytes,
        how: how.into(),
        source,
        seconds: took.as_secs_f64(),
        detail: String::new(),
    }
}

/// An input lost with every peer that held it, made again by running
/// `producer` (the lineage action explain() reports).
pub(super) fn rebuilt(object: &[u8], producer: Tk) -> pb::InputRead {
    pb::InputRead {
        object_id: object.to_vec(),
        how: "rebuilt".into(),
        detail: format!("lost with every peer that held it: its call {} ran again", producer.task.hex()),
        ..Default::default()
    }
}

/// What the attempt record keeps of `reads`: the largest INPUT_READS, with
/// every read counted in `measures` (input_bytes_here, _linked, _fetched).
pub(super) fn summarize(reads: &[pb::InputRead], measures: &mut HashMap<String, pb::Measure>) -> Vec<pb::InputRead> {
    for how in ["here", "linked", "fetched"] {
        let bytes: u64 = reads.iter().filter(|r| r.how == how).map(|r| r.bytes).sum();
        if bytes > 0 {
            let m =
                pb::Measure { value: bytes as f64, scope: "attempt".into(), method: "counted".into(), coverage: 1.0 };
            measures.insert(format!("input_bytes_{how}"), m);
        }
    }
    let mut kept = reads.to_vec();
    kept.sort_by_key(|r| std::cmp::Reverse(r.bytes));
    kept.truncate(INPUT_READS);
    kept
}

/// Where the attempt's inputs stood when it ran here, in words.
pub(super) fn placement(reads: &[pb::InputRead]) -> String {
    let total: u64 = reads.iter().map(|r| r.bytes).sum();
    if reads.is_empty() {
        return "ran on this peer; it reads no stored inputs".into();
    }
    let here: u64 = reads.iter().filter(|r| r.how == "here").map(|r| r.bytes).sum();
    let mut said = format!("ran on this peer with {} of its {} of stored inputs already here", mib(here), mib(total));
    for how in ["linked", "fetched"] {
        let moved: Vec<&pb::InputRead> = reads.iter().filter(|r| r.how == how).collect();
        if !moved.is_empty() {
            let bytes: u64 = moved.iter().map(|r| r.bytes).sum();
            let wher = if how == "linked" { "from a peer on this machine" } else { "over the network" };
            said += &format!("; {} {how} {wher}", mib(bytes));
        }
    }
    if reads.iter().any(|r| r.how == "rebuilt") {
        said += "; an input was made again (see inputs)";
    }
    said
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1 << 20) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(how: &str, bytes: u64) -> pb::InputRead {
        pb::InputRead { how: how.into(), bytes, ..Default::default() }
    }

    #[test]
    fn every_read_is_counted_and_the_largest_are_kept() {
        let mut reads: Vec<pb::InputRead> = (0..40).map(|i| read("fetched", i)).collect();
        reads.push(read("here", 1000));
        reads.push(read("linked", 7));
        let mut m = HashMap::new();
        let kept = summarize(&reads, &mut m);
        assert_eq!(kept.len(), INPUT_READS);
        assert_eq!(kept[0].bytes, 1000, "largest first");
        assert_eq!(m["input_bytes_fetched"].value, (0..40).sum::<u64>() as f64, "all 40 counted, not only those kept");
        assert_eq!(m["input_bytes_here"].value, 1000.0);
        assert_eq!(m["input_bytes_linked"].value, 7.0);
    }

    #[test]
    fn the_placement_says_what_was_here_and_what_moved() {
        assert_eq!(placement(&[]), "ran on this peer; it reads no stored inputs");
        let said = placement(&[read("here", 1 << 20), read("fetched", 3 << 20)]);
        assert_eq!(
            said,
            "ran on this peer with 1.0 MiB of its 4.0 MiB of stored inputs already here; 3.0 MiB fetched over the network"
        );
        assert!(placement(&[read("linked", 1 << 20)]).contains("1.0 MiB linked from a peer on this machine"));
    }
}
