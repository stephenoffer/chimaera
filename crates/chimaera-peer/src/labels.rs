//! Peer labels for placement: `Placement(require={...}, prefer=[...])`.
//!
//! A peer's labels are `$CHIMAERA_LABELS` (`zone=us-east1-b,rack=r7`) plus
//! `node` (its peer id, hex), `host`, `os` and `arch`. `require` keeps the peers
//! whose labels match every pair; each `prefer` entry `key=value` a peer
//! matches makes it cheaper to choose.

use std::collections::{BTreeMap, HashMap};

use chimaera_core::pb;

/// This peer's labels.
pub fn own(peer: &[u8]) -> BTreeMap<String, String> {
    let mut labels = parse(&std::env::var("CHIMAERA_LABELS").unwrap_or_default());
    let host = std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    labels.insert("node".into(), peer.iter().map(|b| format!("{b:02x}")).collect());
    labels.insert("host".into(), host);
    labels.insert("os".into(), std::env::consts::OS.into());
    labels.insert("arch".into(), std::env::consts::ARCH.into());
    labels
}

/// `k=v,k2=v2`; entries without `=` are ignored.
pub fn parse(text: &str) -> BTreeMap<String, String> {
    text.split(',')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .filter(|(k, _)| !k.is_empty())
        .collect()
}

/// True when `labels` has every `require` pair.
pub fn satisfies(labels: &HashMap<String, String>, require: &HashMap<String, String>) -> bool {
    require.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// How many `prefer` entries (`key=value`) `labels` matches.
pub fn preferred(labels: &HashMap<String, String>, prefer: &[String]) -> usize {
    prefer.iter().filter_map(|p| p.split_once('=')).filter(|(k, v)| labels.get(*k).is_some_and(|x| x == v)).count()
}

/// Failure domains from the widest down: the `zone` and `rack` labels, then
/// the machine (`Member.host`).
pub const FAILURE_DOMAINS: [&str; 3] = ["zone", "rack", "machine"];

/// A member's domain at `level` (see FAILURE_DOMAINS); None when unknown.
fn domain<'a>(m: &'a pb::Member, level: &str) -> Option<&'a str> {
    let v = if level == "machine" { Some(m.host.as_str()) } else { m.labels.get(level).map(String::as_str) };
    v.filter(|v| !v.is_empty())
}

/// Up to `n` of `ranked` (best first) spread over failure domains: at each of
/// `levels` in turn, widest first, the members whose domain there differs
/// from those of the members already taken and of `avoid` (whose loss the
/// picks are to survive) are taken in rank order; the rest follow in rank
/// order. A domain is named with the levels above it (`rack` r1 of zone a is
/// not r1 of zone b); a member that does not say its domain at a level is not
/// taken at that level. The order does not depend on `n`, so a smaller `n`
/// takes a prefix.
pub fn spread(ranked: &[&pb::Member], n: usize, levels: &[&str], avoid: Option<&pb::Member>) -> Vec<u64> {
    let mut picked: Vec<&pb::Member> = Vec::new();
    for depth in 1..=levels.len() {
        let key = |m: &pb::Member| -> Option<Vec<String>> {
            domain(m, levels[depth - 1])?;
            Some(levels[..depth].iter().map(|l| domain(m, l).unwrap_or_default().to_string()).collect())
        };
        let mut taken: Vec<Vec<String>> = picked.iter().copied().chain(avoid).filter_map(key).collect();
        for m in ranked {
            if picked.len() >= n {
                break;
            }
            if let Some(k) = key(m).filter(|k| !taken.contains(k))
                && !picked.iter().any(|p| p.node_id == m.node_id)
            {
                taken.push(k);
                picked.push(m);
            }
        }
    }
    for m in ranked {
        if picked.len() >= n {
            break;
        }
        if !picked.iter().any(|p| p.node_id == m.node_id) {
            picked.push(m);
        }
    }
    picked.iter().map(|m| m.node_id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn require_needs_every_pair_and_prefer_counts_matches() {
        let labels = map(&[("zone", "a"), ("rack", "r1")]);
        assert!(satisfies(&labels, &map(&[("zone", "a")])));
        assert!(!satisfies(&labels, &map(&[("zone", "a"), ("gpu", "h100")])));
        assert!(satisfies(&labels, &HashMap::new()));
        assert_eq!(preferred(&labels, &["zone=a".into(), "rack=r2".into(), "nonsense".into()]), 1);
        assert_eq!(
            parse(" zone = a ,bad, rack=r1"),
            [("rack".into(), "r1".into()), ("zone".into(), "a".into())].into()
        );
    }

    #[test]
    fn own_labels_include_the_node_and_platform() {
        let l = own(&[0xab, 0x01]);
        assert_eq!(l["node"], "ab01");
        assert_eq!(l["os"], std::env::consts::OS);
    }

    fn member(node_id: u64, labels: &[(&str, &str)], host: &str) -> pb::Member {
        let labels = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        pb::Member { node_id, labels, host: host.into(), ..Default::default() }
    }

    #[test]
    fn spreading_takes_other_racks_than_the_writers_before_the_next_peers() {
        let ms: Vec<pb::Member> = ["a", "a", "b", "b", "c", "c"]
            .iter()
            .enumerate()
            .map(|(i, r)| member(i as u64, &[("rack", r)], "m"))
            .collect();
        let writer = &ms[0];
        let ranked: Vec<&pb::Member> = ms[1..].iter().collect(); // ring order after the writer
        let racks = |ids: &[u64]| ids.iter().map(|&i| ms[i as usize].labels["rack"].clone()).collect::<Vec<_>>();
        let two = spread(&ranked, 2, &FAILURE_DOMAINS, Some(writer));
        assert_eq!(racks(&two), ["b", "c"], "the writer's rack-mate is next on the ring, yet comes last");
        assert_eq!(spread(&ranked, 5, &FAILURE_DOMAINS, Some(writer)), [2, 4, 1, 3, 5]);
        assert_eq!(spread(&ranked, 1, &FAILURE_DOMAINS, Some(writer)), [2], "a smaller n takes a prefix");
    }

    #[test]
    fn spreading_goes_zone_then_rack_then_machine() {
        let ms = [
            member(1, &[("zone", "z1"), ("rack", "r1")], "m1"), // the writer
            member(2, &[("zone", "z1"), ("rack", "r1")], "m1"), // same machine
            member(3, &[("zone", "z1"), ("rack", "r1")], "m2"), // same rack, other machine
            member(4, &[("zone", "z1"), ("rack", "r2")], "m3"), // same zone, other rack
            member(5, &[("zone", "z2"), ("rack", "r1")], "m4"), // other zone (its r1 is not z1's)
        ];
        let ranked: Vec<&pb::Member> = ms[1..].iter().collect();
        assert_eq!(spread(&ranked, 4, &FAILURE_DOMAINS, Some(&ms[0])), [5, 4, 3, 2]);
        // Without labels, other machines still come first.
        let bare = [member(1, &[], "m1"), member(2, &[], "m1"), member(3, &[], "m2")];
        let ranked: Vec<&pb::Member> = bare[1..].iter().collect();
        assert_eq!(spread(&ranked, 2, &FAILURE_DOMAINS, Some(&bare[0])), [3, 2]);
    }
}
