//! Peer labels for placement: `Placement(require={...}, prefer=[...])`.
//!
//! A peer's labels are `$CHIMAERA_LABELS` (`zone=us-east1-b,rack=r7`) plus
//! `node` (its peer id, hex), `host`, `os` and `arch`. `require` keeps the peers
//! whose labels match every pair; each `prefer` entry `key=value` a peer
//! matches makes it cheaper to choose.

use std::collections::{BTreeMap, HashMap};

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
}
