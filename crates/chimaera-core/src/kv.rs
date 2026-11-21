//! The ordered key-value interface the shard state machine writes through.
//!
//! Production backs it with a redb write transaction; tests use [`MemKv`]. Both
//! iterate in ascending key order, which keeps `apply` deterministic.

use std::collections::BTreeMap;

/// Read access, also served by read-only snapshots (e.g. a redb read transaction).
pub trait KvRead {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    /// Entries whose key starts with `prefix`, ascending, at most `limit`.
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// Like `scan_prefix`, but only keys strictly greater than `after`.
    fn scan_after(&self, prefix: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub trait Kv: KvRead {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&mut self, key: &[u8]);
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemKv(pub BTreeMap<Vec<u8>, Vec<u8>>);

impl Kv for MemKv {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.0.insert(key, value);
    }
    fn delete(&mut self, key: &[u8]) {
        self.0.remove(key);
    }
}

impl KvRead for MemKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).cloned()
    }
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.0
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn scan_after(&self, prefix: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::ops::Bound;
        let start = if after.is_empty() { Bound::Included(prefix.to_vec()) } else { Bound::Excluded(after.to_vec()) };
        self.0
            .range((start, Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl MemKv {
    /// Digest of the full contents, used to prove replicas converge.
    pub fn digest(&self) -> [u8; 32] {
        digest_entries(self.0.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
    }
}

/// blake3 over length-prefixed (key, value) pairs in the given order.
pub fn digest_entries<'a>(entries: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    for (k, v) in entries {
        h.update(&(k.len() as u64).to_le_bytes());
        h.update(k);
        h.update(&(v.len() as u64).to_le_bytes());
        h.update(v);
    }
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_prefix_is_ordered_and_bounded() {
        let mut kv = MemKv::default();
        for k in [b"ab3".to_vec(), b"ab1".to_vec(), b"b".to_vec(), b"ab2".to_vec(), b"a".to_vec()] {
            kv.put(k, vec![]);
        }
        let keys: Vec<_> = kv.scan_prefix(b"ab", 10).into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"ab1".to_vec(), b"ab2".to_vec(), b"ab3".to_vec()]);
        assert_eq!(kv.scan_prefix(b"ab", 2).len(), 2);
        let after: Vec<_> = kv.scan_after(b"ab", b"ab1", 10).into_iter().map(|(k, _)| k).collect();
        assert_eq!(after, vec![b"ab2".to_vec(), b"ab3".to_vec()]);
    }
}
