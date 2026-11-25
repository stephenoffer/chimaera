//! A double-ended queue that also counts its items by key, so "is anything
//! for this key queued?" costs a hash lookup instead of a scan.
//!
//! The core checks that for every finished call in every group it hosts
//! (most of which never queued here), against a queue thousands deep during a
//! burst. Reads go through `Deref`; there is no `DerefMut`, so every mutation
//! passes through the methods below and the counts cannot drift.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::Deref;

pub struct KeyedQueue<T, K> {
    items: VecDeque<T>,
    counts: HashMap<K, u32>,
    key: fn(&T) -> K,
}

impl<T, K: Copy + Eq + Hash> KeyedQueue<T, K> {
    pub fn new(key: fn(&T) -> K) -> Self {
        KeyedQueue { items: VecDeque::new(), counts: HashMap::new(), key }
    }

    pub fn contains_key(&self, k: &K) -> bool {
        self.counts.contains_key(k)
    }

    /// Whether an item for `k` satisfies `f`. A hash lookup, and a scan only
    /// while something for `k` is queued: a check made for every finished
    /// call must not cost the queue's depth.
    pub fn any_for(&self, k: &K, f: impl FnMut(&T) -> bool) -> bool {
        self.contains_key(k) && self.items.iter().any(f)
    }

    /// Where the first item for `k` that satisfies `f` is, scanning only
    /// while something for `k` is queued.
    pub fn position_for(&self, k: &K, f: impl FnMut(&T) -> bool) -> Option<usize> {
        if self.contains_key(k) { self.items.iter().position(f) } else { None }
    }

    fn added(&mut self, item: &T) {
        *self.counts.entry((self.key)(item)).or_default() += 1;
    }

    fn removed(&mut self, item: &T) {
        let k = (self.key)(item);
        if let Some(n) = self.counts.get_mut(&k) {
            *n -= 1;
            if *n == 0 {
                self.counts.remove(&k);
            }
        }
    }

    pub fn push_back(&mut self, item: T) {
        self.added(&item);
        self.items.push_back(item);
    }

    pub fn push_front(&mut self, item: T) {
        self.added(&item);
        self.items.push_front(item);
    }

    pub fn insert(&mut self, at: usize, item: T) {
        self.added(&item);
        self.items.insert(at, item);
    }

    pub fn remove(&mut self, at: usize) -> Option<T> {
        let item = self.items.remove(at)?;
        self.removed(&item);
        Some(item)
    }

    /// An item to update in place. Its key must not change.
    pub fn get_mut(&mut self, at: usize) -> Option<&mut T> {
        self.items.get_mut(at)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let (counts, key) = (&mut self.counts, self.key);
        self.items.retain(|item| {
            let kept = keep(item);
            if !kept && let Some(n) = counts.get_mut(&key(item)) {
                *n -= 1;
                if *n == 0 {
                    counts.remove(&key(item));
                }
            }
            kept
        });
    }
}

impl<T, K> Deref for KeyedQueue<T, K> {
    type Target = VecDeque<T>;

    fn deref(&self) -> &VecDeque<T> {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_follow_every_mutation() {
        let mut q: KeyedQueue<(u32, &str), u32> = KeyedQueue::new(|x| x.0);
        q.push_back((1, "a"));
        q.push_back((2, "b"));
        q.push_front((1, "c"));
        q.insert(1, (3, "d"));
        assert!(q.contains_key(&1) && q.contains_key(&2) && q.contains_key(&3));
        q.retain(|x| x.0 != 1);
        assert!(!q.contains_key(&1), "both entries for 1 went");
        let removed = q.remove(0).unwrap();
        assert!(!q.contains_key(&removed.0));
        assert_eq!(q.len(), 1);
        assert!(q.contains_key(&2) && !q.contains_key(&4));
    }

    /// A key with nothing queued is answered without looking at the items: a
    /// local peer asked this for each of a million queued calls as it finished,
    /// and a scan per call made the burst quadratic (it did not finish in 900 s).
    #[test]
    fn a_lookup_for_a_key_with_nothing_queued_reads_no_items() {
        let mut q: KeyedQueue<(u32, u32), u32> = KeyedQueue::new(|x| x.0);
        for i in 0..100_000 {
            q.push_back((i, 0));
        }
        let mut read = 0;
        assert!(!q.any_for(&100_000, |_| {
            read += 1;
            true
        }));
        assert_eq!(
            q.position_for(&100_001, |_| {
                read += 1;
                true
            }),
            None
        );
        assert_eq!(read, 0, "the items were scanned for a key with none queued");
        // A key that is queued is still found, by its predicate.
        assert!(q.any_for(&99_999, |x| x.0 == 99_999));
        assert_eq!(q.position_for(&99_999, |x| x.0 == 99_999), Some(99_999));
        assert!(!q.any_for(&99_999, |x| x.0 == 99_999 && x.1 == 1), "the predicate still decides");
    }
}
