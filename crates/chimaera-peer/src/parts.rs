//! Results of several values (`returns=N`) whose large values are objects of
//! their own. The result's file lists them (Python `_serde.PARTS_MAGIC`): a
//! header (magic, reserved, count), then per value its part id (zeros: kept
//! in the result's file), offset and length. A consumer of one value fetches
//! only that part. The peer reads the list to free parts with their result
//! and to know which peer to fetch them from. A result of many values most
//! of which are left out (Python `_serde.SPARSE_MAGIC`, a shuffle scatter's
//! buckets) lists the ids of its parts right after its header, and counts
//! them where the other layout counts its values.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::Mutex;

const MAGIC: &[u8; 4] = b"CHP1";
const HEAD: usize = 12;
const ITEM: usize = 32;
/// Python's `SEQ_MAX`: no result lists more values.
const MAX_PARTS: usize = 4096;
const SPARSE: &[u8; 4] = b"CHQ1";
const SPARSE_HEAD: usize = 24;
/// Parts of a sparse result read at most: each holds a value of 64 KiB or
/// more, so a result lists fewer unless it is over 64 GiB.
const MAX_SPARSE_PARTS: usize = 1 << 20;
/// Hints kept before they are dropped and learned again.
const MAX_HINTS: usize = 1 << 16;
/// The newest layout of stored results this peer and the executors of its
/// install read, advertised to the others (`Member.object_layout`): 1, the
/// sparse one. A shuffle writes it only while every live peer reads it.
pub const OBJECT_LAYOUT: u32 = 1;

/// The part ids a result's bytes list (empty for any other object).
pub fn ids(bytes: &[u8]) -> Vec<Vec<u8>> {
    if bytes.len() >= SPARSE_HEAD && &bytes[..4] == SPARSE {
        let n = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
        return bytes[SPARSE_HEAD..].chunks_exact(16).take(n.min(MAX_SPARSE_PARTS)).map(<[u8]>::to_vec).collect();
    }
    if bytes.len() < HEAD || &bytes[..4] != MAGIC {
        return Vec::new();
    }
    let n = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
    bytes[HEAD..]
        .chunks_exact(ITEM)
        .take(n.min(MAX_PARTS))
        .map(|item| &item[..16])
        .filter(|id| id.iter().any(|&b| b != 0))
        .map(<[u8]>::to_vec)
        .collect()
}

/// The part ids the object file at `path` lists (empty when it is not a
/// result of several values, or cannot be read).
pub fn file_ids(path: &Path) -> Vec<Vec<u8>> {
    let Ok(mut f) = std::fs::File::open(path) else { return Vec::new() };
    let mut head = [0u8; HEAD];
    if f.read_exact(&mut head).is_err() {
        return Vec::new();
    }
    let n = u32::from_le_bytes(head[8..12].try_into().expect("4 bytes")) as usize;
    let len = match &head[..4] {
        m if m == MAGIC => HEAD + n.min(MAX_PARTS) * ITEM,
        m if m == SPARSE => SPARSE_HEAD + n.min(MAX_SPARSE_PARTS) * 16,
        _ => return Vec::new(),
    };
    let mut bytes = vec![0u8; len];
    bytes[..HEAD].copy_from_slice(&head);
    if f.read_exact(&mut bytes[HEAD..]).is_err() {
        return Vec::new();
    }
    ids(&bytes)
}

/// Where parts are: the peer each result was fetched from wrote its parts
/// beside it (or holds the replicas of both). A part is asked for by id
/// alone, so without this every live peer would be tried in turn. Keyed by
/// run and part id, concatenated.
#[derive(Default)]
pub struct Hints(Mutex<HashMap<Vec<u8>, u64>>);

impl Hints {
    /// The result at `path` of `run` came from `node`.
    pub fn learn(&self, run: &[u8], path: &Path, node: u64) {
        let parts = file_ids(path);
        if parts.is_empty() {
            return;
        }
        let mut hints = self.0.lock().expect("part hints");
        if hints.len() + parts.len() > MAX_HINTS {
            hints.clear();
        }
        for part in parts {
            hints.insert([run, &part].concat(), node);
        }
    }

    /// The peer that holds `part` of `run`, if known.
    pub fn holder(&self, run: &[u8], part: &[u8]) -> Vec<u64> {
        self.0.lock().expect("part hints").get(&[run, part].concat()).copied().into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(entries: &[[u8; 16]]) -> Vec<u8> {
        let mut b = MAGIC.to_vec();
        b.extend(0u32.to_le_bytes());
        b.extend((entries.len() as u32).to_le_bytes());
        for id in entries {
            b.extend(id);
            b.extend(0u64.to_le_bytes());
            b.extend(7u64.to_le_bytes());
        }
        b
    }

    #[test]
    fn a_result_lists_the_values_stored_on_their_own_and_nothing_else_lists_any() {
        let bytes = result(&[[1; 16], [0; 16], [3; 16]]);
        assert_eq!(ids(&bytes), [vec![1; 16], vec![3; 16]], "a zero id is kept in the result's file");
        assert!(ids(b"CHS1\0\0\0\0\x02\0\0\0").is_empty(), "a tuple stored element by element in one file");
        assert!(ids(b"CHO1").is_empty() && ids(b"").is_empty());
        assert_eq!(ids(&bytes[..bytes.len() - 1]), [vec![1; 16]], "a short table yields only whole entries");
    }

    #[test]
    fn hints_name_the_peer_a_result_came_from_for_each_of_its_parts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r");
        std::fs::write(&path, result(&[[1; 16], [2; 16]])).unwrap();
        assert_eq!(file_ids(&path), [vec![1; 16], vec![2; 16]]);
        let hints = Hints::default();
        hints.learn(b"run", &path, 9);
        assert_eq!(hints.holder(b"run", &[2; 16]), [9]);
        assert!(hints.holder(b"other", &[2; 16]).is_empty() && hints.holder(b"run", &[5; 16]).is_empty());
        std::fs::write(&path, b"CHO1 not a result of several values").unwrap();
        hints.learn(b"run2", &path, 4);
        assert!(file_ids(&path).is_empty() && hints.holder(b"run2", &[1; 16]).is_empty());
    }

    #[test]
    fn a_sparse_result_lists_its_parts_after_its_header() {
        // As Python's pack_seq writes one: magic, reserved, parts, entries,
        // count, the parts' ids, then one entry (position, part, offset, length) per value.
        let mut b = SPARSE.to_vec();
        b.extend(0u32.to_le_bytes());
        b.extend(2u32.to_le_bytes());
        b.extend(3u32.to_le_bytes());
        b.extend(1_000_000u64.to_le_bytes());
        b.extend([[4u8; 16], [5u8; 16]].concat());
        for (position, part) in [(7u64, 1u64), (70, 0), (700_000, 2)] {
            for v in [position, part, 0, 9] {
                b.extend(v.to_le_bytes());
            }
        }
        assert_eq!(ids(&b), [vec![4; 16], vec![5; 16]]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r");
        std::fs::write(&path, &b).unwrap();
        assert_eq!(file_ids(&path), [vec![4; 16], vec![5; 16]], "read from its header, not its entries");
        std::fs::write(&path, &b[..SPARSE_HEAD + 16]).unwrap();
        assert!(file_ids(&path).is_empty(), "a short list is not read");
        assert_eq!(ids(&b[..SPARSE_HEAD + 16]), [vec![4; 16]], "a short list yields only whole ids");
    }
}
