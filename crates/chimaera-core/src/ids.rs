//! 128-bit identifiers that carry their own routing.
//!
//! Layout: bytes `[0..2]` cell, `[2..4]` vshard (big endian), `[4..16]` hash.
//! Any peer can route a run, task or actor id to its owning virtual shard
//! without a lookup, which is what lets a client reattach through any peer.

use std::fmt;

/// Virtual shards per cell. Fixed for the lifetime of a cell.
pub const VSHARDS: u16 = 1024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Id(pub [u8; 16]);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("identifier must be 16 bytes, got {0}")]
pub struct IdLenError(pub usize);

impl Id {
    pub const ZERO: Id = Id([0; 16]);

    /// Deterministic id in `vshard` of `cell`, from length-prefixed parts.
    pub fn derive(cell: u16, vshard: u16, parts: &[&[u8]]) -> Id {
        let mut h = blake3::Hasher::new();
        for p in parts {
            h.update(&(p.len() as u64).to_le_bytes());
            h.update(p);
        }
        let digest = h.finalize();
        let mut b = [0u8; 16];
        b[0..2].copy_from_slice(&cell.to_be_bytes());
        b[2..4].copy_from_slice(&(vshard % VSHARDS).to_be_bytes());
        b[4..16].copy_from_slice(&digest.as_bytes()[..12]);
        Id(b)
    }

    /// Child id that stays in the parent's vshard (tasks of a run live with it).
    pub fn child(&self, parts: &[&[u8]]) -> Id {
        let mut all: Vec<&[u8]> = Vec::with_capacity(parts.len() + 1);
        all.push(&self.0);
        all.extend_from_slice(parts);
        Id::derive(self.cell(), self.vshard(), &all)
    }

    pub fn cell(&self) -> u16 {
        u16::from_be_bytes([self.0[0], self.0[1]])
    }

    pub fn vshard(&self) -> u16 {
        u16::from_be_bytes([self.0[2], self.0[3]]) % VSHARDS
    }

    pub fn from_slice(b: &[u8]) -> Result<Id, IdLenError> {
        let arr: [u8; 16] = b.try_into().map_err(|_| IdLenError(b.len()))?;
        Ok(Id(arr))
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn parse_hex(s: &str) -> Option<Id> {
        if s.len() != 32 {
            return None;
        }
        let mut b = [0u8; 16];
        for (i, out) in b.iter_mut().enumerate() {
            *out = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
        }
        Some(Id(b))
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.hex()[..12])
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_embeds_routing_and_is_stable() {
        let a = Id::derive(3, 700, &[b"tenant", b"key"]);
        assert_eq!(a.cell(), 3);
        assert_eq!(a.vshard(), 700);
        assert_eq!(a, Id::derive(3, 700, &[b"tenant", b"key"]));
        // Length prefixing keeps ("ab","c") distinct from ("a","bc").
        assert_ne!(Id::derive(0, 1, &[b"ab", b"c"]), Id::derive(0, 1, &[b"a", b"bc"]));
    }

    #[test]
    fn child_stays_in_parent_vshard() {
        let run = Id::derive(0, 42, &[b"r"]);
        let t = run.child(&[&7u64.to_le_bytes()]);
        assert_eq!(t.vshard(), 42);
        assert_ne!(t, run);
    }

    #[test]
    fn hex_roundtrip_and_bad_lengths() {
        let a = Id::derive(1, 2, &[b"x"]);
        assert_eq!(Id::parse_hex(&a.hex()), Some(a));
        assert_eq!(Id::parse_hex("zz"), None);
        assert_eq!(Id::from_slice(&[1, 2, 3]), Err(IdLenError(3)));
    }
}
