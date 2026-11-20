//! Hybrid logical clock arithmetic, packed as `wall_ms << 20 | logical`.
//!
//! Pure functions: the caller supplies the physical time, so the shard state
//! machine stays deterministic and peers own the actual clock reads.

pub const LOGICAL_BITS: u32 = 20;
pub const LOGICAL_MAX: u64 = (1 << LOGICAL_BITS) - 1;

pub fn pack(wall_ms: u64, logical: u64) -> u64 {
    (wall_ms << LOGICAL_BITS) | (logical & LOGICAL_MAX)
}

pub fn wall_ms(hlc: u64) -> u64 {
    hlc >> LOGICAL_BITS
}

/// Duration in milliseconds expressed in HLC units, for deadlines and TTLs.
pub fn from_ms(ms: u64) -> u64 {
    ms << LOGICAL_BITS
}

/// Next timestamp for a local event: strictly greater than `prev`.
pub fn next(prev: u64, physical_ms: u64) -> u64 {
    let candidate = pack(physical_ms, 0);
    if candidate > prev { candidate } else { advance(prev) }
}

/// Timestamp after receiving `remote`: strictly greater than both.
/// Rejects a remote clock more than `max_skew_ms` ahead of the local one.
pub fn merge(prev: u64, physical_ms: u64, remote: u64, max_skew_ms: u64) -> Result<u64, SkewError> {
    let local_wall = physical_ms.max(wall_ms(prev));
    if wall_ms(remote) > local_wall + max_skew_ms {
        return Err(SkewError { remote_ms: wall_ms(remote), local_ms: local_wall, max_skew_ms });
    }
    Ok(next(prev.max(remote), physical_ms))
}

// When the logical counter would overflow, borrow from the wall component so
// the result stays strictly increasing without waiting.
fn advance(prev: u64) -> u64 {
    prev + 1
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("remote clock {remote_ms} ms is more than {max_skew_ms} ms ahead of local {local_ms} ms")]
pub struct SkewError {
    pub remote_ms: u64,
    pub local_ms: u64,
    pub max_skew_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn next_is_strictly_monotonic(physical in proptest::collection::vec(0u64..10_000, 1..200)) {
            let mut prev = 0;
            for p in physical {
                let n = next(prev, p);
                prop_assert!(n > prev);
                prev = n;
            }
        }

        #[test]
        fn merge_exceeds_both_inputs(prev in 0u64..1 << 40, p in 0u64..1 << 19, remote_ms in 0u64..1 << 19, l in 0u64..LOGICAL_MAX) {
            let remote = pack(remote_ms, l);
            match merge(prev, p, remote, 60_000) {
                Ok(m) => { prop_assert!(m > prev); prop_assert!(m > remote); }
                Err(e) => prop_assert!(e.remote_ms > e.local_ms + 60_000),
            }
        }
    }

    #[test]
    fn logical_overflow_carries_into_wall() {
        let prev = pack(5, LOGICAL_MAX);
        let n = next(prev, 5);
        assert!(n > prev);
        assert_eq!(wall_ms(n), 6);
    }

    #[test]
    fn rejects_far_future_remote() {
        let err = merge(0, 1_000, pack(1_000 + 60_001, 0), 60_000).unwrap_err();
        assert_eq!(err.max_skew_ms, 60_000);
    }
}
