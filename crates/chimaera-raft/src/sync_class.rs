//! Which raft entries a `durable` peer may acknowledge before they reach the
//! disk. A voter told to drop or replace entries it committed, which only a
//! majority losing such entries together can cause, refuses (`crate::diverge`).

/// `ShardCmd.sync_optional`: a top-level `bool` (proto/chimaera/v1/shard.proto).
const SYNC_OPTIONAL_FIELD: u64 = 5;

/// Whether an encoded `ShardCmd` allows being acknowledged unsynced. It reads
/// only the top-level fields before the command's body (prost writes fields in
/// number order, so the flag comes before the body), and answers `false`
/// for anything it cannot read: an entry it is unsure of is synced.
pub fn sync_optional(cmd: &[u8]) -> bool {
    let mut at = 0usize;
    while at < cmd.len() {
        let Some(tag) = varint(cmd, &mut at) else { return false };
        let (field, wire) = (tag >> 3, tag & 7);
        if field == SYNC_OPTIONAL_FIELD {
            return wire == 0 && varint(cmd, &mut at).is_some_and(|v| v != 0);
        }
        if field > SYNC_OPTIONAL_FIELD {
            return false; // past where the flag would be
        }
        let skipped = match wire {
            0 => varint(cmd, &mut at).map(|_| 0),
            1 => Some(8),
            2 => varint(cmd, &mut at).and_then(|n| usize::try_from(n).ok()),
            5 => Some(4),
            _ => None,
        };
        match skipped.and_then(|n| at.checked_add(n)) {
            Some(next) if next <= cmd.len() => at = next,
            _ => return false,
        }
    }
    false
}

fn varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*at)?;
        *at += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimaera_core::pb;
    use prost::Message;

    #[test]
    fn the_flag_is_read_from_an_encoded_command_and_absent_means_synced() {
        let mut cmd = pb::ShardCmd {
            vshard: 7,
            vshard_epoch: 3,
            issuer: Some(pb::PeerInc { peer: vec![1; 16], inc: 2 }),
            hlc: 99,
            body: Some(pb::shard_cmd::Body::Release(pb::Release::default())),
            ..Default::default()
        };
        assert!(!sync_optional(&cmd.encode_to_vec()));
        cmd.sync_optional = true;
        assert!(sync_optional(&cmd.encode_to_vec()));
        // Garbage, a truncated command and nothing at all are all synced.
        assert!(!sync_optional(&[0xff, 0xff, 0xff]));
        let enc = cmd.encode_to_vec();
        assert!(!sync_optional(&enc[..3]));
        assert!(!sync_optional(&[]));
    }
}
