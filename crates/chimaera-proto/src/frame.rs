//! Length-prefixed frames with optional compression and a blake3 checksum.
//!
//! Layout: `[u32 body_len LE][u8 kind][u8 flags][body]`. When `FLAG_CHECKSUM` is
//! set the body ends with 16 bytes of blake3 over the preceding body bytes.
//! When a compression flag is set the body (before the checksum) is compressed
//! and decompression is bounded by the caller's limit, so a small hostile frame
//! cannot expand into an unbounded allocation.

use std::io::Read;

pub const HEADER_LEN: usize = 6;
pub const CHECKSUM_LEN: usize = 16;

pub const FLAG_LZ4: u8 = 0b0000_0001;
pub const FLAG_ZSTD: u8 = 0b0000_0010;
pub const FLAG_CHECKSUM: u8 = 0b0000_0100;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame body of {len} bytes exceeds the {max}-byte limit")]
    TooLarge { len: usize, max: usize },
    #[error("decompressed size {declared} exceeds the {max}-byte limit")]
    DecompressionLimit { declared: usize, max: usize },
    #[error("checksum mismatch")]
    Checksum,
    #[error("malformed frame: {0}")]
    Malformed(String),
}

/// First 16 bytes of the blake3 hash of `data`.
pub fn checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    let h = blake3::hash(data);
    let mut out = [0u8; CHECKSUM_LEN];
    out.copy_from_slice(&h.as_bytes()[..CHECKSUM_LEN]);
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    None,
    Lz4,
    Zstd,
}

/// Encode one frame. Compression is applied only when it shrinks the body.
pub fn encode(kind: u8, body: &[u8], codec: Codec, with_checksum: bool) -> Vec<u8> {
    let (payload, flag) = match codec {
        Codec::None => (None, 0),
        Codec::Lz4 => (Some(lz4_flex::compress_prepend_size(body)), FLAG_LZ4),
        Codec::Zstd => (zstd::bulk::compress(body, 1).ok(), FLAG_ZSTD),
    };
    let (payload, mut flags): (&[u8], u8) = match &payload {
        Some(p) if p.len() < body.len() => (p, flag),
        _ => (body, 0),
    };
    if with_checksum {
        flags |= FLAG_CHECKSUM;
    }
    let len = payload.len() + if with_checksum { CHECKSUM_LEN } else { 0 };
    let mut out = Vec::with_capacity(HEADER_LEN + len);
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.push(kind);
    out.push(flags);
    out.extend_from_slice(payload);
    if with_checksum {
        out.extend_from_slice(&checksum(payload));
    }
    out
}

/// Parsed header: (body length, kind, flags).
pub fn parse_header(h: &[u8; HEADER_LEN], max_body: usize) -> Result<(usize, u8, u8), FrameError> {
    let len = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
    if len > max_body {
        return Err(FrameError::TooLarge { len, max: max_body });
    }
    Ok((len, h[4], h[5]))
}

/// Verify and decompress a frame body read after `parse_header`.
pub fn decode_body(flags: u8, body: &[u8], max_decompressed: usize) -> Result<Vec<u8>, FrameError> {
    let payload = if flags & FLAG_CHECKSUM != 0 {
        if body.len() < CHECKSUM_LEN {
            return Err(FrameError::Malformed("body shorter than checksum".into()));
        }
        let (p, sum) = body.split_at(body.len() - CHECKSUM_LEN);
        if checksum(p) != sum {
            return Err(FrameError::Checksum);
        }
        p
    } else {
        body
    };
    match flags & (FLAG_LZ4 | FLAG_ZSTD) {
        0 => {
            if payload.len() > max_decompressed {
                return Err(FrameError::TooLarge { len: payload.len(), max: max_decompressed });
            }
            Ok(payload.to_vec())
        }
        FLAG_LZ4 => decompress_lz4(payload, max_decompressed),
        FLAG_ZSTD => decompress_zstd(payload, max_decompressed),
        _ => Err(FrameError::Malformed("both lz4 and zstd flags set".into())),
    }
}

fn decompress_lz4(data: &[u8], max: usize) -> Result<Vec<u8>, FrameError> {
    // lz4_flex prepends the decompressed size; check it before allocating.
    let declared = data
        .get(..4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        .ok_or_else(|| FrameError::Malformed("lz4 body shorter than its size prefix".into()))?;
    if declared > max {
        return Err(FrameError::DecompressionLimit { declared, max });
    }
    lz4_flex::decompress_size_prepended(data).map_err(|e| FrameError::Malformed(e.to_string()))
}

fn decompress_zstd(data: &[u8], max: usize) -> Result<Vec<u8>, FrameError> {
    // Stream with a hard cap: the frame header's content size can lie or be absent.
    let decoder = zstd::stream::read::Decoder::new(data).map_err(|e| FrameError::Malformed(e.to_string()))?;
    let mut out = Vec::new();
    let read = decoder.take(max as u64 + 1).read_to_end(&mut out).map_err(|e| FrameError::Malformed(e.to_string()))?;
    if read > max {
        return Err(FrameError::DecompressionLimit { declared: read, max });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(codec: Codec, sum: bool, body: &[u8]) -> Vec<u8> {
        let f = encode(7, body, codec, sum);
        let mut h = [0u8; HEADER_LEN];
        h.copy_from_slice(&f[..HEADER_LEN]);
        let (len, kind, flags) = parse_header(&h, 1 << 30).unwrap();
        assert_eq!(kind, 7);
        assert_eq!(len, f.len() - HEADER_LEN);
        decode_body(flags, &f[HEADER_LEN..], 1 << 30).unwrap()
    }

    #[test]
    fn roundtrips_every_codec() {
        let body: Vec<u8> = (0..100_000u32).flat_map(|i| (i % 251).to_le_bytes()).collect();
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
            for sum in [false, true] {
                assert_eq!(roundtrip(codec, sum, &body), body, "{codec:?} {sum}");
            }
        }
        assert_eq!(roundtrip(Codec::Zstd, true, b""), b"");
    }

    #[test]
    fn incompressible_body_is_sent_raw() {
        let body: Vec<u8> = (0..64u8).collect();
        let f = encode(1, &body, Codec::Zstd, false);
        assert_eq!(f[5] & (FLAG_LZ4 | FLAG_ZSTD), 0);
    }

    #[test]
    fn zstd_bomb_is_rejected_without_full_expansion() {
        let bomb = zstd::bulk::compress(&vec![0u8; 64 << 20], 19).unwrap();
        assert!(bomb.len() < 64 << 10, "bomb should be tiny: {}", bomb.len());
        let err = decode_body(FLAG_ZSTD, &bomb, 1 << 20).unwrap_err();
        assert!(matches!(err, FrameError::DecompressionLimit { max, .. } if max == 1 << 20));
    }

    #[test]
    fn lz4_declared_size_is_checked_before_allocation() {
        let mut lying = (u32::MAX).to_le_bytes().to_vec();
        lying.extend_from_slice(&[0x10, 0x00]);
        let err = decode_body(FLAG_LZ4, &lying, 1 << 20).unwrap_err();
        assert_eq!(err, FrameError::DecompressionLimit { declared: u32::MAX as usize, max: 1 << 20 });
    }

    #[test]
    fn corrupted_body_fails_checksum() {
        let mut f = encode(1, b"hello world, hello world", Codec::None, true);
        f[HEADER_LEN + 2] ^= 1;
        assert_eq!(decode_body(f[5], &f[HEADER_LEN..], 1 << 20), Err(FrameError::Checksum));
    }

    #[test]
    fn oversized_header_is_rejected() {
        let f = encode(1, &[0u8; 100], Codec::None, false);
        let mut h = [0u8; HEADER_LEN];
        h.copy_from_slice(&f[..HEADER_LEN]);
        assert_eq!(parse_header(&h, 50), Err(FrameError::TooLarge { len: 100, max: 50 }));
    }
}
