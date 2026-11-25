//! How a durable actor's state is split into chunks once it is too large to
//! commit inline (ActorChunkPut, ActorCommitRoot).
//!
//! Boundaries are content-defined (a gear rolling hash over the last 64
//! bytes), so an edit moves only the boundaries near it: a commit after a
//! small change writes the chunks around it and names the others again.

/// No boundary before this many bytes of a chunk.
pub const MIN: usize = 64 << 10;
/// Nor after this many: every chunk fits one ActorChunkPut.
pub const MAX: usize = super::MAX_INLINE_STATE;
/// A boundary where these 18 high bits of the hash are zero: about every
/// 256 KiB past MIN.
const MASK: u64 = ((1 << 18) - 1) << 46;

const GEAR: [u64; 256] = gear();

/// A fixed table of random-looking words (splitmix64 from 0), the same on
/// every replica and in every version.
const fn gear() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut x: u64 = 0;
    let mut i = 0;
    while i < 256 {
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        table[i] = z ^ (z >> 31);
        i += 1;
    }
    table
}

/// `data` cut into consecutive chunks of MIN..=MAX bytes (the last may be
/// shorter); empty data has none.
pub fn split(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(data.len() / (MIN + (1 << 18)) + 1);
    let mut rest = data;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(boundary(rest));
        out.push(chunk);
        rest = tail;
    }
    out
}

/// What committing `data` takes when the actor's state is now the chunks
/// `stored`: the digests of its chunks in order (the list to commit), and
/// the chunks not stored yet, once each (the ones to put first).
pub fn plan<'a>(data: &'a [u8], stored: &[Vec<u8>]) -> (Vec<Vec<u8>>, Vec<&'a [u8]>) {
    let mut have: std::collections::HashSet<Vec<u8>> = stored.iter().cloned().collect();
    let mut root = Vec::new();
    let mut puts = Vec::new();
    for chunk in split(data) {
        let digest = blake3::hash(chunk).as_bytes().to_vec();
        if have.insert(digest.clone()) {
            puts.push(chunk);
        }
        root.push(digest);
    }
    (root, puts)
}

fn boundary(d: &[u8]) -> usize {
    if d.len() <= MIN {
        return d.len();
    }
    let end = d.len().min(MAX);
    let mut h = 0u64;
    // The hash depends on the last 64 bytes only: start it 64 before MIN.
    for (i, &b) in d.iter().enumerate().take(end).skip(MIN - 64) {
        h = (h << 1).wrapping_add(GEAR[b as usize]);
        if i >= MIN && h & MASK == 0 {
            return i + 1;
        }
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn chunks_cover_the_data_within_their_bounds() {
        let data = noise(5 << 20, 1);
        let chunks = split(&data);
        assert_eq!(chunks.concat(), data);
        assert!(chunks.len() > 4, "{} chunks", chunks.len());
        assert!(chunks[..chunks.len() - 1].iter().all(|c| (MIN..=MAX).contains(&c.len())));
        assert!(split(&[]).is_empty());
        assert_eq!(split(&data[..100]), vec![&data[..100]]);
    }

    #[test]
    fn an_insertion_changes_only_the_chunks_near_it() {
        let data = noise(8 << 20, 2);
        let mut edited = data.clone();
        edited.splice(3 << 20..3 << 20, b"a few more bytes".iter().copied());
        let (stored, first) = plan(&data, &[]);
        assert_eq!(first.len(), stored.len(), "nothing stored yet: every chunk is put");
        let (root, puts) = plan(&edited, &stored);
        assert!(puts.len() <= 2, "{} of {} chunks put again", puts.len(), root.len());
        assert_eq!(plan(&edited, &root).1.len(), 0, "an unchanged state puts nothing");
    }
}
