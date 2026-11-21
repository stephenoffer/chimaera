//! Cell summaries: what a routing cell (up to 512 peers) tells the routing
//! tier every couple of seconds, instead of every peer reporting globally.
//!
//! A summary says how many peers are alive, how many free slots each shape
//! class has, how deep the queues are, and which few peers have the most room
//! (candidates for work arriving from other cells). It must stay small (the
//! design budgets 2 KiB) whatever the cell's size. Pure: no clock, no IO.

/// One peer's load as its cell sees it.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerLoad {
    pub alive: bool,
    /// Index of the peer's shape class (CPU count, memory, devices bucketed).
    pub shape: u8,
    pub free_slots: u16,
    pub queued: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellSummary {
    pub cell: u32,
    pub alive: u32,
    /// (shape class, free slots) for every class with any.
    pub free_by_shape: Vec<(u8, u32)>,
    pub queued: u64,
    /// (peer index within the cell, free slots) of the roomiest peers.
    pub candidates: Vec<(u16, u16)>,
}

/// Summarize `loads` (indexed by peer position in the cell), keeping the
/// `top` roomiest peers as candidates.
pub fn summarize(cell: u32, loads: &[PeerLoad], top: usize) -> CellSummary {
    let mut free: [u32; 256] = [0; 256];
    let mut s = CellSummary { cell, ..Default::default() };
    let mut roomy: Vec<(u16, u16)> = Vec::new();
    for (i, l) in loads.iter().enumerate() {
        if !l.alive {
            continue;
        }
        s.alive += 1;
        free[l.shape as usize] += u32::from(l.free_slots);
        s.queued += u64::from(l.queued);
        if l.free_slots > 0 {
            roomy.push((i as u16, l.free_slots));
        }
    }
    s.free_by_shape = free.iter().enumerate().filter(|(_, f)| **f > 0).map(|(c, f)| (c as u8, *f)).collect();
    roomy.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    roomy.truncate(top);
    s.candidates = roomy;
    s
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

impl CellSummary {
    /// Compact wire form (varints), as the routing tier receives it.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + 6 * self.candidates.len());
        varint(&mut out, u64::from(self.cell));
        varint(&mut out, u64::from(self.alive));
        varint(&mut out, self.queued);
        varint(&mut out, self.free_by_shape.len() as u64);
        for (c, f) in &self.free_by_shape {
            out.push(*c);
            varint(&mut out, u64::from(*f));
        }
        varint(&mut out, self.candidates.len() as u64);
        for (p, f) in &self.candidates {
            varint(&mut out, u64::from(*p));
            varint(&mut out, u64::from(*f));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_cell_summarizes_within_the_budget() {
        let loads: Vec<PeerLoad> = (0..512)
            .map(|i| PeerLoad {
                alive: i % 50 != 0,
                shape: (i % 12) as u8,
                free_slots: (i % 33) as u16,
                queued: i as u32,
            })
            .collect();
        let s = summarize(7, &loads, 64);
        assert_eq!(s.alive, 512 - 11);
        assert_eq!(s.candidates.len(), 64);
        assert!(s.candidates.windows(2).all(|w| w[0].1 >= w[1].1), "roomiest first");
        assert!(s.encode().len() <= 2048, "{} bytes", s.encode().len());
    }

    #[test]
    fn dead_peers_count_for_nothing() {
        let loads = [PeerLoad { alive: false, shape: 1, free_slots: 32, queued: 9 }];
        let s = summarize(0, &loads, 8);
        assert_eq!((s.alive, s.queued, s.free_by_shape.len(), s.candidates.len()), (0, 0, 0, 0));
    }
}
