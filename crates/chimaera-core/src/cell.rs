//! Cell summaries: what a routing cell (up to 512 peers) tells the routing
//! tier every couple of seconds, instead of every peer reporting globally.
//!
//! A summary says how many peers are alive, how many free slots each shape
//! class has, how deep the queues are, and which few peers have the most room
//! (candidates for work arriving from other cells). It must stay small (the
//! design budgets 2 KiB) whatever the cell's size. Pure: no clock, no IO.

use crate::pb;

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

// ---- The capacity exchange between cells (docs/federation.md) -------------
//
// Each round a cell's summary publisher asks `NEIGHBORS` other cells for their
// capacity. A reply holds the answering cell's own reports and its digest of
// what it heard, bounded by `DIGEST`. So a cell's work per round is fixed
// whatever the fleet's size, and a report reaches cells farther away a hop
// per round.

/// Cells a cell asks each round.
pub const NEIGHBORS: usize = 4;
/// Reports a digest keeps per (region, resource class), roomiest first.
pub const PER_GROUP: usize = 8;
/// Reports a digest keeps in all.
pub const DIGEST: usize = 64;

/// The cells (indices into `n` cells, in one order every cell uses) that cell
/// `me` asks: those at offsets 1, b, b², … after it, with b the smallest base
/// whose k-th power reaches n. Every cell asks `k` and is asked by `k`, and a
/// report travels at most k·(b−1) hops.
pub fn neighbors(n: usize, me: usize, k: usize) -> Vec<usize> {
    let want = k.min(n.saturating_sub(1));
    let mut base = 2usize;
    while base.saturating_pow(k as u32) < n {
        base += 1;
    }
    let mut out = Vec::with_capacity(want);
    // The same offsets for every cell (filling collisions in a small fleet).
    for offset in (0..k as u32).map(|e| base.saturating_pow(e) % n.max(1)).chain(1..n) {
        if out.len() == want {
            break;
        }
        let cell = (me + offset) % n;
        if cell != me && !out.contains(&cell) {
            out.push(cell);
        }
    }
    out
}

/// Reports older than this many rounds are dropped: by then their room has
/// likely gone to other cells' calls. In tests/fleet_routing.rs at 512 cells,
/// 1.3% of the calls sent find no room at 4, 16% at 8 and 48% at 16; at 3 a
/// cell knows too few others, and calls wait 70 times as long as at 4.
pub const MAX_AGE: u32 = 4;

/// Fold what a cell heard this round (each neighbor's reply: its own reports,
/// at age 0, and its digest) into its `view`. Every report is a round older
/// than its holder had it, and each (cell, resource) keeps its newest report.
/// This cell's own reports and those older than `MAX_AGE` go, and `digest`
/// bounds the rest. Ages count rounds, not time: no cell compares another's clock.
pub fn merge(view: &mut Vec<pb::CellCapacity>, heard: impl IntoIterator<Item = pb::CellCapacity>, me: &str) {
    let mut all: Vec<pb::CellCapacity> = view
        .drain(..)
        .chain(heard)
        .filter_map(|mut c| {
            c.age = c.age.saturating_add(1);
            (c.cell != me && c.age <= MAX_AGE).then_some(c)
        })
        .collect();
    all.sort_by(|a, b| {
        (&a.cell, &a.resource).cmp(&(&b.cell, &b.resource)).then(b.number.cmp(&a.number)).then(a.age.cmp(&b.age))
    });
    all.dedup_by(|later, kept| later.cell == kept.cell && later.resource == kept.resource);
    *view = digest(all);
}

/// At most `PER_GROUP` reports of each (region, resource) group, roomiest
/// first, and `DIGEST` in all, taken from the groups in turn.
pub fn digest(mut reports: Vec<pb::CellCapacity>) -> Vec<pb::CellCapacity> {
    reports.sort_by(|a, b| {
        (&a.region, &a.resource)
            .cmp(&(&b.region, &b.resource))
            .then(b.free.total_cmp(&a.free))
            .then(a.age.cmp(&b.age))
            .then(a.cell.cmp(&b.cell))
    });
    let mut ranked: Vec<(usize, pb::CellCapacity)> = Vec::with_capacity(reports.len());
    for c in reports {
        let rank = match ranked.last() {
            Some((r, l)) if (&l.region, &l.resource) == (&c.region, &c.resource) => r + 1,
            _ => 0,
        };
        ranked.push((rank, c));
    }
    ranked.retain(|(r, _)| *r < PER_GROUP);
    ranked.sort_by_key(|(r, _)| *r); // stable: groups in turn
    ranked.truncate(DIGEST);
    ranked.into_iter().map(|(_, c)| c).collect()
}

/// Where a call that needs `need` goes, of `room` (each candidate's region and
/// the room this cell counts on there): one in `region` if any there has room,
/// else any with room, drawn in proportion to its room so that cells which
/// heard the same reports spread their calls. `draw` is uniform over u64.
pub fn choose(room: &[(&str, f64)], region: &str, need: f64, draw: u64) -> Option<usize> {
    let fits = |r: &(&str, f64)| r.1 + 1e-9 >= need;
    let near = |r: &(&str, f64)| fits(r) && !region.is_empty() && r.0 == region;
    let pool: Vec<usize> = if room.iter().any(near) {
        (0..room.len()).filter(|i| near(&room[*i])).collect()
    } else {
        (0..room.len()).filter(|i| fits(&room[*i])).collect()
    };
    let total: f64 = pool.iter().map(|i| room[*i].1).sum();
    let mut at = (draw >> 11) as f64 / (1u64 << 53) as f64 * total;
    for i in &pool {
        at -= room[*i].1;
        if at < 0.0 {
            return Some(*i);
        }
    }
    pool.last().copied()
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

    #[test]
    fn every_cell_asks_k_others_and_is_asked_by_k() {
        for n in [2, 3, 5, 17, 196, 512] {
            let mut asked = vec![0; n];
            for me in 0..n {
                let ns = neighbors(n, me, NEIGHBORS);
                assert_eq!(ns.len(), NEIGHBORS.min(n - 1), "{n} cells");
                assert!(!ns.contains(&me) && ns.iter().all(|c| *c < n));
                let mut unique = ns.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), ns.len());
                ns.iter().for_each(|c| asked[*c] += 1);
            }
            assert!(asked.iter().all(|a| *a == NEIGHBORS.min(n - 1)), "{n} cells: {asked:?}");
        }
        assert!(neighbors(1, 0, NEIGHBORS).is_empty());
    }

    fn report(cell: &str, region: &str, free: f64, number: u64, age: u32) -> pb::CellCapacity {
        pb::CellCapacity {
            cell: cell.into(),
            region: region.into(),
            resource: "cpu".into(),
            free,
            total: 64.0,
            number,
            age,
            ..Default::default()
        }
    }

    #[test]
    fn a_view_keeps_each_cells_newest_report_ages_them_and_drops_its_own() {
        let mut view = vec![report("b", "r", 10.0, 5, 1), report("c", "r", 3.0, 9, MAX_AGE)];
        let heard = vec![
            report("b", "r", 2.0, 7, 0),  // b's newer report, straight from b
            report("b", "r", 30.0, 4, 0), // an older one by another way
            report("a", "r", 50.0, 9, 0), // this cell's own, passed back
            report("d", "r", 1.0, 1, 2),
        ];
        merge(&mut view, heard, "a");
        let got: Vec<(&str, f64, u32)> = view.iter().map(|c| (c.cell.as_str(), c.free, c.age)).collect();
        // c was MAX_AGE rounds old: a round more is too old.
        assert_eq!(got, [("b", 2.0, 1), ("d", 1.0, 3)]);
    }

    #[test]
    fn a_digest_keeps_the_roomiest_of_each_group_and_is_bounded() {
        let mut reports = Vec::new();
        for region in 0..12 {
            for cell in 0..20 {
                reports.push(report(&format!("c{region}-{cell}"), &format!("r{region}"), cell as f64, 1, 0));
            }
        }
        let d = digest(reports);
        assert_eq!(d.len(), DIGEST);
        // Groups in turn: every region keeps its roomiest first.
        let first: Vec<f64> = d.iter().take(12).map(|c| c.free).collect();
        assert_eq!(first, [19.0; 12]);
        for region in 0..12 {
            let kept: Vec<f64> = d.iter().filter(|c| c.region == format!("r{region}")).map(|c| c.free).collect();
            assert!(kept.len() <= PER_GROUP && kept.windows(2).all(|w| w[0] >= w[1]), "{kept:?}");
        }
    }

    #[test]
    fn calls_go_to_cells_with_room_near_first_in_proportion_to_room() {
        let room = [("far", 9.0), ("near", 1.0), ("near", 3.0), ("near", 0.5)];
        assert_eq!(choose(&room, "near", 1.0, 0), Some(1), "the region with room first");
        assert_eq!(choose(&room, "near", 1.0, u64::MAX), Some(2));
        assert_eq!(choose(&room, "near", 2.0, 0), Some(2), "only one near cell fits 2");
        assert_eq!(choose(&room, "near", 5.0, 0), Some(0), "else anywhere");
        assert_eq!(choose(&room, "near", 10.0, 0), None);
        assert_eq!(choose(&room, "", 1.0, 0), Some(0), "no region: all");
        // Room 1 and 3 near: about a quarter of the draws go to the first.
        let draws = 4000u64;
        let firsts = (0..draws).filter(|i| choose(&room, "near", 1.0, i * (u64::MAX / draws)) == Some(1)).count();
        assert!((900..1100).contains(&firsts), "{firsts}");
    }
}
