//! The capacity exchange between cells (`cell::neighbors`, `merge`, `choose`)
//! at 196 and 512 cells, against an oracle that knows every cell's free
//! capacity at the moment it routes.
//!
//! Each round (one summary period) every cell asks its neighbors for their
//! reports and digests, ends and starts calls, and sends each call it cannot
//! start to another cell: by its view (the exchange) or by the truth (the
//! oracle). A call sent to a cell without room waits in that cell's queue.
//! Cells are in four regions and have 32 to 96 CPUs; every fourth has 8 GPUs
//! too. A tenth of the cells, a new tenth every 30 rounds, get 1.6 times their
//! capacity in calls, the others half of theirs.

use std::collections::{BTreeMap, VecDeque};

use chimaera_core::cell::{self, DIGEST, NEIGHBORS};
use chimaera_core::pb;

const REGIONS: usize = 4;
const ROUNDS: u64 = 150;
const WARMUP: u64 = 30;
const RESOURCES: [&str; 2] = ["cpu", "gpu"];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

struct Call {
    arrived: u64,
    rounds: u64,
    /// Sent here from another cell: it waits here.
    delegated: bool,
}

struct Cell {
    name: String,
    region: String,
    total: [f64; 2],
    /// End rounds of the calls running, per resource.
    running: [Vec<u64>; 2],
    queue: [VecDeque<Call>; 2],
    view: Vec<pb::CellCapacity>,
    /// Sent to each (cell, resource) since its report numbered .1.
    sent: BTreeMap<(usize, usize), (f64, u64)>,
}

impl Cell {
    /// What it could start now, less what waits for it.
    fn room(&self, r: usize) -> f64 {
        self.total[r] - self.running[r].len() as f64 - self.queue[r].len() as f64
    }

    fn reports(&self, now: u64) -> Vec<pb::CellCapacity> {
        (0..2)
            .filter(|r| self.total[*r] > 0.0)
            .map(|r| pb::CellCapacity {
                cell: self.name.clone(),
                region: self.region.clone(),
                resource: RESOURCES[r].into(),
                free: self.room(r).max(0.0),
                total: self.total[r],
                number: now,
                ..Default::default()
            })
            .collect()
    }
}

#[derive(Default)]
struct Outcome {
    /// Rounds each call counted waited before it started (or until the end).
    waits: Vec<u64>,
    sent: u64,
    /// Calls sent to a cell that had no room for them.
    missed: u64,
    max_asks: usize,
    max_asked: usize,
    max_reply: usize,
}

impl Outcome {
    fn mean_wait(&self) -> f64 {
        self.waits.iter().sum::<u64>() as f64 / self.waits.len().max(1) as f64
    }

    fn p99_wait(&self) -> u64 {
        let mut w = self.waits.clone();
        w.sort_unstable();
        w.get(w.len() * 99 / 100).copied().unwrap_or(0)
    }
}

fn run(n: usize, oracle: bool, seed: u64) -> Outcome {
    // Arrivals draw from one stream and routing from another, so both ways of
    // routing see the same calls.
    let (mut rng, mut draws) = (Rng(seed), Rng(seed ^ 0x9e37_79b9_7f4a_7c15));
    let mut cells: Vec<Cell> = (0..n)
        .map(|i| Cell {
            name: format!("c{i:04}"),
            region: format!("r{}", i % REGIONS),
            total: [32.0 * (1 + i % 3) as f64, if i % 4 == 1 { 8.0 } else { 0.0 }],
            running: [Vec::new(), Vec::new()],
            queue: [VecDeque::new(), VecDeque::new()],
            view: Vec::new(),
            sent: BTreeMap::new(),
        })
        .collect();
    // One order every cell uses, as the peers do: by name.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|a, b| cells[*a].name.cmp(&cells[*b].name));
    let asks: Vec<Vec<usize>> = {
        let mut place = vec![0; n];
        for (pos, i) in order.iter().enumerate() {
            place[*i] = pos;
        }
        (0..n).map(|i| cell::neighbors(n, place[i], NEIGHBORS).into_iter().map(|p| order[p]).collect()).collect()
    };
    let index: BTreeMap<String, usize> = cells.iter().enumerate().map(|(i, c)| (c.name.clone(), i)).collect();
    let mut hot = vec![false; n];
    let mut out = Outcome::default();
    for now in 0..ROUNDS {
        if now % 30 == 0 {
            hot.iter_mut().for_each(|h| *h = rng.unit() < 0.1);
        }
        // 1. The exchange: every cell asks its neighbors, which all answer
        // with what they had when the round began.
        if !oracle {
            let replies: Vec<Vec<pb::CellCapacity>> =
                cells.iter().map(|c| c.reports(now).into_iter().chain(c.view.iter().cloned()).collect()).collect();
            let mut asked = vec![0usize; n];
            for (i, mine) in asks.iter().enumerate() {
                out.max_asks = out.max_asks.max(mine.len());
                for j in mine {
                    asked[*j] += 1;
                    out.max_reply = out.max_reply.max(replies[*j].len());
                }
                let me = cells[i].name.clone();
                cell::merge(&mut cells[i].view, mine.iter().flat_map(|j| replies[*j].iter().cloned()), &me);
            }
            out.max_asked = out.max_asked.max(asked.into_iter().max().unwrap_or(0));
        }
        // 2. Calls end, and waiting calls start where they wait.
        for c in cells.iter_mut() {
            for r in 0..2 {
                c.running[r].retain(|end| *end > now);
                while !c.queue[r].is_empty() && (c.running[r].len() as f64) < c.total[r] {
                    let call = c.queue[r].pop_front().expect("not empty");
                    c.running[r].push(now + call.rounds);
                    if call.arrived >= WARMUP {
                        out.waits.push(now - call.arrived);
                    }
                }
            }
        }
        // 3. New calls start at home if they can, else wait there.
        for (i, c) in cells.iter_mut().enumerate() {
            for r in 0..2 {
                let load = if hot[i] { 1.6 } else { 0.5 };
                let rate = load * c.total[r] / 5.5; // calls last 2 to 9 rounds
                for _ in 0..(rate + rng.unit()) as usize {
                    let call = Call { arrived: now, rounds: 2 + rng.next() % 8, delegated: false };
                    if c.room(r) >= 1.0 {
                        c.running[r].push(now + call.rounds);
                        if call.arrived >= WARMUP {
                            out.waits.push(0);
                        }
                    } else {
                        c.queue[r].push_back(call);
                    }
                }
            }
        }
        // 4. Calls waiting at home leave for another cell with room.
        for i in 0..n {
            for r in 0..2 {
                let (home, staying): (VecDeque<Call>, VecDeque<Call>) =
                    cells[i].queue[r].drain(..).partition(|c| !c.delegated);
                cells[i].queue[r] = staying;
                for mut call in home {
                    let to = if oracle { truth(&cells, i, r) } else { exchange(&mut cells, &index, i, r, &mut draws) };
                    let Some(j) = to else {
                        cells[i].queue[r].push_back(call);
                        continue;
                    };
                    call.delegated = true;
                    let room = cells[j].room(r) >= 1.0;
                    if call.arrived >= WARMUP {
                        out.sent += 1;
                        out.missed += u64::from(!room);
                    }
                    if room {
                        cells[j].running[r].push(now + call.rounds);
                        if call.arrived >= WARMUP {
                            out.waits.push(now - call.arrived);
                        }
                    } else {
                        cells[j].queue[r].push_back(call);
                    }
                }
            }
        }
    }
    // Calls still waiting count what they waited so far.
    for c in &cells {
        let waiting = c.queue.iter().flatten().filter(|c| c.arrived >= WARMUP);
        out.waits.extend(waiting.map(|c| ROUNDS - c.arrived));
    }
    out
}

/// The oracle: the roomiest cell, in the sender's region if one there has room.
fn truth(cells: &[Cell], i: usize, r: usize) -> Option<usize> {
    let key = |j: usize| (cells[j].region == cells[i].region, cells[j].room(r));
    (0..cells.len())
        .filter(|j| *j != i && cells[*j].room(r) >= 1.0)
        .max_by(|a, b| key(*a).0.cmp(&key(*b).0).then(key(*a).1.total_cmp(&key(*b).1)).then(b.cmp(a)))
}

/// The exchange: cell `i`'s view, less what it sent since each report.
fn exchange(cells: &mut [Cell], index: &BTreeMap<String, usize>, i: usize, r: usize, draws: &mut Rng) -> Option<usize> {
    let me = &cells[i];
    let candidates: Vec<(usize, &pb::CellCapacity)> =
        me.view.iter().filter(|c| c.resource == RESOURCES[r]).map(|c| (index[&c.cell], c)).collect();
    let room: Vec<(&str, f64)> = candidates
        .iter()
        .map(|(j, c)| {
            let sent = me.sent.get(&(*j, r)).filter(|(_, n)| *n == c.number).map_or(0.0, |s| s.0);
            (c.region.as_str(), (c.free - sent).min(c.total))
        })
        .collect();
    let k = cell::choose(&room, &me.region, 1.0, draws.next())?;
    let (j, number) = (candidates[k].0, candidates[k].1.number);
    let entry = cells[i].sent.entry((j, r)).or_insert((0.0, number));
    if entry.1 != number {
        *entry = (0.0, number);
    }
    entry.0 += 1.0;
    Some(j)
}

#[test]
fn a_bounded_exchange_through_four_neighbors_routes_close_to_an_oracle() {
    for n in [196, 512] {
        let oracle = run(n, true, 7);
        let ours = run(n, false, 7);
        let missed = ours.missed as f64 / ours.sent.max(1) as f64;
        let summary = format!(
            "{n} cells: wait mean {:.4} p99 {} rounds (oracle {:.4}, {}), {} calls sent, {:.1}% found no room; \
             each cell asks {} and is asked by at most {}, a reply holds at most {} reports",
            ours.mean_wait(),
            ours.p99_wait(),
            oracle.mean_wait(),
            oracle.p99_wait(),
            ours.sent,
            100.0 * missed,
            ours.max_asks,
            ours.max_asked,
            ours.max_reply,
        );
        eprintln!("{summary}");
        // Per-cell work is bounded whatever the fleet's size: each round a cell
        // asks NEIGHBORS cells and is asked by as many, and each reply holds its
        // own reports (one per resource) and a digest of at most DIGEST.
        assert_eq!((ours.max_asks, ours.max_asked), (NEIGHBORS, NEIGHBORS), "{summary}");
        assert!(ours.max_reply <= DIGEST + RESOURCES.len(), "{summary}");
        // Routing quality next to the oracle's. Both route about the same
        // number of calls; ours rarely sends one where the room has gone.
        assert!(ours.sent as f64 >= 0.9 * oracle.sent as f64, "{summary} (oracle sent {})", oracle.sent);
        assert!(missed < 0.05, "{summary}");
        assert!(ours.mean_wait() < oracle.mean_wait() + 0.01, "{summary}");
        assert!(ours.p99_wait() <= oracle.p99_wait(), "{summary}");
    }
}
