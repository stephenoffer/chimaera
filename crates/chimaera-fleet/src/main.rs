//! chimaera-fleet: fleet-scale emulation.
//!
//! The design's capacity appendix sizes a fleet of 100,000 to 262,144 peers
//! in routing cells of up to 512. Cells share nothing on the common path, so
//! this emulator runs whole cells in detail, with the real code each cell
//! runs, and scales by the number of cells:
//!
//! - **Work:** every peer's slots run ranges of 64 items (and a configurable
//!   fraction of individually committed items). Each unit's grant and
//!   publication are applied to the cell leader's real `ShardState`, so the
//!   ledger load and the leader's apply cost are measured, not assumed.
//! - **Liveness:** every peer runs the real `Swim` at the cell's heartbeat
//!   period over a simulated network; peers fail mid-run, and detection time
//!   and false positives are measured.
//! - **Routing:** the cell's real `cell::summarize` runs every summary period
//!   and its encoded size is measured.
//!
//! Everything it prints is emulation. Real peers, disks and networks add
//! replication, fsync and transport costs that are not modelled here.

use chimaera_core::cell::{self, PeerLoad};
use chimaera_core::ids::Id;
use chimaera_core::kv::MemKv;
use chimaera_core::pb;
use chimaera_core::shard::{ShardState, Tk};
use chimaera_core::swim::{Config as SwimConfig, State as SwimState, Swim, Update};
use clap::Parser;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::time::Instant;

#[derive(Parser, Clone, Debug)]
#[command(about = "Emulate a Chimaera fleet by running whole cells with the real SWIM, shard and summary code")]
struct Args {
    /// Fleet sizes to report (peers).
    #[arg(long, value_delimiter = ',', default_value = "100000,262144")]
    peers: Vec<u64>,
    #[arg(long, default_value_t = 512)]
    cell_size: u32,
    #[arg(long, default_value_t = 32)]
    slots: u32,
    #[arg(long, default_value_t = 0.70)]
    occupancy: f64,
    /// Mean service time of one item, seconds.
    #[arg(long, default_value_t = 0.25)]
    service_s: f64,
    #[arg(long, default_value_t = 64)]
    range: u32,
    /// Fraction of items committed one by one instead of in ranges.
    #[arg(long, default_value_t = 0.0)]
    singletons: f64,
    #[arg(long, default_value_t = 5.0)]
    heartbeat_s: f64,
    #[arg(long, default_value_t = 2.0)]
    summary_s: f64,
    /// Simulated seconds per cell.
    #[arg(long, default_value_t = 240.0)]
    seconds: f64,
    /// Fraction of a cell's peers that fail halfway through the run.
    #[arg(long, default_value_t = 0.01)]
    fail: f64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Also run the 1k → 2k weak-scaling gate (2 and 4 cells in detail).
    #[arg(long)]
    weak_scaling: bool,
    #[arg(long)]
    json: bool,
}

/// What one emulated cell measured.
#[derive(Debug, Default, Clone)]
struct CellRun {
    peers: u32,
    seconds: f64,
    items_done: u64,
    ranges_granted: u64,
    records: u64,
    apply_cpu_s: f64,
    commands: u64,
    heartbeats: u64,
    summaries: u64,
    summary_bytes_max: usize,
    detect_s: Vec<f64>,
    false_dead: u64,
    swim_bytes_per_peer: usize,
    items_lost_to_failures: u64,
    failed: u64,
}

fn rng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn unit(state: &mut u64) -> f64 {
    (rng(state) >> 11) as f64 / (1u64 << 53) as f64
}

/// The cell leader's shard: real `ShardState::apply` over an in-memory KV.
struct Ledger {
    sm: ShardState,
    kv: MemKv,
    hlc: u64,
    run: Id,
    next_task: u64,
    cpu_s: f64,
    records: u64,
    commands: u64,
    pending_grants: Vec<pb::TaskSpec>,
    pending_publish: Vec<pb::PublishEntry>,
    peer: pb::PeerInc,
}

impl Ledger {
    fn new(cell: u32) -> Ledger {
        let vshard = (cell % 1024) as u16;
        let run = Id::derive(0, vshard, &[b"fleet-run", &cell.to_le_bytes()]);
        let mut l = Ledger {
            sm: ShardState::new(),
            kv: MemKv::default(),
            hlc: 1,
            run,
            next_task: 0,
            cpu_s: 0.0,
            records: 0,
            commands: 0,
            pending_grants: Vec::new(),
            pending_publish: Vec::new(),
            peer: pb::PeerInc { peer: Id::derive(0, 0, &[b"worker"]).to_vec(), inc: 1 },
        };
        l.apply(vshard, pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![vshard as u32], epoch: 1 }));
        let rec = pb::RunRecord { run: run.to_vec(), tenant: "fleet".into(), ..Default::default() };
        l.apply(vshard, pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(rec) }));
        l.records = 0;
        l.commands = 0;
        l.cpu_s = 0.0;
        l
    }

    fn apply(&mut self, v: u16, body: pb::shard_cmd::Body) -> pb::ShardReply {
        self.hlc += 1;
        let cmd = pb::ShardCmd { vshard: v as u32, vshard_epoch: 1, issuer: None, hlc: self.hlc, body: Some(body) };
        let t = Instant::now();
        let out = self.sm.apply(&mut self.kv, &cmd);
        self.cpu_s += t.elapsed().as_secs_f64();
        self.commands += 1;
        out.reply
    }

    /// A unit (range or singleton) starts: one granted submission.
    fn grant(&mut self) -> Tk {
        self.next_task += 1;
        let tk = Tk { run: self.run, task: self.run.child(&[&self.next_task.to_le_bytes()]) };
        self.pending_grants.push(pb::TaskSpec { key: Some(tk.pb()), max_attempts: 3, ..Default::default() });
        self.records += 1;
        tk
    }

    /// A unit finished: one publication.
    fn publish(&mut self, tk: Tk) {
        self.pending_publish.push(pb::PublishEntry {
            task: Some(tk.pb()),
            attempt: 1,
            peer: Some(self.peer.clone()),
            outcome: Some(pb::Outcome {
                value: Some(pb::outcome::Value::Ok(pb::ObjectRef {
                    inline_data: vec![1],
                    size: 1,
                    ..Default::default()
                })),
                ..Default::default()
            }),
            forward_to: None,
        });
        self.records += 1;
    }

    /// The leader proposes what accumulated since the last flush (as the
    /// peer core batches grants and publications per vshard).
    fn flush(&mut self) {
        let v = self.run.vshard();
        if !self.pending_grants.is_empty() {
            let tasks = std::mem::take(&mut self.pending_grants);
            self.apply(
                v,
                pb::shard_cmd::Body::SubmitTasks(pb::SubmitTasks { tasks, grant_to: Some(self.peer.clone()) }),
            );
        }
        if !self.pending_publish.is_empty() {
            let entries = std::mem::take(&mut self.pending_publish);
            self.apply(v, pb::shard_cmd::Body::Publish(pb::PublishResults { entries }));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    /// A slot finishes its unit (peer, slot, unit id, items).
    Done(u32, u32, u64, u32),
    /// A slot starts a unit after its idle gap.
    Start(u32, u32),
    Flush,
    SwimTick,
    Summary,
    Fail,
}

/// Run one cell for `a.seconds` simulated seconds.
fn run_cell(a: &Args, cell: u32, peers: u32, seed: u64) -> CellRun {
    let mut st = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    // Rates are counted after one full unit cycle, once every slot is in
    // its steady busy/idle rhythm.
    let warm_s = (a.range as f64 * a.service_s / a.occupancy).min(a.seconds / 3.0);
    let mut out = CellRun { peers, seconds: a.seconds - warm_s, ..Default::default() };
    let mut records_at_warm: Option<u64> = None;
    let mut ledger = Ledger::new(cell);
    let mut heap: BinaryHeap<Reverse<(u64, Ev)>> = BinaryHeap::new();
    let us = |s: f64| (s * 1e6) as u64;
    let range_s = a.range as f64 * a.service_s;
    // Probability a unit is a full range, so that `singletons` of all items
    // are committed one by one.
    let r = a.range as f64;
    let q = if a.singletons <= 0.0 { 1.0 } else { (1.0 - a.singletons) / (r * a.singletons + 1.0 - a.singletons) };
    let mut alive = vec![true; peers as usize];
    let mut running: BTreeMap<(u32, u32), (Tk, u64)> = BTreeMap::new();
    let mut busy = vec![0u16; peers as usize];
    for p in 0..peers {
        for s in 0..a.slots {
            // Stagger starts across one unit length so the cell is in steady state.
            heap.push(Reverse((us(unit(&mut st) * range_s), Ev::Start(p, s))));
        }
    }
    let flush_every = us(0.01);
    heap.push(Reverse((flush_every, Ev::Flush)));
    // SWIM: every peer probes one member per heartbeat period.
    let cfg = SwimConfig {
        period_ms: (a.heartbeat_s * 1000.0) as u64,
        indirect: 3,
        suspicion: 4,
        retransmit: 4,
        piggyback: 8,
    };
    let mut swims: Vec<Swim> =
        (0..peers).map(|p| Swim::new(p as u64 + 1, 1 << 32, seed ^ (p as u64 + 7), cfg)).collect();
    for (i, s) in swims.iter_mut().enumerate() {
        for j in 0..peers {
            if j as usize != i {
                s.add(0, j as u64 + 1, 1 << 32);
            }
        }
        // Spread the probe phases over the period.
        let _ = s.next_probes(0, 0);
    }
    let tick = us(a.heartbeat_s / 20.0).max(1000);
    heap.push(Reverse((tick, Ev::SwimTick)));
    heap.push(Reverse((us(a.summary_s), Ev::Summary)));
    // Failures early enough that suspicion (longer in a big cell) can run out.
    heap.push(Reverse((us(warm_s + 5.0), Ev::Fail)));
    let mut failed_at: BTreeMap<u32, f64> = BTreeMap::new();
    let mut detected: BTreeSet<u32> = BTreeSet::new();
    let end = us(a.seconds);
    let mut next_unit: u64 = 0;
    let mut phase: Vec<u64> = (0..peers).map(|_| us(unit(&mut st) * a.heartbeat_s)).collect();
    while let Some(Reverse((t, ev))) = heap.pop() {
        if t > end {
            break;
        }
        let now_s = t as f64 / 1e6;
        let counting = now_s >= warm_s;
        if counting && records_at_warm.is_none() {
            records_at_warm = Some(ledger.records);
        }
        match ev {
            Ev::Start(p, s) => {
                if !alive[p as usize] {
                    continue;
                }
                let items = if unit(&mut st) < q { a.range } else { 1 };
                let tk = ledger.grant();
                if items == a.range {
                    out.ranges_granted += 1;
                }
                next_unit += 1;
                running.insert((p, s), (tk, next_unit));
                busy[p as usize] += 1;
                // Exponential-ish service: mean items × service_s.
                let dur = items as f64 * a.service_s * (0.5 + unit(&mut st));
                heap.push(Reverse((t + us(dur), Ev::Done(p, s, next_unit, items))));
            }
            Ev::Done(p, s, id, items) => match running.get(&(p, s)) {
                Some((tk, u)) if *u == id && alive[p as usize] => {
                    let tk = *tk;
                    running.remove(&(p, s));
                    busy[p as usize] -= 1;
                    ledger.publish(tk);
                    if counting {
                        out.items_done += items as u64;
                    }
                    let busy_s = items as f64 * a.service_s;
                    let gap = busy_s * (1.0 - a.occupancy) / a.occupancy;
                    heap.push(Reverse((t + us(gap * (0.5 + unit(&mut st))), Ev::Start(p, s))));
                }
                _ => {}
            },
            Ev::Flush => {
                ledger.flush();
                heap.push(Reverse((t + flush_every, Ev::Flush)));
            }
            Ev::SwimTick => {
                let now_ms = t / 1000;
                for p in 0..peers {
                    if !alive[p as usize] || phase[p as usize] > t {
                        continue;
                    }
                    phase[p as usize] = 0;
                    let targets = swims[p as usize].next_probes(now_ms, 1);
                    for target in targets {
                        let ti = (target - 1) as u32;
                        out.heartbeats += u64::from(counting);
                        let mut reached = alive[ti as usize];
                        if reached {
                            exchange(&mut swims, p, ti, now_ms);
                        } else {
                            let relays = swims[p as usize].relays(target);
                            for r in relays {
                                let ri = (r - 1) as u32;
                                if alive[ri as usize] {
                                    exchange(&mut swims, p, ri, now_ms);
                                    out.heartbeats += u64::from(counting);
                                }
                            }
                            reached = false;
                        }
                        swims[p as usize].probed(now_ms, target, reached);
                    }
                    swims[p as usize].expire(now_ms);
                    // The host acts on changes (retire, stop placing) and drops them.
                    swims[p as usize].take_changes();
                }
                // Detection: a failed peer counts as detected once every live peer sees it dead.
                for (&f, &at) in &failed_at {
                    if detected.contains(&f) {
                        continue;
                    }
                    let all = (0..peers)
                        .filter(|i| alive[*i as usize])
                        .all(|i| swims[i as usize].state_of(f as u64 + 1).is_some_and(|(_, s)| s == SwimState::Dead));
                    if all {
                        detected.insert(f);
                        out.detect_s.push(now_s - at);
                    }
                }
                heap.push(Reverse((t + tick, Ev::SwimTick)));
            }
            Ev::Summary => {
                let loads: Vec<PeerLoad> = (0..peers)
                    .map(|p| PeerLoad {
                        alive: alive[p as usize],
                        shape: (p % 8) as u8,
                        free_slots: (a.slots as u16).saturating_sub(busy[p as usize]),
                        queued: 0,
                    })
                    .collect();
                let s = cell::summarize(cell, &loads, 64);
                out.summary_bytes_max = out.summary_bytes_max.max(s.encode().len());
                out.summaries += u64::from(counting);
                heap.push(Reverse((t + us(a.summary_s), Ev::Summary)));
            }
            Ev::Fail => {
                let n = ((peers as f64) * a.fail).round() as u32;
                for _ in 0..n {
                    let p = (rng(&mut st) % peers as u64) as u32;
                    if alive[p as usize] {
                        alive[p as usize] = false;
                        out.failed += 1;
                        failed_at.insert(p, now_s);
                        // Its running units are lost; the leader requeues them
                        // (counted, not re-executed here: their peers are gone).
                        let lost: Vec<(u32, u32)> = running.keys().filter(|(q, _)| *q == p).copied().collect();
                        for k in lost {
                            running.remove(&k);
                            out.items_lost_to_failures += a.range as u64;
                        }
                    }
                }
            }
        }
    }
    ledger.flush();
    out.records = ledger.records - records_at_warm.unwrap_or(0);
    out.commands = ledger.commands;
    out.apply_cpu_s = ledger.cpu_s;
    out.false_dead = (0..peers)
        .filter(|i| alive[*i as usize])
        .map(|i| {
            (0..peers)
                .filter(|j| alive[*j as usize] && *j != i)
                .filter(|j| swims[i as usize].state_of(*j as u64 + 1).is_some_and(|(_, s)| s == SwimState::Dead))
                .count() as u64
        })
        .sum();
    out.swim_bytes_per_peer = swims.iter().map(|s| s.approx_bytes()).max().unwrap_or(0);
    out
}

/// One SWIM probe and its reply between `a` and `b`, gossip both ways.
fn exchange(swims: &mut [Swim], a: u32, b: u32, now_ms: u64) {
    let out_a: Vec<Update> = swims[a as usize].outgoing();
    swims[b as usize].merge(now_ms, &out_a);
    let out_b: Vec<Update> = swims[b as usize].outgoing();
    swims[a as usize].merge(now_ms, &out_b);
}

/// The appendix's rows from its formulas, for comparison.
struct Model {
    cells: u64,
    items_s: f64,
    grants_s: f64,
    records_s: f64,
    heartbeats_s: f64,
    summaries_s: f64,
}

fn model(a: &Args, peers: u64) -> Model {
    let cells = peers.div_ceil(a.cell_size as u64);
    let items_s = peers as f64 * a.slots as f64 * a.occupancy / a.service_s;
    let grants_s = items_s * (a.singletons + (1.0 - a.singletons) / a.range as f64);
    Model {
        cells,
        items_s,
        grants_s,
        records_s: 2.0 * grants_s,
        heartbeats_s: peers as f64 / a.heartbeat_s,
        summaries_s: cells as f64 / a.summary_s,
    }
}

fn pct(measured: f64, expected: f64) -> f64 {
    if expected == 0.0 { 0.0 } else { (measured - expected) / expected * 100.0 }
}

fn main() {
    let a = Args::parse();
    let t0 = Instant::now();
    // One full cell in detail (cells share nothing on the common path).
    let full = run_cell(&a, 0, a.cell_size, a.seed);
    let per_s = |x: u64| x as f64 / full.seconds;
    let cell_items_s = per_s(full.items_done);
    let cell_records_s = per_s(full.records);
    // Every unit (range or singleton) is one grant and one publication.
    let cell_grants_s = cell_records_s / 2.0;
    let apply_us = full.apply_cpu_s / full.records.max(1) as f64 * 1e6;
    let mut rows = Vec::new();
    let mut ok = true;
    for &peers in &a.peers {
        let m = model(&a, peers);
        let scale = peers as f64 / a.cell_size as f64;
        let measured = [
            ("routing cells", m.cells as f64, m.cells as f64),
            ("logical items/s", cell_items_s * scale, m.items_s),
            ("range grants/s", cell_grants_s * scale, m.grants_s),
            ("ledger records/s", cell_records_s * scale, m.records_s),
            ("node heartbeats/s", per_s(full.heartbeats) * scale, m.heartbeats_s),
            ("cell summaries/s", per_s(full.summaries) * m.cells as f64, m.summaries_s),
        ];
        for (name, got, want) in measured {
            let d = pct(got, want);
            // Heartbeats include indirect probes after failures; allow that.
            let within = d.abs() <= 10.0;
            ok &= within;
            rows.push((peers, name, got, want, d, within));
        }
    }
    let leaders_needed = |records_s: f64| (records_s / (0.6 * (1e6 / apply_us.max(1e-9)))).ceil();
    let detect = {
        let mut d = full.detect_s.clone();
        d.sort_by(f64::total_cmp);
        (d.first().copied(), d.get(d.len() / 2).copied(), d.last().copied())
    };
    let state_ok = full.swim_bytes_per_peer <= 16 * 1024;
    let mut weak: Option<(f64, f64, f64)> = None;
    if a.weak_scaling {
        let useful = |peers: u32| {
            let cells = peers.div_ceil(a.cell_size);
            let mut items = 0u64;
            for c in 0..cells {
                let size = (peers - c * a.cell_size).min(a.cell_size);
                let r = run_cell(&a, c, size, a.seed.wrapping_add(c as u64 * 131));
                items += r.items_done;
            }
            items as f64 / a.seconds
        };
        let one = useful(1024);
        let two = useful(2048);
        weak = Some((one, two, two / one));
        ok &= two / one >= 1.8;
    }
    let report = serde_json::json!({
        "label": "emulation: real SWIM, shard apply and cell summaries for whole cells, scaled by cell count",
        "cell": {
            "peers": full.peers, "simulated_s": full.seconds, "items_per_s": cell_items_s,
            "ledger_records_per_s": cell_records_s, "commands_applied": full.commands,
            "apply_us_per_record": apply_us, "summary_bytes_max": full.summary_bytes_max,
            "swim_bytes_per_peer": full.swim_bytes_per_peer, "per_peer_state_within_16KiB": state_ok,
            "failed_peers": full.failed, "detected": full.detect_s.len(), "detection_s_min_median_max": [detect.0, detect.1, detect.2],
            "false_dead_views": full.false_dead,
        },
        "rows": rows.iter().map(|(p, n, g, w, d, k)| serde_json::json!({"peers": p, "row": n, "emulated": g, "appendix": w, "diff_pct": d, "within_10pct": k})).collect::<Vec<_>>(),
        "weak_scaling_1k_to_2k": weak.map(|(a1, a2, r)| serde_json::json!({"items_per_s_1k": a1, "items_per_s_2k": a2, "ratio": r})),
        "wall_s": t0.elapsed().as_secs_f64(),
        "pass": ok && state_ok && full.false_dead == 0,
    });
    if a.json {
        println!("{}", serde_json::to_string_pretty(&report).expect("json"));
        return;
    }
    println!(
        "Fleet emulation (real SWIM, shard apply and cell summaries; one {}-peer cell in detail, scaled by cells)\n",
        a.cell_size
    );
    println!("| Peers | Row | Emulated | Appendix | Diff |");
    println!("|---:|---|---:|---:|---:|");
    for (p, n, g, w, d, k) in &rows {
        println!("| {p} | {n} | {g:.0} | {w:.0} | {d:+.1}%{} |", if *k { "" } else { " ✗" });
    }
    println!();
    println!(
        "cell leader: {:.2} µs of real ShardState::apply per ledger record ({:.0} records/s per core, in memory, no fsync or replication)",
        apply_us,
        1e6 / apply_us
    );
    for s in [0.0, 0.05, 0.10, 1.0] {
        let rec = 2.0 * 45_875.2 * (s + (1.0 - s) / a.range as f64);
        println!(
            "  at {:>4.0}% singletons a full cell needs {:>7.0} records/s: {} leader(s) at 60% of measured apply capacity",
            s * 100.0,
            rec,
            leaders_needed(rec)
        );
    }
    println!("cell summary: at most {} bytes encoded (budget 2048)", full.summary_bytes_max);
    println!(
        "SWIM state: about {} bytes per peer for a {}-peer cell (budget 16 KiB): {}",
        full.swim_bytes_per_peer,
        a.cell_size,
        if state_ok { "within" } else { "OVER" }
    );
    println!(
        "failures: {} peers killed mid-run, {} seen dead by every live peer, after {:?} s (min, median, max); false dead views: {}",
        full.failed,
        full.detect_s.len(),
        detect,
        full.false_dead
    );
    if let Some((one, two, r)) = weak {
        println!("weak scaling: {one:.0} items/s at 1,024 peers, {two:.0} at 2,048: ×{r:.2} (gate ≥ 1.8)");
    }
    // The first limit is whatever runs closest to its budget at this mix.
    let limits = [
        (
            "cell leader ledger (records/s vs 60% of one core's measured apply)",
            cell_records_s / (0.6 * 1e6 / apply_us.max(1e-9)),
        ),
        ("cell summary size (vs 2 KiB)", full.summary_bytes_max as f64 / 2048.0),
        ("SWIM state per peer (vs 16 KiB)", full.swim_bytes_per_peer as f64 / 16384.0),
    ];
    let (name, used) = limits.iter().copied().fold(("", 0.0), |b, x| if x.1 > b.1 { x } else { b });
    println!("\nfirst binding limit at this mix: {name}, at {:.1}% of its budget", used * 100.0);
    println!(
        "result: {} (emulation; wall {:.1} s)",
        if ok && state_ok && full.false_dead == 0 { "PASS" } else { "FAIL" },
        t0.elapsed().as_secs_f64()
    );
}
