//! What more capacity would start sooner on this peer (pb::Demand, carried in
//! its load report and its ClusterNode): queued calls by shape and expected
//! run time, actors waiting for CPUs, and when the cores in use free up. A
//! provisioner adds these up across peers to decide whether new peers would
//! be busy before the backlog clears on its own.
//!
//! Each queued attempt is in one peer's queue at a time (a give-back takes it
//! out before its group places it again), so summaries add up without
//! counting a call twice; one in transit is missed for a moment. Calls still
//! waiting on their inputs are not queued anywhere yet, and an actor's calls
//! go to its mailbox: neither is demand for more peers.

use super::{Core, Work};
use chimaera_core::pb;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Shapes per summary; the rest fold into the last one kept.
const MAX_SHAPES: usize = 16;
/// Upper bounds (seconds) of the release buckets; after them, "not known".
const RELEASE_BUCKETS: [f64; 5] = [1.0, 4.0, 16.0, 64.0, 256.0];
/// A call of a function never measured: assumed to take this long at least
/// (as placement assumes).
const COLD_S: f64 = 1.0;
/// Queued calls looked at per summary at most: a longer queue is sampled
/// evenly, each call looked at standing for those around it. A summary goes
/// out with every load report, and a million queued calls must not stall the
/// core that often.
const MAX_SCAN: usize = 8192;
/// An actor seen waiting for CPUs this recently still counts.
pub(super) const ACTOR_WAIT_FRESH: Duration = Duration::from_secs(10);

/// One queued call, as the summary counts it.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Queued {
    /// Queued calls this one stands for (1 unless the queue was sampled).
    pub calls: u32,
    pub cpus: f64,
    pub memory: u64,
    /// Estimated wall seconds; for a cold call, a floor.
    pub wall_s: f64,
    pub cold: bool,
    pub waited_s: f64,
    /// Hard placement labels or accelerators: a peer like the others may
    /// not run it, so it is counted apart.
    pub labels: bool,
    pub devices: bool,
}

/// Cores in use and when they free up (None: not known).
#[derive(Clone, Copy, Debug)]
pub(super) struct Held {
    pub cores: f64,
    pub left_s: Option<f64>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ShapeKey {
    centi_cpus: u64,
    memory: u64,
    duration: i32,
}

/// The summary of `queued` calls, `held` cores and `actors` waiting (their CPUs).
pub(super) fn summarize(
    queued: impl IntoIterator<Item = Queued>,
    held: impl IntoIterator<Item = Held>,
    actors: impl IntoIterator<Item = f64>,
    started: (u64, f64),
) -> pb::Demand {
    let mut d = pb::Demand { started_total: started.0, started_core_s_total: started.1, ..Default::default() };
    let mut shapes: BTreeMap<ShapeKey, pb::DemandShape> = BTreeMap::new();
    for q in queued {
        d.oldest_ready_s = d.oldest_ready_s.max(q.waited_s);
        let n = q.calls.max(1);
        if q.labels {
            d.excluded_labels += n;
            continue;
        }
        if q.devices {
            d.excluded_devices += n;
            continue;
        }
        let memory = if q.memory == 0 { 0 } else { q.memory.next_power_of_two() };
        // Calls of one shape within a factor of 4 of each other's time share a row.
        let duration = q.wall_s.max(1e-3).log(4.0).ceil() as i32;
        let key = ShapeKey { centi_cpus: (q.cpus * 100.0).round() as u64, memory, duration };
        let s = shapes.entry(key).or_insert_with(|| pb::DemandShape { cpus: q.cpus, memory, ..Default::default() });
        s.queued += n;
        s.wall_s += q.wall_s * f64::from(n);
        s.cold += if q.cold { n } else { 0 };
    }
    for cpus in actors {
        let key = ShapeKey { centi_cpus: (cpus * 100.0).round() as u64, memory: 0, duration: i32::MAX };
        shapes.entry(key).or_insert_with(|| pb::DemandShape { cpus, ..Default::default() }).actors += 1;
    }
    // Most core-seconds first; an actor holds its CPUs for its life, so it sorts first.
    let weight = |s: &pb::DemandShape| if s.actors > 0 { f64::INFINITY } else { s.cpus * s.wall_s };
    let mut rows: Vec<pb::DemandShape> = shapes.into_values().collect();
    rows.sort_by(|a, b| weight(b).total_cmp(&weight(a)));
    if rows.len() > MAX_SHAPES {
        let rest = rows.split_off(MAX_SHAPES - 1);
        let mut last = pb::DemandShape::default();
        for r in rest {
            // The largest shape among them, so the fold never fits where its calls would not.
            last.cpus = last.cpus.max(r.cpus);
            last.memory = last.memory.max(r.memory);
            last.queued += r.queued;
            last.wall_s += r.wall_s;
            last.cold += r.cold;
            last.actors += r.actors;
        }
        rows.push(last);
    }
    d.shapes = rows;
    let mut buckets = [0.0f64; RELEASE_BUCKETS.len() + 1];
    for h in held {
        let i = h.left_s.map_or(RELEASE_BUCKETS.len(), |left| {
            RELEASE_BUCKETS.iter().position(|b| left <= *b).unwrap_or(RELEASE_BUCKETS.len())
        });
        buckets[i] += h.cores.max(0.0);
    }
    d.releases = buckets
        .iter()
        .enumerate()
        .filter(|(_, cores)| **cores > 0.0)
        .map(|(i, cores)| pb::DemandRelease { upper_s: RELEASE_BUCKETS.get(i).copied().unwrap_or(0.0), cores: *cores })
        .collect();
    d
}

/// The function last looked up in a demand scan (fn digest, profile digest,
/// method), with its estimate and, never measured, its floor.
struct Looked<'a> {
    id: (&'a [u8], &'a [u8], &'a str),
    est: Option<f64>,
    floor: f64,
}

impl Core {
    /// This peer's demand summary, now.
    pub(super) fn demand(&self) -> pb::Demand {
        let n = self.queue.len();
        let step = n.div_ceil(MAX_SCAN).max(1);
        // A function never measured when its calls were reserved may be by
        // now; consecutive queued calls are mostly one function (a map), so
        // look it up once per run of them.
        let mut last: Option<Looked> = None;
        let mut queued = Vec::with_capacity(n.min(MAX_SCAN));
        for i in (0..n).step_by(step) {
            let w = &self.queue[i];
            if w.policy.launch.is_some() {
                continue; // a gang rank or bundle: its slot is already held for it
            }
            let items = f64::from(w.policy.items.max(1));
            let (est, cold_floor) = if w.est_s > 0.0 {
                (Some(w.est_s), 0.0)
            } else {
                let id = (w.spec.fn_digest.as_slice(), w.policy.profile_digest.as_slice(), w.spec.method.as_str());
                match &last {
                    Some(l) if l.id == id => (l.est, l.floor),
                    _ => {
                        let est = self.profiles.summary(&w.key()).map(|s| s.wall_p50_s).filter(|s| *s > 0.0);
                        let floor = if est.is_none() { self.cold_floor(w) } else { 0.0 };
                        last = Some(Looked { id, est, floor });
                        (est, floor)
                    }
                }
            };
            queued.push(Queued {
                calls: step.min(n - i) as u32,
                cpus: w.cpus,
                memory: w.mem,
                wall_s: est.map_or(cold_floor * items, |e| e * items),
                cold: est.is_none(),
                waited_s: w.ready_at.elapsed().as_secs_f64(),
                labels: !w.policy.require.is_empty(),
                devices: w.policy.gpus > 0.0 || !w.policy.device.is_empty(),
            });
        }
        let mut held = Vec::with_capacity(self.running.len() + 1);
        let mut known = 0.0;
        for r in self.running.iter().filter(|r| !r.blocked) {
            let w = &r.items[0];
            let calls: f64 = r.items.iter().map(|w| f64::from(w.policy.items.max(1))).sum();
            let elapsed = r.started.elapsed().as_secs_f64();
            let est = (w.est_s > 0.0).then_some(w.est_s * calls);
            // Past its estimate, or never measured: as long again as it has run.
            let left = est.filter(|e| *e > elapsed).map_or(elapsed.max(0.1), |e| e - elapsed);
            held.push(Held { cores: r.cpus, left_s: Some(left) });
            known += r.cpus;
        }
        // The rest of what is in use (actors' CPUs, held slots, calls waiting
        // on others that keep theirs) frees at no time anyone can say.
        held.push(Held { cores: (self.cpu_used - known).max(0.0), left_s: None });
        let actors = self
            .actor_demand
            .values()
            .filter(|(_, seen)| seen.elapsed() < ACTOR_WAIT_FRESH)
            .map(|(cpus, _)| *cpus)
            .collect::<Vec<_>>();
        summarize(queued, held, actors, self.started)
    }

    /// The least one call (one item of a chunk) of a function never
    /// measured will take: as long as its longest running call has run so
    /// far, or COLD_S.
    fn cold_floor(&self, w: &Work) -> f64 {
        let key = w.key();
        self.running
            .iter()
            .filter(|r| r.items[0].key() == key)
            .map(|r| {
                let calls: f64 = r.items.iter().map(|w| f64::from(w.policy.items.max(1))).sum();
                r.started.elapsed().as_secs_f64() / calls
            })
            .fold(COLD_S, f64::max)
    }

    /// Counts attempts as they start, for the arrival rate a provisioner
    /// derives (Δqueued + Δstarted), with the estimate `reserve` took, so
    /// the dispatch path does no more lookups.
    pub(super) fn note_started(&mut self, items: &[Work]) {
        for w in items {
            let est = if w.est_s > 0.0 { w.est_s } else { COLD_S };
            self.started.0 += 1;
            self.started.1 += w.cpus * est * f64::from(w.policy.items.max(1));
        }
    }

    /// An actor that waits for a peer with its CPUs free (or no longer does).
    pub(super) fn note_actor_waiting(&mut self, actor: chimaera_core::ids::Id, cpus: Option<f64>) {
        match cpus {
            Some(c) => {
                self.actor_demand.insert(actor, (c, Instant::now()));
            }
            None => {
                self.actor_demand.remove(&actor);
            }
        }
        self.actor_demand.retain(|_, (_, seen)| seen.elapsed() < ACTOR_WAIT_FRESH * 6);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(cpus: f64, wall_s: f64) -> Queued {
        Queued { cpus, wall_s, ..Default::default() }
    }

    #[test]
    fn calls_group_by_shape_and_time_and_count_once() {
        let mut q = vec![call(1.0, 1.0); 40];
        q.push(Queued { cold: true, ..call(1.0, 1.0) });
        q.extend(vec![call(1.0, 60.0); 3]);
        q.push(Queued { memory: 3 << 20, ..call(2.0, 60.0) });
        let d = summarize(q, [], [], (7, 9.0));
        let rows: Vec<_> = d.shapes.iter().map(|s| (s.cpus, s.memory, s.queued, s.wall_s, s.cold)).collect();
        assert_eq!(rows, [(1.0, 0, 3, 180.0, 0), (2.0, 4 << 20, 1, 60.0, 0), (1.0, 0, 41, 41.0, 1)]);
        assert_eq!((d.started_total, d.started_core_s_total), (7, 9.0));
    }

    #[test]
    fn labelled_and_accelerator_calls_are_counted_apart() {
        let q = [Queued { labels: true, ..call(1.0, 1.0) }, Queued { devices: true, waited_s: 5.0, ..call(1.0, 1.0) }];
        let d = summarize(q, [], [], (0, 0.0));
        assert!(d.shapes.is_empty());
        assert_eq!((d.excluded_labels, d.excluded_devices, d.oldest_ready_s), (1, 1, 5.0));
    }

    #[test]
    fn waiting_actors_come_first_and_cores_in_use_fall_into_buckets() {
        let held = [
            Held { cores: 2.0, left_s: Some(0.5) },
            Held { cores: 1.0, left_s: Some(30.0) },
            Held { cores: 1.0, left_s: Some(1e6) },
            Held { cores: 3.0, left_s: None },
        ];
        let d = summarize([call(1.0, 100.0)], held, [4.0], (0, 0.0));
        assert_eq!((d.shapes[0].cpus, d.shapes[0].actors, d.shapes[0].queued), (4.0, 1, 0));
        let rel: Vec<_> = d.releases.iter().map(|r| (r.upper_s, r.cores)).collect();
        assert_eq!(rel, [(1.0, 2.0), (64.0, 1.0), (0.0, 4.0)], "past the last bucket is not known");
    }

    #[test]
    fn a_sampled_call_stands_for_the_calls_around_it() {
        let q = [Queued { calls: 1000, ..call(1.0, 2.0) }, Queued { calls: 5, labels: true, ..call(1.0, 1.0) }];
        let d = summarize(q, [], [], (0, 0.0));
        assert_eq!((d.shapes[0].queued, d.shapes[0].wall_s, d.excluded_labels), (1000, 2000.0, 5));
    }

    #[test]
    fn many_shapes_fold_into_the_largest() {
        let q: Vec<_> = (1..=40).map(|i| call(f64::from(i), 1.0)).collect();
        let d = summarize(q, [], [], (0, 0.0));
        assert_eq!(d.shapes.len(), MAX_SHAPES);
        let last = d.shapes.last().unwrap();
        assert_eq!(d.shapes.iter().map(|s| s.queued).sum::<u32>(), 40);
        assert_eq!(last.cpus, 25.0, "the folded row is as large as its largest call");
    }
}
