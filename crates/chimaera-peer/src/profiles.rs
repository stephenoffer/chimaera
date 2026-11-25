//! The peer's per-function profiles: samples from attempts that ran here,
//! priors from disk and from other peers, and the reservations and batch
//! sizes derived from them (A1, A4, A6; the rules are in
//! `chimaera_core::profile`).
//!
//! Safety changes apply at once: more memory, more CPU for a function that
//! uses more than its grant. A smaller CPU grant is canaried first
//! ([`CpuTrial`]); a revert backs off before the next attempt.

use chimaera_core::adapt::Verdict;
use chimaera_core::pb;
use chimaera_core::profile::{CpuTrial, Fit, MemEstimate, Sample, Stats, Summary, cpu_quantum};
use prost::Message;
use std::collections::BTreeMap;
use std::path::Path;

/// Profiles kept before the least recently used are dropped (bounded
/// retention, Ray lesson 6).
pub const MAX_PROFILES: usize = 20_000;
/// Largest auto-batched range (A1); the design's range clamp.
pub const MAX_BATCH: usize = 4096;
/// Target wall time of an auto-batched range (A1).
const BATCH_TARGET_S: f64 = 0.2;
/// Items faster than this are worth batching (A1).
const BATCH_BELOW_S: f64 = 0.05;
/// Most memory one batch may leave in its worker. Calls that keep memory after
/// they return (caches, leaks) are batched no deeper than this allows, so the
/// leak watch, which looks between batches, sees every few calls: 107 calls of
/// a 2 ms function that each kept 20 MiB left 2 GiB in one batch, more than a
/// small peer's pool, and the pressure kill blamed each call for all of it.
const BATCH_KEPT_BYTES: f64 = (64u64 << 20) as f64;
/// Memory kept per call below this is noise, not a reason to batch less.
const KEPT_NOISE: f64 = (1u64 << 20) as f64;
/// Local samples between CPU trials, doubled per consecutive revert.
const TRIAL_BACKOFF: u64 = 50;
/// A prior from older code counts as at most this many samples.
const PRIOR_FROM_OLD_CODE: u64 = 9;

/// A function (or actor method) by code digest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FnKey {
    pub digest: Vec<u8>,
    pub method: String,
}

impl FnKey {
    pub fn new(digest: &[u8], method: &str) -> FnKey {
        FnKey { digest: digest.to_vec(), method: method.to_string() }
    }

    fn scope(&self, name: &str) -> String {
        let hex: String = self.digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
        match (name.is_empty(), self.method.is_empty()) {
            (true, true) => format!("fn {hex}"),
            (true, false) => format!("fn {hex}.{}", self.method),
            _ => format!("{name} ({hex})"),
        }
    }
}

#[derive(Default)]
struct Entry {
    name: String,
    stats: Stats,
    /// Summary loaded from disk, and the latest from each other peer.
    disk: Option<Summary>,
    remotes: BTreeMap<u64, Summary>,
    last_used: u64,
    dirty: bool,
    /// Bumped on each local sample, so other peers get only what changed.
    version: u64,
    batch: usize,
    /// Memory each call leaves in its worker after returning (bytes, averaged).
    kept: f64,
    /// CPU grant learned (grown, or adopted by a canary); 0 = none.
    cpus: f64,
    trial: Option<CpuTrial>,
    trial_after: u64,
    reverts: u32,
}

impl Entry {
    fn refresh_prior(&mut self) {
        let mut prior = self.disk.clone();
        for s in self.remotes.values() {
            prior = Some(prior.map_or_else(|| s.clone(), |p| p.merge(s)));
        }
        self.stats.prior = prior;
    }
}

/// Where a reservation came from, for explanations.
#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    User,
    /// ch.auto(initial=...): the caller's starting value, before learning.
    Initial,
    Default,
    Learned(String),
    /// Running a CPU trial: this attempt is the canary (true) or control.
    Trial(bool),
    /// `adaptive="observe"`: what would have been reserved.
    Observed(String),
}

impl Source {
    /// One word for where the value came from (AttemptRecord.settings).
    pub fn kind(&self) -> &'static str {
        match self {
            Source::User => "user",
            Source::Initial => "initial",
            Source::Default => "default",
            Source::Learned(_) => "learned",
            Source::Trial(_) => "trial",
            Source::Observed(_) => "observed",
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Source::User => "set by the caller".into(),
            Source::Initial => "your initial setting".into(),
            Source::Default => "default".into(),
            Source::Learned(e) => format!("learned: {e}"),
            Source::Trial(true) => "trial: canary".into(),
            Source::Trial(false) => "trial: control".into(),
            Source::Observed(e) => format!("not applied (adaptive=\"observe\"): {e}"),
        }
    }
}

/// CPU and memory for one attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct Reservation {
    pub cpus: f64,
    pub cpus_source: Source,
    /// Bytes; 0 = unknown (admission probes instead).
    pub memory: u64,
    pub memory_source: Source,
    /// Some(arm) when this attempt belongs to a CPU trial.
    pub arm: Option<bool>,
    /// The function's median call time, seconds; 0 = never measured.
    pub est_s: f64,
}

/// What the caller asked for (from the frozen policy).
#[derive(Clone, Copy, Debug, Default)]
pub struct Request {
    pub cpus: f64,
    pub memory: u64,
    pub adaptive: pb::policy::Adaptive,
    /// `cpus` / `memory` is ch.auto's initial value: where learning starts,
    /// not a value it must keep.
    pub cpus_initial: bool,
    pub memory_initial: bool,
    /// ch.auto(min=, max=) on what is learned; 0 = none. Core::reserve
    /// applies them to the grant; here they shape what observe reports.
    pub cpus_min: f64,
    pub cpus_max: f64,
    pub memory_min: u64,
    pub memory_max: u64,
}

impl Request {
    pub fn of(p: &pb::Policy) -> Request {
        Request {
            cpus: p.cpus,
            memory: p.memory,
            adaptive: p.adaptive(),
            cpus_initial: p.cpus_initial,
            memory_initial: p.memory_initial,
            cpus_min: p.cpus_min,
            cpus_max: p.cpus_max,
            memory_min: p.memory_min,
            memory_max: p.memory_max,
        }
    }

    /// `x` within `lo..hi`, where 0 is no bound.
    fn clip<T: PartialOrd + Default + Copy>(x: T, lo: T, hi: T) -> T {
        let zero = T::default();
        let x = if lo > zero && x < lo { lo } else { x };
        if hi > zero && x > hi { hi } else { x }
    }

    fn cpus_pinned(&self) -> bool {
        self.cpus > 0.0 && !self.cpus_initial
    }

    fn memory_pinned(&self) -> bool {
        self.memory > 0 && !self.memory_initial
    }

    /// Learned CPU applies to this call: learning is on and the caller did
    /// not pin a value.
    pub fn learns_cpus(&self) -> bool {
        self.adaptive == pb::policy::Adaptive::On && !self.cpus_pinned()
    }

    /// The CPU grant when learning has nothing to say (or may not apply):
    /// the initial value, else one core.
    fn cpus_unlearned(&self) -> (f64, Source) {
        if self.cpus > 0.0 { (self.cpus, Source::Initial) } else { (1.0, Source::Default) }
    }

    /// Memory when learning has nothing to say: the initial value, else
    /// none (admission probes instead).
    fn memory_unlearned(&self) -> (u64, Source) {
        if self.memory > 0 { (self.memory, Source::Initial) } else { (0, Source::Default) }
    }
}

#[derive(Default)]
pub struct Profiles {
    entries: BTreeMap<FnKey, Entry>,
    names: BTreeMap<String, FnKey>,
    tick: u64,
    version: u64,
    /// This peer's hardware class: profiles from other classes inform
    /// memory only (their CPU use does not carry over).
    pub hw_class: u64,
}

impl Profiles {
    fn entry(&mut self, key: &FnKey, name: &str) -> &mut Entry {
        self.tick += 1;
        if !self.entries.contains_key(key) {
            let mut e = Entry { name: name.to_string(), last_used: self.tick, ..Default::default() };
            // Edited code gets a new digest; start from the old version's
            // profile, counted as a handful of samples.
            if let Some(old) = self.names.get(name).filter(|k| *k != key).and_then(|k| self.entries.get(k)) {
                let mut s = old.stats.summary();
                s.n = s.n.min(PRIOR_FROM_OLD_CODE);
                e.disk = Some(s);
                e.refresh_prior();
            }
            self.entries.insert(key.clone(), e);
            if self.entries.len() > MAX_PROFILES + MAX_PROFILES / 16 {
                self.evict();
            }
        }
        if !name.is_empty() {
            self.names.insert(name.to_string(), key.clone());
        }
        let tick = self.tick;
        let e = self.entries.get_mut(key).expect("inserted above");
        e.last_used = tick;
        if e.name.is_empty() {
            e.name = name.to_string();
        }
        e
    }

    fn evict(&mut self) {
        let mut ages: Vec<(u64, FnKey)> = self.entries.iter().map(|(k, e)| (e.last_used, k.clone())).collect();
        ages.sort();
        for (_, k) in ages.into_iter().take(self.entries.len() - MAX_PROFILES) {
            if let Some(e) = self.entries.remove(&k)
                && self.names.get(&e.name) == Some(&k)
            {
                self.names.remove(&e.name);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn summary(&self, key: &FnKey) -> Option<Summary> {
        self.entries.get(key).map(|e| e.stats.summary())
    }

    /// The reservation for the next attempt of `key` reading `arg_bytes`.
    pub fn reserve(&mut self, key: &FnKey, name: &str, arg_bytes: u64, req: Request) -> Reservation {
        use pb::policy::Adaptive;
        let e = self.entry(key, name);
        let summary = e.stats.summary();
        let learned = || format!("{} calls, {:.2} cores on average", summary.n, summary.cores_mean);
        // Only calls learning applies to take part in a CPU trial: an
        // observed or frozen call neither runs an arm nor draws one (which
        // would shift the next call's arm).
        let (cpus, cpus_source, arm) = if req.cpus_pinned() {
            (req.cpus, Source::User, None)
        } else if req.adaptive != Adaptive::On {
            let (c, source) = req.cpus_unlearned();
            match req.adaptive {
                // What learning would reserve, said but not applied.
                Adaptive::Observe if e.cpus > 0.0 => {
                    let would = Request::clip(e.cpus, req.cpus_min, req.cpus_max);
                    (c, Source::Observed(format!("would reserve {would} cores: {}", learned())), None)
                }
                _ => (c, source, None),
            }
        } else if let Some(t) = e.trial.as_mut() {
            let (c, canary) = t.assign();
            (c, Source::Trial(canary), Some(canary))
        } else if e.cpus > 0.0 {
            (e.cpus, Source::Learned(learned()), None)
        } else {
            let (c, source) = req.cpus_unlearned();
            (c, source, None)
        };
        let (memory, memory_source) = if req.memory_pinned() {
            (req.memory, Source::User)
        } else {
            let unlearned = req.memory_unlearned();
            match (req.adaptive, summary.memory_for(arg_bytes)) {
                (Adaptive::Frozen, _) | (_, None) => unlearned,
                (Adaptive::Observe, Some(m)) => {
                    let would = Request::clip(m.bytes, req.memory_min, req.memory_max);
                    (
                        unlearned.0,
                        Source::Observed(format!("would reserve {} MiB: {}", would.div_ceil(1 << 20), m.evidence)),
                    )
                }
                (Adaptive::On, Some(m)) => (m.bytes, Source::Learned(m.evidence)),
            }
        };
        Reservation { cpus, cpus_source, memory, memory_source, arm, est_s: summary.wall_p50_s.max(0.0) }
    }

    /// The CPU grant for queued work of `key` about to start, when learning
    /// has something to say: a running trial's next arm, or a learned grant.
    /// `None`: keep what the work has (the caller's value, or the default).
    /// `arm` is the trial arm the work already holds: it keeps it while that
    /// trial runs. Drawing again on every dispatch pass would turn a control
    /// that does not fit yet into a canary that does, starving the control.
    pub fn cpus_now(&mut self, key: &FnKey, req: Request, arm: Option<bool>) -> Option<(f64, Source, Option<bool>)> {
        if !req.learns_cpus() {
            return None;
        }
        let e = self.entries.get_mut(key)?;
        if let Some(t) = e.trial.as_mut() {
            if let Some(canary) = arm {
                return Some((if canary { t.to } else { t.from }, Source::Trial(canary), Some(canary)));
            }
            let (c, canary) = t.assign();
            return Some((c, Source::Trial(canary), Some(canary)));
        }
        (e.cpus > 0.0).then(|| {
            let s = e.stats.summary();
            (e.cpus, Source::Learned(format!("{} calls, {:.2} cores on average", s.n, s.cores_mean)), None)
        })
    }

    /// Memory for a call of `key` reading `arg_bytes`, if its profile has an
    /// estimate yet (read-only: no trial step, no recency update).
    pub fn memory_estimate(&self, key: &FnKey, arg_bytes: u64) -> Option<MemEstimate> {
        self.entries.get(key)?.stats.summary().memory_for(arg_bytes)
    }

    /// Map items per range for `key` (A1); 1 = no batching.
    pub fn batch(&self, key: &FnKey) -> usize {
        self.entries.get(key).map_or(1, |e| {
            let cap = if e.kept >= KEPT_NOISE { (BATCH_KEPT_BYTES / e.kept) as usize } else { usize::MAX };
            e.batch.clamp(1, cap.max(1))
        })
    }

    /// What one call of `key` left in its worker after returning (a batch's
    /// growth over its calls): batches of calls that keep memory stay small.
    pub fn note_kept(&mut self, key: &FnKey, per_call: u64) {
        if let Some(e) = self.entries.get_mut(key) {
            e.kept = 0.5 * e.kept + 0.5 * per_call as f64;
        }
    }

    /// Record a finished attempt and return the decisions it caused.
    /// `kills_recent`: memory kills happened on this peer in the last window.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        key: &FnKey,
        name: &str,
        sample: Sample,
        arm: Option<bool>,
        req: Request,
        kills_recent: bool,
        now_s: f64,
        at_unix_ns: u64,
    ) -> Vec<pb::Decision> {
        let scope = key.scope(name);
        self.version += 1;
        let version = self.version;
        let e = self.entry(key, name);
        e.stats.push(sample);
        e.dirty = true;
        e.version = version;
        let mut out = Vec::new();
        let decision =
            |controller: &str, knob: &str, old: String, new: String, outcome: &str, evidence, reason| pb::Decision {
                at_unix_ns,
                controller: controller.into(),
                knob: knob.into(),
                scope: scope.clone(),
                old_value: old,
                new_value: new,
                outcome: outcome.into(),
                evidence,
                reason,
            };
        // A CPU trial in progress takes this sample into its window.
        if let (Some(canary), Some(t)) = (arm, e.trial.as_mut()) {
            t.observe(canary, sample.wall_s, sample.oom);
            if let Some(v) = t.tick(now_s) {
                let (from, to, pairs) = (t.from, t.to, t.pairs());
                e.trial = None;
                match v {
                    Verdict::Adopt { mean_gain, se } => {
                        e.cpus = to;
                        e.reverts = 0;
                        out.push(decision(
                            "cpu-sizing",
                            "cpus",
                            from.to_string(),
                            to.to_string(),
                            "adopted",
                            format!(
                                "{pairs} paired windows: {:+.0}% completions per reserved core (se {:.0}%)",
                                mean_gain * 100.0,
                                se * 100.0
                            ),
                            "calls mostly wait; a smaller grant runs more of them on the same cores".into(),
                        ));
                    }
                    Verdict::Revert { reason, mean_gain } => {
                        e.reverts += 1;
                        e.trial_after = e.stats.total + (TRIAL_BACKOFF << e.reverts.min(6));
                        out.push(decision(
                            "cpu-sizing",
                            "cpus",
                            from.to_string(),
                            to.to_string(),
                            "reverted",
                            format!("{pairs} paired windows, gain {:+.0}%", mean_gain * 100.0),
                            reason,
                        ));
                    }
                    Verdict::Continue => {}
                }
            }
        }
        let s = e.stats.summary();
        let current = if e.cpus > 0.0 { e.cpus } else { req.cpus_unlearned().0 };
        let learning = req.learns_cpus();
        // More CPU at once for a function that already uses it (A5).
        if learning
            && e.trial.is_none()
            && let Some(c) = s.cpu_grow().map(|c| Request::clip(c, req.cpus_min, req.cpus_max))
            && c > current
        {
            out.push(decision(
                "cpu-sizing",
                "cpus",
                current.to_string(),
                c.to_string(),
                "adopted",
                format!("{} calls used {:.2} cores on average with {} granted", s.n, s.cores_mean, s.cpus_granted),
                "calls use more CPU than granted; reserving it stops the peer overpacking".into(),
            ));
            e.cpus = c;
        }
        // Propose a smaller grant, to be canaried.
        if learning
            && e.trial.is_none()
            && e.stats.total >= e.trial_after
            // Within the caller's ch.auto bounds: a canary the peer would
            // clip runs at another grant than the trial measures.
            && let Some(to) = s.cpu_shrink(kills_recent).map(|c| Request::clip(c, req.cpus_min, req.cpus_max))
            && to < current
        {
            e.trial = Some(CpuTrial::new(current, to, s.wall_p50_s, now_s));
            out.push(decision(
                "cpu-sizing",
                "cpus",
                current.to_string(),
                to.to_string(),
                "pending",
                format!("{} calls used {:.2} cores on average (CV {:.2})", s.n, s.cores_mean, s.cores_cv),
                if s.io_bound() {
                    "calls mostly wait on I/O; canarying a smaller grant".into()
                } else {
                    "calls use well under their grant; canarying a smaller one".into()
                },
            ));
        }
        // Granularity (A1): batch fast items toward BATCH_TARGET_S per range.
        // Learned batching applies only where learning does (Core::take_batch),
        // so only those calls move it.
        if s.n >= 3 && req.adaptive == pb::policy::Adaptive::On {
            let p50 = s.wall_p50_s.max(1e-6);
            let want =
                if p50 < BATCH_BELOW_S { ((BATCH_TARGET_S / p50).ceil() as usize).clamp(1, MAX_BATCH) } else { 1 };
            let old = e.batch.max(1);
            // Up by doubling (one step per sample), down at once.
            let next = if want > old { (old * 2).min(want) } else { want };
            if next != old {
                e.batch = next;
                out.push(decision(
                    "granularity",
                    "range_size",
                    old.to_string(),
                    next.to_string(),
                    "adopted",
                    format!("{} calls, median {:.3} ms", s.n, p50 * 1e3),
                    format!(
                        "calls under {} ms are batched toward {} ms per range",
                        BATCH_BELOW_S * 1e3,
                        BATCH_TARGET_S * 1e3
                    ),
                ));
            }
        }
        out
    }

    /// Prior from another peer's summary of the same function. From other
    /// hardware only memory carries over.
    pub fn merge_remote(&mut self, node: u64, p: &pb::ProfileSummary) {
        let (key, name, mut summary, _) = from_pb(p);
        if p.hw_class != self.hw_class {
            (summary.cores_mean, summary.cores_cv, summary.cpus_granted) = (0.0, 0.0, 0.0);
        }
        let e = self.entry(&key, &name);
        e.remotes.insert(node, summary);
        e.refresh_prior();
    }

    /// Profiles learned here since `since` (a version this returned before),
    /// oldest change first, at most `limit`; and the version to ask from next.
    pub fn changed_since(&self, since: u64, limit: usize) -> (Vec<pb::ProfileSummary>, u64) {
        let mut changed: Vec<(u64, &FnKey, &Entry)> =
            self.entries.iter().filter(|(_, e)| e.version > since).map(|(k, e)| (e.version, k, e)).collect();
        changed.sort_by_key(|c| c.0);
        changed.truncate(limit);
        let next = changed.last().map_or(since, |c| c.0);
        let out =
            changed.iter().map(|(_, k, e)| to_pb(k, &e.name, &e.stats.summary(), e.cpus, self.hw_class)).collect();
        (out, next)
    }

    /// The CPU grant learned for `key`, 0 if none (read-only, for placement).
    pub fn learned_cpus(&self, key: &FnKey) -> f64 {
        self.entries.get(key).map_or(0.0, |e| e.cpus)
    }

    /// Prior loaded from this peer's own snapshot.
    pub fn load(&mut self, key: &FnKey, name: &str, summary: Summary, cpus: f64) {
        let e = self.entry(key, name);
        e.disk = Some(summary);
        if cpus > 0.0 {
            e.cpus = cpu_quantum(cpus);
        }
        e.refresh_prior();
    }

    /// Profiles changed since the last call (for snapshots and gossip):
    /// (key, name, summary, learned cpus).
    pub fn take_dirty(&mut self) -> Vec<(FnKey, String, Summary, f64)> {
        self.entries
            .iter_mut()
            .filter(|(_, e)| e.dirty)
            .map(|(k, e)| {
                e.dirty = false;
                (k.clone(), e.name.clone(), e.stats.summary(), e.cpus)
            })
            .collect()
    }

    /// Every profile (for a full snapshot), most recently used first.
    pub fn all(&self) -> Vec<(FnKey, String, Summary, f64)> {
        let mut v: Vec<_> = self.entries.iter().map(|(k, e)| (e.last_used, k, e)).collect();
        v.sort_by(|a, b| b.0.cmp(&a.0));
        v.into_iter().map(|(_, k, e)| (k.clone(), e.name.clone(), e.stats.summary(), e.cpus)).collect()
    }

    /// Write every profile to `path` (atomically), so a restarted peer and
    /// the next run of a script start from what was learned.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let snap = pb::ProfileSnapshot {
            profiles: self.all().iter().map(|(k, name, s, cpus)| to_pb(k, name, s, *cpus, self.hw_class)).collect(),
        };
        crate::stores::write_atomic(path, &snap.encode_to_vec())
    }

    /// Profiles saved by an earlier run of this peer; empty if none (or if
    /// the file cannot be read, which only costs relearning).
    pub fn load_file(path: &Path) -> Profiles {
        let mut p = Profiles::default();
        let Ok(bytes) = std::fs::read(path) else { return p };
        match pb::ProfileSnapshot::decode(bytes.as_slice()) {
            Ok(snap) => {
                // Oldest first, so the most recently used end up newest.
                for s in snap.profiles.iter().rev() {
                    let (key, name, summary, cpus) = from_pb(s);
                    p.load(&key, &name, summary, cpus);
                }
            }
            Err(e) => tracing::warn!("ignoring unreadable profiles in {}: {e}", path.display()),
        }
        p
    }
}

/// A peer's hardware class: profiles measured on the same class carry their
/// CPU use between peers; on another class only memory does.
pub fn hw_class(shape: &chimaera_core::topology::HostShape) -> u64 {
    let gpus: Vec<&str> = shape.gpus.iter().map(|g| g.name.as_str()).collect();
    let key = format!(
        "{}|{}|{:.0}|{}|{}",
        shape.os,
        shape.arch,
        shape.usable_cores().round(),
        shape.memory_bytes >> 30,
        gpus.join(",")
    );
    u64::from_le_bytes(blake3::hash(key.as_bytes()).as_bytes()[..8].try_into().expect("8 bytes"))
}

pub fn to_pb(key: &FnKey, name: &str, s: &Summary, cpus: f64, hw_class: u64) -> pb::ProfileSummary {
    let bucket = |(bucket, bytes, samples): (u8, u64, u32)| pb::SizeBucket { bucket: bucket as u32, bytes, samples };
    pb::ProfileSummary {
        fn_digest: key.digest.clone(),
        method: key.method.clone(),
        name: name.to_string(),
        hw_class,
        n: s.n,
        cores_mean: s.cores_mean,
        cores_cv: s.cores_cv,
        cpus_granted: s.cpus_granted,
        wall_p50_s: s.wall_p50_s,
        wall_p90_s: s.wall_p90_s,
        peak_n: s.peak_n,
        peak_p95: s.peak_p95,
        peak_cv: s.peak_cv,
        max_arg_bytes: s.max_arg_bytes,
        has_fit: s.fit.is_some(),
        fit_a: s.fit.map_or(0.0, |f| f.a),
        fit_b: s.fit.map_or(0.0, |f| f.b),
        fit_r2: s.fit.map_or(0.0, |f| f.r2),
        fit_resid_cv: s.fit.map_or(0.0, |f| f.resid_cv),
        buckets: s.buckets.iter().copied().map(bucket).collect(),
        oom_bounds: s.oom_bounds.iter().map(|(b, bytes)| bucket((*b, *bytes, 0))).collect(),
        out_p90: s.out_p90,
        learned_cpus: cpus,
    }
}

pub fn from_pb(p: &pb::ProfileSummary) -> (FnKey, String, Summary, f64) {
    let summary = Summary {
        n: p.n,
        cores_mean: p.cores_mean,
        cores_cv: p.cores_cv,
        cpus_granted: p.cpus_granted,
        wall_p50_s: p.wall_p50_s,
        wall_p90_s: p.wall_p90_s,
        peak_n: p.peak_n,
        peak_p95: p.peak_p95,
        peak_cv: p.peak_cv,
        max_arg_bytes: p.max_arg_bytes,
        fit: p.has_fit.then_some(Fit { a: p.fit_a, b: p.fit_b, r2: p.fit_r2, resid_cv: p.fit_resid_cv }),
        buckets: p.buckets.iter().map(|b| (b.bucket.min(64) as u8, b.bytes, b.samples)).collect(),
        oom_bounds: p.oom_bounds.iter().map(|b| (b.bucket.min(64) as u8, b.bytes)).collect(),
        out_p90: p.out_p90,
    };
    (FnKey::new(&p.fn_digest, &p.method), p.name.clone(), summary, p.learned_cpus)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::policy::Adaptive;

    const MIB: u64 = 1 << 20;

    fn ok(wall: f64, cores: f64, peak: u64, cpus: f64) -> Sample {
        Sample { wall_s: wall, cpu_s: wall * cores, peak, arg_bytes: MIB, out_bytes: 0, cpus, oom: false }
    }

    fn learn() -> Request {
        Request { adaptive: Adaptive::On, ..Default::default() }
    }

    /// Records sleepy calls (0.02 cores over 0.2 s) until a CPU trial runs.
    fn start_trial(p: &mut Profiles, k: &FnKey, req: Request) -> f64 {
        let mut now = 0.0;
        while p.entries.get(k).is_none_or(|e| e.trial.is_none()) {
            now += 0.05;
            p.record(k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, 1.0), None, req, false, now, 0);
        }
        now
    }

    #[test]
    fn an_initial_value_is_where_learning_starts_and_a_pin_never_moves() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        let initial = Request { cpus: 2.0, cpus_initial: true, ..learn() };
        let first = p.reserve(&k, "m.sleepy", MIB, initial);
        assert_eq!((first.cpus, first.cpus_source.describe().as_str()), (2.0, "your initial setting"));
        let mut now = 0.0;
        for _ in 0..300 {
            let r = p.reserve(&k, "m.sleepy", MIB, initial);
            now += 0.05;
            p.record(&k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, r.cpus), r.arm, initial, false, now, 0);
        }
        let r = p.reserve(&k, "m.sleepy", MIB, initial);
        assert!(r.cpus < 2.0 && matches!(r.cpus_source, Source::Learned(_)), "{r:?}");
        // The same function pinned at 2 keeps 2, learned value or not.
        let pinned = Request { cpus: 2.0, ..learn() };
        assert_eq!(p.reserve(&k, "m.sleepy", MIB, pinned).cpus_source, Source::User);
        assert_eq!(p.cpus_now(&k, pinned, None), None);
        // An initial memory value is reserved until there is an estimate, then replaced.
        let k2 = FnKey::new(b"mem", "");
        let mem = Request { memory: 64 * MIB, memory_initial: true, ..learn() };
        let r = p.reserve(&k2, "m.mem", MIB, mem);
        assert_eq!((r.memory, r.memory_source), (64 * MIB, Source::Initial));
        for _ in 0..3 {
            p.record(&k2, "m.mem", ok(1.0, 1.0, 200 * MIB, 1.0), None, mem, false, 0.0, 0);
        }
        assert_eq!(p.reserve(&k2, "m.mem", MIB, mem).memory, 240 * MIB);
    }

    #[test]
    fn observed_and_frozen_calls_leave_a_running_trial_alone() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        start_trial(&mut p, &k, learn());
        let observe = Request { adaptive: Adaptive::Observe, ..learn() };
        let frozen = Request { adaptive: Adaptive::Frozen, ..learn() };
        let first = p.reserve(&k, "m.sleepy", MIB, learn()).arm.expect("an On call takes an arm");
        for req in [observe, frozen, observe] {
            let r = p.reserve(&k, "m.sleepy", MIB, req);
            assert_eq!((r.cpus, r.arm), (1.0, None), "{req:?} keeps the default and runs no arm");
            assert_eq!(p.cpus_now(&k, req, None), None);
            // Its samples still count as measurements, but feed no arm.
            p.record(&k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, 1.0), None, req, false, 1e3, 0);
        }
        let next = p.reserve(&k, "m.sleepy", MIB, learn()).arm.expect("trial still running");
        assert_ne!(next, first, "the arms alternate as if the other calls had not come");
    }

    #[test]
    fn observe_says_what_learning_would_reserve() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"threads", "");
        for _ in 0..3 {
            p.record(&k, "m.f", ok(1.0, 3.5, 200 * MIB, 1.0), None, learn(), false, 0.0, 0);
        }
        let observe = Request { adaptive: Adaptive::Observe, ..learn() };
        let r = p.reserve(&k, "m.f", MIB, observe);
        assert_eq!((r.cpus, r.memory), (1.0, 0), "observe reserves what frozen would");
        let (cpus, mem) = (r.cpus_source.describe(), r.memory_source.describe());
        assert!(cpus.starts_with("not applied (adaptive=\"observe\"): would reserve 4 cores"), "{cpus}");
        assert!(mem.contains("would reserve 240 MiB"), "{mem}");
        let capped = p.reserve(&k, "m.f", MIB, Request { cpus_max: 2.0, ..observe });
        assert!(capped.cpus_source.describe().contains("would reserve 2 cores"), "within the caller's bounds");
        let initial = Request { cpus: 2.0, cpus_initial: true, ..observe };
        assert_eq!(p.reserve(&k, "m.f", MIB, initial).cpus, 2.0, "observe keeps the caller's initial value");
    }

    #[test]
    fn an_oom_in_the_canary_reverts_the_trial_at_once() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        let now = start_trial(&mut p, &k, learn());
        let canary = loop {
            let r = p.reserve(&k, "m.sleepy", MIB, learn());
            if r.arm == Some(true) {
                break r;
            }
        };
        let oom = Sample { peak: 500 * MIB, arg_bytes: MIB, cpus: canary.cpus, oom: true, ..Default::default() };
        let ds = p.record(&k, "m.sleepy", oom, canary.arm, learn(), true, now + 0.01, 0);
        assert!(ds.iter().any(|d| d.knob == "cpus" && d.outcome == "reverted"), "{ds:#?}");
        assert_eq!(p.reserve(&k, "m.sleepy", MIB, learn()).arm, None, "no trial after the revert");
    }

    #[test]
    fn trials_and_growth_stay_within_the_callers_bounds() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        let bounded = Request { cpus_min: 0.5, ..learn() };
        let (mut now, mut ds) = (0.0, Vec::new());
        for _ in 0..300 {
            let r = p.reserve(&k, "m.sleepy", MIB, bounded);
            now += 0.05;
            ds.extend(p.record(&k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, r.cpus), r.arm, bounded, false, now, 0));
        }
        let cpus: Vec<f64> = ds.iter().filter(|d| d.knob == "cpus").map(|d| d.new_value.parse().unwrap()).collect();
        assert!(!cpus.is_empty() && cpus.iter().all(|c| *c >= 0.5), "{ds:#?}");
        assert_eq!(p.reserve(&k, "m.sleepy", MIB, bounded).cpus, 0.5);
        let k2 = FnKey::new(b"threads", "");
        let capped = Request { cpus_max: 2.0, ..learn() };
        for _ in 0..3 {
            p.record(&k2, "m.f", ok(1.0, 3.5, 0, 1.0), None, capped, false, 0.0, 0);
        }
        assert_eq!(p.reserve(&k2, "m.f", MIB, capped).cpus, 2.0);
    }

    #[test]
    fn only_calls_learning_applies_to_move_batching() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"tiny", "");
        for adaptive in [Adaptive::Observe, Adaptive::Frozen] {
            for _ in 0..12 {
                p.record(&k, "m.tiny", ok(0.0001, 1.0, 0, 1.0), None, Request { adaptive, ..learn() }, false, 0.0, 0);
            }
            assert_eq!(p.batch(&k), 1, "{adaptive:?} calls do not batch");
        }
        p.record(&k, "m.tiny", ok(0.0001, 1.0, 0, 1.0), None, learn(), false, 0.0, 0);
        assert_eq!(p.batch(&k), 2);
    }

    #[test]
    fn memory_is_learned_after_three_calls_and_user_values_win() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"digest-a", "");
        assert_eq!(p.reserve(&k, "m.f", MIB, learn()).memory, 0);
        for _ in 0..3 {
            p.record(&k, "m.f", ok(1.0, 1.0, 200 * MIB, 1.0), None, learn(), false, 0.0, 0);
        }
        let r = p.reserve(&k, "m.f", MIB, learn());
        assert_eq!(r.memory, 240 * MIB);
        assert!(matches!(r.memory_source, Source::Learned(_)));
        let user = p.reserve(&k, "m.f", MIB, Request { memory: 64 * MIB, cpus: 2.0, ..learn() });
        assert_eq!((user.memory, user.cpus, user.cpus_source), (64 * MIB, 2.0, Source::User));
        let observe = p.reserve(&k, "m.f", MIB, Request { adaptive: Adaptive::Observe, ..learn() });
        assert_eq!(observe.memory, 0);
        assert!(matches!(observe.memory_source, Source::Observed(_)));
    }

    #[test]
    fn a_waiting_function_ends_up_at_a_quarter_core() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        let mut now = 0.0;
        let mut decisions = Vec::new();
        for _ in 0..200 {
            let r = p.reserve(&k, "m.sleepy", MIB, learn());
            now += 0.05;
            decisions.extend(p.record(&k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, r.cpus), r.arm, learn(), false, now, 0));
        }
        let outcomes: Vec<_> = decisions.iter().filter(|d| d.knob == "cpus").map(|d| d.outcome.as_str()).collect();
        assert_eq!(outcomes, ["pending", "adopted"], "{decisions:#?}");
        let r = p.reserve(&k, "m.sleepy", MIB, learn());
        assert_eq!((r.cpus, r.arm), (0.25, None));
    }

    #[test]
    fn queued_work_keeps_its_trial_arm_however_often_it_is_looked_at() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"sleepy", "");
        let mut now = 0.0;
        while p.entries.get(&k).is_none_or(|e| e.trial.is_none()) {
            now += 0.05;
            p.record(&k, "m.sleepy", ok(0.2, 0.02, 50 * MIB, 1.0), None, learn(), false, now, 0);
        }
        // A control waiting for a full core is looked at on every dispatch
        // pass; it must stay a control, not become a canary that fits sooner.
        let (_, _, arm) = p.cpus_now(&k, learn(), None).expect("trial running");
        for _ in 0..5 {
            assert_eq!(
                p.cpus_now(&k, learn(), arm).map(|(c, _, a)| (c, a)),
                Some((if arm == Some(true) { 0.25 } else { 1.0 }, arm))
            );
        }
        // New work alternates between the arms.
        let (_, _, next) = p.cpus_now(&k, learn(), None).expect("trial running");
        assert_ne!(next, arm);
    }

    #[test]
    fn a_computing_function_keeps_its_core_and_backs_off() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"half-busy", "");
        let mut now = 0.0;
        let mut decisions = Vec::new();
        for _ in 0..200 {
            let r = p.reserve(&k, "m.f", MIB, learn());
            // Uses 0.2 cores, but at a quarter core it shares and slows 4×.
            let wall = if r.cpus < 1.0 { 0.8 } else { 0.2 };
            now += 0.05;
            decisions.extend(p.record(&k, "m.f", ok(wall, 0.2, 0, r.cpus), r.arm, learn(), false, now, 0));
        }
        let outcomes: Vec<_> = decisions.iter().filter(|d| d.knob == "cpus").map(|d| d.outcome.as_str()).collect();
        assert_eq!(&outcomes[..2], ["pending", "reverted"]);
        // The next trial waits 100 more calls (50 doubled per revert).
        let first_revert = decisions.iter().position(|d| d.outcome == "reverted").expect("revert");
        let retried = decisions.iter().skip(first_revert).filter(|d| d.outcome == "pending").count();
        assert!(retried <= 1, "{retried} retries within 200 calls");
        assert_eq!(p.reserve(&k, "m.f", MIB, learn()).cpus, 1.0);
    }

    #[test]
    fn cpu_grows_at_once_for_a_function_that_uses_more() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"threads", "");
        let mut ds = Vec::new();
        for _ in 0..3 {
            ds.extend(p.record(&k, "m.f", ok(1.0, 3.5, 0, 1.0), None, learn(), false, 0.0, 0));
        }
        assert!(ds.iter().any(|d| d.knob == "cpus" && d.new_value == "4" && d.outcome == "adopted"), "{ds:#?}");
        assert_eq!(p.reserve(&k, "m.f", MIB, learn()).cpus, 4.0);
    }

    #[test]
    fn fast_calls_are_batched_by_doubling() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"tiny", "");
        for _ in 0..12 {
            p.record(&k, "m.tiny", ok(0.0001, 1.0, 0, 1.0), None, learn(), false, 0.0, 0);
        }
        assert_eq!(p.batch(&k), 1024, "doubles each sample after the third");
        for _ in 0..3 {
            p.record(&k, "m.tiny", ok(0.0001, 1.0, 0, 1.0), None, learn(), false, 0.0, 0);
        }
        assert_eq!(p.batch(&k), 2000);
    }

    #[test]
    fn calls_that_keep_memory_are_batched_shallowly() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"keeps", "");
        for _ in 0..12 {
            p.record(&k, "m.keeps", ok(0.002, 1.0, 0, 1.0), None, learn(), false, 0.0, 0);
        }
        assert!(p.batch(&k) > 64, "2 ms calls batch deeply: {}", p.batch(&k));
        for _ in 0..4 {
            p.note_kept(&k, 20 * MIB);
        }
        assert!(p.batch(&k) <= 3, "20 MiB kept per call: {}", p.batch(&k));
        for _ in 0..8 {
            p.note_kept(&k, 0);
        }
        assert!(p.batch(&k) > 64, "calls that keep nothing batch as before: {}", p.batch(&k));
    }

    #[test]
    fn edited_code_starts_from_the_old_profile() {
        let mut p = Profiles::default();
        let old = FnKey::new(b"v1", "");
        for _ in 0..30 {
            p.record(&old, "m.f", ok(1.0, 1.0, 300 * MIB, 1.0), None, learn(), false, 0.0, 0);
        }
        let new = FnKey::new(b"v2", "");
        let r = p.reserve(&new, "m.f", MIB, learn());
        assert_eq!(r.memory, 360 * MIB);
        assert_eq!(p.summary(&new).expect("entry").n, PRIOR_FROM_OLD_CODE);
    }

    #[test]
    fn another_peers_profile_is_a_prior_until_local_calls_suffice() {
        let mut p = Profiles::default();
        let k = FnKey::new(b"remote", "");
        let mut other = Profiles::default();
        for _ in 0..10 {
            other.record(&k, "m.f", ok(1.0, 1.0, 500 * MIB, 1.0), None, learn(), false, 0.0, 0);
        }
        let (sent, next) = other.changed_since(0, 64);
        assert_eq!(sent.len(), 1);
        assert!(other.changed_since(next, 64).0.is_empty(), "nothing new since");
        p.merge_remote(7, &sent[0]);
        p.merge_remote(7, &sent[0]);
        assert_eq!(p.summary(&k).expect("entry").n, 10, "the same peer's summary replaces, not adds");
        assert_eq!(p.reserve(&k, "m.f", MIB, learn()).memory, 600 * MIB);
        other.record(&k, "m.f", ok(1.0, 1.0, 500 * MIB, 1.0), None, learn(), false, 0.0, 0);
        assert_eq!(other.changed_since(next, 64).0.len(), 1, "a new sample is sent again");
    }

    #[test]
    fn other_hardware_informs_memory_but_not_cpu() {
        let k = FnKey::new(b"threads", "");
        let mut other = Profiles { hw_class: 1, ..Default::default() };
        for _ in 0..12 {
            other.record(&k, "m.f", ok(1.0, 3.5, 500 * MIB, 1.0), None, learn(), false, 0.0, 0);
        }
        let (sent, _) = other.changed_since(0, 64);
        let mut same = Profiles { hw_class: 1, ..Default::default() };
        same.merge_remote(9, &sent[0]);
        assert_eq!(same.reserve(&k, "m.f", MIB, learn()).memory, 600 * MIB);
        let mut different = Profiles { hw_class: 2, ..Default::default() };
        different.merge_remote(9, &sent[0]);
        let s = different.summary(&k).expect("entry");
        assert_eq!((s.cpu_grow(), s.cpu_shrink(false)), (None, None), "no CPU decision from other hardware");
        assert_eq!(different.reserve(&k, "m.f", MIB, learn()).memory, 600 * MIB, "memory still carries over");
    }

    #[test]
    fn profiles_survive_a_restart_through_the_snapshot() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("profiles.pb");
        let mut p = Profiles::default();
        let k = FnKey::new(b"saved", "method");
        for i in 1..=16u64 {
            let peak = 50 * MIB + 3 * i * MIB;
            p.record(
                &k,
                "m.C.method",
                Sample { arg_bytes: i * MIB, ..ok(1.0, 3.5, peak, 1.0) },
                None,
                learn(),
                false,
                0.0,
                0,
            );
        }
        p.save(&path).unwrap();
        let mut q = Profiles::load_file(&path);
        assert_eq!(q.summary(&k), p.summary(&k), "the summary round-trips");
        assert_eq!(q.reserve(&k, "m.C.method", 8 * MIB, learn()), p.reserve(&k, "m.C.method", 8 * MIB, learn()));
        assert_eq!(q.reserve(&k, "m.C.method", MIB, learn()).cpus, 4.0, "the learned grant is kept");
        assert!(Profiles::load_file(&d.path().join("missing")).is_empty());
        std::fs::write(&path, b"not a snapshot").unwrap();
        assert!(Profiles::load_file(&path).is_empty(), "a damaged file costs relearning, not a crash");
    }

    #[test]
    fn profiles_are_bounded() {
        let mut p = Profiles::default();
        for i in 0..(MAX_PROFILES + MAX_PROFILES / 16 + 1) {
            p.reserve(&FnKey::new(&(i as u64).to_le_bytes(), ""), "", 0, learn());
        }
        assert_eq!(p.len(), MAX_PROFILES);
        assert!(p.summary(&FnKey::new(&0u64.to_le_bytes(), "")).is_none(), "oldest dropped");
    }
}
