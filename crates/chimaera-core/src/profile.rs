//! What a function's calls actually used, and the reservation the next call
//! should get.
//!
//! The peer records one [`Sample`] per finished attempt in that function's
//! [`Stats`]. A [`Summary`] is the sufficient statistic: it is computed from
//! the recent samples, persisted, and gossiped to other peers. Every estimate
//! is computed from a summary, so a restarted peer or a peer that never ran
//! the function decides the same way as the one that measured it.
//!
//! Rules (sources in `docs/requirements.md`, A4/A6/R4 and the guides' CMR and
//! CALC rule sets; where they disagree the more conservative value is used):
//! - Memory: p95 private peak × 1.2 (CV < 0.1), × 1.5 (CV < 0.3), × 2.0
//!   (higher CV, or any OOM). When peak grows linearly with input size
//!   (R² ≥ 0.8 over ≥ 8 samples) the fit predicts each call's own need. An OOM
//!   is a censored sample: the true peak was at least what was observed, so
//!   later calls of that size or larger get at least 1.5× it, but no more
//!   than the larger of it and 4× what calls of that size use when they
//!   succeed. Each 8 successful calls after it halve the gap between the
//!   bound and the most they used, so one kill fades instead of holding every
//!   later call to it until it leaves the sample ring (CALC §22.5).
//! - CPU grows at once when a function uses over 1.3× its grant. A smaller
//!   grant is only proposed (the peer canaries it): never below 10 samples,
//!   for tasks under 50 ms, with CV over 0.5, or while memory kills are recent.
//!
//! Pure and deterministic: no clock, no randomness, no hash-map iteration.

use crate::adapt::{Config, Verdict, Window, judge};
use std::collections::VecDeque;

/// Samples kept per function.
pub const RING: usize = 64;
/// CPU reservations come in a few sizes so warm executors are shared.
pub const CPU_QUANTA: [f64; 6] = [0.25, 0.5, 1.0, 2.0, 4.0, 8.0];
/// Successful samples before any estimate is used.
pub const MIN_SAMPLES: u64 = 3;
/// Successful samples before a smaller CPU grant is proposed.
pub const SHRINK_SAMPLES: u64 = 10;
const SHORT_TASK_S: f64 = 0.05;
const IO_BOUND_CORES: f64 = 0.3;
/// Variation in CPU use vetoes a smaller grant only when it is this large in
/// cores: a call that sleeps varies a lot relative to its tiny use.
const MATERIAL_CORES: f64 = 0.05;
const FIT_SAMPLES: usize = 8;
/// Successful calls after an OOM that halve its bound's excess over them.
const OOM_HALVING_CALLS: usize = 8;
/// An OOM bound asks at most this many times the p95 of successful calls of
/// its size (never less than the bound itself).
const OOM_CAP: f64 = 4.0;
const FIT_R2: f64 = 0.8;
const MIB: u64 = 1 << 20;

/// log2 size class of an input: 0 for empty, else the bit length.
pub fn size_bucket(bytes: u64) -> u8 {
    (64 - bytes.leading_zeros()) as u8
}

/// Smallest CPU quantum at or above `x` (whole cores above the largest).
pub fn cpu_quantum(x: f64) -> f64 {
    CPU_QUANTA.iter().copied().find(|q| *q + 1e-9 >= x).unwrap_or_else(|| x.ceil())
}

/// One finished attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Sample {
    pub wall_s: f64,
    pub cpu_s: f64,
    /// Private (anonymous) memory peak in bytes; 0 when not measured.
    pub peak: u64,
    /// Argument, blob and dependency bytes the call read.
    pub arg_bytes: u64,
    pub out_bytes: u64,
    /// CPU granted to the attempt.
    pub cpus: f64,
    /// Killed for memory: `peak` is a lower bound of what it needed.
    pub oom: bool,
}

/// Linear model of peak memory against input bytes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fit {
    pub a: f64,
    pub b: f64,
    pub r2: f64,
    /// Coefficient of variation of observed / predicted.
    pub resid_cv: f64,
}

/// What the recent calls of one function used.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    /// Successful samples behind these numbers.
    pub n: u64,
    pub cores_mean: f64,
    pub cores_cv: f64,
    /// CPU the summarized attempts were granted (most recent).
    pub cpus_granted: f64,
    pub wall_p50_s: f64,
    pub wall_p90_s: f64,
    /// Samples with a measured peak, and their p95 and CV.
    pub peak_n: u64,
    pub peak_p95: u64,
    pub peak_cv: f64,
    pub max_arg_bytes: u64,
    pub fit: Option<Fit>,
    /// (size bucket, p95 peak, samples), ascending by bucket.
    pub buckets: Vec<(u8, u64, u32)>,
    /// (size bucket, peak observed when killed), ascending by bucket.
    pub oom_bounds: Vec<(u8, u64)>,
    pub out_p90: u64,
}

/// A memory reservation and why.
#[derive(Clone, Debug, PartialEq)]
pub struct MemEstimate {
    pub bytes: u64,
    pub evidence: String,
}

fn margin(cv: f64, oom: bool) -> f64 {
    if oom || cv > 0.3 {
        2.0
    } else if cv >= 0.1 {
        1.5
    } else {
        1.2
    }
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() { 0.0 } else { xs.iter().sum::<f64>() / xs.len() as f64 }
}

fn cv(xs: &[f64]) -> f64 {
    let m = mean(xs);
    if xs.len() < 2 || m <= 0.0 {
        return 0.0;
    }
    let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64;
    var.sqrt() / m
}

/// Nearest-rank percentile.
fn pct(xs: &[f64], q: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v = xs.to_vec();
    v.sort_by(f64::total_cmp);
    let rank = ((q * v.len() as f64).ceil() as usize).clamp(1, v.len());
    v[rank - 1]
}

fn fit(points: &[(f64, f64)]) -> Option<Fit> {
    if points.len() < FIT_SAMPLES {
        return None;
    }
    let n = points.len() as f64;
    let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
    let my = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx = points.iter().map(|p| (p.0 - mx).powi(2)).sum::<f64>();
    let syy = points.iter().map(|p| (p.1 - my).powi(2)).sum::<f64>();
    let sxy = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum::<f64>();
    // Inputs must actually vary (by at least 2× across the samples).
    let (lo, hi) = points.iter().fold((f64::MAX, 0.0f64), |(l, h), p| (l.min(p.0), h.max(p.0)));
    if sxx <= 0.0 || syy <= 0.0 || hi < 2.0 * lo.max(1.0) {
        return None;
    }
    let b = sxy / sxx;
    let a = my - b * mx;
    let r2 = sxy * sxy / (sxx * syy);
    if b <= 0.0 || r2 < FIT_R2 {
        return None;
    }
    let ratios: Vec<f64> = points.iter().map(|p| p.1 / (a + b * p.0).max(1.0)).collect();
    Some(Fit { a, b, r2, resid_cv: cv(&ratios) })
}

/// Recent samples of one function, plus what was known before them.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    ring: VecDeque<Sample>,
    /// Successful samples ever pushed (not only those kept).
    pub total: u64,
    /// Summary loaded from disk or received from another peer; used until
    /// enough local samples exist.
    pub prior: Option<Summary>,
}

impl Stats {
    pub fn push(&mut self, s: Sample) {
        if !s.oom {
            self.total += 1;
        }
        if self.ring.len() == RING {
            self.ring.pop_front();
        }
        self.ring.push_back(s);
    }

    pub fn samples(&self) -> impl Iterator<Item = &Sample> {
        self.ring.iter()
    }

    /// Successful samples held locally.
    pub fn local_ok(&self) -> u64 {
        self.ring.iter().filter(|s| !s.oom).count() as u64
    }

    /// The local summary once there are enough samples, else the prior (with
    /// local OOM bounds added), else whatever the local samples show.
    pub fn summary(&self) -> Summary {
        let local = summarize(self.ring.iter());
        match &self.prior {
            Some(p) if local.n < MIN_SAMPLES => {
                let mut s = p.clone();
                for (b, peak) in &local.oom_bounds {
                    add_bound(&mut s.oom_bounds, *b, *peak);
                }
                s
            }
            _ => local,
        }
    }
}

/// What an OOM at `peak` still says after the successful calls of that size
/// or larger that followed it used `later` (peaks, oldest first): bisected
/// toward the most they used, gone once within 10% of it or below it.
fn oom_bound(peak: u64, later: &[u64]) -> Option<u64> {
    let Some(top) = later.iter().copied().max() else { return Some(peak) };
    if top >= peak {
        return None; // calls since used as much: their own samples carry it
    }
    let gap = (peak - top) >> (later.len() / OOM_HALVING_CALLS).min(63);
    (gap.saturating_mul(10) > top).then_some(top + gap)
}

fn add_bound(bounds: &mut Vec<(u8, u64)>, bucket: u8, peak: u64) {
    match bounds.binary_search_by_key(&bucket, |b| b.0) {
        Ok(i) => bounds[i].1 = bounds[i].1.max(peak),
        Err(i) => bounds.insert(i, (bucket, peak)),
    }
}

/// Summarize samples (oldest first).
pub fn summarize<'a>(samples: impl Iterator<Item = &'a Sample>) -> Summary {
    let mut s = Summary::default();
    let (mut cores, mut walls, mut peaks, mut outs) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut points = Vec::new();
    let mut by_bucket: Vec<(u8, Vec<f64>)> = Vec::new();
    let all: Vec<&Sample> = samples.collect();
    for (i, x) in all.iter().enumerate() {
        if x.oom {
            let bucket = size_bucket(x.arg_bytes);
            let later: Vec<u64> = all[i + 1..]
                .iter()
                .filter(|y| !y.oom && y.peak > 0 && size_bucket(y.arg_bytes) >= bucket)
                .map(|y| y.peak)
                .collect();
            if let Some(bound) = oom_bound(x.peak, &later) {
                add_bound(&mut s.oom_bounds, bucket, bound);
            }
            continue;
        }
        s.n += 1;
        s.cpus_granted = x.cpus;
        walls.push(x.wall_s);
        outs.push(x.out_bytes as f64);
        if x.wall_s > 0.0 {
            cores.push(x.cpu_s / x.wall_s);
        }
        s.max_arg_bytes = s.max_arg_bytes.max(x.arg_bytes);
        if x.peak > 0 {
            peaks.push(x.peak as f64);
            points.push((x.arg_bytes as f64, x.peak as f64));
            let b = size_bucket(x.arg_bytes);
            match by_bucket.binary_search_by_key(&b, |e| e.0) {
                Ok(i) => by_bucket[i].1.push(x.peak as f64),
                Err(i) => by_bucket.insert(i, (b, vec![x.peak as f64])),
            }
        }
    }
    s.cores_mean = mean(&cores);
    s.cores_cv = cv(&cores);
    s.wall_p50_s = pct(&walls, 0.5);
    s.wall_p90_s = pct(&walls, 0.9);
    s.peak_n = peaks.len() as u64;
    s.peak_p95 = pct(&peaks, 0.95) as u64;
    s.peak_cv = cv(&peaks);
    s.out_p90 = pct(&outs, 0.9) as u64;
    s.fit = fit(&points);
    s.buckets = by_bucket.into_iter().map(|(b, v)| (b, pct(&v, 0.95) as u64, v.len() as u32)).collect();
    s
}

impl Summary {
    /// Confidence in the numbers from the sample count (CALC §5.1).
    pub fn confidence(&self) -> f64 {
        let n = self.n as f64;
        match self.n {
            0 | 1 => 0.2,
            2..=9 => 0.4,
            10..=49 => 0.6,
            _ => 0.4 + 0.5 * (((n - 1.0) / n).sqrt() * (1.0 - 1.0 / n.sqrt())).min(1.0),
        }
    }

    pub fn has_oom(&self) -> bool {
        !self.oom_bounds.is_empty()
    }

    /// Memory to reserve for a call reading `arg_bytes`, or `None` when
    /// nothing is known yet.
    pub fn memory_for(&self, arg_bytes: u64) -> Option<MemEstimate> {
        let oom = self.has_oom();
        let bucket = size_bucket(arg_bytes);
        let mut best: Option<MemEstimate> = None;
        let mut offer = |bytes: f64, evidence: String| {
            let bytes = bytes.max(0.0).ceil() as u64;
            if best.as_ref().is_none_or(|b| bytes > b.bytes) {
                best = Some(MemEstimate { bytes, evidence });
            }
        };
        if self.n >= MIN_SAMPLES && self.peak_n >= MIN_SAMPLES {
            let in_bucket = self.buckets.iter().find(|b| b.0 == bucket).filter(|b| b.2 as u64 >= MIN_SAMPLES);
            if let Some(f) = self.fit {
                let pred = f.a + f.b * arg_bytes as f64;
                let m = margin(f.resid_cv, oom);
                offer(
                    pred * m,
                    format!(
                        "peak grows with input (R² {:.2}): predicted {} MiB at {} MiB in × {m}",
                        f.r2,
                        pred as u64 / MIB,
                        arg_bytes / MIB
                    ),
                );
            } else if let Some((_, p95, k)) = in_bucket {
                let m = margin(self.peak_cv, oom);
                offer(*p95 as f64 * m, format!("p95 peak {} MiB over {k} calls of this input size × {m}", p95 / MIB));
            } else if self.max_arg_bytes == 0 || arg_bytes <= self.max_arg_bytes.saturating_mul(2) {
                let m = margin(self.peak_cv, oom);
                offer(
                    self.peak_p95 as f64 * m,
                    format!("p95 peak {} MiB over {} calls × {m}", self.peak_p95 / MIB, self.peak_n),
                );
            }
        }
        for (b, peak) in &self.oom_bounds {
            if *b <= bucket {
                // Capped by what calls of that size use when they succeed,
                // but never below the peak the killed call reached.
                let usual = self.buckets.iter().find(|x| x.0 == *b && x.2 as u64 >= MIN_SAMPLES).map(|x| x.1 as f64);
                let cap = usual.map_or(f64::MAX, |u| (OOM_CAP * u).max(*peak as f64));
                let bytes = (*peak as f64 * 1.5).min(cap);
                offer(bytes, format!("killed for memory at {} MiB; reserving {} MiB", peak / MIB, bytes as u64 / MIB));
            }
        }
        best
    }

    /// A larger CPU grant when calls use well over what they get (applied
    /// directly: the function is already using the cores).
    pub fn cpu_grow(&self) -> Option<f64> {
        if self.n < MIN_SAMPLES
            || self.wall_p50_s < SHORT_TASK_S
            || self.cpus_granted <= 0.0
            || self.cores_mean <= 1.3 * self.cpus_granted
        {
            return None;
        }
        Some(cpu_quantum(self.cores_mean)).filter(|c| *c > self.cpus_granted)
    }

    /// A smaller CPU grant worth canarying, or `None`. `kills_recent`: memory
    /// kills happened in the current window.
    pub fn cpu_shrink(&self, kills_recent: bool) -> Option<f64> {
        if self.n < SHRINK_SAMPLES
            || kills_recent
            || self.has_oom()
            || self.cores_cv > 0.5 && self.cores_mean * self.cores_cv > MATERIAL_CORES
            || self.wall_p50_s < SHORT_TASK_S
            || self.cpus_granted <= 0.0
        {
            return None;
        }
        Some(cpu_quantum(self.cores_mean * 1.5)).filter(|c| *c + 1e-9 < self.cpus_granted)
    }

    /// Mostly waiting (network, disk, sleep) rather than computing.
    pub fn io_bound(&self) -> bool {
        self.n >= SHRINK_SAMPLES && self.wall_p50_s >= SHORT_TASK_S && self.cores_mean < IO_BOUND_CORES
    }

    /// Combine two summaries of the same function (another peer's, or one
    /// loaded from disk), erring toward more memory and less certainty.
    pub fn merge(&self, other: &Summary) -> Summary {
        let (na, nb) = (self.n as f64, other.n as f64);
        let w = |a: f64, b: f64| if na + nb > 0.0 { (a * na + b * nb) / (na + nb) } else { 0.0 };
        let (big, small) = if self.n >= other.n { (self, other) } else { (other, self) };
        let mut buckets = big.buckets.clone();
        for (b, p, k) in &small.buckets {
            match buckets.binary_search_by_key(b, |e| e.0) {
                Ok(i) => {
                    buckets[i].1 = buckets[i].1.max(*p);
                    buckets[i].2 += k;
                }
                Err(i) => buckets.insert(i, (*b, *p, *k)),
            }
        }
        let mut oom_bounds = big.oom_bounds.clone();
        for (b, p) in &small.oom_bounds {
            add_bound(&mut oom_bounds, *b, *p);
        }
        Summary {
            n: self.n + other.n,
            cores_mean: w(self.cores_mean, other.cores_mean),
            cores_cv: self.cores_cv.max(other.cores_cv),
            cpus_granted: big.cpus_granted,
            wall_p50_s: w(self.wall_p50_s, other.wall_p50_s),
            wall_p90_s: self.wall_p90_s.max(other.wall_p90_s),
            peak_n: self.peak_n + other.peak_n,
            peak_p95: self.peak_p95.max(other.peak_p95),
            peak_cv: self.peak_cv.max(other.peak_cv),
            max_arg_bytes: self.max_arg_bytes.max(other.max_arg_bytes),
            fit: big.fit,
            buckets,
            oom_bounds,
            out_p90: self.out_p90.max(other.out_p90),
        }
    }
}

/// Calls per arm before a window closes.
const TRIAL_WINDOW_CALLS: u32 = 5;
/// Shortest window, in seconds.
const TRIAL_WINDOW_S: f64 = 1.0;
/// Tail-latency differences smaller than this are not a regression.
const TRIAL_JITTER_S: f64 = 0.010;

#[derive(Clone, Debug, Default)]
struct Arm {
    done: u32,
    reserved_core_s: f64,
    walls: Vec<f64>,
    ooms: u32,
}

impl Arm {
    fn window(&self) -> Window {
        Window {
            useful_per_s: if self.reserved_core_s > 0.0 { self.done as f64 / self.reserved_core_s } else { 0.0 },
            p99_s: pct(&self.walls, 0.99),
            peak_reserved_bytes: 0,
            oom_kills: self.ooms,
        }
    }
}

/// A canary of a smaller CPU grant for one function (C2). Attempts alternate
/// between the current grant (control) and the candidate (canary), so both
/// arms see the same load; each window compares completions per reserved
/// core-second, and [`judge`] decides with its latency and OOM guards. A
/// function that is really waiting keeps its wall time at a quarter core and
/// wins; one that computes slows down and trips the latency guard.
#[derive(Clone, Debug)]
pub struct CpuTrial {
    pub from: f64,
    pub to: f64,
    next_canary: bool,
    arms: [Arm; 2],
    pairs: Vec<(Window, Window)>,
    window_start_s: f64,
}

impl CpuTrial {
    pub fn new(from: f64, to: f64, now_s: f64) -> CpuTrial {
        CpuTrial { from, to, next_canary: false, arms: Default::default(), pairs: Vec::new(), window_start_s: now_s }
    }

    /// CPU for the next attempt, and whether it is the canary.
    pub fn assign(&mut self) -> (f64, bool) {
        let canary = self.next_canary;
        self.next_canary = !canary;
        (if canary { self.to } else { self.from }, canary)
    }

    pub fn observe(&mut self, canary: bool, wall_s: f64, oom: bool) {
        let cpus = if canary { self.to } else { self.from };
        let arm = &mut self.arms[canary as usize];
        if oom {
            arm.ooms += 1;
            return;
        }
        arm.done += 1;
        arm.reserved_core_s += cpus * wall_s.max(1e-6);
        arm.walls.push(wall_s);
    }

    /// Close a window when both arms have enough calls, and judge.
    pub fn tick(&mut self, now_s: f64) -> Option<Verdict> {
        let [control, canary] = &self.arms;
        let ready = control.done >= TRIAL_WINDOW_CALLS && canary.done >= TRIAL_WINDOW_CALLS;
        let oom = canary.ooms > control.ooms;
        if !(oom || ready && now_s - self.window_start_s >= TRIAL_WINDOW_S) {
            return None;
        }
        let (control, mut canary) = (control.window(), canary.window());
        // Latency differences of a few milliseconds are scheduling jitter
        // (more processes share the cores), not the slowdown of a call that
        // needs its CPU, which takes several times as long.
        if (canary.p99_s - control.p99_s).abs() < TRIAL_JITTER_S {
            canary.p99_s = control.p99_s;
        }
        self.pairs.push((control, canary));
        self.arms = Default::default();
        self.window_start_s = now_s;
        let cfg = Config { budget_bytes: u64::MAX, ..Config::default() };
        match judge(&self.pairs, &cfg) {
            Verdict::Continue => None,
            v => Some(v),
        }
    }

    pub fn pairs(&self) -> usize {
        self.pairs.len()
    }
}

/// Mann–Kendall test for an increasing trend, one-sided at 1%.
pub fn increasing_trend(xs: &[f64]) -> bool {
    let n = xs.len();
    if n < 8 {
        return false;
    }
    let mut s = 0i64;
    for i in 0..n {
        for j in i + 1..n {
            s += (xs[j] - xs[i]).partial_cmp(&0.0).map_or(0, |o| o as i64);
        }
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mut ties = 0.0;
    let mut i = 0;
    while i < n {
        let mut j = i;
        while j + 1 < n && sorted[j + 1] == sorted[i] {
            j += 1;
        }
        let t = (j - i + 1) as f64;
        ties += t * (t - 1.0) * (2.0 * t + 5.0);
        i = j + 1;
    }
    let nf = n as f64;
    let var = (nf * (nf - 1.0) * (2.0 * nf + 5.0) - ties) / 18.0;
    var > 0.0 && s > 0 && (s as f64 - 1.0) / var.sqrt() > 2.326
}

/// Sen's slope: the median of pairwise slopes.
pub fn sen_slope(xs: &[f64]) -> f64 {
    let mut slopes = Vec::new();
    for i in 0..xs.len() {
        for j in i + 1..xs.len() {
            slopes.push((xs[j] - xs[i]) / (j - i) as f64);
        }
    }
    pct(&slopes, 0.5)
}

/// Watches an executor's private memory between calls (R4). A leak is a
/// steady rise across calls, not a high peak: raising memory would hide it.
#[derive(Clone, Debug, Default)]
pub struct LeakWatch {
    first: Option<u64>,
    recent: VecDeque<f64>,
}

/// Calls in the trend window.
pub const LEAK_WINDOW: usize = 20;

impl LeakWatch {
    /// Record the baseline after a call; `Some(reason)` means replace the
    /// executor after this call.
    pub fn after_call(&mut self, baseline: u64) -> Option<String> {
        let first = *self.first.get_or_insert(baseline);
        if self.recent.len() == LEAK_WINDOW {
            self.recent.pop_front();
        }
        self.recent.push_back(baseline as f64);
        if baseline > first.saturating_mul(2).saturating_add(512 * MIB) {
            return Some(format!("memory between calls grew from {} MiB to {} MiB", first / MIB, baseline / MIB));
        }
        // A trend needs real growth behind it: testing every call over a
        // sliding window would otherwise retire healthy executors on noise.
        if self.recent.len() == LEAK_WINDOW && baseline >= first.saturating_add(128 * MIB) {
            let xs: Vec<f64> = self.recent.iter().copied().collect();
            let slope = sen_slope(&xs);
            if slope >= MIB as f64 && increasing_trend(&xs) {
                return Some(format!(
                    "memory between calls rose {:.1} MiB per call over {LEAK_WINDOW} calls",
                    slope / MIB as f64
                ));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(wall: f64, cores: f64, peak: u64, arg: u64) -> Sample {
        Sample { wall_s: wall, cpu_s: wall * cores, peak, arg_bytes: arg, out_bytes: 0, cpus: 1.0, oom: false }
    }

    /// Deterministic noise in [-1, 1) (splitmix64 of the index).
    fn noise(i: u64) -> f64 {
        let mut z = i.wrapping_add(0x9e3779b97f4a7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;
        ((z >> 11) as f64 / (1u64 << 53) as f64) * 2.0 - 1.0
    }

    fn stats(samples: impl IntoIterator<Item = Sample>) -> Stats {
        let mut s = Stats::default();
        for x in samples {
            s.push(x);
        }
        s
    }

    #[test]
    fn quanta_round_up() {
        assert_eq!(cpu_quantum(0.1), 0.25);
        assert_eq!(cpu_quantum(0.25), 0.25);
        assert_eq!(cpu_quantum(0.3), 0.5);
        assert_eq!(cpu_quantum(1.4), 2.0);
        assert_eq!(cpu_quantum(11.2), 12.0);
    }

    #[test]
    fn nothing_is_estimated_before_three_calls() {
        let s = stats([ok(1.0, 1.0, 100 * MIB, MIB), ok(1.0, 1.0, 100 * MIB, MIB)]).summary();
        assert_eq!(s.memory_for(MIB), None);
        assert_eq!(s.confidence(), 0.4);
    }

    #[test]
    fn steady_peaks_get_a_small_margin_and_noisy_ones_a_large_one() {
        let steady = stats((0..10).map(|i| ok(1.0, 1.0, 100 * MIB + i * 64 * 1024, MIB))).summary();
        let m = steady.memory_for(MIB).expect("estimate");
        assert!(m.bytes >= 120 * MIB && m.bytes < 122 * MIB, "{} MiB", m.bytes / MIB);
        let noisy = stats((0..10).map(|i| ok(1.0, 1.0, if i % 2 == 0 { 50 * MIB } else { 200 * MIB }, MIB))).summary();
        assert_eq!(noisy.memory_for(MIB).expect("estimate").bytes, 400 * MIB);
    }

    #[test]
    fn an_oom_is_a_lower_bound_and_widens_the_margin() {
        let mut s = stats((0..5).map(|_| ok(1.0, 1.0, 100 * MIB, MIB)));
        s.push(Sample { peak: 300 * MIB, arg_bytes: MIB, oom: true, ..Default::default() });
        let sum = s.summary();
        let m = sum.memory_for(MIB).expect("estimate");
        // 1.5× the kill would be 450 MiB; capped at 4× the usual 100 MiB,
        // which is still above the 300 MiB the killed call reached.
        assert_eq!(m.bytes, 400 * MIB, "{}", m.evidence);
        // A smaller input than the one that was killed is not held to its bound.
        assert_eq!(sum.memory_for(1024).expect("estimate").bytes, 200 * MIB);
    }

    #[test]
    fn one_kill_fades_as_later_calls_succeed_below_it() {
        let reserve = |later: usize| {
            let mut s = stats((0..5).map(|_| ok(1.0, 1.0, 100 * MIB, MIB)));
            s.push(Sample { peak: 900 * MIB, arg_bytes: MIB, oom: true, ..Default::default() });
            for _ in 0..later {
                s.push(ok(1.0, 1.0, 100 * MIB, MIB));
            }
            let sum = s.summary();
            (sum.memory_for(MIB).expect("estimate").bytes / MIB, sum.has_oom())
        };
        // At the kill itself, not 1.5× it: 9× what calls of that size use.
        assert_eq!(reserve(0), (900, true));
        // Each 8 successes halve the excess over what they used.
        assert_eq!(reserve(8), (500, true));
        assert_eq!(reserve(16), (400, true));
        // Within 10% of the calls that succeed, the kill no longer counts:
        // a steady 100 MiB gets its usual 1.2× margin.
        assert_eq!(reserve(56), (120, false));
    }

    #[test]
    fn an_oom_with_no_successes_still_sizes_the_retry() {
        let s = stats([Sample { peak: 1 << 30, arg_bytes: 0, oom: true, ..Default::default() }]).summary();
        assert_eq!(s.memory_for(0).expect("bound").bytes, 3 << 29);
    }

    #[test]
    fn memory_that_grows_with_input_is_predicted_per_call() {
        // peak = 50 MiB + 3 × input, inputs 1..=16 MiB, ±2% noise.
        let samples = (1..=16u64).map(|k| {
            let arg = k * MIB;
            let peak = ((50 * MIB + 3 * arg) as f64 * (1.0 + 0.02 * noise(k))) as u64;
            ok(1.0, 1.0, peak, arg)
        });
        let s = stats(samples).summary();
        let f = s.fit.expect("fit");
        assert!(f.r2 > 0.95 && (f.b - 3.0).abs() < 0.2, "{f:?}");
        let small = s.memory_for(MIB).expect("estimate").bytes;
        let big = s.memory_for(64 * MIB).expect("estimate").bytes;
        assert!(small < 70 * MIB && big > 280 * MIB, "{} {}", small / MIB, big / MIB);
    }

    #[test]
    fn unrelated_sizes_do_not_produce_a_fit() {
        let samples = (1..=16u64).map(|k| ok(1.0, 1.0, (100.0 * (1.0 + 0.5 * noise(k))) as u64 * MIB, k * MIB));
        let s = stats(samples).summary();
        assert_eq!(s.fit, None);
        assert!(s.memory_for(4 * MIB).is_some());
        // Far beyond any observed input, with no model: unknown.
        assert_eq!(s.memory_for(1 << 30), None);
    }

    #[test]
    fn cpu_grows_when_calls_use_more_than_granted() {
        let s = stats((0..3).map(|_| ok(1.0, 3.4, 0, 0))).summary();
        assert_eq!(s.cpu_grow(), Some(4.0));
        let fits = stats((0..3).map(|_| ok(1.0, 1.1, 0, 0))).summary();
        assert_eq!(fits.cpu_grow(), None);
        // Tiny calls never grow: their CPU is mostly the runtime's own work.
        let tiny = stats((0..30).map(|_| ok(0.0002, 12.0, 0, 0))).summary();
        assert_eq!(tiny.cpu_grow(), None);
    }

    #[test]
    fn io_bound_calls_propose_a_quarter_core_and_the_vetoes_hold() {
        let io = stats((0..10).map(|i| ok(0.2, 0.05 + 0.01 * noise(i), 0, 0))).summary();
        assert!(io.io_bound());
        assert_eq!(io.cpu_shrink(false), Some(0.25));
        assert_eq!(io.cpu_shrink(true), None, "memory kills in the window");
        let few = stats((0..9).map(|_| ok(0.2, 0.05, 0, 0))).summary();
        assert_eq!(few.cpu_shrink(false), None, "under 10 samples");
        let short = stats((0..20).map(|_| ok(0.01, 0.05, 0, 0))).summary();
        assert_eq!(short.cpu_shrink(false), None, "tasks under 50 ms");
        let spiky = stats((0..20).map(|i| ok(0.2, if i % 2 == 0 { 0.02 } else { 0.9 }, 0, 0))).summary();
        assert_eq!(spiky.cpu_shrink(false), None, "CV over 0.5");
        // Relative variation of a tiny use is not a reason to keep a core.
        let sleepy = stats((0..21).map(|i| ok(0.2, if i % 3 == 0 { 0.003 } else { 0.0003 }, 0, 0))).summary();
        assert!(sleepy.cores_cv > 0.5, "{}", sleepy.cores_cv);
        assert_eq!(sleepy.cpu_shrink(false), Some(0.25));
        let busy = stats((0..20).map(|_| ok(0.2, 0.97, 0, 0))).summary();
        assert_eq!(busy.cpu_shrink(false), None, "already right-sized");
    }

    #[test]
    fn confidence_follows_the_sample_count() {
        let c = |n| Summary { n, ..Default::default() }.confidence();
        assert_eq!((c(1), c(5), c(20)), (0.2, 0.4, 0.6));
        assert!(c(50) > 0.75 && c(10_000) < 0.9);
    }

    #[test]
    fn a_prior_is_used_until_local_samples_suffice() {
        let remote = stats((0..20).map(|_| ok(1.0, 1.0, 500 * MIB, MIB))).summary();
        let mut s = stats([ok(1.0, 1.0, 100 * MIB, MIB)]);
        s.prior = Some(remote.clone());
        assert_eq!(s.summary().peak_p95, 500 * MIB);
        s.push(ok(1.0, 1.0, 100 * MIB, MIB));
        s.push(ok(1.0, 1.0, 100 * MIB, MIB));
        assert_eq!(s.summary().peak_p95, 100 * MIB);
    }

    #[test]
    fn merging_errs_toward_more_memory() {
        let a = stats((0..10).map(|_| ok(1.0, 1.0, 100 * MIB, MIB))).summary();
        let mut b = stats((0..30).map(|_| ok(1.0, 2.0, 300 * MIB, MIB))).summary();
        b.oom_bounds = vec![(3, 400 * MIB)];
        let m = a.merge(&b);
        assert_eq!((m.n, m.peak_p95), (40, 300 * MIB));
        assert!((m.cores_mean - 1.75).abs() < 1e-9);
        assert_eq!(m.oom_bounds, vec![(3, 400 * MIB)]);
        assert_eq!(m, b.merge(&a).merge(&Summary::default()).clone().merge(&Summary::default()));
    }

    #[test]
    fn summaries_are_deterministic() {
        let xs: Vec<Sample> = (0..64).map(|i| ok(0.1 + noise(i).abs(), 1.0, (100 + i) * MIB, i * MIB)).collect();
        assert_eq!(summarize(xs.iter()), summarize(xs.iter()));
    }

    #[test]
    fn a_planted_leak_retires_the_executor_and_noise_does_not() {
        let mut w = LeakWatch::default();
        let base = 400 * MIB;
        let mut verdict = None;
        for i in 0..40u64 {
            let b = (base as f64 + (5 * MIB * i) as f64 + 2.0 * MIB as f64 * noise(i)) as u64;
            verdict = w.after_call(b);
            if verdict.is_some() {
                assert!(i < 32, "retired late, at call {i}");
                break;
            }
        }
        assert!(verdict.is_some(), "a 5 MiB-per-call leak was never retired");

        let mut steady = LeakWatch::default();
        for i in 0..500u64 {
            let b = (base as f64 + 20.0 * MIB as f64 * noise(i)) as u64;
            assert_eq!(steady.after_call(b), None, "noise retired at call {i}");
        }
    }

    #[test]
    fn a_large_jump_in_retained_memory_retires_and_a_modest_one_does_not() {
        let mut w = LeakWatch::default();
        assert_eq!(w.after_call(100 * MIB), None);
        assert_eq!(w.after_call(400 * MIB), None);
        assert_eq!(w.after_call(410 * MIB), None);
        assert!(w.after_call(800 * MIB).is_some(), "over 2× the first plus 512 MiB");
    }

    /// Run a CPU trial where an attempt at `cpus` takes `wall(cpus, i)`.
    fn run_trial(wall: impl Fn(f64, u64) -> f64) -> Verdict {
        let mut t = CpuTrial::new(1.0, 0.25, 0.0);
        let mut now = 0.0;
        for i in 0..400u64 {
            let (cpus, canary) = t.assign();
            let w = wall(cpus, i);
            now += w / 4.0;
            t.observe(canary, w, false);
            if let Some(v) = t.tick(now) {
                return v;
            }
        }
        panic!("no verdict after 400 calls ({} pairs)", t.pairs());
    }

    #[test]
    fn a_waiting_function_is_adopted_at_a_quarter_core() {
        // Sleeps 200 ms ± 10% whatever its grant.
        let v = run_trial(|_, i| 0.2 * (1.0 + 0.1 * noise(i)));
        assert!(matches!(v, Verdict::Adopt { mean_gain, .. } if mean_gain > 2.0), "{v:?}");
        // A 100 ms sleep whose canary wakes up to 8 ms late (more processes
        // share the cores) is still a win, not a latency regression.
        let v = run_trial(|cpus, i| 0.1 + if cpus < 1.0 { 0.004 + 0.004 * noise(i) } else { 0.0 });
        assert!(matches!(v, Verdict::Adopt { .. }), "{v:?}");
    }

    #[test]
    fn a_computing_function_is_reverted() {
        // Needs a full core: at a quarter core it shares and takes 4× longer.
        let v = run_trial(|cpus, i| 0.2 / cpus.min(1.0) * (1.0 + 0.1 * noise(i)));
        assert!(matches!(v, Verdict::Revert { .. }), "{v:?}");
    }

    #[test]
    fn an_oom_in_the_canary_reverts_at_once() {
        let mut t = CpuTrial::new(1.0, 0.25, 0.0);
        t.observe(false, 0.2, false);
        t.observe(true, 0.2, true);
        assert!(matches!(t.tick(0.1), Some(Verdict::Revert { .. })));
    }

    #[test]
    fn mann_kendall_detects_trends_only() {
        let up: Vec<f64> = (0..20).map(|i| i as f64 + noise(i) * 2.0).collect();
        assert!(increasing_trend(&up));
        let flat: Vec<f64> = (0..20).map(noise).collect();
        assert!(!increasing_trend(&flat));
        assert!(!increasing_trend(&[1.0; 20]));
        let down: Vec<f64> = (0..20).map(|i| -(i as f64)).collect();
        assert!(!increasing_trend(&down));
    }
}
