//! Controllers that change a setting only when a canary beats a control.
//!
//! A change is proposed for one knob at a time and measured in paired
//! windows: the control keeps the current value, the canary runs the
//! candidate on comparable work (same input-size bucket and hardware class)
//! at the same time. For each pair `d = canary/control - 1`. The change is
//! adopted when the mean gain clears the threshold and the one-sided 95%
//! Student-t lower bound clears half of it, while the guards hold (tail
//! latency, reserved memory, no new OOM kills). It is reverted when a guard
//! fails, when the upper bound falls below the threshold, or when the pair
//! budget runs out. A revert starts a cooldown that doubles with each
//! consecutive revert. Pure logic: the peer feeds measurements in and applies
//! the decisions.
//!
//! Why the t bound: with ±15% window noise and no real gain, "mean >= 8% and
//! mean - 2 SE > 0" adopted 11.5% of null changes (it checks after every pair
//! with a normal quantile and a three-sample variance). This rule adopts 3.7%
//! of null changes and 93% of real 20% gains (`noise_*` test, 200 runs).

/// Measurements of one window of one arm.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Window {
    /// Useful completions per second (the objective).
    pub useful_per_s: f64,
    pub p99_s: f64,
    pub peak_reserved_bytes: u64,
    pub oom_kills: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub min_improvement: f64,
    pub min_pairs: usize,
    pub max_pairs: usize,
    pub p99_ratio_guard: f64,
    /// Memory budget the canary's reservation must stay within.
    pub budget_bytes: u64,
    /// Fraction of the budget kept free by the pressure clamp.
    pub headroom: f64,
    /// Windows of cooldown after a revert, doubled per consecutive revert.
    pub cooldown_windows: u32,
    pub max_cooldown_windows: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            min_improvement: 0.08,
            min_pairs: 3,
            max_pairs: 8,
            p99_ratio_guard: 1.05,
            budget_bytes: 5 << 30,
            headroom: 0.10,
            cooldown_windows: 5,
            max_cooldown_windows: 120,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    Adopt { mean_gain: f64, se: f64 },
    Revert { reason: String, mean_gain: f64 },
    Continue,
}

/// Judge paired windows `(control, canary)`.
pub fn judge(pairs: &[(Window, Window)], cfg: &Config) -> Verdict {
    for (a, b) in pairs {
        if b.oom_kills > a.oom_kills {
            return Verdict::Revert { reason: "canary had OOM kills".into(), mean_gain: 0.0 };
        }
        if b.peak_reserved_bytes > cfg.budget_bytes {
            return Verdict::Revert { reason: "canary exceeded the memory budget".into(), mean_gain: 0.0 };
        }
        if a.p99_s > 0.0 && b.p99_s > cfg.p99_ratio_guard * a.p99_s {
            return Verdict::Revert { reason: "canary p99 latency regressed".into(), mean_gain: 0.0 };
        }
    }
    let d: Vec<f64> = pairs
        .iter()
        .filter(|(a, _)| a.useful_per_s > 0.0)
        .map(|(a, b)| b.useful_per_s / a.useful_per_s - 1.0)
        .collect();
    let n = d.len();
    if n < cfg.min_pairs.max(2) {
        return Verdict::Continue;
    }
    let mean = d.iter().sum::<f64>() / n as f64;
    let var = d.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
    let se = (var / n as f64).sqrt();
    let t = t_one_sided_95(n - 1);
    if mean >= cfg.min_improvement && mean - t * se >= cfg.min_improvement / 2.0 {
        return Verdict::Adopt { mean_gain: mean, se };
    }
    if mean + t * se < cfg.min_improvement {
        return Verdict::Revert {
            reason: format!("gain {:.1}% is below the {:.0}% threshold", mean * 100.0, cfg.min_improvement * 100.0),
            mean_gain: mean,
        };
    }
    if n >= cfg.max_pairs {
        return Verdict::Revert { reason: "no decisive gain within the pair budget".into(), mean_gain: mean };
    }
    Verdict::Continue
}

/// One-sided 95% Student-t quantile for `df` degrees of freedom.
fn t_one_sided_95(df: usize) -> f64 {
    const T: [f64; 12] = [f64::INFINITY, 6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812, 1.796];
    T.get(df).copied().unwrap_or(1.7)
}

/// One recorded decision (mirrors `pb::Decision` without the proto types).
#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    pub knob: &'static str,
    pub old: u64,
    pub new: u64,
    pub outcome: &'static str,
    pub evidence: String,
    pub reason: String,
}

/// Prefetch depth (input buffers ahead of a consumer), on a power-of-two ladder.
#[derive(Clone, Debug)]
pub struct Prefetch {
    pub cfg: Config,
    pub depth: u64,
    pub max_depth: u64,
    /// Memory held regardless of depth (model, runtime).
    pub base_bytes: u64,
    experiment: Option<(u64, Vec<(Window, Window)>)>,
    consecutive_reverts: u32,
    cooldown: u32,
    increases_frozen: u32,
    pub decisions: Vec<Decision>,
}

impl Prefetch {
    pub fn new(cfg: Config, depth: u64, max_depth: u64, base_bytes: u64) -> Prefetch {
        Prefetch {
            cfg,
            depth: depth.max(1),
            max_depth,
            base_bytes,
            experiment: None,
            consecutive_reverts: 0,
            cooldown: 0,
            increases_frozen: 0,
            decisions: Vec::new(),
        }
    }

    /// The value the canary arm should run, if an experiment is in progress.
    pub fn canary(&self) -> Option<u64> {
        self.experiment.as_ref().map(|(c, _)| *c)
    }

    fn reserved(&self, depth: u64, buffer_bytes: u64) -> u64 {
        self.base_bytes + depth * buffer_bytes
    }

    fn fits(&self, depth: u64, buffer_bytes: u64) -> bool {
        self.reserved(depth, buffer_bytes) as f64 <= (1.0 - self.cfg.headroom) * self.cfg.budget_bytes as f64
    }

    /// Consider starting an experiment. `input_bound` is the binding-stage
    /// evidence: the consumer waits for input (and is not itself saturated).
    pub fn maybe_propose(&mut self, input_bound: bool, buffer_bytes: u64) -> Option<u64> {
        if self.experiment.is_some() || self.cooldown > 0 || self.increases_frozen > 0 || !input_bound {
            return None;
        }
        let next = (self.depth * 2).min(self.max_depth);
        if next == self.depth || !self.fits(next, buffer_bytes) {
            return None;
        }
        self.experiment = Some((next, Vec::new()));
        Some(next)
    }

    /// Feed one paired window. Returns the decision when the experiment ends.
    pub fn observe(&mut self, control: Window, canary: Window) -> Option<Decision> {
        let (candidate, mut pairs) = self.experiment.take()?;
        pairs.push((control, canary));
        let verdict = judge(&pairs, &self.cfg);
        let evidence = |pairs: &[(Window, Window)]| {
            let (a, b) = pairs.last().copied().unwrap_or_default();
            format!("{} pairs; last {:.2} -> {:.2} useful/s", pairs.len(), a.useful_per_s, b.useful_per_s)
        };
        match verdict {
            Verdict::Continue => {
                self.experiment = Some((candidate, pairs));
                None
            }
            Verdict::Adopt { mean_gain, se } => {
                let d = Decision {
                    knob: "prefetch",
                    old: self.depth,
                    new: candidate,
                    outcome: "adopted",
                    evidence: evidence(&pairs),
                    reason: format!(
                        "gain {:+.1}% (se {:.1}%) clears {:.0}%",
                        mean_gain * 100.0,
                        se * 100.0,
                        self.cfg.min_improvement * 100.0
                    ),
                };
                self.depth = candidate;
                self.consecutive_reverts = 0;
                self.decisions.push(d.clone());
                Some(d)
            }
            Verdict::Revert { reason, .. } => {
                self.consecutive_reverts += 1;
                self.cooldown =
                    (self.cfg.cooldown_windows << (self.consecutive_reverts - 1)).min(self.cfg.max_cooldown_windows);
                let d = Decision {
                    knob: "prefetch",
                    old: self.depth,
                    new: candidate,
                    outcome: "reverted",
                    evidence: evidence(&pairs),
                    reason: format!("{reason}; cooldown {} windows", self.cooldown),
                };
                self.decisions.push(d.clone());
                Some(d)
            }
        }
    }

    /// Call once per window with the current per-buffer size (e.g. p95 of
    /// input bytes). If the depth no longer fits the budget with headroom, it
    /// drops to the largest ladder value that does, and increases freeze for a
    /// window. Guards act immediately: they may only reduce exposure.
    pub fn tick(&mut self, buffer_bytes: u64) -> Option<Decision> {
        self.cooldown = self.cooldown.saturating_sub(1);
        self.increases_frozen = self.increases_frozen.saturating_sub(1);
        if self.fits(self.depth, buffer_bytes) {
            return None;
        }
        let mut d = self.depth;
        while d > 1 && !self.fits(d, buffer_bytes) {
            d /= 2;
        }
        self.experiment = None;
        let old = self.depth;
        self.depth = d;
        self.increases_frozen = 2;
        let dec = Decision {
            knob: "prefetch",
            old,
            new: d,
            outcome: "clamped",
            evidence: format!(
                "buffer {} MiB: base {} MiB + {old}x{} MiB = {} MiB > {:.0}% of the {} MiB budget",
                buffer_bytes >> 20,
                self.base_bytes >> 20,
                buffer_bytes >> 20,
                self.reserved(old, buffer_bytes) >> 20,
                (1.0 - self.cfg.headroom) * 100.0,
                self.cfg.budget_bytes >> 20
            ),
            reason: "memory guard: largest power-of-two depth within budget; increases frozen".into(),
        };
        self.decisions.push(dec.clone());
        Some(dec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The design doc's synthetic pipeline: one device at 10 ms per batch,
    /// reads cycling 14/18/12/16 ms, a buffer held from read start until its
    /// batch finishes, reads independent, the device consuming in order.
    fn pipeline(depth: u64, n: usize) -> f64 {
        const READS: [f64; 4] = [14.0, 18.0, 12.0, 16.0];
        let mut free = vec![0.0f64; depth as usize];
        let mut device = 0.0f64;
        for i in 0..n {
            let b = (0..free.len()).min_by(|x, y| free[*x].total_cmp(&free[*y])).expect("depth >= 1");
            let ready = free[b] + READS[i % 4];
            let start = ready.max(device);
            device = start + 10.0;
            free[b] = device;
        }
        n as f64 / (device / 1000.0)
    }

    fn window(depth: u64, buffer: u64, base: u64) -> Window {
        Window {
            useful_per_s: pipeline(depth, 400),
            p99_s: 0.010,
            peak_reserved_bytes: base + depth * buffer,
            oom_kills: 0,
        }
    }

    #[test]
    fn pipeline_model_matches_the_design_doc() {
        let r: Vec<String> = [1, 2, 4, 8].iter().map(|d| format!("{:.2}", pipeline(*d, 400))).collect();
        assert_eq!(r, ["40.00", "73.99", "99.65", "99.65"]);
    }

    /// The doc's worked correction: adopt 2, adopt 4, revert 8, then clamp
    /// to 2 when buffers grow to 896 MiB.
    #[test]
    fn worked_prefetch_example() {
        let (base, mib160, mib896) = (2u64 << 30, 160u64 << 20, 896u64 << 20);
        let mut c = Prefetch::new(Config::default(), 1, 64, base);
        let mut outcomes = Vec::new();
        for _ in 0..200 {
            if c.tick(mib160).is_some() {
                break;
            }
            let input_bound = pipeline(c.depth, 400) < 99.0;
            if let Some(candidate) = c.maybe_propose(input_bound || c.depth < 8, mib160) {
                loop {
                    let d = c.observe(window(c.depth, mib160, base), window(candidate, mib160, base));
                    if let Some(d) = d {
                        outcomes.push((d.old, d.new, d.outcome));
                        break;
                    }
                }
            }
            if outcomes.len() == 3 {
                break;
            }
        }
        assert_eq!(outcomes, vec![(1, 2, "adopted"), (2, 4, "adopted"), (4, 8, "reverted")]);
        assert_eq!(c.depth, 4);
        assert!(c.decisions[2].reason.contains("below"), "{}", c.decisions[2].reason);
        // Inputs grow: 2 GiB + 4x896 MiB = 5,632 MiB exceeds the budget; 3x would fit
        // without headroom (4,736 MiB), so the ladder and the 10% headroom both matter.
        let clamp = c.tick(mib896).expect("clamped");
        assert_eq!((clamp.old, clamp.new, clamp.outcome), (4, 2, "clamped"));
        assert_eq!(c.depth, 2);
        assert_eq!(c.maybe_propose(true, mib896), None, "increases are frozen after a clamp");
    }

    /// Run one experiment of up to `max_pairs` paired windows under ±15%
    /// multiplicative noise; true gain `gain`. Returns the verdict.
    fn noisy_experiment(gain: f64, seed: u64) -> Verdict {
        let mut s = seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
        let mut noise = || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            1.0 + ((s >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.30
        };
        let cfg = Config::default();
        let mut pairs = Vec::new();
        loop {
            let a = Window { useful_per_s: 100.0 * noise(), p99_s: 0.01, ..Default::default() };
            let b = Window { useful_per_s: 100.0 * (1.0 + gain) * noise(), p99_s: 0.01, ..Default::default() };
            pairs.push((a, b));
            let v = judge(&pairs, &cfg);
            if pairs.len() < cfg.min_pairs {
                assert_eq!(v, Verdict::Continue, "fewer than min_pairs never decides");
            }
            if v != Verdict::Continue {
                return v;
            }
        }
    }

    #[test]
    fn noise_does_not_produce_adoptions_and_real_gains_are_found() {
        let runs: usize = 200;
        let false_adopts =
            (0..runs).filter(|s| matches!(noisy_experiment(0.0, *s as u64), Verdict::Adopt { .. })).count();
        let true_adopts =
            (0..runs).filter(|s| matches!(noisy_experiment(0.30, *s as u64), Verdict::Adopt { .. })).count();
        assert!(false_adopts * 20 <= runs, "{false_adopts}/{runs} adoptions with no real gain");
        assert!(true_adopts * 10 >= runs * 9, "only {true_adopts}/{runs} adoptions of a real 30% gain");
    }

    #[test]
    fn guards_revert_immediately() {
        let cfg = Config::default();
        let a = Window { useful_per_s: 100.0, p99_s: 0.010, peak_reserved_bytes: 1 << 30, oom_kills: 0 };
        let slow_tail = Window { useful_per_s: 150.0, p99_s: 0.020, ..a };
        assert!(matches!(judge(&[(a, slow_tail)], &cfg), Verdict::Revert { .. }));
        let oom = Window { useful_per_s: 150.0, oom_kills: 1, ..a };
        assert!(matches!(judge(&[(a, oom)], &cfg), Verdict::Revert { .. }));
    }

    #[test]
    fn steady_state_does_not_oscillate() {
        // At the throughput ceiling no proposal is made and nothing changes.
        let mut c = Prefetch::new(Config::default(), 4, 64, 2 << 30);
        for _ in 0..10 {
            assert!(c.tick(160 << 20).is_none());
            assert_eq!(c.maybe_propose(false, 160 << 20), None);
        }
        assert_eq!(c.depth, 4);
        assert!(c.decisions.is_empty());
    }

    #[test]
    fn consecutive_reverts_double_the_cooldown() {
        let mut c = Prefetch::new(Config::default(), 1, 64, 0);
        let same = Window { useful_per_s: 100.0, p99_s: 0.01, ..Default::default() };
        let mut cooldowns = Vec::new();
        for _ in 0..3 {
            while c.maybe_propose(true, 1 << 20).is_none() {
                c.tick(1 << 20);
            }
            while c.observe(same, same).is_none() {}
            cooldowns.push(c.cooldown);
        }
        assert_eq!(cooldowns, vec![5, 10, 20]);
    }
}
