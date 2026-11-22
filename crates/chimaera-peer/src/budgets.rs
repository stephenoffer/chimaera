//! Budgets (`budget={"cpu_seconds": ..., "gpu_seconds": ...}`): what calls
//! under a budget use on this peer, charged to the budgeted call's shard,
//! which cancels it and everything it submitted once a limit is passed
//! (`Charge` in the shard).
//!
//! A call is charged while it runs, at most once a second, and once more when
//! its attempt ends, so one long call is stopped too. CPU seconds are the
//! worker's measured CPU when the call has the worker to itself, else its CPU
//! grant times the time it ran; GPU seconds are its GPUs times that time.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chimaera_core::pb;
use chimaera_core::shard::Tk;

/// How often a running call is charged.
pub const EVERY: Duration = Duration::from_secs(1);

#[derive(Default)]
pub struct Budgets {
    /// Per running attempt (executor, call, attempt): the worker's CPU counter
    /// (when it is the call's alone) and the time at its last charge.
    marks: HashMap<(u32, Tk, u32), (Option<u64>, Instant)>,
    /// Usage not yet sent, per budgeted call.
    due: HashMap<Tk, pb::Charge>,
}

/// The budgeted call that `policy` charges, with its limits; None without a budget.
pub fn root_of(tk: Tk, policy: &pb::Policy) -> Option<Tk> {
    if policy.budget_cpu_seconds <= 0.0 && policy.budget_gpu_seconds <= 0.0 {
        return None;
    }
    Some(policy.budget_root.as_ref().and_then(Tk::parse).unwrap_or(tk))
}

impl Budgets {
    /// Charge one running (or just ended) attempt for the time since its last
    /// charge. `started` is its CPU counter and time at dispatch; `cpu_ns` the
    /// counter now (None when the worker is shared); `cpus` its grant. Unless
    /// `ended`, nothing is charged within `EVERY` of the last charge.
    #[allow(clippy::too_many_arguments)]
    pub fn charge(
        &mut self,
        exec: u32,
        tk: Tk,
        attempt: u32,
        policy: &pb::Policy,
        started: (Option<u64>, Instant),
        cpu_ns: Option<u64>,
        cpus: f64,
        ended: bool,
    ) {
        let Some(root) = root_of(tk, policy) else { return };
        let mark = (exec, tk, attempt);
        let (last_cpu, last_at) = self.marks.get(&mark).copied().unwrap_or(started);
        let now = Instant::now();
        let dt = now.duration_since(last_at);
        if !ended && dt < EVERY {
            return;
        }
        let cpu_s = match (last_cpu, cpu_ns) {
            (Some(a), Some(b)) => b.saturating_sub(a) as f64 / 1e9,
            _ => cpus * dt.as_secs_f64(),
        };
        if ended {
            self.marks.remove(&mark);
        } else {
            self.marks.insert(mark, (cpu_ns.or(last_cpu), now));
        }
        let due = self.due.entry(root).or_insert_with(|| pb::Charge {
            root: Some(root.pb()),
            limit_cpu_seconds: policy.budget_cpu_seconds,
            limit_gpu_seconds: policy.budget_gpu_seconds,
            ..Default::default()
        });
        due.cpu_seconds += cpu_s;
        due.gpu_seconds += policy.gpus.max(0.0) * dt.as_secs_f64();
    }

    /// The charges to send now, one per budgeted call.
    pub fn take(&mut self) -> Vec<(Tk, pb::Charge)> {
        self.due.drain().collect()
    }

    /// Drop the marks of attempts that stopped without a last charge (killed, lost).
    pub fn keep(&mut self, live: &std::collections::HashSet<(u32, Tk, u32)>) {
        self.marks.retain(|m, _| live.contains(m));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimaera_core::ids::Id;

    fn tk(n: u8) -> Tk {
        Tk { run: Id([1; 16]), task: Id([n; 16]) }
    }

    fn budget(cpu: f64, gpu: f64, gpus: f64, root: Option<Tk>) -> pb::Policy {
        pb::Policy {
            budget_cpu_seconds: cpu,
            budget_gpu_seconds: gpu,
            budget_root: root.map(|r| r.pb()),
            gpus,
            ..Default::default()
        }
    }

    #[test]
    fn calls_charge_their_root_from_measured_cpu_or_their_grant() {
        let mut b = Budgets::default();
        let long_ago = Instant::now() - Duration::from_secs(10);
        assert_eq!(root_of(tk(1), &pb::Policy::default()), None, "no budget, no charge");
        // The root itself, measured: 3 s of CPU between dispatch and now.
        b.charge(
            7,
            tk(1),
            1,
            &budget(100.0, 50.0, 0.0, None),
            (Some(1_000_000_000), long_ago),
            Some(4_000_000_000),
            1.0,
            true,
        );
        // A child of tk(1) on a shared worker: its 2-CPU grant times ~10 s, and 1 GPU times ~10 s.
        b.charge(8, tk(2), 1, &budget(100.0, 50.0, 1.0, Some(tk(1))), (None, long_ago), None, 2.0, true);
        let charges = b.take();
        assert_eq!(charges.len(), 1, "both charge the same root");
        let (root, c) = &charges[0];
        assert_eq!(*root, tk(1));
        assert!((c.cpu_seconds - 23.0).abs() < 0.5, "{}", c.cpu_seconds);
        assert!((c.gpu_seconds - 10.0).abs() < 0.5, "{}", c.gpu_seconds);
        assert_eq!((c.limit_cpu_seconds, c.limit_gpu_seconds), (100.0, 50.0));
        assert!(b.take().is_empty());
    }

    #[test]
    fn a_running_call_is_charged_at_most_once_a_second_without_double_counting() {
        let mut b = Budgets::default();
        let p = budget(10.0, 0.0, 0.0, None);
        let started = (Some(0), Instant::now() - Duration::from_secs(5));
        b.charge(1, tk(1), 1, &p, started, Some(2_000_000_000), 1.0, false);
        b.charge(1, tk(1), 1, &p, started, Some(2_500_000_000), 1.0, false); // within a second: not yet
        assert_eq!(b.take()[0].1.cpu_seconds, 2.0);
        b.charge(1, tk(1), 1, &p, started, Some(3_000_000_000), 1.0, true); // ended: the rest
        assert_eq!(b.take()[0].1.cpu_seconds, 1.0, "only what was used since the last charge");
        assert!(b.marks.is_empty());
        b.charge(2, tk(3), 1, &p, started, None, 1.0, false);
        b.keep(&std::collections::HashSet::new()); // that attempt's worker died
        assert!(b.marks.is_empty());
    }
}
