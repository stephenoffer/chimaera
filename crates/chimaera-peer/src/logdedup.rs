//! Repeated output across workers, collapsed on its way to the driver: a line
//! that several of a run's worker processes print within [`WINDOW`] reaches
//! the driver once, then once more as `<line> [repeated Nx across workers]`
//! when the window closes. The first copy is never held back, and a worker
//! repeating its own line is not collapsed. The run's kept log (`chimaera
//! logs`) has every line. `CHIMAERA_DEDUP_LOGS=0` turns it off.
//!
//! Lines are compared exactly. Ray also merges lines that differ only in
//! their numbers, which would fold one rank's "loss 0.31" into another's
//! "loss 0.27".

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chimaera_core::ids::Id;
use chimaera_core::pb;

pub const WINDOW: Duration = Duration::from_secs(5);
/// Lines tracked per run at once; past it, new lines pass through untracked.
const MAX_LINES: usize = 10_000;

struct Seen {
    since: Instant,
    pids: HashSet<u32>,
    held: u32,
    /// The last held copy: its task and process name the summary line.
    last: pb::LogEvent,
}

pub struct LogDedup {
    on: bool,
    runs: HashMap<Id, HashMap<(String, String), Seen>>,
}

impl Default for LogDedup {
    fn default() -> Self {
        LogDedup { on: std::env::var("CHIMAERA_DEDUP_LOGS").map_or(true, |v| v.trim() != "0"), runs: HashMap::new() }
    }
}

impl LogDedup {
    /// What of `ev` the run's drivers should see now: `ev` itself, or a copy
    /// without the lines held back (None if every line was). An event's text
    /// is whole lines joined by newlines, as the executor sends them.
    pub fn filter(&mut self, run: Id, ev: &pb::LogEvent, now: Instant) -> Option<pb::LogEvent> {
        if !self.on {
            return Some(ev.clone());
        }
        let lines = self.runs.entry(run).or_default();
        let (mut kept, mut held_any) = (Vec::new(), false);
        for line in ev.text.split('\n') {
            if line.trim().is_empty() {
                kept.push(line);
                continue;
            }
            let key = (ev.stream.clone(), line.to_string());
            let full = lines.len() >= MAX_LINES;
            match lines.get_mut(&key) {
                Some(seen) if now.duration_since(seen.since) < WINDOW && !seen.pids.contains(&ev.pid) => {
                    seen.pids.insert(ev.pid);
                    seen.held += 1;
                    seen.last = pb::LogEvent { text: line.to_string(), ..ev.clone() };
                    held_any = true;
                }
                Some(seen) if now.duration_since(seen.since) < WINDOW => kept.push(line),
                None if full => kept.push(line),
                _ => {
                    let last = pb::LogEvent { text: line.to_string(), ..ev.clone() };
                    lines.insert(key, Seen { since: now, pids: HashSet::from([ev.pid]), held: 0, last });
                    kept.push(line);
                }
            }
        }
        if held_any && kept.iter().all(|l| l.trim().is_empty()) {
            return None;
        }
        Some(pb::LogEvent { text: kept.join("\n"), ..ev.clone() })
    }

    /// Lines whose window closed: forget them, and return one summary event
    /// for each that had copies held back.
    pub fn expire(&mut self, now: Instant) -> Vec<(Id, pb::LogEvent)> {
        let mut out = Vec::new();
        self.runs.retain(|run, lines| {
            lines.retain(|_, seen| {
                if now.duration_since(seen.since) < WINDOW {
                    return true;
                }
                if seen.held > 0 {
                    let summary = format!("{} [repeated {}x across workers]", seen.last.text, seen.held);
                    out.push((*run, pb::LogEvent { text: summary, ..seen.last.clone() }));
                }
                false
            });
            !lines.is_empty()
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(pid: u32, text: &str) -> pb::LogEvent {
        pb::LogEvent { task: None, stream: "stdout".into(), text: text.into(), pid }
    }

    #[test]
    fn a_line_from_several_workers_reaches_the_driver_once_then_as_a_count() {
        let mut d = LogDedup { on: true, runs: HashMap::new() };
        let (run, t0) = (Id([1; 16]), Instant::now());
        assert_eq!(d.filter(run, &ev(1, "warn: slow disk\nrank 1"), t0).unwrap().text, "warn: slow disk\nrank 1");
        assert_eq!(d.filter(run, &ev(2, "warn: slow disk\nrank 2"), t0).unwrap().text, "rank 2");
        assert!(d.filter(run, &ev(3, "warn: slow disk"), t0).is_none(), "every line held back");
        // The same worker repeating itself is not collapsed.
        assert_eq!(d.filter(run, &ev(1, "warn: slow disk"), t0).unwrap().text, "warn: slow disk");
        // Another stream, and a line that only starts the same, pass through.
        let err = pb::LogEvent { stream: "stderr".into(), ..ev(2, "warn: slow disk") };
        assert_eq!(d.filter(run, &err, t0).unwrap().text, "warn: slow disk");
        assert_eq!(d.filter(run, &ev(2, "warn: slow"), t0).unwrap().text, "warn: slow");
        assert!(d.expire(t0 + WINDOW / 2).is_empty(), "the window is still open");
        let summaries = d.expire(t0 + WINDOW);
        assert_eq!(summaries.len(), 1, "{:?}", summaries.iter().map(|(_, e)| &e.text).collect::<Vec<_>>());
        assert_eq!(summaries[0].1.text, "warn: slow disk [repeated 2x across workers]");
        assert_eq!(summaries[0].1.pid, 3, "named after the last copy held back");
        // After the window the line prints again.
        assert_eq!(d.filter(run, &ev(4, "warn: slow disk"), t0 + WINDOW).unwrap().text, "warn: slow disk");
    }

    #[test]
    fn runs_are_kept_apart_and_off_means_untouched() {
        let mut d = LogDedup { on: true, runs: HashMap::new() };
        let t0 = Instant::now();
        d.filter(Id([1; 16]), &ev(1, "same"), t0);
        assert_eq!(d.filter(Id([2; 16]), &ev(2, "same"), t0).unwrap().text, "same");
        let mut off = LogDedup { on: false, runs: HashMap::new() };
        off.filter(Id([1; 16]), &ev(1, "same"), t0);
        assert_eq!(off.filter(Id([1; 16]), &ev(2, "same"), t0).unwrap().text, "same");
    }
}
