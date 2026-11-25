//! Workers that exit before they connect: an import error, a file in the
//! program's directory named like a module the worker needs, a broken
//! interpreter or environment. The peer would start another for the same
//! queued call forever, and the call would wait with no reason given (Ray's
//! "worker never registers"). After a few in a row for one kind of worker, its
//! queued calls fail with the last worker's own error instead.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Workers of one kind that exit before connecting, in a row, before the
/// calls queued for that kind fail.
pub const IN_A_ROW: u32 = 3;
/// Older failures are forgotten: the program may have been fixed since.
const FORGET_AFTER: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct StartFailures {
    /// Started and not yet connected: executor id and its kind (pool key).
    starting: HashMap<u32, String>,
    /// Per kind: failures in a row, the last one's detail, and when.
    failed: HashMap<String, (u32, String, Instant)>,
}

impl StartFailures {
    pub fn spawned(&mut self, exec: u32, key: &str) {
        self.starting.insert(exec, key.to_string());
    }

    /// `exec` connected: workers of its kind start.
    pub fn ready(&mut self, exec: u32) {
        if let Some(key) = self.starting.remove(&exec) {
            self.failed.remove(&key);
        }
    }

    /// `exec` exited. It counts when it never connected and exited by itself
    /// with a failure (`clean` covers a zero status and a signal, which the
    /// peer or the system sent).
    pub fn exited(&mut self, exec: u32, clean: bool, detail: &str) {
        let Some(key) = self.starting.remove(&exec) else { return };
        if clean {
            return;
        }
        let now = Instant::now();
        let e = self.failed.entry(key).or_insert((0, String::new(), now));
        let before = if now.duration_since(e.2) > FORGET_AFTER { 0 } else { e.0 };
        *e = (before + 1, detail.to_string(), now);
    }

    /// Why calls for `key` cannot run: IN_A_ROW of its workers exited before
    /// connecting, the last of them recently.
    pub fn gave_up(&self, key: &str) -> Option<String> {
        let (n, detail, at) = self.failed.get(key)?;
        (*n >= IN_A_ROW && at.elapsed() <= FORGET_AFTER)
            .then(|| format!("its workers exit before they start ({n} in a row); the last: {detail}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workers_that_keep_dying_before_they_connect_fail_their_calls() {
        let mut f = StartFailures::default();
        for id in 0..IN_A_ROW - 1 {
            f.spawned(id, "py|1");
            f.exited(id, false, "exit status: 1: ImportError: no module named x");
            assert_eq!(f.gave_up("py|1"), None, "not yet {IN_A_ROW} in a row");
        }
        f.spawned(9, "py|1");
        f.exited(9, false, "exit status: 1: ImportError: no module named x");
        let why = f.gave_up("py|1").expect("gave up");
        assert!(why.contains("3 in a row") && why.ends_with("ImportError: no module named x"), "{why}");
        assert_eq!(f.gave_up("py|2"), None, "another kind of worker is unaffected");
        // One that connects clears its kind.
        f.spawned(10, "py|1");
        f.ready(10);
        assert_eq!(f.gave_up("py|1"), None);
    }

    #[test]
    fn clean_exits_and_workers_that_had_connected_do_not_count() {
        let mut f = StartFailures::default();
        for id in 0..5 {
            f.spawned(id, "py|1");
            f.exited(id, true, "exit status: 0");
        }
        f.spawned(7, "py|1");
        f.ready(7);
        f.exited(7, false, "exit status: 1");
        f.exited(8, false, "exit status: 1"); // never seen spawning
        assert_eq!(f.gave_up("py|1"), None);
    }
}
