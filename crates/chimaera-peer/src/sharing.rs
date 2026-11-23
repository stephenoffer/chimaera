//! Calls sharing one executor process while one waits.
//!
//! A pooled executor runs at most one call that is not waiting on futures.
//! When every call on it waits, and each said it may host (`ExecBlocked.host`),
//! the peer may send it another call of the same run (a guest), which runs on
//! a thread of its own. A deep tree of calls waiting on their children then
//! needs about one process per CPU instead of one per waiting call.
//!
//! The process's CPU and memory readings are shared, so they are attributed
//! here: to the one call not waiting at that moment, measured from what the
//! process held when it started (its baseline). A moment when two calls ran at
//! once (a host resumed while its guest ran) leaves both unmeasured.

use std::sync::OnceLock;

/// Calls one process may hold at once, the first included: each guest is a
/// thread with its own stack. A deeper chain of waits starts processes, as
/// without hosting.
pub const MAX_HOSTED: usize = 16;

/// `CHIMAERA_HOST_WHILE_WAITING=0` turns hosting off (an empty value is unset).
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("CHIMAERA_HOST_WHILE_WAITING").map_or(true, |v| v.trim() != "0"))
}

/// One call on an executor, as hosting sees it.
#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// Waiting on futures (or a store or queue).
    pub blocked: bool,
    /// Its latest wait offered to host.
    pub host: bool,
    /// Calls in its Execute (a batch's helpers already overlap it).
    pub items: usize,
}

/// Every call there waits and offered to host, with room for one more.
pub fn can_host(entries: &[Entry]) -> bool {
    !entries.is_empty() && entries.len() < MAX_HOSTED && entries.iter().all(|e| e.blocked && e.host && e.items == 1)
}

/// The one call not waiting, if exactly one is: a process-wide reading is its.
pub fn sole_active(active: &[bool]) -> Option<usize> {
    let mut it = active.iter().enumerate().filter(|(_, a)| **a).map(|(i, _)| i);
    let first = it.next()?;
    it.next().is_none().then_some(first)
}

/// Process CPU used since the last change on a shared executor: it goes to
/// the one call that was not waiting. With several running at once none of
/// them is measured; with none, it is the waiting calls' few cycles, kept by
/// no one.
pub fn attribute_cpu(delta_ns: u64, active: &[bool], own: &mut [Option<u64>]) {
    match sole_active(active) {
        Some(i) => own[i] = own[i].map(|c| c + delta_ns),
        None => {
            for (o, a) in own.iter_mut().zip(active) {
                if *a {
                    *o = None;
                }
            }
        }
    }
}

/// A memory sample of a shared process: the index of the call it belongs to
/// and that call's own memory (what the process holds beyond its baseline).
pub fn attribute_memory(sample: u64, active: &[bool], baselines: &[u64]) -> Option<(usize, u64)> {
    sole_active(active).map(|i| (i, sample.saturating_sub(baselines[i])))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(blocked: bool, host: bool, items: usize) -> Entry {
        Entry { blocked, host, items }
    }

    #[test]
    fn hosts_only_when_every_call_waits_and_offered() {
        assert!(can_host(&[e(true, true, 1)]));
        assert!(can_host(&[e(true, true, 1), e(true, true, 1)]));
        assert!(!can_host(&[]));
        assert!(!can_host(&[e(false, true, 1)]), "a running call");
        assert!(!can_host(&[e(true, false, 1)]), "a poll, a gang rank, an actor");
        assert!(!can_host(&[e(true, true, 3)]), "a batch already overlaps");
        assert!(!can_host(&[e(true, true, 1), e(false, false, 1)]), "a guest still running");
        assert!(!can_host(&vec![e(true, true, 1); MAX_HOSTED]), "full");
    }

    #[test]
    fn cpu_goes_to_the_one_call_running() {
        let mut own = [Some(10), Some(0)];
        attribute_cpu(5, &[false, true], &mut own);
        assert_eq!(own, [Some(10), Some(5)]);
        attribute_cpu(7, &[true, false], &mut own);
        assert_eq!(own, [Some(17), Some(5)]);
        // All waiting: nobody's.
        attribute_cpu(3, &[false, false], &mut own);
        assert_eq!(own, [Some(17), Some(5)]);
        // Two at once: neither can be measured from here on.
        attribute_cpu(4, &[true, true], &mut own);
        assert_eq!(own, [None, None]);
        attribute_cpu(4, &[true, false], &mut own);
        assert_eq!(own, [None, None]);
    }

    #[test]
    fn memory_is_measured_from_the_baseline() {
        // A host holding 300 MiB waits; its guest grows the process to 320.
        assert_eq!(attribute_memory(320, &[false, true], &[0, 300]), Some((1, 20)));
        assert_eq!(attribute_memory(290, &[false, true], &[0, 300]), Some((1, 0)));
        assert_eq!(attribute_memory(320, &[true, true], &[0, 300]), None);
        assert_eq!(attribute_memory(320, &[false, false], &[0, 300]), None);
    }
}
