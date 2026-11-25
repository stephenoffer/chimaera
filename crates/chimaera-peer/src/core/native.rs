//! CPU that waiting calls' own threads keep using.
//!
//! A call waiting on futures lends its CPUs so the calls it waits for can run
//! (`on_exec`, Blocked). Threads it started may run on through the wait,
//! though: a BLAS or OpenMP pool, a thread of its own. Every [`WINDOW`], the
//! CPU its process used beyond the grants of the calls running there (a
//! guest's) is taken back for it, up to what its waiting calls lent, so those
//! cores are not granted twice. A waiting process that only idles keeps
//! lending everything. A call that waits again (the next of its futures)
//! takes back at once what its threads used in its last wait, since they
//! have likely run on; the next window corrects it. What is taken back stays
//! charged to the waiting calls' tenant quota as well (`Quotas::keep`).
//!
//! Cores taken back must not keep the waited-for calls from running: when
//! only those cores stand in the way of the next call (or of an actor's
//! placement, or of the tenant's quota or share), it is let in anyway, over
//! the limit by its grant ([`Letting`]). That is at once while nothing else
//! granted CPU here runs or starts. Otherwise it waits for one of those to
//! end, and is let in only once nothing has started here for [`PATIENCE`],
//! since what runs may never end (a server loop). The next is let in only
//! once that one has ended, waits on other calls, or has idled for a whole
//! window, no thread of its process running or waiting for a CPU (a call
//! waiting on something of its own). So one grant at a time runs beside those
//! threads. An actor let in counts while it is placed or starting and while
//! it runs calls, including calls sent straight to it by their caller (the
//! direct path) while it runs one call at a time and none of its calls waits.
//! It keeps its CPUs once idle, as on lent CPUs, and its later calls start
//! without being let in: actors let in one after another, no more than
//! lending every CPU would have let in, may run calls at once.

use super::*;

/// How long a waiting process is watched before what it used counts.
const WINDOW: Duration = Duration::from_millis(250);

/// Cores a waiting process may use and still count as idle (its own wake-ups).
const IDLE: f64 = 0.1;

/// How long a call kept out only by those cores, while other calls run here,
/// waits for one of them to end (or for anything to start) before it is let in.
const PATIENCE: Duration = Duration::from_secs(2);

/// An executor whose calls have waited.
pub(super) struct Native {
    /// While a call there waits: when the current window began, and the
    /// process's CPU then.
    mark: Option<(Instant, u64)>,
    /// Cores taken back from what its waiting calls lent (part of `cpu_used`).
    cores: f64,
    /// What its waiting calls' threads used in the last window measured.
    last: f64,
}

/// What was let in over the cores taken back.
#[derive(Clone, Copy)]
enum To {
    Call(Tk),
    Actor(Id),
}

/// Letting calls in over the cores taken back.
#[derive(Default)]
pub(super) struct Letting {
    /// The last call or actor let in (over those cores, or over the quota
    /// they are charged to).
    lent: Option<Lent>,
    /// When a queued call last started here, or an actor was last placed here.
    progress: Option<Instant>,
}

struct Lent {
    to: To,
    /// While it runs: its process, how many of its calls run there, and when
    /// the current window began.
    mark: Option<(u32, usize, Instant)>,
    /// A thread of its process was seen running or waiting for a CPU in the
    /// current window.
    busy: bool,
    /// None was through the last whole window, nor since.
    idle: bool,
}

impl Lent {
    fn new(to: To) -> Lent {
        Lent { to, mark: None, busy: false, idle: false }
    }
}

/// Where what was let in stands.
enum Stand {
    /// Placed or starting: its CPU is about to be used.
    Starting,
    /// `usize` of its calls run in executor `u32`.
    Running(u32, usize),
    /// It runs nothing now: it ended, waits on other calls, or is an idle actor.
    Quiet,
}

/// Cores to take back: what the process used over `wall` beyond `running`
/// (the grants of its calls not waiting), up to `lent`; none when idle.
fn reclaim(cpu_ns: u64, wall: Duration, running: f64, lent: f64) -> f64 {
    let theirs = cpu_ns as f64 / wall.as_nanos().max(1) as f64 - running;
    if theirs < IDLE { 0.0 } else { theirs.min(lent) }
}

/// Whether a thread of process `pid` is running or waiting for a CPU (None
/// where that cannot be read). Unlike the CPU time it gets, that does not
/// depend on how loaded the host is.
#[cfg(target_os = "macos")]
fn runnable(pid: u32) -> Option<bool> {
    let size = std::mem::size_of::<libc::proc_taskinfo>() as i32;
    // SAFETY: proc_taskinfo is plain data; proc_pidinfo writes at most `size` bytes into it.
    let ti = unsafe {
        let mut ti: libc::proc_taskinfo = std::mem::zeroed();
        let at = (&mut ti as *mut libc::proc_taskinfo).cast();
        if libc::proc_pidinfo(pid as i32, libc::PROC_PIDTASKINFO, 0, at, size) != size {
            return None;
        }
        ti
    };
    Some(ti.pti_numrunning > 0)
}

#[cfg(target_os = "linux")]
fn runnable(pid: u32) -> Option<bool> {
    // State R, after the parenthesised command name in each thread's stat.
    let running = |stat: String| stat.rfind(')').is_some_and(|i| stat[i + 1..].trim_start().starts_with('R'));
    let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?;
    Some(tasks.flatten().any(|t| std::fs::read_to_string(t.path().join("stat")).is_ok_and(running)))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn runnable(_pid: u32) -> Option<bool> {
    None
}

impl Core {
    /// The grants of the calls on `exec` that wait (lent) and that run, and
    /// how many calls are there.
    fn grants_on(&self, exec: u32) -> (f64, f64, usize) {
        let (mut lent, mut running, mut calls) = (0.0, 0.0, 0);
        for r in self.running.iter().filter(|r| r.exec == exec) {
            calls += 1;
            if r.blocked {
                lent += r.cpus;
            } else {
                running += r.cpus;
            }
        }
        (lent, running, calls)
    }

    /// A call on `exec` began or stopped waiting, or ended: give back what was
    /// taken there. While a call there waits, take back what its threads last
    /// used and start a new window; the record goes once no call is there.
    pub(super) fn native_restart(&mut self, exec: u32) {
        let last = self.native.remove(&exec).map_or(0.0, |n| {
            self.cpu_used -= n.cores;
            n.last
        });
        let Some(pid) = self.execs.get(&exec).map(|e| e.pid) else { return };
        let (lent, _, calls) = self.grants_on(exec);
        if calls == 0 {
            return;
        }
        let mark = if lent > 0.0 { accounting::read(pid).map(|c| (Instant::now(), c.cpu_ns)) } else { None };
        let cores = if mark.is_some() { last.min(lent) } else { 0.0 };
        self.cpu_used += cores;
        self.native_keep(exec, cores);
        self.native.insert(exec, Native { mark, cores, last });
    }

    /// Charge `cores` taken back on `exec` to the quotas of the calls waiting
    /// there, each up to its own grant.
    fn native_keep(&mut self, exec: u32, mut cores: f64) {
        for r in self.running.iter().filter(|r| r.exec == exec && r.blocked) {
            if let Some(w) = r.items.first() {
                let mine = cores.min(r.cpus);
                self.quotas.keep((w.tk.run, w.tk.task), mine);
                cores -= mine;
            }
        }
    }

    /// Each window: take back what the threads of waiting calls used, and
    /// see whether what was let in beside them idles.
    pub(super) fn native_sample(&mut self) {
        let now = Instant::now();
        let due: Vec<u32> = self
            .native
            .iter()
            .filter(|(_, n)| n.mark.is_some_and(|(at, _)| now.duration_since(at) >= WINDOW))
            .map(|(e, _)| *e)
            .collect();
        for exec in due {
            let Some(cpu) = self.execs.get(&exec).and_then(|e| accounting::read(e.pid)) else { continue };
            let (lent, running, _) = self.grants_on(exec);
            let Some(n) = self.native.get_mut(&exec) else { continue };
            let Some((at, mark)) = n.mark else { continue };
            let cores = reclaim(cpu.cpu_ns.saturating_sub(mark), now.duration_since(at), running, lent);
            self.cpu_used += cores - n.cores;
            n.cores = cores;
            n.last = cores;
            n.mark = Some((now, cpu.cpu_ns));
            self.native_keep(exec, cores);
        }
        self.lent_sample(now);
    }

    fn native_cores(&self) -> f64 {
        self.native.values().map(|n| n.cores).sum()
    }

    /// Cores taken back from waiting calls that a new call or actor, waiting
    /// for `waited`, may have anyway (`native_free`).
    pub(super) fn native_yielding(&self, waited: Duration) -> f64 {
        let taken = self.native_cores();
        if taken > 0.0 && self.native_free(waited) { taken } else { 0.0 }
    }

    /// Whether a call waiting for `waited` may be let in over what waiting
    /// calls' threads keep: the last one let in has ended, waits or idles,
    /// and nothing else granted CPU here runs or starts, or nothing has
    /// started here for [`PATIENCE`] (what runs may never end).
    pub(super) fn native_free(&self, waited: Duration) -> bool {
        let lent = self.letting.lent.as_ref().is_none_or(|l| match self.stand(l.to) {
            Stand::Starting => false,
            Stand::Running(e, n) => l.idle && l.mark.is_some_and(|(me, mn, ..)| (me, mn) == (e, n)),
            Stand::Quiet => true,
        });
        let stalled = self.letting.progress.map_or(waited, |p| p.elapsed().min(waited)) >= PATIENCE;
        lent && (stalled || !self.granted_running())
    }

    /// Only those cores keep a call of `cpus`, waiting for `waited`, from
    /// starting: it starts.
    pub(super) fn native_in_the_way(&self, cpus: f64, waited: Duration) -> bool {
        let taken = self.native_yielding(waited);
        taken > 0.0 && self.cpu_used - taken + cpus <= self.capacity + 1e-9
    }

    /// Whether CPU granted here is in use, or about to be: by a call that does
    /// not wait (a task's, or an actor's or isolated attempt's, which run in
    /// their executor's CPUs), by an actor's or isolated attempt's process
    /// starting, or by an actor placed here that has not started.
    fn granted_running(&self) -> bool {
        let holds = |exec: u32| self.execs.get(&exec).is_some_and(|e| e.held_cpus > 0.0);
        let me = &self.router.identity.peer.0[..];
        self.running.iter().any(|r| !r.blocked && (r.cpus > 0.0 || holds(r.exec)))
            || self.execs.values().any(|e| e.held_cpus > 0.0 && e.state == ExecState::Starting)
            || self.actor_holds.iter().any(|(_, p, _, at)| p.as_slice() == me && at.elapsed() < ACTOR_HOLD_TTL)
    }

    /// A queued call starts; `lent`: let in over the cores (or quota) waiting
    /// calls' threads keep.
    pub(super) fn native_started(&mut self, tk: Tk, lent: bool) {
        self.letting.progress = Some(Instant::now());
        if lent {
            self.letting.lent = Some(Lent::new(To::Call(tk)));
        }
    }

    /// An actor of `cpus` is placed on `peer`: here, with fewer CPUs free than
    /// it holds, it was let in over the cores waiting calls' threads keep.
    pub(super) fn native_placed(&mut self, actor: Id, peer: &[u8], cpus: f64) {
        let me = &self.router.identity.peer.0[..];
        if peer != me {
            return;
        }
        let held: f64 = self.actor_holds.iter().filter(|(_, p, ..)| p.as_slice() == me).map(|(_, _, c, _)| c).sum();
        if self.capacity - self.cpu_used - held + 1e-9 < cpus {
            self.letting.lent = Some(Lent::new(To::Actor(actor)));
        }
        self.letting.progress = Some(Instant::now());
    }

    fn stand(&self, to: To) -> Stand {
        match to {
            To::Call(tk) => match self.running.iter().find(|r| r.items.iter().any(|w| w.tk == tk)) {
                Some(r) if !r.blocked => Stand::Running(r.exec, 1),
                Some(_) => Stand::Quiet,
                None if self.iso_waiting.values().any(|w| w.tk == tk) => Stand::Starting,
                None => Stand::Quiet,
            },
            To::Actor(a) => {
                let me = &self.router.identity.peer.0[..];
                let held = || {
                    self.actor_holds
                        .iter()
                        .any(|(x, p, _, at)| *x == a && p.as_slice() == me && at.elapsed() < ACTOR_HOLD_TTL)
                };
                let exec = self.actors.get(&a).and_then(|h| h.exec).and_then(|e| Some((e, self.execs.get(&e)?)));
                match exec {
                    Some((_, x)) if x.state == ExecState::Starting => Stand::Starting,
                    Some((e, x)) => {
                        let mut calls = self.running.iter().filter(|r| r.exec == e && !r.blocked).count();
                        // A concurrent actor does not say whether its direct calls wait.
                        if x.concurrency <= 1 && !self.actor_waits.keys().any(|(w, _)| *w == e) {
                            calls += self.direct.at(a);
                        }
                        if calls > 0 { Stand::Running(e, calls) } else { Stand::Quiet }
                    }
                    None if held() => Stand::Starting,
                    None => Stand::Quiet,
                }
            }
        }
    }

    /// Watch the process of what was let in: once no thread of it has been
    /// seen running or waiting for a CPU through a whole window of the same
    /// calls, it holds back no other.
    fn lent_sample(&mut self, now: Instant) {
        let Some(to) = self.letting.lent.as_ref().map(|l| l.to) else { return };
        let running = match self.stand(to) {
            Stand::Running(e, n) => self.execs.get(&e).map(|x| (e, n, runnable(x.pid) != Some(false))),
            _ => None,
        };
        let Some(l) = self.letting.lent.as_mut() else { return };
        match (running, l.mark) {
            (Some((e, n, busy)), Some((me, mn, at))) if (me, mn) == (e, n) => {
                l.busy |= busy;
                l.idle &= !busy;
                if now.duration_since(at) >= WINDOW {
                    (l.idle, l.busy, l.mark) = (!l.busy, false, Some((e, n, now)));
                }
            }
            (running, _) => {
                l.mark = running.map(|(e, n, _)| (e, n, now));
                l.busy = running.is_some_and(|(.., busy)| busy);
                l.idle = false;
            }
        }
    }

    /// For a wait reason: the tenant quota or share a queued call waits for.
    pub(super) fn quota_note(&self, w: &Work) -> Option<String> {
        let tenant = self.quotas.tenant_of(w.tk.run)?;
        let others = if self.quotas.has_share(tenant) {
            self.tenants_elsewhere().get(tenant).copied().unwrap_or_default()
        } else {
            (0.0, 0.0)
        };
        match self.quotas.admit(tenant, w.cpus, others, || self.native_free(w.ready_at.elapsed())) {
            crate::quotas::Admit::Wait(why) => Some(why),
            _ => None,
        }
    }

    /// For a wait reason: how much of the CPU in use waiting calls' threads hold.
    pub(super) fn native_note(&self) -> String {
        let taken = self.native_cores();
        if taken > 0.0 {
            format!(", {taken:.2} of it by threads still running in calls that wait on other calls")
        } else {
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    #[test]
    fn a_waiting_process_that_idles_lends_everything() {
        assert_eq!(reclaim(20_000_000, S, 0.0, 2.0), 0.0, "0.02 cores of wake-ups");
    }

    #[test]
    fn threads_running_through_a_wait_take_back_what_they_use() {
        assert!((reclaim(1_500_000_000, S, 0.0, 2.0) - 1.5).abs() < 1e-9);
        assert_eq!(reclaim(8_000_000_000, S, 0.0, 2.0), 2.0, "never more than it lent");
    }

    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn a_process_that_sleeps_has_no_thread_that_runs() {
        assert_eq!(runnable(std::process::id()), Some(true), "this test's own thread runs");
        let mut sleeper = std::process::Command::new("sleep").arg("30").spawn().expect("sleep");
        let deadline = Instant::now() + Duration::from_secs(10); // it may still be starting on a loaded host
        let mut got = runnable(sleeper.id());
        while got != Some(false) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
            got = runnable(sleeper.id());
        }
        let _ = sleeper.kill();
        let _ = sleeper.wait();
        assert_eq!(got, Some(false));
        assert_eq!(runnable(sleeper.id()), None, "gone");
    }

    #[test]
    fn a_guests_grant_is_its_own() {
        // A guest holding 1 CPU runs beside a host whose pool uses 1 more.
        assert!((reclaim(2_000_000_000, S, 1.0, 1.0) - 1.0).abs() < 1e-9);
        assert_eq!(reclaim(900_000_000, S, 1.0, 1.0), 0.0, "the guest alone");
    }
}
