//! The lost-call sweep. Under `CHIMAERA_RAFT_SYNC=durable` (or `none`), the
//! machines holding a majority of a group's voters losing power together take
//! the group's last commits with them: an attached run's per-call commands,
//! acknowledged from memory. A driver waiting on such a call would wait
//! forever, so this peer reads, now and then and through each group's leader,
//! the calls its sessions wait on:
//! - a call whose submission this peer acknowledged, with no record at all on
//!   two sweeps at least [`AGAIN`] apart, never reached a disk: its waiters
//!   get `DataLost` (`ch.DataLost`). A call watched without that
//!   acknowledgement may not be submitted yet, and is never declared lost;
//! - an attempt its record says runs, which its host (the same incarnation)
//!   no longer holds, queued, running or publishing, on two sweeps [`AGAIN`]
//!   apart, lost its result's publication: it is failed as an infrastructure
//!   failure, and the shard runs the call again. Only calls whose results may
//!   be published unsynced qualify (`sync_class::call`: functions and map
//!   items without external effects); any other's publication was synced.
//!
//! The same holds for the arguments of a watched call still waiting on them:
//! one whose result's publication was lost runs again, and a call whose
//! argument has no record on two sweeps [`AGAIN`] apart gets `DataLost`.
//!
//! Calls of a run that ended are no longer waited on: their waiters are
//! dropped, never told `DataLost`. A group that cannot answer (its quorum is
//! lost: its calls are refused as such) is read again later. A sweep follows
//! a change of a watched group's leader, and runs every [`EVERY`] regardless:
//! one read per group, not one per call. It runs only while some member
//! acknowledges commits from memory (`CHIMAERA_RAFT_SYNC=durable` or `none`).

use super::*;

/// How often every watched group is read, whatever happens.
const EVERY: Duration = Duration::from_secs(30);
/// After a watched group's leader changes, a sweep this soon.
const AFTER_LEADER: Duration = Duration::from_secs(5);
/// How far apart the two sightings that declare a call or an attempt lost are.
const AGAIN: Duration = Duration::from_secs(30);
/// Calls per read.
const CHUNK: usize = 1024;
/// Arguments of waiting calls read per sweep, at most.
const MAX_ARGS: usize = 65_536;

/// The sweep's state, kept by the core.
#[derive(Default)]
pub(super) struct Sweep {
    /// Each watched group's leader as last seen.
    leaders: HashMap<u32, Option<u64>>,
    /// When to read each group next.
    due: BTreeMap<u32, Instant>,
    last_all: Option<Instant>,
    busy: bool,
    /// Watched calls whose submission was acknowledged here.
    accepted: HashSet<Tk>,
    /// Calls first read without a record.
    absent: HashMap<Tk, Instant>,
    /// Attempts first found not held by their host.
    unheld: HashMap<(Tk, u32), Instant>,
    /// Arguments of watched calls waiting on them, each with such a call.
    args: HashMap<Tk, Tk>,
    /// Arguments first read without a record.
    missing: HashMap<Tk, Instant>,
    /// How many sweeps read each watched call, so a stuck one can be found.
    pub(super) checks: HashMap<Tk, u32>,
}

/// What one sweep read.
#[derive(Debug, Default)]
pub struct Found {
    /// Calls read with no record (a group that could not answer has none here).
    pub(super) absent: Vec<Tk>,
    pub(super) present: Vec<Tk>,
    /// Calls of runs that ended.
    pub(super) ended: Vec<Tk>,
    /// Attempts their host no longer holds: (call, its host, attempt).
    pub(super) unheld: Vec<(Tk, pb::PeerInc, u32)>,
    /// Arguments read, as (the watched call waiting on it, argument).
    pub(super) args: Vec<(Tk, Tk)>,
    /// Those with no record.
    pub(super) missing: Vec<(Tk, Tk)>,
    /// Groups that did not answer (no leader yet, say): read again soon.
    pub(super) unanswered: Vec<u32>,
}

/// What to do after a sweep.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Verdict {
    pub(super) lost: Vec<Tk>,
    /// Watched calls whose argument was lost: (call, argument).
    pub(super) lost_args: Vec<(Tk, Tk)>,
    pub(super) rerun: Vec<(Tk, pb::PeerInc, u32)>,
    pub(super) dropped: Vec<Tk>,
    /// Groups with something still under suspicion: read again past AGAIN.
    pub(super) again: Vec<u32>,
}

impl Sweep {
    /// A watched call whose submission this peer acknowledged.
    pub(super) fn accepted(&mut self, tk: Tk) {
        self.accepted.insert(tk);
    }

    /// Fold one sweep's reads, made at `now`, into what is known.
    pub(super) fn verdict(&mut self, now: Instant, f: Found, group_of: impl Fn(Tk) -> u32) -> Verdict {
        let mut v = Verdict::default();
        for tk in f.ended {
            self.accepted.remove(&tk);
            self.absent.remove(&tk);
            self.checks.remove(&tk);
            self.unheld.retain(|(t, _), _| *t != tk);
            v.dropped.push(tk);
        }
        // The calls read this time: an attempt of one not reported unheld again is held again, or done.
        let read: HashSet<Tk> = f.present.iter().chain(&f.absent).copied().collect();
        for tk in &read {
            *self.checks.entry(*tk).or_default() += 1;
        }
        for tk in &f.present {
            self.absent.remove(tk);
            self.missing.remove(tk);
        }
        self.args.extend(f.args.iter().map(|(call, arg)| (*arg, *call)));
        for (call, arg) in f.missing {
            self.args.insert(arg, call);
            match self.missing.get(&arg) {
                Some(&first) if now.duration_since(first) >= AGAIN => v.lost_args.push((call, arg)),
                Some(_) => v.again.push(group_of(call)),
                None => {
                    self.missing.insert(arg, now);
                    v.again.push(group_of(call));
                }
            }
        }
        for tk in f.absent {
            if !self.accepted.contains(&tk) {
                continue;
            }
            match self.absent.get(&tk) {
                Some(&first) if now.duration_since(first) >= AGAIN => {
                    self.absent.remove(&tk);
                    self.accepted.remove(&tk);
                    v.lost.push(tk);
                }
                Some(_) => v.again.push(group_of(tk)),
                None => {
                    self.absent.insert(tk, now);
                    v.again.push(group_of(tk));
                }
            }
        }
        let still: HashSet<(Tk, u32)> = f.unheld.iter().map(|(t, _, a)| (*t, *a)).collect();
        self.unheld.retain(|k, _| !read.contains(&k.0) || still.contains(k));
        for (tk, host, attempt) in f.unheld {
            // An argument is read again through the call waiting on it.
            let group = group_of(self.args.get(&tk).copied().unwrap_or(tk));
            match self.unheld.get(&(tk, attempt)) {
                Some(&first) if now.duration_since(first) >= AGAIN => {
                    self.unheld.remove(&(tk, attempt));
                    v.rerun.push((tk, host, attempt));
                }
                Some(_) => v.again.push(group),
                None => {
                    self.unheld.insert((tk, attempt), now);
                    v.again.push(group);
                }
            }
        }
        v.again.sort_unstable();
        v.again.dedup();
        v
    }

    /// Forget calls no longer waited on.
    fn keep(&mut self, waited: impl Fn(&Tk) -> bool) {
        self.accepted.retain(|t| waited(t));
        self.absent.retain(|t, _| waited(t));
        self.args.retain(|_, call| waited(call));
        let args = &self.args;
        self.missing.retain(|t, _| args.contains_key(t));
        self.unheld.retain(|(t, _), _| waited(t) || args.contains_key(t));
        self.checks.retain(|t, _| waited(t));
    }
}

/// Whether a result for attempt `attempt` of `tk` waits to be published here
/// (`pending`), or the attempt is held outside the queue (`away`: its result
/// being published, or the call run in another cell).
pub(super) fn publishing(
    pending: &BTreeMap<u16, Vec<pb::PublishEntry>>,
    away: &HashSet<(Tk, u32)>,
    tk: Tk,
    attempt: u32,
) -> bool {
    away.contains(&(tk, attempt))
        || pending.get(&tk.v()).is_some_and(|es| {
            es.iter().any(|e| e.attempt == attempt && e.task.as_ref().and_then(Tk::parse) == Some(tk))
        })
}

fn data_lost(tk: Tk, arg: Option<Tk>) -> pb::Outcome {
    let lost = arg.unwrap_or(tk);
    let mut message = format!(
        "call {} of run {} was lost: the machines holding a majority of its group's voters restarted together \
         before its record reached their disks (CHIMAERA_RAFT_SYNC=durable or none)",
        lost.task.hex(),
        lost.run.hex()
    );
    if arg.is_some() {
        message = format!("an argument of call {} is lost: {message}", tk.task.hex());
    }
    pb::Outcome {
        value: Some(pb::outcome::Value::Err(pb::ErrorRecord {
            kind: pb::error_record::Kind::DataLost as i32,
            message,
            ..Default::default()
        })),
        ..Default::default()
    }
}

impl Core {
    /// From housekeeping: start a sweep of the groups due, unless one runs.
    pub(super) fn sweep_tick(&mut self) {
        let Some(mesh) = self.router.mesh.clone() else { return };
        if self.watchers.is_empty() || !from_memory(&mesh) {
            if self.sweep.last_all.is_some() {
                self.sweep = Sweep::default();
            }
            return;
        }
        // Every tick looks at the groups' leaders (a few), not at the calls.
        let now = Instant::now();
        for g in self.router.data_groups() {
            let leader = self.router.leader_hint(g);
            if self.sweep.leaders.insert(g, leader).is_some_and(|was| was != leader) {
                let at = self.sweep.due.entry(g).or_insert(now + AFTER_LEADER);
                *at = (*at).min(now + AFTER_LEADER);
            }
        }
        if self.sweep.last_all.is_none_or(|t| now.duration_since(t) >= EVERY) {
            self.sweep.last_all = Some(now);
            for g in self.router.data_groups() {
                self.sweep.due.insert(g, now);
            }
        }
        if self.sweep.busy {
            return;
        }
        let due: HashSet<u32> = self.sweep.due.iter().filter(|(_, at)| **at <= now).map(|(g, _)| *g).collect();
        if due.is_empty() {
            return;
        }
        self.sweep.due.retain(|g, _| !due.contains(g));
        let mut calls: BTreeMap<u32, Vec<Tk>> = BTreeMap::new();
        for tk in self.watchers.keys() {
            let g = self.router.group_of(tk.v());
            if due.contains(&g) {
                calls.entry(g).or_default().push(*tk);
            }
        }
        if calls.is_empty() {
            return;
        }
        self.sweep.busy = true;
        let (router, tx) = (self.router.clone(), self.me_tx.clone());
        tokio::spawn(async move {
            // A sweep stuck on unanswering peers gives way to the next.
            let groups: Vec<u32> = calls.keys().copied().collect();
            let reads = read(&router, &mesh, &tx, calls.into_iter().collect());
            let found = tokio::time::timeout(EVERY, reads)
                .await
                .unwrap_or_else(|_| Found { unanswered: groups, ..Default::default() });
            let _ = tx.send(CoreMsg::Swept(Box::new(found))).await;
        });
    }

    /// A sweep's reads came back.
    pub(super) fn on_swept(&mut self, mut found: Found) {
        self.sweep.busy = false;
        tracing::debug!(
            present = found.present.len(),
            absent = found.absent.len(),
            ended = found.ended.len(),
            unheld = found.unheld.len(),
            missing = found.missing.len(),
            unanswered = ?found.unanswered,
            "lost-call sweep"
        );
        let soon = Instant::now() + AFTER_LEADER;
        for g in std::mem::take(&mut found.unanswered) {
            let at = self.sweep.due.entry(g).or_insert(soon);
            *at = (*at).min(soon);
        }
        let router = self.router.clone();
        let v = self.sweep.verdict(Instant::now(), found, |tk| router.group_of(tk.v()));
        for tk in v.dropped {
            self.watchers.remove(&tk);
            self.remote_watched.remove(&tk);
        }
        for tk in v.lost {
            tracing::warn!(task = %tk.task.hex(), "a call this peer's sessions wait on has no record: it was lost");
            self.notify_watchers(tk, &data_lost(tk, None));
            self.remote_watched.remove(&tk);
        }
        for (tk, arg) in v.lost_args {
            tracing::warn!(task = %tk.task.hex(), arg = %arg.task.hex(), "an argument of a call this peer's sessions wait on has no record: it was lost");
            self.notify_watchers(tk, &data_lost(tk, Some(arg)));
            self.remote_watched.remove(&tk);
        }
        for (tk, host, attempt) in v.rerun {
            tracing::warn!(
                task = %tk.task.hex(),
                attempt,
                "an attempt's host no longer holds it and its result was never recorded: running the call again"
            );
            let message = "its host finished it, but the result's publication was lost when the machines holding \
                           a majority of its group's voters restarted together"
                .to_string();
            self.propose(
                tk.v(),
                pb::shard_cmd::Body::RecordFailure(pb::RecordFailure {
                    task: Some(tk.pb()),
                    attempt,
                    peer: Some(host),
                    kind: pb::record_failure::Kind::Infra as i32,
                    error: Some(pb::ErrorRecord { message, attempt, ..Default::default() }),
                    ..Default::default()
                }),
            );
        }
        let at = Instant::now() + AGAIN;
        for g in v.again {
            self.sweep.due.entry(g).or_insert(at);
        }
        let watchers = &self.watchers;
        self.sweep.keep(|t| watchers.contains_key(t));
    }

    /// Whether this peer still holds attempt `attempt` of `tk`: queued,
    /// running (here, or in another cell), waiting for a lost input to be
    /// made again, or with its result waiting to be published or being published.
    pub(super) fn holds_attempt(&self, tk: Tk, attempt: u32) -> bool {
        self.runs_attempt(tk, attempt)
            || publishing(&self.pending_publish, &self.away, tk, attempt)
            || self.rebuilding.values().flatten().any(|w| w.tk == tk && w.attempt == attempt)
    }
}

/// Whether some member acknowledges commits from memory: then a correlated
/// restart can take committed calls with it.
fn from_memory(mesh: &MeshClient) -> bool {
    let unsynced =
        |m: &pb::Member| m.raft_sync_selective || !matches!(m.raft_sync(), pb::RaftSync::Fsync | pb::RaftSync::Full);
    unsynced(&mesh.me) || mesh.members().iter().any(|p| !p.dead && unsynced(&p.member))
}

/// One sweep's reads: each group's watched calls through its leader, the
/// runs they belong to, the arguments of those still waiting, and the hosts
/// of their running attempts.
async fn read(router: &Router, mesh: &MeshClient, tx: &mpsc::Sender<CoreMsg>, calls: Vec<(u32, Vec<Tk>)>) -> Found {
    let mut f = Found::default();
    let mut runs: HashMap<Id, Option<bool>> = HashMap::new();
    let mut ask = Ask::default();
    let watched: HashSet<Tk> = calls.iter().flat_map(|(_, tks)| tks.iter().copied()).collect();
    let mut waiting: BTreeMap<u32, Vec<(Tk, Tk)>> = BTreeMap::new();
    let mut args = 0;
    for (group, tks) in calls {
        let Some(records) = records(router, mesh, group, &tks).await else {
            f.unanswered.push(group);
            continue;
        };
        for (tk, rec) in tks.into_iter().zip(records) {
            let active = match runs.get(&tk.run) {
                Some(a) => *a,
                None => {
                    let a = run_active(router, mesh, tk.run).await;
                    runs.insert(tk.run, a);
                    a
                }
            };
            match active {
                None => continue, // its run's record could not be read: next time
                Some(false) => f.ended.push(tk),
                Some(true) if absent(&rec) => f.absent.push(tk),
                Some(true) => match rec.spec.as_ref() {
                    None => f.present.push(tk), // a released call kept small
                    Some(spec) => {
                        f.present.push(tk);
                        if rec.state() == pb::task_record::State::Pending {
                            for arg in spec.deps.iter().filter_map(Tk::parse).filter(|a| !watched.contains(a)) {
                                if args < MAX_ARGS {
                                    args += 1;
                                    waiting.entry(router.group_of(arg.v())).or_default().push((tk, arg));
                                }
                            }
                        }
                        ask.hosts(router, mesh, tk, &rec);
                    }
                },
            }
        }
    }
    for (group, pairs) in waiting {
        let keys: Vec<Tk> = pairs.iter().map(|(_, arg)| *arg).collect();
        let Some(records) = records(router, mesh, group, &keys).await else {
            // Read again through the calls waiting on them.
            f.unanswered.extend(pairs.iter().map(|(call, _)| router.group_of(call.v())));
            continue;
        };
        for ((call, arg), rec) in pairs.into_iter().zip(records) {
            if absent(&rec) {
                f.missing.push((call, arg));
                continue;
            }
            f.present.push(arg);
            f.args.push((call, arg));
            ask.hosts(router, mesh, arg, &rec);
        }
    }
    f.unheld = ask.held(mesh, tx).await;
    f
}

/// Whether a record read is the empty one a read gives for a call with no
/// record (a released call that succeeded keeps a small one, with no spec).
fn absent(rec: &pb::TaskRecord) -> bool {
    rec.spec.is_none()
        && rec.outcome.is_none()
        && rec.attempts.is_empty()
        && rec.state() != pb::task_record::State::Done
}

/// `tks`' records in `group` through its leader, in order (a call with none
/// gets an empty one); None if the group did not answer.
async fn records(router: &Router, mesh: &MeshClient, group: u32, tks: &[Tk]) -> Option<Vec<pb::TaskRecord>> {
    let mut out = Vec::with_capacity(tks.len());
    for chunk in tks.chunks(CHUNK) {
        let req =
            pb::ReadRequest { tasks: chunk.iter().map(|t| t.pb()).collect(), linearizable: true, ..Default::default() };
        let reply = leader_read(router, mesh, group, req).await.ok()?;
        if reply.tasks.len() != chunk.len() {
            return None;
        }
        out.extend(reply.tasks);
    }
    Some(out)
}

/// The running attempts to ask about, by their host's node.
#[derive(Default)]
struct Ask {
    by_node: HashMap<u64, Vec<(Tk, pb::PeerInc, u32)>>,
    asked: HashSet<(Tk, u32)>,
}

impl Ask {
    /// The live attempts of `rec`, when its result may have been published
    /// unsynced, on hosts still in the same incarnation.
    fn hosts(&mut self, router: &Router, mesh: &MeshClient, tk: Tk, rec: &pb::TaskRecord) {
        let Some(spec) = rec.spec.as_ref() else { return };
        if rec.state() != pb::task_record::State::Running || !crate::sync_class::call(spec, true) {
            return;
        }
        for a in rec.attempts.iter().filter(|a| !a.revoked) {
            let Some(peer) = a.peer.clone() else { continue };
            let host = if peer.peer == router.identity.peer.to_vec() {
                Some(mesh.me.node_id).filter(|_| peer.inc == router.identity.incarnation)
            } else {
                // A dead host's attempts are failed when it is retired.
                mesh.member_by_peer(&peer.peer)
                    .filter(|p| !p.dead && p.member.incarnation == peer.inc)
                    .map(|p| p.member.node_id)
            };
            if let Some(node) = host
                && self.asked.insert((tk, a.attempt))
            {
                self.by_node.entry(node).or_default().push((tk, peer, a.attempt));
            }
        }
    }

    /// The attempts their hosts say they no longer hold.
    async fn held(self, mesh: &MeshClient, tx: &mpsc::Sender<CoreMsg>) -> Vec<(Tk, pb::PeerInc, u32)> {
        let mut unheld = Vec::new();
        for (node, attempts) in self.by_node {
            let held: Option<Vec<bool>> = if node == mesh.me.node_id {
                let keys = attempts.iter().map(|(t, _, a)| (Some(*t), *a)).collect();
                let (reply, rx) = oneshot::channel();
                match tx.send(CoreMsg::AttemptsHeld { keys, reply }).await {
                    Ok(()) => rx.await.ok(),
                    Err(_) => None,
                }
            } else {
                let tasks = attempts.iter().map(|(t, _, _)| t.pb()).collect();
                let nums = attempts.iter().map(|(_, _, a)| *a).collect();
                match mesh.attempts_held(node, tasks, nums).await {
                    // An answer from another incarnation says nothing of these attempts.
                    Ok(r) if r.peer.as_ref().is_some_and(|p| attempts.iter().all(|(_, h, _)| h.inc == p.inc)) => {
                        Some(r.held)
                    }
                    _ => None,
                }
            };
            let Some(held) = held.filter(|h| h.len() == attempts.len()) else { continue };
            unheld.extend(attempts.into_iter().zip(held).filter(|(_, h)| !h).map(|(a, _)| a));
        }
        unheld
    }
}

/// A read of `group`'s state reflecting everything committed before it: on
/// this peer when it leads the group, else through a replica that does.
async fn leader_read(
    router: &Router,
    mesh: &MeshClient,
    group: u32,
    req: pb::ReadRequest,
) -> std::io::Result<pb::ReadReply> {
    if router.leads(group)
        && let Some(g) = router.hosted(group)
    {
        g.linearizable().await.map_err(|e| std::io::Error::other(e.to_string()))?;
        let keys: Vec<Option<Tk>> = req.tasks.iter().map(Tk::parse).collect();
        let run = Id::from_slice(&req.run).ok();
        let (tasks, run) = g.read(|kv| {
            let tasks = keys.iter().map(|k| k.and_then(|t| read::task(kv, t)).unwrap_or_default()).collect();
            (tasks, run.and_then(|r| read::run(kv, r)))
        })?;
        return Ok(pb::ReadReply { tasks, run, ..Default::default() });
    }
    mesh.read_group(group, req).await
}

/// Whether `run` may still be waited on (None: its record could not be read
/// now). A run with no record counts: under `none` its creation can be lost too.
async fn run_active(router: &Router, mesh: &MeshClient, run: Id) -> Option<bool> {
    let req = pb::ReadRequest { run: run.to_vec(), linearizable: true, ..Default::default() };
    let reply = leader_read(router, mesh, router.group_of(run.vshard()), req).await.ok()?;
    Some(reply.run.is_none_or(|r| r.state() == pb::run_record::State::Active))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tk(n: u8) -> Tk {
        Tk { run: Id::derive(0, 1, &[b"run".as_slice()]), task: Id::derive(0, 1, &[[n].as_slice()]) }
    }

    /// A sweep that knows `tks` were acknowledged.
    fn sweep(tks: &[Tk]) -> Sweep {
        let mut s = Sweep::default();
        tks.iter().for_each(|t| s.accepted(*t));
        s
    }

    fn host() -> pb::PeerInc {
        pb::PeerInc { peer: vec![7; 16], inc: 1 }
    }

    const G: u32 = 4;

    #[test]
    fn a_call_is_lost_only_when_it_has_no_record_on_two_sweeps_far_enough_apart() {
        let mut s = sweep(&[tk(1), tk(2), tk(3)]);
        let t0 = Instant::now();
        let read = |absent: Vec<Tk>, present: Vec<Tk>| Found { absent, present, ..Default::default() };
        let v = s.verdict(t0, read(vec![tk(1)], vec![tk(2)]), |_| G);
        assert!(v.lost.is_empty() && v.again == vec![G], "one sighting is a suspicion: {v:?}");
        let v = s.verdict(t0 + AGAIN / 2, read(vec![tk(1)], vec![]), |_| G);
        assert!(v.lost.is_empty(), "two sightings too close together: {v:?}");
        let v = s.verdict(t0 + AGAIN, read(vec![tk(1)], vec![]), |_| G);
        assert_eq!(v.lost, vec![tk(1)]);
        // A record seen in between clears the suspicion.
        let mut s = sweep(&[tk(3)]);
        s.verdict(t0, read(vec![tk(3)], vec![]), |_| G);
        s.verdict(t0 + AGAIN / 2, read(vec![], vec![tk(3)]), |_| G);
        assert!(s.verdict(t0 + AGAIN * 2, read(vec![tk(3)], vec![]), |_| G).lost.is_empty());
        assert_eq!(s.checks[&tk(3)], 3, "each sweep that read it counts");
        // One watched without an acknowledged submission may not be submitted yet.
        let mut s = Sweep::default();
        s.verdict(t0, read(vec![tk(4)], vec![]), |_| G);
        assert!(s.verdict(t0 + AGAIN * 2, read(vec![tk(4)], vec![]), |_| G).lost.is_empty());
    }

    #[test]
    fn a_call_of_an_ended_run_is_dropped_never_reported_lost() {
        let mut s = sweep(&[tk(1)]);
        let t0 = Instant::now();
        s.verdict(t0, Found { absent: vec![tk(1)], ..Default::default() }, |_| G);
        let v = s.verdict(t0 + AGAIN, Found { ended: vec![tk(1)], ..Default::default() }, |_| G);
        assert_eq!((v.lost, v.dropped), (vec![], vec![tk(1)]));
        assert!(!s.absent.contains_key(&tk(1)) && !s.checks.contains_key(&tk(1)));
    }

    #[test]
    fn an_attempt_its_host_no_longer_holds_is_run_again_only_after_two_sightings() {
        let mut s = Sweep::default();
        let t0 = Instant::now();
        let unheld = |a| Found { present: vec![tk(1)], unheld: vec![(tk(1), host(), a)], ..Default::default() };
        assert!(s.verdict(t0, unheld(1), |_| G).rerun.is_empty());
        assert_eq!(s.verdict(t0 + AGAIN, unheld(1), |_| G).rerun, vec![(tk(1), host(), 1)]);
        // Held again in between (a publish that was queued then applied): never run again.
        let mut s = Sweep::default();
        s.verdict(t0, unheld(2), |_| G);
        s.verdict(t0 + AGAIN / 2, Found { present: vec![tk(1)], ..Default::default() }, |_| G);
        assert!(s.verdict(t0 + AGAIN, unheld(2), |_| G).rerun.is_empty(), "the clock starts over");
        // A group that could not be read keeps its suspicions.
        let mut s = Sweep::default();
        s.verdict(t0, unheld(3), |_| G);
        s.verdict(t0 + AGAIN / 2, Found::default(), |_| G);
        assert_eq!(s.verdict(t0 + AGAIN, unheld(3), |_| G).rerun, vec![(tk(1), host(), 3)]);
    }

    #[test]
    fn a_waiting_calls_argument_is_followed_while_the_call_is_watched() {
        let (call, arg) = (tk(1), tk(2));
        let t0 = Instant::now();
        // No record of the argument, twice and far enough apart: the call is lost.
        let mut s = sweep(&[call]);
        let missing = || Found { present: vec![call], missing: vec![(call, arg)], ..Default::default() };
        assert!(s.verdict(t0, missing(), |_| G).lost_args.is_empty());
        assert_eq!(s.verdict(t0 + AGAIN, missing(), |_| G).lost_args, vec![(call, arg)]);
        // Seen in between, it was not lost.
        let mut s = sweep(&[call]);
        s.verdict(t0, missing(), |_| G);
        s.verdict(
            t0 + AGAIN / 2,
            Found { present: vec![call, arg], args: vec![(call, arg)], ..Default::default() },
            |_| G,
        );
        assert!(s.verdict(t0 + AGAIN, missing(), |_| G).lost_args.is_empty());
        // An argument's attempt its host no longer holds survives the pruning
        // of what is not watched, and runs again.
        let mut s = sweep(&[call]);
        let unheld = || Found {
            present: vec![call, arg],
            args: vec![(call, arg)],
            unheld: vec![(arg, host(), 1)],
            ..Default::default()
        };
        s.verdict(t0, unheld(), |_| G);
        s.keep(|t| *t == call);
        assert_eq!(s.verdict(t0 + AGAIN, unheld(), |_| G).rerun, vec![(arg, host(), 1)]);
        // Once the call is no longer watched, nothing of its arguments is kept.
        s.keep(|_| false);
        assert!(s.args.is_empty() && s.missing.is_empty() && s.unheld.is_empty());
    }

    #[test]
    fn a_result_waiting_or_being_published_keeps_its_attempt_held() {
        let entry = pb::PublishEntry { task: Some(tk(1).pb()), attempt: 2, ..Default::default() };
        let mut pending: BTreeMap<u16, Vec<pb::PublishEntry>> = BTreeMap::new();
        let mut away: HashSet<(Tk, u32)> = HashSet::new();
        assert!(!publishing(&pending, &away, tk(1), 2));
        pending.entry(tk(1).v()).or_default().push(entry);
        assert!(publishing(&pending, &away, tk(1), 2), "queued to publish");
        assert!(!publishing(&pending, &away, tk(1), 3), "another attempt");
        // The sweep asks while the batch is proposed: still held until its answer.
        pending.clear();
        away.insert((tk(1), 2));
        assert!(publishing(&pending, &away, tk(1), 2), "being published");
        away.remove(&(tk(1), 2));
        assert!(!publishing(&pending, &away, tk(1), 2), "published: the record says so now");
    }
}
