//! The replicated shard state machine.
//!
//! [`ShardState::apply`] is the only code that mutates replicated state. It is
//! deterministic: it reads no clock (the leader stamps an HLC into each
//! command), uses no randomness, and iterates keys in order. Two replicas fed
//! the same log hold byte-identical state, which the tests check.
//!
//! Invariants this module maintains (and `tests.rs` checks under random
//! command sequences):
//! - a task's outcome is written once and never changes;
//! - a retired peer incarnation or a revoked attempt never publishes;
//! - cancel and publish are mutually exclusive (the first to commit wins);
//! - an external-effect task consumes its start at most once;
//! - a task's dependencies become satisfied at most once;
//! - a durable actor command id maps to one reply forever (within retention).

mod actors;
pub mod keys;
pub mod store;
#[cfg(test)]
mod tests;

use crate::hlc;
use crate::ids::{Id, VSHARDS};
use crate::kv::{Kv, KvRead};
use chimaera_proto::v1 as pb;
use pb::shard_cmd::Body;
use prost::Message;

pub const MAX_FAILURE_HISTORY: usize = 8;
pub const MAX_INLINE_STATE: usize = 1 << 20;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Pre-start losses of an external-effect task are safe to retry this many times.
pub const EXTERNAL_PRESTART_RETRIES: u32 = 3;
pub const ACTOR_CMD_RETENTION_MS: u64 = 7 * 24 * 3600 * 1000;
pub const DEFAULT_RUN_RETENTION_MS: u64 = 24 * 3600 * 1000;
pub const CANCEL_BATCH: usize = 4096;

/// A task's identity: (run, task). Its vshard is the task id's vshard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tk {
    pub run: Id,
    pub task: Id,
}

impl Tk {
    pub fn v(&self) -> u16 {
        self.task.vshard()
    }
    pub fn pb(&self) -> pb::TaskKey {
        pb::TaskKey { run: self.run.to_vec(), task: self.task.to_vec() }
    }
    pub fn parse(k: &pb::TaskKey) -> Option<Tk> {
        Some(Tk { run: Id::from_slice(&k.run).ok()?, task: Id::from_slice(&k.task).ok()? })
    }
    fn key(&self) -> Vec<u8> {
        keys::task(self.v(), &self.run, &self.task)
    }
    fn pair(&self) -> (&Id, &Id) {
        (&self.run, &self.task)
    }
}

/// A peer incarnation.
pub type PeerInc = (Id, u64);

pub fn peer_pb(p: PeerInc) -> pb::PeerInc {
    pb::PeerInc { peer: p.0.to_vec(), inc: p.1 }
}

pub(super) fn peer_of(p: &Option<pb::PeerInc>) -> Option<PeerInc> {
    let p = p.as_ref()?;
    Some((Id::from_slice(&p.peer).ok()?, p.inc))
}

/// Derived notifications for the local leader. Not stored; followers ignore them.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// Runnable and not granted (deps satisfied, or requeued after a failure).
    Ready(Tk),
    /// All dependencies were satisfied. Emitted at most once per task.
    DepsSatisfied(Tk),
    Granted {
        task: Tk,
        attempt: u32,
        peer: PeerInc,
        speculative: bool,
    },
    Done {
        task: Tk,
        outcome: pb::Outcome,
    },
    /// A dependent here waits on a producer in another vshard.
    Subscribe {
        producer: Tk,
        subscriber_vshard: u16,
    },
    Outbox {
        seq: u64,
        target_vshard: u16,
    },
    /// Outcome committed and no client pin remains; payload bytes can be freed.
    Releasable(Tk),
    ActorChanged(Id),
    /// A bounded CancelRun stopped early; propose it again.
    CancelRunContinues(Id),
    /// A run and its tasks were deleted after retention; its objects can go.
    RunExpired(Id),
    /// A run stopped being active (finished or cancelled): its non-detached
    /// actors end with it.
    RunEnded(Id),
}

pub struct Applied {
    pub reply: pb::ShardReply,
    pub events: Vec<Event>,
}

/// Deliberately broken variants, used only to prove the tests can fail.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Mutants {
    pub skip_fencing: bool,
    pub overwrite_outcome: bool,
    pub skip_command_dedup: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ShardState {
    #[cfg(test)]
    pub(crate) mutants: Mutants,
}

pub(super) struct Cx<'a> {
    kv: &'a mut dyn Kv,
    now: u64,
    events: Vec<Event>,
}

pub(super) type Status = pb::task_ack::Status;
pub(super) type TState = pb::task_record::State;
pub(super) type Kind = pb::error_record::Kind;
pub(super) type AStatus = pb::actor_ack::Status;
pub(super) type AState = pb::actor_record::State;

pub(super) fn load<M: Message + Default>(kv: &dyn KvRead, key: &[u8]) -> Option<M> {
    kv.get(key).map(|b| M::decode(b.as_slice()).expect("shard state is corrupt: undecodable record"))
}

pub(super) fn store<M: Message>(kv: &mut dyn Kv, key: Vec<u8>, m: &M) {
    kv.put(key, m.encode_to_vec());
}

pub(super) fn reply(body: pb::shard_reply::Body) -> pb::ShardReply {
    pb::ShardReply { body: Some(body) }
}

pub(super) fn rejected(why: &str) -> pb::ShardReply {
    reply(pb::shard_reply::Body::Rejected(why.to_string()))
}

fn ack(task: &pb::TaskKey, status: Status) -> pb::TaskAck {
    pb::TaskAck { task: Some(task.clone()), status: status as i32, ..Default::default() }
}

fn ack_rec(tk: Tk, status: Status, rec: &pb::TaskRecord) -> pb::TaskAck {
    pb::TaskAck {
        task: Some(tk.pb()),
        status: status as i32,
        state: rec.state,
        outcome: rec.outcome.clone(),
        ..Default::default()
    }
}

fn err_outcome(kind: Kind, msg: impl Into<String>) -> pb::Outcome {
    pb::Outcome {
        value: Some(pb::outcome::Value::Err(pb::ErrorRecord {
            kind: kind as i32,
            message: msg.into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn is_ok(o: &pb::Outcome) -> bool {
    matches!(o.value, Some(pb::outcome::Value::Ok(_)))
}

fn live_attempts(rec: &pb::TaskRecord) -> usize {
    rec.attempts.iter().filter(|a| !a.revoked).count()
}

fn max_attempts(rec: &pb::TaskRecord) -> u32 {
    match rec.spec.as_ref().map_or(0, |s| s.max_attempts) {
        0 => DEFAULT_MAX_ATTEMPTS,
        n => n,
    }
}

fn effects(rec: &pb::TaskRecord) -> pb::Effects {
    rec.spec.as_ref().map_or(pb::Effects::None, |s| s.effects())
}

/// The error a dependent records when its prerequisite failed: the root
/// cause's type, message and pickled exception, pointing at the producer.
/// The cancellation kind of a cancelled or expired outcome.
fn cancelled_kind(o: &pb::Outcome) -> Option<Kind> {
    match &o.value {
        Some(pb::outcome::Value::Err(e)) if matches!(e.kind(), Kind::Cancelled | Kind::Deadline) => Some(e.kind()),
        _ => None,
    }
}

fn causal(producer: Tk, root: &pb::Outcome) -> pb::Outcome {
    let mut e = match &root.value {
        Some(pb::outcome::Value::Err(e)) => e.clone(),
        _ => pb::ErrorRecord::default(),
    };
    if e.kind != Kind::Causal as i32 {
        e.message = format!("{} (in prerequisite {})", e.message, producer.task.hex());
    }
    e.kind = Kind::Causal as i32;
    e.upstream = Some(producer.pb());
    pb::Outcome { value: Some(pb::outcome::Value::Err(e)), ..Default::default() }
}

impl ShardState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one committed command.
    pub fn apply(&self, kv: &mut dyn Kv, cmd: &pb::ShardCmd) -> Applied {
        let last = load_u64(kv, &keys::meta(keys::META_LAST_HLC));
        let now = cmd.hlc.max(last);
        kv.put(keys::meta(keys::META_LAST_HLC), now.to_be_bytes().to_vec());
        let mut cx = Cx { kv, now, events: Vec::new() };
        let reply = self.dispatch(&mut cx, cmd);
        Applied { reply, events: cx.events }
    }

    fn dispatch(&self, cx: &mut Cx, cmd: &pb::ShardCmd) -> pb::ShardReply {
        let Some(body) = &cmd.body else {
            return rejected("empty command");
        };
        // Group-wide commands are not scoped to one vshard.
        match body {
            Body::AdoptVshard(a) => return self.adopt(cx, a),
            Body::Retire(r) => return self.retire(cx, r),
            Body::OutboxAck(a) => return self.outbox_ack(cx, a),
            Body::Gc(g) => return self.gc(cx, g),
            _ => {}
        }
        let v = cmd.vshard as u16;
        if cmd.vshard >= VSHARDS as u32 {
            return rejected("vshard out of range");
        }
        match load_u64_opt(cx.kv, &keys::owned(v)) {
            None => return rejected("vshard not owned by this group"),
            Some(e) if e != cmd.vshard_epoch => return rejected("stale vshard epoch"),
            Some(_) => {}
        }
        if let Some(issuer) = peer_of(&cmd.issuer)
            && self.is_retired(cx.kv, issuer)
        {
            return rejected("issuer incarnation is retired");
        }
        match body {
            Body::RunCreate(c) => self.run_create(cx, v, c),
            Body::SubmitTasks(s) => self.submit(cx, v, s),
            Body::Grant(g) => self.grant_all(cx, v, g),
            Body::StartExternal(s) => self.start_external(cx, v, s),
            Body::Publish(p) => self.publish(cx, v, p),
            Body::RecordFailure(f) => self.record_failure(cx, v, f),
            Body::Cancel(c) => self.cancel(cx, v, c),
            Body::CancelRun(c) => self.cancel_run(cx, v, c),
            Body::Subscribe(s) => self.subscribe(cx, v, s),
            Body::DepNotify(n) => self.dep_notify(cx, v, n),
            Body::Release(r) => self.release(cx, v, r),
            Body::ActorCreate(a) => self.actor_create(cx, v, a),
            Body::ActorPlace(a) => self.actor_place(cx, v, a),
            Body::ActorDied(a) => self.actor_died(cx, v, a),
            Body::ActorCommit(a) => self.actor_commit(cx, v, a),
            Body::ActorCheckpoint(a) => self.actor_checkpoint(cx, v, a),
            Body::RunFinish(r) => self.run_finish(cx, v, r),
            Body::StoreWrite(w) => reply(pb::shard_reply::Body::Store(store::write(cx, v, w))),
            Body::StoreRead(r) => reply(pb::shard_reply::Body::Store(store::read(cx.kv, v, r, cx.now))),
            Body::AdoptVshard(_) | Body::Retire(_) | Body::OutboxAck(_) | Body::Gc(_) => unreachable!(),
        }
    }

    pub fn is_retired(&self, kv: &dyn KvRead, p: PeerInc) -> bool {
        #[cfg(test)]
        if self.mutants.skip_fencing {
            return false;
        }
        kv.get(&keys::retired(&p.0, p.1)).is_some()
    }

    // ---- group-wide -------------------------------------------------------

    fn adopt(&self, cx: &mut Cx, a: &pb::AdoptVShard) -> pb::ShardReply {
        if a.vshards.iter().any(|v| *v >= VSHARDS as u32) {
            return rejected("vshard out of range");
        }
        let mut adopted = 0u64;
        for v in &a.vshards {
            let key = keys::owned(*v as u16);
            if load_u64_opt(cx.kv, &key).is_none_or(|c| a.epoch > c) {
                cx.kv.put(key, a.epoch.to_be_bytes().to_vec());
                adopted += 1;
            }
        }
        reply(pb::shard_reply::Body::Count(adopted))
    }

    fn retire(&self, cx: &mut Cx, r: &pb::RetireIncarnation) -> pb::ShardReply {
        let Some(peer) = peer_of(&r.peer) else { return rejected("missing peer") };
        cx.kv.put(keys::retired(&peer.0, peer.1), Vec::new());
        let mut affected = 0u64;
        for (okey, _) in cx.kv.scan_prefix(&keys::owned_prefix(), usize::MAX) {
            let v = u16::from_be_bytes([okey[okey.len() - 2], okey[okey.len() - 1]]);
            for (gkey, _) in cx.kv.scan_prefix(&keys::grants_of(v, &peer.0, peer.1), usize::MAX) {
                cx.kv.delete(&gkey);
                let n = gkey.len();
                let (run, task) = keys::trailing_pair(&gkey[..n - 4]);
                let attempt = u32::from_be_bytes(gkey[n - 4..].try_into().expect("4-byte attempt"));
                let tk = Tk { run, task };
                let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else { continue };
                if rec.state() == TState::Done {
                    continue;
                }
                if let Some(a) = rec.attempts.iter_mut().find(|a| a.attempt == attempt && !a.revoked) {
                    a.revoked = true;
                }
                let err = pb::ErrorRecord {
                    kind: Kind::Infra as i32,
                    message: format!("peer {} incarnation {} was retired", peer.0.hex(), peer.1),
                    attempt,
                    ..Default::default()
                };
                self.after_attempt_lost(cx, tk, rec, attempt, pb::record_failure::Kind::Infra, err, &[], 0);
                affected += 1;
            }
            for (hkey, _) in cx.kv.scan_prefix(&keys::actors_hosted_by(v, &peer.0, peer.1), usize::MAX) {
                cx.kv.delete(&hkey);
                let Ok(actor) = Id::from_slice(&hkey[hkey.len() - 16..]) else { continue };
                let Some(rec) = load::<pb::ActorRecord>(cx.kv, &keys::actor(v, &actor)) else { continue };
                let reason = pb::ErrorRecord {
                    kind: Kind::ActorDied as i32,
                    message: format!("host peer {} incarnation {} was retired", peer.0.hex(), peer.1),
                    ..Default::default()
                };
                self.actor_lost(cx, v, actor, rec, reason);
                affected += 1;
            }
        }
        reply(pb::shard_reply::Body::Count(affected))
    }

    fn outbox_ack(&self, cx: &mut Cx, a: &pb::OutboxAck) -> pb::ShardReply {
        for seq in &a.seqs {
            cx.kv.delete(&keys::outbox(*seq));
        }
        reply(pb::shard_reply::Body::Count(a.seqs.len() as u64))
    }

    fn gc(&self, cx: &mut Cx, g: &pb::Gc) -> pb::ShardReply {
        let budget = g.max_keys.max(1) as usize;
        let mut deleted = 0usize;
        let prefix = keys::expiry_prefix();
        for (ekey, _) in cx.kv.scan_prefix(&prefix, budget) {
            let at = u64::from_be_bytes(ekey[prefix.len()..prefix.len() + 8].try_into().expect("8-byte hlc"));
            if at > cx.now || deleted >= budget {
                break;
            }
            let target = ekey[prefix.len() + 8..].to_vec();
            if target.len() == 3 + 16 && target[2] == keys::RUN {
                // A run expires together with all of its tasks, in bounded steps.
                let v = u16::from_be_bytes([target[0], target[1]]);
                let run = Id::from_slice(&target[3..]).expect("16-byte run id");
                for prefix in [
                    keys::tasks_of_run(v, &run),
                    keys::pairs_of_run(v, keys::CHILD, &run),
                    keys::pairs_of_run(v, keys::FORWARD, &run),
                ] {
                    for (k, _) in cx.kv.scan_prefix(&prefix, budget - deleted) {
                        cx.kv.delete(&k);
                        deleted += 1;
                    }
                }
                if deleted < budget {
                    if let Some(r) = load::<pb::RunRecord>(cx.kv, &target) {
                        cx.kv.delete(&keys::submission_key(v, &r.tenant, &r.submission_key));
                    }
                    cx.kv.delete(&target);
                    cx.kv.delete(&ekey);
                    deleted += 1;
                    cx.events.push(Event::RunExpired(run));
                }
            } else {
                cx.kv.delete(&target);
                cx.kv.delete(&ekey);
                deleted += 1;
            }
        }
        reply(pb::shard_reply::Body::Count(deleted as u64))
    }

    // ---- runs -------------------------------------------------------------

    fn run_create(&self, cx: &mut Cx, v: u16, c: &pb::RunCreate) -> pb::ShardReply {
        let Some(mut run) = c.run.clone() else { return rejected("missing run") };
        let Ok(id) = Id::from_slice(&run.run) else { return rejected("bad run id") };
        if id.vshard() != v {
            return rejected("run id routes to another vshard");
        }
        if !run.submission_key.is_empty()
            && let Some(existing) = cx.kv.get(&keys::submission_key(v, &run.tenant, &run.submission_key))
            && let Some(r) =
                Id::from_slice(&existing).ok().and_then(|e| load::<pb::RunRecord>(cx.kv, &keys::run(v, &e)))
        {
            return reply(pb::shard_reply::Body::Run(r));
        }
        if let Some(existing) = load::<pb::RunRecord>(cx.kv, &keys::run(v, &id)) {
            return reply(pb::shard_reply::Body::Run(existing));
        }
        run.set_state(pb::run_record::State::Active);
        run.created_hlc = cx.now;
        if run.retention_ms == 0 {
            run.retention_ms = DEFAULT_RUN_RETENTION_MS;
        }
        if !run.submission_key.is_empty() {
            cx.kv.put(keys::submission_key(v, &run.tenant, &run.submission_key), id.to_vec());
        }
        store(cx.kv, keys::run(v, &id), &run);
        reply(pb::shard_reply::Body::Run(run))
    }

    fn run_finish(&self, cx: &mut Cx, v: u16, r: &pb::RunFinish) -> pb::ShardReply {
        let state = match r.state() {
            pb::run_record::State::Cancelled => pb::run_record::State::Cancelled,
            _ => pb::run_record::State::Finished,
        };
        self.end_run(cx, v, &r.run, state, "run finished with this task outstanding")
    }

    fn cancel_run(&self, cx: &mut Cx, v: u16, c: &pb::CancelRun) -> pb::ShardReply {
        let msg = if c.message.is_empty() { "run cancelled" } else { c.message.as_str() };
        self.end_run(cx, v, &c.run, pb::run_record::State::Cancelled, msg)
    }

    /// Mark a run ended and cancel its outstanding tasks, one bounded page per
    /// command (`Event::CancelRunContinues` asks the leader to propose again).
    /// The run and its tasks expire `retention_ms` after it ends.
    fn end_run(&self, cx: &mut Cx, v: u16, run: &[u8], state: pb::run_record::State, msg: &str) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(run) else { return rejected("bad run id") };
        let key = keys::run(v, &id);
        let Some(mut rec) = load::<pb::RunRecord>(cx.kv, &key) else { return rejected("unknown run") };
        if rec.state() == pb::run_record::State::Active {
            rec.set_state(state);
            rec.finished_hlc = cx.now;
            store(cx.kv, key.clone(), &rec);
            cx.kv.put(keys::expiry(cx.now + hlc::from_ms(rec.retention_ms), &key), Vec::new());
            cx.events.push(Event::RunEnded(id));
        }
        let page = cx.kv.scan_prefix(&keys::tasks_of_run(v, &id), CANCEL_BATCH);
        let mut cancelled = 0u64;
        for (tkey, bytes) in &page {
            let task = pb::TaskRecord::decode(bytes.as_slice()).expect("shard state is corrupt");
            if task.state() == TState::Done {
                continue;
            }
            let (run, t) = keys::trailing_pair(tkey);
            self.finish(cx, Tk { run, task: t }, task, err_outcome(Kind::Cancelled, msg));
            cancelled += 1;
        }
        if page.len() == CANCEL_BATCH && cancelled > 0 {
            cx.events.push(Event::CancelRunContinues(id));
        }
        reply(pb::shard_reply::Body::Run(rec))
    }

    // ---- tasks ------------------------------------------------------------

    fn submit(&self, cx: &mut Cx, v: u16, s: &pb::SubmitTasks) -> pb::ShardReply {
        let grant_to = peer_of(&s.grant_to);
        let mut acks = Vec::with_capacity(s.tasks.len());
        for spec in &s.tasks {
            let Some(tk) = spec.key.as_ref().and_then(Tk::parse) else {
                acks.push(ack(&spec.key.clone().unwrap_or_default(), Status::Rejected));
                continue;
            };
            if tk.v() != v {
                let mut a = ack(&tk.pb(), Status::Rejected);
                a.reason = "task id routes to another vshard".into();
                acks.push(a);
                continue;
            }
            if let Some(rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) {
                // A replayed workflow asks for its calls again under the same
                // keys; one that asks for something else at a step diverged.
                let stored = rec.spec.as_ref().map_or(&[][..], |s| s.command_fingerprint.as_slice());
                if !stored.is_empty() && !spec.command_fingerprint.is_empty() && stored != spec.command_fingerprint {
                    let mut a = ack_rec(tk, Status::Rejected, &rec);
                    a.reason = "workflow divergence: on replay this step made a different call than the run recorded \
                                (a workflow must make the same calls in the same order; use ch.workflow.now/random/record \
                                for anything that varies)"
                        .into();
                    acks.push(a);
                    continue;
                }
                acks.push(ack_rec(tk, Status::Duplicate, &rec));
                continue;
            }
            match load::<pb::RunRecord>(cx.kv, &keys::run(tk.run.vshard(), &tk.run)) {
                Some(r) if r.state() == pb::run_record::State::Active => {}
                _ => {
                    let mut a = ack(&tk.pb(), Status::Rejected);
                    a.reason = "run is not active".into();
                    acks.push(a);
                    continue;
                }
            }
            // One pin for the submitting client, plus one per dependent or
            // forwarder that arrived before this producer did.
            let early = [keys::DEPENDENT, keys::FORWARD]
                .iter()
                .map(|tag| cx.kv.scan_prefix(&keys::of_producer(v, *tag, tk.pair()), usize::MAX).len() as u32)
                .sum::<u32>();
            let mut rec = pb::TaskRecord {
                spec: Some(spec.clone()),
                next_attempt: 1,
                pins: 1 + early,
                submitted_hlc: cx.now,
                ..Default::default()
            };
            rec.set_state(TState::Pending);
            // Cancelling a call cancels what it submitted, including calls a
            // cancelled body submits afterwards (cancellation is cooperative).
            let parent = spec.parent.as_ref().and_then(Tk::parse).filter(|p| p.v() == v);
            if let Some(p) = parent {
                cx.kv.put(keys::pair(v, keys::CHILD, p.pair(), tk.pair()), Vec::new());
            }
            let mut failed_dep: Option<pb::Outcome> = parent
                .and_then(|p| load::<pb::TaskRecord>(cx.kv, &p.key())?.outcome)
                .and_then(|o| cancelled_kind(&o))
                .map(|k| err_outcome(k, "the call that submitted this one was cancelled"));
            let mut seen = std::collections::BTreeSet::new();
            for dep in &spec.deps {
                let Some(d) = Tk::parse(dep) else {
                    failed_dep.get_or_insert_with(|| err_outcome(Kind::DataLost, "malformed dependency key"));
                    continue;
                };
                // The same producer passed twice is one dependency: its index
                // entry is one key, so it is counted and pinned once.
                if !seen.insert(d) {
                    continue;
                }
                // Every local dependency is pinned exactly once here and
                // unpinned exactly once when this task completes.
                self.pin(cx, v, d);
                if failed_dep.is_some() {
                    continue;
                }
                if let Some(o) = self.known_outcome(cx.kv, v, d) {
                    if !is_ok(&o) {
                        failed_dep = Some(causal(d, &o));
                    }
                    continue;
                }
                if d.v() == v && cx.kv.get(&d.key()).is_none() {
                    // A producer here that is not submitted yet must belong to a live run.
                    let live = load::<pb::RunRecord>(cx.kv, &keys::run(d.run.vshard(), &d.run))
                        .is_some_and(|r| r.state() == pb::run_record::State::Active);
                    if !live {
                        failed_dep = Some(err_outcome(
                            Kind::DataLost,
                            format!("prerequisite {} is not known and its run is not active", d.task.hex()),
                        ));
                        continue;
                    }
                }
                cx.kv.put(keys::pair(v, keys::DEPENDENT, d.pair(), tk.pair()), Vec::new());
                rec.deps_remaining += 1;
                if d.v() != v {
                    cx.events.push(Event::Subscribe { producer: d, subscriber_vshard: v });
                }
            }
            if let Some(o) = failed_dep {
                // complete() drops this task's dependency entries and pins.
                rec.deps_remaining = 0;
                let rec = self.finish(cx, tk, rec, o);
                acks.push(ack_rec(tk, Status::Accepted, &rec));
                continue;
            }
            if rec.deps_remaining == 0 {
                self.mark_ready(cx, tk, &mut rec, true);
            }
            let mut a_status = Status::Accepted;
            let mut attempt = 0;
            if let (Some(peer), TState::Ready) = (grant_to, rec.state())
                && let Ok(a) = self.grant(cx, tk, &mut rec, peer, false)
            {
                a_status = Status::Granted;
                attempt = a;
            }
            store(cx.kv, tk.key(), &rec);
            let mut a = ack_rec(tk, a_status, &rec);
            a.attempt = attempt;
            acks.push(a);
        }
        reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks }))
    }

    /// Outcome of `d` if this vshard already knows it (local record or a
    /// received cross-vshard notification).
    fn known_outcome(&self, kv: &dyn KvRead, v: u16, d: Tk) -> Option<pb::Outcome> {
        if d.v() == v {
            load::<pb::TaskRecord>(kv, &d.key()).and_then(|r| r.outcome)
        } else {
            load::<pb::Outcome>(kv, &keys::notified(v, d.pair()))
        }
    }

    fn mark_ready(&self, cx: &mut Cx, tk: Tk, rec: &mut pb::TaskRecord, deps_satisfied: bool) {
        rec.set_state(TState::Ready);
        cx.kv.put(keys::ready(tk.v(), &tk.run, &tk.task), Vec::new());
        if deps_satisfied {
            cx.events.push(Event::DepsSatisfied(tk));
        }
        cx.events.push(Event::Ready(tk));
    }

    fn grant(
        &self,
        cx: &mut Cx,
        tk: Tk,
        rec: &mut pb::TaskRecord,
        peer: PeerInc,
        speculative: bool,
    ) -> Result<u32, &'static str> {
        if rec.state() == TState::Done {
            return Err("task is done");
        }
        if self.is_retired(cx.kv, peer) {
            return Err("peer incarnation is retired");
        }
        if speculative {
            let allowed = rec.spec.as_ref().is_some_and(|s| s.speculation_allowed) && effects(rec) == pb::Effects::None;
            if !allowed {
                return Err("speculation is not allowed for this task");
            }
            if rec.state() != TState::Running || live_attempts(rec) != 1 {
                return Err("speculation needs exactly one live attempt");
            }
        } else if rec.state() != TState::Ready {
            return Err("task is not ready");
        }
        let attempt = rec.next_attempt.max(1);
        rec.next_attempt = attempt + 1;
        rec.attempts.push(pb::Attempt { attempt, peer: Some(peer_pb(peer)), revoked: false, speculative });
        rec.set_state(TState::Running);
        cx.kv.delete(&keys::ready(tk.v(), &tk.run, &tk.task));
        cx.kv.put(keys::grant(tk.v(), &peer.0, peer.1, &tk.run, &tk.task, attempt), Vec::new());
        cx.events.push(Event::Granted { task: tk, attempt, peer, speculative });
        Ok(attempt)
    }

    fn grant_all(&self, cx: &mut Cx, v: u16, g: &pb::GrantAttempts) -> pb::ShardReply {
        let mut acks = Vec::with_capacity(g.grants.len());
        for gr in &g.grants {
            let key = gr.task.clone().unwrap_or_default();
            let (Some(tk), Some(peer)) = (Tk::parse(&key), peer_of(&gr.peer)) else {
                acks.push(ack(&key, Status::Rejected));
                continue;
            };
            if tk.v() != v {
                acks.push(ack(&key, Status::Rejected));
                continue;
            }
            let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else {
                acks.push(ack(&key, Status::NotFound));
                continue;
            };
            match self.grant(cx, tk, &mut rec, peer, gr.speculative) {
                Ok(attempt) => {
                    store(cx.kv, tk.key(), &rec);
                    let mut a = ack_rec(tk, Status::Granted, &rec);
                    a.attempt = attempt;
                    acks.push(a);
                }
                Err(why) => {
                    let status = if rec.state() == TState::Done { Status::Existing } else { Status::Denied };
                    let mut a = ack_rec(tk, status, &rec);
                    a.reason = why.into();
                    acks.push(a);
                }
            }
        }
        reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks }))
    }

    fn start_external(&self, cx: &mut Cx, v: u16, s: &pb::StartExternal) -> pb::ShardReply {
        let key = s.task.clone().unwrap_or_default();
        let single = |a: pb::TaskAck| reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![a] }));
        let (Some(tk), Some(peer)) = (Tk::parse(&key), peer_of(&s.peer)) else {
            return single(ack(&key, Status::Rejected));
        };
        if tk.v() != v {
            return single(ack(&key, Status::Rejected));
        }
        let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else {
            return single(ack(&key, Status::NotFound));
        };
        if rec.state() == TState::Done {
            return single(ack_rec(tk, Status::Existing, &rec));
        }
        let deny = |rec: &pb::TaskRecord, status: Status, why: &str| {
            let mut a = ack_rec(tk, status, rec);
            a.reason = why.into();
            single(a)
        };
        if effects(&rec) != pb::Effects::External {
            return deny(&rec, Status::Denied, "task does not declare external effects");
        }
        if rec.external_started_attempt != 0 {
            if rec.external_started_attempt == s.attempt && self.attempt_live(&rec, s.attempt, peer) {
                let mut a = ack_rec(tk, Status::Accepted, &rec);
                a.attempt = s.attempt;
                return single(a);
            }
            return deny(&rec, Status::Denied, "start already consumed by another attempt");
        }
        if self.is_retired(cx.kv, peer) || !self.attempt_live(&rec, s.attempt, peer) {
            return deny(&rec, Status::Stale, "attempt is not live");
        }
        rec.external_started_attempt = s.attempt;
        store(cx.kv, tk.key(), &rec);
        let mut a = ack_rec(tk, Status::Accepted, &rec);
        a.attempt = s.attempt;
        single(a)
    }

    fn attempt_live(&self, rec: &pb::TaskRecord, attempt: u32, peer: PeerInc) -> bool {
        rec.attempts.iter().any(|a| a.attempt == attempt && !a.revoked && peer_of(&a.peer) == Some(peer))
    }

    fn publish(&self, cx: &mut Cx, v: u16, p: &pb::PublishResults) -> pb::ShardReply {
        let mut acks = Vec::with_capacity(p.entries.len());
        for e in &p.entries {
            acks.push(self.publish_one(cx, v, e));
        }
        reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks }))
    }

    fn publish_one(&self, cx: &mut Cx, v: u16, e: &pb::PublishEntry) -> pb::TaskAck {
        let key = e.task.clone().unwrap_or_default();
        let (Some(tk), Some(peer)) = (Tk::parse(&key), peer_of(&e.peer)) else {
            return ack(&key, Status::Rejected);
        };
        let forward = e.forward_to.as_ref().and_then(Tk::parse);
        let outcome = e.outcome.clone().filter(|o| o.value.is_some());
        if (forward.is_none() && outcome.is_none()) || tk.v() != v || e.attempt == 0 {
            return ack(&key, Status::Rejected);
        }
        let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else {
            return ack(&key, Status::NotFound);
        };
        #[cfg(test)]
        let overwrite = self.mutants.overwrite_outcome;
        #[cfg(not(test))]
        let overwrite = false;
        if rec.state() == TState::Done && !overwrite {
            return ack_rec(tk, Status::Existing, &rec);
        }
        if self.is_retired(cx.kv, peer) {
            return stale(tk, &rec, "publisher incarnation is retired");
        }
        if rec.state() == TState::Pending {
            return stale(tk, &rec, "dependencies are not satisfied; this task cannot have run");
        }
        let known = rec.attempts.iter().position(|a| a.attempt == e.attempt && peer_of(&a.peer) == Some(peer));
        match known {
            Some(i) if rec.attempts[i].revoked && !self.skip_fencing() => {
                return stale(tk, &rec, "attempt was revoked");
            }
            Some(_) => {}
            None if effects(&rec) == pb::Effects::External => {
                return stale(tk, &rec, "external-effect attempt was never granted");
            }
            None => {
                // Optimistic dispatch: a replayable task's attempt may publish
                // before its grant commits. Any non-fenced attempt is valid.
                rec.attempts.push(pb::Attempt {
                    attempt: e.attempt,
                    peer: Some(peer_pb(peer)),
                    revoked: false,
                    speculative: false,
                });
                rec.next_attempt = rec.next_attempt.max(e.attempt + 1);
            }
        }
        if effects(&rec) == pb::Effects::External && rec.external_started_attempt != e.attempt {
            return stale(tk, &rec, "attempt did not consume the external start");
        }
        if let Some(f) = forward {
            return self.forward(cx, tk, rec, f);
        }
        let mut outcome = outcome.expect("checked above");
        outcome.attempt = e.attempt;
        outcome.peer = Some(peer_pb(peer));
        let rec = self.finish(cx, tk, rec, outcome);
        ack_rec(tk, Status::Published, &rec)
    }

    fn skip_fencing(&self) -> bool {
        #[cfg(test)]
        return self.mutants.skip_fencing;
        #[cfg(not(test))]
        false
    }

    fn record_failure(&self, cx: &mut Cx, v: u16, f: &pb::RecordFailure) -> pb::ShardReply {
        let key = f.task.clone().unwrap_or_default();
        let single = |a: pb::TaskAck| reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![a] }));
        let (Some(tk), Some(peer)) = (Tk::parse(&key), peer_of(&f.peer)) else {
            return single(ack(&key, Status::Rejected));
        };
        if tk.v() != v || f.attempt == 0 {
            return single(ack(&key, Status::Rejected));
        }
        let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else {
            return single(ack(&key, Status::NotFound));
        };
        if rec.state() == TState::Done {
            return single(ack_rec(tk, Status::Existing, &rec));
        }
        if self.is_retired(cx.kv, peer) {
            return single(stale(tk, &rec, "reporter incarnation is retired"));
        }
        if rec.state() == TState::Pending {
            return single(stale(tk, &rec, "dependencies are not satisfied; this task cannot have run"));
        }
        let external = effects(&rec) == pb::Effects::External;
        if f.kind() == pb::record_failure::Kind::Requeued {
            let live =
                rec.attempts.iter().any(|a| a.attempt == f.attempt && !a.revoked && peer_of(&a.peer) == Some(peer));
            if !live {
                return single(stale(tk, &rec, "only a live attempt can be given back"));
            }
            if external && rec.external_started_attempt == f.attempt {
                return single(stale(tk, &rec, "the attempt's external effect may have started"));
            }
        }
        match rec.attempts.iter().position(|a| a.attempt == f.attempt && peer_of(&a.peer) == Some(peer)) {
            Some(i) if rec.attempts[i].revoked => return single(stale(tk, &rec, "attempt was already revoked")),
            Some(i) => rec.attempts[i].revoked = true,
            None if external => {
                return single(stale(tk, &rec, "external-effect attempt was never granted"));
            }
            None => {
                rec.attempts.push(pb::Attempt {
                    attempt: f.attempt,
                    peer: Some(peer_pb(peer)),
                    revoked: true,
                    speculative: false,
                });
                rec.next_attempt = rec.next_attempt.max(f.attempt + 1);
            }
        }
        cx.kv.delete(&keys::grant(v, &peer.0, peer.1, &tk.run, &tk.task, f.attempt));
        let mut err = f.error.clone().unwrap_or_default();
        err.attempt = f.attempt;
        let rec = self.after_attempt_lost(cx, tk, rec, f.attempt, f.kind(), err, &f.fingerprint, f.memory_peak);
        single(ack_rec(tk, Status::Accepted, &rec))
    }

    /// Decide what happens after an attempt is lost: keep waiting on other
    /// live attempts, requeue within the retry budget, or commit a terminal
    /// outcome. Saves the record.
    #[allow(clippy::too_many_arguments)]
    fn after_attempt_lost(
        &self,
        cx: &mut Cx,
        tk: Tk,
        mut rec: pb::TaskRecord,
        attempt: u32,
        kind: pb::record_failure::Kind,
        err: pb::ErrorRecord,
        fingerprint: &[u8],
        memory_peak: u64,
    ) -> pb::TaskRecord {
        use pb::record_failure::Kind as F;
        if kind != F::Requeued {
            rec.failure_history.push(err.clone());
            if rec.failure_history.len() > MAX_FAILURE_HISTORY {
                rec.failure_history.remove(0);
            }
        }
        if live_attempts(&rec) > 0 {
            store(cx.kv, tk.key(), &rec);
            return rec;
        }
        let external = effects(&rec) == pb::Effects::External;
        let started = external && rec.external_started_attempt == attempt && attempt != 0;
        let terminal_err = |kind: Kind, msg: String| {
            let mut e = err.clone();
            e.kind = kind as i32;
            e.message = msg;
            pb::Outcome { value: Some(pb::outcome::Value::Err(e)), ..Default::default() }
        };
        let decision: Option<pb::Outcome> = match kind {
            F::Infra | F::Oom if started => Some(terminal_err(
                Kind::OutcomeUnknown,
                format!("attempt {attempt} was lost after its external effect may have started: {}", err.message),
            )),
            F::RetryableUser if external => Some(terminal_err(Kind::User, err.message.clone())),
            // Given back unstarted: ready again, nothing counted.
            F::Requeued => None,
            F::Infra if external => {
                rec.infra_failures += 1;
                (rec.infra_failures >= EXTERNAL_PRESTART_RETRIES).then(|| {
                    terminal_err(
                        Kind::Infra,
                        format!("{} attempts lost before start: {}", rec.infra_failures, err.message),
                    )
                })
            }
            F::Infra => {
                rec.infra_failures += 1;
                (rec.infra_failures + rec.user_retries >= max_attempts(&rec)).then(|| {
                    terminal_err(
                        Kind::Infra,
                        format!(
                            "{} attempts lost to infrastructure failures; last: {}",
                            rec.infra_failures, err.message
                        ),
                    )
                })
            }
            F::Oom => {
                rec.oom_failures += 1;
                rec.memory_hint = rec.memory_hint.max(memory_peak.saturating_add(memory_peak / 2));
                let budget = rec.spec.as_ref().map_or(2, |s| s.max_oom_escalations);
                (rec.oom_failures > budget).then(|| {
                    terminal_err(
                        Kind::Oom,
                        format!("out of memory after {} escalations: {}", rec.oom_failures - 1, err.message),
                    )
                })
            }
            F::RetryableUser => {
                if !fingerprint.is_empty() && rec.last_fingerprint == fingerprint {
                    Some(terminal_err(Kind::User, err.message.clone()))
                } else {
                    rec.last_fingerprint = fingerprint.to_vec();
                    rec.user_retries += 1;
                    (rec.infra_failures + rec.user_retries >= max_attempts(&rec))
                        .then(|| terminal_err(Kind::User, err.message.clone()))
                }
            }
        };
        match decision {
            Some(o) => self.finish(cx, tk, rec, o),
            None => {
                self.mark_ready(cx, tk, &mut rec, false);
                store(cx.kv, tk.key(), &rec);
                rec
            }
        }
    }

    /// `tk`'s attempt returned the future of `f`: `tk` completes with `f`'s
    /// outcome. If `f` is not done, `tk` waits without a grant (its worker is
    /// free) and `cascade` completes it. `f` stays pinned for the run's
    /// lifetime, because `tk`'s outcome will name `f`'s result.
    fn forward(&self, cx: &mut Cx, tk: Tk, mut rec: pb::TaskRecord, f: Tk) -> pb::TaskAck {
        if f == tk {
            return stale(tk, &rec, "a call cannot forward to its own result");
        }
        self.pin(cx, tk.v(), f);
        if let Some(o) = self.known_outcome(cx.kv, tk.v(), f) {
            let rec = self.finish(cx, tk, rec, o);
            return ack_rec(tk, Status::Published, &rec);
        }
        for a in &rec.attempts {
            if let Some(p) = peer_of(&a.peer) {
                cx.kv.delete(&keys::grant(tk.v(), &p.0, p.1, &tk.run, &tk.task, a.attempt));
            }
        }
        cx.kv.delete(&keys::ready(tk.v(), &tk.run, &tk.task));
        rec.set_state(TState::Pending);
        rec.forwarded_to = Some(f.pb());
        cx.kv.put(keys::pair(tk.v(), keys::FORWARD, f.pair(), tk.pair()), Vec::new());
        if f.v() != tk.v() {
            cx.events.push(Event::Subscribe { producer: f, subscriber_vshard: tk.v() });
        }
        store(cx.kv, tk.key(), &rec);
        ack_rec(tk, Status::Published, &rec)
    }

    fn cancel(&self, cx: &mut Cx, v: u16, c: &pb::Cancel) -> pb::ShardReply {
        let key = c.task.clone().unwrap_or_default();
        let single = |a: pb::TaskAck| reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![a] }));
        let Some(tk) = Tk::parse(&key).filter(|t| t.v() == v) else {
            return single(ack(&key, Status::Rejected));
        };
        let Some(rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else {
            return single(ack(&key, Status::NotFound));
        };
        if rec.state() == TState::Done {
            return single(ack_rec(tk, Status::Existing, &rec));
        }
        let kind = match c.reason() {
            k @ (Kind::Deadline | Kind::ActorDied | Kind::Infeasible | Kind::Oom) => k,
            _ => Kind::Cancelled,
        };
        let msg = if c.message.is_empty() { "cancelled".to_string() } else { c.message.clone() };
        let rec = self.finish(cx, tk, rec, err_outcome(kind, msg));
        // Then everything it submitted, transitively, that has not finished.
        let mut parents = vec![tk];
        while let Some(p) = parents.pop() {
            for (ckey, _) in cx.kv.scan_prefix(&keys::of_producer(v, keys::CHILD, p.pair()), usize::MAX) {
                let (run, task) = keys::trailing_pair(&ckey);
                let child = Tk { run, task };
                parents.push(child);
                if let Some(crec) = load::<pb::TaskRecord>(cx.kv, &child.key()).filter(|r| r.state() != TState::Done) {
                    let why = format!("the call that submitted it ({}) was cancelled", tk.task.hex());
                    self.finish(cx, child, crec, err_outcome(kind, why));
                }
            }
        }
        single(ack_rec(tk, Status::Accepted, &rec))
    }

    /// Commit `outcome` and propagate it to dependents and subscribers.
    fn finish(&self, cx: &mut Cx, tk: Tk, rec: pb::TaskRecord, outcome: pb::Outcome) -> pb::TaskRecord {
        let rec = self.complete(cx, tk, rec, outcome.clone());
        self.cascade(cx, tk.v(), tk, outcome);
        rec
    }

    /// Pin a local producer's result for a dependent. Remote producers are
    /// pinned through their own vshard's subscription.
    fn pin(&self, cx: &mut Cx, v: u16, producer: Tk) {
        if producer.v() != v {
            return;
        }
        if let Some(mut p) = load::<pb::TaskRecord>(cx.kv, &producer.key()) {
            p.pins += 1;
            store(cx.kv, producer.key(), &p);
        }
    }

    fn unpin(&self, cx: &mut Cx, producer: Tk) {
        let Some(mut p) = load::<pb::TaskRecord>(cx.kv, &producer.key()) else { return };
        if p.pins == 0 {
            return;
        }
        p.pins -= 1;
        if p.pins == 0 && p.state() == TState::Done {
            cx.events.push(Event::Releasable(producer));
        }
        store(cx.kv, producer.key(), &p);
    }

    fn complete(&self, cx: &mut Cx, tk: Tk, mut rec: pb::TaskRecord, outcome: pb::Outcome) -> pb::TaskRecord {
        for a in &rec.attempts {
            if let Some(p) = peer_of(&a.peer) {
                cx.kv.delete(&keys::grant(tk.v(), &p.0, p.1, &tk.run, &tk.task, a.attempt));
            }
        }
        cx.kv.delete(&keys::ready(tk.v(), &tk.run, &tk.task));
        rec.set_state(TState::Done);
        rec.outcome = Some(outcome.clone());
        rec.done_hlc = cx.now;
        store(cx.kv, tk.key(), &rec);
        cx.events.push(Event::Done { task: tk, outcome });
        if rec.pins == 0 {
            cx.events.push(Event::Releasable(tk));
        }
        // A completed dependent stops waiting and no longer needs its local
        // producers' results. Removing its entries keeps a later-submitted
        // producer from counting it as a pin.
        let deps: std::collections::BTreeSet<Tk> =
            rec.spec.as_ref().map_or_else(Default::default, |s| s.deps.iter().filter_map(Tk::parse).collect());
        for d in deps {
            cx.kv.delete(&keys::pair(tk.v(), keys::DEPENDENT, d.pair(), tk.pair()));
            if d.v() == tk.v() {
                self.unpin(cx, d);
            }
        }
        rec
    }

    /// Propagate `outcome` of `producer` to dependents recorded in vshard `v`
    /// and to remote subscribers. Each dependency edge is visited once because
    /// its index entry is deleted when processed. Dependents always live in
    /// their own vshard, so a causal failure continues in `d.v()`.
    fn cascade(&self, cx: &mut Cx, v: u16, producer: Tk, outcome: pb::Outcome) {
        let mut work = vec![(v, producer, outcome)];
        while let Some((v, p, o)) = work.pop() {
            for (dkey, _) in cx.kv.scan_prefix(&keys::of_producer(v, keys::DEPENDENT, p.pair()), usize::MAX) {
                cx.kv.delete(&dkey);
                let (run, task) = keys::trailing_pair(&dkey);
                let d = Tk { run, task };
                let Some(mut drec) = load::<pb::TaskRecord>(cx.kv, &d.key()) else { continue };
                if drec.state() == TState::Done {
                    continue;
                }
                if is_ok(&o) {
                    drec.deps_remaining = drec.deps_remaining.saturating_sub(1);
                    if drec.deps_remaining == 0 && drec.state() == TState::Pending {
                        self.mark_ready(cx, d, &mut drec, true);
                    }
                    store(cx.kv, d.key(), &drec);
                } else {
                    let c = causal(p, &o);
                    self.complete(cx, d, drec, c.clone());
                    work.push((d.v(), d, c));
                }
            }
            for (fkey, _) in cx.kv.scan_prefix(&keys::of_producer(v, keys::FORWARD, p.pair()), usize::MAX) {
                cx.kv.delete(&fkey);
                let (run, task) = keys::trailing_pair(&fkey);
                let f = Tk { run, task };
                if let Some(frec) = load::<pb::TaskRecord>(cx.kv, &f.key()).filter(|r| r.state() != TState::Done) {
                    self.complete(cx, f, frec, o.clone());
                    work.push((f.v(), f, o.clone()));
                }
            }
            if p.v() != v {
                continue; // remote producer: its subscribers live in its own group
            }
            for (skey, _) in cx.kv.scan_prefix(&keys::of_producer(v, keys::SUBSCRIBER, p.pair()), usize::MAX) {
                cx.kv.delete(&skey);
                let sub_v = u16::from_be_bytes([skey[skey.len() - 2], skey[skey.len() - 1]]);
                self.append_outbox(cx, sub_v, pb::DepNotify { producer: Some(p.pb()), outcome: Some(o.clone()) });
            }
        }
    }

    fn append_outbox(&self, cx: &mut Cx, target_vshard: u16, notify: pb::DepNotify) {
        let seq_key = keys::meta(keys::META_OUTBOX_SEQ);
        let seq = load_u64(cx.kv, &seq_key) + 1;
        cx.kv.put(seq_key, seq.to_be_bytes().to_vec());
        let entry = pb::OutboxEntry { seq, target_vshard: target_vshard as u32, notify: Some(notify) };
        store(cx.kv, keys::outbox(seq), &entry);
        cx.events.push(Event::Outbox { seq, target_vshard });
    }

    fn subscribe(&self, cx: &mut Cx, v: u16, s: &pb::Subscribe) -> pb::ShardReply {
        let key = s.producer.clone().unwrap_or_default();
        let single = |a: pb::TaskAck| reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![a] }));
        let Some(p) = Tk::parse(&key).filter(|t| t.v() == v) else {
            return single(ack(&key, Status::Rejected));
        };
        let sub_v = s.subscriber_vshard as u16;
        match load::<pb::TaskRecord>(cx.kv, &p.key()) {
            Some(rec) if rec.state() == TState::Done => {
                let o = rec.outcome.clone().unwrap_or_default();
                self.append_outbox(cx, sub_v, pb::DepNotify { producer: Some(p.pb()), outcome: Some(o) });
                single(ack_rec(p, Status::Existing, &rec))
            }
            Some(rec) => {
                cx.kv.put(keys::subscriber(v, p.pair(), sub_v), Vec::new());
                single(ack_rec(p, Status::Accepted, &rec))
            }
            None => {
                let live = load::<pb::RunRecord>(cx.kv, &keys::run(p.run.vshard(), &p.run))
                    .is_some_and(|r| r.state() == pb::run_record::State::Active);
                if live {
                    cx.kv.put(keys::subscriber(v, p.pair(), sub_v), Vec::new());
                    single(ack(&key, Status::Accepted))
                } else {
                    let o = err_outcome(Kind::DataLost, format!("producer {} is not known here", p.task.hex()));
                    self.append_outbox(cx, sub_v, pb::DepNotify { producer: Some(p.pb()), outcome: Some(o) });
                    single(ack(&key, Status::NotFound))
                }
            }
        }
    }

    fn dep_notify(&self, cx: &mut Cx, v: u16, n: &pb::DepNotify) -> pb::ShardReply {
        let key = n.producer.clone().unwrap_or_default();
        let single = |a: pb::TaskAck| reply(pb::shard_reply::Body::Tasks(pb::TaskAcks { acks: vec![a] }));
        let (Some(p), Some(o)) = (Tk::parse(&key), n.outcome.clone()) else {
            return single(ack(&key, Status::Rejected));
        };
        let nkey = keys::notified(v, p.pair());
        if cx.kv.get(&nkey).is_some() {
            return single(ack(&key, Status::Duplicate));
        }
        store(cx.kv, nkey, &o);
        self.cascade(cx, v, p, o);
        single(ack(&key, Status::Accepted))
    }

    fn release(&self, cx: &mut Cx, v: u16, r: &pb::Release) -> pb::ShardReply {
        let mut released = 0u64;
        for key in &r.tasks {
            let Some(tk) = Tk::parse(key).filter(|t| t.v() == v) else { continue };
            let Some(mut rec) = load::<pb::TaskRecord>(cx.kv, &tk.key()) else { continue };
            if rec.pins == 0 {
                continue;
            }
            rec.pins -= 1;
            if rec.pins == 0 && rec.state() == TState::Done {
                cx.events.push(Event::Releasable(tk));
            }
            store(cx.kv, tk.key(), &rec);
            released += 1;
        }
        reply(pb::shard_reply::Body::Count(released))
    }
}

/// Read helpers for peers serving queries from (possibly stale) local state.
pub mod read {
    use super::*;

    pub fn task(kv: &dyn KvRead, tk: Tk) -> Option<pb::TaskRecord> {
        load(kv, &tk.key())
    }

    /// Outcome of `producer` as seen from a dependent in `vshard`: its own
    /// record when the same group holds it, else the copy a DepNotify left.
    pub fn dep_outcome(kv: &dyn KvRead, vshard: u16, producer: Tk) -> Option<pb::Outcome> {
        task(kv, producer)
            .and_then(|r| r.outcome)
            .or_else(|| load(kv, &keys::notified(vshard, (&producer.run, &producer.task))))
    }

    pub fn run(kv: &dyn KvRead, run: Id) -> Option<pb::RunRecord> {
        load(kv, &keys::run(run.vshard(), &run))
    }

    /// One page of a run's tasks after the task id `after` (exclusive; empty =
    /// from the start), in task-id order.
    pub fn tasks_page(kv: &dyn KvRead, run: Id, after: Option<Id>, limit: usize) -> Vec<(Tk, pb::TaskRecord)> {
        let prefix = keys::tasks_of_run(run.vshard(), &run);
        let after_key = after.map(|t| keys::task(run.vshard(), &run, &t)).unwrap_or_default();
        kv.scan_after(&prefix, &after_key, limit)
            .into_iter()
            .map(|(k, v)| {
                let (r, t) = keys::trailing_pair(&k);
                (Tk { run: r, task: t }, pb::TaskRecord::decode(v.as_slice()).expect("shard state is corrupt"))
            })
            .collect()
    }

    /// Task records of one run, in key order, at most `limit`.
    pub fn tasks_of_run(kv: &dyn KvRead, run: Id, limit: usize) -> Vec<pb::TaskRecord> {
        kv.scan_prefix(&keys::tasks_of_run(run.vshard(), &run), limit)
            .into_iter()
            .map(|(_, v)| pb::TaskRecord::decode(v.as_slice()).expect("shard state is corrupt"))
            .collect()
    }

    pub fn actor(kv: &dyn KvRead, actor: Id) -> Option<pb::ActorRecord> {
        load(kv, &keys::actor(actor.vshard(), &actor))
    }

    /// Ids of the actors whose records live in `vshard`.
    pub fn actors(kv: &dyn KvRead, vshard: u16, limit: usize) -> Vec<Id> {
        let mut prefix = vshard.to_be_bytes().to_vec();
        prefix.push(keys::ACTOR);
        kv.scan_prefix(&prefix, limit)
            .into_iter()
            .filter(|(k, _)| k.len() == 3 + 16)
            .filter_map(|(k, _)| Id::from_slice(&k[3..]).ok())
            .collect()
    }

    /// Runnable, ungranted tasks of `vshard` (a new leader schedules these).
    pub fn ready(kv: &dyn KvRead, vshard: u16, limit: usize) -> Vec<Tk> {
        let prefix = {
            let mut p = vshard.to_be_bytes().to_vec();
            p.push(keys::READY);
            p
        };
        kv.scan_prefix(&prefix, limit)
            .into_iter()
            .map(|(k, _)| {
                let (run, task) = keys::trailing_pair(&k);
                Tk { run, task }
            })
            .collect()
    }

    /// Live attempts of `vshard`: (task, attempt, holder). A new leader
    /// re-sends these to their holders, which ignore duplicates.
    pub fn granted(kv: &dyn KvRead, vshard: u16, limit: usize) -> Vec<(Tk, u32, PeerInc)> {
        let mut prefix = vshard.to_be_bytes().to_vec();
        prefix.push(keys::GRANT);
        kv.scan_prefix(&prefix, limit)
            .into_iter()
            .filter_map(|(k, _)| {
                // [v:2][G][peer:16][inc:8][run:16][task:16][attempt:4]
                if k.len() != 3 + 16 + 8 + 32 + 4 {
                    return None;
                }
                let peer = Id::from_slice(&k[3..19]).ok()?;
                let inc = u64::from_be_bytes(k[19..27].try_into().ok()?);
                let (run, task) = keys::trailing_pair(&k[..k.len() - 4]);
                let attempt = u32::from_be_bytes(k[k.len() - 4..].try_into().ok()?);
                Some((Tk { run, task }, attempt, (peer, inc)))
            })
            .collect()
    }

    pub fn outbox(kv: &dyn KvRead, limit: usize) -> Vec<pb::OutboxEntry> {
        kv.scan_prefix(&keys::outbox_prefix(), limit)
            .into_iter()
            .map(|(_, v)| pb::OutboxEntry::decode(v.as_slice()).expect("shard state is corrupt"))
            .collect()
    }

    pub fn owned_vshards(kv: &dyn KvRead) -> Vec<(u16, u64)> {
        kv.scan_prefix(&keys::owned_prefix(), usize::MAX)
            .into_iter()
            .map(|(k, v)| {
                let vs = u16::from_be_bytes([k[k.len() - 2], k[k.len() - 1]]);
                (vs, u64::from_be_bytes(v.as_slice().try_into().expect("8-byte epoch")))
            })
            .collect()
    }
}

fn stale(tk: Tk, rec: &pb::TaskRecord, why: &str) -> pb::TaskAck {
    let mut a = ack_rec(tk, Status::Stale, rec);
    a.reason = why.into();
    a
}

fn load_u64_opt(kv: &dyn KvRead, key: &[u8]) -> Option<u64> {
    kv.get(key).map(|b| u64::from_be_bytes(b.as_slice().try_into().expect("8-byte counter")))
}

fn load_u64(kv: &dyn KvRead, key: &[u8]) -> u64 {
    load_u64_opt(kv, key).unwrap_or(0)
}
