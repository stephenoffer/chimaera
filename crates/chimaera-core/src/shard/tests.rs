//! Scenario tests and a model-based property test of the shard state machine.
//!
//! The property test drives random command sequences (including stale
//! incarnations, revoked attempts, duplicated and reordered cross-vshard
//! notifications) through two replicas and checks the module invariants after
//! every step. `mutants_are_caught` proves the checks can fail by running the
//! same test against deliberately broken state machines.

use super::*;
use crate::kv::{KvRead, MemKv};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use std::collections::{BTreeMap, BTreeSet};

const V: u16 = 5;
const V2: u16 = 6;
const EPOCH: u64 = 1;

fn run_id(v: u16) -> Id {
    Id::derive(0, v, &[b"run", &v.to_le_bytes()])
}

fn task_id(v: u16, i: usize) -> Tk {
    let run = run_id(v);
    Tk { run, task: run.child(&[&(i as u64).to_le_bytes()]) }
}

fn peer(i: usize) -> Id {
    Id::derive(0, 0, &[b"peer", &(i as u64).to_le_bytes()])
}

fn obj(tag: u8) -> pb::Outcome {
    pb::Outcome {
        value: Some(pb::outcome::Value::Ok(pb::ObjectRef { inline_data: vec![tag], size: 1, ..Default::default() })),
        ..Default::default()
    }
}

fn user_err(msg: &str) -> pb::Outcome {
    err_outcome(Kind::User, msg)
}

struct Harness {
    sm: ShardState,
    a: MemKv,
    b: MemKv,
    hlc: u64,
}

impl Harness {
    fn new(sm: ShardState) -> Self {
        let mut h = Harness { sm, a: MemKv::default(), b: MemKv::default(), hlc: 1 };
        h.raw(Body::AdoptVshard(pb::AdoptVShard { vshards: vec![V as u32, V2 as u32], epoch: EPOCH }), V);
        for v in [V, V2] {
            h.raw(
                Body::RunCreate(pb::RunCreate {
                    run: Some(pb::RunRecord { run: run_id(v).to_vec(), tenant: "t".into(), ..Default::default() }),
                }),
                v,
            );
        }
        h
    }

    /// Apply on both replicas; replica B sees the command after a wire round
    /// trip. Returns replica A's result.
    fn raw(&mut self, body: Body, v: u16) -> Applied {
        self.hlc += 1;
        let cmd = pb::ShardCmd { vshard: v as u32, vshard_epoch: EPOCH, issuer: None, hlc: self.hlc, body: Some(body) };
        let a = self.sm.apply(&mut self.a, &cmd);
        let wire = pb::ShardCmd::decode(cmd.encode_to_vec().as_slice()).unwrap();
        let b = self.sm.apply(&mut self.b, &wire);
        assert_eq!(a.reply, b.reply, "replicas returned different replies");
        assert_eq!(a.events, b.events, "replicas emitted different events");
        a
    }

    fn task(&self, tk: Tk) -> Option<pb::TaskRecord> {
        load(&self.a, &tk.key())
    }

    fn submit(&mut self, spec: pb::TaskSpec, grant_to: Option<PeerInc>) -> (pb::TaskAck, Vec<Event>) {
        let v = Tk::parse(spec.key.as_ref().unwrap()).unwrap().v();
        let r = self.raw(Body::SubmitTasks(pb::SubmitTasks { tasks: vec![spec], grant_to: grant_to.map(peer_pb) }), v);
        (first_ack(&r.reply), r.events)
    }

    fn grant(&mut self, tk: Tk, p: PeerInc, speculative: bool) -> pb::TaskAck {
        let r = self.raw(
            Body::Grant(pb::GrantAttempts {
                grants: vec![pb::Grant { task: Some(tk.pb()), peer: Some(peer_pb(p)), speculative }],
            }),
            tk.v(),
        );
        first_ack(&r.reply)
    }

    fn publish(&mut self, tk: Tk, attempt: u32, p: PeerInc, o: pb::Outcome) -> pb::TaskAck {
        let r = self.raw(
            Body::Publish(pb::PublishResults {
                entries: vec![pb::PublishEntry {
                    task: Some(tk.pb()),
                    attempt,
                    peer: Some(peer_pb(p)),
                    outcome: Some(o),
                }],
            }),
            tk.v(),
        );
        first_ack(&r.reply)
    }

    fn fail(&mut self, tk: Tk, attempt: u32, p: PeerInc, kind: pb::record_failure::Kind, fp: &[u8]) -> pb::TaskAck {
        let r = self.raw(
            Body::RecordFailure(pb::RecordFailure {
                task: Some(tk.pb()),
                attempt,
                peer: Some(peer_pb(p)),
                kind: kind as i32,
                error: Some(pb::ErrorRecord { message: "boom".into(), ..Default::default() }),
                fingerprint: fp.to_vec(),
                memory_peak: 1000,
            }),
            tk.v(),
        );
        first_ack(&r.reply)
    }

    fn start(&mut self, tk: Tk, attempt: u32, p: PeerInc) -> pb::TaskAck {
        let r = self.raw(
            Body::StartExternal(pb::StartExternal { task: Some(tk.pb()), attempt, peer: Some(peer_pb(p)) }),
            tk.v(),
        );
        first_ack(&r.reply)
    }

    fn cancel(&mut self, tk: Tk) -> pb::TaskAck {
        let r = self.raw(
            Body::Cancel(pb::Cancel { task: Some(tk.pb()), reason: Kind::Cancelled as i32, message: String::new() }),
            tk.v(),
        );
        first_ack(&r.reply)
    }

    fn retire(&mut self, p: PeerInc) -> Applied {
        self.raw(Body::Retire(pb::RetireIncarnation { peer: Some(peer_pb(p)) }), V)
    }
}

fn first_ack(r: &pb::ShardReply) -> pb::TaskAck {
    match &r.body {
        Some(pb::shard_reply::Body::Tasks(t)) => t.acks[0].clone(),
        other => panic!("expected task acks, got {other:?}"),
    }
}

fn spec(tk: Tk, deps: &[Tk], effects: pb::Effects, max_attempts: u32) -> pb::TaskSpec {
    pb::TaskSpec {
        key: Some(tk.pb()),
        deps: deps.iter().map(Tk::pb).collect(),
        effects: effects as i32,
        max_attempts,
        speculation_allowed: effects == pb::Effects::None,
        max_oom_escalations: 2,
        ..Default::default()
    }
}

fn status(a: &pb::TaskAck) -> Status {
    a.status()
}

fn err_kind(rec: &pb::TaskRecord) -> Option<Kind> {
    match rec.outcome.as_ref()?.value.as_ref()? {
        pb::outcome::Value::Err(e) => Some(e.kind()),
        pb::outcome::Value::Ok(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[test]
fn chain_becomes_ready_after_producer_publishes() {
    let mut h = Harness::new(ShardState::new());
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    let p = (peer(0), 1);
    assert_eq!(status(&h.submit(spec(a, &[], pb::Effects::None, 3), Some(p)).0), Status::Granted);
    let (ack_b, ev) = h.submit(spec(b, &[a], pb::Effects::None, 3), None);
    assert_eq!(ack_b.state(), TState::Pending);
    assert!(!ev.contains(&Event::Ready(b)));
    assert_eq!(status(&h.publish(a, 1, p, obj(1))), Status::Published);
    assert_eq!(h.task(b).unwrap().state(), TState::Ready);
    assert_eq!(status(&h.grant(b, p, false)), Status::Granted);
    assert_eq!(status(&h.publish(b, 1, p, obj(2))), Status::Published);
    // A late duplicate publication returns the stored decision.
    let dup = h.publish(a, 1, p, obj(9));
    assert_eq!(status(&dup), Status::Existing);
    assert_eq!(dup.outcome, Some(pb::Outcome { attempt: 1, peer: Some(peer_pb(p)), ..obj(1) }));
}

#[test]
fn failed_prerequisite_completes_dependents_with_causal_error() {
    let mut h = Harness::new(ShardState::new());
    let (a, b, c) = (task_id(V, 0), task_id(V, 1), task_id(V, 2));
    let p = (peer(0), 1);
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[a], pb::Effects::None, 3), None);
    h.submit(spec(c, &[b], pb::Effects::None, 3), None);
    h.publish(a, 1, p, user_err("ValueError: bad input"));
    for t in [b, c] {
        let rec = h.task(t).unwrap();
        assert_eq!(err_kind(&rec), Some(Kind::Causal));
        assert!(rec.attempts.is_empty(), "a dependent of a failure never runs");
    }
    let Some(pb::outcome::Value::Err(e)) = h.task(c).unwrap().outcome.unwrap().value else { panic!() };
    assert_eq!(Tk::parse(&e.upstream.unwrap()), Some(b));
    assert!(e.message.contains("ValueError: bad input"));
    // A dependent submitted after the failure fails immediately too.
    let d = task_id(V, 3);
    let (ack, _) = h.submit(spec(d, &[a], pb::Effects::None, 3), None);
    assert_eq!(ack.state(), TState::Done);
}

#[test]
fn infrastructure_failures_retry_within_budget_then_fail() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let p = (peer(0), 1);
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.fail(a, 1, p, pb::record_failure::Kind::Infra, b"");
    assert_eq!(h.task(a).unwrap().state(), TState::Ready);
    assert_eq!(h.grant(a, p, false).attempt, 2);
    h.fail(a, 2, p, pb::record_failure::Kind::Infra, b"");
    h.grant(a, p, false);
    h.fail(a, 3, p, pb::record_failure::Kind::Infra, b"");
    let rec = h.task(a).unwrap();
    assert_eq!(err_kind(&rec), Some(Kind::Infra));
    assert_eq!(rec.failure_history.len(), 3);
}

#[test]
fn oom_escalations_have_their_own_budget_and_raise_the_memory_hint() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let p = (peer(0), 1);
    h.submit(spec(a, &[], pb::Effects::None, 1), Some(p));
    h.fail(a, 1, p, pb::record_failure::Kind::Oom, b"");
    let rec = h.task(a).unwrap();
    assert_eq!(rec.state(), TState::Ready, "OOM retries do not consume max_attempts=1");
    assert_eq!(rec.memory_hint, 1500);
    h.grant(a, p, false);
    h.fail(a, 2, p, pb::record_failure::Kind::Oom, b"");
    h.grant(a, p, false);
    h.fail(a, 3, p, pb::record_failure::Kind::Oom, b"");
    assert_eq!(err_kind(&h.task(a).unwrap()), Some(Kind::Oom));
}

#[test]
fn second_identical_retryable_exception_is_terminal() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let p = (peer(0), 1);
    h.submit(spec(a, &[], pb::Effects::None, 5), Some(p));
    h.fail(a, 1, p, pb::record_failure::Kind::RetryableUser, b"fp1");
    h.grant(a, p, false);
    h.fail(a, 2, p, pb::record_failure::Kind::RetryableUser, b"fp2");
    assert_eq!(h.task(a).unwrap().state(), TState::Ready);
    h.grant(a, p, false);
    h.fail(a, 3, p, pb::record_failure::Kind::RetryableUser, b"fp2");
    assert_eq!(err_kind(&h.task(a).unwrap()), Some(Kind::User));
}

#[test]
fn external_effect_start_is_consumed_once_and_loss_after_start_is_unknown() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let (p, q) = ((peer(0), 1), (peer(1), 1));
    h.submit(spec(a, &[], pb::Effects::External, 1), Some(p));
    // An attempt that was never granted cannot start or publish.
    assert_eq!(status(&h.start(a, 7, q)), Status::Stale);
    assert_eq!(status(&h.start(a, 1, p)), Status::Accepted);
    assert_eq!(status(&h.start(a, 1, p)), Status::Accepted, "idempotent for the same attempt");
    h.fail(a, 1, p, pb::record_failure::Kind::Infra, b"");
    assert_eq!(err_kind(&h.task(a).unwrap()), Some(Kind::OutcomeUnknown));

    // Lost before start: safe to retry.
    let b = task_id(V, 1);
    h.submit(spec(b, &[], pb::Effects::External, 1), Some(p));
    h.fail(b, 1, p, pb::record_failure::Kind::Infra, b"");
    assert_eq!(h.task(b).unwrap().state(), TState::Ready);
    h.grant(b, q, false);
    assert_eq!(status(&h.publish(b, 2, q, obj(1))), Status::Stale, "must consume the start first");
    assert_eq!(status(&h.start(b, 2, q)), Status::Accepted);
    assert_eq!(status(&h.publish(b, 2, q, obj(1))), Status::Published);
}

#[test]
fn retired_incarnation_is_fenced_and_its_work_requeued() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let old = (peer(0), 1);
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(old));
    h.retire(old);
    let rec = h.task(a).unwrap();
    assert_eq!(rec.state(), TState::Ready);
    assert!(rec.attempts[0].revoked);
    assert_eq!(status(&h.publish(a, 1, old, obj(1))), Status::Stale);
    assert_eq!(status(&h.grant(a, old, false)), Status::Denied);
    let new = (peer(0), 2);
    assert_eq!(h.grant(a, new, false).attempt, 2);
    assert_eq!(status(&h.publish(a, 2, new, obj(2))), Status::Published);
}

#[test]
fn cancel_and_publish_are_exclusive() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(status(&h.cancel(a)), Status::Accepted);
    assert_eq!(status(&h.publish(a, 1, p, obj(1))), Status::Existing);
    assert_eq!(err_kind(&h.task(a).unwrap()), Some(Kind::Cancelled));
    assert_eq!(status(&h.publish(b, 1, p, obj(1))), Status::Published);
    let late = h.cancel(b);
    assert_eq!(status(&late), Status::Existing);
    assert!(is_ok(late.outcome.as_ref().unwrap()), "cancellation after publication reports the result");
}

#[test]
fn speculation_runs_two_attempts_and_first_publication_wins() {
    let mut h = Harness::new(ShardState::new());
    let a = task_id(V, 0);
    let (p, q) = ((peer(0), 1), (peer(1), 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(h.grant(a, q, true).attempt, 2);
    assert_eq!(status(&h.grant(a, q, true)), Status::Denied, "at most one speculative copy");
    assert_eq!(status(&h.publish(a, 2, q, obj(2))), Status::Published);
    assert_eq!(status(&h.publish(a, 1, p, obj(1))), Status::Existing);
    // Idempotent tasks are never speculated.
    let b = task_id(V, 1);
    h.submit(spec(b, &[], pb::Effects::Idempotent, 3), Some(p));
    assert_eq!(status(&h.grant(b, q, true)), Status::Denied);
}

#[test]
fn cross_vshard_dependency_resolves_through_outbox_once() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let prod = task_id(V2, 0);
    let cons = task_id(V, 0);
    h.submit(spec(prod, &[], pb::Effects::None, 3), Some(p));
    let (_, ev) = h.submit(spec(cons, &[prod], pb::Effects::None, 3), None);
    assert!(ev.contains(&Event::Subscribe { producer: prod, subscriber_vshard: V }));
    h.raw(Body::Subscribe(pb::Subscribe { producer: Some(prod.pb()), subscriber_vshard: V as u32 }), V2);
    let r = h.publish(prod, 1, p, obj(3));
    assert_eq!(status(&r), Status::Published);
    let entries = h.a.scan_prefix(&keys::outbox_prefix(), 10);
    assert_eq!(entries.len(), 1);
    let entry = pb::OutboxEntry::decode(entries[0].1.as_slice()).unwrap();
    assert_eq!(entry.target_vshard, V as u32);
    let notify = entry.notify.unwrap();
    let first = h.raw(Body::DepNotify(notify.clone()), V);
    assert!(first.events.contains(&Event::DepsSatisfied(cons)));
    let dup = h.raw(Body::DepNotify(notify), V);
    assert_eq!(status(&first_ack(&dup.reply)), Status::Duplicate);
    h.raw(Body::OutboxAck(pb::OutboxAck { seqs: vec![entry.seq] }), V2);
    assert!(h.a.scan_prefix(&keys::outbox_prefix(), 10).is_empty());
}

#[test]
fn durable_actor_commands_are_deduplicated() {
    let mut h = Harness::new(ShardState::new());
    let actor = Id::derive(0, V, &[b"cart/42"]);
    let host = (peer(0), 1);
    let ack = |r: &Applied| match &r.reply.body {
        Some(pb::shard_reply::Body::Actor(a)) => a.clone(),
        other => panic!("{other:?}"),
    };
    let spec = pb::ActorSpec { actor: actor.to_vec(), durable: true, ..Default::default() };
    h.raw(Body::ActorCreate(pb::ActorCreate { spec: Some(spec) }), V);
    let placed = ack(&h.raw(
        Body::ActorPlace(pb::ActorPlace { actor: actor.to_vec(), expected_generation: 0, host: Some(peer_pb(host)) }),
        V,
    ));
    assert_eq!(placed.record.unwrap().generation, 1);
    let commit = |h: &mut Harness, cmd: &[u8], fp: &[u8], prev: u64, state: u8, actor_gen: u64| {
        ack(&h.raw(
            Body::ActorCommit(pb::ActorCommandCommit {
                actor: actor.to_vec(),
                generation: actor_gen,
                cmd_id: cmd.to_vec(),
                fingerprint: fp.to_vec(),
                prev_version: prev,
                new_state: vec![state],
                reply: Some(pb::ObjectRef { inline_data: vec![state], ..Default::default() }),
            }),
            V,
        ))
    };
    // The design doc's crash case: the reply to cmd1 is lost, cmd2 commits,
    // then cmd1 is retried. It returns (5, v1); current state stays (7, v2).
    assert_eq!(commit(&mut h, b"cmd1", b"add 5", 0, 5, 1).status(), AStatus::Ok);
    assert_eq!(commit(&mut h, b"cmd2", b"add 2", 1, 7, 1).status(), AStatus::Ok);
    let retry = commit(&mut h, b"cmd1", b"add 5", 0, 5, 1);
    assert_eq!(retry.status(), AStatus::Duplicate);
    assert_eq!((retry.reply.unwrap().inline_data, retry.version), (vec![5], 1));
    let rec: pb::ActorRecord = load(&h.a, &keys::actor(V, &actor)).unwrap();
    assert_eq!((rec.durable_state, rec.state_version), (vec![7], 2));
    assert_eq!(commit(&mut h, b"cmd1", b"add 6", 0, 6, 1).status(), AStatus::Conflict);
    assert_eq!(commit(&mut h, b"cmd3", b"x", 2, 8, 0).status(), AStatus::StaleGeneration);
    assert_eq!(commit(&mut h, b"cmd3", b"x", 1, 8, 1).status(), AStatus::VersionConflict);
    // Losing the host restarts the actor; the old generation is fenced.
    // (A terminal end, below, never restarts.)
    h.retire(host);
    let rec: pb::ActorRecord = load(&h.a, &keys::actor(V, &actor)).unwrap();
    assert_eq!(rec.state(), AState::Restarting);
    assert_eq!(commit(&mut h, b"cmd3", b"x", 2, 8, 0).status(), AStatus::StaleGeneration);
    h.raw(
        Body::ActorPlace(pb::ActorPlace {
            actor: actor.to_vec(),
            expected_generation: 1,
            host: Some(peer_pb((peer(0), 2))),
        }),
        V,
    );
    h.raw(Body::ActorDied(pb::ActorDied { actor: actor.to_vec(), generation: 2, reason: None, terminal: true }), V);
    let rec: pb::ActorRecord = load(&h.a, &keys::actor(V, &actor)).unwrap();
    assert_eq!(rec.state(), AState::Dead, "a terminal end is never restarted");
}

#[test]
fn a_result_stays_pinned_until_its_dependents_complete() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[a], pb::Effects::None, 3), None);
    h.publish(a, 1, p, obj(1));
    // The client drops its handle to `a`, but `b` has not run yet.
    let r = h.raw(Body::Release(pb::Release { tasks: vec![a.pb()] }), V);
    assert!(!r.events.contains(&Event::Releasable(a)));
    h.grant(b, p, false);
    let done = h.raw(
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(b.pb()),
                attempt: 1,
                peer: Some(peer_pb(p)),
                outcome: Some(obj(2)),
            }],
        }),
        V,
    );
    assert!(done.events.contains(&Event::Releasable(a)), "{:?}", done.events);
    // A dependent submitted before its producer still pins it.
    let (c, d) = (task_id(V, 2), task_id(V, 3));
    h.submit(spec(d, &[c], pb::Effects::None, 3), None);
    h.submit(spec(c, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(h.task(c).unwrap().pins, 2);
    // A dependent cancelled before its producer exists leaves no stale pin.
    let (e, f) = (task_id(V, 4), task_id(V, 5));
    h.submit(spec(f, &[e], pb::Effects::None, 3), None);
    h.cancel(f);
    h.submit(spec(e, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(h.task(e).unwrap().pins, 1, "only the client pin remains");
    // A dependent that fails on one dep releases the pins it took on the others.
    let (g, x, y) = (task_id(V, 6), task_id(V, 7), task_id(V, 8));
    h.submit(spec(g, &[], pb::Effects::None, 3), Some(p));
    h.publish(g, 1, p, user_err("boom"));
    h.submit(spec(x, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(y, &[g, x], pb::Effects::None, 3), None);
    assert_eq!(err_kind(&h.task(y).unwrap()), Some(Kind::Causal));
    assert_eq!(h.task(x).unwrap().pins, 1, "pin on x released by y's completion");
}

#[test]
fn finished_run_cancels_leftovers_and_expires() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[], pb::Effects::None, 3), Some(p));
    h.publish(a, 1, p, obj(1));
    let fin = h.raw(Body::RunFinish(pb::RunFinish { run: run_id(V).to_vec(), state: 0 }), V);
    assert!(fin.events.contains(&Event::RunEnded(run_id(V))));
    assert_eq!(err_kind(&h.task(b).unwrap()), Some(Kind::Cancelled));
    let (rejected, _) = h.submit(spec(task_id(V, 2), &[], pb::Effects::None, 3), None);
    assert_eq!(status(&rejected), Status::Rejected);
    // Before retention elapses nothing is collected.
    h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V);
    assert!(h.task(a).is_some());
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    let gc = h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V);
    assert!(gc.events.contains(&Event::RunExpired(run_id(V))));
    assert!(h.task(a).is_none() && h.task(b).is_none());
    assert!(h.a.get(&keys::run(V, &run_id(V))).is_none());
}

#[test]
fn envelope_rejects_unowned_stale_and_retired() {
    let mut h = Harness::new(ShardState::new());
    let cmd = |v: u16, epoch: u64, issuer: Option<PeerInc>| pb::ShardCmd {
        vshard: v as u32,
        vshard_epoch: epoch,
        issuer: issuer.map(peer_pb),
        hlc: 100,
        body: Some(Body::Release(pb::Release { tasks: vec![] })),
    };
    let rej = |a: Applied| matches!(a.reply.body, Some(pb::shard_reply::Body::Rejected(_)));
    assert!(rej(h.sm.apply(&mut h.a, &cmd(9, EPOCH, None))));
    assert!(rej(h.sm.apply(&mut h.a, &cmd(V, EPOCH + 1, None))));
    h.retire((peer(2), 1));
    assert!(rej(h.sm.apply(&mut h.a, &cmd(V, EPOCH, Some((peer(2), 1))))));
    assert!(!rej(h.sm.apply(&mut h.a, &cmd(V, EPOCH, Some((peer(2), 2))))));
}

// ---------------------------------------------------------------------------
// Model-based property test
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct TaskDef {
    v: u16,
    deps: Vec<usize>,
    effects: pb::Effects,
    max_attempts: u32,
}

#[derive(Clone, Debug)]
enum Op {
    Submit { t: usize, grant: Option<usize> },
    Grant { t: usize, p: usize, stale: bool, spec: bool },
    Start { t: usize, attempt: u32, p: usize, informed: bool },
    Publish { t: usize, attempt: u32, p: usize, stale: bool, ok: bool, informed: bool },
    Fail { t: usize, attempt: u32, p: usize, kind: i32, fp: u8, informed: bool },
    Cancel { t: usize },
    Retire { p: usize },
    Deliver { i: usize, keep: bool },
    ActorCreate { a: usize },
    ActorPlace { a: usize, gen_off: i64, p: usize },
    ActorCommit { a: usize, cmd: u8, fp: u8, gen_off: i64, prev_off: i64 },
    ActorDied { a: usize },
    Release { t: usize },
}

const N_TASKS: usize = 8;
const N_PEERS: usize = 3;

fn task_defs() -> impl Strategy<Value = Vec<TaskDef>> {
    let one = |i: usize| {
        (
            prop_oneof![Just(V), Just(V), Just(V2)],
            proptest::collection::vec(0..i.max(1), 0..=2.min(i)),
            prop_oneof![
                3 => Just(pb::Effects::None),
                1 => Just(pb::Effects::Idempotent),
                1 => Just(pb::Effects::External)
            ],
            1u32..=3,
        )
            .prop_map(move |(v, deps, effects, max_attempts)| TaskDef {
                v,
                deps: if i == 0 { vec![] } else { deps.into_iter().collect::<BTreeSet<_>>().into_iter().collect() },
                effects,
                max_attempts,
            })
    };
    (0..N_TASKS).map(one).collect::<Vec<_>>()
}

fn op() -> impl Strategy<Value = Op> {
    let t = 0..N_TASKS;
    let p = 0..N_PEERS;
    prop_oneof![
        3 => (t.clone(), proptest::option::of(p.clone())).prop_map(|(t, grant)| Op::Submit { t, grant }),
        3 => (t.clone(), p.clone(), any::<bool>(), proptest::bool::weighted(0.2))
            .prop_map(|(t, p, stale, spec)| Op::Grant { t, p, stale: stale && p == 0, spec }),
        2 => (t.clone(), 1u32..4, p.clone(), proptest::bool::weighted(0.7))
            .prop_map(|(t, attempt, p, informed)| Op::Start { t, attempt, p, informed }),
        4 => (t.clone(), 1u32..4, p.clone(), proptest::bool::weighted(0.2), proptest::bool::weighted(0.8), proptest::bool::weighted(0.6))
            .prop_map(|(t, attempt, p, stale, ok, informed)| Op::Publish { t, attempt, p, stale, ok, informed }),
        2 => (t.clone(), 1u32..4, p.clone(), 0i32..3, 0u8..2, proptest::bool::weighted(0.6))
            .prop_map(|(t, attempt, p, kind, fp, informed)| Op::Fail { t, attempt, p, kind, fp, informed }),
        1 => t.clone().prop_map(|t| Op::Cancel { t }),
        1 => p.clone().prop_map(|p| Op::Retire { p }),
        3 => (any::<usize>(), proptest::bool::weighted(0.3)).prop_map(|(i, keep)| Op::Deliver { i, keep }),
        1 => (0usize..2).prop_map(|a| Op::ActorCreate { a }),
        1 => (0usize..2, -1i64..=1, p.clone()).prop_map(|(a, gen_off, p)| Op::ActorPlace { a, gen_off, p }),
        3 => (0usize..2, 0u8..4, 0u8..2, -1i64..=1, -1i64..=1)
            .prop_map(|(a, cmd, fp, gen_off, prev_off)| Op::ActorCommit { a, cmd, fp, gen_off, prev_off }),
        1 => (0usize..2).prop_map(|a| Op::ActorDied { a }),
        1 => t.prop_map(|t| Op::Release { t }),
    ]
}

thread_local! {
    static STATS: std::cell::RefCell<BTreeMap<String, u64>> = const { std::cell::RefCell::new(BTreeMap::new()) };
}

fn bump(what: impl Into<String>) {
    STATS.with(|s| *s.borrow_mut().entry(what.into()).or_default() += 1);
}

/// Follow-up commands the leaders would propose in response to events.
#[derive(Clone)]
struct Pending {
    v: u16,
    body: Body,
    ack_seq: Option<u64>,
}

fn run_case(defs: &[TaskDef], ops: &[Op], sm: ShardState) -> Result<(), TestCaseError> {
    let mut h = Harness::new(sm);
    let tks: Vec<Tk> = (0..N_TASKS).map(|i| task_id(defs[i].v, i)).collect();
    let actors: Vec<Id> = (0..2).map(|i| Id::derive(0, V, &[b"actor", &[i as u8]])).collect();
    let mut inc = [1u64; N_PEERS];
    let mut retired: BTreeSet<PeerInc> = BTreeSet::new();
    let mut first_outcome: BTreeMap<Tk, Vec<u8>> = BTreeMap::new();
    let mut deps_satisfied: BTreeMap<Tk, u32> = BTreeMap::new();
    let mut done_events: BTreeMap<Tk, u32> = BTreeMap::new();
    let mut ext_started: BTreeMap<Tk, u32> = BTreeMap::new();
    let mut actor_cmds: BTreeMap<(usize, u8), (u8, Option<pb::ObjectRef>, u64)> = BTreeMap::new();
    let mut pending: Vec<Pending> = Vec::new();

    for op in ops {
        let pre: BTreeMap<Tk, pb::TaskRecord> = tks.iter().filter_map(|t| Some((*t, h.task(*t)?))).collect();
        let peer_at = |p: usize, stale: bool, inc: &[u64; N_PEERS]| -> PeerInc {
            let i = if stale && inc[p] > 1 { inc[p] - 1 } else { inc[p] };
            (peer(p), i)
        };
        // An informed op targets the task's newest live attempt, so start,
        // publish and failure paths are reached, not only rejected.
        let live = |t: usize| -> Option<(u32, PeerInc)> {
            let rec = pre.get(&tks[t])?;
            rec.attempts.iter().rev().find(|a| !a.revoked).and_then(|a| Some((a.attempt, peer_of(&a.peer)?)))
        };
        let pick = |t: usize, informed: bool, attempt: u32, pi: PeerInc| -> (u32, PeerInc) {
            if informed { live(t).unwrap_or((attempt, pi)) } else { (attempt, pi) }
        };
        let applied: Option<Applied> = match op {
            Op::Submit { t, grant } => {
                let d = &defs[*t];
                let deps: Vec<Tk> = d.deps.iter().map(|&i| tks[i]).collect();
                let s = spec(tks[*t], &deps, d.effects, d.max_attempts);
                Some(h.raw(
                    Body::SubmitTasks(pb::SubmitTasks {
                        tasks: vec![s],
                        grant_to: grant.map(|p| peer_pb(peer_at(p, false, &inc))),
                    }),
                    tks[*t].v(),
                ))
            }
            Op::Grant { t, p, stale, spec } => {
                let pi = peer_at(*p, *stale, &inc);
                Some(h.raw(
                    Body::Grant(pb::GrantAttempts {
                        grants: vec![pb::Grant {
                            task: Some(tks[*t].pb()),
                            peer: Some(peer_pb(pi)),
                            speculative: *spec,
                        }],
                    }),
                    tks[*t].v(),
                ))
            }
            Op::Start { t, attempt, p, informed } => {
                let (attempt, pi) = pick(*t, *informed, *attempt, peer_at(*p, false, &inc));
                let attempt = &attempt;
                let a = h.raw(
                    Body::StartExternal(pb::StartExternal {
                        task: Some(tks[*t].pb()),
                        attempt: *attempt,
                        peer: Some(peer_pb(pi)),
                    }),
                    tks[*t].v(),
                );
                let ack = first_ack(&a.reply);
                if ack.status() == Status::Accepted {
                    let prev = ext_started.insert(tks[*t], *attempt);
                    prop_assert!(prev.is_none() || prev == Some(*attempt), "external start consumed twice");
                }
                Some(a)
            }
            Op::Publish { t, attempt, p, stale, ok, informed } => {
                let (attempt, pi) = pick(*t, *informed && !*stale, *attempt, peer_at(*p, *stale, &inc));
                let attempt = &attempt;
                let o = if *ok { obj(*attempt as u8) } else { user_err("e") };
                let a = h.raw(
                    Body::Publish(pb::PublishResults {
                        entries: vec![pb::PublishEntry {
                            task: Some(tks[*t].pb()),
                            attempt: *attempt,
                            peer: Some(peer_pb(pi)),
                            outcome: Some(o),
                        }],
                    }),
                    tks[*t].v(),
                );
                if first_ack(&a.reply).status() == Status::Published {
                    prop_assert!(!retired.contains(&pi), "retired incarnation published");
                    if let Some(rec) = pre.get(&tks[*t]) {
                        let revoked = rec
                            .attempts
                            .iter()
                            .any(|x| x.attempt == *attempt && x.revoked && peer_of(&x.peer) == Some(pi));
                        prop_assert!(!revoked, "revoked attempt published");
                        prop_assert!(rec.state() != TState::Pending, "task published before its dependencies");
                    }
                }
                Some(a)
            }
            Op::Fail { t, attempt, p, kind, fp, informed } => {
                let (attempt, pi) = pick(*t, *informed, *attempt, peer_at(*p, false, &inc));
                let attempt = &attempt;
                Some(h.raw(
                    Body::RecordFailure(pb::RecordFailure {
                        task: Some(tks[*t].pb()),
                        attempt: *attempt,
                        peer: Some(peer_pb(pi)),
                        kind: *kind,
                        error: Some(pb::ErrorRecord { message: "x".into(), ..Default::default() }),
                        fingerprint: vec![*fp],
                        memory_peak: 10,
                    }),
                    tks[*t].v(),
                ))
            }
            Op::Cancel { t } => Some(h.raw(
                Body::Cancel(pb::Cancel {
                    task: Some(tks[*t].pb()),
                    reason: Kind::Cancelled as i32,
                    message: String::new(),
                }),
                tks[*t].v(),
            )),
            Op::Retire { p } => {
                let pi = (peer(*p), inc[*p]);
                retired.insert(pi);
                inc[*p] += 1;
                Some(h.retire(pi))
            }
            Op::Deliver { i, keep } => {
                if pending.is_empty() {
                    None
                } else {
                    let idx = i % pending.len();
                    let item = if *keep { pending[idx].clone() } else { pending.remove(idx) };
                    let a = h.raw(item.body.clone(), item.v);
                    if let (Some(seq), false) = (item.ack_seq, *keep) {
                        h.raw(Body::OutboxAck(pb::OutboxAck { seqs: vec![seq] }), V);
                    }
                    Some(a)
                }
            }
            Op::ActorCreate { a } => Some(h.raw(
                Body::ActorCreate(pb::ActorCreate {
                    spec: Some(pb::ActorSpec {
                        actor: actors[*a].to_vec(),
                        durable: true,
                        restart: pb::actor_spec::Restart::Fresh as i32,
                        ..Default::default()
                    }),
                }),
                V,
            )),
            Op::ActorPlace { a, gen_off, p } => {
                let actor_gen = load::<pb::ActorRecord>(&h.a, &keys::actor(V, &actors[*a])).map_or(0, |r| r.generation);
                Some(h.raw(
                    Body::ActorPlace(pb::ActorPlace {
                        actor: actors[*a].to_vec(),
                        expected_generation: (actor_gen as i64 + gen_off).max(0) as u64,
                        host: Some(peer_pb(peer_at(*p, false, &inc))),
                    }),
                    V,
                ))
            }
            Op::ActorCommit { a, cmd, fp, gen_off, prev_off } => {
                let rec = load::<pb::ActorRecord>(&h.a, &keys::actor(V, &actors[*a]));
                let (actor_gen, ver) = rec.as_ref().map_or((0, 0), |r| (r.generation, r.state_version));
                let res = h.raw(
                    Body::ActorCommit(pb::ActorCommandCommit {
                        actor: actors[*a].to_vec(),
                        generation: (actor_gen as i64 + gen_off).max(0) as u64,
                        cmd_id: vec![*cmd],
                        fingerprint: vec![*fp],
                        prev_version: (ver as i64 + prev_off).max(0) as u64,
                        new_state: vec![*cmd, *fp],
                        reply: Some(pb::ObjectRef { inline_data: vec![*cmd, *fp, ver as u8], ..Default::default() }),
                    }),
                    V,
                );
                if let Some(pb::shard_reply::Body::Actor(ack)) = &res.reply.body {
                    match (ack.status(), actor_cmds.get(&(*a, *cmd))) {
                        (AStatus::Ok, None) => {
                            actor_cmds.insert((*a, *cmd), (*fp, ack.reply.clone(), ack.version));
                        }
                        (AStatus::Ok, Some(_)) => prop_assert!(false, "command id applied twice"),
                        (AStatus::Duplicate, Some((f, r, v))) => {
                            prop_assert_eq!(*f, *fp);
                            prop_assert_eq!((&ack.reply, ack.version), (r, *v), "duplicate returned a different reply");
                        }
                        (AStatus::Conflict, Some((f, _, _))) => prop_assert_ne!(*f, *fp),
                        (AStatus::Duplicate | AStatus::Conflict, None) => {
                            prop_assert!(false, "dedup of unknown command")
                        }
                        _ => {}
                    }
                }
                Some(res)
            }
            Op::ActorDied { a } => {
                let actor_gen = load::<pb::ActorRecord>(&h.a, &keys::actor(V, &actors[*a])).map_or(0, |r| r.generation);
                Some(h.raw(
                    Body::ActorDied(pb::ActorDied {
                        actor: actors[*a].to_vec(),
                        generation: actor_gen,
                        reason: None,
                        terminal: false,
                    }),
                    V,
                ))
            }
            Op::Release { t } => Some(h.raw(Body::Release(pb::Release { tasks: vec![tks[*t].pb()] }), tks[*t].v())),
        };

        if let Some(a) = &applied {
            match &a.reply.body {
                Some(pb::shard_reply::Body::Tasks(t)) => {
                    for ack in &t.acks {
                        bump(
                            format!("{:?}/{:?}", std::mem::discriminant(op), ack.status())
                                .replace("Discriminant", "op"),
                        );
                    }
                }
                Some(pb::shard_reply::Body::Actor(ack)) => bump(format!("actor/{:?}", ack.status())),
                _ => {}
            }
            for e in &a.events {
                match e {
                    Event::Done { outcome, .. } => bump(format!(
                        "done/{}",
                        if is_ok(outcome) {
                            "ok".to_string()
                        } else {
                            match &outcome.value {
                                Some(pb::outcome::Value::Err(e)) => format!("{:?}", e.kind()),
                                _ => "?".into(),
                            }
                        }
                    )),
                    Event::DepsSatisfied(_) => bump("deps_satisfied"),
                    Event::Outbox { .. } => bump("outbox"),
                    _ => {}
                }
            }
            for e in &a.events {
                match e {
                    Event::DepsSatisfied(t) => *deps_satisfied.entry(*t).or_default() += 1,
                    Event::Done { task, .. } => *done_events.entry(*task).or_default() += 1,
                    Event::Subscribe { producer, subscriber_vshard } => pending.push(Pending {
                        v: producer.v(),
                        body: Body::Subscribe(pb::Subscribe {
                            producer: Some(producer.pb()),
                            subscriber_vshard: *subscriber_vshard as u32,
                        }),
                        ack_seq: None,
                    }),
                    Event::Outbox { seq, target_vshard } => {
                        let entry: pb::OutboxEntry = load(&h.a, &keys::outbox(*seq)).expect("outbox entry exists");
                        pending.push(Pending {
                            v: *target_vshard,
                            body: Body::DepNotify(entry.notify.unwrap()),
                            ack_seq: Some(*seq),
                        });
                    }
                    _ => {}
                }
            }
        }

        // Invariants over the whole state after the step.
        prop_assert_eq!(h.a.digest(), h.b.digest(), "replicas diverged");
        for (i, tk) in tks.iter().enumerate() {
            let Some(rec) = h.task(*tk) else { continue };
            prop_assert!(deps_satisfied.get(tk).copied().unwrap_or(0) <= 1, "deps satisfied twice for {}", i);
            prop_assert!(done_events.get(tk).copied().unwrap_or(0) <= 1, "task {} completed twice", i);
            if let Some(o) = &rec.outcome {
                let bytes = o.encode_to_vec();
                let first = first_outcome.entry(*tk).or_insert_with(|| bytes.clone());
                prop_assert_eq!(&*first, &bytes, "outcome of task {} changed", i);
                if is_ok(o) {
                    for &d in &defs[i].deps {
                        if let Some(drec) = h.task(tks[d]) {
                            let failed = drec.outcome.as_ref().is_some_and(|x| !is_ok(x));
                            prop_assert!(!failed, "task {} succeeded although prerequisite {} failed", i, d);
                        }
                    }
                    if defs[i].effects == pb::Effects::External {
                        prop_assert_eq!(
                            Some(&o.attempt),
                            ext_started.get(tk),
                            "external result from an unstarted attempt"
                        );
                    }
                }
            }
            prop_assert_eq!(rec.state() == TState::Done, rec.outcome.is_some());
        }
    }
    Ok(())
}

fn cases() -> impl Strategy<Value = (Vec<TaskDef>, Vec<Op>)> {
    (task_defs(), proptest::collection::vec(op(), 1..80))
}

proptest! {
    // PROPTEST_CASES overrides the default for deeper runs.
    #![proptest_config(Config {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(512),
        failure_persistence: None,
        ..Config::default()
    })]
    #[test]
    fn invariants_hold_under_random_histories((defs, ops) in cases()) {
        run_case(&defs, &ops, ShardState::new())?;
    }
}

/// Each deliberately broken state machine must be caught by the same test.
#[test]
fn mutants_are_caught() {
    let mutants = [
        ("skip_fencing", Mutants { skip_fencing: true, ..Default::default() }),
        ("overwrite_outcome", Mutants { overwrite_outcome: true, ..Default::default() }),
        ("skip_command_dedup", Mutants { skip_command_dedup: true, ..Default::default() }),
    ];
    for (name, m) in mutants {
        let mut runner = TestRunner::new(Config { cases: 2048, failure_persistence: None, ..Config::default() });
        let result = runner.run(&cases(), |(defs, ops)| run_case(&defs, &ops, ShardState { mutants: m }));
        assert!(result.is_err(), "mutant {name} survived the property test");
    }
}

#[test]
#[ignore = "diagnostic: prints how often each path is exercised"]
fn coverage_report() {
    use std::collections::BTreeMap;
    let mut runner = TestRunner::new(Config { cases: 512, failure_persistence: None, ..Config::default() });
    let stats = std::cell::RefCell::new(BTreeMap::<String, u64>::new());
    runner
        .run(&cases(), |(defs, ops)| {
            for op in &ops {
                let name = format!("{op:?}").split_whitespace().next().unwrap_or("").to_string();
                *stats.borrow_mut().entry(name).or_default() += 1;
            }
            run_case(&defs, &ops, ShardState::new())
        })
        .unwrap();
    println!("{:#?}", stats.borrow());
    STATS.with(|s| println!("{:#?}", s.borrow()));
    for (name, m) in [
        ("skip_fencing", Mutants { skip_fencing: true, ..Default::default() }),
        ("overwrite_outcome", Mutants { overwrite_outcome: true, ..Default::default() }),
        ("skip_command_dedup", Mutants { skip_command_dedup: true, ..Default::default() }),
    ] {
        let mut runner = TestRunner::new(Config { cases: 2048, failure_persistence: None, ..Config::default() });
        let r = runner.run(&cases(), |(defs, ops)| run_case(&defs, &ops, ShardState { mutants: m }));
        println!(
            "{name}: {}",
            match r {
                Err(e) => format!("{e}").lines().next().unwrap_or("").to_string(),
                Ok(()) => "SURVIVED".into(),
            }
        );
    }
}
