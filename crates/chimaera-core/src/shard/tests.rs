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
                    forward_to: None,
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

fn publish_raw(h: &mut Harness, tk: Tk, attempt: u32, p: PeerInc, o: pb::Outcome) -> Applied {
    h.raw(
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(tk.pb()),
                attempt,
                peer: Some(peer_pb(p)),
                outcome: Some(o),
                forward_to: None,
            }],
        }),
        tk.v(),
    )
}

#[test]
fn a_dependent_submitted_for_a_peer_is_granted_there_in_the_commit_that_readies_it() {
    let mut h = Harness::new(ShardState::new());
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    let p = (peer(0), 1);
    assert_eq!(status(&h.submit(spec(a, &[], pb::Effects::None, 3), Some(p)).0), Status::Granted);
    let (ack_b, _) = h.submit(spec(b, &[a], pb::Effects::None, 3), Some(p));
    assert_eq!(ack_b.state(), TState::Pending);
    let applied = publish_raw(&mut h, a, 1, p, obj(1));
    let rec = h.task(b).unwrap();
    assert_eq!(rec.state(), TState::Running, "granted, no placement round");
    assert_eq!(rec.attempts.last().and_then(|a| a.peer.clone()), Some(peer_pb(p)));
    assert!(applied.events.contains(&Event::Granted { task: b, attempt: 1, peer: p, speculative: false }));
    assert!(!applied.events.contains(&Event::Ready(b)), "no Ready: the leader must not place it again");
}

#[test]
fn a_dependent_whose_peer_was_retired_meanwhile_is_placed_as_usual() {
    let mut h = Harness::new(ShardState::new());
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    let (p, q) = ((peer(0), 1), (peer(1), 1));
    assert_eq!(status(&h.submit(spec(a, &[], pb::Effects::None, 3), Some(p)).0), Status::Granted);
    h.submit(spec(b, &[a], pb::Effects::None, 3), Some(q));
    h.retire(q);
    let applied = publish_raw(&mut h, a, 1, p, obj(1));
    assert_eq!(h.task(b).unwrap().state(), TState::Ready);
    assert!(applied.events.contains(&Event::Ready(b)));
}

#[test]
fn a_call_waiting_on_many_inputs_is_not_rewritten_as_each_finishes() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let ins: Vec<Tk> = (0..3).map(|i| task_id(V, i)).collect();
    let d = task_id(V, 9);
    for &t in &ins {
        h.submit(spec(t, &[], pb::Effects::None, 3), Some(p));
    }
    h.submit(spec(d, &ins, pb::Effects::None, 3), None);
    let before = h.task(d).unwrap().encode_to_vec();
    h.publish(ins[0], 1, p, obj(1));
    h.publish(ins[1], 1, p, obj(2));
    assert_eq!(h.task(d).unwrap().encode_to_vec(), before, "only the counter moved");
    assert_eq!(read::deps_left(&h.a, d), Some(1));
    h.publish(ins[2], 1, p, obj(3));
    assert_eq!(h.task(d).unwrap().state(), TState::Ready);
    assert_eq!(read::deps_left(&h.a, d), None, "counter gone once ready");
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
    assert!(ev.contains(&Event::Subscribe { producer: prod, subscriber_vshard: V, hold: 1 }));
    h.raw(Body::Subscribe(pb::Subscribe { producer: Some(prod.pb()), subscriber_vshard: V as u32, hold: 1 }), V2);
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
fn a_dependent_in_another_vshard_holds_its_producer_until_it_finishes() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (prod, cons) = (task_id(V2, 0), task_id(V, 0));
    h.submit(spec(prod, &[], pb::Effects::None, 3), Some(p));
    let (_, ev) = h.submit(spec(cons, &[prod], pb::Effects::None, 3), None);
    assert!(ev.contains(&Event::Subscribe { producer: prod, subscriber_vshard: V, hold: 1 }));
    // Every replica of the subscriber's group sends it: it pins once.
    let sub = pb::Subscribe { producer: Some(prod.pb()), subscriber_vshard: V as u32, hold: 1 };
    h.raw(Body::Subscribe(sub.clone()), V2);
    h.raw(Body::Subscribe(sub), V2);
    assert_eq!(read::pins(&h.a, prod), 2, "the client's pin and the subscriber's hold");
    // The client lets go before the producer finishes: the hold keeps its result.
    h.raw(Body::Release(pb::Release { tasks: vec![prod.pb()] }), V2);
    h.publish(prod, 1, p, obj(3));
    assert!(h.task(prod).unwrap().spec.is_some(), "kept while a call in another vshard still reads it");
    let outbox = |h: &Harness| -> Vec<pb::OutboxEntry> {
        h.a.scan_prefix(&keys::outbox_prefix(), 100)
            .iter()
            .map(|(_, v)| pb::OutboxEntry::decode(v.as_slice()).unwrap())
            .collect()
    };
    let notify = outbox(&h).into_iter().find_map(|e| e.notify).expect("the outcome goes to the subscriber");
    assert!(h.raw(Body::DepNotify(notify), V).events.contains(&Event::DepsSatisfied(cons)));
    h.grant(cons, p, false);
    h.publish(cons, 1, p, obj(4));
    // The reader finished: its vshard releases the hold through the outbox.
    let release = outbox(&h).into_iter().find_map(|e| e.unsubscribe).expect("the hold is released");
    assert_eq!((release.subscriber_vshard, release.hold), (V as u32, 1));
    let stale = h.raw(Body::Unsubscribe(pb::Subscribe { hold: 0, ..release.clone() }), V2);
    assert!(stale.events.is_empty() && read::pins(&h.a, prod) == 1, "an older hold's release is ignored");
    let done = h.raw(Body::Unsubscribe(release.clone()), V2);
    assert!(done.events.iter().any(|e| matches!(e, Event::Releasable { task, .. } if *task == prod)));
    assert!(h.task(prod).unwrap().spec.is_none(), "released: shrunk to a tombstone");
    assert!(h.raw(Body::Unsubscribe(release), V2).events.is_empty(), "a repeated release is ignored");
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

fn releases(events: &[Event], tk: Tk) -> bool {
    events.iter().any(|e| matches!(e, Event::Releasable { task, .. } if *task == tk))
}

#[test]
fn a_released_result_leaves_a_small_tombstone_and_resubmission_is_answered() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let a = task_id(V, 0);
    // A typical call: its arguments travel inline in the spec.
    let mut s = spec(a, &[], pb::Effects::None, 3);
    s.args = Some(pb::ObjectRef { inline_data: vec![7; 1024], size: 1024, ..Default::default() });
    h.submit(s, Some(p));
    h.publish(a, 1, p, obj(1));
    let full = h.task(a).unwrap().encoded_len();
    h.raw(Body::Release(pb::Release { tasks: vec![a.pb()] }), V);
    let tomb = h.task(a).unwrap().encoded_len();
    assert!(tomb <= 40 && tomb * 10 < full, "{tomb} bytes left of {full}");
    // The same id again: answered from the tombstone, never run twice.
    let (ack, ev) = h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    assert_ne!(status(&ack), Status::Granted);
    assert!(!ev.iter().any(|e| matches!(e, Event::Granted { .. } | Event::Ready(_))));
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
    assert!(!releases(&r.events, a));
    h.grant(b, p, false);
    let done = h.raw(
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(b.pb()),
                attempt: 1,
                peer: Some(peer_pb(p)),
                outcome: Some(obj(2)),
                forward_to: None,
            }],
        }),
        V,
    );
    assert!(releases(&done.events, a), "{:?}", done.events);
    // The event carries the result for the peer to free; the record shrinks to
    // a tombstone that still answers "done".
    let freed = done.events.iter().find_map(|e| match e {
        Event::Releasable { task, object } if *task == a => object.clone(),
        _ => None,
    });
    assert!(freed.is_some(), "the released result's object is handed to the peer");
    let tomb = h.task(a).unwrap();
    assert_eq!(tomb.state(), TState::Done);
    assert!(tomb.spec.is_none() && tomb.attempts.is_empty(), "spec and attempts dropped");
    assert!(tomb.outcome.as_ref().is_some_and(is_ok), "the outcome stays: replay and attach read it");
    // A dependent submitted before its producer still pins it.
    let (c, d) = (task_id(V, 2), task_id(V, 3));
    h.submit(spec(d, &[c], pb::Effects::None, 3), None);
    h.submit(spec(c, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(read::pins(&h.a, c), 2);
    // A dependent cancelled before its producer exists leaves no stale pin.
    let (e, f) = (task_id(V, 4), task_id(V, 5));
    h.submit(spec(f, &[e], pb::Effects::None, 3), None);
    h.cancel(f);
    h.submit(spec(e, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(read::pins(&h.a, e), 1, "only the client pin remains");
    // A dependent that fails on one dep releases the pins it took on the others.
    let (g, x, y) = (task_id(V, 6), task_id(V, 7), task_id(V, 8));
    h.submit(spec(g, &[], pb::Effects::None, 3), Some(p));
    h.publish(g, 1, p, user_err("boom"));
    h.submit(spec(x, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(y, &[g, x], pb::Effects::None, 3), None);
    assert_eq!(err_kind(&h.task(y).unwrap()), Some(Kind::Causal));
    assert_eq!(read::pins(&h.a, x), 1, "pin on x released by y's completion");
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
fn cancelling_a_spread_call_cancels_its_calls_in_the_runs_own_vshard_and_later_ones_at_once() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let run = run_id(V);
    let copy = pb::RunRecord { run: run.to_vec(), spread: vec![V as u32, V2 as u32], ..Default::default() };
    h.raw(Body::RunCreate(pb::RunCreate { run: Some(copy) }), V2);
    // A call spread to V2 makes an actor call, which stays in the run's own vshard.
    let parent = Tk { run, task: Id::derive(0, V2, &[b"spread", b"parent"]) };
    let child = Tk { run, task: run.child(&[b"actor call"]) };
    h.submit(spec(parent, &[], pb::Effects::None, 3), Some(p));
    h.submit(child_of(child, parent), Some(p));
    assert_eq!(child.v(), V);
    let budget = pb::Cancel { task: Some(parent.pb()), reason: Kind::Budget as i32, message: "budget exceeded".into() };
    h.raw(Body::Cancel(budget), V2);
    assert_eq!(err_kind(&h.task(parent).unwrap()), Some(Kind::Budget));
    assert_eq!(err_kind(&h.task(child).unwrap()), None, "not yet: it lives in another vshard");
    let entries: Vec<pb::OutboxEntry> =
        h.a.scan_prefix(&keys::outbox_prefix(), 100)
            .iter()
            .map(|(_, v)| pb::OutboxEntry::decode(v.as_slice()).unwrap())
            .collect();
    let entry = entries.iter().find(|e| e.cancel_children.is_some()).expect("the run's own vshard hears of it");
    assert_eq!(entry.target_vshard, V as u32);
    let cancel = entry.cancel_children.clone().unwrap();
    h.raw(Body::CancelChildren(cancel.clone()), V);
    let rec = h.task(child).unwrap();
    assert_eq!(err_kind(&rec), Some(Kind::Budget), "the budget's kind reaches the child");
    // Delivered again (the outbox retries): nothing changes.
    h.raw(Body::CancelChildren(cancel), V);
    assert_eq!(h.task(child).unwrap().outcome, rec.outcome);
    // A call the cancelled body makes afterwards fails at once.
    let late = Tk { run, task: run.child(&[b"late actor call"]) };
    h.submit(child_of(late, parent), Some(p));
    assert_eq!(err_kind(&h.task(late).unwrap()), Some(Kind::Budget));
    // The links and the cancellation expire with the run.
    h.raw(Body::RunFinish(pb::RunFinish { run: run.to_vec(), state: 0 }), V);
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    h.raw(Body::Gc(pb::Gc { max_keys: 1000 }), V);
    assert!(h.a.scan_prefix(&keys::pairs_of_run(V, keys::REMOTE_CHILD, &run), 10).is_empty());
}

#[test]
fn a_run_spread_over_another_vshard_admits_calls_there_and_its_copy_ends_and_expires_there() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let run = run_id(V);
    let rec = |spread: Vec<u32>| pb::RunRecord { run: run.to_vec(), tenant: "t".into(), spread, ..Default::default() };
    let copy = |h: &mut Harness, spread| h.raw(Body::RunCreate(pb::RunCreate { run: Some(rec(spread)) }), V2);
    let in_v2 = Tk { run, task: Id::derive(0, V2, &[b"spread", b"1"]) };
    let (ack, _) = h.submit(spec(in_v2, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(status(&ack), Status::Rejected, "no copy there yet: the run is not active in that vshard");
    let refused = copy(&mut h, vec![V as u32]);
    assert!(matches!(refused.reply.body, Some(pb::shard_reply::Body::Rejected(_))), "a vshard the run does not name");
    copy(&mut h, vec![V as u32, V2 as u32]);
    let (ack, _) = h.submit(spec(in_v2, &[], pb::Effects::None, 3), None);
    assert_eq!(status(&ack), Status::Accepted);
    // The copy is not a run of its own: listings show the run once.
    let listed = |h: &Harness, v| read::runs(&h.a, v, 100).iter().filter(|r| r.run == run.to_vec()).count();
    assert_eq!((listed(&h, V), listed(&h, V2)), (1, 0));
    assert_eq!(read::run_in(&h.a, V2, run).map(|r| r.tenant), Some("t".to_string()));
    let page = read::tasks_page(&h.a, run, Some(read::page_start(run, V2)), 10);
    assert_eq!(page.iter().map(|(tk, _)| *tk).collect::<Vec<_>>(), vec![in_v2]);
    assert!(read::tasks_page(&h.a, run, None, 10).iter().all(|(tk, _)| tk.v() == V), "the run's own vshard alone");
    // Ending the copy cancels the calls it admitted and leaves the run's own record.
    let end = h.raw(Body::CancelRun(pb::CancelRun { run: run.to_vec(), message: String::new() }), V2);
    assert!(end.events.contains(&Event::RunEnded(run)));
    assert_eq!(err_kind(&h.task(in_v2).unwrap()), Some(Kind::Cancelled));
    assert_eq!(read::run(&h.a, run).unwrap().state(), pb::run_record::State::Active);
    // The copy expires with its calls; the run's own record stays until it ends.
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    let gc = h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V2);
    assert!(gc.events.contains(&Event::RunExpired(run)));
    assert!(h.task(in_v2).is_none() && read::run_in(&h.a, V2, run).is_none());
    assert!(read::run(&h.a, run).is_some());
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
                            forward_to: None,
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
                    Event::Subscribe { producer, subscriber_vshard, hold } => pending.push(Pending {
                        v: producer.v(),
                        body: Body::Subscribe(pb::Subscribe {
                            producer: Some(producer.pb()),
                            subscriber_vshard: *subscriber_vshard as u32,
                            hold: *hold,
                        }),
                        ack_seq: None,
                    }),
                    Event::Outbox { seq, target_vshard } => {
                        let entry: pb::OutboxEntry = load(&h.a, &keys::outbox(*seq)).expect("outbox entry exists");
                        let body = match (entry.notify, entry.unsubscribe, entry.cancel_children) {
                            (Some(n), _, _) => Body::DepNotify(n),
                            (None, Some(u), _) => Body::Unsubscribe(u),
                            (None, None, c) => {
                                Body::CancelChildren(c.expect("an outbox entry carries one of the three"))
                            }
                        };
                        pending.push(Pending { v: *target_vshard, body, ack_seq: Some(*seq) });
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

// ---------------------------------------------------------------------------
// Forwarding and subtree cancellation
// ---------------------------------------------------------------------------

fn forward(h: &mut Harness, tk: Tk, attempt: u32, p: PeerInc, to: Tk) -> pb::TaskAck {
    let r = h.raw(
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(tk.pb()),
                attempt,
                peer: Some(peer_pb(p)),
                outcome: None,
                forward_to: Some(to.pb()),
            }],
        }),
        tk.v(),
    );
    first_ack(&r.reply)
}

fn child_of(tk: Tk, parent: Tk) -> pb::TaskSpec {
    pb::TaskSpec { parent: Some(parent.pb()), ..spec(tk, &[], pb::Effects::None, 3) }
}

#[test]
fn a_forwarded_call_waits_without_a_worker_and_takes_the_target_outcome() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, f, b) = (task_id(V, 0), task_id(V, 1), task_id(V, 2));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(f, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[a], pb::Effects::None, 3), None);
    assert_eq!(status(&forward(&mut h, a, 1, p, f)), Status::Published);
    let rec = h.task(a).unwrap();
    assert_eq!((rec.state(), rec.forwarded_to.clone()), (TState::Pending, Some(f.pb())));
    let grants = h.a.scan_prefix(&keys::grants_of(V, &p.0, p.1), usize::MAX);
    assert!(grants.iter().all(|(k, _)| !k.windows(16).any(|w| w == a.task.0)), "the waiting forwarder holds no grant");
    // A late publication from the same attempt is refused: the call is waiting, not running.
    assert_eq!(status(&h.publish(a, 1, p, obj(9))), Status::Stale);
    let done = h.raw(
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(f.pb()),
                attempt: 1,
                peer: Some(peer_pb(p)),
                outcome: Some(obj(7)),
                forward_to: None,
            }],
        }),
        V,
    );
    let f_outcome = h.task(f).unwrap().outcome;
    assert_eq!(h.task(a).unwrap().outcome, f_outcome, "the forwarder resolves to the target's result");
    assert!(done.events.iter().any(|e| matches!(e, Event::Done { task, .. } if *task == a)));
    assert_eq!(h.task(b).unwrap().state(), TState::Ready, "dependents of the forwarder are released");
    assert!(read::pins(&h.a, f) >= 2, "the target stays pinned for the forwarder");
}

#[test]
fn forwarding_to_a_finished_or_failed_call_completes_at_once() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, f, c, g) = (task_id(V, 0), task_id(V, 1), task_id(V, 2), task_id(V, 3));
    for t in [a, f, c, g] {
        h.submit(spec(t, &[], pb::Effects::None, 3), Some(p));
    }
    h.publish(f, 1, p, obj(3));
    forward(&mut h, a, 1, p, f);
    assert_eq!(h.task(a).unwrap().outcome, h.task(f).unwrap().outcome);
    h.publish(g, 1, p, user_err("KeyError: k"));
    forward(&mut h, c, 1, p, g);
    assert_eq!(err_kind(&h.task(c).unwrap()), Some(Kind::User), "the target's own error, not a causal one");
    assert_eq!(status(&forward(&mut h, task_id(V, 4), 1, p, a)), Status::NotFound);
}

#[test]
fn forwarding_to_a_call_not_yet_submitted_resolves_when_it_finishes() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, f) = (task_id(V, 0), task_id(V, 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    forward(&mut h, a, 1, p, f);
    let (ack, _) = h.submit(spec(f, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(status(&ack), Status::Granted);
    assert_eq!(read::pins(&h.a, f), 2, "the early forwarder pins the target");
    h.publish(f, 1, p, obj(5));
    assert_eq!(h.task(a).unwrap().outcome, h.task(f).unwrap().outcome);
}

#[test]
fn cancelling_a_call_cancels_everything_it_submitted() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (root, c1, c2, g, other) = (task_id(V, 0), task_id(V, 1), task_id(V, 2), task_id(V, 3), task_id(V, 4));
    h.submit(spec(root, &[], pb::Effects::None, 3), Some(p));
    h.submit(child_of(c1, root), Some(p));
    h.submit(child_of(c2, root), None);
    h.submit(child_of(g, c1), Some(p));
    h.submit(spec(other, &[], pb::Effects::None, 3), Some(p));
    h.publish(c1, 1, p, obj(1)); // finished, but its own child is still running
    assert_eq!(status(&h.cancel(root)), Status::Accepted);
    assert_eq!(err_kind(&h.task(root).unwrap()), Some(Kind::Cancelled));
    assert_eq!(err_kind(&h.task(c2).unwrap()), Some(Kind::Cancelled));
    assert_eq!(err_kind(&h.task(g).unwrap()), Some(Kind::Cancelled), "grandchildren under a finished child too");
    assert_eq!(err_kind(&h.task(c1).unwrap()), None, "a finished child keeps its result");
    assert_eq!(h.task(other).unwrap().state(), TState::Running, "unrelated work is untouched");
    // A cancelled body may still submit: its late child is cancelled at once.
    let late = task_id(V, 5);
    let (ack, _) = h.submit(child_of(late, root), Some(p));
    assert_eq!((ack.state(), err_kind(&h.task(late).unwrap())), (TState::Done, Some(Kind::Cancelled)));
}

#[test]
fn child_and_forward_entries_expire_with_their_run() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (root, c, a, f) = (task_id(V, 0), task_id(V, 1), task_id(V, 2), task_id(V, 3));
    h.submit(spec(root, &[], pb::Effects::None, 3), Some(p));
    h.submit(child_of(c, root), Some(p));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    forward(&mut h, a, 1, p, f); // f never arrives
    h.raw(charge(root, 1.0, 0.0, 10.0, 0.0), V);
    h.raw(Body::RunFinish(pb::RunFinish { run: run_id(V).to_vec(), state: 0 }), V);
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    h.raw(Body::Gc(pb::Gc { max_keys: 1000 }), V);
    for tag in [keys::CHILD, keys::FORWARD, keys::BUDGET] {
        assert!(h.a.scan_prefix(&keys::pairs_of_run(V, tag, &run_id(V)), usize::MAX).is_empty(), "tag {tag}");
    }
}

#[test]
fn the_same_producer_passed_twice_is_one_dependency() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (a, b) = (task_id(V, 0), task_id(V, 1));
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.submit(spec(b, &[a, a], pb::Effects::None, 3), None);
    assert_eq!(h.task(b).unwrap().deps_remaining, 1);
    assert_eq!(read::pins(&h.a, a), 2, "the client's pin plus one for the dependent");
    h.publish(a, 1, p, obj(1));
    assert_eq!(h.task(b).unwrap().state(), TState::Ready, "a duplicated dependency must not block forever");
    h.grant(b, p, false);
    h.publish(b, 1, p, obj(2));
    assert_eq!(read::pins(&h.a, a), 1, "unpinned once when the dependent completes");
}

// ---------------------------------------------------------------------------
// ch.Store
// ---------------------------------------------------------------------------

fn store_write(h: &mut Harness, w: pb::StoreWrite) -> pb::StoreReply {
    let v = store::vshard_of(&w.tenant, &w.namespace);
    h.raw(Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v as u32], epoch: EPOCH }), v);
    match h.raw(Body::StoreWrite(w), v).reply.body {
        Some(pb::shard_reply::Body::Store(r)) => r,
        other => panic!("expected a store reply, got {other:?}"),
    }
}

fn store_read(h: &mut Harness, r: pb::StoreRead) -> pb::StoreReply {
    let v = store::vshard_of(&r.tenant, &r.namespace);
    match h.raw(Body::StoreRead(r), v).reply.body {
        Some(pb::shard_reply::Body::Store(r)) => r,
        other => panic!("expected a store reply, got {other:?}"),
    }
}

fn w(op: pb::store_write::Op, key: &str) -> pb::StoreWrite {
    pb::StoreWrite { tenant: "t".into(), namespace: "ns".into(), op: op as i32, key: key.into(), ..Default::default() }
}

fn rd(kind: pb::store_read::Kind, key: &str) -> pb::StoreRead {
    pb::StoreRead {
        tenant: "t".into(),
        namespace: "ns".into(),
        kind: kind as i32,
        key: key.into(),
        ..Default::default()
    }
}

use pb::store_read::Kind as RK;
use pb::store_write::Op as SO;

#[test]
fn store_versions_make_compare_and_set() {
    let mut h = Harness::new(ShardState::new());
    let put = |value: &[u8], if_version: Option<u64>| pb::StoreWrite {
        value: value.to_vec(),
        has_if_version: if_version.is_some(),
        if_version: if_version.unwrap_or(0),
        ..w(SO::Put, "cfg")
    };
    let a = store_write(&mut h, put(b"a", Some(0)));
    assert!(a.ok && a.entry.as_ref().unwrap().version == a.revision);
    assert!(!store_write(&mut h, put(b"x", Some(0))).ok, "create-only must fail when the key exists");
    let b = store_write(&mut h, put(b"b", Some(a.revision)));
    assert!(b.ok && b.revision > a.revision);
    let stale = store_write(&mut h, put(b"c", Some(a.revision)));
    assert!(!stale.ok && stale.error.contains("version mismatch"));
    assert_eq!(store_read(&mut h, rd(RK::Get, "cfg")).entry.unwrap().value, b"b");
    let del = pb::StoreWrite { has_if_version: true, if_version: a.revision, ..w(SO::Delete, "cfg") };
    assert!(!store_write(&mut h, del).ok);
    assert!(store_write(&mut h, w(SO::Delete, "cfg")).ok);
    assert!(!store_read(&mut h, rd(RK::Get, "cfg")).ok);
    let big = pb::StoreWrite { value: vec![0; store::MAX_VALUE + 1], ..w(SO::Put, "big") };
    assert!(store_write(&mut h, big).error.contains("1 MiB"));
}

#[test]
fn store_counters_add_and_refuse_plain_values() {
    let mut h = Harness::new(ShardState::new());
    for _ in 0..3 {
        store_write(&mut h, pb::StoreWrite { delta: 5, ..w(SO::Add, "seen") });
    }
    assert_eq!(store_read(&mut h, rd(RK::Get, "seen")).entry.unwrap().counter, 15);
    store_write(&mut h, pb::StoreWrite { value: b"v".to_vec(), ..w(SO::Put, "plain") });
    assert!(store_write(&mut h, pb::StoreWrite { delta: 1, ..w(SO::Add, "plain") }).error.contains("not a counter"));
}

#[test]
fn store_leases_fence_a_holder_whose_lease_expired() {
    let mut h = Harness::new(ShardState::new());
    let lock = |holder: &str| pb::StoreWrite { holder: holder.into(), ttl_ms: 1000, ..w(SO::Lock, "leader") };
    let a = store_write(&mut h, lock("a"));
    let token_a = a.lease.as_ref().unwrap().token;
    let refused = store_write(&mut h, lock("b"));
    assert!(!refused.ok && refused.lease.unwrap().holder == "a");
    let fenced = |token: u64| pb::StoreWrite {
        value: b"x".to_vec(),
        fence: Some(pb::StoreFence { lock: "leader".into(), token }),
        ..w(SO::Put, "state")
    };
    assert!(store_write(&mut h, fenced(token_a)).ok, "the holder writes under its token");
    assert!(
        store_write(
            &mut h,
            pb::StoreWrite { holder: "a".into(), token: token_a, ttl_ms: 1000, ..w(SO::Renew, "leader") }
        )
        .ok
    );
    // a stalls past its lease; b takes over with a larger token.
    h.hlc += hlc::from_ms(5_000);
    let b = store_write(&mut h, lock("b"));
    let token_b = b.lease.as_ref().unwrap().token;
    assert!(b.ok && token_b > token_a);
    let stale = store_write(&mut h, fenced(token_a));
    assert!(!stale.ok && stale.error.starts_with("fenced"), "{}", stale.error);
    assert!(
        !store_write(
            &mut h,
            pb::StoreWrite { holder: "a".into(), token: token_a, ttl_ms: 1000, ..w(SO::Renew, "leader") }
        )
        .ok
    );
    assert_eq!(store_read(&mut h, rd(RK::Lease, "leader")).lease.unwrap().holder, "b");
    assert!(store_write(&mut h, pb::StoreWrite { holder: "b".into(), token: token_b, ..w(SO::Unlock, "leader") }).ok);
    assert!(!store_read(&mut h, rd(RK::Lease, "leader")).ok);
}

#[test]
fn store_barrier_trips_when_every_party_arrives() {
    let mut h = Harness::new(ShardState::new());
    let arrive = || pb::StoreWrite { parties: 3, ..w(SO::Arrive, "epoch") };
    for expected in [(1, 0), (2, 0), (0, 1)] {
        let b = store_write(&mut h, arrive()).barrier.unwrap();
        assert_eq!((b.arrived, b.generation), expected);
    }
    assert!(store_write(&mut h, pb::StoreWrite { parties: 2, ..w(SO::Arrive, "epoch") }).error.contains("3 parties"));
}

#[test]
fn store_changes_serve_watches_and_report_compaction() {
    let mut h = Harness::new(ShardState::new());
    let first = store_write(&mut h, pb::StoreWrite { value: b"1".to_vec(), ..w(SO::Put, "jobs/a") });
    store_write(&mut h, pb::StoreWrite { value: b"2".to_vec(), ..w(SO::Put, "other") });
    store_write(&mut h, w(SO::Delete, "jobs/a"));
    let ch = store_read(&mut h, pb::StoreRead { after_revision: first.revision - 1, ..rd(RK::Changes, "jobs/") });
    let seen: Vec<(String, bool)> = ch.changes.iter().map(|c| (c.key.clone(), c.deleted)).collect();
    assert_eq!(seen, [("jobs/a".to_string(), false), ("jobs/a".to_string(), true)]);
    assert!(!ch.compacted);
    for i in 0..4100 {
        store_write(&mut h, pb::StoreWrite { value: vec![1], ..w(SO::Put, &format!("k{i}")) });
    }
    assert!(store_read(&mut h, pb::StoreRead { after_revision: 0, ..rd(RK::Changes, "") }).compacted);
    let scan = store_read(&mut h, pb::StoreRead { limit: 10, ..rd(RK::Scan, "k1") });
    assert!(scan.more && scan.entries.len() == 10 && scan.entries.iter().all(|e| e.key.starts_with("k1")));
}

#[test]
fn store_ttl_entries_vanish_and_are_collected() {
    let mut h = Harness::new(ShardState::new());
    store_write(&mut h, pb::StoreWrite { value: b"t".to_vec(), ttl_ms: 1000, ..w(SO::Put, "session") });
    assert!(store_read(&mut h, rd(RK::Get, "session")).ok);
    let v = store::vshard_of("t", "ns");
    let stored = |h: &Harness| h.a.scan_prefix(&keys::store(v, keys::STORE, &[], b""), usize::MAX).len();
    h.hlc += hlc::from_ms(2_000);
    assert!(!store_read(&mut h, rd(RK::Get, "session")).ok, "an expired entry is not visible");
    assert_eq!(stored(&h), 1, "still stored until collected");
    h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V);
    assert_eq!(stored(&h), 0, "gc removes the expired entry");
}

#[test]
fn a_requeued_attempt_is_ready_again_without_spending_a_retry() {
    let mut h = Harness::new(ShardState::new());
    let tk = task_id(V, 900);
    let (a, _) = h.submit(spec(tk, &[], pb::Effects::None, 1), Some((peer(1), 1)));
    assert_eq!(status(&a), Status::Granted);
    for round in 0..3 {
        let attempt = h.task(tk).unwrap().attempts.iter().rev().find(|x| !x.revoked).map(|x| x.attempt).unwrap();
        let ack = h.fail(tk, attempt, (peer(1), 1), pb::record_failure::Kind::Requeued, b"");
        assert_eq!(status(&ack), Status::Accepted, "round {round}");
        let rec = h.task(tk).unwrap();
        assert_eq!(rec.state(), TState::Ready, "given back, not failed");
        assert_eq!((rec.infra_failures, rec.failure_history.len()), (0, 0));
        assert_eq!(status(&h.grant(tk, (peer(2), 1), false)), Status::Granted);
        if round < 2 {
            // Back to peer 1 for the next round.
            let a2 = h.task(tk).unwrap().attempts.last().unwrap().attempt;
            h.fail(tk, a2, (peer(2), 1), pb::record_failure::Kind::Requeued, b"");
            assert_eq!(status(&h.grant(tk, (peer(1), 1), false)), Status::Granted);
        }
    }
    // max_attempts = 1 and five give-backs later it can still finish.
    let last = h.task(tk).unwrap().attempts.last().unwrap().attempt;
    assert_eq!(status(&h.publish(tk, last, (peer(2), 1), obj(7))), Status::Published);
}

#[test]
fn a_started_external_attempt_cannot_be_given_back() {
    let mut h = Harness::new(ShardState::new());
    let tk = task_id(V, 901);
    h.submit(spec(tk, &[], pb::Effects::External, 3), Some((peer(1), 1)));
    let attempt = h.task(tk).unwrap().attempts[0].attempt;
    assert_eq!(status(&h.start(tk, attempt, (peer(1), 1))), Status::Accepted);
    let ack = h.fail(tk, attempt, (peer(1), 1), pb::record_failure::Kind::Requeued, b"");
    assert_eq!(status(&ack), Status::Stale);
    assert_eq!(h.task(tk).unwrap().state(), TState::Running);
}

fn charge(root: Tk, cpu: f64, gpu: f64, limit_cpu: f64, limit_gpu: f64) -> Body {
    Body::Charge(pb::Charge {
        root: Some(root.pb()),
        cpu_seconds: cpu,
        gpu_seconds: gpu,
        limit_cpu_seconds: limit_cpu,
        limit_gpu_seconds: limit_gpu,
    })
}

#[test]
fn a_spent_budget_cancels_its_call_and_the_calls_it_made() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (root, child, grandchild, other) = (task_id(V, 0), task_id(V, 1), task_id(V, 2), task_id(V, 3));
    h.submit(spec(root, &[], pb::Effects::None, 3), Some(p));
    h.submit(child_of(child, root), Some(p));
    h.submit(child_of(grandchild, child), Some(p));
    h.submit(spec(other, &[], pb::Effects::None, 3), Some(p));
    assert_eq!(status(&first_ack(&h.raw(charge(root, 6.0, 0.0, 10.0, 0.0), V).reply)), Status::Accepted);
    h.raw(charge(root, 0.0, 500.0, 10.0, 0.0), V); // no GPU limit: GPU seconds alone never cancel
    assert_eq!(err_kind(&h.task(root).unwrap()), None, "under its limit the call runs on");
    h.raw(charge(root, 4.5, 0.0, 10.0, 0.0), V); // 10.5 > 10
    for t in [root, child, grandchild] {
        assert_eq!(err_kind(&h.task(t).unwrap()), Some(Kind::Budget), "{t:?}");
    }
    let Some(pb::outcome::Value::Err(e)) = h.task(root).unwrap().outcome.and_then(|o| o.value) else { panic!() };
    assert_eq!(e.message, "budget exceeded: cpu_seconds=10 allowed, 10.50 used by this call and the calls it made");
    assert_eq!(h.task(other).unwrap().state(), TState::Running, "unrelated work is untouched");
    // Later charges for the finished call are dropped, and so are its totals.
    assert_eq!(status(&first_ack(&h.raw(charge(root, 1.0, 0.0, 10.0, 0.0), V).reply)), Status::Existing);
    assert!(h.a.get(&keys::of_producer(V, keys::BUDGET, root.pair())).is_none());
    // A GPU limit is checked the same way.
    let gpu_root = task_id(V, 4);
    h.submit(spec(gpu_root, &[], pb::Effects::None, 3), Some(p));
    h.raw(charge(gpu_root, 0.0, 3601.0, 0.0, 3600.0), V);
    assert_eq!(err_kind(&h.task(gpu_root).unwrap()), Some(Kind::Budget));
}
