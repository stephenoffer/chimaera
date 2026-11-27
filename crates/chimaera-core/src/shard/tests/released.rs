//! What a released call's record keeps (shard::released).

use super::*;

/// An outcome as a peer publishes one: a small result inline, with the
/// object's id, run, checksum and holder.
fn published(tag: u8) -> pb::Outcome {
    pb::Outcome {
        value: Some(pb::outcome::Value::Ok(pb::ObjectRef {
            object_id: vec![tag; 32],
            size: 24,
            inline_data: vec![tag; 24],
            locations: vec![peer_pb((peer(0), 1))],
            checksum: vec![tag; 16],
            run: run_id(V).to_vec(),
            ..Default::default()
        })),
        attempt: 1,
        peer: Some(peer_pb((peer(0), 1))),
    }
}

fn release(h: &mut Harness, tasks: &[Tk]) -> Applied {
    h.raw(Body::Release(pb::Release { tasks: tasks.iter().map(Tk::pb).collect() }), V)
}

/// The value a finished call's record holds, if it succeeded.
fn value(h: &Harness, tk: Tk) -> Option<pb::ObjectRef> {
    match h.task(tk)?.outcome?.value? {
        pb::outcome::Value::Ok(obj) => Some(obj),
        pb::outcome::Value::Err(_) => None,
    }
}

fn has_pins_key(h: &Harness, tk: Tk) -> bool {
    h.a.get(&pins_key(tk)).is_some()
}

#[test]
fn a_released_call_that_succeeded_keeps_only_that_it_did() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let a = task_id(V, 0);
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.publish(a, 1, p, published(1));
    let r = release(&mut h, &[a]);
    let freed = r.events.iter().find_map(|e| match e {
        Event::Releasable { task, object } if *task == a => object.clone(),
        _ => None,
    });
    assert_eq!(freed.map(|o| o.object_id), Some(vec![1; 32]), "the peer still frees the value");
    let rec = h.task(a).unwrap();
    assert_eq!(rec.state(), TState::Done);
    assert_eq!(value(&h, a), Some(pb::ObjectRef::default()), "a success, without the freed value's reference");
    assert_eq!(rec.outcome.as_ref().map(|o| o.attempt), Some(1), "and which attempt it was");
    assert!(rec.encoded_len() <= 16, "{} bytes", rec.encoded_len());
    assert!(!has_pins_key(&h, a), "a finished call's count of 0 is read from its record");
    assert_eq!(read::pins(&h.a, a), 0);
    // The same id again: answered as finished, never run twice.
    let (ack, ev) = h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    assert_ne!(status(&ack), Status::Granted);
    assert_eq!(ack.state, TState::Done as i32);
    assert!(!ev.iter().any(|e| matches!(e, Event::Granted { .. } | Event::Ready(_))));
}

#[test]
fn calls_that_may_be_asked_for_again_keep_their_outcome_whole() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let detached = Id::derive(0, V, &[b"detached run"]);
    h.raw(
        Body::RunCreate(pb::RunCreate {
            run: Some(pb::RunRecord {
                run: detached.to_vec(),
                tenant: "t".into(),
                detached: true,
                ..Default::default()
            }),
        }),
        V,
    );
    let mut cases = Vec::new();
    let mut stream = spec(task_id(V, 0), &[], pb::Effects::None, 3);
    stream.stream = true;
    cases.push((stream, published(1)));
    let mut step = spec(task_id(V, 1), &[], pb::Effects::None, 3);
    step.workflow_step = r#"{"step": 1}"#.into();
    cases.push((step, published(2)));
    let mut command = spec(task_id(V, 2), &[], pb::Effects::None, 3);
    command.command_fingerprint = vec![9; 8];
    cases.push((command, published(3)));
    let in_detached = Tk { run: detached, task: detached.child(&[b"call"]) };
    cases.push((spec(in_detached, &[], pb::Effects::None, 3), published(4)));
    cases.push((spec(task_id(V, 4), &[], pb::Effects::None, 3), user_err("ValueError: bad row")));
    for (s, outcome) in cases {
        let tk = Tk::parse(s.key.as_ref().unwrap()).unwrap();
        h.submit(s, Some(p));
        h.publish(tk, 1, p, outcome);
        let recorded = h.task(tk).unwrap().outcome;
        release(&mut h, &[tk]);
        let rec = h.task(tk).unwrap();
        assert_eq!(rec.state(), TState::Done, "{tk:?}");
        assert_eq!(rec.outcome, recorded, "{tk:?} keeps its outcome whole");
    }
}

#[test]
fn a_call_whose_run_is_not_known_here_keeps_its_outcome_whole() {
    // Calls are admitted only where a copy of their run is: should one ever
    // be missing, the call is kept as if its run were detached.
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let a = task_id(V, 0);
    h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    h.publish(a, 1, p, published(1));
    let recorded = h.task(a).unwrap().outcome;
    for kv in [&mut h.a, &mut h.b] {
        kv.0.remove(&keys::run(V, &run_id(V)));
    }
    release(&mut h, &[a]);
    assert_eq!(h.task(a).unwrap().outcome, recorded);
}

/// A record and its keys, with the clock's stamps zeroed: two histories that
/// apply the same commands in another order differ only in those.
fn shape(h: &Harness, tk: Tk) -> (Option<pb::TaskRecord>, Vec<Vec<u8>>) {
    let rec = h.task(tk).map(|mut r| {
        (r.submitted_hlc, r.done_hlc) = (0, 0);
        r
    });
    (rec, h.a.0.keys().cloned().collect())
}

#[test]
fn releasing_before_or_after_a_call_ends_leaves_the_same_record() {
    let p = (peer(0), 1);
    let a = task_id(V, 0);
    let mut before = Harness::new(ShardState::new());
    before.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    release(&mut before, &[a]);
    before.publish(a, 1, p, published(1));
    let mut after = Harness::new(ShardState::new());
    after.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
    after.publish(a, 1, p, published(1));
    release(&mut after, &[a]);
    assert_eq!(shape(&before, a), shape(&after, a));
    assert_eq!(value(&before, a), Some(pb::ObjectRef::default()));
}

/// A reply with each acknowledged success's value left out: what a released
/// call's record no longer keeps.
fn answered(mut r: pb::ShardReply) -> pb::ShardReply {
    if let Some(pb::shard_reply::Body::Tasks(t)) = r.body.as_mut() {
        for ack in &mut t.acks {
            if let Some(o) = ack.outcome.as_mut().filter(|o| matches!(o.value, Some(pb::outcome::Value::Ok(_)))) {
                *o = crate::shard::released::succeeded_only(o);
            }
        }
    }
    r
}

#[test]
fn later_commands_are_answered_alike_whether_or_not_the_call_was_released() {
    let p = (peer(0), 1);
    let (a, other) = (task_id(V, 0), task_id(V, 1));
    let history = |released: bool| {
        let mut h = Harness::new(ShardState::new());
        h.submit(spec(a, &[], pb::Effects::None, 3), Some(p));
        h.submit(spec(other, &[], pb::Effects::None, 3), Some(p));
        h.publish(a, 1, p, published(1));
        release(&mut h, &[if released { a } else { other }]); // one command either way: the clocks agree
        h
    };
    let (mut gone, mut held) = (history(true), history(false));
    let later = [
        Body::SubmitTasks(pb::SubmitTasks { tasks: vec![spec(a, &[], pb::Effects::None, 3)], grant_to: None }),
        Body::Grant(pb::GrantAttempts {
            grants: vec![pb::Grant { task: Some(a.pb()), peer: Some(peer_pb((peer(1), 1))), speculative: true }],
        }),
        Body::Publish(pb::PublishResults {
            entries: vec![pb::PublishEntry {
                task: Some(a.pb()),
                attempt: 2,
                peer: Some(peer_pb((peer(1), 1))),
                outcome: Some(published(2)),
                forward_to: None,
            }],
        }),
        Body::Cancel(pb::Cancel { task: Some(a.pb()), reason: Kind::Cancelled as i32, message: String::new() }),
        Body::Subscribe(pb::Subscribe { producer: Some(a.pb()), subscriber_vshard: V2 as u32, hold: 1 }),
        Body::RunFinish(pb::RunFinish { run: run_id(V).to_vec(), state: 0 }),
    ];
    for body in later {
        let what = format!("{body:?}");
        let (x, y) = (gone.raw(body.clone(), V), held.raw(body, V));
        assert_eq!(answered(x.reply), answered(y.reply), "{what}");
    }
}

#[test]
fn a_failed_call_its_caller_released_can_still_run_again_from_its_dead_letter() {
    let mut h = Harness::new(ShardState::new());
    let p = (peer(0), 1);
    let (input, failed, retry) = (task_id(V, 0), task_id(V, 1), task_id(V, 2));
    h.submit(spec(input, &[], pb::Effects::None, 3), Some(p));
    h.publish(input, 1, p, published(1));
    h.submit(spec(failed, &[input], pb::Effects::None, 3), Some(p));
    h.publish(failed, 1, p, user_err("ValueError: bad row"));
    release(&mut h, &[input, failed]);
    assert_eq!(read::pins(&h.a, input), 1, "the dead letter's");
    assert!(has_pins_key(&h, input));
    assert_eq!(value(&h, input).map(|o| o.object_id), Some(vec![1; 32]), "an input still held keeps its value");
    assert!(err_kind(&h.task(failed).unwrap()).is_some(), "a failure keeps its error");
    // Run.retry_failed: a new call with the letter's spec, then the letter goes.
    let (_, letter) = read::dead_letters(&h.a, V, run_id(V)).pop().unwrap();
    let mut again = letter.spec.unwrap();
    again.key = Some(retry.pb());
    let (ack, _) = h.submit(again, Some(p));
    assert_eq!(status(&ack), Status::Granted, "its input is ready, with its value");
    h.raw(Body::DiscardDead(pb::Release { tasks: vec![failed.pb()] }), V);
    assert_eq!(read::pins(&h.a, input), 1, "the retry's");
    assert_eq!(status(&h.publish(retry, 1, p, published(3))), Status::Published);
    release(&mut h, &[retry]);
    assert_eq!(read::pins(&h.a, input), 0, "the retry finished: nothing holds the input");
    assert_eq!(value(&h, input), Some(pb::ObjectRef::default()));
    assert!(!has_pins_key(&h, input));
}
