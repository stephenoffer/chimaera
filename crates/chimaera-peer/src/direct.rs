//! The direct actor path's bookkeeping on the peer (docs/architecture.md, D2).
//!
//! On a local peer a driver sends an eligible actor call straight to the
//! actor's executor and gets the outcome back on the same socket. Around that:
//! - As it sends the call, the driver submits a copy with `TaskSpec.direct`
//!   set. The peer only notes it as in flight at its actor (`Inflight`), so
//!   `explain()` and deadlock messages can name it. It never reaches a shard.
//! - Once the outcome is in, the driver submits the call with `TaskSpec.ran`
//!   set. The shard records it as done (shard/mod.rs, `submit`), and this
//!   module turns the executor's timings into the attempt `explain()` shows
//!   and the sample the profiles learn from, as `record_attempt` and `learn`
//!   do for calls the peer ran itself.
//! - A call whose connection broke before its outcome came back, and that may
//!   have begun, is never run again: its caller records it with `ran` set to
//!   `ActorDied` or `OutcomeUnknown` and no timings (python/chimaera/_direct.py,
//!   `Conn._settle`). That record files no attempt: nothing saw it run.
//! - The peer leases its executors the right to begin direct calls, only
//!   while it hears most of its cluster (`lease`).

pub mod lease;

use crate::profiles::FnKey;
use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::shard::Tk;
use prost::Message;
use std::collections::{BTreeSet, HashMap, VecDeque};

/// Calls noted in flight at most; more are not tracked (explain() then says
/// "unknown task" for them until their record lands).
const INFLIGHT_MAX: usize = 65_536;

/// A direct call as its driver announced it.
pub struct Call {
    pub actor: Id,
    /// module.Class.method, short: Class.method.
    pub name: String,
    pub method: String,
    pub key: FnKey,
}

/// Calls sent straight to actors' executors whose records have not landed.
#[derive(Default)]
pub struct Inflight {
    calls: HashMap<Tk, Call>,
    order: HashMap<Id, VecDeque<Tk>>, // per actor, in the order they were sent
}

impl Inflight {
    pub fn note(&mut self, calls: Vec<(Tk, Call)>) {
        for (tk, call) in calls {
            if self.calls.len() >= INFLIGHT_MAX {
                return;
            }
            self.order.entry(call.actor).or_default().push_back(tk);
            self.calls.insert(tk, call);
        }
    }

    /// Its record landed, or it finished through the peer after all.
    pub fn done(&mut self, tk: Tk) {
        let Some(call) = self.calls.remove(&tk) else { return };
        if let Some(q) = self.order.get_mut(&call.actor) {
            if q.front() == Some(&tk) {
                q.pop_front();
            } else {
                q.retain(|t| *t != tk);
            }
            if q.is_empty() {
                self.order.remove(&call.actor);
            }
        }
    }

    /// The run ended: its drivers will not record what is left.
    pub fn end_run(&mut self, run: Id) {
        let gone: Vec<Tk> = self.calls.keys().filter(|tk| tk.run == run).copied().collect();
        for tk in gone {
            self.done(tk);
        }
    }

    pub fn get(&self, tk: &Tk) -> Option<&Call> {
        self.calls.get(tk)
    }

    /// How many of its actor's direct calls were sent before it and are not done.
    pub fn ahead(&self, tk: Tk) -> Option<usize> {
        let call = self.calls.get(&tk)?;
        self.order.get(&call.actor)?.iter().position(|t| *t == tk)
    }
}

/// Take the in-flight notices out of `s` (they are never submitted).
pub fn inflight(s: &mut pb::SubmitTasks) -> Vec<(Tk, Call)> {
    if !s.tasks.iter().any(|t| t.direct) {
        return Vec::new();
    }
    let (notes, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut s.tasks).into_iter().partition(|t| t.direct);
    s.tasks = rest;
    notes
        .into_iter()
        .filter_map(|spec| {
            let tk = spec.key.as_ref().and_then(Tk::parse)?;
            let actor = Id::from_slice(&spec.actor).ok()?;
            let policy = pb::Policy::decode(spec.policy.as_slice()).unwrap_or_default();
            let parts: Vec<&str> = policy.fn_name.rsplit('.').take(2).collect();
            let name = parts.into_iter().rev().collect::<Vec<_>>().join(".");
            let key = crate::core::profile_key(&spec, &policy);
            Some((tk, Call { actor, name, method: spec.method, key }))
        })
        .collect()
}

/// A call the direct path ran, for the attempt log and the profiles.
pub struct Ran {
    pub tk: Tk,
    pub attempt: pb::AttemptRecord,
    pub key: FnKey,
    /// The call's arguments and result, in bytes.
    pub arg_bytes: u64,
    pub out_bytes: u64,
}

/// The calls in `s` recorded with their outcome, which their caller has:
/// the session is not sent it again (one event and one read a call).
pub fn recorded(s: &pb::SubmitTasks) -> BTreeSet<Tk> {
    s.tasks.iter().filter(|t| t.ran.is_some()).filter_map(|t| t.key.as_ref().and_then(Tk::parse)).collect()
}

/// The calls in `s` that the direct path ran.
pub fn attempts(s: &pb::SubmitTasks, peer: &pb::PeerInc) -> Vec<Ran> {
    s.tasks
        .iter()
        .filter_map(|spec| {
            let tk = spec.key.as_ref().and_then(Tk::parse)?;
            let attempt = attempt(spec, peer)?;
            let policy = attempt.granted.clone().unwrap_or_default();
            let out_bytes = match spec.ran.as_ref()?.outcome.as_ref().and_then(|o| o.value.as_ref()) {
                Some(pb::outcome::Value::Ok(obj)) => obj.size + obj.blobs.iter().map(|b| b.size).sum::<u64>(),
                _ => 0,
            };
            let arg_bytes = spec.args.as_ref().map_or(0, |a| a.size) + spec.blobs.iter().map(|b| b.size).sum::<u64>();
            Some(Ran { tk, key: crate::core::profile_key(spec, &policy), attempt, arg_bytes, out_bytes })
        })
        .collect()
}

fn attempt(spec: &pb::TaskSpec, peer: &pb::PeerInc) -> Option<pb::AttemptRecord> {
    let ran = spec.ran.as_ref().filter(|r| r.entry_unix_ns != 0)?; // 0: settled by its caller, not seen to run
    let wall_s = ran.exit_unix_ns.saturating_sub(ran.entry_unix_ns) as f64 / 1e9;
    let cpu_s = ran.cpu_ns as f64 / 1e9;
    let m = |value: f64, scope: &str, method: &str| pb::Measure {
        value,
        scope: scope.into(),
        method: method.into(),
        coverage: 1.0,
    };
    let mut measures = HashMap::new();
    measures.insert("wall_seconds".into(), m(wall_s, "attempt", "clock"));
    measures.insert("cpu_seconds".into(), m(cpu_s, "process", "process_time"));
    if wall_s > 0.0 {
        measures.insert("mean_cores".into(), m(cpu_s / wall_s, "process", "process_time"));
    }
    let end_reason = match ran.outcome.as_ref().and_then(|o| o.value.as_ref()) {
        Some(pb::outcome::Value::Ok(_)) => "ok",
        Some(pb::outcome::Value::Err(_)) => "user_error",
        None => "unknown",
    };
    Some(pb::AttemptRecord {
        task: spec.key.clone(),
        attempt: 1,
        peer: Some(peer.clone()),
        executor: ran.executor,
        fn_name: spec.method.clone(),
        dispatched_unix_ns: ran.entry_unix_ns,
        py_entry_unix_ns: ran.entry_unix_ns,
        py_exit_unix_ns: ran.exit_unix_ns,
        granted: pb::Policy::decode(spec.policy.as_slice()).ok(),
        measures,
        end_reason: end_reason.into(),
        placement_reason: match ran.actor_generation {
            0 => "direct: the caller sent it to the actor's executor on this machine".into(),
            g => format!("direct: the caller sent it to the actor's executor on this machine (instance {g})"),
        },
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimaera_core::ids::Id;

    #[test]
    fn only_calls_the_direct_path_ran_get_an_attempt_with_its_timings() {
        let run = Id::derive(0, 3, &[b"run"]);
        let key = |i: u64| Tk { run, task: run.child(&[&i.to_le_bytes()]) }.pb();
        let ok = pb::Outcome { value: Some(pb::outcome::Value::Ok(Default::default())), ..Default::default() };
        let ran = pb::Ran {
            outcome: Some(ok),
            entry_unix_ns: 1_000,
            exit_unix_ns: 3_001_000,
            cpu_ns: 1_500_000,
            executor: 7,
            actor_generation: 1,
        };
        let s = pb::SubmitTasks {
            tasks: vec![
                pb::TaskSpec { key: Some(key(1)), method: "add".into(), ran: Some(ran), ..Default::default() },
                pb::TaskSpec { key: Some(key(2)), ..Default::default() },
            ],
            grant_to: None,
        };
        let got = attempts(&s, &pb::PeerInc::default());
        assert_eq!(got.len(), 1, "a call the peer will run gets its attempt from the peer");
        let rec = &got[0].attempt;
        assert_eq!((rec.fn_name.as_str(), rec.executor, rec.end_reason.as_str()), ("add", 7, "ok"));
        assert!(rec.placement_reason.ends_with("(instance 1)"), "{}", rec.placement_reason);
        assert!((rec.measures["wall_seconds"].value - 0.003).abs() < 1e-9);
        assert!((rec.measures["mean_cores"].value - 0.5).abs() < 1e-9);
    }

    #[test]
    fn a_call_its_caller_settled_without_seeing_it_run_files_no_attempt() {
        let run = Id::derive(0, 3, &[b"run"]);
        let tk = Tk { run, task: run.child(&[b"1"]) };
        let died = pb::ErrorRecord { kind: pb::error_record::Kind::ActorDied as i32, ..Default::default() };
        let outcome = pb::Outcome { value: Some(pb::outcome::Value::Err(died)), ..Default::default() };
        let ran = pb::Ran { outcome: Some(outcome), ..Default::default() };
        let s = pb::SubmitTasks {
            tasks: vec![pb::TaskSpec { key: Some(tk.pb()), ran: Some(ran), ..Default::default() }],
            grant_to: None,
        };
        assert!(attempts(&s, &pb::PeerInc::default()).is_empty(), "no timings: nothing saw it run");
        assert_eq!(recorded(&s), BTreeSet::from([tk]), "its caller has the outcome: no event for it");
    }

    #[test]
    fn in_flight_calls_queue_per_actor_and_leave_when_done_or_their_run_ends() {
        let (run, other) = (Id::derive(0, 3, &[b"run"]), Id::derive(0, 4, &[b"other"]));
        let actor = Id::derive(0, 3, &[b"actor"]);
        let tk = |r: Id, i: u64| Tk { run: r, task: r.child(&[&i.to_le_bytes()]) };
        let call = || Call { actor, name: "A.m".into(), method: "m".into(), key: FnKey::new(b"d", "m") };
        let mut f = Inflight::default();
        f.note(vec![(tk(run, 1), call()), (tk(run, 2), call()), (tk(other, 3), call())]);
        assert_eq!((f.ahead(tk(run, 1)), f.ahead(tk(run, 2)), f.ahead(tk(other, 3))), (Some(0), Some(1), Some(2)));
        f.done(tk(run, 2));
        assert_eq!(f.ahead(tk(other, 3)), Some(1), "a call that finished out of order no longer counts");
        f.end_run(run);
        assert!(f.get(&tk(run, 1)).is_none());
        assert_eq!(f.ahead(tk(other, 3)), Some(0));
        f.done(tk(other, 3));
        assert!(f.order.is_empty(), "nothing is kept for an actor with no calls in flight");
    }
}
