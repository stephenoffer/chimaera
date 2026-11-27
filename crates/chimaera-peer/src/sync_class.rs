//! Which proposals may be acknowledged before the voters sync them
//! (`ShardCmd.sync_optional`, honoured under `CHIMAERA_RAFT_SYNC=durable`).
//!
//! Only an attached run's per-call commands: a SubmitTasks, GrantAttempts or
//! PublishResults whose every call belongs to an active, attached run, has
//! ordinary effects (not `external`), is a function, map item or actor method
//! (not a constructor), is neither a durable actor's command nor a durable
//! workflow's step or root, holds no launch token (a gang rank, an actor
//! gang's rank or a reservation's host: the gang's decision record is synced,
//! so its rank calls must be too) and, for results, keeps no value
//! `retained`. A fact this peer cannot establish counts against: unknown or
//! unsure means synced. A submission whose calls differ is proposed as two
//! commands, the synced calls first.
//!
//! The facts come from this peer's replica of the call's group when it holds
//! one, else from what it was told: a run's record when it created the run
//! here, and the work a leader sent here to run. A local peer's group has no
//! log to sync, so its commands are never marked.

use crate::router::Router;
use chimaera_core::ids::Id;
use chimaera_core::kv::KvRead;
use chimaera_core::pb;
use chimaera_core::shard::{Tk, read};
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a run's attachment learned from a message is believed. A keyed
/// run's id is used again only once the old run has expired, a day after it
/// ended, so an hour-old fact is never another run's.
const RUN_FACT_LIFE: Duration = Duration::from_secs(3600);
/// Facts kept before a cache starts over (forgetting one only syncs more).
const KEPT: usize = 1 << 16;

/// Whether a call's own facts let its commands go unsynced (`with_result`:
/// its result's publication too): ordinary effects, a function, map item or
/// actor method, no durable actor command or durable workflow step (both
/// carry a command fingerprint) or root, no launch token, and no result of
/// the direct path kept `retained`. A policy or enum value this peer cannot
/// read counts against.
fn call(spec: &pb::TaskSpec, with_result: bool) -> bool {
    let Ok(policy) = pb::Policy::decode(spec.policy.as_slice()) else { return false };
    let effects = [pb::Effects::None, pb::Effects::Idempotent].map(|e| e as i32);
    let kinds =
        [pb::CallKind::CallFunction, pb::CallKind::CallMapItem, pb::CallKind::CallActorMethod].map(|k| k as i32);
    effects.contains(&spec.effects)
        && kinds.contains(&spec.kind)
        && spec.command_id.is_empty()
        && spec.command_fingerprint.is_empty()
        && spec.workflow_step.is_empty()
        && !spec.durable_workflow
        && policy.launch.is_none()
        && (!with_result || plain(policy.durability))
        && spec.ran.as_ref().and_then(|r| r.outcome.as_ref()).is_none_or(outcome)
}

/// Whether no value of an outcome is kept `retained` (an error keeps none).
fn outcome(o: &pb::Outcome) -> bool {
    match &o.value {
        Some(pb::outcome::Value::Ok(obj)) => plain(obj.durability) && obj.blobs.iter().all(|b| plain(b.durability)),
        _ => true,
    }
}

/// A durability class this peer knows, other than `retained`.
fn plain(durability: i32) -> bool {
    [pb::Durability::Default, pb::Durability::Ephemeral, pb::Durability::Replayable]
        .map(|d| d as i32)
        .contains(&durability)
}

/// Whether a run's record says it is active and attached.
fn attached(rec: &pb::RunRecord) -> bool {
    rec.state == pb::run_record::State::Active as i32 && !rec.detached
}

/// The calls a batch names, all in vshard `v`; None when one is malformed
/// or elsewhere, or there are none.
fn keys<'a>(v: u16, named: impl Iterator<Item = Option<&'a pb::TaskKey>>) -> Option<Vec<Tk>> {
    let tks: Vec<Tk> = named.map(|k| k.and_then(Tk::parse).filter(|tk| tk.v() == v)).collect::<Option<_>>()?;
    (!tks.is_empty()).then_some(tks)
}

/// Per call, from this replica's records: whether its commands may go
/// unsynced (`with_result`: its result's publication too); None when the
/// replica holds no record of the call or no copy of its run's.
fn facts_in(kv: &dyn KvRead, tks: &[Tk], with_result: bool) -> Vec<Option<bool>> {
    let mut runs: HashMap<Id, Option<bool>> = HashMap::new();
    tks.iter()
        .map(|tk| {
            let spec = read::task(kv, *tk)?.spec?;
            let run = *runs.entry(tk.run).or_insert_with(|| read::run_in(kv, tk.v(), tk.run).map(|r| attached(&r)));
            Some(run? && call(&spec, with_result))
        })
        .collect()
}

/// A submission's commands: its batches by (vshard, granted here), each
/// split into the calls that must be synced and those that may go unsynced,
/// as (batch, calls, sync_optional), the synced part of a batch first.
pub fn submits(
    router: &Router,
    batches: BTreeMap<(u16, bool), Vec<pb::TaskSpec>>,
) -> Vec<((u16, bool), Vec<pb::TaskSpec>, bool)> {
    let cluster = router.is_cluster();
    let mut runs: HashMap<Id, bool> = HashMap::new();
    let mut out = Vec::with_capacity(batches.len());
    for ((v, here), tasks) in batches {
        let (optional, synced) = if cluster {
            let mut attached = |run: Id| {
                *runs.entry(run).or_insert_with(|| {
                    router.known.is_attached(run, || router.read(v, |kv| read::run_in(kv, v, run)).flatten())
                })
            };
            split(v, tasks, &mut attached)
        } else {
            (Vec::new(), tasks)
        };
        out.extend(
            [(synced, false), (optional, true)]
                .into_iter()
                .filter(|(t, _)| !t.is_empty())
                .map(|(t, o)| ((v, here), t, o)),
        );
    }
    out
}

/// The calls of a submission to vshard `v` that may go unsynced (their own
/// facts allow it and `attached` says their run is), and the rest.
fn split(
    v: u16,
    tasks: Vec<pb::TaskSpec>,
    attached: &mut impl FnMut(Id) -> bool,
) -> (Vec<pb::TaskSpec>, Vec<pb::TaskSpec>) {
    tasks.into_iter().partition(|t| {
        call(t, false) && t.key.as_ref().and_then(Tk::parse).filter(|tk| tk.v() == v).is_some_and(|tk| attached(tk.run))
    })
}

/// A GrantAttempts of `grants` in vshard `v`, proposed by its group's leader,
/// which holds the calls' records.
pub fn grants(router: &Router, v: u16, grants: &[pb::Grant]) -> bool {
    if !router.is_cluster() {
        return false;
    }
    let Some(tks) = keys(v, grants.iter().map(|g| g.task.as_ref())) else { return false };
    router.read(v, |kv| facts_in(kv, &tks, false).into_iter().all(|f| f == Some(true))).unwrap_or(false)
}

/// A PublishResults of `entries` in vshard `v`: from this replica's records
/// when it holds them, else from the work a leader sent here.
pub fn publish(router: &Router, v: u16, entries: &[pb::PublishEntry]) -> bool {
    if !router.is_cluster() {
        return false;
    }
    let Some(tks) = keys(v, entries.iter().map(|e| e.task.as_ref())) else { return false };
    let sent = router.known.take(&tks);
    let here = router.read(v, |kv| facts_in(kv, &tks, true));
    decide(entries, &sent, here.as_deref())
}

/// Every entry keeps no value `retained`, and its call may go unsynced by its
/// records here (`here`), or, where this replica holds none, by its work (`sent`).
fn decide(entries: &[pb::PublishEntry], sent: &[bool], here: Option<&[Option<bool>]>) -> bool {
    entries
        .iter()
        .enumerate()
        .all(|(i, e)| e.outcome.as_ref().is_none_or(outcome) && here.and_then(|h| h[i]).unwrap_or(sent[i]))
}

/// What this peer was told about runs and calls, for the groups it holds no
/// replica of. Shared by ingress and the core through the router.
#[derive(Default)]
pub struct Known {
    /// Runs: attached or not, and when that was learned.
    runs: Mutex<HashMap<Id, (bool, Instant)>>,
    /// Calls a leader sent here whose results may be published unsynced.
    calls: Mutex<HashSet<Tk>>,
}

impl Known {
    /// A run's record: created here, or read from a replica here.
    pub fn note_run(&self, rec: &pb::RunRecord) {
        if let Ok(run) = Id::from_slice(&rec.run) {
            self.set_run(run, attached(rec), Instant::now());
        }
    }

    /// An attempt a leader sent here. The leader read the run's record when it
    /// sent the run's environment (a replayed constructor carries its actor's
    /// flag instead, and publishes nothing); the call's result may then be
    /// published unsynced when its run is attached and its own facts allow.
    /// A later attempt of the same call replaces what an earlier one said.
    pub fn note_remote(&self, rw: &pb::RemoteWork) {
        let Some(tk) = rw.task.as_ref().and_then(Tk::parse) else { return };
        let read = rw.env.is_some() && !rw.replay;
        if read {
            self.set_run(tk.run, !rw.detached, Instant::now());
        }
        let optional = read && !rw.detached && rw.spec.as_ref().is_some_and(|s| call(s, true));
        let mut calls = self.calls.lock().expect("lock");
        if !optional {
            calls.remove(&tk);
            return;
        }
        if calls.len() >= KEPT {
            calls.clear();
        }
        calls.insert(tk);
    }

    fn set_run(&self, run: Id, attached: bool, at: Instant) {
        let mut runs = self.runs.lock().expect("lock");
        if runs.len() >= KEPT && !runs.contains_key(&run) {
            runs.clear();
        }
        runs.insert(run, (attached, at));
    }

    /// Whether `run` is attached: as learned within RUN_FACT_LIFE, else by
    /// its record in a replica here (`replica`), else not.
    fn is_attached(&self, run: Id, replica: impl FnOnce() -> Option<pb::RunRecord>) -> bool {
        if let Some(a) = self.run(run, Instant::now()) {
            return a;
        }
        let Some(rec) = replica() else { return false };
        self.note_run(&rec);
        attached(&rec)
    }

    /// Whether `run` is attached, as last learned within RUN_FACT_LIFE of `now`.
    fn run(&self, run: Id, now: Instant) -> Option<bool> {
        let runs = self.runs.lock().expect("lock");
        runs.get(&run).filter(|(_, at)| now.saturating_duration_since(*at) < RUN_FACT_LIFE).map(|(a, _)| *a)
    }

    /// Per call: whether it was sent here with a result that may be published
    /// unsynced. Each is forgotten: it is being published.
    fn take(&self, tks: &[Tk]) -> Vec<bool> {
        let mut calls = self.calls.lock().expect("lock");
        tks.iter().map(|tk| calls.remove(tk)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimaera_core::kv::MemKv;
    use chimaera_core::shard::ShardState;
    use pb::shard_cmd::Body;

    const V: u16 = 7;

    fn spec(tk: Tk) -> pb::TaskSpec {
        pb::TaskSpec { key: Some(tk.pb()), max_attempts: 3, ..Default::default() }
    }

    fn ok(durability: pb::Durability) -> pb::Outcome {
        let obj = pb::ObjectRef { inline_data: vec![1], size: 1, durability: durability as i32, ..Default::default() };
        pb::Outcome { value: Some(pb::outcome::Value::Ok(obj)), ..Default::default() }
    }

    fn policy(durability: pb::Durability) -> Vec<u8> {
        pb::Policy { durability: durability as i32, ..Default::default() }.encode_to_vec()
    }

    /// The policy of a call holding a slot's launch token (a gang rank, an
    /// actor gang's rank or a reservation's host).
    fn launched(owner: &str) -> Vec<u8> {
        let launch = pb::LaunchToken { owner: owner.into(), generation: 1, slot: 0 };
        pb::Policy { launch: Some(launch), ..Default::default() }.encode_to_vec()
    }

    fn tk(run: Id, i: u64) -> Tk {
        Tk { run, task: run.child(&[&i.to_le_bytes()]) }
    }

    /// A cluster peer's router that holds no replica: its facts come from
    /// what it was told.
    fn router() -> Router {
        let identity = crate::stores::Identity { peer: Id::derive(0, 1, &[b"peer"]), incarnation: 1 };
        let me = pb::Member { node_id: identity.node_id(), ..Default::default() };
        let mesh = crate::mesh::MeshClient::new("c".into(), me, None, None).unwrap();
        Router::cluster(identity, 1, std::sync::Arc::new(mesh))
    }

    #[test]
    fn a_calls_own_facts() {
        let t = tk(Id::derive(0, V, &[b"run"]), 1);
        let plain = spec(t);
        assert!(call(&plain, false) && call(&plain, true), "a plain function call");
        for (why, s) in [
            ("map item", pb::TaskSpec { kind: pb::CallKind::CallMapItem as i32, ..plain.clone() }),
            ("actor method", pb::TaskSpec { kind: pb::CallKind::CallActorMethod as i32, ..plain.clone() }),
            ("idempotent", pb::TaskSpec { effects: pb::Effects::Idempotent as i32, ..plain.clone() }),
            ("a replayable result", pb::TaskSpec { policy: policy(pb::Durability::Replayable), ..plain.clone() }),
            (
                "direct-path result",
                pb::TaskSpec {
                    ran: Some(pb::Ran { outcome: Some(ok(pb::Durability::Default)), ..Default::default() }),
                    ..plain.clone()
                },
            ),
        ] {
            assert!(call(&s, false) && call(&s, true), "{why} may go unsynced");
        }
        for (why, s) in [
            ("external effects", pb::TaskSpec { effects: pb::Effects::External as i32, ..plain.clone() }),
            ("unknown effects", pb::TaskSpec { effects: 9, ..plain.clone() }),
            ("actor constructor", pb::TaskSpec { kind: pb::CallKind::CallActorCtor as i32, ..plain.clone() }),
            ("unknown kind", pb::TaskSpec { kind: 9, ..plain.clone() }),
            ("durable actor command id", pb::TaskSpec { command_id: b"c".to_vec(), ..plain.clone() }),
            ("durable command fingerprint", pb::TaskSpec { command_fingerprint: vec![1; 32], ..plain.clone() }),
            ("durable workflow root", pb::TaskSpec { durable_workflow: true, ..plain.clone() }),
            ("workflow step", pb::TaskSpec { workflow_step: "{\"step\": 0}".into(), ..plain.clone() }),
            ("gang rank", pb::TaskSpec { policy: launched("gang/g"), ..plain.clone() }),
            (
                "actor gang rank",
                pb::TaskSpec {
                    kind: pb::CallKind::CallActorMethod as i32,
                    policy: launched("gang/a"),
                    ..plain.clone()
                },
            ),
            ("reservation host", pb::TaskSpec { policy: launched("reserve/r"), ..plain.clone() }),
            ("an undecodable policy", pb::TaskSpec { policy: vec![0xff], ..plain.clone() }),
            (
                "retained direct-path result",
                pb::TaskSpec {
                    ran: Some(pb::Ran { outcome: Some(ok(pb::Durability::Retained)), ..Default::default() }),
                    ..plain.clone()
                },
            ),
        ] {
            assert!(!call(&s, false) && !call(&s, true), "{why} must be synced");
        }
        // A retained result: the call may be submitted unsynced, its result not published so.
        let retained = pb::TaskSpec { policy: policy(pb::Durability::Retained), ..plain.clone() };
        assert!(call(&retained, false) && !call(&retained, true));
        let unknown = pb::Policy { durability: 9, ..Default::default() }.encode_to_vec();
        assert!(!call(&pb::TaskSpec { policy: unknown, ..plain }, true), "a class this peer does not know");
    }

    #[test]
    fn retained_values_are_synced() {
        for d in [pb::Durability::Default, pb::Durability::Ephemeral, pb::Durability::Replayable] {
            assert!(outcome(&ok(d)), "{d:?}");
        }
        assert!(!outcome(&ok(pb::Durability::Retained)));
        let mut unknown = ok(pb::Durability::Default);
        let Some(pb::outcome::Value::Ok(obj)) = unknown.value.as_mut() else { unreachable!() };
        obj.durability = 9;
        assert!(!outcome(&unknown), "a class this peer does not know");
        let mut blob = ok(pb::Durability::Default);
        let Some(pb::outcome::Value::Ok(obj)) = blob.value.as_mut() else { unreachable!() };
        obj.blobs.push(pb::ObjectRef { durability: pb::Durability::Retained as i32, ..Default::default() });
        assert!(!outcome(&blob), "a retained buffer of the value");
        let err = pb::Outcome { value: Some(pb::outcome::Value::Err(Default::default())), ..Default::default() };
        assert!(outcome(&err) && outcome(&pb::Outcome::default()));
    }

    /// A shard replica holding an attached and a detached run, each with
    /// calls of every kind the rule tells apart.
    struct Replica {
        sm: ShardState,
        kv: MemKv,
        hlc: u64,
    }

    impl Replica {
        fn new() -> Replica {
            let mut r = Replica { sm: ShardState::new(), kv: MemKv::default(), hlc: 1 };
            r.apply(Body::AdoptVshard(pb::AdoptVShard { vshards: vec![V as u32], epoch: 1 }));
            r
        }

        fn apply(&mut self, body: Body) -> pb::ShardReply {
            self.hlc += 1;
            let cmd = pb::ShardCmd {
                vshard: V as u32,
                vshard_epoch: 1,
                hlc: self.hlc,
                body: Some(body),
                ..Default::default()
            };
            self.sm.apply(&mut self.kv, &cmd).reply
        }

        fn run(&mut self, name: &[u8], detached: bool) -> Id {
            let run = Id::derive(0, V, &[name]);
            let rec = pb::RunRecord { run: run.to_vec(), tenant: "t".into(), detached, ..Default::default() };
            self.apply(Body::RunCreate(pb::RunCreate { run: Some(rec) }));
            run
        }

        fn submit(&mut self, tasks: Vec<pb::TaskSpec>) {
            let n = tasks.len();
            let reply = self.apply(Body::SubmitTasks(pb::SubmitTasks { tasks, grant_to: None }));
            let Some(pb::shard_reply::Body::Tasks(t)) = reply.body else { panic!("{reply:?}") };
            assert_eq!(t.acks.iter().filter(|a| a.status() == pb::task_ack::Status::Accepted).count(), n, "{t:?}");
        }
    }

    #[test]
    fn a_replica_holds_the_facts() {
        let mut r = Replica::new();
        let (attached, detached) = (r.run(b"attached", false), r.run(b"detached", true));
        let (plain, external, step, retained) = (tk(attached, 1), tk(attached, 2), tk(attached, 3), tk(attached, 4));
        let (rank, other) = (tk(attached, 5), tk(detached, 1));
        r.submit(vec![
            spec(plain),
            pb::TaskSpec { effects: pb::Effects::External as i32, ..spec(external) },
            pb::TaskSpec { workflow_step: "{}".into(), command_fingerprint: vec![1; 32], ..spec(step) },
            pb::TaskSpec { policy: policy(pb::Durability::Retained), ..spec(retained) },
            pb::TaskSpec { policy: launched("gang/g"), ..spec(rank) },
        ]);
        r.submit(vec![spec(other)]);
        let missing = tk(attached, 99);
        let facts = facts_in(&r.kv, &[plain, external, step, retained, rank, other, missing], false);
        assert_eq!(facts, [Some(true), Some(false), Some(false), Some(true), Some(false), Some(false), None]);
        let facts = facts_in(&r.kv, &[plain, retained], true);
        assert_eq!(facts, [Some(true), Some(false)], "a retained call's result is published synced");
        // A run that ended no longer has per-call commands to spare.
        r.apply(Body::RunFinish(pb::RunFinish { run: attached.to_vec(), ..Default::default() }));
        assert_eq!(facts_in(&r.kv, &[plain], false), [Some(false)]);
        // A run whose record is not here is unknown.
        let elsewhere = Tk { run: Id::derive(0, V, &[b"elsewhere"]), task: plain.task };
        assert_eq!(facts_in(&r.kv, &[elsewhere], false), [None]);
    }

    #[test]
    fn a_batch_needs_every_entry() {
        let run = Id::derive(0, V, &[b"run"]);
        let entry = |i, o: pb::Outcome| pb::PublishEntry {
            task: Some(tk(run, i).pb()),
            outcome: Some(o),
            ..Default::default()
        };
        let entries = vec![entry(1, ok(pb::Durability::Default)), entry(2, ok(pb::Durability::Default))];
        assert!(decide(&entries, &[true, true], None), "both sent here unsynced, no replica");
        assert!(!decide(&entries, &[true, false], None), "one of them not");
        assert!(decide(&entries, &[false, false], Some(&[Some(true), Some(true)])), "the replica says so");
        assert!(!decide(&entries, &[true, true], Some(&[Some(true), Some(false)])), "the replica's records win");
        assert!(decide(&entries, &[true, true], Some(&[Some(true), None])), "the work, where the replica lags");
        assert!(!decide(&entries, &[false, false], Some(&[Some(true), None])), "neither knows");
        let mixed = vec![entry(1, ok(pb::Durability::Default)), entry(2, ok(pb::Durability::Retained))];
        assert!(!decide(&mixed, &[true, true], None), "a retained value");
        let forward =
            pb::PublishEntry { task: Some(tk(run, 1).pb()), forward_to: Some(tk(run, 2).pb()), ..Default::default() };
        assert!(decide(&[forward], &[true], None), "a forward publishes no value");
        assert!(keys(V, std::iter::empty()).is_none(), "an empty batch");
        let elsewhere = Id::derive(0, V + 1, &[b"run"]);
        assert!(keys(V, [Some(&tk(run, 1).pb()), Some(&tk(elsewhere, 1).pb())].into_iter()).is_none());
        assert!(keys(V, [Some(&pb::TaskKey::default())].into_iter()).is_none(), "a malformed key");
    }

    #[test]
    fn what_a_leader_sent() {
        let known = Known::default();
        let run = Id::derive(0, V, &[b"run"]);
        let env = Some(pb::ExecEnvSpec::default());
        let work = |i, detached: bool| pb::RemoteWork {
            task: Some(tk(run, i).pb()),
            spec: Some(spec(tk(run, i))),
            env: env.clone(),
            detached,
            ..Default::default()
        };
        known.note_remote(&work(1, false));
        assert_eq!(known.run(run, Instant::now()), Some(true));
        known.note_remote(&pb::RemoteWork { replay: true, detached: true, ..work(2, true) });
        assert_eq!(known.run(run, Instant::now()), Some(true), "a replayed constructor carries its actor's flag");
        known.note_remote(&pb::RemoteWork { env: None, ..work(3, false) });
        let external = pb::TaskSpec { effects: pb::Effects::External as i32, ..spec(tk(run, 4)) };
        known.note_remote(&pb::RemoteWork { spec: Some(external), ..work(4, false) });
        let retained = pb::TaskSpec { policy: policy(pb::Durability::Retained), ..spec(tk(run, 5)) };
        known.note_remote(&pb::RemoteWork { spec: Some(retained), ..work(5, false) });
        let rank = pb::TaskSpec { policy: launched("gang/g"), ..spec(tk(run, 6)) };
        known.note_remote(&pb::RemoteWork { spec: Some(rank), ..work(6, false) });
        let all: Vec<Tk> = (1..=6).map(|i| tk(run, i)).collect();
        assert_eq!(known.take(&all), [true, false, false, false, false, false]);
        assert_eq!(known.take(&all[..1]), [false], "taken once, by its publication");
        // A later attempt that may not go unsynced replaces an earlier one's word.
        known.note_remote(&work(1, false));
        known.note_remote(&work(1, true));
        assert_eq!(known.take(&all[..1]), [false]);
        assert_eq!(known.run(run, Instant::now()), Some(false));
        // An old fact about a run is not believed.
        assert_eq!(known.run(run, Instant::now() + RUN_FACT_LIFE), None);
        let rec = pb::RunRecord { run: run.to_vec(), ..Default::default() };
        known.note_run(&rec);
        assert_eq!(known.run(run, Instant::now()), Some(true));
        known.note_run(&pb::RunRecord { state: pb::run_record::State::Finished as i32, ..rec });
        assert_eq!(known.run(run, Instant::now()), Some(false), "an ended run");
    }

    #[test]
    fn a_submission_asks_the_replica_once() {
        let known = Known::default();
        let run = Id::derive(0, V, &[b"run"]);
        let rec = pb::RunRecord { run: run.to_vec(), ..Default::default() };
        assert!(!known.is_attached(run, || None), "neither told nor held here");
        assert!(known.is_attached(run, || Some(rec.clone())), "the replica's record");
        assert!(known.is_attached(run, || panic!("asked again")), "remembered");
        let detached = Id::derive(0, V, &[b"detached"]);
        let rec = pb::RunRecord { run: detached.to_vec(), detached: true, ..Default::default() };
        assert!(!known.is_attached(detached, || Some(rec.clone())));
        assert!(!known.is_attached(detached, || panic!("asked again")));
    }

    #[test]
    fn a_gangs_rank_submits_are_synced_while_an_ordinary_call_in_the_same_batch_is_not() {
        let r = router();
        let run = Id::derive(0, V, &[b"run"]);
        r.known.note_run(&pb::RunRecord { run: run.to_vec(), ..Default::default() });
        let (plain, rank, host) = (spec(tk(run, 1)), tk(run, 2), tk(run, 3));
        let rank = pb::TaskSpec { policy: launched("gang/g"), ..spec(rank) };
        let host = pb::TaskSpec { policy: launched("reserve/r"), ..spec(host) };
        let more = spec(tk(run, 4));
        let batch = BTreeMap::from([((V, false), vec![plain.clone(), rank.clone(), host.clone(), more.clone()])]);
        let got = submits(&r, batch);
        assert_eq!(got, [((V, false), vec![rank, host], false), ((V, false), vec![plain.clone(), more], true)]);
        // Batches stay apart, and one with nothing to sync is one command.
        let batch = BTreeMap::from([((V, false), vec![plain.clone()]), ((V, true), vec![plain.clone()])]);
        assert_eq!(submits(&r, batch), [((V, false), vec![plain.clone()], true), ((V, true), vec![plain], true)]);
    }

    #[test]
    fn a_submission_this_peer_cannot_place_in_an_attached_run_is_synced() {
        let r = router();
        let (attached, detached, unknown) =
            (Id::derive(0, V, &[b"a"]), Id::derive(0, V, &[b"d"]), Id::derive(0, V, &[b"u"]));
        r.known.note_run(&pb::RunRecord { run: attached.to_vec(), ..Default::default() });
        r.known.note_run(&pb::RunRecord { run: detached.to_vec(), detached: true, ..Default::default() });
        let ended = Id::derive(0, V, &[b"e"]);
        let finished = pb::run_record::State::Finished as i32;
        r.known.note_run(&pb::RunRecord { run: ended.to_vec(), state: finished, ..Default::default() });
        let elsewhere = Id::derive(0, V + 1, &[b"a"]);
        r.known.note_run(&pb::RunRecord { run: elsewhere.to_vec(), ..Default::default() });
        let malformed = pb::TaskSpec { key: Some(pb::TaskKey::default()), ..Default::default() };
        let tasks = vec![
            spec(tk(detached, 1)),
            spec(tk(unknown, 1)),
            spec(tk(ended, 1)),
            spec(tk(elsewhere, 1)),
            pb::TaskSpec { key: None, ..Default::default() },
            malformed,
            pb::TaskSpec { effects: pb::Effects::External as i32, ..spec(tk(attached, 1)) },
            pb::TaskSpec { durable_workflow: true, ..spec(tk(attached, 2)) },
        ];
        let got = submits(&r, BTreeMap::from([((V, false), tasks.clone())]));
        assert_eq!(got, [((V, false), tasks, false)], "every one synced, in one command");
        // Grants and publishes this peer holds no records of are synced too.
        let grant = pb::Grant { task: Some(tk(attached, 3).pb()), ..Default::default() };
        assert!(!grants(&r, V, &[grant]), "no replica here: unknown");
        let entry = |i| pb::PublishEntry {
            task: Some(tk(attached, i).pb()),
            outcome: Some(ok(pb::Durability::Default)),
            ..Default::default()
        };
        assert!(!publish(&r, V, &[entry(3)]), "neither held here nor sent here");
        let work = pb::RemoteWork {
            task: Some(tk(attached, 3).pb()),
            spec: Some(spec(tk(attached, 3))),
            env: Some(pb::ExecEnvSpec::default()),
            ..Default::default()
        };
        r.known.note_remote(&work);
        assert!(publish(&r, V, &[entry(3)]), "sent here to run, in an attached run");
        assert!(!publish(&r, V, &[entry(3)]), "a second publication was not sent");
        assert!(!publish(&r, V, &[]), "an empty batch");
    }
}
