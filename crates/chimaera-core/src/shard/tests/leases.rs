//! Leases on stored files (leases.rs): scenarios, and a property test of
//! lease counting against a model.

use super::*;
use crate::shard::leases::held;

/// A file of run_id(V): a value (gone when the run ends) or named like its
/// results (it stays until the run expires).
fn file(result: bool, i: u8) -> Vec<u8> {
    let mut f = vec![i; 16];
    f[..4].copy_from_slice(&run_id(V).0[..4]);
    if !result {
        f[0] ^= 0xFF;
    }
    f
}

/// Holders: (peer, incarnation, id on it). The first two are sessions of one
/// peer incarnation.
const HOLDERS: [(usize, u64, u64); 3] = [(0, 1, 7), (0, 1, 8), (1, 1, 7)];

fn lease(h: &mut Harness, files: &[&Vec<u8>], who: usize, release: bool) -> Applied {
    let (p, inc, holder) = HOLDERS[who];
    let l = pb::LeaseObjects {
        run: run_id(V).to_vec(),
        object_ids: files.iter().map(|f| f.to_vec()).collect(),
        peer: Some(peer_pb((peer(p), inc))),
        holder,
        release,
    };
    h.raw(Body::LeaseObjects(l), V)
}

fn refused(a: &Applied) -> bool {
    matches!(a.reply.body, Some(pb::shard_reply::Body::Rejected(_)))
}

fn end(h: &mut Harness) -> Applied {
    h.raw(Body::RunFinish(pb::RunFinish { run: run_id(V).to_vec(), state: 0 }), V)
}

fn retire(h: &mut Harness, p: usize) -> Applied {
    h.raw(Body::Retire(pb::RetireIncarnation { peer: Some(peer_pb((peer(p), 1))) }), V)
}

fn unleased(files: &[&Vec<u8>]) -> Vec<Event> {
    vec![Event::Unleased { run: run_id(V), files: files.iter().map(|f| f.to_vec()).collect() }]
}

#[test]
fn a_held_value_outlives_its_run_until_its_last_holder_lets_go() {
    let mut h = Harness::new(ShardState::new());
    let (a, b) = (file(false, 1), file(false, 2));
    assert!(!refused(&lease(&mut h, &[&a, &b], 0, false)));
    assert!(!refused(&lease(&mut h, &[&a], 1, false)));
    assert!(!refused(&lease(&mut h, &[&a], 0, false)), "holding again is the same lease");
    assert!(end(&mut h).events.contains(&Event::RunEnded(run_id(V))));
    let mut kept = vec![a.clone(), b.clone()];
    kept.sort();
    assert_eq!(held(&h.a, run_id(V)), kept, "what the peer keeps when the run ends");
    // b's only holder lets go: it can go, and nobody can hold it again.
    assert_eq!(lease(&mut h, &[&b], 0, true).events, unleased(&[&b]));
    let again = lease(&mut h, &[&b], 2, false);
    assert!(matches!(&again.reply.body, Some(pb::shard_reply::Body::Rejected(r)) if r.starts_with("its run ended")));
    // a stays while anyone holds it; another session may still join.
    assert!(lease(&mut h, &[&a], 0, true).events.is_empty());
    assert!(!refused(&lease(&mut h, &[&a], 2, false)));
    assert!(!retire(&mut h, 0).events.iter().any(|e| matches!(e, Event::Unleased { .. })), "holder 2 holds a");
    assert!(refused(&lease(&mut h, &[&a], 1, false)), "a retired incarnation holds nothing");
    assert_eq!(retire(&mut h, 1).events, unleased(&[&a]), "the last holder's incarnation was retired");
    assert!(held(&h.a, run_id(V)).is_empty());
}

#[test]
fn a_held_file_holds_its_runs_expiry_back() {
    let mut h = Harness::new(ShardState::new());
    let r = file(true, 1); // named like a result: it stays until the run expires
    lease(&mut h, &[&r], 0, false);
    end(&mut h);
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    let gc = h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V);
    assert!(!gc.events.contains(&Event::RunExpired(run_id(V))));
    assert!(h.a.get(&keys::run(V, &run_id(V))).is_some(), "a held file keeps its run");
    assert!(lease(&mut h, &[&r], 0, true).events.is_empty(), "a result's file goes when its run expires");
    h.hlc += hlc::from_ms(DEFAULT_RUN_RETENTION_MS + 1);
    let gc = h.raw(Body::Gc(pb::Gc { max_keys: 100 }), V);
    assert!(gc.events.contains(&Event::RunExpired(run_id(V))));
}

// ---- property test -------------------------------------------------------------

#[derive(Clone, Debug)]
enum Op {
    /// Files (a bit mask over FILES), holder.
    Hold(u8, usize),
    Release(u8, usize),
    End,
    /// The peer whose incarnation 1 is retired.
    Retire(usize),
}

/// Two values and two files named like results.
fn files() -> Vec<Vec<u8>> {
    vec![file(false, 1), file(false, 2), file(true, 3), file(true, 4)]
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        4 => (1u8..16, 0..HOLDERS.len()).prop_map(|(m, w)| Op::Hold(m, w)),
        4 => (1u8..16, 0..HOLDERS.len()).prop_map(|(m, w)| Op::Release(m, w)),
        1 => Just(Op::End),
        1 => (0usize..2).prop_map(Op::Retire),
    ];
    proptest::collection::vec(op, 1..40)
}

/// Leases as (file, (peer, incarnation, holder)).
type Leases = BTreeSet<(Vec<u8>, (Id, u64, u64))>;

/// Every lease, read from each of its two indexes.
fn stored(kv: &MemKv) -> (Leases, Leases) {
    let run = run_id(V);
    let by_file = kv
        .scan_prefix(&keys::pairs_of_run(V, keys::LEASE, &run), usize::MAX)
        .into_iter()
        .map(|(k, _)| {
            let id = |r: std::ops::Range<usize>| u64::from_be_bytes(k[r].try_into().unwrap());
            (k[19..35].to_vec(), (Id::from_slice(&k[35..51]).unwrap(), id(51..59), id(59..67)))
        })
        .collect();
    let mut by_holder = BTreeSet::new();
    for p in 0..2 {
        for (k, _) in kv.scan_prefix(&keys::leases_held_by(V, &peer(p), 1), usize::MAX) {
            assert_eq!(&k[27..43], &run.0[..], "one run in this test");
            let holder = u64::from_be_bytes(k[59..67].try_into().unwrap());
            by_holder.insert((k[43..59].to_vec(), (peer(p), 1, holder)));
        }
    }
    (by_file, by_holder)
}

fn run_lease_case(ops: &[Op]) -> Result<(), TestCaseError> {
    let mut h = Harness::new(ShardState::new());
    let files = files();
    let value = |f: usize| f < 2;
    let who = |w: usize| (peer(HOLDERS[w].0), HOLDERS[w].1, HOLDERS[w].2);
    let mut model: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new(); // file -> holders
    let (mut ended, mut retired) = (false, [false; 2]);
    for op in ops {
        let picked = |m: u8| (0..files.len()).filter(|i| m & (1 << i) != 0).collect::<Vec<_>>();
        let names = |fs: &[usize]| fs.iter().map(|&f| &files[f]).collect::<Vec<_>>();
        let leased = |model: &BTreeMap<usize, BTreeSet<usize>>, f: usize| model.get(&f).is_some_and(|s| !s.is_empty());
        match *op {
            Op::Hold(m, w) => {
                let fs = picked(m);
                let refuse = retired[HOLDERS[w].0] || (ended && fs.iter().any(|&f| value(f) && !leased(&model, f)));
                let a = lease(&mut h, &names(&fs), w, false);
                prop_assert_eq!(refused(&a), refuse, "{:?}", op);
                if !refuse {
                    for &f in &fs {
                        model.entry(f).or_default().insert(w);
                    }
                }
            }
            Op::Release(m, w) => {
                let fs = picked(m);
                let last: Vec<usize> = fs
                    .iter()
                    .copied()
                    .filter(|&f| model.get_mut(&f).is_some_and(|s| s.remove(&w) && s.is_empty()))
                    .collect();
                let gone: Vec<&Vec<u8>> = last.iter().filter(|&&f| ended && value(f)).map(|&f| &files[f]).collect();
                let a = lease(&mut h, &names(&fs), w, true);
                prop_assert!(!refused(&a));
                prop_assert_eq!(a.events, if gone.is_empty() { vec![] } else { unleased(&gone) }, "{:?}", op);
            }
            Op::End => {
                let a = end(&mut h);
                prop_assert_eq!(a.events.contains(&Event::RunEnded(run_id(V))), !ended);
                ended = true;
            }
            Op::Retire(p) => {
                let mut last: Vec<&Vec<u8>> = Vec::new();
                for (&f, holders) in model.iter_mut() {
                    let had = !holders.is_empty();
                    holders.retain(|&w| HOLDERS[w].0 != p);
                    if had && holders.is_empty() && ended && value(f) {
                        last.push(&files[f]);
                    }
                }
                last.sort();
                retired[p] = true;
                let a = retire(&mut h, p);
                let events: Vec<Event> = a.events.into_iter().filter(|e| matches!(e, Event::Unleased { .. })).collect();
                prop_assert_eq!(events, if last.is_empty() { vec![] } else { unleased(&last) }, "{:?}", op);
            }
        }
        let expected: Leases = model
            .iter()
            .flat_map(|(&f, s)| s.iter().map(move |&w| (f, w)))
            .map(|(f, w)| (files[f].clone(), who(w)))
            .collect();
        let (by_file, by_holder) = stored(&h.a);
        prop_assert_eq!(&by_file, &expected, "after {:?}", op);
        prop_assert_eq!(&by_holder, &expected, "the holder index, after {:?}", op);
        let mut kept: Vec<Vec<u8>> =
            model.iter().filter(|(_, s)| !s.is_empty()).map(|(&f, _)| files[f].clone()).collect();
        kept.sort();
        prop_assert_eq!(held(&h.a, run_id(V)), kept);
    }
    Ok(())
}

proptest! {
    // PROPTEST_CASES overrides the default for deeper runs.
    #![proptest_config(Config {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(512),
        failure_persistence: None,
        ..Config::default()
    })]
    #[test]
    fn lease_counts_match_a_model_under_random_histories(ops in ops()) {
        run_lease_case(&ops)?;
    }
}
