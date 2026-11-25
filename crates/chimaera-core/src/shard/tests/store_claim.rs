//! ch.Store claims: a work queue's consumers take its items in order, each
//! under a lease, through an index that keeps a claim to two keys however many
//! are queued or held; and a property test of that against a model.

use super::*;

const TTL: u64 = 1000;

fn harness() -> Harness {
    let mut h = Harness::new(ShardState::new());
    let v = store::vshard_of("t", "ns");
    h.raw(Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v as u32], epoch: EPOCH }), v);
    h
}

fn claim_for(h: &mut Harness, holder: &str, ttl_ms: u64) -> pb::StoreReply {
    let c = pb::StoreClaim {
        tenant: "t".into(),
        namespace: "ns".into(),
        prefix: "item/".into(),
        lock_prefix: "claim/".into(),
        holder: holder.into(),
        ttl_ms,
    };
    match h.raw(Body::StoreClaim(c), store::vshard_of("t", "ns")).reply.body {
        Some(pb::shard_reply::Body::Store(r)) => r,
        other => panic!("expected a store reply, got {other:?}"),
    }
}

fn claim(h: &mut Harness, holder: &str) -> pb::StoreReply {
    claim_for(h, holder, TTL)
}

fn item(n: u32) -> String {
    format!("item/{n:04}")
}

fn enqueue(h: &mut Harness, n: u32) {
    assert!(store_write(h, pb::StoreWrite { value: n.to_be_bytes().to_vec(), ..w(SO::Put, &item(n)) }).ok);
}

/// A RENEW or UNLOCK of the claim `l`.
fn on(op: SO, l: &pb::StoreLease, ttl_ms: u64) -> pb::StoreWrite {
    pb::StoreWrite { holder: l.holder.clone(), token: l.token, ttl_ms, ..w(op, &format!("claim/{}", l.item)) }
}

/// Finish a claim as WorkQueue does: delete the item under the claim's fence,
/// then unlock. The replies of both.
fn complete(h: &mut Harness, l: &pb::StoreLease) -> (pb::StoreReply, pb::StoreReply) {
    let fence = Some(pb::StoreFence { lock: format!("claim/{}", l.item), token: l.token });
    let done = store_write(h, pb::StoreWrite { fence, ..w(SO::Delete, &l.item) });
    (done, store_write(h, on(SO::Unlock, l, 0)))
}

#[test]
fn claims_take_items_in_order_and_each_looks_at_two_keys() {
    let mut h = harness();
    for n in 1..=200 {
        enqueue(&mut h, n);
    }
    let mut held = Vec::new();
    for n in 1..=100 {
        let r = claim(&mut h, &format!("c{n}"));
        assert!(r.ok, "{}", r.error);
        assert!(r.examined <= 2, "claim {n} looked at {} keys", r.examined);
        assert_eq!(r.entry.unwrap().key, item(n));
        held.push(r.lease.unwrap());
    }
    for l in held.iter().step_by(2) {
        assert!(complete(&mut h, l).0.ok);
    }
    for n in 101..=200 {
        let r = claim(&mut h, &format!("c{n}"));
        assert!(r.ok && r.examined <= 2, "{} after {} keys", r.error, r.examined);
        assert_eq!(r.entry.unwrap().key, item(n));
    }
    let none = claim(&mut h, "late");
    assert!(!none.ok && none.error.starts_with("nothing to claim"), "{}", none.error);
    assert!(none.more, "claimed items are left");
    assert!((1..=TTL).contains(&none.retry_ms), "{}", none.retry_ms);
}

#[test]
fn a_lapsed_claim_is_claimed_again_before_new_items() {
    let mut h = harness();
    for n in 1..=3 {
        enqueue(&mut h, n);
    }
    let stale = claim(&mut h, "a").lease.unwrap();
    h.hlc += hlc::from_ms(2 * TTL); // a stalls past its lease
    let again = claim(&mut h, "b");
    assert_eq!(again.entry.unwrap().key, item(1));
    let fresh = again.lease.unwrap();
    assert!(fresh.token > stale.token);
    assert!(!store_write(&mut h, on(SO::Renew, &stale, TTL)).ok, "a lapsed claim cannot be renewed");
    let (late, _) = complete(&mut h, &stale);
    assert!(!late.ok && late.error.starts_with("fenced"), "{}", late.error);
    assert!(complete(&mut h, &fresh).0.ok);
    assert_eq!(claim(&mut h, "c").entry.unwrap().key, item(2));
}

#[test]
fn a_claim_given_back_goes_first_as_a_change() {
    let mut h = harness();
    for n in 1..=3 {
        enqueue(&mut h, n);
    }
    let first = claim(&mut h, "a").lease.unwrap();
    assert_eq!(claim(&mut h, "b").entry.unwrap().key, item(2));
    let before = store_read(&mut h, rd(RK::Get, "")).revision;
    let back = store_write(&mut h, on(SO::Unlock, &first, 0));
    assert!(back.ok && back.revision == before + 1, "a give-back is a change: {back:?}");
    let changes = store_read(&mut h, pb::StoreRead { after_revision: before, ..rd(RK::Changes, "") }).changes;
    let changes: Vec<_> = changes.iter().map(|c| (c.key.clone(), c.deleted, c.revision)).collect();
    assert_eq!(changes, [(item(1), false, back.revision)], "it wakes claims waiting for a change");
    assert_eq!(store_read(&mut h, rd(RK::Get, &item(1))).entry.unwrap().version, back.revision);
    let r = claim(&mut h, "c");
    assert_eq!((r.entry.unwrap().key, r.examined), (item(1), 1));
}

#[test]
fn renewing_a_claim_moves_when_it_comes_due() {
    let mut h = harness();
    enqueue(&mut h, 1);
    let l = claim(&mut h, "a").lease.unwrap();
    h.hlc += hlc::from_ms(TTL / 2);
    assert!(store_write(&mut h, on(SO::Renew, &l, 10 * TTL)).ok);
    h.hlc += hlc::from_ms(2 * TTL); // past the first lease, well inside the renewed one
    let none = claim(&mut h, "b");
    assert!(!none.ok && none.more, "{}", none.error);
    assert!((7 * TTL..=8 * TTL).contains(&none.retry_ms), "lapses in about 8 s, not {} ms", none.retry_ms);
    h.hlc += hlc::from_ms(none.retry_ms);
    assert_eq!(claim(&mut h, "b").entry.unwrap().key, item(1));
}

#[test]
fn nothing_to_claim_says_whether_items_are_left_and_when_a_claim_lapses() {
    let mut h = harness();
    let empty = claim(&mut h, "a");
    assert!(!empty.ok && empty.error.starts_with("nothing to claim") && !empty.more && empty.retry_ms == 0);
    enqueue(&mut h, 1);
    let l = claim(&mut h, "a").lease.unwrap();
    let held = claim(&mut h, "b");
    assert!(!held.ok && held.more && (1..=TTL).contains(&held.retry_ms), "{held:?}");
    let (done, unlocked) = complete(&mut h, &l);
    assert!(done.ok && unlocked.ok);
    let drained = claim(&mut h, "b");
    assert!(!drained.ok && !drained.more && drained.retry_ms == 0, "{drained:?}");
}

#[test]
fn an_item_held_by_a_plain_lock_comes_due_when_that_lapses() {
    // An earlier version claimed items with a plain lock on "claim/" + key.
    let mut h = harness();
    for n in 1..=2 {
        enqueue(&mut h, n);
    }
    let old = pb::StoreWrite { holder: "old".into(), ttl_ms: TTL, ..w(SO::Lock, &format!("claim/{}", item(1))) };
    assert!(store_write(&mut h, old).ok);
    let r = claim(&mut h, "new");
    assert_eq!((r.entry.unwrap().key, r.examined), (item(2), 2));
    let none = claim(&mut h, "late");
    assert!(!none.ok && none.more && none.retry_ms > 0, "{none:?}");
    h.hlc += hlc::from_ms(2 * TTL);
    assert_eq!(claim(&mut h, "late").entry.unwrap().key, item(1), "it lapsed first");
}

#[test]
fn a_claim_drops_stale_entries_within_its_budget() {
    let mut h = harness();
    for n in 1..=100 {
        enqueue(&mut h, n);
        assert!(claim(&mut h, "a").ok);
    }
    // Each item is deleted under its claim, which is never unlocked.
    for n in 1..=100 {
        assert!(store_write(&mut h, w(SO::Delete, &item(n))).ok);
    }
    enqueue(&mut h, 101);
    h.hlc += hlc::from_ms(2 * TTL);
    let busy = claim(&mut h, "b");
    assert!(!busy.ok && busy.error.starts_with("busy") && busy.examined == 64, "{busy:?}");
    let r = claim(&mut h, "b");
    assert!(r.ok, "{}", r.error);
    assert_eq!((r.entry.unwrap().key, r.examined), (item(101), 37), "36 stale entries left, then the item");
}

#[test]
fn scans_and_change_reads_start_where_they_are_asked_to() {
    let mut h = harness();
    for k in ["a/1", "b/1", "b/2", "b/3", "c/1"] {
        store_write(&mut h, pb::StoreWrite { value: k.into(), ..w(SO::Put, k) });
    }
    let scan = |h: &mut Harness, after: &str, limit| {
        let r = store_read(h, pb::StoreRead { start_after: after.into(), limit, ..rd(RK::Scan, "b/") });
        (r.entries.into_iter().map(|e| e.key).collect::<Vec<_>>(), r.more)
    };
    assert_eq!(scan(&mut h, "", 2), (vec!["b/1".to_string(), "b/2".into()], true));
    assert_eq!(scan(&mut h, "b/2", 2), (vec!["b/3".to_string()], false));
    assert_eq!(scan(&mut h, "a/0", 1), (vec!["b/1".to_string()], true), "a start below the prefix, before a/1");
    assert_eq!(scan(&mut h, "c", 5), (vec![], false), "a start above it");
    let at = store_read(&mut h, rd(RK::Get, "")).revision;
    let since = |h: &mut Harness, after| {
        let r = store_read(h, pb::StoreRead { after_revision: after, ..rd(RK::Changes, "") });
        r.changes.into_iter().map(|c| c.key).collect::<Vec<_>>()
    };
    assert_eq!(since(&mut h, at - 2), ["b/3", "c/1"]);
    assert_eq!(since(&mut h, at), Vec::<String>::new());
}

// ---- property test -------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Step {
    Put,
    Claim(u64),
    /// Finish the n-th claim granted and not unlocked (as WorkQueue does).
    Complete(usize),
    GiveBack(usize),
    Renew(usize, u64),
    Advance(u64),
    /// Delete the n-th item directly, under whatever claim it has.
    Drop(usize),
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let ttl = prop_oneof![Just(TTL), Just(3 * TTL)];
    let step = prop_oneof![
        3 => Just(Step::Put),
        4 => ttl.clone().prop_map(Step::Claim),
        2 => any::<usize>().prop_map(Step::Complete),
        1 => any::<usize>().prop_map(Step::GiveBack),
        1 => (any::<usize>(), ttl).prop_map(|(i, t)| Step::Renew(i, t)),
        2 => prop_oneof![Just(300u64), Just(1100), Just(2500)].prop_map(Step::Advance),
        1 => any::<usize>().prop_map(Step::Drop),
    ];
    proptest::collection::vec(step, 1..60)
}

/// Run `steps` beside a model of the items (each with when it comes due:
/// None if never claimed, 0 once given back) and of the claims not unlocked.
/// A claim gets the free item that came due first, else the first item never
/// claimed, and looks at no more than two keys while no entry went stale.
fn run_claim_case(steps: &[Step]) -> Result<(), TestCaseError> {
    let mut h = harness();
    let (mut next, mut stale) = (0, false);
    let mut items: BTreeMap<String, Option<u64>> = BTreeMap::new();
    let mut claims: BTreeMap<String, pb::StoreLease> = BTreeMap::new();
    let live = |l: &pb::StoreLease, at: u64| l.expires_hlc > at;
    let nth = |m: &BTreeMap<String, pb::StoreLease>, i: usize| m.keys().nth(i % m.len().max(1)).cloned();
    for step in steps {
        let at = h.hlc + 1; // the next command's time
        match *step {
            Step::Put => {
                next += 1;
                enqueue(&mut h, next);
                items.insert(item(next), None);
            }
            Step::Claim(ttl) => {
                let free: Vec<&String> =
                    items.keys().filter(|k| !claims.get(*k).is_some_and(|l| live(l, at))).collect();
                let due = free.iter().filter_map(|k| items[*k].map(|d| (d, (*k).clone()))).min();
                let expected = due.map(|(_, k)| k).or_else(|| free.first().map(|k| (*k).clone()));
                let r = claim_for(&mut h, &format!("c{at}"), ttl);
                prop_assert!(stale || r.examined <= 2, "looked at {} keys", r.examined);
                match expected {
                    Some(k) => {
                        prop_assert!(r.ok, "{}", r.error);
                        let l = r.lease.unwrap();
                        prop_assert_eq!(&l.item, &k);
                        prop_assert_eq!(l.expires_hlc, at + hlc::from_ms(ttl));
                        items.insert(k.clone(), Some(l.expires_hlc));
                        claims.insert(k, l);
                    }
                    None => {
                        prop_assert!(!r.ok && r.error.starts_with("nothing to claim"), "{}", r.error);
                        prop_assert_eq!(r.more, !items.is_empty());
                        let first = claims.values().filter(|l| live(l, at)).map(|l| l.expires_hlc).min();
                        prop_assert_eq!(r.retry_ms, first.map_or(0, |e| hlc::wall_ms(e - at) + 1));
                    }
                }
            }
            Step::Complete(i) => {
                let Some(k) = nth(&claims, i) else { continue };
                let l = claims[&k].clone();
                let (done, unlocked) = complete(&mut h, &l);
                prop_assert_eq!(done.ok, live(&l, at) && items.contains_key(&k), "{}", done.error);
                prop_assert_eq!(unlocked.ok, live(&l, at + 1));
                if done.ok {
                    items.remove(&k);
                    stale |= !unlocked.ok; // lapsed in between: its entry outlives the item
                }
                if unlocked.ok {
                    claims.remove(&k);
                }
            }
            Step::GiveBack(i) => {
                let Some(k) = nth(&claims, i) else { continue };
                let r = store_write(&mut h, on(SO::Unlock, &claims[&k], 0));
                prop_assert_eq!(r.ok, live(&claims[&k], at));
                if r.ok {
                    claims.remove(&k);
                    if let Some(due) = items.get_mut(&k) {
                        *due = Some(0);
                    }
                }
            }
            Step::Renew(i, ttl) => {
                let Some(k) = nth(&claims, i) else { continue };
                let r = store_write(&mut h, on(SO::Renew, &claims[&k], ttl));
                prop_assert_eq!(r.ok, live(&claims[&k], at));
                if r.ok {
                    let until = at + hlc::from_ms(ttl);
                    claims.get_mut(&k).unwrap().expires_hlc = until;
                    if let Some(due) = items.get_mut(&k) {
                        *due = Some(until);
                    }
                }
            }
            Step::Advance(ms) => h.hlc += hlc::from_ms(ms),
            Step::Drop(i) => {
                let Some(k) = items.keys().nth(i % items.len().max(1)).cloned() else { continue };
                prop_assert!(store_write(&mut h, w(SO::Delete, &k)).ok);
                stale |= items.remove(&k).flatten().is_some();
            }
        }
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
    fn store_claim_invariants_hold_under_random_histories(steps in steps()) {
        run_claim_case(&steps)?;
    }
}
