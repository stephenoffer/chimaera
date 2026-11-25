//! ch.Store transactions: every write as one revision once every condition
//! holds, or nothing; and a property test of that against a model.

use super::*;

/// A shard that owns the test namespace's vshard.
fn harness() -> Harness {
    let mut h = Harness::new(ShardState::new());
    let v = store::vshard_of("t", "ns");
    h.raw(Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v as u32], epoch: EPOCH }), v);
    h
}

fn txn(h: &mut Harness, checks: &[(&str, u64)], ops: Vec<pb::StoreWrite>) -> pb::StoreReply {
    let checks = checks.iter().map(|&(key, version)| pb::StoreCheck { key: key.into(), version }).collect();
    let t = pb::StoreTxn { tenant: "t".into(), namespace: "ns".into(), checks, ops };
    match h.raw(Body::StoreTxn(t), store::vshard_of("t", "ns")).reply.body {
        Some(pb::shard_reply::Body::Store(r)) => r,
        other => panic!("expected a store reply, got {other:?}"),
    }
}

fn get(h: &mut Harness, key: &str) -> Option<pb::StoreEntry> {
    store_read(h, rd(RK::Get, key)).entry
}

fn put(key: &str, value: &[u8]) -> pb::StoreWrite {
    pb::StoreWrite { value: value.to_vec(), ..w(SO::Put, key) }
}

fn add(key: &str, delta: i64) -> pb::StoreWrite {
    pb::StoreWrite { delta, ..w(SO::Add, key) }
}

/// A work queue's put: refused if the queue was closed or its size or tail
/// changed since they were read; otherwise size, tail and item together.
fn enqueue(h: &mut Harness, item: &[u8]) -> pb::StoreReply {
    let (size, tail) = (get(h, "size"), get(h, "tail"));
    let version = |e: &Option<pb::StoreEntry>| e.as_ref().map_or(0, |e| e.version);
    let seq = tail.as_ref().map_or(0, |e| e.counter) + 1;
    txn(
        h,
        &[("closed", 0), ("size", version(&size)), ("tail", version(&tail))],
        vec![add("size", 1), add("tail", 1), put(&format!("item/{seq}"), item)],
    )
}

#[test]
fn a_transaction_writes_every_key_as_one_revision_or_none() {
    let mut h = harness();
    let first = enqueue(&mut h, b"a");
    assert!(first.ok, "{}", first.error);
    for key in ["size", "tail", "item/1"] {
        assert_eq!(get(&mut h, key).unwrap().version, first.revision, "{key}");
    }
    // A consumer finishes an item between the producer's reads and its transaction.
    let size_version = get(&mut h, "size").unwrap().version;
    store_write(&mut h, add("size", -1));
    let stale = txn(&mut h, &[("size", size_version)], vec![add("size", 1), add("tail", 1), put("item/2", b"b")]);
    assert!(!stale.ok && stale.error.starts_with("version mismatch"), "{}", stale.error);
    assert_eq!(stale.entry.unwrap().key, "size", "the reply names the key that moved");
    assert!(get(&mut h, "item/2").is_none());
    assert_eq!((get(&mut h, "size").unwrap().counter, get(&mut h, "tail").unwrap().counter), (0, 1));
    // Closing refuses every later put.
    store_write(&mut h, put("closed", b"yes"));
    let closed = enqueue(&mut h, b"c");
    assert!(!closed.ok && closed.entry.unwrap().key == "closed");
    assert_eq!((get(&mut h, "size").unwrap().counter, get(&mut h, "tail").unwrap().counter), (0, 1));
}

#[test]
fn a_completion_under_a_lapsed_claim_removes_nothing() {
    let mut h = harness();
    assert!(enqueue(&mut h, b"a").ok);
    let lock = |holder: &str| pb::StoreWrite { holder: holder.into(), ttl_ms: 1000, ..w(SO::Lock, "claim/item/1") };
    let complete = |token: u64| {
        let fence = Some(pb::StoreFence { lock: "claim/item/1".into(), token });
        vec![pb::StoreWrite { fence, ..w(SO::Delete, "item/1") }, add("size", -1)]
    };
    let token_a = store_write(&mut h, lock("a")).lease.unwrap().token;
    h.hlc += hlc::from_ms(5_000); // a stalls past its lease; b claims the item again
    let token_b = store_write(&mut h, lock("b")).lease.unwrap().token;
    let late = txn(&mut h, &[], complete(token_a));
    assert!(!late.ok && late.error.starts_with("fenced"), "{}", late.error);
    assert!(get(&mut h, "item/1").is_some());
    assert_eq!(get(&mut h, "size").unwrap().counter, 1, "a refused completion keeps the count");
    assert!(txn(&mut h, &[], complete(token_b)).ok);
    assert!(get(&mut h, "item/1").is_none());
    assert_eq!(get(&mut h, "size").unwrap().counter, 0);
    let again = txn(&mut h, &[], complete(token_b));
    assert!(!again.ok && again.error.ends_with("does not exist"));
    assert_eq!(get(&mut h, "size").unwrap().counter, 0, "completing twice counts once");
}

#[test]
fn a_transaction_with_one_refused_write_writes_nothing() {
    let mut h = harness();
    store_write(&mut h, put("plain", b"v"));
    let before = store_read(&mut h, rd(RK::Get, "")).revision;
    let half = vec![0; store::MAX_VALUE / 2 + 1];
    let refusals = [
        (vec![put("a", b"1"), add("plain", 1)], "not a counter"),
        (vec![put("a", b"1"), w(SO::Delete, "missing")], "does not exist"),
        (vec![put("a", b"1"), put("a", b"2")], "written twice"),
        (vec![put("a", b"1"), w(SO::Lock, "l")], "puts, deletes and adds"),
        (vec![put("a", &half), put("b", &half)], "1 MiB"),
        ((0..65).map(|i| add(&format!("c{i}"), 1)).collect(), "at most 64"),
    ];
    for (ops, why) in refusals {
        let r = txn(&mut h, &[], ops);
        assert!(!r.ok && r.error.contains(why), "{why}: {}", r.error);
        assert!(get(&mut h, "a").is_none());
    }
    let absent: Vec<String> = (0..65).map(|i| format!("k{i}")).collect();
    let checks: Vec<(&str, u64)> = absent.iter().map(|k| (k.as_str(), 0)).collect();
    let r = txn(&mut h, &checks, vec![put("a", b"1")]);
    assert!(!r.ok && r.error.contains("checks at most 64"), "{}", r.error);
    assert!(get(&mut h, "a").is_none());
    assert_eq!(store_read(&mut h, rd(RK::Get, "")).revision, before, "a refused transaction bumps nothing");
}

#[test]
fn a_watch_page_holds_a_whole_transaction() {
    let mut h = harness();
    let t = txn(&mut h, &[], vec![put("j/a", b"1"), put("j/b", b"2"), put("j/c", b"3")]);
    store_write(&mut h, put("j/d", b"4"));
    let page = |h: &mut Harness, after| {
        let r = store_read(h, pb::StoreRead { limit: 1, after_revision: after, ..rd(RK::Changes, "j/") });
        (r.changes.iter().map(|c| (c.key.clone(), c.revision)).collect::<Vec<_>>(), r.more)
    };
    let r = t.revision;
    assert_eq!(page(&mut h, r - 1), (vec![("j/a".into(), r), ("j/b".into(), r), ("j/c".into(), r)], true));
    assert_eq!(page(&mut h, r), (vec![("j/d".into(), r + 1)], false));
}

// ---- property test -------------------------------------------------------------

const KEYS: [&str; 4] = ["k0", "k1", "k2", "k3"];

#[derive(Clone, Debug)]
enum Write {
    Put(usize, u8),
    Delete(usize),
    Add(usize, i64),
}

#[derive(Clone, Debug)]
enum Cmd {
    /// Checks (key, true: its current version, false: another), then writes.
    Txn(Vec<(usize, bool)>, Vec<Write>),
    One(Write),
}

#[derive(Clone, Debug, PartialEq)]
enum Val {
    Bytes(Vec<u8>),
    Counter(i64),
}

fn writes() -> impl Strategy<Value = Write> {
    let k = 0..KEYS.len();
    prop_oneof![
        (k.clone(), any::<u8>()).prop_map(|(k, b)| Write::Put(k, b)),
        k.clone().prop_map(Write::Delete),
        (k, -3i64..4).prop_map(|(k, d)| Write::Add(k, d)),
    ]
}

fn cmds() -> impl Strategy<Value = Vec<Cmd>> {
    let checks = proptest::collection::vec((0..KEYS.len(), proptest::bool::weighted(0.8)), 0..3);
    let cmd = prop_oneof![
        3 => (checks, proptest::collection::vec(writes(), 0..4)).prop_map(|(c, w)| Cmd::Txn(c, w)),
        1 => writes().prop_map(Cmd::One),
    ];
    proptest::collection::vec(cmd, 1..40)
}

impl Write {
    fn key(&self) -> &'static str {
        match *self {
            Write::Put(k, _) | Write::Delete(k) | Write::Add(k, _) => KEYS[k],
        }
    }

    fn pb(&self) -> pb::StoreWrite {
        match *self {
            Write::Put(k, b) => put(KEYS[k], &[b]),
            Write::Delete(k) => w(SO::Delete, KEYS[k]),
            Write::Add(k, d) => add(KEYS[k], d),
        }
    }
}

/// Run `cmds` beside a model: a command applies whole, as the next revision,
/// exactly when every check holds and each write could apply alone on a key of
/// its own; otherwise nothing changes. Each write logs one change, in order.
fn run_store_case(cmds: &[Cmd]) -> Result<(), TestCaseError> {
    let mut h = harness();
    let mut rev = txn(&mut h, &[], vec![]).revision;
    let mut model: BTreeMap<String, (u64, Val)> = BTreeMap::new();
    for cmd in cmds {
        let version = |key: &str| model.get(key).map_or(0, |e| e.0);
        let (checks, writes): (Vec<(&str, u64)>, Vec<Write>) = match cmd {
            Cmd::Txn(c, w) => {
                let c = c.iter().map(|&(k, right)| (KEYS[k], version(KEYS[k]) + u64::from(!right))).collect();
                (c, w.clone())
            }
            Cmd::One(w) => (vec![], vec![w.clone()]),
        };
        let mut seen = BTreeSet::new();
        let applies = checks.iter().all(|&(k, v)| version(k) == v)
            && writes.iter().all(|wr| {
                seen.insert(wr.key())
                    && match wr {
                        Write::Put(..) => true,
                        Write::Delete(_) => model.contains_key(wr.key()),
                        Write::Add(..) => !matches!(model.get(wr.key()), Some((_, Val::Bytes(_)))),
                    }
            });
        let reply = match cmd {
            Cmd::Txn(..) => txn(&mut h, &checks, writes.iter().map(Write::pb).collect()),
            Cmd::One(wr) => store_write(&mut h, wr.pb()),
        };
        prop_assert_eq!(reply.ok, applies, "{:?}: {}", cmd, reply.error);
        if applies && !writes.is_empty() {
            rev += 1;
            for wr in &writes {
                let key = wr.key().to_string();
                match *wr {
                    Write::Put(_, b) => drop(model.insert(key, (rev, Val::Bytes(vec![b])))),
                    Write::Delete(_) => drop(model.remove(&key)),
                    Write::Add(_, d) => {
                        let n = match model.get(&key) {
                            Some((_, Val::Counter(n))) => *n,
                            _ => 0,
                        };
                        model.insert(key, (rev, Val::Counter(n.wrapping_add(d))));
                    }
                }
            }
            let logged = store_read(&mut h, pb::StoreRead { after_revision: rev - 1, ..rd(RK::Changes, "") }).changes;
            let logged: Vec<_> = logged.iter().map(|c| (c.key.as_str(), c.revision, c.deleted)).collect();
            let expected: Vec<_> = writes.iter().map(|wr| (wr.key(), rev, matches!(wr, Write::Delete(_)))).collect();
            prop_assert_eq!(logged, expected, "one change per write, in order, at the new revision");
        }
        prop_assert_eq!(reply.revision, rev, "the revision moves by one per applied command, never back");
        let stored: BTreeMap<String, (u64, Val)> = store_read(&mut h, rd(RK::Scan, ""))
            .entries
            .into_iter()
            .map(|e| {
                let val = if e.is_counter { Val::Counter(e.counter) } else { Val::Bytes(e.value) };
                (e.key, (e.version, val))
            })
            .collect();
        prop_assert_eq!(&stored, &model, "after {:?}", cmd);
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
    fn store_txn_invariants_hold_under_random_histories(cmds in cmds()) {
        run_store_case(&cmds)?;
    }
}
