//! Durable actor state: binding a named actor to newer code, and committing
//! state too large to keep inline as chunks under one atomically replaced list.

use super::*;

fn ack(r: &Applied) -> pb::ActorAck {
    match &r.reply.body {
        Some(pb::shard_reply::Body::Actor(a)) => a.clone(),
        other => panic!("{other:?}"),
    }
}

fn record(h: &Harness, actor: Id) -> pb::ActorRecord {
    load(&h.a, &keys::actor(V, &actor)).unwrap()
}

/// A placed durable actor (generation 1) of class `digest`, state version `schema`.
fn durable(h: &mut Harness, name: &[u8], digest: &[u8], schema: u32) -> Id {
    let actor = Id::derive(0, V, &[name]);
    let spec = pb::ActorSpec {
        actor: actor.to_vec(),
        durable: true,
        class_digest: digest.to_vec(),
        state_schema: schema,
        ..Default::default()
    };
    h.raw(Body::ActorCreate(pb::ActorCreate { spec: Some(spec) }), V);
    let place = pb::ActorPlace { actor: actor.to_vec(), expected_generation: 0, host: Some(peer_pb((peer(0), 1))) };
    assert_eq!(ack(&h.raw(Body::ActorPlace(place), V)).status(), AStatus::Ok);
    actor
}

fn commit(actor: Id, cmd: &[u8], prev: u64, state: Vec<u8>) -> pb::ActorCommandCommit {
    pb::ActorCommandCommit {
        actor: actor.to_vec(),
        generation: 1,
        cmd_id: cmd.to_vec(),
        fingerprint: cmd.to_vec(),
        prev_version: prev,
        new_state: state,
        reply: Some(pb::ObjectRef { inline_data: cmd.to_vec(), ..Default::default() }),
    }
}

fn upgrade(h: &mut Harness, actor: Id, digest: &[u8], schema: u32) -> Applied {
    let up = pb::ActorUpgrade {
        actor: actor.to_vec(),
        class_digest: digest.to_vec(),
        state_schema: schema,
        ..Default::default()
    };
    h.raw(Body::ActorUpgrade(up), V)
}

#[test]
fn a_durable_actor_is_bound_to_newer_code_only_and_keeps_its_state_and_receipts() {
    let mut h = Harness::new(ShardState::new());
    let actor = durable(&mut h, b"acct", b"v1", 1);
    assert_eq!(ack(&h.raw(Body::ActorCommit(commit(actor, b"op-1", 0, b"s1".to_vec())), V)).status(), AStatus::Ok);

    // Older or equal code never rebinds it: drivers on old code cannot undo an upgrade.
    assert_eq!(ack(&upgrade(&mut h, actor, b"v1-fixed", 1)).status(), AStatus::Existing);
    assert_eq!(record(&h, actor).spec.unwrap().class_digest, b"v1");

    let up = upgrade(&mut h, actor, b"v2", 2);
    assert_eq!(ack(&up).status(), AStatus::Ok);
    assert!(up.events.contains(&Event::ActorChanged(actor)), "its host hears of the new class");
    let rec = record(&h, actor);
    let spec = rec.spec.clone().unwrap();
    assert_eq!((spec.class_digest.as_slice(), spec.state_schema), (&b"v2"[..], 2));
    // The committed state is the v1 snapshot the new class restores from.
    assert_eq!((rec.durable_state.as_slice(), rec.state_version, rec.generation), (&b"s1"[..], 1, 1));
    assert_eq!(ack(&upgrade(&mut h, actor, b"v1", 1)).status(), AStatus::Existing);
    assert_eq!(record(&h, actor).spec.unwrap().class_digest, b"v2");
    // The new class's per-method limits replace the old ones.
    let limit = pb::MethodLimit { method: "export".into(), max_calls: 2, reject: true, ..Default::default() };
    let up = pb::ActorUpgrade {
        actor: actor.to_vec(),
        class_digest: b"v3".to_vec(),
        state_schema: 3,
        methods: vec![limit.clone()],
    };
    assert_eq!(ack(&h.raw(Body::ActorUpgrade(up), V)).status(), AStatus::Ok);
    assert_eq!(record(&h, actor).spec.unwrap().methods, vec![limit]);

    // A command committed under v1 still answers from its stored receipt.
    let again = ack(&h.raw(Body::ActorCommit(commit(actor, b"op-1", 0, b"other".to_vec())), V));
    assert_eq!(again.status(), AStatus::Duplicate);
    assert_eq!((again.reply.unwrap().inline_data, again.version), (b"op-1".to_vec(), 1));
    assert_eq!(ack(&h.raw(Body::ActorCommit(commit(actor, b"op-2", 1, b"s2".to_vec())), V)).status(), AStatus::Ok);

    // Only live durable actors are bound to new code.
    let plain = Id::derive(0, V, &[b"plain"]);
    let spec = pb::ActorSpec { actor: plain.to_vec(), class_digest: b"p1".to_vec(), ..Default::default() };
    h.raw(Body::ActorCreate(pb::ActorCreate { spec: Some(spec) }), V);
    assert_eq!(ack(&upgrade(&mut h, plain, b"p2", 2)).status(), AStatus::Rejected);
    let died = pb::ActorDied { actor: actor.to_vec(), generation: 1, reason: None, terminal: true, moved: false };
    h.raw(Body::ActorDied(died), V);
    assert_eq!(ack(&upgrade(&mut h, actor, b"v3", 3)).status(), AStatus::Dead);
}

fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn put(actor: Id, generation: u64, data: &[u8]) -> Body {
    Body::ActorChunkPut(pb::ActorChunkPut { actor: actor.to_vec(), generation, data: data.to_vec() })
}

fn commit_root(actor: Id, cmd: &[u8], prev: u64, chunks: Vec<Vec<u8>>) -> Body {
    Body::ActorCommitRoot(pb::ActorCommitRoot { commit: Some(commit(actor, cmd, prev, Vec::new())), chunks })
}

/// How many chunks are stored for the actor, on both replicas, checking
/// that their list (keys::actor_chunks) names exactly those.
fn stored_chunks(h: &Harness, actor: Id) -> usize {
    let index = keys::actor_chunks(V, &actor);
    let digests = |kv: &MemKv| {
        let chunks: BTreeSet<Vec<u8>> = kv
            .scan_prefix(&index, usize::MAX)
            .into_iter()
            .filter(|(k, _)| k.len() > index.len())
            .map(|(k, _)| k[index.len()..].to_vec())
            .collect();
        let listed: BTreeSet<Vec<u8>> = kv.get(&index).unwrap_or_default().chunks(32).map(<[u8]>::to_vec).collect();
        assert_eq!(chunks, listed, "the list names the stored chunks");
        chunks
    };
    let n = digests(&h.a);
    assert_eq!(n, digests(&h.b));
    n.len()
}

/// Commit `data` as the next state, putting only the chunks not stored.
fn commit_chunked(h: &mut Harness, actor: Id, cmd: &[u8], data: &[u8]) -> pb::ActorAck {
    let rec = record(h, actor);
    let (root, puts) = chunks::plan(data, &rec.state_chunks);
    for chunk in puts {
        assert_eq!(ack(&h.raw(put(actor, rec.generation, chunk), V)).status(), AStatus::Ok);
    }
    ack(&h.raw(commit_root(actor, cmd, rec.state_version, root), V))
}

#[test]
fn a_crash_after_any_chunk_write_restores_one_complete_revision() {
    let old = noise(3 << 20, 1);
    let mut new = old.clone();
    new[1 << 20..(1 << 20) + 4096].copy_from_slice(&noise(4096, 2)); // a 4 KiB change
    new.extend_from_slice(&noise(64 << 10, 3)); // and 64 KiB more
    let (old_root, _) = chunks::plan(&old, &[]);
    let (new_root, new_puts) = chunks::plan(&new, &old_root);
    // Incremental: the chunks around the change and the end, not the rest.
    assert!(new_puts.len() <= 4 && new_root.len() >= 8, "{} of {} chunks put", new_puts.len(), new_root.len());

    // Crash after the k-th chunk write of the second commit, for every k, and
    // after its commit: the stored state is the old or the new, whole.
    for k in 0..=new_puts.len() + 1 {
        let mut h = Harness::new(ShardState::new());
        let actor = durable(&mut h, b"big", b"v1", 1);
        assert_eq!(commit_chunked(&mut h, actor, b"c1", &old).status(), AStatus::Ok);
        let base = stored_chunks(&h, actor);
        assert_eq!(base, record(&h, actor).state_chunks.len());
        for chunk in new_puts.iter().take(k) {
            h.raw(put(actor, 1, chunk), V);
        }
        if k > new_puts.len() {
            assert_eq!(ack(&h.raw(commit_root(actor, b"c2", 1, new_root.clone()), V)).status(), AStatus::Ok);
        }
        let rec = record(&h, actor);
        let restored = read::actor_state(&h.a, actor, &rec).unwrap();
        assert_eq!(restored, read::actor_state(&h.b, actor, &rec).unwrap());
        assert_eq!(restored == new, k > new_puts.len(), "crash after {k} writes");
        assert!(restored == old || restored == new);
        // Disk: at most the committed chunks and one unfinished commit's.
        assert!(stored_chunks(&h, actor) <= base + new_puts.len());
        // The instance restarts: chunks no commit named are dropped.
        let died = pb::ActorDied { actor: actor.to_vec(), generation: 1, reason: None, terminal: false, moved: false };
        h.raw(Body::ActorDied(died), V);
        let place = pb::ActorPlace { actor: actor.to_vec(), expected_generation: 1, host: Some(peer_pb((peer(1), 1))) };
        h.raw(Body::ActorPlace(place), V);
        let rec = record(&h, actor);
        assert_eq!(stored_chunks(&h, actor), rec.state_chunks.iter().collect::<BTreeSet<_>>().len());
        assert_eq!(read::actor_state(&h.a, actor, &rec).unwrap(), restored, "the restart restores what was committed");
    }
}

#[test]
fn chunks_are_fenced_and_a_commit_needs_every_chunk_it_names() {
    let mut h = Harness::new(ShardState::new());
    let actor = durable(&mut h, b"fenced", b"v1", 1);
    let data = noise(2 << 20, 4);
    let (root, puts) = chunks::plan(&data, &[]);
    assert_eq!(ack(&h.raw(put(actor, 0, puts[0]), V)).status(), AStatus::StaleGeneration);
    assert_eq!(ack(&h.raw(put(actor, 1, &vec![0; MAX_INLINE_STATE + 1]), V)).status(), AStatus::Rejected);
    assert_eq!(stored_chunks(&h, actor), 0);
    for chunk in &puts[1..] {
        h.raw(put(actor, 1, chunk), V);
    }
    let missing = ack(&h.raw(commit_root(actor, b"c1", 0, root.clone()), V));
    assert_eq!(
        (missing.status(), missing.reason.as_str()),
        (AStatus::Rejected, "a chunk of the durable state is missing")
    );
    assert_eq!(record(&h, actor).state_version, 0);
    // A refused commit leaves none of the chunks put for it.
    assert_eq!(stored_chunks(&h, actor), 0);
    for chunk in &puts {
        h.raw(put(actor, 1, chunk), V);
    }
    assert_eq!(ack(&h.raw(commit_root(actor, b"c1", 0, root), V)).status(), AStatus::Ok);
    assert_eq!(read::actor_state(&h.a, actor, &record(&h, actor)).unwrap(), data);
    // State small enough to keep inline again: the chunks go.
    assert_eq!(ack(&h.raw(Body::ActorCommit(commit(actor, b"c2", 1, b"small".to_vec())), V)).status(), AStatus::Ok);
    let rec = record(&h, actor);
    assert!(rec.state_chunks.is_empty() && stored_chunks(&h, actor) == 0);
    assert_eq!(read::actor_state(&h.a, actor, &rec).unwrap(), b"small");
}

#[test]
fn a_repeated_command_leaves_no_chunks_behind() {
    let mut h = Harness::new(ShardState::new());
    let actor = durable(&mut h, b"repeat", b"v1", 1);
    assert_eq!(commit_chunked(&mut h, actor, b"c1", &noise(2 << 20, 5)).status(), AStatus::Ok);
    let committed = stored_chunks(&h, actor);
    // The command again, its call having run again to other state: the
    // stored reply stands, and the chunks put for it go.
    for seed in 6..9 {
        assert_eq!(commit_chunked(&mut h, actor, b"c1", &noise(2 << 20, seed)).status(), AStatus::Duplicate);
        assert_eq!(stored_chunks(&h, actor), committed);
    }
}

#[test]
fn a_dead_actor_keeps_no_state() {
    let died = |actor: Id, generation: u64, terminal: bool| {
        Body::ActorDied(pb::ActorDied { actor: actor.to_vec(), generation, reason: None, terminal, moved: false })
    };
    let half_put = |h: &mut Harness, actor: Id, generation: u64| {
        let data = noise(2 << 20, 11);
        let (_, puts) = chunks::plan(&data, &record(h, actor).state_chunks);
        for chunk in &puts[..puts.len() / 2] {
            h.raw(put(actor, generation, chunk), V);
        }
    };
    // Closed (terminal) with a commit half put.
    let mut h = Harness::new(ShardState::new());
    let actor = durable(&mut h, b"closed", b"v1", 1);
    assert_eq!(commit_chunked(&mut h, actor, b"c1", &noise(3 << 20, 10)).status(), AStatus::Ok);
    half_put(&mut h, actor, 1);
    h.raw(died(actor, 1, true), V);
    let rec = record(&h, actor);
    assert_eq!(rec.state(), AState::Dead);
    assert!(rec.state_chunks.is_empty() && rec.durable_state.is_empty());
    assert_eq!(stored_chunks(&h, actor), 0);

    // Its last restart used up while a commit was half put.
    let actor = durable(&mut h, b"crashed", b"v1", 1);
    assert_eq!(commit_chunked(&mut h, actor, b"c1", &noise(3 << 20, 12)).status(), AStatus::Ok);
    for generation in 1..=DEFAULT_MAX_ATTEMPTS as u64 + 1 {
        half_put(&mut h, actor, generation);
        h.raw(died(actor, generation, false), V);
        if record(&h, actor).state() == AState::Dead {
            break;
        }
        let host = Some(peer_pb((peer(generation as usize), 1)));
        h.raw(Body::ActorPlace(pb::ActorPlace { actor: actor.to_vec(), expected_generation: generation, host }), V);
        assert!(stored_chunks(&h, actor) > 0, "a restarted instance keeps its committed state");
    }
    assert_eq!(record(&h, actor).state(), AState::Dead);
    assert_eq!(stored_chunks(&h, actor), 0);

    // Inline state goes too.
    let actor = durable(&mut h, b"small", b"v1", 1);
    assert_eq!(ack(&h.raw(Body::ActorCommit(commit(actor, b"c1", 0, b"s1".to_vec())), V)).status(), AStatus::Ok);
    h.raw(died(actor, 1, true), V);
    assert!(record(&h, actor).durable_state.is_empty());
}

#[test]
fn a_record_from_before_versions_holds_version_1() {
    let mut h = Harness::new(ShardState::new());
    let actor = durable(&mut h, b"legacy", b"v0", 0);
    assert_eq!(ack(&upgrade(&mut h, actor, b"v1", 1)).status(), AStatus::Existing);
    assert_eq!(record(&h, actor).spec.unwrap().class_digest, b"v0");
    assert_eq!(ack(&upgrade(&mut h, actor, b"v2", 2)).status(), AStatus::Ok);
}
