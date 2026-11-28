//! A durable actor's state beyond one inline commit: binding a live actor
//! to newer code (ActorUpgrade), and committing state larger than
//! MAX_INLINE_STATE as content-addressed chunks under one list that its
//! commit replaces whole (ActorChunkPut, then ActorCommitRoot).

use super::*;
use chimaera_core::shard::{MAX_CHUNKED_STATE, MAX_INLINE_STATE, chunks};

/// Tests: wait this long before each chunk put, so a test can stop the
/// actor's process while its commit is half written.
fn chunk_delay() -> Option<Duration> {
    static DELAY: std::sync::OnceLock<Option<Duration>> = std::sync::OnceLock::new();
    *DELAY.get_or_init(|| {
        std::env::var("CHIMAERA_STATE_CHUNK_DELAY_MS").ok().and_then(|v| v.parse().ok()).map(Duration::from_millis)
    })
}

/// Tests (debug builds only), together what a leader dying between a commit
/// and its answer leaves the actor's host with: `CHIMAERA_TEST_LOSE_COMMIT_REPLY=n`
/// drops the answer to this peer's n-th durable commit after it landed, and
/// `CHIMAERA_TEST_NO_RECORD_PUSH=1` keeps an instance that holds a committed
/// state from taking a newer one from the records its leader pushes.
fn test_setting(name: &str) -> u64 {
    if cfg!(debug_assertions) { std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(0) } else { 0 }
}

/// Whether this commit's answer is to be dropped (see `test_setting`).
fn lose_this_reply() -> bool {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    static SEEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = *N.get_or_init(|| test_setting("CHIMAERA_TEST_LOSE_COMMIT_REPLY"));
    n > 0 && SEEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 == n
}

/// Whether pushed records may not move a started instance's state (see `test_setting`).
pub(super) fn no_record_push() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| test_setting("CHIMAERA_TEST_NO_RECORD_PUSH") == 1)
}

impl Core {
    /// A commit's answer carries the actor's record as committed. When this
    /// instance's state is older (its last commit landed, but the answer was
    /// lost, and so was the record its leader would have pushed after it, as
    /// when that leader died), take the record's state: the restore that
    /// follows puts back what was committed, and the next commit starts from
    /// its version. A record of another generation (another instance took
    /// over) is left alone. A state kept in chunks is not in the answer; this
    /// peer's replica of the actor's group may hold it whole.
    pub(super) fn adopt_committed(&mut self, actor: Id, answered: Option<&pb::ActorRecord>) {
        let Some(answered) = answered else { return };
        let whole = |r: &pb::ActorRecord| r.state_chunks.is_empty() || !r.durable_state.is_empty();
        let rec = if whole(answered) {
            Some(answered.clone())
        } else {
            self.router.read(actor.vshard(), |kv| whole_record(kv, actor)).flatten().filter(|r| whole(r))
        };
        let (Some(rec), Some(h)) = (rec, self.actors.get_mut(&actor)) else { return };
        if rec.generation == h.generation && rec.state_version > h.state_version {
            h.state_version = rec.state_version;
            h.durable_state = rec.durable_state;
            h.state_chunks = rec.state_chunks;
        }
    }

    /// A durable call whose commit did not go through is retried under its
    /// own sequence number: its handle's order waits for it again, as when
    /// its worker dies while it runs. (Nothing after it started: the commit
    /// held the actor's next call back.)
    pub(super) fn retry_in_order(&mut self, actor: Id, w: &Work) {
        if let Some(h) = self.actors.get_mut(&actor) {
            h.inflight.remove(&(w.spec.handle, w.spec.seq));
            let next = h.next_seq.entry(w.spec.handle).or_insert(0);
            *next = (*next).min(w.spec.seq);
        }
    }

    /// Between calls, rebuild an instance whose record now names a newer
    /// class: the executor restores the committed state with that class,
    /// whose `State(migrate=)` converts it. Restores go to the executor in
    /// order with calls, so the next call runs on the rebuilt instance.
    pub(super) fn actor_upgrade_now(&mut self, actor: Id, exec: u32) {
        let Some(h) = self.actors.get_mut(&actor) else { return };
        if !h.upgrade || !h.constructed || h.committing {
            return;
        }
        h.upgrade = false;
        self.send_restore(actor, exec);
    }

    /// Have the actor's executor restore its committed state with the class
    /// its record names. That class's code may not be here yet (a newer
    /// class's, or a restart on another peer): it is fetched now, and the
    /// executor waits for it before the calls that follow the restore.
    pub(super) fn send_restore(&mut self, actor: Id, exec: u32) {
        let Some(h) = self.actors.get(&actor) else { return };
        let digest = h.class_digest.clone();
        if !self.code.has(&digest)
            && let Some(mesh) = self.router.mesh.clone()
        {
            let (code, digest) = (self.code.clone(), digest.clone());
            tokio::spawn(async move {
                if let Err(e) = mesh.localize_code(&code, &digest).await {
                    tracing::warn!("fetching the code of actor {}'s class {}: {e}", actor.hex(), hex(&digest));
                }
            });
        }
        let msg = pb::ExecRestore {
            actor: actor.to_vec(),
            fn_digest: digest,
            state: h.durable_state.clone(),
            version: h.state_version,
        };
        self.sup.send(exec, pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Restore(msg)) });
    }
}

/// The actor's record, and for a live one its committed state whole, as its
/// host restores it: chunks put back together into `durable_state`.
pub(super) fn whole_record(kv: &dyn chimaera_core::kv::KvRead, actor: Id) -> Option<pb::ActorRecord> {
    let mut rec = read::actor(kv, actor)?;
    if !rec.state_chunks.is_empty()
        && rec.state() == pb::actor_record::State::Alive
        && let Some(state) = read::actor_state(kv, actor, &rec)
    {
        rec.durable_state = state;
    }
    Some(rec)
}

/// Commit a durable call's command with the state it left: inline when it
/// fits, else as chunks. Only chunks the actor does not hold already
/// (`stored`, its state's) are put, then the commit names them all; a
/// failure before that commit leaves the previous state whole.
pub(super) async fn commit(
    router: &Router,
    mut commit: pb::ActorCommandCommit,
    state: &[u8],
    stored: &[Vec<u8>],
) -> Commit {
    let actor = Id::from_slice(&commit.actor).unwrap_or_default();
    let answered = |r: Result<pb::ShardReply, chimaera_raft::ProposeError>| match r {
        Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Actor(a)) }) => Commit::Answered(Box::new(a)),
        Ok(other) => Commit::Unknown(format!("unexpected commit reply: {other:?}")),
        Err(e) => Commit::Unknown(e.to_string()),
    };
    if state.len() <= MAX_INLINE_STATE {
        commit.new_state = state.to_vec();
        let ack = answered(router.propose(actor.vshard(), pb::shard_cmd::Body::ActorCommit(commit)).await);
        if lose_this_reply() {
            return Commit::Unknown("test: the commit's answer was dropped".into());
        }
        return ack;
    }
    if state.len() > MAX_CHUNKED_STATE {
        return Commit::Answered(Box::new(pb::ActorAck {
            actor: commit.actor,
            status: pb::actor_ack::Status::Rejected as i32,
            reason: format!(
                "durable state of {} MiB exceeds the {} MiB limit",
                state.len() >> 20,
                MAX_CHUNKED_STATE >> 20
            ),
            ..Default::default()
        }));
    }
    let (root, puts) = chunks::plan(state, stored);
    for data in puts {
        if let Some(delay) = chunk_delay() {
            tokio::time::sleep(delay).await;
        }
        let put = pb::ActorChunkPut { actor: commit.actor.clone(), generation: commit.generation, data: data.to_vec() };
        match answered(router.propose(actor.vshard(), pb::shard_cmd::Body::ActorChunkPut(put)).await) {
            Commit::Answered(a) if a.status() == pb::actor_ack::Status::Ok => {}
            refused => return refused,
        }
    }
    let body = pb::ActorCommitRoot { commit: Some(commit), chunks: root };
    answered(router.propose(actor.vshard(), pb::shard_cmd::Body::ActorCommitRoot(body)).await)
}
