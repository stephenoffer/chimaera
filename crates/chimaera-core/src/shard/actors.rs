//! Actor lifecycle and durable command commits.

use super::*;

impl ShardState {
    pub(super) fn actor_reply(status: AStatus, rec: Option<&pb::ActorRecord>, actor: &[u8]) -> pb::ShardReply {
        reply(pb::shard_reply::Body::Actor(pb::ActorAck {
            actor: actor.to_vec(),
            status: status as i32,
            record: rec.cloned(),
            version: rec.map_or(0, |r| r.state_version),
            ..Default::default()
        }))
    }

    pub(super) fn actor_create(&self, cx: &mut Cx, v: u16, a: &pb::ActorCreate) -> pb::ShardReply {
        let Some(spec) = a.spec.clone() else { return rejected("missing actor spec") };
        let Ok(id) = Id::from_slice(&spec.actor) else { return rejected("bad actor id") };
        if id.vshard() != v {
            return rejected("actor id routes to another vshard");
        }
        let key = keys::actor(v, &id);
        if let Some(rec) = load::<pb::ActorRecord>(cx.kv, &key) {
            return Self::actor_reply(AStatus::Existing, Some(&rec), &spec.actor);
        }
        let mut rec = pb::ActorRecord { spec: Some(spec.clone()), ..Default::default() };
        rec.set_state(AState::Pending);
        store(cx.kv, key, &rec);
        cx.events.push(Event::ActorChanged(id));
        Self::actor_reply(AStatus::Ok, Some(&rec), &spec.actor)
    }

    pub(super) fn actor_place(&self, cx: &mut Cx, v: u16, a: &pb::ActorPlace) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let key = keys::actor(v, &id);
        let Some(mut rec) = load::<pb::ActorRecord>(cx.kv, &key) else {
            return Self::actor_reply(AStatus::NotFound, None, &a.actor);
        };
        if rec.state() == AState::Dead {
            return Self::actor_reply(AStatus::Dead, Some(&rec), &a.actor);
        }
        if rec.generation != a.expected_generation {
            return Self::actor_reply(AStatus::StaleGeneration, Some(&rec), &a.actor);
        }
        let Some(host) = peer_of(&a.host) else { return rejected("missing host") };
        if self.is_retired(cx.kv, host) {
            return Self::actor_reply(AStatus::StaleGeneration, Some(&rec), &a.actor);
        }
        if let Some(old) = peer_of(&rec.host) {
            cx.kv.delete(&keys::actor_host(v, &old.0, old.1, &id));
        }
        rec.generation += 1;
        rec.host = Some(peer_pb(host));
        rec.set_state(AState::Alive);
        // Chunks of a commit the previous instance did not finish: a new
        // instance never commits them.
        Self::drop_unnamed_chunks(cx, v, &id, &rec.state_chunks);
        cx.kv.put(keys::actor_host(v, &host.0, host.1, &id), Vec::new());
        store(cx.kv, key, &rec);
        cx.events.push(Event::ActorChanged(id));
        Self::actor_reply(AStatus::Ok, Some(&rec), &a.actor)
    }

    pub(super) fn actor_died(&self, cx: &mut Cx, v: u16, a: &pb::ActorDied) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let Some(rec) = load::<pb::ActorRecord>(cx.kv, &keys::actor(v, &id)) else {
            return Self::actor_reply(AStatus::NotFound, None, &a.actor);
        };
        if rec.generation != a.generation || rec.state() == AState::Dead {
            return Self::actor_reply(AStatus::StaleGeneration, Some(&rec), &a.actor);
        }
        if let Some(old) = peer_of(&rec.host) {
            cx.kv.delete(&keys::actor_host(v, &old.0, old.1, &id));
        }
        let reason = a.reason.clone().unwrap_or_default();
        let rec = if a.terminal {
            self.actor_ended(cx, v, id, rec, reason)
        } else {
            self.actor_lost(cx, v, id, rec, reason, !a.moved)
        };
        Self::actor_reply(AStatus::Ok, Some(&rec), &a.actor)
    }

    pub(super) fn actor_lost(
        &self,
        cx: &mut Cx,
        v: u16,
        id: Id,
        mut rec: pb::ActorRecord,
        reason: pb::ErrorRecord,
        counted: bool,
    ) -> pb::ActorRecord {
        let spec = rec.spec.clone().unwrap_or_default();
        let restart = spec.restart() != pb::actor_spec::Restart::Never || spec.durable;
        let max = if spec.max_restarts == 0 { DEFAULT_MAX_ATTEMPTS } else { spec.max_restarts };
        rec.host = None;
        // A move (its host drained) restarts it without using up a restart.
        if restart && (rec.restarts < max || !counted) {
            if counted {
                rec.restarts += 1;
            }
            rec.set_state(AState::Restarting);
        } else {
            rec.set_state(AState::Dead);
            rec.death = Some(reason);
            Self::forget_state(cx, v, &id, &mut rec);
        }
        store(cx.kv, keys::actor(v, &id), &rec);
        cx.events.push(Event::ActorChanged(id));
        rec
    }

    /// End an actor on purpose: dead regardless of its restart policy.
    pub(super) fn actor_ended(
        &self,
        cx: &mut Cx,
        v: u16,
        id: Id,
        mut rec: pb::ActorRecord,
        reason: pb::ErrorRecord,
    ) -> pb::ActorRecord {
        rec.host = None;
        rec.set_state(AState::Dead);
        rec.death = Some(reason);
        Self::forget_state(cx, v, &id, &mut rec);
        store(cx.kv, keys::actor(v, &id), &rec);
        cx.events.push(Event::ActorChanged(id));
        rec
    }

    pub(super) fn actor_commit(&self, cx: &mut Cx, v: u16, a: &pb::ActorCommandCommit) -> pb::ShardReply {
        self.commit_state(cx, v, a, None)
    }

    pub(super) fn actor_commit_root(&self, cx: &mut Cx, v: u16, a: &pb::ActorCommitRoot) -> pb::ShardReply {
        let Some(commit) = &a.commit else { return rejected("missing commit") };
        let reply = self.commit_state(cx, v, commit, Some(&a.chunks));
        // Committed or refused, the chunks put for it that no state names go:
        // an instance commits one call at a time, so they are no other
        // commit's. (A stale instance leaves them to the newer one's placement.)
        if let Ok(id) = Id::from_slice(&commit.actor)
            && let Some(rec) = load::<pb::ActorRecord>(cx.kv, &keys::actor(v, &id))
            && rec.generation == commit.generation
        {
            Self::drop_unnamed_chunks(cx, v, &id, &rec.state_chunks);
        }
        reply
    }

    /// Commit a durable actor's command and the state it left: `a.new_state`
    /// inline, or the `chunks` already put (ActorChunkPut). Either replaces
    /// the previous state whole.
    fn commit_state(
        &self,
        cx: &mut Cx,
        v: u16,
        a: &pb::ActorCommandCommit,
        chunks: Option<&[Vec<u8>]>,
    ) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let key = keys::actor(v, &id);
        let Some(mut rec) = load::<pb::ActorRecord>(cx.kv, &key) else {
            return Self::actor_reply(AStatus::NotFound, None, &a.actor);
        };
        let with =
            |status: AStatus, rec: &pb::ActorRecord, reply_obj: Option<pb::ObjectRef>, version: u64, reason: &str| {
                reply(pb::shard_reply::Body::Actor(pb::ActorAck {
                    actor: a.actor.clone(),
                    status: status as i32,
                    record: Some(rec.clone()),
                    reply: reply_obj,
                    version,
                    reason: reason.into(),
                }))
            };
        if rec.state() == AState::Dead {
            return with(AStatus::Dead, &rec, None, rec.state_version, "actor is dead");
        }
        if !rec.spec.as_ref().is_some_and(|s| s.durable) {
            return with(AStatus::Rejected, &rec, None, rec.state_version, "actor does not have durable state");
        }
        let ckey = keys::actor_cmd(v, &id, &a.cmd_id);
        #[cfg(test)]
        let dedup = !self.mutants.skip_command_dedup;
        #[cfg(not(test))]
        let dedup = true;
        if let Some(prev) = load::<pb::ActorCommandRecord>(cx.kv, &ckey).filter(|_| dedup) {
            return if prev.fingerprint == a.fingerprint {
                with(AStatus::Duplicate, &rec, prev.reply, prev.version, "")
            } else {
                with(AStatus::Conflict, &rec, None, prev.version, "command id reused with a different request")
            };
        }
        if rec.generation != a.generation {
            return with(AStatus::StaleGeneration, &rec, None, rec.state_version, "stale actor generation");
        }
        if a.prev_version != rec.state_version {
            return with(AStatus::VersionConflict, &rec, None, rec.state_version, "state version moved");
        }
        if a.new_state.len() > MAX_INLINE_STATE {
            return with(
                AStatus::Rejected,
                &rec,
                None,
                rec.state_version,
                "durable state exceeds the 1 MiB inline limit",
            );
        }
        let named = chunks.unwrap_or_default();
        if named.len() > MAX_STATE_CHUNKS {
            return with(AStatus::Rejected, &rec, None, rec.state_version, "durable state has too many chunks");
        }
        let index = cx.kv.get(&keys::actor_chunks(v, &id)).unwrap_or_default();
        let stored: std::collections::HashSet<&[u8]> = index.chunks(32).collect();
        if named.iter().any(|d| !stored.contains(d.as_slice())) {
            return with(AStatus::Rejected, &rec, None, rec.state_version, "a chunk of the durable state is missing");
        }
        rec.state_version += 1;
        rec.durable_state = a.new_state.clone();
        rec.state_chunks = named.to_vec();
        if chunks.is_none() {
            // Inline now: the previous state's chunks go (a chunked commit
            // drops what it no longer names, actor_commit_root).
            Self::drop_unnamed_chunks(cx, v, &id, &[]);
        }
        rec.commands_retained += 1;
        let cmd = pb::ActorCommandRecord {
            cmd_id: a.cmd_id.clone(),
            fingerprint: a.fingerprint.clone(),
            version: rec.state_version,
            reply: a.reply.clone(),
            committed_hlc: cx.now,
        };
        store(cx.kv, ckey.clone(), &cmd);
        cx.kv.put(keys::expiry(cx.now + hlc::from_ms(ACTOR_CMD_RETENTION_MS), &ckey), Vec::new());
        store(cx.kv, key, &rec);
        with(AStatus::Ok, &rec, a.reply.clone(), rec.state_version, "")
    }

    pub(super) fn actor_checkpoint(&self, cx: &mut Cx, v: u16, a: &pb::ActorCheckpoint) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let key = keys::actor(v, &id);
        let Some(mut rec) = load::<pb::ActorRecord>(cx.kv, &key) else {
            return Self::actor_reply(AStatus::NotFound, None, &a.actor);
        };
        if rec.generation != a.generation {
            return Self::actor_reply(AStatus::StaleGeneration, Some(&rec), &a.actor);
        }
        if a.version > rec.checkpoint_version {
            rec.checkpoint_version = a.version;
            rec.checkpoint = a.snapshot.clone();
            store(cx.kv, key, &rec);
        }
        Self::actor_reply(AStatus::Ok, Some(&rec), &a.actor)
    }

    /// Store a chunk of a durable actor's next state under its digest. The
    /// reply carries no record: a large state takes many puts.
    pub(super) fn actor_chunk_put(&self, cx: &mut Cx, v: u16, a: &pb::ActorChunkPut) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let status = match load::<pb::ActorRecord>(cx.kv, &keys::actor(v, &id)) {
            None => AStatus::NotFound,
            Some(rec) if rec.state() == AState::Dead => AStatus::Dead,
            Some(rec) if !rec.spec.as_ref().is_some_and(|s| s.durable) || a.data.len() > MAX_INLINE_STATE => {
                AStatus::Rejected
            }
            Some(rec) if rec.generation != a.generation => AStatus::StaleGeneration,
            Some(_) => {
                // A chunk stored already (its state's, or put by a commit
                // tried before) is not written again.
                let (digest, key) = (blake3::hash(&a.data), keys::actor_chunks(v, &id));
                let mut index = cx.kv.get(&key).unwrap_or_default();
                if !index.chunks(32).any(|d| d == digest.as_bytes()) {
                    cx.kv.put(keys::actor_chunk(v, &id, digest.as_bytes()), a.data.clone());
                    index.extend_from_slice(digest.as_bytes());
                    cx.kv.put(key, index);
                }
                AStatus::Ok
            }
        };
        reply(pb::shard_reply::Body::Actor(pb::ActorAck {
            actor: a.actor.clone(),
            status: status as i32,
            ..Default::default()
        }))
    }

    /// Bind a live durable actor to a class whose state version is newer than
    /// its record's. An older or equal version changes nothing (Existing), so
    /// creators running older code never undo an upgrade.
    pub(super) fn actor_upgrade(&self, cx: &mut Cx, v: u16, a: &pb::ActorUpgrade) -> pb::ShardReply {
        let Ok(id) = Id::from_slice(&a.actor) else { return rejected("bad actor id") };
        let key = keys::actor(v, &id);
        let Some(mut rec) = load::<pb::ActorRecord>(cx.kv, &key) else {
            return Self::actor_reply(AStatus::NotFound, None, &a.actor);
        };
        if rec.state() == AState::Dead {
            return Self::actor_reply(AStatus::Dead, Some(&rec), &a.actor);
        }
        let spec = rec.spec.get_or_insert_default();
        if !spec.durable || a.class_digest.is_empty() {
            return Self::actor_reply(AStatus::Rejected, Some(&rec), &a.actor);
        }
        // A record from before versions (0) holds version 1's state.
        if a.state_schema <= spec.state_schema.max(1) {
            return Self::actor_reply(AStatus::Existing, Some(&rec), &a.actor);
        }
        spec.class_digest = a.class_digest.clone();
        spec.state_schema = a.state_schema;
        spec.methods = a.methods.clone();
        store(cx.kv, key, &rec);
        cx.events.push(Event::ActorChanged(id));
        Self::actor_reply(AStatus::Ok, Some(&rec), &a.actor)
    }

    /// Delete the actor's stored chunks other than `keep` (its state's),
    /// found from their list (keys::actor_chunks) without reading them.
    fn drop_unnamed_chunks(cx: &mut Cx, v: u16, id: &Id, keep: &[Vec<u8>]) {
        let key = keys::actor_chunks(v, id);
        let Some(index) = cx.kv.get(&key) else { return };
        let keep: std::collections::HashSet<&[u8]> = keep.iter().map(Vec::as_slice).collect();
        let (kept, dropped): (Vec<&[u8]>, Vec<&[u8]>) = index.chunks(32).partition(|d| keep.contains(d));
        if dropped.is_empty() {
            return;
        }
        for digest in dropped {
            cx.kv.delete(&keys::actor_chunk(v, id, digest));
        }
        if kept.is_empty() { cx.kv.delete(&key) } else { cx.kv.put(key, kept.concat()) }
    }

    /// A dead actor is never restored: its state goes, inline or in chunks
    /// (with those of a commit it did not finish), and its record stays.
    fn forget_state(cx: &mut Cx, v: u16, id: &Id, rec: &mut pb::ActorRecord) {
        rec.durable_state.clear();
        rec.state_chunks.clear();
        Self::drop_unnamed_chunks(cx, v, id, &[]);
    }
}
