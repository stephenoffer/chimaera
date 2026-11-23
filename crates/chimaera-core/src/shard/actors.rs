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
        store(cx.kv, keys::actor(v, &id), &rec);
        cx.events.push(Event::ActorChanged(id));
        rec
    }

    pub(super) fn actor_commit(&self, cx: &mut Cx, v: u16, a: &pb::ActorCommandCommit) -> pb::ShardReply {
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
        rec.state_version += 1;
        rec.durable_state = a.new_state.clone();
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
}
