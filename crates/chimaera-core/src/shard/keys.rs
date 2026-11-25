//! Key layout of the shard state machine.
//!
//! Per-vshard keys start with the vshard (u16 big endian) and a tag byte, so a
//! vshard's whole state is one contiguous range that can be sealed, snapshotted
//! and adopted by another group. Group-wide keys use the reserved prefix 0xFFFF.

use crate::ids::Id;

pub const GROUP: u16 = 0xFFFF;

// Per-vshard tags.
pub const RUN: u8 = b'R';
pub const SUBMISSION_KEY: u8 = b'K';
pub const TASK: u8 = b'T';
pub const DEPENDENT: u8 = b'D'; // producer -> dependent in this vshard
pub const FORWARD: u8 = b'F'; // producer -> task that forwarded to it, in this vshard
pub const CHILD: u8 = b'P'; // parent -> child submitted from inside it, in this vshard
pub const REMOTE_CHILD: u8 = b'c'; // parent in another vshard -> child here; alone: the parent's cancellation
pub const STORE: u8 = b'S'; // ch.Store entry: namespace, key
pub const STORE_REV: u8 = b'Y'; // ch.Store namespace revision
pub const STORE_LEASE: u8 = b'L'; // ch.Store lock
pub const STORE_BARRIER: u8 = b'Z'; // ch.Store barrier
pub const STORE_LOG: u8 = b'J'; // ch.Store change log: namespace, revision
pub const STORE_LOG_META: u8 = b'U'; // ch.Store change log bounds
pub const STORE_CLAIM: u8 = b'q'; // ch.Store claims of a prefix: cursor, and keys by when their claim lapses
pub const SUBSCRIBER: u8 = b'B'; // producer -> remote subscriber vshard
pub const NOTIFIED: u8 = b'N'; // remote producer outcome received
pub const PINS: u8 = b'u'; // task -> its live pin count (callers, dependents, holds)
pub const HELD: u8 = b'h'; // producer -> remote subscriber vshard holding it: the hold's generation
pub const REMOTE_PINS: u8 = b'r'; // remote producer -> calls here still reading it, and the hold's generation
pub const GRANT: u8 = b'G'; // live attempt by peer incarnation
pub const READY: u8 = b'Q';
pub const ACTOR: u8 = b'A';
pub const ACTOR_HOST: u8 = b'H';
pub const ACTOR_CMD: u8 = b'C';
pub const ACTOR_CHUNK: u8 = b'a'; // actor, digest -> a chunk of its durable state; actor alone -> their digests
pub const BUDGET: u8 = b'W'; // budgeted call -> CPU and GPU seconds charged to it, in this vshard
pub const DEPS_LEFT: u8 = b'I'; // pending call -> prerequisites it still waits on
pub const DEAD: u8 = b'd'; // run, done hlc, task -> a call that failed for good (DeadLetter), oldest first
pub const DEAD_COUNT: u8 = b'k'; // run -> how many dead letters it keeps
pub const LEASE: u8 = b'l'; // run, file, holder (peer, incarnation, id) -> a hold on a stored file (leases.rs)
pub const LEASE_HOLDER: u8 = b'o'; // the same hold by holder: peer, incarnation, run, file, id

// Group-wide tags.
pub const OWNED: u8 = b'V';
pub const RETIRED: u8 = b'X';
pub const OUTBOX: u8 = b'O';
pub const META: u8 = b'M';
pub const EXPIRY: u8 = b'E';

pub const META_LAST_HLC: &[u8] = b"hlc";
pub const META_OUTBOX_SEQ: &[u8] = b"outbox_seq";

fn base(vshard: u16, tag: u8, cap: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(3 + cap);
    k.extend_from_slice(&vshard.to_be_bytes());
    k.push(tag);
    k
}

pub fn run(v: u16, run: &Id) -> Vec<u8> {
    let mut k = base(v, RUN, 16);
    k.extend_from_slice(&run.0);
    k
}

pub fn submission_key(v: u16, tenant: &str, key: &str) -> Vec<u8> {
    let mut k = base(v, SUBMISSION_KEY, tenant.len() + key.len() + 1);
    k.extend_from_slice(tenant.as_bytes());
    k.push(0);
    k.extend_from_slice(key.as_bytes());
    k
}

pub fn task(v: u16, run: &Id, task: &Id) -> Vec<u8> {
    let mut k = base(v, TASK, 32);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(&task.0);
    k
}

/// How many prerequisites a pending call still waits on: kept apart from its
/// record so each finished prerequisite rewrites four bytes, not the call's
/// whole spec.
pub fn deps_left(v: u16, run: &Id, task: &Id) -> Vec<u8> {
    let mut k = base(v, DEPS_LEFT, 32);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(&task.0);
    k
}

/// A dead letter, ordered by when its call ended within its run.
pub fn dead(v: u16, run: &Id, done_hlc: u64, task: &Id) -> Vec<u8> {
    let mut k = base(v, DEAD, 40);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(&done_hlc.to_be_bytes());
    k.extend_from_slice(&task.0);
    k
}

pub fn dead_count(v: u16, run: &Id) -> Vec<u8> {
    pairs_of_run(v, DEAD_COUNT, run)
}

pub fn tasks_of_run(v: u16, run: &Id) -> Vec<u8> {
    let mut k = base(v, TASK, 16);
    k.extend_from_slice(&run.0);
    k
}

pub fn pair(v: u16, tag: u8, a: (&Id, &Id), b: (&Id, &Id)) -> Vec<u8> {
    let mut k = base(v, tag, 64);
    k.extend_from_slice(&a.0.0);
    k.extend_from_slice(&a.1.0);
    k.extend_from_slice(&b.0.0);
    k.extend_from_slice(&b.1.0);
    k
}

pub fn of_producer(v: u16, tag: u8, producer: (&Id, &Id)) -> Vec<u8> {
    let mut k = base(v, tag, 32);
    k.extend_from_slice(&producer.0.0);
    k.extend_from_slice(&producer.1.0);
    k
}

pub fn subscriber(v: u16, producer: (&Id, &Id), sub_vshard: u16) -> Vec<u8> {
    let mut k = of_producer(v, SUBSCRIBER, producer);
    k.extend_from_slice(&sub_vshard.to_be_bytes());
    k
}

pub fn held(v: u16, producer: (&Id, &Id), sub_vshard: u16) -> Vec<u8> {
    let mut k = of_producer(v, HELD, producer);
    k.extend_from_slice(&sub_vshard.to_be_bytes());
    k
}

pub fn notified(v: u16, producer: (&Id, &Id)) -> Vec<u8> {
    of_producer(v, NOTIFIED, producer)
}

pub fn grants_of(v: u16, peer: &Id, inc: u64) -> Vec<u8> {
    let mut k = base(v, GRANT, 24);
    k.extend_from_slice(&peer.0);
    k.extend_from_slice(&inc.to_be_bytes());
    k
}

pub fn grant(v: u16, peer: &Id, inc: u64, run: &Id, task: &Id, attempt: u32) -> Vec<u8> {
    let mut k = grants_of(v, peer, inc);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(&task.0);
    k.extend_from_slice(&attempt.to_be_bytes());
    k
}

/// Every `tag` pair entry whose first task belongs to `run` (for run expiry).
pub fn pairs_of_run(v: u16, tag: u8, run: &Id) -> Vec<u8> {
    let mut k = base(v, tag, 16);
    k.extend_from_slice(&run.0);
    k
}

/// The leases on `file` of `run` (every lease on the run's files: `pairs_of_run(v, LEASE, run)`).
pub fn leases_of(v: u16, run: &Id, file: &[u8]) -> Vec<u8> {
    let mut k = base(v, LEASE, 32 + 32);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(file);
    k
}

/// A holder's lease on `file` of `run`.
pub fn lease(v: u16, run: &Id, file: &[u8], peer: &Id, inc: u64, holder: u64) -> Vec<u8> {
    let mut k = leases_of(v, run, file);
    k.extend_from_slice(&peer.0);
    k.extend_from_slice(&inc.to_be_bytes());
    k.extend_from_slice(&holder.to_be_bytes());
    k
}

/// The leases held from a peer incarnation (for its retirement).
pub fn leases_held_by(v: u16, peer: &Id, inc: u64) -> Vec<u8> {
    let mut k = base(v, LEASE_HOLDER, 24 + 40);
    k.extend_from_slice(&peer.0);
    k.extend_from_slice(&inc.to_be_bytes());
    k
}

/// `lease`, indexed by its holder.
pub fn lease_by_holder(v: u16, run: &Id, file: &[u8], peer: &Id, inc: u64, holder: u64) -> Vec<u8> {
    let mut k = leases_held_by(v, peer, inc);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(file);
    k.extend_from_slice(&holder.to_be_bytes());
    k
}

/// A ch.Store key: `tag`, the length-prefixed namespace id, then `key`.
pub fn store(v: u16, tag: u8, namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut k = base(v, tag, namespace.len() + key.len());
    k.extend_from_slice(namespace);
    k.extend_from_slice(key);
    k
}

pub fn ready(v: u16, run: &Id, task: &Id) -> Vec<u8> {
    let mut k = base(v, READY, 32);
    k.extend_from_slice(&run.0);
    k.extend_from_slice(&task.0);
    k
}

pub fn actor(v: u16, actor: &Id) -> Vec<u8> {
    let mut k = base(v, ACTOR, 16);
    k.extend_from_slice(&actor.0);
    k
}

pub fn actors_hosted_by(v: u16, peer: &Id, inc: u64) -> Vec<u8> {
    let mut k = base(v, ACTOR_HOST, 24);
    k.extend_from_slice(&peer.0);
    k.extend_from_slice(&inc.to_be_bytes());
    k
}

pub fn actor_host(v: u16, peer: &Id, inc: u64, actor: &Id) -> Vec<u8> {
    let mut k = actors_hosted_by(v, peer, inc);
    k.extend_from_slice(&actor.0);
    k
}

pub fn actor_cmd(v: u16, actor: &Id, cmd_id: &[u8]) -> Vec<u8> {
    let mut k = base(v, ACTOR_CMD, 16 + cmd_id.len());
    k.extend_from_slice(&actor.0);
    k.extend_from_slice(cmd_id);
    k
}

/// The digests of every chunk stored for the actor (32 bytes each, in the
/// order put), so a commit checks and drops chunks without reading them;
/// each chunk is under this key and its digest (`actor_chunk`).
pub fn actor_chunks(v: u16, actor: &Id) -> Vec<u8> {
    let mut k = base(v, ACTOR_CHUNK, 48);
    k.extend_from_slice(&actor.0);
    k
}

pub fn actor_chunk(v: u16, actor: &Id, digest: &[u8]) -> Vec<u8> {
    let mut k = actor_chunks(v, actor);
    k.extend_from_slice(digest);
    k
}

pub fn owned(vshard: u16) -> Vec<u8> {
    let mut k = base(GROUP, OWNED, 2);
    k.extend_from_slice(&vshard.to_be_bytes());
    k
}

pub fn owned_prefix() -> Vec<u8> {
    base(GROUP, OWNED, 0)
}

pub fn retired(peer: &Id, inc: u64) -> Vec<u8> {
    let mut k = base(GROUP, RETIRED, 24);
    k.extend_from_slice(&peer.0);
    k.extend_from_slice(&inc.to_be_bytes());
    k
}

pub fn outbox(seq: u64) -> Vec<u8> {
    let mut k = base(GROUP, OUTBOX, 8);
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

pub fn outbox_prefix() -> Vec<u8> {
    base(GROUP, OUTBOX, 0)
}

pub fn meta(name: &[u8]) -> Vec<u8> {
    let mut k = base(GROUP, META, name.len());
    k.extend_from_slice(name);
    k
}

/// Expiry index entry: `hlc` first so a prefix scan visits the oldest first.
pub fn expiry(hlc: u64, target: &[u8]) -> Vec<u8> {
    let mut k = base(GROUP, EXPIRY, 8 + target.len());
    k.extend_from_slice(&hlc.to_be_bytes());
    k.extend_from_slice(target);
    k
}

pub fn expiry_prefix() -> Vec<u8> {
    base(GROUP, EXPIRY, 0)
}

/// Split a key of the form `prefix ++ id ++ id` (32 trailing bytes).
pub fn trailing_pair(key: &[u8]) -> (Id, Id) {
    let n = key.len();
    (Id::from_slice(&key[n - 32..n - 16]).unwrap_or_default(), Id::from_slice(&key[n - 16..]).unwrap_or_default())
}
