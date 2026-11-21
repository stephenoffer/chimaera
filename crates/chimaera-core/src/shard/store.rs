//! `ch.Store`: replicated keys, counters, leases and barriers in a namespace.
//!
//! A namespace lives in one vshard, so every command on it is applied in log
//! order: reads and writes across its keys are linearizable. Every change
//! bumps the namespace's revision; an entry's version is the revision of its
//! last change, so `if_version` is a compare-and-set and 0 means "absent".
//! Lock grants also bump it, which makes their fencing tokens grow
//! monotonically: a write carrying a stale token is refused by the shard
//! itself. A bounded change log serves watches; a watcher behind it is told
//! to rescan (`compacted`). Expiry compares against the command's HLC, so
//! every replica decides it the same way.

use super::{Cx, keys, load};
use crate::kv::KvRead;
use chimaera_proto::v1 as pb;
use pb::store_read::Kind as ReadKind;
use pb::store_write::Op;
use prost::Message;

pub const MAX_VALUE: usize = 1 << 20;
const LOG_ENTRIES: u64 = 4096;
const LOG_BYTES: u64 = 64 << 20;
const SCAN_LIMIT: usize = 1000;

/// The namespace id inside keys: length-prefixed tenant and name.
fn namespace(tenant: &str, name: &str) -> Vec<u8> {
    let mut n = Vec::with_capacity(4 + tenant.len() + name.len());
    for part in [tenant, name] {
        n.extend_from_slice(&(part.len() as u16).to_be_bytes());
        n.extend_from_slice(part.as_bytes());
    }
    n
}

fn fail(error: impl Into<String>, revision: u64) -> pb::StoreReply {
    pb::StoreReply { ok: false, error: error.into(), revision, ..Default::default() }
}

fn visible(e: &pb::StoreEntry, now: u64) -> bool {
    e.expires_hlc == 0 || e.expires_hlc > now
}

fn revision(kv: &dyn KvRead, v: u16, ns: &[u8]) -> u64 {
    kv.get(&keys::store(v, keys::STORE_REV, ns, b""))
        .map_or(0, |b| u64::from_be_bytes(b[..8].try_into().unwrap_or([0; 8])))
}

fn bump(cx: &mut Cx, v: u16, ns: &[u8]) -> u64 {
    let next = revision(cx.kv, v, ns) + 1;
    cx.kv.put(keys::store(v, keys::STORE_REV, ns, b""), next.to_be_bytes().to_vec());
    next
}

/// (highest trimmed revision, entries, bytes) of the namespace's change log.
fn log_meta(kv: &dyn KvRead, v: u16, ns: &[u8]) -> (u64, u64, u64) {
    let Some(b) = kv.get(&keys::store(v, keys::STORE_LOG_META, ns, b"")) else { return (0, 0, 0) };
    let at = |i: usize| u64::from_be_bytes(b[i * 8..i * 8 + 8].try_into().expect("24-byte log meta"));
    (at(0), at(1), at(2))
}

fn log_append(cx: &mut Cx, v: u16, ns: &[u8], change: pb::StoreChange) {
    let (mut trimmed, mut count, mut bytes) = log_meta(cx.kv, v, ns);
    let size = (change.key.len() + change.value.len()) as u64;
    super::store(cx.kv, keys::store(v, keys::STORE_LOG, ns, &change.revision.to_be_bytes()), &change);
    count += 1;
    bytes += size;
    let prefix = keys::store(v, keys::STORE_LOG, ns, b"");
    while count > 1 && (count > LOG_ENTRIES || bytes > LOG_BYTES) {
        let Some((oldest, raw)) = cx.kv.scan_prefix(&prefix, 1).into_iter().next() else { break };
        let old = pb::StoreChange::decode(raw.as_slice()).unwrap_or_default();
        cx.kv.delete(&oldest);
        trimmed = trimmed.max(old.revision);
        count -= 1;
        bytes -= (old.key.len() + old.value.len()) as u64;
    }
    let meta = [trimmed, count, bytes].iter().flat_map(|x| x.to_be_bytes()).collect();
    cx.kv.put(keys::store(v, keys::STORE_LOG_META, ns, b""), meta);
}

/// Apply one write; the reply says whether it took effect and why not.
pub(super) fn write(cx: &mut Cx, v: u16, w: &pb::StoreWrite) -> pb::StoreReply {
    let ns = namespace(&w.tenant, &w.namespace);
    let now = cx.now;
    let rev = revision(cx.kv, v, &ns);
    if let Some(f) = &w.fence {
        let lease: Option<pb::StoreLease> = load(cx.kv, &keys::store(v, keys::STORE_LEASE, &ns, f.lock.as_bytes()));
        if !lease.is_some_and(|l| l.token == f.token && l.expires_hlc > now) {
            return fail(format!("fenced: the lease on {:?} with token {} is no longer held", f.lock, f.token), rev);
        }
    }
    let ekey = keys::store(v, keys::STORE, &ns, w.key.as_bytes());
    let current: Option<pb::StoreEntry> = load::<pb::StoreEntry>(cx.kv, &ekey).filter(|e| visible(e, now));
    let version_ok = !w.has_if_version || current.as_ref().map_or(0, |e| e.version) == w.if_version;
    let ttl_expiry = |ttl_ms: u64| if ttl_ms == 0 { 0 } else { now + crate::hlc::from_ms(ttl_ms) };
    match w.op() {
        Op::Put | Op::Delete if !version_ok => pb::StoreReply {
            ok: false,
            error: format!("version mismatch: {:?} is at version {}", w.key, current.as_ref().map_or(0, |e| e.version)),
            revision: rev,
            entry: current,
            ..Default::default()
        },
        Op::Put if w.value.len() > MAX_VALUE => fail(
            format!("value of {} bytes is over the 1 MiB limit; store a ch.put future instead", w.value.len()),
            rev,
        ),
        Op::Put => {
            let r = bump(cx, v, &ns);
            if let Some(old) = load::<pb::StoreEntry>(cx.kv, &ekey).filter(|e| e.expires_hlc != 0) {
                cx.kv.delete(&keys::expiry(old.expires_hlc, &ekey));
            }
            let entry = pb::StoreEntry {
                key: w.key.clone(),
                value: w.value.clone(),
                version: r,
                created: current.as_ref().map_or(r, |e| e.created),
                expires_hlc: ttl_expiry(w.ttl_ms),
                ..Default::default()
            };
            if entry.expires_hlc != 0 {
                cx.kv.put(keys::expiry(entry.expires_hlc, &ekey), Vec::new());
            }
            super::store(cx.kv, ekey, &entry);
            log_append(
                cx,
                v,
                &ns,
                pb::StoreChange { revision: r, key: w.key.clone(), value: w.value.clone(), ..Default::default() },
            );
            pb::StoreReply { ok: true, revision: r, entry: Some(entry), ..Default::default() }
        }
        Op::Delete => {
            let Some(old) = current else { return fail(format!("{:?} does not exist", w.key), rev) };
            let r = bump(cx, v, &ns);
            cx.kv.delete(&ekey);
            if old.expires_hlc != 0 {
                cx.kv.delete(&keys::expiry(old.expires_hlc, &ekey));
            }
            log_append(
                cx,
                v,
                &ns,
                pb::StoreChange { revision: r, key: w.key.clone(), deleted: true, ..Default::default() },
            );
            pb::StoreReply { ok: true, revision: r, entry: Some(old), ..Default::default() }
        }
        Op::Add => {
            if current.as_ref().is_some_and(|e| !e.is_counter) {
                return fail(format!("{:?} holds a value, not a counter", w.key), rev);
            }
            let r = bump(cx, v, &ns);
            let value = current.as_ref().map_or(0, |e| e.counter).wrapping_add(w.delta);
            let entry = pb::StoreEntry {
                key: w.key.clone(),
                version: r,
                created: current.as_ref().map_or(r, |e| e.created),
                is_counter: true,
                counter: value,
                ..Default::default()
            };
            super::store(cx.kv, ekey, &entry);
            log_append(
                cx,
                v,
                &ns,
                pb::StoreChange {
                    revision: r,
                    key: w.key.clone(),
                    is_counter: true,
                    counter: value,
                    ..Default::default()
                },
            );
            pb::StoreReply { ok: true, revision: r, entry: Some(entry), ..Default::default() }
        }
        Op::Lock | Op::Renew | Op::Unlock => lease(cx, v, &ns, w, rev),
        Op::Arrive => arrive(cx, v, &ns, w, rev),
    }
}

fn lease(cx: &mut Cx, v: u16, ns: &[u8], w: &pb::StoreWrite, rev: u64) -> pb::StoreReply {
    let now = cx.now;
    let lkey = keys::store(v, keys::STORE_LEASE, ns, w.key.as_bytes());
    let held: Option<pb::StoreLease> = load::<pb::StoreLease>(cx.kv, &lkey).filter(|l| l.expires_hlc > now);
    let expiry = now + crate::hlc::from_ms(w.ttl_ms.max(1));
    let mine = |l: &pb::StoreLease| l.holder == w.holder && (w.op() == Op::Lock || l.token == w.token);
    match (w.op(), held) {
        (Op::Lock, Some(l)) if l.holder != w.holder => pb::StoreReply {
            ok: false,
            error: format!("held by {}", l.holder),
            revision: rev,
            lease: Some(l),
            ..Default::default()
        },
        (Op::Lock, None) => {
            let token = bump(cx, v, ns);
            let l = pb::StoreLease { holder: w.holder.clone(), token, expires_hlc: expiry };
            super::store(cx.kv, lkey, &l);
            pb::StoreReply { ok: true, revision: token, lease: Some(l), ..Default::default() }
        }
        (Op::Lock | Op::Renew, Some(mut l)) if mine(&l) => {
            l.expires_hlc = expiry;
            super::store(cx.kv, lkey, &l);
            pb::StoreReply { ok: true, revision: rev, lease: Some(l), ..Default::default() }
        }
        (Op::Unlock, Some(l)) if mine(&l) => {
            cx.kv.delete(&lkey);
            pb::StoreReply { ok: true, revision: rev, lease: Some(l), ..Default::default() }
        }
        (_, l) => pb::StoreReply {
            ok: false,
            error: format!("the lease on {:?} is no longer held by {}", w.key, w.holder),
            revision: rev,
            lease: l,
            ..Default::default()
        },
    }
}

fn arrive(cx: &mut Cx, v: u16, ns: &[u8], w: &pb::StoreWrite, rev: u64) -> pb::StoreReply {
    if w.parties == 0 {
        return fail("a barrier needs at least one party", rev);
    }
    let bkey = keys::store(v, keys::STORE_BARRIER, ns, w.key.as_bytes());
    let mut b: pb::StoreBarrier =
        load(cx.kv, &bkey).unwrap_or(pb::StoreBarrier { parties: w.parties, ..Default::default() });
    if b.parties != w.parties {
        return fail(format!("barrier {:?} has {} parties, not {}", w.key, b.parties, w.parties), rev);
    }
    b.arrived += 1;
    if b.arrived == b.parties {
        b.arrived = 0;
        b.generation += 1;
    }
    super::store(cx.kv, bkey, &b);
    pb::StoreReply { ok: true, revision: rev, barrier: Some(b), ..Default::default() }
}

/// A read at `now` (the command's HLC when applied through the log, the
/// peer's clock for a local replica read).
pub fn read(kv: &dyn KvRead, v: u16, r: &pb::StoreRead, now: u64) -> pb::StoreReply {
    let ns = namespace(&r.tenant, &r.namespace);
    let rev = revision(kv, v, &ns);
    let limit = if r.limit == 0 { SCAN_LIMIT } else { (r.limit as usize).min(SCAN_LIMIT) };
    let mut reply = pb::StoreReply { ok: true, revision: rev, ..Default::default() };
    match r.kind() {
        ReadKind::Get => {
            reply.entry = load::<pb::StoreEntry>(kv, &keys::store(v, keys::STORE, &ns, r.key.as_bytes()))
                .filter(|e| visible(e, now));
            reply.ok = reply.entry.is_some();
        }
        ReadKind::Scan => {
            let prefix = keys::store(v, keys::STORE, &ns, r.key.as_bytes());
            let base = keys::store(v, keys::STORE, &ns, b"").len();
            let after = keys::store(v, keys::STORE, &ns, r.start_after.as_bytes());
            for (k, raw) in kv.scan_prefix(&prefix, usize::MAX) {
                if !r.start_after.is_empty() && k <= after {
                    continue;
                }
                if reply.entries.len() == limit {
                    reply.more = true;
                    break;
                }
                let mut e = pb::StoreEntry::decode(raw.as_slice()).unwrap_or_default();
                if visible(&e, now) && k.len() >= base {
                    if r.keys_only {
                        e.value.clear();
                    }
                    reply.entries.push(e);
                }
            }
        }
        ReadKind::Changes => {
            let (trimmed, _, _) = log_meta(kv, v, &ns);
            reply.compacted = r.after_revision < trimmed;
            let from = keys::store(v, keys::STORE_LOG, &ns, &(r.after_revision + 1).to_be_bytes());
            for (k, raw) in kv.scan_prefix(&keys::store(v, keys::STORE_LOG, &ns, b""), usize::MAX) {
                if k < from {
                    continue;
                }
                if reply.changes.len() == limit {
                    reply.more = true;
                    break;
                }
                let c = pb::StoreChange::decode(raw.as_slice()).unwrap_or_default();
                if c.key.starts_with(&r.key) {
                    reply.changes.push(c);
                }
            }
        }
        ReadKind::Lease => {
            reply.lease = load::<pb::StoreLease>(kv, &keys::store(v, keys::STORE_LEASE, &ns, r.key.as_bytes()))
                .filter(|l| l.expires_hlc > now);
            reply.ok = reply.lease.is_some();
        }
        ReadKind::Barrier => {
            reply.barrier = load(kv, &keys::store(v, keys::STORE_BARRIER, &ns, r.key.as_bytes()));
        }
    }
    reply
}

/// The vshard a namespace lives in.
pub fn vshard_of(tenant: &str, name: &str) -> u16 {
    let h = blake3::hash(&[b"store\0".as_slice(), tenant.as_bytes(), b"\0", name.as_bytes()].concat());
    u16::from_le_bytes([h.as_bytes()[0], h.as_bytes()[1]]) % crate::ids::VSHARDS
}
