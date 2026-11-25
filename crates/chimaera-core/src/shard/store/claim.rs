//! Claims (StoreClaim): a work queue's consumers take the keys under a prefix
//! one at a time, each under a lease, without listing what is queued or held.
//!
//! For each prefix the namespace keeps a cursor, the last key handed out for
//! the first time, and an index of the keys handed out by when their claim
//! lapses; a key given back (its claim unlocked while the key is still there)
//! is indexed at 0. A claim takes the first index entry if it is due, else the
//! first key after the cursor. Renewing or unlocking a claim's lease moves its
//! entry, so the index stays exact and a claim looks at two keys. An entry that
//! went stale (its key deleted under a claim never unlocked, or held by a plain
//! lock) is put right when it comes due; a claim looks at no more than SCAN
//! keys before it asks to be sent again.

use super::{Cx, bump, entry, log_append, namespace, revision, visible};
use crate::hlc;
use crate::kv::KvRead;
use crate::shard::{keys, load};
use chimaera_proto::v1 as pb;
use prost::Message;

/// Index entries and keys one claim looks at, at most.
const SCAN: u32 = 64;
const CURSOR: u8 = 0;
const DUE: u8 = 1;

/// The cursor (CURSOR) or the start of the index (DUE) of claims under `prefix`.
fn of(v: u16, ns: &[u8], part: u8, prefix: &str) -> Vec<u8> {
    let mut k = vec![part];
    k.extend_from_slice(&(prefix.len() as u32).to_be_bytes());
    k.extend_from_slice(prefix.as_bytes());
    keys::store(v, keys::STORE_CLAIM, ns, &k)
}

fn due(v: u16, ns: &[u8], prefix: &str, at: u64, key: &str) -> Vec<u8> {
    let mut k = of(v, ns, DUE, prefix);
    k.extend_from_slice(&at.to_be_bytes());
    k.extend_from_slice(key.as_bytes());
    k
}

/// The lease on `lock`, unless it has lapsed.
fn held(kv: &dyn KvRead, v: u16, ns: &[u8], lock: &str, now: u64) -> Option<pb::StoreLease> {
    load::<pb::StoreLease>(kv, &keys::store(v, keys::STORE_LEASE, ns, lock.as_bytes())).filter(|l| l.expires_hlc > now)
}

/// Grant `c.holder` a claim on the first free key under `c.prefix`.
pub(in crate::shard) fn claim(cx: &mut Cx, v: u16, c: &pb::StoreClaim) -> pb::StoreReply {
    let ns = namespace(&c.tenant, &c.namespace);
    let mut reply = pb::StoreReply { revision: revision(cx.kv, v, &ns), ..Default::default() };
    let mut lapses = u64::MAX; // when the first claim still held lapses
    // A key claimed before whose claim lapsed or was given back.
    let index = of(v, &ns, DUE, &c.prefix);
    while reply.examined < SCAN {
        let Some((ikey, _)) = cx.kv.scan_after(&index, &[], 1).pop() else { break };
        reply.examined += 1;
        let at = u64::from_be_bytes(ikey[index.len()..index.len() + 8].try_into().expect("8-byte hlc"));
        if at > cx.now {
            lapses = at;
            break;
        }
        cx.kv.delete(&ikey);
        let key = String::from_utf8_lossy(&ikey[index.len() + 8..]).into_owned();
        let Some(e) = entry(cx.kv, v, &ns, &key, cx.now) else { continue }; // deleted while claimed
        match held(cx.kv, v, &ns, &format!("{}{key}", c.lock_prefix), cx.now) {
            Some(l) => {
                cx.kv.put(due(v, &ns, &c.prefix, l.expires_hlc, &key), Vec::new());
                lapses = lapses.min(l.expires_hlc);
            }
            None => return grant(cx, v, &ns, c, e, reply.examined),
        }
    }
    // Else the first key never claimed.
    let cursor = of(v, &ns, CURSOR, &c.prefix);
    let items = keys::store(v, keys::STORE, &ns, c.prefix.as_bytes());
    let base = items.len() - c.prefix.len();
    while reply.examined < SCAN {
        let from = cx.kv.get(&cursor).map_or_else(Vec::new, |k| keys::store(v, keys::STORE, &ns, &k));
        let Some((k, raw)) = cx.kv.scan_after(&items, &from, 1).pop() else { break };
        reply.examined += 1;
        cx.kv.put(cursor.clone(), k[base..].to_vec());
        let e = pb::StoreEntry::decode(raw.as_slice()).unwrap_or_default();
        if !visible(&e, cx.now) {
            continue;
        }
        // Held by a plain lock (an earlier version's claim): it comes due when that lapses.
        match held(cx.kv, v, &ns, &format!("{}{}", c.lock_prefix, e.key), cx.now) {
            Some(l) => {
                cx.kv.put(due(v, &ns, &c.prefix, l.expires_hlc, &e.key), Vec::new());
                lapses = lapses.min(l.expires_hlc);
            }
            None => return grant(cx, v, &ns, c, e, reply.examined),
        }
    }
    if reply.examined >= SCAN {
        reply.error = format!("busy: looked at {SCAN} keys under {:?}; claim again", c.prefix);
        return reply;
    }
    reply.error = format!("nothing to claim under {:?}", c.prefix);
    reply.more = super::after(cx.kv, &items, Vec::new(), SCAN as usize)
        .take(SCAN as usize)
        .any(|(_, raw)| visible(&pb::StoreEntry::decode(raw.as_slice()).unwrap_or_default(), cx.now));
    if lapses != u64::MAX {
        reply.retry_ms = hlc::wall_ms(lapses.saturating_sub(cx.now)) + 1;
    }
    reply
}

fn grant(cx: &mut Cx, v: u16, ns: &[u8], c: &pb::StoreClaim, e: pb::StoreEntry, examined: u32) -> pb::StoreReply {
    let token = bump(cx, v, ns);
    let lease = pb::StoreLease {
        holder: c.holder.clone(),
        token,
        expires_hlc: cx.now + hlc::from_ms(c.ttl_ms.max(1)),
        item: e.key.clone(),
        items: c.prefix.clone(),
    };
    cx.kv.put(due(v, ns, &c.prefix, lease.expires_hlc, &e.key), Vec::new());
    let lock = format!("{}{}", c.lock_prefix, e.key);
    crate::shard::store(cx.kv, keys::store(v, keys::STORE_LEASE, ns, lock.as_bytes()), &lease);
    pb::StoreReply { ok: true, revision: token, entry: Some(e), lease: Some(lease), examined, ..Default::default() }
}

/// A claim's lease was renewed until `expiry`: its key comes due then.
pub(super) fn renewed(cx: &mut Cx, v: u16, ns: &[u8], l: &pb::StoreLease, expiry: u64) {
    if !l.item.is_empty() {
        cx.kv.delete(&due(v, ns, &l.items, l.expires_hlc, &l.item));
        cx.kv.put(due(v, ns, &l.items, expiry, &l.item), Vec::new());
    }
}

/// A claim's lease was unlocked. If its key is still there it was given back:
/// it goes before every other, and its change at a new revision wakes waiting
/// claims. Returns that revision.
pub(super) fn released(cx: &mut Cx, v: u16, ns: &[u8], l: &pb::StoreLease) -> Option<u64> {
    if l.item.is_empty() {
        return None;
    }
    cx.kv.delete(&due(v, ns, &l.items, l.expires_hlc, &l.item));
    let mut e = entry(cx.kv, v, ns, &l.item, cx.now)?;
    cx.kv.put(due(v, ns, &l.items, 0, &l.item), Vec::new());
    let r = bump(cx, v, ns);
    e.version = r;
    let change = pb::StoreChange {
        revision: r,
        key: e.key.clone(),
        value: e.value.clone(),
        is_counter: e.is_counter,
        counter: e.counter,
        deleted: false,
    };
    crate::shard::store(cx.kv, keys::store(v, keys::STORE, ns, l.item.as_bytes()), &e);
    log_append(cx, v, ns, change, 0);
    Some(r)
}
