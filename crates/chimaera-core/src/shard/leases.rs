//! Leases: a session's hold on stored files of a run that is not its own (a
//! `ch.put` handle unpickled where another run is current; LeaseObjects).
//!
//! A run's values (its stored arguments and its driver's `ch.put` values) go
//! when the run ends. One that a session holds stays until its last holder
//! lets it go: by releasing it, by its session ending (the peer releases
//! what the session held), or by its peer incarnation being retired. When
//! the run ends the peer keeps the files `held` names; `Event::Unleased`
//! names the value files whose last lease went after that. Holding a file
//! again is the same lease (the peer counts handles per session). Files
//! named like the run's results stay until the run expires anyway, and a
//! lease on any of the run's files holds its expiry back (see `held_any`).
//!
//! Invariants (tests/leases.rs checks them against a model): every lease is
//! indexed by its holder and the other way round; a value of an ended run
//! appears in `Unleased` exactly once, when its last lease goes; and one that
//! no lease kept when its run ended cannot be held again.

use super::{Cx, Event, PeerInc, keys, load, peer_of, rejected, reply};
use crate::ids::Id;
use crate::kv::KvRead;
use chimaera_proto::v1 as pb;
use std::collections::BTreeMap;

/// Object ids are 16 bytes.
const FILE: usize = 16;

/// Whether `file` of `run` goes when the run ends: every file but those
/// named like its results, which start with the run's first four bytes
/// (the peer's stores::delete_values).
fn is_value(run: &Id, file: &[u8]) -> bool {
    file[..4] != run.0[..4]
}

/// Whether the run is active (None: it is unknown here, or expired).
fn active(kv: &dyn KvRead, v: u16, run: &Id) -> Option<bool> {
    load::<pb::RunRecord>(kv, &keys::run(v, run)).map(|r| r.state() == pb::run_record::State::Active)
}

fn any(kv: &dyn KvRead, v: u16, run: &Id, file: &[u8]) -> bool {
    !kv.scan_prefix(&keys::leases_of(v, run, file), 1).is_empty()
}

pub(super) fn apply(cx: &mut Cx, v: u16, l: &pb::LeaseObjects) -> pb::ShardReply {
    let Ok(run) = Id::from_slice(&l.run) else { return rejected("bad run id") };
    if run.vshard() != v {
        return rejected("run id routes to another vshard");
    }
    let Some((peer, inc)) = peer_of(&l.peer) else { return rejected("missing holder peer") };
    if !l.release && cx.kv.get(&keys::retired(&peer, inc)).is_some() {
        return rejected("the holder's peer incarnation is retired");
    }
    if l.object_ids.iter().any(|f| f.len() != FILE) {
        return rejected("object ids are 16 bytes");
    }
    let Some(active) = active(cx.kv, v, &run) else { return rejected("unknown run (it expired)") };
    if l.release {
        let gone: Vec<Vec<u8>> =
            l.object_ids.iter().filter(|f| unlease(cx, v, &run, f, (peer, inc), l.holder)).cloned().collect();
        unleased(cx, run, active, gone);
        return reply(pb::shard_reply::Body::Count(l.object_ids.len() as u64));
    }
    // A value of an ended run that no lease kept went with it.
    if !active && let Some(f) = l.object_ids.iter().find(|f| is_value(&run, f) && !any(cx.kv, v, &run, f)) {
        return rejected(&format!("its run ended, and value {} with it", Id::from_slice(f).expect("16 bytes").hex()));
    }
    for f in &l.object_ids {
        cx.kv.put(keys::lease(v, &run, f, &peer, inc, l.holder), Vec::new());
        cx.kv.put(keys::lease_by_holder(v, &run, f, &peer, inc, l.holder), Vec::new());
    }
    reply(pb::shard_reply::Body::Count(l.object_ids.len() as u64))
}

/// Take away a holder's lease on `file`. Whether it was the file's last.
fn unlease(cx: &mut Cx, v: u16, run: &Id, file: &[u8], (peer, inc): PeerInc, holder: u64) -> bool {
    let key = keys::lease(v, run, file, &peer, inc, holder);
    if cx.kv.get(&key).is_none() {
        return false;
    }
    cx.kv.delete(&key);
    cx.kv.delete(&keys::lease_by_holder(v, run, file, &peer, inc, holder));
    !any(cx.kv, v, run, file)
}

/// Files whose last lease went: an ended run's values among them can go.
fn unleased(cx: &mut Cx, run: Id, active: bool, mut files: Vec<Vec<u8>>) {
    files.retain(|f| is_value(&run, f));
    if !active && !files.is_empty() {
        cx.events.push(Event::Unleased { run, files });
    }
}

/// A retired peer incarnation's sessions hold nothing any more.
pub(super) fn retire(cx: &mut Cx, v: u16, (peer, inc): PeerInc) {
    let mut gone: BTreeMap<Id, Vec<Vec<u8>>> = BTreeMap::new();
    for (key, _) in cx.kv.scan_prefix(&keys::leases_held_by(v, &peer, inc), usize::MAX) {
        let n = key.len();
        let run = Id::from_slice(&key[n - 40..n - 24]).expect("16-byte run id");
        let holder = u64::from_be_bytes(key[n - 8..].try_into().expect("8-byte holder"));
        if unlease(cx, v, &run, &key[n - 24..n - 8], (peer, inc), holder) {
            gone.entry(run).or_default().push(key[n - 24..n - 8].to_vec());
        }
    }
    for (run, files) in gone {
        let active = active(cx.kv, v, &run).unwrap_or(true);
        unleased(cx, run, active, files);
    }
}

/// Whether any file of `run` is held: its expiry waits.
pub(super) fn held_any(kv: &dyn KvRead, v: u16, run: &Id) -> bool {
    !kv.scan_prefix(&keys::pairs_of_run(v, keys::LEASE, run), 1).is_empty()
}

/// The files of `run` some session holds, in order (what stays when it ends).
pub fn held(kv: &dyn KvRead, run: Id) -> Vec<Vec<u8>> {
    let mut files: Vec<Vec<u8>> = Vec::new();
    for (key, _) in kv.scan_prefix(&keys::pairs_of_run(run.vshard(), keys::LEASE, &run), usize::MAX) {
        let file = &key[3 + 16..3 + 16 + FILE];
        if files.last().is_none_or(|f| f[..] != file[..]) {
            files.push(file.to_vec());
        }
    }
    files
}
