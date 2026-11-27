//! One run, across the cell, of the call that made a value lost with every
//! peer that held it. Each peer whose readers find the value lost would
//! otherwise run the call again on its own. Instead the first to take the
//! lease on that call's attempt (a lock in the cell's store) runs it, records
//! where it made the value, and the others fetch it from there. The record is
//! written under the lease's fencing token, so a peer whose lease lapsed (it
//! stalled, or was cut off and another took the lease) cannot announce its
//! copy. A peer that cannot reach the store runs the call itself, as before.

use crate::mesh::MeshClient;
use crate::router::Router;
use crate::stores::ObjectStore;
use chimaera_core::pb;
use chimaera_core::shard::{Tk, store};
use std::sync::Arc;
use std::time::Duration;

const TENANT: &str = "";
const NAMESPACE: &str = "rebuild";
/// How long a lease holds unless renewed; its holder renews it three times
/// as often. A holder that died lets another peer take over after this long.
const LEASE: Duration = Duration::from_secs(10);
/// How long the record of where a value was made again is kept.
const MADE_FOR: Duration = Duration::from_secs(600);
/// How often a peer waiting on another's run looks for its record: first
/// after POLL, then twice as long each time up to POLL_MAX. Each look is two
/// reads through the store's group, at most one peer waiting per lost value
/// on each peer (the others wait in its core): 64 peers waiting on a call
/// that takes a minute make about 64 reads a second, not 512.
const POLL: Duration = Duration::from_millis(250);
const POLL_MAX: Duration = Duration::from_secs(2);

/// What taking the lease on a lost value's call found.
#[derive(Debug, PartialEq)]
pub enum Claim {
    /// This peer runs the call; `token` fences what it records (0: no store
    /// to coordinate through, so it runs alone and records nothing).
    Won(u64),
    /// Another peer, this node, made the value again.
    Made(u64),
    /// Another peer's run of the call failed, for this reason.
    Failed(String),
    /// Another peer holds the lease and is running the call.
    Elsewhere,
}

fn lease_key(producer: Tk, attempt: u32) -> String {
    format!("{}/{}/{attempt}", producer.run.hex(), producer.task.hex())
}

fn made_key(producer: Tk, attempt: u32) -> String {
    lease_key(producer, attempt) + "/made"
}

/// This peer as a lease holder: its id and incarnation (a restarted peer is
/// another holder).
fn me(router: &Router) -> String {
    format!("{}:{}", router.identity.peer.hex(), router.identity.incarnation)
}

/// A record's value: where the value was made again, or why its run failed.
fn record(made: &Result<(), String>, me: &str) -> String {
    match made {
        Ok(()) => format!("at:{me}"),
        Err(why) => format!("failed:{why}"),
    }
}

/// What a record says, read back.
fn read_record(value: &[u8]) -> Claim {
    let value = String::from_utf8_lossy(value);
    if let Some(holder) = value.strip_prefix("at:") {
        let peer = holder.split(':').next().and_then(crate::objplane::decode_hex).unwrap_or_default();
        return Claim::Made(crate::stores::node_id_of(&peer));
    }
    Claim::Failed(value.strip_prefix("failed:").unwrap_or(&value).to_string())
}

async fn command(router: &Router, body: pb::shard_cmd::Body) -> Result<pb::StoreReply, String> {
    match router.propose(store::vshard_of(TENANT, NAMESPACE), body).await {
        Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Store(r)) }) => Ok(r),
        Ok(other) => Err(format!("store command rejected: {other:?}")),
        Err(e) => Err(e.to_string()),
    }
}

fn read(kind: pb::store_read::Kind, key: String) -> pb::shard_cmd::Body {
    let r = pb::StoreRead {
        tenant: TENANT.into(),
        namespace: NAMESPACE.into(),
        kind: kind as i32,
        key,
        ..Default::default()
    };
    pb::shard_cmd::Body::StoreRead(r)
}

/// The record of a run of the call, if one was made.
async fn made(router: &Router, producer: Tk, attempt: u32) -> Result<Option<Claim>, String> {
    let r = command(router, read(pb::store_read::Kind::Get, made_key(producer, attempt))).await?;
    Ok(r.entry.filter(|e| !e.value.is_empty()).map(|e| read_record(&e.value)))
}

/// Take the lease on the call `producer` ran as `attempt`, unless its value
/// was made again already (or its run failed).
pub async fn claim(router: &Router, producer: Tk, attempt: u32) -> Claim {
    match made(router, producer, attempt).await {
        Ok(Some(c)) => return c,
        Ok(None) => {}
        Err(_) => return Claim::Won(0),
    }
    let lock = pb::StoreWrite {
        tenant: TENANT.into(),
        namespace: NAMESPACE.into(),
        op: pb::store_write::Op::Lock as i32,
        key: lease_key(producer, attempt),
        holder: me(router),
        ttl_ms: LEASE.as_millis() as u64,
        ..Default::default()
    };
    match command(router, pb::shard_cmd::Body::StoreWrite(lock)).await {
        Ok(r) if r.ok => Claim::Won(r.lease.map_or(0, |l| l.token)),
        Ok(_) => Claim::Elsewhere,
        Err(_) => Claim::Won(0),
    }
}

/// Wait while another peer runs the call: its record, or the lease once it
/// lapsed with no record (its holder died).
pub async fn wait(router: &Router, producer: Tk, attempt: u32) -> Claim {
    let mut pause = POLL;
    loop {
        if let Ok(Some(c)) = made(router, producer, attempt).await {
            return c;
        }
        match command(router, read(pb::store_read::Kind::Lease, lease_key(producer, attempt))).await {
            Ok(r) if r.lease.is_some() => {}
            Ok(_) => match claim(router, producer, attempt).await {
                Claim::Elsewhere => {}
                c => return c,
            },
            Err(_) => return Claim::Won(0),
        }
        tokio::time::sleep(pause).await;
        pause = next_pause(pause);
    }
}

/// The pause after `pause` between looks for a record (see POLL).
fn next_pause(pause: Duration) -> Duration {
    (pause * 2).min(POLL_MAX)
}

/// Keep the lease while the call runs here, renewing it until the returned
/// handle is aborted (or a renewal fails: then another peer may take it).
pub fn keep(router: Arc<Router>, producer: Tk, attempt: u32, token: u64) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(LEASE / 3).await;
            let renew = pb::StoreWrite {
                tenant: TENANT.into(),
                namespace: NAMESPACE.into(),
                op: pb::store_write::Op::Renew as i32,
                key: lease_key(producer, attempt),
                holder: me(&router),
                token,
                ttl_ms: LEASE.as_millis() as u64,
                ..Default::default()
            };
            if !command(&router, pb::shard_cmd::Body::StoreWrite(renew)).await.is_ok_and(|r| r.ok) {
                return;
            }
        }
    })
    .abort_handle()
}

/// Record where the call made the value again, or why it failed, under the
/// lease (refused once the lease lapsed), then let the lease go.
pub async fn publish(router: Arc<Router>, producer: Tk, attempt: u32, token: u64, made: Result<(), String>) {
    if token == 0 {
        return;
    }
    let put = pb::StoreWrite {
        tenant: TENANT.into(),
        namespace: NAMESPACE.into(),
        op: pb::store_write::Op::Put as i32,
        key: made_key(producer, attempt),
        value: record(&made, &me(&router)).into_bytes(),
        ttl_ms: MADE_FOR.as_millis() as u64,
        fence: Some(pb::StoreFence { lock: lease_key(producer, attempt), token }),
        ..Default::default()
    };
    let _ = command(&router, pb::shard_cmd::Body::StoreWrite(put)).await;
    let unlock = pb::StoreWrite {
        tenant: TENANT.into(),
        namespace: NAMESPACE.into(),
        op: pb::store_write::Op::Unlock as i32,
        key: lease_key(producer, attempt),
        holder: me(&router),
        token,
        ..Default::default()
    };
    let _ = command(&router, pb::shard_cmd::Body::StoreWrite(unlock)).await;
}

/// Fetch the files of `made`, a value made again on `node`, from there.
pub async fn fetch(mesh: &MeshClient, objects: &ObjectStore, made: &pb::ObjectRef, node: u64) -> Result<(), String> {
    let traffic = pb::TrafficClass::TrafficRecovery;
    for o in super::with_blobs(made).iter().filter(|o| o.inline_data.is_empty() && !o.run.is_empty()) {
        mesh.localize(objects, &o.run, &o.object_id, o.size, &[node], traffic)
            .await
            .map_err(|e| format!("fetching the value made again on node {node}: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_says_where_the_value_was_made_or_why_its_run_failed() {
        let peer = [7u8; 16];
        let holder = format!("{}:3", crate::stores::hex(&peer));
        assert_eq!(read_record(record(&Ok(()), &holder).as_bytes()), Claim::Made(crate::stores::node_id_of(&peer)));
        let failed = record(&Err("it raised ValueError: no".into()), &holder);
        assert_eq!(read_record(failed.as_bytes()), Claim::Failed("it raised ValueError: no".into()));
    }

    #[test]
    fn a_peer_waiting_on_another_looks_less_often_the_longer_it_waits() {
        let mut pauses = vec![POLL];
        for _ in 0..5 {
            pauses.push(next_pause(*pauses.last().unwrap()));
        }
        let ms: Vec<u128> = pauses.iter().map(Duration::as_millis).collect();
        assert_eq!(ms, [250, 500, 1000, 2000, 2000, 2000]);
        // Looks over a minute: a handful while it is short, then one each POLL_MAX.
        let (mut t, mut n, mut p) = (Duration::ZERO, 0, POLL);
        while t < Duration::from_secs(60) {
            (t, n, p) = (t + p, n + 1, next_pause(p));
        }
        assert!(n <= 33, "{n} looks in a minute");
    }

    #[test]
    fn each_attempt_of_each_call_has_its_own_lease_and_record() {
        let run = chimaera_core::ids::Id::derive(0, 1, &[b"run"]);
        let (a, b) = (chimaera_core::ids::Id::derive(0, 1, &[b"a"]), chimaera_core::ids::Id::derive(0, 1, &[b"b"]));
        let (ta, tb) = (Tk { run, task: a }, Tk { run, task: b });
        assert_ne!(lease_key(ta, 1), lease_key(ta, 2));
        assert_ne!(lease_key(ta, 1), lease_key(tb, 1));
        assert_eq!(made_key(ta, 1), lease_key(ta, 1) + "/made");
    }
}
