//! What a stored value survives (`durability=` on a call or `ch.put`,
//! pb::Durability), and where the copies of objects go.
//!
//! - default: the replicator (objplane::replicate_new_objects) copies every
//!   new object to `replica_count()` other peers in the background, and a
//!   lost result of a call that may run again is made again (lineage,
//!   core::producer_plan);
//! - ephemeral: no copy, never made again;
//! - replayable: no copy; made again by running its call;
//! - retained: copies on other peers are made before the value is
//!   acknowledged (a result's outcome is published, a put returns), and a
//!   result's ObjectRef names them.
//!
//! A writer (an executor, a driver) that wants no background copy of an
//! object first leaves an empty marker beside it, `<object>.tmp-local`; the
//! replicator takes the marker away when it sees it (or the object) and
//! skips the object. A peer that makes no background copies tells its
//! clients so (PeerInfo.replicas = 0), and they leave no markers.
//!
//! Copies go to other failure domains first (`targets`), and readers whose
//! holder is gone look there first (mesh::fetch_from).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chimaera_core::pb;
use tokio::sync::mpsc;

use crate::core::CoreMsg;
use crate::mesh::MeshClient;
use crate::stores::ObjectStore;

/// Suffix of the marker a writer leaves beside an object it wants no
/// background copy of (".tmp": never spilled or counted as an object).
pub const MARKER: &str = ".tmp-local";

/// Objects whose marker the replicator took, by id, until it sees them.
static KEPT: Mutex<Option<HashMap<Vec<u8>, Instant>>> = Mutex::new(None);

/// How long a marked object that never appeared (its write failed) is remembered.
const KEPT_FOR: Duration = Duration::from_secs(600);

/// Make no background copy of `object` (this peer is about to write it).
pub fn keep_local(object: &[u8]) {
    let mut kept = KEPT.lock().expect("lock");
    let kept = kept.get_or_insert_with(HashMap::new);
    if kept.len() > 65536 {
        kept.retain(|_, at| at.elapsed() < KEPT_FOR);
    }
    kept.insert(object.to_vec(), Instant::now());
}

/// If `name` in `run_dir` is a marker, take it away and remember its object
/// (true); the object may appear later.
pub fn note_marker(run_dir: &Path, name: &str) -> bool {
    let Some(object) = name.strip_suffix(MARKER).and_then(crate::objplane::decode_hex) else { return false };
    if object.len() != 16 {
        return false;
    }
    keep_local(&object);
    let _ = std::fs::remove_file(run_dir.join(name));
    true
}

/// Whether the object `name` in `run_dir` was marked to have no background
/// copy (its marker is taken away).
pub fn kept_local(run_dir: &Path, name: &str) -> bool {
    let marked = std::fs::remove_file(run_dir.join(format!("{name}{MARKER}"))).is_ok();
    let id = crate::objplane::decode_hex(name).unwrap_or_default();
    let noted = KEPT.lock().expect("lock").as_mut().and_then(|k| k.remove(&id)).is_some();
    marked || noted
}

/// The `n` peers that hold copies of the objects `writer` stores: live peers
/// other than it, in other failure domains first (another zone, then another
/// rack, then another machine), each in ring order of node ids after the
/// writer, then the next peers round the ring. Every peer computes the same
/// list from the same membership (including itself when it is not the
/// writer), so a reader whose holder is gone knows where to look, and a
/// smaller `n` gives a prefix.
pub fn targets(mesh: &MeshClient, writer: u64, n: usize) -> Vec<u64> {
    let me = mesh.me.node_id;
    let members = mesh.members();
    let writer_member = if writer == me {
        Some(mesh.me.clone())
    } else {
        members.iter().find(|p| p.member.node_id == writer).map(|p| p.member.clone())
    };
    let mut live: Vec<pb::Member> = mesh.live_others().into_iter().map(|p| p.member).collect();
    if writer != me {
        live.push(mesh.me.clone());
    }
    live.retain(|m| m.node_id != writer);
    // Ring order after the writer: the ids above it, then those below.
    live.sort_by_key(|m| (m.node_id < writer, m.node_id));
    let ranked: Vec<&pb::Member> = live.iter().collect();
    crate::labels::spread(&ranked, n, &crate::labels::FAILURE_DOMAINS, writer_member.as_ref())
}

/// Copies of retained values made at once, at most (as many as background
/// copies, objplane::REPLICAS_AT_ONCE, and apart from them, so a retained put
/// does not wait behind a backlog of those): a copy's time limit counts on
/// this share of the rate (mesh::replicate).
fn retain_turns() -> &'static tokio::sync::Semaphore {
    static TURNS: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    TURNS.get_or_init(|| tokio::sync::Semaphore::new(crate::objplane::REPLICAS_AT_ONCE))
}

/// Why `retain` made no copies: none could survive losing this peer
/// (Infeasible: no other peer, or none on another machine), or a copy failed.
#[derive(Debug)]
pub enum NotRetained {
    Infeasible(String),
    Failed(String),
}

impl std::fmt::Display for NotRetained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotRetained::Infeasible(why) | NotRetained::Failed(why) => f.write_str(why),
        }
    }
}

/// Why copies made on `nodes` would be lost with this machine, or None: one
/// of them is on another machine (or this peer does not know its machine).
pub fn same_machine(mesh: &MeshClient, nodes: &[u64]) -> Option<String> {
    let host = &mesh.me.host;
    if host.is_empty() || nodes.iter().any(|&n| mesh.member(n).is_some_and(|p| p.member.host != *host)) {
        return None;
    }
    Some("every live peer that could hold a copy is on this machine, and a copy there is lost with it".into())
}

/// Copy the objects `ids` of `run`, stored here, to `copies` peers each (see
/// `targets`; fewer when fewer are live), one of them at least on another
/// machine, and wait for every copy. Returns the peers holding the copies.
pub async fn retain(
    mesh: &MeshClient,
    objects: &ObjectStore,
    run: &[u8],
    ids: &[Vec<u8>],
    copies: usize,
) -> Result<Vec<u64>, NotRetained> {
    let mut sized = Vec::with_capacity(ids.len());
    for id in ids {
        let size = tokio::fs::metadata(objects.path(run, id))
            .await
            .map_err(|e| {
                NotRetained::Failed(format!("object {} is not stored on this peer: {e}", crate::stores::hex(id)))
            })?
            .len();
        sized.push((id.clone(), size));
    }
    let nodes = targets(mesh, mesh.me.node_id, copies.max(1));
    if nodes.is_empty() {
        return Err(NotRetained::Infeasible("there is no other live peer to hold a copy".into()));
    }
    if let Some(why) = same_machine(mesh, &nodes) {
        return Err(NotRetained::Infeasible(why));
    }
    let copied = futures_util::future::join_all(
        nodes.iter().flat_map(|&node| sized.iter().map(move |(id, size)| (node, id, *size))).map(
            |(node, id, size)| async move {
                let _turn = retain_turns().acquire().await.map_err(|e| e.to_string())?;
                mesh.replicate(node, run, id, size, true, pb::TrafficClass::TrafficCheckpoint).await.map_err(|e| {
                    crate::counters::REPLICATION_FAILURES.add(1);
                    format!("the copy of object {} on node {node} failed: {e}", crate::stores::hex(id))
                })
            },
        ),
    )
    .await;
    copied.into_iter().collect::<Result<Vec<()>, String>>().map_err(NotRetained::Failed)?;
    Ok(nodes)
}

/// The files a published value is stored in on this peer: its own (unless
/// inline), the parts it lists, and the blobs its frame links.
fn files_of(objects: &ObjectStore, obj: &pb::ObjectRef) -> Vec<Vec<u8>> {
    let mut ids = Vec::new();
    if obj.inline_data.is_empty() {
        ids.extend(crate::parts::file_ids(&objects.path(&obj.run, &obj.object_id)));
        ids.push(obj.object_id.clone());
    }
    ids.extend(obj.blobs.iter().map(|b| b.object_id.clone()));
    ids
}

/// Publish a retained result once its copies are made: answer the core with
/// `CoreMsg::Retained`, the entry's ObjectRef naming the copies as
/// locations, or why there are none. An inline value without blobs is in
/// the log already; an error or a forward stores nothing.
pub fn publish_when_retained(
    mesh: Option<std::sync::Arc<MeshClient>>,
    objects: ObjectStore,
    mut entry: pb::PublishEntry,
    core: mpsc::Sender<CoreMsg>,
) {
    tokio::spawn(async move {
        let mut error = None;
        if let Some(pb::outcome::Value::Ok(obj)) = entry.outcome.as_mut().and_then(|o| o.value.as_mut()) {
            let (store, o) = (objects.clone(), obj.clone());
            // Off the async workers: it reads the header of a result of several values.
            let ids = tokio::task::spawn_blocking(move || files_of(&store, &o)).await.map_err(|e| e.to_string());
            error = match (&mesh, ids) {
                (_, Err(e)) => Some(format!("durability='retained': its files could not be listed: {e}")),
                (_, Ok(ids)) if ids.is_empty() => None,
                (None, Ok(_)) => {
                    Some("durability='retained' keeps a copy on another peer, and this peer runs alone".into())
                }
                (Some(mesh), Ok(ids)) => {
                    let copies = crate::objplane::replica_count().max(1);
                    match retain(mesh, &objects, &obj.run, &ids, copies).await {
                        Ok(nodes) => {
                            let at: Vec<pb::PeerInc> = nodes
                                .iter()
                                .filter_map(|&n| mesh.member(n))
                                .map(|p| pb::PeerInc { peer: p.member.peer, inc: p.member.incarnation })
                                .collect();
                            obj.locations.extend(at.iter().cloned());
                            for b in &mut obj.blobs {
                                b.locations.extend(at.iter().cloned());
                            }
                            None
                        }
                        Err(why) => Some(format!("durability='retained': no copy of its result was made: {why}")),
                    }
                }
            };
        }
        let _ = core.send(CoreMsg::Retained { entry: Box::new(entry), error }).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_keeps_its_object_local_once_whichever_is_seen_first() {
        let d = tempfile::tempdir().unwrap();
        let (a, b, c) = ("0a".repeat(16), "0b".repeat(16), "0c".repeat(16));
        // The marker seen first (inotify's order): taken into memory.
        std::fs::write(d.path().join(format!("{a}{MARKER}")), b"").unwrap();
        assert!(note_marker(d.path(), &format!("{a}{MARKER}")));
        assert!(!d.path().join(format!("{a}{MARKER}")).exists());
        std::fs::write(d.path().join(&a), b"x").unwrap();
        assert!(kept_local(d.path(), &a));
        assert!(!kept_local(d.path(), &a), "remembered until seen once");
        // The object listed before its marker (a directory scan): the marker file says so.
        std::fs::write(d.path().join(format!("{b}{MARKER}")), b"").unwrap();
        std::fs::write(d.path().join(&b), b"x").unwrap();
        assert!(kept_local(d.path(), &b));
        assert!(!d.path().join(format!("{b}{MARKER}")).exists(), "the marker is taken away");
        assert!(!kept_local(d.path(), &c), "an unmarked object is copied as usual");
        assert!(!note_marker(d.path(), &c) && !note_marker(d.path(), "zz.tmp-local"));
    }
}
