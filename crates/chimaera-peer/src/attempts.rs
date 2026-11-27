//! explain() for a call that ran on other peers. Each peer keeps the attempts
//! it ran (pb::AttemptRecord: stage times, measures, how its inputs came), so
//! the peer a driver asks gathers them from the peers the task record names.

use crate::router::Router;
use chimaera_core::pb;
use std::time::Duration;

/// How long explain() waits on another peer for the attempts it ran.
const ASK: Duration = Duration::from_secs(2);

/// Add to `out` the attempts of `task` that other peers ran, as they recorded
/// them. A peer that is gone, or too old to answer, leaves its attempts out.
pub async fn gather(router: &Router, task: &pb::TaskKey, out: &mut pb::ExplainReply) {
    let (Some(mesh), Some(rec)) = (router.mesh.clone(), out.task.as_ref()) else { return };
    let me = router.identity.peer.to_vec();
    let mut elsewhere: Vec<Vec<u8>> = Vec::new();
    for a in &rec.attempts {
        let Some(peer) = &a.peer else { continue };
        let known = out.attempts.iter().any(|r| r.attempt == a.attempt);
        if peer.peer != me && !known && !elsewhere.contains(&peer.peer) {
            elsewhere.push(peer.peer.clone());
        }
    }
    let asks = elsewhere.iter().filter_map(|p| mesh.member_by_peer(p)).map(|p| {
        let (mesh, task) = (mesh.clone(), task.clone());
        async move { tokio::time::timeout(ASK, mesh.attempts(p.member.node_id, task)).await }
    });
    for got in futures_util::future::join_all(asks).await {
        let Ok(Ok(records)) = got else { continue };
        for r in records {
            if !out.attempts.iter().any(|have| have.attempt == r.attempt && have.peer == r.peer) {
                out.attempts.push(r);
            }
        }
    }
    out.attempts.sort_by_key(|r| r.attempt);
}
