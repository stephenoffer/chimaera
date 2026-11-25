//! Sessions' holds on stored files of runs that are not theirs (the shard's
//! leases.rs): a `ch.put` handle unpickled where another run is current
//! holds its value's files (Peer.Borrow) until the handle goes or its
//! session ends. The shard keeps each lease under this peer incarnation and
//! a holder id (the id of the session that took it; a resumed session keeps
//! it); `Borrows` counts each holder's handles per file and asks for a
//! lease's release when the last of them goes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chimaera_core::ids::Id;
use chimaera_core::pb;
use tokio::sync::{Mutex, mpsc, oneshot};
use tonic::Status;

use crate::core::CoreMsg;
use crate::router::Router;

/// Lease commands commit in the order they are counted. Otherwise a release
/// counted before a hold of the same file could commit after it, and the
/// shard would drop a lease a handle still holds; or an ended session's
/// releases could commit before a hold counted earlier, and the shard would
/// keep a lease nothing holds. A Borrow holds this from its count to its last
/// commit, and an ended session's releases are proposed under it.
static ORDER: Mutex<()> = Mutex::const_new(());

/// Leases to propose: (holder, files) of one run.
pub type Leases = Vec<(u64, Vec<Vec<u8>>)>;

/// Handles per file of each holder, and the session that holds them now.
#[derive(Default)]
pub struct Borrows(HashMap<u64, Held>);

struct Held {
    session: u64,
    files: HashMap<(Id, Vec<u8>), u32>,
}

impl Borrows {
    /// A Borrow request of `session`, whose credential is `owner` (None: no
    /// such session here), from a caller with `tenant` (None: any): the
    /// leases to propose, taken (or released) on its behalf.
    pub fn request(
        &mut self,
        owner: Option<&Option<String>>,
        tenant: &Option<String>,
        session: u64,
        run: Id,
        files: &[Vec<u8>],
        release: bool,
    ) -> Result<Leases, String> {
        match owner {
            None => return Err(format!("session {session} is not open on this peer")),
            Some(o) if tenant.is_some() && o != tenant => {
                return Err("the session belongs to another credential".into());
            }
            Some(_) => {}
        }
        if release {
            return Ok(self.release(session, run, files));
        }
        let holder = self.0.iter().find(|(_, h)| h.session == session).map_or(session, |(id, _)| *id);
        let held = self.0.entry(holder).or_insert_with(|| Held { session, files: HashMap::new() });
        for f in files {
            *held.files.entry((run, f.clone())).or_default() += 1;
        }
        Ok(vec![(holder, files.to_vec())])
    }

    /// `session` lets go of `files` of `run` once: those no handle holds now.
    fn release(&mut self, session: u64, run: Id, files: &[Vec<u8>]) -> Leases {
        let mut gone: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
        for f in files {
            let key = (run, f.clone());
            let found = self.0.iter_mut().find(|(_, h)| h.session == session && h.files.contains_key(&key));
            let Some((&holder, held)) = found else { continue };
            let n = held.files.get_mut(&key).expect("found above");
            *n -= 1;
            if *n == 0 {
                held.files.remove(&key);
                gone.entry(holder).or_default().push(f.clone());
            }
        }
        self.0.retain(|_, h| !h.files.is_empty());
        gone.into_iter().collect()
    }

    /// Everything `session` held (it ended), as (holder, run, files).
    pub fn end(&mut self, session: u64) -> Vec<(u64, Id, Vec<Vec<u8>>)> {
        let holders: Vec<u64> = self.0.iter().filter(|(_, h)| h.session == session).map(|(id, _)| *id).collect();
        let mut out = Vec::new();
        for holder in holders {
            let mut by_run: BTreeMap<Id, Vec<Vec<u8>>> = BTreeMap::new();
            for (run, f) in self.0.remove(&holder).expect("listed above").files.into_keys() {
                by_run.entry(run).or_default().push(f);
            }
            out.extend(by_run.into_iter().map(|(run, files)| (holder, run, files)));
        }
        out
    }

    /// A resumed session goes on holding what `old` held.
    pub fn adopt(&mut self, old: u64, new: u64) {
        for h in self.0.values_mut().filter(|h| h.session == old) {
            h.session = new;
        }
    }
}

/// The shard command that leases (or releases) `files` of `run` for `holder`.
pub fn command(router: &Router, run: Id, files: Vec<Vec<u8>>, holder: u64, release: bool) -> pb::shard_cmd::Body {
    let peer = Some(router.identity.pb());
    pb::shard_cmd::Body::LeaseObjects(pb::LeaseObjects { run: run.to_vec(), object_ids: files, peer, holder, release })
}

/// Count a request for its session in the core: the leases to propose.
async fn count(
    core: &mpsc::Sender<CoreMsg>,
    tenant: Option<String>,
    r: &pb::BorrowRequest,
    run: Id,
    release: bool,
) -> Result<Leases, Status> {
    let (reply, answer) = oneshot::channel();
    let files = r.object_ids.clone();
    let msg = CoreMsg::Lease { session: r.session, tenant, run, files, release, reply };
    core.send(msg).await.map_err(|_| Status::unavailable("the peer is stopping"))?;
    answer.await.map_err(|_| Status::unavailable("the peer is stopping"))?.map_err(Status::failed_precondition)
}

/// Propose `leases`: NotFound when the shard refuses one (an ended run's
/// value that nothing kept, or an expired run).
async fn propose(router: &Router, run: Id, leases: Leases, release: bool) -> Result<(), Status> {
    let mut refused = Ok(());
    for (holder, files) in leases {
        match router.propose(run.vshard(), command(router, run, files, holder, release)).await {
            Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Rejected(why)) }) => {
                refused = Err(Status::not_found(why))
            }
            Ok(_) => {}
            Err(e) => refused = Err(Status::unavailable(e.to_string())),
        }
    }
    refused
}

/// Serve a Borrow request: count it for its session, then lease (or
/// release) the files in their run's vshard. A hold the shard refuses is
/// counted back.
pub async fn borrow(
    core: &mpsc::Sender<CoreMsg>,
    router: &Router,
    tenant: Option<String>,
    r: pb::BorrowRequest,
) -> Result<(), Status> {
    let run = Id::from_slice(&r.run).map_err(|_| Status::invalid_argument("bad run id"))?;
    let _order = ORDER.lock().await;
    let leases = count(core, tenant.clone(), &r, run, r.release).await?;
    let Err(why) = propose(router, run, leases, r.release).await else { return Ok(()) };
    if r.release {
        tracing::warn!(run = %run.hex(), "a lease was not released: {}", why.message());
    } else {
        let back = count(core, tenant, &r, run, true).await?;
        let _ = propose(router, run, back, true).await;
    }
    Err(why)
}

/// Release what an ended session held (`Borrows::end`), after the lease
/// commands counted before it ended.
pub fn release_ended(router: Arc<Router>, in_flight: Arc<AtomicU64>, ended: Vec<(u64, Id, Vec<Vec<u8>>)>) {
    if ended.is_empty() {
        return;
    }
    in_flight.fetch_add(1, Ordering::Relaxed);
    tokio::spawn(async move {
        let _order = ORDER.lock().await;
        for (holder, run, files) in ended {
            if let Err(e) = router.propose(run.vshard(), command(&router, run, files, holder, true)).await {
                tracing::warn!(run = %run.hex(), "a lease was not released: {e}");
            }
        }
        in_flight.fetch_sub(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_goes_with_the_last_handle_or_the_session_and_follows_a_resume() {
        let mut b = Borrows::default();
        let run = Id::derive(0, 3, &[b"run"]);
        let (f, g) = (vec![1u8; 16], vec![2u8; 16]);
        let open = Some(&None);
        assert_eq!(
            b.request(open, &None, 7, run, &[f.clone(), g.clone()], false).unwrap(),
            [(7, vec![f.clone(), g.clone()])]
        );
        assert_eq!(b.request(open, &None, 7, run, std::slice::from_ref(&f), false).unwrap(), [(7, vec![f.clone()])]);
        assert_eq!(b.request(open, &None, 7, run, &[f.clone(), g.clone()], true).unwrap(), [(7, vec![g.clone()])]);
        assert!(b.request(None, &None, 9, run, std::slice::from_ref(&f), false).is_err(), "no such session");
        let tenant = Some("a".to_string());
        assert!(
            b.request(Some(&Some("b".into())), &tenant, 7, run, std::slice::from_ref(&f), true).is_err(),
            "another credential"
        );
        // Session 7 is resumed as 8: the lease keeps its holder.
        b.adopt(7, 8);
        assert!(b.end(7).is_empty());
        assert_eq!(b.request(open, &None, 8, run, std::slice::from_ref(&g), false).unwrap(), [(7, vec![g.clone()])]);
        let mut ended = b.end(8);
        ended[0].2.sort();
        assert_eq!(ended, [(7, run, vec![f, g])]);
        assert!(b.end(8).is_empty());
    }
}
