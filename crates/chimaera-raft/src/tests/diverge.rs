//! Voters that committed entries the rest of their group lost: a majority
//! lost power together, having acknowledged entries still in their page
//! cache. The survivors refuse the new history (see `crate::diverge`) and
//! rejoin as new members.

use super::*;
use crate::log_store::{dec, enc};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::testing::{blank_ent, log_id};
use openraft::type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf, VoteOf};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// The groups the divergence hook has been called with, in this process.
fn divergences() -> &'static Mutex<Vec<u32>> {
    static SEEN: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    SEEN.get_or_init(|| {
        set_divergence_hook(|g| divergences().lock().unwrap().push(g));
        Mutex::default()
    })
}

fn diverged(group: u32) -> usize {
    divergences().lock().unwrap().iter().filter(|g| **g == group).count()
}

/// A voter never truncates entries at or below its commit point. The store
/// refuses, tells the divergence hook, and keeps its log; above the point a
/// truncation works as before, and a new store for the group (a restart)
/// reads the point from disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_voter_refuses_to_truncate_entries_it_committed() {
    const GROUP: u32 = 41;
    divergences();
    let dir = tempfile::tempdir().unwrap();
    let (host, _rx) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Process).unwrap();
    let mut log = LogStore::new(GROUP, host.log_db.clone(), host.writer.clone());
    log.append((1..=5).map(|i| blank_ent::<TypeConfig>(1, 1, i)), IOFlushed::noop()).await.unwrap();
    log.save_committed(Some(log_id::<TypeConfig>(1, 1, 3))).await.unwrap();
    let indexes = |got: Vec<EntryOf<TypeConfig>>| got.iter().map(|e| e.log_id.index).collect::<Vec<_>>();
    let err = log.truncate_after(Some(log_id::<TypeConfig>(1, 1, 2))).await.unwrap_err();
    assert!(err.to_string().contains("commit index 3"), "{err}");
    assert_eq!(diverged(GROUP), 1);
    assert_eq!(indexes(log.try_get_log_entries(1..=5).await.unwrap()), vec![1, 2, 3, 4, 5], "the log was kept");
    log.truncate_after(Some(log_id::<TypeConfig>(1, 1, 3))).await.unwrap();
    assert_eq!(indexes(log.try_get_log_entries(1..=5).await.unwrap()), vec![1, 2, 3]);
    assert_eq!(diverged(GROUP), 1, "above the commit point a truncation is ordinary");
    let mut restarted = LogStore::new(GROUP, host.log_db.clone(), host.writer.clone());
    assert!(restarted.truncate_after(None).await.is_err());
    assert_eq!(diverged(GROUP), 2);
    assert_eq!(indexes(restarted.try_get_log_entries(1..=5).await.unwrap()), vec![1, 2, 3]);
}

/// Calls from or to a node in `cut` fail, as across a network partition.
struct Partition {
    net: Arc<LocalNet>,
    me: NodeId,
    cut: Arc<Mutex<BTreeSet<NodeId>>>,
}

impl Partition {
    fn severed(&self, target: NodeId) -> Option<RpcFuture> {
        let cut = self.cut.lock().unwrap();
        (cut.contains(&self.me) || cut.contains(&target)).then(|| -> RpcFuture {
            Box::pin(async move { Err(std::io::Error::other(format!("{target} is cut off"))) })
        })
    }
}

impl Transport for Partition {
    fn call(&self, target: NodeId, addr: &str, group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture {
        self.severed(target).unwrap_or_else(|| self.net.call(target, addr, group, kind, body))
    }

    fn send_snapshot(&self, target: NodeId, addr: &str, group: u32, header: Vec<u8>, file: std::fs::File) -> RpcFuture {
        self.severed(target).unwrap_or_else(|| self.net.send_snapshot(target, addr, group, header, file))
    }
}

fn node(net: &LocalNet, id: NodeId) -> Group {
    net.nodes.lock().unwrap().get(&id).cloned().unwrap()
}

async fn wait_applied(net: &LocalNet, ids: &[NodeId], index: u64) {
    for _ in 0..800 {
        if ids.iter().all(|id| node(net, *id).stats().applied >= index) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("not every one of {ids:?} applied index {index}");
}

async fn leader_among(net: &LocalNet, ids: &[NodeId]) -> Group {
    for _ in 0..800 {
        for id in ids {
            let g = node(net, *id);
            if g.is_leader().await {
                return g;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("none of {ids:?} became the leader");
}

async fn propose_among(net: &LocalNet, ids: &[NodeId], c: &pb::ShardCmd) {
    for _ in 0..100 {
        if leader_among(net, ids).await.propose(c).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{ids:?} could not commit");
}

async fn reopen(dir: &Path, id: NodeId, transport: Arc<dyn Transport>, durability: DiskDurability) -> RaftHost {
    for _ in 0..200 {
        if let Ok((host, rx)) = RaftHost::open(dir, id, transport.clone(), durability) {
            std::mem::forget(rx);
            return host;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{} stayed locked", dir.display());
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if src.is_dir() {
            copy_dir(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

/// Snapshot `g`'s state and copy the file to `path`, as a transfer would.
async fn snapshot_of(g: &Group, path: &Path) -> (VoteOf<TypeConfig>, SnapshotMetaOf<TypeConfig>, PathBuf) {
    let applied = g.stats().applied;
    g.raft.trigger().snapshot().await.unwrap();
    wait_snapshot_past(g, applied).await;
    let mut s = g.raft.get_snapshot().await.unwrap().expect("a snapshot");
    std::io::copy(&mut s.snapshot, &mut std::fs::File::create(path).unwrap()).unwrap();
    (g.raft.metrics().borrow_watched().vote, s.meta, path.to_path_buf())
}

const IDS: [NodeId; 5] = [1, 2, 3, 4, 5];
const SURVIVORS: [NodeId; 2] = [1, 2];
/// The voters that lose power together.
pub(super) const LOST: [NodeId; 3] = [3, 4, 5];
/// Entries the three acknowledge and then lose.
pub(super) const LOST_ENTRIES: u64 = 6;

type State = (Option<LogIdOf<TypeConfig>>, Vec<(Vec<u8>, Vec<u8>)>);

/// How the history of [`History::build`] goes.
pub(super) struct History {
    pub(super) gid: u32,
    pub(super) durability: DiskDurability,
    /// The commands the three acknowledge and then lose.
    pub(super) lost: fn(u64) -> pb::ShardCmd,
    /// Checks on the voters' hosts just before the three lose power.
    pub(super) before_loss: fn(&BTreeMap<NodeId, RaftHost>),
    /// Entries the three write after the first before the two come back;
    /// leadership then moves among them, so the new leader's first probe
    /// reaches past the end of the two's logs.
    pub(super) written_while_cut: u64,
}

impl History {
    /// Voters under `none`: what they acknowledge is in the page cache only.
    fn unsynced(gid: u32, written_while_cut: u64) -> History {
        History {
            gid,
            durability: DiskDurability::Process,
            lost: |i| submit(9, i),
            before_loss: |_| {},
            written_while_cut,
        }
    }

    /// Five voters. Three lose power together and come back with what their
    /// disks kept, without entries they had acknowledged, which the other two
    /// committed and applied. With the two cut off, the three elect a leader
    /// and write an entry at an index the two committed. Returns with the two
    /// still cut off.
    pub(super) async fn build(self) -> Split {
        divergences();
        let (gid, durability) = (self.gid, self.durability);
        let dir = tempfile::tempdir().unwrap();
        let net = Arc::new(LocalNet::default());
        *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
        let mut split = Split {
            history: self,
            dir,
            net,
            cut: Arc::default(),
            hosts: BTreeMap::new(),
            suffix: 0,
            before: BTreeMap::new(),
        };
        let root = split.dir.path().to_path_buf();
        let kept = |id: NodeId| root.join(format!("n{id}-kept"));
        for id in IDS {
            let host = reopen(&split.home(id), id, split.transport(id), durability).await;
            let g = host.start_group(gid, members(&IDS), id == 1).await.unwrap();
            split.net.nodes.lock().unwrap().insert(id, g);
            split.hosts.insert(id, host);
        }
        wait_leader(&split.node(1)).await;
        setup_run(&split.node(1), 9).await;
        let kept_index = split.node(1).stats().last_log;
        wait_applied(&split.net, &IDS, kept_index).await;
        // What the three's disks will keep: each in turn stops, is copied, and
        // starts again (four keep a quorum).
        for id in LOST {
            let g = split.net.nodes.lock().unwrap().remove(&id).unwrap();
            g.shutdown().await;
            drop((g, split.hosts.remove(&id)));
            let host = reopen(&split.home(id), id, split.transport(id), durability).await;
            copy_dir(&split.home(id), &kept(id));
            let g = host.start_group(gid, members(&IDS), false).await.unwrap();
            split.net.nodes.lock().unwrap().insert(id, g);
            split.hosts.insert(id, host);
        }
        wait_applied(&split.net, &IDS, kept_index).await;
        for i in 0..LOST_ENTRIES {
            propose_among(&split.net, &IDS, &(split.history.lost)(i)).await;
        }
        split.suffix = leader_among(&split.net, &IDS).await.stats().last_log;
        wait_applied(&split.net, &IDS, split.suffix).await;
        (split.history.before_loss)(&split.hosts);
        split.before = SURVIVORS.iter().map(|id| (*id, split.state(*id))).collect();
        for id in SURVIVORS {
            let ready = split.node(id).read(|kv| read::ready(kv, 9, 1000).len()).unwrap();
            assert_eq!(ready as u64, LOST_ENTRIES);
        }

        // The two are cut off; the three lose power together.
        split.cut.lock().unwrap().extend(SURVIVORS);
        for id in LOST {
            let g = split.net.nodes.lock().unwrap().remove(&id).unwrap();
            g.shutdown().await;
            drop((g, split.hosts.remove(&id)));
        }
        for id in LOST {
            std::fs::remove_dir_all(split.home(id)).unwrap();
            copy_dir(&kept(id), &split.home(id));
            // Another boot without a clean stop: a peer that syncs its log
            // rebuilds its state machine's unsynced file.
            std::fs::write(split.home(id).join("raft-sm.state"), "another-boot\nrunning\n").unwrap();
            let host = reopen(&split.home(id), id, split.transport(id), durability).await;
            let g = host.start_group(gid, members(&IDS), false).await.unwrap();
            split.net.nodes.lock().unwrap().insert(id, g);
            split.hosts.insert(id, host);
        }
        propose_among(&split.net, &LOST, &submit(9, 1000)).await;
        for i in 0..split.history.written_while_cut {
            propose_among(&split.net, &LOST, &submit(9, 2000 + i)).await;
        }
        let rewritten = leader_among(&split.net, &LOST).await.stats().last_log;
        assert!(kept_index < rewritten, "the new history rewrites indexes the two committed");
        assert_eq!(rewritten > split.suffix, split.history.written_while_cut > 0, "{rewritten} vs {}", split.suffix);
        if split.history.written_while_cut > 0 {
            let from = leader_among(&split.net, &LOST).await;
            let to = *LOST.iter().find(|id| **id != from.node_id).unwrap();
            from.transfer_leader(to).await.unwrap();
            wait_leader(&split.node(to)).await;
        }
        split
    }
}

/// A group after [`History::build`].
pub(super) struct Split {
    history: History,
    dir: tempfile::TempDir,
    net: Arc<LocalNet>,
    cut: Arc<Mutex<BTreeSet<NodeId>>>,
    hosts: BTreeMap<NodeId, RaftHost>,
    /// The last index the two survivors committed and applied.
    suffix: u64,
    /// The survivors' applied position and state before the loss.
    before: BTreeMap<NodeId, State>,
}

impl Split {
    fn home(&self, id: NodeId) -> PathBuf {
        self.dir.path().join(format!("n{id}"))
    }

    fn transport(&self, id: NodeId) -> Arc<dyn Transport> {
        Arc::new(Partition { net: self.net.clone(), me: id, cut: self.cut.clone() })
    }

    fn node(&self, id: NodeId) -> Group {
        node(&self.net, id)
    }

    fn state(&self, id: NodeId) -> State {
        let g = self.node(id);
        let applied = g.raft.metrics().borrow_watched().last_applied;
        (applied, g.read(|kv| kv.scan_prefix(b"", usize::MAX)).unwrap())
    }

    /// Let the two back in and wait for the divergence hook.
    async fn reconnect(&self) {
        self.cut.lock().unwrap().clear();
        for _ in 0..1200 {
            if diverged(self.history.gid) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let running = |id: NodeId| self.node(id).raft.metrics().borrow_watched().running_state.clone();
        panic!("the hook never fired; the two: {:?}, {:?}", running(1), running(2));
    }

    /// The two come back. Neither takes the new history over what it
    /// applied: the divergence hook hears of the group, the two keep exactly
    /// the state they had, and the three keep committing.
    pub(super) async fn survivors_refuse(self) {
        let stop = Arc::new(AtomicBool::new(false));
        let watch = {
            let (net, stop) = (self.net.clone(), stop.clone());
            let applied: BTreeMap<NodeId, _> = self.before.iter().map(|(id, (a, _))| (*id, *a)).collect();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    for (id, a) in &applied {
                        let now = node(&net, *id).raft.metrics().borrow_watched().last_applied;
                        assert_eq!(&now, a, "node {id} applied past the state it had");
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        self.reconnect().await;
        for i in 1001..1011 {
            propose_among(&self.net, &LOST, &submit(9, i)).await;
        }
        let last = leader_among(&self.net, &LOST).await.stats().last_log;
        assert!(last > self.suffix, "{last} <= {}", self.suffix);
        wait_applied(&self.net, &LOST, last).await;
        for id in LOST {
            let ready = self.node(id).read(|kv| read::ready(kv, 9, 1000).len()).unwrap();
            assert_eq!(ready as u64, 11 + self.history.written_while_cut, "node {id}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        stop.store(true, Ordering::Relaxed);
        watch.await.unwrap();
        for id in SURVIVORS {
            assert!(self.state(id) == self.before[&id], "node {id}'s state changed");
        }
    }
}

/// The two come back to a new history that rewrote indexes they committed
/// and did not reach past their logs' end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_refuse_a_history_that_rewrote_what_they_committed() {
    History::unsynced(23, 0).build().await.survivors_refuse().await;
}

/// The same, but the three write past the last index the two hold, and
/// leadership moves among them, before the two come back. The new leader's
/// first probe then reaches past the end of the two's logs (openraft 0.10.1
/// in a release build takes the entries up to its commit point as matching,
/// appends the rest without asking to truncate, and applies them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivors_refuse_a_longer_history_that_rewrote_what_they_committed() {
    History::unsynced(24, LOST_ENTRIES + 4).build().await.survivors_refuse().await;
}

/// A snapshot from the new history, ending at an index a survivor committed,
/// is refused before openraft sees it: the hook hears of the group, and the
/// survivor's raft keeps running with its state. One from the survivors' own
/// history passes. Handed to openraft directly, the contradicting snapshot
/// stops that voter's raft (an `assert!` that release builds keep).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_survivor_refuses_a_snapshot_that_contradicts_what_it_committed() {
    let split = History::unsynced(26, 0).build().await;
    let gid = split.history.gid;
    let (one, two) = (split.node(1), split.node(2));
    let (vote, meta, path) = snapshot_of(&one, &split.dir.path().join("same.snap")).await;
    assert_eq!(meta.last_log_id.map(|l| l.index), Some(split.suffix));
    let reply = handle_snapshot(&two, &enc(&(vote, meta)).unwrap(), &path).await.unwrap();
    let reply: Result<openraft::raft::SnapshotResponse<TypeConfig>, String> = dec(&reply).unwrap();
    assert!(reply.is_ok(), "{reply:?}");
    assert_eq!(diverged(gid), 0, "the survivors' own history passes");

    let leader = leader_among(&split.net, &LOST).await;
    let (vote, meta, path) = snapshot_of(&leader, &split.dir.path().join("rewritten.snap")).await;
    assert!(meta.last_log_id.is_some_and(|l| l.index <= split.suffix), "{meta:?} vs {}", split.suffix);
    let err = handle_snapshot(&two, &enc(&(vote, meta.clone())).unwrap(), &path).await.unwrap_err();
    assert!(err.to_string().contains("contradicts"), "{err}");
    assert_eq!(diverged(gid), 1);
    assert!(two.raft.with_raft_state(|_| ()).await.is_ok(), "its raft stopped");
    assert!(split.state(2) == split.before[&2], "its state changed");

    let snapshot = openraft::storage::Snapshot { meta, snapshot: std::fs::File::open(&path).unwrap() };
    let installed = tokio::time::timeout(Duration::from_secs(30), one.raft.install_full_snapshot(vote, snapshot)).await;
    assert!(matches!(installed, Ok(Err(_))), "openraft took it: {installed:?}");
}

/// Through whichever of the three leads: add `new` as a learner (once it has
/// caught up), then make the voters the current ones without `old`, with `new`.
async fn reseat(net: &LocalNet, old: NodeId, new: Option<NodeId>) {
    for _ in 0..20 {
        let leader = leader_among(net, &LOST).await;
        if let Some(new) = new {
            let addr = format!("node-{new}");
            let added = tokio::time::timeout(Duration::from_secs(60), leader.add_learner(new, &addr)).await;
            if !matches!(added, Ok(Ok(()))) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
        let mut voters: BTreeSet<NodeId> = leader.view().1.into_iter().collect();
        voters.remove(&old);
        voters.extend(new);
        if leader.change_voters(voters).await.is_ok() {
            return;
        }
    }
    panic!("node {old}'s seat was never given to {new:?}");
}

/// After the hook fires, a survivor's peer marks its raft state untrusted
/// and exits; at its next start it discards that state (the others lead)
/// and joins under a new node id, which the group takes in place of the old
/// one, like any dead voter. Here each survivor stops, its state is wiped,
/// and it starts again as a new node in the same directory. The first is
/// swapped for its old id in one change and catches up from the log. The
/// second's old id is removed first (a leader keeps logs a replication
/// stream still uses), the three snapshot and purge their logs, and it is
/// added and catches up from a snapshot. The group ends with five voters
/// holding the leader's state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diverged_voters_rejoin_as_new_members_and_the_group_is_whole_again() {
    let mut split = History::unsynced(27, 0).build().await;
    let gid = split.history.gid;
    split.reconnect().await;
    for (old, new, from_snapshot) in [(2, 6, false), (1, 7, true)] {
        let g = split.net.nodes.lock().unwrap().remove(&old).unwrap();
        g.shutdown().await;
        drop((g, split.hosts.remove(&old)));
        std::fs::remove_dir_all(split.home(old)).unwrap();
        if from_snapshot {
            reseat(&split.net, old, None).await;
            for id in LOST {
                let g = split.node(id);
                let applied = g.stats().applied;
                g.raft.trigger().snapshot().await.unwrap();
                wait_snapshot_past(&g, applied).await;
                g.raft.trigger().purge_log(applied).await.unwrap();
                let purged = |g: &Group| g.raft.metrics().borrow_watched().purged.is_some_and(|p| p.index >= applied);
                for _ in 0..400 {
                    if purged(&g) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                assert!(purged(&g), "node {id} kept its log");
            }
        }
        let host = reopen(&split.home(old), new, split.transport(new), split.history.durability).await;
        let g = host.start_group(gid, BTreeMap::new(), false).await.unwrap();
        split.net.nodes.lock().unwrap().insert(new, g);
        split.hosts.insert(new, host);
        reseat(&split.net, old, Some(new)).await;
        let snapshots = snapshot_files(&split.home(old)).len();
        assert_eq!(snapshots, usize::from(from_snapshot), "node {new} caught up from the wrong source");
    }

    let five = [3, 4, 5, 6, 7];
    for i in 3000..3005 {
        propose_among(&split.net, &five, &submit(9, i)).await;
    }
    let leader = leader_among(&split.net, &five).await;
    let last = leader.stats().last_log;
    wait_applied(&split.net, &five, last).await;
    let mut stats = leader.stats();
    for _ in 0..400 {
        if stats.max_lag == Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        stats = leader.stats();
    }
    let mut voters = stats.voters.clone();
    voters.sort_unstable();
    assert_eq!((voters, stats.max_lag), (five.to_vec(), Some(0)), "{stats:?}");
    let want = leader.read(|kv| kv.scan_prefix(b"", usize::MAX)).unwrap();
    assert_eq!(leader.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 6, "the call written while cut, and five");
    for id in five {
        let g = split.node(id);
        assert_eq!(g.stats().leader, Some(leader.node_id), "node {id}");
        assert!(g.read(|kv| kv.scan_prefix(b"", usize::MAX)).unwrap() == want, "node {id} holds another state");
    }
}
