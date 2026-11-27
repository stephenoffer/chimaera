//! Which batches the disk writer syncs (`DiskDurability::Durable` syncs only
//! those that need it; `none` syncs votes), what a machine that loses its
//! power keeps of them, a commit point that outlived the entries it covers,
//! and the histories of `diverge` with the lost entries acknowledged
//! unsynced, as `durable` does an attached run's calls.

use super::diverge::{
    History, LOST, LOST_ENTRIES, copy_dir, diverged, divergences, leader_among, node, propose_among, reopen,
    wait_applied,
};
use super::*;
use crate::log_store::{LOG, LOG_META, Syncing, dec};
use openraft::EntryPayload;
use openraft::raft::VoteRequest;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::testing::{blank_ent, log_id, membership_ent};
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use prost::Message;
use redb::{ReadableDatabase, ReadableTable};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// A command entry at `index`, marked sync-optional or not.
fn command(index: u64, optional: bool) -> EntryOf<TypeConfig> {
    let mut c = submit(9, index);
    c.sync_optional = optional;
    let mut e = blank_ent::<TypeConfig>(1, 1, index);
    e.payload = EntryPayload::Normal(Bytes(c.encode_to_vec()));
    e
}

fn marked(i: u64) -> pb::ShardCmd {
    let mut c = submit(9, i);
    c.sync_optional = true;
    c
}

fn vote(term: u64, node: NodeId) -> VoteOf<TypeConfig> {
    openraft::Vote::new(term, node)
}

/// Append `entries` and wait until the writer has completed them.
async fn append(log: &mut LogStore, entries: Vec<EntryOf<TypeConfig>>) {
    let (tx, rx) = TypeConfig::oneshot();
    log.append(entries, IOFlushed::signal(tx)).await.unwrap();
    rx.await.unwrap().unwrap();
}

/// A log database in `dir` on a disk whose syncs take `delay`, counted in `syncs`.
fn log_db(dir: &Path, delay: Duration, syncs: &Arc<AtomicU64>) -> Arc<Database> {
    let disk = backend::OsSyncBackend::open(&dir.join("log.redb"), true).unwrap();
    let disk = SlowDisk { inner: disk, delay, syncs: syncs.clone(), gone: Arc::default() };
    let db = Database::builder().create_with_backend(disk).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(LOG).unwrap();
    txn.open_table(LOG_META).unwrap();
    txn.commit().unwrap();
    Arc::new(db)
}

/// Group `group`'s entry indexes, and its vote and commit point, in `db`.
fn contents(db: &Database, group: u32) -> (Vec<u64>, Option<VoteOf<TypeConfig>>, Option<LogIdOf<TypeConfig>>) {
    let txn = db.begin_read().unwrap();
    let log = txn.open_table(LOG).unwrap();
    let indexes = log.range((group, 0)..=(group, u64::MAX)).unwrap().map(|r| r.unwrap().0.value().1).collect();
    let meta = txn.open_table(LOG_META).unwrap();
    let get = |key: &str| meta.get((group, key)).unwrap().map(|v| v.value().to_vec());
    (indexes, get("vote").map(|b| dec(&b).unwrap()), get("committed").map(|b| dec(&b).unwrap()))
}

/// Copy the files under `from` to `to` as a machine that loses its power
/// now keeps them: the write-ahead log `wal` only up to its last sync, the
/// rest as they are. A redb file keeps its last durable commit (a later one,
/// made without a sync, never wrote the file's header), and the next start
/// replays the wal over it. The wal is copied first: a checkpoint between
/// the copies then leaves a redb that holds what the copied wal held.
fn power_cut(from: &Path, to: &Path, wal: &str, synced: &wal::Synced) {
    std::fs::create_dir_all(to).unwrap();
    let kept = synced.to.load(Ordering::Relaxed);
    std::fs::copy(from.join(wal), to.join(wal)).unwrap();
    std::fs::OpenOptions::new().write(true).open(to.join(wal)).unwrap().set_len(kept).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if src.is_dir() {
            copy_dir(&src, &dst);
        } else if e.file_name() != wal {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

enum Step {
    Append(Vec<EntryOf<TypeConfig>>),
    Committed(u64),
    Vote,
    Truncate(u64),
    Purge(u64),
}

/// A `durable` writer syncs a batch only when one of its operations needs
/// it. Appends of commands marked sync-optional, and commit points, are
/// written unsynced; votes, membership and blank entries, unmarked commands,
/// truncations and purges are synced, and so is a batch that mixes the two.
/// A writer that syncs everything still does. With the wal the file's synced
/// offset shows it; without, redb's own syncs do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_writer_syncs_only_the_batches_that_need_it() {
    for (selective, with_wal) in [(true, true), (true, false), (false, true), (false, false)] {
        let dir = tempfile::tempdir().unwrap();
        let syncs = Arc::new(AtomicU64::new(0));
        let db = log_db(dir.path(), Duration::ZERO, &syncs);
        let wal = with_wal.then(|| wal::Wal::recover(&db, &dir.path().join("log.wal"), false).unwrap().0);
        let mode = if selective { Syncing::Selective } else { Syncing::All };
        let writer = DiskWriter::start(db.clone(), MAX_COMMIT_DELAY, wal, mode).unwrap();
        let (stats, synced) = (writer.stats(), writer.wal_synced.clone());
        let mut log = LogStore::new(1, db.clone(), writer);
        let steps = [
            ("sync-optional commands", true, Step::Append(vec![command(1, true), command(2, true)])),
            ("a commit point", true, Step::Committed(2)),
            ("a vote", false, Step::Vote),
            ("a membership entry", false, Step::Append(vec![membership_ent::<TypeConfig>(1, 1, 3, vec![[1].into()])])),
            ("an unmarked command", false, Step::Append(vec![command(4, false)])),
            ("a blank entry", false, Step::Append(vec![blank_ent::<TypeConfig>(1, 1, 5)])),
            ("marked and unmarked commands", false, Step::Append(vec![command(6, true), command(7, false)])),
            ("a truncation", false, Step::Truncate(6)),
            ("a purge", false, Step::Purge(1)),
            ("a sync-optional command after them", true, Step::Append(vec![command(7, true)])),
        ];
        for (n, (what, optional, step)) in steps.into_iter().enumerate() {
            let (before, syncs_before) = (stats.report(), syncs.load(Ordering::Relaxed));
            let checkpoints = synced.as_ref().map(|s| s.checkpoints.load(Ordering::Relaxed));
            match step {
                Step::Append(e) => append(&mut log, e).await,
                Step::Committed(i) => log.save_committed(Some(log_id::<TypeConfig>(1, 1, i))).await.unwrap(),
                Step::Vote => log.save_vote(&vote(1, 1)).await.unwrap(),
                Step::Truncate(i) => log.truncate_after(Some(log_id::<TypeConfig>(1, 1, i))).await.unwrap(),
                Step::Purge(i) => log.purge(log_id::<TypeConfig>(1, 1, i)).await.unwrap(),
            }
            let r = stats.report();
            let case = format!("{what} (durable: {selective}, wal: {with_wal})");
            let unsynced = selective && optional;
            assert_eq!(r.commits + r.unsynced_commits, n as u64 + 1, "{case}: one batch per step");
            assert_eq!(r.unsynced_commits - before.unsynced_commits, u64::from(unsynced), "{case}");
            match &synced {
                // A checkpoint since the step began makes everything durable,
                // and the offsets say nothing about the step.
                Some(s) if checkpoints == Some(s.checkpoints.load(Ordering::Relaxed)) => {
                    let (to, end) = (s.to.load(Ordering::Relaxed), s.end.load(Ordering::Relaxed));
                    assert_eq!(to < end, unsynced, "{case}: the wal is synced to {to} of {end}");
                }
                Some(_) => {}
                None => {
                    let n = syncs.load(Ordering::Relaxed) - syncs_before;
                    assert_eq!(n == 0, unsynced, "{case}: redb synced {n} times");
                }
            }
        }
        // Synced or not, every entry is readable, in order.
        let got = log.try_get_log_entries(2..=7).await.unwrap();
        assert_eq!(got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![2, 3, 4, 5, 6, 7]);
        assert!(matches!(&got[5].payload, EntryPayload::Normal(d) if sync_optional(&d.0)), "the last append won");
    }
}

/// What a machine that loses its power keeps of a `durable` writer's log:
/// entries and a commit point written unsynced are lost, until a synced
/// batch (a vote) follows them; then they and it survive, and an entry
/// written unsynced after the vote is lost again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsynced_writes_become_durable_with_the_next_synced_batch() {
    for _ in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let db = log_db(dir.path(), Duration::ZERO, &Arc::default());
        let wal = wal::Wal::recover(&db, &dir.path().join("log.wal"), false).unwrap().0;
        let writer = DiskWriter::start(db.clone(), MAX_COMMIT_DELAY, Some(wal), Syncing::Selective).unwrap();
        let synced = writer.wal_synced.clone().unwrap();
        let mut log = LogStore::new(1, db.clone(), writer);
        let cuts = tempfile::tempdir().unwrap();
        let cut = |name: &str| {
            let to = cuts.path().join(name);
            power_cut(dir.path(), &to, "log.wal", &synced);
            let db = Database::create(to.join("log.redb")).unwrap();
            wal::Wal::recover(&db, &to.join("log.wal"), false).unwrap();
            contents(&db, 1)
        };
        append(&mut log, vec![command(1, true), command(2, true)]).await;
        log.save_committed(Some(log_id::<TypeConfig>(1, 1, 2))).await.unwrap();
        let lost = cut("lost");
        log.save_vote(&vote(5, 3)).await.unwrap();
        let kept = cut("kept");
        append(&mut log, vec![command(3, true)]).await;
        let lost_again = cut("lost-again");
        if synced.checkpoints.load(Ordering::Relaxed) > 0 {
            continue; // a checkpoint made everything durable: the cuts say nothing
        }
        assert_eq!(lost, (vec![], None, None), "unsynced writes survived the power loss");
        let committed = Some(log_id::<TypeConfig>(1, 1, 2));
        assert_eq!(kept, (vec![1, 2], Some(vote(5, 3)), committed), "the vote's sync did not cover them");
        assert_eq!(lost_again, (vec![1, 2], Some(vote(5, 3)), committed));
        return;
    }
    panic!("a checkpoint came before the power cuts in every attempt");
}

/// A `durable` writer's timings, and the pace that decides whether it holds
/// batches open, cover synced batches alone: a stream of unsynced ones, each
/// far faster than the disk's sync, leaves them as the synced ones set them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsynced_batches_leave_the_disks_timings_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = log_db(dir.path(), Duration::from_millis(5), &Arc::default());
    let writer = DiskWriter::start(db.clone(), MAX_COMMIT_DELAY, None, Syncing::Selective).unwrap();
    let stats = writer.stats();
    let mut log = LogStore::new(1, db, writer);
    for term in 1..=10 {
        log.save_vote(&vote(term, 1)).await.unwrap();
    }
    let slow = stats.report();
    assert!(slow.commits == 10 && slow.commit_p50_us >= 5000, "{slow:?}");
    for i in 1..=200 {
        append(&mut log, vec![command(i, true)]).await;
    }
    let after = stats.report();
    assert_eq!(after.unsynced_commits, 200);
    assert_eq!(DiskReport { unsynced_commits: 0, ..after }, slow, "unsynced batches changed the synced ones' report");
}

/// A whole group loses power together, under `none`: each voter's disk kept
/// its commit point but lost entries the point covers (written before it,
/// neither synced). Nobody else leads the group, so the voters keep their
/// state and start again from what their disks kept. The commit points are
/// clamped to the logs' ends: unclamped, openraft would replay entries it
/// cannot read and the group would not start, and the divergence guard would
/// take the new leader's first entry, at an index each voter's old point
/// covers, for a rewritten history. Nodes 1 and 2 lost the last entry, node 3
/// its last four; the group comes back without the last call and goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_point_past_the_surviving_log_is_clamped_at_start() {
    const GID: u32 = 35;
    divergences();
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let ids = [1, 2, 3];
    let home = |id: NodeId| dir.path().join(format!("n{id}"));
    let start = |id: NodeId, bootstrap: bool| {
        let (net, home) = (net.clone(), home(id));
        async move {
            let host = reopen(&home, id, net.clone(), DiskDurability::Durable).await;
            let g = host.start_group(GID, members(&ids), bootstrap).await.expect("the voter starts");
            net.nodes.lock().unwrap().insert(id, g);
            host
        }
    };
    let mut hosts = BTreeMap::new();
    for id in ids {
        hosts.insert(id, start(id, id == 1).await);
    }
    wait_leader(&node(&net, 1)).await;
    setup_run(&node(&net, 1), 9).await;
    for i in 0..10 {
        node(&net, 1).propose(&marked(i)).await.unwrap();
    }
    let last = node(&net, 1).stats().last_log;
    wait_applied(&net, &ids, last).await;
    for id in ids {
        let g = net.nodes.lock().unwrap().remove(&id).unwrap();
        g.shutdown().await;
        drop((g, hosts.remove(&id)));
    }

    for (id, lost) in [(1, 1), (2, 1), (3, 4)] {
        let host = reopen(&home(id), id, net.clone(), DiskDurability::Durable).await;
        let (_, _, committed) = contents(&host.log_db, GID);
        assert_eq!(committed.map(|c| c.index), Some(last), "node {id} saved its commit point");
        let txn = host.log_db.begin_write().unwrap();
        txn.open_table(LOG).unwrap().retain_in((GID, last + 1 - lost)..=(GID, u64::MAX), |_, _| false).unwrap();
        txn.commit().unwrap();
        let mut log = LogStore::new(GID, host.log_db.clone(), host.writer.clone());
        let end = log.get_log_state().await.unwrap().last_log_id;
        assert_eq!(end.map(|l| l.index), Some(last - lost));
        assert_eq!(log.read_committed().await.unwrap(), end, "node {id}: openraft is told the log's end");
        assert_eq!(*log.committed.lock().unwrap(), end, "node {id}: the guard holds the log's end");
        drop((log, host));
        // Another boot: the unsynced state machine is rebuilt, and replays the log.
        std::fs::write(home(id).join("raft-sm.state"), "another-boot\nrunning\n").unwrap();
    }
    for id in ids {
        hosts.insert(id, start(id, false).await);
    }
    propose_among(&net, &ids, &marked(100)).await;
    let last = leader_among(&net, &ids).await.stats().last_log;
    wait_applied(&net, &ids, last).await;
    assert_eq!(diverged(GID), 0, "a voter took the group's new history for a rewritten one");
    let state = |id| node(&net, id).read(|kv| kv.scan_prefix(b"", usize::MAX)).unwrap();
    for id in ids {
        assert!(state(id) == state(1), "node {id} holds another state");
        assert_eq!(node(&net, id).read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 10, "node {id}");
    }
}

/// A voter's vote survives its machine losing power, so it cannot vote twice
/// in a term: having granted node 3 term 5, it refuses node 2 that term once
/// it starts again from what its disk kept. Under `durable` the vote is
/// synced in the wal: the cut keeps it, and loses an entry another group
/// wrote unsynced after it. Under `none` its batch syncs the database's file
/// before the vote is answered, and nothing else does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vote_survives_a_power_loss_so_a_voter_cannot_vote_twice_in_a_term() {
    const GID: u32 = 36;
    let ask = |g: Group, candidate: NodeId| async move {
        let r = g.raft.vote(VoteRequest::new(vote(5, candidate), None)).await.unwrap();
        (r.vote_granted, r.vote)
    };
    for durability in [DiskDurability::Durable, DiskDurability::Process] {
        let mut attempts = 0..5;
        let dir = loop {
            assert!(attempts.next().is_some(), "a checkpoint came before the power cut in every attempt");
            let dir = tempfile::tempdir().unwrap();
            let home = dir.path().join("n1");
            let host = reopen(&home, 1, Arc::new(NoTransport), durability).await;
            let g = host.start_group(GID, BTreeMap::new(), false).await.unwrap();
            let synced_votes = || host.disk_stats().vote_syncs.load(Ordering::Relaxed);
            let before = synced_votes();
            assert_eq!(ask(g.clone(), 3).await, (true, vote(5, 3)), "{durability:?}");
            let mut other = LogStore::new(GID + 1, host.log_db.clone(), host.writer.clone());
            append(&mut other, vec![command(1, true), command(2, true)]).await;
            let cut = dir.path().join("cut");
            let disturbed = match &host.writer.wal_synced {
                Some(s) => {
                    power_cut(&home, &cut, "raft-log.wal", s);
                    std::fs::write(cut.join("raft-sm.state"), "another-boot\nrunning\n").unwrap();
                    // A checkpoint made everything durable: the cut says nothing.
                    s.checkpoints.load(Ordering::Relaxed) > 0
                }
                None => {
                    assert_eq!(synced_votes() - before, 1, "under none the vote's batch was not synced alone");
                    copy_dir(&home, &cut); // a page cache cannot be dropped in a test
                    false
                }
            };
            g.shutdown().await;
            if !disturbed {
                break dir;
            }
        };
        let host = reopen(&dir.path().join("cut"), 1, Arc::new(NoTransport), durability).await;
        if durability == DiskDurability::Durable {
            let other = contents(&host.log_db, GID + 1).0;
            assert!(other.is_empty(), "the cut kept entries written unsynced: {other:?}");
        }
        let g = host.start_group(GID, BTreeMap::new(), false).await.unwrap();
        assert_eq!(ask(g.clone(), 2).await, (false, vote(5, 3)), "{durability:?}: it voted twice in term 5");
        g.shutdown().await;
    }
}

fn acknowledged_unsynced(hosts: &BTreeMap<NodeId, RaftHost>) {
    for id in LOST {
        assert!(hosts[&id].disk_stats().report().unsynced_commits > 0, "node {id} synced every append");
    }
}

fn durable(gid: u32, written_while_cut: u64) -> History {
    History {
        gid,
        durability: DiskDurability::Durable,
        lost: marked,
        before_loss: acknowledged_unsynced,
        written_while_cut,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_survivors_refuse_a_history_that_rewrote_what_they_committed() {
    durable(33, 0).build().await.survivors_refuse().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_survivors_refuse_a_longer_history_that_rewrote_what_they_committed() {
    durable(34, LOST_ENTRIES + 4).build().await.survivors_refuse().await;
}
