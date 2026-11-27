//! Storage conformance (openraft's own suite), restart durability, and a
//! three-node in-process cluster with leader loss.

use super::*;
use chimaera_core::ids::Id;
use chimaera_core::shard::{Event, Tk, read};
use openraft::async_runtime::WatchReceiver;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

mod diverge;
mod restore;

struct Builder;

impl openraft::testing::log::StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for Builder {
    async fn build(&self) -> Result<(tempfile::TempDir, LogStore, StateMachine), openraft::StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().unwrap();
        let log_db = Arc::new(Database::create(dir.path().join("log.redb")).unwrap());
        let sm_db = Arc::new(Database::create(dir.path().join("sm.redb")).unwrap());
        create_tables(&log_db, &sm_db).unwrap();
        std::fs::create_dir_all(dir.path().join("snap")).unwrap();
        let writer = DiskWriter::start(log_db.clone(), MAX_COMMIT_DELAY, None).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1 << 16);
        std::mem::forget(rx); // the suite never reads events; keep sends from failing
        let log = LogStore::new(7, log_db, writer);
        let sm = StateMachine::new(7, sm_db, tx, dir.path().join("snap"));
        Ok((dir, log, sm))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openraft_storage_suite_passes() {
    openraft::testing::log::Suite::test_all(Builder).await.unwrap();
}

fn members(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
    ids.iter().map(|i| (*i, BasicNode::new(format!("node-{i}")))).collect()
}

fn cmd(v: u16, body: pb::shard_cmd::Body) -> pb::ShardCmd {
    pb::ShardCmd { vshard: v as u32, vshard_epoch: 1, issuer: None, hlc: 1, body: Some(body) }
}

fn run_id(v: u16) -> Id {
    Id::derive(0, v, &[b"run"])
}

async fn wait_leader(g: &Group) {
    for _ in 0..200 {
        if g.is_leader().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("group {} never elected a leader", g.id);
}

/// Propose `c`, retrying while leadership settles: just after an election or
/// a transfer it can still move, or not be committed yet, and the propose
/// fails meanwhile (`NotLeader`, leader unknown).
async fn propose_settled(g: &Group, c: &pb::ShardCmd) -> pb::ShardReply {
    let mut r = g.propose(c).await;
    for _ in 0..40 {
        if r.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        r = g.propose(c).await;
    }
    r.unwrap()
}

async fn setup_run(g: &Group, v: u16) {
    let adopt = cmd(v, pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v as u32], epoch: 1 }));
    let r = propose_settled(g, &adopt).await;
    assert!(matches!(r.body, Some(pb::shard_reply::Body::Count(1))));
    let run = pb::RunRecord { run: run_id(v).to_vec(), tenant: "t".into(), ..Default::default() };
    g.propose(&cmd(v, pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(run) }))).await.unwrap();
}

fn submit(v: u16, i: u64) -> pb::ShardCmd {
    let run = run_id(v);
    let tk = Tk { run, task: run.child(&[&i.to_le_bytes()]) };
    cmd(
        v,
        pb::shard_cmd::Body::SubmitTasks(pb::SubmitTasks {
            tasks: vec![pb::TaskSpec { key: Some(tk.pb()), ..Default::default() }],
            grant_to: None,
        }),
    )
}

fn task_of(v: u16, i: u64) -> Tk {
    let run = run_id(v);
    Tk { run, task: run.child(&[&i.to_le_bytes()]) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_voter_group_applies_emits_events_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (host, mut events) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Full).unwrap();
        let g = host.start_group(0, members(&[1]), true).await.unwrap();
        wait_leader(&g).await;
        setup_run(&g, 3).await;
        for i in 0..50 {
            g.propose(&submit(3, i)).await.unwrap();
        }
        // Events for the last submit arrive after its commit.
        let mut ready = 0;
        while ready < 50 {
            let ev = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap().unwrap();
            ready += ev.events.iter().filter(|e| matches!(e, Event::Ready(_))).count();
        }
        assert!(g.read(|kv| read::task(kv, task_of(3, 49))).unwrap().is_some());
        g.shutdown().await;
    }
    // Reopen: the group recovers its state from disk and keeps accepting writes.
    // A real restart is a new process; in-process, wait for the old raft tasks
    // and disk writer to release the database lock.
    let mut reopened = None;
    for _ in 0..100 {
        match RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Full) {
            Ok(h) => {
                reopened = Some(h);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let (host, _events) = reopened.expect("database lock released after shutdown");
    let g = host.start_group(0, members(&[1]), true).await.unwrap();
    wait_leader(&g).await;
    g.linearizable().await.unwrap();
    assert_eq!(g.read(|kv| read::ready(kv, 3, 1000).len()).unwrap(), 50);
    let r = g.propose(&submit(3, 1)).await.unwrap();
    let Some(pb::shard_reply::Body::Tasks(acks)) = r.body else { panic!("{r:?}") };
    assert_eq!(acks.acks[0].status(), pb::task_ack::Status::Duplicate, "state survived the restart");
}

/// Routes RPCs to groups of other in-process hosts; removing a node makes it unreachable.
#[derive(Default)]
struct LocalNet {
    nodes: Mutex<BTreeMap<NodeId, Group>>,
    scratch: Mutex<Option<std::path::PathBuf>>,
}

impl Transport for LocalNet {
    fn call(&self, target: NodeId, _addr: &str, _group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture {
        let g = self.nodes.lock().unwrap().get(&target).cloned();
        Box::pin(async move {
            let g = g.ok_or_else(|| std::io::Error::other(format!("node {target} is down")))?;
            handle_rpc(&g, kind, &body).await
        })
    }

    fn send_snapshot(
        &self,
        target: NodeId,
        _addr: &str,
        _group: u32,
        header: Vec<u8>,
        mut file: std::fs::File,
    ) -> RpcFuture {
        let g = self.nodes.lock().unwrap().get(&target).cloned();
        let scratch = self.scratch.lock().unwrap().clone().unwrap();
        Box::pin(async move {
            let g = g.ok_or_else(|| std::io::Error::other(format!("node {target} is down")))?;
            let path = scratch.join(format!("incoming-{target}-{}.snap", std::process::id()));
            std::io::copy(&mut file, &mut std::fs::File::create(&path)?)?;
            let r = handle_snapshot(&g, &header, &path).await;
            let _ = std::fs::remove_file(&path);
            r
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_replicate_and_survive_leader_loss() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let ids = [1u64, 2, 3];
    let mut hosts = Vec::new();
    for id in ids {
        let (host, rx) =
            RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
        net.nodes.lock().unwrap().insert(id, g);
        hosts.push(host);
    }
    let group = |id: NodeId| net.nodes.lock().unwrap().get(&id).cloned().unwrap();
    wait_leader(&group(1)).await;
    let leader = group(1);
    setup_run(&leader, 9).await;
    for i in 0..20 {
        leader.propose(&submit(9, i)).await.unwrap();
    }
    // A follower rejects proposals with a leader hint.
    match group(2).propose(&submit(9, 100)).await {
        Err(ProposeError::NotLeader { leader: Some(1), .. }) => {}
        other => panic!("expected NotLeader(1), got {other:?}"),
    }
    // Kill the leader; one of the survivors takes over with all committed state.
    leader.shutdown().await;
    net.nodes.lock().unwrap().remove(&1);
    let mut new_leader = None;
    for _ in 0..200 {
        for id in [2, 3] {
            if group(id).is_leader().await {
                new_leader = Some(group(id));
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let nl = new_leader.expect("a survivor became leader");
    nl.linearizable().await.unwrap();
    assert_eq!(nl.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 20, "no committed write was lost");
    nl.propose(&submit(9, 20)).await.unwrap();
    assert_eq!(nl.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 21);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement: run with --ignored --nocapture"]
async fn measure_single_voter_propose_latency() {
    let dir = tempfile::tempdir().unwrap();
    let (host, mut events) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os).unwrap();
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    let g = host.start_group(0, members(&[1]), true).await.unwrap();
    wait_leader(&g).await;
    setup_run(&g, 3).await;
    let mut lat = Vec::new();
    for i in 0..300u64 {
        let t = std::time::Instant::now();
        g.propose(&submit(3, i)).await.unwrap();
        lat.push(t.elapsed().as_micros() as u64);
    }
    lat.sort_unstable();
    println!("sequential: p50 {} us, p99 {} us", lat[lat.len() / 2], lat[lat.len() * 99 / 100]);
    let t = std::time::Instant::now();
    let n = 5000u64;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        let g = g.clone();
        set.spawn(async move { g.propose(&submit(3, 10_000 + i)).await.unwrap() });
    }
    while set.join_next().await.is_some() {}
    let el = t.elapsed();
    println!("concurrent: {n} proposals in {el:?} = {:.0}/s", n as f64 / el.as_secs_f64());
}

#[test]
fn a_database_caches_at_most_its_cache_size() {
    // The peer sizes its databases' caches from its memory reserve. Without
    // that size reaching redb, each would cache up to 1 GiB.
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    const T: TableDefinition<u64, &[u8]> = TableDefinition::new("t");
    let dir = tempfile::tempdir().unwrap();
    let cap = 1 << 20;
    let db = open_db(&dir.path().join("c.redb"), DiskDurability::Process, cap).unwrap();
    let value = [7u8; 4096];
    for chunk in 0..16u64 {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(T).unwrap();
            for i in 0..256 {
                table.insert(chunk * 256 + i, value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(T).unwrap();
    for i in 0..4096 {
        assert!(table.get(i).unwrap().is_some());
    }
    let used = db.cache_stats().used_bytes();
    assert!(used > 0 && used <= cap, "16 MiB written and read back; the cache holds {used} bytes");
}

/// Reads racing the disk writer must never see a gap: an entry being
/// flushed is either still in the unflushed tail or already on disk.
/// (A gap made openraft's replication assert and stall the group.)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn reads_racing_flushes_never_see_a_gap() {
    use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
    use openraft::testing::blank_ent;
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let log_db = Arc::new(open_db(&dir.path().join("log.redb"), DiskDurability::Process, DEFAULT_CACHE_BYTES).unwrap());
    let sm_db = Arc::new(open_db(&dir.path().join("sm.redb"), DiskDurability::Process, DEFAULT_CACHE_BYTES).unwrap());
    create_tables(&log_db, &sm_db).unwrap();
    let writer = DiskWriter::start(log_db.clone(), MAX_COMMIT_DELAY, None).unwrap();
    let mut log = LogStore::new(3, log_db, writer);
    let last = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let mut reader = log.get_log_reader().await;
        let (last, stop) = (last.clone(), stop.clone());
        readers.push(tokio::spawn(async move {
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let hi = last.load(Ordering::Acquire);
                if hi < 2 {
                    tokio::task::yield_now().await;
                    continue;
                }
                let lo = hi.saturating_sub(3).max(1);
                let got = reader.try_get_log_entries(lo..=hi).await.unwrap();
                let idx: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
                let want: Vec<u64> = (lo..=hi).collect();
                assert_eq!(idx, want, "a read of {lo}..={hi} saw a gap");
                reads += 1;
            }
            reads
        }));
    }
    let mut next = 1u64;
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(8) {
        let n = 1 + next % 3;
        let entries: Vec<_> = (next..next + n).map(|i| blank_ent::<TypeConfig>(1, 1, i)).collect();
        log.append(entries, IOFlushed::noop()).await.unwrap();
        next += n;
        last.store(next - 1, Ordering::Release);
    }
    stop.store(true, Ordering::Relaxed);
    let mut total = 0;
    for r in readers {
        total += r.await.unwrap();
    }
    assert!(total > 1000, "only {total} reads raced the writer");
}

/// Commits per second with many proposals in flight: one voter vs three, with
/// and without fsync. Run with `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement: run with --ignored --nocapture"]
async fn measure_concurrent_commit_throughput() {
    let only = std::env::var("MEASURE_ONLY").unwrap_or_default();
    for (voters, durability) in [
        (1usize, DiskDurability::Os),
        (3, DiskDurability::Os),
        (1, DiskDurability::Process),
        (3, DiskDurability::Process),
    ] {
        if !only.is_empty() && only != format!("{voters}{durability:?}") {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let net = Arc::new(LocalNet::default());
        *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
        let ids: Vec<NodeId> = (1..=voters as u64).collect();
        let mut hosts = Vec::new();
        for &id in &ids {
            let (host, rx) = RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), durability).unwrap();
            std::mem::forget(rx);
            let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
            net.nodes.lock().unwrap().insert(id, g);
            hosts.push(host);
        }
        let leader = net.nodes.lock().unwrap().get(&1).cloned().unwrap();
        wait_leader(&leader).await;
        setup_run(&leader, 9).await;
        eprintln!("voters={voters} {durability:?}: leader ready");
        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let lat = Arc::new(Mutex::new(Vec::new()));
        let t0 = std::time::Instant::now();
        let mut joins = Vec::new();
        for _ in 0..64 {
            let (g, next, lat) = (leader.clone(), next.clone(), lat.clone());
            joins.push(tokio::spawn(async move {
                while t0.elapsed() < Duration::from_secs(3) {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let t = std::time::Instant::now();
                    tokio::time::timeout(Duration::from_secs(10), g.propose(&submit(9, 1000 + i)))
                        .await
                        .expect("a proposal hung for 10 s")
                        .unwrap();
                    lat.lock().unwrap().push(t.elapsed().as_micros() as u64);
                }
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        let mut l = lat.lock().unwrap().clone();
        l.sort();
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "voters={voters} {durability:?}: {:.0} commits/s, p50 {} us, p99 {} us",
            l.len() as f64 / dt,
            l[l.len() / 2],
            l[l.len() * 99 / 100]
        );
        let groups: Vec<Group> = net.nodes.lock().unwrap().values().cloned().collect();
        for g in groups {
            g.shutdown().await;
        }
    }
}

fn snapshot_files(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("snapshots"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

async fn wait_snapshot_past(g: &Group, index: u64) {
    for _ in 0..200 {
        if g.raft.metrics().borrow_watched().snapshot.is_some_and(|s| s.index >= index) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no snapshot at or after index {index}");
}

/// A new snapshot replaces the old one on disk (they used to pile up: about
/// 8.6 GB per test-suite run), and a restart deletes files no group points at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_snapshot_deletes_the_one_it_replaces() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (host, rx) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&[1]), true).await.unwrap();
        wait_leader(&g).await;
        setup_run(&g, 3).await;
        for round in 0..3u64 {
            for i in 0..20 {
                g.propose(&submit(3, round * 100 + i)).await.unwrap();
            }
            let applied = g.raft.metrics().borrow_watched().last_applied.unwrap().index;
            g.raft.trigger().snapshot().await.unwrap();
            wait_snapshot_past(&g, applied).await;
        }
        assert_eq!(snapshot_files(dir.path()).len(), 1, "{:?}", snapshot_files(dir.path()));
        g.shutdown().await;
    }
    // Leftovers from a crash: an unreferenced snapshot, a half-written one, an unfinished transfer.
    for name in ["snapshots/group-0-1.snap", "snapshots/group-0-99.tmp"] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    std::fs::write(dir.path().join("incoming-0-123.snap"), b"x").unwrap();
    let mut reopened = None;
    for _ in 0..100 {
        match RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os) {
            Ok(h) => {
                reopened = Some(h);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let (host, _rx) = reopened.expect("database lock released");
    assert_eq!(snapshot_files(dir.path()).len(), 1, "{:?}", snapshot_files(dir.path()));
    assert!(!dir.path().join("incoming-0-123.snap").exists());
    // The surviving snapshot is the current one and still loads.
    let g = host.start_group(0, members(&[1]), true).await.unwrap();
    wait_leader(&g).await;
    g.linearizable().await.unwrap();
    assert_eq!(g.read(|kv| read::ready(kv, 3, 1000).len()).unwrap(), 60);
}

/// A peer that syncs its log leaves its state machine's file unsynced: after
/// a machine restart without a clean stop the file may be torn, and is
/// rebuilt from the snapshot file, with the log replaying what came after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_machine_restart_rebuilds_the_state_from_the_snapshot_and_the_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (host, rx) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&[1]), true).await.unwrap();
        wait_leader(&g).await;
        setup_run(&g, 3).await;
        for i in 0..20 {
            g.propose(&submit(3, i)).await.unwrap();
        }
        let applied = g.raft.metrics().borrow_watched().last_applied.unwrap().index;
        g.raft.trigger().snapshot().await.unwrap();
        wait_snapshot_past(&g, applied).await;
        for i in 20..40 {
            g.propose(&submit(3, i)).await.unwrap(); // in the log only
        }
        g.shutdown().await;
    }
    // The machine restarts: the marker is from another boot, and the file
    // never synced is garbage (as a torn one could be).
    std::fs::write(dir.path().join("raft-sm.state"), "another-boot\nrunning\n").unwrap();
    std::fs::write(dir.path().join("raft-sm.redb"), vec![7u8; 4096]).unwrap();
    let mut reopened = None;
    for _ in 0..100 {
        match RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os) {
            Ok(h) => {
                reopened = Some(h);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let (host, _rx) = reopened.expect("reopened after rebuilding the state machine");
    let g = host.start_group(0, members(&[1]), true).await.unwrap();
    wait_leader(&g).await;
    g.linearizable().await.unwrap();
    assert_eq!(g.read(|kv| read::ready(kv, 3, 1000).len()).unwrap(), 40, "20 from the snapshot, 20 replayed");
    let r = g.propose(&submit(3, 39)).await.unwrap();
    let Some(pb::shard_reply::Body::Tasks(acks)) = r.body else { panic!("{r:?}") };
    assert_eq!(acks.acks[0].status(), pb::task_ack::Status::Duplicate, "the replayed call is known");
    let marker = std::fs::read_to_string(dir.path().join("raft-sm.state")).unwrap();
    assert!(marker.ends_with("running\n") && !marker.starts_with("another-boot"), "{marker}");
}

#[test]
fn the_state_machine_file_is_trusted_unless_a_machine_restart_may_have_torn_it() {
    use crate::state_machine::{mark_sm, sm_trusted};
    let dir = tempfile::tempdir().unwrap();
    assert!(sm_trusted(dir.path()), "no marker: new, or synced by an earlier version");
    mark_sm(dir.path(), false).unwrap();
    assert!(sm_trusted(dir.path()), "running in this boot: a process restart keeps the page cache");
    std::fs::write(dir.path().join("raft-sm.state"), "another-boot\nrunning\n").unwrap();
    assert!(!sm_trusted(dir.path()), "another boot, no clean stop: maybe torn");
    std::fs::write(dir.path().join("raft-sm.state"), "another-boot\nclean\n").unwrap();
    assert!(sm_trusted(dir.path()), "stopped cleanly: synced before the restart");
}

/// Snapshots follow the log's size: once the log since the last snapshot
/// outgrows the threshold, one is built without anyone asking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_is_built_once_the_log_outgrows_the_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let (mut host, rx) = RaftHost::open(dir.path(), 1, Arc::new(NoTransport), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    host.snapshot_min_log_bytes = 16 << 10;
    let g = host.start_group(0, members(&[1]), true).await.unwrap();
    wait_leader(&g).await;
    setup_run(&g, 3).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(snapshot_files(dir.path()).is_empty(), "no snapshot before the log reaches the threshold");
    for i in 0..400 {
        g.propose(&submit(3, i)).await.unwrap();
    }
    let applied = g.raft.metrics().borrow_watched().last_applied.unwrap().index;
    wait_snapshot_past(&g, applied.saturating_sub(400)).await;
    assert_eq!(snapshot_files(dir.path()).len(), 1);
}

/// A learner that joins after the leader purged its log catches up from the
/// snapshot, and its installed state and pointer agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_learner_installs_the_leaders_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let (h1, rx) = RaftHost::open(&dir.path().join("n1"), 1, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let leader = h1.start_group(0, members(&[1]), true).await.unwrap();
    net.nodes.lock().unwrap().insert(1, leader.clone());
    wait_leader(&leader).await;
    setup_run(&leader, 9).await;
    for i in 0..30 {
        leader.propose(&submit(9, i)).await.unwrap();
    }
    let applied = leader.raft.metrics().borrow_watched().last_applied.unwrap().index;
    leader.raft.trigger().snapshot().await.unwrap();
    wait_snapshot_past(&leader, applied).await;
    leader.raft.trigger().purge_log(applied).await.unwrap();
    for _ in 0..200 {
        if leader.raft.metrics().borrow_watched().purged.is_some_and(|p| p.index >= applied) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (h2, mut learner_events) = RaftHost::open(&dir.path().join("n2"), 2, net.clone(), DiskDurability::Os).unwrap();
    let learner = h2.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(2, learner.clone());
    tokio::time::timeout(Duration::from_secs(20), leader.add_learner(2, "node-2")).await.unwrap().unwrap();
    for _ in 0..200 {
        if learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap() == 30 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 30);
    assert_eq!(snapshot_files(&dir.path().join("n2")).len(), 1);
    // The commands the snapshot covered raised no events: the install says so,
    // so what waits on this replica looks at its state again.
    let mut reloaded = false;
    while let Ok(ev) = learner_events.try_recv() {
        reloaded |= ev.reloaded && ev.group == 0;
    }
    assert!(reloaded, "installing a snapshot announces that the state was replaced");
    drop((h1, h2));
}

/// Two of three voters are gone for good: the survivor, recovered as the only
/// voter, serves every write it had committed, and a new node then joins and
/// catches up from the snapshot recovery wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_recovers_around_its_one_surviving_voter() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let ids = [1u64, 2, 3];
    {
        let mut hosts = Vec::new();
        for id in ids {
            let (host, rx) =
                RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), DiskDurability::Os).unwrap();
            std::mem::forget(rx);
            let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
            net.nodes.lock().unwrap().insert(id, g);
            hosts.push(host);
        }
        let leader = net.nodes.lock().unwrap().get(&1).cloned().unwrap();
        wait_leader(&leader).await;
        setup_run(&leader, 9).await;
        for i in 0..50 {
            leader.propose(&submit(9, i)).await.unwrap();
        }
        let groups: Vec<Group> = std::mem::take(&mut *net.nodes.lock().unwrap()).into_values().collect();
        for g in groups {
            g.shutdown().await;
        }
    }
    let n1 = dir.path().join("n1");
    let mut resumed = None;
    for _ in 0..100 {
        match force_single_voter(&n1, 0, 1) {
            Ok(i) => {
                resumed = Some(i);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await, // the old host still holds the files
        }
    }
    assert!(resumed.expect("recovered") > 50);
    let (h1, rx) = RaftHost::open(&n1, 1, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let g1 = h1.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(1, g1.clone());
    wait_leader(&g1).await;
    g1.linearizable().await.unwrap();
    assert_eq!(g1.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 50, "every committed write survived");
    assert_eq!(g1.view().1, vec![1]);
    g1.propose(&submit(9, 50)).await.unwrap();
    let (h4, rx) = RaftHost::open(&dir.path().join("n4"), 4, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let g4 = h4.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(4, g4.clone());
    tokio::time::timeout(Duration::from_secs(20), g1.add_learner(4, "node-4")).await.unwrap().unwrap();
    g1.change_voters([1, 4].into()).await.unwrap();
    for _ in 0..200 {
        if g4.read(|kv| read::ready(kv, 9, 1000).len()).unwrap() == 51 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(g4.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 51);
    drop((h1, h4));
}

/// A leader can hand its group to another voter (leader balancing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_hands_its_group_to_another_voter() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let ids = [1u64, 2, 3];
    let mut hosts = Vec::new();
    for id in ids {
        let (host, rx) =
            RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
        net.nodes.lock().unwrap().insert(id, g);
        hosts.push(host);
    }
    let group = |id: NodeId| net.nodes.lock().unwrap().get(&id).cloned().unwrap();
    wait_leader(&group(1)).await;
    setup_run(&group(1), 9).await;
    group(1).transfer_leader(2).await.unwrap();
    wait_leader(&group(2)).await;
    assert!(!group(1).is_leader().await);
    propose_settled(&group(2), &submit(9, 1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_reports_its_leader_voters_and_follower_lag() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    let ids = [1u64, 2, 3];
    let mut hosts = Vec::new();
    for id in ids {
        let (host, rx) =
            RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
        net.nodes.lock().unwrap().insert(id, g);
        hosts.push(host);
    }
    let group = |id: NodeId| net.nodes.lock().unwrap().get(&id).cloned().unwrap();
    wait_leader(&group(1)).await;
    setup_run(&group(1), 9).await;
    for t in 1..=5 {
        group(1).propose(&submit(9, t)).await.unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let leader = loop {
        let s = group(1).stats();
        if (s.max_lag == Some(0) && s.applied == s.last_log) || std::time::Instant::now() > deadline {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!((leader.leader, leader.voters.len(), leader.max_lag), (Some(1), 3, Some(0)), "{leader:?}");
    assert!(leader.last_log >= 6 && leader.applied == leader.last_log, "{leader:?}");
    let follower = group(2).stats();
    assert_eq!((follower.leader, follower.max_lag), (Some(1), None), "only the leader knows the lag: {follower:?}");
    // A voter that stops answering falls behind.
    net.nodes.lock().unwrap().remove(&3);
    for t in 6..=10 {
        group(1).propose(&submit(9, t)).await.unwrap();
    }
    assert_eq!(group(1).stats().max_lag, Some(5), "{:?}", group(1).stats());
}

/// A voter swapped out of a group is never sent the change that removed it;
/// it stops hearing from the leader, whose membership no longer lists it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_swapped_out_voter_stops_hearing_from_the_leader_that_dropped_it() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    let ids = [1u64, 2, 3];
    let mut hosts = Vec::new();
    for id in ids {
        let (host, rx) =
            RaftHost::open(&dir.path().join(format!("n{id}")), id, net.clone(), DiskDurability::Os).unwrap();
        std::mem::forget(rx);
        let g = host.start_group(0, members(&ids), id == 1).await.unwrap();
        net.nodes.lock().unwrap().insert(id, g);
        hosts.push(host);
    }
    let group = |id: NodeId| net.nodes.lock().unwrap().get(&id).cloned().unwrap();
    wait_leader(&group(1)).await;
    setup_run(&group(1), 9).await;
    let (h4, rx) = RaftHost::open(&dir.path().join("n4"), 4, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let g4 = h4.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(4, g4);
    tokio::time::timeout(Duration::from_secs(20), group(1).add_learner(4, "node-4")).await.unwrap().unwrap();
    group(1).change_voters([1, 2, 4].into()).await.unwrap();
    let mut leader = group(1).members();
    leader.sort();
    assert_eq!(leader, vec![1, 2, 4], "the leader lists the new members only");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let gone = group(3).since_leader_contact().expect("it heard from the leader before");
    let kept = group(2).since_leader_contact().expect("a follower hears from the leader");
    assert!(gone > Duration::from_secs(1) && kept < Duration::from_secs(1), "removed {gone:?}, kept {kept:?}");
    assert_eq!(group(1).since_leader_contact(), None, "the leader does not count contact with itself");
    drop(h4);
}

/// A disk whose every sync takes `delay`, as a network volume's does; it
/// counts the syncs it was asked for. Once `gone` is set it takes no more
/// writes, as when the machine loses its power or its volume.
#[derive(Debug)]
struct SlowDisk {
    inner: backend::OsSyncBackend,
    delay: Duration,
    syncs: Arc<std::sync::atomic::AtomicU64>,
    gone: Arc<std::sync::atomic::AtomicBool>,
}

impl SlowDisk {
    fn here(&self) -> Result<(), redb::BackendError> {
        if self.gone.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(std::io::Error::other("the disk is gone").into());
        }
        Ok(())
    }
}

impl redb::StorageBackend for SlowDisk {
    fn len(&self) -> Result<u64, redb::BackendError> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), redb::BackendError> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> Result<(), redb::BackendError> {
        self.here()?;
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> Result<(), redb::BackendError> {
        self.here()?;
        std::thread::sleep(self.delay);
        self.syncs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), redb::BackendError> {
        self.here()?;
        self.inner.write(offset, data)
    }
    fn close(&self) -> Result<(), redb::BackendError> {
        self.inner.close()
    }
    fn try_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.inner.try_lock_range(start, end)
    }
}

/// What a closed loop of writers saw on a slow disk.
struct Loop {
    per_second: f64,
    wait_p99_us: u64,
    disk: DiskReport,
    syncs: u64,
}

/// `clients` groups, each writing one record after another for `run` on a
/// disk whose syncs take `sync`, with batches held open at most `max_hold`.
/// With `with_wal` each batch is first synced in a write-ahead file and then
/// put in redb unsynced, as on every syncing peer (`RaftHost::open`); `sync`
/// then slows only redb's checkpoints, since the wal syncs a plain file.
/// Without it every commit is a synced redb transaction.
async fn closed_loop(clients: u32, sync: Duration, max_hold: Duration, run: Duration, with_wal: bool) -> Loop {
    use openraft::storage::RaftLogStorage;
    let dir = tempfile::tempdir().unwrap();
    let syncs = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let disk = backend::OsSyncBackend::open(&dir.path().join("log.redb"), true).unwrap();
    let db = Database::builder().create_with_backend(SlowDisk {
        inner: disk,
        delay: sync,
        syncs: syncs.clone(),
        gone: Arc::default(),
    });
    let db = Arc::new(db.unwrap());
    let txn = db.begin_write().unwrap();
    txn.open_table(log_store::LOG).unwrap();
    txn.open_table(log_store::LOG_META).unwrap();
    txn.commit().unwrap();
    let wal = with_wal.then(|| wal::Wal::recover(&db, &dir.path().join("log.wal"), false).unwrap().0);
    let writer = DiskWriter::start(db.clone(), max_hold, wal).unwrap();
    let stats = writer.stats();
    let before = syncs.load(std::sync::atomic::Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    let mut joins = Vec::new();
    for g in 0..clients {
        let mut log = LogStore::new(g, db.clone(), writer.clone());
        joins.push(tokio::spawn(async move {
            let mut waits = Vec::new();
            for i in 1.. {
                if t0.elapsed() >= run {
                    break;
                }
                let t = std::time::Instant::now();
                log.save_committed(Some(openraft::testing::log_id::<TypeConfig>(1, 1, i))).await.unwrap();
                waits.push(t.elapsed().as_micros() as u64);
            }
            waits
        }));
    }
    let mut waits = Vec::new();
    for j in joins {
        waits.extend(j.await.unwrap());
    }
    let elapsed = t0.elapsed().as_secs_f64();
    waits.sort_unstable();
    Loop {
        per_second: waits.len() as f64 / elapsed,
        wait_p99_us: waits[waits.len() * 99 / 100],
        disk: stats.report(),
        syncs: syncs.load(std::sync::atomic::Ordering::Relaxed) - before,
    }
}

/// On a disk whose fsync takes 5 ms every commit still syncs (the durability
/// asked for is kept), the slowness shows in the writer's report, and the
/// writer holds batches open (never past its maximum) so more operations
/// share each fsync than when it commits whatever happens to be queued.
/// It runs without the wal, whose sync cannot be slowed here; the writer
/// times a commit, holds and reports the same way on both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_slow_disk_keeps_every_commit_synced_and_batches_more() {
    let sync = Duration::from_millis(5);
    let run = Duration::from_millis(1500);
    let max = Duration::from_micros(1000); // under a quarter of a commit: the maximum binds
    let eager = closed_loop(8, sync, Duration::ZERO, run, false).await;
    let held = closed_loop(8, sync, max, run, false).await;
    let per_commit = |l: &Loop| l.disk.operations as f64 / l.disk.commits as f64;
    for (name, l) in [("eager", &eager), ("held", &held)] {
        eprintln!(
            "{name}: {:.0} ops/s, p99 wait {} us, {:.1} ops per commit, commit p50 {} us, held at most {} us",
            l.per_second,
            l.wait_p99_us,
            per_commit(l),
            l.disk.commit_p50_us,
            l.disk.hold_max_us
        );
        assert!(l.syncs >= l.disk.commits && l.disk.commits > 0, "{name}: a commit skipped its fsync");
        assert!(u64::from(l.disk.commit_p50_us) >= 5000, "{name}: the report hides the slow disk");
    }
    assert_eq!(eager.disk.hold_max_us, 0, "with no hold allowed the writer held a batch open");
    assert!(held.disk.hold_max_us > 0, "the writer never held a batch open on a slow disk");
    assert!(held.disk.hold_max_us <= 1000, "a batch was held {} us, past its maximum", held.disk.hold_max_us);
    assert!(
        per_commit(&held) > per_commit(&eager) * 1.2,
        "holding did not batch more: {:.1} vs {:.1} operations per commit",
        per_commit(&held),
        per_commit(&eager)
    );
}

/// Commits per second and p99 wait on a slow disk, with and without holding
/// batches open. Run with `--ignored --nocapture`; MEASURE_SYNC_US sets the
/// disk's sync time (default 5000) and MEASURE_CLIENTS the writers (16).
/// MEASURE_SYNC_US=0 runs the path a syncing peer takes, through the wal; to
/// measure a real volume, set it and TMPDIR to a directory on the volume. Any
/// other value adds that sleep to redb's sync and runs without the wal, every
/// commit a synced redb transaction, since the wal's sync cannot be slowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement: run with --ignored --nocapture"]
async fn measure_group_commit_hold_on_a_slow_disk() {
    let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let sync = Duration::from_micros(env("MEASURE_SYNC_US", 5000));
    let clients = env("MEASURE_CLIENTS", 16) as u32;
    let with_wal = sync.is_zero();
    let path = if with_wal { "wal" } else { "synced redb" };
    // Each setting twice, in mirrored order, so drift in the machine's load
    // does not favor one.
    for max in [Duration::ZERO, MAX_COMMIT_DELAY, MAX_COMMIT_DELAY, Duration::ZERO] {
        let l = closed_loop(clients, sync, max, Duration::from_secs(5), with_wal).await;
        eprintln!(
            "{path}, sync +{sync:?}, {clients} writers, hold at most {max:?}: {:.0} ops/s, p99 wait {} us, {:.1} \
             ops per commit, commit p50 {} us, p99 {} us, held at most {} us",
            l.per_second,
            l.wait_p99_us,
            l.disk.operations as f64 / l.disk.commits as f64,
            l.disk.commit_p50_us,
            l.disk.commit_p99_us,
            l.disk.hold_max_us
        );
    }
}

/// How a transfer damages the snapshot it carries.
#[derive(Clone, Copy, Debug)]
enum Damage {
    /// Keep only the first bytes, as a stream cut short.
    Cut(usize),
    /// Flip a byte, counted from the end when negative.
    Flip(isize),
}

/// Delivers snapshots damaged in turn as `damage` lists, then whole.
struct Damaging {
    net: Arc<LocalNet>,
    damage: Mutex<std::collections::VecDeque<Damage>>,
}

impl Transport for Damaging {
    fn call(&self, target: NodeId, addr: &str, group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture {
        self.net.call(target, addr, group, kind, body)
    }

    fn send_snapshot(
        &self,
        target: NodeId,
        _addr: &str,
        _group: u32,
        header: Vec<u8>,
        mut file: std::fs::File,
    ) -> RpcFuture {
        use std::io::Read;
        let g = self.net.nodes.lock().unwrap().get(&target).cloned();
        let scratch = self.net.scratch.lock().unwrap().clone().unwrap();
        let damage = self.damage.lock().unwrap().pop_front();
        Box::pin(async move {
            let g = g.ok_or_else(|| std::io::Error::other(format!("node {target} is down")))?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            match damage {
                Some(Damage::Cut(n)) => bytes.truncate(n),
                Some(Damage::Flip(at)) => {
                    let at = if at < 0 { bytes.len() - at.unsigned_abs() } else { at as usize };
                    bytes[at] ^= 0x01;
                }
                None => {}
            }
            let path = scratch.join(format!("incoming-{target}-{}.snap", std::process::id()));
            std::fs::write(&path, &bytes)?;
            let r = handle_snapshot(&g, &header, &path).await;
            let _ = std::fs::remove_file(&path);
            r
        })
    }
}

/// A leader whose group's log before its snapshot is purged, so a new
/// member can only catch up from that snapshot. Returns the snapshot file.
async fn leader_with_a_snapshot(h: &RaftHost, net: &LocalNet, dir: &std::path::Path) -> (Group, std::path::PathBuf) {
    let leader = h.start_group(0, members(&[1]), true).await.unwrap();
    net.nodes.lock().unwrap().insert(1, leader.clone());
    wait_leader(&leader).await;
    setup_run(&leader, 9).await;
    for i in 0..30 {
        leader.propose(&submit(9, i)).await.unwrap();
    }
    let applied = leader.raft.metrics().borrow_watched().last_applied.unwrap().index;
    leader.raft.trigger().snapshot().await.unwrap();
    wait_snapshot_past(&leader, applied).await;
    leader.raft.trigger().purge_log(applied).await.unwrap();
    for _ in 0..200 {
        if leader.raft.metrics().borrow_watched().purged.is_some_and(|p| p.index >= applied) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let file = dir.join("snapshots").join(snapshot_files(dir).pop().unwrap());
    (leader, file)
}

/// A snapshot transfer interrupted at each point (in the meta, in the
/// checksum, before the first record, part way, at the last byte) or changed
/// in transit (the checksum, a record) is refused by the receiving replica
/// without stopping its group, and the next whole transfer installs: the
/// learner ends with the leader's state and one snapshot, never a prefix of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_damaged_snapshot_transfer_is_refused_and_the_next_whole_one_installs() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let damaging = Arc::new(Damaging { net: net.clone(), damage: Mutex::default() });
    let (h1, rx) = RaftHost::open(&dir.path().join("n1"), 1, damaging.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let (leader, file) = leader_with_a_snapshot(&h1, &net, &dir.path().join("n1")).await;
    let bytes = std::fs::read(&file).unwrap();
    // The header is `[u32 len][meta, format, group, hash]`; the records follow it.
    let (size, head) = (bytes.len(), 4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize);
    *damaging.damage.lock().unwrap() = [
        Damage::Cut(0),
        Damage::Cut(6),
        Damage::Cut(head - 16),
        Damage::Cut(head),
        Damage::Cut(size / 2),
        Damage::Cut(size - 1),
        Damage::Flip(head as isize - 1),
        Damage::Flip(size as isize / 2),
        Damage::Flip(-1),
    ]
    .into_iter()
    .collect();
    let (h2, rx) = RaftHost::open(&dir.path().join("n2"), 2, damaging.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let learner = h2.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(2, learner.clone());
    tokio::time::timeout(Duration::from_secs(120), leader.add_learner(2, "node-2")).await.unwrap().unwrap();
    assert!(damaging.damage.lock().unwrap().is_empty(), "every damaged transfer was tried");
    for _ in 0..200 {
        if learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap() == 30 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 30);
    assert_eq!(snapshot_files(&dir.path().join("n2")), snapshot_files(&dir.path().join("n1")));
    let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().flatten().filter(|e| e.path().is_file()).collect();
    assert!(leftovers.is_empty(), "transfers left {leftovers:?}");
    drop((h1, h2));
}

/// Installing a damaged snapshot file over one the group already installed
/// fails before anything changes: the group keeps its state, position and
/// snapshot, and the damaged file is not kept. So does an install whose disk
/// stops taking writes after the new file is in place but before the commit
/// that points at it, once the peer restarts. The whole file then installs
/// and replaces the old snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_damaged_snapshot_install_leaves_the_state_it_had() {
    use openraft::storage::RaftStateMachine;
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    let n1 = dir.path().join("n1");
    let (h1, rx) = RaftHost::open(&n1, 1, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let (leader, first) = leader_with_a_snapshot(&h1, &net, &n1).await;
    let (old_meta, old) = (snapshot_file::check(&first, 0).unwrap(), std::fs::read(&first).unwrap());
    // The leader's next snapshot, ten commands later: the one to install over the first.
    for i in 30..40 {
        leader.propose(&submit(9, i)).await.unwrap();
    }
    let applied = leader.raft.metrics().borrow_watched().last_applied.unwrap().index;
    leader.raft.trigger().snapshot().await.unwrap();
    wait_snapshot_past(&leader, applied).await;
    let name = snapshot_files(&n1).pop().unwrap();
    let file = n1.join("snapshots").join(&name);
    let (meta, good) = (snapshot_file::check(&file, 0).unwrap(), std::fs::read(&file).unwrap());

    let d = dir.path().join("n2");
    std::fs::create_dir_all(d.join("snapshots")).unwrap();
    let log_db = Database::create(d.join("log.redb")).unwrap();
    let gone = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let disk = SlowDisk {
        inner: backend::OsSyncBackend::open(&d.join("sm.redb"), true).unwrap(),
        delay: Duration::ZERO,
        syncs: Arc::default(),
        gone: gone.clone(),
    };
    let sm_db = Arc::new(Database::builder().create_with_backend(disk).unwrap());
    create_tables(&log_db, &sm_db).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    std::mem::forget(rx);
    let mut sm = StateMachine::new(0, sm_db.clone(), tx.clone(), d.join("snapshots"));
    let bad = d.join("bad");
    std::fs::write(&bad, &old).unwrap();
    sm.install_snapshot(&old_meta, std::fs::File::open(&bad).unwrap()).await.unwrap();
    let state = |db: &Database| GroupRead::open(db, 0).unwrap().scan_prefix(b"", usize::MAX);
    let ready = |db: &Database| read::ready(&GroupRead::open(db, 0).unwrap(), 9, 1000).len();
    let (before, files) = (state(&sm_db), snapshot_files(&d));
    assert_eq!((ready(&sm_db), files.len()), (30, 1));

    let mut flipped = good.clone();
    flipped[good.len() / 3] ^= 0x04;
    // Cut, cut by a byte, changed, and whole but taken at another position than the leader said.
    let damaged =
        [(good[..good.len() / 2].to_vec(), &meta), (good[..good.len() - 1].to_vec(), &meta), (flipped, &meta)];
    for (bytes, said) in damaged.into_iter().chain([(good.clone(), &old_meta)]) {
        std::fs::write(&bad, &bytes).unwrap();
        let r = sm.install_snapshot(said, std::fs::File::open(&bad).unwrap()).await;
        assert!(r.is_err(), "a damaged snapshot installed");
        assert_eq!(sm.applied_state().await.unwrap().0, old_meta.last_log_id, "the position moved");
        assert_eq!(sm.get_current_snapshot().await.unwrap().unwrap().meta, old_meta, "the snapshot pointer moved");
        assert!(state(&sm_db) == before, "the state changed");
        assert_eq!(snapshot_files(&d), files);
    }
    // The disk stops taking writes once the whole file is checked and renamed
    // into place, so the commit that points at it fails. The peer restarts:
    // the database has its last commit, and opening deletes the file no group
    // points at (and the temporary file a stop before the rename leaves).
    gone.store(true, std::sync::atomic::Ordering::Relaxed);
    std::fs::write(&bad, &good).unwrap();
    assert!(sm.install_snapshot(&meta, std::fs::File::open(&bad).unwrap()).await.is_err(), "it committed");
    assert!(snapshot_files(&d).contains(&name), "the new file was not in place before the commit");
    std::fs::write(d.join("snapshots").join(&name).with_extension("tmp"), &good).unwrap();
    drop((sm, sm_db));
    let sm_db = Arc::new(Database::create(d.join("sm.redb")).unwrap());
    state_machine::prune_snapshots(&sm_db, &d.join("snapshots"), &d).unwrap();
    let mut sm = StateMachine::new(0, sm_db.clone(), tx, d.join("snapshots"));
    assert_eq!(sm.applied_state().await.unwrap().0, old_meta.last_log_id, "the position moved");
    assert_eq!(sm.get_current_snapshot().await.unwrap().unwrap().meta, old_meta, "the snapshot pointer moved");
    assert!(state(&sm_db) == before, "the state changed");
    assert_eq!(snapshot_files(&d), files);

    std::fs::write(&bad, &good).unwrap();
    sm.install_snapshot(&meta, std::fs::File::open(&bad).unwrap()).await.unwrap();
    assert_eq!(sm.applied_state().await.unwrap().0, meta.last_log_id);
    assert_eq!((ready(&sm_db), snapshot_files(&d)), (40, vec![name]), "the old snapshot is deleted");
    drop(h1);
}

/// A leader whose own snapshot was damaged on disk does not send it on: it
/// takes a new one from its state, and a learner catches up from that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_replaces_a_damaged_snapshot_before_sending_it() {
    let dir = tempfile::tempdir().unwrap();
    let net = Arc::new(LocalNet::default());
    *net.scratch.lock().unwrap() = Some(dir.path().to_path_buf());
    let n1 = dir.path().join("n1");
    let (h1, rx) = RaftHost::open(&n1, 1, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let (leader, file) = leader_with_a_snapshot(&h1, &net, &n1).await;
    let mut bytes = std::fs::read(&file).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01;
    std::fs::write(&file, &bytes).unwrap();
    let (h2, rx) = RaftHost::open(&dir.path().join("n2"), 2, net.clone(), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let learner = h2.start_group(0, BTreeMap::new(), false).await.unwrap();
    net.nodes.lock().unwrap().insert(2, learner.clone());
    tokio::time::timeout(Duration::from_secs(20), leader.add_learner(2, "node-2")).await.unwrap().unwrap();
    for _ in 0..200 {
        if learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap() == 30 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(learner.read(|kv| read::ready(kv, 9, 1000).len()).unwrap(), 30);
    let current = n1.join("snapshots").join(snapshot_files(&n1).pop().unwrap());
    snapshot_file::check(&current, 0).unwrap();
    drop((h1, h2));
}
