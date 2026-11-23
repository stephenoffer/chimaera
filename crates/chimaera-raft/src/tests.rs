//! Storage conformance (openraft's own suite), restart durability, and a
//! three-node in-process cluster with leader loss.

use super::*;
use chimaera_core::ids::Id;
use chimaera_core::shard::{Event, Tk, read};
use openraft::async_runtime::WatchReceiver;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

struct Builder;

impl openraft::testing::log::StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for Builder {
    async fn build(&self) -> Result<(tempfile::TempDir, LogStore, StateMachine), openraft::StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().unwrap();
        let log_db = Arc::new(Database::create(dir.path().join("log.redb")).unwrap());
        let sm_db = Arc::new(Database::create(dir.path().join("sm.redb")).unwrap());
        create_tables(&log_db, &sm_db).unwrap();
        std::fs::create_dir_all(dir.path().join("snap")).unwrap();
        let writer = DiskWriter::start(log_db.clone()).unwrap();
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

async fn setup_run(g: &Group, v: u16) {
    // Just after an election leadership can still move: retry until it settles.
    let adopt = cmd(v, pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v as u32], epoch: 1 }));
    let mut r = g.propose(&adopt).await;
    for _ in 0..40 {
        if r.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        r = g.propose(&adopt).await;
    }
    let r = r.unwrap();
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

/// Reads racing the disk writer must never see a gap: an entry being
/// flushed is either still in the unflushed tail or already on disk.
/// (A gap made openraft's replication assert and stall the group.)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn reads_racing_flushes_never_see_a_gap() {
    use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
    use openraft::testing::blank_ent;
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let log_db = Arc::new(open_db(&dir.path().join("log.redb"), DiskDurability::Process).unwrap());
    let sm_db = Arc::new(open_db(&dir.path().join("sm.redb"), DiskDurability::Process).unwrap());
    create_tables(&log_db, &sm_db).unwrap();
    let writer = DiskWriter::start(log_db.clone()).unwrap();
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
    group(2).propose(&submit(9, 1)).await.unwrap();
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
