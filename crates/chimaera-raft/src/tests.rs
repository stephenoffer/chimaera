//! Storage conformance (openraft's own suite), restart durability, and a
//! three-node in-process cluster with leader loss.

use super::*;
use chimaera_core::ids::Id;
use chimaera_core::shard::{Event, Tk, read};
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
        let scratch = self.scratch.lock().unwrap().clone().unwrap();
        Box::pin(async move {
            let g = g.ok_or_else(|| std::io::Error::other(format!("node {target} is down")))?;
            handle_rpc(&g, kind, &body, &scratch).await
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
