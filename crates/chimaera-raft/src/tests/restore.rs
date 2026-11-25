//! Backup of a stopped peer's groups, and a cell restored from it into an
//! empty directory as a new node.

use super::*;

/// A single-voter group with 50 submissions, restarted twice (each start
/// elects it again, in a later term), its peer stopped.
async fn stopped_peer(dir: &std::path::Path) {
    for round in 0..3 {
        let (host, rx) = retrying(|| RaftHost::open(dir, 1, Arc::new(NoTransport), DiskDurability::Os)).await;
        std::mem::forget(rx);
        let g = host.start_group(0, members(&[1]), round == 0).await.unwrap();
        wait_leader(&g).await;
        if round == 0 {
            setup_run(&g, 5).await;
        }
        for i in 0..50 {
            g.propose(&submit(5, i)).await.unwrap();
        }
        g.shutdown().await;
    }
}

/// `f` once the old host has let go of the directory's files.
async fn retrying<T>(mut f: impl FnMut() -> std::io::Result<T>) -> T {
    for _ in 0..100 {
        if let Ok(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    f().unwrap()
}

fn untouched(dir: &std::path::Path) -> bool {
    !dir.join("raft-sm.redb").exists() && !dir.join("raft-log.redb").exists()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_restores_into_an_empty_directory_as_a_group_of_the_new_node() {
    let dir = tempfile::tempdir().unwrap();
    let (a, out, b) = (dir.path().join("a"), dir.path().join("backup"), dir.path().join("b"));
    stopped_peer(&a).await;
    let backed = retrying(|| backup(&a, &out)).await;
    assert_eq!(backed.len(), 1);
    assert!(backed[0].index > 50, "every committed write is in the backup: {backed:?}");

    let restored = restore(&b, &backed, 7, "node-7").unwrap();
    assert_eq!(restored.groups, vec![(0, backed[0].index)]);
    assert!(restored.retire.is_empty(), "nothing was granted to a peer: {:?}", restored.retire);
    let (host, rx) = RaftHost::open(&b, 7, Arc::new(NoTransport), DiskDurability::Os).unwrap();
    std::mem::forget(rx);
    let g = host.start_group(0, BTreeMap::new(), false).await.unwrap();
    wait_leader(&g).await;
    g.linearizable().await.unwrap();
    assert_eq!(g.view().1, vec![7], "the new node is the group's only voter");
    assert_eq!(g.read(|kv| read::ready(kv, 5, 1000).len()).unwrap(), 50);
    let r = g.propose(&submit(5, 1)).await.unwrap();
    let Some(pb::shard_reply::Body::Tasks(acks)) = r.body else { panic!("{r:?}") };
    assert_eq!(acks.acks[0].status(), pb::task_ack::Status::Duplicate, "the restored state is the backup's");
    g.propose(&submit(5, 50)).await.unwrap(); // and it takes new writes
    g.shutdown().await;
    // The peer the backup came from still starts on its own state.
    let (host, rx) = retrying(|| RaftHost::open(&a, 1, Arc::new(NoTransport), DiskDurability::Os)).await;
    std::mem::forget(rx);
    let g = host.start_group(0, BTreeMap::new(), false).await.unwrap();
    wait_leader(&g).await;
    assert_eq!(g.read(|kv| read::ready(kv, 5, 1000).len()).unwrap(), 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torn_stale_or_doubled_backup_or_a_used_directory_is_refused_before_anything_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let (a, out) = (dir.path().join("a"), dir.path().join("backup"));
    stopped_peer(&a).await;
    let good = retrying(|| backup(&a, &out)).await;

    let torn = dir.path().join("torn.snap");
    let mut bytes = std::fs::read(&good[0].file).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    std::fs::write(&torn, &bytes).unwrap();
    let stale = Backed { index: good[0].index + 1, ..good[0].clone() };
    let doubled = vec![good[0].clone(), good[0].clone()];
    for (name, backed) in [
        ("torn", vec![Backed { file: torn.clone(), ..good[0].clone() }]),
        ("stale", vec![stale]),
        ("doubled", doubled),
        ("of another group", vec![Backed { group: 3, ..good[0].clone() }]),
    ] {
        let target = dir.path().join(name);
        assert!(restore(&target, &backed, 7, "node-7").is_err(), "{name} was restored");
        assert!(untouched(&target), "{name}: a refused restore wrote raft state");
    }
    // A directory holding a peer's state already: never overwritten.
    assert!(restore(&a, &good, 7, "node-7").is_err());
}
