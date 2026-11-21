//! A local peer's group without a Raft log (docs/architecture.md D2).
//!
//! A single-member group gains nothing from a replicated log: openraft writes
//! each entry to the log database, then applies it to the state machine
//! database, two redb write transactions per command. Here one thread applies
//! commands itself, in batches of concurrent proposals.
//!
//! What a batch waits for (D2: consensus for lifetimes, not for calls):
//! - A batch of only an ordinary (attached) run's call commands (submit,
//!   grant, publish, release, subscribe, dependency notice) is applied to an
//!   in-memory overlay above the database, with no write transaction.
//! - Anything else (run, actor, store and external-effect commands, and every
//!   command of a detached run, which must resume after a crash) is written
//!   with the overlay in one `Durability::Immediate` commit, which survives
//!   the peer process dying on the local peer's no-sync backend.
//! - The overlay is also written durably once it is `CHECKPOINT_EVERY` old or
//!   holds `MAX_OVERLAY` entries. A crash loses at most that much of an
//!   attached run's calls, whose driver loses its connection to the peer in
//!   the same crash and does not reconnect by itself.
//!
//! Every database commit is durable, so the overlay is exactly what a crash
//! may lose. Reads ([`Applier::read`]) see the overlay over the database.
//! Replies go out after a batch is applied and events after the replies, as in
//! `StateMachine::apply`.

use crate::log_store::io_err;
use crate::state_machine::{CHECKPOINT_EVERY, GroupEvents, SM, apply_cmd, run_detached, with_prefix};
use chimaera_core::kv::{Kv, KvRead};
use chimaera_core::pb;
use chimaera_core::shard::ShardState;
use redb::{Database, Durability, ReadableDatabase, ReadableTable};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::ops::Bound;
use std::sync::{Arc, RwLock, mpsc};
use std::time::Instant;
use tokio::sync::oneshot;

/// Commands applied in one batch at most.
const MAX_BATCH: usize = 1024;
/// Runs whose detached flag is remembered; the cache is cleared beyond this.
const MAX_CACHED_RUNS: usize = 4096;
/// Overlay entries at which the overlay is written out.
const MAX_OVERLAY: usize = 65_536;

type Proposal = (pb::ShardCmd, oneshot::Sender<Result<pb::ShardReply, String>>);

/// Writes not yet in the database, by group-local key (None: deleted), and
/// when the oldest of them was made.
#[derive(Default)]
struct Overlay {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    since: Option<Instant>,
}

pub(crate) struct Applier {
    tx: mpsc::Sender<Proposal>,
    group: u32,
    db: Arc<Database>,
    overlay: Arc<RwLock<Overlay>>,
}

impl Applier {
    pub(crate) fn start(
        group: u32,
        db: Arc<Database>,
        events: tokio::sync::mpsc::Sender<GroupEvents>,
    ) -> io::Result<Applier> {
        let (tx, rx) = mpsc::channel::<Proposal>();
        let overlay = Arc::new(RwLock::new(Overlay::default()));
        let (d, o) = (db.clone(), overlay.clone());
        std::thread::Builder::new().name(format!("apply-{group}")).spawn(move || run(group, &d, &o, &events, &rx))?;
        Ok(Applier { tx, group, db, overlay })
    }

    pub(crate) async fn propose(&self, cmd: pb::ShardCmd) -> Result<pb::ShardReply, String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((cmd, tx)).map_err(|_| "local applier stopped".to_string())?;
        rx.await.map_err(|_| "local applier stopped".to_string())?
    }

    /// Run `f` over the group's state: the overlay over the database.
    pub(crate) fn read<T>(&self, f: impl FnOnce(&dyn KvRead) -> T) -> io::Result<T> {
        let overlay = self.overlay.read().expect("overlay lock");
        let base = self.db.begin_read().map_err(io_err)?.open_table(SM).map_err(io_err)?;
        Ok(f(&View { base: &base, group: self.group, overlay: &overlay.map }))
    }
}

fn run(
    group: u32,
    db: &Database,
    overlay: &RwLock<Overlay>,
    events: &tokio::sync::mpsc::Sender<GroupEvents>,
    rx: &mpsc::Receiver<Proposal>,
) {
    let shard = ShardState::new();
    let mut detached = HashMap::new();
    loop {
        let first = match rx.recv_timeout(CHECKPOINT_EVERY) {
            Ok(p) => p,
            Err(e) => {
                // Idle, or the group is gone: make the overlay durable.
                let mut o = overlay.write().expect("overlay lock");
                if !o.map.is_empty() {
                    let _ = write_out(db, group, &shard, &mut o, &[]);
                }
                if e == mpsc::RecvTimeoutError::Disconnected {
                    return;
                }
                continue;
            }
        };
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(p) => batch.push(p),
                Err(_) => break,
            }
        }
        if detached.len() > MAX_CACHED_RUNS {
            detached.clear();
        }
        match apply(db, group, &shard, overlay, &batch, &mut detached) {
            Ok((replies, evs)) => {
                for ((_, tx), reply) in batch.into_iter().zip(replies) {
                    let _ = tx.send(Ok(reply));
                }
                if !evs.is_empty() {
                    // A dropped receiver means the peer is shutting down; state is applied.
                    let _ = events.blocking_send(GroupEvents { group, events: evs });
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for (_, tx) in batch {
                    let _ = tx.send(Err(msg.clone()));
                }
            }
        }
    }
}

type Applied = (Vec<pb::ShardReply>, Vec<chimaera_core::shard::Event>);

/// Apply `batch`: to the overlay when it holds only an attached run's call
/// commands and the overlay is young and small, otherwise durably with it.
fn apply(
    db: &Database,
    group: u32,
    shard: &ShardState,
    overlay: &RwLock<Overlay>,
    batch: &[Proposal],
    detached: &mut HashMap<Vec<u8>, bool>,
) -> io::Result<Applied> {
    let mut o = overlay.write().expect("overlay lock");
    let full = o.map.len() >= MAX_OVERLAY || o.since.is_some_and(|t| t.elapsed() >= CHECKPOINT_EVERY);
    let base = db.begin_read().map_err(io_err)?.open_table(SM).map_err(io_err)?;
    let durable = full || {
        let view = View { base: &base, group, overlay: &o.map };
        batch.iter().any(|(cmd, _)| needs_durability(&view, cmd, detached))
    };
    if durable {
        drop(base);
        return write_out(db, group, shard, &mut o, batch);
    }
    let o = &mut *o;
    o.since.get_or_insert_with(Instant::now);
    let mut kv = OverlayKv { base: &base, group, overlay: &mut o.map };
    let mut out: Applied = (Vec::with_capacity(batch.len()), Vec::new());
    for (cmd, _) in batch {
        let applied = shard.apply(&mut kv, cmd);
        out.0.push(applied.reply);
        out.1.extend(applied.events);
    }
    Ok(out)
}

/// Write the overlay, then apply `batch`, in one durable commit; the overlay
/// is empty afterwards.
fn write_out(
    db: &Database,
    group: u32,
    shard: &ShardState,
    o: &mut Overlay,
    batch: &[Proposal],
) -> io::Result<Applied> {
    let mut txn = db.begin_write().map_err(io_err)?;
    txn.set_durability(Durability::Immediate).map_err(io_err)?;
    let mut out: Applied = (Vec::with_capacity(batch.len()), Vec::new());
    {
        let mut table = txn.open_table(SM).map_err(io_err)?;
        for (k, v) in &o.map {
            let key = with_prefix(group, k);
            match v {
                Some(v) => table.insert(key.as_slice(), v.as_slice()).map(|_| ()),
                None => table.remove(key.as_slice()).map(|_| ()),
            }
            .map_err(io_err)?;
        }
        for (cmd, _) in batch {
            let (reply, evs) = apply_cmd(&mut table, group, shard, cmd);
            out.0.push(reply);
            out.1.extend(evs);
        }
    }
    txn.commit().map_err(io_err)?;
    *o = Overlay::default();
    Ok(out)
}

/// False only for an ordinary call's commands whose tasks all belong to runs
/// known not to be detached; anything else must survive the peer dying.
fn needs_durability(view: &dyn KvRead, cmd: &pb::ShardCmd, detached: &mut HashMap<Vec<u8>, bool>) -> bool {
    use pb::shard_cmd::Body;
    let keys: Vec<&pb::TaskKey> = match &cmd.body {
        Some(Body::SubmitTasks(s)) => s.tasks.iter().filter_map(|t| t.key.as_ref()).collect(),
        Some(Body::Grant(g)) => g.grants.iter().filter_map(|g| g.task.as_ref()).collect(),
        Some(Body::Publish(p)) => p.entries.iter().filter_map(|e| e.task.as_ref()).collect(),
        Some(Body::Release(r)) => r.tasks.iter().collect(),
        Some(Body::Subscribe(s)) => s.producer.iter().collect(),
        Some(Body::DepNotify(d)) => d.producer.iter().collect(),
        _ => return true,
    };
    keys.iter().any(|k| {
        if let Some(&d) = detached.get(&k.run) {
            return d;
        }
        match run_detached(view, &k.run) {
            Some(d) => *detached.entry(k.run.clone()).or_insert(d),
            None => true, // unknown run: be safe
        }
    })
}

/// The overlay over one group's range of the database, read as `MemKv` reads
/// its map: a scan starts at the prefix (or just after `after`) and stops at
/// the first key that lacks the prefix.
struct View<'a, T: ReadableTable<&'static [u8], &'static [u8]>> {
    base: &'a T,
    group: u32,
    overlay: &'a BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> KvRead for View<'_, T> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.overlay.get(key) {
            Some(v) => v.clone(),
            None => self
                .base
                .get(with_prefix(self.group, key).as_slice())
                .expect("redb read failed")
                .map(|v| v.value().to_vec()),
        }
    }
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_after(prefix, &[], limit)
    }
    fn scan_after(&self, prefix: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = if after.is_empty() { Bound::Included(prefix) } else { Bound::Excluded(after) };
        let mut over = self.overlay.range::<[u8], _>((start, Bound::Unbounded)).peekable();
        let lo = with_prefix(self.group, if after.is_empty() { prefix } else { after });
        let hi = (self.group + 1).to_be_bytes();
        let lower: Bound<&[u8]> =
            if after.is_empty() { Bound::Included(lo.as_slice()) } else { Bound::Excluded(lo.as_slice()) };
        let upper: Bound<&[u8]> = Bound::Excluded(hi.as_slice());
        let mut base = self.base.range((lower, upper)).expect("redb range failed").map(|r| {
            let (k, v) = r.expect("redb read failed");
            (k.value()[4..].to_vec(), v.value().to_vec())
        });
        let mut next_base = base.next();
        let mut out = Vec::new();
        while out.len() < limit {
            let from_overlay = match (over.peek(), &next_base) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((ok, _)), Some((bk, _))) => ok.as_slice() <= bk.as_slice(),
            };
            let (key, value) = if from_overlay {
                let (k, v) = over.next().expect("peeked");
                if next_base.as_ref().is_some_and(|(bk, _)| bk == k) {
                    // mutant: shadow not skipped
                }
                match v {
                    Some(v) => (k.clone(), v.clone()),
                    None => continue, // deleted
                }
            } else {
                let kv = next_base.take().expect("checked");
                next_base = base.next();
                kv
            };
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key, value));
        }
        out
    }
}

/// Applies commands to the overlay: reads see the overlay over the database,
/// writes go to the overlay.
struct OverlayKv<'a, T: ReadableTable<&'static [u8], &'static [u8]>> {
    base: &'a T,
    group: u32,
    overlay: &'a mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> OverlayKv<'_, T> {
    fn view(&self) -> View<'_, T> {
        View { base: self.base, group: self.group, overlay: self.overlay }
    }
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> KvRead for OverlayKv<'_, T> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.view().get(key)
    }
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.view().scan_prefix(prefix, limit)
    }
    fn scan_after(&self, prefix: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.view().scan_after(prefix, after, limit)
    }
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> Kv for OverlayKv<'_, T> {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.overlay.insert(key, Some(value));
    }
    fn delete(&mut self, key: &[u8]) {
        self.overlay.insert(key.to_vec(), None);
    }
}

#[cfg(test)]
mod tests {
    use crate::{DiskDurability, NoTransport, RaftHost};
    use chimaera_core::ids::Id;
    use chimaera_core::pb;
    use chimaera_core::shard::{Event, Tk, read};
    use openraft::BasicNode;
    use std::sync::Arc;
    use std::time::Duration;

    fn cmd(body: pb::shard_cmd::Body) -> pb::ShardCmd {
        pb::ShardCmd { vshard: 3, vshard_epoch: 1, issuer: None, hlc: 1, body: Some(body) }
    }

    fn submit(i: u64) -> pb::ShardCmd {
        let run = Id::derive(0, 3, &[b"run"]);
        let tk = Tk { run, task: run.child(&[&i.to_le_bytes()]) };
        let spec = pb::TaskSpec { key: Some(tk.pb()), ..Default::default() };
        cmd(pb::shard_cmd::Body::SubmitTasks(pb::SubmitTasks { tasks: vec![spec], grant_to: None }))
    }

    /// Open the host under `dir` (retrying while a previous one releases the
    /// database lock), start group 0 as a local peer does, and return it.
    async fn open(
        dir: &std::path::Path,
        log: bool,
    ) -> (RaftHost, tokio::sync::mpsc::Receiver<crate::GroupEvents>, crate::Group) {
        for _ in 0..100 {
            if let Ok((host, events)) = RaftHost::open(dir, 1, Arc::new(NoTransport), DiskDurability::Process) {
                let g = host.start_group(0, [(1, BasicNode::new("local"))].into(), true).await.unwrap();
                for _ in 0..200 {
                    if g.is_leader().await {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                let g = if log { g } else { host.without_log(g).await.unwrap() };
                return (host, events, g);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("database lock never released");
    }

    /// The overlay over the database reads exactly like one map, through
    /// random writes, deletes and write-outs, with other groups' keys on both
    /// sides of the group's range.
    #[test]
    fn the_overlay_reads_like_one_map() {
        use super::{OverlayKv, SM, View, with_prefix, write_out};
        use chimaera_core::kv::{Kv, KvRead, MemKv};
        use redb::{Database, ReadableDatabase};
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("sm.redb")).unwrap();
        let group = 1;
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(SM).unwrap();
                for g in [0, 2] {
                    for k in [&b""[..], b"a", b"\xff\xff\xff"] {
                        t.insert(with_prefix(g, k).as_slice(), &b"other"[..]).unwrap();
                    }
                }
            }
            txn.commit().unwrap();
        }
        let shard = chimaera_core::shard::ShardState::new();
        let (mut overlay, mut model) = (super::Overlay::default(), MemKv::default());
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = [0x00u8, 0x01, b'a', b'b', 0xff];
        let key = |n: usize, r: &mut dyn FnMut() -> u64| -> Vec<u8> {
            (0..n).map(|_| alphabet[(r() % alphabet.len() as u64) as usize]).collect()
        };
        for step in 0..20_000u64 {
            let op = next() % 10;
            if op == 7 {
                write_out(&db, group, &shard, &mut overlay, &[]).unwrap();
                continue;
            }
            let base = db.begin_read().unwrap().open_table(SM).unwrap();
            if op < 7 {
                let n = 1 + (next() % 3) as usize;
                let k = key(n, &mut next);
                let mut kv = OverlayKv { base: &base, group, overlay: &mut overlay.map };
                if op < 5 {
                    let v = step.to_le_bytes().to_vec();
                    kv.put(k.clone(), v.clone());
                    model.put(k, v);
                } else {
                    kv.delete(&k);
                    model.delete(&k);
                }
                continue;
            }
            let view = View { base: &base, group, overlay: &overlay.map };
            let prefix = key((next() % 3) as usize, &mut next);
            let after = if next() % 2 == 0 { Vec::new() } else { key(1 + (next() % 3) as usize, &mut next) };
            let limit = (next() % 6) as usize;
            assert_eq!(
                view.scan_after(&prefix, &after, limit),
                model.scan_after(&prefix, &after, limit),
                "step {step}"
            );
            assert_eq!(view.scan_prefix(&prefix, limit), model.scan_prefix(&prefix, limit), "step {step}");
            let k = key(1 + (next() % 3) as usize, &mut next);
            assert_eq!(view.get(&k), model.get(&k), "step {step}");
        }
    }

    fn ready(g: &crate::Group) -> usize {
        g.read(|kv| read::ready(kv, 3, 10_000).len()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn applies_without_a_log_and_survives_restarts_from_either_mode() {
        let dir = tempfile::tempdir().unwrap();
        {
            // A data directory last run with the Raft log.
            let (_host, _events, g) = open(dir.path(), true).await;
            g.propose(&cmd(pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![3], epoch: 1 })))
                .await
                .unwrap();
            let run =
                pb::RunRecord { run: Id::derive(0, 3, &[b"run"]).to_vec(), tenant: "t".into(), ..Default::default() };
            g.propose(&cmd(pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(run) }))).await.unwrap();
            for i in 0..20 {
                g.propose(&submit(i)).await.unwrap();
            }
            g.shutdown().await;
        }
        {
            let (_host, mut events, g) = open(dir.path(), false).await;
            assert_eq!(ready(&g), 20, "the Raft log's writes are recovered before switching");
            assert!(g.is_leader().await);
            assert_eq!(g.view(), (Some(1), vec![1]));
            // Concurrent proposals: each gets its own reply, batched or not.
            let replies = futures_util::future::join_all((20..50).map(|i| {
                let g = g.clone();
                async move { g.propose(&submit(i)).await.unwrap() }
            }))
            .await;
            for r in &replies {
                let Some(pb::shard_reply::Body::Tasks(acks)) = &r.body else { panic!("{r:?}") };
                assert_eq!(acks.acks[0].status(), pb::task_ack::Status::Accepted);
            }
            let mut seen = 0;
            while seen < 30 {
                let ev = tokio::time::timeout(Duration::from_secs(5), events.recv()).await.unwrap().unwrap();
                seen += ev.events.iter().filter(|e| matches!(e, Event::Ready(_))).count();
            }
            assert_eq!(ready(&g), 50);
            g.shutdown().await;
        }
        let (_host, _events, g) = open(dir.path(), false).await;
        assert_eq!(ready(&g), 50, "writes applied without the log survive a restart");
        let r = g.propose(&submit(7)).await.unwrap();
        let Some(pb::shard_reply::Body::Tasks(acks)) = r.body else { panic!("{r:?}") };
        assert_eq!(acks.acks[0].status(), pb::task_ack::Status::Duplicate);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "measurement: run with --ignored --nocapture"]
    async fn measure_propose_with_and_without_the_log() {
        for log in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let (_host, mut events, g) = open(dir.path(), log).await;
            tokio::spawn(async move { while events.recv().await.is_some() {} });
            g.propose(&cmd(pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![3], epoch: 1 })))
                .await
                .unwrap();
            let run =
                pb::RunRecord { run: Id::derive(0, 3, &[b"run"]).to_vec(), tenant: "t".into(), ..Default::default() };
            g.propose(&cmd(pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(run) }))).await.unwrap();
            let mut lat = Vec::new();
            for i in 0..2000u64 {
                let t = std::time::Instant::now();
                g.propose(&submit(i)).await.unwrap();
                lat.push(t.elapsed().as_micros() as u64);
            }
            lat.sort_unstable();
            let t = std::time::Instant::now();
            let n = 20_000u64;
            let mut set = tokio::task::JoinSet::new();
            for i in 0..n {
                let g = g.clone();
                set.spawn(async move { g.propose(&submit(100_000 + i)).await.unwrap() });
            }
            while set.join_next().await.is_some() {}
            let el = t.elapsed();
            println!(
                "{}: sequential p50 {} us, p99 {} us; concurrent {:.0}/s",
                if log { "raft log" } else { "no log" },
                lat[lat.len() / 2],
                lat[lat.len() * 99 / 100],
                n as f64 / el.as_secs_f64()
            );
            g.shutdown().await;
        }
    }

    const CRASH_DIR: &str = "CHIMAERA_TEST_CRASH_DIR";
    const CRASH_MODE: &str = "CHIMAERA_TEST_CRASH_MODE";

    /// The child of the crash tests: create a run (detached or not), submit 40
    /// calls, optionally sit idle, then die without any shutdown or drop. A
    /// no-op unless the parent set CRASH_DIR.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_child() {
        let Ok(dir) = std::env::var(CRASH_DIR) else { return };
        let mode = std::env::var(CRASH_MODE).unwrap();
        let (_host, _events, g) = open(std::path::Path::new(&dir), false).await;
        g.propose(&cmd(pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![3], epoch: 1 })))
            .await
            .unwrap();
        let run = pb::RunRecord {
            run: Id::derive(0, 3, &[b"run"]).to_vec(),
            tenant: "t".into(),
            detached: mode == "detached",
            ..Default::default()
        };
        g.propose(&cmd(pb::shard_cmd::Body::RunCreate(pb::RunCreate { run: Some(run) }))).await.unwrap();
        for i in 0..40 {
            g.propose(&submit(i)).await.unwrap();
        }
        if mode == "idle" {
            std::thread::sleep(super::CHECKPOINT_EVERY * 3);
        }
        std::process::abort();
    }

    /// Run `crash_child` in `mode`, then reopen its directory.
    async fn after_crash(mode: &str) -> (tempfile::TempDir, crate::Group) {
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "local::tests::crash_child", "--nocapture"])
            .env(CRASH_DIR, dir.path())
            .env(CRASH_MODE, mode)
            .status()
            .unwrap();
        assert!(!status.success(), "the child aborts after its writes");
        let (_host, _events, g) = open(dir.path(), false).await;
        (dir, g)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_detached_runs_calls_survive_the_process_dying_at_once() {
        let (_dir, g) = after_crash("detached").await;
        assert_eq!(ready(&g), 40, "every acknowledged call of a detached run is on disk");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_attached_runs_calls_are_durable_within_the_checkpoint_window() {
        let (_dir, g) = after_crash("idle").await;
        assert_eq!(ready(&g), 40, "the idle checkpoint made the attached run's calls durable");
    }
}
