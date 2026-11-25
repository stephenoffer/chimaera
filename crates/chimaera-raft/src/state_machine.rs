//! The Raft state machine of one shard group, stored in redb.
//!
//! Applies commit without an fsync (`Durability::None`); a durable checkpoint
//! commits every `CHECKPOINT_ENTRIES` applies and always before a snapshot is
//! built. After a crash redb reverts to the last durable commit, `applied_state`
//! reports that position, and openraft replays the committed log tail (the log
//! store persists `committed`). Logs are purged only up to a snapshot, which is
//! taken after a durable checkpoint, so the replay never needs a purged entry.
//!
//! All groups share one database; keys of group `g` are prefixed with `g` (u32
//! big endian), so a group's state is one contiguous range.

use crate::TypeConfig;
use crate::log_store::{dec, enc, io_err};
use crate::snapshot_file;
use chimaera_core::kv::{Kv, KvRead};
use chimaera_core::pb;
use chimaera_core::shard::{Event, ShardState};
use futures_util::StreamExt;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use std::collections::BTreeMap;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const SM: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sm");
pub(crate) const SM_META: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("sm_meta");

/// Durable checkpoint cadence, in applied entries.
const CHECKPOINT_ENTRIES: u64 = 512;
/// A durable checkpoint at least this often while entries are applied: redb
/// cannot reuse pages freed by non-durable commits until a durable one, so a
/// long run of them makes every later commit slower.
pub(crate) const CHECKPOINT_EVERY: std::time::Duration = std::time::Duration::from_millis(500);

/// Log written since a group's last snapshot, against that snapshot's size.
/// A snapshot rewrites the group's whole state, so one is due once the log
/// has outgrown the last snapshot (and `min` bytes): rebuilding then costs no
/// more than the log it replaces, however large the state grows.
#[derive(Debug, Default)]
pub(crate) struct SnapshotDue {
    pub(crate) log_bytes: AtomicU64,
    pub(crate) last_size: AtomicU64,
}

impl SnapshotDue {
    pub(crate) fn due(&self, min: u64) -> bool {
        self.log_bytes.load(Ordering::Relaxed) >= min.max(self.last_size.load(Ordering::Relaxed))
    }
}

/// Events emitted by one apply call of one group, delivered after the commit.
#[derive(Debug)]
pub struct GroupEvents {
    pub group: u32,
    pub events: Vec<Event>,
    /// The group's state was replaced wholesale (a snapshot installed): the
    /// commands it skipped produced no events, so anything waiting on this
    /// replica must look at the state again.
    pub reloaded: bool,
}

pub(crate) fn prefix(group: u32) -> [u8; 4] {
    group.to_be_bytes()
}

/// `Kv` over one group's key range of a redb write transaction.
struct GroupKv<'a, 't> {
    table: &'a mut Table<'t, &'static [u8], &'static [u8]>,
    group: u32,
}

pub(crate) fn with_prefix(group: u32, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len());
    k.extend_from_slice(&prefix(group));
    k.extend_from_slice(key);
    k
}

impl KvRead for GroupKv<'_, '_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let k = with_prefix(self.group, key);
        self.table.get(k.as_slice()).expect("redb read failed").map(|v| v.value().to_vec())
    }
    fn scan_prefix(&self, p: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        scan(self.table, self.group, p, limit)
    }
    fn scan_after(&self, p: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        scan_after(self.table, self.group, p, after, limit)
    }
}

impl Kv for GroupKv<'_, '_> {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let k = with_prefix(self.group, &key);
        self.table.insert(k.as_slice(), value.as_slice()).expect("redb write failed");
    }
    fn delete(&mut self, key: &[u8]) {
        let k = with_prefix(self.group, key);
        self.table.remove(k.as_slice()).expect("redb write failed");
    }
}

fn scan(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    group: u32,
    p: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    scan_after(table, group, p, &[], limit)
}

fn scan_after(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    group: u32,
    p: &[u8],
    after: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    use std::ops::Bound;
    let prefix = with_prefix(group, p);
    let after = with_prefix(group, after);
    let lower = if after.len() > 4 { Bound::Excluded(after.as_slice()) } else { Bound::Included(prefix.as_slice()) };
    let mut out = Vec::new();
    let upper: Bound<&[u8]> = Bound::Unbounded;
    for r in table.range((lower, upper)).expect("redb range failed") {
        let (k, v) = r.expect("redb read failed");
        let k = k.value();
        if !k.starts_with(&prefix) || out.len() >= limit {
            break;
        }
        out.push((k[4..].to_vec(), v.value().to_vec()));
    }
    out
}

/// Read-only view of one group's committed state (served from local storage).
pub struct GroupRead {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    group: u32,
}

impl GroupRead {
    pub(crate) fn open(db: &Database, group: u32) -> io::Result<GroupRead> {
        let txn = db.begin_read().map_err(io_err)?;
        let table = txn.open_table(SM).map_err(io_err)?;
        Ok(GroupRead { table, group })
    }
}

impl KvRead for GroupRead {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let k = with_prefix(self.group, key);
        self.table.get(k.as_slice()).expect("redb read failed").map(|v| v.value().to_vec())
    }
    fn scan_prefix(&self, p: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        scan(&self.table, self.group, p, limit)
    }
    fn scan_after(&self, p: &[u8], after: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        scan_after(&self.table, self.group, p, after, limit)
    }
}

pub struct StateMachine {
    group: u32,
    db: Arc<Database>,
    shard: ShardState,
    events: tokio::sync::mpsc::Sender<GroupEvents>,
    snapshot_dir: PathBuf,
    since_checkpoint: u64,
    last_checkpoint: std::time::Instant,
    due: Arc<SnapshotDue>,
}

impl StateMachine {
    pub fn new(
        group: u32,
        db: Arc<Database>,
        events: tokio::sync::mpsc::Sender<GroupEvents>,
        snapshot_dir: PathBuf,
    ) -> StateMachine {
        StateMachine {
            group,
            db,
            shard: ShardState::new(),
            events,
            snapshot_dir,
            since_checkpoint: 0,
            last_checkpoint: std::time::Instant::now(),
            due: Arc::default(),
        }
    }

    /// Share the group's snapshot accounting with its log store.
    pub(crate) fn with_due(self, due: Arc<SnapshotDue>) -> StateMachine {
        StateMachine { due, ..self }
    }

    fn meta<T: serde::de::DeserializeOwned>(&self, key: &str) -> io::Result<Option<T>> {
        let txn = self.db.begin_read().map_err(io_err)?;
        let t = txn.open_table(SM_META).map_err(io_err)?;
        t.get((self.group, key)).map_err(io_err)?.map(|v| dec(v.value())).transpose()
    }

    /// Make every earlier non-durable commit durable.
    fn checkpoint(db: &Database) -> io::Result<()> {
        let txn = db.begin_write().map_err(io_err)?;
        txn.commit().map_err(io_err)
    }
}

pub(crate) type Responder = Option<openraft::storage::ApplyResponder<TypeConfig>>;

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotData = std::fs::File;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        // Stored as Option<LogId>: a group can apply a blank entry before any log id exists.
        let applied = self.meta::<Option<LogIdOf<TypeConfig>>>("applied")?.flatten();
        let membership = self.meta("membership")?.unwrap_or_default();
        Ok((applied, membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut batch: Vec<(openraft::type_config::alias::EntryOf<TypeConfig>, Responder)> = Vec::new();
        while let Some(item) = entries.next().await {
            batch.push(item?);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.since_checkpoint += batch.len() as u64;
        let durable = self.since_checkpoint >= CHECKPOINT_ENTRIES || self.last_checkpoint.elapsed() >= CHECKPOINT_EVERY;
        if durable {
            self.since_checkpoint = 0;
            self.last_checkpoint = std::time::Instant::now();
        }
        let (db, group, shard) = (self.db.clone(), self.group, self.shard.clone());
        let (replies, events) = tokio::task::spawn_blocking(move || apply_batch(&db, group, &shard, batch, durable))
            .await
            .map_err(io_err)??;
        for (responder, reply) in replies {
            if let Some(r) = responder {
                r.send(reply);
            }
        }
        if !events.is_empty() {
            // A dropped receiver means the peer is shutting down; state is already committed.
            let _ = self.events.send(GroupEvents { group: self.group, events, reloaded: false }).await;
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            group: self.group,
            db: self.db.clone(),
            dir: self.snapshot_dir.clone(),
            due: self.due.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let path = snapshot_path(&self.snapshot_dir, self.group, meta);
        let (db, group, meta2) = (self.db.clone(), self.group, meta.clone());
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            // The file is in place and synced before the commit that points
            // at it, and that one commit replaces the state, `applied`, the
            // membership and the pointer together: a crash leaves either the
            // old snapshot and state or the new ones, never a mix. A file
            // that fails its checks (cut short, changed, or taken for another
            // group or position than `meta`) is refused before the commit, and
            // the group keeps the state and snapshot it had.
            let tmp = path.with_extension("tmp");
            {
                let mut out = std::fs::File::create(&tmp)?;
                io::copy(&mut BufReader::new(snapshot), &mut out)?;
                out.sync_all()?;
            }
            let refused = |e: io::Error| {
                let _ = std::fs::remove_file(&tmp);
                e
            };
            let mut r = snapshot_file::Reader::open(&tmp, group).and_then(|r| r.expect(&meta2)).map_err(refused)?;
            let txn = db.begin_write().map_err(io_err)?;
            let old = {
                let mut t = txn.open_table(SM).map_err(io_err)?;
                let (lo, hi) = (prefix(group), (group + 1).to_be_bytes());
                t.retain_in(lo.as_slice()..hi.as_slice(), |_, _| false).map_err(io_err)?;
                while let Some((k, v)) = r.next_pair().map_err(refused)? {
                    t.insert(with_prefix(group, &k).as_slice(), v.as_slice()).map_err(io_err)?;
                }
                std::fs::rename(&tmp, &path)?;
                snapshot_file::sync_dir(&path)?;
                let mut m = txn.open_table(SM_META).map_err(io_err)?;
                m.insert((group, "applied"), enc(&meta2.last_log_id)?.as_slice()).map_err(io_err)?;
                m.insert((group, "membership"), enc(&meta2.last_membership)?.as_slice()).map_err(io_err)?;
                point_at(&mut m, group, &path)?
            };
            txn.commit().map_err(io_err)?;
            remove_replaced(old, &path);
            Ok(())
        })
        .await
        .map_err(io_err)??;
        self.since_checkpoint = 0;
        let _ = self.events.send(GroupEvents { group: self.group, events: Vec::new(), reloaded: true }).await;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let Some(path) = self.meta::<String>("snapshot")? else { return Ok(None) };
        let (db, group, dir, due) = (self.db.clone(), self.group, self.snapshot_dir.clone(), self.due.clone());
        tokio::task::spawn_blocking(move || {
            // Checked whole before it is sent or trusted at boot: every peer
            // would refuse a damaged one, and no replica could catch up from
            // it. The state it was taken from is still here, so take another.
            let snapshot = match snapshot_file::check(Path::new(&path), group) {
                Ok(meta) => openraft::storage::Snapshot { meta, snapshot: std::fs::File::open(&path)? },
                Err(e) => {
                    tracing::warn!(group, "taking a new snapshot in place of an unusable one: {e}");
                    take_snapshot(&db, group, &dir, &due)?
                }
            };
            Ok(Some(snapshot))
        })
        .await
        .map_err(io_err)?
    }
}

type Replies = Vec<(Responder, crate::Bytes)>;

/// Apply one command to group `group`'s range of `table`.
pub(crate) fn apply_cmd(
    table: &mut Table<'_, &'static [u8], &'static [u8]>,
    group: u32,
    shard: &ShardState,
    cmd: &pb::ShardCmd,
) -> (pb::ShardReply, Vec<Event>) {
    let applied = shard.apply(&mut GroupKv { table, group }, cmd);
    (applied.reply, applied.events)
}

/// Whether run `run` is detached in `kv` (one group's state); None when there is no such run.
pub(crate) fn run_detached(kv: &dyn KvRead, run: &[u8]) -> Option<bool> {
    let id = chimaera_core::ids::Id::from_slice(run).ok()?;
    chimaera_core::shard::read::run(kv, id).map(|r| r.detached)
}

pub(crate) fn apply_batch(
    db: &Database,
    group: u32,
    shard: &ShardState,
    batch: Vec<(openraft::type_config::alias::EntryOf<TypeConfig>, Responder)>,
    durable: bool,
) -> io::Result<(Replies, Vec<Event>)> {
    let mut txn = db.begin_write().map_err(io_err)?;
    if !durable {
        txn.set_durability(Durability::None).map_err(io_err)?;
    }
    let mut replies = Vec::with_capacity(batch.len());
    let mut events = Vec::new();
    {
        let mut table = txn.open_table(SM).map_err(io_err)?;
        let mut meta = txn.open_table(SM_META).map_err(io_err)?;
        let mut last = None;
        for (entry, responder) in batch {
            last = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => replies.push((responder, crate::Bytes::default())),
                EntryPayload::Normal(data) => {
                    let reply = match pb::ShardCmd::decode(data.0.as_slice()) {
                        Ok(cmd) => {
                            let (reply, evs) = apply_cmd(&mut table, group, shard, &cmd);
                            events.extend(evs);
                            reply
                        }
                        Err(e) => pb::ShardReply {
                            body: Some(pb::shard_reply::Body::Rejected(format!("undecodable command: {e}"))),
                        },
                    };
                    replies.push((responder, crate::Bytes(reply.encode_to_vec())));
                }
                EntryPayload::Membership(m) => {
                    let stored: StoredMembershipOf<TypeConfig> = openraft::StoredMembership::new(last, m);
                    meta.insert((group, "membership"), enc(&stored)?.as_slice()).map_err(io_err)?;
                    replies.push((responder, crate::Bytes::default()));
                }
            }
        }
        meta.insert((group, "applied"), enc(&last)?.as_slice()).map_err(io_err)?;
    }
    txn.commit().map_err(io_err)?;
    Ok((replies, events))
}

pub(crate) fn snapshot_path(dir: &std::path::Path, group: u32, meta: &SnapshotMetaOf<TypeConfig>) -> PathBuf {
    let idx = meta.last_log_id.as_ref().map_or(0, |l| l.index);
    dir.join(format!("group-{group}-{idx}.snap"))
}

/// Point group `group` at the snapshot in `path`; returns the file it pointed at before.
fn point_at(
    m: &mut Table<'_, (u32, &'static str), &'static [u8]>,
    group: u32,
    path: &Path,
) -> io::Result<Option<String>> {
    let old = m.get((group, "snapshot")).map_err(io_err)?.map(|v| dec::<String>(v.value())).transpose()?;
    m.insert((group, "snapshot"), enc(&path.to_string_lossy().to_string())?.as_slice()).map_err(io_err)?;
    Ok(old)
}

/// Delete the snapshot a new one replaced. A transfer still reading it keeps
/// its open descriptor, so this never cuts one short.
fn remove_replaced(old: Option<String>, new: &Path) {
    if let Some(old) = old.filter(|o| Path::new(o) != new)
        && let Err(e) = std::fs::remove_file(&old)
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!("could not delete the replaced snapshot {old}: {e}");
    }
}

/// Delete snapshot files no group points at (left by a crash between writing
/// one and committing the pointer, or by older peers that never deleted
/// replaced snapshots), and unfinished transfers in `scratch`.
pub(crate) fn prune_snapshots(db: &Database, dir: &Path, scratch: &Path) -> io::Result<()> {
    let txn = db.begin_read().map_err(io_err)?;
    let m = txn.open_table(SM_META).map_err(io_err)?;
    let mut current = std::collections::HashSet::new();
    for r in m.iter().map_err(io_err)? {
        let (k, v) = r.map_err(io_err)?;
        if k.value().1 == "snapshot" {
            current.insert(PathBuf::from(dec::<String>(v.value())?));
        }
    }
    let stale = |p: &Path, name: &str| {
        (name.starts_with("group-") && !current.contains(p)) || name.ends_with(".tmp") || name.starts_with("incoming-")
    };
    for d in [dir, scratch] {
        for e in std::fs::read_dir(d)?.flatten() {
            let p = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if (name.ends_with(".snap") || name.ends_with(".tmp")) && stale(&p, &name) {
                let _ = std::fs::remove_file(&p);
            }
        }
    }
    Ok(())
}

/// A peer that syncs its raft log never syncs its state machine's file: the
/// state is the log applied, and the log and snapshot files are synced. This
/// marker (the boot it was written in, and whether the process stopped
/// cleanly) says whether the file can be trusted at the next start: a
/// process that died within the same boot left it whole in the page cache; a
/// machine that restarted without a clean shutdown may have left it torn.
const SM_MARKER: &str = "raft-sm.state";

/// Whether the peer under `dir` keeps its state machine unsynced (see SM_MARKER).
pub(crate) fn has_marker(dir: &Path) -> bool {
    dir.join(SM_MARKER).exists()
}

/// Whether the state machine's file under `dir` can be trusted (see SM_MARKER).
pub(crate) fn sm_trusted(dir: &Path) -> bool {
    let Ok(s) = std::fs::read_to_string(dir.join(SM_MARKER)) else {
        return true; // a new directory, or a file an earlier version synced
    };
    let mut it = s.lines();
    let (boot, state) = (it.next().unwrap_or_default(), it.next().unwrap_or_default());
    state == "clean" || crate::boot_id().is_some_and(|b| b == boot)
}

/// Record (synced) that this process runs on the file, or stopped cleanly.
pub(crate) fn mark_sm(dir: &Path, clean: bool) -> io::Result<()> {
    let boot = crate::boot_id().unwrap_or_else(|| "unknown".into());
    let state = if clean { "clean" } else { "running" };
    let (path, tmp) = (dir.join(SM_MARKER), dir.join(format!("{SM_MARKER}.tmp")));
    {
        let mut f = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut f, format!("{boot}\n{state}\n").as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    snapshot_file::sync_dir(&path)
}

/// After a clean stop: sync the state machine's file, then say so.
pub fn mark_state_machine_clean(raft_dir: &Path) -> io::Result<()> {
    if !raft_dir.join(SM_MARKER).exists() {
        return Ok(()); // this peer's state machine is synced as it goes
    }
    let sm = raft_dir.join("raft-sm.redb");
    if sm.exists() {
        std::fs::File::open(&sm)?.sync_all()?;
    }
    mark_sm(raft_dir, true)
}

/// Fill an empty state machine from the newest whole snapshot file of each
/// group in `dir` (its state, position, membership and pointer), as an
/// install would; the log replays the rest. Returns the groups restored.
pub(crate) fn rebuild_from_snapshots(db: &Database, dir: &Path) -> io::Result<usize> {
    let mut files: BTreeMap<u32, Vec<(u64, PathBuf)>> = BTreeMap::new();
    for e in std::fs::read_dir(dir)?.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some((g, idx)) =
            name.strip_prefix("group-").and_then(|r| r.strip_suffix(".snap")).and_then(|r| r.split_once('-'))
        else {
            continue;
        };
        if let (Ok(g), Ok(idx)) = (g.parse::<u32>(), idx.parse::<u64>()) {
            files.entry(g).or_default().push((idx, e.path()));
        }
    }
    let txn = db.begin_write().map_err(io_err)?;
    let mut restored = 0;
    {
        let mut t = txn.open_table(SM).map_err(io_err)?;
        let mut m = txn.open_table(SM_META).map_err(io_err)?;
        for (group, mut candidates) in files {
            candidates.sort_by(|a, b| b.0.cmp(&a.0));
            for (_, path) in candidates {
                // A file that fails its checks is skipped for an older one.
                let Ok(meta) = snapshot_file::check(&path, group) else { continue };
                let mut r = snapshot_file::Reader::open(&path, group)?.expect(&meta)?;
                while let Some((k, v)) = r.next_pair()? {
                    t.insert(with_prefix(group, &k).as_slice(), v.as_slice()).map_err(io_err)?;
                }
                m.insert((group, "applied"), enc(&meta.last_log_id)?.as_slice()).map_err(io_err)?;
                m.insert((group, "membership"), enc(&meta.last_membership)?.as_slice()).map_err(io_err)?;
                point_at(&mut m, group, &path)?;
                restored += 1;
                break;
            }
        }
    }
    txn.commit().map_err(io_err)?;
    Ok(restored)
}

/// Write group `group`'s committed state as a snapshot, point the group at it
/// and delete the one it replaces. The position, membership and state come
/// from one read transaction, so the snapshot is exactly the state at its
/// position even while applies continue. Returns its meta, path and size.
pub(crate) fn write_group_snapshot(
    db: &Database,
    group: u32,
    dir: &Path,
) -> io::Result<(SnapshotMetaOf<TypeConfig>, PathBuf, u64)> {
    let txn = db.begin_read().map_err(io_err)?;
    let meta_t = txn.open_table(SM_META).map_err(io_err)?;
    let get = |k: &str| -> io::Result<Option<Vec<u8>>> {
        Ok(meta_t.get((group, k)).map_err(io_err)?.map(|v| v.value().to_vec()))
    };
    let last_log_id: Option<LogIdOf<TypeConfig>> = get("applied")?.map(|b| dec(&b)).transpose()?.flatten();
    let last_membership: StoredMembershipOf<TypeConfig> =
        get("membership")?.map(|b| dec(&b)).transpose()?.unwrap_or_default();
    let meta = SnapshotMetaOf::<TypeConfig> { last_log_id, last_membership };
    let path = snapshot_path(dir, group, &meta);
    let t = txn.open_table(SM).map_err(io_err)?;
    let (lo, hi) = (prefix(group), (group + 1).to_be_bytes());
    let pairs = t.range(lo.as_slice()..hi.as_slice()).map_err(io_err)?.map(|r| {
        let (k, v) = r.map_err(io_err)?;
        Ok((k.value()[4..].to_vec(), v.value().to_vec()))
    });
    let size = snapshot_file::write(&path, group, &meta, pairs)?;
    let w = db.begin_write().map_err(io_err)?;
    let old = point_at(&mut w.open_table(SM_META).map_err(io_err)?, group, &path)?;
    w.commit().map_err(io_err)?;
    remove_replaced(old, &path);
    Ok((meta, path, size))
}

pub struct SnapshotBuilder {
    group: u32,
    db: Arc<Database>,
    dir: PathBuf,
    due: Arc<SnapshotDue>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = std::fs::File;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, std::fs::File>, io::Error> {
        let (db, group, dir, due) = (self.db.clone(), self.group, self.dir.clone(), self.due.clone());
        tokio::task::spawn_blocking(move || take_snapshot(&db, group, &dir, &due)).await.map_err(io_err)?
    }
}

/// Snapshot group `group` at its applied position and count it in `due`.
fn take_snapshot(
    db: &Database,
    group: u32,
    dir: &Path,
    due: &SnapshotDue,
) -> io::Result<SnapshotOf<TypeConfig, std::fs::File>> {
    // Everything this snapshot covers becomes durable first, so logs
    // purged after it are never needed for a replay.
    StateMachine::checkpoint(db)?;
    due.log_bytes.store(0, Ordering::Relaxed);
    let (meta, path, size) = write_group_snapshot(db, group, dir)?;
    due.last_size.store(size, Ordering::Relaxed);
    Ok(openraft::storage::Snapshot { meta, snapshot: std::fs::File::open(&path)? })
}
