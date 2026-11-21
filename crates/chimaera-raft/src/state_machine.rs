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
use chimaera_core::kv::{Kv, KvRead};
use chimaera_core::pb;
use chimaera_core::shard::{Event, ShardState};
use futures_util::StreamExt;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, OptionalSend};
use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) const SM: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sm");
pub(crate) const SM_META: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("sm_meta");

/// Durable checkpoint cadence, in applied entries.
const CHECKPOINT_ENTRIES: u64 = 10_000;

/// Events emitted by one apply call of one group, delivered after the commit.
#[derive(Debug)]
pub struct GroupEvents {
    pub group: u32,
    pub events: Vec<Event>,
}

pub(crate) fn prefix(group: u32) -> [u8; 4] {
    group.to_be_bytes()
}

/// `Kv` over one group's key range of a redb write transaction.
struct GroupKv<'a, 't> {
    table: &'a mut Table<'t, &'static [u8], &'static [u8]>,
    group: u32,
}

fn with_prefix(group: u32, key: &[u8]) -> Vec<u8> {
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
}

impl StateMachine {
    pub fn new(
        group: u32,
        db: Arc<Database>,
        events: tokio::sync::mpsc::Sender<GroupEvents>,
        snapshot_dir: PathBuf,
    ) -> StateMachine {
        StateMachine { group, db, shard: ShardState::new(), events, snapshot_dir, since_checkpoint: 0 }
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

type Responder = Option<openraft::storage::ApplyResponder<TypeConfig>>;

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
        let durable = self.since_checkpoint >= CHECKPOINT_ENTRIES;
        if durable {
            self.since_checkpoint = 0;
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
            let _ = self.events.send(GroupEvents { group: self.group, events }).await;
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder { group: self.group, db: self.db.clone(), dir: self.snapshot_dir.clone() }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let path = snapshot_path(&self.snapshot_dir, self.group, meta);
        let (db, group, meta2) = (self.db.clone(), self.group, meta.clone());
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let mut r = BufReader::new(snapshot);
            let (_, pairs) = read_snapshot(&mut r)?;
            let txn = db.begin_write().map_err(io_err)?;
            {
                let mut t = txn.open_table(SM).map_err(io_err)?;
                let (lo, hi) = (prefix(group), (group + 1).to_be_bytes());
                t.retain_in(lo.as_slice()..hi.as_slice(), |_, _| false).map_err(io_err)?;
                for (k, v) in &pairs {
                    t.insert(with_prefix(group, k).as_slice(), v.as_slice()).map_err(io_err)?;
                }
                let mut m = txn.open_table(SM_META).map_err(io_err)?;
                m.insert((group, "applied"), enc(&meta2.last_log_id)?.as_slice()).map_err(io_err)?;
                m.insert((group, "membership"), enc(&meta2.last_membership)?.as_slice()).map_err(io_err)?;
            }
            txn.commit().map_err(io_err)?;
            write_snapshot_file(&path, &meta2, &pairs)?;
            set_current_snapshot(&db, group, &path)
        })
        .await
        .map_err(io_err)??;
        self.since_checkpoint = 0;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let Some(path) = self.meta::<String>("snapshot")? else { return Ok(None) };
        let mut f = std::fs::File::open(&path)?;
        let (meta, _) = read_snapshot(&mut BufReader::new(&mut f))?;
        let f = std::fs::File::open(&path)?;
        Ok(Some(openraft::storage::Snapshot { meta, snapshot: f }))
    }
}

type Replies = Vec<(Responder, crate::Bytes)>;

fn apply_batch(
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
                            let mut kv = GroupKv { table: &mut table, group };
                            let applied = shard.apply(&mut kv, &cmd);
                            events.extend(applied.events);
                            applied.reply
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

fn snapshot_path(dir: &std::path::Path, group: u32, meta: &SnapshotMetaOf<TypeConfig>) -> PathBuf {
    let idx = meta.last_log_id.as_ref().map_or(0, |l| l.index);
    dir.join(format!("group-{group}-{idx}.snap"))
}

/// Snapshot file: `[u32 len][postcard meta]` then `[u32 klen][key][u32 vlen][value]`*.
fn write_snapshot_file(
    path: &std::path::Path,
    meta: &SnapshotMetaOf<TypeConfig>,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut w = BufWriter::new(std::fs::File::create(&tmp)?);
        let m = enc(meta)?;
        w.write_all(&(m.len() as u32).to_le_bytes())?;
        w.write_all(&m)?;
        for (k, v) in pairs {
            w.write_all(&(k.len() as u32).to_le_bytes())?;
            w.write_all(k)?;
            w.write_all(&(v.len() as u32).to_le_bytes())?;
            w.write_all(v)?;
        }
        w.into_inner().map_err(io_err)?.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[allow(clippy::type_complexity)]
fn read_snapshot(r: &mut impl Read) -> io::Result<(SnapshotMetaOf<TypeConfig>, Vec<(Vec<u8>, Vec<u8>)>)> {
    fn chunk(r: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
        let mut len = [0u8; 4];
        match r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
        r.read_exact(&mut buf)?;
        Ok(Some(buf))
    }
    let meta = dec(&chunk(r)?.ok_or_else(|| io_err("empty snapshot"))?)?;
    let mut pairs = Vec::new();
    while let Some(k) = chunk(r)? {
        let v = chunk(r)?.ok_or_else(|| io_err("truncated snapshot"))?;
        pairs.push((k, v));
    }
    Ok((meta, pairs))
}

fn set_current_snapshot(db: &Database, group: u32, path: &std::path::Path) -> io::Result<()> {
    let txn = db.begin_write().map_err(io_err)?;
    {
        let mut m = txn.open_table(SM_META).map_err(io_err)?;
        m.insert((group, "snapshot"), enc(&path.to_string_lossy().to_string())?.as_slice()).map_err(io_err)?;
    }
    txn.commit().map_err(io_err)
}

pub struct SnapshotBuilder {
    group: u32,
    db: Arc<Database>,
    dir: PathBuf,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = std::fs::File;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, std::fs::File>, io::Error> {
        let (db, group, dir) = (self.db.clone(), self.group, self.dir.clone());
        tokio::task::spawn_blocking(move || {
            // Everything this snapshot covers becomes durable first, so logs
            // purged after it are never needed for a replay.
            StateMachine::checkpoint(&db)?;
            let txn = db.begin_read().map_err(io_err)?;
            let meta_t = txn.open_table(SM_META).map_err(io_err)?;
            let get = |k: &str| -> io::Result<Option<Vec<u8>>> {
                Ok(meta_t.get((group, k)).map_err(io_err)?.map(|v| v.value().to_vec()))
            };
            let last_log_id: Option<LogIdOf<TypeConfig>> = get("applied")?.map(|b| dec(&b)).transpose()?.flatten();
            let last_membership: StoredMembershipOf<TypeConfig> =
                get("membership")?.map(|b| dec(&b)).transpose()?.unwrap_or_default();
            let meta = SnapshotMetaOf::<TypeConfig> { last_log_id, last_membership };
            let t = txn.open_table(SM).map_err(io_err)?;
            let pairs = scan(&t, group, &[], usize::MAX);
            let path = snapshot_path(&dir, group, &meta);
            write_snapshot_file(&path, &meta, &pairs)?;
            set_current_snapshot(&db, group, &path)?;
            Ok(openraft::storage::Snapshot { meta, snapshot: std::fs::File::open(&path)? })
        })
        .await
        .map_err(io_err)?
    }
}
