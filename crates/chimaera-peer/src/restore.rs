//! `chimaera cluster backup` and `chimaera cluster restore`: a cell rebuilt
//! from its groups' Raft snapshots (see chimaera_raft::restore).
//!
//! A backup is taken on a stopped peer, one at a time: while it is stopped its
//! groups run with one voter fewer, so another loss then costs their quorum.
//! A restore makes a fresh peer directory hold every group of the backup, with
//! this peer as their only voter; started, it serves them, and peers that
//! join become voters as after `cluster recover`. The lost cell's peers are
//! retired at the first start: their attempts are retried, and results only
//! they stored are lost.

use crate::router::Router;
use crate::stores::{new_peer_id, node_id_of, write_atomic};
use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_raft::Backed;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// The backup's list of snapshot files, in its directory.
const MANIFEST: &str = "backup.json";
/// The peer's code store (functions and classes by digest), under its home
/// and a backup alike: not in Raft, so copied beside the snapshots, or a
/// restored call whose code only the lost peers held could not run.
const CODE: &str = "code";
/// Peer incarnations of the lost cell for the restored groups to retire: one
/// `group peer-hex incarnation` per line; removed once all are retired.
const RETIRE: &str = "restored-incarnations";

/// Back up every group the stopped peer under `dir` holds into `out`.
pub fn backup(dir: &Path, out: &Path) -> io::Result<Vec<Backed>> {
    let _lock = crate::runtime::lock_dir(dir)?;
    if !dir.join("raft").join("raft-sm.redb").exists() {
        return Err(io::Error::other(format!("{} holds no raft state to back up", dir.display())));
    }
    let backed = chimaera_raft::backup(&dir.join("raft"), out)?;
    let entries: Vec<serde_json::Value> = backed
        .iter()
        .map(|b| {
            let file = b.file.file_name().expect("a snapshot file has a name").to_string_lossy();
            serde_json::json!({ "group": b.group, "index": b.index, "file": file })
        })
        .collect();
    copy_files(&dir.join(CODE), &out.join(CODE))?;
    write_atomic(&out.join(MANIFEST), &serde_json::to_vec_pretty(&entries).map_err(io::Error::other)?)?;
    Ok(backed)
}

/// Restore the newest snapshot of each group in the backups `from` into the
/// fresh peer directory `dir`, this peer (reached at `advertise`) their only
/// voter. Nothing is written unless every file checks out.
pub fn restore(dir: &Path, from: &[PathBuf], advertise: &str) -> io::Result<chimaera_raft::Restored> {
    if dir.join("peer-id").exists() || dir.join("raft").exists() {
        return Err(io::Error::other(format!("{} holds a peer already: restore into a new directory", dir.display())));
    }
    let mut newest: BTreeMap<u32, Backed> = BTreeMap::new();
    for b in from {
        let text = std::fs::read(b.join(MANIFEST))
            .map_err(|e| io::Error::other(format!("{}: no backup here ({MANIFEST}: {e})", b.display())))?;
        let bad = |why: &str| io::Error::other(format!("{}: {why}", b.join(MANIFEST).display()));
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&text).map_err(|e| bad(&e.to_string()))?;
        for e in entries {
            let (Some(group), Some(index), Some(file)) = (e["group"].as_u64(), e["index"].as_u64(), e["file"].as_str())
            else {
                return Err(bad(&format!("an entry without group, index and file: {e}")));
            };
            let group = u32::try_from(group).map_err(|_| bad("a group number out of range"))?;
            if newest.get(&group).is_none_or(|n| n.index < index) {
                newest.insert(group, Backed { group, index, file: b.join(file) });
            }
        }
    }
    if newest.is_empty() {
        return Err(io::Error::other("the backups hold no group"));
    }
    let peer = new_peer_id()?;
    let picked: Vec<Backed> = newest.into_values().collect();
    let raft = dir.join("raft");
    let restored = chimaera_raft::restore(&raft, &picked, node_id_of(&peer.0), advertise)?;
    // The peer's identity, now its groups name it (the first start takes
    // incarnation 1).
    for b in from {
        copy_files(&b.join(CODE), &dir.join(CODE))?; // named by digest: the union of every backup's
    }
    write_atomic(&dir.join("peer-id"), peer.hex().as_bytes())?;
    let lines: String = restored.retire.iter().map(|(g, p, i)| format!("{g} {} {i}\n", hex(p))).collect();
    write_atomic(&dir.join(RETIRE), lines.as_bytes())?;
    Ok(restored)
}

/// Copy every file of `from` (if it exists) into `to`, synced.
fn copy_files(from: &Path, to: &Path) -> io::Result<()> {
    let Ok(entries) = std::fs::read_dir(from) else { return Ok(()) };
    std::fs::create_dir_all(to)?;
    for e in entries.flatten() {
        if e.file_type()?.is_file() {
            let dest = to.join(e.file_name());
            std::fs::copy(e.path(), &dest)?;
            std::fs::File::open(&dest)?.sync_all()?;
        }
    }
    std::fs::File::open(to)?.sync_all()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// After a restore: retire the lost cell's incarnations in each group (this
/// peer leads every one at first), then forget the list.
pub async fn retire_restored(router: Arc<Router>, dir: PathBuf) {
    let Ok(text) = std::fs::read_to_string(dir.join(RETIRE)) else { return };
    let mut left: Vec<(u32, pb::PeerInc)> = text
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let group = it.next()?.parse().ok()?;
            let peer = Id::parse_hex(it.next()?)?;
            let inc = it.next()?.parse().ok()?;
            Some((group, pb::PeerInc { peer: peer.0.to_vec(), inc }))
        })
        .collect();
    while !left.is_empty() {
        let mut failed = Vec::new();
        for (group, peer) in left {
            let Some(&first) = router.vshards_of(group).first() else { continue }; // a group without vshards holds no attempts
            let body = pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(peer.clone()) });
            if let Err(e) = router.propose(first as u16, body).await {
                tracing::warn!("restored group {group}: cannot retire a lost peer yet ({e})");
                failed.push((group, peer));
            }
        }
        left = failed;
        if !left.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    let _ = std::fs::remove_file(dir.join(RETIRE));
}
