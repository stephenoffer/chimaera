//! Output of calls that ran here, for runs whose driver is on another peer:
//! batched per home peer and pushed there (mesh `PushLogs`) on each
//! housekeeping tick. The home peer keeps it in the run's log and passes it
//! to the run's drivers, so `print()` on any peer reaches the driver and
//! `chimaera logs` on the home peer has the whole run. Best effort: a push
//! that fails is dropped (the executing peer keeps its own copy).

use std::collections::HashMap;
use std::sync::Arc;

use chimaera_core::ids::Id;
use chimaera_core::pb;

use crate::mesh::MeshClient;

/// A batch this large is sent without waiting for the next tick.
const FLUSH_BYTES: usize = 64 << 10;

#[derive(Default)]
pub struct LogShip {
    out: HashMap<u64, HashMap<Vec<u8>, Vec<pb::LogEvent>>>,
    bytes: usize,
    /// Each run's home node (None: unknown here, or this peer).
    homes: HashMap<Id, Option<u64>>,
}

impl LogShip {
    /// The run's home node, computed once per run by `lookup`.
    pub fn home(&mut self, run: Id, lookup: impl FnOnce() -> Option<u64>) -> Option<u64> {
        *self.homes.entry(run).or_insert_with(lookup)
    }

    /// The run's home node as the work's sender knew it.
    pub fn set_home(&mut self, run: Id, node: Option<u64>) {
        self.homes.insert(run, node);
    }

    pub fn forget_run(&mut self, run: &Id) {
        self.homes.remove(run);
    }

    /// Queue `ev` for `node`; true when the batch is big enough to send now.
    pub fn add(&mut self, node: u64, run: &[u8], ev: pb::LogEvent) -> bool {
        self.bytes += ev.text.len();
        self.out.entry(node).or_default().entry(run.to_vec()).or_default().push(ev);
        self.bytes >= FLUSH_BYTES
    }

    /// Send everything queued, one request per (node, run).
    pub fn flush(&mut self, mesh: &Arc<MeshClient>) {
        if self.out.is_empty() {
            return;
        }
        self.bytes = 0;
        for (node, runs) in std::mem::take(&mut self.out) {
            for (run, lines) in runs {
                let mesh = mesh.clone();
                tokio::spawn(async move {
                    let req = pb::PushLogsRequest { run, lines, ..Default::default() };
                    if let Err(e) = mesh.push_logs(node, req).await {
                        tracing::debug!("output for node {node} dropped: {e}");
                    }
                });
            }
        }
    }
}
