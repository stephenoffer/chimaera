//! This cell's view of the other cells: which there are, and how much room
//! each has (docs/federation.md).
//!
//! One peer of each cell, its summary publisher, talks to the other cells for
//! it. It holds a lease in the cell's own store. Every `SUMMARY_EVERY` it asks
//! `cell::NEIGHBORS` other cells for their capacity (`Status` asked with
//! `fleet`: each answers with its own and the reports it heard), folds the
//! replies into the cell's view (`cell::merge`), then renews the lease and
//! writes the view to the store, fenced by it. The cell's other peers read the
//! view from a replica of the store, their own or another peer's, which takes
//! no log entry. If the publisher stops, the view stops changing, and another
//! peer takes the lease with a higher fencing token: a write by the old one is
//! refused.
//!
//! The other cells are a static list (`CHIMAERA_FLEET`), the cells listed in
//! a directory cell (`CHIMAERA_FLEET_DIRECTORY`), or both. While it publishes,
//! the publisher keeps a task that renews its cell's entry there
//! (`CHIMAERA_CELL=name@endpoint`) with a short TTL and reads the directory's
//! changes since the revision the view holds, listing it whole only when the
//! view holds none or the directory's change log has moved past it. Each
//! round takes the list as that task last read it, so a slow directory delays
//! discovery and never a round. If the directory is lost, the cell keeps the
//! last list it read: discovery stops, delegation does not.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chimaera_core::cell;
use chimaera_core::pb;
use chimaera_core::shard::store;
use pb::store_read::Kind;
use prost::Message;

use super::{Auth, CELL_ENV, CPU, Client, Fleet};
use crate::router::Router;

const DIRECTORY_TENANT: &str = "default";
const DIRECTORY_NAMESPACE: &str = "fleet/cells";
/// The publisher renews its cell's directory entry and reads the directory's
/// changes this often, or as soon as the last pass ends while the directory
/// takes longer than this to answer.
const DIRECTORY_EVERY: Duration = Duration::from_secs(2);
/// A cell's directory entry outlives a few missed renewals, not a dead cell. A
/// listed cell the directory, when read, has not shown renewed for this long
/// leaves the view.
const DIRECTORY_TTL: Duration = Duration::from_secs(10);
/// A directory request waits this long at most (half an entry's TTL): a slow
/// directory is still read, and one that stopped answering is asked again
/// before this cell's entry there runs out.
const DIRECTORY_TIMEOUT: Duration = Duration::from_secs(DIRECTORY_TTL.as_secs() / 2);
/// A round: the publisher asks its neighbors and writes the view, and the
/// other peers read it. What is sent to a cell counts against its room until
/// its next report, so this bounds how long freed room there goes unused.
const SUMMARY_EVERY: Duration = Duration::from_millis(500);
/// The publisher's lease, renewed every round just before the view's write. A
/// peer that has not seen the view change for this long, plus up to as long
/// again (by peer, so they do not all try at once), takes the lease once it
/// has expired.
const LEASE_TTL_MS: u64 = 3_000;
/// A request for a neighbor's summary waits this long at most, so a round's
/// requests end well inside the lease whatever the other cells do.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
/// The cell's view and its publisher's lease, in its own store, under a
/// tenant no tenant's credential can name (only the cluster token reaches it).
const TENANT: &str = "";
const NAMESPACE: &str = "fleet";
const LEASE_KEY: &str = "publisher";
const VIEW_KEY: &str = "view";

/// Keep `fleet`'s view for as long as the peer runs: as its cell's
/// publisher, or by reading what the publisher wrote.
pub(super) fn spawn(fleet: &Fleet, router: Arc<Router>) {
    let mut p = Publisher {
        router,
        fixed: fleet.fixed.clone(),
        directory: fleet.directory.clone(),
        me: fleet.me.clone(),
        auth: fleet.auth.clone(),
        view: fleet.view.clone(),
        token: None,
        changed: (0, None),
        listed: None,
        clients: HashMap::new(),
    };
    tokio::spawn(async move {
        loop {
            let started = Instant::now();
            p.round().await;
            tokio::time::sleep(SUMMARY_EVERY.saturating_sub(started.elapsed())).await;
        }
    });
}

/// One peer's part in keeping its cell's view of the fleet.
struct Publisher {
    router: Arc<Router>,
    fixed: Vec<(String, String)>,
    directory: Option<String>,
    /// This cell's name and the endpoint it registers in the directory at.
    me: (String, String),
    auth: Auth,
    view: Arc<Mutex<pb::FleetView>>,
    /// The lease's fencing token, while this peer publishes.
    token: Option<u64>,
    /// The stored view's version, and when this peer saw it change.
    changed: (u64, Option<Instant>),
    /// While this peer publishes with a directory: the directory's list as
    /// `watch_directory` last read it, and that task.
    listed: Option<(Arc<Mutex<Listed>>, tokio::task::AbortHandle)>,
    /// Connections to other cells, by endpoint.
    clients: HashMap<String, Client>,
}

impl Publisher {
    async fn round(&mut self) {
        if self.token.is_none() {
            self.follow().await;
        }
        if let Some(token) = self.token
            && let Err(e) = self.publish(token).await
        {
            tracing::info!("this peer no longer publishes its cell's fleet view: {e}");
            self.token = None;
            self.view.lock().expect("fleet view").publishing = false;
            if let Some((_, task)) = self.listed.take() {
                task.abort();
            }
        }
    }

    /// Read the view the publisher wrote, and take the lease once the view
    /// has stopped changing (or there is none yet).
    async fn follow(&mut self) {
        // A replica's copy, this peer's or another's: it may lag a little.
        let (v, now) = (store::vshard_of(TENANT, NAMESPACE), self.router.hlc());
        let read = match self.router.read(v, |kv| store::read(kv, v, &view_read(), now)) {
            Some(r) => Ok(r),
            None => self.replica_read().await,
        };
        if let Ok(r) = read {
            self.saw(r);
        }
        let patience = Duration::from_millis(LEASE_TTL_MS + self.router.node_id() % LEASE_TTL_MS);
        if self.changed.1.is_some_and(|t| t.elapsed() < patience) {
            return;
        }
        let Ok(token) = self.lock().await else {
            // Another peer holds it: give it a lease's time to write.
            self.changed.1 = Some(Instant::now());
            return;
        };
        // Carry on from the view as the last publisher wrote it, read through
        // the log: a replica's copy may lag it.
        match self.store(pb::shard_cmd::Body::StoreRead(view_read())).await {
            Ok(r) => self.saw(r),
            Err(e) => {
                tracing::debug!("reading this cell's fleet view: {e}");
                return;
            }
        }
        tracing::info!(token, "this peer now publishes its cell's fleet view");
        if let Some(dir) = &self.directory {
            // The stored view's cells count as just renewed.
            let (view, now) = (self.view.lock().expect("fleet view").clone(), Instant::now());
            let cells = view
                .cells
                .into_iter()
                .filter(|c| !self.fixed.iter().any(|(n, _)| *n == c.name))
                .map(|c| (c.name, (c.endpoint, now)))
                .collect();
            let listing = Listing { cells, revision: view.directory_revision, scan: None };
            let out = Arc::new(Mutex::new(listing.listed()));
            let task = watch_directory(dir.clone(), self.me.clone(), self.auth.clone(), listing, out.clone());
            self.listed = Some((out, tokio::spawn(task).abort_handle()));
        }
        self.token = Some(token);
    }

    /// Take the stored view `r` holds, if it changed since this peer last saw it.
    fn saw(&mut self, r: pb::StoreReply) {
        if let Some(e) = r.entry
            && e.version != self.changed.0
        {
            self.changed = (e.version, Some(Instant::now()));
            if let Ok(view) = pb::FleetView::decode(e.value.as_slice()) {
                *self.view.lock().expect("fleet view") = view;
            }
        }
    }

    /// The stored view as a replica on another peer of the cell has it.
    async fn replica_read(&self) -> Result<pb::StoreReply, String> {
        let mesh = self.router.mesh.as_ref().ok_or("no replica of the cell's store")?;
        let group = self.router.group_of(store::vshard_of(TENANT, NAMESPACE));
        let req = pb::ReadRequest { store: Some(view_read()), ..Default::default() };
        let r = mesh.read_group(group, req).await.map_err(|e| e.to_string())?;
        r.store.ok_or(r.error)
    }

    /// One round as the publisher: take the directory's list as
    /// `watch_directory` last read it, ask the neighbors, then renew the lease
    /// and write the view, fenced by it.
    async fn publish(&mut self, held: u64) -> Result<(), String> {
        let mut view = self.view.lock().expect("fleet view").clone();
        // The cell's id, which its reports carry, outlives its publishers: the first one's node id.
        if view.cell == 0 {
            view.cell = self.router.node_id();
        }
        let listed = match &self.listed {
            Some((listed, _)) => {
                let (cells, revision) = listed.lock().expect("fleet directory").clone();
                view.directory_revision = revision;
                cells
            }
            None => Vec::new(),
        };
        view.cells = merged(&self.fixed, listed, &self.me.0)
            .into_iter()
            .map(|(name, endpoint)| pb::FleetCell { name, endpoint })
            .collect();
        self.exchange(&mut view).await;
        // Renewed after the requests above, however long they took: it is
        // held for the write it fences.
        let token = self.lock().await?;
        if token != held {
            tracing::info!(token, "this cell's fleet view lease lapsed and was taken again");
        }
        self.token = Some(token);
        (view.epoch, view.publishing) = (token, false);
        let put = pb::StoreWrite {
            tenant: TENANT.into(),
            namespace: NAMESPACE.into(),
            key: VIEW_KEY.into(),
            value: view.encode_to_vec(),
            fence: Some(pb::StoreFence { lock: LEASE_KEY.into(), token }),
            ..Default::default()
        };
        let r = self.store(pb::shard_cmd::Body::StoreWrite(put)).await?;
        if !r.ok {
            return Err(r.error);
        }
        self.changed = (r.revision, Some(Instant::now()));
        *self.view.lock().expect("fleet view") = pb::FleetView { publishing: true, ..view };
        Ok(())
    }

    /// Ask this cell's neighbors (`asked`) for their capacity, and fold their
    /// replies into `view`.
    async fn exchange(&mut self, view: &mut pb::FleetView) {
        let asked = asked(&view.cells, &self.me.0);
        let clients: Vec<Result<Client, String>> = asked.iter().map(|c| self.client(&c.endpoint)).collect();
        let replies = futures_util::future::join_all(clients.into_iter().map(|client| async move {
            let mut client = client?;
            let req = pb::StatusRequest { fleet: true, ..Default::default() };
            match tokio::time::timeout(REQUEST_TIMEOUT, client.status(req)).await {
                Ok(r) => r.map(|r| r.into_inner()).map_err(|e| e.message().to_string()),
                Err(_) => Err(format!("no answer within {REQUEST_TIMEOUT:?}")),
            }
        }))
        .await;
        let mut all = Vec::new();
        for (c, reply) in asked.iter().zip(replies) {
            match reply {
                Ok(r) => all.extend(heard(r, &c.name, view.cell)),
                Err(e) => tracing::debug!("fleet summary from cell {}: {e}", c.name),
            }
        }
        cell::merge(&mut view.capacity, all, &self.me.0);
        let keep: HashSet<&str> = asked.iter().map(|c| c.endpoint.as_str()).collect();
        self.clients.retain(|a, _| keep.contains(a.as_str()));
    }

    /// Take or keep the lease: its fencing token.
    async fn lock(&self) -> Result<u64, String> {
        let w = pb::StoreWrite {
            tenant: TENANT.into(),
            namespace: NAMESPACE.into(),
            op: pb::store_write::Op::Lock as i32,
            key: LEASE_KEY.into(),
            holder: format!("{:016x}", self.router.node_id()),
            ttl_ms: LEASE_TTL_MS,
            ..Default::default()
        };
        let r = self.store(pb::shard_cmd::Body::StoreWrite(w)).await?;
        match r.lease {
            Some(l) if r.ok => Ok(l.token),
            _ => Err(r.error),
        }
    }

    /// A command on the cell's own store.
    async fn store(&self, body: pb::shard_cmd::Body) -> Result<pb::StoreReply, String> {
        match self.router.propose(store::vshard_of(TENANT, NAMESPACE), body).await {
            Ok(pb::ShardReply { body: Some(pb::shard_reply::Body::Store(r)) }) => Ok(r),
            Ok(other) => Err(format!("store command rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    /// The connection to a neighbor's endpoint, kept while it is one.
    fn client(&mut self, addr: &str) -> Result<Client, String> {
        if let Some(c) = self.clients.get(addr) {
            return Ok(c.clone());
        }
        let c = connect(addr, REQUEST_TIMEOUT, &self.auth)?;
        self.clients.insert(addr.to_string(), c.clone());
        Ok(c)
    }
}

/// A connection to another cell's endpoint (made on first use, and remade by
/// the channel when it drops) whose requests wait `timeout` at most.
fn connect(addr: &str, timeout: Duration, auth: &Auth) -> Result<Client, String> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .tcp_nodelay(true)
        .connect_lazy();
    Ok(pb::peer_client::PeerClient::with_interceptor(channel, auth.clone()))
}

/// A read of the cell's stored view.
fn view_read() -> pb::StoreRead {
    let (tenant, namespace, key) = (TENANT.into(), NAMESPACE.into(), VIEW_KEY.into());
    pb::StoreRead { tenant, namespace, kind: Kind::Get as i32, key, ..Default::default() }
}

/// The static cells, then the directory's, without this cell or duplicates.
fn merged(fixed: &[(String, String)], listed: Vec<(String, String)>, me: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = fixed.to_vec();
    for (name, endpoint) in listed {
        if name != me && !out.iter().any(|(n, _)| *n == name) {
            out.push((name, endpoint));
        }
    }
    out
}

/// The cells that cell `me` asks each round: `cell::NEIGHBORS` of `cells`,
/// placed by name among them and itself. Cells that list the same cells by
/// the same names place them alike, so each is asked by as many as it asks.
fn asked(cells: &[pb::FleetCell], me: &str) -> Vec<pb::FleetCell> {
    let mut names: Vec<&str> = cells.iter().map(|c| c.name.as_str()).chain([me]).collect();
    names.sort_unstable();
    names.dedup();
    let at = names.iter().position(|n| *n == me).expect("added above");
    cell::neighbors(names.len(), at, cell::NEIGHBORS)
        .into_iter()
        .filter_map(|i| cells.iter().find(|c| c.name == names[i]).cloned())
        .collect()
}

/// What the publisher has read of the directory.
#[derive(Default)]
struct Listing {
    cells: Cells,
    /// The directory revision `cells` is up to date with (0: none).
    revision: u64,
    /// A whole listing under way: the cells it has read, the last key read,
    /// and its first page's revision.
    scan: Option<(Cells, String, u64)>,
}

/// Each listed cell's endpoint, and when this peer last saw its entry renewed.
type Cells = HashMap<String, (String, Instant)>;

/// The cells a directory lists, by name and endpoint, and the revision that
/// list is up to date with.
type Listed = (Vec<(String, String)>, u64);

impl Listing {
    fn listed(&self) -> Listed {
        (self.cells.iter().map(|(n, (e, _))| (n.clone(), e.clone())).collect(), self.revision)
    }
}

/// For as long as this peer publishes: renew cell `me`'s entry in directory
/// `dir` and bring `listing` up to date (`sync_directory`) every
/// `DIRECTORY_EVERY`, or back to back while the directory takes longer than
/// that, handing each pass's list to the rounds through `out`.
async fn watch_directory(dir: String, me: (String, String), auth: Auth, mut listing: Listing, out: Arc<Mutex<Listed>>) {
    let client = match connect(&dir, DIRECTORY_TIMEOUT, &auth) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("fleet directory {dir}: {e}; this cell keeps the cells its view lists");
            return;
        }
    };
    let mut failing = false;
    loop {
        let started = Instant::now();
        let ask = |req: pb::StoreRequest| {
            let mut c = client.clone();
            async move {
                match tokio::time::timeout(DIRECTORY_TIMEOUT, c.store(req)).await {
                    Ok(r) => r.map(|r| r.into_inner()).map_err(|e| e.message().to_string()),
                    Err(_) => Err(format!("no answer within {DIRECTORY_TIMEOUT:?}")),
                }
            }
        };
        let result = sync_directory(ask, &me, &mut listing).await;
        *out.lock().expect("fleet directory") = listing.listed();
        match (&result, failing) {
            (Err(e), false) => {
                tracing::warn!("fleet directory {dir}: {e}; this cell keeps the cells it last read until it answers")
            }
            (Err(e), true) => tracing::debug!("fleet directory {dir}: {e}"),
            (Ok(()), true) => tracing::info!("fleet directory {dir} answers again"),
            (Ok(()), false) => {}
        }
        failing = result.is_err();
        tokio::time::sleep(DIRECTORY_EVERY.saturating_sub(started.elapsed())).await;
    }
}

/// Renew cell `me`'s directory entry (when it has an endpoint), alongside the
/// first read, and bring `l` up to date: with the directory's changes since
/// its revision, or with a whole listing when there is none, the directory's
/// change log has moved past it, or the directory went back. What a pass read
/// before a request failed stays read, and the next pass carries on from
/// there. `ask` sends the directory a request.
async fn sync_directory<F: Future<Output = Result<pb::StoreReply, String>>>(
    mut ask: impl FnMut(pb::StoreRequest) -> F,
    me: &(String, String),
    l: &mut Listing,
) -> Result<(), String> {
    let (name, endpoint) = me;
    let put = (!endpoint.is_empty()).then(|| {
        let put = pb::StoreWrite {
            tenant: DIRECTORY_TENANT.into(),
            namespace: DIRECTORY_NAMESPACE.into(),
            key: name.clone(),
            value: endpoint.clone().into_bytes(),
            ttl_ms: DIRECTORY_TTL.as_millis() as u64,
            ..Default::default()
        };
        ask(pb::StoreRequest { write: Some(put), ..Default::default() })
    });
    let register = async {
        match put {
            Some(put) => match put.await? {
                r if r.ok => Ok(()),
                r => Err(format!("registering {name}: {}", r.error)),
            },
            None => Ok(()),
        }
    };
    let (registered, read) = futures_util::future::join(register, read_directory(ask, l)).await;
    registered.and(read)
}

/// `sync_directory`'s reads.
async fn read_directory<F: Future<Output = Result<pb::StoreReply, String>>>(
    mut ask: impl FnMut(pb::StoreRequest) -> F,
    l: &mut Listing,
) -> Result<(), String> {
    let read = |kind: Kind| pb::StoreRead {
        tenant: DIRECTORY_TENANT.into(),
        namespace: DIRECTORY_NAMESPACE.into(),
        kind: kind as i32,
        ..Default::default()
    };
    let request = |read: pb::StoreRead| pb::StoreRequest { read: Some(read), ..Default::default() };
    while l.revision > 0 && l.scan.is_none() {
        let r = ask(request(pb::StoreRead { after_revision: l.revision, ..read(Kind::Changes) })).await?;
        if !r.ok {
            return Err(format!("reading the cells' changes: {}", r.error));
        }
        if r.compacted || r.revision < l.revision {
            break;
        }
        let now = Instant::now();
        for ch in &r.changes {
            match String::from_utf8(ch.value.clone()) {
                Ok(endpoint) if !ch.deleted => l.cells.insert(ch.key.clone(), (endpoint, now)),
                _ => l.cells.remove(&ch.key),
            };
        }
        if !r.more {
            l.revision = r.revision;
            l.cells.retain(|_, (_, seen)| seen.elapsed() < DIRECTORY_TTL);
            return Ok(());
        }
        l.revision = r.changes.last().map_or(r.revision, |ch| ch.revision);
    }
    // The whole listing, a page at a time; changes after its first page's
    // revision follow it next time.
    let (cells, start_after, first) = l.scan.get_or_insert_default();
    loop {
        let r = ask(request(pb::StoreRead { start_after: start_after.clone(), ..read(Kind::Scan) })).await?;
        if !r.ok {
            return Err(format!("listing cells: {}", r.error));
        }
        if *first == 0 {
            *first = r.revision;
        }
        let now = Instant::now();
        for e in r.entries {
            *start_after = e.key.clone();
            if let Ok(endpoint) = String::from_utf8(e.value) {
                cells.insert(e.key, (endpoint, now));
            }
        }
        if !r.more {
            break;
        }
    }
    let (cells, _, first) = l.scan.take().expect("listed above");
    crate::counters::FLEET_DIRECTORY_LISTINGS.add(1);
    (l.cells, l.revision) = (cells, first);
    Ok(())
}

/// The reply to a `Status` asked with `fleet`: this cell's own capacity, then
/// the reports its view holds of the others (none without a fleet). A cell in
/// a fleet leaves its own out until this peer has read the cell's id, which
/// lets the cell know its report when it comes back.
pub fn answer(s: pb::StatusReply) -> pb::StatusReply {
    let name = super::this_cell(std::env::var(CELL_ENV).ok().as_deref()).map(|(n, _)| n);
    let view = s.fleet.as_ref();
    let own = view.is_none_or(|v| v.cell != 0).then(|| pb::CellCapacity {
        origin: view.map_or(0, |v| v.cell),
        ..own_report(&s.nodes, name.unwrap_or_default())
    });
    let capacity = own.into_iter().chain(s.fleet.map(|f| f.capacity).unwrap_or_default()).collect();
    pb::StatusReply {
        peer: s.peer,
        fleet: Some(pb::FleetView { capacity, ..Default::default() }),
        ..Default::default()
    }
}

/// What a neighbor's `Status` reply tells cell `me` (its id): the neighbor's
/// own reports, under the `name` this cell knows it by, then the reports it
/// heard, less this cell's own, which come back under the names other cells
/// know it by. A peer that predates `fleet` sent its whole status.
fn heard(r: pb::StatusReply, name: &str, me: u64) -> Vec<pb::CellCapacity> {
    let reports = r.fleet.map(|f| f.capacity).unwrap_or_else(|| vec![own_report(&r.nodes, String::new())]);
    reports
        .into_iter()
        .filter(|c| c.origin == 0 || c.origin != me)
        .map(|mut c| {
            if c.age == 0 {
                c.cell = name.into();
            }
            c
        })
        .collect()
}

/// A cell's CPUs, free (less the calls waiting in its queues) and in all,
/// over the live peers in `nodes`, in the region of the peer that answered.
/// The peer delegates only CPU calls, so CPUs are the one class it reports.
fn own_report(nodes: &[pb::ClusterNode], cell: String) -> pb::CellCapacity {
    let alive = nodes.iter().filter(|n| n.alive);
    let region = nodes.iter().find(|n| n.this_peer).and_then(|n| n.labels.get("region"));
    pb::CellCapacity {
        cell,
        region: region.cloned().unwrap_or_default(),
        resource: CPU.into(),
        free: alive.clone().map(|n| (n.cpus - n.cpu_used - n.queued as f64).max(0.0)).sum(),
        total: alive.map(|n| n.cpus).sum(),
        number: SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chimaera_core::kv::MemKv;
    use chimaera_core::shard::ShardState;

    fn report(cell: &str, region: &str, free: f64, number: u64) -> pb::CellCapacity {
        let (cell, region, resource) = (cell.into(), region.into(), CPU.into());
        pb::CellCapacity { cell, region, resource, free, total: 4.0, number, ..Default::default() }
    }

    #[test]
    fn a_fleet_status_answers_with_this_cells_room_then_its_view() {
        let node = |alive, cpus, cpu_used, queued, this_peer| pb::ClusterNode {
            alive,
            cpus,
            cpu_used,
            queued,
            this_peer,
            labels: [("region".to_string(), "eu".to_string())].into(),
            ..Default::default()
        };
        let s = pb::StatusReply {
            nodes: vec![node(true, 4.0, 1.0, 1, true), node(true, 4.0, 4.0, 3, false), node(false, 8.0, 0.0, 0, false)],
            fleet: Some(pb::FleetView {
                capacity: vec![report("c", "us", 3.0, 7)],
                cells: vec![pb::FleetCell { name: "c".into(), endpoint: "h:2".into() }],
                cell: 42,
                ..Default::default()
            }),
            runs: vec![pb::RunRecord::default()],
            ..Default::default()
        };
        let r = answer(s.clone());
        assert!(r.runs.is_empty() && r.nodes.is_empty(), "only the fleet view: a bounded reply");
        let view = r.fleet.unwrap();
        assert!(view.cells.is_empty());
        let own = &view.capacity[0];
        // Free CPUs less what waits, over the live peers: (4 - 1 - 1) + 0.
        assert_eq!((own.free, own.total, own.region.as_str(), own.age, own.origin), (2.0, 8.0, "eu", 0, 42));
        assert_eq!(view.capacity[1].cell, "c");
        // Before this peer has read its cell's id, the cell's own report stays home.
        let mut unread = s.clone();
        unread.fleet.as_mut().unwrap().cell = 0;
        assert_eq!(answer(unread).fleet.unwrap().capacity.iter().map(|c| c.cell.as_str()).collect::<Vec<_>>(), ["c"]);
        // A cell outside any fleet reports itself with no id: it never hears reports back.
        let outside = answer(pb::StatusReply { fleet: None, ..s }).fleet.unwrap().capacity;
        assert_eq!((outside.len(), outside[0].origin, outside[0].free), (1, 0, 2.0));
    }

    #[test]
    fn a_cell_takes_its_neighbors_reports_but_not_its_own_back() {
        let mine = |cell: &str, age| pb::CellCapacity { origin: 42, age, ..report(cell, "eu", 1.0, 3) };
        let reply = pb::StatusReply {
            fleet: Some(pb::FleetView {
                capacity: vec![
                    pb::CellCapacity { origin: 7, ..report("", "us", 3.0, 9) }, // b's own
                    pb::CellCapacity { age: 1, ..report("d", "us", 2.0, 5) },   // from outside any fleet
                    mine("a", 1),                                               // this cell, as b knows it
                    mine("alpha", 2), // this cell, as some other cell knows it
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let got: Vec<(String, u32)> = heard(reply, "b", 42).into_iter().map(|c| (c.cell, c.age)).collect();
        assert_eq!(got, [("b".into(), 0), ("d".into(), 1)]);
        // A peer that predates `fleet` answers with its whole status: its own report is made here.
        let old = pb::StatusReply {
            nodes: vec![pb::ClusterNode { alive: true, cpus: 4.0, cpu_used: 1.0, ..Default::default() }],
            ..Default::default()
        };
        let got = heard(old, "b", 42);
        assert_eq!((got.len(), got[0].cell.as_str(), got[0].free, got[0].origin), (1, "b", 3.0, 0));
    }

    #[test]
    fn the_directory_adds_cells_but_never_this_one_or_a_listed_one_twice() {
        let fixed = vec![("b".to_string(), "h:1".to_string())];
        let listed = vec![("a".into(), "h:0".into()), ("b".into(), "h:9".into()), ("c".into(), "h:2".into())];
        assert_eq!(merged(&fixed, listed, "a"), [("b".into(), "h:1".into()), ("c".into(), "h:2".into())]);
    }

    #[test]
    fn every_cell_of_a_fleet_is_asked_by_as_many_as_it_asks() {
        for n in [2, 5, 196, 512] {
            let names: Vec<String> = (0..n).map(|i| format!("cell-{:x}", i * 7919 % 1000)).collect();
            let mut asked_by: HashMap<String, usize> = HashMap::new();
            for me in &names {
                // Each lists every other cell (by the names they all use) but not itself.
                let cells: Vec<pb::FleetCell> = names
                    .iter()
                    .filter(|c| *c != me)
                    .map(|c| pb::FleetCell { name: c.clone(), endpoint: format!("{c}:1") })
                    .collect();
                let ask = asked(&cells, me);
                assert_eq!(ask.len(), cell::NEIGHBORS.min(n - 1));
                ask.into_iter().for_each(|c| *asked_by.entry(c.name).or_default() += 1);
            }
            assert_eq!(asked_by.len(), n, "{n} cells: every cell is asked");
            assert!(asked_by.values().all(|a| *a == cell::NEIGHBORS.min(n - 1)), "{n} cells: {asked_by:?}");
        }
    }

    /// A directory cell's namespace, applied by the shard's own state machine.
    struct Directory {
        kv: MemKv,
        hlc: u64,
        listings: usize,
    }

    impl Directory {
        fn new() -> Directory {
            let mut d = Directory { kv: MemKv::default(), hlc: 0, listings: 0 };
            let v = store::vshard_of("default", DIRECTORY_NAMESPACE) as u32;
            d.apply(pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: vec![v], epoch: 1 }));
            d
        }

        fn apply(&mut self, body: pb::shard_cmd::Body) -> pb::ShardReply {
            self.hlc += 1;
            let vshard = store::vshard_of("default", DIRECTORY_NAMESPACE) as u32;
            let cmd = pb::ShardCmd { vshard, vshard_epoch: 1, hlc: self.hlc, body: Some(body), ..Default::default() };
            ShardState::default().apply(&mut self.kv, &cmd).reply
        }

        fn ask(&mut self, req: pb::StoreRequest) -> Result<pb::StoreReply, String> {
            let body = match (req.write, req.read) {
                (Some(w), _) => pb::shard_cmd::Body::StoreWrite(w),
                (None, Some(r)) => {
                    self.listings += usize::from(r.kind() == Kind::Scan && r.start_after.is_empty());
                    pb::shard_cmd::Body::StoreRead(r)
                }
                (None, None) => return Err("an empty request".into()),
            };
            match self.apply(body).body {
                Some(pb::shard_reply::Body::Store(r)) => Ok(r),
                other => Err(format!("{other:?}")),
            }
        }

        /// Register `cell` at `endpoint`, or with none, delete its entry.
        fn set(&mut self, cell: &str, endpoint: Option<&str>) {
            let op = if endpoint.is_some() { pb::store_write::Op::Put } else { pb::store_write::Op::Delete };
            let w = pb::StoreWrite {
                tenant: "default".into(),
                namespace: DIRECTORY_NAMESPACE.into(),
                op: op as i32,
                key: cell.into(),
                value: endpoint.unwrap_or_default().into(),
                ..Default::default()
            };
            assert!(self.ask(pb::StoreRequest { write: Some(w), ..Default::default() }).unwrap().ok);
        }
    }

    fn me() -> (String, String) {
        ("a".to_string(), "a:1".to_string())
    }

    /// One `sync_directory` of cell a (at `a:1`) against `d`.
    async fn sync(d: &mut Directory, l: &mut Listing) {
        sync_directory(|req| std::future::ready(d.ask(req)), &me(), l).await.unwrap();
    }

    fn names(l: &Listing) -> Vec<&str> {
        let mut n: Vec<&str> = l.cells.keys().map(String::as_str).collect();
        n.sort_unstable();
        n
    }

    #[tokio::test]
    async fn a_publisher_reads_the_directorys_changes_and_lists_it_whole_only_when_it_must() {
        let (mut d, mut l) = (Directory::new(), Listing::default());
        // Over a page of cells: the first read lists them all, a page at a time.
        for i in 0..1001 {
            d.set(&format!("c{i:04}"), Some("h:1"));
        }
        sync(&mut d, &mut l).await;
        assert_eq!((l.cells.len(), d.listings), (1002, 1), "1001 cells and a itself, in one listing");
        assert!(l.revision > 0);

        // Then only what changed: a cell joins, one leaves, one moves.
        d.set("c0000", None);
        d.set("c0001", Some("h:2"));
        d.set("new", Some("h:3"));
        sync(&mut d, &mut l).await;
        assert_eq!((l.cells.len(), d.listings), (1002, 1), "no second listing");
        assert!(!l.cells.contains_key("c0000") && l.cells["c0001"].0 == "h:2" && l.cells["new"].0 == "h:3");

        // A cell the directory has not shown renewed for its TTL leaves.
        let long_ago = Instant::now().checked_sub(DIRECTORY_TTL + Duration::from_secs(1)).unwrap();
        l.cells.get_mut("c0002").unwrap().1 = long_ago;
        sync(&mut d, &mut l).await;
        assert!(!l.cells.contains_key("c0002") && l.cells.contains_key("c0003"));

        // A change log trimmed past this revision: listed whole again.
        for i in 0..4100 {
            d.set("busy", Some(&format!("h:{i}")));
        }
        sync(&mut d, &mut l).await;
        assert_eq!(d.listings, 2);
        assert_eq!(l.cells["busy"].0, "h:4099");
        assert!(l.cells.contains_key("c0002"), "a whole listing has every live entry");

        // A directory that went back (rebuilt, its revisions lower): listed whole.
        let mut fresh = Directory::new();
        fresh.set("only", Some("h:9"));
        sync(&mut fresh, &mut l).await;
        assert_eq!((names(&l), fresh.listings), (vec!["a", "only"], 1));
    }

    #[tokio::test]
    async fn a_pass_cut_off_by_a_failed_request_keeps_what_it_read() {
        let (mut d, mut l) = (Directory::new(), Listing::default());
        for i in 0..2500 {
            d.set(&format!("c{i:04}"), Some("h:1"));
        }
        // The `n`th request of `kind` fails (times out); the others reach `d`.
        // Returns how many requests of `kind` were sent.
        async fn cut(d: &mut Directory, l: &mut Listing, kind: Kind, n: usize) -> (Result<(), String>, usize) {
            let mut sent = 0;
            let ask = |req: pb::StoreRequest| {
                let this = req.read.as_ref().is_some_and(|r| r.kind() == kind);
                sent += usize::from(this);
                std::future::ready(if this && sent == n { Err("no answer".to_string()) } else { d.ask(req) })
            };
            let r = sync_directory(ask, &me(), l).await;
            (r, sent)
        }
        // A whole listing of three pages whose second fails: none of it counts
        // yet, and the next pass asks for the second page, not the first again.
        assert!(cut(&mut d, &mut l, Kind::Scan, 2).await.0.is_err());
        assert_eq!((l.cells.len(), l.revision), (0, 0));
        assert_eq!(cut(&mut d, &mut l, Kind::Scan, 0).await, (Ok(()), 2));
        assert_eq!((l.cells.len(), d.listings), (2501, 1), "2500 cells and a, in one listing");

        // Changes over three pages whose second fails: the first counts, and
        // the next pass reads only the rest.
        for i in 0..2500 {
            d.set(&format!("c{i:04}"), Some("h:2"));
        }
        let before = l.revision;
        assert!(cut(&mut d, &mut l, Kind::Changes, 2).await.0.is_err());
        assert!(l.revision > before && l.cells["c0000"].0 == "h:2" && l.cells["c2499"].0 == "h:1");
        assert_eq!(cut(&mut d, &mut l, Kind::Changes, 0).await, (Ok(()), 2));
        assert!(l.cells.iter().all(|(n, (e, _))| e == if n == "a" { "a:1" } else { "h:2" }));
        assert_eq!(d.listings, 1, "never listed whole again");
    }
}
