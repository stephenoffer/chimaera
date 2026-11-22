//! Federation (docs/federation.md): when this cell is full, run a granted
//! attempt in another cell, as an ordinary authenticated client of it.
//!
//! The owning shard never learns the call left the cell. This peer publishes
//! the other cell's outcome as its attempt's outcome, or reports the attempt
//! failed, and the shard retries it like any lost attempt. Only calls whose
//! whole meaning travels with them are eligible (`eligible`). Each cell's free
//! CPUs come from polling its `Status` every `SUMMARY_EVERY`.
//!
//! The other cells are a static list (`CHIMAERA_FLEET`), the cells listed in
//! a directory cell (`CHIMAERA_FLEET_DIRECTORY`), or both. In the directory,
//! each cell (`CHIMAERA_CELL=name@endpoint`) keeps an entry with a short TTL,
//! in a store namespace replicated by the directory cell's Raft groups. If
//! the directory is lost, peers keep the last list they read: discovery
//! stops, delegation does not.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chimaera_core::ids::Id;
use chimaera_core::pb;
use tokio::sync::{mpsc, oneshot};

use crate::executor::ExecEnv;

pub const ENV: &str = "CHIMAERA_FLEET";
pub const TOKEN_ENV: &str = "CHIMAERA_FLEET_TOKEN_FILE";
pub const DIRECTORY_ENV: &str = "CHIMAERA_FLEET_DIRECTORY";
pub const CELL_ENV: &str = "CHIMAERA_CELL";
const DIRECTORY_NAMESPACE: &str = "fleet/cells";
/// Every this many summary rounds, the directory is read and this cell's entry renewed.
const DIRECTORY_EVERY: u32 = 4;
/// A cell's directory entry outlives a few missed renewals, not a dead cell.
const DIRECTORY_TTL_MS: u64 = 10_000;
/// A queued call waits at least this long before it may leave the cell.
pub const DELEGATE_AFTER: Duration = Duration::from_millis(300);
/// What is sent to a cell counts against its room until its next report, so
/// this bounds how long freed room there goes unused. shortcut: every peer polls
/// every cell; a large fleet would read summaries through per-cell ambassadors.
const SUMMARY_EVERY: Duration = Duration::from_millis(500);
/// A cell that failed a delegation gets none for this long.
const DOWN_FOR: Duration = Duration::from_secs(30);

type Client =
    pb::peer_client::PeerClient<tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Auth>>;

#[derive(Clone)]
struct Auth(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);

impl tonic::service::Interceptor for Auth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut().insert("authorization", self.0.clone());
        Ok(req)
    }
}

/// The other cells, what they last reported free, and what this peer has sent them.
pub struct Fleet {
    /// The cells listed in `CHIMAERA_FLEET`, then those the directory lists.
    cells: Arc<Mutex<Vec<(String, String)>>>,
    fixed: Vec<(String, String)>,
    directory: Option<String>,
    /// This cell's (name, client endpoint), as it registers in the directory.
    me: Option<(String, String)>,
    auth: Auth,
    /// Each cell's last report: (free CPUs, all CPUs, report number).
    free: Arc<Mutex<HashMap<String, Report>>>,
    /// CPUs sent to each cell since the report this peer last read from it.
    sent: HashMap<String, (f64, u64)>,
    /// CPUs of this peer's calls running in each cell now.
    in_flight: HashMap<String, f64>,
    down_until: HashMap<String, Instant>,
    links: HashMap<String, Arc<Link>>,
}

#[derive(Clone, Copy, Debug)]
struct Report {
    free: f64,
    cpus: f64,
    number: u64,
}

/// Why a delegation produced no outcome.
pub enum Lost {
    /// The other cell never accepted the call: give it back uncounted.
    NotSent(String),
    /// It accepted the call, then lost it or could not answer.
    Lost(String),
}

/// `name=host:port,name=host:port`.
pub fn parse(text: &str) -> Result<Vec<(String, String)>, String> {
    text.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| {
            let (name, addr) = e.split_once('=').ok_or_else(|| format!("{ENV}: {e:?} is not cell=host:port"))?;
            if !addr.contains(':') {
                return Err(format!("{ENV}: cell {name:?} needs host:port, got {addr:?}"));
            }
            Ok((name.trim().to_string(), addr.trim().to_string()))
        })
        .collect()
}

/// Whether a call may run in another cell: a plain function call whose
/// arguments travel inline, with no dependencies, devices, placement or
/// budget, and no effects (so running it twice is harmless).
pub fn eligible(spec: &pb::TaskSpec, policy: &pb::Policy) -> bool {
    spec.kind() == pb::CallKind::CallFunction
        && spec.deps.is_empty()
        && !spec.stream
        && spec.actor.is_empty()
        && spec.effects() == pb::Effects::None
        && spec.args.is_some()
        && stored(spec).all(|o| o.object_id.len() == 16)
        && policy.gpus <= 0.0
        && policy.device.is_empty()
        && policy.require.is_empty()
        && policy.resources.is_empty()
        && policy.budget_cpu_seconds <= 0.0
        && policy.budget_gpu_seconds <= 0.0
}

/// The stored values a call reads (large arguments, put handles): uploaded to
/// the other cell with it. A call whose values are not all on this peer stays home.
pub fn stored(spec: &pb::TaskSpec) -> impl Iterator<Item = &pb::ObjectRef> {
    spec.args.iter().chain(spec.blobs.iter()).filter(|o| o.inline_data.is_empty() && !o.object_id.is_empty())
}

/// Stored values larger than this in all keep a call home: they would cross cells for one call.
pub const MAX_STORED_BYTES: u64 = 256 << 20;

impl Fleet {
    /// The fleet this peer may delegate to, from `CHIMAERA_FLEET` and the token
    /// in `CHIMAERA_FLEET_TOKEN_FILE`; None when no fleet is set.
    pub fn from_env() -> io::Result<Option<Fleet>> {
        let fixed = parse(&std::env::var(ENV).unwrap_or_default()).map_err(io::Error::other)?;
        let directory = std::env::var(DIRECTORY_ENV).ok().filter(|d| !d.trim().is_empty());
        if fixed.is_empty() && directory.is_none() {
            return Ok(None);
        }
        let me = match std::env::var(CELL_ENV) {
            Ok(v) => Some(
                v.split_once('@')
                    .map(|(n, e)| (n.trim().to_string(), e.trim().to_string()))
                    .ok_or_else(|| io::Error::other(format!("{CELL_ENV}: expected name@host:port, got {v:?}")))?,
            ),
            Err(_) => None,
        };
        let path = std::env::var(TOKEN_ENV).map_err(|_| io::Error::other(format!("a fleet needs {TOKEN_ENV}")))?;
        let token = std::fs::read_to_string(&path)?;
        let auth = format!("Bearer {}", token.trim())
            .parse()
            .map_err(|_| io::Error::other(format!("{TOKEN_ENV}: the token must be printable ASCII")))?;
        Ok(Some(Fleet {
            cells: Arc::new(Mutex::new(fixed.clone())),
            fixed,
            directory,
            me,
            auth: Auth(auth),
            free: Arc::default(),
            sent: HashMap::new(),
            in_flight: HashMap::new(),
            down_until: HashMap::new(),
            links: HashMap::new(),
        }))
    }

    /// Poll every cell's `Status` for its free CPUs, for as long as the peer runs.
    pub fn spawn_summaries(&self) {
        let (cells, free, auth) = (self.cells.clone(), self.free.clone(), self.auth.clone());
        let (fixed, directory, me) = (self.fixed.clone(), self.directory.clone(), self.me.clone());
        tokio::spawn(async move {
            for round in 0u32.. {
                if let Some(dir) = directory.as_deref().filter(|_| round % DIRECTORY_EVERY == 0) {
                    match register_and_list(dir, &auth, me.as_ref()).await {
                        Ok(listed) => *cells.lock().expect("fleet cells") = merged(&fixed, listed, me.as_ref()),
                        Err(e) => tracing::debug!("fleet directory {dir}: {e}"),
                    }
                }
                let known = cells.lock().expect("fleet cells").clone();
                for (name, addr) in known {
                    let (room, cpus) = summary(&addr, &auth).await;
                    record(&free, &name, room, cpus);
                }
                tokio::time::sleep(SUMMARY_EVERY).await;
            }
        });
    }

    /// A cell with room for `cpus` more, the roomiest first (the first listed on
    /// a tie). This peer counts on `share` of each cell (1 / the live peers of
    /// its own cell, which all delegate without talking to each other): of the
    /// cell's reported free CPUs, less what this peer sent since that report
    /// (so a burst cannot flood it between reports), and never more than its
    /// CPUs less this peer's calls still running there (reports lag, and those
    /// calls may not show in them yet).
    pub fn pick(&mut self, cpus: f64, share: f64) -> Option<String> {
        let now = Instant::now();
        let free = self.free.lock().expect("fleet summaries").clone();
        let mut best: Option<(String, f64, u64)> = None;
        let cells = self.cells.lock().expect("fleet cells").clone();
        for (name, _) in &cells {
            let Some(r) = free.get(name) else { continue };
            if self.down_until.get(name).is_some_and(|t| *t > now) {
                continue;
            }
            let report = r.number;
            let sent = self.sent.get(name).filter(|(_, n)| *n == report).map_or(0.0, |(s, _)| *s);
            let running = self.in_flight.get(name).copied().unwrap_or(0.0);
            let left = (r.free * share - sent).min(r.cpus * share - running);
            if left + 1e-9 >= cpus && best.as_ref().is_none_or(|(_, b, _)| left > *b) {
                best = Some((name.clone(), left, report));
            }
        }
        let (name, _, report) = best?;
        let entry = self.sent.entry(name.clone()).or_insert((0.0, report));
        if entry.1 != report {
            *entry = (0.0, report);
        }
        entry.0 += cpus;
        *self.in_flight.entry(name.clone()).or_default() += cpus;
        Some(name)
    }

    /// A call sent to `cell` ended (or came back).
    pub fn done(&mut self, cell: &str, cpus: f64) {
        if let Some(r) = self.in_flight.get_mut(cell) {
            *r = (*r - cpus).max(0.0);
        }
    }

    /// A delegation to `cell` failed: it gets none for `DOWN_FOR`.
    pub fn failed(&mut self, cell: &str) {
        self.down_until.insert(cell.to_string(), Instant::now() + DOWN_FOR);
        self.links.remove(cell);
    }

    /// The connection to `cell` that delegations share.
    pub fn link(&mut self, cell: &str) -> Option<Arc<Link>> {
        let addr = self.cells.lock().expect("fleet cells").iter().find(|(n, _)| n == cell)?.1.clone();
        let auth = self.auth.clone();
        Some(self.links.entry(cell.to_string()).or_insert_with(|| Arc::new(Link::new(addr, auth))).clone())
    }
}

/// The static cells, then the directory's, without this cell or duplicates.
fn merged(
    fixed: &[(String, String)],
    listed: Vec<(String, String)>,
    me: Option<&(String, String)>,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = fixed.to_vec();
    for (name, endpoint) in listed {
        if me.is_none_or(|(m, _)| *m != name) && !out.iter().any(|(n, _)| *n == name) {
            out.push((name, endpoint));
        }
    }
    out
}

/// Renew this cell's directory entry (when it has a name) and read every cell's.
async fn register_and_list(
    directory: &str,
    auth: &Auth,
    me: Option<&(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let mut c = connect(directory, auth).await?;
    let store =
        |write: Option<pb::StoreWrite>, read: Option<pb::StoreRead>| pb::StoreRequest { write, read, wait_ms: 0 };
    if let Some((name, endpoint)) = me {
        let put = pb::StoreWrite {
            tenant: "default".into(),
            namespace: DIRECTORY_NAMESPACE.into(),
            key: name.clone(),
            value: endpoint.clone().into_bytes(),
            ttl_ms: DIRECTORY_TTL_MS,
            ..Default::default()
        };
        let r = c.store(store(Some(put), None)).await.map_err(|e| e.message().to_string())?.into_inner();
        if !r.ok {
            return Err(format!("registering {name}: {}", r.error));
        }
    }
    let scan = pb::StoreRead {
        tenant: "default".into(),
        namespace: DIRECTORY_NAMESPACE.into(),
        kind: pb::store_read::Kind::Scan as i32,
        limit: 1000,
        ..Default::default()
    };
    let r = c.store(store(None, Some(scan))).await.map_err(|e| e.message().to_string())?.into_inner();
    if !r.ok {
        return Err(format!("listing cells: {}", r.error));
    }
    Ok(r.entries.into_iter().filter_map(|e| Some((e.key, String::from_utf8(e.value).ok()?))).collect())
}

/// Keep `cell`'s newest report, numbered after the one before.
fn record(reports: &Mutex<HashMap<String, Report>>, cell: &str, free: f64, cpus: f64) {
    let mut reports = reports.lock().expect("fleet summaries");
    let number = reports.get(cell).map_or(1, |r| r.number + 1);
    reports.insert(cell.to_string(), Report { free, cpus, number });
}

/// A cell's (free CPUs, all CPUs) over its live peers, from its `Status`; an
/// unreachable cell has none. Calls waiting in its queues take room too.
async fn summary(addr: &str, auth: &Auth) -> (f64, f64) {
    let nodes = match connect(addr, auth).await {
        Ok(mut c) => c.status(pb::StatusRequest::default()).await.map(|r| r.into_inner().nodes).unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    let alive = nodes.iter().filter(|n| n.alive);
    let room = alive.clone().map(|n| (n.cpus - n.cpu_used - n.queued as f64).max(0.0)).sum();
    (room, alive.map(|n| n.cpus).sum())
}

async fn connect(addr: &str, auth: &Auth) -> Result<Client, String> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_secs(3))
        .tcp_nodelay(true)
        .connect()
        .await
        .map_err(|e| format!("cannot reach {addr}: {e}"))?;
    Ok(pb::peer_client::PeerClient::with_interceptor(channel, auth.clone()).max_decoding_message_size(256 << 20))
}

/// One cell's client side: a connection, a shadow run per owning run (with a
/// session that routes outcomes to the delegations waiting for them), and
/// the code already shipped.
pub struct Link {
    addr: String,
    auth: Auth,
    state: tokio::sync::Mutex<LinkState>,
}

#[derive(Default)]
struct LinkState {
    client: Option<Client>,
    runs: HashMap<Id, Shadow>,
    code: HashSet<Vec<u8>>,
    /// (shadow run, value id) already uploaded: a value many calls share goes once.
    values: HashSet<(Vec<u8>, Vec<u8>)>,
}

struct Shadow {
    run: Vec<u8>,
    up: mpsc::UnboundedSender<pb::ClientMsg>,
    waiting: Arc<Mutex<HashMap<Vec<u8>, oneshot::Sender<pb::Outcome>>>>,
}

/// A delegated call's outcome, with a large value's files: (object id,
/// bytes), the value's own file last after the parts it lists (see parts).
pub type Delegated = Result<(pb::Outcome, Vec<(Vec<u8>, Vec<u8>)>), Lost>;

impl Link {
    fn new(addr: String, auth: Auth) -> Link {
        Link { addr, auth, state: tokio::sync::Mutex::default() }
    }

    /// Run `spec` (a call of `run`, whose executors start as `env`) in this
    /// cell; its outcome, with a large value's files fetched.
    pub async fn delegate(
        &self,
        run: Id,
        env: &ExecEnv,
        spec: pb::TaskSpec,
        code: Vec<u8>,
        values: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Delegated {
        let (mut client, shadow_run, up, rx, task) = {
            let mut st = self.state.lock().await;
            if st.client.is_none() {
                let mut c = connect(&self.addr, &self.auth).await.map_err(Lost::NotSent)?;
                let hello = pb::ClientHello {
                    wire_version: chimaera_proto::WIRE_VERSION,
                    client: "chimaera-peer fleet".into(),
                    ..Default::default()
                };
                c.hello(hello).await.map_err(|e| Lost::NotSent(e.message().to_string()))?;
                st.client = Some(c);
            }
            let mut client = st.client.clone().expect("connected above");
            if !st.code.contains(&spec.fn_digest) {
                let req = pb::PutCodeRequest { digest: spec.fn_digest.clone(), code };
                client.put_code(req).await.map_err(|e| Lost::NotSent(e.message().to_string()))?;
                st.code.insert(spec.fn_digest.clone());
            }
            let shadow = match st.runs.entry(run) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(open_shadow(&mut client, run, env).await.map_err(Lost::NotSent)?)
                }
            };
            let (tx, rx) = oneshot::channel();
            // The call's id there lives in the shadow run's vshard (a task's vshard is its id's).
            let home_task = spec.key.as_ref().map(|k| k.task.clone()).unwrap_or_default();
            let shadow_id = Id::from_slice(&shadow.run).map_err(|e| Lost::NotSent(e.to_string()))?;
            let task = shadow_id.child(&[&home_task]).to_vec();
            shadow.waiting.lock().expect("shadow waiters").insert(task.clone(), tx);
            (client, shadow.run.clone(), shadow.up.clone(), rx, task)
        };
        let key = pb::TaskKey { run: shadow_run.clone(), task };
        // Its stored values go to the shadow run there, once each, under their own ids
        // (PutObject: a ch.put value's id is not its content); that cell's ingress then
        // names itself their holder when the call is submitted.
        for (id, bytes) in values {
            let key = (shadow_run.clone(), id.clone());
            if self.state.lock().await.values.contains(&key) {
                continue;
            }
            let (size, checksum) = (bytes.len() as u64, blake3::hash(&bytes).as_bytes()[..16].to_vec());
            let mut chunks: Vec<pb::PutObjectChunk> =
                bytes.chunks(1 << 20).map(|c| pb::PutObjectChunk { data: c.to_vec(), ..Default::default() }).collect();
            if chunks.is_empty() {
                chunks.push(pb::PutObjectChunk::default());
            }
            let first = &mut chunks[0];
            (first.run, first.object_id, first.size, first.checksum) = (shadow_run.clone(), id, size, checksum);
            client.put_object(tokio_stream::iter(chunks)).await.map_err(|e| Lost::NotSent(e.message().to_string()))?;
            self.state.lock().await.values.insert(key);
        }
        let mut moved = spec;
        moved.key = Some(key.clone());
        moved.parent = None;
        // Every value the call names now belongs to the shadow run: its worker reads
        // the blobs an inline frame refers to from the frame's run's directory too.
        for o in moved.args.iter_mut().chain(moved.blobs.iter_mut()) {
            o.run = shadow_run.clone();
            if o.inline_data.is_empty() {
                o.locations.clear();
            }
        }
        let sent = up.send(pb::ClientMsg {
            msg: Some(pb::client_msg::Msg::Submit(pb::SubmitTasks { tasks: vec![moved], grant_to: None })),
            ..Default::default()
        });
        let watched = up.send(pb::ClientMsg {
            msg: Some(pb::client_msg::Msg::Watch(pb::WatchMsg { tasks: vec![key] })),
            ..Default::default()
        });
        if sent.is_err() || watched.is_err() {
            return Err(Lost::NotSent("the session to the other cell closed".into()));
        }
        let outcome = rx.await.map_err(|_| Lost::Lost("the other cell's session ended before the outcome".into()))?;
        match &outcome.value {
            Some(pb::outcome::Value::Err(e)) if e.kind() != pb::error_record::Kind::User => {
                Err(Lost::Lost(format!("the other cell failed it: {}", e.message)))
            }
            Some(pb::outcome::Value::Ok(obj)) if obj.inline_data.is_empty() => {
                let bytes = fetch(&mut client, obj).await.map_err(Lost::Lost)?;
                let mut files = Vec::new();
                for id in crate::parts::ids(&bytes) {
                    let part = pb::ObjectRef { object_id: id.clone(), size: 0, ..obj.clone() };
                    files.push((id, fetch(&mut client, &part).await.map_err(Lost::Lost)?));
                }
                files.push((obj.object_id.clone(), bytes));
                Ok((outcome, files))
            }
            _ => Ok((outcome, Vec::new())),
        }
    }
}

async fn open_shadow(client: &mut Client, run: Id, env: &ExecEnv) -> Result<Shadow, String> {
    let rec = client
        .create_run(pb::CreateRunRequest {
            submission_key: format!("fleet-{}", run.hex()),
            // Not detached, and tied to this peer's session: if this peer dies,
            // the other cell cancels the run's calls after this grace.
            detached: false,
            client_grace_ms: 10_000,
            description: format!("calls of run {} from another cell", run.hex()),
            python: env.python.clone(),
            cwd: env.cwd.clone(),
            sys_path: env.sys_path.clone(),
            env: env.env.clone().into_iter().collect(),
            ..Default::default()
        })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let (up, up_rx) = mpsc::unbounded_channel();
    up.send(pb::ClientMsg { msg: Some(pb::client_msg::Msg::AttachRun(rec.run.clone())), ..Default::default() })
        .map_err(|_| "session closed".to_string())?;
    let mut down = client
        .session(tokio_stream::wrappers::UnboundedReceiverStream::new(up_rx))
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let waiting: Arc<Mutex<HashMap<Vec<u8>, oneshot::Sender<pb::Outcome>>>> = Arc::default();
    let routes = waiting.clone();
    tokio::spawn(async move {
        while let Ok(Some(ev)) = down.message().await {
            if let Some(pb::session_event::Event::Outcome(o)) = ev.event {
                let task = o.task.map(|t| t.task).unwrap_or_default();
                if let Some(tx) = routes.lock().expect("shadow waiters").remove(&task) {
                    let _ = tx.send(o.outcome.unwrap_or_default());
                }
            }
        }
        routes.lock().expect("shadow waiters").clear(); // waiters see the session end
    });
    Ok(Shadow { run: rec.run, up, waiting })
}

async fn fetch(client: &mut Client, obj: &pb::ObjectRef) -> Result<Vec<u8>, String> {
    let req = pb::GetObjectRequest { run: obj.run.clone(), object_id: obj.object_id.clone(), offset: 0, len: 0 };
    let mut stream = client.get_object(req).await.map_err(|e| e.message().to_string())?.into_inner();
    let mut bytes = Vec::with_capacity(obj.size as usize);
    while let Some(chunk) = stream.message().await.map_err(|e| e.message().to_string())? {
        bytes.extend_from_slice(&chunk.data);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fleet_setting_names_cells_and_their_endpoints() {
        assert_eq!(
            parse(" b=10.0.0.5:7710, c=h:1 ").unwrap(),
            [("b".into(), "10.0.0.5:7710".into()), ("c".into(), "h:1".into())]
        );
        assert!(parse("b").unwrap_err().contains("cell=host:port"));
        assert!(parse("b=host").unwrap_err().contains("needs host:port"));
    }

    #[test]
    fn only_self_contained_calls_leave_the_cell() {
        let args = pb::ObjectRef { inline_data: vec![1], ..Default::default() };
        let plain = pb::TaskSpec { args: Some(args.clone()), ..Default::default() };
        let policy = pb::Policy::default();
        assert!(eligible(&plain, &policy));
        let with = |f: &dyn Fn(&mut pb::TaskSpec)| {
            let mut s = plain.clone();
            f(&mut s);
            eligible(&s, &policy)
        };
        assert!(!with(&|s| s.deps.push(pb::TaskKey::default())), "a dependency stays home");
        let stored_args = pb::ObjectRef { object_id: vec![7; 16], size: 5 << 20, ..Default::default() };
        assert!(with(&|s| s.args = Some(stored_args.clone())), "stored args travel (uploaded with the call)");
        assert!(with(&|s| s.blobs.push(stored_args.clone())), "so do put handles");
        assert!(
            !with(&|s| s.args = Some(pb::ObjectRef { object_id: vec![1], ..Default::default() })),
            "a malformed id"
        );
        assert!(!with(&|s| s.effects = pb::Effects::External as i32), "effects");
        assert!(!with(&|s| s.kind = pb::CallKind::CallActorMethod as i32), "actor calls");
        assert!(!with(&|s| s.stream = true), "generators");
        assert!(!eligible(&plain, &pb::Policy { gpus: 1.0, ..Default::default() }), "devices");
        assert!(!eligible(&plain, &pb::Policy { budget_cpu_seconds: 5.0, ..Default::default() }), "budgets");
    }

    #[test]
    fn delegations_count_against_a_cells_room_until_its_next_report() {
        let mut f = Fleet {
            cells: Arc::new(Mutex::new(vec![("b".into(), "h:1".into()), ("c".into(), "h:2".into())])),
            fixed: Vec::new(),
            directory: None,
            me: None,
            auth: Auth("Bearer t".parse().unwrap()),
            free: Arc::default(),
            sent: HashMap::new(),
            in_flight: HashMap::new(),
            down_until: HashMap::new(),
            links: HashMap::new(),
        };
        let report = |free: f64, cpus: f64, number: u64| Report { free, cpus, number };
        assert_eq!(f.pick(1.0, 1.0), None, "no report yet: no room known");
        f.free.lock().unwrap().extend([("b".to_string(), report(2.0, 4.0, 1)), ("c".to_string(), report(3.0, 4.0, 1))]);
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("c"));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("b"), "a tie goes to the first listed");
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("c"));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("b"));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("c"));
        assert_eq!(f.pick(1.0, 1.0), None, "both full until they report again");
        // b's next report lags: it still shows 2 free, but 2 of b's 4 CPUs run
        // this peer's calls, and 2 more were sent: none left until they end.
        f.free.lock().unwrap().insert("b".to_string(), report(2.0, 4.0, 2));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("b"));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("b"));
        assert_eq!(f.pick(1.0, 1.0), None, "4 of b's 4 CPUs run this peer's calls");
        f.done("b", 1.0);
        assert_eq!(f.pick(1.0, 1.0), None, "what was sent counts until b reports again");
        f.free.lock().unwrap().insert("b".to_string(), report(1.0, 4.0, 3));
        assert_eq!(f.pick(1.0, 1.0).as_deref(), Some("b"), "one ended and b reported it: room again");
        assert_eq!(f.pick(1.0, 1.0), None);
        f.free.lock().unwrap().insert("b".to_string(), report(4.0, 4.0, 4));
        f.done("b", 4.0);
        assert_eq!(f.pick(2.0, 0.25), None, "a quarter of b is 1 CPU: not 2");
        assert_eq!(f.pick(1.0, 0.25).as_deref(), Some("b"));
        f.free.lock().unwrap().insert("c".to_string(), report(4.0, 4.0, 2));
        f.failed("c"); // out for a while, whatever its room: b takes what fits, c nothing
        let picks: Vec<String> = std::iter::from_fn(|| f.pick(1.0, 1.0)).collect();
        assert_eq!(picks, ["b", "b", "b"]);
    }

    #[test]
    fn the_directory_adds_cells_but_never_this_one_or_a_listed_one_twice() {
        let fixed = vec![("b".to_string(), "h:1".to_string())];
        let listed = vec![("a".into(), "h:0".into()), ("b".into(), "h:9".into()), ("c".into(), "h:2".into())];
        let me = ("a".to_string(), "h:0".to_string());
        assert_eq!(merged(&fixed, listed, Some(&me)), [("b".into(), "h:1".into()), ("c".into(), "h:2".into())]);
    }
}
