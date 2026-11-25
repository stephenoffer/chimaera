//! Federation (docs/federation.md): when this cell is full, run a granted
//! attempt in another cell, as an ordinary authenticated client of it.
//!
//! The owning shard never learns the call left the cell. This peer publishes
//! the other cell's outcome as its attempt's outcome, or reports the attempt
//! failed, and the shard retries it like any lost attempt. Only calls whose
//! whole meaning travels with them are eligible (`eligible`).
//!
//! Which cells there are and how much room each has comes from this cell's
//! view of the fleet, which `view` keeps.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chimaera_core::cell;
use chimaera_core::ids::Id;
use chimaera_core::pb;
use tokio::sync::{mpsc, oneshot};

use crate::executor::ExecEnv;
use crate::router::Router;

mod view;
pub use view::answer;

pub const ENV: &str = "CHIMAERA_FLEET";
pub const TOKEN_ENV: &str = "CHIMAERA_FLEET_TOKEN_FILE";
pub const DIRECTORY_ENV: &str = "CHIMAERA_FLEET_DIRECTORY";
pub const CELL_ENV: &str = "CHIMAERA_CELL";
/// A queued call waits at least this long before it may leave the cell.
pub const DELEGATE_AFTER: Duration = Duration::from_millis(300);
/// A cell that failed a delegation gets none for this long.
const DOWN_FOR: Duration = Duration::from_secs(30);
/// The resource class delegated calls need (only CPU calls are eligible).
const CPU: &str = "cpu";

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

/// The other cells, as this cell's view has them, and what this peer sent them.
pub struct Fleet {
    /// The cells listed in `CHIMAERA_FLEET`.
    fixed: Vec<(String, String)>,
    directory: Option<String>,
    /// This cell's name and client endpoint (`this_cell`).
    me: (String, String),
    /// This cell's region (its peers' `region` label): calls go to cells there first.
    region: String,
    auth: Auth,
    /// The cell's view, as this peer last read or wrote it.
    view: Arc<Mutex<pb::FleetView>>,
    /// CPUs sent to each cell since the report (by number) this peer last read from it.
    sent: HashMap<String, (f64, u64)>,
    /// CPUs of this peer's calls running in each cell now.
    in_flight: HashMap<String, f64>,
    down_until: HashMap<String, Instant>,
    links: HashMap<String, Arc<Link>>,
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

/// `CHIMAERA_CELL`: this cell's name, as the other cells list it, then
/// `@host:port` where they reach it, which lists it in the directory (none:
/// not listed). A fleet needs the name: each cell's neighbors follow from it.
fn this_cell(value: Option<&str>) -> Result<(String, String), String> {
    let v = value.unwrap_or_default();
    let (name, endpoint) = v.split_once('@').unwrap_or((v, ""));
    if name.trim().is_empty() {
        return Err(format!("a fleet needs {CELL_ENV}: this cell's name, as the other cells list it"));
    }
    Ok((name.trim().to_string(), endpoint.trim().to_string()))
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
        let me = this_cell(std::env::var(CELL_ENV).ok().as_deref()).map_err(io::Error::other)?;
        let path = std::env::var(TOKEN_ENV).map_err(|_| io::Error::other(format!("a fleet needs {TOKEN_ENV}")))?;
        let token = std::fs::read_to_string(&path)?;
        let auth = format!("Bearer {}", token.trim())
            .parse()
            .map_err(|_| io::Error::other(format!("{TOKEN_ENV}: the token must be printable ASCII")))?;
        let region = crate::labels::parse(&std::env::var("CHIMAERA_LABELS").unwrap_or_default()).remove("region");
        Ok(Some(Fleet {
            fixed,
            directory,
            me,
            region: region.unwrap_or_default(),
            auth: Auth(auth),
            view: Arc::default(),
            sent: HashMap::new(),
            in_flight: HashMap::new(),
            down_until: HashMap::new(),
            links: HashMap::new(),
        }))
    }

    /// Keep this cell's view of the fleet for as long as the peer runs: as the
    /// cell's publisher, or by reading what the publisher wrote.
    pub fn spawn_summaries(&self, router: Arc<Router>) {
        view::spawn(self, router);
    }

    /// The cell's view as this peer last read it (`Status`).
    pub fn view(&self) -> pb::FleetView {
        self.view.lock().expect("fleet view").clone()
    }

    /// A cell this cell lists (reports name others too, but calls go only
    /// where this cell knows to reach) with room for `cpus` more: one in this
    /// cell's region if one there has room, drawn in proportion to room
    /// (`cell::choose`; `key`, the call's id, makes the draw). This peer counts
    /// on `share` of each cell (1 / the live peers of its own cell, which all
    /// delegate without talking to each other): of the cell's reported free
    /// CPUs, less what this peer sent since that report (so a burst cannot
    /// flood it between reports), and never more than its CPUs less this
    /// peer's calls still running there (reports lag, and those calls may not
    /// show in them yet).
    pub fn pick(&mut self, cpus: f64, share: f64, key: &[u8]) -> Option<String> {
        let now = Instant::now();
        let mut cells: Vec<(String, String, u64, f64)> = Vec::new();
        let view = self.view.lock().expect("fleet view");
        let listed: HashSet<&str> = view.cells.iter().map(|c| c.name.as_str()).collect();
        for c in view.capacity.iter().filter(|c| c.resource == CPU && listed.contains(c.cell.as_str())) {
            if self.down_until.get(&c.cell).is_some_and(|t| *t > now) {
                continue;
            }
            let sent = self.sent.get(&c.cell).filter(|(_, n)| *n == c.number).map_or(0.0, |(s, _)| *s);
            let running = self.in_flight.get(&c.cell).copied().unwrap_or(0.0);
            let left = (c.free * share - sent).min(c.total * share - running);
            cells.push((c.cell.clone(), c.region.clone(), c.number, left));
        }
        let room: Vec<(&str, f64)> = cells.iter().map(|(_, region, _, left)| (region.as_str(), *left)).collect();
        let draw = u64::from_le_bytes(blake3::hash(key).as_bytes()[..8].try_into().expect("8 bytes"));
        let chosen = cell::choose(&room, &self.region, cpus, draw)?;
        let (name, _, report, _) = cells.swap_remove(chosen);
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

    /// The connection to `cell` that delegations share, at the endpoint this
    /// cell lists it at.
    pub fn link(&mut self, cell: &str) -> Option<Arc<Link>> {
        let addr = self.view.lock().expect("fleet view").cells.iter().find(|c| c.name == cell)?.endpoint.clone();
        let auth = self.auth.clone();
        Some(self.links.entry(cell.to_string()).or_insert_with(|| Arc::new(Link::new(addr, auth))).clone())
    }
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

/// A delegated call's outcome, with its files: (object id, bytes), the
/// buffers it links and the parts it lists first, the value's own file last.
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
            Some(pb::outcome::Value::Ok(obj)) => {
                let mut files = Vec::new();
                // Buffers stored apart from the value (arena-linked arrays), inline or not.
                for b in &obj.blobs {
                    let run = if b.run.is_empty() { obj.run.clone() } else { b.run.clone() };
                    let blob = pb::ObjectRef { run, ..b.clone() };
                    files.push((b.object_id.clone(), fetch(&mut client, &blob).await.map_err(Lost::Lost)?));
                }
                if obj.inline_data.is_empty() {
                    let bytes = fetch(&mut client, obj).await.map_err(Lost::Lost)?;
                    for id in crate::parts::ids(&bytes) {
                        let part = pb::ObjectRef { object_id: id.clone(), size: 0, blobs: Vec::new(), ..obj.clone() };
                        files.push((id, fetch(&mut client, &part).await.map_err(Lost::Lost)?));
                    }
                    files.push((obj.object_id.clone(), bytes));
                }
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
        assert_eq!(this_cell(Some(" a ")).unwrap(), ("a".into(), String::new()), "a name alone: not in the directory");
        assert_eq!(this_cell(Some("a@10.0.0.5:7710")).unwrap(), ("a".into(), "10.0.0.5:7710".into()));
        for unnamed in [None, Some(""), Some("@h:1")] {
            assert!(this_cell(unnamed).unwrap_err().contains("a fleet needs CHIMAERA_CELL"), "{unnamed:?}");
        }
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

    fn fleet(region: &str) -> Fleet {
        Fleet {
            fixed: Vec::new(),
            directory: None,
            me: ("a".into(), String::new()),
            region: region.into(),
            auth: Auth("Bearer t".parse().unwrap()),
            view: Arc::default(),
            sent: HashMap::new(),
            in_flight: HashMap::new(),
            down_until: HashMap::new(),
            links: HashMap::new(),
        }
    }

    fn report(cell: &str, region: &str, free: f64, number: u64) -> pb::CellCapacity {
        let (cell, region, resource) = (cell.into(), region.into(), CPU.into());
        pb::CellCapacity { cell, region, resource, free, total: 4.0, number, ..Default::default() }
    }

    /// Set `f`'s view to `reports`, of cells it lists.
    fn set(f: &Fleet, reports: Vec<pb::CellCapacity>) {
        let mut view = f.view.lock().unwrap();
        view.cells = reports.iter().map(|r| pb::FleetCell { name: r.cell.clone(), endpoint: "h:1".into() }).collect();
        view.capacity = reports;
    }

    /// Every cell `f` picks for calls of `cpus` until none has room, sorted.
    fn picks(f: &mut Fleet, cpus: f64, share: f64) -> Vec<String> {
        let mut out: Vec<String> = (0u64..).map_while(|i| f.pick(cpus, share, &i.to_le_bytes())).collect();
        out.sort();
        out
    }

    #[test]
    fn delegations_count_against_a_cells_room_until_its_next_report() {
        let mut f = fleet("");
        assert_eq!(f.pick(1.0, 1.0, b"x"), None, "no report yet: no room known");
        set(&f, vec![report("b", "", 2.0, 1), report("c", "", 3.0, 1)]);
        assert_eq!(picks(&mut f, 1.0, 1.0), ["b", "b", "c", "c", "c"], "both full until they report again");
        // b's next report lags: it still shows 2 free, but 2 of b's 4 CPUs run
        // this peer's calls, and 2 more were sent: none left until they end.
        set(&f, vec![report("b", "", 2.0, 2), report("c", "", 0.0, 2)]);
        assert_eq!(picks(&mut f, 1.0, 1.0), ["b", "b"], "4 of b's 4 CPUs run this peer's calls");
        f.done("b", 1.0);
        assert_eq!(f.pick(1.0, 1.0, b"y"), None, "what was sent counts until b reports again");
        set(&f, vec![report("b", "", 1.0, 3), report("c", "", 0.0, 2)]);
        assert_eq!(picks(&mut f, 1.0, 1.0), ["b"], "one ended and b reported it: room again");
        f.done("b", 4.0);
        f.done("c", 3.0);
        set(&f, vec![report("b", "", 4.0, 4), report("c", "", 4.0, 3)]);
        assert_eq!(f.pick(2.0, 0.25, b"z"), None, "a quarter of a cell is 1 CPU: not 2");
        f.failed("c"); // out for a while, whatever its room: b takes what fits, c nothing
        assert_eq!(picks(&mut f, 1.0, 0.25), ["b"]);
    }

    #[test]
    fn calls_go_to_this_regions_cells_first_and_spread_by_room() {
        let mut f = fleet("eu");
        let near = |free| report("b", "eu", free, 1);
        set(&f, vec![near(2.0), report("c", "us", 4.0, 1), report("d", "eu", 1.0, 1)]);
        assert_eq!(picks(&mut f, 1.0, 1.0), ["b", "b", "c", "c", "c", "c", "d"]);
        // The first three went to b or d, the cells in this region with room.
        let mut f = fleet("eu");
        set(&f, vec![near(2.0), report("c", "us", 4.0, 1), report("d", "eu", 1.0, 1)]);
        let first: Vec<String> = (0u64..3).filter_map(|i| f.pick(1.0, 1.0, &i.to_le_bytes())).collect();
        assert!(first.iter().all(|c| c != "c"), "{first:?}");
    }

    #[test]
    fn calls_go_only_to_cells_this_one_lists() {
        let mut f = fleet("");
        set(&f, vec![report("b", "", 1.0, 1)]);
        // A report another cell passed on, of a cell this one does not list:
        // its endpoint, if any, would be the other cell's to choose.
        f.view.lock().unwrap().capacity.push(report("x", "", 4.0, 1));
        assert_eq!(picks(&mut f, 1.0, 1.0), ["b"]);
        assert!(f.link("x").is_none() && f.link("b").is_some());
    }
}
