//! `chimaera._native`: the Rust side of the Python package.
//!
//! - [`Client`]: talks to a peer over its client socket (one ordered session
//!   stream plus unary calls). Submissions are coalesced into batches.
//! - [`ExecConn`]: an executor's connection to its supervising peer.
//! - Id helpers, so Python derives task and object ids exactly as Rust does.
//!
//! Rust owns every wire format; Python passes and receives plain values.

use chimaera_core::hlc;
use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_proto::frame;
use prost::Message;
use pyo3::exceptions::{PyConnectionError, PyFileNotFoundError, PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const BATCH_MAX: usize = 64;
const BATCH_WINDOW: Duration = Duration::from_micros(200);
const MAX_FRAME: usize = 1 << 30;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("chimaera-client")
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

fn id(b: &[u8]) -> PyResult<Id> {
    Id::from_slice(b).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// A task key travels in Python as 32 bytes: run id then task id.
fn task_key(b: &[u8]) -> PyResult<pb::TaskKey> {
    if b.len() != 32 {
        return Err(PyValueError::new_err(format!("task key must be 32 bytes, got {}", b.len())));
    }
    Ok(pb::TaskKey { run: b[..16].to_vec(), task: b[16..].to_vec() })
}

fn key_bytes(k: &pb::TaskKey) -> Vec<u8> {
    [k.run.as_slice(), k.task.as_slice()].concat()
}

// ---- ids ------------------------------------------------------------------

/// `Id::derive(cell, vshard, parts)`.
#[pyfunction]
fn derive_id<'py>(py: Python<'py>, cell: u16, vshard: u16, parts: Vec<Vec<u8>>) -> Bound<'py, PyBytes> {
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    PyBytes::new(py, &Id::derive(cell, vshard, &refs).0)
}

/// `Id::child(parent, parts)`: stays in the parent's vshard.
#[pyfunction]
fn child_id<'py>(py: Python<'py>, parent: &[u8], parts: Vec<Vec<u8>>) -> PyResult<Bound<'py, PyBytes>> {
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    Ok(PyBytes::new(py, &id(parent)?.child(&refs).0))
}

#[pyfunction]
fn vshard_of(b: &[u8]) -> PyResult<u16> {
    Ok(id(b)?.vshard())
}

/// Path of the peer socket `name` for the data directory `dir` (shared rule
/// with the peer: long directories put sockets under /tmp).
#[pyfunction]
fn socket_path(dir: std::path::PathBuf, name: &str) -> String {
    chimaera_proto::uds::socket_path(&dir, name).display().to_string()
}

#[pyfunction]
fn blake3_digest<'py>(py: Python<'py>, data: &[u8]) -> Bound<'py, PyBytes> {
    PyBytes::new(py, blake3::hash(data).as_bytes())
}

// ---- outcomes -------------------------------------------------------------

fn kind_name(k: pb::error_record::Kind) -> &'static str {
    use pb::error_record::Kind as K;
    match k {
        K::User => "user",
        K::Infra => "infra",
        K::Oom => "oom",
        K::Cancelled => "cancelled",
        K::Deadline => "deadline",
        K::OutcomeUnknown => "outcome_unknown",
        K::Causal => "causal",
        K::Infeasible => "infeasible",
        K::WorkerCrashed => "worker_crashed",
        K::DataLost => "data_lost",
        K::ActorDied => "actor_died",
        K::CommandConflict => "command_conflict",
    }
}

fn kind_from(name: &str) -> pb::error_record::Kind {
    use pb::error_record::Kind as K;
    match name {
        "infra" => K::Infra,
        "oom" => K::Oom,
        "cancelled" => K::Cancelled,
        "deadline" => K::Deadline,
        "outcome_unknown" => K::OutcomeUnknown,
        "causal" => K::Causal,
        "infeasible" => K::Infeasible,
        "worker_crashed" => K::WorkerCrashed,
        "data_lost" => K::DataLost,
        "actor_died" => K::ActorDied,
        _ => K::User,
    }
}

/// ObjectRef -> ("ok", inline|None, run|None, object_id, size, oob_buffers).
fn object_py<'py>(py: Python<'py>, o: &pb::ObjectRef) -> PyResult<Bound<'py, PyTuple>> {
    let inline: Option<Bound<'py, PyBytes>> =
        (!o.inline_data.is_empty() || o.size == 0).then(|| PyBytes::new(py, &o.inline_data));
    let run: Option<Bound<'py, PyBytes>> = (!o.run.is_empty()).then(|| PyBytes::new(py, &o.run));
    PyTuple::new(
        py,
        [
            "ok".into_pyobject(py)?.into_any(),
            inline.into_pyobject(py)?.into_any(),
            run.into_pyobject(py)?.into_any(),
            PyBytes::new(py, &o.object_id).into_any(),
            o.size.into_pyobject(py)?.into_any(),
            o.oob_buffers.into_pyobject(py)?.into_any(),
        ],
    )
}

/// Outcome -> ok tuple (see `object_py`) or
/// ("err", kind, type_name, message, exc_pickle, traceback, upstream|None, attempt, detail_json).
fn outcome_py<'py>(py: Python<'py>, o: &pb::Outcome) -> PyResult<Bound<'py, PyTuple>> {
    match &o.value {
        Some(pb::outcome::Value::Ok(obj)) => object_py(py, obj),
        Some(pb::outcome::Value::Err(e)) => {
            let upstream: Option<Bound<'py, PyBytes>> = e.upstream.as_ref().map(|u| PyBytes::new(py, &key_bytes(u)));
            PyTuple::new(
                py,
                [
                    "err".into_pyobject(py)?.into_any(),
                    kind_name(e.kind()).into_pyobject(py)?.into_any(),
                    e.type_name.as_str().into_pyobject(py)?.into_any(),
                    e.message.as_str().into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &e.exc_pickle).into_any(),
                    e.traceback.as_str().into_pyobject(py)?.into_any(),
                    upstream.into_pyobject(py)?.into_any(),
                    o.attempt.into_pyobject(py)?.into_any(),
                    e.detail_json.as_str().into_pyobject(py)?.into_any(),
                ],
            )
        }
        None => PyTuple::new(py, ["missing"]),
    }
}

/// Build an Outcome from Python: ("ok", run, object_id, size, inline|None, oob)
/// or ("err", kind, type_name, message, exc_pickle, traceback, detail_json).
fn outcome_from(t: &Bound<'_, PyTuple>) -> PyResult<pb::Outcome> {
    let tag: String = t.get_item(0)?.extract()?;
    let value = if tag == "ok" {
        let run: Vec<u8> = t.get_item(1)?.extract()?;
        let object_id: Vec<u8> = t.get_item(2)?.extract()?;
        let size: u64 = t.get_item(3)?.extract()?;
        let inline: Option<Vec<u8>> = t.get_item(4)?.extract()?;
        let oob: u32 = t.get_item(5)?.extract()?;
        pb::outcome::Value::Ok(pb::ObjectRef {
            object_id,
            size,
            inline_data: inline.unwrap_or_default(),
            run,
            oob_buffers: oob,
            ..Default::default()
        })
    } else {
        let kind: String = t.get_item(1)?.extract()?;
        pb::outcome::Value::Err(pb::ErrorRecord {
            kind: kind_from(&kind) as i32,
            type_name: t.get_item(2)?.extract()?,
            message: t.get_item(3)?.extract()?,
            exc_pickle: t.get_item(4)?.extract()?,
            traceback: t.get_item(5)?.extract()?,
            detail_json: t.get_item(6)?.extract()?,
            ..Default::default()
        })
    };
    Ok(pb::Outcome { value: Some(value), ..Default::default() })
}

/// Policy from a Python dict of option values.
fn policy_from(d: &Bound<'_, PyDict>) -> PyResult<pb::Policy> {
    let get_f = |k: &str| -> PyResult<f64> { d.get_item(k)?.map_or(Ok(0.0), |v| v.extract()) };
    let get_u = |k: &str| -> PyResult<u64> { d.get_item(k)?.map_or(Ok(0), |v| v.extract()) };
    let get_b = |k: &str| -> PyResult<bool> { d.get_item(k)?.map_or(Ok(false), |v| v.extract()) };
    let get_s = |k: &str| -> PyResult<String> { d.get_item(k)?.map_or(Ok(String::new()), |v| v.extract()) };
    let accounting = match get_s("accounting")?.as_str() {
        "process" => pb::policy::Accounting::Process,
        "isolated" => pb::policy::Accounting::Isolated,
        _ => pb::policy::Accounting::Auto,
    };
    let adaptive = match get_s("adaptive")?.as_str() {
        "observe" => pb::policy::Adaptive::Observe,
        "frozen" => pb::policy::Adaptive::Frozen,
        _ => pb::policy::Adaptive::On,
    };
    let resources: BTreeMap<String, f64> = d.get_item("resources")?.map_or(Ok(BTreeMap::new()), |v| v.extract())?;
    let require: BTreeMap<String, String> = d.get_item("require")?.map_or(Ok(BTreeMap::new()), |v| v.extract())?;
    let prefer: Vec<String> = d.get_item("prefer")?.map_or(Ok(Vec::new()), |v| v.extract())?;
    Ok(pb::Policy {
        cpus: get_f("cpus")?,
        memory: get_u("memory")?,
        memory_limit: get_u("memory_limit")?,
        gpus: get_f("gpus")?,
        device: get_s("device")?,
        resources: resources.into_iter().collect(),
        accounting: accounting as i32,
        adaptive: adaptive as i32,
        concurrency: get_u("concurrency")? as u32,
        range_size: get_u("range_size")? as u32,
        interactive: get_b("interactive")?,
        deterministic: get_b("deterministic")?,
        env_digest: get_s("env_digest")?,
        require: require.into_iter().collect(),
        prefer,
        source: get_s("source")?,
    })
}

fn policy_py<'py>(py: Python<'py>, p: &pb::Policy) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("cpus", p.cpus)?;
    d.set_item("memory", p.memory)?;
    d.set_item("memory_limit", p.memory_limit)?;
    d.set_item("gpus", p.gpus)?;
    d.set_item("device", &p.device)?;
    d.set_item("concurrency", p.concurrency)?;
    d.set_item("interactive", p.interactive)?;
    d.set_item("deterministic", p.deterministic)?;
    d.set_item("source", &p.source)?;
    Ok(d)
}

/// A call as Python describes it (a dict with these keys).
#[derive(FromPyObject)]
struct CallSpec {
    #[pyo3(item)]
    key: Vec<u8>,
    #[pyo3(item)]
    kind: i32,
    #[pyo3(item)]
    fn_digest: Vec<u8>,
    #[pyo3(item)]
    args_inline: Option<Vec<u8>>,
    #[pyo3(item)]
    args_object: Option<Vec<u8>>,
    #[pyo3(item)]
    args_size: u64,
    #[pyo3(item)]
    args_oob: u32,
    #[pyo3(item)]
    deps: Vec<Vec<u8>>,
    #[pyo3(item)]
    effects: i32,
    #[pyo3(item)]
    max_attempts: u32,
    #[pyo3(item)]
    deadline_unix_ms: u64,
    #[pyo3(item)]
    policy: Py<PyDict>,
    #[pyo3(item)]
    actor: Option<Vec<u8>>,
    #[pyo3(item)]
    handle: u64,
    #[pyo3(item)]
    seq: u64,
    #[pyo3(item)]
    method: String,
    #[pyo3(item)]
    item_index: u32,
    #[pyo3(item)]
    parent: Option<Vec<u8>>,
    #[pyo3(item)]
    speculation_allowed: bool,
    #[pyo3(item)]
    retry_exception_types: Vec<String>,
    #[pyo3(item)]
    max_oom_escalations: u32,
}

impl CallSpec {
    fn into_spec(self, py: Python<'_>) -> PyResult<pb::TaskSpec> {
        let key = task_key(&self.key)?;
        let policy = policy_from(self.policy.bind(py))?;
        Ok(pb::TaskSpec {
            args: Some(pb::ObjectRef {
                object_id: self.args_object.unwrap_or_default(),
                size: self.args_size,
                inline_data: self.args_inline.unwrap_or_default(),
                run: key.run.clone(),
                oob_buffers: self.args_oob,
                ..Default::default()
            }),
            key: Some(key),
            kind: self.kind,
            fn_digest: self.fn_digest,
            deps: self.deps.iter().map(|d| task_key(d)).collect::<PyResult<_>>()?,
            effects: self.effects,
            max_attempts: self.max_attempts,
            deadline_hlc: if self.deadline_unix_ms == 0 { 0 } else { hlc::pack(self.deadline_unix_ms, 0) },
            policy: policy.encode_to_vec(),
            actor: self.actor.unwrap_or_default(),
            handle: self.handle,
            seq: self.seq,
            method: self.method,
            item_index: self.item_index,
            parent: self.parent.map(|p| task_key(&p)).transpose()?,
            speculation_allowed: self.speculation_allowed,
            max_oom_escalations: self.max_oom_escalations,
            retry_exception_types: self.retry_exception_types,
        })
    }
}

// ---- client ---------------------------------------------------------------

type PeerClient = pb::peer_client::PeerClient<tonic::transport::Channel>;

struct Shared {
    events: Mutex<VecDeque<pb::SessionEvent>>,
    ready: Condvar,
    closed: AtomicBool,
    pending: Mutex<Vec<pb::TaskSpec>>,
    flush_scheduled: AtomicBool,
    last_flush: Mutex<Instant>,
    seq: AtomicU64,
    up: mpsc::UnboundedSender<pb::ClientMsg>,
}

impl Shared {
    fn send(&self, msg: pb::client_msg::Msg) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.up.send(pb::ClientMsg { seq, msg: Some(msg) });
        seq
    }

    fn flush(&self) {
        let tasks = std::mem::take(&mut *self.pending.lock().expect("pending"));
        self.flush_scheduled.store(false, Ordering::Release);
        *self.last_flush.lock().expect("last_flush") = Instant::now();
        if !tasks.is_empty() {
            self.send(pb::client_msg::Msg::Submit(pb::SubmitTasks { tasks, grant_to: None }));
        }
    }
}

/// Connection to a peer.
#[pyclass(module = "chimaera._native")]
struct Client {
    peer: PeerClient,
    shared: Arc<Shared>,
    info: pb::PeerInfo,
}

fn rpc_err(e: tonic::Status) -> PyErr {
    match e.code() {
        tonic::Code::Unavailable => PyConnectionError::new_err(e.message().to_string()),
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
            PyValueError::new_err(e.message().to_string())
        }
        _ => PyRuntimeError::new_err(e.message().to_string()),
    }
}

#[pymethods]
impl Client {
    /// Connect to the peer listening on `socket` (a Unix socket path).
    #[new]
    fn new(py: Python<'_>, socket: String, client: String) -> PyResult<Client> {
        py.detach(|| {
            runtime().block_on(async move {
                let ch = chimaera_proto::uds::connect(socket.clone())
                    .await
                    .map_err(|e| PyConnectionError::new_err(format!("cannot connect to {socket}: {e}")))?;
                let mut peer = pb::peer_client::PeerClient::new(ch).max_decoding_message_size(256 << 20);
                let info = peer
                    .hello(pb::ClientHello { wire_version: chimaera_proto::WIRE_VERSION, client, ..Default::default() })
                    .await
                    .map_err(rpc_err)?
                    .into_inner();
                let (up, up_rx) = mpsc::unbounded_channel();
                let shared = Arc::new(Shared {
                    events: Mutex::default(),
                    ready: Condvar::new(),
                    closed: AtomicBool::new(false),
                    pending: Mutex::default(),
                    flush_scheduled: AtomicBool::new(false),
                    last_flush: Mutex::new(Instant::now() - Duration::from_secs(1)),
                    seq: AtomicU64::new(0),
                    up,
                });
                let mut down = peer
                    .session(tokio_stream::wrappers::UnboundedReceiverStream::new(up_rx))
                    .await
                    .map_err(rpc_err)?
                    .into_inner();
                let s2 = shared.clone();
                tokio::spawn(async move {
                    loop {
                        match down.message().await {
                            Ok(Some(ev)) => {
                                s2.events.lock().expect("events").push_back(ev);
                                s2.ready.notify_all();
                            }
                            _ => {
                                s2.closed.store(true, Ordering::Release);
                                s2.ready.notify_all();
                                return;
                            }
                        }
                    }
                });
                Ok(Client { peer, shared, info })
            })
        })
    }

    #[getter]
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("peer", PyBytes::new(py, &self.info.peer))?;
        d.set_item("incarnation", self.info.incarnation)?;
        d.set_item("object_dir", &self.info.object_dir)?;
        d.set_item("code_dir", &self.info.code_dir)?;
        d.set_item("inline_limit", self.info.inline_limit)?;
        d.set_item("usable_cores", self.info.usable_cores)?;
        d.set_item("memory_bytes", self.info.memory_bytes)?;
        d.set_item("version", &self.info.version)?;
        d.set_item("gpus", self.info.gpus.clone())?;
        Ok(d)
    }

    /// Create (or, with the same submission key, find) a run. Returns
    /// (run_id, detached).
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tenant, submission_key, detached, python, cwd, sys_path, env, description))]
    fn create_run(
        &self,
        py: Python<'_>,
        tenant: String,
        submission_key: String,
        detached: bool,
        python: String,
        cwd: String,
        sys_path: Vec<String>,
        env: BTreeMap<String, String>,
        description: String,
    ) -> PyResult<(Py<PyBytes>, bool)> {
        let mut peer = self.peer.clone();
        let rec = py.detach(|| {
            runtime().block_on(async move {
                peer.create_run(pb::CreateRunRequest {
                    tenant,
                    submission_key,
                    detached,
                    client_grace_ms: 60_000,
                    description,
                    python,
                    cwd,
                    sys_path,
                    env: env.into_iter().collect(),
                    ..Default::default()
                })
                .await
                .map(|r| r.into_inner())
                .map_err(rpc_err)
            })
        })?;
        Ok((PyBytes::new(py, &rec.run).unbind(), rec.detached))
    }

    /// Bind this session to a run (outcomes, logs and run lifetime).
    fn attach(&self, run: &[u8]) {
        self.shared.send(pb::client_msg::Msg::AttachRun(run.to_vec()));
    }

    /// The driver is exiting normally.
    fn finish_run(&self) {
        self.shared.flush();
        self.shared.send(pb::client_msg::Msg::FinishRun(true));
    }

    fn put_code(&self, py: Python<'_>, digest: Vec<u8>, code: Vec<u8>) -> PyResult<()> {
        let mut peer = self.peer.clone();
        py.detach(|| {
            runtime()
                .block_on(async move { peer.put_code(pb::PutCodeRequest { digest, code }).await })
                .map(|_| ())
                .map_err(rpc_err)
        })
    }

    /// Queue calls for submission. A lone call is sent immediately; bursts are
    /// coalesced for up to 200 µs or 64 calls.
    fn submit(&self, py: Python<'_>, calls: Vec<CallSpec>) -> PyResult<()> {
        let specs: Vec<pb::TaskSpec> = calls.into_iter().map(|c| c.into_spec(py)).collect::<PyResult<_>>()?;
        let len = {
            let mut p = self.shared.pending.lock().expect("pending");
            p.extend(specs);
            p.len()
        };
        let quiet = self.shared.last_flush.lock().expect("last_flush").elapsed() > Duration::from_millis(1);
        if len >= BATCH_MAX || quiet {
            self.shared.flush();
        } else if !self.shared.flush_scheduled.swap(true, Ordering::AcqRel) {
            let s = self.shared.clone();
            runtime().spawn(async move {
                tokio::time::sleep(BATCH_WINDOW).await;
                s.flush();
            });
        }
        Ok(())
    }

    /// Send anything still coalescing.
    fn flush(&self) {
        self.shared.flush();
    }

    fn watch(&self, keys: Vec<Vec<u8>>) -> PyResult<()> {
        let tasks = keys.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?;
        self.shared.send(pb::client_msg::Msg::Watch(pb::WatchMsg { tasks }));
        Ok(())
    }

    fn release(&self, keys: Vec<Vec<u8>>) -> PyResult<()> {
        let tasks = keys.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?;
        self.shared.send(pb::client_msg::Msg::Release(pb::Release { tasks }));
        Ok(())
    }

    fn cancel(&self, key: Vec<u8>, force: bool) -> PyResult<()> {
        self.shared.flush();
        let task = Some(task_key(&key)?);
        self.shared.send(pb::client_msg::Msg::Cancel(pb::CancelMsg { task, force, reason: 0 }));
        Ok(())
    }

    /// Create an actor; returns the seq whose ("actor", ...) event answers it.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (actor, ctor, tenant, key, class_digest, restart, max_restarts, detached))]
    fn create_actor(
        &self,
        py: Python<'_>,
        actor: Vec<u8>,
        ctor: CallSpec,
        tenant: String,
        key: String,
        class_digest: Vec<u8>,
        restart: i32,
        max_restarts: u32,
        detached: bool,
    ) -> PyResult<u64> {
        let ctor = ctor.into_spec(py)?;
        let spec = pb::ActorSpec {
            actor,
            ctor: ctor.key.clone(),
            tenant,
            key,
            class_digest,
            restart,
            durable: false,
            max_restarts,
            policy: ctor.policy.clone(),
            detached,
        };
        self.shared.flush();
        Ok(self
            .shared
            .send(pb::client_msg::Msg::ActorCreate(pb::ActorCreateMsg { spec: Some(spec), ctor: Some(ctor) })))
    }

    /// End an actor: pending calls fail with ActorDied; it is not restarted.
    fn close_actor(&self, actor: Vec<u8>) {
        self.shared.flush();
        self.shared.send(pb::client_msg::Msg::CloseActor(actor));
    }

    /// Wait up to `timeout` seconds for session events and return them as
    /// tuples. Returns None once the session has closed.
    fn next_events<'py>(&self, py: Python<'py>, timeout: f64, max: usize) -> PyResult<Option<Bound<'py, PyList>>> {
        let shared = self.shared.clone();
        let batch: Vec<pb::SessionEvent> = py.detach(|| {
            let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
            let mut q = shared.events.lock().expect("events");
            while q.is_empty() && !shared.closed.load(Ordering::Acquire) {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                q = shared.ready.wait_timeout(q, left).expect("events").0;
            }
            let n = q.len().min(max.max(1));
            q.drain(..n).collect()
        });
        if batch.is_empty() && self.shared.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let out = PyList::empty(py);
        for ev in batch {
            out.append(event_py(py, &ev)?)?;
        }
        Ok(Some(out))
    }

    fn get_task<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let mut peer = self.peer.clone();
        let k = task_key(&key)?;
        let rec = py.detach(|| runtime().block_on(async move { peer.get_task(k).await }));
        match rec {
            Ok(r) => {
                let r = r.into_inner();
                let outcome = r.outcome.as_ref().map(|o| outcome_py(py, o)).transpose()?;
                Ok(Some(PyTuple::new(
                    py,
                    [format!("{:?}", r.state()).into_pyobject(py)?.into_any(), outcome.into_pyobject(py)?.into_any()],
                )?))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(rpc_err(e)),
        }
    }

    /// Have the peer bring an object produced elsewhere into its store.
    fn localize(&self, py: Python<'_>, run: Vec<u8>, object_id: Vec<u8>) -> PyResult<()> {
        let mut peer = self.peer.clone();
        let req = pb::LocalizeRequest { run, object_id };
        py.detach(|| runtime().block_on(async move { peer.localize(req).await }))
            .map(|_| ())
            .map_err(|e| PyFileNotFoundError::new_err(e.message().to_string()))
    }

    /// One page of a run's tasks: ([(key, state, outcome|None)], next_cursor|None).
    fn list_tasks<'py>(
        &self,
        py: Python<'py>,
        run: Vec<u8>,
        after: Vec<u8>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut peer = self.peer.clone();
        let reply = py
            .detach(|| {
                runtime().block_on(async move {
                    peer.list_tasks(pb::ListTasksRequest { run, after_task: after, limit }).await
                })
            })
            .map_err(rpc_err)?
            .into_inner();
        let items = PyList::empty(py);
        for (ev, state) in reply.tasks.iter().zip(reply.states.iter()) {
            let key = ev.task.as_ref().map(key_bytes).unwrap_or_default();
            let st = pb::task_record::State::try_from(*state).map_or("Unknown".to_string(), |s| format!("{s:?}"));
            let outcome = ev.outcome.as_ref().map(|o| outcome_py(py, o)).transpose()?;
            items.append(PyTuple::new(
                py,
                [
                    PyBytes::new(py, &key).into_any(),
                    st.into_pyobject(py)?.into_any(),
                    outcome.into_pyobject(py)?.into_any(),
                ],
            )?)?;
        }
        let next: Option<Bound<'py, PyBytes>> =
            (!reply.next_after.is_empty()).then(|| PyBytes::new(py, &reply.next_after));
        PyTuple::new(py, [items.into_any(), next.into_pyobject(py)?.into_any()])
    }

    /// Explain a task: (state, pending_reason, attempts[...], failure messages).
    fn explain<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyDict>> {
        let mut peer = self.peer.clone();
        let k = task_key(&key)?;
        let reply = py
            .detach(|| {
                runtime().block_on(async move {
                    peer.explain(pb::ExplainRequest {
                        run: k.run.clone(),
                        task: Some(k),
                        decisions: false,
                        pending: true,
                    })
                    .await
                })
            })
            .map_err(rpc_err)?
            .into_inner();
        let d = PyDict::new(py);
        d.set_item("state", reply.task.as_ref().map(|t| format!("{:?}", t.state())))?;
        d.set_item("pending_reason", reply.pending_reason)?;
        let attempts = PyList::empty(py);
        for a in &reply.attempts {
            let ad = PyDict::new(py);
            ad.set_item("attempt", a.attempt)?;
            ad.set_item("end_reason", &a.end_reason)?;
            ad.set_item("detail", &a.detail)?;
            ad.set_item("placement", &a.placement_reason)?;
            let m = PyDict::new(py);
            for (name, meas) in &a.measures {
                m.set_item(name, (meas.value, meas.scope.as_str(), meas.method.as_str()))?;
            }
            ad.set_item("measures", m)?;
            attempts.append(ad)?;
        }
        d.set_item("attempts", attempts)?;
        let failures: Vec<String> =
            reply.task.as_ref().map_or(Vec::new(), |t| t.failure_history.iter().map(|f| f.message.clone()).collect());
        d.set_item("failures", failures)?;
        Ok(d)
    }

    fn close(&self) {
        self.shared.flush();
        self.shared.closed.store(true, Ordering::Release);
        self.shared.ready.notify_all();
    }
}

fn event_py<'py>(py: Python<'py>, ev: &pb::SessionEvent) -> PyResult<Bound<'py, PyTuple>> {
    use pb::session_event::Event as E;
    match &ev.event {
        Some(E::Acks(a)) => {
            let acks = PyList::empty(py);
            for ack in &a.acks {
                let key: Option<Bound<'py, PyBytes>> = ack.task.as_ref().map(|k| PyBytes::new(py, &key_bytes(k)));
                let outcome = ack.outcome.as_ref().map(|o| outcome_py(py, o)).transpose()?;
                acks.append(PyTuple::new(
                    py,
                    [
                        key.into_pyobject(py)?.into_any(),
                        format!("{:?}", ack.status()).into_pyobject(py)?.into_any(),
                        ack.reason.as_str().into_pyobject(py)?.into_any(),
                        outcome.into_pyobject(py)?.into_any(),
                    ],
                )?)?;
            }
            PyTuple::new(
                py,
                ["acks".into_pyobject(py)?.into_any(), ev.seq.into_pyobject(py)?.into_any(), acks.into_any()],
            )
        }
        Some(E::Outcome(o)) => {
            let key = o.task.as_ref().map(key_bytes).unwrap_or_default();
            PyTuple::new(
                py,
                [
                    "outcome".into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &key).into_any(),
                    outcome_py(py, &o.outcome.clone().unwrap_or_default())?.into_any(),
                ],
            )
        }
        Some(E::Actor(a)) => {
            let rec = a.record.clone().unwrap_or_default();
            PyTuple::new(
                py,
                [
                    "actor".into_pyobject(py)?.into_any(),
                    ev.seq.into_pyobject(py)?.into_any(),
                    format!("{:?}", a.status()).into_pyobject(py)?.into_any(),
                    format!("{:?}", rec.state()).into_pyobject(py)?.into_any(),
                    rec.generation.into_pyobject(py)?.into_any(),
                ],
            )
        }
        Some(E::Error(msg)) => PyTuple::new(
            py,
            [
                "error".into_pyobject(py)?.into_any(),
                ev.seq.into_pyobject(py)?.into_any(),
                msg.as_str().into_pyobject(py)?.into_any(),
            ],
        ),
        Some(E::Log(l)) => {
            let key = l.task.as_ref().map(key_bytes).unwrap_or_default();
            PyTuple::new(
                py,
                [
                    "log".into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &key).into_any(),
                    l.stream.as_str().into_pyobject(py)?.into_any(),
                    l.text.as_str().into_pyobject(py)?.into_any(),
                    l.pid.into_pyobject(py)?.into_any(),
                ],
            )
        }
        Some(E::StreamItem(item)) => {
            let key = item.task.as_ref().map(key_bytes).unwrap_or_default();
            PyTuple::new(
                py,
                [
                    "stream_item".into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &key).into_any(),
                    item.attempt.into_pyobject(py)?.into_any(),
                    item.index.into_pyobject(py)?.into_any(),
                    outcome_py(py, &item.value.clone().unwrap_or_default())?.into_any(),
                ],
            )
        }
        None => PyTuple::new(py, ["none"]),
    }
}

// ---- executor connection --------------------------------------------------

/// An executor's connection to its peer. `recv` is called by one reader
/// thread; `send_*` may be called from any thread.
#[pyclass(module = "chimaera._native")]
struct ExecConn {
    reader: Mutex<UnixStream>,
    writer: Mutex<UnixStream>,
    welcome: pb::ExecWelcome,
}

fn write_msg(w: &Mutex<UnixStream>, msg: pb::ExecToPeer) -> PyResult<()> {
    let f = frame::encode(1, &msg.encode_to_vec(), frame::Codec::None, false);
    w.lock().expect("writer").write_all(&f).map_err(|e| PyConnectionError::new_err(e.to_string()))
}

fn read_msg(r: &mut UnixStream) -> std::io::Result<pb::PeerToExec> {
    let mut h = [0u8; frame::HEADER_LEN];
    r.read_exact(&mut h)?;
    let (len, _, flags) = frame::parse_header(&h, MAX_FRAME).map_err(std::io::Error::other)?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    let body = frame::decode_body(flags, &body, MAX_FRAME).map_err(std::io::Error::other)?;
    pb::PeerToExec::decode(body.as_slice()).map_err(std::io::Error::other)
}

#[pymethods]
impl ExecConn {
    #[new]
    fn new(py: Python<'_>, socket: String, executor_id: u32, python: String, abi: String) -> PyResult<ExecConn> {
        py.detach(|| {
            let s = UnixStream::connect(&socket).map_err(|e| PyConnectionError::new_err(format!("{socket}: {e}")))?;
            let writer = Mutex::new(s.try_clone().map_err(|e| PyConnectionError::new_err(e.to_string()))?);
            let mut reader = s;
            write_msg(
                &writer,
                pb::ExecToPeer {
                    msg: Some(pb::exec_to_peer::Msg::Hello(pb::ExecHello {
                        executor_id,
                        pid: std::process::id(),
                        python,
                        abi,
                    })),
                },
            )?;
            let welcome = match read_msg(&mut reader).map_err(|e| PyConnectionError::new_err(e.to_string()))?.msg {
                Some(pb::peer_to_exec::Msg::Welcome(w)) => w,
                _ => return Err(PyConnectionError::new_err("peer did not welcome this executor")),
            };
            Ok(ExecConn { reader: Mutex::new(reader), writer, welcome })
        })
    }

    #[getter]
    fn welcome<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("object_dir", &self.welcome.object_dir)?;
        d.set_item("code_dir", &self.welcome.code_dir)?;
        d.set_item("peer_socket", &self.welcome.peer_socket)?;
        d.set_item("peer", PyBytes::new(py, &self.welcome.peer))?;
        d.set_item("incarnation", self.welcome.incarnation)?;
        d.set_item("inline_limit", self.welcome.inline_limit)?;
        Ok(d)
    }

    /// Next instruction from the peer, or None when the connection closed:
    /// ("execute", dict) | ("cancel", key) | ("shutdown",).
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let msg = py.detach(|| read_msg(&mut self.reader.lock().expect("reader")));
        let Ok(msg) = msg else { return Ok(None) };
        match msg.msg {
            Some(pb::peer_to_exec::Msg::Execute(x)) => {
                let d = PyDict::new(py);
                d.set_item("kind", x.kind)?;
                d.set_item("fn_digest", PyBytes::new(py, &x.fn_digest))?;
                d.set_item("method", &x.method)?;
                d.set_item("actor", PyBytes::new(py, &x.actor))?;
                d.set_item("run", PyBytes::new(py, &x.run))?;
                d.set_item("deadline_unix_ms", x.deadline_unix_ms)?;
                d.set_item("policy", policy_py(py, &x.policy.unwrap_or_default())?)?;
                d.set_item("retry_exception_types", x.retry_exception_types)?;
                let items = PyList::empty(py);
                for it in &x.items {
                    let deps = PyList::empty(py);
                    for dep in &it.deps {
                        deps.append(outcome_py(py, dep)?)?;
                    }
                    let args = object_py(py, &it.args.clone().unwrap_or_default())?;
                    items.append(PyTuple::new(
                        py,
                        [
                            PyBytes::new(py, &it.task.as_ref().map(key_bytes).unwrap_or_default()).into_any(),
                            it.attempt.into_pyobject(py)?.into_any(),
                            args.into_any(),
                            deps.into_any(),
                            it.item_index.into_pyobject(py)?.into_any(),
                        ],
                    )?)?;
                }
                d.set_item("items", items)?;
                Ok(Some(PyTuple::new(py, ["execute".into_pyobject(py)?.into_any(), d.into_any()])?))
            }
            Some(pb::peer_to_exec::Msg::Cancel(c)) => Ok(Some(PyTuple::new(
                py,
                [
                    "cancel".into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &c.task.as_ref().map(key_bytes).unwrap_or_default()).into_any(),
                ],
            )?)),
            Some(pb::peer_to_exec::Msg::Shutdown(_)) | None => Ok(Some(PyTuple::new(py, ["shutdown"])?)),
            Some(pb::peer_to_exec::Msg::Welcome(_)) => Ok(Some(PyTuple::new(py, ["noop"])?)),
            Some(pb::peer_to_exec::Msg::Credit(c)) => Ok(Some(PyTuple::new(
                py,
                [
                    "credit".into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &c.task.as_ref().map(key_bytes).unwrap_or_default()).into_any(),
                    c.up_to.into_pyobject(py)?.into_any(),
                ],
            )?)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn send_done(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        attempt: u32,
        outcome: Bound<'_, PyTuple>,
        retryable: bool,
        fingerprint: Vec<u8>,
        entry_unix_ns: u64,
        exit_unix_ns: u64,
    ) -> PyResult<()> {
        let done = pb::ExecDone {
            task: Some(task_key(&key)?),
            attempt,
            outcome: Some(outcome_from(&outcome)?),
            retryable,
            fingerprint,
            entry_unix_ns,
            exit_unix_ns,
        };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::Done(done)) }))
    }

    fn send_log(&self, py: Python<'_>, key: Vec<u8>, stream: String, text: String) -> PyResult<()> {
        let log = pb::ExecLog { task: Some(task_key(&key)?), stream, text };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::Log(log)) }))
    }

    fn send_blocked(&self, py: Python<'_>, key: Vec<u8>, blocked: bool, waiting_on: Vec<Vec<u8>>) -> PyResult<()> {
        let msg = pb::ExecBlocked {
            task: Some(task_key(&key)?),
            blocked,
            waiting_on: waiting_on.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?,
        };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::Blocked(msg)) }))
    }
}

/// HLC timestamp for an absolute wall-clock deadline (used by tests).
#[pyfunction]
fn hlc_of_ms(ms: u64) -> u64 {
    hlc::pack(ms, 0)
}

#[pyfunction]
fn timeout_error(msg: String) -> PyErr {
    PyTimeoutError::new_err(msg)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<ExecConn>()?;
    m.add_function(wrap_pyfunction!(derive_id, m)?)?;
    m.add_function(wrap_pyfunction!(child_id, m)?)?;
    m.add_function(wrap_pyfunction!(vshard_of, m)?)?;
    m.add_function(wrap_pyfunction!(blake3_digest, m)?)?;
    m.add_function(wrap_pyfunction!(socket_path, m)?)?;
    m.add_function(wrap_pyfunction!(hlc_of_ms, m)?)?;
    m.add_function(wrap_pyfunction!(timeout_error, m)?)?;
    m.add("WIRE_VERSION", chimaera_proto::WIRE_VERSION)?;
    Ok(())
}
