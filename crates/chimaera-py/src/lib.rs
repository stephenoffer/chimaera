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

mod objects;
mod plain;
mod scatter;
mod upload;

const BATCH_MAX: usize = 64;
const BATCH_WINDOW: Duration = Duration::from_micros(200);
/// Submissions sent and not yet acknowledged, at most. The peer commits a
/// session's submissions one after another, so calls arriving meanwhile wait
/// and go together: batches grow with the commit latency (a cloud disk's
/// fsync, a remote group leader) instead of staying at `BATCH_MAX`.
const SUBMITS_IN_FLIGHT: u64 = 2;
/// A batch this large goes even while submissions are in flight.
const BATCH_HARD: usize = 4096;
const MAX_FRAME: usize = 1 << 30;
/// Outcomes and generator items received between acknowledgements, which
/// let the peer forget what a resume would send again.
const ACK_EVERY: u64 = 256;
/// Tries to resume a session whose stream broke, 100 ms apart and doubling
/// (about 3 s; the peer keeps the session 5 s).
const RESUME_TRIES: u32 = 5;
/// How long a try waits for the peer to answer a resume before the next try.
/// The peer answers once the old stream has ended there (it ends a stream it
/// has not seen break after the request it is handling).
const RESUME_ANSWER: Duration = Duration::from_secs(5);

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        if let Some(e) = chimaera_proto::uds::pair_refused() {
            panic!("{e}"); // as building the runtime would, but saying why
        }
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

/// blake3 of any contiguous buffer (bytes, memoryview, NumPy array) without
/// copying it. The GIL is released while hashing, and inputs of 4 MiB or more
/// are hashed on every core.
#[pyfunction]
fn blake3_buffer<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let buf = pyo3::buffer::PyUntypedBuffer::get(obj)?;
    if !buf.is_c_contiguous() {
        return Err(PyValueError::new_err("blake3_buffer needs a contiguous buffer"));
    }
    let (ptr, len) = (buf.buf_ptr() as usize, buf.len_bytes());
    let digest = py.detach(move || {
        // SAFETY: `buf` keeps the buffer exported, so its memory stays valid
        // until it drops after this closure returns. A caller that mutates the
        // buffer from another thread meanwhile gets the digest of some mix of
        // old and new bytes, as with any snapshot taken during a write.
        let data = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
        let mut h = blake3::Hasher::new();
        if len >= 4 << 20 {
            h.update_rayon(data);
        } else {
            h.update(data);
        }
        *h.finalize().as_bytes()
    });
    drop(buf);
    Ok(PyBytes::new(py, &digest))
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
        K::Overloaded => "overloaded",
        K::Budget => "budget",
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
        "budget" => K::Budget,
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

/// Build an Outcome from Python: ("ok", run, object_id, size, inline|None, oob
/// [, [(blob_id, size), ...]]) or ("err", kind, type_name, message, exc_pickle,
/// traceback, detail_json). The blobs are files the frame refers to.
fn outcome_from(t: &Bound<'_, PyTuple>) -> PyResult<pb::Outcome> {
    let tag: String = t.get_item(0)?.extract()?;
    let value = if tag == "ok" {
        let run: Vec<u8> = t.get_item(1)?.extract()?;
        let object_id: Vec<u8> = t.get_item(2)?.extract()?;
        let size: u64 = t.get_item(3)?.extract()?;
        let inline: Option<Vec<u8>> = t.get_item(4)?.extract()?;
        let oob: u32 = t.get_item(5)?.extract()?;
        let blobs: Vec<(Vec<u8>, u64)> = if t.len() > 6 {
            t.get_item(6)?.extract::<Option<Vec<(Vec<u8>, u64)>>>()?.unwrap_or_default()
        } else {
            Vec::new()
        };
        let blobs = blobs
            .into_iter()
            .map(|(object_id, size)| pb::ObjectRef { object_id, size, run: run.clone(), ..Default::default() })
            .collect();
        pb::outcome::Value::Ok(pb::ObjectRef {
            object_id,
            size,
            inline_data: inline.unwrap_or_default(),
            run,
            oob_buffers: oob,
            blobs,
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

/// A peer's demand summary (pb::Demand) as the dict `Node.demand` holds
/// (python/chimaera/_planner.py reads it). A release bound of 0 ("not known") is infinity.
fn demand_dict<'py>(py: Python<'py>, m: &pb::Demand) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    let shapes = m
        .shapes
        .iter()
        .map(|s| {
            let row = PyDict::new(py);
            row.set_item("cpus", s.cpus)?;
            row.set_item("memory", s.memory)?;
            row.set_item("queued", s.queued)?;
            row.set_item("wall_s", s.wall_s)?;
            row.set_item("cold", s.cold)?;
            row.set_item("actors", s.actors)?;
            Ok(row)
        })
        .collect::<PyResult<Vec<_>>>()?;
    d.set_item("shapes", shapes)?;
    let releases: Vec<(f64, f64)> =
        m.releases.iter().map(|r| (if r.upper_s > 0.0 { r.upper_s } else { f64::INFINITY }, r.cores)).collect();
    d.set_item("releases", releases)?;
    d.set_item("started_total", m.started_total)?;
    d.set_item("started_core_s_total", m.started_core_s_total)?;
    d.set_item("oldest_ready_s", m.oldest_ready_s)?;
    let excluded = PyDict::new(py);
    excluded.set_item("labels", m.excluded_labels)?;
    excluded.set_item("devices", m.excluded_devices)?;
    d.set_item("excluded", excluded)?;
    Ok(d)
}

/// Policy from a Python dict of option values.
fn policy_from(d: &Bound<'_, PyDict>) -> PyResult<pb::Policy> {
    let get_f = |k: &str| -> PyResult<f64> { d.get_item(k)?.map_or(Ok(0.0), |v| v.extract()) };
    let get_u = |k: &str| -> PyResult<u64> { d.get_item(k)?.map_or(Ok(0), |v| v.extract()) };
    // A count too wide for its 32-bit field is refused, not wrapped.
    let get_u32 = |k: &str| -> PyResult<u32> {
        let v = get_u(k)?;
        u32::try_from(v)
            .map_err(|_| PyValueError::new_err(format!("{k}: {v} is more than {}, the most there is", u32::MAX)))
    };
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
    let durability = match get_s("durability")?.as_str() {
        "ephemeral" => pb::Durability::Ephemeral,
        "replayable" => pb::Durability::Replayable,
        "retained" => pb::Durability::Retained,
        _ => pb::Durability::Default,
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
        concurrency: get_u32("concurrency")?,
        range_size: get_u32("range_size")?,
        interactive: get_b("interactive")?,
        deterministic: get_b("deterministic")?,
        env_digest: get_s("env_digest")?,
        require: require.into_iter().collect(),
        prefer,
        source: get_s("source")?,
        fn_name: get_s("fn_name")?,
        cpus_initial: get_b("cpus_initial")?,
        memory_initial: get_b("memory_initial")?,
        cpus_min: get_f("cpus_min")?,
        cpus_max: get_f("cpus_max")?,
        memory_min: get_u("memory_min")?,
        memory_max: get_u("memory_max")?,
        priority: d.get_item("priority")?.map_or(Ok(0), |v| v.extract())?,
        max_in_flight: d.get_item("max_in_flight")?.map_or(Ok(0), |v| v.extract())?,
        max_in_flight_bytes: get_u("max_in_flight_bytes")?,
        profile_digest: d.get_item("profile_digest")?.map_or(Ok(Vec::new()), |v| v.extract())?,
        items: d.get_item("items")?.map_or(Ok(0), |v| v.extract())?,
        budget_cpu_seconds: get_f("budget_cpu_seconds")?,
        budget_gpu_seconds: get_f("budget_gpu_seconds")?,
        budget_root: d
            .get_item("budget_root")?
            .map(|v| v.extract::<Vec<u8>>())
            .transpose()?
            .filter(|k| k.len() == 32)
            .map(|k| pb::TaskKey { run: k[..16].to_vec(), task: k[16..].to_vec() }),
        durability: durability as i32,
        explained: d.get_item("explained")?.map_or(Ok(Vec::new()), |v| v.extract())?,
        // (owner, generation, slot) of a hold on the peer's ledger: a gang rank or a bundle's host.
        launch: d
            .get_item("launch")?
            .map(|v| v.extract::<(String, u64, u32)>())
            .transpose()?
            .map(|(owner, generation, slot)| pb::LaunchToken { owner, generation, slot }),
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
    d.set_item("max_in_flight", p.max_in_flight)?;
    d.set_item("max_in_flight_bytes", p.max_in_flight_bytes)?;
    d.set_item("budget_cpu_seconds", p.budget_cpu_seconds)?;
    d.set_item("budget_gpu_seconds", p.budget_gpu_seconds)?;
    let root = p.budget_root.as_ref().map_or_else(Vec::new, |k| [k.run.as_slice(), k.task.as_slice()].concat());
    d.set_item("budget_root", PyBytes::new(py, &root))?;
    let durability = match p.durability() {
        pb::Durability::Default => "",
        pb::Durability::Ephemeral => "ephemeral",
        pb::Durability::Replayable => "replayable",
        pb::Durability::Retained => "retained",
    };
    d.set_item("durability", durability)?;
    if !p.explained.is_empty() {
        d.set_item("explained", &p.explained)?;
    }
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
    /// (blob id, size) of the content-addressed values the frame reads.
    #[pyo3(item, default)]
    blobs: Vec<(Vec<u8>, u64)>,
    /// Durable actor calls: idempotency key and argument fingerprint.
    #[pyo3(item, default)]
    command_id: Vec<u8>,
    #[pyo3(item, default)]
    command_fingerprint: Vec<u8>,
    /// A generator call: items stream back as they are yielded.
    #[pyo3(item, default)]
    stream: bool,
    /// An actor call: the key of the previous call on the same handle.
    #[pyo3(item, default)]
    prev: Option<Vec<u8>>,
    /// A call the direct path already ran: (outcome as an executor reports
    /// it, entry ns, exit ns, executor CPU ns, executor id). Recorded as done.
    #[pyo3(item, default)]
    ran: Option<(Py<PyTuple>, u64, u64, u64, u32)>,
    /// Sent straight to its actor's executor: the peer notes it in flight.
    #[pyo3(item, default)]
    direct: bool,
    /// A durable workflow's call, described (JSON) for a replay that diverges.
    #[pyo3(item, default)]
    workflow_step: String,
}

impl CallSpec {
    fn into_spec(self, py: Python<'_>) -> PyResult<pb::TaskSpec> {
        let key = task_key(&self.key)?;
        let policy = policy_from(self.policy.bind(py))?;
        let blobs = self
            .blobs
            .into_iter()
            .map(|(object_id, size)| pb::ObjectRef { object_id, size, run: key.run.clone(), ..Default::default() })
            .collect();
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
            blobs,
            command_id: self.command_id,
            command_fingerprint: self.command_fingerprint,
            stream: self.stream,
            prev: self.prev.map(|p| task_key(&p)).transpose()?,
            ran: self
                .ran
                .map(|(outcome, entry_unix_ns, exit_unix_ns, cpu_ns, executor)| {
                    Ok::<_, PyErr>(pb::Ran {
                        outcome: Some(outcome_from(outcome.bind(py))?),
                        entry_unix_ns,
                        exit_unix_ns,
                        cpu_ns,
                        executor,
                    })
                })
                .transpose()?,
            direct: self.direct,
            workflow_step: self.workflow_step,
        })
    }
}

// ---- client ---------------------------------------------------------------

use chimaera_proto::client::{Auth, PeerClient, channel};

struct Shared {
    events: Mutex<VecDeque<pb::SessionEvent>>,
    ready: Condvar,
    closed: AtomicBool,
    pending: Mutex<Vec<pb::TaskSpec>>,
    flush_scheduled: AtomicBool,
    last_flush: Mutex<Instant>,
    /// Submit messages sent and not yet acknowledged.
    submits: AtomicU64,
    seq: AtomicU64,
    up: Mutex<Up>,
    /// The peer's id for this session (0: it cannot be resumed), the last
    /// cursor received and the last acknowledged, and the last request the
    /// peer reported handled.
    session: AtomicU64,
    cursor: AtomicU64,
    acked_cursor: AtomicU64,
    taken: AtomicU64,
}

/// The stream to the peer (a closed one while a resume is under way), and
/// the requests sent that the peer has not reported handled
/// (SessionEvent.taken), which a resume sends again. None for a peer that
/// cannot resume sessions.
struct Up {
    tx: mpsc::UnboundedSender<pb::ClientMsg>,
    kept: Option<VecDeque<pb::ClientMsg>>,
}

impl Shared {
    fn send(&self, msg: pb::client_msg::Msg) -> u64 {
        let mut up = self.up.lock().expect("up");
        // Numbered under the lock, so requests go, and are kept, in seq order.
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let msg = pb::ClientMsg { seq, msg: Some(msg) };
        if let Some(kept) = &mut up.kept {
            let taken = self.taken.load(Ordering::Acquire);
            while kept.front().is_some_and(|m| m.seq <= taken) {
                kept.pop_front();
            }
            // A resume carries the cursor itself.
            if !matches!(msg.msg, Some(pb::client_msg::Msg::AckCursor(_))) {
                kept.push_back(msg.clone());
            }
        }
        let _ = up.tx.send(msg);
        seq
    }

    /// End the stream to the peer (the reader then resumes the session).
    fn break_stream(&self) {
        self.up.lock().expect("up").tx = mpsc::unbounded_channel().0;
    }

    /// An event from the peer.
    fn receive(&self, ev: pb::SessionEvent) {
        use pb::session_event::Event as E;
        if let Some(E::Opened(id)) = ev.event {
            self.session.store(id, Ordering::Release);
            return;
        }
        if self.session.load(Ordering::Acquire) == 0 {
            // The first event was not the session's id: a peer that cannot
            // resume sessions, so nothing is kept for one.
            self.up.lock().expect("up").kept = None;
        }
        if ev.taken != 0 {
            self.taken.fetch_max(ev.taken, Ordering::AcqRel);
        }
        if ev.cursor != 0 {
            self.cursor.store(ev.cursor, Ordering::Release);
            if ev.cursor >= self.acked_cursor.load(Ordering::Acquire) + ACK_EVERY {
                self.acked_cursor.store(ev.cursor, Ordering::Release);
                self.send(pb::client_msg::Msg::AckCursor(ev.cursor));
            }
        }
        let acks = matches!(ev.event, Some(E::Acks(_)));
        self.events.lock().expect("events").push_back(ev);
        self.ready.notify_all();
        if acks {
            self.acked();
        }
    }

    /// The stream ended: continue the session on a new one, from the last
    /// cursor received, unless the client was closed or the peer cannot.
    /// Nothing else goes on the new stream until the peer has taken the
    /// session over; then the requests the old stream did not deliver go
    /// again (their replies, and those of the ones it did, come once).
    async fn resume(&self, peer: &PeerClient) -> Option<tonic::Streaming<pb::SessionEvent>> {
        self.break_stream(); // what is sent from now on waits in `kept`
        let mut wait = Duration::from_millis(100);
        for _ in 0..RESUME_TRIES {
            let session = self.session.load(Ordering::Acquire);
            if session == 0 || self.closed.load(Ordering::Acquire) {
                return None;
            }
            let (up, up_rx) = mpsc::unbounded_channel();
            let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
            let cursor = self.cursor.load(Ordering::Acquire);
            let resume = pb::client_msg::Msg::Resume(pb::ResumeSession { session, cursor });
            let _ = up.send(pb::ClientMsg { seq, msg: Some(resume) });
            let attempt = async {
                let ups = tokio_stream::wrappers::UnboundedReceiverStream::new(up_rx);
                let mut down = peer.clone().session(ups).await?.into_inner();
                Ok::<_, tonic::Status>(resumed(&mut down).await?.map(|answer| (answer, down)))
            };
            match tokio::time::timeout(RESUME_ANSWER, attempt).await {
                Ok(Ok(Some(((id, taken), down)))) => {
                    self.session.store(id, Ordering::Release);
                    self.taken.fetch_max(taken, Ordering::AcqRel);
                    let mut cur = self.up.lock().expect("up");
                    if let Some(kept) = &mut cur.kept {
                        kept.retain(|m| m.seq > taken);
                        for msg in kept.iter() {
                            let _ = up.send(msg.clone());
                        }
                    }
                    cur.tx = up;
                    return Some(down);
                }
                Ok(Ok(None)) => return None, // refused
                // This stream broke too, or no answer came in time. The peer
                // may have taken the session over all the same: it knows the
                // session by the old id too.
                Ok(Err(_)) | Err(_) => {}
            }
            tokio::time::sleep(wait).await;
            wait *= 2;
        }
        None
    }

    /// Send everything pending now (before the caller waits on results).
    fn flush(&self) {
        let tasks = std::mem::take(&mut *self.pending.lock().expect("pending"));
        self.flush_scheduled.store(false, Ordering::Release);
        *self.last_flush.lock().expect("last_flush") = Instant::now();
        if !tasks.is_empty() {
            self.submits.fetch_add(1, Ordering::AcqRel);
            self.send(pb::client_msg::Msg::Submit(pb::SubmitTasks { tasks, grant_to: None }));
        }
    }

    /// Send pending calls unless enough submissions are already in flight;
    /// then the next acknowledgement sends them, with whatever came since.
    fn flush_if_open(&self) {
        if self.submits.load(Ordering::Acquire) < SUBMITS_IN_FLIGHT {
            self.flush();
        } else {
            self.flush_scheduled.store(false, Ordering::Release);
        }
    }

    fn acked(&self) {
        let _ = self.submits.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| Some(n.saturating_sub(1)));
        if !self.pending.lock().expect("pending").is_empty() {
            self.flush_if_open();
        }
    }
}

/// Read a resumed stream up to the peer's answer: the session's new id and
/// the last request the old stream delivered, or None if the peer refused.
async fn resumed(down: &mut tonic::Streaming<pb::SessionEvent>) -> Result<Option<(u64, u64)>, tonic::Status> {
    use pb::session_event::Event as E;
    let mut id = 0;
    while let Some(ev) = down.message().await? {
        match ev.event {
            Some(E::Opened(n)) => id = n,
            Some(E::Resumed(taken)) => return Ok(Some((id, taken))),
            Some(E::Error(_)) => return Ok(None),
            _ => {}
        }
    }
    Err(tonic::Status::unavailable("the stream ended before the peer answered the resume"))
}

/// Connection to a peer.
#[pyclass(module = "chimaera._native")]
struct Client {
    peer: PeerClient,
    shared: Arc<Shared>,
    info: pb::PeerInfo,
    /// Who store requests are for (`set_tenant`); the peer checks it against the credential.
    tenant: Mutex<String>,
}

impl Client {
    fn store_call<'py>(&self, py: Python<'py>, req: pb::StoreRequest) -> PyResult<Bound<'py, PyDict>> {
        let mut peer = self.peer.clone();
        let r = py.detach(|| runtime().block_on(async move { peer.store(req).await })).map_err(rpc_err)?.into_inner();
        let entry = |py: Python<'py>, e: &pb::StoreEntry| -> PyResult<Bound<'py, PyDict>> {
            let d = PyDict::new(py);
            d.set_item("key", &e.key)?;
            d.set_item("value", PyBytes::new(py, &e.value))?;
            d.set_item("version", e.version)?;
            d.set_item("created", e.created)?;
            d.set_item("is_counter", e.is_counter)?;
            d.set_item("counter", e.counter)?;
            Ok(d)
        };
        let d = PyDict::new(py);
        d.set_item("ok", r.ok)?;
        d.set_item("error", &r.error)?;
        d.set_item("revision", r.revision)?;
        d.set_item("entry", r.entry.as_ref().map(|e| entry(py, e)).transpose()?)?;
        d.set_item("entries", r.entries.iter().map(|e| entry(py, e)).collect::<PyResult<Vec<_>>>()?)?;
        let lease = r.lease.as_ref().map(|l| (l.holder.clone(), l.token, l.expires_hlc));
        d.set_item("lease", lease)?;
        let barrier = r.barrier.as_ref().map(|b| (b.parties, b.arrived, b.generation));
        d.set_item("barrier", barrier)?;
        let changes = r
            .changes
            .iter()
            .map(|c| {
                let x = PyDict::new(py);
                x.set_item("revision", c.revision)?;
                x.set_item("key", &c.key)?;
                x.set_item("deleted", c.deleted)?;
                x.set_item("value", PyBytes::new(py, &c.value))?;
                x.set_item("is_counter", c.is_counter)?;
                x.set_item("counter", c.counter)?;
                Ok(x)
            })
            .collect::<PyResult<Vec<_>>>()?;
        d.set_item("changes", changes)?;
        d.set_item("compacted", r.compacted)?;
        d.set_item("more", r.more)?;
        d.set_item("examined", r.examined)?;
        d.set_item("retry_ms", r.retry_ms)?;
        Ok(d)
    }
}

/// One write of a store transaction: (op, key, value, delta, fence).
type TxnOp = (String, String, Option<Vec<u8>>, i64, Option<(String, u64)>);

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
    /// Connect to a peer: a Unix socket path (the local peer), or
    /// `tcp://host:port` with the cluster `token` and, for TLS, the cluster's `ca` file.
    #[new]
    #[pyo3(signature = (socket, client, token=None, ca=None))]
    fn new(
        py: Python<'_>,
        socket: String,
        client: String,
        token: Option<String>,
        ca: Option<String>,
    ) -> PyResult<Client> {
        let auth = Auth::bearer(token.as_deref()).map_err(PyValueError::new_err)?;
        py.detach(|| {
            runtime().block_on(async move {
                let ch = channel(&socket, ca.as_deref())
                    .await
                    .map_err(|e| PyConnectionError::new_err(format!("cannot connect to {socket}: {e}")))?;
                let mut peer =
                    pb::peer_client::PeerClient::with_interceptor(ch, auth).max_decoding_message_size(256 << 20);
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
                    submits: AtomicU64::new(0),
                    seq: AtomicU64::new(0),
                    up: Mutex::new(Up { tx: up, kept: Some(VecDeque::new()) }),
                    session: AtomicU64::new(0),
                    cursor: AtomicU64::new(0),
                    acked_cursor: AtomicU64::new(0),
                    taken: AtomicU64::new(0),
                });
                // First: the peer keeps what a resume of this session needs.
                shared.send(pb::client_msg::Msg::AckCursor(0));
                let mut down = peer
                    .session(tokio_stream::wrappers::UnboundedReceiverStream::new(up_rx))
                    .await
                    .map_err(rpc_err)?
                    .into_inner();
                let (s2, p2) = (shared.clone(), peer.clone());
                tokio::spawn(async move {
                    loop {
                        match down.message().await {
                            Ok(Some(ev)) => s2.receive(ev),
                            _ => match s2.resume(&p2).await {
                                Some(d) => down = d,
                                None => break,
                            },
                        }
                    }
                    s2.session.store(0, Ordering::Release);
                    s2.closed.store(true, Ordering::Release);
                    s2.ready.notify_all();
                });
                Ok(Client { peer, shared, info, tenant: Mutex::new("default".into()) })
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
        d.set_item("direct_actor_calls", self.info.direct_actor_calls)?;
        d.set_item("replicas", self.info.replicas)?;
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
    ) -> PyResult<(Py<PyBytes>, bool, Vec<u32>)> {
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
        Ok((PyBytes::new(py, &rec.run).unbind(), rec.detached, rec.spread))
    }

    /// Bind this session to a run (outcomes, logs and run lifetime).
    fn attach(&self, run: &[u8]) {
        self.shared.send(pb::client_msg::Msg::AttachRun(run.to_vec()));
    }

    /// The tenant store requests are for: the run's.
    fn set_tenant(&self, tenant: String) {
        *self.tenant.lock().expect("lock") = tenant;
    }

    /// Finish (or cancel) any run by id: its unfinished calls are cancelled.
    fn end_run(&self, run: Vec<u8>, cancel: bool) {
        self.shared.flush();
        self.shared.send(pb::client_msg::Msg::EndRun(pb::EndRun { run, cancel }));
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
    /// coalesced for up to 200 µs or 64 calls, and for as long as
    /// `SUBMITS_IN_FLIGHT` submissions await their acknowledgement.
    fn submit(&self, py: Python<'_>, calls: Vec<CallSpec>) -> PyResult<()> {
        let specs: Vec<pb::TaskSpec> = calls.into_iter().map(|c| c.into_spec(py)).collect::<PyResult<_>>()?;
        let len = {
            let mut p = self.shared.pending.lock().expect("pending");
            p.extend(specs);
            p.len()
        };
        let quiet = self.shared.last_flush.lock().expect("last_flush").elapsed() > Duration::from_millis(1);
        if len >= BATCH_HARD {
            self.shared.flush();
        } else if len >= BATCH_MAX || quiet {
            self.shared.flush_if_open();
        } else if !self.shared.flush_scheduled.swap(true, Ordering::AcqRel) {
            let s = self.shared.clone();
            runtime().spawn(async move {
                tokio::time::sleep(BATCH_WINDOW).await;
                s.flush_if_open();
            });
        }
        Ok(())
    }

    /// Send anything still coalescing.
    fn flush(&self) {
        self.shared.flush();
    }

    /// End this session's stream as a broken connection would: the client
    /// resumes the session on a new stream, where the peer sends again what
    /// the client had not received.
    fn reconnect(&self) {
        self.shared.break_stream();
    }

    /// The last cursor received: outcomes and generator items count up from
    /// 1, and what a resume sends again gets new ones.
    #[getter]
    fn cursor(&self) -> u64 {
        self.shared.cursor.load(Ordering::Acquire)
    }

    fn watch(&self, keys: Vec<Vec<u8>>) -> PyResult<()> {
        let tasks = keys.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?;
        self.shared.send(pb::client_msg::Msg::Watch(pb::WatchMsg { tasks }));
        Ok(())
    }

    /// Free ch.put values on every peer: their futures were dropped and the
    /// calls that read them have finished.
    fn release_objects(&self, run: Vec<u8>, object_ids: Vec<Vec<u8>>) -> PyResult<()> {
        self.shared.send(pb::client_msg::Msg::ReleaseObjects(pb::ReleaseObjects { run, object_ids }));
        Ok(())
    }

    fn release(&self, keys: Vec<Vec<u8>>) -> PyResult<()> {
        let tasks = keys.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?;
        self.shared.send(pb::client_msg::Msg::Release(pb::Release { tasks }));
        Ok(())
    }

    /// Tell the peer the consumer of a generator call took items below `up_to`.
    fn stream_credit(&self, key: Vec<u8>, up_to: u64) -> PyResult<()> {
        let task = Some(task_key(&key)?);
        self.shared.send(pb::client_msg::Msg::StreamCredit(pb::StreamCredit { task, up_to }));
        Ok(())
    }

    fn cancel(&self, key: Vec<u8>, force: bool) -> PyResult<()> {
        self.shared.flush();
        let task = Some(task_key(&key)?);
        self.shared.send(pb::client_msg::Msg::Cancel(pb::CancelMsg { task, force, reason: 0 }));
        Ok(())
    }

    /// Create an actor; returns the seq whose ("actor", ...) event answers it.
    /// `methods`: per-method limits, (method, max_calls, max_bytes, reject,
    /// max_wait_ms) each; `state_schema`: a durable class's state version.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (actor, ctor, tenant, key, class_digest, restart, max_restarts, detached, durable=false, nonce=Vec::new(), state_schema=0, methods=Vec::new()))]
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
        durable: bool,
        nonce: Vec<u8>,
        state_schema: u32,
        methods: Vec<(String, u32, u64, bool, u64)>,
    ) -> PyResult<u64> {
        let ctor = ctor.into_spec(py)?;
        let methods = methods
            .into_iter()
            .map(|(method, max_calls, max_bytes, reject, max_wait_ms)| pb::MethodLimit {
                method,
                max_calls,
                max_bytes,
                reject,
                max_wait_ms,
            })
            .collect();
        let spec = pb::ActorSpec {
            nonce,
            actor,
            ctor: ctor.key.clone(),
            tenant,
            key,
            class_digest,
            restart,
            durable,
            max_restarts,
            policy: ctor.policy.clone(),
            detached,
            state_schema,
            methods,
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

    /// Where `key`'s value is stored: (peer ids in hex, its size in bytes,
    /// whether it is held inline). A call's record names the peers its result
    /// was published at (its holder, and a retained value's copies); a result
    /// held inline has none. A `ch.put` has no record: it is this peer's
    /// while its file is in this peer's store. None for a call not finished
    /// or failed, and for a put whose file is not here.
    fn object_locations(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<Option<(Vec<String>, u64, bool)>> {
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        let mut peer = self.peer.clone();
        let k = task_key(&key)?;
        let rec = py.detach(|| runtime().block_on(async move { peer.get_task(k).await }));
        match rec {
            Ok(r) => match r.into_inner().outcome.and_then(|o| o.value) {
                Some(pb::outcome::Value::Ok(obj)) if !obj.inline_data.is_empty() && obj.blobs.is_empty() => {
                    Ok(Some((vec![], obj.size, true)))
                }
                Some(pb::outcome::Value::Ok(obj)) => {
                    // A frame that names files (a result's large buffers, on Linux
                    // linked from the arena) is stored where those files are.
                    let mut peers: Vec<String> = Vec::new();
                    for l in obj.locations.iter().chain(obj.blobs.iter().flat_map(|b| b.locations.iter())) {
                        let p = hex(&l.peer);
                        if !peers.contains(&p) {
                            peers.push(p);
                        }
                    }
                    Ok(Some((peers, obj.size + obj.blobs.iter().map(|b| b.size).sum::<u64>(), false)))
                }
                _ => Ok(None),
            },
            Err(e) if e.code() == tonic::Code::NotFound => {
                let (run, object) = key.split_at(16); // a put's key: its run and object id
                let path = std::path::Path::new(&self.info.object_dir).join(hex(run)).join(hex(object));
                Ok(std::fs::metadata(path).ok().map(|m| (vec![hex(&self.info.peer)], m.len(), false)))
            }
            Err(e) => Err(rpc_err(e)),
        }
    }

    /// The cluster as the connected peer sees it: a list of node dicts.
    fn nodes<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let mut peer = self.peer.clone();
        let r = py
            .detach(|| runtime().block_on(async move { peer.status(pb::StatusRequest::default()).await }))
            .map_err(rpc_err)?
            .into_inner();
        r.nodes
            .iter()
            .map(|n| {
                let d = PyDict::new(py);
                d.set_item("peer", n.peer.iter().map(|b| format!("{b:02x}")).collect::<String>())?;
                d.set_item("address", &n.addr)?;
                d.set_item("cpus", n.cpus)?;
                d.set_item("memory_bytes", n.memory_bytes)?;
                d.set_item("gpus", n.gpus.clone())?;
                d.set_item("alive", n.alive)?;
                d.set_item("cpu_used", n.cpu_used)?;
                d.set_item("queued", n.queued)?;
                d.set_item("this_peer", n.this_peer)?;
                d.set_item("labels", n.labels.clone())?;
                d.set_item("actors", n.actors)?;
                d.set_item("object_bytes", n.object_bytes)?;
                d.set_item("draining", n.draining)?;
                d.set_item("object_layout", n.object_layout)?;
                d.set_item("demand", n.demand.as_ref().map(|m| demand_dict(py, m)).transpose()?)?;
                d.set_item("load_age_s", n.load_age_ms as f64 / 1e3)?;
                d.set_item("starting", n.starting)?;
                d.set_item("incarnation", n.incarnation)?;
                Ok(d)
            })
            .collect()
    }

    /// An actor's record: (state, constructor task key, class digest,
    /// durable), or None if unknown.
    fn get_actor<'py>(&self, py: Python<'py>, actor: Vec<u8>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let mut peer = self.peer.clone();
        let rec = py.detach(|| runtime().block_on(async move { peer.get_actor(pb::ActorQuery { actor }).await }));
        match rec {
            Ok(r) => {
                let r = r.into_inner();
                let spec = r.spec.clone().unwrap_or_default();
                let ctor = spec.ctor.map(|k| [k.run, k.task].concat()).unwrap_or_default();
                Ok(Some(PyTuple::new(
                    py,
                    [
                        format!("{:?}", r.state()).into_pyobject(py)?.into_any(),
                        PyBytes::new(py, &ctor).into_any(),
                        PyBytes::new(py, &spec.class_digest).into_any(),
                        spec.durable.into_pyobject(py)?.to_owned().into_any(),
                    ],
                )?))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(rpc_err(e)),
        }
    }

    /// Live named actors whose name starts with `prefix`, that this client
    /// may see: (tenant, name, state, actor id) each.
    fn list_actors<'py>(&self, py: Python<'py>, prefix: String) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let mut peer = self.peer.clone();
        let r = py
            .detach(|| runtime().block_on(async move { peer.list_actors(pb::ListActorsRequest { prefix }).await }))
            .map_err(rpc_err)?
            .into_inner();
        r.actors
            .iter()
            .map(|a| {
                let spec = a.spec.clone().unwrap_or_default();
                PyTuple::new(
                    py,
                    [
                        spec.tenant.into_pyobject(py)?.into_any(),
                        spec.key.into_pyobject(py)?.into_any(),
                        format!("{:?}", a.state()).into_pyobject(py)?.into_any(),
                        PyBytes::new(py, &spec.actor).into_any(),
                    ],
                )
            })
            .collect()
    }

    /// A ch.Store write in `namespace`; returns the reply as a dict.
    #[pyo3(signature = (namespace, op, key, value=None, if_version=None, ttl_ms=0, delta=0, holder=String::new(), token=0, parties=0, fence=None))]
    #[allow(clippy::too_many_arguments)]
    fn store_write<'py>(
        &self,
        py: Python<'py>,
        namespace: String,
        op: &str,
        key: String,
        value: Option<Vec<u8>>,
        if_version: Option<u64>,
        ttl_ms: u64,
        delta: i64,
        holder: String,
        token: u64,
        parties: u32,
        fence: Option<(String, u64)>,
    ) -> PyResult<Bound<'py, PyDict>> {
        use pb::store_write::Op;
        let op = match op {
            "put" => Op::Put,
            "delete" => Op::Delete,
            "add" => Op::Add,
            "lock" => Op::Lock,
            "renew" => Op::Renew,
            "unlock" => Op::Unlock,
            "arrive" => Op::Arrive,
            other => return Err(PyValueError::new_err(format!("unknown store op {other:?}"))),
        };
        let write = pb::StoreWrite {
            tenant: self.tenant.lock().expect("lock").clone(),
            namespace,
            op: op as i32,
            key,
            value: value.unwrap_or_default(),
            has_if_version: if_version.is_some(),
            if_version: if_version.unwrap_or(0),
            ttl_ms,
            delta,
            holder,
            token,
            parties,
            fence: fence.map(|(lock, token)| pb::StoreFence { lock, token }),
        };
        self.store_call(py, pb::StoreRequest { write: Some(write), ..Default::default() })
    }

    /// A ch.Store transaction in `namespace`: `ops` are (op, key, value,
    /// delta, fence) with op put, delete or add. They apply together once
    /// every (key, version) of `checks` holds (0: absent) and each op could
    /// apply alone, or none of them does.
    fn store_txn<'py>(
        &self,
        py: Python<'py>,
        namespace: String,
        checks: Vec<(String, u64)>,
        ops: Vec<TxnOp>,
    ) -> PyResult<Bound<'py, PyDict>> {
        use pb::store_write::Op;
        let ops = ops
            .into_iter()
            .map(|(op, key, value, delta, fence)| {
                let op = match op.as_str() {
                    "put" => Op::Put,
                    "delete" => Op::Delete,
                    "add" => Op::Add,
                    other => {
                        return Err(PyValueError::new_err(format!(
                            "a store transaction puts, deletes and adds, not {other:?}"
                        )));
                    }
                };
                Ok(pb::StoreWrite {
                    op: op as i32,
                    key,
                    value: value.unwrap_or_default(),
                    delta,
                    fence: fence.map(|(lock, token)| pb::StoreFence { lock, token }),
                    ..Default::default()
                })
            })
            .collect::<PyResult<_>>()?;
        let txn = pb::StoreTxn {
            tenant: self.tenant.lock().expect("lock").clone(),
            namespace,
            checks: checks.into_iter().map(|(key, version)| pb::StoreCheck { key, version }).collect(),
            ops,
        };
        self.store_call(py, pb::StoreRequest { txn: Some(txn), ..Default::default() })
    }

    /// A ch.Store claim in `namespace`: the lease `lock_prefix` + key for
    /// `holder` on the first key under `prefix` nobody holds.
    fn store_claim<'py>(
        &self,
        py: Python<'py>,
        namespace: String,
        prefix: String,
        lock_prefix: String,
        holder: String,
        ttl_ms: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let claim = pb::StoreClaim {
            tenant: self.tenant.lock().expect("lock").clone(),
            namespace,
            prefix,
            lock_prefix,
            holder,
            ttl_ms,
        };
        self.store_call(py, pb::StoreRequest { claim: Some(claim), ..Default::default() })
    }

    /// A ch.Store read (`kind`: get, scan, changes, lease, barrier); `wait_ms`
    /// makes a changes read wait for a change.
    #[pyo3(signature = (namespace, kind, key, after_revision=0, start_after=String::new(), limit=0, wait_ms=0, keys_only=false))]
    #[allow(clippy::too_many_arguments)]
    fn store_read<'py>(
        &self,
        py: Python<'py>,
        namespace: String,
        kind: &str,
        key: String,
        after_revision: u64,
        start_after: String,
        limit: u32,
        wait_ms: u32,
        keys_only: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        use pb::store_read::Kind;
        let kind = match kind {
            "get" => Kind::Get,
            "scan" => Kind::Scan,
            "changes" => Kind::Changes,
            "lease" => Kind::Lease,
            "barrier" => Kind::Barrier,
            other => return Err(PyValueError::new_err(format!("unknown store read {other:?}"))),
        };
        let read = pb::StoreRead {
            tenant: self.tenant.lock().expect("lock").clone(),
            namespace,
            kind: kind as i32,
            key,
            after_revision,
            start_after,
            limit,
            keys_only,
        };
        self.store_call(py, pb::StoreRequest { read: Some(read), wait_ms, ..Default::default() })
    }

    /// Ids among `blob_ids` the peer does not store yet (for a remote client's uploads).
    fn has_blobs(&self, py: Python<'_>, run: Vec<u8>, blob_ids: Vec<Vec<u8>>) -> PyResult<Vec<Vec<u8>>> {
        let mut peer = self.peer.clone();
        let req = pb::HasBlobsRequest { run, blob_ids };
        py.detach(|| runtime().block_on(async move { peer.has_blobs(req).await }))
            .map(|r| r.into_inner().missing)
            .map_err(rpc_err)
    }

    /// Upload a content-addressed blob (`blob_id` = blake3 of `data`, 16 bytes)
    /// from any contiguous buffer (bytes, a memoryview, an array), copying a
    /// few 1 MiB chunks at a time rather than the whole payload (upload.rs).
    /// `keep_local`: the peer makes no background copy of it.
    #[pyo3(signature = (run, blob_id, data, keep_local=false))]
    fn put_blob(
        &self,
        py: Python<'_>,
        run: Vec<u8>,
        blob_id: Vec<u8>,
        data: &Bound<'_, PyAny>,
        keep_local: bool,
    ) -> PyResult<()> {
        let buf = pyo3::buffer::PyUntypedBuffer::get(data)?;
        if !buf.is_c_contiguous() {
            return Err(PyValueError::new_err("put_blob needs a contiguous buffer"));
        }
        let (ptr, len) = (buf.buf_ptr() as usize, buf.len_bytes());
        let mut peer = self.peer.clone();
        let head = pb::PutBlobChunk { run, blob_id, keep_local, ..Default::default() };
        let sent = py.detach(move || {
            // SAFETY: `buf` keeps the buffer exported, so its memory stays
            // valid until it drops after this closure returns, and
            // upload::put_blob reads it only while it runs. A caller that
            // mutates the buffer from another thread meanwhile sends some mix
            // of old and new bytes, which the peer refuses: they no longer
            // match the blob's id.
            let data: &[u8] = if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(ptr as *const u8, len) } };
            runtime().block_on(upload::put_blob(&mut peer, head, data))
        });
        drop(buf);
        sent.map_err(rpc_err)
    }

    /// Copy an object from the connected peer (which fetches it first if another
    /// peer holds it, or runs `task` again if every copy is lost) into the file
    /// `path`, checking each chunk's checksum.
    #[pyo3(signature = (run, object_id, path, task=None))]
    fn fetch_object(
        &self,
        py: Python<'_>,
        run: Vec<u8>,
        object_id: Vec<u8>,
        path: String,
        task: Option<Vec<u8>>,
    ) -> PyResult<()> {
        let mut peer = self.peer.clone();
        let task = task.map(|t| task_key(&t)).transpose()?;
        py.detach(|| {
            runtime().block_on(async move {
                let missing = |e: tonic::Status| PyFileNotFoundError::new_err(e.message().to_string());
                peer.localize(pb::LocalizeRequest {
                    run: run.clone(),
                    object_id: object_id.clone(),
                    task,
                    ..Default::default()
                })
                .await
                .map_err(missing)?;
                let req = pb::GetObjectRequest { run, object_id, offset: 0, len: 0 };
                let mut stream = peer.get_object(req).await.map_err(missing)?.into_inner();
                let tmp = format!("{path}.part-{}", std::process::id());
                if let Some(dir) = std::path::Path::new(&path).parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let mut file = std::fs::File::create(&tmp)?;
                while let Some(chunk) = stream.message().await.map_err(rpc_err)? {
                    if blake3::hash(&chunk.data).as_bytes()[..16] != chunk.checksum[..] {
                        let _ = std::fs::remove_file(&tmp);
                        return Err(PyRuntimeError::new_err("an object chunk arrived corrupted (checksum mismatch)"));
                    }
                    file.write_all(&chunk.data)?;
                }
                file.sync_all()?;
                std::fs::rename(&tmp, &path)?;
                Ok(())
            })
        })
    }

    /// Events this peer saw with seq >= `since` (of `kinds`, or all), waiting
    /// up to `wait_ms` for one: (events as dicts, next since, peer, incarnation).
    #[pyo3(signature = (kinds, since, wait_ms=0))]
    fn events<'py>(
        &self,
        py: Python<'py>,
        kinds: Vec<String>,
        since: u64,
        wait_ms: u32,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut peer = self.peer.clone();
        let req = pb::EventsRequest { kinds, since, wait_ms };
        let r = py.detach(|| runtime().block_on(async move { peer.events(req).await })).map_err(rpc_err)?.into_inner();
        let out = PyList::empty(py);
        for e in r.events {
            let d = PyDict::new(py);
            d.set_item("seq", e.seq)?;
            d.set_item("at_unix_ms", e.at_unix_ms)?;
            d.set_item("kind", e.kind)?;
            d.set_item("node", PyBytes::new(py, &e.node))?;
            d.set_item("run", PyBytes::new(py, &e.run))?;
            d.set_item("task", e.task.as_ref().map(|k| PyBytes::new(py, &key_bytes(k))))?;
            d.set_item("actor", PyBytes::new(py, &e.actor))?;
            d.set_item("message", e.message)?;
            d.set_item("detail_json", e.detail_json)?;
            out.append(d)?;
        }
        PyTuple::new(
            py,
            [
                out.into_any(),
                r.next.into_pyobject(py)?.into_any(),
                PyBytes::new(py, &r.peer).into_any(),
                r.incarnation.into_pyobject(py)?.into_any(),
            ],
        )
    }

    /// A run's output from the calls this peer ran, from byte `offset`,
    /// waiting up to `wait_ms` for more: (bytes, next offset, size so far).
    #[pyo3(signature = (run, offset, wait_ms=0))]
    fn logs<'py>(&self, py: Python<'py>, run: Vec<u8>, offset: u64, wait_ms: u32) -> PyResult<Bound<'py, PyTuple>> {
        let mut peer = self.peer.clone();
        let req = pb::LogsRequest { run, offset, wait_ms };
        let r = py.detach(|| runtime().block_on(async move { peer.logs(req).await })).map_err(rpc_err)?.into_inner();
        PyTuple::new(
            py,
            [
                PyBytes::new(py, &r.text).into_any(),
                r.next.into_pyobject(py)?.into_any(),
                r.size.into_pyobject(py)?.into_any(),
            ],
        )
    }

    /// Have the peer bring an object produced elsewhere into its store.
    /// `traffic`: why it is read ("shuffle" for a gather's parts; default: a
    /// call reads it now), the class its holder paces the bytes as.
    #[pyo3(signature = (run, object_id, task=None, traffic=""))]
    fn localize(
        &self,
        py: Python<'_>,
        run: Vec<u8>,
        object_id: Vec<u8>,
        task: Option<Vec<u8>>,
        traffic: &str,
    ) -> PyResult<()> {
        let mut peer = self.peer.clone();
        let task = task.map(|t| task_key(&t)).transpose()?;
        let traffic = match traffic {
            "" => pb::TrafficClass::TrafficForeground,
            name => pb::TrafficClass::from_str_name(&format!("TRAFFIC_{}", name.to_ascii_uppercase()))
                .ok_or_else(|| PyValueError::new_err(format!("no traffic class {name:?}")))?,
        };
        let req = pb::LocalizeRequest { run, object_id, task, traffic: traffic as i32 };
        py.detach(|| runtime().block_on(async move { peer.localize(req).await }))
            .map(|_| ())
            .map_err(|e| PyFileNotFoundError::new_err(e.message().to_string()))
    }

    /// Have the peer copy objects it stores to other peers and return once
    /// the copies are made (a retained ch.put). ValueError: no other peer
    /// can hold one; RuntimeError: a copy failed.
    fn retain(&self, py: Python<'_>, run: Vec<u8>, object_ids: Vec<Vec<u8>>) -> PyResult<()> {
        let mut peer = self.peer.clone();
        let req = pb::RetainRequest { run, object_ids };
        py.detach(|| runtime().block_on(async move { peer.retain(req).await })).map(|_| ()).map_err(rpc_err)
    }

    /// Hold slots of resources on `peer`'s ledger (empty: this peer's) for
    /// `owner`'s `generation`, every one or none: `slots` are (cpus, gpus,
    /// device); none releases the owner's hold, and `renew` only extends a
    /// hold it has. Returns (held, why not, the peer's holds, one line each,
    /// and whether that peer could never hold the slots, even idle).
    #[pyo3(signature = (peer, owner, generation, slots, ttl_ms, renew=false))]
    #[allow(clippy::too_many_arguments)]
    fn hold(
        &self,
        py: Python<'_>,
        peer: Vec<u8>,
        owner: String,
        generation: u64,
        slots: Vec<(f64, f64, String)>,
        ttl_ms: u64,
        renew: bool,
    ) -> PyResult<(bool, String, Vec<String>, bool)> {
        let mut client = self.peer.clone();
        let slots = slots.into_iter().map(|(cpus, gpus, device)| pb::HoldShape { cpus, gpus, device }).collect();
        let req = pb::HoldRequest { peer, owner, generation, slots, ttl_ms, renew, ..Default::default() };
        let r = py.detach(|| runtime().block_on(async move { client.hold(req).await })).map_err(rpc_err)?.into_inner();
        Ok((r.held, r.reason, r.holds, r.never))
    }

    /// Hold stored files of another run for this session, past that run's
    /// end, until each hold is released (`release=True`, which returns at
    /// once) or the session ends. A hold waits for its lease.
    /// FileNotFoundError: the run ended, and the value with it.
    #[pyo3(signature = (run, object_ids, release=false))]
    fn borrow(&self, py: Python<'_>, run: Vec<u8>, object_ids: Vec<Vec<u8>>, release: bool) -> PyResult<()> {
        let (shared, mut peer) = (self.shared.clone(), self.peer.clone());
        let req = move |session| pb::BorrowRequest { session, run, object_ids, release };
        if release {
            let req = req(shared.session.load(Ordering::Acquire));
            runtime().spawn(async move { peer.borrow(req).await });
            return Ok(());
        }
        let held = py.detach(|| {
            runtime().block_on(async move {
                // The peer names the session first thing on its stream.
                let deadline = Instant::now() + Duration::from_secs(10);
                while shared.session.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                peer.borrow(req(shared.session.load(Ordering::Acquire))).await
            })
        });
        held.map(|_| ()).map_err(|e| match e.code() {
            tonic::Code::NotFound => PyFileNotFoundError::new_err(e.message().to_string()),
            _ => rpc_err(e),
        })
    }

    /// A run's dead letters (Run.failed()): [(key, fn_name, error outcome, spec bytes)].
    fn failed<'py>(&self, py: Python<'py>, run: Vec<u8>) -> PyResult<Bound<'py, PyList>> {
        let mut peer = self.peer.clone();
        let reply = py
            .detach(|| {
                runtime().block_on(async move {
                    peer.list_tasks(pb::ListTasksRequest { run, failed: true, ..Default::default() }).await
                })
            })
            .map_err(rpc_err)?
            .into_inner();
        let out = PyList::empty(py);
        for (key, d) in reply.failed_keys.iter().zip(&reply.failed) {
            let spec = d.spec.clone().unwrap_or_default();
            let name = pb::Policy::decode(spec.policy.as_slice()).map(|p| p.fn_name).unwrap_or_default();
            let error = outcome_py(py, &d.error.clone().unwrap_or_default())?;
            out.append((PyBytes::new(py, &key_bytes(key)), name, error, PyBytes::new(py, &spec.encode_to_vec())))?;
        }
        Ok(out)
    }

    /// Submit a dead letter's call again, as a new call `key` (Run.retry_failed).
    fn resubmit(&self, spec: Vec<u8>, key: Vec<u8>) -> PyResult<()> {
        let mut s = pb::TaskSpec::decode(spec.as_slice()).map_err(|e| PyValueError::new_err(e.to_string()))?;
        s.key = Some(task_key(&key)?);
        s.parent = None; // the call that made it may have ended, even been cancelled
        s.ran = None;
        s.direct = false;
        self.shared.pending.lock().expect("pending").push(s);
        self.shared.flush();
        Ok(())
    }

    /// Forget dead letters; resubmissions queued before this go first.
    fn discard_failed(&self, keys: Vec<Vec<u8>>) -> PyResult<()> {
        let tasks = keys.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?;
        self.shared.flush();
        self.shared.send(pb::client_msg::Msg::DiscardFailed(pb::Release { tasks }));
        Ok(())
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
                    peer.list_tasks(pb::ListTasksRequest { run, after_task: after, limit, failed: false }).await
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
            ad.set_item("reservation", &a.reservation)?;
            // Each stage's wall-clock time (ns since the epoch), None where the
            // peer did not record it. Stamps from different hosts carry their skew.
            for (k, v) in [
                ("submitted_unix_ns", a.submitted_unix_ns),
                ("ready_unix_ns", a.ready_unix_ns),
                ("queued_unix_ns", a.queued_unix_ns),
                ("dispatched_unix_ns", a.dispatched_unix_ns),
                ("py_entry_unix_ns", a.py_entry_unix_ns),
                ("py_exit_unix_ns", a.py_exit_unix_ns),
                ("published_unix_ns", a.published_unix_ns),
            ] {
                ad.set_item(k, (v != 0).then_some(v))?;
            }
            let settings = PyDict::new(py);
            for s in &a.settings {
                let one = PyDict::new(py);
                one.set_item("value", s.value)?;
                one.set_item("requested", s.requested)?;
                one.set_item("min", s.min)?;
                one.set_item("max", s.max)?;
                one.set_item("source", &s.source)?;
                one.set_item("reason", &s.reason)?;
                settings.set_item(&s.name, one)?;
            }
            ad.set_item("settings", settings)?;
            if let Some(g) = &a.granted {
                ad.set_item("granted", policy_py(py, g)?)?;
            }
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
            // (.., status, state, generation, constructor task key, durable):
            // an actor found by name was built by its record's constructor.
            let rec = a.record.clone().unwrap_or_default();
            let spec = rec.spec.clone().unwrap_or_default();
            let ctor = spec.ctor.map(|k| [k.run, k.task].concat()).unwrap_or_default();
            PyTuple::new(
                py,
                [
                    "actor".into_pyobject(py)?.into_any(),
                    ev.seq.into_pyobject(py)?.into_any(),
                    format!("{:?}", a.status()).into_pyobject(py)?.into_any(),
                    format!("{:?}", rec.state()).into_pyobject(py)?.into_any(),
                    rec.generation.into_pyobject(py)?.into_any(),
                    PyBytes::new(py, &ctor).into_any(),
                    spec.durable.into_pyobject(py)?.to_owned().into_any(),
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
        // `opened` and `resumed` stay in Shared::receive and resumed()
        Some(E::Opened(_) | E::Resumed(_)) | None => PyTuple::new(py, ["none"]),
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
                d.set_item("durable", x.durable)?;
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
            Some(pb::peer_to_exec::Msg::Restore(r)) => {
                let d = PyDict::new(py);
                d.set_item("actor", PyBytes::new(py, &r.actor))?;
                d.set_item("fn_digest", PyBytes::new(py, &r.fn_digest))?;
                d.set_item("state", PyBytes::new(py, &r.state))?;
                d.set_item("version", r.version)?;
                Ok(Some(PyTuple::new(py, ["restore".into_pyobject(py)?.into_any(), d.into_any()])?))
            }
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
    #[pyo3(signature = (key, attempt, outcome, retryable, fingerprint, entry_unix_ns, exit_unix_ns, durable_state=Vec::new(), helper_cpu_ns=0))]
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
        durable_state: Vec<u8>,
        helper_cpu_ns: u64,
    ) -> PyResult<()> {
        // ("forward", key): the call returned that call's future.
        let tag: String = outcome.get_item(0)?.extract()?;
        let (outcome, forward_to) = if tag == "forward" {
            let to: Vec<u8> = outcome.get_item(1)?.extract()?;
            (None, Some(task_key(&to)?))
        } else {
            (Some(outcome_from(&outcome)?), None)
        };
        let done = pb::ExecDone {
            task: Some(task_key(&key)?),
            attempt,
            outcome,
            retryable,
            fingerprint,
            entry_unix_ns,
            exit_unix_ns,
            forward_to,
            durable_state,
            helper_cpu_ns,
        };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::Done(done)) }))
    }

    /// One item of a generator call (an "ok" outcome tuple, as for send_done).
    fn send_yield(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        attempt: u32,
        index: u64,
        value: Bound<'_, PyTuple>,
    ) -> PyResult<()> {
        let item = pb::ExecYield { task: Some(task_key(&key)?), attempt, index, value: Some(outcome_from(&value)?) };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::YieldItem(item)) }))
    }

    fn send_log(&self, py: Python<'_>, key: Vec<u8>, stream: String, text: String) -> PyResult<()> {
        let log = pb::ExecLog { task: Some(task_key(&key)?), stream, text };
        py.detach(|| write_msg(&self.writer, pb::ExecToPeer { msg: Some(pb::exec_to_peer::Msg::Log(log)) }))
    }

    #[pyo3(signature = (key, blocked, waiting_on, host=false))]
    fn send_blocked(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        blocked: bool,
        waiting_on: Vec<Vec<u8>>,
        host: bool,
    ) -> PyResult<()> {
        let msg = pb::ExecBlocked {
            task: Some(task_key(&key)?),
            blocked,
            waiting_on: waiting_on.iter().map(|k| task_key(k)).collect::<PyResult<_>>()?,
            host,
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
    m.add_function(wrap_pyfunction!(blake3_buffer, m)?)?;
    m.add_function(wrap_pyfunction!(socket_path, m)?)?;
    m.add_function(wrap_pyfunction!(hlc_of_ms, m)?)?;
    m.add_function(wrap_pyfunction!(timeout_error, m)?)?;
    objects::register(m)?;
    plain::register(m)?;
    scatter::register(m)?;
    m.add("WIRE_VERSION", chimaera_proto::WIRE_VERSION)?;
    // What the peer binary prints with --version; a wheel's two halves must agree.
    m.add("VERSION", env!("CARGO_PKG_VERSION"))?;
    m.add("PEER_LEVEL", chimaera_proto::PEER_LEVEL)?;
    m.add("BUILD_ID", chimaera_proto::BUILD_ID)?;
    Ok(())
}
