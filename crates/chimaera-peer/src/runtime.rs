//! Starting a peer: storage, identity, the raft group, executors, the core and
//! the client service.

use crate::core::{Core, CoreMsg};
use crate::executor::Supervisor;
use crate::ingress::Ingress;
use crate::router::Router;
use crate::stores::{CodeStore, Identity, ObjectStore};
use chimaera_core::ids::VSHARDS;
use chimaera_core::pb;
use chimaera_raft::{BasicNode, DiskDurability, NoTransport, RaftHost};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

pub const CLIENT_SOCKET: &str = "peer.sock";
pub const EXEC_SOCKET: &str = "exec.sock";

#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub dir: PathBuf,
    /// Override the CPU capacity (defaults to the discovered allocation).
    pub cpus: Option<f64>,
    /// Exit after this long with no sessions and no work (local peers).
    pub idle_timeout: Option<Duration>,
}

/// Default data directory of the local peer: `$CHIMAERA_HOME` or `~/.chimaera/local`.
pub fn default_local_dir() -> PathBuf {
    if let Ok(h) = std::env::var("CHIMAERA_HOME") {
        return PathBuf::from(h);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    Path::new(&home).join(".chimaera").join("local")
}

/// Hold an exclusive lock on the data directory for the process lifetime.
fn lock_dir(dir: &Path) -> io::Result<std::fs::File> {
    std::fs::create_dir_all(dir)?;
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("peer.lock"))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(io::Error::new(io::ErrorKind::AddrInUse, format!("another peer is running in {}", dir.display())))
        }
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

/// What every peer opens first: its directory lock, identity, shape, object
/// store and the core's channel (which the mesh server needs before the core runs).
pub struct Node {
    pub dir: PathBuf,
    pub identity: Identity,
    pub shape: chimaera_core::topology::HostShape,
    pub capacity: f64,
    pub objects: ObjectStore,
    pub code: CodeStore,
    pub core_tx: mpsc::Sender<CoreMsg>,
    pub core_rx: mpsc::Receiver<CoreMsg>,
}

impl Node {
    /// Open the peer under `dir`. The returned file holds the directory lock
    /// for as long as it lives.
    pub fn open(dir: &Path, cpus: Option<f64>) -> io::Result<(Node, std::fs::File)> {
        let lock = lock_dir(dir)?;
        let identity = Identity::load(dir)?;
        let shape = crate::topology::discover();
        let capacity = cpus.unwrap_or_else(|| shape.usable_cores());
        let objects = ObjectStore::open(object_dir(dir, &identity))?;
        let code = CodeStore::open(dir.join("code"))?;
        let (core_tx, core_rx) = mpsc::channel::<CoreMsg>(65536);
        Ok((Node { dir: dir.to_path_buf(), identity, shape, capacity, objects, code, core_tx, core_rx }, lock))
    }
}

/// Run a local peer until shutdown (signal, Shutdown RPC or idle timeout).
pub async fn run_local(cfg: PeerConfig) -> io::Result<()> {
    let (node, _lock) = Node::open(&cfg.dir, cfg.cpus)?;
    let identity = node.identity;
    let (host, raft_events) =
        RaftHost::open(&cfg.dir.join("raft"), identity.node_id(), Arc::new(NoTransport), DiskDurability::Process)?;
    let members: BTreeMap<_, _> = [(identity.node_id(), BasicNode::new("local"))].into();
    let group = host.start_group(0, members, true).await?;
    wait_leader(&group).await?;
    let router = Arc::new(Router::local(identity, group.clone()));
    let all: Vec<u32> = (0..VSHARDS as u32).collect();
    router
        .propose(0, pb::shard_cmd::Body::AdoptVshard(pb::AdoptVShard { vshards: all, epoch: 1 }))
        .await
        .map_err(io::Error::other)?;
    // Retire the previous incarnation: anything it held is fenced and requeued.
    if identity.incarnation > 1 {
        let prev = pb::PeerInc { peer: identity.peer.to_vec(), inc: identity.incarnation - 1 };
        router
            .propose(0, pb::shard_cmd::Body::Retire(pb::RetireIncarnation { peer: Some(prev) }))
            .await
            .map_err(io::Error::other)?;
    }
    serve(node, router, raft_events, cfg.idle_timeout).await
}

/// Run the executors, the core and the client service of a peer whose groups
/// are up, until shutdown (signal, Shutdown RPC or idle timeout).
pub async fn serve(
    node: Node,
    router: Arc<Router>,
    mut raft_events: mpsc::Receiver<chimaera_raft::GroupEvents>,
    idle_timeout: Option<Duration>,
) -> io::Result<()> {
    let Node { dir, identity, shape, capacity, objects, code, core_tx, core_rx } = node;
    let client_socket = chimaera_proto::uds::socket_path(&dir, CLIENT_SOCKET);
    let exec_socket = chimaera_proto::uds::socket_path(&dir, EXEC_SOCKET);
    chimaera_proto::uds::prepare(&client_socket)?;
    let welcome = pb::ExecWelcome {
        object_dir: objects.dir().display().to_string(),
        code_dir: code.dir().display().to_string(),
        peer_socket: client_socket.display().to_string(),
        peer: identity.peer.to_vec(),
        incarnation: identity.incarnation,
        inline_limit: 64 * 1024,
    };
    let (exec_tx, mut exec_rx) = mpsc::channel(4096);
    let sup = Supervisor::start(exec_socket, dir.join("logs"), welcome.clone(), exec_tx)?;

    let isolation = if shape.cgroup_delegated {
        match crate::topology::own_cgroup_dir().map(|own| crate::accounting::Isolation::setup(&own)) {
            Some(Ok(iso)) => Some(iso),
            Some(Err(e)) => {
                tracing::warn!("cgroup v2 delegation is visible but setup failed ({e}); isolated accounting is off");
                None
            }
            None => None,
        }
    } else {
        None
    };
    let isolated_available = isolation.is_some();
    let core = Core::new(
        core_tx.clone(),
        router.clone(),
        sup,
        objects.clone(),
        code.clone(),
        capacity,
        client_socket.display().to_string(),
        isolation,
    );
    tokio::spawn(core.run(core_rx));
    let c2 = core_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = raft_events.recv().await {
            if c2.send(CoreMsg::Raft(ev)).await.is_err() {
                return;
            }
        }
    });
    let c3 = core_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = exec_rx.recv().await {
            if c3.send(CoreMsg::Exec(Box::new(ev))).await.is_err() {
                return;
            }
        }
    });

    let info = pb::PeerInfo {
        wire_version: chimaera_proto::WIRE_VERSION,
        peer: identity.peer.to_vec(),
        incarnation: identity.incarnation,
        object_dir: welcome.object_dir.clone(),
        code_dir: welcome.code_dir.clone(),
        inline_limit: welcome.inline_limit,
        usable_cores: capacity,
        memory_bytes: shape.memory_bytes,
        version: env!("CARGO_PKG_VERSION").into(),
        gpus: shape.gpus.iter().map(|g| g.uuid.clone()).collect(),
    };
    let shutdown = Arc::new(Notify::new());
    let ingress = Arc::new(Ingress::new(
        core_tx.clone(),
        router.clone(),
        code,
        objects.clone(),
        info,
        shutdown.clone(),
        isolated_available,
    ));
    let _ = std::fs::remove_file(&client_socket);
    let listener = tokio::net::UnixListener::bind(&client_socket)?;
    crate::executor::restrict_to_owner(&client_socket)?;
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    let server = tonic::transport::Server::builder()
        .add_service(
            pb::peer_server::PeerServer::new(crate::ingress::Service(ingress)).max_decoding_message_size(256 << 20),
        )
        .serve_with_incoming_shutdown(incoming, wait_for_stop(shutdown.clone(), core_tx, idle_timeout));
    tracing::info!(peer = %identity.peer.hex(), incarnation = identity.incarnation, capacity, "peer ready");
    std::fs::write(dir.join("peer.pid"), std::process::id().to_string())?;
    server.await.map_err(io::Error::other)?;
    for g in router.hosted_groups() {
        g.shutdown().await;
    }
    let _ = std::fs::remove_file(&client_socket);
    Ok(())
}

/// Smallest /dev/shm worth using for objects; containers often mount 64 MiB.
const MIN_SHM_BYTES: u64 = 2 << 30;

/// Objects live in shared memory on Linux when it has room, else on disk.
fn object_dir(dir: &Path, id: &Identity) -> PathBuf {
    let shm = Path::new("/dev/shm");
    if cfg!(target_os = "linux") && shm.is_dir() && free_bytes(shm).is_some_and(|b| b >= MIN_SHM_BYTES) {
        return shm.join(format!("chimaera-{}", &id.peer.hex()[..16]));
    }
    dir.join("objects")
}

fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs writes one struct into `st`; `c` is NUL-terminated.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut st) != 0 {
            return None;
        }
        // Field widths differ by platform (u32 vs u64), hence the casts.
        #[allow(clippy::unnecessary_cast)]
        Some(st.f_bavail as u64 * st.f_frsize as u64)
    }
}

async fn wait_leader(group: &chimaera_raft::Group) -> io::Result<()> {
    for _ in 0..400 {
        if group.is_leader().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(io::Error::other("local group did not elect itself leader"))
}

/// How long SIGTERM waits for queued and running work (`CHIMAERA_DRAIN_SECONDS`).
fn drain_grace() -> Duration {
    let secs = std::env::var("CHIMAERA_DRAIN_SECONDS").ok().and_then(|v| v.parse().ok()).unwrap_or(30);
    Duration::from_secs(secs)
}

/// SIGTERM (what Kubernetes, Slurm and systemd send): take no new work, let
/// queued and running attempts finish and their results commit within the
/// grace period, then stop. Attempts still running at the deadline are
/// retried elsewhere.
async fn drain(core: &mpsc::Sender<CoreMsg>) {
    let grace = drain_grace();
    tracing::info!("SIGTERM: draining for up to {grace:?}");
    if core.send(CoreMsg::Drain).await.is_err() {
        return;
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        let (reply, rx) = tokio::sync::oneshot::channel();
        if core.send(CoreMsg::Status { reply }).await.is_err() {
            return;
        }
        let Ok(s) = rx.await else { return };
        if s.queued == 0 && s.running == 0 && s.proposals_in_flight == 0 {
            tracing::info!("drained");
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tracing::warn!("drain grace period ended with work still running");
}

async fn wait_for_stop(shutdown: Arc<Notify>, core: mpsc::Sender<CoreMsg>, idle: Option<Duration>) {
    let notified = shutdown.notified();
    tokio::pin!(notified);
    let mut idle_since = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("cannot watch SIGTERM ({e}); only Ctrl-C and `chimaera stop` stop this peer");
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2()).expect("SIGUSR2 handler")
        }
    };
    loop {
        tokio::select! {
            _ = &mut notified => return,
            _ = tokio::signal::ctrl_c() => return,
            _ = term.recv() => {
                drain(&core).await;
                return;
            }
            _ = tick.tick() => {
                let Some(limit) = idle else { continue };
                let (reply, rx) = tokio::sync::oneshot::channel();
                if core.send(CoreMsg::Status { reply }).await.is_err() {
                    return;
                }
                let Ok(s) = rx.await else { return };
                if s.sessions > 0 || s.queued > 0 || s.running > 0 {
                    idle_since = Instant::now();
                } else if idle_since.elapsed() > limit {
                    tracing::info!("idle for {limit:?}; stopping");
                    return;
                }
            }
        }
    }
}
