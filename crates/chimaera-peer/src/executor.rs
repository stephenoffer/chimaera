//! Supervises Python executor processes.
//!
//! An executor is `python -m chimaera._executor` started with the run's own
//! interpreter, working directory and import path. It connects back over a
//! Unix socket, says hello, and then runs one attempt at a time. The user's
//! main script is never re-executed: callables arrive as code blobs.
//!
//! Each executor leads its own process group, so killing it also kills any
//! processes the task spawned.

use chimaera_core::pb;
use chimaera_proto::frame;
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

const MAX_FRAME: usize = 1 << 30;

/// How to start executors for a run.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecEnv {
    pub python: String,
    pub cwd: String,
    pub sys_path: Vec<String>,
    pub env: BTreeMap<String, String>,
}

impl ExecEnv {
    pub fn from_spec(spec: chimaera_core::pb::ExecEnvSpec) -> ExecEnv {
        ExecEnv { python: spec.python, cwd: spec.cwd, sys_path: spec.sys_path, env: spec.env.into_iter().collect() }
    }

    pub fn to_spec(&self) -> chimaera_core::pb::ExecEnvSpec {
        chimaera_core::pb::ExecEnvSpec {
            python: self.python.clone(),
            cwd: self.cwd.clone(),
            sys_path: self.sys_path.clone(),
            env: self.env.clone().into_iter().collect(),
        }
    }

    /// Executors are shared only between calls with the same environment and
    /// thread cap (threads are fixed at spawn: library pools size themselves once).
    pub fn key(&self, threads: u32) -> String {
        let mut h = blake3::Hasher::new();
        for part in [&self.python, &self.cwd] {
            h.update(part.as_bytes());
            h.update(&[0]);
        }
        for p in &self.sys_path {
            h.update(p.as_bytes());
            h.update(&[1]);
        }
        for (k, v) in &self.env {
            h.update(k.as_bytes());
            h.update(&[2]);
            h.update(v.as_bytes());
            h.update(&[3]);
        }
        h.update(&threads.to_le_bytes());
        h.finalize().to_hex()[..16].to_string()
    }
}

#[derive(Debug)]
pub enum ExecEvent {
    Ready {
        exec: u32,
    },
    Done {
        exec: u32,
        done: Box<pb::ExecDone>,
    },
    Blocked {
        exec: u32,
        msg: pb::ExecBlocked,
    },
    Log {
        exec: u32,
        log: pb::ExecLog,
    },
    /// An item a generator call yielded.
    Yield {
        exec: u32,
        item: pb::ExecYield,
    },
    Exited {
        exec: u32,
        detail: String,
        signal: Option<i32>,
    },
}

pub struct Supervisor {
    socket: PathBuf,
    log_dir: PathBuf,
    welcome: pb::ExecWelcome,
    events: mpsc::Sender<ExecEvent>,
    conns: Mutex<HashMap<u32, mpsc::UnboundedSender<pb::PeerToExec>>>,
    pids: Mutex<HashMap<u32, u32>>,
    next_id: AtomicU32,
}

impl Supervisor {
    pub fn start(
        socket: PathBuf,
        log_dir: PathBuf,
        welcome: pb::ExecWelcome,
        events: mpsc::Sender<ExecEvent>,
    ) -> io::Result<Arc<Supervisor>> {
        let _ = std::fs::remove_file(&socket);
        std::fs::create_dir_all(&log_dir)?;
        let listener = UnixListener::bind(&socket)?;
        restrict_to_owner(&socket)?;
        let sup = Arc::new(Supervisor {
            socket,
            log_dir,
            welcome,
            events,
            conns: Mutex::default(),
            pids: Mutex::default(),
            next_id: AtomicU32::new(1),
        });
        let s2 = sup.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let s3 = s2.clone();
                tokio::spawn(async move {
                    if let Err(e) = s3.serve(stream).await {
                        tracing::debug!("executor connection ended: {e}");
                    }
                });
            }
        });
        Ok(sup)
    }

    /// Start an executor; it reports `Ready` once connected. With `cgroup`, the
    /// child joins that cgroup before it executes Python.
    pub fn spawn(
        &self,
        env: &ExecEnv,
        threads: u32,
        extra_env: &[(&str, Option<String>)],
        cgroup: Option<&Path>,
    ) -> io::Result<(u32, u32)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let log = std::fs::File::create(self.log_dir.join(format!("executor-{id}.log")))?;
        // The client's interpreter and directory may not exist on this host
        // (another machine): fall back to the peer's own.
        let python = if Path::new(&env.python).is_file() {
            env.python.clone()
        } else {
            std::env::var("CHIMAERA_PYTHON").unwrap_or_else(|_| "python3".into())
        };
        let cwd = if !env.cwd.is_empty() && Path::new(&env.cwd).is_dir() {
            PathBuf::from(&env.cwd)
        } else {
            std::env::temp_dir()
        };
        let mut cmd = tokio::process::Command::new(&python);
        cmd.arg("-m")
            .arg("chimaera._executor")
            .arg("--socket")
            .arg(&self.socket)
            .arg("--id")
            .arg(id.to_string())
            .current_dir(&cwd)
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .envs(&env.env)
            .env("PYTHONPATH", env.sys_path.join(":"))
            .env("PYTHONUNBUFFERED", "1")
            .env("CHIMAERA_EXECUTOR", "1");
        let t = threads.max(1).to_string();
        for var in [
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "RAYON_NUM_THREADS",
            // Pools that ignore OMP_NUM_THREADS.
            "POLARS_MAX_THREADS",
            "NUMBA_NUM_THREADS",
        ] {
            if !env.env.contains_key(var) {
                cmd.env(var, &t);
            }
        }
        let mut defaults = vec![];
        if threads <= 1 {
            // Hugging Face tokenizers otherwise start a pool per process.
            defaults.push(("TOKENIZERS_PARALLELISM", "false"));
        }
        if cfg!(target_os = "linux") {
            // glibc keeps up to 8 arenas per core, holding freed memory in
            // long-lived workers; a few arenas keep it returnable.
            defaults.push(("MALLOC_ARENA_MAX", "4"));
        }
        for (k, v) in defaults {
            if !env.env.contains_key(k) {
                cmd.env(k, v);
            }
        }
        // None removes the variable (a ROCm grant unsets CUDA_VISIBLE_DEVICES).
        for (k, v) in extra_env {
            match v {
                Some(v) => cmd.env(k, v),
                None => cmd.env_remove(k),
            };
        }
        no_core_dumps(&mut cmd);
        if let Some(group) = cgroup {
            join_cgroup_before_exec(&mut cmd, group)?;
        }
        let mut child = cmd.spawn()?;
        let pid = child.id().unwrap_or(0);
        self.pids.lock().expect("pids").insert(id, pid);
        let events = self.events.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            let (detail, signal) = match status {
                Ok(s) => {
                    use std::os::unix::process::ExitStatusExt;
                    (s.to_string(), s.signal())
                }
                Err(e) => (e.to_string(), None),
            };
            let _ = events.send(ExecEvent::Exited { exec: id, detail, signal }).await;
        });
        Ok((id, pid))
    }

    pub fn send(&self, exec: u32, msg: pb::PeerToExec) -> bool {
        self.conns.lock().expect("conns").get(&exec).is_some_and(|tx| tx.send(msg).is_ok())
    }

    /// Kill an executor and everything it started.
    pub fn kill(&self, exec: u32) {
        if let Some(pid) = self.pids.lock().expect("pids").get(&exec).copied()
            && pid > 0
        {
            // SAFETY: killpg only sends a signal; the group id is our child's.
            unsafe {
                libc::killpg(pid as i32, libc::SIGKILL);
            }
        }
    }

    pub fn pid(&self, exec: u32) -> Option<u32> {
        self.pids.lock().expect("pids").get(&exec).copied()
    }

    pub fn forget(&self, exec: u32) {
        self.conns.lock().expect("conns").remove(&exec);
        self.pids.lock().expect("pids").remove(&exec);
    }

    async fn serve(self: Arc<Self>, stream: UnixStream) -> io::Result<()> {
        check_same_user(&stream)?;
        let (mut rd, mut wr) = stream.into_split();
        let first = read_frame(&mut rd).await?;
        let hello = match pb::ExecToPeer::decode(first.as_slice()).map_err(io::Error::other)?.msg {
            Some(pb::exec_to_peer::Msg::Hello(h)) => h,
            _ => return Err(io::Error::other("executor did not start with hello")),
        };
        let exec = hello.executor_id;
        let (tx, mut rx) = mpsc::unbounded_channel::<pb::PeerToExec>();
        tx.send(pb::PeerToExec { msg: Some(pb::peer_to_exec::Msg::Welcome(self.welcome.clone())) })
            .map_err(|_| io::Error::other("writer closed"))?;
        self.conns.lock().expect("conns").insert(exec, tx);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let f = frame::encode(1, &msg.encode_to_vec(), frame::Codec::None, false);
                if wr.write_all(&f).await.is_err() {
                    break;
                }
            }
        });
        let _ = self.events.send(ExecEvent::Ready { exec }).await;
        loop {
            let body = read_frame(&mut rd).await?;
            let ev = match pb::ExecToPeer::decode(body.as_slice()).map_err(io::Error::other)?.msg {
                Some(pb::exec_to_peer::Msg::Done(done)) => ExecEvent::Done { exec, done: Box::new(done) },
                Some(pb::exec_to_peer::Msg::Blocked(msg)) => ExecEvent::Blocked { exec, msg },
                Some(pb::exec_to_peer::Msg::Log(log)) => ExecEvent::Log { exec, log },
                Some(pb::exec_to_peer::Msg::YieldItem(item)) => ExecEvent::Yield { exec, item },
                _ => continue,
            };
            if self.events.send(ev).await.is_err() {
                return Ok(());
            }
        }
    }
}

async fn read_frame(rd: &mut (impl AsyncReadExt + Unpin)) -> io::Result<Vec<u8>> {
    let mut h = [0u8; frame::HEADER_LEN];
    rd.read_exact(&mut h).await?;
    let (len, _kind, flags) = frame::parse_header(&h, MAX_FRAME).map_err(io::Error::other)?;
    let mut body = vec![0u8; len];
    rd.read_exact(&mut body).await?;
    frame::decode_body(flags, &body, MAX_FRAME).map_err(io::Error::other)
}

/// Unix sockets are guarded by file mode; additionally require the connecting
/// process to run as the same user.
pub fn check_same_user(stream: &UnixStream) -> io::Result<()> {
    let cred = stream.peer_cred()?;
    // SAFETY: geteuid has no preconditions.
    let me = unsafe { libc::geteuid() };
    if cred.uid() != me {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "connection from another user"));
    }
    Ok(())
}

/// Executors write no core dumps: a dump is the size of the process (they have
/// reached terabytes and filled disks), and a crash is reported anyway.
fn no_core_dumps(cmd: &mut tokio::process::Command) {
    // SAFETY: runs in the forked child before exec and only calls setrlimit,
    // which is async-signal-safe, on a struct built here.
    unsafe {
        cmd.pre_exec(|| {
            let zero = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            libc::setrlimit(libc::RLIMIT_CORE, &zero);
            Ok(())
        });
    }
}

#[cfg(target_os = "linux")]
fn join_cgroup_before_exec(cmd: &mut tokio::process::Command, group: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let procs = std::ffi::CString::new(group.join("cgroup.procs").as_os_str().as_bytes())
        .map_err(|_| io::Error::other("cgroup path contains a NUL byte"))?;
    // SAFETY: runs in the forked child before exec; it only calls
    // async-signal-safe functions (open, write, close) on a path prepared
    // before the fork. Writing "0" moves the calling process.
    unsafe {
        cmd.pre_exec(move || {
            let fd = libc::open(procs.as_ptr(), libc::O_WRONLY);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let ok = libc::write(fd, b"0".as_ptr().cast(), 1) == 1;
            libc::close(fd);
            if ok { Ok(()) } else { Err(io::Error::last_os_error()) }
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn join_cgroup_before_exec(_cmd: &mut tokio::process::Command, _group: &Path) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "per-attempt cgroups need Linux cgroup v2"))
}

pub fn restrict_to_owner(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}
