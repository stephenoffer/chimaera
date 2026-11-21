//! `chimaera slurm-launch -- CMD`: inside a Slurm allocation, one peer per
//! node through `srun`, then CMD on this node against its local peer.

use crate::runtime::CLIENT_SOCKET;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How long the peers get to form before the launch gives up.
const FORM_TIMEOUT: Duration = Duration::from_secs(300);
/// How long stopped peers get to drain and exit before they are killed.
const STOP_TIMEOUT: Duration = Duration::from_secs(60);

/// Start the peers, run `command` with `CHIMAERA_HOME` at this node's peer,
/// stop the peers, and return the command's exit code. `dir` is each
/// node's data directory (default `$TMPDIR/chimaera-$SLURM_JOB_ID`); it
/// must be node-local.
pub async fn launch(dir: Option<PathBuf>, port: Option<u16>, command: &[String]) -> Result<u8, String> {
    let mut env: BTreeMap<String, String> = std::env::vars().collect();
    let Some(job) = env.get("SLURM_JOB_ID").cloned() else {
        return Err("slurm-launch runs inside a Slurm allocation (sbatch, or salloc then srun); \
                    SLURM_JOB_ID is not set"
            .into());
    };
    if let Some(p) = port {
        env.insert("CHIMAERA_PORT".into(), p.to_string());
    }
    // This node must be one of the job's nodes: the driver talks to its own peer.
    let found = super::discover(chimaera_core::provider::ProviderKind::Slurm, &env)?;
    let nodes = chimaera_core::provider::expand_hostlist(
        env.get("SLURM_JOB_NODELIST").or(env.get("SLURM_NODELIST")).map_or("", String::as_str),
    )?
    .len();
    let dir = dir.unwrap_or_else(|| {
        PathBuf::from(env.get("TMPDIR").map_or("/tmp", String::as_str)).join(format!("chimaera-{job}"))
    });
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let sock = chimaera_proto::uds::socket_path(&dir, CLIENT_SOCKET);
    let _ = std::fs::remove_file(&sock);
    // A token of this job's own, unless the caller shares one (spares joining from another job).
    let token = match env.get("CHIMAERA_CLUSTER_TOKEN") {
        Some(t) => t.clone(),
        None => super::new_token().map_err(|e| format!("cannot make a cluster token: {e}"))?,
    };
    let exe = std::env::current_exe().map_err(|e| format!("cannot find this binary: {e}"))?;
    let n = nodes.to_string();
    let mut srun = tokio::process::Command::new("srun");
    // --overlap: steps the command itself starts can still use the nodes.
    srun.args(["--nodes", &n, "--ntasks", &n, "--ntasks-per-node=1", "--kill-on-bad-exit=1", "--overlap"])
        .arg(&exe)
        .arg("--dir")
        .arg(&dir)
        .args(["peer", "start", "--provider", "slurm"])
        .env("CHIMAERA_CLUSTER_TOKEN", token)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if let Some(p) = port {
        srun.env("CHIMAERA_PORT", p.to_string());
    }
    // Peer ports of jobs sharing a node are 7700 + job % 1000, so port + 10
    // would be another job's peer port; + 1000 collides only when they do.
    if !env.contains_key("CHIMAERA_CLIENT_PORT")
        && let Some(client) = found.port.checked_add(1000)
    {
        srun.env("CHIMAERA_CLIENT_PORT", client.to_string());
    }
    let mut peers = srun.spawn().map_err(|e| format!("cannot run srun: {e}"))?;

    let started = Instant::now();
    while !sock.exists() {
        if let Some(status) = peers.try_wait().map_err(|e| e.to_string())? {
            return Err(format!("the peers exited before this node's was ready ({status}); see the job's output"));
        }
        if started.elapsed() > FORM_TIMEOUT {
            stop(&mut peers).await;
            return Err(format!("this node's peer was not ready after {FORM_TIMEOUT:?}; see the job's output"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!(
        "chimaera: {nodes} peers started (this node is {}, data in {}); running {}",
        found.advertise,
        dir.display(),
        command.join(" ")
    );
    let status = run_command(command, &dir).await;
    stop(&mut peers).await;
    // As `chimaera run`: the command's exit code, 1 if a signal ended it.
    status.map(|s| s.code().map_or(1, |c| c.clamp(0, 255) as u8))
}

/// Run the command against this node's peer. If the job is told to stop
/// first (SIGTERM from scancel or preemption, or SIGUSR1 from
/// `sbatch --signal=B:USR1@SECONDS` before the time limit), pass SIGTERM on
/// to the command and wait for it, so the peers stop only after it does.
async fn run_command(command: &[String], dir: &std::path::Path) -> Result<std::process::ExitStatus, String> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut child = tokio::process::Command::new(&command[0])
        .args(&command[1..])
        .env("CHIMAERA_HOME", dir)
        .spawn()
        .map_err(|e| format!("cannot run {}: {e}", command[0]))?;
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut usr1 = signal(SignalKind::user_defined1()).map_err(|e| e.to_string())?;
    let stop_requested = tokio::select! {
        status = child.wait() => return status.map_err(|e| e.to_string()),
        _ = term.recv() => "SIGTERM",
        _ = usr1.recv() => "SIGUSR1",
    };
    eprintln!("chimaera: the job got {stop_requested}; stopping {} first, then the peers", command[0]);
    signal_child(&child, libc::SIGTERM);
    child.wait().await.map_err(|e| e.to_string())
}

fn signal_child(child: &tokio::process::Child, sig: libc::c_int) {
    if let Some(pid) = child.id() {
        // SAFETY: kill has no memory preconditions; `pid` is our own child.
        unsafe { libc::kill(pid as libc::pid_t, sig) };
    }
}

/// SIGUSR1 to srun, which forwards it to every peer: each drains and exits.
/// Not SIGTERM: srun answers SIGTERM by killing its tasks outright.
async fn stop(peers: &mut tokio::process::Child) {
    signal_child(peers, libc::SIGUSR1);
    if tokio::time::timeout(STOP_TIMEOUT, peers.wait()).await.is_err() {
        eprintln!("chimaera: the peers did not stop within {STOP_TIMEOUT:?}; killing them");
        let _ = peers.kill().await;
    }
}
