//! The `chimaera` command: run a peer and inspect runs.
//!
//! Results go to stdout; progress and errors go to stderr; the exit code is
//! non-zero when anything failed.

use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_peer::runtime::{self, PeerConfig};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "chimaera", version, about = "Chimaera distributed runtime")]
struct Cli {
    /// Data directory of the local peer (default: $CHIMAERA_HOME or ~/.chimaera/local).
    #[arg(long, global = true)]
    dir: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a peer in the foreground.
    Peer {
        #[command(subcommand)]
        action: PeerCmd,
    },
    /// Run a program against the local peer (`chimaera run -- python app.py`).
    Run {
        /// Keep the program's run going after the program exits.
        #[arg(long)]
        detach: bool,
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    /// Peer status: executors, queue, CPU in use, why work is waiting.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Summary of a run: tasks by state.
    Inspect {
        run: String,
        #[arg(long)]
        json: bool,
    },
    /// What happened to one task of a run (attempts, measurements, waiting reason),
    /// or the tuning decisions the peer made.
    Explain {
        run: String,
        /// Task id (hex) to explain.
        #[arg(long)]
        task: Option<String>,
        /// Show tuning decisions.
        #[arg(long)]
        decisions: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stop the local peer.
    Stop,
    /// Cluster setup helpers.
    Cluster {
        #[command(subcommand)]
        action: ClusterCmd,
    },
    /// Check what this host supports (cgroups, shared memory, Python).
    Doctor,
}

#[derive(Subcommand)]
enum PeerCmd {
    Start {
        /// CPU capacity to offer (default: this process's allocation).
        #[arg(long)]
        cpus: Option<f64>,
        /// Exit after this many seconds with no sessions and no work.
        #[arg(long)]
        idle_timeout: Option<u64>,
        /// Join a cluster: address to listen on for other peers (host:port).
        /// Without it the peer runs alone.
        #[arg(long)]
        listen: Option<std::net::SocketAddr>,
        /// Address other peers dial to reach this one (default: --listen).
        #[arg(long)]
        advertise: Option<String>,
        /// Founding peers' addresses, comma-separated. A peer whose address
        /// is listed is a founder; others join as spares.
        #[arg(long, value_delimiter = ',')]
        seeds: Vec<String>,
        /// Number of founders; must match --seeds (a guard against typos).
        #[arg(long)]
        bootstrap_expect: Option<usize>,
        /// Cluster name; peers of different clusters refuse each other.
        #[arg(long, default_value = "chimaera")]
        cluster: String,
        /// Shard groups the vshards are split across (fixed at formation).
        #[arg(long, default_value_t = 8)]
        data_groups: u32,
        /// File holding the cluster's shared token (`chimaera cluster token`).
        /// Also read from $CHIMAERA_CLUSTER_TOKEN. Required unless --insecure.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Let peers join without a token (only on a network you trust).
        #[arg(long)]
        insecure: bool,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// Print a new random cluster token.
    Token,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("CHIMAERA_LOG")
                .unwrap_or_else(|_| "chimaera=info,chimaera_peer=info,warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let dir = cli.dir.clone().unwrap_or_else(runtime::default_local_dir);
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    let result =
        rt.block_on(async move {
            match cli.cmd {
                Cmd::Peer {
                    action:
                        PeerCmd::Start {
                            cpus,
                            idle_timeout,
                            listen,
                            advertise,
                            seeds,
                            bootstrap_expect,
                            cluster,
                            data_groups,
                            token_file,
                            insecure,
                        },
                } => match listen {
                    None => {
                        let cfg = PeerConfig { dir, cpus, idle_timeout: idle_timeout.map(Duration::from_secs) };
                        runtime::run_local(cfg).await.map_err(|e| e.to_string()).map(|_| 0)
                    }
                    Some(listen) => {
                        if seeds.is_empty() {
                            return Err("--listen needs --seeds (the founding peers' addresses)".to_string());
                        }
                        if let Some(n) = bootstrap_expect
                            && n != seeds.len()
                        {
                            return Err(format!("--bootstrap-expect {n} but --seeds lists {} addresses", seeds.len()));
                        }
                        let token = cluster_token(token_file.as_deref(), insecure)?;
                        let cfg = chimaera_peer::cluster::ClusterConfig {
                            dir,
                            cluster,
                            listen,
                            advertise: advertise.unwrap_or_else(|| listen.to_string()),
                            seeds,
                            data_groups: data_groups.max(1),
                            cpus,
                            token,
                        };
                        chimaera_peer::cluster::run_cluster(cfg).await.map_err(|e| e.to_string()).map(|_| 0)
                    }
                },
                Cmd::Run { detach, command } => run_program(&dir, detach, &command).await,
                Cmd::Status { json } => status(&dir, json).await,
                Cmd::Inspect { run, json } => inspect(&dir, &run, json).await,
                Cmd::Explain { run, task, decisions, json } => explain(&dir, &run, task, decisions, json).await,
                Cmd::Stop => stop(&dir).await,
                Cmd::Cluster { action: ClusterCmd::Token } => {
                    println!("{}", new_token().map_err(|e| e.to_string())?);
                    Ok(0)
                }
                Cmd::Doctor => Ok(doctor(&dir)),
            }
        });
    match result {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("chimaera: {e}");
            ExitCode::from(1)
        }
    }
}

/// The cluster token from --token-file or $CHIMAERA_CLUSTER_TOKEN. A cluster
/// peer without one refuses to start unless --insecure says so.
fn cluster_token(file: Option<&std::path::Path>, insecure: bool) -> Result<Option<String>, String> {
    let token = match file {
        Some(f) => Some(
            std::fs::read_to_string(f)
                .map_err(|e| format!("cannot read token file {}: {e}", f.display()))?
                .trim()
                .to_string(),
        ),
        None => std::env::var("CHIMAERA_CLUSTER_TOKEN").ok().map(|t| t.trim().to_string()),
    };
    match token {
        Some(t) if t.len() >= 16 => Ok(Some(t)),
        Some(_) => Err("the cluster token is shorter than 16 characters; use `chimaera cluster token`".into()),
        None if insecure => Ok(None),
        None => Err("a cluster peer needs a token: run `chimaera cluster token > token` once, copy the file to \
                     every peer and pass --token-file token (or --insecure on a trusted network)"
            .into()),
    }
}

fn new_token() -> std::io::Result<String> {
    use std::io::Read;
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

type Client = pb::peer_client::PeerClient<tonic::transport::Channel>;

async fn connect(dir: &std::path::Path) -> Result<Client, String> {
    let sock = chimaera_proto::uds::socket_path(dir, runtime::CLIENT_SOCKET);
    let ch = chimaera_proto::uds::connect(&sock)
        .await
        .map_err(|e| format!("no local peer at {} ({e}); start one with `chimaera peer start`", sock.display()))?;
    Ok(pb::peer_client::PeerClient::new(ch))
}

fn parse_run(run: &str) -> Result<Id, String> {
    Id::parse_hex(run.trim_start_matches("r-")).ok_or_else(|| format!("{run:?} is not a run id (32 hex digits)"))
}

async fn run_program(dir: &std::path::Path, detach: bool, command: &[String]) -> Result<u8, String> {
    let status = tokio::process::Command::new(&command[0])
        .args(&command[1..])
        .env("CHIMAERA_HOME", dir)
        .env("CHIMAERA_DETACH", if detach { "1" } else { "0" })
        .status()
        .await
        .map_err(|e| format!("cannot run {}: {e}", command[0]))?;
    Ok(status.code().map_or(1, |c| c.clamp(0, 255) as u8))
}

async fn status(dir: &std::path::Path, json: bool) -> Result<u8, String> {
    let mut c = connect(dir).await?;
    let s = c.status(pb::StatusRequest {}).await.map_err(|e| e.message().to_string())?.into_inner();
    let peer = s.peer.clone().unwrap_or_default();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "peer": Id::from_slice(&peer.peer).map(|i| i.hex()).unwrap_or_default(),
                "incarnation": peer.incarnation,
                "cpus": peer.usable_cores,
                "cpu_in_use": s.cpu_in_use,
                "executors": s.executors,
                "busy_executors": s.busy_executors,
                "queued": s.queued,
                "running": s.running,
                "sessions": s.sessions,
                "object_bytes": s.object_bytes,
                "pending_reasons": s.pending_reasons,
                "dispatch_p50_us": s.dispatch_p50_us,
                "dispatch_p99_us": s.dispatch_p99_us,
                "dispatch_samples": s.dispatch_samples,
            })
        );
    } else {
        println!(
            "peer {} (incarnation {}) · {:.2}/{:.2} CPU in use · {} executors ({} busy) · {} queued · {} running · {} sessions · {} object bytes",
            Id::from_slice(&peer.peer).map(|i| i.hex()[..12].to_string()).unwrap_or_default(),
            peer.incarnation,
            s.cpu_in_use,
            peer.usable_cores,
            s.executors,
            s.busy_executors,
            s.queued,
            s.running,
            s.sessions,
            s.object_bytes
        );
        for r in s.pending_reasons {
            println!("  waiting: {r}");
        }
    }
    Ok(0)
}

async fn inspect(dir: &std::path::Path, run: &str, json: bool) -> Result<u8, String> {
    let run = parse_run(run)?;
    let mut c = connect(dir).await?;
    let r = c
        .explain(pb::ExplainRequest { run: run.to_vec(), ..Default::default() })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let Some(rec) = r.run else { return Err(format!("run {} is not known to this peer", run.hex())) };
    if json {
        println!(
            "{}",
            serde_json::json!({"run": run.hex(), "state": format!("{:?}", rec.state()), "detached": rec.detached, "tasks": r.counts})
        );
    } else {
        println!("run {} · {:?}{}", run.hex(), rec.state(), if rec.detached { " · detached" } else { "" });
        for (k, v) in &r.counts {
            println!("  {k:<24} {v}");
        }
    }
    Ok(0)
}

async fn explain(
    dir: &std::path::Path,
    run: &str,
    task: Option<String>,
    decisions: bool,
    json: bool,
) -> Result<u8, String> {
    let run = parse_run(run)?;
    let task = task
        .map(|t| Id::parse_hex(&t).ok_or_else(|| format!("{t:?} is not a task id")))
        .transpose()?
        .map(|t| pb::TaskKey { run: run.to_vec(), task: t.to_vec() });
    let mut c = connect(dir).await?;
    let r = c
        .explain(pb::ExplainRequest { run: run.to_vec(), task: task.clone(), decisions, pending: true })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    if json {
        let attempts: Vec<_> = r
            .attempts
            .iter()
            .map(|a| {
                serde_json::json!({
                    "attempt": a.attempt,
                    "end_reason": a.end_reason,
                    "detail": a.detail,
                    "placement": a.placement_reason,
                    "measures": a.measures.iter().map(|(k, m)| (k.clone(), serde_json::json!({"value": m.value, "scope": m.scope, "method": m.method}))).collect::<serde_json::Map<_, _>>(),
                })
            })
            .collect();
        let decisions: Vec<_> = r
            .decisions
            .iter()
            .map(|d| serde_json::json!({"controller": d.controller, "knob": d.knob, "scope": d.scope, "old": d.old_value, "new": d.new_value, "outcome": d.outcome, "evidence": d.evidence, "reason": d.reason}))
            .collect();
        println!(
            "{}",
            serde_json::json!({"pending_reason": r.pending_reason, "attempts": attempts, "decisions": decisions, "task_state": r.task.as_ref().map(|t| format!("{:?}", t.state()))})
        );
        return Ok(0);
    }
    if let Some(t) = &r.task {
        println!("task {:?}", t.state());
        if !r.pending_reason.is_empty() {
            println!("  waiting: {}", r.pending_reason);
        }
        if let Some(pb::outcome::Value::Err(e)) = t.outcome.as_ref().and_then(|o| o.value.as_ref()) {
            println!("  error ({:?}): {} {}", e.kind(), e.type_name, e.message);
        }
        for f in &t.failure_history {
            println!("  failure (attempt {}): {}", f.attempt, f.message);
        }
    }
    for a in &r.attempts {
        let m = |k: &str| a.measures.get(k);
        let fmt = |k: &str, unit: &str| {
            m(k).map_or("unavailable".to_string(), |x| format!("{:.3}{unit} [{}]", x.value, x.scope))
        };
        println!(
            "  attempt {} · {} · wall {} · cpu {} · mean cores {} · rss peak {}{}",
            a.attempt,
            a.end_reason,
            fmt("wall_seconds", " s"),
            fmt("cpu_seconds", " s"),
            fmt("mean_cores", ""),
            m("rss_peak_bytes").map_or("unavailable".into(), |x| format!(
                "{:.1} MiB [{}]",
                x.value / 1048576.0,
                x.scope
            )),
            if a.detail.is_empty() { String::new() } else { format!(" · {}", a.detail) }
        );
    }
    for d in &r.decisions {
        println!(
            "  {} {} {}→{} {} · {} · {}",
            d.controller, d.knob, d.old_value, d.new_value, d.outcome, d.evidence, d.reason
        );
    }
    Ok(0)
}

async fn stop(dir: &std::path::Path) -> Result<u8, String> {
    let mut c = connect(dir).await?;
    c.shutdown(pb::ShutdownRequest {}).await.map_err(|e| e.message().to_string())?;
    eprintln!("stopping the local peer");
    Ok(0)
}

fn doctor(dir: &std::path::Path) -> u8 {
    let shape = chimaera_peer::topology::discover();
    println!("os {} {} · kernel {}", shape.os, shape.arch, shape.kernel);
    println!(
        "cpus {} (usable {:.2}) · memory {:.1} GiB",
        shape.cpus.len(),
        shape.usable_cores(),
        shape.memory_bytes as f64 / (1u64 << 30) as f64
    );
    println!("numa nodes {} · gpus {}", shape.numa.len(), shape.gpus.len());
    println!(
        "isolated accounting: {}",
        if shape.cgroup_delegated {
            "available (cgroup v2 delegated)".to_string()
        } else {
            "unavailable: accounting=\"isolated\" fails preflight here; measurements are process-scoped".to_string()
        }
    );
    match fsync_p50_us(dir) {
        // Cluster peers sync every raft commit; a call needs about three.
        Ok(us) => println!("fsync p50 {us:.0} µs in {} (cluster peers pay this per commit)", dir.display()),
        Err(e) => println!("fsync probe failed in {}: {e}", dir.display()),
    }
    0
}

/// Median latency of a small write followed by fsync in `dir`.
fn fsync_p50_us(dir: &std::path::Path) -> std::io::Result<f64> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let path = dir.join(".fsync-probe");
    let mut f = std::fs::File::create(&path)?;
    let mut samples = Vec::with_capacity(50);
    for i in 0..50u8 {
        let t = std::time::Instant::now();
        f.write_all(&[i; 4096])?;
        // Plain fsync, as the cluster backend issues (not macOS F_FULLFSYNC).
        // SAFETY: the descriptor is owned by `f` and open for the call.
        if unsafe { libc::fsync(std::os::fd::AsRawFd::as_raw_fd(&f)) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        samples.push(t.elapsed().as_secs_f64() * 1e6);
    }
    let _ = std::fs::remove_file(&path);
    samples.sort_by(f64::total_cmp);
    Ok(samples[samples.len() / 2])
}
