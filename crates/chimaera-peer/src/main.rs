//! The `chimaera` command: run a peer and inspect runs.
//!
//! Results go to stdout; progress and errors go to stderr; the exit code is
//! non-zero when anything failed.

use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::provider::ProviderKind;
use chimaera_peer::launch;
use chimaera_peer::runtime::{self, PeerConfig};
use clap::{Parser, Subcommand};
use std::collections::BTreeMap;
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
    /// What a run's calls printed on this peer, kept after the driver has gone.
    Logs {
        run: String,
        /// Keep waiting for more output (Ctrl-C to stop).
        #[arg(long, short)]
        follow: bool,
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
    /// What the peer has learned about each function: calls, time, cores,
    /// memory, and the reservation the next call gets.
    Profiles {
        /// Only functions whose name contains this.
        #[arg(long = "fn")]
        name: Option<String>,
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
    /// Kubernetes manifests (`chimaera k8s render --image IMG | kubectl apply -f -`).
    K8s {
        #[command(subcommand)]
        action: K8sCmd,
    },
    /// Inside a Slurm allocation: start a peer on every node, run COMMAND on
    /// this one against its peer, then stop the peers.
    SlurmLaunch {
        /// Peer port (default: 7700 + SLURM_JOB_ID % 1000).
        #[arg(long)]
        port: Option<u16>,
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    /// Start the peers of an SSH cluster profile in clusters.toml.
    Up { name: String },
    /// Stop the peers of an SSH cluster profile.
    Down { name: String },
}

#[derive(Subcommand)]
enum K8sCmd {
    /// Print the Services, StatefulSet and PodDisruptionBudget of a cluster.
    Render(launch::k8s::Spec),
}

#[derive(Subcommand)]
enum PeerCmd {
    Start {
        /// Where the founders and this peer's address come from: auto, local,
        /// static (the flags), k8s, slurm, hpc (PBS/LSF) or sky. `auto`
        /// treats --listen as static and --idle-timeout as local, else
        /// detects the platform from its environment. Explicit flags win
        /// over discovered values.
        #[arg(long, default_value = "auto", value_parser = launch::PROVIDERS)]
        provider: String,
        /// Under k8s, slurm, hpc and sky: accept drivers on other machines on
        /// this port (default: $CHIMAERA_CLIENT_PORT, else the peer port + 10).
        #[arg(long)]
        client_port: Option<u16>,
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
        /// Directory with this peer's mutual-TLS files (`chimaera cluster cert`).
        #[arg(long)]
        tls_dir: Option<PathBuf>,
        /// Also accept drivers on other machines at this address (host:port).
        /// They must present the cluster token, and use TLS when --tls-dir is set.
        #[arg(long)]
        client_listen: Option<std::net::SocketAddr>,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// Print a new random cluster token.
    Token,
    /// Create a cluster CA and token in OUT (ca.pem, ca.key, token).
    Init {
        #[arg(long)]
        out: PathBuf,
    },
    /// Issue a peer certificate from the CA in --ca into OUT, the directory
    /// that peer's --tls-dir names (ca.pem, peer.pem, peer.key).
    Cert {
        #[arg(long)]
        ca: PathBuf,
        /// A name for the peer (its host name, say); recorded in the certificate.
        #[arg(long)]
        name: String,
        #[arg(long)]
        out: PathBuf,
    },
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
    let dir_flag = cli.dir.clone();
    let dir = dir_flag.clone().unwrap_or_else(runtime::default_local_dir);
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    let result = rt.block_on(async move {
        match cli.cmd {
            Cmd::Peer {
                action:
                    PeerCmd::Start {
                        provider,
                        client_port,
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
                        tls_dir,
                        client_listen,
                    },
            } => {
                let env: BTreeMap<String, String> = std::env::vars().collect();
                let kind =
                    launch::choose(&provider, &env, listen.is_some(), !seeds.is_empty(), idle_timeout.is_some());
                if client_port.is_some() && matches!(kind, ProviderKind::Local | ProviderKind::Static) {
                    return Err("--client-port applies to the k8s, slurm, hpc and sky providers; \
                                use --client-listen"
                        .into());
                }
                let addrs = match kind {
                    ProviderKind::Local => {
                        let cfg = PeerConfig { dir, cpus, idle_timeout: idle_timeout.map(Duration::from_secs) };
                        return runtime::run_local(cfg).await.map_err(|e| e.to_string()).map(|_| 0);
                    }
                    ProviderKind::Static => {
                        let Some(listen) = listen else {
                            return Err("--seeds needs --listen (this peer's address)".into());
                        };
                        if seeds.is_empty() {
                            return Err("--listen needs --seeds (the founding peers' addresses)".into());
                        }
                        let advertise = advertise.unwrap_or_else(|| listen.to_string());
                        launch::Addrs { listen, advertise, seeds, client_listen }
                    }
                    kind => {
                        let name = format!("{kind:?}").to_lowercase();
                        let d = launch::discover(kind, &env).map_err(|e| format!("provider {name}: {e}"))?;
                        tracing::info!(provider = %name, advertise = %d.advertise, seeds = ?d.seeds, pool = %d.pool, labels = ?d.labels, "discovered");
                        let flags = launch::Flags { listen, advertise, seeds, client_listen, client_port };
                        launch::platform_addrs(&d, flags, &env)?
                    }
                };
                if let Some(n) = bootstrap_expect
                    && n != addrs.seeds.len()
                {
                    return Err(format!("--bootstrap-expect {n} but there are {} seeds", addrs.seeds.len()));
                }
                let token = cluster_token(token_file.as_deref(), insecure)?;
                let tls = tls_dir.as_deref().map(chimaera_peer::tls::load).transpose().map_err(|e| e.to_string())?;
                let cfg = chimaera_peer::cluster::ClusterConfig {
                    dir,
                    cluster,
                    listen: addrs.listen,
                    advertise: addrs.advertise,
                    seeds: addrs.seeds,
                    data_groups: data_groups.max(1),
                    cpus,
                    token,
                    tls,
                    client_listen: addrs.client_listen,
                };
                chimaera_peer::cluster::run_cluster(cfg).await.map_err(|e| e.to_string()).map(|_| 0)
            }
            Cmd::Run { detach, command } => run_program(&dir, detach, &command).await,
            Cmd::Status { json } => status(&dir, json).await,
            Cmd::Logs { run, follow } => logs(&dir, &run, follow).await,
            Cmd::Inspect { run, json } => inspect(&dir, &run, json).await,
            Cmd::Explain { run, task, decisions, json } => explain(&dir, &run, task, decisions, json).await,
            Cmd::Profiles { name, json } => Ok(profiles(&dir, name.as_deref(), json)),
            Cmd::Stop => stop(&dir).await,
            Cmd::Cluster { action: ClusterCmd::Token } => {
                println!("{}", launch::new_token().map_err(|e| e.to_string())?);
                Ok(0)
            }
            Cmd::Cluster { action: ClusterCmd::Init { out } } => {
                let token = launch::new_token().map_err(|e| e.to_string())?;
                chimaera_peer::tls::init(&out, &token).map_err(|e| format!("{}: {e}", out.display()))?;
                eprintln!(
                    "cluster CA and token in {}; issue each peer a certificate with `chimaera cluster cert`",
                    out.display()
                );
                Ok(0)
            }
            Cmd::Cluster { action: ClusterCmd::Cert { ca, name, out } } => {
                chimaera_peer::tls::issue(&ca, &name, &out).map_err(|e| format!("{}: {e}", out.display()))?;
                Ok(0)
            }
            Cmd::Doctor => Ok(doctor(&dir)),
            Cmd::K8s { action: K8sCmd::Render(spec) } => {
                print!("{}", launch::k8s::render(&spec)?);
                Ok(0)
            }
            Cmd::SlurmLaunch { port, command } => launch::slurm::launch(dir_flag, port, &command).await,
            Cmd::Up { name } => launch::ssh::up(&name),
            Cmd::Down { name } => launch::ssh::down(&name),
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

async fn logs(dir: &std::path::Path, run: &str, follow: bool) -> Result<u8, String> {
    use std::io::Write as _;
    let run = parse_run(run)?;
    let mut c = connect(dir).await?;
    let mut offset = 0;
    loop {
        let req = pb::LogsRequest { run: run.to_vec(), offset, wait_ms: if follow { 10_000 } else { 0 } };
        let r = c.logs(req).await.map_err(|e| e.message().to_string())?.into_inner();
        std::io::stdout().write_all(&r.text).map_err(|e| e.to_string())?;
        offset = r.next;
        if r.text.is_empty() && !follow {
            return Ok(0);
        }
    }
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
                "memory_pool": s.memory_pool,
                "memory_used": s.memory_used,
                "memory_available": s.memory_available,
                "automatic": s.automatic,
                "links": s.links,
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
        println!(
            "  memory: {} of {} MiB pool in use, {} MiB available on the host",
            s.memory_used >> 20,
            s.memory_pool >> 20,
            s.memory_available >> 20
        );
        for a in &s.automatic {
            println!("  automatic: {a}");
        }
        for l in &s.links {
            println!("  link: {l}");
        }
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

/// The peer's saved profiles (written every 30 s while they change, and at
/// shutdown), one function per line.
fn profiles(dir: &std::path::Path, filter: Option<&str>, json: bool) -> u8 {
    let path = dir.join("profiles.pb");
    let Some(age) = std::fs::metadata(&path).and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()) else {
        eprintln!("no profiles saved in {} yet (a peer saves them after its first calls)", dir.display());
        return 1;
    };
    let all = chimaera_peer::profiles::Profiles::load_file(&path).all();
    let rows: Vec<_> = all.iter().filter(|(_, name, ..)| filter.is_none_or(|f| name.contains(f))).collect();
    let mib = |b: u64| b.div_ceil(1 << 20);
    if json {
        let out: Vec<_> = rows
            .iter()
            .map(|(k, name, s, cpus)| {
                let mem = s.memory_for(s.max_arg_bytes);
                serde_json::json!({
                    "name": name,
                    "digest": chimaera_peer::stores::hex(&k.digest),
                    "method": k.method,
                    "calls": s.n,
                    "wall_p50_s": s.wall_p50_s,
                    "cores_mean": s.cores_mean,
                    "cpus": if *cpus > 0.0 { Some(*cpus) } else { None },
                    "memory_bytes": mem.as_ref().map(|m| m.bytes),
                    "memory_evidence": mem.map(|m| m.evidence),
                    "peak_p95_bytes": s.peak_p95,
                    "killed_for_memory_at": s.oom_bounds.iter().map(|b| b.1).max(),
                })
            })
            .collect();
        println!("{}", serde_json::Value::Array(out));
        return 0;
    }
    println!("{} function profile(s), saved {} s ago in {}", rows.len(), age.as_secs(), path.display());
    for (k, name, s, cpus) in rows {
        let name = if name.is_empty() {
            format!("function {}", &chimaera_peer::stores::hex(&k.digest)[..12])
        } else {
            name.clone()
        };
        let cpu = if *cpus > 0.0 { format!("cpus {cpus} (learned)") } else { "cpus 1 (default)".into() };
        let mem = s
            .memory_for(s.max_arg_bytes)
            .map_or("memory not known yet".into(), |m| format!("memory {} MiB ({})", mib(m.bytes), m.evidence));
        println!(
            "  {name}: {} calls, {:.3} s p50, {:.2} cores on average; next call gets {cpu}, {mem}",
            s.n, s.wall_p50_s, s.cores_mean
        );
    }
    0
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
