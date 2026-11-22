//! The `chimaera` command: run a peer and inspect runs.
//!
//! Results go to stdout; progress and errors go to stderr; the exit code is
//! non-zero when anything failed.

use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::provider::ProviderKind;
use chimaera_peer::launch;
use chimaera_peer::runtime::{self, PeerConfig};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
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
    /// Run a program against the local peer, or with --cluster against a
    /// cluster (`chimaera run --cluster lab -- python app.py`).
    Run {
        /// Keep the program's run going after the program exits.
        #[arg(long)]
        detach: bool,
        /// A cluster profile or tcp://host:port, passed to the program as $CHIMAERA_CLUSTER.
        #[arg(long)]
        cluster: Option<String>,
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    /// Peer status: executors, queue, CPU in use, why work is waiting.
    Status {
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        at: At,
    },
    /// The cluster's runs, newest first: state, age and the command that started each.
    Runs {
        /// Show at most this many (0: all).
        #[arg(long, default_value_t = 20)]
        limit: u32,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        at: At,
    },
    /// Cancel a run: its unfinished calls are cancelled and its actors stopped.
    Cancel {
        run: String,
        #[command(flatten)]
        at: At,
    },
    /// What this peer's object store holds: in memory, on disk (spilled or
    /// replicas), per run, the largest objects, NumPy arenas and the reserve.
    Objects {
        #[arg(long)]
        json: bool,
        /// Every live peer's store, not only this one's.
        #[arg(long)]
        cluster: bool,
    },
    /// What a run's calls printed on this peer, kept after the driver has gone.
    Logs {
        run: String,
        /// Keep waiting for more output (Ctrl-C to stop).
        #[arg(long, short)]
        follow: bool,
        #[command(flatten)]
        at: At,
    },
    /// Summary of a run: tasks by state.
    Inspect {
        run: String,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        at: At,
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
        #[command(flatten)]
        at: At,
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

/// Which peer a command asks.
#[derive(clap::Args)]
struct At {
    /// A cluster profile in ~/.config/chimaera/clusters.toml, or tcp://host:port.
    /// Default, as for a driver: $CHIMAERA_CLUSTER, $CHIMAERA_ADDRESS, then the
    /// profile file's default; the local peer when --dir or $CHIMAERA_HOME is
    /// set, or when none of those is.
    #[arg(long)]
    cluster: Option<String>,
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
    /// Print a driver credential for TENANT: drivers presenting it (as
    /// $CHIMAERA_TOKEN, or a profile's token file) reach only that tenant's
    /// runs, actors, store and logs, and cannot stop peers. Changing the
    /// cluster token revokes every credential.
    Credential {
        #[arg(long)]
        tenant: String,
        /// The cluster token's file (default: $CHIMAERA_CLUSTER_TOKEN).
        #[arg(long)]
        token_file: Option<PathBuf>,
    },
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
    /// Rebuild group GROUP around this stopped peer when its other voters are
    /// gone for good: this peer becomes the group's only voter, keeping the
    /// commits it holds (ones it never received are lost), and the cluster
    /// adds voters back once it runs. Use the surviving replica that holds the
    /// most of the group's log.
    Recover {
        #[arg(long)]
        group: u32,
        /// Required: confirms that the group's other voters will not return.
        #[arg(long)]
        force_new_quorum: bool,
    },
}

fn main() -> ExitCode {
    // The peer level decides how a release is rolled out (docs/deploy.md, "Upgrading").
    let version: &'static str =
        format!("{} (peer level {})", env!("CARGO_PKG_VERSION"), chimaera_proto::PEER_LEVEL).leak();
    let cli = Cli::from_arg_matches(&Cli::command().version(version).get_matches()).unwrap_or_else(|e| e.exit());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("CHIMAERA_LOG")
                .unwrap_or_else(|_| "chimaera=info,chimaera_peer=info,warn".into()),
        )
        .with_writer(std::io::stderr)
        // Colours only on a terminal: peer.log is read with less and grep.
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    let dir_flag = cli.dir.clone();
    let dir = dir_flag.clone().unwrap_or_else(runtime::default_local_dir);
    // A data directory named on the command line or in $CHIMAERA_HOME means its peer.
    let local = dir_flag.is_some() || std::env::var_os("CHIMAERA_HOME").is_some();
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
            Cmd::Run { detach, cluster, command } => run_program(&dir, detach, cluster.as_deref(), &command).await,
            Cmd::Status { json, at } => status(connect(&dir, local, &at).await?, json).await,
            Cmd::Runs { limit, json, at } => runs(connect(&dir, local, &at).await?, limit, json).await,
            Cmd::Cancel { run, at } => cancel(connect(&dir, local, &at).await?, &run).await,
            Cmd::Objects { json, cluster } => objects(&dir, json, cluster).await,
            Cmd::Logs { run, follow, at } => logs(connect(&dir, local, &at).await?, &run, follow).await,
            Cmd::Inspect { run, json, at } => inspect(connect(&dir, local, &at).await?, &run, json).await,
            Cmd::Explain { run, task, decisions, json, at } => {
                explain(connect(&dir, local, &at).await?, &run, task, decisions, json).await
            }
            Cmd::Profiles { name, json } => Ok(profiles(&dir, name.as_deref(), json)),
            Cmd::Stop => stop(&dir).await,
            Cmd::Cluster { action: ClusterCmd::Token } => {
                println!("{}", launch::new_token().map_err(|e| e.to_string())?);
                Ok(0)
            }
            Cmd::Cluster { action: ClusterCmd::Credential { tenant, token_file } } => {
                if !chimaera_peer::auth::valid_tenant(&tenant) {
                    return Err(format!("tenant {tenant:?} must be 1-64 letters, digits, '-' or '_'"));
                }
                let token = cluster_token(token_file.as_deref(), false)?.expect("required above");
                println!("{}", chimaera_peer::auth::credential(&token, &tenant));
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
            Cmd::Cluster { action: ClusterCmd::Recover { group, force_new_quorum } } => {
                if !force_new_quorum {
                    return Err(format!(
                        "recover makes this peer the only voter of group {group}, losing any commit it never \
                         received; run it only when the group's other voters are gone for good, and pass \
                         --force-new-quorum"
                    ));
                }
                let at = chimaera_peer::cluster::recover_group(&dir, group).map_err(|e| format!("{}: {e}", dir.display()))?;
                println!(
                    "group {group}: this peer is its only voter, resuming after log index {at}; start the peer, and \
                     the cluster adds voters back from live peers"
                );
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

type Client = chimaera_proto::client::PeerClient;

async fn connect_local(dir: &std::path::Path) -> Result<Client, String> {
    let sock = chimaera_proto::uds::socket_path(dir, runtime::CLIENT_SOCKET);
    chimaera_proto::client::connect(&sock.to_string_lossy(), None, None)
        .await
        .map_err(|e| format!("no local peer at {} ({e}); start one with `chimaera peer start`", sock.display()))
}

/// The peer `at` names, else the one a driver here would use (`local`: the
/// data directory was named, so its peer).
async fn connect(dir: &std::path::Path, local: bool, at: &At) -> Result<Client, String> {
    if at.cluster.is_none() && local {
        return connect_local(dir).await;
    }
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let path = chimaera_peer::launch::ssh::profile_path();
    let text = std::fs::read_to_string(&path).ok();
    let shown = path.display().to_string();
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| ".".into()));
    match chimaera_peer::target::resolve(at.cluster.as_deref(), &env, text.as_deref().map(|t| (shown.as_str(), t)), &home)? {
        Some(t) => chimaera_peer::target::connect(&t).await,
        None => connect_local(dir).await,
    }
}

fn parse_run(run: &str) -> Result<Id, String> {
    Id::parse_hex(run.trim_start_matches("r-")).ok_or_else(|| format!("{run:?} is not a run id (32 hex digits)"))
}

async fn run_program(dir: &std::path::Path, detach: bool, cluster: Option<&str>, command: &[String]) -> Result<u8, String> {
    let mut cmd = tokio::process::Command::new(&command[0]);
    cmd.args(&command[1..]).env("CHIMAERA_DETACH", if detach { "1" } else { "0" });
    match cluster {
        Some(c) => cmd.env("CHIMAERA_CLUSTER", c),
        None => cmd.env("CHIMAERA_HOME", dir),
    };
    let status = cmd
        .status()
        .await
        .map_err(|e| format!("cannot run {}: {e}", command[0]))?;
    Ok(status.code().map_or(1, |c| c.clamp(0, 255) as u8))
}

async fn logs(mut c: Client, run: &str, follow: bool) -> Result<u8, String> {
    use std::io::Write as _;
    let run = parse_run(run)?;
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

async fn objects(dir: &std::path::Path, json: bool, cluster: bool) -> Result<u8, String> {
    let mut c = connect_local(dir).await?;
    let req = pb::StatusRequest { objects: cluster };
    let s = c.status(req).await.map_err(|e| e.message().to_string())?.into_inner();
    let stores: Vec<serde_json::Value> = if cluster {
        s.objects.iter().map(|j| serde_json::from_str(j).unwrap_or_default()).collect()
    } else {
        let object_dir = s.peer.map(|p| p.object_dir).unwrap_or_default();
        vec![chimaera_peer::objplane::inventory(std::path::Path::new(&object_dir))]
    };
    if json {
        let out = if cluster { serde_json::Value::Array(stores) } else { stores.into_iter().next().unwrap_or_default() };
        println!("{out}");
        return Ok(0);
    }
    for inv in &stores {
        if cluster {
            println!("peer {}", inv["addr"].as_str().unwrap_or("?"));
        }
        if let Some(e) = inv["error"].as_str() {
            println!("  cannot read its store: {e}");
            continue;
        }
        print_objects(inv);
    }
    Ok(0)
}

fn print_objects(inv: &serde_json::Value) {
    let mib = |v: &serde_json::Value| v.as_u64().unwrap_or(0) as f64 / (1 << 20) as f64;
    println!("objects in {} (huge pages: {})", inv["object_dir"].as_str().unwrap_or(""), inv["huge_pages"]);
    println!("  in memory: {} objects, {:.1} MiB", inv["in_memory"]["objects"], mib(&inv["in_memory"]["bytes"]));
    println!("  on disk:   {} objects, {:.1} MiB (spilled, or replicas of other peers' objects)",
        inv["on_disk"]["objects"], mib(&inv["on_disk"]["bytes"]));
    let arenas = inv["arenas"].as_array().cloned().unwrap_or_default();
    let arena_bytes: f64 = arenas.iter().map(|a| mib(&a["bytes"])).sum();
    println!("  arenas:    {:.1} MiB of arrays and large Python values in {} processes", arena_bytes, arenas.len());
    println!("  reserve:   {:.1} MiB of pre-zeroed pages", mib(&inv["reserve_bytes"]));
    for r in inv["runs"].as_array().into_iter().flatten() {
        println!("  run {}: {} objects, {:.1} MiB in memory, {:.1} MiB on disk",
            r["run"].as_str().unwrap_or(""), r["objects"], mib(&r["memory_bytes"]), mib(&r["disk_bytes"]));
    }
    for o in inv["largest"].as_array().into_iter().flatten() {
        println!("  {:>10.1} MiB  {}  ({})", mib(&o["bytes"]), o["object"].as_str().unwrap_or(""), o["where"].as_str().unwrap_or(""));
    }
}

/// Groups (the directory, 0, and the data groups) no live peer reports leading.
fn leaderless(s: &pb::StatusReply) -> Vec<u32> {
    let led: std::collections::BTreeSet<u32> =
        s.nodes.iter().filter(|n| n.alive).flat_map(|n| n.leading.iter().copied()).collect();
    (0..=s.data_groups).filter(|g| !led.contains(g)).collect()
}

async fn status(mut c: Client, json: bool) -> Result<u8, String> {
    let s = c.status(pb::StatusRequest::default()).await.map_err(|e| e.message().to_string())?.into_inner();
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
                "nodes": s.nodes.iter().map(|n| serde_json::json!({
                    "addr": n.addr,
                    "alive": n.alive,
                    "cpus": n.cpus,
                    "cpu_used": n.cpu_used,
                    "queued": n.queued,
                    "leading": n.leading,
                    "this_peer": n.this_peer,
                })).collect::<Vec<_>>(),
                "data_groups": s.data_groups,
                "groups": s.groups.iter().map(|g| serde_json::json!({
                    "group": g.group,
                    "leader": g.leader,
                    "leading": g.leading,
                    "voters": g.voters,
                    "last_log": g.last_log,
                    "applied": g.applied,
                    "max_lag": g.leading.then_some(g.max_lag),
                })).collect::<Vec<_>>(),
                "leaderless_groups": leaderless(&s),
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
        for r in &s.pending_reasons {
            println!("  waiting: {r}");
        }
        if s.nodes.len() > 1 {
            println!("  peers:");
            for n in &s.nodes {
                let leads = if n.leading.is_empty() {
                    String::new()
                } else {
                    format!(" · leads {}", n.leading.iter().map(u32::to_string).collect::<Vec<_>>().join(", "))
                };
                let state = if n.alive { "" } else { " · unreachable" };
                println!(
                    "    {}{} · {:.1}/{:.1} CPU in use · {} queued{leads}{state}",
                    n.addr,
                    if n.this_peer { " (this peer)" } else { "" },
                    n.cpu_used,
                    n.cpus,
                    n.queued
                );
            }
        }
        if s.data_groups > 0 {
            println!("  groups this peer holds a replica of ({} data groups in the cluster):", s.data_groups);
            for g in &s.groups {
                let leader = match (g.leading, g.leader.as_str()) {
                    (true, _) => format!("led here, voters behind by at most {} entries", g.max_lag),
                    (false, "") => "no leader".to_string(),
                    (false, addr) => format!("leader {addr}"),
                };
                let name = if g.group == 0 { "0 (directory)".to_string() } else { g.group.to_string() };
                println!("    {name} · {leader} · {} voters · log {}, applied {}", g.voters, g.last_log, g.applied);
            }
            let missing = leaderless(&s);
            if !missing.is_empty() {
                let list: Vec<String> = missing.iter().map(u32::to_string).collect();
                println!(
                    "  no live peer leads group {}: work whose run lives there waits until a majority of its voters is back",
                    list.join(", ")
                );
            }
        }
    }
    Ok(0)
}

fn state_name(s: pb::run_record::State) -> &'static str {
    match s {
        pb::run_record::State::Active => "active",
        pb::run_record::State::Finished => "finished",
        pb::run_record::State::Cancelled => "cancelled",
    }
}

/// "42s", "5m", "3h", "2d".
fn age(ms: u64) -> String {
    let s = ms / 1000;
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m", s / 60),
        3600..86400 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86400),
    }
}

async fn runs(mut c: Client, limit: u32, json: bool) -> Result<u8, String> {
    let r = c.list_runs(pb::ListRunsRequest { limit }).await.map_err(|e| e.message().to_string())?.into_inner();
    let now = chimaera_peer::router::now_ms();
    if json {
        let runs: Vec<_> = r
            .runs
            .iter()
            .map(|run| {
                serde_json::json!({
                    "run": Id::from_slice(&run.run).map(|i| i.hex()).unwrap_or_default(),
                    "state": state_name(run.state()),
                    "detached": run.detached,
                    "tenant": run.tenant,
                    "created_ms": chimaera_core::hlc::wall_ms(run.created_hlc),
                    "finished_ms": (run.finished_hlc > 0).then(|| chimaera_core::hlc::wall_ms(run.finished_hlc)),
                    "command": run.description,
                })
            })
            .collect();
        println!("{}", serde_json::json!({"runs": runs, "unavailable_groups": r.unavailable}));
    } else {
        println!("{:<32}  {:<9}  {:>4}  COMMAND", "RUN", "STATE", "AGE");
        for run in &r.runs {
            let tenant = if run.tenant == "default" { String::new() } else { format!("  [tenant {}]", run.tenant) };
            println!(
                "{:<32}  {:<9}  {:>4}  {}{}{}",
                Id::from_slice(&run.run).map(|i| i.hex()).unwrap_or_default(),
                state_name(run.state()),
                age(now.saturating_sub(chimaera_core::hlc::wall_ms(run.created_hlc))),
                run.description,
                if run.detached { "  (detached)" } else { "" },
                tenant
            );
        }
    }
    if !r.unavailable.is_empty() {
        let groups: Vec<String> = r.unavailable.iter().map(u32::to_string).collect();
        eprintln!("no replica of group {} answered: their runs are missing above", groups.join(", "));
    }
    Ok(0)
}

async fn cancel(mut c: Client, run: &str) -> Result<u8, String> {
    let run = parse_run(run)?;
    let state = |r: pb::ExplainReply| r.run.map(|rec| rec.state());
    let explain = pb::ExplainRequest { run: run.to_vec(), ..Default::default() };
    match state(c.explain(explain.clone()).await.map_err(|e| e.message().to_string())?.into_inner()) {
        None => return Err(format!("run {} is not known to this cluster", run.hex())),
        Some(pb::run_record::State::Active) => {}
        Some(s) => {
            println!("run {} already {}", run.hex(), state_name(s));
            return Ok(0);
        }
    }
    let end = pb::ClientMsg { msg: Some(pb::client_msg::Msg::EndRun(pb::EndRun { run: run.to_vec(), cancel: true })), ..Default::default() };
    c.session(tokio_stream::iter([end])).await.map_err(|e| e.message().to_string())?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let reply = c.explain(explain.clone()).await.map_err(|e| e.message().to_string())?.into_inner();
        if let Some(s) = state(reply).filter(|s| *s != pb::run_record::State::Active) {
            println!("run {} {}", run.hex(), state_name(s));
            return Ok(0);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("run {} is still active 30 s after the cancel was sent", run.hex()))
}

async fn inspect(mut c: Client, run: &str, json: bool) -> Result<u8, String> {
    let run = parse_run(run)?;
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

async fn explain(mut c: Client, run: &str, task: Option<String>, decisions: bool, json: bool) -> Result<u8, String> {
    let run = parse_run(run)?;
    let task = task
        .map(|t| Id::parse_hex(&t).ok_or_else(|| format!("{t:?} is not a task id")))
        .transpose()?
        .map(|t| pb::TaskKey { run: run.to_vec(), task: t.to_vec() });
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
    let mut c = connect_local(dir).await?;
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
    match chimaera_peer::durability::fsync_p50_us(dir, 50) {
        // Cluster peers sync every raft commit; a call needs about three.
        Ok(us) => println!("fsync p50 {us:.0} µs in {} (cluster peers pay this per commit)", dir.display()),
        Err(e) => println!("fsync probe failed in {}: {e}", dir.display()),
    }
    0
}
