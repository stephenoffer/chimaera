//! `chimaera up NAME` / `chimaera down NAME`: peers on SSH hosts, from the
//! cluster profile file the Python client reads (`$CHIMAERA_CLUSTERS`, else
//! `~/.config/chimaera/clusters.toml`):
//!
//! ```toml
//! [clusters.lab]
//! provider = "ssh"
//! hosts = ["gpu1", "gpu2", "gpu3"]                    # ssh destinations; peers dial these names
//! peers = ["gpu1:7710", "gpu2:7710", "gpu3:7710"]   # where drivers connect (the client port)
//! # optional: dir, port (7700), client_port (port + 10), ssh_user, remote_bin
//! ```
//!
//! Uses the system `ssh` and `scp`, so `~/.ssh/config` applies.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const MAX_FOUNDERS: usize = 5;
const READY_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, PartialEq)]
pub struct Profile {
    pub name: String,
    pub hosts: Vec<String>,
    /// Whether the profile lists `peers`, the client addresses drivers use.
    pub has_peers: bool,
    /// Data directory on each host; `~/` is the remote home.
    pub dir: String,
    pub port: u16,
    pub client_port: u16,
    pub ssh_user: Option<String>,
    /// The binary on each host; when unset, `up` copies this one there.
    pub remote_bin: Option<String>,
    /// The cluster token on this machine, shared with the Python client.
    pub token_file: PathBuf,
}

pub fn profile_path() -> PathBuf {
    match std::env::var_os("CHIMAERA_CLUSTERS") {
        Some(p) => PathBuf::from(p),
        None => home().join(".config/chimaera/clusters.toml"),
    }
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| ".".into()))
}

/// The `[clusters.NAME]` table of a profile file's text.
pub fn parse(text: &str, name: &str, source: &str) -> Result<Profile, String> {
    let doc = toml_edit::Document::parse(text).map_err(|e| format!("{source}: {e}"))?;
    let Some(spec) = doc.as_item().get("clusters").and_then(|c| c.get(name)) else {
        return Err(format!("no [clusters.{name}] in {source}"));
    };
    let bad = |key: &str, want: &str| format!("{source}: clusters.{name}.{key} must be {want}");
    let string = |key: &str| -> Result<Option<String>, String> {
        spec.get(key).map(|v| v.as_str().map(String::from).ok_or_else(|| bad(key, "a string"))).transpose()
    };
    let number = |key: &str| -> Result<Option<u16>, String> {
        spec.get(key)
            .map(|v| v.as_integer().and_then(|n| u16::try_from(n).ok()).ok_or_else(|| bad(key, "a port number")))
            .transpose()
    };
    let provider = string("provider")?.unwrap_or_default();
    if provider != "ssh" {
        return Err(format!(
            "cluster {name:?} has provider {provider:?}; `chimaera up` starts provider = \"ssh\" clusters \
             (Kubernetes: `chimaera k8s render`; Slurm: `chimaera slurm-launch`)"
        ));
    }
    let hosts: Vec<String> = match spec.get("hosts").and_then(|h| h.as_array()) {
        Some(list) => list.iter().map(|h| h.as_str().map(String::from)).collect::<Option<_>>(),
        None => None,
    }
    .ok_or_else(|| bad("hosts", "a list of host names"))?;
    if hosts.is_empty() || hosts.iter().any(|h| h.is_empty() || h.starts_with('-')) {
        return Err(bad("hosts", "a non-empty list of host names"));
    }
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) {
        return Err(format!("cluster name {name:?} may hold only letters, digits, '-', '_' and '.'"));
    }
    let port = number("port")?.unwrap_or(chimaera_core::provider::DEFAULT_PORT);
    let client_port = match number("client_port")? {
        Some(p) => p,
        None => port.checked_add(10).ok_or_else(|| bad("port", "at most 65525 when client_port is not set"))?,
    };
    Ok(Profile {
        name: name.to_string(),
        hosts,
        has_peers: spec.get("peers").is_some(),
        dir: string("dir")?.unwrap_or_else(|| format!("~/.chimaera/peers/{name}")),
        port,
        client_port,
        ssh_user: string("ssh_user")?,
        remote_bin: string("remote_bin")?,
        token_file: string("token_file")?
            .map_or_else(|| home().join(".chimaera/clusters").join(name).join("token"), PathBuf::from),
    })
}

fn load(name: &str) -> Result<Profile, String> {
    let path = profile_path();
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    parse(&text, name, &path.display().to_string())
}

/// `s` as one word for a POSIX shell.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// A remote path for the shell, with a leading `~/` as the remote home.
fn remote_path(p: &str) -> String {
    match p.strip_prefix("~/") {
        Some(rest) => format!("\"$HOME\"/{}", sh_quote(rest)),
        None => sh_quote(p),
    }
}

/// Where `up` copies this binary on each host, relative to the remote home.
const COPIED_BIN: &str = ".chimaera/bin/chimaera";

impl Profile {
    /// Sets `D` (data directory) and `B` (binary) for a remote script.
    fn vars(&self) -> String {
        let bin = self.remote_bin.clone().unwrap_or_else(|| format!("~/{COPIED_BIN}"));
        format!("D={}; B={}; ", remote_path(&self.dir), remote_path(&bin))
    }

    fn ssh(&self, host: &str, script: &str) -> Command {
        let mut c = Command::new("ssh");
        c.args(["-o", "BatchMode=yes"]);
        if let Some(u) = &self.ssh_user {
            c.arg("-o").arg(format!("User={u}"));
        }
        c.arg("--").arg(host).arg(format!("{}{script}", self.vars()));
        c
    }

    /// Run `script` on `host`; its stdout, or an error with its stderr.
    fn run(&self, host: &str, script: &str, stdin: Option<&str>) -> Result<String, String> {
        let mut cmd = self.ssh(host, script);
        cmd.stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("cannot run ssh: {e}"))?;
        if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
            use std::io::Write;
            pipe.write_all(input.as_bytes()).map_err(|e| format!("{host}: {e}"))?;
        }
        let out = child.wait_with_output().map_err(|e| format!("{host}: {e}"))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            Err(format!("{host}: {}", String::from_utf8_lossy(&out.stderr).trim()))
        }
    }

    /// `ready`, `starting` (launched, not serving yet) or `down`.
    fn probe(&self, host: &str) -> Result<String, String> {
        self.run(
            host,
            r#""$B" --dir "$D" status >/dev/null 2>&1 && echo ready && exit
               kill -0 "$(cat "$D/launch.pid" 2>/dev/null)" 2>/dev/null && echo starting || echo down"#,
            None,
        )
    }

    fn copy_binary(&self, host: &str, exe: &Path) -> Result<(), String> {
        self.run(host, &format!("mkdir -p \"$HOME\"/{}", sh_quote(".chimaera/bin")), None)?;
        let mut scp = Command::new("scp");
        scp.args(["-q", "-o", "BatchMode=yes"]);
        if let Some(u) = &self.ssh_user {
            scp.arg("-o").arg(format!("User={u}"));
        }
        let out = scp
            .arg("--")
            .arg(exe)
            .arg(format!("{host}:{COPIED_BIN}.new"))
            .output()
            .map_err(|e| format!("cannot run scp: {e}"))?;
        if !out.status.success() {
            return Err(format!("{host}: copying {}: {}", exe.display(), String::from_utf8_lossy(&out.stderr).trim()));
        }
        // Renamed into place, so a binary a running peer uses is never rewritten.
        self.run(host, r#"chmod 755 "$B.new" && mv -f "$B.new" "$B""#, None).map(drop)
    }

    fn start(&self, host: &str, seeds: &str, token: &str) -> Result<(), String> {
        self.run(host, r#"umask 077 && mkdir -p "$D" && cat > "$D/token""#, Some(token))?;
        let args = [
            "--cluster",
            &self.name,
            "--listen",
            &format!("0.0.0.0:{}", self.port),
            "--advertise",
            &format!("{host}:{}", self.port),
            "--seeds",
            seeds,
            "--client-listen",
            &format!("0.0.0.0:{}", self.client_port),
        ]
        .map(sh_quote)
        .join(" ");
        // Only the peer itself goes to the background, with every stream
        // redirected: a backgrounded `cd && ...` list would be a subshell that
        // holds ssh's stdout open and whose pid `$!` would name.
        let script = format!(
            r#"cd "$D" || exit 1
               nohup "$B" --dir "$D" peer start --provider static --token-file "$D/token" {args} >> "$D/peer.log" 2>&1 < /dev/null &
               echo $! > "$D/launch.pid""#
        );
        self.run(host, &script, None).map(drop)
    }
}

/// The cluster token on this machine (the profile's `token_file`, else
/// `~/.chimaera/clusters/NAME/token`), made on first use. Drivers read the same file.
fn token(path: &Path) -> Result<String, String> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    if let Ok(t) = std::fs::read_to_string(path) {
        return Ok(t.trim().to_string());
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    let t = super::new_token().map_err(|e| format!("cannot make a cluster token: {e}"))?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    std::io::Write::write_all(&mut f, format!("{t}\n").as_bytes()).map_err(|e| format!("{}: {e}", path.display()))?;
    eprintln!("chimaera: new cluster token in {}", path.display());
    Ok(t)
}

/// Start a peer on every host that is not running one, and wait until all serve.
pub fn up(name: &str) -> Result<u8, String> {
    let p = load(name)?;
    let token = token(&p.token_file)?;
    let seeds = p.hosts.iter().take(MAX_FOUNDERS).map(|h| format!("{h}:{}", p.port)).collect::<Vec<_>>().join(",");
    let exe = std::env::current_exe().map_err(|e| format!("cannot find this binary: {e}"))?;
    for host in &p.hosts {
        if p.probe(host)? != "down" {
            eprintln!("chimaera: {host}: a peer is already running");
            continue;
        }
        if p.remote_bin.is_none() {
            p.copy_binary(host, &exe)?;
        }
        p.start(host, &seeds, &token)?;
        eprintln!("chimaera: {host}: started");
    }
    let started = Instant::now();
    let mut waiting: Vec<&String> = p.hosts.iter().collect();
    while !waiting.is_empty() {
        let mut still = Vec::new();
        for host in waiting {
            match p.probe(host)?.as_str() {
                "ready" => eprintln!("chimaera: {host}: ready"),
                "starting" if started.elapsed() < READY_TIMEOUT => still.push(host),
                state => {
                    let log = p.run(host, r#"tail -n 20 "$D/peer.log""#, None).unwrap_or_else(|e| e);
                    let hint = if p.remote_bin.is_none() {
                        format!(
                            "\n(the copied binary is built for {}-{}; set remote_bin if the hosts differ)",
                            std::env::consts::OS,
                            std::env::consts::ARCH
                        )
                    } else {
                        String::new()
                    };
                    return Err(format!("{host}: the peer is {state}; its log ends:\n{log}{hint}"));
                }
            }
        }
        waiting = still;
        std::thread::sleep(Duration::from_millis(500));
    }
    let clients: Vec<String> = p.hosts.iter().map(|h| format!("\"{h}:{}\"", p.client_port)).collect();
    if !p.has_peers {
        eprintln!(
            "chimaera: add peers = [{}] to [clusters.{name}] so ch.init(cluster={name:?}) can reach it",
            clients.join(", ")
        );
    }
    println!("cluster {name}: {} peers up; drivers connect to {}", p.hosts.len(), clients.join(", "));
    Ok(0)
}

/// Stop the peer on every host (their data directories stay for the next `up`).
pub fn down(name: &str) -> Result<u8, String> {
    let p = load(name)?;
    let mut failed = false;
    for host in &p.hosts {
        let script = r#"pid=$(cat "$D/launch.pid" 2>/dev/null)
            if ! "$B" --dir "$D" stop >/dev/null 2>&1; then echo "not running"; exit; fi
            i=0; while kill -0 "$pid" 2>/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
            kill -0 "$pid" 2>/dev/null && echo "still running" || echo stopped"#;
        match p.run(host, script, None) {
            Ok(state) => {
                failed |= state == "still running";
                eprintln!("chimaera: {host}: {state}");
            }
            Err(e) => {
                failed = true;
                eprintln!("chimaera: {e}");
            }
        }
    }
    Ok(u8::from(failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_parse_with_defaults_and_refuse_what_up_cannot_start() {
        let text = r#"
default = "lab"
[clusters.dev]
peers = ["10.0.0.1:7710"]
pools = [
  { name = "cpu", replicas = 5 },
]
[clusters.lab]
provider = "ssh"
hosts = ["h1", "h2", "h3"]
peers = ["h1:7710"]
[clusters.big]
provider = "ssh"
hosts = ["a"]
dir = "/scratch/c"
port = 9000
ssh_user = "ops"
remote_bin = "/opt/chimaera"
"#;
        let lab = parse(text, "lab", "t").unwrap();
        assert_eq!(lab.hosts, ["h1", "h2", "h3"]);
        assert_eq!((lab.port, lab.client_port, lab.dir.as_str()), (7700, 7710, "~/.chimaera/peers/lab"));
        assert!(lab.has_peers && lab.ssh_user.is_none() && lab.remote_bin.is_none());
        let big = parse(text, "big", "t").unwrap();
        assert_eq!((big.port, big.client_port, big.dir.as_str()), (9000, 9010, "/scratch/c"));
        assert_eq!((big.ssh_user.as_deref(), big.remote_bin.as_deref()), (Some("ops"), Some("/opt/chimaera")));
        assert!(!big.has_peers);

        assert!(parse(text, "dev", "t").unwrap_err().contains("provider"));
        assert!(parse(text, "nope", "t").unwrap_err().contains("no [clusters.nope]"));
        assert!(parse("x = [", "lab", "t").is_err());
        for bad in [
            r#"hosts = []"#,
            r#"hosts = ["-oProxyCommand=x"]"#,
            r#"hosts = "h1""#,
            r#"hosts = ["h"]
port = 70000"#,
        ] {
            let t = format!("[clusters.c]\nprovider = \"ssh\"\n{bad}\n");
            assert!(parse(&t, "c", "t").is_err(), "{bad}");
        }
        let t = "[clusters.\"a b\"]\nprovider = \"ssh\"\nhosts = [\"h\"]\n";
        assert!(parse(t, "a b", "t").unwrap_err().contains("cluster name"));
    }

    #[test]
    fn remote_scripts_quote_every_profile_value() {
        assert_eq!(sh_quote("it's"), r"'it'\''s'");
        assert_eq!(remote_path("~/a b/c"), r#""$HOME"/'a b/c'"#);
        assert_eq!(remote_path("/x;rm -rf /"), "'/x;rm -rf /'");
        let p = parse("[clusters.c]\nprovider = \"ssh\"\nhosts = [\"h\"]\n", "c", "t").unwrap();
        assert_eq!(p.vars(), r#"D="$HOME"/'.chimaera/peers/c'; B="$HOME"/'.chimaera/bin/chimaera'; "#);
    }
}
