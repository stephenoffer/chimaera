//! The checks of `chimaera doctor` that can fail at one step, each naming
//! that step: the Python that runs workers, the Unix socket a local peer
//! listens on, and the cluster a driver here would use. A peer that cannot
//! bind its own sockets reports the call, the path and the remedy the same
//! way ([`socket_error`]).

use crate::target::Target;
use chimaera_core::pb;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// What to do when a Unix socket call fails with `e`, when that is known.
fn socket_remedy(e: &io::Error) -> Option<&'static str> {
    match e.raw_os_error()? {
        libc::EPERM | libc::EACCES => Some(
            "Unix sockets are refused here, by a sandbox or seccomp profile that denies them or by the \
             directory's permissions: allow Unix sockets for chimaera, or set CHIMAERA_HOME (or --dir) to a \
             directory this user can write on a local disk",
        ),
        n if n == libc::EOPNOTSUPP || n == libc::ENOTSUP => Some(
            "this filesystem cannot hold Unix sockets (some network and FUSE mounts cannot): set CHIMAERA_HOME \
             (or --dir) to a directory on a local disk",
        ),
        libc::EAFNOSUPPORT => Some("this environment has no Unix sockets: run chimaera where it may create them"),
        _ => None,
    }
}

/// `e` from `call` (`socket`, `bind`, `connect`) on the Unix socket at
/// `path`, its message naming the call, the path and the remedy.
pub fn socket_error(call: &str, path: &Path, e: io::Error) -> io::Error {
    let remedy = socket_remedy(&e).map(|r| format!(". {r}")).unwrap_or_default();
    io::Error::new(e.kind(), format!("{call}() on the Unix socket {}: {e}{remedy}", path.display()))
}

/// `e` from binding a listener at `path`, which makes a socket and then
/// binds it: named as socket()'s when this process may not make Unix
/// sockets at all (a seccomp profile that denies them), else as bind()'s.
pub fn bind_error(path: &Path, e: io::Error) -> io::Error {
    socket_error(if socket_refused().is_some() { "socket" } else { "bind" }, path, e)
}

/// Why socket(2) refuses this process a Unix socket, if it does.
fn socket_refused() -> Option<io::Error> {
    // SAFETY: socket(2) has no preconditions; the descriptor is closed below.
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Some(io::Error::last_os_error());
    }
    // SAFETY: `fd` is the descriptor socket(2) just returned, closed once.
    unsafe { libc::close(fd) };
    None
}

/// Create, bind and connect to a Unix socket at `path`, as a local peer and
/// its drivers do. The error names the call that failed.
pub fn unix_socket(path: &Path) -> io::Result<()> {
    chimaera_proto::uds::prepare(path).map_err(|e| {
        let dir = path.parent().unwrap_or(path).display();
        io::Error::new(e.kind(), format!("cannot create the socket directory {dir}: {e}"))
    })?;
    if let Some(e) = socket_refused() {
        return Err(socket_error("socket", path, e));
    }
    let _ = std::fs::remove_file(path);
    let listener = std::os::unix::net::UnixListener::bind(path).map_err(|e| socket_error("bind", path, e))?;
    let connected = std::os::unix::net::UnixStream::connect(path).map_err(|e| socket_error("connect", path, e));
    drop(listener);
    let _ = std::fs::remove_file(path);
    connected.map(drop)
}

/// Run with this binary's build id as its argument: prints what it imports,
/// or exits with why it cannot.
const PYTHON_PROBE: &str = r#"
import sys
here = f"python {sys.version.split()[0]} ({sys.executable})"
try:
    import chimaera._native as native
    from chimaera._runtime import _peer_binary
except Exception as e:
    sys.exit(f"{here} cannot import chimaera: {type(e).__name__}: {e}. Install the chimaera wheel into it, "
             "or set CHIMAERA_PYTHON to an interpreter that has it")
build = getattr(native, "BUILD_ID", "unknown")
if build != sys.argv[1]:
    sys.exit(f"{here} imports chimaera._native from {native.__file__}, build {build}, but this binary is build "
             f"{sys.argv[1]}: install both from one wheel, or rebuild both")
try:
    print(f"{here} imports chimaera ({native.__file__}, this binary's build) and starts the peer binary {_peer_binary()}")
except Exception as e:
    sys.exit(f"{here} imports chimaera, but {e}")
"#;

/// Whether the Python the peer falls back to for workers and `chimaera
/// submit` runs (`$CHIMAERA_PYTHON`, else python3) imports chimaera of this
/// binary's build, and which peer binary it would start.
pub async fn python() -> Result<String, String> {
    let python =
        std::env::var("CHIMAERA_PYTHON").ok().filter(|p| !p.trim().is_empty()).unwrap_or_else(|| "python3".into());
    let run = tokio::process::Command::new(&python)
        .args(["-c", PYTHON_PROBE, chimaera_proto::BUILD_ID])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = match tokio::time::timeout(Duration::from_secs(60), run).await {
        Err(_) => return Err(format!("{python} did not finish importing chimaera within 60 s")),
        Ok(Err(e)) => {
            return Err(format!("cannot run {python}: {e}; set CHIMAERA_PYTHON to an interpreter that has chimaera"));
        }
        Ok(Ok(out)) => out,
    };
    let text = |b: &[u8]| String::from_utf8_lossy(b).trim().to_string();
    if out.status.success() { Ok(text(&out.stdout)) } else { Err(text(&out.stderr)) }
}

/// The cluster a driver here would use: `cluster`, else as drivers choose
/// (see [`crate::target`]). None means the local peer.
pub fn selected(cluster: Option<&str>) -> Result<Option<Target>, String> {
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let path = crate::launch::ssh::profile_path();
    let text = std::fs::read_to_string(&path).ok();
    let shown = path.display().to_string();
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| ".".into()));
    crate::target::resolve(cluster, &env, text.as_deref().map(|t| (shown.as_str(), t)), &home)
}

/// One line per address of `t`: whether TCP reaches it, then whether the
/// gRPC handshake (TLS when the profile has a CA) and a Hello carrying the
/// token succeed, naming the first step that fails.
pub async fn reach(t: &Target) -> Vec<String> {
    let mut lines = Vec::new();
    for address in &t.addresses {
        lines.push(format!("{address}: {}", reach_one(address, t).await));
    }
    lines
}

async fn reach_one(address: &str, t: &Target) -> String {
    let hostport = address.trim_start_matches("tcp://");
    match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(hostport)).await {
        Err(_) => return "connect() did not finish within 5 s: a firewall drops it, or no route leads there".into(),
        Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
            return format!("connect(): {e}: no peer listens there for drivers (a peer's --client-listen address)");
        }
        Ok(Err(e)) => return format!("connect(): {e}"),
        Ok(Ok(_)) => {}
    }
    let handshake = |e: &str| match t.ca {
        Some(_) => format!("TCP connects, but the TLS handshake fails: {e}"),
        None => format!(
            "TCP connects, but the gRPC handshake fails ({e}); a cluster with TLS needs its ca.pem in the profile"
        ),
    };
    let mut client = match chimaera_proto::client::connect(address, t.token.as_deref(), t.ca.as_deref()).await {
        Ok(c) => c,
        Err(e) => return handshake(&e),
    };
    let hello = pb::ClientHello {
        wire_version: chimaera_proto::WIRE_VERSION,
        client: "chimaera-doctor".into(),
        ..Default::default()
    };
    match client.hello(hello).await {
        Ok(r) => {
            let info = r.into_inner();
            format!("answers: Chimaera {}, wire version {}", info.version, info.wire_version)
        }
        Err(s) if matches!(s.code(), tonic::Code::Unauthenticated | tonic::Code::PermissionDenied) => {
            format!("refuses this driver: {} (check the cluster token or driver credential)", s.message())
        }
        // The peer's own refusal (a newer wire version than it speaks).
        Err(s) if s.code() == tonic::Code::FailedPrecondition => format!("refuses the handshake: {}", s.message()),
        Err(s) => handshake(s.message()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("chimaera-doctor-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_unix_socket_error_names_the_call_the_path_and_the_remedy() {
        let path = Path::new("/somewhere/peer.sock");
        let e = socket_error("bind", path, io::Error::from_raw_os_error(libc::EPERM));
        let msg = e.to_string();
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
        assert!(msg.starts_with("bind() on the Unix socket /somewhere/peer.sock: "), "{msg}");
        assert!(msg.contains("allow Unix sockets for chimaera, or set CHIMAERA_HOME"), "{msg}");
        let other = socket_error("connect", path, io::Error::from_raw_os_error(libc::ECONNRESET)).to_string();
        assert!(other.starts_with("connect() on the Unix socket /somewhere/peer.sock: ") && !other.contains(". "));
        // socket(2) works here, so a listener that failed to bind failed in bind().
        let bound = bind_error(path, io::Error::from_raw_os_error(libc::EACCES)).to_string();
        assert!(bound.starts_with("bind() on the Unix socket /somewhere/peer.sock: "), "{bound}");
    }

    #[test]
    fn the_socket_probe_binds_and_connects_where_it_may() {
        let dir = temp_dir("ok");
        let sock = dir.join("doctor.sock");
        unix_socket(&sock).unwrap();
        assert!(!sock.exists(), "the probe removes its socket");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_socket_probe_names_bind_when_the_directory_refuses_it() {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: geteuid has no preconditions and cannot fail.
        if unsafe { libc::geteuid() } == 0 {
            return; // root writes anywhere: there is no refusal to see
        }
        let dir = temp_dir("refused");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let sock = dir.join("doctor.sock");
        let err = unix_socket(&sock).unwrap_err().to_string();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(err.starts_with(&format!("bind() on the Unix socket {}: ", sock.display())), "{err}");
        assert!(err.contains("allow Unix sockets for chimaera"), "{err}");
    }

    #[tokio::test]
    async fn the_cluster_probe_names_the_step_that_fails() {
        let target = |address: String| Target { name: "lab".into(), addresses: vec![address], token: None, ca: None };
        // Nothing listens: TCP itself is refused.
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let lines = reach(&target(format!("tcp://{free}"))).await;
        assert!(lines[0].starts_with(&format!("tcp://{free}: connect(): ")), "{lines:?}");
        assert!(lines[0].contains("no peer listens there"), "{lines:?}");
        // Something listens but speaks no gRPC: the handshake fails.
        let mute = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mute.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((s, _)) = mute.accept().await {
                drop(s);
            }
        });
        let lines = reach(&target(format!("tcp://{addr}"))).await;
        assert!(lines[0].contains("TCP connects, but the gRPC handshake fails"), "{lines:?}");
    }
}
