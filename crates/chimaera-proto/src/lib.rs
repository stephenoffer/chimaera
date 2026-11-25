//! Wire and log schemas for Chimaera, generated from `proto/chimaera/v1`.
//!
//! Every message that crosses a process boundary or is stored in a Raft log is
//! defined there, so peers of adjacent versions can decode each other's data.

pub mod frame;

/// Version of the peer and client wire protocol. Peers negotiate the minimum
/// of both sides; a log command type is proposed only once the cluster's
/// committed feature level includes it.
pub const WIRE_VERSION: u32 = 1;

/// What a peer's replicated state machine and peer-to-peer RPCs understand.
/// Peers refuse mesh calls from a peer at another level: every replica of a
/// group must apply its log with the same code. Raise it with any change to
/// shard commands, their effects, or mesh messages; an upgrade then restarts
/// every peer onto the new release (docs/deploy.md, "Upgrading").
pub const PEER_LEVEL: u32 = 1;

/// Which build this is: `<release>-<schema digest>`, where the release is
/// the id `make wheel` gives one wheel's peer binary and extension module
/// (`dev` for other builds) and the digest covers every .proto compiled in.
/// A wheel whose two halves disagree is refused before it is published.
pub const BUILD_ID: &str = env!("CHIMAERA_BUILD_ID");

#[allow(clippy::all, clippy::pedantic)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/chimaera.v1.rs"));
}

pub use prost::Message;

/// How drivers and the CLI reach a peer: over TCP with the cluster token
/// (and TLS), or over the local peer's socket.
#[cfg(feature = "grpc")]
pub mod client {
    use std::time::Duration;
    use tonic::metadata::{Ascii, MetadataValue};
    use tonic::service::interceptor::InterceptedService;
    use tonic::transport::Channel;

    pub type PeerClient = crate::v1::peer_client::PeerClient<InterceptedService<Channel, Auth>>;

    /// Adds the cluster token to every call (TCP clients; the local socket needs none).
    #[derive(Clone)]
    pub struct Auth(Option<MetadataValue<Ascii>>);

    impl Auth {
        pub fn bearer(token: Option<&str>) -> Result<Auth, String> {
            token
                .map(|t| format!("Bearer {}", t.trim()).parse())
                .transpose()
                .map(Auth)
                .map_err(|_| "the cluster token must be printable ASCII".to_string())
        }
    }

    impl tonic::service::Interceptor for Auth {
        fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
            if let Some(v) = &self.0 {
                req.metadata_mut().insert("authorization", v.clone());
            }
            Ok(req)
        }
    }

    /// A channel to a peer: `tcp://host:port` (TLS when `ca` names the cluster's
    /// ca.pem, verifying the name every peer certificate carries) or a Unix socket path.
    pub async fn channel(address: &str, ca: Option<&str>) -> Result<Channel, String> {
        let Some(hostport) = address.strip_prefix("tcp://") else {
            return crate::uds::connect(address.to_string()).await.map_err(|e| with_causes(&e));
        };
        let scheme = if ca.is_some() { "https" } else { "http" };
        let mut ep = tonic::transport::Endpoint::from_shared(format!("{scheme}://{hostport}"))
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(10));
        if let Some(ca) = ca {
            let pem = std::fs::read(ca).map_err(|e| format!("{ca}: {e}"))?;
            let tls = tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(pem))
                .domain_name("chimaera-peer");
            ep = ep.tls_config(tls).map_err(|e| e.to_string())?;
        }
        ep.connect().await.map_err(|e| with_causes(&e))
    }

    /// `e` and the errors under it: tonic's own message is only "transport
    /// error", and the cause (the OS error of a refused connect(), a TLS
    /// failure) is what says what to do.
    fn with_causes(e: &dyn std::error::Error) -> String {
        let mut text = e.to_string();
        let mut cause = e.source();
        while let Some(c) = cause {
            let more = c.to_string();
            if !text.contains(&more) {
                text = format!("{text}: {more}");
            }
            cause = c.source();
        }
        text
    }

    /// A client of the peer at `address` (as for [`channel`]) presenting `token`.
    pub async fn connect(address: &str, token: Option<&str>, ca: Option<&str>) -> Result<PeerClient, String> {
        let auth = Auth::bearer(token)?;
        let ch = channel(address, ca).await?;
        Ok(crate::v1::peer_client::PeerClient::with_interceptor(ch, auth).max_decoding_message_size(256 << 20))
    }

    #[cfg(test)]
    mod tests {
        #[tokio::test]
        async fn a_failed_connect_names_its_os_error() {
            let path = format!("/tmp/chimaera-no-peer-{}.sock", std::process::id());
            let err = super::channel(&path, None).await.unwrap_err();
            assert!(err.starts_with("transport error: ") && err.contains("(os error 2)"), "{err}");
        }
    }
}

/// gRPC over a Unix domain socket (local peers).
#[cfg(feature = "grpc")]
pub mod uds {
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};
    use tonic::transport::{Channel, Endpoint, Uri};

    /// Longest socket path used as is; `sockaddr_un` holds 104 bytes on macOS.
    const MAX_SOCKET_PATH: usize = 100;

    /// Where the socket `name` of the peer whose data directory is `dir`
    /// lives: inside `dir` when the path fits in a socket address, else in
    /// [`fallback_dir`], named by a hash of the canonical directory. The peer,
    /// the CLI and the Python client all call this one function.
    pub fn socket_path(dir: &Path, name: &str) -> PathBuf {
        let inside = dir.join(name);
        if inside.as_os_str().len() <= MAX_SOCKET_PATH {
            return inside;
        }
        let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let tag = blake3::hash(canonical.as_os_str().as_bytes()).to_hex();
        fallback_dir().join(format!("{}-{name}", &tag[..16]))
    }

    /// `/tmp/chimaera-<uid>`: sockets of peers whose data directory is too deep.
    pub fn fallback_dir() -> PathBuf {
        // SAFETY: getuid has no preconditions and cannot fail.
        let uid = unsafe { libc::getuid() };
        PathBuf::from(format!("/tmp/chimaera-{uid}"))
    }

    /// Make sure the directory holding `socket` exists. The shared fallback
    /// directory is created private to this user, and refused if anyone else
    /// owns it or can enter it (another user could otherwise plant a socket).
    pub fn prepare(socket: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        let Some(parent) = socket.parent() else { return Ok(()) };
        if parent != fallback_dir() {
            return std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::DirBuilder::new().mode(0o700).create(parent)
            && e.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(e);
        }
        let meta = std::fs::symlink_metadata(parent)?;
        // SAFETY: getuid has no preconditions and cannot fail.
        let mine = meta.uid() == unsafe { libc::getuid() };
        if !meta.is_dir() || !mine || meta.mode() & 0o077 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("{} must be a directory owned by this user with mode 0700", parent.display()),
            ));
        }
        Ok(())
    }

    /// Why this process may not make a pair of connected Unix sockets, if it
    /// may not. tokio's I/O driver makes one (for signals) when a runtime is
    /// built and panics without it, so the peer and the Python client check
    /// first and report this, naming the call and the remedy, instead.
    pub fn pair_refused() -> Option<std::io::Error> {
        std::os::unix::net::UnixStream::pair().err().map(pair_error)
    }

    fn pair_error(e: std::io::Error) -> std::io::Error {
        let remedy = match e.raw_os_error() {
            Some(libc::EPERM | libc::EACCES) => {
                ". A sandbox or seccomp profile refuses Unix sockets here: allow them for chimaera"
            }
            _ => "",
        };
        std::io::Error::new(e.kind(), format!("socketpair() of Unix sockets, which chimaera needs: {e}{remedy}"))
    }

    pub async fn connect(path: impl Into<PathBuf>) -> Result<Channel, tonic::transport::Error> {
        let path = path.into();
        // The URI is ignored by the connector; it only has to parse.
        Endpoint::from_static("http://local.peer")
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tokio::net::UnixStream::connect(path).await?))
                }
            }))
            .await
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        #[test]
        fn a_short_data_dir_keeps_its_socket_whatever_its_mode() {
            // Regression: a 0755 data directory under /tmp was refused.
            let dir = PathBuf::from(format!("/tmp/chimaera-test-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            let sock = socket_path(&dir, "peer.sock");
            assert_eq!(sock, dir.join("peer.sock"));
            let r = prepare(&sock);
            std::fs::remove_dir_all(&dir).unwrap();
            r.unwrap();
        }

        #[test]
        fn a_refused_socket_pair_names_the_call_and_the_remedy() {
            assert!(pair_refused().is_none(), "socket pairs work here");
            let msg = pair_error(std::io::Error::from_raw_os_error(libc::EPERM)).to_string();
            assert!(msg.starts_with("socketpair() of Unix sockets, which chimaera needs: "), "{msg}");
            assert!(msg.ends_with("allow them for chimaera"), "{msg}");
        }

        #[test]
        fn a_deep_data_dir_puts_its_socket_in_the_private_fallback() {
            let deep = std::env::temp_dir().join("d".repeat(120));
            let sock = socket_path(&deep, "peer.sock");
            assert_eq!(sock.parent().unwrap(), fallback_dir());
            assert!(sock.as_os_str().len() <= MAX_SOCKET_PATH);
            assert_eq!(sock, socket_path(&deep, "peer.sock"), "the rule is deterministic");
            prepare(&sock).unwrap();
            let mode = std::fs::metadata(fallback_dir()).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0);
        }
    }
}
