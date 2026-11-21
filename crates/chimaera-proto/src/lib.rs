//! Wire and log schemas for Chimaera, generated from `proto/chimaera/v1`.
//!
//! Every message that crosses a process boundary or is stored in a Raft log is
//! defined there, so peers of adjacent versions can decode each other's data.

pub mod frame;

/// Version of the peer and client wire protocol. Peers negotiate the minimum
/// of both sides; a log command type is proposed only once the cluster's
/// committed feature level includes it.
pub const WIRE_VERSION: u32 = 1;

#[allow(clippy::all, clippy::pedantic)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/chimaera.v1.rs"));
}

pub use prost::Message;

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
