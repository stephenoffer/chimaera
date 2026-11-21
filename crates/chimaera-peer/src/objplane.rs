//! Where this host's objects live, and the housekeeping of the same-host
//! object plane.
//!
//! Objects are files. In memory (tmpfs) they are shared by every process on
//! the host with no copies; a tmpfs mounted with huge pages (`huge=always` or
//! `huge=within_size`) makes mapping them about a hundred times cheaper than
//! with 4 KiB pages, so one is preferred when present. Writers allocate NumPy
//! arrays as files under `<objects>/.arena/<pid>/` (see chimaera-py's
//! objects.rs); a process that dies leaves its arena behind, and the sweeper
//! removes it. Stored objects link those files, so sweeping never loses one.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Smallest in-memory filesystem worth using for objects; containers often
/// mount a 64 MiB /dev/shm.
pub const MIN_SHM_BYTES: u64 = 2 << 30;

fn free_bytes(p: &Path) -> Option<u64> {
    let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: statvfs fills `st` for a NUL-terminated path.
    // Field widths differ by platform (u32 vs u64), hence the casts.
    #[allow(clippy::unnecessary_cast)]
    (unsafe { libc::statvfs(c.as_ptr(), &mut st) } == 0).then(|| st.f_bavail as u64 * st.f_frsize as u64)
}

fn writable(p: &Path) -> bool {
    let Ok(c) = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()) else { return false };
    // SAFETY: access(2) on a NUL-terminated path.
    unsafe { libc::access(c.as_ptr(), libc::W_OK | libc::X_OK) == 0 }
}

/// Writable tmpfs mounts with huge pages enabled, from a mounts table
/// (`/proc/self/mounts` format), most free space first.
pub fn huge_tmpfs_mounts(table: &str) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = table
        .lines()
        .filter_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 4 || f[2] != "tmpfs" {
                return None;
            }
            let huge = f[3].split(',').any(|o| o == "huge=always" || o == "huge=within_size");
            let path = PathBuf::from(f[1].replace("\\040", " "));
            (huge && writable(&path)).then_some(path)
        })
        .filter_map(|p| free_bytes(&p).filter(|b| *b >= MIN_SHM_BYTES).map(|b| (b, p)))
        .collect();
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

/// This peer's object directory: under `CHIMAERA_OBJECT_DIR` when set; else a
/// huge-page tmpfs if one is mounted; else /dev/shm when it has room (Linux);
/// else `<dir>/objects` on disk.
pub fn object_dir(dir: &Path, peer_hex: &str) -> PathBuf {
    let name = format!("chimaera-{}", &peer_hex[..16]);
    if let Some(base) = std::env::var_os("CHIMAERA_OBJECT_DIR").filter(|v| !v.is_empty()) {
        return PathBuf::from(base).join(name);
    }
    if cfg!(target_os = "linux") {
        if let Some(m) =
            std::fs::read_to_string("/proc/self/mounts").ok().and_then(|t| huge_tmpfs_mounts(&t).into_iter().next())
        {
            return m.join(name);
        }
        let shm = Path::new("/dev/shm");
        if shm.is_dir() && free_bytes(shm).is_some_and(|b| b >= MIN_SHM_BYTES) {
            return shm.join(name);
        }
    }
    dir.join("objects")
}

fn pid_alive(pid: i32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    pid > 0
        && (unsafe { libc::kill(pid, 0) } == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM))
}

/// Remove the arena directories of processes that no longer exist. Returns
/// how many were removed.
pub fn sweep_arenas(objects: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(objects.join(".arena")) else { return 0 };
    let mut n = 0;
    for e in entries.flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<i32>().ok()) else { continue };
        if !pid_alive(pid) && std::fs::remove_dir_all(e.path()).is_ok() {
            n += 1;
        }
    }
    n
}

/// Start the object plane's housekeeping thread for `objects`.
pub fn start(objects: PathBuf) {
    let _ = std::thread::Builder::new().name("chimaera-objplane".into()).spawn(move || {
        loop {
            let n = sweep_arenas(&objects);
            if n > 0 {
                tracing::info!("removed the shared-memory arenas of {n} exited processes");
            }
            std::thread::sleep(Duration::from_secs(10));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arenas_of_dead_processes_are_swept_and_live_ones_kept() {
        let d = tempfile::tempdir().unwrap();
        let me = std::process::id().to_string();
        for name in [me.as_str(), "999999999", "not-a-pid"] {
            std::fs::create_dir_all(d.path().join(".arena").join(name)).unwrap();
            std::fs::write(d.path().join(".arena").join(name).join("0"), b"x").unwrap();
        }
        assert_eq!(sweep_arenas(d.path()), 1);
        assert!(d.path().join(".arena").join(&me).exists(), "a live process keeps its arena");
        assert!(!d.path().join(".arena/999999999").exists());
        assert!(d.path().join(".arena/not-a-pid").exists(), "unknown names are left alone");
    }

    #[test]
    fn only_writable_huge_tmpfs_mounts_with_room_are_chosen() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().display();
        let table = format!(
            "tmpfs /dev/shm tmpfs rw,nosuid,nodev 0 0\n\
             tmpfs {p} tmpfs rw,relatime,size=67108864k,huge=within_size 0 0\n\
             tmpfs /nonexistent/x tmpfs rw,huge=always 0 0\n"
        );
        // The temp dir is on whatever filesystem it is; the parser only sees
        // the table, so it qualifies when it has room and is writable.
        let got = huge_tmpfs_mounts(&table);
        assert!(!got.iter().any(|m| m == Path::new("/dev/shm")), "no huge= option: not chosen");
        assert!(!got.iter().any(|m| m == Path::new("/nonexistent/x")), "missing mount: not chosen");
        if free_bytes(d.path()).is_some_and(|b| b >= MIN_SHM_BYTES) {
            assert_eq!(got, vec![d.path().to_path_buf()]);
        }
    }
}
