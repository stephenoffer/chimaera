//! `CHIMAERA_HUGE_PAGES=mount`: a peer starting on a Linux host with no
//! huge-page tmpfs mounts one, where `sudo` needs no password, and keeps its
//! objects there (objplane::object_dir). On /dev/shm's 4 KiB pages a value
//! read in another process faults in a page at a time: NumPy and Arrow
//! values of 16 MB to 1 GB read at 0.10-0.73x Ray's speed
//! (bench/objects/RESULTS.md). On huge pages those reads take 512 times
//! fewer faults. Mounting needs root, so it is asked for, never assumed.

use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Command, Stdio};

/// Where it mounts one: the path the provisioner's nodes use too.
pub const MOUNT_POINT: &str = "/mnt/chimaera-objects";

/// Whether `CHIMAERA_HUGE_PAGES` asks for a mount.
pub fn asked() -> bool {
    std::env::var("CHIMAERA_HUGE_PAGES").is_ok_and(|v| v.eq_ignore_ascii_case("mount"))
}

/// Whether `table` (`/proc/self/mounts` format) has a mount at `path`.
pub fn mounted_at(table: &str, path: &Path) -> bool {
    table.lines().filter_map(|l| l.split_whitespace().nth(1)).any(|m| Path::new(&m.replace("\\040", " ")) == path)
}

/// When `CHIMAERA_HUGE_PAGES=mount` asks, mount a huge-page tmpfs at
/// MOUNT_POINT unless something is mounted there already: at most half the
/// RAM, as /dev/shm is (pages are taken only as objects are written), open
/// to every user as /dev/shm is. `sudo -n` never prompts: where sudo wants a
/// password nothing is mounted. Peers starting together take a lock first,
/// so none mounts over another's. True when a mount is there afterwards; on
/// a failure, the log says why and the peer stays on /dev/shm.
pub fn mount_if_asked() -> bool {
    if !asked() {
        return false;
    }
    let lock_path = std::env::temp_dir().join("chimaera-huge-pages.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&lock_path)
        .or_else(|_| std::fs::File::open(&lock_path)); // another user's: a read-only descriptor locks too
    if let Ok(f) = &lock {
        // SAFETY: flock(2) on a descriptor this function owns; released when `lock` drops.
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
    }
    let table = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
    if mounted_at(&table, Path::new(MOUNT_POINT)) {
        return true; // another peer's, or the provisioner's; object_dir checks it is a huge-page tmpfs
    }
    let mounted = sudo(&["mkdir", "-p", MOUNT_POINT]).and_then(|()| {
        sudo(&["mount", "-t", "tmpfs", "-o", "huge=within_size,size=50%,mode=1777", "tmpfs", MOUNT_POINT])
    });
    match &mounted {
        Ok(()) => tracing::info!("CHIMAERA_HUGE_PAGES=mount: mounted a huge-page tmpfs at {MOUNT_POINT}"),
        Err(e) => tracing::warn!("CHIMAERA_HUGE_PAGES=mount: objects stay on /dev/shm's 4 KiB pages: {e}"),
    }
    mounted.is_ok()
}

fn sudo(args: &[&str]) -> Result<(), String> {
    let out = Command::new("sudo")
        .arg("-n")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("cannot run sudo: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!("sudo -n {} failed: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mount_point_is_found_in_the_mounts_table() {
        let table = "tmpfs /dev/shm tmpfs rw,nosuid,nodev 0 0\n\
                     tmpfs /mnt/chimaera-objects tmpfs rw,relatime,size=6291456k,mode=1777,huge=within_size 0 0\n\
                     tmpfs /mnt/with\\040space tmpfs rw 0 0\n";
        assert!(mounted_at(table, Path::new(MOUNT_POINT)));
        assert!(mounted_at(table, Path::new("/mnt/with space")), "octal escapes decoded");
        assert!(!mounted_at(table, Path::new("/mnt")), "a prefix is not a mount");
        assert!(!mounted_at("", Path::new(MOUNT_POINT)));
    }
}
