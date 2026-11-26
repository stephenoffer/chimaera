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

pub(crate) fn free_bytes(p: &Path) -> Option<u64> {
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

/// Where objects in memory spill to: the directories in `CHIMAERA_SPILL_DIR`
/// when set (colon-separated, like PATH; any filesystem: larger local disks,
/// or a network mount such as NFS, EFS or FSx, like Ray's filesystem
/// spilling), each in a directory of this peer's own so peers can share
/// them; else `<dir>/spill`. Spilled objects go to each in turn, and are read
/// by mapping their file there, wherever that is.
pub fn spill_dirs(dir: &Path, peer_hex: &str) -> Vec<PathBuf> {
    let own = format!("chimaera-{}", &peer_hex[..16]);
    let listed: Vec<PathBuf> = std::env::var_os("CHIMAERA_SPILL_DIR")
        .map(|v| std::env::split_paths(&v).filter(|p| !p.as_os_str().is_empty()).map(|p| p.join(&own)).collect())
        .unwrap_or_default();
    if listed.is_empty() { vec![dir.join("spill")] } else { listed }
}

/// How many of a store's largest objects an inventory lists when asked for
/// `n`: ten for 0, and never more than 10,000 (Ray's `list_objects` cap).
pub fn listed(n: u32) -> usize {
    if n == 0 { 10 } else { (n as usize).min(10_000) }
}

/// What a peer's object store holds, from its directory (what `ray memory`
/// reports for Ray's store): objects in memory and on disk (spilled, or
/// replicas of other peers' objects), per run and the `keep` largest; the
/// NumPy arenas of live processes; the pre-zeroed reserve.
pub fn inventory(objects: &Path, keep: usize) -> serde_json::Value {
    let (mut mem, mut disk) = ((0u64, 0u64), (0u64, 0u64));
    let mut runs = Vec::new();
    let mut largest: Vec<(u64, String, &'static str)> = Vec::new();
    for run in std::fs::read_dir(objects).into_iter().flatten().flatten() {
        let name = run.file_name().to_string_lossy().into_owned();
        if !is_object_name(&name) {
            continue;
        }
        let (mut rm, mut rd, mut n) = (0u64, 0u64, 0u64);
        for e in std::fs::read_dir(run.path()).into_iter().flatten().flatten() {
            let obj = e.file_name().to_string_lossy().into_owned();
            let Ok(link) = e.metadata() else { continue }; // does not follow symlinks
            if !is_object_name(&obj) {
                continue; // a temporary or a partial transfer
            }
            let on_disk = link.file_type().is_symlink();
            let size = if on_disk { std::fs::metadata(e.path()).map_or(0, |m| m.len()) } else { link.len() };
            n += 1;
            if on_disk {
                rd += size;
                disk = (disk.0 + 1, disk.1 + size);
            } else {
                rm += size;
                mem = (mem.0 + 1, mem.1 + size);
            }
            largest.push((size, format!("{name}/{obj}"), if on_disk { "disk" } else { "memory" }));
        }
        runs.push(serde_json::json!({"run": name, "objects": n, "memory_bytes": rm, "disk_bytes": rd}));
    }
    largest.sort_by(|a, b| b.0.cmp(&a.0));
    largest.truncate(keep);
    let arenas: Vec<serde_json::Value> = std::fs::read_dir(objects.join(".arena"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            serde_json::json!({"pid": e.file_name().to_string_lossy(), "bytes": crate::stores::dir_bytes(&e.path())})
        })
        .collect();
    serde_json::json!({
        "object_dir": objects.display().to_string(),
        "huge_pages": huge_tmpfs_mounts(&std::fs::read_to_string("/proc/self/mounts").unwrap_or_default())
            .iter().any(|m| objects.starts_with(m)),
        "in_memory": {"objects": mem.0, "bytes": mem.1},
        "on_disk": {"objects": disk.0, "bytes": disk.1},
        "arenas": arenas,
        "reserve_bytes": crate::stores::dir_bytes(&objects.join(".reserve")),
        "runs": runs,
        "largest": largest.iter().map(|(s, p, w)| serde_json::json!({"object": p, "bytes": s, "where": w})).collect::<Vec<_>>(),
    })
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

/// Who owns an object directory in a shared root (/dev/shm, a huge-page
/// tmpfs): `<pid>\n<data dir>\n`, rewritten at each start.
const OWNER: &str = ".owner";

/// Clear what dead peers left in the shared root that holds `objects`. A
/// peer whose data directory is gone will not come back (a test cluster
/// cleaned up, a node removed): its whole object directory goes. A peer
/// that is only not running may restart and reclaim its objects, so only
/// its pre-zeroed reserve, which is filler, goes. Directories with no owner
/// record, or whose owner still runs, are left alone. Returns (directories
/// removed, reserves freed).
pub fn sweep_peers(objects: &Path) -> (usize, usize) {
    let (Some(root), Some(own)) = (objects.parent(), objects.file_name()) else { return (0, 0) };
    if !own.to_string_lossy().starts_with("chimaera-") {
        return (0, 0); // in the peer's own data directory: no neighbours
    }
    let (mut dirs, mut reserves) = (0, 0);
    for e in std::fs::read_dir(root).into_iter().flatten().flatten() {
        let name = e.file_name();
        if name == own || !name.to_string_lossy().starts_with("chimaera-") {
            continue;
        }
        let Ok(owner) = std::fs::read_to_string(e.path().join(OWNER)) else { continue };
        let mut lines = owner.lines();
        let (Some(pid), Some(data)) = (lines.next().and_then(|p| p.parse::<i32>().ok()), lines.next()) else {
            continue;
        };
        if pid_alive(pid) {
            continue;
        }
        if !Path::new(data).exists() {
            if std::fs::remove_dir_all(e.path()).is_ok() {
                dirs += 1;
            }
        } else if std::fs::remove_dir_all(e.path().join(".reserve")).is_ok() {
            reserves += 1;
        }
    }
    (dirs, reserves)
}

/// Reserve classes: files of 2^class bytes (256 KiB .. 1 GiB), the same
/// classes writers ask for (chimaera-py's objects.rs).
const MIN_CLASS: u32 = 18;
const MAX_CLASS: u32 = 30;
/// A class gets files only once it missed this many times within MISS_WINDOW:
/// a one-off large array is not worth idle pre-zeroed memory, which the pool
/// counts as used.
const MISSES_TO_FILL: usize = 2;
const MISS_WINDOW: Duration = Duration::from_secs(10);
/// A class nobody asked for in this long gives back one file (large classes
/// sooner: their idle pages cost more).
const DECAY: Duration = Duration::from_secs(60);
const DECAY_LARGE: Duration = Duration::from_secs(10);
const LARGE_CLASS: u32 = 26;

/// Pre-zeroed tmpfs files that writers take by rename, so a new object or
/// NumPy array starts on pages that are already allocated and zeroed: no
/// page faults where the copy or the computation happens. The kernel's
/// zeroing (the bulk of a fresh page's cost) moves to this thread.
///
/// Demand-driven: a writer that finds no file of its class leaves a
/// `want-<class>-...` marker per miss. A class that misses MISSES_TO_FILL
/// times within MISS_WINDOW gets its target doubled (at least one file), and
/// targets decay by one file per idle DECAY (DECAY_LARGE for large classes).
/// The whole reserve stays under `budget`.
pub struct Reserve {
    dir: PathBuf,
    budget: u64,
    target: [u32; 32],
    last_want: [Option<std::time::Instant>; 32],
    misses: [std::collections::VecDeque<std::time::Instant>; 32],
    seq: u64,
}

impl Reserve {
    pub fn new(dir: PathBuf, budget: u64) -> Reserve {
        let _ = std::fs::create_dir_all(&dir);
        // Files a previous peer left are kept; half-filled ones are dropped.
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten().filter(|e| e.file_name().to_string_lossy().starts_with(".fill-")) {
                let _ = std::fs::remove_file(e.path());
            }
        }
        Reserve { dir, budget, target: [0; 32], last_want: [None; 32], misses: Default::default(), seq: 0 }
    }

    /// One round: read wants and stock, decay idle classes, fill what is
    /// missing (smallest classes first) and drop what is over target.
    pub fn tick(&mut self) {
        let now = std::time::Instant::now();
        let mut stock: [Vec<PathBuf>; 32] = Default::default();
        let Ok(rd) = std::fs::read_dir(&self.dir) else { return };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(k) = name.strip_prefix("want-").and_then(|r| r.split('-').next()?.parse::<u32>().ok()) {
                let _ = std::fs::remove_file(e.path());
                if (MIN_CLASS..=MAX_CLASS).contains(&k) {
                    let i = k as usize;
                    let recent = &mut self.misses[i];
                    recent.push_back(now);
                    while recent.front().is_some_and(|t| now.duration_since(*t) > MISS_WINDOW) {
                        recent.pop_front();
                    }
                    if recent.len() >= MISSES_TO_FILL {
                        let t = &mut self.target[i];
                        *t = (*t * 2).clamp(1, (self.budget / class_bytes(k)).max(1) as u32);
                    }
                    self.last_want[i] = Some(now);
                }
            } else if let Some(k) = name.split_once('-').and_then(|(k, _)| k.parse::<u32>().ok())
                && (MIN_CLASS..=MAX_CLASS).contains(&k)
            {
                stock[k as usize].push(e.path());
            }
        }
        for k in MIN_CLASS..=MAX_CLASS {
            let i = k as usize;
            let decay = if k >= LARGE_CLASS { DECAY_LARGE } else { DECAY };
            if self.target[i] > 0 && self.last_want[i].is_none_or(|t| now.duration_since(t) >= decay) {
                self.target[i] -= 1;
                self.last_want[i] = Some(now);
            }
            while stock[i].len() > self.target[i] as usize {
                let _ = std::fs::remove_file(stock[i].pop().expect("non-empty"));
            }
        }
        // What is missing, smallest classes first, within the budget; filled
        // by FILLERS threads at once (zeroing pages is the kernel's work, and
        // one thread cannot keep up with a burst of new objects).
        let mut bytes: u64 = (MIN_CLASS..=MAX_CLASS).map(|k| stock[k as usize].len() as u64 * class_bytes(k)).sum();
        let mut todo: Vec<(u32, u64)> = Vec::new();
        for k in MIN_CLASS..=MAX_CLASS {
            let mut have = stock[k as usize].len();
            while have < self.target[k as usize] as usize && bytes + class_bytes(k) <= self.budget {
                self.seq += 1;
                todo.push((k, self.seq));
                have += 1;
                bytes += class_bytes(k);
            }
        }
        let dir = &self.dir;
        for batch in todo.chunks(FILLERS) {
            std::thread::scope(|s| {
                for &(k, seq) in batch {
                    s.spawn(move || {
                        if let Err(e) = fill(dir, k, seq) {
                            tracing::debug!("reserve: cannot fill a 2^{k} file: {e}");
                        }
                    });
                }
            });
        }
    }
}

/// Threads filling the reserve at once.
const FILLERS: usize = 8;

/// Create `<dir>/<k>-<seq>`: 2^k bytes of allocated, zeroed pages.
fn fill(dir: &Path, k: u32, seq: u64) -> std::io::Result<PathBuf> {
    {
        use std::os::fd::AsRawFd;
        let tmp = dir.join(format!(".fill-{k}-{seq}"));
        let done = dir.join(format!("{k}-{seq}"));
        let len = class_bytes(k) as usize;
        let r = (|| {
            let f = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&tmp)?;
            f.set_len(len as u64)?; // first: a huge-page tmpfs then allocates huge pages
            // SAFETY: a fresh shared mapping of our own file, unmapped below.
            unsafe {
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    f.as_raw_fd(),
                    0,
                );
                if p == libc::MAP_FAILED {
                    return Err(std::io::Error::last_os_error());
                }
                const MADV_POPULATE_WRITE: libc::c_int = 23;
                let rc = libc::madvise(p, len, MADV_POPULATE_WRITE);
                let err = std::io::Error::last_os_error();
                if rc != 0 && err.raw_os_error() == Some(libc::EINVAL) {
                    // Before Linux 5.14: touch each page instead. The space was
                    // checked by the caller's budget against a tmpfs that has room.
                    for off in (0..len).step_by(4096) {
                        std::ptr::write_volatile((p as *mut u8).add(off), 0);
                    }
                } else if rc != 0 {
                    libc::munmap(p, len);
                    return Err(err);
                }
                libc::munmap(p, len);
            }
            std::fs::rename(&tmp, &done)
        })();
        if r.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        r.map(|()| done)
    }
}

fn fs_size(p: &Path) -> Option<u64> {
    let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: statvfs fills `st` for a NUL-terminated path.
    #[allow(clippy::unnecessary_cast)]
    (unsafe { libc::statvfs(c.as_ptr(), &mut st) } == 0).then(|| st.f_blocks as u64 * st.f_frsize as u64)
}

/// Bytes of objects the in-memory store may hold before writers put new
/// objects straight on disk: `CHIMAERA_MEMORY_POOL` when set, else 90% of
/// the object filesystem.
pub fn memory_limit(objects: &Path) -> u64 {
    std::env::var("CHIMAERA_MEMORY_POOL")
        .ok()
        .and_then(|v| crate::budget::parse_size(&v))
        .unwrap_or_else(|| fs_size(objects).map_or(0, |b| b / 10 * 9))
}

/// Publish the bytes of objects in memory (the core measures them each
/// second) to `<objects>/.usage` as (sequence, bytes), little-endian u64s.
/// Writers compare it, plus what they wrote since, with the memory limit:
/// the filesystem's own usage counts everyone else's files too.
pub fn publish_usage(objects: &Path, bytes: u64) {
    use std::os::unix::fs::FileExt;
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&seq.to_le_bytes());
    buf[8..].copy_from_slice(&bytes.to_le_bytes());
    if let Ok(f) = std::fs::OpenOptions::new().write(true).create(true).truncate(false).open(objects.join(".usage")) {
        let _ = f.write_all_at(&buf, 0);
    }
}

/// Start the object plane of an in-memory store at `objects` whose spill
/// directories (on disk) are `spill`: tell writers where things are (the
/// `.plane` file, read by chimaera-py's _objects), keep the page reserve
/// filled, and sweep the arenas of exited processes and what dead peers left
/// beside it (see `sweep_peers`). A store on disk (`spill` empty) only gets
/// the sweeping. `data` is this peer's data directory, recorded as the owner.
pub fn start(objects: PathBuf, spill: Vec<PathBuf>, data: &Path) {
    let data = std::fs::canonicalize(data).unwrap_or_else(|_| data.to_path_buf());
    let owner = format!("{}\n{}\n", std::process::id(), data.display());
    if let Err(e) = crate::stores::write_atomic(&objects.join(OWNER), owner.as_bytes()) {
        tracing::warn!("object plane: cannot write {}: {e}", objects.join(OWNER).display());
    }
    let mut reserve = None;
    if let Ok(spill) = std::env::join_paths(&spill)
        && !spill.is_empty()
    {
        let limit = memory_limit(&objects);
        let dir = objects.join(".reserve");
        let plane = format!("reserve={}\nspill={}\nlimit={limit}\n", dir.display(), spill.to_string_lossy());
        if let Err(e) = crate::stores::write_atomic(&objects.join(".plane"), plane.as_bytes()) {
            tracing::warn!("object plane: cannot write {}: {e}", objects.join(".plane").display());
        }
        // Small: the reserve is memory the pool counts, so idle pre-zeroed pages
        // must not crowd out calls; larger objects fault their pages in.
        reserve = Some(Reserve::new(dir, (2u64 << 30).min(limit / 16)));
    }
    let _ = std::thread::Builder::new().name("chimaera-objplane".into()).spawn(move || {
        let mut last_sweep = std::time::Instant::now() - Duration::from_secs(60);
        loop {
            if last_sweep.elapsed() >= Duration::from_secs(10) {
                last_sweep = std::time::Instant::now();
                let n = sweep_arenas(&objects);
                if n > 0 {
                    tracing::info!("removed the shared-memory arenas of {n} exited processes");
                }
                let (dirs, reserves) = sweep_peers(&objects);
                if dirs + reserves > 0 {
                    tracing::info!(
                        "freed shared memory of dead peers: {dirs} object directories whose peer is gone, \
                         {reserves} reserves of peers not running"
                    );
                }
            }
            if let Some(r) = reserve.as_mut() {
                r.tick();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

/// Bytes in a reserve file of class `k`: 2^k plus 1/64 of it (the same
/// formula as chimaera-py's objects.rs), so a power-of-two payload plus its
/// header fits its own class.
fn class_bytes(k: u32) -> u64 {
    (1u64 << k) + (1u64 << k >> 6).max(4096)
}

/// How long a new object waits before it is replicated (see watch):
/// `CHIMAERA_REPLICA_DELAY_MS`, default 0. At once, there is no window in
/// which a dead peer takes a `put` value with it (a call's result can be made
/// again, a put cannot). A wait lets a reader on another peer fetch it first,
/// which then counts as the copy and saves a transfer.
fn replica_delay() -> Duration {
    static AT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *AT.get_or_init(|| {
        let ms = std::env::var("CHIMAERA_REPLICA_DELAY_MS").ok().and_then(|v| v.parse::<u64>().ok());
        Duration::from_millis(ms.filter(|&m| m <= 60_000).unwrap_or(0))
    })
}

fn is_object_name(name: &str) -> bool {
    name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Replicate every object this peer stores to other peers (see the
/// Replicate RPC and replicas::targets), so a lost peer loses no data: a
/// consumer whose holder is gone finds the copy on another live member.
/// Objects are seen through inotify as they appear in the store: renamed
/// into place from a writer's `.tmp` name, or hard-linked from a NumPy arena.
/// Copies fetched from other peers (`.part` names), spills and objects their
/// writer marked (a durability class other than the default) are not
/// replicated. The copy lags the write by one transfer.
/// `CHIMAERA_OBJECT_REPLICAS=N` keeps N copies, in other failure domains
/// first (default 1); 0 turns this off.
pub fn replicate_new_objects(
    objects: PathBuf,
    mesh: std::sync::Arc<crate::mesh::MeshClient>,
    rt: tokio::runtime::Handle,
) {
    if replica_count() == 0 {
        return;
    }
    let _ = std::thread::Builder::new().name("chimaera-replicas".into()).spawn(move || {
        #[cfg(target_os = "linux")]
        let watched = watch(&objects, &mesh, &rt);
        #[cfg(not(target_os = "linux"))]
        let watched = poll(&objects, &mesh, &rt);
        if let Err(e) = watched {
            tracing::warn!("object replicas are off: cannot watch {}: {e}", objects.display());
        }
    });
}

/// Where there is no inotify (macOS): list the store every 250 ms and
/// replicate the objects that appeared since, but not copies that arrived
/// from other peers (the mesh records those) and not spills (symlinks).
/// Objects already there when it starts are left alone, as with watch.
#[cfg(not(target_os = "linux"))]
fn poll(
    objects: &Path,
    mesh: &std::sync::Arc<crate::mesh::MeshClient>,
    rt: &tokio::runtime::Handle,
) -> std::io::Result<()> {
    use std::collections::HashSet;
    std::fs::read_dir(objects)?; // the store exists
    let mut seen: Option<HashSet<(String, String)>> = None;
    loop {
        let mut now = HashSet::new();
        for run in std::fs::read_dir(objects).into_iter().flatten().flatten() {
            let run_name = run.file_name().to_string_lossy().into_owned();
            if !is_object_name(&run_name) {
                continue;
            }
            for e in std::fs::read_dir(run.path()).into_iter().flatten().flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if crate::replicas::note_marker(&run.path(), &name) {
                    continue; // its object gets no copy here (see replicas)
                }
                // A symlink is a spilled object or a replica of another peer's.
                if !is_object_name(&name) || e.file_type().map_or(true, |t| t.is_symlink()) {
                    continue;
                }
                let key = (run_name.clone(), name);
                if let Some(seen) = seen.as_ref()
                    && !seen.contains(&key)
                    && !decode_hex(&key.1).is_some_and(|o| mesh.arrived(&o))
                {
                    replicate_one(&run.path(), &key.1, mesh, rt);
                }
                now.insert(key);
            }
        }
        seen = Some(now);
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[cfg(target_os = "linux")]
fn watch(
    objects: &Path,
    mesh: &std::sync::Arc<crate::mesh::MeshClient>,
    rt: &tokio::runtime::Handle,
) -> std::io::Result<()> {
    use std::collections::HashMap;
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: inotify syscalls on paths we own; the event buffer is parsed
    // within the length read() returned.
    let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let add = |p: &Path, mask: u32| -> i32 {
        let c = std::ffi::CString::new(p.as_os_str().as_bytes()).expect("no NUL in paths");
        unsafe { libc::inotify_add_watch(fd, c.as_ptr(), mask) }
    };
    const RUN_MASK: u32 = libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_MOVED_FROM;
    let root = add(objects, libc::IN_CREATE);
    if root < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut runs: HashMap<i32, PathBuf> = HashMap::new();
    for e in std::fs::read_dir(objects)?.flatten() {
        if is_object_name(&e.file_name().to_string_lossy()) {
            let wd = add(&e.path(), RUN_MASK);
            if wd >= 0 {
                runs.insert(wd, e.path());
            }
        }
    }
    // Rename sources by cookie: true when the old name was a writer's temporary.
    let mut moved: HashMap<u32, bool> = HashMap::new();
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        let mut off = 0usize;
        let head = std::mem::size_of::<libc::inotify_event>();
        while off + head <= n as usize {
            // SAFETY: the kernel wrote a whole event (header + name) here.
            let ev: libc::inotify_event = unsafe { std::ptr::read_unaligned(buf.as_ptr().add(off).cast()) };
            let raw = &buf[off + head..off + head + ev.len as usize];
            let name =
                String::from_utf8_lossy(&raw[..raw.iter().position(|&b| b == 0).unwrap_or(raw.len())]).into_owned();
            off += head + ev.len as usize;
            if ev.mask & libc::IN_IGNORED != 0 {
                runs.remove(&ev.wd);
                continue;
            }
            if ev.wd == root {
                if ev.mask & libc::IN_ISDIR != 0 && is_object_name(&name) {
                    let dir = objects.join(&name);
                    let wd = add(&dir, RUN_MASK);
                    if wd >= 0 {
                        runs.insert(wd, dir.clone());
                    }
                    // Objects linked or renamed in before the watch existed
                    // raised no event: replicate what is already there.
                    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                        let f = e.file_name().to_string_lossy().into_owned();
                        if is_object_name(&f) {
                            replicate_one(&dir, &f, mesh, rt);
                        }
                    }
                }
                continue;
            }
            let Some(run_dir) = runs.get(&ev.wd) else { continue };
            if ev.mask & libc::IN_CREATE != 0 && crate::replicas::note_marker(run_dir, &name) {
                continue; // its object gets no copy here (see replicas)
            }
            if ev.mask & libc::IN_MOVED_FROM != 0 {
                if moved.len() > 4096 {
                    moved.clear(); // unpaired cookies (moves out of the directory)
                }
                moved.insert(ev.cookie, name.contains(".tmp"));
                continue;
            }
            let local = if ev.mask & libc::IN_MOVED_TO != 0 {
                moved.remove(&ev.cookie) == Some(true)
            } else {
                ev.mask & libc::IN_CREATE != 0 && ev.mask & libc::IN_ISDIR == 0 // a hard link from an arena
            };
            if local && is_object_name(&name) {
                replicate_one(run_dir, &name, mesh, rt);
            }
        }
    }
}

/// Copy `<run_dir>/<name>` to the peers that hold this peer's copies
/// (replicas::targets) after replica_delay(), unless its writer marked it to
/// have none (another durability class), another peer fetched it meanwhile
/// (that copy suffices) or it was freed. The grace keeps a replica from
/// competing with a reader on another peer for this peer's bandwidth.
fn replicate_one(
    run_dir: &Path,
    name: &str,
    mesh: &std::sync::Arc<crate::mesh::MeshClient>,
    rt: &tokio::runtime::Handle,
) {
    if crate::replicas::kept_local(run_dir, name) {
        return;
    }
    let path = run_dir.join(name);
    let (Some(run), Some(object)) =
        (run_dir.file_name().and_then(|r| decode_hex(&r.to_string_lossy())), decode_hex(name))
    else {
        return;
    };
    let Ok(size) = std::fs::metadata(&path).map(|m| m.len()) else { return };
    let nodes = crate::replicas::targets(mesh, mesh.me.node_id, replica_count());
    if nodes.is_empty() {
        return;
    }
    let mesh = mesh.clone();
    rt.spawn(async move {
        tokio::time::sleep(replica_delay()).await;
        // At most REPLICAS_AT_ONCE objects being copied; the rest wait their turn.
        let Ok(_turn) = replica_turns().acquire().await else { return };
        if tokio::fs::metadata(&path).await.is_err() {
            return;
        }
        // A peer that fetched it already holds one copy.
        let want = nodes.len() - usize::from(mesh.was_served(&object));
        for &node in &nodes[..want] {
            let checkpoint = chimaera_core::pb::TrafficClass::TrafficCheckpoint;
            if let Err(e) = mesh.replicate(node, &run, &object, size, true, checkpoint).await {
                crate::counters::REPLICATION_FAILURES.add(1);
                tracing::warn!("no replica of {} on node {node}: {e}", path.display());
            }
        }
    });
}

/// Copies of each object on other peers (`CHIMAERA_OBJECT_REPLICAS`, default
/// 1; 0 turns background replicas off), in other failure domains first.
pub(crate) fn replica_count() -> usize {
    std::env::var("CHIMAERA_OBJECT_REPLICAS").ok().and_then(|v| v.parse().ok()).unwrap_or(1)
}

/// Replicas being copied at once, from one peer.
pub(crate) const REPLICAS_AT_ONCE: usize = 8;

fn replica_turns() -> &'static tokio::sync::Semaphore {
    static TURNS: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    TURNS.get_or_init(|| tokio::sync::Semaphore::new(REPLICAS_AT_ONCE))
}

pub(crate) fn decode_hex(s: &str) -> Option<Vec<u8>> {
    s.len()
        .is_multiple_of(2)
        .then(|| (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok()).collect())
        .flatten()
}

/// Before this peer leaves (a drain): give every object it holds a copy on
/// the first peer that holds its copies (replicas::targets: other failure
/// domains first, among live peers that are not draining), which is where
/// readers look once a holder is gone. Each copy is waited for, eight
/// at a time, until `deadline`. A peer that already has an object
/// answers at once. Returns (objects with a copy there, objects left behind).
pub async fn hand_off(
    objects: &crate::stores::ObjectStore,
    mesh: &std::sync::Arc<crate::mesh::MeshClient>,
    deadline: std::time::Instant,
) -> (u64, u64) {
    let held = held_objects(objects.dir());
    let total = held.len() as u64;
    let Some(&node) = crate::replicas::targets(mesh, mesh.me.node_id, 1).first() else {
        if total > 0 {
            tracing::warn!("drain: no live peer to hand {total} objects to; they leave with this peer");
        }
        return (0, total);
    };
    let turns = std::sync::Arc::new(tokio::sync::Semaphore::new(REPLICAS_AT_ONCE));
    let mut copies = tokio::task::JoinSet::new();
    for (run, object, size) in held {
        let (mesh, turns) = (mesh.clone(), turns.clone());
        copies.spawn(async move {
            let _turn = turns.acquire_owned().await.ok()?;
            let left = deadline.checked_duration_since(std::time::Instant::now())?;
            let recovery = chimaera_core::pb::TrafficClass::TrafficRecovery;
            tokio::time::timeout(left, mesh.replicate(node, &run, &object, size, true, recovery)).await.ok()?.ok()
        });
    }
    let mut copied = 0;
    while let Some(r) = copies.join_next().await {
        copied += u64::from(matches!(r, Ok(Some(()))));
    }
    if copied < total {
        tracing::warn!(
            "drain: {} of {total} objects have no copy on node {node} (out of time, or the copy failed)",
            total - copied
        );
    }
    (copied, total - copied)
}

/// (run, object id, size) of every complete object in the store at `dir`:
/// in memory or spilled (a link to the disk copy), not partial files or the
/// plane's own directories.
fn held_objects(dir: &Path) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
    let mut out = Vec::new();
    for run in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let Some(run_id) = decode_hex(&run.file_name().to_string_lossy()) else { continue };
        for e in std::fs::read_dir(run.path()).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !is_object_name(&name) {
                continue;
            }
            let Ok(m) = std::fs::metadata(e.path()) else { continue }; // follows a spilled object's link
            out.push((run_id.clone(), decode_hex(&name).expect("checked"), m.len()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_dead_peers_leave_in_a_shared_root_is_freed_but_a_restartable_peers_objects_stay() {
        let (root, data) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let me = std::process::id();
        let peer = |name: &str, owner: Option<String>| {
            let d = root.path().join(name);
            std::fs::create_dir_all(d.join(".reserve")).unwrap();
            std::fs::create_dir_all(d.join("run")).unwrap();
            std::fs::write(d.join("run/obj"), b"x").unwrap();
            if let Some(o) = owner {
                std::fs::write(d.join(OWNER), o).unwrap();
            }
            d
        };
        let own = peer("chimaera-own", Some("999999999\n/nonexistent\n".to_string()));
        let gone = peer("chimaera-gone", Some("999999999\n/nonexistent/peer\n".into()));
        let resting = peer("chimaera-resting", Some(format!("999999999\n{}\n", data.path().display())));
        let live = peer("chimaera-live", Some(format!("{me}\n/nonexistent/peer\n")));
        let unknown = peer("chimaera-unknown", None);
        assert_eq!(sweep_peers(&own), (1, 1));
        assert!(!gone.exists(), "its data directory is gone: it will not come back");
        assert!(!resting.join(".reserve").exists() && resting.join("run/obj").exists(), "may restart: objects stay");
        assert!(live.join(".reserve").exists() && unknown.join(".reserve").exists(), "running, or unknown: kept");
        assert!(own.join(".reserve").exists(), "never its own");
        assert_eq!(sweep_peers(&data.path().join("objects")), (0, 0), "a store in its own data dir has no neighbours");
    }

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
    fn a_drain_hands_off_complete_objects_in_memory_and_spilled() {
        let (d, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let run = d.path().join("ab".repeat(16));
        std::fs::create_dir_all(&run).unwrap();
        std::fs::write(run.join("11".repeat(16)), vec![1u8; 300]).unwrap();
        std::fs::write(disk.path().join("spilled"), vec![2u8; 500]).unwrap();
        std::os::unix::fs::symlink(disk.path().join("spilled"), run.join("22".repeat(16))).unwrap();
        std::fs::write(run.join(format!("{}.part-0", "33".repeat(16))), b"partial").unwrap();
        std::fs::create_dir_all(d.path().join(".arena/1")).unwrap();
        std::fs::write(d.path().join(".arena/1/0"), b"arena").unwrap();
        let mut held = held_objects(d.path());
        held.sort();
        assert_eq!(held, vec![(vec![0xab; 16], vec![0x11; 16], 300), (vec![0xab; 16], vec![0x22; 16], 500)]);
    }

    #[test]
    fn the_inventory_tells_memory_from_disk_and_skips_partial_files() {
        let (mem, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let run = "aa".repeat(16);
        let (a, b) = ("01".repeat(16), "02".repeat(16));
        std::fs::create_dir_all(mem.path().join(&run)).unwrap();
        std::fs::write(mem.path().join(&run).join(&a), vec![0u8; 3000]).unwrap();
        std::fs::write(disk.path().join(&b), vec![0u8; 5000]).unwrap();
        std::os::unix::fs::symlink(disk.path().join(&b), mem.path().join(&run).join(&b)).unwrap();
        std::fs::write(mem.path().join(&run).join(format!("{a}.tmp1-2")), b"partial").unwrap();
        let inv = inventory(mem.path(), listed(0));
        assert_eq!(inv["in_memory"]["bytes"], 3000);
        assert_eq!(inv["on_disk"]["bytes"], 5000);
        assert_eq!(inv["runs"][0]["objects"], 2, "the temporary is not an object");
        assert_eq!(inv["largest"][0]["where"], "disk");
        assert_eq!(inventory(mem.path(), 1)["largest"].as_array().map(Vec::len), Some(1), "as many as asked");
        assert_eq!((listed(0), listed(3), listed(u32::MAX)), (10, 3, 10_000));
    }

    #[test]
    fn the_reserve_follows_demand_within_its_budget() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join(".reserve");
        let mut r = Reserve::new(dir.clone(), 9 << 20);
        let want = |k: u32, n: u32| std::fs::write(dir.join(format!("want-{k}-1-{n}")), b"").unwrap();
        let files = || -> Vec<String> {
            let mut v: Vec<String> =
                std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
            v.sort();
            v
        };
        r.tick();
        assert!(files().is_empty(), "nothing until someone asks");
        want(21, 0);
        r.tick();
        assert!(files().is_empty(), "one miss is a one-off: no idle pages for it");
        want(21, 1);
        r.tick();
        let f = files();
        assert_eq!(f.len(), 1, "a second miss within the window fills one 2 MiB file: {f:?}");
        assert!(f[0].starts_with("21-"));
        assert_eq!(std::fs::metadata(dir.join(&f[0])).unwrap().len(), class_bytes(21));
        for n in 2..6 {
            want(21, n);
            r.tick();
        }
        assert_eq!(files().len(), 4, "doubling stops at the 9 MiB budget (four 2 MiB files)");
        want(26, 0);
        want(26, 1);
        r.tick();
        assert!(!files().iter().any(|n| n.starts_with("26-")), "a 64 MiB file does not fit the 9 MiB budget");
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
