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

/// What a peer's object store holds, from its directory (what `ray memory`
/// reports for Ray's store): objects in memory and on disk (spilled, or
/// replicas of other peers' objects), per run and the largest; the NumPy
/// arenas of live processes; the pre-zeroed reserve.
pub fn inventory(objects: &Path) -> serde_json::Value {
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
    largest.truncate(10);
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
/// filled, and sweep the arenas of exited processes. A store on disk
/// (`spill` empty) only gets the sweeping.
pub fn start(objects: PathBuf, spill: Vec<PathBuf>) {
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

/// How long a new object waits before it is replicated (see watch).
#[cfg(target_os = "linux")]
const REPLICA_DELAY: Duration = Duration::from_secs(1);

fn is_object_name(name: &str) -> bool {
    name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Replicate every object this peer stores to its ring successor (see the
/// Replicate RPC), so a lost peer loses no data: a consumer whose holder is
/// gone finds the copy on another live member. Objects are seen through
/// inotify as they appear in the store: renamed into place from a writer's
/// `.tmp` name, or hard-linked from a NumPy arena. Copies fetched from other
/// peers (`.part` names) and spills are not replicated again. The copy lags
/// the write by one transfer. `CHIMAERA_OBJECT_REPLICAS=0` turns this off.
#[cfg(target_os = "linux")]
pub fn replicate_new_objects(
    objects: PathBuf,
    mesh: std::sync::Arc<crate::mesh::MeshClient>,
    rt: tokio::runtime::Handle,
) {
    if std::env::var("CHIMAERA_OBJECT_REPLICAS").as_deref() == Ok("0") {
        return;
    }
    let _ = std::thread::Builder::new().name("chimaera-replicas".into()).spawn(move || {
        if let Err(e) = watch(&objects, &mesh, &rt) {
            tracing::warn!("object replicas are off: cannot watch {}: {e}", objects.display());
        }
    });
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

/// Copy `<run_dir>/<name>` to this peer's successor after REPLICA_DELAY,
/// unless another peer fetched it meanwhile (that copy suffices) or it was
/// freed. The grace keeps a replica from competing with a reader on another
/// peer for this peer's bandwidth.
#[cfg(target_os = "linux")]
fn replicate_one(
    run_dir: &Path,
    name: &str,
    mesh: &std::sync::Arc<crate::mesh::MeshClient>,
    rt: &tokio::runtime::Handle,
) {
    let path = run_dir.join(name);
    let (Some(run), Some(object)) =
        (run_dir.file_name().and_then(|r| decode_hex(&r.to_string_lossy())), decode_hex(name))
    else {
        return;
    };
    let Ok(size) = std::fs::metadata(&path).map(|m| m.len()) else { return };
    let Some(node) = mesh.successor() else { return };
    let mesh = mesh.clone();
    rt.spawn(async move {
        tokio::time::sleep(REPLICA_DELAY).await;
        // At most REPLICAS_AT_ONCE copies in flight; the rest wait their turn.
        let Ok(_turn) = replica_turns().acquire().await else { return };
        if mesh.was_served(&object) || tokio::fs::metadata(&path).await.is_err() {
            return;
        }
        if let Err(e) = mesh.replicate(node, &run, &object, size, true).await {
            crate::counters::REPLICATION_FAILURES.add(1);
            tracing::warn!("no replica of {} on node {node}: {e}", path.display());
        }
    });
}

/// Replicas being copied at once, from one peer.
#[cfg(target_os = "linux")]
const REPLICAS_AT_ONCE: usize = 8;

#[cfg(target_os = "linux")]
fn replica_turns() -> &'static tokio::sync::Semaphore {
    static TURNS: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    TURNS.get_or_init(|| tokio::sync::Semaphore::new(REPLICAS_AT_ONCE))
}

#[cfg(target_os = "linux")]
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    s.len()
        .is_multiple_of(2)
        .then(|| (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok()).collect())
        .flatten()
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
    fn the_inventory_tells_memory_from_disk_and_skips_partial_files() {
        let (mem, disk) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let run = "aa".repeat(16);
        let (a, b) = ("01".repeat(16), "02".repeat(16));
        std::fs::create_dir_all(mem.path().join(&run)).unwrap();
        std::fs::write(mem.path().join(&run).join(&a), vec![0u8; 3000]).unwrap();
        std::fs::write(disk.path().join(&b), vec![0u8; 5000]).unwrap();
        std::os::unix::fs::symlink(disk.path().join(&b), mem.path().join(&run).join(&b)).unwrap();
        std::fs::write(mem.path().join(&run).join(format!("{a}.tmp1-2")), b"partial").unwrap();
        let inv = inventory(mem.path());
        assert_eq!(inv["in_memory"]["bytes"], 3000);
        assert_eq!(inv["on_disk"]["bytes"], 5000);
        assert_eq!(inv["runs"][0]["objects"], 2, "the temporary is not an object");
        assert_eq!(inv["largest"][0]["where"], "disk");
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
