//! The same-host object plane.
//!
//! - [`write_object`] writes an object file from buffer parts without hashing
//!   them. Large files are populated and filled by several threads at once,
//!   and a full tmpfs is an `ENOSPC` error, never a `SIGBUS`.
//! - The NumPy arena (Linux): arrays of at least `min_bytes` are allocated as
//!   files under the object directory, so storing one (`ch.put`, a task's
//!   return value) links the file instead of copying it. [`arena_claim`]
//!   links the file under the object's name and remaps the owner's pages
//!   copy-on-write, so the owner's later writes never reach the stored object.
//!   Readers map the same pages. Freed, never-stored allocations are kept for
//!   reuse (bounded), as malloc keeps its heap, so temporaries do not fault in
//!   fresh pages each time.

use pyo3::exceptions::{PyEOFError, PyOSError, PyValueError};
use pyo3::prelude::*;
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// Below this a file is written by one thread with plain writes.
const PARALLEL_MIN: usize = 8 << 20;
/// Unit of work for the copying threads.
const CHUNK: usize = 4 << 20;
const MAX_THREADS: usize = 16;

fn os_err(e: io::Error) -> PyErr {
    let errno = e.raw_os_error().unwrap_or(if e.kind() == io::ErrorKind::StorageFull { libc::ENOSPC } else { 0 });
    PyOSError::new_err((errno, e.to_string()))
}

fn tmp_name(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut s = path.as_os_str().to_owned();
    s.push(format!(".tmp{}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
    PathBuf::from(s)
}

fn create_new(path: &Path) -> io::Result<std::fs::File> {
    let open = || std::fs::OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(path);
    match open() {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;
            open()
        }
        r => r,
    }
}

/// Whether `f` is on tmpfs.
#[allow(clippy::unnecessary_cast)] // f_type's width differs by platform
fn in_memory(f: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd;
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: fstatfs fills `st` for a valid descriptor.
    let ok = unsafe { libc::fstatfs(f.as_raw_fd(), &mut st) } == 0;
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    ok && st.f_type as i64 == TMPFS_MAGIC
}

/// Free bytes on the filesystem holding `f`.
#[allow(clippy::unnecessary_cast)] // statvfs field widths differ by platform
fn free_bytes(f: &std::fs::File) -> Option<u64> {
    use std::os::fd::AsRawFd;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: fstatvfs fills `st` for a valid descriptor.
    (unsafe { libc::fstatvfs(f.as_raw_fd(), &mut st) } == 0).then(|| st.f_bavail as u64 * st.f_frsize as u64)
}

/// Reserve `len` bytes of disk for `f` (posix_fallocate; F_PREALLOCATE on
/// macOS), as the peer's stores::preallocate does.
fn preallocate(f: &std::fs::File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: posix_fallocate on a descriptor we own; it returns the error.
        match unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, len as libc::off_t) } {
            0 | libc::EOPNOTSUPP => Ok(()),
            e => Err(io::Error::from_raw_os_error(e)),
        }
    }
    #[cfg(target_os = "macos")]
    {
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        // SAFETY: F_PREALLOCATE reads and fills `store` for a descriptor we own.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (f.as_raw_fd(), len);
        Ok(())
    }
}

fn no_space(path: &Path, want: usize, free: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::StorageFull,
        format!(
            "no space for a {} MiB object in {} ({} MiB free)",
            want >> 20,
            path.parent().unwrap_or(path).display(),
            free >> 20
        ),
    )
}

/// Where this host's object plane keeps things, from the peer (see the peer's
/// objplane.rs): set once per process by [`objects_config`].
#[derive(Default)]
struct Config {
    /// The object directory (tmpfs).
    root: Option<PathBuf>,
    /// Pre-zeroed files, `<class>-<n>`, refilled by the peer.
    reserve: Option<PathBuf>,
    /// Disk directories for objects written while memory is over `limit`,
    /// each in turn.
    spill: Vec<PathBuf>,
    /// Bytes the object filesystem may hold before new objects go to disk.
    limit: u64,
}

static CONFIG: std::sync::RwLock<Option<Config>> = std::sync::RwLock::new(None);

/// Configure the object plane of this process: the object directory, its
/// reserve of pre-zeroed pages, the disk directories (colon-separated) that
/// objects go to when memory is over `limit` bytes (0: no limit).
#[pyfunction]
#[pyo3(signature = (root, reserve=None, spill=None, limit=0))]
pub fn objects_config(root: PathBuf, reserve: Option<PathBuf>, spill: Option<PathBuf>, limit: u64) {
    let spill = spill.map(|s| std::env::split_paths(&s).filter(|p| !p.as_os_str().is_empty()).collect());
    *CONFIG.write().expect("lock") =
        Some(Config { root: Some(root), reserve, spill: spill.unwrap_or_default(), limit });
}

/// Smallest and largest reserve classes: files of 2^class bytes.
pub const RESERVE_MIN_CLASS: u32 = 18;
pub const RESERVE_MAX_CLASS: u32 = 30;

/// Bytes in a reserve file of class `k`: 2^k plus 1/64 of it, so a payload
/// of a power of two plus its header (or an array of exactly 2^k bytes) fits
/// its own class instead of taking a file twice its size. The peer's
/// objplane.rs uses the same formula.
pub fn class_bytes(k: u32) -> usize {
    (1usize << k) + (1usize << k >> 6).max(4096)
}

fn class_of(len: usize) -> u32 {
    let mut k = RESERVE_MIN_CLASS;
    while class_bytes(k) < len && k < usize::BITS - 1 {
        k += 1;
    }
    k
}

/// Reserve files this process has listed and not yet taken: the directory,
/// and its file names by class. Listing a reserve of 2,000 files for every
/// take cost more than the write it served; a listed file that another
/// process took meanwhile fails its rename and the next is tried, and the
/// directory is listed again when the classes asked for run out.
type Listed = (PathBuf, std::collections::HashMap<u32, Vec<std::ffi::OsString>>);
static LISTED: std::sync::Mutex<Option<Listed>> = std::sync::Mutex::new(None);

fn list_reserve(dir: &Path) -> std::collections::HashMap<u32, Vec<std::ffi::OsString>> {
    let mut by_class: std::collections::HashMap<u32, Vec<std::ffi::OsString>> = Default::default();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let n = e.file_name();
        if let Some(class) = n.to_str().and_then(|s| s.split_once('-')).and_then(|(c, _)| c.parse::<u32>().ok()) {
            by_class.entry(class).or_default().push(n); // "want-" markers do not parse
        }
    }
    by_class
}

/// Move a pre-zeroed file of at least `len` bytes (at most twice that) from
/// the reserve to `to`. On a miss, leave a `want-<class>` marker so the peer
/// keeps some of that size from now on.
fn take_reserve(dir: &Path, len: usize, to: &Path) -> bool {
    let k = class_of(len);
    if k > RESERVE_MAX_CLASS {
        return false;
    }
    let mut listed = LISTED.lock().expect("lock");
    let mut fresh = false;
    loop {
        if listed.as_ref().is_none_or(|(d, _)| d != dir) {
            *listed = Some((dir.to_path_buf(), list_reserve(dir)));
            fresh = true;
        }
        let names = &mut listed.as_mut().expect("listed above").1;
        for class in [k, k + 1] {
            while let Some(n) = names.get_mut(&class).and_then(Vec::pop) {
                if std::fs::rename(dir.join(&n), to).is_ok() {
                    return true; // others may race for the same file: the rename decides
                }
            }
        }
        if fresh {
            break;
        }
        *listed = None; // none left of these classes since the last listing
    }
    drop(listed);
    // One marker per miss (the peer fills a class that misses repeatedly).
    static MISSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let name = format!("want-{k}-{}-{n}", std::process::id());
    let _ = std::fs::OpenOptions::new().write(true).create_new(true).open(dir.join(name));
    false
}

/// Bytes in use on the filesystem holding `p`.
fn fs_used(p: &Path) -> Option<u64> {
    let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: statvfs on a NUL-terminated path.
    #[allow(clippy::unnecessary_cast)]
    (unsafe { libc::statvfs(c.as_ptr(), &mut st) } == 0)
        .then(|| (st.f_blocks as u64).saturating_sub(st.f_bfree as u64) * st.f_frsize as u64)
}

/// (sequence of the peer's last `.usage`, bytes this process wrote to memory
/// since): writes between the peer's once-a-second measurements count too.
static WRITTEN: std::sync::Mutex<(u64, u64)> = std::sync::Mutex::new((0, 0));

/// Bytes of objects in memory: the peer's measurement (`<root>/.usage`)
/// plus this process's writes since; the filesystem's usage when the peer
/// publishes none.
fn objects_used(root: &Path) -> Option<u64> {
    let mut buf = [0u8; 16];
    let Ok(f) = std::fs::File::open(root.join(".usage")) else { return fs_used(root) };
    if f.read_exact_at(&mut buf, 0).is_err() {
        return fs_used(root);
    }
    let seq = u64::from_le_bytes(buf[..8].try_into().expect("8 bytes"));
    let bytes = u64::from_le_bytes(buf[8..].try_into().expect("8 bytes"));
    let mut w = WRITTEN.lock().expect("lock");
    if w.0 != seq {
        *w = (seq, 0);
    }
    Some(bytes + w.1)
}

fn note_written(n: u64) {
    WRITTEN.lock().expect("lock").1 += n;
}

/// Whether `len` more bytes of objects in memory would pass the hard cap:
/// the limit plus a quarter. Between the two, new objects still go to memory
/// while the peer's background spiller (which starts at 80% of the pool)
/// moves the oldest to disk; past the cap they go straight to disk.
fn over_limit(len: u64) -> bool {
    let cfg = CONFIG.read().expect("lock");
    cfg.as_ref().is_some_and(|c| {
        c.limit > 0 && c.root.as_ref().and_then(|r| objects_used(r)).is_some_and(|u| u + len > c.limit + c.limit / 4)
    })
}

/// When a write to each spill directory last failed, in this process.
static SPILL_FAILED: std::sync::Mutex<Vec<(PathBuf, std::time::Instant)>> = std::sync::Mutex::new(Vec::new());

/// A spill directory whose write failed is tried after the others for this long.
const SPILL_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// Where an object of `total` bytes headed for `path` should be written
/// instead, when memory is over the limit: its spill paths to try in turn,
/// starting from the next directory round-robin, those with no failed write
/// in the last SPILL_RETRY_AFTER first. Empty: it stays in memory.
fn spill_targets(path: &Path, total: usize) -> Vec<(PathBuf, PathBuf)> {
    if !over_limit(total as u64) {
        return Vec::new();
    }
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let cfg = CONFIG.read().expect("lock");
    let Some((cfg, root)) = cfg.as_ref().and_then(|c| Some((c, c.root.as_ref()?))) else { return Vec::new() };
    let Ok(rel) = path.strip_prefix(root) else { return Vec::new() };
    let n = cfg.spill.len();
    let start = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let failed = SPILL_FAILED.lock().expect("lock");
    let recent = |d: &PathBuf| failed.iter().any(|(f, at)| f == d && at.elapsed() < SPILL_RETRY_AFTER);
    let mut dirs: Vec<&PathBuf> = (0..n).map(|k| &cfg.spill[(start + k) % n]).collect();
    dirs.sort_by_key(|d| recent(d)); // stable: round-robin order within each
    dirs.into_iter().map(|d| (d.clone(), d.join(rel))).collect()
}

/// Write `parts` (concatenated) to `path` atomically: a temporary name, then
/// a rename. Returns the bytes written. Over the memory limit the object is
/// written to a spill directory on disk and `path` becomes a symlink to it
/// (spilled at birth: nothing waits and nothing is moved later); readers map
/// it either way. Its bytes are reserved there first; a spill directory
/// that is full or fails leaves no file behind, and the next one is tried.
pub fn write_parts(path: &Path, parts: &[&[u8]]) -> io::Result<u64> {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut last = None;
    for (dir, target) in spill_targets(path, total) {
        let n = match write_file(&target, parts, total, true) {
            Ok(n) => n,
            Err(e) => {
                let mut failed = SPILL_FAILED.lock().expect("lock");
                failed.retain(|(d, _)| *d != dir);
                failed.push((dir, std::time::Instant::now()));
                last = Some(e);
                continue;
            }
        };
        let link = tmp_name(path);
        let r = std::os::unix::fs::symlink(&target, &link).or_else(|e| {
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e);
            }
            std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;
            std::os::unix::fs::symlink(&target, &link)
        });
        if let Err(e) = r.and_then(|()| std::fs::rename(&link, path)) {
            let _ = std::fs::remove_file(&link);
            let _ = std::fs::remove_file(&target);
            return Err(e);
        }
        return Ok(n);
    }
    if let Some(e) = last {
        return Err(e); // every spill directory failed; memory is over its limit
    }
    let n = write_file(path, parts, total, false)?;
    note_written(n);
    Ok(n)
}

/// Write `parts` to `path` through a temporary file; `on_disk` (a spill
/// directory): its bytes are reserved first, so a full disk fails before any
/// is written.
fn write_file(path: &Path, parts: &[&[u8]], total: usize, on_disk: bool) -> io::Result<u64> {
    let tmp = tmp_name(path);
    let reserve = CONFIG.read().expect("lock").as_ref().and_then(|c| c.reserve.clone());
    let warm = reserve.is_some_and(|dir| total >= 1 << RESERVE_MIN_CLASS && take_reserve(&dir, total, &tmp));
    let file = if warm {
        std::fs::OpenOptions::new().read(true).write(true).open(&tmp).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })?
    } else {
        create_new(&tmp)?
    };
    let result = (|| {
        if !warm
            && let Some(free) = free_bytes(&file)
            && (free as usize) < total
        {
            return Err(no_space(path, total, free));
        }
        if on_disk {
            preallocate(&file, total as u64).map_err(|e| match free_bytes(&file) {
                Some(free) if e.raw_os_error() == Some(libc::ENOSPC) => no_space(path, total, free),
                _ => e,
            })?;
        }
        file.set_len(total as u64)?; // a reserve file shrinks to fit
        // Shared mappings of a disk file fault and allocate blocks page by page;
        // plain writes into the page cache are faster there. tmpfs is filled
        // through a mapping, by several threads.
        if total < PARALLEL_MIN || !cfg!(target_os = "linux") || !in_memory(&file) {
            let mut off = 0u64;
            for p in parts {
                file.write_all_at(p, off)?;
                off += p.len() as u64;
            }
            return Ok(());
        }
        parallel_fill(&file, parts, total).map_err(|e| match free_bytes(&file) {
            Some(free) if (free as usize) < total => no_space(path, total, free),
            _ => e,
        })
    })();
    match result {
        Ok(()) => {
            std::fs::rename(&tmp, path)?;
            Ok(total as u64)
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// Map `file` (already `total` bytes long) and fill it from `parts` on
/// several threads. Each thread populates its pages first: on Linux that
/// reports a full tmpfs as an error instead of a SIGBUS at first touch.
fn parallel_fill(file: &std::fs::File, parts: &[&[u8]], total: usize) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: a fresh shared mapping of a file we own; unmapped below.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            total,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    let base_addr = base as usize;
    let mut starts = Vec::with_capacity(parts.len());
    let mut off = 0;
    for p in parts {
        starts.push(off);
        off += p.len();
    }
    let r = for_chunks(total, |cs, ce| {
        populate_write(base_addr + cs, ce - cs)?;
        for (i, p) in parts.iter().enumerate() {
            let (ps, pe) = (starts[i], starts[i] + p.len());
            let (s0, e0) = (ps.max(cs), pe.min(ce));
            if s0 < e0 {
                // SAFETY: [s0, e0) lies inside the mapping and inside part i;
                // chunks are disjoint, so no two threads write the same bytes.
                unsafe {
                    std::ptr::copy_nonoverlapping(p.as_ptr().add(s0 - ps), (base_addr + s0) as *mut u8, e0 - s0);
                }
            }
        }
        Ok(())
    });
    // SAFETY: unmapping the mapping created above.
    unsafe { libc::munmap(base, total) };
    r
}

/// Run `work(start, end)` over `len` bytes in CHUNK pieces, on the calling
/// thread and up to MAX_THREADS - 1 helpers. A helper the system refuses (a
/// pod's pids.max counts threads) only leaves more pieces for the others,
/// never a panic. Stops at the first error and returns it.
fn for_chunks(len: usize, work: impl Fn(usize, usize) -> io::Result<()> + Sync) -> io::Result<()> {
    for_chunks_on(len, MAX_THREADS, work)
}

thread_local! {
    /// CPU time the helpers of this thread's `for_chunks` calls have spent.
    /// An executor reports it with each call, and the peer leaves it out of
    /// the cores it learns for the function (see `helper_cpu_ns`).
    static HELPER_CPU_NS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// This thread's CPU time, in nanoseconds (0 if the clock is unavailable).
fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: a valid out pointer for clock_gettime.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
        return 0;
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// The CPU time, in nanoseconds, that helper threads have spent for the
/// calling thread so far: runtime work (copying and populating large values
/// in parallel), not the user's code. An executor reads it before and after a
/// call and reports the difference, so a one-core function whose 256 MiB
/// argument was read on four threads isn't learned as needing four CPUs.
#[pyfunction]
pub fn helper_cpu_ns() -> u64 {
    HELPER_CPU_NS.with(|c| c.get())
}

/// `for_chunks` on at most `max_threads` threads, the caller's included.
fn for_chunks_on(
    len: usize,
    max_threads: usize,
    work: impl Fn(usize, usize) -> io::Result<()> + Sync,
) -> io::Result<()> {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    let chunks = len.div_ceil(CHUNK);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()).clamp(1, max_threads.max(1)).min(chunks);
    let next = AtomicUsize::new(0);
    let helpers_ns = AtomicU64::new(0);
    let failed: std::sync::Mutex<Option<io::Error>> = std::sync::Mutex::new(None);
    let run = || {
        loop {
            let c = next.fetch_add(1, Ordering::Relaxed);
            if c >= chunks || failed.lock().expect("lock").is_some() {
                return;
            }
            if let Err(e) = work(c * CHUNK, ((c + 1) * CHUNK).min(len)) {
                *failed.lock().expect("lock") = Some(e);
                return;
            }
        }
    };
    let helper = || {
        let t0 = thread_cpu_ns();
        run();
        helpers_ns.fetch_add(thread_cpu_ns().saturating_sub(t0), Ordering::Relaxed);
    };
    std::thread::scope(|s| {
        for _ in 1..threads {
            let _ = std::thread::Builder::new().spawn_scoped(s, helper);
        }
        run();
    });
    let spent = helpers_ns.into_inner();
    HELPER_CPU_NS.with(|c| c.set(c.get().saturating_add(spent)));
    failed.into_inner().expect("lock").map_or(Ok(()), Err)
}

#[cfg(target_os = "linux")]
fn populate_write(addr: usize, len: usize) -> io::Result<()> {
    const MADV_POPULATE_WRITE: libc::c_int = 23;
    let start = addr & !4095;
    // SAFETY: the range is inside a live shared mapping.
    if unsafe { libc::madvise(start as *mut libc::c_void, len + (addr - start), MADV_POPULATE_WRITE) } != 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EINVAL) {
            return Err(e); // EINVAL: a kernel older than 5.14; pages fault in as written
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn populate_write(_addr: usize, _len: usize) -> io::Result<()> {
    Ok(())
}

/// Buffers exported from Python objects, kept alive while their bytes are used.
fn buffers(objs: &[Bound<'_, PyAny>]) -> PyResult<Vec<pyo3::buffer::PyUntypedBuffer>> {
    objs.iter()
        .map(|o| {
            let b = pyo3::buffer::PyUntypedBuffer::get(o)?;
            if !b.is_c_contiguous() {
                return Err(PyValueError::new_err("object parts must be contiguous buffers"));
            }
            Ok(b)
        })
        .collect()
}

/// Write an object file at `path` from buffer `parts` (bytes, memoryviews,
/// arrays), atomically; the GIL is released while copying. Returns its size.
#[pyfunction]
pub fn write_object(py: Python<'_>, path: PathBuf, parts: Vec<Bound<'_, PyAny>>) -> PyResult<u64> {
    let bufs = buffers(&parts)?;
    let raw: Vec<(usize, usize)> = bufs.iter().map(|b| (b.buf_ptr() as usize, b.len_bytes())).collect();
    let r = py.detach(move || {
        // SAFETY: `bufs` keeps every buffer exported (so valid) until it drops
        // after this closure returns.
        let slices: Vec<&[u8]> =
            raw.iter().map(|&(p, n)| unsafe { std::slice::from_raw_parts(p as *const u8, n) }).collect();
        write_parts(&path, &slices)
    });
    drop(bufs);
    r.map_err(os_err)
}

/// `length` bytes of `fd` from `offset`, as a new `bytes` object on the heap,
/// not in the arena, read with the GIL released. A value read is seldom
/// stored again, and an arena block costs a new tmpfs file whose 4 KiB pages
/// fault in one at a time as the copy reaches them: reading 2,048 values of
/// 1 MiB in one call took 590 us each into the arena against 475 on the heap
/// (Ray: 511). From READ_PARALLEL_MIN the read is split over READ_THREADS
/// threads: one 256 MiB read went from 120 to 47 ms, and ten at once on 16
/// vCPUs from 235 ms (into the arena) to 141. `EOFError` when the file is
/// shorter.
#[pyfunction]
pub fn pread_heap(py: Python<'_>, fd: i32, length: usize, offset: u64) -> PyResult<Bound<'_, pyo3::types::PyBytes>> {
    let n = pyo3::ffi::Py_ssize_t::try_from(length).map_err(|_| PyValueError::new_err("length too large"))?;
    // SAFETY: a new bytes object of `length` uninitialized bytes, every one
    // written below before Python can see the object.
    let obj = with_heap(|| unsafe { pyo3::ffi::PyBytes_FromStringAndSize(std::ptr::null(), n) });
    if obj.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `obj` is a new reference to a bytes object.
    let bytes = unsafe { Bound::from_owned_ptr(py, obj).cast_into_unchecked::<pyo3::types::PyBytes>() };
    // SAFETY: the buffer of that bytes object, `length` bytes long.
    let base = unsafe { pyo3::ffi::PyBytes_AsString(obj) } as usize;
    #[cfg(target_os = "linux")]
    if length >= READ_PARALLEL_MIN {
        arena::defer_free(obj as usize); // the object is its own raw block (see arena::defer_free)
    }
    py.detach(|| {
        if length < READ_PARALLEL_MIN {
            return pread_exact(fd, base, length, offset);
        }
        for_chunks_on(length, READ_THREADS, |cs, ce| pread_exact(fd, base + cs, ce - cs, offset + cs as u64))
    })
    .map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof { PyEOFError::new_err(e.to_string()) } else { os_err(e) }
    })?;
    Ok(bytes)
}

/// `s` encoded to UTF-8 (`surrogatepass`, as pickle encodes a `str`) into a
/// new bytes object on the heap, not in the arena: a large `str` stored out
/// of band (chimaera._serde) is then written into its object file with one
/// copy. Encoding a 1 MiB `str` into a new arena block, whose 4 KiB pages
/// fault in one at a time, then storing it by link took 583 us on
/// `/dev/shm`; encoding on the heap and writing took 313 us.
#[pyfunction]
pub fn encode_utf8_heap<'py>(
    py: Python<'py>,
    s: &Bound<'py, pyo3::types::PyString>,
) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    // SAFETY: `s` is a str; the call returns a new reference or null with an
    // exception set.
    let obj = with_heap(|| unsafe {
        pyo3::ffi::PyUnicode_AsEncodedString(s.as_ptr(), c"utf-8".as_ptr(), c"surrogatepass".as_ptr())
    });
    if obj.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: a new reference to a bytes object (the UTF-8 codec returns bytes).
    Ok(unsafe { Bound::from_owned_ptr(py, obj).cast_into_unchecked::<pyo3::types::PyBytes>() })
}

/// Reads at least this large are split over READ_THREADS threads.
const READ_PARALLEL_MIN: usize = 16 << 20;
/// Threads for one large read. More helped a lone read (8: 36 ms for 256 MiB)
/// but not ten at once, which is when it matters.
const READ_THREADS: usize = 4;

/// Fill `len` bytes at `base` from `fd` at `offset`, retrying short reads.
fn pread_exact(fd: i32, base: usize, len: usize, offset: u64) -> io::Result<()> {
    let mut at = 0usize;
    while at < len {
        // SAFETY: [base + at, base + len) lies in a buffer the caller owns.
        let r =
            unsafe { libc::pread(fd, (base + at) as *mut libc::c_void, len - at, (offset + at as u64) as libc::off_t) };
        if r < 0 {
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        } else if r == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the file ended early"));
        } else {
            at += r as usize;
        }
    }
    Ok(())
}

/// Run `f` with this thread's large Python allocations on the heap, not in
/// the arena.
fn with_heap<T>(f: impl FnOnce() -> T) -> T {
    #[cfg(target_os = "linux")]
    return arena::with_heap(f);
    #[cfg(not(target_os = "linux"))]
    f()
}

#[cfg(target_os = "linux")]
pub use arena::{arena_claim, arena_install, arena_pool_api, arena_shutdown, arena_stats};

#[cfg(not(target_os = "linux"))]
mod portable {
    use super::*;

    /// The arena needs Linux (tmpfs, MAP_FIXED remaps, pagemap).
    #[pyfunction]
    #[pyo3(signature = (dir, min_bytes, cache_bytes=0, raw=false))]
    pub fn arena_install(dir: PathBuf, min_bytes: usize, cache_bytes: usize, raw: bool) -> bool {
        let _ = (dir, min_bytes, cache_bytes, raw);
        false
    }

    #[pyfunction]
    pub fn arena_claim(_buffer: &Bound<'_, PyAny>, _dest: PathBuf) -> Option<u64> {
        None
    }

    #[pyfunction]
    pub fn arena_stats() -> (u64, u64, u64, u64, u64) {
        (0, 0, 0, 0, 0)
    }

    #[pyfunction]
    pub fn arena_shutdown() {}

    #[pyfunction]
    pub fn arena_pool_api() -> usize {
        0
    }
}

#[cfg(not(target_os = "linux"))]
pub use portable::{arena_claim, arena_install, arena_pool_api, arena_shutdown, arena_stats};

#[cfg(target_os = "linux")]
mod arena {
    use super::*;
    use std::collections::BTreeMap;
    use std::ffi::{CString, c_void};
    use std::os::unix::ffi::OsStrExt;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    type Malloc = unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void;
    type Calloc = unsafe extern "C" fn(*mut c_void, usize, usize) -> *mut c_void;
    type Realloc = unsafe extern "C" fn(*mut c_void, *mut c_void, usize) -> *mut c_void;
    type Free = unsafe extern "C" fn(*mut c_void, *mut c_void, usize);

    /// NumPy's `PyDataMemAllocator` (numpy/ndarraytypes.h, version 1).
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Allocator {
        ctx: *mut c_void,
        malloc: Option<Malloc>,
        calloc: Option<Calloc>,
        realloc: Option<Realloc>,
        free: Option<Free>,
    }

    /// NumPy's `PyDataMem_Handler`.
    #[repr(C)]
    struct Handler {
        name: [libc::c_char; 127],
        version: u8,
        allocator: Allocator,
    }

    #[derive(Clone, Copy)]
    struct Prev(Allocator);
    // SAFETY: NumPy's default handler functions are thread-safe and the ctx is
    // an opaque pointer they own.
    unsafe impl Send for Prev {}
    unsafe impl Sync for Prev {}

    type RawFree = unsafe extern "C" fn(*mut c_void, *mut c_void);

    /// CPython's `PyMemAllocatorEx` (pymem.h, 3.5+; not in the stable ABI).
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct RawAllocator {
        ctx: *mut c_void,
        malloc: Option<Malloc>,
        calloc: Option<Calloc>,
        realloc: Option<Realloc>,
        free: Option<RawFree>,
    }

    struct RawPrev(RawAllocator);
    // SAFETY: CPython's raw allocator is thread-safe by contract (it is called
    // without the GIL) and the ctx is an opaque pointer it owns.
    unsafe impl Send for RawPrev {}
    unsafe impl Sync for RawPrev {}

    const PYMEM_DOMAIN_RAW: libc::c_int = 0;

    unsafe extern "C" {
        fn PyMem_GetAllocator(domain: libc::c_int, allocator: *mut RawAllocator);
        fn PyMem_SetAllocator(domain: libc::c_int, allocator: *mut RawAllocator);
    }

    /// The raw allocator Python had before ours wrapped it.
    static RAW: OnceLock<RawPrev> = OnceLock::new();

    fn raw() -> &'static RawAllocator {
        &RAW.get().expect("raw hooks are installed only after RAW").0
    }

    struct Alloc {
        len: usize,
        path: PathBuf,
        /// Linked under an object's name at least once; the owner's mapping is
        /// private (copy-on-write) since.
        published: bool,
        /// Inherited through fork: the parent owns the file.
        foreign: bool,
    }

    struct Arena {
        dir: PathBuf,
        min: usize,
        cache_max: usize,
        /// NumPy's allocator, once it is patched.
        numpy: OnceLock<Prev>,
        seq: AtomicU64,
        live: Mutex<BTreeMap<usize, Alloc>>,
        /// Freed allocations never stored anywhere, kept mapped for reuse.
        cache: Mutex<Vec<(usize, Alloc)>>,
        cache_bytes: AtomicUsize,
        lo: AtomicUsize,
        hi: AtomicUsize,
        enabled: AtomicBool,
        claims: AtomicU64,
        fallbacks: AtomicU64,
    }

    static ARENA: OnceLock<Arena> = OnceLock::new();

    fn arena() -> &'static Arena {
        ARENA.get().expect("arena functions are installed only after the arena")
    }

    /// The arena as Arrow's memory pool calls it (python/chimaera/_arrow_pool.py,
    /// whose C++ declares the same struct): allocations of `min` bytes and up
    /// come from here, so a value of Arrow buffers is stored by linking their
    /// files, as a NumPy array is.
    #[repr(C)]
    pub struct PoolApi {
        min: usize,
        /// A page-aligned allocation of `size` bytes, or null (the arena is
        /// off or memory is full: the caller's other pool serves it).
        alloc: unsafe extern "C" fn(usize) -> *mut c_void,
        /// -1: not ours. 0: ours, but it must move (allocate, copy, free).
        /// 1: resized; the allocation is now at `*out`.
        resize: unsafe extern "C" fn(*mut c_void, usize, *mut *mut c_void) -> i32,
        /// Free an allocation of ours and return true; false if not ours.
        free: unsafe extern "C" fn(*mut c_void) -> bool,
    }

    unsafe extern "C" fn pool_alloc(size: usize) -> *mut c_void {
        arena().alloc(size, false)
    }

    unsafe extern "C" fn pool_resize(ptr: *mut c_void, new: usize, out: *mut *mut c_void) -> i32 {
        match arena().resize(ptr as usize, new) {
            None => -1,
            Some((_, None)) => 0,
            Some((_, Some(q))) => {
                // SAFETY: the caller passes a valid out-pointer (the C++ pool's local).
                unsafe { *out = q as *mut c_void };
                1
            }
        }
    }

    unsafe extern "C" fn pool_free(ptr: *mut c_void) -> bool {
        arena().free(ptr as usize)
    }

    /// The address of the arena's PoolApi, or 0 when no arena is installed.
    #[pyfunction]
    pub fn arena_pool_api() -> usize {
        static API: OnceLock<PoolApi> = OnceLock::new();
        let Some(ar) = ARENA.get() else { return 0 };
        let api = API.get_or_init(|| PoolApi { min: ar.min, alloc: pool_alloc, resize: pool_resize, free: pool_free });
        api as *const PoolApi as usize
    }

    fn page_up(n: usize) -> usize {
        (n + 4095) & !4095
    }

    fn cstr(p: &Path) -> CString {
        CString::new(p.as_os_str().as_bytes()).expect("paths have no NUL")
    }

    impl Arena {
        #[allow(clippy::unnecessary_cast)] // statvfs field widths differ by platform
        fn fs_has_room(&self, fd: libc::c_int, len: usize) -> bool {
            let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
            // SAFETY: fstatvfs on a valid descriptor.
            if unsafe { libc::fstatvfs(fd, &mut st) } != 0 {
                return false;
            }
            let free = st.f_bavail as u64 * st.f_frsize as u64;
            let size = st.f_blocks as u64 * st.f_frsize as u64;
            // Keep a tenth of the filesystem free: a write past its end would be
            // a SIGBUS, so arrays go to the heap well before that.
            free >= len as u64 + size / 10
        }

        fn alloc(&self, size: usize, zero: bool) -> *mut c_void {
            if !self.enabled.load(Ordering::Relaxed) {
                return std::ptr::null_mut();
            }
            let len = page_up(size);
            if let Some((base, a)) = self.take_cached(len) {
                if zero {
                    // SAFETY: `base` maps at least `len` writable bytes.
                    unsafe { std::ptr::write_bytes(base as *mut u8, 0, size) };
                }
                self.live.lock().expect("lock").insert(base, a);
                return base as *mut c_void;
            }
            let path = self.dir.join(format!("{}", self.seq.fetch_add(1, Ordering::Relaxed)));
            if over_limit(len as u64) {
                return std::ptr::null_mut(); // memory is full of objects: the heap it is
            }
            let reserve = CONFIG.read().expect("lock").as_ref().and_then(|c| c.reserve.clone());
            // Pre-zeroed pages when the reserve has them: no faults to fill.
            let warm = reserve.is_some_and(|d| take_reserve(&d, len, &path));
            let c = cstr(&path);
            // SAFETY: plain syscalls on a path we own; every error is checked.
            unsafe {
                let create = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC;
                let mut fd = if warm {
                    libc::open(c.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC)
                } else {
                    libc::open(c.as_ptr(), create, 0o600)
                };
                if fd < 0 && !warm && *libc::__errno_location() == libc::ENOENT {
                    let _ = std::fs::create_dir_all(&self.dir);
                    fd = libc::open(c.as_ptr(), create, 0o600);
                }
                if fd < 0 {
                    libc::unlink(c.as_ptr());
                    return std::ptr::null_mut();
                }
                let ok = (warm || self.fs_has_room(fd, len)) && libc::ftruncate(fd, len as libc::off_t) == 0;
                let flags = if warm { libc::MAP_SHARED | libc::MAP_POPULATE } else { libc::MAP_SHARED };
                let base = if ok {
                    libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, flags, fd, 0)
                } else {
                    libc::MAP_FAILED
                };
                libc::close(fd);
                if base == libc::MAP_FAILED {
                    libc::unlink(c.as_ptr());
                    return std::ptr::null_mut();
                }
                let b = base as usize;
                if !warm && len >= POPULATE_MIN && populate_parallel(b, len).is_err() {
                    libc::munmap(base, len);
                    libc::unlink(c.as_ptr());
                    return std::ptr::null_mut(); // the filesystem is full: the heap it is
                }
                self.lo.fetch_min(b, Ordering::Relaxed);
                self.hi.fetch_max(b + len, Ordering::Relaxed);
                self.live.lock().expect("lock").insert(b, Alloc { len, path, published: false, foreign: false });
                base
            }
        }

        fn take_cached(&self, len: usize) -> Option<(usize, Alloc)> {
            let mut cache = self.cache.lock().expect("lock");
            // Smallest cached mapping that fits without wasting more than a quarter.
            let i = cache
                .iter()
                .enumerate()
                .filter(|(_, (_, a))| a.len >= len && a.len <= len + len / 4)
                .min_by_key(|(_, (_, a))| a.len)
                .map(|(i, _)| i)?;
            let (b, a) = cache.swap_remove(i);
            self.cache_bytes.fetch_sub(a.len, Ordering::Relaxed);
            Some((b, a))
        }

        /// Resize an allocation of ours in place when no object links its
        /// file: the file grows or shrinks and the mapping follows (mremap),
        /// with no copy. None: not ours. Some((old length, None)): ours, but it
        /// must move (an object links it, it is inherited, or it is now below
        /// the arena's minimum).
        fn resize(&self, p: usize, new: usize) -> Option<(usize, Option<usize>)> {
            if p < self.lo.load(Ordering::Relaxed) || p >= self.hi.load(Ordering::Relaxed) {
                return None;
            }
            let mut live = self.live.lock().expect("lock");
            let a = live.get(&p)?;
            let (old, len) = (a.len, page_up(new));
            if a.published || a.foreign || new < self.min || !self.enabled.load(Ordering::Relaxed) {
                return Some((old, None));
            }
            if len == old {
                return Some((old, Some(p)));
            }
            if len > old && over_limit((len - old) as u64) {
                return Some((old, None));
            }
            let c = cstr(&a.path);
            // SAFETY: our own file and mapping. The file covers the mapping at
            // every step (it grows first and shrinks last), so no access to
            // the mapping falls past the file's end.
            let q = unsafe {
                let fd = libc::open(c.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
                if fd < 0 {
                    return Some((old, None));
                }
                let room =
                    len < old || (self.fs_has_room(fd, len - old) && libc::ftruncate(fd, len as libc::off_t) == 0);
                let q = if room {
                    libc::mremap(p as *mut c_void, old, len, libc::MREMAP_MAYMOVE)
                } else {
                    libc::MAP_FAILED
                };
                if q != libc::MAP_FAILED && len < old {
                    libc::ftruncate(fd, len as libc::off_t);
                }
                libc::close(fd);
                q
            };
            if q == libc::MAP_FAILED {
                return Some((old, None));
            }
            let q = q as usize;
            let mut a = live.remove(&p).expect("held under the lock");
            a.len = len;
            live.insert(q, a);
            self.lo.fetch_min(q, Ordering::Relaxed);
            self.hi.fetch_max(q + len, Ordering::Relaxed);
            Some((old, Some(q)))
        }

        /// Ours: unmap (or cache) and return true. Not ours: false.
        fn free(&self, ptr: usize) -> bool {
            if ptr < self.lo.load(Ordering::Relaxed) || ptr >= self.hi.load(Ordering::Relaxed) {
                return false;
            }
            let Some(a) = self.live.lock().expect("lock").remove(&ptr) else { return false };
            if !a.published && !a.foreign && self.enabled.load(Ordering::Relaxed) {
                let mut cache = self.cache.lock().expect("lock");
                if self.cache_bytes.load(Ordering::Relaxed) + a.len <= self.cache_max && cache.len() < 64 {
                    self.cache_bytes.fetch_add(a.len, Ordering::Relaxed);
                    cache.push((ptr, a));
                    return true;
                }
            }
            release(ptr, &a);
            true
        }
    }

    /// Unmap an allocation and drop our name for it (an object that links the
    /// file keeps its pages).
    fn release(ptr: usize, a: &Alloc) {
        // SAFETY: `ptr` maps `a.len` bytes created by this arena.
        unsafe { libc::munmap(ptr as *mut c_void, a.len) };
        if !a.foreign {
            let _ = std::fs::remove_file(&a.path);
        }
    }

    unsafe extern "C" fn h_malloc(ctx: *mut c_void, size: usize) -> *mut c_void {
        let ar = arena();
        if size >= ar.min {
            let p = ar.alloc(size, false);
            if !p.is_null() {
                return p;
            }
            ar.fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwarding to NumPy's own allocator with its context.
        unsafe { np(ar).malloc.expect("malloc")(ctx, size) }
    }

    unsafe extern "C" fn h_calloc(ctx: *mut c_void, n: usize, el: usize) -> *mut c_void {
        let ar = arena();
        if let Some(size) = n.checked_mul(el)
            && size >= ar.min
        {
            let p = ar.alloc(size, true);
            if !p.is_null() {
                return p;
            }
            ar.fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: as above.
        unsafe { np(ar).calloc.expect("calloc")(ctx, n, el) }
    }

    fn np(ar: &Arena) -> &Allocator {
        &ar.numpy.get().expect("NumPy's hooks run only once it is patched").0
    }

    unsafe extern "C" fn h_realloc(ctx: *mut c_void, ptr: *mut c_void, new: usize) -> *mut c_void {
        let ar = arena();
        // SAFETY: NumPy's own functions with its context.
        let heap_realloc = || unsafe { np(ar).realloc.expect("realloc")(ctx, ptr, new) };
        let heap_malloc = |n| unsafe { np(ar).malloc.expect("malloc")(ctx, n) };
        // SAFETY: `ptr` came from this allocator (NumPy's contract).
        unsafe { realloc_any(ptr, new, heap_realloc, heap_malloc) }
    }

    /// realloc for either hooked allocator: in place for a block of ours that
    /// no object links, else moved (into the arena, or the previous
    /// allocator's heap when small or the arena is full). Not ours: the
    /// previous allocator's realloc.
    unsafe fn realloc_any(
        ptr: *mut c_void,
        new: usize,
        heap_realloc: impl FnOnce() -> *mut c_void,
        heap_malloc: impl FnOnce(usize) -> *mut c_void,
    ) -> *mut c_void {
        let ar = arena();
        let p = ptr as usize;
        let Some((old_len, resized)) = ar.resize(p, new) else { return heap_realloc() };
        if let Some(q) = resized {
            return q as *mut c_void;
        }
        let mut q = if new >= ar.min { ar.alloc(new, false) } else { std::ptr::null_mut() };
        if q.is_null() {
            q = heap_malloc(new.max(1));
            if q.is_null() {
                return q;
            }
        }
        // SAFETY: both ranges are valid for min(old_len, new) bytes and distinct.
        unsafe { std::ptr::copy_nonoverlapping(ptr as *const u8, q as *mut u8, old_len.min(new)) };
        ar.free(p);
        q
    }

    thread_local! {
        /// Set while `with_heap` runs: this thread's raw allocations skip the arena.
        static HEAP_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// Run `f` with this thread's large Python allocations on the heap.
    pub fn with_heap<T>(f: impl FnOnce() -> T) -> T {
        HEAP_NEXT.with(|h| h.set(true));
        let r = f();
        HEAP_NEXT.with(|h| h.set(false));
        r
    }

    /// Python's raw allocator (under its large `bytes`, `str`, lists and
    /// dicts): blocks of at least the arena's minimum come from the arena,
    /// so a large `bytes` is stored by link, like a NumPy array.
    unsafe extern "C" fn raw_malloc(ctx: *mut c_void, size: usize) -> *mut c_void {
        let ar = arena();
        if size >= ar.min && !HEAP_NEXT.with(|h| h.get()) {
            let p = ar.alloc(size, false);
            if !p.is_null() {
                return p;
            }
            ar.fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwarding to the previous raw allocator with its context.
        unsafe { raw().malloc.expect("malloc")(ctx, size) }
    }

    unsafe extern "C" fn raw_calloc(ctx: *mut c_void, n: usize, el: usize) -> *mut c_void {
        let ar = arena();
        if let Some(size) = n.checked_mul(el)
            && size >= ar.min
        {
            let p = ar.alloc(size, true);
            if !p.is_null() {
                return p;
            }
            ar.fallbacks.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: as above.
        unsafe { raw().calloc.expect("calloc")(ctx, n, el) }
    }

    unsafe extern "C" fn raw_realloc(ctx: *mut c_void, ptr: *mut c_void, new: usize) -> *mut c_void {
        if ptr.is_null() {
            // SAFETY: realloc(NULL, n) is malloc(n).
            return unsafe { raw_malloc(ctx, new) };
        }
        // SAFETY: the previous raw allocator's functions with its context.
        let heap_realloc = || unsafe { raw().realloc.expect("realloc")(ctx, ptr, new) };
        let heap_malloc = |n| unsafe { raw().malloc.expect("malloc")(ctx, n) };
        // SAFETY: `ptr` came from the raw allocator (CPython's contract).
        unsafe { realloc_any(ptr, new, heap_realloc, heap_malloc) }
    }

    unsafe extern "C" fn raw_free(ctx: *mut c_void, ptr: *mut c_void) {
        if arena().free(ptr as usize) {
            return;
        }
        if DEFERRED_LIVE.load(Ordering::Relaxed) > 0 && free_later(ptr as usize) {
            return;
        }
        // SAFETY: not ours: the previous raw allocator owns `ptr`.
        unsafe { raw().free.expect("free")(ctx, ptr) }
    }

    /// Heap blocks of large values read for a call (`pread_heap`), freed on a
    /// background thread once Python lets go of them. Unmapping 256 MiB took
    /// 22 ms, 33 to 42 ms with ten calls doing it at once, and it happened
    /// as the call's frame ended, before its outcome could be reported.
    static DEFERRED: std::sync::LazyLock<Mutex<std::collections::HashSet<usize>>> =
        std::sync::LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));
    static DEFERRED_LIVE: AtomicUsize = AtomicUsize::new(0);
    /// The reclaiming thread's queue, and the process that started it (a
    /// forked child starts its own).
    static RECLAIM: Mutex<Option<(u32, std::sync::mpsc::Sender<usize>)>> = Mutex::new(None);

    /// Free `ptr` (a block of the previous raw allocator) on the background
    /// thread once Python frees it, if the raw hooks are in place.
    pub fn defer_free(ptr: usize) {
        if RAW.get().is_none() || !arena().enabled.load(Ordering::Relaxed) {
            return;
        }
        DEFERRED.lock().expect("lock").insert(ptr);
        DEFERRED_LIVE.fetch_add(1, Ordering::Relaxed);
    }

    /// Hand `ptr` to the reclaiming thread if it was deferred; false if not,
    /// or if no thread could be started (the caller frees it now).
    fn free_later(ptr: usize) -> bool {
        if !DEFERRED.lock().expect("lock").remove(&ptr) {
            return false;
        }
        DEFERRED_LIVE.fetch_sub(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut q = RECLAIM.lock().expect("lock");
        if q.as_ref().is_none_or(|(p, _)| *p != pid) {
            let (tx, rx) = std::sync::mpsc::channel::<usize>();
            let started = std::thread::Builder::new().name("chimaera-free".into()).spawn(move || {
                for p in rx {
                    // SAFETY: a block of the previous raw allocator that
                    // Python has freed; nothing else refers to it.
                    unsafe { raw().free.expect("free")(raw().ctx, p as *mut c_void) }
                }
            });
            if started.is_err() {
                *q = None;
                return false;
            }
            *q = Some((pid, tx));
        }
        q.as_ref().is_some_and(|(_, tx)| tx.send(ptr).is_ok())
    }

    /// Wrap Python's raw allocator (once). A hook that forwards everything
    /// not its own is the only kind CPython allows after start-up.
    fn hook_raw() {
        if RAW.get().is_some() {
            return;
        }
        let mut prev =
            RawAllocator { ctx: std::ptr::null_mut(), malloc: None, calloc: None, realloc: None, free: None };
        // SAFETY: copies the current raw allocator into `prev`.
        unsafe { PyMem_GetAllocator(PYMEM_DOMAIN_RAW, &mut prev) };
        if prev.malloc.is_none() || prev.calloc.is_none() || prev.realloc.is_none() || prev.free.is_none() {
            return;
        }
        if RAW.set(RawPrev(prev)).is_err() {
            return;
        }
        let mut ours = RawAllocator {
            ctx: prev.ctx,
            malloc: Some(raw_malloc),
            calloc: Some(raw_calloc),
            realloc: Some(raw_realloc),
            free: Some(raw_free),
        };
        // SAFETY: installs a hook over `prev` (saved above); called with the GIL.
        unsafe { PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &mut ours) };
    }

    unsafe extern "C" fn h_free(ctx: *mut c_void, ptr: *mut c_void, size: usize) {
        let ar = arena();
        if !ar.free(ptr as usize) {
            // SAFETY: not ours: NumPy's allocator owns `ptr`.
            unsafe { np(ar).free.expect("free")(ctx, ptr, size) }
        }
    }

    type Guards =
        (std::sync::MutexGuard<'static, BTreeMap<usize, Alloc>>, std::sync::MutexGuard<'static, Vec<(usize, Alloc)>>);

    /// New blocks at least this large are populated as they are handed out, on
    /// several threads. Otherwise the code filling one (a NumPy copy, a `str`
    /// encode) faults its tmpfs pages in 4 KiB at a time on its own thread,
    /// which costs more than the copy itself; write_object does the same.
    const POPULATE_MIN: usize = 8 << 20;

    fn populate_parallel(base: usize, len: usize) -> io::Result<()> {
        for_chunks(len, |cs, ce| populate_write(base + cs, ce - cs))
    }

    thread_local! {
        /// The arena's locks, held by the forking thread across fork() so the
        /// child never inherits them locked by a thread it does not have.
        static FORK_GUARDS: std::cell::RefCell<Option<Guards>> = const { std::cell::RefCell::new(None) };
    }

    extern "C" fn atfork_prepare() {
        if let Some(ar) = ARENA.get() {
            let mut live = ar.live.lock().expect("lock");
            // The child must not share pages the parent keeps writing (an
            // array's data, a Python object's reference count): make the
            // parent's mappings copy-on-write first, as storing one does, so
            // fork gives the child the usual private copy.
            for (&b, a) in live.iter_mut() {
                if !a.published && !a.foreign && remap_private(b, a) {
                    a.published = true;
                }
            }
            let g = (live, ar.cache.lock().expect("lock"));
            FORK_GUARDS.with(|f| *f.borrow_mut() = Some(g));
        }
    }

    extern "C" fn atfork_parent() {
        FORK_GUARDS.with(|f| drop(f.borrow_mut().take()));
    }

    /// The child inherits the parent's (copy-on-write) mappings: it never
    /// unlinks their files, and allocates from the heap. A mapping the parent
    /// could not make private is made private here.
    extern "C" fn atfork_child() {
        let Some(ar) = ARENA.get() else { return };
        ar.enabled.store(false, Ordering::Relaxed);
        let Some((mut live, mut cache)) = FORK_GUARDS.with(|f| f.borrow_mut().take()) else { return };
        for (&b, a) in live.iter_mut() {
            a.foreign = true;
            if !a.published {
                remap_private(b, a);
            }
        }
        for (b, a) in cache.drain(..) {
            // SAFETY: an inherited mapping nobody in the child uses.
            unsafe { libc::munmap(b as *mut c_void, a.len) };
        }
        ar.cache_bytes.store(0, Ordering::Relaxed);
    }

    fn remap_private(base: usize, a: &Alloc) -> bool {
        let c = cstr(&a.path);
        // SAFETY: replace our own mapping with a private one of the same file
        // and length at the same address; contents are identical.
        unsafe {
            let fd = libc::open(c.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC);
            if fd < 0 {
                return false;
            }
            let p = libc::mmap(
                base as *mut c_void,
                a.len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_FIXED,
                fd,
                0,
            );
            libc::close(fd);
            p as usize == base
        }
    }

    /// `struct pm_scan_arg` (linux/fs.h, Linux 6.7+).
    #[repr(C)]
    #[derive(Default)]
    struct PmScanArg {
        size: u64,
        flags: u64,
        start: u64,
        end: u64,
        walk_end: u64,
        vec: u64,
        vec_len: u64,
        max_pages: u64,
        category_inverted: u64,
        category_mask: u64,
        category_anyof_mask: u64,
        return_mask: u64,
    }

    /// Whether [ptr, ptr+len) of a stored allocation differs from its file.
    /// Pages wholly inside are checked in the page map; the partial pages at
    /// either end are compared with the file (a Python object's header, which
    /// its reference count rewrites, shares the first page with its data).
    fn changed(base: usize, a: &Alloc, ptr: usize, len: usize) -> bool {
        let (lo, hi) = (page_up(ptr), (ptr + len) & !4095);
        if lo < hi && dirty(lo, hi - lo) {
            return true;
        }
        let ends = if lo > hi { [(ptr, len), (0, 0)] } else { [(ptr, lo - ptr), (hi, ptr + len - hi)] };
        let Ok(f) = std::fs::File::open(&a.path) else { return true };
        let mut buf = [0u8; 4096];
        for (start, n) in ends.into_iter().filter(|&(_, n)| n > 0) {
            if f.read_exact_at(&mut buf[..n], (start - base) as u64).is_err() {
                return true;
            }
            // SAFETY: [start, start+n) lies inside the live allocation.
            if unsafe { std::slice::from_raw_parts(start as *const u8, n) } != &buf[..n] {
                return true;
            }
        }
        false
    }

    /// Whether the owner wrote to any page of [base, base+len) since the
    /// mapping became private (a written page is anonymous, not the file's).
    /// PAGEMAP_SCAN answers in the kernel and stops at the first such page;
    /// older kernels read the page map instead.
    fn dirty(base: usize, len: usize) -> bool {
        let Ok(f) = std::fs::File::open("/proc/self/pagemap") else { return true };
        {
            use std::os::fd::AsRawFd;
            const PAGE_IS_FILE: u64 = 1 << 2;
            const PAGE_IS_PRESENT: u64 = 1 << 3;
            const PAGEMAP_SCAN: libc::c_ulong = 0xC060_6610; // _IOWR('f', 16, struct pm_scan_arg)
            let mut region = [0u64; 3];
            let mut arg = PmScanArg {
                size: std::mem::size_of::<PmScanArg>() as u64,
                start: base as u64,
                end: (base + len) as u64,
                vec: region.as_mut_ptr() as u64,
                vec_len: 1,
                max_pages: 1,
                category_inverted: PAGE_IS_FILE,
                category_mask: PAGE_IS_PRESENT | PAGE_IS_FILE, // present and not the file's page
                return_mask: PAGE_IS_PRESENT,
                ..Default::default()
            };
            // SAFETY: PAGEMAP_SCAN reads `arg` and writes at most `vec_len`
            // regions into `region`, both live for the call.
            let n = unsafe { libc::ioctl(f.as_raw_fd(), PAGEMAP_SCAN as _, &mut arg as *mut PmScanArg) };
            if n >= 0 {
                return n > 0;
            }
        }
        let pages = len / 4096;
        let mut buf = vec![0u8; pages.min(1 << 16) * 8];
        let mut done = 0;
        while done < pages {
            let n = (pages - done).min(1 << 16);
            let off = ((base / 4096 + done) * 8) as u64;
            if f.read_exact_at(&mut buf[..n * 8], off).is_err() {
                return true;
            }
            for e in buf[..n * 8].chunks_exact(8) {
                let v = u64::from_le_bytes(e.try_into().expect("8 bytes"));
                let present = v >> 63 & 1 == 1;
                let file_page = v >> 61 & 1 == 1;
                if present && !file_page {
                    return true;
                }
            }
            done += n;
        }
        false
    }

    /// Route allocations of at least `min_bytes` to files in `dir` (on the
    /// object store's filesystem): NumPy's, once NumPy is loaded (call again
    /// after importing it), and with `raw` Python's raw allocator's too.
    /// Idempotent. False only when `dir` cannot be created.
    #[pyfunction]
    #[pyo3(signature = (dir, min_bytes, cache_bytes=256 << 20, raw=false))]
    pub fn arena_install(
        py: Python<'_>,
        dir: PathBuf,
        min_bytes: usize,
        cache_bytes: usize,
        raw: bool,
    ) -> PyResult<bool> {
        if ARENA.get().is_none() {
            std::fs::create_dir_all(&dir).map_err(os_err)?;
            let ar = Arena {
                dir,
                min: min_bytes.max(4096),
                cache_max: cache_bytes,
                numpy: OnceLock::new(),
                seq: AtomicU64::new(0),
                live: Mutex::new(BTreeMap::new()),
                cache: Mutex::new(Vec::new()),
                cache_bytes: AtomicUsize::new(0),
                lo: AtomicUsize::new(usize::MAX),
                hi: AtomicUsize::new(0),
                enabled: AtomicBool::new(true),
                claims: AtomicU64::new(0),
                fallbacks: AtomicU64::new(0),
            };
            if ARENA.set(ar).is_ok() {
                // SAFETY: registers the process-wide fork handlers, once.
                unsafe { libc::pthread_atfork(Some(atfork_prepare), Some(atfork_parent), Some(atfork_child)) };
            }
        }
        if raw {
            hook_raw();
        }
        hook_numpy(py);
        Ok(true)
    }

    /// Patch NumPy's default allocator, if NumPy is loaded (never importing
    /// it) and not patched yet.
    fn hook_numpy(py: Python<'_>) {
        let ar = arena();
        if ar.numpy.get().is_some() {
            return;
        }
        let Ok(modules) = py.import("sys").and_then(|s| s.getattr("modules")) else { return };
        let mods = ["numpy._core._multiarray_umath", "numpy.core._multiarray_umath"];
        let Some(api) = mods.iter().find_map(|m| modules.get_item(*m).ok()?.getattr("_ARRAY_API").ok()) else {
            return;
        };
        // SAFETY: `_ARRAY_API` is NumPy's C-API table (a capsule of void*[]);
        // entry 305 is PyDataMem_GetHandler (NumPy >= 1.22), which returns a
        // capsule named "mem_handler" holding the current PyDataMem_Handler.
        unsafe {
            let table = pyo3::ffi::PyCapsule_GetPointer(api.as_ptr(), std::ptr::null()) as *const *const c_void;
            if table.is_null() {
                pyo3::ffi::PyErr_Clear();
                return;
            }
            let get_handler: unsafe extern "C" fn() -> *mut pyo3::ffi::PyObject = std::mem::transmute(*table.add(305));
            let cap = get_handler();
            if cap.is_null() {
                pyo3::ffi::PyErr_Clear();
                return;
            }
            let handler = pyo3::ffi::PyCapsule_GetPointer(cap, c"mem_handler".as_ptr()) as *mut Handler;
            pyo3::ffi::Py_DecRef(cap); // the handler struct is static in NumPy
            if handler.is_null() || (*handler).version != 1 {
                pyo3::ffi::PyErr_Clear();
                return;
            }
            let prev = (*handler).allocator;
            if prev.malloc.is_none() || prev.calloc.is_none() || prev.realloc.is_none() || prev.free.is_none() {
                return;
            }
            if ar.numpy.set(Prev(prev)).is_err() {
                return;
            }
            // The handler struct is NumPy's default policy, shared by every
            // context and thread: patching it in place routes them all here.
            (*handler).allocator.malloc = Some(h_malloc);
            (*handler).allocator.calloc = Some(h_calloc);
            (*handler).allocator.realloc = Some(h_realloc);
            (*handler).allocator.free = Some(h_free);
        }
    }

    /// Store the arena allocation holding `buffer` as the file `dest`
    /// (a hard link: no copy) and return the buffer's offset in it, or None
    /// when the buffer is not in the arena or its owner changed it since an
    /// earlier store (the caller then copies).
    #[pyfunction]
    pub fn arena_claim(py: Python<'_>, buffer: &Bound<'_, PyAny>, dest: PathBuf) -> PyResult<Option<u64>> {
        let Some(ar) = ARENA.get() else { return Ok(None) };
        let buf = pyo3::buffer::PyUntypedBuffer::get(buffer)?;
        let (ptr, len) = (buf.buf_ptr() as usize, buf.len_bytes());
        if ptr < ar.lo.load(Ordering::Relaxed) || ptr >= ar.hi.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let r = py.detach(|| -> io::Result<Option<u64>> {
            let mut live = ar.live.lock().expect("lock");
            let Some((&base, a)) = live.range_mut(..=ptr).next_back() else { return Ok(None) };
            if ptr + len > base + a.len || a.foreign {
                return Ok(None);
            }
            if a.published && changed(base, a, ptr, len) {
                return Ok(None);
            }
            let mut linked = std::fs::hard_link(&a.path, &dest);
            if matches!(&linked, Err(e) if e.kind() == io::ErrorKind::NotFound) {
                std::fs::create_dir_all(dest.parent().unwrap_or(Path::new(".")))?;
                linked = std::fs::hard_link(&a.path, &dest); // a concurrent claim may win: checked below
            }
            match linked {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    // Fine if it is this allocation, claimed before. Another file
                    // (a result of the same call, run again on this peer) is never
                    // linked against or removed: the caller copies instead.
                    use std::os::unix::fs::MetadataExt;
                    let id = |p: &Path| std::fs::metadata(p).map(|m| (m.dev(), m.ino())).ok();
                    if id(&a.path).is_none() || id(&a.path) != id(&dest) {
                        return Ok(None);
                    }
                }
                Err(e) => return Err(e),
            }
            if !a.published {
                if !remap_private(base, a) {
                    let _ = std::fs::remove_file(&dest);
                    return Ok(None);
                }
                a.published = true;
                note_written(a.len as u64); // an object now, until the peer's next measurement
            }
            ar.claims.fetch_add(1, Ordering::Relaxed);
            Ok(Some((ptr - base) as u64))
        });
        drop(buf);
        r.map_err(os_err)
    }

    /// (live allocations, live bytes, cached bytes, claims, heap fallbacks).
    #[pyfunction]
    pub fn arena_stats() -> (u64, u64, u64, u64, u64) {
        let Some(ar) = ARENA.get() else { return (0, 0, 0, 0, 0) };
        let live = ar.live.lock().expect("lock");
        (
            live.len() as u64,
            live.values().map(|a| a.len as u64).sum(),
            ar.cache_bytes.load(Ordering::Relaxed) as u64,
            ar.claims.load(Ordering::Relaxed),
            ar.fallbacks.load(Ordering::Relaxed),
        )
    }

    /// At exit: drop cached mappings and every name in this process's arena
    /// directory. Live arrays keep their pages until unmapped; stored objects
    /// keep theirs through their own links.
    #[pyfunction]
    pub fn arena_shutdown() {
        let Some(ar) = ARENA.get() else { return };
        ar.enabled.store(false, Ordering::Relaxed);
        for (b, a) in ar.cache.lock().expect("lock").drain(..) {
            release(b, &a);
        }
        ar.cache_bytes.store(0, Ordering::Relaxed);
        for a in ar.live.lock().expect("lock").values() {
            if !a.foreign {
                let _ = std::fs::remove_file(&a.path);
            }
        }
        let _ = std::fs::remove_dir(&ar.dir);
    }
}

/// Whether `obj` is plain data that pickle's fast mode (no memo) encodes
/// exactly: built only from str, bytes, int, float, bool and None inside
/// dicts, lists and tuples, with no container reachable twice (so no cycles
/// and no shared containers whose identity a memo would preserve). The memo
/// is most of the cost of pickling large containers of strings.
#[pyfunction]
pub fn plain_tree(obj: &Bound<'_, PyAny>) -> bool {
    use pyo3::ffi;
    /// Containers worth tracking before giving up (memory for the seen set).
    const MAX_CONTAINERS: usize = 1 << 22;
    // SAFETY: the GIL is held throughout and no Python code runs, so the
    // borrowed references from PyDict_Next / PyList_GetItem / PyTuple_GetItem
    // stay valid while the walk uses them.
    unsafe {
        let (t_dict, t_list, t_tuple) =
            (&raw mut ffi::PyDict_Type, &raw mut ffi::PyList_Type, &raw mut ffi::PyTuple_Type);
        let atoms = [
            &raw mut ffi::PyUnicode_Type,
            &raw mut ffi::PyLong_Type,
            &raw mut ffi::PyFloat_Type,
            &raw mut ffi::PyBytes_Type,
            &raw mut ffi::PyBool_Type,
        ];
        let none = ffi::Py_None();
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut stack: Vec<*mut ffi::PyObject> = Vec::new();
        // Atoms are checked in place; only containers go on the stack.
        let mut visit = |o: *mut ffi::PyObject, stack: &mut Vec<*mut ffi::PyObject>| -> bool {
            if o == none {
                return true;
            }
            let t = ffi::Py_TYPE(o);
            if atoms.contains(&t) {
                return true;
            }
            if t == t_dict || t == t_list || t == t_tuple {
                if !seen.insert(o as usize) || seen.len() > MAX_CONTAINERS {
                    return false;
                }
                stack.push(o);
                return true;
            }
            false
        };
        if !visit(obj.as_ptr(), &mut stack) {
            return false;
        }
        while let Some(o) = stack.pop() {
            let t = ffi::Py_TYPE(o);
            if t == t_dict {
                let (mut pos, mut k, mut v) = (0, std::ptr::null_mut(), std::ptr::null_mut());
                while ffi::PyDict_Next(o, &mut pos, &mut k, &mut v) != 0 {
                    if !visit(k, &mut stack) || !visit(v, &mut stack) {
                        return false;
                    }
                }
            } else if t == t_list {
                for i in 0..ffi::PyList_Size(o) {
                    if !visit(ffi::PyList_GetItem(o, i), &mut stack) {
                        return false;
                    }
                }
            } else {
                for i in 0..ffi::PyTuple_Size(o) {
                    if !visit(ffi::PyTuple_GetItem(o, i), &mut stack) {
                        return false;
                    }
                }
            }
        }
        true
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(plain_tree, m)?)?;
    m.add_function(wrap_pyfunction!(objects_config, m)?)?;
    m.add_function(wrap_pyfunction!(write_object, m)?)?;
    m.add_function(wrap_pyfunction!(pread_heap, m)?)?;
    m.add_function(wrap_pyfunction!(helper_cpu_ns, m)?)?;
    m.add_function(wrap_pyfunction!(encode_utf8_heap, m)?)?;
    m.add_function(wrap_pyfunction!(arena_install, m)?)?;
    m.add_function(wrap_pyfunction!(arena_claim, m)?)?;
    m.add_function(wrap_pyfunction!(arena_stats, m)?)?;
    m.add_function(wrap_pyfunction!(arena_shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(arena_pool_api, m)?)?;
    Ok(())
}

#[cfg(test)]
mod reserve_tests {
    use super::*;

    #[test]
    fn reserve_files_are_taken_from_one_listing_and_listed_again_when_it_runs_out() {
        let dir = std::env::temp_dir().join(format!("chimaera-reserve-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for n in ["20-1", "20-2", "21-1"] {
            std::fs::write(dir.join(n), b"").unwrap();
        }
        let to = |i: u32| dir.join(format!("taken-{i}"));
        assert!(take_reserve(&dir, 1 << 20, &to(1)), "from the listing");
        let left: Vec<_> = ["20-1", "20-2"].into_iter().filter(|n| dir.join(n).exists()).collect();
        assert_eq!(left.len(), 1);
        std::fs::remove_file(dir.join(left[0])).unwrap(); // another process took it after the listing
        assert!(take_reserve(&dir, 1 << 20, &to(2)), "the next listed file, of the class above");
        assert!(!dir.join("21-1").exists());
        std::fs::write(dir.join("20-3"), b"").unwrap(); // the peer filled the reserve since
        assert!(take_reserve(&dir, 1 << 20, &to(3)), "listed again when the classes ran out");
        assert!(!dir.join("20-3").exists() && to(3).exists());
        assert!(!take_reserve(&dir, 1 << 20, &to(4)), "a miss");
        let markers = std::fs::read_dir(&dir).unwrap().flatten();
        assert_eq!(markers.filter(|e| e.file_name().to_string_lossy().starts_with("want-20-")).count(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
