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

use pyo3::exceptions::{PyOSError, PyValueError};
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

/// Free bytes on the filesystem holding `f`.
#[allow(clippy::unnecessary_cast)] // statvfs field widths differ by platform
fn free_bytes(f: &std::fs::File) -> Option<u64> {
    use std::os::fd::AsRawFd;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: fstatvfs fills `st` for a valid descriptor.
    (unsafe { libc::fstatvfs(f.as_raw_fd(), &mut st) } == 0).then(|| st.f_bavail as u64 * st.f_frsize as u64)
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

/// Write `parts` (concatenated) to `path` atomically: a temporary name, then
/// a rename. Returns the bytes written.
pub fn write_parts(path: &Path, parts: &[&[u8]]) -> io::Result<u64> {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let tmp = tmp_name(path);
    let file = create_new(&tmp)?;
    let result = (|| {
        if let Some(free) = free_bytes(&file)
            && (free as usize) < total
        {
            return Err(no_space(path, total, free));
        }
        if total < PARALLEL_MIN || !cfg!(target_os = "linux") {
            let mut off = 0u64;
            for p in parts {
                file.write_all_at(p, off)?;
                off += p.len() as u64;
            }
            return Ok(());
        }
        file.set_len(total as u64)?;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
    let chunks = total.div_ceil(CHUNK);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()).clamp(1, MAX_THREADS).min(chunks);
    let next = AtomicUsize::new(0);
    let failed: std::sync::Mutex<Option<io::Error>> = std::sync::Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                loop {
                    let c = next.fetch_add(1, Ordering::Relaxed);
                    if c >= chunks || failed.lock().expect("lock").is_some() {
                        return;
                    }
                    let (cs, ce) = (c * CHUNK, ((c + 1) * CHUNK).min(total));
                    if let Err(e) = populate_write(base_addr + cs, ce - cs) {
                        *failed.lock().expect("lock") = Some(e);
                        return;
                    }
                    for (i, p) in parts.iter().enumerate() {
                        let (ps, pe) = (starts[i], starts[i] + p.len());
                        let (s0, e0) = (ps.max(cs), pe.min(ce));
                        if s0 < e0 {
                            // SAFETY: [s0, e0) lies inside the mapping and inside
                            // part i; chunks are disjoint, so no two threads write
                            // the same bytes.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    p.as_ptr().add(s0 - ps),
                                    (base_addr + s0) as *mut u8,
                                    e0 - s0,
                                );
                            }
                        }
                    }
                }
            });
        }
    });
    // SAFETY: unmapping the mapping created above.
    unsafe { libc::munmap(base, total) };
    match failed.into_inner().expect("lock") {
        Some(e) => Err(e),
        None => Ok(()),
    }
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

#[cfg(target_os = "linux")]
pub use arena::{arena_claim, arena_install, arena_shutdown, arena_stats};

#[cfg(not(target_os = "linux"))]
mod portable {
    use super::*;

    /// The arena needs Linux (tmpfs, MAP_FIXED remaps, pagemap).
    #[pyfunction]
    #[pyo3(signature = (dir, min_bytes, cache_bytes=0))]
    pub fn arena_install(dir: PathBuf, min_bytes: usize, cache_bytes: usize) -> bool {
        let _ = (dir, min_bytes, cache_bytes);
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
}

#[cfg(not(target_os = "linux"))]
pub use portable::{arena_claim, arena_install, arena_shutdown, arena_stats};

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
        prev: Prev,
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
            let c = cstr(&path);
            // SAFETY: plain syscalls on a path we own; every error is checked.
            unsafe {
                let mut fd =
                    libc::open(c.as_ptr(), libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC, 0o600);
                if fd < 0 && *libc::__errno_location() == libc::ENOENT {
                    let _ = std::fs::create_dir_all(&self.dir);
                    fd = libc::open(c.as_ptr(), libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC, 0o600);
                }
                if fd < 0 {
                    return std::ptr::null_mut();
                }
                let ok = self.fs_has_room(fd, len) && libc::ftruncate(fd, len as libc::off_t) == 0;
                let base = if ok {
                    libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0)
                } else {
                    libc::MAP_FAILED
                };
                libc::close(fd);
                if base == libc::MAP_FAILED {
                    libc::unlink(c.as_ptr());
                    return std::ptr::null_mut();
                }
                let b = base as usize;
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
        unsafe { ar.prev.0.malloc.expect("malloc")(ctx, size) }
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
        unsafe { ar.prev.0.calloc.expect("calloc")(ctx, n, el) }
    }

    unsafe extern "C" fn h_realloc(ctx: *mut c_void, ptr: *mut c_void, new: usize) -> *mut c_void {
        let ar = arena();
        let p = ptr as usize;
        let old = if p >= ar.lo.load(Ordering::Relaxed) && p < ar.hi.load(Ordering::Relaxed) {
            ar.live.lock().expect("lock").get(&p).map(|a| a.len)
        } else {
            None
        };
        let Some(old_len) = old else {
            // SAFETY: not ours: NumPy's allocator owns `ptr`.
            return unsafe { ar.prev.0.realloc.expect("realloc")(ctx, ptr, new) };
        };
        // Ours: move into a new allocation (ours or the heap's) and copy.
        let mut q = if new >= ar.min { ar.alloc(new, false) } else { std::ptr::null_mut() };
        if q.is_null() {
            // SAFETY: NumPy's malloc with its context.
            q = unsafe { ar.prev.0.malloc.expect("malloc")(ctx, new.max(1)) };
            if q.is_null() {
                return q;
            }
        }
        // SAFETY: both ranges are valid for min(old_len, new) bytes and distinct.
        unsafe { std::ptr::copy_nonoverlapping(ptr as *const u8, q as *mut u8, old_len.min(new)) };
        ar.free(p);
        q
    }

    unsafe extern "C" fn h_free(ctx: *mut c_void, ptr: *mut c_void, size: usize) {
        let ar = arena();
        if !ar.free(ptr as usize) {
            // SAFETY: not ours: NumPy's allocator owns `ptr`.
            unsafe { ar.prev.0.free.expect("free")(ctx, ptr, size) }
        }
    }

    type Guards =
        (std::sync::MutexGuard<'static, BTreeMap<usize, Alloc>>, std::sync::MutexGuard<'static, Vec<(usize, Alloc)>>);

    thread_local! {
        /// The arena's locks, held by the forking thread across fork() so the
        /// child never inherits them locked by a thread it does not have.
        static FORK_GUARDS: std::cell::RefCell<Option<Guards>> = const { std::cell::RefCell::new(None) };
    }

    extern "C" fn atfork_prepare() {
        if let Some(ar) = ARENA.get() {
            let g = (ar.live.lock().expect("lock"), ar.cache.lock().expect("lock"));
            FORK_GUARDS.with(|f| *f.borrow_mut() = Some(g));
        }
    }

    extern "C" fn atfork_parent() {
        FORK_GUARDS.with(|f| drop(f.borrow_mut().take()));
    }

    /// The child shares the parent's files: make its mappings private (so its
    /// writes stay its own), never unlink them, and allocate from the heap.
    extern "C" fn atfork_child() {
        let Some(ar) = ARENA.get() else { return };
        ar.enabled.store(false, Ordering::Relaxed);
        let Some((mut live, mut cache)) = FORK_GUARDS.with(|f| f.borrow_mut().take()) else { return };
        for (&b, a) in live.iter_mut() {
            a.foreign = true;
            remap_private(b, a);
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

    /// Whether the owner wrote to any page of [base, base+len) since the
    /// mapping became private (a written page is anonymous, not the file's).
    fn dirty(base: usize, len: usize) -> bool {
        let Ok(f) = std::fs::File::open("/proc/self/pagemap") else { return true };
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

    /// Route NumPy allocations of at least `min_bytes` to files in `dir` (on
    /// the object store's filesystem). Idempotent; false when NumPy's handler
    /// API is missing or not the expected version.
    #[pyfunction]
    #[pyo3(signature = (dir, min_bytes, cache_bytes=256 << 20))]
    pub fn arena_install(py: Python<'_>, dir: PathBuf, min_bytes: usize, cache_bytes: usize) -> PyResult<bool> {
        if ARENA.get().is_some() {
            return Ok(true);
        }
        let mods = ["numpy._core._multiarray_umath", "numpy.core._multiarray_umath"];
        let Some(api) = mods.iter().find_map(|m| py.import(*m).ok().and_then(|m| m.getattr("_ARRAY_API").ok())) else {
            return Ok(false);
        };
        std::fs::create_dir_all(&dir).map_err(os_err)?;
        // SAFETY: `_ARRAY_API` is NumPy's C-API table (a capsule of void*[]);
        // entry 305 is PyDataMem_GetHandler (NumPy >= 1.22), which returns a
        // capsule named "mem_handler" holding the current PyDataMem_Handler.
        unsafe {
            let table = pyo3::ffi::PyCapsule_GetPointer(api.as_ptr(), std::ptr::null()) as *const *const c_void;
            if table.is_null() {
                pyo3::ffi::PyErr_Clear();
                return Ok(false);
            }
            let get_handler: unsafe extern "C" fn() -> *mut pyo3::ffi::PyObject = std::mem::transmute(*table.add(305));
            let cap = get_handler();
            if cap.is_null() {
                pyo3::ffi::PyErr_Clear();
                return Ok(false);
            }
            let handler = pyo3::ffi::PyCapsule_GetPointer(cap, c"mem_handler".as_ptr()) as *mut Handler;
            pyo3::ffi::Py_DecRef(cap); // the handler struct is static in NumPy
            if handler.is_null() || (*handler).version != 1 {
                pyo3::ffi::PyErr_Clear();
                return Ok(false);
            }
            let prev = (*handler).allocator;
            if prev.malloc.is_none() || prev.calloc.is_none() || prev.realloc.is_none() || prev.free.is_none() {
                return Ok(false);
            }
            let ar = Arena {
                dir,
                min: min_bytes.max(4096),
                cache_max: cache_bytes,
                prev: Prev(prev),
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
            if ARENA.set(ar).is_err() {
                return Ok(true);
            }
            libc::pthread_atfork(Some(atfork_prepare), Some(atfork_parent), Some(atfork_child));
            // The handler struct is NumPy's default policy, shared by every
            // context and thread: patching it in place routes them all here.
            (*handler).allocator.malloc = Some(h_malloc);
            (*handler).allocator.calloc = Some(h_calloc);
            (*handler).allocator.realloc = Some(h_realloc);
            (*handler).allocator.free = Some(h_free);
        }
        Ok(true)
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
            if a.published && dirty(base, a.len) {
                return Ok(None);
            }
            match std::fs::hard_link(&a.path, &dest) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    std::fs::create_dir_all(dest.parent().unwrap_or(Path::new(".")))?;
                    std::fs::hard_link(&a.path, &dest)?;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
            if !a.published {
                if !remap_private(base, a) {
                    let _ = std::fs::remove_file(&dest);
                    return Ok(None);
                }
                a.published = true;
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

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(write_object, m)?)?;
    m.add_function(wrap_pyfunction!(arena_install, m)?)?;
    m.add_function(wrap_pyfunction!(arena_claim, m)?)?;
    m.add_function(wrap_pyfunction!(arena_stats, m)?)?;
    m.add_function(wrap_pyfunction!(arena_shutdown, m)?)?;
    Ok(())
}
