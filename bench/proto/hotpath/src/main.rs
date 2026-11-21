//! Prototype of the call path in docs/architecture.md §3 (D2): calls are
//! journaled, not committed.
//!
//! `hotpath agent SOCK BACKUP_SOCK...` is a peer agent. Drivers and executors
//! connect to SOCK. A call frame from a driver is appended to the journal
//! buffer and forwarded to its actor's executor at once; the result frame is
//! appended and forwarded to the driver at once. A replicator thread ships the
//! journal to every backup every 1 ms or 64 KiB and waits for their acks, off
//! the call's path. An `external` call is held until its record is safe on
//! every backup, as D2 requires for `effects="external"`.
//!
//! `hotpath backup SOCK` is a backup: it applies each batch to a table of
//! unfinished calls and acknowledges it.
//!
//! What it leaves out: placement, admission, accounting, failover (D3) and the
//! real shard state machine. Backups apply into a hash map.
//!
//! Frame: u32 length of the rest, u8 kind, u64 call, u32 actor, u32 driver,
//! payload. Journal record: u8 kind, u32 length, the frame.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{env, io, thread};

const CALL: u8 = 1;
const RESULT: u8 = 2;
const HELLO_DRIVER: u8 = 3;
const HELLO_EXEC: u8 = 4;
const CALL_EXT: u8 = 5;
const STATS: u8 = 6;

const SUBMITTED: u8 = 1;
const STARTED: u8 = 2;
const DONE: u8 = 3;

const HDR: usize = 21;
const FLUSH_EVERY: Duration = Duration::from_millis(1);
const FLUSH_BYTES: usize = 64 << 10;
/// Unsafe journal bytes (buffered plus in flight) at which submissions wait.
const MAX_UNSAFE: usize = 8 << 20;

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}

fn frame(kind: u8, call: u64, actor: u32, driver: u32, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(HDR + payload.len());
    f.extend_from_slice(&((HDR - 4 + payload.len()) as u32).to_le_bytes());
    f.push(kind);
    f.extend_from_slice(&call.to_le_bytes());
    f.extend_from_slice(&actor.to_le_bytes());
    f.extend_from_slice(&driver.to_le_bytes());
    f.extend_from_slice(payload);
    f
}

/// Read `s` until EOF, calling `on` with every run of complete frames.
fn pump(mut s: UnixStream, mut on: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
    let mut buf = vec![0u8; 1 << 20];
    let mut have = 0;
    loop {
        if have == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }
        let n = s.read(&mut buf[have..])?;
        if n == 0 {
            return Ok(());
        }
        have += n;
        let mut end = 0;
        while have - end >= 4 {
            let len = 4 + u32_at(&buf, end) as usize;
            if have - end < len {
                break;
            }
            end += len;
        }
        if end > 0 {
            on(&buf[..end])?;
            buf.copy_within(end..have, 0);
            have -= end;
        }
    }
}

fn frames(mut b: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        if b.is_empty() {
            return None;
        }
        let len = 4 + u32_at(b, 0) as usize;
        let (f, rest) = b.split_at(len);
        b = rest;
        Some(f)
    })
}

#[derive(Default)]
struct Journal {
    buf: Vec<u8>,
    oldest: Option<Instant>,
    /// External calls waiting for their `Started` record to be safe.
    held: Vec<Vec<u8>>,
    /// Bytes of the batch being replicated.
    inflight: usize,
}

impl Journal {
    fn append(&mut self, kind: u8, f: &[u8]) -> bool {
        let first = self.oldest.is_none();
        if first {
            self.oldest = Some(Instant::now());
        }
        self.buf.push(kind);
        self.buf.extend_from_slice(&(f.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(f);
        first || self.buf.len() >= FLUSH_BYTES
    }
}

#[derive(Default)]
struct Agent {
    execs: RwLock<HashMap<u32, Mutex<UnixStream>>>,
    drivers: RwLock<HashMap<u32, Mutex<UnixStream>>>,
    next_driver: AtomicU32,
    journal: Mutex<Journal>,
    wake: Condvar,
    /// Signalled when acks make room under MAX_UNSAFE.
    room: Condvar,
    calls: AtomicU64,
    shipped_bytes: AtomicU64,
    batches: AtomicU64,
    /// Microseconds from a batch's first record to both acks, per batch.
    lags: Mutex<Vec<u32>>,
}

/// Group frames by destination and write each group with one call.
fn send(dest: &RwLock<HashMap<u32, Mutex<UnixStream>>>, out: &mut HashMap<u32, Vec<u8>>) -> io::Result<()> {
    let map = dest.read().unwrap();
    for (id, bytes) in out.iter_mut().filter(|(_, b)| !b.is_empty()) {
        let conn = map.get(id).ok_or_else(|| io::Error::other(format!("no connection {id}")))?;
        conn.lock().unwrap().write_all(bytes)?;
        bytes.clear();
    }
    Ok(())
}

fn stats(a: &Agent) -> String {
    let mut lags = a.lags.lock().unwrap().clone();
    lags.sort_unstable();
    let pct = |p: f64| lags.get(((lags.len() as f64 * p) as usize).min(lags.len().saturating_sub(1))).copied().unwrap_or(0);
    format!(
        r#"{{"calls":{},"journal_bytes":{},"batches":{},"lag_us_p50":{},"lag_us_p99":{},"lag_us_max":{}}}"#,
        a.calls.load(Relaxed),
        a.shipped_bytes.load(Relaxed),
        a.batches.load(Relaxed),
        pct(0.5),
        pct(0.99),
        lags.last().copied().unwrap_or(0)
    )
}

fn serve_driver(a: Arc<Agent>, s: UnixStream) -> io::Result<()> {
    let id = a.next_driver.fetch_add(1, Relaxed) + 1;
    a.drivers.write().unwrap().insert(id, Mutex::new(s.try_clone()?));
    a.drivers.read().unwrap()[&id].lock().unwrap().write_all(&frame(HELLO_DRIVER, 0, 0, id, b""))?;
    let mut out: HashMap<u32, Vec<u8>> = HashMap::new();
    pump(s, |batch| {
        let mut wake = false;
        {
            let mut j = a.journal.lock().unwrap();
            while j.buf.len() + j.inflight >= MAX_UNSAFE {
                j = a.room.wait(j).unwrap();
            }
            for f in frames(batch) {
                match f[4] {
                    CALL => {
                        wake |= j.append(SUBMITTED, f);
                        out.entry(u32_at(f, 13)).or_default().extend_from_slice(f);
                    }
                    CALL_EXT => {
                        j.append(STARTED, f);
                        j.held.push(f.to_vec());
                        wake = true;
                    }
                    STATS => {
                        out.clear();
                        let reply = frame(STATS, 0, 0, id, stats(&a).as_bytes());
                        drop(j);
                        return a.drivers.read().unwrap()[&id].lock().unwrap().write_all(&reply);
                    }
                    k => return Err(io::Error::other(format!("driver sent kind {k}"))),
                }
            }
        }
        if wake {
            a.wake.notify_one();
        }
        send(&a.execs, &mut out)
    })
}

fn serve_executor(a: Arc<Agent>, s: UnixStream, actor: u32) -> io::Result<()> {
    a.execs.write().unwrap().insert(actor, Mutex::new(s.try_clone()?));
    let mut out: HashMap<u32, Vec<u8>> = HashMap::new();
    pump(s, |batch| {
        let mut wake = false;
        let mut n = 0;
        {
            let mut j = a.journal.lock().unwrap();
            for f in frames(batch) {
                if f[4] != RESULT {
                    return Err(io::Error::other(format!("executor sent kind {}", f[4])));
                }
                wake |= j.append(DONE, f);
                out.entry(u32_at(f, 17)).or_default().extend_from_slice(f);
                n += 1;
            }
        }
        a.calls.fetch_add(n, Relaxed);
        if wake {
            a.wake.notify_one();
        }
        send(&a.drivers, &mut out)
    })
}

/// Ship journal batches to every backup; one batch in flight at a time.
fn replicate(a: Arc<Agent>, mut backups: Vec<UnixStream>) -> io::Result<()> {
    let mut out: HashMap<u32, Vec<u8>> = HashMap::new();
    loop {
        let (batch, held, oldest) = {
            let mut j = a.journal.lock().unwrap();
            loop {
                if !j.held.is_empty() || j.buf.len() >= FLUSH_BYTES {
                    break;
                }
                match j.oldest {
                    Some(t) if t.elapsed() >= FLUSH_EVERY => break,
                    Some(t) => j = a.wake.wait_timeout(j, FLUSH_EVERY - t.elapsed()).unwrap().0,
                    None => j = a.wake.wait(j).unwrap(),
                }
            }
            let oldest = j.oldest.take().unwrap();
            j.inflight = j.buf.len();
            (std::mem::take(&mut j.buf), std::mem::take(&mut j.held), oldest)
        };
        let len = (batch.len() as u32).to_le_bytes();
        for b in &mut backups {
            b.write_all(&len)?;
            b.write_all(&batch)?;
        }
        let mut ack = [0u8; 8];
        for b in &mut backups {
            b.read_exact(&mut ack)?;
        }
        a.lags.lock().unwrap().push(oldest.elapsed().as_micros() as u32);
        a.journal.lock().unwrap().inflight = 0;
        a.room.notify_all();
        a.batches.fetch_add(1, Relaxed);
        a.shipped_bytes.fetch_add((batch.len() * backups.len()) as u64, Relaxed);
        for f in &held {
            out.entry(u32_at(f, 13)).or_default().extend_from_slice(f);
        }
        send(&a.execs, &mut out)?;
    }
}

fn agent(sock: &str, backup_socks: &[String]) -> io::Result<()> {
    let a = Arc::new(Agent::default());
    let backups = backup_socks.iter().map(UnixStream::connect).collect::<io::Result<Vec<_>>>()?;
    let r = a.clone();
    thread::spawn(move || {
        if let Err(e) = replicate(r, backups) {
            eprintln!("replicator: {e}");
            std::process::exit(1);
        }
    });
    let _ = std::fs::remove_file(sock);
    for conn in UnixListener::bind(sock)?.incoming() {
        let mut s = conn?;
        let a = a.clone();
        thread::spawn(move || {
            let mut hello = [0u8; HDR];
            let r = s.read_exact(&mut hello).and_then(|()| match hello[4] {
                HELLO_DRIVER => serve_driver(a, s),
                HELLO_EXEC => serve_executor(a, s, u32_at(&hello, 13)),
                k => Err(io::Error::other(format!("unknown hello {k}"))),
            });
            if let Err(e) = r {
                eprintln!("connection: {e}");
            }
        });
    }
    Ok(())
}

fn backup(sock: &str) -> io::Result<()> {
    let _ = std::fs::remove_file(sock);
    let (mut s, _) = UnixListener::bind(sock)?.accept()?;
    let mut open: HashMap<(u32, u64), Vec<u8>> = HashMap::new();
    let mut applied: u64 = 0;
    let mut len = [0u8; 4];
    let mut batch = Vec::new();
    loop {
        if s.read_exact(&mut len).is_err() {
            return Ok(());
        }
        batch.resize(u32::from_le_bytes(len) as usize, 0);
        s.read_exact(&mut batch)?;
        let mut i = 0;
        while i < batch.len() {
            let kind = batch[i];
            let n = u32_at(&batch, i + 1) as usize;
            let f = &batch[i + 5..i + 5 + n];
            let key = (u32_at(f, 17), u64::from_le_bytes(f[5..13].try_into().unwrap()));
            match kind {
                SUBMITTED | STARTED => {
                    open.insert(key, f.to_vec());
                }
                DONE => {
                    open.remove(&key);
                }
                k => return Err(io::Error::other(format!("record kind {k}"))),
            }
            applied += 1;
            i += 5 + n;
        }
        s.write_all(&applied.to_le_bytes())?;
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("agent") if args.len() >= 3 => agent(&args[2], &args[3..]),
        Some("backup") if args.len() == 3 => backup(&args[2]),
        _ => {
            eprintln!("usage: hotpath agent SOCK [BACKUP_SOCK...] | hotpath backup SOCK");
            std::process::exit(2);
        }
    }
}
