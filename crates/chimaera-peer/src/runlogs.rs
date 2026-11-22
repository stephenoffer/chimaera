//! Each run's output, kept on the peer that ran its calls, so `chimaera logs
//! RUN` and `Run.logs()` work after the driver has gone (a detached run, or
//! one you attach to later).
//!
//! One append-only file per run, `DIR/logs/runs/<run>.log`. Each line is
//! `<task> <stream>| <text>` (task: the first 12 hex digits of its id). A run
//! keeps at most [`CAP`] bytes; output beyond that is dropped with one note.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::stores::{CodeStore, hex};

pub const CAP: u64 = 64 << 20;
/// Open files kept at once; older ones are closed and reopened on demand.
const OPEN: usize = 64;

/// Where `run`'s output lives on this peer.
pub fn path(code: &CodeStore, run: &[u8]) -> PathBuf {
    let root = code.dir().parent().unwrap_or_else(|| code.dir());
    root.join("logs").join("runs").join(format!("{}.log", hex(run)))
}

#[derive(Default)]
pub struct RunLogs {
    open: HashMap<Vec<u8>, (File, u64)>,
}

impl RunLogs {
    /// `run` expired: its output goes with it.
    pub fn remove(&mut self, code: &CodeStore, run: &[u8]) {
        self.open.remove(run);
        let _ = std::fs::remove_file(path(code, run));
    }

    /// Append one chunk of a call's output.
    pub fn append(&mut self, code: &CodeStore, run: &[u8], task: &[u8], stream: &str, text: &str) -> io::Result<()> {
        if !self.open.contains_key(run) {
            if self.open.len() >= OPEN {
                self.open.clear(); // reopened as needed; appends never lose bytes
            }
            let p = path(code, run);
            if let Some(dir) = p.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(&p)?;
            let size = file.metadata()?.len();
            self.open.insert(run.to_vec(), (file, size));
        }
        let (file, size) = self.open.get_mut(run).expect("opened above");
        if *size >= CAP {
            return Ok(());
        }
        let task = hex(task);
        let task = &task[..task.len().min(12)];
        let mut out = String::new();
        for line in text.split_inclusive('\n') {
            out.push_str(task);
            out.push(' ');
            out.push_str(stream);
            out.push_str("| ");
            out.push_str(line);
            if !line.ends_with('\n') {
                out.push('\n');
            }
        }
        if *size + out.len() as u64 > CAP {
            out = format!("[chimaera: this run's output passed {} MiB; the rest is not kept]\n", CAP >> 20);
            *size = CAP;
        } else {
            *size += out.len() as u64;
        }
        file.write_all(out.as_bytes())
    }

    /// Close a run's file (it stays on disk for `chimaera logs`).
    pub fn close(&mut self, run: &[u8]) {
        self.open.remove(run);
    }
}

/// Up to `max` bytes of a run's output from `offset`, ending at a line
/// boundary when one is in reach: (bytes, next offset, current size). A run
/// with no output here is empty.
pub fn read(path: &Path, offset: u64, max: usize) -> io::Result<(Vec<u8>, u64, u64)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), offset, 0)),
        Err(e) => return Err(e),
    };
    let size = file.metadata()?.len();
    if offset >= size {
        return Ok((Vec::new(), offset, size));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0; max.min((size - offset) as usize)];
    file.read_exact(&mut buf)?;
    if offset + (buf.len() as u64) < size
        && let Some(end) = buf.iter().rposition(|&b| b == b'\n')
    {
        buf.truncate(end + 1);
    }
    let next = offset + buf.len() as u64;
    Ok((buf, next, size))
}

/// Answer a `Logs` request from this peer's copy: up to 1 MiB after
/// `offset`, waiting up to `wait_ms` (at most 30 s) for some.
pub async fn serve(code: &CodeStore, r: &chimaera_core::pb::LogsRequest) -> io::Result<chimaera_core::pb::LogsReply> {
    let path = path(code, &r.run);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(u64::from(r.wait_ms.min(30_000)));
    loop {
        let (p, offset) = (path.clone(), r.offset);
        let (text, next, size) =
            tokio::task::spawn_blocking(move || read(&p, offset, 1 << 20)).await.map_err(io::Error::other)??;
        if !text.is_empty() || tokio::time::Instant::now() >= deadline {
            return Ok(chimaera_core::pb::LogsReply { text, next, size });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expired_runs_output_is_deleted() {
        let root = tempfile::tempdir().unwrap();
        let code = CodeStore::open(root.path().join("code")).unwrap();
        let mut logs = RunLogs::default();
        let (run, task) = ([9u8; 16], [1u8; 16]);
        logs.append(&code, &run, &task, "stdout", "kept until the run expires\n").unwrap();
        assert!(path(&code, &run).exists());
        logs.remove(&code, &run);
        assert!(!path(&code, &run).exists());
        logs.append(&code, &run, &task, "stdout", "a late line reopens it\n").unwrap();
        assert!(path(&code, &run).exists(), "no stale handle to a deleted file");
    }

    #[test]
    fn appends_prefixed_lines_reads_from_offsets_and_caps_a_run() {
        let root = std::env::temp_dir().join(format!("chimaera-runlogs-{}", std::process::id()));
        let code = CodeStore::open(root.join("code")).unwrap();
        let mut logs = RunLogs::default();
        let (run, task) = ([7u8; 16], [0xab; 16]);
        logs.append(&code, &run, &task, "stdout", "hello\nworld\n").unwrap();
        logs.append(&code, &run, &task, "stderr", "oops").unwrap();
        let p = path(&code, &run);
        assert!(p.starts_with(root.join("logs").join("runs")));
        let (all, next, size) = read(&p, 0, 1 << 20).unwrap();
        assert_eq!(
            String::from_utf8(all).unwrap(),
            "abababababab stdout| hello\nabababababab stdout| world\nabababababab stderr| oops\n"
        );
        assert_eq!(next, size);
        // A small read stops at a line boundary; the next one resumes there.
        let (first, n1, _) = read(&p, 0, 40).unwrap();
        assert_eq!(String::from_utf8(first).unwrap(), "abababababab stdout| hello\n");
        let (rest, _, _) = read(&p, n1, 1 << 20).unwrap();
        assert!(String::from_utf8(rest).unwrap().starts_with("abababababab stdout| world"));
        // Unknown runs read as empty; a run's output is capped with one note.
        assert_eq!(read(&path(&code, &[1; 16]), 0, 10).unwrap(), (Vec::new(), 0, 0));
        let big = "x".repeat(1 << 20);
        for _ in 0..70 {
            logs.append(&code, &run, &task, "stdout", &big).unwrap();
        }
        let len = std::fs::metadata(&p).unwrap().len();
        assert!(len <= CAP + 200, "{len}");
        let (tail, _, _) = read(&p, len - 120, 200).unwrap();
        assert!(String::from_utf8_lossy(&tail).contains("the rest is not kept"));
        std::fs::remove_dir_all(&root).unwrap();
    }
}
