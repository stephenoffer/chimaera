//! Disk a peer would otherwise keep forever: executors' logs (one per
//! executor started) and unpacked projects and their environments that no
//! run has used for a while. A run's own output goes when the run expires
//! (`RunLogs::remove`); objects have their own lifetimes.

use std::path::Path;
use std::time::{Duration, SystemTime};

/// Executor logs are kept this long after their last write.
const EXECUTOR_LOGS: Duration = Duration::from_secs(2 * 24 * 3600);
/// Projects and environments are kept this long after a run last used one.
const UNUSED: Duration = Duration::from_secs(14 * 24 * 3600);

/// Remove what has aged out under the peer directory `dir`; returns how many entries.
pub fn sweep(dir: &Path, now: SystemTime) -> usize {
    let executor_log = |n: &str| n.starts_with("executor-") && n.ends_with(".log");
    let unpacked = |n: &str| !n.starts_with('.');
    remove_older(&dir.join("logs"), executor_log, EXECUTOR_LOGS, now)
        + remove_older(&dir.join("projects"), unpacked, UNUSED, now)
        + remove_older(&dir.join("envs"), unpacked, UNUSED, now)
}

fn remove_older(dir: &Path, wanted: impl Fn(&str) -> bool, age: Duration, now: SystemTime) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let mut removed = 0;
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let old =
            e.metadata().and_then(|m| m.modified()).is_ok_and(|t| now.duration_since(t).unwrap_or_default() > age);
        if !wanted(&name) || !old {
            continue;
        }
        let path = e.path();
        let gone = if path.is_dir() { std::fs::remove_dir_all(&path) } else { std::fs::remove_file(&path) };
        if gone.is_ok() {
            removed += 1;
        }
    }
    removed
}

/// Sweep `dir` hourly for as long as the peer runs.
pub async fn every_hour(dir: std::path::PathBuf) {
    let mut tick = tokio::time::interval(Duration::from_secs(3600));
    loop {
        tick.tick().await;
        let d = dir.clone();
        if let Ok(n) = tokio::task::spawn_blocking(move || sweep(&d, SystemTime::now())).await
            && n > 0
        {
            tracing::info!(removed = n, "removed aged executor logs, projects and environments");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aged(path: &Path, days: u64) {
        let t = SystemTime::now() - Duration::from_secs(days * 24 * 3600);
        std::fs::File::options().write(true).open(path).unwrap().set_modified(t).unwrap();
    }

    #[test]
    fn only_what_aged_out_is_removed() {
        let d = tempfile::tempdir().unwrap();
        for sub in ["logs", "projects/p-old", "projects/p-new", "envs/e-old", "envs/.lock-dir"] {
            std::fs::create_dir_all(d.path().join(sub)).unwrap();
        }
        let files = ["logs/executor-1.log", "logs/executor-2.log", "logs/other.log", "envs/e-old/marker"];
        for f in files {
            std::fs::write(d.path().join(f), b"x").unwrap();
        }
        aged(&d.path().join("logs/executor-1.log"), 3);
        aged(&d.path().join("logs/other.log"), 30);
        let set_dir = |p: &str, days: u64| {
            let t = SystemTime::now() - Duration::from_secs(days * 24 * 3600);
            std::fs::File::open(d.path().join(p)).unwrap().set_modified(t).unwrap();
        };
        set_dir("projects/p-old", 20);
        set_dir("envs/e-old", 20);
        set_dir("envs/.lock-dir", 20);
        assert_eq!(sweep(d.path(), SystemTime::now()), 3);
        assert!(!d.path().join("logs/executor-1.log").exists());
        assert!(d.path().join("logs/executor-2.log").exists(), "recent");
        assert!(d.path().join("logs/other.log").exists(), "not an executor log");
        assert!(!d.path().join("projects/p-old").exists() && d.path().join("projects/p-new").exists());
        assert!(!d.path().join("envs/e-old").exists() && d.path().join("envs/.lock-dir").exists());
    }
}
