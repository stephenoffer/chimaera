//! A Prometheus endpoint: `GET /metrics` renders the peer's status (the same
//! snapshot `chimaera status` prints) in the text exposition format.
//!
//! Off unless `CHIMAERA_METRICS_LISTEN` names an address, e.g. `0.0.0.0:9700`.
//! It serves counts and sizes only (no run names, arguments or values), so it
//! takes no token; bind it where your scraper can reach it and others can't.

use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use chimaera_core::pb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::core::CoreMsg;

pub const ENV: &str = "CHIMAERA_METRICS_LISTEN";

/// Start the endpoint if `CHIMAERA_METRICS_LISTEN` is set; a bad address or a
/// port in use fails the peer's start, not the first scrape.
pub fn spawn_from_env(core: mpsc::Sender<CoreMsg>) -> io::Result<()> {
    let Ok(value) = std::env::var(ENV) else { return Ok(()) };
    let addr: SocketAddr =
        value.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{ENV}={value}: {e}")))?;
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    tracing::info!(%addr, "serving Prometheus metrics at /metrics");
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((conn, _)) => {
                    let core = core.clone();
                    tokio::spawn(async move {
                        if let Err(e) = answer(conn, &core).await {
                            tracing::debug!("metrics request: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("metrics listener: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(())
}

async fn answer(mut conn: TcpStream, core: &mpsc::Sender<CoreMsg>) -> io::Result<()> {
    let mut buf = [0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), conn.read(&mut buf))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no request line"))??;
    let request = String::from_utf8_lossy(&buf[..n]);
    let mut words = request.split_whitespace();
    let (method, path) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    let (status, body) = if method != "GET" || path.split('?').next() != Some("/metrics") {
        ("404 Not Found", "only GET /metrics is served\n".to_string())
    } else {
        let (reply, rx) = oneshot::channel();
        let status = match core.send(CoreMsg::Status { reply }).await {
            Ok(()) => tokio::time::timeout(Duration::from_secs(10), rx).await.ok().and_then(Result::ok),
            Err(_) => None,
        };
        match status {
            Some(s) => ("200 OK", render(&s)),
            None => ("503 Service Unavailable", "the peer did not answer\n".to_string()),
        }
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    conn.write_all(head.as_bytes()).await?;
    conn.write_all(body.as_bytes()).await?;
    conn.shutdown().await
}

/// The status as Prometheus gauges (every value is a current level).
pub fn render(s: &pb::StatusReply) -> String {
    let mut out = String::new();
    let mut gauge = |name: &str, help: &str, value: f64| {
        let _ = writeln!(out, "# HELP chimaera_{name} {help}\n# TYPE chimaera_{name} gauge\nchimaera_{name} {value}");
    };
    gauge("tasks_queued", "Calls waiting for a slot on this peer.", s.queued as f64);
    gauge("tasks_running", "Calls running on this peer.", s.running as f64);
    gauge("cpu_in_use", "CPUs granted to running calls.", s.cpu_in_use);
    gauge("executors", "Worker processes.", f64::from(s.executors));
    gauge("executors_busy", "Worker processes running a call.", f64::from(s.busy_executors));
    gauge("object_bytes", "Bytes of results and put values stored on this peer.", s.object_bytes as f64);
    gauge("memory_pool_bytes", "Memory this peer may grant to calls and objects.", s.memory_pool as f64);
    gauge("memory_used_bytes", "Memory granted to calls plus objects in memory.", s.memory_used as f64);
    gauge("memory_available_bytes", "Memory still grantable.", s.memory_available as f64);
    gauge("sessions", "Connected drivers.", f64::from(s.sessions));
    gauge("proposals_in_flight", "Shard commands proposed and not yet answered.", s.proposals_in_flight as f64);
    gauge("dispatch_p50_microseconds", "Median grant-to-handoff latency of recent calls.", s.dispatch_p50_us);
    gauge("dispatch_p99_microseconds", "99th percentile grant-to-handoff latency of recent calls.", s.dispatch_p99_us);
    if s.data_groups > 0 {
        let led: std::collections::BTreeSet<u32> =
            s.nodes.iter().filter(|n| n.alive).flat_map(|n| n.leading.iter().copied()).collect();
        gauge(
            "groups_without_leader",
            "Groups (the directory and the data groups) no live peer reports leading: their work waits.",
            (0..=s.data_groups).filter(|g| !led.contains(g)).count() as f64,
        );
    }
    for c in crate::counters::ALL {
        let _ = writeln!(
            out,
            "# HELP chimaera_{0} {1}\n# TYPE chimaera_{0} counter\nchimaera_{0} {2}",
            c.name,
            c.help,
            c.get()
        );
    }
    if s.data_groups > 0 {
        let _ = writeln!(
            out,
            "# HELP chimaera_group_leading Whether this peer leads the group (one series per group it holds).\n\
             # TYPE chimaera_group_leading gauge"
        );
        for g in &s.groups {
            let _ = writeln!(out, "chimaera_group_leading{{group=\"{}\"}} {}", g.group, u8::from(g.leading));
        }
        let _ = writeln!(
            out,
            "# HELP chimaera_group_max_lag_entries On a group's leader: log entries its furthest-behind voter lacks.\n\
             # TYPE chimaera_group_max_lag_entries gauge"
        );
        for g in s.groups.iter().filter(|g| g.leading) {
            let _ = writeln!(out, "chimaera_group_max_lag_entries{{group=\"{}\"}} {}", g.group, g.max_lag);
        }
    }
    let alive = s.nodes.iter().filter(|n| n.alive).count();
    let _ = writeln!(
        out,
        "# HELP chimaera_cluster_nodes Peers this one knows, by liveness.\n# TYPE chimaera_cluster_nodes gauge\n\
         chimaera_cluster_nodes{{state=\"alive\"}} {alive}\nchimaera_cluster_nodes{{state=\"dead\"}} {}",
        s.nodes.len() - alive
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_parseable_gauges() {
        let s = pb::StatusReply {
            queued: 7,
            running: 3,
            cpu_in_use: 2.5,
            nodes: vec![
                pb::ClusterNode { alive: true, leading: vec![1], ..Default::default() },
                pb::ClusterNode { alive: false, leading: vec![2], ..Default::default() },
                pb::ClusterNode { alive: true, ..Default::default() },
            ],
            data_groups: 3,
            groups: vec![
                pb::GroupStatus { group: 1, leading: true, max_lag: 4, ..Default::default() },
                pb::GroupStatus { group: 2, ..Default::default() },
            ],
            ..Default::default()
        };
        let text = render(&s);
        let samples: Vec<&str> = text.lines().filter(|l| !l.starts_with('#')).collect();
        for line in &samples {
            let (name, value) = line.rsplit_once(' ').expect("name value");
            assert!(name.starts_with("chimaera_"), "{line}");
            value.parse::<f64>().unwrap_or_else(|_| panic!("not a number: {line}"));
        }
        assert!(samples.contains(&"chimaera_tasks_queued 7"));
        assert!(samples.contains(&"chimaera_cpu_in_use 2.5"));
        assert!(samples.contains(&"chimaera_cluster_nodes{state=\"alive\"} 2"));
        assert!(samples.contains(&"chimaera_cluster_nodes{state=\"dead\"} 1"));
        assert!(samples.contains(&"chimaera_groups_without_leader 3"), "group 2's leader is dead; nobody leads 0 or 3");
        assert!(samples.contains(&"chimaera_group_leading{group=\"1\"} 1"));
        assert!(samples.contains(&"chimaera_group_leading{group=\"2\"} 0"));
        assert!(samples.contains(&"chimaera_group_max_lag_entries{group=\"1\"} 4"));
        assert!(!text.contains("chimaera_group_max_lag_entries{group=\"2\"}"), "only a leader knows the lag");
        assert!(text.contains("# TYPE chimaera_proposals_total counter"));
        // Every sample has HELP and TYPE lines before it.
        for name in samples.iter().map(|l| l.split(['{', ' ']).next().unwrap()) {
            assert!(text.contains(&format!("# HELP {name} ")) && text.contains(&format!("# TYPE {name} ")), "{name}");
        }
    }
}
