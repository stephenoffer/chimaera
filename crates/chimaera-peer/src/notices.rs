//! Cloud preemption and maintenance notices. With
//! `CHIMAERA_PREEMPTION_NOTICES=gce|aws|azure` the peer polls its VM's
//! metadata server every few seconds and, once the VM is about to be
//! preempted, stopped or taken down for maintenance, drains as on SIGUSR1:
//! no new work, running work finishes or moves within `CHIMAERA_DRAIN_SECONDS`.
//!
//! What counts as a notice:
//! - GCE: `instance/preempted` is `TRUE`, or `instance/maintenance-event`
//!   starts with `TERMINATE` (a live migration does not drain).
//! - AWS: `spot/instance-action` exists (IMDSv2 token first).
//! - Azure: Scheduled Events lists a `Preempt`, `Terminate`, `Reboot` or `Redeploy`.
//!
//! Off by default: it talks to the link-local metadata address, which only a
//! cloud VM has. Kubernetes and Slurm send their own signals, which already drain.
//! Unverified against real clouds here; the endpoints and answers are from
//! each provider's documentation.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const ENV: &str = "CHIMAERA_PREEMPTION_NOTICES";
const METADATA: &str = "169.254.169.254:80";
const POLL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cloud {
    Gce,
    Aws,
    Azure,
}

pub fn parse(value: &str) -> io::Result<Cloud> {
    match value.trim().to_ascii_lowercase().as_str() {
        "gce" | "gcp" => Ok(Cloud::Gce),
        "aws" | "ec2" => Ok(Cloud::Aws),
        "azure" => Ok(Cloud::Azure),
        other => {
            Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{ENV}={other:?}: expected gce, aws or azure")))
        }
    }
}

/// Start watching if `CHIMAERA_PREEMPTION_NOTICES` names a cloud; a bad value
/// fails the peer's start. A notice raises SIGUSR1 on this process, the
/// signal the peer drains on. The handler is registered here first, so an
/// early notice never meets SIGUSR1's default action (exit), and the signal
/// repeats until the peer stops, so one sent before the drain loop listens is
/// not lost.
pub fn spawn_from_env() -> io::Result<()> {
    let Ok(value) = std::env::var(ENV) else { return Ok(()) };
    spawn(parse(&value)?, METADATA)
}

fn spawn(cloud: Cloud, addr: &'static str) -> io::Result<()> {
    drop(tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?);
    tokio::spawn(async move {
        let reason = watch(cloud, addr).await;
        tracing::warn!("{reason}: draining");
        loop {
            // SAFETY: kill(2) on our own pid with a signal whose handler is installed above.
            unsafe { libc::kill(libc::getpid(), libc::SIGUSR1) };
            tokio::time::sleep(POLL).await;
        }
    });
    Ok(())
}

/// Poll until the cloud says this VM is going away; the notice, in words.
pub async fn watch(cloud: Cloud, addr: &str) -> String {
    loop {
        match check(cloud, addr).await {
            Ok(Some(reason)) => return reason,
            Ok(None) => {}
            Err(e) => tracing::debug!("{cloud:?} metadata: {e}"),
        }
        tokio::time::sleep(POLL).await;
    }
}

/// One look at the metadata server: `Some(notice)` if the VM is going away.
pub async fn check(cloud: Cloud, addr: &str) -> io::Result<Option<String>> {
    match cloud {
        Cloud::Gce => {
            let google = [("Metadata-Flavor", "Google")];
            let (_, preempted) = get(addr, "GET", "/computeMetadata/v1/instance/preempted", &google).await?;
            if preempted.trim() == "TRUE" {
                return Ok(Some("GCE: this VM is being preempted".into()));
            }
            let (_, event) = get(addr, "GET", "/computeMetadata/v1/instance/maintenance-event", &google).await?;
            Ok(event.trim().starts_with("TERMINATE").then(|| format!("GCE maintenance: {}", event.trim())))
        }
        Cloud::Aws => {
            let ttl = [("X-aws-ec2-metadata-token-ttl-seconds", "300")];
            let (status, token) = get(addr, "PUT", "/latest/api/token", &ttl).await?;
            if status != 200 {
                return Err(io::Error::other(format!("IMDSv2 token: HTTP {status}")));
            }
            let auth = [("X-aws-ec2-metadata-token", token.trim())];
            let (status, action) = get(addr, "GET", "/latest/meta-data/spot/instance-action", &auth).await?;
            Ok((status == 200).then(|| format!("AWS spot interruption: {}", action.trim())))
        }
        Cloud::Azure => {
            let path = "/metadata/scheduledevents?api-version=2020-07-01";
            let (_, body) = get(addr, "GET", path, &[("Metadata", "true")]).await?;
            let doc: serde_json::Value = serde_json::from_str(&body).map_err(io::Error::other)?;
            let events = doc["Events"].as_array().cloned().unwrap_or_default();
            let ending = ["Preempt", "Terminate", "Reboot", "Redeploy"];
            Ok(events
                .iter()
                .find(|e| e["EventType"].as_str().is_some_and(|t| ending.contains(&t)))
                .map(|e| format!("Azure scheduled event: {} at {}", e["EventType"], e["NotBefore"])))
        }
    }
}

/// A plain HTTP/1.0 request (the server closes the connection, so the body is
/// everything after the headers); the status and the body.
async fn get(addr: &str, method: &str, path: &str, headers: &[(&str, &str)]) -> io::Result<(u16, String)> {
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await?;
        let mut req = format!("{method} {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Length: 0\r\n");
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        stream.write_all(format!("{req}\r\n").as_bytes()).await?;
        let mut raw = Vec::new();
        stream.take(1 << 20).read_to_end(&mut raw).await?;
        Ok::<_, io::Error>(String::from_utf8_lossy(&raw).into_owned())
    };
    let text = tokio::time::timeout(Duration::from_secs(3), exchange).await.map_err(io::Error::other)??;
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other("no HTTP status"))?;
    Ok((status, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    /// A metadata server answering `routes[path] = (status, body)`; it records each request's head.
    async fn server(routes: HashMap<&'static str, (u16, &'static str)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 4096];
                let n = s.read(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                let path = head.split_whitespace().nth(1).unwrap_or_default().to_string();
                log.lock().unwrap().push(head);
                let (status, body) = routes.get(path.as_str()).copied().unwrap_or((404, "not found"));
                let reply = format!("HTTP/1.0 {status} X\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                s.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        (addr, seen)
    }

    #[tokio::test]
    async fn gce_drains_on_preemption_and_terminating_maintenance_only() {
        let preempted = "/computeMetadata/v1/instance/preempted";
        let event = "/computeMetadata/v1/instance/maintenance-event";
        let (calm, seen) =
            server(HashMap::from([(preempted, (200, "FALSE")), (event, (200, "MIGRATE_ON_HOST_MAINTENANCE"))])).await;
        assert_eq!(check(Cloud::Gce, &calm).await.unwrap(), None, "a live migration does not drain");
        assert!(seen.lock().unwrap().iter().all(|h| h.contains("Metadata-Flavor: Google")));
        let (going, _) = server(HashMap::from([(preempted, (200, "TRUE\n"))])).await;
        assert_eq!(check(Cloud::Gce, &going).await.unwrap().as_deref(), Some("GCE: this VM is being preempted"));
        let (maint, _) =
            server(HashMap::from([(preempted, (200, "FALSE")), (event, (200, "TERMINATE_ON_HOST_MAINTENANCE"))])).await;
        assert!(check(Cloud::Gce, &maint).await.unwrap().unwrap().contains("TERMINATE_ON_HOST_MAINTENANCE"));
    }

    #[tokio::test]
    async fn aws_asks_for_a_token_then_drains_on_a_spot_action() {
        let token = "/latest/api/token";
        let action = "/latest/meta-data/spot/instance-action";
        let (calm, seen) = server(HashMap::from([(token, (200, "tok-1"))])).await;
        assert_eq!(check(Cloud::Aws, &calm).await.unwrap(), None, "404: no interruption scheduled");
        let heads = seen.lock().unwrap().clone();
        assert!(
            heads[0].starts_with("PUT /latest/api/token") && heads[0].contains("X-aws-ec2-metadata-token-ttl-seconds")
        );
        assert!(heads[1].contains("X-aws-ec2-metadata-token: tok-1"));
        let (going, _) = server(HashMap::from([
            (token, (200, "t")),
            (action, (200, r#"{"action":"terminate","time":"2026-11-21T12:00:00Z"}"#)),
        ]))
        .await;
        assert!(check(Cloud::Aws, &going).await.unwrap().unwrap().contains("\"action\":\"terminate\""));
        let (no_token, _) = server(HashMap::new()).await;
        assert!(check(Cloud::Aws, &no_token).await.is_err());
    }

    #[tokio::test]
    async fn azure_drains_on_events_that_take_the_vm_down() {
        let path = "/metadata/scheduledevents?api-version=2020-07-01";
        let (calm, seen) =
            server(HashMap::from([(path, (200, r#"{"DocumentIncarnation":1,"Events":[{"EventType":"Freeze"}]}"#))]))
                .await;
        assert_eq!(check(Cloud::Azure, &calm).await.unwrap(), None, "a freeze is a pause, not an end");
        assert!(seen.lock().unwrap()[0].contains("Metadata: true"));
        let body = r#"{"Events":[{"EventType":"Preempt","NotBefore":"Sat, 21 Nov 2026 12:00:00 GMT"}]}"#;
        let (going, _) = server(HashMap::from([(path, (200, body))])).await;
        assert!(check(Cloud::Azure, &going).await.unwrap().unwrap().starts_with("Azure scheduled event: \"Preempt\""));
    }

    #[tokio::test]
    async fn a_notice_raises_the_drain_signal() {
        let mut usr1 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1()).unwrap();
        let (addr, _) = server(HashMap::from([("/computeMetadata/v1/instance/preempted", (200, "TRUE"))])).await;
        spawn(Cloud::Gce, Box::leak(addr.into_boxed_str())).unwrap();
        tokio::time::timeout(Duration::from_secs(10), usr1.recv()).await.expect("SIGUSR1 within the first poll");
    }

    #[test]
    fn the_setting_names_a_cloud() {
        assert_eq!(parse(" GCP ").unwrap(), Cloud::Gce);
        assert_eq!(parse("ec2").unwrap(), Cloud::Aws);
        assert!(parse("oracle").unwrap_err().to_string().contains("expected gce, aws or azure"));
    }

    #[tokio::test]
    async fn an_unreachable_metadata_server_is_an_error_not_a_notice() {
        let closed = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().to_string();
        assert!(check(Cloud::Gce, &closed).await.is_err());
    }
}
