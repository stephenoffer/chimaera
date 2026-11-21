//! Starting peers on a platform: which addresses `peer start --provider`
//! uses, and the launchers `k8s render`, `slurm-launch` and `up`/`down`.
//! Discovery itself is pure and lives in `chimaera_core::provider`.

pub mod k8s;
pub mod slurm;
pub mod ssh;

use chimaera_core::provider::{self, Discovery, ProviderKind};
use std::collections::BTreeMap;
use std::net::SocketAddr;

/// Values `--provider` accepts.
pub const PROVIDERS: [&str; 7] = ["auto", "local", "static", "k8s", "slurm", "hpc", "sky"];

/// The provider a `peer start` uses. Under `auto`, flags keep the meaning
/// they had before providers existed: `--listen` is a hand-configured
/// (static) peer and `--idle-timeout` a local one, as drivers start on
/// demand. Otherwise the platform is detected from the environment.
pub fn choose(
    name: &str,
    env: &BTreeMap<String, String>,
    listen_given: bool,
    seeds_given: bool,
    idle_timeout_given: bool,
) -> ProviderKind {
    match name {
        "local" => ProviderKind::Local,
        "static" => ProviderKind::Static,
        "k8s" => ProviderKind::K8s,
        "slurm" => ProviderKind::Slurm,
        "hpc" => ProviderKind::Hpc,
        "sky" => ProviderKind::Sky,
        _ if listen_given => ProviderKind::Static,
        _ if idle_timeout_given => ProviderKind::Local,
        _ => provider::detect(env, seeds_given),
    }
}

/// Discovery for a platform provider, read from this process's environment
/// (and the PBS nodefile, when `PBS_NODEFILE` names one).
pub fn discover(kind: ProviderKind, env: &BTreeMap<String, String>) -> Result<Discovery, String> {
    match kind {
        ProviderKind::K8s => provider::k8s(env),
        ProviderKind::Slurm => provider::slurm(env, &hostname()),
        ProviderKind::Sky => provider::sky(env),
        ProviderKind::Hpc => {
            let nodefile = env
                .get("PBS_NODEFILE")
                .map(|f| std::fs::read_to_string(f).map_err(|e| format!("cannot read PBS_NODEFILE {f}: {e}")))
                .transpose()?;
            provider::hpc(env, nodefile.as_deref(), &hostname())
        }
        ProviderKind::Local | ProviderKind::Static => Err("local and static peers are configured by flags".into()),
    }
}

/// A cluster peer's addresses: explicit flags win over discovered values.
#[derive(Debug, PartialEq, Eq)]
pub struct Addrs {
    pub listen: SocketAddr,
    pub advertise: String,
    pub seeds: Vec<String>,
    pub client_listen: Option<SocketAddr>,
}

/// The flags `peer start` was given that a provider can fill in.
pub struct Flags {
    pub listen: Option<SocketAddr>,
    pub advertise: Option<String>,
    pub seeds: Vec<String>,
    pub client_listen: Option<SocketAddr>,
    pub client_port: Option<u16>,
}

/// Addresses under a platform provider. Drivers on other machines are
/// always accepted (with the cluster token) on `--client-listen`, else on
/// `--client-port`, `CHIMAERA_CLIENT_PORT` or the peer port + 10.
pub fn platform_addrs(d: &Discovery, f: Flags, env: &BTreeMap<String, String>) -> Result<Addrs, String> {
    let client_port = match (f.client_port, env.get("CHIMAERA_CLIENT_PORT")) {
        (Some(p), _) => p,
        (None, Some(p)) => p.parse().map_err(|_| format!("CHIMAERA_CLIENT_PORT {p:?} is not a port"))?,
        (None, None) => d.port.checked_add(10).ok_or_else(|| format!("port {} + 10 is not a port", d.port))?,
    };
    let any = |port| SocketAddr::from(([0, 0, 0, 0], port));
    Ok(Addrs {
        listen: f.listen.unwrap_or(any(d.port)),
        advertise: f.advertise.unwrap_or_else(|| d.advertise.clone()),
        seeds: if f.seeds.is_empty() { d.seeds.clone() } else { f.seeds },
        client_listen: Some(f.client_listen.unwrap_or(any(client_port))),
    })
}

/// A new random cluster token (64 hex digits).
pub fn new_token() -> std::io::Result<String> {
    use std::io::Read;
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

/// This host's name, as `hostname` prints it.
pub fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most `buf.len()` bytes into `buf`.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
        return String::new();
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn auto_keeps_listen_static_and_idle_timeout_local_inside_platforms() {
        let pod = env(&[("KUBERNETES_SERVICE_HOST", "10.0.0.1")]);
        assert_eq!(choose("auto", &pod, false, false, false), ProviderKind::K8s);
        assert_eq!(choose("auto", &pod, true, true, false), ProviderKind::Static);
        assert_eq!(choose("auto", &pod, false, false, true), ProviderKind::Local, "a driver's on-demand peer");
        assert_eq!(choose("auto", &env(&[]), false, true, false), ProviderKind::Static);
        assert_eq!(choose("auto", &env(&[]), false, false, false), ProviderKind::Local);
        assert_eq!(choose("slurm", &pod, true, true, true), ProviderKind::Slurm, "an explicit provider wins");
    }

    #[test]
    fn flags_win_over_discovery_and_clients_listen_on_port_plus_ten() {
        let d =
            provider::k8s(&env(&[("HOSTNAME", "c-1"), ("CHIMAERA_K8S_SERVICE", "s"), ("POD_NAMESPACE", "n")])).unwrap();
        let none = || Flags { listen: None, advertise: None, seeds: vec![], client_listen: None, client_port: None };
        let a = platform_addrs(&d, none(), &env(&[])).unwrap();
        assert_eq!(a.listen, "0.0.0.0:7700".parse().unwrap());
        assert_eq!(a.advertise, "c-1.s.n.svc:7700");
        assert_eq!(a.seeds, d.seeds);
        assert_eq!(a.client_listen, Some("0.0.0.0:7710".parse().unwrap()));

        let a = platform_addrs(&d, none(), &env(&[("CHIMAERA_CLIENT_PORT", "9000")])).unwrap();
        assert_eq!(a.client_listen, Some("0.0.0.0:9000".parse().unwrap()));
        let f = Flags {
            listen: Some("127.0.0.1:1".parse().unwrap()),
            advertise: Some("me:1".into()),
            seeds: vec!["x:1".into()],
            client_listen: None,
            client_port: Some(2),
        };
        let a = platform_addrs(&d, f, &env(&[("CHIMAERA_CLIENT_PORT", "9000")])).unwrap();
        assert_eq!(
            a,
            Addrs {
                listen: "127.0.0.1:1".parse().unwrap(),
                advertise: "me:1".into(),
                seeds: vec!["x:1".into()],
                client_listen: Some("0.0.0.0:2".parse().unwrap()),
            }
        );
        assert!(platform_addrs(&d, none(), &env(&[("CHIMAERA_CLIENT_PORT", "x")])).unwrap_err().contains("not a port"));
    }
}
