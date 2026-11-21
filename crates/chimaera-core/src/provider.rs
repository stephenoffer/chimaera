//! Who are the seeds, and who am I: the pure half of each launch platform.
//!
//! A provider only starts the peer on every host; these functions turn the
//! platform's environment into that peer's `--seeds` and `--advertise`. The
//! seeds are the founders (`--bootstrap-expect` is their count): the first
//! min(5, n) hosts in the platform's order, so every host derives the same
//! list. A founder recognizes itself by its advertise address appearing in
//! the seeds, so both are built from the same host strings.

use std::collections::BTreeMap;

/// The peer port unless `CHIMAERA_PORT` says otherwise.
pub const DEFAULT_PORT: u16 = 7700;
const MAX_FOUNDERS: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    Local,
    Static,
    K8s,
    Slurm,
    Hpc,
    Sky,
}

/// `--provider auto`: the first platform whose marker variable is set, then
/// static if `--seeds` was given, else local.
pub fn detect(env: &BTreeMap<String, String>, seeds_given: bool) -> ProviderKind {
    let set = |key: &str| env.contains_key(key);
    if set("KUBERNETES_SERVICE_HOST") {
        ProviderKind::K8s
    } else if set("SLURM_JOB_ID") {
        ProviderKind::Slurm
    } else if set("PBS_NODEFILE") || set("LSB_MCPU_HOSTS") {
        ProviderKind::Hpc
    } else if set("SKYPILOT_NODE_IPS") {
        ProviderKind::Sky
    } else if seeds_given {
        ProviderKind::Static
    } else {
        ProviderKind::Local
    }
}

/// What a peer needs to join: the founders and its own address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Discovery {
    /// `host:port` of the founders.
    pub seeds: Vec<String>,
    /// `host:port` other peers dial to reach this one.
    pub advertise: String,
    pub port: u16,
    pub pool: String,
    pub labels: BTreeMap<String, String>,
}

/// The first of `keys` that is set; the error names them all.
fn var<'a>(env: &'a BTreeMap<String, String>, keys: &[&str]) -> Result<&'a str, String> {
    keys.iter().find_map(|k| env.get(*k)).map(String::as_str).ok_or_else(|| format!("{} is not set", keys.join(" or ")))
}

/// `CHIMAERA_PORT`, else `default`.
fn port(env: &BTreeMap<String, String>, default: u16) -> Result<u16, String> {
    env.get("CHIMAERA_PORT")
        .map_or(Ok(default), |p| p.parse().map_err(|_| format!("CHIMAERA_PORT {p:?} is not a port")))
}

/// The listed host that is `me`: an exact match, else the one entry whose
/// short name matches (a nodefile often lists `n1.cluster.local` while
/// `hostname` says `n1`, or the reverse). IP addresses only match exactly.
fn find_self<'a>(hosts: &'a [String], me: &str) -> Option<&'a String> {
    fn short(h: &str) -> &str {
        h.split('.').next().unwrap_or(h)
    }
    let is_ip = |h: &str| h.parse::<std::net::IpAddr>().is_ok();
    hosts.iter().find(|h| *h == me).or_else(|| {
        let mut same = hosts.iter().filter(|h| !is_ip(h) && !is_ip(me) && short(h) == short(me));
        match (same.next(), same.next()) {
            (Some(h), None) => Some(h),
            _ => None,
        }
    })
}

/// Founders are the first min(5, n) `hosts`. `me` must be one of them, or it
/// could never recognize itself as a founder; `source` names where they came from.
fn from_hosts(
    hosts: &[String],
    me: &str,
    source: &str,
    port: u16,
    pool: String,
    labels: BTreeMap<String, String>,
) -> Result<Discovery, String> {
    let Some(me) = find_self(hosts, me) else {
        return Err(format!("this host {me:?} is not listed in {source}"));
    };
    Ok(Discovery {
        seeds: hosts.iter().take(MAX_FOUNDERS).map(|h| format!("{h}:{port}")).collect(),
        advertise: format!("{me}:{port}"),
        port,
        pool,
        labels,
    })
}

/// Expands a Slurm host list: comma-separated hosts or patterns, where
/// `n[1-3,7]` is a range set (zero padding kept: `n[01-03]`) and several
/// bracket groups form a cartesian product (`r[1-2]n[1-2]`).
pub fn expand_hostlist(list: &str) -> Result<Vec<String>, String> {
    let unbalanced = || format!("unbalanced brackets in host list {list:?}");
    let (mut hosts, mut open, mut start) = (Vec::new(), false, 0);
    // A trailing ',' sentinel flushes the last pattern.
    for (i, c) in list.char_indices().chain([(list.len(), ',')]) {
        match c {
            '[' if !open => open = true,
            ']' if open => open = false,
            '[' | ']' => return Err(unbalanced()),
            ',' if !open => {
                let pattern = list[start..i].trim();
                if pattern.is_empty() {
                    return Err(format!("empty host in host list {list:?}"));
                }
                hosts.extend(expand_pattern(pattern)?);
                start = i + 1;
            }
            _ => {}
        }
    }
    if open { Err(unbalanced()) } else { Ok(hosts) }
}

/// One pattern with balanced, unnested bracket groups.
fn expand_pattern(pattern: &str) -> Result<Vec<String>, String> {
    let Some((head, rest)) = pattern.split_once('[') else { return Ok(vec![pattern.to_string()]) };
    let (ranges, tail) = rest.split_once(']').ok_or_else(|| format!("unbalanced brackets in {pattern:?}"))?;
    let tails = expand_pattern(tail)?;
    let mut hosts = Vec::new();
    for range in ranges.split(',') {
        let (lo, hi) = range.split_once('-').unwrap_or((range, range));
        let num = |s: &str| s.trim().parse::<u64>().map_err(|_| format!("bad range {range:?} in {pattern:?}"));
        let (first, last, width) = (num(lo)?, num(hi)?, lo.trim().len());
        if first > last {
            return Err(format!("reversed range {range:?} in {pattern:?}"));
        }
        for n in first..=last {
            hosts.extend(tails.iter().map(|t| format!("{head}{n:0width$}{t}")));
        }
    }
    Ok(hosts)
}

/// Slurm: hosts from `SLURM_JOB_NODELIST` (else `SLURM_NODELIST`), self from
/// `SLURMD_NODENAME` (else `hostname`). The port defaults to
/// 7700 + `SLURM_JOB_ID` % 1000, so jobs sharing a node don't collide and it
/// stays below Linux's ephemeral range.
pub fn slurm(env: &BTreeMap<String, String>, hostname: &str) -> Result<Discovery, String> {
    let job = var(env, &["SLURM_JOB_ID"])?;
    let job_number: u64 = job.parse().map_err(|_| format!("SLURM_JOB_ID {job:?} is not a number"))?;
    let hosts = expand_hostlist(var(env, &["SLURM_JOB_NODELIST", "SLURM_NODELIST"])?)?;
    let me = env.get("SLURMD_NODENAME").map_or(hostname, String::as_str);
    let port = port(env, DEFAULT_PORT + (job_number % 1000) as u16)?;
    let pool = env.get("SLURM_JOB_PARTITION").map_or("default", String::as_str).to_string();
    let mut labels = BTreeMap::from([("slurm.job".to_string(), job.to_string())]);
    if let Some(topology) = env.get("SLURM_TOPOLOGY_ADDR") {
        labels.insert("topology".to_string(), topology.clone());
    }
    from_hosts(&hosts, me, "SLURM_JOB_NODELIST", port, pool, labels)
}

/// Kubernetes StatefulSet: this pod's `HOSTNAME` is `<statefulset>-<ordinal>`
/// and pods `0..CHIMAERA_FOUNDERS` (default 3) are the founders, reached by
/// their per-pod DNS names under the headless service. These variables are
/// the contract the future `chimaera k8s render` manifest sets:
/// `CHIMAERA_K8S_SERVICE`, `CHIMAERA_K8S_NAMESPACE` (else `POD_NAMESPACE`),
/// `CHIMAERA_FOUNDERS`, `CHIMAERA_PORT` and `CHIMAERA_POOL` (else the
/// StatefulSet's name).
pub fn k8s(env: &BTreeMap<String, String>) -> Result<Discovery, String> {
    let host = var(env, &["HOSTNAME"])?;
    let statefulset = host
        .rsplit_once('-')
        .filter(|(sts, ordinal)| !sts.is_empty() && ordinal.parse::<u32>().is_ok())
        .map(|(sts, _)| sts)
        .ok_or_else(|| format!("HOSTNAME {host:?} is not a StatefulSet pod name (<statefulset>-<ordinal>)"))?;
    let service = var(env, &["CHIMAERA_K8S_SERVICE"])?;
    let namespace = var(env, &["CHIMAERA_K8S_NAMESPACE", "POD_NAMESPACE"])?;
    let founders: u32 = env.get("CHIMAERA_FOUNDERS").map_or(Ok(3), |f| {
        f.parse().ok().filter(|n| *n > 0).ok_or_else(|| format!("CHIMAERA_FOUNDERS {f:?} is not a positive number"))
    })?;
    let port = port(env, DEFAULT_PORT)?;
    let dns = |pod: &str| format!("{pod}.{service}.{namespace}.svc:{port}");
    Ok(Discovery {
        seeds: (0..founders).map(|i| dns(&format!("{statefulset}-{i}"))).collect(),
        advertise: dns(host),
        port,
        pool: env.get("CHIMAERA_POOL").map_or(statefulset, String::as_str).to_string(),
        labels: BTreeMap::new(),
    })
}

/// PBS or LSF: hosts from the PBS nodefile's contents (one line per CPU, so
/// repeats are dropped, keeping order), else from LSF's `LSB_MCPU_HOSTS`
/// (`host count` pairs). Self is `hostname`.
pub fn hpc(env: &BTreeMap<String, String>, pbs_nodefile: Option<&str>, hostname: &str) -> Result<Discovery, String> {
    let (listed, source): (Vec<&str>, &str) = match pbs_nodefile {
        Some(text) => (text.lines().map(str::trim).filter(|l| !l.is_empty()).collect(), "PBS_NODEFILE"),
        None => {
            let lsf = env.get("LSB_MCPU_HOSTS").ok_or("no PBS nodefile was read and LSB_MCPU_HOSTS is not set")?;
            (lsf.split_whitespace().step_by(2).collect(), "LSB_MCPU_HOSTS")
        }
    };
    let mut hosts: Vec<String> = Vec::new();
    for host in listed {
        if !hosts.iter().any(|h| h == host) {
            hosts.push(host.to_string());
        }
    }
    from_hosts(&hosts, hostname, source, port(env, DEFAULT_PORT)?, "default".to_string(), BTreeMap::new())
}

/// SkyPilot: `SKYPILOT_NODE_IPS` (one per line) in rank order; self is the
/// entry at `SKYPILOT_NODE_RANK`.
pub fn sky(env: &BTreeMap<String, String>) -> Result<Discovery, String> {
    let hosts: Vec<String> =
        var(env, &["SKYPILOT_NODE_IPS"])?.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from).collect();
    let rank = var(env, &["SKYPILOT_NODE_RANK"])?;
    let me = rank.parse::<usize>().ok().and_then(|r| hosts.get(r)).ok_or_else(|| {
        format!("SKYPILOT_NODE_RANK {rank:?} is not an index into SKYPILOT_NODE_IPS ({} hosts)", hosts.len())
    })?;
    from_hosts(&hosts, me, "SKYPILOT_NODE_IPS", port(env, DEFAULT_PORT)?, "default".to_string(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn hostlists_expand_like_slurm() {
        let cases: [(&str, &[&str]); 7] = [
            ("n[1-3,7]", &["n1", "n2", "n3", "n7"]),
            ("n[01-03]", &["n01", "n02", "n03"]),
            ("n[08-10]", &["n08", "n09", "n10"]),
            ("r[1-2]n[1-2]", &["r1n1", "r1n2", "r2n1", "r2n2"]),
            ("login,gpu[1-2]-ib,cpu", &["login", "gpu1-ib", "gpu2-ib", "cpu"]),
            ("node7", &["node7"]),
            ("a[9-11]", &["a9", "a10", "a11"]),
        ];
        for (list, hosts) in cases {
            assert_eq!(expand_hostlist(list).unwrap(), hosts, "{list}");
        }
        for bad in ["", " ", "n[1-3", "n1-3]", "n[[1]]", "n[3-1]", "n[]", "n[1-x]", "a,,b", "a,"] {
            assert!(expand_hostlist(bad).is_err(), "{bad:?} should fail");
        }
        assert!(expand_hostlist("n[3-1]").unwrap_err().contains("reversed"));
    }

    #[test]
    fn detect_checks_platforms_in_order() {
        let mut e = env(&[
            ("KUBERNETES_SERVICE_HOST", "10.0.0.1"),
            ("SLURM_JOB_ID", "1"),
            ("PBS_NODEFILE", "/var/spool/pbs/aux/1"),
            ("SKYPILOT_NODE_IPS", "10.0.0.2"),
        ]);
        let order = [
            ("KUBERNETES_SERVICE_HOST", ProviderKind::K8s),
            ("SLURM_JOB_ID", ProviderKind::Slurm),
            ("PBS_NODEFILE", ProviderKind::Hpc),
            ("SKYPILOT_NODE_IPS", ProviderKind::Sky),
        ];
        for (marker, kind) in order {
            assert_eq!(detect(&e, true), kind);
            e.remove(marker);
        }
        assert_eq!(detect(&env(&[("LSB_MCPU_HOSTS", "h1 4")]), false), ProviderKind::Hpc);
        assert_eq!(detect(&e, true), ProviderKind::Static);
        assert_eq!(detect(&e, false), ProviderKind::Local);
    }

    #[test]
    fn slurm_from_job_env() {
        let e = env(&[
            ("SLURM_JOB_ID", "123456"),
            ("SLURM_JOB_NODELIST", "gpu[01-07]"),
            ("SLURMD_NODENAME", "gpu03"),
            ("SLURM_JOB_PARTITION", "h100"),
            ("SLURM_TOPOLOGY_ADDR", "s1.gpu03"),
        ]);
        let d = slurm(&e, "ignored").unwrap();
        assert_eq!(d.seeds, ["gpu01:8156", "gpu02:8156", "gpu03:8156", "gpu04:8156", "gpu05:8156"]);
        assert_eq!((d.advertise.as_str(), d.port, d.pool.as_str()), ("gpu03:8156", 8156, "h100"));
        assert_eq!(d.labels, env(&[("slurm.job", "123456"), ("topology", "s1.gpu03")]));

        let e = env(&[("SLURM_JOB_ID", "42"), ("SLURM_NODELIST", "a,b"), ("CHIMAERA_PORT", "9000")]);
        let d = slurm(&e, "b").unwrap();
        assert_eq!(
            (d.seeds, d.advertise, d.pool),
            (vec!["a:9000".to_string(), "b:9000".into()], "b:9000".into(), "default".into())
        );

        assert!(slurm(&env(&[]), "a").unwrap_err().contains("SLURM_JOB_ID"));
        assert!(slurm(&env(&[("SLURM_JOB_ID", "1")]), "a").unwrap_err().contains("SLURM_JOB_NODELIST"));
        assert!(
            slurm(&env(&[("SLURM_JOB_ID", "1"), ("SLURM_NODELIST", "a,b")]), "c")
                .unwrap_err()
                .contains("\"c\" is not listed")
        );
    }

    #[test]
    fn k8s_from_statefulset_env() {
        let mut e = env(&[("HOSTNAME", "chimaera-4"), ("CHIMAERA_K8S_SERVICE", "peers"), ("POD_NAMESPACE", "ml")]);
        let d = k8s(&e).unwrap();
        assert_eq!(
            d.seeds,
            ["chimaera-0.peers.ml.svc:7700", "chimaera-1.peers.ml.svc:7700", "chimaera-2.peers.ml.svc:7700"]
        );
        assert_eq!((d.advertise.as_str(), d.port, d.pool.as_str()), ("chimaera-4.peers.ml.svc:7700", 7700, "chimaera"));

        e.extend(env(&[
            ("CHIMAERA_K8S_NAMESPACE", "prod"),
            ("CHIMAERA_FOUNDERS", "1"),
            ("CHIMAERA_PORT", "7800"),
            ("CHIMAERA_POOL", "h100"),
        ]));
        let d = k8s(&e).unwrap();
        assert_eq!(
            (d.seeds, d.advertise, d.pool),
            (
                vec!["chimaera-0.peers.prod.svc:7800".to_string()],
                "chimaera-4.peers.prod.svc:7800".into(),
                "h100".into()
            )
        );

        for (key, value, named) in [
            ("HOSTNAME", "chimaera", "HOSTNAME"),
            ("HOSTNAME", "chimaera-x", "HOSTNAME"),
            ("CHIMAERA_FOUNDERS", "0", "CHIMAERA_FOUNDERS"),
            ("CHIMAERA_PORT", "70000", "CHIMAERA_PORT"),
        ] {
            let mut bad = e.clone();
            bad.insert(key.to_string(), value.to_string());
            assert!(k8s(&bad).unwrap_err().contains(named), "{key}={value}");
        }
        assert!(k8s(&env(&[("HOSTNAME", "c-0")])).unwrap_err().contains("CHIMAERA_K8S_SERVICE"));
        let no_namespace = env(&[("HOSTNAME", "c-0"), ("CHIMAERA_K8S_SERVICE", "s")]);
        assert!(k8s(&no_namespace).unwrap_err().contains("CHIMAERA_K8S_NAMESPACE or POD_NAMESPACE"));
    }

    #[test]
    fn hpc_from_pbs_nodefile_or_lsf_hosts() {
        let d = hpc(&env(&[]), Some("n1\nn1\nn2\nn2\nn3\nn3\n"), "n2").unwrap();
        assert_eq!(
            (d.seeds, d.advertise),
            (vec!["n1:7700".to_string(), "n2:7700".into(), "n3:7700".into()], "n2:7700".into())
        );

        let lsf = env(&[("LSB_MCPU_HOSTS", "h1 4 h2 4 h3 2"), ("CHIMAERA_PORT", "7701")]);
        let d = hpc(&lsf, None, "h1").unwrap();
        assert_eq!(
            (d.seeds, d.advertise),
            (vec!["h1:7701".to_string(), "h2:7701".into(), "h3:7701".into()], "h1:7701".into())
        );

        assert!(hpc(&env(&[]), None, "h1").unwrap_err().contains("LSB_MCPU_HOSTS"));
        assert!(hpc(&lsf, None, "h9").unwrap_err().contains("LSB_MCPU_HOSTS"));

        // Short and full names match each other, using the listed form.
        assert_eq!(hpc(&lsf, None, "h2.cluster.local").unwrap().advertise, "h2:7701");
        let fqdn = hpc(&env(&[]), Some("n1.hpc.edu\nn2.hpc.edu\n"), "n2").unwrap();
        assert_eq!(fqdn.advertise, "n2.hpc.edu:7700");
        // Ambiguous short names and IP prefixes never match loosely.
        assert!(hpc(&env(&[]), Some("n1.a\nn1.b\n"), "n1").is_err());
        assert!(hpc(&env(&[]), Some("10.0.0.1\n10.0.0.2\n"), "10.0.0.3").is_err());
    }

    #[test]
    fn sky_from_node_ips_and_rank() {
        let ips: Vec<String> = (1..=7).map(|i| format!("10.0.0.{i}")).collect();
        let e = env(&[("SKYPILOT_NODE_IPS", &ips.join("\n")), ("SKYPILOT_NODE_RANK", "6")]);
        let d = sky(&e).unwrap();
        assert_eq!(d.seeds, ips[..5].iter().map(|ip| format!("{ip}:7700")).collect::<Vec<_>>());
        assert_eq!(d.advertise, "10.0.0.7:7700");

        let mut bad = e.clone();
        bad.insert("SKYPILOT_NODE_RANK".to_string(), "7".to_string());
        assert!(sky(&bad).unwrap_err().contains("SKYPILOT_NODE_RANK"));
        assert!(sky(&env(&[("SKYPILOT_NODE_IPS", "10.0.0.1")])).unwrap_err().contains("SKYPILOT_NODE_RANK"));
        assert!(sky(&env(&[])).unwrap_err().contains("SKYPILOT_NODE_IPS"));
    }
}
