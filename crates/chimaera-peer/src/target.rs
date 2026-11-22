//! Which cluster a CLI command asks, chosen as a driver chooses
//! (python/chimaera/_cluster.py): `--cluster`, else `$CHIMAERA_CLUSTER`, else
//! `$CHIMAERA_ADDRESS` (with `$CHIMAERA_TOKEN` or `$CHIMAERA_TOKEN_FILE`, and
//! `$CHIMAERA_CA`), else the profile file's `default`, else the local peer.

use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, PartialEq)]
pub struct Target {
    pub name: String,
    /// `tcp://host:port`, tried in order.
    pub addresses: Vec<String>,
    pub token: Option<String>,
    pub ca: Option<String>,
}

fn tcp(address: &str) -> String {
    let a = address.trim();
    format!("tcp://{}", a.strip_prefix("chimaera://").or_else(|| a.strip_prefix("tcp://")).unwrap_or(a))
}

fn read(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path).map(|s| s.trim().to_string()).map_err(|e| format!("{path}: {e}"))
}

/// The remote cluster to ask, or None for the local peer. `env` is the
/// process environment, `profiles` the profile file's (path, text) when it
/// exists, and `home` the user's home directory.
pub fn resolve(
    cluster: Option<&str>,
    env: &BTreeMap<String, String>,
    profiles: Option<(&str, &str)>,
    home: &Path,
) -> Result<Option<Target>, String> {
    let get = |k: &str| env.get(k).filter(|v| !v.is_empty()).cloned();
    let env_token = || match get("CHIMAERA_TOKEN") {
        Some(t) => Ok(Some(t)),
        None => get("CHIMAERA_TOKEN_FILE").map(|p| read(&p)).transpose(),
    };
    let cluster = cluster.map(String::from).or_else(|| get("CHIMAERA_CLUSTER"));
    if cluster.is_none()
        && let Some(list) = get("CHIMAERA_ADDRESS")
    {
        let addresses = list.split(',').filter(|a| !a.trim().is_empty()).map(tcp).collect();
        return Ok(Some(Target {
            name: "$CHIMAERA_ADDRESS".into(),
            addresses,
            token: env_token()?,
            ca: get("CHIMAERA_CA"),
        }));
    }
    let doc = profiles
        .map(|(path, text)| toml_edit::Document::parse(text).map_err(|e| format!("{path}: {e}")))
        .transpose()?;
    let default = doc.as_ref().and_then(|d| d.get("default")).and_then(|v| v.as_str()).map(String::from);
    let Some(name) = cluster.or(default).filter(|n| n != "local") else { return Ok(None) };
    if name.starts_with("tcp://") || name.starts_with("chimaera://") {
        return Ok(Some(Target { addresses: vec![tcp(&name)], name, token: env_token()?, ca: get("CHIMAERA_CA") }));
    }
    let path = profiles.map_or("~/.config/chimaera/clusters.toml", |p| p.0);
    let known = doc.as_ref().and_then(|d| d.get("clusters")).and_then(|c| c.as_table_like());
    let Some(spec) = known.and_then(|k| k.get(&name)) else {
        let names: Vec<&str> = known.map(|k| k.iter().map(|(n, _)| n).collect()).unwrap_or_default();
        let names = if names.is_empty() { "none".to_string() } else { names.join(", ") };
        return Err(format!("no cluster {name:?} in {path} (profiles: {names})"));
    };
    let addresses: Vec<String> = spec
        .get("peers")
        .and_then(|p| p.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).map(tcp).collect())
        .unwrap_or_default();
    if addresses.is_empty() {
        return Err(format!(
            "cluster {name:?} lists no peers: add peers = [\"host:port\", ...] (their --client-listen)"
        ));
    }
    let creds = home.join(".chimaera/clusters").join(&name);
    let file = |key: &str, default: &str| {
        spec.get(key).and_then(|v| v.as_str()).map_or_else(|| creds.join(default).display().to_string(), String::from)
    };
    let token_file = file("token_file", "token");
    let token = match env_token()? {
        Some(t) => Some(t),
        None if Path::new(&token_file).exists() => Some(read(&token_file)?),
        None => None,
    };
    let ca = Some(file("ca", "ca.pem")).filter(|ca| Path::new(ca).exists());
    Ok(Some(Target { name, addresses, token, ca }))
}

/// A client of the first of `t`'s peers that answers.
pub async fn connect(t: &Target) -> Result<chimaera_proto::client::PeerClient, String> {
    let mut last = String::new();
    for a in &t.addresses {
        match chimaera_proto::client::connect(a, t.token.as_deref(), t.ca.as_deref()).await {
            Ok(c) => return Ok(c),
            Err(e) => last = format!("{a}: {e}"),
        }
    }
    Err(format!("no peer of cluster {} answered ({last})", t.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILES: &str = r#"
default = "lab"

[clusters.lab]
peers = ["10.0.0.1:7710", "chimaera://10.0.0.2:7710"]

[clusters.empty]
peers = []
"#;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn a_cluster_is_chosen_as_a_driver_chooses_it() {
        let home = std::env::temp_dir().join(format!("chimaera-target-{}", std::process::id()));
        let creds = home.join(".chimaera/clusters/lab");
        std::fs::create_dir_all(&creds).unwrap();
        std::fs::write(creds.join("token"), "secret\n").unwrap();
        let files = Some(("clusters.toml", PROFILES));
        let pick = |cluster, e: &[(&str, &str)], f| resolve(cluster, &env(e), f, &home);

        let lab = pick(None, &[], files).unwrap().unwrap();
        assert_eq!(lab.name, "lab", "the profile file's default");
        assert_eq!(lab.addresses, ["tcp://10.0.0.1:7710", "tcp://10.0.0.2:7710"]);
        assert_eq!((lab.token.as_deref(), lab.ca), (Some("secret"), None), "the token beside the profile; no ca.pem");

        let addr = pick(None, &[("CHIMAERA_ADDRESS", "a:1, b:2"), ("CHIMAERA_TOKEN", "t")], files).unwrap().unwrap();
        assert_eq!((addr.addresses, addr.token.as_deref()), (vec!["tcp://a:1".into(), "tcp://b:2".into()], Some("t")));

        let flag = pick(Some("tcp://h:7710"), &[("CHIMAERA_ADDRESS", "a:1")], files).unwrap().unwrap();
        assert_eq!(flag.addresses, ["tcp://h:7710"], "--cluster wins over the environment");
        assert_eq!(pick(Some("local"), &[], files).unwrap(), None);
        assert_eq!(pick(None, &[], None).unwrap(), None, "no profile file: the local peer");

        let err = pick(Some("nope"), &[], files).unwrap_err();
        assert!(err.contains("no cluster \"nope\"") && err.contains("profiles: lab, empty"), "{err}");
        assert!(pick(Some("empty"), &[], files).unwrap_err().contains("lists no peers"));
        std::fs::remove_dir_all(&home).unwrap();
    }
}
