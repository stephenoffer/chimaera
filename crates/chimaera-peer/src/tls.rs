//! Mutual TLS between cluster peers.
//!
//! `chimaera cluster init --out DIR` makes a cluster CA (`ca.pem`, `ca.key`)
//! and a token. `chimaera cluster cert --ca DIR --name NAME --out PEERDIR`
//! issues a peer certificate signed by that CA. A peer started with
//! `--tls-dir PEERDIR` serves and dials the mesh over TLS, presents its own
//! certificate and accepts only peers whose certificates the same CA signed.
//! Every peer certificate names `chimaera-peer`, which is the name peers
//! verify, so addresses can change without reissuing certificates.

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::io;
use std::path::Path;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// The name every peer certificate carries and every peer checks.
pub const PEER_NAME: &str = "chimaera-peer";

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// The CA's parameters. They are fixed, so the issuer can be rebuilt from
/// the CA key alone when issuing later.
fn ca_params() -> io::Result<CertificateParams> {
    let mut p = CertificateParams::new(Vec::<String>::new()).map_err(err)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "chimaera cluster CA");
    p.distinguished_name = dn;
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign, KeyUsagePurpose::DigitalSignature];
    Ok(p)
}

fn write_private(path: &Path, data: &str) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
    io::Write::write_all(&mut f, data.as_bytes())
}

/// Create `out/ca.pem`, `out/ca.key` and `out/token`. Refuses to overwrite.
pub fn init(out: &Path, token: &str) -> io::Result<()> {
    if !init_ca(out)? {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("{} already holds a CA", out.display())));
    }
    write_private(&out.join("token"), &format!("{token}\n"))
}

/// Create a cluster CA (`out/ca.pem`, `out/ca.key`) unless `out` holds one.
/// Whether it was created now.
pub fn init_ca(out: &Path) -> io::Result<bool> {
    if out.join("ca.pem").exists() {
        return Ok(false);
    }
    let (pem, key) = new_ca()?;
    write_ca(out, &pem, &key)?;
    Ok(true)
}

/// A new cluster CA: (certificate PEM, private key PEM).
pub fn new_ca() -> io::Result<(String, String)> {
    let key = KeyPair::generate().map_err(err)?;
    let cert = ca_params()?.self_signed(&key).map_err(err)?;
    Ok((cert.pem(), key.serialize_pem()))
}

/// Write a CA into `out` (replacing one there), as `--tls-ca` reads it.
pub fn write_ca(out: &Path, pem: &str, key: &str) -> io::Result<()> {
    std::fs::create_dir_all(out)?;
    let _ = std::fs::remove_file(out.join("ca.key"));
    write_private(&out.join("ca.key"), key)?;
    std::fs::write(out.join("ca.pem"), pem)
}

/// The variables a launcher passes a CA in to peers on other nodes, where
/// no shared directory holds it (Slurm: node-local data, srun's environment).
pub const CA_PEM_ENV: &str = "CHIMAERA_TLS_CA_PEM";
pub const CA_KEY_ENV: &str = "CHIMAERA_TLS_CA_KEY";

/// Issue a peer certificate from the CA in `ca_dir` into `out` (`ca.pem`,
/// `peer.pem`, `peer.key`): the directory a peer's `--tls-dir` points at.
pub fn issue(ca_dir: &Path, name: &str, out: &Path) -> io::Result<()> {
    let ca_key = KeyPair::from_pem(&std::fs::read_to_string(ca_dir.join("ca.key"))?).map_err(err)?;
    let issuer = Issuer::new(ca_params()?, ca_key);
    let mut p = CertificateParams::new(vec![PEER_NAME.to_string(), name.to_string()]).map_err(err)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    p.distinguished_name = dn;
    p.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().map_err(err)?;
    let cert = p.signed_by(&key, &issuer).map_err(err)?;
    std::fs::create_dir_all(out)?;
    std::fs::copy(ca_dir.join("ca.pem"), out.join("ca.pem"))?;
    std::fs::write(out.join("peer.pem"), cert.pem())?;
    write_private(&out.join("peer.key"), &key.serialize_pem())
}

/// Server and client TLS settings loaded from a `--tls-dir`.
#[derive(Clone)]
pub struct Tls {
    /// Mesh server: peers must present a certificate from the cluster CA.
    pub server: ServerTlsConfig,
    pub client: ClientTlsConfig,
    /// Client listener (`--client-listen`): drivers verify this peer against
    /// the cluster CA (`ca.pem`) and authenticate with the cluster token, not
    /// with certificates of their own.
    pub clients: ServerTlsConfig,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Tls { .. }") // keys stay out of logs
    }
}

pub fn load(dir: &Path) -> io::Result<Tls> {
    let read = |f: &str| {
        std::fs::read(dir.join(f)).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", dir.join(f).display())))
    };
    let ca = Certificate::from_pem(read("ca.pem")?);
    let identity = Identity::from_pem(read("peer.pem")?, read("peer.key")?);
    let server = ServerTlsConfig::new().identity(identity.clone()).client_ca_root(ca.clone());
    let clients = ServerTlsConfig::new().identity(identity.clone());
    let client = ClientTlsConfig::new().ca_certificate(ca).identity(identity).domain_name(PEER_NAME);
    Ok(Tls { server, client, clients })
}
