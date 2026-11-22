//! Who a client on another machine is. The cluster token is an
//! administrator; a driver credential (`chimaera cluster credential --tenant
//! T`) is tenant T, and reaches only T's runs, actors, store and logs. Clients
//! on the local socket are administrators: the socket is private to the user
//! that owns the peer.
//!
//! A credential is `T.<hex(keyed_blake3(derive_key(CONTEXT, token), T))>`, so
//! every peer holding the token checks it without a table, and changing the
//! token revokes every credential issued from it.

const CONTEXT: &str = "chimaera driver credential v1";

/// The caller of a client RPC. `CheckToken::clients` puts it in the request's
/// extensions; a request without one came over the local socket.
#[derive(Clone, Debug, PartialEq)]
pub enum Caller {
    Admin,
    Tenant(String),
}

impl Caller {
    pub fn of<T>(req: &tonic::Request<T>) -> Caller {
        req.extensions().get::<Caller>().cloned().unwrap_or(Caller::Admin)
    }

    /// The tenant this caller is bound to; None for an administrator.
    pub fn tenant(&self) -> Option<&str> {
        match self {
            Caller::Admin => None,
            Caller::Tenant(t) => Some(t),
        }
    }

    /// Whether this caller may act on what `owner` owns.
    pub fn may(&self, owner: &str) -> bool {
        self.tenant().is_none_or(|t| t == owner)
    }
}

/// A tenant name: 1-64 letters, digits, '-' or '_'.
pub fn valid_tenant(t: &str) -> bool {
    !t.is_empty() && t.len() <= 64 && t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn key(token: &str) -> [u8; 32] {
    blake3::derive_key(CONTEXT, token.as_bytes())
}

/// The credential of `tenant` under the cluster `token`.
pub fn credential(token: &str, tenant: &str) -> String {
    format!("{tenant}.{}", blake3::keyed_hash(&key(token), tenant.as_bytes()).to_hex())
}

/// Checks bearer values against one cluster token.
#[derive(Clone)]
pub struct Verifier {
    token: blake3::Hash,
    key: [u8; 32],
}

impl Verifier {
    pub fn new(token: &str) -> Verifier {
        Verifier { token: blake3::hash(token.as_bytes()), key: key(token) }
    }

    /// Who presented `bearer`, or None when it is neither the token nor a
    /// credential issued from it. Comparisons are of blake3 hashes, whose
    /// equality runs in constant time.
    pub fn caller(&self, bearer: &str) -> Option<Caller> {
        if blake3::hash(bearer.as_bytes()) == self.token {
            return Some(Caller::Admin);
        }
        let (tenant, mac) = bearer.rsplit_once('.')?;
        let mac = blake3::Hash::from_hex(mac).ok()?;
        (valid_tenant(tenant) && blake3::keyed_hash(&self.key, tenant.as_bytes()) == mac)
            .then(|| Caller::Tenant(tenant.to_string()))
    }

    /// Whether `bearer` is the cluster token itself (peers present only that).
    pub fn is_token(&self, bearer: &str) -> bool {
        blake3::hash(bearer.as_bytes()) == self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credential_names_its_tenant_and_nothing_else() {
        let v = Verifier::new("cluster-secret");
        assert_eq!(v.caller("cluster-secret"), Some(Caller::Admin));
        let alice = credential("cluster-secret", "alice");
        assert_eq!(v.caller(&alice), Some(Caller::Tenant("alice".into())));
        // Another tenant's name with alice's MAC, a credential from another
        // token, a truncated MAC, and junk are all refused.
        let forged = alice.replacen("alice", "bob", 1);
        assert_eq!(v.caller(&forged), None);
        assert_eq!(v.caller(&credential("other-secret", "alice")), None);
        assert_eq!(v.caller(&alice[..alice.len() - 2]), None);
        for junk in ["", ".", "alice.", "alice.zz", "cluster-secret.x"] {
            assert_eq!(v.caller(junk), None, "{junk:?}");
        }
        assert!(!v.is_token(&alice) && v.is_token("cluster-secret"));
        let bad_name = format!("a b.{}", blake3::keyed_hash(&key("cluster-secret"), b"a b").to_hex());
        assert_eq!(v.caller(&bad_name), None, "only valid tenant names are issued or accepted");
    }

    #[test]
    fn an_admin_may_act_for_any_tenant_and_a_tenant_only_for_itself() {
        assert!(Caller::Admin.may("alice"));
        assert!(Caller::Tenant("alice".into()).may("alice"));
        assert!(!Caller::Tenant("alice".into()).may("bob"));
    }
}
