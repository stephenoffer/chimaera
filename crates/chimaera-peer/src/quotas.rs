//! Per-tenant CPU limits: `CHIMAERA_TENANT_QUOTAS="team-a=8,team-b=2.5"` on
//! this peer, and `CHIMAERA_TENANT_SHARES="team-a=64"` across the cluster.
//!
//! A run belongs to the tenant it was created for (`ch.init(tenant=...)`,
//! default "default"). While a tenant's running calls hold its quota of CPUs
//! here, or its share across every peer, its other calls wait (other tenants'
//! work goes ahead); a call that asks for more than a whole quota or share is
//! infeasible. Tenants without either are not limited. Each peer reports its
//! tenants' CPUs in its load replies, and a share counts the other peers'
//! last reports, so peers admitting at the same moment can overshoot a share
//! briefly, by at most the calls started between two reports.

use std::collections::HashMap;

use chimaera_core::ids::Id;

pub const ENV: &str = "CHIMAERA_TENANT_QUOTAS";
pub const SHARES_ENV: &str = "CHIMAERA_TENANT_SHARES";

#[derive(Default)]
pub struct Quotas {
    caps: HashMap<String, f64>,
    shares: HashMap<String, f64>,
    used: HashMap<String, f64>,
    held: HashMap<(Id, Id), (String, f64, bool)>,
    tenants: HashMap<Id, String>,
}

/// Whether a call may start now.
#[derive(Debug, PartialEq)]
pub enum Admit {
    Yes,
    /// Over the tenant's quota for now; the reason names it.
    Wait(String),
    /// Larger than the tenant's whole quota.
    Never(String),
}

impl Quotas {
    pub fn from_env() -> Result<Quotas, String> {
        Ok(Quotas {
            caps: parse(ENV, &std::env::var(ENV).unwrap_or_default())?,
            shares: parse(SHARES_ENV, &std::env::var(SHARES_ENV).unwrap_or_default())?,
            ..Default::default()
        })
    }

    pub fn enabled(&self) -> bool {
        !self.caps.is_empty() || !self.shares.is_empty()
    }

    pub fn has_share(&self, tenant: &str) -> bool {
        self.shares.contains_key(tenant)
    }

    /// CPUs held here per tenant with a share, for this peer's load replies.
    pub fn usage(&self) -> HashMap<String, f64> {
        self.used.iter().filter(|(t, _)| self.shares.contains_key(*t)).map(|(t, u)| (t.clone(), *u)).collect()
    }

    fn limited(&self, tenant: &str) -> bool {
        self.caps.contains_key(tenant) || self.shares.contains_key(tenant)
    }

    /// The run's tenant, looked up once per run with `lookup`.
    pub fn tenant(&mut self, run: Id, lookup: impl FnOnce() -> Option<String>) -> String {
        self.tenants
            .entry(run)
            .or_insert_with(|| lookup().filter(|t| !t.is_empty()).unwrap_or_else(|| "default".into()))
            .clone()
    }

    /// Whether a call of `tenant` needing `cpus` may start here, given the
    /// CPUs the tenant holds on other peers (`elsewhere`, from their reports).
    pub fn admit(&self, tenant: &str, cpus: f64, elsewhere: f64) -> Admit {
        let used = self.used.get(tenant).copied().unwrap_or(0.0);
        if let Some(&cap) = self.caps.get(tenant) {
            if cpus > cap + 1e-9 {
                return Admit::Never(format!("needs {cpus} CPUs; tenant {tenant:?} has a quota of {cap} on this peer"));
            }
            if used + cpus > cap + 1e-9 {
                return Admit::Wait(format!("tenant {tenant:?} is using {used} of its {cap}-CPU quota on this peer"));
            }
        }
        if let Some(&share) = self.shares.get(tenant) {
            if cpus > share + 1e-9 {
                return Admit::Never(format!(
                    "needs {cpus} CPUs; tenant {tenant:?} has a share of {share} in the cluster"
                ));
            }
            let total = used + elsewhere;
            if total + cpus > share + 1e-9 {
                return Admit::Wait(format!(
                    "tenant {tenant:?} is using {total} of its {share}-CPU share in the cluster"
                ));
            }
        }
        Admit::Yes
    }

    /// A call of `tenant` started holding `cpus`.
    pub fn take(&mut self, call: (Id, Id), tenant: &str, cpus: f64) {
        if self.limited(tenant) && !self.held.contains_key(&call) {
            *self.used.entry(tenant.to_string()).or_default() += cpus;
            self.held.insert(call, (tenant.to_string(), cpus, false));
        }
    }

    /// The call ended (or its worker died); a second give is a no-op.
    pub fn give(&mut self, call: (Id, Id)) {
        if let Some((tenant, cpus, paused)) = self.held.remove(&call)
            && !paused
            && let Some(u) = self.used.get_mut(&tenant)
        {
            *u = (*u - cpus).max(0.0);
        }
    }

    /// The call waits on other calls: it lends its share, as it lends its CPU,
    /// so the work it waits for can run (nested calls never deadlock a quota).
    pub fn pause(&mut self, call: (Id, Id)) {
        if let Some((tenant, cpus, paused)) = self.held.get_mut(&call)
            && !*paused
        {
            *paused = true;
            if let Some(u) = self.used.get_mut(tenant.as_str()) {
                *u = (*u - *cpus).max(0.0);
            }
        }
    }

    /// The call runs again (it may go over the quota for a while, as the CPU count does).
    pub fn resume(&mut self, call: (Id, Id)) {
        if let Some((tenant, cpus, paused)) = self.held.get_mut(&call)
            && *paused
        {
            *paused = false;
            *self.used.entry(tenant.clone()).or_default() += *cpus;
        }
    }
}

/// `name=cpus,name=cpus`, as set in the variable `var`.
pub fn parse(var: &str, text: &str) -> Result<HashMap<String, f64>, String> {
    let mut caps = HashMap::new();
    for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, cpus) = entry.split_once('=').ok_or_else(|| format!("{var}: {entry:?} is not tenant=cpus"))?;
        let cpus: f64 = cpus.trim().parse().map_err(|_| format!("{var}: {cpus:?} is not a number of CPUs"))?;
        if cpus.is_nan() || cpus <= 0.0 {
            return Err(format!("{var}: tenant {name:?} needs a positive number of CPUs"));
        }
        caps.insert(name.trim().to_string(), cpus);
    }
    Ok(caps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(n: u8) -> (Id, Id) {
        (Id([1; 16]), Id([n; 16]))
    }

    #[test]
    fn a_tenant_waits_at_its_quota_and_others_go_ahead() {
        let mut q = Quotas { caps: parse(ENV, "team-a=2, team-b=0.5").unwrap(), ..Default::default() };
        assert!(q.enabled());
        assert_eq!(q.admit("team-a", 1.0, 0.0), Admit::Yes);
        q.take(call(1), "team-a", 1.0);
        q.take(call(2), "team-a", 1.0);
        assert!(matches!(q.admit("team-a", 1.0, 0.0), Admit::Wait(w) if w.contains("using 2 of its 2-CPU quota")));
        assert_eq!(q.admit("other", 64.0, 0.0), Admit::Yes, "tenants without a quota are not limited");
        assert!(matches!(q.admit("team-b", 1.0, 0.0), Admit::Never(_)));
        q.give(call(1));
        q.give(call(1)); // idempotent
        assert_eq!(q.admit("team-a", 1.0, 0.0), Admit::Yes);
        q.take(call(1), "team-a", 1.0);
        q.take(call(1), "team-a", 1.0); // the same call counts once
        assert!(matches!(q.admit("team-a", 0.5, 0.0), Admit::Wait(_)));
        // A call waiting on its children lends its share, and takes it back.
        q.pause(call(1));
        q.pause(call(1));
        assert_eq!(q.admit("team-a", 1.0, 0.0), Admit::Yes);
        q.resume(call(1));
        assert!(matches!(q.admit("team-a", 0.5, 0.0), Admit::Wait(_)));
        q.pause(call(1));
        q.give(call(1)); // ends while paused: nothing to give back twice
        q.give(call(2));
        assert_eq!(q.admit("team-a", 2.0, 0.0), Admit::Yes);
    }

    #[test]
    fn quotas_parse_strictly_and_runs_default_their_tenant() {
        assert!(parse(ENV, "").unwrap().is_empty());
        assert!(parse(ENV, "a").is_err());
        assert!(parse(ENV, "a=x").is_err());
        assert!(parse(ENV, "a=0").is_err());
        let mut q = Quotas::default();
        assert_eq!(q.tenant(Id([3; 16]), || Some(String::new())), "default");
        assert_eq!(q.tenant(Id([4; 16]), || Some("ml".into())), "ml");
        assert_eq!(q.tenant(Id([4; 16]), || unreachable!("cached")), "ml");
    }

    #[test]
    fn a_share_counts_what_the_tenant_holds_on_other_peers() {
        let mut q = Quotas { shares: parse(SHARES_ENV, "ml=8").unwrap(), ..Default::default() };
        assert!(q.enabled() && q.has_share("ml") && !q.has_share("web"));
        // 6 CPUs held elsewhere: 2 fit here, a third waits; other tenants are not limited.
        assert_eq!(q.admit("ml", 2.0, 6.0), Admit::Yes);
        q.take(call(1), "ml", 2.0);
        assert!(
            matches!(q.admit("ml", 1.0, 6.0), Admit::Wait(w) if w.contains("using 8 of its 8-CPU share in the cluster"))
        );
        assert_eq!(q.admit("web", 64.0, 0.0), Admit::Yes);
        assert!(matches!(q.admit("ml", 9.0, 0.0), Admit::Never(w) if w.contains("share of 8")));
        // What this peer reports for the others: only tenants with a share.
        assert_eq!(q.usage(), HashMap::from([("ml".to_string(), 2.0)]));
        q.give(call(1));
        assert_eq!(q.admit("ml", 2.0, 6.0), Admit::Yes);
        assert!(parse(SHARES_ENV, "ml=-1").unwrap_err().starts_with("CHIMAERA_TENANT_SHARES:"));
    }
}
