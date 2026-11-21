//! Per-tenant CPU quotas on this peer: `CHIMAERA_TENANT_QUOTAS="team-a=8,team-b=2.5"`.
//!
//! A run belongs to the tenant it was created for (`ch.init(tenant=...)`,
//! default "default"). While a tenant's running calls hold its quota of CPUs
//! here, its other calls wait (other tenants' work goes ahead); a call that
//! asks for more than the whole quota is infeasible. Tenants without a quota
//! are not limited. shortcut: quotas are per peer; cluster-wide shares (a
//! tenant's total across peers) come with the provisioner's view (plan P6).

use std::collections::HashMap;

use chimaera_core::ids::Id;

pub const ENV: &str = "CHIMAERA_TENANT_QUOTAS";

#[derive(Default)]
pub struct Quotas {
    caps: HashMap<String, f64>,
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
        Ok(Quotas { caps: parse(&std::env::var(ENV).unwrap_or_default())?, ..Default::default() })
    }

    pub fn enabled(&self) -> bool {
        !self.caps.is_empty()
    }

    /// The run's tenant, looked up once per run with `lookup`.
    pub fn tenant(&mut self, run: Id, lookup: impl FnOnce() -> Option<String>) -> String {
        self.tenants
            .entry(run)
            .or_insert_with(|| lookup().filter(|t| !t.is_empty()).unwrap_or_else(|| "default".into()))
            .clone()
    }

    pub fn admit(&self, tenant: &str, cpus: f64) -> Admit {
        let Some(&cap) = self.caps.get(tenant) else { return Admit::Yes };
        if cpus > cap + 1e-9 {
            return Admit::Never(format!("needs {cpus} CPUs; tenant {tenant:?} has a quota of {cap} on this peer"));
        }
        let used = self.used.get(tenant).copied().unwrap_or(0.0);
        if used + cpus > cap + 1e-9 {
            return Admit::Wait(format!("tenant {tenant:?} is using {used} of its {cap}-CPU quota on this peer"));
        }
        Admit::Yes
    }

    /// A call of `tenant` started holding `cpus`.
    pub fn take(&mut self, call: (Id, Id), tenant: &str, cpus: f64) {
        if self.caps.contains_key(tenant) && !self.held.contains_key(&call) {
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

/// `name=cpus,name=cpus`.
pub fn parse(text: &str) -> Result<HashMap<String, f64>, String> {
    let mut caps = HashMap::new();
    for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, cpus) = entry.split_once('=').ok_or_else(|| format!("{ENV}: {entry:?} is not tenant=cpus"))?;
        let cpus: f64 = cpus.trim().parse().map_err(|_| format!("{ENV}: {cpus:?} is not a number of CPUs"))?;
        if cpus.is_nan() || cpus <= 0.0 {
            return Err(format!("{ENV}: tenant {name:?} needs a positive quota"));
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
        let mut q = Quotas { caps: parse("team-a=2, team-b=0.5").unwrap(), ..Default::default() };
        assert!(q.enabled());
        assert_eq!(q.admit("team-a", 1.0), Admit::Yes);
        q.take(call(1), "team-a", 1.0);
        q.take(call(2), "team-a", 1.0);
        assert!(matches!(q.admit("team-a", 1.0), Admit::Wait(w) if w.contains("using 2 of its 2-CPU quota")));
        assert_eq!(q.admit("other", 64.0), Admit::Yes, "tenants without a quota are not limited");
        assert!(matches!(q.admit("team-b", 1.0), Admit::Never(_)));
        q.give(call(1));
        q.give(call(1)); // idempotent
        assert_eq!(q.admit("team-a", 1.0), Admit::Yes);
        q.take(call(1), "team-a", 1.0);
        q.take(call(1), "team-a", 1.0); // the same call counts once
        assert!(matches!(q.admit("team-a", 0.5), Admit::Wait(_)));
        // A call waiting on its children lends its share, and takes it back.
        q.pause(call(1));
        q.pause(call(1));
        assert_eq!(q.admit("team-a", 1.0), Admit::Yes);
        q.resume(call(1));
        assert!(matches!(q.admit("team-a", 0.5), Admit::Wait(_)));
        q.pause(call(1));
        q.give(call(1)); // ends while paused: nothing to give back twice
        q.give(call(2));
        assert_eq!(q.admit("team-a", 2.0), Admit::Yes);
    }

    #[test]
    fn quotas_parse_strictly_and_runs_default_their_tenant() {
        assert!(parse("").unwrap().is_empty());
        assert!(parse("a").is_err());
        assert!(parse("a=x").is_err());
        assert!(parse("a=0").is_err());
        let mut q = Quotas::default();
        assert_eq!(q.tenant(Id([3; 16]), || Some(String::new())), "default");
        assert_eq!(q.tenant(Id([4; 16]), || Some("ml".into())), "ml");
        assert_eq!(q.tenant(Id([4; 16]), || unreachable!("cached")), "ml");
    }
}
