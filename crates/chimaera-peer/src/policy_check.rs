//! A call's policy as a client sent it, checked before it is stored: the
//! Python package checks options too, but any client may talk to a peer, and
//! a NaN or an inverted bound in the log would steer every later decision
//! about the call (a NaN CPU request is neither more nor less than anything).

use chimaera_core::pb;
use prost::Message;

/// The policy in `bytes` (empty: the default), or why it cannot be used.
pub fn decode(bytes: &[u8]) -> Result<pb::Policy, String> {
    let p = pb::Policy::decode(bytes).map_err(|e| format!("the call's policy does not decode: {e}"))?;
    check(&p)?;
    Ok(p)
}

/// Why `p` cannot be used, if it cannot.
pub fn check(p: &pb::Policy) -> Result<(), String> {
    let amount = |name: &str, v: f64| {
        if v.is_finite() && v >= 0.0 { Ok(()) } else { Err(format!("{name}: expected a finite number >= 0, got {v}")) }
    };
    amount("cpus", p.cpus)?;
    amount("gpus", p.gpus)?;
    amount("cpus min", p.cpus_min)?;
    amount("cpus max", p.cpus_max)?;
    amount("budget cpu_seconds", p.budget_cpu_seconds)?;
    amount("budget gpu_seconds", p.budget_gpu_seconds)?;
    for (k, v) in &p.resources {
        amount(&format!("resources[{k:?}]"), *v)?;
    }
    // 0 is "no bound" on either side.
    if p.cpus_min > 0.0 && p.cpus_max > 0.0 && p.cpus_min > p.cpus_max {
        return Err(format!("cpus: ch.auto min {} is more than max {}", p.cpus_min, p.cpus_max));
    }
    if p.memory_min > 0 && p.memory_max > 0 && p.memory_min > p.memory_max {
        return Err(format!("memory: ch.auto min {} is more than max {} bytes", p.memory_min, p.memory_max));
    }
    // An initial value is where learning starts, inside its bounds.
    let outside = |x: f64, lo: f64, hi: f64| (lo > 0.0 && x < lo) || (hi > 0.0 && x > hi);
    if p.cpus_initial && outside(p.cpus, p.cpus_min, p.cpus_max) {
        return Err(format!("cpus: ch.auto initial {} is outside min {} and max {}", p.cpus, p.cpus_min, p.cpus_max));
    }
    if p.memory_initial && outside(p.memory as f64, p.memory_min as f64, p.memory_max as f64) {
        return Err(format!(
            "memory: ch.auto initial {} is outside min {} and max {} bytes",
            p.memory, p.memory_min, p.memory_max
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(p: &pb::Policy) -> Vec<u8> {
        p.encode_to_vec()
    }

    #[test]
    fn the_default_and_ordinary_policies_pass() {
        assert!(decode(&[]).is_ok(), "no policy is the default policy");
        let p =
            pb::Policy { cpus: 2.0, cpus_min: 0.5, cpus_max: 4.0, memory_min: 1, memory_max: 1, ..Default::default() };
        assert_eq!(decode(&bytes(&p)).unwrap(), p);
    }

    #[test]
    fn non_finite_and_negative_amounts_are_refused_by_name() {
        for (name, p) in [
            ("cpus", pb::Policy { cpus: f64::NAN, ..Default::default() }),
            ("cpus", pb::Policy { cpus: f64::INFINITY, ..Default::default() }),
            ("gpus", pb::Policy { gpus: -1.0, ..Default::default() }),
            ("cpus max", pb::Policy { cpus_max: f64::NAN, ..Default::default() }),
            ("budget cpu_seconds", pb::Policy { budget_cpu_seconds: -3.0, ..Default::default() }),
            ("resources[\"tpu\"]", pb::Policy { resources: [("tpu".into(), f64::NAN)].into(), ..Default::default() }),
        ] {
            let why = decode(&bytes(&p)).unwrap_err();
            assert!(why.starts_with(name), "{why}");
        }
    }

    #[test]
    fn inverted_bounds_are_refused() {
        let p = pb::Policy { cpus_min: 4.0, cpus_max: 1.0, ..Default::default() };
        assert!(decode(&bytes(&p)).unwrap_err().contains("min 4 is more than max 1"));
        let p = pb::Policy { memory_min: 2, memory_max: 1, ..Default::default() };
        assert!(decode(&bytes(&p)).is_err());
        // One side unbounded is fine.
        assert!(decode(&bytes(&pb::Policy { cpus_min: 4.0, ..Default::default() })).is_ok());
        // An initial value outside its bounds is refused; a pinned one is not bounded by them.
        let p = pb::Policy { cpus: 8.0, cpus_initial: true, cpus_max: 4.0, ..Default::default() };
        assert!(decode(&bytes(&p)).unwrap_err().contains("initial 8 is outside"));
        assert!(decode(&bytes(&pb::Policy { cpus_initial: false, ..p })).is_ok());
        let p = pb::Policy { memory: 1, memory_initial: true, memory_min: 2, ..Default::default() };
        assert!(decode(&bytes(&p)).is_err());
    }

    #[test]
    fn bytes_that_are_not_a_policy_are_refused() {
        assert!(decode(&[0xff, 0xff, 0xff]).unwrap_err().contains("does not decode"));
    }
}
