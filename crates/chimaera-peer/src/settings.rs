//! What an attempt ran with, setting by setting (AttemptRecord.settings): the
//! value granted, the value its source chose before any bound or limit
//! applied, the caller's ch.auto bounds, where the value came from, and why
//! the two differ when they do. `future.explain()` shows them per attempt.

use chimaera_core::pb;

/// One setting's numbers, as `Core::reserve` and the refreshes leave them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Chosen {
    /// What the source chose (the caller, learning, a trial, the default).
    pub asked: f64,
    /// What the attempt was granted.
    pub granted: f64,
    /// Profiles' `Source::kind`, or "raised" for memory an OOM retry raised.
    pub source: &'static str,
}

impl Default for Chosen {
    fn default() -> Chosen {
        Chosen { asked: 0.0, granted: 0.0, source: "default" }
    }
}

/// The settings of one attempt: cpus, memory and batch (calls that shared
/// its executor round trip). `capacity`: this peer's CPUs; `floor`: memory an
/// earlier attempt was killed at; `placer`: what the placing peer reserved.
pub fn of(
    p: &pb::Policy,
    cpus: Chosen,
    memory: Chosen,
    floor: u64,
    placer: (f64, u64),
    capacity: f64,
    batch: u32,
) -> Vec<pb::Setting> {
    let cpus_why = why(cpus, p.cpus_min, p.cpus_max, || {
        if cpus.granted == capacity {
            format!("this peer has {capacity} CPUs")
        } else if cpus.granted == placer.0 {
            format!("the peer that placed it reserved {}", placer.0)
        } else {
            String::new()
        }
    });
    let memory_why = why(memory, p.memory_min as f64, p.memory_max as f64, || {
        if floor > 0 && memory.granted >= floor as f64 {
            "raised after an attempt was killed for memory".into()
        } else if memory.granted == placer.1 as f64 {
            format!("the peer that placed it reserved {} bytes", placer.1)
        } else {
            String::new()
        }
    });
    vec![
        setting("cpus", cpus, p.cpus_min, p.cpus_max, cpus_why),
        setting("memory", memory, p.memory_min as f64, p.memory_max as f64, memory_why),
        pb::Setting {
            name: "batch".into(),
            value: f64::from(batch.max(1)),
            requested: f64::from(batch.max(1)),
            source: if batch > 1 { "learned" } else { "default" }.into(),
            reason: if batch > 1 {
                format!("{batch} fast calls of this function ran in one executor round trip")
            } else {
                String::new()
            },
            ..Default::default()
        },
    ]
}

fn setting(name: &str, c: Chosen, min: f64, max: f64, reason: String) -> pb::Setting {
    pb::Setting {
        name: name.into(),
        value: c.granted,
        requested: c.asked,
        min,
        max,
        source: if c.granted != c.asked { "clamped" } else { c.source }.into(),
        reason,
    }
}

/// Why `c` was granted something other than what its source chose ("" when
/// it was not): a ch.auto bound, else what `other` says.
fn why(c: Chosen, min: f64, max: f64, other: impl Fn() -> String) -> String {
    if c.granted == c.asked {
        return String::new();
    }
    let said = if min > 0.0 && c.granted == min && c.asked < min {
        format!("held at ch.auto min {min}")
    } else if max > 0.0 && c.granted == max && c.asked > max {
        format!("held at ch.auto max {max}")
    } else {
        other()
    };
    let from = format!("{} chose {}", c.source, c.asked);
    if said.is_empty() { from } else { format!("{said} ({from})") }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn by_name(s: &[pb::Setting], name: &str) -> pb::Setting {
        s.iter().find(|x| x.name == name).cloned().expect(name)
    }

    #[test]
    fn a_value_granted_as_chosen_names_its_source() {
        let s = of(
            &pb::Policy::default(),
            Chosen { asked: 2.0, granted: 2.0, source: "initial" },
            Chosen { asked: 64.0, granted: 64.0, source: "learned" },
            0,
            (0.0, 0),
            8.0,
            1,
        );
        let cpus = by_name(&s, "cpus");
        assert_eq!((cpus.value, cpus.requested, cpus.source.as_str(), cpus.reason.as_str()), (2.0, 2.0, "initial", ""));
        assert_eq!(by_name(&s, "memory").source, "learned");
        assert_eq!((by_name(&s, "batch").value, by_name(&s, "batch").source.as_str()), (1.0, "default"));
    }

    #[test]
    fn a_clamp_keeps_both_values_and_says_why() {
        let p = pb::Policy { cpus_min: 0.5, memory_max: 100, ..Default::default() };
        let s = of(
            &p,
            Chosen { asked: 0.25, granted: 0.5, source: "learned" },
            Chosen { asked: 300.0, granted: 100.0, source: "learned" },
            0,
            (0.0, 0),
            8.0,
            4,
        );
        let cpus = by_name(&s, "cpus");
        assert_eq!((cpus.value, cpus.requested, cpus.source.as_str()), (0.5, 0.25, "clamped"));
        assert_eq!(cpus.reason, "held at ch.auto min 0.5 (learned chose 0.25)");
        assert_eq!(by_name(&s, "memory").reason, "held at ch.auto max 100 (learned chose 300)");
        assert!(by_name(&s, "batch").reason.starts_with("4 fast calls"));
        let small = of(
            &pb::Policy::default(),
            Chosen { asked: 16.0, granted: 4.0, source: "learned" },
            Chosen { asked: 10.0, granted: 30.0, source: "learned" },
            30,
            (0.0, 0),
            4.0,
            1,
        );
        assert_eq!(by_name(&small, "cpus").reason, "this peer has 4 CPUs (learned chose 16)");
        assert!(by_name(&small, "memory").reason.starts_with("raised after an attempt was killed"));
    }
}
