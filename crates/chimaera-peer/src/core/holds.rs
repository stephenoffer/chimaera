//! The resource ledger: what this peer holds for gang generations and
//! reservations (`Peer.Hold`).
//!
//! A hold is a list of slots (CPUs and whole devices) for one owner's
//! generation, taken from free capacity all or none. Held CPUs count in
//! `cpu_used` and held devices are busy, so no other call or actor gets them.
//! A call or actor carrying a slot's `LaunchToken` starts on exactly that
//! slot: its CPUs and devices become its own grant, released when it ends as
//! any call's. A hold ends when its owner releases it, when `ttl` passes
//! without a renewal (the owner is gone: its untaken slots are free again),
//! or when a newer generation of the same owner holds here. A token of an
//! ended hold, of an older generation or of a taken slot is refused, so a
//! stale rank never starts on its successor's resources.
//!
//! An owner is named under its tenant (`tenant/gang/...`): a tenant's
//! credential holds only its own, and only its own runs start on them.

use super::*;
use crate::devices::Devices;
use chimaera_core::devices::Device;

struct Slot {
    cpus: f64,
    devices: Vec<Device>,
    taken: bool,
}

struct Hold {
    generation: u64,
    slots: Vec<Slot>,
    ttl: Duration,
    renewed: Instant,
}

impl Hold {
    fn expired(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.renewed) > self.ttl
    }
}

#[derive(Default)]
pub(super) struct Ledger {
    holds: HashMap<String, Hold>,
}

impl Ledger {
    /// CPUs held in slots no call or actor has taken yet (part of `cpu_used`).
    pub(super) fn cpus(&self) -> f64 {
        self.holds.values().flat_map(|h| &h.slots).filter(|s| !s.taken).map(|s| s.cpus).sum()
    }

    /// Hold `r.slots` for `r.owner`'s generation, every slot or none, from
    /// `room` (the CPUs calls and actors leave free) and the free devices,
    /// in place of the owner's hold of that generation or an older one (which
    /// ends first, whether or not the new one fits). With `r.renew` it only
    /// extends the hold this generation has. A refusal says why, and whether
    /// this peer could never hold the slots, even with nothing else running.
    pub(super) fn hold(
        &mut self,
        r: &pb::HoldRequest,
        (room, capacity): (f64, f64),
        devices: &mut Devices,
        now: Instant,
    ) -> Result<(), (String, bool)> {
        let (owner, generation, ttl) = (r.owner.as_str(), r.generation, Duration::from_millis(r.ttl_ms));
        match self.holds.get_mut(owner) {
            Some(h) if r.renew && h.generation == generation && !h.expired(now) => {
                (h.renewed, h.ttl) = (now, ttl);
                return Ok(());
            }
            Some(h) if h.generation > generation => {
                let why =
                    format!("generation {generation} of {owner} is stale: generation {} holds here", h.generation);
                return Err((why, false));
            }
            _ if r.renew => {
                return Err((format!("{owner} holds nothing here to renew (it expired or was released)"), false));
            }
            _ => self.release(owner, generation, devices),
        }
        let need: f64 = r.slots.iter().map(|s| s.cpus.max(0.0)).sum();
        if need > capacity + 1e-9 {
            return Err((format!("needs {need} CPUs; this peer has {capacity}"), true));
        }
        let wants: Vec<(f64, &str)> = r.slots.iter().map(|s| (s.gpus, s.device.as_str())).collect();
        if let Some(why) = devices.never(&wants) {
            return Err((why, true));
        }
        let free = room - self.cpus();
        if need > free + 1e-9 {
            return Err((format!("needs {need} CPUs; {:.2} of {capacity} free", free.max(0.0)), false));
        }
        let mut slots: Vec<Slot> = Vec::with_capacity(r.slots.len());
        for s in &r.slots {
            match devices.take(s.gpus, &s.device) {
                Ok(held) => slots.push(Slot { cpus: s.cpus.max(0.0), devices: held, taken: false }),
                Err(why) => {
                    for slot in &slots {
                        devices.give(&slot.devices);
                    }
                    return Err((why, false));
                }
            }
        }
        self.holds.insert(owner.to_string(), Hold { generation, slots, ttl, renewed: now });
        Ok(())
    }

    /// End `owner`'s hold if it is of `generation` or older: its untaken
    /// slots are free.
    pub(super) fn release(&mut self, owner: &str, generation: u64, devices: &mut Devices) {
        if self.holds.get(owner).is_some_and(|h| h.generation <= generation)
            && let Some(h) = self.holds.remove(owner)
        {
            for s in h.slots.iter().filter(|s| !s.taken) {
                devices.give(&s.devices);
            }
        }
    }

    /// End every hold not renewed within its ttl; the owners whose holds ended.
    pub(super) fn expire(&mut self, now: Instant, devices: &mut Devices) -> Vec<(String, u64)> {
        let gone: Vec<(String, u64)> =
            self.holds.iter().filter(|(_, h)| h.expired(now)).map(|(o, h)| (o.clone(), h.generation)).collect();
        for (owner, generation) in &gone {
            self.release(owner, *generation, devices);
        }
        gone
    }

    /// The slot `t` names, if a call or actor may start on it now: its CPUs
    /// and devices.
    pub(super) fn peek(&self, t: &pb::LaunchToken, now: Instant) -> Result<(f64, Vec<Device>), String> {
        let stale =
            |why: String| format!("the launch token of {} generation {} is stale: {why}", t.owner, t.generation);
        let Some(h) = self.holds.get(&t.owner).filter(|h| !h.expired(now)) else {
            return Err(stale("this peer holds nothing for it (the hold expired or was released)".into()));
        };
        if h.generation != t.generation {
            return Err(stale(format!("generation {} holds here", h.generation)));
        }
        match h.slots.get(t.slot as usize) {
            None => Err(stale(format!("the hold has {} slot(s), not {}", h.slots.len(), t.slot + 1))),
            Some(s) if s.taken => Err(stale(format!("slot {} was taken already", t.slot))),
            Some(s) => Ok((s.cpus, s.devices.clone())),
        }
    }

    /// Take the slot `t` names for the call or actor starting on it: its
    /// devices go back to the allocator, for that executor to hold.
    pub(super) fn take(
        &mut self,
        t: &pb::LaunchToken,
        now: Instant,
        devices: &mut Devices,
    ) -> Result<Vec<Device>, String> {
        let (_, held) = self.peek(t, now)?;
        let h = self.holds.get_mut(&t.owner).expect("peek found it");
        h.slots[t.slot as usize].taken = true;
        devices.give(&held);
        Ok(held)
    }

    /// One line per hold, for `HoldReply.holds`.
    pub(super) fn describe(&self, now: Instant) -> Vec<String> {
        let mut lines: Vec<String> = self
            .holds
            .iter()
            .map(|(owner, h)| {
                let open: Vec<&Slot> = h.slots.iter().filter(|s| !s.taken).collect();
                let left = h.ttl.saturating_sub(now.saturating_duration_since(h.renewed));
                format!(
                    "{owner} generation {}: {} of {} slot(s) untaken ({} CPUs, {} device(s)); expires in {:.1} s",
                    h.generation,
                    open.len(),
                    h.slots.len(),
                    open.iter().map(|s| s.cpus).sum::<f64>(),
                    open.iter().map(|s| s.devices.len()).sum::<usize>(),
                    left.as_secs_f64()
                )
            })
            .collect();
        lines.sort();
        lines
    }
}

impl Core {
    /// `Peer.Hold` asked of this peer: hold, renew, release or list.
    pub(super) fn on_hold(&mut self, r: pb::HoldRequest) -> pb::HoldReply {
        let (now, before) = (Instant::now(), self.ledger.cpus());
        let result = if r.owner.is_empty() {
            Ok(())
        } else if r.slots.is_empty() {
            self.ledger.release(&r.owner, r.generation, &mut self.devices);
            Ok(())
        } else if self.draining && !r.renew {
            Err(("this peer is draining: it takes no new holds".to_string(), false))
        } else {
            let room = self.capacity - (self.cpu_used - before);
            self.ledger.hold(&r, (room, self.capacity), &mut self.devices, now)
        };
        self.cpu_used += self.ledger.cpus() - before;
        self.dispatch(); // a release or a superseded hold may have freed room
        let held = result.is_ok();
        let (reason, never) = result.err().unwrap_or_default();
        pb::HoldReply { held, reason, holds: self.ledger.describe(now), never }
    }

    /// Holds whose owners stopped renewing them end (housekeeping).
    pub(super) fn expire_holds(&mut self) {
        let before = self.ledger.cpus();
        let gone = self.ledger.expire(Instant::now(), &mut self.devices);
        if gone.is_empty() {
            return;
        }
        for (owner, generation) in gone {
            tracing::info!(owner, generation, "a hold expired unrenewed: its untaken slots are free");
        }
        self.cpu_used += self.ledger.cpus() - before;
        self.dispatch();
    }

    /// The slot a call or actor of `run` may start on now: its CPUs and
    /// devices.
    pub(super) fn slot_of(&mut self, run: Id, t: &pb::LaunchToken) -> Result<(f64, Vec<Device>), String> {
        let tenant = self.tenant_of(run);
        if t.owner.split_once('/').is_none_or(|(of, _)| of != tenant) {
            return Err(format!("the launch token of {} is not for the calls of tenant {tenant}", t.owner));
        }
        self.ledger.peek(t, Instant::now())
    }

    /// Take the slot a call or actor of `run` starting now carries the token
    /// of: its CPUs stop counting as held (the caller charges them as the
    /// call's or the actor's), and its devices, returned, are for the caller
    /// to hold.
    pub(super) fn take_slot(&mut self, run: Id, t: &pb::LaunchToken) -> Result<Vec<Device>, String> {
        self.slot_of(run, t)?;
        let before = self.ledger.cpus();
        let taken = self.ledger.take(t, Instant::now(), &mut self.devices);
        self.cpu_used += self.ledger.cpus() - before;
        taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn eight_gpus() -> Devices {
        let root = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/topology/dgx-h100"));
        Devices::new(crate::devices::discover(&root, &BTreeMap::new()))
    }

    /// A hold of `n` slots of `cpus` and `gpus` each, for 10 s.
    fn req(owner: &str, generation: u64, n: usize, cpus: f64, gpus: f64, renew: bool) -> pb::HoldRequest {
        let slots = vec![pb::HoldShape { cpus, gpus, device: String::new() }; n];
        pb::HoldRequest { owner: owner.into(), generation, slots, ttl_ms: 10_000, renew, ..Default::default() }
    }

    fn ranks(owner: &str, generation: u64, n: usize) -> pb::HoldRequest {
        req(owner, generation, n, 1.0, 1.0, false)
    }

    fn token(owner: &str, generation: u64, slot: u32) -> pb::LaunchToken {
        pb::LaunchToken { owner: owner.into(), generation, slot }
    }

    #[test]
    fn two_eight_rank_gangs_on_eight_gpus_hold_one_whole_gang() {
        let (mut l, mut d, now) = (Ledger::default(), eight_gpus(), Instant::now());
        l.hold(&ranks("gang/a", 1, 8), (16.0, 16.0), &mut d, now).unwrap();
        let why = l.hold(&ranks("gang/b", 1, 8), (16.0, 16.0), &mut d, now).unwrap_err();
        assert_eq!(why, ("needs 1 accelerator(s); 0 of 8 free".into(), false), "a wait: a gang's turn comes");
        assert_eq!(l.cpus(), 8.0, "b holds nothing: none of its slots stayed held");
        assert_eq!(l.describe(now).len(), 1);
        // A partial fit is no fit either: the slots it took go back.
        l.release("gang/a", 1, &mut d);
        l.hold(&ranks("gang/c", 1, 3), (16.0, 16.0), &mut d, now).unwrap();
        assert!(l.hold(&ranks("gang/b", 1, 8), (16.0, 16.0), &mut d, now).is_err());
        assert_eq!(d.wait_reason(5.0, ""), None, "the five b took for a moment are free again");
        l.release("gang/c", 1, &mut d);
        l.hold(&ranks("gang/b", 1, 8), (16.0, 16.0), &mut d, now).unwrap();
        assert_eq!(l.cpus(), 8.0);
    }

    #[test]
    fn slots_no_idle_peer_could_hold_are_refused_for_good() {
        let (mut l, mut d, now) = (Ledger::default(), eight_gpus(), Instant::now());
        l.hold(&ranks("gang/a", 1, 8), (16.0, 16.0), &mut d, now).unwrap();
        // Nine devices are never free here, busy or not; a rocm device does not exist.
        let why = l.hold(&ranks("gang/b", 1, 9), (16.0, 16.0), &mut d, now).unwrap_err();
        assert_eq!(why, ("needs 9 accelerator(s); this peer has 8".into(), true));
        let mut rocm = ranks("gang/b", 1, 1);
        rocm.slots[0].device = "rocm".into();
        assert_eq!(
            l.hold(&rocm, (16.0, 16.0), &mut d, now).unwrap_err(),
            ("needs 1 rocm device(s); this peer has 0".into(), true)
        );
        assert_eq!(l.cpus(), 8.0, "a's hold is untouched");
    }

    #[test]
    fn asking_again_replaces_the_hold_and_renewing_only_extends_it() {
        let (mut l, mut d, now) = (Ledger::default(), eight_gpus(), Instant::now());
        l.hold(&ranks("gang/r", 1, 8), (8.0, 8.0), &mut d, now).unwrap();
        l.hold(&ranks("gang/r", 1, 3), (8.0, 8.0), &mut d, now).unwrap(); // a later try's plan
        assert_eq!(l.cpus(), 3.0);
        assert_eq!(d.wait_reason(5.0, ""), None, "the earlier try's other five devices are free");
        l.hold(&req("gang/r", 1, 8, 1.0, 1.0, true), (8.0, 8.0), &mut d, now).unwrap();
        assert_eq!(l.cpus(), 3.0, "a renewal takes nothing more");
    }

    #[test]
    fn cpus_are_held_from_what_calls_leave_free() {
        let (mut l, mut d, now) = (Ledger::default(), eight_gpus(), Instant::now());
        assert_eq!(
            l.hold(&req("r", 1, 3, 2.0, 0.0, false), (5.0, 8.0), &mut d, now).unwrap_err(),
            ("needs 6 CPUs; 5.00 of 8 free".into(), false)
        );
        assert_eq!(
            l.hold(&req("r", 1, 5, 2.0, 0.0, false), (8.0, 8.0), &mut d, now).unwrap_err(),
            ("needs 10 CPUs; this peer has 8".into(), true)
        );
        l.hold(&req("r", 1, 2, 2.0, 0.0, false), (5.0, 8.0), &mut d, now).unwrap();
        assert!(l.hold(&req("s", 1, 1, 2.0, 0.0, false), (5.0, 8.0), &mut d, now).is_err(), "1 of the 5 is left");
    }

    #[test]
    fn a_hold_not_renewed_expires_and_a_renewal_keeps_it() {
        let (mut l, mut d, t0) = (Ledger::default(), eight_gpus(), Instant::now());
        l.hold(&ranks("reserve/x", 1, 8), (8.0, 8.0), &mut d, t0).unwrap();
        let t1 = t0 + Duration::from_secs(8);
        l.hold(&req("reserve/x", 1, 8, 1.0, 1.0, true), (8.0, 8.0), &mut d, t1).unwrap();
        assert!(l.expire(t0 + Duration::from_secs(15), &mut d).is_empty(), "renewed at 8 s: good until 18 s");
        assert_eq!(l.expire(t0 + Duration::from_secs(19), &mut d), vec![("reserve/x".to_string(), 1)]);
        assert_eq!((l.cpus(), d.wait_reason(8.0, "")), (0.0, None), "every device is free again");
        let late = t0 + Duration::from_secs(20);
        assert!(
            l.hold(&req("reserve/x", 1, 8, 1.0, 1.0, true), (8.0, 8.0), &mut d, late)
                .unwrap_err()
                .0
                .contains("to renew")
        );
    }

    #[test]
    fn a_token_starts_once_on_its_own_generation_only() {
        let (mut l, mut d, now) = (Ledger::default(), eight_gpus(), Instant::now());
        l.hold(&ranks("gang/g", 1, 2), (8.0, 8.0), &mut d, now).unwrap();
        let one = l.take(&token("gang/g", 1, 1), now, &mut d).unwrap();
        assert_eq!((one.len(), l.cpus()), (1, 1.0), "its CPU is the call's now");
        assert_eq!(d.wait_reason(7.0, ""), None, "its device went back for the call's executor to hold");
        assert!(l.peek(&token("gang/g", 1, 1), now).unwrap_err().ends_with("slot 1 was taken already"));
        assert!(l.peek(&token("gang/g", 1, 2), now).is_err());
        // Generation 2 holds: generation 1's untaken slot is free, and its token stale.
        l.hold(&ranks("gang/g", 2, 2), (8.0, 8.0), &mut d, now).unwrap();
        let why = l.peek(&token("gang/g", 1, 0), now).unwrap_err();
        assert_eq!(why, "the launch token of gang/g generation 1 is stale: generation 2 holds here");
        assert!(l.hold(&ranks("gang/g", 1, 1), (8.0, 8.0), &mut d, now).unwrap_err().0.contains("is stale"));
        l.release("gang/g", 1, &mut d);
        assert!(l.peek(&token("gang/g", 2, 0), now).is_ok(), "an older generation's release ends nothing newer");
        l.release("gang/g", 2, &mut d);
        assert!(l.peek(&token("gang/g", 2, 0), now).unwrap_err().contains("holds nothing"));
        assert_eq!(l.cpus(), 0.0);
    }
}
