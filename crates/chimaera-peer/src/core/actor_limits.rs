//! Per-method limits on an actor's calls (ActorSpec.methods): a method's
//! `ch.Mailbox`, and a batched method's queue, in calls and argument bytes.
//!
//! A mailbox holds up to `max_calls` of the method's calls: those queued
//! here and those the actor's process holds (running, or waiting there for
//! a thread). Queued calls take its room in the order they came (`Waiting`);
//! the rest wait for room, or fail with OVERLOADED at once (`reject`) or once
//! they have waited `max_wait_ms`. A batched method's queue bounds only the
//! calls and argument bytes its process holds; the rest wait here.
//!
//! A limited method's calls are counted apart from the actor's others, so
//! the actor's concurrency is left to its other methods and a flood of one
//! method never takes the room another needs. An actor that runs calls
//! concurrently promises no order, so there a mailbox method's calls leave
//! their handle's order: they start as their mailbox makes room, and the
//! handle's other calls go on without waiting for them. (An actor that runs
//! one call at a time still runs its calls one at a time and in order: there
//! the next call is the one ready longest, a mailbox call ready from when it
//! got room, so a mailbox bounds how many of the method's calls go ahead of
//! another handle's call to its max_calls.)

use super::*;
use std::ops::Bound;

/// What a limited method's calls hold at an executor: how many, and their bytes.
#[derive(Clone, Copy, Default)]
struct Held {
    calls: u32,
    bytes: u64,
}

/// Whether `w` fits beside what its method holds. A call larger than
/// `max_bytes` goes when nothing else of its method is there.
fn room(l: &pb::MethodLimit, h: Held, w: &Work) -> bool {
    (l.max_calls == 0 || h.calls < l.max_calls)
        && (l.max_bytes == 0 || h.calls == 0 || h.bytes + w.read_bytes <= l.max_bytes)
}

/// A mailbox counts the calls queued here too; a batch queue does not.
fn mailbox(l: &pb::MethodLimit) -> bool {
    l.reject || l.max_wait_ms > 0
}

/// Calls a mailbox has room for beside the `h.calls` its process holds.
fn mailbox_room(l: &pb::MethodLimit, h: Held) -> usize {
    if l.max_calls == 0 { usize::MAX } else { l.max_calls.saturating_sub(h.calls) as usize }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1 << 20) as f64
}

/// Queued calls, by (handle, seq).
type Keys = Vec<(u64, u64)>;
/// Queued calls that have room, each with when it first had room.
type Admitted = Vec<((u64, u64), Instant)>;

/// The calls of an actor's mailbox methods queued here, in the order they
/// take room, kept as calls come and go so that a pump reads only the first
/// few (ActorHost.waiting). A call's place is when it came, or the place of
/// its handle's next later call of the method if that is earlier: a
/// handle's calls of a method take room in order, so none waits for room
/// that calls which must start after it took. Calls that left the mailbox
/// some other way (cancelled, given back) are dropped where they are met.
#[derive(Default)]
pub(super) struct Waiting {
    /// The limits it was built for: a record with others rebuilds it.
    limits: Vec<pb::MethodLimit>,
    /// Per limit: (place, call) of its calls queued here (none for a batch queue).
    order: Vec<BTreeSet<(Instant, (u64, u64))>>,
    /// Per (limit, handle): the place of each of its calls queued here, by seq.
    places: HashMap<(usize, u64), BTreeMap<u64, Instant>>,
    /// When each of its calls queued here first had room (`front`).
    room_since: HashMap<(u64, u64), Instant>,
}

impl Waiting {
    /// Rebuild it for `limits` if they changed, and empty it with the mailbox.
    fn sync(&mut self, limits: &[pb::MethodLimit], mailbox: &BTreeMap<(u64, u64), Work>) {
        if (self.places.is_empty() || !mailbox.is_empty()) && self.limits == limits {
            return;
        }
        *self = Waiting { limits: limits.to_vec(), order: vec![BTreeSet::new(); limits.len()], ..Default::default() };
        for (&key, w) in mailbox.iter().rev() {
            self.add(key, w);
        }
    }

    fn slot(&self, method: &str) -> Option<usize> {
        self.limits.iter().position(|l| l.method == method && mailbox(l))
    }

    /// Queue `key`'s call at its place, if its method has a mailbox.
    fn add(&mut self, key: (u64, u64), w: &Work) {
        let Some(slot) = self.slot(&w.spec.method) else { return };
        let (handle, seq) = key;
        let order = &mut self.order[slot];
        let places = self.places.entry((slot, handle)).or_default();
        if let Some(old) = places.remove(&seq) {
            order.remove(&(old, key)); // queued again
            self.room_since.remove(&key);
        }
        let later = places.range((Bound::Excluded(seq), Bound::Unbounded)).next().map(|(_, p)| *p);
        let place = later.map_or(w.ready_at, |p| p.min(w.ready_at));
        // Its handle's earlier calls of the method go no later than it.
        let earlier: Vec<(u64, Instant)> =
            places.range(..seq).rev().take_while(|(_, p)| **p > place).map(|(s, p)| (*s, *p)).collect();
        for (s, p) in earlier {
            order.remove(&(p, (handle, s)));
            order.insert((place, (handle, s)));
            places.insert(s, place);
        }
        places.insert(seq, place);
        order.insert((place, key));
    }

    /// Take `key`'s call out: it started, or was turned away.
    fn remove(&mut self, key: (u64, u64), method: &str) {
        if let Some(slot) = self.slot(method) {
            self.remove_at(slot, key);
        }
    }

    fn remove_at(&mut self, slot: usize, key: (u64, u64)) {
        self.room_since.remove(&key);
        let Some(places) = self.places.get_mut(&(slot, key.0)) else { return };
        if let Some(place) = places.remove(&key.1) {
            self.order[slot].remove(&(place, key));
        }
        if places.is_empty() {
            self.places.remove(&(slot, key.0));
        }
    }

    /// A mailbox's calls as far as a pump needs them: the first `room`,
    /// which have room, each with when it first had it (`now` if not before
    /// this), then those past them that are turned away: all (`reject`), or
    /// those that came before `cutoff`. A call whose place is after `cutoff`
    /// came after it too, so the reading stops there.
    fn front(
        &mut self,
        slot: usize,
        mailbox: &BTreeMap<(u64, u64), Work>,
        room: usize,
        reject: bool,
        cutoff: Option<Instant>,
        now: Instant,
    ) -> (Admitted, Keys) {
        let (mut ok, mut away, mut gone) = (Vec::new(), Vec::new(), Vec::new());
        for &(place, key) in &self.order[slot] {
            let Some(w) = mailbox.get(&key) else {
                gone.push(key);
                continue;
            };
            if ok.len() < room {
                ok.push((key, *self.room_since.entry(key).or_insert(now)));
            } else if reject {
                away.push(key);
            } else if let Some(cutoff) = cutoff.filter(|c| place <= *c) {
                if w.ready_at <= cutoff {
                    away.push(key);
                }
            } else {
                break;
            }
        }
        for key in gone {
            self.remove_at(slot, key);
        }
        (ok, away)
    }

    /// Whether `key` is among the first `room` calls of its mailbox.
    fn has_room(&self, slot: usize, mailbox: &BTreeMap<(u64, u64), Work>, room: usize, key: (u64, u64)) -> bool {
        self.order[slot].iter().filter(|(_, k)| mailbox.contains_key(k)).take(room).any(|(_, k)| *k == key)
    }
}

/// Queue a call at its actor: every call goes into `ActorHost.mailbox`
/// through here, which keeps its mailbox's order.
pub(super) fn enqueue(host: &mut ActorHost, w: Work) {
    let key = (w.spec.handle, w.spec.seq);
    host.waiting.sync(&host.limits, &host.mailbox);
    host.waiting.add(key, &w);
    host.mailbox.insert(key, w);
}

impl Core {
    /// For each of the actor's limited methods, what its executor holds; and
    /// how many of its other calls (batches of them) run there.
    fn held(&self, host: &ActorHost) -> (Vec<Held>, usize) {
        let mut held = vec![Held::default(); host.limits.len()];
        let mut others = 0;
        for r in self.running.iter().filter(|r| Some(r.exec) == host.exec) {
            let Some(head) = r.items.first() else { continue };
            match host.limits.iter().position(|l| l.method == head.spec.method) {
                Some(i) => {
                    held[i].calls += 1;
                    let unfinished = r.items.iter().zip(&r.finished).filter(|(_, done)| !**done);
                    held[i].bytes += unfinished.map(|(w, _)| w.read_bytes).sum::<u64>();
                }
                None => others += 1,
            }
        }
        (held, others)
    }

    /// `actor_start_next` for an actor with method limits: turn away the
    /// calls their mailboxes have no room for, else start the next call. In
    /// an actor that runs calls concurrently that is a call a mailbox has
    /// room for, or else the first call next in its handle's order that may
    /// go now. In one that runs a call at a time it is, of the calls next in
    /// their handle's order that may go now, the one ready longest.
    pub(super) fn actor_start_limited(&mut self, actor: Id, exec: u32) -> bool {
        let concurrency = self.execs.get(&exec).map_or(1, |e| e.concurrency);
        let Some(host) = self.actors.get(&actor) else { return false };
        let (held, others) = self.held(host);
        // One call at a time means one call at a time, limited or not.
        let serial = concurrency <= 1;
        let busy = others + if serial { held.iter().map(|h| h.calls as usize).sum::<usize>() } else { 0 };
        let host = self.actors.get_mut(&actor).expect("present");
        host.waiting.sync(&host.limits, &host.mailbox);
        let now = Instant::now();
        let mut admitted = Vec::new();
        let mut away = Vec::new();
        for (slot, l) in host.limits.iter().enumerate().filter(|(_, l)| mailbox(l)) {
            let cutoff = now.checked_sub(Duration::from_millis(l.max_wait_ms));
            let room = mailbox_room(l, held[slot]);
            let (ok, late) = host.waiting.front(slot, &host.mailbox, room, l.reject, cutoff, now);
            admitted.extend(ok);
            let why = if l.reject {
                format!("{}: its mailbox is full ({} calls, its max_calls)", l.method, l.max_calls)
            } else {
                format!(
                    "{}: its mailbox stayed full ({} calls, its max_calls) for the {:.1} s this call may wait for room",
                    l.method,
                    l.max_calls,
                    l.max_wait_ms as f64 / 1e3
                )
            };
            away.extend(late.into_iter().map(|key| (key, why.clone())));
        }
        if !away.is_empty() {
            for (key, why) in away {
                self.turn_away(actor, key, why);
            }
            return true;
        }
        let host = self.actors.get_mut(&actor).expect("present");
        if !serial {
            // Mailbox calls at the head of their handle's order step out of
            // it, so the handle's next calls need not wait for their room.
            for (handle, _) in heads(host) {
                loop {
                    let at = host.next_seq.get(&handle).copied().unwrap_or(0);
                    let Some(w) = host.mailbox.get(&(handle, at)) else { break };
                    if host.waiting.slot(&w.spec.method).is_none() {
                        break;
                    }
                    host.known_handles.insert(handle);
                    host.next_seq.insert(handle, at + 1);
                    advance(host, handle);
                }
            }
        }
        let admitted_set: HashMap<(u64, u64), Instant> = admitted.iter().copied().collect();
        let fits = |key: &(u64, u64)| {
            let w = &host.mailbox[key];
            match host.limits.iter().position(|l| l.method == w.spec.method) {
                None => busy < concurrency,
                Some(i) => {
                    let l = &host.limits[i];
                    let ok = if mailbox(l) { admitted_set.contains_key(key) } else { room(l, held[i], w) };
                    ok && (!serial || busy < concurrency)
                }
            }
        };
        let next = if serial {
            // A mailbox call is ready from when it got room, so another
            // handle's call goes behind at most max_calls of the method's
            // calls, however many more of them wait for room.
            let since = |key: &(u64, u64)| admitted_set.get(key).copied().unwrap_or(host.mailbox[key].ready_at);
            heads(host).into_iter().filter(fits).min_by_key(since)
        } else {
            admitted.first().map(|(key, _)| *key).or_else(|| heads(host).into_iter().find(fits))
        };
        let Some(key @ (handle, seq)) = next else { return false };
        let w = host.mailbox.remove(&key).expect("a queued call");
        host.waiting.remove(key, &w.spec.method);
        // A call that starts out of its handle's order is skipped when the
        // order reaches it.
        let at = host.next_seq.get(&handle).copied().unwrap_or(0);
        if at == seq {
            host.known_handles.insert(handle);
        }
        if at <= seq {
            host.done_seqs.entry(handle).or_default().insert(seq);
        }
        advance(host, handle);
        host.inflight.insert(key);
        self.start(exec, vec![w]);
        true
    }

    /// Fail a call its method's mailbox turned away. It never runs, so its
    /// handle's later calls need not wait for it, wherever it stood.
    fn turn_away(&mut self, actor: Id, (handle, seq): (u64, u64), why: String) {
        let Some(host) = self.actors.get_mut(&actor) else { return };
        let Some(w) = host.mailbox.remove(&(handle, seq)) else { return };
        host.waiting.remove((handle, seq), &w.spec.method);
        if host.next_seq.get(&handle).copied().unwrap_or(0) <= seq {
            host.done_seqs.entry(handle).or_default().insert(seq);
        }
        advance(host, handle);
        self.overloaded(&w, why);
    }

    /// Why a call queued at its actor waits, when its method's limit holds
    /// it back (explain).
    pub(super) fn limit_reason(&self, host: &ActorHost, w: &Work) -> Option<String> {
        let i = host.limits.iter().position(|l| l.method == w.spec.method)?;
        let (l, h) = (&host.limits[i], self.held(host).0[i]);
        if mailbox(l) {
            let key = (w.spec.handle, w.spec.seq);
            if host.waiting.limits != host.limits || host.waiting.has_room(i, &host.mailbox, mailbox_room(l, h), key) {
                return None;
            }
            return Some(format!(
                "waiting for room in its mailbox: its actor has {} {} calls (its max_calls); this call waits at most \
                 {:.1} s",
                l.max_calls,
                l.method,
                l.max_wait_ms as f64 / 1e3
            ));
        }
        if room(l, h, w) {
            return None;
        }
        let limit = match (l.max_calls, l.max_bytes) {
            (0, b) => format!("max_bytes {:.1} MiB", mib(b)),
            (n, 0) => format!("max_calls {n}"),
            (n, b) => format!("max_calls {n}, max_bytes {:.1} MiB", mib(b)),
        };
        Some(format!(
            "waiting for room: its actor's process holds {} {} calls with {:.1} MiB of arguments ({limit})",
            h.calls,
            l.method,
            mib(h.bytes)
        ))
    }

    /// Fail a call its method's mailbox turned away. It never started.
    fn overloaded(&mut self, w: &Work, message: String) {
        let err = pb::ErrorRecord {
            kind: pb::error_record::Kind::Overloaded as i32,
            type_name: "chimaera.Overloaded".into(),
            message,
            attempt: w.attempt,
            ..Default::default()
        };
        self.pending_publish.entry(w.tk.v()).or_default().push(pb::PublishEntry {
            task: Some(w.tk.pb()),
            attempt: w.attempt,
            peer: Some(self.router.identity.pb()),
            outcome: Some(pb::Outcome { value: Some(pb::outcome::Value::Err(err)), ..Default::default() }),
            forward_to: None,
        });
    }

    /// Housekeeping: actors whose calls may be waiting for room in a
    /// mailbox, so those that waited their limit out fail now. Every
    /// actor's mailbox order is emptied once its mailbox is.
    pub(super) fn pump_limited_actors(&mut self) {
        let mut waiting = Vec::new();
        for (actor, h) in self.actors.iter_mut() {
            h.waiting.sync(&h.limits, &h.mailbox);
            if h.limits.iter().any(|l| l.max_wait_ms > 0) && !h.mailbox.is_empty() {
                waiting.push(*actor);
            }
        }
        for actor in waiting {
            self.actor_pump(actor);
        }
    }
}

/// The call each handle would start next, if it is here: one lookup per
/// handle rather than a pass over every queued call.
fn heads(host: &ActorHost) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(&(handle, _)) = host.mailbox.range((from, 0)..).next().map(|(k, _)| k) {
        let next = (handle, host.next_seq.get(&handle).copied().unwrap_or(0));
        if host.mailbox.contains_key(&next) {
            out.push(next);
        }
        let Some(after) = handle.checked_add(1) else { break };
        from = after;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(method: &str, handle: u64, seq: u64, ready_at: Instant) -> Work {
        let spec = pb::TaskSpec { method: method.into(), handle, seq, ..Default::default() };
        let mut w = Work::new(Tk { run: Id::default(), task: Id::default() }, 1, spec, pb::Policy::default(), vec![]);
        w.ready_at = ready_at;
        w
    }

    #[test]
    fn a_mailbox_keeps_its_calls_in_order_as_they_come_and_go() {
        let t0 = Instant::now();
        let at = |ms| t0 + Duration::from_millis(ms);
        let slow = pb::MethodLimit { method: "slow".into(), max_calls: 2, max_wait_ms: 1000, ..Default::default() };
        let mut host = ActorHost { limits: vec![slow.clone()], ..Default::default() };
        // Handle 1's calls 0 and 1 come after its call 2: they take room no later than it.
        enqueue(&mut host, call("slow", 1, 2, at(10)));
        enqueue(&mut host, call("slow", 2, 0, at(20)));
        enqueue(&mut host, call("fast", 1, 3, at(25))); // no mailbox: not in its order
        enqueue(&mut host, call("slow", 1, 0, at(30)));
        enqueue(&mut host, call("slow", 1, 1, at(40)));
        let order = |host: &mut ActorHost, room, reject, cutoff| {
            let (ok, away) = host.waiting.front(0, &host.mailbox, room, reject, cutoff, at(100));
            (ok.into_iter().map(|(key, _)| key).collect::<Keys>(), away)
        };
        // Past the room: those that came before the cutoff, and nothing after a place past it.
        assert_eq!(order(&mut host, 2, false, Some(at(15))), (vec![(1, 0), (1, 1)], vec![(1, 2)]));
        assert_eq!(order(&mut host, 2, false, Some(at(5))), (vec![(1, 0), (1, 1)], vec![]));
        assert_eq!(order(&mut host, 1, true, None), (vec![(1, 0)], vec![(1, 1), (1, 2), (2, 0)]));
        // A call keeps the time it first had room.
        let (ok, _) = host.waiting.front(0, &host.mailbox, 3, false, None, at(200));
        assert_eq!(ok, [((1, 0), at(100)), ((1, 1), at(100)), ((1, 2), at(200))]);
        // A call that came back with an earlier time moves its handle's earlier calls up with it.
        enqueue(&mut host, call("slow", 3, 0, at(50)));
        enqueue(&mut host, call("slow", 3, 1, at(1)));
        assert_eq!(order(&mut host, 2, false, None).0, [(3, 0), (3, 1)]);
        // Calls that left the mailbox another way are dropped where they are met.
        host.mailbox.remove(&(3, 0));
        host.waiting.remove((3, 1), "slow");
        host.mailbox.remove(&(3, 1));
        host.mailbox.remove(&(1, 0));
        assert_eq!(order(&mut host, 2, false, None).0, [(1, 1), (1, 2)]);
        assert_eq!(host.waiting.places.values().map(BTreeMap::len).sum::<usize>(), 3);
        assert_eq!(host.waiting.room_since.keys().collect::<BTreeSet<_>>(), [(1, 1), (1, 2)].iter().collect());
        // New limits rebuild it from the mailbox; an empty mailbox empties it.
        host.limits = vec![pb::MethodLimit { max_calls: 1, ..slow }];
        host.waiting.sync(&host.limits, &host.mailbox);
        assert_eq!(order(&mut host, 1, true, None), (vec![(1, 1)], vec![(1, 2), (2, 0)]));
        host.mailbox.clear();
        host.waiting.sync(&host.limits, &host.mailbox);
        assert!(
            host.waiting.places.is_empty() && host.waiting.order[0].is_empty() && host.waiting.room_since.is_empty()
        );
    }
}
