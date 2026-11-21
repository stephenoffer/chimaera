//! SWIM membership: who is alive, without every peer pinging every other.
//!
//! Each protocol period a peer probes one member, in a shuffled round-robin
//! order. A member that does not answer is probed again through `indirect`
//! other members; if none of them reaches it, it becomes *suspect*. Suspicion
//! spreads by gossip, and a suspect that hears of it refutes by raising its
//! incarnation. A suspect that does not refute within the suspicion timeout
//! (longer in larger clusters, as in Lifeguard) is declared *dead*.
//!
//! Membership changes ride on every probe and reply, each retransmitted about
//! `retransmit × log2(n)` times, so a change reaches all members in
//! O(log n) periods while each peer sends O(1) messages per period.
//!
//! Pure: no clock, no randomness source, no IO. The caller passes the time,
//! performs the probes and reports their results.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum State {
    Alive,
    Suspect,
    Dead,
}

/// A membership claim: `node` is in `state` at incarnation `inc`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Update {
    pub node: u64,
    pub inc: u64,
    pub state: State,
}

impl Update {
    /// Whether this claim replaces what is known (`inc`, `state`) about the node.
    fn overrides(&self, inc: u64, state: State) -> bool {
        match (self.state, state) {
            // Dead is final for an incarnation; a restarted peer comes back
            // with a higher one (callers put the process incarnation in the
            // high bits).
            (State::Alive, State::Dead) => self.inc > inc,
            (_, State::Dead) => false,
            (State::Dead, _) => self.inc >= inc,
            (State::Alive, State::Alive | State::Suspect) => self.inc > inc,
            (State::Suspect, State::Alive) => self.inc >= inc,
            (State::Suspect, State::Suspect) => self.inc > inc,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub period_ms: u64,
    /// Relays asked to probe a member that missed a direct probe.
    pub indirect: usize,
    /// Suspicion lasts `suspicion × max(1, log10(n)) × period` before death.
    pub suspicion: u64,
    /// Each update is sent `retransmit × ceil(log2(n + 1))` times.
    pub retransmit: u32,
    /// Updates carried by one message.
    pub piggyback: usize,
    /// Suspicion never runs out sooner than this: a peer that is alive but
    /// stalled (paging, a long collection, a saturated disk) is not dead.
    /// A process known to be gone is declared dead with `fail` instead.
    pub suspicion_floor_ms: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config { period_ms: 1000, indirect: 3, suspicion: 4, retransmit: 4, piggyback: 8, suspicion_floor_ms: 0 }
    }
}

/// What is known about one member.
#[derive(Clone, Copy, Debug)]
struct Member {
    inc: u64,
    /// When the state last changed: centiseconds, wrapping at 2^30 (about
    /// 124 days). Only spans far shorter than that are ever measured.
    since_cs: u32,
    state: State,
}

const SINCE_MASK: u32 = (1 << 30) - 1;

impl Member {
    fn new(inc: u64, state: State, now_ms: u64) -> Member {
        Member { inc, state, since_cs: ((now_ms / 10) as u32) & SINCE_MASK }
    }

    /// Time in this state.
    fn elapsed_ms(&self, now_ms: u64) -> u64 {
        u64::from(((now_ms / 10) as u32).wrapping_sub(self.since_cs) & SINCE_MASK) * 10
    }
}

/// One table row in 20 bytes (node, incarnation, and state packed with its
/// time), so a 512-peer cell's table fits the 16 KiB per-peer budget.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct Entry {
    node: u64,
    inc: u64,
    meta: u32,
}

impl Entry {
    fn new(node: u64, m: Member) -> Entry {
        let state = match m.state {
            State::Alive => 0,
            State::Suspect => 1,
            State::Dead => 2,
        };
        Entry { node, inc: m.inc, meta: (m.since_cs << 2) | state }
    }

    fn member(self) -> Member {
        let state = match self.meta & 3 {
            0 => State::Alive,
            1 => State::Suspect,
            _ => State::Dead,
        };
        Member { inc: self.inc, since_cs: self.meta >> 2, state }
    }
}

/// Grow a vector by an eighth rather than doubling: these tables live as
/// long as the peer, and a doubled capacity would be mostly empty.
fn grow<T>(v: &mut Vec<T>) {
    if v.len() == v.capacity() {
        v.reserve_exact((v.len() / 8).max(8));
    }
}

/// Members sorted by node id (binary search): no tree-node overhead.
#[derive(Default)]
struct Table(Vec<Entry>);

impl Table {
    fn pos(&self, node: u64) -> Result<usize, usize> {
        self.0.binary_search_by_key(&node, |e| e.node)
    }
    fn get(&self, node: &u64) -> Option<Member> {
        self.pos(*node).ok().map(|i| self.0[i].member())
    }
    /// The member at a position: (node, member).
    fn at(&self, i: usize) -> Option<(u64, Member)> {
        self.0.get(i).map(|e| (e.node, e.member()))
    }
    fn contains_key(&self, node: &u64) -> bool {
        self.pos(*node).is_ok()
    }
    /// Insert or replace; returns the position of a newly inserted member.
    fn insert(&mut self, node: u64, m: Member) -> Option<usize> {
        match self.pos(node) {
            Ok(i) => {
                self.0[i] = Entry::new(node, m);
                None
            }
            Err(i) => {
                grow(&mut self.0);
                self.0.insert(i, Entry::new(node, m));
                Some(i)
            }
        }
    }
    fn iter(&self) -> impl Iterator<Item = (u64, Member)> + '_ {
        self.0.iter().map(|e| (e.node, e.member()))
    }
    fn values(&self) -> impl Iterator<Item = Member> + '_ {
        self.0.iter().map(|e| e.member())
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A state change the host acts on (retire a dead peer, stop placing on a suspect).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub node: u64,
    pub inc: u64,
    pub state: State,
}

pub struct Swim {
    me: u64,
    inc: u64,
    cfg: Config,
    members: Table,
    /// Probe order for the current round: positions in `members`.
    order: Vec<u32>,
    pos: usize,
    rng: u64,
    next_probe_ms: u64,
    gossip: Vec<(Update, u32)>,
    changes: Vec<Change>,
}

impl Swim {
    pub fn new(me: u64, inc: u64, seed: u64, cfg: Config) -> Swim {
        // A new instance announces itself: members that saw an earlier
        // incarnation of this peer as dead learn it is back.
        Swim {
            me,
            inc,
            cfg,
            members: Table::default(),
            order: Vec::new(),
            pos: 0,
            rng: seed | 1,
            next_probe_ms: 0,
            gossip: vec![(Update { node: me, inc, state: State::Alive }, cfg.retransmit * 4)],
            changes: Vec::new(),
        }
    }

    pub fn me(&self) -> u64 {
        self.me
    }

    pub fn incarnation(&self) -> u64 {
        self.inc
    }

    pub fn config(&self) -> Config {
        self.cfg
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64*: deterministic for a seed, good enough to spread probes.
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Members other than this one, alive or suspect.
    pub fn live(&self) -> impl Iterator<Item = u64> + '_ {
        self.members.iter().filter(|(_, m)| m.state != State::Dead).map(|(n, _)| n)
    }

    pub fn state_of(&self, node: u64) -> Option<(u64, State)> {
        self.members.get(&node).map(|m| (m.inc, m.state))
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn transmits(&self) -> u32 {
        let n = self.members.len() as u64 + 2;
        self.cfg.retransmit * (64 - n.leading_zeros())
    }

    fn suspicion_ms(&self) -> u64 {
        let n = (self.members.len() as f64 + 1.0).max(10.0);
        ((self.cfg.suspicion as f64 * n.log10() * self.cfg.period_ms as f64) as u64).max(self.cfg.suspicion_floor_ms)
    }

    fn spread(&mut self, u: Update) {
        self.gossip.retain(|(g, _)| g.node != u.node);
        let t = self.transmits();
        self.gossip.push((u, t));
    }

    /// Insert a member, keeping the probe order's positions valid.
    fn insert(&mut self, node: u64, m: Member) {
        if let Some(at) = self.members.insert(node, m) {
            for p in &mut self.order {
                if *p as usize >= at {
                    *p += 1;
                }
            }
        }
    }

    fn set(&mut self, now_ms: u64, u: Update) {
        let old = self.members.get(&u.node);
        let changed = old.is_none_or(|m| m.state != u.state || m.inc != u.inc);
        let mut m = Member::new(u.inc, u.state, now_ms);
        if let Some(o) = old.filter(|_| !changed) {
            m.since_cs = o.since_cs;
        }
        self.insert(u.node, m);
        if changed {
            self.changes.push(Change { node: u.node, inc: u.inc, state: u.state });
        }
        self.spread(u);
    }

    /// Add a member known from elsewhere (the cluster plan, a join). Not
    /// gossiped: whoever told this peer tells others too, and a joining peer
    /// announces itself. Gossip carries news only (members first heard of
    /// through gossip, and state changes); a cell forming from its plan would
    /// otherwise queue every member, `transmits()` times over, ahead of it.
    pub fn add(&mut self, now_ms: u64, node: u64, inc: u64) {
        if node == self.me || self.members.contains_key(&node) {
            return;
        }
        self.insert(node, Member::new(inc, State::Alive, now_ms));
        // Insert at a random position so new members are probed soon.
        let idx = self.members.pos(node).expect("just inserted") as u32;
        let at = if self.order.is_empty() { 0 } else { (self.next_rand() as usize) % (self.order.len() + 1) };
        grow(&mut self.order);
        self.order.insert(at, idx);
    }

    /// Fold in claims carried by a message. A claim that this peer is
    /// suspect or dead is refuted with a higher incarnation.
    pub fn merge(&mut self, now_ms: u64, updates: &[Update]) {
        for u in updates {
            if u.node == self.me {
                if u.state != State::Alive && u.inc >= self.inc {
                    self.inc = u.inc + 1;
                    let me = Update { node: self.me, inc: self.inc, state: State::Alive };
                    self.spread(me);
                }
                continue;
            }
            match self.members.get(&u.node) {
                None => {
                    if u.state != State::Dead {
                        self.add(now_ms, u.node, u.inc);
                        // News to this peer: pass it on.
                        self.spread(Update { node: u.node, inc: u.inc, state: State::Alive });
                        if u.state == State::Suspect {
                            self.set(now_ms, *u);
                        }
                    } else {
                        self.insert(u.node, Member::new(u.inc, State::Dead, now_ms));
                    }
                }
                Some(m) => {
                    if u.overrides(m.inc, m.state) {
                        let returning = m.state == State::Dead && u.state == State::Alive;
                        self.set(now_ms, *u);
                        let idx = self.members.pos(u.node).expect("known") as u32;
                        if returning && !self.order.contains(&idx) {
                            grow(&mut self.order);
                            self.order.push(idx);
                        }
                    }
                }
            }
        }
    }

    /// Updates to carry on the next message (each send counts toward its retransmits).
    pub fn outgoing(&mut self) -> Vec<Update> {
        self.gossip.sort_by_key(|(_, left)| std::cmp::Reverse(*left));
        let n = self.cfg.piggyback.min(self.gossip.len());
        let out: Vec<Update> = self.gossip[..n].iter().map(|(u, _)| *u).collect();
        for g in &mut self.gossip[..n] {
            g.1 = g.1.saturating_sub(1);
        }
        self.gossip.retain(|(_, left)| *left > 0);
        // A burst (a cell forming, a mass failure) fills the queue for a
        // while; give the space back once it drains.
        if self.gossip.capacity() > 4 * self.gossip.len().max(16) {
            self.gossip.shrink_to(2 * self.gossip.len());
        }
        out
    }

    /// Up to `k` distinct members to probe now, if a period has started.
    /// More than one per period keeps small clusters' views fresh; the
    /// per-peer message rate stays bounded by `k` as the cluster grows.
    pub fn next_probes(&mut self, now_ms: u64, k: usize) -> Vec<u64> {
        if now_ms < self.next_probe_ms {
            return Vec::new();
        }
        let live = self.members.values().filter(|m| m.state != State::Dead).count();
        let want = k.min(live);
        let mut out = Vec::with_capacity(want);
        // A reshuffle mid-batch can repeat a member: skip it, within bounds.
        for _ in 0..2 * live + 1 {
            if out.len() >= want {
                break;
            }
            self.next_probe_ms = 0;
            match self.next_probe(now_ms) {
                Some(n) if !out.contains(&n) => out.push(n),
                Some(_) => {}
                None => break,
            }
        }
        self.next_probe_ms = now_ms + self.cfg.period_ms;
        out
    }

    /// The member to probe now, if a period has started.
    pub fn next_probe(&mut self, now_ms: u64) -> Option<u64> {
        if now_ms < self.next_probe_ms {
            return None;
        }
        self.next_probe_ms = now_ms + self.cfg.period_ms;
        let live: usize = self.members.values().filter(|m| m.state != State::Dead).count();
        if live == 0 {
            return None;
        }
        loop {
            if self.pos >= self.order.len() {
                // A fresh shuffle each round: every member is probed once per round.
                let members = &self.members;
                self.order.retain(|i| members.at(*i as usize).is_some_and(|(_, m)| m.state != State::Dead));
                for i in (1..self.order.len()).rev() {
                    let j = (self.next_rand() as usize) % (i + 1);
                    self.order.swap(i, j);
                }
                self.pos = 0;
                if self.order.is_empty() {
                    return None;
                }
            }
            let (n, m) = self.members.at(self.order[self.pos] as usize).expect("probe order holds valid positions");
            self.pos += 1;
            if m.state != State::Dead {
                return Some(n);
            }
        }
    }

    /// Relays for an indirect probe of `target` after a missed direct probe.
    pub fn relays(&mut self, target: u64) -> Vec<u64> {
        let candidates: Vec<u64> =
            self.members.iter().filter(|(n, m)| *n != target && m.state == State::Alive).map(|(n, _)| n).collect();
        let mut out = Vec::new();
        let mut pool = candidates;
        while out.len() < self.cfg.indirect && !pool.is_empty() {
            let i = (self.next_rand() as usize) % pool.len();
            out.push(pool.swap_remove(i));
        }
        out
    }

    /// Outcome of probing `target` (directly, or through relays): reached or not.
    pub fn probed(&mut self, now_ms: u64, target: u64, reached: bool) {
        let Some(m) = self.members.get(&target) else { return };
        if reached {
            if m.state == State::Suspect {
                // Reached again: alive at the same incarnation is not newer, so
                // only the member's own refutation clears it elsewhere; locally
                // a direct answer is proof enough.
                self.insert(target, Member::new(m.inc, State::Alive, now_ms));
                self.changes.push(Change { node: target, inc: m.inc, state: State::Alive });
            }
            return;
        }
        if m.state == State::Alive {
            self.set(now_ms, Update { node: target, inc: m.inc, state: State::Suspect });
        }
    }

    /// Declare `node` dead now, without waiting out suspicion: the caller
    /// knows its process is gone (its port refuses connections).
    pub fn fail(&mut self, now_ms: u64, node: u64) {
        if let Some(m) = self.members.get(&node)
            && m.state != State::Dead
        {
            self.set(now_ms, Update { node, inc: m.inc, state: State::Dead });
        }
    }

    /// Declare suspects dead once their suspicion has lasted long enough.
    pub fn expire(&mut self, now_ms: u64) {
        let timeout = self.suspicion_ms();
        let due: Vec<Update> = self
            .members
            .iter()
            .filter(|(_, m)| m.state == State::Suspect && m.elapsed_ms(now_ms) >= timeout)
            .map(|(n, m)| Update { node: n, inc: m.inc, state: State::Dead })
            .collect();
        for u in due {
            self.set(now_ms, u);
        }
    }

    /// Approximate heap and inline bytes this instance holds (for the
    /// per-peer state budget in fleet emulation).
    pub fn approx_bytes(&self) -> usize {
        std::mem::size_of::<Swim>()
            + self.members.0.capacity() * std::mem::size_of::<Entry>()
            + self.order.capacity() * std::mem::size_of::<u32>()
            + self.gossip.capacity() * std::mem::size_of::<(Update, u32)>()
            + self.changes.capacity() * std::mem::size_of::<Change>()
    }

    /// State changes since the last call.
    pub fn take_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A lossless in-memory network of SWIM peers, some of them down.
    struct Net {
        peers: BTreeMap<u64, Swim>,
        down: std::collections::BTreeSet<u64>,
        now: u64,
        msgs: u64,
    }

    impl Net {
        fn new(n: u64, cfg: Config) -> Net {
            let mut peers = BTreeMap::new();
            for i in 1..=n {
                let mut s = Swim::new(i, 1, i * 7919, cfg);
                for j in 1..=n {
                    s.add(0, j, 1);
                }
                peers.insert(i, s);
            }
            Net { peers, down: Default::default(), now: 0, msgs: 0 }
        }

        /// One probe exchange between `a` and `b` (both alive), gossip both ways.
        fn exchange(&mut self, a: u64, b: u64) -> bool {
            if self.down.contains(&b) {
                return false;
            }
            self.msgs += 2;
            let out_a = self.peers.get_mut(&a).unwrap().outgoing();
            self.peers.get_mut(&b).unwrap().merge(self.now, &out_a);
            let out_b = self.peers.get_mut(&b).unwrap().outgoing();
            self.peers.get_mut(&a).unwrap().merge(self.now, &out_b);
            true
        }

        fn step(&mut self, dt: u64) {
            self.now += dt;
            let ids: Vec<u64> = self.peers.keys().copied().filter(|i| !self.down.contains(i)).collect();
            for a in ids {
                let Some(t) = self.peers.get_mut(&a).unwrap().next_probe(self.now) else { continue };
                let mut reached = self.exchange(a, t);
                if !reached {
                    let relays = self.peers.get_mut(&a).unwrap().relays(t);
                    for r in relays {
                        if self.exchange(a, r) && self.exchange(r, t) {
                            reached = true;
                        }
                    }
                }
                let now = self.now;
                let p = self.peers.get_mut(&a).unwrap();
                p.probed(now, t, reached);
                p.expire(now);
            }
        }

        fn everyone_sees(&self, node: u64, state: State) -> bool {
            self.peers
                .iter()
                .filter(|(i, _)| !self.down.contains(i) && **i != node)
                .all(|(_, p)| p.state_of(node).is_some_and(|(_, s)| s == state))
        }
    }

    #[test]
    fn a_dead_member_is_detected_by_everyone_and_nobody_else_is() {
        let cfg = Config { period_ms: 100, ..Config::default() };
        let mut net = Net::new(64, cfg);
        net.down.insert(17);
        let mut steps = 0;
        while !net.everyone_sees(17, State::Dead) {
            net.step(25);
            steps += 1;
            assert!(steps < 2000, "the dead member was not declared dead everywhere");
        }
        for (i, p) in &net.peers {
            if *i == 17 {
                continue;
            }
            for (n, (_, s)) in p.members.iter().map(|(n, m)| (n, (m.inc, m.state))) {
                assert!(n == 17 || s == State::Alive, "{i} wrongly thinks {n} is {s:?}");
            }
        }
    }

    #[test]
    fn messages_per_peer_per_period_stay_constant_as_the_cluster_grows() {
        let cfg = Config { period_ms: 100, ..Config::default() };
        let mut rates = Vec::new();
        for n in [16, 64, 256] {
            let mut net = Net::new(n, cfg);
            for _ in 0..40 {
                net.step(100);
            }
            rates.push(net.msgs as f64 / (n as f64 * 40.0));
        }
        assert!(rates.iter().all(|r| *r <= 2.5), "{rates:?}");
    }

    #[test]
    fn a_suspected_member_refutes_with_a_higher_incarnation() {
        let mut a = Swim::new(1, 1, 3, Config::default());
        a.merge(0, &[Update { node: 1, inc: 1, state: State::Suspect }]);
        assert_eq!(a.incarnation(), 2);
        assert!(a.outgoing().contains(&Update { node: 1, inc: 2, state: State::Alive }));
        let mut b = Swim::new(2, 1, 5, Config::default());
        b.add(0, 1, 1);
        b.merge(0, &[Update { node: 1, inc: 1, state: State::Suspect }]);
        assert_eq!(b.state_of(1), Some((1, State::Suspect)));
        b.merge(1, &[Update { node: 1, inc: 2, state: State::Alive }]);
        assert_eq!(b.state_of(1), Some((2, State::Alive)), "the refutation clears the suspicion");
    }

    #[test]
    fn dead_is_final_for_an_incarnation_but_a_restart_rejoins() {
        let mut b = Swim::new(2, 1, 5, Config::default());
        b.add(0, 1, 1);
        b.merge(0, &[Update { node: 1, inc: 1, state: State::Dead }]);
        b.merge(1, &[Update { node: 1, inc: 1, state: State::Alive }]);
        assert_eq!(b.state_of(1), Some((1, State::Dead)), "no resurrection at the same incarnation");
        b.merge(2, &[Update { node: 1, inc: 2 << 32, state: State::Alive }]);
        assert_eq!(b.state_of(1), Some((2 << 32, State::Alive)), "the restarted process rejoins");
        assert!(b.next_probes(10, 8).contains(&1), "and is probed again");
    }

    #[test]
    fn several_probes_per_period_cover_a_small_cluster_each_period() {
        let mut a = Swim::new(1, 1, 3, Config { period_ms: 100, ..Config::default() });
        for n in 2..=5 {
            a.add(0, n, 1);
        }
        let mut first = a.next_probes(0, 8);
        first.sort();
        assert_eq!(first, vec![2, 3, 4, 5]);
        assert!(a.next_probes(50, 8).is_empty(), "one batch per period");
        assert_eq!(a.next_probes(100, 8).len(), 4);
    }

    #[test]
    fn a_paused_member_that_returns_in_time_is_not_declared_dead() {
        let cfg = Config { period_ms: 100, suspicion: 6, ..Config::default() };
        let mut net = Net::new(32, cfg);
        net.down.insert(5);
        for _ in 0..8 {
            net.step(25); // 200 ms: some peers suspect it, none can have expired it yet
        }
        net.down.remove(&5);
        for _ in 0..400 {
            net.step(25);
        }
        assert!(net.everyone_sees(5, State::Alive), "the returning member refuted the suspicion everywhere");
    }

    #[test]
    fn a_row_packs_into_20_bytes_and_round_trips() {
        assert_eq!(std::mem::size_of::<Entry>(), 20);
        for state in [State::Alive, State::Suspect, State::Dead] {
            let m = Member::new(7 << 32 | 3, state, 123_456_789);
            let back = Entry::new(42, m).member();
            assert_eq!((back.inc, back.state, back.since_cs), (m.inc, m.state, m.since_cs));
        }
    }

    #[test]
    fn suspicion_time_survives_the_clock_wrapping() {
        // 2^30 centiseconds after start, the stored time wraps to zero.
        let wrap_ms = u64::from(SINCE_MASK + 1) * 10;
        let m = Member::new(1, State::Suspect, wrap_ms - 500);
        assert_eq!(m.elapsed_ms(wrap_ms + 1_500), 2_000);
        let mut a = Swim::new(1, 1, 3, Config { period_ms: 100, ..Config::default() });
        a.add(wrap_ms - 1_000, 2, 1);
        a.probed(wrap_ms - 1_000, 2, false);
        a.expire(wrap_ms - 900);
        assert_eq!(a.state_of(2), Some((1, State::Suspect)), "not dead 100 ms in");
        a.expire(wrap_ms + 60_000);
        assert_eq!(a.state_of(2), Some((1, State::Dead)), "dead once suspicion has lasted, across the wrap");
    }

    #[test]
    fn a_full_cell_fits_the_per_peer_budget_once_gossip_drains() {
        let mut a = Swim::new(0, 1, 9, Config::default());
        for n in 1..=512 {
            a.add(0, n, 1);
        }
        while !a.outgoing().is_empty() {}
        assert!(a.approx_bytes() <= 16 * 1024, "{} bytes for 512 members", a.approx_bytes());
    }

    #[test]
    fn a_cell_formed_from_its_plan_gossips_only_news() {
        let mut a = Swim::new(1, 1, 3, Config::default());
        for n in 2..=512 {
            a.add(0, n, 1);
        }
        // Only its own announcement: every member already has the plan.
        let first = a.outgoing();
        assert_eq!(first.iter().map(|u| u.node).collect::<Vec<_>>(), vec![1]);
        // A member first heard of through gossip is news, and is passed on.
        a.merge(10, &[Update { node: 900, inc: 1, state: State::Alive }]);
        assert!(a.outgoing().iter().any(|u| u.node == 900 && u.state == State::Alive));
    }

    #[test]
    fn a_stalled_member_outlives_the_floor_and_a_gone_one_fails_at_once() {
        let cfg = Config { period_ms: 300, suspicion_floor_ms: 6_000, ..Config::default() };
        let mut a = Swim::new(1, 1, 3, cfg);
        a.add(0, 2, 1);
        a.add(0, 3, 1);
        a.probed(0, 2, false);
        a.expire(5_000);
        assert_eq!(a.state_of(2), Some((1, State::Suspect)), "5 s of silence is a stall, not a death");
        a.expire(6_000);
        assert_eq!(a.state_of(2), Some((1, State::Dead)));
        a.fail(6_100, 3);
        assert_eq!(a.state_of(3), Some((1, State::Dead)), "refused connections: dead now");
        assert!(a.outgoing().iter().any(|u| u.node == 3 && u.state == State::Dead), "and everyone is told");
    }
}
