//! What a peer sends a session's reader (a driver, or another peer watching
//! outcomes here), bounded so that a reader that stops reading cannot grow
//! this peer's memory.
//!
//! A session's mailbox holds at most MAX_EVENTS events and MAX_BYTES bytes.
//! Outcomes and replies may fill all of it; output and generator items stop a
//! quarter short, so they never take the room a call's outcome needs. What
//! does not fit waits in the session in compact form: an outcome as its task
//! key (read again from its shard once the reader catches up), a generator
//! item as itself (a generator runs at most its credit window ahead of its
//! reader), and output as a count of lines (the run's log keeps every line:
//! `chimaera logs`). A reply (an ack) waits for room, and the session reads
//! no further requests from its client meanwhile.
//!
//! A client that acknowledges cursors can resume its session on a new stream
//! after the old one broke. Outcomes, generator items and replies carry a
//! cursor, and the session keeps those not yet acknowledged; a resume sends
//! again the ones after the last cursor the client received. Events also
//! carry `taken`, the seq of the last request the session has handled: the
//! client keeps what it sent after that, and sends it again once the resume
//! says where the old session stopped. Each request is handled once, and
//! each reply and outcome arrives once.

use chimaera_core::ids::Id;
use chimaera_core::pb;
use chimaera_core::shard::Tk;
use pb::session_event::Event;
use prost::Message;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub const MAX_EVENTS: usize = 4096;
pub const MAX_BYTES: usize = 16 << 20;
/// Kept for outcomes and replies: output and generator items stop short of it.
const RESERVE_EVENTS: usize = MAX_EVENTS / 4;
const RESERVE_BYTES: usize = MAX_BYTES / 4;
/// Outcomes read again from their shards each time the reader makes room.
const REFILL: usize = MAX_EVENTS / 4;
/// Sent and not yet acknowledged, kept for a resume; beyond these the oldest
/// are forgotten (a client that stopped acknowledging cannot resume from them).
const UNACKED_EVENTS: usize = 4 * MAX_EVENTS;
const UNACKED_BYTES: usize = 2 * MAX_BYTES;
/// How long a session whose stream ended waits for its client to resume it
/// before it ends (and its run, if no other session holds it, is cancelled).
pub const RESUME_WINDOW: Duration = Duration::from_secs(5);

/// What an event is: the room it may take, and whether a resume sends it again.
#[derive(Clone, Copy)]
pub enum Kind {
    /// A call's outcome, kept for a resume as its task key.
    Outcome(Tk),
    /// A reply to one of the client's requests.
    Reply,
    /// A generator's item; it leaves the reserve free.
    Item,
    /// Output; it leaves the reserve free and is never sent again.
    Log,
    /// About the stream itself (its session's id, a resume's answer): never sent again.
    Notice,
}

impl Kind {
    fn progress(self) -> bool {
        matches!(self, Kind::Item | Kind::Log)
    }
}

/// An event sent with a cursor, kept until acknowledged.
enum Sent {
    Outcome(Tk),
    Event(Box<pb::SessionEvent>),
}

#[derive(Default)]
struct Queue {
    events: VecDeque<(pb::SessionEvent, usize)>,
    bytes: usize,
    closed: bool,
    /// Something was refused since the reader last made room for it.
    refused: bool,
    /// Its client acknowledges cursors: what a resume needs is kept.
    resumable: bool,
    cursor: u64,
    unacked: VecDeque<(u64, Sent, usize)>,
    unacked_bytes: usize,
    /// The last cursor forgotten unacknowledged: no resume from before it.
    forgotten: u64,
}

impl Queue {
    fn fits(&self, size: usize, progress: bool) -> bool {
        let (events, bytes) =
            if progress { (MAX_EVENTS - RESERVE_EVENTS, MAX_BYTES - RESERVE_BYTES) } else { (MAX_EVENTS, MAX_BYTES) };
        // One event larger than the mailbox still goes, alone.
        self.events.is_empty() || (self.events.len() < events && self.bytes + size <= bytes)
    }

    /// Give `ev` the next cursor and keep it until the client acknowledges it.
    fn keep(&mut self, ev: &mut pb::SessionEvent, kind: Kind) {
        if !self.resumable {
            return;
        }
        let (sent, size) = match kind {
            Kind::Outcome(tk) => (Sent::Outcome(tk), size_of::<Tk>()),
            Kind::Reply | Kind::Item => (Sent::Event(Box::new(ev.clone())), ev.encoded_len()),
            Kind::Log | Kind::Notice => return,
        };
        self.cursor += 1;
        ev.cursor = self.cursor;
        self.unacked.push_back((self.cursor, sent, size));
        self.unacked_bytes += size;
        while self.unacked.len() > UNACKED_EVENTS || self.unacked_bytes > UNACKED_BYTES {
            let (cursor, _, size) = self.unacked.pop_front().expect("over the limit, so not empty");
            self.unacked_bytes -= size;
            self.forgotten = cursor;
        }
    }
}

#[derive(Default)]
pub struct Mailbox {
    q: Mutex<Queue>,
    readable: Notify,
    room: Notify,
    /// The seq of the last request from the client the session has handled.
    taken: AtomicU64,
    /// Told when a resume takes the session over while its stream is open.
    superseded: Notify,
}

impl Mailbox {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    fn push(&self, mut q: std::sync::MutexGuard<'_, Queue>, ev: pb::SessionEvent, size: usize) {
        q.bytes += size;
        q.events.push_back((ev, size));
        drop(q);
        self.readable.notify_one();
    }

    /// Queue `ev` if it fits (see Kind). A refused event comes back.
    pub fn offer(&self, mut ev: pb::SessionEvent, kind: Kind) -> Result<(), Box<pb::SessionEvent>> {
        let size = ev.encoded_len();
        let mut q = self.q.lock().expect("mailbox");
        if q.closed || !q.fits(size, kind.progress()) {
            q.refused = true;
            return Err(Box::new(ev));
        }
        q.keep(&mut ev, kind);
        self.push(q, ev, size);
        Ok(())
    }

    /// Queue a reply to the client's own request, waiting for room. Once the
    /// stream has ended, a resumable session keeps the reply for the resume
    /// (true), and any other drops it (false).
    pub async fn send(&self, mut ev: pb::SessionEvent) -> bool {
        let size = ev.encoded_len();
        loop {
            let room = self.room.notified();
            {
                let mut q = self.q.lock().expect("mailbox");
                if q.closed || q.fits(size, false) {
                    q.keep(&mut ev, Kind::Reply);
                    if q.closed {
                        return q.resumable;
                    }
                    self.push(q, ev, size);
                    return true;
                }
            }
            room.await;
        }
    }

    /// The client's request `seq` has been handled, and any reply to it sent.
    pub fn took(&self, seq: u64) {
        self.taken.store(seq, Ordering::Release);
    }

    /// The next event; None once closed.
    pub async fn recv(&self) -> Option<pb::SessionEvent> {
        loop {
            let readable = self.readable.notified();
            {
                let mut q = self.q.lock().expect("mailbox");
                if q.closed {
                    return None;
                }
                if let Some((mut ev, size)) = q.events.pop_front() {
                    q.bytes -= size;
                    if q.resumable {
                        ev.taken = self.taken.load(Ordering::Acquire);
                    }
                    drop(q);
                    self.room.notify_waiters();
                    return Some(ev);
                }
            }
            readable.await;
        }
    }

    /// The reader has gone: what is queued is dropped, and so is all that is
    /// offered from now on (what a resume needs is still kept).
    pub fn close(&self) {
        let mut q = self.q.lock().expect("mailbox");
        q.closed = true;
        q.events.clear();
        q.bytes = 0;
        drop(q);
        self.readable.notify_one();
        self.room.notify_waiters();
    }

    /// Whether what waits beyond the mailbox may be offered again: the reader
    /// has taken it down to half since something was refused (true once per
    /// refusal).
    pub fn wants_more(&self) -> bool {
        let mut q = self.q.lock().expect("mailbox");
        let more = q.refused && !q.closed && q.events.len() <= MAX_EVENTS / 2 && q.bytes <= MAX_BYTES / 2;
        if more {
            q.refused = false;
        }
        more
    }

    /// Something still waits beyond the mailbox: ask again once the reader has made room.
    fn owe(&self) {
        self.q.lock().expect("mailbox").refused = true;
    }

    pub fn queued_bytes(&self) -> usize {
        self.q.lock().expect("mailbox").bytes
    }

    /// A resume takes the session over while its stream is still open here
    /// (the client has moved on before this peer saw the stream break): the
    /// stream ends, and its requests stop being read (`superseded`).
    pub fn supersede(&self) {
        self.close();
        self.superseded.notify_one();
    }

    /// Resolves once `supersede` has been called.
    pub async fn superseded(&self) {
        self.superseded.notified().await
    }

    /// The client received everything up to `cursor`. The first
    /// acknowledgment makes the session resumable; it is made before the
    /// session handles the next request, so the reply to that is kept.
    pub fn ack(&self, cursor: u64) {
        let mut q = self.q.lock().expect("mailbox");
        q.resumable = true;
        while let Some(&(c, _, size)) = q.unacked.front()
            && c <= cursor
        {
            q.unacked_bytes -= size;
            q.unacked.pop_front();
        }
    }

    /// Take over `old`'s numbering for a client that received up to `cursor`.
    /// First on this stream goes how far `old` read the client's requests;
    /// returned is what `old` sent after `cursor`, to send again.
    fn adopt(&self, old: &Mailbox, cursor: u64) -> Vec<Sent> {
        let taken = old.taken.load(Ordering::Acquire);
        self.taken.store(taken, Ordering::Release);
        let mut o = old.q.lock().expect("mailbox");
        let again = std::mem::take(&mut o.unacked).into_iter().filter(|(c, ..)| *c > cursor).map(|(_, s, _)| s);
        let again = again.collect();
        let mut q = self.q.lock().expect("mailbox");
        (q.resumable, q.cursor, q.forgotten) = (true, o.cursor, o.forgotten);
        drop(o);
        let ev = pb::SessionEvent { event: Some(Event::Resumed(taken)), ..Default::default() };
        let size = ev.encoded_len();
        self.push(q, ev, size);
        again
    }
}

/// The reading end of a mailbox: dropped (its stream ended, or the client
/// went away), it closes the mailbox.
pub struct Reader(pub Arc<Mailbox>);

impl Drop for Reader {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A session's events as a stream for its client. `room` is told when the
/// reader has made room for what waits beyond the mailbox.
pub fn stream<F, Fut>(
    mailbox: Arc<Mailbox>,
    room: F,
) -> impl futures_util::Stream<Item = Result<pb::SessionEvent, tonic::Status>> + Send + 'static
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    futures_util::stream::unfold((Reader(mailbox), room), |(reader, room)| async move {
        let ev = reader.0.recv().await?;
        if reader.0.wants_more() {
            room().await;
        }
        Some((Ok(ev), (reader, room)))
    })
}

/// A session as the core keeps it.
pub struct Session {
    pub mailbox: Arc<Mailbox>,
    pub run: Option<Id>,
    /// The caller's tenant (None: any tenant), which a resume must match.
    pub tenant: Option<String>,
    /// When its stream ended, while it waits to be resumed.
    parked: Option<Instant>,
    /// Outcomes that did not fit, read again from their shards later.
    owed: Vec<Tk>,
    /// Generator items, and replies a resume sends again, that did not fit, in order.
    items: VecDeque<pb::SessionEvent>,
    /// Lines of output dropped since the reader fell behind, and the last of them.
    dropped: (u64, Option<pb::LogEvent>),
}

impl Session {
    pub fn new(mailbox: Arc<Mailbox>, tenant: Option<String>) -> Self {
        Session {
            mailbox,
            run: None,
            tenant,
            parked: None,
            owed: Vec::new(),
            items: VecDeque::new(),
            dropped: (0, None),
        }
    }

    /// A call's outcome; one that does not fit is read again later. While
    /// generator items wait, so does the outcome (read again after them): a
    /// generator's items reach its reader before its end.
    pub fn outcome(&mut self, tk: Tk, outcome: pb::Outcome) {
        if !self.items.is_empty() {
            return self.owed.push(tk);
        }
        let ev = pb::SessionEvent {
            event: Some(Event::Outcome(pb::OutcomeEvent { task: Some(tk.pb()), outcome: Some(outcome) })),
            ..Default::default()
        };
        if self.mailbox.offer(ev, Kind::Outcome(tk)).is_err() {
            self.owed.push(tk);
        }
    }

    /// A generator's item; one that does not fit waits, behind any already waiting.
    pub fn item(&mut self, ev: pb::SessionEvent) {
        if !self.items.is_empty() {
            self.items.push_back(ev);
        } else if let Err(ev) = self.mailbox.offer(ev, Kind::Item) {
            self.items.push_back(*ev);
        }
    }

    /// Output: dropped, and counted, while the reader is behind.
    pub fn log(&mut self, ev: &pb::LogEvent) {
        if self.dropped.0 == 0 {
            let msg = pb::SessionEvent { event: Some(Event::Log(ev.clone())), ..Default::default() };
            if self.mailbox.offer(msg, Kind::Log).is_ok() {
                return;
            }
        }
        self.dropped.0 += ev.text.lines().count().max(1) as u64;
        self.dropped.1 =
            Some(pb::LogEvent { task: ev.task.clone(), stream: "stderr".into(), text: String::new(), pid: ev.pid });
    }

    /// The reader made room: send what waited, as far as it fits. Returns
    /// outcomes to read again from their shards (they come back through
    /// `outcome`).
    pub fn refill(&mut self) -> Vec<Tk> {
        while let Some(ev) = self.items.pop_front() {
            // A reply answers a request's seq; an item has none.
            let kind = if ev.seq == 0 { Kind::Item } else { Kind::Reply };
            if let Err(ev) = self.mailbox.offer(ev, kind) {
                self.items.push_front(*ev);
                return Vec::new();
            }
        }
        if let (n, Some(last)) = &self.dropped {
            let run = last.task.as_ref().and_then(|t| Id::from_slice(&t.run).ok()).map(|r| r.hex()).unwrap_or_default();
            let text = format!(
                "[chimaera: {n} line(s) of output skipped while this program read slowly; `chimaera logs {run}` has them all]\n"
            );
            let msg = pb::SessionEvent {
                event: Some(Event::Log(pb::LogEvent { text, ..last.clone() })),
                ..Default::default()
            };
            if self.mailbox.offer(msg, Kind::Log).is_err() {
                return Vec::new();
            }
            self.dropped = (0, None);
        }
        let again = self.owed.split_off(self.owed.len().saturating_sub(REFILL));
        if !self.owed.is_empty() {
            self.mailbox.owe();
        }
        again
    }

    /// Its stream ended. A resumable session waits RESUME_WINDOW for its
    /// client (true); any other ends now (false).
    pub fn park(&mut self, now: Instant) -> bool {
        self.mailbox.close();
        let resumable = self.mailbox.q.lock().expect("mailbox").resumable;
        if resumable {
            self.parked = Some(now);
        }
        resumable
    }

    pub fn parked(&self) -> bool {
        self.parked.is_some()
    }

    pub fn expired(&self, now: Instant) -> bool {
        self.parked.is_some_and(|t| now.saturating_duration_since(t) >= RESUME_WINDOW)
    }

    /// Why a client that received up to `cursor` cannot resume this session.
    pub fn cannot_resume(&self, cursor: u64) -> Option<String> {
        let q = self.mailbox.q.lock().expect("mailbox");
        if !q.resumable {
            Some("its client acknowledged nothing".to_string())
        } else if cursor < q.forgotten {
            Some(format!("it received up to {cursor}, and this peer kept only what came after {}", q.forgotten))
        } else if cursor > q.cursor {
            Some(format!("it claims cursor {cursor}, and the session sent only {}", q.cursor))
        } else {
            None
        }
    }

    /// Continue `old`, whose client received up to `cursor`: what it missed is
    /// sent again (call `refill`), with new cursors.
    pub fn adopt(&mut self, old: Session, cursor: u64) {
        self.run = old.run;
        for sent in self.mailbox.adopt(&old.mailbox, cursor) {
            match sent {
                Sent::Outcome(tk) => self.owed.push(tk),
                Sent::Event(ev) => self.items.push_back(*ev),
            }
        }
        self.owed.extend(old.owed);
        self.items.extend(old.items);
        self.dropped = old.dropped;
    }

    /// What waits beyond the mailbox: outcomes, generator items and replies.
    pub fn backlog(&self) -> usize {
        self.owed.len() + self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tk(i: u8) -> Tk {
        Tk { run: Id([1; 16]), task: Id([i; 16]) }
    }

    fn outcome(bytes: usize) -> pb::Outcome {
        let value = pb::ObjectRef { inline_data: vec![7; bytes], ..Default::default() };
        pb::Outcome { value: Some(pb::outcome::Value::Ok(value)), ..Default::default() }
    }

    fn log(text: &str) -> pb::LogEvent {
        pb::LogEvent { task: Some(tk(0).pb()), stream: "stdout".into(), text: text.into(), pid: 9 }
    }

    fn reply(seq: u64) -> pb::SessionEvent {
        pb::SessionEvent { seq, event: Some(Event::Error(format!("reply to {seq}"))), ..Default::default() }
    }

    fn drain(mb: &Mailbox) -> Vec<pb::SessionEvent> {
        let mut out = Vec::new();
        while let Some(ev) = futures_util::FutureExt::now_or_never(mb.recv()).flatten() {
            out.push(ev);
        }
        out
    }

    fn task_of(ev: &pb::SessionEvent) -> Option<Tk> {
        match &ev.event {
            Some(Event::Outcome(o)) => o.task.as_ref().and_then(Tk::parse),
            _ => None,
        }
    }

    #[test]
    fn a_mailbox_holds_bounded_events_and_bytes_and_keeps_room_for_outcomes() {
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        // Output fills three quarters of the bytes, then is dropped and counted.
        const MIB: usize = 1 << 20;
        for _ in 0..20 {
            s.log(&log(&"x".repeat(MIB - 100)));
        }
        assert!(mb.queued_bytes() > MAX_BYTES - RESERVE_BYTES - MIB && mb.queued_bytes() <= MAX_BYTES - RESERVE_BYTES);
        assert_eq!(s.dropped.0, 8);
        // Outcomes still fit in the reserve, up to the limit; the rest are owed.
        for i in 0..8 {
            s.outcome(tk(i), outcome(MIB - 100));
        }
        assert!(mb.queued_bytes() <= MAX_BYTES);
        assert_eq!(s.owed.len(), 4);
        // Events are bounded too, small or not.
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        for i in 0..MAX_EVENTS + 10 {
            s.outcome(tk(i as u8), outcome(1));
        }
        assert_eq!(drain(&mb).len(), MAX_EVENTS);
        assert_eq!(s.owed.len(), 10);
    }

    #[test]
    fn what_waited_is_sent_once_the_reader_makes_room() {
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        s.log(&log(&"y".repeat(MAX_BYTES)));
        s.log(&log("two\nlines\n"));
        let item = pb::SessionEvent {
            event: Some(Event::StreamItem(pb::StreamItem { index: 3, ..Default::default() })),
            ..Default::default()
        };
        s.item(item.clone());
        s.outcome(tk(1), outcome(MAX_BYTES / 2));
        assert_eq!((s.backlog(), s.dropped.0), (2, 2));
        assert!(!mb.wants_more(), "the reader has not made room yet");
        assert_eq!(drain(&mb).len(), 1);
        assert!(mb.wants_more(), "a refusal, and the mailbox half empty");
        assert!(!mb.wants_more(), "once per refusal");
        let again = s.refill();
        assert_eq!(again, vec![tk(1)]);
        let evs = drain(&mb);
        assert!(matches!(&evs[0].event, Some(Event::StreamItem(i)) if i.index == 3));
        assert!(matches!(&evs[1].event, Some(Event::Log(l)) if l.text.contains("2 line(s)") && l.pid == 9));
        assert_eq!(s.backlog(), 0);
    }

    #[test]
    fn a_generators_end_waits_behind_its_items() {
        let item = |index| pb::SessionEvent {
            event: Some(Event::StreamItem(pb::StreamItem { index, value: Some(outcome(4096)), ..Default::default() })),
            ..Default::default()
        };
        let kinds = |evs: Vec<pb::SessionEvent>| -> Vec<String> {
            let kind = |e: pb::SessionEvent| match e.event {
                Some(Event::StreamItem(i)) => format!("item {}", i.index),
                Some(Event::Outcome(_)) => "end".into(),
                Some(Event::Resumed(_)) => "resumed".into(),
                other => format!("{other:?}"),
            };
            evs.into_iter().map(kind).collect()
        };
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        mb.ack(0);
        // Output takes the room items may: the item waits, and so does the
        // end behind it, though the end would fit in the reserve.
        s.log(&log(&"z".repeat(MAX_BYTES - RESERVE_BYTES)));
        s.item(item(0));
        s.outcome(tk(1), outcome(1));
        assert_eq!(s.backlog(), 2);
        assert_eq!(drain(&mb).len(), 1);
        assert!(mb.wants_more());
        assert_eq!(s.refill(), vec![tk(1)], "read again once the item has gone");
        s.outcome(tk(1), outcome(1));
        assert_eq!(kinds(drain(&mb)), ["item 0", "end"]);
        // After a resume: what is sent again goes before an outcome made since.
        s.item(item(1)); // sent, and never received
        assert!(s.park(Instant::now()));
        let mb2 = Mailbox::new();
        let mut s2 = Session::new(mb2.clone(), None);
        s2.adopt(s, 2);
        s2.outcome(tk(2), outcome(1));
        assert_eq!(s2.refill(), vec![tk(2)]);
        s2.outcome(tk(2), outcome(1));
        assert_eq!(kinds(drain(&mb2)), ["resumed", "item 1", "end"]);
    }

    #[tokio::test]
    async fn a_reply_waits_for_room_and_a_closed_mailbox_refuses_it() {
        let mb = Mailbox::new();
        for i in 0..MAX_EVENTS {
            mb.offer(pb::SessionEvent { seq: i as u64, ..Default::default() }, Kind::Notice).unwrap();
        }
        let waiting = tokio::spawn({
            let mb = mb.clone();
            async move { mb.send(pb::SessionEvent { seq: 99_999, ..Default::default() }).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "no room for the reply yet");
        assert_eq!(mb.recv().await.unwrap().seq, 0);
        assert!(waiting.await.unwrap());
        let blocked = tokio::spawn({
            let mb = mb.clone();
            async move { mb.send(pb::SessionEvent::default()).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(Reader(mb.clone())); // the client went away
        assert!(!blocked.await.unwrap());
        assert!(mb.recv().await.is_none());
    }

    #[test]
    fn a_resume_sends_again_exactly_what_the_client_did_not_receive() {
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), Some("t".into()));
        mb.ack(0);
        for i in 0..(MAX_EVENTS + 100) {
            s.outcome(tk((i % 250) as u8), outcome(1));
        }
        // The client received 1000 events, acknowledged 600, then its stream broke.
        let got: Vec<u64> = drain(&mb).into_iter().take(1000).map(|e| e.cursor).collect();
        assert_eq!(got, (1..=1000).collect::<Vec<_>>());
        mb.ack(600);
        assert!(s.park(Instant::now()));
        assert!(s.cannot_resume(1000).is_none());
        assert!(s.cannot_resume(MAX_EVENTS as u64 + 1).is_some(), "past what was sent");
        let mb2 = Mailbox::new();
        let mut s2 = Session::new(mb2.clone(), Some("t".into()));
        s2.adopt(s, 1000);
        assert_eq!(s2.backlog(), MAX_EVENTS - 1000 + 100);
        assert!(matches!(drain(&mb2)[..], [pb::SessionEvent { event: Some(Event::Resumed(0)), cursor: 0, .. }]));
        let mut cursors = Vec::new();
        loop {
            let again = s2.refill();
            if again.is_empty() {
                break;
            }
            for t in again {
                s2.outcome(t, outcome(1));
            }
            cursors.extend(drain(&mb2).iter().map(|e| (task_of(e).is_some(), e.cursor)));
        }
        assert_eq!(cursors.len(), MAX_EVENTS - 1000 + 100);
        assert!(cursors.iter().all(|(outcome, _)| *outcome));
        let fresh: Vec<u64> = cursors.iter().map(|(_, c)| *c).collect();
        let want: Vec<u64> = (MAX_EVENTS as u64 + 1..=2 * MAX_EVENTS as u64 + 100 - 1000).collect();
        assert_eq!(fresh, want, "new cursors, after every one sent before");
    }

    #[tokio::test]
    async fn a_reply_the_client_missed_is_sent_again_and_the_resume_says_which_requests_were_handled() {
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        mb.ack(0);
        mb.took(1);
        // Reply 7 was queued and not read when the stream broke; reply 8 was
        // made after it broke. Request 9 never reached the session.
        assert!(mb.send(reply(7)).await);
        mb.took(7);
        assert!(s.park(Instant::now()));
        assert!(mb.send(reply(8)).await, "kept for the resume");
        mb.took(8);
        let mb2 = Mailbox::new();
        let mut s2 = Session::new(mb2.clone(), None);
        s2.adopt(s, 0);
        assert!(s2.refill().is_empty());
        let evs = drain(&mb2);
        assert!(matches!(evs[0].event, Some(Event::Resumed(8))), "first: the client sends again from 9");
        let replies: Vec<(u64, u64)> = evs[1..].iter().map(|e| (e.seq, e.cursor)).collect();
        assert_eq!(replies, [(7, 3), (8, 4)], "each once, with new cursors");
        assert!(evs.iter().all(|e| e.taken == 8));
        // Requests the new stream handles count from there.
        mb2.took(9);
        s2.outcome(tk(1), outcome(1));
        assert_eq!(drain(&mb2)[0].taken, 9);
        // A session whose client does not acknowledge cursors says nothing of the kind.
        let plain = Mailbox::new();
        plain.took(3);
        assert!(plain.send(reply(3)).await);
        assert_eq!((drain(&plain)[0].taken, drain(&plain).len()), (0, 0));
    }

    #[test]
    fn a_client_that_stops_acknowledging_cannot_resume_from_what_was_forgotten() {
        let mb = Mailbox::new();
        let mut s = Session::new(mb.clone(), None);
        mb.ack(0);
        for round in 0..5 {
            for i in 0..MAX_EVENTS {
                s.outcome(tk((i % 250) as u8), outcome(1));
            }
            assert_eq!(drain(&mb).len(), MAX_EVENTS, "round {round}");
        }
        let cursor = {
            let q = mb.q.lock().unwrap();
            assert!(q.unacked.len() <= UNACKED_EVENTS);
            q.cursor
        };
        assert!(s.cannot_resume(10).is_some());
        assert!(s.cannot_resume(cursor - 10).is_none());
        // A session whose client never acknowledged keeps nothing, and is not parked.
        let plain = Mailbox::new();
        let mut s = Session::new(plain.clone(), None);
        s.outcome(tk(1), outcome(1));
        assert!(plain.q.lock().unwrap().unacked.is_empty() && !s.park(Instant::now()));
    }
}
