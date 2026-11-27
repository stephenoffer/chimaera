//! What a finished call's record keeps once nothing holds it: no caller,
//! dependent, dead letter or hold.
//!
//! A run's records stay until it ends, and a long-lived driver's run never
//! does. Kept whole, a released call cost about 340 bytes: its outcome, with
//! the reference to a value its release had already freed, and a pin count
//! of 0. Two million tiny calls grew the state to 2 GiB and slowed every
//! read of it.

use super::*;

/// The outcome a released call's record keeps. A plain call that succeeded
/// keeps only that it did, and which attempt it was: its release freed the
/// value, and only a call that can be asked for again by key reads the
/// reference later. Plain is a function or map item call, not a stream or a
/// durable workflow's step, in a run known not to be detached (a driver
/// attaching to it again reads its calls' results). Any other call, and one
/// whose run is not known here, keeps its outcome whole.
pub(super) fn kept_outcome(kv: &dyn KvRead, tk: Tk, rec: &pb::TaskRecord) -> Option<pb::Outcome> {
    let plain = rec.spec.as_ref().is_some_and(|s| {
        matches!(s.kind(), pb::CallKind::CallFunction | pb::CallKind::CallMapItem)
            && !s.stream
            && s.command_fingerprint.is_empty()
            && s.workflow_step.is_empty()
    });
    let succeeded = matches!(rec.outcome.as_ref().and_then(|o| o.value.as_ref()), Some(pb::outcome::Value::Ok(_)));
    if plain && succeeded && read::run_in(kv, tk.v(), tk.run).is_some_and(|r| !r.detached) {
        return rec.outcome.as_ref().map(succeeded_only);
    }
    rec.outcome.clone()
}

/// A success as a released plain call's record keeps it.
pub(super) fn succeeded_only(o: &pb::Outcome) -> pb::Outcome {
    pb::Outcome { value: Some(pb::outcome::Value::Ok(pb::ObjectRef::default())), attempt: o.attempt, peer: None }
}
