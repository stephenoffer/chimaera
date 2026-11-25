//! Durable workflow replays: a call that differs from the recorded one at a
//! step is rejected with both calls as the workflow described them.

use super::*;

#[test]
fn a_divergent_replay_is_rejected_with_the_recorded_call_and_its_own() {
    let mut h = Harness::new(ShardState::new());
    let t = task_id(V, 0);
    let step = |fingerprint: &[u8], shown: &str| pb::TaskSpec {
        command_fingerprint: fingerprint.to_vec(),
        workflow_step: shown.into(),
        ..spec(t, &[], pb::Effects::None, 3)
    };
    let recorded = r#"{"step": 2, "function": "m.f", "site": "w.py:7", "args": "(1)", "fingerprint": "aa"}"#;
    let received = r#"{"step": 2, "function": "m.f", "site": "w.py:7", "args": "(2)", "fingerprint": "bb"}"#;
    assert_ne!(status(&h.submit(step(b"aa", recorded), None).0), Status::Rejected);
    // The same call again is the recorded step.
    assert_eq!(status(&h.submit(step(b"aa", received), None).0), Status::Duplicate);
    let (ack, _) = h.submit(step(b"bb", received), None);
    assert_eq!(status(&ack), Status::Rejected);
    assert_eq!(ack.reason, format!("workflow divergence: [{recorded}, {received}]"));
    // A client that describes nothing still learns what was recorded.
    let (ack, _) = h.submit(step(b"cc", ""), None);
    assert_eq!(ack.reason, format!("workflow divergence: [{recorded}, null]"));
}
