//! The emulator's exit code is its verdict, so a lane that records a run
//! (bench/qualification) passes only when every gate did.

use std::process::Command;

fn run(args: &[&str]) -> (Option<i32>, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_chimaera-fleet")).args(args).output().expect("run chimaera-fleet");
    (out.status.code(), String::from_utf8_lossy(&out.stdout).into_owned())
}

#[test]
fn a_failed_gate_fails_the_run_and_a_passing_one_does_not() {
    // 20 simulated seconds per cell is too short for the rows to come within 10% of the appendix.
    let (code, out) = run(&["--seconds", "20"]);
    assert!(code == Some(1) && out.contains("result: FAIL"), "{code:?}\n{out}");
    let (code, out) = run(&["--seconds", "20", "--json"]);
    assert!(code == Some(1) && out.contains("\"pass\": false"), "{code:?}\n{out}");
    let (code, out) = run(&[]);
    assert!(code == Some(0) && out.contains("result: PASS"), "{code:?}\n{out}");
}
