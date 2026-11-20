//! Wire and log schemas for Chimaera, generated from `proto/chimaera/v1`.
//!
//! Every message that crosses a process boundary or is stored in a Raft log is
//! defined there, so peers of adjacent versions can decode each other's data.

pub mod frame;

/// Version of the peer and client wire protocol. Peers negotiate the minimum
/// of both sides; a log command type is proposed only once the cluster's
/// committed feature level includes it.
pub const WIRE_VERSION: u32 = 1;

#[allow(clippy::all, clippy::pedantic)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/chimaera.v1.rs"));
}

pub use prost::Message;
