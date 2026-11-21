//! Pure, deterministic Chimaera logic.
//!
//! Nothing in this crate performs IO, reads a clock or draws randomness, so the
//! replicated state machine and the membership logic behave identically on
//! every replica and in simulation.

pub mod adapt;
pub mod hlc;
pub mod ids;
pub mod kv;
pub mod shard;
pub mod topology;

pub use chimaera_proto::v1 as pb;
