//! Protocol-agnostic rateless-IBLT core.
//!
//! Shared by every protocol with a rateless-IBLT phase (riblt, rbf_riblt,
//! rf_riblt): the wire-format symbol types, the blocking encode/decode session
//! helpers, and the transport-agnostic streaming engine. Nothing here depends
//! on any particular protocol's messages or ids; each protocol plugs in
//! through the `stream` traits or calls the `session` helpers directly.

pub mod session;
pub mod stream;
pub mod symbols;

pub use symbols::{RIBLTCodedSymbol, RIBLTSymbol};
