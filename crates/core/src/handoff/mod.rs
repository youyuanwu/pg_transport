//! Handoff transports — accept-loops that produce kernel fds for the
//! backend pool.
//!
//! v0 has exactly one transport (`tcp_handoff`); no Cargo features
//! gate it. See [transports.md](../../../../docs/design/transports.md)
//! and [workspace.md §2](../../../../docs/design/workspace.md).
//!
//! Per [workspace.md §1](../../../../docs/design/workspace.md), the
//! design earmarks `handoff/listener.rs` as a shared accept-loop
//! helper used by every transport. With one transport in v0 that
//! split is YAGNI; phase 6 (when a second handoff transport
//! un-defers) is the natural time to factor it out.

pub mod tcp;
