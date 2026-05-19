//! Backend pool — slot bgworkers and the FE-side machinery that
//! sends fds to them.
//!
//! Layered per [backend-handoff.md §3](../../../docs/design/backend-handoff.md):
//! * `pool` — frontend-side: per-slot UDS listener + sendmsg
//! * `slot` — backend-side: per-slot bgworker entry + recvmsg loop
//! * `fd_pass` — low-level SCM_RIGHTS sendmsg / recvmsg helpers
//! * `paths` — well-known socket paths
//!
//! Phase 2 lands the socket plumbing only; the wire layer is phase 4.

pub mod extended;
pub mod fd_pass;
pub mod paths;
pub mod pool;
pub mod slot;
pub mod spi_bridge;
