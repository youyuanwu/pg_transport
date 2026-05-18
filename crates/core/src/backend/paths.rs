//! Well-known paths for per-slot Unix-domain control sockets.
//!
//! Phase 2 hard-codes the socket directory. Phase 7 introduces
//! `pg_transport.socket_directory` per
//! [configuration.md](../../../../docs/design/configuration.md) and
//! [frontend-handoff.md §2.1](../../../../docs/design/frontend-handoff.md);
//! this module's `slot_dir()` will then read that GUC.

use std::path::PathBuf;

/// Hard-coded socket directory for phase 2. World-writable parent
/// is fine (the socket files themselves are `chmod 0600`).
///
/// TODO(phase-7): replace with `pg_transport.socket_directory` GUC,
/// defaulting to the first entry of `unix_socket_directories`.
const SLOT_DIR: &str = "/tmp/pg_transport_sockets";

pub fn slot_dir() -> PathBuf {
    PathBuf::from(SLOT_DIR)
}

/// `/tmp/pg_transport_sockets/slot.<slot_id>.sock`
///
/// Per [frontend-handoff.md §2.1](../../../../docs/design/frontend-handoff.md),
/// the design's preferred filename pattern includes the frontend
/// bgworker PID for collision safety across restarts. Under the
/// Q22-resolved static-registration model there is exactly one
/// frontend per cluster at a time, so the slot-id-only path is
/// unambiguous; a defensive `unlink()` before `bind()` covers
/// leftover files from a crashed previous run.
pub fn slot_socket_path(slot_id: u32) -> PathBuf {
    slot_dir().join(format!("slot.{slot_id}.sock"))
}
