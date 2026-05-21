//! Well-known paths for the frontend's Unix-domain control socket.
//!
//! Single-listener topology per
//! [`docs/design/deferred/slot-readiness.md`](../../../../docs/design/deferred/slot-readiness.md)
//! §2.0: the FE binds one well-known UDS path, every slot bgworker
//! connects to it, and the FE assigns a monotonic `slot_id` on each
//! accept. No per-slot paths, no `bgw_main_arg` plumbing.
//!
//! Phase 2 hard-codes the socket directory. Phase 7 introduces
//! `pg_transport.socket_directory` per
//! [configuration.md](../../../../docs/design/configuration.md);
//! this module's `slot_dir()` will then read that GUC. Until then,
//! multi-cluster collision on the directory is the operator's
//! responsibility — only one PG cluster per host can use
//! pg_transport simultaneously.

use std::path::PathBuf;

/// Hard-coded socket directory for phase 2. World-writable parent
/// is fine (the socket file itself is `chmod 0600`).
///
/// TODO(phase-7): replace with `pg_transport.socket_directory` GUC,
/// defaulting to the first entry of `unix_socket_directories`.
const SLOT_DIR: &str = "/tmp/pg_transport_sockets";

pub fn slot_dir() -> PathBuf {
    PathBuf::from(SLOT_DIR)
}

/// `/tmp/pg_transport_sockets/frontend.sock`
///
/// The single UDS path the FE binds and every BE slot connects to.
/// Cluster discriminator is the parent directory (see [`slot_dir`]);
/// the filename intentionally stays cluster-agnostic.
pub fn frontend_socket_path() -> PathBuf {
    slot_dir().join("frontend.sock")
}
