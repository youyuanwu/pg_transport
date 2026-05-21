//! pg_transport — Postgres extension that hosts alternative network
//! transports as a frontend bgworker + a pool of backend bgworkers
//! handing client fds via `SCM_RIGHTS`.
//!
//! See [`docs/design/README.md`](../../../docs/design/README.md) for
//! the topic index.
//!
//! v0 surface (incremental, see `docs/design/roadmap.md`):
//! * phase 0 — scaffold: workspace compiles; `_PG_init` is a no-op.
//! * phase 1 — `_PG_init` SPL-checks and registers the frontend
//!   bgworker, which boots a tokio current-thread runtime with
//!   heartbeat, SIGHUP/SIGTERM, and postmaster-death watchdog.
//! * phase 2 — slot bgworkers; FE binds a UDS listener and accepts
//!   slot connections; FE can `sendmsg(SCM_RIGHTS)` a client fd to
//!   a slot. Per
//!   [`docs/design/deferred/slot-readiness.md`](../../../docs/design/deferred/slot-readiness.md):
//!   single FE listener, demand-driven autoscaling, cooperative
//!   drain. `_PG_init` registers **only** the FE bgworker; all
//!   slots are dynamic (`load_dynamic`) and created on demand by
//!   the dispatcher.
//! * phase 3+ — tcp_handoff transport, wire layer, SPI bridge.

use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

mod backend;
mod frontend;
mod guc;
mod handoff;
mod wire;

/// Postgres calls this once per backend when it loads the extension's
/// shared library. In the postmaster (when `shared_preload_libraries`
/// includes us) this is the only context where static
/// `RegisterBackgroundWorker` calls are legal — see
/// [`docs/design/backend-handoff.md`](../../../docs/design/backend-handoff.md) §1
/// and the Q4 / Q22 resolutions in `docs/design/roadmap.md` §2.3.
///
/// Phase 1: SPL check + frontend bgworker registration. Slot pool
/// registration (phase 2) lands in the same function alongside the
/// frontend.
///
/// `#[pg_guard]` converts panics into PG ereports rather than
/// unwinding into PG's C frames. Workspace sets `panic = "unwind"`
/// (Q9 re-resolved via Q24 in docs/design/roadmap.md). Boundary
/// errors here use `ereport!(FATAL, …)` rather than `error!()`
/// because the latter routes through `panic_any` — with unwind
/// that's fine, but FATAL is the semantically correct level for
/// startup-time failures that must terminate the process (rather
/// than be longjmp'd to an outer handler).
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // SAFETY: `process_shared_preload_libraries_in_progress` is a
    // process-global PG flag set during postmaster startup; reading
    // it is a plain memory load with no aliasing concerns.
    let in_spl = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    if !in_spl {
        // Not in shared_preload_libraries — we're being loaded by a
        // regular backend (e.g. on first `CREATE EXTENSION`). Static
        // bgworker registration is illegal here, and we refuse to
        // proceed rather than silently degrade. Rationale + the
        // rejected lazy-spawn path: docs/design/roadmap.md Q4.
        //
        // FATAL (not ERROR) deliberately: FATAL exits the backend
        // process cleanly via `proc_exit(1)`, which is what we want
        // for "you forgot to configure SPL". ERROR would just be
        // longjmp'd to the CREATE EXTENSION call site and the user
        // could keep retrying with the same misconfiguration.
        pgrx::ereport!(
            FATAL,
            PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR,
            "pg_transport must be in shared_preload_libraries",
            "Set `shared_preload_libraries = 'pg_transport'` in postgresql.conf and restart."
        );
    }

    // Register GUCs before anything reads them. Phase 2 surface:
    // pg_transport.backend_pool_size only (Q23: GUC list starts at
    // phase 2). Phase 7 adds pg_transport.auth_source — required,
    // no default, validated immediately so an unset value FATALs at
    // cluster boot rather than at the first slot's first auth.
    guc::register();
    guc::validate_required();

    // Phase 8: install rustls' `ring` crypto provider process-wide.
    // rustls 0.23 requires an explicit provider install; without
    // this, `ServerConfig::builder()` panics at "no process-level
    // CryptoProvider available". Idempotent — `install_default`
    // returns Err if already set, which we ignore (covers the
    // case where two bgworkers in the same postmaster process
    // race; postmaster is single-process so they don't, but
    // belt-and-suspenders).
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    // Frontend bgworker (Q22 → option (a)): registered statically so
    // it comes up at postmaster start. The FE binds the single UDS
    // listener (`paths::frontend_socket_path`) and runs the
    // demand-driven autoscaler. Slot bgworkers are NOT registered
    // here — they're spawned dynamically by the FE's dispatcher via
    // `load_dynamic` when a handoff arrives with no ready slot. See
    // `docs/design/deferred/slot-readiness.md` §5.4.
    BackgroundWorkerBuilder::new("pg_transport frontend")
        .set_type("pg_transport_frontend")
        .set_library("pg_transport")
        .set_function("pg_transport_frontend_main")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_shmem_access(None)
        .load();
}

/// Returns the extension version as a packed integer:
/// `MAJOR * 10_000 + MINOR * 100 + PATCH`.
///
/// Sourced from `Cargo.toml` at compile time, so bumping the package
/// version automatically bumps this.
#[pg_extern(immutable, parallel_safe)]
fn pg_transport_extension_version() -> i32 {
    const MAJOR: i32 = parse_u16(env!("CARGO_PKG_VERSION_MAJOR")) as i32;
    const MINOR: i32 = parse_u16(env!("CARGO_PKG_VERSION_MINOR")) as i32;
    const PATCH: i32 = parse_u16(env!("CARGO_PKG_VERSION_PATCH")) as i32;
    MAJOR * 10_000 + MINOR * 100 + PATCH
}

/// Tiny `const fn` u16 parser so the version is computed at compile
/// time and we don't pay for `str::parse` at every call.
const fn parse_u16(s: &str) -> u16 {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut acc: u16 = 0;
    while i < bytes.len() {
        let b = bytes[i];
        assert!(b.is_ascii_digit(), "version segment must be numeric");
        acc = acc * 10 + (b - b'0') as u16;
        i += 1;
    }
    acc
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn version_is_packed_int() {
        // v0.0.1 → 0 * 10_000 + 0 * 100 + 1 = 1
        let v = Spi::get_one::<i32>("SELECT pg_transport_extension_version()")
            .expect("SPI failure")
            .expect("NULL from version()");
        assert_eq!(v, 1);
    }
}

/// `cargo pgrx test` discovery hook.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // Required for the SPL check in `_PG_init()` to pass and for
        // `BackgroundWorkerBuilder::load()` (static registration) to
        // be legal — see docs/design/backend-handoff.md §1.
        // `pg_transport.auth_source` is required by `_PG_init()` since
        // phase 7 (FATAL if unset); pg_hba is the v0 documented default.
        vec![
            "shared_preload_libraries = 'pg_transport'",
            "pg_transport.auth_source = 'pg_hba'",
        ]
    }
}
