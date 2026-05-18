//! pg_transport — Postgres extension that hosts alternative network
//! transports as a frontend bgworker + a pool of backend bgworkers
//! handing client fds via `SCM_RIGHTS`.
//!
//! See [`docs/design/README.md`](../../../docs/design/README.md) for
//! the topic index; v0 scope is locked at phase 0 (this scaffold).
//!
//! v0 surface (incremental, see `docs/design/roadmap.md`):
//! * phase 0 — this scaffold: workspace compiles; `_PG_init`
//!   registers nothing yet.
//! * phase 1 — frontend bgworker boots a tokio current-thread
//!   runtime; heartbeat; SIGHUP/SIGTERM.
//! * phase 2+ — backend pool, handoff, wire layer, SPI bridge.

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

/// Postgres calls this once per backend when it loads the extension's
/// shared library. v0 phase 0: no-op. Subsequent phases will register
/// GUCs (`guc::init()`), start the frontend bgworker
/// (`BackgroundWorkerBuilder::load()`), and validate `auth_source`
/// (see [`docs/design/backend-handoff.md`](../../../docs/design/backend-handoff.md)
/// §1 and [`docs/design/configuration.md`](../../../docs/design/configuration.md)).
///
/// Must be `#[pg_guard]`'d so that any panic / `ereport(ERROR)` inside
/// initialization is converted to a Postgres ERROR rather than
/// unwinding into Postgres' C frames. (Workspace also sets
/// `panic = "abort"` per docs/design/roadmap.md Q9, so an uncaught
/// panic aborts the postmaster — keep this body minimal.)
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Intentionally empty in phase 0.
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
        // Phase 0: empty. Future phases will add
        //   "shared_preload_libraries = 'pg_transport'"
        // here (required for static bgworker registration via
        // BackgroundWorkerBuilder::load() — see
        // docs/design/backend-handoff.md §1).
        vec![]
    }
}
