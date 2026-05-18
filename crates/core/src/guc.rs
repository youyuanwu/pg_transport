//! GUC registration.
//!
//! Phase 2 adds the first user-facing knob (Q23 said zero new GUCs
//! in phase 1; the GUC surface starts at phase 2). See
//! `docs/design/configuration.md` for the eventual phase-≥2 list.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

/// `pg_transport.backend_pool_size` — how many slot bgworkers
/// `_PG_init()` registers at postmaster start.
///
/// Read once at postmaster start; changes require restart (`PGC_POSTMASTER`).
/// Live pool resize is [Q15](../../docs/design/roadmap.md) — deferred.
///
/// Default `2` is a phase-2 testing convenience; the design's eventual
/// default is `max(4, num_cpus)` (see
/// [backend-handoff.md §1](../../docs/design/backend-handoff.md)).
pub static BACKEND_POOL_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2);

pub fn register() {
    GucRegistry::define_int_guc(
        c"pg_transport.backend_pool_size",
        c"Number of slot bgworkers in the backend pool.",
        c"Read once at postmaster start; changes require a restart.",
        &BACKEND_POOL_SIZE,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

#[inline]
pub fn backend_pool_size() -> u32 {
    BACKEND_POOL_SIZE.get() as u32
}
