//! Per-query observability plumbing — pins
//! [`pg_sys::debug_query_string`] and reports `STATE_RUNNING` /
//! `STATE_IDLE` to pgstat across a query body, matching what
//! vanilla `exec_simple_query` does at
//! [postgres.c:1046-1048](../../../../../postgres/src/backend/tcop/postgres.c#L1046-L1048).
//!
//! Design: [`docs/design/deferred/simple-query-direct-path.md §7.5`](../../../../docs/design/deferred/simple-query-direct-path.md#75-per-query-observability-globals).
//!
//! Used by every backend entry point:
//! - [`super::simple_direct::execute_simple_query_direct`] —
//!   simple-query direct backend.
//! - [`super::spi_bridge::execute_simple_query`] —
//!   simple-query SPI backend.
//! - [`super::extended::prepare`] —
//!   extended-query Parse phase (both backends).
//! - [`super::extended::PreparedStatement::execute`] —
//!   extended-query Bind/Execute phase (both backends).
//!
//! Consumers that depend on these globals being correct:
//! - `pg_stat_statements` (reads `debug_query_string` at
//!   `post_parse_analyze_hook` / `ExecutorStart_hook` time).
//! - `auto_explain` (reads `debug_query_string` at
//!   `ExecutorStart_hook` time for the EXPLAIN heading).
//! - `pg_stat_activity.query` column (fed by
//!   `pgstat_report_activity`).
//! - Server-log `STATEMENT: <sql>` line emitted by
//!   `send_message_to_server_log` for ERROR events.

use std::ffi::CStr;
use std::marker::PhantomData;

use pgrx::pg_sys;

/// RAII guard that pins `pg_sys::debug_query_string` to a query
/// body and reports `STATE_RUNNING` to pgstat for the lifetime
/// of the guard. Restores both on drop — Ok, Err, AND PG-ERROR
/// panic paths (via `catch_unwind`) all run cleanup.
///
/// The `'a` lifetime ties the guard to the borrowed `CStr` whose
/// pointer we install into the global. The borrow checker
/// enforces that the backing buffer outlives the guard, so the
/// pointer cannot dangle (e.g. a caller who builds a temporary
/// `CString` and lets it drop while the guard is alive would be
/// rejected at compile time).
///
/// Mirrors vanilla [`exec_simple_query` at postgres.c:1046-1048](../../../../../postgres/src/backend/tcop/postgres.c#L1046-L1048):
///
/// ```c
/// debug_query_string = query_string;
/// pgstat_report_activity(STATE_RUNNING, query_string);
/// ```
///
/// and the matching tail at [`postgres.c` end-of-function](../../../../../postgres/src/backend/tcop/postgres.c#L1378)
/// which sets `debug_query_string = NULL`. We additionally
/// transition pgstat back to `STATE_IDLE` on drop, since
/// pg_transport's slot bgworker loop has no equivalent of
/// `PostgresMain`'s ReadCommand-loop idle reporter.
///
/// Without this guard:
/// - `pg_stat_statements` / `auto_explain` mis-attribute every
///   pg_transport-served query to whatever SQL the outer backend
///   frame happened to be running.
/// - The server-log `STATEMENT:` line for any ERROR raised
///   during execution names the wrong SQL.
/// - `pg_stat_activity.query` for the slot bgworker never
///   reflects the SQL the worker is actually running.
pub(crate) struct DebugQueryGuard<'a> {
    prev_debug_query_string: *const std::ffi::c_char,
    /// Compile-time witness that the `CStr` we installed into
    /// `pg_sys::debug_query_string` outlives the guard. The
    /// installed pointer is never dereferenced through this
    /// field — it's only here to anchor the lifetime.
    _sql_cstr: PhantomData<&'a CStr>,
}

impl<'a> DebugQueryGuard<'a> {
    /// Install `sql_cstr` as the active `debug_query_string` and
    /// report `STATE_RUNNING` to pgstat. The returned guard
    /// borrows `sql_cstr` for its entire lifetime, so the
    /// compiler will reject any caller that drops the underlying
    /// `CString` before the guard.
    ///
    /// # Safety
    ///
    /// Must be called from a PG backend / bgworker (the only
    /// place `debug_query_string` and `pgstat_report_activity`
    /// are meaningful).
    pub(crate) unsafe fn install(sql_cstr: &'a CStr) -> Self {
        let ptr = sql_cstr.as_ptr();
        // SAFETY: caller guarantees a backend context; ptr
        // lifetime is enforced by `'a`; both calls are PG
        // server-API entry points.
        let prev_debug_query_string = unsafe {
            let prev = pg_sys::debug_query_string;
            pg_sys::debug_query_string = ptr;
            pg_sys::pgstat_report_activity(pg_sys::BackendState::STATE_RUNNING, ptr);
            prev
        };
        DebugQueryGuard {
            prev_debug_query_string,
            _sql_cstr: PhantomData,
        }
    }
}

impl Drop for DebugQueryGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: paired with install(); both globals are
        // writable from any backend.
        unsafe {
            pg_sys::debug_query_string = self.prev_debug_query_string;
            pg_sys::pgstat_report_activity(pg_sys::BackendState::STATE_IDLE, std::ptr::null());
        }
    }
}

// ---------------------------------------------------------------------------
// StatementTimeoutGuard — arm/disarm STATEMENT_TIMEOUT per query
// ---------------------------------------------------------------------------

// Manual FFI bindings for the PG timeout API. pgrx-pg-sys 0.18
// does not expose `enable_timeout_after` / `disable_timeout` /
// `get_timeout_active`, even though they are plain extern C
// functions in `src/backend/utils/misc/timeout.c`. Vanilla
// `enable_statement_timeout` (postgres.c:5208) wraps these with
// the StatementTimeout > 0 + already-active checks; we replicate
// that policy here. The wrapper itself is `static` in
// `postgres.c` and therefore not linkable from outside the
// backend binary.
unsafe extern "C-unwind" {
    fn enable_timeout_after(id: std::ffi::c_int, delay_ms: std::ffi::c_int);
    fn disable_timeout(id: std::ffi::c_int, keep_indicator: bool);
    fn get_timeout_active(id: std::ffi::c_int) -> bool;
}

/// `TimeoutId::STATEMENT_TIMEOUT` from
/// [`src/include/utils/timeout.h`](../../../../../postgres/src/include/utils/timeout.h)
/// at PG 18. The enum is documented as stable across PG majors
/// for the predefined entries; STATEMENT_TIMEOUT is the 4th
/// (index 3) member.
const STATEMENT_TIMEOUT: std::ffi::c_int = 3;

/// RAII guard that arms `STATEMENT_TIMEOUT` for the lifetime of
/// the guard and disables it on drop. Mirrors vanilla
/// [`enable_statement_timeout` in postgres.c:5208-5223](../../../../../postgres/src/backend/tcop/postgres.c#L5208-L5223)
/// plus the matching
/// [`disable_statement_timeout`](../../../../../postgres/src/backend/tcop/postgres.c#L5230-L5236).
///
/// Drop runs on every exit path (Ok / typed-Err / PG-ERROR
/// panic caught by `with_xact` / `with_spi`'s `catch_unwind`),
/// so a fired statement-timeout ERROR correctly disarms the
/// timer before the panic propagates out.
///
/// **Activation policy.** Only arms when:
/// - `pg_sys::StatementTimeout > 0` (the GUC is set), AND
/// - the timer is not already active (matches vanilla's "don't
///   restart on re-entry" comment at postgres.c:2811 — restarting
///   would skew the deadline forward and is observably wrong for
///   multi-statement bodies).
///
/// The guard remembers whether *it* armed the timer
/// (`armed_by_us`); only then does drop disable. This preserves
/// the "outer code may have armed it for its own purposes"
/// invariant, though under pg_transport's current slot-bgworker
/// shape no outer code does so.
///
/// Without this guard, `statement_timeout` is silently inactive
/// for every pg_transport-served query — an operational hazard
/// for any deployment that relies on the GUC to bound runaway
/// queries.
pub(crate) struct StatementTimeoutGuard {
    armed_by_us: bool,
}

impl StatementTimeoutGuard {
    /// Arm `STATEMENT_TIMEOUT` if the GUC is set and the timer
    /// isn't already running. Returns a guard whose `Drop` will
    /// disarm only if this call did the arming.
    ///
    /// # Safety
    ///
    /// Must be called from a PG backend / bgworker (the only
    /// place the timeout API is meaningful) and inside an active
    /// transaction (matches vanilla's `Assert(xact_started)`).
    pub(crate) unsafe fn install() -> Self {
        // SAFETY: backend-context globals + extern fns. The
        // GUC read is a plain memory load; the FFI calls are
        // PG server-API entry points.
        let armed_by_us = unsafe {
            let timeout_ms = pg_sys::StatementTimeout;
            if timeout_ms <= 0 {
                false
            } else if get_timeout_active(STATEMENT_TIMEOUT) {
                // Already armed by outer code; leave alone so
                // their drop doesn't see us steal it. Drop of
                // this guard will be a no-op.
                false
            } else {
                enable_timeout_after(STATEMENT_TIMEOUT, timeout_ms);
                true
            }
        };
        StatementTimeoutGuard { armed_by_us }
    }
}

impl Drop for StatementTimeoutGuard {
    fn drop(&mut self) {
        if self.armed_by_us {
            // SAFETY: matches install(). `keep_indicator=false`
            // clears the "fired" flag so a subsequent
            // get_timeout_indicator from outer code doesn't see
            // our expiry.
            unsafe { disable_timeout(STATEMENT_TIMEOUT, false) };
        }
    }
}

/// Crate-internal probe for whether `STATEMENT_TIMEOUT` is
/// currently armed in this backend. Wraps the manually-declared
/// `get_timeout_active` FFI symbol with a safe Rust signature
/// so test helpers don't need to re-declare the extern block.
///
/// Used by the `statement_timeout` regression test: a SQL
/// callable runs inside `PortalRun` and asks "did
/// [`StatementTimeoutGuard`] actually arm the timer for this
/// query?" — without needing to provoke an actual cancel (which
/// would corrupt the pg_test harness's outer transaction).
#[cfg(any(test, feature = "pg_test"))]
pub(crate) fn is_statement_timeout_active() -> bool {
    // SAFETY: get_timeout_active is a side-effect-free read of
    // backend-local timer state; safe to call from any backend
    // context that has an active TimerCallbackContext (i.e. any
    // bgworker or backend).
    unsafe { get_timeout_active(STATEMENT_TIMEOUT) }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Test-only helpers shared across backend regression tests. Used
/// to assert the [`DebugQueryGuard`] is installed at every backend
/// entry point by observing what an `ExecutorStart_hook` extension
/// (e.g. `pg_stat_statements`, `auto_explain`) would see when our
/// query reaches the executor.
#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod test_helpers {
    use super::*;

    /// Snapshot of state visible to an `ExecutorStart_hook`
    /// callback at the moment PG enters `ExecutorStart` for our
    /// query.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct CapturedExecutorStart {
        /// `QueryDesc->sourceText` PG passed to the executor.
        /// Pg_transport sets this to the originating SQL via
        /// `PortalDefineQuery(... query_string ...)` — the
        /// simple-query path passes the raw user SQL; the
        /// extended direct path passes the stub
        /// `"direct-path statement"` literal. Callers that care
        /// about the distinction assert on this field; callers
        /// that only care about the `debug_query_string`
        /// invariant ignore it.
        pub portal_source_text: Option<String>,
        /// `pg_sys::debug_query_string` value at hook entry.
        /// This is the global `pg_stat_statements`'s
        /// `pgss_ExecutorStart` and `auto_explain`'s
        /// `explain_ExecutorStart` read to bucket / annotate the
        /// query. Must equal the originating SQL for downstream
        /// attribution to work.
        pub debug_query_string_at_hook: Option<String>,
    }

    static CAPTURED: std::sync::Mutex<Option<CapturedExecutorStart>> = std::sync::Mutex::new(None);

    /// `ExecutorStart_hook` that captures the first invocation's
    /// `(QueryDesc->sourceText, debug_query_string)` pair, then
    /// chains to `standard_ExecutorStart`. Only the first event
    /// is captured so any teardown / harness-driven executor
    /// starts after the body returns don't clobber the test's
    /// observation.
    unsafe extern "C-unwind" fn capturing_hook(
        query_desc: *mut pg_sys::QueryDesc,
        eflags: std::ffi::c_int,
    ) {
        {
            let mut slot = CAPTURED.lock().unwrap();
            if slot.is_none() {
                // SAFETY: PG guarantees `query_desc` is valid for
                // the hook's lifetime; `sourceText` is a const
                // C-string-or-NULL; `debug_query_string` is
                // similarly const-C-string-or-NULL.
                let portal_source_text = unsafe {
                    let p = (*query_desc).sourceText;
                    if p.is_null() {
                        None
                    } else {
                        Some(CStr::from_ptr(p).to_string_lossy().into_owned())
                    }
                };
                let debug_query_string_at_hook = unsafe {
                    let p = pg_sys::debug_query_string;
                    if p.is_null() {
                        None
                    } else {
                        Some(CStr::from_ptr(p).to_string_lossy().into_owned())
                    }
                };
                *slot = Some(CapturedExecutorStart {
                    portal_source_text,
                    debug_query_string_at_hook,
                });
            }
        }
        // SAFETY: standard_ExecutorStart is the exported default
        // implementation; safe to call from any
        // ExecutorStart_hook wrapper.
        unsafe { pg_sys::standard_ExecutorStart(query_desc, eflags) };
    }

    /// Run `body`, capturing the first `ExecutorStart_hook`
    /// invocation that fires during it. Returns the body's
    /// result and the captured snapshot.
    ///
    /// Wraps install + reset + body + restore + take into one
    /// call so backend regression tests don't need to repeat
    /// the boilerplate (or the `static`-Mutex pattern). The
    /// restore runs unconditionally before this function
    /// returns, even if `body` panics — `body` is wrapped in
    /// `catch_unwind` and the panic is resumed after restore.
    ///
    /// Not safe for concurrent use across tests (single global
    /// `CAPTURED` slot, single global hook pointer). Pgrx's
    /// pg_test framework runs tests one at a time per backend,
    /// so this is fine in practice.
    pub(crate) fn capture_executor_start<F, R>(body: F) -> (R, CapturedExecutorStart)
    where
        F: FnOnce() -> R,
    {
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

        *CAPTURED.lock().unwrap() = None;

        // SAFETY: ExecutorStart_hook is a writable function-
        // pointer global. We install + restore in a strict pair
        // bracketing `body`. Restore runs even if `body` panics.
        let prev_hook = unsafe { pg_sys::ExecutorStart_hook };
        unsafe {
            pg_sys::ExecutorStart_hook = Some(capturing_hook);
        }

        let outcome = catch_unwind(AssertUnwindSafe(body));

        unsafe {
            pg_sys::ExecutorStart_hook = prev_hook;
        }

        let captured = CAPTURED.lock().unwrap().take().unwrap_or_default();

        match outcome {
            Ok(value) => (value, captured),
            Err(payload) => resume_unwind(payload),
        }
    }

    // -----------------------------------------------------------------------
    // post_parse_analyze_hook probe — per-statement query_id reset
    // -----------------------------------------------------------------------

    /// Sentinel base used by
    /// [`capture_query_id_at_parse_analyze`]'s
    /// fake-`pg_stat_statements` hook. Each hook invocation
    /// writes `QUERY_ID_SENTINEL_BASE + n` to `st_query_id` via
    /// `pgstat_report_query_id(..., false)` (n = 1-based
    /// invocation index), so callers can compose the per-stmt
    /// sentinel for failure messages and pin which statement
    /// leaked an id forward.
    pub(crate) const QUERY_ID_SENTINEL_BASE: i64 = 0x0BAD_BEEF_DEAD_0000;

    static OBSERVED_QUERY_IDS: std::sync::Mutex<Vec<i64>> = std::sync::Mutex::new(Vec::new());

    /// `post_parse_analyze_hook` that mimics what
    /// `pg_stat_statements` does for every parsed statement:
    ///
    /// 1. *Records* `pgstat_get_my_query_id()` at hook entry —
    ///    the value PG hands us, i.e. whatever the previous
    ///    statement left behind.
    /// 2. *Writes* a fresh sentinel via
    ///    `pgstat_report_query_id(SENTINEL_BASE + n, false)` —
    ///    the `false` mirrors the real extension's call shape
    ///    (`pg_stat_statements` never uses `force=true`).
    ///
    /// The recorded sequence is what the per-statement query_id
    /// reset regression tests assert on: vanilla's per-statement
    /// reset makes every invocation observe `0` at entry; without
    /// the reset, statement N+1's invocation observes statement
    /// N's sentinel.
    unsafe extern "C-unwind" fn fake_pgss_post_parse_analyze_hook(
        _pstate: *mut pg_sys::ParseState,
        _query: *mut pg_sys::Query,
        _jstate: *mut pg_sys::JumbleState,
    ) {
        // SAFETY: pgstat_get_my_query_id is a backend-local
        // PgBackendStatus read; always safe after pgstat init.
        let observed_at_entry = unsafe { pg_sys::pgstat_get_my_query_id() };
        let mut log = OBSERVED_QUERY_IDS.lock().unwrap();
        log.push(observed_at_entry);
        let nth = log.len() as i64;
        drop(log);
        // SAFETY: pgstat_report_query_id is the documented
        // installer; `force=false` mirrors real
        // `pg_stat_statements` behaviour (and is the call shape
        // the per-statement reset has to clear for to work).
        unsafe {
            pg_sys::pgstat_report_query_id(QUERY_ID_SENTINEL_BASE + nth, false);
        }
    }

    /// Run `body` with a `post_parse_analyze_hook` installed
    /// that mimics `pg_stat_statements` (records `st_query_id`
    /// at hook entry, then writes a per-invocation sentinel via
    /// `pgstat_report_query_id(SENTINEL+n, false)`). Returns
    /// the body's result and the vector of `st_query_id` values
    /// observed at each hook entry, one per parsed statement in
    /// invocation order.
    ///
    /// Used by the per-statement query_id reset regression
    /// tests on both the
    /// direct and SPI backends. See those tests'
    /// doc-comments for the full mechanism write-up — why a
    /// `post_parse_analyze_hook`-based probe is the only way
    /// to expose the gap, why the leak is invisible for
    /// single-statement `'Q'` bodies, and how the `(_, false)`
    /// call shape mirrors `pg_stat_statements`.
    ///
    /// Restores the previous hook and clears any sentinel left
    /// in `st_query_id` / `st_plan_id` before returning, even
    /// if `body` panics — `body` is wrapped in `catch_unwind`
    /// and the panic is resumed after restore.
    ///
    /// Not safe for concurrent use across tests (single global
    /// `OBSERVED_QUERY_IDS`, single global hook pointer). Pgrx's
    /// pg_test framework runs tests one at a time per backend,
    /// so this is fine in practice.
    pub(crate) fn capture_query_id_at_parse_analyze<F, R>(body: F) -> (R, Vec<i64>)
    where
        F: FnOnce() -> R,
    {
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

        OBSERVED_QUERY_IDS.lock().unwrap().clear();

        // SAFETY: post_parse_analyze_hook is a writable function-
        // pointer global. Strict install / restore pair around
        // body. Restore runs even if body panics.
        let prev_hook = unsafe { pg_sys::post_parse_analyze_hook };
        unsafe {
            pg_sys::post_parse_analyze_hook = Some(fake_pgss_post_parse_analyze_hook);
        }

        let outcome = catch_unwind(AssertUnwindSafe(body));

        // Restore the hook FIRST so an assertion failure in the
        // caller can't leave the hook installed for subsequent
        // tests.
        unsafe {
            pg_sys::post_parse_analyze_hook = prev_hook;
        }
        // Belt-and-braces clear any sentinel we left behind so
        // subsequent pg_tests in this backend frame don't
        // observe our injected ids.
        //
        // SAFETY: both reporters are pure writes to MyBEEntry;
        // `force=true` makes them unconditional.
        unsafe {
            pg_sys::pgstat_report_query_id(0, true);
            pg_sys::pgstat_report_plan_id(0, true);
        }

        let observations = std::mem::take(&mut *OBSERVED_QUERY_IDS.lock().unwrap());

        match outcome {
            Ok(value) => (value, observations),
            Err(payload) => resume_unwind(payload),
        }
    }
}
