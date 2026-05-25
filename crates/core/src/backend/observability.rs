//! Per-query observability plumbing — pins
//! [`pg_sys::debug_query_string`] and reports `STATE_RUNNING` /
//! `STATE_IDLE` to pgstat across a query body, matching what
//! vanilla `exec_simple_query` does at
//! [postgres.c:1046-1048](../../../../../postgres/src/backend/tcop/postgres.c#L1046-L1048).
//!
//! Audit: [`docs/design/reviews/2026-05-24-pg-code-findings.md`](../../../../docs/design/reviews/2026-05-24-pg-code-findings.md)
//! §6 item 5.
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
}
