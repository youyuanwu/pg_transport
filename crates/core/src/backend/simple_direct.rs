//! Simple-query direct backend — `Q` message → `parse_and_keep`
//! → per-statement `pg_analyze_and_rewrite_fixedparams` →
//! `pg_plan_queries` → `Portal*` + `WireDestReceiver`. No SPI.
//!
//! Design: [deferred/simple-query-direct-path.md](../../../../docs/design/deferred/simple-query-direct-path.md).
//!
//! Selected when `pg_transport.execution_backend = 'direct'`.
//! Default backend remains `spi`
//! ([`super::spi_bridge::execute_simple_query`]); both backends
//! ship side-by-side indefinitely.
//!
//! ## Data flow
//!
//! ```text
//! execute_simple_query_direct(query)
//!   ├─ parse_and_keep                         (one raw_parser pass)
//!   │    └─ ParsedQuery { statements, parse_ctx, sql_cstr }
//!   └─ for each statement:
//!        with_xact(|ctx| run_one_direct(ctx, raw_stmt, &sql_cstr))
//!         ├─ CreateCommandTag(raw_stmt)
//!         ├─ pg_analyze_and_rewrite_fixedparams  (→ TopTransactionContext)
//!         ├─ pg_plan_queries                      (→ TopTransactionContext)
//!         ├─ Portal::create_anonymous → define → start → run(WireDestReceiver)
//!         └─ Portal drops (PortalDrop)
//! ```
//!
//! `with_xact`'s `StartTransactionCommand` creates a fresh
//! `TopTransactionContext` and switches `CurrentMemoryContext`
//! into it; `CommitTransactionCommand` deletes it. Per-statement
//! analyze/plan transients land in `TopTransactionContext` and
//! are freed automatically at end-of-statement. This matches
//! vanilla `exec_simple_query`'s memory discipline — no
//! framework-owned per-statement context is needed.

use std::ffi::{CStr, CString};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use pgrx::PgTryBuilder;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use super::dest_receiver::{
    ColumnEncoder, WireDestReceiver, command_tag_name, schema_and_encoders_text,
};
use super::executor::{ParamList, Portal, ScopedMemoryContext, TupleDescRef, XactCtx, with_xact};
use super::spi::{caught_error_to_pgwire, generic_error};

// ---------------------------------------------------------------------------
// parse_and_keep — single raw_parser pass; keeps parsetrees alive
// ---------------------------------------------------------------------------

/// Parsed multi-statement `'Q'` body. Owns the
/// [`ScopedMemoryContext`] the [`pg_sys::RawStmt`] pointers live
/// in, plus the single [`CString`] copy of the input query that
/// PG's `*_fixedparams` / `pg_plan_queries` / `PortalDefineQuery`
/// helpers all want as a raw `*const c_char`.
///
/// **Pointer-lifetime invariant.** Each `*mut pg_sys::RawStmt`
/// in `statements` is valid for the lifetime of `parse_ctx`.
/// Dropping `ParsedQuery` (or `parse_ctx`) invalidates every
/// pointer. The `'q` lifetime ties the slice text to the input
/// query string only; do not extract a raw pointer past
/// `ParsedQuery`'s scope.
pub(crate) struct ParsedQuery<'q> {
    /// Per-statement `(text-slice, raw RawStmt pointer)` pairs
    /// in source order. Whitespace/comment-only segments are
    /// retained so the caller can `text.trim().is_empty()`-skip
    /// them (matches PG's `'Q'` semantics).
    pub(crate) statements: Vec<(&'q str, *mut pg_sys::RawStmt)>,
    /// MemoryContext that owns every `RawStmt` in `statements`.
    /// Held purely for its `Drop` side effect: dropping
    /// `ParsedQuery` deletes the context, freeing the parsetree.
    /// Per-statement analyze/plan transients land in PG's
    /// `TopTransactionContext` via [`with_xact`] and don't touch
    /// this context.
    #[allow(dead_code)]
    pub(crate) parse_ctx: ScopedMemoryContext,
    /// NUL-terminated copy of the original query string, used by
    /// `pg_analyze_and_rewrite_fixedparams` /
    /// `PortalDefineQuery` as the source-text pointer they store
    /// for error-reporting. Borrowed by `run_one_direct` per
    /// statement to avoid re-allocating per `'Q'`.
    pub(crate) sql_cstr: CString,
}

/// One raw-parser pass over `query`. Keeps the parsetree alive
/// in a fresh `AllocSetContext` (`pg_transport_parse_ctx`) and
/// returns owning [`ParsedQuery`].
///
/// `pg_sys::raw_parser` allocates the `List *RawStmt` and each
/// inner `RawStmt`/`Node` into `CurrentMemoryContext`; we switch
/// to `parse_ctx` for the duration so they all live in there.
///
/// PG ERRORs (syntax errors etc.) are caught via `PgTryBuilder`
/// and converted to `PgWireError` so the caller gets a typed
/// failure rather than a longjmp.
#[allow(clippy::result_large_err)] // CaughtError is 232 B and lives only
// across this function; boxing would gain nothing.
pub(crate) fn parse_and_keep(query: &str) -> PgWireResult<ParsedQuery<'_>> {
    let cstr = CString::new(query)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;

    // Create parse_ctx as a child of whatever the current
    // MessageContext is (typically the bgworker top-level loop
    // context). It outlives this function via the returned
    // ParsedQuery.
    let parse_ctx = ScopedMemoryContext::new(c"pg_transport_parse_ctx");

    let outcome: Result<Vec<(usize, usize, *mut pg_sys::RawStmt)>, CaughtError> =
        PgTryBuilder::new(AssertUnwindSafe(|| {
            // SAFETY: switch_to + raw_parser + extraction inside
            // the PG-error try block. The guard restores
            // CurrentMemoryContext before we leave this closure
            // even on the happy path.
            let _guard = parse_ctx.switch_to();
            let list = unsafe {
                pg_sys::raw_parser(cstr.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT)
            };
            // SAFETY: raw_parser's contract — null or *mut List of
            // *mut RawStmt elements.
            let spans = unsafe { extract_raw_stmt_spans(list, query.len()) };
            drop(_guard);
            Ok(spans)
        }))
        .catch_others(Err)
        .execute();

    let raw_spans = match outcome {
        Ok(v) => v,
        Err(caught) => {
            // parse_ctx will be deleted by its Drop, taking any
            // partially-allocated parsetrees with it.
            return Err(caught_error_to_pgwire(&caught));
        }
    };

    let statements: Vec<(&str, *mut pg_sys::RawStmt)> = raw_spans
        .into_iter()
        .map(|(loc, len, raw_stmt)| (&query[loc..loc + len], raw_stmt))
        .collect();

    Ok(ParsedQuery {
        statements,
        parse_ctx,
        sql_cstr: cstr,
    })
}

/// Walk the `List*` returned by `raw_parser`, extracting one
/// `(stmt_location, stmt_len, *mut RawStmt)` per element.
/// Resolves `stmt_len == 0` (PG's "to end of input" sentinel)
/// against `total_len`.
///
/// Mirrors [`super::spi_bridge::extract_statement_spans`] but
/// returns the raw `*mut RawStmt` pointer (which the direct path
/// needs for `pg_analyze_and_rewrite_fixedparams`) instead of a
/// classification.
///
/// # Safety
///
/// `list` must be either null or a valid `*mut List` of
/// `*mut RawStmt` elements (the documented `raw_parser` return
/// shape).
unsafe fn extract_raw_stmt_spans(
    list: *mut pg_sys::List,
    total_len: usize,
) -> Vec<(usize, usize, *mut pg_sys::RawStmt)> {
    if list.is_null() {
        return Vec::new();
    }

    let length = unsafe { (*list).length } as usize;
    let elements = unsafe { (*list).elements };
    let mut out = Vec::with_capacity(length);

    for i in 0..length {
        // SAFETY: elements[0..length] are valid ListCells per the
        // List invariant; ptr_value union variant holds the
        // RawStmt pointer for node-pointer lists.
        let cell = unsafe { elements.add(i) };
        let raw_stmt = unsafe { (*cell).ptr_value } as *mut pg_sys::RawStmt;
        if raw_stmt.is_null() {
            continue;
        }

        let loc = unsafe { (*raw_stmt).stmt_location } as i64;
        let len = unsafe { (*raw_stmt).stmt_len } as i64;
        let (slice_loc, slice_len) = if len == 0 {
            let loc_usize = loc.max(0) as usize;
            (loc_usize, total_len.saturating_sub(loc_usize))
        } else {
            (loc.max(0) as usize, len as usize)
        };
        out.push((slice_loc, slice_len, raw_stmt));
    }
    out
}

// ---------------------------------------------------------------------------
// execute_simple_query_direct — top-level entry, mirrors execute_simple_query
// ---------------------------------------------------------------------------

/// Run a simple-query string through the direct backend and
/// shape the result into pgwire `Response`s.
///
/// Same semantics as [`super::spi_bridge::execute_simple_query`]:
/// * Per-statement xact bracketing via [`with_xact`].
/// * First-error returns `Err`; pgwire emits `ErrorResponse` +
///   `ReadyForQuery` from the `SimpleQueryHandler` layer.
/// * Empty / whitespace-only / comment-only segments are skipped
///   (PG drops them silently).
/// * Utility statements (`SET`, `CREATE`, xact-control, …)
///   route through `PortalRun` → `ProcessUtility`. No special
///   xact-control intercept: PG handles them natively.
pub fn execute_simple_query_direct(query: &str) -> PgWireResult<Vec<Response>> {
    let parsed = parse_and_keep(query)?;

    let mut responses = Vec::with_capacity(parsed.statements.len());
    for (text, raw_stmt) in &parsed.statements {
        if text.trim().is_empty() {
            continue;
        }
        let raw_stmt = *raw_stmt;
        // Borrow sql_cstr for the closure's lifetime; raw_stmt
        // is alive as long as parsed.parse_ctx is, and with_xact
        // runs the closure to completion synchronously.
        let sql_cstr_ref: &CStr = parsed.sql_cstr.as_c_str();
        let resp = with_xact(|ctx| run_one_direct(ctx, raw_stmt, sql_cstr_ref))?;
        responses.push(resp);
    }
    // parsed drops here; parse_ctx deletes the MemoryContext that
    // owned every raw_stmt pointer, and sql_cstr is freed.
    Ok(responses)
}

// ---------------------------------------------------------------------------
// run_one_direct — per-statement worker
// ---------------------------------------------------------------------------

/// Per-statement worker. Analyze + plan + portal define/start/run
/// /drop, encoding rows inline via `WireDestReceiver`.
///
/// Mirrors PG's `exec_simple_query` shape, with two differences:
/// 1. We pass a NULL `cached_plan` to `PortalDefineQuery` — same
///    as `exec_simple_query` (simple-query plans are one-shot).
/// 2. The `DestReceiver` writes encoded `DataRow` frames into a
///    Rust-side `Vec<DataRow>`; pgwire consumes that vec after
///    we return.
///
/// Memory-context discipline: relies on [`with_xact`]'s
/// `StartTransactionCommand` having switched
/// `CurrentMemoryContext` to a fresh `TopTransactionContext`.
/// Everything `pg_analyze_and_rewrite_fixedparams` and
/// `pg_plan_queries` `palloc` lands there;
/// `CommitTransactionCommand` (called by `with_xact` on the way
/// out) deletes the context, freeing those transients without
/// an explicit per-statement `AllocSetContextCreateInternal` +
/// `MemoryContextDelete` pair. `PortalDefineQuery` copies what
/// it needs onto the portal's own independent context, so
/// `PortalRun` is unaffected by the TopTransactionContext
/// teardown.
fn run_one_direct(
    ctx: &XactCtx,
    raw_stmt: *mut pg_sys::RawStmt,
    sql_cstr: &CStr,
) -> PgWireResult<Response> {
    // 1. Command tag from the raw parsetree. Matches
    //    exec_simple_query, which calls CreateCommandTag before
    //    analyze so it survives even if analyze raises.
    // SAFETY: CreateCommandTag is a pure walk of the parsetree;
    // raw_stmt is alive in parse_ctx for the whole call.
    let command_tag = unsafe { pg_sys::CreateCommandTag((*raw_stmt).stmt) };

    // Move the unsafe block out of the inner scope so the
    // captured (schema, encoders, qc, data_rows) tuple is owned
    // by safe Rust after PortalDrop ran (Portal drops at end of
    // the unsafe block).
    let (schema, encoders, qc, data_rows) = unsafe {
        // 2. Analyze + rewrite (no client-side type hints in
        //    simple-query; pass NULL/0/NULL). Output querytree
        //    list is palloc'd in CurrentMemoryContext =
        //    TopTransactionContext (set by with_xact's
        //    StartTransactionCommand).
        let query_list = pg_sys::pg_analyze_and_rewrite_fixedparams(
            raw_stmt,
            sql_cstr.as_ptr(),
            std::ptr::null(),     // paramTypes
            0,                    // numParams
            std::ptr::null_mut(), // queryEnv
        );

        // 3. Plan. Same call shape as exec_simple_query. Output
        //    PlannedStmt list also lands in TopTransactionContext.
        let plan_list = pg_sys::pg_plan_queries(
            query_list,
            sql_cstr.as_ptr(),
            pg_sys::CURSOR_OPT_PARALLEL_OK as i32,
            std::ptr::null_mut(), // boundParams
        );

        // 4. Portal lifecycle via the existing wrapper. Portal's
        //    own MemoryContext is independent of
        //    TopTransactionContext; PortalDefineQuery copies
        //    what it needs onto it.
        let portal = Portal::create_anonymous(ctx);
        portal.define(sql_cstr, command_tag, plan_list, std::ptr::null_mut());

        // 5. Start. with_xact already PushActiveSnapshot'd;
        //    PortalStart records it on the QueryDesc and
        //    populates portal->tupDesc as a side effect (we read
        //    it next). PortalRun later pushes its own active
        //    snapshot per scan internally for SELECT — nested
        //    push/pop is fine (matches the extended-query direct
        //    backend).
        portal.start(&ParamList::empty(), 0, pg_sys::GetActiveSnapshot());

        // 6. Schema + per-column encoders from the now-populated
        //    portal->tupDesc. NULL tupdesc → utility / no-tuple
        //    statement → empty schema, no encoders.
        let tupdesc = TupleDescRef::from_raw((*portal.as_ptr()).tupDesc);
        let (schema, encoders): (Vec<_>, Vec<ColumnEncoder>) = match tupdesc {
            Some(td) => schema_and_encoders_text(&td),
            None => (Vec::new(), Vec::new()),
        };

        // 7. Execute. Per-row encoding happens inside the
        //    receiver's receiveSlot callback, which appends to
        //    `data_rows`. Pre-size to a modest default so a typical
        //    multi-row SELECT doesn't pay the 0→4→8→16 geometric
        //    growth chain `Vec::new` would force.
        const DATA_ROWS_DEFAULT_CAP: usize = 16;
        let mut data_rows: Vec<DataRow> = Vec::with_capacity(DATA_ROWS_DEFAULT_CAP);
        let ncols = schema.len() as i16;
        let mut dest = WireDestReceiver::new(&encoders, &mut data_rows, ncols);
        let mut qc: pg_sys::QueryCompletion = Default::default();
        pg_sys::InitializeQueryCompletion(&mut qc);
        let _completed = portal.run(
            i64::MAX, // FETCH_ALL — PortalRun treats count<=0 as NoMovementScanDirection
            true,     // is_top_level
            dest.as_dest_receiver(),
            dest.as_dest_receiver(),
            &mut qc,
        );

        // 8. portal drops here (PortalDrop on scope exit),
        //    tearing down executor state and freeing the portal's
        //    MemoryContext. dest also drops; its rows/encoders
        //    borrow ends.
        (schema, encoders, qc, data_rows)
    };
    drop(encoders); // no longer referenced; explicit for clarity

    // 9. Shape into pgwire Response. data_rows holds encoded
    //    wire bytes; no SPI_tuptable step.
    let tag_name = command_tag_name(qc.commandTag);
    if schema.is_empty() {
        let mut tag = Tag::new(tag_name);
        if unsafe { pg_sys::command_tag_display_rowcount(qc.commandTag) } {
            tag = tag.with_rows(qc.nprocessed as usize);
        }
        Ok(Response::Execution(tag))
    } else {
        let schema_arc = Arc::new(schema);
        let row_stream = stream::iter(data_rows).map(Ok);
        let mut response = QueryResponse::new(schema_arc, row_stream);
        response.set_command_tag(tag_name);
        Ok(Response::Query(response))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use bytes::Buf;
    use pgrx::pg_test;

    /// Empty / whitespace-only / comment-only inputs parse to
    /// zero executable statements and yield an empty Vec — pgwire
    /// renders that as EmptyQueryResponse + ReadyForQuery.
    #[pg_test]
    fn pg_simple_direct_empty_query() {
        let r = execute_simple_query_direct("").expect("empty query should succeed");
        assert!(r.is_empty(), "expected no responses for empty body");
        let r = execute_simple_query_direct("   \n\t  ").expect("whitespace should succeed");
        assert!(r.is_empty(), "expected no responses for whitespace body");
        let r = execute_simple_query_direct("-- just a comment\n").expect("comment should succeed");
        assert!(r.is_empty(), "expected no responses for comment-only body");
    }

    /// Single SELECT returns one Query response with the expected
    /// row encoded as wire bytes.
    #[pg_test]
    fn pg_simple_direct_select_one_row() {
        let r = execute_simple_query_direct("SELECT 1 AS n").expect("select should succeed");
        assert_eq!(r.len(), 1, "expected exactly one response");
        let mut q = match r.into_iter().next().unwrap() {
            Response::Query(q) => q,
            other => panic!("expected Query, got {other:?}"),
        };
        let maybe_row = futures::executor::block_on(async { q.data_rows().next().await });
        let row = maybe_row
            .expect("expected one row")
            .expect("row item should be Ok");
        assert_eq!(row.field_count, 1);
        let mut data = row.data;
        let len = data.get_i32();
        assert!(len > 0);
        let cell = data.split_to(len as usize);
        assert_eq!(std::str::from_utf8(&cell).unwrap(), "1");
    }

    /// Multi-statement body: each statement gets its own Response
    /// in source order.
    #[pg_test]
    fn pg_simple_direct_multi_statement() {
        let r =
            execute_simple_query_direct("SELECT 1; SELECT 2").expect("multi-stmt should succeed");
        assert_eq!(r.len(), 2, "expected two responses");
        for resp in r {
            match resp {
                Response::Query(_) => {}
                other => panic!("expected Query, got {other:?}"),
            }
        }
    }

    /// Utility statement (`SET`) routes through PortalRun →
    /// ProcessUtility and returns an Execution response, not a
    /// Query.
    #[pg_test]
    fn pg_simple_direct_utility_set() {
        let r = execute_simple_query_direct("SET client_min_messages = 'warning'")
            .expect("SET should succeed");
        assert_eq!(r.len(), 1);
        match r.into_iter().next().unwrap() {
            Response::Execution(_) => {}
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    /// xact-control via raw `BEGIN`/`COMMIT` — direct path does
    /// not need the SPI-bridge xact-intercept, since PortalRun
    /// routes TransactionStmt through ProcessUtility natively.
    ///
    /// **Tested in e2e only** (see
    /// `crates/e2e/tests/basic.rs::simple_direct_xact_control_*`).
    /// pg_test wraps each unit test in an outer `START TRANSACTION`;
    /// running `BEGIN` inside that triggers a `WARNING: there is
    /// already a transaction in progress`, mutates xact state in
    /// a way that confuses pg_test's commit-on-success teardown,
    /// and segfaults with a snapshot leak. Vanilla PG hits the
    /// same WARNING path; e2e tests against a real connection
    /// have no outer xact and exercise this cleanly.
    #[cfg(any())] // intentionally disabled — see note above
    #[pg_test]
    fn pg_simple_direct_xact_control() {}

    /// Syntax error → PgWireError. Subsequent statements are
    /// not executed (matches PG's 'Q' semantics).
    #[pg_test]
    fn pg_simple_direct_syntax_error_stops_batch() {
        let err = execute_simple_query_direct("SELECTT 1; SELECT 2").expect_err("must fail");
        let msg = format!("{err:?}");
        assert!(
            msg.to_ascii_lowercase().contains("syntax")
                || msg.to_ascii_lowercase().contains("selectt"),
            "expected syntax error, got: {msg}"
        );
    }
}
