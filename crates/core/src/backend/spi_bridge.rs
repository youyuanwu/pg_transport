//! SPI bridge — convert wire-layer simple queries into rows by going
//! through pgrx's [`pgrx::Spi`].
//!
//! Phase 4b: text-format only. Every column gets serialised via PG's
//! `SPI_getvalue`, which under the hood calls the type's registered
//! `typoutput` function — so we transparently cover every type the
//! cluster knows how to print.
//!
//! PG ERRORs raised inside the SPI call (re-raised as Rust panics
//! by pgrx's cee-scape wrapper per [Q24](../../../../docs/design/roadmap.md))
//! are caught and converted into pgwire `ErrorResponse` frames.
//!
//! Multi-statement simple-query bodies are split via PG's own
//! [`pg_sys::raw_parser`] — the same in-process bison-generated
//! parser SPI uses internally, so dollar-quoting, string literals,
//! comments, and every grammar variant are by-definition handled
//! correctly. The same parse pass yields `TransactionStmt` nodes,
//! which we classify to route `BEGIN` / `COMMIT` / `ROLLBACK` (and
//! all their mode-list variants) around SPI's atomic-mode rejection
//! through PG's xact-block API. Resolves Q25 in
//! [roadmap.md](../../../../docs/design/roadmap.md).

use std::ffi::{CStr, CString};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::pg_sys::{self};
use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use super::dest_receiver::ColumnEncoder;
use super::observability::{DebugQueryGuard, StatementTimeoutGuard};
use super::spi::{
    SpiCtx, SpiTuples, caught_error_to_pgwire, generic_error, panic_to_pgwire, spi_rc_error,
    with_spi,
};

/// Run a simple-query string through SPI and shape the result into
/// pgwire `Response`s.
///
/// * Statements that produce no tuples (e.g. `SET …`, `CREATE TABLE …`)
///   yield `Response::Execution(Tag("OK").with_rows(N))`.
/// * SELECT-like statements yield `Response::Query` with a
///   text-format `RowDescription` derived from the SPI tupdesc and
///   `DataRow` frames for every materialised row.
/// * Transaction-control statements (`BEGIN`, `COMMIT`, `ROLLBACK`,
///   and the full PG `TransactionStmt` variant set including
///   transaction-mode lists like `BEGIN ISOLATION LEVEL SERIALIZABLE`)
///   are intercepted *before* SPI sees them and routed through PG's
///   xact-block API directly — SPI in atomic mode rejects xact-
///   control commands with `SPI_ERROR_TRANSACTION`, so we never let
///   them reach `SPI_execute`. See [`parse_and_classify`].
/// * Multi-statement bodies (`SELECT 1; SELECT 2` in one `'Q'`) are
///   split by the same parse pass and each piece runs through the
///   per-statement path independently. On first error the batch
///   stops — matches real PG's `'Q'` semantics.
/// * Empty / whitespace-only / comment-only bodies return an empty
///   `Vec<Response>`; pgwire's SimpleQueryHandler renders that as a
///   single `EmptyQueryResponse` + `ReadyForQuery`.
///
/// **Multi-statement atomicity.** When the `'Q'` body contains more
/// than one executable statement and none of them is a recognised
/// xact-control (BEGIN / COMMIT / ROLLBACK), the batch is wrapped
/// in `BeginImplicitTransactionBlock` /
/// `EndImplicitTransactionBlock` so the whole batch commits or
/// rolls back atomically — matching vanilla `exec_simple_query`
/// ([postgres.c:1097 + :1167-1170 + :1310](../../../../../postgres/src/backend/tcop/postgres.c#L1097)).
/// Mixed batches containing xact-control fall back to the per-
/// statement bracket path so [`handle_xact_control`]'s xact-block
/// API calls don't fight an outer implicit block. Closes review
/// 2026-05-24 §6 item 2 for the SPI backend (parity with
/// [`super::simple_direct::execute_with_implicit_block`]).
pub fn execute_simple_query(query: &str) -> PgWireResult<Vec<Response>> {
    // Pin per-query observability (debug_query_string + pgstat
    // STATE_RUNNING) for the lifetime of the query body. The
    // guard drops on every exit path (Ok / Err / panic), restoring
    // the previous global and transitioning pgstat to STATE_IDLE.
    // Matches vanilla `exec_simple_query` at postgres.c:1046-1048;
    // closes review 2026-05-24 §6 item 5 for the SPI backend.
    //
    // The CString is owned here for the whole function so the
    // guard's `'a` lifetime covers the entire query body. A second
    // CString is materialised inside `parse_and_classify` and
    // again inside the per-statement SPI path — both are tiny
    // allocations relative to the query work, not worth threading
    // through. If profiling shows it, lift the CString through
    // `parse_and_classify` and reuse.
    let sql_cstr = CString::new(query)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;
    // SAFETY: we're in the slot bgworker; `sql_cstr` outlives
    // `_guard` (both drop at end-of-function in reverse decl
    // order: _guard first, then sql_cstr).
    let _guard = unsafe { DebugQueryGuard::install(sql_cstr.as_c_str()) };

    // Arm statement_timeout for the duration of this 'Q' body.
    // See [`StatementTimeoutGuard`] for the semantics and closes
    // review 2026-05-24 §6 item 6 for the SPI backend.
    let _stmt_timeout = unsafe { StatementTimeoutGuard::install() };

    let statements = parse_and_classify(query)?;

    // Decide which dispatch shape to use. Vanilla skips the
    // implicit block when `list_length(parsetree_list) <= 1`; we
    // match that. Batches that include xact-control fall back to
    // the per-statement path: `handle_xact_control` issues its
    // own `StartTransactionCommand` / `BeginTransactionBlock` /
    // `CommitTransactionCommand` sequence that doesn't compose
    // cleanly with an outer implicit block.
    let non_empty_count = statements
        .iter()
        .filter(|(text, _)| !text.trim().is_empty())
        .count();
    let has_xact_control = statements.iter().any(|(_, meta)| meta.xact.is_some());

    if non_empty_count > 1 && !has_xact_control {
        return execute_with_implicit_block_spi(statements);
    }

    let mut responses = Vec::with_capacity(statements.len());
    for (text, classification) in statements {
        if text.trim().is_empty() {
            // RawStmt slice with no executable content (just
            // whitespace / comments); PG drops these silently.
            continue;
        }
        responses.extend(execute_one_statement(text, classification)?);
    }
    Ok(responses)
}

/// Multi-statement implicit-block executor for the SPI bridge —
/// SPI counterpart to
/// [`super::simple_direct::execute_with_implicit_block`]. Closes
/// review 2026-05-24 §6 item 2 for the SPI backend.
///
/// Pre-condition (enforced by the caller in
/// [`execute_simple_query`]): every entry in `statements` has
/// `meta.xact == None`. Mixed batches stay on the per-statement
/// path because [`handle_xact_control`]'s xact-block API would
/// fight an outer implicit block.
///
/// Mirrors vanilla `exec_simple_query`'s per-iter pattern
/// ([postgres.c:1097-1340](../../../../../postgres/src/backend/tcop/postgres.c#L1097-L1340)):
///
/// 1. **Per iteration**: `StartTransactionCommand` (idempotent —
///    no-op when already in xact) + `BeginImplicitTransactionBlock`
///    (idempotent — no-op when already inside an implicit block) +
///    `PushActiveSnapshot` + `SPI_connect`.
/// 2. **Run the statement** via [`run_via_spi`] under a freshly-
///    minted [`SpiCtx`] (`SpiCtx::new()` is `pub(super)` for
///    exactly this reason — see [`super::spi::SpiCtx::new`]).
/// 3. **Tail**: `SPI_finish` + `PopActiveSnapshot` then either
///    `EndImplicitTransactionBlock` + `CommitTransactionCommand`
///    (last iter — commits the whole batch atomically) or
///    `CommandCounterIncrement` (mid-iter — make this stmt's
///    effects visible to the next without ending the xact).
/// 4. **PG-ERROR catch**: one outer `catch_unwind` wraps the
///    whole loop body. A statement raising `ERROR` longjmp's
///    through pgrx into a Rust panic, unwinds out of the loop,
///    and lands in the `Err(panic)` arm — which calls
///    `AbortCurrentTransaction` (collapsing the implicit block
///    and rolling back every earlier successful sub-statement)
///    and returns `Err`.
///
/// Why this can't reuse [`with_spi`]: that helper has its own
/// `catch_unwind` + `AbortCurrentTransaction`, so a PG ERROR
/// raised in iteration N would abort the outer xact before iter
/// N+1 — but our outer code has already opened the implicit
/// block, so the abort would collapse it and silently leave the
/// remaining loop iterations running in a fresh auto-commit
/// context. The whole batch must abort as one unit, which means
/// the panic has to escape through `with_spi`'s normal seam and
/// into the catch_unwind installed here.
fn execute_with_implicit_block_spi(
    statements: Vec<(&str, StmtMeta)>,
) -> PgWireResult<Vec<Response>> {
    // Strip whitespace-only entries up front; they were tolerated
    // in single-statement mode (vanilla drops them silently) and
    // we keep the same shape here.
    let prepared: Vec<(&str, Option<CString>)> = statements
        .into_iter()
        .filter(|(text, _)| !text.trim().is_empty())
        .map(|(text, meta)| (text, meta.fetch_portalname))
        .collect();
    debug_assert!(
        prepared.len() > 1,
        "execute_with_implicit_block_spi must only be called for multi-statement bodies"
    );
    let last_idx = prepared.len() - 1;

    let body_outcome = catch_unwind(AssertUnwindSafe(|| -> PgWireResult<Vec<Response>> {
        let mut responses: Vec<Response> = Vec::with_capacity(prepared.len());
        for (i, (text, fetch_portalname)) in prepared.iter().enumerate() {
            // SAFETY: all calls are PG server-API entry points
            // safe from any TBLOCK state. Start +
            // BeginImplicitTransactionBlock are idempotent for
            // an already-open implicit block (the second and
            // later iters take that path).
            unsafe {
                pg_sys::SetCurrentStatementStartTimestamp();
                pg_sys::StartTransactionCommand();
                pg_sys::BeginImplicitTransactionBlock();
                pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
            }

            // Open and close one SPI session per iter, inline
            // (no `with_spi`). See the function-level comment
            // above for why nesting `with_spi` here would
            // defeat the implicit-block atomicity.
            //
            // SAFETY: we're inside the xact opened above;
            // SPI_connect / SPI_finish form a matched pair.
            let connect_rc = unsafe { pg_sys::SPI_connect() };
            if connect_rc != pg_sys::SPI_OK_CONNECT as i32 {
                // Don't try to call SPI_finish on a failed
                // connect; pop snapshot + propagate via the
                // outer abort path (return Err — caught
                // below, AbortCurrentTransaction collapses
                // the implicit block).
                unsafe { pg_sys::PopActiveSnapshot() };
                return Err(spi_rc_error("SPI_connect", connect_rc));
            }
            let ctx = SpiCtx::new();

            let body_result = run_via_spi(&ctx, text, fetch_portalname.as_deref());

            // SAFETY: matched with SPI_connect above. Always
            // close, even on body error — the SPI session
            // was opened.
            let finish_rc = unsafe { pg_sys::SPI_finish() };
            // SAFETY: matched with the Push above.
            unsafe { pg_sys::PopActiveSnapshot() };

            let resp = body_result?;
            if finish_rc != pg_sys::SPI_OK_FINISH as i32 {
                return Err(spi_rc_error("SPI_finish", finish_rc));
            }
            responses.extend(resp);

            // SAFETY: each branch matches vanilla's per-iter
            // tail (postgres.c:1310 / :1340).
            unsafe {
                if i == last_idx {
                    pg_sys::EndImplicitTransactionBlock();
                    pg_sys::CommitTransactionCommand();
                } else {
                    // Mid-batch non-xact-control: make this
                    // statement's effects visible to the
                    // next without ending the xact.
                    pg_sys::CommandCounterIncrement();
                }
            }
        }
        Ok(responses)
    }));

    match body_outcome {
        Ok(Ok(responses)) => Ok(responses),
        Ok(Err(err)) => {
            // Typed error from run_via_spi without a PG ERROR
            // (e.g. NUL-in-query from generic_error, or
            // SPI_connect rc != OK). Xact is still open; abort
            // to honour the implicit-block "first error rolls
            // back the whole batch" rule.
            //
            // SAFETY: AbortCurrentTransaction is safe from any
            // non-DEFAULT TBLOCK_* state.
            unsafe { pg_sys::AbortCurrentTransaction() };
            Err(err)
        }
        Err(panic_payload) => {
            // PG ERROR longjmp'd through `run_via_spi`. This is
            // the load-bearing branch for the §6.2 fix on the
            // SPI backend: the implicit block plus every
            // earlier sub-statement's work rolls back here.
            //
            // SAFETY: AbortCurrentTransaction handles SPI state
            // and the snapshot stack itself; we do NOT call
            // SPI_finish or PopActiveSnapshot here.
            unsafe { pg_sys::AbortCurrentTransaction() };
            Err(panic_to_pgwire(panic_payload))
        }
    }
}

/// Per-statement executor. Routes xact-control statements through
/// the xact-block API; everything else through the SPI wrapper.
fn execute_one_statement(query: &str, meta: StmtMeta) -> PgWireResult<Vec<Response>> {
    if let Some(cmd) = meta.xact {
        return handle_xact_control(cmd);
    }
    run_spi_statement(query, meta.fetch_portalname)
}

/// Execute one non-xact-control statement via SPI under the shared
/// [`with_spi`] xact wrapper. The wrapper encodes the same
/// Start/Push/SPI_connect/SPI_finish/Pop/Commit discipline that this
/// function previously rolled by hand, with `catch_unwind` + abort
/// cleanup on PG ERROR — see
/// [crates/core/src/backend/spi.rs](spi.rs) for the rationale on why
/// we don't use `pgrx::BackgroundWorker::transaction` here.
///
/// Both implicit (auto-commit between queries) and explicit (inside
/// an open `BEGIN` block) entry states work: `StartTransactionCommand`
/// is a no-op from `TBLOCK_INPROGRESS`, and `CommitTransactionCommand`
/// from `TBLOCK_INPROGRESS` does `CommandCounterIncrement` rather
/// than an actual commit (PG `src/backend/access/transam/xact.c`).
///
/// Deviation from real PG on the panic path: PG would move the xact
/// state to `TBLOCK_ABORT` (forcing the client to issue `ROLLBACK`
/// to clean up). `with_spi`'s `AbortCurrentTransaction` collapses
/// state to `DEFAULT`, so subsequent statements run as if in a fresh
/// auto-commit context — incorrect in spec terms but harmless for
/// the pgbench workloads we target.
fn run_spi_statement(
    query: &str,
    fetch_portalname: Option<CString>,
) -> PgWireResult<Vec<Response>> {
    let query_owned = query.to_string();
    with_spi(|ctx: &SpiCtx| -> PgWireResult<Vec<Response>> {
        run_via_spi(ctx, &query_owned, fetch_portalname.as_deref())
    })
}

/// Subset of PG's `TransactionStmt` (gram.y) we route around SPI.
/// v0 covers BEGIN / START / COMMIT / ROLLBACK (and their mode-list
/// forms, since `raw_parser` returns the same `TransactionStmtKind`
/// regardless of mode list). SAVEPOINT / RELEASE / ROLLBACK TO /
/// PREPARE TRANSACTION / COMMIT|ROLLBACK PREPARED are also
/// `TransactionStmt` nodes in PG's grammar, but we deliberately
/// don't intercept them — they flow through SPI and get the
/// `SPI_ERROR_TRANSACTION` rejection with a clear message. Phase ≥9
/// may revisit savepoints if a real workload needs them.
///
/// Visibility note: `pub(super)` so [`super::extended::spi`] can
/// reuse the same classification + [`handle_xact_control`]
/// dispatch when a client sends BEGIN / COMMIT / ROLLBACK via
/// Parse + Bind + Execute (the shape sysbench's libpq driver
/// uses by default). Without sharing, the extended path would
/// forward xact-control to `SPI_execute_plan`, which raises
/// `SPI_ERROR_TRANSACTION` from atomic mode.
#[derive(Debug, Clone, Copy)]
pub(super) enum XactCmd {
    Begin,
    Commit,
    Rollback,
}

/// Per-statement metadata extracted in the single `raw_parser`
/// pass `parse_and_classify` does.
///
/// Bundles every piece of parsetree information run-time callers
/// need so that **no statement re-parses the same SQL**:
///
/// - `xact` — `Some(XactCmd)` for BEGIN/COMMIT/ROLLBACK and their
///   mode-list variants; routed around SPI via
///   [`handle_xact_control_one`] (atomic-mode SPI rejects xact
///   control with `SPI_ERROR_TRANSACTION`).
/// - `fetch_portalname` — `Some(name)` for `FETCH … FROM <name>`
///   (not `MOVE`), copied out of the parsetree as a Rust-owned
///   `CString`. Lets [`run_via_spi`] do `GetPortalByName(name)`
///   and a `CURSOR_OPT_BINARY` check to pick the result format
///   without a second `raw_parser` call on the FETCH path.
#[derive(Debug, Default, Clone)]
pub(super) struct StmtMeta {
    pub(super) xact: Option<XactCmd>,
    pub(super) fetch_portalname: Option<CString>,
}

/// Parse `query` with PG's in-process `raw_parser` and return one
/// `(slice, classification)` tuple per top-level statement.
///
/// The same parse pass solves both the splitting problem (each
/// `RawStmt` carries `stmt_location` + `stmt_len`, giving us a
/// byte slice into the original `query` for SPI) and the
/// classification problem (for nodes of `T_TransactionStmt` we
/// inspect `kind` and map BEGIN / START / COMMIT / ROLLBACK into
/// `XactCmd`; everything else returns `None` to flow through SPI).
///
/// Memory: the parse tree is palloc'd in a scratch `MemoryContext`
/// owned by this function; we extract the spans + classifications
/// into Rust-owned values and delete the context before returning,
/// so nothing leaks into the slot's long-lived TopMemoryContext.
///
/// Errors: `raw_parser` raises PG ERRORs for syntax errors. We
/// wrap the call in `pgrx::PgTryBuilder` (cee-scape `sigsetjmp`)
/// so the longjmp is caught locally rather than escaping to the
/// bgworker's outer `pg_guard`. The `CaughtError` is converted to
/// a wire `ErrorResponse` via [`super::spi::caught_error_to_pgwire`].
#[allow(clippy::result_large_err)] // CaughtError is 232 B and lives only
// across this function; boxing would gain nothing.
fn parse_and_classify(query: &str) -> PgWireResult<Vec<(&str, StmtMeta)>> {
    use pgrx::PgTryBuilder;

    let cstr = CString::new(query)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;

    let outcome: Result<Vec<(usize, usize, StmtMeta)>, CaughtError> =
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let saved_ctx = pg_sys::CurrentMemoryContext;
            let scratch = pg_sys::AllocSetContextCreateInternal(
                saved_ctx,
                c"pg_transport_raw_parse".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as pg_sys::Size,
            );
            pg_sys::MemoryContextSwitchTo(scratch);

            let list = pg_sys::raw_parser(cstr.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT);
            let spans = extract_statement_spans(list, query.len());

            pg_sys::MemoryContextSwitchTo(saved_ctx);
            pg_sys::MemoryContextDelete(scratch);
            Ok(spans)
        }))
        .catch_others(Err)
        .execute();

    let raw_spans = match outcome {
        Ok(v) => v,
        Err(caught) => return Err(caught_error_to_pgwire(&caught)),
    };

    // Slice the original query string by the locations PG reported.
    Ok(raw_spans
        .into_iter()
        .map(|(loc, len, meta)| (&query[loc..loc + len], meta))
        .collect())
}

/// Walk the `List*` returned by `raw_parser`, extracting one
/// `(stmt_location, stmt_len, classification)` per `RawStmt`.
/// Resolves `stmt_len == 0` (PG's "to end of input" sentinel)
/// against `total_len`.
///
/// SAFETY: caller guarantees `list` is either null or a valid
/// `*mut List` of `*mut RawStmt` elements (which is the documented
/// `raw_parser` return shape).
unsafe fn extract_statement_spans(
    list: *mut pg_sys::List,
    total_len: usize,
) -> Vec<(usize, usize, StmtMeta)> {
    if list.is_null() {
        return Vec::new();
    }

    let length = unsafe { (*list).length } as usize;
    let elements = unsafe { (*list).elements };
    let mut out = Vec::with_capacity(length);

    for i in 0..length {
        // SAFETY: elements[0..length] are valid ListCells per the
        // List invariant; ptr_value union variant holds the actual
        // RawStmt pointer for node-pointer lists.
        let cell = unsafe { elements.add(i) };
        let raw_stmt = unsafe { (*cell).ptr_value } as *mut pg_sys::RawStmt;
        if raw_stmt.is_null() {
            continue;
        }

        let loc = unsafe { (*raw_stmt).stmt_location } as i64;
        let len = unsafe { (*raw_stmt).stmt_len } as i64;
        // stmt_len == 0 is PG's "runs to end of input" sentinel; the
        // standalone-statement single-input case also reports loc 0
        // / len 0 (see PG src/backend/parser/scan.l).
        let (slice_loc, slice_len) = if len == 0 {
            let loc_usize = loc.max(0) as usize;
            (loc_usize, total_len.saturating_sub(loc_usize))
        } else {
            (loc.max(0) as usize, len as usize)
        };

        // Single parsetree walk extracts both pieces of metadata
        // the run-time path may need. The cstring copy in
        // `extract_fetch_portalname` is Rust-owned, so the
        // parsetree's scratch MemoryContext is free to die at
        // end-of-function.
        let meta = StmtMeta {
            xact: unsafe { classify_raw_stmt(raw_stmt) },
            fetch_portalname: unsafe { extract_fetch_portalname(raw_stmt) },
        };
        out.push((slice_loc, slice_len, meta));
    }
    out
}

/// Inspect a `RawStmt`'s inner node; return `Some(cmd)` if it's a
/// `TransactionStmt` we want to intercept, `None` otherwise.
///
/// Pure pointer inspection — no parsing, no allocation. The
/// extended-query backends call this on the `RawStmt` their own
/// `pg_parse_query` already produced (direct path: directly;
/// SPI path: via [`infer_param_types_and_classify`]) so neither
/// pays an extra parse pass for xact-control detection.
///
/// SAFETY: caller guarantees `raw_stmt` points at a valid `RawStmt`
/// inside a parse tree returned by `raw_parser` / `pg_parse_query`.
pub(super) unsafe fn classify_raw_stmt(raw_stmt: *mut pg_sys::RawStmt) -> Option<XactCmd> {
    let node = unsafe { (*raw_stmt).stmt };
    if node.is_null() {
        return None;
    }
    if unsafe { (*node).type_ } != pg_sys::NodeTag::T_TransactionStmt {
        return None;
    }
    let txn = node as *mut pg_sys::TransactionStmt;
    match unsafe { (*txn).kind } {
        pg_sys::TransactionStmtKind::TRANS_STMT_BEGIN
        | pg_sys::TransactionStmtKind::TRANS_STMT_START => Some(XactCmd::Begin),
        pg_sys::TransactionStmtKind::TRANS_STMT_COMMIT => Some(XactCmd::Commit),
        pg_sys::TransactionStmtKind::TRANS_STMT_ROLLBACK => Some(XactCmd::Rollback),
        // SAVEPOINT / RELEASE / ROLLBACK TO / PREPARE TRANSACTION /
        // COMMIT|ROLLBACK PREPARED — not intercepted in v0; let SPI
        // reject with its own error.
        _ => None,
    }
}

/// Inspect a `RawStmt`'s inner node; return `Some(portalname)`
/// (Rust-owned copy) if it's a `FETCH … FROM <name>` against a
/// named cursor, `None` for `MOVE` and every other parsetree.
///
/// Pure pointer inspection — no parsing — called from
/// [`extract_statement_spans`] inside the same `raw_parser` pass
/// that produced `raw_stmt`. Copies the portalname out as a Rust
/// `CString` so it survives the scratch-context teardown at
/// end-of-`parse_and_classify`. The CString is then handed
/// through [`run_spi_statement`] → [`run_via_spi`] which does
/// the `GetPortalByName` + `CURSOR_OPT_BINARY` check **without
/// any second parse pass**. Closes review 2026-05-24 §6 item 1
/// for the SPI backend on the same parsetree the existing
/// statement-splitter already produced.
///
/// # Safety
///
/// Caller guarantees `raw_stmt` points at a valid `RawStmt`
/// inside a parse tree returned by `raw_parser`.
unsafe fn extract_fetch_portalname(raw_stmt: *mut pg_sys::RawStmt) -> Option<CString> {
    let node = unsafe { (*raw_stmt).stmt };
    if node.is_null() || unsafe { (*node).type_ } != pg_sys::NodeTag::T_FetchStmt {
        return None;
    }
    let fs = node as *mut pg_sys::FetchStmt;
    if unsafe { (*fs).ismove } {
        // MOVE produces no rows; format is moot. Skip the cstring
        // copy entirely.
        return None;
    }
    let pname = unsafe { (*fs).portalname };
    if pname.is_null() {
        return None;
    }
    // Copy the portalname out of the scratch context before it gets
    // deleted by parse_and_classify's epilogue. CString::new
    // allocates Rust-side and is independent of any PG context.
    let bytes = unsafe { std::ffi::CStr::from_ptr(pname) }.to_bytes();
    CString::new(bytes).ok()
}

fn handle_xact_control(cmd: XactCmd) -> PgWireResult<Vec<Response>> {
    handle_xact_control_one(cmd).map(|r| vec![r])
}

/// Single-`Response` variant of [`handle_xact_control`] reused by
/// the extended-query SPI path (which dispatches one statement per
/// Execute message, not a `Vec` like the simple-query batch).
///
/// Visibility: `pub(super)` so [`super::extended::spi`] can call
/// it from its xact-control short-circuit. Both callers funnel
/// through here so the xact-block API sequence (Start /
/// BeginTransactionBlock / Commit and friends) lives in exactly
/// one place.
pub(super) fn handle_xact_control_one(cmd: XactCmd) -> PgWireResult<Response> {
    // Pre-check state. `IsTransactionBlock()` returns false from both
    // TBLOCK_DEFAULT (between auto-commit queries) and TBLOCK_STARTED
    // (mid-implicit-xact, which shouldn't be reachable between simple
    // queries given our Start/Commit pairing). It returns true only
    // for TBLOCK_*INPROGRESS — i.e. inside an explicit BEGIN block.
    let in_block = unsafe { pg_sys::IsTransactionBlock() };

    // Run the xact API call inside a catch_unwind so a PG ERROR
    // (e.g. nested-xact rejection) becomes a clean PgWireError
    // instead of unwinding past the wire layer. On panic we reset to
    // DEFAULT via AbortCurrentTransaction.
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Tag {
        match cmd {
            XactCmd::Begin => {
                if in_block {
                    // PG would emit `WARNING: there is already a
                    // transaction in progress` here and still report
                    // CommandTag "BEGIN". We skip the WARNING for
                    // v0 simplicity — pgbench doesn't observe it.
                    return Tag::new("BEGIN");
                }
                unsafe {
                    pg_sys::SetCurrentStatementStartTimestamp();
                    pg_sys::StartTransactionCommand();
                    pg_sys::BeginTransactionBlock();
                    pg_sys::CommitTransactionCommand();
                }
                Tag::new("BEGIN")
            }
            XactCmd::Commit => {
                if !in_block {
                    // PG would WARN; we just return COMMIT.
                    return Tag::new("COMMIT");
                }
                unsafe {
                    pg_sys::SetCurrentStatementStartTimestamp();
                    pg_sys::StartTransactionCommand();
                    let _committed = pg_sys::EndTransactionBlock(false);
                    pg_sys::CommitTransactionCommand();
                }
                Tag::new("COMMIT")
            }
            XactCmd::Rollback => {
                if !in_block {
                    // PG would WARN; we just return ROLLBACK.
                    return Tag::new("ROLLBACK");
                }
                unsafe {
                    pg_sys::SetCurrentStatementStartTimestamp();
                    pg_sys::StartTransactionCommand();
                    pg_sys::UserAbortTransactionBlock(false);
                    pg_sys::CommitTransactionCommand();
                }
                Tag::new("ROLLBACK")
            }
        }
    }));

    match outcome {
        Ok(tag) => Ok(Response::Execution(tag)),
        Err(panic_payload) => {
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            Err(panic_to_pgwire(panic_payload))
        }
    }
}

/// Body of [`run_spi_statement`]'s `with_spi` closure. Executes one
/// statement via `SPI_execute`, then either:
///
/// * Returns a `Response::Execution(Tag::new("OK").with_rows(N))`
///   for utility / no-RETURNING DML (no result tuples).
/// * Materialises every result row directly into a per-row
///   [`BytesMut`] via [`ColumnEncoder`] (text format only, since the
///   simple-query protocol is text-format-by-definition), wraps each
///   in a [`DataRow`], and returns `Response::Query` with a
///   length-prefixed wire-format-ready stream.
///
/// The encoding shape is symmetric with
/// [`super::extended`]'s extended-query path: cache the per-column
/// encoder once (one syscache lookup amortised over all rows
/// instead of per cell — folds in performance.md §3.5), then per
/// cell either write `-1` (NULL marker) or have `encode_into`
/// write `(len: i32, bytes)` straight into the per-row buffer. We
/// deliberately bypass pgwire's `DataRowEncoder` (which is
/// Rust-value-oriented and would force `Vec<Option<String>>`
/// re-encoding) — see performance.md §3.2.
///
/// PG ERRORs raised by `SPI_execute` (syntax error, type mismatch,
/// constraint violation, …) longjmp out; [`with_spi`]'s `catch_unwind`
/// catches and converts them to wire `ErrorResponse` frames.
fn run_via_spi(
    ctx: &SpiCtx,
    query: &str,
    fetch_portalname: Option<&CStr>,
) -> PgWireResult<Vec<Response>> {
    let query_c = CString::new(query)
        .map_err(|_| generic_error("simple-query", "query string contains a NUL byte"))?;

    // Decide the result format BEFORE encoder construction. For
    // FETCH against a `DECLARE … BINARY CURSOR`,
    // `parse_and_classify` already extracted the portalname from
    // the parsetree it made; here we only pay a `GetPortalByName`
    // lookup + a `CURSOR_OPT_BINARY` bit check — no parse pass.
    // The cursor's portal was created by an earlier statement
    // (this Q or a previous one in the same outer xact) and is
    // visible to `GetPortalByName` now. Mirrors
    // [`super::simple_direct::fetch_result_format`]; closes
    // review 2026-05-24 §6 item 1 for the SPI backend.
    let result_format = match fetch_portalname {
        Some(name) => unsafe {
            // SAFETY: `GetPortalByName` is a backend-context lookup;
            // null means "no such cursor", which we treat as Text
            // (and let `SPI_execute` later surface the canonical
            // "cursor X does not exist" error).
            let cursor_portal = pg_sys::GetPortalByName(name.as_ptr());
            if cursor_portal.is_null() {
                FieldFormat::Text
            } else if ((*cursor_portal).cursorOptions & pg_sys::CURSOR_OPT_BINARY as i32) != 0 {
                FieldFormat::Binary
            } else {
                FieldFormat::Text
            }
        },
        None => FieldFormat::Text,
    };

    // SAFETY: inside an SPI session (proven by &SpiCtx); SPI_execute
    // is the documented entry point. `read_only=false` matches the
    // prior `Spi::connect_mut`-based behaviour (DDL/DML allowed);
    // `tcount=0` means "unbounded rows".
    let exec_rc = unsafe { pg_sys::SPI_execute(query_c.as_ptr(), false, 0) };
    if exec_rc < 0 {
        return Err(spi_rc_error("SPI_execute", exec_rc));
    }

    // Utility / no-RETURNING DML: SPI_tuptable is null; report row
    // count only. We preserve the "OK" command tag the previous impl
    // used (rather than mapping rc → SELECT/INSERT/UPDATE/etc. as
    // extended.rs does) so the simple-query bridge's externally
    // observable command tags don't shift in this change.
    let tuples = match SpiTuples::current(ctx) {
        Some(t) if t.ncols() > 0 => t,
        _ => {
            let processed = SpiTuples::processed_rows_without_table(ctx);
            return Ok(vec![Response::Execution(
                Tag::new("OK").with_rows(processed),
            )]);
        }
    };

    let ncols = tuples.ncols();
    let nrows = tuples.len();

    // SAFETY: tuptable is non-null (proven by SpiTuples::current
    // returning Some); its tupdesc is valid for the SPI session.
    let tupdesc = unsafe { (*pg_sys::SPI_tuptable).tupdesc };

    // Build the RowDescription schema and the per-column encoder
    // cache in a single tupdesc walk. Columns with an OID pgwire
    // doesn't recognise fall back to TEXT — harmless because we're
    // emitting text format anyway; the OID is just a parsing hint
    // for the client.
    let mut schema_vec: Vec<FieldInfo> = Vec::with_capacity(ncols);
    let mut encoders: Vec<ColumnEncoder> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        // SAFETY: tupdesc non-null; i is in 0..natts; the returned
        // FormData_pg_attribute lives as long as the tupdesc.
        let attr = unsafe { &*pg_sys::TupleDescAttr(tupdesc, i as i32) };
        // SAFETY: attname is a PG NameData buffer containing an
        // ASCII identifier.
        let name =
            unsafe { crate::backend::executor::pg_ident_to_string(attr.attname.data.as_ptr()) };
        let pgwire_type = Type::from_oid(attr.atttypid.to_u32()).unwrap_or(Type::TEXT);
        schema_vec.push(FieldInfo::new(name, None, None, pgwire_type, result_format));
        encoders.push(ColumnEncoder::for_column(attr.atttypid, result_format));
    }

    // Materialise every row up-front into BytesMut. The SPI tupdesc
    // and heap tuples die when `with_spi`'s SPI_finish runs (after
    // this closure returns); pgwire's row stream is consumed *after*
    // `do_query` returns, so we can't lazily borrow. Large-result
    // streaming is a phase ≥ 9.5 concern.
    let mut data_rows: Vec<DataRow> = Vec::with_capacity(nrows);
    for row_idx in 0..nrows {
        let mut buf = BytesMut::with_capacity(64);
        for (c, enc) in encoders.iter().enumerate() {
            match tuples.cell(row_idx, c) {
                None => buf.put_i32(-1),
                Some(datum) => enc.encode_into(datum, &mut buf),
            }
        }
        data_rows.push(DataRow::new(buf, ncols as i16));
    }

    let schema = Arc::new(schema_vec);
    let row_stream = stream::iter(data_rows).map(Ok);
    Ok(vec![Response::Query(QueryResponse::new(
        schema, row_stream,
    ))])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// `#[pg_test]` runs each test inside a regular PG backend with an
// outer `BEGIN; <body>; ROLLBACK` wrapper. That lets us exercise:
//
//   * [`parse_and_classify`] — calls `pg_sys::raw_parser` in-process,
//     producing our `(slice, classification)` tuples. Catches any
//     future pgrx upgrade that shifts `TransactionStmtKind` enum
//     values or `RawStmt` layout, plus our slicing logic.
//   * [`classify_raw_stmt`] / [`extract_statement_spans`] — exercised
//     transitively through `parse_and_classify`.
//
// What we deliberately do NOT cover here:
//
//   * [`run_spi_statement`] / [`execute_simple_query`] — both call
//     `StartTransactionCommand`, which trips
//     `"unexpected state INPROGRESS"` against pg_test's pre-opened
//     outer transaction. Their coverage lives in `just e2e` /
//     `just pgbench` (real out-of-process flow against the slot).
//   * [`handle_xact_control`] — same reason.
//   * Wire layer + slot handoff — multi-process, out of scope for
//     `#[pg_test]` entirely. See [`docs/design/testing.md`](../../../../docs/design/testing.md).
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::{XactCmd, parse_and_classify};
    use pgrx::prelude::*;
    use pgwire::error::PgWireError;

    /// Convenience: assert parse succeeded and return the spans.
    /// Convenience: assert parse succeeded and return the spans,
    /// projecting `StmtMeta` down to `Option<XactCmd>` so existing
    /// xact-classification tests don't have to spell out the full
    /// metadata struct.
    fn parse_ok(query: &str) -> Vec<(&str, Option<XactCmd>)> {
        parse_and_classify(query)
            .expect("parse should succeed")
            .into_iter()
            .map(|(s, meta)| (s, meta.xact))
            .collect()
    }

    #[pg_test]
    fn empty_inputs_return_no_statements() {
        // PG drops whitespace-only and comment-only inputs at parse
        // time; we must do the same so pgwire emits a single
        // EmptyQueryResponse rather than running anything.
        assert!(parse_ok("").is_empty());
        assert!(parse_ok("   \t  \n").is_empty());
        assert!(parse_ok("-- just a comment\n").is_empty());
        assert!(parse_ok("/* block comment */").is_empty());
    }

    #[pg_test]
    fn single_select_is_unclassified() {
        let stmts = parse_ok("SELECT 1");
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].1.is_none(),
            "SELECT should flow through to SPI uncategorised, got {:?}",
            stmts[0].1
        );
    }

    #[pg_test]
    fn begin_is_classified() {
        let stmts = parse_ok("BEGIN");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0].1, Some(XactCmd::Begin)));
    }

    #[pg_test]
    fn start_transaction_classified_as_begin() {
        // `START TRANSACTION` shares the gram.y TransactionStmt node
        // with `BEGIN` but has its own TransactionStmtKind variant.
        // We collapse both to XactCmd::Begin so the xact-block
        // routing is uniform.
        let stmts = parse_ok("START TRANSACTION");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0].1, Some(XactCmd::Begin)));
    }

    #[pg_test]
    fn commit_and_rollback_are_classified() {
        assert!(matches!(parse_ok("COMMIT")[0].1, Some(XactCmd::Commit)));
        assert!(matches!(parse_ok("ROLLBACK")[0].1, Some(XactCmd::Rollback)));
    }

    #[pg_test]
    fn begin_with_isolation_mode_list_still_begin() {
        // Mode-list variants share TransactionStmtKind::TRANS_STMT_BEGIN;
        // the mode list is carried as `options` on the same node. This
        // is the case that motivated using the real parser rather
        // than bare-keyword matching.
        let stmts = parse_ok("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE");
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0].1, Some(XactCmd::Begin)));
    }

    #[pg_test]
    fn savepoint_not_intercepted() {
        // SAVEPOINT is a TransactionStmt in PG's grammar but v0
        // deliberately does NOT intercept it — let SPI surface the
        // SPI_ERROR_TRANSACTION rejection with its native message.
        let stmts = parse_ok("SAVEPOINT s1");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].1.is_none(), "SAVEPOINT must flow through to SPI");
    }

    #[pg_test]
    fn multi_statement_split() {
        let stmts = parse_ok("SELECT 1; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].0.trim(), "SELECT 1");
        assert_eq!(stmts[1].0.trim().trim_end_matches(';'), "SELECT 2");
        assert!(stmts[0].1.is_none() && stmts[1].1.is_none());
    }

    #[pg_test]
    fn multi_statement_mixed_classification() {
        let stmts = parse_ok("BEGIN; SELECT 1; COMMIT");
        assert_eq!(stmts.len(), 3);
        assert!(matches!(stmts[0].1, Some(XactCmd::Begin)));
        assert!(stmts[1].1.is_none());
        assert!(matches!(stmts[2].1, Some(XactCmd::Commit)));
    }

    #[pg_test]
    fn semicolon_inside_string_literal_preserved() {
        // The semicolon is inside a string literal — `raw_parser`
        // must keep `SELECT ';'` as one statement, not split on the
        // literal's semicolon. This is why we use the real parser
        // instead of byte-level splitting.
        let stmts = parse_ok("SELECT ';'; SELECT 2");
        assert_eq!(stmts.len(), 2, "literal semicolon must not split");
        assert!(stmts[0].0.contains("';'"));
    }

    #[pg_test]
    fn dollar_quoted_string_handled() {
        // Dollar-quoting was the canonical use-case for adopting a
        // real parser (any byte splitter would mis-handle this).
        let stmts = parse_ok("SELECT $tag$ a ; b $tag$; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].0.contains("$tag$"));
    }

    #[pg_test]
    fn syntax_error_returns_pgwire_error_with_native_message() {
        // `raw_parser` raises a PG ERROR for bad syntax; we catch it
        // via PgTryBuilder and surface a PgWireError instead of
        // longjmp'ing past the wire layer. We also get PG's native
        // "syntax error at or near …" message for free.
        let err = parse_and_classify("SELECTT 1").expect_err("should fail");
        match err {
            PgWireError::UserError(info) => {
                assert!(
                    info.message.contains("syntax error"),
                    "expected PG-native syntax error, got: {info:?}"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Observability — debug_query_string downstream attribution
    // (review 2026-05-24 §6 item 5)
    // -----------------------------------------------------------------------

    /// Mirror of the simple-direct backend's
    /// `pg_simple_direct_executor_start_hook_sees_inner_query`
    /// test for the SPI bridge. Verifies
    /// [`super::execute_simple_query`] installs
    /// [`super::super::observability::DebugQueryGuard`] before
    /// running the executor, so `pg_stat_statements` /
    /// `auto_explain` attribute correctly.
    ///
    /// Uses the shared
    /// [`super::super::observability::test_helpers::capture_executor_start`]
    /// helper, which co-locates the hook + capture-state with
    /// the guard it asserts.
    #[pg_test]
    fn pg_spi_bridge_executor_start_hook_sees_inner_query() {
        use super::super::observability::test_helpers::capture_executor_start;

        const SQL: &str = "SELECT 1 AS only_one";

        let (result, captured) = capture_executor_start(|| super::execute_simple_query(SQL));
        result.expect("SELECT 1 should succeed");

        assert_eq!(
            captured.debug_query_string_at_hook.as_deref(),
            Some(SQL),
            "REGRESSION on review 2026-05-24 §6 item 5: at \
             ExecutorStart_hook time, `debug_query_string` is not \
             set to the inner SQL for the SPI backend. \
             `pg_stat_statements` / `auto_explain` would \
             mis-attribute every SPI-backend query. Likely cause: \
             `DebugQueryGuard::install` was moved or dropped in \
             `execute_simple_query`.",
        );
    }

    // -----------------------------------------------------------------------
    // FETCH-binary cursor format (review 2026-05-24 §6 item 1)
    // -----------------------------------------------------------------------

    /// Mirror of the simple-direct backend's
    /// `pg_simple_direct_fetch_binary_cursor_returns_binary`
    /// test for the SPI bridge. Verifies
    /// [`super::execute_simple_query`] consults
    /// [`super::fetch_result_format_for_spi`] and routes FETCH
    /// against a `BINARY CURSOR` through binary encoders so the
    /// returned `DataRow` carries the 4-byte big-endian int4
    /// encoding rather than text.
    ///
    /// **Important.** The DECLARE and the FETCH each invoke
    /// `with_spi`, which opens its own SPI session inside the
    /// outer pg_test transaction. The cursor declared by the
    /// first call lives in the outer xact (not in SPI's per-call
    /// session) and is visible to `GetPortalByName` from the
    /// second call.
    #[pg_test]
    fn pg_spi_bridge_fetch_binary_cursor_returns_binary() {
        super::execute_simple_query("DECLARE bincur_spi BINARY CURSOR FOR SELECT 42::int4")
            .expect("DECLARE BINARY CURSOR should succeed");

        let r =
            super::execute_simple_query("FETCH 1 FROM bincur_spi").expect("FETCH should succeed");
        assert_eq!(r.len(), 1, "expected one Response from FETCH");
        let mut q = match r.into_iter().next().unwrap() {
            pgwire::api::results::Response::Query(q) => q,
            other => panic!("expected Query response from FETCH, got {other:?}"),
        };

        let row = futures::executor::block_on(async {
            use futures::StreamExt;
            q.data_rows().next().await
        })
        .expect("FETCH should return one row")
        .expect("row item should be Ok");
        assert_eq!(row.field_count, 1, "single int4 column");

        let mut data = row.data;
        use bytes::Buf;
        let len = data.get_i32();
        assert_eq!(len, 4, "binary int4 is 4 bytes; got len {len}");
        let cell = data.split_to(len as usize).to_vec();

        let expected_binary: Vec<u8> = 42i32.to_be_bytes().to_vec();
        assert_eq!(
            cell, expected_binary,
            "REGRESSION on review 2026-05-24 §6 item 1: FETCH from a \
             BINARY cursor through the SPI bridge produced {cell:?} \
             instead of the expected 4-byte big-endian int4 encoding \
             {expected_binary:?}. `fetch_result_format_for_spi` was \
             bypassed or returned Text incorrectly.",
        );
    }
}
