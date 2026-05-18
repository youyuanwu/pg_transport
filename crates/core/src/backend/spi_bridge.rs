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

use std::any::Any;
use std::ffi::{CStr, CString};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys::panic::{CaughtError, ErrorReportWithLevel};
use pgrx::pg_sys::{self};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

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
pub fn execute_simple_query(query: &str) -> PgWireResult<Vec<Response>> {
    let statements = parse_and_classify(query)?;
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

/// Per-statement executor. Routes xact-control statements through
/// the xact-block API; everything else through the SPI wrapper.
fn execute_one_statement(
    query: &str,
    classification: Option<XactCmd>,
) -> PgWireResult<Vec<Response>> {
    if let Some(cmd) = classification {
        return handle_xact_control(cmd);
    }
    run_spi_statement(query)
}

/// Execute one non-xact-control statement via SPI under our own
/// transaction wrapper.
fn run_spi_statement(query: &str) -> PgWireResult<Vec<Response>> {
    // We can't use pgrx's `BackgroundWorker::transaction` directly:
    // in pgrx 0.18 its `PopActiveSnapshot` + `CommitTransactionCommand`
    // calls are OUTSIDE the `PgTryBuilder`, so a PG ERROR raised
    // anywhere in the body leaks an open transaction (xact state =
    // TBLOCK_STARTED + pushed active snapshot). The next call then
    // asserts `"StartTransactionCommand: unexpected state STARTED"`,
    // which we hit running pgbench against the slot. So we open and
    // close the transaction ourselves with explicit panic cleanup.
    //
    // This works for both implicit (auto-commit between queries) and
    // explicit (inside an open `BEGIN` block) entry states:
    // `StartTransactionCommand` is a no-op for xact-start when called
    // from `TBLOCK_INPROGRESS`, and `CommitTransactionCommand` from
    // `TBLOCK_INPROGRESS` does `CommandCounterIncrement` rather than
    // an actual commit. See PG `src/backend/access/transam/xact.c`.
    let query_owned = query.to_string();

    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome: Result<Result<Vec<Response>, PgWireError>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| run_via_spi(&query_owned)));

    match outcome {
        Ok(Ok(responses)) => {
            // SAFETY: matched Start/Push above. From TBLOCK_STARTED
            // (implicit-xact entry) Pop+Commit returns to DEFAULT;
            // from TBLOCK_INPROGRESS (inside an open BEGIN) Pop
            // unstacks our snapshot and Commit just bumps the
            // command counter.
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Ok(responses)
        }
        Ok(Err(err)) => {
            // Body returned a clean PgWireError (e.g. our SPI-error
            // translation). Xact state hasn't been corrupted — no PG
            // ERROR was raised — so Pop+Commit is the right teardown.
            // We do NOT Abort here: the application-level error is
            // not an SPI-level rollback request, and forcing one
            // would mask the fact that nothing PG-side actually
            // failed.
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Err(err)
        }
        Err(panic_payload) => {
            // A PG ERROR (or Rust panic) longjmp'd / unwound through
            // the body. SPI cleanup is handled by PG's own xact-abort
            // path inside AbortCurrentTransaction; we must NOT touch
            // the active snapshot stack here (PG already popped it
            // during abort).
            //
            // SAFETY: AbortCurrentTransaction is safe to call from
            // any non-DEFAULT TBLOCK_* state; it resets state to
            // DEFAULT and releases resources.
            //
            // Deviation from real PG: PG would set state to
            // TBLOCK_ABORT (forcing the client to issue ROLLBACK to
            // clean up) and reject every subsequent statement until
            // ROLLBACK. We collapse that to DEFAULT, so subsequent
            // statements run as if in a fresh auto-commit context.
            // This is incorrect in spec terms but harmless for the
            // pgbench workloads we target; phase ≥ 9 (extended-query
            // + proper xact state machine) can fix this when it
            // matters.
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            Err(panic_to_pgwire(panic_payload))
        }
    }
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
#[derive(Debug, Clone, Copy)]
enum XactCmd {
    Begin,
    Commit,
    Rollback,
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
/// a wire `ErrorResponse` via the existing
/// [`extract_report`] helper.
#[allow(clippy::result_large_err)] // CaughtError is 232 B and lives only
// across this function; boxing would gain nothing.
fn parse_and_classify(query: &str) -> PgWireResult<Vec<(&str, Option<XactCmd>)>> {
    use pgrx::PgTryBuilder;

    let cstr = CString::new(query)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;

    let outcome: Result<Vec<(usize, usize, Option<XactCmd>)>, CaughtError> =
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
        Err(caught) => return Err(error_report_to_pgwire(extract_report(&caught))),
    };

    // Slice the original query string by the locations PG reported.
    Ok(raw_spans
        .into_iter()
        .map(|(loc, len, cmd)| (&query[loc..loc + len], cmd))
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
) -> Vec<(usize, usize, Option<XactCmd>)> {
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

        let classification = unsafe { classify_raw_stmt(raw_stmt) };
        out.push((slice_loc, slice_len, classification));
    }
    out
}

/// Inspect a `RawStmt`'s inner node; return `Some(cmd)` if it's a
/// `TransactionStmt` we want to intercept, `None` otherwise.
///
/// SAFETY: caller guarantees `raw_stmt` points at a valid `RawStmt`
/// inside a parse tree returned by `raw_parser`.
unsafe fn classify_raw_stmt(raw_stmt: *mut pg_sys::RawStmt) -> Option<XactCmd> {
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

/// Pull the `ErrorReportWithLevel` out of a `CaughtError` for
/// conversion to a wire `ErrorResponse`. Centralises the variant
/// match so callers don't repeat it.
fn extract_report(caught: &CaughtError) -> &ErrorReportWithLevel {
    match caught {
        CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => report,
        CaughtError::RustPanic { ereport, .. } => ereport,
    }
}

fn handle_xact_control(cmd: XactCmd) -> PgWireResult<Vec<Response>> {
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
        Ok(tag) => Ok(vec![Response::Execution(tag)]),
        Err(panic_payload) => {
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            Err(panic_to_pgwire(panic_payload))
        }
    }
}

fn run_via_spi(query: &str) -> Result<Vec<Response>, PgWireError> {
    // Spi::connect_mut runs the closure inside an SPI_connect /
    // SPI_finish pair, with a read-write SPI session — required for
    // DDL/DML (pgbench -i, UPDATE, etc.). PG ERRORs from inside
    // panic out; the outer `execute_simple_query` catches them via
    // catch_unwind + AbortCurrentTransaction.
    pgrx::Spi::connect_mut(|client| -> Result<Vec<Response>, PgWireError> {
        client
            .update(query, None, &[])
            .map_err(|e| spi_error_to_pgwire(&e.to_string()))?;

        // After select succeeds we walk SPI's globals directly: it's
        // both simpler than fighting pgrx's typed accessors (we want
        // text-format-for-any-type, not Rust-typed) and gives us the
        // raw heap tuples we need for SPI_getvalue.
        //
        // SAFETY: `SPI_tuptable` / `SPI_processed` are PG globals
        // populated by the previous SPI call; valid until the next
        // SPI call or SPI_finish.
        let (tuptable, nrows) = unsafe { (pg_sys::SPI_tuptable, pg_sys::SPI_processed as usize) };

        // Utility statements (CREATE TABLE, etc.) leave SPI_tuptable
        // null and only populate SPI_processed.
        if tuptable.is_null() {
            return Ok(vec![Response::Execution(Tag::new("OK").with_rows(nrows))]);
        }

        // SAFETY: SPI_tuptable is non-null per the check above; its
        // tupdesc and vals are valid for the duration of this SPI
        // connection.
        let tupdesc = unsafe { (*tuptable).tupdesc };
        let ncols = unsafe { (*tupdesc).natts } as usize;
        if ncols == 0 {
            return Ok(vec![Response::Execution(Tag::new("OK").with_rows(nrows))]);
        }

        // Build the RowDescription schema once. Each FieldInfo wraps
        // a pgwire Type (= postgres_types::Type) derived from the
        // column's atttypid; columns with an OID we don't recognise
        // fall back to TEXT — which is harmless because we're
        // emitting text format anyway and the OID is just a hint
        // for the client's parsing code.
        let schema_vec: Vec<FieldInfo> = (0..ncols)
            .map(|i| {
                // SAFETY: tupdesc is non-null, i is in 0..natts; the
                // returned FormData_pg_attribute lives as long as
                // tupdesc.
                let attr = unsafe { &*pg_sys::TupleDescAttr(tupdesc, i as i32) };
                let name = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
                    .to_string_lossy()
                    .into_owned();
                let pgwire_type = Type::from_oid(attr.atttypid.to_u32()).unwrap_or(Type::TEXT);
                FieldInfo::new(name, None, None, pgwire_type, FieldFormat::Text)
            })
            .collect();

        // Materialise every row up-front. The SPI tupdesc + heap
        // tuples die when the `Spi::connect` closure returns, and
        // pgwire's row stream is consumed *after* `do_query` returns;
        // we can't lazily borrow into a future SPI session. Large-
        // result handling is a phase ≥ 9 concern.
        let mut materialised: Vec<Vec<Option<String>>> = Vec::with_capacity(nrows);
        for row_idx in 0..nrows {
            // SAFETY: row_idx < SPI_processed; vals[row_idx] is a
            // valid HeapTuple in SPI_tuptable.
            let heap_tuple = unsafe { *(*tuptable).vals.add(row_idx) };
            let mut row = Vec::with_capacity(ncols);
            for col_idx in 0..ncols {
                // SPI_getvalue calls the type's text output function
                // and returns a palloc'd C string, or NULL if the
                // datum is NULL (or if there's an error — phase 4b
                // treats both as NULL, which is wire-correct for
                // NULL and at-worst-confusing for the rare error
                // case; phase ≥ 5 may add SPI_getbinval +
                // explicit is_null when this bites someone).
                let cstr_ptr =
                    unsafe { pg_sys::SPI_getvalue(heap_tuple, tupdesc, (col_idx + 1) as i32) };
                if cstr_ptr.is_null() {
                    row.push(None);
                } else {
                    // SAFETY: SPI_getvalue returns NUL-terminated.
                    let s = unsafe { CStr::from_ptr(cstr_ptr) }
                        .to_string_lossy()
                        .into_owned();
                    unsafe { pg_sys::pfree(cstr_ptr as *mut _) };
                    row.push(Some(s));
                }
            }
            materialised.push(row);
        }

        let schema = Arc::new(schema_vec);
        let schema_for_stream = schema.clone();
        let row_stream = stream::iter(materialised).map(move |row| {
            let mut encoder = DataRowEncoder::new(schema_for_stream.clone());
            for cell in row {
                encoder.encode_field(&cell)?;
            }
            Ok(encoder.take_row())
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema, row_stream,
        ))])
    })
}

fn spi_error_to_pgwire(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("pg_transport SPI error: {msg}"),
    )))
}

/// Convert a caught panic payload back into a pgwire `ErrorResponse`.
///
/// pgrx's pg_guard machinery catches PG ERROR longjmps and re-raises
/// them via `resume_unwind(Box::new(CaughtError::…))`, so the outer
/// `catch_unwind` receives a `Box<dyn Any>` whose concrete type is
/// `CaughtError`. We downcast to that first and extract the wrapped
/// `ErrorReportWithLevel`; bare `panic_any(ErrorReportWithLevel)` and
/// bare `panic!("…")` are also handled as fallbacks.
fn panic_to_pgwire(payload: Box<dyn Any + Send>) -> PgWireError {
    if let Some(report) = caught_error_report(&payload) {
        return error_report_to_pgwire(report);
    }
    if let Some(report) = payload.downcast_ref::<ErrorReportWithLevel>() {
        return error_report_to_pgwire(report);
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return generic_error("pg_transport panic", s);
    }
    if let Some(s) = payload.downcast_ref::<&str>() {
        return generic_error("pg_transport panic", s);
    }
    generic_error("pg_transport", "unknown panic payload in SPI bridge")
}

fn caught_error_report(payload: &Box<dyn Any + Send>) -> Option<&ErrorReportWithLevel> {
    let caught = payload.downcast_ref::<CaughtError>()?;
    Some(extract_report(caught))
}

fn error_report_to_pgwire(report: &ErrorReportWithLevel) -> PgWireError {
    // `sql_error_code()` returns a `PgSqlErrorCode` enum; its `Debug`
    // form is the variant name (e.g. `ERRCODE_DIVISION_BY_ZERO`).
    // pgwire wants the 5-char SQLSTATE string for ErrorInfo. Phase 4b
    // accepts the variant-name fallback; phase ≥ 7 will plumb the
    // real 5-char code via a lookup table once the auth layer needs
    // SQLSTATE handling anyway.
    PgWireError::UserError(Box::new(ErrorInfo::new(
        format!("{:?}", report.level()),
        format!("{:?}", report.sql_error_code()),
        report.message().to_string(),
    )))
}

fn generic_error(prefix: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("{prefix}: {message}"),
    )))
}
