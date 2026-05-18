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
//! Multi-statement simple-query bodies are split via sqlparser's
//! tokenizer (a pure-Rust PostgreSQL tokenizer; handles
//! dollar-quoting, string literals, and comments correctly) and
//! each statement runs through the per-statement path independently.
//! Transaction-control statements are classified via sqlparser's
//! `Statement` AST — covering every grammar variant we recognise
//! including transaction-mode lists — and routed around SPI's
//! atomic mode through PG's xact-block API. Resolves Q25 in
//! [roadmap.md](../../../../docs/design/roadmap.md).

use std::any::Any;
use std::ffi::CStr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys::panic::{CaughtError, ErrorReportWithLevel};
use pgrx::pg_sys::{self};
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

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
///   them reach `SPI_execute`. See [`classify_xact_control`].
/// * Multi-statement bodies (`SELECT 1; SELECT 2` in one `'Q'`) are
///   split via sqlparser's tokenizer and each piece runs through the
///   per-statement path independently. On first error the batch
///   stops — matches real PG's `'Q'` semantics.
/// * Empty / whitespace-only / comment-only bodies return an empty
///   `Vec<Response>`; pgwire's SimpleQueryHandler renders that as a
///   single `EmptyQueryResponse` + `ReadyForQuery`.
pub fn execute_simple_query(query: &str) -> PgWireResult<Vec<Response>> {
    // sqlparser's tokenizer is more permissive than its parser —
    // it just classifies bytes (handles string literals,
    // dollar-quoting, line/block comments), so anything PG can
    // tokenize survives the split. Statements that the *parser*
    // chokes on still flow through to SPI with their original text
    // intact via the parse-error fallback in classify_xact_control.
    let statements = match split_statements(query) {
        Ok(v) => v,
        Err(e) => return Err(parse_error_to_pgwire(&e)),
    };

    let mut responses = Vec::with_capacity(statements.len());
    for stmt in statements {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            // split_statements can return whitespace-only chunks
            // around stray semicolons; PG drops them silently.
            continue;
        }
        responses.extend(execute_one_statement(trimmed)?);
    }
    Ok(responses)
}

/// Per-statement executor. Routes xact-control statements through
/// the xact-block API; everything else through the SPI wrapper.
fn execute_one_statement(query: &str) -> PgWireResult<Vec<Response>> {
    if let Some(cmd) = classify_xact_control(query) {
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
/// v0 covers BEGIN / START / COMMIT / END / ROLLBACK / ABORT and
/// the transaction-mode-list forms of each. SAVEPOINT / RELEASE /
/// ROLLBACK TO / PREPARE TRANSACTION / COMMIT|ROLLBACK PREPARED are
/// also `TransactionStmt` nodes in PG's grammar, but we deliberately
/// don't intercept them — they flow through SPI and get the
/// `SPI_ERROR_TRANSACTION` rejection with a clear message. Phase ≥9
/// may revisit savepoints if a real workload needs them.
#[derive(Debug, Clone, Copy)]
enum XactCmd {
    Begin,
    Commit,
    Rollback,
}

/// Classify the first statement of `stmt`. Returns `Some(cmd)` for
/// v0-supported xact-control commands and `None` for anything else,
/// including statements the parser can't handle — those flow through
/// SPI which sees the original text and emits its own error if any.
///
/// Coverage via sqlparser's `Statement` variants:
/// * `Statement::StartTransaction { .. }` — covers `BEGIN`,
///   `BEGIN WORK`, `BEGIN TRANSACTION [mode…]`, `START TRANSACTION
///   [mode…]`, and every transaction-mode-list combination.
/// * `Statement::Commit { end: false, .. }` — covers `COMMIT`,
///   `COMMIT WORK`, `COMMIT TRANSACTION`. Also `END` / `END WORK` /
///   `END TRANSACTION` (sqlparser parses these as `Commit { end:
///   true, .. }`; same semantics for us).
/// * `Statement::Rollback { savepoint: None, .. }` — covers
///   `ROLLBACK`, `ROLLBACK WORK`, `ROLLBACK TRANSACTION`, `ABORT`.
///   `ROLLBACK TO [SAVEPOINT] name` falls through to SPI (we don't
///   implement savepoints in v0).
///
/// `Statement::Savepoint`, `Statement::ReleaseSavepoint`, and
/// `Statement::Prepare` (PG `PREPARE TRANSACTION`) also fall through
/// to SPI today — v0 doesn't intercept them. The bare-keyword
/// classifier this replaces missed all the modes; we're now
/// grammar-accurate for the cases we handle.
fn classify_xact_control(stmt: &str) -> Option<XactCmd> {
    let asts = Parser::parse_sql(&PostgreSqlDialect {}, stmt).ok()?;
    match asts.first()? {
        Statement::StartTransaction { .. } => Some(XactCmd::Begin),
        Statement::Commit { .. } => Some(XactCmd::Commit),
        Statement::Rollback {
            savepoint: None, ..
        } => Some(XactCmd::Rollback),
        _ => None,
    }
}

/// Split `query` at top-level semicolons via the PostgreSQL
/// tokenizer. Returns substrings (each may still contain a trailing
/// semicolon if the SQL ended with one) preserving the original
/// text — so each statement reaches SPI exactly as the client sent
/// it, modulo the split.
///
/// Empty / whitespace-only / comment-only segments are kept here;
/// the caller trims and drops empties before executing.
fn split_statements(query: &str) -> Result<Vec<&str>, String> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, query)
        .tokenize_with_location()
        .map_err(|e| format!("tokenizer: {e}"))?;

    let mut out = Vec::new();
    let mut start = 0usize;
    for tok in &tokens {
        // SemiColon is the only delimiter pgwire's `'Q'` recognises.
        // The tokenizer has already excluded semicolons inside
        // string literals / comments / dollar-quoted strings, so
        // top-level is the only kind we see.
        if matches!(tok.token, Token::SemiColon) {
            // tok.span.end is exclusive; include the semicolon in
            // the slice so SPI sees identical bytes to what was sent.
            let end = byte_offset(query, tok.span.end.line, tok.span.end.column);
            out.push(&query[start..end]);
            start = end;
        }
    }
    if start < query.len() {
        out.push(&query[start..]);
    }
    Ok(out)
}

/// sqlparser's `Location` is 1-based `(line, column)` over UTF-8
/// chars; convert to a byte offset into the original string.
fn byte_offset(s: &str, line: u64, column: u64) -> usize {
    let mut current_line: u64 = 1;
    let mut current_col: u64 = 1;
    for (idx, ch) in s.char_indices() {
        if current_line == line && current_col == column {
            return idx;
        }
        if ch == '\n' {
            current_line += 1;
            current_col = 1;
        } else {
            current_col += 1;
        }
    }
    s.len()
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

/// sqlparser reported a tokenizer error (malformed input). Surface
/// it with SQLSTATE `42601` (SYNTAX_ERROR) so clients see the same
/// error class real PG would emit for a malformed statement.
fn parse_error_to_pgwire(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42601".to_string(),
        format!("pg_transport parse error: {msg}"),
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
    match caught {
        CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => Some(report),
        CaughtError::RustPanic { ereport, .. } => Some(ereport),
    }
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
