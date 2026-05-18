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

use std::any::Any;
use std::ffi::CStr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures::StreamExt;
use futures::stream;
use pgrx::bgworkers::BackgroundWorker;
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
pub fn execute_simple_query(query: &str) -> PgWireResult<Vec<Response>> {
    // BackgroundWorker::transaction wraps the body in
    // StartTransactionCommand / CommitTransactionCommand and a
    // PgTryBuilder catch — without this, Spi::connect would assert
    // (no active transaction snapshot). The query string is borrowed
    // through an AssertUnwindSafe wrapper so the UnwindSafe bound
    // doesn't require interior mutability.
    let query_owned = query.to_string();
    let outcome: Result<Result<Vec<Response>, PgWireError>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| {
            BackgroundWorker::transaction(|| run_via_spi(&query_owned))
        }));

    match outcome {
        Ok(Ok(responses)) => Ok(responses),
        Ok(Err(err)) => Err(err),
        Err(panic_payload) => Err(panic_to_pgwire(panic_payload)),
    }
}

fn run_via_spi(query: &str) -> Result<Vec<Response>, PgWireError> {
    // Spi::connect runs the closure inside an SPI_connect / SPI_finish
    // pair. PG ERRORs from inside panic out; the outer
    // `execute_simple_query` catches them.
    pgrx::Spi::connect(|client| -> Result<Vec<Response>, PgWireError> {
        client
            .select(query, None, &[])
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
/// pgrx's `PgTryBuilder` (used inside `BackgroundWorker::transaction`)
/// catches PG ERROR longjmps and re-raises them via
/// `resume_unwind(Box::new(CaughtError::…))`, so the outer
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
