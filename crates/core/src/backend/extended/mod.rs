//! Extended-query backend dispatch — Parse / Bind / Execute,
//! pluggable between the SPI bridge (v0 default, stable) and the
//! planner+executor direct path (Stage A, opt-in via
//! `pg_transport.execution_backend = 'direct'`).
//!
//! The wire layer ([`crate::wire::extended`]) calls
//! [`prepare`] and [`PreparedStatement::execute`] without knowing
//! which backend is active — the GUC selects the backend at Parse
//! time and the choice is sticky on the resulting statement (the
//! statement carries a `Box<dyn PreparedPlan>` whose impl is
//! provided by either [`spi`] or [`direct`]).
//!
//! ## Module layout
//!
//! - [`spi`] — `SPI_prepare` + `SPI_execute_plan` path. v0 default.
//!   Mirrors what [`super::spi_bridge`] does for simple-query.
//! - [`direct`] — `CreateCachedPlan` + `Portal*` + Tuplestore
//!   destination path. Stage A of the §3.1 direct-path migration
//!   (see [deferred/planner-executor-direct-path.md](../../../../docs/design/deferred/planner-executor-direct-path.md)).
//!   Opt-in until soak completes; default flips after.
//!
//! Both backends produce a [`PreparedStatement`] with the same
//! wire-facing surface (`sql`, `param_types`, `result_schema`), so
//! the wire layer's `get_parameter_types` / `get_result_schema` /
//! `do_describe_*` callbacks are backend-agnostic.

pub mod direct;
pub mod spi;

use std::ffi::CString;

use bytes::Bytes;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::error::PgWireResult;

use super::observability::{DebugQueryGuard, StatementTimeoutGuard};
use super::spi::generic_error;
use crate::guc::{self, ExecutionBackend};

/// What the wire layer stores in pgwire's `StoredStatement` for a
/// successfully parsed query.
///
/// The `plan` field is backend-specific (SPI or direct) behind a
/// `Box<dyn PreparedPlan>`; the wire layer reads `sql`,
/// `param_types`, and `result_schema` directly without caring
/// which backend produced the statement.
pub struct PreparedStatement {
    /// Original SQL string, retained for diagnostics / logging.
    pub sql: String,
    /// Resolved parameter types (after merging client hints with
    /// the analyzer's inferred types). One entry per `$n`
    /// placeholder.
    pub param_types: Vec<Type>,
    /// Result-column schema. Empty for utility / DML-no-RETURNING
    /// statements (matches pgwire's `is_no_data()` semantics).
    pub result_schema: Vec<FieldInfo>,
    /// Backend-specific plan handle. Drops itself (and releases
    /// any underlying PG resource — `SPI_freeplan` for SPI,
    /// `ReleaseCachedPlan` for direct) when the statement is
    /// dropped.
    plan: Box<dyn PreparedPlan>,
}

/// Backend-specific execution surface for a prepared statement.
///
/// Both [`spi::SpiBackendPlan`] and (in a later commit)
/// [`direct::DirectBackendPlan`] implement this trait. The plan
/// owns its own PG handle (SPI plan or CachedPlanSource) and any
/// cached per-column metadata it needs.
pub trait PreparedPlan: Send + Sync + 'static {
    /// Execute the plan with the bound parameters and return a
    /// pgwire `Response`. `max_rows` of `0` means "no limit".
    fn execute(
        &self,
        parameters: &[Option<Bytes>],
        parameter_format: &Format,
        result_format: &Format,
        max_rows: usize,
    ) -> PgWireResult<Response>;
}

impl PreparedStatement {
    /// Execute this statement's plan. The wire layer's `do_query`
    /// callback funnels through here; backend dispatch is
    /// already-resolved at this point because the plan was created
    /// by whichever backend won the GUC check at Parse time.
    ///
    /// Pins `debug_query_string` and pgstat `STATE_RUNNING` to
    /// this statement's SQL for the duration of the plan call so
    /// `pg_stat_statements`, `auto_explain`,
    /// `pg_stat_activity.query`, and the server-log `STATEMENT:`
    /// line all attribute correctly (review 2026-05-24 §6.5).
    pub fn execute(
        &self,
        parameters: &[Option<Bytes>],
        parameter_format: &Format,
        result_format: &Format,
        max_rows: usize,
    ) -> PgWireResult<Response> {
        // `self.sql` was accepted by the analyzer at prepare time,
        // so a parser-rejected interior NUL is impossible.
        let sql_cstr =
            CString::new(self.sql.as_str()).expect("prepared SQL is parser-validated NUL-free");
        // SAFETY: slot bgworker context; `sql_cstr` outlives
        // `_guard` (drops last).
        let _guard = unsafe { DebugQueryGuard::install(sql_cstr.as_c_str()) };
        // Arm statement_timeout for the duration of this Execute.
        // Closes review 2026-05-24 §6 item 6 for the extended path.
        let _stmt_timeout = unsafe { StatementTimeoutGuard::install() };
        self.plan
            .execute(parameters, parameter_format, result_format, max_rows)
    }
}

/// Parse + plan a SQL string. Reads `pg_transport.execution_backend`
/// to pick the SPI or direct backend; the returned
/// [`PreparedStatement`] is sticky on that choice (re-executing it
/// later does not re-consult the GUC).
///
/// Pins `debug_query_string` and pgstat `STATE_RUNNING` for the
/// duration of the parse + plan work so `pg_stat_statements`'s
/// `post_parse_analyze_hook` attributes correctly (review
/// 2026-05-24 §6.5).
pub fn prepare(sql: &str, param_hints: &[Option<u32>]) -> PgWireResult<PreparedStatement> {
    let sql_cstr = CString::new(sql)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;
    // SAFETY: slot bgworker context; `sql_cstr` outlives `_guard`.
    let _guard = unsafe { DebugQueryGuard::install(sql_cstr.as_c_str()) };
    // Arm statement_timeout for parse + plan. Mirrors vanilla
    // exec_parse_message, which arms via start_xact_command at
    // the top of parse-time. Drops at end of prepare.
    let _stmt_timeout = unsafe { StatementTimeoutGuard::install() };
    match guc::execution_backend() {
        ExecutionBackend::Spi => spi::prepare(sql, param_hints),
        ExecutionBackend::Direct => direct::prepare(sql, param_hints),
    }
}

// ---------------------------------------------------------------------------
// Shared backend plan for xact-control (Parse + Bind + Execute)
// ---------------------------------------------------------------------------

/// Backend plan for transaction-control statements (BEGIN /
/// COMMIT / ROLLBACK and their mode-list variants) routed
/// through Parse + Bind + Execute.
///
/// Both extended-query backends short-circuit on xact-control
/// before they would otherwise reach a backend-specific path
/// that can't dispatch it correctly:
///
/// * **SPI backend** ([`spi::prepare`]) — `SPI_execute_plan` in
///   atomic mode rejects xact-control with `SPI_ERROR_TRANSACTION`.
/// * **Direct backend** ([`direct::prepare`]) — the `Portal*`
///   path mis-dispatches `TransactionStmt` and trips a
///   `0x7f7f7f7f` WIPE_MEM use-after-free.
///
/// Both backends instead build a `PreparedStatement` whose plan
/// is this type, classified at Parse time via
/// [`super::spi_bridge::classify_single_statement`]. On Execute,
/// the dispatch funnels through
/// [`super::spi_bridge::handle_xact_control_one`] — the same
/// xact-block API path the simple-query bridge uses.
///
/// Carries no per-Execute state beyond the classified
/// `XactCmd`; parameter binding is rejected at Execute time
/// because xact-control has no `$n` placeholders.
pub(super) struct XactControlBackendPlan {
    cmd: super::spi_bridge::XactCmd,
}

impl XactControlBackendPlan {
    pub(super) fn new(cmd: super::spi_bridge::XactCmd) -> Self {
        Self { cmd }
    }
}

impl PreparedPlan for XactControlBackendPlan {
    fn execute(
        &self,
        parameters: &[Option<Bytes>],
        _parameter_format: &Format,
        _result_format: &Format,
        _max_rows: usize,
    ) -> PgWireResult<Response> {
        if !parameters.is_empty() {
            return Err(super::spi::generic_error(
                "pg_transport extended",
                &format!(
                    "transaction-control statement takes no parameters; got {}",
                    parameters.len()
                ),
            ));
        }
        super::spi_bridge::handle_xact_control_one(self.cmd)
    }
}
