//! Direct-path extended-query backend — `CreateCachedPlan` +
//! `Portal*` + Tuplestore destination.
//!
//! Stage A of the §3.1 direct-path migration (see
//! [deferred/planner-executor-direct-path.md §7](../../../../../docs/design/deferred/planner-executor-direct-path.md)).
//!
//! ## Status
//!
//! **Stage A WIP — `prepare` returns "not yet implemented".** The
//! GUC `pg_transport.execution_backend = 'direct'` route reaches
//! this module but errors out at Parse time until the impl lands
//! in a follow-up commit. SPI (default) is unaffected.

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::PreparedStatement;

/// Parse a SQL string via the direct path: `pg_parse_query` +
/// `pg_analyze_and_rewrite_*` + `CreateCachedPlanForQuery` +
/// `CompleteCachedPlan` + `SaveCachedPlan`.
///
/// **Not yet implemented.** Returns an `ErrorResponse` with
/// SQLSTATE `0A000` (feature_not_supported) so clients see a
/// clean failure rather than an opaque panic if they select the
/// `direct` backend before Stage A's Parse path lands.
pub fn prepare(_sql: &str, _param_hints: &[Option<u32>]) -> PgWireResult<PreparedStatement> {
    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "pg_transport.execution_backend = 'direct' selects the planner+executor \
         direct path, which is not yet implemented in this build. \
         Set pg_transport.execution_backend = 'spi' (the default) to use SPI."
            .to_string(),
    ))))
}
