//! Extended-query wire handler — Parse / Bind / Describe / Execute /
//! Sync / Close on top of pgwire's [`ExtendedQueryHandler`] trait.
//!
//! Defers all the protocol-framing default behaviour (ParseComplete,
//! BindComplete, CloseComplete, ReadyForQuery, etc.) to pgwire and
//! provides only the three callbacks that need real work:
//!
//! * [`PgTransportQueryParser::parse_sql`] — calls
//!   [`crate::backend::extended::prepare`] to do `SPI_prepare` +
//!   `SPI_keepplan` and pull back the parameter and result schemas.
//! * [`PgTransportQueryParser::get_parameter_types`] /
//!   [`get_result_schema`](PgTransportQueryParser::get_result_schema)
//!   — return the cached schemas (no PG calls; these are read off
//!   the [`PgTransportStatement`]).
//! * [`PgTransportExtendedQuery::do_query`] — calls
//!   [`crate::backend::extended::execute`] to bind parameters,
//!   `SPI_execute_plan`, and materialise the result.
//!
//! Per [backend-wire.md §8 Q1](../../../../docs/design/backend-wire.md#8-open-questions),
//! the wire layer owns the statement *names* (via pgwire's
//! `PortalStore`) and the SPI bridge owns the *plans*. Per-handoff
//! reset is "drop the pgwire `DefaultClient`", which is what
//! happens naturally when `process_socket` returns — the
//! `MemPortalStore<PgTransportStatement>` drops, each `Arc<...>`
//! reaches zero, and our [`SpiPlan`]'s `Drop` impl calls
//! `SPI_freeplan`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response,
};
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;

use crate::backend::extended::{self as spi_ext, PreparedStatement};
use futures::Sink;

/// The statement value pgwire stores in its `PortalStore`. We wrap
/// the SPI-side [`PreparedStatement`] in an `Arc` so pgwire's
/// `Portal` (which holds `Arc<StoredStatement<S>>`) can share
/// ownership without us paying for a `Clone` on every Bind.
///
/// pgwire requires `S: Clone + Send + Sync + 'static` on the
/// statement type; `Arc<T>` gives us cheap clone and inherits Send/
/// Sync from the inner type, which `PreparedStatement` provides via
/// its unsafe-impl'd `SpiPlan`.
#[derive(Clone)]
pub struct PgTransportStatement(pub Arc<PreparedStatement>);

/// `QueryParser` that calls [`spi_ext::prepare`] on Parse.
///
/// pgwire's default `on_parse` calls `query_parser().parse_sql(...)`
/// and then `client.portal_store().put_statement(...)`. Errors here
/// propagate as `ErrorResponse` with our SPI-derived SQLSTATE +
/// message.
#[derive(Default)]
pub struct PgTransportQueryParser;

#[async_trait]
impl QueryParser for PgTransportQueryParser {
    type Statement = PgTransportStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        pgrx::log!(
            "pgwire-v3 extended parse: {sql:?} (param hints: {n})",
            n = types.len()
        );
        let hints: Vec<Option<u32>> = types.iter().map(|t| t.as_ref().map(|t| t.oid())).collect();
        let prepared = spi_ext::prepare(sql, &hints)?;
        Ok(PgTransportStatement(Arc::new(prepared)))
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt.0.param_types.clone())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // Phase 9: we always emit text-format results (see
        // backend/extended.rs module docs). The requested column
        // format is therefore ignored; the FieldInfo's
        // FieldFormat::Text already matches what we'll send.
        Ok(stmt.0.result_schema.clone())
    }
}

/// `ExtendedQueryHandler` impl. Inherits all the default message-
/// framing behaviour (ParseComplete, BindComplete, CloseComplete,
/// ReadyForQuery via Sync) and provides the four required hooks:
/// `query_parser`, `do_query`, `do_describe_statement`,
/// `do_describe_portal`.
#[derive(Default)]
pub struct PgTransportExtendedQuery {
    parser: Arc<PgTransportQueryParser>,
}

#[async_trait]
impl ExtendedQueryHandler for PgTransportExtendedQuery {
    type Statement = PgTransportStatement;
    type QueryParser = PgTransportQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        pgwire::error::PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = &portal.statement.statement.0;
        pgrx::log!(
            "pgwire-v3 extended execute: {sql:?} params={n} max_rows={max_rows}",
            sql = stmt.sql,
            n = portal.parameters.len()
        );
        stmt.execute(
            &portal.parameters,
            &portal.parameter_format,
            &portal.result_column_format,
            max_rows,
        )
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &pgwire::api::stmt::StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        pgwire::error::PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // We deliberately don't fall through to the default
        // implementation (which calls `query_parser().get_*` again
        // and adds a parameter-type-merge step). Our cached schemas
        // already reflect the SPI-resolved param types; merging
        // with `target.parameter_types` would re-introduce any
        // client-supplied 0-OID hints that the planner has already
        // resolved.
        let stmt = &target.statement.0;
        Ok(DescribeStatementResponse::new(
            stmt.param_types.clone(),
            stmt.result_schema.clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        pgwire::error::PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = &target.statement.statement.0;
        Ok(DescribePortalResponse::new(stmt.result_schema.clone()))
    }
}
