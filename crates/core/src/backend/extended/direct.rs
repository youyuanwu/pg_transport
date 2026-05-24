//! Direct-path extended-query backend — `CreateCachedPlan` +
//! `Portal*` + custom `DestReceiver` that encodes DataRows
//! inline during `PortalRun`.
//!
//! §3.1 direct-path migration (see
//! [deferred/planner-executor-direct-path.md §7](../../../../../docs/design/deferred/planner-executor-direct-path.md)).
//!
//! ## Status
//!
//! **Stage B — Parse, Describe, Bind, Execute all land here.**
//! Selecting `pg_transport.execution_backend = 'direct'` routes
//! extended-query traffic through `CachedPlanSource` + `Portal`
//! with a [`WireDestReceiver`] whose `receiveSlot` callback
//! encodes each row directly into a `BytesMut` — zero
//! intermediate tuplestore copy.
//!
//! ## Parse pipeline
//!
//! Mirrors PG's `exec_parse_message` (postgres.c):
//!
//! 1. [`pg_sys::pg_parse_query`] -> raw parse-tree list. We
//!    process only the first `RawStmt` (matches the SPI path;
//!    multi-stmt Parse is a non-goal for Stage A).
//! 2. [`pg_sys::CreateCommandTag`] for the cache source's tag.
//! 3. Fresh `AllocSetContext` for analyzer output - PG reparents
//!    this into the `CachedPlanSource` at `CompleteCachedPlan`
//!    time and `DropCachedPlan` later `MemoryContextDelete`s it,
//!    so it must NOT be the xact's own context (see
//!    [`super::super::executor::pg_direct_path_smoke`] for the
//!    original bisect of this UAF).
//! 4. [`pg_sys::pg_analyze_and_rewrite_fixedparams`] if the
//!    client sent parameter type hints, else
//!    [`pg_sys::pg_analyze_and_rewrite_varparams`] (analyzer
//!    infers; we upgrade `UNKNOWNOID`/`InvalidOid` to `TEXTOID`
//!    so client-side serialisers don't refuse).
//! 5. [`CachedPlanSource::create`] -> `complete` -> `save` -
//!    promotes the source into `CachedMemoryContext` so it
//!    survives the parse-time xact ending in `with_xact`.
//! 6. Read `(*source).resultDesc` to build the `result_schema`.
//!
//! Replanning (when invalidations happen between Parse and
//! Execute) is automatic via `GetCachedPlan` at execute time;
//! commit 5 will wire that in.

use std::ffi::CString;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use super::super::dest_receiver::{ColumnEncoder, WireDestReceiver, command_tag_name};
use super::super::executor::{CachedPlanSource, ParamList, Portal, with_xact};
use super::super::spi::{TypeInput, TypeReceive, generic_error};
use super::{PreparedPlan, PreparedStatement};

// ---------------------------------------------------------------------------
// DirectBackendPlan — the [`super::PreparedPlan`] impl for the direct path
// ---------------------------------------------------------------------------

/// Direct-path execution state for a single prepared statement.
/// Holds the saved [`CachedPlanSource`] plus pre-computed per-column
/// metadata so the per-execute hot path doesn't re-walk the
/// `resultDesc`.
///
/// Lives behind `Box<dyn PreparedPlan>` inside
/// [`super::PreparedStatement`]. `Send + Sync` is auto-derived
/// because [`CachedPlanSource`] is unsafe-impl Send+Sync (see its
/// doc comment) and the cached `Vec`s are trivially so.
#[allow(dead_code)]
pub(crate) struct DirectBackendPlan {
    /// Saved CachedPlanSource. Freed on Drop via [`CachedPlanSource`]`::Drop`.
    source: CachedPlanSource,
    /// Per-`$n` parameter OIDs (echoes [`super::PreparedStatement::param_types`]).
    param_oids: Vec<pg_sys::Oid>,
    /// Per-result-column type OIDs (echoes [`super::PreparedStatement::result_schema`]).
    column_oids: Vec<pg_sys::Oid>,
    /// Parse-time command tag reused in `PortalDefineQuery`.
    command_tag: pg_sys::CommandTag::Type,
    /// Pre-resolved display name for [`Self::command_tag`] (e.g.
    /// `"SELECT"`, `"INSERT"`). Computed once at Parse from
    /// PG's static `commandTagBuiltinList`, so per-Execute pays
    /// neither the `GetCommandTagName` FFI nor a `String` alloc.
    tag_name: &'static str,
    /// Pre-resolved `command_tag_display_rowcount` flag for
    /// [`Self::command_tag`]. Same motivation as [`Self::tag_name`]:
    /// look it up once at Parse, skip the per-Execute FFI.
    tag_display_rowcount: bool,
    /// Result-column schema with every column's [`FieldFormat`]
    /// set to `Text`. `Arc`-wrapped so the common case
    /// ([`Format::UnifiedText`]) on Execute is a refcount bump
    /// instead of a fresh `Vec<FieldInfo>` + per-column `String`
    /// allocation. Non-text (Individual / UnifiedBinary) Bind
    /// requests rebuild a per-Execute `Vec<FieldInfo>` from this
    /// one, paying the same cost as before.
    base_schema: Arc<Vec<FieldInfo>>,
    /// Cached CString for `PortalDefineQuery` — avoids a heap
    /// alloc per Execute.
    portal_src_text: CString,
}

impl PreparedPlan for DirectBackendPlan {
    /// Execute the direct-path prepared statement via
    /// `GetCachedPlan` + `PortalRun(..., WireDestReceiver, ...)`.
    ///
    /// Parameter format is taken from the portal's
    /// `parameter_format`; per-parameter format can vary if the
    /// Bind sent `parameter_format_codes` of length > 1. Result
    /// format is honoured per-column from the portal's
    /// `result_column_format`.
    fn execute(
        &self,
        parameters: &[Option<Bytes>],
        parameter_format: &Format,
        result_format: &Format,
        max_rows: usize,
    ) -> PgWireResult<Response> {
        execute_impl(self, parameters, parameter_format, result_format, max_rows)
    }
}

/// Free-function body for [`DirectBackendPlan::execute`]; pulled
/// out so the with_xact closure gets the inner borrows cleanly.
fn execute_impl(
    backend: &DirectBackendPlan,
    parameters: &[Option<Bytes>],
    parameter_format: &Format,
    result_format: &Format,
    max_rows: usize,
) -> PgWireResult<Response> {
    if parameters.len() != backend.param_oids.len() {
        return Err(generic_error(
            "pg_transport direct",
            &format!(
                "bind parameter count mismatch: got {}, expected {}",
                parameters.len(),
                backend.param_oids.len()
            ),
        ));
    }

    let base_schema = &backend.base_schema;
    let ncols = base_schema.len();
    let result_format_per_col: Vec<FieldFormat> =
        (0..ncols).map(|i| result_format.format_for(i)).collect();
    // Fast path: when the client asked for all-text results (the
    // pgwire `Format::UnifiedText` default and the common case for
    // pgbench / tokio-postgres), we can ship the cached
    // text-format `Arc<Vec<FieldInfo>>` straight through. Per
    // Bind the cost collapses to one `Arc::clone` (atomic
    // increment) instead of `ncols` `String` allocations + a
    // fresh `Vec<FieldInfo>` + `Arc::new`.
    let schema_arc: Arc<Vec<FieldInfo>> = if matches!(result_format, Format::UnifiedText) {
        Arc::clone(base_schema)
    } else {
        // Slow path: rebuild per-column with the requested
        // format codes. Pays N `String::clone` + 1 `Vec` alloc +
        // 1 `Arc::new`, same as before.
        let rebuilt: Vec<FieldInfo> = base_schema
            .iter()
            .zip(result_format_per_col.iter())
            .map(|(fi, fmt)| {
                FieldInfo::new(
                    fi.name().to_string(),
                    fi.table_id(),
                    fi.column_id(),
                    fi.datatype().clone(),
                    *fmt,
                )
            })
            .collect();
        Arc::new(rebuilt)
    };

    let param_is_binary: Vec<bool> = (0..parameters.len())
        .map(|i| parameter_format.is_binary(i))
        .collect();

    with_xact(|ctx| -> PgWireResult<Response> {
        let (param_values, param_is_null) =
            decode_parameters(parameters, &backend.param_oids, &param_is_binary)?;
        // SAFETY: inside with_xact; Datums are palloc'd in this
        // xact's MemoryContext and live until the closure returns.
        let params =
            unsafe { ParamList::build(&backend.param_oids, &param_values, &param_is_null) };

        // SAFETY: inside with_xact; source is saved/live.
        let plan = unsafe { backend.source.get_plan(&params) };
        let portal = unsafe { Portal::create_anonymous(ctx) };

        unsafe {
            portal.define(
                &backend.portal_src_text,
                backend.command_tag,
                plan.stmt_list(),
                std::ptr::null_mut(),
            );
            portal.start(&params, 0, pg_sys::GetActiveSnapshot());
        }

        let encoders: Vec<ColumnEncoder> = if ncols > 0 {
            (0..ncols)
                .map(|c| {
                    ColumnEncoder::for_column(backend.column_oids[c], result_format_per_col[c])
                })
                .collect()
        } else {
            Vec::new()
        };

        // WireDestReceiver encodes DataRows inline during
        // PortalRun — no tuplestore intermediate. Pre-size to the
        // client-supplied row cap when reasonable (max_rows>0 and
        // small), else a modest default that skips the
        // 0→4→8→16 geometric-growth chain `Vec::new` would pay
        // on any multi-row SELECT.
        const DATA_ROWS_DEFAULT_CAP: usize = 16;
        const DATA_ROWS_HINT_CAP: usize = 1024;
        let cap = if max_rows > 0 && max_rows <= DATA_ROWS_HINT_CAP {
            max_rows
        } else {
            DATA_ROWS_DEFAULT_CAP
        };
        let mut data_rows: Vec<DataRow> = Vec::with_capacity(cap);
        let mut wire_recv = WireDestReceiver::new(&encoders, &mut data_rows, ncols as i16);

        let mut qc: pg_sys::QueryCompletion = Default::default();
        unsafe {
            pg_sys::InitializeQueryCompletion(&mut qc);
        }
        let count = if max_rows == 0 {
            i64::MAX
        } else {
            max_rows as i64
        };
        let _done = unsafe {
            portal.run(
                count,
                true,
                wire_recv.as_dest_receiver(),
                wire_recv.as_dest_receiver(),
                &mut qc,
            )
        };

        if ncols == 0 {
            let mut tag = Tag::new(backend.tag_name);
            if backend.tag_display_rowcount {
                tag = tag.with_rows(qc.nprocessed as usize);
            }
            return Ok(Response::Execution(tag));
        }

        let row_stream = stream::iter(data_rows).map(Ok);
        let mut response = QueryResponse::new(schema_arc, row_stream);
        response.set_command_tag(backend.tag_name);
        Ok(Response::Query(response))
    })
}

// ---------------------------------------------------------------------------
// prepare — Parse path (planner+executor direct)
// ---------------------------------------------------------------------------

/// Parse + plan a SQL string via the direct path. Returns a
/// [`PreparedStatement`] whose `plan` is a [`DirectBackendPlan`]
/// boxed behind [`super::PreparedPlan`].
///
/// `param_hints` is one entry per `$n` parameter the client
/// declared in the Parse message; `Some(oid)` is the OID the
/// client asserted, `None` means "infer from context".
///
/// See the module-level docs for the step-by-step pipeline.
pub fn prepare(sql: &str, param_hints: &[Option<u32>]) -> PgWireResult<PreparedStatement> {
    let sql_cstring = CString::new(sql).map_err(|_| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "08P01".to_string(),
            "pg_transport direct: query string contains a NUL byte".to_string(),
        )))
    })?;
    let sql_owned = sql.to_string();
    let any_hint_present = param_hints.iter().any(|o| o.is_some());

    with_xact(|ctx| -> PgWireResult<PreparedStatement> {
        // 1. Raw parse.
        // SAFETY: inside with_xact; pg_parse_query is the
        // standard PG raw parser. Allocates in CurrentMemoryContext.
        let raw_list = unsafe { pg_sys::pg_parse_query(sql_cstring.as_ptr()) };
        if raw_list.is_null() || unsafe { (*raw_list).length } == 0 {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                "pg_transport direct: empty query string".to_string(),
            ))));
        }

        // First RawStmt only (multi-stmt Parse follows the SPI
        // path's policy of processing only the first statement).
        let raw_stmt = unsafe { (*(*raw_list).elements).ptr_value } as *mut pg_sys::RawStmt;
        if raw_stmt.is_null() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "pg_transport direct: pg_parse_query returned a null RawStmt".to_string(),
            ))));
        }

        // 1b. xact-control short-circuit.
        //
        // BEGIN / COMMIT / ROLLBACK (and their mode-list variants)
        // must NOT reach the `Portal*` path — utility statements
        // (`TransactionStmt`) mis-dispatch there and trip a
        // `0x7f7f7f7f` WIPE_MEM use-after-free, pinned for posterity
        // by `xact_control_begin_via_extended_query_direct_backend_*`
        // in `crates/e2e/tests/basic.rs`. Detection is free here:
        // `classify_raw_stmt` is a pure pointer inspection of the
        // node we already parsed above, so this fix adds zero
        // additional parser work to the non-xact-control hot path.
        //
        // Only meaningful for single-statement Parse — multi-stmt
        // Parse can't be a valid xact-control shape. Returning Ok
        // lets `with_xact`'s cleanup (Pop + Commit) run normally;
        // the actual xact-block API call (which manages its own
        // Start/Begin/Commit sequence) only fires on Execute, by
        // which time we're back in TBLOCK_DEFAULT.
        if unsafe { (*raw_list).length } == 1
            && let Some(cmd) = unsafe { super::super::spi_bridge::classify_raw_stmt(raw_stmt) }
        {
            return Ok(PreparedStatement {
                sql: sql_owned,
                param_types: Vec::new(),
                result_schema: Vec::new(),
                plan: Box::new(super::XactControlBackendPlan::new(cmd)),
            });
        }

        // 2. Command tag from the raw parse tree (used by the
        // CachedPlanSource and later by PortalDefineQuery).
        // SAFETY: CreateCommandTag is a pure walk of the parsetree.
        let command_tag = unsafe { pg_sys::CreateCommandTag((*raw_stmt).stmt) };

        // 3. Fresh AllocSet for analyzer output. This context
        // becomes the CachedPlanSource's `context` at
        // CompleteCachedPlan time and is `MemoryContextDelete`d
        // by DropCachedPlan — must NOT be the xact's own
        // CurrentMemoryContext or we UAF on commit.
        // SAFETY: AllocSetContextCreateInternal is the documented
        // entry point; raises on alloc failure.
        let query_ctx = unsafe {
            pg_sys::AllocSetContextCreateInternal(
                pg_sys::CurrentMemoryContext,
                c"pgxport_query_context".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        };
        // SAFETY: MemoryContextSwitchTo returns the previous ctx.
        let prev_ctx = unsafe { pg_sys::MemoryContextSwitchTo(query_ctx) };

        // 4. Analyze + rewrite. Two modes: client-supplied type
        // hints (fixedparams, refuses InvalidOid) or analyzer
        // infers (varparams, we upgrade UNKNOWNOID/InvalidOid ->
        // TEXTOID so client-side serialisers don't refuse).
        let (querytrees, resolved_oids) = if any_hint_present {
            let hints: Vec<pg_sys::Oid> = param_hints
                .iter()
                .map(|o| pg_sys::Oid::from(o.unwrap_or(0)))
                .collect();
            // SAFETY: fixedparams takes a const *Oid + nparams;
            // queryEnv = NULL is the standard call shape.
            let qt = unsafe {
                pg_sys::pg_analyze_and_rewrite_fixedparams(
                    raw_stmt,
                    sql_cstring.as_ptr(),
                    hints.as_ptr(),
                    hints.len() as i32,
                    std::ptr::null_mut(),
                )
            };
            (qt, hints)
        } else {
            let mut types_ptr: *mut pg_sys::Oid = std::ptr::null_mut();
            let mut n_params: std::ffi::c_int = 0;
            // SAFETY: varparams writes the inferred OID array
            // into CurrentMemoryContext (= query_ctx). It survives
            // because the CachedPlanSource takes ownership.
            let qt = unsafe {
                pg_sys::pg_analyze_and_rewrite_varparams(
                    raw_stmt,
                    sql_cstring.as_ptr(),
                    &mut types_ptr,
                    &mut n_params,
                    std::ptr::null_mut(),
                )
            };
            let n = n_params as usize;
            let mut oids: Vec<pg_sys::Oid> = Vec::with_capacity(n);
            for i in 0..n {
                // SAFETY: i in 0..n_params; analyzer guarantees
                // the array length.
                let oid = unsafe { *types_ptr.add(i) };
                let resolved =
                    if oid == pg_sys::Oid::INVALID || oid.to_u32() == pg_sys::UNKNOWNOID.to_u32() {
                        pg_sys::TEXTOID
                    } else {
                        oid
                    };
                oids.push(resolved);
            }
            (qt, oids)
        };

        // Switch back to the xact context BEFORE the
        // CachedPlanSource takes ownership of query_ctx. Any
        // remaining allocations (CachedPlanSource itself, our
        // result_schema strings) go in the xact context, not the
        // soon-to-be-cached query_ctx.
        // SAFETY: pairs with the switch above.
        unsafe { pg_sys::MemoryContextSwitchTo(prev_ctx) };

        if querytrees.is_null() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "XX000".to_string(),
                "pg_transport direct: analyze returned null".to_string(),
            ))));
        }

        // 5. CachedPlanSource lifecycle: create -> complete -> save.
        // SAFETY: inside with_xact; raw_stmt is from the raw_list
        // we just parsed (CreateCachedPlan copyObject's it into
        // the source's own context).
        let source = unsafe { CachedPlanSource::create(ctx, raw_stmt, &sql_cstring, command_tag) };
        // SAFETY: complete with fixed_result=true so resultDesc
        // is populated immediately (commit 5 reads it again to
        // build per-execute slots).
        unsafe {
            source.complete(
                querytrees,
                query_ctx,
                &resolved_oids,
                0,    // cursor_options: none
                true, // fixed_result
            );
            source.save();
        }

        // 6. Result schema from source.resultDesc.
        // SAFETY: source is live + complete; resultDesc is null
        // for utility / DML-no-RETURNING (returned as empty Vec).
        let base_schema = unsafe { result_schema_from_source(&source) };
        let column_oids: Vec<pg_sys::Oid> = base_schema
            .iter()
            .map(|fi| pg_sys::Oid::from(fi.datatype().oid()))
            .collect();

        // 7. pgwire Type vector for `PreparedStatement::param_types`.
        let param_types: Vec<Type> = resolved_oids
            .iter()
            .map(|oid| Type::from_oid(oid.to_u32()).unwrap_or(Type::UNKNOWN))
            .collect();

        // Share one Arc-wrapped Vec<FieldInfo> between the
        // backend plan (per-Execute hot path) and the
        // PreparedStatement.result_schema clone used by
        // Describe (cold). Per-Execute UnifiedText then
        // `Arc::clone`s this same allocation.
        let base_schema_arc = Arc::new(base_schema);
        let result_schema_for_describe = (*base_schema_arc).clone();

        let backend_plan = DirectBackendPlan {
            source,
            param_oids: resolved_oids,
            column_oids,
            command_tag,
            // SAFETY: command_tag was produced by `CreateCommandTag`
            // on the raw_stmt above; it's a valid enum discriminant
            // for the PG cmdtag table, so `command_tag_name` returns
            // a pointer into `commandTagBuiltinList` (static-lifetime
            // ASCII) and `command_tag_display_rowcount` is a pure
            // table lookup.
            tag_name: command_tag_name(command_tag),
            tag_display_rowcount: unsafe { pg_sys::command_tag_display_rowcount(command_tag) },
            base_schema: base_schema_arc,
            portal_src_text: CString::new("direct-path statement").expect("literal has no NUL"),
        };

        Ok(PreparedStatement {
            sql: sql_owned,
            param_types,
            result_schema: result_schema_for_describe,
            plan: Box::new(backend_plan),
        })
    })
}

/// Walk a saved [`CachedPlanSource`]'s `resultDesc` and pull out
/// the post-analysis result-row schema (column name + type OID).
/// Returns an empty Vec for utility statements and DML without
/// `RETURNING` (matches pgwire's `is_no_data()` semantics).
///
/// # Safety
///
/// `source` must be live (i.e. not yet dropped) and `complete`
/// must have been called with `fixed_result=true`.
unsafe fn result_schema_from_source(source: &CachedPlanSource) -> Vec<FieldInfo> {
    let Some(tupdesc) = (unsafe { source.result_desc() }) else {
        return Vec::new();
    };
    tupdesc
        .iter()
        .map(|attr| {
            // SAFETY: attname is a PG NameData buffer containing
            // an ASCII identifier.
            let name =
                unsafe { super::super::executor::pg_ident_to_string(attr.attname.data.as_ptr()) };
            FieldInfo::new(
                name,
                None,
                None,
                Type::from_oid(attr.atttypid.to_u32()).unwrap_or(Type::TEXT),
                FieldFormat::Text,
            )
        })
        .collect()
}

/// Decode pgwire's `Vec<Option<Bytes>>` into direct-executor
/// parameter vectors: one `Datum` per bind slot plus a null mask.
fn decode_parameters(
    parameters: &[Option<Bytes>],
    param_oids: &[pg_sys::Oid],
    is_binary: &[bool],
) -> PgWireResult<(Vec<pg_sys::Datum>, Vec<bool>)> {
    let n = parameters.len();
    let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(n);
    let mut nulls: Vec<bool> = Vec::with_capacity(n);

    for i in 0..n {
        match &parameters[i] {
            None => {
                values.push(pg_sys::Datum::from(0_usize));
                nulls.push(true);
            }
            Some(bytes) => {
                let datum = if is_binary[i] {
                    TypeReceive::for_type(param_oids[i]).call(bytes)
                } else {
                    TypeInput::for_type(param_oids[i]).call(bytes.as_ref())?
                };
                values.push(datum);
                nulls.push(false);
            }
        }
    }
    Ok((values, nulls))
}

// ---------------------------------------------------------------------------
// WireDestReceiver, ColumnEncoder, command_tag_name lifted to
// `super::super::dest_receiver` (shared with the simple-query
// direct backend per
// [deferred/simple-query-direct-path.md](../../../../../docs/design/deferred/simple-query-direct-path.md)
// §7 reuse map).
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use bytes::Buf;
    use pgrx::pg_test;

    #[pg_test]
    fn pg_direct_prepare_extracts_types_and_schema() {
        let stmt = prepare("SELECT $1::int + 10 AS n", &[]).expect("direct prepare should succeed");
        assert_eq!(stmt.param_types.len(), 1, "expected one inferred parameter");
        assert_eq!(
            stmt.param_types[0],
            Type::INT4,
            "expected inferred type INT4 for $1::int"
        );
        assert_eq!(stmt.result_schema.len(), 1, "expected one result column");
        assert_eq!(stmt.result_schema[0].name(), "n");
        assert_eq!(stmt.result_schema[0].datatype(), &Type::INT4);
    }

    #[pg_test]
    fn pg_direct_execute_select_one_row_text() {
        let stmt = prepare("SELECT $1::int + 10", &[]).expect("direct prepare should succeed");
        let response = stmt
            .execute(
                &[Some(Bytes::from_static(b"5"))],
                &Format::UnifiedText,
                &Format::UnifiedText,
                0,
            )
            .expect("direct execute should succeed");

        let mut query = match response {
            Response::Query(q) => q,
            other => panic!("expected Query response, got: {other:?}"),
        };
        let maybe_row = futures::executor::block_on(async { query.data_rows().next().await });
        let row = maybe_row
            .expect("expected at least one row")
            .expect("row stream item should be Ok");
        assert_eq!(row.field_count, 1);

        let mut data = row.data;
        let len = data.get_i32();
        assert!(len > 0, "first column should be non-null");
        let cell = data.split_to(len as usize);
        let text = std::str::from_utf8(&cell).expect("valid utf8");
        assert_eq!(text, "15");
    }
}
