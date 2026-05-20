//! Direct-path extended-query backend — `CreateCachedPlan` +
//! `Portal*` + Tuplestore destination.
//!
//! Stage A of the §3.1 direct-path migration (see
//! [deferred/planner-executor-direct-path.md §7](../../../../../docs/design/deferred/planner-executor-direct-path.md)).
//!
//! ## Status
//!
//! **Stage A WIP — Parse + Describe land here; Execute is still
//! stubbed (commit 5 wires Bind+Execute).** Selecting
//! `pg_transport.execution_backend = 'direct'` lets clients
//! Parse + Describe their queries through the planner directly,
//! but any attempt to Execute the resulting portal returns
//! SQLSTATE `0A000` (feature_not_supported). SPI (default) is
//! unaffected.
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

use std::ffi::{CStr, CString};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use super::super::executor::{CachedPlanSource, Portal, with_xact};
use super::super::spi::{TypeInput, TypeOutput, TypeReceive, TypeSend, generic_error};
use super::super::tuplestore::{Tuplestore, TuplestoreReceiver};
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
    /// Result-column schema (echoes [`super::PreparedStatement::result_schema`]).
    /// Kept here too so the per-execute response builder can wrap
    /// it in an Arc without re-walking the source.
    base_schema: Vec<FieldInfo>,
}

impl PreparedPlan for DirectBackendPlan {
    /// Execute the direct-path prepared statement via
    /// `GetCachedPlan` + `PortalRun(..., DestTuplestore, ...)`.
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
    let schema_vec: Vec<FieldInfo> = base_schema
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

    let param_bytes: Vec<Option<Bytes>> = parameters.to_vec();
    let param_is_binary: Vec<bool> = (0..parameters.len())
        .map(|i| parameter_format.is_binary(i))
        .collect();

    with_xact(|ctx| -> PgWireResult<Response> {
        let (param_values, param_is_null) =
            decode_parameters(&param_bytes, &backend.param_oids, &param_is_binary)?;
        // SAFETY: makeParamList allocates a ParamListInfo with
        // numParams slots in CurrentMemoryContext.
        let params =
            unsafe { build_param_list(&backend.param_oids, &param_values, &param_is_null) };

        // SAFETY: inside with_xact; source is saved/live.
        let plan = unsafe { backend.source.get_plan(params) };
        let portal = unsafe { Portal::create_anonymous(ctx) };

        let sql_cstr = CString::new("direct-path statement").expect("literal has no NUL");

        unsafe {
            portal.define(
                &sql_cstr,
                backend.command_tag,
                plan.stmt_list(),
                std::ptr::null_mut(),
            );
            portal.start(params, 0, pg_sys::GetActiveSnapshot());
        }

        // Capture rows into a tuplestore so we can encode them
        // after PortalRun returns.
        let store = unsafe { Tuplestore::begin_heap(true, false, pg_sys::work_mem) };
        let target_tupdesc = unsafe { (*backend.source.as_ptr()).resultDesc };
        let receiver = unsafe {
            TuplestoreReceiver::for_tuplestore(
                &store,
                pg_sys::CurrentMemoryContext,
                false,
                target_tupdesc,
            )
        };

        let mut qc: pg_sys::QueryCompletion = Default::default();
        unsafe {
            pg_sys::InitializeQueryCompletion(&mut qc);
        }
        let count = if max_rows == 0 {
            i64::MAX
        } else {
            max_rows as i64
        };
        let _done =
            unsafe { portal.run(count, true, receiver.as_ptr(), receiver.as_ptr(), &mut qc) };
        unsafe {
            pg_sys::tuplestore_rescan(store.as_ptr());
        }

        if ncols == 0 {
            let tag_name = command_tag_name(qc.commandTag);
            let mut tag = Tag::new(&tag_name);
            if unsafe { pg_sys::command_tag_display_rowcount(qc.commandTag) } {
                tag = tag.with_rows(qc.nprocessed as usize);
            }
            return Ok(Response::Execution(tag));
        }

        let encoders: Vec<ColumnEncoder> = (0..ncols)
            .map(|c| ColumnEncoder::for_column(backend.column_oids[c], result_format_per_col[c]))
            .collect();

        let mut data_rows: Vec<DataRow> = Vec::new();
        unsafe {
            let tupdesc = (*backend.source.as_ptr()).resultDesc;
            let slot = pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsMinimalTuple);
            let slot_guard = SlotGuard { raw: slot };
            while pg_sys::tuplestore_gettupleslot(store.as_ptr(), true, true, slot_guard.raw) {
                pg_sys::slot_getallattrs(slot_guard.raw);

                let mut buf = BytesMut::with_capacity(64);
                for (c, encoder) in encoders.iter().enumerate() {
                    let is_null = *(*slot_guard.raw).tts_isnull.add(c);
                    if is_null {
                        buf.put_i32(-1);
                    } else {
                        let datum = *(*slot_guard.raw).tts_values.add(c);
                        let bytes = encoder.encode(datum);
                        buf.put_i32(bytes.len() as i32);
                        buf.put_slice(&bytes);
                    }
                }
                data_rows.push(DataRow::new(buf, ncols as i16));
                pg_sys::ExecClearTuple(slot_guard.raw);
            }
        }

        let tag_name = command_tag_name(qc.commandTag);
        let schema_arc = Arc::new(schema_vec);
        let row_stream = stream::iter(data_rows).map(Ok);
        let mut response = QueryResponse::new(schema_arc, row_stream);
        response.set_command_tag(&tag_name);
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

        let backend_plan = DirectBackendPlan {
            source,
            param_oids: resolved_oids,
            column_oids,
            command_tag,
            base_schema: base_schema.clone(),
        };

        Ok(PreparedStatement {
            sql: sql_owned,
            param_types,
            result_schema: base_schema,
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
    let src = source.as_ptr();
    if src.is_null() {
        return Vec::new();
    }
    // SAFETY: resultDesc field is a stable TupleDesc across PG 12+;
    // null when the statement produces no rows.
    let tupdesc = unsafe { (*src).resultDesc };
    if tupdesc.is_null() {
        return Vec::new();
    }
    // SAFETY: natts is the canonical attribute count.
    let ncols = unsafe { (*tupdesc).natts } as usize;
    let mut out = Vec::with_capacity(ncols);
    for i in 0..ncols {
        // SAFETY: i in 0..natts.
        let attr = unsafe { &*pg_sys::TupleDescAttr(tupdesc, i as i32) };
        let name = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        out.push(FieldInfo::new(
            name,
            None,
            None,
            Type::from_oid(attr.atttypid.to_u32()).unwrap_or(Type::TEXT),
            FieldFormat::Text,
        ));
    }
    out
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

/// Build a PG `ParamListInfo` from decoded values. Caller keeps
/// ownership in CurrentMemoryContext; no explicit free is needed.
///
/// # Safety
///
/// Must run inside an active transaction with a valid
/// `CurrentMemoryContext`.
unsafe fn build_param_list(
    param_oids: &[pg_sys::Oid],
    values: &[pg_sys::Datum],
    is_null: &[bool],
) -> pg_sys::ParamListInfo {
    let n = param_oids.len();
    let params = unsafe { pg_sys::makeParamList(n as i32) };
    if params.is_null() {
        return std::ptr::null_mut();
    }

    let base = unsafe { (*params).params.as_mut_ptr() };
    for i in 0..n {
        let slot = unsafe { base.add(i) };
        unsafe {
            (*slot).value = values[i];
            (*slot).isnull = is_null[i];
            (*slot).pflags = pg_sys::PARAM_FLAG_CONST as u16;
            (*slot).ptype = param_oids[i];
        }
    }
    params
}

/// Per-column result encoder. Holds the cached
/// (typoutput | typsend) lookup so the row loop just calls
/// `encoder.encode(datum)` per cell.
enum ColumnEncoder {
    Text(TypeOutput),
    Binary(TypeSend),
}

impl ColumnEncoder {
    fn for_column(type_oid: pg_sys::Oid, format: FieldFormat) -> Self {
        match format {
            FieldFormat::Text => Self::Text(TypeOutput::for_type(type_oid)),
            FieldFormat::Binary => Self::Binary(TypeSend::for_type(type_oid)),
        }
    }

    fn encode(&self, datum: pg_sys::Datum) -> Vec<u8> {
        match self {
            Self::Text(fns) => fns.call(datum),
            Self::Binary(fns) => fns.call(datum),
        }
    }
}

/// Small RAII guard for a single tuple table slot created by
/// `MakeSingleTupleTableSlot`.
struct SlotGuard {
    raw: *mut pg_sys::TupleTableSlot,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { pg_sys::ExecDropSingleTupleTableSlot(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }
}

/// Convert a `CommandTag` enum to its display name.
fn command_tag_name(tag: pg_sys::CommandTag::Type) -> String {
    let ptr = unsafe { pg_sys::GetCommandTagName(tag) };
    if ptr.is_null() {
        "OK".to_string()
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

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
