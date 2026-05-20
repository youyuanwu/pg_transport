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

use bytes::Bytes;
use pgrx::pg_sys;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::super::executor::{CachedPlanSource, with_xact};
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
    /// Result-column schema (echoes [`super::PreparedStatement::result_schema`]).
    /// Kept here too so the per-execute response builder can wrap
    /// it in an Arc without re-walking the source.
    base_schema: Vec<FieldInfo>,
}

impl PreparedPlan for DirectBackendPlan {
    /// **Stage A WIP — commit 5 wires Bind+Execute.** Returns
    /// `0A000` (feature_not_supported) so clients see a clean
    /// failure rather than a panic if they reach Execute under
    /// the direct backend before commit 5 lands.
    fn execute(
        &self,
        _parameters: &[Option<Bytes>],
        _parameter_format: &Format,
        _result_format: &Format,
        _max_rows: usize,
    ) -> PgWireResult<Response> {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "0A000".to_string(),
            "pg_transport.execution_backend = 'direct': Execute path not yet implemented (Stage A commit 5). Parse + Describe work; switch back to pg_transport.execution_backend = 'spi' to execute queries.".to_string(),
        ))))
    }
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
