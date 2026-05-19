//! Extended-query SPI bridge — Parse / Bind / Execute backed by
//! `SPI_prepare` + `SPI_execute_plan`.
//!
//! Layered alongside [`super::spi_bridge`] (simple-query); both
//! share the same xact-wrapping discipline. Per
//! [backend-wire.md §6](../../../../docs/design/backend-wire.md) +
//! [§8 Q1](../../../../docs/design/backend-wire.md#8-open-questions),
//! the wire layer owns the prepared-statement and portal *names*
//! (via pgwire's `PortalStore`); SPI owns the *plans*. We hand the
//! wire layer back a [`SpiPlan`] that frees itself on Drop, so
//! per-handoff reset is "drop the pgwire portal store" — which the
//! current handoff structure already does, because we build a fresh
//! [`crate::wire::pgwire_v3::PgTransportHandlers`] (and therefore a
//! fresh pgwire `DefaultClient` with its own per-connection store)
//! per `process_socket` call.
//!
//! ## Parameter formats
//!
//! v0 phase 9 supports both **text** and **binary** wire-format
//! parameters by routing through PG's per-type `typinput` /
//! `typreceive` functions (`OidInputFunctionCall` and
//! `OidReceiveFunctionCall`). Text format is mandatory because some
//! clients (psql) never opt into binary; binary is required because
//! tokio-postgres binds parameters in binary by default for any type
//! it knows how to serialise.
//!
//! ## Result formats
//!
//! Result columns honour the per-column format requested in the
//! `Bind` message (`result_column_format_codes`). For each column:
//!
//! * **Text format** (or column-unspecified) — emit the bytes that
//!   the type's `typoutput` function produces (same as the simple-
//!   query path uses, via `SPI_getvalue`).
//! * **Binary format** — call the type's `typsend` function via
//!   `OidSendFunctionCall`, then pull the raw bytes out of the
//!   returned `bytea` (skipping the varlena header).
//!
//! tokio-postgres' typed accessors default to binary format for
//! every type it knows how to deserialise (the common numeric +
//! string types), so without this code path `client.query(...)` +
//! `row.get::<_, i32>(0)` would fail with "error deserializing
//! column 0" (4 bytes expected, 1 text byte received).

use std::any::Any;
use std::ffi::{CStr, CString};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys::panic::{CaughtError, ErrorReportWithLevel};
use pgrx::pg_sys::{self};
use pgrx::varlena;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

// ---------------------------------------------------------------------------
// SpiPlan — RAII wrapper around a kept SPI plan
// ---------------------------------------------------------------------------

/// Owned, kept SPI plan pointer. `Drop` calls `SPI_freeplan`, which
/// is documented to work outside an SPI session for plans that were
/// promoted via `SPI_keepplan` (see PG `src/backend/executor/spi.c`).
///
/// pgwire's `PortalStore` requires `Send + Sync` on the statement
/// type; the unsafe impls below are justified by the slot bgworker
/// being single-threaded — the plan is only ever touched from the
/// slot's own thread, where the tokio current-thread runtime runs
/// our handlers. There is no cross-thread access.
pub struct SpiPlan {
    ptr: pg_sys::SPIPlanPtr,
}

// SAFETY: see SpiPlan doc comment — slot is single-threaded.
unsafe impl Send for SpiPlan {}
unsafe impl Sync for SpiPlan {}

impl Drop for SpiPlan {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // SAFETY: ptr was returned by SPI_prepare + SPI_keepplan in
        // [`prepare`]; we are the only owner; SPI_freeplan works
        // outside an SPI session for kept plans. If it fails (e.g.
        // negative return code) we have nothing useful to do — the
        // plan is leaked at worst, the slot continues. We log via
        // pgrx::log so a real leak shows up in the server log.
        unsafe {
            let rc = pg_sys::SPI_freeplan(self.ptr);
            if rc != 0 {
                pgrx::log!("pg_transport extended: SPI_freeplan returned {rc}");
            }
        }
    }
}

impl SpiPlan {
    /// Raw pointer accessor. Caller must hold the slot's single
    /// thread of execution.
    fn as_ptr(&self) -> pg_sys::SPIPlanPtr {
        self.ptr
    }
}

// ---------------------------------------------------------------------------
// PreparedStatement — wire-layer-visible result of [`prepare`]
// ---------------------------------------------------------------------------

/// What the wire layer stores in pgwire's `StoredStatement` for a
/// successfully parsed query. Holds the [`SpiPlan`] alive until the
/// statement is dropped (i.e. `Close(Statement)` or connection
/// teardown).
pub struct PreparedStatement {
    /// Original SQL string, retained for diagnostics / logging.
    pub sql: String,
    /// Kept SPI plan. Freed on Drop.
    pub plan: SpiPlan,
    /// Resolved parameter types (after merging client hints with
    /// SPI's inferred types). One entry per `$n` placeholder.
    pub param_types: Vec<Type>,
    /// Result-column schema. Empty for utility / DML-no-RETURNING
    /// statements (matches pgwire's `is_no_data()` semantics).
    pub result_schema: Vec<FieldInfo>,
}

// ---------------------------------------------------------------------------
// Parameter-type inference (pre-pass before SPI_prepare)
// ---------------------------------------------------------------------------
//
// `SPI_prepare(sql, nargs, argtypes)` runs the analyzer in
// `fixedparams` mode which refuses `InvalidOid` entries with
// "could not determine data type of parameter $N". Clients that
// send `Parse` with empty `type_oids` (tokio-postgres' default
// path for a typical `client.query(...)` call) leave us with no
// types to declare.
//
// We resolve those upfront with a single-statement pre-pass:
// `pg_parse_query` to get the RawStmt list, then
// `pg_analyze_and_rewrite_varparams` to run the analyzer in
// variable-params mode — the same mode `parse_analyze_varparams`
// uses inside real PG's `exec_parse_message`. The analyzer
// palloc's an Oid array and updates it through our `Oid**` /
// `int*` pointers; we copy the resolved OIDs into a Rust `Vec`
// before the analyzer's memory context goes away, then pass them
// to `SPI_prepare` as fixed types.
//
// (We tried `SPI_prepare_params` + `setup_parse_variable_parameters`
// to do this in one pass, but the inferred-OID array landed in a
// memory context that gets clobbered after analysis returns,
// leaving us reading `0x7F7F7F7F` post-call. The two-pass
// approach is ~5µs extra parse + analyze per Parse message,
// acceptable for v0.)

/// Run pg_parse_query + pg_analyze_and_rewrite_varparams to infer
/// the parameter OIDs for a SQL string that arrived without
/// client-supplied type hints. Returns a Vec of resolved OIDs;
/// any parameter the analyzer left as `UNKNOWNOID` is upgraded to
/// `TEXTOID` (matching real PG's `check_variable_parameters`
/// behaviour for the wire-level `Parse` path).
///
/// SAFETY: must be called inside an SPI session (which sets up
/// the snapshot + queryEnv the analyzer needs). Raises PG ERROR
/// on syntax errors / analysis failures, which our caller catches
/// via the outer `catch_unwind`.
unsafe fn infer_param_types(sql: &CString) -> Vec<pg_sys::Oid> {
    let raw_list = unsafe { pg_sys::pg_parse_query(sql.as_ptr()) };
    if raw_list.is_null() {
        return Vec::new();
    }
    let length = unsafe { (*raw_list).length } as usize;
    if length == 0 {
        return Vec::new();
    }
    // For multi-statement Parse strings, fall back to running the
    // analyzer just on the first RawStmt. v0's extended path is
    // single-statement in practice (tokio-postgres splits on
    // boundaries before sending Parse); a multi-statement Parse
    // would also confuse SPI_prepare itself.
    let elements = unsafe { (*raw_list).elements };
    let first_raw = unsafe { (*elements).ptr_value } as *mut pg_sys::RawStmt;
    if first_raw.is_null() {
        return Vec::new();
    }

    let mut types_ptr: *mut pg_sys::Oid = std::ptr::null_mut();
    let mut n_params: std::ffi::c_int = 0;
    let _ = unsafe {
        pg_sys::pg_analyze_and_rewrite_varparams(
            first_raw,
            sql.as_ptr(),
            &mut types_ptr,
            &mut n_params,
            std::ptr::null_mut(),
        )
    };
    // Copy out immediately, before PG resets / reuses the
    // analyzer's CurrentMemoryContext. UNKNOWNOID is the
    // analyzer's fallback for unresolvable parameters; upgrade
    // those to TEXTOID so client-side serialisers don't refuse
    // (tokio-postgres won't serialise i32 for type "Unknown").
    let n = n_params as usize;
    let mut out: Vec<pg_sys::Oid> = Vec::with_capacity(n);
    for i in 0..n {
        let oid = unsafe { *types_ptr.add(i) };
        let resolved = if oid == pg_sys::Oid::INVALID || oid.to_u32() == pg_sys::UNKNOWNOID.to_u32()
        {
            pg_sys::TEXTOID
        } else {
            oid
        };
        out.push(resolved);
    }
    out
}

// ---------------------------------------------------------------------------
// prepare — Parse path
// ---------------------------------------------------------------------------

/// SPI_prepare a query string with the client-supplied parameter
/// type hints; return a [`PreparedStatement`] with a kept plan,
/// resolved parameter OIDs, and the result schema (if any).
///
/// `param_hints` is one entry per `$n` parameter the client
/// declared in the Parse message; `Some(oid)` is the OID the
/// client asserted, `None` means "infer from context".
///
/// Inference path: when `param_hints` is empty *or* all `None`,
/// we run a varparams pre-pass (see [`infer_param_types`]) and
/// pass the resolved OIDs to `SPI_prepare` as fixed types. Then
/// the plan is promoted via `SPI_keepplan` so it survives
/// `SPI_finish`.
///
/// Wraps the whole thing in our own xact + catch_unwind, like
/// [`super::spi_bridge::run_spi_statement`].
pub fn prepare(sql: &str, param_hints: &[Option<u32>]) -> PgWireResult<PreparedStatement> {
    let sql_cstring = CString::new(sql)
        .map_err(|_| generic_error("pg_transport", "query string contains a NUL byte"))?;
    let sql_owned = sql.to_string();

    // Decide upfront whether we need inference. If ANY hint is
    // Some, we trust the client's hint vector wholesale (with 0/
    // UNKNOWN substituted for None entries — PG's fixedparams
    // analyzer accepts UNKNOWN as long as the cast context fills
    // it in, but raises if it can't).
    let any_hint_present = param_hints.iter().any(|o| o.is_some());

    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome: Result<Result<PreparedStatement, PgWireError>, Box<dyn Any + Send>> = catch_unwind(
        AssertUnwindSafe(|| -> Result<PreparedStatement, PgWireError> {
            unsafe {
                let rc = pg_sys::SPI_connect();
                if rc != pg_sys::SPI_OK_CONNECT as i32 {
                    return Err(spi_rc_error("SPI_connect", rc));
                }
            }

            // Resolve types (either echo client hints, or run
            // varparams inference).
            let mut arg_oids: Vec<pg_sys::Oid> = if any_hint_present {
                param_hints
                    .iter()
                    .map(|o| pg_sys::Oid::from(o.unwrap_or(0)))
                    .collect()
            } else {
                unsafe { infer_param_types(&sql_cstring) }
            };
            let n_args = arg_oids.len() as i32;

            let plan_ptr = unsafe {
                pg_sys::SPI_prepare(
                    sql_cstring.as_ptr(),
                    n_args,
                    if arg_oids.is_empty() {
                        std::ptr::null_mut()
                    } else {
                        arg_oids.as_mut_ptr()
                    },
                )
            };

            if plan_ptr.is_null() {
                let rc = unsafe { pg_sys::SPI_result };
                unsafe { pg_sys::SPI_finish() };
                return Err(spi_rc_error("SPI_prepare", rc));
            }

            // SPI_prepare with resolved fixed types stores them
            // on plan->nargs / plan->argtypes; SPI_getargcount
            // and SPI_getargtypeid return them.
            let arg_count = unsafe { pg_sys::SPI_getargcount(plan_ptr) } as usize;
            let resolved_types: Vec<Type> = (0..arg_count)
                .map(|i| {
                    let oid = unsafe { pg_sys::SPI_getargtypeid(plan_ptr, i as i32) }.to_u32();
                    Type::from_oid(oid).unwrap_or(Type::UNKNOWN)
                })
                .collect();

            let result_schema = unsafe { extract_result_schema(plan_ptr) };

            let keep_rc = unsafe { pg_sys::SPI_keepplan(plan_ptr) };
            if keep_rc != 0 {
                unsafe { pg_sys::SPI_finish() };
                return Err(spi_rc_error("SPI_keepplan", keep_rc));
            }

            let finish_rc = unsafe { pg_sys::SPI_finish() };
            if finish_rc != pg_sys::SPI_OK_FINISH as i32 {
                return Err(spi_rc_error("SPI_finish", finish_rc));
            }

            Ok(PreparedStatement {
                sql: sql_owned,
                plan: SpiPlan { ptr: plan_ptr },
                param_types: resolved_types,
                result_schema,
            })
        }),
    );

    finalize_xact(outcome)
}

/// Walk the plan's first `CachedPlanSource` and pull out the result
/// schema via its `resultDesc`. Returns an empty Vec for utility
/// statements / queries without a result, and (importantly) also
/// returns empty when the plan source defers result-desc resolution
/// to plan-cache time — in which case the caller will fill the
/// schema on the first Execute (post-plan execution leaves a tupdesc
/// on `SPI_tuptable`).
///
/// SAFETY: `plan` is a valid kept plan pointer; the returned
/// `*mut List` of `CachedPlanSource*` is valid while the plan is.
unsafe fn extract_result_schema(plan: pg_sys::SPIPlanPtr) -> Vec<FieldInfo> {
    let list = unsafe { pg_sys::SPI_plan_get_plan_sources(plan) };
    if list.is_null() {
        return Vec::new();
    }
    let length = unsafe { (*list).length };
    if length == 0 {
        return Vec::new();
    }
    let elements = unsafe { (*list).elements };
    let first = unsafe { (*elements).ptr_value } as *mut pg_sys::CachedPlanSource;
    if first.is_null() {
        return Vec::new();
    }
    let tupdesc = unsafe { (*first).resultDesc };
    if tupdesc.is_null() {
        return Vec::new();
    }
    let ncols = unsafe { (*tupdesc).natts } as usize;
    let mut out = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let attr = unsafe { &*pg_sys::TupleDescAttr(tupdesc, i as i32) };
        let name = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        let pgwire_type = Type::from_oid(attr.atttypid.to_u32()).unwrap_or(Type::TEXT);
        out.push(FieldInfo::new(
            name,
            None,
            None,
            pgwire_type,
            FieldFormat::Text,
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// execute — Bind+Execute path
// ---------------------------------------------------------------------------

/// Execute a prepared portal's plan with the given parameters and
/// return a pgwire `Response`. `max_rows` of `0` means "no limit"
/// (PG's `SPI_execute_plan` uses `tcount = 0` for unbounded).
///
/// Parameter format is taken from the portal's `parameter_format`;
/// per-parameter format can vary if the Bind sent
/// `parameter_format_codes` of length > 1, which pgwire stores as
/// `Format::Individual(Vec<i16>)`. Result format is taken from the
/// portal's `result_column_format` and honoured per-column — see
/// the module docs.
pub fn execute(
    prepared: &PreparedStatement,
    parameters: &[Option<Bytes>],
    parameter_format: &Format,
    result_format: &Format,
    max_rows: usize,
) -> PgWireResult<Response> {
    if parameters.len() != prepared.param_types.len() {
        return Err(generic_error(
            "pg_transport extended",
            &format!(
                "bind parameter count mismatch: got {}, expected {}",
                parameters.len(),
                prepared.param_types.len()
            ),
        ));
    }

    let plan_ptr = prepared.plan.as_ptr();
    let base_schema = prepared.result_schema.clone();
    let ncols = base_schema.len();
    let result_format_per_col: Vec<FieldFormat> =
        (0..ncols).map(|i| result_format.format_for(i)).collect();
    // Schema we hand back to pgwire — each FieldInfo carries the
    // requested per-column format so a follow-up Describe(Portal)
    // would report the same codes the client asked for. Cheap to
    // rebuild because base_schema is small (one entry per result
    // column).
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
    let column_oids: Vec<pg_sys::Oid> = base_schema
        .iter()
        .map(|fi| pg_sys::Oid::from(fi.datatype().oid()))
        .collect();
    let n_params = parameters.len();
    let param_oids: Vec<pg_sys::Oid> = prepared
        .param_types
        .iter()
        .map(|t| pg_sys::Oid::from(t.oid()))
        .collect();
    let param_bytes: Vec<Option<Bytes>> = parameters.to_vec();
    let param_is_binary: Vec<bool> = (0..n_params)
        .map(|i| parameter_format.is_binary(i))
        .collect();

    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome: Result<Result<Response, PgWireError>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| -> Result<Response, PgWireError> {
            unsafe {
                let rc = pg_sys::SPI_connect();
                if rc != pg_sys::SPI_OK_CONNECT as i32 {
                    return Err(spi_rc_error("SPI_connect", rc));
                }
            }

            let (mut values, nulls) =
                match decode_parameters(&param_bytes, &param_oids, &param_is_binary) {
                    Ok(pair) => pair,
                    Err(e) => {
                        unsafe { pg_sys::SPI_finish() };
                        return Err(e);
                    }
                };

            let exec_rc = unsafe {
                pg_sys::SPI_execute_plan(
                    plan_ptr,
                    if values.is_empty() {
                        std::ptr::null_mut()
                    } else {
                        values.as_mut_ptr()
                    },
                    if nulls.is_empty() {
                        std::ptr::null()
                    } else {
                        nulls.as_ptr()
                    },
                    false,
                    max_rows as std::ffi::c_long,
                )
            };

            if exec_rc < 0 {
                unsafe { pg_sys::SPI_finish() };
                return Err(spi_rc_error("SPI_execute_plan", exec_rc));
            }

            let processed = unsafe { pg_sys::SPI_processed } as usize;
            let tuptable = unsafe { pg_sys::SPI_tuptable };

            // Utility statements (CREATE/DROP/SET) and DML without
            // RETURNING leave SPI_tuptable null. Return the SPI
            // result-code-derived tag with the processed row count.
            if tuptable.is_null() || base_schema.is_empty() {
                let tag_name = command_tag_from_rc(exec_rc);
                let tag = Tag::new(&tag_name).with_rows(processed);
                let finish_rc = unsafe { pg_sys::SPI_finish() };
                if finish_rc != pg_sys::SPI_OK_FINISH as i32 {
                    return Err(spi_rc_error("SPI_finish", finish_rc));
                }
                return Ok(Response::Execution(tag));
            }

            // Resolve per-column output info once. For text columns
            // we cache typoutput; for binary columns we cache
            // typsend. Calling getTypeOutputInfo / getTypeBinaryOutputInfo
            // hits the syscache, so doing it inside the row loop
            // would still be cheap, but pulling it out makes the
            // hot per-row code shorter.
            let mut output_oids: Vec<pg_sys::Oid> = vec![pg_sys::Oid::INVALID; ncols];
            for c in 0..ncols {
                let oid = column_oids[c];
                let mut fn_oid = pg_sys::Oid::INVALID;
                let mut is_varlena = false;
                unsafe {
                    match result_format_per_col[c] {
                        FieldFormat::Text => {
                            pg_sys::getTypeOutputInfo(oid, &mut fn_oid, &mut is_varlena);
                        }
                        FieldFormat::Binary => {
                            pg_sys::getTypeBinaryOutputInfo(oid, &mut fn_oid, &mut is_varlena);
                        }
                    }
                }
                output_oids[c] = fn_oid;
            }

            // Materialise rows. SPI tuples die at SPI_finish; we
            // own the resulting Vec<DataRow> which is sent to the
            // client after we leave the SPI session.
            let tupdesc = unsafe { (*tuptable).tupdesc };
            let mut data_rows: Vec<DataRow> = Vec::with_capacity(processed);
            for row_idx in 0..processed {
                let heap_tuple = unsafe { *(*tuptable).vals.add(row_idx) };
                let mut buf = BytesMut::with_capacity(64);
                for c in 0..ncols {
                    let mut is_null = false;
                    let datum = unsafe {
                        pg_sys::SPI_getbinval(heap_tuple, tupdesc, (c + 1) as i32, &mut is_null)
                    };
                    if is_null {
                        buf.put_i32(-1);
                        continue;
                    }
                    match result_format_per_col[c] {
                        FieldFormat::Text => {
                            let cstr_ptr =
                                unsafe { pg_sys::OidOutputFunctionCall(output_oids[c], datum) };
                            // OidOutputFunctionCall always returns a
                            // palloc'd cstring — it's the type's
                            // text representation. Copy + free.
                            let bytes = unsafe { CStr::from_ptr(cstr_ptr) }.to_bytes();
                            buf.put_i32(bytes.len() as i32);
                            buf.put_slice(bytes);
                            unsafe { pg_sys::pfree(cstr_ptr as *mut _) };
                        }
                        FieldFormat::Binary => {
                            let bytea_ptr =
                                unsafe { pg_sys::OidSendFunctionCall(output_oids[c], datum) };
                            // SAFETY: OidSendFunctionCall always
                            // returns a non-null palloc'd bytea.
                            // varlena_to_byte_slice handles short/
                            // long/external headers.
                            let bytes = unsafe {
                                varlena::varlena_to_byte_slice(bytea_ptr as *const pg_sys::varlena)
                            };
                            buf.put_i32(bytes.len() as i32);
                            buf.put_slice(bytes);
                            unsafe { pg_sys::pfree(bytea_ptr as *mut _) };
                        }
                    }
                }
                data_rows.push(DataRow::new(buf, ncols as i16));
            }

            let finish_rc = unsafe { pg_sys::SPI_finish() };
            if finish_rc != pg_sys::SPI_OK_FINISH as i32 {
                return Err(spi_rc_error("SPI_finish", finish_rc));
            }

            let tag_name = command_tag_from_rc(exec_rc);
            let schema_arc = Arc::new(schema_vec);
            let row_stream = stream::iter(data_rows).map(Ok);
            let mut response = QueryResponse::new(schema_arc, row_stream);
            response.set_command_tag(&tag_name);
            Ok(Response::Query(response))
        }));

    finalize_xact(outcome)
}

// ---------------------------------------------------------------------------
// Parameter decoding
// ---------------------------------------------------------------------------

/// Decode pgwire's `Vec<Option<Bytes>>` into the SPI-shaped
/// `(values, nulls)` arrays. Null bytes in `nulls` are `b' '` for
/// non-null and `b'n'` for null (PG `SPI_execute_plan` convention).
///
/// SAFETY: caller is inside SPI_connect; this function calls
/// `OidInputFunctionCall` / `OidReceiveFunctionCall`, both of which
/// raise PG ERRORs (panics) on malformed input — those propagate
/// out of the outer `catch_unwind` and get converted to wire errors.
fn decode_parameters(
    parameters: &[Option<Bytes>],
    param_oids: &[pg_sys::Oid],
    is_binary: &[bool],
) -> PgWireResult<(Vec<pg_sys::Datum>, Vec<std::ffi::c_char>)> {
    let n = parameters.len();
    if n == 0 {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(n);
    let mut nulls: Vec<std::ffi::c_char> = Vec::with_capacity(n);

    for i in 0..n {
        let oid = param_oids[i];
        let binary = is_binary[i];
        match &parameters[i] {
            None => {
                values.push(pg_sys::Datum::from(0_usize));
                nulls.push(b'n' as std::ffi::c_char);
            }
            Some(bytes) => {
                let datum = if binary {
                    decode_binary_param(oid, bytes)?
                } else {
                    decode_text_param(oid, bytes)?
                };
                values.push(datum);
                nulls.push(b' ' as std::ffi::c_char);
            }
        }
    }
    Ok((values, nulls))
}

fn decode_text_param(oid: pg_sys::Oid, bytes: &Bytes) -> PgWireResult<pg_sys::Datum> {
    // Build a NUL-terminated C string from the wire bytes. The
    // wire spec doesn't guarantee NUL-termination for text-format
    // parameters; we copy + append.
    let cstring = CString::new(bytes.as_ref()).map_err(|_| {
        generic_error(
            "pg_transport extended",
            "text parameter contains a NUL byte",
        )
    })?;
    let mut typinput = pg_sys::Oid::INVALID;
    let mut typioparam = pg_sys::Oid::INVALID;
    // SAFETY: oid is from SPI's resolved param types or 0 (unknown);
    // getTypeInputInfo handles both. For oid == 0 (unknown) PG
    // returns the cstringin function which we treat as text.
    let resolved_oid = if oid == pg_sys::Oid::INVALID {
        // 0 OID — fall back to text. PG would normally infer at
        // analyze time; if we got here with 0, the prepare-time
        // inference must have left it unresolved.
        pg_sys::TEXTOID
    } else {
        oid
    };
    unsafe {
        pg_sys::getTypeInputInfo(resolved_oid, &mut typinput, &mut typioparam);
        // OidInputFunctionCall mutates the string buffer in some
        // typinput implementations (e.g. textin's de-escaping), so
        // we cast away const via the CString's owned buffer pointer.
        let datum = pg_sys::OidInputFunctionCall(
            typinput,
            cstring.as_ptr() as *mut std::ffi::c_char,
            typioparam,
            -1,
        );
        Ok(datum)
    }
}

fn decode_binary_param(oid: pg_sys::Oid, bytes: &Bytes) -> PgWireResult<pg_sys::Datum> {
    let mut typreceive = pg_sys::Oid::INVALID;
    let mut typioparam = pg_sys::Oid::INVALID;
    let resolved_oid = if oid == pg_sys::Oid::INVALID {
        pg_sys::TEXTOID
    } else {
        oid
    };
    // Build a StringInfoData over a palloc'd copy of the wire
    // buffer. typreceive consumes from this buffer's cursor; we
    // also append a trailing NUL because some receive functions
    // (e.g. inet_recv) expect to look one byte past the data.
    unsafe {
        pg_sys::getTypeBinaryInputInfo(resolved_oid, &mut typreceive, &mut typioparam);

        let buf_len = bytes.len();
        // palloc + memcpy to land in SPI's context; freed at SPI_finish.
        let buf = pg_sys::palloc(buf_len + 1) as *mut std::ffi::c_char;
        std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const std::ffi::c_char, buf, buf_len);
        *buf.add(buf_len) = 0;

        let mut sid: pg_sys::StringInfoData = std::mem::zeroed();
        pg_sys::initReadOnlyStringInfo(&mut sid, buf, buf_len as i32);

        let datum = pg_sys::OidReceiveFunctionCall(typreceive, &mut sid, typioparam, -1);
        Ok(datum)
    }
}

// ---------------------------------------------------------------------------
// Shared xact + error plumbing
// ---------------------------------------------------------------------------

/// Common Pop/Commit-on-Ok, Pop/Commit-on-clean-Err,
/// AbortCurrentTransaction-on-panic dance. Mirrors the post-body
/// match arm in [`super::spi_bridge::run_spi_statement`]; pulled
/// out into a helper because both [`prepare`] and [`execute`] need
/// the exact same shape.
fn finalize_xact<T>(
    outcome: Result<Result<T, PgWireError>, Box<dyn Any + Send>>,
) -> PgWireResult<T> {
    match outcome {
        Ok(Ok(value)) => {
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Ok(value)
        }
        Ok(Err(err)) => {
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Err(err)
        }
        Err(panic_payload) => {
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            Err(panic_to_pgwire(panic_payload))
        }
    }
}

/// Build a PgWireError from an SPI numeric return code.
fn spi_rc_error(api: &str, rc: i32) -> PgWireError {
    let name = unsafe { pg_sys::SPI_result_code_string(rc) };
    let name_str = if name.is_null() {
        format!("rc={rc}")
    } else {
        unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned()
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("pg_transport extended: {api} failed: {name_str}"),
    )))
}

/// Pick a CommandTag name from the SPI result code. We only handle
/// the common SELECT / INSERT / UPDATE / DELETE / utility-OK cases;
/// everything else degrades to "OK" which is the same string the
/// simple-query path uses for utility.
fn command_tag_from_rc(rc: i32) -> String {
    match rc as u32 {
        pg_sys::SPI_OK_SELECT => "SELECT".to_string(),
        pg_sys::SPI_OK_INSERT => "INSERT".to_string(),
        pg_sys::SPI_OK_UPDATE => "UPDATE".to_string(),
        pg_sys::SPI_OK_DELETE => "DELETE".to_string(),
        pg_sys::SPI_OK_INSERT_RETURNING => "INSERT".to_string(),
        pg_sys::SPI_OK_UPDATE_RETURNING => "UPDATE".to_string(),
        pg_sys::SPI_OK_DELETE_RETURNING => "DELETE".to_string(),
        pg_sys::SPI_OK_MERGE => "MERGE".to_string(),
        pg_sys::SPI_OK_UTILITY => "OK".to_string(),
        _ => "OK".to_string(),
    }
}

fn generic_error(prefix: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("{prefix}: {message}"),
    )))
}

fn panic_to_pgwire(payload: Box<dyn Any + Send>) -> PgWireError {
    if let Some(caught) = payload.downcast_ref::<CaughtError>() {
        return error_report_to_pgwire(extract_report(caught));
    }
    if let Some(report) = payload.downcast_ref::<ErrorReportWithLevel>() {
        return error_report_to_pgwire(report);
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return generic_error("pg_transport extended panic", s);
    }
    if let Some(s) = payload.downcast_ref::<&str>() {
        return generic_error("pg_transport extended panic", s);
    }
    generic_error(
        "pg_transport extended",
        "unknown panic payload in extended bridge",
    )
}

fn extract_report(caught: &CaughtError) -> &ErrorReportWithLevel {
    match caught {
        CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => report,
        CaughtError::RustPanic { ereport, .. } => ereport,
    }
}

fn error_report_to_pgwire(report: &ErrorReportWithLevel) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        format!("{:?}", report.level()),
        format!("{:?}", report.sql_error_code()),
        report.message().to_string(),
    )))
}
