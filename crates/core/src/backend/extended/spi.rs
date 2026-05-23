//! Extended-query SPI bridge — Parse / Bind / Execute backed by
//! `SPI_prepare` + `SPI_execute_plan`, with all SPI / xact / type
//! I/O plumbing routed through [`super::super::spi`].
//!
//! Layered alongside [`super::super::spi_bridge`] (simple-query);
//! both share the [`spi::with_spi`] xact-wrapping discipline. Per
//! [backend-wire.md §6](../../../../../docs/design/backend-wire.md) +
//! [§8 Q1](../../../../../docs/design/backend-wire.md#8-open-questions),
//! the wire layer owns the prepared-statement and portal *names*
//! (via pgwire's `PortalStore`); SPI owns the *plans*. We hand the
//! wire layer back a [`super::PreparedStatement`] whose `plan`
//! field is a `Box<SpiBackendPlan>` that holds an
//! [`spi::SpiPlan`] freeing itself on Drop, so per-handoff reset
//! is "drop the pgwire portal store" — which the current handoff
//! structure already does, because we build a fresh
//! [`crate::wire::pgwire_v3::PgTransportHandlers`] (and therefore
//! a fresh pgwire `DefaultClient` with its own per-connection
//! store) per `process_socket` call.
//!
//! ## Parameter formats
//!
//! v0 phase 9 supports both **text** and **binary** wire-format
//! parameters by routing through PG's per-type `typinput` /
//! `typreceive` functions, wrapped behind [`spi::TypeInput`] /
//! [`spi::TypeReceive`]. Text format is mandatory because some
//! clients (psql) never opt into binary; binary is required
//! because tokio-postgres binds parameters in binary by default
//! for any type it knows how to serialise.
//!
//! ## Result formats
//!
//! Result columns honour the per-column format requested in the
//! `Bind` message (`result_column_format_codes`). For each column:
//!
//! * **Text format** — [`spi::TypeOutput`] (calls the type's
//!   `typoutput` function, same bytes the simple-query path uses).
//! * **Binary format** — [`spi::TypeSend`] (calls the type's
//!   `typsend` function and pulls bytes out of the returned
//!   `bytea`).
//!
//! tokio-postgres' typed accessors default to binary format for
//! every type it knows how to deserialise (the common numeric +
//! string types), so without the binary path `client.query(...)`
//! + `row.get::<_, i32>(0)` would fail with "error deserializing
//!   column 0" (4 bytes expected, 1 text byte received).

use std::ffi::CString;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream;
use pgrx::pg_sys;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use super::super::dest_receiver::ColumnEncoder;
use super::super::spi::{
    self, SpiCtx, SpiPlan, SpiTuples, TypeInput, TypeReceive, generic_error, spi_rc_error, with_spi,
};
use super::PreparedPlan;

// ---------------------------------------------------------------------------
// SpiBackendPlan — the [`super::PreparedPlan`] impl for the SPI path
// ---------------------------------------------------------------------------

/// SPI-side execution state for a single prepared statement.
/// Holds the kept SPI plan plus pre-computed per-column metadata
/// so per-execute hot-path work is minimal.
///
/// Lives behind `Box<dyn PreparedPlan>` inside [`super::PreparedStatement`].
pub(crate) struct SpiBackendPlan {
    /// Kept SPI plan. Freed on Drop via [`SpiPlan::Drop`].
    plan: SpiPlan,
    /// Per-`$n` parameter OIDs (echoes [`super::PreparedStatement::param_types`]).
    param_oids: Vec<pg_sys::Oid>,
    /// Per-result-column type OIDs (echoes [`super::PreparedStatement::result_schema`]).
    column_oids: Vec<pg_sys::Oid>,
    /// Result-column schema with every column's [`FieldFormat`]
    /// set to `Text`. `Arc`-wrapped so the common case
    /// ([`Format::UnifiedText`]) on Execute is a refcount bump
    /// instead of a fresh `Vec<FieldInfo>` + per-column `String`
    /// allocation. Non-text (Individual / UnifiedBinary) Bind
    /// requests rebuild a per-Execute `Vec<FieldInfo>` from this
    /// one, paying the same cost as before.
    base_schema: Arc<Vec<FieldInfo>>,
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
/// SAFETY: must be called inside an SPI session (which sets up the
/// snapshot + queryEnv the analyzer needs). Raises PG ERROR on
/// syntax errors / analysis failures, which our [`with_spi`]
/// wrapper catches.
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
/// type hints; return a [`super::PreparedStatement`] whose `plan`
/// is an [`SpiBackendPlan`] boxed behind
/// [`super::PreparedPlan`].
///
/// `param_hints` is one entry per `$n` parameter the client
/// declared in the Parse message; `Some(oid)` is the OID the
/// client asserted, `None` means "infer from context".
///
/// Inference path: when `param_hints` is empty *or* all `None`,
/// we run a varparams pre-pass (see [`infer_param_types`]) and
/// pass the resolved OIDs to `SPI_prepare` as fixed types. The
/// plan is then promoted via [`SpiCtx::keep_plan`] so it survives
/// `SPI_finish`.
pub fn prepare(sql: &str, param_hints: &[Option<u32>]) -> PgWireResult<super::PreparedStatement> {
    let sql_cstring = CString::new(sql)
        .map_err(|_| generic_error("pg_transport extended", "query string contains a NUL byte"))?;
    let sql_owned = sql.to_string();
    let any_hint_present = param_hints.iter().any(|o| o.is_some());

    with_spi(|ctx: &SpiCtx| -> PgWireResult<super::PreparedStatement> {
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

        // SAFETY: inside SPI; SPI_prepare is the standard plan
        // builder. Null return → check SPI_result for the error.
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
            // SAFETY: SPI_result is a PG global set by the most
            // recent SPI call; safe to read inside the session.
            let rc = unsafe { pg_sys::SPI_result };
            return Err(spi_rc_error("SPI_prepare", rc));
        }

        let plan = ctx.keep_plan(plan_ptr)?;

        let arg_count = plan.arg_count();
        let resolved_types: Vec<Type> = (0..arg_count)
            .map(|i| Type::from_oid(plan.arg_type(i).to_u32()).unwrap_or(Type::UNKNOWN))
            .collect();
        let param_oids: Vec<pg_sys::Oid> = (0..arg_count).map(|i| plan.arg_type(i)).collect();

        let result_schema: Vec<FieldInfo> = spi::plan_result_columns(plan.as_ptr())
            .unwrap_or_default()
            .into_iter()
            .map(|col| {
                FieldInfo::new(
                    col.name,
                    None,
                    None,
                    Type::from_oid(col.type_oid.to_u32()).unwrap_or(Type::TEXT),
                    FieldFormat::Text,
                )
            })
            .collect();
        let column_oids: Vec<pg_sys::Oid> = result_schema
            .iter()
            .map(|fi| pg_sys::Oid::from(fi.datatype().oid()))
            .collect();

        let backend_plan = SpiBackendPlan {
            plan,
            param_oids,
            column_oids,
            base_schema: Arc::new(result_schema.clone()),
        };

        Ok(super::PreparedStatement {
            sql: sql_owned,
            param_types: resolved_types,
            result_schema,
            plan: Box::new(backend_plan),
        })
    })
}

// ---------------------------------------------------------------------------
// execute — Bind+Execute path (PreparedPlan trait impl)
// ---------------------------------------------------------------------------

impl PreparedPlan for SpiBackendPlan {
    /// Execute the SPI plan with bound parameters. `max_rows` of
    /// `0` means "no limit" (PG's `SPI_execute_plan` uses
    /// `tcount = 0` for unbounded).
    ///
    /// Parameter format is taken from the portal's
    /// `parameter_format`; per-parameter format can vary if the
    /// Bind sent `parameter_format_codes` of length > 1, which
    /// pgwire stores as `Format::Individual(Vec<i16>)`. Result
    /// format is taken from the portal's `result_column_format`
    /// and honoured per-column — see the module docs.
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

/// Free-function body for [`SpiBackendPlan::execute`]; pulled out
/// of the trait method so the with_spi closure has the inner
/// borrows we want without fighting `&self` lifetimes.
fn execute_impl(
    backend: &SpiBackendPlan,
    parameters: &[Option<Bytes>],
    parameter_format: &Format,
    result_format: &Format,
    max_rows: usize,
) -> PgWireResult<Response> {
    if parameters.len() != backend.param_oids.len() {
        return Err(generic_error(
            "pg_transport extended",
            &format!(
                "bind parameter count mismatch: got {}, expected {}",
                parameters.len(),
                backend.param_oids.len()
            ),
        ));
    }

    let plan_ptr = backend.plan.as_ptr();
    let base_schema = &backend.base_schema;
    let ncols = base_schema.len();
    let result_format_per_col: Vec<FieldFormat> =
        (0..ncols).map(|i| result_format.format_for(i)).collect();
    // Fast path: when the client asked for all-text results (the
    // pgwire `Format::UnifiedText` default and the common case for
    // pgbench / tokio-postgres), we ship the cached text-format
    // `Arc<Vec<FieldInfo>>` straight through. Per Execute the cost
    // collapses to one `Arc::clone` (atomic increment) instead of
    // `ncols` `String` allocations + a fresh `Vec<FieldInfo>` +
    // `Arc::new`.
    let schema_arc: Arc<Vec<FieldInfo>> = if matches!(result_format, Format::UnifiedText) {
        Arc::clone(base_schema)
    } else {
        // Slow path: rebuild per-column with the requested
        // format codes. Same cost as before.
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
    let column_oids = &backend.column_oids;
    let param_oids = &backend.param_oids;
    let param_is_binary: Vec<bool> = (0..parameters.len())
        .map(|i| parameter_format.is_binary(i))
        .collect();

    with_spi(|ctx: &SpiCtx| -> PgWireResult<Response> {
        // Decode params into the SPI-shaped (values, nulls) pair.
        // Per-type input/receive helpers raise PG ERROR on
        // malformed bytes; with_spi catches that.
        let (mut values, nulls) = decode_parameters(parameters, param_oids, &param_is_binary)?;

        // SAFETY: inside SPI; SPI_execute_plan is the documented
        // executor for SPI_prepare'd plans. Negative rc → error.
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
            return Err(spi_rc_error("SPI_execute_plan", exec_rc));
        }

        // Utility statements (CREATE/DROP/SET) and DML without
        // RETURNING leave SPI_tuptable null. Return the SPI
        // result-code-derived tag with the processed row count.
        let tuples = match SpiTuples::current(ctx) {
            Some(t) if !base_schema.is_empty() => t,
            _ => {
                let processed = SpiTuples::processed_rows_without_table(ctx);
                let tag = Tag::new(command_tag_from_rc(exec_rc)).with_rows(processed);
                return Ok(Response::Execution(tag));
            }
        };

        // Cache the per-column output function once (text or
        // binary, per result_column_format).
        let encoders: Vec<ColumnEncoder> = (0..ncols)
            .map(|c| ColumnEncoder::for_column(column_oids[c], result_format_per_col[c]))
            .collect();

        // Materialise rows. SPI tuples die at SPI_finish; we own
        // the resulting Vec<DataRow> which is sent to the client
        // after we leave the SPI session. `encode_into` writes
        // length-prefixed cells straight into `buf`, avoiding the
        // per-cell `Vec<u8>` allocation the older `encode` shape
        // forced.
        let mut data_rows: Vec<DataRow> = Vec::with_capacity(tuples.len());
        for row_idx in 0..tuples.len() {
            let mut buf = BytesMut::with_capacity(64);
            for (c, encoder) in encoders.iter().enumerate() {
                match tuples.cell(row_idx, c) {
                    None => buf.put_i32(-1),
                    Some(datum) => encoder.encode_into(datum, &mut buf),
                }
            }
            data_rows.push(DataRow::new(buf, ncols as i16));
        }

        let tag_name = command_tag_from_rc(exec_rc);
        let row_stream = stream::iter(data_rows).map(Ok);
        let mut response = QueryResponse::new(schema_arc, row_stream);
        response.set_command_tag(tag_name);
        Ok(Response::Query(response))
    })
}

// ---------------------------------------------------------------------------
// Parameter / result encoding helpers
// ---------------------------------------------------------------------------

/// Decode pgwire's `Vec<Option<Bytes>>` into the SPI-shaped
/// `(values, nulls)` arrays. Null bytes in `nulls` are `b' '` for
/// non-null and `b'n'` for null (PG `SPI_execute_plan` convention).
fn decode_parameters(
    parameters: &[Option<Bytes>],
    param_oids: &[pg_sys::Oid],
    is_binary: &[bool],
) -> PgWireResult<(Vec<pg_sys::Datum>, Vec<std::ffi::c_char>)> {
    let n = parameters.len();
    let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(n);
    let mut nulls: Vec<std::ffi::c_char> = Vec::with_capacity(n);

    for i in 0..n {
        match &parameters[i] {
            None => {
                values.push(pg_sys::Datum::from(0_usize));
                nulls.push(b'n' as std::ffi::c_char);
            }
            Some(bytes) => {
                let datum = if is_binary[i] {
                    TypeReceive::for_type(param_oids[i]).call(bytes)
                } else {
                    TypeInput::for_type(param_oids[i]).call(bytes.as_ref())?
                };
                values.push(datum);
                nulls.push(b' ' as std::ffi::c_char);
            }
        }
    }
    Ok((values, nulls))
}

/// Pick a CommandTag name from the SPI result code. We only handle
/// the common SELECT / INSERT / UPDATE / DELETE / utility-OK cases;
/// everything else degrades to `"OK"` which is the same string the
/// simple-query path uses for utility.
///
/// Returns a `&'static str` (string literals) so per-Execute pays
/// no allocation.
fn command_tag_from_rc(rc: i32) -> &'static str {
    match rc as u32 {
        pg_sys::SPI_OK_SELECT => "SELECT",
        pg_sys::SPI_OK_INSERT => "INSERT",
        pg_sys::SPI_OK_UPDATE => "UPDATE",
        pg_sys::SPI_OK_DELETE => "DELETE",
        pg_sys::SPI_OK_INSERT_RETURNING => "INSERT",
        pg_sys::SPI_OK_UPDATE_RETURNING => "UPDATE",
        pg_sys::SPI_OK_DELETE_RETURNING => "DELETE",
        pg_sys::SPI_OK_MERGE => "MERGE",
        pg_sys::SPI_OK_UTILITY => "OK",
        _ => "OK",
    }
}
