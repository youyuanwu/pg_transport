//! Safe wrappers over PG SPI, the surrounding xact / snapshot
//! lifecycle, per-type I/O functions, and the post-execute tuple
//! table.
//!
//! ## Why this module exists
//!
//! SPI's C API is fundamentally global-state-driven:
//! `SPI_connect` / `SPI_finish` bracket a session, the
//! `SPI_tuptable` / `SPI_processed` globals receive results, plan
//! lifetime is tied to the bgworker process, and *every* call sits
//! inside an open transaction that owns the snapshot the planner
//! reads. The simple-query bridge ([`super::spi_bridge`]) and the
//! extended-query bridge ([`super::extended`]) both pay that
//! per-call sandwich tax — without this module they'd each repeat
//! ~30 lines of `unsafe { pg_sys::* }` plumbing per public entry
//! point.
//!
//! What we wrap here:
//!
//! * [`with_spi`] takes a closure and runs it inside the
//!   `StartTransactionCommand` + `PushActiveSnapshot` +
//!   `SPI_connect` ... `SPI_finish` + `PopActiveSnapshot` +
//!   `CommitTransactionCommand` sandwich, wrapping the body in
//!   `catch_unwind` so a PG ERROR (which pgrx re-raises as a
//!   Rust panic) becomes a typed `PgWireError`, and on the panic
//!   path runs `AbortCurrentTransaction` for correct xact state.
//!   The closure receives a `&`[`SpiCtx`] token — an opaque
//!   compile-time proof that the holder is inside an SPI session.
//!
//! * [`SpiPlan`] wraps a `SPIPlanPtr` promoted via `SPI_keepplan`;
//!   Drop calls `SPI_freeplan`. Plans are `Send + Sync` (slot
//!   bgworker is single-threaded; the wire layer hands them to
//!   pgwire's `PortalStore` which has those bounds).
//!
//! * [`SpiTuples`] is a safe accessor for the per-statement
//!   `SPI_tuptable` / `SPI_processed` globals. Borrowed from the
//!   `SpiCtx`, so it can't outlive the session.
//!
//! * [`TypeInput`] / [`TypeReceive`] / [`TypeOutput`] / [`TypeSend`]
//!   each bundle the per-type I/O function OID with a typed `call`
//!   method that does the right unsafe FFI internally (palloc,
//!   StringInfoData, pfree on cstring/bytea, varlena
//!   accessor-macro choice).
//!
//! ## Error translation
//!
//! Both bridges convert PG ERRORs / SPI return codes to pgwire
//! `ErrorResponse` frames in identical ways. [`spi_rc_error`],
//! [`generic_error`], and [`panic_to_pgwire`] live here so the
//! two bridges share one implementation.

use std::any::Any;
use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};

use bytes::Bytes;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::{CaughtError, ErrorReportWithLevel};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

// ---------------------------------------------------------------------------
// SpiCtx — the in-SPI-session token
// ---------------------------------------------------------------------------

/// Compile-time proof that the holder is running inside an
/// `SPI_connect` ... `SPI_finish` session opened by [`with_spi`].
///
/// You don't construct this; you receive a `&SpiCtx` from
/// `with_spi`'s closure argument. Helpers that require an open
/// SPI session (e.g. [`SpiTuples::current`], [`SpiCtx::keep_plan`])
/// take `&SpiCtx` to enforce the invariant at compile time.
///
/// The PhantomData makes this `!Send + !Sync` so it can't be
/// stashed somewhere and reused outside the closure — though the
/// real protection is that nobody can construct one outside this
/// module.
pub struct SpiCtx {
    _marker: PhantomData<*const ()>,
    // `Cell` so we can flip "we already called SPI_finish" from
    // inside the wrapper after the closure returns. Closures don't
    // need this — they never touch it.
    finished: Cell<bool>,
}

impl SpiCtx {
    fn new() -> Self {
        Self {
            _marker: PhantomData,
            finished: Cell::new(false),
        }
    }

    /// Promote a `SPIPlanPtr` to a long-lived plan via
    /// `SPI_keepplan`; the returned [`SpiPlan`] owns the plan and
    /// frees it on Drop. Must be called *before* the surrounding
    /// `with_spi` closure returns — `SPI_keepplan` only works
    /// inside an SPI session.
    pub fn keep_plan(&self, raw: pg_sys::SPIPlanPtr) -> PgWireResult<SpiPlan> {
        // SAFETY: ptr came from an SPI_prepare* call inside our
        // session (caller obligation captured by `&SpiCtx`);
        // SPI_keepplan is a server-side stable API.
        let rc = unsafe { pg_sys::SPI_keepplan(raw) };
        if rc != 0 {
            return Err(spi_rc_error("SPI_keepplan", rc));
        }
        Ok(SpiPlan { ptr: raw })
    }
}

// ---------------------------------------------------------------------------
// with_spi — the xact + SPI sandwich
// ---------------------------------------------------------------------------

/// Run `body` inside `StartTransactionCommand` + `PushActiveSnapshot`
/// + `SPI_connect` ... `SPI_finish` + `PopActiveSnapshot` +
///   `CommitTransactionCommand`.
///
/// On clean Ok / clean Err return from `body`: closes SPI, pops
/// the snapshot, commits.
/// On panic (PG ERROR longjmp'd through pgrx into a Rust panic):
/// calls `AbortCurrentTransaction` (which itself cleans up SPI
/// state and the snapshot) and converts the panic payload to a
/// `PgWireError` via [`panic_to_pgwire`].
///
/// `body` receives a `&`[`SpiCtx`] it can use to call SPI-only
/// helpers ([`SpiCtx::keep_plan`], [`SpiTuples::current`]).
///
/// Mirrors the xact discipline of
/// [`super::spi_bridge::run_spi_statement`] but is reusable: both
/// the simple-query bridge and the extended-query bridge call
/// through this.
pub fn with_spi<T, F>(body: F) -> PgWireResult<T>
where
    F: FnOnce(&SpiCtx) -> PgWireResult<T>,
{
    // SAFETY: SetCurrentStatementStartTimestamp / StartTransactionCommand
    // / PushActiveSnapshot are PG server-API entry points safe to
    // call from a bgworker in any TBLOCK state — Start from DEFAULT
    // moves to STARTED; from INPROGRESS it's a no-op. See the long
    // comment in spi_bridge::run_spi_statement for why we don't use
    // pgrx's BackgroundWorker::transaction here.
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome: Result<PgWireResult<T>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| -> PgWireResult<T> {
            // SAFETY: in xact; SPI_connect is safe to call once
            // per session. RC check below catches the "double
            // connect" / OOM error paths.
            unsafe {
                let rc = pg_sys::SPI_connect();
                if rc != pg_sys::SPI_OK_CONNECT as i32 {
                    return Err(spi_rc_error("SPI_connect", rc));
                }
            }
            let ctx = SpiCtx::new();
            let body_result = body(&ctx);

            // Only call SPI_finish if the user didn't already.
            // (We don't currently expose a way for them to, but
            // future SpiCtx::finish_early support would flip this
            // bool.) Body errors don't skip SPI_finish — the
            // session was opened, must be closed.
            if !ctx.finished.get() {
                // SAFETY: we own this session; SPI_finish is
                // matched with the SPI_connect above. The RC
                // check converts a finish-time error to PgWireError.
                let rc = unsafe { pg_sys::SPI_finish() };
                if rc != pg_sys::SPI_OK_FINISH as i32 {
                    // If body already errored, prefer that error.
                    return body_result.and(Err(spi_rc_error("SPI_finish", rc)));
                }
            }
            body_result
        }));

    match outcome {
        Ok(Ok(value)) => {
            // SAFETY: matched Start+Push above; closure returned
            // cleanly (SPI_finish already ran inside the body
            // sandwich).
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Ok(value)
        }
        Ok(Err(err)) => {
            // Body returned a typed PgWireError without raising
            // a PG ERROR. SPI session was closed cleanly above;
            // xact state is intact, so Pop+Commit (not Abort).
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Err(err)
        }
        Err(panic_payload) => {
            // A PG ERROR (or Rust panic) longjmp'd / unwound
            // through the body. AbortCurrentTransaction handles
            // SPI cleanup and snapshot stack itself; we must NOT
            // call Pop here.
            //
            // SAFETY: AbortCurrentTransaction is safe from any
            // non-DEFAULT TBLOCK_* state.
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            Err(panic_to_pgwire(panic_payload))
        }
    }
}

// ---------------------------------------------------------------------------
// SpiPlan — RAII kept-plan handle
// ---------------------------------------------------------------------------

/// Owned, kept SPI plan pointer. `Drop` calls `SPI_freeplan`,
/// which is documented to work outside an SPI session for plans
/// promoted via `SPI_keepplan` (see PG `src/backend/executor/spi.c`).
///
/// pgwire's `PortalStore` requires `Send + Sync` on the statement
/// type it holds; the unsafe impls below are justified by the slot
/// bgworker being single-threaded — the plan is only ever touched
/// from the slot's own thread, where the tokio current-thread
/// runtime runs our wire-layer handlers.
pub struct SpiPlan {
    ptr: pg_sys::SPIPlanPtr,
}

// SAFETY: see SpiPlan doc comment — slot is single-threaded.
unsafe impl Send for SpiPlan {}
unsafe impl Sync for SpiPlan {}

impl SpiPlan {
    /// Raw pointer accessor for callers that need to pass the
    /// plan into `SPI_*` C APIs. Caller obligation: hold the
    /// slot's single thread of execution.
    pub fn as_ptr(&self) -> pg_sys::SPIPlanPtr {
        self.ptr
    }

    /// Number of `$N` parameters baked into the plan (PG-side
    /// `plan->nargs`, populated by `SPI_prepare`'s fixed-types
    /// path). Returns 0 for plans built via `SPI_prepare_params`.
    pub fn arg_count(&self) -> usize {
        // SAFETY: SPI_getargcount is a const-style query on the
        // plan struct; safe to call outside an SPI session.
        unsafe { pg_sys::SPI_getargcount(self.ptr) as usize }
    }

    /// OID of the `i`-th argument type (zero-based). Out-of-range
    /// indices return `Oid::INVALID`.
    pub fn arg_type(&self, i: usize) -> pg_sys::Oid {
        // SAFETY: see arg_count.
        unsafe { pg_sys::SPI_getargtypeid(self.ptr, i as i32) }
    }
}

impl Drop for SpiPlan {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // SAFETY: ptr came from SPI_prepare* + SPI_keepplan; we
        // are the only owner. SPI_freeplan works outside an SPI
        // session for kept plans. Failure (non-zero rc) leaves
        // the plan leaked — we log and continue rather than panic
        // in Drop.
        unsafe {
            let rc = pg_sys::SPI_freeplan(self.ptr);
            if rc != 0 {
                pgrx::log!("pg_transport spi: SPI_freeplan returned {rc}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SpiTuples — safe accessor for SPI_tuptable / SPI_processed
// ---------------------------------------------------------------------------

/// Borrowed view of `SPI_tuptable` + `SPI_processed` after a
/// successful `SPI_execute*` call. Only constructable through
/// [`SpiTuples::current`], which takes `&SpiCtx`, so the lifetime
/// guarantees that the SPI session is still open (and the tuple
/// table is still alive).
///
/// Returns `None` for utility statements / DML-without-RETURNING:
/// those leave `SPI_tuptable` null and only populate
/// `SPI_processed`. Use [`Self::processed_rows_without_table`] in
/// that branch.
pub struct SpiTuples<'ctx> {
    table: *mut pg_sys::SPITupleTable,
    rows: usize,
    _marker: PhantomData<&'ctx SpiCtx>,
}

impl<'ctx> SpiTuples<'ctx> {
    /// Read the current SPI globals and wrap them. Returns `None`
    /// when `SPI_tuptable` is null (no result set was materialised
    /// — typical for `CREATE TABLE`, `INSERT` without RETURNING,
    /// `SET`, etc.).
    pub fn current(_ctx: &'ctx SpiCtx) -> Option<Self> {
        // SAFETY: SPI_tuptable + SPI_processed are PG global
        // variables populated by the most recent SPI call; valid
        // for the duration of the SPI session (guaranteed by the
        // `&SpiCtx` borrow).
        let table = unsafe { pg_sys::SPI_tuptable };
        if table.is_null() {
            return None;
        }
        let rows = unsafe { pg_sys::SPI_processed } as usize;
        Some(Self {
            table,
            rows,
            _marker: PhantomData,
        })
    }

    /// Read just the processed-row count when the tuple table
    /// itself is null (utility / DML-no-RETURNING). Always safe
    /// inside an SPI session.
    pub fn processed_rows_without_table(_ctx: &'ctx SpiCtx) -> usize {
        // SAFETY: see current() — same global, always valid
        // inside an SPI session.
        unsafe { pg_sys::SPI_processed as usize }
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Column count of the result tuple descriptor.
    pub fn ncols(&self) -> usize {
        // SAFETY: tuptable is non-null (checked in `current`);
        // its tupdesc is valid for the SPI session lifetime.
        unsafe { (*self.tupdesc()).natts as usize }
    }

    /// Read cell `(row, col)` as a `Datum`. Returns `None` for
    /// SQL NULL.
    pub fn cell(&self, row: usize, col: usize) -> Option<pg_sys::Datum> {
        let nrows = self.rows;
        let ncols = self.ncols();
        assert!(
            row < nrows && col < ncols,
            "cell ({row},{col}) out of range ({nrows},{ncols})"
        );
        let mut is_null = false;
        // SAFETY: bounds checked; vals[row] is a valid HeapTuple
        // in the tuptable; SPI_getbinval is the canonical accessor.
        let datum = unsafe {
            let tuple = *(*self.table).vals.add(row);
            pg_sys::SPI_getbinval(tuple, self.tupdesc(), (col + 1) as i32, &mut is_null)
        };
        if is_null { None } else { Some(datum) }
    }

    fn tupdesc(&self) -> pg_sys::TupleDesc {
        // SAFETY: tuptable non-null by construction.
        unsafe { (*self.table).tupdesc }
    }
}

// ---------------------------------------------------------------------------
// CachedPlanSource accessors
// ---------------------------------------------------------------------------

/// One column of a plan's result-row schema, derived from the
/// plan's first `CachedPlanSource`'s `resultDesc`.
pub struct PlanColumn {
    pub name: String,
    pub type_oid: pg_sys::Oid,
}

/// Walk a `SPIPlanPtr`'s first `CachedPlanSource` and pull out the
/// post-analysis result-row schema (column name + type OID per
/// column). Returns `None` when the plan has no result columns —
/// utility statements (`CREATE TABLE`, `SET`), DML without
/// `RETURNING`, or queries where `resultDesc` is deferred to
/// plan-cache time.
///
/// SAFETY: caller must hold a valid kept-plan pointer (i.e. came
/// from [`SpiCtx::keep_plan`] or `SPI_prepare*` + kept).
pub fn plan_result_columns(plan: pg_sys::SPIPlanPtr) -> Option<Vec<PlanColumn>> {
    use super::executor::TupleDescRef;
    // SAFETY: SPI_plan_get_plan_sources is the documented accessor;
    // returns a List* of CachedPlanSource* owned by the plan.
    let list = unsafe { pg_sys::SPI_plan_get_plan_sources(plan) };
    if list.is_null() {
        return None;
    }
    let length = unsafe { (*list).length };
    if length == 0 {
        return None;
    }
    let elements = unsafe { (*list).elements };
    let first = unsafe { (*elements).ptr_value } as *mut pg_sys::CachedPlanSource;
    if first.is_null() {
        return None;
    }
    let tupdesc = unsafe { TupleDescRef::from_raw((*first).resultDesc) }?;
    let out = tupdesc
        .iter()
        .map(|attr| {
            // SAFETY: attname is a PG NameData buffer containing
            // an ASCII identifier.
            let name = unsafe { super::executor::pg_ident_to_string(attr.attname.data.as_ptr()) };
            PlanColumn {
                name,
                type_oid: attr.atttypid,
            }
        })
        .collect();
    Some(out)
}

// ---------------------------------------------------------------------------
// Type I/O wrappers
// ---------------------------------------------------------------------------

/// Convert a PG OID to a safer "TEXTOID for INVALID" fallback used
/// by the parameter decoders when SPI hasn't resolved a type.
fn resolve_oid_or_text(oid: pg_sys::Oid) -> pg_sys::Oid {
    if oid == pg_sys::Oid::INVALID || oid.to_u32() == pg_sys::UNKNOWNOID.to_u32() {
        pg_sys::TEXTOID
    } else {
        oid
    }
}

/// Bundled `(typinput, typioparam)` lookup for one type. Calling
/// [`Self::call`] runs the type's text-format input function on a
/// NUL-terminated C string and returns a `Datum`.
pub struct TypeInput {
    typinput: pg_sys::Oid,
    typioparam: pg_sys::Oid,
}

impl TypeInput {
    /// Look up the per-type I/O info. Substitutes `TEXTOID` for
    /// `InvalidOid` / `UnknownOid` — see [`resolve_oid_or_text`].
    pub fn for_type(oid: pg_sys::Oid) -> Self {
        let oid = resolve_oid_or_text(oid);
        let mut typinput = pg_sys::Oid::INVALID;
        let mut typioparam = pg_sys::Oid::INVALID;
        // SAFETY: getTypeInputInfo is a server-API syscache lookup.
        unsafe {
            pg_sys::getTypeInputInfo(oid, &mut typinput, &mut typioparam);
        }
        Self {
            typinput,
            typioparam,
        }
    }

    /// Build a `Datum` by passing `bytes` through the type's
    /// `typinput` function. Raises PG ERROR on malformed input
    /// (which our `with_spi` wrapper catches).
    ///
    /// The bytes must not contain a NUL — we error early with a
    /// typed `PgWireError` rather than letting `CString::new`
    /// panic.
    pub fn call(&self, bytes: &[u8]) -> PgWireResult<pg_sys::Datum> {
        let cstring = CString::new(bytes).map_err(|_| {
            generic_error(
                "pg_transport spi",
                "text parameter contains an interior NUL byte",
            )
        })?;
        // SAFETY: typinput / typioparam came from getTypeInputInfo
        // for a known OID; the cstring's pointer is valid for the
        // duration of this call (typinput functions copy out what
        // they need before returning).
        let datum = unsafe {
            pg_sys::OidInputFunctionCall(
                self.typinput,
                cstring.as_ptr() as *mut std::ffi::c_char,
                self.typioparam,
                -1,
            )
        };
        Ok(datum)
    }
}

/// Bundled `(typreceive, typioparam)` lookup. [`Self::call`] runs
/// the type's binary-format `typreceive` function on a byte slice
/// and returns a `Datum`.
pub struct TypeReceive {
    typreceive: pg_sys::Oid,
    typioparam: pg_sys::Oid,
}

impl TypeReceive {
    pub fn for_type(oid: pg_sys::Oid) -> Self {
        let oid = resolve_oid_or_text(oid);
        let mut typreceive = pg_sys::Oid::INVALID;
        let mut typioparam = pg_sys::Oid::INVALID;
        // SAFETY: as TypeInput::for_type.
        unsafe {
            pg_sys::getTypeBinaryInputInfo(oid, &mut typreceive, &mut typioparam);
        }
        Self {
            typreceive,
            typioparam,
        }
    }

    /// Decode `bytes` through the type's `typreceive` function.
    pub fn call(&self, bytes: &Bytes) -> pg_sys::Datum {
        // typreceive expects a StringInfoData; build a read-only
        // one over a palloc'd copy of the wire bytes. The trailing
        // NUL is for the handful of receive functions (e.g.
        // inet_recv) that look one byte past the end.
        //
        // SAFETY: palloc lands in CurrentMemoryContext; freed
        // automatically at SPI_finish. initReadOnlyStringInfo is
        // the documented constructor for wire-buffer-backed
        // StringInfoData. OidReceiveFunctionCall raises PG ERROR
        // on malformed bytes — caught by our with_spi wrapper.
        unsafe {
            let buf_len = bytes.len();
            let buf = pg_sys::palloc(buf_len + 1) as *mut std::ffi::c_char;
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const std::ffi::c_char, buf, buf_len);
            *buf.add(buf_len) = 0;

            let mut sid: pg_sys::StringInfoData = std::mem::zeroed();
            pg_sys::initReadOnlyStringInfo(&mut sid, buf, buf_len as i32);

            pg_sys::OidReceiveFunctionCall(self.typreceive, &mut sid, self.typioparam, -1)
        }
    }
}

/// Cached `typoutput` `FmgrInfo` for a result-column type. Built
/// once at portal setup; consumed by
/// [`super::dest_receiver::ColumnEncoder::encode_into`] in the
/// per-row hot loop.
///
/// Holding the resolved [`pg_sys::FmgrInfo`] (rather than just
/// the `typoutput` Oid) lets the hot loop dispatch through
/// `OutputFunctionCall(&finfo, datum)` — a direct call through
/// the cached `fn_addr` function pointer. The `Oid`-only variant
/// went through `OidOutputFunctionCall`, which does
/// `fmgr_info(typoutput)` — a `SearchSysCache1(PROCOID, ...)`
/// lookup — on *every cell*. Matches vanilla
/// [`printtup_prepare_info`](../../../../../postgres/src/backend/access/common/printtup.c#L251)
/// and the [`printtup` hot loop](../../../../../postgres/src/backend/access/common/printtup.c#L361);
/// closes the per-cell syscache cost called out in
/// [review §3.1.1](../../../../docs/design/reviews/2026-05-24-pg-code-findings.md).
///
/// **Lifetime / context invariant.** `fmgr_info` writes
/// `fn_mcxt = CurrentMemoryContext` into the FmgrInfo at
/// construction; that pointer must remain valid for the
/// lifetime of the encoder. Callers therefore build encoders
/// inside [`with_spi`] / `with_xact`, where the active
/// MemoryContext (TopTransactionContext) outlives the encoder
/// Vec.
pub struct TypeOutput {
    pub(crate) finfo: pg_sys::FmgrInfo,
}

impl TypeOutput {
    pub fn for_type(oid: pg_sys::Oid) -> Self {
        let mut typoutput_oid = pg_sys::Oid::INVALID;
        let mut is_varlena = false;
        let mut finfo = pg_sys::FmgrInfo::default();
        // SAFETY: getTypeOutputInfo + fmgr_info are PG server-API
        // entry points; caller is in an active xact context (per
        // the lifetime invariant above), so CurrentMemoryContext
        // is the TopTransactionContext that owns this allocation
        // boundary.
        unsafe {
            pg_sys::getTypeOutputInfo(oid, &mut typoutput_oid, &mut is_varlena);
            pg_sys::fmgr_info(typoutput_oid, &mut finfo);
        }
        Self { finfo }
    }
}

/// Cached `typsend` `FmgrInfo` for a result-column type. Binary-
/// format counterpart to [`TypeOutput`]; same caching strategy
/// (resolved FmgrInfo, not just the typsend Oid). Consumed by
/// [`super::dest_receiver::ColumnEncoder::encode_into`] which
/// dispatches via `SendFunctionCall(&finfo, datum)` and writes
/// the resulting `bytea` payload (header stripped) into a
/// [`bytes::BytesMut`].
///
/// Same context-lifetime invariant as `TypeOutput`.
pub struct TypeSend {
    pub(crate) finfo: pg_sys::FmgrInfo,
}

impl TypeSend {
    pub fn for_type(oid: pg_sys::Oid) -> Self {
        let mut typsend_oid = pg_sys::Oid::INVALID;
        let mut is_varlena = false;
        let mut finfo = pg_sys::FmgrInfo::default();
        // SAFETY: see TypeOutput::for_type.
        unsafe {
            pg_sys::getTypeBinaryOutputInfo(oid, &mut typsend_oid, &mut is_varlena);
            pg_sys::fmgr_info(typsend_oid, &mut finfo);
        }
        Self { finfo }
    }
}

// ---------------------------------------------------------------------------
// Error translation
// ---------------------------------------------------------------------------

/// Build a `PgWireError` from an SPI numeric return code, looking
/// up the human name via `SPI_result_code_string`. Used when an
/// SPI call returns < 0 (failure) or a positive code that the
/// caller didn't expect.
pub fn spi_rc_error(api: &str, rc: i32) -> PgWireError {
    // SAFETY: SPI_result_code_string is a const lookup table
    // accessor; returns a static C string or NULL.
    let name_ptr = unsafe { pg_sys::SPI_result_code_string(rc) };
    let name = if name_ptr.is_null() {
        format!("rc={rc}")
    } else {
        // SAFETY: non-null + NUL-terminated static.
        unsafe { CStr::from_ptr(name_ptr) }
            .to_string_lossy()
            .into_owned()
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("pg_transport spi: {api} failed: {name}"),
    )))
}

/// Build a generic XX000 `PgWireError` with a `prefix: message`
/// rendering. The two bridges share this for "we caught a non-PG
/// error condition" cases (NUL bytes in input, count mismatches,
/// etc.).
pub fn generic_error(prefix: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("{prefix}: {message}"),
    )))
}

/// Convert a caught panic payload into a `PgWireError`. pgrx's
/// `pg_guard` catches PG ERROR longjmps and re-raises them via
/// `resume_unwind(Box::new(CaughtError::*))`, so the outer
/// `catch_unwind` receives a `Box<dyn Any>` whose concrete type is
/// `CaughtError`. Bare `panic_any(ErrorReportWithLevel)` and bare
/// `panic!("...")` are handled as fallbacks.
pub fn panic_to_pgwire(payload: Box<dyn Any + Send>) -> PgWireError {
    if let Some(caught) = payload.downcast_ref::<CaughtError>() {
        return error_report_to_pgwire(extract_report(caught));
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
    generic_error("pg_transport", "unknown panic payload")
}

/// Convert a [`CaughtError`] (the value type produced by
/// `pgrx::PgTryBuilder::execute().catch_others(Err)`) to a
/// `PgWireError`. Used by call sites that catch PG ERRORs via
/// PgTryBuilder rather than `catch_unwind` (which produces a
/// `Box<dyn Any>` payload — see [`panic_to_pgwire`]).
pub fn caught_error_to_pgwire(caught: &CaughtError) -> PgWireError {
    error_report_to_pgwire(extract_report(caught))
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
