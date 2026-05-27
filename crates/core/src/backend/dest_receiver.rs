//! Shared `WireDestReceiver` — custom PG `DestReceiver` that
//! encodes [`DataRow`] frames inline during `PortalRun`, with no
//! intermediate tuplestore copy.
//!
//! Lifted out of [`super::extended::direct`] so both the
//! extended-query direct backend and the upcoming simple-query
//! direct backend
//! ([deferred/simple-query-direct-path.md](../../../../docs/design/deferred/simple-query-direct-path.md))
//! share a single `DestReceiver` implementation. Behaviour is
//! byte-for-byte identical to the pre-lift version; only
//! visibility (now `pub(crate)`) and the home module changed.
//!
//! ## Usage
//!
//! ```text
//! let mut data_rows: Vec<DataRow> = Vec::new();
//! let mut recv = WireDestReceiver::new(&encoders, &mut data_rows, ncols);
//! portal.run(count, true, recv.as_dest_receiver(), recv.as_dest_receiver(), &mut qc);
//! // data_rows now holds encoded DataRow frames.
//! ```

use std::ffi::CStr;
use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};
use pgrx::pg_sys;
use pgwire::api::Type;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::messages::data::DataRow;

use super::executor::{TupleDescRef, pg_ident_to_string};
use super::spi::{TypeOutput, TypeSend};

// ---------------------------------------------------------------------------
// WireDestReceiver — the DestReceiver vtable struct
// ---------------------------------------------------------------------------

/// State passed through the `DestReceiver` vtable callbacks.
///
/// `#[repr(C)]` with `base` at offset 0 so the cast from
/// `*mut DestReceiver` to `*mut WireDestReceiver` is sound —
/// standard C "inheritance" pattern used by PG's own
/// `DR_transientrel`, `DR_copy`, etc.
///
/// The lifetime `'a` ties this receiver to the borrowed encoder
/// slice and output `Vec<DataRow>` — the borrow checker prevents
/// either from being dropped while the receiver exists.
#[repr(C)]
pub(crate) struct WireDestReceiver<'a> {
    /// Must be at offset 0. PG calls through this vtable.
    base: pg_sys::DestReceiver,
    /// Per-column encoders (borrowed from the caller's Vec).
    encoders: *const ColumnEncoder,
    /// Output rows pushed here by `receiveSlot`.
    rows: *mut Vec<DataRow>,
    /// Number of result columns.
    ncols: i16,
    /// Ties lifetime to the borrowed encoders + rows.
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> WireDestReceiver<'a> {
    /// Build a `WireDestReceiver` on the stack. The returned
    /// struct borrows `encoders` and `rows` — caller must keep
    /// both alive for the duration of `PortalRun`.
    pub(crate) fn new(
        encoders: &'a [ColumnEncoder],
        rows: &'a mut Vec<DataRow>,
        ncols: i16,
    ) -> Self {
        WireDestReceiver {
            base: pg_sys::DestReceiver {
                receiveSlot: Some(wire_receive_slot),
                rStartup: Some(wire_startup),
                rShutdown: Some(wire_shutdown),
                rDestroy: Some(wire_destroy),
                mydest: pg_sys::CommandDest::DestNone,
            },
            encoders: encoders.as_ptr(),
            rows: rows as *mut _,
            ncols,
            _lifetime: PhantomData,
        }
    }

    /// Return a `*mut DestReceiver` suitable for `Portal::run`.
    pub(crate) fn as_dest_receiver(&mut self) -> *mut pg_sys::DestReceiver {
        &mut self.base as *mut pg_sys::DestReceiver
    }
}

/// `receiveSlot` callback — encodes one row directly into a
/// `DataRow` and pushes it onto the output Vec.
///
/// # Safety
///
/// Called by the executor inside `PortalRun`. `slot` is a valid
/// `TupleTableSlot` owned by the executor. `self_` points to the
/// `base` field of our `WireDestReceiver` (offset 0).
unsafe extern "C-unwind" fn wire_receive_slot(
    slot: *mut pg_sys::TupleTableSlot,
    self_: *mut pg_sys::DestReceiver,
) -> bool {
    let recv = self_ as *mut WireDestReceiver;
    let ncols = unsafe { (*recv).ncols } as usize;

    // Deform all columns so tts_values / tts_isnull are populated.
    unsafe { pg_sys::slot_getallattrs(slot) };

    let mut buf = BytesMut::with_capacity(64);
    for c in 0..ncols {
        let is_null = unsafe { *(*slot).tts_isnull.add(c) };
        if is_null {
            buf.put_i32(-1);
        } else {
            let datum = unsafe { *(*slot).tts_values.add(c) };
            let encoder = unsafe { &*(*recv).encoders.add(c) };
            encoder.encode_into(datum, &mut buf);
        }
    }
    unsafe { (*(*recv).rows).push(DataRow::new(buf, (*recv).ncols)) };
    true
}

/// `rStartup` callback — no-op (encoders are pre-built at Bind /
/// PortalStart time).
unsafe extern "C-unwind" fn wire_startup(
    _self: *mut pg_sys::DestReceiver,
    _operation: std::ffi::c_int,
    _typeinfo: pg_sys::TupleDesc,
) {
}

/// `rShutdown` callback — no-op (BytesMut is Rust-owned).
unsafe extern "C-unwind" fn wire_shutdown(_self: *mut pg_sys::DestReceiver) {}

/// `rDestroy` callback — no-op (struct is stack-allocated in Rust,
/// not palloc'd — Rust Drop handles cleanup).
unsafe extern "C-unwind" fn wire_destroy(_self: *mut pg_sys::DestReceiver) {}

// ---------------------------------------------------------------------------
// ColumnEncoder — per-column type I/O cache
// ---------------------------------------------------------------------------

/// Per-column result encoder. Holds the cached
/// (typoutput | typsend) lookup so the row loop just calls
/// `encoder.encode_into(datum, buf)` per cell.
pub(crate) enum ColumnEncoder {
    Text(TypeOutput),
    Binary(TypeSend),
}

impl ColumnEncoder {
    pub(crate) fn for_column(type_oid: pg_sys::Oid, format: FieldFormat) -> Self {
        match format {
            FieldFormat::Text => Self::Text(TypeOutput::for_type(type_oid)),
            FieldFormat::Binary => Self::Binary(TypeSend::for_type(type_oid)),
        }
    }

    /// Encode `datum` directly into `buf` as a length-prefixed
    /// field, avoiding the intermediate `Vec<u8>` allocation.
    ///
    /// Hot path. Dispatches through the cached
    /// [`pg_sys::FmgrInfo`] held by [`TypeOutput`] / [`TypeSend`]
    /// so the per-cell cost is one direct function-pointer call
    /// (`fn_addr`) — no `fmgr_info` / syscache lookup per cell.
    /// Matches vanilla [`printtup`'s hot loop](../../../../../postgres/src/backend/access/common/printtup.c#L361)
    /// shape.
    pub(crate) fn encode_into(&self, datum: pg_sys::Datum, buf: &mut BytesMut) {
        match self {
            Self::Text(fns) => {
                // SAFETY: `OutputFunctionCall` reads `fn_addr` /
                // `fn_oid` / etc. from the FmgrInfo to invoke the
                // type's text-output function — it does not
                // mutate the FmgrInfo struct itself, despite the
                // `*mut` in the C signature (PG headers have no
                // `const` discipline on FmgrInfo). Returns a
                // palloc'd NUL-terminated cstring owned by the
                // current MemoryContext.
                unsafe {
                    let finfo_ptr = &fns.finfo as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
                    let ptr = pg_sys::OutputFunctionCall(finfo_ptr, datum);
                    let slice = CStr::from_ptr(ptr).to_bytes();
                    buf.put_i32(slice.len() as i32);
                    buf.put_slice(slice);
                    pg_sys::pfree(ptr as *mut _);
                }
            }
            Self::Binary(fns) => {
                // SAFETY: as Text branch. `SendFunctionCall`
                // returns a palloc'd bytea (varlena).
                unsafe {
                    let finfo_ptr = &fns.finfo as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
                    let ptr = pg_sys::SendFunctionCall(finfo_ptr, datum);
                    let slice = pgrx::varlena::varlena_to_byte_slice(ptr as *const pg_sys::varlena);
                    buf.put_i32(slice.len() as i32);
                    buf.put_slice(slice);
                    pg_sys::pfree(ptr as *mut _);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schema + encoder construction
// ---------------------------------------------------------------------------

/// Convert a `CommandTag` enum to its display name. Falls back to
/// `"OK"` for unknown / null tags (utility statements before
/// `InitializeQueryCompletion` ran).
///
/// Returns a `&'static str` because PG's command-tag table
/// (`commandTagBuiltinList[]`) is statically allocated for the
/// lifetime of the backend — see `src/backend/tcop/cmdtag.c`. The
/// strings are ASCII (e.g. `"SELECT"`, `"INSERT 0 1"`), so
/// `from_utf8_unchecked` is sound. Caller pays no allocation.
pub(crate) fn command_tag_name(tag: pg_sys::CommandTag::Type) -> &'static str {
    let ptr = unsafe { pg_sys::GetCommandTagName(tag) };
    if ptr.is_null() {
        "OK"
    } else {
        // SAFETY: GetCommandTagName returns a pointer into PG's
        // static commandTagBuiltinList table; lifetime is 'static
        // and contents are ASCII.
        unsafe {
            let bytes = CStr::from_ptr(ptr).to_bytes();
            std::str::from_utf8_unchecked(bytes)
        }
    }
}

/// Build a `(schema, encoders)` pair from a tupdesc using a
/// single uniform [`FieldFormat`] for every column. Used by the
/// simple-query direct backend: the `'Q'` protocol carries no
/// per-column format vector, but PG's FETCH-from-binary-cursor
/// shorthand promotes the *whole* result set to binary (see
/// [postgres.c:1259-1273](../../../../../postgres/src/backend/tcop/postgres.c#L1259-L1273)),
/// so a single format applies to every column. Caller picks
/// Text or Binary based on whether the parsetree is a
/// `FetchStmt` against a `CURSOR_OPT_BINARY` portal.
///
/// Mirrors the per-execute construction in the extended-query
/// direct backend
/// ([`super::extended::direct`](../extended/direct.rs)), but
/// reads the column name + OID directly off the portal's tupdesc
/// instead of using pre-cached `param_oids` / `column_oids`
/// vectors, and applies a single format instead of an
/// extended-query per-column format vector.
pub(crate) fn schema_and_encoders_uniform(
    tupdesc: &TupleDescRef<'_>,
    format: FieldFormat,
) -> (Vec<FieldInfo>, Vec<ColumnEncoder>) {
    let n = tupdesc.len();
    let mut schema = Vec::with_capacity(n);
    let mut encoders = Vec::with_capacity(n);
    for attr in tupdesc.iter() {
        // SAFETY: attr.attname is a PG `NameData` buffer
        // containing an ASCII identifier (column name).
        let name = unsafe { pg_ident_to_string(attr.attname.data.as_ptr()) };
        let oid = attr.atttypid;
        schema.push(FieldInfo::new(
            name,
            None,
            None,
            Type::from_oid(oid.to_u32()).unwrap_or(Type::TEXT),
            format,
        ));
        encoders.push(ColumnEncoder::for_column(oid, format));
    }
    (schema, encoders)
}
