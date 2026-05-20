//! Tuplestore RAII wrapper + the small FFI surface needed by the
//! §3.1 direct-path migration ([Stage A](../../../../docs/design/deferred/planner-executor-direct-path.md)).
//!
//! pgrx 0.18's pg18 bindings expose `tuplestore_begin_heap`,
//! `tuplestore_gettupleslot`, `tuplestore_end`, and friends, plus
//! `CreateDestReceiver(DestTuplestore)` — but they do **not**
//! expose `SetTuplestoreDestReceiverParams` (declared in
//! `executor/tstoreReceiver.h`, not pulled into bindgen's default
//! include set). We declare it ourselves via an `extern "C"` shim
//! in [`ffi`]; the symbol is in `libpostgres` so the link resolves.
//!
//! ## RAII discipline
//!
//! The wrappers here are *only* safe to construct inside an active
//! transaction with a valid `CurrentMemoryContext` — the
//! tuplestore allocates from that context and the destination
//! receiver expects xact-scoped state. v0 Stage A constructs them
//! inside [`super::spi::with_spi`] (which provides both); a future
//! `with_xact` primitive (Stage A planning) will provide the same
//! shape without the SPI overhead.
//!
//! Drop calls into PG (`tuplestore_end`, `DestReceiver::rDestroy`)
//! — also requires the original MemoryContext + xact still alive.
//! Outliving them causes UAF. Callers must ensure the wrappers
//! drop **before** the SPI session / xact bracket ends.
//!
//! Most items in this module are `dead_code`-allow'd: they're the
//! FFI surface for the Stage A direct executor (lands in a
//! follow-up commit) and have no in-crate caller yet. The
//! `#[pg_test]`s below exercise the lifecycle so the FFI link is
//! validated on every `just test` run.
#![allow(dead_code)]

use pgrx::pg_sys;

/// FFI symbols filtered out of pgrx's default bindings that we
/// re-declare locally. The PG signatures are stable; this is a
/// header-inclusion gap, not an ABI risk.
pub(crate) mod ffi {
    use pgrx::pg_sys;

    // SAFETY: declared in `src/include/executor/tstoreReceiver.h`,
    // implemented in `src/backend/executor/tstoreReceiver.c`. The
    // symbol is exported from `libpostgres` so the link resolves
    // against the postmaster the slot bgworker runs inside.
    unsafe extern "C" {
        /// Configure a `DestReceiver` returned by
        /// `CreateDestReceiver(DestTuplestore)` to write into the
        /// given tuplestore + memory context.
        ///
        /// PG 18 signature (verified against the system header):
        /// ```text
        /// extern void SetTuplestoreDestReceiverParams(
        ///     DestReceiver *self,
        ///     Tuplestorestate *tStore,
        ///     MemoryContext tContext,
        ///     bool detoast,
        ///     TupleDesc target_tupdesc,
        ///     const char *map_failure_msg);
        /// ```
        pub(crate) fn SetTuplestoreDestReceiverParams(
            self_: *mut pg_sys::DestReceiver,
            t_store: *mut pg_sys::Tuplestorestate,
            t_context: pg_sys::MemoryContext,
            detoast: bool,
            target_tupdesc: pg_sys::TupleDesc,
            map_failure_msg: *const std::ffi::c_char,
        );
    }
}

// ---------------------------------------------------------------------------
// Tuplestore — RAII over Tuplestorestate
// ---------------------------------------------------------------------------

/// Owned `*mut Tuplestorestate`. Drops via `tuplestore_end`.
///
/// `!Send` + `!Sync` via `PhantomData<*const ()>` because the
/// underlying PG state is bound to the thread's `CurrentMemoryContext`
/// and `CurrentResourceOwner` and must not cross threads. The
/// Stage A direct path lives on the slot bgworker's single
/// execution thread, same as the SPI bridge.
pub struct Tuplestore {
    raw: *mut pg_sys::Tuplestorestate,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Tuplestore {
    /// Create a heap-backed tuplestore via PG's
    /// `tuplestore_begin_heap`.
    ///
    /// * `random_access` — allow `tuplestore_rescan` / random
    ///   pointer ops. For the direct path's forward-only result
    ///   iteration this is `false`.
    /// * `inter_xact` — survive transaction boundaries. Always
    ///   `false` in v0 (we drop the store at end of the per-query
    ///   xact bracket).
    /// * `max_kbytes` — work_mem ceiling before spilling to disk.
    ///   Caller usually passes PG's `work_mem` GUC value.
    ///
    /// # Safety
    ///
    /// Must be called inside an active transaction with a valid
    /// `CurrentMemoryContext` (e.g. inside
    /// [`super::spi::with_spi`]). Calling `Drop` outside the
    /// originating MemoryContext is UB.
    pub unsafe fn begin_heap(random_access: bool, inter_xact: bool, max_kbytes: i32) -> Self {
        // SAFETY: tuplestore_begin_heap is a pure PG allocator
        // call that returns a fresh state pointer from the
        // current MemoryContext. Cannot fail (raises ERROR on OOM,
        // which longjmps out and is caught by the surrounding
        // with_spi / with_xact bracket).
        let raw = unsafe { pg_sys::tuplestore_begin_heap(random_access, inter_xact, max_kbytes) };
        Tuplestore {
            raw,
            _not_send: std::marker::PhantomData,
        }
    }

    /// Borrow the raw `*mut Tuplestorestate` for passing into FFI
    /// (`SetTuplestoreDestReceiverParams`,
    /// `tuplestore_gettupleslot`, etc). The pointer remains
    /// owned by `self`; do not free.
    pub fn as_ptr(&self) -> *mut pg_sys::Tuplestorestate {
        self.raw
    }
}

impl Drop for Tuplestore {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches begin_heap's allocation; caller
            // contract guarantees we're still in the same xact +
            // MemoryContext.
            unsafe { pg_sys::tuplestore_end(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// TuplestoreReceiver — RAII over a DestReceiver(DestTuplestore)
// ---------------------------------------------------------------------------

/// Owned `*mut DestReceiver` allocated by
/// `CreateDestReceiver(DestTuplestore)` and configured via
/// [`ffi::SetTuplestoreDestReceiverParams`].
///
/// Drops via the receiver's own `rDestroy` callback (which frees
/// the receiver struct in its allocating MemoryContext). The
/// tuplestore the receiver writes into is **not** owned by this
/// struct — caller manages the [`Tuplestore`] separately and must
/// keep it alive at least as long as the receiver writes into it.
pub struct TuplestoreReceiver {
    raw: *mut pg_sys::DestReceiver,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl TuplestoreReceiver {
    /// Allocate a `DestTuplestore` receiver and bind it to the
    /// given `tuplestore`.
    ///
    /// * `tuplestore` — destination. Must outlive the executor's
    ///   use of this receiver. Stage A creates both inside the
    ///   same `with_spi` closure so the lifetime is trivially
    ///   correct.
    /// * `t_context` — MemoryContext the receiver allocates its
    ///   per-row scratch from. Stage A passes
    ///   `CurrentMemoryContext`.
    /// * `detoast` — when true the receiver detoasts varlena
    ///   columns into the tuplestore (the writer pays; readers
    ///   skip detoast). Stage A passes `false` (we re-toast at
    ///   wire-encode time anyway).
    /// * `target_tupdesc` — the descriptor of the rows the
    ///   producer (executor) will emit. May be NULL for
    ///   "use whatever the producer reports."
    ///
    /// # Safety
    ///
    /// As for [`Tuplestore::begin_heap`]: must be called inside an
    /// active transaction with a valid `CurrentMemoryContext`. The
    /// `tuplestore` pointer's lifetime is enforced by reference;
    /// PG's pointer aliasing rules require it to remain valid for
    /// the receiver's lifetime.
    pub unsafe fn for_tuplestore(
        tuplestore: &Tuplestore,
        t_context: pg_sys::MemoryContext,
        detoast: bool,
        target_tupdesc: pg_sys::TupleDesc,
    ) -> Self {
        // SAFETY: CreateDestReceiver allocates from
        // CurrentMemoryContext; returns the function-table
        // appropriate for the requested CommandDest.
        let raw = unsafe { pg_sys::CreateDestReceiver(pg_sys::CommandDest::DestTuplestore) };

        // SAFETY: SetTuplestoreDestReceiverParams writes into the
        // function-table just allocated. The map_failure_msg is
        // NULL — we don't request type coercion failure
        // reporting in v0.
        unsafe {
            ffi::SetTuplestoreDestReceiverParams(
                raw,
                tuplestore.as_ptr(),
                t_context,
                detoast,
                target_tupdesc,
                std::ptr::null(),
            );
        }

        TuplestoreReceiver {
            raw,
            _not_send: std::marker::PhantomData,
        }
    }

    /// Borrow the raw `*mut DestReceiver` for passing into FFI
    /// (e.g. `PortalRun(..., dest, ...)`).
    pub fn as_ptr(&self) -> *mut pg_sys::DestReceiver {
        self.raw
    }
}

impl Drop for TuplestoreReceiver {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: every DestReceiver populated by
            // CreateDestReceiver has a non-null rDestroy callback
            // that frees the receiver in its allocating context.
            // Matches CreateDestReceiver's allocation.
            unsafe {
                if let Some(destroy) = (*self.raw).rDestroy {
                    destroy(self.raw);
                }
            }
            self.raw = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::backend::spi::with_spi;
    use pgrx::pg_test;
    use pgwire::error::PgWireResult;

    /// Validate the basic Tuplestore lifecycle: allocate inside an
    /// SPI session, drop on scope end. Catches link failures
    /// (`tuplestore_begin_heap` / `tuplestore_end` not resolving)
    /// and obvious memory corruption (xact teardown would crash).
    #[pg_test]
    fn pg_tuplestore_begin_end_lifecycle() {
        with_spi(|_ctx| -> PgWireResult<()> {
            // SAFETY: with_spi establishes the xact + MemoryContext
            // contract Tuplestore::begin_heap requires.
            let store = unsafe { Tuplestore::begin_heap(false, false, 1024) };
            // Use as_ptr() so the compiler can't optimise the
            // allocation away.
            assert!(
                !store.as_ptr().is_null(),
                "tuplestore_begin_heap returned null"
            );
            Ok(())
        })
        .expect("with_spi failed");
    }

    /// Validate the FFI shim for SetTuplestoreDestReceiverParams
    /// resolves and the full receiver-on-tuplestore lifecycle
    /// drops cleanly. Catches link failures of the locally-
    /// declared extern.
    #[pg_test]
    fn pg_tuplestore_receiver_lifecycle() {
        with_spi(|_ctx| -> PgWireResult<()> {
            // SAFETY: see pg_tuplestore_begin_end_lifecycle.
            let store = unsafe { Tuplestore::begin_heap(false, false, 1024) };
            let receiver = unsafe {
                TuplestoreReceiver::for_tuplestore(
                    &store,
                    pg_sys::CurrentMemoryContext,
                    false,
                    std::ptr::null_mut(),
                )
            };
            assert!(
                !receiver.as_ptr().is_null(),
                "CreateDestReceiver returned null"
            );
            // Drop order: receiver first, then store (the receiver
            // holds the store pointer; freeing the store first
            // would dangle inside the receiver until its rDestroy
            // runs).
            drop(receiver);
            drop(store);
            Ok(())
        })
        .expect("with_spi failed");
    }
}
