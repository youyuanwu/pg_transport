//! Planner+executor primitives for the §3.1 Stage A direct path.
//!
//! Mirrors the [`super::spi`] discipline (RAII wrappers + a
//! catch_unwind-bracketed entry point) but uses PG's planner +
//! executor APIs directly instead of going through SPI. The
//! direct executor ([`super::extended::direct`]) consumes these
//! primitives in the order:
//!
//! ```text
//! with_xact(|ctx| {
//!     pg_parse_query → raw RawStmt list
//!     pg_analyze_and_rewrite_varparams → analyzed Query list
//!     CachedPlanSource::create + complete + save  (parse time)
//!     CachedPlanSource::get_plan → CachedPlan      (execute time)
//!     ParamList::build → owned ParamListInfo        (execute time)
//!     Portal::create_anonymous + define + start + run
//!         ↓ executor writes into WireDestReceiver
//!     // drop order: Portal first, then CachedPlan, then ParamList
//! })
//! ```
//!
//! Naming follows the [doc Stage C](../../../../docs/design/deferred/planner-executor-direct-path.md)
//! rename plan (`with_xact`, not `with_spi`) so subsequent stages
//! converge on the final names.
//!
//! Most items are `dead_code`-allow'd until the direct executor
//! lands (next commit); the `#[pg_test]`s below validate the FFI
//! link + lifecycle so regressions surface on every `just test`.

#![allow(dead_code)]

use std::any::Any;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};

use pgrx::pg_sys;
use pgwire::error::PgWireResult;

use super::spi::panic_to_pgwire;

// ---------------------------------------------------------------------------
// XactCtx + with_xact — xact bracket without SPI
// ---------------------------------------------------------------------------

/// In-xact-session token. Mirrors [`super::spi::SpiCtx`] but proves
/// only that an active transaction + snapshot exist; SPI is **not**
/// open. The direct path's wrappers ([`CachedPlanSource::create`],
/// [`Portal::create_anonymous`], etc.) take `&XactCtx` to enforce
/// this at the type level.
///
/// `!Send` + `!Sync` via `PhantomData<*const ()>`. PG xact state
/// is per-backend, single-threaded.
pub struct XactCtx {
    _phantom: PhantomData<*const ()>,
}

impl XactCtx {
    pub(crate) fn new() -> Self {
        XactCtx {
            _phantom: PhantomData,
        }
    }
}

/// Run `body` inside `StartTransactionCommand` + optional
/// `PushActiveSnapshot` ... `PopActiveSnapshot` +
/// `CommitTransactionCommand`. No `SPI_connect` (that's
/// [`super::spi::with_spi`]'s job).
///
/// `needs_snapshot` mirrors vanilla `exec_simple_query`'s
/// `analyze_requires_snapshot(parsetree)` gate at
/// [postgres.c:1191](../../../../../postgres/src/backend/tcop/postgres.c#L1191):
/// pass `true` when the body will run a parse-analysis /
/// rewrite / plan that needs a transaction snapshot
/// (`SELECT` / DML / `EXPLAIN` / `DECLARE CURSOR` /
/// `CREATE TABLE AS` / `CALL`), and `false` for everything else
/// (`SET`, `BEGIN` / `COMMIT` / `ROLLBACK`, `SAVEPOINT`,
/// `CREATE TABLE`, …). Wrong `false` makes `analyze` raise on
/// `GetActiveSnapshot`; wrong `true` only costs a snapshot
/// acquisition. Callers that already hold a parsetree should
/// use `pg_sys::analyze_requires_snapshot(raw_stmt)` directly.
///
/// `false` is the only safe value when entering from
/// `TBLOCK_ABORT` (`GetTransactionSnapshot` from `TBLOCK_ABORT`
/// asserts). Combined with [`Portal::start`] being called with
/// `InvalidSnapshot`, this is exactly vanilla's pattern for
/// running `ROLLBACK` / `COMMIT` from `TBLOCK_ABORT` without
/// any short-circuit.
///
/// Error path:
/// - Body returns `Ok(value)` → `Pop` (if pushed) + `Commit`, return value.
/// - Body returns `Err(e)` → `Pop` (if pushed) + `Commit` (xact
///   state intact; we just propagate the typed error). Mirrors
///   `with_spi`.
/// - Body panics (PG ERROR longjmp'd to Rust panic) →
///   `AbortCurrentTransaction` (handles snapshot stack itself;
///   we must NOT `Pop` after Abort), convert payload to
///   `PgWireError`.
///
/// `body` receives a `&`[`XactCtx`] to use the in-xact-only APIs.
///
/// # Reentrancy
///
/// Calling `with_xact` from inside another `with_xact` or
/// `with_spi` would double-Start. PG's xact machinery makes the
/// second Start a no-op (becomes `CommandCounterIncrement`) but
/// the second Commit would be incorrect. **Do not nest.** Direct
/// path callers go straight from the wire layer, same as
/// `with_spi`.
pub fn with_xact<T, F>(needs_snapshot: bool, body: F) -> PgWireResult<T>
where
    F: FnOnce(&XactCtx) -> PgWireResult<T>,
{
    // SAFETY: Start/Push are PG server-API entry points safe to
    // call from a bgworker. Same shape as with_spi's prologue.
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        if needs_snapshot {
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        }
    }

    let outcome: Result<PgWireResult<T>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| {
            let ctx = XactCtx::new();
            body(&ctx)
        }));

    match outcome {
        Ok(Ok(value)) => {
            // SAFETY: matched Start/Push above; closure returned
            // cleanly. Pop (if pushed) + Commit drains the
            // snapshot stack and closes the xact.
            unsafe {
                if needs_snapshot {
                    pg_sys::PopActiveSnapshot();
                }
                pg_sys::CommitTransactionCommand();
            }
            Ok(value)
        }
        Ok(Err(err)) => {
            // Body returned a typed PgWireError without raising a
            // PG ERROR. Xact state intact; Pop (if pushed) +
            // Commit (not Abort).
            unsafe {
                if needs_snapshot {
                    pg_sys::PopActiveSnapshot();
                }
                pg_sys::CommitTransactionCommand();
            }
            Err(err)
        }
        Err(panic_payload) => {
            // PG ERROR longjmp'd through the body. AbortCurrent-
            // Transaction handles snapshot cleanup itself; we
            // must NOT call Pop here.
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
// CachedPlanSource — long-lived parse-time artifact
// ---------------------------------------------------------------------------

/// Owned `*mut CachedPlanSource`. Drops via `DropCachedPlan`.
///
/// Lifecycle: [`Self::create`] from a raw parse tree, then
/// [`Self::complete`] with the analyzed querytree list, then
/// [`Self::save`] to promote into PG's cache memory (so the
/// source survives MemoryContext resets between parse and
/// execute). [`Self::get_plan`] returns a per-execute [`CachedPlan`].
///
/// `Send + Sync` are unsafe-impl'd by hand (mirrors
/// [`super::spi::SpiPlan`]): the underlying PG state is per-backend
/// thread-local, but pgwire's `PortalStore` stashes the
/// containing [`super::extended::PreparedStatement`] as
/// `Arc<dyn ... + Send + Sync>`. The slot bgworker is
/// single-threaded, so the stash never actually crosses threads;
/// the bound is a type-system constraint, not a runtime one.
pub struct CachedPlanSource {
    raw: *mut pg_sys::CachedPlanSource,
}

// SAFETY: see CachedPlanSource doc comment — slot is single-threaded.
unsafe impl Send for CachedPlanSource {}
unsafe impl Sync for CachedPlanSource {}

impl CachedPlanSource {
    /// Create an empty CachedPlanSource from a raw parse tree.
    /// Must be followed by [`Self::complete`] before any
    /// [`Self::get_plan`].
    ///
    /// # Safety
    ///
    /// Must be called inside [`with_xact`] (or another active
    /// transaction). `raw_parse_tree` must remain valid until
    /// `CreateCachedPlan` returns — it `copyObject`s the tree
    /// into the source's own memory context.
    pub unsafe fn create(
        _ctx: &XactCtx,
        raw_parse_tree: *mut pg_sys::RawStmt,
        query_string: &CStr,
        command_tag: pg_sys::CommandTag::Type,
    ) -> Self {
        // SAFETY: CreateCachedPlan never returns null; it raises
        // ERROR on alloc failure which longjmps through us.
        let raw =
            unsafe { pg_sys::CreateCachedPlan(raw_parse_tree, query_string.as_ptr(), command_tag) };
        CachedPlanSource { raw }
    }

    /// Fill in the analyzed querytree list, parameter types, and
    /// planning options. Must be called exactly once between
    /// [`Self::create`] and the first [`Self::get_plan`].
    ///
    /// # Safety
    ///
    /// Must be called inside [`with_xact`]. `querytree_list` must
    /// be the output of `pg_analyze_and_rewrite_*` on the same
    /// raw parse tree the source was created from. `query_context`
    /// is the MemoryContext the querytree list lives in; pass
    /// `CurrentMemoryContext` for the typical case.
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn complete(
        &self,
        querytree_list: *mut pg_sys::List,
        query_context: pg_sys::MemoryContext,
        param_types: &[pg_sys::Oid],
        cursor_options: i32,
        fixed_result: bool,
    ) {
        let (ptypes, nparams) = if param_types.is_empty() {
            (std::ptr::null_mut(), 0_i32)
        } else {
            (
                param_types.as_ptr() as *mut pg_sys::Oid,
                param_types.len() as i32,
            )
        };
        // SAFETY: matches the PG 18 signature; nulls for parser
        // setup are valid ("we already have the analyzed list").
        unsafe {
            pg_sys::CompleteCachedPlan(
                self.raw,
                querytree_list,
                query_context,
                ptypes,
                nparams,
                None,                 // parserSetup: none, we did the analyze ourselves
                std::ptr::null_mut(), // parserSetupArg
                cursor_options,
                fixed_result,
            );
        }
    }

    /// Promote this source to PG's `CachedMemoryContext` so it
    /// survives the parse-time MemoryContext reset.
    ///
    /// # Safety
    ///
    /// Must be called after [`Self::complete`] and inside a
    /// transaction.
    pub unsafe fn save(&self) {
        // SAFETY: simple pointer call; PG handles all the cache
        // bookkeeping.
        unsafe { pg_sys::SaveCachedPlan(self.raw) };
    }

    /// Get a planned form (re-plan if needed). Returned
    /// [`CachedPlan`] is refcounted via `CurrentResourceOwner` and
    /// must be released before the resource owner goes away.
    ///
    /// # Safety
    ///
    /// Must be called inside [`with_xact`]. The current process's
    /// `CurrentResourceOwner` is used as the holding owner.
    pub unsafe fn get_plan(&self, params: &ParamList<'_>) -> CachedPlan<'_> {
        // SAFETY: GetCachedPlan is the documented entry point;
        // raises ERROR on failure which longjmps out.
        let owner = unsafe { pg_sys::CurrentResourceOwner };
        let raw = unsafe {
            pg_sys::GetCachedPlan(self.raw, params.as_ptr(), owner, std::ptr::null_mut())
        };
        CachedPlan {
            raw,
            owner,
            _phantom: PhantomData,
        }
    }

    /// Borrow the raw `*mut CachedPlanSource` (e.g. to feed into
    /// `PortalDefineQuery`).
    pub fn as_ptr(&self) -> *mut pg_sys::CachedPlanSource {
        self.raw
    }

    /// Borrow the result-row descriptor (`resultDesc`).
    ///
    /// Returns `None` for utility statements and DML without
    /// `RETURNING`. The returned [`TupleDescRef`] borrows from
    /// `self` — it cannot outlive the source.
    ///
    /// # Safety
    ///
    /// [`Self::complete`] must have been called with
    /// `fixed_result = true` so that `resultDesc` is populated.
    pub unsafe fn result_desc(&self) -> Option<TupleDescRef<'_>> {
        let tupdesc = unsafe { (*self.raw).resultDesc };
        unsafe { TupleDescRef::from_raw(tupdesc) }
    }
}

impl Drop for CachedPlanSource {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches CreateCachedPlan. DropCachedPlan
            // refuses to drop a source with extant CachedPlans
            // (refcount > 0) so the per-execute `CachedPlan` must
            // be dropped first.
            unsafe { pg_sys::DropCachedPlan(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// CachedPlan — per-execute refcounted plan handle
// ---------------------------------------------------------------------------

/// Owned `*mut CachedPlan`. Drops via `ReleaseCachedPlan(plan,
/// owner)` to decrement the refcount.
///
/// Short-lived: created at the start of an Execute via
/// [`CachedPlanSource::get_plan`], dropped after `PortalRun`
/// returns (before the parent [`CachedPlanSource`] is dropped).
///
/// The lifetime `'src` ties this plan to the [`CachedPlanSource`]
/// it came from — the borrow checker prevents dropping the source
/// while a plan handle is alive.
pub struct CachedPlan<'src> {
    raw: *mut pg_sys::CachedPlan,
    /// The `ResourceOwner` that was current at `GetCachedPlan` time.
    /// We must release against the same owner — PG records the
    /// refcount-holder identity here.
    owner: pg_sys::ResourceOwner,
    _phantom: PhantomData<&'src CachedPlanSource>,
}

impl CachedPlan<'_> {
    /// Borrow the raw `*mut CachedPlan` (e.g. to feed into
    /// `PortalDefineQuery`'s `cplan` argument).
    pub fn as_ptr(&self) -> *mut pg_sys::CachedPlan {
        self.raw
    }

    /// Get the planned-statement list owned by this CachedPlan.
    /// The list must not outlive `self`.
    ///
    /// # Safety
    ///
    /// Reads through the CachedPlan's `stmt_list` field. Safe as
    /// long as `self` is alive (refcount held).
    pub unsafe fn stmt_list(&self) -> *mut pg_sys::List {
        // SAFETY: CachedPlan has a stable stmt_list field across
        // PG 12+.
        unsafe { (*self.raw).stmt_list }
    }
}

impl Drop for CachedPlan<'_> {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches GetCachedPlan with the same owner.
            unsafe { pg_sys::ReleaseCachedPlan(self.raw, self.owner) };
            self.raw = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// ParamList — owned ParamListInfo
// ---------------------------------------------------------------------------

/// Owned `pg_sys::ParamListInfo` allocated via `makeParamList`.
///
/// Allocated in `CurrentMemoryContext` (which is the xact context
/// inside [`with_xact`]). PG frees the underlying palloc'd memory
/// when the xact commits/aborts, so Drop is a no-op — but we zero
/// the pointer defensively to prevent use-after-free.
///
/// The raw pointer is passed to [`CachedPlanSource::get_plan`] and
/// [`Portal::start`]; both borrow it for the duration of their
/// call only (PG copies what it needs into its own contexts).
pub struct ParamList<'a> {
    raw: pg_sys::ParamListInfo,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> ParamList<'a> {
    /// Build a `ParamList` from decoded parameter values.
    ///
    /// All three slices must have the same length (one entry per
    /// `$n` bind parameter).
    ///
    /// # Safety
    ///
    /// Must be called inside [`with_xact`] with a valid
    /// `CurrentMemoryContext`. The `values` Datums must remain
    /// valid for the lifetime `'a` (they point into palloc'd
    /// memory that lives until the xact ends).
    pub unsafe fn build(
        param_oids: &[pg_sys::Oid],
        values: &'a [pg_sys::Datum],
        is_null: &[bool],
    ) -> Self {
        let n = param_oids.len();
        let raw = unsafe { pg_sys::makeParamList(n as i32) };
        if !raw.is_null() {
            let base = unsafe { (*raw).params.as_mut_ptr() };
            for i in 0..n {
                unsafe {
                    let slot = base.add(i);
                    (*slot).value = values[i];
                    (*slot).isnull = is_null[i];
                    (*slot).pflags = pg_sys::PARAM_FLAG_CONST as u16;
                    (*slot).ptype = param_oids[i];
                }
            }
        }
        ParamList {
            raw,
            _lifetime: PhantomData,
        }
    }

    /// Borrow the raw pointer for passing into FFI
    /// (`GetCachedPlan`, `PortalStart`, etc.).
    pub fn as_ptr(&self) -> pg_sys::ParamListInfo {
        self.raw
    }

    /// Create an empty `ParamList` (null pointer) for
    /// parameterless queries.
    pub fn empty() -> Self {
        ParamList {
            raw: std::ptr::null_mut(),
            _lifetime: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// pg_ident_to_string — ASCII-only CStr → String shortcut
// ---------------------------------------------------------------------------

/// Convert a PG identifier-class NUL-terminated C string to an
/// owned `String` without paying for UTF-8 validation.
///
/// PG enforces ASCII for identifier-class strings at parse time
/// (table / column / type names, command tags, SPI rc names,
/// etc.). `CStr::to_string_lossy().into_owned()` re-scans the
/// bytes to validate UTF-8 every call; for short hot-path names
/// (e.g. one per result column per `'Q'`) that scan is pure
/// overhead.
///
/// In debug builds the ASCII assumption is checked via
/// `debug_assert!` so a regression — e.g. PG ever loosening the
/// identifier-charset rule, or this helper being applied to a
/// non-identifier C string — surfaces loudly in tests.
///
/// # Safety
///
/// `ptr` must be a valid pointer to a NUL-terminated C string
/// whose bytes are ASCII. Identifier-class fields from
/// `Form_pg_attribute`, `GetCommandTagName`,
/// `SPI_result_code_string`, etc. all satisfy this.
pub(crate) unsafe fn pg_ident_to_string(ptr: *const std::ffi::c_char) -> String {
    let bytes = unsafe { CStr::from_ptr(ptr) }.to_bytes();
    debug_assert!(
        bytes.is_ascii(),
        "expected ASCII PG identifier, got {bytes:?}"
    );
    // SAFETY: ASCII is valid UTF-8.
    unsafe { std::str::from_utf8_unchecked(bytes) }.to_owned()
}

// ---------------------------------------------------------------------------
// TupleDescRef — borrowed TupleDesc accessor
// ---------------------------------------------------------------------------

/// Borrowed view of a `pg_sys::TupleDesc` owned by another PG
/// object (`CachedPlanSource.resultDesc`, `SPITupleTable.tupdesc`,
/// etc.).
///
/// Non-owning: no Drop, no refcount management. The lifetime `'a`
/// ties this reference to the owner, preventing use-after-free at
/// compile time.
///
/// Provides safe iteration over column attributes without
/// repeating the `TupleDescAttr` + null-check boilerplate at
/// every call site.
pub struct TupleDescRef<'a> {
    raw: pg_sys::TupleDesc,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> TupleDescRef<'a> {
    /// Wrap a non-null `pg_sys::TupleDesc` borrowed from `owner`.
    ///
    /// Returns `None` if `raw` is null (utility statements, DML
    /// without `RETURNING`, etc.).
    ///
    /// # Safety
    ///
    /// `raw` must be a valid `TupleDesc` that remains live for
    /// `'a`. The caller must not free or modify the underlying
    /// `TupleDescData` while this reference exists.
    pub unsafe fn from_raw(raw: pg_sys::TupleDesc) -> Option<Self> {
        if raw.is_null() {
            None
        } else {
            Some(TupleDescRef {
                raw,
                _lifetime: PhantomData,
            })
        }
    }

    /// Number of user attributes.
    pub fn len(&self) -> usize {
        // SAFETY: raw is non-null by construction.
        unsafe { (*self.raw).natts as usize }
    }

    /// Is this a zero-column descriptor?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get attribute `i` (0-based). Returns `None` if out of range.
    pub fn get(&self, i: usize) -> Option<&pg_sys::FormData_pg_attribute> {
        if i >= self.len() {
            None
        } else {
            // SAFETY: i in 0..natts; TupleDescAttr is the
            // canonical PG accessor macro.
            Some(unsafe { &*pg_sys::TupleDescAttr(self.raw, i as i32) })
        }
    }

    /// Column name for attribute `i` (0-based).
    pub fn col_name(&self, i: usize) -> Option<String> {
        self.get(i)
            // SAFETY: attname is a PG NameData buffer containing
            // an ASCII identifier.
            .map(|attr| unsafe { pg_ident_to_string(attr.attname.data.as_ptr()) })
    }

    /// Column type OID for attribute `i` (0-based).
    pub fn col_type_oid(&self, i: usize) -> Option<pg_sys::Oid> {
        self.get(i).map(|attr| attr.atttypid)
    }

    /// Iterate over all attributes.
    pub fn iter(&self) -> impl Iterator<Item = &pg_sys::FormData_pg_attribute> {
        (0..self.len()).map(move |i| self.get(i).unwrap())
    }

    /// Borrow the raw pointer for FFI pass-through.
    pub fn as_ptr(&self) -> pg_sys::TupleDesc {
        self.raw
    }
}

// ---------------------------------------------------------------------------
// Portal — execution context
// ---------------------------------------------------------------------------

/// Owned `pg_sys::Portal`. Drops via `PortalDrop(portal, false)`.
///
/// Stage A uses anonymous portals only ([`Self::create_anonymous`]).
/// Named portals (cursors held across Execute calls) are a Stage
/// C+ concern.
pub struct Portal {
    raw: pg_sys::Portal,
    _phantom: PhantomData<*const ()>,
}

impl Portal {
    /// Create an anonymous portal with a process-unique
    /// generated name (e.g. `pgxport_42`).
    ///
    /// We do **not** use PG's `""` (unnamed) portal because
    /// `CreatePortal("", allowDup=true, dupSilent=true)` would
    /// drop any pre-existing portal of the same name — and inside
    /// nested execution contexts (a SQL function, pgrx pg_test's
    /// `SELECT fn()` wrapper, etc.) the unnamed portal is the
    /// currently-executing one. Dropping a `PORTAL_ACTIVE` portal
    /// triggers an `AssertState` in `PortalDrop` → SIGABRT.
    ///
    /// Generated names are unique within the process (atomic
    /// counter), so `allowDup=false` is safe and catches
    /// genuine bugs.
    ///
    /// The name is written into a stack `[u8; 32]` buffer and
    /// passed straight to `CreatePortal` (which copies the bytes
    /// into the portal's own MemoryContext before returning), so
    /// per-statement portal creation pays no heap allocation for
    /// the name — neither `format!` nor `CString::new`.
    ///
    /// # Safety
    ///
    /// Must be called inside [`with_xact`].
    pub unsafe fn create_anonymous(_ctx: &XactCtx) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        // Layout: "pgxport_" (8) + up-to-20 ASCII digits (u64::MAX
        // is 20 digits) + trailing NUL (1) = 29 bytes max, rounded
        // up to 32 for alignment.
        let mut buf = [0u8; 32];
        buf[..8].copy_from_slice(b"pgxport_");
        // Write digits least-significant-first into a scratch
        // buffer, then copy them in MSB-first order into `buf`.
        let mut digits = [0u8; 20];
        let mut d = digits.len();
        if id == 0 {
            d -= 1;
            digits[d] = b'0';
        } else {
            let mut n = id;
            while n > 0 {
                d -= 1;
                digits[d] = b'0' + (n % 10) as u8;
                n /= 10;
            }
        }
        let digit_len = digits.len() - d;
        buf[8..8 + digit_len].copy_from_slice(&digits[d..]);
        // buf[8 + digit_len] is already 0 from the array init, so
        // the string is NUL-terminated.

        // SAFETY: CreatePortal(name, allowDup=false, dupSilent=true).
        // The name pointer is read synchronously and copied into
        // the portal's MemoryContext; our stack buffer doesn't
        // need to outlive this call. dupSilent doesn't matter
        // when allowDup=false (collision raises ERROR which
        // longjmps out).
        let raw =
            unsafe { pg_sys::CreatePortal(buf.as_ptr() as *const std::ffi::c_char, false, true) };
        Portal {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Populate the portal with a query + planned statements +
    /// cached plan. PG copies the strings into the portal's own
    /// context, so caller does not need to keep them alive past
    /// this call.
    ///
    /// # Safety
    ///
    /// `stmts` must be a `List*` of `PlannedStmt*` (typically from
    /// [`CachedPlan::stmt_list`]); `cached_plan` must be the
    /// matching live [`CachedPlan`].
    pub unsafe fn define(
        &self,
        query_string: &CStr,
        command_tag: pg_sys::CommandTag::Type,
        stmts: *mut pg_sys::List,
        cached_plan: *mut pg_sys::CachedPlan,
    ) {
        // SAFETY: prepStmtName = NULL for the unnamed-statement
        // case; PortalDefineQuery copies what it needs.
        unsafe {
            pg_sys::PortalDefineQuery(
                self.raw,
                std::ptr::null(),
                query_string.as_ptr(),
                command_tag,
                stmts,
                cached_plan,
            );
        }
    }

    /// Bind parameters + initialise the portal's executor state.
    /// `eflags` is the standard `EXEC_FLAG_*` mask (pass `0` for
    /// "no special flags").
    ///
    /// Callers should normally pass
    /// `std::ptr::null_mut::<pg_sys::SnapshotData>()` (i.e.
    /// `InvalidSnapshot`) for `snapshot` — mirrors vanilla
    /// `exec_simple_query` /
    /// `exec_execute_message` at
    /// [postgres.c:1235](../../../../../postgres/src/backend/tcop/postgres.c#L1235)
    /// and [`:2052`](../../../../../postgres/src/backend/tcop/postgres.c#L2052).
    /// `PortalStart` itself takes a fresh
    /// `GetTransactionSnapshot` for `PORTAL_ONE_SELECT` and pops
    /// it before returning; passing an explicit
    /// `GetActiveSnapshot()` is correct only when [`with_xact`]
    /// was called with `needs_snapshot = true`, and even then
    /// `InvalidSnapshot` is the recommended shape because
    /// vanilla uses it unconditionally and it's the only value
    /// that's safe across the `needs_snapshot = false` path.
    ///
    /// # Safety
    ///
    /// Caller must have already called [`Self::define`].
    pub unsafe fn start(&self, params: &ParamList<'_>, eflags: i32, snapshot: pg_sys::Snapshot) {
        // SAFETY: PortalStart wires up the QueryDesc + ExecutorStart.
        unsafe { pg_sys::PortalStart(self.raw, params.as_ptr(), eflags, snapshot) };
    }

    /// Execute the portal, writing results to `dest`.
    /// Returns the executor's bool (false → more rows pending,
    /// only relevant for cursor portals which we don't use yet).
    ///
    /// `count = 0` means "all rows".
    ///
    /// # Safety
    ///
    /// Caller must have called [`Self::define`] + [`Self::start`].
    pub unsafe fn run(
        &self,
        count: i64,
        is_top_level: bool,
        dest: *mut pg_sys::DestReceiver,
        altdest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) -> bool {
        // SAFETY: PortalRun is the executor entry point. Raises
        // ERROR on plan failure; with_xact's catch_unwind handles
        // it.
        unsafe { pg_sys::PortalRun(self.raw, count, is_top_level, dest, altdest, qc) }
    }

    /// Borrow the raw `pg_sys::Portal`.
    pub fn as_ptr(&self) -> pg_sys::Portal {
        self.raw
    }
}

impl Drop for Portal {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches CreatePortal. PortalDrop is the
            // documented teardown; releases executor state and
            // frees the portal's MemoryContext.
            unsafe { pg_sys::PortalDrop(self.raw, false) };
            self.raw = std::ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// ScopedMemoryContext — RAII wrapper around AllocSetContextCreate*
// ---------------------------------------------------------------------------

/// Owned `pg_sys::MemoryContext`. Drops via `MemoryContextDelete`.
///
/// Used by the simple-query direct path (see
/// [deferred/simple-query-direct-path.md §4.1](../../../../docs/design/deferred/simple-query-direct-path.md#41-parse_and_keep))
/// to keep a raw parsetree alive across multiple per-statement
/// portals, and per-statement to confine analyze + plan
/// allocations so they don't leak into the parse context.
///
/// `!Send` + `!Sync` via `PhantomData<*const ()>`. PG memory
/// contexts are per-backend and not safe to cross threads.
pub struct ScopedMemoryContext {
    raw: pg_sys::MemoryContext,
    _phantom: PhantomData<*const ()>,
}

impl ScopedMemoryContext {
    /// Create a fresh `AllocSetContext` as a child of `parent`.
    /// Uses the default min/initial/max sizes
    /// (`ALLOCSET_DEFAULT_*`).
    ///
    /// `name` is shown in `MemoryContextStats` output / EXPLAIN
    /// memory accounting; pick a stable static string per call
    /// site.
    pub fn new_child(parent: pg_sys::MemoryContext, name: &'static CStr) -> Self {
        // SAFETY: AllocSetContextCreateInternal is the documented
        // entry point; raises on alloc failure which longjmps
        // through us.
        let raw = unsafe {
            pg_sys::AllocSetContextCreateInternal(
                parent,
                name.as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as pg_sys::Size,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as pg_sys::Size,
            )
        };
        ScopedMemoryContext {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Create as a child of the current `CurrentMemoryContext`.
    /// Convenience wrapper for the common case.
    pub fn new(name: &'static CStr) -> Self {
        // SAFETY: CurrentMemoryContext is a TLS-like global.
        let parent = unsafe { pg_sys::CurrentMemoryContext };
        Self::new_child(parent, name)
    }

    /// Create as a child of `parent`'s underlying context.
    pub fn new_child_of(parent: &ScopedMemoryContext, name: &'static CStr) -> Self {
        Self::new_child(parent.as_ptr(), name)
    }

    /// Borrow the raw pointer for FFI pass-through.
    pub fn as_ptr(&self) -> pg_sys::MemoryContext {
        self.raw
    }

    /// Switch `CurrentMemoryContext` to this context for the
    /// returned guard's lifetime. Dropping the guard restores the
    /// previous `CurrentMemoryContext`.
    ///
    /// **The returned guard must outlive any pallocs you want
    /// routed into this context.** Use `let _guard = ...` (NOT
    /// `let _ = ...`, which drops immediately).
    pub fn switch_to(&self) -> MemoryContextGuard {
        // SAFETY: MemoryContextSwitchTo returns the previously
        // active context. We restore it on Drop.
        let prev = unsafe { pg_sys::MemoryContextSwitchTo(self.raw) };
        MemoryContextGuard {
            prev,
            _phantom: PhantomData,
        }
    }
}

impl Drop for ScopedMemoryContext {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches AllocSetContextCreateInternal. If
            // the caller forgot to drop a switch_to guard first
            // and CurrentMemoryContext is still pointing here,
            // MemoryContextDelete will refuse via Assert
            // (ERRORCODE_OUT_OF_MEMORY-ish) — caller bug.
            unsafe { pg_sys::MemoryContextDelete(self.raw) };
            self.raw = std::ptr::null_mut();
        }
    }
}

/// RAII guard returned by [`ScopedMemoryContext::switch_to`].
/// Restores the previously-active `CurrentMemoryContext` on
/// `Drop`. `!Send` + `!Sync` via `PhantomData<*const ()>`.
pub struct MemoryContextGuard {
    prev: pg_sys::MemoryContext,
    _phantom: PhantomData<*const ()>,
}

impl Drop for MemoryContextGuard {
    fn drop(&mut self) {
        // SAFETY: prev is whatever MemoryContextSwitchTo returned
        // when we activated; restoring it is symmetric.
        unsafe { pg_sys::MemoryContextSwitchTo(self.prev) };
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::pg_test;

    /// Validate the basic with_xact bracket: start, body runs,
    /// commit. Catches link failures of Start/Push/Pop/Commit and
    /// obvious bracket-state corruption (a later FFI call would
    /// crash).
    #[pg_test]
    fn pg_with_xact_basic_lifecycle() {
        with_xact(true, |_ctx| -> PgWireResult<i32> { Ok(42) }).expect("with_xact body failed");
    }

    /// `needs_snapshot = false` mirrors vanilla's path for
    /// utility / xact-control statements: `with_xact` skips
    /// the `PushActiveSnapshot(GetTransactionSnapshot())`
    /// prologue. We exercise this end-to-end via the e2e
    /// regression `aborted_block_direct_backend_wire_rollback_succeeds`
    /// (which `with_xact(false, ...)` enters from `TBLOCK_ABORT`);
    /// the in-PG smoke here just confirms the call shape compiles
    /// + completes (the pgrx test framework wraps the whole test
    /// in an outer xact that already has an active snapshot, so
    /// asserting `!ActiveSnapshotSet()` would only check the
    /// framework, not `with_xact`).
    #[pg_test]
    fn pg_with_xact_no_snapshot_call_shape() {
        with_xact(false, |_ctx| -> PgWireResult<()> { Ok(()) })
            .expect("with_xact(false, ...) body failed");
    }

    /// Validate the Portal create/drop lifecycle in isolation. No
    /// DefineQuery → PortalDrop should still clean up the empty
    /// portal.
    #[pg_test]
    fn pg_portal_create_drop() {
        with_xact(true, |ctx| -> PgWireResult<()> {
            // SAFETY: with_xact established the xact + memcontext.
            let portal = unsafe { Portal::create_anonymous(ctx) };
            assert!(!portal.as_ptr().is_null(), "CreatePortal returned null");
            Ok(())
        })
        .expect("with_xact failed");
    }

    /// End-to-end smoke test: parse `SELECT 1`, build a cached
    /// plan source against a fresh AllocSet context, get a plan,
    /// create a portal, define + start + run with `DestNone`,
    /// drop everything in order. Exercises the entire FFI chain
    /// the Stage A direct executor will use — except the
    /// Tuplestore destination, which is exercised separately by
    /// `pg_tuplestore_receiver_lifecycle`.
    ///
    /// Key correctness pin: `query_ctx` must be a fresh context,
    /// not `CurrentMemoryContext`. PG's `DropCachedPlan` later
    /// `MemoryContextDelete`s the query_ctx; if it were the
    /// xact's own context, that would UAF on commit.
    #[pg_test]
    fn pg_direct_path_smoke() {
        use std::ffi::CString;

        with_xact(true, |ctx| -> PgWireResult<()> {
            let sql = CString::new("SELECT 1").unwrap();

            // 1. Parse.
            let raw_list = unsafe { pg_sys::pg_parse_query(sql.as_ptr()) };
            assert!(!raw_list.is_null(), "pg_parse_query returned null");
            let raw_stmt = unsafe { (*(*raw_list).elements).ptr_value as *mut pg_sys::RawStmt };
            assert!(!raw_stmt.is_null(), "first RawStmt is null");

            // 2. Fresh AllocSet for analyze output. PG reparents
            // this context into the CachedPlanSource at
            // CompleteCachedPlan time, then DropCachedPlan
            // MemoryContextDelete's it. Must NOT be the xact's
            // own context.
            let query_ctx = unsafe {
                pg_sys::AllocSetContextCreateInternal(
                    pg_sys::CurrentMemoryContext,
                    c"pgxport_query_context".as_ptr(),
                    pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                    pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
                )
            };
            let prev_ctx = unsafe { pg_sys::MemoryContextSwitchTo(query_ctx) };
            let mut types_ptr: *mut pg_sys::Oid = std::ptr::null_mut();
            let mut n_params: std::ffi::c_int = 0;
            let querytrees = unsafe {
                pg_sys::pg_analyze_and_rewrite_varparams(
                    raw_stmt,
                    sql.as_ptr(),
                    &mut types_ptr,
                    &mut n_params,
                    std::ptr::null_mut(),
                )
            };
            unsafe { pg_sys::MemoryContextSwitchTo(prev_ctx) };
            assert!(!querytrees.is_null(), "analyze returned null");

            // 3. CachedPlanSource: create + complete + save.
            let source = unsafe {
                CachedPlanSource::create(ctx, raw_stmt, &sql, pg_sys::CommandTag::CMDTAG_SELECT)
            };
            unsafe {
                source.complete(querytrees, query_ctx, &[], 0, true);
                source.save();
            }

            // 4. Get a per-execute plan.
            let params = ParamList::empty();
            let plan = unsafe { source.get_plan(&params) };
            assert!(!plan.as_ptr().is_null(), "GetCachedPlan returned null");

            // 5. Portal: create + define + start + run with a
            // DestNone receiver. cached_plan=NULL on define
            // because our [`CachedPlan`] RAII owns the refcount.
            let portal = unsafe { Portal::create_anonymous(ctx) };
            unsafe {
                portal.define(
                    &sql,
                    pg_sys::CommandTag::CMDTAG_SELECT,
                    plan.stmt_list(),
                    std::ptr::null_mut(),
                );
                portal.start(&params, 0, pg_sys::GetActiveSnapshot());
            }
            let dest = unsafe { pg_sys::CreateDestReceiver(pg_sys::CommandDest::DestNone) };
            let mut qc: pg_sys::QueryCompletion = unsafe { std::mem::zeroed() };
            let _done = unsafe { portal.run(0, true, dest, dest, &mut qc) };

            // 6. Drop in order: portal, plan, source. (Implicit
            // via Rust drop order; explicit here to document the
            // PG-side lifecycle dependency.)
            drop(portal);
            drop(plan);
            drop(source);

            Ok(())
        })
        .expect("direct-path smoke test failed");
    }
}
