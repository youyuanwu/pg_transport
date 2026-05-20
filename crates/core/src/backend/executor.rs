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
//!     Portal::create_anonymous + define + start + run
//!         ↓ executor writes into TuplestoreReceiver
//!     iterate tuplestore → wire-encode → DataRow
//!     // drop order: Portal first, then CachedPlan
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
use std::ffi::{CStr, CString};
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
    fn new() -> Self {
        XactCtx {
            _phantom: PhantomData,
        }
    }
}

/// Run `body` inside `StartTransactionCommand` + `PushActiveSnapshot`
/// ... `PopActiveSnapshot` + `CommitTransactionCommand`. No
/// `SPI_connect` (that's [`super::spi::with_spi`]'s job).
///
/// Error path:
/// - Body returns `Ok(value)` → `Pop` + `Commit`, return value.
/// - Body returns `Err(e)` → `Pop` + `Commit` (xact state intact;
///   we just propagate the typed error). Mirrors `with_spi`.
/// - Body panics (PG ERROR longjmp'd to Rust panic) →
///   `AbortCurrentTransaction` (handles snapshot stack itself; we
///   must NOT `Pop` after Abort), convert payload to `PgWireError`.
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
pub fn with_xact<T, F>(body: F) -> PgWireResult<T>
where
    F: FnOnce(&XactCtx) -> PgWireResult<T>,
{
    // SAFETY: Start/Push are PG server-API entry points safe to
    // call from a bgworker. Same shape as with_spi's prologue.
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome: Result<PgWireResult<T>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| {
            let ctx = XactCtx::new();
            body(&ctx)
        }));

    match outcome {
        Ok(Ok(value)) => {
            // SAFETY: matched Start/Push above; closure returned
            // cleanly. Pop + Commit drains the snapshot stack and
            // closes the xact.
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Ok(value)
        }
        Ok(Err(err)) => {
            // Body returned a typed PgWireError without raising a
            // PG ERROR. Xact state intact; Pop + Commit (not Abort).
            unsafe {
                pg_sys::PopActiveSnapshot();
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
    /// Must be called inside [`with_xact`]. `params` may be null
    /// for parameterless queries. The current process's
    /// `CurrentResourceOwner` is used as the holding owner.
    pub unsafe fn get_plan(&self, params: pg_sys::ParamListInfo) -> CachedPlan {
        // SAFETY: GetCachedPlan is the documented entry point;
        // raises ERROR on failure which longjmps out.
        let owner = unsafe { pg_sys::CurrentResourceOwner };
        let raw = unsafe { pg_sys::GetCachedPlan(self.raw, params, owner, std::ptr::null_mut()) };
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
pub struct CachedPlan {
    raw: *mut pg_sys::CachedPlan,
    /// The `ResourceOwner` that was current at `GetCachedPlan` time.
    /// We must release against the same owner — PG records the
    /// refcount-holder identity here.
    owner: pg_sys::ResourceOwner,
    _phantom: PhantomData<*const ()>,
}

impl CachedPlan {
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

impl Drop for CachedPlan {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            // SAFETY: matches GetCachedPlan with the same owner.
            unsafe { pg_sys::ReleaseCachedPlan(self.raw, self.owner) };
            self.raw = std::ptr::null_mut();
        }
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
    /// # Safety
    ///
    /// Must be called inside [`with_xact`].
    pub unsafe fn create_anonymous(_ctx: &XactCtx) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let name = CString::new(format!("pgxport_{id}")).expect("portal name has no NUL");
        // SAFETY: CreatePortal(name, allowDup=false, dupSilent=true).
        // dupSilent doesn't matter when allowDup=false (collision
        // raises ERROR which longjmps out).
        let raw = unsafe { pg_sys::CreatePortal(name.as_ptr(), false, true) };
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
    /// # Safety
    ///
    /// Caller must have already called [`Self::define`].
    pub unsafe fn start(
        &self,
        params: pg_sys::ParamListInfo,
        eflags: i32,
        snapshot: pg_sys::Snapshot,
    ) {
        // SAFETY: PortalStart wires up the QueryDesc + ExecutorStart.
        unsafe { pg_sys::PortalStart(self.raw, params, eflags, snapshot) };
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
        with_xact(|_ctx| -> PgWireResult<i32> { Ok(42) }).expect("with_xact body failed");
    }

    /// Validate the Portal create/drop lifecycle in isolation. No
    /// DefineQuery → PortalDrop should still clean up the empty
    /// portal.
    #[pg_test]
    fn pg_portal_create_drop() {
        with_xact(|ctx| -> PgWireResult<()> {
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

        with_xact(|ctx| -> PgWireResult<()> {
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
            let plan = unsafe { source.get_plan(std::ptr::null_mut()) };
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
                portal.start(std::ptr::null_mut(), 0, pg_sys::GetActiveSnapshot());
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
