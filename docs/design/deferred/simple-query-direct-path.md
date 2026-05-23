# Simple-query direct path — deferred design

> Parent: [../README.md](../README.md) · Strategic parent: [planner-executor-direct-path.md](planner-executor-direct-path.md)
> Siblings: [../backend-wire.md](../backend-wire.md) §6 (SPI bridge) · [../performance.md](../performance.md) §2
>
> **Status: not yet implemented; most building blocks already in
> tree.** Today every `'Q'` message routes through
> [`spi_bridge.rs`](../../../crates/core/src/backend/spi_bridge.rs)
> → `with_spi` → `SPI_execute` → `SPI_tuptable` materialisation →
> `Vec<DataRow>` copy. This document scopes the work to bring the
> same `Portal*` + `WireDestReceiver` pattern that
> [`extended/direct.rs`](../../../crates/core/src/backend/extended/direct.rs)
> uses for `'P'`/`'B'`/`'E'` to the simple-query side.
>
> Most of the executor-side plumbing the parent doc described as
> "Stage C work" (rename `spi.rs` → `executor.rs`, `with_spi` →
> `with_xact`, `SpiPlan` → `CachedPlan`) already shipped: see
> [`crates/core/src/backend/executor.rs`](../../../crates/core/src/backend/executor.rs)
> for the live `with_xact`, `XactCtx`, `Portal` (`create_anonymous`
> /`define`/`start`/`run`/Drop), `CachedPlanSource`, `CachedPlan`,
> `ParamList`, `TupleDescRef`. The remaining work is a small new
> module plus three private items lifted from `extended/direct.rs`
> to a shared location (see §7).
>
> This is the concrete Stage-B work from
> [planner-executor-direct-path.md §7](planner-executor-direct-path.md#7-phased-adoption-if-and-when-we-un-defer);
> the parent doc's Stage B sketch was written before
> `WireDestReceiver` landed for extended-query and before
> `executor.rs` factored out the xact/portal wrappers. The shipped
> design will use the custom `DestReceiver` from day one and
> reuse `executor.rs` end-to-end.

## 1. Why now / why not

**Why now.** Latest pgbench numbers
([performance.md §2](../performance.md#2-latest-stable-numbers-2026-05-21))
show pg_transport at **0.94×–0.96×** of vanilla PG on `pgbench
select` and `pgbench nupdate`. The custom extended-query bench
shows **1.11×–1.14×**. The gap is entirely the simple-query
path's extra cost over PG's `exec_simple_query`:

1. **Double parse** — `parse_and_classify` calls `raw_parser`; then
   `SPI_execute` re-parses the same string inside SPI. ~5 µs per
   query (Q25 measurement).
2. **`SPI_tuptable` materialisation** — every tuple is heap-tuple-
   copied into `SPI_tuptable` before we walk it. Vanilla PG's
   `printtup` writes wire bytes directly during the executor scan.
3. **`Vec<DataRow>` materialisation** — we then copy `SPI_tuptable`
   into a Rust-side `Vec<DataRow>` because pgwire's
   `Response::Query(QueryResponse::new(schema, row_stream))`
   consumes rows *after* the handler returns, and `SPI_finish`
   would have torn down the tuples by then.

Items 1+2 are pure overhead vs vanilla PG; item 3 is forced by the
pgwire `Response` API (this doc retains it; see §6.2).

**Why not until now.** The parent doc's Q18 deferred this until
"bench shows SPI overhead is material". Phase-5 bench numbers were
within ±5 pp of vanilla so the trigger never fired. Phase-9
extended-query work added the direct backend for `Parse`/`Bind`/
`Execute`; pgbench (`-M simple`) doesn't exercise that path, so the
simple-query gap stayed visible. With the per-accept handoff spawn
landed (commits `aca1df3`/`9abd857`/`5d11292`), startup overhead is
no longer dominating, and the per-query 4–6 pp gap is the only
remaining sub-vanilla measurement.

## 2. Current simple-query data path

[`spi_bridge.rs::execute_simple_query`](../../../crates/core/src/backend/spi_bridge.rs):

```text
'Q' message → SimpleQueryHandler (pgwire) → execute_simple_query(query_string)
                                              │
                                              ▼ parse_and_classify(query)
                              ┌──────────────────────────────────────┐
                              │ raw_parser → List *RawStmt           │ (parse pass #1)
                              │ extract (slice, classification)      │
                              │ delete scratch MemoryContext         │
                              └──────────────────────────────────────┘
                                              │ Vec<(&str, Option<XactCmd>)>
                                              ▼
                              ┌──────────────────────────────────────┐
                              │ for each statement:                  │
                              │   xact?  → handle_xact_control(cmd)  │
                              │   else  → run_spi_statement(text)    │
                              │            └→ with_spi(...):         │
                              │               SPI_execute(text)      │ (parse pass #2 + plan + exec)
                              │               iterate SPI_tuptable   │
                              │               copy → Vec<DataRow>    │
                              │               SPI_finish             │
                              └──────────────────────────────────────┘
                                              │
                                              ▼ Vec<Response>
                                  pgwire encodes Response → wire bytes
```

The xact-control intercept exists because SPI in atomic mode rejects
`BEGIN`/`COMMIT`/`ROLLBACK` with `SPI_ERROR_TRANSACTION`. PG's own
`exec_simple_query` doesn't need an intercept — `PortalRun` routes
`TransactionStmt` through `ProcessUtility` → `BeginTransactionBlock`
naturally. **The direct path inherits PG's behaviour for free**:
once we use `Portal*` directly, the intercept goes away. See §4.3.

## 3. Target simple-query data path (direct)

```text
'Q' message → SimpleQueryHandler → execute_simple_query_direct(query)
                                              │
                                              ▼ parse_and_keep(query)
                              ┌──────────────────────────────────────┐
                              │ raw_parser → List *RawStmt           │ (parse pass #1)
                              │ keep parsetree alive in a            │
                              │ per-call MemoryContext               │
                              │ no classification needed             │
                              └──────────────────────────────────────┘
                                              │ Vec<(&str, *mut RawStmt)>
                                              ▼
                              ┌──────────────────────────────────────┐
                              │ for each statement:                  │
                              │   with_xact(|| run_one_direct(...))  │
                              │    └→ pg_analyze_and_rewrite_fixed   │ (no second parse)
                              │       pg_plan_queries                │
                              │       CreatePortal                   │
                              │       PortalDefineQuery              │
                              │       PortalStart                    │
                              │       PortalRun(WireDestReceiver)    │ ← rows encoded inline
                              │       PortalDrop                     │
                              │       extract schema + cmd tag       │
                              └──────────────────────────────────────┘
                                              │ Vec<Response>
                                              ▼
                                  pgwire encodes Response → wire bytes
```

Key differences from §2:

1. **One parse pass.** The `raw_parser` output is *kept* in a
   per-call MemoryContext and fed to `pg_analyze_and_rewrite_fixedparams`
   directly. No second parse.
2. **No SPI_tuptable.** `WireDestReceiver::receiveSlot` writes each
   tuple's encoded bytes into a per-row `BytesMut` during executor
   scan. Tuples never enter SPI's tuptable buffer.
3. **No xact intercept.** `PortalRun` handles `TransactionStmt`
   via `ProcessUtility` → PG's xact-block API. The same is true
   for all utility statements (`SET`, `CREATE`, `VACUUM`, etc.),
   which today go through SPI but should go through `Portal*` for
   semantic parity with vanilla PG.
4. **Xact wrapper trims.** `with_spi` becomes `with_xact`: just
   `StartTransactionCommand` + `PushActiveSnapshot` (+ matching
   teardown), no `SPI_connect`/`SPI_finish` bracket.

## 4. Function shapes

### 4.1 `parse_and_keep`

Replaces [`parse_and_classify`](../../../crates/core/src/backend/spi_bridge.rs)
for the direct path. Returns owned parsetree pointers and the
context they live in — caller is responsible for deleting the
context once all statements have run.

```rust
struct ParsedQuery<'q> {
    /// Per-statement (text-slice, raw RawStmt pointer) pairs in
    /// source order.
    statements: Vec<(&'q str, *mut pg_sys::RawStmt)>,
    /// MemoryContext that owns every RawStmt in `statements`.
    /// Drop the parsed query to delete it.
    parse_ctx: ScopedMemoryContext,
}

fn parse_and_keep(query: &str) -> PgWireResult<ParsedQuery<'_>>;
```

Implementation: same `PgTryBuilder`-wrapped `raw_parser` call as
[`parse_and_classify`](../../../crates/core/src/backend/spi_bridge.rs#L153),
but the scratch `MemoryContext` is moved into an RAII wrapper
(`ScopedMemoryContext`, dropping calls `MemoryContextDelete`) and
the classification step is dropped. The `(&str, *mut RawStmt)`
pairs are taken from each `RawStmt.stmt_location/stmt_len` after
the parse, same as today.

`ScopedMemoryContext` doesn't exist yet — it's the only new RAII
helper this work needs (~20 LoC; trivial Drop impl around
`AllocSetContextCreateInternal` / `MemoryContextDelete`). Lives in
[`executor.rs`](../../../crates/core/src/backend/executor.rs)
alongside `with_xact`.

**Memory-context invariant.** `ParsedQuery.parse_ctx` is the
*parent* context of the per-statement analyze/plan working
contexts — those are children created during `run_one_direct` and
deleted when each statement's `PortalDrop` runs. The parsetree
itself stays alive in `parse_ctx` until `ParsedQuery` drops.

### 4.2 `with_xact` (already shipped)

Already in tree at
[`crates/core/src/backend/executor.rs::with_xact`](../../../crates/core/src/backend/executor.rs)
and used by `extended/direct.rs`. Body signature:

```rust
pub fn with_xact<T, F>(body: F) -> PgWireResult<T>
where F: FnOnce(&XactCtx) -> PgWireResult<T>;
```

Opens `StartTransactionCommand` + `PushActiveSnapshot`, runs the
closure, closes on success or aborts on PG ERROR caught via
`PgTryBuilder`. The `XactCtx` token gates downstream calls
(`Portal::create_anonymous`, etc.) that require an open xact.
Reused as-is; no changes needed.

### 4.3 `run_one_direct`

The new per-statement worker. Mirrors what `exec_simple_query`
does in vanilla PG (analyze → plan → portal define/start/run/drop)
and reuses the existing
[`Portal`](../../../crates/core/src/backend/executor.rs)
wrapper. Note: we deliberately **don't** go through
`CachedPlanSource` — simple-query plans are one-shot, so a direct
`pg_plan_queries` matches `exec_simple_query` and avoids the cache
bookkeeping. The `Portal::define` wrapper accepts a NULL
`cached_plan`, which is exactly what PG's `exec_simple_query`
passes.

```rust
fn run_one_direct(
    ctx: &XactCtx,
    parse_ctx: &ScopedMemoryContext,   // owns raw_stmt; analyze/plan ctx is its child
    raw_stmt: *mut pg_sys::RawStmt,
    _sql: &str,
    sql_cstr: &CStr,
) -> PgWireResult<Response> {
    // 1. Command tag from the raw parsetree (matches PG's
    //    exec_simple_query, which calls CreateCommandTag on the
    //    raw stmt before analyze).
    // SAFETY: CreateCommandTag is a pure walk of the parsetree;
    // raw_stmt is alive in parse_ctx for the whole call.
    let command_tag = unsafe { pg_sys::CreateCommandTag((*raw_stmt).stmt) };

    // 2. Per-statement working context. Child of parse_ctx so
    //    parsetree lookups still resolve, but everything
    //    pg_analyze_and_rewrite_fixedparams + pg_plan_queries
    //    palloc into here is freed at end-of-statement.
    //    PortalDefineQuery copies what it needs onto the portal's
    //    own context, so we can drop this *after* PortalStart.
    let stmt_ctx = ScopedMemoryContext::child_of(parse_ctx, c"pg_transport_stmt");
    let _guard = stmt_ctx.switch_to(); // restores prior CurrentMemoryContext on drop

    let (schema, encoders, qc, data_rows) = unsafe {
        // 3. Analyze + rewrite (fixed params; simple-query has none).
        let query_list = pg_sys::pg_analyze_and_rewrite_fixedparams(
            raw_stmt,
            sql_cstr.as_ptr(),
            std::ptr::null(),       // paramTypes
            0,                       // numParams
            std::ptr::null_mut(),   // queryEnv
        );

        // 4. Plan. Matches PG's exec_simple_query call shape.
        let plan_list = pg_sys::pg_plan_queries(
            query_list,
            sql_cstr.as_ptr(),
            pg_sys::CURSOR_OPT_PARALLEL_OK as i32,
            std::ptr::null_mut(),   // boundParams
        );

        // 5. Portal lifecycle via the existing wrapper. Portal's
        //    own MemoryContext is independent of stmt_ctx.
        let portal = Portal::create_anonymous(ctx);
        portal.define(sql_cstr, command_tag, plan_list, std::ptr::null_mut());

        // 6. Build schema + per-column encoders from the portal's
        //    tupDesc (NULL for utility / no-tuple statements).
        let tupdesc = TupleDescRef::from_raw((*portal.as_ptr()).tupDesc);
        let (schema, encoders) = match tupdesc {
            Some(td) => build_schema_and_encoders(&td),
            None => (Vec::new(), Vec::new()),
        };

        // 7. Start. with_xact has already PushActiveSnapshot'd;
        //    PortalStart records it on the QueryDesc. PortalRun
        //    will push its own active snapshot internally for
        //    SELECT; nested push/pop is fine — matches what the
        //    extended-query direct backend already does.
        portal.start(&ParamList::empty(), 0, pg_sys::GetActiveSnapshot());

        // 8. Execute. Per-row encoding happens inside receiveSlot,
        //    which appends to `data_rows`.
        let mut data_rows: Vec<DataRow> = Vec::new();
        let ncols = schema.len() as i16;
        let mut dest = WireDestReceiver::new(&encoders, &mut data_rows, ncols);
        let mut qc: pg_sys::QueryCompletion = std::mem::zeroed();
        pg_sys::InitializeQueryCompletion(&mut qc);
        let _completed = portal.run(
            0,                                 // count = 0 => FETCH_ALL
            true,                              // is_top_level
            dest.as_dest_receiver(),
            dest.as_dest_receiver(),
            &mut qc,
        );

        // 9. portal drops here (PortalDrop on scope exit), tearing
        //    down executor state and freeing the portal's
        //    MemoryContext.
        (schema, encoders, qc, data_rows)
    };
    drop(encoders); // no longer referenced; explicit for clarity
    drop(_guard);   // switch back to parse_ctx
    drop(stmt_ctx); // deletes the per-stmt analyze/plan context

    // 10. Shape into pgwire Response. Same construction pattern as
    //     extended/direct.rs::execute (see lines ~210-228).
    if schema.is_empty() {
        let tag_name = command_tag_name(qc.commandTag);
        let mut tag = Tag::new(&tag_name);
        if unsafe { pg_sys::command_tag_display_rowcount(qc.commandTag) } {
            tag = tag.with_rows(qc.nprocessed as usize);
        }
        Ok(Response::Execution(tag))
    } else {
        let tag_name = command_tag_name(qc.commandTag);
        let schema_arc = Arc::new(schema);
        let row_stream = stream::iter(data_rows).map(Ok);
        let mut response = QueryResponse::new(schema_arc, row_stream);
        response.set_command_tag(&tag_name);
        Ok(Response::Query(response))
    }
}
```

**Per-statement working context (step 2).** Analyze + plan
`palloc` into `CurrentMemoryContext`. Without the child-context
switch, every per-statement allocation would survive in
`parse_ctx` until the whole `'Q'` message finishes — a leak
linear in statement count. The `stmt_ctx` switch matches what
`exec_simple_query` does via `MessageContext` + per-statement
resets in vanilla PG.

**Why `count = 0`.** The `Portal::run` wrapper inherits PG 18's
signature; `count = 0` means "all rows". `exec_simple_query`
passes `FETCH_ALL` which expands to the same value.

**Helpers used.** `command_tag_name`, `Tag`, `QueryResponse`,
`Response::Execution`/`Response::Query`, `stream::iter(..).map(Ok)`,
`Arc::new(schema)`, `command_tag_display_rowcount` — all already
used at [extended/direct.rs ~L210-228](../../../crates/core/src/backend/extended/direct.rs).
The `Response`-construction block in this sketch is a literal
lift of those lines. `command_tag_name` and `as_dest_receiver`
move from `extended/direct.rs` to the shared `dest_receiver.rs`
as part of the §7 refactor.

**PG 18 signature note.** PG 16 removed the `run_once` argument
from `PortalRun`; the live `Portal::run` wrapper matches PG 18
and takes `(count, is_top_level, dest, altdest, qc)`. Earlier
revisions of this doc included `run_once` based on the PG 14/15
signature; that's stale.

**Command-tag plumbing.** `command_tag_for(raw_stmt)` from earlier
revisions of this doc isn't a separate helper; `pg_sys::CreateCommandTag`
already returns the right enum and `extended/direct.rs` calls it
exactly the same way.

### 4.3.1 Building schema + encoders from `tupDesc`

The schema/encoder construction (step 5 above) duplicates work
that `extended/direct.rs` already does at `prepare` time from
`CachedPlanSource::result_desc()`. The refactor in §7 lifts this
into a shared helper `build_schema_and_encoders(&TupleDescRef)` so
both sites call into the same code.

### 4.4 `WireDestReceiver` reuse

`extended/direct.rs` already defines a `WireDestReceiver` that
walks `TupleTableSlot` columns and writes encoded bytes to a
`BytesMut`. Two options:

1. **Lift it to a shared module** (`crates/core/src/backend/dest_receiver.rs`)
   used by both the simple-query and extended-query paths. Same
   `#[repr(C)]` struct, same `receiveSlot`/`rStartup`/`rShutdown`
   callbacks, parameterised on the per-column encoder cache.
   **Recommended.**
2. **Duplicate** the struct in a new `simple_direct.rs` module.
   Faster to land but creates two `DestReceiver`s to maintain
   through PG version changes.

Option 1 is the right move; the receiver's API (set encoders +
target buffer, then call as `*mut DestReceiver`) is already
agnostic about who invokes `PortalRun`.

### 4.5 `execute_simple_query_direct`

Top-level entry, mirroring [`execute_simple_query`](../../../crates/core/src/backend/spi_bridge.rs#L65):

```rust
pub fn execute_simple_query_direct(query: &str) -> PgWireResult<Vec<Response>> {
    let parsed = parse_and_keep(query)?;
    let sql_cstr = CString::new(query).map_err(|_| {
        generic_error("pg_transport", "query string contains a NUL byte")
    })?;
    let mut responses = Vec::with_capacity(parsed.statements.len());
    for (text, raw_stmt) in &parsed.statements {
        if text.trim().is_empty() {
            continue;
        }
        // with_xact gives us per-statement xact bracketing matching
        // PG's exec_simple_query (start_xact_command /
        // finish_xact_command). The XactCtx is the token threaded
        // through Portal::create_anonymous.
        //
        // Error shape matches today's spi_bridge::execute_simple_query:
        // first-error returns Err and pgwire emits ErrorResponse +
        // ReadyForQuery from the SimpleQueryHandler layer. We do
        // **not** push Response::Error into the Vec; that would
        // change framing relative to the SPI backend and break
        // dual-backend test parity.
        let resp = with_xact(|ctx| {
            run_one_direct(ctx, &parsed.parse_ctx, *raw_stmt, text, &sql_cstr)
        })?;  // ← 'Q' semantics: stop on first error, propagate Err
        responses.push(resp);
    }
    // parsed drops here; ScopedMemoryContext deletes the parse context.
    Ok(responses)
}
```

**Error-shape rationale.** `?`-propagation is identical to
today's [`execute_simple_query`](../../../crates/core/src/backend/spi_bridge.rs#L64);
pgwire-v3's `SimpleQueryHandler::do_query` returns
`PgWireResult<Vec<Response>>` and converts a top-level `Err`
into wire `ErrorResponse` + `ReadyForQuery('I'|'E')`. Embedding
`Response::Error` in the Vec would emit the same bytes for the
error payload itself, but would *also* push an extra
`ReadyForQuery` per response — different framing, observable to
the client.

## 5. Dispatch shape — `pg_transport.execution_backend` extended

The existing GUC's documentation
([configuration.md](../configuration.md#26))
says "Extended-query execution backend". This doc proposes
**extending its meaning to cover simple-query too**:

```rust
// crates/core/src/wire/pgwire_v3.rs (SimpleQueryHandler impl)
match guc::execution_backend() {
    ExecutionBackend::Spi    => spi_bridge::execute_simple_query(query),
    ExecutionBackend::Direct => spi_bridge::execute_simple_query_direct(query),
}
```

Default stays `'spi'` until the direct simple-query path soaks
(parallel to the parent doc's Stage A→C migration timeline). The
GUC docs need a small wording update: "Execution backend for
`Q`/`Parse`/`Execute` messages" instead of "Extended-query
execution backend".

**Alternative considered**: a separate
`pg_transport.simple_execution_backend` GUC. Rejected — operators
canarying one backend over the other will want both ends to move
together (avoids cross-protocol behaviour drift inside a single
session), and the two backends share the same `WireDestReceiver`
infrastructure.

## 6. Tricky bits

### 6.1 Memory-context lifecycle across statements

Today, each `with_spi` opens its own SPI memory context and tears
it down at `SPI_finish`. The direct path needs:

- **Parse context** (`ParsedQuery.parse_ctx`) — lives across all
  statements; owns the parsetree list.
- **Per-statement analyze/plan context** — created inside
  `run_one_direct` as a child of `parse_ctx`; deleted at function
  exit. Holds the `List *Query` and `List *PlannedStmt` (they're
  fed to `PortalDefineQuery` which `palloc`s them onto the
  portal's own context, so deleting the working context after
  `PortalStart` is safe).
- **Portal context** — owned by PG (`portal->portalContext`);
  `PortalDrop` releases it.

Invariant: when statement N+1 starts analyzing, statement N's
portal has been dropped but `parse_ctx` is still alive (so
statement N+1's `raw_stmt` pointer is still valid). When
`ParsedQuery` drops at the end, every per-statement context has
already been deleted by `PortalDrop`, and `parse_ctx` is the last
remaining context tied to the query.

**Snapshot push interaction.** `with_xact` calls
`PushActiveSnapshot(GetTransactionSnapshot())`. `PortalRun` for
SELECT internally does its own `PushActiveSnapshot` + matching
pop around the executor scan. The nested push/pop is fine —
PG's snapshot stack is designed for this (vanilla
`exec_simple_query` hits the same path); the live extended-query
direct backend already exercises it on every `'E'` message
without issue. The `PortalStart(snapshot=GetActiveSnapshot())`
call in step 7 of §4.3 records the with_xact snapshot on the
QueryDesc; PortalRun's internal push is per-scan and doesn't
replace it.

### 6.2 Why `Vec<DataRow>` materialisation can't go away (yet)

pgwire's `SimpleQueryHandler::do_query` signature returns
`PgWireResult<Vec<Response>>`. `Response::Query(QueryResponse)`
takes a `Stream<Item = PgWireResult<DataRow>>`. pgwire consumes
that stream **after** `do_query` returns to push the rows into the
wire codec — *after* `PortalDrop` has already run.

We cannot stream from a live `Portal`/`WireDestReceiver` past
`PortalDrop`. So `receiveSlot` writes encoded rows into a `BytesMut`,
we slice that into `DataRow` frames after `PortalDrop`, and return
`stream::iter(data_rows)` to pgwire.

We still **avoid the `SPI_tuptable` step**: `BytesMut` holds
already-encoded wire bytes (the per-row encoding work
`receiveSlot` did), not heap tuples. Going from "two copies"
(`SPI_tuptable` + `Vec<DataRow>`) to "one copy" (`BytesMut`
materialised by the receiver). True row-by-row streaming would
need pgwire-side API changes; out of scope for this work.

### 6.3 Multi-statement error semantics

Vanilla PG's `'Q'` semantics: on first error, abort the current
transaction and stop processing later statements in the same
message. `execute_simple_query` already implements this via
`responses.extend(execute_one_statement(...)?)` + `?`. The
direct version does the same via `?`-propagation in §4.5 — same
shape, same wire framing (one `ErrorResponse` + one
`ReadyForQuery`, not one per partial response).

Subtlety: `with_xact`'s abort path calls
`AbortCurrentTransaction` which collapses xact state to `DEFAULT`,
matching `spi_bridge`'s current behaviour. See the deviation note
in [`run_spi_statement`'s doc comment](../../../crates/core/src/backend/spi_bridge.rs#L106)
— same caveat applies here.

### 6.4 Utility statements

In current SPI path, `SET pg_transport.execution_backend = 'direct'`
inside a `'Q'` message goes through `SPI_execute("SET ...")`, which
routes utility statements via SPI's internal `ProcessUtility` call.
Works fine.

In the direct path, the same SQL would go through `PortalRun` →
which itself routes utility through `ProcessUtility`. Same effect,
same call site (literally the same PG function). The only
difference is one less indirection layer.

Edge case: utility statements that *change* xact state mid-query
(e.g. `BEGIN; SELECT 1;` in one `'Q'`). PG routes
`TransactionStmt` through `ProcessUtility` → `BeginTransactionBlock`
/ `EndTransactionBlock`, which mutate xact-block state (e.g.
`TBLOCK_STARTED` → `TBLOCK_INPROGRESS`). The bracketing
`StartTransactionCommand` / `CommitTransactionCommand` that
`with_xact` wraps each statement with handle the *command*
boundary on top of any open block: from
`TBLOCK_INPROGRESS`, `CommitTransactionCommand` issues
`CommandCounterIncrement` rather than an actual commit. Same
machinery vanilla `exec_simple_query` relies on. Tested by the
existing `xact_control_*` e2e tests
([crates/e2e/tests/basic.rs](../../../crates/e2e/tests/basic.rs));
these would need to pass against the direct backend too.

### 6.5 Cancel + interrupt handling

Today's SPI path has no special cancel plumbing — `SPI_execute`
calls `CHECK_FOR_INTERRUPTS()` internally during executor scan.
`PortalRun` does the same. No change needed for this work.
Cancel routing itself is still deferred per
[cancel-routing.md](cancel-routing.md).

### 6.6 The `with_spi` → `with_xact` split (already shipped)

The parent doc's Stage C is "rename `spi.rs` to `executor.rs`,
`with_spi` becomes `with_xact`". This has already happened: the
rename is in [`executor.rs`](../../../crates/core/src/backend/executor.rs)
and the live extended-query direct backend already uses `with_xact`.
`with_spi` still exists in [`spi.rs`](../../../crates/core/src/backend/spi.rs)
and continues to back the simple-query SPI path that this work
introduces an *alternative* to. **This doc does not propose
removing the SPI bridge.** Both backends ship side-by-side,
selectable via `pg_transport.execution_backend`; any future
deprecation decision is out of scope here.

## 7. Reuse map

| Component | Source | Reuse strategy |
|---|---|---|
| `with_xact` / `XactCtx` | `executor.rs` | ✅ Already shipped. Used as-is. |
| `Portal` (`create_anonymous`/`define`/`start`/`run`/Drop) | `executor.rs` | ✅ Already shipped. Used as-is; Drop calls `PortalDrop`. |
| `ParamList::empty()` | `executor.rs` | ✅ Already shipped. Used as-is (simple-query has no params). |
| `TupleDescRef` | `executor.rs` | ✅ Already shipped. Used as-is for schema construction. |
| `caught_error_to_pgwire` | `spi.rs` | ✅ Already shipped. Used as-is via `with_xact`. |
| `TypeOutput`/`TypeSend` (per-cell I/O) | `spi.rs` | ✅ Already shipped. Used as-is via `ColumnEncoder`. |
| `pg_transport.execution_backend` GUC | `guc.rs` | ✅ Already shipped. New call site in `SimpleQueryHandler` reads it. |
| `extract_statement_spans` | `spi_bridge.rs` | ✅ Already shipped. Reusable as-is by `parse_and_keep`. |
| `WireDestReceiver` (custom `DestReceiver`) | `extended/direct.rs` | ⚠ Lift to new `backend/dest_receiver.rs`, parameterise by `&[ColumnEncoder]` + `&mut Vec<DataRow>`. |
| `ColumnEncoder` enum + `for_column` | `extended/direct.rs` | ⚠ Lift to `dest_receiver.rs` alongside the receiver. |
| `wire_receive_slot` C-callback | `extended/direct.rs` | ⚠ Lift to `dest_receiver.rs`. |
| `WireDestReceiver::as_dest_receiver` | `extended/direct.rs` | ⚠ Lift with parent struct; bump visibility to `pub(crate)`. |
| `command_tag_name(CommandTag::Type) -> String` | `extended/direct.rs` | ⚠ Lift to `dest_receiver.rs` (used by both backends); bump to `pub(crate)`. |
| `generic_error(prefix, msg)` | `spi.rs` | ✅ Already shipped; reused for `CString::new` NUL conversion in `execute_simple_query_direct`. |
| `build_schema_and_encoders(&TupleDescRef)` | derived from `extended/direct.rs` body | ⚠ New shared helper (~40 LoC) in `dest_receiver.rs`. |
| `ScopedMemoryContext` RAII | — | 🆕 New (~20 LoC). Lives in `executor.rs`. |
| `parse_and_keep` | derived from `parse_and_classify` | 🆕 New (~50 LoC). Lives in `simple_direct.rs`. |
| `run_one_direct` | — | 🆕 New (~80 LoC). Lives in `simple_direct.rs`. |
| `execute_simple_query_direct` | — | 🆕 New (~30 LoC). Lives in `simple_direct.rs`. |
| GUC dispatch at `SimpleQueryHandler` | — | 🆕 New (~5 LoC). `wire/pgwire_v3.rs`. |

**New code estimate:** ~150–200 LoC (the simple_direct.rs module +
`ScopedMemoryContext` + tests). **Lifted/refactored:** ~100 LoC
moved from `extended/direct.rs` to `dest_receiver.rs`
(`WireDestReceiver` + `as_dest_receiver` + `ColumnEncoder` +
`wire_receive_slot` + `command_tag_name` +
`build_schema_and_encoders`).

**SPI bridge is *not* removed by this work.** Both backends ship
side-by-side; `execute_simple_query` (SPI) remains the default
route and stays maintained. Any future deprecation of the SPI
bridge is out of scope and would need its own design pass.

## 8. Test plan

### 8.1 Black-box: existing e2e tests against both backends

[crates/e2e/tests/basic.rs](../../../crates/e2e/tests/basic.rs)
already drives 31 scenarios via `psql` + tokio-postgres. The
suite covers simple-query and extended-query paths against a real
TCP socket. Plan:

1. Add a test-helper that runs the entire suite twice: once with
   default GUC (`spi`) and once with `SET pg_transport.execution_backend
   = 'direct'` injected at session start.
2. Both runs must pass. Tests that legitimately diverge (none
   expected) get `#[cfg(...)]` gates with a documented reason.

### 8.2 Unit tests in `simple_direct.rs`

Mirror the existing `parse_and_classify` tests
([crates/core/src/backend/spi_bridge.rs](../../../crates/core/src/backend/spi_bridge.rs#L475)):

- Empty / whitespace-only / comment-only query → no statements.
- Multi-statement split: `SELECT 1; SELECT 2` → 2 statements with
  correct text slices.
- Syntax error: `SELECTT 1` → `PgWireError::ParserError`.
- Utility statement: `SET pg_transport.execution_backend = 'spi'`
  → executes via `PortalRun` → `ProcessUtility`, no row output,
  `OK` command tag.
- Xact-control: `BEGIN; SELECT 1; COMMIT` → 3 statements, all via
  `PortalRun`, no special xact intercept needed.

### 8.3 Performance baseline

Run `just pgbench pg18 select 15 8` with `execution_backend=direct`
and verify the gap to vanilla closes from 0.94× to within ±2 pp.
Expected outcome: 0.97×–1.00× on `select`, ≥ 1.00× on `nupdate`,
unchanged or slightly better on `tpcb`. Custom bench numbers
unchanged.

### 8.4 Memory-leak soak

A `pgbench select` run for 5 minutes should show flat RSS on the
slot bgworker process. Any leak in the per-statement context
plumbing surfaces here.

## 9. Migration / rollout

1. Land `simple_direct.rs` + GUC dispatch + tests. Default GUC
   stays `spi`. Soak window starts.
2. Operators canary `direct` per session / per role; report bugs.
3. After soak passes, flip default to `direct`.

Both backends remain available indefinitely after step 3 —
this doc explicitly does **not** propose removing the SPI bridge.
Operators who hit a direct-path regression can `SET
pg_transport.execution_backend = 'spi'` per session/role to fall
back. Whether/when to deprecate SPI is a separate decision out
of scope here.

Mirrors the rollout shape sketched in
[planner-executor-direct-path.md §7.5](planner-executor-direct-path.md#75-coexistence-during-migration--runtime-guc),
minus that doc's Stage-C removal step.

## 10. Expected perf impact

Decomposition of the 4–6 pp gap on `pgbench select` against vanilla
PG (per the §1 analysis):

| Removed overhead | Estimated pp gain on `pgbench select` |
|---|---|
| Second parse pass (today's `SPI_execute` re-parses) | ~1.5 pp |
| `SPI_tuptable` heap-tuple materialisation | ~1.5 pp |
| `SPI_connect`/`SPI_finish` bracket overhead | <1 pp |
| Xact-intercept branch removed | <0.5 pp |

Sum: ~4 pp. Closes the 4–6 pp gap entirely or leaves ≤2 pp residual
(the irreducible tokio/pgwire async-framing cost on small queries,
shared by both backends).

`pgbench tpcb` (6 statements per `'Q'`) should gain proportionally
more in absolute terms but proportionally less per statement —
already at 1.07× vanilla, so ceiling-bound by the executor work
itself.

Custom bench numbers: **no change**. The custom bench exercises
extended-query, which already uses the direct backend.

## 11. Open questions

> Q2 / Q3 / Q4 are **"verify at landing"** items, not design
> blockers. Tag them in the landing PR description so reviewers
> can confirm before merge.

- **Q1.** Should `pg_transport.execution_backend = 'spi'` keep
  routing simple-query through the existing SPI path indefinitely?
  **This doc's answer: yes, indefinitely.** SPI stays as a
  supported backend; no deprecation horizon is proposed here.
  Any future change to that policy belongs in a separate design.
- **Q2 (verify at landing).** Multi-statement `'Q'` with mixed
  utility + DML (e.g. `SET x = 1; SELECT current_setting('x')`)
  needs an extra `CommandCounterIncrement` between statements?
  PG's `exec_simple_query` calls `CommandCounterIncrement`
  between statements implicitly via xact state transitions;
  `Portal*` inherits this. Confirm via the dedicated e2e test
  (§8.1) before flipping the GUC default.
- **Q3 (verify at landing).** Should `run_one_direct` thread
  parse-error reporting through `caught_error_to_pgwire` the same
  way `with_spi` does, or use a thinner wrapper?
  `pg_analyze_and_rewrite_fixedparams` raises PG ERRORs via
  `ereport`, same machinery, so the existing `with_xact`
  `PgTryBuilder` catch should already cover it. Confirm during
  implementation by deliberately triggering an analyze-time
  error (`SELECT * FROM nonexistent`) and asserting wire shape.
- **Q4 (verify at landing).** Does the parse `MemoryContext`
  need to be a *child* of the per-statement working context, or a
  *parent*? §6.1 picks parent (parsetree outlives each
  statement's analyze/plan ctx); verify there's no PG-internal
  call that walks up the context parent chain and trips on a
  parse-ctx-owned `RawStmt` after a statement's working context
  is gone. The memory-leak soak (§8.4) plus a deliberate
  3-statement query exercising parsetree references should catch
  any bug.

## 12. References

- [Parent: planner-executor-direct-path.md](planner-executor-direct-path.md) — strategic SPI-vs-direct trade space; this doc is its Stage B implementation plan.
- [backend-wire.md §6](../backend-wire.md#6-spi-bridge) — SPI bridge architecture.
- [performance.md §2](../performance.md#2-latest-stable-numbers-2026-05-21) — pgbench numbers that motivate this work.
- [configuration.md](../configuration.md#26) — `pg_transport.execution_backend` GUC docs.
- [crates/core/src/backend/spi_bridge.rs](../../../crates/core/src/backend/spi_bridge.rs) — current simple-query impl.
- [crates/core/src/backend/executor.rs](../../../crates/core/src/backend/executor.rs) — `with_xact`, `XactCtx`, `Portal`, `CachedPlanSource`, `CachedPlan`, `ParamList`, `TupleDescRef`. Already shipped; the simple-query direct path consumes this.
- [crates/core/src/backend/extended/direct.rs](../../../crates/core/src/backend/extended/direct.rs) — extended-query direct impl (`WireDestReceiver` / `ColumnEncoder` reuse source).
- [crates/core/src/backend/spi.rs](../../../crates/core/src/backend/spi.rs) — `with_spi`, `caught_error_to_pgwire`, `TypeOutput`/`TypeSend`, the per-handoff state reset.
- PG source: [`src/backend/tcop/postgres.c::exec_simple_query`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c) — the shape we're approximating.
- PG source: [`src/backend/parser/analyze.c::pg_analyze_and_rewrite_fixedparams`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/parser/analyze.c).
- PG source: [`src/backend/tcop/pquery.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c) — `PortalRun`, the routing into `ProcessUtility` for utility statements.
