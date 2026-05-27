# Simple-query direct path

> Parent: [../README.md](../README.md) · Strategic context: [planner-executor-direct-path.md](planner-executor-direct-path.md)
> Siblings: [../backend-wire.md](../backend-wire.md) §6 (SPI bridge) · [../performance.md](../performance.md) §2
>
> **Status: shipped.** Selecting
> `pg_transport.execution_backend = 'direct'` routes simple-query
> `'Q'` messages through `pg_analyze_and_rewrite_fixedparams` +
> `pg_plan_queries` + `PortalRun` with a custom `DestReceiver`
> that encodes wire `DataRow` frames inline. Default backend
> remains `spi`; both ship side-by-side indefinitely.
>
> Code: [`crates/core/src/backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs)
> (top-level entry + per-statement worker),
> [`crates/core/src/backend/dest_receiver.rs`](../../../crates/core/src/backend/dest_receiver.rs)
> (the `WireDestReceiver`, shared with the extended-query direct
> backend),
> [`crates/core/src/backend/executor.rs`](../../../crates/core/src/backend/executor.rs)
> (`with_xact`, `Portal`, `ScopedMemoryContext`).

## 1. Summary

`execute_simple_query_direct(query)` is the entry point invoked
by [`SimpleQueryHandler::do_query`](../../../crates/core/src/wire/pgwire_v3.rs)
when the session GUC is set to `direct`. It mirrors PG's
`exec_simple_query` shape (one raw parse, then a per-statement
analyze + plan + Portal + `PortalRun` loop) and skips the three
overheads the SPI backend pays:

1. **Second parse** — `SPI_execute` re-parses the string inside
   SPI. The direct backend keeps the `raw_parser` output alive
   in a per-call MemoryContext and feeds it straight to
   `pg_analyze_and_rewrite_fixedparams`.
2. **`SPI_tuptable` materialisation** — every SPI tuple is
   heap-tuple-copied into the SPI table. The direct backend
   uses `WireDestReceiver::receiveSlot` to encode each row as
   wire bytes during the executor scan.
3. **Xact-control intercept** — SPI in atomic mode rejects
   `BEGIN`/`COMMIT`/`ROLLBACK`, so the SPI bridge hand-routes
   them via the xact-block API. `PortalRun` routes
   `TransactionStmt` through `ProcessUtility` natively, same as
   vanilla PG. The intercept does not exist on the direct path.

One pgwire-side copy remains and is intrinsic to the
`SimpleQueryHandler` API; see §5.6.

## 2. Data path

```text
'Q' message → SimpleQueryHandler::do_query → execute_simple_query_direct(query)
                                              │
                                              ▼ parse_and_keep(query)
                              ┌──────────────────────────────────────┐
                              │ raw_parser → List *RawStmt           │ (the only parse pass)
                              │ kept alive in ScopedMemoryContext    │
                              │  (parse_ctx, dropped on return)      │
                              └──────────────────────────────────────┘
                                              │ ParsedQuery { statements, parse_ctx }
                                              ▼
                              ┌──────────────────────────────────────┐
                              │ for each (text, *mut RawStmt):       │
                              │   with_xact(|ctx|                    │
                              │     run_one_direct(ctx, raw_stmt,    │
                              │                    &sql_cstr))       │
                              │      ├─ CreateCommandTag             │
                              │      ├─ pg_analyze_and_rewrite_fixed │   ← lands in
                              │      ├─ pg_plan_queries              │     TopTransactionContext
                              │      ├─ Portal::create_anonymous     │     (created/dropped by
                              │      ├─ Portal::define               │     with_xact's Start /
                              │      ├─ Portal::start                │     CommitTransactionCommand)
                              │      ├─ read portal->tupDesc         │
                              │      │   → schema + ColumnEncoder[]  │
                              │      ├─ Portal::run(WireDestReceiver,│
                              │      │              count=FETCH_ALL) │
                              │      └─ Drop: Portal                 │
                              └──────────────────────────────────────┘
                                              │ Vec<Response>
                                              ▼ pgwire encodes Response → wire bytes
```

The `Portal::run` call drives `WireDestReceiver::receiveSlot`
once per row. Each invocation walks the tuple's columns and
appends one length-prefixed `DataRow` frame to a Rust-side
`Vec<DataRow>`. After `PortalDrop`, the response wrapper hands
that vec to pgwire as a `stream::iter`.

## 3. Module layout

| Symbol | Location | Visibility |
|---|---|---|
| `execute_simple_query_direct` | [`backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs) | `pub` |
| `ParsedQuery` / `parse_and_keep` | [`backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs) | `pub(crate)` |
| `run_one_direct` | [`backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs) | private |
| `extract_raw_stmt_spans` | [`backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs) | private |
| `WireDestReceiver`, `ColumnEncoder`, `wire_receive_slot`, `as_dest_receiver`, `command_tag_name`, `schema_and_encoders_text` | [`backend/dest_receiver.rs`](../../../crates/core/src/backend/dest_receiver.rs) | `pub(crate)` |
| `with_xact`, `XactCtx`, `Portal`, `ParamList`, `TupleDescRef`, `ScopedMemoryContext`, `MemoryContextGuard` | [`backend/executor.rs`](../../../crates/core/src/backend/executor.rs) | `pub` |
| `caught_error_to_pgwire`, `generic_error`, `TypeOutput`, `TypeSend` | [`backend/spi.rs`](../../../crates/core/src/backend/spi.rs) | `pub` (carried forward; SPI backend still uses these) |
| `pg_transport.execution_backend` GUC dispatch | [`wire/pgwire_v3.rs`](../../../crates/core/src/wire/pgwire_v3.rs) `SimpleQueryHandler::do_query` | — |

`WireDestReceiver` + `ColumnEncoder` originally lived in
[`extended/direct.rs`](../../../crates/core/src/backend/extended/direct.rs);
they were lifted into a shared `dest_receiver` module so both
the extended-query and simple-query direct backends share one
`DestReceiver` implementation and one type-I/O cache. Behaviour
was preserved byte-for-byte; only visibility changed.

## 4. Memory-context discipline

Two contexts, two lifetimes:

- **Parse context** (`ParsedQuery.parse_ctx`, named
  `pg_transport_parse_ctx`) owns the parsetree list returned by
  `raw_parser`. Created by `parse_and_keep`; deleted when
  `ParsedQuery` drops at the end of
  `execute_simple_query_direct`. Spans the whole `'Q'` body so
  the `*mut RawStmt` pointers stay valid across the
  per-statement `with_xact` loop.
- **Portal context** (`portal->portalContext`) is owned by PG;
  `PortalDefineQuery` copies the planned-statement list onto it,
  and `PortalDrop` releases it on `Portal::drop`. Independent of
  the parse + transaction contexts.

Per-statement analyze/plan transients land in PG's own
**`TopTransactionContext`**, which `with_xact`'s
`StartTransactionCommand` creates and switches
`CurrentMemoryContext` into for the body of each statement.
`CommitTransactionCommand` on the way out deletes that context,
freeing the transients in one shot. `PortalDefineQuery` having
copied the plan onto the portal's own context means
`Portal::run` is unaffected by that teardown. This matches
vanilla `exec_simple_query`'s discipline — no framework-owned
per-statement context is needed.

The pointer-lifetime invariant: each `*mut pg_sys::RawStmt` in
`ParsedQuery.statements` is valid for the lifetime of
`parse_ctx`. Dropping `ParsedQuery` invalidates every pointer
the slice held. The lifetime parameter `'q` on `ParsedQuery`
ties only the text slices to the input string; the raw pointers
are not protected by the type system. The `for` loop in
`execute_simple_query_direct` dereferences each pointer while
`parsed` is in scope, which is the only correct usage pattern.

**Snapshot interaction.** `with_xact` calls
`PushActiveSnapshot(GetTransactionSnapshot())`. `PortalRun` for
SELECT internally does its own `PushActiveSnapshot` + matching
pop per scan. Nested push/pop is fine — PG's snapshot stack is
designed for this, vanilla `exec_simple_query` hits the same
path, and the extended-query direct backend has exercised it on
every `'E'` message since Stage B. The
`PortalStart(snapshot=GetActiveSnapshot())` call records the
`with_xact` snapshot on the `QueryDesc`; PortalRun's internal
push is per-scan and doesn't replace it.

## 5. Subtleties (read before editing)

### 5.1 `FETCH_ALL` is `LONG_MAX`, not `0`

`PortalRun(count: i64, ...)`: PG 18 treats `count <= 0` as
`NoMovementScanDirection` (i.e. "scan no rows"). The all-rows
sentinel is `FETCH_ALL`, which `#define`s to `LONG_MAX`. The
direct path passes `i64::MAX`. Passing `0` returns zero rows
with no diagnostic, which surfaces as an empty `DataRow` vec
even for `SELECT 1`. Both
[`simple_direct::run_one_direct`](../../../crates/core/src/backend/simple_direct.rs)
and
[`extended::direct::execute_impl`](../../../crates/core/src/backend/extended/direct.rs)
do this; do not regress.

### 5.2 `portal->tupDesc` is populated by `PortalStart`, not `PortalDefineQuery`

The tupdesc is `NULL` immediately after `PortalDefineQuery` and
only becomes valid after `PortalStart` (which runs
`ExecutorStart`, which sets up the `QueryDesc`, which writes
back the tupdesc onto the portal). The direct path reads
`portal->tupDesc` strictly between `Portal::start(…)` and
`Portal::run(…)`. Reading it earlier returns `None` and
silently routes every SELECT down the utility-no-rows branch.

### 5.3 Error shape matches the SPI backend

`execute_simple_query_direct` uses `?`-propagation. On first
error the function returns `Err` and pgwire's
`SimpleQueryHandler` emits one `ErrorResponse` + one
`ReadyForQuery`. Pushing `Response::Error` into the `Vec`
instead would emit the same error payload but double-emit
`ReadyForQuery` — observable to clients and breaks
dual-backend test parity. Keep the `?` shape.

### 5.4 `with_xact` matches vanilla `TBLOCK_ABORT` recovery

On a PG `ERROR` from inside `with_xact`'s closure for an
explicit `BEGIN` block, `AbortCurrentTransaction` moves the
xact state machine to `TBLOCK_ABORT` (not `DEFAULT` — see PG
[`xact.c::AbortCurrentTransaction`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/transam/xact.c)).
The client must issue `COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION`
/ `ROLLBACK TO SAVEPOINT` to exit the aborted block; every
other parsetree on the next `'Q'` is rejected with SQLSTATE
`25P02` by the pre-Push aborted-block check in
[`execute_simple_query_direct`](../../../crates/core/src/backend/simple_direct.rs).
Matches vanilla `exec_simple_query` exactly. Crossing this
state requires `with_xact(needs_snapshot=false, ...)` — see
[§7.3](#73-per-statement-snapshot-gating) for why an
unconditional `PushActiveSnapshot(GetTransactionSnapshot())`
from `TBLOCK_ABORT` is unsafe.

### 5.5 Utility statements go through `ProcessUtility`

`SET`, `CREATE`, `VACUUM`, and `TransactionStmt`
(BEGIN/COMMIT/ROLLBACK + their mode-list variants) all route
through `PortalRun` → `ProcessUtility`. Same function PG's own
`exec_simple_query` calls. The SPI backend's
`handle_xact_control` / `XactCmd` intercept doesn't exist on
the direct path because it isn't needed.

`BEGIN; SELECT 1; COMMIT` in one `'Q'`: PG routes
`TransactionStmt` via `ProcessUtility` →
`BeginTransactionBlock` / `EndTransactionBlock`, which mutate
xact-block state (`TBLOCK_STARTED` → `TBLOCK_INPROGRESS`). The
`StartTransactionCommand` / `CommitTransactionCommand` brackets
that `with_xact` wraps each statement with handle the
*command* boundary on top of any open block: from
`TBLOCK_INPROGRESS`, `CommitTransactionCommand` issues
`CommandCounterIncrement` rather than an actual commit.

### 5.6 `Vec<DataRow>` materialisation is intrinsic

pgwire's `SimpleQueryHandler::do_query` returns
`PgWireResult<Vec<Response>>`. `Response::Query(QueryResponse)`
takes a `Stream<Item = PgWireResult<DataRow>>` that pgwire
consumes *after* `do_query` returns — i.e. after `PortalDrop`.
We cannot stream from a live `Portal`/`WireDestReceiver` past
`PortalDrop`, so `receiveSlot` writes encoded rows into a
`Vec<DataRow>` and the response wraps `stream::iter(data_rows)`.

This is one Rust-side `Vec` copy of already-encoded wire bytes,
not a tuple copy. Removing it would need a pgwire-side API
change (streaming `DataRow` from inside `do_query`); out of
scope.

### 5.7 Cancel + interrupt handling

No special plumbing. `PortalRun` calls `CHECK_FOR_INTERRUPTS()`
internally during executor scan, same as `SPI_execute` does
today. Cancel-message routing itself is still deferred per
[cancel-routing.md](cancel-routing.md).

## 6. Tests

Unit tests in [`simple_direct.rs::tests`](../../../crates/core/src/backend/simple_direct.rs)
cover the in-process paths:

- `pg_simple_direct_empty_query` — empty / whitespace-only /
  comment-only inputs parse to zero statements.
- `pg_simple_direct_select_one_row` — basic SELECT round-trip
  including text-format decoding of the encoded `DataRow`.
- `pg_simple_direct_multi_statement` — multi-statement `'Q'`
  splits into one response per statement.
- `pg_simple_direct_utility_set` — `SET` routes through
  `ProcessUtility` and returns `Response::Execution`.
- `pg_simple_direct_syntax_error_stops_batch` — syntax error
  surfaces as `PgWireError`; later statements aren't executed.

`xact-control` (`BEGIN`/`COMMIT`/`ROLLBACK`) is **disabled** as a
`#[pg_test]` because the pg_test framework wraps each test in
an outer `START TRANSACTION`; running `BEGIN` inside that
triggers a `WARNING: there is already a transaction in
progress`, leaks a snapshot reference, and segfaults the test
backend on commit. Vanilla PG hits the same WARNING path. The
case is covered in [e2e](../../../crates/e2e/tests/basic.rs) by
`simple_direct_xact_control_begin_commit` and
`simple_direct_xact_control_rollback`, which speak to a real
connection with no outer xact.

End-to-end coverage in [`crates/e2e/tests/basic.rs`](../../../crates/e2e/tests/basic.rs)
runs the dual-backend cases against the live wire layer (8
tests, `simple_direct_*` prefix): SELECT round-trip,
multi-column, multi-statement, BEGIN/COMMIT, BEGIN/ROLLBACK,
syntax error recovery, division-by-zero recovery, SET +
read-back.

`just check` runs both pgrx and e2e suites.

## 7. Vanilla `exec_simple_query` parity

The direct backend mirrors vanilla
[`exec_simple_query`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c)
in every behaviour a client can observe through a `'Q'`
message: aborted-block rejection, implicit-block atomicity,
snapshot discipline, FETCH-binary cursor format, the
observability globals downstream extensions read, and the
GUC-armed timeout. The SPI bridge holds the same invariants by
the same shape, with two deliberate divergences documented in
[backend-wire.md §6](../backend-wire.md#6-spi-bridge)
(xact-control re-routing, warning suppression).

Each subsection below describes one such invariant: what the
code does, where it lives, the vanilla line it tracks, and the
test that pins it. Per-query performance shapes (FmgrInfo cache,
per-row buffer) are in [§8](#8-per-query-performance-shape).

### 7.1 FETCH-binary cursor format

[`fetch_result_format(raw_stmt)`](../../../crates/core/src/backend/simple_direct.rs)
walks the parsetree for `IsA(stmt, FetchStmt)`, calls
`GetPortalByName(portalname)`, and returns `FieldFormat::Binary`
when the cursor was declared with `CURSOR_OPT_BINARY`.
`run_one_direct` consults it right before
`schema_and_encoders_uniform` so the entire result set picks the
matching encoder. The SPI bridge piggybacks on the existing
`parse_and_classify` pass: `StmtMeta { fetch_portalname }`
stashes the cursor name as a Rust-owned `CString` so
`run_via_spi` makes the same lookup without a second parse.
Mirrors vanilla
[postgres.c:1259-1273](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1259-L1273).
Pinned by `pg_simple_direct_fetch_binary_cursor_returns_binary`
(direct) + `pg_spi_bridge_fetch_binary_cursor_returns_binary`
(SPI).

### 7.2 Implicit-block multi-statement atomicity

When a `'Q'` body parses to more than one non-empty statement,
[`execute_with_implicit_block`](../../../crates/core/src/backend/simple_direct.rs)
wraps the per-statement loop in
`BeginImplicitTransactionBlock` /
`EndImplicitTransactionBlock`, with one outer `catch_unwind`
around the whole body. A PG `ERROR` in any sub-statement
unwinds to `AbortCurrentTransaction`, collapsing the implicit
block and rolling back every earlier sub-statement's work.
Last-iteration tail is `EndImplicit + Commit`; mid-batch
`TransactionStmt` ends the implicit block at that statement
(`Commit`); everything else uses `CommandCounterIncrement`.
Mirrors vanilla
[postgres.c:1097 + :1167-1170 + :1310](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1097).

The SPI bridge's
[`execute_with_implicit_block_spi`](../../../crates/core/src/backend/spi_bridge.rs)
inlines `SPI_connect` / `SPI_finish` per iter under one outer
`catch_unwind`; it can't reuse
[`with_spi`](../../../crates/core/src/backend/spi.rs) because
the nested `catch_unwind` inside `with_spi` would collapse the
implicit block on first ERROR. Pinned by
`implicit_block_rolls_back_partial_*_backend` in
[`crates/e2e/tests/basic.rs`](../../../crates/e2e/tests/basic.rs).

### 7.3 Per-statement snapshot gating

The dispatch loops compute
`needs_snapshot = pg_sys::analyze_requires_snapshot(raw_stmt)`
per statement and pass it through to
[`with_xact(needs_snapshot, body)`](../../../crates/core/src/backend/executor.rs),
which conditionally `PushActiveSnapshot(GetTransactionSnapshot())`s
only when analyze needs one. `TransactionStmt` / `SET` / `SHOW`
return `false`; SELECT / DML / `EXPLAIN` / `DECLARE CURSOR` /
`CTAS` / `CALL` return `true`. The conditional Push is
load-bearing for `TBLOCK_ABORT` entry: from that state,
`GetTransactionSnapshot` walks into a PG assert (which would
`SIGABRT` the slot in a cassert build).

[`run_one_direct`](../../../crates/core/src/backend/simple_direct.rs)
passes `InvalidSnapshot` (`std::ptr::null_mut::<SnapshotData>()`)
to `PortalStart`. For `PORTAL_ONE_SELECT`, `PortalStart`
reacquires via `GetActiveSnapshot` internally; for
`PORTAL_MULTI_QUERY` (utility statements) no snapshot is needed.
Mirrors vanilla
[postgres.c:1191-1195](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1191)
and [postgres.c:1235](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1235).
Pinned by the §6.3 abort-recovery e2e test
(`aborted_block_direct_backend_wire_rollback_succeeds` —
`TBLOCK_ABORT` → `ROLLBACK` → `SELECT` end-to-end).

### 7.4 Aborted-block rejection (SQLSTATE `25P02`)

Both per-statement dispatch loops on each backend gate the
xact-bracket entry on
`pg_sys::IsAbortedTransactionBlockState() && !is_transaction_exit_stmt(raw_stmt)`
and return
[`aborted_transaction_block_error()`](../../../crates/core/src/backend/spi.rs)
(SQLSTATE `25P02`) when the gate trips. `is_transaction_exit_stmt`
mirrors vanilla's `IsTransactionExitStmt`: `COMMIT` /
`ROLLBACK` / `PREPARE TRANSACTION` / `ROLLBACK TO SAVEPOINT`
are allowed through; everything else (including `BEGIN`,
`SAVEPOINT`, `RELEASE`, `COMMIT PREPARED`) is rejected.
Mirrors vanilla
[postgres.c:1058-1063](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1058-L1063).

Companion:
[`reset_per_handoff_state`](../../../crates/core/src/backend/slot.rs)
calls `AbortOutOfAnyTransaction()` before `ResetAllOptions()`,
so a client that disconnects mid-aborted-block doesn't leave
xact state for the next handoff's first statement to inherit.
Pinned by `aborted_block_*_backend_returns_25p02` and
`aborted_block_direct_backend_wire_rollback_succeeds` in
[`crates/e2e/tests/regressions.rs`](../../../crates/e2e/tests/regressions.rs).

### 7.5 Per-query observability globals

[`DebugQueryGuard::install`](../../../crates/core/src/backend/observability.rs)
(RAII) pins `pg_sys::debug_query_string` to the query body and
reports `pgstat_report_activity(STATE_RUNNING, ...)` for the
guard's lifetime. Drop restores the previous global and reports
`STATE_IDLE`. Installed at every backend entry point: simple
direct, simple SPI, extended Parse, extended Execute. These
globals are the ones `pg_stat_statements`, `auto_explain`,
`pg_stat_activity.query`, and the server-log `STATEMENT:` line
read for attribution.

[`StatementTimeoutGuard::install`](../../../crates/core/src/backend/observability.rs)
arms `enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout)`
when the GUC is set and disarms on Drop — including on the
PG-ERROR panic path, so a fired
"canceling statement due to statement timeout" (SQLSTATE
`57014`) disarms the timer before the panic propagates. Mirrors
vanilla [postgres.c:1046-1048 + :1131-1139](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1046-L1048).

Pinned by `pg_*_backend_executor_start_hook_sees_inner_query`
(via the shared
[`capture_executor_start`](../../../crates/core/src/backend/observability.rs)
test helper) and `pg_simple_direct_statement_timeout_arms_timer`
(via the SQL-callable `pg_transport_statement_timeout_is_active`
probe — reads `get_timeout_active(STATEMENT_TIMEOUT)`
mid-execution to avoid the pg_test outer-xact corruption a real
cancel would cause).

### 7.6 `pg_stat_statements` query_id reset per statement

[`run_one_direct`](../../../crates/core/src/backend/simple_direct.rs)
(direct) and [`run_via_spi`](../../../crates/core/src/backend/spi_bridge.rs)
(SPI) call `pg_sys::pgstat_report_query_id(0, true)` +
`pg_sys::pgstat_report_plan_id(0, true)` immediately before the
analyze step (`pg_analyze_and_rewrite_fixedparams` on direct,
inside `SPI_execute` on SPI). Both reporters bail out with
`force=false` when `st_query_id` is non-zero, so without the
explicit reset statement N's id (installed by
`pg_stat_statements`'s `post_parse_analyze_hook` via
`pgstat_report_query_id(hash, false)`) would leak into
statement N+1's attribution.

The reset is redundant on iter 1 — §7.5's
`pgstat_report_activity(STATE_RUNNING)` already zeroes
`st_query_id` as a side-effect (`backend_status.c:660-668`) —
but load-bearing on iter 2+. Issued uniformly to keep the code
shape symmetric across single/multi callers. Mirrors vanilla
[postgres.c:1110-1111](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c#L1110-L1111).
Pinned by `pg_*_resets_query_id_between_statements` (both
backends) via the shared
[`capture_query_id_at_parse_analyze`](../../../crates/core/src/backend/observability.rs)
helper, which installs a fake-`pg_stat_statements`
`post_parse_analyze_hook` that records `st_query_id` at entry
and writes a per-invocation sentinel via
`pgstat_report_query_id(SENTINEL + n, false)`.

## 8. Per-query performance shape

### 8.1 Per-column `FmgrInfo` cache

[`TypeOutput`](../../../crates/core/src/backend/spi.rs) and
`TypeSend` cache a full `FmgrInfo` (resolved once at
`for_column` time via `fmgr_info_cxt` against the receiver's
context) instead of the type OID. The hot row loop dispatches
via `OutputFunctionCall(&finfo, datum)` /
`SendFunctionCall(&finfo, datum)` — same call shape as vanilla
[`printtup`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/common/printtup.c#L361),
no per-cell syscache lookup. Pinned by
`pg_simple_direct_wide_multirow_select_uses_cached_finfo`
(five distinct types so a broken cache surfaces regardless of
which column it lives on).

### 8.2 Per-row `BytesMut` allocation (known divergence)

[`dest_receiver.rs`](../../../crates/core/src/backend/dest_receiver.rs)'s
`receiveSlot` callback allocates a fresh
`BytesMut::with_capacity(64)` per row. Vanilla's
[`printtup_prepare_info`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/common/printtup.c#L122)
runs `initStringInfo(&myState->buf)` once per portal and then
recycles via `pq_beginmessage_reuse` /
`pq_endmessage_reuse`
([`printtup.c:327`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/common/printtup.c#L327)).

The receiver materialises every row as a `DataRow` owning its
own bytes and pushes onto a `Vec<DataRow>` that pgwire consumes
*after* `PortalDrop` (see [§5.6](#56-vecdatarow-materialisation-is-intrinsic)).
A cursor that resets the same `BytesMut` across rows would have
to coordinate with pgwire's `DataRow` ownership; the smaller
allocation footprint isn't worth the pgwire-side change at
current bench-driven priorities. Tracked in
[performance.md](../performance.md).

## 9. References

- Parent: [planner-executor-direct-path.md](planner-executor-direct-path.md) — strategic SPI-vs-direct trade space and the survey of what's callable from `postgres.c`.
- [backend-wire.md §6](../backend-wire.md#6-spi-bridge) — SPI bridge architecture (the default backend).
- [performance.md](../performance.md) — bench-driven motivation and current numbers.
- [configuration.md](../configuration.md#26) — `pg_transport.execution_backend` GUC docs.
- [`crates/core/src/backend/spi_bridge.rs`](../../../crates/core/src/backend/spi_bridge.rs) — the SPI backend (`execute_simple_query`); kept maintained side-by-side.
- PG source: [`src/backend/tcop/postgres.c::exec_simple_query`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c) — the vanilla shape the direct path mirrors.
- PG source: [`src/backend/tcop/pquery.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c) — `PortalRun`, the routing into `ProcessUtility` for utility statements.
- PG source: [`src/backend/parser/analyze.c::pg_analyze_and_rewrite_fixedparams`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/parser/analyze.c).
