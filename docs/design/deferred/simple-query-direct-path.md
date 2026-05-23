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
                              │     run_one_direct(ctx, &parse_ctx,  │
                              │                    raw_stmt,         │
                              │                    text, &sql_cstr)) │
                              │      ├─ CreateCommandTag             │
                              │      ├─ child stmt_ctx               │
                              │      ├─ pg_analyze_and_rewrite_fixed │
                              │      ├─ pg_plan_queries              │
                              │      ├─ Portal::create_anonymous     │
                              │      ├─ Portal::define               │
                              │      ├─ Portal::start                │
                              │      ├─ read portal->tupDesc         │
                              │      │   → schema + ColumnEncoder[]  │
                              │      ├─ Portal::run(WireDestReceiver,│
                              │      │              count=FETCH_ALL) │
                              │      └─ Drop: Portal, stmt_ctx       │
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

Three contexts, three lifetimes:

- **Parse context** (`ParsedQuery.parse_ctx`, named
  `pg_transport_parse_ctx`) owns the parsetree list returned by
  `raw_parser`. Created by `parse_and_keep`; deleted when
  `ParsedQuery` drops at the end of
  `execute_simple_query_direct`. Spans the whole `'Q'` body.
- **Per-statement working context** (`stmt_ctx`, named
  `pg_transport_stmt_ctx`) is a child of `parse_ctx`. Created
  inside `run_one_direct`; everything
  `pg_analyze_and_rewrite_fixedparams` and `pg_plan_queries`
  `palloc`s during that call lands here. Deleted at function
  exit (after `PortalDrop` has copied what it needs onto the
  portal's own context). Matches what `exec_simple_query` does
  via `MessageContext` + per-statement reset.
- **Portal context** (`portal->portalContext`) is owned by PG;
  `PortalDrop` releases it on `Portal::drop`.

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

### 5.4 `with_xact` abort path collapses to `DEFAULT`

On a PG `ERROR` from inside the closure, `with_xact` calls
`AbortCurrentTransaction`, which moves the xact state machine
to `DEFAULT`. Vanilla PG would have left it at `TBLOCK_ABORT`,
requiring the client to issue `ROLLBACK`. This deviation is
inherited from the SPI bridge (see
[`run_spi_statement`'s doc comment](../../../crates/core/src/backend/spi_bridge.rs))
— harmless for our target workloads; flagged here so anyone
chasing a "why doesn't my next statement need ROLLBACK after an
error" question finds the answer.

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

## 7. References

- Parent: [planner-executor-direct-path.md](planner-executor-direct-path.md) — strategic SPI-vs-direct trade space and the survey of what's callable from `postgres.c`.
- [backend-wire.md §6](../backend-wire.md#6-spi-bridge) — SPI bridge architecture (the default backend).
- [performance.md](../performance.md) — bench-driven motivation and current numbers.
- [configuration.md](../configuration.md#26) — `pg_transport.execution_backend` GUC docs.
- [`crates/core/src/backend/spi_bridge.rs`](../../../crates/core/src/backend/spi_bridge.rs) — the SPI backend (`execute_simple_query`); kept maintained side-by-side.
- PG source: [`src/backend/tcop/postgres.c::exec_simple_query`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c) — the vanilla shape the direct path mirrors.
- PG source: [`src/backend/tcop/pquery.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c) — `PortalRun`, the routing into `ProcessUtility` for utility statements.
- PG source: [`src/backend/parser/analyze.c::pg_analyze_and_rewrite_fixedparams`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/parser/analyze.c).
