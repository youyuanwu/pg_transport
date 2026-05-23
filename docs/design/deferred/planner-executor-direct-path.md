# Planner + executor direct path

> Parent: [../README.md](../README.md)
> Sibling: [../backend-wire.md](../backend-wire.md) §6 (SPI bridge) · [simple-query-direct-path.md](simple-query-direct-path.md) (concrete simple-query implementation)
>
> **Status: shipped, both paths.** Selecting
> `pg_transport.execution_backend = 'direct'` routes both
> simple-query (`'Q'`) and extended-query (`'P'`/`'B'`/`'E'`)
> through `Portal*` + a custom `WireDestReceiver` whose
> `receiveSlot` encodes wire `DataRow` frames inline during
> `PortalRun`. The default backend is `spi`; both ship
> side-by-side indefinitely, selectable per-session.

This doc explains the strategic shape (why a parallel
backend at all, why this particular reuse strategy, what's
callable from the postgres binary). For the simple-query
implementation, see
[simple-query-direct-path.md](simple-query-direct-path.md).
For the extended-query implementation, see
[`extended/direct.rs`](../../../crates/core/src/backend/extended/direct.rs).

## 1. Summary

| Wire message | Default backend (`spi`) | Direct backend (`direct`) |
|---|---|---|
| `'Q'` simple-query | `SPI_execute` via `spi_bridge::execute_simple_query` | `PortalRun` + `WireDestReceiver` via `simple_direct::execute_simple_query_direct` |
| `'P'` Parse | `SPI_prepare` + `SPI_keepplan` via `extended::spi::prepare` | `CreateCachedPlan` + `pg_analyze_and_rewrite_*` via `extended::direct::prepare` |
| `'B'` Bind / `'E'` Execute | `SPI_execute_plan` via `extended::spi::execute` | `GetCachedPlan` + `PortalRun` + `WireDestReceiver` via `extended::direct::execute_impl` |

Both backends share the `WireDestReceiver` /
`ColumnEncoder` / `command_tag_name` implementation in
[`backend/dest_receiver.rs`](../../../crates/core/src/backend/dest_receiver.rs)
and the `with_xact` / `Portal` / `CachedPlanSource` /
`ParamList` / `TupleDescRef` / `ScopedMemoryContext`
wrappers in [`backend/executor.rs`](../../../crates/core/src/backend/executor.rs).

## 2. Background — what's callable from the postgres binary

PG's wire-protocol dispatch lives entirely in
[`src/backend/tcop/postgres.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c).
The dispatchers themselves are **not** reusable from a loadable
extension; two independent blockers, both verified with `nm -D`.

### 2.1 The `exec_*` dispatchers are `static`

```text
$ nm -D ~/.pgrx/18.4/pgrx-install/bin/postgres |
    grep -E " T (exec_simple_query|exec_parse_message|exec_bind_message|exec_execute_message)\>"
(no output)
```

`exec_simple_query`, `exec_parse_message`, `exec_bind_message`,
`exec_execute_message`, `start_xact_command`, and
`finish_xact_command` are file-scope only.

### 2.2 They write to a libpq socket we don't own

Even if they were exported, they use
`CreateDestReceiver(whereToSendOutput)` → `DestRemote` →
`printtup_*` → `pq_putmessage` → `MyProcPort->sock`. The
bgworker we run inside has no `MyProcPort` — we own the
client fd ourselves and speak [pgwire crate](https://crates.io/crates/pgwire)
framing on top of it. Redirecting `pq_putmessage` per-bgworker
would require patching every `pq_*` call site, i.e. forking
PG.

### 2.3 `PostgresMain` itself is a process owner

```text
$ nm -D ~/.pgrx/18.4/pgrx-install/bin/postgres |
    grep " T PostgresMain\>"
00000000005801b0 T PostgresMain
```

Exported, but assumes `MyProcPort` is connected, installs the
sigsetjmp-based ERROR recovery for the entire process, drives
the message loop, and only returns on disconnect / `proc_exit`.
We can't enter and leave it per query.

### 2.4 What is callable

The layer *below* the dispatchers — parser, analyzer, planner,
portal lifecycle, plan cache, destination receivers — is all
exported and stable PG-server API:

| Symbol | Use |
|---|---|
| `raw_parser` / `pg_parse_query` | raw → `List* RawStmt*` |
| `pg_analyze_and_rewrite_fixedparams` | RawStmt + known param types → analyzed `Query*` |
| `pg_analyze_and_rewrite_varparams` | RawStmt with inferred params |
| `pg_plan_queries` | `List* Query*` → `List* PlannedStmt*` |
| `CreatePortal` / `PortalDefineQuery` / `PortalStart` / `PortalRun` / `PortalDrop` | per-statement execution lifecycle |
| `CreateCachedPlan` / `CompleteCachedPlan` / `SaveCachedPlan` / `GetCachedPlan` / `ReleaseCachedPlan` | extended-query plan caching |
| Custom `DestReceiver` (`#[repr(C)]` with `receiveSlot` callback) | direct-to-wire row encoding |

The reusable surface is roughly: everything *except* the
protocol-message I/O. The dispatchers' job — and what we
re-implement in Rust — is the glue between protocol message
and this lower layer.

## 3. Reuse strategy chosen — custom `DestReceiver`

A `#[repr(C)]` Rust struct
([`WireDestReceiver`](../../../crates/core/src/backend/dest_receiver.rs))
prepends `DestReceiver`'s C function-table layout and appends
our `Vec<DataRow>` + per-column encoders. The `receiveSlot`
callback walks the slot's columns and writes encoded bytes
straight into a `BytesMut` per row, then pushes the `DataRow`
onto the output vec. Zero intermediate tuple copies between the
executor and Rust-side wire bytes.

Two alternatives considered and rejected:

- **Tuplestore destination.** `CreateDestReceiver(DestTuplestore)`
  + `tuplestore_gettupleslot` post-`PortalRun` adds one
  row-copy (executor → tuplestore → wire buffer). Validated as
  Stage A during initial bring-up, then replaced when the
  custom `DestReceiver` measured 1.12× vs vanilla on the
  custom bench (up from 0.98× with the tuplestore intermediate).
- **Keep SPI.** Stable PG API surface, ~5 µs second-parse cost
  per query, one extra heap-tuple copy through `SPI_tuptable`.
  Still ships as the default backend; both backends remain
  available indefinitely so operators can fall back if the
  direct path regresses on their workload.

### 3.1 Risk of the `DestReceiver` ABI

`TupleTableSlot` got significantly reworked in PG 12 (TTS_VIRTUAL
/ TTS_HEAP / TTS_MINIMAL) and is the kind of struct PG
occasionally touches. We track ABI changes per PG major via
`#[cfg(feature = "pgNN")]` shims, same pattern
[Citus](https://github.com/citusdata/citus) uses to carry its
own `TupleDestination` abstraction through PG 11 → 18.
Production precedent: Citus, archived
[PipelineDB](https://github.com/pipelinedb/pipelinedb),
[Spock](https://github.com/pgEdge/spock).

## 4. Module layout

| Component | Location | Purpose |
|---|---|---|
| `with_xact`, `XactCtx` | [`executor.rs`](../../../crates/core/src/backend/executor.rs) | Per-call xact bracket (`StartTransactionCommand` + `PushActiveSnapshot` + matching teardown, abort on PG ERROR caught via `PgTryBuilder`). Replaces the `SPI_connect` / `SPI_finish` bracket. |
| `Portal` (`create_anonymous` / `define` / `start` / `run` / Drop) | [`executor.rs`](../../../crates/core/src/backend/executor.rs) | RAII wrapper over `CreatePortal` / `PortalDefineQuery` / `PortalStart` / `PortalRun` / `PortalDrop`. |
| `CachedPlanSource` / `CachedPlan` | [`executor.rs`](../../../crates/core/src/backend/executor.rs) | RAII over PG's plancache (`CreateCachedPlan` / `CompleteCachedPlan` / `SaveCachedPlan` / `GetCachedPlan` / `ReleaseCachedPlan`). Used by extended-query only; simple-query plans are one-shot. |
| `ParamList`, `TupleDescRef` | [`executor.rs`](../../../crates/core/src/backend/executor.rs) | Borrow-checked accessors for `ParamListInfo` / `TupleDesc`. |
| `ScopedMemoryContext` + `MemoryContextGuard` | [`executor.rs`](../../../crates/core/src/backend/executor.rs) | RAII over `AllocSetContextCreateInternal` / `MemoryContextDelete`, plus a switch-guard. |
| `WireDestReceiver`, `ColumnEncoder`, `command_tag_name`, `schema_and_encoders_text` | [`dest_receiver.rs`](../../../crates/core/src/backend/dest_receiver.rs) | The custom DestReceiver shared by both direct backends. |
| `prepare`, `execute_impl`, `DirectBackendPlan` | [`extended/direct.rs`](../../../crates/core/src/backend/extended/direct.rs) | Extended-query direct backend (`'P'`/`'B'`/`'E'`). |
| `parse_and_keep`, `run_one_direct`, `execute_simple_query_direct` | [`simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs) | Simple-query direct backend (`'Q'`). |
| GUC dispatch | [`wire/pgwire_v3.rs`](../../../crates/core/src/wire/pgwire_v3.rs) `SimpleQueryHandler::do_query`, [`extended/mod.rs`](../../../crates/core/src/backend/extended/mod.rs) `prepare` | Reads `guc::execution_backend()` per call. |

## 5. GUC for backend selection

`pg_transport.execution_backend = 'spi' | 'direct'`, `USERSET`,
defaults to `'spi'`. Per-session and per-role
(`ALTER ROLE ... SET pg_transport.execution_backend = 'direct'`)
selectable. Documented in
[configuration.md](../configuration.md#26).

The dispatcher reads the GUC at query start (one indirect call
per query; measurable as zero on the bench harness). Both
backends remain available indefinitely; switching is per-session
rollback. The doc deliberately does **not** propose deprecating
the SPI bridge — that would be a separate design.

## 6. References

- [simple-query-direct-path.md](simple-query-direct-path.md) — concrete simple-query implementation + memory-context discipline + gotchas (FETCH_ALL, tupDesc timing).
- [backend-wire.md §6](../backend-wire.md#6-spi-bridge) — the SPI bridge (default backend).
- [performance.md](../performance.md) — bench numbers driving both backends.
- [Q18 in roadmap.md](../roadmap.md#23-resolved) — original SPI-vs-direct decision (v0 ships both).
- [Q25 in roadmap.md](../roadmap.md#23-resolved) — parse-once vs parse-twice resolution.
- [Q27 in roadmap.md](../roadmap.md#23-resolved) — extended-query parameter inference path.
- PG source: [`src/backend/tcop/postgres.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c) — the `exec_*` dispatchers (all `static`).
- PG source: [`src/backend/utils/cache/plancache.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/cache/plancache.c) — `CreateCachedPlan` / `CompleteCachedPlan` / `GetCachedPlan`.
- PG source: [`src/backend/tcop/dest.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/dest.c), [`src/backend/access/common/printtup.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/common/printtup.c) — `DestReceiver` API + `printtup_*` (`DestRemote` implementation).
- PG source: [`src/backend/tcop/pquery.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/pquery.c) — `PortalRun` + utility routing.
