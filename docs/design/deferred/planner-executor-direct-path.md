# Planner + executor direct path — deferred design

> Parent: [../README.md](../README.md)
> Sibling: [../backend-wire.md](../backend-wire.md) §6 (SPI bridge) · [../performance.md](../performance.md) §3.1 (the same optimisation in its full-data-path ranking)
>
> **Status: deferred from v0.** This document captures why v0 routes
> SQL execution through SPI (`SPI_prepare` / `SPI_execute_plan`)
> rather than calling PG's planner + executor directly via Portal /
> CachedPlan / DestReceiver, what direct-path strategies look like,
> and the re-entry conditions for un-deferring.
>
> v0 ships with the SPI path. Current bench parity is 0.99x ± noise;
> SPI overhead is **not** material at the v0 acceptance bar (per
> [bench.md](../bench.md)).

## 1. Why deferred

[Q18 in the roadmap](../roadmap.md#23-resolved) resolved the design
question with: "v0 uses SPI exclusively; a planner+executor direct
path is a post-phase-5 optimization, only revisited if bench shows
SPI overhead is material." The bench-harness numbers (phase 5
onwards) have stayed in the 0.84x – 1.04x range across phases 4 → 9.
That's noise, not signal — no perf-driven trigger.

The secondary motivation (code clarity on the simple-query side —
losing `handle_xact_control` / `XactCmd` because `PortalRun` →
`ProcessUtility` handles BEGIN/COMMIT/ROLLBACK naturally) is real
but not urgent: the [`spi.rs` refactor](../../../crates/core/src/backend/spi.rs)
already pulled the worst of the unsafe FFI plumbing into typed
wrappers, so `extended.rs` and `spi_bridge.rs` are clean as-is.

## 2. Background: what default PG does for the wire-protocol dispatchers

PG's wire-protocol dispatch lives entirely in [`src/backend/tcop/postgres.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c).
The relevant functions:

| Function | Purpose |
|---|---|
| `PostgresMain` | Per-connection main loop: reads protocol messages off `MyProcPort->sock`, switches on type byte, dispatches to one of the `exec_*` functions below. |
| `exec_simple_query(query_string)` | Handles `'Q'` messages. One `pg_parse_query` call, then a per-statement `start_xact_command` + analyze + plan + Portal + `PortalRun` + `finish_xact_command` loop. |
| `exec_parse_message(name, q, paramTypes, numParams)` | Handles `'P'`. One parse, one analyze, `CreateCachedPlan` + `CompleteCachedPlan`, stash in `prepared_queries` hashmap. |
| `exec_bind_message(input_message)` | Handles `'B'`. Decodes param values via type input/receive functions, builds a `ParamListInfo`, `GetCachedPlan` + `CreatePortal` + `PortalDefineQuery` + `PortalStart`. |
| `exec_execute_message(portal_name, max_rows)` | Handles `'E'`. `PortalRun` against the bound portal. |

**Output sink:** `whereToSendOutput = DestRemote` is set up by
`BackendInitialize` before `PostgresMain` runs.
`CreateDestReceiver(DestRemote)` returns a `DestReceiver` whose
`receiveSlot` callback walks the slot's columns and writes
`'D'`-frame bytes directly into the libpq output buffer via
`pq_sendint16` / `pq_putmessage`. Everything (RowDescription,
DataRow, CommandComplete, ReadyForQuery) leaves the backend through
`MyProcPort->sock`.

## 3. Can we just call `exec_simple_query`?

**No.** Two independent blockers, verified via `nm -D`:

### 3.1 They're `static`

```text
$ nm -D ~/.pgrx/18.4/pgrx-install/bin/postgres |
    grep -E " T (exec_simple_query|exec_parse_message|exec_bind_message|exec_execute_message)\>"
(no output)
```

`exec_simple_query`, `exec_parse_message`, `exec_bind_message`,
`exec_execute_message`, `start_xact_command`, and
`finish_xact_command` are all declared `static` in `postgres.c`
(file-scope only). Not exported from the postgres binary; not
linkable from a loadable extension.

### 3.2 Even if they were exported, they write to a libpq socket we don't own

They all use `CreateDestReceiver(whereToSendOutput)` →
`DestRemote` → `printtup_*` → `pq_putmessage` → `MyProcPort->sock`.
The bgworker we run inside has no `MyProcPort` — we own the client
fd ourselves and speak [pgwire crate](https://crates.io/crates/pgwire)
framing on top of it. Redirecting `pq_putmessage` per-bgworker would
require patching every `pq_*` site, i.e. forking PG. Not an option.

### 3.3 And `PostgresMain` itself — although exported — is also unusable

```text
$ nm -D ~/.pgrx/18.4/pgrx-install/bin/postgres |
    grep " T PostgresMain\>"
00000000005801b0 T PostgresMain
```

It's a per-process owner: assumes `MyProcPort` is connected, installs
the sigsetjmp-based ERROR recovery for the entire process, drives
the message loop, and only returns on disconnect / `proc_exit`. We
can't enter and leave it per query.

## 4. What is callable from the postgres binary

The layer *below* the dispatchers — the parser, analyzer, planner,
portal lifecycle, plan cache, and destination receivers — is all
exported and stable PG-server API:

| Symbol | Use |
|---|---|
| `pg_parse_query` | raw → `List* RawStmt*` |
| `pg_analyze_and_rewrite_fixedparams` | RawStmt + known param types → analyzed `Query*` |
| `pg_analyze_and_rewrite_varparams` | RawStmt with inferred params (used by [`extended::infer_param_types`](../../../crates/core/src/backend/extended.rs)) |
| `pg_analyze_and_rewrite_withcb` | RawStmt + parserSetup callback |
| `pg_plan_queries` | `List* Query*` → `List* PlannedStmt*` |
| `CreatePortal` / `PortalDefineQuery` / `PortalStart` / `PortalRun` / `PortalDrop` | per-statement execution lifecycle |
| `CreateCachedPlan` / `CompleteCachedPlan` / `SaveCachedPlan` / `GetCachedPlan` / `ReleaseCachedPlan` | extended-query plan caching |
| `CreateDestReceiver(DestNone)` | drops all rows (useful for utility statements that don't return tuples) |
| `CreateDestReceiver(DestTuplestore)` + `SetTuplestoreDestReceiverParams` | materialise rows into a `Tuplestore` (in-memory + spill-to-disk; what `dblink` and SRFs use) |
| `tuplestore_begin_heap` / `tuplestore_gettupleslot` / `tuplestore_end` | iterate the tuplestore back |
| Any custom `DestReceiver` we define ourselves | direct-to-wire row encoding (see §6.2) |

The reusable surface is roughly: everything *except* the
protocol-message I/O. The dispatchers' job — and what we'd have to
re-implement in Rust — is the glue between protocol message and
this lower layer.

## 5. The parse-pass tax (what would actually be saved)

Comparison of parse passes per wire message:

| Stage | Default PG | pg_transport (SPI) | Direct-path |
|---|---|---|---|
| Simple query `'Q'` | 1 | 2 (raw_parser + SPI's inner) | 1 |
| Extended `'P'` | 1 | 2 (varparams pre-pass + `SPI_prepare`) | 1 |
| Extended `'E'` | 0 (cached plan) | 0 (cached plan via `SPI_execute_plan`) | 0 |

The cost of one extra `pg_parse_query` call is **~5 µs** per Q25's
measurement (PG's bison parser is fast; the gap to default-PG is
small). At 0.99x bench parity, the second-parse cost is below
measurement noise.

Beyond the parse pass, the direct path also saves the intermediate
`SPI_tuptable` materialisation (executor → SPI tuptable → our wire
buffer becomes executor → wire buffer if we use a custom
`DestReceiver`). Hard to quantify without implementation; not
expected to be material for the typical 1–100 row result sizes the
benchmark covers.

## 6. Three reuse strategies

Ranked from most-reuse / least-new-code to most-control / most-new-code:

### 6.1 Strategy 1 — Tuplestore destination

`CreateDestReceiver(DestTuplestore)` + `SetTuplestoreDestReceiverParams`
gives us a destination that materialises into a `Tuplestore`. After
`PortalRun`, we walk the tuplestore via `tuplestore_gettupleslot`,
calling the existing [`TypeOutput` / `TypeSend`](../../../crates/core/src/backend/spi.rs)
wrappers per column.

```rust
let portal = create_portal_anonymous()?;
portal_define_query(portal, q_slice, command_tag, plantree_list);
portal_start(portal)?;
let tupstore = TuplestoreReceiver::new(work_mem);
portal_run(portal, fetch_all, &tupstore.dest_receiver())?;
for slot in tupstore.iter() {
    // existing per-column encoding via TypeOutput / TypeSend
}
portal_drop(portal);
```

**New unsafe FFI:** ~80 LOC — `tuplestore_begin_heap`,
`SetTuplestoreDestReceiverParams`, `tuplestore_gettupleslot`,
`slot_getallattrs`, `tuplestore_end`, plus the `CreatePortal` /
`PortalDefineQuery` / `PortalStart` / `PortalRun` / `PortalDrop`
chain wrapped as RAII like [`SpiPlan`](../../../crates/core/src/backend/spi.rs).

**Cost vs custom DestReceiver:** one extra row-copy (executor →
tuplestore → wire buffer).

**Cost vs current SPI:** roughly equivalent (SPI itself materialises
into `SPI_tuptable`, so it's also one copy).

**Win:** single parse instead of double parse, +
`handle_xact_control` / `XactCmd` go away because BEGIN/COMMIT/
ROLLBACK route naturally via `PortalRun` → `ProcessUtility` →
`BeginTransactionBlock` etc.

**PG version coupling:** medium (arguably low). `Tuplestore` ABI is
stable but non-trivial; `PortalDefineQuery` signature changed in
PG 15. Realistic adaptation cost per PG major: ~10–50 LOC of
`#[cfg(feature = "pg19")]` signature shims.

**Production precedent:** every set-returning function in PG uses
this pattern. Concrete users in the ecosystem:
[`dblink`](https://www.postgresql.org/docs/current/dblink.html) and
[`postgres_fdw`](https://www.postgresql.org/docs/current/postgres-fdw.html)
in contrib; `plpgsql` refcursors; TimescaleDB continuous
aggregates. This is the gold-standard PG extension pattern
— not novel risk.

### 6.2 Strategy 2 — Custom `DestReceiver`

> **Status: deferred to Stage D.** [§7](#7-phased-adoption-if-and-when-we-un-defer)
> commits to Strategy 1 (Tuplestore) for the initial §3.1 adoption
> (Stages A + B). DestReceiver is the perf ceiling but its win is
> not material for v0 workloads (1–100 row results); the doc's
> Stage D promotes only when a profile or feature demands it (see
> [§8](#8-re-entry-conditions) feature-driven triggers).

Implement a `#[repr(C)]` Rust struct that prepends `DestReceiver`'s
C function-table layout and appends our `BytesMut` + per-column
encoders. The `receiveSlot` callback walks the slot's columns and
writes encoded bytes straight into the wire buffer. Zero
intermediate copies.

```rust
#[repr(C)]
struct WireDest {
    base: pg_sys::DestReceiver,   // (*receiveSlot, *rStartup, *rShutdown, *rDestroy, mydest)
    buf: *mut BytesMut,
    encoders: *const [ColumnEncoder],
    // ...
}

unsafe extern "C-unwind" fn receive_slot(
    slot: *mut pg_sys::TupleTableSlot,
    self_: *mut pg_sys::DestReceiver,
) -> bool {
    let dest = self_ as *mut WireDest;
    let buf = &mut *(*dest).buf;
    pg_sys::slot_getallattrs(slot);
    let nattrs = (*slot).tts_nvalid as usize;
    for c in 0..nattrs {
        let datum = *(*slot).tts_values.add(c);
        let is_null = *(*slot).tts_isnull.add(c);
        if is_null {
            buf.put_i32(-1);
        } else {
            let bytes = (*(*dest).encoders.add(c)).encode(datum);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(&bytes);
        }
    }
    true
}
```

**New unsafe FFI:** ~150 LOC — the DestReceiver function table +
portal-lifecycle wrappers + `slot_getallattrs` per-slot decoder.

**Cost:** more unsafe surface; tied to the `DestReceiver` ABI
(stable across PG versions but version-coupled to `TupleTableSlot`
layout).

**Win:** single parse + zero intermediate row copies. Matches `psql
-c` performance by construction. Plus all the `handle_xact_control`
removal wins from Strategy 1.

**PG version coupling:** high. `TupleTableSlot` got significantly
reworked in PG 12 (TTS_VIRTUAL / TTS_HEAP / TTS_MINIMAL etc.) and is
the kind of struct PG occasionally touches.

**Production precedent:** [Citus](https://github.com/citusdata/citus)
implements its own `TupleDestination` abstraction layered over
`DestReceiver` ([`src/include/distributed/tuple_destination.h`](https://github.com/citusdata/citus/blob/main/src/include/distributed/tuple_destination.h))
and has tracked it through PG 11 → 18 in production. Other users:
[PipelineDB](https://github.com/pipelinedb/pipelinedb) (continuous
queries; archived but shipped in production for years across
PG 9.6 → 11), [pg_task](https://github.com/RekGRpth/pg_task),
[Spock](https://github.com/pgEdge/spock). Pattern that survives
the coupling: wrap the raw function-table struct in a higher-level
abstraction, use `#[cfg(feature = "pgNN")]` shims for signature
changes, keep the `receiveSlot` callback minimal (forward to the
abstraction; don't inline slot-internal logic). If Citus can carry
this through 5+ PG majors, so can we.

### 6.3 Strategy 3 — Keep SPI

Current state. Double parse on the SPI side, intermediate
`SPI_tuptable` materialisation, but the code is clean post-`spi.rs`
refactor and bench is 0.99x parity.

**New unsafe FFI:** none.

**Cost:** ~5 µs extra parse per `'Q'` / `'P'`; one extra row copy
per result row through `SPI_tuptable`.

**Win:** stable SPI API absorbs PG-internals churn (when PG 19
moves `TupleTableSlot` fields around, SPI's wrapper absorbs it for
us); minimum unsafe surface; the `spi.rs` wrappers already provide
RAII for plans, sessions, and per-type I/O.

## 7. Phased adoption (if and when we un-defer)

**Decision: Strategy 1 (Tuplestore) is the chosen implementation
for the initial §3.1 adoption.** Stages A + B both land Tuplestore-
based bridges; Strategy 2 (custom DestReceiver) is deferred to
Stage D and promoted only when a profile or feature demands it.
Rationale: the perf delta on v0 workloads (1–100 row results) is
noise; Tuplestore is ~half the unsafe LOC of DestReceiver; the
FFI surface (CachedPlan + Portal lifecycle + slot iteration) is
identical between the two, so the Stage A work invested in
Tuplestore carries forward to a Stage D promotion at zero extra
cost. See [§6.1](#61-strategy-1--tuplestore-destination) /
[§6.2](#62-strategy-2--custom-destreceiver) for the strategy
comparison and [§8](#8-re-entry-conditions) for the Stage D
promotion triggers.

A clean staircase that lets each step land and stabilise before the
next:

1. **Stage A — extended-only direct path (Strategy 1).** Replace
   `SPI_prepare` / `SPI_execute_plan` in [`extended.rs`](../../../crates/core/src/backend/extended.rs)
   with `CreateCachedPlan` / `GetCachedPlan` / `Portal*` + tuplestore
   destination. Validates the cached-plan + portal-lifecycle FFI on a
   single-statement-only surface. The `infer_param_types` varparams
   pre-pass we already do can feed `CreateCachedPlan` directly,
   eliminating the second analyze pass too. **~150 LOC new unsafe;
   removes ~50 LOC SPI-specific code.**

2. **Stage B — simple-query direct path (Strategy 1).** Apply the
   same pattern to [`spi_bridge.rs`](../../../crates/core/src/backend/spi_bridge.rs).
   Reuses the tuplestore + portal-lifecycle wrappers from Stage A.
   **Removes** `handle_xact_control`, `XactCmd`, `command_tag_from_rc`,
   and `parse_and_classify`'s classification logic (still needs the
   per-statement span list, just not the `TransactionStmt` detection).
   **~80 LOC new unsafe; removes ~200 LOC SPI-specific code.** Net
   code reduction.

3. **Stage C — `spi.rs` cleanup.** Rename to `pg.rs` or `executor.rs`;
   `SpiPlan` → `CachedPlan`; `with_spi` becomes `with_xact` (the
   `SPI_connect` / `SPI_finish` bracket goes away because we no
   longer use SPI at all). The `SpiTuples` / `TypeInput` /
   `TypeReceive` wrappers stay (still used for param decoding +
   tuplestore iteration). **Mostly renames; no behaviour change.**

4. **Stage D — promote to Strategy 2.** If the tuplestore copy shows
   up in profiles (it shouldn't for typical workloads), swap the
   tuplestore destination for a custom `DestReceiver`. Per-bridge
   change, isolated to the executor-output side. **~70 LOC new
   unsafe; removes ~30 LOC tuplestore plumbing.**

Each stage is independently shippable and validated by the existing
`just test` + `just e2e` + `just bench` suite.

### 7.5 Coexistence during migration — runtime GUC

The Stage A → C transition replaces SPI with the direct path on a
file-by-file basis. To de-risk that migration, **both backends
should coexist during Stages A+B, gated by a runtime GUC**, with an
explicit sunset that retires SPI at Stage C.

**GUC.** `pg_transport.execution_backend = 'spi' | 'direct'`,
`USERSET`, defaults to `'spi'` until soak completes. Per-session and
per-role (`ALTER ROLE ... SET pg_transport.execution_backend =
'direct'`) selectability lets operators canary one workload at a
time without redeploying.

**Dispatch shape.** A small trait at the
[`SimpleQueryHandler`](../../../crates/core/src/wire/pgwire_v3.rs) /
[`ExtendedQueryHandler`](../../../crates/core/src/wire/extended.rs)
boundary; the existing `spi_bridge.rs` / `extended.rs` become one
impl, the new direct-path code becomes the other. The dispatcher
reads the GUC at query start (one indirect call per query;
measurable as zero on the bench harness).

**Migration timeline.**

| Stage | Default backend | What's available |
| --- | --- | --- |
| A landed | `spi` | `direct` available for extended-query opt-in |
| B landed | `spi` | `direct` available for both simple + extended |
| Soak (~6 months, 2–3 PG minor cycles) | `spi` | operators canary `direct` per-session / per-role; bug + perf reports drive iteration |
| Soak passes | flip default to `direct` | SPI still selectable; deprecation notice in [`configuration.md`](../configuration.md) |
| Sunset (one cycle after default flip) | `direct` only | Stage C runs: delete SPI bridge + dispatcher trait + GUC |

**Cost during soak.** ~20–50 LOC for the dispatcher trait + GUC
plumbing. CI runs `just test` + `just e2e` against both backends
(doubled bench cost; same e2e cost since tests pass against either).
Both code paths stay green or the soak resets.

**Why not the alternatives.**

- **Cargo feature flag** (compile-time selection) denies us the
  runtime canary capability that's the whole point. Considered and
  rejected unless binary-size becomes a constraint (it won't for v0).
- **Permanent coexistence** (no sunset) carries the maintenance tax
  of two query backends forever — every new feature (cancel, COPY,
  new auth method) must work against both. The sunset is part of
  the decision; without it this turns into a maintenance hole.
- **Per-query routing** (SPI for utility, direct for SELECT) is
  more complex than the win justifies; the GUC lets operators
  effectively do this themselves via `ALTER ROLE` if they want.

**Open until Stage A is actually scoped.** Concrete decisions
deferred to that point: exact GUC name (`execution_backend` vs
`query_path` vs ...); whether the GUC is `SUSET` (admin-only) or
`USERSET` (any session); how to log per-query backend selection
without bloating the log; whether to expose a
`pg_stat_pg_transport_backend` view counting queries by backend.
These are easy decisions to make once the trait shape exists; not
worth pre-locking now.

## 8. Re-entry conditions

Un-defer when **any** of the following is true:

- **Bench-driven.** Phase-5 bench harness shows >10% qps gap to
  vanilla PG on a representative workload, attributable to SPI
  overhead (parse cost or tuptable materialisation). Current
  numbers (0.84x – 1.04x range across phases) don't trigger this.
- **Feature-driven.** A v0+ feature needs functionality SPI doesn't
  expose. Two known candidates: streaming-large-result via cursor
  (Strategy 1 + Strategy 2 both support `PortalRun` with
  `max_rows < SQL_MAX_VAL_REQUEST`; SPI's wrapper materialises
  everything regardless), and per-column streaming for COPY OUT
  (DestReceiver is the natural surface; SPI's flow doesn't fit).
- **Maintenance-driven.** SPI changes in a future PG version in a
  way that breaks our wrapping more severely than the direct path
  would. Unlikely — SPI is one of PG's most stable APIs.

## 9. References

- [Q18 in roadmap.md](../roadmap.md#23-resolved) — original SPI-vs-direct decision.
- [Q25 in roadmap.md](../roadmap.md#23-resolved) — parse-once vs parse-twice resolution for simple-query (raw_parser + SPI's inner parse).
- [Q27 in roadmap.md](../roadmap.md#23-resolved) — extended-query parameter inference path (the `pg_analyze_and_rewrite_varparams` pre-pass).
- [backend-wire.md §6](../backend-wire.md) — SPI bridge.
- [backend-wire.md §8 Q1](../backend-wire.md#8-open-questions) — extended-query state ownership (resolved by phase 9).
- PG source: [`src/backend/tcop/postgres.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c) — the `exec_*` dispatchers (all `static`).
- PG source: [`src/backend/utils/cache/plancache.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/cache/plancache.c) — `CreateCachedPlan` / `CompleteCachedPlan` / `GetCachedPlan`.
- PG source: [`src/backend/tcop/dest.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/dest.c), [`src/backend/access/common/printtup.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/common/printtup.c) — `DestReceiver` API + `printtup_*` (`DestRemote` implementation).
- PG source: [`src/backend/utils/sort/tuplestore.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplestore.c) — tuplestore destination.
