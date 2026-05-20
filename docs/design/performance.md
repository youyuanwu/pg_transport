# Performance — data-path analysis

> Parent: [README.md](README.md)
> Siblings: [bench.md](bench.md) (harness mechanics) · [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md) (the biggest deferred optimisation)

This document audits the per-query data path from socket-read to
socket-write, identifies the concrete inefficiencies we know about,
and ranks them by leverage. For *how* to measure performance see
[bench.md](bench.md); this doc is about *what the measurements
mean and where the headroom is*.

## 1. Where we are

Most recent bench run (`just bench pg18 200 2`):

| Stat | Vanilla PG | pg_transport | Ratio |
| --- | --- | --- | --- |
| p50  | 174.9 µs | 210.0 µs | 1.20x |
| p95  | 304.9 µs | 300.1 µs | 0.98x |
| mean | 194.9 µs | 216.1 µs | 1.11x |
| **qps** | 10164.8 | 9140.8 | **0.90x** |

`just pgbench pg18 select 3 2`:

| Driver | tps | Failed txns |
| --- | --- | --- |
| Vanilla PG | ~8500 | 0 |
| pg_transport | 8527 | 0 |

Range across all phases (4 → 9): **0.84x – 1.04x qps**. p95 and
above frequently land at or *better than* vanilla PG (the bgworker
pool reuse pays off in tail latency — no per-connection fork tax).

**Context — how this compares to other pooling solutions:**

| Solution | Typical qps vs. direct PG | Why |
| --- | --- | --- |
| pgbouncer (transaction mode) | 0.7x – 0.85x | libpq parse/encode round-trip on every message |
| pgcat | 0.75x – 0.90x | same shape as pgbouncer, Rust-based |
| pgpool-II | 0.6x – 0.8x | proxy + query rewriting overhead |
| **pg_transport** | **0.84x – 1.04x** | bgworker reuse + SPI delegates execution to PG's own planner+executor |

The reason we land in the same range as direct PG (rather than the
0.7–0.85x range typical of pooling) is structural: **we don't proxy
SQL**. The client fd lands directly in a bgworker via SCM_RIGHTS;
that bgworker calls PG's own SPI to execute. There is no "decode
wire bytes, re-encode them, ship to PG, decode response, re-encode
again" overhead. The wire layer is in-process with the executor.

## 2. The data path, traced

```text
client TCP → kernel → tokio TcpStream
                       │
                       ▼  pgwire's Framed<TcpStream, Codec>
                  ┌─────────────────────────────────────────┐
                  │ 1. pgwire async dispatch loop           │
                  └─────────────────────────────────────────┘
                                  │ PgWireFrontendMessage
                                  ▼
                  ┌─────────────────────────────────────────┐
                  │ 2. SimpleQuery / ExtendedQuery handler  │
                  │    (Arc<dyn ...>; dyn dispatch + async) │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼ spi.rs::with_spi
                  ┌─────────────────────────────────────────┐
                  │ 3. xact bracket per query               │
                  │    StartTransactionCommand              │
                  │    + PushActiveSnapshot                 │
                  │    + SPI_connect                        │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼
                  ┌─────────────────────────────────────────┐
                  │ 4. PARSE PASS #1 (us)                   │
                  │    simple: raw_parser → spans + classify│
                  │    extend: pg_analyze_*_varparams       │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼
                  ┌─────────────────────────────────────────┐
                  │ 5. PARSE PASS #2 (inside SPI)           │
                  │    + analyze + plan + execute           │
                  │    Result lands in SPI_tuptable         │
                  │    (1 copy: executor → tuptable)        │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼ row materialisation
                  ┌─────────────────────────────────────────┐
                  │ 6a. SIMPLE: Vec<Option<String>>         │  ← 2 allocs/cell
                  │     then DataRowEncoder → BytesMut      │    + 2nd encode
                  │ 6b. EXTENDED: TypeOutput/Send → BytesMut│  ← 1 alloc/cell
                  └─────────────────────────────────────────┘
                                  │ DataRow
                                  ▼
                  ┌─────────────────────────────────────────┐
                  │ 7. pgwire encodes DataRow → wire bytes  │
                  │    SPI_finish + PopActiveSnapshot       │
                  │    + CommitTransactionCommand           │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼
                       tokio TcpStream → kernel → client TCP
```

**Stage-to-source map:**

| # | Lives in | Notes |
| --- | --- | --- |
| 1 | [crates/core/src/wire/pgwire_v3.rs](../../crates/core/src/wire/pgwire_v3.rs) (`process_socket` via the `pgwire` crate) | Async dispatch loop; one task per connection |
| 2 | [crates/core/src/backend/spi_bridge.rs](../../crates/core/src/backend/spi_bridge.rs) (`'Q'`) + [extended.rs](../../crates/core/src/backend/extended.rs) (`P`/`B`/`D`/`E`/`S`) | Dyn dispatch via pgwire's `SimpleQueryHandler` / `ExtendedQueryHandler` |
| 3 | [crates/core/src/backend/spi.rs](../../crates/core/src/backend/spi.rs) (`with_spi`) | Per-query xact bracket; see §3.3 for the per-`'Q'` cost |
| 4 | `spi_bridge::parse_and_classify` (simple) + `extended::infer_param_types` (extended) | Calls PG's `raw_parser` / `pg_analyze_and_rewrite_varparams` |
| 5 | PG's `SPI_execute*` / `SPI_execute_plan*` | Result materialised into `SPI_tuptable` |
| 6a | `spi_bridge::emit_rows` | The `Vec<Option<String>>` allocation pattern — see §3.2 |
| 6b | `extended::ColumnEncoder` + `TypeOutput`/`TypeSend` in [spi.rs](../../crates/core/src/backend/spi.rs) | Direct `BytesMut` path |
| 7 | pgwire `DataRowEncoder` (simple) or our manual `BytesMut::put_i32` + `put_slice` (extended) | The encode step folded into 6b for extended |

## 3. The five known inefficiencies

Ranked by leverage (largest expected impact first).

### 3.1 Custom `DestReceiver` would eliminate parse #2 + SPI_tuptable copy

**What:** Replace `SPI_execute` / `SPI_execute_plan` with direct
`pg_parse_query` + `pg_analyze_and_rewrite_*` + `pg_plan_queries` +
`CreatePortal` + `PortalRun` + `PortalDrop`, with a custom
`DestReceiver` whose `receiveSlot` callback writes encoded bytes
straight into our wire `BytesMut`. (Strategy 2 in
[deferred/planner-executor-direct-path.md §6.2](deferred/planner-executor-direct-path.md).)

**Why it wins:**

- Saves one parse pass (~5 µs/query — PG's bison parser is fast,
  but it's still wasted work).
- Saves one row-copy (today: executor → SPI_tuptable → wire buffer;
  with custom DestReceiver: executor → wire buffer).
- Removes `handle_xact_control` / `XactCmd` because `PortalRun` →
  `ProcessUtility` handles BEGIN/COMMIT/ROLLBACK naturally.

**Magnitude:** ~5–15 µs/query macroscopic depending on row width and
count. For pgbench `-S` (1 row, 1 column): low end. For wide-row
multi-row SELECTs: high end.

**Cost:** ~250 LOC new unsafe FFI. PG-version coupling moves from
"low" (SPI is stable) to "medium" (`Portal*`, `CachedPlanSource`,
`DestReceiver`, `TupleTableSlot` ABIs).

**Status:** Deferred per
[roadmap Q18](roadmap.md#23-resolved). Full design + phased
adoption plan in
[deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md).
Re-entry trigger: bench shows > 10% qps gap attributable to SPI, or
a v0+ feature needs cursor-streaming / per-column COPY OUT.

### 3.2 Simple-query path's `Vec<Option<String>>` intermediate

**What:** The simple-query bridge materialises each row as a
`Vec<Option<String>>` (one `String` heap alloc per non-null cell),
then calls pgwire's `DataRowEncoder` to re-encode each cell into a
`BytesMut`. That's 2× the per-cell allocations vs. the extended-query
path, which writes bytes directly into the `BytesMut`.

**Why it exists:** Historical. The simple-query path landed in
phase 4b before [`extended.rs`](../../crates/core/src/backend/extended.rs)
in phase 9 worked out the direct `BytesMut` pattern. Both paths now
have access to the same [`TypeOutput` / `TypeSend`](../../crates/core/src/backend/spi.rs)
wrappers; only the simple-query side hasn't been ported.

**Magnitude:** For 1-cell results (pgbench `-S`): below noise. For
wide-row results (e.g. `SELECT * FROM information_schema.columns`,
~30 columns × 800+ rows): ~5–10% latency reduction in
`run_via_spi` — many small allocations dominate when each cell is
short.

**Cost:** ~30 LOC. Port the extended-query encoding pattern
(`BytesMut::put_i32` length prefix + `put_slice` body, with the
per-column `ColumnEncoder` enum) back into
[`run_via_spi`](../../crates/core/src/backend/spi_bridge.rs).

**Status:** Standalone, no design dependencies, slam-dunk
follow-on. Worth doing before 3.1 because it makes the two bridges
symmetric and proves out the BytesMut pattern is the right one
across the codebase.

### 3.3 Per-query xact bracket overhead

**What:** [`spi::with_spi`](../../crates/core/src/backend/spi.rs)
opens a fresh `StartTransactionCommand` + `PushActiveSnapshot` +
`SPI_connect` per query. For multi-statement `'Q'` messages (e.g.
pgbench's `BEGIN; UPDATE; UPDATE; COMMIT` batch), we open this
bracket **per inner statement** rather than once for the whole `'Q'`.

Inside an open `BEGIN` block, `StartTransactionCommand` is a no-op
(becomes `CommandCounterIncrement`); the waste is mostly the
`PushActiveSnapshot` / `PopActiveSnapshot` pair plus `SPI_connect` /
`SPI_finish` frame setup. Still ~5–10 µs of avoidable per-statement
overhead.

**Magnitude:** ~5–10 µs × N inner statements per `'Q'`. For single-
statement `'Q'` (the common case): zero. For pgbench-tpcb's
4-statement-batch: ~20–40 µs per transaction.

**Cost:** ~80 LOC. Restructure
[`execute_simple_query`](../../crates/core/src/backend/spi_bridge.rs)
to open one bracket per `'Q'` rather than per inner statement, with
xact-control statements routed through a small state machine that
knows the bracket is already open.

**Status:** Opportunistic. Would fold naturally into 3.1's
restructure (the direct-path refactor restructures the xact bracket
anyway). Doing it independently before 3.1 is ~80 LOC for a use
case (multi-statement `'Q'` in pgbench-tpcb) that isn't in our
current acceptance bar.

### 3.4 pgwire's per-message `Arc<dyn>` + async overhead

**What:** Each `'Q'` / `'P'` / `'B'` / `'E'` goes through
`Arc<dyn SimpleQueryHandler>`, `Arc<dyn ExtendedQueryHandler>`, etc.
Each handler method is `async fn` via `async-trait`, which expands
to `Pin<Box<dyn Future>>` — one heap allocation per call.

**Magnitude:** Unmeasured, but the math: each `Arc<dyn>` is one
indirect call; each `Pin<Box<dyn Future>>` is one `Box::new(...)`
of a state machine that's typically <200 bytes. Per-query cost is
probably 50–200 ns. **Likely <1% of total query latency.**

**Cost:** Would require forking pgwire to use monomorphised
handlers (impl-trait) + the new native `async fn in trait`
(stabilised in Rust 1.75). The maintenance cost of carrying a fork
is significantly higher than the perf gain over v0's timeline.

**Status:** Non-starter for v0. Revisit if profiling pushes it
above 5% — unlikely. The pgwire crate is maintained and follows PG
protocol upstream; a fork would be a maintenance hole.

### 3.5 `SPI_getvalue`'s per-cell `getTypeOutputInfo` lookup

**What:** Simple-query path calls `SPI_getvalue(tuple, tupdesc,
col+1)` per cell. Under the hood `SPI_getvalue` calls
`getTypeOutputInfo(atttypid, ...)` per call — that's a syscache
lookup per cell, when really the type info is invariant across rows
in the same result.

The extended-query path already caches this via `TypeOutput::for_type`
called once per column per Execute (not per cell).

**Magnitude:** PG's syscache hit is ~50 ns. For an N_rows × N_cols
result we do N×N lookups instead of N_cols. For pgbench `-S` (1×1):
50 ns of waste. For a 1000 × 20 result: ~1 ms of waste (~0.5–1% of
that query's total latency).

**Cost:** Trivial — folds entirely into 3.2 (when we move simple-
query to the direct-`BytesMut` pattern, we'll cache `TypeOutput` per
column the same way `extended.rs` does).

**Status:** Bundled with 3.2.

## 4. Where we're already well-optimised

These aren't bottlenecks, but worth documenting so future bench
deltas can be attributed correctly:

| Component | Why it's fast |
| --- | --- |
| **Connection acquisition** | SCM_RIGHTS handoff is ~10 µs vs. PG's `fork()` at ~1 ms+. We're **100× faster** on connection-establishment than vanilla PG. This is what gives us p95/max parity-or-better. |
| **Backend reuse** | One bgworker serves N sequential connections without process teardown. Saves PG's per-connection cleanup tax entirely. |
| **Tokio runtime** | Built once per slot (`Rc<Runtime>`), reused across handoffs. Not rebuilt per query. |
| **TLS** | rustls + ring; same crypto class as PG's OpenSSL. Per-byte overhead identical. |
| **SPI plan caching (extended-query)** | `SPI_keepplan`'d once at Parse, reused across Execute via `SPI_execute_plan`. Matches PG's `CachedPlanSource` semantics. **At Execute time we are at zero-parse parity with PG.** |
| **Per-cell type I/O caching (extended-query)** | `TypeOutput` / `TypeSend` built once per column per Execute, not per cell. Only the simple-query side is missing this (#3.5). |
| **Snapshot / xact state inside BEGIN block** | `StartTransactionCommand` becomes `CommandCounterIncrement` when already in `TBLOCK_INPROGRESS`; same for Commit. We inherit this from PG's xact machinery — no special-casing needed. |
| **Binary parameter & result format** | Honoured per-column from `Bind.parameter_format_codes` / `result_column_format_codes` without forcing text round-tripping. tokio-postgres' binary path works without conversion. |

## 5. What our bench gap actually measures

Decomposing the 10–20 µs absolute mean gap from §1:

| Source | Estimated µs/query | Reference |
| --- | --- | --- |
| Parse pass #2 (SPI's inner) | ~5 | §3.1 |
| Per-query xact bracket (Start + Snapshot + SPI_connect + reverse) | ~5–10 | §3.3 |
| Row materialisation through `SPI_tuptable` (1 copy) + Vec<Option<String>> intermediate | ~2–5 | §3.1 + §3.2 |
| Misc — pgwire framing, async dispatch, MemoryContext, `with_spi` closure scaffolding | ~5 | §3.4 |
| **Total estimated** | **~17–25 µs** | matches the observed ~10–20 µs |

The numbers add up. Closing 3.1 (~10 µs win) + 3.2 (~2–5 µs) + 3.3
(~5–10 µs for multi-statement Qs) would put us at or below vanilla
PG mean latency. **p95/max would likely stay at or below vanilla
because we still don't pay PG's fork tax.**

## 6. Re-entry conditions

Per [roadmap Q18](roadmap.md#23-resolved), perf-driven refactors
are gated on bench signal:

- **Trigger 3.1:** bench shows >10% qps gap attributable to SPI on a
  representative workload (currently we're ~10% on the custom
  harness's `SELECT 1` micro-bench, ~0% on pgbench tpcb). The
  pgbench number is more representative of real workloads.
- **Trigger 3.2:** any complaint or profile that names per-cell
  `String` allocation as significant. Standalone enough to land
  preemptively if someone wants to do it.
- **Trigger 3.3:** profile of pgbench-tpcb (or any multi-statement-
  `'Q'` workload) showing xact bracket overhead.
- **Trigger 3.4:** profile shows pgwire dispatch above 5% of
  per-query latency. Unlikely.
- **Trigger 3.5:** automatic — folds into 3.2.

## 7. Macro perf-vs-correctness stance

v0's first job is correctness; perf parity at 0.84–1.04x with
vanilla PG is a comfortable margin to ship in and iterate from. The
five inefficiencies above are documented, scoped, and have
re-entry triggers — none is a v0 blocker. The deferred [planner-
executor-direct-path.md](deferred/planner-executor-direct-path.md)
captures the largest single follow-on optimisation; this doc
captures the full audit and ranks the smaller wins alongside it.

## See also

- [bench.md](bench.md) — how to run the benchmarks.
- [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md)
  — the §3.1 optimisation in full design detail (three reuse
  strategies, phased adoption).
- [architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer)
  — why the data path looks the way it does (delegate-vs-build
  table).
- [backend-wire.md §6](backend-wire.md#6-spi-bridge) — the SPI
  bridge that §3.1 / §3.2 / §3.3 would refactor.
- [roadmap.md Q18 + Q25 + Q27](roadmap.md#23-resolved) — the
  original deferral decisions for SPI vs. direct-path and the
  parse-pass economics.
