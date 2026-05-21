# Performance — data-path analysis

> Parent: [README.md](README.md)
> Siblings: [bench.md](bench.md) (harness mechanics) · [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md) (the biggest deferred optimisation)

This document audits the per-query data path from socket-read to
socket-write, identifies the concrete inefficiencies we know about,
and ranks them by leverage. For *how* to measure performance see
[bench.md](bench.md); this doc is about *what the measurements
mean and where the headroom is*.

## 1. Where we are

Most recent stable custom-bench sweep (`just bench pg18 20000 8 "" "" {spi|direct}`):

| Mode | Stat | Vanilla PG | pg_transport | Ratio |
| --- | --- | --- | --- | --- |
| `spi` | p50  | 425.2 µs | 376.7 µs | 0.89x |
| `spi` | p95  | 559.9 µs | 493.2 µs | 0.88x |
| `spi` | p99  | 623.4 µs | 550.7 µs | 0.88x |
| `spi` | mean | 430.4 µs | 383.1 µs | 0.89x |
| `spi` | **qps** | 18496.7 | 20794.0 | **1.12x** |
| `direct` | p50  | 435.2 µs | 386.1 µs | 0.89x |
| `direct` | p95  | 561.3 µs | 505.2 µs | 0.90x |
| `direct` | p99  | — | — | — |
| `direct` | mean | 438.8 µs | 393.2 µs | 0.90x |
| `direct` | **qps** | 18146.8 | 20236.7 | **1.12x** |

Most recent stable pgbench select (`just pgbench pg18 select 30 8 "" {spi|direct}`):

- `backend=spi`: vanilla 27069.7 tps / 0.296 ms, pg_transport 25138.7 tps / 0.318 ms (0.93x tps).
- `backend=direct`: vanilla 26641.6 tps / 0.300 ms, pg_transport 25111.4 tps / 0.319 ms (0.94x tps).

Most recent stable pgbench nupdate (`just pgbench pg18 nupdate 30 8 "" {spi|direct}`):

- `backend=spi`: vanilla 6756.2 tps / 1.184 ms, pg_transport 6310.0 tps / 1.268 ms (0.93x tps).
- `backend=direct`: vanilla 6332.8 tps / 1.263 ms, pg_transport 6346.8 tps / 1.260 ms (1.00x tps, parity).

Latest recorded `just pgbench pg18 tpcb 30 8` (TPC-B-like, 4 UPDATEs + SELECT + INSERT
per multi-statement `'Q'`): vanilla PG 1884 tps / 4.25 ms avg latency,
pg_transport 2060 tps / 3.88 ms (**1.09x qps**, 0.91x latency). This
is the workload [§3.3](#33-per-query-xact-bracket-overhead) targets;
we lead by ~9%, so §3.3's re-entry trigger is not met.

**Pre-§3.2 baseline for comparison** (`just bench pg18 200 2`,
recorded before the simple-query BytesMut direct path landed):

| Stat | Vanilla PG | pg_transport | Ratio |
| --- | --- | --- | --- |
| p50  | 174.9 µs | 210.0 µs | 1.20x |
| p95  | 304.9 µs | 300.1 µs | 0.98x |
| mean | 194.9 µs | 216.1 µs | 1.11x |
| **qps** | 10164.8 | 9140.8 | **0.90x** |

The §3.2 port (~30 LOC, eliminating the `Vec<Option<String>>`
intermediate + pgwire `DataRowEncoder` re-encode pass on the
simple-query path) is responsible for a **~20 percentage point qps
swing** on the custom harness — we now run faster than vanilla PG
on this workload. p50/mean win by ~10% each.

Range across recent stable sweeps: **0.93x – 1.13x qps**. On the
custom harness, tail latencies (p95 / p99 / max) tend to land at or
better than vanilla PG — the bgworker pool reuse pays off there (no
per-connection fork tax).


**Context — how this compares to other pooling solutions:**

| Solution | Typical qps vs. direct PG | Why |
| --- | --- | --- |
| pgbouncer (transaction mode) | 0.7x – 0.85x | libpq parse/encode round-trip on every message |
| pgcat | 0.75x – 0.90x | same shape as pgbouncer, Rust-based |
| pgpool-II | 0.6x – 0.8x | proxy + query rewriting overhead |
| **pg_transport** | **0.93x – 1.13x** | bgworker reuse + in-process execution path (SPI and direct/WireDestReceiver modes) |

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

### 3.1 Custom `DestReceiver` eliminates SPI_tuptable copy — ~~deferred~~ **SHIPPED**

**What was the plan:** Replace `SPI_execute` / `SPI_execute_plan` with direct
`pg_parse_query` + `pg_analyze_and_rewrite_*` + `pg_plan_queries` +
`CreatePortal` + `PortalRun` + `PortalDrop`, with a custom
`DestReceiver` whose `receiveSlot` callback writes encoded bytes
straight into our wire `BytesMut`. (Strategy 2 in
[deferred/planner-executor-direct-path.md §6.2](deferred/planner-executor-direct-path.md).)

**What landed:** The direct backend
([`extended/direct.rs`](../../crates/core/src/backend/extended/direct.rs))
now uses a `#[repr(C)]` `WireDestReceiver` struct whose
`receiveSlot` callback calls `slot_getallattrs` + per-column
`ColumnEncoder::encode_into` to write length-prefixed encoded
bytes directly into a `BytesMut` during `PortalRun`. Zero
intermediate tuplestore copy.

The implementation went through two stages:

1. **Stage A (Tuplestore):** `CachedPlanSource` + `Portal` +
   `TuplestoreReceiver` as the DestReceiver, with a post-`PortalRun`
   slot-iteration loop. This validated the Portal lifecycle FFI
   but carried a double-copy penalty (executor → tuplestore →
   wire buffer) and portal/tuplestore setup overhead that made
   `direct` ~6% slower than SPI on trivial SELECTs.

2. **Stage B (WireDestReceiver):** Replaced the tuplestore with a
   custom `WireDestReceiver` whose `receiveSlot` writes DataRows
   inline. ~90 LOC added, ~50 LOC tuplestore plumbing removed.
   Direct went from **0.98x to 1.12x** vs vanilla — a ~14
   percentage point swing, now matching SPI.

**Bench impact (measured):** `just bench pg18 20000 8 "" "" direct`
now lands at **1.09x – 1.13x qps** vs vanilla PG (was 0.98x – 1.04x
with tuplestore). p50 ~390 µs (was ~430 µs). Direct and SPI are
now at parity on the custom harness.

**Re-entry trigger:** — (done). Simple-query path (§3.3 scope)
remains on SPI; the direct WireDestReceiver is extended-query only.

### 3.2 Simple-query path's `Vec<Option<String>>` intermediate — ~~deferred~~ **SHIPPED**

**What was wrong:** The simple-query bridge materialised each row as
a `Vec<Option<String>>` (one `String` heap alloc per non-null cell),
then called pgwire's `DataRowEncoder` to re-encode each cell into a
`BytesMut`. That was 2× the per-cell allocations vs. the extended-query
path, which writes bytes directly into the `BytesMut`.

**What landed:** Ported the extended-query encoding pattern
(`BytesMut::put_i32` length prefix + `put_slice` body, with per-column
[`TypeOutput`](../../crates/core/src/backend/spi.rs) cache) into
[`run_via_spi`](../../crates/core/src/backend/spi_bridge.rs). Switched
the outer xact bracket to [`with_spi`](../../crates/core/src/backend/spi.rs)
so the two bridges share one xact discipline.

**Bench impact (measured):** ~20 percentage points of qps on `just
bench pg18 5000 2` (from 0.90x to 1.11x qps vs vanilla PG). p50 /
mean improved ~10% each. Per-cell `getTypeOutputInfo` syscache
lookups (§3.5) folded in for free.

**Re-entry trigger:** — (done).


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

**Status:** Trigger not met. Measured: `just pgbench pg18 tpcb 30 8`
yields pg_transport at **1.09x vanilla qps** (2060 vs 1884 tps),
0.91x latency — we *lead* on the workload §3.3 targets. The
per-statement xact bracket exists, but its overhead is dominated by
the bgworker-reuse / no-fork-tax advantage we have over vanilla on
the rest of the path. Re-evaluate only if a future workload shows
multi-statement `'Q'` regressing below parity, or fold the
restructure into 3.1 when *that* re-entry trigger fires.

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

### 3.5 `SPI_getvalue`'s per-cell `getTypeOutputInfo` lookup — ~~bundled with 3.2~~ **SHIPPED with 3.2**

Resolved as part of [§3.2](#32-simple-query-paths-vecoptionstring-intermediate--deferred-shipped):
the new [`run_via_spi`](../../crates/core/src/backend/spi_bridge.rs)
builds a per-column `Vec<TypeOutput>` once via
[`TypeOutput::for_type`](../../crates/core/src/backend/spi.rs) and
reuses it across every row, replacing the per-cell
`SPI_getvalue` → `getTypeOutputInfo` syscache lookup. Cell access
now goes through [`SpiTuples::cell`](../../crates/core/src/backend/spi.rs)
(uses `SPI_getbinval`) directly.


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
| **Per-cell type I/O caching (both paths)** | `TypeOutput` / `TypeSend` built once per column per query/Execute, not per cell. Simple-query path adopted this in [§3.2](#32-simple-query-paths-vecoptionstring-intermediate--deferred-shipped); extended-query path has had it since phase 9. |
| **Snapshot / xact state inside BEGIN block** | `StartTransactionCommand` becomes `CommandCounterIncrement` when already in `TBLOCK_INPROGRESS`; same for Commit. We inherit this from PG's xact machinery — no special-casing needed. |
| **Binary parameter & result format** | Honoured per-column from `Bind.parameter_format_codes` / `result_column_format_codes` without forcing text round-tripping. tokio-postgres' binary path works without conversion. |

## 5. What our bench gap actually measures

Decomposing the bench gap. **As of the §3.2 port, the gap has
inverted** — pg_transport now leads on this workload. The table
below accounts for what's *still pending* (would widen the lead
further on the workloads that exercise it):

| Source | Estimated µs/query | Reference | Status |
| --- | --- | --- | --- |
| Parse pass #2 (SPI's inner) | ~5 | §3.1 | **shipped** (direct backend uses single-parse CachedPlanSource) |
| SPI_tuptable row copy | ~2–5 | §3.1 | **shipped** (WireDestReceiver writes inline) |
| Per-query xact bracket (Start + Snapshot + SPI_connect + reverse) | ~5–10 | §3.3 | deferred (trigger not met — tpcb at 1.09x) |
| Row materialisation `Vec<Option<String>>` intermediate | ~2–5 | ~~§3.2~~ | **shipped** |
| Per-cell `SPI_getvalue` syscache lookup | ~50 ns/cell | ~~§3.5~~ | **shipped** (with §3.2) |
| Misc — pgwire framing, async dispatch, MemoryContext, `with_spi` closure scaffolding | ~5 | §3.4 | non-starter |

Closing 3.3 (~5–10 µs on multi-statement `'Q'`) would extend the
lead on pgbench-tpcb. **p95/max already stay at or below vanilla**
because we still don't pay PG's fork tax.


## 6. Re-entry conditions

Per [roadmap Q18](roadmap.md#23-resolved), perf-driven refactors
are gated on bench signal:

- **Trigger 3.1:** — (shipped). Direct backend uses
  `WireDestReceiver` with zero intermediate copies. Post-ship
  bench: direct at 1.12x on the custom harness.
- **Trigger 3.2:** — (shipped).
- **Trigger 3.3:** profile of pgbench-tpcb (or any multi-statement-
  `'Q'` workload) showing xact bracket overhead. **Not met** — we
  lead tpcb 1.09x post-§3.2.
- **Trigger 3.4:** profile shows pgwire dispatch above 5% of
  per-query latency. Unlikely.
- **Trigger 3.5:** — (shipped with 3.2).

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
