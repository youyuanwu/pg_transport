# Performance — measured numbers and data-path trace

> Parent: [README.md](README.md)
> Siblings: [bench.md](bench.md) (harness mechanics) · [pool.md](pool.md) (the pool whose `MIN_WARM_SLOTS = 0` default explains the only `pgbench` regression below)

This document records the most recent benchmark numbers and the
per-query data path they measure. It is **descriptive**, not an
implementation plan: the goal is to make it easy to compare a new
run against the baseline and see what the framework is paying for
on the hot path. For *how* to run the benchmarks see
[bench.md](bench.md); for the data-path components see
[backend-wire.md](backend-wire.md) and the live source linked from
the trace in §4.

## 1. Workloads

Two harnesses; six benchmark configurations across both.

### Custom bench (`just bench`)

Single SQL statement (`SELECT 1`) in a tight tokio loop. One row,
one column, no parameters. Extended-query protocol on the wire,
parameter format text. Each worker holds one long-lived connection
and runs `iterations / connections` round-trips after a 100-iter
warmup phase. Per-sample latency captured to compute p50 / p95 /
p99 / mean; throughput measured as `total_iterations /
wall_clock` of the post-warmup phase.

Two execution backends on the pg_transport side, selected via
`SET pg_transport.execution_backend`:

- **`spi`** — `SPI_prepare` + `SPI_execute_plan_with_params`.
- **`direct`** — `CreateCachedPlan` + `Portal*` + a `#[repr(C)]`
  `WireDestReceiver` whose `receiveSlot` writes length-prefixed
  encoded bytes directly into the wire `BytesMut`.

Recipe: `just bench pg18 <iters> <connections> "" "" {spi|direct}`.

### pgbench

Standard PG-shipped `pgbench` driving three workloads:

- **`select`** — `pgbench -S` (read-only `SELECT` against the
  bench tables).
- **`nupdate`** — `pgbench -N` (simple update; skips branch /
  teller updates).
- **`tpcb`** — default TPC-B-like: BEGIN + 3 UPDATEs + SELECT +
  INSERT + END inside a single multi-statement `'Q'`.

Recipe: `just pgbench pg18 {select|nupdate|tpcb} <duration_s> <clients> "" {spi|direct}`.

pgbench connections are long-lived for the test duration; `initial
connection time` is reported separately and isn't folded into the
`tps` headline number.

## 2. Latest stable numbers (2026-05-21)

3 runs each, 8 concurrent connections, on the project's reference
dev box. Compares `dev` (autoscaling pool, `MIN_WARM_SLOTS = 0`)
against `main` (static pre-warmed pool, predecessor branch) for
the same workloads on the same host back-to-back.

### Custom bench, 20000 iters, 8 conns

`dev` (autoscaling, `MIN_WARM_SLOTS = 0`):

| Mode | p50 | p95 | mean | **qps** | Ratio (qps) |
|---|---|---|---|---|---|
| `spi` vanilla | 432.4 µs | 570.7 µs | 438.6 µs | 18 109 | — |
| `spi` pg_transport | 377.0 µs | 497.6 µs | 384.4 µs | **20 718** | **1.14x** |
| `direct` vanilla | 424.2 µs | 555.2 µs | 430.2 µs | 18 470 | — |
| `direct` pg_transport | 381.3 µs | 502.3 µs | 388.7 µs | **20 513** | **1.11x** |

`main` (static pre-warmed) ran the same harness with `qps` ratios
of **1.09x** (`spi`) and **1.11x** (`direct`). The two branches
sit inside this host's run-to-run variance band (±3–5 pp on a
20000-iter sample).

### pgbench, 15 s, 8 clients

`dev` (autoscaling):

| Mode | Backend | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn |
|---|---|---|---|---|---|---|
| `select` | `spi` | 25 510 | 24 875 | **0.975x** | 7.9 ms | 30.3 ms |
| `select` | `direct` | 26 161† | 25 105† | **0.96x** | 7.8 ms | 31.8 ms |
| `nupdate` | `spi` | 6 226‡ | 6 261 | **1.006x** | 8.5 ms | 31.0 ms |
| `nupdate` | `direct` | 6 281 | 6 250 | **0.995x** | 7.9 ms | 29.8 ms |
| `tpcb` | `spi` | 1 921 | 2 066 | **1.076x** | 7.1 ms | 30.4 ms |
| `tpcb` | `direct` | 1 933 | 2 094 | **1.083x** | 7.7 ms | 30.8 ms |

† `select direct` run 3 saw a host-side stall on *both* vanilla
(19 439 tps) and pg_transport (21 497 tps); ratio is unaffected
because the comparison takes the same hit on both halves. The
numbers above exclude that run; the all-3-runs raw ratio is
**1.00x**.

‡ `nupdate spi` vanilla run 1 stalled at 5 457 tps vs ~6 226 in
runs 2–3; excluded above. Raw-all-3 ratio with the stall is
**1.05x**.

`main` (static pre-warmed) measured tps ratios in the same range
on the same host (0.976x / 0.984x / 1.00x / 1.009x / 1.10x /
1.083x across the six rows). The autoscaling refactor preserved
steady-state throughput within ±3 pp of `main` on every workload.

### Known regression: `pgbench` initial connection time

On `dev`, pgbench's `initial connection time` metric jumped 7–9×
because the autoscaling pool starts at zero slots and the 8
pgbench clients race into an empty pool; each pays the cold-grow
cost (postmaster fork + slot tokio runtime build + TLS acceptor
+ first `b'R'`) once.

| Workload / Backend | main (pre-warmed) | dev (cold-grow) | Δ |
|---|---|---|---|
| `select` `spi` | 4.4 ms | 30.3 ms | +26 ms (6.9x) |
| `select` `direct` | 3.7 ms | 31.8 ms | +28 ms (8.6x) |
| `nupdate` `spi` | 3.4 ms | 31.0 ms | +28 ms (9.1x) |
| `nupdate` `direct` | 3.9 ms | 29.8 ms | +26 ms (7.6x) |
| `tpcb` `spi` | 3.6 ms | 30.4 ms | +27 ms (8.4x) |
| `tpcb` `direct` | 3.5 ms | 30.8 ms | +27 ms (8.8x) |

This is a one-shot startup cost paid once per pgbench run, not a
per-query cost, so it doesn't show up in the `tps` numbers (which
are post-connection). It also doesn't show up in the custom bench
because the bench's warmup phase covers cold-grow before the
measurement window opens.

The documented lever is `MIN_WARM_SLOTS` (currently a compile-time
constant defaulting to 0 in [pool.md §5.1](pool.md#51-the-single-knob)).
Promoting it to a GUC and setting it ≥ `clients` recovers the
`main` numbers, at the cost of holding that many bgworkers
resident even on an idle cluster.

## 3. Where we sit vs. other pooling solutions

| Solution | Typical qps vs. direct PG | Why |
|---|---|---|
| pgbouncer (transaction mode) | 0.7x – 0.85x | libpq parse/encode round-trip on every message |
| pgcat | 0.75x – 0.90x | same shape as pgbouncer, Rust-based |
| pgpool-II | 0.6x – 0.8x | proxy + query rewriting overhead |
| **pg_transport** | **0.97x – 1.14x** | bgworker reuse + in-process execution path (SPI and direct/WireDestReceiver modes) |

The reason we land in the same range as direct PG (rather than the
0.7–0.85x range typical of pooling) is structural: **we don't proxy
SQL**. The client fd lands directly in a bgworker via SCM_RIGHTS;
that bgworker calls PG's own SPI (or direct planner+executor) to
execute. There is no "decode wire bytes, re-encode them, ship to
PG, decode response, re-encode again" overhead. The wire layer is
in-process with the executor.

## 4. The data path, traced

The per-query path from socket-read to socket-write, with each
stage mapped to its source file. Useful for attributing a future
bench delta to a specific component.

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
                  │    spi backend:  result lands in        │
                  │                  SPI_tuptable           │
                  │    direct backend: WireDestReceiver     │
                  │                    writes inline; no    │
                  │                    tuptable copy        │
                  └─────────────────────────────────────────┘
                                  │
                                  ▼ row materialisation
                  ┌─────────────────────────────────────────┐
                  │ 6. TypeOutput/Send → BytesMut           │
                  │    (per-column cache built once per     │
                  │     query/Execute; cell write is        │
                  │     BytesMut::put_i32 + put_slice)      │
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
|---|---|---|
| 1 | [crates/core/src/wire/pgwire_v3.rs](../../crates/core/src/wire/pgwire_v3.rs) (`process_socket` via the `pgwire` crate) | Async dispatch loop; one task per connection |
| 2 | [crates/core/src/backend/spi_bridge.rs](../../crates/core/src/backend/spi_bridge.rs) (`'Q'`) + [extended.rs](../../crates/core/src/backend/extended.rs) (`P`/`B`/`D`/`E`/`S`) | Dyn dispatch via pgwire's `SimpleQueryHandler` / `ExtendedQueryHandler` |
| 3 | [crates/core/src/backend/spi.rs](../../crates/core/src/backend/spi.rs) (`with_spi`) | Per-query xact bracket; opened once per `'Q'`-inner-statement or once per Execute |
| 4 | `spi_bridge::parse_and_classify` (simple) + `extended::infer_param_types` (extended) | Calls PG's `raw_parser` / `pg_analyze_and_rewrite_varparams` |
| 5 | spi backend: PG's `SPI_execute*` / `SPI_execute_plan*` · direct backend: [extended/direct.rs](../../crates/core/src/backend/extended/direct.rs) (`CachedPlanSource` + `Portal*` + `WireDestReceiver`) | Result materialised into `SPI_tuptable` (spi) or written inline by `receiveSlot` (direct) |
| 6 | [extended::ColumnEncoder](../../crates/core/src/backend/extended.rs) + [`TypeOutput`/`TypeSend`](../../crates/core/src/backend/spi.rs) | Per-column encoder cache built once; cell write is `BytesMut::put_i32` + `put_slice` |
| 7 | pgwire `DataRowEncoder` (simple) or our manual `BytesMut::put_i32` + `put_slice` (extended) | The encode step is folded into stage 6 for the extended-query path |

## 5. What's structurally fast

The numbers in §2 are not the result of micro-optimisation. They
fall out of the dispatch model.

| Component | Why it's fast |
|---|---|
| **Connection acquisition (warm)** | SCM_RIGHTS handoff is ~10 µs vs. PG's `fork()` at ~1 ms+. ~100× faster on warm-pool connection establishment than vanilla PG. |
| **Connection acquisition (cold)** | The autoscaling pool's first connection per slot pays ~25–30 ms cold-grow (postmaster fork + tokio runtime + TLS acceptor + first `b'R'`); see [pool.md §5.2](pool.md#52-grow-on-demand) and §2's `initial connection time` regression. Subsequent connections are warm-path. |
| **Backend reuse** | One bgworker serves N sequential connections without process teardown. Saves PG's per-connection cleanup tax entirely. |
| **Per-handoff state isolation** | `ResetAllOptions()` between handoffs scrubs USERSET/SUSET GUCs (since vanilla PG would have `proc_exit`'d). Cost: walks the GUC array; not on the per-query hot path. |
| **Tokio runtime** | Built once per slot (`Rc<Runtime>`), reused across handoffs. Not rebuilt per query. |
| **TLS** | rustls + ring; same crypto class as PG's OpenSSL. Per-byte overhead identical. |
| **SPI plan caching (extended-query)** | `SPI_keepplan`'d once at Parse, reused across Execute via `SPI_execute_plan`. Matches PG's `CachedPlanSource` semantics. At Execute time we are at zero-parse parity with PG. |
| **Per-cell type I/O caching (both paths)** | `TypeOutput` / `TypeSend` built once per column per query/Execute, not per cell. |
| **Snapshot / xact state inside BEGIN block** | `StartTransactionCommand` becomes `CommandCounterIncrement` when already in `TBLOCK_INPROGRESS`; same for Commit. Inherited from PG's xact machinery, no special-casing needed. |
| **Binary parameter & result format** | Honoured per-column from `Bind.parameter_format_codes` / `result_column_format_codes` without forcing text round-tripping. tokio-postgres' binary path works without conversion. |
| **Row materialisation (both paths)** | Both simple-query and extended-query write encoded bytes directly into the wire `BytesMut`. No `Vec<Option<String>>` intermediate. Direct backend additionally skips the `SPI_tuptable` step entirely via `WireDestReceiver`. |

## See also

- [bench.md](bench.md) — how to run the benchmarks.
- [pool.md](pool.md) — the autoscaling pool whose `MIN_WARM_SLOTS = 0`
  default explains the `pgbench` initial-connection regression
  in §2.
- [architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer)
  — why the data path looks the way it does (delegate-vs-build
  table).
- [backend-wire.md §6](backend-wire.md#6-spi-bridge) — the SPI
  bridge that stages 4–6 of the trace exercise.
- [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md)
  — the design behind the `direct` execution backend.
