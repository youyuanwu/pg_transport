# Performance — measured numbers and data-path trace

> Parent: [README.md](README.md)
> Siblings: [bench.md](bench.md) (harness mechanics) · [pool.md](pool.md) (the autoscaling pool referenced in §2's cold-grow analysis)

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

## 2. Latest stable numbers (2026-05-22)

3 runs each, 8 concurrent connections, on the project's reference
dev box. `dev` branch with both the autoscaling pool and the
simple-query direct backend
([deferred/simple-query-direct-path.md](deferred/simple-query-direct-path.md))
shipped. `MIN_WARM_SLOTS = 0`.

### Custom bench, 20000 iters, 8 conns

`dev` (autoscaling, `MIN_WARM_SLOTS = 0`):

| Mode | p50 | p95 | mean | **qps** | Ratio (qps) |
|---|---|---|---|---|---|
| `spi` vanilla | 432.4 µs | 570.7 µs | 438.6 µs | 18 109 | — |
| `spi` pg_transport | 377.0 µs | 497.6 µs | 384.4 µs | **20 718** | **1.14x** |
| `direct` vanilla | 424.2 µs | 555.2 µs | 430.2 µs | 18 470 | — |
| `direct` pg_transport | 381.3 µs | 502.3 µs | 388.7 µs | **20 513** | **1.11x** |

The custom bench exercises extended-query, which has used the
direct backend since Stage A shipped.

### pgbench, 15 s, 8 clients

pgbench `-M simple` exercises the simple-query path
exclusively, so the `direct` rows are the
[`simple_direct::execute_simple_query_direct`](../../crates/core/src/backend/simple_direct.rs)
path; the `spi` rows are the
[`spi_bridge::execute_simple_query`](../../crates/core/src/backend/spi_bridge.rs)
path. 3-run means:

| Mode | Backend | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---|---|---|---|---|---|---|
| `select` | `spi` | 28 381 | 26 703 | **0.94x** | 7.7 ms | 8.4 ms | **1.09x** |
| `select` | `direct` | 28 151 | 26 833 | **0.95x** | 7.5 ms | 9.0 ms | **1.20x** |
| `nupdate` | `spi` | 6 571 | 6 493 | **0.99x** | 7.3 ms | 9.0 ms | **1.23x** |
| `nupdate` | `direct` | 6 724 | 6 619 | **0.98x** | 8.3 ms | 8.7 ms | **1.05x** |
| `tpcb` | `spi` | 2 054 | 2 203 | **1.07x** | 7.3 ms | 8.8 ms | **1.20x** |
| `tpcb` | `direct` | 2 148 | 2 199 | **1.02x** | 7.7 ms | 8.8 ms | **1.14x** |

**Direct vs SPI on the pg_transport side only** (`select` /
`nupdate` / `tpcb`): +0.5% / +1.9% / −0.2%. The direct backend
does not materially move tps on these `'Q'`-protocol workloads;
at 1-row hot-path result sets, SPI's amortisation of analyze +
plan + execute into one `SPI_execute` FFI call is competitive
with the direct path's separate `pg_analyze_and_rewrite_*` +
`pg_plan_queries` + `Portal*` sequence.

Mechanical reading of these numbers:

- **Direct is at parity with SPI on these workloads, not better.**
  The gap to vanilla is itself within this host's run-to-run
  variance band (±3–5 pp on a 15 s pgbench sample), so the
  +0.5% / +1.9% / −0.2% column above should be read as "no
  signal" rather than as a real swing in either direction.

- **What the direct path *saves* on `'Q'` is small for 1-row
  results.** Skipping the `SPI_tuptable` → `Vec<DataRow>` copy
  avoids one allocation + one copy per row. At pgbench's row
  count of 1 that wins back ~1 pp; for multi-row SELECTs (which
  pgbench does not exercise on these workloads) the saving
  scales linearly.

- **What the direct path *adds* on `'Q'` is per-statement FFI
  traffic.** SPI compresses analyze + plan + execute into one
  `SPI_execute` FFI call. The direct path issues separately
  wrapped calls to `pg_analyze_and_rewrite_fixedparams`,
  `pg_plan_queries`, `CreatePortal`, `PortalDefineQuery`,
  `PortalStart`, `PortalRun`, `PortalDrop`, `CreateCommandTag`,
  and `InitializeQueryCompletion`. Each pgrx FFI shim installs
  a `sigsetjmp` savepoint for PG-ERROR catching that vanilla
  PG's internal C-to-C calls do not pay. The cumulative cost
  shows up most on `tpcb` — pgbench `-M simple` issues each
  TPC-B inner statement as its own `'Q'`, so a transaction
  pays the per-`'Q'` overhead 7 times.

- **What both backends pay vs. vanilla `exec_simple_query`** on
  the `'Q'` path is structural to going through pgwire instead
  of libpq:
  - **Intermediate `Vec<DataRow>`.** Vanilla's `DestRemote`
    writes directly into libpq's output buffer; direct's
    `WireDestReceiver` fills a `Vec<DataRow>` which pgwire
    then iterates via `futures::stream::iter`. Killing this
    intermediate would require a pgwire-side change to let
    the receiver hold the output `BytesMut` directly.
  - **Async dispatch.** pgwire dispatches via
    `Arc<dyn SimpleQueryHandler>` + `async fn` + tokio
    poll/wake per `'Q'`.
  - **Per-`'Q'` schema build (simple-query only).**
    `dest_receiver::schema_and_encoders_text` walks the
    portal's tupdesc and builds a fresh `Vec<FieldInfo>` per
    `'Q'` (one column-name `String` per column,
    `from_utf8_unchecked` since PG identifiers are ASCII),
    wrapped in `Arc::new` for `QueryResponse`. The slot
    doesn't track simple-query SQL across `'Q'`s, so each
    query rebuilds. Extended-query (`'B'`/`'E'`) avoids this
    entirely — see §5 "Schema caching (extended-query)".

The write-heavy `tpcb` rows still land above vanilla overall
(1.02–1.07×) because both execution paths skip a libpq-side
round-trip per inner statement — the network half of the round
trip absent on a localhost loopback is the latency half present
on a TCP-over-loopback `pgbench` run.

Where direct *does* pay off, and pgbench does not exercise it:
extended-query (custom bench shows 1.11× vanilla), multi-row
SELECTs (the `Vec<DataRow>` copy avoided per row), and
binary-format results (no SPI text→binary detour).

### Initial connection time

`pgbench`'s `initial connection time` metric covers TCP connect →
auth → first ready-for-query for all clients. On `dev` with
`MIN_WARM_SLOTS = 0` the pool starts empty, so the first handoff
triggers a cold-grow (postmaster fork + slot tokio runtime build +
TLS acceptor + first `b'R'`, ~25–30 ms wall-clock). The
remaining clients wait for that one warmed slot to turn over and
serve them in microseconds each.

The TCP accept loop in
[`tcp.rs`](../../crates/core/src/handoff/tcp.rs)
runs each accept's handoff in a per-accept `spawn_local` task, so
the accept rate is decoupled from handoff latency — the 8 clients'
TCP backlog drains at accept speed while the pool grows once and
recycles. The measured 1.1–1.2× gap above is the single cold-grow
amortized across 8 clients, not 8× cold-grow.

If first-connection latency matters operationally, the documented
lever is `MIN_WARM_SLOTS` (currently a compile-time constant in
[pool.md §5.1](pool.md#51-the-single-knob)). Setting it ≥ peak
expected `clients` keeps that many bgworkers resident on idle
clusters and collapses `initial connection time` to vanilla-PG
levels.

A second lever, not yet implemented: relax the pool's
single-in-flight grow gate to allow `min(pending_waiters,
ceiling - total)` concurrent grows. `load_dynamic` is a ~10 µs
syscall on the FE main thread; firing N of them in serial lets N
new BEs cold-boot in parallel in postmaster-managed processes,
collapsing the burst case from "one cold-boot + N×turnover" to
"one cold-boot, all served in parallel". See
[pool.md §5.2](pool.md#52-grow-on-demand).

### pgbench, connection-churn (`-C`), 20 s, 3 runs each (2026-05-23)

The numbers above measure the **steady-state** regime: a small,
fixed number of long-lived connections doing many transactions
each. The framework hop is a meaningful fraction of cost but the
absolute tps delta is small or near parity. The connection-churn
regime — `pgbench -C`, fresh TCP connection per transaction — is
where the architectural win shows up. Recipe:
[bench.md §2.7](bench.md#27-connection-churn-mode-connect1).

3-run means at each client count; same dev box as §2; `backend=spi`,
`mode=select`; pgbench's built-in `-S` (a real index lookup):

| Clients | Vanilla tps | pg_transport tps | **tps ratio** | Vanilla avg conn time | pg_transport avg conn time | conn-time ratio | Vanilla avg latency | pg_transport avg latency |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4   |   609 |  4 740 |  **7.78x** |  4.36 ms | 0.57 ms |  7.65x |  6.58 ms | 0.84 ms |
| 16  | 1 131 | 11 562 | **10.22x** |  9.74 ms | 0.95 ms | 10.30x | 14.14 ms | 1.38 ms |
| 32  | 1 153 | 13 605 | **11.80x** | 19.58 ms | 1.49 ms | 13.10x | 27.77 ms | 2.35 ms |

Probe comparison at clients=16, single run with
[`bench/scripts/select_one.sql`](../../bench/scripts/select_one.sql)
(bare `SELECT 1;`, no index lookup) substituted for the built-in
`-S`:

| Probe | Vanilla tps | pg_transport tps | tps ratio |
|---|---:|---:|---:|
| Built-in `-S` (~50 µs index lookup per tx) | 1 131 | 11 562 | 10.22x |
| `select_one.sql` (zero exec cost)          | 1 623 | 13 754 |  8.47x |

Mechanical reading:

- **The ratio grows with client count** (7.78x → 10.22x → 11.80x).
  Vanilla serializes on the single-threaded postmaster `fork()`
  loop plus per-backend RelCache / CatCache warm-up; adding
  clients shifts the bottleneck from per-tx cost to fork
  serialization. pg_transport's `SCM_RIGHTS` handoff hands each
  new fd to an already-warm slot bgworker without involving the
  postmaster, so per-client throughput holds up under contention.
  See [frontend-handoff.md](frontend-handoff.md) for the handoff
  mechanism and [pool.md §5.2](pool.md#52-grow-on-demand) for the
  warm-pool behaviour.

- **Connection-time ratio mirrors tps ratio.** At every client
  count the two ratios match within ~10%. The 7.78–11.80x tps
  win is almost entirely from the 7.65–13.10x drop in
  per-connection setup cost — query execution is ≤ 0.5 ms on
  either side for pgbench's single-row index lookup.

- **The custom probe compresses the ratio (10.22x → 8.47x).**
  `select_one.sql` removes the ~50 µs of index-lookup work, which
  is the same overhead on both sides. The residual 8.47x is the
  asymptotic "handoff vs fork-and-init" win once execution cost
  is removed from the denominator. The fact that the built-in
  `-S` shows a *higher* ratio than the zero-exec probe is the
  normal "smaller denominator means smaller ratio" effect, not a
  reversal.

- **Vanilla per-connection time hits 19.6 ms at clients=32.**
  That's serialized waiting on the postmaster fork loop, not a
  fork cost per se. Real applications doing per-transaction
  reconnects at this concurrency would either gate behind a
  connection pool (pgbouncer / PgBouncer / our handoff path) or
  accept the wall-clock tax. The headline of this comparison is
  that pg_transport collapses the 19.6 ms wait to 1.5 ms because
  the postmaster fork loop is not in the path.

- **No failures even at clients=32 × 20 s.** That's ~140 k
  connection establishments on loopback per side over the run.
  The ephemeral-port pressure warned about in
  [bench.md §2.7](bench.md#27-connection-churn-mode-connect1) does
  not manifest at this size; the recipe's `max_connections`
  auto-bump (to 128 here) is what makes the vanilla side hold up
  long enough to be measurable.

- **Side-by-side, same invocation.** Both numbers in each row
  come from a single `just pgbench` call, eliminating cross-run
  host-state variance. Wall-clock asymmetry is real and intended:
  in 20 s the vanilla side completes ~23 k transactions while
  pg_transport completes ~275 k.

Reproduce:

```sh
just pgbench pg18 select 20  4 "" spi 1                                # clients=4
just pgbench pg18 select 20 16 "" spi 1                                # clients=16
just pgbench pg18 select 20 32 "" spi 1                                # clients=32
just pgbench pg18 select 20 16 "" spi 1 bench/scripts/select_one.sql   # probe
```

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
| 6 | [dest_receiver::ColumnEncoder](../../crates/core/src/backend/dest_receiver.rs) wrapping [`TypeOutput`/`TypeSend`](../../crates/core/src/backend/spi.rs) | One encoder per column built once per query/Execute; cell write is `ColumnEncoder::encode_into(datum, &mut buf)` — one `OidOutputFunctionCall` / `OidSendFunctionCall` then `put_slice` straight into the row's `BytesMut`, no per-cell `Vec<u8>` |
| 7 | pgwire `DataRowEncoder` (simple) or our manual `BytesMut::put_i32` + `put_slice` (extended) | The encode step is folded into stage 6 for the extended-query path |

## 5. What's structurally fast

The numbers in §2 are not the result of micro-optimisation. They
fall out of the dispatch model.

| Component | Why it's fast |
|---|---|
| **Connection acquisition (warm)** | SCM_RIGHTS handoff is ~10 µs vs. PG's `fork()` at ~1 ms+. ~100× faster on warm-pool connection establishment than vanilla PG. |
| **Connection acquisition (cold)** | First connection per pool pays one cold-grow (~25–30 ms: postmaster fork + tokio runtime + TLS acceptor + first `b'R'`). The per-accept `spawn_local` in [tcp.rs](../../crates/core/src/handoff/tcp.rs) decouples accept rate from handoff latency, so a burst of N TCP clients pays *one* cold-grow plus N microsecond-scale slot turnovers, not N × cold-grow. See [pool.md §5.2](pool.md#52-grow-on-demand) and §2's initial-connection-time analysis. |
| **Backend reuse** | One bgworker serves N sequential connections without process teardown. Saves PG's per-connection cleanup tax entirely. |
| **Per-handoff state isolation** | `ResetAllOptions()` between handoffs scrubs USERSET/SUSET GUCs (since vanilla PG would have `proc_exit`'d). Cost: walks the GUC array; not on the per-query hot path. |
| **Tokio runtime** | Built once per slot (`Rc<Runtime>`), reused across handoffs. Not rebuilt per query. |
| **TLS** | rustls + ring; same crypto class as PG's OpenSSL. Per-byte overhead identical. |
| **SPI plan caching (extended-query)** | `SPI_keepplan`'d once at Parse, reused across Execute via `SPI_execute_plan`. Matches PG's `CachedPlanSource` semantics. At Execute time we are at zero-parse parity with PG. |
| **Schema caching (extended-query)** | Both extended backends (`extended/direct.rs`, `extended/spi.rs`) cache the result-column schema as `Arc<Vec<FieldInfo>>` on the `PreparedStatement` at Parse time, with every column's `FieldFormat` set to `Text`. Per-Execute on the common `Format::UnifiedText` path the response uses `Arc::clone` of that cached schema — one atomic refcount increment, zero per-column `String` allocations and no fresh `Vec`/`Arc`. `Format::UnifiedBinary` / `Format::Individual` rebuilds a per-Execute `Vec<FieldInfo>` from the cached one to carry the requested format codes. Simple-query doesn't have an equivalent cache (no SQL→statement memo); see §2. |
| **Per-cell type I/O caching (both paths)** | The shared [`dest_receiver::ColumnEncoder`](../../crates/core/src/backend/dest_receiver.rs) wraps `TypeOutput` (text) or `TypeSend` (binary), built once per column per query/Execute, never per cell. Per-cell write is `encode_into(datum, &mut buf)`: the palloc'd cstring / `bytea` is read once and `put_slice`d straight into the row's `BytesMut` before being `pfree`'d — no per-cell `Vec<u8>` intermediate on either the simple-query or extended-query path. |
| **Parameter slice (extended-query)** | Bind/Execute hands the wire-format `&[Option<Bytes>]` straight through to the per-type input/receive decoders — no per-Execute clone of the slice. `Bytes` is refcounted, so each `Some(bytes)` is an `Arc`-style reference, not a payload copy. |
| **Snapshot / xact state inside BEGIN block** | `StartTransactionCommand` becomes `CommandCounterIncrement` when already in `TBLOCK_INPROGRESS`; same for Commit. Inherited from PG's xact machinery, no special-casing needed. |
| **Binary parameter & result format** | Honoured per-column from `Bind.parameter_format_codes` / `result_column_format_codes` without forcing text round-tripping. tokio-postgres' binary path works without conversion. |
| **Row materialisation (both paths)** | Both simple-query and extended-query write encoded bytes directly into the wire `BytesMut`. No `Vec<Option<String>>` intermediate. The `direct` backend additionally skips the `SPI_tuptable` step via `WireDestReceiver`; that win is visible on the custom bench (extended-query, 1.11× vanilla) but measured at +0.5% / +1.9% / −0.2% on pgbench `select` / `nupdate` / `tpcb` (§2) — at 1-row hot-path result sets the per-`'Q'` framing overhead (intermediate `Vec<DataRow>`, async dispatch, simple-query schema rebuild) dominates the per-row saving. |

## See also

- [bench.md](bench.md) — how to run the benchmarks.
- [pool.md](pool.md) — the autoscaling pool whose `MIN_WARM_SLOTS = 0`
  default sets the cold-grow baseline analyzed in §2.
- [architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer)
  — why the data path looks the way it does (delegate-vs-build
  table).
- [backend-wire.md §6](backend-wire.md#6-spi-bridge) — the SPI
  bridge that stages 4–6 of the trace exercise.
- [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md)
  — the design behind the `direct` execution backend.
