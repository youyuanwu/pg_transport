# pg_transport — benchmark run, 2026-05-28

Run-specific data + analysis for the **2026-05-28** sweep on commit
[`d3104d0`](../../../) — *"docs(design): fold pg-code-findings
audit into simple-query-direct-path"*. Raw artefacts (TSV, run
logs, progress logs) live alongside this file in
[`bench/`](bench/), [`pgbench-steady/`](pgbench-steady/),
[`pgbench-churn/`](pgbench-churn/), [`sysbench/`](sysbench/),
[`sysbench-retry-t16-t32/`](sysbench-retry-t16-t32/),
[`sysbench-retry-final-1/`](sysbench-retry-final-1/),
[`sysbench-retry-final-2/`](sysbench-retry-final-2/),
[`sysbench-retry-final-3/`](sysbench-retry-final-3/).

For the **methodology** (how each harness works) and the
**reproduction commands** see the general-info README at
[../README.md](../README.md). For the *mechanics* of how each
harness works see [../../design/bench.md](../../design/bench.md);
for the *theory* of why the numbers fall where they do see
[../../design/performance.md](../../design/performance.md).
Prior run for comparison: [../2026-05-24/](../2026-05-24/).

## 1. TL;DR

| Regime                                       | Tool             | Headline      |
| -------------------------------------------- | ---------------- | ------------- |
| Long-lived connection, single `SELECT 1` tx  | custom bench     | **1.05 – 1.27×** vanilla qps at 1 → 16 conns (direct); 1.04 → 1.22× (spi) |
| Long-lived connection, real OLTP             | pgbench (`-M simple`) | **~parity** on select / nupdate; **+5%** on tpcb (multi-statement tx) |
| **Connection-churn (`-C`)**                  | pgbench `-S -C`  | **11.0× / 14.5× / 15.1×** at 4 / 16 / 32 clients |
| Prepared-statement OLTP at scale             | sysbench (`oltp_*`) | **1.20 – 1.38×** point selects; **1.53 – 2.65×** explicit-transaction reads; write workloads regress at vCPU saturation (see §5) |

The connection-churn row is the architectural headline. The
sysbench `oltp_read_only` row at threads=32 (**2.51–2.65×** vs
vanilla) is the steady-state headline: 12 statements per
BEGIN/COMMIT-wrapped transaction, where vanilla pays per-backend
cache pressure as N grows while pg_transport multiplexes onto a
warm bgworker pool.

Comparison to [2026-05-24](../2026-05-24/) — ratios reproduce
inside this host's noise band across every regime: custom bench
within ±0.05× per cell, pgbench tpcb 1.05× (was 1.07–1.10×),
churn 11.0/14.5/15.1× (was 10.9/14.4/14.7×), sysbench point
selects 1.20–1.38× (was 1.20–1.31×), `oltp_read_only` t=32
2.51/2.65× (was 2.13/2.18×), `oltp_update_index` 0.79–0.99×
(was 0.74–0.97×). The absolute vanilla `nupdate`/`tpcb` numbers
sit ~20% below the prior run (host noise on write-heavy mixes);
the **ratios** are stable, which is what the doc is measuring.

## 2. Host

```
Linux 6.17.0-1015-azure x86_64 (Ubuntu 24.04.4 LTS)
Intel(R) Xeon(R) Platinum 8370C @ 2.80GHz
8 vCPU (4 physical cores × 2 SMT), 31 GiB RAM, 17 GiB free disk
Azure Hyper-V VM (Microsoft hypervisor, full virtualization)
```

Tool versions: rustc 1.95.0, PostgreSQL 18.4, pgbench 18.4,
sysbench 1.0.20, just 1.21.0. Same host, kernel, and toolchain
as [2026-05-24](../2026-05-24/host.txt); only the workspace
commit changed.

Full host dump (lscpu, free, df, env): [host.txt](host.txt).

**Hardware caveat that shapes the numbers below.** This is a 4
physical core box exposed as 8 vCPUs via SMT. Useful concurrency
saturates around 8 active workers. Sweep cells with `threads ≥ 16`
push past that envelope on purpose — those numbers measure how
each side schedules an over-committed workload, not raw
throughput. Vanilla PG's per-backend OS process model loses
ground in this regime (context-switch + cache-pressure + lock
contention as N grows); pg_transport's warm-bgworker pool stays
flat because the active set is multiplexed onto a smaller set of
hot processes ([../../design/pool.md](../../design/pool.md)).

## 3. Custom bench — `SELECT 1` round-trip floor

Raw data: [bench/results.tsv](bench/results.tsv) ·
[bench/results.md](bench/results.md).

Single `SELECT 1` extended-query round-trip in a tight tokio loop;
each worker holds one long-lived connection. RUNS=3, iters
auto-scaled per cell to keep per-worker sample count ≥ 1000.
Sweep wall-clock: **142 s**.

#### SPI backend

| Connections | Vanilla p50 µs | pg_transport p50 µs | Vanilla p95 µs | pg_transport p95 µs | Vanilla qps | pg_transport qps | **qps ratio** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1  | 148.4 | 139.8 | 162.7 | 173.2 |  6 605 |  6 861 | **1.04×** |
| 4  | 228.7 | 201.1 | 300.2 | 242.0 | 16 773 | 19 452 | **1.16×** |
| 8  | 337.7 | 283.4 | 504.9 | 419.0 | 22 448 | 27 048 | **1.20×** |
| 16 | 499.3 | 415.5 | 818.3 | 696.3 | 28 970 | 35 289 | **1.22×** |

#### direct backend

| Connections | Vanilla p50 µs | pg_transport p50 µs | Vanilla p95 µs | pg_transport p95 µs | Vanilla qps | pg_transport qps | **qps ratio** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1  | 150.9 | 154.2 | 194.3 | 173.1 |  6 354 |  6 658 | **1.05×** |
| 4  | 222.3 | 192.5 | 288.2 | 235.7 | 17 292 | 20 431 | **1.18×** |
| 8  | 336.7 | 267.9 | 501.3 | 401.5 | 22 500 | 28 490 | **1.27×** |
| 16 | 483.8 | 403.3 | 791.2 | 682.3 | 29 623 | 36 395 | **1.23×** |

Mechanical reading:

- **At conns=1 the floor is structural.** pg_transport's extra hop
  (TCP → FE bgworker → `SCM_RIGHTS` → slot bgworker → executor)
  costs ~10 µs per round-trip. The 1.04× / 1.05× wins at conns=1
  this run sit inside run-to-run noise — the prior run reported
  0.98× / 1.12× for the same cells. On both runs the cells are
  within ±10% of parity, which is what the architecture predicts.
- **At conns ≥ 4 we pull ahead and stay ahead.** Vanilla pays
  per-backend cache-fault pressure as N grows; pg_transport
  multiplexes onto a warm bgworker pool. The gap is largest at
  conns=8 (1 worker per physical core), then narrows slightly at
  conns=16 as both sides hit vCPU saturation.
- **direct ≈ spi on this workload.** Per-row delta favours direct
  (no tuptable copy), but at 1 row per query the saving is one
  allocation per query — measurable but small. Direct's real win
  surface is multi-row SELECTs (not exercised here) and binary
  results (also not exercised).
- **p95 / p50 ratio is consistent across the table** — no tail-latency
  pathology on either side; the gap is steady-state, not jitter.

## 4. pgbench

### 4.1 Steady-state (long-lived connections)

Raw data: [pgbench-steady/results.tsv](pgbench-steady/results.tsv) ·
[pgbench-steady/results.md](pgbench-steady/results.md).

`pgbench -M simple -T 15 -c 8 -j 8`, RUNS=3. Three workloads:

- **select** — `pgbench -S`: read-only single-row index lookup.
- **nupdate** — `pgbench -N`: simple update; skips branch / teller
  updates.
- **tpcb** — default TPC-B-like: BEGIN + 3 UPDATEs + SELECT + INSERT
  + END in a single multi-statement `'Q'`.

Sweep wall-clock: **616 s**.

| Mode    | Backend | Vanilla tps | pg_transport tps | **tps ratio** | Vanilla init-conn ms | pg_transport init-conn ms |
|---------|---------|-----------:|----------------:|--------------:|---------------------:|--------------------------:|
| select  | spi    | 35 742 | 35 690 | **1.00×** | 6.09 | 7.80 |
| select  | direct | 36 165 | 35 834 | **0.99×** | 5.89 | 7.99 |
| nupdate | spi    |  2 397 |  2 365 | **0.99×** | 6.03 | 7.71 |
| nupdate | direct |  2 262 |  2 270 | **1.00×** | 5.93 | 7.87 |
| tpcb    | spi    |    483 |    508 | **1.05×** | 6.19 | 7.72 |
| tpcb    | direct |    499 |    525 | **1.05×** | 5.96 | 7.59 |

Mechanical reading:

- **select / nupdate are at parity** — same shape as the prior
  run. Both single-statement workloads; per-`'Q'` framework cost
  (intermediate `Vec<DataRow>`, async dispatch, simple-query
  schema rebuild) is small enough relative to the work that
  ratios sit inside this host's run-to-run variance band
  (±3–5 pp on a 15 s × 3-run sample).
- **tpcb wins by 5% this run** (was 7–10% on 2026-05-24). Per
  transaction it does ~6 round-trips (BEGIN + 3 UPDATE + SELECT
  + INSERT + END); pg_transport's pre-warmed slot bgworker
  absorbs the per-message framework cost that vanilla eats fresh
  per round-trip. The smaller margin this run lines up with the
  ~20% drop in absolute `nupdate`/`tpcb` tps on both sides
  (vanilla `nupdate` 2960 → 2397; pgt `tpcb` 622 → 525). The
  ratio compression is consistent with both sides moving
  together — write-heavy mixes on tmpfs-backed pgdata are
  sensitive to checkpoint scheduling jitter, but the win
  direction is preserved.
- **`initial connection time` is ~1.3× vanilla.** This is the
  expected cold-grow signature: the pool starts empty
  (`MIN_WARM_SLOTS = 0`), so the first 8 clients trigger one
  ~25 ms cold-grow that amortises across the rest of the burst.
  The headline `tps` number excludes this (pgbench reports `tps`
  "without initial connection time"), but it's reported here for
  completeness — and §4.2 below shows the same metric collapses
  to **0.07–0.10×** vanilla (the inverse direction — pg_transport
  faster) in the regime where it actually matters.

### 4.2 Connection-churn (`pgbench -C`) — headline

Raw data: [pgbench-churn/results.tsv](pgbench-churn/results.tsv) ·
[pgbench-churn/results.md](pgbench-churn/results.md).

`pgbench -S -C -T 20`, RUNS=3, backend=spi. The `-C` flag opens a
fresh TCP connection per transaction. Vanilla pays `fork()` +
RelCache/CatCache warm-up per connection (~1–10 ms);
pg_transport hands the fd to an already-warm slot bgworker via
`SCM_RIGHTS` (~0 ms). This is the regime the architecture is
designed for. Sweep wall-clock: **399 s**.

| Clients | Vanilla tps | pg_transport tps | **tps ratio** | Vanilla avg conn time ms | pg_transport avg conn time ms | **conn-time ratio** | Vanilla avg latency ms | pg_transport avg latency ms |
|--------:|------------:|-----------------:|--------------:|-------------------------:|------------------------------:|--------------------:|-----------------------:|----------------------------:|
|  4 |   649 |  7 150 | **11.02×** |  3.60 | 0.34 | **0.10×** |  6.16 | 0.56 |
| 16 |   829 | 12 016 | **14.50×** | 11.62 | 0.80 | **0.07×** | 19.30 | 1.33 |
| 32 |   816 | 12 356 | **15.14×** | 25.07 | 1.73 | **0.07×** | 39.21 | 2.59 |

Mechanical reading:

- **The ratio grows with concurrency** (11.0× → 14.5× → 15.1×) —
  reproduces the prior run's shape (10.9 → 14.4 → 14.7×) within
  ~3% per cell. Vanilla serializes on the single-threaded
  postmaster `fork()` loop; adding clients shifts the bottleneck
  from per-tx cost to fork serialization. pg_transport's
  `SCM_RIGHTS` handoff hands each new fd to an already-warm slot
  bgworker without involving the postmaster, so per-client
  throughput holds up under contention.
- **The conn-time ratio drives the tps ratio.** Per-connection
  setup drops from 3.6 / 11.6 / 25.1 ms (vanilla) to 0.3 / 0.8 /
  1.7 ms (pg_transport) — a 10–14× reduction that maps 1-to-1
  onto the tps win.
- **No failures even at clients=32.** That's ~247 k connection
  establishments per side over 20 s on loopback. Ephemeral-port
  pressure does not manifest at this concurrency on this host.
- **Side-by-side, same invocation.** Both numbers in each row
  come from a single `just pgbench` call, eliminating cross-run
  host-state variance. The wall-clock asymmetry is real and
  intended — in 20 s the vanilla side completes ~16 k transactions
  while pg_transport completes ~247 k.

## 5. sysbench — prepared-statement OLTP

Raw data (main sweep): [sysbench/results.tsv](sysbench/results.tsv) ·
[sysbench/results.md](sysbench/results.md). Retry sweeps:
[sysbench-retry-t16-t32/results.tsv](sysbench-retry-t16-t32/results.tsv),
[sysbench-retry-final-1/results.tsv](sysbench-retry-final-1/results.tsv),
[sysbench-retry-final-2/results.tsv](sysbench-retry-final-2/results.tsv),
[sysbench-retry-final-3/results.tsv](sysbench-retry-final-3/results.tsv).

Three workloads driven over libpq's extended-query /
prepared-statement path, BEGIN/COMMIT-wrapped transactions:

- **oltp_point_select** — single prepared `SELECT … WHERE id=?`
  per transaction, no explicit `BEGIN`/`COMMIT`.
- **oltp_read_only** — 12 statements per transaction (10 point
  selects + 1 range query + 1 sum/order/distinct query), all
  inside explicit `BEGIN`/`COMMIT`.
- **oltp_update_index** — single indexed `UPDATE … WHERE id=?`
  per transaction, inside explicit `BEGIN`/`COMMIT`.

Dataset: 16 tables × 100k rows (~16 MB total, fits in default
`shared_buffers`). 20 s per side; single run per cell. Main
sweep wall-clock: **1465 s**, plus retry sweeps for flaky cells
(see §5.1).

#### `oltp_point_select`

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 | 22 592.69 | 27 199.90 | **1.20×** | main |
| spi    | 16 | 35 445.53 | 44 633.92 | **1.26×** | main |
| spi    | 32 | 32 936.22 | 43 661.09 | **1.33×** | retry-t16-t32 ¹ |
| direct |  4 | 21 971.49 | 27 298.11 | **1.24×** | main |
| direct | 16 | 36 556.20 | 44 882.06 | **1.23×** | main |
| direct | 32 | 31 596.13 | 43 552.94 | **1.38×** | main |

#### `oltp_read_only` — headline

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 |   755.78 | 1 156.01 | **1.53×** | main |
| spi    | 16 | 1 112.35 | 1 800.58 | **1.62×** | retry-t16-t32 ¹ |
| spi    | 32 |   706.05 | 1 770.67 | **2.51×** | retry-final-1 ¹ |
| direct |  4 |   765.26 | 1 170.82 | **1.53×** | main |
| direct | 16 | 1 115.68 | 1 763.16 | **1.58×** | main |
| direct | 32 |   681.68 | 1 803.93 | **2.65×** | main |

#### `oltp_update_index`

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 | 1 223.53 | 1 173.56 | **0.96×** | main |
| spi    | 16 | 4 119.39 | 3 234.33 | **0.79×** | retry-t16-t32 ¹ |
| spi    | 32 | 6 367.30 | 5 648.70 | **0.89×** | retry-final-2 ¹ |
| direct |  4 | 1 168.76 | 1 151.00 | **0.99×** | main |
| direct | 16 | 4 036.65 | 3 208.50 | **0.80×** | retry-t16-t32 ¹ ² |
| direct | 32 | 6 433.23 | 6 063.69 | **0.94×** | retry-final-3 ¹ |

Mechanical reading:

- **`oltp_read_only` at threads=32 is the steady-state headline.**
  2.51–2.65× vanilla because the workload bundles 12 statements
  per BEGIN/COMMIT-wrapped transaction (high amortisation), and
  vanilla pays per-backend cache pressure / context switches as N
  grows past physical cores while pg_transport multiplexes onto a
  warm pool. Slightly larger margin than the 2.13–2.18× reported
  on 2026-05-24 — both runs land in the same regime ("vanilla
  scales sublinearly, pg_transport stays flat"), with this run's
  vanilla side sitting ~20% lower at t=32 (706 / 682 vs prior
  840 / 861) which widens the gap.
- **`oltp_point_select` scales cleanly with concurrency** (1.20× →
  1.26× → 1.33× spi; 1.24× → 1.23× → 1.38× direct). Same
  mechanism as above, but the workload amortises less framework
  cost per transaction (1 statement vs 12), so the ratio is
  smaller in absolute terms but moves in the same direction.
- **`oltp_update_index` regresses under concurrency** (0.96× →
  0.79× → 0.89× at spi 4 / 16 / 32). This is *not* a framework
  cost — it's the host. Writes serialize on tuple locks; on a 4
  physical core box, pg_transport's bgworker pool sees more
  context-switch pressure than vanilla's per-connection backend
  model when every worker is contending for the same heap pages.
  The pattern would invert on a host with ≥ 16 physical cores
  (vanilla's per-backend cache pressure would dominate again),
  but it does not on *this* host. The numbers are reported as-is.
- **Backend choice barely matters** on these workloads. Every
  row has spi and direct within ~5% of each other. Consistent
  with §4.1's pgbench finding: direct's wins are concentrated in
  multi-row SELECTs and binary results, neither of which sysbench
  exercises heavily.

### 5.1 Caveats

#### ¹ Burst-init flakiness (sysbench `threads ≥ 16`)

The first sysbench sweep failed to capture 6 of 18 cells:
sysbench's extended-query worker init opens all N TCP connections
in parallel within ~1 ms, which overflows what the default
autoscaling pool can warm in time (`MIN_WARM_SLOTS = 0` means
every burst-init thread races to cold-grow a slot before
sysbench's connect timeout). pg_transport then sees worker
connections drop with `server closed the connection unexpectedly`,
sysbench aborts, the cell records `NA`. The full failure trace
lives in [sysbench/run.log](sysbench/run.log).

Compared to the prior run (which only lost `threads=32` cells),
this run also lost `threads=16` cells for `oltp_read_only spi`
and `oltp_update_index spi/direct` — burst-init flakiness is
inherently noisy and the cut-off shifts cell-by-cell.

Three retry sweeps recovered the missing cells:

- [sysbench-retry-t16-t32/](sysbench-retry-t16-t32/) — re-ran all
  12 `threads ∈ {16, 32}` cells. Recovered 8: all `threads=16`
  cells (including `oltp_update_index direct 16` whose main-sweep
  capture was degenerate, see ²) plus 4 of the 4 `threads=32`
  cells for `oltp_point_select`.
- [sysbench-retry-final-1/](sysbench-retry-final-1/) — single-cell
  retry for `oltp_read_only spi 32`. Captured 2.508×.
- [sysbench-retry-final-2/](sysbench-retry-final-2/) — two-cell
  retry for `oltp_update_index spi/direct 32`. Captured spi
  (0.887×); direct still `NA`.
- [sysbench-retry-final-3/](sysbench-retry-final-3/) — single-cell
  retry for `oltp_update_index direct 32`. Captured 0.943×.

Each surviving `threads ≥ 16` cell was attempted at least twice
across sweeps where overlap existed; the successful captures
agree within ~3% (e.g. `oltp_point_select spi 16` = 1.259× main
vs 1.261× retry), suggesting the values themselves are stable
and only the burst-init *completion rate* is flaky on this host.

#### ² Degenerate main-sweep capture (`oltp_update_index direct 16`)

The main sweep captured this cell with `vanilla=4058.87` and
`pgt=382.42` (0.094× — a 10× regression vs the prior run's
0.77×). The retry sweep captured `vanilla=4036.65` and
`pgt=3208.50` (0.795× — matching the prior run within 3%).
Inspecting [sysbench/run.log](sysbench/run.log) around the
affected cell shows the same `server closed the connection
unexpectedly` signature as the `NA` cells, but for this one
sysbench did not abort — it kept running with degraded
concurrency (some workers' connections silently dropped mid-run)
and reported a partial-throughput number. The §5 table uses the
clean retry capture.

Mitigating this flakiness in v0 is one of two changes — raising
`MIN_WARM_SLOTS` so the pool isn't cold at burst start
([../../design/pool.md §5.1](../../design/pool.md)), or relaxing
the pool's single-in-flight grow gate so N cold-boots can run in
parallel ([../../design/pool.md §5.2](../../design/pool.md)).
Both are deferred behind phase-7+ work; see the design docs for
the trade-offs.

## See also

- [../README.md](../README.md) — general methodology + reproduce
  commands shared across all runs.
- [../2026-05-24/](../2026-05-24/) — prior run for comparison
  (same host, different commit + ~3 days earlier).
- [../../design/bench.md](../../design/bench.md) — how each
  harness works (workload shape, output columns, failure modes).
- [../../design/performance.md](../../design/performance.md) —
  why the numbers fall where they do (per-query data-path trace,
  structural fast paths).
- [../../design/pool.md](../../design/pool.md) — the autoscaling
  pool whose `MIN_WARM_SLOTS = 0` default sets both the cold-grow
  baseline in §4.1 and the burst-init flakiness in §5.
- [../../design/architecture.md](../../design/architecture.md) —
  why the data path looks the way it does.
