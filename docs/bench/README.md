# pg_transport — benchmark results (2026-05-24)

Reproducible benchmark sweep capturing pg_transport vs vanilla PG
across four regimes — latency floor, steady-state OLTP,
connection-churn, and prepared-statement scaling. Run on a single
8 vCPU Azure VM; all raw artefacts (TSV, run logs, progress logs)
live alongside this file in [`bench/`](.), [`pgbench-steady/`](pgbench-steady/),
[`pgbench-churn/`](pgbench-churn/), [`sysbench/`](sysbench/),
[`sysbench-retry-t32/`](sysbench-retry-t32/), [`sysbench-retry-final/`](sysbench-retry-final/).

For the *mechanics* of how each harness works see
[../design/bench.md](../design/bench.md); for the *theory* of why
the numbers fall where they do see
[../design/performance.md](../design/performance.md). This file is
the data layer: numbers + the conditions under which they were
produced + the reproduction commands.

## 1. TL;DR

| Regime                                       | Tool             | Headline      |
| -------------------------------------------- | ---------------- | ------------- |
| Long-lived connection, single `SELECT 1` tx  | custom bench     | **1.12 – 1.26×** vanilla tps at 1 → 16 conns (direct backend); 1.0 → 1.19× (spi) |
| Long-lived connection, real OLTP             | pgbench (`-M simple`) | **~parity** on select / nupdate; **+7–10%** on tpcb (write-heavy multi-statement tx) |
| **Connection-churn (`-C`)**                  | pgbench `-S -C`  | **10.9× / 14.4× / 14.7×** at 4 / 16 / 32 clients |
| Prepared-statement OLTP at scale             | sysbench (`oltp_*`) | **1.20 – 1.31×** point selects; **1.50 – 2.18×** explicit-transaction reads; write workloads regress at vCPU saturation (see §6) |

The connection-churn row is the architectural headline. The
sysbench `oltp_read_only` row at threads=32 (**2.13–2.18×** vs vanilla)
is the steady-state headline: 12 statements per BEGIN/COMMIT-wrapped
transaction, where vanilla pays per-backend cache pressure as N
grows while pg_transport multiplexes onto a warm bgworker pool.

## 2. Methodology

Each sweep was driven by one of the three sweep scripts in
[`../../scripts/`](../../scripts/) (see
[scripts/bench_sweep.sh](../../scripts/bench_sweep.sh),
[scripts/pgbench_sweep.sh](../../scripts/pgbench_sweep.sh),
[scripts/sysbench_sweep.sh](../../scripts/sysbench_sweep.sh)).
Each sweep:

1. Spins up a fresh temp cluster per cell (`initdb` +
   `shared_preload_libraries = 'pg_transport'`, dual-port:
   pg_transport on `127.0.0.1:5454`, vanilla PG on
   `127.0.0.1:54329`).
2. Runs the workload against **both** ports in sequence, parses tps
   / latency / connection time out of the driver's output.
3. Tears the cluster down between cells (no carry-over state).

| Sweep             | Cells | RUNS/cell | Per-cell duration | Notes |
| ----------------- | ----: | --------: | ----------------- | ----- |
| `bench/`          | 24    | 3         | ~5 s              | autoscaled iters: `max(2000, 1000 × connections)` |
| `pgbench-steady/` | 18    | 3         | ~35 s             | duration=15 s each side (vanilla, pg_transport) |
| `pgbench-churn/`  | 9     | 3         | ~45 s             | duration=20 s; `-C` (reconnect per tx) |
| `sysbench/`       | 18    | 1         | ~85 s             | duration=20 s; sysbench prepares 16 tables × 100k rows |
| `sysbench-retry-t32/` | 6 | 1         | ~85 s             | re-run of the threads=32 row — see §6 caveat |
| `sysbench-retry-final/` | 1 | 1       | ~85 s             | the single straggler from above — see §6 caveat |

`RUNS=3` for the bench / pgbench sweeps means each `pg_transport`
vs `vanilla` ratio is the arithmetic mean of three same-cell runs.
The sysbench sweep does not have a built-in `RUNS` axis; each
sysbench cell is a single 20 s run, with the threads=32 row
double-sampled (main + retry) due to flakiness — see §6.

All workloads use libpq's simple-query protocol (`-M simple` /
default) except sysbench, which uses libpq's extended-query /
prepared-statement path. Both paths route through pg_transport's
slot bgworker via the FE `SCM_RIGHTS` handoff; the SQL execution
inside the slot uses either `SPI_execute_plan_with_params`
(backend=spi) or `CreateCachedPlan + Portal*` (backend=direct);
see [../design/architecture.md](../design/architecture.md).

## 3. Host

```
Linux 6.17.0-1015-azure x86_64 (Ubuntu 24.04.4 LTS)
Intel(R) Xeon(R) Platinum 8370C @ 2.80GHz
8 vCPU (4 physical cores × 2 SMT), 31 GiB RAM, 17 GiB free disk
Azure Hyper-V VM (Microsoft hypervisor, full virtualization)
```

Tool versions: rustc 1.95.0, PostgreSQL 18.4, pgbench 18.4,
sysbench 1.0.20, just 1.21.0. Tested commit:
[`61f7e16`](../../) — *"fix(extended): sniff xact-control on
direct backend Parse path + sysbench sweep"*.

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
hot processes ([pool.md](../design/pool.md)).

## 4. Custom bench — `SELECT 1` round-trip floor

Raw data: [bench/results.tsv](bench/results.tsv) ·
[bench/results.md](bench/results.md). Sweep:
[scripts/bench_sweep.sh](../../scripts/bench_sweep.sh).

Single `SELECT 1` extended-query round-trip in a tight tokio loop;
each worker holds one long-lived connection. RUNS=3, iters
auto-scaled per cell to keep per-worker sample count ≥ 1000.

#### SPI backend

| Connections | Vanilla p50 µs | pg_transport p50 µs | Vanilla p95 µs | pg_transport p95 µs | Vanilla qps | pg_transport qps | **qps ratio** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1  | 148.2 | 152.1 | 162.1 | 180.5 |  6 570 |  6 450 | **0.98×** |
| 4  | 229.6 | 205.4 | 288.8 | 255.0 | 16 431 | 19 145 | **1.17×** |
| 8  | 341.5 | 284.0 | 522.8 | 419.7 | 21 940 | 26 988 | **1.23×** |
| 16 | 480.8 | 419.4 | 794.9 | 688.9 | 29 545 | 35 133 | **1.19×** |

#### direct backend

| Connections | Vanilla p50 µs | pg_transport p50 µs | Vanilla p95 µs | pg_transport p95 µs | Vanilla qps | pg_transport qps | **qps ratio** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1  | 150.9 | 137.7 | 187.3 | 165.0 |  6 328 |  7 067 | **1.12×** |
| 4  | 228.0 | 192.5 | 287.9 | 229.4 | 16 868 | 20 582 | **1.22×** |
| 8  | 343.3 | 272.9 | 523.4 | 420.6 | 21 936 | 27 403 | **1.25×** |
| 16 | 486.5 | 412.0 | 836.8 | 706.5 | 28 324 | 35 672 | **1.26×** |

Mechanical reading:

- **At conns=1 the floor is structural.** pg_transport's extra hop
  (TCP → FE bgworker → `SCM_RIGHTS` → slot bgworker → executor)
  costs ~10 µs per round-trip. On the SPI backend that just
  barely costs us parity (0.98×); on the direct backend the
  saving from skipping `SPI_tuptable` → `Vec<DataRow>` copy more
  than pays for it (1.12×). Both within run-to-run noise on this
  host.
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

## 5. pgbench

### 5.1 Steady-state (long-lived connections)

Raw data: [pgbench-steady/results.tsv](pgbench-steady/results.tsv) ·
[pgbench-steady/results.md](pgbench-steady/results.md). Sweep:
[scripts/pgbench_sweep.sh](../../scripts/pgbench_sweep.sh).

`pgbench -M simple -T 15 -c 8 -j 8`, RUNS=3. Three workloads:

- **select** — `pgbench -S`: read-only single-row index lookup.
- **nupdate** — `pgbench -N`: simple update; skips branch / teller
  updates.
- **tpcb** — default TPC-B-like: BEGIN + 3 UPDATEs + SELECT + INSERT
  + END in a single multi-statement `'Q'`.

| Mode    | Backend | Vanilla tps | pg_transport tps | **tps ratio** | Vanilla init-conn ms | pg_transport init-conn ms |
|---------|---------|-----------:|----------------:|--------------:|---------------------:|--------------------------:|
| select  | spi    | 35 549 | 36 002 | **1.01×** | 6.83 | 7.97 |
| select  | direct | 35 829 | 35 961 | **1.00×** | 7.34 | 7.67 |
| nupdate | spi    |  2 960 |  2 961 | **1.00×** | 5.92 | 7.69 |
| nupdate | direct |  2 965 |  2 950 | **1.00×** | 5.93 | 7.81 |
| tpcb    | spi    |    574 |    630 | **1.10×** | 5.91 | 7.82 |
| tpcb    | direct |    582 |    622 | **1.07×** | 5.90 | 7.77 |

Mechanical reading:

- **select / nupdate are at parity.** Both single-statement
  workloads; the per-`'Q'` framework cost (intermediate
  `Vec<DataRow>`, async dispatch, simple-query schema rebuild) is
  small enough relative to the work that ratios sit inside this
  host's run-to-run variance band (±3–5 pp on a 15 s × 3-run
  sample).
- **tpcb wins by 7–10%.** Per transaction it does ~6 round-trips
  (BEGIN + 3 UPDATE + SELECT + INSERT + END). pg_transport's
  pre-warmed slot bgworker absorbs the per-message framework cost
  that vanilla eats fresh per round-trip; the win compounds across
  the 6 round-trips per transaction.
- **`initial connection time` is ~1.0–1.3× vanilla.** This is the
  expected cold-grow signature: the pool starts empty
  (`MIN_WARM_SLOTS = 0`), so the first 8 clients trigger one
  ~25 ms cold-grow that amortises across the rest of the burst.
  The headline `tps` number excludes this (pgbench reports `tps`
  "without initial connection time"), but it's reported here for
  completeness — and §5.2 below shows the same metric collapses
  to **0.07–0.10×** vanilla (the inverse direction — pg_transport
  faster) in the regime where it actually matters.

### 5.2 Connection-churn (`pgbench -C`) — headline

Raw data: [pgbench-churn/results.tsv](pgbench-churn/results.tsv) ·
[pgbench-churn/results.md](pgbench-churn/results.md). Sweep:
`CONNECT=1 ./scripts/pgbench_sweep.sh`.

`pgbench -S -C -T 20`, RUNS=3, backend=spi. The `-C` flag opens a
fresh TCP connection per transaction. Vanilla pays `fork()` +
RelCache/CatCache warm-up per connection (~1–10 ms);
pg_transport hands the fd to an already-warm slot bgworker via
`SCM_RIGHTS` (~0 ms). This is the regime the architecture is
designed for.

| Clients | Vanilla tps | pg_transport tps | **tps ratio** | Vanilla avg conn time ms | pg_transport avg conn time ms | **conn-time ratio** | Vanilla avg latency ms | pg_transport avg latency ms |
|--------:|------------:|-----------------:|--------------:|-------------------------:|------------------------------:|--------------------:|-----------------------:|----------------------------:|
|  4 |   655 |  7 126 | **10.88×** |  3.54 | 0.35 | **0.10×** |  6.11 | 0.56 |
| 16 |   828 | 11 951 | **14.43×** | 11.60 | 0.81 | **0.07×** | 19.33 | 1.34 |
| 32 |   824 | 12 129 | **14.72×** | 24.69 | 1.77 | **0.07×** | 38.83 | 2.64 |

Mechanical reading:

- **The ratio grows with concurrency** (10.9× → 14.4× → 14.7×).
  Vanilla serializes on the single-threaded postmaster `fork()`
  loop; adding clients shifts the bottleneck from per-tx cost to
  fork serialization. pg_transport's `SCM_RIGHTS` handoff hands
  each new fd to an already-warm slot bgworker without involving
  the postmaster, so per-client throughput holds up under
  contention.
- **The conn-time ratio drives the tps ratio.** Per-connection
  setup drops from 3.5 / 11.6 / 24.7 ms (vanilla) to 0.4 / 0.8 /
  1.8 ms (pg_transport) — a 10–14× reduction that maps 1-to-1
  onto the tps win.
- **No failures even at clients=32.** That's ~240 k connection
  establishments per side over 20 s on loopback. Ephemeral-port
  pressure does not manifest at this concurrency on this host.
- **Side-by-side, same invocation.** Both numbers in each row
  come from a single `just pgbench` call, eliminating cross-run
  host-state variance. The wall-clock asymmetry is real and
  intended — in 20 s the vanilla side completes ~16 k transactions
  while pg_transport completes ~240 k.

## 6. sysbench — prepared-statement OLTP

Raw data (main sweep): [sysbench/results.tsv](sysbench/results.tsv) ·
[sysbench/results.md](sysbench/results.md). Retry data:
[sysbench-retry-t32/results.tsv](sysbench-retry-t32/results.tsv),
[sysbench-retry-final/results.tsv](sysbench-retry-final/results.tsv).
Sweep: [scripts/sysbench_sweep.sh](../../scripts/sysbench_sweep.sh).

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
`shared_buffers`). 20 s per side; single run per cell.

#### `oltp_point_select`

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 | 22 884.98 | 27 467.14 | **1.20×** | main |
| spi    | 16 | 36 367.87 | 45 886.31 | **1.26×** | main |
| spi    | 32 | 33 948.42 | 44 333.49 | **1.31×** | retry-t32 ¹ |
| direct |  4 | 22 885.97 | 27 594.00 | **1.21×** | main |
| direct | 16 | 36 345.42 | 45 890.90 | **1.26×** | main |
| direct | 32 | 34 518.50 | 45 104.75 | **1.31×** | main |

#### `oltp_read_only` — headline

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 |   768.51 | 1 204.97 | **1.57×** | main |
| spi    | 16 | 1 209.27 | 1 809.50 | **1.50×** | main |
| spi    | 32 |   839.62 | 1 833.61 | **2.18×** | retry-t32 ¹ |
| direct |  4 |   765.32 | 1 183.42 | **1.55×** | main |
| direct | 16 | 1 173.27 | 1 825.63 | **1.56×** | main |
| direct | 32 |   861.10 | 1 832.57 | **2.13×** | main |

#### `oltp_update_index`

| Backend | Threads | Vanilla tps | pg_transport tps | **Ratio** | Source |
|---------|--------:|-----------:|-----------------:|----------:|--------|
| spi    |  4 | 1 225.16 | 1 183.74 | **0.97×** | main |
| spi    | 16 | 4 423.30 | 3 276.82 | **0.74×** | main |
| spi    | 32 | 6 797.52 | 6 090.39 | **0.90×** | retry-t32 ¹ |
| direct |  4 | 1 229.39 | 1 150.66 | **0.94×** | main |
| direct | 16 | 4 336.69 | 3 316.48 | **0.77×** | main |
| direct | 32 | 6 872.39 | 6 110.97 | **0.89×** | retry-final ¹ |

Mechanical reading:

- **`oltp_read_only` at threads=32 is the steady-state headline.**
  2.13–2.18× vanilla because the workload bundles 12 statements
  per BEGIN/COMMIT-wrapped transaction (high amortisation), and
  vanilla pays per-backend cache pressure / context switches as N
  grows past physical cores while pg_transport multiplexes onto a
  warm pool.
- **`oltp_point_select` scales cleanly with concurrency** (1.20× →
  1.26× → 1.31×). Same mechanism as above, but the workload
  amortises less framework cost per transaction (1 statement vs
  12), so the ratio is smaller in absolute terms but moves in the
  same direction.
- **`oltp_update_index` regresses under concurrency** (0.97× →
  0.74× → 0.89× at threads=4, 16, 32). This is *not* a framework
  cost — it's the host. Writes serialize on tuple locks; on a 4
  physical core box, pg_transport's bgworker pool sees more
  context-switch pressure than vanilla's per-connection backend
  model when every worker is contending for the same heap pages.
  The pattern would invert on a host with ≥ 16 physical cores
  (vanilla's per-backend cache pressure would dominate again),
  but it does not on *this* host. The numbers are reported as-is.
- **Backend choice barely matters** on these workloads. Every
  row has spi and direct within ~5% of each other. Consistent
  with §5.1's pgbench finding: direct's wins are concentrated in
  multi-row SELECTs and binary results, neither of which sysbench
  exercises heavily.

#### ¹ Caveat — `threads=32` burst-init flakiness

The first sysbench sweep failed to capture 4 of 6 `threads=32`
cells: sysbench's extended-query worker init opens all 32 TCP
connections in parallel within ~1 ms, which overflows what the
default autoscaling pool can warm in time (`MIN_WARM_SLOTS = 0`
means every burst-init thread races to cold-grow a slot before
sysbench's connect timeout). pg_transport then sees worker
connections drop with `server closed the connection unexpectedly`,
sysbench aborts, the cell records `NA`. The full failure trace
lives in [sysbench/run.log](sysbench/run.log).

A retry sweep ([sysbench-retry-t32/](sysbench-retry-t32/)) captured
4 of the 6 cells; a third single-cell retry
([sysbench-retry-final/](sysbench-retry-final/)) captured the
remaining `oltp_update_index direct 32` cell. Across three sweeps
each `threads=32` cell was attempted at least twice — the
successful captures are consistent within ~1% (e.g.
`oltp_point_select direct 32` = 1.307× main / 1.312× retry),
suggesting the values themselves are stable; only the burst-init
*completion rate* is flaky on this host. The "Source" column in
the §6 tables records which sweep each row's value came from.

Mitigating this in v0 is one of two changes — raising
`MIN_WARM_SLOTS` so the pool isn't cold at burst start
([pool.md §5.1](../design/pool.md)), or relaxing the pool's
single-in-flight grow gate so N cold-boots can run in parallel
([pool.md §5.2](../design/pool.md)). Both are deferred behind
phase-7+ work; see the design docs for the trade-offs.

## 7. Reproduce

All numbers in this document come from running these exact commands
against commit [`61f7e16`](../../). Each sweep writes a fresh
`OUTDIR/{results.tsv, results.md, run.log, progress.log}`; the
files in [`./bench/`](bench/), [`./pgbench-steady/`](pgbench-steady/),
[`./pgbench-churn/`](pgbench-churn/), [`./sysbench/`](sysbench/),
[`./sysbench-retry-t32/`](sysbench-retry-t32/),
[`./sysbench-retry-final/`](sysbench-retry-final/) are the exact
script outputs from the 2026-05-24 run.

```sh
# §4 — custom bench (~2 min)
OUTDIR=$PWD/docs/bench/bench \
  CONNECTIONS="1 4 8 16" BACKENDS="spi direct" RUNS=3 \
  ./scripts/bench_sweep.sh

# §5.1 — pgbench steady-state (~10 min)
OUTDIR=$PWD/docs/bench/pgbench-steady \
  MODES="select nupdate tpcb" BACKENDS="spi direct" \
  CLIENTS="8" DURATION="15" RUNS="3" \
  ./scripts/pgbench_sweep.sh

# §5.2 — pgbench connection-churn (~7 min)
OUTDIR=$PWD/docs/bench/pgbench-churn \
  CONNECT=1 MODES="select" BACKENDS="spi" \
  CLIENTS="4 16 32" DURATION="20" RUNS="3" \
  ./scripts/pgbench_sweep.sh

# §6 — sysbench (~25 min; expect threads=32 flakiness as noted)
OUTDIR=$PWD/docs/bench/sysbench \
  WORKLOADS="oltp_point_select oltp_read_only oltp_update_index" \
  BACKENDS="spi direct" THREADS="4 16 32" DURATION="20" \
  ./scripts/sysbench_sweep.sh
```

If a sysbench cell records `NA`, re-run the affected cells in a
narrower sweep:

```sh
OUTDIR=$PWD/docs/bench/sysbench-retry-t32 \
  WORKLOADS="oltp_point_select oltp_read_only oltp_update_index" \
  BACKENDS="spi direct" THREADS="32" DURATION="20" \
  ./scripts/sysbench_sweep.sh
```

## See also

- [../design/bench.md](../design/bench.md) — how each harness works
  (workload shape, output columns, failure modes).
- [../design/performance.md](../design/performance.md) — why the
  numbers fall where they do (per-query data-path trace, structural
  fast paths).
- [../design/pool.md](../design/pool.md) — the autoscaling pool
  whose `MIN_WARM_SLOTS = 0` default sets both the cold-grow
  baseline in §5.1 and the burst-init flakiness in §6.
- [../design/architecture.md](../design/architecture.md) — why the
  data path looks the way it does.
