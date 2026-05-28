# pg_transport — benchmarks

This folder holds reproducible benchmark sweeps comparing
pg_transport against vanilla PostgreSQL across four regimes —
latency floor, steady-state OLTP, connection-churn, and
prepared-statement scaling.

**This file is the general info** — methodology, how each sweep
works, and the commands to reproduce a run. **Run-specific data
lives in dated subfolders**, each with its own README that
captures the commit tested, host snapshot, headline numbers, and
caveats from that run.

For the *mechanics* of how each harness works see
[../design/bench.md](../design/bench.md); for the *theory* of why
the numbers fall where they do see
[../design/performance.md](../design/performance.md).

## Runs

| Date | Commit | Notes |
| ---- | ------ | ----- |
| [2026-05-28](2026-05-28/) | [`d3104d0`](../../) | Current run. Same host as 2026-05-24; ratios reproduce within run-to-run noise. |
| [2026-05-24](2026-05-24/) | [`61f7e16`](../../) | Initial published sweep. |

## Methodology

Each sweep is driven by one of the three sweep scripts in
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
| `sysbench-retry-*` | varies | 1       | ~85 s             | re-runs for cells that hit burst-init flakiness — see per-run README §5.1 |

`RUNS=3` for the bench / pgbench sweeps means each `pg_transport`
vs `vanilla` ratio is the arithmetic mean of three same-cell runs.
The sysbench sweep does not have a built-in `RUNS` axis; each
sysbench cell is a single 20 s run.

All workloads use libpq's simple-query protocol (`-M simple` /
default) except sysbench, which uses libpq's extended-query /
prepared-statement path. Both paths route through pg_transport's
slot bgworker via the FE `SCM_RIGHTS` handoff; the SQL execution
inside the slot uses either `SPI_execute_plan_with_params`
(backend=spi) or `CreateCachedPlan + Portal*` (backend=direct);
see [../design/architecture.md](../design/architecture.md).

## Reproduce

The commands below reproduce a full sweep set. Each sweep writes
a fresh `OUTDIR/{results.tsv, results.md, run.log,
progress.log}`. Pick an `OUTDIR` path (typically a dated
subfolder so per-run artefacts stay co-located), then:

```sh
DATE="$(date -u +%Y-%m-%d)"
OUT="$PWD/docs/bench/${DATE}"
mkdir -p "$OUT"

# Custom bench — single SELECT 1 round-trip floor (~2 min)
OUTDIR="$OUT/bench" \
  CONNECTIONS="1 4 8 16" BACKENDS="spi direct" RUNS=3 \
  ./scripts/bench_sweep.sh

# pgbench steady-state — real OLTP, long-lived conns (~10 min)
OUTDIR="$OUT/pgbench-steady" \
  MODES="select nupdate tpcb" BACKENDS="spi direct" \
  CLIENTS="8" DURATION="15" RUNS="3" \
  ./scripts/pgbench_sweep.sh

# pgbench connection-churn — architectural headline (~7 min)
OUTDIR="$OUT/pgbench-churn" \
  CONNECT=1 MODES="select" BACKENDS="spi" \
  CLIENTS="4 16 32" DURATION="20" RUNS="3" \
  ./scripts/pgbench_sweep.sh

# sysbench — prepared-statement OLTP at scale (~25 min;
# expect threads ≥ 16 burst-init flakiness as noted below)
OUTDIR="$OUT/sysbench" \
  WORKLOADS="oltp_point_select oltp_read_only oltp_update_index" \
  BACKENDS="spi direct" THREADS="4 16 32" DURATION="20" \
  ./scripts/sysbench_sweep.sh
```

Also capture a host snapshot for the run-specific README:

```sh
{
  echo "=== uname ==="; uname -a; echo
  echo "=== /etc/os-release ==="; cat /etc/os-release; echo
  echo "=== lscpu ==="; lscpu; echo
  echo "=== free -h ==="; free -h; echo
  echo "=== df -h ==="; df -h; echo
  echo "=== /proc/cpuinfo (first cpu) ==="
  awk '/^processor/ {if(p==1) exit; p=1} p' /proc/cpuinfo; echo
  echo "=== systemd-detect-virt ==="; systemd-detect-virt || true; echo
  echo "=== tool versions ==="
  rustc --version; cargo --version; just --version
  sysbench --version 2>&1 | head -1
  "$HOME"/.pgrx/18.4/pgrx-install/bin/pgbench --version | head -1
  "$HOME"/.pgrx/18.4/pgrx-install/bin/postgres --version | head -1
} > "$OUT/host.txt"
```

### Handling sysbench burst-init flakiness

The sysbench sweep at `threads ≥ 16` can race with the
pg_transport pool's cold-grow path and lose cells to `NA`. See
the per-run README §5.1 for the failure mechanism and how it was
worked around in each run. Typical recovery — retry just the
affected cells in a narrower sweep, e.g.:

```sh
# all threads=32 cells, all workloads
OUTDIR="$OUT/sysbench-retry-t32" \
  WORKLOADS="oltp_point_select oltp_read_only oltp_update_index" \
  BACKENDS="spi direct" THREADS="32" DURATION="20" \
  ./scripts/sysbench_sweep.sh

# single straggler cell
OUTDIR="$OUT/sysbench-retry-final-1" \
  WORKLOADS="oltp_update_index" BACKENDS="direct" THREADS="32" \
  DURATION="20" ./scripts/sysbench_sweep.sh
```

Mitigating the flakiness structurally (raising `MIN_WARM_SLOTS`
or relaxing the pool's single-in-flight grow gate) is deferred —
see [../design/pool.md](../design/pool.md) §5 for the trade-offs.

## Writing up a new run

When publishing a new run, drop a `README.md` into the dated
subfolder using the existing per-run READMEs as templates. The
structure each run-specific README follows:

1. **TL;DR** — one-line headline ratios per regime, plus a
   sentence comparing to the prior run.
2. **Host** — kernel/CPU/RAM snapshot + tool versions, with a
   link to `host.txt` for the full dump.
3. **Custom bench** — table of p50/p95/qps per (connections,
   backend), 3-run means, mechanical reading.
4. **pgbench** — steady-state (§4.1) and connection-churn (§4.2)
   tables, 3-run means, mechanical reading.
5. **sysbench** — three workloads × spi/direct × threads tables,
   single capture per cell, with a "Source" column when retry
   sweeps were needed. Caveats (burst-init flakiness, degenerate
   captures) go in §5.1.
6. **See also** — link back to this file and to the design docs.

Keep the run-specific TL;DR ratios within ±10% of the published
prior run before publishing — if a regime drifts further, it's
usually either a real regression (worth investigating before
publishing) or host noise (worth re-running before publishing).
The custom-bench `qps ratio` row is the most reproducible signal
for sanity-checking the host state.

## See also

- [../design/bench.md](../design/bench.md) — how each harness
  works (workload shape, output columns, failure modes).
- [../design/performance.md](../design/performance.md) — why the
  numbers fall where they do (per-query data-path trace,
  structural fast paths).
- [../design/pool.md](../design/pool.md) — the autoscaling pool
  whose `MIN_WARM_SLOTS = 0` default sets both the cold-grow
  baseline and the sysbench burst-init flakiness ceiling.
- [../design/architecture.md](../design/architecture.md) — why
  the data path looks the way it does.
