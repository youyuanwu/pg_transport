# Bench harness — what it measures, what to read into it

> Parent: [README.md](README.md)
> Siblings: [testing.md](testing.md) · [roadmap.md](roadmap.md) · [performance.md](performance.md) (what the numbers mean + ranked optimisations)

The bench harness is the phase-5 deliverable per
[roadmap.md §1](roadmap.md#1-phased-build-plan). Two complementary
recipes exist:

| Recipe          | Driver                                     | What it actually exercises                                                                 |
| --------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `just bench`    | [`crates/bench`](../../crates/bench)       | Custom tokio-postgres harness; `SELECT 1` round-trips at chosen concurrency.               |
| `just pgbench`  | [`pgbench`](https://www.postgresql.org/docs/current/pgbench.html) shipped with PG | Real-world TPC-B-like workload (`-S` select-only and full tpcb mode).        |

Both spin up an ephemeral cluster with `shared_preload_libraries =
'pg_transport'`, run the workload against both the pg_transport
listener (`127.0.0.1:5454`) and the cluster's vanilla PG listener
(`127.0.0.1:54329`), print a side-by-side comparison, then tear
the cluster down.

---

## 1. `just bench` — custom harness

```sh
just bench [pg] [iters] [connections] [pool] [csv] [mode]
```

Positional arguments (just 1.51 has no kwarg form — see
[just/bench.just](../../just/bench.just)):

| Position | Name          | Default      | Meaning                                                                                       |
| -------- | ------------- | ------------ | --------------------------------------------------------------------------------------------- |
| 1        | `pg`          | `pg18`       | Postgres major (only `pg18` provisioned in v0).                                               |
| 2        | `iters`       | `1000`       | **Total** measured iterations per side, split across workers.                                 |
| 3        | `connections` | `1`          | Concurrent worker connections per side; bound by `pg_transport.max_backend_pool_size` (autoscaling ceiling — see [pool.md](pool.md) and [Q10](roadmap.md#22-still-open--deferred-only)). |
| 4        | `pool`        | `""`         | Override `pg_transport.max_backend_pool_size`. Empty ⇒ `pool = connections` (apples-to-apples, no contention). |
| 5        | `csv`         | `""`         | Optional per-sample CSV path: `label,iter,latency_ns`.                                        |
| 6        | `mode`        | `spi`        | pg_transport execution backend mode: `spi` (default) or `direct` (`SET pg_transport.execution_backend`). |

### 1.1 Workload shape

Per [`run_one_worker`](../../crates/bench/src/main.rs):

1. Open a `tokio_postgres` connection.
2. `SET pg_transport.execution_backend` for the connection based on
  selected mode (`spi` or `direct`).
3. Run `warmup` (default 100) `SELECT 1` round-trips (extended-query) — primes the
   slot, primes the plan cache, warms libc allocators.
4. Hit a barrier shared with all other workers (timeout: 30 s; see
   §1.4 below).
5. Run `iterations / connections` measured `SELECT 1` round-trips,
   recording per-query latency.

Round-trips always use extended-query (`query_opt("SELECT 1", &[])`) so
the selected backend mode is actually exercised.

### 1.2 Output

```
connections=8
stat        vanilla µs  pg_transport µs         diff µs       pt/pg
----------  ----------  --------------  --------------  ----------
min              123.7           126.5             2.9       1.02x
p50              237.2           226.3             0.0       0.95x
p95              316.1           311.4             0.0       0.99x
p99              360.5           346.8             0.0       0.96x
max             1311.4           525.0             0.0       0.40x
mean             236.3           228.9             0.0       0.97x
qps           33570.6         34382.6                       1.02x
```

Per row:
- **min**: fastest sample. Floor of "how cheap can a round-trip
  possibly be" on this machine + path. pg_transport's extra hop
  (TCP → FE bgworker → SCM_RIGHTS → slot bgworker → SPI) means
  the floor is structurally a few µs higher than vanilla.
- **p50 / p95 / p99**: latency percentiles via nearest-rank method
  on the sorted sample set across all workers.
- **max**: worst single sample. Captures GC pauses, scheduler
  jitter, autovacuum bumps.
- **mean**: arithmetic mean; included for completeness but `p50`
  is usually more informative.
- **qps**: `total_iterations / wall_clock` of the measured phase
  (NOT `1 / mean`, which over-counts under concurrency).
- **diff µs**: `max(0, pg_transport - vanilla)`. Negative
  differences are clamped to 0 so the column reads as "how much
  extra time did pg_transport take" without sign clutter.
- **pt/pg**: `pg_transport / vanilla` as a ratio. **< 1.0 means
  pg_transport is better; > 1.0 means worse.**

### 1.3 What the numbers can and cannot tell you

**Can tell you:**
- *Whether the architecture is paying a tax at idle*. `min` and
  `p50` at `connections=1` measure the structural per-hop cost.
- *Whether the pool absorbs jitter*. `max` consistently lower under
  pg_transport at `connections >= 8` is the signature of pre-warmed
  bgworkers absorbing variance that vanilla PG eats fresh per
  connection.
- *Whether throughput scales*. `qps` ratio across runs at increasing
  `connections` (1 → 4 → 8 → 16) shows whether either side
  starts saturating.

**Cannot tell you:**
- *Connection-setup wins*. The harness opens connections once,
  re-uses them for thousands of queries. Vanilla PG's fork cost is
  amortised to zero. Use `pgbench` short-session mode for that.
- *TLS handshake cost*. Phase 8 work; the harness disables TLS
  (`NoTls`).
- *Real query cost*. `SELECT 1` is cheaper than the per-hop
  framework overhead, so the framework dominates. Real workloads
  swing the ratio toward 1.0x because the hop becomes a smaller
  fraction of total work.
- *Anything about extended-query / prepared statements*. Phase 9.
- *Anything about long-running connections under realistic
  concurrency*. The bench is a closed-loop, fixed-arrival-rate
  burst; production workloads are more spiky.

### 1.4 Failure modes the harness surfaces

| Symptom                                                                | What it means                                                                                                                                                |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `barrier timeout after 30s …`                                          | `connections > pg_transport.max_backend_pool_size`. v0 pins one TCP connection per slot ([Q10](roadmap.md#22-still-open--deferred-only)); extras saturate the pool and get TCP-reset on `HANDOFF_WAIT` timeout (5 s) per [pool.md §5.5](pool.md#55-saturation-behavior). Either lower `connections` or raise `pool`. |
| `slot did not become ready within 5s` in server log                    | Same root cause as above (pool ceiling hit). See [pool.md §5.5](pool.md#55-saturation-behavior).                                                            |
| Compare numbers swing wildly between runs                              | Other process on the box. Pin one socket via `taskset`, disable turbo, or just re-run.                                                                       |
| pg_transport `min` lower than vanilla `min`                            | Likely noise — the structural floor is higher for pg_transport. If reproducible across many runs, suspect a measurement skew.                                |

### 1.5 Recommended sweep

A useful "is the architecture pulling its weight?" sweep:

```sh
just bench pg18 4000 1
just bench pg18 4000 4
just bench pg18 8000 8
just bench pg18 8000 16
```

Read `p50` (median latency) and `max` (tail latency). A healthy v0
result looks like: `p50` within ±10% of vanilla across the sweep,
`max` consistently better at `connections ≥ 8`. As of phase 5+
that is roughly what we observe; see [`reviews/`](reviews/) for
captured runs.

---

## 2. `just pgbench` — TPC-B-like workload via pgbench

```sh
just pgbench [pg] [mode] [duration] [clients] [pool] [backend]
```

Positional arguments:

| Position | Name       | Default | Meaning                                                                                                  |
| -------- | ---------- | ------- | -------------------------------------------------------------------------------------------------------- |
| 1        | `pg`       | `pg18`  | Postgres major.                                                                                          |
| 2        | `mode`     | `select`| `select` runs `pgbench -S` (read-only), `nupdate` runs `pgbench -N` (simple update), and `tpcb` / `tpcb-rw` run default TPC-B-like mode. |
| 3        | `duration` | `10`    | Seconds (`pgbench -T`).                                                                                   |
| 4        | `clients`  | `4`     | Concurrent clients per side (`pgbench -c -j`); bound by `max_backend_pool_size` against pg_transport.   |
| 5        | `pool`     | `""`    | Override `max_backend_pool_size`. Empty ⇒ `pool = clients`.                                            |
| 6        | `backend`  | `spi`   | pg_transport execution backend (`spi` or `direct`) passed via `PGOPTIONS=-c pg_transport.execution_backend=...` on the pg_transport-side run. |

### 2.1 Initialisation

`pgbench -i` runs against the **vanilla port** (54329). Initialisation
is heavy DDL/DML and bench tables only need to exist on disk once —
pg_transport reads/writes the same data files via SPI.

The recipe drops the bench tables in `cleanup()` so re-running gives
a deterministic baseline.

### 2.2 Workload shape and what it adds over `just bench`

| `mode`    | pgbench script             | What it exercises that `just bench` does not                                                       |
| --------- | -------------------------- | --------------------------------------------------------------------------------------------------- |
| `select`  | `<builtin: select only>`   | Real index lookup on `pgbench_accounts` instead of `SELECT 1` — moves the cost from "framework hop only" toward "framework hop + real query work".                |
| `nupdate` | `<builtin: simple update>` | Write-heavy transaction without branch/teller updates. Good midpoint between `-S` and full TPC-B. |
| `tpcb`    | `<builtin: TPC-B (sort of)>` | Adds writes (`UPDATE`, `INSERT`), explicit transaction blocks (`BEGIN` / `END`), and 4 statements per transaction. Tests the SPI bridge's read-write path, the xact-control sniff that routes `BEGIN` / `COMMIT` around SPI's atomic mode (see [`spi_bridge.rs`](../../crates/core/src/backend/spi_bridge.rs) `parse_xact_control`), and per-transaction latency under realistic workload shape. |

### 2.3 Output

pgbench's own report; key fields:

```
transaction type: <builtin: select only>
scaling factor: 1
query mode: simple
number of clients: 4
number of threads: 4
duration: 10 s
number of transactions actually processed: 132450
latency average = 0.302 ms
tps = 13245.0
```

`number of failed transactions: > 0` is a **failure**, not a warning —
investigate, do not paper over.

### 2.4 What pgbench can tell you that `just bench` cannot

- **Real query cost (`-S`)**: pgbench's select-only script is `SELECT abalance FROM pgbench_accounts WHERE aid = :aid` — a real index lookup, not `SELECT 1`. Moves the cost mix from "100% framework hop" toward "hop + real query work".
- **Write path (`tpcb`)**: `UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid` plus two more updates plus an INSERT into `pgbench_history`. The `SELECT 1` bench never touches the write path.
- **Explicit transaction blocks (`tpcb`)**: `tpcb` wraps four statements in `BEGIN`/`END`. The xact-control sniff in the SPI bridge intercepts these before SPI sees them; a regression in that sniff (or in PG's `BeginTransactionBlock` / `EndTransactionBlock` dance) is invisible to `just bench`.
- **Per-transaction latency at realistic granularity**: pgbench measures wall-clock per transaction (4 statements + 2 xact boundary commands in `tpcb`), not per-round-trip. Closer to what an application sees.
- **Comparable to PG community numbers**: pgbench is the conventional yardstick; results are roughly comparable to anyone else's published numbers on similar hardware.

### 2.5 Known limitations (v0)

Limitations the recipe surfaces today. These are the next things to fix if you want pgbench coverage beyond the current scope.

- **`pgbench -i` runs against the vanilla port, not pg_transport.** pgbench's `-i` uses the extended-query protocol for its `regclass` lookup (`SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::regclass`); the v0 wire layer only handles simple-query messages — phase 9 territory. The recipe always initialises via the vanilla port; both listeners share the same data files. The actual benchmark phase (`-S`, `-N`, or `tpcb`) runs against pg_transport via simple-query.
- **`-M simple` only.** `-M extended` and `-M prepared` need phase 9 for the same reason as `-i`. The recipe exposes a `backend` arg (`spi|direct`) for pg_transport, but with `-M simple` its practical effect is limited until extended/prepared modes are supported end-to-end.
- **Error inside an explicit `BEGIN` block collapses to `TBLOCK_DEFAULT`, not `TBLOCK_ABORT`.** Real PG marks the transaction as aborted and rejects every subsequent statement until `ROLLBACK`; we auto-abort to `DEFAULT` so subsequent statements run as if in a fresh auto-commit context. Harmless for pgbench (which has no expected error paths); phase ≥ 9 fixes when a real client cares.
- **No auth.** All connections use `-U postgres -d postgres` with trust. Phase 7 plumbs real auth.
- **No TLS.** Phase 8.
- **Bench tables live in database `postgres`.** v0 slots hard-code the SPI database to `postgres` (see [`crates/core/src/backend/slot.rs`](../../crates/core/src/backend/slot.rs)). Phase 7 routes by `StartupMessage.database`.

### 2.6 What current numbers look like

Recent stable runs (`-T 30 -c 8 -j 8`, `pool=8`) show:

```
mode=select (pgbench -S), backend=spi
  vanilla PG       : 27069.7 tps   latency_avg 0.296 ms
  pg_transport     : 25138.7 tps   latency_avg 0.318 ms   (~92.9% of vanilla TPS)

mode=select (pgbench -S), backend=direct
  vanilla PG       : 26641.6 tps   latency_avg 0.300 ms
  pg_transport     : 25111.4 tps   latency_avg 0.319 ms   (~94.3% of vanilla TPS)

mode=nupdate (pgbench -N), backend=spi
  vanilla PG       :  6756.2 tps   latency_avg 1.184 ms
  pg_transport     :  6310.0 tps   latency_avg 1.268 ms   (~93.4% of vanilla TPS)

mode=nupdate (pgbench -N), backend=direct
  vanilla PG       :  6332.8 tps   latency_avg 1.263 ms
  pg_transport     :  6346.8 tps   latency_avg 1.260 ms   (~100.2% of vanilla TPS)
```

Mechanism:

- `-S` is a real single-row index lookup; vanilla does the work and replies, pg_transport does the same work plus the extra hop AND a per-query parse via PG's `raw_parser` (resolves Q25 — needed to split multi-statement strings and classify xact-control). The parse cost is ~5 µs/query; hop dominates the remaining delta.
- `-N` (`simple update`) is write-heavier and narrows the relative framework tax because each transaction does more useful server work than `-S`.
- `tpcb` per transaction does ~4 statements + `BEGIN` + `END`. The 6 round-trips amortise the per-message framework cost, including the per-query parse; pg_transport's pre-warmed bgworker pool absorbs jitter that vanilla eats fresh per connection.
- **Q25 resolution path**: an earlier attempt at sqlparser-rs cost ~15 µs/query (~5% throughput regression). Replaced with PG's in-process `raw_parser` (3-5× faster, perfect grammar fidelity, zero new deps); see [roadmap.md §2.3 Q25](roadmap.md). We still parse twice (once in our classifier, once inside SPI) but both are PG's own ~5 µs parser. Getting to "parse once" requires bypassing SPI with a direct `Portal`/`pg_analyze_and_rewrite_fixedparams`/`pg_plan_queries` path; deferred to phase 9.

Concrete numbers from recent runs live in [`reviews/`](reviews/); design decisions that change the SPI bridge or wire layer should re-run and update.

---

## 3. Source-of-truth pointers

- Custom harness: [`crates/bench/src/main.rs`](../../crates/bench/src/main.rs)
- `just bench` recipe: [`just/bench.just`](../../just/bench.just)
- `just pgbench` recipe: [`just/pgbench.just`](../../just/pgbench.just)
- SPI bridge (the thing both harnesses ultimately stress on the
  pg_transport side): [`crates/core/src/backend/spi_bridge.rs`](../../crates/core/src/backend/spi_bridge.rs)
- Pool sizing GUC: [`crates/core/src/guc.rs`](../../crates/core/src/guc.rs)
- Capacity caveats (`max_worker_processes`): [`crates/core/src/lib.rs`](../../crates/core/src/lib.rs) `_PG_init`
