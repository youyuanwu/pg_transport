# Bench harness — what it measures, what to read into it

> Parent: [README.md](README.md)
> Siblings: [testing.md](testing.md) · [roadmap.md](roadmap.md)

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
just bench [pg] [iters] [connections] [pool] [csv]
```

Positional arguments (just 1.51 has no kwarg form — see
[just/bench.just](../../just/bench.just)):

| Position | Name          | Default      | Meaning                                                                                       |
| -------- | ------------- | ------------ | --------------------------------------------------------------------------------------------- |
| 1        | `pg`          | `pg18`       | Postgres major (only `pg18` provisioned in v0).                                               |
| 2        | `iters`       | `1000`       | **Total** measured iterations per side, split across workers.                                 |
| 3        | `connections` | `1`          | Concurrent worker connections per side; bound by `pg_transport.backend_pool_size` (see [Q10](roadmap.md#22-still-open--deferred-only)). |
| 4        | `pool`        | `""`         | Override `pg_transport.backend_pool_size`. Empty ⇒ `pool = connections` (apples-to-apples, no contention). |
| 5        | `csv`         | `""`         | Optional per-sample CSV path: `label,iter,latency_ns`.                                        |

### 1.1 Workload shape

Per [`run_one_worker`](../../crates/bench/src/main.rs):

1. Open a `tokio_postgres` connection.
2. Run `warmup` (default 100) `SELECT 1` round-trips — primes the
   slot, primes the plan cache, warms libc allocators.
3. Hit a barrier shared with all other workers (timeout: 30 s; see
   §1.4 below).
4. Run `iterations / connections` measured `SELECT 1` round-trips,
   recording per-query latency.

Round-trip uses `simple_query("SELECT 1")` — same wire path psql
`-c` uses (`'Q'` message in pgwire v3). Extended-query path
(`Parse`/`Bind`/`Execute`) is **not** exercised; that lands in
phase 9.

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
| `barrier timeout after 30s …`                                          | `connections > pg_transport.backend_pool_size`. v0 pins one TCP connection per slot ([Q10](roadmap.md#22-still-open--deferred-only)); extras queue on the slot's UDS and never finish startup. Either lower `connections` or raise `pool`. |
| `slot N did not connect within 30s` from `BackendPool::start`          | `max_worker_processes` ceiling hit. The recipe sets it to `pool + 8`; if you bypass the recipe, set it yourself.                                              |
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
just pgbench [pg] [mode] [duration] [clients] [pool]
```

Positional arguments:

| Position | Name       | Default | Meaning                                                                                                  |
| -------- | ---------- | ------- | -------------------------------------------------------------------------------------------------------- |
| 1        | `pg`       | `pg18`  | Postgres major.                                                                                          |
| 2        | `mode`     | `select`| `select` runs `pgbench -S` (read-only). `tpcb` / `tpcb-rw` are **rejected by v0** — see §2.5.            |
| 3        | `duration` | `10`    | Seconds (`pgbench -T`).                                                                                   |
| 4        | `clients`  | `4`     | Concurrent clients per side (`pgbench -c -j`); bound by `backend_pool_size` against pg_transport.        |
| 5        | `pool`     | `""`    | Override `backend_pool_size`. Empty ⇒ `pool = clients`.                                                  |

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

- **`pgbench -i` runs against the vanilla port, not pg_transport.** pgbench's `-i` uses the extended-query protocol for its `regclass` lookup (`SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::regclass`); the v0 wire layer only handles simple-query messages — phase 9 territory. The recipe always initialises via the vanilla port; both listeners share the same data files. The actual benchmark phase (`-S` or `tpcb`) runs against pg_transport via simple-query.
- **`-M simple` only.** `-M extended` and `-M prepared` need phase 9 for the same reason as `-i`.
- **Multi-statement simple-query is broken** (tracked as Q25 in [roadmap.md](roadmap.md#21-still-open--v0-path)). A single `'Q'` message body with multiple statements separated by `;` (e.g. `psql -c 'SELECT 1; SELECT 2'`) returns only the last result; leading SELECT rows are silently dropped, and multi-statement strings containing `BEGIN`/`COMMIT` fall through to SPI atomic-mode rejection. pgbench is unaffected — each statement in its scripts is sent as its own `'Q'` — but `psql -c 'A; B'` users will hit it. Fix needs a real SQL splitter (`pg_query` / libpg_query).
- **Error inside an explicit `BEGIN` block collapses to `TBLOCK_DEFAULT`, not `TBLOCK_ABORT`.** Real PG marks the transaction as aborted and rejects every subsequent statement until `ROLLBACK`; we auto-abort to `DEFAULT` so subsequent statements run as if in a fresh auto-commit context. Harmless for pgbench (which has no expected error paths); phase ≥ 9 fixes when a real client cares.
- **No auth.** All connections use `-U postgres -d postgres` with trust. Phase 7 plumbs real auth.
- **No TLS.** Phase 8.
- **Bench tables live in database `postgres`.** v0 slots hard-code the SPI database to `postgres` (see [`crates/core/src/backend/slot.rs`](../../crates/core/src/backend/slot.rs)). Phase 7 routes by `StartupMessage.database`.

### 2.6 What current numbers look like

`pgbench -T 10 -c 4 -j 4` against a fresh cluster, post-fix:

```
mode=select (pgbench -S)
  vanilla PG       :  17047 tps   latency_avg 0.235 ms
  pg_transport     :  16416 tps   latency_avg 0.244 ms     (96.3% of vanilla)

mode=tpcb
  vanilla PG       :   2249 tps   latency_avg 1.779 ms
  pg_transport     :   2358 tps   latency_avg 1.696 ms     (104.9% of vanilla)
```

Mechanism:

- `-S` is a real single-row index lookup; vanilla does the work and replies, pg_transport does the same work plus the extra hop. Hop cost is fixed; relative overhead shrinks as underlying query gets heavier but selects this cheap are roughly the worst case for the architecture.
- `tpcb` per transaction does ~4 statements + `BEGIN` + `END`. The 6 round-trips amortise the per-message framework cost, and pg_transport's pre-warmed bgworker pool absorbs jitter that vanilla eats fresh per connection — net result, pg_transport beats vanilla on this workload at this scale. This is the architecture's headline workload.

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
