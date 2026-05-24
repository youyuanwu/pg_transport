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
just pgbench [pg] [mode] [duration] [clients] [pool] [backend] [connect] [script]
```

Positional arguments:

| Position | Name       | Default | Meaning                                                                                                  |
| -------- | ---------- | ------- | -------------------------------------------------------------------------------------------------------- |
| 1        | `pg`       | `pg18`  | Postgres major.                                                                                          |
| 2        | `mode`     | `select`| `select` runs `pgbench -S` (read-only), `nupdate` runs `pgbench -N` (simple update), and `tpcb` / `tpcb-rw` run default TPC-B-like mode. Ignored when `script` is set. |
| 3        | `duration` | `10`    | Seconds (`pgbench -T`).                                                                                   |
| 4        | `clients`  | `4`     | Concurrent clients per side (`pgbench -c -j`); bound by `max_backend_pool_size` against pg_transport.   |
| 5        | `pool`     | `""`    | Override `max_backend_pool_size`. Empty ⇒ `pool = clients`.                                            |
| 6        | `backend`  | `spi`   | pg_transport execution backend (`spi` or `direct`) passed via `PGOPTIONS=-c pg_transport.execution_backend=...` on the pg_transport-side run. |
| 7        | `connect`  | `0`     | `1` appends `-C` (reconnect per transaction) to pgbench. Triggers the connection-churn showcase — see §2.7. Also auto-bumps `max_connections` to `max(100, clients * 4)` to survive the reconnect burst. |
| 8        | `script`   | `""`    | Custom pgbench script path (`-f`). Overrides `mode`. Use [`bench/scripts/select_one.sql`](../../bench/scripts/select_one.sql) for the minimal-execution probe that pairs with `connect=1`. |

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

### 2.7 Connection-churn mode (`connect=1`)

The steady-state numbers in §2.6 measure the framework cost when
connections are long-lived and the per-connection fork/init is
amortised to zero — the regime where `pg_transport` is, by design,
at parity with vanilla. The connection-churn mode is where the
architecture *wins*.

**What it does:** appends pgbench's `-C` flag, which opens a fresh
TCP connection per transaction. Vanilla PG pays the full
per-connection cost on every transaction; `pg_transport` hands the
fd to an already-warm slot bgworker via `SCM_RIGHTS` and pays
essentially nothing (see [frontend-handoff.md](frontend-handoff.md)).

**Per-transaction cost decomposition:**

| Component                                | Vanilla PG          | pg_transport (handoff)    |
| ---------------------------------------- | ------------------- | ------------------------- |
| `fork()` postmaster → backend            | ~1–2 ms             | **0** (slot already alive)|
| Backend init (RelCache / CatCache warm)  | ~0.5–1 ms (warm)    | **0** (slot is hot)       |
| Authentication round-trip                | ~0.2 ms             | ~0.2 ms                   |
| TCP handshake (loopback)                 | ~0.1 ms             | ~0.1 ms                   |
| `SELECT 1` execution                     | ~0.05 ms            | ~0.05 ms                  |
| Backend exit + cleanup                   | ~0.5 ms             | **0** (slot returns to pool)|
| **Per-tx total**                         | **~2–4 ms**         | **~0.4 ms**               |

Expected ratio: **5–15×** depending on whether vanilla's catalog
caches are warm and on hardware. Published pgbouncer-vs-vanilla
numbers in the same regime stabilise around 4–8× for the same
reason — we're solving the same problem.

**Sample invocations:**

```sh
just pgbench pg18 select 30 32 "" spi 1                                # -C with built-in -S
just pgbench pg18 select 30 32 "" spi 1 bench/scripts/select_one.sql   # -C with minimal probe
just pgbench pg18 select 30 32 "" spi 0                                # baseline (no -C)
```

Run the `connect=0` / `connect=1` pair on the same hardware in the
same harness invocation — the ratio between them is the
connection-churn story we publish.

**Custom-script probe ([`bench/scripts/select_one.sql`](../../bench/scripts/select_one.sql)):**
A bare `SELECT 1;` rather than pgbench's built-in select-only
(`SELECT abalance FROM pgbench_accounts WHERE aid = :aid`). The
built-in does a real index lookup that adds ~50 µs of execution per
tx; the custom probe drops that to near zero so the `connect=1`
ratio reflects pure connection-setup overhead with no query-cost
confounder.

**Caveats:**

- **`max_connections` auto-bump.** The recipe sets
  `max_connections = max(100, clients * 4)` whenever `connect=1`.
  Default 100 chokes under burst at `clients ≥ 32` because backends
  linger briefly in lifecycle states after disconnect, and pgbench's
  `-C` reconnect rate outpaces that drain. The bump is in the
  generated cluster config only — it does not affect any other
  recipe.
- **Ephemeral-port pressure.** At very high `clients` (≥64) on
  loopback, the kernel's ephemeral port range (default ~28k) plus
  `TIME_WAIT` (60 s default) can exhaust source ports during the
  reconnect burst. Symptom: `pgbench` reports connection failures
  partway through the run. Mitigation: lower `clients` or shorten
  `duration`. We don't ship sysctl tuning in the recipe.
- **Vanilla side does most of the work.** Because vanilla is the
  one paying the cost, the per-side runtime is asymmetric: the
  vanilla run takes meaningfully longer wall-clock than the
  pg_transport run at the same `-T`, because vanilla completes
  fewer transactions per second. This is the point of the
  benchmark, not a bug.

---

## 3. Planned showcase work

The §1 and §2 recipes cover **steady-state same-connection** workloads.
At fixed concurrency with long-lived connections, the framework hop is
a meaningful fraction of cost but the *absolute* win is small (within
~10%, often within measurement noise) because vanilla PG amortises its
per-connection cost over thousands of queries.

The architectural wins that motivate `pg_transport` show up in two
regimes neither recipe currently exercises:

1. **Connection churn** — workloads that open a fresh TCP connection
   per transaction. Vanilla PG pays `fork()` + RelCache/CatCache
   warm-up per connection (~1–10 ms); `pg_transport` hands the fd to
   an already-warm slot bgworker (~0 ms). Expected ratio: **5–15×**
   on `pgbench -S -C` at moderate concurrency.
2. **TPC-C-class OLTP at high concurrency** — many simultaneous
   sessions issuing parameterised multi-statement transactions. The
   tps delta vs `vanilla + pgbouncer` (the realistic production
   alternative) is modest (~1.05–1.15×), but the resident-memory and
   scheduler-pressure deltas can be 3–5×.

This section captures the planned work to surface both regimes
honestly in the harness. The full ecosystem positioning that motivates
the comparison set lives in [comparison.md §1](comparison.md#1-pgbouncer).

### 3.1 Showcase 1 — connection churn (`pgbench -C`)

> **Status: shipped.** User-facing docs in §2.7; this subsection is
> kept as the historical planning record. Implementation:
> [`just/pgbench.just`](../../just/pgbench.just) `connect` + `script`
> parameters; probe script
> [`bench/scripts/select_one.sql`](../../bench/scripts/select_one.sql).

Extend [`just/pgbench.just`](../../just/pgbench.just) with two new
positional parameters:

| Position | Name      | Default | Meaning                                                                                          |
| -------- | --------- | ------- | ------------------------------------------------------------------------------------------------ |
| 7        | `connect` | `0`     | When `1`, appends `-C` to `pgbench` invocations (reconnect per transaction). Triggers the win.   |
| 8        | `script`  | `""`    | When non-empty, appends `-f <path>` (overrides built-in `-S`/`-N`/tpcb). For minimal probes.     |

Supporting deliverables:

- **`bench/scripts/select_one.sql`** — `SELECT 1;`. The cleanest probe
  for "what is the round-trip floor when execution cost is zero" —
  isolates the handoff win from any query work.
- **`bench/scripts/noop.sql`** — empty (or `;`). Optional; pure protocol
  round-trip floor.
- **`max_connections` auto-bump** in the generated `postgresql.conf`
  when `connect=1`: set to `max(100, clients * 4)`. Default 100 chokes
  under `-C` at `clients ≥ 32` because of `TIME_WAIT`-equivalent
  backend lifecycle states during the reconnect burst. Document the
  caveat in §3.4 below.

Sample invocations after the change:

```sh
just pgbench pg18 select 30 32 "" spi 1                      # -C with built-in -S
just pgbench pg18 select 30 32 "" spi 1 bench/scripts/select_one.sql
just pgbench pg18 select 30 32 "" spi 0                      # baseline (no -C)
```

The `connect=0` / `connect=1` pair on the same hardware in the same
harness run is the chart that tells the connection-churn story.

**Effort:** ~30 LOC justfile + 2 trivial `.sql` files + ~40 lines of
doc here in §3.4 once landed.

**Risks:**

- `pgbench -C` saturates the kernel's loopback ephemeral port range
  at very high `-c`. Keep `clients ≤ 64` for `-C` mode and document
  the ceiling.
- Vanilla's `max_connections` ceiling needs bumping (above).

### 3.2 Showcase 2 — high-concurrency OLTP (sysbench → pgbouncer comparison)

The honest comparison for steady-state OLTP is `vanilla + pgbouncer`
vs `pg_transport`, not bare vanilla. Phased so we can land the
sysbench harness alone first and decide whether the pgbouncer/HammerDB
work is justified by the early numbers.

#### Phase A — sysbench harness

[`sysbench`](https://github.com/akopytov/sysbench) is the cheapest
third-party OLTP driver to plumb (apt/brew install, clean CLI, no TCL
runtime). `oltp_read_write` is roughly TPC-C-shaped in transaction
mix; `oltp_point_select` mirrors `pgbench -S` with more diverse query
shapes.

New file **`just/sysbench.just`** modelled on `pgbench.just`:

```
just sysbench [pg=pg18] [workload=oltp_read_write] \
              [tables=16] [table_size=1000000] \
              [duration=60] [threads=64] [reconnect=0] \
              [pool=""] [backend=spi]
```

Same cluster-spin-up pattern (install extension, fresh `initdb`,
`shared_preload_libraries = 'pg_transport'`, dual-port). Initialises
via `sysbench --table-size=… prepare` against the vanilla port (same
shared-heap argument as `pgbench -i` in §2.1), runs the workload
against both ports side-by-side, `--cleanup` between runs.

**Effort:** ~150 LOC justfile + a new §4 here in this doc. 1–2 days
including bench-and-tune.

**Risks:** small. The one knob that matters is `--table-size`: too
small and the working set fits in buffer cache, making execution
near-free and over-stating our protocol win. Size to ~10× cluster
`shared_buffers` to ensure real disk traffic in the mix.

#### Phase B — pgbouncer comparison target

Additive on top of Phase A. Pgbouncer is the production deployment
we're competing with; including it in the harness is what turns the
numbers from "pg_transport vs vanilla" (interesting but unrealistic)
into "pg_transport vs the realistic alternative" (publishable).

New file **`just/_pgbouncer.just`** with helpers:

| Helper             | Purpose                                                                                |
| ------------------ | -------------------------------------------------------------------------------------- |
| `_pgbouncer-install` | Check `which pgbouncer`; print install hint if missing. Not auto-installed.          |
| `_pgbouncer-start` | Generate `pgbouncer.ini` + `userlist.txt` for given `pool_mode`, `max_client_conn`, `default_pool_size`; start daemon on `127.0.0.1:6432` pointing at vanilla `127.0.0.1:54329`. |
| `_pgbouncer-stop`  | Clean shutdown + remove generated configs.                                             |

Extend **`just/sysbench.just`** (and optionally `just/pgbench.just`)
with a `target` parameter:

| Value          | Listener                                                            |
| -------------- | ------------------------------------------------------------------- |
| `direct`       | `127.0.0.1:5454` — pg_transport                                     |
| `vanilla`      | `127.0.0.1:54329` — bare vanilla PG                                 |
| `bouncer-sess` | `127.0.0.1:6432` → vanilla, pgbouncer in **session** mode           |
| `bouncer-tx`   | `127.0.0.1:6432` → vanilla, pgbouncer in **transaction** mode       |

Run the same workload against all four for an honest 4-way comparison.
The expected ordering matches [comparison.md §1.2](comparison.md#12-pooling-mode-equivalence):

- `bare vanilla`: loses on connection establishment, OK on steady-state
- `pg_transport`: replaces pgbouncer-session-mode role; matches it on
  steady-state, beats it on connection establishment
- `pgbouncer-session`: ≈ pg_transport on steady-state
- `pgbouncer-transaction`: beats pg_transport for many-client/
  few-backend workloads (the [known gap](comparison.md#15-known-gap-transaction-pooling))

**Effort:** ~120 LOC justfile + pgbouncer config templates + ~100
lines of doc. 2–3 days.

**Risks:** pgbouncer install path varies by distro; CI may not have
it. Make the target opt-in with a clear "install pgbouncer" error.

#### Phase C — HammerDB TPC-C (deferred, decision-gated)

[HammerDB](https://www.hammerdb.com/) gives a formal TPC-C tpmC
number (publishable) at the cost of a TCL toolchain, manual download,
and ~300 LOC of harness to drive `hammerdbcli` headless.

**Decision gate:** run Phase A+B first. If sysbench
`oltp_read_write` shows the predicted "~1.05–1.15× vs
pgbouncer-session" steady-state delta, HammerDB is unlikely to
surprise. Skip it. If sysbench shows something unexpected (much
better or much worse than predicted), HammerDB becomes the
corroborating tool.

Effort estimate (only if pursued): ~300 LOC harness + per-run TCL
config templates + ~1 week bench-and-tune for clean numbers.

### 3.3 Sidecar — RSS / memory capture

For Showcase 2, the steady-state tps delta is small but the
**resident-memory** delta is the more compelling story (often 3–5×
less RSS than the equivalent vanilla + 200-backend setup).

A small sidecar — `ps -o rss,vsz --ppid <postmaster_pid> --no-headers`
snapshots at fixed intervals during the run — turns this from a
narrative claim into a measurable axis. Roughly ~30 LOC of shell
inside the existing recipes, output as a second CSV column alongside
tps. Worth landing alongside Phase A or B.

### 3.4 Suggested order and decision points

| Step              | Effort  | Decision after                                                                                       |
| ----------------- | ------- | ---------------------------------------------------------------------------------------------------- |
| Showcase 1        | ~1 day  | If `connect=1` shows the predicted 5–15× ratio, this becomes the headline benchmark.                |
| Phase A (sysbench)| ~2 days | If `oltp_read_write` numbers track the predicted "~1.05–1.15× vs vanilla" story, proceed to Phase B. |
| Sidecar (RSS)     | ~½ day  | Combine with Phase A; without it the "memory win" story is hand-waved.                              |
| Phase B (pgbouncer)| ~3 days | The point at which we have publishable 4-way numbers.                                              |
| Phase C (HammerDB)| ~1 week | Only if external audience asks for a formal TPC-C tpmC number.                                     |

### 3.5 Open policy questions (decide before Phase A)

- **Are sysbench/HammerDB required dev-machine prereqs, or always
  opt-in / gated behind a `which` check?** Recommendation: gate them.
  Keeps `just init` and CI lean; bench tooling is opt-in for the
  developer doing the bench.
- **CSV-out parity?** `just bench` has `csv=`; `just pgbench` does
  not. If we want cross-run regression detection (and we should, for
  a published benchmark page), align both. ~20 LOC.
- **A formal "is the new harness broken?" smoke test in CI** — run a
  single short `just sysbench pg18 oltp_point_select … threads=2 duration=5`
  on PR builds to catch harness regressions. No throughput assertion,
  just exit code. Avoids the harness rotting between publications.

---

## 4. Source-of-truth pointers

- Custom harness: [`crates/bench/src/main.rs`](../../crates/bench/src/main.rs)
- `just bench` recipe: [`just/bench.just`](../../just/bench.just)
- `just pgbench` recipe: [`just/pgbench.just`](../../just/pgbench.just)
- SPI bridge (the thing both harnesses ultimately stress on the
  pg_transport side): [`crates/core/src/backend/spi_bridge.rs`](../../crates/core/src/backend/spi_bridge.rs)
- Pool sizing GUC: [`crates/core/src/guc.rs`](../../crates/core/src/guc.rs)
- Capacity caveats (`max_worker_processes`): [`crates/core/src/lib.rs`](../../crates/core/src/lib.rs) `_PG_init`
