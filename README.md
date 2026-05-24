# pg_transport

> Status: **research framework** — not production software.
> Language: **Rust + pgrx 0.18** · Target: **PostgreSQL 18** · License: MIT.

`pg_transport` is a PostgreSQL extension that experiments with
**alternative in-process network entry points** for PG. It runs as a
single `shared_preload_libraries` cdylib that boots a tokio-based
frontend bgworker and an autoscaling pool of backend bgworkers. New
connections are accepted by the frontend, then handed off to a
pre-warmed slot bgworker via `SCM_RIGHTS` (kernel fd-passing over a
Unix socket) — skipping postmaster `fork()` and the per-backend
CatCache/RelCache cold start.

The explicit goal is to make swapping transports a small, controlled
experiment with comparable benchmark numbers, *not* to ship a
production server. That said, the warm-pool + fd-handoff design
already **outperforms vanilla PG 18 in connection-churn workloads
(up to ~15×) and in concurrent prepared-statement OLTP past physical-core
saturation (up to ~2.2×)**, while staying at parity for steady-state
single-statement workloads on a long-lived connection — see
[Benchmarks](#benchmarks) below for the full picture, including the
regimes where it currently regresses.

## Features

- Transport plugin: **`tcp_handoff`** (FE/BE v3 over TCP, optional
  TLS via rustls).
- Autoscaling backend pool of bgworkers (`max_backend_pool_size`
  GUC, demand-driven grow, idle reap).
- Two execution backends, selectable per-session via
  `pg_transport.execution_backend`:
  - `spi`     — routes SQL through `SPI_execute_plan_with_params`.
  - `direct`  — routes SQL through `CreateCachedPlan` + `Portal*`
    with a custom `DestReceiver` (no `SPI_tuptable` materialisation).
- Both simple-query (`'Q'`) and extended-query
  (Parse/Bind/Describe/Execute) paths.
- Auth via `pg_hba.conf` or extension-owned table
  (`pg_transport.auth_source` GUC).

See [docs/design/README.md](docs/design/README.md) for the full doc index.

## Process architecture

```
                       ┌───────────────────────────────────────────────┐
                       │  PostgreSQL postmaster   (unmodified PG 18)   │
                       │  spawns bgworkers from                        │
                       │  shared_preload_libraries                     │
                       └────────────────────────┬──────────────────────┘
                                                │  register + fork
                                                │  (frontend at startup,
                                                │   slots via load_dynamic)
                                                ▼
   ┌────────────┐    TCP    ┌─────────────────────────────────────────┐
   │   client   │──────────▶│  Frontend bgworker   (1 process)        │
   │   libpq /  │   :5454   │  • tokio current-thread + LocalSet      │
   │   psql /   │           │  • tcp_handoff accept loop              │
   │   driver   │           │  • backend pool dispatcher              │
   └──────┬─────┘           │  • HandoffHandle vending                │
          │                 └────────────────────┬────────────────────┘
          │                                      │  SCM_RIGHTS fd-pass
          │                                      │  over UDS frontend.sock
          │  FE/BE v3                            ▼
          │  (post-handoff) ┌─────────────────────────────────────────┐
          │                 │  Slot bgworker pool   (autoscaling,     │
          │                 │                        up to 64 slots)  │
          │                 │  ┌──────────────┐  ┌──────────────┐     │
          └────────────────▶│  │ slot[0]      │  │ slot[1]      │ ... │
                            │  │ pgwire-v3    │  │ pgwire-v3    │     │
                            │  │ TLS + auth   │  │ TLS + auth   │     │
                            │  │ SPI / direct │  │ SPI / direct │     │
                            │  └──────────────┘  └──────────────┘     │
                            └────────────────────┬────────────────────┘
                                                 │  SPI_execute_plan_with_params   (spi)
                                                 │  CreateCachedPlan + Portal*     (direct)
                                                 ▼
                                  PG shared catalogs, buffers, WAL, locks
```

Three process classes, one cdylib:

- **Postmaster** — unmodified PG 18. Boots the extension via
  `shared_preload_libraries`; spawns the frontend bgworker at
  startup and slot bgworkers on demand (`load_dynamic`).
- **Frontend bgworker** (1 process) — owns the tokio runtime, the
  TCP listener (`tcp_handoff` on `:5454`), and the pool dispatcher.
  Accepts client connections and ships their fds to ready slots over
  a single UDS (`SCM_RIGHTS`). Never touches the FE/BE wire bytes.
- **Slot bgworkers** (autoscaling up to `max_backend_pool_size`,
  default 64) — one tokio runtime per slot. Receives client fds,
  drives the FE/BE v3 wire (via the `pgwire` crate), terminates TLS,
  performs auth, then executes SQL through the **SPI bridge** or the
  **direct backend** (`Portal*` + custom `DestReceiver`) per the
  session's `pg_transport.execution_backend` setting. Slots are reset
  between handoffs and reaped when idle.

Source map: [`crates/core/`](crates/core) (cdylib),
[`crates/api/`](crates/api) (trait surface),
[`crates/bench/`](crates/bench) + [`crates/e2e/`](crates/e2e) (drivers),
[`docs/`](docs) (design + benchmark artefacts),
[`scripts/`](scripts) (sweep runners), [`just/`](just) (recipes).

## Quick start

Requires Rust 1.95+ and the `just` task runner.

```sh
# One-time: provision PG 18 under $PGRX_HOME (~5–10 min, ~1.5 GB)
just init

# Build + install into the pgrx PG 18 tree, run e2e suite
just e2e
```

Connect via either port emitted by the e2e harness — vanilla PG on
`127.0.0.1:54329`, pg_transport on `127.0.0.1:5454`.

## Benchmarks

Measured 2026-05-24 on an Azure 8-vCPU (4 physical core) VM at commit
[`61f7e16`](docs/bench/README.md). Full methodology, raw TSV, and
reproduction commands in [docs/bench/README.md](docs/bench/README.md).

| Regime                                       | Tool             | pg_transport vs vanilla PG 18 |
| -------------------------------------------- | ---------------- | ----------------------------- |
| Long-lived conn, `SELECT 1` round-trip       | custom bench     | **1.12 – 1.26×** (1 → 16 conns, direct) |
| Long-lived conn, single-statement OLTP       | pgbench `-S` / `-N` | **~parity** |
| Long-lived conn, multi-statement OLTP (TPC-B)| pgbench (default)   | **1.07 – 1.10×** |
| **Connection-churn (`pgbench -S -C`)**       | pgbench          | **10.9× / 14.4× / 14.7×** at 4 / 16 / 32 clients |
| Prepared-statement point select              | sysbench         | **1.20 – 1.31×** (4 → 32 threads) |
| Prepared-statement BEGIN/COMMIT read-only    | sysbench         | **1.50 – 2.18×** (12 statements per tx) |
| Prepared-statement indexed UPDATE            | sysbench         | **0.74 – 0.97×** (regresses past physical-core saturation) |

The connection-churn row is the architectural headline: vanilla pays
`fork()` + cache warm-up per connection (~1–10 ms); pg_transport
hands the fd to an already-warm slot bgworker (~0 ms). The
`oltp_read_only` row at threads=32 is the steady-state headline:
warm pool multiplexing under over-committed concurrency.

The write regression at threads ≥ 16 is host-shaped (4 physical cores
-- tuple-lock serialization) and is reported as-is rather than tuned
around; see [docs/bench/README.md §6](docs/bench/README.md#6-sysbench--prepared-statement-oltp).

## Further reading

- [docs/design/README.md](docs/design/README.md) — full design doc index.
- [docs/design/architecture.md](docs/design/architecture.md) — 3-layer
  architecture (transport / frontend core / backend pool).
- [docs/design/performance.md](docs/design/performance.md) —
  per-query data-path trace and the structural fast paths.
- [docs/design/comparison.md](docs/design/comparison.md) — how
  pg_transport relates to pgbouncer, Odyssey, pg_background, Omnigres.
- [docs/design/roadmap.md](docs/design/roadmap.md) — phased build
  plan, open questions, risks.

## License

MIT — see [LICENSE](LICENSE).
