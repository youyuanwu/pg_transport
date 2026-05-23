# `pg_transport` — Design

> Status: **draft v0** — design intent and constraints, not yet implemented.
> Language: **Rust + pgrx**.
> License (proposed): PostgreSQL License (matches PG ecosystem norms).

`pg_transport` is a **research framework** for experimenting with alternative
in-process network entry points for PostgreSQL. It is *not* a production
server; the explicit goal is to make swapping transports a small, controlled
experiment with comparable benchmark numbers.

**Current scope**: TCP, Unix domain sockets, TLS (over TCP/UDS), the FE/BE v3
wire protocol, and a demand-driven autoscaling backend pool of bgworkers that
receive handed-off kernel sockets via `SCM_RIGHTS`. **Only the handoff
path is in scope.**

**Deferred** (preserved as design intent, not built in v0):

- **`SessionTransport` + `SessionHandle` (the shm_mq general path)** — for
  non-FE/BE wires (HTTP/2 + SQL, custom binary) and FE/BE transports that
  need plaintext inspection. Design captured in
  [api.md](api.md) and [backend-pool.md](deferred/backend-pool.md); revisited
  once the handoff baseline produces benchmark numbers.
- **Exotic transports** — QUIC, io_uring, AF_XDP, DPDK, RDMA,
  shared-memory loopback. Live in [../future-transports.md](deferred/future-transports.md).

**Design stance.** The framework standardises *only* what touches PostgreSQL:
the handle type vended to transports, the bgworker lifecycle, the shutdown
protocol, the catalog binding. **Transports are free.** Each transport is a
self-contained network entry point that owns its own I/O model, protocol
parsing, connection state, TLS, and auth. The framework defines one
transport trait in v0 (`HandoffTransport`) and the handle type it
receives (`HandoffHandle`); everything else is the transport's business.
A second trait (`SessionTransport`) is anticipated but **deferred**; the
current trait is named `HandoffTransport` (not just `Transport`) to
leave room for it.

**Single path into the backend pool (v0).** Transports hold a kernel
`OwnedFd`, speak FE/BE v3, implement `HandoffTransport`, and receive a
`HandoffHandle` whose one method `handoff(fd).await` ships the socket
to a bgworker over `SCM_RIGHTS` after the dispatcher has found (or
grown) a ready slot. Inside the bgworker, a slot runner drives
a wire layer that handles FE/BE v3 itself — see
[architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer)
for why we cannot reuse PG's `PostgresMain` / `ProcessStartupPacket` /
`ClientAuthentication` / `secure_open_server`, and
[pool.md](pool.md) /
[frontend-handoff.md](frontend-handoff.md) /
[backend-handoff.md](backend-handoff.md) /
[backend-wire.md](backend-wire.md) for the pool / FE / BE-socket /
BE-wire implementations.

---

## Doc index

| File | Status | What's in it |
|---|---|---|
| [architecture.md](architecture.md) | active | 3-layer architecture diagram + runtime integration (tokio × pgrx × signals) |
| [api.md](api.md) | active | The framework's trait surface: `HandoffTransport`, `HandoffHandle`, `ShutdownToken`, plus an end-to-end example transport. Deferred surface (`SessionTransport`, `SessionHandle`) sketched at end. |
| [pool.md](pool.md) | active | Pool-level design: single FE UDS listener, three slot containers (`ready` / `in_flight` / `draining`), four-opcode wire protocol (`b'\0'`/`b'X'`/`b'R'`/`b'A'`), demand-driven autoscaling under one ceiling GUC, cooperative drain with ack-timeout fallback. |
| [frontend-handoff.md](frontend-handoff.md) | active | The v0 path — **FE/IPC side**: when to use, listener setup/teardown, end-to-end per-connection flow, comparison with default PG, performance. The dispatch model mirrors default PG (`SCM_RIGHTS` replaces `fork`); what runs on the fd is our wire layer. |
| [backend-handoff.md](backend-handoff.md) | active | The v0 path — **BE socket layer**: slot runner, fd receipt, per-handoff lifecycle, per-handoff reset. |
| [backend-wire.md](backend-wire.md) | active | The v0 path — **BE wire layer**: Wire trait + `pgwire-v3` implementation via the `pgwire` (sunng87) crate; TLS via rust-openssl; auth via `hba_getauthmethod` + Rust-side method impls; SPI bridge; open Qs. |
| [backend-pool.md](deferred/backend-pool.md) | **deferred** | Design draft for the shm_mq general path (`SessionTransport` + `SessionHandle`: `execute` / `acquire` / `submit`). Not implemented in v0. |
| [deferred/cancel-routing.md](deferred/cancel-routing.md) | **deferred** | Cancel-routing options + recommended design (frontend-owned registry over the FE UDS protocol; `kill(SIGINT)` for the interrupt). v0 drops `CancelRequest` fds silently. |
| [deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md) | active | Strategic SPI-vs-direct trade-off + the survey of what's callable from `postgres.c` (none of the `exec_*` dispatchers — all `static`; the parser / analyzer / planner / Portal / CachedPlan / DestReceiver layer below them is). Both backends shipped: `pg_transport.execution_backend = 'spi' \| 'direct'` selects per session for both simple- and extended-query. SPI bridge stays maintained side-by-side. |
| [deferred/simple-query-direct-path.md](deferred/simple-query-direct-path.md) | active | Concrete simple-query (`'Q'`) direct backend: routes through `Portal*` + `WireDestReceiver` instead of SPI, eliminating the second parse + the `SPI_tuptable` materialisation. Module layout, memory-context discipline (parse_ctx / stmt_ctx / portal_ctx + snapshot interaction), and editing gotchas (FETCH_ALL == LONG_MAX, tupDesc populated by PortalStart, not PortalDefineQuery). |
| [transports.md](transports.md) | active | Per-transport notes for the current-scope set + helper crates the framework ships |
| [workspace.md](workspace.md) | active | Cargo workspace layout, build commands (v0 has no Cargo features) |
| [configuration.md](configuration.md) | active | Catalog tables, GUCs, SQL surface, observability/metrics |
| [comparison.md](comparison.md) | active | How `pg_transport` relates to pgbouncer, Odyssey, `pg_background`, Omnigres, and default PG |
| [roadmap.md](roadmap.md) | active | Phased build plan, open questions, risks, references |
| [testing.md](testing.md) | active | Correctness testing strategy: subsystem risk register, test seams the design must provide, harness shape, error injection, per-phase acceptance criteria |
| [bench.md](bench.md) | active | Performance bench harness — `just bench` (custom tokio-postgres harness) and `just pgbench` (TPC-B-like via PG-shipped pgbench); per-recipe args, output, what the numbers can and cannot tell you, known limitations |
| [performance.md](performance.md) | active | Data-path performance audit — traces socket-read → socket-write, ranks the 5 known inefficiencies by leverage (custom DestReceiver, simple-query `BytesMut` direct path, per-`'Q'` xact bracket, pgwire async dispatch, per-cell typoutput cache), with µs estimates and re-entry triggers |

Companion docs (outside this design dir):

- [../future-transports.md](deferred/future-transports.md) — deferred transport experiments
- [../background/pg_background.md](../background/pg_background.md) — the DSM + `shm_mq` + `pq_redirect_to_shm_mq` mechanics we lift
- [../background/omnigres.md](../background/omnigres.md) — the listener-bgworker + worker-pool pattern we mirror

---

## 1. Goals & non-goals

### Goals

1. **Frontend bgworker** that hosts a tokio current-thread runtime, owns
   the backend pool, and supervises a configurable set of transports.
2. **Pluggable transports** as Rust crates in the workspace, statically
   linked into the `core` extension at build time and gated by Cargo
   features. Each crate implements `HandoffTransport` (v0) and is
   otherwise unconstrained.
3. **Stable, narrow PG-interaction contract.** One handle type is the
   **only** surface a transport uses to talk to PostgreSQL in v0:
   `HandoffHandle` (one method, `handoff(fd)`). A second handle
   (`SessionHandle`) is planned for the deferred general path; it will
   be the *only* place that surface widens.
4. **Autoscaling backend pool** — a dynamically-sized fleet of slot
   bgworkers, each running a slot runner
   ([backend-handoff.md](backend-handoff.md)) that takes ownership
   of handed-off kernel sockets and drives a wire layer
   ([backend-wire.md](backend-wire.md)) on the fd. The pool grows
   on demand and shrinks on idle under one operator-visible
   ceiling GUC ([pool.md](pool.md)). SQL execution goes through
   `SPI_*`. The `pg_background`-style DSM + `shm_mq` +
   `pq_redirect_to_shm_mq` machinery is **deferred** along with
   `SessionTransport`.
5. **Benchmark harness** built into the workspace from day one: per-op
   latency histogram, connection-setup cost, throughput, CPU per request.
   Comparable numbers across transport choices are the whole point.
6. **Configuration in SQL** (catalog tables + GUCs), following Omnigres's
   pattern. Reload without restart for transport instances
   (transport *availability* is fixed by the compiled feature set).

### Non-goals (initially)

- Replacing the postmaster's 5432 listener (architecturally impossible from
  inside an extension; see [../background/pg_background.md](../background/pg_background.md) §3
  and [../background/omnigres.md](../background/omnigres.md) §4.5).
- Connection pooling that masquerades as a transparent libpq backend
  (pgbouncer/Odyssey do this well; out of scope).
- Production hardening — concurrency safety, yes; production SLOs, no.
- Cross-version PG support beyond what pgrx already gives (PG 14 – 18).

### Out-of-scope but worth tracking

- Multi-host transports (RDMA-cluster, multi-NIC DPDK fleets).
- Hot reload of plugin `.so` binaries while connections are live (Omnigres
  has working art here via `omni`; we punt to v0.2+).

---

## 2. Constraints we accept up front

These are not negotiable inside a PG extension and shape everything below.

### C-1 — We coexist with PG's latch infrastructure, but tokio drives the loop

`src/backend/storage/ipc/latch.c` (`WaitEventSet`) is how PG bgworkers
classically wait on `MyLatch`, postmaster death, signals, and fds. We do
**not** use it as the central event loop. Instead the frontend runs a
**tokio current-thread runtime** as its top-level scheduler:

- All fd-driven transports (TCP, UDS, TLS, QUIC's UDP, io_uring's ring fd,
  RDMA event channel) register through tokio's reactor — either directly
  (`tokio::net::TcpListener` etc.) or via `tokio::io::unix::AsyncFd` for
  arbitrary `RawFd`s.
- Polling-only transports (DPDK PMDs, busy-poll AF_XDP) keep the
  data-plane-thread + eventfd bridge documented in
  [../future-transports.md §3](deferred/future-transports.md); the eventfd is
  registered with tokio via `AsyncFd`, not with `WaitEventSet`.
- PG signals (`SIGHUP`, `SIGTERM`) are observed via
  `tokio::signal::unix::signal(...)`. We still install pgrx's
  `attach_signal_handlers()` so that PG-conformant bookkeeping
  (e.g. `ConfigReloadPending` for `SIGHUP`) continues to work; a tokio
  task acts on the flags it sets.
- Postmaster-death detection: a small interval task polls
  `PostmasterIsAlive()`; on Linux we additionally rely on the
  `PR_SET_PDEATHSIG` that PG already installs for bgworkers, surfaced as
  `SIGTERM` to tokio.

The trade-off: we lose `WaitEventSet`'s single-call unified wait, but we
gain real Rust `async` ergonomics (`tokio::select!`, `AsyncRead`/
`AsyncWrite`, `tokio::time::timeout`, per-connection state machines as
`async fn`s, the entire tokio ecosystem of TLS / HTTP / QUIC crates).

### C-2 — Single-threaded PG state, period

`MemoryContext`, `ereport`/`elog`, LWLocks, SPI all assume single-threaded
execution. Threads inside a bgworker are *technically* allowed but **must
never touch PG state**. The discipline: data-plane threads write to lock-free
ring buffers; the main thread (which alone calls into PG) is the **only**
place tokio tasks ever touch a PG symbol. Because we use a current-thread
runtime, every `tokio::spawn` runs on that same thread by construction.
For `!Send` per-connection state we use `LocalSet` + `spawn_local`.

### C-3 — pgrx bgworker is sync; we run tokio current-thread on top

`pgrx::bgworker::BackgroundWorker` exposes a sync entrypoint. Inside it we
build a `tokio::runtime::Builder::new_current_thread().enable_all().build()`
and `block_on` the frontend's top-level `async fn`. Choice notes:

- **Current-thread, not multi-thread.** No PG-touching code is ever spawned
  onto a worker thread. The runtime owns exactly the bgworker's main
  thread.
- **`LocalSet` for `!Send` futures.** Per-connection state may hold
  non-`Send` types (e.g. `Rc`, pgrx handles); `LocalSet::spawn_local`
  permits this safely on a current-thread runtime.
- **`enable_all`** turns on tokio's I/O and time drivers.
- **No `#[async_trait]`** — the `HandoffTransport` trait returns a
  manual `Pin<Box<dyn Future + 'static>>` (aliased as `RunFuture`).
  Reasons in [api.md §1](api.md#1-the-handofftransport-trait): one
  async method called once per transport lifetime; same runtime shape
  as the macro desugar; no proc-macro dep in `crates/api/`; cleaner
  errors; trivial migration to native `async fn in traits` once
  `dyn`-safe.
- **No `tokio::task::block_in_place`** — it requires a multi-thread
  runtime. For blocking work (e.g. synchronous SPI from a debug helper)
  we accept that the runtime stalls; if that becomes a problem we move
  the work to the backend pool instead.

### C-4 — Postmaster owns 5432

We add a listener; we never replace one. The postmaster's listener keeps
running. Our listener is bound to a different address (and almost always a
different port / socket family).

---

## Where to go next

- New here? Read [architecture.md](architecture.md) next, then
  [api.md](api.md), then [pool.md](pool.md) (pool topology),
  then [frontend-handoff.md](frontend-handoff.md) (FE/IPC) +
  [backend-handoff.md](backend-handoff.md) (BE).
- Looking up a specific decision? Use the index above.
- Planning to contribute? Read [roadmap.md](roadmap.md) and
  [workspace.md](workspace.md).
- Curious about what was deferred? [backend-pool.md](deferred/backend-pool.md)
  is the captured design for the shm_mq general path; [roadmap.md §4 —
  Deferred for v0](roadmap.md) records why.
