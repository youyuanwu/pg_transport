# Architecture

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [frontend-handoff.md](frontend-handoff.md)

## 1. Three layers

```
┌──────────────────────────────────────────────────────────────────────┐
│  Transport plugins (workspace crates, linked into core)              │
│    tcp_handoff                                                       │
│      (v0 ships exactly one; uds_handoff and friends deferred)        │
│                                                                      │
│    Each transport is a self-contained network entry point. It owns   │
│    its own accept loop and per-listener policy. The framework's      │
│    only requirement in v0:                                           │
│                                                                      │
│      impl HandoffTransport — receives a HandoffHandle                │
│                                                                      │
│    A second trait (SessionTransport) is anticipated and deferred;    │
│    see backend-pool.md.                                              │
│                                                                      │
│    Everything below the `run` boundary is the transport's business.  │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ HandoffHandle::handoff(fd)
┌────────────────────────────┴─────────────────────────────────────────┐
│  Frontend Core  (crate `core`, the pgrx extension)                   │
│    - tokio current-thread runtime + LocalSet                         │
│    - Transport registry (one map in v0; second map reserved for the  │
│      deferred SessionTransport)                                      │
│    - Lifecycle: spawn transports from catalog, signal handling,      │
│      postmaster-death watchdog                                       │
│    - HandoffHandle vending; metrics; GUCs                            │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ sendmsg(SCM_RIGHTS) over per-slot UDS
┌────────────────────────────┴─────────────────────────────────────────┐
│  Backend Pool  (in-tree crate `backend`)                             │
│    │ Slot runner (socket layer; backend-handoff.md)                  │
│    │   - pre-spawned bgworker per slot                               │
│    │   - recvmsg per-slot UDS → (fd, HandoffHints)                   │
│    │   - drive a Wire to completion; per-handoff reset               │
│    │ Wire layer (backend-wire.md; v0 = pgwire-v3 crate-based)        │
│    │   - TLS via rust-openssl; auth via hba_getauthmethod + our Rust │
│    │   - FE/BE v3 message loop; *not* PG's PostgresMain               │
│    │ SPI bridge                                                      │
│    └   - SPI_execute / SPI_execute_plan_with_params for actual SQL   │
│    (DSM + shm_mq + pq_redirect_to_shm_mq plumbing for the              │
│     deferred general path is captured in deferred/backend-pool.md      │
│     but not built in v0.)                                              │
└────────────────────────────────────────────────────────────────────┘
```

Three layers, sharply separated by interface size:

- **Transport plugins** — the thick layer. Each transport implements
  `HandoffTransport` and bundles its byte-pipe (TCP / UDS / future
  io_uring sockets) plus per-listener policy (bind address, IP
  allowlist, optional pre-handoff metadata). The framework imposes one
  method.
- **Frontend core** — the *thin* layer. tokio runtime, signal handling,
  postmaster-death watchdog, transport registry, `HandoffHandle`
  vending, metrics. Knows nothing about wire protocols.
- **Backend pool** — pre-spawned bgworkers, each running a slot runner
  (socket layer; see [backend-handoff.md](backend-handoff.md)) that
  receives handed-off fds and drives the wire layer (FE/BE v3 via the
  [`pgwire`](https://github.com/sunng87/pgwire) crate; TLS via
  rust-openssl; SQL execution through SPI — see
  [backend-wire.md](backend-wire.md)).

The framework deliberately does **not** define a `Connection` trait, an
`AsyncRead`/`AsyncWrite` boundary, or a `Protocol` abstraction. Once the
fd is handed off, the frontend has no further role for that connection.

### One path into the backend pool (v0)

The handle a transport receives in its `run` method has one method:

- **Handoff** — `HandoffHandle::handoff(fd)`. The transport holds an
  `OwnedFd` on which the wire is FE/BE v3; the frontend hands the
  socket to a bgworker via `SCM_RIGHTS`; the bgworker's slot runner
  drives the wire layer on it, which speaks FE/BE v3 via the `pgwire`
  crate (with our own TLS / auth / SPI bridge on top — *not* PG's
  `ProcessStartupPacket`/`ClientAuthentication`/`PostgresMain`). See
  [frontend-handoff.md](frontend-handoff.md) for the FE/IPC side,
  [backend-handoff.md](backend-handoff.md) for the slot runner,
  [backend-wire.md](backend-wire.md) for the wire layer.

### Deferred: the general (shm_mq) path

A second trait — `SessionTransport`, receiving a `SessionHandle` with
`execute(opts, payload)` and `acquire(opts).submit(payload)` — is
planned for non-FE/BE wires (HTTP/2 + SQL, custom binary) and for FE/BE
transports that need plaintext inspection. Its full design is in
[backend-pool.md](deferred/backend-pool.md); it is **not** built in v0. The
frontend's registry reserves a slot for its factory map so it can
land without a registry-shape change.

---

## 2. Why pg_transport owns the wire layer

The single largest design constraint behind everything in this
repository is that **PostgreSQL's wire dispatch is private**. The
four functions that handle the FE/BE protocol — `exec_simple_query`,
`exec_parse_message`, `exec_bind_message`, `exec_execute_message`
— are all `static` to [`src/backend/tcop/postgres.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c).
Not exported. Not linkable. The per-connection main loop
(`PostgresMain`) *is* exported but assumes it owns the process: one
client socket via the `MyProcPort` global, one `whereToSendOutput`
destination, one sigsetjmp-based ERROR recovery scope.

That singleton-`MyProcPort` assumption is correctness-load-bearing
in PG. Every wire-output path — `pq_putmessage`, `pq_sendint16`,
`pq_endmessage`, `printtup`'s `DestRemote` implementation — reads
`MyProcPort->sock` directly. Hundreds of call sites. Making it
pluggable would be a fork of PG, not an extension.

The practical consequence: **anything that wants to serve a wire
connection from inside a PG process, where that connection is *not*
the one postmaster handed to this backend at fork, has to build its
own wire dispatch.** That's pg_transport. The fact that this niche
is empty in the PG extension ecosystem is the same reason it's
empty in the PG codebase — the API for it was never published
because no consumer ever asked.

With that constraint accepted, the division of labour falls out:

| Layer | We delegate to PG | We build ourselves |
| --- | --- | --- |
| Network I/O | — | tokio + `tcp_handoff` transport |
| TLS termination | — | rustls ([roadmap Q26](roadmap.md#23-resolved)) |
| Wire framing (FE/BE v3) | — | [`pgwire`](https://crates.io/crates/pgwire) crate + our `PgTransportHandlers` |
| Startup / auth dispatch | `hba_getauthmethod` lookup | Method impls (trust / SCRAM / MD5 stub) — [roadmap Q1](roadmap.md#23-resolved) |
| BackendKey + cancel routing | — | v0 drops cancel; design in [deferred/cancel-routing.md](deferred/cancel-routing.md) |
| Parser / analyzer / planner / executor | All of it, via SPI | — |
| Plan caching | `SPI_keepplan` + `SPI_execute_plan` | — |
| Result materialisation | `SPI_tuptable` | — |
| Result encoding (text / binary, per-column) | Type I/O functions (`OidOutputFunctionCall` / `OidSendFunctionCall`) | Rust wrappers ([`spi.rs`](../../crates/core/src/backend/spi.rs)) + `DataRow` assembly |
| Per-query xact bracket | `StartTransactionCommand` / `CommitTransactionCommand` / `AbortCurrentTransaction` | `with_spi` closure pattern |
| Xact-control statements | `BeginTransactionBlock` / `EndTransactionBlock` / `UserAbortTransactionBlock` | `handle_xact_control` routing — see [backend-wire.md §6](backend-wire.md#6-spi-bridge) |
| Per-handoff state reset | Implicit (Drop of pgwire's `MemPortalStore` → drops our `SpiPlan`s → `SPI_freeplan`) | — |

The "build ourselves" column is concentrated in two zones:

1. **Wire / network / auth / TLS — the parts hardcoded around
   `MyProcPort`.** This is structural to the constraint; no
   alternative exists short of forking PG.
2. **Glue — per-call xact wrapper, result encoder, dispatch
   handler.** ~200 LOC each. Replaces per-connection-state
   assumptions PG makes that don't fit the pool-of-bgworkers model
   (one slot serving many sequential client connections via fd
   handoff).

Everything below that — every part of PG that's about "given a SQL
string, produce the right tuples with the right types" — we get
from SPI for free. The deferred
[planner-executor-direct-path.md](deferred/planner-executor-direct-path.md)
covers the only realistic alternative: replace SPI with direct
`Portal*` + custom `DestReceiver` calls. That gives us PG-parity
perf at the cost of ~250 LOC of new unsafe FFI, and is deferred
because bench is currently within noise (0.99x parity).

---

## 3. Runtime integration (tokio × pgrx × PG signals)

**Live source:** [crates/core/src/frontend.rs](../../crates/core/src/frontend.rs)
(`pg_transport_frontend_main` + `frontend_main`). The full constraint
rationale (current-thread runtime, `LocalSet`, no `#[async_trait]`,
signal coexistence with pgrx, postmaster-death watchdog) is in
[README.md §2 Constraints — C-1 … C-4](README.md#2-constraints-we-accept-up-front).

The shape, in five lines:

1. **`attach_signal_handlers(SIGHUP | SIGTERM)`** so pgrx flips PG's
   own bookkeeping flags (`ConfigReloadPending`, `ShutdownRequested`)
   in addition to whatever we do.
2. **`connect_worker_to_spi("postgres", None)`** — standard pgrx
   bgworker plumbing.
3. **Build the runtime once**:
   `tokio::runtime::Builder::new_current_thread().enable_all().build()`.
4. **`block_on(local.run_until(frontend_main()))`** where `frontend_main`
   is the supervisor loop — `tokio::select!` between `sigterm.recv()`,
   `sighup.recv()`, and a 500 ms postmaster-death watchdog tick.
5. **Graceful shutdown**: cancel a root `ShutdownToken`, join all
   spawned transport futures, then drop the pool.

Four integration points worth calling out:

1. **Signal handlers, two-layered.** pgrx's `attach_signal_handlers`
   keeps PG's own bookkeeping flags honest;
   `tokio::signal::unix::signal` gives us async wakeups. Both fire;
   the tokio side is what drives our control flow.
2. **Postmaster-death watchdog.** A 500 ms `interval` task polls
   `PostmasterIsAlive()`. On Linux the `PR_SET_PDEATHSIG = SIGTERM`
   that PG installs on bgworker startup means our SIGTERM branch
   usually fires first; the watchdog is the belt to that suspenders.
3. **`LocalSet` for per-connection tasks.** Transports `spawn_local`
   per-connection handlers on the same `LocalSet`. Those handlers may
   hold non-`Send` PG types, which a multi-thread runtime would
   forbid. The price is no work-stealing, which we don't want anyway
   given C-2.
4. **Supervisor never polls transports.** A `Transport::run` future is
   simply spawned and joined at shutdown. There is no per-iteration
   "next event from a listener" arm in the supervisor `select!` — that
   would re-introduce the very accept/dispatch abstraction the
   framework deliberately pushes inside transports.

---

## See also

- [api.md](api.md) — the `HandoffTransport` trait and `HandoffHandle`
  contract shown above (plus the deferred `SessionTransport`/`SessionHandle`
  sketch).
- [frontend-handoff.md](frontend-handoff.md) — what happens on the other side of
  `HandoffHandle::handoff`.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for the
  shm_mq general path.
