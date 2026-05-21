# Backend slot runner — the socket layer

> Parent: [README.md](README.md)
> Sibling: [pool.md](pool.md) · [frontend-handoff.md](frontend-handoff.md) · [backend-wire.md](backend-wire.md) · [api.md](api.md)

This doc describes the **socket layer** of the BE side of `pg_transport`'s
v0 fd-pass path. The slot runner owns the fd, the per-handoff
lifecycle, and the per-handoff reset. It does **no** wire-protocol work
— that's the [backend-wire.md](backend-wire.md) layer. The pool-level
design (autoscaling, single UDS listener, cooperative drain) lives in
[pool.md](pool.md); this doc is the per-slot view.

Companions:

| Layer (BE side)              | Doc                                  |
| ---------------------------- | ------------------------------------ |
| **Slot runner — socket layer (this doc)** | **backend-handoff.md**  |
| Wire — TLS, auth, FE/BE v3 protocol | [backend-wire.md](backend-wire.md) |
| Execution — SQL via SPI       | (implicit; see backend-wire.md §6)   |
| **Pool** — slot lifecycle, dispatch, autoscaling | [pool.md](pool.md) |

The FE/IPC companion ([frontend-handoff.md](frontend-handoff.md))
covers when this path applies and the transport-facing per-connection
sequence; [pool.md](pool.md) covers single-listener topology, slot
spawn/drain, and the four-opcode wire protocol on the UDS. The slot
runner picks up at the moment its main loop receives one
`CtrlMsg::FdHandoff(fd)` over the UDS; it ends when the wire layer's
`run` returns and the slot runner performs the per-handoff reset (or
when a `CtrlMsg::DrainRequest` arrives and the slot exits cleanly).

The slot runner never speaks the IPC protocol bytes directly; it just
receives `CtrlMsg`s from `fd_pass::recv_ctrl_async` and forwards the
contained fd to the wire. The deferred shm_mq general path's
slot-runner equivalent lives in
[backend-pool.md](deferred/backend-pool.md).

---

## 1. Bgworker registration

The frontend bgworker is registered **statically** in `_PG_init()`;
slot bgworkers are **dynamic** and spawned on demand by the FE's
pool dispatcher. The full design lives in [pool.md](pool.md); this
section covers the BE side of the contract.

### Requirement: `pg_transport` must be in `shared_preload_libraries`

The operator's `postgresql.conf` must contain:

```
shared_preload_libraries = 'pg_transport'
```

This causes `pg_transport.so` to be loaded into the postmaster and
the extension's `_PG_init()` to run inside the postmaster, very early
in `PostmasterMain()` — the only context where PG accepts static
`RegisterBackgroundWorker` calls.

If SPL is missing, `_PG_init()` never runs in the postmaster; the FE
bgworker is never registered; nothing comes up. `_PG_init()` raises a
clear `FATAL` ("pg_transport must be in shared_preload_libraries").
We do not have a lazy-spawn fallback in v0. (Rationale and the
rejected lazy path are in
[roadmap.md §2 Q4](roadmap.md#2-open-questions).)

### What `_PG_init()` does

**Live source:** [crates/core/src/lib.rs](../../crates/core/src/lib.rs).
The sequence:

1. Refuse to proceed if `process_shared_preload_libraries_in_progress`
   is false — FATAL with `ERRCODE_CONFIG_FILE_ERROR` and a clear
   `"set shared_preload_libraries = 'pg_transport'"` hint.
2. `guc::register()` + `guc::validate_required()` — register the
   GUCs (`max_backend_pool_size`, `auth_source`, `tls_cert_file`,
   `tls_key_file`, `execution_backend`) and FATAL immediately if
   required ones are unset or invalid. See
   [crates/core/src/guc.rs](../../crates/core/src/guc.rs).
3. Install the rustls `ring` crypto provider process-wide (phase 8
   requirement; idempotent).
4. Register the **frontend bgworker only** (Q22 → option (a) in
   [roadmap.md §2.3](roadmap.md#23-resolved)) with
   `bgw_restart_time = 1s`.

That's it. No slot bgworkers are statically registered. The FE's
pool dispatcher creates them via `BackgroundWorkerBuilder::load_dynamic()`
on demand (see [pool.md §5.2](pool.md#52-grow-on-demand)).

Once `PostmasterMain` finishes recovery, the postmaster spawns only
the single FE bgworker (`pg_transport_frontend_main`, see
[crates/core/src/frontend.rs](../../crates/core/src/frontend.rs)).
The FE binds the single UDS listener at
`paths::frontend_socket_path()` and spawns its `accept_loop` +
`idle_reaper` tasks; slots arrive lazily as traffic does.

**Capacity note.** pgrx's `.load_dynamic()` wraps
`RegisterDynamicBackgroundWorker`, which returns `Err` when the
postmaster's bgworker table is full (bounded by
`max_worker_processes`, PG default = 8). Footprint is 1 FE + up to
`max_backend_pool_size` slots + PG's own bgworkers. For a meaningful
pool the operator must bump `max_worker_processes`; the
`just/{e2e,bench}.just` recipes do this automatically. A failed
`load_dynamic` is surfaced to the dispatcher as a WARNING
(`"bgworker table full (raise max_worker_processes)"`) and the
handoff falls back to waiting on existing slots until `HANDOFF_WAIT`.

### Why dynamic (not static `.load()`)

The earlier static-registration model is documented in
[pool.md §0](pool.md#0-problem-this-design-solves) as the design it
replaces. Briefly:

- Static sizing required a cluster restart to change `backend_pool_size`.
- An idle cluster paid the resident-RAM cost for N slot bgworkers
  even if no one was connected.
- Round-robin dispatch over a static N had no way to grow into
  bursty load.

Dynamic per-demand spawn lets the pool quiesce to zero, grow up to
the operator's ceiling under load, and shrink back via the idle
reaper without an operator in the loop. The single-FE-bgworker
operational story ("the framework is up iff the postmaster is up")
is preserved: the FE is static; only the slots are dynamic.

### What `_PG_init()` does *not* do

- It does not start the tokio runtime in the postmaster. Runtimes
  are per-bgworker, created inside each bgworker's `*_main`
  entry point (see [§3 Async / threading model](#3-async--threading-model)
  for slots; [architecture.md §3](architecture.md#3-runtime-integration-tokio--pgrx--pg-signals)
  for the frontend).
- It does not bind any sockets or read the catalog. The frontend
  bgworker binds its listener at boot (currently hard-coded; the
  planned `pg_transport.start()` / catalog surface in
  [configuration.md §1.3](configuration.md#13-planned-catalog-surface)
  will move this under SQL control).

---

## 2. The slot runner loop

**Live source:** [crates/core/src/backend/slot.rs](../../crates/core/src/backend/slot.rs).
The entry point (`pg_transport_slot_main`) is a C-ABI function
registered by `pool::grow_one`'s `BackgroundWorkerBuilder` call;
pgrx wraps it with `#[pg_guard]` so any uncaught panic surfaces as a
PG ereport at the boundary. The slot's lifetime:

1. `attach_signal_handlers(SIGHUP | SIGTERM)` and
   `connect_worker_to_spi("postgres", None)` — standard pgrx
   bgworker setup.
2. `connect_with_retry()` to the FE's single UDS listener at
   `paths::frontend_socket_path()` (10 s budget over 100 × 100 ms
   retries to tolerate the FE ↔ slot startup race).
3. `stream.set_nonblocking(true)` so the stream is `AsyncFd`-compatible
   for the per-step `block_on` windows below.
4. Build the per-bgworker tokio current-thread runtime *once* and
   keep it across handoffs (Q9-era decision; not rebuilt per
   handoff). Build the `TlsAcceptor` once from the TLS GUCs.
5. `rt.block_on(fd_pass::wrap_for_async(stream))` — register the
   stream with the reactor as an `AsyncFd<UnixStream>`. The
   resulting handle is held across subsequent `block_on` windows.
6. `rt.block_on(send_ready_byte_async(...))` — announce initial
   readiness to the FE (`b'R'`).
7. Loop:
   - `BackgroundWorker::sigterm_received()` quick-check.
   - `rt.block_on(timeout(100ms, recv_ctrl_async(...)))` — wait up
     to 100 ms for the next control message. The timeout makes
     SIGTERM polling responsive without a dedicated tokio task.
   - On `Ok(CtrlMsg::FdHandoff(fd))`: build a `WireCtx`, call
     `PgwireV3::run(fd, ctx)` (sync, with its *own* internal
     `block_on`), then `rt.block_on(send_ready_byte_async(...))` to
     announce readiness for the next handoff.
   - On `Ok(CtrlMsg::DrainRequest)`: call `cleanup_per_slot_state()`,
     `rt.block_on(send_drain_ack_async(...))`, exit cleanly.
   - On `Ok(CtrlMsg::Eof)`: FE closed UDS; exit cleanly.
   - On `Err(timeout)`: re-check SIGTERM and loop.
   - On `Err(other)`: log + exit; postmaster does not auto-respawn
     (slots are dynamic with `BGW_NEVER_RESTART`).

The slot runner never:

- Reads or writes the fd directly (it doesn't even know what wire
  format the bytes are in).
- Calls PG's `ProcessStartupPacket`, `ClientAuthentication`,
  `secure_open_server`, or `PostgresMain` (see
  [backend-wire.md](backend-wire.md) for the rationale).
- Opens or closes TLS sessions.
- Looks at FE/BE message types.

Everything in that list is the wire layer's job. The slot runner is
deliberately small (~250 LOC including comments): a `recv_ctrl`
loop, a wire run, a `send_ready_byte` between iterations, drain
handling, and — once phase ≥ 9.5 lands the per-handoff reset — a
reset step.

The four-opcode UDS protocol (BE → FE: `b'R'` / `b'A'`; FE → BE:
`b'\0' + SCM_RIGHTS(fd)` / `b'X'`) and its race-freedom analysis
live in [pool.md §1–§2](pool.md).

> **Silent handoff loss — resolved as accept (option b).** If the
> backend dies between the frontend's `send_fd` and the slot's
> `recv_ctrl` above, the kernel buffers the SCM_RIGHTS payload but
> nobody consumes it — the current client connection is orphaned
> (TCP RST). The dead slot is observed by the FE's per-slot reader
> task on EOF and removed from the pool. No per-handoff ack; zero
> added cost on the happy path. Full reasoning in
> [roadmap.md §2 Q21](roadmap.md#2-open-questions).

---

## 3. Async / threading model

The slot runner runs on the bgworker's **main thread — the only
thread**. That thread hosts a small **single-threaded tokio
runtime** that the wire layer uses *and* that the slot runner uses
for short async I/O windows; the outer loop body is sync.

```
backend bgworker process (single thread)
├─ slot runner loop                                    (sync)
│    rt.block_on(wrap_for_async(stream))               ← reactor registration
│    rt.block_on(send_ready_byte_async(uds))           ← initial b'R'
│    loop {
│      if sigterm_received() { exit }
│      msg = rt.block_on(timeout(100ms, recv_ctrl_async(uds)))   // sync await
│      match msg {
│        Ok(FdHandoff(fd))  => {
│          PgwireV3::run(fd, ctx)                      // SYNC; uses ctx.rt.block_on internally
│          rt.block_on(send_ready_byte_async(uds))     // next b'R'
│        }
│        Ok(DrainRequest)   => {
│          cleanup_per_slot_state()
│          rt.block_on(send_drain_ack_async(uds))      // b'A'
│          exit
│        }
│        Ok(Eof) | Err(_)   => exit
│        Err(timeout)       => continue                // re-check SIGTERM
│      }
│    }
│
└─ runtime: tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()                              // built once at bgworker boot
                                                   // reused across all block_on windows
```

### Why the outer loop is sync (sequential `block_on`, not nested)

The wire's `PgwireV3::run` is sync at the call site and uses
`ctx.rt.block_on(...)` internally to drive its own async machinery
(pgwire's `process_socket`, TLS handshake, etc.). Wrapping the
slot's outer loop in `rt.block_on(async { loop { ... wire::run ... } })`
would make the wire's internal `block_on` a **nested** `block_on` on
the same runtime, which tokio rejects with
`"Cannot start a runtime from within a runtime"` and the slot
process exits with code 1. (We found this during integration; see
the slot.rs module docstring for the post-mortem.)

The fix is to keep the outer loop sync and use short-lived
`rt.block_on` windows only around the async UDS helpers
(`recv_ctrl_async`, `send_ready_byte_async`, `send_drain_ack_async`).
Between those windows the wire's internal `block_on` is a sequential
entry on the same runtime — not nested.

SIGTERM cancellation comes from `tokio::time::timeout(100ms, ...)`
around `recv_ctrl_async`: every 100 ms the recv future times out, we
re-check `BackgroundWorker::sigterm_received()`, and loop. Bounded
shutdown latency, no `CancellationToken` plumbing, no separate
signal-poller task.

### Why tokio appears in the backend at all

Forced by the choice in [backend-wire.md §2](backend-wire.md) to
reuse the [`pgwire`](https://github.com/sunng87/pgwire) crate and
[`tokio_rustls`](https://crates.io/crates/tokio-rustls) for TLS
(Q26). Both expose `tokio::io::AsyncRead` / `AsyncWrite`-shaped APIs
and `async fn` trait methods; running them needs an async executor.
The UDS-side async helpers piggyback on the same runtime — building
them on the synchronous `recv_fd` we had before the autoscaling pool
(`fd_pass::recv_ctrl_nonblocking` is still there for that variant)
would forgo `tokio::time::timeout` for SIGTERM responsiveness.

### What this runtime is *not*

- **Not a concurrency primitive.** Exactly one task runs on it at a
  time (the current handoff's `W::run` future, or the brief UDS-IO
  window). One client per slot at a time, by construction.
- **Not multi-threaded.** That would violate C-2 (PG state is
  single-threaded). Current-thread only.
- **Not a `LocalSet`-bearer.** No `spawn_local`; the wire's `run`
  future is the whole workload. (An early prototype put a
  `spawn_local`'d SIGTERM poller here; it panicked because the
  bare current-thread runtime is not entered through a `LocalSet`.
  The `tokio::time::timeout` pattern above is what works.) The
  frontend uses `LocalSet` because it multiplexes per-transport,
  per-reader, per-watchdog tasks; the backend has nothing to
  multiplex.
- **Not rebuilt per handoff.** One `Runtime` per bgworker, reused
  for every `block_on(...)` (slot loop UDS-IO *and* wire run).
  Building tokio runtimes costs hundreds of microseconds;
  per-handoff construction would dominate connection setup.

It's effectively a **trampoline / executor for the wire's async
future and the slot's UDS helpers**, not a scheduler.

### SPI blocking the runtime is fine

When the wire layer calls `SPI_execute` (or any other `SPI_*`), the
running task blocks the executor synchronously. There's no other task
to schedule and no other client to serve on this slot, so nothing
starves. The runtime's I/O reactor is also idle during SPI execution
because the only fd the wire was watching is the client fd, and the
client won't see any more bytes until SPI returns and the wire emits
row data anyway.

### C-2 compliance

Satisfied by construction:

- The runtime runs only on the bgworker's main thread.
- All SPI calls happen synchronously from within the single task, on
  that same thread.
- No work-stealing, no helper threads (no `block_in_place`, which
  would require a multi-thread runtime anyway).

### Cost

- ~1 MB of resident state per bgworker (timer wheel + I/O reactor
  allocations). With autoscaling and a quiescable pool
  (`MIN_WARM_SLOTS = 0`), an idle cluster pays zero — no resident
  bgworkers at all.
- Per-handoff cost: one `rt.block_on(recv_ctrl_async)` round-trip
  (microseconds when a message is already queued) + the wire's own
  `block_on` entry + one `rt.block_on(send_ready_byte_async)` after.
  All microseconds; dominated by the actual wire work.
- Per pgwire call: whatever `Future::poll` adds over a sync call —
  negligible next to `read`/`write` syscalls or SPI execution.

---

## 4. `HandoffHints`

```rust
#[repr(C)]
pub struct HandoffHints {
    /// Whether the listener allowed TLS. The wire layer honours this
    /// when deciding to reply 'S' to SSLRequest. Other TLS config
    /// (cert path, ciphers, min version) is GUC-driven; see
    /// backend-wire.md §5.
    pub tls_allowed: bool,
}
```

We deliberately keep this tiny in v0. Per-listener cert variation
(adding a `cert_id` field) is a likely v0.x extension; see
[backend-wire.md §5](backend-wire.md).

---

## 5. Per-handoff state reset

Between handoffs on the same slot, we must purge anything that could
leak from session to session. The reset is split by who owns the
state:

| Owned by  | State                              | How we reset                                             |
| --------- | ---------------------------------- | -------------------------------------------------------- |
| Wire      | Prepared-statement / portal maps   | Wire drops its `HashMap`s on return; no slot-runner work |
| Wire      | TLS session, auth state            | Wire owns; gone when the wire struct is dropped          |
| SPI       | Prepared plans (per-name `SPIPlan`) | `SPI_freeplan` on each name the wire registered (the wire keeps the list) |
| PG        | GUCs touched by `SET LOCAL` / `SET` | `ResetAllOptions()`                                      |
| PG        | Temp tables                        | drop session's temp namespace                            |
| PG        | Portals                            | `PortalDrop` over the slot's portals                     |
| PG        | Per-session memory contexts        | `MemoryContextDelete(MessageContext)`; reinit            |
| Pool slot | Bgworker PGPROC slot, `MyDatabaseId` | unchanged (intentionally — that's the *point* of the pool) |

The shrunken responsibility vs. default PG: we are not freeing
`MyProcPort.peer_dn` or tearing down a libpq-style port, because we
never built one. The wire layer's own constructs are its own
`Drop` impls.

---

## 6. Slot lifecycle

The slot runner doesn't make any policy choices about the slot's
lifetime — that's the pool's job (see [pool.md §5](pool.md)). What
it observes and responds to:

| Event                                  | Slot runner's response                                                              |
| -------------------------------------- | ----------------------------------------------------------------------------------- |
| `recv_ctrl` returns `FdHandoff(fd)`    | Build `WireCtx`, run the wire, send `b'R'`, loop                                    |
| `recv_ctrl` returns `DrainRequest`     | Call `cleanup_per_slot_state`, send `b'A'`, exit cleanly (bgworker terminates)      |
| `recv_ctrl` returns `Eof`              | Frontend closed the UDS (crash or shutdown); exit cleanly                           |
| `recv_ctrl` returns timeout (100 ms)   | Re-check `sigterm_received()`; loop                                                 |
| SIGTERM observed                       | Exit cleanly. The FE's per-slot reader sees EOF on the stream and removes the slot from whichever container it was in. |
| Wire returns `Err`                     | Log warning, send `b'R'`, continue the loop (slot remains in the pool)              |
| Postmaster-death detected              | Postmaster will SIGTERM all bgworkers; handled via the SIGTERM path                 |

The slot runner has no concept of "session" beyond the bounds of one
wire `run` call.

**Drain ack timeout.** If the wire is mid-session when a
`DrainRequest` arrives, the ack only fires after `wire::run`
returns. The FE protects against an arbitrarily long wait with a
`DRAIN_ACK_TIMEOUT` (60 s default) watchdog that falls back to
`shutdown(SHUT_WR)` on the FE side — the slot's next `recv_ctrl`
then returns `Eof` and the slot exits. See
[pool.md §5.3](pool.md#53-shrink-on-idle-reaper--cooperative-drain).

---

## 7. Limitations specific to the slot layer

- **One wire instance per handoff.** The slot runner doesn't
  multiplex wire-layer sessions onto one slot. (The framework's
  [comparison.md](comparison.md) discussion of "slot pinning"
  applies here unchanged.)
- **Wire trait is compile-time, not configurable per row.** A slot
  runs whichever `W: Wire` the build was compiled with. In v0
  that's always `pgwire_v3::PgwireV3`. A future "let the catalog
  row pick the wire" feature would require either pluggable
  factories (the framework already has those for transports — same
  shape) or one slot pool per wire kind.
- **No transport-side message inspection.** The transport's
  [frontend-handoff.md §5](frontend-handoff.md) limitation pulls
  all the way through: the slot runner doesn't see protocol bytes
  either. Anything that needs them must live in the wire layer.
- **Dynamic slots are `BGW_NEVER_RESTART`.** If a slot crashes (or
  exits cleanly via drain / Eof), the postmaster does not respawn
  it. The next saturation event will grow a fresh slot via
  `pool::grow_one`; see [pool.md §5.2](pool.md#52-grow-on-demand).

(Wire-layer-side concerns — TLS, auth, extended-query state — live
in [backend-wire.md](backend-wire.md). Cancel routing is deferred
from v0; design in
[deferred/cancel-routing.md](deferred/cancel-routing.md).)

---

## See also

- [pool.md](pool.md) — the pool-level design: single UDS listener,
  three slot containers, autoscaling, cooperative drain, race-freedom
  proofs.
- [frontend-handoff.md](frontend-handoff.md) — the FE/IPC side:
  when this path applies, the per-connection sequence
  client → transport → handoff → slot dispatch.
- [backend-wire.md](backend-wire.md) — the layer this slot runner
  drives: wire trait, the v0 pgwire-v3 implementation via the
  `pgwire` (sunng87) crate, TLS, auth, SPI bridge, open questions.
- [api.md](api.md) — `HandoffHandle::handoff(fd)`, the only thing a
  transport calls.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design
  for the slot-runner-equivalent on the shm_mq general path.
