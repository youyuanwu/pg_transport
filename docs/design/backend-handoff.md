# Backend slot runner — the socket layer

> Parent: [README.md](README.md)
> Sibling: [frontend-handoff.md](frontend-handoff.md) · [backend-wire.md](backend-wire.md) · [api.md](api.md)

This doc describes the **socket layer** of the BE side of `pg_transport`'s
v0 fd-pass path. The slot runner owns the fd, the per-slot lifecycle,
and the per-handoff reset. It does **no** wire-protocol work — that's
the [backend-wire.md](backend-wire.md) layer.

Companions:

| Layer (BE side)              | Doc                                  |
| ---------------------------- | ------------------------------------ |
| **Slot runner — socket layer (this doc)** | **backend-handoff.md**  |
| Wire — TLS, auth, FE/BE v3 protocol | [backend-wire.md](backend-wire.md) |
| Execution — SQL via SPI       | (implicit; see backend-wire.md §6)   |

The FE/IPC companion ([frontend-handoff.md](frontend-handoff.md)) covers when this path
applies, the per-slot control socket setup/teardown, and the
`sendmsg(SCM_RIGHTS)` mechanics that bring an fd into the slot runner.

The slot runner picks up at the moment its main loop pulls one
`(fd, hints)` off the per-slot control socket; it ends when the wire
layer's `run` returns and the slot runner performs the per-handoff
reset.

The slot runner never speaks the IPC protocol directly; it just
receives `OwnedFd`s and a small `HandoffHints` block accompanying each
one. The deferred shm_mq general path's slot-runner equivalent lives
in [backend-pool.md](deferred/backend-pool.md).

---

## 1. Pool spawning (`_PG_init` + `shared_preload_libraries`)

Before there is a slot runner there has to be a *slot bgworker* for it
to run in. v0 spawns the entire pool statically at postmaster start.

### Requirement: `pg_transport` must be in `shared_preload_libraries`

The operator's `postgresql.conf` must contain:

```
shared_preload_libraries = 'pg_transport'
```

This causes `pg_transport.so` to be loaded into the postmaster and
the extension's `_PG_init()` to run inside the postmaster, very early
in `PostmasterMain()` — the only context where PG accepts static
`RegisterBackgroundWorker` calls.

If SPL is missing, `_PG_init()` never runs in the postmaster; the pool
is never registered; the first `SELECT pg_transport.start()` raises a
clear `FATAL` ("pg_transport must be in shared_preload_libraries").
We do not have a lazy-spawn fallback in v0. (Rationale and the
rejected lazy path are in
[roadmap.md §2 Q4](roadmap.md#2-open-questions).)

### What `_PG_init()` does

```rust
// crates/core/src/lib.rs (sketch)
#[pg_guard]
pub extern "C" fn _PG_init() {
    // Refuse to proceed when not in shared_preload_libraries: in that
    // case _PG_init runs in a regular backend, not the postmaster, and
    // RegisterBackgroundWorker is illegal.
    if !pg_sys::process_shared_preload_libraries_in_progress {
        ereport!(FATAL, "pg_transport must be in shared_preload_libraries");
    }

    register_gucs();   // pg_transport.backend_pool_size, .auth_source, …

    // Validate required GUCs at postmaster start, BEFORE any tokio
    // runtime or bgworker resources are allocated. A missing
    // pg_transport.auth_source is an operator error we surface
    // immediately rather than letting it FATAL inside a slot bgworker
    // (which would degrade the pool silently). See configuration.md
    // and Q4's resolution in roadmap.md.
    guc::auth_source().unwrap_or_else(|| ereport!(
        FATAL,
        "pg_transport.auth_source must be set to 'pg_hba' or 'pg_transport'"
    ));

    let pool_size = guc::backend_pool_size();   // default max(4, num_cpus)
    for slot_id in 0..pool_size {
        BackgroundWorkerBuilder::new(&format!("pg_transport slot {slot_id}"))
            .set_type("pg_transport_slot")
            .set_library("pg_transport")
            .set_function("pg_transport_slot_main")
            .set_argument(Some((slot_id as i32).into()))
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(1)))
            .enable_shmem_access(None)
            .load();   // ← static; calls RegisterBackgroundWorker under the hood
    }

    // The frontend bgworker (Q22 → option (a) in roadmap.md §2.3).
    // Registered statically here so the operational story is "set
    // shared_preload_libraries and restart"; pg_transport.start() is
    // a listener-set operation, not a process-spawn operation.
    BackgroundWorkerBuilder::new("pg_transport frontend")
        .set_type("pg_transport_frontend")
        .set_library("pg_transport")
        .set_function("pg_transport_frontend_main")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_shmem_access(None)
        .load();
}
```

Once `PostmasterMain` finishes recovery, the postmaster spawns all
`pool_size` slot workers (each running `pg_transport_slot_main(slot_id)`,
the entry point for the slot runner loop in §2 below) plus the single
frontend worker (running `pg_transport_frontend_main`, the entry point
in [architecture.md §2](architecture.md#2-runtime-integration-tokio--pgrx--pg-signals)).

### Why static (not `load_dynamic()`)

pgrx also exposes `BackgroundWorkerBuilder::load_dynamic()`, which
wraps PG's `RegisterDynamicBackgroundWorker` and lets a regular
backend register workers at run time. We don't use it in v0 because:

- The pool is sized once and lives for the whole cluster lifetime;
  there is no per-request spawn pattern (unlike `pg_background`).
- Static registration makes the postmaster the parent and the
  restart-policy owner (`bgw_restart_time`). Dynamic registration
  ties the worker's lifetime to the backend that registered it
  unless `bgw_notify_pid` is zeroed — a foot-gun we don't need.
- First-connection latency does not include pool spawn time.
- One operational story: "the pool is up iff the postmaster is up."

Live pool resize via `load_dynamic()` (grow-only) is sketched in
[roadmap.md §2 Q15](roadmap.md#2-open-questions) and **deferred** —
static sizing at `_PG_init()` is sufficient for v0 and the phase-5
bench harness.

### What `_PG_init()` does *not* do

- It does not start the tokio runtime in the postmaster. Runtimes
  are per-bgworker, created inside each bgworker's `*_main`
  entry point (see [§3 Async / threading model](#3-async--threading-model)
  for slots; [architecture.md §2](architecture.md#2-runtime-integration-tokio--pgrx--pg-signals)
  for the frontend).
- It does not bind any sockets or read the catalog. All of that is
  deferred to the frontend bgworker, which lazily binds on
  `pg_transport.start()` (see [Q22 in roadmap.md §2.3](roadmap.md#23-resolved)).

---

## 2. The slot runner loop

```rust
// crates/core/src/backend/slot.rs (sketch; pgrx + raw pg_sys + our wire trait)

fn run_slot<W: Wire>(slot: SlotCtx) -> anyhow::Result<()> {
    loop {
        // Block on the per-slot UDS control socket; receive one
        // (fd, HandoffHints) per iteration.
        let (fd, hints) = match slot.unix_ctrl.recvmsg() {
            Ok(msg)               => decode_handoff(msg)?,
            Err(EofOrShutdown)    => return Ok(()),
        };

        // the wire-layer ctx for this handoff; bundles slot.shutdown,
        // the SPI bridge handle, hints.tls_allowed, etc.
        let ctx = WireCtx::new(&slot, hints);

        // Hand the fd to the wire layer and let it run until the client
        // disconnects (or the wire itself errors fatally, or shutdown).
        // We don't see any protocol bytes; the wire owns them.
        let outcome = W::run(fd, ctx);

        // Per-handoff reset, regardless of how the wire returned.
        // Drops the wire's per-session state and any SPI artefacts
        // the wire created (prepared statements, portals, temp tables,
        // GUCs touched via SET).
        reset_per_handoff_state();

        // Log fatal wire errors; non-fatal ones are part of normal
        // disconnects and don't need attention.
        if let Err(e) = outcome { tracing::warn!(?e, "wire run failed"); }
    }
}
```

The slot runner never:

- Reads or writes the fd directly (it doesn't even know what wire
  format the bytes are in).
- Calls PG's `ProcessStartupPacket`, `ClientAuthentication`,
  `secure_open_server`, or `PostgresMain` (see
  [backend-wire.md](backend-wire.md) for the rationale).
- Opens or closes TLS sessions.
- Looks at FE/BE message types.

Everything in that list is the wire layer's job. The slot runner is
deliberately small: a `recvmsg` loop, a wire instantiation, a wire
run, and a reset.

> **Silent handoff loss — resolved as accept (option b).** If the
> backend dies between the frontend's last observation and the
> `recvmsg` above, the kernel buffers the SCM_RIGHTS payload but
> nobody consumes it — the current client connection is orphaned
> (TCP RST). The dead slot is detected on the *next* `sendmsg →
> EPIPE` and respawned then. No per-handoff ack; zero added cost on
> the happy path. Full reasoning in
> [roadmap.md §2 Q21](roadmap.md#2-open-questions).

---

## 3. Async / threading model

The slot runner runs on the bgworker's **main thread — the only
thread**. That thread also hosts a small **single-threaded tokio
runtime** that the wire layer uses; everything else is sync.

```
backend bgworker process (single thread)
├─ slot runner loop                              (plain sync, no tokio)
│    loop {
│      let (fd, hints) = unix_ctrl.recvmsg();   // blocking syscall
│      runtime.block_on(W::run(fd, build_ctx())); // ← tokio enters here
│      reset_per_handoff_state();
│    }
│
└─ runtime: tokio::runtime::Builder::new_current_thread()
             .enable_all()
             .build()                              // built once at bgworker boot
                                                   // reused across handoffs
```

### Why tokio appears in the backend at all

Forced by the choice in [backend-wire.md §2](backend-wire.md) to reuse
the [`pgwire`](https://github.com/sunng87/pgwire) crate and
`tokio-openssl` for TLS. Both expose `tokio::io::AsyncRead` /
`AsyncWrite`-shaped APIs and `async fn` trait methods; running them
needs an async executor. A hand-rolled sync wire (using `openssl`'s
sync `SslStream` and our own FE/BE codec) would avoid tokio entirely
on this side, at the cost of giving up pgwire's protocol code.

### What this runtime is *not*

- **Not a concurrency primitive.** Exactly one task runs on it (the
  current handoff's `W::run` future). One client per slot at a time,
  by construction.
- **Not multi-threaded.** That would violate C-2 (PG state is
  single-threaded). Current-thread only.
- **Not a `LocalSet`-bearer.** No `spawn_local`; the wire's `run`
  future is the whole workload. The frontend uses `LocalSet` because
  it multiplexes per-transport tasks; the backend has nothing to
  multiplex.
- **Not rebuilt per handoff.** One `Runtime` per bgworker, reused for
  every `block_on(...)`. Building tokio runtimes costs hundreds of
  microseconds; per-handoff construction would dominate connection
  setup.
- **Not running the slot runner loop itself.** The `recvmsg` loop and
  the per-handoff reset are plain sync code *outside* `block_on`. The
  runtime is entered per handoff and exited when the wire returns.

It's effectively a **trampoline / executor for the wire's async
future**, not a scheduler.

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
  allocations). With `backend_pool_size = max(4, num_cpus)` that's
  small in absolute terms.
- Per-handoff `block_on` entry cost is microseconds (no runtime
  construction; just polling the first frame of the wire's `run`
  future).
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

The slot runner doesn't make any choices about the slot's lifetime
itself — that's policy enforced by the pool above. What it observes
and responds to:

| Event                              | Slot runner's response                                                              |
| ---------------------------------- | ----------------------------------------------------------------------------------- |
| `recvmsg` returns a handoff        | Build `WireCtx`, run the wire, reset, loop                                          |
| `recvmsg` returns EOF              | Frontend's end of the per-slot UDS closed; clean exit                               |
| `shutdown.cancelled()` fires       | Stop accepting new handoffs; if a wire is currently running, wait for it to observe the shutdown token via `WireCtx`; reset and exit |
| Wire returns `Err`                 | Log, reset, continue the loop (slot remains in the pool)                            |
| Postmaster-death detected          | Behaves the same as shutdown                                                        |

The slot runner has no concept of "session" beyond the bounds of one
wire `run` call.

---

## 7. Limitations specific to the slot layer

- **One wire instance per handoff.** The slot runner doesn't multiplex
  wire-layer sessions onto one slot. (The framework's [comparison.md](comparison.md)
  discussion of "slot pinning" applies here unchanged.)
- **Wire trait is compile-time, not configurable per row.** A slot
  runs whichever `W: Wire` the build was compiled with. In v0 that's
  always `pgwire_v3::PgwireV3`. A future "let the catalog row pick the
  wire" feature would require either pluggable factories (the
  framework already has those for transports — same shape) or one slot
  pool per wire kind.
- **No transport-side message inspection.** The transport's
  [frontend-handoff.md §5](frontend-handoff.md) limitation pulls all the way through:
  the slot runner doesn't see protocol bytes either. Anything that
  needs them must live in the wire layer.

(Wire-layer-side concerns — TLS, auth, extended-query state — live in
[backend-wire.md](backend-wire.md). Cancel routing is deferred from v0;
design in [deferred/cancel-routing.md](deferred/cancel-routing.md).)

---

## See also

- [frontend-handoff.md](frontend-handoff.md) — the FE/IPC side: when this path applies,
  per-slot control socket setup/teardown, `SCM_RIGHTS` mechanics, the
  end-to-end per-connection sequence.
- [backend-wire.md](backend-wire.md) — the layer this slot runner
  drives: wire trait, the v0 pgwire-v3 implementation via the
  `pgwire` (sunng87) crate, TLS, auth, SPI bridge, open questions.
- [api.md](api.md) — `HandoffHandle::handoff(fd)`, the only thing a
  transport calls.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for
  the slot-runner-equivalent on the shm_mq general path.
