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

## 2. Runtime integration (tokio × pgrx × PG signals)

The frontend's bgworker entry point is roughly:

```rust
#[pg_guard]
pub extern "C" fn pg_transport_frontend_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime");

    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(frontend_main()));
}

async fn frontend_main() {
    let mut sighup  = tokio::signal::unix::signal(SignalKind::hangup()).unwrap();
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut pm_watchdog = tokio::time::interval(Duration::from_millis(500));

    // Stand up the backend pool and a root shutdown token.
    let pool     = BackendPool::start(backend_pool_config()).await;
    let shutdown = ShutdownToken::new();

    // For each enabled row in pg_transport.transports, build the transport
    // and spawn its `run` on the LocalSet. Each transport owns its own
    // accept loop, per-conn tasks, parsing, and TLS internally — the
    // frontend just holds the JoinHandles.
    let mut transports: Vec<JoinHandle<anyhow::Result<()>>> =
        spawn_transports_from_catalog(&pool, &shutdown).await;

    // Supervisor loop. Note there is no `transports.next()` arm: transports
    // run independently and only surface back here when they finish (which,
    // under normal operation, is only after `shutdown.cancel()`).
    loop {
        tokio::select! {
            _ = sigterm.recv()     => break,
            _ = sighup.recv()      => {
                reload_transports(&mut transports, &pool, &shutdown).await;
            }
            _ = pm_watchdog.tick() => {
                if !postmaster_is_alive() { break; }
            }
        }
    }

    // Graceful shutdown: cancel the shared token, then join every
    // transport's `run` future. Each Transport observes
    // `shutdown.cancelled()` and returns from `run` of its own accord;
    // we just wait for them.
    shutdown.cancel();
    let _ = futures::future::join_all(transports).await;
    pool.shutdown().await;
}
```

Four integration points worth calling out:

1. **Signal handlers, two-layered.** pgrx's `attach_signal_handlers` keeps
   PG's own bookkeeping flags (`ConfigReloadPending`, `ShutdownRequested`)
   honest. `tokio::signal::unix::signal` gives us async wakeups. Both fire;
   the tokio side is what drives our control flow.
2. **Postmaster-death watchdog.** A 500 ms `interval` task polls
   `PostmasterIsAlive()` (also reachable via pgrx). On Linux the
   `PR_SET_PDEATHSIG = SIGTERM` that PG installs on bgworker startup means
   our SIGTERM branch usually fires first; the watchdog is the belt to that
   suspenders.
3. **`LocalSet` for per-connection tasks.** Transports `spawn_local`
   per-connection handlers on the same `LocalSet`. Those handlers may
   hold non-`Send` PG types (e.g. cached backend handles), which a
   multi-thread runtime would forbid. The price is no work-stealing, which
   we don't want anyway given C-2.
4. **Supervisor never polls transports.** A `Transport::run` future is
   simply spawned and joined at shutdown. There is no per-iteration
   "next event from a listener" arm in the supervisor `select!` — that
   would re-introduce the very accept/dispatch abstraction the framework
   deliberately pushes inside transports.

---

## See also

- [api.md](api.md) — the `HandoffTransport` trait and `HandoffHandle`
  contract shown above (plus the deferred `SessionTransport`/`SessionHandle`
  sketch).
- [frontend-handoff.md](frontend-handoff.md) — what happens on the other side of
  `HandoffHandle::handoff`.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for the
  shm_mq general path.
