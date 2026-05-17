# Architecture

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [handoff.md](handoff.md)

## 1. Three layers

```
┌──────────────────────────────────────────────────────────────────────┐
│  Transport plugins (workspace crates, linked into core)              │
│    tcp_handoff │ uds_handoff │ …                                       │
│                                                                      │
│    Each transport is a self-contained network entry point. It owns  │
│    its own accept loop and per-listener policy. The framework's     │
│    only requirement in v0:                                          │
│                                                                      │
│      impl HandoffTransport — receives a HandoffHandle               │
│                                                                      │
│    A second trait (SessionTransport) is anticipated and deferred;   │
│    see executor-pool.md.                                            │
│                                                                      │
│    Everything below the `run` boundary is the transport's business.│
└────────────────────────────┬─────────────────────────────────────────┘
                             │ HandoffHandle::handoff(fd)
┌────────────────────────────┴─────────────────────────────────────────┐
│  Dispatcher Core  (crate `core`, the pgrx extension)                 │
│    - tokio current-thread runtime + LocalSet                         │
│    - Transport registry (one map in v0; second map reserved for the  │
│      deferred SessionTransport)                                      │
│    - Lifecycle: spawn transports from catalog, signal handling,      │
│      postmaster-death watchdog                                       │
│    - HandoffHandle vending; metrics; GUCs                            │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ sendmsg(SCM_RIGHTS) over per-slot UDS
┌────────────────────────────┴─────────────────────────────────────────┐
│  Executor Pool  (in-tree crate `executor`)                           │
│    - Pre-spawned bgworker backends                                   │
│    - Owns inherited fd via MyProcPort; runs ProcessStartupPacket,    │
│      TLS, ClientAuthentication, PostgresMain-equivalent loop         │
│    - (DSM + shm_mq + pq_redirect_to_shm_mq plumbing for the          │
│      deferred general path is captured in executor-pool.md but       │
│      not built in v0.)                                               │
└──────────────────────────────────────────────────────────────────────┘
```

Three layers, sharply separated by interface size:

- **Transport plugins** — the thick layer. Each transport implements
  `HandoffTransport` and bundles its byte-pipe (TCP / UDS / future
  io_uring sockets) plus per-listener policy (bind address, IP
  allowlist, optional pre-handoff metadata). The framework imposes one
  method.
- **Dispatcher core** — the *thin* layer. tokio runtime, signal handling,
  postmaster-death watchdog, transport registry, `HandoffHandle`
  vending, metrics. Knows nothing about wire protocols.
- **Executor pool** — pre-spawned bgworker backends. Each receives the
  handed-off fd, makes it `MyProcPort.sock`, and runs PG's own
  `ProcessStartupPacket` → `ClientAuthentication` → `PostgresMain`-equivalent
  loop on it. Structurally identical to default PostgreSQL with
  `SCM_RIGHTS` in place of `fork`.

The framework deliberately does **not** define a `Connection` trait, an
`AsyncRead`/`AsyncWrite` boundary, or a `Protocol` abstraction. Once the
fd is handed off, the dispatcher has no further role for that connection.

### One path into the executor pool (v0)

The handle a transport receives in its `run` method has one method:

- **Handoff** — `HandoffHandle::handoff(fd)`. The transport holds an
  `OwnedFd` on which the wire is FE/BE v3; the dispatcher hands the
  socket to a bgworker via `SCM_RIGHTS`; the bgworker runs
  `ProcessStartupPacket`, TLS, auth, and the FE/BE loop on it directly.
  Structurally identical to default PostgreSQL with `SCM_RIGHTS` in
  place of `fork`. See [handoff.md](handoff.md).

### Deferred: the general (shm_mq) path

A second trait — `SessionTransport`, receiving a `SessionHandle` with
`execute(opts, payload)` and `acquire(opts).submit(payload)` — is
planned for non-FE/BE wires (HTTP/2 + SQL, custom binary) and for FE/BE
transports that need plaintext inspection. Its full design is in
[executor-pool.md](executor-pool.md); it is **not** built in v0. The
dispatcher's registry reserves a slot for its factory map so it can
land without a registry-shape change.

---

## 2. Runtime integration (tokio × pgrx × PG signals)

The dispatcher's bgworker entry point is roughly:

```rust
#[pg_guard]
pub extern "C" fn pg_transport_dispatcher_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime");

    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(dispatcher_main()));
}

async fn dispatcher_main() {
    let mut sighup  = tokio::signal::unix::signal(SignalKind::hangup()).unwrap();
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
    let mut pm_watchdog = tokio::time::interval(Duration::from_millis(500));

    // Stand up the executor pool and a root shutdown token.
    let pool     = ExecutorPool::start(executor_pool_config()).await;
    let shutdown = ShutdownToken::new();

    // For each enabled row in pg_transport.transports, build the transport
    // and spawn its `run` on the LocalSet. Each transport owns its own
    // accept loop, per-conn tasks, parsing, and TLS internally — the
    // dispatcher just holds the JoinHandles.
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
   hold non-`Send` PG types (e.g. cached executor handles), which a
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
- [handoff.md](handoff.md) — what happens on the other side of
  `HandoffHandle::handoff`.
- [executor-pool.md](executor-pool.md) — *deferred* design for the
  shm_mq general path.
