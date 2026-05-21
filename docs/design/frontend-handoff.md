# Frontend handoff — blind fd-pass for kernel-socket transports

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [architecture.md](architecture.md) · [pool.md](pool.md) · [backend-handoff.md](backend-handoff.md) · [backend-wire.md](backend-wire.md)

This doc describes the **FE (transport / frontend / IPC) side** of
`pg_transport`'s v0 fd-pass path: when this path applies, the
end-to-end per-connection sequence, and the performance / comparison
story at the dispatch-model level. The **pool-level design**
(single UDS listener topology, autoscaling, cooperative drain, the
four-opcode wire protocol) lives in its own doc:
[pool.md](pool.md). BE companions:
[backend-handoff.md](backend-handoff.md) (per-slot socket layer)
and [backend-wire.md](backend-wire.md) (TLS, auth, FE/BE v3, SPI bridge).

A transport that holds an `OwnedFd` for a kernel socket (TCP, UDS,
future io_uring-backed sockets) hands the fd over to a backend
bgworker and steps out entirely. The backend's slot runner takes
ownership of the fd and hands it to its wire layer, which speaks
FE/BE v3 to the client directly.

In v0 this is the **only** path. A second path — the shm_mq-based
*general path* for transports that need plaintext inspection, run
non-FE/BE wires (HTTP/2 + SQL, custom binary), or have no shareable
kernel fd (QUIC, DPDK) — is sketched in
[deferred/backend-pool.md](deferred/backend-pool.md) but **deferred**.

**Dispatch model**: conceptually identical to default PostgreSQL
(postmaster `accept()`s, hands the fd to a child, child runs the
backend). We replace `fork` with `SCM_RIGHTS` over a Unix control
socket, and "freshly forked backend" with "dynamically-spawned
bgworker pool member". What runs on the fd is our wire layer, not
unchanged PG code — see
[architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer)
for why. Per-stage comparison: [backend-wire.md §9](backend-wire.md).

---

## 1. When to use this path

In v0: **always**. The single v0 transport (`tcp_handoff`) has a
kernel fd and speaks FE/BE v3, so it hands off. Future fd-producing
transports (deferred `uds_handoff`, io_uring variants, …) will use
the same path. The decision rule, once the deferred general path
lands: **if you have a kernel fd and the wire is FE/BE, use handoff;
otherwise use shm_mq.** A full scenarios table is in
[deferred/backend-pool.md](deferred/backend-pool.md).

---

## 2. The mechanism

The FE binds **one** UDS listener at a well-known path; every slot
bgworker connects to it; the FE assigns a monotonic `slot_id` on
each accept. There is no per-slot listener and no `bgw_main_arg`
plumbing of slot ids. All four message types ride on each connected
stream:

| Direction | Opcode  | Meaning                  | Ancillary       |
|-----------|---------|--------------------------|-----------------|
| FE → BE   | `b'\0'` | fd handoff               | SCM_RIGHTS(fd)  |
| FE → BE   | `b'X'`  | drain request            | none            |
| BE → FE   | `b'R'`  | ready for next handoff   | none            |
| BE → FE   | `b'A'`  | drain ack ("exiting")    | none            |

The full topology, race-freedom argument, and autoscaling/drain
state machine live in [pool.md](pool.md). This section covers the
transport-facing surface; subsections below summarise setup,
teardown, and the per-connection flow.

### 2.1 Listener setup (one-time, at FE boot)

We follow PG's own client-UDS playbook (`src/backend/libpq/pqcomm.c`)
for where to put the socket and how to clean it up:

- **Directory** — `pg_transport.socket_directory` GUC (string, `SUSET`;
  planned, see [configuration.md](configuration.md)).
  Defaults to the first entry of `unix_socket_directories` (and
  `/tmp` if that is empty). Set it to `/var/run/postgresql` on
  distros that put PG's client UDS there, so the framework's
  internal socket shares the same backup / SELinux / AppArmor story.
  Phase-2 hard-codes the directory to `/tmp/pg_transport_sockets/`
  (see
  [paths.rs](../../crates/core/src/backend/paths.rs)).
- **Filename** — `frontend.sock`. One file per cluster; multi-cluster
  collision avoidance is the directory's job (the planned phase-7
  GUC above), not the filename's.
- **Permissions** — `chmod 0700` on the parent dir + `chmod 0600`
  on the socket file. No `socket_permissions` / `socket_group`
  GUCs: this is a framework-internal channel, not a client surface,
  and the design's eventual `SO_PEERCRED` peer-uid check (deferred,
  see [pool.md §2.0](pool.md#20-uds-topology-one-listener-one-stream-per-slot))
  is the real enforcement.
- **Cleanup** — a defensive `unlink(path)` at bind time removes any
  leftover socket file from a crashed previous run; that's the only
  cleanup needed because the file lives across slot lifetimes.

```
Frontend (boot)
────────────────
1. dir  = paths::slot_dir()            // /tmp/pg_transport_sockets (phase 2)
   path = dir.join("frontend.sock")
2. fs::create_dir_all(dir); chmod(dir, 0o700)
3. fs::remove_file(path)               // defensive; ignore ENOENT
4. listener = UnixListener::bind(path)
5. chmod(path, 0o600)
6. spawn(accept_loop(listener, pool, shutdown))
7. spawn(idle_reaper(pool, shutdown))

Backend slot bgworker (`pg_transport_slot_main`)
─────────────────────────────────────────────────
8. stream = UnixStream::connect_with_retry(path)   // 100 × 100 ms
9. stream.set_nonblocking(true)
   // No bgw_main_arg to read: the FE assigns slot_id on accept.

Frontend accept_loop
────────────────────
10. (stream, _) = listener.accept().await
11. async_stream = wrap_for_async(stream)          // Arc<AsyncFd<UnixStream>>
12. slot_id = pool.next_slot_id()
13. pool.in_flight.insert(slot_id, SlotPeer { ... })
14. spawn(slot_reader_wrapper(slot_id, async_stream, pool))

Process-wide
────────────
15. Mask SIGPIPE on the frontend (libc::signal SIG_IGN).
    We want EPIPE returned from sendmsg, not a signal.
```

After step 14 the slot is "parked" in the `in_flight` container.
The BE will send its first `b'R'` immediately after step 9; the FE
reader observes it and calls `pool.on_ready(slot_id)`, which moves
the slot to `ready`. The dispatcher pops it on the next handoff.

### 2.2 Slot teardown — three cases

| Case                              | Trigger                                   | What each side observes                                                                 | Recovery                                                                                  |
| --------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Cooperative drain (idle reaper or SIGHUP-lowered ceiling)** | FE moves slot `ready → draining`, sends `b'X'` | BE's next `recv_ctrl` returns `DrainRequest`; cleans up per-slot state; sends `b'A'`; exits | FE reader sees `b'A'` → `on_drain_ack` removes from `draining`. Slot bgworker terminates with no postmaster respawn (`BGW_NEVER_RESTART`). |
| **Frontend dies unexpectedly**    | FE panic, kill -9, crash                  | Kernel closes the FE endpoint. BE's `recv_ctrl_async` returns `Eof` on next 100 ms timeout tick; slot exits cleanly. | Postmaster restarts the FE bgworker; FE starts with an empty pool. First arriving handoff triggers a fresh `grow_one`. |
| **Backend dies unexpectedly**     | BE crash, kill -9, OOM kill               | Kernel closes the BE endpoint. FE's per-slot reader sees EOF on `read_one_be_opcode`; `cleanup_orphaned(slot_id)` removes the slot from whichever container it was in and `shutdown(SHUT_RDWR)` on the stream. | The dispatcher's next `send_fd_async` (if the slot was in `ready` momentarily) returns EPIPE; the handoff retries on another slot or grows a fresh one. The dead slot is gone; no respawn. |

Notes:

- **Drain ack timeout.** A `DRAIN_ACK_TIMEOUT` (60 s default) watchdog
  is spawned per drain. If `b'A'` doesn't arrive (BE stuck in a
  long-running query that ignores SIGTERM), the watchdog
  `shutdown(SHUT_WR)`s the FE side, the BE's next `recv_ctrl`
  returns `Eof`, and the slot exits. See
  [pool.md §5.3](pool.md#53-shrink-on-idle-reaper--cooperative-drain).
- **In-flight handoffs at backend crash.** If `sendmsg` from the FE
  already succeeded but the BE died before `recv_ctrl` returned (or
  after, but before running the wire), the passed fd is closed when
  the BE process exits and the client sees a TCP reset. The
  transport's `handle.handoff(fd).await` returned `Ok`. This matches
  default PG's `fork`+startup-crash semantics: the postmaster has no
  synchronous confirmation either. Adding a backend-side ack per
  handoff would buy reliability at the cost of a round-trip per
  connection; we accept the asymmetry (Q21).
- **No abstract-namespace UDS.** Linux's abstract namespace (path
  starting with `\0`) would skip the unlink dance, but it's
  Linux-only and doesn't compose with `chmod` for file-permission
  gating. The filesystem path + `chmod 0600` is portable across the
  Unixes we target.
- **Why not `socketpair()`.** Tempting (no path, no permissions, no
  race), but the slot bgworker is forked by **postmaster**, not by
  the FE. The FE can't pre-position one half of a `socketpair` into
  a process the postmaster will fork later. A named UDS both sides
  look up by path is the only portable option.

### 2.3 Per-connection flow

```
Client                  Transport (frontend)            Backend slot (bgworker)
══════                  ══════════════════════            ═══════════════════════

TCP connect ──────────► accept() → tcp_sock
                        │
                        │ handle.handoff(fd, hints).await
                        │   ├─ fast path: pop ready slot
                        │   │   from `ready`; insert into
                        │   │   `in_flight`; send_fd_async
                        │   └─ slow path: grow_one() if under
                        │       ceiling, then wait on has_ready
                        │       up to HANDOFF_WAIT
                        │
                        │ send_fd_async(slot.stream,
                        │   tcp_sock,
                        │   prefixed with b'\0' opcode) ────►   recv_ctrl_async returns
                        │                                       FdHandoff(fd)
                        ▼                                      │
                handoff returns Ok                             │ slot runner builds WireCtx;
                                                               │ hands fd to W::run
                                                               │
                                                               │ -- wire layer (backend-wire.md):
SSLRequest / Startup / Cancel ─────────────────────────────►   │ pgwire crate parses 8-byte probe
                                                               │   ← our dispatcher decides:
       ◄────────────────────────────────────────── 'S' or 'N'  │     SSLRequest → tokio_rustls accept
                                                               │     GSSEncRequest → 'N' (deferred)
                                                               │     StartupV3 → continue
                                                               │     CancelRequest → v0: log + close fd
                                                               │                       (cancel routing deferred;
                                                               │                        see deferred/cancel-routing.md)
                                                               │
TLS ClientHello (if SSL) ──────────────────────────────────►   │ rustls handshake on fd;
                                                               │   pgwire sees plaintext side
                                                               │
StartupMessage (over TLS) ─────────────────────────────────►   │ pgwire parses; auth dispatcher
                                                               │   calls hba_getauthmethod() and
                                                               │   runs the method (SCRAM via
                                                               │   pgwire helpers; verifier reads
                                                               │   pg_authid.rolpassword via SPI)
       ◄────────────────────────────────────────── Auth req    │
                                                               │
       ◄────────────────────────────────────────── ParameterStatus, BackendKeyData
       ◄────────────────────────────────────────── ReadyForQuery
                                                               │
                                            FE message loop in the wire layer;
                                            SQL execution via SPI_*
                                                               │
                                            on EOF / Terminate / wire fatal:
                                                                W::run returns,
                                                                slot runner closes fd,
                                                                resets per-handoff state,
                                                                sends b'R' for next handoff,
                                                                slot returns to `ready`
```

After `handle.handoff(fd).await` returns `Ok` in the FE, the FE has
no further role for this connection. All FE/BE I/O is between the BE
wire layer and the client over the inherited fd, using
`pgwire`-parsed messages and (for TLS) `tokio_rustls`'s `TlsStream`.

### 2.4 Saturation

If the pool is at `max_backend_pool_size` and every slot is
`in_flight`, `handle.handoff(fd, hints).await` waits on
`has_ready.notified()` for up to `HANDOFF_WAIT` (5 s default).
Timeout yields `Err(anyhow!("no slot became ready within ..."))`;
the transport (`tcp_handoff`) logs WARNING and drops the client fd,
which the client observes as a TCP reset. See
[pool.md §5.5](pool.md#55-saturation-behavior). A future enhancement
could split `handoff()` into "reserve slot" + "send" so the
transport could write a v3 `ErrorResponse("sorry, too many clients
already")` on the client fd before closing — for now, saturated
clients see a TCP reset.

---

## 3. Comparison with default PG

This is the dispatch-model comparison. For the wire-layer vs PG's wire
code comparison, see [backend-wire.md §9](backend-wire.md).

| Stage                           | Default PG                          | `pg_transport` handoff path             |
| ------------------------------- | ----------------------------------- | --------------------------------------- |
| Listen on port                  | postmaster's `ServerLoop`           | frontend's tokio accept loop            |
| Accept a connection             | postmaster                          | frontend                                |
| Hand socket to child            | `fork()`; child inherits fd         | `sendmsg(SCM_RIGHTS)` to slot's bgworker |
| Per-child startup cost          | full fork (~1 ms on Linux)          | fd-pass (~10 µs); bgworker pre-spawned  |
| Protocol probe (SSLRequest etc) | `ProcessStartupPacket` (PG C code)  | wire layer via `pgwire` crate           |
| TLS handshake                   | `secure_open_server` (PG-wrapped OpenSSL) | `tokio_openssl::accept` (rust-openssl) in the wire layer |
| Auth                            | `ClientAuthentication` + `pg_hba.conf` | wire layer: `hba_getauthmethod` lookup + our Rust-side method execution |
| FE/BE message loop              | `PostgresMain` (`tcop/postgres.c`)  | wire layer's driver loop                |
| SQL execution                   | inline in `exec_*` calls            | SPI bridge: `SPI_execute` / `SPI_execute_plan_with_params` |
| Lifecycle after disconnect      | backend exits; postmaster reaps     | slot runner resets per-handoff state, slot returns to pool |
| Cancel routing                  | postmaster looks up PID, signals    | **v0: not supported.** Wire layer drops `CancelRequest` fds silently; Ctrl-C in `psql` terminates the connection. Design for un-deferral: [deferred/cancel-routing.md](deferred/cancel-routing.md). |

Dispatch-model differences:

1. **`fork()` → `SCM_RIGHTS`**: pre-spawned children, ~100× cheaper per
   connection, recycled instead of exiting.
2. **One global listener (postmaster) → many configurable listeners
   (our transports)**: TCP on a custom port, UDS at a custom path,
   future io_uring-backed listeners, all simultaneously, all
   dispatching to the same backend pool.

What-runs-on-the-fd differences (see [backend-wire.md](backend-wire.md)
for detail):

3. **We don't reuse PG's wire code.** Our wire layer is Rust + the
   `pgwire` crate; `ProcessStartupPacket`, `ClientAuthentication`,
   `secure_open_server`, and `PostgresMain` are not called. SPI
   (planner, executor, snapshots) *is* reused for the actual SQL
   execution — the wire-layer surface is what changes.
---

## 4. Performance

| Operation                       | Default PG | `pg_transport` handoff                          |
| ------------------------------- | ---------- | ----------------------------------------------- |
| Connect → first query (warm pool) | ~1 ms (fork + setup) | ~60 µs (pool ready slot + fd-pass + startup) |
| Connect → first query (cold grow) | ~1 ms (fork + setup) | ~20–100 ms (postmaster fork + tokio runtime + TLS acceptor + first b'R'; see [pool.md §5.2](pool.md#52-grow-on-demand)) |
| `SELECT 1` round-trip           | ~30–50 µs  | ~40–60 µs (~10 µs fd-pass-related overhead, amortised after first query) |
| TLS handshake                   | ~1–5 ms (RSA dominates) | identical (same OpenSSL code, or rustls of comparable performance) |
| Per-query TLS data-path         | ~5–20% throughput hit | identical (TLS work is intra-backend) |
| Concurrent connections          | one fork per       | one ready-slot dispatch per                   |

The cold-grow row is only paid by the very first connection after an
idle period (when `MIN_WARM_SLOTS = 0`, the default). Subsequent
arrivals find the slot in `ready` and pay the warm cost. Operators
that want zero cold-grow latency set a higher `MIN_WARM_SLOTS` —
currently a compile-time constant, see [pool.md §5.1](pool.md#51-the-single-knob).

Effectively the same as default PG for steady-state queries, with a
*better* connection-establishment story (no fork) in the warm-pool
case. The framework's overhead disappears into the noise of any
non-trivial query.

This is the design ceiling. Going faster than this within `pg_transport`'s
bgworker model would mean letting backend processes do their own `accept()`
on a shared listening socket (so the frontend is bypassed in the data
path). We considered and **rejected** that approach — see
[Rejected alternatives in roadmap.md](roadmap.md). The single-frontend /
autoscaling-backend-pool / `SCM_RIGHTS`-handoff model is the design.

---

## 5. Limitations (FE side)

- **No transport-side message inspection.** Once handoff is done the
  transport doesn't see protocol bytes. If you need to log, filter, or
  rewrite FE/BE traffic, you need the deferred shm_mq path — see
  [backend-pool.md](deferred/backend-pool.md).
- **No pre-auth filtering by FE/BE content.** The transport can still
  filter by client IP, TLS SNI, or whatever it sees pre-handoff, but
  nothing in StartupMessage. (Auth happens entirely in the backend.)
- **Slot is pinned for the connection's lifetime.** No multiplexing
  multiple handoff connections onto one slot. (Same as default PG's
  fork model, deliberately.)
- **Saturation surfaces as TCP reset, not `ErrorResponse`.** When the
  pool is at its ceiling and the dispatcher times out on
  `HANDOFF_WAIT`, the client sees the TCP socket close — not a v3
  `ErrorResponse("too many connections")` like vanilla PG sends. A
  future enhancement could split the handoff into "reserve slot" +
  "send" so the transport can write a wire-format error before
  dropping the fd. See §2.4.
- **OS portability.** `SCM_RIGHTS` works on every Unix; Windows needs
  `DuplicateHandle` + named pipes. Phase 6+ Linux/BSD/macOS only.

(BE-side concerns — wire-layer TLS choice, auth model, cancel
routing, per-handoff reset detail — live in [backend-handoff.md §5](backend-handoff.md#5-per-handoff-state-reset)
and [backend-wire.md](backend-wire.md).)

---

## 6. What this resolves vs the deferred shm_mq path

Several open questions in earlier drafts of the design were really
"how does this work for fd-based transports?". The handoff path
resolves them inside this doc, [pool.md](pool.md), and the BE companions:

| Earlier open question                       | Resolution under handoff                                                                  |
| ------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Auth handshake location                     | Entirely in the backend wire layer; details in [backend-wire.md §4](backend-wire.md).      |
| TLS termination location                    | Entirely in the backend wire layer (rustls); details in [backend-wire.md §5](backend-wire.md). |
| SCM_RIGHTS vs no-SCM_RIGHTS for fd transfer | SCM_RIGHTS, single FE UDS listener — see [pool.md §2](pool.md#2-uds-message-framing).      |
| Per-connection backend pinning              | Implied by the handoff itself — slot is pinned to connection.                            |
| Cross-process wakeup for fast path          | Not needed — once the fd is handed off, all I/O is direct between client and backend.    |
| Slot dispatch — round-robin vs demand       | Demand-driven via `b'R'` ready signal; FE only sends fds to slots that have advertised readiness. See [pool.md §1](pool.md#1-design-ready-byte-protocol). |
| Pool sizing — static vs dynamic             | Autoscaled under a single ceiling GUC. See [pool.md §5](pool.md#5-autoscaling--single-guc-demand-driven). |

Those questions reopen for the deferred shm_mq path; see
[backend-pool.md](deferred/backend-pool.md) and
[roadmap.md §4 — Deferred for v0](roadmap.md).

---

## See also

- [pool.md](pool.md) — the pool-level design: single UDS listener,
  three slot containers, autoscaling, cooperative drain, race-freedom
  proofs.
- [backend-handoff.md](backend-handoff.md) — BE slot runner (per-handoff
  socket layer): fd receipt, slot lifecycle, per-handoff reset.
- [backend-wire.md](backend-wire.md) — BE wire layer: TLS, auth, FE/BE
  protocol via `pgwire` crate, SPI bridge, open questions.
- [api.md](api.md) — `HandoffHandle::handoff` (async; returns
  `HandoffFuture<'_>`).
- [transports.md](transports.md) — which transports use this path (in
  v0: all of them).
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for the
  shm_mq general path.
- [../background/pg_background.md](../background/pg_background.md) — the
  pg_background mechanism the deferred shm_mq path lifts.
