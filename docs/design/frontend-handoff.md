# Frontend handoff — blind fd-pass for kernel-socket transports

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [architecture.md](architecture.md) · [backend-handoff.md](backend-handoff.md) · [backend-wire.md](backend-wire.md)

This doc describes the **FE (transport / frontend / IPC) side** of
`pg_transport`'s v0 fd-pass path: when this path applies, how each
backend slot's per-slot control socket is set up and torn down, the
end-to-end per-connection sequence, and the performance / comparison
story at the dispatch-model level. Its BE companions:

| Doc                                          | What it owns                                      |
| -------------------------------------------- | ------------------------------------------------- |
| [backend-handoff.md](backend-handoff.md)     | Slot runner (socket layer): fd receipt, slot lifecycle, per-handoff reset |
| [backend-wire.md](backend-wire.md)           | Wire layer: TLS, auth, FE/BE v3 protocol, SPI bridge — implemented in Rust, *not* using PG's `ProcessStartupPacket` / `ClientAuthentication` / `secure_open_server` / `PostgresMain` |

A transport that holds an `OwnedFd` for a kernel socket (TCP, UDS,
future io_uring-backed sockets) hands the fd over to a backend
bgworker and steps out entirely. The backend's slot runner takes
ownership of the fd and hands it to its wire layer, which speaks
FE/BE v3 to the client directly (via the [`pgwire`](https://github.com/sunng87/pgwire)
crate, see [backend-wire.md](backend-wire.md)) and translates SQL into
`SPI_*` calls.

In v0 this is the **only** path. A second path — the shm_mq-based
*general path* for transports that need plaintext inspection, run
non-FE/BE wires (HTTP/2 + SQL, custom binary), or have no shareable
kernel fd (QUIC, DPDK) — is sketched in [backend-pool.md](deferred/backend-pool.md)
but **deferred**.

The dispatch model is conceptually identical to default PostgreSQL:
postmaster `accept()`s, hands the fd to a child via `fork`, and the
child runs the backend. We replace `fork` with `SCM_RIGHTS` over a
Unix control socket, and we replace "freshly forked backend" with
"pre-spawned bgworker pool member". What runs on the fd, however, is
our wire layer — not unchanged PG code. See
[backend-wire.md §9](backend-wire.md) for the per-stage comparison.

---

## 1. When to use this path

In v0: **always**. The single v0 transport (`tcp_handoff`) has a
kernel fd and speaks FE/BE v3, so it hands off. Future fd-producing
transports (deferred `uds_handoff`, io_uring variants, …) will use
the same path.

The table below records which future transport scenarios fit this path
versus the deferred general path. "shm_mq" rows are reachable only once
that path lands:

| Transport scenario                                | Path             |
| ------------------------------------------------- | ---------------- |
| TCP + FE/BE v3 (no transport-side processing)     | **handoff** (v0) |
| UDS + FE/BE v3                                    | **handoff** (v0) |
| TCP + FE/BE v3 + TLS                              | **handoff** (v0; backend terminates TLS) |
| io_uring-backed TCP + FE/BE v3                    | **handoff** (fd is a regular socket) |
| TCP + FE/BE + transport-side per-message logic    | shm_mq (deferred)|
| HTTP/2 + JSON ↔ SQL adapter                       | shm_mq (deferred)|
| QUIC + FE/BE                                      | shm_mq (deferred)|
| DPDK / AF_XDP                                     | shm_mq (deferred; no kernel fd) |
| Shared-memory loopback                            | shm_mq (deferred)|

The decision rule, once the general path lands: **if you have a kernel
fd and the wire is FE/BE, use handoff; otherwise use shm_mq.** For v0,
everything is handoff by construction.

---

## 2. The mechanism

Each backend-pool slot owns one cross-process channel, set up once at
slot startup:

| Channel                        | Type                     | Direction          | Used by      |
| ------------------------------ | ------------------------ | ------------------ | ------------ |
| Unix control socket            | `AF_UNIX` `SOCK_STREAM`  | frontend → exec  | handoff path |

(When the deferred general path lands, each slot will additionally
allocate a DSM segment containing `req_q` / `resp_q` `shm_mq`s. See
[backend-pool.md](deferred/backend-pool.md).)

### 2.1 Slot setup (one-time, per slot)

We follow PG's own client-UDS playbook (`src/backend/libpq/pqcomm.c`)
for where to put the socket and how to clean it up:

- **Directory** — `pg_transport.socket_directory` GUC (string, `SUSET`).
  Defaults to the first entry of `unix_socket_directories` (and `/tmp`
  if that is empty). Set it to `/var/run/postgresql` on distros that
  put PG's client UDS there, so the framework's internal sockets share
  the same backup / SELinux / AppArmor story.
- **Filename** — `.s.PG_TRANSPORT.<frontend_pid>.<slot_id>`. Leading
  `.s` mirrors PG's `.s.PGSQL.<port>`.
- **Permissions** — always `0600`, owned by the PG cluster user. No
  `socket_permissions` / `socket_group` GUCs: this is a framework-internal
  channel, not a client surface, and `SO_PEERCRED` is the real enforcement.
- **Cleanup** — each slot registers `on_proc_exit(unlink_slot_socket,
  slot_idx)` at setup time, the same mechanism `pqcomm.c::StreamDoUnlink`
  uses. The unlink fires on clean exit *and* on `ereport(FATAL)` /
  `proc_exit`-from-signal, so we don't need bespoke `Drop` plumbing
  around the listener.
- **No `Lock_AF_UNIX` lock file** — the frontend's PID is part of
  the filename, so two dispatchers cannot collide. PID reuse after the
  frontend dies is handled by the defensive `unlink` before `bind`.

The frontend creates the listener **before** registering the bgworker
(otherwise the bgworker's first `connect()` races and gets `ECONNREFUSED`):

```
Frontend (slot init, per slot, at pool startup)
─────────────────────────────────────────────────
1. dir  = GetConfigOption("pg_transport.socket_directory")
        ?: first(unix_socket_directories)
        ?: "/tmp"
   path = format!("{dir}/.s.PG_TRANSPORT.{frontend_pid}.{slot_id}")
2. unlink(path)                          // defensive, in case of stale file
3. listener_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)
4. bind(listener_fd, path); chmod(path, 0600)
   // chmod is explicit — bind's mode arg isn't honoured on all platforms.
5. on_proc_exit(unlink_slot_socket, slot_idx_as_datum)
   // PG's exit machinery does the unlink for us, including on FATAL.
6. listen(listener_fd, 1)                // backlog=1; only one client expected
7. RegisterDynamicBackgroundWorker(...)  // main_arg = (slot_id, path)

Backend bgworker (main)
────────────────────────
8. (slot_id, path) = read bgw_main_arg
9. conn_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)
10. connect(conn_fd, path)               // retry a few times with short
                                         // backoff to tolerate startup races

Frontend (continues)
──────────────────────
11. (stream, _) = listener_fd.accept().await
12. verify_peer(stream):                 // SO_PEERCRED on Linux,
      assert pid  == registered bgworker PID
      assert uid  == PG cluster owner
    // The path is filesystem-visible. Without this check a same-host
    // hostile process could race to connect first and receive fds we
    // send (leaking client sockets) or feed us crafted ones.
13. // Keep the path live on disk: a respawned bgworker (see §2.2)
    // needs to connect again. The on_proc_exit hook removes it at
    // frontend exit; that is the only unlink point.

Process-wide
────────────
14. Mask SIGPIPE on the frontend (sigaction SIG_IGN or pthread_sigmask).
    We want EPIPE returned from sendmsg, not a signal.
```

Steady state: the channel is one-way. The frontend does
`sendmsg(SCM_RIGHTS, hints_block)` per handoff; the backend's main loop
is just `recvmsg()` returning one fd plus the `HandoffHints` body each
iteration:

```
loop {
  let (fd, hints) = unix_ctrl.recvmsg()?;   // blocks
  handle_handoff(fd, hints);                // see §3
}
```

### 2.2 Slot teardown — three cases

| Case                              | Trigger                                   | What each side observes                                                                 | Recovery                                                                                  |
| --------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Orderly shutdown**              | `ShutdownToken::cancel()` (SIGTERM, postmaster death, catalog disable) | Frontend drops every per-slot `UnixStream` + listener_fd. Backend's blocking `recvmsg` returns 0. The `on_proc_exit` hook from §2.1 unlinks the per-slot path. | Backend runs `reset_per_session_state()` one last time, exits its loop, exits process; postmaster reaps. |
| **Frontend dies unexpectedly**  | Frontend panic, kill -9, crash          | Kernel closes the frontend endpoint. Backend sees `recvmsg → 0`. PG's `proc_exit` machinery still runs the `on_proc_exit` unlink on a panic / `ereport(FATAL)`; only `kill -9` / `SIGKILL` leaves the path behind (handled by the next start's defensive `unlink`). | Backend exits the same way. `PostmasterIsAlive()` + `PR_SET_PDEATHSIG` are the belt-and-braces. |
| **Backend dies unexpectedly**    | Backend crash, kill -9, OOM kill         | Kernel closes the backend endpoint. Frontend's next `sendmsg` returns `EPIPE` (no signal thanks to the mask). | Frontend marks the slot faulted and `RegisterDynamicBackgroundWorker`s a replacement; the new bgworker `connect()`s on the same still-bound `listener_fd` (path is unchanged). |

Notes:

- **In-flight handoffs at backend crash.** If `sendmsg` from the
  frontend already succeeded but the backend died before `recvmsg`
  returned (or after, but before doing anything with the fd), the
  passed fd is closed when the backend process exits and the client
  sees a TCP reset. The transport doesn't know — `handle.handoff(fd)`
  returned `Ok`. This matches default PG's `fork`+startup-crash
  semantics: the postmaster has no synchronous confirmation either.
  Adding a backend-side ack per handoff would buy reliability at the
  cost of a round-trip per connection; we accept the asymmetry.
- **No abstract-namespace UDS.** Linux's abstract namespace
  (path starting with `\0`) would skip the unlink dance, but it's
  Linux-only and doesn't compose with `chmod` for file-permission
  gating. The filesystem path + `chmod 0600` + `SO_PEERCRED` is
  portable across the Unixes we target.
- **Why not `socketpair()`.** Tempting (no path, no permissions, no
  race), but the backend bgworker is forked by **postmaster**, not
  the frontend. The frontend can't pre-position one half of a
  socketpair into a process postmaster will fork later. A named UDS
  both sides look up by path is the only portable option.

### 2.3 Per-connection flow

```
Client                  Transport (frontend)            Backend slot (bgworker)
══════                  ══════════════════════            ═══════════════════════

TCP connect ──────────► accept() → tcp_sock
                        │
                        │ slot = pool.pick();            ───►
                        │ sendmsg(slot.unix_ctrl,
                        │   SCM_RIGHTS(tcp_sock),
                        │   hints = HandoffHints{...})   ───►  recvmsg → (fd, hints)
                        │ close(tcp_sock) locally              │
                        ▼                                      │ slot runner builds WireCtx;
                handoff returns                                │ hands fd to W::run
                                                               │
                                                               │ -- wire layer (backend-wire.md):
SSLRequest / Startup / Cancel ─────────────────────────────►   │ pgwire crate parses 8-byte probe
                                                               │   ← our dispatcher decides:
       ◄────────────────────────────────────────── 'S' or 'N'  │     SSLRequest → tokio_openssl::accept
                                                               │     GSSEncRequest → 'N' (deferred)
                                                               │     StartupV3 → continue
                                                               │     CancelRequest → v0: log + close fd
                                                               │                       (cancel routing deferred;
                                                               │                        see deferred/cancel-routing.md)
                                                               │
TLS ClientHello (if SSL) ──────────────────────────────────►   │ rust-openssl handshake on fd;
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
                                                                slot returns to pool
```

After `handle.handoff(fd).await` returns in the frontend, the frontend
has no further role for this connection. All FE/BE I/O is between the
backend's wire layer and the client over the inherited fd, using
`pgwire`-parsed messages and (for TLS) `tokio_openssl`'s `SslStream`.

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
| Connect → first query           | ~1 ms (fork + setup) | ~60 µs (pool checkout + fd-pass + startup) |
| `SELECT 1` round-trip           | ~30–50 µs  | ~40–60 µs (~10 µs fd-pass-related overhead, amortised after first query) |
| TLS handshake                   | ~1–5 ms (RSA dominates) | identical (same OpenSSL code, or rustls of comparable performance) |
| Per-query TLS data-path         | ~5–20% throughput hit | identical (TLS work is intra-backend) |
| Concurrent connections          | one fork per       | one pool-slot checkout per                    |

Effectively the same as default PG for steady-state queries, with a
*better* connection-establishment story (no fork). The framework's
overhead disappears into the noise of any non-trivial query.

This is the design ceiling. Going faster than this within `pg_transport`'s
bgworker model would mean letting backend processes do their own `accept()`
on a shared listening socket (so the frontend is bypassed in the data
path). We considered and **rejected** that approach — see
[Rejected alternatives in roadmap.md](roadmap.md). The single-frontend /
pre-spawned-backend-pool / `SCM_RIGHTS`-handoff model is the design.

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
- **OS portability.** `SCM_RIGHTS` works on every Unix; Windows needs
  `DuplicateHandle` + named pipes. Phase 6+ Linux/BSD/macOS only.

(BE-side concerns — wire-layer TLS choice, auth model, cancel
routing, per-handoff reset detail — live in [backend-handoff.md §5](backend-handoff.md#5-per-handoff-state-reset)
and [backend-wire.md](backend-wire.md).)

---

## 6. What this resolves vs the deferred shm_mq path

Several open questions in earlier drafts of the design were really
"how does this work for fd-based transports?". The handoff path
resolves them inside this doc and its BE companions:

| Earlier open question                       | Resolution under handoff                                                                  |
| ------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Auth handshake location                     | Entirely in the backend wire layer; details in [backend-wire.md §4](backend-wire.md).      |
| TLS termination location                    | Entirely in the backend wire layer (rust-openssl); details in [backend-wire.md §5](backend-wire.md). |
| SCM_RIGHTS vs no-SCM_RIGHTS for fd transfer | SCM_RIGHTS, per-slot Unix control socket.                                                 |
| Per-connection backend pinning              | Implied by the handoff itself — slot is pinned to connection.                            |
| Cross-process wakeup for fast path          | Not needed — once the fd is handed off, all I/O is direct between client and backend.    |

Those questions reopen for the deferred shm_mq path; see
[backend-pool.md](deferred/backend-pool.md) and
[roadmap.md §4 — Deferred for v0](roadmap.md).

---

## See also

- [backend-handoff.md](backend-handoff.md) — BE slot runner (socket
  layer): fd receipt, slot lifecycle, per-handoff reset.
- [backend-wire.md](backend-wire.md) — BE wire layer: TLS, auth, FE/BE
  protocol via `pgwire` crate, SPI bridge, open questions.
- [api.md](api.md) — `HandoffHandle::handoff`.
- [transports.md](transports.md) — which transports use this path (in
  v0: all of them).
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for the
  shm_mq general path.
- [../background/pg_background.md](../background/pg_background.md) — the
  pg_background mechanism the deferred shm_mq path lifts.
