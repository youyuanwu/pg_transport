# Handoff — blind fd-pass for kernel-socket transports

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [architecture.md](architecture.md)

This doc describes `pg_transport`'s **v0 path**: a transport that holds an
`OwnedFd` for a kernel socket (TCP, UDS, future io_uring-backed sockets) and
whose wire protocol is FE/BE v3 hands the fd over to an executor bgworker
and steps out entirely. The executor takes ownership and runs essentially
`PostgresMain` on the inherited fd.

In v0 this is the **only** path. A second path — the shm_mq-based
*general path* for transports that need plaintext inspection, run
non-FE/BE wires (HTTP/2 + SQL, custom binary), or have no shareable
kernel fd (QUIC, DPDK) — is sketched in [executor-pool.md](executor-pool.md)
but **deferred**.

The model is conceptually identical to default PostgreSQL: postmaster
`accept()`s, hands the fd to a child via `fork`, and the child runs the
backend. We replace `fork` with `SCM_RIGHTS` over a Unix control socket,
and we replace "freshly forked backend" with "pre-spawned bgworker pool
member". Everything else — `ProcessStartupPacket`, TLS handshake,
`ClientAuthentication`, FE/BE message loop, cancel handling — is unchanged
PG code running in the executor.

---

## 1. When to use this path

In v0: **always**. Every supported transport (`tcp_handoff`, `uds_handoff`)
has a kernel fd and speaks FE/BE v3, so they all hand off.

The table below records which future transport scenarios fit this path
versus the deferred general path. "shm_mq" rows are reachable only once
that path lands:

| Transport scenario                                | Path             |
| ------------------------------------------------- | ---------------- |
| TCP + FE/BE v3 (no transport-side processing)     | **handoff** (v0) |
| UDS + FE/BE v3                                    | **handoff** (v0) |
| TCP + FE/BE v3 + TLS                              | **handoff** (v0; executor terminates TLS) |
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

Each executor-pool slot owns one cross-process channel, set up once at
slot startup:

| Channel                        | Type                     | Direction          | Used by      |
| ------------------------------ | ------------------------ | ------------------ | ------------ |
| Unix control socket            | `AF_UNIX` `SOCK_STREAM`  | dispatcher → exec  | handoff path |

(When the deferred general path lands, each slot will additionally
allocate a DSM segment containing `req_q` / `resp_q` `shm_mq`s. See
[executor-pool.md](executor-pool.md).)

### 2.1 Slot setup (one-time, per slot)

We follow PG's own client-UDS playbook (`src/backend/libpq/pqcomm.c`)
for where to put the socket and how to clean it up:

- **Directory** — `pg_transport.socket_directory` GUC (string, `SUSET`).
  Defaults to the first entry of `unix_socket_directories` (and `/tmp`
  if that is empty). Set it to `/var/run/postgresql` on distros that
  put PG's client UDS there, so the framework's internal sockets share
  the same backup / SELinux / AppArmor story.
- **Filename** — `.s.PG_TRANSPORT.<dispatcher_pid>.<slot_id>`. Leading
  `.s` mirrors PG's `.s.PGSQL.<port>`.
- **Permissions** — always `0600`, owned by the PG cluster user. No
  `socket_permissions` / `socket_group` GUCs: this is a framework-internal
  channel, not a client surface, and `SO_PEERCRED` is the real enforcement.
- **Cleanup** — each slot registers `on_proc_exit(unlink_slot_socket,
  slot_idx)` at setup time, the same mechanism `pqcomm.c::StreamDoUnlink`
  uses. The unlink fires on clean exit *and* on `ereport(FATAL)` /
  `proc_exit`-from-signal, so we don't need bespoke `Drop` plumbing
  around the listener.
- **No `Lock_AF_UNIX` lock file** — the dispatcher's PID is part of
  the filename, so two dispatchers cannot collide. PID reuse after the
  dispatcher dies is handled by the defensive `unlink` before `bind`.

The dispatcher creates the listener **before** registering the bgworker
(otherwise the bgworker's first `connect()` races and gets `ECONNREFUSED`):

```
Dispatcher (slot init, per slot, at pool startup)
─────────────────────────────────────────────────
1. dir  = GetConfigOption("pg_transport.socket_directory")
        ?: first(unix_socket_directories)
        ?: "/tmp"
   path = format!("{dir}/.s.PG_TRANSPORT.{dispatcher_pid}.{slot_id}")
2. unlink(path)                          // defensive, in case of stale file
3. listener_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)
4. bind(listener_fd, path); chmod(path, 0600)
   // chmod is explicit — bind's mode arg isn't honoured on all platforms.
5. on_proc_exit(unlink_slot_socket, slot_idx_as_datum)
   // PG's exit machinery does the unlink for us, including on FATAL.
6. listen(listener_fd, 1)                // backlog=1; only one client expected
7. RegisterDynamicBackgroundWorker(...)  // main_arg = (slot_id, path)

Executor bgworker (main)
────────────────────────
8. (slot_id, path) = read bgw_main_arg
9. conn_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)
10. connect(conn_fd, path)               // retry a few times with short
                                         // backoff to tolerate startup races

Dispatcher (continues)
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
    // dispatcher exit; that is the only unlink point.

Process-wide
────────────
14. Mask SIGPIPE on the dispatcher (sigaction SIG_IGN or pthread_sigmask).
    We want EPIPE returned from sendmsg, not a signal.
```

Steady state: the channel is one-way. The dispatcher does
`sendmsg(SCM_RIGHTS, hints_block)` per handoff; the executor's main loop
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
| **Orderly shutdown**              | `ShutdownToken::cancel()` (SIGTERM, postmaster death, catalog disable) | Dispatcher drops every per-slot `UnixStream` + listener_fd. Executor's blocking `recvmsg` returns 0. The `on_proc_exit` hook from §2.1 unlinks the per-slot path. | Executor runs `reset_per_session_state()` one last time, exits its loop, exits process; postmaster reaps. |
| **Dispatcher dies unexpectedly**  | Dispatcher panic, kill -9, crash          | Kernel closes the dispatcher endpoint. Executor sees `recvmsg → 0`. PG's `proc_exit` machinery still runs the `on_proc_exit` unlink on a panic / `ereport(FATAL)`; only `kill -9` / `SIGKILL` leaves the path behind (handled by the next start's defensive `unlink`). | Executor exits the same way. `PostmasterIsAlive()` + `PR_SET_PDEATHSIG` are the belt-and-braces. |
| **Executor dies unexpectedly**    | Executor crash, kill -9, OOM kill         | Kernel closes the executor endpoint. Dispatcher's next `sendmsg` returns `EPIPE` (no signal thanks to the mask). | Dispatcher marks the slot faulted and `RegisterDynamicBackgroundWorker`s a replacement; the new bgworker `connect()`s on the same still-bound `listener_fd` (path is unchanged). |

Notes:

- **In-flight handoffs at executor crash.** If `sendmsg` from the
  dispatcher already succeeded but the executor died before `recvmsg`
  returned (or after, but before doing anything with the fd), the
  passed fd is closed when the executor process exits and the client
  sees a TCP reset. The transport doesn't know — `handle.handoff(fd)`
  returned `Ok`. This matches default PG's `fork`+startup-crash
  semantics: the postmaster has no synchronous confirmation either.
  Adding an executor-side ack per handoff would buy reliability at the
  cost of a round-trip per connection; we accept the asymmetry.
- **No abstract-namespace UDS.** Linux's abstract namespace
  (path starting with `\0`) would skip the unlink dance, but it's
  Linux-only and doesn't compose with `chmod` for file-permission
  gating. The filesystem path + `chmod 0600` + `SO_PEERCRED` is
  portable across the Unixes we target.
- **Why not `socketpair()`.** Tempting (no path, no permissions, no
  race), but the executor bgworker is forked by **postmaster**, not
  the dispatcher. The dispatcher can't pre-position one half of a
  socketpair into a process postmaster will fork later. A named UDS
  both sides look up by path is the only portable option.

### 2.3 Per-connection flow

```
Client                  Transport (dispatcher)            Executor bgworker
══════                  ══════════════════════            ═══════════════════

TCP connect ──────────► accept() → tcp_sock
                        │
                        │ slot = pool.pick();            ───►
                        │ sendmsg(slot.unix_ctrl,
                        │   SCM_RIGHTS(tcp_sock),
                        │   payload = none/minimal)      ───►  recvmsg → fd
                        │ close(tcp_sock) locally              │
                        ▼                                      │ MyProcPort.sock = fd
                handoff returns                            │
                                                               │ ProcessStartupPacket()
SSLRequest/Startup/Cancel ─────────────────────────────────►   │   ← reads 8-byte probe
                                                               │   ← dispatches:
       ◄────────────────────────────────────────── 'S' or 'N'  │     SSLRequest → secure_open_server
                                                               │     GSSEncRequest → similar
                                                               │     StartupV3 → continue plaintext
                                                               │     CancelRequest → cancel-map lookup, exit
                                                               │
TLS ClientHello (if SSL) ──────────────────────────────────►   │ TLS handshake (PG OpenSSL OR
                                                               │   rustls sidecar — see §4)
                                                               │
StartupMessage (over TLS) ─────────────────────────────────►   │ ProcessStartupPacket continues
                                                               │
       ◄────────────────────────────────────────── Auth req    │ ClientAuthentication
                                                               │   ← consults pg_hba.conf
                                                               │
       ◄────────────────────────────────────────── ReadyForQuery
                                                               │
                                            FE/BE loop on the inherited fd
                                                               │
                                            on EOF / Terminate: close fd,
                                                                reset per-session state,
                                                                slot returns to pool
```

After `handle.handoff(fd).await` returns in the dispatcher, the dispatcher has no
further role for this connection. All FE/BE I/O is between the executor
and the client over the inherited fd, using PG's normal `pq_getbyte` /
`pq_putmessage`.

---

## 3. Executor side: what runs on the fd

The executor's per-handoff handler reuses PG's own protocol-startup code
wholesale. We are not reimplementing FE/BE — we are calling the existing
PG functions with `MyProcPort` set up around the inherited fd.

```rust
// crates/executor/src/handoff.rs (sketch; pgrx + raw pg_sys)

unsafe fn handle_handoff(fd: OwnedFd, hints: HandoffHints) -> anyhow::Result<()> {
    // 1. Build a fresh MyProcPort for this connection.
    let mut port = init_port_from_fd(fd, &hints);
    pg_sys::MyProcPort = &mut port;

    // 2. PG's own startup-packet processor handles ALL protocol probes:
    //    SSLRequest, GSSENCRequest, StartupMessageV3, CancelRequest.
    //    For SSLRequest it calls secure_open_server directly.
    //    For CancelRequest it does the cancel lookup and returns
    //    a status that causes us to exit early.
    if pg_sys::ProcessStartupPacket(&mut port, false, false) != STATUS_OK {
        // CancelRequest handled, or fatal protocol error.
        return Ok(());
    }

    // 3. Authentication. Uses pg_hba.conf as normal.
    pg_sys::ClientAuthentication(&mut port);

    // 4. PostgresMain-equivalent loop until the client disconnects.
    run_backend_loop(&mut port)?;

    // 5. Reset per-session state so the slot is ready for the next handoff.
    reset_per_session_state();
    Ok(())
}
```

`HandoffHints` is a tiny opaque struct passed in the `sendmsg` payload
alongside the fd. Currently just one field:

```rust
#[repr(C)]
pub struct HandoffHints {
    /// Whether the listener allowed TLS. The executor honours this when
    /// deciding to reply 'S' to SSLRequest. Other PG-level TLS config
    /// (cert path, ciphers, min version) comes from PG GUCs, not here.
    pub tls_allowed: bool,
}
```

We deliberately keep this tiny. Per-listener cert variation is not
supported in phase 6; everyone shares the cluster's `ssl_cert_file`.

### 3.1 Per-session state reset

Between handoffs on the same slot, we must purge anything that could
leak from session to session:

| State                             | How we reset                                                          |
| --------------------------------- | --------------------------------------------------------------------- |
| GUCs touched by `SET LOCAL` / `SET` | `ResetAllOptions()`                                                  |
| Temp tables                       | drop session's temp namespace                                          |
| Prepared statements               | `DropAllPreparedStatements()`                                          |
| Cursors                           | `PortalDrop` over the session's portals                                |
| Per-session memory contexts       | `MemoryContextDelete(MessageContext)`; reinit                          |
| `MyProcPort`                      | free `peer_dn`, TLS state, then null `MyProcPort`                      |
| Backend ID                        | unchanged (slot reuses its bgworker `PGPROC` slot across handoffs)     |

This is roughly `DISCARD ALL` plus a few extras. PG already exposes most
of these as discrete calls; we wrap them in `reset_per_session_state()`.

### 3.2 PostgresMain-equivalent

We don't call `PostgresMain()` directly because it has `proc_exit` calls
that would terminate the bgworker after one connection. Instead we
extract its inner loop into a callable function:

```rust
fn run_backend_loop(port: &mut Port) -> anyhow::Result<()> {
    loop {
        let msg_type = pg_sys::pq_getbyte();
        if msg_type < 0 { return Ok(()); }    // EOF / disconnect
        match msg_type as u8 {
            b'Q' => exec_simple_query(port),
            b'P' | b'B' | b'E' | b'D' | b'C' | b'S' | b'H' => exec_extended_protocol(msg_type as u8),
            b'X' => return Ok(()),             // Terminate
            b'F' => exec_function_call(port),
            b'c' | b'd' | b'f' => exec_copy_data(msg_type as u8),
            _    => bail!("unsupported FE message {msg_type}"),
        }
    }
}
```

For phase 3 the body delegates to the underlying PG functions
(`exec_simple_query`, `exec_parse_message`, …) which are all already
linkable. Phase 4 (bench harness) tells us whether anything's missing.

---

## 4. TLS handling

TLS is **entirely the executor's concern**. The dispatcher never sees
plaintext; it never sees ciphertext beyond the kernel fd it forwarded.
The transport's `options jsonb` does not carry cert paths.

Two implementations the executor can be configured to use, by GUC:

| `pg_transport.tls_impl` | TLS code path                          | Notes                                 |
| ----------------------- | -------------------------------------- | ------------------------------------- |
| `"openssl"` *(default)* | PG's `secure_open_server` (`be-secure-openssl.c`) | Uses cluster's `ssl_*` GUCs; mTLS via `pg_hba.conf cert`; `pg_stat_ssl` populated |
| `"rustls"`              | rustls in a sidecar OS thread inside the executor | Uses `pg_transport.rustls_cert` / `pg_transport.rustls_key`; mTLS via rustls's `WebPkiClientVerifier`; metrics surfaced via `pg_transport.list_v2()` |

Both run **inside the executor process**, so TLS CPU work parallelises
across slots the same way it does in default PG (one TLS handshake per
backend at a time). Neither bottlenecks on the dispatcher's tokio thread.

The rustls case uses a sync OS thread per connection that drives rustls
state machines against the inherited TCP fd, exposes a plaintext UDS
fd to PG via `MyProcPort.sock`, and exits when the connection closes.
The sidecar thread strictly does not touch PG state (C-2). This is the
same pattern that would have been needed in the dispatcher to run rustls
there; relocating it to the executor preserves PG's parallel scaling.

See [open Q1 in roadmap.md](roadmap.md) — defaulting to OpenSSL vs rustls.

### What the transport does for TLS

Nothing. The transport sets `hints.tls_allowed = (catalog row says TLS
is enabled)` and that's it. Cert paths, ciphers, protocol versions are
all PG-side configuration.

### Per-listener TLS variation

If you want listener A to allow TLS but listener B to forbid it, that's
expressed entirely via `hints.tls_allowed`. The executor reads the bit
and replies 'S' or 'N' accordingly. Per-listener *cert* variation is not
supported in phase 6 (the executor uses the cluster's `ssl_*` GUCs);
phase 7+ may add per-handoff cert selection if needed.

---

## 5. CancelRequest handling

PG's `ProcessStartupPacket` handles CancelRequest natively — it looks up
the target backend by `(pid, secret_key)` in PG's shared cancel-request
state and sends SIGINT. We just need to make this work across our
multi-process pool model:

1. When an executor accepts a handoff and completes authentication, it
   publishes its `(MyProc->backendId, BackendKey)` into a shared-memory
   map keyed by `(pid, secret_key)`. The map lives in our pool's main
   DSM segment, alongside the per-slot data.
2. The StartupMessage handler in the executor advertises `(MyProc->pid,
   secret_key)` to the client, same as default PG.
3. When a *different* executor receives a CancelRequest fd, its
   `ProcessStartupPacket` runs PG's cancel logic, which consults the
   shared map (we patch PG's cancel-lookup to also consult our map) and
   signals the target executor.

Implementation detail: PG already provides `SendCancelRequest` and a
backend-lookup hook; we register our pool's map as an additional source
for that hook. If the hook isn't usable for our case, we fall back to a
plain SIGINT to the target executor's PID (which we read from our map),
since the target's signal handler already does the right thing.

---

## 6. Comparison with default PG

| Stage                           | Default PG                          | `pg_transport` handoff path             |
| ------------------------------- | ----------------------------------- | --------------------------------------- |
| Listen on port                  | postmaster's `ServerLoop`           | dispatcher's tokio accept loop          |
| Accept a connection             | postmaster                          | dispatcher                              |
| Hand socket to child            | `fork()`; child inherits fd         | `sendmsg(SCM_RIGHTS)` to slot's bgworker |
| Per-child startup cost          | full fork (~1 ms on Linux)          | fd-pass (~10 µs); bgworker pre-spawned  |
| Protocol probe (SSLRequest etc) | backend reads in `ProcessStartupPacket` | executor reads in `ProcessStartupPacket` |
| TLS handshake                   | `secure_open_server` in backend     | `secure_open_server` *or* rustls sidecar in executor |
| `ClientAuthentication`          | backend                             | executor                                |
| `PostgresMain` loop             | backend                             | executor (PostgresMain-equivalent)      |
| Lifecycle after disconnect      | backend exits; postmaster reaps     | executor resets per-session state, slot returns to pool |
| Cancel routing                  | postmaster looks up PID, signals    | executor consults shared cancel map, signals target executor |

Structurally identical. The two differences are:

1. **`fork()` → `SCM_RIGHTS`**: pre-spawned children, ~100× cheaper per
   connection, but those children are recycled instead of exiting.
2. **One global listener (postmaster) → many configurable listeners
   (our transports)**: we can run TCP on a custom port, UDS at a custom
   path, future io_uring-backed listeners, all simultaneously, all
   dispatching to the same executor pool.

---

## 7. Performance

| Operation                       | Default PG | `pg_transport` handoff                          |
| ------------------------------- | ---------- | ----------------------------------------------- |
| Connect → first query           | ~1 ms (fork + setup) | ~60 µs (pool checkout + fd-pass + startup) |
| `SELECT 1` round-trip           | ~30–50 µs  | ~40–60 µs (~10 µs fd-pass-related overhead, amortised after first query) |
| TLS handshake                   | ~1–5 ms (RSA dominates) | identical (same OpenSSL code, or rustls of comparable performance) |
| Per-query TLS data-path         | ~5–20% throughput hit | identical (TLS work is intra-executor) |
| Concurrent connections          | one fork per       | one pool-slot checkout per                    |

Effectively the same as default PG for steady-state queries, with a
*better* connection-establishment story (no fork). The framework's
overhead disappears into the noise of any non-trivial query.

This is the design ceiling. Going faster than this within `pg_transport`'s
bgworker model would mean letting executor processes do their own `accept()`
on a shared listening socket (so the dispatcher is bypassed in the data
path). We considered and **rejected** that approach — see
[Rejected alternatives in roadmap.md](roadmap.md). The single-dispatcher /
pre-spawned-executor-pool / `SCM_RIGHTS`-handoff model is the design.

---

## 8. Limitations and known gaps

- **No transport-side message inspection.** Once handoff is done the
  transport doesn't see protocol bytes. If you need to log, filter, or
  rewrite FE/BE traffic, you need the deferred shm_mq path — see
  [executor-pool.md](executor-pool.md).
- **No pre-auth filtering by FE/BE content.** The transport can still
  filter by client IP, TLS SNI, or whatever it sees pre-handoff, but
  nothing in StartupMessage. (Auth happens entirely in the executor.)
- **Per-listener cert variation is not supported in phase 6.** All
  listeners using TLS share the cluster's `ssl_cert_file`.
- **Slot is pinned for the connection's lifetime.** No multiplexing
  multiple handoff connections onto one slot. (Same as default PG's
  fork model, deliberately.)
- **Cancel-map dependency.** We need a small shared-memory map for
  cross-pool cancel routing; this is extra mechanism over default PG's
  built-in cancel state.
- **OS portability.** `SCM_RIGHTS` works on every Unix; Windows needs
  `DuplicateHandle` + named pipes. Phase 6+ Linux/BSD/macOS only.

---

## 9. What this resolves vs the deferred shm_mq path

Several open questions in earlier drafts of the design were really
"how does this work for fd-based transports?". The handoff path
resolves them inside this doc:

| Earlier open question                       | Resolution under handoff                                     |
| ------------------------------------------- | ------------------------------------------------------------ |
| Auth handshake location                     | Entirely in the executor, via PG's `ClientAuthentication`.   |
| TLS termination location                    | Entirely in the executor, OpenSSL or rustls sidecar.         |
| SCM_RIGHTS vs no-SCM_RIGHTS for fd transfer | SCM_RIGHTS, per-slot Unix control socket.                    |
| Per-connection executor pinning             | Implied by the handoff itself — slot is pinned to connection.|
| Cross-process wakeup for fast path          | Not needed — once the fd is handed off, all I/O is direct.   |

Those questions reopen for the deferred shm_mq path; see
[executor-pool.md](executor-pool.md) and
[roadmap.md §4 — Deferred for v0](roadmap.md).

---

## See also

- [api.md](api.md) — `HandoffHandle::handoff`.
- [transports.md](transports.md) — which transports use this path (in
  v0: all of them).
- [executor-pool.md](executor-pool.md) — *deferred* design for the
  shm_mq general path.
- [../background/pg_background.md](../background/pg_background.md) — the
  pg_background mechanism the deferred shm_mq path lifts.
