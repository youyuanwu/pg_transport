# Backend — running PG on the handed-off fd

> Parent: [README.md](README.md)
> Sibling: [handoff.md](handoff.md) · [api.md](api.md)

This doc describes the **BE (backend) side** of `pg_transport`'s v0
fd-pass path. Its FE (transport / frontend / IPC) companion is
[handoff.md](handoff.md), which covers when this path applies, how the
per-slot control socket is set up and torn down, and the `sendmsg(SCM_RIGHTS)`
mechanics that bring an fd into the backend in the first place.

This doc picks up at the moment the backend's main loop pulls one
`(fd, hints)` off its per-slot control socket. It is **all** PG-side
work — `ProcessStartupPacket`, TLS, `ClientAuthentication`,
`PostgresMain`-equivalent, cancel routing, per-session cleanup — done
inside a recycled bgworker rather than a freshly forked backend.

The backend never speaks the IPC protocol directly; it just receives
`OwnedFd`s and the small `HandoffHints` block accompanying each one. The
deferred shm_mq general path's backend-side machinery lives separately
in [backend-pool.md](deferred/backend-pool.md).

---

## 1. Per-handoff handler

The backend's per-handoff handler reuses PG's own protocol-startup code
wholesale. We are not reimplementing FE/BE — we are calling the existing
PG functions with `MyProcPort` set up around the inherited fd.

```rust
// crates/backend/src/handoff.rs (sketch; pgrx + raw pg_sys)

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
    /// Whether the listener allowed TLS. The backend honours this when
    /// deciding to reply 'S' to SSLRequest. Other PG-level TLS config
    /// (cert path, ciphers, min version) comes from PG GUCs, not here.
    pub tls_allowed: bool,
}
```

We deliberately keep this tiny. Per-listener cert variation is not
supported in phase 6; everyone shares the cluster's `ssl_cert_file`.

---

## 2. Per-session state reset

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

---

## 3. PostgresMain-equivalent

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

TLS is **entirely the backend's concern**. The frontend never sees
plaintext; it never sees ciphertext beyond the kernel fd it forwarded.
The transport's `options jsonb` does not carry cert paths.

Two implementations the backend can be configured to use, by GUC:

| `pg_transport.tls_impl` | TLS code path                          | Notes                                 |
| ----------------------- | -------------------------------------- | ------------------------------------- |
| `"openssl"` *(default)* | PG's `secure_open_server` (`be-secure-openssl.c`) | Uses cluster's `ssl_*` GUCs; mTLS via `pg_hba.conf cert`; `pg_stat_ssl` populated |
| `"rustls"`              | rustls in a sidecar OS thread inside the backend | Uses `pg_transport.rustls_cert` / `pg_transport.rustls_key`; mTLS via rustls's `WebPkiClientVerifier`; metrics surfaced via `pg_transport.list_v2()` |

Both run **inside the backend process**, so TLS CPU work parallelises
across slots the same way it does in default PG (one TLS handshake per
backend at a time). Neither bottlenecks on the frontend's tokio thread.

The rustls case uses a sync OS thread per connection that drives rustls
state machines against the inherited TCP fd, exposes a plaintext UDS
fd to PG via `MyProcPort.sock`, and exits when the connection closes.
The sidecar thread strictly does not touch PG state (C-2). This is the
same pattern that would have been needed in the frontend to run rustls
there; relocating it to the backend preserves PG's parallel scaling.

See [open Q1 in roadmap.md](roadmap.md) — defaulting to OpenSSL vs rustls.

### What the transport does for TLS

Nothing. The transport sets `hints.tls_allowed = (catalog row says TLS
is enabled)` and that's it. Cert paths, ciphers, protocol versions are
all PG-side configuration.

### Per-listener TLS variation

If you want listener A to allow TLS but listener B to forbid it, that's
expressed entirely via `hints.tls_allowed`. The backend reads the bit
and replies 'S' or 'N' accordingly. Per-listener *cert* variation is not
supported in phase 6 (the backend uses the cluster's `ssl_*` GUCs);
phase 7+ may add per-handoff cert selection if needed.

---

## 5. CancelRequest handling

PG's `ProcessStartupPacket` handles CancelRequest natively — it looks up
the target backend by `(pid, secret_key)` in PG's shared cancel-request
state and sends SIGINT. We just need to make this work across our
multi-process pool model:

1. When a backend accepts a handoff and completes authentication, it
   publishes its `(MyProc->backendId, BackendKey)` into a shared-memory
   map keyed by `(pid, secret_key)`. The map lives in our pool's main
   DSM segment, alongside the per-slot data.
2. The StartupMessage handler in the backend advertises `(MyProc->pid,
   secret_key)` to the client, same as default PG.
3. When a *different* backend receives a CancelRequest fd, its
   `ProcessStartupPacket` runs PG's cancel logic, which consults the
   shared map (we patch PG's cancel-lookup to also consult our map) and
   signals the target backend.

Implementation detail: PG already provides `SendCancelRequest` and a
backend-lookup hook; we register our pool's map as an additional source
for that hook. If the hook isn't usable for our case, we fall back to a
plain SIGINT to the target backend's PID (which we read from our map),
since the target's signal handler already does the right thing.

---

## 6. Backend-side limitations

- **Per-listener cert variation is not supported in phase 6.** All
  listeners using TLS share the cluster's `ssl_cert_file`. Per-handoff
  cert selection (driven from `HandoffHints`) is a phase 7+ extension.
- **Cancel-map dependency.** We need a small shared-memory map for
  cross-pool cancel routing; this is extra mechanism over default PG's
  built-in cancel state.
- **`PostgresMain` is not called directly.** We extract the inner
  message loop into `run_backend_loop` (§3) because `PostgresMain` calls
  `proc_exit` which would terminate the bgworker after one connection.
  Any new FE message types added to PG must be added to the match arm
  in `run_backend_loop`.
- **Per-session state reset must keep up.** Every new piece of
  per-session backend state PG introduces is a potential cross-handoff
  leak. The reset table (§2) is exhaustive for today's PG but needs
  review on every PG major-version bump.

(FE/IPC-side limitations — no transport-side inspection, slot pinning,
OS portability of `SCM_RIGHTS` — live in [handoff.md §5](handoff.md).)

---

## See also

- [handoff.md](handoff.md) — the FE/IPC side: when this path applies,
  per-slot control socket setup/teardown, `SCM_RIGHTS` mechanics, the
  end-to-end per-connection sequence, performance ceiling, and the
  comparison with default PG's `fork`-based model.
- [api.md](api.md) — `HandoffHandle::handoff(fd)`, the only thing a
  transport calls.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for the
  backend side of the shm_mq general path.
- [../background/pg_background.md](../background/pg_background.md) — the
  pg_background mechanism the deferred shm_mq path lifts.
