# Per-transport notes (current scope)

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md)

Current-scope transports only. Deferred entries live in
[../future-transports.md](deferred/future-transports.md). In v0, every
transport is a `HandoffTransport`: it owns an accept loop on a kernel
socket and hands fds to the backend. The framework does not yet ship
or support a `SessionTransport` — see
[backend-pool.md](deferred/backend-pool.md) for the deferred design.

**Naming convention.** Transport names describe *the role the framework
plays with the fd* (handoff) on *what kind of socket* (tcp / uds /
future iouring / …). They deliberately do **not** name a wire protocol:
the bgworker decides what to speak on the handed-off fd. In v0 that's
always FE/BE v3 via PG's own `PostgresMain`-equivalent, but the
transport doesn't claim or care. Forward-compatible: a future
HTTP/2 + SQL transport on TCP would be a `SessionTransport` named
`tcp_http2_sql`, not in conflict with `tcp_handoff`.

## 1. Concrete transports

| Transport         | Trait               | Bundles                                                       | Helper crates used                       | Notes                                                  |
| ----------------- | ------------------- | ------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| `tcp_handoff`      | `HandoffTransport`  | TCP accept + blind `SCM_RIGHTS` fd-pass                       | (none in transport)                      | Baseline; the reference implementation. ~20 lines of code. TLS / auth / FE/BE all happen in the backend (see [handoff.md](handoff.md)). |
| `uds_handoff`      | `HandoffTransport`  | Unix-domain accept + blind `SCM_RIGHTS` fd-pass               | (none in transport)                      | Same shape as `tcp_handoff` over a different listener kind. |

Deferred (not built in v0): `http2_sql` and any other transport whose
wire is not FE/BE on a kernel socket would implement the deferred
`SessionTransport` trait and receive a `SessionHandle`. See
[backend-pool.md](deferred/backend-pool.md).

## 2. Helper crates the framework ships

These are plain Rust libraries — not framework plugins, no Cargo feature
of their own. Transports that want them depend on them directly.

| Helper                | Wraps                                  | Used by                                                         |
| --------------------- | -------------------------------------- | --------------------------------------------------------------- |
| `handoff-listener`    | (none — small std + tokio glue)        | **`tcp_handoff`, `uds_handoff`** in v0                          |
| `protocol-pgwire-v3`  | [`pgwire`](https://github.com/sunng87/pgwire) (sunng87) | *(none in v0)* — reserved for deferred shm_mq-path FE/BE transports |
| `protocol-http2`      | [`h2`](https://github.com/hyperium/h2) | *(none in v0)* — reserved for deferred `http2_sql`              |
| `tls-rustls`          | [`tokio-rustls`](https://github.com/rustls/tokio-rustls) | *(none in v0)* — reserved for deferred shm_mq-path transports that terminate TLS themselves; v0 handoff transports use backend-side TLS instead |
| `polled-bridge` (later) | data-plane-thread ↔ main-thread eventfd bridge | Future polled transports (DPDK / AF_XDP / RDMA-CM)        |

### 2.1 `handoff-listener`

The entire accept loop — `select!` on shutdown, drain the listener, hand
each fd to `HandoffHandle::handoff`, log accept errors — is the same in
every handoff transport. Rather than duplicate it per crate, we ship it
in one helper that takes a generic stream of incoming fds:

```rust
// crates/handoff-listener/src/lib.rs
use api::{HandoffHandle, ShutdownToken};
use futures::Stream;
use std::{io, os::fd::OwnedFd};

/// Drive an `OwnedFd` stream until shutdown, handing each fd to the
/// backend pool. Returns when the shutdown token fires or the stream
/// terminates.
pub async fn run_handoff_loop<S>(
    mut incoming: S,
    handle: HandoffHandle,
    shutdown: ShutdownToken,
) -> anyhow::Result<()>
where
    S: Stream<Item = io::Result<OwnedFd>> + Unpin,
{
    use futures::StreamExt;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            next = incoming.next() => match next {
                Some(Ok(fd)) => {
                    if let Err(e) = handle.handoff(fd).await {
                        tracing::warn!(?e, "handoff failed");
                    }
                }
                Some(Err(e)) => tracing::warn!(?e, "accept failed"),
                None         => break,                  // listener closed
            },
        }
    }
    Ok(())
}

/// Adapter: turn a `tokio::net::TcpListener` into an `OwnedFd` stream.
pub fn tcp_incoming(l: tokio::net::TcpListener)
    -> impl Stream<Item = io::Result<OwnedFd>>;

/// Adapter: turn a `tokio::net::UnixListener` into an `OwnedFd` stream.
pub fn uds_incoming(l: tokio::net::UnixListener)
    -> impl Stream<Item = io::Result<OwnedFd>>;
```

Why a stream-shaped helper rather than collapsing TCP + UDS into one
transport plugin:

- **The framework's stance** — "one transport plugin = one network entry
  point" — stays intact. `tcp_handoff` and `uds_handoff` remain
  separate, self-describing rows in `pg_transport.transports`.
- **Catalog readability**: `SELECT kind, bind_addr FROM pg_transport.transports`
  shows the listener kind directly; it isn't buried in `options jsonb`.
- **Granular Cargo features stay**: `--no-default-features --features tcp-handoff`
  builds without UDS support cleanly. A single combined transport
  would need internal sub-features.
- **Future fd-producing transports compose for free.** When
  `iouring_handoff` arrives (or an abstract-namespace UDS variant, or
  a systemd-`sd_listen_fds`-fed listener), it ships its own tiny
  transport crate that builds whatever listener it needs and hands
  the resulting stream to `run_handoff_loop`.
- **Per-loop concerns** — accept-error backoff, future per-listener
  metrics, `tracing` integration — live in one place and stay in sync.

The duplication that's *not* eliminated (per-kind config parsing,
per-kind listener construction) is genuinely different per kind and
wouldn't shrink under a combined transport either.

## 3. FE/BE specifics

For every v0 transport, the transport doesn't see FE/BE at all — the
backend's PG code does it natively (see [backend-handoff.md §1](backend-handoff.md)).
There is no transport-side state machine, no parameter-status emission,
no `Payload::Raw` pass-through; that surface only appears in the
deferred shm_mq path (see [backend-pool.md](deferred/backend-pool.md)).

For a worked example see [api.md §4](api.md).

## 4. Library choices vs. rust-postgres

[`tokio-postgres`](https://github.com/sfackler/rust-postgres) is used by
the **bench harness** as a client; it has no role inside the frontend
or backend (which never speak libpq to themselves). Sibling crates from
the rust-postgres family:

| Sub-crate                   | Used here?                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------- |
| `tokio-postgres`            | **Yes** — bench harness client                                                              |
| `postgres-types`            | Not in v0 — reserved for the deferred shm_mq path (e.g. `http2_sql` synthesising queries from typed inputs) |
| `postgres-protocol`         | Used transitively via `pgwire` (sunng87); not depended on directly                          |
| `postgres-protocol::sasl`   | No — implements client-side SCRAM; we need server-side (which `pgwire` provides)            |
| `postgres-native-tls/openssl` | No — client TLS connectors                                                                |

## See also

- [api.md](api.md) — the `HandoffTransport` trait every v0 entry above
  implements.
- [handoff.md](handoff.md) — what the backend does after `handle.handoff(fd)`.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for
  `SessionTransport` and the shm_mq general path.
- [../future-transports.md](deferred/future-transports.md) — deferred transport
  ideas.
