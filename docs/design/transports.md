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
always FE/BE v3 via our [pgwire-v3 wire](backend-wire.md) impl
(built on the `pgwire` crate, with rust-openssl TLS and
`hba_getauthmethod`-driven auth), but the transport doesn't claim or
care. Forward-compatible: a future HTTP/2 + SQL transport on TCP would
be a `SessionTransport` named `tcp_http2_sql`, not in conflict with
`tcp_handoff`.

## 1. Concrete transports

v0 ships exactly **one** transport (`tcp_handoff`). No Cargo features
gate it; it's compiled into `core` directly (see
[workspace.md §2](workspace.md#2-cargo-features--deliberately-minimal)).

| Transport         | Trait               | Bundles                                                       | Source                                                                                  |
| ----------------- | ------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `tcp_handoff`      | `HandoffTransport`  | TCP accept + blind `SCM_RIGHTS` fd-pass                       | [crates/core/src/handoff/tcp.rs](../../crates/core/src/handoff/tcp.rs) (~150 LOC, mostly comments) |

TLS, auth, and FE/BE all happen in the backend (see
[frontend-handoff.md](frontend-handoff.md)); the transport never
reads or writes a byte of client data.

Deferred (not built in v0): `uds_handoff` (Unix-domain accept,
otherwise identical shape to `tcp_handoff`), `http2_sql` (a
`SessionTransport` on TCP), and every transport in
[deferred/future-transports.md](deferred/future-transports.md).
Adding a second transport is what reopens the Cargo-features
question (see [roadmap.md §2 Q5](roadmap.md#2-open-questions)).

## 2. Shared helpers

v0 has **no extracted accept-loop helper**. The `select!`-on-shutdown
+ `accept()`-and-handoff loop is inlined directly in
[handoff/tcp.rs](../../crates/core/src/handoff/tcp.rs) because
`tcp_handoff` is its only consumer; ~30 lines of straightforward
tokio. When a second handoff transport lands (deferred `uds_handoff`,
`iouring_handoff`, abstract-namespace UDS, `sd_listen_fds`-fed
listener, …), the natural refactor is to extract a generic
`run_accept_loop<S: Stream<Item = io::Result<OwnedFd>>>` into
`crates/core/src/handoff/listener.rs` at that point. Doing it
speculatively now buys nothing while there's exactly one transport.

The wire layer ([crates/core/src/wire/](../../crates/core/src/wire/),
built on the sunng87 [`pgwire`](https://github.com/sunng87/pgwire)
crate) is the other piece a future handoff transport will reuse; that
one *is* shared today (the slot runner picks `PgwireV3` at compile
time), but it's not transport-facing — transports never touch wire
bytes.

Helper crates the original design penciled in (`protocol-http2`,
`tls-rustls`, `polled-bridge`) are all tied to the deferred shm_mq
/ `SessionTransport` path and are not built in v0; see
[deferred/backend-pool.md](deferred/backend-pool.md) and
[deferred/future-transports.md](deferred/future-transports.md).

## 3. FE/BE specifics

For every v0 transport, the transport doesn't see FE/BE at all — the
backend's wire layer ([backend-wire.md](backend-wire.md)) handles it
via the [`pgwire`](https://github.com/sunng87/pgwire) crate, with TLS
in the backend (rust-openssl) and auth via PG's `pg_hba.conf` lookup
helpers. The transport doesn't claim or care; the wire layer is
compiled in at build time.
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
- [frontend-handoff.md](frontend-handoff.md) — what the backend does after `handle.handoff(fd)`.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for
  `SessionTransport` and the shm_mq general path.
- [../future-transports.md](deferred/future-transports.md) — deferred transport
  ideas.
