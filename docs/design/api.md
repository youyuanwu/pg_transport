# The framework's trait surface (minimal by design)

> Parent: [README.md](README.md)
> Sibling: [architecture.md](architecture.md) · [frontend-handoff.md](frontend-handoff.md)

Transport plugins are regular Rust crates in the workspace, linked into
the `core` extension at build time. v0 ships a single transport
(`tcp_handoff`) compiled in directly — there are no Cargo features in
v0 (see [workspace.md §2](workspace.md#2-cargo-features--deliberately-minimal)).
No dynamic loading, no FFI ABI, no `libloading`, no `unsafe extern "C"`.
Within that build model, the framework standardises **only what must
talk to PostgreSQL**:

| Surface                  | Defined by framework? | Notes                                                                          |
| ------------------------ | --------------------- | ------------------------------------------------------------------------------ |
| Transport lifecycle      | ✅ `HandoffTransport` | One method: `fn run(self, handle, shutdown) -> RunFuture`                     |
| Handle type              | ✅ `HandoffHandle`    | One method: `handoff(fd)`                                                      |
| Cancellation             | ✅ `ShutdownToken`    | Async-wakeable signal; transport must honour it                               |
| Catalog → instance       | ✅ Config + factory   | One factory per transport; registry has one map (a second is reserved for the deferred `SessionTransport`) |
| Byte I/O model           | ❌                    | Transport's choice (tokio, mio, future io_uring, raw threads, …)              |
| Wire protocol parsing    | ❌ (and not needed)   | The backend wire layer ([backend-wire.md](backend-wire.md)) parses FE/BE v3 itself via the `pgwire` crate |
| Connection-state model   | ❌ (and not needed)   | The backend wire layer owns the FE/BE message loop and SPI dispatch |
| TLS termination          | ❌ (and not needed)   | Backend wire-layer-side (rust-openssl); transport just sets `tls_allowed` in the handoff hints |
| Auth flow                | ❌ (and not needed)   | Backend wire layer: `hba_getauthmethod` lookup + our Rust method execution (SCRAM via `pgwire`) |
| Threading inside `run`   | ❌                    | Transport's choice (subject to C-2 — no PG access off the bgworker main thread) |

Most of the cells marked ❌ are "not needed" rather than "transport's
choice" because in v0 every transport is a `HandoffTransport`, which by
construction hands the wire to the backend before any protocol
processing. When `SessionTransport` lands (see §6 below), several of
those cells flip to "transport's choice".

---

## 1. The `HandoffTransport` trait

In v0 there is **one** transport trait. It is intentionally named
`HandoffTransport` (not just `Transport`) to leave room for the
`SessionTransport` sibling sketched in §6.

```rust
// crates/api/src/transport.rs
use std::future::Future;
use std::pin::Pin;
use crate::{Config, HandoffHandle, ShutdownToken};

/// Future returned by `HandoffTransport::run`. Boxed so the trait stays
/// object-safe (we need `Box<dyn HandoffTransport>` for the registry).
///
/// Not `Send`-bound: the frontend runs a tokio current-thread runtime
/// with `LocalSet`, so the future can hold `!Send` state across awaits
/// (pgrx handles, `Rc<…>` for shared per-instance config, etc.).
pub type RunFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static>>;

/// For transports that hand kernel sockets to the backend pool.
/// The handle exposes only `handoff` — no Payload, no Session,
/// no FrameStream visible to the transport.
pub trait HandoffTransport: 'static {
    /// Stable identifier used in catalog config (e.g. "tcp_handoff").
    fn name(&self) -> &'static str;

    /// Run until shutdown. Consumed because the framework spawns it once.
    /// Implementers typically write `Box::pin(async move { … })`.
    fn run(
        self: Box<Self>,
        handle: HandoffHandle,
        shutdown: ShutdownToken,
    ) -> RunFuture;
}

/// Transport factory. Each transport crate exports one `build` fn of
/// this type and registers it (see [workspace.md](workspace.md)).
pub type HandoffFactory = fn(cfg: &Config) -> anyhow::Result<Box<dyn HandoffTransport>>;
```

Why manual `Pin<Box<dyn Future + '_>>` and not `#[async_trait]`:

- **One async method, called once per transport per frontend lifetime.**
  The macro's ergonomics payoff (write many `async fn`s naturally)
  doesn't apply; one `Box::pin(async move { … })` at the impl site is
  the entire tax.
- **Same runtime shape.** `async_trait` desugars to exactly this. No
  extra cost, no saving.
- **No proc-macro dep in `crates/api/`.** Plugin authors pull in only
  `tokio` + `api` to build a transport.
- **Cleaner errors.** No injected `'async_trait` lifetime mangling
  diagnostics.
- **Cleaner migration path.** Once native `async fn in traits` composes
  with `dyn` on stable, the move is `s/RunFuture/impl Future<…>/` and
  dropping the `Box::pin` at impl sites. With `async_trait` we'd be
  removing the attribute *and* unbreaking the lifetimes it injected.

Why the name `HandoffTransport` rather than just `Transport`:

- The eventual `SessionTransport` sibling (deferred — see §6) will sit
  alongside it; both names already exist in the design vocabulary
  ([architecture.md](architecture.md), [backend-pool.md](deferred/backend-pool.md)).
- Keeps the v0 → v0.x rename-free: when the second trait lands, no
  existing transport changes shape.

---

## 2. `HandoffHandle` — the v0 handle type

```rust
// crates/api/src/handoff.rs
use std::os::fd::OwnedFd;

/// Handed to `HandoffTransport::run`. One method, deliberately.
#[derive(Clone)]
pub struct HandoffHandle { /* opaque */ }

impl HandoffHandle {
    /// Hand a kernel socket to a backend bgworker. The bgworker takes
    /// ownership and runs FE/BE v3 (including TLS and auth) on it
    /// directly. Returns when the client disconnects.
    ///
    /// Use this when the transport has an OwnedFd and the wire is FE/BE.
    /// See [frontend-handoff.md](frontend-handoff.md) for the SCM_RIGHTS mechanism, the
    /// per-slot control socket, and the backend-side TLS choices.
    pub async fn handoff(&self, sock: OwnedFd) -> anyhow::Result<()>;
}
```

Nothing else. No Payload, no SessionOpts, no FrameStream — the backend
wire layer reads `StartupMessage` from the fd itself, so the framework
doesn't need to pass database/user/auth metadata.

For the implementation of `handoff` (`SCM_RIGHTS`, per-slot control
socket, slot runner, wire layer), see [frontend-handoff.md](frontend-handoff.md),
[backend-handoff.md](backend-handoff.md), and
[backend-wire.md](backend-wire.md).

> **Resolved: simplest contract.** `handoff()` returns `Ok(())` on
> successful kernel-level handoff ("kernel accepted the fd-pass" —
> per Q21 this is `Ok(())` even if the backend dies before
> `recvmsg`; the client connection is then lost via TCP RST and the
> slot respawns on the next `sendmsg → EPIPE`), or `Err(Cancelled)`
> if the frontend shuts down while the future is pending. Pool
> exhaustion blocks on a semaphore until a slot frees; `EAGAIN`/
> `EINTR` retry transparently; slot death mid-`sendmsg` is invisible
> to the caller. Full reasoning in
> [roadmap.md §2 Q20](roadmap.md#2-open-questions) and
> [Q21](roadmap.md#2-open-questions).

---

## 3. The `ShutdownToken` contract

```rust
// crates/api/src/shutdown.rs
#[derive(Clone)]
pub struct ShutdownToken { /* opaque, wraps tokio_util::sync::CancellationToken */ }

impl ShutdownToken {
    /// Resolves when shutdown is requested (SIGTERM, postmaster death,
    /// catalog disable). Cancel-safe.
    pub async fn cancelled(&self);

    /// Non-blocking check.
    pub fn is_cancelled(&self) -> bool;

    /// Child token: cancelled when the parent cancels, *also* cancellable
    /// independently (e.g. for a per-connection sub-shutdown).
    pub fn child(&self) -> ShutdownToken;
}
```

A transport is expected to `tokio::select!` on `shutdown.cancelled()` in
its accept loop. (There are no per-connection tasks to cancel under the
v0 handoff model: once `handoff(fd)` returns, the frontend has no
further role for that connection.)

---

## 4. Example transport: `tcp_handoff`

With the in-tree `handoff::listener` helper (see
[transports.md §2.1](transports.md)), the body of `run` is six lines
— build a listener, hand it to the shared loop:

```rust
// crates/core/src/handoff/tcp.rs
use crate::handoff::listener::{run_handoff_loop, tcp_incoming};

pub struct TcpHandoff { cfg: TcpHandoffCfg }

pub fn build(cfg: &Config) -> anyhow::Result<Box<dyn HandoffTransport>> {
    Ok(Box::new(TcpHandoff { cfg: TcpHandoffCfg::from(cfg)? }))
}

impl HandoffTransport for TcpHandoff {
    fn name(&self) -> &'static str { "tcp_handoff" }

    fn run(
        self: Box<Self>,
        handle: HandoffHandle,           // ← only `handoff` is reachable
        shutdown: ShutdownToken,
    ) -> RunFuture {
        Box::pin(async move {
            let listener = tokio::net::TcpListener::bind(&self.cfg.bind_addr).await?;
            run_handoff_loop(tcp_incoming(listener), handle, shutdown).await
        })
    }
}
```

That's the entire transport. The only async tax is the
`Box::pin(async move { … })` wrapper at the top of `run` (because the
trait method returns a `Pin<Box<dyn Future>>` rather than using the
`#[async_trait]` macro — see §1 and
[roadmap.md §2 Q8](roadmap.md#2-open-questions)). The accept loop
itself, including shutdown discipline and per-accept error logging,
lives in `handoff::listener` and is shared across every fd-producing
transport (today and future ones). No FE/BE parsing, no
auth code, no TLS code, no per-conn state machine: the backend
([backend-handoff.md](backend-handoff.md) slot runner +
[backend-wire.md](backend-wire.md) wire layer) handles FE/BE v3 itself
via the `pgwire` crate, terminates TLS via rust-openssl, runs auth
against `pg_hba.conf` via `hba_getauthmethod`, and executes SQL via
`SPI_*`.

---

## 5. Registry & wiring

For how transports are registered at compile time and how the catalog
binds names to factories, see [workspace.md §4](workspace.md#4-registry--how-transports-get-wired-in).

v0 has no Cargo features and exactly one registered transport
(`tcp_handoff`); any other name in `pg_transport.transports` fails with
a clear `"transport X is not present"` error at start-up. See
[configuration.md](configuration.md) and
[workspace.md §5](workspace.md#5-catalog--registry-interaction).

---

## 6. Deferred surface: `SessionTransport` (sketch)

> **Status: not implemented in v0.** This section captures the planned
> shape so it can land without breaking the v0 surface. The full design
> is in [backend-pool.md](deferred/backend-pool.md).

A second trait is anticipated for transports whose wire is *not* FE/BE
on a kernel socket (HTTP/2 + SQL, custom binary, future QUIC/DPDK) or
that need to inspect plaintext FE/BE bytes before submission:

```rust
// crates/api/src/transport.rs  (planned, not in v0)
pub trait SessionTransport: 'static {
    fn name(&self) -> &'static str;
    fn run(
        self: Box<Self>,
        handle: SessionHandle,
        shutdown: ShutdownToken,
    ) -> RunFuture;                       // same alias as HandoffTransport
}

pub type SessionFactory = fn(cfg: &Config) -> anyhow::Result<Box<dyn SessionTransport>>;
```

The handle exposes two methods — `execute(opts, payload)` for stateless
one-shots and `acquire(opts)` for stateful multi-submit sessions — that
route payloads through the backend pool's `shm_mq`s and surface
responses as a `FrameStream`. The chooser table (when to use `execute`
vs `acquire`), the `Payload` enum (`Raw` / `Sql` / `Extended`), and the
`ExecutorSession` type all live in [backend-pool.md](deferred/backend-pool.md).

Why two traits rather than one generic `Transport<H>` when the second
lands:

- **Object safety drops out for free.** `Box<dyn HandoffTransport>` and
  `Box<dyn SessionTransport>` are straightforward; `Box<dyn Transport<H>>`
  needs the generic parameter spelled out at every storage site.
- **The registry is two clearly-typed maps**, not one type-erased map
  with `Any`-based dispatch.
- **Error messages are clearer**: "`TcpHandoff` does not implement
  `HandoffTransport`" is more direct than
  "`TcpHandoff` does not implement `Transport<HandoffHandle>`".
- **Mixed-mode transports are rare**, and a transport that genuinely
  needs both can implement both traits.

Roadmap: see [roadmap.md §4 — Deferred for v0](roadmap.md).

---

## See also

- [architecture.md](architecture.md) — how the trait surface fits into the
  larger picture.
- [frontend-handoff.md](frontend-handoff.md) — the implementation behind
  `HandoffHandle::handoff`.
- [transports.md](transports.md) — which concrete transports implement
  the v0 trait.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* design for
  `SessionHandle::execute` / `acquire` / `submit`.
