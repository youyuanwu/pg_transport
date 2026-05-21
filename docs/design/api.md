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

**Live source:** [crates/api/src/lib.rs](../../crates/api/src/lib.rs).
The trait shape:

```rust
pub type RunFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static>>;

pub trait HandoffTransport: 'static {
    fn name(&self) -> &'static str;
    fn run(self: Box<Self>, handle: HandoffHandle, shutdown: ShutdownToken) -> RunFuture;
}

pub type HandoffFactory = fn(cfg: &[u8]) -> anyhow::Result<Box<dyn HandoffTransport>>;
```

The `RunFuture` is intentionally **not** `Send`-bound — the frontend
runs a tokio current-thread runtime with `LocalSet`, so the future
can hold `!Send` state across awaits (`Rc<…>`, raw fds via
`OwnedFd`, etc.).

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

**Live source:** [crates/api/src/lib.rs](../../crates/api/src/lib.rs)
(`HandoffHandle` + the framework-internal `HandoffSink` trait the
pool implements). The transport-facing surface:

```rust
pub type HandoffFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

impl HandoffHandle {
    pub fn handoff(&self, fd: OwnedFd, hints: HandoffHints) -> HandoffFuture<'_>;
}
```

`handoff` is **async** because the pool dispatcher awaits a ready
slot before `send_fd`'ing (see [pool.md §1.3](pool.md#13-dispatch-path)
for the demand-driven dispatch model). On the warm-pool fast path,
the await resolves in microseconds; on cold-grow or saturation it
blocks up to `HANDOFF_WAIT` (5 s default). The returned `HandoffFuture`
is boxed-local because the `HandoffSink` trait stays object-safe and
the pool's implementation is `!Send`.

No `Payload`, no `SessionOpts`, no `FrameStream` — the backend wire
layer reads `StartupMessage` from the fd itself, so the framework
doesn't need to pass database/user/auth metadata. The handle is
cheaply cloneable (`Rc` internally) and `!Send` (the sink lives on
the frontend's current-thread runtime).

`HandoffHints` is a tiny per-handoff metadata block (currently
`{ tls_allowed: bool }`); a `cert_id` field for per-listener TLS
variation is deferred per Q13.

For the implementation of `handoff` (`SCM_RIGHTS`, single FE UDS
listener, dispatcher's ready-slot pop, slot runner, wire layer), see
[pool.md](pool.md),
[frontend-handoff.md](frontend-handoff.md),
[backend-handoff.md](backend-handoff.md), and
[backend-wire.md](backend-wire.md).

> **Resolved: simplest contract.** `handoff()` resolves to `Ok(())`
> on successful kernel-level handoff ("kernel accepted the fd-pass"
> — per Q21 this is `Ok(())` even if the backend dies before
> `recv_ctrl`; the client connection is then lost via TCP RST). On
> pool saturation (no ready slot within `HANDOFF_WAIT`), resolves
> to `Err(anyhow!("no slot became ready within ..."))`; the
> transport drops the fd, which the client observes as TCP reset.
> On frontend shutdown mid-handoff: the future is dropped; the fd
> is freed by `OwnedFd`'s drop. `EAGAIN`/`EINTR` retry transparently.
> Full reasoning in
> [roadmap.md §2 Q20](roadmap.md#2-open-questions) and
> [Q21](roadmap.md#2-open-questions).

---

## 3. The `ShutdownToken` contract

`ShutdownToken` is a type alias for
[`tokio_util::sync::CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)
(see [crates/api/src/lib.rs](../../crates/api/src/lib.rs)). Cheap to
clone; resolves when shutdown is requested (SIGTERM, postmaster
death, catalog disable); cancel-safe; supports child tokens for
per-connection sub-shutdown via `child_token()`.

A transport is expected to `tokio::select!` on `shutdown.cancelled()`
in its accept loop. (There are no per-connection tasks to cancel
under the v0 handoff model: once `handoff(fd)` returns, the frontend
has no further role for that connection.)

---

## 4. Example transport: `tcp_handoff`

The single v0 transport. Implementation:
[crates/core/src/handoff/tcp.rs](../../crates/core/src/handoff/tcp.rs).
The whole accept body fits in ~30 lines — build a `TcpListener`,
`select!` between `shutdown.cancelled()` and `listener.accept()`,
convert each accepted `TcpStream` to `OwnedFd` and call
`handle.handoff(fd, HandoffHints::no_tls())`. Trait surface:

```rust
impl HandoffTransport for TcpHandoff {
    fn name(&self) -> &'static str { "tcp_handoff" }
    fn run(self: Box<Self>, handle: HandoffHandle, shutdown: ShutdownToken) -> RunFuture {
        Box::pin(run_inner(*self, handle, shutdown))
    }
}
```

The only async tax is the `Box::pin(async move { … })` wrapper at the
top of `run` (because the trait method returns a
`Pin<Box<dyn Future>>` rather than using the `#[async_trait]` macro
— see §1 and [roadmap.md §2 Q8](roadmap.md#2-open-questions)). No
FE/BE parsing, no auth code, no TLS code, no per-conn state machine:
the backend ([backend-handoff.md](backend-handoff.md) slot runner +
[backend-wire.md](backend-wire.md) wire layer) handles FE/BE v3
itself via the `pgwire` crate, terminates TLS via rust-openssl, runs
auth against `pg_hba.conf` via `hba_getauthmethod`, and executes SQL
via `SPI_*`.

---

## 5. Registry & wiring

For how transports are registered at compile time, see
[workspace.md §4](workspace.md#4-registry--how-transports-get-wired-in).
v0 has no Cargo features and exactly one registered transport
(`tcp_handoff`), instantiated directly in
[crates/core/src/frontend.rs](../../crates/core/src/frontend.rs); no
catalog table exists yet. When the catalog surface lands (see
[configuration.md §1.3](configuration.md#13-planned-catalog-surface)),
unknown `kind` values will fail with a clear `"transport X is not
present"` error.

---

## 6. Deferred surface: `SessionTransport`

A second trait — for transports whose wire is *not* FE/BE on a
kernel socket (HTTP/2 + SQL, custom binary, future QUIC/DPDK) or
that need to inspect plaintext FE/BE bytes before submission — is
**deferred**. Trait signature, rationale for keeping two traits
(`HandoffTransport` + `SessionTransport`) rather than one generic
`Transport<H>`, and full handle/`Payload`/`FrameStream` design live
in [deferred/backend-pool.md §0](deferred/backend-pool.md#0-trait-surface-sessiontransport--sessionhandle).
Roadmap entry: [roadmap.md §4](roadmap.md#4-deferred-for-v0).

---

## See also

- [architecture.md](architecture.md) — how the trait surface fits into the
  larger picture.
- [frontend-handoff.md](frontend-handoff.md) — the implementation behind
  `HandoffHandle::handoff`.
- [transports.md](transports.md) — which concrete transports implement
  the v0 trait.
- [deferred/backend-pool.md](deferred/backend-pool.md) — *deferred*
  design for `SessionHandle::execute` / `acquire` / `submit`.
