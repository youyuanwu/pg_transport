//! pg_transport public API — trait surface for transport plugins.
//!
//! No `pgrx`, no PG headers, no proc-macros. A plugin author writes
//! pure Rust against the trait definitions here without dragging in
//! the extension toolchain. See
//! [`docs/design/api.md`](../../../docs/design/api.md) for the
//! contract; this crate is the canonical source of truth.
//!
//! v0 surface: one trait (`HandoffTransport`), one handle type
//! (`HandoffHandle`), one shutdown token, one hints struct. The
//! deferred `SessionTransport` sibling lands when the shm_mq
//! general path un-defers (see
//! [`docs/design/deferred/backend-pool.md`](../../../docs/design/deferred/backend-pool.md)).

use std::future::Future;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;

pub use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Transport surface
// ---------------------------------------------------------------------------

/// Future returned by [`HandoffTransport::run`]. Boxed so the trait
/// stays object-safe (the registry holds `Box<dyn HandoffTransport>`).
///
/// Not `Send`-bound: the frontend runs a tokio current-thread runtime
/// with a `LocalSet`, so the future can hold `!Send` state across
/// awaits (`Rc<…>`, raw fds via `OwnedFd`, etc.).
pub type RunFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static>>;

/// Future returned by [`HandoffSink::handoff`] (and therefore by
/// [`HandoffHandle::handoff`]).
///
/// Boxed-local because `HandoffSink` is held behind
/// `Rc<dyn HandoffSink>` and must stay object-safe. The pool's
/// implementation awaits a slot to enter the `ready` container
/// before returning (see
/// [`docs/design/deferred/slot-readiness.md`](../../../docs/design/deferred/slot-readiness.md)
/// §1.3), so the future is no longer a fire-and-forget syscall.
pub type HandoffFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

/// Plugin surface for transports that hand kernel sockets to the
/// backend pool. The handle they get exposes only `handoff(fd)` —
/// no payload, no session, no FE/BE message types.
///
/// See [api.md §1](../../../docs/design/api.md#1-the-handofftransport-trait).
pub trait HandoffTransport: 'static {
    /// Stable identifier used in catalog config (e.g. `"tcp_handoff"`).
    fn name(&self) -> &'static str;

    /// Run until shutdown. Consumed because the framework spawns it
    /// once per transport instance. Implementers typically write
    /// `Box::pin(async move { … })` at the top of `run`.
    fn run(self: Box<Self>, handle: HandoffHandle, shutdown: ShutdownToken) -> RunFuture;
}

/// Transport factory. Each transport module exports one `build` fn of
/// this shape and registers it (see
/// [`docs/design/workspace.md`](../../../docs/design/workspace.md) §4).
///
/// `Config` is intentionally `&[u8]` for v0 — the catalog row's
/// per-transport config blob, parsed by the transport itself.
/// Phase ≥7 may swap this for a structured type once we have more
/// than one transport.
pub type HandoffFactory = fn(cfg: &[u8]) -> anyhow::Result<Box<dyn HandoffTransport>>;

// ---------------------------------------------------------------------------
// Handle: the only thing a transport calls to push an fd downstream
// ---------------------------------------------------------------------------

/// Per-transport handle for handing kernel fds to the backend pool.
///
/// The framework constructs one of these per transport instance and
/// passes it to [`HandoffTransport::run`]. The transport calls
/// [`HandoffHandle::handoff`] for every client fd it accepts.
///
/// Cheaply cloneable (`Rc` internally) so transports can spawn
/// per-accept tasks that each carry a handle.
///
/// `!Send`: the underlying sink lives on the frontend's
/// current-thread runtime (`LocalSet`).
#[derive(Clone)]
pub struct HandoffHandle {
    sink: Rc<dyn HandoffSink>,
}

impl HandoffHandle {
    /// Construct from any sink. The framework's pool implements
    /// [`HandoffSink`]; tests construct mock sinks.
    pub fn new(sink: Rc<dyn HandoffSink>) -> Self {
        Self { sink }
    }

    /// Hand `fd` to a backend slot. Ownership of the fd transfers to
    /// the framework on success (the kernel duplicates it into the
    /// slot process); the caller's `OwnedFd` is dropped here either
    /// way.
    ///
    /// Awaits a slot to enter the pool's `ready` container before
    /// `sendmsg`'ing the fd (see
    /// [`docs/design/deferred/slot-readiness.md`](../../../docs/design/deferred/slot-readiness.md)
    /// §1.3 — demand-driven dispatch). Returns once the kernel has
    /// accepted the `SCM_RIGHTS` `sendmsg` — *not* once the backend
    /// has confirmed receipt (Q20 in
    /// [roadmap.md §2.3](../../../docs/design/roadmap.md)). In-flight
    /// `Ok(())` handoffs may still be lost if the slot dies between
    /// `sendmsg` and `recvmsg`; the client sees a TCP reset
    /// (Q21 option (b), accepted).
    ///
    /// On saturation (all slots busy, pool at `max_backend_pool_size`,
    /// no ready slot within `HANDOFF_WAIT`) the future resolves to
    /// `Err(...)`; the caller (typically `tcp_handoff::run_inner`)
    /// turns this into a `ErrorResponse("too many connections")` for
    /// the client.
    pub fn handoff(&self, fd: OwnedFd, hints: HandoffHints) -> HandoffFuture<'_> {
        self.sink.handoff(fd, hints)
    }
}

/// Framework-internal trait that the pool implements; lets the api
/// crate stay independent of the pool's concrete type.
///
/// `!Send`-bound implicit (the `dyn HandoffSink` inside
/// [`HandoffHandle`] is held in `Rc`, which forces single-thread
/// usage).
pub trait HandoffSink {
    fn handoff(&self, fd: OwnedFd, hints: HandoffHints) -> HandoffFuture<'_>;
}

// ---------------------------------------------------------------------------
// HandoffHints
// ---------------------------------------------------------------------------

/// Small per-handoff metadata block sent alongside the fd. v0 keeps
/// this tiny per [backend-handoff.md §4](../../../docs/design/backend-handoff.md).
///
/// Phase ≥7 may add a `cert_id` for per-listener TLS variation
/// (currently deferred per Q13).
#[derive(Default, Debug, Clone, Copy)]
#[non_exhaustive]
pub struct HandoffHints {
    /// Whether the listener allowed TLS. The wire layer honours this
    /// when deciding to reply `'S'` to `SSLRequest`. Other TLS
    /// config (cert path, ciphers, min version) is GUC-driven.
    pub tls_allowed: bool,
}

impl HandoffHints {
    /// Convenience constructor for the common case.
    pub const fn no_tls() -> Self {
        Self { tls_allowed: false }
    }
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// Cancellation signal handed to every transport's `run`. Transports
/// `await` `token.cancelled().await` in a `select!` arm and exit
/// cleanly on cancellation.
///
/// Backed by [`tokio_util::sync::CancellationToken`] — a stable,
/// idiomatic primitive. Cheap to clone.
pub type ShutdownToken = CancellationToken;
