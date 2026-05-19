//! Wire layer — pluggable FE/BE protocol implementations.
//!
//! Per [backend-wire.md §1](../../../../docs/design/backend-wire.md),
//! the slot runner picks one `W: Wire` at compile time and calls
//! `W::run(fd, ctx)` per handoff. The trait surface is sync (the
//! slot loop is sync); the implementation enters tokio via
//! `block_on` inside its `run` body.
//!
//! v0 has one wire: [`pgwire_v3::PgwireV3`] (built on the sunng87
//! [`pgwire`](https://crates.io/crates/pgwire) crate).

pub mod auth;
pub mod extended;
pub mod pgwire_v3;
pub mod tls;

use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;

/// Pluggable wire-protocol implementation. The slot runner builds
/// one `WireCtx` per handoff, calls `W::run`, then drops it.
///
/// Phase 4: trait + one impl (pgwire-v3). Phase ≥ 9 adds the
/// per-handoff reset bookkeeping (prepared statements, portals,
/// etc.) to `WireCtx`.
pub trait Wire {
    /// Stable identifier (e.g. `"pgwire-v3"`).
    fn name() -> &'static str
    where
        Self: Sized;

    /// Run the wire on `fd` until the client disconnects, the wire
    /// returns a fatal error, or `ctx.shutdown` fires.
    fn run(fd: OwnedFd, ctx: WireCtx) -> anyhow::Result<()>;
}

/// Per-handoff context. Bundles the per-bgworker tokio runtime and
/// (phase ≥ 4b) the SPI bridge handle, hints, shutdown token, etc.
pub struct WireCtx {
    /// Per-bgworker tokio current-thread runtime, built once at slot
    /// startup and reused across handoffs. Wire impls enter it via
    /// `ctx.rt.block_on(...)`. See
    /// [backend-handoff.md §3](../../../../docs/design/backend-handoff.md).
    pub rt: Rc<tokio::runtime::Runtime>,
    /// Per-slot TLS acceptor built once from
    /// `pg_transport.tls_{cert,key}_file` GUCs (see
    /// [`tls::build`]). `None` when TLS is disabled. `Arc` because
    /// pgwire's `process_socket` takes ownership of the acceptor
    /// per call — clones are cheap (the inner `ServerConfig` is
    /// already `Arc`'d by `TlsAcceptor::from`).
    pub tls_acceptor: Option<Arc<TlsAcceptor>>,
}
