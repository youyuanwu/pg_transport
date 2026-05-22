//! `tcp_handoff` — the single v0 transport.
//!
//! Binds a TCP listener, accepts client connections, and hands the
//! accepted fd to the backend pool via [`HandoffHandle::handoff`].
//! Wire-protocol, TLS, and auth happen entirely in the backend slot
//! — this module never reads or writes a byte of client data.
//!
//! See [transports.md §1](../../../../docs/design/transports.md) and
//! the example sketch in [api.md §4](../../../../docs/design/api.md).
//!
//! Phase 3 hard-codes the bind address; phase ≥ 7 reads it from the
//! `pg_transport.transports` catalog row via the registry.

use std::net::SocketAddr;
use std::os::fd::OwnedFd;

use api::{HandoffHandle, HandoffHints, HandoffTransport, RunFuture, ShutdownToken};
use tokio::net::TcpListener;

/// `tcp_handoff` transport configuration. Phase 3 carries only the
/// bind address; phase ≥ 8 will grow `tls_allowed` once TLS-aware
/// frontends matter.
#[derive(Debug, Clone)]
pub struct TcpHandoffCfg {
    pub bind_addr: SocketAddr,
}

pub struct TcpHandoff {
    cfg: TcpHandoffCfg,
}

impl TcpHandoff {
    pub fn new(cfg: TcpHandoffCfg) -> Self {
        Self { cfg }
    }

    /// Boxed-trait-object form for callers that don't want to
    /// downcast. Equivalent to `Box::new(TcpHandoff::new(cfg))`.
    pub fn boxed(cfg: TcpHandoffCfg) -> Box<dyn HandoffTransport> {
        Box::new(Self::new(cfg))
    }
}

impl HandoffTransport for TcpHandoff {
    fn name(&self) -> &'static str {
        "tcp_handoff"
    }

    fn run(self: Box<Self>, handle: HandoffHandle, shutdown: ShutdownToken) -> RunFuture {
        Box::pin(run_inner(*self, handle, shutdown))
    }
}

async fn run_inner(
    this: TcpHandoff,
    handle: HandoffHandle,
    shutdown: ShutdownToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(this.cfg.bind_addr)
        .await
        .map_err(|e| anyhow::anyhow!("tcp_handoff: bind {}: {e}", this.cfg.bind_addr))?;
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| this.cfg.bind_addr.to_string());

    pgrx::log!("pg_transport tcp_handoff: listening on {local}");

    // The accept loop — `select!` arms:
    //
    // * `shutdown.cancelled()` — supervisor asked us to stop. Drop
    //   the listener, return cleanly.
    // * `listener.accept()` — got a client fd. Convert to OwnedFd
    //   and `spawn_local` a per-accept handoff task. Spawning
    //   decouples accept throughput from handoff latency: under
    //   bursts (esp. cold-grow ~30 ms) we keep draining the TCP
    //   backlog while pool-side waiters race for ready slots.
    //   `HandoffHandle` is `Clone` + `Rc`-backed precisely so we
    //   can do this (api.md §2). The pool's `HANDOFF_WAIT` (5 s)
    //   bounds each task's lifetime; downstream the pool caps
    //   total slots at `max_backend_pool_size`. A future hardening
    //   pass may add a per-listener `Semaphore` to bound in-flight
    //   handoffs under SYN flood.
    //
    // Accept errors are split: `ConnectionAborted` / `Interrupted`
    // are transient and we just continue; anything else is logged
    // as a WARNING and we also continue. Per the design, the
    // accept loop is best-effort: a persistent listener failure
    // shows up as 100% accept errors and is the operator's signal
    // to investigate.
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                pgrx::log!("pg_transport tcp_handoff: shutdown");
                return Ok(());
            }
            r = listener.accept() => {
                match r {
                    Ok((stream, peer)) => {
                        let handle = handle.clone();
                        tokio::task::spawn_local(async move {
                            // Convert TcpStream into OwnedFd.
                            // into_std() moves us out of tokio's
                            // reactor (which is what we want — the
                            // slot will own this fd from here).
                            let std_stream = match stream.into_std() {
                                Ok(s) => s,
                                Err(e) => {
                                    pgrx::warning!(
                                        "pg_transport tcp_handoff: into_std failed for {peer}: {e}"
                                    );
                                    return;
                                }
                            };
                            let fd: OwnedFd = std_stream.into();
                            if let Err(e) = handle.handoff(fd, HandoffHints::no_tls()).await {
                                // Saturation or send error. The
                                // error string is the operator-visible
                                // signal; we don't translate to
                                // ErrorResponse here because the
                                // client fd has already been dropped
                                // (consumed by handoff()). A future
                                // enhancement could split handoff()
                                // into "reserve slot" + "send" so we
                                // can write ErrorResponse on the
                                // client fd before dropping it. For
                                // now, saturated clients see a TCP
                                // reset.
                                pgrx::warning!(
                                    "pg_transport tcp_handoff: handoff for {peer} failed: {e}"
                                );
                            }
                        });
                    }
                    Err(e) if matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::Interrupted
                    ) => continue,
                    Err(e) => {
                        pgrx::warning!(
                            "pg_transport tcp_handoff: accept error: {e}"
                        );
                    }
                }
            }
        }
    }
}
