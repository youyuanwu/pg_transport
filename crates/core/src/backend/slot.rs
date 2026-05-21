//! Slot bgworker entry point — the backend side of the v0 handoff
//! path.
//!
//! Per [backend-handoff.md §3](../../../../docs/design/backend-handoff.md)
//! and [`docs/design/deferred/slot-readiness.md`](../../../../docs/design/deferred/slot-readiness.md)
//! §1.1: the outer slot loop is SYNC so the per-handoff
//! `PgwireV3::run` (which internally calls `ctx.rt.block_on(...)`)
//! is not nested inside another `block_on`. The async UDS helpers
//! (`recv_ctrl_async`, `send_ready_byte_async`,
//! `send_drain_ack_async`) get their own brief `rt.block_on(...)`
//! windows between wire runs; SIGTERM cancellation comes from a
//! `tokio::time::timeout` around `recv_ctrl_async` (100 ms tick),
//! which re-checks `BackgroundWorker::sigterm_received()` on each
//! tick.
//!
//! Wire protocol on the UDS (see `fd_pass.rs` for the full opcode
//! table):
//!
//! * BE → FE: `b'R'` after every readiness boundary; `b'A'` after
//!   receiving a drain request.
//! * FE → BE: `b'\0' + SCM_RIGHTS(fd)` for a handoff; `b'X'` for a
//!   cooperative drain request.

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;

use super::fd_pass::{self, CtrlMsg};
use super::paths;
use crate::wire::{Wire, WireCtx, pgwire_v3::PgwireV3};

/// Slot bgworker C entry point.
///
/// Registered dynamically via
/// `BackgroundWorkerBuilder::new("pg_transport slot").load_dynamic()`
/// from the FE's pool when the dispatcher needs to grow. No
/// `bgw_main_arg` is passed: with the single-listener UDS topology
/// (`docs/design/deferred/slot-readiness.md` §2.0), the FE assigns
/// a `slot_id` on accept and the BE never needs to know it.
///
/// `#[unsafe(no_mangle)]` keeps the symbol callable by PG via `dlsym`.
/// Workspace sets `panic = "unwind"` (Q9 re-resolved via Q24).
/// An uncaught panic unwinds out of this function; `#[pg_guard]`
/// catches it at the C boundary and emits an ereport. The bgworker
/// is BGW_NEVER_RESTART (no `set_restart_time`), so an exit on
/// panic leaves the slot gone — the FE's next saturation event
/// grows a fresh one.
#[unsafe(no_mangle)]
#[pg_guard]
pub extern "C-unwind" fn pg_transport_slot_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Initialize an SPI-capable backend connection. Required
    // before any `Spi::*` call inside this bgworker; see
    // backend-wire.md §6 (SPI bridge). Phase 4b hard-codes the
    // target database to "postgres" — phase ≥ 7 will route based
    // on the client's StartupMessage `database` parameter.
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    pgrx::log!("pg_transport slot: starting");

    if let Err(e) = run_slot() {
        // WARNING is sufficient: the slot is exiting anyway. Unlike
        // the static-pool world, the FE will not auto-respawn this
        // process; the next saturation event will grow a fresh one.
        pgrx::warning!("pg_transport slot exited: {e}");
    } else {
        pgrx::log!("pg_transport slot: clean exit");
    }
}

fn run_slot() -> io::Result<()> {
    let path = paths::frontend_socket_path();

    // Connect to the FE's single UDS listener. The FE binds at boot
    // (see `crate::backend::pool::bind_listener`); a fresh slot may
    // be spawned by `grow_one` while the FE is still mid-bind, so
    // we retry with short backoff to tolerate the race.
    let stream = connect_with_retry(&path)?;
    stream.set_nonblocking(true)?;
    let stream_fd = stream.as_raw_fd();

    pgrx::log!(
        "pg_transport slot: connected to {} (wire={})",
        path.display(),
        PgwireV3::name()
    );

    // Per-bgworker tokio runtime, reused across handoffs. Rc so it
    // can be cloned into each WireCtx; we don't need Send because
    // the slot is single-threaded.
    let rt = Rc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| io::Error::other(format!("tokio runtime: {e}")))?,
    );

    // Per-slot TLS acceptor built once from pg_transport.tls_*
    // GUCs (phase 8). None when TLS is disabled. We build at slot
    // boot rather than per-handoff so cert-file IO + rustls config
    // assembly happens off the handoff hot path.
    let tls_acceptor = crate::wire::tls::build()?;
    if tls_acceptor.is_some() {
        pgrx::log!("pg_transport slot: TLS enabled");
    }

    // Slot main loop. The outer loop is SYNC so we can call the
    // sync `PgwireV3::run` (which internally uses `ctx.rt.block_on`)
    // between UDS-I/O steps. Wrapping the loop in `rt.block_on`
    // would make `PgwireV3::run`'s internal `block_on` a NESTED
    // block_on, which panics (tokio's
    // "Cannot start a runtime from within a runtime"). Instead, we
    // use short-lived `rt.block_on` calls for the async UDS helpers
    // (recv_ctrl, send_ready_byte, send_drain_ack) and let
    // `PgwireV3::run` use its own `block_on` between them.
    //
    // The SIGTERM-driven UDS read cancellation comes via
    // `tokio::time::timeout` inside the recv block_on: every 100 ms
    // the recv future times out, we re-check
    // `BackgroundWorker::sigterm_received()`, and loop. Bounded
    // shutdown latency, no CancellationToken plumbing needed.

    // wrap_for_async needs to run on the tokio runtime (it registers
    // the fd with the reactor). Construct the AsyncFd inside an
    // initial `block_on` and hold it across subsequent `block_on`
    // calls — that's safe because the reactor registration outlives
    // any single block_on invocation.
    let async_stream = rt.block_on(async { fd_pass::wrap_for_async(stream) })?;

    // Announce initial readiness. Subsequent b'R' bytes are sent in
    // the FdHandoff arm after wire::run returns — invariant: at
    // most one outstanding b'R'.
    rt.block_on(fd_pass::send_ready_byte_async(&async_stream))
        .map_err(|e| io::Error::other(format!("send_ready_byte: {e}")))?;

    let mut handoffs: u64 = 0;
    loop {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_transport slot: SIGTERM, exiting");
            break;
        }

        // Wait up to 100 ms for the next control message. The
        // timeout makes SIGTERM polling responsive without
        // dedicating a tokio task to it.
        let recv_outcome = rt.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(100),
                fd_pass::recv_ctrl_async(&async_stream),
            )
            .await
        });

        let ctrl = match recv_outcome {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => return Err(io::Error::other(format!("recv_ctrl: {e}"))),
            Err(_timeout) => continue, // re-check sigterm
        };

        match ctrl {
            CtrlMsg::FdHandoff(fd) => {
                handoffs += 1;
                pgrx::log!(
                    "pg_transport slot: received fd {} (handoff #{handoffs})",
                    fd.as_raw_fd()
                );
                let ctx = WireCtx {
                    rt: rt.clone(),
                    tls_acceptor: tls_acceptor.clone(),
                };
                // PgwireV3::run is SYNC; it calls ctx.rt.block_on
                // internally. We're OUTSIDE any block_on here, so
                // that's a flat sequential entry — not nested.
                if let Err(e) = PgwireV3::run(fd, ctx) {
                    pgrx::warning!("pg_transport slot: wire run failed: {e}");
                }
                // Per-handoff reset: scrub any session-level state
                // the previous client left behind on this PG
                // backend process. Without this, a `SET
                // pg_transport.execution_backend = 'direct'` (or
                // any other USERSET GUC) issued by one client
                // leaks to subsequent clients on the same slot —
                // vanilla PG hides this because the backend
                // process exits between sessions; our slots
                // survive across many handoffs. See backend-handoff.md
                // §5.
                reset_per_handoff_state();
                // Wire done — announce readiness for the next
                // handoff.
                rt.block_on(fd_pass::send_ready_byte_async(&async_stream))
                    .map_err(|e| io::Error::other(format!("send_ready_byte: {e}")))?;
            }
            CtrlMsg::DrainRequest => {
                pgrx::log!(
                    "pg_transport slot: drain request received \
                     (handoffs served: {handoffs}); cleaning up"
                );
                cleanup_per_slot_state();
                // Best-effort ack — if the FE has gone away, the
                // kernel returns EPIPE and we exit anyway.
                if let Err(e) = rt.block_on(fd_pass::send_drain_ack_async(&async_stream)) {
                    pgrx::warning!("pg_transport slot: drain ack send failed: {e}");
                }
                break;
            }
            CtrlMsg::Eof => {
                pgrx::log!("pg_transport slot: FE closed UDS, exiting");
                break;
            }
        }
    }

    // Silence "unused" warning on the SIGTERM exit path.
    let _ = handoffs;
    // Drop async_stream → drops the wrapped UnixStream → kernel
    // closes our end → FE's slot_reader sees EOF.
    drop(async_stream);
    let _ = stream_fd;
    Ok(())
}

/// Free per-slot caches before sending the drain ack. Phase 9
/// will plumb prepared-plan registries and tokio runtime drops
/// here; for now the bgworker's `proc_exit` does the heavy lifting
/// on the way out.
fn cleanup_per_slot_state() {
    // Intentionally empty for phase ≥ 4 baseline. Hook is here so
    // future per-slot state (plan cache, TLS session resumption,
    // wire-level pools) lands in one obvious place.
}

/// Reset session-scoped PG state between handoffs.
///
/// Vanilla PG processes one client per backend and `proc_exit`s
/// between sessions; the GUC stack, prepared statements, temp
/// tables, etc. are gone implicitly. Our slot bgworker survives
/// across many handoffs, so we must scrub session state
/// explicitly or it leaks between unrelated client connections.
///
/// Current scope:
///
/// * **GUCs** — `pg_sys::ResetAllOptions()` walks the GUC variable
///   list and reverts every `SET` (USERSET / SUSET / SIGHUP) to
///   its boot-time default. This is the immediate cross-handoff
///   isolation gate; without it a client's `SET
///   pg_transport.execution_backend = 'direct'` leaks to the next
///   handoff on the same slot, which historically reproduced as
///   `ERROR: unrecognized node type: 0x7F7F7F7F` (a use-after-free
///   in the direct backend's plan cache) under e2e parallelism.
///
/// Future scope (phase ≥ 9.5):
/// * SQL-level prepared statements (`DropAllPreparedStatements`).
/// * Temp namespace cleanup.
/// * Cursors / portals not already torn down by pgwire's drop.
/// * Reset transaction state if dirty.
///
/// SAFETY: `ResetAllOptions` is callable from any backend with
/// GUC machinery initialised (we are — `connect_worker_to_spi`
/// ran at slot boot). It may `ereport(ERROR)` if a check_hook
/// fails on a default value, which pgrx surfaces as a Rust panic
/// the slot's `#[pg_guard]` boundary catches; the panic propagates
/// out of `run_slot`, the bgworker exits, and the FE's reader
/// observes EOF and removes the slot. No state corruption.
fn reset_per_handoff_state() {
    // SAFETY: see fn doc.
    unsafe {
        pg_sys::ResetAllOptions();
    }
}

fn connect_with_retry(path: &std::path::Path) -> io::Result<UnixStream> {
    // Total budget: 100 retries × 100 ms = 10 s. The FE binds at
    // its bgworker boot which races with this slot's boot; a fresh
    // dynamic slot may also outrun the FE's accept_loop briefly.
    // 10 s is comfortable margin for either case.
    const MAX_RETRIES: u32 = 100;
    const BACKOFF: Duration = Duration::from_millis(100);

    for attempt in 0..MAX_RETRIES {
        match UnixStream::connect(path) {
            Ok(s) => return Ok(s),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                if attempt == 0 {
                    pgrx::log!(
                        "pg_transport slot: waiting for FE listener at {}",
                        path.display()
                    );
                }
                thread::sleep(BACKOFF);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other(format!(
        "pg_transport slot: FE listener at {} never appeared",
        path.display()
    )))
}
