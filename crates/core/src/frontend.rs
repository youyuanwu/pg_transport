//! Frontend bgworker — the long-lived supervisor process.
//!
//! Registered statically by [`crate::_PG_init`] (Q22 → option (a) in
//! `docs/design/roadmap.md` §2.3). Boots a tokio current-thread runtime
//! with a `LocalSet` and runs the frontend supervisor loop until
//! `SIGTERM` or postmaster death.
//!
//! Phase 3 scope (adds to phases 1 + 2): on boot, also spawn the
//! single v0 transport (`tcp_handoff`) on the LocalSet, with a
//! [`HandoffHandle`] backed by the [`Pool`]. Every accepted TCP
//! client is fd-passed to a slot via SCM_RIGHTS, where the phase-3
//! null wire writes a synthetic v3 ErrorResponse and closes.
//!
//! Pool topology per
//! [`docs/design/deferred/slot-readiness.md`](../../../docs/design/deferred/slot-readiness.md):
//! the FE binds a single UDS listener, spawns an `accept_loop` task
//! that assigns `slot_id`s on accept, and an `idle_reaper` task
//! that drains slots idle past `IDLE_REAP_AFTER`. Slots are
//! created dynamically by the dispatcher's `grow_one()` whenever a
//! handoff arrives with no ready slot.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use api::{HandoffHandle, ShutdownToken};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use tokio::signal::unix::{SignalKind, signal};

use crate::backend::pool::{self, Pool};
use crate::handoff::tcp::{TcpHandoff, TcpHandoffCfg};

/// Phase-3 hard-coded bind address for the single `tcp_handoff`
/// transport. Phase ≥ 7 reads this from `pg_transport.transports`.
const PHASE_3_TCP_BIND: &str = "127.0.0.1:5454";

/// Frontend bgworker entry point.
///
/// `#[unsafe(no_mangle)]` keeps the symbol name pgrx-callable via
/// `dlsym` once `BackgroundWorkerBuilder::set_function(
/// "pg_transport_frontend_main")` resolves it at postmaster start.
///
/// Workspace sets `panic = "unwind"` (Q9 re-resolved via Q24). An
/// uncaught panic here unwinds out of `block_on` and out of this
/// `extern "C-unwind"` function; `#[pg_guard]` catches it at the C
/// boundary and emits an ereport. The postmaster then respawns the
/// bgworker after `bgw_restart_time` (1 s).
#[unsafe(no_mangle)]
#[pg_guard]
pub extern "C-unwind" fn pg_transport_frontend_main(_arg: pg_sys::Datum) {
    // Register PG signal bookkeeping. Sets ConfigReloadPending on
    // SIGHUP and ShutdownRequestPending on SIGTERM. We additionally
    // take both via `tokio::signal::unix` below for async wakeups —
    // the PG flags are not what drives our control flow but they
    // keep PG's own state honest (some PG paths check them).
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // SIGPIPE → EPIPE return rather than process death (per
    // frontend-handoff.md §2.1 final step). We rely on `MSG_NOSIGNAL`
    // on every `sendmsg` call too, but masking the signal globally
    // is belt-and-braces for any other library code that writes to a
    // socket without our flag.
    //
    // SAFETY: signal() with SIG_IGN is well-defined and thread-safe.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime");

    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(frontend_main()));

    pgrx::log!("pg_transport frontend: clean shutdown");
}

/// Frontend supervisor loop. Returns when `SIGTERM` fires or the
/// postmaster dies.
async fn frontend_main() {
    let mut sighup = signal(SignalKind::hangup()).expect("tokio sighup handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("tokio sigterm handler");

    // Bind the single UDS listener. Failure here (e.g. permissions
    // on the socket directory) is a hard config error; exit cleanly
    // and let the postmaster respawn us so the operator sees the
    // error repeatedly until they fix it.
    let listener = match pool::bind_listener() {
        Ok(l) => l,
        Err(e) => {
            pgrx::warning!(
                "pg_transport frontend: bind_listener failed: {e}; exiting (will be respawned)"
            );
            return;
        }
    };

    // The pool itself. `Rc` for cheap sharing across the
    // dispatcher (via HandoffHandle), the accept_loop task, the
    // idle_reaper task, and the SIGHUP handler.
    let pool: Rc<Pool> = Rc::new(Pool::new());

    // Optional pre-warm. Fire MIN_WARM_SLOTS `grow_one()` calls
    // before any client arrives. Compile-time MIN_WARM_SLOTS = 0
    // by default — no pre-warm; first connection pays cold-grow.
    pool::pre_warm();

    // HandoffHandle wraps the pool behind a Rc<dyn HandoffSink>.
    // The transport's `run` future is spawn_local'd, so !Send is
    // fine (LocalSet semantics).
    let handle = HandoffHandle::new(pool.clone());

    // Shared shutdown token; one source, every long-lived task
    // gets a clone. Cancelled below before we drop the pool.
    let shutdown = ShutdownToken::new();

    // Pool lifecycle tasks: accept_loop owns the listener and
    // assigns slot ids on accept; idle_reaper drains slots that
    // have been ready longer than IDLE_REAP_AFTER. Both run for
    // the lifetime of the FE.
    let accept_task = {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        tokio::task::spawn_local(async move {
            if let Err(e) = pool::accept_loop(listener, pool, shutdown).await {
                pgrx::warning!("pg_transport pool: accept_loop returned error: {e}");
            }
        })
    };
    let reaper_task = {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        tokio::task::spawn_local(pool::idle_reaper(pool, shutdown))
    };

    // Spawn the single v0 transport. Phase ≥ 4 will drive this from
    // a catalog read (`pg_transport.transports`); phase 3 hard-codes
    // the one config we have.
    let transport_handle = {
        let bind_addr: SocketAddr = match PHASE_3_TCP_BIND.parse() {
            Ok(a) => a,
            Err(e) => {
                pgrx::warning!(
                    "pg_transport frontend: invalid bind addr {PHASE_3_TCP_BIND:?}: {e}"
                );
                shutdown.cancel();
                accept_task.abort();
                reaper_task.abort();
                return;
            }
        };
        let transport = TcpHandoff::boxed(TcpHandoffCfg { bind_addr });
        let handle = handle.clone();
        let shutdown = shutdown.clone();
        tokio::task::spawn_local(async move {
            if let Err(e) = transport.run(handle, shutdown).await {
                pgrx::warning!("pg_transport tcp_handoff: run returned error: {e}");
            }
        })
    };

    // Intervals: the FIRST `.tick()` on a fresh `Interval` fires
    // immediately, so consume it before the loop to avoid an instant
    // tick at startup. After this, all tickers fire at their
    // configured periods from "now".
    //
    // Phase-1 heartbeat / watchdog periods are hard-coded constants
    // per Q23 (zero new GUCs in phase 1). Phase 3 dropped the
    // phase-2 test-handoff tick — real transport traffic supersedes
    // self-test.
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut watchdog = tokio::time::interval(Duration::from_millis(500));
    heartbeat.tick().await;
    watchdog.tick().await;

    pgrx::log!("pg_transport frontend: tokio runtime ready");

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                pgrx::log!("pg_transport frontend: SIGTERM received");
                break;
            }
            _ = sighup.recv() => {
                // SIGHUP reconciles the pool against the (possibly
                // changed) `pg_transport.max_backend_pool_size`
                // GUC. If the new ceiling is lower than current
                // total, drain excess idle slots; in-flight slots
                // are left to finish their session naturally (the
                // dispatcher's grow gate will refuse to add new
                // ones while at the new lower ceiling).
                pgrx::log!("pg_transport frontend: SIGHUP received");
                pool::sighup_reconcile(&pool);
            }
            _ = watchdog.tick() => {
                if postmaster_died() {
                    pgrx::log!("pg_transport frontend: postmaster died, exiting");
                    break;
                }
            }
            _ = heartbeat.tick() => {
                pgrx::log!("pg_transport frontend: heartbeat");
            }
        }
    }

    // Cancel the shared shutdown token; the transport's accept loop
    // and the pool's accept_loop/idle_reaper all observe
    // `shutdown.cancelled()` and return from their own accord. We
    // `.await` the join handles so the listener is gone before we
    // drop the pool.
    shutdown.cancel();
    let _ = transport_handle.await;
    let _ = accept_task.await;
    let _ = reaper_task.await;
    // pool dropped at fn exit — drops the slot streams; slot
    // bgworkers observe EOF on their recv_ctrl_async and exit
    // cleanly via the CtrlMsg::Eof branch.
    drop(pool);
}

/// Non-blocking check: is the postmaster still alive?
///
/// PG 18 keeps a self-pipe whose write end is held by the postmaster
/// and whose read end (`postmaster_alive_fds[POSTMASTER_FD_WATCH]`,
/// non-blocking) is inherited by every child. EOF on the read end
/// means the postmaster has exited; `EAGAIN` means it's still alive.
///
/// PG's own `PostmasterIsAliveInternal()` (storage/pmsignal.c) is a
/// six-line static-inline that bindgen does not export; this is the
/// same body. The wrapping `PostmasterIsAlive()` macro just calls it.
///
/// Returns `true` only on the unambiguous EOF case; every other
/// outcome (EAGAIN, EINTR, unexpected error, surprise byte) is
/// reported as "alive" — same posture as PG, which means a transient
/// glitch leaves us relying on the SIGTERM path rather than racing
/// it.
fn postmaster_died() -> bool {
    let fd = unsafe { pg_sys::postmaster_alive_fds[pg_sys::POSTMASTER_FD_WATCH as usize] };
    let mut byte: u8 = 0;
    // SAFETY: reading from a valid, non-blocking pipe fd inherited
    // from the postmaster; the buffer is a single byte on this
    // function's stack.
    let rc = unsafe { libc::read(fd, &mut byte as *mut u8 as *mut libc::c_void, 1) };
    if rc == 0 {
        return true;
    }
    if rc < 0 {
        let errno = unsafe { *libc::__errno_location() };
        // EAGAIN is the expected "alive" signal on the non-blocking
        // pipe (EWOULDBLOCK == EAGAIN on Linux). EINTR is harmless
        // (caller re-checks next tick). Anything else: log once and
        // treat as alive so a transient glitch doesn't kill the FE.
        if !matches!(errno, libc::EAGAIN | libc::EINTR) {
            pgrx::warning!(
                "pg_transport frontend: watchdog read on fd {fd} returned errno {errno} (treating as alive)"
            );
        }
    }
    false
}
