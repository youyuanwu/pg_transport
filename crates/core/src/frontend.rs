//! Frontend bgworker — the long-lived supervisor process.
//!
//! Registered statically by [`crate::_PG_init`] (Q22 → option (a) in
//! `docs/design/roadmap.md` §2.3). Boots a tokio current-thread runtime
//! with a `LocalSet` and runs the frontend supervisor loop until
//! `SIGTERM` or postmaster death.
//!
//! Phase 2 scope (in addition to phase 1's heartbeat + signals +
//! watchdog): stand up the [`crate::backend::pool::BackendPool`] —
//! per-slot UDS listener, wait for each slot bgworker to connect —
//! and exercise the send path with a periodic dummy fd handoff for
//! self-verification. No wire layer yet (phase 4).

use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use tokio::signal::unix::{SignalKind, signal};

use crate::backend::pool::BackendPool;
use crate::guc;

/// How long we wait for any single slot bgworker to connect to its
/// listener before giving up. Static registration spawns slots
/// concurrently with the FE; the slot side retries `connect()` for
/// 3 s (see `backend::slot::connect_with_retry`), so 5 s here has
/// comfortable margin.
const SLOT_ACCEPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Frontend bgworker entry point.
///
/// `#[unsafe(no_mangle)]` keeps the symbol name pgrx-callable via
/// `dlsym` once `BackgroundWorkerBuilder::set_function(
/// "pg_transport_frontend_main")` resolves it at postmaster start.
///
/// `panic = "abort"` is set workspace-wide (Q9), so any `panic!()`
/// here aborts the bgworker; the postmaster respawns it after
/// `bgw_restart_time` (1 s). Same blast radius as PG `FATAL`.
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

    // Bring up the backend pool — bind per-slot listeners, wait for
    // each slot bgworker to connect. The pool is held alive for the
    // lifetime of the FE supervisor; dropping it on shutdown closes
    // every per-slot UnixStream, which the slot side observes as
    // clean EOF on its blocking recvmsg.
    let pool_size = guc::backend_pool_size();
    let pool = match BackendPool::start(pool_size, SLOT_ACCEPT_TIMEOUT).await {
        Ok(p) => p,
        Err(e) => {
            // Don't `error!` here — it would `panic_any` and abort
            // (Q24). A WARNING + clean exit lets the postmaster
            // respawn us; if the pool just never comes up we'll
            // loop on this error, which is the right operator
            // signal that something is wrong (e.g. permissions on
            // the socket directory).
            pgrx::warning!(
                "pg_transport frontend: BackendPool::start failed: {e}; exiting (will be respawned)"
            );
            return;
        }
    };

    // Intervals: the FIRST `.tick()` on a fresh `Interval` fires
    // immediately, so consume it before the loop to avoid an instant
    // tick at startup. After this, all tickers fire at their
    // configured periods from "now".
    //
    // Phase-1 heartbeat / watchdog periods are hard-coded constants
    // per Q23 (zero new GUCs in phase 1). The phase-2 test-handoff
    // tick (5 s) is also hard-coded — phase 3 replaces it with real
    // transport-driven traffic, so a knob would be obsolete by then.
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut watchdog = tokio::time::interval(Duration::from_millis(500));
    let mut test_handoff = tokio::time::interval(Duration::from_secs(5));
    heartbeat.tick().await;
    watchdog.tick().await;
    test_handoff.tick().await;

    pgrx::log!("pg_transport frontend: tokio runtime ready");

    // Round-robin slot id for the test-handoff tick.
    let mut next_test_slot: u32 = 0;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                pgrx::log!("pg_transport frontend: SIGTERM received");
                break;
            }
            _ = sighup.recv() => {
                // Phase 2: nothing to reload yet. Phase ≥3 will
                // reconcile the live listener set against
                // pg_transport.transports here.
                pgrx::log!("pg_transport frontend: SIGHUP received (no-op in phase 2)");
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
            _ = test_handoff.tick() => {
                // Phase-2 acceptance: prove the FE→slot sendmsg
                // path works. Round-robin through the pool so every
                // slot exercises its recvmsg loop. Phase 3 drops
                // this — real transports produce real fds.
                let slot_id = next_test_slot;
                next_test_slot = (next_test_slot + 1) % pool_size;
                match pool.test_handoff(slot_id) {
                    Ok(()) => pgrx::log!(
                        "pg_transport frontend: test handoff -> slot {slot_id} OK"
                    ),
                    Err(e) => pgrx::warning!(
                        "pg_transport frontend: test handoff -> slot {slot_id} failed: {e}"
                    ),
                }
            }
        }
    }

    // Explicit drop so the per-slot UnixStream closure (= slot-side
    // EOF) happens before this fn returns. The slot bgworkers see
    // recvmsg → 0 and exit cleanly.
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
    rc == 0
}
