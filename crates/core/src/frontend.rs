//! Frontend bgworker — the long-lived supervisor process.
//!
//! Registered statically by [`crate::_PG_init`] (Q22 → option (a) in
//! `docs/design/roadmap.md` §2.3). Boots a tokio current-thread runtime
//! with a `LocalSet` and runs the frontend supervisor loop until
//! `SIGTERM` or postmaster death.
//!
//! Phase 1 scope: heartbeat + signal handling + postmaster watchdog.
//! No transports, no listeners, no SQL surface, no slot pool yet —
//! those land in subsequent phases per `docs/design/roadmap.md`.

use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use tokio::signal::unix::{SignalKind, signal};

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
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime");

    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(frontend_main()));

    pgrx::log!("pg_transport frontend: clean shutdown");
}

/// Frontend supervisor loop. Returns when `SIGTERM` fires or the
/// postmaster dies. The body intentionally has no transport-spawn
/// logic — that lands in phase ≥3. Phase 1 just proves the runtime
/// boots and reacts to the right wakeups.
async fn frontend_main() {
    let mut sighup = signal(SignalKind::hangup()).expect("tokio sighup handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("tokio sigterm handler");

    // Intervals: the FIRST `.tick()` on a fresh `Interval` fires
    // immediately, so consume it before the loop to avoid an instant
    // "heartbeat" line at startup. After this, both tick at their
    // configured period from "now".
    //
    // Phase-1 heartbeat / watchdog periods are hard-coded constants
    // per Q23 (zero new GUCs in phase 1). Knobs can land per phase
    // if operational experience asks for them.
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
                // Phase 1: nothing to reload yet. Phase ≥3 will
                // reconcile the live listener set against
                // pg_transport.transports here.
                pgrx::log!("pg_transport frontend: SIGHUP received (no-op in phase 1)");
            }
            _ = watchdog.tick() => {
                if postmaster_died() {
                    pgrx::log!("pg_transport frontend: postmaster died, exiting");
                    break;
                }
            }
            _ = heartbeat.tick() => {
                // Phase-1 acceptance criterion (roadmap.md §1).
                pgrx::log!("pg_transport frontend: heartbeat");
            }
        }
    }
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
    let fd =
        unsafe { pg_sys::postmaster_alive_fds[pg_sys::POSTMASTER_FD_WATCH as usize] };
    let mut byte: u8 = 0;
    // SAFETY: reading from a valid, non-blocking pipe fd inherited
    // from the postmaster; the buffer is a single byte on this
    // function's stack.
    let rc = unsafe {
        libc::read(
            fd,
            &mut byte as *mut u8 as *mut libc::c_void,
            1,
        )
    };
    rc == 0
}
