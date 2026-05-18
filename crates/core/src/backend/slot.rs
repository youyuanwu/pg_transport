//! Slot bgworker entry point — the backend side of the v0 handoff
//! path.
//!
//! Per [backend-handoff.md §3](../../../../docs/design/backend-handoff.md):
//! the slot runner runs on the bgworker's main thread (the only
//! thread); the loop body is sync. A small single-threaded tokio
//! runtime is built once per bgworker and entered via `block_on`
//! per handoff to drive the wire's async machinery (pgwire).

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;

use super::fd_pass;
use super::paths;
use crate::wire::{Wire, WireCtx, pgwire_v3::PgwireV3};

/// Slot bgworker C entry point.
///
/// Registered via `BackgroundWorkerBuilder::set_function(
/// "pg_transport_slot_main")` in [`crate::_PG_init`]. The slot id is
/// passed via `bgw_main_arg` (a 64-bit `Datum`); we extract it as
/// `u32`.
///
/// `#[unsafe(no_mangle)]` keeps the symbol callable by PG via `dlsym`.
/// Workspace sets `panic = "unwind"` (Q9 re-resolved via
/// [Q24](../../../../docs/design/roadmap.md)). An uncaught panic
/// unwinds out of this function; `#[pg_guard]` catches it at the C
/// boundary and emits an ereport. The postmaster respawns the slot
/// after `bgw_restart_time` (1 s).
#[unsafe(no_mangle)]
#[pg_guard]
pub extern "C-unwind" fn pg_transport_slot_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // SAFETY: `bgw_main_arg` is a plain 64-bit value; the FE-side
    // registration (see crate::_PG_init) put the slot id there as
    // `(slot_id as i32).into_datum()`. `Datum::value()` returns the
    // raw 64-bit payload.
    let slot_id = arg.value() as u32;

    pgrx::log!("pg_transport slot {slot_id}: starting");

    if let Err(e) = run_slot(slot_id) {
        // WARNING is sufficient: the slot is exiting anyway and the
        // postmaster will respawn it. We deliberately don't escalate
        // to ereport(ERROR) which would just turn into a panic that
        // pg_guard catches and re-emits as the same ereport —
        // pointless extra hop.
        pgrx::warning!("pg_transport slot {slot_id} exited: {e}");
    } else {
        pgrx::log!("pg_transport slot {slot_id}: clean exit");
    }
}

fn run_slot(slot_id: u32) -> io::Result<()> {
    let path = paths::slot_socket_path(slot_id);

    // Connect to the frontend's per-slot listener. The FE creates
    // the listener in its own bgworker entry (see
    // crate::backend::pool::BackendPool::start). FE and slots boot
    // in parallel under static registration; retry until the FE has
    // bound, or until we hit a hard ceiling.
    //
    // Per [frontend-handoff.md §2.1](../../../../docs/design/frontend-handoff.md)
    // the connect side just retries with short backoff to tolerate
    // the startup race.
    let stream = connect_with_retry(&path, slot_id)?;
    let stream_fd = stream.as_raw_fd();

    pgrx::log!(
        "pg_transport slot {slot_id}: connected to {} (wire={})",
        path.display(),
        PgwireV3::name()
    );

    // Per-bgworker tokio runtime, reused across handoffs (NOT
    // rebuilt per handoff — see backend-handoff.md §3). Rc so it
    // can be cloned into each WireCtx; we don't need Send because
    // the slot is single-threaded.
    let rt = Rc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| io::Error::other(format!("tokio runtime: {e}")))?,
    );

    let mut handoffs: u64 = 0;
    loop {
        // Cheap shutdown check between handoffs. The slot is sync
        // outside of `block_on`, so we poll PG's signal-driven flag
        // rather than `tokio::signal`.
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_transport slot {slot_id}: SIGTERM, exiting");
            return Ok(());
        }

        // Blocking recvmsg. Returns Ok(None) on clean EOF (FE closed
        // its end); Ok(Some(fd)) on a handoff; Err on socket error.
        //
        // EINTR (e.g. SIGTERM hitting mid-recvmsg) returns `Err`
        // with `ErrorKind::Interrupted`; we treat that as a re-loop
        // so the sigterm_received check above fires next iteration.
        match fd_pass::recv_fd(stream_fd) {
            Ok(None) => {
                pgrx::log!("pg_transport slot {slot_id}: peer closed control socket, exiting");
                return Ok(());
            }
            Ok(Some(fd)) => {
                handoffs += 1;
                pgrx::log!(
                    "pg_transport slot {slot_id}: received fd {} (handoff #{handoffs})",
                    fd.as_raw_fd()
                );
                let ctx = WireCtx { rt: rt.clone() };
                if let Err(e) = PgwireV3::run(fd, ctx) {
                    pgrx::warning!("pg_transport slot {slot_id}: wire run failed: {e}");
                }
                // Phase 9 lands `reset_per_handoff_state()` here.
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

fn connect_with_retry(path: &std::path::Path, slot_id: u32) -> io::Result<UnixStream> {
    // Total budget: 30 retries × 100 ms = 3 s. Enough to ride out
    // the postmaster spawning FE and slots concurrently; if the FE
    // really isn't coming up that's a hard error worth aborting on.
    const MAX_RETRIES: u32 = 30;
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
                        "pg_transport slot {slot_id}: waiting for FE listener at {}",
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
        "pg_transport slot {slot_id}: FE listener at {} never appeared",
        path.display()
    )))
}
