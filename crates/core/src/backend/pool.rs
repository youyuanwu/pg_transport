//! Frontend-side backend pool: single UDS listener + autoscaled slot
//! bgworker fleet with cooperative drain.
//!
//! Designed per
//! [`docs/design/deferred/slot-readiness.md`](../../../../docs/design/deferred/slot-readiness.md):
//!
//! * **Single listener.** The FE binds one well-known UDS path; every
//!   slot BE connects to it; the FE assigns a monotonic `slot_id` on
//!   accept (§2.0).
//! * **Three containers + `Notify`.** Slots transit `in_flight` →
//!   `ready` → `in_flight` (dispatch) → `ready` → `draining` → gone.
//!   Container membership *is* the state; transitions are atomic
//!   moves under the pool lock (§5.7).
//! * **Demand-driven grow + idle-reap shrink.** A `grow_one()` fires
//!   when a handoff arrives with no ready slot; an idle reaper drains
//!   slots that have been ready longer than `IDLE_REAP_AFTER` (§5.2,
//!   §5.3).
//! * **Cooperative drain.** The reaper sends `b'X'` and waits for
//!   `b'A'`; an ack-timeout watchdog falls back to `SHUT_WR` if the
//!   slot is stuck mid-session (§5.3).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use api::{HandoffFuture, HandoffHints, HandoffSink};
use futures::FutureExt;
use indexmap::IndexMap;
use parking_lot::Mutex;
use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
use tokio::io::unix::AsyncFd;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout_at};
use tokio_util::sync::CancellationToken;

use super::fd_pass;
use super::paths;
use crate::guc;

// ---------------------------------------------------------------------------
// Tunables (compile-time per slot-readiness.md §5.1 "keep the
// operator surface small"). Promote to GUCs only with deployment
// evidence.
// ---------------------------------------------------------------------------

/// A slot idle in `ready` for this long is drained by the reaper.
/// Long enough to absorb typical workload lulls; short enough that
/// idle clusters don't squat on bgworker slots.
const IDLE_REAP_AFTER: Duration = Duration::from_secs(60);

/// How often the idle-reaper task wakes to check candidates.
const IDLE_REAPER_TICK: Duration = Duration::from_secs(10);

/// Floor on pool size. 0 means the cluster can fully quiesce. Set
/// higher if first-connection latency matters.
const MIN_WARM_SLOTS: u32 = 0;

/// Per-handoff deadline on `Notify::notified()`. Timeout yields an
/// `Err` to the transport, which the transport turns into a
/// `ErrorResponse("too many connections")` for the client.
const HANDOFF_WAIT: Duration = Duration::from_secs(5);

/// Per-drain deadline before force-close. Set conservatively long
/// because a draining slot may be mid long-running query;
/// force-close on a busy slot resets the client connection.
const DRAIN_ACK_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One connected slot bgworker — its FE-internal id and the
/// `AsyncFd` we use for both `sendmsg(SCM_RIGHTS)` and EPIPE
/// detection. The stream is `Arc`-shared with the per-slot reader
/// task; whichever owner drops last closes our end of the UDS.
struct SlotPeer {
    /// Redundant with the container key, but kept on the struct
    /// for forward-compat (panic-handling code paths, future
    /// per-slot metrics, etc.).
    #[allow(dead_code)]
    slot_id: u64,
    stream: Arc<AsyncFd<StdUnixStream>>,
    last_ready_at: Instant,
}

/// Container state. Container membership *is* the slot state: a
/// slot is in exactly one of `ready`, `in_flight`, `draining` at
/// any moment under the pool lock. `IndexMap` on `ready` gives
/// FIFO fairness (insertion order) with O(1) pop-front via
/// `shift_remove_index(0)`.
struct PoolState {
    ready: IndexMap<u64, SlotPeer>,
    in_flight: HashMap<u64, SlotPeer>,
    draining: HashMap<u64, SlotPeer>,
    slot_id_counter: u64,
}

impl PoolState {
    fn new() -> Self {
        Self {
            ready: IndexMap::new(),
            in_flight: HashMap::new(),
            draining: HashMap::new(),
            slot_id_counter: 0,
        }
    }

    fn next_slot_id(&mut self) -> u64 {
        let id = self.slot_id_counter;
        self.slot_id_counter += 1;
        id
    }

    /// `ready + in_flight + draining` — used by the dispatcher's
    /// grow gate and the SIGHUP ceiling check.
    fn total(&self) -> usize {
        self.ready.len() + self.in_flight.len() + self.draining.len()
    }
}

/// The pool.
///
/// `!Send`: held inside `Rc<dyn HandoffSink>` on the FE LocalSet.
/// `Notify` is `Send + Sync`, `parking_lot::Mutex<PoolState>` is
/// `Send + Sync`, but the `Arc<AsyncFd<UnixStream>>`s registered
/// with the current-thread reactor make the whole graph `!Send`
/// in practice (and our usage assumes it).
pub struct Pool {
    state: Mutex<PoolState>,
    has_ready: Notify,
    /// At-most-one-concurrent-grow gate. Outside `PoolState` so the
    /// dispatcher can CAS without acquiring the pool lock. See
    /// slot-readiness.md §5.2.
    growing: AtomicBool,
}

impl Pool {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(PoolState::new()),
            has_ready: Notify::new(),
            growing: AtomicBool::new(false),
        }
    }
}

// ---------------------------------------------------------------------------
// Bring-up: bind the listener, optionally pre-warm
// ---------------------------------------------------------------------------

/// Bind the FE's single UDS listener at `paths::frontend_socket_path()`.
///
/// Listen backlog is snapshotted from `pg_transport.max_backend_pool_size`
/// at bind time — SIGHUP raising the GUC later doesn't re-bind (the
/// kernel backlog is a transient queue, microseconds-deep in practice;
/// see slot-readiness.md §2.0).
///
/// Sets `chmod 0700` on the parent dir and `chmod 0600` on the
/// socket file itself — phase-2 filesystem-perms posture, same as
/// before.
pub fn bind_listener() -> io::Result<UnixListener> {
    ensure_slot_dir()?;
    let path = paths::frontend_socket_path();

    // Defensive unlink — a leftover socket file from a crashed
    // previous run would cause bind() to EADDRINUSE. ENOENT is the
    // happy path, ignored.
    match fs::remove_file(&path) {
        Ok(()) => pgrx::log!(
            "pg_transport pool: unlinked stale socket {}",
            path.display()
        ),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("unlinking stale socket {}: {e}", path.display()),
            ));
        }
    }

    // tokio's `UnixListener::bind` uses libstd's `bind`, which sets
    // backlog to 1024 by default — fine for our purposes. We
    // accept-loop fast enough that the kernel queue should never
    // accumulate beyond the burst of a single grow.
    let listener = UnixListener::bind(&path)
        .map_err(|e| io::Error::new(e.kind(), format!("binding {}: {e}", path.display())))?;

    // chmod 0600 explicitly — bind's mode arg isn't honoured on all
    // platforms.
    let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    // SAFETY: cpath is a valid C string; mode 0600 is well-defined.
    if unsafe { libc::chmod(cpath.as_ptr(), 0o600) } != 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            format!("chmod 0600 {}: {err}", path.display()),
        ));
    }

    pgrx::log!(
        "pg_transport pool: bound frontend listener at {}",
        path.display()
    );
    Ok(listener)
}

fn ensure_slot_dir() -> io::Result<()> {
    let dir = paths::slot_dir();
    if let Err(e) = fs::create_dir_all(&dir) {
        return Err(io::Error::new(
            e.kind(),
            format!("creating {}: {e}", dir.display()),
        ));
    }
    let cdir = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    // SAFETY: cdir is a valid C string; mode 0700 is well-defined.
    if unsafe { libc::chmod(cdir.as_ptr(), 0o700) } != 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            format!("chmod 0700 {}: {err}", dir.display()),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// accept_loop — long-lived task: assigns slot_ids, parks new BEs in
// `in_flight`, spawns per-slot reader tasks.
// ---------------------------------------------------------------------------

/// Run the FE's accept loop until `shutdown` is cancelled. Spawned
/// once at FE boot.
pub async fn accept_loop(
    listener: UnixListener,
    pool: Rc<Pool>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                pgrx::log!("pg_transport pool: accept_loop shutdown");
                return Ok(());
            }
            r = listener.accept() => {
                let (stream, _peer) = match r {
                    Ok(p) => p,
                    Err(e) => {
                        pgrx::warning!("pg_transport pool: accept error: {e}");
                        continue;
                    }
                };

                // Move out of tokio's UnixListener-managed reactor
                // registration and re-register as our own AsyncFd.
                // into_std() returns a blocking std stream; we set
                // it non-blocking (required by AsyncFd) inside
                // `wrap_for_async`.
                let std_stream = match stream.into_std() {
                    Ok(s) => s,
                    Err(e) => {
                        pgrx::warning!("pg_transport pool: into_std failed on accept: {e}");
                        continue;
                    }
                };
                let async_stream = match fd_pass::wrap_for_async(std_stream) {
                    Ok(a) => Arc::new(a),
                    Err(e) => {
                        pgrx::warning!("pg_transport pool: wrap_for_async failed: {e}");
                        continue;
                    }
                };

                // Allocate slot_id and park in in_flight. The first
                // `b'R'` from this BE will move it to `ready`; same
                // code path as a post-session `b'R'`.
                let slot_id = {
                    let mut st = pool.state.lock();
                    let id = st.next_slot_id();
                    st.in_flight.insert(id, SlotPeer {
                        slot_id: id,
                        stream: Arc::clone(&async_stream),
                        last_ready_at: Instant::now(),
                    });
                    id
                };
                pgrx::log!("pg_transport pool: slot {slot_id} connected");

                // Spawn the per-slot reader. The wrapper
                // catch_unwinds the body so a reader panic still
                // runs cleanup.
                let pool_clone = pool.clone();
                tokio::task::spawn_local(slot_reader_wrapper(
                    slot_id,
                    async_stream,
                    pool_clone,
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-slot reader task
// ---------------------------------------------------------------------------

/// Reader-task wrapper that catch_unwinds the inner reader and
/// always runs `cleanup_orphaned` on exit. Replaces the design's
/// JoinSet-supervisor pattern with per-task panic-handling — same
/// invariant: a dead reader never leaves a slot stuck in
/// `in_flight` or `ready` (slot-readiness.md §1.2 "Reader lifecycle").
async fn slot_reader_wrapper(slot_id: u64, stream: Arc<AsyncFd<StdUnixStream>>, pool: Rc<Pool>) {
    let body = slot_reader(slot_id, Arc::clone(&stream), pool.clone());
    if let Err(panic) = AssertUnwindSafe(body).catch_unwind().await {
        pgrx::warning!("pg_transport pool: slot {slot_id} reader panicked: {panic:?}");
    }
    pool.cleanup_orphaned(slot_id, &stream);
}

/// Read bytes from the BE one at a time; dispatch to `on_ready` /
/// `on_drain_ack`. Exits on EOF, error, unknown opcode, or `b'A'`.
///
/// The FE side reads a different opcode set than the BE side
/// (`b'R'` / `b'A'` vs `b'\0'` / `b'X'`), and the FE never receives
/// SCM_RIGHTS from a slot, so this is a thin direct `recv(MSG_DONTWAIT)`
/// rather than going through `fd_pass::recv_ctrl_async`.
async fn slot_reader(slot_id: u64, stream: Arc<AsyncFd<StdUnixStream>>, pool: Rc<Pool>) {
    loop {
        match read_one_be_opcode(&stream).await {
            Ok(fd_pass::OP_READY) => {
                pool.on_ready(slot_id);
            }
            Ok(fd_pass::OP_DRAIN_ACK) => {
                pool.on_drain_ack(slot_id);
                return;
            }
            Ok(op) => {
                pgrx::warning!(
                    "pg_transport pool: slot {slot_id} sent invalid opcode \
                     {op:#04x}; closing reader"
                );
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                pgrx::log!("pg_transport pool: slot {slot_id} closed UDS; reader exiting");
                return;
            }
            Err(e) => {
                pgrx::log!("pg_transport pool: slot {slot_id} reader recv error: {e}; closing");
                return;
            }
        }
    }
}

/// FE-side single-byte non-blocking recv on an `AsyncFd<UnixStream>`.
/// Returns `UnexpectedEof` on clean EOF; the caller treats this as
/// "BE exited" and runs cleanup.
async fn read_one_be_opcode(stream: &AsyncFd<StdUnixStream>) -> io::Result<u8> {
    loop {
        let mut guard = stream.readable().await?;
        match guard.try_io(|inner| {
            let fd = inner.get_ref().as_raw_fd();
            let mut buf = [0u8; 1];
            // SAFETY: recv on a valid socket fd with a 1-byte buffer.
            let n = unsafe {
                libc::recv(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    1,
                    libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            Ok(buf[0])
        }) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

impl Pool {
    /// `b'R'` from a BE: in_flight → ready, stamp `last_ready_at`,
    /// wake one pending dispatcher.
    fn on_ready(self: &Rc<Self>, slot_id: u64) {
        let mut st = self.state.lock();
        let Some(mut peer) = st.in_flight.remove(&slot_id) else {
            // Either the slot was concurrently moved to `draining`
            // (b'X' beat the b'R' to the lock) or the slot is
            // already in `ready` somehow (would be a bug). No-op
            // either way; the b'A' will arrive and finish cleanup.
            return;
        };
        peer.last_ready_at = Instant::now();
        st.ready.insert(slot_id, peer);
        drop(st);
        self.has_ready.notify_one();
    }

    /// `b'A'` from a BE: draining → gone.
    fn on_drain_ack(&self, slot_id: u64) {
        let mut st = self.state.lock();
        if let Some(peer) = st.draining.remove(&slot_id) {
            let elapsed = peer.last_ready_at.elapsed();
            pgrx::log!(
                "pg_transport pool: slot {slot_id} drained in {} ms",
                elapsed.as_millis()
            );
        }
        // else: watchdog or another path already cleaned up.
    }

    /// Reader exit cleanup. Removes the slot from whichever
    /// container it's in. For `in_flight` / `ready` exits this is
    /// the recovery path for a crashed BE; the dispatcher's next
    /// `send_fd_async` on this stream would fail with EPIPE
    /// anyway.
    fn cleanup_orphaned(&self, slot_id: u64, stream: &Arc<AsyncFd<StdUnixStream>>) {
        let mut st = self.state.lock();
        let was = if st.ready.shift_remove(&slot_id).is_some() {
            Some("ready")
        } else if st.in_flight.remove(&slot_id).is_some() {
            Some("in_flight")
        } else if st.draining.remove(&slot_id).is_some() {
            Some("draining")
        } else {
            None
        };
        drop(st);
        if let Some(c) = was {
            pgrx::log!("pg_transport pool: slot {slot_id} reader exit (was {c}); cleaned up");
        }
        // Force-close our end so any pending send_fd_async on a
        // peer Arc gets EPIPE rather than hanging. SHUT_WR on the
        // FE side; the kernel's other side teardown is automatic
        // once both Arc refs drop.
        let fd = stream.get_ref().as_raw_fd();
        // SAFETY: shutdown() on a valid socket fd is well-defined.
        unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    }
}

// ---------------------------------------------------------------------------
// Dispatch (HandoffSink)
// ---------------------------------------------------------------------------

impl HandoffSink for Pool {
    fn handoff(&self, fd: OwnedFd, _hints: HandoffHints) -> HandoffFuture<'_> {
        Box::pin(async move {
            // Fast path: an existing slot is in `ready`.
            if let Some(stream) = self.try_pop_ready() {
                return send_fd_to(stream, fd, "fast-path").await;
            }

            // No slot ready. Kick off a grow if we're under the
            // ceiling and not already growing. AtomicBool gate;
            // RAII guard resets it on every exit path of the spawn.
            // The grow itself is fire-and-forget — the new slot's
            // first `b'R'` lands in `ready` and fires
            // `has_ready.notify_one()` just like any other slot.
            let max = guc::max_backend_pool_size();
            let under_ceiling = self.state.lock().total() < max as usize;
            if under_ceiling
                && self
                    .growing
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                // load_dynamic is a microsecond syscall; doing it
                // inline (no spawn) keeps the AtomicBool window
                // tight and avoids spawning a task purely to do one
                // syscall. RAII guard so a panic still resets the
                // flag.
                let _guard = scopeguard::guard((), |_| {
                    self.growing.store(false, Ordering::Release);
                });
                match grow_one() {
                    Ok(()) => pgrx::log!("pg_transport pool: grow_one issued"),
                    Err(e) => pgrx::warning!("pg_transport pool: grow_one failed: {e}"),
                }
                // _guard drops here, resets `growing`.
            }

            // Wait for any slot to enter `ready` — could be a busy
            // existing slot finishing its session, or the new slot
            // we just spawned, or any other concurrent grow. The
            // dispatcher doesn't distinguish.
            let deadline = tokio::time::Instant::now() + HANDOFF_WAIT;
            loop {
                let notified = self.has_ready.notified();
                match timeout_at(deadline, notified).await {
                    Ok(()) => {
                        if let Some(stream) = self.try_pop_ready() {
                            return send_fd_to(stream, fd, "slow-path").await;
                        }
                        // Spurious wake (another handoff took the
                        // slot first); keep waiting.
                    }
                    Err(_) => {
                        anyhow::bail!(
                            "pg_transport pool: no slot became ready within {:?}",
                            HANDOFF_WAIT
                        );
                    }
                }
            }
        })
    }
}

impl Pool {
    /// Atomic ready→in_flight move under the lock; returns the
    /// stream `Arc` for the dispatcher to `send_fd_async` to.
    fn try_pop_ready(&self) -> Option<Arc<AsyncFd<StdUnixStream>>> {
        let mut st = self.state.lock();
        let (slot_id, peer) = st.ready.shift_remove_index(0)?;
        let stream = Arc::clone(&peer.stream);
        st.in_flight.insert(slot_id, peer);
        Some(stream)
    }
}

async fn send_fd_to(
    stream: Arc<AsyncFd<StdUnixStream>>,
    fd: OwnedFd,
    path_label: &'static str,
) -> anyhow::Result<()> {
    fd_pass::send_fd_async(&stream, fd.as_raw_fd())
        .await
        .map_err(|e| anyhow::anyhow!("pg_transport pool ({path_label}): send_fd: {e}"))?;
    // `fd` dropped here — closes our local fd reference. The slot
    // has its own (kernel-dup'd) copy from the SCM_RIGHTS delivery.
    drop(fd);
    Ok(())
}

// ---------------------------------------------------------------------------
// Grow on demand
// ---------------------------------------------------------------------------

/// Spawn one new slot bgworker via `BackgroundWorkerBuilder::load_dynamic`.
///
/// Synchronous and returns as soon as `load_dynamic` enqueues the
/// worker; does not wait for the BE to actually connect to our
/// listener. The BE's first `b'R'` will arrive on `accept_loop` /
/// `slot_reader` and fire `has_ready.notify_one()` (slot-readiness.md
/// §5.2).
///
/// Failures:
/// * `Err("bgworker table full")` — `max_worker_processes` exhausted.
///   The dispatcher logs WARNING and falls back to waiting on
///   existing slots until `HANDOFF_WAIT`.
fn grow_one() -> Result<(), &'static str> {
    BackgroundWorkerBuilder::new("pg_transport slot")
        .set_type("pg_transport_slot")
        .set_library("pg_transport")
        .set_function("pg_transport_slot_main")
        // No .set_argument — single-listener design, the BE doesn't
        // need a slot_id; the FE assigns one on accept.
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        // No .set_restart_time — dynamic slots are
        // BGW_NEVER_RESTART by default. If a slot dies, the
        // dispatcher will grow_one again on the next saturation
        // event.
        .enable_spi_access()
        .load_dynamic()
        .map_err(|_| "bgworker table full (raise max_worker_processes)")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Idle reaper — long-lived task
// ---------------------------------------------------------------------------

/// Periodically drain slots that have been ready longer than
/// `IDLE_REAP_AFTER`. Spawned once at FE boot.
pub async fn idle_reaper(pool: Rc<Pool>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                pgrx::log!("pg_transport pool: idle_reaper shutdown");
                return;
            }
            _ = sleep(IDLE_REAPER_TICK) => {
                reap_once(&pool);
            }
        }
    }
}

fn reap_once(pool: &Rc<Pool>) {
    let mut to_drain: Vec<u64> = Vec::new();
    {
        let st = pool.state.lock();
        // Snapshot `now` after acquiring the lock so that any
        // starvation on lock acquisition doesn't yield a stale
        // timestamp that would reap recently-active slots.
        let now = Instant::now();
        let candidates: Vec<u64> = st
            .ready
            .iter()
            // Defensive: if a clock skew or task-starvation glitch
            // produces last_ready_at > now, treat the slot as fresh.
            .filter(|(_, p)| {
                now > p.last_ready_at && now.duration_since(p.last_ready_at) > IDLE_REAP_AFTER
            })
            .map(|(id, _)| *id)
            .collect();
        // Respect MIN_WARM_SLOTS floor: keep at least this many
        // slots in `ready`. If we'd drop below the floor by
        // draining all candidates, skip the first `keep` of them.
        let post_reap = st.ready.len().saturating_sub(candidates.len());
        let keep = (MIN_WARM_SLOTS as usize).saturating_sub(post_reap);
        for (i, slot_id) in candidates.into_iter().enumerate() {
            if i < keep {
                continue;
            }
            to_drain.push(slot_id);
        }
    }
    // Drain outside the iteration; request_drain reacquires the
    // lock to do the ready → draining move.
    for slot_id in to_drain {
        request_drain(pool, slot_id);
    }
}

// ---------------------------------------------------------------------------
// Cooperative drain (reaper / SIGHUP ceiling)
// ---------------------------------------------------------------------------

/// Move slot from `ready` → `draining`, send `b'X'`, spawn the
/// ack-timeout watchdog. The reader will see `b'A'` next and call
/// `on_drain_ack`. See slot-readiness.md §5.3.
fn request_drain(pool: &Rc<Pool>, slot_id: u64) {
    let stream = {
        let mut st = pool.state.lock();
        let Some(peer) = st.ready.shift_remove(&slot_id) else {
            return; // raced with another path
        };
        let stream = Arc::clone(&peer.stream);
        st.draining.insert(slot_id, peer);
        stream
    };
    // The send is non-blocking and microseconds; doing it outside
    // the lock keeps the critical section minimal. The reader is
    // already watching the same stream for `b'A'`.
    let fd = stream.get_ref().as_raw_fd();
    if let Err(e) = fd_pass::send_drain_request_nonblocking(fd) {
        pgrx::warning!(
            "pg_transport pool: slot {slot_id} drain request send failed: {e} \
             (will rely on ack-timeout watchdog)"
        );
    } else {
        pgrx::log!("pg_transport pool: sent drain request to slot {slot_id}");
    }

    // Spawn the ack-timeout watchdog. If `b'A'` arrives within
    // DRAIN_ACK_TIMEOUT, on_drain_ack already removed the slot;
    // the watchdog sees `draining.remove() == None` and no-ops.
    let pool_clone = pool.clone();
    let stream_clone = Arc::clone(&stream);
    tokio::task::spawn_local(async move {
        sleep(DRAIN_ACK_TIMEOUT).await;
        let mut st = pool_clone.state.lock();
        if st.draining.remove(&slot_id).is_some() {
            // Ack never arrived. Force-close so the BE's next
            // `recv_ctrl_async` sees EOF and exits via the Eof
            // branch. The reader task observes the same EOF and
            // calls `cleanup_orphaned`.
            let fd = stream_clone.get_ref().as_raw_fd();
            // SAFETY: shutdown on a valid socket fd is well-defined.
            unsafe { libc::shutdown(fd, libc::SHUT_WR) };
            pgrx::warning!("pg_transport pool: slot {slot_id} drain ack timeout; force-closed");
        }
    });
}

// ---------------------------------------------------------------------------
// SIGHUP ceiling — drain excess slots if max_backend_pool_size was lowered
// ---------------------------------------------------------------------------

/// Called from the FE's SIGHUP handler. If the new
/// `max_backend_pool_size` is lower than `total()`, drain
/// `(total - max)` slots starting from `ready` (idle first).
///
/// Note: this does not drain `in_flight` slots — they're actively
/// serving sessions; aborting them mid-flight would reset client
/// connections. Excess in_flight slots are drained naturally as
/// they finish their current session (the dispatcher's grow gate
/// will refuse to add new slots while at the new lower ceiling).
pub fn sighup_reconcile(pool: &Rc<Pool>) {
    let max = guc::max_backend_pool_size() as usize;
    let to_drain: Vec<u64> = {
        let st = pool.state.lock();
        let total = st.total();
        if total <= max {
            return;
        }
        let excess = total - max;
        st.ready.keys().take(excess).copied().collect()
    };
    if !to_drain.is_empty() {
        pgrx::log!(
            "pg_transport pool: SIGHUP — max_backend_pool_size lowered, \
             draining {} idle slot(s)",
            to_drain.len()
        );
        for slot_id in to_drain {
            request_drain(pool, slot_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Optional pre-warm at FE boot
// ---------------------------------------------------------------------------

/// Fire `MIN_WARM_SLOTS` `grow_one()` calls in parallel at FE boot.
/// Fire-and-forget: does not wait for slots to actually connect.
/// Failures are logged at WARNING and reduce the warm pool by one;
/// the dispatcher will fall back to cold-grow on the first arriving
/// connection.
//
// `clippy::reversed_empty_ranges` fires when `MIN_WARM_SLOTS == 0`
// (the current default), because the range `0..0` is statically
// empty. The constant is intentionally tunable to a non-zero value
// without other code changes, so we silence the lint rather than
// gate the loop on the constant being non-zero.
#[allow(clippy::reversed_empty_ranges)]
pub fn pre_warm() {
    for i in 0..MIN_WARM_SLOTS {
        if let Err(e) = grow_one() {
            pgrx::warning!("pg_transport pool: pre-warm slot {i} grow_one failed: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// FE→BE reader: dedicated impl that distinguishes `b'R'` from `b'A'`.
// ---------------------------------------------------------------------------
//
// `fd_pass::recv_ctrl_async` is BE-oriented (expects FE→BE opcodes).
// The FE side reads a different opcode set, so we use the local
// `read_one_be_opcode` helper above. The reader logic lives in
// `slot_reader` near the top of this file alongside `slot_reader_wrapper`.
