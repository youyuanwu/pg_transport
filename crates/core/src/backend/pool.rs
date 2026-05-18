//! Frontend-side backend pool: per-slot UDS listeners + the peer
//! `UnixStream` we use to `sendmsg(SCM_RIGHTS)` handoffs.
//!
//! Per [frontend-handoff.md §2.1](../../../../docs/design/frontend-handoff.md),
//! the FE creates the per-slot listeners and waits for each slot
//! bgworker to `connect()`. Once accepted, the per-slot peer stream
//! is long-lived; we only drop it on shutdown.
//!
//! Implements [`api::HandoffSink`] so [`api::HandoffHandle`] can
//! dispatch fds into the pool from transport accept loops. Phase 3
//! uses a simple round-robin: every handoff goes to the next slot
//! modulo `pool_size`. Phase ≥ 7 may grow this into per-slot load
//! tracking or affinity hashing.

use std::cell::Cell;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::PathBuf;
use std::time::Duration;

use api::{HandoffHints, HandoffSink};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

use super::fd_pass;
use super::paths;

/// One peer entry in the pool. In phase 2 we only need the long-lived
/// `UnixStream` to the slot; phase ≥ 4 will add bookkeeping (state,
/// in-flight handoff count, etc.).
pub struct SlotPeer {
    pub slot_id: u32,
    /// The connected stream to the slot bgworker. Used as both the
    /// transport for SCM_RIGHTS sends *and* (eventually) the EPIPE
    /// detector — when the slot dies, our next send fails.
    pub stream: UnixStream,
}

/// Frontend-side pool. Owns the per-slot listener-and-peer pair.
///
/// `Cell<u32>` for the round-robin cursor: the pool is held in an
/// `Rc` inside [`api::HandoffHandle`], so `HandoffSink::handoff`
/// gets `&self` (not `&mut self`). The cursor is single-threaded
/// (LocalSet) so `Cell` is sufficient — no need for `RefCell` or
/// atomics.
pub struct BackendPool {
    pub peers: Vec<SlotPeer>,
    next_slot: Cell<u32>,
}

impl BackendPool {
    /// Bind `pool_size` per-slot listeners, then wait for every slot
    /// bgworker to connect. Returns once all peers are established.
    ///
    /// `accept_timeout` bounds how long we wait for any single slot
    /// to connect — under the static-registration model the
    /// postmaster spawns slots concurrently with the FE, so they
    /// should connect within a couple of seconds at most.
    pub async fn start(pool_size: u32, accept_timeout: Duration) -> io::Result<Self> {
        ensure_slot_dir()?;

        // Step 1: bind every listener BEFORE accepting any. This
        // matches the design's pre-register pattern (frontend-handoff.md
        // §2.1) — slot bgworkers may already be retrying connect()s
        // and we want them to find a listener as fast as possible.
        let mut listeners: Vec<(u32, PathBuf, UnixListener)> =
            Vec::with_capacity(pool_size as usize);
        for slot_id in 0..pool_size {
            let path = paths::slot_socket_path(slot_id);

            // Defensive unlink — leftover socket file from a
            // crashed previous run would cause bind() to EADDRINUSE.
            // ENOENT is the expected happy path here, ignored.
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

            let listener = UnixListener::bind(&path).map_err(|e| {
                io::Error::new(e.kind(), format!("binding {}: {e}", path.display()))
            })?;

            // chmod 0600 explicitly — bind's mode arg isn't honoured
            // on all platforms (frontend-handoff.md §2.1).
            //
            // SAFETY: `libc::chmod` takes a NUL-terminated path; we
            // construct one from PathBuf via CString. The path is
            // valid for the duration of the call.
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
                "pg_transport pool: bound slot {slot_id} listener at {}",
                path.display()
            );
            listeners.push((slot_id, path, listener));
        }

        // Step 2: accept on each. The slot bgworkers connect with
        // 100 ms backoff for up to 3 s (see backend::slot::connect_with_retry),
        // so a 5 s per-slot timeout here has comfortable margin.
        let mut peers = Vec::with_capacity(pool_size as usize);
        for (slot_id, path, listener) in listeners {
            let accept_fut = listener.accept();
            let (stream, _addr) = match timeout(accept_timeout, accept_fut).await {
                Ok(Ok(pair)) => pair,
                Ok(Err(e)) => {
                    return Err(io::Error::new(
                        e.kind(),
                        format!("accept on slot {slot_id} ({}): {e}", path.display()),
                    ));
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "slot {slot_id} did not connect to {} within {:?}",
                            path.display(),
                            accept_timeout
                        ),
                    ));
                }
            };
            // Phase 7 will add `verify_peer(stream)` here per
            // frontend-handoff.md §2.1 (SO_PEERCRED check). For
            // phase 2 we trust the path (chmod 0600 + filesystem
            // permissions on the parent dir give a first-cut
            // protection).
            pgrx::log!("pg_transport pool: slot {slot_id} connected");
            peers.push(SlotPeer { slot_id, stream });
        }

        pgrx::log!("pg_transport pool: all {} slots connected", peers.len());
        Ok(Self {
            peers,
            next_slot: Cell::new(0),
        })
    }

    /// Round-robin pick the next slot, wrap modulo `peers.len()`.
    fn pick_slot(&self) -> &SlotPeer {
        let len = self.peers.len() as u32;
        let i = self.next_slot.get() % len;
        self.next_slot.set(i.wrapping_add(1));
        &self.peers[i as usize]
    }
}

impl HandoffSink for BackendPool {
    /// Round-robin dispatch the fd to a slot via SCM_RIGHTS. Returns
    /// once the kernel has accepted the `sendmsg` (Q20 semantics:
    /// `Ok(())` means "kernel accepted", not "backend in hand").
    ///
    /// Synchronous despite running inside a tokio task: SCM_RIGHTS
    /// plus one placeholder byte will never block the kernel socket
    /// buffer in practice. Phase ≥ 4 may revisit if the wire layer
    /// starts heavy traffic.
    ///
    /// `_hints` is captured for forward-compat but not yet read —
    /// the slot's null wire (phase 3) is hints-agnostic; phase ≥ 4
    /// will plumb hints into a per-handoff metadata frame.
    fn handoff(&self, fd: OwnedFd, _hints: HandoffHints) -> anyhow::Result<()> {
        if self.peers.is_empty() {
            anyhow::bail!("pg_transport pool: no slots available");
        }
        let peer = self.pick_slot();
        fd_pass::send_fd(peer.stream.as_raw_fd(), fd.as_raw_fd())
            .map_err(|e| anyhow::anyhow!("sendmsg to slot {}: {e}", peer.slot_id))?;
        // `fd` dropped here — closes our local fd reference.
        // The slot has its own (kernel-dup'd) copy from the SCM_RIGHTS
        // delivery.
        Ok(())
    }
}

fn ensure_slot_dir() -> io::Result<()> {
    let dir = paths::slot_dir();
    if let Err(e) = fs::create_dir_all(&dir) {
        return Err(io::Error::new(
            e.kind(),
            format!("creating {}: {e}", dir.display()),
        ));
    }
    // Restrictive parent-dir permissions: only the PG user can
    // enter, so the 0600 sockets inside are doubly protected.
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
