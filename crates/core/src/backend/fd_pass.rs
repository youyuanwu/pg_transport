//! Low-level UDS control plane for the FE↔BE slot protocol.
//!
//! Wire format (single-byte opcodes; only `b'\0'` carries SCM_RIGHTS):
//!
//! | Direction | Opcode  | Meaning                  | Ancillary       |
//! |-----------|---------|--------------------------|-----------------|
//! | FE → BE   | `b'\0'` | fd handoff               | SCM_RIGHTS(fd)  |
//! | FE → BE   | `b'X'`  | drain request            | none            |
//! | BE → FE   | `b'R'`  | ready for next handoff   | none            |
//! | BE → FE   | `b'A'`  | drain ack ("exiting")    | none            |
//!
//! All single-byte writes are atomic on Linux SOCK_STREAM (see
//! [`docs/design/deferred/slot-readiness.md`](../../../../docs/design/deferred/slot-readiness.md)
//! §2). Helpers come in two flavours:
//!
//! * **`*_nonblocking`** — drop to libc with `MSG_DONTWAIT`. These
//!   are what the async wrappers call inside `AsyncFd::try_io`.
//!   May also be called directly under the pool lock when a tiny
//!   guaranteed-non-blocking syscall is needed inside a critical
//!   section (see §5.3 lock-vs-async note).
//! * **`*_async`** — `AsyncFd<UnixStream>` wrapper that
//!   `readable()`/`writable()`-guards the loop. Used everywhere the
//!   caller is on a tokio runtime and may need cancellation.
//!
//! The SCM_RIGHTS cmsg space (`CMSG_SPACE(sizeof(int))` ≈ 20 bytes
//! on Linux x86_64) is sized as a 64-byte stack array; safe
//! over-estimate, single stack allocation.

use std::io;
use std::mem;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::raw::c_int;
use std::os::unix::net::UnixStream;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

// ---------------------------------------------------------------------------
// Opcodes (1 byte each)
// ---------------------------------------------------------------------------

/// FE → BE: prefix byte before a SCM_RIGHTS fd handoff. The kernel
/// requires the iovec be non-empty for ancillary data delivery; we
/// use NUL so we can distinguish handoffs from drain requests by
/// opcode without parsing the cmsg list first.
const OP_FD_HANDOFF: u8 = 0;

/// FE → BE: drain request. The BE finishes its current session (if
/// any), frees per-slot state, replies with `OP_DRAIN_ACK`, and
/// exits.
const OP_DRAIN_REQUEST: u8 = b'X';

/// BE → FE: ready for the next handoff. Sent once after connect
/// and once after every `wire::run` returns. Crate-public so the
/// FE-side reader in `pool.rs` can dispatch on it.
pub(crate) const OP_READY: u8 = b'R';

/// BE → FE: drain ack. Sent in response to `OP_DRAIN_REQUEST`
/// after per-slot cleanup; the BE exits immediately afterward.
/// Crate-public for the FE-side reader.
pub(crate) const OP_DRAIN_ACK: u8 = b'A';

// ---------------------------------------------------------------------------
// CtrlMsg — parsed control message
// ---------------------------------------------------------------------------

/// One UDS control message as observed by the BE-side `recv_ctrl*`.
#[derive(Debug)]
pub enum CtrlMsg {
    /// `OP_FD_HANDOFF` byte + SCM_RIGHTS — a client fd to serve.
    FdHandoff(OwnedFd),
    /// `OP_DRAIN_REQUEST` byte, no ancillary — exit cleanly after
    /// replying with `OP_DRAIN_ACK`.
    DrainRequest,
    /// Clean EOF — the peer (FE) closed its end. Treat as drain.
    Eof,
}

// ---------------------------------------------------------------------------
// FE → BE (handoff + drain request)
// ---------------------------------------------------------------------------

/// Send `fd_to_send` to the peer of `stream_fd` as a SCM_RIGHTS
/// handoff. Non-blocking (`MSG_DONTWAIT` + `MSG_NOSIGNAL`).
///
/// Wire format: one byte (`OP_FD_HANDOFF` = `0x00`) of regular data
/// + SCM_RIGHTS cmsg carrying the fd.
///
/// Returns `Err` with `WouldBlock` if the kernel buffer is full
/// (vanishingly rare for our 1-byte messages but the async wrapper
/// loops on this).
pub fn send_fd_nonblocking(stream_fd: RawFd, fd_to_send: RawFd) -> io::Result<()> {
    let buf = [OP_FD_HANDOFF; 1];
    let mut iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cmsg_buf = [0u8; 64];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    // SAFETY: CMSG_SPACE is purely arithmetic.
    msg.msg_controllen = unsafe { libc::CMSG_SPACE(mem::size_of::<c_int>() as u32) as _ };

    // SAFETY: msghdr fully initialised; cmsg writes are inside cmsg_buf.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::other(
                "CMSG_FIRSTHDR returned null — cmsg_buf too small",
            ));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<c_int>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut c_int, fd_to_send);

        let n = libc::sendmsg(stream_fd, &msg, libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// FE-side: ask the peer slot to drain. Non-blocking single byte.
/// The slot is expected to reply with `OP_DRAIN_ACK` and exit. See
/// [`docs/design/deferred/slot-readiness.md`](../../../../docs/design/deferred/slot-readiness.md)
/// §5.3.
pub fn send_drain_request_nonblocking(stream_fd: RawFd) -> io::Result<()> {
    write_opcode(stream_fd, OP_DRAIN_REQUEST)
}

// ---------------------------------------------------------------------------
// BE → FE (ready + drain ack)
// ---------------------------------------------------------------------------

/// BE-side: tell the FE we're ready for the next handoff.
/// Non-blocking single byte (`OP_READY`). Kept for symmetry with the
/// other non-blocking helpers; current callers go through
/// [`send_ready_byte_async`].
#[allow(dead_code)]
pub fn send_ready_byte_nonblocking(stream_fd: RawFd) -> io::Result<()> {
    write_opcode(stream_fd, OP_READY)
}

/// BE-side: acknowledge a drain request. Non-blocking single byte
/// (`OP_DRAIN_ACK`); the slot exits immediately afterward. Kept
/// for symmetry; current callers go through
/// [`send_drain_ack_async`].
#[allow(dead_code)]
pub fn send_drain_ack_nonblocking(stream_fd: RawFd) -> io::Result<()> {
    write_opcode(stream_fd, OP_DRAIN_ACK)
}

fn write_opcode(stream_fd: RawFd, opcode: u8) -> io::Result<()> {
    let buf = [opcode];
    // SAFETY: send() with a fixed buffer and well-known fd.
    let n = unsafe {
        libc::send(
            stream_fd,
            buf.as_ptr() as *const libc::c_void,
            1,
            libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT,
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    // The kernel never short-writes a single-byte SOCK_STREAM send;
    // n == 0 isn't a documented outcome either. Treat n != 1 as a
    // logic error so we notice if any platform breaks our atomicity
    // assumption.
    debug_assert_eq!(n, 1, "single-byte UDS send must be atomic");
    Ok(())
}

// ---------------------------------------------------------------------------
// recv_ctrl — BE-side
// ---------------------------------------------------------------------------

/// Receive one control message from the peer of `stream_fd`,
/// non-blocking. Returns `Err(WouldBlock)` if no data is buffered;
/// the async wrapper retries on the next `readable()` wake.
///
/// Returns:
/// * `Ok(CtrlMsg::FdHandoff(fd))` — `OP_FD_HANDOFF` byte + SCM_RIGHTS.
/// * `Ok(CtrlMsg::DrainRequest)` — `OP_DRAIN_REQUEST` byte, no
///   ancillary.
/// * `Ok(CtrlMsg::Eof)` — clean EOF (FE closed its end).
/// * `Err(io::ErrorKind::InvalidData)` — wire violation (unknown
///   opcode, or `OP_FD_HANDOFF` without an accompanying SCM_RIGHTS
///   cmsg).
pub fn recv_ctrl_nonblocking(stream_fd: RawFd) -> io::Result<CtrlMsg> {
    let mut buf = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: 1,
    };
    let mut cmsg_buf = [0u8; 64];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    // SAFETY: msghdr fully initialised; recvmsg is a syscall.
    let n = unsafe { libc::recvmsg(stream_fd, &mut msg, libc::MSG_DONTWAIT) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Ok(CtrlMsg::Eof);
    }
    debug_assert_eq!(n, 1, "single-byte UDS recv must be atomic");

    match buf[0] {
        OP_FD_HANDOFF => {
            // Walk the cmsg list looking for the SCM_RIGHTS entry.
            // SAFETY: msg.msg_control points to cmsg_buf; CMSG macros
            // walk inside the controllen we set.
            unsafe {
                let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
                while !cmsg.is_null() {
                    if (*cmsg).cmsg_level == libc::SOL_SOCKET
                        && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                    {
                        let fd = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg) as *const c_int);
                        return Ok(CtrlMsg::FdHandoff(OwnedFd::from_raw_fd_checked(fd)?));
                    }
                    cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "OP_FD_HANDOFF without SCM_RIGHTS cmsg",
            ))
        }
        OP_DRAIN_REQUEST => Ok(CtrlMsg::DrainRequest),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown control opcode {other:#04x}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// AsyncFd wrappers
// ---------------------------------------------------------------------------

/// Loop on `AsyncFd::readable()` + `try_io` over the non-blocking
/// libc helper. Spurious wakes (`WouldBlock` from `try_io`) just
/// loop back to `readable().await`.
pub async fn recv_ctrl_async(stream: &AsyncFd<UnixStream>) -> io::Result<CtrlMsg> {
    loop {
        let mut guard = stream.readable().await?;
        match guard.try_io(|inner| recv_ctrl_nonblocking(inner.get_ref().as_raw_fd())) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

/// Loop on `AsyncFd::writable()` + `try_io` over `send_fd_nonblocking`.
pub async fn send_fd_async(stream: &AsyncFd<UnixStream>, fd_to_send: RawFd) -> io::Result<()> {
    loop {
        let mut guard = stream.writable().await?;
        match guard.try_io(|inner| send_fd_nonblocking(inner.get_ref().as_raw_fd(), fd_to_send)) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

/// Async wrapper for [`send_ready_byte_nonblocking`].
pub async fn send_ready_byte_async(stream: &AsyncFd<UnixStream>) -> io::Result<()> {
    write_opcode_async(stream, OP_READY).await
}

/// Async wrapper for [`send_drain_request_nonblocking`].
#[allow(dead_code)] // FE-side reaper call site lands in pool.rs::request_drain.
pub async fn send_drain_request_async(stream: &AsyncFd<UnixStream>) -> io::Result<()> {
    write_opcode_async(stream, OP_DRAIN_REQUEST).await
}

/// Async wrapper for [`send_drain_ack_nonblocking`].
pub async fn send_drain_ack_async(stream: &AsyncFd<UnixStream>) -> io::Result<()> {
    write_opcode_async(stream, OP_DRAIN_ACK).await
}

async fn write_opcode_async(stream: &AsyncFd<UnixStream>, opcode: u8) -> io::Result<()> {
    loop {
        let mut guard = stream.writable().await?;
        match guard.try_io(|inner| write_opcode(inner.get_ref().as_raw_fd(), opcode)) {
            Ok(result) => return result,
            Err(_would_block) => continue,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wrap a freshly-connected/accepted `UnixStream` for use with the
/// async helpers. Sets `O_NONBLOCK` on the underlying fd (required
/// by `AsyncFd`) and registers it with the current tokio reactor.
///
/// `Interest::READABLE | Interest::WRITABLE` is the default
/// readiness mask `AsyncFd::new` uses; we name it explicitly for
/// future readers.
pub fn wrap_for_async(stream: UnixStream) -> io::Result<AsyncFd<UnixStream>> {
    stream.set_nonblocking(true)?;
    AsyncFd::with_interest(stream, Interest::READABLE | Interest::WRITABLE)
}

// Helper trait so we can `?` on raw-fd → OwnedFd construction.
trait OwnedFdFromRawChecked: Sized {
    fn from_raw_fd_checked(fd: RawFd) -> io::Result<Self>;
}
impl OwnedFdFromRawChecked for OwnedFd {
    fn from_raw_fd_checked(fd: RawFd) -> io::Result<Self> {
        if fd < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SCM_RIGHTS cmsg carried a negative fd",
            ));
        }
        // SAFETY: kernel just dup'd this fd into our process via
        // SCM_RIGHTS; we own it.
        Ok(unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) })
    }
}
