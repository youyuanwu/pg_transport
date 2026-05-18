//! Low-level SCM_RIGHTS fd-passing over connected `AF_UNIX SOCK_STREAM`
//! sockets.
//!
//! Tokio doesn't expose ancillary data on `UnixStream` in 1.x, and
//! the standard library only does on nightly. We drop to `libc::sendmsg`
//! / `libc::recvmsg` for both sides. Kept tiny — one fd at a time, one
//! placeholder byte of regular data (the kernel requires non-empty
//! iovec for SCM_RIGHTS delivery).
//!
//! Both sides pass `cmsg_buf` as a fixed 64-byte array. The actual
//! cmsg space needed is `CMSG_SPACE(sizeof(int))` ≈ 20 bytes on
//! Linux x86_64; 64 is a safe over-estimate that compiles down to
//! a single stack allocation.
//!
//! All helpers are SAFE-to-call from any thread because they use
//! purely thread-local state (msghdr / iovec / cmsg buffers all on
//! the stack, fds are owned).

use std::io;
use std::mem;
use std::os::fd::{OwnedFd, RawFd};
use std::os::raw::c_int;

/// Send `fd_to_send` to the peer of `stream_fd`. The peer must
/// `recvmsg` to receive it.
///
/// On success: the kernel duplicates the fd into the peer process
/// (the peer gets a NEW fd number for the same underlying file
/// description). The local `fd_to_send` is unchanged here — the
/// caller is responsible for closing their reference if they no
/// longer need it.
///
/// Returns `Err` on `EAGAIN`/`EWOULDBLOCK`/`EPIPE` and other socket
/// errors. `EPIPE` specifically means the peer's slot bgworker has
/// died — the FE-side pool uses this to drive respawn (phase ≥ 3;
/// not implemented in phase 2 because v0 phase 2 has no respawn
/// machinery yet).
pub fn send_fd(stream_fd: RawFd, fd_to_send: RawFd) -> io::Result<()> {
    // One byte of regular data — kernel requires the iovec be
    // non-empty for the ancillary data to be delivered.
    let buf = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: 1,
    };

    // SCM_RIGHTS control buffer. 64 bytes covers CMSG_SPACE(sizeof(int))
    // on every Unix we target with margin to spare.
    let mut cmsg_buf = [0u8; 64];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    // SAFETY: CMSG_SPACE is a const-fn-shaped macro in libc; calling
    // it is purely arithmetic.
    msg.msg_controllen = unsafe { libc::CMSG_SPACE(mem::size_of::<c_int>() as u32) as _ };

    // SAFETY: `cmsg` derived from a well-formed msghdr we just
    // initialised; writes are inside cmsg_buf which is large enough.
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

        let n = libc::sendmsg(stream_fd, &msg, libc::MSG_NOSIGNAL);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Receive one fd from the peer of `stream_fd`, blocking until a
/// message arrives.
///
/// Returns:
/// * `Ok(Some(fd))` — received an fd via SCM_RIGHTS, wrapped in
///   `OwnedFd` so the caller closes it on drop.
/// * `Ok(None)` — clean EOF; the peer (frontend) closed its end.
///   The slot loop interprets this as shutdown.
/// * `Err(...)` — socket error.
///
/// If the message arrived but carries no SCM_RIGHTS cmsg, we treat
/// it as a protocol violation (`InvalidData`) — phase-2 FE only ever
/// sends fds, never bare bytes.
pub fn recv_fd(stream_fd: RawFd) -> io::Result<Option<OwnedFd>> {
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
    let n = unsafe { libc::recvmsg(stream_fd, &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n == 0 {
        return Ok(None);
    }

    // Walk the cmsg list looking for the SCM_RIGHTS entry.
    // SAFETY: msg.msg_control points to cmsg_buf; CMSG_FIRSTHDR /
    // CMSG_NXTHDR walk inside the controllen we set.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let fd = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg) as *const c_int);
                return Ok(Some(OwnedFd::from_raw_fd_checked(fd)?));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "recvmsg returned data but no SCM_RIGHTS cmsg",
    ))
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
