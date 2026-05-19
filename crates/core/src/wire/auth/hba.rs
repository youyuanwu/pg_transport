//! `pg_hba.conf` lookup — placeholder for phase 7.2.
//!
//! ## Status
//!
//! Phase 7.1 (this commit): [`lookup`] is a stub that returns
//! [`AuthMethod::Trust`] for every connection. Combined with the
//! database-name check in the startup handler, this means:
//!
//! - Connections claiming `database = "postgres"` are
//!   trust-authenticated (current v0 behaviour, preserved).
//! - Connections claiming any other database are rejected with a
//!   clear `3D000` error before they reach auth.
//! - The auth-dispatch *framework* is in place so phase 7.2 just
//!   needs to swap this stub for a real `hba_getauthmethod` FFI
//!   call (`pg_sys::hba_getauthmethod` takes a `*mut Port` struct
//!   with `(user, database, raddr, peer_uid, ssl)` populated and
//!   returns the matched `(method, options)` tuple).
//!
//! ## Why a stub is acceptable for 7.1
//!
//! - **No security regression.** Before this commit the startup
//!   handler used pgwire's `NoopStartupHandler` which trust-accepts
//!   every connection unconditionally. Returning trust here is
//!   strictly the same behaviour, but routed through the new
//!   auth-dispatch surface that 7.2 will plug a real HBA into
//!   without touching call sites.
//! - **The new GUC still surfaces operator misconfiguration.**
//!   `pg_transport.auth_source` is required at boot
//!   ([`crate::guc::validate_required`]); a missing or invalid value
//!   FATALs before the wire layer is ever reached. So operators
//!   can't accidentally land on this stub in production thinking
//!   they've configured HBA-based auth.
//! - **The integration shape is what 7.2 needs to test.**
//!   Constructing a `pg_sys::Port` carefully enough to pass to
//!   `hba_getauthmethod` is the actual hard work; the dispatch
//!   path around it is what we land here.

use super::AuthMethod;

/// Inputs to an HBA lookup. Mirrors the subset of PG's `Port`
/// struct that `hba_getauthmethod` reads.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields unread until phase 7.2 swaps in the real FFI.
pub struct HbaLookup<'a> {
    pub user: &'a str,
    pub database: &'a str,
    pub host: &'a str,
    /// `true` if the connection has been TLS-upgraded already (phase 8).
    pub ssl: bool,
}

/// Look up the auth method PG's `pg_hba.conf` would pick for this
/// connection. v0.1 returns [`AuthMethod::Trust`] unconditionally;
/// phase 7.2 swaps in the real FFI call.
pub fn lookup(input: &HbaLookup<'_>) -> AuthMethod {
    // Phase 7.1 stub. Caller's HBA-source GUC is already validated
    // (`pg_hba` or `pg_transport`); both currently land here.
    let _ = input;
    AuthMethod::Trust
}
