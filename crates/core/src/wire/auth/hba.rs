//! Auth credential lookup — v0 backend for `pg_transport.auth_source`.
//!
//! ## Status
//!
//! Phase 7.2 / 7.3: we look up the user's credential by reading
//! `pg_authid.rolpassword` via SPI (see
//! [`crate::wire::auth::verifier`]) and return the typed
//! [`RolPassword`] variant. The startup handler then dispatches on
//! that — trust for None / UnknownRole, SCRAM for Scram(verifier),
//! md5-not-implemented for Md5, reject for Unsupported.
//!
//! This is **not** what PG's `hba_getauthmethod` does — that
//! function consults `pg_hba.conf` and applies per-host /
//! per-database rules. Full HBA matching needs `pg_sys::Port` /
//! `hba_getauthmethod`, both of which are filtered out of pgrx's
//! default bindings; deferred until a real deployment needs
//! HBA-rule granularity.
//!
//! ## Why this is acceptable for v0
//!
//! - The phase-7 acceptance criterion is method-based, not
//!   HBA-rule-based: "`psql -c "…"` works with SCRAM-SHA-256 against
//!   a SCRAM-stored role" ([roadmap §1](../../../../../docs/design/roadmap.md)).
//! - The GUC opt-in (`pg_transport.auth_source`) is still required
//!   at boot so operators can't accidentally land here in production
//!   thinking they've configured HBA-based auth.
//! - Real HBA can drop in later — the seam ([`lookup`] taking a
//!   populated [`HbaLookup`] and returning [`RolPassword`]) is
//!   exactly the shape a real `hba_getauthmethod` call would have.

use super::verifier::{self, RolPassword};

/// Inputs to a credential lookup. Mirrors the subset of PG's
/// `Port` struct that a real `hba_getauthmethod` call would read.
/// Phase 7.2 / 7.3 only consume `user`; `database` / `host` / `ssl`
/// are retained for the seam.
#[derive(Debug, Clone)]
#[allow(dead_code)] // database/host/ssl unused until full HBA lookup lands.
pub struct HbaLookup<'a> {
    pub user: &'a str,
    pub database: &'a str,
    pub host: &'a str,
    /// `true` if the connection has been TLS-upgraded already (phase 8).
    pub ssl: bool,
}

/// Look up the credential for an incoming connection. On SPI
/// failure logs a warning and returns [`RolPassword::Unsupported`]
/// — fail-closed so an internal error doesn't silently
/// trust-accept.
pub fn lookup(input: &HbaLookup<'_>) -> RolPassword {
    match verifier::load_rolpassword(input.user) {
        Ok(pw) => pw,
        Err(msg) => {
            pgrx::warning!("pg_transport auth: rolpassword lookup failed for {input:?}: {msg}");
            RolPassword::Unsupported
        }
    }
}
