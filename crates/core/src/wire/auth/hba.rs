//! Auth method lookup — v0 backend for `pg_transport.auth_source`.
//!
//! ## Status
//!
//! Phase 7.2 (this commit): we look up the method by reading
//! `pg_authid.rolpassword` via SPI and inferring the auth method
//! from the credential format (see [`crate::wire::auth::verifier`]).
//! This is **not** what PG's `hba_getauthmethod` does — that
//! function consults `pg_hba.conf` and applies per-host /
//! per-database rules. Full HBA matching needs `pg_sys::Port` /
//! `hba_getauthmethod`, both of which are filtered out of pgrx's
//! default bindings and would require a fragile hand-declared FFI
//! or a C shim. Deferred until a real deployment needs HBA-rule
//! granularity.
//!
//! v0 mapping:
//!
//! - `rolpassword IS NULL` or role unknown → [`AuthMethod::Trust`]
//! - `SCRAM-SHA-256$…` → [`AuthMethod::ScramSha256`]
//! - `md5<32-hex>` → [`AuthMethod::Md5`]
//! - any other format → [`AuthMethod::Reject`]
//! - SPI failure (extremely rare) → [`AuthMethod::Reject`] (fail
//!   closed, not open)
//!
//! ## Why this is acceptable for v0
//!
//! - **The acceptance criterion is method-based, not HBA-rule-based.**
//!   Phase 7's [roadmap §1](../../../../../docs/design/roadmap.md)
//!   acceptance is "`psql -c "…"` works with SCRAM-SHA-256 against
//!   a SCRAM-stored role". That's a per-user thing, not a
//!   per-host-allowlist thing.
//! - **The GUC opt-in is preserved.** `pg_transport.auth_source` is
//!   still required at boot; operators still have to know they're
//!   wiring up a non-default auth source.
//! - **Real HBA can drop in later.** The seam — `hba::lookup` taking
//!   a populated [`HbaLookup`] and returning a single
//!   [`AuthMethod`] — is exactly the shape that a real
//!   `hba_getauthmethod` call would have. When operator demand
//!   materialises, swap the body; call sites need no change.

use super::AuthMethod;
use super::verifier;

/// Inputs to an auth-method lookup. Mirrors the subset of PG's
/// `Port` struct that a real `hba_getauthmethod` call would read.
/// Phase 7.2 only consumes `user`; `database` / `host` / `ssl` are
/// retained for the seam.
#[derive(Debug, Clone)]
#[allow(dead_code)] // database/host/ssl unused until full HBA lookup lands.
pub struct HbaLookup<'a> {
    pub user: &'a str,
    pub database: &'a str,
    pub host: &'a str,
    /// `true` if the connection has been TLS-upgraded already (phase 8).
    pub ssl: bool,
}

/// Look up the auth method for an incoming connection. See module
/// docs for the v0 mapping rules and trade-offs.
pub fn lookup(input: &HbaLookup<'_>) -> AuthMethod {
    match verifier::infer_method(input.user) {
        Ok(method) => method,
        Err(msg) => {
            // SPI failure during catalog read — fail closed. We
            // log because this is unusual and operators should
            // see it; the client gets a generic FATAL via the
            // startup handler's reject path.
            pgrx::warning!("pg_transport auth: rolpassword lookup failed for {input:?}: {msg}");
            AuthMethod::Reject
        }
    }
}
