//! Wire-layer authentication.
//!
//! Per [backend-wire.md §4](../../../../../docs/design/backend-wire.md)
//! and roadmap phase 7: we do **not** call PG's `ClientAuthentication`.
//! Instead the wire-layer:
//!
//! 1. Looks up the auth method for `(role, database, client_addr,
//!    peer_uid, ssl)` via PG's `hba_getauthmethod` (when
//!    `pg_transport.auth_source = 'pg_hba'`) — see [`hba::lookup`].
//! 2. Runs the resulting method in Rust ([`AuthMethod`]).
//!
//! ## Phase split
//!
//! - **7.1 (this commit):** framework, GUC validation, trust /
//!   reject dispatch, **clear FATAL ErrorResponse for every method
//!   that needs wire-protocol crypto** (md5 / scram-sha-256 /
//!   password). [`hba::lookup`] is stubbed to return [`AuthMethod::Trust`]
//!   unconditionally — combined with the StartupMessage `database`
//!   check, this means the connection has to claim `database =
//!   "postgres"` and is trust-authenticated.
//! - **7.2:** real `hba_getauthmethod` FFI + `pg_authid.rolpassword`
//!   SPI reader.
//! - **7.3:** SCRAM-SHA-256 verifier (using StoredKey/ServerKey
//!   directly per PG's storage format — pgwire's `ScramAuth` expects
//!   the SaltedPassword which PG does not store, so we implement
//!   `compute_client_proof` ourselves against StoredKey).
//! - **Later phases:** md5, password (needs TLS = phase 8), cert
//!   (needs TLS), peer.

pub mod hba;
pub mod scram;
pub mod verifier;

/// Methods PG's `pg_hba.conf` can specify. Mirrors PG's `UserAuth`
/// enum (`src/include/libpq/hba.h`). v0.1 implements [`Trust`] and
/// [`Reject`]; the others are placeholders for later phases.
///
/// [`Trust`]: AuthMethod::Trust
/// [`Reject`]: AuthMethod::Reject
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Most variants unused until phase 7.2+.
pub enum AuthMethod {
    Trust,
    Reject,
    Password,
    Md5,
    ScramSha256,
    Cert,
    Peer,
    Gss,
    Sspi,
    Ldap,
    Pam,
    Radius,
    Bsd,
    Ident,
}

impl AuthMethod {
    /// The keyword PG's HBA grammar uses for this method. Used in
    /// operator-facing error messages.
    pub const fn keyword(&self) -> &'static str {
        match self {
            AuthMethod::Trust => "trust",
            AuthMethod::Reject => "reject",
            AuthMethod::Password => "password",
            AuthMethod::Md5 => "md5",
            AuthMethod::ScramSha256 => "scram-sha-256",
            AuthMethod::Cert => "cert",
            AuthMethod::Peer => "peer",
            AuthMethod::Gss => "gss",
            AuthMethod::Sspi => "sspi",
            AuthMethod::Ldap => "ldap",
            AuthMethod::Pam => "pam",
            AuthMethod::Radius => "radius",
            AuthMethod::Bsd => "bsd",
            AuthMethod::Ident => "ident",
        }
    }
}

/// Outcome of running an auth method against a client. The wire
/// layer's startup handler translates this into the right pgwire
/// `Authentication*` + `ReadyForQuery` / `ErrorResponse` sequence.
#[derive(Debug)]
pub enum AuthOutcome {
    /// Auth succeeded — proceed to `finish_authentication0`.
    Accept,
    /// Auth failed with a wire-level error frame. The connection is
    /// closed after the frame is sent.
    Reject {
        /// PG SQLSTATE — usually `"28P01"` for bad credentials,
        /// `"28000"` for an explicit `reject` rule, `"0A000"` for
        /// methods we don't implement.
        sqlstate: &'static str,
        /// Human-readable reason; goes into the ErrorResponse
        /// `Message` field verbatim.
        message: String,
    },
}

impl AuthOutcome {
    /// Helper for "method not yet implemented in this build".
    /// SQLSTATE `0A000` (feature_not_supported) per PG convention.
    pub fn unimplemented(method: AuthMethod) -> Self {
        AuthOutcome::Reject {
            sqlstate: "0A000",
            message: format!(
                "pg_transport: auth method {:?} selected by pg_hba.conf is \
                 not implemented in this build",
                method.keyword()
            ),
        }
    }

    /// Helper for "this connection's database is not configured for
    /// pg_transport". v0 slot bgworkers hard-code SPI to `postgres`;
    /// connections claiming a different database would silently run
    /// against the wrong DB if we let them through.
    pub fn wrong_database(claimed: &str) -> Self {
        AuthOutcome::Reject {
            sqlstate: "3D000", // invalid_catalog_name
            message: format!(
                "pg_transport: database {claimed:?} not configured for pg_transport \
                 (v0 supports only \"postgres\"; per-database routing arrives in a later phase)"
            ),
        }
    }
}

/// Dispatch table: given the auth method PG's HBA chose, return the
/// outcome. Currently unused — phase 7.3 dispatches on
/// [`verifier::RolPassword`] directly in the startup handler
/// because the SCRAM branch needs the parsed verifier in hand and
/// the variant-to-outcome mapping is more naturally expressed at
/// the call site. Kept here for the rare future path that wants
/// method-only dispatch (e.g. a real `hba_getauthmethod` swap).
#[allow(dead_code)]
pub fn run(method: AuthMethod) -> AuthOutcome {
    match method {
        AuthMethod::Trust => AuthOutcome::Accept,
        AuthMethod::Reject => AuthOutcome::Reject {
            sqlstate: "28000", // invalid_authorization_specification
            message: "pg_transport: connection rejected by pg_hba.conf rule".to_string(),
        },
        other => AuthOutcome::unimplemented(other),
    }
}
