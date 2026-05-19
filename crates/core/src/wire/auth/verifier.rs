//! `pg_authid.rolpassword` reader + verifier-format parser.
//!
//! For each connection auth attempt we need to know:
//!
//! 1. Does this role have a password configured at all?
//! 2. If so, what method does it require (SCRAM-SHA-256 / MD5 /
//!    cleartext)?
//!
//! PG stores the answer in `pg_authid.rolpassword` as one of:
//!
//! - `NULL` — role has no password; trust-equivalent.
//! - `"SCRAM-SHA-256$<iters>:<salt_b64>$<stored_key_b64>:<server_key_b64>"`
//!   — SCRAM verifier (modern, used since PG 10).
//! - `"md5<hex>"` — MD5 verifier (legacy, often disabled by
//!   `password_encryption = scram-sha-256` in PG ≥ 14).
//! - Anything else — historically cleartext; this is exceedingly
//!   rare in modern clusters and we treat it as an unsupported
//!   format (reject).
//!
//! The verifier loader is the closest v0 gets to PG's
//! `hba_getauthmethod` (which we deliberately don't call — see
//! [`crate::wire::auth::hba`]). Instead of consulting
//! `pg_hba.conf`, we look at the credential format and pick the
//! method that matches. v0 deviation from full PG:
//!
//! - **No HBA rule matching.** Operators who want per-host or
//!   per-database method overrides can't get them yet. Full HBA
//!   lookup needs `pg_sys::hba_getauthmethod` which requires a
//!   carefully-constructed `Port` struct (filtered out of pgrx's
//!   default bindings) — deferred to a later phase if a real
//!   deployment needs it.
//! - **`trust` is the only fallback.** A role with NULL rolpassword
//!   is trust-accepted; PG would consult HBA for that case too.
//!   Acceptable for v0 because we already documented (configuration.md
//!   §3 GUC `pg_transport.auth_source`) that operator opt-in is
//!   mandatory and the deployment narrative is "everything's trust
//!   unless the role has a credential".

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pgrx::pg_sys;

/// Parsed `pg_authid.rolpassword` for one role.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Scram/Md5 payloads consumed in phase 7.3 (crypto).
pub enum RolPassword {
    /// `rolpassword IS NULL` — role exists but has no password.
    None,
    /// `"SCRAM-SHA-256$<iters>:<salt>$<stored_key>:<server_key>"`.
    Scram(ScramVerifier),
    /// `"md5<32-hex>"` — legacy verifier.
    Md5(String),
    /// Role does not exist in `pg_authid`.
    UnknownRole,
    /// The credential is in a format we don't recognise (e.g.
    /// cleartext or a future PG verifier type). Phase 7.2 treats
    /// this as a reject; phase ≥ 7.4 may revisit.
    Unsupported,
}

/// Parsed SCRAM-SHA-256 verifier components. All three byte fields
/// are the post-base64-decode bytes (raw HMAC outputs / salt).
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields read in phase 7.3 (SCRAM crypto).
pub struct ScramVerifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

/// Look up the password verifier for `role` via SPI.
///
/// Runs in a freshly-opened transaction (we can't reuse a caller-
/// owned xact because the auth handler runs *before* any normal
/// SPI flow), with the same explicit `StartTransactionCommand` +
/// `AbortCurrentTransaction`-on-panic dance as
/// [`crate::backend::spi_bridge::run_spi_statement`]. Panic-safety
/// rationale lives there.
///
/// Returns:
/// - `Ok(RolPassword::*)` for a successful lookup (the variant
///   tells the caller what method to dispatch).
/// - `Err(_)` only for SPI-level failures unrelated to the user's
///   credential — these are surfaced as a wire-level FATAL to the
///   client (the alternative would be silently trust-accepting on
///   internal error, which is the wrong default for an auth path).
pub fn load_rolpassword(role: &str) -> Result<RolPassword, String> {
    // Open a fresh transaction for the catalog read. Matches the
    // shape of spi_bridge::run_spi_statement — see that function
    // for why we don't use BackgroundWorker::transaction (pgrx
    // 0.18 cleanup-outside-PgTryBuilder bug) and why Abort-on-
    // panic is the right teardown.
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let role_owned = role.to_string();
    let outcome: Result<Result<RolPassword, String>, Box<dyn Any + Send>> =
        catch_unwind(AssertUnwindSafe(|| spi_load(&role_owned)));

    match outcome {
        Ok(Ok(pw)) => {
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Ok(pw)
        }
        Ok(Err(err)) => {
            unsafe {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            Err(err)
        }
        Err(_panic_payload) => {
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }
            // We intentionally swallow the panic payload here:
            // surfacing a PG-internal panic message to an
            // unauthenticated client is an information leak. The
            // bgworker will still see it via pg_guard's ereport.
            Err("auth: pg_authid lookup failed (internal error)".to_string())
        }
    }
}

/// Run the actual SPI query inside the caller's xact wrapper.
fn spi_load(role: &str) -> Result<RolPassword, String> {
    // Spi::connect_mut + a manual is_empty() guard because
    // get_one_with_args raises Err for empty result sets (documented
    // in user-memory pgrx.md) and we need to distinguish "no such
    // role" from "SPI error". get_one::<String>() returns an owned
    // String so the lifetime doesn't dangle past the closure.
    pgrx::Spi::connect_mut(|client| -> Result<RolPassword, String> {
        let table = client
            .update(
                "SELECT rolpassword::text FROM pg_authid WHERE rolname = $1",
                Some(1),
                &[role.into()],
            )
            .map_err(|e| format!("auth: SPI failure during pg_authid lookup: {e}"))?;

        if table.is_empty() {
            return Ok(RolPassword::UnknownRole);
        }

        let raw: Option<String> = table
            .first()
            .get_one::<String>()
            .map_err(|e| format!("auth: SPI getter failed: {e}"))?;

        Ok(match raw {
            None => RolPassword::None,
            Some(s) => parse_verifier(&s),
        })
    })
}

/// Classify a non-NULL `pg_authid.rolpassword` string. See module
/// docs for the format catalog.
pub fn parse_verifier(s: &str) -> RolPassword {
    if let Some(rest) = s.strip_prefix("SCRAM-SHA-256$") {
        return parse_scram(rest).map_or(RolPassword::Unsupported, RolPassword::Scram);
    }
    if let Some(rest) = s.strip_prefix("md5") {
        // PG stores MD5 verifier as "md5" + 32 lowercase hex chars.
        // Anything else with the prefix is malformed — treat as
        // unsupported so we don't silently accept garbage.
        if rest.len() == 32 && rest.chars().all(|c| c.is_ascii_hexdigit()) {
            return RolPassword::Md5(rest.to_string());
        }
        return RolPassword::Unsupported;
    }
    RolPassword::Unsupported
}

/// Parse the body of a `SCRAM-SHA-256$<iters>:<salt>$<stored>:<server>`
/// verifier (the prefix has already been stripped by the caller).
fn parse_scram(body: &str) -> Option<ScramVerifier> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    // Split on the single '$': left half is "iters:salt_b64",
    // right half is "stored_b64:server_b64".
    let (left, right) = body.split_once('$')?;
    let (iters_str, salt_b64) = left.split_once(':')?;
    let (stored_b64, server_b64) = right.split_once(':')?;

    let iterations: u32 = iters_str.parse().ok()?;
    let salt = STANDARD.decode(salt_b64).ok()?;
    let stored_key = STANDARD.decode(stored_b64).ok()?;
    let server_key = STANDARD.decode(server_b64).ok()?;

    // Sanity bounds: PG's SCRAM uses iterations ≥ 4096 (RFC
    // minimum); stored_key and server_key are SHA-256 outputs (32
    // bytes); salt is variable but in practice 16 bytes. We
    // accept any length that isn't obviously wrong — phase 7.3
    // SCRAM crypto will catch length mismatches at use time.
    if iterations < 4096 || stored_key.len() != 32 || server_key.len() != 32 {
        return None;
    }

    Some(ScramVerifier {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}

// (Earlier 7.2 versions exposed an `infer_method(role) -> AuthMethod`
// helper that combined load_rolpassword + variant-to-method mapping.
// Removed in 7.3: the startup handler dispatches on RolPassword
// directly because the SCRAM path needs the full verifier in hand,
// not just the method tag — avoids a second SPI call.)
