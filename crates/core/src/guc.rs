//! GUC registration.
//!
//! Phase 2 added the first user-facing knob (Q23 said zero new GUCs
//! in phase 1; the GUC surface starts at phase 2). Phase 7 adds
//! `pg_transport.auth_source` (required, no default; validated at
//! `_PG_init()`). See `docs/design/configuration.md` for the
//! eventual full list.

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::PgSqlErrorCode;

/// `pg_transport.backend_pool_size` — how many slot bgworkers
/// `_PG_init()` registers at postmaster start.
///
/// Read once at postmaster start; changes require restart (`PGC_POSTMASTER`).
/// Live pool resize is [Q15](../../docs/design/roadmap.md) — deferred.
///
/// Default `2` is a phase-2 testing convenience; the design's eventual
/// default is `max(4, num_cpus)` (see
/// [backend-handoff.md §1](../../docs/design/backend-handoff.md)).
pub static BACKEND_POOL_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2);

/// `pg_transport.auth_source` — selects whether wire-layer auth
/// resolves methods via PG's `pg_hba.conf` helpers (`'pg_hba'`) or
/// a framework-owned catalog (`'pg_transport'`, deferred).
///
/// **No default; required.** An unset value raises FATAL at
/// `_PG_init()` so operators see the configuration mistake at
/// cluster boot rather than via mid-flight slot deaths. See
/// [backend-wire.md §4](../../docs/design/backend-wire.md) and
/// [configuration.md](../../docs/design/configuration.md).
///
/// String GUC backed by `Option<CString>`; accessor [`auth_source`]
/// decodes into the typed [`AuthSource`] enum.
pub static AUTH_SOURCE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// `pg_transport.tls_cert_file` — path to the server TLS certificate
/// in PEM format. Empty / unset disables TLS entirely (the wire
/// layer responds `'N'` to `SSLRequest`). See
/// [backend-wire.md §5](../../docs/design/backend-wire.md).
///
/// Phase 8: no default. The design ([configuration.md §3](../../docs/design/configuration.md))
/// would have us inherit the cluster's `ssl_cert_file`, but that's
/// another layer of GUC resolution to wire up; for v0 we accept the
/// "operator names the cert explicitly" friction.
pub static TLS_CERT_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// `pg_transport.tls_key_file` — path to the server TLS private
/// key in PEM (PKCS#8) format. Empty / unset disables TLS. Both
/// `tls_cert_file` and `tls_key_file` must be set together; setting
/// only one FATALs at slot boot.
pub static TLS_KEY_FILE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// Typed view of [`AUTH_SOURCE`]. See module-level docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthSource {
    /// Call PG's `hba_getauthmethod` directly to resolve the
    /// `(method, options)` tuple for each incoming connection.
    PgHba,
    /// Consult a framework-owned catalog (`pg_transport.hba`). The
    /// catalog schema is deferred until a real deployment needs it
    /// — until then, selecting this value raises FATAL at the
    /// first auth attempt. The value is still accepted at boot so
    /// the GUC's either/or model is honest rather than a one-option
    /// fiction.
    PgTransport,
}

pub fn register() {
    GucRegistry::define_int_guc(
        c"pg_transport.backend_pool_size",
        c"Number of slot bgworkers in the backend pool.",
        c"Read once at postmaster start; changes require a restart.",
        &BACKEND_POOL_SIZE,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_transport.auth_source",
        c"Source for wire-layer authentication lookup ('pg_hba' or 'pg_transport').",
        c"REQUIRED — no default; an unset value FATALs at cluster start.",
        &AUTH_SOURCE,
        // Postmaster context: read once at boot, can't be changed
        // without a restart. Reload-safe alternatives (Sighup,
        // Backend) are wrong here — auth source is policy, not
        // per-session, and silently changing it mid-flight would
        // be a security-relevant surprise.
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_transport.tls_cert_file",
        c"Path to the server TLS certificate (PEM). Empty disables TLS.",
        c"Both tls_cert_file and tls_key_file must be set to enable TLS.",
        &TLS_CERT_FILE,
        // Postmaster: cert is loaded once at slot startup. SIGHUP
        // reload of cert/key is a phase ≥ 9 concern (would need
        // tracking the SslAcceptor Arc in shared state with
        // atomic swap; not worth the complexity for v0).
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_transport.tls_key_file",
        c"Path to the server TLS private key (PEM, PKCS#8). Empty disables TLS.",
        c"Both tls_cert_file and tls_key_file must be set to enable TLS.",
        &TLS_KEY_FILE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

/// Validate required GUCs immediately after [`register`] returns.
///
/// Called from `_PG_init()`; FATALs on any missing or invalid
/// required GUC. We deliberately don't use a per-GUC `check_hook`:
/// the hook fires when PG applies the config value, which can be
/// awkward to surface a clean operator-facing error from. A direct
/// `ereport(FATAL, …)` here is simpler and produces a message
/// that names the GUC and the legal values.
pub fn validate_required() {
    if let Err(msg) = decode_auth_source() {
        pgrx::ereport!(
            FATAL,
            PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR,
            msg,
            "Set `pg_transport.auth_source = 'pg_hba'` (or 'pg_transport') in postgresql.conf and restart."
        );
    }
}

/// Typed accessor for [`AUTH_SOURCE`]. Panics if called before
/// [`validate_required`] has succeeded (which would mean the GUC
/// is unset or invalid — an `_PG_init` bug).
#[allow(dead_code)] // Read site lands in phase 7.2 (hba::lookup dispatch).
pub fn auth_source() -> AuthSource {
    decode_auth_source().expect("auth_source must be validated by _PG_init before any read")
}

fn decode_auth_source() -> Result<AuthSource, String> {
    let raw = AUTH_SOURCE.get();
    let s = raw
        .as_ref()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default();
    match s.as_str() {
        "pg_hba" => Ok(AuthSource::PgHba),
        "pg_transport" => Ok(AuthSource::PgTransport),
        "" => Err("pg_transport.auth_source is required but unset — \
             set it to 'pg_hba' or 'pg_transport' in postgresql.conf"
            .to_string()),
        other => Err(format!(
            "invalid pg_transport.auth_source value {other:?} — \
             expected 'pg_hba' or 'pg_transport'"
        )),
    }
}

#[inline]
pub fn backend_pool_size() -> u32 {
    BACKEND_POOL_SIZE.get() as u32
}

/// Read both TLS-file GUCs and return the typed pair
/// `(cert_path, key_path)` if both are set, or `None` if TLS is
/// disabled (both empty). Returns an error if exactly one is set —
/// that's almost certainly an operator mistake (asymmetric config
/// usually means "I forgot to set the other one"). The slot's TLS
/// builder converts this to a FATAL at boot.
pub fn tls_files() -> Result<Option<(String, String)>, String> {
    let cert = TLS_CERT_FILE
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default();
    let key = TLS_KEY_FILE
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default();
    match (cert.is_empty(), key.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => Ok(Some((cert, key))),
        (true, false) => Err(
            "pg_transport.tls_key_file is set but pg_transport.tls_cert_file is not".to_string(),
        ),
        (false, true) => Err(
            "pg_transport.tls_cert_file is set but pg_transport.tls_key_file is not".to_string(),
        ),
    }
}
