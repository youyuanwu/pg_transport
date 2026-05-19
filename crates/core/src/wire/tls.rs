//! TLS termination — build a `tokio_rustls::TlsAcceptor` from the
//! `pg_transport.tls_cert_file` / `pg_transport.tls_key_file` GUCs.
//!
//! ## Plumbing
//!
//! pgwire 0.40 hard-binds its TLS plumbing to `tokio_rustls`
//! (see Q26 in `docs/design/roadmap.md`; original Q12 picked
//! rust-openssl but switching to rustls avoids forking pgwire).
//! `process_socket(tcp, Some(acceptor), handlers)` does:
//!
//! 1. Peek for the 8-byte `SSLRequest` magic (0x04, 0xD2, 0x16, 0x2F).
//! 2. If present + we passed `Some(acceptor)`: reply `'S'` and run
//!    the TLS handshake via `acceptor.accept(socket)`. From there
//!    on, all wire I/O is encrypted.
//! 3. If absent or we passed `None`: reply `'N'` (or proceed
//!    cleartext) and continue.
//!
//! So we just need to construct the acceptor once at slot startup
//! and pass it into [`crate::wire::pgwire_v3::PgwireV3::run`].
//!
//! ## Phase 8 scope
//!
//! - **Single cert per cluster.** [`build`] reads
//!   `pg_transport.tls_{cert,key}_file` once. Per-listener cert
//!   variation (Q13 in the roadmap) is deferred.
//! - **Server-only auth.** No client-cert verification; the
//!   `pg_transport.tls_ca_file` GUC + `cert` auth method come with
//!   the v0 acceptance criterion only requiring `sslmode=require`.
//! - **TLS 1.2+ via the rustls default.** rustls' default
//!   `ServerConfig` rejects < TLS 1.2; we don't expose
//!   `tls_min_proto` as a GUC in v0.
//! - **Crypto provider.** `_PG_init` installs the `ring` provider
//!   process-wide; if the operator forgot to do that we'd panic
//!   at `ServerConfig::builder()` with "no process-level
//!   CryptoProvider available".

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::sync::Arc;

use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::guc;

/// Read the configured cert + key files and return a ready-to-use
/// `TlsAcceptor`, or `Ok(None)` if TLS is disabled (both GUCs
/// empty). Errors propagate operator misconfiguration:
///
/// - Exactly one of cert/key set → asymmetric config error.
/// - File missing / unreadable → wrap the underlying I/O error.
/// - PEM parse failure → rustls / rustls-pemfile error.
/// - Wrong key format (only PKCS#8 supported in v0) → no keys
///   parsed, return a clear "expected PKCS#8" error.
///
/// Called once per slot bgworker at startup; the result lives in
/// [`crate::wire::WireCtx`] and is cloned into each handoff's
/// `process_socket` call (TlsAcceptor is `Arc<ServerConfig>`
/// internally, so clone is cheap).
pub fn build() -> Result<Option<Arc<TlsAcceptor>>, IoError> {
    let Some((cert_path, key_path)) =
        guc::tls_files().map_err(|e| IoError::new(ErrorKind::InvalidInput, e))?
    else {
        return Ok(None);
    };

    let certs_vec: Vec<CertificateDer<'static>> = certs(&mut BufReader::new(
        File::open(&cert_path)
            .map_err(|e| IoError::new(e.kind(), format!("opening {cert_path}: {e}")))?,
    ))
    .collect::<Result<Vec<_>, IoError>>()
    .map_err(|e| IoError::new(ErrorKind::InvalidData, format!("parsing {cert_path}: {e}")))?;
    if certs_vec.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("no certificates found in {cert_path}"),
        ));
    }

    let mut keys: Vec<PrivateKeyDer<'static>> = pkcs8_private_keys(&mut BufReader::new(
        File::open(&key_path)
            .map_err(|e| IoError::new(e.kind(), format!("opening {key_path}: {e}")))?,
    ))
    .map(|k| k.map(PrivateKeyDer::from))
    .collect::<Result<Vec<_>, IoError>>()
    .map_err(|e| IoError::new(ErrorKind::InvalidData, format!("parsing {key_path}: {e}")))?;
    if keys.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "no PKCS#8 private keys found in {key_path} \
                 (only PKCS#8 is supported in v0; convert with `openssl pkey`)"
            ),
        ));
    }
    let key = keys.remove(0);

    // ServerConfig::builder picks the default crypto provider
    // installed in _PG_init() (ring). `with_no_client_auth` =
    // server-only TLS; client-cert verification (and the `cert`
    // auth method) come in a later phase.
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs_vec, key)
        .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("rustls cert config: {e}")))?;

    Ok(Some(Arc::new(TlsAcceptor::from(Arc::new(config)))))
}
