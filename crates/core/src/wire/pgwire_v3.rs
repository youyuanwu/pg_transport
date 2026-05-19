//! `pgwire-v3` Wire impl built on the [`pgwire`](https://crates.io/crates/pgwire)
//! crate. Brings up FE/BE v3: startup handshake, simple query →
//! SPI bridge, extended query → "not implemented" error frame
//! (default `NoopHandler` behaviour, see
//! [backend-wire.md §2](../../../../docs/design/backend-wire.md)).
//!
//! Phase 7.3 (this commit): SCRAM-SHA-256 end-to-end. The startup
//! handler is a small state machine across three frontend messages:
//!
//!   Startup            → AuthenticationSASL(["SCRAM-SHA-256"])
//!   SASLInitialResponse → AuthenticationSASLContinue(server_first)
//!   SASLResponse        → AuthenticationSASLFinal + AuthenticationOk + RFQ
//!
//! Credential lookup runs once (during Startup) via
//! [`crate::wire::auth::hba::lookup`]; the parsed verifier
//! ([`crate::wire::auth::verifier::RolPassword`]) decides which
//! path to take. Trust / unimplemented-method / wrong-database
//! flows are unchanged from phase 7.1.

use std::fmt::Debug;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler, finish_authentication};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, METADATA_DATABASE, METADATA_USER, PgWireConnectionState,
    PgWireServerHandlers, PidSecretKeyGenerator, RandomPidSecretKeyGenerator,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

use super::auth::scram::{SCRAM_SHA_256, ScramError, ScramServer};
use super::auth::verifier::RolPassword;
use super::auth::{AuthMethod, AuthOutcome, hba};
use super::extended::PgTransportExtendedQuery;
use super::{Wire, WireCtx};

/// The v0 wire implementation. Stateless marker — per-connection
/// state lives in the pgwire-owned `DefaultClient` and in our handler
/// arc (the SCRAM state machine).
pub struct PgwireV3;

impl Wire for PgwireV3 {
    fn name() -> &'static str {
        "pgwire-v3"
    }

    fn run(fd: OwnedFd, ctx: WireCtx) -> anyhow::Result<()> {
        let std_stream: std::net::TcpStream = fd.into();
        std_stream
            .set_nonblocking(true)
            .map_err(|e| anyhow::anyhow!("set_nonblocking on client fd: {e}"))?;

        // PgTransportHandlers is built fresh per handoff, so the
        // SCRAM `Mutex<SaslPhase>` inside PgTransportStartup is
        // per-connection state. No cross-connection leakage.
        let handlers = Arc::new(PgTransportHandlers::default());

        // Move the TLS acceptor out of ctx so we can pass it into
        // `process_socket`. pgwire handles the SSLRequest peek +
        // handshake transparently when `Some(acceptor)` is passed.
        // `None` means TLS-disabled: pgwire replies 'N' to the
        // SSLRequest magic and proceeds cleartext.
        let tls_acceptor = ctx.tls_acceptor.map(|arc| (*arc).clone());

        ctx.rt.block_on(async move {
            let tcp_stream = tokio::net::TcpStream::from_std(std_stream)
                .map_err(|e| anyhow::anyhow!("tokio::TcpStream::from_std: {e}"))?;
            process_socket(tcp_stream, tls_acceptor, handlers)
                .await
                .map_err(|e| anyhow::anyhow!("pgwire process_socket: {e}"))?;
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handler bundle
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PgTransportHandlers {
    startup: Arc<PgTransportStartup>,
    simple_query: Arc<SimpleQuery>,
    extended_query: Arc<PgTransportExtendedQuery>,
}

impl PgWireServerHandlers for PgTransportHandlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_query.clone()
    }

    // Phase 9: real ExtendedQueryHandler. Replaces the default
    // `NoopHandler` that returned `'This feature is not
    // implemented'` for every Parse/Bind/Execute, which is what
    // forced the e2e harness to use `simple_query` exclusively
    // through phases 4 – 8.
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_query.clone()
    }
}

// ---------------------------------------------------------------------------
// Startup handler — phase 7.3 SCRAM-aware auth dispatch
// ---------------------------------------------------------------------------

/// Per-connection SCRAM state. Stored in `PgTransportStartup` via
/// `Mutex<SaslPhase>` so the handler can thread state across the
/// three frontend messages (Startup → SASLInitialResponse →
/// SASLResponse).
#[derive(Default)]
enum SaslPhase {
    /// Have not received the Startup message yet.
    #[default]
    Idle,
    /// Sent `AuthenticationSASL(["SCRAM-SHA-256"])`; waiting for
    /// SASLInitialResponse with the client-first-message body.
    AwaitingClientFirst { server: Box<ScramServer> },
    /// Processed client-first, sent server-first; waiting for
    /// SASLResponse with the client-final-message body.
    AwaitingClientFinal { server: Box<ScramServer> },
    /// Auth complete (or rejected); the connection is either
    /// moving into the simple-query phase or closing.
    Done,
}

/// Startup handler that does proper auth dispatch via
/// [`crate::wire::auth`]. Implements `StartupHandler` directly
/// (rather than via `NoopStartupHandler`) so we can decide
/// accept-vs-reject *before* any auth-success frame goes on the
/// wire — and so we can drive the SCRAM multi-message exchange.
///
/// Flow:
///
/// 1. **Startup** → negotiate protocol, save metadata, validate the
///    claimed `database` (must be `"postgres"` in v0), look up the
///    role's credential via [`hba::lookup`].
/// 2. Dispatch on the loaded [`RolPassword`]:
///    - `None` / `UnknownRole` → trust → `finish_authentication` +
///      done.
///    - `Scram(verifier)` → kick off SASL: send
///      `AuthenticationSASL(["SCRAM-SHA-256"])`, store
///      [`ScramServer`] in `SaslPhase::AwaitingClientFirst`.
///    - `Md5(_)` → reject `0A000` (method not yet implemented).
///    - `Unsupported` → reject `28000`.
/// 3. **SASLInitialResponse** (only if `AwaitingClientFirst`) →
///    decode the body as `client-first-message`, call
///    `ScramServer::on_client_first`, send `SASLContinue`,
///    transition to `AwaitingClientFinal`.
/// 4. **SASLResponse** (only if `AwaitingClientFinal`) → decode as
///    `client-final-message`, call `ScramServer::on_client_final`.
///    On `AuthFailed` → FATAL `28P01`. On success → send
///    `SASLFinal(server-final)`, then `finish_authentication`.
#[derive(Default)]
struct PgTransportStartup {
    sasl: Mutex<SaslPhase>,
}

#[async_trait]
impl StartupHandler for PgTransportStartup {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                self.handle_startup(client, startup).await
            }
            PgWireFrontendMessage::PasswordMessageFamily(msg) => {
                // SCRAM messages arrive here. Read current phase
                // and dispatch; we never hold the Mutex across
                // await (pgwire requires Send), so take ownership
                // of the phase via mem::replace + restore.
                let phase = {
                    let mut guard = self.sasl.lock().expect("sasl phase mutex");
                    std::mem::replace(&mut *guard, SaslPhase::Done)
                };
                match phase {
                    SaslPhase::AwaitingClientFirst { server } => {
                        self.handle_client_first(client, msg, *server).await
                    }
                    SaslPhase::AwaitingClientFinal { server } => {
                        self.handle_client_final(client, msg, *server).await
                    }
                    SaslPhase::Idle | SaslPhase::Done => {
                        // PasswordMessageFamily arrived outside of
                        // an active SASL exchange — protocol
                        // violation, reject.
                        reject(
                            client,
                            &AuthOutcome::Reject {
                                sqlstate: "08P01",
                                message: "pg_transport: unexpected password message".to_string(),
                            },
                        )
                        .await
                    }
                }
            }
            // Other frontend messages aren't expected during
            // startup; pgwire's outer loop won't deliver them
            // until we reach ReadyForQuery anyway.
            _ => Ok(()),
        }
    }
}

impl PgTransportStartup {
    /// Phase 1: handle the StartupMessage. Either short-circuits
    /// to accept/reject for non-SCRAM, or kicks off the SASL
    /// exchange.
    async fn handle_startup<C>(
        &self,
        client: &mut C,
        startup: &pgwire::messages::startup::Startup,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgwire::api::auth::protocol_negotiation(client, startup).await?;
        pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);

        let user = client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_default();
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| user.clone()); // PG semantics: db defaults to user
        let host = client.socket_addr().ip().to_string();
        let ssl = client.is_secure();

        pgrx::log!(
            "pgwire-v3 startup: peer={} tls={} proto={:?} user={user:?} db={database:?}",
            client.socket_addr(),
            ssl,
            client.protocol_version(),
        );

        // Slot SPI is pinned to "postgres" at slot startup; reject
        // any other claimed database cleanly.
        if database != "postgres" {
            return reject(client, &AuthOutcome::wrong_database(&database)).await;
        }

        let pw = hba::lookup(&hba::HbaLookup {
            user: &user,
            database: &database,
            host: &host,
            ssl,
        });

        match pw {
            // Trust path: no credential required.
            RolPassword::None | RolPassword::UnknownRole => accept(client).await,
            // SCRAM path: kick off the SASL exchange.
            RolPassword::Scram(verifier) => {
                let server = Box::new(ScramServer::new(verifier));
                {
                    let mut guard = self.sasl.lock().expect("sasl phase mutex");
                    *guard = SaslPhase::AwaitingClientFirst { server };
                }
                // pgwire's process_socket loop dispatches messages
                // by connection state. We must move out of
                // AwaitingStartup into AuthenticationInProgress so
                // the SASLInitialResponse / SASLResponse that
                // follow get routed back to this handler instead
                // of being dropped. Mirrors pgwire's own
                // SASLAuthStartupHandler.
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec![SCRAM_SHA_256.to_string()],
                    )))
                    .await?;
                Ok(())
            }
            // MD5 lands later; reject 0A000 for now.
            RolPassword::Md5(_) => {
                reject(client, &AuthOutcome::unimplemented(AuthMethod::Md5)).await
            }
            // Unknown / unsupported credential format. Closed-fail.
            RolPassword::Unsupported => {
                reject(
                    client,
                    &AuthOutcome::Reject {
                        sqlstate: "28000",
                        message: "pg_transport: role has no supported credential format"
                            .to_string(),
                    },
                )
                .await
            }
        }
    }

    /// Phase 2: process the SASLInitialResponse (client-first-message).
    async fn handle_client_first<C>(
        &self,
        client: &mut C,
        msg: pgwire::messages::startup::PasswordMessageFamily,
        mut server: ScramServer,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sasl_initial = msg.into_sasl_initial_response()?;
        if sasl_initial.auth_method != SCRAM_SHA_256 {
            return reject(
                client,
                &AuthOutcome::Reject {
                    sqlstate: "28P01",
                    message: format!(
                        "pg_transport: unsupported SASL mechanism {:?}",
                        sasl_initial.auth_method
                    ),
                },
            )
            .await;
        }
        let data = sasl_initial.data.unwrap_or_default();
        match server.on_client_first(&data) {
            Ok(server_first) => {
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLContinue(Bytes::from(server_first.into_bytes())),
                    ))
                    .await?;
                {
                    let mut guard = self.sasl.lock().expect("sasl phase mutex");
                    *guard = SaslPhase::AwaitingClientFinal {
                        server: Box::new(server),
                    };
                }
                Ok(())
            }
            Err(e) => reject_scram(client, &e).await,
        }
    }

    /// Phase 3: process the SASLResponse (client-final-message),
    /// verify, finish auth or reject.
    async fn handle_client_final<C>(
        &self,
        client: &mut C,
        msg: pgwire::messages::startup::PasswordMessageFamily,
        mut server: ScramServer,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sasl_response = msg.into_sasl_response()?;
        match server.on_client_final(&sasl_response.data) {
            Ok(server_final) => {
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLFinal(Bytes::from(server_final.into_bytes())),
                    ))
                    .await?;
                accept(client).await
            }
            Err(e) => reject_scram(client, &e).await,
        }
    }
}

/// Finish a successful auth: generate PID + secret key, then send
/// the `AuthenticationOk` + `ParameterStatus` + `BackendKeyData` +
/// `ReadyForQuery` quartet via pgwire's `finish_authentication`.
async fn accept<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let pid_gen = RandomPidSecretKeyGenerator::default();
    let (pid, secret_key) = pid_gen.generate(client);
    client.set_pid_and_secret_key(pid, secret_key);
    finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
    Ok(())
}

/// Send a FATAL ErrorResponse for an [`AuthOutcome::Reject`] and
/// return `Ok(())` so pgwire treats the connection as cleanly
/// closed rather than as a transport-level error.
async fn reject<C>(client: &mut C, outcome: &AuthOutcome) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let AuthOutcome::Reject { sqlstate, message } = outcome else {
        unreachable!("reject called with non-Reject outcome");
    };
    pgrx::log!("pgwire-v3 auth reject: sqlstate={sqlstate} message={message:?}");
    let info = ErrorInfo::new("FATAL".to_string(), sqlstate.to_string(), message.clone());
    let err_response: ErrorResponse = info.into();
    client
        .send(PgWireBackendMessage::ErrorResponse(err_response))
        .await?;
    Ok(())
}

/// Map a SCRAM-level error to the right wire SQLSTATE + message
/// and emit a FATAL ErrorResponse. Both wrong-password and
/// malformed-message map to `28P01` (PG does the same — leaking
/// the difference would be an info-disclosure footgun).
async fn reject_scram<C>(client: &mut C, err: &ScramError) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let (sqlstate, message) = match err {
        ScramError::AuthFailed | ScramError::Malformed(_) => (
            "28P01",
            "pg_transport: SCRAM authentication failed".to_string(),
        ),
        ScramError::NonceGenerationFailed => (
            "XX000",
            "pg_transport: SCRAM nonce generation failed (entropy source error)".to_string(),
        ),
    };
    // We log the underlying detail server-side for debugging.
    pgrx::log!("pgwire-v3 SCRAM reject: {err:?}");
    let outcome = AuthOutcome::Reject { sqlstate, message };
    reject(client, &outcome).await
}

// ---------------------------------------------------------------------------
// Simple-query handler — phase 4b SPI bridge
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SimpleQuery;

#[async_trait]
impl SimpleQueryHandler for SimpleQuery {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        pgrx::log!("pgwire-v3 simple query: {query:?}");
        crate::backend::spi_bridge::execute_simple_query(query)
    }
}
