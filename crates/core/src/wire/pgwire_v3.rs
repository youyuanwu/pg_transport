//! `pgwire-v3` Wire impl built on the [`pgwire`](https://crates.io/crates/pgwire)
//! crate. Brings up FE/BE v3: startup handshake, simple query →
//! SPI bridge, extended query → "not implemented" error frame
//! (default `NoopHandler` behaviour, see
//! [backend-wire.md §2](../../../../docs/design/backend-wire.md)).
//!
//! Phase 7.1 (auth framework): startup handler does proper auth
//! dispatch via [`crate::wire::auth`] — looks up the HBA method
//! (currently stubbed to `Trust`) and either accepts or sends a
//! FATAL `ErrorResponse` for methods we don't yet implement.
//! Database-name check: connections claiming `database != "postgres"`
//! are rejected with SQLSTATE `3D000` since v0 slots hard-code SPI
//! to the `postgres` database.

use std::fmt::Debug;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler, finish_authentication};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, METADATA_DATABASE, METADATA_USER, PgWireServerHandlers,
    PidSecretKeyGenerator, RandomPidSecretKeyGenerator,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::ErrorResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

use super::auth::{self, AuthOutcome, hba};
use super::{Wire, WireCtx};

/// The v0 wire implementation. Stateless marker — per-connection
/// state lives in the pgwire-owned `DefaultClient` and in our handler
/// arc.
pub struct PgwireV3;

impl Wire for PgwireV3 {
    fn name() -> &'static str {
        "pgwire-v3"
    }

    fn run(fd: OwnedFd, ctx: WireCtx) -> anyhow::Result<()> {
        // OwnedFd → std::net::TcpStream. The set_nonblocking call
        // is on the std side because tokio::net::TcpStream::from_std
        // requires non-blocking; failing that lookup gives a
        // confusing "would block" error later instead of upfront.
        let std_stream: std::net::TcpStream = fd.into();
        std_stream
            .set_nonblocking(true)
            .map_err(|e| anyhow::anyhow!("set_nonblocking on client fd: {e}"))?;

        let handlers = Arc::new(PgTransportHandlers::default());

        // Enter the per-bgworker runtime ONCE for the whole handoff.
        // `TcpStream::from_std` requires a running runtime context
        // (it registers the fd with tokio's I/O driver), so we
        // construct it inside `block_on`'s async block.
        // Per backend-handoff.md §3 the slot has nothing else to
        // schedule on this runtime, so single-threading is fine.
        ctx.rt.block_on(async move {
            let tcp_stream = tokio::net::TcpStream::from_std(std_stream)
                .map_err(|e| anyhow::anyhow!("tokio::TcpStream::from_std: {e}"))?;
            process_socket(tcp_stream, None, handlers)
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

/// Per-connection handler bundle. `PgWireServerHandlers` is what
/// `process_socket` consumes; we wire in our own startup +
/// simple-query handlers and let the default `NoopHandler` cover
/// extended query / copy / cancel / error.
///
/// `NoopHandler`'s default impls of `ExtendedQueryHandler` and
/// `SimpleQueryHandler` return a `FATAL 08P01 "This feature is not
/// implemented."` error frame — exactly the phase-4 acceptance
/// criterion for extended query ("clear 'not supported' error frame").
#[derive(Default)]
struct PgTransportHandlers {
    startup: Arc<PgTransportStartup>,
    simple_query: Arc<SimpleQuery>,
}

impl PgWireServerHandlers for PgTransportHandlers {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_query.clone()
    }

    // extended_query_handler / copy_handler / error_handler /
    // cancel_handler default to NoopHandler — phase 4 acceptance
    // criterion (extended query returns "not implemented") is
    // satisfied by NoopHandler's default impl. Phase 9 lands the
    // real ExtendedQueryHandler.
}

// ---------------------------------------------------------------------------
// Startup handler — phase 7.1 auth dispatch
// ---------------------------------------------------------------------------

/// Startup handler that does proper auth dispatch via
/// [`crate::wire::auth`]:
///
/// 1. Negotiates protocol version + saves StartupMessage parameters
///    into client metadata.
/// 2. Validates the claimed `database` parameter (must be
///    `"postgres"` in v0; per-database routing is a later phase).
/// 3. Looks up the auth method via [`hba::lookup`] (stubbed to
///    trust in 7.1; real `hba_getauthmethod` FFI in 7.2).
/// 4. Runs the method via [`auth::run`]:
///    - `Trust` → finish_authentication0 + ReadyForQuery.
///    - `Reject` / unimplemented method → FATAL ErrorResponse, close.
///
/// We implement `StartupHandler` directly rather than going through
/// `NoopStartupHandler` because the latter sends `AuthenticationOk`
/// **before** calling `post_startup` — making it impossible to
/// reject a connection cleanly after method lookup. Implementing
/// `on_startup` ourselves lets us decide accept-vs-reject *before*
/// any auth-success frame goes on the wire.
#[derive(Default)]
struct PgTransportStartup;

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
        // We only handle the initial Startup message; password /
        // SASL response messages flow back here once we implement
        // those methods (phase 7.3+).
        let PgWireFrontendMessage::Startup(ref startup) = message else {
            return Ok(());
        };

        pgwire::api::auth::protocol_negotiation(client, startup).await?;
        pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);

        // Read what the client claims; if either is missing the
        // pgwire-side metadata helpers would already have defaulted
        // to empty — we still treat missing as an error.
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

        // Phase 7.1 hard constraint: slot SPI is pinned to
        // "postgres" at slot startup; per-handoff database routing
        // is a later phase. Reject any other claimed database
        // cleanly rather than silently running queries against the
        // wrong DB.
        if database != "postgres" {
            return reject(client, &AuthOutcome::wrong_database(&database)).await;
        }

        let method = hba::lookup(&hba::HbaLookup {
            user: &user,
            database: &database,
            host: &host,
            ssl,
        });
        let outcome = auth::run(method);

        match outcome {
            AuthOutcome::Accept => {
                // Generate PID + secret key per pgwire's
                // NoopStartupHandler. We don't register a connection
                // manager (cancel-routing is deferred — roadmap §1
                // "deferred" table).
                let pid_gen = RandomPidSecretKeyGenerator::default();
                let (pid, secret_key) = pid_gen.generate(client);
                client.set_pid_and_secret_key(pid, secret_key);

                // `finish_authentication` (public) does the
                // AuthenticationOk + ParameterStatus + BackendKeyData
                // + ReadyForQuery + state-transition dance. Equivalent
                // to NoopStartupHandler's tail.
                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
                Ok(())
            }
            outcome @ AuthOutcome::Reject { .. } => reject(client, &outcome).await,
        }
    }
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
