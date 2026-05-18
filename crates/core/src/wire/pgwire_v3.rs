//! `pgwire-v3` Wire impl built on the [`pgwire`](https://crates.io/crates/pgwire)
//! crate. Brings up FE/BE v3: startup handshake (trust auth in
//! phase 4), simple query → SPI bridge, extended query → "not
//! implemented" error frame (default `NoopHandler` behaviour, see
//! [backend-wire.md §2](../../../../docs/design/backend-wire.md)).
//!
//! Phase 4a (this commit): trust auth + idle simple-query handler
//! (responds with `Tag::new("OK")`). Phase 4b lands the SPI bridge
//! so `SELECT 1` returns `1`.

use std::fmt::Debug;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::Response;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;

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
    startup: Arc<TrustStartup>,
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
// Trust startup
// ---------------------------------------------------------------------------

/// Trust-auth startup handler — accepts every client.
///
/// Phase 4 is `trust` only (no password, no SCRAM, no `hba_getauthmethod`
/// lookup). Phase 7 replaces this with the real auth machinery.
#[derive(Default)]
struct TrustStartup;

#[async_trait]
impl NoopStartupHandler for TrustStartup {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgrx::log!(
            "pgwire-v3 startup: peer={} tls={} proto={:?}",
            client.socket_addr(),
            client.is_secure(),
            client.protocol_version(),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Simple-query handler — phase 4a echo, phase 4b SPI bridge
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
