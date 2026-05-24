//! Demonstrative end-to-end tests covering the phase-4b through
//! phase-9 acceptance matrix from
//! [`docs/design/roadmap.md`](../../../../docs/design/roadmap.md).
//!
//! All tests share a single cluster via [`e2e::Cluster::shared`];
//! they use distinct table / role names to avoid stepping on each
//! other when cargo runs them in parallel.
//!
//! ## Simple-query vs extended-query
//!
//! - **Simple-query** (`'Q'`): use
//!   [`tokio_postgres::Client::simple_query`] plus the
//!   [`e2e::first_text_cell`] / [`e2e::text_column`] helpers. All
//!   values come back as `Option<&str>` in text format. Phase 4b
//!   onwards.
//! - **Extended-query** (`Parse` / `Bind` / `Describe` / `Execute`
//!   / `Sync`): tokio-postgres' typed accessors (`query`,
//!   `query_one`, `execute`, `prepare`) all use this path. Phase 9
//!   onwards. Results come back as typed `Row`s; parameters are
//!   bound in binary format by default.
//!
//! Pre-phase-9 commits asserted simple-query exclusively because
//! the wire layer returned `'This feature is not implemented'`
//! for any extended message.

use anyhow::Result;
use e2e::{Cluster, first_text_cell, text_column};

#[tokio::test]
async fn select_one_roundtrip() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let msgs = client.simple_query("SELECT 1").await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("1"));
    Ok(())
}

#[tokio::test]
async fn multi_column_select() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let msgs = client
        .simple_query("SELECT 1+1 AS sum, 'hello' AS greeting")
        .await?;
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("at least one row");
    assert_eq!(row.get(0), Some("2"));
    assert_eq!(row.get(1), Some("hello"));
    Ok(())
}

#[tokio::test]
async fn catalog_multi_row() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let msgs = client
        .simple_query(
            "SELECT relname FROM pg_class \
             WHERE relname IN ('pg_class','pg_type','pg_proc') \
             ORDER BY relname",
        )
        .await?;
    assert_eq!(text_column(&msgs), vec!["pg_class", "pg_proc", "pg_type"]);
    Ok(())
}

#[tokio::test]
async fn division_by_zero_surfaces_as_db_error() -> Result<()> {
    // Phase-4b panic-to-wire path: SPI raises PG ERROR -> our
    // catch_unwind / panic_to_pgwire converts to ErrorResponse.
    // The connection must remain usable for the next query.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let err = client
        .simple_query("SELECT 1/0")
        .await
        .expect_err("division by zero should fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    assert!(
        db.message().contains("division by zero"),
        "expected PG-native message, got: {}",
        db.message()
    );
    let msgs = client.simple_query("SELECT 42").await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("42"));
    Ok(())
}

#[tokio::test]
async fn multi_statement_simple_query() -> Result<()> {
    // Q25 raw_parser split: multi-statement bodies become two
    // CommandComplete + two Row messages in one round-trip.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let msgs = client.simple_query("SELECT 1; SELECT 2").await?;
    assert_eq!(text_column(&msgs), vec!["1", "2"]);
    Ok(())
}

#[tokio::test]
async fn xact_control_begin_commit() -> Result<()> {
    // BEGIN / COMMIT must be intercepted around SPI's atomic-mode
    // rejection and routed through the xact-block API. Inside the
    // block, DDL+DML must commit atomically.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let table = "e2e_xact_commit_demo";
    let _ = client
        .simple_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await;
    client
        .simple_query(&format!(
            "BEGIN; \
             CREATE TABLE {table} (n int); \
             INSERT INTO {table} VALUES (1), (2), (3); \
             COMMIT"
        ))
        .await?;
    let msgs = client
        .simple_query(&format!("SELECT count(*) FROM {table}"))
        .await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("3"));
    client.simple_query(&format!("DROP TABLE {table}")).await?;
    Ok(())
}

#[tokio::test]
async fn xact_control_rollback_discards_changes() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let table = "e2e_xact_rollback_demo";
    client
        .simple_query(&format!(
            "DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table} (n int); \
             INSERT INTO {table} VALUES (1)"
        ))
        .await?;
    client
        .simple_query(&format!(
            "BEGIN; INSERT INTO {table} VALUES (2), (3); ROLLBACK"
        ))
        .await?;
    let msgs = client
        .simple_query(&format!("SELECT count(*) FROM {table}"))
        .await?;
    assert_eq!(
        first_text_cell(&msgs).as_deref(),
        Some("1"),
        "rollback must discard INSERTs"
    );
    client.simple_query(&format!("DROP TABLE {table}")).await?;
    Ok(())
}

#[tokio::test]
async fn syntax_error_surfaces_pg_native_message() -> Result<()> {
    // Q25 raw_parser swap gave us PG-native syntax-error messages
    // for free; pin that.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let err = client
        .simple_query("SELECTT 1")
        .await
        .expect_err("bad token should fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    assert!(
        db.message().contains("syntax error"),
        "expected PG-native syntax error, got: {}",
        db.message()
    );
    Ok(())
}

#[tokio::test]
async fn admin_connect_speaks_full_protocol() -> Result<()> {
    // The admin connection bypasses pg_transport and talks to
    // vanilla PG directly, so it supports the full extended-query
    // protocol — verify by sending a parameterised query that would
    // fail against the pg_transport port.
    let c = Cluster::shared().await;
    let admin = c.admin_connect("postgres").await?;
    let row = admin.query_one("SELECT $1::int + 1", &[&41i32]).await?;
    assert_eq!(row.get::<_, i32>(0), 42);
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 7.1 auth-dispatch tests
// ---------------------------------------------------------------------------
//
// `Cluster::new` boots with `pg_transport.auth_source = 'pg_hba'`;
// `wire::auth::hba::lookup` is stubbed to AuthMethod::Trust in 7.1,
// so connections claiming `database = "postgres"` are accepted and
// every other database is rejected before auth via the new
// startup-handler database check.

#[tokio::test]
async fn auth_trust_accepts_postgres_database() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let msgs = client.simple_query("SELECT 1").await?;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "expected at least one Row message after trust auth"
    );
    Ok(())
}

#[tokio::test]
async fn auth_non_postgres_database_rejected() -> Result<()> {
    // v0 slot SPI is pinned to "postgres"; connections claiming a
    // different database get a clear FATAL (SQLSTATE 3D000,
    // invalid_catalog_name) before any query runs. This guards
    // against silently routing all DBs to the postgres database.
    let c = Cluster::shared().await;
    let err = c
        .connect("template1")
        .await
        .expect_err("connect to non-postgres db should fail");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("3D000") || msg.contains("template1"),
        "expected wrong-database error to mention 3D000 or the database name, got: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn auth_dispatch_log_line_present() -> Result<()> {
    // The phase-7 startup handler emits a structured log line
    // `pgwire-v3 startup: peer=… tls=… proto=… user=… db=…`
    // (the user= / db= suffixes are the 7.1 additions). Force a
    // connection to guarantee at least one such line; assert the
    // `user=` marker exists.
    let c = Cluster::shared().await;
    let _client = c.connect("postgres").await?;
    let log = std::fs::read_to_string(c.log_path())?;
    assert!(
        log.contains("user=\"postgres\""),
        "expected the phase-7 auth-dispatch log line with user=…; tail:\n{}",
        log.lines().rev().take(20).collect::<Vec<_>>().join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn auth_scram_stored_role_accepts_correct_password() -> Result<()> {
    // Phase 7.3: verifier loader + SCRAM-SHA-256 server-side
    // verifier (StoredKey path) end-to-end. tokio-postgres
    // negotiates SASL with the server and runs the proof; we
    // verify it succeeds *and* that a follow-up query works
    // (proves the post-auth ReadyForQuery + SimpleQuery handler
    // chain is intact after the multi-message auth exchange).
    let c = Cluster::shared().await;
    let admin = c.admin_connect("postgres").await?;

    let role = "e2e_scram_ok";
    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    // PG 14+ default `password_encryption` = scram-sha-256, so
    // CREATE ROLE … PASSWORD 'pw' stores a SCRAM verifier. The
    // role needs pg_read_all_data to read pg_class for the
    // verification query below.
    admin
        .simple_query(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'correctpw' \
             IN ROLE pg_read_all_data"
        ))
        .await?;

    // The SCRAM exchange happens transparently inside connect_as.
    let client = c.connect_as(role, "postgres", Some("correctpw")).await?;
    let msgs = client
        .simple_query("SELECT relname FROM pg_class WHERE relname = 'pg_authid'")
        .await?;
    assert_eq!(
        e2e::first_text_cell(&msgs).as_deref(),
        Some("pg_authid"),
        "SCRAM-authed connection must be able to query"
    );

    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    Ok(())
}

#[tokio::test]
async fn auth_scram_wrong_password_rejected_with_28p01() -> Result<()> {
    // Wrong password → SCRAM proof verification fails →
    // FATAL 28P01. The wire-level rejection is intentionally
    // identical to malformed-message rejection (no info leak).
    let c = Cluster::shared().await;
    let admin = c.admin_connect("postgres").await?;
    let role = "e2e_scram_wrong";
    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    admin
        .simple_query(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'realpw' IN ROLE pg_read_all_data"
        ))
        .await?;

    let err = c
        .connect_as(role, "postgres", Some("wrongpw"))
        .await
        .expect_err("wrong SCRAM password must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("28P01") || msg.contains("SCRAM") || msg.contains("authentication failed"),
        "expected SCRAM auth-failed error, got: {msg}"
    );

    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    Ok(())
}

#[tokio::test]
async fn auth_passwordless_role_routes_to_trust() -> Result<()> {
    // Verifier loader returns RolPassword::None for a role with
    // NULL rolpassword; hba::lookup maps that to AuthMethod::Trust,
    // matching the v0 default. This pins that the loader handles
    // NULL correctly (the SPI path otherwise raises an Err for
    // empty result sets — see user-memory pgrx.md).
    let c = Cluster::shared().await;
    let admin = c.admin_connect("postgres").await?;
    let role = "e2e_trust_user";
    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    admin
        .simple_query(&format!(
            "CREATE ROLE {role} LOGIN IN ROLE pg_read_all_data"
        ))
        .await?;

    let client = c.connect_as(role, "postgres", None).await?;
    let msgs = client.simple_query("SELECT 1").await?;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "passwordless role should trust-auth and run SELECT 1"
    );

    let _ = admin
        .simple_query(&format!("DROP ROLE IF EXISTS {role}"))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Server-observability tests — ported from the legacy `just smoke` recipe.
// ---------------------------------------------------------------------------
//
// These don't fit the "drive a tokio-postgres Client" shape: they assert on
// the server log file (bgworker startup chain fired in the right order) and
// on `psql` stdout (pgwire `CommandComplete` Tag rendering — tokio-postgres
// parses the tag into a typed value and never surfaces the "(N rows)" string).

#[tokio::test]
async fn server_log_records_bgworker_boot_chain() -> Result<()> {
    // By the time `Cluster::shared` returns, `wait_ready` has done at
    // least one client connect + `SELECT 1` through the wire, so every
    // log line below should already be present.
    let c = Cluster::shared().await;
    // Force a query through the wire to guarantee the startup-handshake
    // log line, even if `wait_ready`'s connection got dropped before this
    // test was scheduled.
    let client = c.connect("postgres").await?;
    let _ = client.simple_query("SELECT 1").await?;

    let log = std::fs::read_to_string(c.log_path())?;
    for needle in [
        "frontend: tokio runtime ready",
        // Autoscaling pool: FE binds the single UDS listener at
        // boot; slots connect on demand (see
        // docs/design/deferred/slot-readiness.md §2.0).
        "pg_transport pool: bound frontend listener at",
        "tcp_handoff: listening on 127.0.0.1:5454",
        "wire=pgwire-v3",
        "pgwire-v3 startup: peer=",
    ] {
        assert!(
            log.contains(needle),
            "missing log substring {needle:?}; tail follows:\n---\n{}\n---",
            log.lines().rev().take(40).collect::<Vec<_>>().join("\n")
        );
    }
    Ok(())
}

#[tokio::test]
async fn psql_renders_simple_query_row_count_tags() -> Result<()> {
    // tokio-postgres parses CommandComplete into a typed value, so the
    // "(1 row)" / "(3 rows)" psql formatting can only be asserted via
    // a real psql client. This pins the pgwire Tag's rows-counter path
    // in `spi_bridge::run_via_spi`.
    let c = Cluster::shared().await;

    let out1 = c.psql("postgres", "SELECT 1")?;
    assert!(
        out1.contains("(1 row)"),
        "missing '(1 row)' in psql output:\n{out1}"
    );

    let out3 = c.psql(
        "postgres",
        "SELECT relname FROM pg_class \
         WHERE relname IN ('pg_class','pg_type','pg_proc') ORDER BY relname",
    )?;
    assert!(
        out3.contains("(3 rows)"),
        "missing '(3 rows)' in psql output:\n{out3}"
    );
    Ok(())
}

#[tokio::test]
async fn division_by_zero_does_not_fall_through_to_unknown_panic() -> Result<()> {
    // Regression guard for `spi_bridge::panic_to_pgwire`'s CaughtError
    // downcast. If the downcast falls through to the generic branch we'd
    // see "unknown panic payload" or "pg_transport panic: …" in place of
    // the PG-native message. The tokio-postgres-only `division_by_zero_*`
    // test above asserts the *positive* (correct message present); this
    // one asserts the *negative* (no fall-through artefact) via psql so
    // we'd catch both branches even if the message got concatenated.
    let c = Cluster::shared().await;
    let out = c.psql("postgres", "SELECT 1/0")?;
    assert!(
        out.contains("division by zero"),
        "expected PG-native message in psql output:\n{out}"
    );
    assert!(
        !out.contains("unknown panic payload"),
        "panic_to_pgwire downcast fell through to generic branch:\n{out}"
    );
    assert!(
        !out.contains("pg_transport panic:"),
        "panic_to_pgwire downcast hit the String/&str branch:\n{out}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 8 TLS tests
// ---------------------------------------------------------------------------
//
// The e2e harness generates a self-signed cert at cluster boot
// (see `Cluster::new`) and points `pg_transport.tls_{cert,key}_file`
// at it. The wire layer's slot bgworkers load the cert once at
// startup; the pgwire `process_socket` second arg is now
// `Some(acceptor)` (was `None` pre-7.3).
//
// We test via `psql` because PG's built-in libpq has TLS built in
// via libssl — provided pgrx's PG was built with `--with-openssl`
// (see `just init`). tokio-postgres' default `NoTls` connector
// can't do TLS, so this side of the test would otherwise need a
// `tokio-postgres-rustls` dev-dep; psql is simpler.

#[tokio::test]
async fn tls_sslmode_require_succeeds() -> Result<()> {
    // PGSSLMODE=require → psql sends SSLRequest, expects 'S' from
    // the server, does the TLS handshake, then runs SELECT 1 over
    // the encrypted stream. The self-signed cert is not validated
    // (sslmode=require doesn't check the chain — use verify-ca /
    // verify-full for that, which would need a CA bundle we don't
    // ship in v0).
    let c = Cluster::shared().await;
    let out = c.psql_with_sslmode("postgres", "SELECT 1", "require")?;
    assert!(
        out.contains("(1 row)"),
        "expected SELECT 1 to succeed over TLS; got:\n{out}"
    );
    Ok(())
}

#[tokio::test]
async fn tls_sslmode_disable_still_works() -> Result<()> {
    // Sanity check: enabling TLS in the cluster must NOT break
    // plaintext connections. pgwire's process_socket peeks for
    // SSLRequest before doing anything TLS-specific, so a client
    // that doesn't send SSLRequest just proceeds cleartext.
    let c = Cluster::shared().await;
    let out = c.psql_with_sslmode("postgres", "SELECT 1", "disable")?;
    assert!(
        out.contains("(1 row)"),
        "expected plaintext SELECT 1 to still work; got:\n{out}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 9 extended-query tests
// ---------------------------------------------------------------------------
//
// tokio-postgres' `query` / `query_one` / `execute` / `prepare`
// methods all go through Parse / Bind / Describe / Execute / Sync.
// Pre-phase-9 the wire layer returned `'This feature is not
// implemented'` for every extended-protocol message, so these
// tests would have failed at the first call.

#[tokio::test]
async fn extended_query_no_params_typed_row() -> Result<()> {
    // The smallest possible extended-query path: `query` with no
    // params, one column, one row. Exercises Parse + Bind +
    // Describe + Execute round-trip end-to-end. The typed
    // `row.get::<_, i32>(0)` requires Describe to have returned
    // the right column OID (INT4) — proves our
    // `SPI_plan_get_plan_sources` → `resultDesc` extraction works.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let row = client.query_one("SELECT 1::int", &[]).await?;
    let v: i32 = row.get(0);
    assert_eq!(v, 1);
    Ok(())
}

#[tokio::test]
async fn extended_query_binary_int_param() -> Result<()> {
    // tokio-postgres binds `&5_i32` in BINARY format with the
    // INT4 OID. Exercises `OidReceiveFunctionCall` for INT4
    // plus the resolved-param-type path
    // (`SPI_getargtypeid` extracts the OID, the wire layer
    // round-trips it back to pgwire via `get_parameter_types`).
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let row = client.query_one("SELECT $1::int + 10", &[&5_i32]).await?;
    let v: i32 = row.get(0);
    assert_eq!(v, 15);
    Ok(())
}

#[tokio::test]
async fn extended_query_text_param_and_null() -> Result<()> {
    // Two parameters of distinct types (TEXT, INT4) and a NULL.
    // Verifies parameter-array indexing + null-mask handling
    // (`b'n'` for null entries in SPI's nulls argument).
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let row = client
        .query_one(
            "SELECT $1::text || ' #' || COALESCE($2::text, 'NULL'), $3::int",
            &[&"hello", &Option::<&str>::None, &42_i32],
        )
        .await?;
    let s: &str = row.get(0);
    let n: i32 = row.get(1);
    assert_eq!(s, "hello #NULL");
    assert_eq!(n, 42);
    Ok(())
}

#[tokio::test]
async fn extended_query_multi_row_select() -> Result<()> {
    // `query` returns Vec<Row>. Exercises the per-row materialise
    // path and ordering through pgwire's DataRow stream.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let rows = client
        .query(
            "SELECT relname FROM pg_class \
             WHERE relname IN ('pg_class','pg_type','pg_proc') \
             ORDER BY relname",
            &[],
        )
        .await?;
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        names,
        vec![
            "pg_class".to_string(),
            "pg_proc".to_string(),
            "pg_type".to_string()
        ]
    );
    Ok(())
}

#[tokio::test]
async fn extended_query_prepare_then_reuse() -> Result<()> {
    // `prepare` issues Parse only; subsequent `query` calls
    // against the Statement reuse it via Bind+Execute. This
    // proves the kept plan survives multiple Execute calls and
    // that pgwire's PortalStore keeps the StoredStatement
    // referenced across Sync messages.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let stmt = client.prepare("SELECT $1::int * 2").await?;
    for n in 1..=5 {
        let row = client.query_one(&stmt, &[&n]).await?;
        let got: i32 = row.get(0);
        assert_eq!(got, n * 2);
    }
    Ok(())
}

#[tokio::test]
async fn extended_query_direct_backend_prepare_reports_types() -> Result<()> {
    // Commit 4 target: direct backend must handle Parse + Describe
    // cleanly. We do not Execute the prepared statement yet; that
    // remains the commit-5 follow-up.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;

    let stmt = client.prepare("SELECT $1::int + 1, 42::int").await?;

    assert_eq!(stmt.params().len(), 1, "expected one inferred parameter");
    assert_eq!(stmt.params()[0], tokio_postgres::types::Type::INT4);

    assert_eq!(stmt.columns().len(), 2, "expected two result columns");
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );
    assert_eq!(
        stmt.columns()[1].type_(),
        &tokio_postgres::types::Type::INT4
    );
    Ok(())
}

#[tokio::test]
async fn extended_query_direct_backend_executes_typed_row() -> Result<()> {
    // Commit 5 target: direct backend must execute prepared
    // statements (Bind + Execute), not just Parse + Describe.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;

    let row = client.query_one("SELECT $1::int + 10", &[&5_i32]).await?;
    let v: i32 = row.get(0);
    assert_eq!(v, 15);
    Ok(())
}

#[tokio::test]
async fn extended_query_execute_returns_row_count() -> Result<()> {
    // `execute` runs an INSERT / UPDATE / DELETE without
    // RETURNING and returns the affected row count from the
    // CommandComplete tag's numeric suffix. Exercises the
    // utility / DML-no-RETURNING branch (`SPI_tuptable.is_null()`
    // or empty result_schema) plus the `command_tag_from_rc`
    // mapping for SPI_OK_INSERT / _UPDATE / _DELETE.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let table = "e2e_ext_exec";
    let _ = client
        .execute(&format!("DROP TABLE IF EXISTS {table}"), &[])
        .await?;
    client
        .execute(&format!("CREATE TABLE {table} (id int, val text)"), &[])
        .await?;
    let inserted = client
        .execute(
            &format!("INSERT INTO {table} VALUES ($1, $2), ($3, $4)"),
            &[&1_i32, &"one", &2_i32, &"two"],
        )
        .await?;
    assert_eq!(inserted, 2, "INSERT row count");

    let updated = client
        .execute(
            &format!("UPDATE {table} SET val = 'X' WHERE id = $1"),
            &[&1_i32],
        )
        .await?;
    assert_eq!(updated, 1, "UPDATE row count");

    let deleted = client
        .execute(&format!("DELETE FROM {table} WHERE id >= $1"), &[&0_i32])
        .await?;
    assert_eq!(deleted, 2, "DELETE row count");

    let _ = client.execute(&format!("DROP TABLE {table}"), &[]).await?;
    Ok(())
}

#[tokio::test]
async fn extended_query_syntax_error_keeps_connection_usable() -> Result<()> {
    // Parse-time syntax error must surface as a typed DbError
    // (not connection death) and the connection must remain
    // usable for the next query. Mirrors the simple-query
    // `division_by_zero_*` test but for the extended path.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let err = client
        .query_one("SELECTT 1", &[])
        .await
        .expect_err("syntax error should fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    assert!(
        db.message().contains("syntax error"),
        "expected PG-native syntax-error message, got: {}",
        db.message()
    );
    // Connection still works for a follow-up query.
    let row = client.query_one("SELECT 99::int", &[]).await?;
    let v: i32 = row.get(0);
    assert_eq!(v, 99);
    Ok(())
}

#[tokio::test]
async fn extended_query_division_by_zero_surfaces_as_db_error() -> Result<()> {
    // Execute-time PG ERROR (not Parse-time). Exercises the
    // catch_unwind path inside `extended::execute` plus
    // AbortCurrentTransaction recovery. Connection must remain
    // usable afterwards.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    let err = client
        .query_one("SELECT $1::int / $2::int", &[&1_i32, &0_i32])
        .await
        .expect_err("division by zero should fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    assert!(
        db.message().contains("division by zero"),
        "expected PG-native message, got: {}",
        db.message()
    );
    let row = client.query_one("SELECT 1::int", &[]).await?;
    let v: i32 = row.get(0);
    assert_eq!(v, 1);
    Ok(())
}

#[tokio::test]
async fn extended_query_per_handoff_state_isolation() -> Result<()> {
    // Per-handoff reset correctness — the v0 design's biggest
    // correctness risk per `backend-wire.md §8 Q1`. Two sequential
    // connections (very likely landing on the same slot, since the
    // pool is small) must not share prepared-statement state.
    //
    // Client A prepares an unnamed-portal Statement (tokio-
    // postgres' `prepare` uses an explicit name; we craft an
    // explicit-name PREPARE at the wire layer via `simple_query` so
    // its lifetime is clearly per-session, not bound to tokio-
    // postgres' Statement struct).
    //
    // Because the SPL pool is sized so much larger than the test
    // concurrency, the two connections might land on different
    // slots — in which case the test still passes trivially. The
    // useful assertion is: regardless of routing, no client ever
    // sees a stale prepared statement from a previous connection.
    let c = Cluster::shared().await;
    {
        let client_a = c.connect("postgres").await?;
        client_a
            .simple_query("PREPARE phase9_reset_probe AS SELECT 7")
            .await?;
        // client_a drops here — connection closes; pgwire's
        // DefaultClient (and the PortalStore inside it) is dropped;
        // our SpiPlan::Drop runs SPI_freeplan.
    }
    // Give the slot a beat to recycle back to the pool.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client_b = c.connect("postgres").await?;
    let err = client_b
        .simple_query("EXECUTE phase9_reset_probe")
        .await
        .expect_err("client B must not see client A's prepared statement");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    // PG error code for "prepared statement does not exist" is
    // 26000 (invalid_sql_statement_name). The message also
    // mentions the name.
    assert!(
        db.message().contains("phase9_reset_probe") || db.code().code() == "26000",
        "expected 'no such prepared statement' error, got: {} ({})",
        db.message(),
        db.code().code()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Simple-query direct backend
// ---------------------------------------------------------------------------
//
// Mirrors the core simple-query tests above against
// `pg_transport.execution_backend = 'direct'`, exercising the
// Portal* + WireDestReceiver path landed per
// [deferred/simple-query-direct-path.md](../../../../docs/design/deferred/simple-query-direct-path.md).
//
// The default backend stays `spi`; these tests `SET` per session
// to opt into direct.

#[tokio::test]
async fn simple_direct_select_one_roundtrip() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let msgs = client.simple_query("SELECT 1").await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("1"));
    Ok(())
}

#[tokio::test]
async fn simple_direct_multi_column_select() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let msgs = client
        .simple_query("SELECT 1+1 AS sum, 'hello' AS greeting")
        .await?;
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("at least one row");
    assert_eq!(row.get(0), Some("2"));
    assert_eq!(row.get(1), Some("hello"));
    Ok(())
}

#[tokio::test]
async fn simple_direct_multi_statement() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let msgs = client.simple_query("SELECT 1; SELECT 2").await?;
    assert_eq!(text_column(&msgs), vec!["1", "2"]);
    Ok(())
}

#[tokio::test]
async fn simple_direct_xact_control_begin_commit() -> Result<()> {
    // Direct path doesn't need the SPI xact-intercept;
    // PortalRun routes TransactionStmt through ProcessUtility
    // natively. Verifies that round-trip works end-to-end.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let table = "e2e_simple_direct_xact";
    let _ = client
        .simple_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await;
    client
        .simple_query(&format!(
            "BEGIN; \
             CREATE TABLE {table} (n int); \
             INSERT INTO {table} VALUES (1), (2), (3); \
             COMMIT"
        ))
        .await?;
    let msgs = client
        .simple_query(&format!("SELECT count(*) FROM {table}"))
        .await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("3"));
    client.simple_query(&format!("DROP TABLE {table}")).await?;
    Ok(())
}

#[tokio::test]
async fn simple_direct_xact_control_rollback() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let table = "e2e_simple_direct_xact_rb";
    client
        .simple_query(&format!(
            "DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table} (n int); \
             INSERT INTO {table} VALUES (1)"
        ))
        .await?;
    client
        .simple_query(&format!(
            "BEGIN; INSERT INTO {table} VALUES (2), (3); ROLLBACK"
        ))
        .await?;
    let msgs = client
        .simple_query(&format!("SELECT count(*) FROM {table}"))
        .await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("1"));
    client.simple_query(&format!("DROP TABLE {table}")).await?;
    Ok(())
}

#[tokio::test]
async fn simple_direct_syntax_error_surfaces_pg_native_message() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let err = client
        .simple_query("SELECTT 1")
        .await
        .expect_err("syntax error must fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got {err:?}"));
    assert!(
        db.message().contains("syntax error"),
        "expected PG-native message, got: {}",
        db.message()
    );
    // Connection still usable.
    let msgs = client.simple_query("SELECT 1").await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("1"));
    Ok(())
}

#[tokio::test]
async fn simple_direct_division_by_zero_recovers() -> Result<()> {
    // Execute-time PG ERROR through PortalRun. with_xact's
    // AbortCurrentTransaction must reset xact state so the next
    // query on the same connection succeeds.
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let err = client
        .simple_query("SELECT 1/0")
        .await
        .expect_err("division by zero must fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got {err:?}"));
    assert!(
        db.message().contains("division by zero"),
        "expected PG-native message, got: {}",
        db.message()
    );
    let msgs = client.simple_query("SELECT 99").await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("99"));
    Ok(())
}

#[tokio::test]
async fn simple_direct_set_and_read_back() -> Result<()> {
    // Mixed utility + DML in one 'Q': SET sets a GUC; a later
    // SELECT current_setting reads it. Validates that
    // CommandCounterIncrement (or equivalent) fires between
    // statements so the second statement sees the first's
    // effects. (Spec'd as Q2 in the design doc.)
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;
    let msgs = client
        .simple_query(
            "SET LOCAL pg_transport.execution_backend = 'direct'; \
             SELECT current_setting('pg_transport.execution_backend')",
        )
        .await?;
    assert_eq!(first_text_cell(&msgs).as_deref(), Some("direct"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Bug pins — xact-control via extended-query (NOT YET FIXED)
// ---------------------------------------------------------------------------
//
// The simple-query path intercepts BEGIN / COMMIT / ROLLBACK
// before SPI sees them via `spi_bridge.rs::parse_xact_control`,
// routing them through PG's xact-block API so they don't trip
// SPI's atomic-mode rejection. The extended-query paths
// (`extended/spi.rs` and `extended/direct.rs`) have no such sniff:
// when a client sends BEGIN via Parse + Bind + Execute (which is
// what libpq's `PQprepare` / `PQexecPrepared` and tokio-postgres'
// `client.execute("BEGIN", &[])` do), the message is forwarded
// straight to `SPI_execute_plan`, which returns
// `SPI_ERROR_TRANSACTION` because xact-control is illegal inside
// SPI's atomic mode.
//
// Surfaced by `just sysbench` Phase A: sysbench's
// `oltp_read_only` / `oltp_read_write` workloads wrap each tx in
// `con:prepare("BEGIN")` + `stmt.begin:execute()` and hit this
// bug on the first transaction. The `skip_trx=on` workaround
// documented in `bench.md §3.2` bypasses BEGIN/COMMIT entirely.
//
// Fix path: lift the existing sniff logic out of
// `spi_bridge.rs::parse_xact_control` (or its
// `parse_classified_command` helper) into a shared classifier
// that the extended-query Parse hook can also call. On match,
// short-circuit the executor and dispatch the same
// `handle_xact_control` response sequence that the simple-query
// path uses (CommandComplete tag, no DataRow stream).
//
// The two tests below pin the *current* broken behaviour. When
// the bug is fixed they will start failing — at which point flip
// each assertion to expect success, matching what
// `xact_control_begin_commit` already asserts for the
// simple-query path.

#[tokio::test]
async fn xact_control_begin_via_extended_query_spi_backend_pins_bug() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'spi'")
        .await?;

    // `client.execute(sql, &[])` goes through Parse + Bind +
    // Execute on the wire (the extended-query path), even with
    // zero parameters. This is the exact shape sysbench uses
    // for its BEGIN/COMMIT statements.
    let err = client
        .execute("BEGIN", &[])
        .await
        .expect_err("KNOWN BUG: BEGIN via extended-query must currently fail");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    assert!(
        db.message().contains("SPI_ERROR_TRANSACTION"),
        "bug pin: expected SPI_ERROR_TRANSACTION (the symptom of \
         the missing xact-control sniff in extended/spi.rs); got: {}",
        db.message()
    );
    Ok(())
}

#[tokio::test]
async fn xact_control_begin_via_extended_query_direct_backend_pins_bug() -> Result<()> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query("SET pg_transport.execution_backend = 'direct'")
        .await?;

    // Same shape as the SPI-backend test, but with the direct
    // backend selected. The direct path's failure mode is
    // **more severe** than the SPI path's: instead of returning
    // a clean SPI_ERROR_TRANSACTION, it surfaces PG's
    // `unrecognized node type: 2139062143` — that magic number
    // is `0x7f7f7f7f`, PG's WIPE_MEM clobber pattern for freed
    // palloc'd memory. This is a use-after-free symptom on
    // `extended/direct.rs`'s `Portal*` path when handed a
    // utility statement (BEGIN) that the path was not designed
    // to dispatch.
    //
    // Both paths share the same root cause (no xact-control
    // sniff at Parse time), but the direct path's UAF makes
    // fixing it strictly more urgent than the SPI path's
    // graceful rejection. Worth filing as a separate bug if
    // we ever stand up an issue tracker — pinning the
    // `0x7f7f7f7f` symptom here for now so we notice if it
    // changes or regresses into a hard crash.
    let err = client
        .execute("BEGIN", &[])
        .await
        .expect_err("KNOWN BUG: BEGIN via extended-query must currently fail (direct backend)");
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got: {err:?}"));
    let m = db.message();
    assert!(
        m.contains("unrecognized node type") && m.contains("2139062143"),
        "bug pin: expected the 0x7f7f7f7f WIPE_MEM symptom from \
         direct-backend's utility-statement path; got: {m}"
    );
    Ok(())
}
