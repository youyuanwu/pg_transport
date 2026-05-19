//! Demonstrative end-to-end tests covering the phase-4b acceptance
//! matrix from [`docs/design/roadmap.md`](../../../../docs/design/roadmap.md):
//! `SELECT 1` round-trip, multi-column / multi-row results, the
//! division-by-zero error path, and multi-statement / xact-control
//! interactions.
//!
//! All tests share a single cluster via [`e2e::Cluster::shared`];
//! they use distinct table names to avoid stepping on each other
//! when cargo runs them in parallel.
//!
//! ## Why everything uses `simple_query`
//!
//! pg_transport v0 only implements the simple-query (`'Q'`) wire path
//! — extended query (`Parse` / `Bind` / `Execute`) lands in roadmap
//! phase 9. tokio-postgres' typed accessors (`query`, `query_one`,
//! `execute`) all go through extended query, so they fail against the
//! pg_transport port with `FATAL: This feature is not implemented`.
//! Use [`tokio_postgres::Client::simple_query`] plus the
//! [`e2e::first_text_cell`] / [`e2e::text_column`] helpers — every
//! value comes back as `Option<&str>` in text format (exactly what
//! pg_transport's SPI bridge emits via `SPI_getvalue`).
//!
//! When phase 9 lands, this file's tests can be reshaped to use the
//! typed accessors and a separate suite added for the simple-query
//! path.

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
        "pg_transport pool: all ",
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
