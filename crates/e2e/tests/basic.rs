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
