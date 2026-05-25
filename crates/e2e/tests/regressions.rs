//! End-to-end regression tests for known-gap pins and isolated
//! reproducers that don't belong in the demonstrative
//! [`basic.rs`](basic.rs) suite.
//!
//! Each entry should:
//!
//!   * Cite the audit / issue / PR the gap or regression
//!     originated from.
//!   * State plainly which outcome it currently asserts (the
//!     buggy shape, the fixed shape, or a moving-window post-fix
//!     assertion).
//!   * If `#[ignore]`-gated, explain *why* — typically because
//!     the reproducer takes the cluster down or otherwise
//!     poisons sibling tests, and document the rename / flip
//!     instructions for the post-fix transition.
//!
//! ## Why this lives in its own test binary
//!
//! Cargo gives each `tests/*.rs` file its own binary, which
//! means an independent `Cluster::shared()` `OnceCell`. When a
//! destructive reproducer here crashes the slot, only *this*
//! binary's cluster goes down — the other e2e test binaries
//! ([`basic.rs`](basic.rs) etc.) keep their own clusters intact.
//! Keeps the gap-pin tests from cascading into every other
//! parallel/subsequent e2e test in CI.
//!
//! Within this binary, tests still share one cluster; a
//! destructive test (if any are `#[ignore]`-gated for that
//! reason) will crash it for everything that runs after. Such
//! tests are `#[ignore]`-gated by default — run them
//! individually for diagnosis (see each test's `#[ignore]`
//! message for the exact command).
//!
//! ---
//!
//! # Index of regressions
//!
//! ## §6.3 — Aborted-block rejection (CLOSED)
//!
//! `aborted_block_*_backend_returns_25p02` —
//! Review 2026-05-24 §6 item 3. After a statement raises
//! `ERROR` inside an explicit `BEGIN` block, vanilla PG moves
//! the xact state to `TBLOCK_ABORT` and rejects every
//! subsequent non-`TransactionStmt` with SQLSTATE `25P02`
//! ("current transaction is aborted, commands ignored until
//! end of transaction block" —
//! `ERRCODE_IN_FAILED_SQL_TRANSACTION`). The aborted block
//! stays sticky until the client sends `COMMIT` / `ROLLBACK` /
//! `PREPARE TRANSACTION` / `ROLLBACK TO`.
//!
//! Originally pinned as `aborted_block_*_silently_clears_abort`
//! (asserting the buggy succeed-anyway behaviour, `#[ignore]`-
//! gated because the slot `SIGABRT`'d on the second statement —
//! the audit doc's "AbortCurrentTransaction collapses state to
//! DEFAULT" claim was wrong; the abort actually moves state to
//! `TBLOCK_ABORT` and the next statement walks into a PG-side
//! assert on `GetTransactionSnapshot`).
//!
//! The fix adds an `IsAbortedTransactionBlockState()` check at
//! the top of the per-statement dispatch loops in both backends,
//! firing *before* any `PushActiveSnapshot(GetTransactionSnapshot())`
//! call. The check raises `25P02` for every parsetree that isn't
//! in vanilla's `IsTransactionExitStmt` set
//! (`COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION` /
//! `ROLLBACK TO`) — for the SPI backend we currently only
//! recognise the first two as exit, slightly over-strict for
//! `PREPARE` / `ROLLBACK TO` (which aren't in our `XactCmd`
//! classification yet). Documented in
//! `spi_bridge::is_xact_exit_stmt`.
//!
//! The companion piece is the per-handoff
//! `AbortOutOfAnyTransaction` call now in
//! [`backend::slot::reset_per_handoff_state`](crate::backend::slot):
//! when a client disconnects mid-aborted-block (e.g. the test
//! here doesn't issue an explicit `ROLLBACK` after the 25P02
//! reject), the slot self-recovers to `TBLOCK_DEFAULT` for the
//! next handoff. This is also what lets us avoid the §6.4 gap:
//! a real-client `ROLLBACK` after the abort *would* still
//! crash the direct backend (`with_xact` unconditionally pushes
//! the active snapshot, which assert-fires from `TBLOCK_ABORT`),
//! but our tests sidestep it by closing the connection instead.
//! Resolving the `ROLLBACK`-from-`TBLOCK_ABORT` path properly
//! requires §6.4 (`analyze_requires_snapshot` gating) plus a
//! refactor of `run_one_direct` to pass `InvalidSnapshot` to
//! `PortalStart`. Tracked separately.
//!
//! These tests are no longer `#[ignore]`-gated — they run by
//! default and assert `PostAbortOutcome::Rejected("25P02")`. A
//! regression to the pre-fix behaviour would either return
//! `Succeeded("42")` (check bypassed) or take the cluster down
//! (check missing).

use anyhow::Result;
use e2e::{Cluster, first_text_cell};

/// One of two possible outcomes for the `BEGIN` → `SELECT 1/0`
/// → `SELECT 42` scenario:
///
///  * `Succeeded(text)` — pg_transport ran the post-error
///    `SELECT` to completion. Pre-§6.3-fix this happened
///    sometimes (and the slot usually `SIGABRT`'d on the
///    `GetTransactionSnapshot` path immediately after); post-
///    fix it should never happen.
///  * `Rejected(sqlstate)` — pg_transport returned a wire ERROR
///    instead of running the SELECT. Post-§6.3-fix this is
///    `"25P02"`.
#[derive(Debug)]
enum PostAbortOutcome {
    Succeeded(String),
    Rejected(String),
}

/// Drive `BEGIN; SELECT 1/0; SELECT 42::text` as three separate
/// `'Q'` messages under `execution_backend = <backend>` and report
/// whether the post-error `SELECT` was allowed through.
async fn observe_post_abort_query(backend: &str) -> Result<PostAbortOutcome> {
    let c = Cluster::shared().await;
    let client = c.connect("postgres").await?;
    client
        .simple_query(&format!("SET pg_transport.execution_backend = '{backend}'"))
        .await?;

    client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN must succeed");
    let div_err = client
        .simple_query("SELECT 1/0")
        .await
        .expect_err("SELECT 1/0 must surface as a wire ERROR so the xact aborts");
    let db = div_err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError for SELECT 1/0, got: {div_err:?}"));
    assert!(
        db.message().contains("division by zero"),
        "expected the divide-by-zero error to surface so we know \
         it was THIS error that aborted the block, got: {}",
        db.message()
    );

    // The load-bearing probe: another statement against the
    // now-aborted block. Use ::text to keep `first_text_cell`
    // working regardless of backend default format.
    let probe = client.simple_query("SELECT 42::text").await;

    // No explicit `ROLLBACK` cleanup: that path would route
    // through `with_xact` → `PushActiveSnapshot(GetTransactionSnapshot())`
    // for the direct backend, which assert-fires from
    // `TBLOCK_ABORT` (the analyze-time snapshot gating that
    // would fix it is review §6 item 4, deferred — see the
    // §6.4 follow-up note in
    // `docs/design/reviews/2026-05-24-pg-code-findings.md`).
    // The slot's `reset_per_handoff_state` calls
    // `AbortOutOfAnyTransaction` after `client` drops, so the
    // slot returns to `TBLOCK_DEFAULT` for the next handoff
    // without us needing to issue a wire-level `ROLLBACK`.
    drop(client);

    match probe {
        Ok(msgs) => {
            let val = first_text_cell(&msgs)
                .unwrap_or_else(|| panic!("expected one text cell, got: {msgs:?}"));
            Ok(PostAbortOutcome::Succeeded(val))
        }
        Err(err) => {
            let db = err
                .as_db_error()
                .unwrap_or_else(|| panic!("expected DbError for post-abort SELECT, got: {err:?}"));
            Ok(PostAbortOutcome::Rejected(db.code().code().to_string()))
        }
    }
}

#[tokio::test]
async fn aborted_block_direct_backend_returns_25p02() -> Result<()> {
    // POST-FIX REGRESSION on review 2026-05-24 §6 item 3
    // (backend=direct). The pre-Push
    // `IsAbortedTransactionBlockState()` check in the per-
    // statement dispatch loop of `execute_simple_query_direct`
    // (and in `execute_with_implicit_block`'s per-iter loop)
    // rejects every non-exit statement in `TBLOCK_ABORT` with
    // SQLSTATE `25P02` — mirroring vanilla `exec_simple_query`
    // at postgres.c:1058-1063.
    //
    // If this test starts asserting `Succeeded(_)`, the check
    // has been bypassed or accidentally moved past
    // `with_xact`'s `PushActiveSnapshot` (which would re-
    // introduce the SIGABRT on `GetTransactionSnapshot` from
    // `TBLOCK_ABORT`).
    let outcome = observe_post_abort_query("direct").await?;
    match outcome {
        PostAbortOutcome::Rejected(code) => assert_eq!(
            code, "25P02",
            "REGRESSION on §6.3 (backend=direct): post-abort \
             SELECT was rejected with SQLSTATE {code:?} but vanilla \
             / our fix raises 25P02 (in_failed_sql_transaction). \
             The dispatch loop's IsAbortedTransactionBlockState() \
             check in `execute_simple_query_direct` / \
             `execute_with_implicit_block` may be returning a \
             different error type."
        ),
        PostAbortOutcome::Succeeded(val) => panic!(
            "REGRESSION on §6.3 (backend=direct): post-abort SELECT \
             ran to completion and returned {val:?}. The \
             IsAbortedTransactionBlockState() check is missing or \
             unreachable; the slot will SIGABRT on the next \
             non-trivial post-abort statement."
        ),
    }
    Ok(())
}

#[tokio::test]
async fn aborted_block_spi_backend_returns_25p02() -> Result<()> {
    // POST-FIX REGRESSION on review 2026-05-24 §6 item 3
    // (backend=spi). Same shape as the direct-backend test: the
    // `IsAbortedTransactionBlockState()` check at the top of
    // `execute_one_statement` (and in
    // `execute_with_implicit_block_spi`'s per-iter loop)
    // rejects every non-exit statement in `TBLOCK_ABORT` with
    // SQLSTATE `25P02` before `with_spi`'s
    // `PushActiveSnapshot(GetTransactionSnapshot())` could
    // assert.
    let outcome = observe_post_abort_query("spi").await?;
    match outcome {
        PostAbortOutcome::Rejected(code) => assert_eq!(
            code, "25P02",
            "REGRESSION on §6.3 (backend=spi): post-abort SELECT was \
             rejected with SQLSTATE {code:?} but vanilla / our fix \
             raises 25P02 (in_failed_sql_transaction). The dispatch \
             loop's IsAbortedTransactionBlockState() check in \
             `spi_bridge::execute_one_statement` / \
             `execute_with_implicit_block_spi` may be returning a \
             different error type."
        ),
        PostAbortOutcome::Succeeded(val) => panic!(
            "REGRESSION on §6.3 (backend=spi): post-abort SELECT ran \
             to completion and returned {val:?}. The \
             IsAbortedTransactionBlockState() check is missing or \
             unreachable; the slot will SIGABRT on the next non-\
             trivial post-abort statement."
        ),
    }
    Ok(())
}
