# just/e2e.just — end-to-end tokio-postgres tests
#
# Imported by the root Justfile (recipe invoked as `just e2e`).
#
# Scope: install the extension into $PGRX_HOME, then run the `e2e`
# crate's `#[tokio::test]` suite. Each test binary spins up its own
# temp cluster (via `e2e::Cluster::shared`), waits for the
# pg_transport listener (127.0.0.1:5454) to come up, and drives
# scenarios against it via tokio-postgres + psql.
#
# Covers the matrix the legacy `just smoke` (now deleted) used to
# guard, plus a growing surface of programmatic scenarios. See
# `docs/design/testing.md` §3.3.

# Where the e2e cluster's pgdata + sockets live. Mirrors the constants
# in crates/e2e/src/lib.rs (`Cluster::new`'s temp_dir() / ...). Keep
# in sync — the pre/post pkill+wipe is the safety net for Rust not
# running Drop for `static` values (see lib.rs::Cluster::Drop).
E2E_ROOT     := "/tmp/pg_transport_e2e"
E2E_SOCKETS  := "/tmp/pg_transport_e2e_sockets"

# Run the full e2e suite against one Postgres major. Idempotent —
# wipes any prior cluster first and after.
e2e pg=default_pg:
    #!/usr/bin/env bash
    set -euo pipefail

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "e2e: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi

    # Step 1 — install the extension. The e2e harness assumes the
    # .so + .control are present in $PGRX_HOME's share / lib trees
    # (it does NOT do this itself — separation of concerns: the
    # harness only manages cluster lifecycle).
    cargo pgrx install --package {{EXT}} --pg-config "$pgcfg" >/dev/null

    # Step 2 — belt-and-suspenders cleanup of any prior leak. The
    # harness's Cluster::new is idempotent and does this internally,
    # but Rust does NOT run Drop for `static OnceCell` values, so a
    # previous `cargo test` that was Ctrl-C'd leaves a live
    # postmaster. Kill it before we start.
    pkill -9 -f pg_transport_e2e 2>/dev/null || true
    rm -rf {{E2E_ROOT}} {{E2E_SOCKETS}} 2>/dev/null || true
    sleep 0.2

    # Always clean up on exit. Trap-based teardown survives
    # assertion failures and Ctrl-C.
    cleanup() {
        local rc=$?
        pkill -9 -f pg_transport_e2e 2>/dev/null || true
        if (( rc != 0 )) && [[ -f {{E2E_ROOT}}/server.log ]]; then
            echo "e2e: FAILED — server log tail follows ----"
            tail -60 {{E2E_ROOT}}/server.log || true
            echo "e2e: ---- end server log"
        fi
        rm -rf {{E2E_ROOT}} {{E2E_SOCKETS}} 2>/dev/null || true
        exit $rc
    }
    trap cleanup EXIT

    # Step 3 — run the tests. `--test-threads` left at the cargo
    # default (num_cpus) because tests within a binary share the
    # cluster via OnceCell and tokio-postgres is happily concurrent
    # (each test gets its own Client). The connection ceiling is
    # `pg_transport.backend_pool_size` (= 8 in lib.rs); cargo's
    # default parallelism is well under that on typical dev boxes.
    #
    # `--nocapture` so eprintln! diagnostics from the connection
    # task surface on test failure.
    cargo test -p e2e --tests -- --nocapture
