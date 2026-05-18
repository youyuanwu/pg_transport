# just/smoke.just — end-to-end smoke test
#
# Imported by the root Justfile, so the recipe is invoked as
# `just smoke`. Same namespace as the root file.
#
# Scope: install the extension into $PGRX_HOME, spin up a fresh
# temporary cluster with `shared_preload_libraries = 'pg_transport'`,
# drive the pg_transport TCP listener (127.0.0.1:5454) with `psql`
# through a small query matrix, assert each expected substring is
# present in the output, then tear the cluster down.
#
# Covers phases 1–4: bgworker spawn, slot pool / UDS handoff,
# tcp_handoff transport, pgwire-v3 wire + trust auth + SPI bridge.

# Port the FE bgworker hard-codes for the tcp_handoff transport in
# phase 3 (frontend.rs::PHASE_3_TCP_BIND). Keep in sync with that
# constant — phase ≥ 7 will move it into pg_transport.transports.
PT_PORT := "5454"

# Temp dirs we own and clean up between runs.
SMOKE_PGDATA   := "/tmp/pg_transport_smoke/pgdata"
SMOKE_LOG      := "/tmp/pg_transport_smoke/server.log"
SMOKE_SOCKETS  := "/tmp/pg_transport_sockets"

# End-to-end smoke. Exit 0 on success; non-zero on first failed
# assertion. Idempotent — wipes previous state first.
smoke pg=default_pg:
    #!/usr/bin/env bash
    set -euo pipefail

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "smoke: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    # Step 1 — install the extension into PGRX_HOME ($PGRX_HOME's
    # share / lib trees). Same path the cluster will look in.
    cargo pgrx install --package {{EXT}} --pg-config "$pgcfg" >/dev/null

    # Step 2 — wipe any previous cluster, recreate.
    pkill -9 -f pg_transport_smoke 2>/dev/null || true
    sleep 0.2
    rm -rf {{SMOKE_PGDATA}} {{SMOKE_LOG}} {{SMOKE_SOCKETS}}
    mkdir -p "$(dirname {{SMOKE_PGDATA}})"

    "$pg_bin/initdb" -D {{SMOKE_PGDATA}} -U postgres \
        --auth-local=trust --auth-host=trust --no-instructions >/dev/null

    cat >>{{SMOKE_PGDATA}}/postgresql.conf <<'EOF'
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_smoke'
    logging_collector = off
    log_min_messages = log
    # max_worker_processes default is 8 — covers our 1 FE + 2 slots
    # + PG-internal bgworkers comfortably. Bump explicitly here so
    # someone editing `backend_pool_size` higher in this file
    # doesn't silently lose registrations (see bench.just for the
    # phase-5 framing of this).
    max_worker_processes = 16
    EOF

    # Step 3 — start. The cluster pid file lives in $SMOKE_PGDATA so
    # `pg_ctl stop` later finds it.
    "$pg_bin/pg_ctl" -D {{SMOKE_PGDATA}} -l {{SMOKE_LOG}} -w start >/dev/null
    # Give the FE bgworker + slot pool a beat to bind + connect
    # (pg_ctl -w waits for the postmaster, not our bgworkers).
    sleep 1

    # Always tear down, even on assertion failure.
    cleanup() {
        local rc=$?
        "$pg_bin/pg_ctl" -D {{SMOKE_PGDATA}} -m fast stop -w >/dev/null 2>&1 || true
        pkill -9 -f pg_transport_smoke 2>/dev/null || true
        if (( rc != 0 )); then
            echo "smoke: FAILED — server log tail follows ----"
            tail -40 {{SMOKE_LOG}} || true
            echo "smoke: ---- end server log"
        fi
        rm -rf {{SMOKE_PGDATA}} {{SMOKE_LOG}} {{SMOKE_SOCKETS}}
        rmdir "$(dirname {{SMOKE_PGDATA}})" 2>/dev/null || true
        exit $rc
    }
    trap cleanup EXIT

    # Step 4 — run the assertion matrix. `assert_contains "label"
    # "expected substring" "actual output"` exits 1 with a clear
    # message on miss.
    PSQL="$pg_bin/psql -h 127.0.0.1 -p {{PT_PORT}} -U postgres -d postgres"
    export PGSSLMODE=disable

    assert_contains() {
        local label="$1" needle="$2" haystack="$3"
        if ! grep -F -q -- "$needle" <<<"$haystack"; then
            echo "smoke: ASSERTION FAILED [$label]" >&2
            echo "  expected substring: $needle" >&2
            echo "  actual output:" >&2
            sed 's/^/    /' <<<"$haystack" >&2
            return 1
        fi
        echo "smoke: ok — $label"
    }

    # 1) SELECT 1 returns 1 (phase-4b acceptance, roadmap.md §1).
    out=$(timeout 5 $PSQL -c 'SELECT 1' 2>&1)
    assert_contains "select-1 row" "1" "$out"
    assert_contains "select-1 count" "(1 row)" "$out"

    # 2) Multi-column literal (exercises the schema-walk path).
    out=$(timeout 5 $PSQL -c "SELECT 1+1 AS sum, 'hello' AS greeting" 2>&1)
    assert_contains "multi-col header" "sum" "$out"
    assert_contains "multi-col greeting" "hello" "$out"

    # 3) Catalog read with multi-row result (exercises the SPI bridge
    # for real PG types, not just integer literals).
    out=$(timeout 5 $PSQL -c \
        "SELECT relname FROM pg_class WHERE relname IN ('pg_class','pg_type','pg_proc') ORDER BY relname" 2>&1)
    assert_contains "pg_class row" "pg_class" "$out"
    assert_contains "pg_type row" "pg_type" "$out"
    assert_contains "pg_proc row" "pg_proc" "$out"
    assert_contains "catalog count" "(3 rows)" "$out"

    # 4) Error path — division by zero must surface as a wire
    # ErrorResponse with the PG message, NOT close the connection.
    # (Phase-4b panic_to_pgwire downcast lives in spi_bridge.rs.)
    # psql renders only the localized message by default; SQLSTATE
    # is only emitted under VERBOSITY=verbose. We assert on the
    # message — sufficient to prove the panic->wire path works.
    out=$(timeout 5 $PSQL -c 'SELECT 1/0' 2>&1 || true)
    assert_contains "div-by-zero is ERROR" "ERROR" "$out"
    assert_contains "div-by-zero message" "division by zero" "$out"
    # If panic_to_pgwire's downcast had fallen through to the generic
    # branch, the message would be "unknown panic payload" or
    # "pg_transport panic: …" — guard against the regression.
    if grep -F -q "unknown panic payload" <<<"$out"; then
        echo "smoke: ASSERTION FAILED [div-by-zero downcast]" >&2
        echo "  panic_to_pgwire fell through to the unknown branch" >&2
        exit 1
    fi
    echo "smoke: ok — div-by-zero downcast"

    # 5) Log assertions — make sure the bgworker chain actually fired
    # the right log lines (not e.g. a fork crash we didn't notice).
    log=$(cat {{SMOKE_LOG}})
    assert_contains "FE booted"         "frontend: tokio runtime ready" "$log"
    assert_contains "pool bound"        "all "                          "$log"
    assert_contains "tcp listener"      "tcp_handoff: listening on 127.0.0.1:{{PT_PORT}}" "$log"
    assert_contains "slot connected"    "wire=pgwire-v3"                "$log"
    assert_contains "startup handshake" "pgwire-v3 startup: peer="      "$log"

    echo "smoke: ALL GREEN"
