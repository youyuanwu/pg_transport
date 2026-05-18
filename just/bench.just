# just/bench.just — phase-5 bench harness driver
#
# Imported by the root Justfile, so the recipe is invoked as
# `just bench`. Same namespace as the root file.
#
# Scope: install the extension into $PGRX_HOME, spin up a fresh
# temporary cluster with `shared_preload_libraries = 'pg_transport'`,
# run the `bench compare` workload against the pg_transport
# tcp_handoff listener (127.0.0.1:5454) AND the cluster's vanilla
# Postgres listener (127.0.0.1:54329), tear the cluster down.
# Prints a side-by-side latency / qps table.

BENCH_PGDATA  := "/tmp/pg_transport_bench/pgdata"
BENCH_LOG     := "/tmp/pg_transport_bench/server.log"
BENCH_SOCKETS := "/tmp/pg_transport_sockets"

# Run `bench compare` between pg_transport and vanilla PG.
#
# `iters` defaults to 1000 measured iterations per side (after
# 100 warmup). For a longer run pass e.g. `just bench iters=10000`.
# `csv` writes a per-sample CSV if set: `just bench csv=/tmp/b.csv`.
#
# pg=pg18 selects the Postgres major (matches the rest of the
# Justfile).
bench pg=default_pg iters="1000" csv="":
    #!/usr/bin/env bash
    set -euo pipefail

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "bench: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    # Step 1 — install the extension into PGRX_HOME ($PGRX_HOME's
    # share / lib trees). Also build the bench binary in release —
    # we don't want debug-build overhead skewing the numbers.
    cargo pgrx install --release --package {{EXT}} --pg-config "$pgcfg" >/dev/null
    cargo build --release -p bench >/dev/null

    # Step 2 — wipe any previous bench cluster, recreate.
    pkill -9 -f pg_transport_bench 2>/dev/null || true
    sleep 0.2
    rm -rf {{BENCH_PGDATA}} {{BENCH_LOG}} {{BENCH_SOCKETS}}
    mkdir -p "$(dirname {{BENCH_PGDATA}})"

    "$pg_bin/initdb" -D {{BENCH_PGDATA}} -U postgres \
        --auth-local=trust --auth-host=trust --no-instructions >/dev/null

    cat >>{{BENCH_PGDATA}}/postgresql.conf <<EOF
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_bench'
    listen_addresses = '127.0.0.1'
    logging_collector = off
    log_min_messages = warning
    EOF

    "$pg_bin/pg_ctl" -D {{BENCH_PGDATA}} -l {{BENCH_LOG}} -w start >/dev/null
    # Give the FE bgworker + slot pool a beat to bind + connect.
    sleep 1

    # Always tear down, even on failure.
    cleanup() {
        local rc=$?
        "$pg_bin/pg_ctl" -D {{BENCH_PGDATA}} -m fast stop -w >/dev/null 2>&1 || true
        pkill -9 -f pg_transport_bench 2>/dev/null || true
        if (( rc != 0 )); then
            echo "bench: FAILED — server log tail follows ----" >&2
            tail -20 {{BENCH_LOG}} >&2 || true
            echo "bench: ---- end server log" >&2
        fi
        rm -rf {{BENCH_PGDATA}} {{BENCH_LOG}} {{BENCH_SOCKETS}}
        rmdir "$(dirname {{BENCH_PGDATA}})" 2>/dev/null || true
        exit $rc
    }
    trap cleanup EXIT

    # Step 3 — run the bench. `bench compare` connects to both
    # ports, runs the workload sequentially (no contention), prints
    # the comparison table to stderr.
    args=(compare --iterations {{iters}})
    if [[ -n "{{csv}}" ]]; then
        args+=(--csv "{{csv}}")
    fi
    target/release/bench "${args[@]}"
