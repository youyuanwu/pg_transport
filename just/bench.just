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
# Positional parameters (just 1.51 does NOT support name=value kwargs
# at the call site — args bind strictly by position):
#   1. pg          (default pg18)  — Postgres major; matches the rest
#                                    of the Justfile.
#   2. iters       (default 1000)  — total measured iterations PER SIDE
#                                    (split across workers).
#   3. connections (default 1)     — concurrent worker connections per
#                                    side.
#   4. pool        (default "")    — pg_transport.backend_pool_size.
#                                    Empty string ⇒ defaults to
#                                    `connections` (apples-to-apples,
#                                    no contention). Set higher than
#                                    `connections` to test underutilised
#                                    pool, or lower to test contention
#                                    (each connection beyond pool_size
#                                    queues on slot availability).
#   5. csv         (default "")    — optional per-sample CSV path.
#   6. mode        (default spi)    — pg_transport execution backend:
#                                    `spi` (default) or `direct`.
#
# To skip a middle parameter while setting a later one, pass an
# explicit value or the default literally — there is no keyword form.
#
# Examples:
#   just bench                                # all defaults: 1 conn, 1 slot
#   just bench pg18 1000 4                    # 4 conns, 4 slots (no contention)
#   just bench pg18 1000 16 4                 # 16 conns, pool=4 (contention)
#   just bench pg18 10000 8 "" /tmp/b.csv     # 8 conns, default pool, CSV out
#   just bench pg18 10000 8 "" "" direct      # direct backend mode
bench pg=default_pg iters="1000" connections="1" pool="" csv="" mode="spi":
    #!/usr/bin/env bash
    set -euo pipefail

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "bench: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    # Pool size: default to == connections (apples-to-apples), else
    # whatever the caller asked for.
    pool_size="{{pool}}"
    if [[ -z "$pool_size" ]]; then
        pool_size="{{connections}}"
    fi

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

    # PG's `max_worker_processes` defaults to 8 — that ceiling
    # covers OUR FE bgworker + N slot bgworkers + PG's own
    # bgworker users (logical replication launcher, etc).
    # `BackgroundWorkerBuilder::load()` silently drops
    # registrations beyond this limit; symptom is "slot N did not
    # connect within 30s" from BackendPool::start. Give us pool +
    # 8 slack: covers FE + PG-internal bgworkers with margin.
    mwp=$(( pool_size + 8 ))

    cat >>{{BENCH_PGDATA}}/postgresql.conf <<EOF
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_bench'
    listen_addresses = '127.0.0.1'
    logging_collector = off
    log_min_messages = warning
    pg_transport.backend_pool_size = $pool_size
    # auth_source is required by _PG_init() since phase 7.
    pg_transport.auth_source = 'pg_hba'
    max_worker_processes = $mwp
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

    echo "bench: backend_pool_size=$pool_size connections={{connections}} iters={{iters}} mode={{mode}}"

    # Step 3 — run the bench. `bench compare` connects to both
    # ports, runs the workload sequentially per side (no contention
    # between vanilla and pg_transport), prints the comparison
    # table to stderr.
    args=(compare --iterations {{iters}} --connections {{connections}} --mode {{mode}})
    if [[ -n "{{csv}}" ]]; then
        args+=(--csv "{{csv}}")
    fi
    target/release/bench "${args[@]}"
