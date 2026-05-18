# just/pgbench.just — pgbench driver against pg_transport
#
# Imported by the root Justfile, so the recipe is invoked as
# `just pgbench`. Same namespace as the root file.
#
# Scope: install the extension into $PGRX_HOME, spin up a fresh
# temporary cluster with `shared_preload_libraries = 'pg_transport'`,
# initialise the pgbench tables via the cluster's VANILLA Postgres
# listener (54329) — pgbench `-i` uses extended-query protocol for
# its `regclass` lookup, which is phase-9 territory in our wire
# layer (see docs/design/roadmap.md §1 phase 9) — then run the
# actual benchmark against BOTH the pg_transport listener (5454)
# AND the vanilla listener, printing the two reports back-to-back
# for comparison.
#
# tpcb mode itself uses only simple-query messages (BEGIN / UPDATE
# / SELECT / INSERT / END), all of which the v0 wire + SPI bridge
# handle.

PGBENCH_PGDATA  := "/tmp/pg_transport_pgbench/pgdata"
PGBENCH_LOG     := "/tmp/pg_transport_pgbench/server.log"
PGBENCH_SOCKETS := "/tmp/pg_transport_sockets"

# Run pgbench against pg_transport vs vanilla PG, side by side.
#
# Positional parameters (just 1.51 has no kwarg form — see
# just/bench.just for the same restriction):
#   1. pg          (default pg18)  — Postgres major.
#   2. mode        (default select) — `select` runs `pgbench -S`
#                                    (read-only). `tpcb` (alias
#                                    `tpcb-rw`) runs the default
#                                    TPC-B-like workload (BEGIN +
#                                    3 UPDATEs + SELECT + INSERT +
#                                    END per transaction).
#   3. duration    (default 10)    — seconds; `pgbench -T`.
#   4. clients     (default 4)     — concurrent clients per side
#                                    (`pgbench -c -j`); bound by
#                                    `pg_transport.backend_pool_size`.
#   5. pool        (default "")    — override `backend_pool_size`.
#                                    Empty ⇒ defaults to `clients`.
#
# Examples:
#   just pgbench                              # 10s select, 4 clients
#   just pgbench pg18 select 30 8             # 30s select, 8 clients
#   just pgbench pg18 tpcb 30 8               # 30s tpcb-like, 8 clients
#   just pgbench pg18 select 10 16 16         # 16 clients & 16-slot pool
pgbench pg=default_pg mode="select" duration="10" clients="4" pool="":
    #!/usr/bin/env bash
    set -euo pipefail

    case "{{mode}}" in
        select|tpcb|tpcb-rw) ;;
        *)
            echo "pgbench: unknown mode '{{mode}}' (expected: select|tpcb|tpcb-rw)" >&2
            exit 2
            ;;
    esac

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "pgbench: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    pool_size="{{pool}}"
    if [[ -z "$pool_size" ]]; then
        pool_size="{{clients}}"
    fi

    # Step 1 — install the extension into PGRX_HOME.
    cargo pgrx install --release --package {{EXT}} --pg-config "$pgcfg" >/dev/null

    # Step 2 — wipe any previous pgbench cluster, recreate.
    pkill -9 -f pg_transport 2>/dev/null || true
    sleep 0.2
    rm -rf {{PGBENCH_PGDATA}} {{PGBENCH_LOG}} {{PGBENCH_SOCKETS}}
    mkdir -p "$(dirname {{PGBENCH_PGDATA}})"

    "$pg_bin/initdb" -D {{PGBENCH_PGDATA}} -U postgres \
        --auth-local=trust --auth-host=trust --no-instructions >/dev/null

    # Same max_worker_processes math as just/bench.just — see the
    # comment there for why `pool + 8` is right.
    mwp=$(( pool_size + 8 ))

    cat >>{{PGBENCH_PGDATA}}/postgresql.conf <<EOF
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_pgbench'
    listen_addresses = '127.0.0.1'
    logging_collector = off
    log_min_messages = warning
    pg_transport.backend_pool_size = $pool_size
    max_worker_processes = $mwp
    EOF

    "$pg_bin/pg_ctl" -D {{PGBENCH_PGDATA}} -l {{PGBENCH_LOG}} -w start >/dev/null
    sleep 1

    # Always tear down.
    cleanup() {
        local rc=$?
        "$pg_bin/pg_ctl" -D {{PGBENCH_PGDATA}} -m fast stop -w >/dev/null 2>&1 || true
        pkill -9 -f pg_transport 2>/dev/null || true
        if (( rc != 0 )); then
            echo "pgbench: FAILED — server log tail follows ----" >&2
            tail -20 {{PGBENCH_LOG}} >&2 || true
            echo "pgbench: ---- end server log" >&2
        fi
        rm -rf {{PGBENCH_PGDATA}} {{PGBENCH_LOG}} {{PGBENCH_SOCKETS}}
        rmdir "$(dirname {{PGBENCH_PGDATA}})" 2>/dev/null || true
        exit $rc
    }
    trap cleanup EXIT

    # Step 3 — pgbench -i via VANILLA port. pgbench's `-i` uses the
    # extended-query protocol for its `regclass` lookup
    # (`SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::regclass`),
    # which the v0 wire layer doesn't yet implement — phase 9.
    # The bench tables live in PG's heap; pg_transport reads/writes
    # them via SPI without needing its own -i pass.
    echo "pgbench: initialising via vanilla port (54329) ..."
    "$pg_bin/pgbench" -i -h 127.0.0.1 -p 54329 -U postgres -d postgres 2>&1 | tail -2

    common_args=(-T {{duration}} -c {{clients}} -j {{clients}} --no-vacuum -M simple)
    case "{{mode}}" in
        select) common_args+=(-S) ;;
        tpcb|tpcb-rw) ;;
    esac

    echo ""
    echo "pgbench: backend_pool_size=$pool_size clients={{clients}} duration={{duration}}s mode={{mode}}"

    # Step 4 — run vanilla first as the baseline.
    echo ""
    echo "=================================================="
    echo "  vanilla PG (127.0.0.1:54329)"
    echo "=================================================="
    "$pg_bin/pgbench" "${common_args[@]}" -h 127.0.0.1 -p 54329 -U postgres -d postgres

    # Step 5 — run pg_transport.
    echo ""
    echo "=================================================="
    echo "  pg_transport (127.0.0.1:5454)"
    echo "=================================================="
    "$pg_bin/pgbench" "${common_args[@]}" -h 127.0.0.1 -p 5454 -U postgres -d postgres
