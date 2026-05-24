# just/sysbench.just — sysbench OLTP driver vs pg_transport
#
# Phase A of Showcase 2 per bench.md §3.2. Same dual-listener
# pattern as `just pgbench`: spin up a fresh cluster, run sysbench
# against both the pg_transport port (5454) and the cluster's
# vanilla PG listener (54329), print results back-to-back.
#
# Complements `just pgbench` rather than replacing it:
#   - pgbench   → tpcb-shaped, simple-query (`'Q'`) protocol.
#   - sysbench  → diverse OLTP workloads (point-select, range-scan,
#                 mixed read-write, updates, deletes), uses libpq's
#                 extended-query / prepared-statement path by default,
#                 so it exercises crates/core/src/backend/extended/
#                 (different code path from pgbench).
#
# sysbench is OPT-IN. The recipe gates on `which sysbench` and prints
# an install hint if missing:
#   Debian/Ubuntu:  sudo apt install sysbench
#   macOS:          brew install sysbench
#
# sysbench 1.0.20 (current Debian/Ubuntu packaged version) does NOT
# support `--reconnect` for per-transaction churn. That regime is
# already covered by `just pgbench … connect=1` (bench.md §2.7); this
# recipe is steady-state-only by design.

SYSBENCH_PGDATA  := "/tmp/pg_transport_sysbench/pgdata"
SYSBENCH_LOG     := "/tmp/pg_transport_sysbench/server.log"
SYSBENCH_SOCKETS := "/tmp/pg_transport_sysbench"

# Run sysbench against pg_transport vs vanilla PG, side by side.
#
# Positional parameters (just 1.51 has no kwarg form — same restriction
# as just/bench.just and just/pgbench.just):
#   1. pg          (default pg18)  — Postgres major.
#   2. workload    (default oltp_read_write) — sysbench test name:
#                                    oltp_read_only | oltp_read_write
#                                    | oltp_point_select | oltp_insert
#                                    | oltp_update_index | oltp_update_non_index
#                                    | oltp_delete
#                                    See `sysbench oltp_read_write help`
#                                    for the per-transaction query mix
#                                    knobs (point_selects, range_size,
#                                    index_updates, etc.) — not exposed
#                                    here, sysbench defaults are used.
#   3. tables      (default 16)    — number of bench tables.
#   4. table_size  (default 100000) — rows per table. The default
#                                    (~16 MB total dataset) fits in
#                                    PG's default shared_buffers
#                                    (128 MB), giving a cache-hot
#                                    benchmark — useful for isolating
#                                    framework + executor overhead
#                                    from disk I/O. For a realistic
#                                    OLTP working set with real disk
#                                    traffic, bump to 1000000 (16 ×
#                                    1M = ~1.6 GB, ~12× default
#                                    shared_buffers). Prepare time
#                                    scales linearly: ~5 s at 100k,
#                                    ~60 s at 1M.
#   5. duration    (default 30)    — seconds (sysbench `--time`).
#   6. threads     (default 16)    — concurrent worker threads per
#                                    side (sysbench `--threads`);
#                                    bound by
#                                    `pg_transport.max_backend_pool_size`
#                                    on the pg_transport side.
#   7. pool        (default "")    — override
#                                    `pg_transport.max_backend_pool_size`.
#                                    Empty ⇒ ceiling stays at the
#                                    default (64) and the autoscaler
#                                    grows on demand up to `threads`.
#   8. backend     (default spi)   — pg_transport execution backend:
#                                    `spi` (extended/spi.rs) or
#                                    `direct` (extended/direct.rs).
#                                    Passed via PGOPTIONS to the
#                                    pg_transport-side run only.
#   9. skip_trx    (default off)   — `on` appends `--skip_trx=on`,
#                                    which makes sysbench skip
#                                    explicit BEGIN/COMMIT wrapping
#                                    and run every query in libpq
#                                    AUTOCOMMIT mode. No longer
#                                    required on the SPI backend
#                                    (the extended-query Parse
#                                    path now sniffs xact-control
#                                    via spi_bridge::classify_single_statement
#                                    + handle_xact_control_one). Still
#                                    required for the `direct`
#                                    backend (xact-control UAF
#                                    pinned by
#                                    `xact_control_begin_via_extended_query_direct_backend_pins_bug`).
#                                    Also a real measurement axis:
#                                    with vs without explicit-
#                                    transaction overhead.
#                                    `oltp_point_select` doesn't need
#                                    this (no transactions to skip).
#
# Examples:
#   just sysbench                                                       # 30s read-write — works on SPI backend with default skip_trx=off
#   just sysbench pg18 oltp_point_select 16 100000  30 16              # works as-is, no skip_trx needed
#   just sysbench pg18 oltp_read_only    16 100000  30 16 "" spi off   # SPI backend handles native BEGIN/COMMIT
#   just sysbench pg18 oltp_read_only    16 100000  30 16 "" direct on # direct backend still needs skip_trx=on
#   just sysbench pg18 oltp_read_write   16 1000000 60 32 "" spi off   # larger-than-cache, native xact-control
sysbench pg=default_pg workload="oltp_read_write" tables="16" table_size="100000" duration="30" threads="16" pool="" backend="spi" skip_trx="off":
    #!/usr/bin/env bash
    set -euo pipefail

    if ! command -v sysbench >/dev/null 2>&1; then
        echo "sysbench: not found in PATH" >&2
        echo "" >&2
        echo "  Debian/Ubuntu:  sudo apt install sysbench" >&2
        echo "  macOS:          brew install sysbench" >&2
        echo "" >&2
        echo "sysbench is opt-in for the bench harness; see bench.md §4." >&2
        exit 2
    fi

    case "{{workload}}" in
        oltp_read_only|oltp_read_write|oltp_point_select|oltp_insert|oltp_update_index|oltp_update_non_index|oltp_delete) ;;
        *)
            echo "sysbench: unknown workload '{{workload}}'" >&2
            echo "expected: oltp_read_only | oltp_read_write | oltp_point_select | oltp_insert | oltp_update_index | oltp_update_non_index | oltp_delete" >&2
            exit 2
            ;;
    esac

    case "{{backend}}" in
        spi|direct) ;;
        *)
            echo "sysbench: unknown backend '{{backend}}' (expected: spi|direct)" >&2
            exit 2
            ;;
    esac

    case "{{skip_trx}}" in
        on|off) ;;
        *)
            echo "sysbench: skip_trx must be 'on' or 'off' (got '{{skip_trx}}')" >&2
            exit 2
            ;;
    esac

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "sysbench: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    # Step 1 — install the extension into PGRX_HOME.
    cargo pgrx install --release --package {{EXT}} --pg-config "$pgcfg" >/dev/null

    # Step 2 — wipe any previous sysbench cluster, recreate.
    pkill -9 -f pg_transport 2>/dev/null || true
    sleep 0.2
    rm -rf {{SYSBENCH_PGDATA}} {{SYSBENCH_LOG}} {{SYSBENCH_SOCKETS}}
    mkdir -p "$(dirname {{SYSBENCH_PGDATA}})"

    "$pg_bin/initdb" -D {{SYSBENCH_PGDATA}} -U postgres \
        --auth-local=trust --auth-host=trust --no-instructions >/dev/null

    # Same `max_worker_processes` math as just/pgbench.just — the
    # autoscaler grows up to `threads` slots under load, and we want
    # PG's bgworker table to have headroom for that plus the FE
    # bgworker plus PG-internal bgworkers.
    mwp=$(( {{threads}} + 8 ))

    if [[ -n "{{pool}}" ]]; then
        pool_clause="pg_transport.max_backend_pool_size = {{pool}}"
    else
        pool_clause="# (pool ceiling left at default; autoscaler grows on demand)"
    fi

    cat >>{{SYSBENCH_PGDATA}}/postgresql.conf <<EOF
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_sysbench'
    listen_addresses = '127.0.0.1'
    logging_collector = off
    log_min_messages = warning
    $pool_clause
    # auth_source is required by _PG_init() since phase 7.
    pg_transport.auth_source = 'pg_hba'
    max_worker_processes = $mwp
    EOF

    "$pg_bin/pg_ctl" -D {{SYSBENCH_PGDATA}} -l {{SYSBENCH_LOG}} -w start >/dev/null
    sleep 1

    # Always tear down.
    cleanup() {
        local rc=$?
        "$pg_bin/pg_ctl" -D {{SYSBENCH_PGDATA}} -m fast stop -w >/dev/null 2>&1 || true
        pkill -9 -f pg_transport 2>/dev/null || true
        if (( rc != 0 )); then
            echo "sysbench: FAILED — server log tail follows ----" >&2
            tail -20 {{SYSBENCH_LOG}} >&2 || true
            echo "sysbench: ---- end server log" >&2
        fi
        rm -rf {{SYSBENCH_PGDATA}} {{SYSBENCH_LOG}} {{SYSBENCH_SOCKETS}}
        rmdir "$(dirname {{SYSBENCH_PGDATA}})" 2>/dev/null || true
        exit $rc
    }
    trap cleanup EXIT

    # Step 3 — sysbench `prepare` (DDL + bulk insert). Runs against
    # the vanilla port, in the `postgres` database (sysbench's
    # `--pgsql-db` defaults to `sbtest`; we override to `postgres`
    # because v0 slots hard-code the SPI database to `postgres` —
    # per-database routing arrives in a later phase, see bench.md §2.5).
    # pg_transport reads/writes the same data files via SPI/direct.
    echo "sysbench: preparing {{tables}} tables × {{table_size}} rows in 'postgres' DB ..."
    common_opts=(
        --db-driver=pgsql
        --pgsql-host=127.0.0.1
        --pgsql-user=postgres
        --pgsql-db=postgres
        --tables={{tables}}
        --table-size={{table_size}}
    )
    sysbench "${common_opts[@]}" --pgsql-port=54329 {{workload}} prepare 2>&1 | tail -3

    # Step 4 — run options shared between sides.
    run_opts=(
        --time={{duration}}
        --threads={{threads}}
    )
    if [[ "{{skip_trx}}" == "on" ]]; then
        run_opts+=(--skip_trx=on)
    fi

    echo ""
    echo "sysbench: workload={{workload}} threads={{threads}} duration={{duration}}s tables={{tables}} table_size={{table_size}} backend={{backend}} skip_trx={{skip_trx}} pool_ceiling={{pool}}"

    # Step 5 — vanilla baseline first.
    echo ""
    echo "=================================================="
    echo "  vanilla PG (127.0.0.1:54329)"
    echo "=================================================="
    sysbench "${common_opts[@]}" "${run_opts[@]}" --pgsql-port=54329 {{workload}} run

    # Step 6 — pg_transport. PGOPTIONS sets the backend GUC at startup
    # so each thread's session honours the choice.
    echo ""
    echo "=================================================="
    echo "  pg_transport (127.0.0.1:5454)"
    echo "=================================================="
    PGOPTIONS="-c pg_transport.execution_backend={{backend}}" \
        sysbench "${common_opts[@]}" "${run_opts[@]}" --pgsql-port=5454 {{workload}} run
