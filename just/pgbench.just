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
#                                    (read-only). `nupdate` runs
#                                    `pgbench -N` (simple update,
#                                    skips branch/teller updates).
#                                    `tpcb` (alias `tpcb-rw`) runs
#                                    the default TPC-B-like workload
#                                    (BEGIN + 3 UPDATEs + SELECT +
#                                    INSERT + END per transaction).
#   3. duration    (default 10)    — seconds; `pgbench -T`.
#   4. clients     (default 4)     — concurrent clients per side
#                                    (`pgbench -c -j`); bound by
#                                    `pg_transport.max_backend_pool_size`
#                                    (default ceiling 64).
#   5. pool        (default "")    — override the autoscaling
#                                    *ceiling*
#                                    (`pg_transport.max_backend_pool_size`).
#                                    Empty ⇒ ceiling stays at the
#                                    default (64) and the autoscaler
#                                    grows on demand up to `clients`.
#                                    Set lower than `clients` to
#                                    exercise the saturation path.
#   6. backend     (default spi)   — pg_transport execution backend:
#                                    `spi` or `direct`.
#   7. connect     (default 0)     — `1` appends `-C` (reconnect per
#                                    transaction) to pgbench. The
#                                    "connection-churn" showcase: vanilla
#                                    pays fork+catalog warm-up per tx,
#                                    pg_transport's slot pool eats the
#                                    same handoff cost regardless. Auto-
#                                    bumps `max_connections` to
#                                    `max(100, clients * 4)` to avoid
#                                    backend-lifecycle wall under burst.
#                                    See bench.md §2.7.
#   8. script      (default "")    — custom pgbench script path (`-f`).
#                                    Overrides `mode` (no `-S`/`-N` set).
#                                    Use `bench/scripts/select_one.sql`
#                                    for the minimal-execution probe that
#                                    pairs with `connect=1`.
#
# Examples:
#   just pgbench                              # 10s select, 4 clients, default pool
#   just pgbench pg18 select 30 8             # 30s select, 8 clients
#   just pgbench pg18 nupdate 30 8            # 30s simple-update, 8 clients
#   just pgbench pg18 tpcb 30 8               # 30s tpcb-like, 8 clients
#   just pgbench pg18 select 10 16 1          # 16 clients, pool ceiling=1 (saturation)
#   just pgbench pg18 select 10 4 "" direct   # force direct backend on pg_transport
#   just pgbench pg18 select 30 32 "" spi 1   # connection-churn (-C) showcase
#   just pgbench pg18 select 30 32 "" spi 1 bench/scripts/select_one.sql
pgbench pg=default_pg mode="select" duration="10" clients="4" pool="" backend="spi" connect="0" script="":
    #!/usr/bin/env bash
    set -euo pipefail

    case "{{mode}}" in
        select|nupdate|tpcb|tpcb-rw) ;;
        *)
            echo "pgbench: unknown mode '{{mode}}' (expected: select|nupdate|tpcb|tpcb-rw)" >&2
            exit 2
            ;;
    esac

    case "{{backend}}" in
        spi|direct) ;;
        *)
            echo "pgbench: unknown backend '{{backend}}' (expected: spi|direct)" >&2
            exit 2
            ;;
    esac

    case "{{connect}}" in
        0|1) ;;
        *)
            echo "pgbench: connect must be 0 or 1 (got '{{connect}}')" >&2
            exit 2
            ;;
    esac

    if [[ -n "{{script}}" && ! -r "{{script}}" ]]; then
        echo "pgbench: script '{{script}}' does not exist or is not readable" >&2
        exit 2
    fi

    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "pgbench: no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_bin="$(dirname "$pgcfg")"

    # Step 1 — install the extension into PGRX_HOME.
    cargo pgrx install --release --package {{EXT}} --pg-config "$pgcfg" >/dev/null

    # Step 2 — wipe any previous pgbench cluster, recreate.
    pkill -9 -f pg_transport 2>/dev/null || true
    sleep 0.2
    rm -rf {{PGBENCH_PGDATA}} {{PGBENCH_LOG}} {{PGBENCH_SOCKETS}}
    mkdir -p "$(dirname {{PGBENCH_PGDATA}})"

    "$pg_bin/initdb" -D {{PGBENCH_PGDATA}} -U postgres \
        --auth-local=trust --auth-host=trust --no-instructions >/dev/null

    # Same `max_worker_processes` math as just/bench.just — the
    # autoscaler grows up to `connections` slots under load, and
    # we want PG's bgworker table to have headroom for that plus
    # the FE bgworker plus PG-internal bgworkers. The pool's
    # *ceiling* (`pg_transport.max_backend_pool_size`) is
    # decoupled from this — see the optional `pool` parameter
    # below.
    mwp=$(( {{clients}} + 8 ))

    # The `pool` parameter, when set, overrides the autoscaling
    # *ceiling*. Useful for bench scenarios that want to exercise
    # saturation (e.g. `pool=1, clients=8` to verify the
    # HANDOFF_WAIT-then-TCP-reset path; see pool.md §5.5). When
    # unset, the default ceiling (64) stands and the autoscaler
    # grows on demand up to `clients`.
    if [[ -n "{{pool}}" ]]; then
        pool_clause="pg_transport.max_backend_pool_size = {{pool}}"
    else
        pool_clause="# (pool ceiling left at default; autoscaler grows on demand)"
    fi

    # When `connect=1`, pgbench opens a fresh connection per transaction.
    # Vanilla PG defaults to max_connections=100 which chokes under burst
    # at clients ≥ 32 due to backend-lifecycle states (PG is not as forgiving
    # as it could be about reconnect-rate). Bump to a safe ceiling.
    if [[ "{{connect}}" == "1" ]]; then
        mc_target=$(( {{clients}} * 4 ))
        if (( mc_target < 100 )); then mc_target=100; fi
        mc_clause="max_connections = $mc_target"
    else
        mc_clause="# (max_connections left at PG default)"
    fi

    cat >>{{PGBENCH_PGDATA}}/postgresql.conf <<EOF
    shared_preload_libraries = 'pg_transport'
    port = 54329
    unix_socket_directories = '/tmp/pg_transport_pgbench'
    listen_addresses = '127.0.0.1'
    logging_collector = off
    log_min_messages = warning
    $pool_clause
    $mc_clause
    # auth_source is required by _PG_init() since phase 7.
    pg_transport.auth_source = 'pg_hba'
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
    if [[ -n "{{script}}" ]]; then
        # Custom script overrides built-in workloads; pgbench's `-f` is
        # mutually exclusive with `-S` / `-N` at the conventional usage
        # level, so skip those flags entirely.
        common_args+=(-f "{{script}}")
        workload_desc="script={{script}}"
    else
        case "{{mode}}" in
            select) common_args+=(-S) ;;
            nupdate) common_args+=(-N) ;;
            tpcb|tpcb-rw) ;;
        esac
        workload_desc="mode={{mode}}"
    fi
    if [[ "{{connect}}" == "1" ]]; then
        common_args+=(-C)
    fi

    echo ""
    echo "pgbench: clients={{clients}} duration={{duration}}s $workload_desc backend={{backend}} pool_ceiling={{pool}} connect={{connect}}"

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
    # Note: pgbench currently runs in `-M simple` mode for pg_transport,
    # so execution_backend primarily future-proofs this recipe until
    # extended/prepared query modes are supported end-to-end.
    PGOPTIONS="-c pg_transport.execution_backend={{backend}}" \
        "$pg_bin/pgbench" "${common_args[@]}" -h 127.0.0.1 -p 5454 -U postgres -d postgres
