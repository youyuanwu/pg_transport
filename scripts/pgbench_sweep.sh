#!/usr/bin/env bash
#
# scripts/pgbench_sweep.sh
#
# Drive `just pgbench` across a matrix of mode x backend x clients
# (plus an optional connect={0,1} axis), capture each run's vanilla
# + pg_transport tps / latency / connection-time numbers, and write
# a TSV + a pre-rendered Markdown table fit for paste into
# docs/design/performance.md.
#
# Configuration (env vars, all optional):
#
#   MODES        space-separated pgbench modes
#                  default: "select nupdate tpcb"
#                  one of: select | nupdate | tpcb | tpcb-rw
#   BACKENDS     space-separated pg_transport backends
#                  default: "spi direct"
#   CLIENTS      space-separated pgbench client counts (-c -j)
#                  default: "8"
#                  steady-state default mirrors performance.md §2's
#                  "pgbench, 15 s, 8 clients" table; bump to e.g.
#                  "4 8 16" for a clients-sweep
#   DURATION     pgbench -T, seconds (default 15)
#   CONNECT      0 = steady-state (long-lived connections; default)
#                1 = connection-churn (-C, fresh TCP connection per tx)
#                  with CONNECT=1, the Markdown table includes the
#                  per-connection time columns and is shaped like
#                  the §2 "pgbench, connection-churn (-C)" table
#   SCRIPT       optional custom pgbench script path (overrides MODES;
#                  use bench/scripts/select_one.sql for the
#                  minimal-execution probe that pairs with CONNECT=1)
#   POOL         pg_transport.max_backend_pool_size override
#                  default empty (autoscaler ceiling stays at default)
#   PG           pg major suffix the harness expects (default pg18)
#   RUNS         number of repetitions per cell (default 1)
#                  results.tsv keeps every run; results.md reports the
#                  mean of each (vanilla, pg_transport) pair
#   OUTDIR       output directory (default
#                  /tmp/pg_transport_pgbench_sweep/<UTC-timestamp>)
#
# Outputs (in $OUTDIR):
#
#   results.tsv     one row per (mode,backend,clients,run): tps,
#                   latency_ms, conn_ms, failed_txns — for both
#                   vanilla and pg_transport
#   results.md      Markdown table grouped by mode — paste into
#                   performance.md as-is
#   run.log         full pgbench captures (per-cell)
#   progress.log    START / DONE markers with wall-clock times
#
# Examples:
#
#   # steady-state default: 3 modes × 2 backends × 8 clients, 1 run
#   # (~3.5 min)
#   ./scripts/pgbench_sweep.sh
#
#   # 3-run means at 15s each (~10 min), matches performance.md §2's
#   # methodology for the "pgbench, 15 s, 8 clients" table
#   RUNS=3 ./scripts/pgbench_sweep.sh
#
#   # connection-churn sweep (matches §2 "pgbench, connection-churn
#   # (-C), 20s, 3 runs each" table)
#   CONNECT=1 MODES=select BACKENDS=spi CLIENTS="4 16 32" \
#     DURATION=20 RUNS=3 ./scripts/pgbench_sweep.sh
#
#   # clients-sweep on the SPI backend, select mode only
#   MODES=select BACKENDS=spi CLIENTS="1 4 8 16 32" \
#     ./scripts/pgbench_sweep.sh
#
#   # custom-script probe paired with -C (minimal-execution probe)
#   CONNECT=1 SCRIPT=bench/scripts/select_one.sql CLIENTS=16 \
#     ./scripts/pgbench_sweep.sh
#
# Notes:
#
#   - `just pgbench` itself owns cluster setup/teardown per cell, so
#     interrupting this script mid-sweep is safe (no leftover state).
#   - Each cell takes ~initdb + pgbench -i + 2 * DURATION + teardown.
#     At default DURATION=15 each cell is ~35 s; the default 3 x 2 x 1
#     = 6 cell sweep takes ~3.5 minutes.
#   - tps is parsed from pgbench's `tps = NUMBER` line; latency from
#     `latency average = NUMBER ms`. Connection time is parsed from
#     `initial connection time` (CONNECT=0) or `average connection
#     time` (CONNECT=1) — pgbench prints exactly one of the two.
#   - `just pgbench` prints vanilla first, then pg_transport; parsing
#     pairs them in that fixed order.

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuration

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

MODES="${MODES:-select nupdate tpcb}"
BACKENDS="${BACKENDS:-spi direct}"
CLIENTS="${CLIENTS:-8}"
DURATION="${DURATION:-15}"
CONNECT="${CONNECT:-0}"
SCRIPT="${SCRIPT:-}"
POOL="${POOL:-}"
PG="${PG:-pg18}"
RUNS="${RUNS:-1}"

case "$CONNECT" in
    0|1) ;;
    *) echo "pgbench_sweep: CONNECT must be 0 or 1 (got '$CONNECT')" >&2; exit 2 ;;
esac

if (( RUNS < 1 )); then
    echo "pgbench_sweep: RUNS must be >= 1 (got '$RUNS')" >&2
    exit 2
fi

# When SCRIPT is set it overrides MODES at the `just pgbench` level
# (the `script` positional flips off `-S`/`-N`). For the matrix we
# treat it as a single synthetic mode = "script" so the output
# table reads sensibly.
if [[ -n "$SCRIPT" ]]; then
    if [[ ! -r "$repo_root/$SCRIPT" && ! -r "$SCRIPT" ]]; then
        echo "pgbench_sweep: SCRIPT '$SCRIPT' is not readable" >&2
        exit 2
    fi
    MODES="script"
fi

if [[ -z "${OUTDIR:-}" ]]; then
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    OUTDIR="/tmp/pg_transport_pgbench_sweep/${ts}"
fi
mkdir -p "$OUTDIR"

results_tsv="${OUTDIR}/results.tsv"
results_md="${OUTDIR}/results.md"
run_log="${OUTDIR}/run.log"
prog_log="${OUTDIR}/progress.log"

: > "$results_tsv"
: > "$run_log"
: > "$prog_log"

# Header. conn_label varies with CONNECT but the column name is
# stable in the TSV so downstream tooling has one schema.
printf 'mode\tbackend\tclients\trun\tvanilla_tps\tpgt_tps\tvanilla_lat_ms\tpgt_lat_ms\tvanilla_conn_ms\tpgt_conn_ms\tvanilla_failed\tpgt_failed\n' \
    > "$results_tsv"

# ---------------------------------------------------------------------------
# Banner

if [[ "$CONNECT" == "1" ]]; then
    conn_label="average connection time (per tx, -C)"
else
    conn_label="initial connection time"
fi

start_ts="$(date -u +%s)"
{
    echo "pgbench sweep — pg_transport vs vanilla PG"
    echo "  repo:      ${repo_root}"
    echo "  outdir:    ${OUTDIR}"
    echo "  pg:        ${PG}"
    echo "  modes:     ${MODES}"
    echo "  backends:  ${BACKENDS}"
    echo "  clients:   ${CLIENTS}"
    echo "  duration:  ${DURATION}s"
    echo "  connect:   ${CONNECT} (${conn_label})"
    echo "  script:    ${SCRIPT:-<none>}"
    echo "  pool:      ${POOL:-<default>}"
    echo "  runs/cell: ${RUNS}"
    echo "  started:   $(date -u --iso-8601=seconds)"
    echo
} | tee -a "$prog_log"

cd "$repo_root"

# ---------------------------------------------------------------------------
# Helpers

# Extract the N-th occurrence of a regex `<key> = <value>` from a
# pgbench-driven `just pgbench` capture. `just pgbench` prints
# vanilla first, then pg_transport, so N=1 is vanilla and N=2 is
# pg_transport.
#
# Args: text  key_regex  n  unit_suffix(optional)
# Echoes the captured numeric value (NaN-safe regex), or NA if not
# found.
extract_nth() {
    local text="$1" key_regex="$2" n="$3" unit_suffix="${4:-}"
    # The pgbench output lines look like:
    #   tps = 26703.123456 (without initial connection time)
    #   latency average = 0.302 ms
    #   initial connection time = 7.5 ms
    #   average connection time = 4.36 ms
    # We match `<key> = NUM` and grab NUM.
    local pattern="^${key_regex}[[:space:]]*=[[:space:]]*[0-9]+(\.[0-9]+)?"
    local val
    val="$(printf '%s\n' "$text" | grep -E "$pattern" | sed -n "${n}p" \
            | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)"
    if [[ -n "$val" ]]; then
        printf '%s\n' "$val"
    else
        printf 'NA\n'
    fi
}

# Extract the N-th "number of failed transactions" count.
extract_failed_nth() {
    local text="$1" n="$2"
    local val
    val="$(printf '%s\n' "$text" \
            | grep -E '^number of failed transactions:' \
            | sed -n "${n}p" | grep -oE '[0-9]+' | head -1)"
    if [[ -n "$val" ]]; then
        printf '%s\n' "$val"
    else
        printf 'NA\n'
    fi
}

# Format a numeric ratio "p/v" to 2 decimal places (matches the
# performance.md "Ratio" columns). Prints NA on non-numeric input.
ratio() {
    local v="$1" p="$2"
    if [[ "$v" =~ ^[0-9.]+$ && "$p" =~ ^[0-9.]+$ ]]; then
        awk -v p="$p" -v v="$v" 'BEGIN{ if (v == 0) print "NA"; else printf "%.2f", p/v }'
    else
        printf 'NA\n'
    fi
}

# Arithmetic mean of whitespace-separated numbers (NaN-safe — any NA
# input collapses the whole mean to NA). Prints to 2 decimal places.
mean() {
    awk 'BEGIN{n=0; sum=0}
         { for (i=1; i<=NF; i++) {
             if ($i ~ /^[0-9]+(\.[0-9]+)?$/) { sum += $i; n++ }
             else { print "NA"; exit }
         } }
         END{ if (n > 0) printf "%.2f", sum/n; else print "NA" }' <<<"$*"
}

# Render the sweep results as a Markdown table. One section per
# mode; rows = backend x clients. Aggregates RUNS samples per cell
# by arithmetic mean. Shape depends on CONNECT:
#   CONNECT=0 → mode column header reads "Backend | Clients | Vanilla
#               tps | pg_transport tps | tps ratio | Vanilla initial-conn |
#               pg_transport initial-conn | init-conn ratio"
#   CONNECT=1 → "Backend | Clients | Vanilla tps | pg_transport tps |
#               tps ratio | Vanilla avg conn time | pg_transport avg
#               conn time | conn-time ratio | Vanilla avg latency |
#               pg_transport avg latency"
emit_markdown() {
    local conn_header_v conn_header_p conn_ratio_header extra_lat
    if [[ "$CONNECT" == "1" ]]; then
        conn_header_v="Vanilla avg conn time"
        conn_header_p="pg_transport avg conn time"
        conn_ratio_header="conn-time ratio"
        extra_lat="| Vanilla avg latency | pg_transport avg latency "
    else
        conn_header_v="Vanilla initial-conn"
        conn_header_p="pg_transport initial-conn"
        conn_ratio_header="init-conn ratio"
        extra_lat=""
    fi

    awk -F'\t' \
        -v conn_hv="$conn_header_v" \
        -v conn_hp="$conn_header_p" \
        -v conn_hr="$conn_ratio_header" \
        -v extra_lat="$extra_lat" \
        -v connect="$CONNECT" \
        '
        function push(arr, key, val) { arr[key] = (key in arr) ? arr[key] " " val : val }
        function avg(s,    n, sum, i, fields) {
            n = split(s, fields, " ")
            if (n == 0) return "NA"
            sum = 0
            for (i = 1; i <= n; i++) {
                if (fields[i] !~ /^[0-9]+(\.[0-9]+)?$/) return "NA"
                sum += fields[i]
            }
            return sum / n
        }
        function ratio_str(v, p) {
            if (v == "NA" || p == "NA" || v == 0) return "NA"
            return sprintf("%.2fx", p / v)
        }
        function fmt(x, places) {
            if (x == "NA") return "NA"
            return sprintf("%." places "f", x)
        }
        NR == 1 { next }
        {
            mode = $1; be = $2; cl = $3
            # $4 = run (ignored — we aggregate)
            key = mode "\t" be "\t" cl
            push(vtps, key, $5); push(ptps, key, $6)
            push(vlat, key, $7); push(plat, key, $8)
            push(vcon, key, $9); push(pcon, key, $10)
            modes_seen[mode] = 1
            keys_seen[key] = 1
        }
        END {
            # Sort modes deterministically (input env order is lost
            # by awk; alphabetic is fine for the table).
            nm = 0
            for (m in modes_seen) mode_list[nm++] = m
            asort(mode_list)
            nb = split(ENVIRON["BACKENDS"], backends, " ")
            nc = split(ENVIRON["CLIENTS"], clients, " ")
            for (mi = 1; mi <= nm; mi++) {
                m = mode_list[mi]
                printf "#### %s\n\n", m
                # Header row
                printf "| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | %s | %s | %s ", conn_hv, conn_hp, conn_hr
                if (extra_lat != "") printf "%s", extra_lat
                printf "|\n"
                # Alignment row
                printf "|---|---:|---:|---:|---:|---:|---:|---:"
                if (extra_lat != "") printf "|---:|---:"
                printf "|\n"
                for (bi = 1; bi <= nb; bi++) {
                    for (ci = 1; ci <= nc; ci++) {
                        key = m "\t" backends[bi] "\t" clients[ci]
                        if (!(key in keys_seen)) continue
                        v_tps = avg(vtps[key]); p_tps = avg(ptps[key])
                        v_lat = avg(vlat[key]); p_lat = avg(plat[key])
                        v_con = avg(vcon[key]); p_con = avg(pcon[key])
                        printf "| %s | %s | %s | %s | %s | %s | %s | %s ",
                            backends[bi], clients[ci],
                            fmt(v_tps, "0"), fmt(p_tps, "0"),
                            ratio_str(v_tps, p_tps),
                            fmt(v_con, "2"), fmt(p_con, "2"),
                            ratio_str(v_con, p_con)
                        if (extra_lat != "") {
                            printf "| %s | %s ", fmt(v_lat, "2"), fmt(p_lat, "2")
                        }
                        printf "|\n"
                    }
                }
                printf "\n"
            }
        }
        ' "$results_tsv"
}

# ---------------------------------------------------------------------------
# Sweep

cell_idx=0
total_cells=0
for _ in $MODES; do for _ in $BACKENDS; do for _ in $CLIENTS; do
    for _ in $(seq 1 "$RUNS"); do
        total_cells=$((total_cells + 1))
    done
done; done; done

for mode in $MODES; do
    # Resolve the just-pgbench `mode` and `script` positional pair
    # from our (mode, SCRIPT) inputs.
    if [[ -n "$SCRIPT" ]]; then
        just_mode="select"   # ignored when script is set, but the
                             # positional must be a valid mode value
        just_script="$SCRIPT"
    else
        just_mode="$mode"
        just_script=""
    fi
    for be in $BACKENDS; do
        for cl in $CLIENTS; do
            for run in $(seq 1 "$RUNS"); do
                cell_idx=$((cell_idx + 1))
                label="[$cell_idx/$total_cells] mode=$mode be=$be clients=$cl run=$run"
                cell_start="$(date -u +%s)"
                echo "[$(date -u --iso-8601=seconds)] START $label" | tee -a "$prog_log"

                echo "=========================================================" >>"$run_log"
                echo "=== $label ===" >>"$run_log"
                echo "=========================================================" >>"$run_log"
                out="$(just pgbench "$PG" "$just_mode" "$DURATION" "$cl" "$POOL" "$be" "$CONNECT" "$just_script" 2>&1)"
                rc=$?
                printf '%s\n' "$out" >>"$run_log"

                v_tps="$(extract_nth "$out" 'tps' 1)"
                p_tps="$(extract_nth "$out" 'tps' 2)"
                v_lat="$(extract_nth "$out" 'latency average' 1)"
                p_lat="$(extract_nth "$out" 'latency average' 2)"
                # pgbench prints `initial connection time` (no -C) or
                # `average connection time` (-C). The two patterns are
                # mutually exclusive in a single run, so a single
                # alternation regex catches whichever one is present.
                v_con="$(extract_nth "$out" '(initial|average) connection time' 1)"
                p_con="$(extract_nth "$out" '(initial|average) connection time' 2)"
                v_failed="$(extract_failed_nth "$out" 1)"
                p_failed="$(extract_failed_nth "$out" 2)"

                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$mode" "$be" "$cl" "$run" \
                    "$v_tps" "$p_tps" \
                    "$v_lat" "$p_lat" \
                    "$v_con" "$p_con" \
                    "$v_failed" "$p_failed" \
                    >> "$results_tsv"

                cell_end="$(date -u +%s)"
                elapsed=$((cell_end - cell_start))
                (( cell_end > cell_start )) || elapsed=0
                status="ok"
                (( rc != 0 )) && status="rc=$rc"
                # Warn loudly if pgbench reported failed transactions —
                # per bench.md §2.3 this is a real failure, not a warning.
                if [[ "$v_failed" != "NA" && "$v_failed" != "0" ]]; then
                    status="$status vanilla_failed=$v_failed"
                fi
                if [[ "$p_failed" != "NA" && "$p_failed" != "0" ]]; then
                    status="$status pgt_failed=$p_failed"
                fi
                ratio_str="$(ratio "$v_tps" "$p_tps")"
                echo "[$(date -u --iso-8601=seconds)] DONE  $label  vanilla_tps=$v_tps pgt_tps=$p_tps ratio=$ratio_str ${elapsed}s $status" \
                    | tee -a "$prog_log"
            done
        done
    done
done

# ---------------------------------------------------------------------------
# Render markdown table

BACKENDS="$BACKENDS" CLIENTS="$CLIENTS" emit_markdown > "$results_md"

# ---------------------------------------------------------------------------
# Summary

end_ts="$(date -u +%s)"
total_elapsed=$((end_ts - start_ts))
{
    echo
    echo "[$(date -u --iso-8601=seconds)] SWEEP COMPLETE in ${total_elapsed}s"
    echo
    echo "results.tsv:"
    column -t -s $'\t' < "$results_tsv"
    echo
    echo "Outputs:"
    echo "  ${results_tsv}"
    echo "  ${results_md}"
    echo "  ${run_log}"
    echo "  ${prog_log}"
} | tee -a "$prog_log"
