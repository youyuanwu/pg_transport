#!/usr/bin/env bash
#
# scripts/sysbench_sweep.sh
#
# Drive `just sysbench` across a matrix of workload x backend x thread
# count, capture each run's vanilla + pg_transport TPS numbers, and
# write a TSV + a pre-rendered Markdown table fit for paste into
# docs/design/performance.md.
#
# Configuration (env vars, all optional):
#
#   WORKLOADS    space-separated sysbench workload names
#                  default: "oltp_point_select oltp_read_only oltp_update_index"
#                  (write-heavy workloads omitted by default because
#                   sysbench's delete_inserts pattern races on dup IDs
#                   at threads > 1 — see bench.md §3.2 footnote)
#   BACKENDS     space-separated pg_transport backends
#                  default: "spi direct"
#   THREADS      space-separated sysbench --threads values
#                  default: "4 16 32"
#   TABLES       sysbench --tables                  (default 16)
#   TABLE_SIZE   sysbench --table-size              (default 100000;
#                  cache-hot ~16 MB total, fits in default
#                  shared_buffers — isolates framework/executor
#                  overhead from disk I/O)
#   DURATION     sysbench --time, seconds           (default 20)
#   POOL         pg_transport.max_backend_pool_size (default empty;
#                  autoscaler grows on demand)
#   PG           pg major suffix the harness expects (default pg18)
#   OUTDIR       output directory                   (default
#                  /tmp/pg_transport_sweep/<UTC-timestamp>)
#
# Outputs (in $OUTDIR):
#
#   results.tsv     one row per cell: workload, backend, threads,
#                   vanilla_tps, pgt_tps, ratio
#   results.md      Markdown table grouped by workload — paste into
#                   performance.md as-is
#   run.log         full sysbench captures (per-cell)
#   progress.log    START / DONE markers with wall-clock times
#
# Examples:
#
#   # default sweep, ~15 minutes
#   ./scripts/sysbench_sweep.sh
#
#   # quick smoke at low concurrency only, 10 s per run
#   THREADS=4 DURATION=10 ./scripts/sysbench_sweep.sh
#
#   # one-axis sweep: scaling on SPI backend only
#   BACKENDS=spi THREADS="1 2 4 8 16 32 64" ./scripts/sysbench_sweep.sh
#
#   # disk-traffic shape (1.6 GB dataset, ~12x shared_buffers)
#   TABLE_SIZE=1000000 DURATION=60 ./scripts/sysbench_sweep.sh
#
# Notes:
#
#   - Each cell takes ~`initdb` + sysbench prepare + 2 * DURATION + teardown.
#     At default DURATION=20 each cell is ~50 s; the default 3 x 2 x 3 = 18
#     cell sweep takes ~15 minutes.
#   - `just sysbench` itself owns cluster setup/teardown per cell, so
#     interrupting this script mid-sweep is safe (no leftover state).
#   - TPS is parsed from sysbench's "transactions: N (X per sec.)" line;
#     since `just sysbench` prints exactly two of these per cell (vanilla
#     first, pg_transport second), parsing is unambiguous.

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuration

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

WORKLOADS="${WORKLOADS:-oltp_point_select oltp_read_only oltp_update_index}"
BACKENDS="${BACKENDS:-spi direct}"
THREADS="${THREADS:-4 16 32}"
TABLES="${TABLES:-16}"
TABLE_SIZE="${TABLE_SIZE:-100000}"
DURATION="${DURATION:-20}"
POOL="${POOL:-}"
PG="${PG:-pg18}"

if [[ -z "${OUTDIR:-}" ]]; then
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    OUTDIR="/tmp/pg_transport_sweep/${ts}"
fi
mkdir -p "$OUTDIR"

results_tsv="${OUTDIR}/results.tsv"
results_md="${OUTDIR}/results.md"
run_log="${OUTDIR}/run.log"
prog_log="${OUTDIR}/progress.log"

: > "$results_tsv"
: > "$run_log"
: > "$prog_log"

printf 'workload\tbackend\tthreads\tvanilla_tps\tpgt_tps\tratio\n' > "$results_tsv"

# ---------------------------------------------------------------------------
# Banner

start_ts="$(date -u +%s)"
{
    echo "sysbench sweep — pg_transport vs vanilla PG"
    echo "  repo:       ${repo_root}"
    echo "  outdir:     ${OUTDIR}"
    echo "  pg:         ${PG}"
    echo "  workloads:  ${WORKLOADS}"
    echo "  backends:   ${BACKENDS}"
    echo "  threads:    ${THREADS}"
    echo "  tables:     ${TABLES}"
    echo "  table_size: ${TABLE_SIZE}"
    echo "  duration:   ${DURATION}s"
    echo "  pool:       ${POOL:-<default>}"
    echo "  started:    $(date -u --iso-8601=seconds)"
    echo
} | tee -a "$prog_log"

cd "$repo_root"

# ---------------------------------------------------------------------------
# Helpers

# Parse the last two "(NUM per sec.)" values from a sysbench-driven
# `just sysbench` stdout/stderr capture. Returns "vanilla_tps pgt_tps"
# on one line; prints "NA NA" if either is missing.
parse_tps() {
    local text="$1"
    local arr
    # Expect exactly two "transactions: <count> (<rate> per sec.)" lines.
    # Use the *last* two so any incidental matches earlier in the log
    # (none today, but defensive) don't confuse us.
    mapfile -t arr < <(printf '%s\n' "$text" \
        | grep -oE 'transactions:[[:space:]]+[0-9]+[[:space:]]+\([0-9]+(\.[0-9]+)? per sec\.\)' \
        | grep -oE '\([0-9]+(\.[0-9]+)? per sec\.\)' \
        | grep -oE '[0-9]+(\.[0-9]+)?' \
        | tail -2)
    if (( ${#arr[@]} == 2 )); then
        printf '%s %s\n' "${arr[0]}" "${arr[1]}"
    else
        printf 'NA NA\n'
    fi
}

# Format a numeric ratio "p/v" to 3 decimal places. Prints "NA" if
# either operand is non-numeric.
ratio() {
    local v="$1" p="$2"
    if [[ "$v" =~ ^[0-9.]+$ && "$p" =~ ^[0-9.]+$ ]]; then
        awk -v p="$p" -v v="$v" 'BEGIN{printf "%.3f", p/v}'
    else
        printf 'NA\n'
    fi
}

# Render the sweep results as a Markdown table (one section per
# workload, rows = backend x threads). Reads $results_tsv.
emit_markdown() {
    awk -F'\t' '
        NR == 1 { next }  # skip header
        { rows[$1 "\t" $2 "\t" $3] = $4 "\t" $5 "\t" $6; workloads[$1] = 1 }
        END {
            n = 0
            for (w in workloads) wl_list[n++] = w
            # workloads array is hash-ordered; sort for deterministic output
            asort(wl_list)
            for (i = 1; i <= n; i++) {
                w = wl_list[i]
                print "#### " w
                print ""
                print "| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |"
                print "|---|---:|---:|---:|---:|"
                for (be_i = 0; be_i < length(backends); be_i++) {
                    be = backends[be_i + 1]
                    for (th_i = 0; th_i < length(threads_arr); th_i++) {
                        th = threads_arr[th_i + 1]
                        key = w "\t" be "\t" th
                        if (key in rows) {
                            split(rows[key], parts, "\t")
                            printf "| %s | %s | %s | %s | %sx |\n", be, th, parts[1], parts[2], parts[3]
                        }
                    }
                }
                print ""
            }
        }
        BEGIN {
            split(ENVIRON["BACKENDS"], backends, " ")
            split(ENVIRON["THREADS"], threads_arr, " ")
        }
    ' "$results_tsv"
}

# ---------------------------------------------------------------------------
# Sweep

cell_idx=0
total_cells=0
for _ in $WORKLOADS; do for _ in $BACKENDS; do for _ in $THREADS; do
    total_cells=$((total_cells + 1))
done; done; done

for wl in $WORKLOADS; do
    for be in $BACKENDS; do
        for th in $THREADS; do
            cell_idx=$((cell_idx + 1))
            label="[$cell_idx/$total_cells] $wl be=$be threads=$th"
            cell_start="$(date -u +%s)"
            echo "[$(date -u --iso-8601=seconds)] START $label" | tee -a "$prog_log"

            # Capture the `just sysbench` output for this cell. Each
            # invocation owns the full cluster lifecycle, so cells are
            # independent and the script is interrupt-safe.
            echo "=========================================================" >>"$run_log"
            echo "=== $label ===" >>"$run_log"
            echo "=========================================================" >>"$run_log"
            out="$(just sysbench "$PG" "$wl" "$TABLES" "$TABLE_SIZE" "$DURATION" "$th" "$POOL" "$be" off 2>&1)"
            rc=$?
            printf '%s\n' "$out" >>"$run_log"

            read -r v p < <(parse_tps "$out")
            r="$(ratio "$v" "$p")"
            printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$wl" "$be" "$th" "$v" "$p" "$r" >> "$results_tsv"

            cell_end="$(date -u +%s)"
            elapsed=$((cell_end - cell_start))
            status="ok"
            (( rc != 0 )) && status="rc=$rc"
            (( cell_end > cell_start )) || elapsed=0
            echo "[$(date -u --iso-8601=seconds)] DONE  $label  vanilla=$v pgt=$p ratio=$r ${elapsed}s $status" \
                | tee -a "$prog_log"
        done
    done
done

# ---------------------------------------------------------------------------
# Render markdown table

# Re-export BACKENDS / THREADS for the awk env (in case they came from
# the environment with extra whitespace).
BACKENDS="$BACKENDS" THREADS="$THREADS" emit_markdown > "$results_md"

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
