#!/usr/bin/env bash
#
# scripts/bench_sweep.sh
#
# Drive `just bench` (the custom tokio-postgres SELECT-1 harness)
# across a matrix of connections x backend, capture each cell's
# p50 / p95 / qps for both vanilla and pg_transport, and write a
# TSV + a pre-rendered Markdown table fit for paste into
# docs/design/performance.md.
#
# Companion to scripts/pgbench_sweep.sh (driving `just pgbench`)
# and scripts/sysbench_sweep.sh (driving `just sysbench`). Same
# env-var / output shape; only the metrics differ — `just bench`
# is the only harness that reports latency percentiles directly,
# so the rendered table mirrors performance.md §2's "Custom bench"
# table (p50 / p95 / mean / qps / ratio).
#
# Configuration (env vars, all optional):
#
#   CONNECTIONS  space-separated worker-connection counts (per side)
#                  default: "1 4 8 16"
#                  matches the bench.md §1.5 recommended sweep
#   BACKENDS     space-separated pg_transport backends
#                  default: "spi direct"
#   ITERS        total measured iters per side; empty = autoscale
#                  per cell as `max(2000, 1000 * connections)`,
#                  which keeps per-worker sample count ≥ 1000 and
#                  matches the bench.md §1.5 sweep shape
#                  (4000@1 / 4000@4 / 8000@8 / 8000@16)
#                  (default: empty = autoscale)
#   POOL         pg_transport.max_backend_pool_size override
#                  default empty (autoscaler ceiling stays at default)
#   PG           pg major suffix the harness expects (default pg18)
#   RUNS         number of repetitions per cell (default 1)
#                  results.tsv keeps every run; results.md reports
#                  the arithmetic mean of each metric
#   OUTDIR       output directory (default
#                  /tmp/pg_transport_bench_sweep/<UTC-timestamp>)
#
# Outputs (in $OUTDIR):
#
#   results.tsv     one row per (connections,backend,run):
#                   vanilla_p50_us, pgt_p50_us,
#                   vanilla_p95_us, pgt_p95_us,
#                   vanilla_mean_us, pgt_mean_us,
#                   vanilla_qps,    pgt_qps
#   results.md      Markdown table grouped by backend — paste into
#                   performance.md as-is
#   run.log         full `just bench` captures (per-cell)
#   progress.log    START / DONE markers with wall-clock times
#
# Examples:
#
#   # default sweep: 4 connection counts x 2 backends, 1 run each
#   # (~3-4 minutes; autoscaled iters per cell)
#   ./scripts/bench_sweep.sh
#
#   # reproduce performance.md §2 "Custom bench, 20000 iters, 8 conns"
#   # (3-run means)
#   CONNECTIONS=8 ITERS=20000 RUNS=3 ./scripts/bench_sweep.sh
#
#   # scaling curve on the SPI backend only
#   BACKENDS=spi CONNECTIONS="1 2 4 8 16 32" ./scripts/bench_sweep.sh
#
#   # saturation: pool ceiling below the connection count
#   POOL=4 CONNECTIONS="4 8 16" ./scripts/bench_sweep.sh
#
# Notes:
#
#   - `just bench` itself owns cluster setup/teardown per cell, so
#     interrupting this script mid-sweep is safe (no leftover state).
#   - Each cell takes ~initdb + cargo install + warmup + measured
#     phase + teardown. At default autoscaled ITERS each cell is
#     ~25–30 s; the default 4 × 2 = 8 cell sweep takes ~3–4 minutes.
#   - p50 / p95 / mean / qps are parsed from the "stat | vanilla µs |
#     pg_transport µs | diff µs | pt/pg" comparison table printed
#     by `bench compare`; columns are space-separated and the
#     vanilla/pg_transport values are columns 2 and 3.

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuration

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

CONNECTIONS="${CONNECTIONS:-1 4 8 16}"
BACKENDS="${BACKENDS:-spi direct}"
ITERS="${ITERS:-}"
POOL="${POOL:-}"
PG="${PG:-pg18}"
RUNS="${RUNS:-1}"

if (( RUNS < 1 )); then
    echo "bench_sweep: RUNS must be >= 1 (got '$RUNS')" >&2
    exit 2
fi

# Validate ITERS up front if set explicitly (autoscale path handles
# the empty case).
if [[ -n "$ITERS" && ! "$ITERS" =~ ^[0-9]+$ ]]; then
    echo "bench_sweep: ITERS must be a positive integer (got '$ITERS')" >&2
    exit 2
fi

if [[ -z "${OUTDIR:-}" ]]; then
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    OUTDIR="/tmp/pg_transport_bench_sweep/${ts}"
fi
mkdir -p "$OUTDIR"

results_tsv="${OUTDIR}/results.tsv"
results_md="${OUTDIR}/results.md"
run_log="${OUTDIR}/run.log"
prog_log="${OUTDIR}/progress.log"

: > "$results_tsv"
: > "$run_log"
: > "$prog_log"

printf 'connections\tbackend\trun\titers\tvanilla_p50_us\tpgt_p50_us\tvanilla_p95_us\tpgt_p95_us\tvanilla_mean_us\tpgt_mean_us\tvanilla_qps\tpgt_qps\n' \
    > "$results_tsv"

# ---------------------------------------------------------------------------
# Banner

start_ts="$(date -u +%s)"
{
    echo "bench sweep — pg_transport vs vanilla PG (SELECT 1, extended-query)"
    echo "  repo:        ${repo_root}"
    echo "  outdir:      ${OUTDIR}"
    echo "  pg:          ${PG}"
    echo "  connections: ${CONNECTIONS}"
    echo "  backends:    ${BACKENDS}"
    echo "  iters:       ${ITERS:-<autoscaled per cell>}"
    echo "  pool:        ${POOL:-<default>}"
    echo "  runs/cell:   ${RUNS}"
    echo "  started:     $(date -u --iso-8601=seconds)"
    echo
} | tee -a "$prog_log"

cd "$repo_root"

# ---------------------------------------------------------------------------
# Helpers

# Extract vanilla and pg_transport values for a given metric label
# from `just bench`'s `bench compare` table. Each data row has the
# shape "<label> <vanilla> <pg_transport> <diff> <ratio>" with
# whitespace-separated columns. `qps` is a special case (one fewer
# column on the diff side; the regex still picks $2 / $3 correctly).
#
# Args: text  label_regex
# Echoes "<vanilla> <pg_transport>" on one line, or "NA NA" on miss.
extract_metric() {
    local text="$1" label="$2"
    local line
    line="$(printf '%s\n' "$text" | grep -E "^${label}[[:space:]]" | head -1)"
    if [[ -z "$line" ]]; then
        printf 'NA NA\n'
        return
    fi
    # awk handles arbitrary whitespace between columns.
    local v p
    read -r v p < <(printf '%s\n' "$line" | awk '{print $2, $3}')
    if [[ -z "$v" || -z "$p" ]]; then
        printf 'NA NA\n'
    else
        printf '%s %s\n' "$v" "$p"
    fi
}

# Format a numeric ratio "p/v" to 2 decimal places. Prints NA on
# non-numeric input.
ratio() {
    local v="$1" p="$2"
    if [[ "$v" =~ ^[0-9.]+$ && "$p" =~ ^[0-9.]+$ ]]; then
        awk -v p="$p" -v v="$v" 'BEGIN{ if (v == 0) print "NA"; else printf "%.2f", p/v }'
    else
        printf 'NA\n'
    fi
}

# Render results as a Markdown table. One section per backend; rows
# = connections. Aggregates RUNS samples per cell by arithmetic
# mean. Mirrors performance.md §2 "Custom bench" column layout:
# vanilla / pg_transport for p50, p95, mean, qps + qps ratio.
emit_markdown() {
    awk -F'\t' '
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
            cn = $1; be = $2
            # $3 = run, $4 = iters (ignored — we aggregate)
            key = be "\t" cn
            push(vp50, key, $5);   push(pp50, key, $6)
            push(vp95, key, $7);   push(pp95, key, $8)
            push(vmean, key, $9);  push(pmean, key, $10)
            push(vqps, key, $11);  push(pqps, key, $12)
            backends_seen[be] = 1
            keys_seen[key] = 1
        }
        END {
            # Use BACKENDS / CONNECTIONS env order for determinism;
            # asort would alphabetise and lose intent.
            nb = split(ENVIRON["BACKENDS"], backends, " ")
            nc = split(ENVIRON["CONNECTIONS"], conns, " ")
            for (bi = 1; bi <= nb; bi++) {
                be = backends[bi]
                if (!(be in backends_seen)) continue
                printf "#### %s\n\n", be
                printf "| Connections | Vanilla p50 µs | pg_transport p50 µs | Vanilla p95 µs | pg_transport p95 µs | Vanilla mean µs | pg_transport mean µs | Vanilla qps | pg_transport qps | qps ratio |\n"
                printf "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
                for (ci = 1; ci <= nc; ci++) {
                    cn = conns[ci]
                    key = be "\t" cn
                    if (!(key in keys_seen)) continue
                    v50  = avg(vp50[key]);  p50 = avg(pp50[key])
                    v95  = avg(vp95[key]);  p95 = avg(pp95[key])
                    vm   = avg(vmean[key]); pm  = avg(pmean[key])
                    vq   = avg(vqps[key]);  pq  = avg(pqps[key])
                    printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
                        cn,
                        fmt(v50, "1"), fmt(p50, "1"),
                        fmt(v95, "1"), fmt(p95, "1"),
                        fmt(vm,  "1"), fmt(pm,  "1"),
                        fmt(vq,  "0"), fmt(pq,  "0"),
                        ratio_str(vq, pq)
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
for _ in $CONNECTIONS; do for _ in $BACKENDS; do
    for _ in $(seq 1 "$RUNS"); do
        total_cells=$((total_cells + 1))
    done
done; done

for cn in $CONNECTIONS; do
    # Resolve per-cell iters: explicit ITERS overrides; otherwise
    # autoscale as max(2000, 1000 * connections) so per-worker
    # sample count is >= 1000 and percentiles are stable.
    if [[ -n "$ITERS" ]]; then
        cell_iters="$ITERS"
    else
        cell_iters=$(( cn * 1000 ))
        (( cell_iters < 2000 )) && cell_iters=2000
    fi
    for be in $BACKENDS; do
        for run in $(seq 1 "$RUNS"); do
            cell_idx=$((cell_idx + 1))
            label="[$cell_idx/$total_cells] connections=$cn be=$be iters=$cell_iters run=$run"
            cell_start="$(date -u +%s)"
            echo "[$(date -u --iso-8601=seconds)] START $label" | tee -a "$prog_log"

            echo "=========================================================" >>"$run_log"
            echo "=== $label ===" >>"$run_log"
            echo "=========================================================" >>"$run_log"
            # bench.just positional order: pg iters connections pool csv mode
            out="$(just bench "$PG" "$cell_iters" "$cn" "$POOL" "" "$be" 2>&1)"
            rc=$?
            printf '%s\n' "$out" >>"$run_log"

            read -r vp50 pp50  < <(extract_metric "$out" 'p50')
            read -r vp95 pp95  < <(extract_metric "$out" 'p95')
            read -r vmean pmean < <(extract_metric "$out" 'mean')
            read -r vqps pqps  < <(extract_metric "$out" 'qps')

            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                "$cn" "$be" "$run" "$cell_iters" \
                "$vp50" "$pp50" \
                "$vp95" "$pp95" \
                "$vmean" "$pmean" \
                "$vqps" "$pqps" \
                >> "$results_tsv"

            cell_end="$(date -u +%s)"
            elapsed=$((cell_end - cell_start))
            (( cell_end > cell_start )) || elapsed=0
            status="ok"
            (( rc != 0 )) && status="rc=$rc"
            qps_ratio="$(ratio "$vqps" "$pqps")"
            echo "[$(date -u --iso-8601=seconds)] DONE  $label  vanilla_p50=$vp50 pgt_p50=$pp50 vanilla_qps=$vqps pgt_qps=$pqps qps_ratio=$qps_ratio ${elapsed}s $status" \
                | tee -a "$prog_log"
        done
    done
done

# ---------------------------------------------------------------------------
# Render markdown table

BACKENDS="$BACKENDS" CONNECTIONS="$CONNECTIONS" emit_markdown > "$results_md"

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
