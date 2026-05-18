# Justfile — task runner for pg_transport
#
# Run `just` with no args (or `just list`) to see available recipes.
# All `cargo pgrx …` recipes target the Postgres builds under
# `$PGRX_HOME` (see below). No sudo, no system Postgres needed.

set shell := ["bash", "-cu"]
set dotenv-load := true

# Sub-files. `import` flat-merges into the same recipe namespace, so a
# recipe `foo` in `just/x.just` is invoked as `just foo`.
import 'just/doctor.just'

# Pin every cargo-pgrx invocation to a user-global Postgres install at
# `~/.pgrx` so multiple pgrx repos on the same machine share the same
# slow-to-build PG tree (~1.5 GB per major). Override by exporting
# PGRX_HOME before calling just (e.g. PGRX_HOME=$PWD/.pgrx for a
# repo-isolated install).
export PGRX_HOME := env_var_or_default("PGRX_HOME", env_var("HOME") / ".pgrx")

# The extension crate name; used by `cargo pgrx --package`. The Cargo
# package lives at `crates/core/` but is named `pg_transport` (the
# user-facing extension name) so the .so and .control files are
# named accordingly. See docs/design/workspace.md.
EXT := "pg_transport"

# Postgres major targeted by single-version recipes.
# v0 ships PG 18 only (see docs/design/roadmap.md §1).
default_pg := "pg18"

# ---------------------------------------------------------------------------
# Default recipe: list everything.
# ---------------------------------------------------------------------------

default:
    @just --list

# ---------------------------------------------------------------------------
# One-time environment setup
# ---------------------------------------------------------------------------

# Provision PG 18 in $PGRX_HOME (downloads source, builds with
# --enable-cassert). One-time, ~5–10 min, ~1.5 GB.
init:
    @echo "PGRX_HOME=$PGRX_HOME"
    cargo pgrx init --pg18 download

# Provision a single major. Usage: `just init-one pg=pg17`.
init-one pg=default_pg:
    cargo pgrx init --{{pg}} download

# Print the resolved pg_config for each provisioned major.
which-pg:
    @cat "$PGRX_HOME/config.toml" 2>/dev/null || \
        echo "no $PGRX_HOME/config.toml — run 'just init' first"

# ---------------------------------------------------------------------------
# Build / run / test
# ---------------------------------------------------------------------------

# Compile the extension against a Postgres major. Usage: `just build pg=pg17`.
build pg=default_pg:
    cargo pgrx package --package {{EXT}} --pg-config "$(just _pg-config {{pg}})"

# Start a psql session on a freshly-built extension. Usage: `just run pg=pg17`.
run pg=default_pg:
    cargo pgrx run {{pg}} --package {{EXT}}

# Run the extension test suite against one Postgres major.
test pg=default_pg:
    cargo pgrx test {{pg}} --package {{EXT}}

# pg_regress-style end-to-end tests (golden-file diffs against the
# psql output captured under `expected/`). Installs the extension
# into the in-repo PG, then drives the Postgres-bundled `pg_regress`
# binary with `--temp-instance --load-extension` so each run starts
# from a fresh cluster with `CREATE EXTENSION pg_transport`
# already loaded.
#
# Regenerate expected files when intentionally changing output:
#   1. just regress
#   2. cp crates/core/tests/pg_regress/results/*.out \
#         crates/core/tests/pg_regress/expected/
#   3. just regress  # confirms green
regress pg=default_pg:
    #!/usr/bin/env bash
    set -euo pipefail
    pgcfg="$(just _pg-config {{pg}})"
    if [[ -z "$pgcfg" || ! -x "$pgcfg" ]]; then
        echo "no pg_config for {{pg}} — run 'just init' first" >&2
        exit 1
    fi
    pg_root="$(dirname "$(dirname "$pgcfg")")"
    pg_regress="$pg_root/lib/postgresql/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "$pg_regress" ]]; then
        echo "pg_regress not found at $pg_regress" >&2
        exit 1
    fi
    # No --release: pg_regress runs golden-file diffs, not benchmarks,
    # so the dev profile is fine. Sharing the dev target dir with
    # `just test pg18` avoids a second full pgrx-pg-sys rebuild
    # (saves ~1 minute on `just check`).
    cargo pgrx install --package {{EXT}} --pg-config "$pgcfg"
    inputdir="crates/core/tests/pg_regress"
    pushd "$inputdir" >/dev/null
    rm -rf tmp_check results regression.diffs regression.out log
    tests=()
    for f in sql/*.sql; do
        tests+=("$(basename "$f" .sql)")
    done
    if [[ ${#tests[@]} -eq 0 ]]; then
        echo "no .sql tests in $inputdir/sql/" >&2
        popd >/dev/null
        exit 1
    fi
    "$pg_regress" \
        --bindir="$pg_root/bin" \
        --temp-instance=./tmp_check \
        --load-extension={{EXT}} \
        "${tests[@]}"
    popd >/dev/null

# Alias kept for forward-compat with a PG 17 + PG 18 matrix.
test-all: test

# Pure-Rust unit tests (no Postgres needed). Fast inner loop.
unit:
    cargo test --workspace --lib --bins

# Phase-0 gate from docs/design/roadmap.md §1: `cargo check` on the
# two crates that hold the public contract.
phase0-gate:
    cargo check -p api -p {{EXT}}

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings

# CI gate: format check + lint + unit tests + pg18 + pg_regress.
check: fmt-check lint unit
    just test pg18
    just regress pg18

# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------

# Produce an installable tree for one major. Usage: `just package pg=pg17`.
package pg=default_pg:
    cargo pgrx package --package {{EXT}} --pg-config "$(just _pg-config {{pg}})"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean:
    cargo clean

# Wipe the local Postgres install. Forces re-`init`. Slow to recover.
nuke-pgrx:
    @echo "Removing $PGRX_HOME"
    rm -rf "$PGRX_HOME"

# ---------------------------------------------------------------------------
# Internal helpers (prefixed `_` so they don't show in `just --list`).
# ---------------------------------------------------------------------------

# Resolve the pg_config path for a given major from $PGRX_HOME/config.toml.
_pg-config pg:
    @awk -F'=' '/^{{pg}}[[:space:]]*=/ {gsub(/[" ]/,"",$2); print $2}' \
        "$PGRX_HOME/config.toml"
