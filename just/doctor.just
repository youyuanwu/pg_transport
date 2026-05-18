# just/doctor.just — environment diagnostics
#
# Imported by the root Justfile. Recipes here live in the same namespace
# as the root file, so this is invoked as `just doctor`.

# Verify every dependency needed to develop and test pg_transport.
# Exits non-zero if any required item is missing. Pure diagnosis: it
# never installs or modifies anything.
doctor:
    #!/usr/bin/env bash
    set -uo pipefail

    fail=0
    bold=$(tput bold 2>/dev/null || true)
    red=$(tput setaf 1 2>/dev/null || true)
    grn=$(tput setaf 2 2>/dev/null || true)
    ylw=$(tput setaf 3 2>/dev/null || true)
    rst=$(tput sgr0   2>/dev/null || true)

    ok()   { printf "  ${grn}\u2713${rst} %-22s %s\n" "$1" "$2"; }
    warn() { printf "  ${ylw}!${rst} %-22s %s\n"   "$1" "$2"; }
    bad()  { printf "  ${red}\u2717${rst} %-22s %s\n" "$1" "$2"; fail=1; }

    have() { command -v "$1" >/dev/null 2>&1; }
    ver()  { "$@" 2>&1 | head -n1; }

    section() { printf "\n${bold}%s${rst}\n" "$1"; }

    section "Rust toolchain"
    if have rustc;  then ok rustc  "$(ver rustc --version)";  else bad rustc  "not on PATH (install via rustup)"; fi
    if have cargo;  then ok cargo  "$(ver cargo --version)";  else bad cargo  "not on PATH"; fi
    if have rustup; then ok rustup "$(ver rustup --version | head -n1)"; else warn rustup "not found (toolchain pinning may not work)"; fi
    if have rustfmt; then ok rustfmt "$(ver rustfmt --version)"; else bad rustfmt "missing — install via 'rustup component add rustfmt'"; fi
    if have cargo-clippy; then ok clippy "$(ver cargo-clippy --version)"; else bad clippy "missing — install via 'rustup component add clippy'"; fi

    section "pgrx"
    if have cargo-pgrx; then ok cargo-pgrx "$(ver cargo-pgrx --version)"; \
        else bad cargo-pgrx "missing — run 'cargo install --locked cargo-pgrx'"; fi

    section "Task runner"
    if have just; then ok just "$(ver just --version)"; else bad just "missing"; fi

    section "C toolchain & PG build deps"
    for tool in gcc g++ make pkg-config bison flex perl clang; do
        if have "$tool"; then ok "$tool" "$(ver "$tool" --version 2>/dev/null | head -n1)"; \
            else bad "$tool" "missing (apt: build-essential bison flex pkg-config perl libclang-dev clang)"; fi
    done

    # libclang is loaded dynamically by bindgen at build time; presence
    # of `clang` above is a strong proxy but we still probe the .so.
    if ldconfig -p 2>/dev/null | grep -qE 'libclang(\.so|-[0-9]+\.so)'; then
        ok libclang.so "$(ldconfig -p | awk '/libclang(\.so|-[0-9]+\.so)/ {print $1; exit}')"
    else
        bad libclang.so "missing — install 'libclang-dev' (apt) or set LIBCLANG_PATH"
    fi

    section "System libraries (pkg-config)"
    if have pkg-config; then
        for lib in icu-uc icu-i18n openssl libssl zlib readline; do
            if pkg-config --exists "$lib" 2>/dev/null; then
                ok "$lib" "$(pkg-config --modversion "$lib")"
            else
                # libssl / openssl ship one or the other depending on distro
                warn "$lib" "not advertised by pkg-config (may still be present)"
            fi
        done
    else
        warn pkg-config "skipping library probe (pkg-config missing)"
    fi

    section "Network / archive utilities"
    for tool in curl tar; do
        if have "$tool"; then ok "$tool" "$(ver "$tool" --version | head -n1)"; \
            else bad "$tool" "missing (apt: $tool)"; fi
    done

    section "pgrx provisioning ($PGRX_HOME)"
    if [[ -f "$PGRX_HOME/config.toml" ]]; then
        ok "$PGRX_HOME/config.toml" "present"
        cfg=$(awk -F'=' '/^pg18[[:space:]]*=/ {gsub(/[" ]/,"",$2); print $2}' "$PGRX_HOME/config.toml")
        if [[ -n "$cfg" && -x "$cfg" ]]; then
            ok pg18 "$("$cfg" --version)"
        else
            bad pg18 "config.toml has no usable pg18 entry — run 'just init'"
        fi
    else
        bad PGRX_HOME "no config.toml — run 'just init'"
    fi

    echo
    if [[ $fail -eq 0 ]]; then
        printf "${grn}${bold}all required dependencies present${rst}\n"
    else
        printf "${red}${bold}missing one or more required dependencies${rst} (see ${red}\u2717${rst} above)\n"
        exit 1
    fi
