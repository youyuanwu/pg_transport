# Workspace, features, build

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [roadmap.md](roadmap.md)

## 1. Cargo workspace layout

Four crates under `crates/`; documentation under `docs/`. The tree
below reflects the actual on-disk layout as of phase 9.

```
pg_transport/
├── Cargo.toml                     # workspace manifest
├── Justfile                       # build / test / bench recipes
├── crates/
│   ├── api/                       # rlib; no PG deps; trait definitions only
│   │   └── src/lib.rs              # HandoffTransport, HandoffHandle, ShutdownToken
│   ├── core/                      # the pgrx extension (cdylib via pgrx)
│   │   ├── pg_transport.control
│   │   └── src/
│   │       ├── lib.rs              # _PG_init, pg_module_magic!
│   │       ├── frontend.rs         # frontend bgworker: tokio + LocalSet supervisor
│   │       ├── guc.rs              # GUC registration / validation
│   │       ├── backend/            # backend pool + slot bgworker
│   │       │   ├── mod.rs
│   │       │   ├── pool.rs         # FE-side slot table + sendmsg(SCM_RIGHTS)
│   │       │   ├── slot.rs         # BE-side per-slot loop (recvmsg + Wire::run)
│   │       │   ├── fd_pass.rs      # SCM_RIGHTS sendmsg/recvmsg helpers
│   │       │   ├── paths.rs        # well-known FE UDS path (frontend.sock)
│   │       │   ├── spi.rs          # safe Rust wrappers around SPI_* (see source)
│   │       │   ├── spi_bridge.rs   # simple-query handler (Q')
│   │       │   └── extended.rs     # extended-query handler (P/B/D/E/S)
│   │       ├── wire/               # FE/BE v3 wire layer
│   │       │   ├── mod.rs          # Wire trait
│   │       │   ├── pgwire_v3.rs    # impl atop the pgwire crate (sunng87)
│   │       │   ├── extended.rs     # ExtendedQueryHandler + QueryParser glue
│   │       │   ├── tls.rs          # rustls TlsAcceptor builder
│   │       │   └── auth/           # SCRAM, MD5, password, trust/reject
│   │       │       ├── mod.rs
│   │       │       ├── hba.rs      # hba_getauthmethod wrapper
│   │       │       ├── scram.rs
│   │       │       └── verifier.rs # pg_authid.rolpassword lookup
│   │       └── handoff/            # transport plugins
│   │           ├── mod.rs
│   │           └── tcp.rs          # the tcp_handoff HandoffTransport
│   ├── bench/                     # bin: latency + throughput harness
│   │   └── src/
│   └── e2e/                       # in-process integration tests against a real cluster
│       ├── src/                    # Cluster helper (initdb + pg_ctl start)
│       └── tests/                  # tokio-postgres + psql scenarios
├── docs/
│   ├── design/                    # this design dir
│   │   └── deferred/              # design captured for not-yet-built work
│   └── background/                # prior art (pg_background, Omnigres)
└── README.md
```

Four crates: `api`, `core`, `bench`, `e2e`. The split is the minimum
needed to keep distinct concerns honest:

- **`api/`** ([crates/api/src/lib.rs](../../crates/api/src/lib.rs)) has
  no `pgrx`, no PG headers, no proc-macros, and never will. A plugin
  author writes pure Rust against the trait definitions without
  dragging in the extension toolchain. Trait async methods return
  `Pin<Box<dyn Future + 'static>>` (see [api.md §1](api.md)); no
  `#[async_trait]`. Deps: `tokio`, `futures`, `tokio-util`, `anyhow`.
  The whole crate is one file — the surface really is that small.
- **`core/`** is the `cdylib` pgrx emits. Cargo package name is
  `pg_transport`, so the resulting `.so` and `.control` carry the
  user-facing extension name. Everything that statically links into
  the `.so` — pool, slot, wire, handoff transport, auth, TLS, GUCs —
  lives as modules under `src/`. The module tree (`backend/`, `wire/`,
  `handoff/`) preserves the conceptual partitioning earlier drafts
  expressed as separate crates, without per-crate `Cargo.toml` upkeep
  that gives nothing back while there's exactly one wire and one
  transport.
- **`bench/`** is a `bin` target, not a lib, so it stays separate.
  See [bench.md](bench.md) for what it measures and how.
- **`e2e/`** is an in-process integration suite. The shared `Cluster`
  helper does `initdb` + `pg_ctl start` with our extension preloaded,
  exposes a `tokio-postgres` client, and is reused across all tests
  via a process-wide `OnceCell`. See [testing.md §3.3](testing.md).

When a second wire impl or a second transport lands, the trigger to
*split* a module out into its own crate is concrete: an external
consumer (another extension, an out-of-tree plugin, a binary that
isn't `bench`) needs the contract without `pgrx`. Moving Rust modules
between crates is a no-op refactor — `mv` plus one `mod` line.
Doing it speculatively now buys nothing.

pgrx schema generation: only `core` runs `cargo pgrx`. The other
crates have no SQL surface.

## 2. Cargo features — deliberately minimal

`crates/core/Cargo.toml` has only the two features pgrx itself
requires (`pg18` for the PG-version-specific symbols, `pg_test` for
`cargo pgrx test`). There are no *transport* feature gates: the
single v0 transport (`tcp_handoff`) is unconditionally compiled in
as a `handoff/tcp.rs` module.

```toml
# crates/core/Cargo.toml
[features]
default = ["pg18"]
pg18    = ["pgrx/pg18", "pgrx-tests/pg18"]
pg_test = []

[dependencies]
pgrx    = { workspace = true }
api     = { path = "../api" }
tokio   = { workspace = true, features = ["rt", "net", "sync", "signal", "io-util", "macros", "time"] }
futures = { workspace = true }
# pgwire / openssl / tokio-openssl land in their respective phases.
```

Transport feature gates come back into the picture only when a second
transport lands (post-v0). At that point we'll decide — with a
concrete second transport in front of us — whether to gate it on a
feature, ship it always-on, or split into separate dist packages.
Picking a feature shape now would be guessing.

## 3. Build commands

```bash
# Build the extension
cargo build -p pg_transport

# Schema + install (pgrx machinery)
cargo pgrx install --release

# Phase-0 gate (docs/design/roadmap.md §1)
just phase0-gate     # = cargo check -p api -p pg_transport
```

There is no `--features` flag in v0 worth setting; pg18 + pg_test
default on, and there's nothing else to toggle.

## 4. Registry — how transports get wired in

v0 has **one** trait (`HandoffTransport`) and one transport
(`tcp_handoff`), so there is **no registry abstraction**: the frontend
bgworker imports the transport type directly and instantiates it once
at boot.

See [crates/core/src/frontend.rs](../../crates/core/src/frontend.rs)
(`use crate::handoff::tcp::{TcpHandoff, TcpHandoffCfg}` and the
`TcpHandoff::boxed` call site). A `kind` → factory map will land when
the second transport does — alongside (and not before) a SQL catalog
table that names it. We deliberately don't use the `inventory` crate
or ctor-based auto-registration: explicit imports keep the dependency
graph greppable.

When the deferred `SessionTransport` path lands
([deferred/backend-pool.md](deferred/backend-pool.md)) the dispatch
grows a second category (handoff vs. session); doing the partition
speculatively now buys nothing.

## See also

- [api.md](api.md) — `HandoffTransport`, `HandoffHandle` (and the
  deferred `SessionTransport`).
- [configuration.md](configuration.md) — current GUCs and the planned
  catalog surface.
- [roadmap.md](roadmap.md) — phased plan; v0 has no transport-feature gates.
