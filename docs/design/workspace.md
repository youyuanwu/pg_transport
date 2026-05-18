# Workspace, features, build

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [roadmap.md](roadmap.md)

## 1. Cargo workspace layout

```
pg_transport/
├── Cargo.toml                     # workspace
├── docs/
│   ├── design/                    # this design dir (active v0 design)
│   │   ├── README.md
│   │   ├── architecture.md
│   │   ├── api.md
│   │   ├── frontend-handoff.md
│   │   ├── backend-handoff.md
│   │   ├── backend-wire.md
│   │   ├── transports.md
│   │   ├── workspace.md           # ← this file
│   │   ├── configuration.md
│   │   ├── testing.md
│   │   ├── comparison.md
│   │   ├── roadmap.md
│   │   └── deferred/              # design captured for deferred work
│   │       ├── backend-pool.md         # shm_mq general path / SessionTransport
│   │       ├── cancel-routing.md       # wire-layer CancelRequest plumbing
│   │       └── future-transports.md    # QUIC, io_uring, AF_XDP, DPDK, RDMA, shmem
│   ├── background/
│   │   ├── pg_background.md
│   │   └── omnigres.md
│   └── adr/                       # architecture decision records
├── crates/
│   ├── api/                       # rlib; no PG deps; trait definitions only.
│   │   └── src/{transport.rs, handoff.rs, shutdown.rs, types.rs}
│   ├── core/                      # the pgrx extension (cdylib via pgrx).
│   │   │                          # Cargo package name = `pg_transport`; this
│   │   │                          # name becomes the `.so` and `.control`.
│   │   ├── pg_transport.control
│   │   └── src/
│   │       ├── lib.rs              # `_PG_init`, `pg_module_magic!`
│   │       ├── frontend.rs         # tokio current-thread + LocalSet
│   │       ├── registry.rs         # transport-name → factory map
│   │       ├── guc.rs              # GUC registration / validation
│   │       ├── catalog.rs          # SQL-surface tables and triggers
│   │       ├── metrics.rs          # cumulative counters
│   │       ├── backend/            # backend pool / slot bgworker
│   │       │   ├── mod.rs
│   │       │   ├── pool.rs         # frontend-side slot table + sendmsg
│   │       │   ├── slot.rs         # backend-side per-slot loop
│   │       │   ├── spi_bridge.rs   # wire → SPI execution
│   │       │   └── hba.rs          # `hba_getauthmethod` wrapper
│   │       ├── wire/               # FE/BE v3 wire implementation
│   │       │   ├── mod.rs          # `Wire` trait
│   │       │   └── pgwire_v3.rs    # impl using sunng87/pgwire
│   │       └── handoff/            # accept loop + tcp_handoff transport
│   │           ├── mod.rs
│   │           ├── listener.rs     # shared accept-loop helper
│   │           └── tcp.rs          # the `tcp_handoff` HandoffTransport
│   └── bench/                     # std binary; latency + throughput harness
└── README.md
```

Three crates: `api`, `core`, `bench`. The split is the minimum needed
to keep two distinct concerns honest:

- **`api/`** has no `pgrx`, no PG headers, no proc-macros, and never
  will. A plugin author writes pure Rust against the trait
  definitions without dragging in the extension toolchain. Trait
  async methods return `Pin<Box<dyn Future + 'static>>` (see [api.md
  §1](api.md)); no `#[async_trait]`. It depends on `tokio` (for fd
  types and async fundamentals) and `futures` (for the `Stream` trait
  used by the handoff listener helper).
- **`core/`** is the `cdylib` pgrx emits. Everything that statically
  links into the `.so` — pool, slot, wire, handoff listener, the
  `tcp_handoff` transport, GUCs, catalog, registry — lives here as
  modules under `src/`. The module tree (`backend/`, `wire/`,
  `handoff/`) preserves the conceptual partitioning earlier drafts
  expressed as separate crates, but without the per-crate
  `Cargo.toml` upkeep that gives nothing back while there's exactly
  one wire and one transport.
- **`bench/`** is a `bin` target, not a lib, so it stays separate.

When a second wire impl or a second transport lands, the trigger to
*split* a module out into its own crate is concrete: an external
consumer (another extension, an out-of-tree plugin, a binary that
isn't `bench`) needs the contract without `pgrx`. Moving Rust modules
between crates is a no-op refactor — `mv` plus one `mod` line. Doing
it speculatively now buys nothing.

`testutil` (helpers for integration tests) is intentionally absent in
v0. It comes back as either a `crates/testutil/` rlib or a
`crates/core/tests/common/` module on the first test that needs
shared helpers — see [testing.md](testing.md).

pgrx schema generation: only `core` runs `cargo pgrx`. `api` and
`bench` have no SQL surface.

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

A single explicit function in `core` registers built-in transports. v0
has **one** trait (`HandoffTransport`) and one transport (`tcp_handoff`),
so the registry is correspondingly small. A second map (`session:
HashMap<&'static str, SessionFactory>`) is reserved in the `Registry`
shape so the deferred `SessionTransport` (see
[backend-pool.md](deferred/backend-pool.md)) can land without a
registry-shape change; in v0 that map is always empty.

We deliberately do **not** use the `inventory` crate or ctor-based
auto-registration: explicit lines here keep the dependency graph greppable.

```rust
// crates/core/src/registry.rs
use api::HandoffFactory;
use std::collections::HashMap;

use crate::handoff;

pub struct Registry {
    pub handoff: HashMap<&'static str, HandoffFactory>,
    // Reserved for the deferred SessionTransport path; always empty in v0.
    // pub session: HashMap<&'static str, SessionFactory>,
}

pub fn register_builtin_transports(reg: &mut Registry) {
    // The single v0 transport. Compiled in unconditionally; no Cargo
    // feature gates this. A second handoff transport (uds-handoff, etc.)
    // is deferred — see docs/design/deferred/future-transports.md and
    // docs/design/deferred/backend-pool.md — and would add another
    // insert here (and another module under handoff/) when it lands.
    reg.handoff.insert("tcp_handoff", handoff::tcp::build);
}
```

At spawn time the frontend looks the row's `kind` up in the handoff
map and dispatches accordingly:

```rust
match reg.handoff.get(kind) {
    Some(build) => {
        let t = build(&row.cfg)?;
        let h = HandoffHandle::new(pool.clone());
        local.spawn_local(async move { t.run(h, shutdown).await });
    }
    None => bail!("transport {kind:?} not present (or deferred)"),
}
```

When the deferred `SessionTransport` path lands the `match` grows a
second arm that resolves through the `session` map. No new catalog
column is required — the registry knows which category each name
belongs to.

## 5. Catalog ↔ registry interaction

The `pg_transport.transports` catalog table references a transport by name
(e.g. `"tcp_handoff"`). The frontend resolves the name against the
**compile-time** registry. In v0 the only registered name is
`tcp_handoff`; any other name fails with a clear error:

```
ERROR:  transport "uds_handoff" referenced by transport id 7 is not present
DETAIL: this transport is currently deferred (see docs/design/deferred/future-transports.md).
```

This keeps "configure via SQL" while trading runtime plugin choice for
compile-time enablement. Once a second transport lands (post-v0) and
the Cargo-features question is reopened (see [roadmap.md §2
Q5](roadmap.md#2-open-questions)), the error message may grow a
build-flag hint.

## See also

- [api.md](api.md) — `HandoffTransport`, `HandoffFactory` (and the
  deferred `SessionTransport` / `SessionFactory`).
- [configuration.md](configuration.md) — the catalog schema this resolves against.
- [roadmap.md](roadmap.md) — phased plan; v0 has no transport-feature gates.
