# Workspace, features, build

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [roadmap.md](roadmap.md)

## 1. Cargo workspace layout

```
pg_transport/
├── Cargo.toml                     # workspace
├── pg_transport.control           # the actual PG extension (rendered by pgrx)
├── docs/
│   ├── design/                    # this design dir (active v0 design)
│   │   ├── README.md
│   │   ├── architecture.md
│   │   ├── api.md
│   │   ├── frontend-handoff.md
│   │   ├── backend-handoff.md
│   │   ├── transports.md
│   │   ├── workspace.md           # ← this file
│   │   ├── configuration.md
│   │   ├── comparison.md
│   │   ├── roadmap.md
│   │   └── deferred/              # design captured for deferred work
│   │       ├── backend-pool.md         # shm_mq general path / SessionTransport
│   │       └── future-transports.md    # QUIC, io_uring, AF_XDP, DPDK, RDMA, shmem
│   ├── background/
│   │   ├── pg_background.md
│   │   └── omnigres.md
│   └── adr/                       # architecture decision records
├── crates/
│   ├── api/                       # rlib; no PG deps; trait definitions only
│   │   └── src/{transport.rs, handoff.rs, shutdown.rs, types.rs}
│   ├── core/                      # the pgrx extension (cdylib via pgrx)
│   │   └── src/{lib.rs, frontend.rs, registry.rs, guc.rs, catalog.rs, metrics.rs}
│   ├── backend/                  # rlib; linked into core; backend pool bgworker
│   │   └── src/{pool.rs, slot.rs, wire.rs, spi_bridge.rs, hba.rs}
│   │                              #   slot.rs = socket layer (backend-handoff.md);
│   │                              #   wire.rs = Wire trait (backend-wire.md);
│   │                              #   spi_bridge.rs = wire → SPI;
│   │                              #   hba.rs = hba_getauthmethod wrapper.
│   ├── wire-pgwire-v3/           # rlib; v0 Wire impl using sunng87/pgwire.
│   │                              #   FE/BE v3 parse/encode, SCRAM exchange,
│   │                              #   driver loop, error frames. See
│   │                              #   docs/design/backend-wire.md §2.
│   ├── handoff-listener/          # rlib; shared accept-loop helper used by
│   │                              #   every HandoffTransport. Stream-shaped
│   │                              #   API: takes Stream<Item=Result<OwnedFd>>,
│   │                              #   drives it until shutdown. See
│   │                              #   docs/design/transports.md §2.1.
│   ├── transport-tcp-handoff/      # rlib; the single v0 transport.
│   │                              #   Complete transport: TCP socket +
│   │                              #   accept loop + SCM_RIGHTS handoff.
│   │                              #   Compiled into `core` unconditionally;
│   │                              #   there is no Cargo feature gate.
│   │                              #   Deferred transports (uds-handoff,
│   │                              #   iouring-pgwire, quic-quinn-pgwire,
│   │                              #   afxdp-*, dpdk-*, rdma-*,
│   │                              #   shmem-loopback-*, http2-sql) live
│   │                              #   in their own crates that will be
│   │                              #   added when their phase begins. See
│   │                              #   docs/design/deferred/future-transports.md
│   │                              #   and docs/design/deferred/backend-pool.md.
│   ├── bench/                     # std binary; drives clients of each protocol
│   └── testutil/                  # cluster spin-up helpers for integration tests
└── README.md
```

Notes:

- The PG extension is `crates/core` alone — plugin crates are `rlib`s pulled
  in as Cargo dependencies and statically linked. There is exactly **one**
  `.so` shipped (`pg_transport.so`), produced by pgrx.
- `api/` depends on `tokio` (for fd types and async fundamentals) and a
  small `futures` dep (for the `Stream` trait used by `handoff-listener`),
  but **not** on `pgrx`, any PG header, or any proc-macro. Trait async
  methods return `Pin<Box<dyn Future + 'static>>` (see [api.md §1](api.md));
  no `#[async_trait]`. A plugin author writes pure Rust against the trait
  definitions without dragging in the extension toolchain.
- pgrx schema generation: only `core` runs `cargo pgrx`. Plugin crates have
  no SQL surface.
- Deferred helper crates (`protocol-http2`, `tls-rustls`) are not in
  the v0 tree — they only appear when the deferred `SessionTransport`
  path lands. See [transports.md §2](transports.md). The pgwire-v3
  wire is v0 (lives in `crates/wire-pgwire-v3/`, depends on the
  [`pgwire`](https://github.com/sunng87/pgwire) crate).

## 2. Cargo features — deliberately none in v0

`crates/core/Cargo.toml` has **no `[features]` table** in v0. The single
v0 transport (`tcp_handoff`) is a direct dependency of `core` and is
compiled in unconditionally:

```toml
# crates/core/Cargo.toml
[dependencies]
api               = { path = "../api" }
backend           = { path = "../backend" }
wire-pgwire-v3    = { path = "../wire-pgwire-v3" }
handoff-listener  = { path = "../handoff-listener" }
transport-tcp-handoff = { path = "../transport-tcp-handoff" }
# (no [features] section)
```

Features come back into the picture when a second transport lands
(post-v0). At that point we'll decide — with a concrete second
transport in front of us — whether to gate it on a feature, ship it
always-on, or split into separate dist packages. Picking a feature
shape now would be guessing.

## 3. Build commands

```bash
# Build the extension
cargo build -p pg_transport

# Schema + install (pgrx machinery)
cargo pgrx install --release
```

There is no `--features` flag in v0; there's nothing to toggle.

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
    // insert here (and a corresponding crates/ dep) when it lands.
    reg.handoff.insert("tcp_handoff", transport_tcp_handoff::build);
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
- [roadmap.md](roadmap.md) — phased plan; v0 has no Cargo features.
