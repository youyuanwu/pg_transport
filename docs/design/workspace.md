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
│   │   ├── handoff.md
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
│   ├── backend/                  # rlib; linked into core; backend-pool bgworker
│   │   └── src/{pool.rs, worker_main.rs, handoff.rs}
│   ├── handoff-listener/          # rlib; shared accept-loop helper used by
│   │                              #   every HandoffTransport. Stream-shaped
│   │                              #   API: takes Stream<Item=Result<OwnedFd>>,
│   │                              #   drives it until shutdown. See
│   │                              #   docs/design/transports.md §2.1.
│   ├── transport-tcp-handoff/      # rlib; gated by core feature `tcp-handoff`
│   │                              #   complete transport: TCP socket +
│   │                              #   accept loop + SCM_RIGHTS handoff
│   ├── transport-uds-handoff/      # rlib; gated by core feature `uds-handoff`
│   │                              # — deferred transports (iouring-pgwire,
│   │                              #   quic-quinn-pgwire, afxdp-*, dpdk-*,
│   │                              #   rdma-*, shmem-loopback-*, http2-sql)
│   │                              #   live in their own crates that will be
│   │                              #   added when their phase begins. See
│   │                              #   docs/design/deferred/future-transports.md and
│   │                              #   docs/design/deferred/backend-pool.md.
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
- Deferred helper crates (`protocol-pgwire-v3`, `protocol-http2`,
  `tls-rustls`) are not in the v0 tree — they only appear when the
  deferred `SessionTransport` path lands. See
  [transports.md §2](transports.md).

## 2. Cargo feature design

```toml
# crates/core/Cargo.toml
[features]
default = ["tcp-handoff", "uds-handoff"]

# Transports (each is a complete network entry point). v0 ships handoff
# transports only; SessionTransport-based features (http2-sql, etc.)
# are deferred — see docs/design/deferred/backend-pool.md.
tcp-handoff = ["dep:transport-tcp-handoff"]
uds-handoff = ["dep:transport-uds-handoff"]

# Convenience bundle
all-transports = ["tcp-handoff", "uds-handoff"]

# Deferred (see docs/design/deferred/future-transports.md and docs/design/deferred/backend-pool.md):
# iouring-pgwire, quic-quinn-pgwire, afxdp-*, dpdk-*, rdma-*,
# shmem-loopback-*, http2-sql. Each is added here as its phase begins.
```

This mirrors Omnigres's `-DOMNIGRES_INCLUDE / -DOMNIGRES_EXCLUDE` mechanism
with Cargo's native machinery.

## 3. Build commands

```bash
# Default build — TCP + UDS, both speaking FE/BE v3
cargo build -p pg_transport

# Custom subset
cargo build -p pg_transport --no-default-features \
    --features "tcp-handoff,uds-handoff"

# Schema + install (pgrx machinery)
cargo pgrx install --release
```

## 4. Registry — how transports get wired in

A single explicit function in `core` registers built-in transports, gated
by Cargo features. v0 has **one** trait (`HandoffTransport`) and
therefore one factory map. A second map (`session: HashMap<&'static
str, SessionFactory>`) is reserved in the `Registry` shape so the
deferred `SessionTransport` (see [backend-pool.md](deferred/backend-pool.md))
can land without a registry shape change; in v0 that map is always
empty.

We deliberately do **not** use the `inventory` crate or ctor-based
auto-registration: explicit lines here keep the dependency graph greppable
and the build matrix obvious.

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
    #[cfg(feature = "tcp-handoff")]
    reg.handoff.insert("tcp_handoff", transport_tcp_handoff::build);

    #[cfg(feature = "uds-handoff")]
    reg.handoff.insert("uds_handoff", transport_uds_handoff::build);

    // Deferred (iouring, quic, afxdp, dpdk, rdma, http2-sql, …) will land
    // in whichever map matches their trait. See docs/design/deferred/future-transports.md
    // and docs/design/deferred/backend-pool.md.
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

## 5. Catalog ↔ feature interaction

The `pg_transport.transports` catalog table references a transport by name
(e.g. `"tcp_handoff"`). The frontend resolves the name against the
**compile-time** registry. If the user references a name that wasn't
compiled in — or that is currently deferred — start-up fails with a clear
error:

```
ERROR:  transport "quic_quinn" referenced by transport id 7 is not present
DETAIL: this transport is currently deferred (see docs/design/deferred/future-transports.md).
        Once it lands, rebuild pg_transport with `--features quic-quinn`.
```

This keeps "configure via SQL" while trading runtime plugin choice for
compile-time enablement.

## See also

- [api.md](api.md) — `HandoffTransport`, `HandoffFactory` (and the
  deferred `SessionTransport` / `SessionFactory`).
- [configuration.md](configuration.md) — the catalog schema this resolves against.
- [roadmap.md](roadmap.md) — phased plan; new transports map to new Cargo features.
