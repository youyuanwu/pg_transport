//! pg_transport public API.
//!
//! Trait definitions only — no Postgres deps, no proc-macros. Plugin
//! authors write pure Rust against these definitions. See
//! [`docs/design/api.md`](../../docs/design/api.md) for the contract.
//!
//! v0 surface (phase 0 stub): `HandoffTransport`, `HandoffHandle`,
//! shared types. Wire-up happens in subsequent phases per
//! `docs/design/roadmap.md`.
