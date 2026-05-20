# Configuration & observability

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [workspace.md](workspace.md)

## 1. Configuration model

v0 ships **GUCs only**. The eventual model (borrowed from Omnigres)
is catalog state modifiable from SQL and watched via cache-invalidation
triggers; that surface is **not implemented yet** (see
[§1.3 Planned catalog surface](#13-planned-catalog-surface) below for
the sketch).

### 1.1 GUCs (implemented)

Defined in [crates/core/src/guc.rs](../../crates/core/src/guc.rs)
and registered from `_PG_init()`
([crates/core/src/lib.rs](../../crates/core/src/lib.rs)). All four
are `PGC_POSTMASTER` — read once at boot; changing them requires a
cluster restart.

| GUC | Type | Default | Notes |
| --- | --- | --- | --- |
| `pg_transport.backend_pool_size` | int (1–64) | `2` | Number of slot bgworkers `_PG_init()` registers. The design's eventual default is `max(4, num_cpus)`; the testing-friendly `2` lets `cargo pgrx test` run inside PG's default `max_worker_processes = 8`. See [backend-handoff.md §1](backend-handoff.md). |
| `pg_transport.auth_source` | string | **required, no default** | `'pg_hba'` or `'pg_transport'`. Validated at `_PG_init()`; an unset/invalid value FATALs at cluster start so operators see the mistake at boot rather than via mid-flight slot deaths. See [backend-wire.md §4](backend-wire.md). |
| `pg_transport.tls_cert_file` | string | empty | Path to the server TLS certificate (PEM). Empty disables TLS. See [backend-wire.md §5](backend-wire.md). |
| `pg_transport.tls_key_file` | string | empty | Path to the server TLS private key (PEM, PKCS#8). Empty disables TLS. Setting only one of `tls_cert_file` / `tls_key_file` FATALs at slot boot. |

GUCs the [original design](#13-planned-catalog-surface) reserved but
that aren't registered yet — `pg_transport.socket_directory`,
`pg_transport.tls_ca_file`, `pg_transport.tls_min_proto`,
`pg_transport.default_queue_size`, `pg_transport.metrics_port` — land
in their respective follow-on phases (mTLS / live reload / shm_mq /
metrics endpoint) and not before. See
[roadmap.md §2.3 Q23](roadmap.md#23-resolved) for the
"GUC surface starts at the phase that needs it" rule.

### 1.2 SQL surface (implemented)

One function, exported from
[crates/core/src/lib.rs](../../crates/core/src/lib.rs):

```sql
SELECT pg_transport_extension_version();
--  pg_transport_extension_version
-- --------------------------------
--                              1
-- (packed: MAJOR * 10_000 + MINOR * 100 + PATCH)
```

That is the entire SQL surface today. No catalog tables, no
`start()` / `stop()` / `reload()` — the v0 frontend bgworker comes up
with the postmaster and the single v0 transport binds at frontend
boot from `pg_transport.tls_*` and a hard-coded bind address.

### 1.3 Planned catalog surface (not implemented)

The design intent, kept here so the implementation phase has a target.
**None of this is implemented in v0.** Implementing it is gated on a
real need to reconfigure listeners without a restart — today there's
one transport bound to one address and an operator restart suffices.

#### Catalog tables (schema `pg_transport`)

| Table                          | Columns                                                       |
| ------------------------------ | ------------------------------------------------------------- |
| `pg_transport.transports`      | id, kind, bind_addr, options jsonb, enabled bool              |
| `pg_transport.backend_pools`   | id, size, options                                             |

`kind` is the transport name registered at compile time. In v0 the
only registered name is `tcp_handoff`; additional handoff transports
(`uds_handoff`, …) are deferred (see
[roadmap.md §1](roadmap.md#1-phased-build-plan) and
[deferred/future-transports.md](deferred/future-transports.md)). The
`options jsonb` column is **deliberately free-form**: each transport
interprets it as it sees fit (TLS cert paths, auth method, h2
settings, …). The framework neither parses nor validates the contents
beyond handing the blob to the transport's factory.

(`pg_transport.backend_pools.options` is the parking spot for
sizing knobs the deferred shm_mq path will need — `queue_depth`,
shared-slot policy, etc. — see
[deferred/backend-pool.md](deferred/backend-pool.md).)

There is **no** `plugins` catalog table — the set of available
transports is fixed at compile time. v0 has no Cargo features (see
[workspace.md §2](workspace.md#2-cargo-features--deliberately-minimal));
when the catalog lands, a read-only function will surface what's
compiled in:

```sql
SELECT * FROM pg_transport.available();
--  kind        | description
-- -------------+-----------------------------------------------------------
--  tcp_handoff | TCP listener; blind SCM_RIGHTS fd-pass to backend
```

The name describes *what the transport does with the fd* (handoff) on
*what kind of socket* (tcp). The wire protocol the bgworker actually
speaks on the handed-off fd is the backend's concern — FE/BE v3 in v0,
but the transport doesn't claim or care.

#### Planned SQL functions

The frontend bgworker is registered statically in `_PG_init()` (see
[Q22 in roadmap.md §2.3](roadmap.md#23-resolved)) and is therefore
running from postmaster start onward. The functions below would
operate on *listeners*, not on the frontend bgworker itself:

```sql
SELECT pg_transport.start();    -- bind listeners for every row where enabled=true
SELECT pg_transport.stop();     -- drop all live listeners (catalog rows untouched)
SELECT pg_transport.reload();   -- reconcile live listeners against the catalog
SELECT pg_transport.add_transport(...);
SELECT * FROM pg_transport.available(); -- compile-time transport inventory
SELECT * FROM pg_transport.list_v2();   -- live frontend state
```

Semantics worth pinning when the surface lands:

- `start()` is idempotent — calling it twice is a no-op on the
  second call.
- `stop()` drops listener fds; in-flight handoffs already past
  `HandoffHandle::handoff()` are unaffected (the slot owns the fd).
- `reload()` is `stop()` + `start()` with the catalog re-read in
  between; it is the only operation that picks up changes to
  `pg_transport.transports`.
- The frontend bgworker itself only exits on postmaster shutdown
  or `SIGTERM`; none of these SQL functions can take it down.

---

## 2. Observability (planned, not implemented)

None of the surface below exists in v0. Today's observability is
`pgrx::log!` / `pgrx::info!` lines in the standard PG log, plus
`pg_stat_activity` for the slot bgworkers (they show up as regular
backends because they `connect_worker_to_spi`).

### Prometheus metrics (text format on `metrics_port`)

- `pg_transport_connections{transport=…,protocol=…,state=…}` — gauges.
- `pg_transport_frames_total{direction=…,protocol=…}` — counters.
- `pg_transport_bytes_total{direction=…,transport=…}` — counters.
- `pg_transport_dispatch_latency_seconds{stage=…}` — histograms
  (stages: `accept`, `auth`, `parse`, `enqueue`, `backend_busy`, `relay`).
- `pg_transport_backend_pool{state=…}` — gauges.

### Per-connection introspection

`pg_transport.list_v2()` would surface the live frontend state —
active transports, their connections, backend-slot assignments, last
error. Mirrors `pg_background_list_v2()` in style. See
[../background/pg_background.md](../background/pg_background.md) for
the spiritual ancestor.

---

## See also

- [workspace.md](workspace.md) — the compile-time registry that backs
  `available()`; v0 has no Cargo features.
- [deferred/backend-pool.md](deferred/backend-pool.md) —
  `backend_pool_size` sizing, `metrics_port` exposure.
