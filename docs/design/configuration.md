# Configuration & observability

> Parent: [README.md](README.md)
> Sibling: [api.md](api.md) · [workspace.md](workspace.md)

## 1. Configuration model

Borrowed from Omnigres: configuration is **catalog state**, modifiable from
SQL, watched via cache-invalidation triggers (see the user-memory note on
`CacheInvalidateRelcache*` for the trigger pattern).

### Catalog tables (schema `pg_transport`)

| Table                          | Columns                                                       |
| ------------------------------ | ------------------------------------------------------------- |
| `pg_transport.transports`      | id, kind, bind_addr, options jsonb, enabled bool              |
| `pg_transport.backend_pools`  | id, size, options                                             |

`kind` is the transport name registered at compile time. In v0 the
only registered name is `tcp_handoff`; additional handoff transports
(`uds_handoff`, …) are deferred (see
[roadmap.md §1](roadmap.md#1-phased-build-plan) and
[../future-transports.md](deferred/future-transports.md)). The
`options jsonb` column is **deliberately free-form**: each transport
interprets it as it sees fit (TLS cert paths, auth method, h2
settings, …). The framework neither parses nor validates the contents
beyond handing the blob to the transport's factory.

(`pg_transport.backend_pools.options` is the parking spot for
sizing knobs the deferred shm_mq path will need — `queue_depth`,
shared-slot policy, etc. — see [backend-pool.md](deferred/backend-pool.md).)

There is **no** `plugins` catalog table — the set of available
transports is fixed at compile time. v0 has no Cargo features (see
[workspace.md §2](workspace.md#2-cargo-features--deliberately-minimal));
the registry is a single `insert` in `core` for `tcp_handoff`. A
read-only function surfaces what's compiled in:

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

For the error path when a row references a transport that isn't
registered, see
[workspace.md §5](workspace.md#5-catalog--registry-interaction).

### GUCs

Registered on first use, à la `pg_background`:

- `pg_transport.backend_pool_size` (default `max(4, num_cpus)`).
- `pg_transport.socket_directory` (default `""` — fall back to the first
  entry of `unix_socket_directories`, then `/tmp`). Directory in which
  per-slot handoff control sockets live, named
  `.s.PG_TRANSPORT.<frontend_pid>.<slot_id>`. Mirrors PG's own
  `unix_socket_directories` story; set it to `/var/run/postgresql` on
  distros that put PG's client UDS there. See
  [frontend-handoff.md §2.1](frontend-handoff.md).
- **Backend wire layer** ([backend-wire.md](backend-wire.md)) GUCs:
  - `pg_transport.auth_source` (**no default; required**). One of
    `"pg_hba"` or `"pg_transport"`. Selects whether the wire-layer
    auth lookup goes through PG's `pg_hba.conf` helpers or a
    framework-owned `pg_transport.hba` catalog. The two are mutually
    exclusive: a connection's auth is resolved against exactly one
    source, with no fall-through. **Validated at `_PG_init()`** (i.e.
    inside the postmaster, before any bgworker is allocated), not at
    the slot's first auth; missing or invalid values raise `FATAL` at
    cluster start so the operator sees the problem during boot rather
    than via mid-flight slot deaths. See
    [backend-handoff.md §1](backend-handoff.md#1-pool-spawning-_pg_init--shared_preload_libraries)
    and [backend-wire.md §4](backend-wire.md).
  - `pg_transport.tls_cert_file` / `pg_transport.tls_key_file` /
    `pg_transport.tls_ca_file` (defaults reuse cluster `ssl_cert_file`
    / `ssl_key_file` / `ssl_ca_file` if empty). TLS material for the
    backend's rust-openssl-based wire layer. See
    [backend-wire.md §5](backend-wire.md).
  - `pg_transport.tls_min_proto` (default `"TLSv1.2"`).
- `pg_transport.default_queue_size` (default 64 KiB) — reserved for the
  deferred shm_mq path; unused in v0.
- `pg_transport.metrics_port` (Prometheus scrape endpoint).
- Per-transport GUCs (e.g. `pg_transport.dpdk_cores`) are added as their
  phase begins. See [../future-transports.md](deferred/future-transports.md).

### SQL surface

```sql
SELECT pg_transport.start();
SELECT pg_transport.stop();
SELECT pg_transport.reload();           -- reload transport set from catalog
SELECT pg_transport.add_transport(...);
SELECT * FROM pg_transport.available(); -- compile-time transport inventory
SELECT * FROM pg_transport.list_v2();   -- live frontend state
```

---

## 2. Observability

### Prometheus metrics (text format on `metrics_port`)

- `pg_transport_connections{transport=…,protocol=…,state=…}` — gauges.
- `pg_transport_frames_total{direction=…,protocol=…}` — counters.
- `pg_transport_bytes_total{direction=…,transport=…}` — counters.
- `pg_transport_dispatch_latency_seconds{stage=…}` — histograms
  (stages: `accept`, `auth`, `parse`, `enqueue`, `backend_busy`, `relay`).
- `pg_transport_backend_pool{state=…}` — gauges.

### Per-connection introspection

`pg_transport.list_v2()` surfaces the live frontend state — active
transports, their connections, backend-slot assignments, last error.
Mirrors `pg_background_list_v2()` in style. See
[../background/pg_background.md](../background/pg_background.md) for the
spiritual ancestor.

---

## See also

- [workspace.md](workspace.md) — the compile-time registry that backs
  `available()`; v0 has no Cargo features.
- [backend-pool.md](deferred/backend-pool.md) — `backend_pool_size` sizing,
  `metrics_port` exposure.
