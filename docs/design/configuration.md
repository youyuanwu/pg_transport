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
| `pg_transport.executor_pools`  | id, size, options                                             |

`kind` is the transport name registered at compile time (`tcp_handoff`,
`uds_handoff`, …). The `options jsonb` column is **deliberately free-form**:
each transport interprets it as it sees fit (TLS cert paths, auth method,
h2 settings, …). The framework neither parses nor validates the contents
beyond handing the blob to the transport's factory.

(`pg_transport.executor_pools.options` is the parking spot for
sizing knobs the deferred shm_mq path will need — `queue_depth`,
shared-slot policy, etc. — see [executor-pool.md](executor-pool.md).)

There is **no** `plugins` catalog table — the set of available transports is
fixed at compile time by the Cargo feature set. A read-only function
surfaces what's compiled in:

```sql
SELECT * FROM pg_transport.available();
--  kind        | description
-- -------------+-----------------------------------------------------------
--  tcp_handoff | TCP listener; blind SCM_RIGHTS fd-pass to executor
--  uds_handoff | Unix-domain listener; blind SCM_RIGHTS fd-pass to executor
```

The names describe *what the transport does with the fd* (handoff) on
*what kind of socket* (tcp / uds). The wire protocol the bgworker
actually speaks on the handed-off fd is the executor's concern — FE/BE
v3 in v0, but the transport doesn't claim or care.

For the rebuild-with-feature error path when a row references an
uncompiled transport, see [workspace.md §5](workspace.md#5-catalog--feature-interaction).

### GUCs

Registered on first use, à la `pg_background`:

- `pg_transport.executor_pool_size` (default `max(4, num_cpus)`).
- `pg_transport.socket_directory` (default `""` — fall back to the first
  entry of `unix_socket_directories`, then `/tmp`). Directory in which
  per-slot handoff control sockets live, named
  `.s.PG_TRANSPORT.<dispatcher_pid>.<slot_id>`. Mirrors PG's own
  `unix_socket_directories` story; set it to `/var/run/postgresql` on
  distros that put PG's client UDS there. See
  [handoff.md §2.1](handoff.md).
- `pg_transport.default_queue_size` (default 64 KiB) — reserved for the
  deferred shm_mq path; unused in v0.
- `pg_transport.metrics_port` (Prometheus scrape endpoint).
- Per-transport GUCs (e.g. `pg_transport.dpdk_cores`) are added as their
  phase begins. See [../future-transports.md](../future-transports.md).

### SQL surface

```sql
SELECT pg_transport.start();
SELECT pg_transport.stop();
SELECT pg_transport.reload();           -- reload transport set from catalog
SELECT pg_transport.add_transport(...);
SELECT * FROM pg_transport.available(); -- compile-time transport inventory
SELECT * FROM pg_transport.list_v2();   -- live dispatcher state
```

---

## 2. Observability

### Prometheus metrics (text format on `metrics_port`)

- `pg_transport_connections{transport=…,protocol=…,state=…}` — gauges.
- `pg_transport_frames_total{direction=…,protocol=…}` — counters.
- `pg_transport_bytes_total{direction=…,transport=…}` — counters.
- `pg_transport_dispatch_latency_seconds{stage=…}` — histograms
  (stages: `accept`, `auth`, `parse`, `enqueue`, `executor_busy`, `relay`).
- `pg_transport_executor_pool{state=…}` — gauges.

### Per-connection introspection

`pg_transport.list_v2()` surfaces the live dispatcher state — active
transports, their connections, executor-slot assignments, last error.
Mirrors `pg_background_list_v2()` in style. See
[../background/pg_background.md](../background/pg_background.md) for the
spiritual ancestor.

---

## See also

- [workspace.md](workspace.md) — Cargo features map to `available()` rows.
- [executor-pool.md](executor-pool.md) — `executor_pool_size` sizing,
  `metrics_port` exposure.
