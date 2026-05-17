# Comparisons — how `pg_transport` relates to neighbouring systems

> Parent: [README.md](README.md)

This doc situates `pg_transport` against the systems people will reasonably
compare it to. The goal is to set expectations honestly: where it overlaps
with a familiar tool, where it does something the other can't, and where
the other does something it can't. **pgbouncer is the most important
comparison and is covered first.**

For background reading on the systems whose ideas we lift wholesale, see
[../background/pg_background.md](../background/pg_background.md) and
[../background/omnigres.md](../background/omnigres.md).

---

## 1. pgbouncer

Both solve "stop forking a PG backend per client connection", but they
take **structurally different paths to it** — and the comparison is more
illuminating than that headline similarity suggests.

### 1.1 The big picture

|  | **pgbouncer** | **`pg_transport`** |
|---|---|---|
| Process model | External daemon | PG extension (bgworker inside the cluster) |
| Where it sits | In front of PG (client → pgbouncer → PG over TCP/UDS) | Inside PG (client → frontend bgworker → backend bgworker, all in-process) |
| Production maturity | Hardened since 2007, ubiquitous | Research framework, **deliberately not production** |
| Wire protocols on the client side | FE/BE v3 only | FE/BE v3 today; QUIC, HTTP/2, custom binary on the roadmap |
| Wire protocols on the backend side | FE/BE v3 over TCP/UDS to real PG backends | None — the "backend" is the same process talking via DSM + `shm_mq`, no FE/BE on the wire |
| Config surface | `pgbouncer.ini`, `userlist.txt`, signals to reload | SQL catalog tables (`pg_transport.transports`, GUCs), `pg_transport.reload()` |
| Code base | C, ~30 kLOC | Rust + pgrx, projected ~5–10 kLOC |
| OS portability | Unix + Windows | Unix-first (SCM_RIGHTS); Windows would need work |

### 1.2 Pooling-mode equivalence

pgbouncer's killer feature is its three pooling modes. Each maps to a
different path in our design:

| pgbouncer mode | Behaviour | `pg_transport` analog |
|---|---|---|
| **session** | One backend assigned for the whole client session; released on disconnect | **Handoff path** (`HandoffTransport` + `HandoffHandle`). Identical semantics: slot pinned for connection lifetime. See [handoff.md](handoff.md). |
| **transaction** | Backend assigned at `BEGIN`, released at `COMMIT` / `ROLLBACK`. Multiple clients share fewer backends, swapping per transaction. | **Not supported.** Would require transaction-boundary detection on the FE/BE wire; the v0 handoff path pins one connection to one slot for its whole lifetime. See [Known gap](#15-known-gap-transaction-pooling) below. |
| **statement** | Backend assigned per statement, released after each `Query` / `Sync`. Most aggressive multiplexing; prepared statements break. | **Not supported in v0.** The closest equivalent is `SessionHandle::execute(opts, payload)` on the **deferred** shm_mq general path — the slot would be acquired internally, the payload runs, the slot returns to the pool when `FrameStream` ends. See [api.md §6](api.md) and [backend-pool.md](deferred/backend-pool.md). |

So `pg_transport` covers two of pgbouncer's three modes natively (session
via handoff, statement via execute) and is missing the middle one
(transaction).

### 1.3 Latency, structurally

Per-query overhead, on top of what PG itself costs:

```
Direct PG (no pooler):           0 µs hop overhead (fork once at connect)

pgbouncer (transaction mode):    ~50–100 µs per query
  └── client → kernel → pgbouncer → kernel → PG backend → kernel → pgbouncer
       → kernel → client
       (~4 kernel context switches, full FE/BE parsing in pgbouncer)

pg_transport handoff path:       ~10 µs *once* at connect, then 0 µs ongoing
  └── after fd-pass, backend talks directly to client; frontend is gone

pg_transport execute() path:     ~50–150 µs per query
  └── full IPC: shm_mq + envelope + frame demux + mpsc routing
```

Headline result: **`handoff` is structurally faster than pgbouncer** for
steady-state queries (because pgbouncer has to round-trip every byte
through its process). **`execute()` is roughly in pgbouncer's ballpark**
— slightly faster (in-process, no kernel TCP) but with the same general
pattern.

### 1.4 What `pg_transport` gives you that pgbouncer can't

1. **Fd-pass performance.** No external pooler can match the v0 handoff
   path's near-zero steady-state overhead, because being in front of PG
   is the whole point of an external pooler.
2. **No separate daemon.** Configured via SQL, lifecycles tied to PG.
   One binary, one process tree.
3. **PG-native auth and TLS.** Handoff path uses PG's own
   `ClientAuthentication` and (optionally) `secure_open_server`.
   `pg_hba.conf`, `ssl_*` GUCs, the `cert` auth method — all "just
   work". pgbouncer has its own auth layer with subtle gotchas (SCRAM
   passthrough nuances, `auth_query`/`auth_user` setup, etc.).
4. **Tight integration with PG.** Catalog state, `pg_stat_*` views
   (planned), GUCs, signals — `pg_transport` participates in PG's
   existing observability rather than being a black box in front of it.
5. **(Deferred) Alternative wire protocols.** pgbouncer speaks FE/BE
   only. The deferred `SessionTransport` (see
   [backend-pool.md](deferred/backend-pool.md)) will let you serve HTTP/2 +
   JSON, custom binary protocols, or eventually QUIC, all dispatching to
   the same backend pool. Not in v0.

### 1.5 Known gap: transaction pooling

pgbouncer's transaction mode is the single biggest feature we don't have
and probably won't have at v0.x. Real apps using pgbouncer overwhelmingly
run in transaction mode for the connection-amplification factor.
Implementing it correctly is *possible* but non-trivial — you'd need to:

- Detect transaction boundaries on the FE/BE wire (`BEGIN` / `COMMIT` /
  `ROLLBACK` / implicit transactions from `BEGIN ATOMIC` / autocommit
  semantics).
- Decide what to do with **prepared statements** (pgbouncer either
  disables them or uses server-side preps with caveats).
- Reject or refuse `SET` (session-level GUC changes don't compose across
  transactions; clients break in subtle ways).
- Handle `WITH HOLD` cursors (they break transaction pooling).
- Surface the right error when a feature is used that transaction
  pooling can't support.
- Get all of this right for every edge case that breaks transaction
  pooling — pgbouncer has spent two decades polishing these.

For workloads that fundamentally need transaction pooling, stack
pgbouncer in front (see [§1.7](#17-can-you-stack-them)).

### 1.6 What pgbouncer gives you that `pg_transport` doesn't

1. **Transaction pooling** — see [§1.5](#15-known-gap-transaction-pooling).
2. **Multiple PG clusters from one pool.** pgbouncer can pool
   connections to many different PG instances (different hosts even).
   `pg_transport` is intrinsically one-cluster — it lives inside that
   cluster.
3. **External admin interface.** pgbouncer's `SHOW POOLS` / `SHOW
   CLIENTS` / `RECONNECT` / `PAUSE` are operationally rich. We have
   `pg_transport.list_v2()` and `pg_transport.reload()`; equivalent
   coverage would take real work.
4. **Production hardening.** Years of edge-case fixes for connection
   counting, retry behaviour, slow-client handling, server timeouts,
   idle disconnects, etc.
5. **`SUSPEND` / online restart.** pgbouncer can drain and restart with
   zero connection loss. We have no equivalent.
6. **No PG involvement.** pgbouncer works against unmodified PG.
   `pg_transport` requires the extension installed in the cluster.

### 1.7 Can you stack them?

Yes, and in some scenarios it makes sense:

```
client ── TCP ──→ pgbouncer ── TCP ──→ pg_transport.tcp_handoff ── handoff ──→ backend
         (transaction pooling)         (inside PG)
```

pgbouncer in front gets you transaction-mode pooling at scale;
`pg_transport` behind it makes the "PG backend" cheap to acquire
(pre-spawned slot rather than fork). The downside: you've added
pgbouncer's latency *plus* the handoff overhead. For most setups that
means "use one or the other"; the stack only pays off if you
specifically need both:

1. transaction-mode multiplexing for thousands of idle web clients, **and**
2. sub-millisecond connection establishment within pgbouncer's pool churn.

### 1.8 When to pick which

| You want… | Pick |
|---|---|
| Production connection pool for a typical web/SaaS workload | **pgbouncer** (transaction mode) |
| Custom wire protocol on top of PG (HTTP, QUIC, binary RPC, …) | **`pg_transport`** (no other option) |
| In-process listener with near-zero per-query overhead | **`pg_transport`** handoff |
| Pool connections across multiple PG hosts | **pgbouncer** (or Odyssey) |
| Research on alternative transports / kernel-bypass I/O | **`pg_transport`** |
| Single-binary deployment, SQL-managed config | **`pg_transport`** |
| Massive connection amplification (thousands of idle clients on tens of backends) | **pgbouncer** transaction mode |
| Tight observability integration with PG (`pg_stat_*`, GUCs, catalog) | **`pg_transport`** |
| TLS termination with `pg_hba.conf cert` mTLS, zero new code | **`pg_transport`** handoff (backend uses PG's TLS) |

### 1.9 One-line summary

> **pgbouncer** is what you reach for in production. **`pg_transport`** is
> what you reach for when you want to experiment with what's behind the
> listener — alternative transports, alternative wire protocols,
> in-process execution, lower per-query overhead than any external proxy
> can achieve. The Venn-diagram overlap is "pre-spawned PG backends to
> avoid fork-per-connect"; everywhere else they're solving different
> problems.

---

## 2. Odyssey (Yandex)

A faster, multi-threaded re-implementation of pgbouncer in C. Same
external-daemon model, same FE/BE-only scope. Faster than pgbouncer
under high concurrency; comparable feature set for our purposes.
Everything in [§1](#1-pgbouncer) applies; substitute "Odyssey" for
"pgbouncer" if that's the proxy you actually run.

---

## 3. `pg_background`

The direct ancestor of the backend pool. See
[../background/pg_background.md](../background/pg_background.md) for
the full architecture review.

| | `pg_background` | `pg_transport` |
|---|---|---|
| Trigger | SQL function call (`pg_background_launch(...)`) | Inbound network connection |
| Worker lifecycle | One bgworker per call; exits after the SQL completes | Pre-spawned pool; bgworkers live for the whole frontend lifetime |
| Per-call overhead | Full `RegisterDynamicBackgroundWorker` + fork (~1 ms+) | Pool checkout + `SCM_RIGHTS` handoff (~10 µs) |
| Result delivery | DSM + `shm_mq`, consumed by `pg_background_result_v2()` | v0: backend runs FE/BE on the inherited fd; bytes go directly to the client. Deferred (shm_mq general path): same DSM + `shm_mq` mechanism as `pg_background`, relayed by the frontend as a `FrameStream` \u2014 see [backend-pool.md](deferred/backend-pool.md). |
| Use case | Autonomous transactions from SQL ("run this in the background") | Listener / proxy / alternative-wire-protocol substrate |

`pg_transport` borrows `pg_background`'s pool-of-bgworkers idea but
takes its mechanism only **for the deferred shm_mq path** (see
[backend-pool.md](deferred/backend-pool.md)). v0 itself does **not** use DSM /
`shm_mq` / `pq_redirect_to_shm_mq` — it hands the kernel fd directly to
the bgworker, which runs PG's own `PostgresMain`-equivalent on it.

---

## 4. Omnigres (`omni_httpd` / `omni_worker`)

The architectural cousin. See
[../background/omnigres.md](../background/omnigres.md) for the full
review.

| | Omnigres `omni_httpd` | `pg_transport` |
|---|---|---|
| Wire protocol scope | HTTP/1.1, HTTP/2, HTTP/3 (via h2o) | FE/BE v3 today; HTTP/2 + others on the roadmap |
| Listener process model | Master bgworker + N HTTP worker bgworkers; each HTTP worker has a PG-touching main thread + non-PG-touching h2o thread | Single frontend bgworker (tokio current-thread) + N backend bgworkers; transports run as tokio tasks inside the frontend |
| Per-connection backend | Each HTTP worker IS a PG backend; runs handlers in-process via SPI | Each backend slot's bgworker IS a PG backend (recycled across handoffs); the transport is decoupled in the frontend |
| Configuration | SQL catalog tables (`omni_httpd.listeners`, route table) | SQL catalog tables (`pg_transport.transports`) — directly inspired by Omnigres |
| TLS termination | h2o-side (the secondary thread) | Backend-side, either PG OpenSSL or rustls sidecar (handoff path) |
| What's being researched | HTTP-as-PG-runtime application platform | Alternative transports / protocols for PG, performance research |

Omnigres confirms the load-bearing parts of our design:
listener-bgworker is viable, two-thread split for non-PG I/O is the
right pattern, configuration-as-catalog-state is the right ergonomics.
Where we diverge:

- **Omnigres bundles the whole stack** (HTTP server + handler routing +
  application primitives like auth/sessions/ledger). We're a deliberately
  narrow framework with a small API surface.
- **Omnigres co-locates listener and execution.** Each `omni_httpd`
  worker is both. We split them: frontend (transports) is separate from
  the backend pool, which lets transports be written without thinking
  about PG semantics.
- **Omnigres is HTTP-first.** We're protocol-agnostic by design — FE/BE
  on day one, with the backend contract specifically shaped to allow
  arbitrary wire protocols.

---

## 5. Default PostgreSQL (no extras)

Worth recapping because the handoff path is structurally a clone of how
default PG already works:

| Step | Default PG | `pg_transport` handoff |
|---|---|---|
| Listen on a port | postmaster's `ServerLoop` | frontend's tokio accept loop |
| Accept a connection | postmaster | frontend |
| Give the fd to a child process | `fork()` | `sendmsg(SCM_RIGHTS)` |
| Per-connection startup cost | full fork (~1 ms) | fd-pass (~10 µs); bgworker pre-spawned |
| Protocol probe + auth + `PostgresMain` | backend | backend bgworker (same PG code) |
| Lifecycle after disconnect | backend exits, postmaster reaps | backend resets per-session state, slot returns to pool |

Differences:

1. **`fork()` → `SCM_RIGHTS`** — pre-spawned children, ~100× cheaper per
   connection, recycled instead of exiting.
2. **One global listener → many configurable listeners** — we can run TCP
   on a custom port, UDS at a custom path, future io_uring-backed
   listeners, all simultaneously, all dispatching to the same pool.

For details and the full step-by-step see
[handoff.md §3](handoff.md#3-comparison-with-default-pg).

---

## See also

- [README.md](README.md) — design entry point.
- [handoff.md](handoff.md) — the fast path that beats pgbouncer on
  steady-state latency.
- [backend-pool.md](deferred/backend-pool.md) — *deferred* general path that
  *would* approximate pgbouncer's statement-mode pooling once it lands.
- [roadmap.md](roadmap.md) — phased build plan; transaction-pooling is
  *not* on the plan but could become Q16 if the project's scope shifts.
- [../background/pg_background.md](../background/pg_background.md)
- [../background/omnigres.md](../background/omnigres.md)
