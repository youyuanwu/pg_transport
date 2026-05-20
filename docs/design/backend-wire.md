# Backend wire layer — our FE/BE v3 implementation

> Parent: [README.md](README.md)
> Sibling: [backend-handoff.md](backend-handoff.md) · [frontend-handoff.md](frontend-handoff.md) · [api.md](api.md)

This doc describes the **wire layer** that runs on a fd handed to a
backend slot by the frontend. The slot runner
([backend-handoff.md](backend-handoff.md)) owns the fd and the
per-handoff lifecycle; the wire layer owns *everything that happens
on the byte stream*: TLS negotiation, FE/BE v3 startup,
authentication, the FE message loop, cancel coordination, error
reporting, parameter-status emission.

The wire layer is a Rust-native implementation. For why we cannot
reuse PG's `PostgresMain` / `ProcessStartupPacket` /
`ClientAuthentication` / `secure_open_server` /
`pq_getbyte` / `pq_putmessage`, see
[architecture.md §2](architecture.md#2-why-pg_transport-owns-the-wire-layer).
The wire layer still runs *inside* a PG backend bgworker, so it can
call `SPI_execute` and friends — but the bytes on the socket are
ours from accept-after-handoff to disconnect.

| Layer (BE side)              | Doc                                  |
| ---------------------------- | ------------------------------------ |
| Slot runner — fd ownership, lifecycle, reset | [backend-handoff.md](backend-handoff.md) |
| **Wire — TLS, auth, FE/BE protocol** | **this doc**                  |
| Execution — SQL via SPI       | (implicit; see §6)                   |

---

## 1. The `Wire` trait

**Live source:** [crates/core/src/wire/mod.rs](../../crates/core/src/wire/mod.rs).
The trait shape:

```rust
pub trait Wire {
    fn name() -> &'static str where Self: Sized;
    fn run(fd: OwnedFd, ctx: WireCtx) -> anyhow::Result<()>;
}
```

The `WireCtx` bundles the per-bgworker tokio runtime (`Rc<Runtime>`,
built once at slot startup, reused across handoffs) and the optional
`Arc<TlsAcceptor>`. Per-handoff reset bookkeeping (prepared
statements, portals) was added in phase 9.

In v0 there is **one** implementation, `pgwire_v3::PgwireV3`. The
trait exists so that v0.x can add `pgwire-v4` when PG ships one
without touching the slot runner, and so the deferred shm_mq path
([deferred/backend-pool.md](deferred/backend-pool.md)) can eventually
offer wire-shaped surfaces too.

There is no `Box<dyn Wire>`. The slot runner picks a concrete wire
at compile time — one slot runs one wire kind for its lifetime, and
avoiding the vtable simplifies the type signatures without changing
behaviour.

---

## 2. v0 implementation: `pgwire-v3` via the `pgwire` crate

We reuse [`pgwire`](https://github.com/sunng87/pgwire) (sunng87) as the
parser/encoder for FE/BE v3 messages — startup, password, SCRAM,
simple-query, extended-query (Parse/Bind/Describe/Execute/Close/Sync/Flush),
COPY, FunctionCall, error/notice frames, parameter-status. What we add:

- **Driver loop** — `pgwire` gives us a `MessageHandler` shape; we
  write the body that decides what to do with each message and how to
  hand SQL to SPI.
- **TLS termination** — `pgwire` itself is BYO-transport; we wrap the
  raw fd with `tokio-openssl` (rust-openssl) inside the slot's
  single-threaded tokio runtime (see
  [backend-handoff.md §3](backend-handoff.md)).
- **Auth wiring** — `pgwire`'s auth helpers handle SCRAM message
  exchange; we plug in our credential lookup (see §4) for the actual
  "is this password correct" decision.
- **Cancel coordination** — v0 does **not** route cancels. The wire
  layer recognises `CancelRequest` (via `pgwire`'s probe parsing) and
  closes the fd without further action. `BackendKeyData` is still
  emitted (libpq parses it), with a random `key` that's never
  honoured. The full cross-slot cancel-routing design is captured in
  [deferred/cancel-routing.md](deferred/cancel-routing.md).
- **SPI bridge** — `pgwire`'s `QueryHandler` / `ExtendedQueryHandler`
  trait bodies call into a small `crates/core/src/backend/spi_bridge.rs`
  module that translates between FE/BE message intent and `SPI_*` calls
  (see §6).

What we deliberately don't take from `pgwire`: the example query
backends and the in-crate auth backends (we want PG-cluster auth, not
toy in-memory backends).

The crate is a hard dep, single point of upstream risk; that's
acceptable because v0's wire scope (FE/BE v3 server) is exactly what
`pgwire` solves, and forking later is bounded work if we have to.

---

## 3. Startup negotiation

**Live source:** [crates/core/src/wire/pgwire_v3.rs](../../crates/core/src/wire/pgwire_v3.rs)
(`on_startup`). The fd we receive may carry, as its first 8 bytes:

| Probe              | Reply                                                              |
| ------------------ | ------------------------------------------------------------------ |
| `SSLRequest`       | `'S'` if `hints.tls_allowed`, then TLS handshake; else `'N'`        |
| `GSSENCRequest`    | **v0: `'N'`.** GSSAPI transport encryption is deferred — see note below. |
| `StartupMessage`   | plaintext continues; proceed to auth                                |
| `CancelRequest`    | **v0: log + close fd; do nothing.** Cancel routing is deferred — see [deferred/cancel-routing.md](deferred/cancel-routing.md). |

`pgwire` parses the probe; we own the dispatch. After TLS (if any) and
auth, we emit the usual `ParameterStatus` block + `BackendKeyData(pid=MyProcPid, key=random_u32)`
followed by `ReadyForQuery 'I'`, then enter the FE message loop. The
`key` in `BackendKeyData` is never honoured in v0 (no registry exists
to look it up against); it is emitted so libpq parses startup
completion correctly.

**`GSSENCRequest` deferred from v0.** Added in PG 12, magic
`80877104` — the Kerberos counterpart of `SSLRequest`. A client uses
it (typically when libpq's `gssencmode=prefer`/`require` finds a
Kerberos ticket) to negotiate GSSAPI transport encryption *before*
any login traffic flows: server replies `'G'` (accept, then a GSS
context handshake + `gss_wrap` / `gss_unwrap` around all subsequent
bytes, analogous to TLS records) or `'N'` (decline, client falls back
per its `gssencmode`). Implementing it would add a Rust GSSAPI binding
(`libgssapi`/`gssapi-sys`), a wrap/unwrap stream adapter analogous to
`tokio_openssl::SslStream`, and the `gss`/`sspi` HBA auth methods.
Almost all use is in Active-Directory / Kerberos shops; outside those
environments it's effectively unused. v0 always replies `'N'`;
revisit when a deployment demands it.

**Replication-mode startup deferred from v0.** A client that includes
`replication=true` (physical streaming — `pg_basebackup`, standbys,
`pg_receivewal`) or `replication=database` (logical — subscribers,
`pg_recvlogical`, Debezium / CDC) in its StartupMessage parameter
list is asking for a different wire mode entirely: the FE message set
narrows to a replication-command grammar (`IDENTIFY_SYSTEM`,
`START_REPLICATION`, `CREATE_REPLICATION_SLOT`, …), the connection
shifts to `CopyBothResponse` for long-running bidirectional streaming
of WAL records (physical) or decoded change events (logical), and the
backend hooks into a separate PG subsystem (`WalSender*` for physical;
`LogicalDecodingContext` + output plugins like `pgoutput` for
logical). That subsystem doesn't go through SPI — it's a parallel
implementation surface alongside everything else our wire layer does.
It's well out of v0 scope. The wire layer detects the `replication`
parameter during StartupMessage parsing and immediately rejects with
`FATAL` / `SQLSTATE 0A000` (feature_not_supported), hinting the
client to connect to PG's own 5432 listener instead. Replication
clients have well-defined handling for this kind of rejection.

---

## 4. Authentication

**Live source:** [crates/core/src/wire/auth/](../../crates/core/src/wire/auth/)
— [`hba.rs`](../../crates/core/src/wire/auth/hba.rs) wraps
`hba_getauthmethod`, [`scram.rs`](../../crates/core/src/wire/auth/scram.rs)
implements SCRAM-SHA-256 via the `pgwire` crate's SCRAM helpers,
[`verifier.rs`](../../crates/core/src/wire/auth/verifier.rs) reads
`pg_authid.rolpassword` via SPI.

Two mutually exclusive sources, selected by GUC `pg_transport.auth_source`.
The GUC has **no default**: an unset value is a startup error
(`FATAL: pg_transport.auth_source must be set to 'pg_hba' or 'pg_transport'`).
Forcing an explicit choice keeps deployments from drifting onto an
auth surface no operator knowingly opted into.

| Value             | Behaviour                                                                |
| ----------------- | ------------------------------------------------------------------------ |
| `"pg_hba"`        | Reuse PG's `pg_hba.conf` lookup, but **not** PG's `ClientAuthentication` C function. We call PG's HBA-lookup helpers (e.g. `hba_getauthmethod`) directly to obtain the `(auth_method, options)` tuple for `(role, database, client_addr, peer_uid, ssl)`. We then implement the method ourselves in Rust (SCRAM-SHA-256 via `pgwire`, MD5 via a small helper, `trust` / `reject` trivially). Methods we don't implement in v0 reject with a clear error. |
| `"pg_transport"`  | Consult a framework-owned catalog (`pg_transport.hba`; schema is deferred until the first deployment uses this mode — see note below) and ignore `pg_hba.conf` entirely. Useful for transport-specific auth policy or when the cluster's `pg_hba.conf` shouldn't apply to our listeners. |

Lookup is **either / or**, not fall-through. If the configured source
has no matching row, the wire layer rejects the connection with a
"no `pg_hba.conf` entry for host …"-style error (mirroring PG's
behaviour for an unmatched HBA scan); it does **not** try the other
source. Composability concerns (e.g. "framework rules first, HBA
fallback") are out of scope; if you need union semantics, encode them
in whichever single source you've selected.

**HBA reload on SIGHUP: deferred from v0.** PG re-reads
`pg_hba.conf` on `SIGHUP`; whether the wire layer picks up changes
without a framework restart depends on whether we cache HBA lookups
and, if so, how we invalidate them. v0 doesn't commit to an answer:
the simplest implementation calls `hba_getauthmethod` fresh on every
auth (which means SIGHUP "works" trivially via PG's own reload),
but a future caching layer (e.g. if `hba_getauthmethod` proves
expensive at high connect rates) needs an invalidation hook. We defer
the cache + invalidation design and accept whichever behaviour falls
out of the no-cache implementation in v0.

Methods supported in v0: `trust`, `reject`, `password` (cleartext over
TLS only), `md5`, `scram-sha-256`. Methods *not* in v0 (need new
phases): `cert`, `gss`, `sspi`, `ldap`, `pam`, `radius`, `peer`,
`bsd`, `ident`.

**`cert` and `peer` were originally next-priority but are both
blocked on prerequisites:**

- `cert` is blocked on a [pgwire 0.40 upstream
  bug](https://github.com/sunng87/pgwire/blob/master/src/tokio/server.rs)
  in `ClientInfo::client_certificates()` — the downcast targets
  `TlsStream<TcpStream>` but `process_socket` wraps in `MaybeTls`,
  so `.unwrap()` panics on every call. Reproduced with a working
  rustls `WebPkiClientVerifier` (allow_unauthenticated mode) +
  x509-certificate-based CN extraction; the dispatch logic is
  trivially correct but the bridge from rustls to the auth
  dispatcher panics. Un-block path: a 5-line upstream fix (or a
  local pgwire fork via `[patch.crates-io]`). Tracking upstream;
  un-defer when a patched pgwire release is available or we elect
  to carry a fork.
- `peer` is blocked on `uds_handoff` — a client-facing UDS
  transport is a separate v0+ work item per
  [frontend-handoff.md §1](frontend-handoff.md#1-when-to-use-this-path).
  `peer` semantics require `SO_PEERCRED` which is AF_UNIX-only;
  with v0's TCP-only listener there's nothing to authenticate
  against.

**`pg_transport.hba` catalog schema (when `auth_source = 'pg_transport'`).**
The table layout for the framework-owned HBA mode is intentionally
left unspecified until the first deployment uses it: design-by-use
is cheaper than design-by-speculation, and the most likely shape
(rows of `(role, database, address, method, options jsonb)`) drops
out naturally when there's a concrete consumer in front of us. In
v0 the `auth_source = 'pg_hba'` mode is the documented default
path and the only one tested in CI; the `'pg_transport'` mode
exists as a placeholder so the GUC's either/or model is honest
rather than a one-option fiction.

Credential storage: we read `pg_authid.rolpassword` for SCRAM/MD5
verifiers via SPI. (`SPI_execute` + the `pg_authid` system catalog;
standard memory-context and snapshot management for free.) The direct
catalog-scan alternative was considered and rejected as needless
risk — it would save microseconds at the cost of carrying our own
`MemoryContext` and snapshot handling for hot, security-sensitive
code. If phase-5 bench numbers show SPI overhead is material here
specifically, revisit (mirrors Q18's stance for SQL execution). We
don't duplicate hashes into our own catalog.

The wire layer never sees plaintext passwords on disk — only on the
wire briefly during the SCRAM exchange or `password` auth.

---

## 5. TLS

**Live source:** [crates/core/src/wire/tls.rs](../../crates/core/src/wire/tls.rs)
(`build` returns `Option<Arc<TlsAcceptor>>` for the slot).

**Library: rustls** via [`tokio_rustls`](https://crates.io/crates/tokio-rustls)
with the `ring` crypto provider (re-resolved as Q26;
[`_PG_init`](../../crates/core/src/lib.rs) installs the provider
process-wide). pgwire 0.40's `process_socket` takes a
`tokio_rustls::TlsAcceptor` and has no plug-in point for a different
TLS backend, which forced the switch from rust-openssl. Cost: TLS
material lives in our own GUCs rather than inheriting the cluster's
`ssl_*`; FIPS via the `aws-lc-rs` provider is a one-feature-flag
swap later if needed.

| GUC                          | Purpose                                                        | Default               |
| ---------------------------- | -------------------------------------------------------------- | --------------------- |
| `pg_transport.tls_cert_file` | Server certificate path (PEM)                                  | empty (TLS disabled)  |
| `pg_transport.tls_key_file`  | Server private-key path (PEM, PKCS#8)                          | empty (TLS disabled)  |

Both GUCs must be set together; setting only one FATALs at slot
boot. Inheriting the cluster `ssl_cert_file` / `ssl_key_file` /
`ssl_ca_file` as defaults is a planned ergonomics improvement; not
implemented in v0.

The slot builds the `TlsAcceptor` once at startup (cert/key load
cost off the handoff hot path) and stores `Arc<TlsAcceptor>` in
`WireCtx`. For each connection, pgwire wraps the raw fd via
`tokio_rustls::TlsAcceptor::accept`, runs the handshake on the
slot's single-threaded tokio runtime (see
[backend-handoff.md §3](backend-handoff.md) for the runtime's shape
and rationale), and from then on reads/writes plaintext through the
TLS adapter. `pgwire` only sees the plaintext side.

**Per-listener cert variation**: deferred. v0 uses one cert per
cluster. `HandoffHints` already carries `tls_allowed` (yes/no); a
later phase can extend it to `cert_id` for per-listener routing.

**`pg_stat_ssl` population**: **deferred from v0.** PG populates
`pg_stat_ssl` from inside `be-secure-openssl.c`; since we route TLS
through rustls in the wire layer, we no longer feed it. v0 accepts
the gap — SSL observability comes from operating-system tools
(`ss -tlnp`) or from clients (`\conninfo` in `psql`, the libpq
`PQsslAttribute` API). When demand surfaces, the natural fix is a
framework view `pg_transport.stat_ssl` populated by the wire layer.

---

## 6. SPI bridge

**Live source:** [crates/core/src/backend/spi.rs](../../crates/core/src/backend/spi.rs)
(safe Rust wrappers — `with_spi`, `SpiCtx`, `SpiPlan`, `SpiTuples`,
`TypeInput`/`TypeOutput`/`TypeReceive`/`TypeSend`) sits between the
FE/BE handlers and PG's `SPI_*` C surface.
[crates/core/src/backend/spi_bridge.rs](../../crates/core/src/backend/spi_bridge.rs)
is the simple-query (`'Q'`) handler;
[crates/core/src/backend/extended.rs](../../crates/core/src/backend/extended.rs)
is the extended-query (`P`/`B`/`D`/`E`/`S`) handler.

Once the wire layer has a parsed FE message (`Q`, `P`, `B`, `E`, …)
it calls into the bridge. The bridge:

- For `Query` (`'Q'`): `SPI_connect` → `SPI_execute` → walk result
  tuples → emit `RowDescription` + `DataRow…` + `CommandComplete` +
  `ReadyForQuery`.
- For `Parse` (`'P'`): `SPI_prepare`, store the resulting plan under
  the prepared-statement name in a wire-owned map (see Q in §8).
- For `Bind` (`'B'`): bind params to the plan, store the bound portal
  in a wire-owned map.
- For `Execute` (`'E'`): `SPI_execute_plan_with_params` against the
  bound portal, emit rows.
- For `Describe` / `Close` / `Sync` / `Flush`: standard FE/BE handling
  against the wire's prep/portal maps.
- For `COPY`: deferred for v0; bridge returns an error frame.

**Why SPI and not the planner+executor directly?**

SPI gives us the standard "run SQL inside a backend" surface — it
handles snapshot management, transaction-state checks, and result
materialisation. It's clean, well-supported across PG versions, and
the right starting point. The cost is one extra `MemoryContext` layer
and the SPI result-cursor materialisation overhead.

**Planner + executor direct path** (post-v0 optimization): once bench
numbers (roadmap phase 5) tell us whether SPI overhead is material
for typical workloads, a follow-up ADR may add a "lower-level" mode
that calls `pg_plan_query` + `CreatePortal` + `PortalDefineQuery` +
`PortalStart` + `PortalRun` + `PortalDrop` directly. v0 commits to
SPI; see [roadmap.md §2 Q18](roadmap.md#2-open-questions). The
full option survey (what `postgres.c` dispatchers are / aren't
callable, what *is* exported, three reuse strategies, phased
adoption plan) lives in
[deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md).

### 6.1 Routing `BEGIN` / `COMMIT` / `ROLLBACK` around SPI

SPI in its default ("atomic") mode rejects `TransactionStmt` nodes
with `ERRCODE_INVALID_TRANSACTION_TERMINATION` ("invalid
transaction termination"). The check lives in
[`src/backend/executor/spi.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/spi.c):

```c
if (IsA(stmt, TransactionStmt))
{
    if (context != PROCESS_UTILITY_QUERY_NONATOMIC)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION),
                 errmsg("invalid transaction termination")));
}
```

The rejection is a *safety* mechanism, not a missing feature. SPI is
recursive-SQL-from-C: the typical caller (PL/pgSQL function,
extension code, our bridge) is itself inside a transaction owned by
someone *above* it on the call stack, and that someone expects the
xact to still exist when the SPI call returns. Letting the inner
SQL `COMMIT` would commit the *caller's* transaction; the caller
would then try to keep using its now-defunct `CurrentTransactionState`
— use-after-free at the executor level.

PG 11 added a non-atomic mode (`SPI_OPT_NONATOMIC`) specifically for
procedures (`CREATE PROCEDURE` / `CALL myproc()`), where the
immediate caller (`exec_simple_query` running `CALL`) *does* know
how to recover when the procedure commits underneath it.

We don't use non-atomic mode in pg_transport for two reasons:

1. **Our xact wrapper owns the bracket.** [`spi::with_spi`](../../crates/core/src/backend/spi.rs)
   opens the xact, runs the closure, commits. A `COMMIT` inside the
   closure would leave `CommitTransactionCommand` asserting on
   already-closed state.
2. **A simpler alternative exists, matching what PG itself does.**
   PG's `exec_simple_query` doesn't go through SPI at all — it
   routes `TransactionStmt` parse nodes through `PortalRun` →
   `ProcessUtility` → `standard_ProcessUtility`, which calls
   `BeginTransactionBlock` / `EndTransactionBlock` /
   `UserAbortTransactionBlock` directly on the TBLOCK state machine.

We mirror that shape. [`parse_and_classify`](../../crates/core/src/backend/spi_bridge.rs)
in the simple-query bridge runs `raw_parser` once over the `'Q'`
message and tags any `TransactionStmt` nodes it finds. Tagged
statements skip SPI entirely and route through
[`handle_xact_control`](../../crates/core/src/backend/spi_bridge.rs),
which calls `BeginTransactionBlock` /
`EndTransactionBlock(false)` / `UserAbortTransactionBlock(false)`
directly inside `catch_unwind` for PG-ERROR recovery. Everything
else (DML, DDL, `SET`, `LISTEN`, ...) goes through SPI as normal.

The extended-query bridge ([`extended.rs`](../../crates/core/src/backend/extended.rs))
doesn't need this dance — tokio-postgres / pgbench-prepared / psql
`\bind` never send `BEGIN` through `Parse` (they always use the
simple-query path for xact-control), so the question doesn't come
up at Parse time. If a `TransactionStmt` *did* arrive on the
extended path, `SPI_prepare` would reject it with the same
`ERRCODE_INVALID_TRANSACTION_TERMINATION` and the wire-layer error
translation would surface a clean DbError to the client.

---

## 7. `WireCtx` — services the slot runner provides

The wire layer talks to the slot runner via a small context object:

```rust
pub struct WireCtx {
    /// Set at handoff time by the slot runner; reflects HandoffHints.
    pub tls_allowed: bool,

    /// Cooperative shutdown signal. Wire should return when this fires.
    pub shutdown: ShutdownToken,

    /// Framework auth source resolution.
    pub auth: AuthLookup,

    /// SPI bridge handle. See §6.
    pub spi: SpiBridge,
}
```

The wire never reaches around the slot runner to e.g. close the fd
out-of-band; lifecycle is the slot runner's job. The wire signals
"I'm done with this connection" by returning from `run`.

---

## 8. Open questions

The wire layer has **no remaining architectural open questions** for
v0. (Extended-query state ownership, cancel routing, `pg_stat_ssl`
parity, `pg_hba.conf` reload semantics, `GSSENCRequest` / GSS auth,
and replication-mode startup were open Qs here in earlier drafts;
they have either been resolved or deferred from v0 — see Q1 below,
[deferred/cancel-routing.md](deferred/cancel-routing.md), §5 above,
§4 above, §3 above, and §3 above respectively.)

One **specification item** was open for phase 9 (an implementation
checklist, no longer an architectural choice). Phase 9 shipped the
wire layer; the checklist below is now a status record rather than a
forward-looking spec.

1. **Extended-query state ownership.** ~~Open~~ **Resolved: option
   (a)** — the wire layer owns the names, SPI owns the plans.
   Implementation lands in [`crates/core/src/wire/extended.rs`](../../crates/core/src/wire/extended.rs)
   (pgwire's `ExtendedQueryHandler` + `QueryParser` impls) and
   [`crates/core/src/backend/extended.rs`](../../crates/core/src/backend/extended.rs)
   (SPI bridge: `prepare` + `execute`). The wire layer reuses
   pgwire 0.40's `MemPortalStore<PgTransportStatement>` (a
   `BTreeMap<String, Arc<StoredStatement<S>>>` + portal map);
   `PgTransportStatement` is `Arc<PreparedStatement>` where
   `PreparedStatement` owns an [`SpiPlan`](../../crates/core/src/backend/extended.rs)
   whose `Drop` impl calls `SPI_freeplan`. Option (b) (a new SPI
   naming surface) was rejected as PG-version-coupled work.

   **How each sub-question came out in the phase-9 impl.**
   - **Order.** N/A as posed — the map drops its `Arc<StoredStatement>`
     entries, the last `Arc` reaching zero calls our `SpiPlan::Drop`
     which calls `SPI_freeplan`. There is no separate plan-vs-portal
     ordering question because the portal holds a strong reference
     to its parent statement; portals drop first (Sync clears the
     unnamed portal; Close drops named ones), then statements when
     no portal still refers to them.
   - **Panic-safety.** As stated: `SPI_prepare` / `SPI_execute_plan`
     run inside `catch_unwind` + `AssertUnwindSafe`. On panic we
     `AbortCurrentTransaction` and convert the caught panic to a
     wire `ErrorResponse` via the same `panic_to_pgwire` machinery
     the simple-query path uses.
   - **Partial-failure semantics.** Per-handoff: not applicable —
     reset is *naturally* per-connection because we build a fresh
     pgwire `DefaultClient` (with its own `MemPortalStore`) per
     `process_socket` call, and the slot loop drops it at the end
     of each handoff. There's no shared cross-handoff state to
     partially-fail on.
   - **GUC / temp-table / cursor scope.** Not enforced in v0. A
     follow-up commit can add `DISCARD ALL` to the slot loop's
     post-handoff hook (after the `process_socket` return) if a
     real workload exhibits leakage. v0's `extended_query_per_handoff_state_isolation`
     test pins the per-connection-portal-store path; session-scope
     `SET` / `CREATE TEMP TABLE` leakage across handoffs on the
     **same slot** is a known v0 gap.
   - **`Sync` mid-extended-flow.** Half-bound portals are dropped
     on connection close (per the per-connection store discipline
     above). Mid-flow `Sync` clears the unnamed portal as pgwire's
     default `on_sync` does.

   Acceptance: the phase-9 e2e suite's
   `extended_query_per_handoff_state_isolation` test pins the
   "client A's PREPARE doesn't leak to client B" invariant for the
   connection-closes case. Same-slot session-scope leakage
   (`SET tz` / `CREATE TEMP TABLE` / session-scoped `PREPARE` on
   the same persistent SPI session across two handoffs) is a known
   v0 gap; the fix is a one-line `DISCARD ALL` in
   `slot::run_slot`'s post-handoff hook, but it interacts with the
   plan cache invalidation question and is held until a real
   workload demands it.

---

## 9. Comparison with PG's wire path

| Stage                           | PG (`src/backend/libpq/*`, `tcop/*`, `libpq/be-secure-openssl.c`) | `pg_transport` wire layer                |
| ------------------------------- | --------------------------------------------------------------- | ---------------------------------------- |
| Read 8-byte probe               | `ProcessStartupPacket`                                          | `pgwire` crate + our dispatcher          |
| TLS handshake                   | `secure_open_server` (OpenSSL via PG's wrappers)                | `tokio_openssl::accept` (rust-openssl)   |
| Startup-message parse           | `ProcessStartupPacket` continues                                | `pgwire` parses                          |
| HBA lookup                      | `ClientAuthentication` calls `hba_getauthmethod`                | we call `hba_getauthmethod` directly     |
| Auth method execution           | `ClientAuthentication` switches on method                       | wire-layer dispatcher; `pgwire` for SCRAM exchange |
| Password verification           | `CheckPWChallengeAuth` etc.                                     | our verifier reading `pg_authid.rolpassword` via SPI |
| Parameter status / BackendKey   | `BackendStartup`, `PostgresMain` prelude                        | wire-layer emits; v0 emits random `key` (no registry; cancels deferred) |
| FE message loop                 | `PostgresMain` switch                                           | wire-layer driver loop                   |
| Simple query execution          | `exec_simple_query`                                             | SPI bridge `SPI_execute`                 |
| Extended query execution        | `exec_parse_message`, `exec_bind_message`, `exec_execute_message` | SPI bridge `SPI_prepare` + `SPI_execute_plan_with_params` |
| CancelRequest                   | postmaster's `processCancelRequest`                             | wire-layer logs + closes fd (v0); routing deferred — see [deferred/cancel-routing.md](deferred/cancel-routing.md) |
| Error reporting                 | `errstart` / `errfinish` / `EmitErrorReport`                    | `ereport`-aware wrapper that emits `ErrorResponse` |

We *do* reuse:

- `SPI_*` for actual SQL execution (planner, executor, snapshot mgmt,
  expression evaluation all come along — we're not reimplementing those).
- `pg_authid` for credential storage.
- `pg_hba.conf` parsing (via `hba_getauthmethod`) — the rules file is
  shared with the cluster.
- `MemoryContext` infrastructure (we're a bgworker, we honour it).
- `ereport` for error semantics inside SPI.

What we *don't* reuse:

- `ProcessStartupPacket`, `ClientAuthentication`, `secure_open_server`,
  `PostgresMain`, `exec_*` from `tcop/postgres.c`, `pq_getbyte` /
  `pq_putmessage` / `pq_endmessage`.

---

## See also

- [backend-handoff.md](backend-handoff.md) — slot runner (the layer
  above this one).
- [frontend-handoff.md](frontend-handoff.md) — FE/IPC side; how the fd reaches us.
- [api.md](api.md) — framework-level transport surface (unaffected by
  the wire-layer rework).
- [roadmap.md](roadmap.md) — phase plan (wire layer = phase 4 / 5;
  per-listener TLS is a deferred Q; cancel routing is fully deferred
  with design in [deferred/cancel-routing.md](deferred/cancel-routing.md)).
