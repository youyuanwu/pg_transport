# Roadmap — plan, open questions, risks, references

> Parent: [README.md](README.md)

## 1. Phased build plan

Strict ordering — each phase locks in a contract that subsequent phases
extend rather than break. **v0 ends at phase 6.5.** Phase 7
(`SessionTransport` + shm_mq general path) is **deferred**; see
[§4 Deferred for v0](#4-deferred-for-v0). Other deferred transport
phases are listed in [../future-transports.md §4](deferred/future-transports.md).

| Phase | Deliverable                                                   | Gate / done criterion                                                |
| ----- | ------------------------------------------------------------- | -------------------------------------------------------------------- |
| **0** | Repo scaffold, ADRs, this design doc set                      | `cargo check -p api -p core` passes                                  |
| **1** | `core` bgworker boots a tokio current-thread runtime          | Heartbeat task logs every 1 s; clean SIGHUP/SIGTERM via `tokio::signal`; postmaster-death watchdog exits the runtime |
| **2** | `backend` pool slot runner — pre-spawned bgworkers, per-slot UDS control socket | Pool starts N backend bgworkers; frontend can `sendmsg(SCM_RIGHTS)` a dummy fd to a slot; slot runner reads and closes it; no wire layer yet |
| **3** | `tcp_handoff` transport (`handoff::tcp`) as a `HandoffTransport` + null-wire | `psql` connects to the listener; the null wire closes immediately with a synthetic `FATAL` (no SQL yet). Validates the FE → BE fd-pass path end-to-end. |
| **4** | `wire::pgwire_v3` first impl — startup, simple-query (`'Q'`), SPI bridge | `psql -c 'SELECT 1'` returns `1`. `trust` auth only (no TLS, no SCRAM yet). Extended-query reports a clear "not supported" error frame. |
| **5** | `bench/` harness                                              | Reports p50/p95/p99 latency, throughput; produces a CSV per run. If numbers show SPI overhead is material, a follow-up ADR considers a planner+executor direct path (post-v0; see [§2 Q18](#2-open-questions)). |
| ~~6~~ | ~~`transport-uds-handoff` as a `HandoffTransport`~~            | **DEFERRED** — v0 ships exactly one transport (`tcp_handoff`). A second handoff transport (Unix-domain, abstract-namespace, `sd_listen_fds`-fed, io_uring-backed, …) is the natural follow-on once the in-tree transport has flushed out the wire / handoff path, but is not on the v0 critical path. |
| **7** | Wire-layer auth — `hba_getauthmethod` lookup + Rust impls (trust / reject / password / md5 / scram-sha-256) | `psql -c "..."` works with SCRAM-SHA-256 against a SCRAM-stored role. Methods not yet implemented (cert/peer/gss/ldap/...) emit a clear rejection. |
| **8** | Wire-layer TLS — rust-openssl via `tokio_openssl`             | `psql sslmode=require` connects via TCP and UDS; mTLS via the `cert` auth method works once that auth method lands (likely in this phase too). |
| **9** | Extended-query support — Parse/Bind/Describe/Execute/Close/Sync/Flush in the wire layer + SPI plan reuse | Prepared statements work end-to-end; per-handoff reset drops the prep/portal maps and frees plans. |
| ~~10~~ | ~~Wire-layer cancel routing~~                                | **DEFERRED** — v0 wire layer drops `CancelRequest` fds; Ctrl-C in `psql` terminates the connection. Design captured in [deferred/cancel-routing.md](deferred/cancel-routing.md). |
| ~~F1+~~ | ~~`transport-http2-sql` as a `SessionTransport`~~              | **DEFERRED** — see [§4 Deferred for v0](#4-deferred-for-v0) |

v0 ends at phase 9. Phases 3 and 4 are the integration spike for the
socket / wire / SPI layer split; expect ADRs out of those before later
phases lock in.

Bench harness arrives **before** the second transport (phase 6)
deliberately — without it, later "did this help?" questions are
unanswerable.

Deferred transports (io_uring, QUIC, AF_XDP, DPDK, RDMA, shmem-loopback)
resume at "phase F1" in [../future-transports.md §4](deferred/future-transports.md),
gated on a stable in-scope baseline producing comparable benchmark numbers.
The deferred `SessionTransport` path (was "phase 7" in earlier drafts)
is a *prerequisite* for several of those (HTTP/2 + SQL, datagram QUIC,
DPDK), so its un-deferral is on the critical path for the wider
transport roadmap.

---

## 2. Open questions

This section is the project's **decisions log**. It is split into
three sub-lists: **still open — v0 path** (decision outstanding,
needed for v0), **still open — deferred only** (decision can wait
until the deferred shm_mq general path un-defers), and **resolved**
(decision made; kept here so the reasoning is auditable).
Q-numbers are stable across the splits — they're cited from other
docs and never get reused when an item moves from one sub-list to
another.

Deferred-only open questions for exotic transports (0-RTT QUIC,
connection migration, RDMA protocol pairing, DPDK CPU budget) live in
[../future-transports.md §5](deferred/future-transports.md).

### 2.1 Still open — v0 path

**Q25. SQL parsing in the wire layer (multi-statement simple-query
and full xact-control classification).** Two related correctness
gaps in [`crates/core/src/backend/spi_bridge.rs`](../../crates/core/src/backend/spi_bridge.rs)
that share one root cause — we have no SQL parser:

- **Multi-statement simple-query.** pgwire's `'Q'` message body can
  contain multiple statements separated by `;` (real PG handles
  this; `psql -c 'SELECT 1; SELECT 2'` sends one `'Q'`). Today we
  pass the entire string to `Spi::connect_mut(|c| c.update(query, …))`
  — SPI parses and runs all statements but only retains the *last*
  `SPI_tuptable`, so `SELECT 1; SELECT 2` returns `2` only (the
  `1` is silently dropped) and the wire frames are wrong
  (single RowDescription instead of one per statement).
- **Partial xact-control classification.** `parse_xact_control`
  matches bare-keyword forms only (`BEGIN`, `COMMIT`, …). Anything
  fancier — `BEGIN ISOLATION LEVEL SERIALIZABLE`,
  `START TRANSACTION READ ONLY`, `SAVEPOINT s1`, `RELEASE s1`,
  `ROLLBACK TO s1` — falls through to SPI and gets rejected with
  `SPI_ERROR_TRANSACTION` from atomic mode.
- **Multi-statement + xact-control** (`BEGIN; SELECT 1; COMMIT` as
  one `'Q'`) hits both gaps: the keyword match doesn't recognise
  the multi-statement string, SPI sees the whole thing, BEGIN
  hits SPI atomic-mode rejection.

Options:

- **(a) Reject multi-statement** (`Vec<&str>` of length > 1 from a
  lexer = error). Conservative interim fix; doesn't address the
  partial-classification gap.
- **(b) Adopt `pg_query` (libpg_query bindings)** — PG's own parser
  extracted as a C library by pganalyze. Provides
  `split_with_parser(&str) -> Vec<String>` for the splitting
  problem (handles dollar-quoting, `--` / `/* */` comments,
  string literals correctly by definition since it IS PG's
  lexer) AND a parse-tree API that gives us `TransactionStmt`
  node classification for free, covering every xact-control
  variant the grammar accepts. Compile cost: ~5 MB of vendored
  C parser source, ~60–90 s on first clean build, cached
  thereafter.
- **(c) Adopt `sqlparser-rs`** — pure-Rust SQL parser with a
  Postgres dialect. Real lexer, but the grammar is not
  byte-compatible with PG's (disagrees on edge cases like
  operator-class precedence and some PG-only DDL). Lighter
  compile cost than libpg_query; slightly worse fidelity.

**Lean (b) — `pg_query`.** Phase 9 (extended query) needs to parse
query strings anyway when handling the `Parse` message, so adding
libpg_query now is also phase-9 infrastructure. Until this is
resolved, the SPI bridge keeps the bare-keyword `parse_xact_control`
sniff and the inline TODO comments documenting the gap; the bench
recipe avoids multi-statement queries (each `'Q'` sends one
statement).

New v0-path questions land here as they're identified.

### 2.2 Still open — deferred only

Decisions that don't need to be made until the deferred shm_mq general
path (see [§4](#4-deferred-for-v0)) un-defers. Listed here for
completeness; un-deferring the feature reopens these for ADRs.

**Q6. Cross-process wakeup model.** Option A (interval poll), Option B
(eventfd bridge), or Option C (PG-latch bridge thread). Only relevant
to the deferred shm_mq path — the v0 handoff path needs no
cross-process wakeup once the fd is handed off. See
[backend-pool.md §6](deferred/backend-pool.md#6-cross-process-wakeup-how-the-frontend-knows-theres-data).

**Q7. Latch bridge fidelity (if we adopt Option C).** pgrx's
`attach_signal_handlers` sets PG-side flags (`ConfigReloadPending`,
`ShutdownRequestPending`). We additionally take `SIGHUP` / `SIGTERM`
via `tokio::signal::unix::signal`. Do we ever need to *also* poll those
flags from a tokio interval task, or is reacting to the signal
sufficient? Probably the latter, but verify. (Conditional on Q6
adopting Option C; otherwise moot.)

**Q10. Shared slots for stateless protocols.** Phase 2 pins one
connection per slot exclusively, which is correct for the handoff
path. A future phase, on top of the deferred shm_mq path, can let
stateless transports (HTTP/2 SQL, datagram protocols) share a slot
with `conn_id`-based multiplexing and explicit per-request session
reset. See
[backend-pool.md §7](deferred/backend-pool.md#7-slot-allocation-pinning-and-lifetime).

**Q11. `Payload` variant set.** Today (in the deferred design): `Raw`,
`Sql`, `Extended`. Do we need more (`CopyIn` / `CopyOut` streams,
`Notify` subscribe, cursor fetch)? Add as needed when the shm_mq path
lands; `Payload` is `#[non_exhaustive]` so it's a non-breaking change.

**Q14. Should we add a third trait + `QueryHandle` for stateless-only?**
Pre-empts a slicing decision *within* the deferred `SessionHandle`:
`execute` (stateless) vs `acquire` (stateful) live on the same handle,
but a transport that never calls `acquire` is *in practice* a query
transport that the type doesn't enforce. Adding `QueryTransport` +
`QueryHandle` would enforce statelessness at the type level. Lean
**no** — the meaningful boundary is handoff-vs-session; over-splitting
forces breaking changes when a transport later wants `BEGIN`/`COMMIT`
continuity. Revisit when the shm_mq path lands.

**Q15. Live pool resize.** `pg_transport.backend_pool_size` is read
once at frontend startup; v0 requires `stop()` / `start()` (or a
postmaster restart) to change it. Should `pg_transport.reload()` be
able to grow and/or shrink the pool without a frontend restart? Lean
**yes for grow, no for shrink** at first:

- *Grow* is cheap and safe — call `RegisterDynamicBackgroundWorker`
  for the additional slots, allocate their Unix control sockets,
  plumb them into the semaphore. No in-flight work is affected.
- *Shrink* needs a drain protocol — mark slots "no new handoffs",
  wait for current handoffs to finish (potentially unbounded), then
  tell the bgworker to exit cleanly. Avoidable for now; if a real
  deployment needs it, it's a follow-up ADR.

Deferred from v0: a static pool sized at `_PG_init()` is sufficient
for the research framework and the phase-5 bench harness (operator
picks a size, the bench characterises it). Un-defer when a real
deployment hits a sizing mistake they can't restart away. Strictly a
v0-feature defer rather than a shm_mq-path defer, but the un-defer
trigger is the same: real-deployment demand.

**Q16. Wire-layer cancel routing.** v0 drops `CancelRequest` fds
silently; Ctrl-C in `psql` terminates the connection rather than
cancelling the query. The option analysis and the recommended Option-B
design (frontend-owned registry over a multi-tag per-slot UDS
protocol, `kill(SIGINT)` for the actual interrupt) are captured in
[deferred/cancel-routing.md](deferred/cancel-routing.md). Un-defer
when a real deployment needs Ctrl-C-cancel semantics. (Strictly this
is a v0-feature defer rather than a shm_mq-path defer, but the
likely un-defer trigger is the same: a real deployment asking for
it.)

**Q19. `pg_stat_ssl` parity.** PG populates `pg_stat_ssl` from
`be-secure-openssl.c`. We don't (our TLS goes through rust-openssl in
the wire layer). v0 accepts the gap; SSL observability comes from OS
tools and clients. When un-deferred, the natural fix is a framework
view `pg_transport.stat_ssl` populated by the wire layer; secondary
fix is a hook into `pg_stat_ssl` itself. See
[backend-wire.md §5](backend-wire.md). (Like Q16, technically a
v0-feature defer rather than shm_mq-path defer.)

### 2.3 Resolved

Decisions made. Kept here so the reasoning is auditable and so future
readers don't re-litigate them by accident.

**Q1. Auth handshake location.** ~~Open~~ **Resolved.** v0 runs auth
in the backend's wire layer ([backend-wire.md §4](backend-wire.md)):
it calls PG's `hba_getauthmethod` to obtain the `(method, options)`
tuple, then implements the method itself in Rust (SCRAM via `pgwire`
crate helpers, MD5 via a small wrapper, trust/reject trivially). We do
**not** call PG's `ClientAuthentication`. Method coverage in v0:
trust / reject / password / md5 / scram-sha-256; cert/peer next, the
rest later as needed. When the deferred shm_mq path lands, FE/BE-aware
transports on it will need to run auth themselves before calling
`acquire`; a thin `protocol-pgwire-auth` helper crate wrapping SCRAM
is a likely addition then.

**Q3. TLS termination location.** ~~Open~~ **Resolved.** v0
handoff-path transports terminate TLS in the backend wire layer using
rust-openssl (`openssl` + `tokio-openssl` crates); see
[backend-wire.md §5](backend-wire.md). We do **not** call PG's
`secure_open_server`. Cert/key default to the cluster's `ssl_*` GUCs
at frontend startup; framework GUCs (`pg_transport.tls_cert_file` etc)
can override. The deferred shm_mq path would have its transports
terminate TLS themselves via the (also deferred) `tls-rustls` helper.

**Q2. Per-session backend pinning vs. session migration.** ~~Open~~
**Resolved — forced by architecture and the other resolutions.** Two
sub-questions, both decided:

- *Pinning vs. migration.* Architecturally forced in v0: the handoff
  path moves the `OwnedFd` into the slot via `SCM_RIGHTS`; once
  received, the fd lives in that slot bgworker's process. You can't
  migrate an open TCP connection between processes, so migration is
  impossible by construction. The slot is pinned for the connection's
  lifetime.
- *Failure model when a backend dies mid-session.* Determined by the
  cluster Q9 + Q20 + Q21. With `panic = "unwind"` (Q9 re-resolved
  via Q24) a fault in a slot unwinds; the wire layer catches it via
  `PgTryBuilder` and emits a wire `ErrorResponse` where it can,
  otherwise the slot exits and the postmaster respawns; with the
  simplest `handoff()` error contract (Q20) the dying slot doesn't
  surface a special error — the future already returned `Ok(())`;
  with Q21 (accept silent loss) the current client connection sees
  TCP RST and the slot is respawned on the *next* `sendmsg → EPIPE`.
  Net: **dropped client connection, slot respawn, no replay** for
  uncaught panics; clean wire `ErrorResponse` for caught SPI errors.

The deferred shm_mq path inherits the same pinning policy by default;
see [backend-pool.md §7](deferred/backend-pool.md#7-slot-allocation-pinning-and-lifetime).

**Q4. `shared_preload_libraries` requirement.** ~~Open~~ **Resolved:
SPL is required.** v0 ships a pre-spawned bgworker pool whose lifetime
matches the cluster's; the natural fit is static registration in
`_PG_init()` from the postmaster, which means
`shared_preload_libraries = 'pg_transport'` is mandatory. `_PG_init()`
checks `process_shared_preload_libraries_in_progress` and raises
`FATAL("pg_transport must be in shared_preload_libraries")` if it's
running in a regular backend instead of the postmaster. The full spawn
sequence (pgrx `BackgroundWorkerBuilder::load()`, restart policy, why
we don't use `load_dynamic()`) is in
[backend-handoff.md §1](backend-handoff.md#1-pool-spawning-_pg_init--shared_preload_libraries).
Trade-offs we accept: operator-side config burden (the SoT review's F2
risk); no `CREATE EXTENSION`-and-go ergonomics like `pg_background`.
Trade-offs we get: first-connection latency excludes pool spawn;
postmaster owns restart policy; one operational story ("pool up iff
postmaster up"). Live pool resize via `load_dynamic()` is deferred
(see Q15).

**Q5. Cargo feature defaults.** ~~Open~~ **Resolved: no Cargo features
in v0.** v0 ships exactly one transport (`tcp_handoff`), compiled in
unconditionally. There is no `[features]` table in
`crates/core/Cargo.toml` in the first plan. Cargo features come back
into the picture only when a second transport lands (post-v0) and the
decision can be made with a concrete second transport in front of us,
not in the abstract.

**Q8. `async_trait` lifetime.** ~~Open~~ **Resolved: no `async_trait`,
ever.** The `HandoffTransport` trait returns a manual
`Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'static>>` (aliased
as `RunFuture`); impls write `Box::pin(async move { … })` at the top
of `run`. Reasons in [api.md §1](api.md#1-the-handofftransport-trait):
one async method called once per transport lifetime makes the macro's
ergonomic payoff invisible; same runtime shape as the macro desugar;
no proc-macro dep in `crates/api/`; cleaner errors; trivial migration
to native `async fn in traits` (drop `Box::pin`, change `RunFuture` to
`impl Future<…>`) without first un-injecting `'async_trait` lifetime
mangling. (Same call will apply to the deferred `SessionTransport`.)

**Q9. Panic isolation in tokio tasks.** ~~Open~~ ~~Resolved:
`panic = "abort"`~~ **Re-resolved: `panic = "unwind"`** (after
[Q24](#23-resolved) showed the original choice was incompatible with
pgrx 0.18's error machinery). Both the workspace `[profile.dev]` and
`[profile.release]` set `panic = "unwind"`.

The original Q9 rationale for `abort` (kept here for the auditable
record) was:

- **Consistent with PG's posture.** `ereport(FATAL)` already exits
  the backend process; making Rust panics behave the same (process
  abort, no Drop-based recovery) keeps one mental model for
  "something is wrong, this process is over."
- **No `catch_unwind` boilerplate.** Wrapping every `spawn_local` or
  `W::run` call in `AssertUnwindSafe` + `catch_unwind` is invasive,
  foot-gun-prone (`UnwindSafe` bounds bite at the wrong moment), and
  loses the actual panic location in the log.
- **Slot death is already handled.** When a backend bgworker dies
  (which is exactly what a panic now does), the frontend's per-slot
  `sendmsg` returns `EPIPE` on the next handoff and the slot is
  respawned (see [Q21](#23-resolved) and
  [backend-handoff.md §6](backend-handoff.md#6-slot-lifecycle)). We
  don't need a *second* recovery path for the in-process case.
- **Per-handoff reset becomes simpler.** With no unwind, the reset
  step in [backend-wire.md §8 Q1](backend-wire.md#8-open-questions)
  doesn't need a `Drop`-guarded fallback path — a panicking slot
  just dies; the next handoff lands in a fresh one.

Those arguments were correct in isolation but missed that pgrx
itself uses Rust panics as its ERROR-propagation mechanism (see
[Q24](#23-resolved)). Under `panic = "abort"` we get SIGABRT instead
of readable errors, and `PgTryBuilder` / `catch_unwind` (which the
phase-4 wire bridge needs to convert SPI errors into wire
`ErrorResponse` frames) cannot fire. Q24 surveyed three options and
picked option (a): switch to `unwind`. The cost is the
`catch_unwind` / `AssertUnwindSafe` boilerplate Q9 was trying to
avoid, applied where we deliberately want to capture (rather than
propagate) a panic.
**Q12. TLS implementation in the backend wire layer.** ~~Open~~
**Resolved.** rust-openssl (`openssl` + `tokio-openssl` crates), by
default. Reuses the same OpenSSL the rest of the cluster links
against, so FIPS modes / OS trust stores / OpenSSL config files keep
applying. The original OpenSSL-vs-rustls-sidecar question became moot
once we decided not to use PG's `secure_open_server` at all. See
[backend-wire.md §5](backend-wire.md).

**Q13. Per-listener TLS variation.** ~~Open~~ **Resolved: one cert per
cluster in v0.** TLS material comes from `pg_transport.tls_cert_file`
/ `pg_transport.tls_key_file` / `pg_transport.tls_ca_file`, defaulting
to the cluster's `ssl_cert_file` / `ssl_key_file` / `ssl_ca_file` when
unset. The single v0 transport (`tcp_handoff`) speaks one cert for
all handoffs. Rationale: with exactly one transport and a research
framework focus, per-listener cert variation has no concrete consumer
yet; building the indirection in advance would be speculative.

The extension point is pre-allocated, not built: when a real
deployment asks for per-listener certs (e.g. distinct cert per
listener address, or per-transport-row cert from the catalog), the
shape is to add a `cert_id: Option<u32>` field to `HandoffHints` (see
[backend-handoff.md §4](backend-handoff.md#4-handoffhints)) and have
the wire layer resolve it per-handoff against a small cert registry
in the frontend. The `HandoffHints` struct is the framework-internal
seam; the wire layer is the consumer; no public-trait churn.

**Q17. Extended-query state ownership.** ~~Open~~ **Resolved: option
(a)** — the wire layer owns the names (`HashMap<String, SpiPlan>` and
`HashMap<String, BoundPortal>`) and SPI owns the underlying plans.
Option (b) (a new SPI-side naming surface) was rejected as
PG-version-coupled work that doesn't earn its keep. **Spec work that
remains** — per-handoff reset correctness (order, partial-failure
handling, GUC/temp-table scope, mid-flow `Sync`) is enumerated as a
phase-9 ADR checklist in
[backend-wire.md §8 Q1](backend-wire.md#8-open-questions); it's a
specification item, no longer an architectural choice.

**Q18. SPI vs. planner+executor direct path.** ~~Open~~ **Resolved:
v0 uses SPI exclusively.** `SPI_execute` for simple-query and
`SPI_prepare` + `SPI_execute_plan_with_params` for extended-query, in
the wire layer's SQL dispatch. Reasoning in
[backend-wire.md §6](backend-wire.md#6-spi-bridge): SPI is the
standard "run SQL inside a backend" surface; it handles snapshot
management, transaction-state checks, and result materialisation;
it's well-supported across PG versions. The cost — one extra
`MemoryContext` layer and result-cursor materialisation — is
acceptable for a research framework whose first job is correctness,
not peak throughput.

A **planner+executor direct path** (`pg_plan_query` + `CreatePortal`
+ `PortalDefineQuery` + `PortalStart` + `PortalRun` + `PortalDrop`)
would trade more PG-version-coupled code for lower per-query
overhead. That's a **post-phase-5 optimization**, not a v0
architectural question: the phase-5 bench harness produces numbers,
and only if those numbers show SPI overhead is material for typical
workloads does a follow-up ADR explore the direct path. Until then,
the v0 wire layer uses SPI without qualification.

**Q20. `HandoffHandle::handoff()` error contract.** ~~Open~~
**Resolved: take the simplest answer in each case.** Research
framework; the API can grow more sophisticated later if real usage
finds the simple answer insufficient.

- **Backend pool exhausted.** Block on a `tokio::sync::Semaphore`
  sized to the pool. `acquire().await` is the cancellation point if
  the frontend shuts down. No special error variant for this case —
  the wait period is invisible to the caller; the docstring "returns
  when the client disconnects" still holds.
- **Slot died between health-check and `sendmsg`.** No special
  handling. `sendmsg` succeeds (kernel buffers); the slot's death is
  observed on the *next* handoff via `EPIPE` (per Q21).
  The current client connection is the one orphaned. `handoff()`
  returns `Ok(())`; the caller sees no error; the client sees TCP RST
  when its packets aren't ACKed.
- **`sendmsg` returns `EAGAIN`/`EINTR`.** Not surfaced to the caller.
  tokio's `AsyncFd` already handles `EAGAIN` (await fd-writable);
  `EINTR` is retried by the runtime / std::io wrappers. The
  `handoff()` future re-polls transparently; the caller never sees
  these.
- **Frontend shutdown while `handoff` is in-flight.** Standard
  cancellation pattern: the slot-acquire `await` is inside a
  `tokio::select!` against `shutdown.cancelled()`. On the shutdown
  branch, the moved-in `OwnedFd` is dropped (closing the kernel
  socket; client sees RST) and the future returns `Err(Cancelled)`.

The concrete signature stays as written in
[api.md §2](api.md#2-handoffhandle--the-v0-handle-type). Only two
error variants surface to callers: `Err(Cancelled)` (frontend
shutting down) and any I/O error that tokio surfaces from a syscall
outside the retry-able set.

**Q21. Silent handoff loss in the slot runner.** ~~Open~~ **Resolved:
option (b) — accept silent loss.** The data path is
`frontend.sendmsg(fd) → kernel buffer → backend.recvmsg(fd)`. If the
backend dies between the frontend's last observation and the
`recvmsg`, the kernel buffers the SCM_RIGHTS payload but nobody
consumes it. v0 accepts that one client connection is lost per slot
death (the client sees TCP RST); the dead slot is detected on the
*next* `sendmsg → EPIPE` and respawned then. Zero per-handoff cost.

The rejected alternative (option (a): 1-byte `[0x06]` ack on the
control socket after `recvmsg`, with a bounded timeout in `handoff()`
returning `Err(SlotDied)`) would have cost one extra syscall and a
roundtrip latency floor on every connection — a high tax for a
rare-and-operationally-obvious failure mode in a research framework.
If real usage shows slot deaths happen often enough to matter,
revisit; the implementation work is small (one syscall in the slot
runner, one `select!` arm in `handoff()`).

This is the choice that lets [Q20](#23-resolved) keep `Ok(())` from
`handoff()` meaning "kernel accepted the fd-pass" rather than
"backend has the fd in hand".

**Q22. Frontend bgworker spawn mechanism.** ~~Open~~ **Resolved:
option (a) — static FE alongside the slots in `_PG_init()`.** The
frontend bgworker is registered via `BackgroundWorkerBuilder::load()`
at postmaster start, identically to the slot bgworkers. The
operator workflow is "set `shared_preload_libraries =
'pg_transport'` and restart"; nothing else is required to get a
running frontend. Rationale:

- **Phase-1 acceptance criterion is automatically met** ("FE
  bgworker boots a tokio current-thread runtime") with no
  additional spawn-path machinery to write.
- **One spawn site, one mental model.** Both FE and slots are
  registered in `_PG_init()`; the postmaster owns restart policy
  for both via `bgw_restart_time`. No dynamic registration in v0.
- **`pg_transport.reload()` becomes unambiguous.** It re-reads
  `pg_transport.transports` from the catalog and reconciles the
  set of live listeners; it does *not* bounce the FE process.
  `start()` / `stop()` map to "enable listeners for all rows where
  `enabled = true`" / "drop all live listeners" respectively
  (drop ≠ disable: catalog state is untouched). Operationally,
  `start()` after a fresh `CREATE EXTENSION` is the first time
  any listener is bound; before that the FE is up but idle on its
  `select!` loop.
- **Cost accepted.** One extra bgworker even when no transports
  are configured. For a research framework this is in the noise;
  the operator can drop `pg_transport` from
  `shared_preload_libraries` if they want zero overhead.

Affects [backend-handoff.md §1](backend-handoff.md#1-pool-spawning-_pg_init--shared_preload_libraries)
("does not register the frontend" bullet inverts),
[architecture.md §2](architecture.md#2-runtime-integration-tokio--pgrx--pg-signals)
(callout removed), [configuration.md](configuration.md) (SQL-surface
semantics pinned). Option (b) "dynamic FE via start()" and option (c)
"pause/resume" are kept in this entry for the record; if a
later phase needs to defer FE startup (e.g. for multi-tenant
clusters where most DBs don't enable the extension), option (c) is
the natural follow-on.

**Q23. Phase-1 GUC inventory.** ~~Open~~ **Resolved: phase 1 ships
with zero new GUCs.** Heartbeat interval (1 s) and postmaster
watchdog interval (500 ms) are hard-coded constants in the frontend
bgworker; log routing uses `pgrx::log!` / `pgrx::info!` against PG's
existing `log_min_messages`. Rationale:

- **Phase-1 acceptance is operational, not configurable** —
  "heartbeat logs every 1 s, SIGHUP / SIGTERM honoured" needs no
  knob to verify.
- **GUC surface starts at phase 2** with `auth_source` +
  `backend_pool_size`, because those have no defensible default
  (auth_source is policy; pool_size depends on workload). Phase 1
  has no comparable forced choice.
- **YAGNI applies hard to research-framework knobs.** Every GUC
  is a forever-API; the cost of adding one now and changing the
  default later is higher than the cost of adding one later when
  someone actually wants a different value.

If phase-1 operational experience surfaces a need, the knob lands
in its phase — same precedent as `pg_transport.tls_min_proto`
(arrives in phase 8) and `pg_transport.metrics_port` (arrives when
the metrics endpoint does). No doc updates required by this
resolution: the [configuration.md](configuration.md) GUC list
already enumerates only phase-≥2 knobs.

**Q24. `panic = "abort"` × pgrx error propagation.** ~~Open~~
**Resolved: option (a) — switch to `panic = "unwind"`.** The Q9
resolution (`panic = "abort"`) was incompatible with pgrx 0.18's
error machinery on two paths:

- `pgrx::error!()` / `ereport!(ERROR, …)` raise via
  `std::panic::panic_any(report)` (see
  `pgrx-pg-sys-0.18.0/src/submodules/panic.rs:158`). Under
  `panic = "abort"` this SIGABRTs instead of emitting a readable
  error. **Workaround applied in phase 1**: use
  `ereport!(FATAL, …)` for boundary errors (FATAL routes through
  `do_ereport()` → `proc_exit(1)` directly, no Rust panic
  involved). Now unnecessary, but kept where FATAL is the right
  semantic.
- When PG raises an ERROR from inside a pgrx-mediated call (e.g.
  SPI), pgrx's `cee-scape` wrapper catches the longjmp and
  re-raises as a Rust panic so `PgTryBuilder` / `catch_unwind` can
  inspect it. Under `panic = "abort"` the catch never happens —
  *any* PG ERROR aborts the bgworker. **Fatal for the phase-4 wire
  bridge**, which needs to convert user SQL errors into wire
  `ErrorResponse` frames.

Option (a) (`unwind`) was the only one that keeps pgrx working as
documented. Options (b) (raw `cee_scape::call_with_setjmp` per
SPI call) and (c) (let SPI errors kill the slot, accept
wire-protocol incorrectness) were considered and rejected; the
phase-1 commit message captures the survey.

The cost we accept: `catch_unwind` / `AssertUnwindSafe` boilerplate
at boundaries where we deliberately want to capture (rather than
propagate) a panic. Q9's original rationale arguments for `abort`
are preserved verbatim in the Q9 entry above as the auditable
record; they were correct in isolation but missed the pgrx-uses-
panic invariant.

Affects [Q9](#23-resolved) (re-resolved as "unwind"), workspace
[Cargo.toml](../../../Cargo.toml) `[profile.dev]` / `[profile.release]`
(now `panic = "unwind"`), and the phase-1/2/3 code comments that
mentioned `abort` (updated in the same commit as this resolution).

---

## 3. Risks & mitigations

Deferred-only risks (DPDK operational pain, polled-bridge cross-thread
soundness, `quinn`/`tokio-uring` ABI churn during the deferral, RDMA hardware
availability) live in
[../future-transports.md §6](deferred/future-transports.md).

| Risk                                                              | Likelihood | Mitigation                                                                    |
| ----------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------- |
| tokio current-thread runtime × pgrx bgworker interaction unproven | High       | Phase-1 spike validates signals, latch, postmaster-death; ADR before phase 2  |
| `SCM_RIGHTS` semantics across PG versions / OSes                  | Medium     | Wrap behind a compat shim in `backend` crate; Unix-only in v0                |
| Tokio task panics tear down the runtime                           | N/A        | Workspace sets `panic = "unwind"` (Q9 re-resolved via Q24). A panic in a tokio task unwinds and `LocalSet` / `JoinHandle` surfaces the error; the slot loop logs and exits, the postmaster respawns. Same blast radius as PG `ereport(FATAL)` for unhandled cases. |
| Bench harness becomes its own quagmire                            | Medium     | Use `criterion` for in-process, plain `tokio-postgres` / raw clients for end-to-end |
| Build matrix explosion across feature combinations                | N/A        | v0 has no Cargo features; only one transport (`tcp_handoff`) is compiled in. Re-evaluate when a second transport lands. |
| Deferred shm_mq path drifts out of sync with v0 changes           | Low        | Treat [backend-pool.md](deferred/backend-pool.md) as a versioned design draft; touch it whenever v0 changes invalidate an assumption |

---

## 4. Deferred for v0

Things that are designed but **not built in v0**, preserved here so they
can land later without re-deriving the design. (Distinct from §5
*Rejected alternatives* below: those are things we've decided not to do
at all.)

### 4.1 `SessionTransport` + `SessionHandle` (the shm_mq general path)

**Scope.** The second transport trait, its handle type, the `Payload`
enum, `ExecutorSession`, `FrameStream`, frame tagging with `conn_id`,
the DSM + `shm_mq` + `pq_redirect_to_shm_mq` plumbing, the cross-process
wakeup options A/B/C, and shared-slot mode for stateless callers.

**Why deferred.**

- **v0 doesn't need it.** The v0 transport (`tcp_handoff`) speaks FE/BE
  on a kernel fd, which is exactly the handoff path's sweet spot.
- **Smaller surface to validate first.** Phase 1 already needs to
  validate that tokio current-thread + pgrx bgworker + PG signal
  handling co-exist correctly; piling `shm_mq` framing, frame tagging,
  and cross-process wakeup on top would multiply the things that can
  go wrong on the integration spike.
- **Bench numbers come first.** The whole project's point is comparable
  benchmark numbers. The handoff path is the baseline; without it
  shipping and bench'd, the shm_mq path has nothing to compare against.
- **The PG-side mechanism is borrowed, not invented.** `pg_background`'s
  DSM + `shm_mq` + `pq_redirect_to_shm_mq` machinery is well-understood
  and known to work; deferring it doesn't carry the design risk that
  novel mechanisms do.

**Where the design lives.**

- API surface (trait, handle, `Payload`): [api.md §6](api.md).
- Mechanism (DSM, `shm_mq`, envelope, frame demux, wakeup, slot model,
  shutdown, errors): [backend-pool.md](deferred/backend-pool.md) (entire doc;
  banner at top marks it deferred).
- Where it shows up in cross-references: every "deferred" / "shm_mq path"
  pointer across the design docs.

**Re-entry conditions.**

- v0 (phases 0–6.5) is shipped and producing stable benchmark numbers.
- A real use case exists for a non-FE/BE wire (HTTP/2 + SQL, custom
  binary) or for transport-side FE/BE inspection.
- The handoff-path baseline is fast and stable enough to be the
  comparator for shm_mq numbers.

When those conditions hold, phase 7 (`transport-http2-sql` as a
`SessionTransport`) reopens with the design in [backend-pool.md](deferred/backend-pool.md)
as its starting point. The trait surface in [api.md §6](api.md) is
intended to be additive: existing `HandoffTransport`s remain unchanged.

---

## 5. Rejected alternatives

Designs we *considered* and chose not to pursue. Recorded here so the
question doesn't keep coming back. (Distinct from §4 *Deferred for v0*:
those are things we intend to build later.)

### 5.1 `SO_REUSEPORT` — executors accept their own connections

**Idea.** Instead of the frontend being the single accepter that hands
fds off to executors via `SCM_RIGHTS`, each backend binds its own
listening socket on the same port with `SO_REUSEPORT`. The kernel hashes
incoming 4-tuples across the listening processes, so each backend calls
its own `accept()` and is the sole owner of the resulting fd. This would
eliminate the ~10 µs handoff cost ([frontend-handoff.md §7](frontend-handoff.md)) and the
frontend's role would shrink to lifecycle + configuration.

**Why rejected.**

- **Backends become async listeners.** Today the backend is a slot
  runner ([backend-handoff.md](backend-handoff.md)) that polls the
  per-slot UDS for handoffs and drives a wire layer
  ([backend-wire.md](backend-wire.md)) on each received fd. With
  `SO_REUSEPORT` each backend would need its own accept loop, signal
  handling, postmaster-death watchdog — duplicating tokio-runtime
  machinery that C-3 deliberately keeps in one place. The
  simplification of "all listener-side async I/O is in the frontend"
  goes away.
- **Central control is lost.** Pre-handoff filtering (IP allowlist, TLS
  SNI, rate limiting, per-tenant policy) only makes sense if there's a
  single accept point. With `SO_REUSEPORT`, those would have to be
  replicated per backend or moved to in-kernel BPF — both significant
  scope expansions.
- **Kernel hash, not framework choice.** `SO_REUSEPORT` distributes
  connections by `hash(src_ip, src_port, dst_ip, dst_port)`. The
  framework gives up the ability to say "send this connection to a
  specific slot" (e.g. for affinity, capacity-aware routing, custom load
  balancing). The handoff path retains that control because the
  frontend picks the slot explicitly.
- **Crashed-backend traffic blackholing.** When a backend dies, the
  kernel keeps hashing 1/N of incoming SYNs to its now-stale socket
  queue until it's respawned. With the frontend-as-accepter model,
  the frontend just stops picking the dead slot.
- **Cross-platform behaviour.** `SO_REUSEPORT` semantics differ between
  Linux (load-balancing), BSD (last-bind-wins or no load-balancing
  depending on variant), and Windows (no equivalent). Unix-first is
  fine for the framework, but `SO_REUSEPORT`-dependent behaviour is
  more Linux-coupled than the rest of the design.
- **The win is small in absolute terms.** Handoff already runs within
  ~10 µs of default PG. For any non-trivial query that overhead is
  noise. The complexity-for-perf trade-off doesn't pencil out at this
  stage of the project.

**What was removed when this was rejected.**

- `pg_transport.frontend_workers` GUC (its only purpose was scaling
  the accept side via `SO_REUSEPORT`).
- "Future work" pointer in [frontend-handoff.md §7](frontend-handoff.md).

**Conditions under which we'd revisit.** Benchmarks (phase 4+) show
handoff is a measurable production bottleneck *and* a clear use case
needs sub-10-µs connection establishment that pgbouncer-style proxies
can't satisfy. Until then: single frontend, pre-spawned backend pool,
`SCM_RIGHTS` handoff is the design.

---

## 6. References

- pgrx: <https://github.com/pgcentralfoundation/pgrx>
  - bgworker example: `pgrx-examples/bgworker/`
  - `BackgroundWorkerBuilder` API docs
- tokio: <https://tokio.rs/>
  - current-thread runtime: <https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.new_current_thread>
  - `LocalSet`: <https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html>
  - `AsyncFd` (wrap arbitrary `RawFd`s for the reactor): <https://docs.rs/tokio/latest/tokio/io/unix/struct.AsyncFd.html>
  - `tokio::signal::unix`: <https://docs.rs/tokio/latest/tokio/signal/unix/index.html>
  - `tokio_util::sync::CancellationToken`: <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html>
- pgwire (Rust FE/BE v3): <https://github.com/sunng87/pgwire>
- rust-postgres family (used for the bench harness): <https://github.com/sfackler/rust-postgres>
- tokio-rustls: <https://github.com/rustls/tokio-rustls>
- Companion docs:
  - [../future-transports.md](deferred/future-transports.md) — deferred transports (QUIC, io_uring, AF_XDP, DPDK, RDMA, shmem-loopback) and the polled-transport bridge pattern
  - [../background/pg_background.md](../background/pg_background.md) — DSM + `shm_mq` + `pq_redirect_to_shm_mq` mechanics we lift wholesale
  - [../background/omnigres.md](../background/omnigres.md) — listener-bgworker pattern + pool architecture we mirror
