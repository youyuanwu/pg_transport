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
| **2** | `backend` pool slot runner — demand-driven autoscaling, single FE UDS listener | FE binds one UDS listener; slot bgworkers spawn dynamically via `pool::grow_one`; pool grows on demand and shrinks on idle under `pg_transport.max_backend_pool_size` ceiling; cooperative drain via `b'X'`/`b'A'`. See [pool.md](pool.md). |
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

*(None.)* All v0-path open questions have been resolved; see
[§2.3](#23-resolved). New v0-path questions land here as they're
identified.

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

**Q15. ~~Live pool resize.~~** **Resolved** — see
[§2.3 Q15](#23-resolved).

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

Decisions made. Each entry is the headline + pointers to the live
code / authoritative design doc. Full deliberation lives in git
history (the original verbose rationale was compressed in 2026-05;
see commit log for the survey / rejected alternatives).

**Q1. Auth handshake location** — wire-layer in Rust; call
`hba_getauthmethod` for the `(method, options)` tuple, implement the
method ourselves. **Not** PG's `ClientAuthentication`. Methods in v0:
trust / reject / password / md5 / scram-sha-256. See
[backend-wire.md §4](backend-wire.md) and
[crates/core/src/wire/auth/](../../crates/core/src/wire/auth/).

**Q2. Per-session backend pinning** — forced by architecture: handoff
moves the `OwnedFd` into the slot via `SCM_RIGHTS`; migration between
processes is impossible. Slot pinned for connection lifetime. Failure
model: dropped client connection + slot respawn for uncaught panics
(Q9 + Q20 + Q21); clean wire `ErrorResponse` for caught SPI errors.

**Q3. TLS termination location** — wire layer, **not** PG's
`secure_open_server`. (TLS lib subsequently re-litigated as rustls in
[Q26](#23-resolved).) See [backend-wire.md §5](backend-wire.md).

**Q4. `shared_preload_libraries` requirement** — SPL is **required**.
`_PG_init()` checks `process_shared_preload_libraries_in_progress`
and FATALs otherwise (`crates/core/src/lib.rs`). Trade: operator
config burden vs. first-connection latency excluding pool spawn +
postmaster-owned restart policy. Live pool resize deferred to Q15.

**Q5. Cargo feature defaults** — none in v0 beyond pgrx's `pg18` +
`pg_test`. Reopens when a second transport lands; for now
`tcp_handoff` is compiled in unconditionally. See
[workspace.md §2](workspace.md#2-cargo-features--deliberately-minimal).

**Q8. `async_trait` lifetime** — **no `async_trait`, ever.**
`HandoffTransport::run` returns a manual `RunFuture =
Pin<Box<dyn Future + 'static>>`; impls write `Box::pin(async move {
… })`. Reasons in [api.md §1](api.md#1-the-handofftransport-trait).

**Q9. Panic isolation in tokio tasks** — ~~`abort`~~ → **`unwind`**,
re-resolved via [Q24](#23-resolved). pgrx 0.18 uses Rust panics as
its ERROR-propagation mechanism, so `abort` SIGABRTs instead of
emitting readable errors. Workspace `[profile.dev]` and
`[profile.release]` both set `panic = "unwind"`. Cost: `catch_unwind`
/ `AssertUnwindSafe` boilerplate where we deliberately want to
capture a panic.

**Q12. TLS implementation** — ~~rust-openssl~~ → **rustls**,
re-resolved via [Q26](#23-resolved) (pgwire 0.40 hard-binds to
`tokio_rustls`).

**Q13. Per-listener TLS variation** — one cert per cluster in v0,
from `pg_transport.tls_{cert,key}_file` GUCs. Extension point
(`cert_id` on `HandoffHints`) pre-allocated but not built; un-defer
when a real deployment needs distinct certs per listener address.

**Q15. Live pool resize** — implemented as **demand-driven
autoscaling under a single ceiling GUC** rather than the originally
sketched grow-via-`reload()` path. `pg_transport.max_backend_pool_size`
(`SUSET`, default 64) is the only operator-visible knob; the pool
grows on demand when a handoff arrives with no ready slot, shrinks
via an idle reaper (`IDLE_REAP_AFTER = 60s`), and reconciles to a
lower ceiling via the SIGHUP handler (drain idle first, leave
`in_flight` to finish naturally). Cooperative drain protocol
(`b'X'` request / `b'A'` ack) with a `DRAIN_ACK_TIMEOUT` (60 s)
watchdog handles slots stuck in long-running queries by falling
back to `shutdown(SHUT_WR)`. Full design and race-freedom analysis
live in [pool.md](pool.md); implementation across
[crates/core/src/backend/pool.rs](../../crates/core/src/backend/pool.rs)
and siblings.

**Q17. Extended-query state ownership** — option (a): wire layer owns
the name maps (`HashMap<String, SpiPlan>` and `HashMap<String,
BoundPortal>`); SPI owns the underlying plans. See
[crates/core/src/backend/extended.rs](../../crates/core/src/backend/extended.rs).
Per-handoff reset bookkeeping lands as a phase-9 spec item in
[backend-wire.md §8 Q1](backend-wire.md#8-open-questions).

**Q18. SPI vs. planner+executor direct path** — v0 uses SPI
exclusively (`SPI_execute` for simple, `SPI_prepare` +
`SPI_execute_plan_with_params` for extended). The direct-path option
survey (which `exec_*` dispatchers are `static`, which lower-level
symbols are exported, three reuse strategies) lives in
[deferred/planner-executor-direct-path.md](deferred/planner-executor-direct-path.md);
performance impact analysed in
[performance.md §3.1](performance.md). Un-defer if bench numbers
show SPI overhead is material for typical workloads.

**Q20. `HandoffHandle::handoff()` error contract** — simplest answer
in each case. Pool exhausted: block on a semaphore (no error
variant). Slot died mid-`sendmsg`: invisible to caller, observed as
`EPIPE` on the next handoff (per Q21). `EAGAIN`/`EINTR`: tokio retry,
caller never sees. Frontend shutdown mid-handoff: `Err(Cancelled)`,
fd dropped (client RST). Surface: only `Err(Cancelled)` and raw I/O
errors. See [api.md §2](api.md#2-handoffhandle--the-v0-handle-type).

**Q21. Silent handoff loss in the slot runner** — option (b), accept
silent loss. If the slot dies between `sendmsg` and `recvmsg`, the
kernel buffers the fd, nobody consumes it, the client sees TCP RST,
the dead slot is detected on the next `sendmsg → EPIPE`. No
per-handoff ack syscall. Lets Q20 keep `handoff()` returning `Ok(())`
to mean "kernel accepted the fd-pass".

**Q22. Frontend bgworker spawn mechanism** — option (a): static FE
alongside the slots in `_PG_init()`. Operator workflow is "set SPL
and restart". `pg_transport.reload()` (planned, see
[configuration.md §1.3](configuration.md#13-planned-catalog-surface))
reconciles listeners against the catalog; it does not bounce the FE.

**Q23. Phase-1 GUC inventory** — zero new GUCs in phase 1. Heartbeat
+ watchdog intervals are hard-coded constants. GUC surface starts at
phase 2 (`max_backend_pool_size`) and grows as each phase adds knobs
without defensible defaults. See
[configuration.md §1.1](configuration.md#11-gucs-implemented).

**Q24. `panic = "abort"` × pgrx error propagation** — option (a):
switch to `panic = "unwind"`. Drove the [Q9](#23-resolved)
re-resolution. Cause: pgrx's `ereport!(ERROR, …)` raises via
`std::panic::panic_any(report)`, and SPI ERROR longjmps are caught
by pgrx's `cee-scape` wrapper and re-raised as Rust panics — neither
works under `abort`. Workspace `Cargo.toml` profiles updated.

**Q25. SQL parsing in the wire layer** — call PG's in-process
`raw_parser` via `pgrx::pg_sys`. Tried (a) `libpg_query`
(static-TLS allocation failure under `dlopen`), (b) `sqlparser-rs`
(~15 µs/query, ~5% regression). (c) `raw_parser` adopted: ~5 µs,
perfect grammar fidelity, zero new deps. Returns `RawStmt` list with
`stmt_location` + `stmt_len` spans for SPI hand-off and node-tag
classification for xact-control detection. See
[crates/core/src/backend/spi_bridge.rs](../../crates/core/src/backend/spi_bridge.rs)
(`parse_and_classify`).

**Q26. TLS library — rust-openssl vs rustls** — **rustls** (via
`tokio_rustls` + ring), re-litigating Q12. Forced by pgwire 0.40
hard-binding its TLS plumbing to `tokio_rustls`. Cost: TLS material
in our own GUCs rather than reusing the cluster's `ssl_*`; FIPS path
(aws-lc-rs) available later if needed. See
[crates/core/src/wire/tls.rs](../../crates/core/src/wire/tls.rs) +
[backend-wire.md §5](backend-wire.md).

**Q27. Extended-query parameter-type inference** — two-pass at Parse
time. Run `pg_parse_query` + `pg_analyze_and_rewrite_varparams` to
extract resolved OIDs (copy into a Rust `Vec` before the analyser's
memory context is freed), upgrade remaining `UNKNOWNOID` to
`TEXTOID`, then call `SPI_prepare` with the resolved fixed types.
Rejected (a) `SPI_prepare_params` + parserSetup callback (analyser
clobbers the OID array) and (c) hand-rolled `CachedPlanSource` (more
PG-internals surface for no v0 benefit). Result-format handling also
shipped: `OidOutputFunctionCall` for text, `OidSendFunctionCall` for
binary. See
[crates/core/src/backend/extended.rs](../../crates/core/src/backend/extended.rs)
+ [crates/core/src/wire/extended.rs](../../crates/core/src/wire/extended.rs).

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
