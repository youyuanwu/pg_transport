# Roadmap — plan, open questions, risks, references

> Parent: [README.md](README.md)

## 1. Phased build plan

Strict ordering — each phase locks in a contract that subsequent phases
extend rather than break. **v0 ends at phase 6.5.** Phase 7
(`SessionTransport` + shm_mq general path) is **deferred**; see
[§4 Deferred for v0](#4-deferred-for-v0). Other deferred transport
phases are listed in [../future-transports.md §4](../future-transports.md).

| Phase | Deliverable                                                   | Gate / done criterion                                                |
| ----- | ------------------------------------------------------------- | -------------------------------------------------------------------- |
| **0** | Repo scaffold, ADRs, this design doc set                      | `cargo check -p api -p core` passes                                  |
| **1** | `core` bgworker boots a tokio current-thread runtime          | Heartbeat task logs every 1 s; clean SIGHUP/SIGTERM via `tokio::signal`; postmaster-death watchdog exits the runtime |
| **2** | `executor` pool — pre-spawned bgworkers, per-slot UDS control socket | Pool starts N executor bgworkers; dispatcher can `sendmsg(SCM_RIGHTS)` a dummy fd to a slot; slot reads and closes it; no shm_mq machinery yet |
| **3** | `transport-tcp-handoff` as a `HandoffTransport` + executor runs `PostgresMain`-equivalent | `psql -h … -p …` connects, runs `SELECT 1` (no TLS yet) |
| **4** | `bench/` harness                                              | Reports p50/p95/p99 latency, throughput; produces a CSV per run |
| **5** | `transport-uds-handoff` as a `HandoffTransport`                | Adds a second transport; flushes out the SCM_RIGHTS control-socket plumbing |
| **6** | TLS in the executor via PG's `secure_open_server`             | `psql sslmode=require` connects via TCP and UDS; transports unchanged |
| **6.5** | (Optional) rustls sidecar TLS in the executor                | Same `psql sslmode=require` flow; demonstrates `tls_impl="rustls"` GUC |
| ~~7~~ | ~~`transport-http2-sql` as a `SessionTransport`~~             | **DEFERRED** — see [§4 Deferred for v0](#4-deferred-for-v0) |

Phase 1 is, in practice, **the** integration spike: validating that a tokio
current-thread runtime cohabits cleanly with a pgrx bgworker, that signal
handling does the right thing under both PG's and tokio's handlers, and
that postmaster-death is observed promptly. Anything weird discovered here
turns into an ADR before phase 2 begins.

Bench harness arrives **before** transport #2 deliberately — without it,
later "did this help?" questions are unanswerable.

Deferred transports (io_uring, QUIC, AF_XDP, DPDK, RDMA, shmem-loopback)
resume at "phase F1" in [../future-transports.md §4](../future-transports.md),
gated on a stable in-scope baseline producing comparable benchmark numbers.
The deferred `SessionTransport` path (phase 7 above) is a *prerequisite*
for several of those (HTTP/2 + SQL, datagram QUIC, DPDK), so its
un-deferral is on the critical path for the wider transport roadmap.

---

## 2. Open questions

These need ADRs before we hit the relevant phase. Not blockers for phase 0–1.
Questions tagged **[deferred]** apply only to the deferred shm_mq general
path (see [§4](#4-deferred-for-v0)); they will need ADRs when that path
is un-deferred, not before. Deferred-only open questions for exotic
transports (0-RTT QUIC, connection migration, RDMA protocol pairing, DPDK
CPU budget) live in
[../future-transports.md §5](../future-transports.md).

1. **Auth handshake location.** ~~Open~~ **Resolved.** v0 handoff-path
   transports delegate auth entirely to the executor, which runs PG's
   own `ClientAuthentication` (uses `pg_hba.conf` as normal). See
   [handoff.md §3](handoff.md). When the deferred shm_mq path lands,
   FE/BE-aware transports on it will need to run auth themselves before
   calling `acquire`; a thin `protocol-pgwire-auth` helper crate
   wrapping SCRAM is a likely addition then.
2. **Per-session executor pinning vs. session migration.** For FE/BE v3 the
   natural model is pinned (session state lives in the executor). What's
   the failure model when an executor dies mid-session? Replay vs. drop the
   connection — we lean drop, but document it. For the v0 handoff path
   this is enforced by construction (the slot owns the fd). For the
   deferred shm_mq path the same policy is the default; see
   [executor-pool.md §7](executor-pool.md#7-slot-allocation-pinning-and-lifetime).
3. **TLS termination location.** ~~Open~~ **Resolved.** v0 handoff-path
   transports terminate TLS in the executor (PG's `secure_open_server`
   by default, optional rustls sidecar — see Q12). The deferred shm_mq
   path would have its transports terminate TLS themselves via the
   (also deferred) `tls-rustls` helper.
4. **`shared_preload_libraries` requirement.** `pg_background` deliberately
   makes SPL optional. We need it for the executor pool to be pre-spawned at
   postmaster start. Trade-off: simpler config (SPL required) vs. broader
   compatibility (lazy pool spawn on first transport start).
5. **Cargo feature defaults.** What's in `default-features`? Lean *minimal*
   — `["tcp-handoff", "uds-handoff"]` only — so binary distributions don't
   silently pull in optional deps users don't need.
6. **Cross-process wakeup model** **[deferred]**. Option A (interval poll),
   Option B (eventfd bridge), or Option C (PG-latch bridge thread). Only
   relevant to the deferred shm_mq path — the v0 handoff path needs no
   cross-process wakeup once the fd is handed off. See
   [executor-pool.md §6](executor-pool.md#6-cross-process-wakeup-how-the-dispatcher-knows-theres-data).
7. **Latch bridge fidelity (if we adopt Option C).** pgrx's
   `attach_signal_handlers` sets PG-side flags (`ConfigReloadPending`,
   `ShutdownRequestPending`). We additionally take `SIGHUP` / `SIGTERM` via
   `tokio::signal::unix::signal`. Do we ever need to *also* poll those
   flags from a tokio interval task, or is reacting to the signal
   sufficient? Probably the latter, but verify.
8. **`async_trait` lifetime.** We start with the `async_trait` macro on
   `HandoffTransport` for ergonomics + object-safety. When do we migrate
   to native `async fn in traits` (Rust 1.75+) — once `dyn` support
   stabilises sufficiently for our `Box<dyn HandoffTransport>` use, or
   never? (Same call will apply to the deferred `SessionTransport`.)
9. **Panic isolation in tokio tasks.** A panic in a `spawn_local`ed
   per-connection task by default unwinds the runtime. Do we wrap every
   spawn with `AssertUnwindSafe` + `catch_unwind`, or rely on panic=abort
   (consistent with PG's posture)? Lean wrap-and-log.
10. **Shared slots for stateless protocols** **[deferred]**. Phase 2 pins
    one connection per slot exclusively, which is correct for the
    handoff path. A future phase, on top of the deferred shm_mq path,
    can let stateless transports (HTTP/2 SQL, datagram protocols) share
    a slot with `conn_id`-based multiplexing and explicit per-request
    session reset. See
    [executor-pool.md §7](executor-pool.md#7-slot-allocation-pinning-and-lifetime).
11. **`Payload` variant set** **[deferred]**. Today (in the deferred design):
    `Raw`, `Sql`, `Extended`. Do we need more (`CopyIn` / `CopyOut`
    streams, `Notify` subscribe, cursor fetch)? Add as needed when the
    shm_mq path lands; `Payload` is `#[non_exhaustive]` so it's a
    non-breaking change.
12. **TLS implementation in the executor (handoff path).** Default to PG's
    `secure_open_server` (OpenSSL) or to a rustls sidecar thread? OpenSSL
    is zero-extra-code and reuses PG's `ssl_*` GUCs and `cert` auth
    method; rustls is memory-safe, modern, decoupled from system OpenSSL,
    and a research-interesting alternative. Lean *OpenSSL default,
    rustls opt-in via `pg_transport.tls_impl` GUC*. See
    [handoff.md §4](handoff.md).
13. **Per-listener TLS variation.** Handoff path currently uses cluster-
    wide PG `ssl_*` GUCs (one cert for the whole cluster). If a real
    deployment needs per-listener certs, add a `cert_path` to
    `HandoffHints` and let the executor resolve it per-connection. Phase
    6 punts; reopen if asked.
14. **Should we add a third trait + `QueryHandle` for stateless-only?**
    **[deferred]**. Pre-empts a slicing decision *within* the deferred
    `SessionHandle`: `execute` (stateless) vs `acquire` (stateful) live
    on the same handle, but a transport that never calls `acquire` is
    *in practice* a query transport that the type doesn't enforce.
    Adding `QueryTransport` + `QueryHandle` would enforce statelessness
    at the type level. Lean **no** — the meaningful boundary is
    handoff-vs-session; over-splitting forces breaking changes when a
    transport later wants `BEGIN`/`COMMIT` continuity. Revisit when the
    shm_mq path lands.
15. **Live pool resize.** `pg_transport.executor_pool_size` is read once
    at dispatcher startup; phase 2 requires `stop()` / `start()` (or a
    postmaster restart) to change it. Should `pg_transport.reload()` be
    able to grow and/or shrink the pool without a dispatcher restart?
    Lean **yes for grow, no for shrink** at first:
    - *Grow* is cheap and safe — call `RegisterDynamicBackgroundWorker`
      for the additional slots, allocate their Unix control sockets,
      plumb them into the semaphore. No in-flight work is affected.
    - *Shrink* needs a drain protocol — mark slots "no new handoffs",
      wait for current handoffs to finish (potentially unbounded), then
      tell the bgworker to exit cleanly. Avoidable for now; if a real
      deployment needs it, it's a follow-up ADR.

    Decision wanted **before** phase 4 (the bench harness) so we can
    benchmark grow-during-load. Until then, sizing is set-once at
    `start()`.

---

## 3. Risks & mitigations

Deferred-only risks (DPDK operational pain, polled-bridge cross-thread
soundness, `quinn`/`tokio-uring` ABI churn during the deferral, RDMA hardware
availability) live in
[../future-transports.md §6](../future-transports.md).

| Risk                                                              | Likelihood | Mitigation                                                                    |
| ----------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------- |
| tokio current-thread runtime × pgrx bgworker interaction unproven | High       | Phase-1 spike validates signals, latch, postmaster-death; ADR before phase 2  |
| `SCM_RIGHTS` semantics across PG versions / OSes                  | Medium     | Wrap behind a compat shim in `executor` crate; Unix-only in v0                |
| Tokio task panics tear down the runtime                           | Medium     | Wrap every `spawn_local` in `AssertUnwindSafe` + `catch_unwind`; log + close  |
| Bench harness becomes its own quagmire                            | Medium     | Use `criterion` for in-process, plain `tokio-postgres` / raw clients for end-to-end |
| Build matrix explosion across feature combinations                | Medium     | CI builds `default`, `all-transports`, and the minimal feature set            |
| Deferred shm_mq path drifts out of sync with v0 changes           | Low        | Treat [executor-pool.md](executor-pool.md) as a versioned design draft; touch it whenever v0 changes invalidate an assumption |

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

- **v0 doesn't need it.** Every v0 transport (`tcp_handoff`, `uds_handoff`)
  speaks FE/BE on a kernel fd, which is exactly the handoff path's
  sweet spot.
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
  shutdown, errors): [executor-pool.md](executor-pool.md) (entire doc;
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
`SessionTransport`) reopens with the design in [executor-pool.md](executor-pool.md)
as its starting point. The trait surface in [api.md §6](api.md) is
intended to be additive: existing `HandoffTransport`s remain unchanged.

---

## 5. Rejected alternatives

Designs we *considered* and chose not to pursue. Recorded here so the
question doesn't keep coming back. (Distinct from §4 *Deferred for v0*:
those are things we intend to build later.)

### 5.1 `SO_REUSEPORT` — executors accept their own connections

**Idea.** Instead of the dispatcher being the single accepter that hands
fds off to executors via `SCM_RIGHTS`, each executor binds its own
listening socket on the same port with `SO_REUSEPORT`. The kernel hashes
incoming 4-tuples across the listening processes, so each executor calls
its own `accept()` and is the sole owner of the resulting fd. This would
eliminate the ~10 µs handoff cost ([handoff.md §7](handoff.md)) and the
dispatcher's role would shrink to lifecycle + configuration.

**Why rejected.**

- **Executors become async listeners.** Today the executor is sync `pgrx`
  C-side code that polls the per-slot UDS for handoffs and runs PG's
  `PostgresMain`-equivalent on demand. With `SO_REUSEPORT` each executor
  would need its own accept loop, signal handling, postmaster-death
  watchdog — duplicating tokio-runtime machinery that C-3 deliberately
  keeps in one place. The simplification of "all PG-touching code is in
  one sync path per executor, all async I/O is in the dispatcher" goes
  away.
- **Central control is lost.** Pre-handoff filtering (IP allowlist, TLS
  SNI, rate limiting, per-tenant policy) only makes sense if there's a
  single accept point. With `SO_REUSEPORT`, those would have to be
  replicated per executor or moved to in-kernel BPF — both significant
  scope expansions.
- **Kernel hash, not framework choice.** `SO_REUSEPORT` distributes
  connections by `hash(src_ip, src_port, dst_ip, dst_port)`. The
  framework gives up the ability to say "send this connection to a
  specific slot" (e.g. for affinity, capacity-aware routing, custom load
  balancing). The handoff path retains that control because the
  dispatcher picks the slot explicitly.
- **Crashed-executor traffic blackholing.** When an executor dies, the
  kernel keeps hashing 1/N of incoming SYNs to its now-stale socket
  queue until it's respawned. With the dispatcher-as-accepter model,
  the dispatcher just stops picking the dead slot.
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

- `pg_transport.dispatcher_workers` GUC (its only purpose was scaling
  the accept side via `SO_REUSEPORT`).
- "Future work" pointer in [handoff.md §7](handoff.md).

**Conditions under which we'd revisit.** Benchmarks (phase 4+) show
handoff is a measurable production bottleneck *and* a clear use case
needs sub-10-µs connection establishment that pgbouncer-style proxies
can't satisfy. Until then: single dispatcher, pre-spawned executor pool,
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
- `async-trait` crate: <https://github.com/dtolnay/async-trait>
- pgwire (Rust FE/BE v3): <https://github.com/sunng87/pgwire>
- rust-postgres family (used for the bench harness): <https://github.com/sfackler/rust-postgres>
- tokio-rustls: <https://github.com/rustls/tokio-rustls>
- Companion docs:
  - [../future-transports.md](../future-transports.md) — deferred transports (QUIC, io_uring, AF_XDP, DPDK, RDMA, shmem-loopback) and the polled-transport bridge pattern
  - [../background/pg_background.md](../background/pg_background.md) — DSM + `shm_mq` + `pq_redirect_to_shm_mq` mechanics we lift wholesale
  - [../background/omnigres.md](../background/omnigres.md) — listener-bgworker pattern + pool architecture we mirror
