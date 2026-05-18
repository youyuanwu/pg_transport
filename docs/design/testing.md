# Testing strategy

> Parent: [README.md](README.md)
> Sibling: [roadmap.md](roadmap.md) · [architecture.md](architecture.md)

What the v0 design assumes about testing, what test seams need to
exist for it to be testable, and the per-phase acceptance criteria
that gate each milestone.

The roadmap's [phase 5](roadmap.md#1-phased-build-plan) is the *bench*
harness — performance. This doc covers **correctness** testing, which
runs alongside every phase and must be in place before the phase is
called done.

---

## 1. Subsystem risk register

Per-subsystem highest-risk scenarios. The phase tests against each
subsystem MUST exercise each scenario explicitly; absence of a test
for a listed scenario blocks the phase's completion.

| Subsystem                            | Highest-risk scenarios                                                              |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| Frontend bgworker                    | Signal handler coexistence (pgrx flags ↔ `tokio::signal`); postmaster-death watchdog fires; clean shutdown drains in-flight `handoff()` futures |
| Pool / handoff seam                  | Pool-exhaustion blocking + cancellation; slot crash → frontend respawn (Q21 path); concurrent handoffs to the same slot are serialised |
| Slot runner (backend-handoff.md)     | Per-handoff state reset isolates session N+1 from session N; SIGKILLed slot does not corrupt frontend pool state; recvmsg cleanly errors when fd-pass is empty |
| Wire layer — startup negotiation     | `SSLRequest` accept/decline; `CancelRequest` drop (v0); unsupported protocol versions emit clean `ErrorResponse` |
| Wire layer — auth                    | Each method (`trust`, `reject`, `password`, `md5`, `scram-sha-256`) succeeds with correct creds and fails with `28P01` for wrong creds; method-not-in-v0 emits clear rejection; `auth_source = 'pg_hba'` and `auth_source = 'pg_transport'` give consistent results for equivalent rules |
| Wire layer — TLS                     | `sslmode=require` over TCP and UDS; cert chain validation; min-protocol enforcement (`TLSv1.2`); TLS errors don't leak fd to next handoff |
| SPI bridge                           | `SELECT 1` round-trip; multi-row result materialisation; SPI error → `ErrorResponse` with correct SQLSTATE; uncaught SPI panic exits slot cleanly (Q9 re-resolved as unwind via Q24) |
| Extended-query (phase 9)             | Per-handoff reset checklist from [backend-wire.md §8 Q1](backend-wire.md#8-open-questions): named prepared statement is cleared between handoffs; bound portal is dropped; `SET tz` does not survive |

The three scenarios that the [SoT review](reviews/2026-05-17-base-design/REVIEW-SYNTHESIS.md)
flagged as the framework's biggest correctness risks live in this
table: **per-handoff reset isolation**, **signal handler coexistence**,
and **slot crash respawn**.

## 2. Test seams the design must provide

Test seams are the boundaries at which we can substitute a mock or
fault-injection adapter for a real dependency. The v0 traits and
internal structs MUST expose the following seams; if they don't, the
test scenarios above are forced into integration shape (slow, brittle,
require real PG).

| Seam                                | What it abstracts                                                                              |
| ----------------------------------- | ---------------------------------------------------------------------------------------------- |
| `HandoffHandle`                      | Real backend pool ↔ in-memory mock pool that records calls. Lets transports be tested without spinning up bgworkers. |
| Per-slot control socket              | Real `AF_UNIX` SOCK_STREAM ↔ in-process `socketpair()`. Lets the slot runner be tested without a frontend. |
| `Wire` trait input                   | Real client `OwnedFd` ↔ `socketpair()`-derived fd loaded with a recorded byte stream. Lets the wire layer be tested without a real client. |
| `WireCtx` SPI bridge handle          | Real SPI ↔ canned-result trait implementation. Lets wire-layer protocol logic be tested without PG SPI. |
| `WireCtx` HBA lookup handle          | Real `hba_getauthmethod` ↔ in-memory rule table. Lets auth-flow tests run without `pg_hba.conf`. |
| `ShutdownToken`                      | Already mockable (`tokio_util::sync::CancellationToken`); tests fire it on demand to verify cancellation paths. |

The wire layer's `Wire::run(fd: OwnedFd, ctx: WireCtx)` signature is
the most consequential one: keeping `WireCtx` a struct of trait
objects (rather than concrete types) makes points 4 and 5 above
trivial. This is a hard requirement on the [api.md §1](api.md) and
[backend-wire.md §1](backend-wire.md) trait definitions.

## 3. Test harness shape

Three concentric levels; each phase delivers at least the inner level
and adds the next when its subsystem lands.

### 3.1 Unit (per-module, no PG)

- `crates/api`: trait-level shape tests; no I/O.
- `core::handoff::listener`: drive `run_handoff_loop` with a
  fake `Stream<Item = io::Result<OwnedFd>>` + mock `HandoffHandle`.
  Assert accept-error backoff and shutdown drain behaviour.
- `core::wire::pgwire_v3`: drive `Wire::run` with `socketpair()` fds
  loaded with recorded FE byte streams (captured via `psql` /
  tcpdump). Mock SPI bridge returns canned `SpiTupleTable`-shaped
  results. Mock HBA bridge returns canned rules.
- `core::handoff::tcp`: trivial — verify `build()` parses
  config and `run()` calls the listener-loop helper.

### 3.2 Integration (in-process PG via pgrx-tests)

- `cargo pgrx test` spins up an ephemeral cluster with our extension
  in `shared_preload_libraries`, runs `#[pg_test]` functions inside
  PG backend processes.
- Tests cover the end-to-end loop with real SPI, real `pg_hba.conf`,
  real bgworker spawn. Use `psql` or `tokio-postgres` as the client.
- Per-handoff state reset acceptance test (the phase-9 one): pin one
  slot, run N back-to-back handoffs each doing
  `PREPARE foo AS …; CREATE TEMP TABLE t…; SET tz = …`, assert the
  next handoff sees no `foo`, no `t`, default `tz`.
- See [user-memory `pgrx.md`](../background/) for the `#[pg_test]`
  gotchas that the bench harness can otherwise re-discover the hard
  way.

### 3.3 End-to-end (out-of-process, sibling to bench)

- Same harness as `crates/bench` (separate cluster, `psql` /
  `tokio-postgres` clients) but asserting correctness rather than
  timing. Useful for cross-version PG matrix testing and for
  scenarios that need a real signal (`kill -SEGV` on a slot
  bgworker, observe respawn).
- Phase-5 ships first because we need the same machinery for bench
  numbers anyway; correctness e2e then drops in as a sibling.

## 4. Error-injection mechanism

The risk register above hinges on being able to inject specific
failure modes. The seams from §2 must support the following injections,
each provided as a trait adapter in `crates/core/tests/common/` (or a
`crates/testutil/` rlib if a non-pgrx-test consumer ever needs them —
see [workspace.md §1](workspace.md#1-cargo-workspace-layout)):

| Failure mode                | How to inject                                                                                  |
| --------------------------- | ---------------------------------------------------------------------------------------------- |
| Slot bgworker death         | `kill -SEGV <slot_pid>` in an integration test; assert frontend `sendmsg → EPIPE` then respawn |
| TLS handshake failure       | Mock `WireCtx.tls_acceptor` that returns `Err` on `accept()`; assert wire emits `ErrorResponse` and closes |
| Auth failure (wrong creds)  | Real SCRAM exchange with wrong password; assert `SQLSTATE 28P01` arrives at client             |
| SPI runtime error           | Mock SPI bridge returns `Err(SPI_ERROR_*)`; assert wire emits `ErrorResponse` with mapped SQLSTATE |
| Pool exhausted              | Set `backend_pool_size = 1`, hold one connection open, attempt second `handoff()`; assert it blocks until first releases |
| Shutdown mid-handoff        | Fire `ShutdownToken` while a handoff is awaiting slot-acquire; assert `Err(Cancelled)` and fd closed |
| Frontend SIGKILL            | `kill -KILL <frontend_pid>` while connections active; assert slot bgworkers detect postmaster-death-watchdog and exit cleanly |

## 5. Per-phase acceptance criteria

A phase is **not** considered done until all listed criteria pass in
CI. The criteria are conservative — they protect the contracts the
phase's deliverable establishes for downstream phases.

| Phase | Acceptance criteria (in addition to roadmap "done" line)                                                            |
| ----- | -------------------------------------------------------------------------------------------------------------------- |
| **0** | `cargo check -p api -p core` passes. Docs build (link validator green).                                              |
| **1** | Signal-coexistence test (SIGHUP/SIGTERM bursts under load); postmaster-death watchdog test; heartbeat task survives 60 s of randomised tokio task spawning |
| **2** | Slot crash → frontend respawn test (`kill -SEGV` on a slot); recvmsg drains the in-flight buffer cleanly on shutdown; concurrent-handoff serialisation per slot |
| **3** | `tcp_handoff` + null wire: `psql` connects, server sends `FATAL`, client disconnects without orphaned fds (verified via `lsof` snapshot) |
| **4** | `SELECT 1` round-trip via real SPI; multi-row result; SPI error → correct `ErrorResponse`; wire-layer unit tests with mocked SPI bridge cover the message-loop state machine |
| **5** | Bench harness produces CSV + correctness e2e harness (sibling) runs the phase-4 vectors against a real cluster      |
| **6** | *(deferred — `transport-uds-handoff` is not in v0)*                                                                  |
| **7** | Each in-v0 auth method has a positive test (correct creds → success) and a negative test (wrong creds → `SQLSTATE 28P01`); both `auth_source` values produce equivalent results for equivalent rules |
| **8** | `sslmode=require` test over TCP; cert chain failure test; `tls_min_proto` enforcement test; HTTP/HTTPS protocol-mixup rejection |
| **9** | Reset-correctness checklist from [backend-wire.md §8 Q1](backend-wire.md#8-open-questions): named prepared statement clears between handoffs; bound portal drops; `SET` does not survive — assert via catalog scan in the *next* handoff on the same slot |

A phase that ships without its acceptance tests in CI carries a
correctness debt the bench harness cannot detect. That's the
foot-gun this whole doc exists to prevent.

## 6. What's out of scope for v0 testing

- **Fuzz testing the wire layer.** `pgwire` crate already has fuzz
  targets; we don't add our own until v0+1.
- **Performance regression detection.** The bench harness records
  numbers; comparing them across commits is its own follow-up.
- **Cross-PG-version compatibility matrix.** v0 targets one PG
  version (whichever pgrx supports at release); matrix testing waits
  until a second deployment cares.
- **Security-focused testing.** The SoT review's red-team findings
  ([reviews/](reviews/2026-05-17-base-design/REVIEW-SECURITY-red-team.md))
  are mostly hardening items deferred for the research-framework
  v0; the corresponding security tests come with un-deferral.

---

## See also

- [roadmap.md](roadmap.md) — phased plan; this doc gates each phase's "done"
- [backend-wire.md §8 Q1](backend-wire.md#8-open-questions) — the phase-9
  reset-correctness checklist this doc codifies
- [api.md §1](api.md) — the trait shape that constrains test seams
- `reviews/2026-05-17-base-design/REVIEW-TESTING-baseline.md` — the SoT
  review finding this doc resolves (must-fix F5)
