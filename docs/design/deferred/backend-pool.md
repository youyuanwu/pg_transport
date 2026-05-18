# Backend pool (general path)

> Parent: [README.md](../README.md)
> Sibling: [frontend-handoff.md](../frontend-handoff.md) · [api.md](../api.md) · [architecture.md](../architecture.md)
>
> **STATUS: DEFERRED.** This document describes the planned `SessionTransport`
> + `SessionHandle` general path. **It is not implemented in v0.** v0 ships
> the handoff path only (see [frontend-handoff.md](../frontend-handoff.md)) and a pre-spawned
> bgworker pool whose only role is receiving handed-off fds. The DSM /
> `shm_mq` / `pq_redirect_to_shm_mq` plumbing, the `Payload` envelope,
> `ExecutorSession`, `FrameStream`, frame tagging, and the cross-process
> wakeup options A/B/C below are captured here so they can land later
> without re-deriving the design. See [roadmap.md §4 — Deferred for v0](../roadmap.md)
> for the rejection rationale.

This doc describes the framework's **general path**: requests submitted
via `SessionHandle::execute(...)` or `SessionHandle::acquire(...).submit(...)` are serialised into
shared-memory queues (`shm_mq`), processed by a backend bgworker, and
streamed back as FE/BE response frames. It is the detailed companion to
[api.md](../api.md), which describes the trait surface in the abstract.

Use this path for transports whose wire protocol is **not** FE/BE on a
kernel socket: HTTP/2 + JSON-SQL, custom binary protocols, datagram
transports (QUIC, DPDK), or any FE/BE transport that needs to
inspect/modify protocol bytes before submission. Transports that hold a
kernel `OwnedFd` and speak FE/BE v3 use the simpler fast path in
[frontend-handoff.md](../frontend-handoff.md) instead.

The high-level mechanism is lifted from
[`pg_background`](../../background/pg_background.md): a pool of background
worker bgworkers, each with a DSM segment and a pair of `shm_mq`s, with
`pq_redirect_to_shm_mq` installed so the backend's FE/BE protocol output
goes into the response queue instead of a real TCP socket. The framework's
contribution on top is (a) a *pre-spawned, long-lived* pool instead of
per-request fork, (b) an async-friendly request/response shape on the
frontend side, and (c) a serialised request envelope so each worker can
serve multiple sessions sequentially over its lifetime.

> The same pool also serves the handoff path. A slot is either holding a
> handed-off connection (handoff path) or serving an `acquire`d session
> (this doc) at any time, never both. See
> [frontend-handoff.md §2](../frontend-handoff.md) for the cross-path slot model.

---

## 1. Anatomy of a backend slot

The pool consists of N slots, where N = `pg_transport.backend_pool_size`
(GUC, defaults to `max(4, num_cpus)`). Each slot is allocated once at
frontend startup and lives until shutdown.

### Per-slot resources

| Resource                    | Created by | Lifetime | Purpose                                                  |
| --------------------------- | ---------- | -------- | -------------------------------------------------------- |
| Slot DSM segment            | Frontend | Pool     | Container for the two `shm_mq`s and the fixed-data block |
| `req_q` (`shm_mq`)          | Frontend | Pool     | Carries serialised `Request` envelopes, frontend → worker |
| `resp_q` (`shm_mq`)         | Frontend | Pool     | Carries FE/BE response frames, worker → frontend       |
| Fixed-data block            | Frontend | Pool     | Slot id, frontend PID, error fields (see §6)           |
| Bgworker child process      | Frontend (via `RegisterDynamicBackgroundWorker` at pool startup) | Pool | Runs the slot's backend loop |
| `slot_loop` tokio task      | Frontend | Pool     | Drains `resp_q` and routes frames to per-session mpscs    |

### Per-session resources (acquired and released through `SessionHandle`)

| Resource                    | Owner            | Lifetime               | Purpose                                                |
| --------------------------- | ---------------- | ---------------------- | ------------------------------------------------------ |
| Slot assignment             | `ExecutorSession`| Session                | A specific slot pinned to this session                 |
| `conn_id: u64`              | `ExecutorSession`| Session                | Multiplexing key in the slot router map                |
| Router entry: `conn_id → mpsc::Sender<Bytes>` | Slot router | Session    | Where the slot's response demux pushes inbound frames  |
| `FrameStream`               | Transport        | Per `submit()` call    | The receiver half surfaced from `session.submit()`     |

### Per-request resources

| Resource                    | Lifetime              | Purpose                                                 |
| --------------------------- | --------------------- | ------------------------------------------------------- |
| Request envelope            | Frontend → worker   | `(conn_id, Request)` length-prefixed on `req_q`         |
| Response frame stream       | Worker → frontend   | Sequence of FE/BE frames tagged with `conn_id` on `resp_q`, terminated by `ReadyForQuery` |

---

## 2. End-to-end path of one `Payload`

```
Transport's tokio task                Frontend's per-slot task              Backend bgworker (separate process)
═════════════════════════             ════════════════════════════════        ═══════════════════════════════════

session.submit(Payload::Raw(b))                                                shm_mq_receive_blocking(req_q)
  │                                                                                       ▲
  │ 1. serialize Request → wire form                                                      │
  │ 2. shm_mq_send(req_q, envelope)      ─────────────────────────────────────────────────┘
  │ 3. register (conn_id → tx) in slot
  │    router; receiver becomes FrameStream
  │ 4. return FrameStream(rx)                                                  decode envelope:
                                                                                  (conn_id, Request)

                                                                               set_current_conn(conn_id)

                                                                               match req {
                                                                                 Raw(b)   → exec_message(b)
                                                                                 Sql(s)   → SPI_execute(s)
                                                                                 Extended → SPI_prepare + execute_plan
                                                                               }

                                                                               pq_redirect_to_shm_mq already
                                                                               installed → SPI output frames
                                                                               flow into resp_q, each prefixed
                                                                               by the current conn_id:
                                                                                  RowDescription
                                                                                  DataRow…
                                                                                  CommandComplete
                                                                                  ReadyForQuery  ← terminates conn_id's stream
                                       loop {
                                         wakeup().await;
                                         while let Some(frame) =
                                             shm_mq_receive_nb(resp_q) {
                                               let (conn_id, bytes) =
                                                   decode_response_frame(frame);
                                               router[conn_id].send(bytes);
                                               if is_ready_for_query(bytes) {
                                                 // do NOT drop the mpsc tx;
                                                 // the session may submit again.
                                               }
                                         }
                                       }

while let Some(frame) = stream.next().await
   socket.write_all(&frame).await
   // protocol-level RFQ signals end-of-stream for this submit().
```

Five things are worth pulling out of the picture:

1. The transport never opens or reads the `shm_mq` directly. It hands
   bytes to `submit()` and receives a `FrameStream`. The shared-memory
   plumbing lives entirely inside the frontend.
2. The frontend's `slot_loop` is the only thing reading `resp_q`. It
   demultiplexes by `conn_id` and pushes onto the right per-session
   mpsc.
3. The backend bgworker's loop is essentially `pg_background`'s loop,
   minus the "exit after one query" behaviour. It serves many envelopes
   over its lifetime.
4. The `conn_id` tag flows in both directions. The envelope carries the
   request's conn_id; every frame the worker emits is prefixed with the
   conn_id that's currently *active* (set by `set_current_conn` right
   before SPI executes).
5. The `FrameStream` terminates on `ReadyForQuery` (or `ErrorResponse`
   followed by `ReadyForQuery`). The frontend does not tear down the
   per-session mpsc on RFQ — that mpsc is reused for the session's
   *next* `submit()` call.

---

## 3. Frontend side: pseudocode

```rust
// crates/core/src/backend/pool.rs

pub struct Pool {
    slots: Vec<Arc<Slot>>,
    next_conn_id: AtomicU64,
}

struct Slot {
    id: u32,
    seg: DsmSegment,
    req_q: ShmMqSender,         // wraps shm_mq_send
    resp_q: ShmMqReceiver,      // wraps shm_mq_receive_nb
    router: Mutex<HashMap<u64, mpsc::UnboundedSender<Bytes>>>,
    shutdown: ShutdownToken,
    in_use: AtomicBool,         // simple pinning gate; later: per-session refcount
}

impl Slot {
    /// One task per slot, spawned on the LocalSet at pool startup.
    async fn slot_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_micros(100));
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = tick.tick() => {
                    while let Some(frame) = unsafe { self.resp_q.recv_nowait() } {
                        let (conn_id, bytes) = decode_response_frame(frame);
                        if let Some(tx) = self.router.lock().get(&conn_id) {
                            let _ = tx.send(bytes); // ignore: receiver may have dropped
                        }
                    }
                }
            }
        }
    }
}
```

```rust
// crates/api/src/session.rs (impl detail behind SessionHandle)

impl SessionHandle {
    pub async fn acquire(&self, opts: SessionOpts) -> anyhow::Result<ExecutorSession> {
        // Phase 2 model: one connection ↔ one slot, exclusive.
        let slot = self.pool.checkout_exclusive().await?;
        let conn_id = self.pool.next_conn_id();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        slot.router.lock().insert(conn_id, tx);
        // Send a "startup" envelope so the backend sets per-session GUCs,
        // application_name, etc. (Optional in phase 2; pgwire passes those
        // through as a normal Raw(StartupMessage)).
        Ok(ExecutorSession { slot, conn_id, rx: Some(rx) })
    }
}

impl ExecutorSession {
    pub async fn submit(&mut self, req: Request) -> anyhow::Result<FrameStream> {
        let envelope = encode_request_envelope(self.conn_id, &req);
        unsafe { self.slot.req_q.send(&envelope)?; }   // shm_mq_send (may block on full queue)
        // The rx half was set up at acquire(). After the FrameStream
        // terminates (RFQ observed), the session is reusable for the next
        // submit; the mpsc stays installed.
        let rx = self.rx.take().expect("submit called twice without re-arming");
        Ok(Box::pin(rfq_terminated(rx)))               // ends at the first RFQ
    }
}

impl Drop for ExecutorSession {
    fn drop(&mut self) {
        self.slot.router.lock().remove(&self.conn_id);
        self.slot.checkin_exclusive();
    }
}
```

`rfq_terminated` is a small `Stream` adapter that yields bytes until it
sees a `Z` (`ReadyForQuery`) frame, after which it yields the RFQ frame
itself and then ends. The transport sees a clean per-`submit` stream and
the session's underlying mpsc is preserved for the next call.

---

## 4. Backend side: pseudocode

```rust
// crates/core/src/backend/worker_main.rs (pgrx, sync, runs in the bgworker child)

#[pg_guard]
pub extern "C" fn pg_transport_backend_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );
    BackgroundWorker::connect_worker_to_spi(/* database */ None, None);

    let slot = unsafe { attach_dsm_slot(arg) };
    unsafe { pq_redirect_to_shm_mq(slot.seg, slot.resp_q) };

    // CURRENT_CONN_ID is consulted by the response-frame emitter (see §5)
    // so each frame written to resp_q is tagged with the conn_id that's
    // logically producing it.
    loop {
        let envelope = match slot.req_q.recv_blocking() {
            Ok(b)  => b,
            Err(_) => break,                              // shutdown / detach
        };
        let (conn_id, req) = decode_request_envelope(&envelope);
        CURRENT_CONN_ID.set(conn_id);

        let outcome = match req {
            Req::Raw(b)              => exec_fe_message(&b),
            Req::Sql(s)              => exec_simple_query(&s),
            Req::Extended { s, p }   => exec_extended(&s, &p),
            Req::Shutdown            => break,
        };

        // After execution: PG's normal FE/BE flow already emits
        // CommandComplete and ReadyForQuery via pq_redirect_to_shm_mq.
        // We just make sure the buffer is flushed.
        pq_flush();
        if let Err(e) = outcome { emit_error_response(&e); pq_flush(); }
    }

    // Slot tear-down: detach DSM, exit.
}
```

The three request paths:

| Variant         | What the worker does                                                  |
| --------------- | --------------------------------------------------------------------- |
| `Raw(bytes)`    | Hands the bytes to a minimal FE-message frontend that mirrors the relevant branch of `PostgresMain` (Query → SPI; Parse/Bind/Execute → extended-protocol path; Terminate → close session). |
| `Sql(s)`        | `SPI_connect` → `SPI_execute(s)` → emit results. Simpler path, suitable for non-FE/BE transports. |
| `Extended { … }`| `SPI_prepare` + `SPI_execute_plan`, with `Param` values converted via `postgres-types`. |

The `Raw` path is the one that requires the most care, because it has to
reproduce just enough of `PostgresMain` to honour Sync, Flush,
Describe, Close, etc. The phase-3 implementation lands a *minimal* `Raw`
handler that supports the FE messages `psql` actually sends; we extend
from there as new transports demand more.

---

## 5. Frame tagging and the response-side envelope

PostgreSQL's FE/BE output via `pq_redirect_to_shm_mq` writes raw protocol
frames into the queue. To multiplex many sessions over one slot, we
**don't** modify PG's emitter — we wrap each enqueued chunk in a small
envelope before it hits the queue:

```
+--------+---------+----------+======================+
|  ver   | conn_id |   len    |   FE/BE frame bytes  |
| 1 byte | 8 bytes | 4 bytes  |        len bytes     |
+--------+---------+----------+======================+
```

Two implementation options for the wrapping:

- **Option α** — the worker takes over the low-level `pq_putmessage` /
  `pq_endmessage` path with a thin shim that prepends the envelope before
  calling the underlying `shm_mq_send`. Requires intercepting the
  destination callback table set up by `pq_redirect_to_shm_mq`.
- **Option β** — the worker keeps a per-message scratch buffer, lets PG
  emit FE/BE frames into it (using a custom CommandDest), and flushes
  to `shm_mq` with the envelope prepended. Slightly higher CPU but no
  intervention in `pq_redirect_to_shm_mq`'s internals.

We pick **β** for phase 2 (simpler, no PG-internal hooks). Phase 4 (the
bench harness) will tell us whether the extra copy matters; if so, we
switch to α.

For phase 2 we only ever have one active session per slot (exclusive
pinning), so the envelope's `conn_id` is technically redundant. We keep
the field anyway as a forward-compat slot — without it, any future shared-slot
experiment would require a protocol change.

---

## 6. Cross-process wakeup: how the frontend knows there's data

This is the one part the design has been deliberately quiet about until
now. The frontend's `slot_loop` needs to be woken when the backend
pushes a frame onto `resp_q`. `shm_mq` does not expose a kernel fd that
tokio's reactor can poll directly, so we have three viable mechanisms:

### Option A — Interval poll (phase 2 default)

Each `slot_loop` task wakes every ~100 µs and runs `shm_mq_receive` in
non-blocking mode until it returns `SHM_MQ_WOULD_BLOCK`. Then back to
sleep.

```rust
let mut tick = tokio::time::interval(Duration::from_micros(100));
loop {
    tokio::select! {
        _ = shutdown.cancelled() => break,
        _ = tick.tick() => drain_resp_q(&slot),
    }
}
```

Pros: trivial, predictable, no extra threads or fds.
Cons: latency floor at ~½ × interval (50 µs average); wakeful CPU at idle.

### Option B — Eventfd bridge

Worker creates an eventfd at startup, writes its number into the slot's
fixed-data block. The frontend then needs the *same* eventfd; since
fds are per-process, this requires either:

- **B-a** — passing the fd over a per-slot Unix socket with `SCM_RIGHTS`,
  or
- **B-b** — having the frontend create the eventfd and pass it to the
  worker the same way (which doesn't actually work because the worker
  is a child of postmaster, not of the frontend, so postmaster-time fd
  inheritance doesn't help us either).

Either way: nontrivial. Lowest latency though (single-µs wakeups).

### Option C — PG-latch bridge thread

A single frontend-owned auxiliary thread sits in
`WaitLatch(MyLatch, ..., 1000ms)`. When the frontend's latch fires (the
worker calls `SetLatch(frontend_proc->procLatch)` after `shm_mq_send`),
the bridge thread writes to a single eventfd that tokio is watching, then
loops back. Tokio sees the wakeup and drains all slots' `resp_q`s.

Pros: matches PG's own conventions; uses infrastructure that's already
documented and stable; single eventfd for the whole pool.
Cons: requires one auxiliary thread strictly disciplined to never touch
PG state beyond `WaitLatch` / `ResetLatch`.

### Recommendation

**Phase 2 lands Option A.** It is unambiguous to implement, has no
operational surprises, and gives us a working request path against which
the bench harness can measure baseline numbers. **Phase 4** (bench) tells
us whether the polling latency hurts; if it does, we move to **Option C**
because it composes best with PG's signalling and only adds a single
thread. **Option B** stays available as a "go faster" knob if we ever need
to drive a single slot to its theoretical limit.

This is open question §6 in [roadmap.md](../roadmap.md).

---

## 7. Slot allocation, pinning, and lifetime

### Phase 2: exclusive pinning

`SessionHandle::acquire` reserves a slot for the entire lifetime of the
`ExecutorSession`. The slot's `in_use` flag is checked atomically; if all
slots are busy, `acquire` either:

- **blocks** on a tokio `Semaphore` until one frees (preferred for FE/BE,
  which expects connect to succeed); or
- **returns** an `Err(PoolExhausted)` if the caller specifies
  `SessionOpts::nonblocking`.

This model is correct for FE/BE v3 because the backend holds server-side
session state (temp tables, prepared statements, GUCs) that must not bleed
between sessions.

### Future: shared slots for stateless protocols

For a stateless HTTP/2 transport that runs one query per request,
exclusive pinning wastes capacity. A later phase can introduce a "shared
mode" where a slot serves multiple in-flight `Payload::Sql` calls
concurrently, identified by `conn_id` in the envelope. The slot's PG
state must be reset between requests (`DISCARD ALL` or a fresh `SPI_connect`
context). Open question §10 in [roadmap.md](../roadmap.md) tracks this.

### Pool sizing

GUC: `pg_transport.backend_pool_size`. Default `max(4, num_cpus)`.
Hard ceiling: `max_worker_processes - safety_margin`. The frontend
refuses to start more than will fit, with a clear error.

---

## 8. Shutdown sequence

```
1.  Supervisor receives SIGTERM (or postmaster-death).
2.  ShutdownToken::cancel() — propagates to every transport's `run`.
3.  Transports stop accepting; existing sessions either drain or get
    cancelled depending on transport policy.
4.  As transports finish their `run` futures, ExecutorSessions drop;
    routers empty; slots become idle.
5.  Once join_all(transports) completes, the frontend calls
    exec.shutdown().
6.  exec.shutdown() sends Req::Shutdown to every slot's req_q.
7.  Each backend bgworker exits its loop, detaches DSM, and exits the
    process.
8.  slot_loop tasks observe their slot's shutdown token, exit.
9.  Frontend bgworker returns from frontend_main; pgrx exits.
```

The ordering matters: we drain transports first so no new requests can
arrive while the backend pool is closing.

---

## 9. Error paths

| Source                                | Surfaced as                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------ |
| SQL error inside backend             | `ErrorResponse` frame on `resp_q`, then `ReadyForQuery`. Transport sees them in the `FrameStream` and forwards. Same as a normal libpq client. |
| Worker process crash                  | Detected by detach callback on the slot's DSM; `slot_loop` notes the loss; pool tries to respawn; in-flight sessions see their `FrameStream` end with `08006 — lost connection to worker process` (synthesised by the slot loop). |
| Frontend exit during a request      | `ShutdownToken` cancellation propagates; transport's per-conn task drops its `FrameStream`; the slot router drops the mpsc tx; new frames coming off `resp_q` for that `conn_id` are dropped silently. |
| Slot pool exhausted                   | `acquire` blocks on its semaphore, or returns `Err(PoolExhausted)` for nonblocking callers. |
| Invalid envelope on `req_q`           | Worker emits an internal error (and may exit); slot loop reports the slot as faulted and the pool tries to respawn it. |

The "structured error info" tail of `pg_background` (SQLSTATE,
DETAIL/HINT/CONTEXT) is captured the same way — in the slot's fixed-data
block, with `error_sqlstate` written last as a publish flag. Transports
that want the structured shape (rather than the wire-level `'E'` frame)
can ask via a helper on `ExecutorSession`.

---

## See also

- [api.md](../api.md) — the `SessionTransport`, `SessionHandle`, `ShutdownToken`
  trait surface that the machinery in this doc implements.
- [../background/pg_background.md](../../background/pg_background.md) — the
  DSM + `shm_mq` + `pq_redirect_to_shm_mq` mechanics we lift.
- [roadmap.md](../roadmap.md) — phase plan; the wakeup-option ADR; the
  shared-slot open question.
