# `pg_transport` — Deferred Transports

> Status: **deferred** — not in scope for the current build phase.
> Owning design doc: [design/README.md](../README.md)

The transports listed here are interesting experimental targets but are
**out of scope for the current iteration** of `pg_transport`. The framework's
plugin trait is shaped so these can be added later without changes to the
core frontend; this document keeps the notes, library choices, risks, and
open questions so we don't lose them.

**Currently in scope** (see [design/README.md](../README.md)): TCP, Unix domain
sockets, TLS (over TCP/UDS via `tokio-rustls`).

**Deferred** (this doc): QUIC, io_uring, AF_XDP, DPDK, RDMA,
shared-memory loopback. The DPDK/AF_XDP polled-transport bridge pattern
also lives here.

---

## 1. When do we revisit?

The trigger for picking up any of these is the same: **the baseline path
(TCP + UDS + TLS + FE/BE v3 + bench harness) is stable and producing
comparable benchmark numbers across runs.** Without those numbers any claim
that QUIC or io_uring "helped" would be unfounded.

Tentative order, once we re-enter this work:

1. **io_uring** — same protocol, swap the syscall path. Smallest delta
   against the TCP baseline; the cleanest "did kernel-bypass-lite help?"
   experiment.
2. **QUIC** — first non-TCP transport; integrates rustls; lets us
   experiment with 0-RTT and connection migration for FE/BE.
3. **AF_XDP** — first transport requiring the polled-bridge pattern, but
   without DPDK's operational complexity.
4. **DPDK** — only on a real test rig with a spare NIC, hugepages, pinned
   cores. The biggest operational lift.
5. **RDMA** — only if we have an InfiniBand / RoCE cluster to use it
   against.
6. **Shared-memory loopback** — a curiosity baseline that establishes a
   "fastest possible local client" reference.

---

## 2. Per-transport notes

Library-of-record recommendations and integration sketches. Driver column
indicates how the transport plugs into the tokio current-thread runtime
(see [design/README.md §2 C-1](../README.md) for context).

| Transport          | Crate(s)                                            | Driver           | Notes                                                                |
| ------------------ | --------------------------------------------------- | ---------------- | -------------------------------------------------------------------- |
| io_uring           | `tokio-uring` (current-thread mode) **or** `io-uring` raw | tokio / AsyncFd | `tokio-uring` is opinionated and *current-thread* by design; perfect fit. Raw `io-uring` if we need full control. |
| QUIC               | **`quinn`** (tokio-native, rustls)                  | tokio            | Default QUIC plugin. `quiche` (Cloudflare, sync/BoringSSL) remains a viable alternate. |
| AF_XDP             | `xsk-rs` / `libxdp` bindings                        | bridge + AsyncFd | Tiny BPF program required; pinned umem; eventfd bridge for wakeups   |
| DPDK               | hand-rolled `bindgen` for `rte_*`                   | bridge + AsyncFd | Data-plane thread + eventfd; pin cores; see §3 below                 |
| RDMA               | `rdma-sys` / `rdma`                                 | AsyncFd          | CQ event channel fd registered via `AsyncFd`; usually paired with a custom protocol |
| Shmem-loopback     | direct DSM + `shm_mq` + AsyncFd over the notify fd  | AsyncFd          | Curiosity baseline — "fastest possible" for local clients            |

### 2.1 QUIC specifics

- `quinn` slots in natively under the frontend's tokio current-thread
  runtime. Pairs with `rustls`.
- Stream-to-connection mapping needs a small protocol-layer adapter. FE/BE
  v3 is byte-stream oriented; we treat one QUIC bidirectional stream as
  one PG session.
- 0-RTT is an interesting research surface — see open question §5.1.
- `quiche` remains useful for comparison runs because it forces a different
  threading model (sync state machine + manual UDP socket driving), which
  is a useful counterpoint to `quinn`'s tokio-native style.

### 2.2 io_uring specifics

- Prefer `tokio-uring` because it's current-thread by design — no
  multi-thread runtime to fight with. The frontend's existing
  `LocalSet` cohabits with `tokio-uring`'s `Runtime` cleanly.
- The raw `io-uring` crate is the escape hatch when we need
  features `tokio-uring` doesn't expose (e.g. multi-shot accept,
  `IORING_OP_SEND_ZC`).
- Don't enable `IORING_SETUP_SQPOLL` until baseline numbers exist; it burns
  a CPU core invisibly and confuses comparisons.

### 2.3 RDMA specifics

- Realistically paired with a *custom* protocol rather than FE/BE v3;
  RDMA's value is sub-microsecond message passing and that's wasted on a
  protocol designed for TCP RTTs.
- CQ event channel fd is the integration point; wrap in `AsyncFd`.
- Memory-region registration cost is non-trivial and amortises poorly for
  short messages — the experiment will mostly tell us about message size
  thresholds.

---

## 3. The polled-transport bridge pattern (DPDK / AF_XDP / RDMA-CM)

The only architecturally "different" case — these transports do not expose
a kernel fd that tokio's reactor can poll for readiness. The pattern
(Seastar-shaped, in miniature):

```
DPDK poll thread (pinned core, no PG access)
        │  rte_ring_enqueue(RxBuf)
        ▼
   SPSC ring (lock-free, cache-line padded)
        │
        │  on enqueue: write(eventfd, 1)
        ▼
   AsyncFd(eventfd).readable().await ─► main thread drains ring,
                                         hands bytes to the `Protocol` impl,
                                         dispatches to BackendPool
                                                     │
                              backend result frames ▼
                                         encode → SPSC ring (Tx)
                                                     │
                              DPDK poll thread tx-bursts
```

- Two rings (Rx, Tx), one eventfd per direction.
- The main thread is the **only** PG-touching thread.
- CPU pinning is the user's responsibility (GUC: `pg_transport.dpdk_cores`).
- Operational caveat: the NIC is taken away from the kernel. `psql` /
  cluster admin cannot reach the node on that NIC. Plan for a separate
  management NIC.

We expose this bridge as a small reusable type
(`api::PolledTransportBridge<Rx, Tx>`) so AF_XDP, DPDK, and RDMA-CM
implementations don't each reinvent it.

---

## 4. Phased plan (continues after the in-scope phases)

Numbering continues from [design/roadmap.md §1](../roadmap.md). The in-scope plan ends
at the HTTP/2 protocol experiment; everything below is what comes after.

| Phase  | Deliverable                                           | Gate / done criterion                                                |
| ------ | ----------------------------------------------------- | -------------------------------------------------------------------- |
| **F1** | `transport-iouring`                                   | Same protocol as TCP baseline; bench delta vs. TCP documented        |
| **F2** | `transport-quic-quinn`                                | First non-TCP transport; rustls-based; bench delta vs. TCP+TLS       |
| **F3** | `transport-afxdp` + `PolledTransportBridge`           | First bridge-pattern transport; bench vs. baseline kernel TCP        |
| **F4** | `transport-dpdk`                                      | Operationally heavy; gated on having a real test rig                 |
| **F5** | `transport-rdma`                                      | Paired with a small custom protocol; only on a real RDMA fabric      |
| **F6** | `transport-shmem-loopback`                            | "Lower bound on latency" reference baseline for local clients        |

Each picks up the same trait surface as the in-scope transports — no
frontend changes expected. Cargo features `iouring`, `quic-quinn`,
`quic-quiche`, `afxdp`, `dpdk`, `rdma`, `shmem-loopback` are added to
`crates/core/Cargo.toml` as those phases begin.

---

## 5. Open questions (deferred until the relevant phase)

### 5.1 0-RTT in QUIC for FE/BE

QUIC's 0-RTT data is sent before the handshake completes. For FE/BE v3
auth this is genuinely a research question: do we accept early-data as
fully-authenticated (relying on the session ticket the client replays), do
we hold execution until the handshake finishes, or do we restrict 0-RTT to
idempotent operations only? Real PostgreSQL deployments will demand a
defensible answer.

### 5.2 Connection migration

QUIC connection migration across client IPs is interesting for mobile /
roaming clients but conflicts with the backend-pinning model (per-conn
state lives in a specific bgworker). Worth exploring but not before basic
QUIC works.

### 5.3 RDMA + protocol pairing

RDMA over FE/BE v3 is unlikely to be worth it; over a small custom protocol
it might be. The question is which custom protocol — `pg_transport`'s own
framework binary protocol, or an RDMA-specific one?

### 5.4 DPDK CPU budget

DPDK requires a pinned core per poll thread. On a 4-core development
machine that's 25% of the cluster's CPU dedicated to one transport. The
operational story (config, cgroups, NUMA placement) needs documentation
before we ship.

---

## 6. Deferred risks

| Risk                                                       | Likelihood | Mitigation                                                                    |
| ---------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------- |
| Cross-thread soundness in the polled-transport bridge      | Medium     | `loom`-based tests on the SPSC ring; `miri` on the unsafe pieces              |
| DPDK plugin is operationally unusable on most dev machines | High       | Mark experimental; provide AF_XDP as the "kernel-bypass-lite" alternative     |
| `quinn` / `tokio-uring` ABI churn during the deferral      | Low        | Pin versions when phase begins; re-evaluate ecosystem state at re-entry       |
| RDMA hardware availability                                 | High       | Skip in dev cycles; only schedule when a real fabric is available             |

---

## 7. References

- quinn (tokio-native QUIC): <https://github.com/quinn-rs/quinn>
- quiche (sync QUIC, BoringSSL): <https://github.com/cloudflare/quiche>
- tokio-uring (current-thread io_uring): <https://github.com/tokio-rs/tokio-uring>
- io-uring crate (raw): <https://github.com/tokio-rs/io-uring>
- xsk-rs (AF_XDP): <https://github.com/DouglasGray/xsk-rs>
- DPDK: <https://www.dpdk.org/>
- F-Stack (DPDK + POSIX shim): <https://github.com/F-Stack/f-stack>
- Seastar (reactor model + shared-nothing inspiration for the bridge pattern): <https://github.com/scylladb/seastar>
- `rdma-core`: <https://github.com/linux-rdma/rdma-core>
- `AsyncFd` (the integration primitive for non-tokio-native fds): <https://docs.rs/tokio/latest/tokio/io/unix/struct.AsyncFd.html>
- Companion docs:
  - [design/README.md](../README.md) — current-scope framework design (index)
  - [design/backend-pool.md](backend-pool.md) — frontend ↔ backend mechanics
  - [background/pg_background.md](../../background/pg_background.md)
  - [background/omnigres.md](../../background/omnigres.md)
