# Cancel routing — deferred design

> Parent: [../README.md](../README.md)
> Sibling: [../backend-wire.md](../backend-wire.md)
>
> **Status: deferred from v0.** This document captures the option
> analysis and a recommended design so the work can be picked up later
> without re-deriving anything. **v0 ships without cancel routing**:
> the wire layer recognises `CancelRequest` (the 8-byte magic in the
> startup probe) but closes the fd without acting on it. Operationally
> this means **Ctrl-C in `psql` terminates the connection** rather
> than cancelling the running query — same as connecting to a
> default-PG that lost its cancel-key lookup.

## 1. Why deferred

Cancel routing is the only piece of `pg_transport`'s wire layer that
requires cross-slot coordination state (every other concern — TLS,
auth, FE/BE loop, SPI bridge — is fully contained inside one slot's
wire layer for the duration of one connection). Adding that state
brings either a shared-memory map (with its size / locking / cleanup
overhead) or a frontend-owned registry plus a new bidirectional UDS
protocol. Both are tractable; neither is the smallest path to a
working v0.

Deferring this lets phase 9 (extended-query support) be the v0
endpoint, with a usable wire surface for `psql` and libpq apps that
don't depend on cancel semantics.

## 2. Background: how default PG does it

Postmaster mints `MyCancelKey` per accepted connection, `fork()`s the
backend (key inherited via CoW), and appends `{pid, cancel_key}` to
its local `BackendList` (in-process heap, *not* shared memory). When a
`CancelRequest` arrives, postmaster's accept loop reads the magic,
walks `BackendList` for a match, `kill(SIGINT, target_pid)`. The
target's `StatementCancelHandler` sets `QueryCancelPending`; SPI
notices at the next `CHECK_FOR_INTERRUPTS()` and throws
`ERRCODE_QUERY_CANCELED`. Postmaster is both the single accepter and
the single authority, so it can keep the lookup table in local heap.

We don't get that for free: our frontend isn't the parent of the
backend bgworkers (postmaster is), our wire layer mints the key after
auth in some random slot, and any other slot's wire layer might be
the one that receives the eventual `CancelRequest`. So some form of
cross-process lookup is needed.

## 3. Options considered

### A — Shared-memory cancel registry + `kill(SIGINT)`

Fixed-size shared-memory `HashMap<(pid, key), slot_pid>` allocated at
`shared_preload_libraries` startup. Wire layer registers post-auth,
deregisters on disconnect / per-handoff reset. Receiving slot's wire
layer does an O(1) shared-memory lookup and `kill(SIGINT, slot_pid)`.

- **Pros**: faithful port of PG's design; concurrent registration uses
  an LWLock around a fixed-size table; `pg_cancel_backend(slot_pid)`
  from SQL works for free because we use real bgworker pid.
- **Cons**: requires SPL (which we already plan to require); shared
  memory needs to be sized at startup; cleanup on bgworker crash needs
  a hook.

### B — Frontend-owned registry over the per-slot UDS *(recommended)*

The per-slot UDS, currently one-way (frontend → backend, only carrying
`Handoff` frames with `SCM_RIGHTS`), grows two more frame tags:

```
frame = | u8 tag | u32 body_len | body[body_len] | [SCM_RIGHTS fd?] |

Frontend → Backend
  0x01 Handoff         body = HandoffHints,             aux = SCM_RIGHTS(client_fd)

Backend → Frontend
  0x10 KeyRegister     body = { pid: u32, key: u32 }    aux = (none)
  0x11 KeyDeregister   body = { pid: u32, key: u32 }    aux = (none)
```

The frontend keeps an in-process map:

```rust
struct CancelRegistry {
    by_key: HashMap<(u32 /*pid*/, u32 /*key*/), SlotId>,
    by_slot: HashMap<SlotId, (u32, u32)>,     // for cleanup on slot crash
}
```

No shared memory, no locks (frontend is tokio current-thread + `LocalSet`).

**Cancel arrival path** (frontend short-circuits before handoff):

1. Frontend `MSG_PEEK`s the first 8 bytes of any newly-accepted fd.
2. If they match the `CancelRequest` magic, frontend reads the
   full 16-byte cancel message itself, looks up `(pid, key)` in
   `by_key`, and `kill(SIGINT, slot_pid)`. Closes the cancel fd
   without handing it to a backend slot.
3. Target slot's PG `StatementCancelHandler` sets `QueryCancelPending`;
   SPI throws `ERRCODE_QUERY_CANCELED` at the next
   `CHECK_FOR_INTERRUPTS()`.

The `kill(SIGINT)` step is deliberate, not avoided: SPI is sync; the
slot runner can't usefully read its UDS while the wire is mid-`SPI_*`.
The only thing that interrupts in-flight SPI is `SIGINT` →
`QueryCancelPending` → `CHECK_FOR_INTERRUPTS`. Sending a `Cancel` frame
over the UDS would require the slot runner to multiplex UDS reads
against the wire's SPI work, and the multiplex would still ultimately
have to raise `SIGINT` to actually interrupt SPI. Direct `kill(SIGINT)`
from the frontend is cleaner.

- **Pros**: no shared memory; no `LWLock`; lookup is one O(1) HashMap
  hit on a single-threaded process; `pg_cancel_backend(slot_pid)` still
  works for free because we emit real bgworker pid in `BackendKeyData`;
  cancels never burn a backend slot (FE short-circuits).
- **Cons**: one `MSG_PEEK` syscall per accepted fd (negligible);
  KeyRegister race window of ~µs between auth completion and frontend
  recv (acceptable — unmatched cancels are silently dropped like in PG,
  user retries).

### C — Route through frontend without `MSG_PEEK`

Variant of B: hand every fd to a slot as today. Slot's wire layer
reads the magic, recognises a cancel, and forwards `(pid, key)` back
to frontend over a new UDS frame. Frontend signals. Doesn't avoid the
extra UDS hop and burns a slot per cancel; no real advantage.

### D — `SO_REUSEPORT`-style: each slot accepts its own cancel fds

Already rejected for the main listener (see [../roadmap.md §5](../roadmap.md));
re-rejected here for the same reasons.

### E — No cancel support

What v0 does.

## 4. Recommended design when un-deferred: Option B

The UDS protocol upgrade and the frontend `CancelRegistry` from §3 B
above. Concretely the work is:

1. **UDS protocol**: introduce the framed format above. Bump a `version`
   field (today's UDS is "always one frame kind"; v1 adds the tags).
2. **Slot runner** (in [../backend-handoff.md](../backend-handoff.md)):
   the loop becomes a frame dispatcher. The only frame it actually
   parses for control purposes is `Handoff`; `KeyRegister` and
   `KeyDeregister` originate inside its own process and are written
   by the wire layer via a thin handle the slot runner provides.
3. **Wire layer** (in [../backend-wire.md](../backend-wire.md)):
   - Mint `secret_key` via `pg_strong_random` after auth.
   - Write `KeyRegister` to the slot runner's UDS writer.
   - Emit `BackendKeyData(pid=MyProcPid, key=secret_key)` to the client.
   - On `run` return (any path), write `KeyDeregister` from the slot
     runner's per-handoff reset.
4. **Frontend**:
   - Maintain `CancelRegistry`.
   - On UDS `KeyRegister` / `KeyDeregister` from any slot, update
     `by_key` and `by_slot`.
   - On slot-crash event (already tracked for respawn), sweep entries
     for the crashed `SlotId`.
   - On *every* accepted fd: `recv(MSG_PEEK, 8)`. If magic matches,
     handle inline; otherwise hand off as today.

## 5. Failure modes (under Option B)

| Scenario                                    | Behaviour                                                                                          |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Backend slot crashes mid-connection         | Slot runner can't send `KeyDeregister`; frontend's slot-crash handler sweeps `by_slot[SlotId]` and removes both entries. |
| Cancel for a slot that's idle               | `kill(SIGINT)` sets `QueryCancelPending`; the slot's PG signal handler picks it up; if no query is running, the flag clears at the next `CHECK_FOR_INTERRUPTS()`. Same as default PG. |
| Cancel arrives before `KeyRegister` reaches frontend | `by_key` lookup misses; frontend silently drops the cancel (PG also returns no response for unmatched cancels). User retries. |
| Frontend dies                              | Whole framework is down; cancel is moot.                                                           |
| Slot bgworker PID reused after crash + respawn | Stale entries with the old PID swept by the crash-handler; new bgworker registers fresh `(new_pid, fresh_key)`. No cross-talk. |

## 6. What v0 does in the meantime

The wire layer (`pgwire-v3` impl, [../backend-wire.md §3](../backend-wire.md)):

- Reads the 8-byte startup probe normally.
- On `CancelRequest` magic: log at `LOG` level, close the fd, return.
  No registry lookup (there's no registry), no signal.
- On `StartupMessage` etc.: continues normally.
- After auth: emits `BackendKeyData(pid=MyProcPid, key=random_u32)`.
  The `key` is never honoured by anything in v0; it's there so libpq
  parses the startup completion correctly.

Observable consequence: a client that sends `CancelRequest` to a
`pg_transport` listener gets the fd closed with no effect on the
in-flight query. `psql` interprets that as "lost the cancel channel"
and falls back to closing the *current* connection (which does abort
the query, since `SPI_execute` running on a backend whose client
socket has closed will eventually fail at the next attempted send and
roll back). Net behaviour: Ctrl-C drops the connection.

This is identical to early-PG behaviour (pre-cancel-protocol) and to
some pooler configurations that strip cancel-key tracking.

## See also

- [../backend-wire.md](../backend-wire.md) — the wire layer where the
  v0 cancel handling (close-the-fd) lives.
- [../frontend-handoff.md](../frontend-handoff.md) — the per-slot UDS protocol this
  design extends.
- [../roadmap.md](../roadmap.md) — Q on cancel routing tracked as
  deferred there too.
