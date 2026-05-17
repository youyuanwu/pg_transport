# `pg_background` — Architecture Review

> Source: [github.com/vibhorkum/pg_background](https://github.com/vibhorkum/pg_background)
> Versions covered: **1.6 → 1.9** (current default `1.9`, see `pg_background.control`).
> Supported PostgreSQL: **14 – 18**.

This document describes the internal architecture, process model, IPC mechanics,
lifecycle, and design rationale of the `pg_background` extension. It is a
*reading companion to the source*, not an API reference — for usage see the
project [README](https://github.com/vibhorkum/pg_background#readme).

---

## 1. What the extension does

`pg_background` lets a normal backend hand a SQL string to a freshly-spawned
**dynamic background worker** (`bgworker`), which executes it in an
**autonomous transaction** and streams the result back through a
**shared-memory message queue** (`shm_mq`). It is the in-process counterpart of
`dblink`/`pg_cron`:

| Property                   | `pg_background`                | `dblink`                  | `pg_cron`            |
| -------------------------- | ------------------------------ | ------------------------- | -------------------- |
| Transport                  | DSM + `shm_mq`                 | libpq TCP/Unix socket     | Scheduler + libpq    |
| Process                    | Postmaster-forked `bgworker`   | New backend via libpq     | Scheduled backend    |
| Transaction relationship   | Autonomous (independent xact)  | Independent connection    | Independent          |
| Shared memory access       | Yes (same cluster)             | No                        | No                   |
| Per-call setup cost        | Low (fork + DSM)               | Connection round-trip     | Cron tick latency    |

The autonomy is the headline feature: the worker's `COMMIT` is unaffected by
the launcher's `ROLLBACK`, which is the foundation for autonomous audit
logging, asynchronous `pg_notify`, and fire-and-forget maintenance.

---

## 2. Process model

There are three actors:

```
 ┌────────────────────┐  RegisterDynamicBackgroundWorker  ┌───────────────┐
 │  Launcher backend  │ ───────────────────────────────►  │   Postmaster  │
 │  (client session)  │                                   └──────┬────────┘
 │                    │                                          │ fork()
 │  pg_background_    │                                          ▼
 │  launch_v2(sql)    │       shm_mq (DSM key 3)        ┌──────────────────┐
 │                    │ ◄──────────────────────────────►│ bgworker process │
 │  pg_background_    │       Frontend/Backend frames   │  pg_background_  │
 │  result_v2(...)    │                                 │  worker_main     │
 └────────────────────┘                                 └──────────────────┘
```

* **Launcher backend** — the client session that called `launch_v2`. Owns the
  DSM segment, the per-session worker hash table, and `shm_mq` receive end.
* **Postmaster** — fulfils `RegisterDynamicBackgroundWorker()` by forking the
  worker with `BgWorkerStart_ConsistentState` so it can attach to a database.
* **Background worker** — runs `pg_background_worker_main`, attaches the DSM,
  loads the launcher's GUCs, runs the SQL through SPI / the executor, and
  ships protocol frames back through `shm_mq`.

The launcher does **not** poll an OS pipe or a socket; the launcher and worker
synchronise through `shm_mq_wait_for_attach` (worker readiness) and the queue's
own latch wakeups (result availability and worker death).

### Loading model

The library is **not required in `shared_preload_libraries`**. Each worker
process `dlopen()`s `pg_background.so` on demand. SPL is optional and only
recommended when GUCs (`pg_background.max_workers`,
`pg_background.default_queue_size`, `pg_background.worker_timeout`) must be
present in `postgresql.conf` before any session has triggered first-use
registration. The deployment-order trade-off this creates is documented in the
README under *Library Loading* and *Deployment Order*.

---

## 3. Dynamic shared memory layout

`launch_v2` allocates one DSM segment per worker. Within the segment a
`shm_toc` (table of contents) carries four keys:

| TOC key | Payload                | Producer  | Consumer | Notes                                                              |
| ------- | ---------------------- | --------- | -------- | ------------------------------------------------------------------ |
| **0**   | Fixed metadata block   | Launcher  | Worker   | Database OID, user OID, launcher PID, 64-bit cookie, error fields. |
| **1**   | SQL command text       | Launcher  | Worker   | Null-terminated UTF-8.                                              |
| **2**   | Serialized GUC snapshot| Launcher  | Worker   | Restored before SPI execution so the worker sees launcher GUCs.    |
| **3**   | `shm_mq`               | Both      | Both     | Bidirectional FE/BE protocol frames; sized by `queue_size` arg.    |

The default queue is 64 KiB (`pg_background.default_queue_size`); the floor is
4 KiB (the `shm_mq` minimum). The cap is 256 MiB. Larger queues reduce
producer blocking on big result sets at the cost of shared memory.

### Lifetime

* DSM is created in the launcher's resource owner during `launch_v2`.
* Worker `dsm_attach`es on startup. Worker detach (normal or crash) fires the
  on-detach callback in the launcher, which marks the hash entry's `consumed`
  flag and clears the queue handle.
* Launcher `dsm_detach`es on `detach_v2`, on `cleanup_worker_info`, or at
  session/transaction end via the resource owner cleanup hook. The handle is
  intentionally **not** `pfree()`d explicitly — fix landed in v1.6 to close a
  race where the launcher freed the handle while the worker was still
  attaching.

---

## 4. Worker startup and the error-propagation contract

`pg_background_worker_main` runs roughly this sequence:

1. `BackgroundWorkerUnblockSignals()`.
2. `dsm_attach(main_arg)` — failure here is fatal-but-silent because the
   shm_mq destination is not yet installed (see §4.1).
3. `shm_toc_lookup` for keys 0–3.
4. Re-derive `MyDatabaseId` / `GetUserIdAndSecContext` from key 0; call
   `BackgroundWorkerInitializeConnectionByOid`.
5. **Install the queue as a protocol destination** via
   `pq_redirect_to_shm_mq(seg, mqh)` — after this point any `ereport(ERROR)`
   that the worker raises will be serialized as an `'E'` ErrorResponse frame
   and read by the launcher.
6. Restore the launcher's GUCs from key 2.
7. `StartTransactionCommand()`, parse → analyze → plan via
   `pg_analyze_and_rewrite_compat` and `pg_plan_queries`.
8. Execute through SPI; each tuple becomes a `'D'` DataRow frame; each
   completed statement becomes a `'C'` CommandComplete with the command tag.
9. `CommitTransactionCommand()`.
10. On success: `ReadyForQuery(DestRemote)` + `pq_flush()` and exit cleanly.

### 4.1 The "early failure" window

Failures *before step 5* (DSM attach failure, missing TOC entry, OOM during
setup) cannot be serialized as an `'E'` frame because the queue is not yet
acting as the protocol destination. The launcher observes only the bgworker
exit and synthesises `SQLSTATE 08006 — lost connection to worker process`.

This is the **only** legitimate source of `08006` from v1.9 onward. The
extension explicitly reserves the code for infra-level failures; user SQL
errors (syntax, constraint violation, divide-by-zero, `RAISE EXCEPTION`,
cancel, etc.) propagate as their real SQLSTATE through the path below.

### 4.2 Structured error capture (v1.9)

A `PG_TRY` / `PG_CATCH` around the SPI execution copies `ErrorData` from a
caught `ereport(ERROR)` into the fixed-data block (key 0):

* `error_sqlstate` (5-char code)
* `error_message`
* `error_detail`
* `error_hint`
* `error_context`

The handler writes `error_sqlstate` **last**; it acts as a publish flag for
the launcher-side reader. After populating the block the worker still calls
`EmitErrorReport()` + `ReadyForQuery(DestRemote)` + `pq_flush()` so the
launcher observes the real `'E'` frame over `shm_mq` and can re-raise on
`result_v2`. The launcher's `pg_background_error_info_v2(pid, cookie)` reads
the same fields out of the DSM block and exposes them to PL/pgSQL — which is
why the supported diagnostic pattern is `launch → wait → error_info → detach`
(and **not** `result_v2`, which re-raises and aborts the current transaction
before `error_info_v2` can be inspected).

---

## 5. Launcher-side bookkeeping

Each launcher session maintains a private hash table (the *worker info*
registry) keyed by `(pid, cookie)`:

```c
typedef struct pg_background_worker_info {
    int                       pid;
    uint64                    cookie;       /* 64-bit random, generated at launch */
    dsm_segment              *seg;
    BackgroundWorkerHandle   *handle;
    shm_mq_handle            *responseq;
    bool                      consumed;     /* result_v2 may run once */
    /* + state/last_error/launched_at/user_id/sql_preview for list_v2 */
} pg_background_worker_info;
```

Properties worth noting:

* The hash table is **session-local backend memory**, not shared. This is why
  `list_v2()` only shows workers launched by the current session (Known
  Limitation #7).
* The cookie is generated with a CSPRNG (v1.7 hardened this from `random()`).
  All operations validate the `(pid, cookie)` tuple, which is what prevents
  PID-reuse confusion (Known Limitation / Security note).
* Cleanup runs from three sources:
  * `cleanup_worker_info` on-detach callback (worker exit triggers it).
  * Explicit `detach_v2` / `detach_all_v2`.
  * Resource owner release at session/transaction end.
* `consumed = true` is set when `result_v2` drains the queue, which is why a
  second call raises `results already consumed for worker PID N`.

### 5.1 GUC-enforced per-session caps (v1.8)

`pg_background.max_workers` (default 16, range 1 – 1000) is checked **inside
the launcher** before `RegisterDynamicBackgroundWorker`. The count is the
number of live entries in the per-session hash, so callers that diligently
`detach_v2` after consumption recover capacity immediately. There is
deliberately no cluster-wide cap inside the extension — the global ceiling
remains PostgreSQL's `max_worker_processes`.

---

## 6. Cancel vs Detach — the most-confused distinction

This pair is the single most common source of production bugs with this
extension and is worth restating in architectural terms:

| Operation        | Effect on worker process        | Effect on launcher bookkeeping | Side-effects (NOTIFY, INSERTs, COMMIT) |
| ---------------- | ------------------------------- | ------------------------------ | -------------------------------------- |
| `cancel_v2`      | `SIGINT` to worker (Unix)       | Hash entry kept (state=canceled)| Prevented if cancel beats commit       |
| `cancel_v2_grace`| `SIGINT` after `grace_ms` (≤1 h)| Hash entry kept                | Prevented if cancel beats commit       |
| `detach_v2`      | **None**                        | Hash entry removed             | Worker still runs to completion        |

`detach_v2` is a **launcher-side** operation only. The worker has no idea it
happened. It will still commit its transaction and still deliver any
`pg_notify` it issues. To actually stop work you need `cancel_v2`.

### Windows caveat

The Windows back-end has no signal infrastructure equivalent to `SIGINT`
delivery from another process. `cancel_v2` on Windows only sets
`InterruptPending`, which is checked at `CHECK_FOR_INTERRUPTS` points (between
statements and inside cooperative loops like `pg_sleep`). A CPU-bound C
function or a tight PL/pgSQL loop without yield points will **not** be
cancellable. The mitigation baked into the README is "always set
`statement_timeout` on Windows".

---

## 7. Concurrency / race-condition hardening history

The extension's race history is well-documented in the source comments. The
ones with the largest architectural impact:

| Race                                      | Solved in | Fix                                                                                       |
| ----------------------------------------- | --------- | ----------------------------------------------------------------------------------------- |
| NOTIFY lost when launcher returned before worker attached the queue. | v1.5    | `shm_mq_wait_for_attach()` blocks `launch_v2` until the worker is fully attached.         |
| PID reuse confusing `result`/`detach`/`cancel` with the wrong worker. | v2 API (1.6) | 64-bit random cookie embedded in handle; validated on every op.                          |
| `pfree(handle)` while worker still attaching → crash.                | v1.6    | Stopped freeing the handle explicitly; rely on DSM detach + resource owner cleanup.       |
| Synthetic `08006` masking real SQLSTATEs in user error handlers.      | v1.9.3  | Structured `ErrorData` capture into the DSM fixed-data block + `EmitErrorReport` on the queue. |
| Backend cookie generated by weak `random()`.                          | v1.7    | Switched to a cryptographic source.                                                       |
| Worker-info hash growing across long sessions.                        | v1.7    | Dedicated memory context with eager cleanup on detach.                                    |

---

## 8. Security model

### Roles and grants

* `pgbackground_role` is created `NOLOGIN INHERIT` by the install script. All
  pg_background functions are granted to **only** that role — there is **no**
  PUBLIC grant anywhere.
* `grant_pg_background_privileges(role, include_helpers bool)` /
  `revoke_pg_background_privileges(...)` are `SECURITY DEFINER` with pinned
  `search_path = pg_catalog`, and they emit explicit `EXECUTE` grants. They
  use a dynamic schema lookup (`@extschema@` was removed in v1.7) so that
  `CREATE EXTENSION ... WITH SCHEMA foo` works.

### Privilege flow into the worker

The worker runs as the **launcher's `current_user`**, not as superuser. This
is enforced because `BackgroundWorkerInitializeConnectionByOid` uses the user
OID stored in TOC key 0 by the launcher.

### Information disclosure surface

`list_v2()` returns `sql_preview` (first 120 chars of the SQL) and
`last_error` for every worker in the **current session only**. The README
documents the pattern of wrapping it in a SECURITY DEFINER view that filters
`user_id = current_user::regrole::oid` and hides the preview/error columns
for multi-tenant deployments.

### SQL-injection responsibility is the caller's

The extension is a transport for whatever SQL string it is handed. All
sanitisation must happen in the calling PL/pgSQL — the established idiom is
`format('VACUUM %I', table_name)` / `format(... %L, value)`.

---

## 9. Build, packaging, and PG-version compatibility

`pg_background.h` is the compatibility surface. It currently bridges 14 → 18:

| Concern                          | Handling                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------- |
| `TupleDescAttr`                  | Polyfilled on `PG_VERSION_NUM < 180000`; provided by core on 18+.                                |
| `shm_toc_lookup`                 | Stable 3-arg signature since PG 10; thin alias `shm_toc_lookup_compat`.                          |
| Command-tag / `CommandTag` enum  | PG 13 changed `CreateCommandTag` to take `Node *` and introduced `QueryCompletion`; wrapped.     |
| `pg_analyze_and_rewrite`         | PG 15 renamed to `pg_analyze_and_rewrite_fixedparams`; wrapped behind `pg_analyze_and_rewrite_compat`. |
| `set_ps_display`, `BeginCommand`, `EndCommand` | Trivial wrappers to keep the worker main readable.                                  |

The control file is minimal:

```ini
# pg_background.control
comment           = 'Run SQL queries in the background'
default_version   = '1.9'
module_pathname   = '$libdir/pg_background'
relocatable       = true
```

`relocatable = true` is genuine — install SQL scripts no longer hardcode
`public.` prefixes (v1.7+), and helper functions resolve the extension's
schema dynamically.

### CI matrix

GitHub Actions runs:

* `test` — PostgreSQL 14 – 18 × Ubuntu 22.04 / 24.04, `make installcheck`.
* `relocatable-test` — installs into a non-`public` schema on PG 17.
* `upgrade-test` — exercises the 1.8 → 1.9 upgrade script chain.
* `lint` — `cppcheck` + `clang-format`.
* `security` — CodeQL static analysis.

All gates must pass to merge.

---

## 10. Observability surface

| Surface                           | Scope            | Notes                                                            |
| --------------------------------- | ---------------- | ---------------------------------------------------------------- |
| `pg_background_list_v2()`         | Session          | Per-worker state, queue size, launched_at, sql_preview, last_error, consumed. |
| `pg_background_stats_v2()`        | Session          | Counters: launched / completed / failed / active, avg ms, max_workers. (v1.8) |
| `pg_background_get_progress_v2()` | Per-worker       | Reads progress percentage + message written by worker calls to `pg_background_progress(pct, msg)`. (v1.8) |
| `pg_background_result_info_v2()`  | Per-worker       | `row_count`, `command_tag`, `completed`, `has_error`. Non-consuming. (v1.9) |
| `pg_background_error_info_v2()`   | Per-worker       | Structured `sqlstate/message/detail/hint/context`. Non-consuming. (v1.9) |
| `pg_stat_activity`                | Cluster          | Workers appear with `backend_type LIKE '%background%'`. Cross-session visibility lives here. |
| `pg_shmem_allocations`            | Cluster          | DSM segments allocated for each live worker.                    |

The deliberate split is: **per-session, app-facing** lives in `*_v2()`
functions backed by the launcher hash; **cluster-wide, ops-facing** lives in
the standard `pg_stat_activity` / `pg_shmem_allocations` views.

---

## 11. Known architectural limits (summary)

These are intrinsic to the design, not bugs:

1. **One database per worker.** `BackgroundWorker` requires a database OID at
   registration; cross-DB work needs `dblink` inside the worker SQL.
2. **No cross-session worker registry.** The worker-info hash is backend-local;
   `list_v2` cannot see another session's workers — use `pg_stat_activity`.
3. **One-shot result consumption.** `result_v2` drains `shm_mq`; there is no
   cursor/pagination layer. Repeat reads require `CREATE TEMP TABLE ... AS`.
4. **No transaction pinning / 2PC.** Worker transactions are fully autonomous
   by design.
5. **`max_worker_processes` is a hard cluster ceiling.** Worker exhaustion
   raises `INSUFFICIENT_RESOURCES`; the recommended pattern is exponential
   backoff with a synchronous fallback (see README §"Robust audit logging").
6. **Windows cancel is best-effort.** No signal delivery from another process;
   `statement_timeout` is mandatory in worker SQL on Windows.
7. **Early-startup failures cannot return a real SQLSTATE.** Anything before
   `pq_redirect_to_shm_mq` surfaces as `08006`; see §4.1.

---

## 12. Mental model for new readers

If you are reading `pg_background.c` for the first time, hold this in your
head:

1. *The DSM segment is the contract.* Everything launcher and worker share —
   metadata, SQL, GUCs, the result queue, the captured error block — is
   keyed in the same TOC. Lifetime is governed by `dsm_detach`.
2. *The shm_mq becomes a protocol destination.* After
   `pq_redirect_to_shm_mq`, the worker is for all practical purposes talking
   the FE/BE protocol to a peer that happens to be in shared memory rather
   than a TCP socket. That is why `EmitErrorReport`, `ReadyForQuery`, and
   `pq_flush` work unmodified.
3. *The cookie is the only protection against PID reuse.* Treat `(pid,
   cookie)` as the canonical handle; bare PIDs are an artefact of the v1
   compatibility layer.
4. *Cancel acts on the worker; detach acts on the launcher.* Most production
   incidents traced to this extension come from confusing the two.
5. *Autonomy is total.* The worker's transaction is wholly independent. There
   is no two-phase commit, no joined snapshot, no shared resource owner.

---

## References

* Source: [vibhorkum/pg_background](https://github.com/vibhorkum/pg_background)
* README & API reference: [README.md](https://github.com/vibhorkum/pg_background/blob/master/README.md)
* PostgreSQL bgworker API: <https://www.postgresql.org/docs/current/bgworker.html>
* `shm_mq` reference: `src/backend/storage/ipc/shm_mq.c` in the PostgreSQL tree
* `pq_redirect_to_shm_mq`: `src/backend/libpq/pqmq.c`
