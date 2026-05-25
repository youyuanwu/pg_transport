# 2026-05-24 — Audit: pg_transport's `exec_simple_query` reimplementation

> **Scope.** Verifies the claims made in
> [`/home/user1/code/postgres/extdocs/background/pg_code.md`](../../../../../postgres/extdocs/background/pg_code.md)
> against PG 18 (`REL_18_STABLE` at `/home/user1/code/postgres`)
> and pg_transport (this repo) as of `dev@2d6f71d`. The source
> doc focuses on the **direct backend**
> ([`crates/core/src/backend/simple_direct.rs`](../../../crates/core/src/backend/simple_direct.rs));
> this audit additionally checks whether the **SPI backend**
> ([`crates/core/src/backend/spi_bridge.rs`](../../../crates/core/src/backend/spi_bridge.rs))
> shares the same gaps. Every claim was verified by reading
> the cited line ranges on disk, not from prior knowledge.

## 1. Verdict at a glance

The source doc is **accurate**. Every "semantic difference" claim
in §2.2 reproduces on the current `dev` branch; every
"observability gap" in §2.3–§2.4 reproduces; the performance
mechanics in §3.1 match the code as written. No false positives
were found.

The **SPI backend inherits every semantic gap from the direct
backend** (it goes through the same `with_*` per-statement xact
bracket and the same hand-rolled `parse_and_classify` splitter)
and adds two of its own. The framing "direct is less featureful
than vanilla `exec_simple_query`" applies equally to SPI — both
backends diverge from `exec_simple_query` in the same load-bearing
places.

## 2. Verification table — direct backend

Direct-path claims from `pg_code.md` §2.2 / §2.3:

| § | Claim | Verified? | Evidence |
| --- | --- | --- | --- |
| 2.2.1 | Per-statement `StartTransactionCommand` + `CommitTransactionCommand` instead of one outer xact + per-iter `CommandCounterIncrement` | ✓ confirmed | [`executor.rs:90-138`](../../../crates/core/src/backend/executor.rs#L90-L138) (`with_xact` does full Start/Push/Pop/Commit); [`simple_direct.rs:226-237`](../../../crates/core/src/backend/simple_direct.rs#L226-L237) (loop calls `with_xact(...)` per statement) vs. [`postgres/src/backend/tcop/postgres.c:1062`](../../../../../postgres/src/backend/tcop/postgres.c#L1062) (one outer `start_xact_command()`) + [`:1159`](../../../../../postgres/src/backend/tcop/postgres.c#L1159) (per-iter `start_xact_command()` becomes CCI) + [`:1328-1336`](../../../../../postgres/src/backend/tcop/postgres.c#L1328-L1336) (`CommandCounterIncrement()`+`disable_statement_timeout()` between statements). |
| 2.2.2 | `BeginImplicitTransactionBlock` / `EndImplicitTransactionBlock` is absent | ✓ confirmed | `rg "BeginImplicitTransactionBlock\|EndImplicitTransactionBlock" crates/` ⇒ **zero matches**. Vanilla calls both at [`postgres.c:1169`](../../../../../postgres/src/backend/tcop/postgres.c#L1169) and [`:1313`](../../../../../postgres/src/backend/tcop/postgres.c#L1313). |
| 2.2.3 | FETCH-binary cursor format silently dropped | ✓ confirmed | [`simple_direct.rs:318-322`](../../../crates/core/src/backend/simple_direct.rs#L318-L322) calls `schema_and_encoders_text` unconditionally; [`Portal::start`](../../../crates/core/src/backend/executor.rs#L677-L680) takes no format vector and never calls `PortalSetResultFormat`. `rg "PortalSetResultFormat" crates/` ⇒ zero matches. Vanilla checks `IsA(parsetree->stmt, FetchStmt)` and flips `format = 1` for binary cursors at [`postgres.c:1259-1273`](../../../../../postgres/src/backend/tcop/postgres.c#L1259-L1273). |
| 2.2.4 | Missing `IsAbortedTransactionBlockState()` rejection (`25P02`) | ✓ confirmed | `rg "IsAbortedTransactionBlockState\|ERRCODE_IN_FAILED_SQL_TRANSACTION" crates/` ⇒ zero matches. Direct's per-statement Start/Commit cannot *enter* `TBLOCK_ABORT` between statements in one `'Q'`, but an extended-query path can park the session there; the next simple-query statement is unguarded. Vanilla rejects at [`postgres.c:1150-1156`](../../../../../postgres/src/backend/tcop/postgres.c#L1150-L1156). |
| 2.2.5 | `analyze_requires_snapshot` not consulted; unconditional snapshot push | ✓ confirmed | [`executor.rs:96-98`](../../../crates/core/src/backend/executor.rs#L96-L98) unconditionally `PushActiveSnapshot(GetTransactionSnapshot())`. `rg "analyze_requires_snapshot" crates/` ⇒ zero matches. Vanilla gates at [`postgres.c:1177-1181`](../../../../../postgres/src/backend/tcop/postgres.c#L1177-L1181). |
| 2.2.6 | `PortalStart` snapshot arg differs (active vs. `InvalidSnapshot`) | ✓ confirmed | [`simple_direct.rs:325`](../../../crates/core/src/backend/simple_direct.rs#L325) passes `pg_sys::GetActiveSnapshot()`. Vanilla passes `InvalidSnapshot` at [`postgres.c:1252`](../../../../../postgres/src/backend/tcop/postgres.c#L1252). |

Observability / GUC claims from §2.3–§2.4 — all verified with one
grep:

```bash
$ rg "debug_query_string|pgstat_report_activity|statement_timeout\
       |set_ps_display|check_log_statement|check_log_duration\
       |TRACE_POSTGRESQL_QUERY|pgstat_report_query_id|BeginCommand\
       |EndCommand|NullCommand|drop_unnamed_stmt" crates/
(no matches)
```

Every one of the 13 vanilla observability call sites
([`postgres.c:1046`](../../../../../postgres/src/backend/tcop/postgres.c#L1046),
[`:1048`](../../../../../postgres/src/backend/tcop/postgres.c#L1048),
[`:1071`](../../../../../postgres/src/backend/tcop/postgres.c#L1071),
[`:1085`](../../../../../postgres/src/backend/tcop/postgres.c#L1085),
[`:1131-1132`](../../../../../postgres/src/backend/tcop/postgres.c#L1131-L1132),
[`:1139`](../../../../../postgres/src/backend/tcop/postgres.c#L1139),
[`:1141`](../../../../../postgres/src/backend/tcop/postgres.c#L1141),
[`:1322`](../../../../../postgres/src/backend/tcop/postgres.c#L1322),
[`:1336`](../../../../../postgres/src/backend/tcop/postgres.c#L1336),
[`:1342`](../../../../../postgres/src/backend/tcop/postgres.c#L1342),
[`:1346`](../../../../../postgres/src/backend/tcop/postgres.c#L1346),
[`TRACE_POSTGRESQL_QUERY_START`/`_DONE`](../../../../../postgres/src/backend/tcop/postgres.c#L1050))
is genuinely missing. The doc's claim that
`log_statement` / `log_duration` / `statement_timeout` /
`pg_stat_activity.query` / `pg_stat_statements.query_id` /
process-title / DTrace probes are all **inactive** for direct
queries is correct.

## 3. Performance-shape claims (§3.1 of the source doc)

Spot-checked the four hot-path claims:

| § | Claim | Verified? | Evidence |
| --- | --- | --- | --- |
| 3.1.1 | Per-cell `OidOutputFunctionCall` (syscache hit per cell × row) vs. vanilla's cached `FmgrInfo` | ✓ confirmed | [`dest_receiver.rs:170-182`](../../../crates/core/src/backend/dest_receiver.rs#L170-L182) calls `pg_sys::OidOutputFunctionCall(fns.typoutput, datum)` in the hot row loop; `TypeOutput` caches only the OID. Vanilla's [`printtup_prepare_info`](../../../../../postgres/src/backend/access/common/printtup.c#L251) caches a full `FmgrInfo[]` once per portal and dispatches via `OutputFunctionCall(&thisState->finfo, ...)` at [`printtup.c:361`](../../../../../postgres/src/backend/access/common/printtup.c#L361). |
| 3.1.2 | Fresh `BytesMut::with_capacity(64)` per row vs. reused `StringInfo` | ✓ confirmed | [`dest_receiver.rs:106`](../../../crates/core/src/backend/dest_receiver.rs#L106) allocates per row. Vanilla's `initStringInfo(&myState->buf)` runs once per portal at [`printtup.c:122`](../../../../../postgres/src/backend/access/common/printtup.c#L122), then `pq_beginmessage_reuse` / `pq_endmessage_reuse` at [`:327`](../../../../../postgres/src/backend/access/common/printtup.c#L327) recycle it. |
| 3.1.3 | Per-cell `pfree` vs. bulk `MemoryContextReset` | ✓ confirmed | [`dest_receiver.rs:181`](../../../crates/core/src/backend/dest_receiver.rs#L181) `pg_sys::pfree(ptr as *mut _)` per cell. Vanilla uses a dedicated `tmpcontext` (allocated at [`printtup.c:128`](../../../../../postgres/src/backend/access/common/printtup.c#L128)) and `MemoryContextReset` at end-of-row ([`printtup.c:381`](../../../../../postgres/src/backend/access/common/printtup.c#L381)). |
| 3.1.4 / 3.1.5 | `Vec<DataRow>` intermediate; per-`'Q'` `Vec<FieldInfo>` build | ✓ confirmed | [`simple_direct.rs:328-360`](../../../crates/core/src/backend/simple_direct.rs#L328-L360) materializes `Vec<DataRow>` then `stream::iter(data_rows).map(Ok)`. [`dest_receiver.rs:230-245`](../../../crates/core/src/backend/dest_receiver.rs#L230-L245) builds `Vec<FieldInfo>` per `'Q'` (no per-slot schema cache). |

The reviewed-and-corrected "memcpy / TLS" framing in §3.1.4 of the
source doc (one extra `Vec<DataRow>` → output-`BytesMut` memcpy on
the pg_transport side; OpenSSL and rustls each do one symmetric
record-encryption pass) matches what is in the code. The original
draft's "two memcpys vs. many" framing — now corrected in the source
doc — was wrong; the current text is what the audit confirms.

## 4. SPI backend — does it share the same gaps?

The source doc deliberately scopes itself to `direct`. The audit
extends the comparison to the SPI bridge.

### 4.1 SPI inherits every direct-path gap

| Gap | SPI status |
| --- | --- |
| Per-statement `Start` + `Commit` | Same. [`spi_bridge.rs:103-108`](../../../crates/core/src/backend/spi_bridge.rs#L103-L108) — `with_spi` per non-xact-control statement; `with_spi`'s prologue/epilogue ([`spi.rs:113-150`](../../../crates/core/src/backend/spi.rs#L113-L150), per code references in `executor.rs`) is the same `Start + Push + ... + Pop + Commit` shape as `with_xact`. |
| No `BeginImplicitTransactionBlock` | Same. Confirmed by the same grep as §2 above. |
| FETCH-binary cursor format dropped | Same — text-format only by construction. The module-level comment at [`spi_bridge.rs:8-9`](../../../crates/core/src/backend/spi_bridge.rs#L8-L9) explicitly says "Phase 4b: text-format only." |
| No `IsAbortedTransactionBlockState` guard | Same. |
| No `analyze_requires_snapshot` gating | Same — `with_spi` unconditionally pushes a snapshot. |
| All observability calls missing | Same — `debug_query_string` / `pgstat_*` / `set_ps_display` / `check_log_*` are absent in *all* backend code, not just direct. |

### 4.2 SPI-specific divergences from `exec_simple_query`

Two additional behaviours that the direct path doesn't have, both
documented in code:

1. **xact-control statements are intercepted before SPI sees them.**
   [`spi_bridge.rs:303-376`](../../../crates/core/src/backend/spi_bridge.rs#L303-L376)'s
   `handle_xact_control_one` routes `BEGIN` / `COMMIT` / `ROLLBACK`
   through PG's xact-block API directly, because `SPI_execute` in
   atomic mode rejects xact-control with `SPI_ERROR_TRANSACTION`.
   Vanilla `exec_simple_query` has no such interception — it just
   lets `start_xact_command` / `finish_xact_command` cooperate
   naturally because there's no SPI in the path.

2. **`WARNING: there is already a transaction in progress` is
   suppressed.** [`spi_bridge.rs:328-330`](../../../crates/core/src/backend/spi_bridge.rs#L328-L330):
   for nested `BEGIN` and out-of-block `COMMIT`/`ROLLBACK`, the
   framework returns the canonical CommandTag but never emits the
   warning vanilla would. Documented in the same code comment.

3. **Abort recovery collapses to `DEFAULT` rather than `TBLOCK_ABORT`.**
   Documented at [`spi_bridge.rs:96-99`](../../../crates/core/src/backend/spi_bridge.rs#L96-L99):
   on a panic, `with_spi`'s `AbortCurrentTransaction` reaches
   `DEFAULT`, so the next statement runs in a fresh auto-commit
   context rather than forcing the client to issue `ROLLBACK`.
   Vanilla's machinery would park the session in `TBLOCK_ABORT`.
   This is a third semantic deviation specific to SPI (the direct
   path's `with_xact` has the same `AbortCurrentTransaction`
   shape, but `simple_direct.rs` doesn't open a transaction that
   could outlive the statement, so the visibility window for
   client-observed `TBLOCK_ABORT` doesn't exist on direct either).

### 4.3 SPI-vs-direct verdict

Functionally, **SPI and direct are co-equally non-conformant** with
`exec_simple_query`. They share the same six semantic gaps plus the
same observability/GUC blackout. SPI carries three extra
SPI-specific deviations (xact-control re-routing, warning
suppression, abort-state collapse) that direct sidesteps because it
goes through `ProcessUtility` / `PortalRun` natively.

A user who *only* cares about row results from a SELECT against a
session that never uses `statement_timeout`, multi-statement `'Q'`,
binary cursors, `log_statement`, `pg_stat_statements`, or
`pg_stat_activity` will see the same answer on direct, SPI, and
vanilla. Outside that envelope, both pg_transport backends will
deviate identically.

## 4.4 Status update — 2026-05-25

Six of the nine §6 punch-list items have landed since the audit
was written; the verification tables above describe the **pre-fix**
state. For the post-fix state, see the per-item Status column in
[§6 Suggested follow-ups](#6-suggested-follow-ups). Summary:

| # | Item | Status | Commits |
| --- | --- | --- | --- |
| 1 | FETCH-binary cursor format | **CLOSED** | `b0dae62` (direct) + `a1802ae` (SPI) |
| 2 | Implicit-block multi-statement | **CLOSED** | `0b41355` (direct) + `f508f61` (SPI) |
| 3 | Aborted-block rejection (`25P02`) | **CLOSED** | `409915c` |
| 4 | `analyze_requires_snapshot` gating | open | — |
| 5 | `debug_query_string` + pgstat | **CLOSED** | `97f1b1e` (direct) + `20cfb97` (extended + SPI) |
| 6 | `statement_timeout` | **CLOSED** | `2e78127` |
| 7 | `pg_stat_statements` query_id | open | — |
| 8 | Cache `FmgrInfo` (perf) | **CLOSED** | `245ad37` |
| 9 | Per-row buffer reuse (perf) | open | — |

One audit-doc correction emerged from the §6.3 work and is folded
into the status section there: the claim that
`AbortCurrentTransaction` "collapses state to `DEFAULT`" was
**wrong** for the explicit-`BEGIN` case (it moves to `TBLOCK_ABORT`),
which made §6.3 a stability gap, not just a spec-deviation gap.
See [§6 item 3 Status](#6-suggested-follow-ups) for the full
correction.

## 5. Documentation drift

The doc-comment on `execute_simple_query_direct` at
[`simple_direct.rs:212`](../../../crates/core/src/backend/simple_direct.rs#L212)
reads "*Same semantics as `super::spi_bridge::execute_simple_query`*",
which is true *internally* (the two backends really do match each
other). The source doc points out that the same comment can be
read as "same semantics as PG's `exec_simple_query`", which is
**not** true. The comment is technically correct but misleading;
follow-up to consider:

- Rewording the doc-comment to "Same semantics as the SPI bridge,
  which differs from vanilla `exec_simple_query` in the ways
  documented at [pg_code.md §2.2](../../../../../postgres/extdocs/background/pg_code.md)."
- Or fixing the deviations and removing the qualifier.

**Status update — 2026-05-25.** The deviation list has shrunk
substantially: §6.1, §6.2, §6.3, §6.5, §6.6, §6.8 are all closed
across the §6.2 + §6.3 commits. The remaining qualifier should
now read along the lines of "Same semantics as the SPI bridge;
both still differ from vanilla in §6.4, §6.7, §6.9 (snapshot
gating, `pg_stat_statements` attribution, per-row buffer reuse)."
The comment hasn't been rewritten in code yet — folded into the
§6.7 pickup.

## 6. Suggested follow-ups

Ordered by correctness-impact then engineering cost. Each item
carries a **Status** block: `CLOSED` items cite the commit that
landed them; `open` items are still as written.

1. **FETCH-binary cursor format (§2.2.3).** Local fix:
   `simple_direct.rs` (and the extended-direct path) should
   mirror vanilla's `IsA(stmt, FetchStmt)` check, call
   `GetPortalByName`, and pass the resulting format vector
   through to `PortalSetResultFormat`. The encoder selector
   would then need a binary branch (`schema_and_encoders_binary`
   exists in spirit — the `Binary` variant of `ColumnEncoder` is
   already wired up at [`dest_receiver.rs:185-196`](../../../crates/core/src/backend/dest_receiver.rs#L185-L196)).

   **Status — CLOSED (`b0dae62` + `a1802ae`).** Direct path
   added `fetch_result_format(raw_stmt)` to inspect the
   parsetree and consult `GetPortalByName` +
   `CURSOR_OPT_BINARY` before encoder construction. SPI bridge
   path piggybacks on the existing `parse_and_classify` pass:
   `StmtMeta { xact, fetch_portalname }` stashes the cursor
   name as a Rust-owned `CString` so `run_via_spi` makes the
   same lookup without a second parse pass.

2. **Implicit-block semantics for multi-statement `'Q'` (§2.2.2).**
   Wrap the per-statement loop in a single
   `BeginImplicitTransactionBlock` / `EndImplicitTransactionBlock`
   pair when `parsed.statements.len() > 1`, and replace the
   per-iter `with_xact` with one outer `StartTransactionCommand`
   + per-iter `CommandCounterIncrement`. Mirrors vanilla
   [`postgres.c:1097` + `:1167-1170` + `:1310`](../../../../../postgres/src/backend/tcop/postgres.c#L1097).
   Touches `executor.rs::with_xact` (split into "open" / "step"
   / "close" primitives) and `simple_direct.rs`'s statement loop.

   **Status — CLOSED (`0b41355` + `f508f61`).** Direct path
   landed `execute_with_implicit_block` (commit `0b41355`): a
   single outer `catch_unwind` over a loop that does per-iter
   `Start + BeginImplicit + Push + run_one_direct + Pop` and
   selects the tail (`EndImplicit + Commit` last vs. `Commit`
   for mid-batch xact-stmt vs. `CCI` otherwise). SPI bridge
   followed in commit `f508f61` with `execute_with_implicit_block_spi`,
   which can't reuse `with_spi` (nested `catch_unwind` would
   collapse the implicit block on first ERROR); instead it
   inlines `SPI_connect` / `SPI_finish` per iter under a single
   outer `catch_unwind`. Mixed batches containing xact-control
   fall back to the per-statement path on the SPI side (where
   `handle_xact_control`'s xact-block API would fight an outer
   implicit block). e2e regression: both
   `implicit_block_rolls_back_partial_*_backend` tests now
   assert `count == "0"` after `INSERT VALUES (1); SELECT 1/0`
   in one `'Q'`.

3. **Aborted-block rejection (§2.2.4).** Cheap: call
   `pg_sys::IsAbortedTransactionBlockState` at the top of
   `run_one_direct`, and `ereport(ERROR, ERRCODE_IN_FAILED_SQL_TRANSACTION, ...)`
   (or build the `PgWireError` directly with SQLSTATE `25P02`) for
   non-`TransactionStmt` parsetrees. Same delta needed in
   `run_spi_statement`.

   **Status — CLOSED (`409915c`); audit-doc claim revised.**
   The original audit's framing was that this was a spec
   deviation:
   `AbortCurrentTransaction` (per §4.2 item 3) was said to
   "collapse state to `DEFAULT`, so subsequent statements run
   as if in a fresh auto-commit context — incorrect in spec
   terms but harmless". **That framing is wrong** for the
   explicit-`BEGIN` case. `AbortCurrentTransaction` from
   `TBLOCK_INPROGRESS` actually moves to `TBLOCK_ABORT` (not
   `DEFAULT` — see PG `xact.c:AbortCurrentTransaction`), and
   our `with_xact` / `with_spi` then unconditionally call
   `PushActiveSnapshot(GetTransactionSnapshot())` on the next
   `'Q'`, which assert-fires from `TBLOCK_ABORT` and `SIGABRT`s
   the slot. So §6.3 was a *stability* gap, not just a spec
   gap. The fix adds the
   `IsAbortedTransactionBlockState() && !is_*_exit_stmt(...)`
   check at the top of each backend's per-statement dispatch
   (mirroring vanilla `IsTransactionExitStmt`: COMMIT /
   ROLLBACK / PREPARE / ROLLBACK_TO are allowed-through; the
   SPI bridge only currently classifies COMMIT / ROLLBACK as
   exits and is slightly over-strict for PREPARE / ROLLBACK_TO).
   Companion change: `reset_per_handoff_state` now calls
   `AbortOutOfAnyTransaction()` before `ResetAllOptions()`, so a
   client that disconnects mid-aborted-block no longer leaks
   xact state onto the next handoff's first statement. e2e
   regression: both `aborted_block_*_backend_returns_25p02`
   tests in `crates/e2e/tests/regressions.rs`.

   **Known limitation pending §6.4.** A real-client wire-level
   `ROLLBACK` from `TBLOCK_ABORT` on the **direct** backend
   still crashes — `with_xact`'s unconditional snapshot Push is
   what asserts. The fix needs §6.4 (`analyze_requires_snapshot`
   gating in `with_xact`) plus a `run_one_direct` refactor to
   pass `InvalidSnapshot` to `PortalStart` like vanilla
   ([`postgres.c:1252`](../../../../../postgres/src/backend/tcop/postgres.c#L1252)).
   The tests sidestep this by relying on the per-handoff
   `AbortOutOfAnyTransaction` instead of issuing wire-level
   `ROLLBACK`. SPI bridge does not have the same crash because
   `handle_xact_control` doesn't push a snapshot for COMMIT /
   ROLLBACK.

4. **`analyze_requires_snapshot` gating (§2.2.5).** Easy.
   `with_xact` becomes
   `with_xact(needs_snapshot: bool, body)`; callers compute the
   bool from `pg_sys::analyze_requires_snapshot(raw_stmt)` and
   pass it through. Saves a `GetTransactionSnapshot` per
   utility-statement `'Q'`.

   **Status — open. Promoted to a correctness gap by §6.3.**
   Originally an "easy" perf win; now also required to make
   wire-level `ROLLBACK` from `TBLOCK_ABORT` on the direct
   backend work (see §6.3 Status above). The minimal
   `with_xact(needs_snapshot, body)` signature change is small,
   but `run_one_direct` also needs to be updated to pass
   `InvalidSnapshot` to `PortalStart` when the parse-time
   snapshot wasn't pushed (mirroring vanilla
   [`postgres.c:1252`](../../../../../postgres/src/backend/tcop/postgres.c#L1252)) —
   currently `run_one_direct` calls
   `portal.start(..., pg_sys::GetActiveSnapshot())`, which
   returns garbage / asserts when no Push happened. Tried in
   commit `409915c` and broke 16+ tests on
   non-`TransactionStmt` utility statements (e.g. `DROP TABLE`);
   reverted in favour of the per-handoff cleanup workaround.

5. **`pg_stat_activity.query` + `debug_query_string` (§2.3 row 1–2).**
   Single point of insertion at the top of
   `execute_simple_query_direct` / `execute_simple_query`:

   ```rust
   pg_sys::debug_query_string = sql_cstr.as_ptr();
   pg_sys::pgstat_report_activity(STATE_RUNNING, sql_cstr.as_ptr());
   // ... query body ...
   pg_sys::pgstat_report_activity(STATE_IDLE, std::ptr::null());
   pg_sys::debug_query_string = std::ptr::null();
   ```

   Restores observability for the two columns operators reach for
   first.

   **Status — CLOSED (`97f1b1e` + `20cfb97`).** Direct backend
   landed `DebugQueryGuard` RAII in `backend/observability.rs`
   (commit `97f1b1e`); follow-up `20cfb97` installed it in the
   other three backend entry points (`spi_bridge::execute_simple_query`,
   `extended::prepare`, `extended::PreparedStatement::execute`).
   Guard pins `debug_query_string` and
   `pgstat_report_activity(STATE_RUNNING, ...)` for the lifetime
   of the query body; Drop restores the previous global and
   reports `STATE_IDLE`. Tests probe via `ExecutorStart_hook`.

6. **`statement_timeout` (§2.4).** Wrap each statement in
   `enable_statement_timeout()` / `disable_statement_timeout()`
   when the GUC is set. Requires care around the
   `disable_statement_timeout` between intermediate statements
   that vanilla calls out at [`postgres.c:1336`](../../../../../postgres/src/backend/tcop/postgres.c#L1336).

   **Status — CLOSED (`2e78127`).** Added `StatementTimeoutGuard`
   in `backend/observability.rs` (manual FFI bindings for
   `enable_timeout_after` / `disable_timeout` / `get_timeout_active`
   and `STATEMENT_TIMEOUT = 3`). Installed in all four backend
   entry points alongside `DebugQueryGuard`. Test uses a 60s
   timeout + SQL-callable probe
   (`tests.pg_transport_statement_timeout_is_active()`) to avoid
   the pg_test xact-corruption pitfall.

7. **`pg_stat_statements` attribution (§2.4).** Call
   `pgstat_report_query_id(0, true)` + `pgstat_report_plan_id(0, true)`
   per statement. Restores per-statement bucketing in
   `pg_stat_statements`.

   **Status — open.** Single point of insertion (per-statement
   in each backend's dispatch loop); the FFI surface is
   straightforward. No correctness dependency on §6.4. Suggested
   pick-up next.

8. **Performance — cache `FmgrInfo` in `ColumnEncoder` (§3.1.1).**
   Highest perf payoff per LoC. Replace `TypeOutput { Oid }` with
   `TypeOutput { FmgrInfo }` resolved once at `for_column` time;
   the hot row loop becomes `OutputFunctionCall(&fns.finfo, datum)`,
   matching vanilla's call shape exactly.

   **Status — CLOSED (`245ad37`).** `TypeOutput` /
   `TypeSend` now cache `FmgrInfo` instead of just the Oid; the
   hot loop calls `OutputFunctionCall(&finfo, datum)`. Matches
   vanilla's `printtup` shape.

9. **Per-row buffer reuse (§3.1.2).** Hold a per-receiver
   `BytesMut` cursor across `receiveSlot` invocations and reset
   rather than `with_capacity` per row. May require coordinating
   with pgwire's `DataRow` ownership.

   **Status — open.** Largest deferred perf item; touches
   pgwire `DataRow` ownership semantics. Likely needs design
   discussion before a code change.

Items 1–4 (correctness) and item 5 (observability headline)
are the smallest deltas that bring the two backends substantially
closer to "same semantics as `exec_simple_query`". Items 6–7
fill in the rest of the day-one GUC surface. Items 8–9 close
the per-query perf gap quantified in §3.

**Post-status footnote.** Of the original five correctness items
(1, 2, 3) plus the observability headline (5) and the timeout /
perf wins (6, 8), six have landed; §6.4 (snapshot gating + the
companion `PortalStart` refactor) and §6.7 / §6.9 remain.

## 7. Source-doc spot-check summary

Of the **21 distinct claims** I tested (six §2.2 semantic, eleven
§2.3 observability, four §3.1 performance):

- **21 confirmed**, 0 refuted.
- Every cited PG file:line in `pg_code.md` resolved on disk and
  the referenced code was present in the form quoted.
- Every claim that `crates/` is missing a vanilla call was
  verified by literal grep.

The source doc is a reliable reference. The deltas it enumerates
are real, located where it says, and (for the correctness items)
should be treated as a load-bearing punch list.
