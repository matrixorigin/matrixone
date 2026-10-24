# Per-statement SELECT INTO OUTFILE audit receipts

Design revision: 1, 2026-10-24. Owner: MatrixOne frontend and statement tracing maintainers. Owning issue: [#30745](https://github.com/matrixorigin/matrixone/issues/30745), partial acceptance slice. Implementation: [PR #30916](https://github.com/matrixorigin/matrixone/pull/30916). Issue [#30815](https://github.com/matrixorigin/matrixone/issues/30815) supplies diagnostic context and is not fixed here.

## Decision and review boundary

This document designs the complete receipt feature already present in PR #30916, the small repairs described below, and its requested rebase. It is intended for `docs/design/statement-outfile-audit-receipts.md` in the implementation branch. Its distinct design-review decision is recorded separately with this file's SHA256; implementation approval remains a later gate.

The mandatory design gate applies because the feature adds persisted `exec_plan.outfile` JSON, crosses frontend/exporter/tracing ownership, changes per-statement aggregation admission, and adds target redaction at a security-sensitive diagnostic boundary. The package and production-line count is not used to waive those complexity triggers.

Source snapshot reviewed: original PR head `ad4dff1adb408bb0b23048f08f501c5a72393041`; original base `2b388dd1690f83e2e807d6809180336dae2a68eb`; requested rebase target frozen at `70e48cbef1235a8ef4b6c887cb37b0947bace714`; clean rebased feature head `3a3052bdc938e0a01e80655f7c38effaec31bf88`. References to symbols below refer to that rebased head. Rebase source review found the same feature patch, with new base-side JSONLINE duplicate-column rejection; that failure must remain incomplete and preserve its existing SQL error.

Evidence classification: the existing PR/issue comments describe historical tests and production observations, not new executed evidence from this design review. All behavior findings below are source-derived unless explicitly labelled otherwise. The implementation owner must supply terminal validation records for the final changed revision. Reviewing a design does not certify an unrun test or an untested backend.

## Problem, goals, and scope

Fast statements can be grouped under a representative SQL string. That representation cannot identify every OUTFILE target or byte volume. Protocol `ClientEgressBytes` measures a different boundary from serialized export files. An audit consumer needs an individual statement identity, an attempted logical target, the effective format, and sink-confirmed byte/row totals that distinguish complete output from a partial or failed attempt.

The selected scope is parsed top-level `SELECT ... INTO OUTFILE` through direct client execution and the supported binary prepared-execution route. `stage://` targets use the same routes; retain the logical stage expression, never the expanded credential-bearing `StageFilePath`. SQL EXECUTE that resolves to the same SELECT may benefit from common machinery but is not a newly guaranteed acceptance surface. No change to parsing, authorization, SQL response semantics, target path policy, or transaction effects is intended.

Excluded: `INSERT INTO external table`, `mo_dump`, independent result/egress exporters, other egress types, hourly account metrics, a new audit storage table, and the collector/session/timer behavior alleged in #30815. Keep both issues open for their remaining scope. Tracing disabled, a process crash, unavailable statement telemetry, existing retention/persistence failures, and pre-parse failures remain outside a guarantee of receipt durability. This is a receipt carried by existing statement telemetry, not a transactional or exactly-once durable audit log.

Success means a terminal statement record for each traced, admitted in-scope execution retains its own receipt and avoids statement aggregation; successful completed exports match independent file byte and row counts; failed/canceled/panicking attempts never claim a complete export; ordinary statements retain their existing representation and aggregation behavior.

## Invariants and explicit meanings

1. **One statement owner.** A newly admitted in-scope statement owns one detached receipt before authorization/compile can fail. Another statement, pooled object, frontend request, exporter, or background worker cannot own or mutate that receipt after terminal publication.
2. **Confirmation, not attempted bytes.** A file contributes totals only when the existing sink contract has confirmed it. CSV/JSONLINE require clean pipe close and successful async `FileService.Write` completion. Parquet requires the completed file bytes and successful synchronous `FileService.Write`. Pipe acceptance and buffered serialization remain provisional.
3. **Completion includes the producer.** `complete=true` requires normal producer completion, ordered conversion drain/finalization, successful sink confirmation, and successful exporter cleanup. A panic, cancellation, error, or incomplete producer path leaves `complete=false`. A later transaction/protocol failure may coexist with a complete sink receipt; `complete` describes the export sink, while statement `status` describes the enclosing statement.
4. **Lower bounds on failure.** An incomplete receipt preserves previously confirmed files. An incomplete zero is a lower bound, never proof that no bytes escaped. Failed files and unconfirmed partial tails may have side effects that are deliberately uncounted.
5. **Checked monotonic arithmetic.** Totals never wrap; confirmed files are counted once. Overflow is a terminal export error with the already confirmed lower bound retained. Per-file provisional row/byte counters cannot wrap before adding confirmed totals.
6. **Privacy before retention.** Retained target identity excludes URI user-info, query/fragment data, dynamic service arguments, and recognized credential-shaped fields. Safe paths retain their byte identity, including sink-legal `@`, literal `%`, and printable Unicode, until an explicit redaction/truncation/invalid flag says otherwise. No URL normalization may conflate distinct sink keys.
7. **Bounded representation.** Receipt target is detached valid UTF-8, at most 4096 bytes. Format is one of `csv`, `jsonline`, `parquet`, `unknown`. Serialized `exec_plan` stays within the existing 65535-byte TEXT capacity and preserves the receipt preferentially.
8. **Existing consumers remain valid.** JSON composition preserves all retained plan members and integer token precision, does not introduce a float64 round trip, and does not change the ordinary no-receipt path. Cloning, cache invalidation, pool reset, and aggregation admission preserve receipt ownership.

Negations covered by validation are: shared/stale target ownership; provisional bytes represented as exact; truncated prefix marked complete; earlier split lost after a later failure; uint64 wrap; credential retention or key conflation; invalid/oversized JSON; rounded plan numbers; ordinary statements receiving or inheriting a receipt.

## Architecture, ownership, and lifecycle

Data/control path:

```text
parsed/effective SELECT + logical ExportParam
    -> diagnostic target sanitization
    -> RecordStatement establishes StatementInfo identity
    -> SetOutfileReceipt installs attempted identity, complete=false, disableAgg=true
    -> compile/authorization (may fail before exporter creation)
    -> Session.InitExportConfig borrows receipt admission
    -> existing exporter/conversion/sink lifecycle
    -> abort or normal drain, join workers, finish sinks
    -> UpdateOutfileReceipt copies scalar confirmed totals to StatementInfo
    -> EndStatement / FillRow serializes bounded exec_plan.outfile
    -> existing telemetry batching/CSV/TEXT persistence/readers
```

`ExportConfig` owns provisional file progress and statement-wide confirmed scalar totals. It does not store a statement pointer in workers. The existing ordered consumer owns CSV/JSONLINE provisional/confirmed counter mutation; Parquet uses its existing synchronous caller. Receipt transfer happens only after those owners stop and workers join. `StatementInfo.mux` owns the receipt, JSON cache, and terminal serialization; existing caller-held lock rules for `ExecPlan2Json` and cloning remain applicable. `InitExportConfig` creates a new configuration for each statement generation. Do not add a registry, global cache, goroutine, queue, or retry layer.

Receipt state transitions:

| State | Entry | Exit |
|---|---|---|
| Absent | ordinary/unsupported statement or existing tracing skip | no new behavior |
| Attempt | current traced statement identity admitted | incomplete, zero lower-bound counters |
| Exporting | export config created; serialization/sink work starts | confirmed prior files accumulate |
| Complete | producer and final drain return normally; finish succeeds | final exact sink totals published |
| Incomplete terminal | auth/compile/conversion/write/close/cancel/panic/overflow failure | retain confirmed lower bound, then ordinary statement terminal accounting |
| Detached clone | existing tracing clone path | copy receipt and target; retain disableAgg |
| Free/reuse | existing end/exported lifecycle permits pool return | nil receipt, nil JSON cache, clear disableAgg |

CSV/JSONLINE count bytes including headers and framing; rows exclude headers. CSV splitting counts each finalized split exactly once. JSONLINE transfers `BatchByte.rowCount` with the serialized batch; split-before-write must credit the old file before resetting provisional counters. Parquet `Rows` remains cumulative; `parquetConfirmedRows` records the prior successful flush boundary, and a successful flush contributes `Rows-parquetConfirmedRows` plus complete file length, including footer. Empty completed exports are valid with zero rows and their actual serialized byte size. No trailing empty split is created by this feature.

`closeFile` clears pipe/group/cancel/file-service state after joining. Repeated cleanup cannot recount the same file because the active file handles are cleared. Confirmed scalar totals remain readable until statement transfer. On a failure, cancellation releases blocked pipe writes and conversion publication before joining workers. Existing primary concrete `*moerr.Error` values must survive cleanup so SQL/transaction error classification remains stable; secondary cleanup errors follow existing logging rules.

There is no receipt retry or recovery protocol. An existing statement retry must use the existing execution-generation rules; no new transparent export retries are introduced. A new user execution receives a new statement/configuration and receipt. Node restart reads historical JSON normally but does not reconstruct interrupted exporter state. A crash during output may leave files without a terminal receipt, as with existing telemetry; do not describe this feature as crash-consistent audit delivery.

## Small coherent repairs and optimization

### A. Panic-aware OUTFILE finalization

Concrete source path: `executeStatusStmt` registers its OUTFILE defer while named `err` is nil; `runner.Run` (or a later exporter call) panics; the defer calls `ep.finish(nil)` before `executeStmtWithWorkspace` recovers that panic. The success branch of `finish` waits conversion workers before canceling, then cleanly closes the sink. A prefix may therefore be marked complete. If a converter is blocked publishing into the full bounded result channel, the same branch can wait indefinitely before the outer recovery can run.

Repair the first owner, the OUTFILE execution defer: detect an unwinding panic directly, convert it only into a cleanup cause, call abort/finish so cancellation precedes join, transfer confirmed totals with `complete=false`, and re-panic the original value so the established outer recovery/error contract remains responsible for the SQL error. Normal named-error returns continue through `ep.finish(err)` and preserve primary concrete errors. All panics must cross the cleanup guard, including errors before `openNewFile` finishes and after a confirmed split. Keep success completion contingent on returning normally through `exportAllDataFromBatches`; a fully drained exporter that encounters a subsequent unrelated transaction error retains accurate sink completion semantics.

Do not recover-and-swallow the panic, replace its payload, call terminal statement accounting inside the exporter, or add a background cleanup owner. This repair closes both the new receipt truthfulness contract and the existing exporter owner’s reachable cleanup path.

### B. Preserve URI path bytes during target sanitization

The earlier reported `@` and literal `%` issue is closed at the reviewed original head: tests and source already retain those sink-legal names. Do not report it as an unresolved defect.

A distinct source path remains: `sanitizeOutfileTarget` protects literal percent bytes, calls `url.Parse`, then `URL.String`, then restores `%25`. `URL.String` percent-encodes printable Unicode path bytes. Thus `stage://audit/报告.csv` and literal `stage://audit/%E6%8A%A5%E5%91%8A.csv` can produce the same receipt identity although `fileservice.ParsePath` accepts the distinct file expressions. Both flags remain false. This violates unflagged target identity even though byte/row totals are unaffected.

Use URL parsing to identify/validate the URI authority and user-info boundary, but strip credentials/query/fragment from the original byte representation instead of re-rendering the path through URL escaping. Keep the existing `ParsePath` admission check, UTF-8 validation, credential-field policy, and 4096-byte bound. For a raw URI, locate its authority after `://` and before the first `/`, `?`, or `#`; remove only the user-info portion indicated by parsing (through the authority’s final `@`), then retain the original scheme/authority/path bytes. Remove query/fragment from the resulting raw representation. Never decode percent sequences; the sink treats legal percent bytes literally. Invalid/ambiguous URI syntax remains conservatively invalid. Retain all existing credential and `@`/`%` tests and add the Unicode/literal-percent pair with distinct expected identities.

### C. Avoid prepared SQL redaction work when its output is unreachable

`makeExecuteSql` currently calls `redactStatementTextForLogging(prepared.PrepareStmt, strings.TrimSpace(prepared.Sql))` before attempting bounded `envStmt` and separator append. That scans the entire prepared SQL through the MongoDB diagnostic scanner even when a short configured output limit is exhausted by the environment prefix. On every prepared execution this adds O(full prepared SQL length) work without producing a prepared-SQL byte.

Move prepared SQL redaction after the bounded environment prefix and ` // ` separator have been admitted. If either append already proves truncation, return the existing abbreviation immediately. When any prepared SQL may be retained, continue redacting the full source before clipping; clipping raw SQL first can hide a later secret marker or split credential syntax. No persistent redaction cache is warranted: it introduces another ownership/invalidation/bound contract for a one-branch optimization. Preserve limit 0, invalid-negative, unlimited -1, exact boundary, UTF-8 marker, binary value, SQL EXECUTE, and existing MongoDB redaction semantics.

The expected benefit is limited and measurable: the new full prepared SQL scan is avoided on output paths already decided by the prefix. Other paths still pay the conservative O(full SQL length) privacy scan. Do not claim zero regression or O(limit) redaction for those paths.

## Persistent JSON and compatibility

Schema version 1, optional top-level member `outfile`:

```json
{"outfile":{"version":1,"target":"stage://audit/out_%d.csv","format":"csv","bytes_written":42,"rows_written":1,"complete":true}}
```

`version`, `target`, `format`, unsigned decimal `bytes_written`/`rows_written`, and boolean `complete` are present. Optional true flags `target_truncated`, `target_redacted`, `target_invalid` describe target degradation. Consumers must read JSON integers without float64 coercion when exact uint64 totals matter. Missing `outfile` means no receipt, not zero export. `complete=false` means a lower bound; consumers must never infer exact zero from it. Target is a logical target expression, including split templates, not an enumerated set of physical keys or stage-expanded credential descriptor. Format reflects the exporter’s effective supported format; unrecognized values serialize as `unknown`.

`execPlanWithOutfileReceipt` composes top-level `json.RawMessage` members, preserving retained numeric tokens. Unknown existing members survive; member order/whitespace are not contractual. Invalid, non-object, absent, or oversized plan diagnostics use the existing bounded plan-summary mechanism and retain the receipt. If adding the receipt crosses the TEXT capacity, summarize the plan rather than truncate JSON or discard the receipt. At the current 4096-byte valid-UTF-8 target bound, even worst-case JSON string escaping plus fixed fields fits beneath 65535 bytes. Tracing independently bounds/detaches the target so frontend callers cannot bypass persistence capacity. The existing nonreceipt `ExecPlan2Json` path remains unchanged.

The existing typed `models.ExplainData` consumer tolerates unknown JSON fields; a focused decode test is the cheapest compatibility proof. The existing statement-info ETL safety cap must never replace a valid in-capacity composed receipt. No catalog migration, new column, SQL syntax, wire message, mixed-node state exchange, storage-format rewrite, or required backfill is introduced. An older binary reading the JSON sees an unknown field; existing readers that decode a struct ignore it, and raw JSON consumers must be tolerant. Older writers simply emit no receipt. Rolling upgrades therefore provide receipts for executions handled by upgraded writers only. Downgrade/rollback leaves stored JSON readable and stops new receipts; historical data requires no removal. Existing backup/restore treats exec_plan as TEXT and preserves the field opaquely; source inspection plus round-trip serialization is sufficient here unless implementation touches a persistence codec.

Version 1 must not acquire incompatible meanings. Future changes to totals, target interpretation, or completion require explicit compatibility review and possibly a new receipt version. There is no new configuration flag: existing tracing and statement retention controls govern availability. Rollout through normal CI and a focused service probe is adequate; the feature should not force a new table or deployment mode.

## Privacy, authorization, and tenant boundaries

The raw target remains necessary only for actual execution under existing SQL file authorization. The audit representation uses the original logical `FilePath`; `StageFilePath` is never copied. URI user-info is removed; query/fragment bytes are removed; file-service argument records are reduced to service identity plus file; recognized inline credential-shaped fields or credential-bearing service names are conservatively redacted. Flags disclose loss of target fidelity. Rendering redacted diagnostic SQL replaces every occurrence of the raw target, including an identical projected literal; if the raw text cannot be safely matched, use a short redacted statement representation.

This feature is not a general secret detector. Arbitrary projected values, prepared parameter values, and unrelated SQL literals retain their established logging policies. Error messages from existing lower layers and pre-parser failures retain their separate policies; do not claim universal secret removal. The design closes the added receipt and its OUTFILE SQL diagnostic source, without expanding to unrelated error/logging redesign. Existing redaction for MongoDB and credential-bearing DDL must remain intact.

Statement account/user/role/session identity and `system.statement_info` access controls remain the authorization boundary. A receipt does not grant destination access, resolve a stage outside normal authorization, or create a new cross-tenant index. Verify that a tenant sees only its permitted statement rows using the existing telemetry access rules. Target strings are bounded diagnostic data, never metric labels. The sanitizer cannot affect the real sink path or authorization result.

## Capacity, performance, and failure containment

Per traced OUTFILE execution: one bounded target string, receipt scalar fields, and four exporter scalar counters. Retained target size is O(min(input,4096)); no per-file target list is retained. Statement cloning copies bounded receipt data; pool cleanup releases it. Existing converter/sink queues and parquet buffers retain their existing bounds and owners; this patch does not add workers or increase their admission capacity. The panic repair makes cancellation precede join on its failure path.

Frontend sanitization and diagnostic redaction may scan/temporarily allocate O(full target/SQL length), bounded by existing statement admission rather than the retained 4096-byte cap. JSON composition costs O(existing plan size plus bounded receipt), with the existing TEXT limit bounding retained composed output; existing plan production cost is unchanged. Checked arithmetic is constant per existing write/chunk/file boundary, with no additional I/O or synchronization in per-row loops. Receipt transfer adds the existing statement mutex acquisition after worker shutdown. Ordinary direct statements incur only small statement-kind checks; ordinary prepared statements preserve the existing redactor behavior where its output can be retained.

Aggregation suppression increases terminal record rate from aggregates to one row per OUTFILE execution. That is required for target identity; it does not make ordinary SELECTs nonaggregatable. Capacity planning should account for roughly one receipt per export, up to 4096 target bytes plus bounded JSON overhead, instead of a representative aggregate. No additional high-cardinality metric is added. Use existing telemetry failure logs and receipt flags for diagnosis; do not introduce per-row logs or retry-on-audit-failure behavior. Export errors remain governed by the existing exporter/statement contract, and telemetry persistence failures follow existing infrastructure behavior.

A source observation outside this repair: `writeParquetToFile` does not close a dynamically created ETL file service, unlike CSV `closeFile`. That ownership issue predates this feature and is not caused by receipt counters; `FileService.Close` returns no confirmation result and is not part of the selected Parquet write-confirmation boundary. Track it separately if required. This design does not expand the repair to unrelated backend ownership changes or claim cloud-backend lifecycle validation.

## Alternatives and accepted decisions

| Alternative | Benefit | Rejection/selection rationale |
|---|---|---|
| Status quo: representative SQL plus protocol egress | no added telemetry rows or JSON cost | loses individual targets; protocol bytes are not file volume |
| New export audit table/type and synchronous durable audit writes | richer schema and potential crash-consistent protocol | requires catalog/migration/retention/error policy and another delivery owner; exceeds this acceptance slice |
| Collect export stats in distributed operators/resource metrics | potentially unifies other egress types | frontend owns final serialized files; operator rows/bytes cannot prove sink close or headers/footer size |
| Per-statement additive receipt at existing frontend sink/tracing boundary | smallest cross-owner contract; preserves existing telemetry persistence/readers | selected; existing tracing availability and crash-loss limits remain explicit |
| Record attempted/buffered bytes as exact | cheap early counters | wrong on conversion/write/close failure; use confirmed file lower bounds instead |
| URL-normalize all audit targets | convenient parser rendering | conflates raw sink keys; parse boundaries while preserving path bytes |
| Clip prepared SQL then redact | bounded scan | can remove a later privacy trigger and leak retained prefix; rejected |
| Cache redacted prepared SQL | removes repeated scans | extra retention and invalidation contract; defer redaction past known prefix truncation instead |

No new interoperability protocol is introduced. JSON handling follows the repository's existing `encoding/json` object/RawMessage conventions and TEXT capacity guard; URI parsing is an aid to credential-boundary recognition, not authority to normalize the fileservice grammar. Existing MySQL protocol status/error identity and file-service authorization are mandatory contracts. The existing per-statement telemetry and additive explain JSON are architectural precedent; they do not imply crash-consistent audit durability.

## Change/risk map and proportionate validation

The original complete diff was read, including test and result files. The rebase was checked against the frozen target's changed exporter/admission code. The following closures account for every changed artifact and required reverse arc.

| Closure/artifacts | Risk and relevant lenses | Required evidence |
|---|---|---|
| `outfile_audit.go`, tests; admission in `mysql_cmd_executor.go`; prepared diagnostic change in `util.go` | R3 privacy and hot path; correctness/security/compatibility/performance | safe target tables including credentials, literal @/%, Unicode pair, UTF-8 limit/invalid input, split templates; direct/prepared privacy and ordinary controls; bounded/unlimited diagnostic behavior; small prepared-prefix benchmark |
| `ExportConfig`/`export.go`, `session.go`, `status_stmt.go`; exporter lifecycle and accounting tests | R3 resource lifecycle/cancellation/shared workers; correctness/state/liveness/bounds | confirmed vs provisional totals; CSV header/row split, JSONLINE, Parquet split/empty, later-split failure; concrete primary errors; deterministic panic cleanup and original panic propagation; recovery/next statement; focused owning-package race |
| `outfile_receipt.go`, `report_statement.go`, receipt tests and existing persistence consumers | R3 persistence/ownership; correctness/compatibility/concurrency/bounds | precise RawMessage integer tokens, unknown-field decode, invalid/oversized/exact-capacity plan fallback, detached clone, cache invalidation, pool reuse, no-receipt control, fast aggregation suppression; owning-package normal/race |
| stage split SQL/result BVT | R2 public SQL contract; result/metadata/test architecture | existing minimal three-row fixture; wait for terminal receipt by connection/target; compare independent reads of all split file lengths and three row count; rerun comparison through the existing harness |
| versioned design/review evidence | R0 delivery artifact | exact reviewed document hash/revision linked from PR; no test claim from prose alone |

Tests must use existing exporter/session fixtures before inventing heavier setup. Specific new oracles:

* Panic: use a fake runner that creates deterministic outstanding conversion publication or injected barrier state and panics with a sentinel payload. Recover only in the test. Assert original payload survives, joined workers/cancel/pipe ownership are cleared, receipt is incomplete, and a prior confirmed split survives. No sleeps, probabilistic channel filling, or leaked completion goroutine. A normal-return neighbor proves completion still true.
* URI identity: distinct sink-legal Unicode and literal-percent paths have distinct unflagged receipt targets; preserved raw path and user-info/query removal compose correctly. Retain the existing closed @/% regression suite rather than duplicate it as a new issue.
* Prepared prefix: limits shorter than/equal to environment and separator boundaries must retain identical abbreviated text and avoid inaccessible SQL work; when prepared SQL is retained, secrets remain removed even when a marker is beyond the output prefix. Include unlimited/UTF-8 controls. Benchmark a large valid prepared SQL under a tiny limit against small SQL, recording ns/op and allocations; setup must not parse/create a cluster inside the measured loop.
* Rebase: current main's duplicate JSONLINE aliases fail with their original concrete SQL error, incomplete receipt, and zero or prior-file lower bound; ordinary valid JSONLINE still succeeds. Reuse existing base-side tests when they already prove the same error and add only the missing receipt oracle.

Run focused named tests first and prove the selection nonempty. Then run owning frontend and motrace packages in normal and race mode because receipt mutex ownership, exporter worker cancellation, and shared test fixtures make those dimensions applicable. Use repository CGo wrapper and matched native artifacts/toolchain. Run incremental gofmt/vet/configured lint on changed Go packages plus directly affected consumers; broad repository SCA is not a substitute or default. Rebase can reuse historical evidence only when relevant base-side contracts are shown unchanged; the new JSONLINE failure contract and all production repairs need fresh evidence. Preserve exact revision, command, mode, terminal output and result.

A focused real service probe must cover direct CSV, binary prepared OUTFILE, stage routes, JSONLINE success/failure, Parquet totals, privacy, ordinary same-session nonreceipt behavior, and terminal individual rows despite sub-threshold duration. Reuse the existing stage split BVT for independent actual-file bytes; no larger workload is needed. Unit fault injection proves inaccessible sink/panic/cancel branches; no cloud credentials or remote services are required for that proof. Cloud backends are an explicit validation limit until exercised; no unsupported remote durability claim is allowed. Full topology/restart/upgrade/load/chaos tests are unnecessary because no new distributed/persistent state protocol, process lifecycle, codec or scale-dependent invariant is introduced.

## Acceptance and remaining limitations

Implementation is ready for review only when the versioned design and traceable design approval precede its repair commits; A/B/C are implemented consistently; focused and required owning-package normal/race/static checks pass or semantically valid equivalent evidence is recorded; stage BVT and focused public SQL/protocol probes pass on the final relevant inputs; final review has no unresolved blockers; delivery diff contains only scoped code/tests/design artifacts.

The final PR description must identify the final source/base, link the design revision and decision, describe partial #30745 scope, retain #30815 as diagnostic context, and state actual test/CI status and unexercised cloud/backend limits. Publishing does not close either issue. There is no guarantee when tracing/collector persistence is disabled or unavailable, no crash-consistent file/receipt transaction, no enumeration of physical split keys, and no claim to audit other egress surfaces. Those are accepted scope decisions, not open design blockers.
