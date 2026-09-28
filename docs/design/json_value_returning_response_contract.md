# JSON_VALUE RETURNING and response contract

Issue: #28037

## Contract

`JSON_VALUE` accepts a document and path, with optional `RETURNING`, `ON EMPTY`,
and `ON ERROR` clauses. MatrixOne keeps its existing extension that permits an
expression (including a prepared marker) as the path. In the clause-bearing
form, a NULL path returns SQL NULL, and every non-NULL path is validated before
the SQL NULL document shortcut; an invalid path remains a statement error. Bare
two-argument calls preserve the legacy SQL NULL document shortcut, including
when the supplied path is invalid. The new grammar accepts the same expression
path as the legacy form.

The default return type is nullable `VARCHAR(512)` in `utf8mb4_bin`. Supported
explicit targets are `CHAR`, `BINARY`, `SIGNED`, `UNSIGNED`, `DECIMAL`, `FLOAT`,
`DOUBLE`, `DATE`, `TIME`, `DATETIME`, `YEAR`, and `JSON`. `DEFAULT` values are
signed literals and are converted and validated while binding/preparing the
statement. A default NULL, expression, column, or parameter is rejected.

The extraction state is separated from response policy. A row can be SQL NULL,
path NULL, empty, a single JSON value, multiple values, a document parse error,
or a hard error. Empty rows use `ON EMPTY`; multiple matches and conversion or
document errors use `ON ERROR`. Invalid paths and excessive JSON depth are hard
errors. After the clause-form path check, SQL NULL documents and JSON null
values return SQL NULL directly.

For a single match, text targets return canonical JSON text (strings are
unquoted), `RETURNING JSON` preserves JSON values, and numeric/temporal targets
use strict conversion. A conversion that exceeds the target or loses data is an
`ON ERROR` event rather than a silently truncated value.

## Plan representation

The parser creates a dedicated JSON_VALUE expression carrying the document,
path, target type, and both response policies. Its formatter preserves an omitted
target so reparsing retains `VARCHAR(512)` rather than an explicit `CHAR(512)`,
and preserves explicit policies. The binder keeps an expression with no
`RETURNING`, `ON EMPTY`, or `ON ERROR` clause on the existing two-argument
JSON_VALUE overload. When any new clause is present, it lowers the expression
to an internal seven-argument JSON_VALUE overload. The target type is carried
by the existing plan `Expr_T`; response modes are integer constants and
validated defaults are typed constants. The internal overload is not reachable
through the public grammar, and no protobuf change is required.

The executor performs extraction first and applies the selected policy per row.
It does not use a limiting ordinary cast for a document or prepared value. The
function remains nullable and handles policy arguments itself instead of relying
on generic strict-function NULL short-circuiting.

## Compatibility and rollout

Existing two-argument calls remain valid on the legacy two-argument plan and
receive the MySQL default type and collation. Existing persisted generated
values are not automatically recomputed. Upgrade validation covers views,
generated columns, indexes, values over the 512-character boundary, and the
explicit rebuild procedure.

### Versioned compatibility decision (revision 4, 2026-09-28)

Status: independent maintainer approval pending. This revision supersedes
revision 3/v85; the historical decision is retained below. Approval must identify
this revision's immutable commit and an independent approver. Author replies,
thread resolution and approvals of earlier revisions do not approve this one.

The current allocation candidate is MORPC v100 for JSON_VALUE function ID 462,
overload 2. Main at b9013dc6bb058ec74935a62ac3a35f57d13c2f98 already allocates
v85 through v99 to other capabilities. The final allocation must be coordinated
and checked again before merge; this document does not reserve a mainline number.
All gates, tests and this decision must move together if that allocation changes.

The planner requires the deployment-wide MOProtocolVersion to be at least 100
for RETURNING, ON EMPTY or ON ERROR. Bare two-argument calls keep the legacy
plan and its NULL-document short circuit. In the new clause-bearing contract,
a non-NULL invalid path is a hard error even when the document is SQL NULL.

Both current sender and receiver validate overload 2 against v100 before remote
execution. Those checks do not retrofit an old receiver: the deployment minimum
and old-node admission must prevent sending the new plan to an old binary.
Missing runtime capability state rejects new remote plans. Function lookup also
bounds-checks overload indices, but a lookup error is not rollout acceptance.

Catalog admission uses one expression feature walk for default, generated,
on-update, check, index-table and optimized view owners. Reads require the
committed floor; authoring additionally requires the enabled/admitted/catalog-
fenced write floor. Checks must run before folding erases the feature and before
catalog side effects. A view preserves its required version in durable metadata.
The SQL grammar rejects JSON_VALUE in ON UPDATE; internal owner-walk coverage
does not imply SQL support for that combination. Prepared execution is tested separately; a session prepared statement is not
assumed to be a durable catalog object.

Do not admit pre-v100 CNs after committing a v100 durable floor. The existing
floor advances monotonically; deleting views/generated expressions or indexes
does not by itself lower it. In-place rollback below that floor is unsupported
unless a separately supported, validated floor-lowering procedure exists.
This change adds no force-downgrade mechanism. Old-node rejection is a required
acceptance result. Removing/rebuilding dependent metadata is necessary for any
future downgrade procedure, but is not proof that downgrade is permitted.

Strict temporal conversions do not discard components: DATE rejects date-time
spellings including midnight; TIME rejects date-bearing values while preserving
ordinary TIME/duration spellings. Both executor conversion and DEFAULT binding
use these boundaries. Runtime loss follows ON ERROR; invalid DEFAULT ON EMPTY
or ON ERROR fails bind/prepare even when unused. Existing fractional precision,
zero-date and ordinary CAST semantics remain unchanged.

Stored JSON retains structural admission and the admitted executor's descendant
shape check. The latter still scans the document; no constant-time or performance
improvement claim is made. Benchmark full validation, admitted extraction and
vector execution separately with setup/admission outside the timed loop. Removing
validation requires a separate provenance/immutability proof and reviewer decision.

Required independent decision: approve the final capability allocation, strict
response semantics, sender/receiver and persisted-owner admission, old-node
rejection and rollback restriction, and retained validation/performance disposition.

### Historical revision 3 (superseded, not an approval record)

Original revision: 3, 2026-09-16

The seven-argument representation is a new wire and persisted-plan contract.
MORPC version 85 is the first version that may carry JSON_VALUE function ID 462
with overload index 2. The planner admits `RETURNING`, `ON EMPTY`, and `ON
ERROR` only when the deployment-wide `MOProtocolVersion` is at least 85; below
that threshold it returns a not-supported error before publishing the plan.
An ordinary two-argument call remains the legacy overload and is still allowed
at version 57. MORPC versions 73 and 74 remain the independent DECIMAL SUM and
numeric HEX contracts already used by the current `main` branch.

Both remote pipeline boundaries enforce the same contract. The sender and
receiver expression validators identify overload 2 and reject it when the
local deployment gate is below version 85 or unavailable. This prevents a new
sender from sending the plan to an old CN and prevents a current receiver from
executing a plan after a rollback lowered the gate. The function-ID lookup also
rejects an out-of-range overload instead of indexing the overload slice.

Creating a view, generated column, index expression, or persisted prepared plan
that contains overload 2 is therefore a version-85-only operation. The
catalog-owner admission covers table defaults, generated columns, checks,
on-update expressions, index tables, and view plans before publication.
Upgrade all CNs and raise the oldest-live protocol gate to 85 before enabling
the syntax. Do not lower the gate or roll back to a pre-85 binary while such a plan remains
persisted; rebuild or remove that metadata first. A legacy two-argument plan
does not carry this prerequisite and remains readable by older CNs. The
regression matrix covers sender/receiver rejection below 85, acceptance at 85,
planner and persisted-owner admission at both thresholds, and the legacy
two-argument control.

Independent design approval for this revision is still pending. This document
records the implementation contract and rollback prerequisite; it is not an
approval record.

## Required evidence

- Temporal helper, executor and binder tests plus canonical JSON_VALUE BVT:
  NULL/DEFAULT/ERROR ON ERROR, unused defaults, column inputs, legacy controls,
  generated/indexed values, CREATE/ALTER VIEW and prepared reuse.
- Real SQL DDL builders at predecessor/new thresholds and read-only versus
  authoring admission; manually constructed protobuf owner tests are supplemental.
- Actual pre-feature/new binaries: mixed-CN refusal before transmission, incomplete
  admission with no catalog residue, admitted creation/restart/read, and old-node
  rejoin rejection. Lowering a test runtime integer is not mixed-binary proof.
- Exact-content static checks, related package tests, canonical BVT comparison,
  benchmark measurements and exact-head CI. A skipped upgrade job is NOT_RUN.
- Independent approval of this revision followed by implementation re-review and
  deployment QA. None is implied by this document or local test success.

### Revision 4 validation record (2026-09-28)

Code and permanent tests: [e3d5c8b956e8b0ec4d7b0b76066fc2fe12093b36](https://github.com/matrixorigin/matrixone/tree/e3d5c8b956e8b0ec4d7b0b76066fc2fe12093b36).
The source merges main `b9013dc6bb058ec74935a62ac3a35f57d13c2f98` without rebasing.
Allocation was rechecked at main `239fe81c82ae07a8c316576cac8555c445c92212`:
v99 remains the highest landed allocation. v100 remains a candidate requiring
maintainer coordination, not a reservation made by this document.

| Check | Result and scope |
| --- | --- |
| Counterexample before repair | Expected FAIL: TIME lost the date under NULL/DEFAULT/ERROR (varchar and JSON inputs); binding accepted an invalid unused TIME DEFAULT. |
| Normal owning packages | PASS: types, bytejson, function, plan, pb/plan and parsers/...; native/CGo build completed in the isolated worktree. |
| Remote boundary | PASS: `TestRemoteExpressionProtocolValidation`, including v99 rejection and v100 admission. Runtime integer tests are not old-binary rollout tests. |
| Race | PASS: JSON_VALUE and stored-JSON tests in bytejson/function. |
| SQL publication | PASS: parser/binder/DDL/catalog paths; failed admission leaves no tables, columns or index metadata; legacy control, generated-column index, read/write separation, failed ALTER VIEW preserving its definition, real CN restart, rebind and subsequent indexed writes. |
| Prepared protocol | PASS: DATE/TIME NULL/DEFAULT/ERROR, repeated COM_STMT_EXECUTE, and invalid unused DEFAULT rejected by COM_STMT_PREPARE. |
| Canonical SQL BVT | PASS twice: 117/117 each, zero failures/ignored/abnormal; table and per-case database teardown verified between runs. SQL PREPARE is included. |
| Historical CI | INCONCLUSIVE cause: old-head Ubuntu UT failed; job logs expired and the run has no artifacts. No unsupported code/environment attribution or blind rerun. New-head CI remains a separate gate. |
| Independent design approval, actual mixed binaries and QA | PENDING / NOT_RUN. No maintainer acceptance, old-node rejoin, full storage restart or downgrade acceptance is claimed. |

The SQL publication scenario reuses the existing sequential one-CN fixture;
its measured scenario body was 3.573 seconds including CN restart. The binary
prepare scenario body was 0.040 seconds. Admission is injected through a
scoped test-only runtime reader, so heartbeat publication cannot overwrite the
selected state. The SQL-built physical index owner was also checked: it contains materialized
keys and no JSON_VALUE expression; its generated-column table owner requires
v100. No new production test hook is added. The constant-folding SQL
counterexample passed the existing unified gate, so no speculative parallel
gate or observer modification was introduced.

BVT used the unmodified canonical `mo-tester -n -g -o -p` path. Its result-file
lookup rewrites dot-suffixes throughout an absolute path, including `.codex`,
so byte-identical source/result files were staged outside that hidden directory.
Initial driver setup/path failures ran no SQL assertions and were retained as
failed evidence. The first actual comparison exposed six newly added expected
TIME rows whose zero fractional digits JDBC omits; only those expected rows
were corrected. Binary protocol checks still assert TIME(6)'s six digits.
Neither server formatting nor fractional-precision semantics changed.

#### Retained-validation measurement

`BenchmarkJSONValueStoredLargeDocumentSmallPath`, Go 1.26.4, darwin/arm64,
Apple M1, GOMAXPROCS=4, `-benchmem -benchtime=500ms -count=3`:

| Elements | Stored bytes | Entry | Median ns/op (min-max) | B/op | allocs/op |
| --- | ---: | --- | ---: | ---: | ---: |
| 16 | 264 | validated | 1087 (974-1347) | 2224 | 11 |
| 16 | 264 | admitted | 1486 (545.9-1779) | 384 | 5 |
| 16 | 264 | vector | 6206 (4911-6407) | 1288 | 28 |
| 256 | 3384 | validated | 9507 (8190-16133) | 34097 | 15 |
| 256 | 3384 | admitted | 2659 (2579-4268) | 384 | 5 |
| 256 | 3384 | vector | 3563 (3322-3886) | 1288 | 28 |
| 4096 | 53304 | validated | 173960 (163801-380511) | 663622 | 21 |
| 4096 | 53304 | admitted | 37726 (31774-65958) | 384 | 5 |
| 4096 | 53304 | vector | 38755 (34568-40116) | 1288 | 28 |

All entries use the same document and assert extraction of 1. Initialization
and stored admission occur outside the timed loop. `validated` calls complete
stored validation then extraction; `admitted` calls admitted extraction with
its retained recursive descendant check; `vector` calls the seven-argument
`JsonValue` executor on a JSON column with RETURNING SIGNED. Helper extraction
and complete vector conversion are different measured entry points.

These local samples show substantial run-to-run variation; they do not establish
a speedup, regression threshold or production acceptance. The admitted path
still scans descendants. The old benchmark labeled admitted actually measured
the complete validation entry and must not be cited as admitted-path evidence.
Removing validation is not part of this repair. The performance CR stays open
until the reviewer explicitly accepts the retained-check/deferred-optimization
disposition.
