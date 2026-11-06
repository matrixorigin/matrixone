# JSON_VALUE RETURNING and response contract

Issue: #28037

Compatibility note (2026-11-05): this candidate follows MatrixOne's
[shutdown-upgrade contract](../../.agents/skills/mo-dev/references/upgrade-compatibility.md).
It introduces no JSON_VALUE-specific MORPC generation or peer gate. Existing
independent capability gates remain unchanged. Performance, supported-release
upgrade, recovery and review evidence remain separate acceptance items.

## Contract

`JSON_VALUE` accepts a document and path, with optional `RETURNING`, `ON EMPTY`,
and `ON ERROR` clauses. MatrixOne keeps its existing extension that permits an
expression (including a prepared marker) as the path. In the clause-bearing
form, a NULL path returns SQL NULL, and every non-NULL path is validated before
the SQL NULL document shortcut; an invalid path remains a statement error. Bare
two-argument calls preserve the legacy SQL NULL document shortcut, including
when the supplied path is invalid. The new grammar accepts the same expression
path as the legacy form.

For clause-bearing calls, the default return type is nullable `VARCHAR(512)`
in `utf8mb4_bin`. Bare two-argument overloads 0/1 retain their legacy unbounded
VARCHAR metadata and text behavior. Supported
explicit targets are `CHAR`, `BINARY`, `SIGNED`, `UNSIGNED`, `DECIMAL`, `FLOAT`,
`DOUBLE`, `DATE`, `TIME`, `DATETIME`, `YEAR`, and `JSON`. `DEFAULT` values are
signed literals and are converted and validated while binding/preparing the
statement. A default NULL, expression, column, or parameter is rejected.

The extraction state is separated from response policy. A row can be SQL NULL,
path NULL, empty, a single JSON value, multiple values, a document conversion
error, or a hard error. Empty rows use `ON EMPTY`; multiple matches and value
conversion errors use `ON ERROR`. Malformed source JSON is a statement error and
does not enter the `ON ERROR` response policy. Invalid paths and excessive JSON
depth are hard errors. After the clause-form path check, SQL NULL documents and
JSON null values return SQL NULL directly.

For a single scalar match, explicit character targets return canonical text
(strings are unquoted), `RETURNING JSON` preserves JSON values, and
numeric/temporal targets use strict conversion. The omitted `RETURNING` form
keeps the legacy text behavior for composite values. A composite value selected
by an explicit scalar target, or a conversion that exceeds the target or loses
data, is an `ON ERROR` event rather than a silently truncated value.

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

Existing two-argument calls remain on the legacy two-argument plan with their
original type, collation and unbounded text behavior; the new default applies
only when a RETURNING/ON EMPTY/ON ERROR clause selects overload 2. Existing persisted generated
values are not automatically recomputed. Upgrade validation covers views,
generated columns, indexes, values over the 512-character boundary, and the
explicit rebuild procedure.

### Shutdown-upgrade compatibility (candidate revision 14)

A supported upgrade stops all services, replaces their binaries, and restarts
against the existing data. Mixed-running-version operation, old-peer rejoin,
and downgrade are not supported. The seven-argument overload does not change
RPC framing and therefore adds no feature-specific protocol version or peer,
placement, sender, receiver, or persisted-expression admission gate. Existing
function-lookup bounds checking remains in place.

The new binary must read supported released data: legacy persisted expressions,
views, generated values and index metadata keep their original semantics and
are not automatically recomputed. The source for compatibility validation is a
pinned supported release and its actual data, including retained snapshots or
backups; an earlier unpublished main commit is not a release fixture. Existing
historical upgrade/restart reports remain attributed to their actual binary
and data identities, not relabelled as final release validation.

Other features in the same expression or owner retain their own existing
admission floors. For example, functional indexes, signed WEEK/DAYOFYEAR,
collation-aware LEAST/GREATEST, JSON text processing, float conversions,
JSON MIN/MAX and ODKU keep their independent contracts. Adding JSON_VALUE must
neither raise nor lower those floors, including for algorithm-expression-only
owners, folded views and both expression orders. In particular, the existing
v129 opaque-JSON text contract also applies when JSON_VALUE consumes a
JSON-typed input; VARCHAR-only inputs do not acquire that floor merely because
they use the seven-argument overload. SQL ON UPDATE does not accept
JSON_VALUE; testing the internal owner walker does not imply SQL support.
Prepared handles are session state, not durable catalog objects.

Strict temporal conversions do not discard components: DATE rejects date-time
spellings including midnight; TIME rejects date-bearing values while preserving
ordinary TIME/duration spellings. Both executor conversion and DEFAULT binding
use these boundaries. Runtime loss follows ON ERROR; invalid DEFAULT ON EMPTY
or ON ERROR fails bind/prepare even when unused. Existing fractional precision,
zero-date and ordinary CAST semantics remain unchanged.

Stored JSON retains structural admission and the admitted executor's descendant
shape check. The latter still scans the document; no constant-time or performance
improvement claim is made. Full validation, admitted extraction and clause-bearing
vector execution are separate measurements. Generic AppendBytes and checked
UnmarshalBinary/UnmarshalBinaryWithCopy must include admission in the timed loop;
legacy two-argument JSON_VALUE must be compared separately on admitted JSON
columns, with VARCHAR input as the text-parsing control. Removing validation
requires a separate provenance/immutability proof and reviewer decision.

The retained validation cost and independent acceptance of the current
immutable design revision remain open. Earlier independent reviewers have not
withdrawn their design-approval requests. Their historical mixed-binary
requests must be reconciled with the current shutdown-only policy; this
revision neither claims those requests were withdrawn nor represents them as
tested. Earlier approvals retain their original scope.

## Required evidence

- Temporal helper, executor and binder tests plus canonical JSON_VALUE BVT:
  NULL/DEFAULT/ERROR ON ERROR, unused defaults, column inputs, legacy controls,
  generated/indexed values, CREATE/ALTER VIEW and prepared reuse.
- Same-binary tests for absence of a JSON_VALUE floor and for independent floors
  in real SQL DDL, persisted owners, placement, sender, receiver and stream;
  injected protocol numbers do not model a supported mixed-version cluster.
- Actual pinned supported release data followed by shutdown upgrade: read and
  rebind legacy persisted metadata and values, then exercise new expressions.
  Restart and backup/restore checks cover supported data and independent existing
  admission state. A CN-only restart is not full storage recovery.
- Exact-content static checks, related package tests, canonical BVT comparison,
  benchmark measurements and exact-head CI. A skipped upgrade job is NOT_RUN.
- Review and deployment QA remain separate; this document and local tests do not
  establish reviewer acceptance.

### Persistence acceptance for shutdown upgrade

| Boundary | Required terminal result | Evidence boundary |
| --- | --- | --- |
| Supported release data -> new reader | Legacy views, DEFAULTs, stored generated values and indexes retain results, types and overloads 0/1; 512/513-character controls remain unbounded. Fresh writes and indexed reads agree with scans; existing values are not silently recomputed. | Pin release tag, resolved commit, binaries and data identity. Historical receipts remain historical; final supported-release validation is still required. |
| New expressions and independent admission | Create/read DEFAULT, generated, check, index and view owners; combined expressions retain unrelated floors and a failed ALTER leaves existing metadata intact. | Same-binary injected-floor and SQL tests validate local behavior; no new JSON_VALUE floor is asserted. |
| Restart and backup/restore | Restart the deployment against existing data and restore an older supported backup on the new binary; verify old and new owners, index contents and subsequent writes. | A CN-only restart does not prove storage recovery or full deployment restore. Existing independent admission state must remain valid. |
| Prepared plans -> new session | Re-prepare legacy and clause-bearing SQL after upgrade/restart; verify defaults, response policies and metadata. | Same-binary prepared tests do not prove the upgraded-session outcome. |

### Performance acceptance

Use `BenchmarkJSONAdmissionExistingPaths` and
`BenchmarkJSONValueLegacySmallPath` with identical 16/4096-element documents,
`$.keep`, Go/platform/native inputs and serial single-CPU runs on base and head.
Record ns/op, B/op, allocs/op, document bytes and repeat variation. Warm up and
verify bytes/results outside timing; reset/copy/admission/execution work stays
inside its respective timed loop. Benchmark setup owns and releases its vectors;
the admitted-execution measurement intentionally excludes ingestion.

The target base is `bab4b3286a0dd5683a9b291763817722233e586c`. A comparison may
use a Go source overlay restoring every base-modified Go file and removing every
PR-added Go file, with the same two benchmark files retained in both arms. Record
the overlay and exact inputs; do not label a handwritten DecodeJson substitute
or the seven-argument benchmark as legacy execution. Neither a matched comparison
nor functional success approves the cost. Record a retained-cost or consolidation
decision against measured common-path overhead before final acceptance. Both
existing scalar/structural and new canonical/range/UTF-8 invariants must survive
any later consolidation in the existing validation owner.

The candidate repair consolidates canonical admission into the existing bounded
structural traversal and reuses the scalar validator. It eliminates the second
descendant traversal and width-dependent child/frame slice allocations, while
retaining canonical ranges/key order/UTF-8, scalar encoding/nonfinite checks,
depth and both serialized-byte and node/entry work budgets. The admitted executor
still validates descendants on every call; no cache, provenance or mutable-vector
trust shortcut is introduced. Incremental publication of this repair does not
approve its remaining cost or the complete design/persistence acceptance gate.
Measurements and the independent decision belong in the task/PR evidence rather
than a permanent execution diary here.

### Evidence status

Historical measurements and revision-4 validation diary remain in git/PR history
at immutable source `88c5d3e65be9a887a5094ae584d371d35d4e95d2`; they are not
new-head PASS evidence. In particular, admitted-extraction benchmarks exclude
generic vector ingestion/unmarshal and cannot answer their regression cost.

Earlier local, CI, BVT, benchmark and upgrade receipts remain tied to their
actual revisions, binaries and environments. They do not validate this candidate
or establish the final supported-release upgrade, full recovery, deployment QA,
or acceptance of the legacy lookup cost. A focused base/head ingestion/unmarshal
and legacy two-argument JSON_VALUE comparison must include actual measurements,
equivalent inputs, environment and variation, plus a retained-cost decision.
Structural and canonical JSON safety checks remain required while that evidence
is pending.
