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

### Versioned compatibility decision (candidate revision 7, 2026-10-09)

Status: independent maintainer approval pending. This candidate updates revision
6 after an intervening main allocation, retaining shutdown-upgrade acceptance;
superseded details remain in PR history rather than this contract.
Approval must identify
this revision's immutable commit and an independent approver. Author replies,
thread resolution and approvals of earlier revisions do not approve this one.

The current source is a provisional MORPC v110 validation candidate for
JSON_VALUE function ID 462, overload 2, based on
`8cc9167cb0a04479cd4c2f083aece5130f8c1946` (v109). It preserves main's
functional-index metadata and generated-key maintenance capability at v107 and
isolated user-variable NULL regexp migration history at v108, plus native Unicode
collation identities in persisted information_schema.COLUMNS metadata at v109;
v109 nodes must still reject this candidate's seven-argument JSON_VALUE contract.
It is not the final landing
build. The [maintainer landing decision](https://github.com/matrixorigin/matrixone/pull/28275#issuecomment-6054197734)
ordered #28935, #28947, then this PR against the former v106 base. Its conditional
v107/v108/v109 assignments are superseded by the actual functional-index landing
in #29472 and the subsequently landed v108 migration and v109 Unicode capabilities. A refreshed
maintainer decision is required before final landing;
neither unmerged predecessor capability is implied by this conflict repair.
The final executable must include the actually preceding landed capabilities
from verified main. Do not reserve empty numbers or mix independent candidate
binaries. The source gates, immediate-predecessor
tests, final design and earlier-capability preservation tests move together
after cumulative integration. No independent design approval is carried forward.

The planner requires the deployment-wide MOProtocolVersion to be at least 110
for RETURNING, ON EMPTY or ON ERROR. Bare two-argument calls keep the legacy
plan and its NULL-document short circuit. In the new clause-bearing contract,
a non-NULL invalid path is a hard error even when the document is SQL NULL.

Both current sender and receiver validate overload 2 against v110 before remote
execution. Those checks do not retrofit an old receiver. The accepted deployment
scope is a shutdown upgrade: stop all old processes, replace the binaries with
the final cumulative build, then start and admit the deployment before authoring
new expressions. Mixed-running-version and old-node-rejoin experiments are
excluded by the [current acceptance review](https://github.com/matrixorigin/matrixone/pull/28275#pullrequestreview-5453629095).
This scope does not waive persisted-data readability or durable-floor safety.
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

Do not admit pre-v110 CNs after committing a v110 durable floor. The existing
floor advances monotonically; deleting views/generated expressions or indexes
does not by itself lower it. In-place rollback below that floor is unsupported
unless a separately supported, validated floor-lowering procedure exists.
This change adds no force-downgrade mechanism. Deployment must keep old binaries
stopped after the durable floor advances. Removing/rebuilding dependent metadata
is necessary for any future downgrade procedure, but is not proof that downgrade
is permitted.

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

Required independent decision: approve the final capability allocation, strict
response semantics, sender/receiver and persisted-owner admission, shutdown
upgrade and rollback restriction, and retained validation/performance disposition.

## Required evidence

- Temporal helper, executor and binder tests plus canonical JSON_VALUE BVT:
  NULL/DEFAULT/ERROR ON ERROR, unused defaults, column inputs, legacy controls,
  generated/indexed values, CREATE/ALTER VIEW and prepared reuse.
- Real SQL DDL builders at predecessor/new thresholds and read-only versus
  authoring admission; manually constructed protobuf owner tests are supplemental.
- Actual pre-feature/final binaries in sequence: create legacy persisted data,
  shut down the predecessor deployment, upgrade, and read/rebind that data before
  authoring the new overload. Verify admitted creation, complete storage restart
  and restore as specified below. Integer thresholds and a CN-only restart do
  not establish storage recovery or predecessor-data compatibility.
- Exact-content static checks, related package tests, canonical BVT comparison,
  benchmark measurements and exact-head CI. A skipped upgrade job is NOT_RUN.
- Independent approval of this revision followed by implementation re-review and
  deployment QA. None is implied by this document or local test success.

### Persistence acceptance for shutdown upgrade

Let V be the final integrated JSON_VALUE capability under the refreshed landing
decision and P its actual immediate predecessor. The provisional v108/v109
fixture is not a substitute for the final P/V pair. Retain the predecessor data
directory and backup; record both binary SHAs, catalog/floor state, SQL results
and process shutdown/start evidence. The deployment operator owns the stop/replace/
start prerequisite; the existing HAKeeper/catalog admission owner owns the
monotonic durable floor. This feature introduces no separate admission controller.

| Boundary | Required terminal result | Available coverage and remaining gap |
| --- | --- | --- |
| Predecessor data -> new reader | Legacy views, DEFAULTs, stored generated values and indexes retain results, types and overloads 0/1; JSON/text 512/513-character controls remain unbounded; existing supported stored JSON subtypes remain readable. New inserts and index lookup agree with a table scan. No automatic generated-value rewrite. | Legacy UT/BVT controls exist; actual predecessor-authored data followed by shutdown upgrade remains required. |
| Read floor -> authoring floor | A committed read floor permits reads while an incomplete authoring fence rejects new DEFAULT/generated/check/index/view metadata, including folded expressions, with no tables/columns/index residue. Rejected ALTER VIEW preserves the published definition. | `TestJSONValuePersistedPublication` covers real SQL with injected floors; final cumulative P/V execution remains required. ON UPDATE JSON_VALUE is rejected syntax, not a supported owner. |
| Admitted authoring -> storage recovery | Create and use new persisted owners only after V is admitted. Stop and restart CN, TN and the durable admission/catalog services against the same data; verify floor >= V, view rebinding, stored values, fresh DEFAULT/generated writes and indexed reads. | The existing test restarts only its CN; full durable storage/floor recovery remains NOT_RUN. A successful CN rebind is not this oracle. |
| Backup/restore -> new deployment | Restore new metadata/data into a deployment at least V; verify floor >= V, old and new owners, generated/index contents and fresh writes. Deleting dependent objects must not lower the floor or grant a downgrade. | Backup/restore and floor monotonicity through restore remain NOT_RUN. |
| Prepared plans -> new session | Re-prepare legacy and clause-bearing SQL after upgrade/restart; verify defaults, response policies and metadata. Session prepared handles are not treated as durable catalog objects. | Existing SQL/binary prepared-reuse tests cover same-binary behavior; final upgraded-session verification remains required. |

Acceptance requires independent approval of the final immutable design revision,
the actual cumulative build, the above terminal evidence and deployment QA.
The shutdown scope removes mixed-running-version/rejoin experiments from this
acceptance set; it does not permit an old binary below the committed floor to
serve the upgraded data. In-place downgrade below V remains unsupported.

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

Current source tests the merged grammar and provisional protocol110 predecessor109
boundaries separately. Final cumulative integration, predecessor-data shutdown
upgrade, durable floor restart/restore, deployment QA and independent approval
remain required and are not claimed complete. A focused base/head ingestion/unmarshal
and legacy two-argument JSON_VALUE comparison must include actual measurements,
equivalent inputs, environment and variation, plus a retained-cost decision.
Preserve structural and canonical JSON safety checks while that evidence is pending.

The main-v109 conflict repair and v110 gates are unvalidated local candidate
content until their applicable planner, sender/receiver, persisted-owner and
functional-index/migration/Unicode preservation checks actually run. Prior v108 JSON_VALUE
measurements remain bound to their historical source; they do not prove the final
cumulative executable, final predecessor-data compatibility or durable recovery.
