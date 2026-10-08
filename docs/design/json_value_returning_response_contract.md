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

### Versioned compatibility decision (revision 5, 2026-10-08)

Status: independent maintainer approval pending. This revision supersedes
revision 4/v101; superseded details remain in PR history rather than this contract.
Approval must identify
this revision's immutable commit and an independent approver. Author replies,
thread resolution and approvals of earlier revisions do not approve this one.

The current allocation candidate is MORPC v107 for JSON_VALUE function ID 462,
overload 2. Actual main `bab4b3286a0dd5683a9b291763817722233e586c` already
allocates v101-v106 to other landed capabilities. v107 is not reserved: other
live candidates may use the same next number. Landing order requires maintainer
coordination and each later PR must integrate its actual cumulative predecessor
and reallocate. No independent approval is carried forward to revision 5.
All gates, tests and this decision must move together if that allocation changes.

The planner requires the deployment-wide MOProtocolVersion to be at least 107
for RETURNING, ON EMPTY or ON ERROR. Bare two-argument calls keep the legacy
plan and its NULL-document short circuit. In the new clause-bearing contract,
a non-NULL invalid path is a hard error even when the document is SQL NULL.

Both current sender and receiver validate overload 2 against v107 before remote
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

Do not admit pre-v107 CNs after committing a v107 durable floor. The existing
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

### Evidence status for revision 5

Historical measurements and revision-4 validation diary remain in git/PR history
at immutable source `88c5d3e65be9a887a5094ae584d371d35d4e95d2`; they are not
new-head PASS evidence. In particular, admitted-extraction benchmarks exclude
generic vector ingestion/unmarshal and cannot answer their regression cost.

Current maintenance validates the merged grammar and protocol107 predecessor106
boundaries separately. Real predecessor/new binaries, old-node rejoin, durable
floor restart/restore/downgrade, deployment QA and independent approval remain
required and are not claimed complete. A focused base/head ingestion/unmarshal
and legacy two-argument JSON_VALUE comparison must include actual measurements,
equivalent inputs, environment and variation, plus a retained-cost decision.
Preserve structural and canonical JSON safety checks while that evidence is pending.
