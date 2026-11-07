# Mixed temporal predicates: index access and costing

Issue #31619 reported incorrect results for an indexed `DATETIME` compared with
`NOW()`, a `TIMESTAMP`. #31624 restored correctness on main by refusing unsafe
index bounds and retaining the SQL predicate. This design adds a cost-based
performance path for eligible comparisons while retaining that correctness fix.

The candidate-range proof is in [temporal index ranges](20261028-temporal-index-ranges.md).
The shared owner and API contracts are in
[index planning owners](20261107-index-planning-owners.md).

## Contract and scope

A `DATETIME` is a wall-clock value; a `TIMESTAMP` is an instant. Mixed
comparisons use the session time zone and timestamp precision. Their serialized
index keys cannot be compared as though the representations were identical.

For every admitted query:

- Every row for which the original comparison is SQL TRUE is inside the
  physical candidate. The candidate may include additional rows.
- The complete original predicate is evaluated on the logical row, including
  its columns when they are not projected. Unknown physical metadata and
  unsupported bounds must not omit rows.
- Statistics select an access path; they never authorize a row omission.
  An unavailable observation leaves the ordinary safe plan available.
- Rewrites preserve observable errors, warnings, authorization, transaction
  and snapshot identity. A rejected optional attempt publishes no partial plan.

AUTO admission is limited to a parameter-free, single-filter SELECT with a
fixed-offset time zone, a positively proved literal or `CURRENT_TIMESTAMP`
bound, local scheduling, and a covering regular index whose leading part is
the temporal column. Prepared templates and bound prepared executions use their
existing reusable-plan ownership and carry no candidate execution input or
candidate-specific rebuild path. Prepared AUTO, named-zone AUTO, LIMIT/OFFSET, aggregates, joins, FORCE INDEX,
and non-covering backfill remain on existing paths. This is
best-effort optimization: no query is promised an index scan. In particular,
the FORCE INDEX and LIMIT query from #31619 does not use this AUTO path.
The demonstrated selected path is a DATETIME key compared with
`CURRENT_TIMESTAMP`. A public TIMESTAMP-key/DATETIME-bound index selection has
not been established; an explicit DATETIME cast currently retains BASE.

## Range and execution

`types.BuildTemporalCandidate` owns inversion between the two time domains.
It accounts for comparison direction, strictness, precision, rounding, NULL,
zero and out-of-calendar physical tails. Its result is a necessary range, not
an exact replacement for SQL comparison. Full or unprovable bounds fail open;
the execution-time expression reads the current statement clock and zone.
For named zones the bound stays Full; no transition-specific inverse is used.
The reader receives a marked necessary block filter and the covering scan
retains a separately remapped complete row filter. Copy, fold and protocol
transport preserve the filter proof. An empty temporal range is widened before
reader pruning so the residual determines the result. Ordinary same-type
comparisons retain their normal index path.

The fixed-zone scalar comparison fast path uses direct offset arithmetic only
for proved fixed zones and canonical DATETIME values. `Local`, named zones,
zero sentinel and physical tails use the existing calendar conversion. Precision
rounding and operand direction remain identical to ordinary comparison.

## Ownership and publication

The existing regular-index selector owns enumeration, scoring and final choice;
normal candidates keep their priority and fallback rules. Temporal candidates
carry only attempt-local cost estimates and never publish a separate cache or
winner state. The ordinary covering-scan constructor supplies shared index
metadata, and a single validated column mapper handles direct and composite
index keys for both ordinary and temporal scans. Ordinary and temporal callers
prepare their own row and block filters. A single publication path registers
names, appends the node, inherits hints and updates scan statistics. Existing
index-merge branches can stage nodes without publishing them.

The temporal path borrows relations returned by the ordinary resolver. Before
publication it verifies account, base/index relation pair, parent, schema and
index membership, transaction, workspace revision, snapshot and catalog
visibility. It builds mappings and a candidate bind tag locally. Failure,
cancellation, changed state or exhausted work before publication returns the
existing error or safe path without changing shared tags, names or mappings.
Borrowed relations are not closed by the observer. Once publication succeeds,
a late cancellation cannot retroactively replace the selected result.

The original owner is analyzed before rewrites. The analysis independently
records whether the choice may need replanning and whether the original scan
has a proved safe single mixed predicate; uncertainty never grants admission.
Eligibility is checked again for the current physical plan. Admitted
clock-dependent queries are replanned for each
execution even when the previous choice was BASE; a cached sparse INDEX choice
must not survive a transition to a dense bound. Ordinary prepared and EWM
plans retain their existing cache ownership. Compile-time clock, time-zone or
placement drift causes an authorized safe replan; a rejected plan is not run.

## Cost and resource bounds

Index selectivity uses ordinary column min/max, NDV and null statistics. The
candidate pays for block-granular input, decoding, residual, output and
construction. Only actual finalized storage filters can earn BASE pruning
credit. For an eligible covering index, the existing relation synchronously
observes cached metadata for the original BASE predicate and the current
snapshot. The observation creates no subscription, performs no file read,
retains no new cache, and is discarded if any metadata, stats, transaction,
subscription or transport identity changes. A held client pointer identifies
the transport: every production reconnect publishes a newly constructed client.
The regular logtail recovery state machine remains the sole recovery owner.

The cached block walk is bounded by the shared 65,536-unit attempt budget;
relation, index, column, expression, name and payload limits apply before
copying or evaluation. Cancellation and resource errors retain their existing
owner; incomplete optional metadata falls back. Statistics' object-overlap
estimate is not required: the complete observed blocks provide the BASE input.

For latency comparison, the BASE input work is divided by the estimated number
of local readers, capped by matching blocks, the query's `MaxDop` and CPU
availability. TP execution uses one reader, following `CalcQueryDOP`. The index
estimate remains conservatively serial because its hidden layout and final
parallelism are not yet known. The bounded query-shape check prevents the
execution-type observation from walking arbitrary plan structures. This is a
cost heuristic, not a proof of balanced execution or a guarantee of optimal
selection. Different object layout, final statistics, load and placement can
change actual latency.

The temporal scan is constrained to the current CN. Existing protocol fields
preserve same-version plan and time-zone state; this change makes no new rolling
mixed-version guarantee. It adds no catalog, on-disk format, background task or
persistent statistics service.

## Alternatives and acceptance

Keeping main's safe BASE path has the least complexity but does not provide the
previously observed sparse access benefit. A general dynamic-plan cache or
unrestricted expression/index framework would require broader lifetime,
diagnostic and distributed proofs. This design keeps the useful covering path,
shares ordinary owners, and falls back when its narrower proof is unavailable.
Historical performance samples are context, not validation of a new revision.

Acceptance requires independent SQL-result and typed-plan checks for both
operand directions, comparison operators, precision, fixed-zone boundaries,
NULL/sentinel/tails, unsupported expressions and fail-open readers. Verify
AUTO against `IGNORE INDEX`, INDEX and BASE controls, sparse/dense transitions,
prepared/same-domain controls, DDL/index removal, permissions, snapshots,
metadata drift, cancellation and transport changes. Run affected normal and
race tests, public SQL/BVT and configured incremental SCA.

Performance evidence uses identical main/candidate source, data, topology and
warmup. Establish A/A noise first, then at least three interleaved ABBA/BAAB
rounds with at least 100 timed samples per phase; record p50/p95, plan,
blocks/rows, CPU and allocations. Include shuffled and ordered sparse/dense
layouts, transitions, same-domain and prepared controls. A/A p50 noise above
3% or p95 above 8% requires investigation. The shuffled selective control must
use INDEX, scan less and beat main; meaningful regressions in dense, ordered or
unaffected paths require diagnosis and correction rather than a broad
no-regression claim.
