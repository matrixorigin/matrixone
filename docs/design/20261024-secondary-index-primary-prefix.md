# Secondary-index selection with composite primary and cluster keys

## Problem and ownership

A predicate on the first component of a composite primary key does not identify
one row. It may select a large tenant history while a complete secondary unique
key identifies at most one row. Three existing decisions prevented that access:
composite-key lowering consumed the logical component predicate before index
matching, point matching treated the first primary component as a complete key,
and a selective sorted predicate returned before evaluating secondary points.

`FilterList` remains the single owner of semantic row predicates. Secondary
matching, hint validation, candidate ranking and backfill remain in the existing
index-selection code. `BlockFilterList` remains optional derived pruning state;
it cannot supply missing logical facts. The existing composite rewriter owns
physical key encoding. Storage, protocol, locking and execution do not change.

## Phase boundary

For scans with a composite physical primary or cluster key and a materialized
ordinary secondary index, retain logical predicates through access selection.
Other scans keep their existing early lowering. This is a metadata-defined
class; generated columns, parameter syntax and particular schemas are
irrelevant.

The existing recursive composite rewriter takes an explicit phase. Its late
phase processes surviving deferred base scans, including the base side of an
index backfill. An index-only replacement has no source secondary metadata and
needs no base-key lowering. The same lowering and constant-folding owners are
reused. Repeated early optimization and late lowering must be stable.

Late lowering occurs after FORCE and automatic selection, before runtime filters
and physical column remapping. Existing helpers preserve consumed component
block filters and append compound pruning filters. Refresh statistics without
resetting chosen hash/shuffle decisions, sort predicates, and recompute complete
column-reference counts because logical columns can disappear and hidden key
columns can appear. Final remapping and block-filter deduplication retain their
existing owners. Do not reconstruct semantic predicates from serialized keys.

## Access decisions

Only equality on every primary-key component proves the existing cheap primary
point path. Explicit FORCE bypasses that cost preference while retaining access
legality, hint filtering, encoding and residual checks.

A selective leading sorted predicate remains a base-scan preference only with
finite positive, nondefault scan cardinality and a valid predicate estimate.
Absent statistics can leave selectivity unset at zero; that is not cost
evidence. A meaningful known-statistics zero remains valid. Let a secondary
point candidate escape it only with stronger evidence. Keep the
covering/range/spatial classes excluded by the old shortcut outside that escape.
Main uses its existing encoded-access scorer and requires a valid candidate with
strictly less work than the base scan. Unknown scores do not bypass this saved
preference. Existing normal candidate ranking and small-table policy remain.

4.2 uses its existing selectivity ranker rather than importing main's cost
framework. A complete equality lookup on a declared UNIQUE key has a schema
bound of at most one index row. With valid nondefault cardinality, cap that
candidate's estimated selectivity at one/table rows and preserve strict
comparison with the sorted preference. Partial UNIQUE, nonunique and range
access do not acquire this bound. The existing small/unflushed-table policy
prefers a complete UNIQUE point over unknown nonunique candidates when no sorted
preference is active. Within each unknown class it keeps catalog order, and
retains a legal nonunique fallback. It does not invent cardinality or compare
unset estimates as numeric evidence. Broad-output admission estimates cannot
veto a selected, cardinality-proven singleton.

## Risks and validation

Earlier join and distribution passes now see logical rather than serialized
predicates for the deferred class. Statistical formulas are unchanged, but plans
can change. Preserve chosen distribution during late lowering and validate
indexed composite joins, ranges, broad secondary predicates, covering access and
nonindexed controls. This is not a universal no-regression claim.

Planner tests cover missing/known statistics, prefix versus complete primary
keys, hints, threshold boundaries, covering/backfill, composite cluster keys,
block pruning disabled, repeated phases and distribution/reference preservation.
The SQL regression covers generated NULL/empty keys, prepared reuse, rollback
and FOR UPDATE. Real-service validation checks exact results and metadata,
actual rows read, competing lock conflict/release, and matched performance
controls on both branches. Floating-point equality/ranges must stay outside
serialized regular-index matching: physical signed zeros differ although SQL
equality merges them. Main already enforces this shared classifier boundary; 4.2
uses the same guard before any singleton proof. Exact predicates remain on the
base scan. No new persistent state or execution path is added. Serialized nonunique covering lookups also
retain exact decoded string predicates: escaped NUL bytes can make the physical
prefix a superset of SQL equality. 4.2 extends its existing residual predicate
owner, preserving its NULL checks and integer fast path.

Residual pushdown must map every reference in the entire predicate, including
nested arguments and both sides of a column comparison. Covering scans and
backfill filters share the regular-index column map: raw single-part keys,
serialized complete index parts, and the stored primary key. Truncated prefix
parts cannot represent the base column; covering projections and residuals
requiring them must use backfill, unless the full value is in the stored PK. The map is bounded by index
and primary-key width and is built only when an extra filter exists. Unmapped
or foreign-table references, volatile expressions, and reference-bearing forms
unsupported by the expression remapper stay on the base scan. Mapping failure
abandons optional pushdown before publishing any filters. The original base
predicate remains intact; the index copy cannot change SQL results. Regression
coverage checks mixed-column predicates, nested mapped expressions, range
access, NULL and prepared reuse against literal SQL result oracles.
