- Status: draft — implementation review remains blocked pending an independent design decision
- Start Date: 2026-08-26
- Authors: iamlinjunhong
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27553
- Issue: https://github.com/matrixorigin/matrixone/issues/27536

# Explicit read-only MongoDB queries for external-table scans

## Decision record

This document is the stable design artifact for issue #27536 and PR #27553.
It is intentionally a draft: the implementation is not the approval authority.
The required independent design review must record the exact commit containing
this document and a PASS/REQUEST_CHANGES decision before implementation
approval can proceed.

**Scope and trigger.** This is a feature, not a bug fix.  It adds a SQL-visible
operation selector, changes the `MongoScan` plan/pipeline payload, and crosses
catalog, frontend, planner, protobuf, compile, execution operator, MongoDB
driver, resource/metrics, and diagnostic boundaries.  It therefore meets both
the five-owner size trigger and the public-wire, security, lifecycle, and
capacity complexity triggers in `feature-design-review.md`.

**Proposed decision.** Accept a narrowly scoped, read-only `__mo_query`
envelope for a MongoDB external table.  It admits one strict Extended JSON
filter or one allowlisted aggregation pipeline, carries only validated BSON and
a digest through the execution plan, and retains MatrixOne's existing mapping,
tenant, timeout, cursor, conversion, batch, and source-limiter boundaries.
Arbitrary commands, writes, cross-collection access, and automatic SQL
aggregate pushdown remain out of scope.

## Problem, goals, and invariants

The existing MongoDB external-table scan can push a conservative subset of
MatrixOne predicates and projection.  It cannot express MongoDB-only filters,
and a reducing MongoDB aggregation must otherwise transfer and decode all raw
documents before MatrixOne aggregates them.  The result is unnecessary network,
memory, and conversion work for workloads whose remote aggregation reduces
cardinality substantially.

Goals:

- select either a MongoDB `find` filter or a read-only aggregation pipeline
  through the hidden `__mo_query` column;
- preserve the legacy scan exactly when that selector is absent;
- fail closed before any MongoDB call on malformed, ambiguous, unsafe, or
  over-limit input;
- retain the external-table mapping and MatrixOne statement/resource controls;
- provide digest-only diagnostics: query fields and literals must not reach
  logs, plans, telemetry, errors, or protobuf text diagnostics.

Non-goals:

- arbitrary MongoDB commands, writes, JavaScript, cross-collection access, or
  server/metadata introspection;
- automatic translation of SQL grouping, predicates, or expressions into a
  user pipeline;
- schema inference, multiple query candidates, `IN`, or a user-controlled
  timeout/disk-spill option;
- changing MongoDB credentials, table authorization, catalog persistence, or
  the ordinary `find` path.

The central invariant is: **a user selector may affect only one validated,
collection-scoped read operation; all execution and diagnostic representations
are bounded, and only the execution representation may contain BSON values.**
MatrixOne still evaluates normal SQL residual filters and converts returned BSON
according to the external-table mapping.

## Contract and examples

The public surface is exactly one constant equality selector:

```sql
SELECT device_id, ts
FROM mongo_events
WHERE __mo_query = '{"filter":{"device_id":"pump-1"}}';

SELECT device_id, event_count
FROM mongo_events_aggregate
WHERE __mo_query = '{"pipeline":[
  {"$match":{"device_id":"pump-1"}},
  {"$group":{"_id":"$device_id","event_count":{"$sum":1}}},
  {"$project":{"_id":0,"device_id":"$_id","event_count":1}}
]}';
```

The top-level envelope has exactly one of `filter` or `pipeline`.  It is strict
MongoDB Extended JSON v2, which preserves BSON type information across the Go
driver boundary ([MongoDB Extended JSON v2](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)).
The implementation canonicalizes the accepted value and hashes that canonical
source with SHA-256; only a shortened digest is diagnostic output.  The raw
source is reconstructed only if the hidden column is explicitly projected.

For a filter, the operator combines the validated filter with safe automatic
pushdown candidates.  For a pipeline, ordinary SQL predicates remain MatrixOne
residual predicates because placing them before an opaque user pipeline can
change semantics.  Both paths append a final mapping projection.  If column
pruning retains no mapped output columns, that projection is a small inclusion
row carrier rather than an exclusion projection, preserving cardinality without
downloading arbitrary fields.

## End-to-end architecture and ownership

```text
SQL __mo_query constant
  -> planner/compile extracts one candidate and removes its selector
  -> mongodb.ParseUserQuery: strict JSON + size/depth + allowlist validation
  -> MongoScan protobuf: kind + validated BSON + SHA-256 digest
  -> execution CN revalidates BSON and digest
  -> mongoscan: Find(filter) or Aggregate(pipeline), final projection
  -> existing cursor/converter/batch limits -> MatrixOne residual/filter/project
```

| Boundary | First owner | Contract |
| --- | --- | --- |
| Synthetic column identity | `pkg/catalog` | Reserved hidden `__mo_query` identity is shared with query-driven external scans; existing real legacy columns are not reclassified. |
| SQL candidate and residual separation | `pkg/sql/compile` | At most one constant selector; query predicate is removed only after selection; unsupported shapes fail before remote work. |
| Parse, canonicalization, policy | `pkg/sql/mongodb/user_query.go` | Strict JSON, duplicate-key rejection, 64 KiB serialized bound, depth 32, at most 16 stages, and allowed shape/operator validation. |
| Plan transport | `proto/plan.proto` and `pkg/pb/plan` | The producer emits validated BSON, kind, digest, and flags; execution revalidates every received payload. |
| Remote operation and result carrier | `pkg/sql/colexec/mongoscan` | One collection operation per scan, final projection, cursor conversion, optional hidden-column output. |
| Driver and resource control | `pkg/sql/mongodb/driver.go` | `Aggregate` is driver-neutral, uses statement context and `allowDiskUse=false`; existing client lease and limiter remain authoritative. |
| Human-facing diagnostics | frontend, compile, plan explain, protobuf text | Raw SQL/BSON is replaced by a safe representation before any diagnostic carrier; operation and digest remain available. |

The linearization point is successful `ParseUserQuery` plus `ApplyUserQueryToPlan`
on the compile-owned scan copy.  The execution CN does not trust that point: it
deserializes BSON, applies the same stage/value checks, verifies canonical
source/digest equality, and only then invokes the MongoDB driver.  A cached or
remote plan consequently cannot bypass the compile-side policy.

## Security and authorization boundary

The query is data, not a command language.  The parser accepts only an object
with one supported envelope, rejects duplicate JSON/BSON keys and trailing
values, and applies an allowlist to stages and `$` operators recursively.
Allowed stages are `$match`, `$project`, `$set`, `$addFields`, `$unset`,
`$group`, `$limit`, `$skip`, and `$count`; each has a shape-specific
validation. `$sort`, `$unwind`, `$push`, and `$addToSet` are excluded from the
initial resource envelope. Unknown stages/operators and server-side JavaScript
BSON values are rejected. This deliberately rejects `$out`, `$merge`,
`$lookup`, `$graphLookup`, `$unionWith`, `$collStats`, `$indexStats`,
`$currentOp`, and `$planCacheStats`, rather than trusting a read-only MongoDB
credential as the only control.

The existing connection lookup verifies account and version, and the table's
configured database, collection, mappings, secrets, and source limiter remain
the authorization boundary.  The selector contains no namespace/credential
override.  Query literals can be sensitive, so diagnostics use only operation
kind and a validated digest; `MongoScan.MarshalText`, textual/structured
EXPLAIN, frontend statement recording, compile origin SQL, remote
`ProcessInfo`, and running-SQL tracking consume the redacted representation.
Binary protobuf marshal/unmarshal preserves BSON only for execution.

## Lifecycle, resource bounds, and failure behavior

`Prepare` validates the execution plan, resolves runtime dependencies and the
connection, then acquires the existing per-source limiter and client lease.
After either resource is acquired, every failure path closes the cursor if
opened and releases the lease and limiter once.  `Reset` and `Free` use the same
idempotent resource cleanup path; `Call` closes resources on conversion,
projection, cursor, cancellation, or batch-limit error.  No background worker
or retry queue is introduced.

The statement context bounds the initial driver operation and every `getMore`.
Driver `Aggregate` explicitly sets `allowDiskUse=false`; users cannot override
it. Existing scan-row, raw-byte, batch-byte, conversion-error, value-size, and
source-concurrency limits are unchanged. The initial envelope is deliberately
narrowed to 64 KiB, depth 32, and 16 stages. It excludes `$sort`, `$unwind`,
`$push`, and `$addToSet`; each permitted accumulator has scalar value state,
while `$group` key cardinality remains subject to MongoDB's configured
aggregation-memory limit. Every explicit query receives a context deadline of
the shorter configured socket timeout or 30 seconds; that context also bounds
every `getMore` and the MatrixOne-side lifetime of the operation. MongoDB may
return a document already buffered locally without consulting the context, so
the scan checks the deadline again before consuming each buffered document.
consume CPU until it observes cancellation, so this is not represented as a
server CPU quota. With `allowDiskUse=false`, a grouping operation that exceeds
the server aggregation-memory limit fails rather than spilling. The accepted
rollout envelope is one operation per `max_parallelism=1` mapping, bounded
input/plan memory, the existing source-concurrency limiter, and this
client-facing 30-second budget. Final projection
limits transferred fields; a zero-column scan uses a bounded row carrier.
Metrics add only fixed labels (`find`/`aggregate` and lifecycle phases), never
query content.

This does not claim that a MongoDB aggregation has a universal fixed CPU or
memory cost: `$match`, `$group`, and `$count` may scan an operator's collection
until cancellation or a MongoDB resource limit. The rollout is therefore opt-in
and admits only the above envelope. Any expansion to sorting, fan-out, array
accumulators, larger input/stage/depth limits, a higher mapping parallelism, or
a longer timeout requires a new capacity decision, workload measurement, and
regression before allowlisting it.

Cancellation before admission returns without a lease; cancellation after
admission reaches the driver context and cleanup.  A stale/disabled or
wrong-tenant connection fails before opening a cursor.  Invalid remote BSON,
digest mismatch, invalid stage, and cached-plan reuse fail closed on the
execution CN.  Empty selector pruning validates catalog identity but opens no
source cursor.  Reset/reuse increments the normal operator generation and does
not retain a prior query/cursor/client lease.

## Wire compatibility, delivery, and fallback

`MongoScan` adds protobuf fields numbered 16–21.  The legacy zero values retain
the old find behavior; newer readers treat a zero kind as no explicit query.
Older readers ignore unknown fields and consequently cannot execute the new
operation semantics. `MORPCVersion33` is therefore the capability gate: a
nonzero `user_query_kind` is rejected while the service-local oldest-live
deployment version is below 33, rather than silently falling back to an
unfiltered find. The gate is checked while compiling, immediately before a
remote scope is serialized, while it is decoded, and in `MongoScan.Prepare`.
Deployment raises that version only after all receivers understand the payload
and lowers it before rollback. This follows the
protobuf field-evolution rule to add rather than repurpose field numbers
([Proto3 update guidance](https://protobuf.dev/programming-guides/proto3/#updating)).
There is no catalog/on-disk migration, so backup/restore and downgrade have no
persistent data conversion.  Removing/rolling back the feature means routing
users back to the legacy query surface; existing tables and scans continue to
work.

Rollout is opt-in because only statements referencing `__mo_query` use the new
path.  Operability is through operation/digest diagnostics and existing cursor,
duration, byte, row, conversion, and limiter metrics.  The immediate fallback
for a rejected selector or operational fault is the legacy scan or a normal
MatrixOne query; there is no automatic retry with a broader MongoDB operation.

## Alternatives and tradeoffs

| Alternative | Decision | Reason |
| --- | --- | --- |
| Status quo: SQL-only conservative pushdown | Rejected | Cannot express supported MongoDB-only filters or reducing aggregation, so it retains avoidable transfer/decode cost. |
| Full MongoDB command passthrough | Rejected | Makes authorization, write, cross-collection, metadata, JavaScript, resource options, and diagnostics unbounded. |
| Denylist of dangerous stages/operators | Rejected | Future MongoDB additions could become executable by default; a recursive allowlist is fail closed. |
| Automatically translate SQL aggregation | Deferred | Requires a separate SQL/BSON semantic compatibility matrix for nulls, types, Decimal128, collation, overflow, and errors. |
| Store raw Extended JSON in plan/logging | Rejected | Simpler transport but violates the sensitive-literal boundary and exposes selectors in nested protobuf diagnostics. |

The selected design trades broad MongoDB expressiveness for a collection-scoped,
bounded and auditable read surface.  It also deliberately leaves ordinary SQL
filters local after a user pipeline, trading possible extra transfer for
semantic correctness.

## Validation and acceptance evidence map

| Invariant | Cheapest proof and owning tests | Public-path/extra evidence |
| --- | --- | --- |
| Envelope parsing, canonical digest, strict duplicate/unsafe/oversize rejection | `pkg/sql/mongodb: TestParseUserQuery*`, `TestUserQueryPlanRevalidationFailsClosed` | Local MongoDB E2E filter/pipeline rejection coverage. |
| Compile selection, residual separation, empty candidate and legacy behavior | `pkg/sql/compile: TestConfigureMongoUserQuery*` | Existing external-table execution path in CI. |
| BSON transport revalidation and safe diagnostics | `pkg/sql/mongodb` plan round trips; `pkg/pb/plan` diagnostic tests; `pkg/sql/compile: TestCompileMongoDBQueryDiagnosticsAreRedacted` | CI UT and coverage jobs on the implementation head. |
| Find/pipeline invocation, mapping projection, zero-column row carrier, cancellation and cleanup | `pkg/sql/colexec/mongoscan: TestMongoScan*` including filter, pipeline, large irrelevant field, reset/free/error controls | Local MongoDB E2E runner uses a real server command profiler: the raw MO aggregation returns four MongoDB documents and the reducing pipeline returns one; the JSON report records both counts. |
| Wire rollback | `pkg/sql/compile: TestMongoScanRemoteProtocolValidationAtSendAndReceiveBoundaries`; `pkg/sql/colexec/mongoscan: TestMongoScanExplicitQueryRejectsRolledBackProtocolBeforeMongoCall` | Compile at v32, lower to v31 before send/receive/prepare, and fail before a MongoDB operation. |
| SQL statement/prepared/parse-failure redaction | `pkg/frontend` focused redaction/statement-recording tests | Statement telemetry and remote `ProcessInfo` diagnostic tests in dependent compile/frontend paths. |
| External SQL contract | `test/mongodb/mongodb_e2e_local.go` with minimum fixture documents | CI compose/BVT lanes remain the service-level regression net; no new distributed case is added because the feature's real MongoDB fixture is isolated in its existing test-owned runner. |
| Concurrency/lifecycle | focused normal and race tests for MongoDB/mongoscan/frontend; repeated reset/free controls | Existing CI UT/coverage exercises the package graph; no new global state or background worker exists. |

The test fixtures use minimum documents and explicit local runner ownership;
they do not use sleep-based synchronization or a throughput assertion as a
functional oracle.  Before implementation approval, the record must attach the
exact head, terminal test results, relevant race mode, and the real MongoDB E2E
result.  A design-only edit does not invalidate already green code evidence;
rebasing requires rechecking only changed base-side contracts.

## Risks and approval questions

1. **Operator allowlist evolution:** every added MongoDB operator/stage requires
   a new security/resource semantics decision and regression before admission.
2. **Capacity expansion:** the initial envelope and 30-second cap above are the
   accepted rollout limit. Any broader stage/operator set or budget is a new
   design decision owned by the MongoDB connector maintainers, with an attached
   workload measurement; it is not an open blocker for this revision.

These are continuing admission conditions, not open design questions for this
revision. The independent approval record must name the reviewed document
commit and state whether the selected invariants and validation plan PASS.
Until then, the design status remains `draft` and implementation approval is
blocked.
