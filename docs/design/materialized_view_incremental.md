# Incrementally Maintained Materialized Views

Status: approved design revision for the implemented scope; implementation complete

Owner issue: https://github.com/matrixorigin/matrixone/issues/24553

Implementation PR: https://github.com/matrixorigin/matrixone/pull/27615

Merge unit: one atomic implementation PR. None of the capability gates in this
document will be merged as separately released product stages.

Last updated: 2026-09-02

This document defines the contract and invariants implemented by PR #27615.
The supported scope is the explicitly admitted aggregate and UNION ALL subset;
the capability families listed as out of scope below are design inputs for
future work and are not advertised by this implementation.

## Design decision

Implement materialized views as consumer-owned physical tables maintained from
one consistent ISCP change stream. One `MaterializedViewConsumer` owns initial
snapshot hydration, tail processing, target and auxiliary state, failure
recovery, and watermark publication. It does not use `ConsumerInfo.InitSQL`, a
pre-registration CTAS, or one independent CDC job per source.

The final implementation in this PR uses one canonical persistent incremental
operator specification. A numeric `format_version` remains an internal
compatibility discriminator so unknown persisted data fails closed; it is not a
feature generation or a staged delivery label. Temporary flat and UNION-specific
encodings in the current branch must be normalized before merge rather than
released as separately supported product versions.

Refresh selection is typed and fail-closed:

- `FAST` accepts only a definition for which the planner produces complete,
  retractable and resource-bounded operator state;
- `FORCE` uses that incremental plan when available and otherwise selects a
  common-boundary complete refresh;
- `COMPLETE` always evaluates and atomically replaces the complete result.

Every capability claimed by this document is implemented behind its admission
and protocol gates. No intermediate implementation state is a supported
release.

## 1. Problem and user contract

MatrixOne has ordinary views but lacks durable pre-aggregates that are
continuously maintained for dashboards and alerts over a trace/metrics
firehose. The primary workload is append-heavy, while expired or corrected
events also require delete and update correctness.

The design stores an MV as a physical table and supports two refresh timings:

- `ON CHANGE`: asynchronously initialize and maintain the MV through ISCP;
- `ON DEMAND`: initialize or replace the result only when the user executes an
  explicit full refresh.

It supports three method policies:

- `FAST` / `INCREMENTAL`: creation succeeds only for the implemented
  incremental SQL subset. Runtime delta failure is fail-closed and never
  silently changes semantics to a full refresh.
- `COMPLETE` / `FULL`: every refresh evaluates the complete definition.
- `FORCE` / `AUTO` (default): use an incremental plan when one can be built;
  otherwise use complete refresh. If a runtime incremental transaction fails,
  it is rolled back before a complete refresh is attempted at the same boundary.

`ON CHANGE` is eventually consistent: source DML does not wait for MV
maintenance. For a successfully published tail watermark W, target rows and
auxiliary state equal evaluation of the definition at W. The implementation
does not define a universal maximum freshness lag.

### Goals

- Keep dashboard and alert queries cheap over sustained append-heavy
  trace/metrics input while remaining correct for delete and update.
- Preserve one snapshot/change boundary across every direct source.
- Make incremental eligibility explicit, deterministic and inspectable.
- Bound or reject state, fan-out and per-iteration work before they can make a
  FAST job unserviceable.
- Recover after retry, restart or executor reassignment without duplicate or
  lost contributions.

### Non-goals

- Oracle-style PCT or source-partition change tracking.
- Treating external, temporary, subscription, internal-state or cyclic sources
  as ordinary durable change streams.
- Claiming synchronous source-transaction semantics for asynchronous
  `ON CHANGE`; synchronous `ON COMMIT` has a separate source-DML contract.
- Silently approximating exact SQL or silently changing FAST into COMPLETE.

### First-principles invariants

1. For every published boundary W, target rows and all auxiliary operator state
   equal one evaluation of the MV definition at W.
2. Target changes, state changes and the tail watermark have one transaction and
   one commit point. None may become visible alone.
3. Every source row delta contributes exactly once to every matching operator
   input or UNION branch, including after retry, duplicate delivery and restart.
4. Every multi-source result uses one common `[fromTS,toTS]`; a faster source
   cannot advance the MV past a slower source.
5. Only the active fenced consumer generation may mutate target, state or
   progress. Cancellation and reassignment terminate the prior owner.
6. FAST eligibility requires a reproducible old-row contribution and a bounded
   update algorithm. Unsupported, volatile, corrupt or over-budget state fails
   before watermark publication.

## 2. SQL surface

Canonical syntax is:

```sql
CREATE MATERIALIZED VIEW mv AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH FAST ON CHANGE
AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH COMPLETE ON DEMAND
AS SELECT ...;

REFRESH MATERIALIZED VIEW mv;
DROP MATERIALIZED VIEW mv;
```

The default is `REFRESH FORCE ON CHANGE`. The parser also accepts the aliases
`INCREMENTAL`, `AUTO`, and `FULL`. `ON CHANGE` is the default timing when the
timing is omitted. `ON COMMIT` is not supported.

Only `COMPLETE/FULL ON DEMAND` can be manually refreshed. Other combinations
reject `REFRESH MATERIALIZED VIEW` rather than racing the ISCP owner.

The parser AST, plan protobuf, statement classification, database remapping,
prepared-statement schema collection, and privilege extraction all recognize
the new DDL. Refresh currently follows the ALTER VIEW/database ownership
privilege path; a dedicated MV privilege is not introduced by this PR.

### Representative observability flow

```sql
CREATE TABLE traces (
  event_ts TIMESTAMP,
  service VARCHAR(128),
  endpoint VARCHAR(256),
  region VARCHAR(32),
  status_code INT,
  duration_ms BIGINT,
  trace_id UUID
);

CREATE MATERIALIZED VIEW trace_minute
  REFRESH FAST ON CHANGE
AS
SELECT date_trunc('minute', event_ts) AS minute,
       service, endpoint, region,
       count(*) AS requests,
       sum(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS errors,
       sum(duration_ms) AS duration_sum,
       avg(duration_ms) AS duration_avg,
       min(duration_ms) AS duration_min,
       max(duration_ms) AS duration_max,
       count(DISTINCT trace_id) AS traces
FROM traces
GROUP BY date_trunc('minute', event_ts), service, endpoint, region;
```

Creation registers the job with `startFromNow=false`. The consumer first
hydrates the physical target and exact-distinct state from one ISCP snapshot.
For tail input, an insert contributes `+1`; a delete contributes the old row as
`-1`; and an update contributes `-old,+new`, including filter entry/exit and
group movement. The consumer consolidates signed group deltas, updates target
and state, and publishes the tail watermark in the same transaction.

A top-level `UNION ALL` compiles each compatible leaf independently. Its hidden
identity is `(branch_id, serialized_group_key)`, so two branches that produce
the same visible row remain two SQL bag rows. If one physical source appears in
multiple leaves, ISCP subscribes to it once and the consumer routes its delta to
every matching branch.

## 3. Definition scope and refresh selection

### 3.1 Source relations

An MV definition must be a top-level select with one to sixteen direct,
persistent ordinary base tables. Sources are resolved and their durable table
IDs are recorded when the ISCP job is registered. Repeated physical sources are
deduplicated in the job source set.

External, temporary, cluster, source, subscription/special, logical view,
materialized-view, and internal MV-state relations are rejected. This avoids
maintenance over relations without the ordinary-table historical-change and
snapshot contracts, and prevents recursive MV dependencies.

### 3.2 Implemented incremental SQL subset

Incremental planning accepts either one direct base table in a top-level
`SelectClause`, or a top-level `UNION ALL` whose leaves are direct single-table
aggregate branches with compatible output and hidden-state schemas. It accepts:

- an optional deterministic row-local `WHERE`;
- ordinary `GROUP BY`, or `SELECT DISTINCT` rewritten as grouping;
- `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, and `AVG(expr)` by algebraic delta;
- `MIN(expr)` and `MAX(expr)` by recomputing only affected groups;
- exact `COUNT(DISTINCT expr)` using persistent value-multiplicity state;
- two to sixteen compatible `UNION ALL` branches. A stable branch ID is included
  in the hidden group identity, so equal visible rows from different branches
  remain distinct. One physical source is subscribed once and its deltas are
  routed to every matching branch;
- insert, delete, and update tails.

Every group expression must be projected exactly once. The scalar-expression
allowlist consists of column references, literals, arithmetic and unary
operators, comparisons, boolean/null/range predicates, casts, `CASE`, and
`date_trunc`, `coalesce`, `ifnull`, `abs`, `floor`, and `ceil`. Unknown or
volatile functions, subqueries, windows, aggregate nesting, and unsupported
expression nodes do not produce an incremental specification.

The following are **not incrementally supported by this implementation** and
remain out of scope until a future design revision:

- `HAVING`;
- `SUM(DISTINCT)` and `AVG(DISTINCT)`;
- `UNION DISTINCT`, `INTERSECT`, `EXCEPT`, nested set operations inside a
  branch, or `UNION ALL` branches whose output/aggregate-state shapes differ;
- JOIN, CTE, subquery, window, `ORDER BY ... LIMIT`, ROLLUP, CUBE, GROUPING
  SETS, Top-K, percentile/quantile, bitmap/HLL, or user-defined aggregate state.

A query must first pass the direct ordinary-source admission in section 3.1.
For an admitted query that cannot produce an incremental specification, `FAST`
rejects creation while `FORCE` stores no specification and takes the complete
refresh path. The current FAST error identifies the unsupported single-table
incremental-aggregate class but not every individual construct. Derived tables,
ordinary views, special relations, and unsupported top-level set operations
fail source admission rather than receiving a complete refresh. A compatible
top-level `UNION ALL` can use FAST; an admitted but non-incremental `UNION ALL`
can use FORCE/COMPLETE.

### 3.3 Implemented complete-refresh scope

`FORCE` and `COMPLETE` accept planner-executable definitions over one to
sixteen direct ordinary sources. This includes multi-table JOIN definitions.
Every source reference in the stored refresh query is rewritten to read the
same ISCP `toTS`; therefore a full result never mixes source boundaries.

Compatible top-level `UNION ALL` branches can already use incremental
multi-source maintenance. Other current multi-source definitions, including
JOIN, use complete refresh until their operator state in section 12 is
implemented. A multi-source job is gated by the MORPC protocol version that
introduced its serialized ISCP shape. All MV consumers use this capability
gate, including single-source jobs, because older consumers cannot safely
claim persisted MV jobs during reassignment.

### 3.4 Current refresh boundary

| Input/policy | Current behavior | Reason |
| --- | --- | --- |
| Initial snapshot of every `ON CHANGE` MV | Complete build | No target/operator state exists; hydrate from one consistent snapshot |
| Tail with a valid current incremental specification under `FAST/FORCE ON CHANGE` | Incremental | The consumer can turn insert/delete/update into signed operator deltas |
| Admitted `FORCE ON CHANGE` query without a spec | Complete refresh | No retractable operator state represents that query yet |
| FORCE incremental transaction failure | Roll back, then complete refresh at the same `toTS` | Partial target/state or a watermark beyond a failed boundary cannot publish |
| FAST incremental transaction failure | Error, no fallback | FAST is the user's incrementality requirement |
| `COMPLETE/FULL ON CHANGE` | Complete refresh at every change boundary | The user explicitly selected complete replacement |
| `COMPLETE/FULL ON DEMAND` | Manual complete refresh | This is the only currently valid ON DEMAND combination |
| Definition failing source admission | Creation error | A common snapshot/change/source-identity contract is missing or dependencies cannot be extracted safely |

Complete refresh means deleting the old target, evaluating the whole definition
at the common boundary, rebuilding required state, and committing atomically.
MatrixOne does not support PCT, so a complete refresh cannot be narrowed to only
changed partitions.

## 4. Physical representation and durable metadata

The MV is stored as an ordinary physical relation rather than a logical view.
Its catalog properties persist:

- the MV marker, refresh method, and refresh timing;
- source database/table names and the one-to-sixteen source list;
- source SQL and executable refresh SQL;
- a versioned, base64-encoded incremental specification when eligible.

The merge target has one canonical incremental operator specification. It
records source identities and columns, operator IDs and edges, typed key and
payload schemas, filters, group expressions, aggregate kinds, visible output
columns, hidden state columns, serialized group/row identities, retraction
strategy, resource admission data, and every auxiliary state-relation ID.

The persisted object includes one numeric `format_version` solely for future
compatibility. At merge, the planner emits one supported format and the
consumer accepts that format only; unknown formats fail closed. Numeric format
values do not name feature phases. The branch's provisional direct-aggregate
and UNION envelope encodings are implementation history and must be converted
to the canonical schema before merge.

Incremental targets use a binary `serial_full(...)` group key as a hidden
primary key. Hidden state includes group row count and SUM/AVG sum/count state.
Full-refresh-only targets use MatrixOne's hidden auto-increment fake primary
key; creation initializes that sequence before a refresh can run.

MIN/MAX and exact COUNT(DISTINCT), and every later stateful operator, use
consumer-owned auxiliary relations named from deterministic MV and operator
identities. Their namespace is reserved. Ordinary users cannot create, read,
mutate, or independently drop these relations. The design does not rely on a
single state table remaining sufficient as JOIN, Top-K and window state are
added.

## 5. ISCP lifecycle and multi-source extension

This PR adds the dedicated `ConsumerType_MaterializedView`. It deliberately does
not use `ConsumerInfo.InitSQL` and does not run CTAS before registration.

For `ON CHANGE`, creation registers one ISCP job with `startFromNow=false`:

```text
register job
  -> ISCP consistent snapshot
  -> MaterializedViewConsumer complete initial build
  -> tail iterations after the snapshot boundary
  -> incremental or complete maintenance
```

The externally relevant state transitions are:

| State | Owner | Success transition | Failure/restart transition |
| --- | --- | --- | --- |
| Registered, no trusted state | ISCP executor | Admit one snapshot worker | Remain schedulable; no watermark is published |
| Snapshot running | Active consumer generation | Atomically publish hydrated target/state, then finalize the snapshot watermark | Roll back/discard unpublished work; a new generation repeats or resumes only a validated shadow build |
| Tail pending/running | Active consumer generation | Commit target/state/tail watermark and schedule the next boundary | Roll back; FAST stops/retries the same boundary, FORCE may rebuild at that same `toTS` |
| Complete refresh running | Active consumer generation or manual caller | Atomically replace the target/state generation | Retain the prior visible generation; do not advance progress |
| Paused/errored | Executor/catalog owner | Explicit retry, resume or rebuild after the cause is recorded | Remain non-advancing and query-visible as unhealthy |
| Dropping | DDL transaction | Fence generations, unregister jobs, remove owned state and target | Abort DDL if ownership cannot be proved; never drop another MV's state |

Worker admission is the ownership transfer point. Transaction commit is the
tail publication point. Snapshot completion is not advertised until the result
transaction succeeds. A generation fence is checked before every publication,
not inferred from in-memory worker existence.

The job retains `SrcTable` as its compatibility anchor and adds `SrcTables` for
the complete source set. Dirty-table detection considers every source, and one
iteration collects the union of all merged jobs' sources over the same
`[fromTS,toTS]`. Each delivered batch carries `SourceTableID` and is routed
only to consumers whose job source set contains that ID; per-source table
definitions are used to resolve CDC batch indexes when schemas differ.
Boundary-only full refresh consumers drain the stream without retaining
table-sized payloads.

The executor polls changes every second independently of the broader task-sync
tick. It marks a submitted iteration pending only after worker admission. On a
new executor generation, persisted Pending/Running rows are normalized to a
schedulable state because no old worker can still own them. LSN restoration,
generation fencing, cancellation/drain, and final-status handling prevent an
abandoned or old generation from publishing over the current one.

Watermark ownership remains the existing ISCP model:

- for a tail, the MV consumer applies target/state changes and calls the normal
  retriever watermark update in the same SQL transaction;
- for the initial snapshot, retriever watermark update is intentionally a
  no-op and successful iteration finalization persists the completed snapshot
  watermark afterward. A crash in this window can repeat the atomic full
  replacement, but cannot advertise a completed snapshot before its result
  transaction succeeds.

No independent MV watermark or progress cache is introduced.

## 6. Refresh algorithms

### 6.1 Snapshot and complete refresh

The consumer opens one refresh transaction, deletes the current physical
result, evaluates all source references at the common boundary, inserts the new
result, rebuilds required exact-distinct state, and commits. Delete and insert
are one transaction, so readers cannot observe a half-replaced target.

Manual `ON DEMAND` refresh acquires a shared database lock and exclusive MV
table lock, then performs the same atomic replacement in the caller's
transaction snapshot instead of adding `MO_TS` clauses.

### 6.2 Incremental tail

ISCP insert rows provide all referenced source values. CDC tombstones contain
row identity but not every old value, so the PR adds an engine `RowIDReader`
contract and a DistTAE implementation. The reader retrieves each deleted row
at the snapshot immediately before that tombstone's commit. This is necessary
when a row was inserted or updated more than once inside one tail interval;
reading every delete at the iteration's original `fromTS` would be incorrect.

An update is therefore one negative old-row contribution plus one positive
new-row contribution. Filter entry/exit and group movement follow the same
rule. Native value extraction preserves NULL and temporal type precision.

For a signed row S in group G:

- `COUNT(*)` adds S;
- `COUNT(expr)` adds S only when expr is non-NULL;
- `SUM(expr)` maintains sum and non-NULL count so deletion of the final value
  returns SQL NULL instead of zero;
- `AVG(expr)` maintains sum and non-NULL count and derives the quotient;
- `MIN/MAX` records G and re-evaluates only affected groups at `toTS`;
- exact `COUNT(DISTINCT expr)` adjusts `(aggregate, G, encoded value)`
  multiplicity and changes the visible count only on 0-to-1 or 1-to-0;
- a group whose row count reaches zero is removed.

Group joins use ordinary equality for planner-proven non-null keys and
NULL-safe equality otherwise. Distinct/group identities retain serialized
values rather than relying on an unverified hash.

Delta work is processed progressively. Logical row chunks are capped at 32,768
rows and generated SQL at 8 MiB; an oversized chunk is split recursively. Each
internal SQL statement advances its statement boundary while sharing the same
transaction.

Incremental eligibility must preserve the ordinary aggregate and grouping
domains, not merely accept an expression syntax tree:

| Domain | Required contract |
| --- | --- |
| Integer/decimal SUM and AVG | Persist a widened state type that has the same overflow/error behavior as complete evaluation; scale and final casts are part of the specification |
| FLOAT/DOUBLE | Preserve NaN, infinities and signed-zero behavior observed by GROUP BY, HAVING and later predicates; otherwise use affected-group rebuild or COMPLETE |
| CHAR/VARCHAR | Serialize the resolved collation and pad-space identity, not raw bytes unless raw-byte equality is the SQL grouping domain |
| Temporal | Preserve source precision and the resolved timezone/session-independent expression semantics |
| NULL | Use a typed NULL marker distinct from every non-NULL encoding; COUNT, SUM/AVG state and NULL-safe group matching follow SQL semantics |
| Binary/UUID/JSON or new types | Remain ineligible until their full-row identity, comparison and state serialization are proven round-trippable |

Every aggregate implementation must document its zero-row, all-NULL,
insert-last/delete-last, overflow and old-value retraction laws. One unsupported
aggregate or expression rejects the complete FAST candidate atomically; the
planner never emits a partially incremental definition.

### 6.3 Failure and fallback

Parse, bind, row lookup, state, target, watermark, cancellation, or transaction
errors roll back the entire incremental transaction. `FAST` returns the error.
`FORCE` may then start a separate full-refresh transaction at the same `toTS`,
only after the failed delta transaction has rolled back. COMPLETE never attempts
deltas.

## 7. DDL, DML, and dependency behavior

Only internal refresh contexts may write hidden MV columns. INSERT, UPDATE, and
DELETE against either the MV target or its state relation are rejected through
planner/binder and DistTAE checks.

Dropping an MV unregisters every active ISCP generation by target identity,
validates the auxiliary relation marker, and drops the state table with the
target. Job names alone are not used because underscore-based names can
collide.

Dropping or renaming a source marks every referencing MV job as errored even if
the job is anchored on another source. Planning a query against the physical MV
revalidates the persisted definition sources; if a source is missing or is no
longer an allowed relation, the query fails instead of returning stale rows.
Generic source DDL also runs in deployments and tests where the optional ISCP
catalog has not been bootstrapped. A missing `mo_iscp_log` therefore makes
dependency invalidation a no-op, because no MV job can exist in that state;
all other catalog and executor errors still abort the DDL transaction.

ALTER MATERIALIZED VIEW, source-schema evolution with automatic incremental
spec regeneration, CASCADE/RESTRICT dependency policy, and MV-as-source are not
implemented.

## 8. Compatibility and atomic delivery

The SQL grammar and plan protobuf add public persistent shapes. Generated
`mysql_sql.go` and `plan.pb.go` are regenerated in the PR. Legacy single-source
ISCP jobs continue to use `SrcTable`; the multi-source extension is additive
and protocol-gated.

No provisional MV format from this unmerged branch is a compatibility contract.
Before merge, all writers, readers, tests and generated metadata must use the
one canonical format described in section 4. After merge, any incompatible
format change requires an explicit migration or shadow rebuild; it must not
reinterpret old state in place.

Mixed-version admission is still required because the PR changes distributed
job and catalog shapes. A node that does not advertise the required MV and
multi-source protocol capability cannot create or own such a job. Backup,
restore and PITR must treat definition, target generation, auxiliary state,
job log, source identities and watermark as one logical object. Restore either
resumes from a validated common boundary or rebuilds a shadow generation; it
must not combine pieces from unrelated generations. These are merge gates for
the single PR, not deferred product-version work.

The rollback path is to drop the MV, which unregisters its job and removes
owned state. This feature is not wired to automatic optimizer query rewrite,
so removing it does not change ordinary query plans.

## 9. Observability and performance behavior

The PR registers these metrics:

- refresh transaction duration labeled by `incremental|full` and result;
- incremental source rows labeled `insert|delete`;
- FORCE incremental-to-full fallback count;
- successful watermark wall-clock lag histogram.

Before merge it must also expose bounded-cardinality per-MV status through SQL,
including hydration/running/paused/error state, selected refresh strategy,
watermark and wall-clock freshness, last success/error, retry/fallback, state
bytes, affected groups and backlog. Metrics should use stable low-cardinality
labels; raw MV names must not create an unbounded metrics label space.

The intended algebraic hot path is proportional to changed rows, distinct-key
transitions, and affected MIN/MAX groups. Complete refresh remains proportional
to source size. Freshness is also bounded by ISCP polling, transaction/planner
overhead, target write amplification, and available disk throughput.

## 10. Validation represented in this PR

Committed unit tests cover:

- parser/formatter syntax and refresh aliases;
- source validation, physical markers, reserved state identity, DML rejection,
  source dependency validation, refresh policy, and incremental-spec planning;
- common-boundary SQL rewriting for one and multiple sources;
- initial/full payload skipping and FAST no-fallback behavior;
- signed delta SQL, batching/size limits, NULL joins, temporal precision,
  exact-distinct specification/state, and pre-tombstone row lookup;
- multi-source dirty detection/batch indexes, retained row IDs, job-log JSON,
  snapshot/tail status and watermark behavior;
- executor restart recovery, admission ordering, fencing, cancellation,
  rollback, and protocol-version gating.

The implementation branch was also exercised with remote SQL scripts and
long-running append benchmarks. Those scripts are intentionally not committed
as BVT in this PR. Every retained result must record exact base/head, host,
hardware, storage, topology and configuration in the PR; an unversioned result
or a result from an earlier semantic implementation is not merge evidence.

The single-PR merge gate requires deterministic public SQL tests
for snapshot plus insert/delete/update tails, complete multi-source refresh,
FAST rejection, FORCE fallback selection, source-drop query failure, direct-DML
rejection, ON DEMAND refresh, and restart recovery. BVT polling must be bounded
to avoid suite timeouts.

Each public behavior must map to a named UT or BVT oracle. The oracle compares
the MV result with complete evaluation at the same boundary and covers NULL,
duplicates, old/new group movement, rollback, duplicate delivery and cleanup.
The evidence table in the PR must distinguish committed deterministic tests,
remote temporary tests, benchmarks and CI, and must identify their exact head.

## 11. Industry comparison and target capability union

There is no single "mainstream MV" contract. The target is the useful union of
the following independently documented models, not a claim that every product
supports every row in the table.

| System | Relevant public behavior | MatrixOne implication |
| --- | --- | --- |
| [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/26/dwhsg/basic-materialized-views.html) | FAST/FORCE/COMPLETE, ON COMMIT/ON DEMAND, log-based and partition-change refresh, aggregate/join/UNION ALL rules, nested MVs, query rewrite, refresh diagnostics | Reference for refresh policy, capability explanation, query rewrite, and dependency DAG; PCT is an industry comparison only and is outside this MatrixOne design |
| [PostgreSQL](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html) | General defining SQL with complete manual refresh, `CONCURRENTLY`, `WITH [NO] DATA`, table storage/index options | Reference for general fallback, deferred population, nonblocking replacement, and physical design |
| [ClickHouse](https://clickhouse.com/docs/materialized-view/incremental-materialized-view) | Insert-trigger incremental views for real-time append and separately scheduled refreshable views with atomic replace/append and dependencies | Reference for a low-overhead append fast path and scheduled complete refresh; not a delete/update correctness baseline |
| [Snowflake](https://docs.snowflake.com/en/user-guide/views-materialized) | Automatic single-table maintenance, query rewrite, clustering, AVG/COUNT/MIN/MAX/SUM, variance/stddev, bitwise aggregates and HLL; no JOIN/HAVING/window/ORDER BY/LIMIT | Reference for single-table aggregate breadth, optimizer integration, clustering, and maintenance-cost visibility |
| [Materialize](https://materialize.com/docs/transform-data/optimization/) | Continuous insert/update/delete maintenance for joins, aggregates, DISTINCT, MIN/MAX and grouped Top-K; arrangements, group-size hints, temporal filters, freshness introspection | Reference for retractable operator state, keyed arrangements, resource hints, and freshness semantics |
| [RisingWave](https://docs.risingwave.com/sql/commands/sql-create-mv) | Continuous backfill plus maintenance, joins, grouped Top-N, tumble/hop/session windows, emit-on-window-close, cascading MVs and online controls | Reference for streaming operator breadth, event-time policy, cascading pipelines, and backfill admission |

This PR implements the admitted capability families above. Any future
capability must add its state, failure, recovery, resource and validation
contracts before it is admitted as FAST.

The comparison table describes capabilities of the referenced systems and
same-PR design inputs, not capabilities already present in the current branch. In
particular, this PR implements log-based ISCP/CDC maintenance but does **not**
implement Oracle-style Partition Change Tracking (PCT) or partition-level MV
refresh. Neither is a target of this design.

## 12. Designs for the remaining mainstream capability families

### 12.0 Moving complete-refresh cases to incremental maintenance

The goal is not to teach ISCP every SQL construct. Before this PR merges, its
provisional flat aggregate and UNION descriptions are normalized into one
canonical incremental operator graph. Planning
returns one of three typed outcomes:

- `INCREMENTAL(spec, cost)`: insert/delete/update algorithms, durable state
  schema, and resource bounds are defined;
- `COMPLETE(reason)`: the query and source contract are valid, but no safe and
  bounded incremental operator exists yet;
- `REJECT(reason)`: source, lifecycle, or security contracts are insufficient,
  so complete refresh cannot guarantee correctness either.

`FAST` accepts only `INCREMENTAL`; `FORCE` prefers `INCREMENTAL` and otherwise
uses `COMPLETE`; `COMPLETE` does not compile delta operators. Errors use stable
construct/reason codes such as `MV_FAST_UNSUPPORTED_HAVING` and
`MV_FAST_UNBOUNDED_JOIN_STATE`, rather than one generic single-table error.

#### Operator graph and intermediate state

Every operator has a stable ID, kind, input edges, typed key/payload schema,
retraction capability, state-relation ID, estimated rows/bytes, and version.
Intermediate relations belong to one consumer generation in a reserved
namespace; ordinary SQL cannot read, mutate, or independently drop them. The
initial snapshot hydrates target and state through this same graph, so snapshot
and tail do not become separate implementations.

| Query capability | Durable intermediate state | Incremental action |
| --- | --- | --- |
| Global aggregate | One fixed zero-dimensional group key | Consolidate every delta into that group |
| Aggregate-free GROUP BY/DISTINCT | `(group key)->row multiplicity` | Publish/retract the visible row on 0-to-1/1-to-0 |
| HAVING | Complete group aggregate state plus visible bit | Re-evaluate the predicate and handle false/true transitions |
| SUM/AVG DISTINCT | `(operator,group,value)->multiplicity` plus distinct sum/count | Change the aggregate only on 0-to-1/1-to-0 |
| UNION ALL | Stable branch ID plus branch row/group identity | Route a source delta to matching branches and preserve duplicates |
| UNION/INTERSECT/EXCEPT | Per-branch multiplicity for each output row | Derive visibility from the set predicate |
| JOIN | Keyed arrangements, payload, multiplicity, and match count per input | Probe other sides and emit signed join-product deltas |
| ROLLUP/CUBE/GROUPING SETS | `(grouping-set ID,group key)` | Fan one input delta out to a finite set of groups |
| Top-K/window | Ordered multiset `(partition,order key,row identity)` | Update one partition and its K/rank boundary |
| Percentile/quantile/HLL/bitmap/UDAF | Typed state with declared merge/retract/serialize contracts | Permit FAST only when mutable-source retraction is correct |

Target changes, intermediate-state changes, and the ISCP tail watermark commit
in one SQL transaction. Nothing publishes on partial failure. A fenced old
generation cannot write target or state. DROP/REBUILD lets the generation owner
clean all operator state; restart reconstructs ownership from catalog spec,
state-relation IDs, and watermark rather than an in-memory cache.

#### Generic ISCP extensions

ISCP remains responsible for changes and consistent boundaries, not SQL
aggregate/JOIN/HAVING semantics. Operator graphs require these generic
extensions to the existing multi-source path:

1. Every batch retains `SourceTableID`; the job spec maps each source to
   operator inputs/branches. One physical source is subscribed once and may
   fan out to several inputs.
2. A multi-source iteration exposes one common `[fromTS,toTS]`; the graph can
   run and commit only after every source reaches the boundary.
3. Delete/update preferably carries before-images for planner-selected columns.
   Older protocol versions or oversized payloads use the existing `RowIDReader`
   at the snapshot before each tombstone commit. Both paths produce the same
   typed row delta.
4. Snapshot data may feed the same graph in bounded chunks to reduce hydration
   peaks. Chunks write only to an unpublished shadow generation and may commit
   independently; after all chunks succeed, one transaction publishes the
   target generation and snapshot watermark. A crash can resume or discard the
   shadow generation, but can never expose a partially hydrated target.
5. Job status persists format version, generation, per-source progress, and last
   successful boundary. A CN without the canonical operator-graph and
   multi-source capabilities must reject the job rather than interpret it as a
   legacy single-source job.

Creating independent CDC jobs per source is rejected because it loses one
multi-source boundary and atomic watermark ownership. Keeping complete base rows
in consumer memory is also rejected because state is unbounded and cannot
recover after restart. Ordinary durable relations with bounded caches/spill are
the default state store.

#### Complete-refresh cases that can be removed incrementally

Delivery order follows value and state complexity:

1. fixed-key global aggregates and multiplicity GROUP BY;
2. HAVING and SUM/AVG DISTINCT (top-level compatible UNION ALL and its
   FORCE/COMPLETE source admission are implemented in this PR);
3. unique-dimension inner equi-join, followed by non-unique and multi-way joins;
4. ROLLUP/CUBE/GROUPING SETS and provably finite-fan-out subquery decorrelation;
5. Top-K, bounded windows, and event-time TUMBLE/HOP;
6. advanced aggregates with explicit retract/accuracy/memory contracts;
7. cascading MVs after the dependency DAG is complete.

A CTE alone is not a reason for complete refresh: non-recursive CTEs should be
inlined into the operator graph. Scalar/correlated subqueries that decorrelate
to join/aggregate reuse those states; a correlated subquery without a provably
bounded impact remains COMPLETE. The deterministic scalar allowlist can expand;
volatile or session-dependent expressions can never be FAST.

#### Boundaries that retain complete refresh or rejection

The following table separates inherent complete work, temporary implementation
fallback, and definitions that must be rejected:

| Case | Why it is not incremental now | Long-term treatment |
| --- | --- | --- |
| First hydration / rebuild from no trusted state | Every source row must contribute once before a tail delta is meaningful | Inherently a complete logical build; bound it with shadow-generation chunks and parallelism |
| Explicit `COMPLETE/FULL`, including manual ON DEMAND | The user selected replacement semantics | Keep complete; optimize scheduling, coalescing, and atomic shadow replacement |
| FORCE with an absent delta operator | The persistent graph cannot represent the query yet | Add the operator/state family in the delivery order above, then choose INCREMENTAL automatically |
| Estimated state/fan-out over budget | A correct algorithm exists, but its admitted resource bound does not | Add indexes, spill, compaction, cardinality hints, or a larger explicit quota; never silently run an unbounded FAST plan |
| Incremental transaction failure | The target, state, and watermark must not diverge | Retry the same idempotent boundary first; FORCE may rebuild at the same `toTS`, while FAST stops without advancing |
| Spec/state checksum, version, or generation failure | Existing state is not trustworthy for deriving the next delta | Fence it and rebuild/shadow-migrate; do not incrementally continue from corrupt or incompatible state |
| Non-equality/cross or explosive many-to-many JOIN | One row may require an unbounded scan or output fan-out | Incremental only after a bounded index/probe plan and state admission exist; otherwise FORCE is complete |
| Window with an unbounded affected suffix | One change may alter an unbounded number of published ranks/values | Use bounded frames/Top-K or rebuild the affected internal window; otherwise FORCE is complete |
| Mutable-source UDAF, HLL, percentile, or sketch without retract | The retained state cannot subtract an old value | Supply a retractable/counting state, exact ordered state, or immutable logical-window state; otherwise FORCE is complete |
| Volatile current-time/random/session expression | Re-evaluating an old row does not reproduce its original contribution | FAST always rejects; FORCE/COMPLETE is admitted only when the ordinary query reproducibility contract permits it |
| External/temporary/special/state relation or cyclic dependency | Snapshot, change identity, lifetime, security, or acyclic scheduling is not guaranteed | REJECT, not COMPLETE, until the missing source/lifecycle contract exists |
| PCT/partition-level MV refresh | MatrixOne has no source-partition change contract in this design | Outside scope; do not advertise it as incremental maintenance |

Thus, only hydration/rebuild and an explicitly selected COMPLETE policy are
inherently complete. Most SQL-shape fallbacks are implementation or boundedness
gaps and should move to the operator graph. Invalid source/lifecycle contracts
must not be disguised as complete refreshes.

#### Resource, compatibility, and validation gates

Planning/admission records estimated state rows/bytes, maximum join fan-out,
hot-group cardinality, spill threshold, and per-iteration work budget. FAST
rejects an over-budget plan; FORCE selects COMPLETE. Exceeding a hard runtime
limit rolls back rather than publishing a watermark after OOM/resource failure.
FORCE complete fallback also has a full-refresh cost budget and change
coalescing policy. When repeated full scans cannot keep up, the job reports
backpressure or pauses with an explicit error instead of building an unbounded
refresh queue.

Because no provisional MV format has been released, this PR does not preserve
parallel flat, UNION-envelope and operator-graph product formats. It converts
all producers, consumers and tests to one canonical schema before merge. That
schema is created behind an MORPC feature gate, so an incapable node cannot
schedule a job. After release, a later incompatible format must use an explicit
shadow migration or complete rebuild; rollback may continue only from a
compatible format and validated generation.

Every new operator requires signed-delta UTs and a public SQL BVT comparing the
MV with the complete definition at the same boundary, covering insert/delete/
update, NULL, duplicates, transaction rollback, consumer restart, duplicate
delivery, and state cleanup. Multi-source operators also cover interleaved
commits, one stalled source, and common-boundary recovery. Performance gates
report source throughput, freshness p50/p95/p99/max, state bytes, write
amplification, CPU/IO, and backlog drain; eventual correctness alone is not a
deliverable incremental implementation.

### 12.1 Aggregate, HAVING, DISTINCT, and set operators

The merge-target specification is one operator graph with typed state operators
and stable operator IDs. The current flat aggregate list is temporary branch
state and is not a separately supported generation.

- **HAVING**: maintain complete group state in the auxiliary relation even when
  the group is absent from the visible target. Evaluate HAVING after every
  consolidated group delta. False-to-true inserts, true-to-false deletes,
  true-to-true updates, and false-to-false changes state only.
- **SUM/AVG DISTINCT**: reuse exact `(operator, group, encoded value) ->
  multiplicity`. Only 0-to-1 and 1-to-0 transitions change distinct sum/count;
  AVG is derived from both states.
- **Variance/stddev**: store retractable count, sum, and sum-of-squares in a
  widened numeric type. Decimal overflow and floating-point error bounds are
  part of eligibility; numerically unstable types use affected-group rebuild.
- **Bitwise aggregates**: maintain per-bit one/zero counts so delete is
  reversible. A single accumulated bitmask is insufficient for retractions.
- **Approximate distinct**: append-only HLL can merge sketches, but ordinary HLL
  cannot delete. Mutable sources require a counting/retractable sketch or
  immutable logical-window sketches plus rebuild of the affected operator-state
  buckets. These buckets are internal state, not source-table PCT. FAST must
  reject an unsafe append-only state on a mutable table.
- **Percentile/quantile/histogram**: use mergeable per-partition sketches for
  append and window-close workloads. Arbitrary delete/update uses a retractable
  ordered state or affected-group rebuild; accuracy and memory parameters are
  persisted in the definition.
- **UNION ALL**: compile every branch independently and attach a stable branch
  ID. Hidden identity is `(branch ID, row/group key)`, preserving duplicates
  across branches. One physical source can feed several branches but is
  registered once.
- **UNION DISTINCT/INTERSECT/EXCEPT**: maintain per-output-row multiplicities
  per input and derive visibility from the SQL set predicate. This requires a
  full-row type-preserving key and can consume state proportional to distinct
  input rows.

Top-level compatible UNION ALL is present in the current branch baseline. The
remaining immediate work in this same PR is HAVING and SUM/AVG DISTINCT,
together with stable construct-specific FAST errors; until that code is
complete, FORCE alone may select complete refresh for an admitted definition
outside the current incremental subset.

### 12.2 Incremental JOIN

Each join input gets a durable keyed arrangement containing row identity,
projected payload, multiplicity, and source progress. A change on side A probes
the arrangements for all other sides and emits the signed join-product delta.
The entire multi-source delta is evaluated at one ISCP boundary and target,
arrangements, and tail watermark commit together.

Initial scope is inner equi-join with immutable equality keys. Fact-to-dimension
joins can optimize unique dimension keys to one lookup. Later scope adds:

- non-unique many-to-many joins with explicit state/cardinality admission;
- left/right/full outer joins using per-row match counts to publish or retract
  null-extended rows on zero-to-one and one-to-zero transitions;
- semi/anti joins using match multiplicity;
- key updates as delete-old plus insert-new;
- multi-way delta joins with optimizer-selected probe order and reusable indexes.

Cross joins, non-equality joins without a bounded index strategy, and explosive
many-to-many estimates require FORCE or an explicit state-size override. Source
foreign keys may improve estimates and unique-side selection, but correctness
must not depend on unenforced constraints.

### 12.3 Top-K, ORDER BY/LIMIT, and window functions

Grouped Top-K uses a persistent ordered multiset keyed by `(partition key,
order key, stable row identity)`. Each change updates one partition and publishes
only membership/rank differences around the K boundary. Ties and NULL ordering
are encoded exactly. Per-group cardinality hints and spill thresholds prevent a
single skewed group from consuming unbounded memory.

The first window subset is partitioned `row_number`, `rank`, `dense_rank`,
`first_value`, `last_value`, and bounded `lead/lag` patterns that can be lowered
to ordered state. General unbounded frames, arbitrary peer-sensitive updates,
and functions whose one insertion changes an unbounded suffix require complete
refresh or rebuild of the affected operator-state window until a bounded
algorithm exists. This is not source-table partition refresh.

### 12.4 Event-time windows, expiration, and late data

Add `TUMBLE`, `HOP`, and later `SESSION` logical operators with an event-time
column and persisted watermark/allowed-lateness policy. Two publication modes
are needed:

- emit-on-update keeps open-window results current;
- emit-on-window-close publishes one final result after the event watermark.

Window closure schedules synthetic negative deltas to reclaim input/operator
state. TTL based on processing time is a separate policy and must not masquerade
as event-time correctness. Late events within allowance reopen/update state;
events beyond allowance follow an explicit drop, error, or correction policy.

### 12.5 Cascading and replaceable MVs

Allow an MV as a source only after catalog dependencies form an acyclic graph.
One logical commit boundary propagates bottom-up through the DAG, and a child
cannot advertise progress newer than every parent. Shared intermediate
arrangements are reference-counted and survive while any consumer needs them.

`CREATE REPLACEMENT MATERIALIZED VIEW`/`ALTER ... APPLY REPLACEMENT` builds a
shadow target and state from a consistent boundary, tails it to the active
watermark, then atomically swaps logical ownership without changing dependent
object IDs. DROP defaults to RESTRICT; CASCADE enumerates and removes the
dependency graph transactionally.

### 12.6 Refresh lifecycle parity

- **BUILD IMMEDIATE / WITH DATA** uses the current ISCP snapshot path.
- **BUILD DEFERRED / WITH NO DATA** records an unscannable object and registers
  no active consumer until REFRESH/RESUME; queries fail explicitly.
- **REFRESH CONCURRENTLY** builds a shadow generation, catches it up, and
  atomically switches catalog identity. It requires a stable unique row key and
  permits only one refresh generation per MV.
- **REFRESH EVERY/AFTER** stores a scheduler expression, jitter, dependency
  list, and replace/append policy. Replace is snapshot-consistent; append
  requires a declared non-overlapping range key and idempotency key.
- **ON COMMIT** is a separate synchronous mode, not an alias for asynchronous
  ISCP. Source DML must invoke planner-produced delta operators inside the
  source transaction and lock targets in deterministic dependency order. The
  initial synchronous scope is restricted to one source and algebraic aggregates; multi-source
  ON COMMIT needs a deadlock and distributed-transaction design.
- **PAUSE/RESUME/REBUILD/CANCEL** fence the active generation. Resume continues
  from a valid watermark; rebuild creates a new snapshot generation.

### 12.7 Query rewrite and freshness-aware reads

The optimizer registers canonical relational expressions for fresh MVs and
matches query subgraphs by source identity, predicates, grouping, aggregates,
and compatible projections. Rewrite can roll up a finer MV to a coarser query
but never infer freshness from table contents alone.

Read policies are `FRESH` (wait for an eligible boundary), `BOUNDED STALENESS
interval` (use an MV only within the bound), and `STALE OK`. If no MV meets the
policy, the optimizer reads base tables or errors according to an explicit hint.
Plans expose the selected MV, its watermark, compensation predicate, and reason
for rejecting alternatives.

### 12.8 Partition, index, storage, and resource controls

This subsection defines same-PR storage/resource work except for PCT, which is
an explicit non-goal. MatrixOne does not currently support
PCT or partition-level MV refresh. The current implementation either applies
row-level ISCP/CDC deltas for its FAST subset or replaces the complete MV
result; it does not use source-partition change metadata to limit a refresh.

MV DDL should accept ordinary index, clustering, distribution, partition,
tablespace/storage, and retention options, but ordinary physical partitioning
does not imply PCT. Exchange/drop/truncate must become row deltas that ISCP can
represent or make FORCE perform a complete rebuild; source-partition metadata
alone cannot claim an incremental refresh.

Every stateful operator reports estimated and actual rows/bytes, spill, hot-key
skew, and write amplification. Admission uses per-MV memory/disk budgets,
backfill rate/parallelism, maintenance priority, and overload policy. State uses
normal durable storage with bounded caches rather than assuming all active
arrangements fit in RAM.

### 12.9 Administration, security, and recovery

Add `SHOW MATERIALIZED VIEWS`, `SHOW MATERIALIZED VIEW STATUS`, and
`EXPLAIN MATERIALIZED VIEW` with definition, selected strategy, unsupported
reason, source/target IDs, hydration state, watermark/freshness, last success and
error, retry/fallback, state bytes, and generation.

Dedicated CREATE/ALTER/REFRESH/DROP privileges and invoker/definer security must
be checked both at DDL and background execution. Backup/restore/PITR treats
definition, target generation, auxiliary state, dependency DAG, job log, and
watermarks as one object; restore either resumes from a validated common
boundary or rebuilds, never combines unrelated generations.

## 13. Public benchmark plan

No single public benchmark covers snapshot build, mutable IVM, streaming SQL,
and serving latency. The validation suite therefore combines reproducible
benchmarks without presenting unlike semantics as a direct ranking.

### 13.1 ClickBench and TSBS: observability append path

[ClickBench](https://github.com/ClickHouse/ClickBench) provides 10M/100M/1B
realistic web-event rows and 43 dashboard/ad-hoc queries. Use its unmodified
schema/data as the append source, then add documented MV definitions for:

- minute/hour time buckets and service-like dimensions;
- row-local error predicates and `SUM(CASE WHEN ...)`;
- COUNT/SUM/AVG/MIN/MAX and exact/approximate distinct;
- high-cardinality multi-dimensional groups and Top-K.

The official 43 queries still measure base/MV serving latency; the derived
continuous loader measures maintenance. Results must be labeled
"ClickBench-derived MV workload," not submitted as official ClickBench numbers.

[TSBS](https://github.com/timescale/tsbs) supplies generated DevOps time-series
data, ingestion tooling, and dashboard-style query workloads. Use its scale,
agent, time-window, and group distributions for sustained and burst append,
window rollup, retention delete, and concurrent dashboard reads. It is a closer
semantic match for metrics, while ClickBench has more realistic event skew.

### 13.2 Nexmark: advanced incremental SQL

[Nexmark](https://github.com/nexmark/nexmark) is the primary operator-coverage
suite for continuous streams. Adapt persons/auctions/bids to MatrixOne tables
and preserve the published event generator and query semantics. Gate capability
milestones with projection/filter, aggregate, join, window, Top-K, and
multi-way-join queries. Measure source commit to visible correct MV result, not
only engine barrier time.

RisingWave publishes a reproducible
[Nexmark result table](https://docs.risingwave.com/get-started/rw-benchmarks-stream-processing)
with query throughput, per-core throughput, CPU, and memory. Those numbers are
context only. A MatrixOne comparison is valid only after running both systems
on identical hardware, source durability, query subset, generator rate,
checkpoint/refresh semantics, and correctness checks.

### 13.3 TPC-H refresh and DBToaster: mutable relational state

Run TPC-H at SF1 for correctness and SF10/SF30 for capacity. Use Q1-derived
aggregates, join/aggregate queries, and RF1/RF2-style insert/delete batches to
exercise snapshot, append, delete, and update-as-delete-plus-insert. The full
TPC-H score is not an MV score and must not be reported as one.

The [DBToaster experimental
method](https://dbtoaster.github.io/papers/2013-dbtoaster-report.pdf) uses
TPC-H/SSB queries over update streams and is the reference for delta-heavy
relational IVM. Reuse its query/update shapes where SQL-compatible; compare
algebraic delta, affected-group rebuild, and complete refresh separately.

### 13.4 Required protocol and durations

For every workload report:

- exact git revision, database version/configuration, hardware, storage,
  topology, durability, dataset, writers, batch size, and run duration;
- snapshot/hydration time and catch-up drain time;
- source throughput without MV, with one MV, and with the full MV set;
- freshness p50/p95/p99/max measured from committed marker timestamp to the
  first query that observes the correct result;
- MV query latency p50/p95/p99/max measured separately from freshness;
- CPU, RSS, state/target bytes, disk throughput/utilization, network, retries,
  fallback, and backlog;
- exact final and sampled-boundary comparison against the definition query.

Run four tiers: deterministic correctness; 10-minute smoke; one-hour steady
state with bursts; and capacity search until freshness/backlog no longer
recovers. Include restart during snapshot, restart during tail, hot-key skew,
million-group cardinality, retention delete, dimension update, and disk
saturation. A run fails if any boundary is incorrect, backlog does not drain,
or resource growth remains unbounded after input stops.

For horizontal comparison, use PostgreSQL for complete/manual/concurrent
refresh, ClickHouse only for matched append-trigger or scheduled-refresh
semantics, and Materialize/RisingWave for mutable continuous SQL. Published
vendor numbers are never mixed with MatrixOne results as if they were from the
same machine.

## 14. Delivery gates and review questions

PR #27615 is the merge unit for the implemented scope. The list below records
future design work and is not a sequence of partial releases:

1. normalize the canonical operator specification and stabilize public SQL
   lifecycle tests;
2. HAVING, SUM/AVG DISTINCT, and construct-specific FAST errors;
3. inner/unique-dimension JOIN plus operator arrangements;
4. Top-K and event-time tumble/hop windows;
5. cascading/replacement, scheduled/concurrent refresh, and status controls;
6. query rewrite, synchronous ON COMMIT, and advanced states.

Each future gate must extend the same canonical operator schema and
compatibility table, include failure/restart tests, and pass its relevant
benchmark subset before being advertised as FAST.

Minimum acceptance criteria on the recorded reference host are:

- zero result mismatches against complete evaluation at every sampled boundary;
- at a sustained 6,000 source rows/s observability workload, freshness p99 no
  greater than 5 seconds and max no greater than 10 seconds after warm-up;
- with one algebraic MV, sustained source throughput no more than 20% below the
  no-MV control at the same offered load and durability;
- after a bounded burst ends, backlog drains in no more than twice the burst
  duration and target/state disk usage stops growing when the logical state is
  stable;
- COMPLETE/FORCE controls remain correct under the same source rate, report
  overload explicitly, and never create an unbounded refresh queue;
- restart, executor reassignment and duplicate delivery preserve exactly-once
  visible contributions and do not advance a failed boundary.

If the reference host cannot sustain the offered source load without an MV, the
test is invalid rather than an MV failure. Capacity-search results above the
acceptance point are reported separately and do not replace the fixed gate.

Reviewers are specifically asked to decide:

- whether the current snapshot-finalization and tail-transaction watermark
  contract is acceptable;
- whether FORCE runtime fallback is acceptable or must be operator-controlled;
- whether the canonical operator graph, state admission and generation model
  are sufficient for every capability in this one merge unit;
- whether ON COMMIT and optimizer rewrite need dedicated subsections and
  separate owner approvals inside this PR because they change source-DML and
  optimizer ownership respectively.

## 15. Alternatives and decision log

| Decision | Result | Reason |
| --- | --- | --- |
| Initialize through `ConsumerInfo.InitSQL` | Rejected | It splits initial result construction from tail ownership and makes snapshot/watermark handoff ambiguous |
| CTAS followed by `startFromNow=true` | Rejected | Commits between the CTAS snapshot and ISCP start watermark can be lost |
| Dedicated MV consumer with `startFromNow=false` | Selected | One owner receives a consistent snapshot and every subsequent tail boundary |
| One independent CDC/ISCP job per source | Rejected | Independent progress cannot provide one multi-source boundary or one atomic watermark |
| One multi-source job with source-tagged batches | Selected | Deduplicates subscriptions and lets one transaction evaluate all source deltas at the same boundary |
| Keep complete source rows in consumer memory | Rejected | State is unbounded, unavailable after restart and outside durable transaction ownership |
| Durable operator relations with bounded caches/spill | Selected | State is recoverable, inspectable and committed with target progress |
| Require CDC before-images unconditionally | Rejected for the current transport | Payload and protocol compatibility costs are unnecessary when row identity can reconstruct the exact pre-commit row |
| Tombstone RowID lookup only forever | Rejected as the final optimum | Correct but can amplify random reads; the canonical input permits compatible before-images with RowID fallback |
| Incremental failure silently becomes complete under FAST | Rejected | It violates the user's method contract and hides unbounded refresh cost |
| FORCE rollback followed by same-boundary complete refresh | Selected | Preserves correctness while allowing a declared automatic fallback |
| Treat numeric format values as product stages | Rejected | The feature is delivered atomically in one PR; the number exists only to reject or migrate persisted schemas after release |
| Oracle PCT/partition refresh | Out of scope | MatrixOne has no source-partition change contract in this design |
| Asynchronous `ON CHANGE` as synchronous `ON COMMIT` | Rejected | Source-transaction latency, lock ordering and distributed deadlock ownership are different contracts |

The design cannot pass while a reviewer question above remains blocking. A
resolved decision records reviewer, design commit and rationale in the PR. A
material implementation deviation updates this document and reopens only the
affected decision before implementation review continues.
