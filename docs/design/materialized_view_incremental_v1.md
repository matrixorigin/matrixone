# Incrementally Maintained Materialized Views V1

Status: draft; design review requested

Owner issue: https://github.com/matrixorigin/matrixone/issues/24553

Implementation PR: https://github.com/matrixorigin/matrixone/pull/27615

This document describes the code currently present in PR #27615. The branch
contains an implementation written before design approval; therefore reviewers
should review this contract and its invariants before treating the code as ready
for implementation review. Sections 3 and 12 separate implemented behavior
from follow-up proposals.

## 1. Problem and user contract

MatrixOne has ordinary views but lacks durable pre-aggregates that are
continuously maintained for dashboards and alerts over a trace/metrics
firehose. The primary workload is append-heavy, while expired or corrected
events also require delete and update correctness.

V1 stores an MV as a physical table and supports two refresh timings:

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

Incremental planning currently requires exactly one direct base table and a
top-level `SelectClause`. It accepts:

- an optional deterministic row-local `WHERE`;
- ordinary `GROUP BY`, or `SELECT DISTINCT` rewritten as grouping;
- `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, and `AVG(expr)` by algebraic delta;
- `MIN(expr)` and `MAX(expr)` by recomputing only affected groups;
- exact `COUNT(DISTINCT expr)` using persistent value-multiplicity state;
- insert, delete, and update tails.

Every group expression must be projected exactly once. The scalar-expression
allowlist consists of column references, literals, arithmetic and unary
operators, comparisons, boolean/null/range predicates, casts, `CASE`, and
`date_trunc`, `coalesce`, `ifnull`, `abs`, `floor`, and `ceil`. Unknown or
volatile functions, subqueries, windows, aggregate nesting, and unsupported
expression nodes do not produce an incremental specification.

The following are **not incrementally supported by the current PR**:

- `HAVING`;
- `SUM(DISTINCT)` and `AVG(DISTINCT)`;
- `UNION ALL` or any other set operation;
- JOIN, CTE, subquery, window, `ORDER BY ... LIMIT`, ROLLUP, CUBE, GROUPING
  SETS, Top-K, percentile/quantile, bitmap/HLL, or user-defined aggregate state.

`FAST` rejects these definitions at creation. The current error identifies the
unsupported incremental-query class, but does not yet report a distinct reason
for every individual SQL construct. Under `FORCE`, the incremental
specification remains empty and the MV takes the complete-refresh path.

### 3.3 Implemented complete-refresh scope

`FORCE` and `COMPLETE` accept planner-executable definitions over one to
sixteen direct ordinary sources. This includes multi-table JOIN definitions.
Every source reference in the stored refresh query is rewritten to read the
same ISCP `toTS`; therefore a full result never mixes source boundaries.

Multi-source support is complete refresh only. A multi-source job is gated by
the MORPC protocol version that introduced its serialized ISCP shape. Older
services reject creation rather than interpreting the job as a legacy
single-source job.

## 4. Physical representation and durable metadata

The MV is stored as an ordinary physical relation rather than a logical view.
Its catalog properties persist:

- the MV marker, refresh method, and refresh timing;
- source database/table names and the one-to-sixteen source list;
- source SQL and executable refresh SQL;
- a versioned, base64-encoded incremental specification when eligible.

The incremental specification is currently version 2. It records source
columns, filter, group expressions, aggregate kinds, visible output columns,
hidden state columns, serialized group-key column, strategy, and auxiliary
state-table identity. Unknown specification versions fail closed in the
consumer.

Incremental targets use a binary `serial_full(...)` group key as a hidden
primary key. Hidden state includes group row count and SUM/AVG sum/count state.
Full-refresh-only targets use MatrixOne's hidden auto-increment fake primary
key; creation initializes that sequence before a refresh can run.

MIN/MAX and exact COUNT(DISTINCT) use a consumer-owned auxiliary table named
from a deterministic hash of database and MV name. Its namespace is reserved.
Ordinary users cannot create, mutate, or independently drop this state table.

## 5. ISCP lifecycle and multi-source extension

V1 adds the dedicated `ConsumerType_MaterializedView`. It deliberately does
not use `ConsumerInfo.InitSQL` and does not run CTAS before registration.

For `ON CHANGE`, creation registers one ISCP job with `startFromNow=false`:

```text
register job
  -> ISCP consistent snapshot
  -> MaterializedViewConsumer complete initial build
  -> tail iterations after the snapshot boundary
  -> incremental or complete maintenance
```

The job retains `SrcTable` as its compatibility anchor and adds `SrcTables` for
the complete source set. Dirty-table detection considers every source, and one
iteration collects every source over the same `[fromTS,toTS]`. Each delivered
batch carries `SourceTableID`; per-source table definitions are used to resolve
CDC batch indexes when schemas differ. Boundary-only full refresh consumers
drain the stream without retaining table-sized payloads.

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

## 8. Compatibility and rollout

The SQL grammar and plan protobuf add public persistent shapes. Generated
`mysql_sql.go` and `plan.pb.go` are regenerated in the PR. Legacy single-source
ISCP jobs continue to use `SrcTable`; the multi-source extension is additive
and protocol-gated.

The implementation has no catalog feature-version negotiation, automatic
downgrade, or migration tool. Running a binary that does not understand the MV
metadata or multi-source consumer is unsupported. Backup/restore/PITR behavior
for target, state, job log, and source identities has not yet been validated as
one logical object and is not claimed by V1.

The rollback path is to drop the MV, which unregisters its job and removes
owned state. This feature is not wired to automatic optimizer query rewrite,
so removing it does not change ordinary query plans.

## 9. Observability and performance behavior

The PR registers these metrics:

- refresh transaction duration labeled by `incremental|full` and result;
- incremental source rows labeled `insert|delete`;
- FORCE incremental-to-full fallback count;
- successful watermark wall-clock lag histogram.

It does not yet expose per-MV labels, state cardinality/bytes, affected-group
count, chunk bytes, retry class, or a SQL status surface.

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

The implementation branch was also exercised with local/remote SQL scripts and
long-running append benchmarks. Those scripts are intentionally not committed
as BVT in this PR. Benchmark numbers belong in the PR description together
with exact revision and hardware; they are evidence, not a portable SLA.

Before merge, the accepted design should require deterministic public SQL tests
for snapshot plus insert/delete/update tails, complete multi-source refresh,
FAST rejection, FORCE fallback selection, source-drop query failure, direct-DML
rejection, ON DEMAND refresh, and restart recovery. BVT polling must be bounded
to avoid suite timeouts.

## 11. Industry comparison and target capability union

There is no single "mainstream MV" contract. The target is the useful union of
the following independently documented models, not a claim that every product
supports every row in the table.

| System | Relevant public behavior | MatrixOne implication |
| --- | --- | --- |
| [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/26/dwhsg/basic-materialized-views.html) | FAST/FORCE/COMPLETE, ON COMMIT/ON DEMAND, log-based and partition-change refresh, aggregate/join/UNION ALL rules, nested MVs, query rewrite, refresh diagnostics | Reference for refresh policy, capability explanation, query rewrite, dependency DAG, and possible future partition refresh; MatrixOne does not currently support PCT |
| [PostgreSQL](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html) | General defining SQL with complete manual refresh, `CONCURRENTLY`, `WITH [NO] DATA`, table storage/index options | Reference for general fallback, deferred population, nonblocking replacement, and physical design |
| [ClickHouse](https://clickhouse.com/docs/materialized-view/incremental-materialized-view) | Insert-trigger incremental views for real-time append and separately scheduled refreshable views with atomic replace/append and dependencies | Reference for a low-overhead append fast path and scheduled complete refresh; not a delete/update correctness baseline |
| [Snowflake](https://docs.snowflake.com/en/user-guide/views-materialized) | Automatic single-table maintenance, query rewrite, clustering, AVG/COUNT/MIN/MAX/SUM, variance/stddev, bitwise aggregates and HLL; no JOIN/HAVING/window/ORDER BY/LIMIT | Reference for single-table aggregate breadth, optimizer integration, clustering, and maintenance-cost visibility |
| [Materialize](https://materialize.com/docs/transform-data/optimization/) | Continuous insert/update/delete maintenance for joins, aggregates, DISTINCT, MIN/MAX and grouped Top-K; arrangements, group-size hints, temporal filters, freshness introspection | Reference for retractable operator state, keyed arrangements, resource hints, and freshness semantics |
| [RisingWave](https://docs.risingwave.com/sql/commands/sql-create-mv) | Continuous backfill plus maintenance, joins, grouped Top-N, tumble/hop/session windows, emit-on-window-close, cascading MVs and online controls | Reference for streaming operator breadth, event-time policy, cascading pipelines, and backfill admission |

MatrixOne should eventually provide all capability families below, but they
must be delivered in dependency order. Marking a feature FAST before its state,
failure, recovery, and resource contracts exist is not acceptable.

The comparison table describes capabilities of the referenced systems and
future design inputs, not capabilities already present in MatrixOne. In
particular, this PR implements log-based ISCP/CDC maintenance but does **not**
implement Oracle-style Partition Change Tracking (PCT) or partition-level MV
refresh.

## 12. Designs for the remaining mainstream capability families

### 12.1 Aggregate, HAVING, DISTINCT, and set operators

The first extension of the current version-2 specification is a version-3
operator graph. It replaces the flat aggregate list with typed state operators
and stable operator IDs.

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
  immutable time-partition sketches plus affected-partition rebuild. FAST must
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

The first code increment remains HAVING, SUM/AVG DISTINCT, and top-level UNION
ALL. It must add stable construct-specific FAST errors; FORCE alone may select
complete refresh.

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
or affected-partition refresh until a bounded algorithm exists.

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
  source transaction and lock targets in deterministic dependency order. V1 of
  this mode is restricted to one source and algebraic aggregates; multi-source
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

This subsection is a future design only. MatrixOne does not currently support
PCT or partition-level MV refresh. The current implementation either applies
row-level ISCP/CDC deltas for its FAST subset or replaces the complete MV
result; it does not use source-partition change metadata to limit a refresh.

MV DDL should accept ordinary index, clustering, distribution, partition,
tablespace/storage, and retention options. Partition change tracking maps each
source partition to affected MV partitions and refreshes only those partitions
after exchange/drop/truncate. Global aggregates that cross partition boundaries
are not falsely labeled partition-refreshable.

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

The capability union is intentionally larger than one safe implementation
change. Delivery order is:

1. stabilize the current PR subset and public SQL lifecycle tests;
2. HAVING, SUM/AVG DISTINCT, UNION ALL, and construct-specific FAST errors;
3. inner/unique-dimension JOIN plus operator arrangements;
4. Top-K and event-time tumble/hop windows;
5. cascading/replacement, scheduled/concurrent refresh, and status controls;
6. query rewrite, partition refresh, synchronous ON COMMIT, and advanced states.

Each gate updates the persistent-spec version and compatibility table, has
failure/restart tests, and passes its relevant benchmark subset before being
advertised as FAST.

Reviewers are specifically asked to decide:

- whether the current snapshot-finalization and tail-transaction watermark
  contract is acceptable;
- whether FORCE runtime fallback is acceptable or must be operator-controlled;
- whether the version-2 spec should be merged as a stable format or replaced by
  the version-3 operator graph before release;
- which capability gate belongs in PR #27615 versus follow-up PRs, given that
  the implementation already predates design approval;
- whether ON COMMIT and optimizer rewrite should be separate design documents
  because they change source-DML and optimizer ownership respectively.
