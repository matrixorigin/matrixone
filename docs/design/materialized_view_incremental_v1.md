# Incrementally Maintained Materialized Views V1

Status: draft

Owner issue: https://github.com/matrixorigin/matrixone/issues/24553

Implementation PR: https://github.com/matrixorigin/matrixone/pull/27615

## 1. Problem and user contract

MatrixOne has ordinary views but no durable, automatically maintained
pre-aggregates for dashboards and alerts over high-volume event tables. The
target workload continuously inserts trace or metric events and repeatedly
reads grouped aggregates. Some workloads also delete expired data or update
late/corrected events.

V1 introduces physical materialized views with two refresh timings:

- `ON CHANGE`: asynchronous maintenance from a consistent ISCP snapshot and
  subsequent tails.
- `ON DEMAND`: explicit transactional full refresh.

It also exposes three method policies:

- `FAST` / `INCREMENTAL`: creation fails unless the definition has a supported
  incremental plan; an incremental runtime error fails closed and does not
  silently perform a full refresh.
- `COMPLETE` / `FULL`: every refresh re-evaluates the full definition.
- `FORCE` / `AUTO` (the default): use an incremental plan when available and
  otherwise perform a full refresh. A runtime incremental failure rolls back
  the delta transaction before a consistent full refresh is attempted.

For every committed MV watermark W, querying the MV must return exactly the
result of evaluating its definition over all source tables at W. The target
rows, auxiliary state, and W advance in one transaction. No execution may
publish target rows or state for W while retaining an older watermark.

V1 is eventually consistent. It does not promise source DML and MV changes in
the same transaction, a maximum wall-clock staleness, automatic optimizer query
rewrite, scheduled refresh, partition refresh, windows, Top-K, or nested MVs.

## 2. Scope

### 2.1 Incremental definitions

The initial incremental subset is a top-level query over one direct ordinary
base table with optional row-local `WHERE`, ordinary `GROUP BY`, and these
aggregates:

- `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, `AVG(expr)`;
- `MIN(expr)`, `MAX(expr)` by recomputing only affected groups;
- exact `COUNT(DISTINCT expr)`, `SUM(DISTINCT expr)`, and
  `AVG(DISTINCT expr)` using value multiplicity state;
- `SELECT DISTINCT` as grouping without aggregate output;
- optional `HAVING` whose expression depends only on group expressions and
  supported aggregate outputs.

The incremental scalar expression allowlist is deterministic and row-local.
It includes column references, literals, arithmetic, boolean/comparison/null
predicates, `CASE`, casts, `date_trunc`, `coalesce`, `ifnull`, `abs`, `floor`,
and `ceil`. Volatile, subquery, window, aggregate-nesting, and unknown functions
are rejected from FAST planning.

The second incremental shape is top-level `UNION ALL` over two to sixteen
branches. Every branch must satisfy the single-table subset, have one direct
ordinary source, and produce union-compatible output types. A source may appear
in multiple branches, but each physical source is registered once and each
change is evaluated against every matching branch. State and hidden row identity
are keyed by `(branch ID, group key)`, so equal output rows from separate
branches remain separate rows as SQL bag semantics require. `UNION` without
`ALL` is outside V1.

Insert, delete, and update are supported. An update is represented by the old
row at the iteration's from-boundary and the new row at its commit boundary, so
changes to filters, groups, distinct values, and HAVING membership are all
equivalent to one negative and one positive contribution.

### 2.2 Full-refresh definitions

FORCE and COMPLETE accept top-level definitions over one to sixteen direct
ordinary base tables when the normal planner can execute the query and every
source supports snapshot reads. JOIN and other non-incremental definitions are
re-evaluated at one common ISCP boundary. FAST rejects every definition not
covered by Section 2.1 with a stable `NotSupported` error naming the first
unsupported construct.

External, temporary, cluster, source, subscription, view, materialized-view,
and internal state relations are rejected as MV sources. This avoids undefined
snapshot, lifecycle, ownership, or recursive-maintenance behavior.

## 3. Architecture and ownership

### 3.1 Planner-owned durable specification

The SQL planner validates the definition and serializes a versioned incremental
description into table metadata. It contains:

- specification version and strategy;
- ordered source identities and per-source branch descriptions, including a
  stable branch ID;
- source aliases and columns needed to evaluate deltas;
- group expressions, filter, and HAVING expression;
- aggregate kinds, inputs, visible output columns, and hidden state columns;
- exact-distinct state indexes and auxiliary state-table identity.

The serialized specification is a persistent compatibility contract. Unknown
versions fail closed. New optional fields must preserve old decoding. A change
that alters existing semantics requires a new version and mixed-version rules.

The planner is the first owner of SQL eligibility. The consumer never guesses
whether arbitrary SQL is incrementally maintainable.

### 3.2 ISCP-owned source progress

One ISCP job owns the ordered source set and one watermark per source. Initial
construction starts from historical data, obtains a common consistent snapshot,
and invokes the same consumer used for tails. No separate CTAS or InitSQL path
may initialize the target.

For a multi-source job, an iteration boundary is publishable only when every
source has advanced to the common `toTS`. Duplicate delivery is harmless
because watermarks and target changes commit atomically. Restart resumes from
persisted watermarks; it must not recreate or skip the snapshot generation.

The ISCP executor owns scheduling, cancellation, retries, source disappearance,
and delivery buffers. The consumer owns no independent progress cache.

### 3.3 Consumer-owned target and state

The consumer owns exactly one refresh transaction per iteration. It drains all
source batches, reconstructs deleted rows at the from-boundary, evaluates
signed branch deltas, and applies state and visible-row changes before calling
the normal ISCP watermark update in the same transaction.

The physical MV target stores visible columns plus hidden aggregate state and a
serialized group key. An internal auxiliary table stores:

- exact distinct value multiplicity keyed by aggregate, group, and serialized
  value;
- affected-group keys for recomputation and HAVING publication.

User DML against either table is rejected. Dropping the MV unregisters all job
generations and drops its state. Dropping a source makes both maintenance and
MV queries fail with the missing-source error; stale results are not served as
if valid.

## 4. Delta semantics

For each accepted source row, the planner-generated branch projection produces
branch B, group key G, aggregate inputs, and sign S in {-1,+1}. Deltas are
combined by `(B,G)` before target DML. Contributions from distinct UNION ALL
branches are never combined merely because their visible values are equal.

- COUNT adds S for `COUNT(*)`, or S only for non-null COUNT input.
- SUM maintains `(sum, non_null_count)` so deleting the final non-null input
  restores SQL NULL rather than zero.
- AVG maintains `(sum, non_null_count)` and derives the visible quotient.
- MIN/MAX record G as affected and recompute only G at the common `toTS`.
- Exact DISTINCT changes multiplicity M(G,V). Visible COUNT/SUM state changes
  only on 0-to-1 and 1-to-0 transitions. AVG(DISTINCT) derives from distinct sum
  and distinct non-null count. NULL follows normal SQL aggregate semantics.
- A group with row count zero is removed from target and auxiliary state.

HAVING is evaluated against post-delta aggregate state. Supported aggregates
used only by HAVING are planned as hidden state even when absent from the SELECT
list. State must survive while a non-empty group does not satisfy HAVING.
Therefore visible rows and complete group state cannot be represented solely by
one target row: V1 stores complete group state in the internal state relation
for HAVING definitions and projects only qualifying groups into the user target.
A transition false-to-true inserts the visible row; true-to-false deletes it;
true-to-true updates it; false-to-false changes state only.

Every serialized group or distinct key uses a type-preserving encoding with
explicit NULL markers. Equality must match SQL grouping semantics, including
nullable groups. Hash-only identity is forbidden unless collisions are resolved
by the encoded value.

## 5. Transactions and failure behavior

Target DML, state DML, and watermark updates share one transaction. The commit
is the only publication point.

- Parse, bind, row lookup, delta-size, state, target, or watermark errors roll
  back the complete iteration.
- FAST records the error and retries according to ISCP policy; it never falls
  back to COMPLETE.
- FORCE may start a new transaction and re-evaluate all sources at the same
  iteration boundary only after the failed delta transaction is known rolled
  back.
- Cancellation and timeout roll back and release every batch and reader.
- A retried boundary must produce the same final state.
- Source or target disappearance terminates maintenance with a durable error.
- DDL that changes source column identity/type is rejected or invalidates the
  job; silently binding the old specification to a new column is forbidden.

Delta SQL is chunked by row count and bounded serialized size. The consumer
must not retain an unbounded iteration in memory: batches are processed and
released progressively. Auxiliary state grows with live `(group, distinct
value)` cardinality and is reclaimed when multiplicity reaches zero or a group
becomes empty.

## 6. Syntax and compatibility

Canonical forms are:

```sql
CREATE MATERIALIZED VIEW mv
  REFRESH FAST ON CHANGE
AS SELECT ...;

CREATE MATERIALIZED VIEW mv
  REFRESH COMPLETE ON DEMAND
AS SELECT ...;

REFRESH MATERIALIZED VIEW mv;
DROP MATERIALIZED VIEW mv;
```

Omitted refresh clauses retain FORCE ON CHANGE behavior. Existing V1 metadata
continues to decode. A mixed-version cluster must reject creation or execution
of a specification version unsupported by any eligible executor; it must not
downgrade FAST to full refresh. Backup/restore persists target, state, source
identities, and watermarks as one logical feature. Downgrade to a binary that
does not understand MV metadata is unsupported unless MVs are dropped first;
this limitation must be documented before rollout.

## 7. Alternatives

### Full refresh for every definition

This is simple and general, but repeatedly scans the firehose and cannot meet
the intended freshness/cost target. It remains the COMPLETE path and FORCE
fallback, not the default implementation for eligible SQL.

### Synchronous DML hooks

Updating MVs in each source transaction provides zero post-commit staleness but
adds source write latency, couples transactions across sources, and requires a
new DML ownership protocol. It conflicts with the requested firehose workload
and is excluded from V1.

### One independent ISCP job per source

Independent jobs simplify retrieval but cannot atomically publish a UNION or
JOIN result at one common boundary and make retries double-apply changes. One
multi-source job is selected.

### Recompute affected groups for every aggregate

This simplifies HAVING and DISTINCT but makes hot groups approach full scans.
V1 uses algebraic deltas for reversible aggregates and reserves affected-group
recomputation for MIN/MAX or explicit fallback.

## 8. Performance and observability

The expected hot path is O(changed rows + changed distinct keys + affected
MIN/MAX groups), not O(source rows). SQL parsing/planning and target writes are
amortized in bounded batches. Metrics expose:

- inserted/deleted source rows;
- incremental and full refresh duration/result;
- watermark wall-clock lag;
- fallback count;
- rows and bytes per delta chunk;
- affected groups and exact-distinct state cardinality;
- retry/error classification.

Acceptance for the observability workload is correctness under sustained
append/update/delete, no unbounded memory growth, and a reported p50/p95/p99/max
freshness profile. V1 does not set a universal latency SLA because disk and
source throughput determine saturation; benchmarks must report hardware and
the no-MV source-write baseline.

## 9. Validation plan

### Planner and parser unit tests

- positive FAST plans for HAVING, SUM(DISTINCT), AVG(DISTINCT), and two to
  sixteen UNION ALL branches;
- stable FAST rejection for UNION, JOIN, subquery, window, LIMIT/Top-K,
  volatile/unknown scalar functions, incompatible branch types, unsupported
  sources, and more than sixteen sources;
- FORCE produces no incremental specification for the same unsupported SQL;
- specification version round-trip and old-version decode.

### Consumer unit tests

- signed insert/delete/update transitions for filters and groups;
- HAVING false/true transition matrix while hidden state remains correct;
- DISTINCT duplicate, final-delete, NULL, negative and decimal values;
- the same source in multiple UNION ALL branches and multiple sources in one
  iteration;
- duplicate delivery, rollback, retry, cancellation, oversized delta, missing
  source, and watermark failure;
- target, state, and watermark atomicity.

### SQL BVT

Use minimum deterministic rows and observable polling only for the asynchronous
public boundary. Cover initial snapshot and tail insert/delete/update for each
new shape; compare MV rows with the definition query. Run the exact case twice
on one clean test-owned service and verify teardown. Test files must stay within
normal BVT time limits; volume and latency assertions belong to benchmarks.

### Benchmarks

- TPC-H Q1-derived single-table aggregates driven by RF1/RF2 for HAVING and
  distinct correctness;
- DBToaster-style mixed insert/delete stream for algebraic versus affected-group
  maintenance;
- a two-source UNION ALL event stream with marker rows for freshness;
- RTABench pre-aggregated queries after the SQL subset is compatible.

Report initial snapshot time, source throughput with and without MV, freshness
p50/p95/p99/max, drain time, MV query latency, CPU/RSS/disk I/O, and exact final
correctness. Public comparison is valid only with matched SQL semantics,
hardware, duration, writers, batch size, and source durability.

## 10. Rollout and decision log

Roll out behind the existing MV syntax with FAST fail-closed. FORCE remains the
compatibility default. Operators diagnose lag and failures from ISCP job state
and MV metrics; dropping the MV is the rollback path.

Decisions proposed for review:

1. HAVING definitions use a separate complete-group state layout; a group that
   does not qualify is absent from the target but never loses maintenance state.
2. UNION ALL keeps branch-local groups and uses hidden `(branch ID, group key)`
   identity, preserving duplicate visible rows across branches.
3. Exact DISTINCT state is persistent and bounded by live source cardinality
   and the account's normal storage quota. Per-iteration memory remains bounded
   by delta chunk size. State-cardinality metrics are required before introducing
   an additional MV-specific quota.
4. A new specification records its minimum executor capability. Creation waits
   until every eligible CN reports that capability; an older executor refuses
   the task rather than interpreting or downgrading it. No task pinning is used.
5. Downgrade requires dropping MVs until catalog-level feature-version
   negotiation provides a safe automated downgrade path.

No implementation-blocking question is intentionally deferred. Review changes
to these decisions require a new document revision before implementation
approval.
