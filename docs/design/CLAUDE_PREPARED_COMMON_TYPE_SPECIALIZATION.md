# Prepared Common-Type Specialization Design

- Status: Proposed — distinct design approval required before merge
- Tracking issue: [#27088](https://github.com/matrixorigin/matrixone/issues/27088)
- Implementation PR: [#27483](https://github.com/matrixorigin/matrixone/pull/27483)
- Baseline: `1d7b6311ce91df0968da435bb405f3d68137c595`
- Implementation review revision: `de771947ab1ccf47c1c2f194e3dbdd2ef948a895`
- Wire capability: MORPC v30

## 1. Summary

Prepared markers are initially bound as transport text, but SQL `EXECUTE` and binary
`COM_STMT_EXECUTE` can later supply integer, decimal, approximate, Boolean, string,
or NULL values. Consumers such as comparisons, `IN`, `BETWEEN`, `COALESCE`,
`GREATEST`, and `LEAST` cannot always select the correct common type at PREPARE
time.

This design permits narrowly scoped execution-time specialization when a runtime
parameter reaches a numeric-prefix-aware exact consumer. It preserves the ordinary
cached prepared path for ineligible statements, rewrites only an isolated plan copy,
and stores at most one specialized plan/compile pair per prepared statement. Reuse is
keyed by stable runtime semantic categories rather than concrete values.

## 2. Goals and non-goals

### Goals

1. Make fresh and reused prepared execution semantically equivalent.
2. Preserve exact DECIMAL behavior when the consumer permits it, including
   Decimal64, Decimal128, and Decimal256 boundaries.
3. Preserve approximate-domain rules when any participating member requires FLOAT.
4. Support SQL `PREPARE`/`EXECUTE`, COM_STMT, SET, CTAS/DDL query plans, retries,
   windows, indexes, and scalar subqueries.
5. Keep ordinary COM_STMT execution on the prepare-time plan/compile fast path.
6. Bound specialized cache ownership and memory independently of executed values.
7. Support rolling upgrades without sending v30 expression semantics to older CNs.

### Non-goals

- Globally treating arbitrary VARCHAR expressions as DECIMAL.
- Reopening arithmetic coercion completed by #25705.
- Mutating the cached PREPARE plan.
- Caching every runtime type or concrete DECIMAL width/scale.
- Replacing the binary-string semantic contract owned by #27214 and #27215-#27218.

## 3. Alternatives considered

### A. Bind every prepared marker as DOUBLE

Rejected. It loses distinctions above `2^53`, changes exact comparisons, and violates
explicit DECIMAL controls.

### B. Bind every prepared marker as DECIMAL256

Rejected. FLOAT participants require an approximate common domain, arbitrary runtime
VARCHAR expressions are not numeric-prefix inputs, and unconditional Decimal256
increases execution and wire costs.

### C. Reprepare and recompile on every execution

Correct but rejected for the steady state. It reintroduces the measured prepared hot-
path regression and makes PREPARE little more than syntax caching.

### D. Mutate the cached PREPARE plan in place

Rejected. Concurrent/retried executions could observe another generation's literals,
types, or result metadata. Rollback after a partial rewrite is not reliable.

### E. Unbounded cache by every observed runtime type

Rejected. The number of width/scale/value combinations is input-controlled and compile
objects own materialized worker topology and resources.

### Chosen approach

Use PREPARE-time capability admission, execution-time final eligibility, isolated plan
copy specialization, and a bounded one-entry semantic-category cache.

## 4. Semantic contract

### 4.1 Runtime provenance

`ParamValue` carries orthogonal facts:

- transport/protocol source;
- conversion kind: integer, decimal, float, Boolean, or none;
- runtime physical type when known;
- whether numeric-prefix behavior is enabled by protocol capability;
- whether a rewritten literal must retain its original `ParamRef` source.

Source and conversion kind are not SQL types. A text packet can be eligible for
numeric-prefix conversion without changing direct string-result metadata.

### 4.2 Domain selection

- Exact integer plus eligible runtime DECIMAL/text-numeric-prefix selects a
  representable exact DECIMAL common domain.
- DECIMAL mixed with FLOAT/DOUBLE selects one FLOAT64 domain across the complete
  multi-operand consumer.
- Runtime FLOAT does not trigger exact numeric-prefix specialization.
- Runtime VARCHAR columns and unrelated text parameters remain in their normal string
  domain.
- NULL preserves typed/untyped NULL semantics and does not provide a stable cache
  category by itself.
- Non-numeric suffix handling follows MatrixOne numeric-prefix conversion rather than
  strict full-string DECIMAL parsing.

### 4.3 Consumer closure

The expression visitor covers functions, lists, windows, subqueries, order/frame
expressions, and literal-source provenance. Index-generated `prefix_*` expressions are
included. `BETWEEN` retains SQL three-valued logic: a FALSE comparison dominates NULL.

## 5. Architecture and ownership

### 5.1 PREPARE owner

`PrepareStmt` owns:

- the immutable prepare-time plan;
- the ordinary cached compile;
- static capability flags for numeric-prefix consumers, pagination parameters, and
  LAG/LEAD offsets;
- at most one runtime specialization key;
- at most one restored specialized runtime plan;
- at most one runtime compile built from that plan.

The prepare-time capability scan is conservative. It prevents ordinary binary Query
execution from constructing runtime parameter objects or traversing/copying plans.

### 5.2 Execution owner

`TxnComputationWrapper` owns the current execution's parameter vector, complete
`ParamValue` snapshot, retry closure, and temporary specialization target. It never
transfers parameter-vector ownership into `PrepareStmt`.

### 5.3 Plan ownership

Specialization always starts from `DeepCopyPlan`. Runtime literals are used to rerun
binding and overload selection. Before a specialized plan enters the cache, every
cache-relevant literal is restored to a `ParamRef` through literal source provenance.
Consequently a cached plan describes a semantic category, not the value that first
created it.

### 5.4 Compile ownership

A runtime compile is installed only after compilation of the restored specialized plan
succeeds. `PrepareStmt` owns and releases it. The ordinary compile and runtime compile
are separate because their overloads, metadata, and worker topology can differ.

## 6. State machine

| State | Event | Next state | Action |
| --- | --- | --- | --- |
| ordinary | ineligible execution | ordinary | reuse prepare plan/compile |
| ordinary | eligible cache miss | specializing | deep-copy and specialize |
| specializing | plan failure | ordinary | discard copy; return error |
| specializing | compile failure | previous state | discard candidate; retain the preceding live cache unchanged |
| specializing | compile success | runtime-cached | restore ParamRefs; atomically install candidate, then release the old compile |
| runtime-cached | same category | runtime-cached | reuse plan/compile with current params |
| runtime-cached | different category | specializing | retain live cache; stage candidate plan/key outside it |
| any | schema definition change | rebuilt/ordinary | rebuild immutable plan; clear runtime cache |
| any | protocol capability change | ordinary | clear plan/compile specialization cache |
| any | statement close | closed | release ordinary and runtime compiles exactly once |

A one-entry replacement is deliberate: category alternation may recompile, but memory
and compile-resource ownership remain constant.

## 7. Cache key and resource bounds

The key is derived from parameter position, conversion kind, and normalized runtime
physical category (OID, width, and scale). Equivalent spellings such as trailing-zero
DECIMAL forms normalize to the same category. Unrelated parameters are included by
stable category so their values can change without embedding stale literals.

NULL without a stable runtime type is not cached. Unsupported values take the existing
rebuild/error path.

Per prepared statement, the additional bound is:

- one key string proportional to parameter count;
- one plan copy proportional to plan size;
- one compile and its fixed worker topology;
- no value-indexed map, history, or input-sized integer allocation.

Exponent parsing is linear in input length and bounds the net exponent before decimal
type construction.

## 8. Invalidation and retry

### Schema changes

Definition-change retry rebuilds the immutable prepared plan, recomputes static
capabilities, clears the runtime plan/compile, and replays the complete immutable
runtime parameter snapshot. A retry must not return a plan containing unresolved
`ParamRef` where literal filling is required.

### Runtime compile replacement

A new category is installed only after successful specialization. Replacing or clearing
a cached compile releases the old compile once. Failed candidates do not displace the
last valid owner prematurely.

### SET, DDL, and CTAS

SET and DDL plans use isolated literal materialization because their execution owners
require typed literals rather than a reusable Query compile. CTAS carries specialized
DDL query state into the generated INSERT and trims only parameters proven to belong
to numeric-prefix cast subtrees.

### Scalar subqueries

Prepared SET scalar subqueries execute through the query pipeline. Results are
materialized from typed vectors, including JSON, date/time, UUID, ENUM, ARRAY, typed
NULL, and explicit-cast fallback domains. The complete specialized outer expression is
then evaluated.

## 9. Mixed-version rollout and rollback

MORPC v30 gates prepared numeric-prefix expression semantics across CN boundaries.

- A v30 sender scans the plan before remote execution.
- If v30-only numeric-prefix casts are present and the target/service capability is
  below v30, remote execution is rejected or retained locally; it is never silently
  serialized with changed semantics.
- Ordinary plans remain compatible with older versions.
- During rolling upgrade, specialization is enabled only when the current protocol
  capability is at least v30.
- Rolling back protocol capability clears prepared plan/runtime caches so a v30
  specialized compile cannot survive into a lower-capability generation.
- v27 ASOF JOIN, v28 owner-local lock, and v29 FOUND_ROWS remain independent capability
  boundaries.

## 10. Performance contract

On an unchanged schema and protocol generation:

1. Ordinary COM_STMT Query execution performs no execute-time plan traversal, deep copy,
   overload rebinding, or compile creation.
2. Eligible SQL EXECUTE and COM_STMT executions may specialize once per current cached
   category; subsequent same-category executions reuse the same runtime plan and
   compile pointers.
3. Cache storage is O(plan size + parameter count), with one entry per prepared
   statement.
4. A same-category hit must not retain the first execution's parameter values.

Current focused measurements are test-machine observations, not universal latency
limits:

- ordinary COM_STMT fast path: approximately 0.62-0.68 us/op, 200 B/op, 3 allocs;
- cached specialized COM_STMT: approximately 3-5 us/op;
- cached specialized SQL EXECUTE: approximately 3.3-3.5 us/op, 696 B/op, 14 allocs;
- prior uncached SQL initialization: approximately 9.3-9.5 us/op, excluding the forced
  full compile.

Release acceptance requires no statistically significant TPCC regression against the
main fast path and pointer-reuse regression gates for both protocols.

## 11. Validation matrix

Required automated coverage includes:

- consumers: comparisons, null-safe comparison, IN/NOT IN, BETWEEN/NOT BETWEEN,
  COALESCE, GREATEST, LEAST, indexes, windows, and subqueries;
- sources: literal, foldable string, SQL variable, COM_STMT DECIMAL/string/integer/
  float/Boolean/NULL;
- boundaries: `2^53 - 1`, `2^53`, `2^53 + 1`, Decimal64/128/256, 65-77 digits,
  exponent compensation, sign and extreme scale;
- generations: fresh, same-category reuse, category replacement, value-to-NULL,
  NULL-to-value, schema retry, protocol upgrade/rollback, and Close;
- metadata: stable result types for numeric and nonnumeric scalar-subquery domains;
- lifecycle: old compile release, failed replacement, retry replay, and no cached
  literal values;
- performance: ordinary fast-path benchmark, repeated COM_STMT cache hit, repeated SQL
  EXECUTE plan/compile pointer reuse, and TPCC comparison.

## 12. Approval and implementation conformance

This document is intentionally marked Proposed until a reviewer distinct from the
implementation author approves the architecture. Implementation review alone does not
constitute design approval.

Before merge:

1. obtain explicit design approval on this versioned artifact;
2. record the approved document revision in PR #27483;
3. verify the implementation diff and test matrix against the approved revision;
4. re-review any architectural deviation rather than silently updating code or this
   contract.
