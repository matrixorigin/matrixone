# Constant-order elimination after singleton-group proof

- Status: implementation complete; planner UT, build, vet, distributed SQL and 55-host A/B validated
- Tracking issue: [matrixorigin/matrixone#27858](https://github.com/matrixorigin/matrixone/issues/27858)
- Depends on: [matrixorigin/matrixone#27850](https://github.com/matrixorigin/matrixone/pull/27850)
- Last updated: 2026-08-31

## 1. Decision

After the complete-primary-key proof has converted a bounded grouped Aggregate
into a row Project, remove its downstream Sort only when every ORDER BY
expression can be evaluated by the planner to a scalar literal without an error,
and at least one expression can be traced through unary Projects to that exact
rewritten node.

The motivating query is:

```sql
select id, count(*) c
from t
group by id
order by c desc
limit 10;
```

The uniqueness proof establishes one row in every group, hence `COUNT(*) = 1`
for every surviving row. Sorting equal keys cannot change the SQL result because
SQL does not define an order among ties. Removing Sort exposes bounded demand to
the existing Project-to-TableScan LIMIT pushdown and changes the work from a full
scan plus sort to a bounded scan, subject to storage-reader granularity.

This revision does not introduce a general constant-ORDER-BY rule. Eligibility
is tied to the set of Aggregate nodes rewritten during the same optimization
pass. That provenance prevents an unrelated expression or stale plan metadata
from activating the rule.

## 2. First-principles invariant

A Sort is redundant exactly when deleting it cannot change which rows satisfy
the query, which rows the pagination window selects, or which expression
evaluations are observable.

For a sort tuple `(e1, ..., en)`, this revision requires:

```text
for every input row r and each ei:
    ei(r) evaluates successfully, without side effects, to the same value
```

The planner proves this constructively rather than inferring it from statistics:

1. resolve output columns through unary Project definitions;
2. require every key to become a safe constant and require at least one key's
   resolution path to reach a Project created by the current
   complete-primary-key singleton-group rewrite;
3. require the fully resolved expression to satisfy the registered constant
   function contract with parameters and variables treated as dynamic;
4. run the normal constant evaluator and require a scalar literal result.

Failure at any step retains the original Sort.

## 3. Plan transformation

The normal bound plan is:

```text
Result Project
  Sort [order, limit, offset]
    Select Project
      Aggregate
        TableScan
```

After the existing singleton-group rewrite, the former Aggregate is a Project
whose aggregate slots contain proven row expressions. The new pass walks the
same plan root bottom-up, resolves each Sort key through intervening Projects,
and, when all guards pass, replaces the Sort child edge with the Sort's child.

Before bypassing Sort, its `(LIMIT, OFFSET)` window is composed with any window
already owned by the child using `composePagination`. Composition is not an
algebraic reimplementation: the existing helper preserves nested-window
cardinality and position, detects uint64 overflow, rejects dynamic arithmetic,
and retains evaluation boundaries when an outer OFFSET exhausts a nonempty
inner LIMIT. If composition fails, Sort is retained.

The rewritten root ID is returned to `createQuery`, so the rule is also correct
when Sort itself is a query step root. The ordinary projection-pruning and
repeatable LIMIT-pushdown passes then move the exposed window to a direct scan
when their own guards permit it.

## 4. Eligibility and semantic barriers

All of the following are required:

- the Sort is unary and has a LIMIT;
- the Sort has at least one well-formed ORDER BY expression;
- `RankOption` is absent, preserving rank/with-ties and vector-rank semantics;
- `SQL_CALC_FOUND_ROWS` is absent, preserving the complete pre-LIMIT stream;
- every key resolves through unary Projects to a safe constant, and at least one
  key resolves through an Aggregate converted by the current singleton-group
  proof pass (other keys may be independent safe constants);
- every resolved key is a non-parameter, non-variable constant according to the
  function registry and folds successfully to one scalar literal;
- pagination can be transferred without changing nested-window semantics.

Consequently these cases retain Sort:

- a mixed tuple such as `ORDER BY COUNT(*), id`;
- `COUNT(nullable_column)`, `SUM`, `MIN`, `MAX`, or another row-dependent
  singleton expression;
- a volatile, real-time, session-variable, prepared-parameter, subquery, or
  unknown expression;
- a constant-looking expression whose evaluation can fail, including division
  by zero or an invalid cast;
- rank/with-ties behavior, malformed plan shapes, or a Project path that does
  not reach the freshly proven singleton group;
- pagination whose composition needs unsafe dynamic arithmetic or overflows.

ASC/DESC, NULL ordering, collation flags, and multiple keys do not matter after
each complete key is proven to be the same scalar value for every row. They are
nevertheless left intact whenever any key fails the proof.

## 5. HAVING scope

The preceding singleton-group implementation already rewrites an eligible
HAVING predicate to its row-equivalent expression before scan LIMIT. The normal
filter optimizer removes an always-true predicate. This revision validates that
path but does not add a second truth evaluator.

An always-false HAVING is not replaced with an empty source here. Such a rewrite
can suppress WHERE, aggregate-argument, or other row evaluations that the former
blocking plan would have reached. It needs a separate evaluation-ownership proof
and offers no bounded successful-result path.

Likewise `ORDER BY MIN(pk)` / `MAX(pk)` is not rewritten to key order in this
revision. Although the singleton value law is valid, exploiting it requires an
independent proof that the scan supplies the requested physical order; otherwise
Sort remains necessary.

## 6. Validation contract

Planner unit tests must prove both activation and non-activation:

- `ORDER BY COUNT(*) LIMIT` removes Aggregate and Sort and puts LIMIT on scan;
- aliases, constant expressions, multiple constant keys, and OFFSET preserve the
  expected scan pagination;
- an always-true singleton HAVING does not block the optimization;
- mixed row-dependent keys, nullable COUNT, volatile/erroring expressions,
  prepared parameters, rank options, SQL_CALC_FOUND_ROWS, and unsafe nested
  windows retain their semantic barrier;
- direct plan-shape tests cover root replacement and pagination-composition
  failure independently of SQL binding.

Distributed SQL tests verify public results for activation, OFFSET, all-tie
ordering, true/false HAVING, mixed keys, and nullable aggregate values. Focused
planner tests, the complete planner package, build/static checks, and an explain
plan are required. A real-server comparison is used when local plan evidence is
insufficient to demonstrate the scan reduction or to check performance.

Implementation evidence on 2026-08-31:

- focused activation/barrier tests and the complete `pkg/sql/plan` package pass;
- `go vet ./pkg/sql/plan` and `make build` pass;
- the distributed SQL case produces the expected singleton aggregate, OFFSET,
  true/false HAVING, mixed-key and nullable-count results;
- on the 55 validation host, the exact base commit `09c9a0ba9e` plans the 10M-row
  target as full TableScan plus Sort, while the current binary plans TableScan
  with `Limit: 10` and no Sort;
- on the same NVMe data, fixed hostname, container runtime and hot-cache state,
  100 fresh-client executions take 3.00 seconds on the base binary and 0.90
  seconds on the current binary, a 3.33x end-to-end improvement;
- the live counterexample matrix retains Sort for a row-dependent tie breaker,
  nullable COUNT, RAND, division by zero and SQL_CALC_FOUND_ROWS, while a
  1,000,000-row OFFSET is transferred exactly to the scan and returns 10 rows.

## 7. Rollback boundary

The feature is one planner-local post-proof transformation. Removing its call
restores the previous Sort and full-scan behavior without changing the
singleton-group proof, expression semantics, protobufs, execution operators, or
storage contracts.
