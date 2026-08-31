# Constant-order elimination after singleton-group proof

- Status: implementation validated; awaiting review
- Tracking issue: [matrixorigin/matrixone#27858](https://github.com/matrixorigin/matrixone/issues/27858)
- Implementation PR: [matrixorigin/matrixone#27889](https://github.com/matrixorigin/matrixone/pull/27889)
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

Constant evaluation is itself part of this fail-closed boundary. Both planner
constant-folding entry points must be total over valid bound expression types:
when a type such as `INTERVAL` is intentionally not foldable as a standalone
scalar, they return the original expression rather than panic. The order proof
then observes a non-literal result and retains Sort for an internal plan.

`INTERVAL` is represented by an internal `(value, unit)` expression list rather
than by a materializable scalar vector. A public query therefore rejects an
unconsumed `INTERVAL` recursively at every generic scalar or aggregate function
boundary, predicate boundary (WHERE, HAVING and JOIN ON), as well as at SELECT
output, GROUP BY, top-level or grouping-set ORDER BY, and window PARTITION BY /
ORDER BY. Subquery comparisons enforce the same rule before interpreting their
left input as a scalar or row tuple. The binder has one explicit set of temporal
consumers; each must rewrite the pseudo-type to ordinary scalar arguments before
publishing its result. Window-frame binding likewise consumes and normalizes the
internal value. This separates two contracts that the initial implementation
conflated: generic constant folding must be total, while a public executable plan
must not contain an unconsumed interval pseudo-type at an ordinary expression
boundary.

## 3. Alternatives and scope decisions

The implementation compares four choices:

1. **Keep the status quo.** This is semantically safe but leaves a blocking
   full scan and Sort on the primary target, so it does not meet the performance
   objective.
2. **Add a global constant-ORDER-BY canonicalization.** This could simplify more
   queries but materially broadens the evaluation, warning, pagination, rank and
   FOUND_ROWS contracts. It is rejected for this change.
3. **Publish a new persistent/protobuf singleton-group property.** This makes
   provenance explicit across optimizer phases, but creates a new plan contract
   and stale-property invalidation problem for one local consumer. It is
   disproportionate.
4. **Use pass-local rewritten-node provenance and a post-proof traversal.** This
   is selected: it cannot activate without the existing complete-key proof, adds
   no durable state, composes pagination through the existing owner, and can be
   rolled back independently.

This PR owns the proof-gated constant-order phase of #27858. The issue's related
non-primary unique-key and ordered-access extensions remain owned by #27856 and
#27857 respectively. An always-true HAVING is already removed by the normal
filter optimizer after the singleton rewrite. An always-false HAVING is kept as
a folded false scan predicate unless a separate evaluation-ownership proof can
replace the source with an empty relation. Consequently PR #27889 references but
does not auto-close the umbrella issue until those explicitly separate closures
are resolved or the issue owner narrows its acceptance scope.

## 4. Plan transformation

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

## 5. Eligibility and semantic barriers

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
- an internal plan containing a valid constant expression whose standalone
  result type is unsupported by the constant evaluator;
- rank/with-ties behavior, malformed plan shapes, or a Project path that does
  not reach the freshly proven singleton group;
- pagination whose composition needs unsafe dynamic arithmetic or overflows.

ASC/DESC, NULL ordering, collation flags, and multiple keys do not matter after
each complete key is proven to be the same scalar value for every row. They are
nevertheless left intact whenever any key fails the proof.

## 6. HAVING scope

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

## 7. Validation contract

Planner unit tests must prove both activation and non-activation:

- `ORDER BY COUNT(*) LIMIT` removes Aggregate and Sort and puts LIMIT on scan;
- aliases, constant expressions, multiple constant keys, and OFFSET preserve the
  expected scan pagination;
- an always-true singleton HAVING does not block the optimization;
- mixed row-dependent keys, nullable COUNT, volatile/erroring expressions,
  prepared parameters, unsupported standalone constant types, rank options,
  SQL_CALC_FOUND_ROWS, and unsafe nested windows retain their semantic barrier;
- direct plan-shape tests cover root replacement and pagination-composition
  failure independently of SQL binding.

The public optimizer path must reject an unconsumed `INTERVAL` at generic scalar
and aggregate arguments, including recursively nested tuple arguments; predicate
and subquery-comparison boundaries; and SELECT, GROUP BY, ORDER BY and window key
boundaries with a normal not-supported error. Controls must prove that
`date_add(..., INTERVAL ...)`, date arithmetic, the scalar `INTERVAL(...)`
function, aggregates and subquery comparisons over already-consumed temporal
results, and interval window-frame bounds remain valid. Direct tests of both
constant-folding entry points and a synthetic internal Sort prove total,
fail-closed behavior independently of public binding.

Distributed SQL tests verify public results for activation, OFFSET, all-tie
ordering, true/false HAVING, mixed keys, and nullable aggregate values. Focused
planner tests, the complete planner package, build/static checks, and an explain
plan are required. A real-server comparison is used when local plan evidence is
insufficient to demonstrate the scan reduction or to check performance.

Implementation evidence on 2026-08-31:

- focused activation/barrier tests and the complete `pkg/sql/plan` and
  `pkg/sql/plan/rule` packages pass;
- `go vet ./pkg/sql/plan ./pkg/sql/plan/rule` and `make build` pass after the
  final main rebase;
- the 31-statement distributed SQL case passes twice on one isolated instance
  with metadata comparison and leaves its test database absent. It
  covers singleton aggregate, OFFSET, true/false HAVING, mixed and nullable
  keys, varying SUM, all five original public interval escape boundaries, the
  nested scalar and aggregate counterexamples, and valid temporal consumers;
- on the 55 validation host, the exact base commit `09c9a0ba9e` plans the 10M-row
  target as full TableScan plus Sort, while the current binary plans TableScan
  with `Limit: 10` and no Sort;
- on the same NVMe data, fixed hostname, container runtime and hot-cache state,
  100 fresh-client executions take 3.00 seconds on the base binary and 0.90
  seconds on the current binary, a 3.33x end-to-end improvement;
- the local-NVMe A/B before the clean main rebase used base binary SHA-256
  `ca8db2676fc5ba8a329d035a138507151fef7edc725c88a189affb0f8d4e3753`
  and feature-worktree binary SHA-256
  `1d450aa4f95d85af298218dc8f5f84476915e1db2ee46b59fdc79d08c5007560`
  in one fixed-hostname container. At 10M rows, 21 fresh-client executions have
  medians 23.359 ms base versus 8.668 ms current (2.70x). At 50M rows they have
  medians 166.745 ms versus 8.853 ms (18.83x). `EXPLAIN ANALYZE` reports 10M/50M
  input rows on base and one 8,192-row storage block on current, so the optimized
  cost remains bounded as data grows. The rebase changed neither feature commit
  semantically and the complete post-rebase planner validation passes;
- required-Sort controls show no regression: the mixed row-key median is
  27.895 ms base versus 27.433 ms current, while nullable COUNT is 52.181 ms base
  versus 52.523 ms current. The live counterexample matrix also retains Sort for
  varying SUM, RAND, division by zero and SQL_CALC_FOUND_ROWS; a 1,000,000-row
  OFFSET is transferred exactly and returns 10 rows;
- live public interval probes return error 20105 instead of an internal error,
  panic or silently materialized list at SELECT, GROUP BY, ORDER BY, window
  PARTITION/ORDER, generic scalar and aggregate boundaries. Unit counterexamples
  also cover predicates and IN/NOT IN/quantified subqueries. `date_add(...,
  INTERVAL ...)`, its use under an aggregate and subquery comparison, and a
  temporal RANGE frame all bind normally.

## 8. Rollback boundary

The performance feature is one planner-local post-proof transformation.
Removing its call restores the previous Sort and full-scan behavior without
changing the singleton-group proof, protobufs, execution operators, or storage
contracts. The companion interval checks and total constant-fold behavior are a
separate correctness guard discovered by counterexample testing; they can be
reverted independently from the performance transformation.
