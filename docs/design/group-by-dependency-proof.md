# GROUP BY dependency proof for #27983

This design is the approved scope of the implementation PR for the remaining
#27983 cases, following #28848. It is recorded before implementation; the
implementation PR links this design revision and its validation evidence.

## Contract and boundaries

Accept a non-grouped column only when equal active grouping keys imply equal
values, including NULL-extended join rows. Implement nullable UNIQUE keys
filtered by explicit IS NOT NULL, transparent derived/CTE/view projections,
and equality-join closure. Preserve MATRIXONE_NATIVE and existing PK/UNIQUE
and single-value exceptions. Do not export proofs to optimizer uniqueness,
aggregate elimination, physical grouping, join algorithms, or DISTINCT rewrites.
Prefix/generated keys and more general relational inference are not this issue.

## Ownership and representation

The query block owns planner-local dependency facts and a lazily solved closure.
Column identity is binding tag plus ordinal, never catalog table identity.
Rules have determinant sets and dependent columns. Seed active direct GROUP BY
columns and the existing WHERE-single-value facts; a monotonic worklist fires
each rule once. Rebuild for grouping sets and rebuilt plans. No process-global,
session, prepared-runtime, persisted, or serialized proof state is introduced.

Capture the bound relational input after WHERE and before aggregate-output
validation. Relation summaries preserve only supported facts. Translate direct
projection outputs by ordinal, with separate instantiation for every CTE/view
reference. Do not treat result-schema provenance as proof of row cardinality.

## Rules

* Reuse existing enforced-key and SQL-equality-compatible type checks. A nullable
  unique key is eligible only when every nullable component has an explicit
  applicable IS NOT NULL conjunct. Do not change catalog nullability or infer
  it from OR/HAVING/outer ON conditions.
* Direct projections, filters, and ordering without LIMIT preserve supported
  facts. Export complete keys and direct-column dependencies only. Aggregation,
  DISTINCT, set operations, windows, LIMIT, recursive CTEs, table functions,
  and unsupported nodes stop new propagation.
* Direct-column equality in INNER JOIN or WHERE is bidirectional only in one
  SQL-equality identity domain. USING consumes its bound predicates. Lossy
  casts and null-safe equality are not identity proofs.
* LEFT JOIN permits only preserved-to-nullable-side propagation. All preserved
  columns referenced anywhere in the deterministic ON condition must be
  determined before inferring its directly equated nullable-side column.
  Apply that side's eligible keys afterward. RIGHT JOIN uses existing
  normalization. Never infer the reverse direction or through FULL OUTER JOIN.
* Keep predicate scope. Across NULL extension, retain only rules for which
  all-NULL determinants imply all-NULL dependents. Do not lift unconditional
  constants or non-null facts from a nullable child. A base eligible key has
  this property because real key components are non-null; direct equality has
  it because NULL cannot satisfy ordinary equality; outer-join directional
  rules have it because an equated preserved-side NULL prevents a match.

The critical counterexample groups a LEFT JOIN by child.pid while another ON
predicate depends on child.flag. Different flags can produce parent values
and NULL in the same group. child.pid alone must not pass; the child's complete
key may pass because it determines the complete preserved-side ON inputs.

## Alternatives and cost

Keeping the status quo leaves the reported valid queries rejected. Adding
independent syntactic exceptions cannot safely compose joins and projections.
A general optimizer key framework would expand scope and change consumers.
Use a private validation proof instead. Memory and work are bounded by query
columns, predicates, and catalog keys, never data rows. Avoid key-subset
enumeration and repeated whole-plan analysis per selected column. Unsupported
or incomplete evidence fails closed without changing existing diagnostics.

## Validation and delivery

Pair accepted SQL with explicit-grouping controls and typed plan assertions.
Cover nullable/composite keys, transparent boundaries and repeated references,
INNER/LEFT/RIGHT and nested joins, NULL/unmatched/duplicate rows, residual ON
predicates, grouping sets, and prepared DROP/ADD UNIQUE/view invalidation.
Retain rejection controls for unsupported nodes and equality domains. Test
ordinary-query planning cost and long projection/join chains. Run planner UT,
binary SQL tests, BVT twice with verified cleanup, and full SCA. No new wire or
storage representation, rollout flag, or MORPC version is required. Merge
newest main before push. Close #27983 only after every reported case is mapped
to passing evidence. Record material design deviations for review first.
