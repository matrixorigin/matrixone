# Proof-driven singleton GROUP BY optimizer roadmap

- Status: approved for P1 implementation
- Tracking issues: #27856, #27859, #27860, #27858, #27857, #27753, #27728
- Foundation: #27850, #27889, #27744, #27758, #28067
- First implementation: #27856 phase 1
- Last updated: 2026-09-22
- Design review: GPT-6 medium session `01a0c78c-278f-77a2-b1bd-d36a9904cb83` (PASS)

## 1. Decision and delivery graph

The seven issues are one roadmap but not one implementation boundary. They
contain three independent correctness contracts: logical multiplicity proofs,
physical stream ordering, and runtime hash identity/ownership. Combining those
contracts would make rollback, review and counterexample isolation unsafe.

The delivery graph is:

```text
D0 roadmap and P1 design
  -> P1 direct-scan NOT NULL UNIQUE proof (#27856 phase 1)
       -> P2 nullable/predicate-reduced and projection closure (#27856 phase 2)
            -> P3 join multiplicity propagation (#27859)
       -> P4 additional overload-bound singleton laws (#27860)

O  exact physical-order contract and streaming grouped LIMIT (#27857)
H  canonical grouping-hash reuse contract (#27753)
V  reproduce and close the already implemented high-NDV umbrella (#27728)
```

O and H require their own R3 design approvals. V is a verification track, not
new production code. #27858 is substantially implemented by #27889; its
constant ORDER BY and true-HAVING behavior remains regression coverage for
P1-P4. False-HAVING source elimination remains out of scope because it may
suppress observable errors or evaluations.

## 2. First implementation contract (P1)

For a direct scan of an ordinary persisted relation, a bounded grouped
Aggregate may use the existing singleton-row aggregate laws when the active
grouping expressions contain every component of either:

1. the existing SQL-equality-compatible primary key; or
2. a complete, enforced regular UNIQUE key whose components are declared NOT
   NULL and whose storage identity is compatible with SQL grouping equality.

The proof is planner-local and immutable. It records the exact scan binding,
component ordinals, source kind, NULL contract and equality domains. It is
derived from the `TableDef` attached to the current scan and is not serialized,
cached globally or inferred from estimates.

P1 changes only the key premise consumed by the established
`rewriteEffectlessAggToProjectImpl` path. Aggregate conversion, HAVING remap,
error/volatility guards, pagination, `SQL_CALC_FOUND_ROWS`, grouping-set and
constant-Sort rules remain unchanged.

## 3. Enforcement and scope audit

An eligible P1 secondary key must satisfy all of these checks:

- `IndexDef.Unique`, `TableExist`, a non-empty physical `IndexTableName` and at
  least one component are present;
- the algorithm is a regular index and has no prefix-length metadata;
- every component resolves exactly once to a visible, non-generated table
  column with complete default/nullability metadata;
- both `ColDef.Default.NullAbility` and type nullability establish NOT NULL;
- every component type is on the existing SQL-equality proof allowlist;
- the scan table affirmatively has `TableType == catalog.SystemOrdinaryRel`, is
  not marked temporary, and does not have `features.Partitioned` set;
- every component appears as a direct grouping column on the exact scan
  binding, with the same equality domain.

An empty relation kind is not accepted for new secondary-UNIQUE eligibility:
current DDL deliberately assigns it to hidden tables, so it is not evidence of
a legacy ordinary user table. Likewise an ordinary relation kind does not erase
the independent partition feature bit. Existing primary-key eligibility remains
unchanged by P1; these stricter relation checks apply only to the newly admitted
secondary-UNIQUE source.

Index visibility is deliberately not an enforcement check. An invisible SQL
UNIQUE constraint continues to reject duplicate writes; visibility controls
access-path selection. Conversely, `TableExist=false`, a missing backing table,
irregular/plugin indexes, prefix indexes, generated/expression components and
malformed metadata fail closed.

P1 excludes nullable UNIQUE keys even when a filter happens to reject NULL,
fixed composite components, projections/CTEs/views, joins, partition-local
keys, temporary/external/source/cluster/partition relations, and any relation
kind whose global enforcement contract is not established here. Those are not
treated as non-unique; they retain the established Aggregate plan.

Prepared statements use the `TableDef` bound into their plan. Existing schema
change handling is the owner of prepared-plan invalidation; P1 adds no cache or
second lifetime. Validation must execute a prepared query before and after
DROP/ADD UNIQUE and table recreation. If the existing invalidation contract
does not rebuild the plan, P1 must not ship until that owner is fixed or P1 is
guarded away from prepared execution.

## 4. First-principles invariants

### Multiplicity

For every surviving input pair `r1`, `r2`, equality of all active grouping
expressions must imply that `r1` and `r2` cannot be distinct visible rows. NDV,
cardinality, foreign-key assumptions, data sampling and ONLY_FULL_GROUP_BY
acceptance never prove this invariant.

### Equality domain

Constraint identity must refine SQL grouping equality. FLOAT signed zero, CHAR
padding and non-raw VARCHAR collations remain controls because storage-distinct
values may compare equal in SQL. Cross-type or lossy conversions cannot prove
identity.

### Evaluation and atomicity

The rewritten Project must preserve values, types, width/scale/collation,
nullability, warnings, errors and evaluation count. Existing total-expression
checks continue to guard early scan termination. If any key, aggregate,
predicate, binding or metadata check fails, no part of that Aggregate rewrite
is published.

### Bounded demand

The optimization remains limit-aware. It never crosses unsafe WHERE/HAVING,
join/set/shared boundaries, inactive grouping-set keys, rank/with-ties behavior,
`SQL_CALC_FOUND_ROWS`, dynamic unsafe pagination or an unsupported aggregate.

## 5. Alternatives

1. Extend the existing primary-key branch with ad hoc index checks. Rejected:
   it duplicates metadata semantics and cannot be safely consumed by later join
   multiplicity work.
2. Reuse ONLY_FULL_GROUP_BY functional-dependency acceptance directly.
   Rejected: functional dependency does not prove row multiplicity; duplicate
   identical rows preserve dependencies while changing `COUNT(*)`.
3. Add a serialized uniqueness property to plan protobufs. Rejected for P1:
   direct-scan proof is cheap and local, while durable state introduces stale
   property and compatibility risks.
4. Add a reusable planner-local key proof and consume it from the existing
   rewrite. Selected: one fail-closed metadata/equality contract, no wire or
   storage change, and a direct path to P2/P3.

## 6. Validation contract

### Deterministic planner tests

- baseline PK remains eligible;
- single/composite NOT NULL UNIQUE becomes eligible and pushes LIMIT/OFFSET to
  the scan;
- extra grouping columns remain eligible;
- partial, reordered-with-missing-component and transformed keys remain
  ineligible;
- nullable UNIQUE, including real duplicate-NULL rows in SQL validation,
  remains ineligible in P1;
- non-unique, missing/unbuilt backing table, malformed/prefix/plugin/generated/
  hidden keys and unsupported table kinds fail closed;
- an ordinary-kind partitioned parent and an empty-kind hidden relation fail
  closed for secondary-UNIQUE proof, while pre-existing PK behavior is unchanged;
- FLOAT, CHAR and collated VARCHAR controls retain Aggregate; raw binary VARCHAR
  and other allowlisted domains are covered;
- a mixed unsupported aggregate, unsafe WHERE/HAVING expression, grouping set,
  join, set operation, prepared dynamic boundary and `SQL_CALC_FOUND_ROWS`
  retain their established barriers;
- different aliases with equal column ordinals cannot share proof identity.

### Public differential SQL

Run base and candidate against the same ordinary tables and compare values,
column metadata, warnings and errors. For unordered LIMIT, compare cardinality,
membership in the full legal result and each returned row's aggregate law; do
not demand the same arbitrary subset. Add a total tie-breaker when exact row
equality is required. Execute prepared statements across DROP/ADD UNIQUE and
table recreation to prove schema invalidation.

### Performance

On one data image and host, compare base and candidate binaries for a large
NOT NULL UNIQUE table with small LIMIT and OFFSET. Record scan rows/bytes,
Aggregate/hash input and memory/spill, CPU, peak RSS and wall-time medians.
Include selective WHERE/HAVING and nullable/partial/unsupported controls. The
feature passes only when the target removes full aggregate work with a material
stable gain and controls do not materially regress; no percentage is claimed
before measurement.

### Repository gates

Run focused proof/rewrite tests, complete planner packages, public BVT, service
build and SCA on the exact submitted revision. Record selection and terminal
status. Large data belongs in integration/performance evidence, not unit tests.

## 7. Later stages

P2 may add nullable-key non-NULL and fixed-component predicate closure plus
transparent direct projections, but only after predicate scope and prepared
parameter NULL behavior are designed. P3 propagates scoped keys and maximum
join multiplicity; it cannot consume the private ONLY_FULL_GROUP_BY dependency
proof without strengthening it from value dependency to row multiplicity.
P4 registers each aggregate overload/configuration with an explicit singleton
law and differential metadata/error evidence; one unsupported aggregate rejects
the whole rewrite.

O must prove global stream order for the exact snapshot and distributed scan
schedule, including group boundaries across objects/workers/CNs. Cluster-key
metadata alone is insufficient. H must prove hash identity compatibility,
collision equality, sidecar ownership/transport/spill lifecycle, mixed-version
fallback and a measured cost envelope. Neither feature may be activated by raw
metadata or estimated benefit alone.

## 8. Rollout and rollback

P1 introduces no catalog, wire, storage or configuration format. Unsupported
or inconsistent metadata falls back deterministically. Rollback restores the
old Aggregate plan without data migration. Plan-shape and execution counters
provide bounded observability; no per-row logging or labels are added.
