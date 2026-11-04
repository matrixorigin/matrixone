# Retire unconsumed planner support

Version 2, 2026-10-22. Owner: #29249.
Implementation branch: `cleanup/29249-structures-20261022`.
Base: `3ad9adbb4ce6c9f8e421917233cc6f6d40ab5f25`.

## Problem and proposed scope

The repository retains a separate planner assertion language with alias maps,
pattern traversal, expression matching and its own SQL parsing/process fixture.
`pkg/sql/plan/tools` has no repository importer or external call site: all uses
are within its single self-test file. Its six implementation files contain
1,413 lines, and its tests contain 381. Keeping another matcher framework with
no consumers increases maintenance without serving the active planner tests.

Retire the complete package and its CODEOWNERS entry after migrating the
independent positive planner assertions below. Do not move the framework wholesale into
`_test.go`, create another matcher abstraction, or add a compatibility shim.
Unknown out-of-tree Go importers are outside the source-compatibility proof.

Also retire these independent disconnected closures, after proving references:

- `rule.PredicatePushdown`: its entire old implementation and commented
  registration. It is not registered in either optimizer and its Apply signature
  does not implement the current Rule contract. Keep QueryBuilder's active
  pushdown, constant-folding rules and prepare/normal optimizer traversal.
- `vector.FixedSizeFindFirstIndexInSortedSliceWithCompare` and
  `FixedSizeGetMinMax`: preserve active ordered/floating/varlen helpers and every
  caller's comparison, NULL and allocation policy.
- `Decimal128.Mul128InPlace` and `AddInplace`: preserve active scalar/vectorized
  arithmetic, overflow handling, scales, codecs and layout. Replace the stale
  inline-algorithm comment referencing the retired multiply with its actual
  unsigned arithmetic description; do not change that live algorithm.
- `models.ExplainData.StatisticsRead`, its private-to-closure scan-name constants
  if there are no external consumers, and unused `NewLabel`. Keep the Label
  model, JSON fields, live statistics owner `explain.GetInputRowsAndInputSize`
  and index-table exclusion. Remove only its stale migration comment.
- `util.SplitTableAndColumn`: keep active parser/binder name resolution and
  utility resource owners.

## Ownership and invariants

No runtime, registration, tagged, interface/function-value or reflective consumer
may remain for a retired entrypoint. Never equate a lexical count with a proof.
The live planner, expression evaluation, fixtures and explain representation
remain the only authorities; no state, queue, cache, worker or alternate path
is introduced. Success/error/cancel/cleanup ownership and SQL results remain
unchanged. Remove only self-tests whose sole contract is the retired matcher.

Catalog/checkpoint replay, readonly/read-instance/write-instance paths and future
wiring are excluded. No persisted schema, field order, version, codec, WAL,
checkpoint, wire or public SQL contract changes. Prior open PRs #30861 and
#30866 are excluded, including concurrent changes to those branches.

## Oracle disposition and test ownership

Migrate the 14 positive tools tests into `TestSelectPlanStructure` in
`pkg/sql/plan/basic_plan_structure_test.go`: ten named cases for the ten existing
SQL inputs. Use existing `newPlanTestProcess`, `NewMockOptimizer(false, ...)`
and `runOneStmt`; parser AST and process/pool/file-service cleanup remain with
the existing fixture owners. Use typed reachable plan nodes with bounds/nil
assertions. Add no matcher, alias registry, parser wrapper or snapshot of node
IDs, costs or formatting. No parallel shared-runtime mutation or new service.

| Retired positive tests | Retained direct oracle |
|---|---|
| `output`, `tableScan` | Single-column SQL: SELECT/PROJECT over lineitem; projected order key and scan column definition. |
| `outputTwoColumns` | Repeated-column SQL: construction and order-key projection; explicitly checking both requested entries strengthens multiplicity coverage. |
| `outputTwoColumns2` | Existing five-expression SQL: construction, order key among outputs, lineitem scan. Extra-output acceptance is a retired matcher rule. |
| `strictOutput`, `strictTableScan` | Two-column SQL: exact output-name multiset and exact scan projection names/type IDs; retain strict cardinality. |
| `constant` | Grouped SQL: projected order key and descendant lineitem scan. The old pattern did not prove literal 2 or a particular aggregation node. |
| `aliasConstant` | Literal-projection SQL: matching literal 2 in PROJECT above the correct scan. |
| `aliasExpr`, `strictProject`, `identityAlias` | Expression SQL: identity column plus expression using literal 1/order key, exact two projections and relevant type IDs; explicitly check the + operator. |
| `joinMatcher` | INNER JOIN, lineitem/orders children, one order-key equality and no join filter. |
| `selfJoin` | Two orders scan occurrences, INNER JOIN, order-key equality and no join filter; no invented distinct-binding-tag oracle. |
| `aggr` | PROJECT/AGG/nation scan, COUNT and projected aggregate output; allow existing count/starcount normalization. |

Names above omit their existing `Test_` prefix. Retire seven matcher-only
negative tests without replacement: `aliasDoesNotExists`,
`referAliasDoesNotExists`, `strictOutputExtraSymbols`,
`strictTableScanExtraSymbols`, `strictProjectExtraSymbols`, `duplicateAliases`,
and `projectLimit`. They reject invalid patterns over valid SQL, not invalid
SQL. `projectLimit` contains no LIMIT; it references unresolved pattern alias
X_ORDERKEY. Their positive structural/cardinality guarantees migrate above.

Retire `TestAssertPlanPreservesRuntime` with its deleted wrapper: the repeated
success/parse-error owner disappears. Keep core testutil process ownership,
borrowed-dependency, pool, manual-lifetime, construction-failure and Linux
file-service guards, auto-increment setup idempotence and planner mock/fixture
ownership tests. These protect their own owners, not an asserted identical
replacement of the removed wrapper. Existing generic SQL success/deep-copy
suites alone do not replace the structural oracles above.

Alternatives: status quo keeps a parallel unused framework; moving it wholesale
into test files retains its complexity; deleting every self-test loses positive
planner observations. Complete retirement plus the small typed migration keeps
those safeguards and removes the abstraction. No compatibility shim or new DSL
has a demonstrated in-tree need.

## Validation and costs

The change crosses at least five owners: design approval precedes implementation,
with a traceable reviewed document hash/revision and decision. Keep review
records/probes outside source and publish approval provenance in the PR body.
Every retained executable production body must stay unchanged; neighboring
edits are only unused imports and the two stale comments/registration comment.
Decimal's two-word layout, ProtoSize/Marshal/Unmarshal hooks, model JSON fields
and statistics/index-table exclusion remain byte-unchanged.

Focused normal checks must visibly select all ten `TestSelectPlanStructure`
cases, live constant-fold/prepare controls, existing single-table/join tests,
fixture ownership and mock-process reuse. Primitive controls include decimal
add/multiply, overflow, marshal/primitive codec cases; D128 fast arithmetic
and alias-overflow cases; vector search/minmax and zone-map consumers. Explain
controls include diagnostic-memory statistics, read size, concurrent JSON
statistics and frontend composite/top-level projection preservation.

Run full normal/race suites once over these surviving owners and bounded real
consumers for the issue-to-pr gate:

```text
./pkg/container/types
./pkg/container/vector
./pkg/sql/models
./pkg/sql/util
./pkg/sql/plan/rule
./pkg/sql/plan
./pkg/sql/plan/function
./pkg/sql/plan/explain
./pkg/testutil
./pkg/vm/engine/tae/index
./pkg/sql/compile
./pkg/frontend
```

The fixture, zone-map, compile and frontend packages exercise retained APIs;
none is presented as a consumer of a retired entrypoint. The removed tools
package cannot receive a post-deletion pass. Route deleted-only package static
checks to the surviving owners above. Use the repository CGo wrapper, disk
scratch space, non-empty selection, count=1, bounded timeouts and terminal exits.
No adaptive race repetitions without a concrete new failure or concurrency change.

Run scoped vet/configured lint and genuine non-JSON pinned molint with baseline
reconciliation, format/reference checks and one CPU main build with verified
native inputs. Check current prior-PR overlap read-only before publication.
GPU, BVT, benchmarks and restart/upgrade are N/A: no GPU registration/native
path, live SQL behavior, hot-path work, or persisted/distributed state changes.
No throughput, memory or CI-time gain is claimed. Independent tests/docs quality
and explicit oracle preservation govern acceptance, not deletion counts.
