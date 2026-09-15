# INSERT ODKU row aliases

- Status: implementation complete; PR #28439 is Ready for CI/QA, issue #28160 remains open
- Tracking issue: https://github.com/matrixorigin/matrixone/issues/28160
- Baseline: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7` (verified `main` before implementation)
- Scope: MySQL-compatible `INSERT ... VALUES/SET ... AS row_alias[(column_alias, ...)]`
- Oracle: MySQL 8.4.0 grammar and `insert_update.test`

## Decision record

This document records the design and implementation boundary for #28160. The
task owner supplied and approved the fixed semantic plan before implementation;
the implementation is Ready for repository review, CI, and QA. Issue closure is
outside this change.

**Design decision (2026-09-09): PASS for implementation scope.** The selected
design keeps row aliases in the INSERT AST, builds a statement-local mapping
after target-table resolution, binds aliases only in the ODKU RHS scope, and
lowers references to existing incoming/old-row plan columns. It adds no plan
operator, protobuf field, catalog entry, feature flag, or persisted state.

The decision log accepts these constraints from the approved plan:

1. `VALUES` and `SET` accept a row alias and an optional, ordered column list;
   the list is valid only with a row alias. A plain INSERT may declare the
   alias, while `INSERT ... SELECT`, `VALUES ROW(...)`, `REPLACE`, and
   `INSERT OVERWRITE` do not enter this grammar.
2. An alias column maps to the target identity in the original INSERT/SET
   order. The mapping is remapped by target identity after generated/default
   projection construction, so it cannot depend on a transient source-array
   position or expose hidden columns.
3. `row_alias.column` reads the candidate incoming row and `target.column`
   reads the old target row. A bare name is ambiguous when both namespaces
   expose it. ODKU assignment targets always remain target-table columns.
4. Subqueries resolve their local bindings first. An inner table alias may
   shadow the outer row alias; an unresolved reference can correlate through
   the existing parent-binder chain and retains its `CorrColRef` depth.
5. `VALUES(column)` keeps its existing semantics and diagnostics. The mapping
   is planner-owned and statement-local; it never mutates catalog
   `Name2ColIndex` or stores execution parameter values.
6. The no-key ODKU fallback validates names and expression types before it
   discards the unreachable update action. Complete expression traversal
   retains every nested prepared-parameter offset without evaluating a fake
   projection.
7. A correlated ODKU subquery is flattened only after a statement-local,
   snapshot target lookup has been joined into the candidate subtree. Its
   private binding tag supplies the old target row to `CorrColRef`; the later
   DEDUP target scan remains the conflict arbiter. This reuses the existing
   LEFT/JOIN and scalar-subquery operators and preserves the target lookup when
   the DEDUP inputs are remapped.
8. Generated-column `DEFAULT` trimming is applied to private statement/source
   copies. The original INSERT columns remain available for row-alias identity
   validation, while the effective legacy fallback receives matching columns
   and value rows, including the no-key plain-insert route.

## Problem and invariant

The parser previously lowered INSERT values into a `ValuesClause` without
retaining the row-alias syntax, and the ODKU binder resolved every reference
against the old target row. As a result, MySQL-compatible statements could not
name the candidate row, could bind a shadowed/correlated name to the wrong
scope, and could lose parameters when a no-key table fell back to the legacy
plain-insert path.

The invariant is:

> For each candidate row, every legal row-alias reference resolves to exactly
> one immutable target-column identity and then to the final incoming
> projection for that identity; every target-table reference resolves to the
> old-row scan; all nested scopes and prepared metadata preserve the existing
> binder and execution contracts.

The negation is any position-based or global AST substitution that can expose a
default/generated/hidden column, evaluate the source expression twice, ignore a
wrong qualifier, leak an alias into another statement, or drop a parameter.

## Ownership and flow

1. The MySQL grammar creates `Insert.RowAlias` and retains whether the source
   used explicit `ROW(...)`; the tree formatter emits the alias between the
   input rows and ODKU.
2. After the target table is resolved, the planner validates the alias, target
   qualifiers, duplicate/count rules, and effective visible INSERT columns.
3. `initInsertReplaceStmt` builds the normal incoming projection. Alias entries
   are then remapped by `table.column` identity to the final projection, which
   includes defaults and generated columns where the existing planner requires
   them.
4. `OndupUpdateBinder` is installed only while ODKU RHS expressions are bound.
   Its local alias map points to `selectTag`; direct target references point to
   `scanTag`, while depth-one references from a subquery use the private target
   lookup tag described above. Nested bind contexts retain the existing parent
   chain. The previous binder is restored on both success and error paths.
5. Existing dedup, ordered assignment, final row materialization, index
   maintenance, and transaction handling consume the resulting ordinary plan
   expressions. No executor or wire change is needed.
6. A no-key fallback performs the same alias/name/type validation, then uses
   the existing legacy plain-insert plan. Parameter offsets are copied as
   metadata only, so unreachable ODKU expressions are not evaluated.

## Alternatives rejected

- Global AST name replacement cannot model nested scope, shadowing,
  correlation, or ambiguous bare names.
- Registering the alias as a catalog/table binding would pollute shared
  `Name2ColIndex`, make lifetime/reuse statement-dependent, and allow the
  alias to escape the ODKU scope.
- Adding a new execution operator or protobuf field would duplicate the
  existing incoming/old-row images without changing the required semantics.
- Keeping source-array positions after generated/default rewriting would map a
  candidate value to the wrong target column; identity remapping is required.

## Validation map and open gates

The parser tests prove AST retention, formatting round trips, rejection of
unsupported forms, SQL PREPARE parsing, and nested expression syntax. Planner
unit tests prove identity remapping, generated-column positions, incoming vs
target/correlated references, target lookup reachability in a real BuildPlan,
the generated-DEFAULT no-key fallback, ambiguity/error checks, and complete
nested parameter-offset collection. The distributed ODKU case proves shuffled
columns, ordered assignments, repeated keys, SET aliases, shadowing,
correlation, CASE/NULL, repeated SQL PREPARE execution, and the generated
column fallback's final row image.

The final implementation report must keep the following separate:

- parser generation and parser tests;
- planner tests and any CGo/toolchain or native-artifact blocker;
- real service and binary-protocol BVT/QA evidence;
- CI/review status on the exact pushed head.

As of this revision, parser and planner package validation pass with the
worktree's provenance-checked native CGo artifacts. A test-owned LOG/TN/CN
instance also passes the correlated SQL, generated-column fallback, SQL
PREPARE, and binary COM_STMT_PREPARE/EXECUTE checks. The repository's Java
mo-tester wrapper was unavailable on this host, so the distributed runner
remains a CI/QA gate.

## Compact review record

```text
Change scope: complete #28160 VALUES/SET row-alias feature
Trigger: SQL-visible feature crossing parser, AST, planner, prepared, and BVT boundaries
Design: this document, reviewed revision is the implementation commit; Ready pending independent CI/QA review
Blocking findings: no design-level blocker; planner CGo/native artifacts and real-service QA remain open evidence gaps
Decision log: statement-local identity mapping; scoped binder; no new executor/wire/catalog state; explicit ROW/SELECT/REPLACE/OVERWRITE exclusions
Decision: PASS for implementation scope; Ready for independent CI/QA review; no merge or issue-closure decision
Implementation deviations: the approved scope is unchanged; correlated target
references are made reachable with an existing LEFT JOIN lookup below the
candidate pipeline before flattening, and generated-default rewrites stay on
private statement copies so legacy fallback columns and values remain aligned
```
