# INSERT ODKU row aliases

- Status: implementation in progress; Draft PR for issue #28160
- Tracking issue: https://github.com/matrixorigin/matrixone/issues/28160
- Baseline: `c51bb4ed868219af720cb5c019fb103bc1e7bcc7` (verified `main` before implementation)
- Scope: MySQL-compatible `INSERT ... VALUES/SET ... AS row_alias[(column_alias, ...)]`
- Oracle: MySQL 8.4.0 grammar and `insert_update.test`

## Decision record

This document records the design and implementation boundary for #28160. The
task owner supplied and approved the fixed semantic plan before implementation;
the implementation remains Draft until the repository review, CI, and QA gates
provide independent evidence. No Ready promotion or issue closure is implied.

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
   Its local alias map points to `selectTag`; target references point to
   `scanTag`; nested bind contexts retain the existing parent chain. The
   previous binder is restored on both success and error paths.
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
target/correlated references, ambiguity/error checks, and complete nested
parameter-offset collection. The distributed ODKU case proves shuffled
columns, ordered assignments, repeated keys, SET aliases, shadowing,
correlation, CASE/NULL, and repeated SQL PREPARE execution.

The final implementation report must keep the following separate:

- parser generation and parser tests;
- planner tests and any CGo/toolchain or native-artifact blocker;
- real service and binary-protocol BVT/QA evidence;
- CI/review status on the exact pushed head.

As of this revision, parser validation is available locally. Planner package
execution is blocked by the worktree's native CGo dependency setup (the
serialization declarations are absent from the cached/primary native header,
and a clean local artifact set is unavailable); real CN/TN BVT and binary
protocol evidence remain QA/CI gates.

## Compact review record

```text
Change scope: complete #28160 VALUES/SET row-alias feature
Trigger: SQL-visible feature crossing parser, AST, planner, prepared, and BVT boundaries
Design: this document, reviewed revision is the implementation commit; Draft pending independent PR review
Blocking findings: no design-level blocker; planner CGo/native artifacts and real-service QA remain open evidence gaps
Decision log: statement-local identity mapping; scoped binder; no new executor/wire/catalog state; explicit ROW/SELECT/REPLACE/OVERWRITE exclusions
Decision: PASS for implementation scope; not a Ready/QA/merge decision
Implementation deviations: none from the approved semantic plan
```
