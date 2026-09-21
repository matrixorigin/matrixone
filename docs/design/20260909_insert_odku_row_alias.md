# INSERT ODKU row aliases

- Status: implementation complete with correlated ODKU subqueries fail-closed; maintainer design approval and real BVT/QA remain open
- Tracking issue: https://github.com/matrixorigin/matrixone/issues/28160
- Baseline for this rebase: `780ef933f2479aa1679faf13d08850ce6c8f7c2f` (current `main` fetched 2026-09-18)
- Scope: MySQL-compatible `INSERT ... VALUES/SET ... AS row_alias[(column_alias, ...)]`
- Oracle: MySQL 8.4.0 grammar and `insert_update.test`

## Decision record

This document records the design and implementation boundary for #28160. The
task owner selected the restricted-correlation design for this round. The
implementation is ready for code review and CI, while the exact support matrix
still requires maintainer approval. Issue closure is outside this change.

**Design decision (2026-09-17): restricted scope pending maintainer approval.** The selected
design keeps row aliases in the INSERT AST, builds a statement-local mapping
after target-table resolution, binds aliases only in the ODKU RHS scope, and
lowers references to existing incoming/old-row plan columns. It adds no plan
operator, protobuf field, catalog entry, feature flag, or persisted state. This
round does not introduce a general correlated subquery execution framework.

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
   shadow the outer row alias. Direct row-alias and target references retain
   their bound tags, but target- or candidate-correlated ODKU subqueries are
   rejected. The current join executor materializes a subquery build side
   before the duplicate-key action is selected, so an ON predicate cannot
   safely defer the complete UPDATE-only subtree.
5. `VALUES(column)` keeps its existing semantics and diagnostics. The mapping
   is planner-owned and statement-local; it never mutates catalog
   `Name2ColIndex` or stores execution parameter values.
6. The no-key ODKU fallback validates names and expression types before it
   discards the unreachable update action. Complete expression traversal
   retains every nested prepared-parameter offset without evaluating a fake
   projection.
7. Target- or candidate-correlated ODKU subqueries are rejected before
   lowering, including single-row `VALUES`/`SET`, multi-row sources,
   `INSERT ... SELECT`, and later assignments. This is a deliberate
   fail-closed boundary: the executor must first gain lazy evaluation for the
   entire UPDATE-only subtree before this syntax can be admitted. Uncorrelated
   subqueries remain governed by the existing planner path.
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
target/correlated references, fail-closed rejection of every
target/candidate-correlated subquery before lowering, the generated-DEFAULT
no-key fallback, ambiguity/error checks, and complete nested
parameter-offset collection. The distributed ODKU case proves shuffled
columns, ordered assignments, repeated keys, SET aliases, shadowing,
uncorrelated-subquery behavior, CASE/NULL, repeated SQL PREPARE execution,
and the generated-column fallback's final row image; correlated subquery
cases are expected to be rejected with the safety diagnostic.

The final implementation report must keep the following separate:

- parser generation and parser tests;
- planner tests and any CGo/toolchain or native-artifact blocker;
- real service and binary-protocol BVT/QA evidence;
- CI/review status on the exact pushed head.

As of this revision, the focused parser/planner tests pass with the
worktree's provenance-checked native CGo artifacts, including the direct alias
binding, supported single-row correlation, and explicit rejection matrix. A
fresh distributed SQL/BVT run was not available in this worktree; the runner
and real-service binary checks remain CI/QA gates and are not claimed here.

## Compact review record

```text
Change scope: #28160 VALUES/SET row-alias feature with restricted correlation
Trigger: SQL-visible feature crossing parser, AST, planner, prepared, and BVT boundaries
Design: this document; maintainer approval of the support matrix is pending
Blocking findings: design approval, planner CGo/native artifacts, and real-service QA remain open evidence gaps
Decision log: statement-local identity mapping; scoped binder; no new executor/wire/catalog state; explicit ROW/SELECT/REPLACE/OVERWRITE exclusions
Decision: restricted implementation is complete; maintainer approval and
independent CI/QA review are still required; no merge or issue-closure decision
Implementation boundary: direct row aliases and uncorrelated subqueries use
the existing planner path. Every target/candidate-correlated ODKU subquery is
an explicit non-goal and is rejected before flattening. Full
correlated-subquery execution is deferred until the executor can make the
entire UPDATE-only subtree lazy.
```
