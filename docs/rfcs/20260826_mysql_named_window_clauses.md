# MySQL Named `WINDOW` Clauses

- Status: in-progress
- Start Date: 2026-08-26
- Owners: SQL parser, planner, and frontend maintainers
- Implementation PR: [#27515](https://github.com/matrixorigin/matrixone/pull/27515)
- Issue: [#27382](https://github.com/matrixorigin/matrixone/issues/27382)
- Design-review decision: pending independent approval

## Summary

Add MySQL-compatible named `WINDOW` clauses to a single `SELECT` query block.
The feature permits a window specification to be declared once, reused by
multiple functions, or derived from another named window. It preserves the
existing inline `OVER (...)` behavior and has no catalog, storage, wire-format,
or configuration change.

## Problem and compatibility target

MySQL 8 supports `WINDOW name AS (specification)` after the query body. Before
this work MatrixOne rejected the syntax during parsing, even when the equivalent
inline `OVER (...)` query executed successfully. The compatibility target is
the MySQL 8 named-window grammar and observable error behavior exercised by
MySQL 8.0.46 and checked against the MySQL 8.4 limit/error contracts:

- declarations are scoped to one query block;
- `OVER name` reuses a declared window, while `OVER (name ...)` is an inline
  derived specification;
- a derived window cannot add partitioning, cannot reference a framed base, and
  cannot redefine its base ordering;
- forward references are valid, cycles and absent names are rejected;
- at most 127 windows are permitted per query block. Every declaration and each
  parenthesized/non-reference `OVER (...)` occurrence counts; a bare `OVER name`
  does not create another window;
- parser/binder failures retain the corresponding MySQL code and SQLSTATE.

The invariant is that every syntactically present window declaration is bound
and validated exactly once in its query-block scope, and every public consumer
of the statement observes the same parameter metadata and error contract. The
smallest negations are an unused declaration losing a prepared marker, an
identical inline `OVER()` bypassing the 127 limit, or a reference resolving in a
different query block.

## Design

### Syntax and AST ownership

The MySQL grammar owns the `WINDOW` clause and produces ordered
`tree.WindowDefinitions` on `tree.SelectClause`; `tree.WindowSpec` owns the
optional base-name reference, partitioning, ordering, and frame. The ordered
slice is the source-of-truth for declaration order and deterministic diagnostic
selection. A map is used only for lookup during resolution.

The parser generator output is regenerated from `mysql_sql.y`. Formatting,
visitation, statement remapping, sidecar transfer, DDL/view traversal, and
prepared-statement parameter collection traverse the new fields just as they
traverse inline window specifications. This prevents a valid clause from being
lost at a consumer boundary.

### Binding and error contract

`buildNamedWindowPlan` is the first owner of query-block window resolution. It
validates the per-block count before expansion, resolves declarations by ordered
DFS, and expands inherited fields without mutating a shared declaration. The
resolver returns stable `moerr` constructors for missing names, circularity,
partitioning/frame/order conflicts, duplicate names, and the limit. Inline
references use the same inheritance helper and pass the child display name
(`\`<unnamed window>\`` for an inline clause) and base display name separately.

Validation-only binding also walks every named declaration. Its parameter
positions are merged into the real query metadata so an unused declaration
still has the prepared-statement arity required by the protocol. Reusing one
declaration does not duplicate its markers.

### Scope, complexity, and lifecycle

Each `SELECT` owns an independent declaration map and counter; a nested select
starts a new scope and has its own 127-window limit. Resolution is bounded by
the same limit and is linear in declarations plus referenced specifications.
There is no retained state, goroutine, cache, background work, persistent data,
or cross-query mutation. Normal success and every bind error discard only the
query-local structures. Consequently no upgrade, downgrade, mixed-version,
rollback, backup/restore, feature flag, or operational migration is needed.

The explicit cap bounds user-controlled planning work and avoids unbounded
recursive resolution. The feature is parser/planner CPU and allocation work per
query; it adds no per-row execution state beyond the already-existing window
operator.

## Alternatives and tradeoffs

1. Keep only inline `OVER (...)`. This leaves a documented MySQL compatibility
   gap and forces callers to duplicate specifications.
2. Expand syntax in the parser but resolve names lazily in each window function.
   This duplicates validation, loses deterministic source ordering, complicates
   parameter metadata, and makes the global query-block cap difficult to prove.
3. Use a common query-block resolver with ordered declarations and a lookup map.
   This is selected because it supplies one ownership boundary for validation,
   diagnostics, the resource bound, and all statement consumers.

The selected design deliberately rejects unsupported inheritance combinations
rather than silently merging conflicting specs. It favors MySQL diagnostic
compatibility over accepting an ambiguous extension.

## Validation and acceptance map

| Contract | Owner-level proof | Public SQL proof |
|---|---|---|
| grammar, AST formatting, traversal | parser/tree/frontend unit tests | named reuse and inheritance result rows |
| ordered resolution and inheritance errors | planner/moerr unit tests with code and SQLSTATE assertions | undefined-name rejection |
| marker retention and reuse de-duplication | planner/frontend prepared metadata tests | prepare an unused named window and execute it with one parameter |
| 127 cap and inline occurrence counting | planner boundary tests, including nested-select control | 127 declarations accepted and 128 rejected |

The BVT uses a four-row table in its own database, deterministic ordering, no
sleeps, and same-instance cleanup. It is run twice against a ready test-owned
service to prove that its database teardown does not leave catalog residue.
Existing focused parser, planner, frontend, and `moerr` tests remain the
cheapest white-box proofs; the BVT supplies the independent frontend/SQL oracle.

## Rollout and review record

There is no feature gate or data migration: support becomes available when a
node runs this binary. Since no persistent or distributed state is introduced,
mixed-version behavior is the ordinary SQL-version behavior: a statement using
`WINDOW` must be routed to a node containing this parser and binder.

This document is the stable, versioned design revision for #27515. Its
independent design-review approval is intentionally recorded as pending rather
than self-certified by the implementation author. Implementation changes must
remain aligned with this document; a semantic expansion (new inheritance rules,
state, or rollout behavior) requires a revised design decision before delivery.
