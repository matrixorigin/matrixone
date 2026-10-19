# Native branch admission for materialized views

Tracker: [#30079](https://github.com/matrixorigin/matrixone/issues/30079).
PR: [#30085](https://github.com/matrixorigin/matrixone/pull/30085).
Review: 5564803316 on `50e7f92bd08e6ba0491d8c63368f8720f874d9f5`.

## Requirement and observed failure

An unrefreshed MV must not be treated as valid empty data. The review's native
SQL reproduction rejected SELECT with MV_NOT_POPULATED but successfully read
that MV through DATA BRANCH DIFF. Branch privilege planning does not execute the
compiled scan guard, and branch relations are opened directly by the frontend.

P1 does not implement Git4Data history/clone/restore semantics for MVs. Therefore
reject MV endpoints explicitly, whether populated or unpopulated, instead of
inventing a partial branch-read contract. This ordinary integration fix adds no
catalog, protocol allocation, worker, or persistent state.

## Design and boundaries

The common `getRelations` owner resolves both branch endpoints and validates
named-snapshot scope. Before creating readers or entering diff/merge/output
processing, inspect both resolved relation definitions and return ErrNotSupported
if either has the native MV relation kind. Reuse the definitions already needed
for logging; ordinary endpoints incur no additional catalog query or allocation.
Both endpoint directions and DIFF OUTPUT AS inherit this same admission.
Existing authorization and named-snapshot validation remain ahead of the check.
Background transaction ownership and rollback/close remain unchanged.

Concurrent MV refresh cannot change the relation kind, so this stateless check
does not need a new lock, retry, or refresh-state query. Existing ordinary-table
snapshot and branch semantics are preserved. Mixed-version MV capability remains
protocol120; catalog4.0.17/offset5, definition format2 and executor17 are unchanged.

## Main conflict resolution

Merge main `75ff736835c3d00d40dc00a0a1d3b11129388e5b` into parent50e7f92b.
Resolve both SQL-executor context conflicts by preserving all four scoped values:
division precision, SQL mode, max DOP, and main's foreign_key_checks. The attached
frontend delegate retains priority. User/global-variable requests do not invoke
the scoped process resolver. Preserve upstream FK default/error/explicit-ignore
controls and the real expression-binding precision and SQL-mode controls.

## Regression map and evidence

- Existing frontend helper fixture: ordinary/target-MV/base-MV/both-MV matrix
  reaches the real relation-opening owner. Mock expectations prohibit reader,
  output and merge-write work after rejection.
- Existing native consumer fixture: ordinary self-DIFF remains empty. Before and
  after refresh, reject DIFF in both directions, OUTPUT AS, and MERGE in both
  directions. Require server errors, no output table, unchanged source/MV rows
  and complete publication metadata. Reuse the existing two-row source and add one empty, schema-compatible ordinary
  comparison table so the negative control matches the review input; cleanup suffices;
  consumer case count grows14 to17 without another service fixture in CI.
- Run the expanded public consumer against the previous service as a negative
  control and the integrated service as the positive proof. Any setup failure is
  separate from the contract result. Source-bound outcomes are in
  [the result ledger](materialized_view_branch_admission_results.json).
- Run merged planner/compiler/frontend owning suites and incremental static
  checks. Prior race, crash/recovery, compatibility and benchmark captures remain
  evidence only for unchanged owners; no new performance claim or full native
  upgrade claim is made by this change. New-head remote CI is separate.


## Preserve the prepared DELETE cache boundary

The first combined harness created its comparison table after PREPARE on the
prepared owner, which advanced that session's DDL generation and masked the
older cached-DELETE regression. This is a test-quality defect, not a new product
fix. Complete all comparison-table DDL before PREPARE and run added branch
queries/mutations on the existing other session. Keep the same prepared handle
and all original source DELETE/provenance/index assertions. Revalidate the final
client against the old branch-reader negative control and both RC/SI candidates;
previous product package/static evidence remains scoped to unchanged sources.
