# INSERT ODKU row-alias readiness

## Policy and evidence identity

The approved design is `20260909_insert_odku_row_alias.md`, blob
`f867636fd7dcaa6643911a129861ba0ba3bbbc0d`.
Design approval was merged as `c61b617e5429f4a65e7c5665c686f46286cfdef9`.
Its acceptance matrix and before-leaving-Draft requirements remain authoritative.
This implementation remains **Draft pending external QA acceptance**.

Current maintenance starts from source
`f39f156059d28cfc0b0ba812bf5455b2755e9b52`, rebased onto main
`3fc00709eccc277c175c7a09eed2605ca1aaee29`. The original 41 commit objects,
parents, authors and exact source tree are archived. Final delivery commit/tree,
commands, input fingerprints and terminal receipts are recorded with the PR.

The binder now rejects subqueries introduced by SQL UDF expansion in the same
row-alias admission owner as explicit subqueries. The earlier whole-RHS AST
check remains, preserving explicit-subquery error priority and no-key rejection.
Legacy uncorrelated UDFs and expression-only row-alias UDFs retain their paths.
No new wire, function ID, execution operator or background state is introduced.

## SQL-facing admission and rollout

- Inventory every SQL-facing parser/planner route, including direct CN and proxy
  routing, by build and supported SQL class. Admit row-alias SQL only when all
  eligible routes use the upgraded parser/planner and pass the same probes.
- Do not randomly dispatch this SQL class across old and new routes. The absence
  of a wire change does not prove parser compatibility or mixed-route safety.
  Keep legacy correlated clients on a separately validated route until migrated.
- Migrate legacy correlated ODKU clients before opening new admission. Upgrade
  and probe the homogeneous route, then enable clients using the new syntax.
- Roll back by stopping new syntax, draining in-flight and prepared statements,
  and downgrading the entire eligible SQL-facing route before verifying the
  legacy corpus. Do not rely on a prepared statement surviving route/version
  changes. Record actual client and operator owners in the rollout record.

For each rejection or discrepancy record SQL shape, alias/legacy classification,
SQL mode, build/route, errno, SQLSTATE and full error text, OK/ERR and affected
rows, table/transaction state before and after, and same-connection recovery.
The main-table write observer proves successful action-path writes, not commit.

## Current scope and pending gates

Local checks use Go1.27.1/darwin-arm64, CPU, and verified native artifacts. A
single existing shared one-CN fixture exercises VALUES/SET, reordered aliases,
left-to-right assignment, generated/default materialization, no-key fallback,
visible fake-key spelling, multiple allocators, rollback, explicit and expanded
UDF subquery rejection without mutation, expression UDF/legacy controls, and
same-handle binary prepared success → failure → success with changed parameters.
The driver control does not assert packet-level cached bind-type or statement-ID
identity and cannot substitute for the design's full binary protocol oracle.

| Gate | Evidence / remaining requirement |
| --- | --- |
| Exact matrix maintainer approval | Approved design merge above; policy unchanged |
| Current-head local regressions | Commands, outcomes and scoped race/SCA in final PR delivery record |
| Frozen old-head defect / exact legacy parity / typed binary corpus / two-batch historical probes | Historical evidence retains its original input identity; not relabeled as rerun at this head |
| Actual route inventory and homogeneous admission | NOT_RUN: local one-CN checks prove only the local route |
| Mixed-route topology, migration, drain and rollback rehearsal | NOT_RUN: require deployment-owner record |
| User-visible compatibility QA acceptance | PENDING: requires QA-owner acceptance; local implementation checks and CI do not grant it |

Leaving Draft requires the approved matrix and these missing acceptance records;
this document is a concrete rollout procedure, not a waiver or deployment claim.
