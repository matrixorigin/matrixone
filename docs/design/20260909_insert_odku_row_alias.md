# INSERT ODKU row aliases: design approval

- Status: proposed design revision; implementation approval is blocked until this document is accepted
- Tracking issue: https://github.com/matrixorigin/matrixone/issues/28160
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28439
- Compatibility baseline: `main@e9bfd9bbdde3b2a66b36fb9f1f39b0d4e31749f5` (2026-09-21)
- Retained-rejection reference: implementation PR #28439 at
  `dd2b0b38f056ac56671a5d78c9de33b68b06ef07`
- Scope: MySQL-compatible `INSERT ... VALUES/SET ... AS row_alias[(column_alias, ...)]`
- Oracle: MySQL 8.4 grammar and `insert_update.test`

This document is the approval boundary for the row-alias implementation. It
does not claim that the implementation PR is mergeable, that CI is QA, or that
issue #28160 is complete. The implementation PR remains Draft until a
maintainer approves this exact support matrix and the implementation proves the
runtime gates below.

## Decision requested

Approve the following restricted contract for the first implementation:

1. Keep the row alias in the INSERT AST and create a statement-local mapping
   only after the target table and effective INSERT columns are known.
2. Bind `row_alias.column` to the immutable incoming-row identity and
   `target.column` to the target-row identity. Resolve inner `FROM` names
   before considering an outer alias; a local name may shadow the row alias.
3. Remap by target-column identity after generated/default projection is built.
   Never use a source-array position, expose hidden columns, mutate catalog
   metadata, or persist execution values.
4. Keep `VALUES(column)` semantics and diagnostics unchanged. The feature adds
   no catalog field, plan operator, protobuf field, protocol capability,
   feature flag, or persisted state.
5. Treat duplicate-key arbitration as the boundary for every UPDATE-only RHS.
   The first implementation supports row-alias RHS expressions only when their
   expression tree contains no subquery. A row-alias RHS containing any
   scalar, `EXISTS`, `IN`, nested, or other subquery is rejected before key
   lookup, flattening, or build-side execution. This applies to both keyed and
   no-key targets.
6. Preserve the main compatibility baseline for legacy direct expressions,
   `VALUES(column)`, and uncorrelated subqueries. The fail-closed rejection of
   legacy target- or candidate-correlated subqueries is a deliberate contract
   introduced by this revision, retained from the implementation reference
   above; it is not claimed to be an existing behavior of `main`. This revision
   admits no new legacy shape.

The first implementation therefore chooses a conservative reject path. A
future lazy execution path may expand the row-alias subquery matrix only in a
separate approved design revision. An `ON` predicate cannot substitute for
that gate because the current join executor can materialize a subquery input
before duplicate-key arbitration.

## Supported and rejected matrix

“Direct” below means that the UPDATE RHS expression tree contains no `Subquery`
node. Function calls, casts, `CASE`, `NULL`, literals, and prepared markers are
direct expressions when they contain no subquery.

| Shape | Decision | Required terminal behavior |
|---|---|---|
| `VALUES (...) AS new` with direct `new.column` expressions | Supported | Candidate values reach the selected INSERT/UPDATE row exactly once |
| `SET ... AS new` with direct `new.column` expressions | Supported | Same identity and assignment-order guarantees as `VALUES` |
| `AS new(c1, c2, ...)` with a one-to-one visible target mapping | Supported | Invalid, duplicate, hidden, generated, or count-mismatched names fail before execution |
| An unqualified incoming alias column after `AS new(c1, ...)` | Supported when unique | It binds to the incoming identity; unknown or ambiguous names fail before execution |
| Qualified `target.column` in a direct RHS | Supported | It binds to the target identity; value visibility keeps the existing left-to-right ODKU assignment contract |
| An inner `FROM` name shadows an outer row alias | Binding invariant | The local binding wins during validation; if the containing row-alias RHS has a subquery, the whole statement is rejected by the gate below |
| `VALUES(column)` in legacy syntax | Supported unchanged | Existing result, error, and diagnostic semantics remain unchanged |
| Any subquery in a new row-alias ODKU RHS, including uncorrelated scalar/`EXISTS`/`IN`, correlated, nested, and multi-row forms | Rejected in this revision | `ErrUnsupportedDML` before key lookup/flatten/build; exact wire error is specified below; no target mutation and the connection remains usable |
| Target- or candidate-correlated ODKU subquery in legacy syntax | Rejected fail-closed by this revision | Use the retained-rejection `ErrUnsupportedDML` contract below; no target mutation and the connection remains usable. This is a deliberate compatibility change from the `main` baseline |
| Legacy uncorrelated ODKU subquery with no row alias | Compatibility-preserved only | The free-variable set excludes target and incoming-row bindings; result, error code/text, and state must match the `main` baseline. This is not new admission or evidence that row-alias subqueries are safe |
| No-key fallback with a direct row-alias RHS and generated-column `DEFAULT` | Supported boundary | Validate names, types, and complete parameter metadata, discard the unreachable UPDATE arm, and perform the ordinary insert |
| No-key fallback with a row-alias RHS containing a subquery | Rejected by the same pre-fallback gate | Return the row-alias subquery error without evaluating the unreachable subquery or partially inserting |
| `INSERT ... SELECT`, `VALUES ROW(...)`, `REPLACE`, `INSERT OVERWRITE` row-alias forms | Rejected | Parser/planner error with no partial target mutation |

The row-alias subquery rejection is deliberate. For example:

```sql
CREATE TABLE t(id INT PRIMARY KEY, b INT);
CREATE TABLE s(y INT);
INSERT INTO s VALUES (10), (20);
INSERT INTO t VALUES (1, 5) AS new
  ON DUPLICATE KEY UPDATE b = new.b + (SELECT y FROM s);
```

With an empty `t`, a lazy future implementation could insert one row without
evaluating the UPDATE arm. The first implementation instead rejects the
row-alias shape before key lookup or flatten/build. The required terminal state
is: the specified unsupported-DML error, `t` unchanged, and a following
statement on the same connection succeeds. This closes both the empty-target
and duplicate-target cases; it does not claim that the current eager path is
safe.

The ordering is explicit. Parser syntax errors and structural row-alias
declaration errors (unknown, duplicate, hidden, generated, or count-mismatched
columns) are reported first. Next, an AST-only walk records subquery nodes,
scope-local names, and prepared-marker offsets without catalog lookup, type
lowering, flattening, or evaluation. For a new row-alias RHS, finding any
subquery returns the row-alias rejection before recursive binding and before
deciding whether the target has a duplicate key; invalid names or types inside
that rejected subquery do not replace the deterministic rejection. Only a
subquery-free direct RHS enters ordinary name/type/parameter binding and may
reach the no-key fallback, where the unreachable update arm is discarded after
validation. Legacy statements retain the `main` fallback behavior for the
baseline-compatible set; the deliberate legacy correlated rejection is checked
by its retained-rejection contract before lowering.

## Error contract

For a subquery in a new row-alias UPDATE RHS, the implementation must use
`moerr.ErrUnsupportedDML` (internal code `20313`, MySQL code `ER_UNKNOWN_ERROR`,
SQLSTATE `HY000`) with this exact full text:

```text
unsupported DML: row-alias subqueries in on duplicate key update cannot be evaluated before duplicate-key action
```

Legacy target- or candidate-correlated ODKU subqueries use the retained
implementation contract (this is not present on the `main` compatibility
baseline) with `ErrUnsupportedDML` and this exact full text:

```text
unsupported DML: target-correlated subqueries in on duplicate key update cannot be evaluated before duplicate-key action
```

Legacy uncorrelated subqueries are compared against the frozen baseline and
must not be normalized to either new message by this revision.

## Scope, ownership, and invariants

The parser owns syntax retention and formatting. The planner owns validation,
identity remapping, name resolution, the subquery gate, and the fail-closed
error. The existing dedup, ordered-assignment, row materialization, and
index-maintenance paths remain the execution owners. No new executor or wire
state is introduced.

For every candidate row:

- each legal row-alias reference resolves to one immutable target-column
  identity and the final incoming projection for that identity;
- each qualified target reference resolves to the target-column identity, and
  its value visibility follows the existing left-to-right assignment rules;
- nested scopes preserve local shadowing and do not leak statement-local alias
  state;
- prepared parameter offsets survive parsing, binding, fallback, and repeated
  execution, including rejected statements;
- no row-alias UPDATE-only subquery is evaluated before the rejection gate;
- rejection and runtime errors leave no partial target mutation and permit the
  connection to execute a following statement.

The gate ordering is part of the contract, not an implementation detail. The
no-key route may discard an unreachable UPDATE arm only after direct-expression
validation; it must not be used to justify evaluating a row-alias subquery.

## Alternatives rejected

- Global AST replacement cannot model nested scope, shadowing, correlation, or
  ambiguous bare names.
- A catalog/table binding would pollute shared metadata and let a statement
  alias escape its ODKU scope.
- A new operator or protobuf field would duplicate incoming/old-row images
  without solving the duplicate-arbitration ordering contract.
- Keeping source-array positions after generated/default rewriting can bind a
  value to the wrong target identity or expose hidden columns.
- Allowing eager subquery build and relying on an `ON` predicate is unsound:
  the build side can fail before the predicate or duplicate decision runs.
- Treating no-key fallback as a subquery exemption is unsound because the
  decision to take that fallback is made after the unsafe expression could be
  built.

## Acceptance evidence required from the implementation PR

The design PR itself is documentation-only. Before the implementation PR can
leave Draft, it must provide:

1. Parser and formatter tests for `VALUES`/`SET`, optional column aliases,
   prepared parsing, unsupported grammar forms, and connection reuse after
   rejection.
2. Planner tests for identity remapping, generated/default/hidden columns,
   local shadowing, ambiguous names, direct no-key fallback, the pre-fallback
   subquery gate, and complete parameter traversal.
3. BVT controls for an empty keyed target, a duplicate keyed target, a no-key
   direct fallback, a no-key row-alias subquery rejection, direct aliases,
   invalid casts, multi-row and nested subqueries, prepared repeated execution,
   legacy correlated rejection, legacy uncorrelated baseline parity, and every
   rejected grammar shape. Each rejection must assert code, SQLSTATE, exact
   text, unchanged table state, and a successful follow-up statement.
4. A defect-control run at the exact implementation head
   `dd2b0b38f056ac56671a5d78c9de33b68b06ef07` showing the eager-build failure
   or premature subquery evaluation for the counterexample above. The current
   `main` baseline cannot parse row-alias syntax, so it must not be reported as
   an executable control for that query. The corrected exact head must show the
   new deterministic rejection and no mutation.
5. A legacy compatibility run that compares the fixed no-row-alias corpus
   that must remain unchanged (direct expressions, `VALUES(column)`, and
   uncorrelated scalar/`EXISTS`) between the `main` compatibility baseline and
   the implementation head, including result/error/state, not only a plan
   shape. Correlated cases are tested separately against the retained-rejection
   contract above, including keyed and no-key targets; they are not claimed as
   `main` parity.
6. Maintainer approval of this exact matrix and QA validation of the
   user-visible SQL compatibility boundary. CI success alone is not design
   approval.

## Approval checklist

- [ ] The maintainer accepts the supported/rejected matrix above, including
      the same pre-fallback rejection for keyed and no-key row-alias subqueries.
- [ ] The maintainer accepts the legacy ODKU compatibility rule and fixed
      baseline corpus.
- [ ] The `ErrUnsupportedDML` code, SQLSTATE, exact text, and no-partial-
      mutation terminal are specified.
- [ ] The implementation PR links this design revision and remains Draft until
      the evidence above is complete.
- [ ] A future lazy UPDATE-only subtree, if desired, will use a new design
      revision rather than silently widening this contract.
