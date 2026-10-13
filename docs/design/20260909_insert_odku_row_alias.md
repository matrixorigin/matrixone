# INSERT ODKU row aliases: design approval

- Status: proposed design revision; implementation approval is blocked until this document is accepted
- Tracking issue: https://github.com/matrixorigin/matrixone/issues/28160
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28439
- Compatibility baseline: `main@e9bfd9bbdde3b2a66b36fb9f1f39b0d4e31749f5` (2026-09-21)
- Retained-rejection reference: implementation PR #28439 at
  `dd2b0b38f056ac56671a5d78c9de33b68b06ef07`
- Scope: MySQL-compatible `INSERT ... VALUES/SET ... AS row_alias[(column_alias, ...)]`
- Oracle: MySQL 8.4 grammar and `insert_update.test`
- Frozen legacy corpus: `legacy-odku-v1` below, pinned to the compatibility baseline and file blobs.

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
   A generated column may occupy an incoming alias-mapping slot and its
   materialized generated value may be read through the row alias. The source
   value for that slot must be `DEFAULT`; an explicit non-`DEFAULT` value is
   rejected. The same rule applies to an ODKU assignment whose target is a
   generated column: `DEFAULT` is allowed, any other value is rejected. Never
   use a source-array position, expose hidden columns, mutate catalog metadata,
   or persist execution values.
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
| `AS new(c1, c2, ...)` with a one-to-one visible target mapping | Supported | Invalid, duplicate, hidden, or count-mismatched names fail before execution; a generated target name is legal when its source slot is `DEFAULT` |
| An unqualified incoming alias column after `AS new(c1, ...)` | Supported when unique | It binds to the incoming identity; unknown or ambiguous names fail before execution |
| Qualified `target.column` in a direct RHS | Supported | It binds to the target identity; value visibility keeps the existing left-to-right ODKU assignment contract |
| An inner `FROM` name shadows an outer row alias | Binding invariant | The local binding wins during validation; if the containing row-alias RHS has a subquery, the whole statement is rejected by the gate below |
| `VALUES(column)` in legacy syntax | Supported unchanged | Existing result, error, and diagnostic semantics remain unchanged |
| Any subquery in a new row-alias ODKU RHS, including uncorrelated scalar/`EXISTS`/`IN`, correlated, nested, and multi-row forms | Rejected in this revision | `ErrUnsupportedDML` before key lookup/flatten/build; exact wire error is specified below; no target mutation and the connection remains usable |
| Target- or candidate-correlated ODKU subquery in legacy syntax | Rejected fail-closed by this revision | Use the retained-rejection `ErrUnsupportedDML` contract below; no target mutation and the connection remains usable. This is a deliberate compatibility change from the `main` baseline |
| Legacy uncorrelated ODKU subquery with no row alias | Compatibility-preserved only | The free-variable set excludes target and incoming-row bindings; result, error code/text, and state must match the `main` baseline. This is not new admission or evidence that row-alias subqueries are safe |
| No-key fallback with a direct row-alias RHS and generated-column `DEFAULT` | Supported boundary | Preserve generated slots while validating the alias mapping, accept `DEFAULT`, materialize the generated value, remap by target identity, discard the unreachable UPDATE arm, and perform the ordinary insert |
| No-key fallback with a row-alias RHS containing a subquery | Rejected by the same pre-fallback gate | Return the row-alias subquery error without evaluating the unreachable subquery or partially inserting |
| `INSERT ... SELECT`, `VALUES ROW(...)`, `REPLACE`, `INSERT OVERWRITE` row-alias forms | Rejected | Parser/planner error with no partial target mutation |

The mapping-count rule is fixed before generated/default rewriting:

- With an explicit `INSERT` or `SET` target list, the effective source list is
  the user-listed visible target columns in their written order. Its width,
  the source tuple/assignment width, and an optional row-alias column-list
  width must agree. A generated column counts as one slot when it is listed
  with `DEFAULT`; it is removed only from the executable projection after the
  alias identity has been recorded.
- With an implicit `VALUES` list, the effective alias-mapping list is the
  table's non-hidden columns in table order, including generated columns. For
  a non-empty tuple, the tuple width and an optional alias column-list width
  must equal that effective width. `VALUES()` is the explicit all-default
  exception: its input tuple width is zero, while an optional alias column list
  still has the full effective width and maps names to every target identity.
  The default projection is materialized after that mapping is recorded, so an
  empty tuple does not become a zero-column alias mapping.
- Hidden columns never enter either effective list and cannot be named by an
  alias. A generated slot with any non-`DEFAULT` source expression, or an
  ODKU assignment other than `generated_col = DEFAULT`, is an invalid write and
  fails before any row is changed.

For example, this explicit generated-column slot is supported on the no-key
fallback path and produces `(a, g) = (1, 2)`:

```sql
CREATE TABLE t(a INT, g INT GENERATED ALWAYS AS (a + 1) STORED);
INSERT INTO t(a, g) VALUES (1, DEFAULT) AS n(x, y)
  ON DUPLICATE KEY UPDATE a = n.x;
```

The corresponding implicit form keeps `g` in the source/alias width until the
`DEFAULT` rewrite, and `n.g` reads the materialized value. Replacing either
`DEFAULT` with an explicit value is a generated-column write and is rejected.

An empty tuple uses the same full-width alias mapping while supplying defaults
for every source column:

```sql
CREATE TABLE t(id INT PRIMARY KEY DEFAULT 1, b INT DEFAULT 7);
INSERT INTO t VALUES (1, 99);
INSERT INTO t VALUES () AS n(k, v)
  ON DUPLICATE KEY UPDATE b = n.v;
```

The candidate is `(1, 7)`, so the duplicate update stores `b = 7`. The input
tuple has width zero; `n(k, v)` still has two mapped identities. The same
exception applies when the full-width mapping includes a generated column;
the generated value is materialized from its default dependency before it is
read through the alias.

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
declaration errors (unknown, duplicate, hidden, or count-mismatched columns)
are reported first, except that `VALUES()` is allowed to have input width zero
when its optional alias list has the full effective mapping width. Generated-
column legality is then checked against the source/assignment expression:
`DEFAULT` is accepted, while a non-`DEFAULT` write is rejected before
execution. Next, an AST-only walk records subquery
nodes, scope-local names, and prepared-marker offsets without catalog lookup,
type lowering, flattening, or evaluation. For a new row-alias RHS, finding any
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

## Frozen baseline corpus and stateful acceptance oracle

`legacy-odku-v1` is a fixed comparison corpus, not a moving copy of whatever
happens to be in the test tree. Its compatibility baseline is exactly
`main@e9bfd9bbdde3b2a66b36fb9f1f39b0d4e31749f5`. The source fixtures are pinned
by both commit and blob:

- `test/distributed/cases/dml/insert/on_duplicate_key.sql`, blob
  `524d3c5da01055c0efdfe8fb93b2da75c23e2e9d`, SHA-256
  `20a146fdddc790dbc0cf694e2f3468c937f18810852ab6439dc5a4db92bd3639`;
  include the legacy direct/`VALUES()`/secondary-key, partitioned, no-op, and
  NULL cases from normal ranges 1–25, 43–89, 96–115, 145–180, and 190–214 of
  that blob. Lines 90–95 are a separate `@bvt:issue#4423` case, not part of
  the normal typed-multiset corpus: retain its lines 90–92 setup/statement and
  93–95 marker/result handling, then apply the runner's explicit BVT issue
  skip/expected-output classification.
- `test/distributed/cases/prepare/prepare.test`, blob
  `256b4b81406985b6794ec1cce58e5682ef02eab0`, SHA-256
  `bb526265ff9b7bb603eeb8140604741b506060517a74b1b7fe3ed300ac364cfc`;
  include the legacy `SET`/ODKU prepared cases from lines 282–319.
- `test/distributed/cases/function/row_count.sql`, blob
  `3bba97990bef1142c3bf417917b21a8427a8973c`, SHA-256
  `ab6df81eaf252b9f7469370539e00e44b5ea16d88634016877823e9b907737b8`;
  use lines 64–76 as the affected-row oracle for an insert, a changed ODKU
  duplicate, and a `CLIENT_FOUND_ROWS` no-op.

Each listed line range is an independent corpus case. The runner must create a
fresh schema/connection or apply the exact setup and cleanup surrounding that
range, including omitted `DROP`/`CREATE` statements needed by a range that
reuses names such as `t1` or `s1`; it must not concatenate the ranges into one
session. The setup/cleanup identity is recorded with the source blob. Result
sets without an explicit `ORDER BY` are compared as typed row multisets, not
by physical row order. In particular, split the `43–115` range at its
`t1` DROP/CREATE boundaries and at the issue case (`43–89`, `90–95`, and
`96–115`), split `145–180` at each partition/secondary-key fixture and
deallocate `s1` after its four executions, and run the `1–25` and `190–214`
ranges from their own fresh setup/cleanup. The prepared `282–319` range
likewise uses its `prepare_test` database and its included
`DEALLOCATE PREPARE` boundaries; no prepared name is carried into another
case.

The corpus also has this small deterministic control fixture for the legacy
statement shapes that the existing files do not isolate. The runner must set
and record `sql_mode = 'STRICT_TRANS_TABLES'` and the session autocommit mode
before each case, capture the protocol status flags described below, use the
same schema/seed on baseline and implementation, and compare
result rows, affected-row/OK-packet fields, error code, SQLSTATE, full message,
and table state. Result rows are compared as ordered rows only when the SQL
contains an explicit `ORDER BY`; otherwise compare typed row multisets. If the
server does not accept the requested mode, the case is `NOT_RUN`; it must not
silently use the server default.

```sql
DROP TABLE IF EXISTS odku_legacy_v1;
CREATE TABLE odku_legacy_v1 (
    id INT PRIMARY KEY,
    u INT UNIQUE,
    v INT NOT NULL
);
INSERT INTO odku_legacy_v1 VALUES (1, 10, 100), (2, 20, 200);

-- L1: direct keyed ODKU.
INSERT INTO odku_legacy_v1 VALUES (1, 11, 101)
    ON DUPLICATE KEY UPDATE v = v + 1;
-- L2: legacy VALUES(column) keyed ODKU.
INSERT INTO odku_legacy_v1 VALUES (1, 12, 102)
    ON DUPLICATE KEY UPDATE v = VALUES(v);
-- L3: legacy uncorrelated scalar subquery.
INSERT INTO odku_legacy_v1 VALUES (1, 13, 103)
    ON DUPLICATE KEY UPDATE v = (SELECT 13);
-- L4: legacy uncorrelated EXISTS subquery.
INSERT INTO odku_legacy_v1 VALUES (1, 14, 104)
    ON DUPLICATE KEY UPDATE v = IF(EXISTS (SELECT 1), VALUES(v), v);

DROP TABLE IF EXISTS odku_legacy_v1_nokey;
CREATE TABLE odku_legacy_v1_nokey (a INT, b INT);
-- L5: direct no-key fallback; the UPDATE arm is unreachable.
INSERT INTO odku_legacy_v1_nokey VALUES (1, 2)
    ON DUPLICATE KEY UPDATE b = b + 1;
```

The stateful acceptance has three independent controls. It uses an actual
`NOT NULL` constraint, never division by zero, a default cast failure, or a
scheduler-dependent expression. The baseline and implementation do not run
the same SQL text: the baseline cannot parse row aliases, so it supplies the
transaction/error envelope with the old `VALUES(v)` form, while the
implementation head supplies the row-alias syntax oracle. These are compared
as corresponding contracts, not claimed as byte-for-byte SQL parity.

For the transaction/error envelope, run each form against a fresh copy of the
same seed with `sql_mode = 'STRICT_TRANS_TABLES'` and an explicitly recorded
session autocommit mode. The status oracle is the MySQL protocol's
`SERVER_STATUS_IN_TRANS` bit (`0x0001`) and `SERVER_STATUS_AUTOCOMMIT` bit
(`0x0002`). Record both bits from the status-bearing OK packet after the
transaction begins and from the successful no-side-effect query after the
failed statement; an ERR packet itself has no status field.

```sql
SET sql_mode = 'STRICT_TRANS_TABLES';
SET autocommit = 1;
DROP TABLE IF EXISTS odku_stateful_v1;
CREATE TABLE odku_stateful_v1 (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO odku_stateful_v1 VALUES (1, 10);
START TRANSACTION;
INSERT INTO odku_stateful_v1 VALUES (9, 90); -- transaction marker
-- Run exactly one of the following statement forms per fresh seed.
-- Baseline only: the old syntax is the compatibility oracle.
INSERT INTO odku_stateful_v1 VALUES (1, NULL)
    ON DUPLICATE KEY UPDATE v = VALUES(v);
-- Reset the seed and run the implementation form instead; never run both in
-- one transaction.
INSERT INTO odku_stateful_v1 VALUES (1, NULL) AS n(id, v)
    ON DUPLICATE KEY UPDATE v = n.v;
-- Both forms must return the pinned constraint packet and no partial change.
SELECT id, v FROM odku_stateful_v1 ORDER BY id; -- assert id=1 and marker state
SELECT 1; -- inspect the following status-bearing OK/EOF packet
ROLLBACK;
SELECT 1; -- inspect status bits after rollback
SELECT id, v FROM odku_stateful_v1 ORDER BY id;
```

The same connection must show no partial row or value after the failed
statement, no implicit transaction commit, and a successful explicit
`ROLLBACK`. For the expected statement-rollback contract, the post-error
`SELECT 1` status packet retains `SERVER_STATUS_IN_TRANS` and the recorded
autocommit bit; the post-rollback packet clears `SERVER_STATUS_IN_TRANS` while
retaining the session autocommit bit. If an exact baseline control instead
proves whole-transaction abort, freeze the cleared in-transaction result and
match it at the implementation head. A result-set protocol that uses EOF must
inspect its status-bearing EOF packet; with deprecate-EOF it must inspect the
status-bearing OK packet. A second connection must observe the unchanged
committed state. If either server rejects `STRICT_TRANS_TABLES`, the
corresponding control is `NOT_RUN` rather than silently inheriting a default
mode. At the implementation head named by this design, the source error contract is
`moerr.ErrConstraintViolation` (internal code `20304`), MySQL errno `3819`
(`ER_CHECK_CONSTRAINT_VIOLATED`), SQLSTATE `HY000`, and
`constraint violation: Column 'v' cannot be null`. Each exact-head run must
assert that packet; another mapped error or message is a contract mismatch,
not permission to widen the oracle.

The prior-action/rollback property is a separate implementation-head test-only
control. It must build the row-alias plan for the same table and feed the
executor's action stream in two controlled batches: batch 1 contains only
`(id=2, v=20)`, and batch 2 contains only `(id=1, v=NULL)`. An action observer
or equivalent barrier must record `id=2` action completion before the harness
releases batch 2. The error must then be raised by the real `NOT NULL` action
check for batch 2. This is the evidence that an earlier candidate reached the
action path; it must not be inferred from the textual order of a multi-row
`VALUES` list. Before feeding batch 1, the harness starts an explicit
transaction and writes marker `(id=9, v=90)` in the same table. Immediately
after the batch-2 error and
before any explicit `ROLLBACK`, the same connection must assert that `id=2` is
not visible, that no commit occurred, and that the marker plus
`SERVER_STATUS_IN_TRANS`/`SERVER_STATUS_AUTOCOMMIT` bits from a subsequent
no-side-effect `SELECT 1` status-bearing OK/EOF packet match the frozen
transaction-failure contract. The ERR packet itself is not used for this
status assertion. The expected statement-rollback contract is marker still
visible, `SERVER_STATUS_IN_TRANS` still set, and the autocommit bit unchanged;
if an exact baseline control instead proves whole-transaction abort, the
marker-absent/cleared-in-transaction result must be frozen and matched at the
implementation head. Either outcome must be decided before rollback, never
hidden by it. A second connection must not see `id=2` or the marker before or
after rollback. The harness then performs `ROLLBACK`, checks the post-rollback
status bits with another `SELECT 1`, and checks the committed table again. This
observer control must not be presented as the baseline SQL parity run or as
proof that a normal BVT multi-row statement has a particular natural execution
order.

Prepared reuse is a separate implementation-head binary-protocol control. One
`COM_STMT_PREPARE` creates this statement and is retained for all three
executions:

```sql
INSERT INTO odku_stateful_v1 VALUES (?, ?) AS n(id, v)
    ON DUPLICATE KEY UPDATE v = n.v
```

The client explicitly negotiates `CLIENT_FOUND_ROWS` (`0x00000002`) for all
three executions, records the session mode, and executes `SET autocommit = 1`
before preparing.
Against a fresh seed and explicit `STRICT_TRANS_TABLES`, it binds both
parameters as signed `MYSQL_TYPE_LONG` (`INT`) values and executes `(1, 10)`,
`(1, NULL)`, and `(1, 12)` without changing the SQL text or preparing again.
With the seed row `(1, 10)`, the first duplicate is a no-op and must return
`affected_rows = 1` under `CLIENT_FOUND_ROWS`; the third changes `v` and must
return `affected_rows = 2`. These values are the pinned MatrixOne baseline
contract in `row_count.sql`, not an unverified generic MySQL assumption. The
middle round returns only the pinned ERR packet, not an OK count. The required
oracle is therefore OK(1) → the same strict `NOT NULL` ERR → OK(2), with the
final row equal to `(1, 12)`. If the exact baseline reports another count or
packet field, stop as a contract mismatch and update the design; do not
substitute a generic “expected fields” rule.

The client records the same statement ID, the two signed `MYSQL_TYPE_LONG`
parameter metadata entries with unsigned flags clear, `new_params_bound = 1`
only on the first execute and `0` on the next two, and NULL bitmaps `0x00`,
`0x02`, `0x00` for the three rounds. It records the exact ERR packet (errno,
SQLSTATE, and message) for the middle round. If the client does not negotiate
`CLIENT_FOUND_ROWS` or cannot bind the stated parameter metadata, the case is
`NOT_RUN` rather than silently accepting different affected-row counts. SQL
`PREPARE`/`EXECUTE` text commands may supplement this case but do not prove
binary statement-ID reuse or absence of an implicit reprepare.

## Rollout, mixed-version, rollback, and diagnosis

This revision deliberately changes acceptance of legacy target- or
candidate-correlated ODKU subqueries while adding row-alias syntax. It adds no
catalog field, persisted state, wire field, capability bit, or feature flag,
so the server cannot negotiate these SQL semantics with an older SQL-facing
planner. “No protocol change” therefore does not mean that a mixed-version SQL
route is behaviorally compatible.

The rollout contract is:

1. Freeze `legacy-odku-v1`, the SQL transaction/error envelope, the
   implementation-head action-observer control, and the prepared success →
   failure → success control before rollout. Capture the exact server build,
   SQL mode, route, parameter/protocol oracle, and table/transaction digests.
2. Before upgrading the first SQL-facing route, inventory clients that emit
   legacy target/candidate-correlated ODKU. Migrate them to a supported
   direct/`VALUES()` form, or explicitly isolate/pin them to an old route until
   migration is complete. They must not be sent to a new route and discover
   the deliberate rejection during the rolling window; blind retries are not
   an isolation strategy. This client gate and route pinning are deployment
   work outside this documentation PR.
3. Upgrade every SQL-facing parser/planner route that may receive this SQL
   class to the approved implementation head before any client sends row-alias
   syntax. A route that still runs the baseline parser is not admitted for the
   new syntax; a route that still runs baseline ODKU binding is not admitted
   for the changed legacy-correlated rejection.
4. During the rolling deployment, keep admitted clients on the frozen legacy
   corpus and direct-expression/`VALUES()` forms until all SQL-facing routes
   pass the same exact-head probes. Do not claim mixed-version acceptance
   merely because existing plan transport has no new field.
5. Migrate clients to row-alias syntax only after homogeneous SQL-facing
   admission is established. Clients that cannot migrate their legacy
   correlated form must remain isolated or handle the deterministic rejection
   on an explicitly compatible route.
6. To roll back, first stop clients from emitting row-alias syntax and drain
   those requests, then downgrade all SQL-facing planner routes as one
   compatibility unit and rerun `legacy-odku-v1`. An old route cannot accept
   the new syntax, and a mixed rollback cannot preserve the deliberate legacy
   rejection boundary. No catalog or wire rollback step is needed because this
   design adds no such state.

For diagnosis, the runner or production incident record must retain a
redacted/normalized statement shape, server build and SQL-facing route,
`sql_mode`, whether the statement used a row alias or a legacy correlated
reference, the exact error code/SQLSTATE/full text, the affected-row/OK or ERR
packet, and before/after table and transaction digests. A parser error for row
aliases identifies a baseline route. `ErrUnsupportedDML` code `20313`,
SQLSTATE `HY000`, with the exact retained-rejection text identifies the
intentional legacy boundary. A failure of a direct expression, `VALUES()`
case, or legacy uncorrelated case in `legacy-odku-v1` is a regression; a
failure that follows only one route is a mixed-version admission problem. The
same-connection follow-up and the stateful control above must distinguish a
statement rejection from partial mutation before any client retry or rollback
decision.

## Scope, ownership, and invariants

The parser owns syntax retention and formatting. The planner owns validation,
identity remapping, name resolution, the subquery gate, and the fail-closed
error. The existing dedup, ordered-assignment, row materialization, and
index-maintenance paths remain the execution owners. No new executor or wire
state is introduced.

For every candidate row:

- each legal row-alias reference resolves to one immutable target-column
  identity and the final incoming projection for that identity;
- generated columns remain in the identity mapping until their `DEFAULT`
  projection is materialized, and `row_alias.generated_col` reads that
  materialized value; only `DEFAULT` may write the generated target slot;
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
2. Planner tests for identity remapping, explicit and implicit column-list
   widths, generated-column `DEFAULT` slots, reading a materialized generated
   value through the row alias, rejection of non-`DEFAULT` generated writes,
   hidden columns, local shadowing, ambiguous names, direct no-key fallback,
   the pre-fallback subquery gate, and complete parameter traversal.
3. BVT controls for an empty keyed target, a duplicate keyed target, a no-key
   direct fallback, a no-key row-alias subquery rejection, direct aliases,
   explicit and implicit generated-column slots, `VALUES()` all-default input
   with an ordinary default column and with a generated column, reading the
   materialized generated value, `generated_col = DEFAULT`, rejection of
   non-`DEFAULT` generated writes, invalid casts, multi-row and nested
   subqueries, prepared repeated execution of the empty-tuple form, legacy
   correlated rejection, legacy uncorrelated baseline parity, and every
   rejected grammar shape. The exact frozen `legacy-odku-v1` corpus must run at
   both exact baseline and exact implementation head. The SQL stateful
   transaction/error envelope must run against fresh seeds at both heads, using
   old `VALUES(v)` syntax for the baseline and row-alias syntax only at the
   implementation head; it is a corresponding state/error oracle, not a claim
   that baseline parses the new SQL. The implementation must additionally
   provide the two-batch action observer/barrier control above to prove a prior
   candidate reached the action path, without inferring order from a `VALUES`
   list. Assert the pinned code, SQLSTATE, exact text, transaction/table state,
   rollback, and follow-up results for the SQL control, and the pre-rollback
   observer event plus post-rollback state for the test-only control.
4. A defect-control run at the exact implementation head
   `dd2b0b38f056ac56671a5d78c9de33b68b06ef07` showing the eager-build failure
   or premature subquery evaluation for the counterexample above. The current
   `main` baseline cannot parse row-alias syntax, so it must not be reported as
   an executable control for that query. The corrected exact head must show the
   new deterministic rejection and no mutation.
5. A legacy compatibility run that compares the pinned `legacy-odku-v1`
   corpus (direct expressions, `VALUES(column)`, uncorrelated scalar/`EXISTS`,
   no-key direct fallback, and the named legacy prepared cases) between the
   `main` compatibility baseline and the implementation head, including
   result/error/state, affected-row/OK or ERR packet, and the fixed SQL mode,
   not only a plan shape. Correlated cases are tested separately against the
   retained-rejection contract above, including keyed and no-key targets; they
   are not claimed as `main` parity. The SQL stateful envelope is compared as
   old `VALUES(v)` on baseline versus row-alias syntax on the implementation
   head, with the syntax difference recorded explicitly. The two-batch action
   observer is implementation-head-only. The prepared stateful control must
   use one binary `COM_STMT_PREPARE` and prove success → failure → success with
   changed parameters and explicit parameter/protocol oracles; SQL
   `PREPARE`/`EXECUTE` is supplemental only.
6. A rollout artifact that records homogeneous SQL-facing admission, the
   mixed-version route decision, client migration/rollback order, and the
   diagnosis fields above. It must identify unsupported/NOT_RUN gates instead
   of treating parser compatibility or absence of a wire change as proof of
   mixed-version acceptance.
7. Maintainer approval of this exact matrix and QA validation of the
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
