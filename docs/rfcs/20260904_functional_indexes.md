- Status: draft
- Start Date: 2026-09-04
- Authors: VioletQwQ-0
- Implementation PR: pending
- Issue for this RFC: #28053

# Functional Indexes on Deterministic Expressions

## Summary

Add MySQL-compatible functional indexes for deterministic scalar expressions on
ordinary persistent MatrixOne tables. The first implementation lowers each
functional key part to an owned hidden generated column and reuses the existing
generated-column write maintenance and regular secondary-index storage.

The first release supports one expression key part per non-unique ordinary
BTREE index. It supports inline `CREATE TABLE` definitions, standalone
`CREATE INDEX`, and `ALTER TABLE ADD INDEX`. Cluster tables, temporary tables,
special index algorithms, unique and composite functional indexes, prefixes,
directions, and multi-valued JSON expressions remain explicitly unsupported.

## Motivation

MatrixOne currently parses functional index syntax but rejects every expression
key part during plan construction. A MySQL application that indexes a JSON
extraction expression therefore has to add a visible generated column first,
which changes the schema and is not syntax-compatible with MySQL.

The target contract is represented by:

```sql
CREATE INDEX idx_kind ON docs ((CAST(doc ->> '$.kind' AS CHAR(20))));
```

This is a target syntax example, not an eligibility bypass. If the cast or
any dependency fails the session-invariance rules below, the first release
rejects the DDL instead of silently accepting a key whose value can vary by
session.

The index must be created and backfilled atomically, maintained after every
write, used by an identical equality predicate, and represented correctly by
`SHOW INDEX`, `SHOW CREATE TABLE`, clone, and checkpoint restore.

## Technical Design

### Supported contract and validation

The parser already produces `tree.KeyPart.Expr`. Planning will validate an
expression key part through the generated-column binder. It must be deterministic,
row-local, scalar, and reference only columns already present in the table. The
result type must satisfy the existing ordinary-index key type rules, including
width, scale, charset, and prefix restrictions.

The first release accepts exactly one expression key part in a non-unique
ordinary BTREE index. It rejects UNIQUE, primary, fulltext, spatial, master,
vector, composite or mixed key parts, multiple expressions, prefixes,
ASC/DESC, multi-valued JSON, raw JSON/TEXT/BLOB/DATALINK/geometry/vector
results, temporary tables, cluster tables, external tables, and expressions
containing variables, parameters, subqueries, aggregates, volatile functions,
or invalid generated-column dependencies. Rejections use a stable
`NotSupported`/`InvalidInput` class and never partially publish metadata.

### Session-invariant expression eligibility

For a persisted key, “deterministic” means that the same base row produces the
same key during backfill, every future writer, restore, and every eligible
query. The existing generated-column `checkExprForVolatileFunc` check is
necessary but not sufficient: an ordinary cast can be classified as foldable
while its runtime conversion still reads session state.

The functional-index binder therefore uses a deny-by-default eligibility
check, separate from the volatile-function check:

- Every operator and function must come from an explicit scalar allowlist whose
  result and error behavior do not read session state. Variables, parameters,
  subqueries, aggregates, volatile or real-time functions, and unknown
  functions are rejected.
- A referenced generated column is eligible only after recursively checking
  its complete dependency closure. Missing or cyclic metadata, or one
  session-dependent node anywhere in that closure, rejects the functional
  index; an already accepted generated column is not treated as proof of its
  dependencies.
- A cast is accepted only when its source and target type, width, scale,
  charset, collation, padding, rounding, overflow, and error behavior are
  fixed by the expression and persisted type metadata. Temporal conversions
  whose result depends on `time_zone` are rejected, including
  `CAST(ts AS CHAR(19))`. A cast that depends on `sql_mode`,
  `collation_connection`, `character_set_connection`, locale, or another
  session setting is rejected as well. If the implementation cannot prove a
  cast or function is session-invariant, it rejects it rather than guessing.
- The checker rejects any implicit coercion or collation choice whose result
  could change with session SQL mode, connection charset/collation, or
  timezone. It records the resolved result type and semantic attributes used
  by backfill, DML, restore, and optimizer matching; no ambient session value
  may be part of the persisted key contract.

The motivating JSON form remains eligible only when its extracted text and
target type resolve to those fixed semantics. If a `CHAR` default charset or
collation is session-derived, the implementation must reject that form or
require an explicitly fixed equivalent.

This conservative rule intentionally narrows the first release. The residual
predicate is a correctness confirmation only; it cannot recover a row that a
session-dependent key caused the index lookup to omit.

### Lowering and ownership

Each functional index owns one hidden VIRTUAL generated column. The physical
column name is `__mo_fi_` followed by the first 32 hexadecimal characters of
the SHA-256 of the normalized index name. The name is generated by the planner,
is never accepted as user syntax, and is checked against the table namespace.
The stored `GeneratedCol.Expr`, `OriginString`, `IsStored=false`, and resolved
type are authoritative for the expression and its output semantics.

`IndexDef.Parts` continues to store the hidden column name followed by the
existing primary-key alias. No new protobuf field or catalog-table migration
is required. A functional index is recognized only when its first part resolves
to a column with both `Hidden=true` and `GeneratedCol != nil`; this keeps the
metadata discriminator local and type-safe.

### Atomic DDL and lifecycle

Inline table creation appends the hidden generated column before building the
regular index definition. Standalone `CREATE INDEX` and `ALTER TABLE ADD INDEX`
use the copy-based atomic DDL primitive shared with #28052, so existing rows
are evaluated before the index is published. The transaction owns the copied
table, hidden index table, constraint, generated-column metadata, and
`mo_indexes` rows; any error, cancellation, or duplicate/invalid key removes
all temporary resources.

Backfill uses the same eligibility-bound expression and resolved type
attributes as later writes; it never captures the creating session's timezone,
SQL mode, or connection collation. Checkpoint/backup restore revalidates the
stored expression and dependency closure and fails closed if the metadata
needed to prove session invariance is missing or malformed.

Dropping a functional index removes its hidden index table, constraint,
`mo_indexes` rows, and the exclusively owned hidden generated column in the
same atomic schema transition. Dropping a base column referenced by the hidden
expression continues to use generated-column dependency validation.

`mo_indexes.column_name` stores the internal backing column and `hidden=1`
marks the functional row. The metadata insertion helpers use the same
functional-index detector. `SHOW INDEX` renders `Column_name=NULL` and
`Expression=<GeneratedCol.OriginString>` for these rows, while retaining the
existing output for ordinary indexes. `SHOW CREATE TABLE` and checkpoint
restore render `((expression))` and never expose the internal column name;
missing or malformed generated metadata is a hard restore error.

### DML and optimizer

INSERT, UPDATE, DELETE, REPLACE, LOAD, and non-unique ODKU continue through the
existing generated-column recomputation and ordinary index-maintenance paths.
No second expression evaluator or special index table is introduced.

For equality optimization, the planner normalizes relation tags and unwraps
one type-equivalent assignment-cast wrapper before comparing the persisted
generated expression with the bound predicate using stable function IDs,
typed literals, column positions, and charset. Both operand orders are
accepted only when the query expression independently passes the same
session-invariance check and its resolved type, charset, collation, and other
semantic attributes match the persisted expression. Algebraically equivalent
but structurally different or session-dependent expressions are not eligible.
A matched candidate uses an index-backfill join only and retains the original
expression as a residual predicate. The residual is not allowed to compensate
for a key that was evaluated under a different context. Covering/index-only
access is disabled for functional indexes in the first release. Missing or
malformed metadata fails closed to a normal table scan.

Functional-index creation is gated on the cluster's oldest-live protocol
capability. The implementation reserves MORPC capability version 48 (the
current main already uses 46 and 47) so a mixed-version CN cannot publish
metadata it cannot render or safely alter.

### DDL foundation from #28052

The generic copy-mode ALTER path must plan a generated column before a
dependent index, expose the evolving column map to subsequent actions, backfill
existing rows once, and publish the replacement table atomically. STORED and
VIRTUAL columns are covered. The same ownership/rollback primitive is reused
by functional-index creation and removal without exposing functional-index
syntax in the foundation PR.

### Validation plan for the implementation PR

The implementation must prove the invariant at the product boundary, not only
by inspecting the eligibility helper:

- DDL acceptance/rejection covers an eligible JSON extraction with fixed text
  semantics, `CAST(ts AS CHAR(19))`, session-dependent SQL-mode or collation
  conversions, and a generated-column dependency whose transitive expression
  is session-dependent. Rejected cases must publish no functional-index
  metadata.
- An index is backfilled by a writer at `+00:00`, then populated or updated by
  a writer at `+08:00`; queries from both timezones and from contrasting SQL
  modes/connection collations compare the index plan and result with a forced
  table-scan oracle. The same cross-context comparison covers INSERT, UPDATE,
  REPLACE, LOAD, and non-unique ODKU where supported.
- Backup/checkpoint restore is followed by the same cross-timezone and
  cross-session query-vs-table-scan oracle. Missing or incompatible persisted
  eligibility metadata must fail closed rather than produce an index lookup.
- Optimizer tests cover exact eligible expression matching, both operand
  orders, type-equivalent assignment casts, structurally different
  expressions, and a query whose session context is not eligible. Only the
  first group may select the functional index; all other groups must use the
  table-scan path and return the same rows as the oracle.

This RFC is documentation-only, so BVT is N/A here. The implementation PR
must add the corresponding production-entry BVT cases and result assertions;
unit tests for the eligibility helper alone are not sufficient.

## Drawbacks

The hidden generated value follows MatrixOne's current generated-column
materialization behavior, so functional indexes add a base-column value and
ordinary index storage. Standalone creation uses copy-mode DDL and can rewrite
the table. The first release deliberately trades broader MySQL syntax coverage
for reuse of proven write and index-maintenance code.

## Rationale / Alternatives

An expression directly embedded only in the hidden index table would require
duplicated expression evaluation in every DML path and would diverge from
generated-column dependency, type, and rollback handling. Adding a new
expression field to `IndexDef` would require a catalog/protobuf compatibility
migration and duplicate the expression already persisted in `mo_columns`.

The hidden generated-column lowering keeps one expression owner, preserves
existing index storage, and lets old ordinary DML continue to be correct after
the feature capability is enabled. The optimizer remains optional and fails
closed when exact semantic matching is unavailable.

## Unresolved Questions

The first release has no unresolved eligibility contract: it uses the
deny-by-default session-invariant rule above and rejects any expression whose
independence from timezone, SQL mode, charset, or collation cannot be proved.
Cluster-table support, UNIQUE functional indexes, composite expressions, range
predicates, covering scans, and virtual-only materialization are follow-up
work after separate design and QA evidence. A future release may define a
persisted evaluation context to broaden the accepted expression set.
