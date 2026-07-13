# Snapshot-aware ANALYZE column resolution

## Context

`ANALYZE TABLE` is implemented by rewriting each table entry into a derived
query containing one `approx_count_distinct` expression per analyzed column.
When the statement omits its column list, `resolveTableVisibleColumns` obtains
the visible columns from the catalog before building that query.

For a table reference carrying `{snapshot=...}`, `{timestamp=...}`, or another
timestamp hint, the derived query preserves the hint and normal query planning
reads the historical table. The catalog lookup currently calls
`TxnCompilerContext.Resolve` with a nil snapshot, however, so it obtains the
current table definition. After schema evolution this can make the generated
column list disagree with the table definition used to execute the query.

## Goals

- Resolve implicit ANALYZE columns from the same historical table version used
  by the derived query.
- Preserve the established semantics and validation of all timestamp hint
  forms supported by normal query planning.
- Keep explicit column lists and non-snapshot ANALYZE behavior unchanged.
- Add an end-to-end regression that fails if current-schema columns are used
  for a historical table.

## Design

Before resolving a table definition, `resolveTableVisibleColumns` will convert
`TableName.AtTsExpr` to a `plan.Snapshot` through the existing frontend
`resolveSnapshot` helper. That helper delegates to
`QueryBuilder.ResolveTsHint`, which is also the canonical path used by query
planning for timestamp parsing, named-snapshot lookup, tenant handling, and
timestamp validity checks.

The resulting snapshot will be passed to:

```go
tcc.Resolve(dbName, tblName, snapshot)
```

When no timestamp hint is present, `resolveSnapshot` returns nil and the
current behavior is preserved. Errors from snapshot parsing or resolution are
returned before the derived query is executed. The derived SQL continues to
contain the original table timestamp hint, so its execution remains on the
same logical snapshot while retaining the normal planner's validation and
authorization behavior.

No snapshot state will be stored in the session or mutated on the input AST.
Each ANALYZE table entry is resolved independently, which also preserves mixed
multi-table statements where entries use different hints.

## Alternatives considered

1. Build and inspect a `SELECT *` plan to obtain the planned historical
   `TableDef`. This would ensure alignment but introduces a second, heavier
   planning path solely for column discovery and complicates error handling.
2. Reject omitted column lists for timestamped tables. This is simpler but
   contradicts the accepted table-name grammar and the expected feature
   behavior.
3. Reimplement timestamp-hint parsing in the ANALYZE handler. This risks
   semantic drift for named snapshots, tenant snapshots, and timestamp
   validation, so the existing canonical resolver is preferred.

## Verification

The distributed ANALYZE case will add this sequence:

1. Create a table with data.
2. Create an account snapshot.
3. Add a column to the current table after the snapshot.
4. Run `ANALYZE TABLE table_name {snapshot = 'name'}` without a column list.
5. Run a marker query on the same connection and clean up the snapshot/table.

Before the fix, implicit expansion includes the newly added current-schema
column and the historical derived query fails because that column does not
exist in the snapshot. After the fix, only snapshot-visible columns are
analyzed and the marker query confirms protocol/session continuity.

Verification will include the focused frontend tests, the focused distributed
ANALYZE case against a locally built MatrixOne service, and the broader
frontend package build/vet/test checks used for the preceding review fixes.
