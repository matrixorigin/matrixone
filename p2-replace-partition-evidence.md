# P2 evidence: merged REPLACE scan missing partition dependency (PR #28477)

- Worktree: `/private/tmp/mo-p2-28477-replace-part-20261013-162340`
- Base SHA: `13219a885a692d620b7ab4191508d929a58992e8`
- Branch: `codex/p2-replace-partition-28477`
- Scope: P2 only — merged REPLACE scan must capture the old partition key for partitioned classic FULLTEXT maintenance. Not FULLTEXT2 / async FULLTEXT / ODKU.

## Repro status: REPRODUCED

### Root cause (file:line)

`pkg/sql/plan/bind_replace.go` — `appendDedupAndMultiUpdateNodesForBindReplace`,
merged-main-scan capture construction (`requiredOldCols`).

When `useMergedMainScan` is true (real PK, no multi-part index, no unique
secondary index), the planner merges the old-row lookup into the PK DEDUP JOIN
and fills old-col slots as NULL placeholders. Only these columns were captured:

- `catalog.Row_ID`
- `tableDef.Pkey.PkeyColName`
- non-serialized index-key columns

Partition-expression columns were **not** captured. Later, REPLACE builds

- `deleteKey = if(isnull(old_rowid), newKey, oldKey)`
- `insertKey = if(isnull(new_pk), oldKey, newKey)`
- irregular-index old route (`replaceOldRoutePos` via `buildPartitionRouteExprFromColumns`)

from `oldColName2Idx[payload]` (etc.). Those slots stayed NULL literals, so the
old-row delete could not route to the physical partition the old row lived in.

The non-merged path (`replaceDedupOldColList`) already collected partition
columns when `partitionedFulltext` — the merged path omitted the same dependency.

### Trigger conditions (all three mission shapes)

| Shape | How it hits the defect |
|---|---|
| no-UNIQUE schema | `docs_ft`-style table: real PK + FULLTEXT only, no unique secondary → `useMergedMainScan` |
| same-partition REPLACE | same PK, partition key unchanged → `deleteKey` still needs old key to drop old postings from that partition |
| cross-partition REPLACE | same PK, partition key moves → `deleteKey` must use the **old** key (not the new one) |

Existing BVT tables (`ft_part`, `ft_replace`, `ft_range`) all declare
`code int unique`, so they take the legacy non-merged path and missed this.

### Failing test output (before fix)

```
=== RUN   TestReplaceMergedScanCapturesPartitionDependency/same_partition_payload_stays
    bind_insert_irregular_odku_test.go:551:
        Error: Should be true
        Messages: merged REPLACE scan must capture the old partition key (payload);
                  capture names=[docs_ft.id docs_ft.__mo_rowid]
=== RUN   TestReplaceMergedScanCapturesPartitionDependency/cross_partition_payload_moves
    ... same failure ...
--- FAIL: TestReplaceMergedScanCapturesPartitionDependency
```

Capture list was exactly `[docs_ft.id, docs_ft.__mo_rowid]` — `payload` (the
partition key, non-PK) missing.

## Fix

`pkg/sql/plan/bind_replace.go` (merged-scan `requiredOldCols` block): when
`partitionedFulltext`, add every column referenced by the partition expressions
via the existing `collectPartitionExprColumnNames` helper — the same set the
legacy `replaceDedupOldColList` already captures.

Minimal, plan-only; no runtime/wire change. `OldColCaptureList` is already
consumed by `pkg/sql/compile/operator.go` and kept alive by
`pkg/sql/plan/query_builder.go` / `opt_misc.go` pruning.

## Tests

### Unit (owning package `pkg/sql/plan`)

`TestReplaceMergedScanCapturesPartitionDependency`
(`pkg/sql/plan/bind_insert_irregular_odku_test.go`):

- schema: `docs_ft` (PK + FULLTEXT, **no unique secondary**) partitioned by
  non-PK `payload`
- subtest `same_partition_payload_stays` — REPLACE keeps payload
- subtest `cross_partition_payload_moves` — REPLACE moves payload across the
  partition expr
- asserts: merged-scan capture path taken; capture names include
  `docs_ft.payload` **and** PK **and** Row_ID; parent `UpdateCtx.PartitionCols`
  has 2 entries; partition-key expressions reference captured placeholders

Results (after fix):

```
--- PASS: TestReplaceMergedScanCapturesPartitionDependency (0.01s)
    --- PASS: TestReplaceMergedScanCapturesPartitionDependency/same_partition_payload_stays (0.00s)
    --- PASS: TestReplaceMergedScanCapturesPartitionDependency/cross_partition_payload_moves (0.00s)
```

### Non-regression (same package)

```
--- PASS: TestReplaceCaptureListNarrowed          (non-partitioned still captures Row_ID+PK only)
--- PASS: TestReplaceCaptureList_NotEmittedWhenMergedScanDisabled
--- PASS: TestReplaceConflictLookupUsesEquiJoins
--- PASS: TestPartitionedFulltextConflictDMLKeepsOldAndNewPartitionKeys
--- PASS: TestPartitionedFulltextMaintenanceRebuildsWhenPartitionColumnChanges
--- PASS: TestPartitionedFulltextDMLShapesBuildRoutedMaintenance
--- PASS: TestPartitionedFulltextDeleteRoutesRegularHiddenIndexes
--- PASS: TestPartitionedFulltextKeyMovesKeepRegularIndexes
--- PASS: TestPartitionedFulltextMaintenanceUsesIndexOnlyMultiUpdate
--- PASS: TestPartitionedFulltextUpdateAutomaticGeneratedRoute
```

`./pkg/sql/colexec/multi_update` Partition* tests: `ok  2.012s`

### SQL / BVT

`test/distributed/cases/fulltext/fulltext_partition.sql` (+`.result`): new
`ft_nuk` table — real PK, **no unique secondary**, `partition by range(bucket)`
(non-PK). Covers:

1. same-partition REPLACE (`bucket` stays 1) — old term `alpha` must vanish
2. cross-partition REPLACE (`bucket` 1 → 20) — old term `gamma` must vanish,
   `delta` searchable, sibling row `beta` untouched

**Not executed against a live cluster in this session** (no BVT harness run).
Expected `.result` values are derived from the maintenance contract. Residual
risk: run `fulltext_partition` BVT before merge.

### Static

- `gofmt -l` on changed Go files: clean
- `go vet ./pkg/sql/plan/`: clean

## Residual risks

1. **SQL BVT not executed here** — `fulltext_partition` `.result` is reasoned,
   not cluster-verified. Must run before merge.
2. **Other mo-bot P2** (the second design/acceptance blocker on PR #28477) is
   **not in this scope** and remains open unless handled elsewhere.
3. Multi-column partition keys: this fix captures every partition-expr column
   (parity with the legacy path). `getPartitionColName` still drives
   `deleteKey`/`insertKey` from a single key name — pre-existing shape, unchanged.
4. Fake-PK + no-unique REPLACE is insert-only (no old-row route needed); this
   change does not alter that path.

## Delivery

- Candidate SHA: see branch `codex/p2-replace-partition-28477`
- Files changed:
  - `pkg/sql/plan/bind_replace.go` (fix)
  - `pkg/sql/plan/bind_insert_irregular_odku_test.go` (regression)
  - `test/distributed/cases/fulltext/fulltext_partition.sql`
  - `test/distributed/cases/fulltext/fulltext_partition.result`
- Writer stopped after this package. No push, no GitHub PR mutation.
