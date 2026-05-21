# Fix: AUTO_INCREMENT Offset Never Decrease (MySQL-compatible behavior)

- **Issue**: #23143 follow-up - `ALTER TABLE AUTO_INCREMENT = 0` on populated tables
- **Date**: 2026-05-20
- **Branch**: worktree-issue-23143-analysis

## Background

Code review found: `ALTER TABLE AUTO_INCREMENT = 0` on a table with existing auto values (e.g., max=500) would reset the offset to 0, causing the next INSERT to produce id=1 — a duplicate key conflict.

**MySQL behavior**: The auto-increment counter can never be set lower than existing column values. `AUTO_INCREMENT = 0` on a populated table is a no-op (counter stays at max+1). On an empty table it resets to 1.

**Root cause**: `SetOffset` unconditionally overwrites `c.Offset`, while the existing `UpdateMinValue` already implements the correct `MAX(current, new)` semantics.

## Design

### Core change: SetOffset uses UpdateMinValue semantics

Instead of unconditionally setting the offset, each `SetOffset` implementation only updates the offset if the new value is **greater** than the current one.

### Changes by file

#### 1. `pkg/incrservice/store_mem.go` — guard with `<` check

```
// Before:
cols[i].Offset = offset

// After:
if cols[i].Offset < offset {
    cols[i].Offset = offset
}
```

#### 2. `pkg/incrservice/store_sql.go` — add `and offset < %d` to WHERE

```
// Before:
"update %s set offset = %d where table_id = %d and col_name = '%s'"

// After:
"update %s set offset = %d where table_id = %d and col_name = '%s' and offset < %d"
```

If the current stored offset is already >= the new value, the UPDATE affects 0 rows. The subsequent `Reload()` in `service.SetOffset` loads the unchanged offset — a clean no-op.

#### 3. `pkg/vm/engine/test/mock_increament_service.go` — guard with `<` check

```
// Before:
counters[colName] = offset

// After:
if current, ok := counters[colName]; !ok || current < offset {
    counters[colName] = offset
}
```

This is already committed in `33f9024e7`, but should be verified consistent.

#### 4. `pkg/frontend/test/incrservice_mock.go` and `pkg/frontend/test/mock_incr/types.go`

These are gomock-generated mocks for `SetOffset`. The mock itself delegates to `ctrl.Call` — no logic change needed. The real mock behavior is defined in test expectations.

### Behavior matrix

| Scenario | Current offset | New offset (value-1) | Action | Next value |
|---|---|---|---|---|
| `AUTO_INCREMENT=1000`, empty table | 0 | 999 | offset → 999 | 1000 |
| `AUTO_INCREMENT=1000`, max=500 | ~500 | 999 | offset → 999 | 1000 |
| `AUTO_INCREMENT=0`, empty table | 0 | 0 | no-op (0 < 0 = false) | 1 |
| `AUTO_INCREMENT=0`, max=500 | ~500 | 0 | no-op (500 < 0 = false) | 501 |
| `AUTO_INCREMENT=100`, max=500 | ~500 | 99 | no-op (500 < 99 = false) | 501 |
| `AUTO_INCREMENT=1000`, max=1500 | ~1500 | 999 | no-op (1500 < 999 = false) | 1501 |

### No changes needed

- `pkg/incrservice/service.go` — unchanged, delegates to store
- `pkg/incrservice/types.go` — interface unchanged
- `pkg/sql/compile/ddl.go` — caller unchanged
- `pkg/sql/plan/build_ddl.go` — plan layer unchanged
- `pkg/sql/plan/build_alter_table.go` — plan layer unchanged
- Proto files — unchanged

### Tests

Add a test case to `TestAlterTableAutoIncrement`:
- `ALTER TABLE dept AUTO_INCREMENT = 5` on a table with existing id values ≥ 5 → verify next INSERT produces max+1 (not 5)

Add a test to `TestAlterTableAutoIncrementRejectNoAutoColumn`:
- Already exists, verify it still passes

## Files changed

| File | Change |
|---|---|
| `pkg/incrservice/store_mem.go` | Guard `SetOffset` with `<` check |
| `pkg/incrservice/store_sql.go` | Add `and offset < %d` to WHERE |
| `pkg/vm/engine/test/mock_increament_service.go` | Already committed, verify |
| `pkg/sql/plan/build_alter_table_test.go` | Add test for low-value AUTO_INCREMENT |
