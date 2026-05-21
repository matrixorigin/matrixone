# Fix: ALTER TABLE AUTO_INCREMENT Review Findings

- **Issue**: #23143 follow-up fixes
- **Date**: 2026-05-20
- **Branch**: worktree-issue-23143-analysis
- **Status**: Approved

## Background

Code review of the AUTO_INCREMENT alter table implementation found:

1. **CRITICAL**: Copy path (`buildAlterTableCopy`) uses `option.Value - 1` without `!= 0` guard, causing uint64 underflow when `AUTO_INCREMENT = 0` (MySQL semantics: reset to default next value = 1).
2. **MEDIUM**: No validation for `ALTER TABLE t AUTO_INCREMENT = N` on a table without auto_increment columns — silent success diverges from MySQL which reports ERROR 1075.

## Design

### Fix 1: Guard copy path against value = 0

**File**: `pkg/sql/plan/build_alter_table.go`

Align `buildAlterTableCopy` with the inplace path and CREATE TABLE:

```go
// Before (uncommitted — underflow bug):
case *tree.TableOptionAutoIncrement:
    copyTableDef.AutoIncrOffset = option.Value - 1

// After:
case *tree.TableOptionAutoIncrement:
    if option.Value != 0 {
        copyTableDef.AutoIncrOffset = option.Value - 1
    }
```

This makes all three paths consistent:

| Path | Value=N | Value=0 |
|---|---|---|
| CREATE TABLE | N-1 | no-op (offset stays 0) |
| Inplace ALTER | N-1 | no-op (offset stays 0) |
| Copy ALTER | N-1 | no-op (offset stays 0) |

### Fix 2: Validate table has an auto_increment column

**Files**: `pkg/sql/plan/build_ddl.go`, `pkg/sql/plan/build_alter_table.go`

Add a helper and call it before processing `TableOptionAutoIncrement` in both paths:

```go
func tableHasAutoIncrementColumn(tableDef *plan.TableDef) bool {
    for _, col := range tableDef.Cols {
        if col.Typ.GetAutoIncrement() {
            return true
        }
    }
    return false
}
```

In both `case *tree.TableOptionAutoIncrement:` blocks, add:

```go
if !tableHasAutoIncrementColumn(tableDef) {
    return nil, moerr.NewInvalidInputf(ctx.GetContext(),
        "Incorrect table definition; there can be only one auto column and it must be defined as a key")
}
```

Error message matches MySQL.

### Test updates

**File**: `pkg/sql/plan/build_alter_table_test.go`

Add test cases to `TestAlterTableAutoIncrement`:
- `ALTER TABLE no_auto_table AUTO_INCREMENT = 100;` — expect error
- `ALTER TABLE dept AUTO_INCREMENT = 0;` — expect success (already present)

## Files changed

| File | Change |
|---|---|
| `pkg/sql/plan/build_alter_table.go` | Fix 1: guard + Fix 2: validation |
| `pkg/sql/plan/build_ddl.go` | Fix 2: validation + helper function |
| `pkg/sql/plan/build_alter_table_test.go` | Additional test cases |
