# ALTER TABLE AUTO_INCREMENT Code Review Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 3 remaining issues from code review: confusing error message wording, and mock SetOffset silently ignoring unknown tableID.

**Architecture:** Three small, independent fixes across 3 files. No new interfaces or data flow changes.

**Tech Stack:** Go, moerr package, gomock-generated mocks

---

### Task 1: Fix error message wording in inplace path

**Files:**
- Modify: `pkg/sql/plan/build_ddl.go` (line ~4422)

- [ ] **Step 1: Update error message**

Replace the misleading error message with one that accurately describes the condition and includes the table name.

**Current code:**
```go
if !tableHasAutoIncrementColumn(tableDef) {
    return nil, moerr.NewInvalidInputf(
        ctx.GetContext(),
        "Incorrect table definition; there can be only one auto column and it must be defined as a key")
}
```

**Replace with:**
```go
if !tableHasAutoIncrementColumn(tableDef) {
    return nil, moerr.NewInvalidInputf(
        ctx.GetContext(),
        "Table '%s' does not have an AUTO_INCREMENT column", tableDef.Name)
}
```

- [ ] **Step 2: Commit**

```bash
git add pkg/sql/plan/build_ddl.go
git commit -m "fix(plan): improve error message for ALTER TABLE AUTO_INCREMENT on table without auto column in inplace path"
```

---

### Task 2: Fix error message wording in COPY path

**Files:**
- Modify: `pkg/sql/plan/build_alter_table.go` (line ~208)

- [ ] **Step 1: Update error message**

Same change as Task 1, in the COPY path handler.

**Current code:**
```go
if !tableHasAutoIncrementColumn(tableDef) {
    return nil, moerr.NewInvalidInputf(ctx,
        "Incorrect table definition; there can be only one auto column and it must be defined as a key")
}
```

**Replace with:**
```go
if !tableHasAutoIncrementColumn(tableDef) {
    return nil, moerr.NewInvalidInputf(ctx,
        "Table '%s' does not have an AUTO_INCREMENT column", tableDef.Name)
}
```

- [ ] **Step 2: Commit**

```bash
git add pkg/sql/plan/build_alter_table.go
git commit -m "fix(plan): improve error message for ALTER TABLE AUTO_INCREMENT on table without auto column in copy path"
```

---

### Task 3: Fix mock SetOffset to return error for unknown tableID

**Files:**
- Modify: `pkg/vm/engine/test/mock_increament_service.go` (line ~208)

- [ ] **Step 1: Add fmt import**

Add `"fmt"` to the import block.

**Current imports:**
```go
import (
    "context"
    "sync"

    "github.com/matrixorigin/matrixone/pkg/container/types"
    ...
)
```

**Add `"fmt"` after `"context"`:**
```go
import (
    "context"
    "fmt"
    "sync"

    "github.com/matrixorigin/matrixone/pkg/container/types"
    ...
)
```

- [ ] **Step 2: Update SetOffset to return error for unknown tableID**

**Current code:**
```go
func (m *MockAutoIncrementService) SetOffset(
    ctx context.Context,
    tableID uint64,
    colName string,
    offset uint64,
    txn client.TxnOperator,
) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if counters, ok := m.counters[tableID]; ok {
        counters[colName] = offset
    }
    return nil
}
```

**Replace with:**
```go
func (m *MockAutoIncrementService) SetOffset(
    ctx context.Context,
    tableID uint64,
    colName string,
    offset uint64,
    txn client.TxnOperator,
) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    counters, ok := m.counters[tableID]
    if !ok {
        return fmt.Errorf("table %d not found in mock auto-increment counters", tableID)
    }
    counters[colName] = offset
    return nil
}
```

- [ ] **Step 3: Commit**

```bash
git add pkg/vm/engine/test/mock_increament_service.go
git commit -m "fix(test): return error in mock SetOffset when tableID not found"
```

---

### Task 4: Verify all changes compile and tests pass

**Files:** (none modified, verification only)

- [ ] **Step 1: Build check**

```bash
cd pkg/sql/plan && go build ./...
cd pkg/vm/engine/test && go build ./...
```

Expected: No compile errors.

- [ ] **Step 2: Run plan tests**

```bash
cd pkg/sql/plan && go test -run "TestAlterTableAutoIncrement" -v -count=1 ./...
```

Expected: `TestAlterTableAutoIncrement` PASS, `TestAlterTableAutoIncrementRejectNoAutoColumn` PASS.

- [ ] **Step 3: Run parser tests**

```bash
cd pkg/sql/parsers/dialect/mysql && go test -run "TestAlterTableAutoIncrement" -v -count=1 ./...
```

Expected: All sub-cases PASS.
