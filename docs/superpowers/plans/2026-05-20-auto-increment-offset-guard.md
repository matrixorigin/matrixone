# Fix AUTO_INCREMENT Offset Never Decrease — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SetOffset` use "only increase, never decrease" semantics, matching MySQL's AUTO_INCREMENT behavior where the counter can never be set below existing values.

**Architecture:** Three independent `<` guard changes across three store implementations (memStore, sqlStore, mock). Each wraps `offset = newOffset` with `if current < newOffset`. Plan/compile/proto layers unchanged.

**Tech Stack:** Go, incrservice, fmt.Sprintf

---

### Task 1: Guard SetOffset in memStore against lowering the offset

**Files:**
- Modify: `pkg/incrservice/store_mem.go:197`

- [ ] **Step 1: Add `<` guard to memStore SetOffset**

Replace unconditional assignment with guarded update:

```go
// line 197, replace:
cols[i].Offset = offset

// with:
if cols[i].Offset < offset {
    cols[i].Offset = offset
}
```

- [ ] **Step 2: Verify compilation**

```bash
cd pkg/incrservice && go build ./...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add pkg/incrservice/store_mem.go
git commit -m "fix(incrservice): guard SetOffset in memStore against lowering the offset"
```

---

### Task 2: Guard SetOffset in sqlStore against lowering the offset

**Files:**
- Modify: `pkg/incrservice/store_sql.go:318-320`

- [ ] **Step 1: Add `and offset < %d` to SQL WHERE clause**

```go
// lines 318-320, replace:
fmt.Sprintf(
    "update %s set offset = %d where table_id = %d and col_name = '%s'",
    incrTableName, offset, tableID, colName,
)

// with:
fmt.Sprintf(
    "update %s set offset = %d where table_id = %d and col_name = '%s' and offset < %d",
    incrTableName, offset, tableID, colName, offset,
)
```

When current offset >= new offset, the UPDATE affects 0 rows. The subsequent `Reload()` in `service.SetOffset` loads the unchanged value — a clean no-op.

- [ ] **Step 2: Verify compilation**

```bash
cd pkg/incrservice && go build ./...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add pkg/incrservice/store_sql.go
git commit -m "fix(incrservice): guard SetOffset in sqlStore against lowering the offset"
```

---

### Task 3: Guard SetOffset in mock against lowering the offset

**Files:**
- Modify: `pkg/vm/engine/test/mock_increament_service.go:215`

- [ ] **Step 1: Add `<` guard to mock SetOffset**

The mock currently has the `!ok` error check (committed in `33f9024e7`) but unconditionally sets the offset. Add the guard:

```go
// line 215, replace:
counters[colName] = offset

// with:
if current, ok := counters[colName]; !ok || current < offset {
    counters[colName] = offset
}
```

The `!ok` branch handles first-time initialization of the counter for a column. The `current < offset` guards against lowering.

- [ ] **Step 2: Verify compilation**

```bash
cd pkg/vm/engine/test && go build ./...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add pkg/vm/engine/test/mock_increament_service.go
git commit -m "fix(test): guard SetOffset in mock against lowering the offset"
```

---

### Task 4: Add plan-level regression test for low-value AUTO_INCREMENT

**Files:**
- Modify: `pkg/sql/plan/build_alter_table_test.go:640-648`

- [ ] **Step 1: Add `AUTO_INCREMENT = 5` test case**

The existing `TestAlterTableAutoIncrement` only tests values 1000 and 0. Add a third case for a value lower than existing data:

```go
// Inside TestAlterTableAutoIncrement, update sqls to:
sqls := []string{
    `ALTER TABLE constraint_test.dept AUTO_INCREMENT = 1000;`,
    `ALTER TABLE constraint_test.dept AUTO_INCREMENT = 0;`,
    `ALTER TABLE constraint_test.dept AUTO_INCREMENT = 5;`,
}
```

This verifies that the plan layer does not reject low AUTO_INCREMENT values (the guard is at the incrservice layer, not plan).

- [ ] **Step 2: Run plan tests**

```bash
cd pkg/sql/plan && go test -run "TestAlterTableAutoIncrement" -v -count=1 ./...
```
Expected: `TestAlterTableAutoIncrement` PASS, `TestAlterTableAutoIncrementRejectNoAutoColumn` PASS.

- [ ] **Step 3: Commit**

```bash
git add pkg/sql/plan/build_alter_table_test.go
git commit -m "test(plan): add AUTO_INCREMENT=5 regression test for low-value guard"
```

---

### Task 5: Full verification

**Files:** (none modified, verification only)

- [ ] **Step 1: Build all affected packages**

```bash
cd pkg/incrservice && go build ./...
cd pkg/vm/engine/test && go build ./...
cd pkg/sql/plan && go build ./...
cd pkg/sql/compile && go build ./...
```
Expected: no errors.

- [ ] **Step 2: Run full plan test suite**

```bash
cd pkg/sql/plan && go test -count=1 ./...
```
Expected: all tests pass (or at least no new failures).

- [ ] **Step 3: Run incrservice tests**

```bash
cd pkg/incrservice && go test -count=1 ./...
```
Expected: all tests pass.

- [ ] **Step 4: Commit any remaining changes**

```bash
git status
```
If there are remaining modified files that should be part of this fix, commit them.
