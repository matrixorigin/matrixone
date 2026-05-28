# Issue #23122 Analysis: ANALYZE/CHECK/SHOW PROFILE Parser Support

**Issue:** [GitHub #23122](https://github.com/matrixorigin/matrixone/issues/23122)
**Branch:** `fix/issue-23122-analyze-check-show-profile`
**Status:** Implementation mostly complete (see `git diff HEAD~1 --stat`)

---

## Summary

MatrixOne was missing parser-level support for three MySQL-compatible SQL statements:
1. `ANALYZE TABLE` with multiple tables (e.g., `ANALYZE TABLE t1(a,b), t2(c,d)`)
2. `CHECK TABLE` (all variants: plain, `EXTENDED`, `FOR UPGRADE`)
3. `SHOW PROFILE` (with optional `FOR QUERY <n>` and `LIMIT` clauses)

All three would previously return SQL parser error 1064 ("You have an error in your SQL syntax"). The root cause was purely a **parser grammar gap**: the Bison grammar (`mysql_sql.y`) lacked rules for these statements, and there were no corresponding AST node types in `pkg/sql/parsers/tree/`.

---

## Root Cause

### 1. ANALYZE TABLE -- Multi-table limitation

The original grammar rule allowed only a single table:

```yacc
analyze_stmt:
    ANALYZE TABLE table_name '(' column_list ')'
```

There was no production for a comma-separated list of tables. The corresponding AST node (`AnalyzeStmt`) held only a single `Table` and `Cols` field:

```go
type AnalyzeStmt struct {
    Table *TableName
    Cols  IdentifierList
}
```

### 2. CHECK TABLE -- No grammar rule at all

There was no `check_table_stmt` production in `normal_stmt`, so any `CHECK TABLE ...` input triggered the generic parser error. No `CheckTableStmt` AST node existed.

### 3. SHOW PROFILE -- No grammar rule at all

Similarly, no `show_profile_stmt` production existed. The `PROFILE` token was not even declared as a reserved keyword. No `ShowProfileStmt` AST node existed.

---

## Changes Made

### Parser Grammar (`pkg/sql/parsers/dialect/mysql/mysql_sql.y`)

- **Added `PROFILE` to tokens** and `non_reserved_keyword` list
- **Added grammar types**: `%type <analyzeTableEntries> analyze_table_list`, `%type <checkTableOption> check_table_option_opt`, `%type <int64Val> for_query_opt`
- **Added `check_table_stmt` and `show_profile_stmt` to `normal_stmt`** (alongside existing `analyze_stmt`)
- **Rewrote `analyze_stmt`** to accept `analyze_table_list` (comma-separated `table_name '(' column_list ')'` entries)
- **Added `check_table_stmt` rule**: `CHECK TABLE table_name_list check_table_option_opt` with options `EXTENDED` or `FOR UPGRADE`
- **Added `show_profile_stmt` rule**: `SHOW PROFILE for_query_opt limit_opt` with `FOR QUERY INTEGRAL` clause

### AST Nodes (`pkg/sql/parsers/tree/`)

- **`analyze.go`** -- Refactored `AnalyzeStmt` from single-table to `[]*AnalyzeTableEntry` (each entry has `Table` and `Cols`). Introduced `AnalyzeTableEntry` struct. Updated `Format()`, `NewAnalyzeStmt()`.
- **`check.go`** (new) -- Added `CheckTableStmt` with `Tables TableNames` and `Option CheckTableOption` (enum: `None`, `Extended`, `ForUpgrade`).
- **`show_profile.go`** (new) -- Added `ShowProfileStmt` with `ForQuery int64` and `Limit *Limit`.

### Frontend Handler (`pkg/frontend/mysql_cmd_executor.go`)

- **`handleAnalyzeStmt`** -- Rewritten to iterate over `stmt.Entries` and issue a `SELECT approx_count_distinct(...) FROM ...` query for each table entry individually, collecting errors.
- **`handleCheckTableStmt`** and **`handleShowProfileStmt`** -- New handler functions that return `moerr.NewNotSupported(...)` with clear messages ("CHECK TABLE is not supported", "SHOW PROFILE is not supported"). This is intentional: the parser now accepts the syntax, but execution semantics are deferred.

### Tests

- **`pkg/sql/parsers/dialect/mysql/mysql_sql_test.go`** -- Updated ANALYZE test cases for the new multi-table grammar
- **`pkg/sql/parsers/tree/issue_23122_test.go`** (new, 240 lines) -- Comprehensive unit tests for `AnalyzeStmt.Format()`, `CheckTableStmt.Format()`, `ShowProfileStmt.Format()`, edge cases (empty entries, zero/negative ForQuery), and metadata (`StmtKind()`, `GetStatementType()`, `GetQueryType()`)
- **`test/distributed/cases/issues/issue23122.sql`** (new, BVT) -- End-to-end integration tests: single-table ANALYZE, multi-table ANALYZE, CHECK TABLE variants (basic, EXTENDED, multi-table, FOR UPGRADE), SHOW PROFILE variants (basic, FOR QUERY, LIMIT, combined), regression for `SHOW PROFILES` (plural) and `CHECK` constraint in CREATE TABLE
- **`pkg/sql/parsers/tree/stmt_test.go`** -- Updated to include new statement types

---

## Files Changed

| File | Change Type | Description |
|------|------------|-------------|
| `pkg/sql/parsers/dialect/mysql/mysql_sql.y` | Modified (+62/-?) | Added CHECK TABLE, SHOW PROFILE grammar; rewrote ANALYZE for multi-table |
| `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go` | Modified (+50) | Updated parser tests for new grammar rules |
| `pkg/sql/parsers/tree/analyze.go` | Modified (+30/-?) | Refactored from single-table to multi-entry AnalyzeStmt |
| `pkg/sql/parsers/tree/check.go` | New (+56) | CheckTableStmt AST node and CheckTableOption enum |
| `pkg/sql/parsers/tree/show_profile.go` | New (+44) | ShowProfileStmt AST node |
| `pkg/sql/parsers/tree/issue_23122_test.go` | New (+240) | Comprehensive unit tests for new AST nodes |
| `pkg/sql/parsers/tree/stmt_test.go` | Modified (+2) | Updated for new statement types |
| `pkg/frontend/mysql_cmd_executor.go` | Modified (+53/-?) | Multi-table ANALYZE execution; CHECK/SHOW PROFILE not-supported handlers |
| `test/distributed/cases/issues/issue23122.sql` | New (+95) | BVT integration test suite |

---

## Design Decisions

1. **Multi-table ANALYZE compiles to sequential `SELECT approx_count_distinct` queries.** The frontend handler iterates over table entries and issues one query per table. Errors from any entry are tracked and the last error is returned.

2. **CHECK TABLE and SHOW PROFILE are parsed but not executed.** The handler functions return `moerr.NewNotSupported`, matching MySQL behavior where these statements have no direct MatrixOne equivalent. This provides a migration-friendly error instead of a syntax error.

3. **`PROFILE` is a non-reserved keyword.** Added to `non_reserved_keyword` so it does not break existing identifiers named `profile`.

4. **`for_query_opt` uses INTEGRAL token** to match MySQL's `FOR QUERY <integer>` syntax, where the query ID is an unsigned integer literal.

5. **`SHOW PROFILES` (plural) remains unaffected** because it was already handled as a separate `show_stmt` production (`PROFILES` was already a keyword). The BVT test includes a regression check for this.

---

## Verification

- Unit tests: `go test ./pkg/sql/parsers/tree/... -run "TestAnalyzeStmtFormat|TestCheckTableStmt|TestShowProfileStmt" -v`
- Parser tests: `go test ./pkg/sql/parsers/dialect/mysql/... -v`
- BVT: `test/distributed/cases/issues/issue23122.sql`
- Regression: `make ut` on affected packages, plus `SHOW PROFILES` and `CHECK` constraint in CREATE TABLE
