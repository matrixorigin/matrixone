# Fix Design: Issue #23122 — ANALYZE/CHECK/SHOW PROFILE Parser Support

## Summary

Add parser-level support for three MySQL administrative statements. Most implementation is already complete (grammar rules, AST nodes, frontend handlers, tests). The remaining work is wiring the new handlers into the dispatch switch, adding FPrint constants, and regenerating the parser.

## Current State (Already Done)

### Files Created
- `pkg/sql/parsers/tree/check.go` — CheckTableStmt AST node
- `pkg/sql/parsers/tree/show_profile.go` — ShowProfileStmt AST node
- `pkg/sql/parsers/tree/issue_23122_test.go` — Unit tests
- `test/distributed/cases/issues/issue23122.sql` — BVT test

### Files Modified
- `pkg/sql/parsers/dialect/mysql/mysql_sql.y` — Grammar rules for all 3 statements
- `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go` — Parser round-trip tests
- `pkg/sql/parsers/tree/analyze.go` — Refactored for multi-table entries
- `pkg/sql/parsers/tree/stmt_test.go` — QueryType assertions for new stmts
- `pkg/frontend/mysql_cmd_executor.go` — handleAnalyzeStmt (multi-table), handleCheckTableStmt, handleShowProfileStmt

## Remaining Tasks

### Task 1: Wire up handlers in self_handle.go dispatch

The `handleCheckTableStmt` and `handleShowProfileStmt` functions are defined but never called. Need to add `case` branches in `self_handle.go` after the existing `AnalyzeStmt` case (~line 209).

### Task 2: Add FPrint constants

New FPrint constants needed in `pkg/frontend/types.go` for the new handler entry/exit points: `FPCheckTableStmt`, `FPHandleCheckTableStmt`, `FPShowProfileStmt`, `FPHandleShowProfileStmt`.

### Task 3: Regenerate mysql_sql.go

`pkg/sql/parsers/dialect/mysql/mysql_sql.go` is generated from `mysql_sql.y`. Must regenerate to apply the grammar changes.

### Task 4: Verify build and tests

Ensure the parser regenerates correctly and all tests pass.

## Design Decisions

- **CHECK TABLE / SHOW PROFILE → "not supported" error**: These features require significant engine work (table integrity checks, query profiling infrastructure). Returning a clear `moerr.NewNotSupported()` is better than a generic syntax error — migration tools get explicit feature-gap signals.
- **ANALYZE TABLE multi-table**: Iterates entries sequentially, rewriting each to `SELECT approx_count_distinct(col)...` and executing via `doComQuery`. Errors are collected; only the last error is returned.
- **`PROFILE` as non-reserved keyword**: Added to `non_reserved_keyword` list so `SHOW PROFILES` (plural) continues to work and `PROFILE` can be used as an identifier.
