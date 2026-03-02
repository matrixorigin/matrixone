# MySQL JSON `->` and `->>` Operators

- Status: in-progress
- Start Date: 2026-02-08
- Authors:
- Implementation PR:
- Issues: [#23006](https://github.com/matrixorigin/matrixone/issues/23006), [#23007](https://github.com/matrixorigin/matrixone/issues/23007)

# Summary

Implement MySQL-compatible **JSON column path operators** `->` and `->>` so that JSON path extraction can be written in the standard MySQL form (e.g. `col -> '$.key'`, `col ->> '$.key'`) in addition to the function form `JSON_EXTRACT` / `JSON_UNQUOTE(JSON_EXTRACT(...))`.

# Motivation

- **`->`**: `expr -> path` is equivalent to `JSON_EXTRACT(expr, path)`. Required for compatibility and for statements like:
  ```sql
  SELECT JSON_OBJECT('key', 'value') -> '$.key' AS result1;
  ```
- **`->>`**: `expr ->> path` is equivalent to `JSON_UNQUOTE(JSON_EXTRACT(expr, path))`, i.e. extract and unquote the value. Commonly used in `WHERE` and `ORDER BY` (e.g. `WHERE col ->> '$.id' = '1'`).

Without these, the parser reports a syntax error near `->` / `->>`.

# Technical Design

## Semantics (MySQL reference)

- **Left operand**: Any expression that yields a JSON value (column, `JSON_OBJECT()`, etc.).
- **Right operand**: JSON path string literal (e.g. `'$.key'`, `'$[0]'`).
- **`->`**: `expr -> path` ≡ `JSON_EXTRACT(expr, path)`.
- **`->>`**: `expr ->> path` ≡ `JSON_UNQUOTE(JSON_EXTRACT(expr, path))`.

## Current state

- **Parser**: Token **ARROW** already exists for `->` in the scanner; grammar does not use it. No token for `->>` yet.
- **Execution**: `json_extract` and `json_unquote` are already implemented; no new runtime logic is needed.

## Implementation

### 1. Lexer (`scanner.go`)

- **`->`**: Keep current behavior: on `-` followed by `>`, return **ARROW**.
- **`->>`**: When seeing `-` then `>`, consume `>`, then if the next character is `>`, consume it and return **LONG_ARROW**; otherwise return **ARROW**.

### 2. Grammar (`mysql_sql.y`)

- Declare token: `%token <str> ARROW` (existing), add `%token <str> LONG_ARROW`.
- Precedence: Add `ARROW` and `LONG_ARROW` to the same level as `SHIFT_LEFT` / `SHIFT_RIGHT` (e.g. `%left <str> SHIFT_LEFT SHIFT_RIGHT ARROW LONG_ARROW`).
- **`->`**: Add production `bit_expr ARROW simple_expr %prec ARROW` with action building `FuncExpr("json_extract", [$1, $3])`.
- **`->>`**: Add production `bit_expr LONG_ARROW simple_expr %prec LONG_ARROW` with action building `FuncExpr("json_unquote", [ FuncExpr("json_extract", [$1, $3]) ])`.

### 3. Binding / execution

No changes. The generated `FuncExpr` trees are the same as hand-written `json_extract` / `json_unquote(json_extract(...))` and are handled by existing resolution and execution.

### 4. Tests

- Parser tests: Add cases in `mysql_sql_test.go` for `SELECT ... -> '$.key'` and `SELECT ... ->> '$.key'`, and optionally chain/nested usage.
- BVT: Add or extend JSON cases to assert result equality with MySQL for both operators.

# Drawbacks

None significant; this is a parser-only extension reusing existing functions.

# Rationale / Alternatives

- **Alternative**: Introduce dedicated AST nodes (e.g. `JsonPathExpr`) and format as `->` / `->>`. Deferred to keep the first implementation minimal; the current approach (rewriting to `FuncExpr`) is sufficient for correctness and compatibility.

# Unresolved Questions

None.
