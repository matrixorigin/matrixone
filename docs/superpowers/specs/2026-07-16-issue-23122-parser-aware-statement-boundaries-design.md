# PR 24659 Parser-Aware Statement Boundaries Design

## Context

PR #24659 materializes rewrite and database-remap policy independently for each SQL statement. The current implementation uses `SplitSqlBySemicolon`, which treats every scanner-visible semicolon as a statement boundary. MatrixOne's MySQL grammar also accepts compound statements whose bodies contain semicolons, including `CREATE TASK ... AS BEGIN ... END`. For those inputs, policy materialization produces multiple fragments while parsing produces one AST, and `AddRewriteHints` rejects the mismatch with `parse hints bug`.

## Goal

Use the MySQL grammar's top-level statement boundaries for policy materialization and its direct consumers, while preserving existing behavior for ordinary multi-statement SQL, blank/comment-only fragments, trailing semicolons, and semicolons inside strings or comments.

## Non-Goals

- Do not implement a second compound-statement grammar in the scanner.
- Do not redesign rewrite/remap policy transport or `UserInput`.
- Do not change `ANALYZE`, `CHECK TABLE`, or `SHOW PROFILE` semantics addressed by earlier PR revisions.
- Do not change lexical splitting used by parse-error recording unless required for correctness.

## Design

### Parser-owned top-level boundaries

Add token-position metadata to the MySQL parser semantic value. The top-level `stmt_list ';' stmt` production records the byte offset of its separator in the lexer. Compound-body productions such as `stmt_list_return ';' block_type_stmt` do not record it. This makes the grammar, rather than keyword heuristics, the single authority on whether a semicolon ends a client statement.

Expose a focused parser helper that returns SQL fragments split only at the recorded top-level separators. It will retain the existing fragment contract: trim fragment edges, preserve empty/comment-only fragments, omit only the synthetic empty tail after a trailing semicolon, and return one empty fragment for empty SQL. ASTs created solely to derive boundaries are freed before return.

Keep `SplitSqlBySemicolon` unchanged for callers that intentionally need lexical behavior and for invalid-SQL recording. This limits compatibility risk.

### Direct consumers

Use the parser-aware helper only where positional alignment with parsed statements is required:

1. `rewriteSQL`, so one materialized policy is added per top-level statement.
2. `extractLeadingHints` / `AddRewriteHints`, so hint count follows the same boundaries as the AST list.
3. `extractRemapDbByStatement`, so each AST receives only its own remap.
4. `sqlForRecordByStatement`, so execution records remain aligned with computation wrappers. Refactor the existing record sanitizer so it can sanitize one parser-aware fragment without lexically splitting its compound body; masking behavior remains unchanged.

If parser-aware splitting returns a syntax error before the normal parse stage, propagate that parser error. There is no executable statement on that path, so rewrite policy cannot be bypassed.

## Alternatives Rejected

### Incrementally reparse lexical fragments

Accumulating semicolon fragments until parsing succeeds avoids grammar changes, but repeatedly parses prefixes, has quadratic worst-case behavior, and makes malformed-input behavior dependent on partial parse errors.

### Track compound depth in the scanner

Counting `BEGIN`, `CASE`, `IF`, `LOOP`, and related tokens is fast but duplicates grammar knowledge. It would need continual updates whenever compound syntax changes and risks recreating the same mismatch under a different construct.

## Testing

Follow red-green-refactor:

1. Add a parser test proving a compound `CREATE TASK ... BEGIN ...; ...; END` is one fragment and remains one fragment when followed by a trailing delimiter.
2. Add mixed-input coverage proving a compound statement followed by a normal top-level statement produces exactly two fragments.
3. Add the review reproducer: with a non-empty role/session rewrite policy, run `rewriteSQL -> Parse -> AddRewriteHints` for compound `CREATE TASK`; assert one AST, one materialized top-level policy, no policy inserted inside the body, and no `parse hints bug`.
4. Retain and rerun existing strings/comments/blank-fragment tests.
5. Cover per-statement remap extraction and SQL-record alignment for compound-plus-normal input.

Run parser generation after editing `mysql_sql.y`, then focused parser and frontend tests, package build/vet/test, and a dependent frontend regression test using the repository CGo environment.

## Decision Log

- Use grammar reductions as the boundary authority because the defect is a grammar/lexical-boundary mismatch.
- Keep the new API narrow and retain the old lexical splitter to avoid unrelated behavior changes.
- Accept the focused parser pass on rewrite-enabled boundary-sensitive paths; avoid adding request-level caches or a new policy-transport abstraction until profiling demonstrates a need.
