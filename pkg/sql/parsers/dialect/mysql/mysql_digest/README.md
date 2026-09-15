# MySQL statement digest

This package is derived from
[`github.com/rashiq/mysql-digest`](https://github.com/rashiq/mysql-digest) at
commit `f3ced5263ce5897541876f8dab6ef061f7c7a9e5` (MIT license; see `LICENSE`).
It is kept internal to MatrixOne's MySQL dialect implementation so the token
stream used by `STATEMENT_DIGEST` can be reviewed and tested with the SQL
function that consumes it.

The MatrixOne copy intentionally targets MySQL 8.4 and carries compatibility
fixes beyond that upstream revision, including:

- `max_digest_length` limits the binary token buffer used by SHA-256.
- unary sign reduction uses MySQL 8.4's exact `m_start_expr` token set.
- Supported MySQL underscore character-set introducers (for example `_latin1`,
  `_utf8mb3`, `_utf8mb4`, and `_binary`) use the `UNDERSCORE_CHARSET` digest
  token; unknown underscore-prefixed names remain identifiers.
- a trailing semicolon remains part of `STATEMENT_DIGEST` input.
- optimizer hints follow MySQL's placement, quoting, and numeric-suffix rules.
- legacy `WITH ROLLUP` uses MySQL's synthetic digest token.
- `NULL` is reduced according to its expression or DDL grammar role.
- `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES` affect both normal and hint lexing.

## Why this is not a hash of the input string

`STATEMENT_DIGEST` is defined as SHA-256 over MySQL's *normalized token
stream*, not over the original SQL bytes. Raw-string hashing would give
different digests to statements that differ only in literal values (for
example, `SELECT 1` and `SELECT 2`) and to equivalent statements that differ
in comments, whitespace, quoting, or SQL-mode-dependent tokenization. It
would therefore violate the grouping and compatibility contract even though
the final operation is SHA-256.

Most of the package's roughly 8,000 lines are generated keyword/token tables,
the derived lexer, and their regression tests; they are not a second SQL
executor. The runtime path is a bounded, deterministic linear scan followed by
the existing parser's validity check. Keeping the token tables and lexer
local makes the MySQL 8.4 token contract explicit and testable without
coupling `STATEMENT_DIGEST` to MatrixOne's broader parser implementation.

Update the MySQL-oracle tests in `digest_test.go` whenever this code is synced
with upstream or the targeted MySQL version changes.

The feature-level compatibility, ownership, resource, and remote-rollout
contract is versioned in
[`docs/rfcs/20260909_mysql_statement_digest.md`](../../../../../../docs/rfcs/20260909_mysql_statement_digest.md).
