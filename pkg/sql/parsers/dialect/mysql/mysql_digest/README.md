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
- `_utf8mb4` string introducers use the `UNDERSCORE_CHARSET` digest token.
- a trailing semicolon remains part of `STATEMENT_DIGEST` input.
- optimizer hints follow MySQL's placement, quoting, and numeric-suffix rules.
- legacy `WITH ROLLUP` uses MySQL's synthetic digest token.
- `NULL` is reduced according to its expression or DDL grammar role.
- `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES` affect both normal and hint lexing.

Update the MySQL-oracle tests in `digest_test.go` whenever this code is synced
with upstream or the targeted MySQL version changes.
