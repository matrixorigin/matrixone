# `STATEMENT_DIGEST_TEXT`

Implements [MatrixOne issue #23025](https://github.com/matrixorigin/matrixone/issues/23025): the MySQL-compatible `STATEMENT_DIGEST_TEXT(statement)` scalar function.

The function accepts one SQL string and returns normalized statement text. It parses one statement using the active MySQL SQL mode, replaces literal values with `?`, omits a terminal client semicolon, returns `NULL` for `NULL`, and respects `max_digest_length`. Invalid input returns a parse error. Parser details are exposed only for direct SQL string literals; expression and binary inputs use the generic digest parse error.

Normalization reuses MatrixOne's MySQL scanner and parser. The function is non-foldable because its result depends on session settings. Remote execution carries the initiating statement's `sql_mode` and `max_digest_length` snapshot so all fragments use the same normalization settings.

This PR implements only the SQL-visible `STATEMENT_DIGEST_TEXT` function, using MySQL 8.4 digest-text semantics. PR #27988 records a MatrixOne AST-format fingerprint for telemetry; it has a different consumer and versioning contract, and neither PR depends on the other's output. They remain separate because a MySQL-compatible SQL result should not be coupled to an internal telemetry fingerprint. Both touch the frontend command executor, so land #27990 first and then rebase #27988 to keep those edits additive. A future SQL digest-hash function should hash this normalizer's output, not reuse the telemetry fingerprint.

This short note records the scope and behavior; per maintainer direction, a separate independent design-approval gate is not required for this issue. The implementation adds no digest aggregation or persistent state. The distributed SQL case covers the documented example and core normalization behavior; focused tests cover settings, errors, and remote-setting compatibility.
