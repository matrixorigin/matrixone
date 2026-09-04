# JSON_VALUE RETURNING and response contract

Issue: #28037

## Contract

`JSON_VALUE` accepts a document and path, with optional `RETURNING`, `ON EMPTY`,
and `ON ERROR` clauses. MatrixOne keeps its existing extension that permits an
expression (including a prepared marker) as the path; a NULL path returns SQL
NULL and an invalid path remains a statement error. The MySQL compatible clause
form accepts the same expression path so existing MatrixOne statements are not
made invalid by the new grammar.

The default return type is nullable `VARCHAR(512)` in `utf8mb4_bin`. Supported
explicit targets are `CHAR`, `BINARY`, `SIGNED`, `UNSIGNED`, `DECIMAL`, `FLOAT`,
`DOUBLE`, `DATE`, `TIME`, `DATETIME`, `YEAR`, and `JSON`. `DEFAULT` values are
signed literals and are converted and validated while binding/preparing the
statement. A default NULL, expression, column, or parameter is rejected.

The extraction state is separated from response policy. A row can be SQL NULL,
path NULL, empty, a single JSON value, multiple values, a document parse error,
or a hard error. Empty rows use `ON EMPTY`; multiple matches and conversion or
document errors use `ON ERROR`. Invalid paths and excessive JSON depth are hard
errors. SQL NULL documents and JSON null values return SQL NULL directly.

For a single match, text targets return canonical JSON text (strings are
unquoted), `RETURNING JSON` preserves JSON values, and numeric/temporal targets
use strict conversion. A conversion that exceeds the target or loses data is an
`ON ERROR` event rather than a silently truncated value.

## Plan representation

The parser creates a dedicated JSON_VALUE expression carrying the document,
path, target type, and both response policies. Its formatter emits the default
target explicitly as `RETURNING CHAR(512)` and preserves explicit policies. The
binder lowers it to an internal seven-argument JSON_VALUE overload. The target
type is carried by the existing plan `Expr_T`; response modes are integer
constants and validated defaults are typed constants. The internal overload is
not reachable through the public grammar, and no protobuf change is required.

The executor performs extraction first and applies the selected policy per row.
It does not use a limiting ordinary cast for a document or prepared value. The
function remains nullable and handles policy arguments itself instead of relying
on generic strict-function NULL short-circuiting.

## Compatibility and rollout

Existing two-argument calls remain valid and now receive the MySQL default type
and collation. Existing persisted generated values are not automatically
recomputed. Upgrade validation covers views, generated columns, indexes, values
over the 512-character boundary, and the explicit rebuild procedure. New syntax
cannot be downgraded to a server that does not parse these clauses.

## Required evidence

Parser, AST, binder/type, executor, protocol, CTAS, VIEW, generated-column and
index tests must cover the state/policy matrix, prepared execution, warnings,
and invalid inputs. A single-CN BVT is run twice on a clean instance. The final
change requires `git diff --check`, `mo-self-review`, schema-v6 semantic
preflight, and exact-head CI.
