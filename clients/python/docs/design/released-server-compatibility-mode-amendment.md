# Release compatibility: preserve the transaction before DDL

Revision: `PR32671-RELEASE-CONTRACT-2026-11-13-r3`.
Amends the SQL-mode lookup in [r2](released-server-compatibility.md), for #32671.

The full immutable-release run found three new Stage failures. Both direct
`SELECT @@SESSION.sql_mode` and `SHOW VARIABLES LIKE 'sql_mode'` followed by
CREATE STAGE on a non-autocommit connection reproduce native20101/unclassified
statement in an uncommitted transaction. A read-only query is not transaction-
neutral on this release. Do not repair this by commit, rollback or changing
autocommit around the caller's work.

Remove SQL-mode database reads from the release listener entirely. For quote
inspection and numeric whitespace, consider the at most four combinations of
ANSI_QUOTES and NO_BACKSLASH_ESCAPES that affect this lexical scan. Use one
interpretation when its delimiters cannot occur. Reject a known feature only
when the interpretations agree on the limitation. Apply numeric normalization
only when they produce identical SQL. Ambiguous or malformed raw SQL retains
its native path; existing parameter rendering still owns its established
executing-connection SQL-mode handling. Explicit execute_driver bytes remain
unchanged. Supported parameterized/builder inputs use unambiguous literal
rendering. This removes rather than adds runtime state and network work.

The listener now performs zero SQL before dispatch. It cannot start a native
transaction, alter its snapshot, modify session flags or acquire a second
checkout. Work is at most a fixed four scans per classifier/normalizer, linear
time and temporary memory in SQL length, without a cache/worker/queue. Native
and caller transaction ownership and cancellation remain unchanged.

Validate the original three double-quoted Stage consumers unchanged, the exact
no-dispatch feature errors, ambiguous quote/comment controls, numeric values
and export bytes, existing transaction/pool/unhappy paths, and full release
online/analytics. Main and candidate branches do not select this profile; their
complete positive evidence remains reusable when their inputs are unchanged.
