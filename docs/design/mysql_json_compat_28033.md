# MySQL JSON compatibility for issue #28033

## Scope and delivery

This document proposes the contracts for JSON_MERGE, JSON_DEPTH,
JSON_ARRAY_INSERT, and JSON_SEARCH, using MySQL 8.0.45 as the oracle.
The design was approved before the JSON_MERGE implementation was added to this
PR. Four partial implementation PRs merge in the order MERGE, DEPTH,
ARRAY_INSERT, SEARCH. Each subsequent PR starts from main after its predecessor
merges; unpublished local prototypes are not accepted validation evidence for
those future heads.

JSON_MERGE implementation waits for #28090, #28091, and #28096 to merge,
because they modify shared function registration and binder code.
JSON_ARRAY_INSERT additionally waits for #28092 followed by #28093 and uses
the latter's final jsonvalue.FromVector constructor conversion. #28094 is
not a prerequisite. Rescan every open function_id.go and list_builtIn.go
writer before assigning any new function ID; do not reserve IDs now.

## JSON_MERGE

Register json_merge as an alias of JSON_MERGE_PRESERVE without a new function
ID or executor. Preserve its arity, result, NULL and invalid-input behavior.
After successfully binding the original alias name, append warning 1287 via
an optional session AppendWarningDiagnostic(code uint16, msg string) sink.
Absence of a session sink is safe. The exact warning text is:

> 'JSON_MERGE' is deprecated and will be removed in a future release. Please use JSON_MERGE_PRESERVE/JSON_MERGE_PATCH instead

Emit once per syntactic call site, never per row or batch. Two call sites
produce two warnings. The binder carries an explicit warning origin through
the plan-building path: a direct statement and a user's initial PREPARE use
the session diagnostic sink, while an internal `rebuildPreparePlan` during
EXECUTE (for schema, SQL-mode or protocol invalidation) suppresses the
duplicate deprecation diagnostic. Rebuilding the executable plan is still
required; only the warning side effect is suppressed. A later user PREPARE is
a new bind lifecycle and warns once per call site. The frontend owns that
lifecycle at the top-level statement boundary: CTAS source metadata and
privilege planning, plus definition-change retry planning, reuse the same
call-site set. Ordinary COM_QUERY statements containing JSON_MERGE are
deliberately excluded from the plan cache, so each new statement binds and
restores its warning instead of reusing a diagnostic-free cached plan. CREATE
VIEW warns at creation; stored-view expansion and any internal plan rebuild
while consuming that view suppress the warning. Changing SHOW CREATE VIEW or
persisted SQL spelling is outside this issue.

## JSON_DEPTH

Accept JSON, MySQL string transport types and T_any for NULL/prepared inputs.
Reject numeric, boolean and ordinary binary documents rather than silently
casting them to varchar. SQL NULL returns SQL NULL; malformed JSON and
invalid argument types use existing MatrixOne error conventions.

Return int64. Scalars and empty containers have depth 1. Nonempty containers
have depth 1 + max(child depth). A dedicated ByteJSON traversal takes O(nodes)
time and O(nesting depth) auxiliary space and respects existing nesting limits.
Keep SQL implementation in a dedicated file.

## JSON_ARRAY_INSERT

Accept a document followed by path/value pairs. Parse and type-check every
pair for a row before modification; apply pairs left-to-right. SQL NULL in
document or path returns SQL NULL; a SQL NULL value inserts JSON null.
Use the unified constructor conversion for typed values, not a second local
conversion implementation.

Add dedicated JsonModifyArrayInsert behavior: JSON_INSERT is unsuitable
because it does nothing at existing targets. Permit simple paths ending in
an array index, including last/last-N. Reject root, object-member terminals,
wildcards, recursive descent and ranges. Missing parents or non-array parents
leave the document unchanged. Insert before an existing target. A forward
(nonnegative) index beyond the current array length appends at the tail; a
reverse `last-N` index that crosses before the array head clamps to index 0
and inserts at the head. For example,
`JSON_ARRAY_INSERT('[1,2]', '$[last-5]', 9)` returns `[9,1,2]`; an empty array
does not distinguish this rule from tail insertion, so the test must also use
a nonempty array. Later pairs resolve indices against earlier modifications.
Continue enforcing document size and depth limits.

## JSON_SEARCH

Signature: JSON_SEARCH(doc, one_or_all, search_str [, escape [, path ...]]).
Search string values only, never keys, numbers, booleans or JSON null.
Mode one/all is case-insensitive; other modes are errors. Required SQL NULL
arguments and SQL NULL path filters return SQL NULL. No match returns SQL
NULL. One match returns a JSON string location; multiple all matches return
a JSON array of string locations.

Reuse the LIKE regexp converter with case-sensitive matching, percent and
underscore wildcards. In the normal SQL mode, an omitted escape uses
backslash, an explicit SQL NULL also selects backslash, an explicit empty
escape disables escaping, and one Unicode rune is accepted as a custom
escape. With `NO_BACKSLASH_ESCAPES`, an omitted escape has no escape
character, an explicit SQL NULL still selects backslash, an explicit empty
escape is rejected with `ER_WRONG_ARGUMENTS`, and a one-rune custom escape is
validated and applied explicitly. Reject multi-rune escapes. For a prepared
statement, the binder receives the SQL mode for the bind or rebuild event and
resolves omitted/NULL/empty/custom escape there; a mode-invalidated internal
reprepare rebuilds the pattern under the current mode rather than reusing
stale escape semantics. Binder must prove an explicitly supplied escape
constant; columns, subqueries and ParamRef are rejected during prepare. Path
filters can be dynamic or prepared values and support wildcard, recursive
descent and range.

A ByteJSON traversal accepts filters, a string-matching callback and
stopAfterFirst, and returns actual complete locations. Deduplicate overlapping
filters by location. Render simple keys as $.a and quoted keys as
$."one potato", reusing existing identifier rules without modifying general
Path.String() or relying on queryWithSubPath. one short-circuits; all follows
deterministic document traversal with bounded auxiliary memory and no full
document copy. JSON_KEYS wildcard restrictions in #28041 are outside scope.

## Validation and acceptance

For every implementation PR, record base FAIL and exact PR-head PASS,
enumerate selected tests, run focused and package UT, and execute canonical
BVT twice on an owned ready instance with resource cleanup. Use mo-cgo-test
for packages with direct or transitive CGo dependencies. No sleep or skip
workarounds; preserve exact expected errors.

- MERGE: alias equivalence, three arguments, NULL, invalid JSON, arity,
  warning sink absence, scans, two call sites, initial PREPARE, CTAS and
  ordinary plan-cache bypass, schema-change reprepare followed by
  EXECUTE/SHOW WARNINGS, compile-retry planning, and views.
  Extend func_json_merge.test/.result, including exact warning 1287.
- DEPTH: scalars, empty/nonempty and mixed containers, maximum depth,
  over-limit JSON, malformed documents, NULL and rejected types.
  Add func_json_depth.sql/.result.
- ARRAY_INSERT: empty/middle/boundary insertion, last/last-N, forward
  out-of-range tail append, nonempty reverse underflow head insertion,
  missing/non-array parents, pair shifts, NULL values, invalid paths and
  document limits. Add func_json_array_insert.sql/.result.
- SEARCH: one/all, no matches, nested strings, case, and the matrix of
  normal/`NO_BACKSLASH_ESCAPES` modes crossed with omitted, SQL NULL, empty
  and custom escapes. Include the exact `ER_WRONG_ARGUMENTS` empty-escape
  error under `NO_BACKSLASH_ESCAPES`, prepared mode-change/reprepare and
  constant restrictions, overlapping filters, real locations, special keys,
  Chinese and emoji. Add func_json_search.sql/.result. Compare all results
  across databases by length and membership; pin local traversal order in UT.

Implementation revisions require full diff review, relevant tests, preflight,
self-review, git diff --check and matching local/remote/PR SHAs before Ready.
QA is required for the implementation series. After all four merge, run a
joint smoke on exact main and hand off to an identified tester using MySQL
8.0.45. Keep issue #28033 open until explicit QA PASS/FAIL includes MatrixOne
version and environment; issue closure and branch cleanup are not authorized
by this design.
