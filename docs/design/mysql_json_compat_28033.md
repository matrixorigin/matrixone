# MySQL JSON compatibility for issue #28033

## Scope and delivery

This document proposes the contracts for JSON_MERGE, JSON_DEPTH,
JSON_ARRAY_INSERT, and JSON_SEARCH, using MySQL 8.0.45 as the oracle.
This initial PR revision is design-only. Explicit design approval precedes
adding JSON_MERGE implementation to the same PR. Four partial implementation
PRs merge in the order MERGE, DEPTH, ARRAY_INSERT, SEARCH. Each subsequent PR
starts from main after its predecessor merges; unpublished local prototypes
are not accepted validation evidence for those future heads.

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
produce two warnings. Prepared statements warn during prepare/bind, not
subsequent execute. CREATE VIEW warns at creation; consuming a stored view
does not warn again. Changing SHOW CREATE VIEW or persisted SQL spelling is
outside this issue.

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
leave the document unchanged. Insert before an existing target; out-of-range
indices append. Later pairs resolve indices against earlier modifications.
Continue enforcing document size and depth limits.

## JSON_SEARCH

Signature: JSON_SEARCH(doc, one_or_all, search_str [, escape [, path ...]]).
Search string values only, never keys, numbers, booleans or JSON null.
Mode one/all is case-insensitive; other modes are errors. Required SQL NULL
arguments and SQL NULL path filters return SQL NULL. No match returns SQL
NULL. One match returns a JSON string location; multiple all matches return
a JSON array of string locations.

Reuse the LIKE regexp converter with case-sensitive matching, percent and
underscore wildcards, default backslash, one Unicode rune custom escape, or
empty escape. SQL NULL escape selects default backslash. Reject multi-rune
escape. Binder must prove an explicitly supplied escape constant; columns,
subqueries and ParamRef are rejected during prepare. Path filters can be
dynamic or prepared values and support wildcard, recursive descent and range.

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
  warning sink absence, scans, two call sites, prepare/execute and views.
  Extend func_json_merge.test/.result, including exact warning 1287.
- DEPTH: scalars, empty/nonempty and mixed containers, maximum depth,
  over-limit JSON, malformed documents, NULL and rejected types.
  Add func_json_depth.sql/.result.
- ARRAY_INSERT: empty/middle/boundary insertion, last/last-N, out-of-range,
  missing/non-array parents, pair shifts, NULL values, invalid paths and
  document limits. Add func_json_array_insert.sql/.result.
- SEARCH: one/all, no matches, nested strings, case, LIKE/escape variants,
  prepared restrictions, overlapping filters, real locations, special keys,
  Chinese and emoji. Add func_json_search.sql/.result. Compare all results
  across databases by length and membership; pin local traversal order in UT.

Implementation revisions require full diff review, relevant tests, preflight,
self-review, git diff --check and matching local/remote/PR SHAs before Ready.
The design-only revision has no runtime behavior to exercise in BVT.
QA is required for the implementation series. After all four merge, run a
joint smoke on exact main and hand off to an identified tester using MySQL
8.0.45. Keep issue #28033 open until explicit QA PASS/FAIL includes MatrixOne
version and environment; issue closure and branch cleanup are not authorized
by this design.
