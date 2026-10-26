# MySQL SOUNDS LIKE compatibility

Status: **D1, proposed; design approval pending.**

Owning issue: [#23010](https://github.com/matrixorigin/matrixone/issues/23010).
Implementation PR: not created; implementation is blocked until this revision
receives a traceable design decision. This document covers the complete change.

## Problem and scope

At baseline `83c1e23526fc0ed31188abd7fc2ca87cd3a6d4b3`, MatrixOne rejects
`SELECT 'Hello' SOUNDS LIKE 'Hallo'` in its MySQL parser. SOUNDEX and equality
already exist. The missing owner is the MySQL predicate grammar, not the
phonetic algorithm or vector executor.

This feature changes the SQL compatibility contract. The repository's
`.agents/skills/mo-dev/references/feature-design-review.md` complexity gate
therefore applies despite the small implementation. No production implementation
is included in this design phase.

The SOUNDS LIKE subitem of [#24496](https://github.com/matrixorigin/matrixone/issues/24496)
shares this cause; its ROW constructor subitem is independent and remains open.
Do not change that issue's assignment or claim that this design resolves it in
full. Closed #22944, #28212 and #28912 are existing prerequisites, not new fixes.
Open [PR #30883](https://github.com/matrixorigin/matrixone/pull/30883), head
`620392eac7a93c3ca022e0aaa759a7316f04cc5c`, owns JSON operands for SOUNDEX
and other functions. This change must not copy, depend on, or alter that work.

## Contract and authoritative references

[MySQL 8.4 string functions](https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#operator_sounds-like)
define SOUNDS LIKE using equality of the two SOUNDEX results.
[MySQL 8.4 grammar](https://github.com/mysql/mysql-server/blob/8.4/sql/sql_yacc.yy)
places it in `predicate` with two `bit_expr` operands and treats SOUNDS as a
non-reserved keyword. The grammar, rather than a flat precedence list alone,
determines comparison nesting.

The invariant is: for every operand domain supported by existing SOUNDEX, the
new syntax must bind and execute exactly like the corresponding pair of
SOUNDEX calls compared with ordinary equality, with the operand grouping
defined by MySQL's grammar. A different result, error class, NULL state,
parameter order, or operand grouping disproves the invariant.

Required behavior:

- `a + b SOUNDS LIKE c * d` compares SOUNDEX of the arithmetic expressions.
- `a = b SOUNDS LIKE c` groups as `a = (SOUNDEX(b) = SOUNDEX(c))`.
- `a SOUNDS LIKE b = c SOUNDS LIKE d` compares the two phonetic predicates.
- `NOT a SOUNDS LIKE b`, AND/OR, CASE, WHERE, scalar subqueries, and explicitly
  parenthesized predicates retain their ordinary grammar and evaluation rules.
  `HIGH_NOT_PRECEDENCE` must retain its existing effect on the left operand.
- Any SQL NULL operand produces NULL, including two NULL operands; this is
  ordinary equality, not null-safe equality. Empty/nonalphabetic inputs retain
  the existing SOUNDEX behavior.
- CHAR/VARCHAR/TEXT, binary values, numeric coercion, collations, SQL EXECUTE
  parameters, and binary-protocol parameters use the current SOUNDEX binder.
  Explicit casts must stay attached to their original operands. No JSON type
  support or new coercion policy is introduced.
- SOUNDS remains usable as a column, table, qualified name and alias. In
  `SELECT sounds LIKE 'hello'`, the left operand is the identifier `sounds`
  and the operator remains ordinary LIKE.
- Reject missing operands, `a NOT SOUNDS LIKE b`, an ESCAPE suffix, and an
  unparenthesized SOUNDS LIKE chain. MySQL's grammar provides no such forms.

The public result uses MatrixOne's existing comparison result representation;
this change does not redefine all MySQL predicate metadata as integer metadata.

## Selected design and alternatives

Add a SOUNDS lexer token, accepted by `non_reserved_keyword`. Append its token
declaration to avoid renumbering existing generated token constants. Add only
the MySQL `predicate: bit_expr SOUNDS LIKE bit_expr` production. Construct two
ordinary `tree.FuncExpr` nodes named `soundex` and a `tree.ComparisonExpr` with
`tree.EQUAL`. Each operand occurs once. Regenerate `mysql_sql.go` using the
repository Makefile and require zero parser conflicts. If alias ambiguity needs
precedence annotation, constrain it locally and prove the identifier controls;
do not reserve SOUNDS or add a legacy keyword-policy exception.

Data flow is SQL -> keyword scanner -> predicate AST -> existing SOUNDEX binder
and parameter rebinder -> existing equality evaluator -> frontend result.
Formatting uses the existing function/comparison formatter, including necessary
right-comparison parentheses. Parsed view definitions can therefore render as
ordinary SOUNDEX/equality SQL. Verify this through format/reparse tests and
actual SHOW CREATE VIEW/export/replay consumers.

| Alternative | Assessment |
| --- | --- |
| Keep the explicit SOUNDEX workaround | No implementation cost, but the requested MySQL syntax still fails. |
| Add a SOUNDS LIKE AST operator and planner rewrite | Preserves original spelling but requires new formatting, traversal and binding cases for behavior already represented by existing nodes. |
| Add an executor function or overload | Duplicates coercion, NULL, parameter, result-domain and evaluation semantics; unnecessary runtime/serialization surface. |
| Lower at the parser boundary (selected) | Matches MySQL's construction, reuses existing semantics, and bounds the production closure to the MySQL parser. |

## Ownership, resources, errors and compatibility

The existing statement owns the resulting AST. Each occurrence uses a fixed
number of standard function/comparison nodes and single-operand lists; no
input-sized duplicate strings or additional expression evaluation are required.
Runtime work and allocations are those of explicit SOUNDEX
equality. There are no new workers, caches, retries, waits, file handles,
cross-query state, native changes, or security/tenant boundaries. Existing parse
limits and statement cleanup continue to own rejection and release. No
concurrency, fault-injection, GPU or performance test is triggered by this path.

No function ID, overload ID, protobuf, wire protocol or catalog schema changes
are planned. New plans contain existing SOUNDEX and equality operations. Old CNs
still cannot parse the new SQL syntax; route such SQL only after all serving
CNs support it. This is not a claim of mixed-version query compatibility.

`buildViewTableDef` persists both `ViewData.Stmt` and a separately formatted
`Definition`. `Stmt` can retain the original SOUNDS LIKE request. Therefore
parser lowering alone does **not** make newly authored views readable by old
binaries. Existing views are unchanged. Before rollback, remove or recreate
new-syntax views using explicit SOUNDEX equality, and convert application SQL
and SQL PREPARE text likewise. Exported canonical definitions must be tested
for replay. No unrelated view-storage rewrite or protocol bump belongs in this
patch. A product requirement for transparent downgrade of new-syntax views
would require design revision before implementation, not an implicit promise.

## Change and validation map

The production closure is R2: SQL grammar, identifiers and generated parser.
Tests are local R1, plus real frontend SQL proof for the R2 boundary. The design
document is R0. Persistence code is read for compatibility limits and is not
modified. Concurrency/liveness/security lenses are inapplicable because this
change introduces no corresponding owner or transition.

| Contract | Cheapest useful proof after approval |
| --- | --- |
| Syntax, grouping and non-reserved identifiers | Focused MySQL parser UT: typed equality/function nodes, arithmetic, comparison-right nesting, logic, CASE, subqueries, parameter indexes, and format/reparse. Include invalid forms and HIGH_NOT_PRECEDENCE controls. |
| Existing syntax remains intact | Zero-conflict parser generation, keyword-policy test, complete MySQL parser owning-package tests, and parser facade consumer tests. |
| Results and types | Extend coherent `func_string_soundex.test` and its result file with explicit known true/false/NULL oracles and equivalence to explicit SOUNDEX equality; include numeric, CHAR/TEXT, binary and expression operands. |
| Parameter specialization and reuse | SQL PREPARE/EXECUTE through text, binary and NULL executions, plus a focused existing binary-protocol frontend fixture when needed to prove marker ordering and runtime types. |
| Query and persistence consumers | Minimum table rows for WHERE/CASE/projection, a view queried and exported/recreated, and CTAS/query result checks. Explicit aliases keep headings stable. |
| Delivery | gofmt/diff checks, scoped vet/configured lint for changed parser packages, feasible normal service build, and final mo-self-review. |

Reuse the existing SOUNDEX BVT fixture; do not create a separate catch-all suite.
Add only rows/queries with distinct semantic oracles. Review every changed
expected result before normal mo-tester comparison. Use a test-owned SQL- and
ISCP-ready instance, normal comparison twice on that same instance, and explicit
teardown residue checks. No multi-CN behavior changes; one-CN execution proves
the local semantics, while remote CI remains separately pending.

Acceptance requires the focused regression to fail for the parser gap at the
clean baseline, then pass after implementation, owning-package/consumer/static
checks to pass, and actual SQL BVT evidence for the changed public contract.
Parser compilation or environment startup alone is not acceptance. If tools,
dependencies, authorization or resources block a check, report its exact
command, target and terminal outcome without weakening the check.

## Review decision and remaining gate

Proposed decisions: parser lowering; preserve existing SOUNDEX domain/error
semantics; retain non-reserved SOUNDS; document old-binary parsing/rollback
limits; keep ROW constructor and JSON execution changes outside this PR.

No design approval is recorded by the author. Required next action is a
traceable review of this exact document revision and approval of those
decisions. Production implementation and readiness claims remain blocked until
that decision exists. A material deviation must return to design review.
