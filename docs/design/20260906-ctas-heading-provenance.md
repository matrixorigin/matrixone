# CTAS heading provenance for format literals

- Status: Review required
- Tracking issue: [#27870](https://github.com/matrixorigin/matrixone/issues/27870)
- Implementation PR: [#27870](https://github.com/matrixorigin/matrixone/pull/27870)
- Design revision: 1
- Last updated: 2026-09-06

## 1. Problem and contract

`CREATE TABLE ... AS SELECT` derives a column name from the rendered SELECT
expression. MatrixOne lowercases that heading to normalize identifiers, but
the format argument of `DATE_FORMAT` and `TIME_FORMAT` is a
case-sensitive SQL string literal: `%M` and `%m` have different meanings.

The required contract is:

> CTAS lowercases identifier text using the existing policy while preserving
> the spelling and quoting of string-literal segments in a rendered heading.
> The contract holds when the format call is wrapped, nested in a scalar
> subquery, or passed through a transparent planner boundary.

This is metadata only. It does not change expression semantics, execution
types, the plan wire format, or catalog storage for ordinary query plans.

## 2. Ownership and representation

The `BindContext` that owns a SELECT output owns its heading metadata. The
metadata is keyed by output ordinal, never by a lowercased heading string.
Each entry is an ordered list of immutable text segments:

| Segment | CTAS normalization |
| --- | --- |
| identifier/ordinary rendered text | apply the existing lowercasing policy |
| SQL string-literal text, including quotes | copy verbatim |

The map is sparse and allocated lazily. A normal projection with no
case-sensitive literal therefore keeps the existing allocation behavior.
Metadata is planner-local and is consumed by `normalizeCTASColumnName`; it
never crosses the protobuf plan boundary.

## 3. Capture boundary

`formatSelectExpressionHeading` is the sole producer of heading provenance.
It uses the existing AST formatter in two passes:

1. A normal-format pass detects `DATE_FORMAT`/`TIME_FORMAT` while rendering.
   This keeps the ordinary path to one formatter pass and avoids reflection or
   a planner-wide AST walk.
2. Only when such a function was rendered, a second pass enables stable
   single-quote output and records string-literal byte ranges in the formatter
   output. The planner converts those ranges to ordered segments.

The formatter owns traversal completeness. Because nested expressions,
`Subquery`, UNION branches, and future expression nodes render through the
same `FmtCtx`, they are included without a second hand-maintained type switch.
The capture is syntactic and does not infer provenance from apostrophes in
rendered identifiers. A dynamic format argument produces no format-literal
range; ordinary identifier normalization therefore remains unchanged. If the
same heading contains another actual string literal, that literal is preserved
as a literal segment as well.

## 4. Propagation and semantic boundaries

Propagation is positional and explicit:

- Direct SELECT expressions call the single producer and append its result.
- Explicit aliases replace the expression heading and clear provenance.
- Derived tables, CTE occurrences, views, and `SELECT *` copy entries by
  column ordinal.
- UNION and other transparent output concatenation shifts entries by the
  branch offset and truncates them with the output list.
- ROLLUP/window generated aliases copy the source entry by output ordinal;
  two expressions with case-sensitive formats cannot overwrite one another.
- JOIN USING copies provenance only when the visible chosen/coalesced value has
  identical provenance on every contributing arm. An ordinary arm or a
  disagreement clears the entry and falls back to safe lowercasing.
- A semantic reset (explicit alias, renamed output column, or a boundary that
  changes the visible value) clears the entry rather than guessing.

Every propagation helper accepts and returns the sparse ordinal map. No helper
looks up provenance by a display name, because display names are already
subject to case folding and can collide.

## 5. Failure, compatibility, and performance

Malformed or inconsistent literal ranges fail closed: the heading uses the
first-pass normal rendering and receives no provenance. This preserves the
pre-change normalization instead of producing a partially transformed name.

The metadata is lazy, bounded by the number of output columns containing
string literals, and not retained by ordinary base-table bindings unless a
derived output actually carries it. The common path does not clone or reflect
over the AST. Existing heading behavior for non-format expressions and dynamic
format patterns remains unchanged.

## 6. Validation plan

Planner tests cover direct and wrapped format calls, scalar-subquery nesting,
derived/CTE/star propagation, JOIN USING agreement and disagreement,
ROLLUP/window ordinal mapping, dynamic patterns, apostrophes in identifiers,
and lazy sparse allocation. Formatter tests cover literal positions and the
normal no-format path. The focused plan package tests and `go vet` are required
before push; the PR CI remains the final validation for the full planner and
distributed SQL suites.
