# MySQL `MEMBER OF` JSON Operator

- Status: in-progress
- Start Date: 2026-09-06
- Authors: MatrixOne SQL automation
- Implementation PR: [#28096](https://github.com/matrixorigin/matrixone/pull/28096)
- Issue: [#23008](https://github.com/matrixorigin/matrixone/issues/23008)
- Design-review decision: pending independent approval

## Summary

Implement the MySQL `MEMBER [OF]` operator for direct JSON-value membership.
The operator compares its left scalar or JSON value with each direct element of
the right JSON array. A non-array right document is treated as one complete
JSON value. The implementation keeps SQL `NULL`, JSON `null`, JSON strings,
numbers, objects, arrays, temporal values, and opaque binary values in distinct
domains.

This document is the versioned design revision requested by the review of
[#28096](https://github.com/matrixorigin/matrixone/pull/28096). It defines the
contract before implementation approval; the approval record remains in the
PR's review history and is intentionally not self-certified here.

## Compatibility target and SQL contract

The grammar and observable behavior target MySQL 8-compatible syntax and the
MySQL 9.6 compatibility cases used for this PR's differential review:

- Both `MEMBER OF` and the existing-compatible `MEMBER` shorthand are accepted;
  `OF` is optional only in that shorthand. `NOT MEMBER OF` is not introduced by
  this change.
- The operator has the precedence of a comparison predicate. Its formatter
  emits the canonical `MEMBER OF` form and preserves parenthesization when the
  surrounding expression requires it. `NOT`, `AND`, `OR`, arithmetic, and
  comparison neighbors continue to bind according to the existing grammar.
- A SQL `NULL` left operand or SQL `NULL` right operand returns SQL `NULL`.
  JSON `null` is a JSON scalar and can match another JSON `null`; it is not
  treated as SQL `NULL`.
- The left operand may be a supported SQL scalar, a JSON document, or a binary
  SQL scalar. Native SQL vector/array types are rejected rather than implicitly
  serialized as JSON arrays.
- The right operand must be `JSON` or a text-domain `CHAR`, `VARCHAR`, or
  `TEXT` value. `BINARY`, `VARBINARY`, `BLOB`, and text-shaped values in the
  binary charset are rejected with MySQL's invalid-JSON-argument error for
  argument 2. Their bytes must never be parsed as JSON text.
- A text-domain right operand is parsed as one JSON document. Malformed JSON
  returns the existing invalid-JSON-document error, including for a non-array
  document. Nesting remains bounded by the existing JSON document depth limit.
- For an array RHS, membership tests direct complete elements only. There is no
  recursive containment, string/numeric coercion, object subset matching, or
  array reordering. Objects and arrays therefore require exact JSON equality.
- A SQL `YEAR` left value is encoded in the JSON number domain. Thus `YEAR
  2024 MEMBER OF ('[2024]')` is true while it does not match the JSON string
  element in `'["2024"]'`. This rule is local to `MEMBER OF`; the existing
  temporal string formatting of `JSON_ARRAY` and `JSON_SET` is unchanged.

## Design

### Parser and planner ownership

The MySQL grammar owns the operator spelling and builds the existing
`tree.FuncExpr` representation with function name `member of`. The planner
resolves it through `INTERNAL_JSON_MEMBER_OF`, checks both operands before
execution, and retains the direct prepared-parameter metadata on the left
operand. The type checker is deliberately asymmetric: the left side accepts
JSON-convertible scalar values, while the right side accepts only JSON or text
JSON documents.

The right-side type predicate uses both the string OID and its static charset.
This prevents a `VARCHAR` with binary charset from bypassing the same rejection
as an intrinsic `BINARY`/`VARBINARY`/`BLOB` type. Runtime prepared metadata and
row-level binary-string provenance are checked again before evaluating a row;
masked rows remain unevaluated.

### Scalar conversion and comparison

`MEMBER OF` owns a small scalar conversion boundary instead of changing the
global JSON-constructor conversion routine. This is required because existing
`JSON_ARRAY`/`JSON_SET` behavior formats temporal `YEAR` values as strings,
whereas the `MEMBER OF` scalar contract compares a `YEAR` as a number. All
other scalar conversions reuse the established JSON conversion helpers.

The right document uses the exact comparator shared with `JSON_OVERLAPS`:

1. Decode and validate a JSON RHS once when it is constant for the batch.
2. For an array, compare the left value with each direct element using exact
   JSON scalar/object/array equality; the existing bounded prepared-array index
   is used when its cost model says preparation is beneficial.
3. For a non-array RHS, compare the two complete documents directly.
4. Preserve the SQL null mask and return an integer `0`/`1` result for every
   non-NULL comparison.

Prepared left parameters retain their concrete protocol type. Numeric types,
including `YEAR` and FLOAT32, are decoded using that type rather than the TEXT
transport representation. Binary left parameters remain opaque JSON values.
An RHS prepared value with binary type or binary-string provenance is rejected
before parsing, so a cached plan cannot turn binary bytes into a JSON document.

### Error and lifecycle behavior

Static type failures use `ER_INVALID_TYPE_FOR_JSON` with argument 2. Runtime
binary metadata uses the same error constructor; malformed text uses the
existing `invalid JSON document` path. Constant decode errors are cached only
for the current expression execution, and the existing vector/process reset
clears prepared metadata between executions. No catalog, storage, wire-format,
configuration, or persistent state is introduced by this operator.

## Alternatives and trade-offs

1. Accept every `IsMySQLString` type on the RHS. This is the previous behavior,
   but it incorrectly treats binary bytes as JSON text and diverges from MySQL.
2. Change `opBuiltInJsonArray.convertToAny` so every JSON constructor formats
   `YEAR` numerically. This would fix this operator by changing unrelated
   `JSON_ARRAY`/`JSON_SET` output; the selected design keeps that public contract
   stable and gives `MEMBER OF` its own documented scalar domain.
3. Add a new JSON membership comparator. This would duplicate the exact
   numeric/object/array comparison and prepared-array cost logic already used by
   `JSON_OVERLAPS`; reuse keeps the two direct-value contracts aligned.

## Performance and bounds

Constant RHS documents are decoded once per batch. Large constant arrays may
use the existing exact prepared-array index, bounded by its element count and
the current evaluation cost threshold; non-constant RHS values are decoded per
row as required by their values. JSON nesting remains bounded by
`JSONDocumentMaxNestingDepth`. The type and provenance checks are O(1) per
evaluated row and add no state beyond the current expression invocation.

## Validation and acceptance map

| Contract | White-box proof | Public SQL proof |
|---|---|---|
| text vs binary RHS domain | `TestJSONMemberOfRejectsBinaryRightDomains` | binary and varbinary casts return `ER_INVALID_TYPE_FOR_JSON` |
| SQL NULL vs JSON null | `TestJSONMemberOfScalarAndNullSemantics` | existing null cases in `func_json_member_of` BVT |
| exact direct equality | JSON value/array/object unit tests | existing nested-array/object BVT cases |
| YEAR numeric domain | `TestJSONMemberOfYearUsesNumericJSONDomain` and prepared YEAR test | numeric and quoted YEAR BVT cases |
| prepared concrete types | prepared scalar/FLOAT32/YEAR tests | prepared statement BVT and frontend protocol tests |
| malformed/selected input | invalid JSON and select-list tests | malformed RHS BVT |
| conflict-free parser integration | regenerated `mysql_sql.go` plus parser tests | full required CI parser/build checks |

The implementation is complete when this matrix passes, the latest PR head has
no applicable reviewer blocker, all required checks are green or intentionally
skipped, and the independent design review has been requested on this document.

## Rollout and review record

There is no migration or feature flag. Nodes running this binary understand the
new parser, planner function, and existing prepared-parameter metadata. Older
nodes retain their normal behavior for statements that do not use `MEMBER OF`;
statements using the new syntax must be routed to a node containing this
implementation.

Independent design approval is requested in PR #28096 after this revision and
its complete implementation are pushed. Any later semantic expansion, new
protocol field, or change to the `JSON_ARRAY` temporal contract requires a new
versioned design revision.
