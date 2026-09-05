- Status: drafted
- Start Date: 2026-09-04
- Authors: MatrixOne SQL team
- Implementation PR: pending (issue #28036)
- Issue for this RFC: #28036

# MySQL `JSON_TABLE()` compatibility

## Summary

Add the MySQL `JSON_TABLE()` table function and its relational row-expansion
semantics. The implementation targets the complete behavior documented for
MySQL 8.0.27 and later, with MySQL 8.0.46 as the pinned differential-test
oracle. The feature is implemented as a dedicated table-function path rather
than as a scalar-function registration.

The change is intentionally delivered in four serial implementation PRs:

1. this compatibility contract;
2. bounded JSON path iteration and scalar conversion primitives;
3. parser, planner, and core execution;
4. remaining MySQL type, diagnostic, lateral-join, and distributed closure.

The issue remains open until the fourth PR is merged and QA supplies an
environment/version-specific PASS.

## Motivation

MatrixOne currently rejects `JSON_TABLE()` in the MySQL parser before planning.
Applications use it to turn a JSON document into rows and columns, including
ordinality, existence tests, nested arrays, defaults, and correlated use with
tables appearing earlier in `FROM`.

A correct implementation must preserve the row-generation contract and the
ownership contract. It must not materialize every path match before the first
output batch, must not silently turn a missing value into JSON null, and must
not confuse an object/array conversion error with a missing path. It also must
work when a table function is evaluated on a remote CN and its diagnostics are
read by `SHOW WARNINGS` on the initiating session.

## Technical Design

### SQL and AST contract

The MySQL surface is:

```text
JSON_TABLE(expr, path COLUMNS(column_list)) [AS] alias

column_list:
  name FOR ORDINALITY
  | name type PATH string_path [on_empty] [on_error]
  | name type EXISTS PATH string_path
  | NESTED [PATH] path COLUMNS(column_list)

on_empty: {NULL | DEFAULT json_string | ERROR} ON EMPTY
on_error: {NULL | DEFAULT json_string | ERROR} ON ERROR
```

`AS` is optional, but the alias itself is mandatory. Paths and defaults are
string literals and are validated while binding. Beginning with MySQL 8.0.27,
output column names are compared case-insensitively; duplicate names are a
bind error. The parser accepts the historical `ON ERROR ... ON EMPTY` order,
records that order in the AST, and lets execution emit MySQL warning 3961.

`tree.TableFunction` gains a dedicated `JSONTable` variant. The variant stores
the source expression, root path, and a recursive column tree. Each column
stores its identifier, kind (`ordinality`, `path`, `exists`, or `nested`),
resolved target type when applicable, relative path, empty/error response, and
children. Existing generic table functions continue to use `FuncExpr`.

### Plan and wire contract

The planner resolves a JSON_TABLE column tree into a dynamic `TableDef` and
keeps the source expression in `TblFuncExprList`. The immutable column tree is
serialized into `TableFunction.Param` as a versioned JSON object:

```json
{"version":1,"root_path":"$[*]","columns":[...]}
```

The payload is an execution detail and does not change `proto/plan.proto`,
catalog metadata, persisted table definitions, or the shape of ordinary
result vectors. A newer CN must reject an unknown payload version explicitly;
an older CN must never interpret it as another table function.

### Path iteration

The ByteJson layer provides a resumable iterator with `Next() (ByteJson, bool,
error)` semantics. It walks one path match at a time, preserves scalar and
JSON-null matches, reports missing as zero matches, and keeps traversal order
identical to the existing JSON path implementation. The iterator uses an
explicit stack bounded by path/container depth and never builds a slice of all
matches. The table-function state owns the iterator until the current source
row is complete, so returned ByteJson views cannot outlive their input batch.

### Scalar conversion

The JSON conversion boundary exposes a single typed conversion operation used
by JSON_TABLE and existing JSON functions. It distinguishes:

- value appended successfully;
- JSON null, appended as SQL NULL;
- object/array (composite) input;
- conversion failure;
- range/overflow failure; and
- successful conversion with truncation diagnostic.

The target type is the planner-resolved MatrixOne type. The conversion path
uses the existing MySQL-compatible numeric, character, temporal, year, and JSON
coercion helpers where they already exist, adding only the missing JSON-to-type
edges. A conversion failure is handled by the column's `ON ERROR` policy; a
missing match is handled only by `ON EMPTY`. Invalid JSON documents, invalid
paths, and invalid DEFAULT JSON remain statement errors and are never hidden by
an `ON ERROR` clause.

### Row generation and lifecycle

The executor keeps a root frame and one frame per active `NESTED` level. A
frame owns its iterator, current match, child cursor, and ordinality. Parent
columns are copied into every produced child row. Sibling nested clauses are
evaluated additively. When a nested path has no match, exactly one row is
produced with that nested subtree NULL-complemented. `FOR ORDINALITY` starts at
one for each applicable row source.

`Call` fills at most the normal MatrixOne output batch and persists all cursors
needed for the next call. `Reset` returns the state to the beginning while
retaining immutable parsed metadata. `Free` is idempotent and releases vectors,
iterators, and borrowed document references on success, error, cancellation,
early LIMIT, and partial output.

### Correlation and joins

An expression in `expr` may reference tables appearing earlier in `FROM`; this
is implicit lateral behavior. A correlated INNER/CROSS use is lowered to
`CROSSAPPLY`. A correlated LEFT use is lowered to `OUTERAPPLY`, with the join
predicate carried into the Apply node and evaluated before deciding whether a
NULL-complemented left row is required. Correlated RIGHT/FULL forms are rejected
according to MySQL lateral-join rules. Non-correlated uses remain ordinary
function scans so they are not evaluated once per left row.

The Apply operator must treat a right batch that is completely removed by the
`ON` predicate as “no match”; this is distinct from a function that produced a
right row. This is required for `LEFT JOIN JSON_TABLE(...) ON predicate`.

### Diagnostics

JSON_TABLE diagnostics use the existing session warning sink and remote terminal
envelope. A keyed-once diagnostic API is added internally so a statement emits
one deprecation warning for reverse clause order and one truncation warning for
multiple truncated values, even when execution spans batches or CNs. Warning
storage remains bounded for `SHOW WARNINGS`; the total protocol warning count
is maintained separately.

### Compatibility and rollout

The feature introduces no catalog or protobuf migration. During a rolling
upgrade, a statement using JSON_TABLE is accepted only when the executing CNs
understand payload version 1; an older CN returns a deterministic unsupported
feature/version error rather than misinterpreting the plan. Rollback is
operationally safe because existing statements and stored objects contain no
JSON_TABLE payload.

## Drawbacks

- The recursive executor and correlated join path add state to a hot table
  function boundary.
- Exact MySQL warning cardinality requires a statement-scoped diagnostic key
  across local and remote execution.
- The first implementation cannot reuse the current `UNNEST` materialization
  algorithm without violating bounded-memory and nested-row semantics.

## Rationale / Alternatives

### Dedicated AST versus encoding the syntax in `FuncExpr`

The dedicated AST keeps column clauses, nested children, and source locations
typed. Encoding them as a variable-length scalar argument list would make
duplicate-name checking, clause ordering, and error ownership ambiguous and
would couple JSON_TABLE to the scalar-function registry.

### Iterator versus eager match collection

An iterator is necessary for bounded memory, cancellation, and output-batch
resumption. Eager collection is simpler but scales with the number of matches
and cannot safely handle a large nested array.

### Apply predicate placement

Filtering a completed OUTER APPLY result would incorrectly discard the left row
when the right function produced rows but none satisfied `ON`. Carrying the
predicate into the per-left-row Apply probe preserves SQL outer-join semantics.

## Testing Contract

The frozen MySQL 8.0.46 corpus covers:

- all four column forms and alias/duplicate-name rules;
- root, wildcard, range, recursive, missing, scalar, object, array, and JSON
  null paths;
- numeric, character, binary, temporal, year, and JSON target types;
- all empty/error actions, invalid defaults, and reverse clause order;
- nested and sibling row cardinality, ordinality, and NULL-complement;
- independent and correlated INNER/CROSS/LEFT joins;
- prepared statements, views, remote CN execution, `SHOW WARNINGS`, cancellation,
  early LIMIT, and repeated reset/free;
- peak memory and first-batch latency for documents with increasing match counts.

Each implementation PR adds focused unit tests; public behavior is covered by
`test/distributed/cases/function/table_func_json_table.test` and its checked-in
`.result` output. Parser generation must remain conflict-free and deterministic.

## Unresolved Questions

None. Any behavior not explicitly listed above is resolved by the pinned
MySQL 8.0.46 differential corpus before PR4 is marked Ready.
