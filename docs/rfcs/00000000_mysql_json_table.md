- Status: drafted
- Revision: 2026-09-07 semantic review repair
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
bind error. The parser accepts the historical `ON ERROR ... ON EMPTY` order
and records that order in the AST. It emits MySQL 8.0.46 warning **1287**
(`ER_WARN_DEPRECATED_SYNTAX`) with the exact message
`Specifying an ON EMPTY clause after the ON ERROR clause in a JSON_TABLE column definition is deprecated syntax and will be removed in a future release. Specify ON EMPTY before ON ERROR instead.`
at parse time, once for each reversed column occurrence. This is a syntax
warning, not an execution warning: it is produced for `PREPARE` while the
statement is parsed, is not suppressed by an empty source, and is not emitted
again by `EXECUTE` unless the statement is parsed again. It is kept separate
from runtime conversion/truncation diagnostics.

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
{
  "version": 1,
  "root_path": "$[*]",
  "columns": [...],
  "apply_conditions": ["<base64 plan.Expr>"]
}
```

The payload is an execution detail and does not change `proto/plan.proto`,
catalog metadata, persisted table definitions, or the shape of ordinary
result vectors. A newer CN must reject an unknown payload version explicitly;
an older CN must never interpret it as another table function. `apply_conditions`
is optional and is present only for a correlated JSON_TABLE on the right side
of APPLY. Each entry is the canonical protobuf serialization of a bound
`plan.Expr`, encoded as standard base64 by the JSON serializer. `Node_APPLY.OnList`
remains the logical source of truth. The planner binds and performs the final
column-reference remap first, then serializes the remapped expressions; any
later remap must re-encode the payload before dispatch. The remote CN decodes,
validates, and installs the same conditions in its `pipeline.Apply` instance.
Missing, malformed, or stale condition payloads are deterministic plan errors,
never an instruction to execute the right side without `ON` filtering. This
transport mirror uses the existing `TableFunction.Param` and remote execution
handoff and does not add a catalog or protobuf migration.

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

A `PATH` column first evaluates all matches for the current row source. One
match is converted directly. Multiple matches are aggregated, in path order,
into one JSON array only when the target type is JSON; for every non-JSON target
they are a conversion error and therefore use that column's `ON ERROR` action.
For example, a JSON column at `'$[*]'` over `[1,2]` receives `[1,2]`, while an
`INT` column at the same path takes `ON ERROR`. `EXISTS` only tests whether at
least one match exists and never builds that array. A missing path remains zero
matches and is handled by `ON EMPTY`.

The iterator must not build an auxiliary slice containing every match. A JSON
array result is the permitted exception: it is the final output cell and is
appended incrementally while matches are consumed. The builder is bounded by
the existing MatrixOne JSON/varlen cell limit
`JSON_TABLE_MAX_CELL_BYTES = types.MaxBlobLen` (64 MiB). Crossing that limit,
or an mpool/vector allocation failure while constructing the final cell, is a
statement error with partial builder state released; it is not converted into
`ON ERROR`. Tests cover one match, many matches, the cell limit, and allocation
failure separately from the iterator's bounded traversal memory.

### Row generation and lifecycle

The executor keeps a root frame and one frame per active `NESTED` level. A
frame owns its iterator, current match, child cursor, and ordinality. Parent
columns are copied into every produced child row. Sibling nested clauses are
evaluated additively, in declaration order, rather than as a Cartesian product.
For one parent row, let each sibling nested clause produce a row set `R_i`.
The output is the concatenation of the non-empty `R_i` sets. A sibling with an
empty `R_i` contributes NULL-complemented columns to rows produced by other
siblings, but does not create a row of its own. Only when **all** sibling sets
are empty does the parent emit one NULL-complemented row. Thus sibling
cardinalities `0/0`, `0/N`, `N/0`, and `N/M` produce respectively one, `N`,
`N`, and `N+M` rows. The same rule is applied recursively at every nested level;
a direct match of a nested path remains a valid row source even when one of its
child nested clauses is empty. An unmatched nested subtree is set directly to
SQL NULL, including when its columns specify `DEFAULT ... ON EMPTY`; the pinned
MySQL 8.0.46 source/result oracle takes precedence over a handbook reading.
`FOR ORDINALITY` starts at one and advances only for an actual match, never for
a NULL-complement row. For example, a parent `{"a":[],"b":[10,20]}` with
sibling paths `a` and `b` returns `(NULL,10)` and `(NULL,20)` only; a parent
with both arrays empty returns one all-NULL sibling row.

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
The condition is evaluated on every CN from the `apply_conditions` payload
described above. A correlated LEFT join whose right rows all fail `ON` emits
exactly one left row with the JSON_TABLE columns NULL-complemented; a correlated
INNER/CROSS join emits no row. A multi-CN test must exercise a function that
does produce right rows, make the predicate reject every one, and assert that
the coordinator returns exactly one NULL-complemented row rather than the
unfiltered right rows.

### Diagnostics

JSON_TABLE diagnostics use the existing session warning sink and remote terminal
envelope, but syntax and execution diagnostics have different phases and count
rules.

* **Parse/prepare phase.** Reverse clause order emits code 1287 and the exact
  message frozen in the SQL/AST section once per reversed column occurrence.
  The occurrence ordinal is part of the local diagnostic identity, so two
  reversed columns produce two warnings. This warning is not keyed together
  with runtime truncation and is not re-emitted by a later `EXECUTE`.
* **Execution phase.** Conversion, truncation, and invalid-value warnings use
  the MySQL 8.0.46 code/message pair for that condition. A warning declared
  `statement_once` (including the JSON_TABLE truncation warning) is emitted at
  most once per statement/table-function/column semantic key, across input
  rows, output batches, CNs, and retries. Ordinary row-level warnings retain
  `each_event` semantics and are never accidentally deduplicated.

The existing terminal JSON envelope is extended, without a protobuf/catalog
migration, with a statement diagnostic scope, attempt/contribution identity,
and three independent transport facts:

1. `warning_once_keys` is the complete set of keyed-once identities observed
   by the contribution. It is bounded by the finite statement key space
   (JSON_TABLE operator, diagnostic kind, and column or parse-occurrence
   ordinal), not by input rows, batches, CNs, retries, or the presentation
   retention limit. A key is never evicted when the presentation list is full.
2. `warning_each_event_count` is the number of committed ordinary
   `each_event` occurrences in the contribution. It is an aggregate count and
   is not inferred from the number of retained records.
3. `warning_diagnostics` is a bounded presentation list for `SHOW WARNINGS`.
   Its records may be dropped after the existing retention limit without
   changing either the complete key set or the ordinary count. A keyed-once
   record is retained at most once per key; an `each_event` record is only a
   presentation sample and never a counting source.

The wire shape is equivalent to:

```json
{
  "warning_scope": "<coordinator statement id>",
  "warning_attempt": "<statement execution attempt id>",
  "warning_contribution_id": "<logical terminal contribution id>",
  "warning_once_keys": ["<complete stable diagnostic key set>"],
  "warning_each_event_count": 0,
  "warning_diagnostics": [
    {
      "key": "<stable diagnostic key>",
      "mode": "statement_once|each_event",
      "phase": "parse|execute",
      "code": 1265,
      "message": "..."
    }
  ]
}
```

The stable key contains the coordinator statement id, table-function/operator
ordinal, diagnostic kind, and column/parse-occurrence ordinal; it deliberately
does not contain CN id, fragment id, batch id, or retry id. `warning_scope` is
stable for the user statement. `warning_attempt` identifies one complete
execution attempt, and `warning_contribution_id` is stable for that logical
terminal contribution when the same terminal envelope is retransmitted. A
new retry uses a new attempt identity; an intermediate CN preserves the
scope, attempt, and contribution identity while forwarding the contribution
or maintains an equivalent child-contribution ledger before emitting its
aggregate. It must never reconstruct keyed-once identity from retained
records or forward only an aggregate total.

Execution warning state is attempt-owned. The collector first accumulates an
attempt-local key set, ordinary count, and presentation list. The coordinator
merges a terminal contribution into a provisional attempt ledger keyed by
`warning_contribution_id`; a duplicate terminal envelope is a no-op. Only
when every required terminal for the statement attempt succeeds are the
provisional facts committed to the statement. A failed, cancelled, timed-out,
or otherwise uncommitted execution attempt discards its keyed-once keys,
ordinary count, and presentation records. If a retry follows a partial
terminal merge, the old provisional ledger is discarded, the retry starts with
an empty attempt ledger, and only the retry's successful contributions are
committed. Thus a successful retry cannot inherit a failed attempt's ordinary
count or double-count a key that appeared in a discarded partial merge.
Parse/prepare warning 1287 remains phase-local: a successful PREPARE commits
its reversed-column occurrence keys and EXECUTE retries do not re-emit them.

For a successfully committed execution attempt, the coordinator computes the
execution warning cardinality as:

```text
len(union(committed warning_once_keys)) +
sum(committed warning_each_event_count)
```

It then combines that result with the separately committed parse/prepare
diagnostics. For this contract, remote `warning_count` is not an authoritative
transport field. If retained for compatibility, it is a derived final-session
value only; it is never summed to recover keyed-once identity. Two CNs
reporting the same keyed-once event therefore contribute one after union even
when both records were omitted by retention, while two distinct omitted keys
contribute two. Intermediate forwarding unions keys, sums committed each-event
counts, and independently applies the presentation cap. A JSON_TABLE-capable
CN that cannot provide the complete key set for a contribution that may contain
a `statement_once` event, or cannot provide the each-event count or contribution
identity, has a protocol/version error rather than a fallback to blind
accumulation. An empty `warning_once_keys` set is valid when no keyed-once event
occurred. The statement-scoped state is released after the statement, including
a failed or cancelled statement; a new statement receives a new scope.

### Compatibility and rollout

The feature introduces no catalog or protobuf migration. During a rolling
upgrade, a statement using JSON_TABLE is accepted only when the executing CNs
understand payload version 1; an older CN returns a deterministic unsupported
feature/version error rather than misinterpreting the plan. The same gate
applies to `CREATE VIEW` and `ALTER VIEW` containing JSON_TABLE.

Views are a deliberate compatibility boundary: MatrixOne persists the view SQL
inside `ViewData.Stmt`/`plan.ViewDef.View`, and `bindView()` reparses that SQL on
the next read. An older binary therefore cannot read a JSON_TABLE view. The
supported downgrade rule is fail-closed: a downgrade preflight must reject the
downgrade while any view definition contains JSON_TABLE, and the operator must
drop or replace those views with syntax understood by the target version before
retrying. There is no automatic SQL rewrite and no claim that catalog-format
compatibility makes the view semantically downgrade-safe. If an environment
cannot inventory views, downgrade with JSON_TABLE views is unsupported. The
testing contract includes create view, restart/read on the new version, an old
version read that returns the deterministic unsupported-feature error, and the
successful drop/replace prerequisite before downgrade.

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
- sibling cardinality matrix `0/0`, `0/N`, `N/0`, and `N/M`, including the
  no-match nested `DEFAULT ON EMPTY` oracle that still returns NULL;
- independent and correlated INNER/CROSS/LEFT joins;
- prepared statements, including direct/prepare/execute reverse-clause
  warning timing, empty sources, and two reversed columns;
- views across restart and old-version read/downgrade gates;
- remote CN execution, remapped APPLY conditions, and LEFT APPLY where every
  right row fails `ON`, yielding exactly one NULL-complemented row;
- `SHOW WARNINGS`, keyed-once warning merging across CNs/batches/retries,
  ordinary row-warning counts, bounded retention, cancellation, early LIMIT,
  and repeated reset/free. The diagnostic transport/counting contract has
  dedicated oracles for: a full presentation list before a keyed-once key K
  arrives; the same versus distinct omitted keys from two CNs; direct and
  intermediate-CN forwarding; mixed keyed-once and ordinary each-event
  counts; duplicate terminal-envelope replay; and a failed attempt after a
  partial terminal merge followed by retry. These oracles assert that the
  complete key set is retained independently of presentation records, failed
  attempt counts are discarded, committed each-event counts are summed, and
  a retry contributes only its successful attempt once. The key-set bound is
  the planned operator/column/diagnostic space; exceeding that bound is a
  deterministic protocol error rather than key eviction.
- single-column multi-match conversion: JSON array aggregation versus
  non-JSON `ON ERROR`, the 64 MiB cell boundary, allocation failure cleanup,
  and peak iterator memory/first-batch latency for documents with increasing
  match counts.

Each implementation PR adds focused unit tests; public behavior is covered by
`test/distributed/cases/function/table_func_json_table.test` and its checked-in
`.result` output. Parser generation must remain conflict-free and deterministic.
The pinned source-of-truth anchors are MySQL 8.0.46 `sql_yacc.yy` for the
parse-time 1287 warning, `table_function.cc` for sibling row production and
multi-match conversion, and `mysql-test/suite/json/r/json_table.result` for
the executable oracle, including unmatched nested paths with `DEFAULT`.

## Review disposition and unresolved questions

The earlier review findings outside the Diagnostics section remain resolved:
sibling fallback is defined by the whole parent row source; APPLY predicates
have a versioned remote representation and post-remap encoding point;
reverse-order syntax is fixed at code 1287 with parse/prepare timing and
occurrence counting; JSON_TABLE views have an explicit downgrade gate; and
multi-match PATH conversion distinguishes the final JSON cell from the
forbidden auxiliary match slice with a concrete size limit and allocation-
failure rule. This revision closes the remaining diagnostic information-loss
gap by separating complete keyed-once identity, committed each-event counting,
bounded presentation, failed-attempt ownership, and terminal retry
deduplication. No unresolved design question remains.
Any behavior not explicitly listed above is resolved by the pinned MySQL 8.0.46
differential corpus before PR4 is marked Ready.
