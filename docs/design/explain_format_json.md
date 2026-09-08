# EXPLAIN FORMAT=JSON compatibility contract

- Status: draft; implementation and QA remain pending
- Tracking issue: [matrixorigin/matrixone#28301](https://github.com/matrixorigin/matrixone/issues/28301)
- Target: MatrixOne main
- Design revision: explain-format-json-2026-09-08-r1
- Scope: MySQL-compatible entry syntax and a stable core JSON contract

## Decision

MatrixOne will accept both `EXPLAIN FORMAT=JSON <explainable_stmt>` and the
existing `EXPLAIN (FORMAT JSON) <explainable_stmt>` spelling. A successful
statement returns one `EXPLAIN` string column and one row containing one valid
JSON document. The document has a `query_block` for the outer query and a
`matrixone` extension containing the complete reachable MatrixOne plan.

This is a core compatibility surface, not a promise to reproduce every MySQL
optimizer field or plan choice. Fields are emitted only when MatrixOne has an
explicit semantic source. Missing fields mean that the mapping is not
available; they are not filled with estimates or placeholders.

## Accepted and rejected combinations

Regular `EXPLAIN` supports SELECT, INSERT, REPLACE, UPDATE, DELETE, and other
statements already accepted by MatrixOne's explainable-statement grammar.
`FORMAT=JSON` does not execute the explained statement.

`EXPLAIN ANALYZE FORMAT=JSON` and `EXPLAIN PHYPLAN FORMAT=JSON` are rejected
before the inner statement is executed. `ANALYZE FALSE` is a regular EXPLAIN
option and therefore may be combined with JSON. JSON cannot be combined with
the text-only `CHECK` option. Duplicate options are rejected. `FOR CONNECTION`,
`FORMAT=TREE`, `FORMAT=TRADITIONAL`, default-format session variables, and
`FORMAT=JSON INTO` are outside this revision.

## JSON contract

The stable top-level shape is:

`json
{
  "query_block": {
    "select_id": 1,
    "table": {
      "table_name": "db.table",
      "attached_condition": "id = 1"
    }
  },
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [
      {
        "root": "7",
        "nodes": [
          {"id": "7", "operator": "TABLE SCAN", "inputs": []}
        ],
        "edges": []
      }
    ]
  }
}
`

The exact `table` value is present only when a single table access and its
binding can be identified without parsing rendered text. A reliable alias is
preferred; otherwise the qualified table name is used. `attached_condition` is
present only when the filter belongs to that table access. Joins are represented
in `matrixone`; a MySQL `nested_loop` is emitted only when the physical shape is
losslessly representable. Cost, key, access-type, and filtering fields are not
part of this revision.

`matrixone.steps` follows query step roots and `matrixone.nodes` contains each
reachable plan node once. `inputs` and `edges` preserve shared-DAG references.
The extension may include operator names, stable node identifiers, statement
type, step roots, and escaped expression descriptions. It does not expose trace
UUIDs, internal error envelopes, or runtime scheduling text. Missing statistics
are omitted and non-finite numeric values are never serialized.

## Examples and source mapping

| Query shape | Core field | Extension source |
|---|---|---|
| Table scan | `query_block.table.table_name` and optional condition | Scan node, object reference, reachable node |
| Join | `query_block` only when a lossless single-table view exists | Join node, both child references, join condition |
| CTE | Outer `query_block.select_id: 1` | CTE producer/consumer nodes and edges |
| Window | Outer `query_block` | Window and partition/order nodes |
| UPDATE | Outer `query_block` | Update and source nodes; no execution side effect |
| DELETE | Outer `query_block` | Delete and source nodes; no execution side effect |

## Validation obligations

Parser tests cover both spellings, case, duplicate/invalid options, SELECT
INTO, option round-trip, and `ANALYZE FALSE`. Renderer tests decode JSON
independently and cover scans, joins, CTEs, windows, DML, shared nodes,
multiple steps, missing statistics, escaping, and non-finite values. Frontend
tests cover ordinary and prepared execution, reprepare, metadata stability,
permission errors, cancellation, and renderer errors. A distributed BVT
exercises the issue reproduction, text control, DML no-side-effect checks, and
the ANALYZE rejection boundary.

The expected oracle is a JSON decoder plus stable semantic paths and a typed
reachable-node comparison. Dynamic costs, node ordering beyond documented
references, and complete MySQL output strings are not acceptance oracles.
