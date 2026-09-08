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

```json
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
    "steps": [{"step": 0, "root": "7"}],
    "nodes": [
      {"id": "7", "operator": "TABLE SCAN", "inputs": []}
    ],
    "edges": []
  }
}
```

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

The following complete examples use abbreviated node lists where the node
identifiers are illustrative. The implementation keeps every reachable node;
the examples only show the fields needed to explain the mapping.

### Table scan

SQL: `EXPLAIN FORMAT=JSON SELECT * FROM db.t WHERE id = 1`

```json
{
  "query_block": {
    "select_id": 1,
    "table": {"table_name": "db.t", "attached_condition": "(id = 1)"}
  },
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "7"}],
    "nodes": [{"id": "7", "operator": "TABLE SCAN", "inputs": [], "table_name": "db.t", "statistics": {"estimated_rows": 1}}],
    "edges": []
  }
}
```

`query_block.table.table_name` comes from the typed scan binding (a reliable
alias wins over the qualified object reference), and `attached_condition`
comes from the scan/filter expression. The extension fields come from the
scan node and its finite typed statistics.

### Join

SQL: `EXPLAIN FORMAT=JSON SELECT a.id FROM db.t AS a JOIN db.u AS b ON a.id = b.id`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "9"}],
    "nodes": [
      {"id": "9", "operator": "Join", "inputs": ["7", "8"], "join_type": "INNER", "join_condition": "(a.id = b.id)"},
      {"id": "7", "operator": "TABLE SCAN", "inputs": [], "table_name": "a"},
      {"id": "8", "operator": "TABLE SCAN", "inputs": [], "table_name": "b"}
    ],
    "edges": [{"from": "7", "to": "9"}, {"from": "8", "to": "9"}]
  }
}
```

The outer core has no fabricated single-table field. `join_type` and
`join_condition` come directly from the typed join node; `inputs` and `edges`
preserve both child references instead of inventing a MySQL `nested_loop`.

### Non-recursive CTE

SQL: `EXPLAIN FORMAT=JSON WITH c AS (SELECT id FROM db.t) SELECT * FROM c`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "12"}, {"step": 1, "root": "15"}],
    "nodes": [
      {"id": "12", "operator": "CTE Scan", "inputs": ["11"], "source_steps": [0]},
      {"id": "11", "operator": "TABLE SCAN", "inputs": [], "table_name": "db.t"},
      {"id": "15", "operator": "Project", "inputs": ["12"], "expressions": ["id"]}
    ],
    "edges": [{"from": "11", "to": "12"}, {"from": "12", "to": "15"}]
  }
}
```

Only the outer `query_block.select_id: 1` is promised. CTE producer/consumer
nodes, step roots, and cross-step references are typed MatrixOne extension
data; they are not converted into invented MySQL query blocks.

### Window

SQL: `EXPLAIN FORMAT=JSON SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM db.t`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "18"}],
    "nodes": [
      {"id": "18", "operator": "Window", "inputs": ["17"], "expressions": ["row_number(); Order By: id"]},
      {"id": "17", "operator": "TABLE SCAN", "inputs": [], "table_name": "db.t"}
    ],
    "edges": [{"from": "17", "to": "18"}]
  }
}
```

The core query block remains stable while window specifications and their input
relationship come from the typed window node. Expression strings are escaped
by the JSON encoder and are never reconstructed by parsing text EXPLAIN output.

### UPDATE

SQL: `EXPLAIN FORMAT=JSON UPDATE db.t SET v = 2 WHERE id = 1`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "UPDATE",
    "steps": [{"step": 0, "root": "23"}],
    "nodes": [
      {"id": "23", "operator": "MULTI_UPDATE", "inputs": ["22"], "table_names": ["db.t"]},
      {"id": "22", "operator": "TABLE SCAN", "inputs": [], "table_name": "db.t", "filter": "(id = 1)"}
    ],
    "edges": [{"from": "22", "to": "23"}]
  }
}
```

`statement_type`, target names, and source/filter relations are read from the
typed DML and scan nodes. Building this document does not run the update
pipeline, so the target data is unchanged.

### DELETE

SQL: `EXPLAIN FORMAT=JSON DELETE FROM db.t WHERE id = 1`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "DELETE",
    "steps": [{"step": 0, "root": "27"}],
    "nodes": [
      {"id": "27", "operator": "DELETE", "inputs": ["26"], "table_name": "db.t"},
      {"id": "26", "operator": "TABLE SCAN", "inputs": [], "table_name": "db.t", "filter": "(id = 1)"}
    ],
    "edges": [{"from": "26", "to": "27"}]
  }
}
```

The delete target and predicate are sourced from `DeleteCtx` and the typed
scan/filter expressions. As with UPDATE, the explain path only builds and
serializes the plan; it never executes the write pipeline.

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
