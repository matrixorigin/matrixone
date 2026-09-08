# EXPLAIN FORMAT=JSON compatibility contract

- Status: draft; implementation and QA remain pending
- Tracking issue: [matrixorigin/matrixone#28301](https://github.com/matrixorigin/matrixone/issues/28301)
- Target: MatrixOne main
- Design revision: explain-format-json-2026-09-09-r2
- Scope: MySQL-compatible entry syntax and a stable core JSON contract

## Decision

MatrixOne accepts `EXPLAIN FORMAT=JSON <explainable_stmt>` (case-insensitive,
with a quoted JSON value) and the existing `EXPLAIN (FORMAT JSON)
<explainable_stmt>` spelling. Both spellings normalize to the same EXPLAIN
options and produce the same document. A successful statement returns one
column named `EXPLAIN`, one row, and one complete JSON object in that cell.

The compatibility target is a MySQL-style outer `query_block` plus a complete
typed MatrixOne plan in `matrixone`. It does not promise every MySQL optimizer
field, every MySQL plan choice, or client-specific visual formatting. A field
is emitted only when its source has an explicit semantic mapping. An omitted
field means that mapping is unavailable; it is never replaced by a guessed
value, a fake query block, or a placeholder estimate.

## Accepted and rejected combinations

Regular EXPLAIN supports SELECT, INSERT, REPLACE, UPDATE, DELETE, and the
other statements already accepted by MatrixOne's explainable-statement
grammar. JSON EXPLAIN describes the plan and does not execute the explained
statement's read or write pipeline.

`EXPLAIN ANALYZE FORMAT=JSON` and `EXPLAIN PHYPLAN FORMAT=JSON` return a
not-supported error before execution. `ANALYZE FALSE` is normalized to a
regular EXPLAIN and is allowed with JSON. JSON with text-only `CHECK` is
rejected. Repeating an option is an error; there is no last-option-wins rule.
The same validation runs for direct SQL, SQL prepared statements, binary
prepared statements, and reprepare.

`FOR CONNECTION`, `FORMAT=TREE`, `FORMAT=TRADITIONAL`, a default-format
session variable, and `FORMAT=JSON INTO` are outside this revision. Existing
SELECT INTO restrictions remain in force.

## JSON schema and completeness rules

The top-level object always contains these members:

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [],
    "nodes": [],
    "edges": []
  }
}
```

The v1 field contract is:

| JSON path | Type | Source and omission rule |
| --- | --- | --- |
| `query_block.select_id` | integer | Always `1`; this is the only MySQL query-block number promised in v1. |
| `query_block.table.table_name` | string | Present only for one reachable table access. A binding-derived alias wins; otherwise the qualified typed object name is used. |
| `query_block.table.attached_condition` | string | Present only when the scan's typed `FilterList` is available. |
| `matrixone.schema_version` | integer | Always `1` for this contract. |
| `matrixone.statement_type` | string | Typed query statement type. |
| `matrixone.steps` | array of `{step:int,root:string}` | Every `Query.Steps` entry, in order. The root is a node id, not a MySQL select id. |
| `matrixone.nodes` | array | Every node reachable from every step root, exactly once by node id. |
| `matrixone.edges` | array of `{from:string,to:string}` | Every child relationship, in child order, including repeated references to a shared node. |
| `node.id` | string | Typed MatrixOne node id. |
| `node.operator` | string | Typed operator name; a generic name is used when no specialized display exists. |
| `node.inputs` | array of strings | Child node ids in the typed child order. |
| `node.filter` | string | Any typed `FilterList` on a non-AGG node. |
| `node.having` | string | Typed `FilterList` on an AGG node, which is the planner's HAVING location. |
| `node.block_filter` | string | Typed `BlockFilterList`, when non-empty. |
| `node.projection` | array of strings | Typed `ProjectList`, when non-empty. |
| `node.limit`, `node.offset` | string | Typed `Limit` and `Offset` on any node, including a node without SORT/PARTITION. |
| `node.table_name`, `node.table_names` | string/array | Typed scan or DML target names. |
| `node.join_type`, `node.join_condition` | string | Typed join enum and `OnList`. |
| `node.group_by`, `node.aggregate` | string | Typed AGG `GroupBy` and `AggList`. |
| `node.order_by` | string | Existing expression description for readability. |
| `node.order_by_specs` | array | Typed order expressions, direction, NULL rule, collation, and unique bit. |
| `node.windows` | array | Typed `WindowSpec` values, including function, partition, order flags, and frame boundaries. |
| `node.assignments` | array of `{target,value}` | DML target row-image expressions resolved from `UpdateCtxList` and the reachable projection binding. |
| `node.expressions` | array of strings | Operator-specific typed expressions with no dedicated v1 field. |
| `node.source_steps` | array of integers | Typed source-step references, when present. |
| `node.statistics` | object of finite numbers | Finite typed statistics only; missing, NaN, and infinity are omitted. |

Completeness means that every reachable typed node, every input edge, and every
step root is represented. A shared DAG node is serialized once and each parent
keeps its reference. A duplicate reachable id, missing child, cycle, or
necessary expression that cannot be serialized is an SQL error. An operator
without a pretty-printer still receives a generic typed node. Only the
compatibility fields in the table above are optional; the MatrixOne graph may
not silently drop a typed field that has a defined v1 mapping. The renderer
does not parse text EXPLAIN output, cache plans, run remote queries, or mutate
the source plan.

The renderer does not emit `cost_info`, `possible_keys`, `key`, `access_type`,
`filtered`, or a fabricated MySQL `nested_loop`. A join tree that cannot be
losslessly expressed in the MySQL shape remains in the MatrixOne graph.
CTE, window, DML, multi-step, and other non-relational shapes succeed through
the extension rather than being converted into invented MySQL query blocks.

## Typed mapping details

The following mappings are normative for schema version 1:

- A scan alias comes only from a typed binding signal retained in the scan's
  `ProjectList` column reference (`ColRef.TblName` or its qualified `Name`) and
  a valid column position. `TableDef.Cols[].TblName` and
  `OriginTblName` are result metadata and are not alias sources. If the typed
  binding is not consistent across scan columns, the alias is omitted and the
  physical object name is used.
- `FilterList`, `BlockFilterList`, `ProjectList`, `Limit`, and `Offset` are
  extracted for every node before operator-specific rendering. Thus a scan
  predicate, AGG HAVING, or a LIMIT attached to a non-SORT node cannot vanish.
- A window's `OrderBySpec.Flag` is decoded into `direction` (`ASC`, `DESC`, or
  `DEFAULT`) and `nulls` (`FIRST`, `LAST`, or `DEFAULT`). Conflicting or
  unknown flag bits are errors. `FrameClause` is decoded into `unit` (`ROWS`
  or `RANGE`) and textual `start`/`end` boundaries, preserving unbounded,
  current-row, value, and preceding/following semantics.
- An UPDATE row image is read from `UpdateCtxList.InsertCols` and the reachable
  projection node identified by its binding tag. Each non-row-id target column
  is emitted as an `assignments` entry, so a plan for `SET v = 2` includes a
  value of `2`; unchanged columns remain visible as their row-image
  expressions. An unresolved required row-image reference is an SQL error.
- Statistics are copied only when finite. The source `Stats` object is never
  cleaned or rewritten as part of serialization.

## Complete examples

Node ids below are fixture ids. Each example lists every reachable node and
edge for the shown fixture; no node list is abbreviated.

### Table scan

SQL: `EXPLAIN FORMAT=JSON SELECT id FROM db.t WHERE id = 1 LIMIT 1`

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
    "nodes": [
      {"id": "7", "operator": "TABLE_SCAN", "inputs": [], "table_name": "db.t", "filter": "(id = 1)", "projection": ["id"], "limit": "1"}
    ],
    "edges": []
  }
}
```

The core table fields come from the single typed scan and its `FilterList`.
The extension fields come from that scan's `TableDef`, `FilterList`,
`ProjectList`, and `Limit`; no SORT node is required for the limit mapping.

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
      {"id": "9", "operator": "JOIN", "inputs": ["7", "8"], "join_type": "INNER", "join_condition": "(a.id = b.id)", "projection": ["a.id"]},
      {"id": "7", "operator": "TABLE_SCAN", "inputs": [], "table_name": "a", "filter": "(a.id > 0)"},
      {"id": "8", "operator": "TABLE_SCAN", "inputs": [], "table_name": "b", "filter": "(b.id > 0)"}
    ],
    "edges": [{"from": "7", "to": "9"}, {"from": "8", "to": "9"}]
  }
}
```

The join condition and both scan predicates are typed `OnList`/`FilterList`
values. The aliases are from the two scan bindings, so a self-join retains
both names without guessing from formatted text. The graph carries the join
shape directly instead of manufacturing a MySQL nested-loop object.

### Non-recursive CTE

SQL: `EXPLAIN FORMAT=JSON WITH c AS (SELECT id FROM db.t) SELECT id FROM c`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "15"}, {"step": 1, "root": "12"}],
    "nodes": [
      {"id": "15", "operator": "PROJECT", "inputs": ["12"], "projection": ["id"]},
      {"id": "12", "operator": "CTE_SCAN", "inputs": ["11"], "source_steps": [1]},
      {"id": "11", "operator": "TABLE_SCAN", "inputs": [], "table_name": "db.t", "projection": ["id"]}
    ],
    "edges": [{"from": "12", "to": "15"}, {"from": "11", "to": "12"}]
  }
}
```

The outer `query_block.select_id` remains `1`. CTE producer/consumer identity,
step roots, and the cross-step source are taken from typed `Query.Steps`,
`SourceStep`, `Children`, and `ProjectList`; they are not treated as MySQL
subquery numbers.

### Window

SQL: `EXPLAIN FORMAT=JSON SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM db.t`

```json
{
  "query_block": {"select_id": 1},
  "matrixone": {
    "schema_version": 1,
    "statement_type": "SELECT",
    "steps": [{"step": 0, "root": "18"}],
    "nodes": [
      {
        "id": "18",
        "operator": "WINDOW",
        "inputs": ["17"],
        "projection": ["id", "row_number"],
        "windows": [{
          "function": "row_number()",
          "partition_by": ["grp"],
          "order_by": [{"expression": "id", "direction": "DESC", "nulls": "LAST"}],
          "frame": {"unit": "RANGE", "start": "UNBOUNDED PRECEDING", "end": "CURRENT ROW"}
        }]
      },
      {"id": "17", "operator": "TABLE_SCAN", "inputs": [], "table_name": "db.t", "projection": ["id", "grp"]}
    ],
    "edges": [{"from": "17", "to": "18"}]
  }
}
```

The `windows` member comes directly from `WinSpecList`; its direction, NULL
rule, collation when present, and frame boundaries come from the typed
`OrderBySpec` and `FrameClause`, rather than the lossy expression description.

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
      {"id": "23", "operator": "MULTI_UPDATE", "inputs": ["22"], "table_names": ["db.t"], "assignments": [{"target": "db.t.id", "value": "id"}, {"target": "db.t.v", "value": "2"}]},
      {"id": "22", "operator": "TABLE_SCAN", "inputs": [], "table_name": "db.t", "filter": "(id = 1)", "projection": ["id", "2"]}
    ],
    "edges": [{"from": "22", "to": "23"}]
  }
}
```

`table_names` and `assignments` are from `UpdateCtxList`, `TableDef`,
`InsertCols`, and the reachable final projection. The row image explicitly
retains `SET v = 2`. Rendering constructs and serializes this graph only; it
does not invoke the write runner or change `db.t`.

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
      {"id": "26", "operator": "TABLE_SCAN", "inputs": [], "table_name": "db.t", "filter": "(id = 1)", "projection": ["id"]}
    ],
    "edges": [{"from": "26", "to": "27"}]
  }
}
```

The target comes from `DeleteCtx.Ref`/`TableDef`, and the predicate and row
shape come from the typed scan. As with UPDATE, only the planner and renderer
run; the DELETE pipeline is never started.

## Result-set and protocol metadata

All result paths use the same metadata construction after JSON serialization
succeeds. The cell is added only after the complete object has been encoded;
renderer errors therefore produce no successful result row.

| Property | Contract for COM_QUERY, COM_STMT_PREPARE/EXECUTE, and reprepare |
| --- | --- |
| Result shape | Exactly one result column and one row; the row contains one complete JSON object, never JSON fragments or one row per plan node. |
| Column name | `EXPLAIN` (the same label in text and binary prepared metadata for JSON mode). |
| Engine type | `types.T_varchar` with unspecified width (`width=0`) for the synthesized result column. |
| MySQL wire type | `MYSQL_TYPE_VAR_STRING`, produced by the existing `convertEngineTypeToMysqlType` path. |
| Charset/collation | Existing `setCharacter` behavior: collation id `0x21` (`charsetVarchar`) for the synthesized default VARCHAR result. JSON EXPLAIN does not silently switch to binary or JSON wire type. |
| Length/flags | Existing VARCHAR metadata (`length=0xffffffff` for unspecified width); flags and decimal metadata remain the common result-column defaults. |
| Prepared consistency | COM_STMT_PREPARE metadata, COM_STMT_EXECUTE rows, SQL PREPARE/EXECUTE, and reprepare expose the same name, wire type, charset/collation, length, flags, and one-cell shape as COM_QUERY. Parameters are bound through the existing plan-copy path. |

The comparison baseline is a fixed MySQL 8.0.45 server and client capture of
the same small schemas and statements. QA must record the actual column
definition packet (name, type, charset/collation, length, flags) for COM_QUERY,
binary prepare/execute, and reprepare; the table above is the MatrixOne
guarantee, while any MySQL 8.0.45 differences are documented as deliberate
compatibility differences rather than inferred from SQL text. MySQL's field
semantics reference is the [EXPLAIN output documentation](https://dev.mysql.com/doc/refman/8.0/en/explain-output.html).

## Validation obligations

Parser/AST tests cover both spellings, case, quoted values, duplicate and
invalid options, SELECT INTO, round-trip formatting, and `ANALYZE FALSE`.
Renderer tests decode JSON independently and cover scan predicates, AGG
HAVING, non-SORT limits, aliases and self-joins, scans/joins/CTEs/windows,
UPDATE assignments, DELETE, shared DAGs, multiple steps, escaping, missing
statistics, and non-finite source values. Frontend tests cover ordinary and
prepared execution, reprepare, metadata stability, permissions, cancellation,
and renderer errors. Protocol tests cover COM_QUERY and COM_STMT paths. The
distributed BVT exercises the issue statements, text control, DML no-side
effects, prepared execution, and the ANALYZE/PHYPLAN/check rejection boundary.

The acceptance oracle is an independent JSON decoder plus stable path/type
assertions and a typed reachable-node/edge comparison. Do not freeze dynamic
costs, complete JSON strings, node order beyond documented references, or
MySQL optimizer choices. Run focused UT, package regression, a single-CN
protocol/BVT pass, and local scale fixtures. CGo package runs and a fixed
MySQL 8.0.45 capture remain explicit QA evidence; they are not replaced by a
successful text EXPLAIN comparison.
