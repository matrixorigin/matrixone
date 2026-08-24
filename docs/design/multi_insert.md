# Multi-table INSERT (`INSERT ALL` / `INSERT FIRST`)

Issue: matrixorigin/matrixone#27470. Branch: `feat-multi-insert`.

Insert the rows of one source query into several tables — possibly with
different schemas — in one statement, Snowflake style.

## Syntax

```sql
-- Unconditional: every source row goes to every target.
INSERT ALL
  INTO t1 [(col, ...)] [VALUES (expr, ...)]
  INTO t2 [(col, ...)] [VALUES (expr, ...)]
  ...
SELECT ... ;

-- Conditional.
INSERT {ALL | FIRST}
  WHEN cond1 THEN INTO t1 [(cols)] [VALUES (...)] [INTO ...]
  WHEN cond2 THEN INTO t2 [(cols)] [VALUES (...)] [INTO ...]
  ...
  [ELSE INTO tN [(cols)] [VALUES (...)] [INTO ...]]
SELECT ... ;
```

- `WHEN` conditions and `VALUES` expressions are evaluated against the
  **output columns of the source query**, referenced by (unqualified) name —
  use aliases in the SELECT list to name them.
- `INTO t` without `VALUES` inserts the source columns positionally; the
  source width must then match the column list (or the table).
- `INSERT ALL` with `WHEN`: a row goes to the targets of **every** `WHEN`
  whose condition is true. `INSERT FIRST`: only the targets of the **first**
  true `WHEN`. Rows matching no `WHEN` go to the `ELSE` targets, if any.
  A condition that evaluates to NULL does not match.
- The same table may appear in several `INTO` clauses.
- A `WITH` clause may precede `INSERT` and applies to the source query.
- Prepared statements (`PREPARE ... FROM 'insert all ...'`) work; parameter
  markers may appear in `VALUES`, `WHEN` and the source query.

Not supported (rejected with a clear error), per the issue's scope:

- targets with foreign key constraints;
- external tables (and streams);
- `ON DUPLICATE KEY UPDATE` / `INSERT IGNORE` / `RETURNING` / `OVERWRITE`
  (not part of the syntax);
- a parenthesized source subquery **directly after a bare `INTO t`**:
  `insert all into t (select ...)` is a parse error because `(` cannot be
  told apart from a column list. Write `select ...` without parentheses, or
  give a column list: `into t (a) (select ...)` parses.

Semantics that follow from the implementation:

- Primary keys, unique keys, NOT NULL, defaults, auto-increment, CHECK
  constraints and every index (B-tree, unique, fulltext, ivfflat) are
  maintained per target exactly as a single `INSERT` would.
- The statement is atomic: a failure in any target (e.g. a duplicate key)
  rolls back all targets.
- Affected rows = sum over all targets. `LAST_INSERT_ID()` is set as for a
  single insert.
- The `INSERT` privilege is required on every target table (and `SELECT`
  on the source); a missing privilege on any target denies the statement.

Example:

```sql
INSERT ALL
  WHEN region = 'EU' THEN INTO customers_eu (id, name, region) VALUES (id, name, region)
  WHEN region = 'US' THEN INTO customers_us (id, name, region) VALUES (id, name, region)
  ELSE INTO customers_other (id, name, region) VALUES (id, name, region)
SELECT id, name, region FROM customers;
```

---

## Design

### Plan shape

The source query is bound **once** and materialized in a `SINK` (step 0).
Every target *table* then becomes its own step that reads the sink through a
`SINK_SCAN` and runs the ordinary modern single-table insert pipeline:

```
Plan 0:
Sink
  ->  Project
        ->  Table Scan on customers

Plan 1:                                       -- customers_eu
Multi Update                                  -- base table + unique + secondary index tables
  ->  Project
        ->  Join DEDUP (FAIL)  (__mo_index_idx_col = __mo_multi_insert_source.name)
              ->  Table Scan on __mo_index_unique_...
              ->  Join DEDUP (FAIL)  (customers_eu.id = __mo_multi_insert_source.id)
                    ->  Table Scan on customers_eu
                    ->  Lock
                          ->  Filter  (__mo_multi_insert_source.region = 'EU')
                                ->  Sink Scan  DataSource: Plan 0

Plan 2:                                       -- customers_other (ELSE)
Multi Update
  ->  Join DEDUP (FAIL) ...
        ->  Lock
              ->  Filter  ((__mo_multi_insert_source.region = 'EU') IS NOT TRUE)
                    ->  Sink Scan  DataSource: Plan 0
```

This is the same fan-out mechanism the engine already uses for IVF/fulltext
index maintenance: one `SINK` dispatching to N `SINK_SCAN` consumers, all
steps executed concurrently by `Compile.runOnce`. Nothing new is needed in
the compiler or the operators; the planner only assembles existing nodes.

Per target, the pipeline is:

```
SINK_SCAN(source image)
  -> FILTER   (WHEN condition; plus "earlier cond IS NOT TRUE" for FIRST/ELSE)
  -> PROJECT  (VALUES expressions, or the source columns positionally)
  -> the regular insert tail:
       appendInsertReplaceSourceCasts        (assignment casts to target types)
       appendNodesForInsertStmt              (defaults, auto-increment, composite pk,
                                              generated cols, CHECK, PreInsert)
       appendDedupAndMultiUpdateNodesForBindInsert
                                             (Lock, DEDUP joins on pk/unique keys,
                                              MULTI_UPDATE writing base + regular
                                              index tables, irregular-index sink)
```

Because the tail is literally the single-table `bindInsert` tail, every
constraint/index behaviour is inherited rather than re-implemented.

### Routing rules as filters

For clause *i* under `WHEN cond_i`:

| statement                     | filter on the branch                                   |
|-------------------------------|--------------------------------------------------------|
| `INSERT ALL WHEN`             | `cond_i`                                               |
| `INSERT FIRST WHEN`           | `cond_i AND cond_1 IS NOT TRUE AND ... AND cond_{i-1} IS NOT TRUE` |
| `ELSE`                        | `cond_1 IS NOT TRUE AND ... AND cond_n IS NOT TRUE`    |
| unconditional `INSERT ALL`    | none                                                   |

`IS NOT TRUE` (function `isnottrue`) rather than `NOT` so that a NULL
condition is treated as "did not match": the row stays eligible for later
`WHEN`s and for `ELSE`, matching Snowflake.

### The same table in several INTO clauses

A first version gave every `INTO` clause its own pipeline. That is wrong when
two clauses write the same table: both DEDUP joins probe the table as of the
statement's snapshot, neither sees the other's rows, and duplicate primary
keys produced by different clauses were **silently inserted**. (A single
`INSERT ... SELECT` does catch in-batch duplicates — the dedup join / multi
update track keys seen within the statement.)

Therefore clauses are grouped by target table and one table gets exactly one
write pipeline:

```
UNION ALL
  ->  PROJECT (clause 1 values, widened + cast)  <- FILTER <- SINK_SCAN
  ->  PROJECT (clause 2 values, widened + cast)  <- FILTER <- SINK_SCAN
-> appendNodesForInsertStmt -> lock/dedup -> MULTI_UPDATE
```

- The clauses may have different column lists. Each branch is widened to the
  **union of the clauses' column lists**: a column the clause sets is its
  bound value cast to the target column type (`castInsertSourceColumn`, the
  same ENUM/SET/GEOMETRY-aware assignment cast the single-table tail uses);
  a column the clause does not set is the column's default expression
  (`getDefaultExpr`). Auto-increment columns left unset become NULL, which
  PreInsert turns into a generated value, so one clause may supply explicit
  ids while another lets the engine generate them.
- Columns set by no clause are left to `appendNodesForInsertStmt`, exactly as
  in a single insert (so a NOT NULL column without default still errors at
  plan time).
- Casting happens *before* the union so all branches have identical types;
  the union node is the ordinary left-deep `Node_UNION_ALL` chain.
- For a table with a single clause nothing is widened: the clause's
  projection goes straight into `appendInsertReplaceSourceCasts`.

With this, `insert all into t (id) values (id) into t (id) values (id)
select ...` raises `Duplicate entry`, and `INSERT FIRST` with the same table
in two branches still routes each row exactly once.

### Irregular (fulltext / ivfflat) indexes

`appendDedupAndMultiUpdateNodesForBindInsert` records, in the builder's
single-target `irregularMaint*` fields, the materialized new-row-image sink
from which fulltext/IVF maintenance sub-plans are later built by
`finishIrregularIndexMaintenance`. Multi-insert moves those fields into
`builder.irregularUpdateMaints` after each target (the list form that
multi-table UPDATE already uses), so every target's maintenance is emitted.

**Ordering trap:** `getIrregularIndexes(tableDef)` must be called *before*
`appendNodesForInsertStmt`, which strips irregular indexes from
`tableDef.Indexes` (they are not MULTI_UPDATE targets). Calling it afterwards
produces a plan without any maintenance step and no error — the index simply
stays empty. A planner unit test (`TestMultiInsertKeepsIrregularIndexMaintenance`,
mock table `docs_ft`) pins this.

### Binding the source image

Each branch gets its own `BindContext`; the `SINK_SCAN` carries a synthetic
`TableDef` named `__mo_multi_insert_source` whose columns are the source
query's headings and projection types, registered with `addBinding`.
`addBinding` deliberately registers `SINK_SCAN` columns only under the table
name, so the planner also fills `ctx.bindingByCol` to make the unqualified
names users write in `WHEN`/`VALUES` resolve. Duplicate source headings are
therefore reported as ambiguous, like in any SELECT.

Conditions are bound with `splitAndBindCondition` (WhereBinder, cast to
bool); `VALUES` expressions with the same binder; subqueries in either are
flattened with `flattenSubqueries` before the FILTER/PROJECT node is added.

---

## Implementation map

| Layer | File | What |
|---|---|---|
| AST | `pkg/sql/parsers/tree/multi_insert.go` | `MultiInsert{First, Targets, Whens, Else, Source, With}`, `MultiInsertTarget{Table, Columns, ColumnNames, Values}`, `MultiInsertWhen{Cond, Targets}`; `Format`, `StmtKind`, `AllTargets`. |
| Grammar | `pkg/sql/parsers/dialect/mysql/mysql_sql.y` | `multi_insert_stmt`, `multi_insert_when_list`, `multi_insert_else_opt`, `multi_insert_into_list`, `multi_insert_into`; new `%nonassoc LOWER_THAN_LPAREN`. Regenerated `mysql_sql.go` with zero conflicts. |
| Planner | `pkg/sql/plan/bind_multi_insert.go` | `bindAndOptimizeMultiInsertQuery` → `bindMultiInsert` (source sink, grouping) → `bindMultiInsertGroup` (one pipeline per table) → `bindMultiInsertBranchSource` (SINK_SCAN/FILTER/PROJECT per clause), `appendMultiInsertUnionAll`, `bindMultiInsertNotTrue`, `validateMultiInsertTarget`. |
| Planner (shared) | `pkg/sql/plan/bind_insert.go` | Tail of `initInsertReplaceStmt` extracted into `appendInsertReplaceSourceCasts`; per-column cast extracted into `castInsertSourceColumn`. Behaviour of single-table INSERT/REPLACE unchanged. |
| Dispatch | `pkg/sql/plan/build.go`, `build_dcl.go` | `BuildPlan` case; PREPARE list. |
| Frontend | `pkg/frontend/stmt_kind.go` | DML, allowed inside transactions. |
| | `pkg/frontend/authenticate.go` | INSERT privilege; the per-table check comes from the plan's MULTI_UPDATE nodes (`extractPrivilegeTipsFromPlan`), so every target is checked. |
| | `pkg/frontend/mysql_cmd_executor.go` | routed through the optimizer like `Insert`. |
| | `pkg/frontend/status_stmt.go` | `last_insert_id`. |
| | `pkg/frontend/remap_db.go` | database remapping of targets, values, conditions, source. |
| Test mock | `pkg/sql/plan/mock.go` | `index` gained `indexAlgo/indexAlgoTableType/indexAlgoParams`; new `docs_ft` table with a fulltext index. |

### Grammar notes

- `ALL` is reserved, so `INSERT ALL` is unambiguous. `FIRST` is a
  non-reserved keyword; `INSERT FIRST WHEN` is disambiguated by the `WHEN`
  lookahead (a table named `first` in a plain `INSERT first ...` still works).
- After `INTO table_name` the parser must choose between shifting `(` (a
  column list) and reducing the clause (a parenthesized source subquery).
  `INTO table_name %prec LOWER_THAN_LPAREN` makes shift win, so the grammar
  stays conflict-free at the cost of the documented limitation above.
- `multi_insert_into` enumerates the four `[(cols)] [VALUES (...)]`
  combinations explicitly instead of using empty optional rules, again to
  avoid empty-reduce conflicts before `(`.

### Adding a new statement type — checklist learned here

A new `tree.Statement` silently misbehaves unless all of these are done:
`StmtKind()` (the embedded default nil-panics in `executeStmt`);
`stmt_kind.go` DML list; `authenticate.go` privilege switch;
`mysql_cmd_executor.go` `buildPlan` optimizer list; `build_dcl.go` prepare
list; `remap_db.go`; `build.go` dispatch.

---

## Verification

- Parser: `pkg/sql/parsers/dialect/mysql/multi_insert_test.go` — round trips,
  AST shape, syntax errors.
- Planner: `pkg/sql/plan/bind_multi_insert_test.go` — step/node shape for
  unconditional and conditional forms, FIRST/ELSE filter composition, ALL not
  negating earlier branches, same-table merge into one MULTI_UPDATE with
  UNION ALL, widening errors, fulltext maintenance retained, rejected targets.
- BVT: `test/distributed/cases/dml/insert/multi_insert.sql` — routing,
  same-table clauses (defaults, duplicate rejection, FIRST, auto-increment),
  fulltext + ivfflat targets, constraint errors and statement atomicity,
  explicit transactions, error messages, EXPLAIN. The EXPLAIN case uses
  targets without unique indexes on purpose: unique-index table names contain
  a UUID and would make the expected output non-deterministic.
- Also checked by hand on a single-node server: affected-row counts,
  `LAST_INSERT_ID()`, prepared statements with parameters, privilege denial
  when one target lacks INSERT, the whole `dml/insert` BVT directory
  (no regression from the shared-tail refactor).

## Possible follow-ups

- `INSERT OVERWRITE ALL` (Snowflake truncates targets first).
- Allow a parenthesized source after a bare `INTO` via a lexer-level
  lookahead instead of the `%prec` resolution.
- Foreign-key targets: reuse `bindAndOptimizeInsertQuery`'s in-plan
  parent-existence checks per target.
