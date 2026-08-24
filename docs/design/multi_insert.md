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

### Runtime: the sink is a streaming fan-out, not a materialization

Nothing is written to temporary storage. At compile time
(`pkg/sql/compile/compile.go`):

- `Node_SINK` becomes a `dispatch` operator in `SendToAllLocal` mode
  (`compileSinkNode`). Each batch produced by the source pipeline is handed
  to a spool and pushed to every registered consumer as one shared,
  reference-counted batch — it is not copied per target.
- `Node_SINK_SCAN` becomes a `merge` operator reading a `PipelineEdge`
  (`compileSinkScanNode`), i.e. a Go channel with **buffer size 1**
  (`NewPipelineEdge(1, 0)` in `compileSinkScan`), so each consumer holds at
  most one batch in flight.
- All steps — the source and every target — are launched concurrently by
  `Compile.runOnce`, so targets consume while the source produces.

Memory is therefore one batch per consumer edge plus whatever the target
pipelines hold, which is what a single `INSERT ... SELECT` holds: the DEDUP
joins build their hash tables on the *existing target table* side and stream
the incoming rows through as probes; `Multi Update` buffers and flushes to S3
as for a plain insert. Backpressure is natural — the dispatcher cannot run
ahead of the slowest target — so there is no unbounded buffering.

Two related facts: the engine's disk/memory-materializing sink variant
(`ExtraOptions == materialized.CTESinkOption`) exists only for explicitly
materialized CTEs and is not used here; and `reduceSinkSinkScanNodes`
collapses a sink with a single consumer, so a statement whose clauses all hit
one table (merged via UNION ALL) has no dispatch at all. The real extra cost
of N targets is CPU — the source rows are pushed through N filter/project
stages — not storage.

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

## Test plan and strategy

### Strategy

The feature is a new front end (grammar + AST) over an unchanged write path.
The risk therefore sits in three places, and the tests are layered to match:

1. **Syntax and AST** — the grammar must accept every documented form, reject
   the malformed ones, and `Format` must round-trip (statement text is logged,
   restored and re-parsed by the frontend). Parser unit tests, no server.
2. **Plan shape** — the planner must produce *exactly* the fan-out described
   above: one SINK, one write step per target table, the right FILTER
   conjuncts for ALL/FIRST/ELSE, same-table clauses merged through UNION ALL,
   and the irregular-index maintenance steps present. These are structural
   properties that end-to-end results cannot distinguish from luck (e.g. a
   missing fulltext step just leaves the index empty; a missing merge only
   shows up with overlapping keys), so they are asserted directly on the plan
   with the mock catalog. Planner unit tests, no server.
3. **Behaviour** — constraints, indexes, atomicity, transactions, privileges
   and error messages must match a single INSERT. These are exercised
   against a real server with BVT cases whose expected results are checked
   in, plus a regression run of the existing insert cases to prove the
   shared insert tail (`appendInsertReplaceSourceCasts` /
   `castInsertSourceColumn`, refactored out of `initInsertReplaceStmt`) did
   not change single-table behaviour.

Every bug found during development got a test at the layer that would have
caught it earliest (see "Regression guards" below).

### Layer 1 — parser: `pkg/sql/parsers/dialect/mysql/multi_insert_test.go`

| Test | Covers |
|---|---|
| `TestMultiInsertSyntaxRoundTrip` | parse → `Format` → re-parse → identical text for: unconditional `INSERT ALL`; explicit column lists and VALUES expressions with a db-qualified target; conditional ALL with ELSE (the issue's example); FIRST with several INTOs under one WHEN; `WITH` prefix; parenthesized source after a column list; `UNION ALL` source. |
| `TestMultiInsertSyntaxShape` | AST structure: `First` flag, `Targets` vs `Whens`/`Else`, per-target `Columns`/`Values` (nil when omitted), `AllTargets()` order. |
| `TestMultiInsertSyntaxErrors` | rejected: `INSERT FIRST` without WHEN, missing source, ELSE without WHEN, WHEN without THEN, plain `INSERT` with two INTOs, the documented `into t (select ...)` ambiguity. |

The grammar build itself is a test: `make mysql_sql.go` fails on any new
shift/reduce conflict.

### Layer 2 — planner: `pkg/sql/plan/bind_multi_insert_test.go`

Mock catalog tables used: `dept` (pk + unique + secondary index, nullable
columns), `t2` (pk, NOT NULL `b`), `t3` (pk only), `emp` (has a foreign key),
`docs_ft` (fulltext index; added to `mock.go` for this feature). Note the mock
resolves tables by bare name, so names must be unique across mock schemas.

| Test | Asserts |
|---|---|
| `TestMultiInsertUnconditionalFansOutOverOneSink` | steps `[SINK, MULTI_UPDATE, MULTI_UPDATE]`; 1 SINK, 2 SINK_SCAN, 2 MULTI_UPDATE, 0 FILTER; `dept`'s MULTI_UPDATE has 3 update contexts (base + 2 index tables), `t2`'s has 1; `StmtType == INSERT`; plan deep-copies. |
| `TestMultiInsertFirstAndElseRouting` | 3 targets → 4 steps; each target step has exactly one FILTER with `[1, 2, 2]` conjuncts; ELSE conjuncts are all `isnottrue`; the second WHEN carries its own `<` plus one `isnottrue`. |
| `TestMultiInsertAllConditionalDoesNotExcludeEarlierBranches` | with `INSERT ALL`, every branch keeps exactly one conjunct (no negation of earlier WHENs). |
| `TestMultiInsertPositionalTargetUsesEverySourceColumn` | `INTO t` without VALUES over a source with matching width plans. |
| `TestMultiInsertWithClauseFeedsSource` | `WITH ... INSERT ALL ... SELECT * FROM cte` plans with 2 steps. |
| `TestMultiInsertSameTableMergesIntoOneWritePipeline` | two clauses on `dept` → steps `[SINK, MULTI_UPDATE]`; 2 SINK_SCAN, 2 FILTER, exactly 1 UNION_ALL and 1 MULTI_UPDATE; union width = union of the clauses' column lists; the merged pipeline still writes the 3 index/base tables. |
| `TestMultiInsertSameTableMixedColumnListsRejectsMissingNotNull` | widening a clause that omits a NOT NULL column without default fails with the single-insert error text. |
| `TestMultiInsertKeepsIrregularIndexMaintenance` | for a fulltext target, single-clause and merged two-clause forms both produce exactly one `fulltext_index_tokenize` FUNCTION_SCAN and more than 2 steps. |
| `TestMultiInsertRejectsUnsupportedTargets` | FK target ("foreign key"), VALUES/column count mismatch, positional width mismatch, unknown source column in VALUES and in WHEN, unknown target column. |

### Layer 3 — BVT: `test/distributed/cases/dml/insert/multi_insert.sql`

79 statements with checked-in expected results (`multi_insert.result`,
generated with mo-tester `-m genrs`, then verified in compare mode).

| Group | Cases |
|---|---|
| Unconditional | positional INTO plus INTO with column list + VALUES expressions; target with auto-increment pk and a DEFAULT column. |
| Conditional ALL + ELSE | the issue's region example; NULL region goes to ELSE. |
| ALL vs FIRST | overlapping WHENs: ALL writes the row to both targets, FIRST to the first only; ELSE receives the rest. |
| Composition | several INTOs under one WHEN, `upper()`/`lower()`/arithmetic in VALUES, `WITH` source with `ORDER BY ... LIMIT`. |
| Constraints | duplicate primary key; duplicate unique key; a failing second target rolls the first target back (`count(*) = 0`). |
| Same table, several clauses | different column lists (defaults filled); overlapping keys across clauses → `Duplicate entry`; FIRST with the same table in both branches routes each row once; one clause sets the auto-increment key, the other lets the engine generate it. |
| Irregular indexes | fulltext target written by two merged clauses, ivfflat target; verified by `MATCH ... AGAINST` and a vector read afterwards. |
| Transactions | `BEGIN` / multi-insert / `ROLLBACK` leaves nothing. |
| Errors | value-count mismatch (explicit and positional), unknown source column in VALUES / WHEN, unknown target column, missing table, `INSERT FIRST` without WHEN, foreign-key target, external-table target. |
| EXPLAIN | one `Sink`, one `Sink Scan → Filter → Lock → DEDUP → Multi Update` per target. Targets without unique indexes are used on purpose: unique-index table names embed a UUID and would make the expected output non-deterministic. |

Run it with:

```
(cd $HOME/m/mo-tester; ./run.sh -p ../matrixone/test/distributed/cases/dml/insert/multi_insert.sql \
   -s ../matrixone/test/distributed/resources/ -n -g)
```

### Regression protection for the shared insert path

- `go test ./pkg/sql/plan/` — full package (the only failure is the
  pre-existing, timezone-dependent
  `TestBuildCreateOrReplaceViewRejectsRecursiveDefinition/future_AS_OF_timestamp`,
  which fails identically on `main`).
- The whole `test/distributed/cases/dml/insert` directory in compare mode
  (2000/2000 on the feature build) — covers `INSERT`, `INSERT IGNORE`,
  ON DUPLICATE KEY, auto-pk, defaults, CHECK constraints, which all flow
  through the refactored cast tail.
- `go test ./pkg/frontend/` subset around the touched switch sites
  (`Remap|Privilege|StmtKind|...`), `go vet` and `golangci-lint` on
  `parsers/tree`, `plan`, `frontend`.

### Verified manually (not encoded in a test file)

On the isolated single-node server:

- affected rows = sum over targets (`9 rows affected` for 3 rows × 3 targets);
  `LAST_INSERT_ID()` after a multi-insert into an auto-increment target;
- `PREPARE ... FROM 'insert all ... values (concat(v, ?)) ... where id = ?'`
  / `EXECUTE ... USING` with parameters in both VALUES and the source;
- privilege: a role with INSERT on only one of two targets can run a
  multi-insert into the permitted table, and is denied — with nothing written
  to either table — when the statement also names the other;
- ambiguous source headings (`select id, id`) are rejected as ambiguous.

### Regression guards — why specific tests exist

| Bug found during development | Guard |
|---|---|
| Same table in two clauses silently inserted duplicate primary keys (each pipeline dedups against the statement snapshot only). | `TestMultiInsertSameTableMergesIntoOneWritePipeline` (structure) + BVT "overlapping keys across clauses → Duplicate entry" (behaviour). |
| `getIrregularIndexes` called after `appendNodesForInsertStmt` (which strips them) → fulltext/IVF maintenance steps vanished with no error. | `TestMultiInsertKeepsIrregularIndexMaintenance` + BVT fulltext/ivfflat group. |
| `SINK_SCAN` binding not visible to unqualified names → "column X does not exist". | every VALUES/WHEN case in all three layers. |
| Missing `StmtKind()` on the new AST node → nil-pointer panic in `executeStmt`. | any BVT statement (the whole file would panic). |
| UUID index-table names in EXPLAIN output. | EXPLAIN case restricted to non-unique targets; result compared in mo-tester `-n -g` mode. |

### Known gaps / suggested additions before merge

- A BVT privilege case (needs an account/role fixture like
  `test/distributed/cases/prepare/prepare.test`) and a BVT `PREPARE`/`EXECUTE`
  case, to turn the manual checks into checked-in ones.
- A run on the multi-CN docker cluster (`make dev-up`): all server-side
  testing was on the single-node instance. The design is per-CN pipelines
  fed by local channels, the same mechanism the IVF/fulltext maintenance
  already relies on, but the remote-scope path
  (`Remote` scopes with a `Multi Update` root, `addAllAffectedRows`) has not
  been exercised by this feature's tests.
- Large-source behaviour (memory stays at one batch per consumer edge by
  construction — see "Runtime" — but no benchmark was run).

## Possible follow-ups

- `INSERT OVERWRITE ALL` (Snowflake truncates targets first).
- Allow a parenthesized source after a bare `INTO` via a lexer-level
  lookahead instead of the `%prec` resolution.
- Foreign-key targets: reuse `bindAndOptimizeInsertQuery`'s in-plan
  parent-existence checks per target.
