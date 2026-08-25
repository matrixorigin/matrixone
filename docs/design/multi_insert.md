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

### Routing: one WHEN occurrence is one route decision

Each `WHEN` is bound and evaluated **exactly once**, in a projection directly
above the source and below the shared `SINK`, and materialized as one boolean
**selector column** appended after the source columns (`appendMultiInsertSelectors`).
Targets never re-bind a predicate; their `FILTER` reads the selector columns:

| statement                     | filter on the branch                                   |
|-------------------------------|--------------------------------------------------------|
| `INSERT ALL WHEN`             | `sel_i`                                                |
| `INSERT FIRST WHEN`           | `sel_i AND sel_1 IS NOT TRUE AND ... AND sel_{i-1} IS NOT TRUE` |
| `ELSE`                        | `sel_1 IS NOT TRUE AND ... AND sel_n IS NOT TRUE`      |
| unconditional `INSERT ALL`    | none                                                   |

`IS NOT TRUE` (function `isnottrue`) rather than `NOT` so that a NULL
condition is treated as "did not match": the row stays eligible for later
`WHEN`s and for `ELSE`, matching Snowflake.

Materializing the decision is not an optimization, it is required for
correctness. An earlier version copied the `WHEN` AST into every branch and
bound it again per target. With a volatile predicate the branches then made
*independent* decisions for the same row: measured on 1000 rows with
`rand() < 0.5`, `INSERT FIRST` put 265 rows in **both** targets and ~262 in
neither (total 1003 instead of 1000), and two `INTO` clauses under one `WHEN`
received completely different row sets. Evaluating once also removes the
O(rows × branches) predicate cost and the duplicated subquery plans that the
per-branch binding caused.

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

### Alternative considered and not chosen: chained inserts in one pipeline

An alternative design is to have no sink at all: put every target into a
**single pipeline** and stream each batch through the targets in turn, each
one writing the rows it wants and passing the batch on. In its literal form
this is `Insert(t1) → Insert(t2) → ... → Insert(tN)`, with `INSERT FIRST`
dropping a row from the batch once a target has taken it and `INSERT ALL`
passing the whole batch untouched.

In MatrixOne terms a "target" is not one operator but `PROJECT → PreInsert →
Lock → DEDUP join(s) → MULTI_UPDATE`, so the practical form of this idea is
the shape MERGE INTO already uses: **one `MULTI_UPDATE` node holding every
target as an update context, with one selector column per target**:

```
Source
  -> PROJECT   (every target's cast/default/serial columns side by side,
                plus sel_1 .. sel_N; sel_i = cond_i for ALL,
                cond_i AND NOT (sel_1 OR .. OR sel_{i-1}) for FIRST,
                NOT (sel_1 OR .. OR sel_N) for ELSE)
  -> PreInsert x N  -> Lock x N
  -> DEDUP join per (target, key), probing only the rows with sel_i
  -> MULTI_UPDATE { ctx_1 .. ctx_N, ctx_i filtered by sel_i }
```

`Multi Update` already routes rows to contexts by selector
(`filterTargetRows`, `TargetUpdateCtxIdx`, used by MERGE), keys its in-batch
dedup (`seenTargetRows`) by table id, and keeps per-table S3 sinkers, so most
of the operator plumbing exists.

Benefits over the sink design:

- **Conditions evaluated once per row.** Today target *i* re-evaluates
  `cond_1 .. cond_{i-1}` as `IS NOT TRUE` filters, so `INSERT FIRST` costs
  O(rows × branches) condition evaluations; selectors make it O(rows).
- **No fan-out machinery.** One goroutine and no channel handoff per batch
  per target; the batch flows through once. Lower per-row overhead,
  especially with many small targets.
- **Natural FIRST/ALL/ELSE.** Routing is data (selector columns) instead of
  plan structure, and a row never needs to be physically removed from a
  batch — each context just ignores rows whose selector is false.
- **Same-table clauses dedup for free** within the operator: contexts for
  the same table share `seenTargetRows`.

Costs and risks, which are why it was not chosen for the first version:

- **The "no memory copy" gain is smaller than it looks.** The sink design
  already shares one ref-counted batch across all consumers (the dispatch
  spool duplicates nothing), and the write path copies in both designs:
  `insert_main_table` extends every row into a per-table off-heap buffer,
  and the S3 writer into per-table sorted sinkers, before writing. The chain
  saves a channel handoff per batch, not a data copy. Physically dropping
  rows for FIRST would in fact *add* a copy (the batch may still be pinned
  by the S3 writer or the spool, so it cannot be shrunk in place).
- **Loses cross-target parallelism.** The sink design runs N target
  pipelines concurrently — dedup probes, locking, sorting and encoding for
  S3 overlap across targets. One pipeline does the N targets sequentially
  per batch (its S3-mode `mcpu` split still parallelizes within the
  pipeline). Which effect dominates is a benchmark question, not obvious.
- **One S3-mode decision for all targets.** The write mode is chosen per
  `MULTI_UPDATE` node from the planner's estimate. With one node, the skew
  case (300k rows to one table, 3 to another) puts the 3-row target on the
  S3 path and writes tiny objects — a condition MatrixOne explicitly flags
  (`LogCNFlushSmallObjs`). Per-context write modes would have to be added
  to the writer.
- **Long, wide pipelines.** N targets × k keys means N·k chained DEDUP hash
  builds, N locks, N PreInserts and a projection carrying every target's
  columns side by side; ten targets with two unique keys each is twenty
  chained joins in one scope. The per-target shape keeps each target's
  runtime-filter, shuffle and remap handling identical to a single INSERT.
- **Same table in several clauses still multiplies rows.** With `INSERT
  ALL`, one source row can legitimately become two rows of the same table;
  selector routing is one-row-in/one-row-out per context, so such clauses
  remain separate contexts and the widening logic stays.
- **Fulltext / IVF maintenance still needs a materialized row image**, so
  the sink machinery cannot be removed anyway.
- **It is operator and compiler work, not just planner work.** The sink
  design touched no operator and inherits every single-table behaviour by
  construction, which is what made it verifiable quickly (415 BVT
  statements). The MERGE-style node needs multi-table selectors on the
  insert path of `Multi Update`, per-context S3 modes, pass-through of the
  pre-cast columns, and re-verification of the single-table INSERT path
  that shares the operator. The literal "insert node streaming into the
  next insert node" variant is weaker still: `Multi Update` in S3 mode
  emits flush-info batches to a merge scope, and an operator has one output
  stream, so data cannot continue to the next target past that merge
  without restructuring flush-info collection.

**Decision: the sink / per-target-pipeline design is what is implemented.**
It was chosen because it reuses the single-table insert tail unchanged
(correctness inherited, no operator changes), gives per-target parallelism
and per-target S3 decisions, and handles skew, same-table clauses and
irregular indexes with the machinery the engine already has. The chained,
selector-based design remains the natural evolution if profiling shows the
per-target fan-out or the repeated condition evaluation to be a bottleneck:
it can be added as a second planning mode for targets with only regular
indexes (falling back to the sink shape for fulltext/IVF targets and
same-table clauses), and the BVT suite is design-independent, so it would
validate that mode as-is. Any such switch should be driven by a benchmark on
the `multi_insert_big.sql` / `multi_insert_skew.sql` shapes — many small
targets versus few big indexed targets — rather than by reasoning alone.

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

### Layer 3 — BVT: `test/distributed/cases/dml/insert/multi_insert*.sql`

Five files, 415 statements, with checked-in expected results (generated with
mo-tester `-m genrs`, then verified in compare mode `-n -g`).

Coverage at a glance (P = parser unit test, L = planner unit test, and the
BVT file(s); "manual" = verified by hand only, see below):

| Behaviour | Where |
|---|---|
| Grammar forms, round trip, syntax errors | P |
| Unconditional `INSERT ALL`, positional and explicit column lists | L, `multi_insert`, `_schema`, `_big` |
| `INSERT ALL` with overlapping WHENs (row to every match) | L, `multi_insert`, `_conditional`, `_big`, `_skew` |
| `INSERT FIRST` (row to first match only) | L, `multi_insert`, `_conditional`, `_big`, `_skew` |
| ELSE: receives non-matching rows / never fires / receives everything | `_conditional`, `_skew` |
| NULL conditions never match (routed to ELSE or dropped) | `multi_insert`, `_conditional` |
| Nothing matches, no ELSE; empty source | `_conditional`, `_skew` |
| Condition shapes: subquery, LIKE/BETWEEN/IN/IS NULL, computed aliases, CASE in VALUES | `_conditional` |
| Several INTOs per WHEN; same table under several WHENs | `multi_insert`, `_conditional`, `_skew` |
| Source shapes: WITH, aggregate, join, UNION ALL, ORDER BY/LIMIT, DISTINCT | L (WITH), `multi_insert`, `_conditional` |
| WHEN/VALUES bind to source *output* columns, not table aliases | `_conditional` (negative case) |
| Same table in several clauses → one pipeline; cross-clause duplicates rejected | L, `multi_insert`, `_big`, `_skew` |
| Widening different column lists (defaults, auto-increment NULL → generated) | L, `multi_insert`, `_schema` |
| Different target schemas: narrower/wider/re-ordered/re-typed, conversions (json, enum, binary, date/time) | `_schema` |
| Constraints per target: pk, unique, composite pk, fake pk, NOT NULL, CHECK, generated, CLUSTER BY, auto-increment | L (pk/unique), `multi_insert`, `_schema` |
| Regular index tables maintained by the target's MULTI_UPDATE | L, `multi_insert`, `_big`, `_skew` |
| Fulltext / ivfflat maintenance (single and merged clauses) | L, `multi_insert` |
| Atomicity: one failing target rolls back all; late failure after S3 flush | `multi_insert`, `_conditional`, `_big`, `_skew` |
| Explicit transactions (ROLLBACK / COMMIT) | `multi_insert`, `_conditional`, `_big` |
| S3-object write path, object presence per target, durability after flush | `_big`, `_skew` |
| Extreme imbalance between targets, empty targets | `_skew` |
| Targets in another database, temporary target, target = source | `_schema` |
| Case-insensitive / qualified target column lists | `_schema` |
| Rejections: FK target, external table, view, unknown columns, count mismatch, generated column as target | L, `multi_insert`, `_schema` |
| EXPLAIN shape (Sink + per-target Sink Scan) | `multi_insert` |
| Affected rows = sum over targets; `LAST_INSERT_ID()` | manual |
| PREPARE / EXECUTE with parameters | manual |
| INSERT privilege required on every target | manual |

**`multi_insert.sql` (79) — smoke coverage of every feature.**

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

**`multi_insert_conditional.sql` (107) — routing semantics on a 1000-row
deterministic source** (`generate_series`; `cat = id % 5`, `val = 3*id`,
`score` NULL for every 10th row). Every assertion is self-checking: the
target count is compared with the equivalent plain filtered `SELECT`, or the
targets are joined to prove overlap/disjointness.

| Group | Cases |
|---|---|
| `INSERT ALL`, overlapping WHENs | three overlapping conditions (`val % 2`, `val % 3`, `id > 900`) + ELSE: each target equals its filtered SELECT; joins between targets prove rows landed in several; the always-true middle WHEN makes ELSE empty; total written = 1600 for 1000 source rows. |
| `INSERT FIRST`, same WHENs | targets are pairwise disjoint, their union covers the source exactly once, the shadowed third WHEN gets nothing. |
| ELSE and NULL conditions | `score > 90` / `score < 10` / ELSE: the 100 NULL scores all reach ELSE (`coalesce(score, -1)`); with `INSERT ALL` and no ELSE the NULL rows are dropped; an ELSE-only statement (`WHEN id < 0`). |
| Condition shapes | `IN (subquery)`, `LIKE ... AND ... BETWEEN`, `IS NULL OR mod()`, conditions on computed source aliases (`bucket`, `doubled`), `CASE` in VALUES. |
| Clause layout | several INTOs under one WHEN, the same table under several WHENs (one gets `val`, the other `-val`), an auto-increment audit table under every branch. |
| Source shapes | `GROUP BY` aggregate, `LEFT JOIN` (and the negative case: WHEN sees output columns, not table aliases — `v.id` is an error, `vid` works), `UNION ALL`, `ORDER BY ... LIMIT`, `DISTINCT`, empty source. |
| Atomicity / transactions | a duplicate in the second branch rolls back the first branch; `BEGIN ... ROLLBACK` and `BEGIN ... COMMIT` with counts checked inside and after the transaction. |

**`multi_insert_schema.sql` (74) — destination tables with different
schemas** from one 6-column source (`int`, `varchar`, `decimal`, `datetime`,
`bool`, `text`, with NULLs).

| Group | Cases |
|---|---|
| Shape | narrower, wider (defaults, `current_timestamp`), re-ordered, and re-typed targets (`char`, `int` from `decimal`, `varchar` from `datetime`/`decimal`, `day()`), all in one statement. |
| Constraints per target | unique + secondary index, composite pk, no pk (fake pk, written by two clauses), auto-increment, NOT NULL with default, CHECK, stored generated column, CLUSTER BY. CHECK violation on one target fails the whole statement; NOT NULL enforced; a generated column cannot be a target column. |
| Conversions | `json_object`/`json_array` into JSON, strings into ENUM (and an out-of-range enum rejected), `varbinary`, `date`/`time` from `datetime`, `float`; too-long string rejected, `left()` accepted. |
| Placement | target in another database, temporary table target, a target that is also the source (source rows are read once, before the writes). |
| Names | case-insensitive column lists, table-qualified column lists accepted, wrong qualifier rejected. |
| Same table, four column lists | widened to the union of the lists; unset columns get their defaults. |
| Errors | value count mismatch, unknown column, missing table, view target. |

**`multi_insert_big.sql` (73) — a source big enough to make every target
write S3 objects.** 300k rows × ~520 bytes ≈ 150 MB per wide target; runs in
~8 s.

How a target ends up on the S3 path: `compileMultiUpdate` picks the write
mode per `MULTI_UPDATE` node at compile time from the planner's estimate —
`Stats.Outcnt × SingleLineSizeEstimate (300 B) > DistributedThreshold (10 MB)`,
i.e. roughly 35k estimated rows — and an S3-mode writer then flushes every
`InsertWriteS3Threshold` (64 MB) of buffered rows and the tail at the end. So
the decision follows the *estimated* row count of each target's branch, not
the byte volume; a narrow 300k-row target is on the S3 path too, while a
branch the planner estimates at a few rows stays in memory. Object presence
is observed with `metadata_scan('db.t', 'id')` (committed objects only:
0 for a small insert, 0 after a rollback).

| Group | Cases |
|---|---|
| Unconditional, 4 targets | pk-only, pk + unique + secondary index, composite pk, and a narrow (id, cat) table that stays far below the threshold. Counts, sums, min/max and `sum(length(pad))` per target. |
| S3 objects really written | `metadata_scan('db.t', 'id')` reports committed objects with `count(*) > 0` and `sum(rows_cnt) = 300000` for all four targets, the narrow one included. |
| Indexes at scale | point lookup through the unique key, counts through the secondary key and the composite pk. |
| FIRST vs ALL at scale | `cat = 0` / `cat < 3` / ELSE over 300k rows: FIRST partitions the source (42857 / 85715 / 171428, checked against filtered SELECTs), ALL overlaps (the `cat = 0` rows appear in two targets). |
| Same big table, two clauses | 600k rows through one merged pipeline, objects present; a duplicate-key variant is rejected and adds nothing. |
| Late failure rolls back written objects | a duplicate at id 299999 fails the statement after the other target has already flushed; that target ends with 0 rows and 0 objects. |
| Durability | `mo_ctl('dn','flush', ...)` on the targets, then counts/sums/lookups re-read. |
| Transaction | a big multi-insert inside `BEGIN ... ROLLBACK` leaves the targets unchanged. |

**`multi_insert_skew.sql` (82) — extreme imbalance between targets** on the same
300k-row source, plus small-scale replicas of every shape. Object checks are
made only on the two deterministic kinds of target — a dominant one the
planner estimates at (almost) all rows (`has_objects = 1`,
`sum(rows_cnt)` = its count) and an empty one (0 objects in either mode) —
never on a 1- or 3-row target, whose mode depends on selectivity estimates.

| Group | Cases |
|---|---|
| FIRST, one dominant WHEN | `id in (1,2,3)` → 3 rows, `id = 300000` → 1 row, `id > 0` → the other 299,996, a never-true WHEN (`id < 0`) → 0 rows, ELSE → 0 rows; the union covers the source once; the dominant target has objects for every row, the never-hit and ELSE targets have none; indexes of the dominant target answer lookups, those of the empty target answer nothing. |
| ALL, only the dominant WHEN matches | `cat >= 0` (all rows) + two impossible WHENs (`cat > 100`, `pad is null`), no ELSE. |
| ELSE takes everything | no WHEN ever matches; ELSE holds all 300k rows with objects. |
| Nothing matches, no ELSE | `INSERT ALL` and `INSERT FIRST` succeed and write nothing; unconditional `INSERT ALL` over an empty source leaves every target empty. |
| Skew the other way | one row to the wide table, 299,999 to a narrow `(id, cat)` table (which is on the S3 path by estimate). |
| Same table, dominant + empty clause | `cat >= 0` and `cat < 0` into the same table: one merged pipeline, 300k rows, objects present. |
| Empty targets still constrained | a target that received nothing in earlier statements rejects a duplicate when it finally gets a row, rolling back the sibling branch. |
| Small scale | 5-row source: FIRST with an unused ELSE, ALL where only ELSE fires, FIRST with no ELSE and one branch. |

Run it with:

```
(cd $HOME/m/mo-tester; ./run.sh -p ../matrixone/test/distributed/cases/dml/insert \
   -s ../matrixone/test/distributed/resources/ -n -g)      # whole directory, or one multi_insert*.sql
```

### Adding a new statement type — the closure that must be swept

A new `tree.Statement` is only "wired up" once every statement-type switch
in the engine has been considered. For this feature the sweep covered, and
the entries were needed in: `frontend/stmt_kind.go` (DML classification),
`authenticate.go` (privilege; its `default:` **panics**), `status_stmt.go`
(`last_insert_id`), `remap_db.go`, `mysql_cmd_executor.go` (buildPlan list),
`plan/build.go` + `build_dcl.go` (dispatch and PREPARE), `compile2.go` (both
vector-search auto-mode rewrites), `parsers/sqlparse.go` (rewrite policy —
the bypass above), plus `StmtKind()` on the node itself.

Deliberately *not* needed, with the reason: the VALUES-only fast paths
(no VALUES-only form exists for multi-insert, and both fall through to the
general lists), `back_exec.go` restore hooks (restore SQL is generated
internally and is always single-table `INSERT ... VALUES`),
`isIgnoreStatement` (no `INSERT ALL IGNORE` in the grammar),
`returning.go` (grammar forbids RETURNING here), and the empty-privilege-tips
fallback in `authenticate.go` (a multi-insert plan always yields at least one
tip per target). Audit/statement_info/metrics and txn handling key off
`GetStatementType()`/`GetQueryType()`, which report `"Insert"`/`"DML"`, so
they were correct without changes. `pkg/cdc`, `pkg/backup`, `pkg/cnservice`
and `pkg/util` contain no statement type switches at all.

### Adding or regenerating BVT cases

- Generate expected results against a server that runs the feature build
  (the persistent docker cluster may not), e.g. the isolated single-node
  instance on port 6101: point `~/m/mo-tester/mo.yml` `addr:` at it, run
  `./run.sh -p <file> -s ../matrixone/test/distributed/resources/ -m genrs`,
  then run the same file with `-n -g` to confirm the recorded result is
  reproducible, and restore `mo.yml`.
- `genrs` records error messages as expected output. Read the generated
  `.result` and make sure every recorded error is an intended one.
- Keep results deterministic:
  - never `EXPLAIN` a target with a unique index — its index table name
    embeds a UUID;
  - assert `metadata_scan` object presence only on targets the planner will
    estimate as dominant (`has_objects = 1`, `sum(rows_cnt)` = the count) or
    on empty targets (`0`); a target of a handful of rows may be planned on
    either path;
  - prefer self-checking assertions (`(select count(*) from t) = (select
    count(*) from src where ...)`, joins between targets) over literal
    numbers where the number is not obviously derivable;
  - build big sources with `generate_series(1, N)` + `repeat('p', 500)`, and
    make the source itself large enough to be object-backed so its
    statistics — and therefore each target's write-mode decision — are
    stable.
- MatrixOne specifics that bit while writing these files: `sum(bool)` is
  not supported (use `sum(case when ... then 1 else 0 end)`), and `WHEN` /
  `VALUES` cannot use the source query's table aliases (`v.id`), only its
  output column names — alias the column in the SELECT list.

### Regression protection for the shared insert path

- `go test ./pkg/sql/plan/` — full package (the only failure is the
  pre-existing, timezone-dependent
  `TestBuildCreateOrReplaceViewRejectsRecursiveDefinition/future_AS_OF_timestamp`,
  which fails identically on `main`).
- The whole `test/distributed/cases/dml/insert` directory in compare mode
  (2000/2000 pre-existing statements on the feature build, plus the 415 new
  ones) — covers `INSERT`, `INSERT IGNORE`, ON DUPLICATE KEY, auto-pk,
  defaults, CHECK constraints, which all flow through the refactored cast
  tail.
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
| Test comments assumed the 64 MB byte threshold decides the S3 path; the mode is actually chosen per target from the planner's row estimate, so small branches may land on either path. | `has_objects` assertions limited to dominant and empty targets (`_big`, `_skew`); rule documented above. |
| A "late failure" case whose arithmetic never produced the conflict (id 999 had `cat = 4`). | Case rewritten around id 996 and its expected `Duplicate entry` is recorded in `_conditional.result`. |

### Known gaps / suggested additions before merge

- A BVT privilege case (needs an account/role fixture like
  `test/distributed/cases/prepare/prepare.test`) and a BVT `PREPARE`/`EXECUTE`
  case, to turn the manual checks into checked-in ones.
- The S3-object assertions in `multi_insert_big.sql` / `multi_insert_skew.sql`
  depend on the planner estimating each dominant branch at more than ~35k
  rows (the `DistributedThreshold` rule above). The source table is itself
  written as S3 objects, so its statistics are derived from object metadata
  and the estimate is stable; if the source were small enough to stay in
  memory, or the thresholds changed, the `has_objects` expectations would
  need revisiting.
- A run on the multi-CN docker cluster (`make dev-up`): all server-side
  testing was on the single-node instance. The design is per-CN pipelines
  fed by local channels, the same mechanism the IVF/fulltext maintenance
  already relies on, but the remote-scope path
  (`Remote` scopes with a `Multi Update` root, `addAllAffectedRows`) has not
  been exercised by this feature's tests.
- Large-source performance: `multi_insert_big.sql` proves the S3 path and
  correctness at 300k rows (memory stays at one batch per consumer edge by
  construction — see "Runtime"), but no throughput benchmark was run.

## Self-review decision log

Recorded from the pre-merge self-review of PR #27560 (multi-angle sweep and
functional closure parser → planner → compile → frontend → tests), so later
review rounds do not re-litigate them.

Fixed after maintainer review (XuPeng-SH, CHANGES_REQUESTED):

- **Route decisions were re-evaluated per target.** `WHEN` conditions were
  copied into every branch and bound again, so one `WHEN` occurrence was not
  one route decision. Volatile predicates therefore broke both documented
  guarantees — `INSERT FIRST` stopped being a partition and the `INTO`
  clauses of a single `WHEN` disagreed (reproduced above). Fixed by
  evaluating each `WHEN` once above the shared sink into a boolean selector
  column that every target consumes; see "Routing" above. Regressions added
  per the review request: `TestMultiInsertEvaluatesEachWhenOnce` (each
  predicate bound exactly once; no `FILTER` re-evaluates one) and BVT cases
  asserting that two `INTO`s under one volatile `WHEN` receive identical key
  sets, and that volatile `INSERT FIRST` targets are disjoint and cover the
  source exactly once.

Fixed during self-review:

- **Access-control bypass:** `AddRewriteHints` (`pkg/sql/parsers/sqlparse.go`)
  attaches the table→query rewrite policy to the statement that reads tables;
  `*tree.Insert` has an explicit case for its inner `SELECT`, but
  `*tree.MultiInsert` fell into the `default:` branch that frees the option,
  so `INSERT ALL/FIRST ... SELECT` read the raw base table and ignored role
  rules, `remap_rewrites`, and inline rewrite hints (prepared statements
  included). Now attached to `MultiInsert.Source` like the `Insert` case;
  regression test `TestAddRewriteHints_AttachesToDMLSourceQuery`.
  (`RemapDb` uses a different channel and was already handled.)
- `remap_db.go` remapped a target's schema twice (table expr remap plus
  `remapInsertTarget` on the same field); now only the qualified column names
  go through `remapInsertTarget`. Unit test added.
- `compile2.go`'s vector-search auto-mode rewrites (`rewriteAutoModeToPre`,
  `forceModePre`) did not reach a `MultiInsert`'s source query. Added, with
  a unit test.

From the Opus correctness pass (both reproduced on a live server):

- **Crash:** the branch `SINK_SCAN` was not registered in
  `builder.positionalSinkScans`, so when `createQuery` pruned the columns no
  branch referenced from the shared sink, the branch kept pre-prune positions
  into a narrower batch and panicked with index-out-of-range. Reachable from
  ordinary SQL (`insert all into t(a) values(c2) select c1, c2 from s`). The
  BVT suite missed it because every case happened to reference all source
  columns; `TestMultiInsertBranchSinkScansAreRepositionedAfterSinkPruning`
  now covers it and was verified to fail without the registration.
- **Nondeterministic auto-increment:** in a merged same-table group, a clause
  that omitted an `AUTO_INCREMENT` column raced the clause that supplied it
  (1 run in 8 gave a duplicate key; silent collisions without a primary key).
  The underlying `PRE_INSERT` race is pre-existing for hand-written
  `UNION ALL` inserts, but multi-insert synthesizes the union. The
  combination is now rejected; all-explicit and all-generated stay supported,
  distinct tables are unaffected. One BVT case relied on the racy mix and was
  replaced by the three deterministic shapes.

From the Opus unhappy-path pass (no hang, leak, double-free or goroutine
growth found — see below — but three hardening gaps):

- INTO clauses are capped at 127 (as Oracle): each adds a write pipeline with
  its own dedup hash build over the whole source, and 60 targets x 100k rows
  moved RSS from 0.96 GB to 3.72 GB.
- The per-table merge key is the resolved table id, not the lower-cased name:
  under `lower_case_table_names=0` the old key would collapse two
  case-distinct tables and write a clause's rows to the wrong table.
- `IgnoreForeignKey` is restored with `defer`, so a panic in `ResolveTables`
  cannot leave FK rejection disabled on the shared CompilerContext.

Accepted as designed (with the reason):

- The statement-level `WITH` is moved onto the source `SELECT` by mutating
  the AST, as `bindInsert` does; idempotent, so PREPARE/EXECUTE re-binding
  is safe.
- Clauses are grouped by the resolved, lower-cased `schema.table`, so `t`,
  `db.t` and `DB.T` share one pipeline (verified: overlapping keys raise
  `Duplicate entry`).
- Parenthesized source directly after a bare `INTO t` is a syntax error
  (`%prec LOWER_THAN_LPAREN`); tables named `first`/`all` keep working with
  plain `INSERT`.
- S3-object assertions in the BVT are limited to dominant and empty
  targets because the write mode is an estimate-based compile-time decision.

Execution-safety audit (unhappy-path pass, recorded so it is not repeated):
a failing target cannot block the dispatcher or its siblings — `merge.Reset`
keeps draining its edge on a detached cleanup context, then `runOnce` cancels
every scope; the DEDUP(FAIL) join builds its hashmap on the sink-scan side, so
every consumer fully drains the sink before it can raise an error (this is why
the lock-step spool is safe here, and why the synthesized UNION ALL must stay
eagerly compiled — noted in the code). Batches are refcounted single copies and
`filter.shrinkWithSels` mutates only its own buffer, so per-branch WHEN filters
cannot corrupt siblings. Measured fail-fast: 16 targets over a 200k-row source
with one failing target errored in 2.9 s, and a mid-statement client kill left
zero leaked goroutines. Cross-target lock ordering is nondeterministic within a
statement — a new surface versus single-table INSERT — but the existing
deadlock detector handles it.

Verified without change: runtime errors in the source, in one branch's
`VALUES`, or in a `WHEN` fail the statement promptly with nothing written and
no hang (sink consumers terminate with the failing scope); partitioned
targets, expression defaults and `EXPLAIN ANALYZE` work; privilege entries
are per-table plan tips, so `writeDatabaseAndTableDirectly` only gates the
light-privilege path exactly as for `Insert`; the mock table ids added for
`docs_ft` do not collide.

## Possible follow-ups

- The chained, selector-based single-pipeline design described above, as a
  benchmark-driven second planning mode.
- `INSERT OVERWRITE ALL` (Snowflake truncates targets first).
- Allow a parenthesized source after a bare `INTO` via a lexer-level
  lookahead instead of the `%prec` resolution.
- Foreign-key targets: reuse `bindAndOptimizeInsertQuery`'s in-plan
  parent-existence checks per target.
