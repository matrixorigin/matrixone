ESQL / SQL External Tables
==========================

This is a design for two more external table engines, `ESQL` and `SQL`, that read
from a foreign database at query time:

```
CREATE EXTERNAL TABLE es_logs (
        ts        timestamp,
        host      varchar(100),
        status    int,
        msg       text
)
ENGINE = ESQL
WITH (
        'config' = '{"addresses":["https://es:9200"],"username":"elastic","password":"..."}'
);

CREATE EXTERNAL TABLE pg_orders (
        id        bigint,
        customer  varchar(64),
        amount    decimal(12,2),
        created   datetime
)
ENGINE = SQL
WITH (
        'config' = '{"driver":"postgres","dsn":"postgres://user:pw@pg:5432/shop"}'
);
```

They are the external-table twins of `esql_tvf` / `sql_tvf` (docs/cn/esql_tvf.md,
docs/cn/esql_tvf_plan.md).  The difference is only *where the schema and the
connection come from*:

| | `esql_tvf` / `sql_tvf` | `ENGINE = ESQL` / `ENGINE = SQL` |
|---|---|---|
| schema | runtime: 2nd TVF arg, parse_jsonl schema spec | static: the column list of `CREATE EXTERNAL TABLE` |
| connection | runtime: handle from `*_tvf_connect` or `@*_tvf_config` | static: table option `config` (or `@*_tvf_config` fallback) |
| query text | 1st TVF arg | predicate on the hidden column `__mo_query` |
| result transport, CSV parse, conn cache | `pkg/sql/foreigntvf` + `external.ForeignTVFReader` | **same code** |

Everything below the syntax layer is reused; the new parts are the DDL/catalog
seam (mirrors `ENGINE = DATASTREAM`) and the `__mo_query` pushdown (mirrors
`__mo_filepath`).


1. Syntax and table options
---------------------------

```
CREATE EXTERNAL TABLE name ( column_definition, ... )
ENGINE = { ESQL | SQL }
[ WITH ( option = 'value' [, ...] ) ]
```

Grammar mirrors `datastream_table_param` in mysql_sql.y: a dedicated production
`ENGINE equal_opt ESQL ...` / `ENGINE equal_opt SQL ...` (note `SQL` is already a
token; `ESQL` becomes a new non-reserved keyword).  Option keys are
case-insensitive, values are strings, duplicates are an error (same validator
shape as `sqldatastream.ParseTableOptions`).

| option | ESQL | SQL | meaning |
|---|---|---|---|
| `config` | optional | optional | The **same JSON** that `esql_tvf_connect(config)` / `sql_tvf_connect(config)` accept: a whitelisted elasticsearch config JSON for ESQL (`addresses`, `username`, `password`, `cloudid`, `apikey`, `servicetoken`, `certificatefingerprint`, `cacert` — lifecycle/global library knobs are rejected), `{"driver": "...", "dsn": "..."}` for SQL.  Passed verbatim to `foreigntvf.Connect(kind, configJSON)`.  All connection info comes from user input or the session — query processing never reads the CN process environment. If omitted, the scan uses `@esql_tvf_config` / `@sql_tvf_config` of the querying session (exactly `foreigntvf.ConfigFromSessionVar`); error if neither is set. |
| `query` | optional | optional | Default query text, used when a `SELECT` has no `__mo_query` predicate (see §2).  Plain text, no placeholders. |
| `pushdown` | rejected | optional | `false` (the default) sends the query text verbatim and MO evaluates every predicate itself; `true` wraps the text so the source applies the predicates MO can render, and MO then stops evaluating exactly those (§2.5).  Whatever could not be rendered stays local.  ESQL rejects the option. |

No other options.  Any `URL`/`FORMAT`/`INFILE` clause is rejected for these
engines, as for DATASTREAM.


2. `__mo_query`: the query is a pushed-down predicate
-----------------------------------------------------

### 2.1 The trick, restated from `__mo_filepath`

A CSV external table gets a hidden trailing column `__mo_filepath`
(`catalog.ExternalFilePath`, appended in `query_builder.go` for
`ExternType_EXTERNAL_TB`).  At compile time `external.FilterFileList`
(`filterByAccountAndFilename`) splits `node.FilterList` into *file-level*
conjuncts — those that reference only `__mo_filepath`, literals/params and
foldable functions (`isFileLevelFilter`/`classifyFileLevelColumns`) — and
*row-level* conjuncts.  File-level conjuncts are evaluated once against a batch
whose rows are the candidate file names, prune the file list, and are removed
from the row-level `FilterList`.  At read time the column is populated per row
with the current file name (`getFieldFromLine` →
`param.Fileparam.Filepath`).

`ENGINE = ESQL|SQL` tables get a hidden trailing column `__mo_query`
(`catalog.ExternalQuery`, new constant next to `ExternalFilePath`; registered
in `catalog.ContainExternalHidenCol`) of type varchar, and the **query text
plays the role of the file name**:

* one query text == one "file" to scan;
* the set of query texts is the "file list" of the scan;
* the per-row value of `__mo_query` is the text of the query that produced the
  row, populated by the very same `getFieldFromLine` path by setting
  `Fileparam.Filepath` to the query text before each virtual file is opened.

```
select ts, host, status
from   es_logs
where  __mo_query = 'FROM logs-* | WHERE status >= 500 | KEEP @timestamp, host, status, message'
and    host like 'web-%';
```

```
select * from pg_orders
where  __mo_query = 'select id, customer, amount, created from orders where created >= now() - interval ''1 day'''
and    amount > 100;
```

### 2.2 Deriving the query list

`__mo_filepath` has a file system to enumerate; `__mo_query` does not, so the
candidate list must be *derived from the predicate*.  At compile time, in the
ESQL/SQL branch of `getExternalFileListAndSize`:

1. Split `node.FilterList` with the existing classifier, parameterized on the
   hidden column (`isFileLevelFilter` gains the column identity as input, or a
   sibling `isQueryLevelFilter` that checks `catalog.ExternalQuery` /
   `ExternalQueryColId` — same body).
2. From the query-level conjuncts, derive the candidate list:
   * `__mo_query = <expr>` → one candidate, `<expr>` evaluated at compile time
     (it is query-level, hence constant-foldable: literals, session/prepare
     params, foldable functions such as `concat`, `date_format(now(), ...)` is
     **not** allowed since `now()` is real-time — identical rules to
     `isSafeFileLevelFunction` minus the `mo_log_date` special case).
   * `__mo_query IN (<e1>, <e2>, ...)` → several candidates;
   * `OR` of the above → the union;
   * any other query-level conjunct (e.g. `__mo_query LIKE '%logs%'`) does not
     generate candidates.
3. If no candidate was derived and the table has a `query` option → the list is
   that single text.  Otherwise error:
   `esql/sql external table requires a '__mo_query = <text>' predicate or a 'query' table option`.
4. Run the **unchanged** `external.FilterFileList` over the candidate list, with
   `__mo_query` in the `__mo_filepath` slot of `makeFilepathBatch` (last
   column).  This applies *all* query-level conjuncts (including the `=`/`IN`
   ones, which trivially pass, and the non-generating ones like `LIKE`, which
   may prune), returns the surviving query texts and the residual row-level
   `FilterList`.  Duplicates are removed (a query is run once).
5. The surviving list is the scan's `FileList`; `fileSize` is unused (`-1`).

Because the `=`/`IN` conjuncts are removed from the row-level filter list, the
foreign result is not re-filtered on `__mo_query`; and because every row carries
its query text, a user projecting `__mo_query` with `IN (...)` can tell which
query produced which row — the same user-visible behaviour as `__mo_filepath`
with a glob.

### 2.3 What the text is

`__mo_query` is **plain text**.  MO does not parse, rewrite, validate, or
parameterize it:

* no `?` placeholders and no bound parameters are sent to the source; the text
  is sent verbatim, as one statement, exactly as `esql_tvf`/`sql_tvf` send their
  first argument (`Conn.Query(ctx, text)`);
* MO-side constant folding may *produce* the text (`concat(...)`, a `@var`, a
  prepare parameter), but whatever it folds to is what the source receives;
* the text must return the declared columns, in declared order (§3);
* nothing else from the MO query (projections, other predicates, LIMIT) is
  pushed into the text, unless the table opted into predicate pushdown (§2.5),
  which wraps the text rather than rewriting it.

### 2.4 Ordinary predicates

By default all conjuncts that touch a declared column stay in
`node.FilterList` and are evaluated by MO on the returned rows, as for any
external table.  The user already wrote the foreign query, so narrowing it is
their job, in their dialect:

```
-- the selective way: the source never materializes the other rows
select * from orders where __mo_query = 'select id, name from src where id > 3';

-- the default way: the source returns every row and MO drops the rest
select * from orders where __mo_query = 'select id, name from src' and id > 3;
```

### 2.5 Predicate pushdown (`pushdown` = `true`, SQL only)

**Status: settled (revision 3).**  Revisions 1 and 2 are recorded at the end of
this section with the reason each was abandoned; this text describes what the
code does.

`ENGINE = SQL` can push the second form's predicates down for you.  It is
**opt-in**, off by default, and unavailable on `ENGINE = ESQL` (which rejects
the option rather than accepting a knob that would do nothing):

```
create external table orders (id bigint, name varchar(64))
engine = sql with ('config' = '...', 'pushdown' = 'true');
```

`compileForeignScan` renders the row-level conjuncts with
`datastream.DeparseFiltersBareIdents` (column refs, literals, comparisons,
`AND`/`OR`/`NOT`, `IN`, `BETWEEN`, `IS [NOT] NULL`, `LIKE` — anything else has
nothing to send and keeps its conjunct local) and wraps each query text as a
derived table:

```
select * from (
select id, name from src
) __mo_subq_1a2b3c4d where (id > 3)
```

#### The five contract points

| | rule |
|---|---|
| **Local evaluation** | Exactly the rendered conjuncts **leave** `node.FilterList` — the source owns them, MO does not re-apply them. Everything unrenderable stays local. |
| **Identifiers** | Written **bare**, never quoted. A name that cannot be written bare is not pushed. |
| **Failure** | A source error is the error. There is no second, unfiltered attempt. |
| **Result columns** | On success they must be the table's declared columns, in order, compared case-insensitively — or that is an error. |
| **Compatibility** | Off by default and inert for every table that does not ask; nothing about a non-opted-in table changes. |

#### Identifiers are never quoted

This is a decision, not an omission.  The quoting character is
dialect-specific — MySQL backticks, standard double quotes — and `sql_tvf`
speaks to PostgreSQL as well as MySQL, so quoting would mean either knowing the
dialect at render time (compile does not hold the connection) or serving only
MySQL.  A bare identifier is accepted by both.

The accepted cost is that a name which *cannot* be written bare is not pushed
at all.  `isBareIdentifier` requires `[A-Za-z_][A-Za-z0-9_]*` **and** refuses
the union of the words MySQL 8.0 and PostgreSQL reserve, so a column called
`order` keeps its conjunct local instead of producing `where order > 3`, which
no dialect parses.

Refusing rather than pushing-and-failing matters *because* pushed conjuncts
leave `FilterList`: a conjunct that is sent and rejected has no local path left,
so the predicate could never be answered at all.  A conjunct that is refused is
merely unoptimized.  Refusing on the **union** of both dialects is deliberate
for the same reason — the renderer does not know which source the text is bound
for, and over-refusing costs only the optimization.

The derived-table alias is MO's own (`__mo_subq_` + crc32 hex), spelled from
`[a-z0-9_]`, so it needs no quoting either.

#### Opting in means the source returns the declared columns

A foreign scan normally maps the result **positionally** (§3) and never reads
the source's column names, which is what makes this legal:

```
select * from orders where __mo_query = 'select a_id, a_name from src';
```

Pushdown cannot live with that freedom: a `WHERE` clause has to *name* what it
filters, and the only names MO has are its own.  So opting in is a statement
about the source — **that its result columns are the declared ones, by name**.

MO checks the claim rather than trusting it.  `database/sql` reports a result's
column names the moment the query returns, before any row is read, so the
reader gets them for free and compares them against the DDL, in order,
case-insensitively.  A mismatch is an **error**:

```
predicate pushdown ('pushdown' = 'true') requires the query to return the
table's declared columns, but column 1 is named "a_id" at the source and "id"
in the table; alias it, or drop the option to keep the positional mapping
```

It has to be an error, not a fallback: the source has already run a query MO
narrowed, and MO cannot tell whether the rows it is about to read were filtered
on the columns it meant.  For the same reason a source error is just an error.

The check reads `ColumnListLen` and each attr's `ColFieldIndex`, not
`param.Cols`, because those survive column pruning: the map they come from is
built before the synthetic columns are appended and before any pruning, so a
scan reading one column of four still knows the table is four wide and checks
its own column at its own position.  It runs for every scan of an opted-in
table, including one whose predicates were all unrenderable, so the contract
does not depend on which predicate a query happens to carry.

The synthetic columns (`__mo_query`, `__mo_filepath`, the error-mode columns)
are masked out of the deparser, so a conjunct touching one is never sent — the
source has no counterpart for it.

#### Dialect differences that remain, stated rather than papered over

| | MySQL | PostgreSQL |
|---|---|---|
| bare identifier in the wrapper | as written | folded to lower case |
| source projects `AS "ID"` for a column declared `id` | resolves; the case-insensitive check then passes | **fails at the source**: bare `id` folds to `id`, which does not match `ID` |
| source projects `AS ID` or `AS id` | resolves | resolves (both fold to `id`) |
| reserved-word column | not pushed (refused) | not pushed (refused) |

So on PostgreSQL, project lower-case or unquoted aliases.  The case-insensitive
check is a *post*-check and cannot repair a query that already failed; it exists
for the MySQL case, where the query succeeds and the names still have to agree.

#### Why it is opt-in

A pushed predicate is **not provably superset-preserving across engines**: the
source may drop a row MO would have kept under a different collation, time
zone, or coercion, and local evaluation cannot bring back a row that never
arrived.  This is the same reasoning as DATASTREAM's `recheck` option.

Two more things change under the wrapper, which is why a table has to ask for
it: a text that is a valid statement but an invalid derived table (a `SHOW`, a
`CALL`, a projection with duplicate column names) now fails, and an `ORDER BY`
inside a derived table is not guaranteed to survive.  A trailing `;` and a
trailing `-- comment` are handled — `WrapPushdownQuery` trims the first and
puts the closing paren on its own line for the second.

#### Compatibility and rollback

* the option defaults to **false**, and a `rel_createsql` envelope written
  before it existed has no `pushdown` field, which decodes to false — a
  pre-existing table keeps getting the verbatim query;
* `ForeignScan.pushdown` is the positive form, so the plan field's zero value
  is also "verbatim": a path that forgets to set it is safe;
* an older binary reading a newer envelope ignores the field it does not know;
* `SHOW CREATE TABLE` renders `"pushdown" = 'true'` only when a table opted in,
  so tables that never did keep rendering exactly the options their owner
  wrote;
* rollback is `alter`/recreate without the option, or setting it to `false`;
  nothing persists beyond the envelope field.

#### Validation matrix

| behaviour | where |
|---|---|
| wrapper shape, alias alphabet, `;` and `-- comment` tails | `pkg/sql/foreignext` unit |
| bare vs quoted rendering; reserved words refused (both dialects' unions); sql-ish but legal names still pushed; string literals keep their own quoting | `pkg/sql/datastream` unit |
| opted-in wraps and trims exactly the pushed conjunct; unrenderable stays local; nothing renderable ⇒ no wrapper; ESQL never wraps; synthetic column never sent | `pkg/sql/compile` unit |
| column check: exact, case-insensitive, wrong order, arity, no column-reporting capability, **pruned scans** | `pkg/sql/colexec/external` unit |
| end-to-end answers with and without pushdown, operators, query tails, `IN` lists, `SHOW CREATE`, option validation | BVT `function/foreign_exttab_pushdown.sql` (**MySQL only** — the BVT harness has no PostgreSQL source) |
| transfer actually reduced | `EXPLAIN ANALYZE`: external scan `inputRows=1` (84 B) with pushdown vs `inputRows=100` (8.20 KiB) without, same answer |

**Known gap:** PostgreSQL has no public-path BVT coverage, only unit coverage
of the rendering rules and the table above.

#### Revision history

1. **`'recheck' = 'false'`, no local recheck.**  Rendered MO's *declared* names
   and MySQL-quoted identifiers.  Abandoned: it broke the positional contract
   (a source with no column of that name received ``where `id` > 3``) and sent
   MySQL quoting to PostgreSQL sources.
2. **Probe + source-name mapping, conjuncts kept local.**  Probed
   `select * from (<text>) alias limit 0` for the source's own column names,
   rendered in its vocabulary, and fell back to the verbatim query whenever
   anything did not line up.  Correct, but it spent a round trip per query and
   an optional connection capability to *discover* a contract instead of
   stating one.
3. **Current.**  State the contract (the source returns the declared columns),
   check it against the names `database/sql` already hands back, and the
   discovery machinery disappears: probe and capability deleted, rendering back
   in compile, identifiers bare so both drivers are served.


3. Schema and result mapping
----------------------------

The column list of `CREATE EXTERNAL TABLE` is the schema; there is no runtime
schema spec and no no-schema/JSON-array mode.  Result mapping is identical to
the TVFs' schema mode:

* the foreign result is a CSV byte stream (`foreigntvf.Conn.Query`): ES|QL
  `_query?format=csv` (RFC 4180, one header line, empty = NULL) or the
  `sql.Rows` → MySQL-dialect CSV encoder (`\N` = NULL, no header);
* `external.BuildForeignTVFExternParam(proc, outCols, fullSchemaNames, src)`
  builds the synthetic `ExternalParam`, with `fullSchemaNames` = the table's
  declared (non-hidden) columns in DDL order, and `outCols` = the scan node's
  projected columns — so optimizer column pruning/reordering never misaligns
  fields, and the source must always return **all declared columns in DDL
  order** (MO cannot rewrite the query to drop columns);
* field i → declared column i; type coercion, NULL handling, and field-count
  mismatch errors are the CSV external table's (`getOneRowData`/`getColData`);
* `__mo_query` is filled by `getFieldFromLine` for the hidden column.

`ExternScan.TbColToDataCol` stays empty (positional), as for DATASTREAM.


4. Connection management
------------------------

Reused wholesale from `pkg/sql/foreigntvf`:

* `foreigntvf.Connect(kind, configJSON)` opens; `foreigntvf.ResolveOrConnect(ctx,
  cache, kind, configJSON)` reuses the session's cached connection keyed by
  `MakeHandle(kind, configJSON)`.  An external table and a TVF with the same
  config in the same session share one connection.
* The cache is the frontend `Session` (`process.ForeignConnCache`), reached via
  `proc.GetSession()`, closed in `Session.Close()`.  Scans therefore run on the
  session's CN (§5).
* Config resolution order at scan time: table option `config` (inline JSON) →
  session variable `@esql_tvf_config` / `@sql_tvf_config`
  (`ConfigFromSessionVar`) → error.
* `esql_tvf_disconnect(handle)` on a handle that an external table is implicitly
  using just removes it from the cache; the next scan reconnects.  A MO session
  executes one statement at a time, so no scan is in flight during a
  disconnect.

The connection is per-session and lazily opened on first scan; `CREATE
EXTERNAL TABLE` does **not** connect (no validation of reachability at DDL
time, same as DATASTREAM), only of option syntax and config JSON shape
(`connectESQL`/`connectSQL` parse paths, without dialing).


5. Execution
------------

Plan: `query_builder.go` recognizes the table via its catalog envelope and
feature flag (§6), sets `ExternType_ESQL_TB` / `ExternType_SQL_TB` (two new
`ExternType` values), fills a new `plan.ForeignScan{kind, config, default_query}`
in `ExternScan`, and appends the hidden `__mo_query` column exactly where
`__mo_filepath` is appended for `EXTERNAL_TB`.  `isPrepareStatement` is allowed
(nothing is bind-time specific beyond what `__mo_filepath` already handles).

Compile (`compileExternScan`): a new branch next to `compileDatastreamScan`:

1. derive + filter the query list (§2.2) into `fileList`;
2. build the synthetic `ExternalParam` via `BuildForeignTVFExternParam`;
3. one scope on the session CN (`constructScopeForExternal(c.addr, ...)`,
   `Mcpu = 1`, not dispatched remotely — same placement argument as D7 of the
   TVF plan: the session-local connection cache must be reachable).  Remote-run
   decoding (`remoterun.go`) carries `ForeignScan` in the pipeline proto like
   `DatastreamScan`, but the scan is pinned to the session CN;
4. `constructExternal(node, param, ctx, fileList, nil, nil, strictSqlMode)`.

Reader: `external.Prepare` gains a dispatch case `param.ForeignScan != nil` →
`NewForeignScanReader(param)`, a thin `ExternalFileReader` that, per entry of
`FileList`, resolves the connection (§4), calls `Conn.Query(ctx, text)`, sets
`Fileparam.Filepath = text`, and wraps the stream in the existing
`ForeignTVFReader` (`newCSVParserFromReader` + `CsvReader.makeBatchRows`).  It
iterates the list sequentially (one foreign query at a time per scan — queries
are not parallelized within a scan; the single-CN, `Mcpu=1` shape mirrors the
TVF).  Different external tables / TVFs in one MO query are separate scopes and
run concurrently; downstream join/agg parallelize per optimizer.

Errors from the source (HTTP 4xx/5xx from ES|QL, driver errors) surface as
`moerr` invalid-input errors carrying the source message, as in the TVFs.


6. Catalog, SHOW CREATE, feature flag
-------------------------------------

Mirror DATASTREAM:

* `rel_createsql` holds a planner-owned, anchored comment envelope
  `/* MO_FOREIGN: version=1; kind=esql|sql; config=<url-escaped>; query=<url-escaped> */`
  (`pkg/sql/foreigntvf` or a sibling `pkg/sql/foreignext` package owns
  `ParseTableOptions` / `BuildCreateSQLEnvelope` / `ParseCreateSQLEnvelope`);
* a durable feature bit (`features.ForeignExternal`, next to
  `DataStreamExternal`) must agree with the envelope — envelope-alone could be
  forged through the user-controlled `rel_createsql` JSON of a generic external
  table (`IsDataStreamTableDef` rationale);
* `SHOW CREATE TABLE` emits `ENGINE = ESQL|SQL WITH ("config" = '...', "query"
  = '...')`.  The config is always emitted **redacted** (`'config' =
  '<redacted>'`) because it carries credentials (ES password / DSN password),
  so a table restored from SHOW CREATE output (snapshot/PITR replay) must have
  `config` re-supplied — same trade-off DATASTREAM makes for `apikey`.  The
  recommended production form is no `config` + session variable.
* `SHOW CREATE` never emits `__mo_query`; `DESC`/`SHOW COLUMNS` hide it like
  `__mo_filepath`.


7. Non-goals
------------

* No projection or limit pushdown, and no rewriting of the foreign query text
  itself: `pushdown` = `true` wraps the text as a derived table (§2.5), it
  never edits it.  ESQL has no pushdown at all.  Both SQL drivers are served,
  because the wrapper is written in the subset both accept — bare identifiers,
  no quoting (§2.5); a column whose name cannot be written that way keeps its
  conjunct local.
* No runtime schema; no JSON-array no-schema mode.
* No parameter binding (`?`) in the foreign text.
* No writes (`INSERT INTO` an ESQL/SQL external table is an error); `INSERT ...
  SELECT FROM` one is the intended ETL path.
* No DDL-time connectivity check.
* Oracle / SQL Server drivers: same deferred status as the TVFs (driver registry).


8. Reuse map
------------

| concern | reused from | new |
|---|---|---|
| grammar, `tree.*TableParam`, option validation, envelope, feature bit, SHOW CREATE | `ENGINE = DATASTREAM` (`mysql_sql.y`, `tree/datastream.go`, `pkg/sql/datastream/config.go`, `plan/datastream_util.go`, `build_show_util.go`) | copy the shape for ESQL/SQL |
| hidden column + query-level filter split + list pruning | `__mo_filepath` (`catalog.ExternalFilePath`, `query_builder.go`, `external.FilterFileList`, `isFileLevelFilter`, `makeFilepathBatch`, `getFieldFromLine`) | `catalog.ExternalQuery`; candidate derivation from `=`/`IN`/`OR` (§2.2 step 2) |
| connect / cache / config resolution / secrets-in-session | `pkg/sql/foreigntvf` (`Connect`, `ResolveOrConnect`, `ConfigFromSessionVar`, `MakeHandle`), `process.ForeignConnCache`, `Session.Close()` | — |
| query execution → CSV stream | `foreigntvf.Conn.Query` (ES|QL `format=csv`, `encodeRowsCSV`) | — |
| CSV → batch, type coercion, NULLs, pruning-safe field mapping | `external.BuildForeignTVFExternParam`, `external.ForeignTVFReader`, `CsvReader` | `NewForeignScanReader` (list iterator around `ForeignTVFReader`) |
| compile/scope/placement | `compileDatastreamScan` shape, `constructExternal` | `compileForeignScan` |


9. Tests
--------

* **Go unit**: option validation + envelope round-trip (incl. url-escaping of
  `;`/`*/` in DSNs); query-list derivation from `=`, `IN`,
  `OR`, mixed with `LIKE` pruning, missing predicate + `query` option fallback,
  missing both → error; `__mo_query` column fill; pruning/reordering of
  projected columns keeps field alignment.
* **BVT (self-contained, in the PR suite)** — loopback `ENGINE = SQL` with
  `driver=mysql`, DSN pointing back at the MO server (same approach as the
  `sql_tvf` BVT): create a source table, an external table, `SELECT ... WHERE
  __mo_query = '...'` with/without extra local predicates, `IN (...)` with
  `__mo_query` projected, `query` option default, `INSERT ... SELECT` ETL, error
  cases (bad SQL text, wrong column count, no config anywhere, unknown option,
  `INSERT INTO` the external table), `SHOW CREATE` redaction, and a prepare
  statement with `__mo_query = ?`.
* **E2E (ephemeral ES, manual workflow)** — extend `optools/esql_ci.bash` /
  `test/esqltvf/esql_e2e_local.go` with an `ENGINE = ESQL` table over the seeded
  index: schema-typed rows, `IN` of two ES|QL queries, local predicate on a
  declared column, config inline and via `@esql_tvf_config`.
