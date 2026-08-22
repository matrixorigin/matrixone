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
| `config` | optional | optional | The **same JSON** that `esql_tvf_connect(config)` / `sql_tvf_connect(config)` accept: an `elasticsearch.Config` JSON for ESQL, `{"driver": "...", "dsn": "..."}` for SQL.  Passed verbatim to `foreigntvf.Connect(kind, configJSON)`.  May also be `env:NAME`, resolved from the CN process environment at scan time (like the datastream `apikey`), so secrets never enter the catalog. If omitted, the scan uses `@esql_tvf_config` / `@sql_tvf_config` of the querying session (exactly `foreigntvf.ConfigFromSessionVar`); error if neither is set. |
| `query` | optional | optional | Default query text, used when a `SELECT` has no `__mo_query` predicate (see §2).  Plain text, no placeholders. |

No other options.  In particular there is **no** `recheck`: nothing but the
query text is pushed to the source, so MO always evaluates every ordinary
predicate locally (§2.4).  Any `URL`/`FORMAT`/`INFILE` clause is rejected for
these engines, as for DATASTREAM.


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
  pushed into the text.

### 2.4 Ordinary predicates

All conjuncts that touch a declared column stay in `node.FilterList` and are
evaluated by MO on the returned rows, as for any external table.  There is no
filter deparser / pushdown hint as in DATASTREAM: the user already wrote the
foreign query, so pushing down is their job, in their dialect.  Consequently
there is no `recheck` option and no cross-engine collation/time-zone
correctness caveat.


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
* Config resolution order at scan time: table option `config` (inline JSON, or
  `env:NAME` resolved on the CN) → session variable `@esql_tvf_config` /
  `@sql_tvf_config` (`ConfigFromSessionVar`) → error.
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
  = '...')`.  An `env:NAME` config is emitted verbatim (it is not a secret).  An
  inline config is emitted **redacted** (`'config' = '<redacted>'`) because it
  carries credentials (ES password / DSN password), so a table restored from
  SHOW CREATE output (snapshot/PITR replay) must have `config` re-supplied —
  same trade-off DATASTREAM makes for `apikey`, stated in the same place.  The
  recommended production form is `env:NAME` or no `config` + session variable.
* `SHOW CREATE` never emits `__mo_query`; `DESC`/`SHOW COLUMNS` hide it like
  `__mo_filepath`.


7. Non-goals
------------

* No predicate/projection/limit pushdown into the foreign query text; no
  deparser; no `recheck`.
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
| connect / cache / config resolution / secrets-in-session | `pkg/sql/foreigntvf` (`Connect`, `ResolveOrConnect`, `ConfigFromSessionVar`, `MakeHandle`), `process.ForeignConnCache`, `Session.Close()` | `env:NAME` resolution for `config` (copy of `ResolveAPIKey`) |
| query execution → CSV stream | `foreigntvf.Conn.Query` (ES|QL `format=csv`, `encodeRowsCSV`) | — |
| CSV → batch, type coercion, NULLs, pruning-safe field mapping | `external.BuildForeignTVFExternParam`, `external.ForeignTVFReader`, `CsvReader` | `NewForeignScanReader` (list iterator around `ForeignTVFReader`) |
| compile/scope/placement | `compileDatastreamScan` shape, `constructExternal` | `compileForeignScan` |


9. Tests
--------

* **Go unit**: option validation + envelope round-trip (incl. `env:` and
  url-escaping of `;`/`*/` in DSNs); query-list derivation from `=`, `IN`,
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
  declared column, config via `env:` and via `@esql_tvf_config`.
